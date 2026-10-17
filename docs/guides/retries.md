---
sidebar_position: 5
description: "Retry a Redis message now or after a delay, keep the delay across a restart, cap the attempts and move what keeps failing to a dead-letter stream."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retries, delays and dead letters

This guide retries a message now or later, caps the attempts and moves what keeps failing to a [dead-letter](../components/acknowledgement/dead-letter.md) stream.

Redis has no per-message delay, no dead-letter structure, and a delivery count only inside a stream's consumer group.
The framework supplies the rest: the mount site declares the cap and the destination, and the subscription chooses how a delay is kept.

The example is a refunds service that sends refunds through a card processor.

## The subscriber

The processor refuses a refund in three ways, and each needs a different outcome.
`Busy` means its rate limit is used up: a retry right now would be refused again, so [`retry_after`](../components/acknowledgement/retry-after.md) waits 30 seconds.
`Unavailable` means it did not answer: `retry` tries again at once.
`Declined` means the card account is closed, and another try gets the same answer, so the refund is dropped.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#refund"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#refund"
```

</TabItem>
</Tabs>

`retry` on a stream appends a copy of the entry to the stream and acknowledges the original.
On the other structures it does what the structure allows:

| Subscription | `retry` without a cap |
|---|---|
| [`RedisStream::new`](../components/streams/index.md), [`reclaim`](../components/streams/reclaim.md) | a copy appended to the stream |
| [`RedisStream::claiming`](../components/streams/claiming.md) | the entry stays pending and comes back after `min_idle` |
| [`RedisList::reliable`](../components/lists/reliable.md) | the entry moves back to the list |
| [`RedisList::new`](../components/lists/index.md), [`RedisPubSub`](../components/pubsub/index.md) | nothing: the message is gone |

Under a declared cap, `retry` becomes a copy on every subscription but a claiming one, so the count travels with the message; a claiming entry carries Redis's own count.

## Three ways to keep a delay

`retry_after` settles the delivery and asks for it again after the delay.
A subscription keeps that delay in one of three ways.

The framework's own way needs nothing on the descriptor.
The runtime holds a copy and publishes it back to the stream key once the delay is up.
The copy carries the retry-count header, [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER), raised by one.
It lives in the process, so a service that stops before the delay ends loses it.

[`DelayedRetry::DurableZset`](../components/acknowledgement/delayed-retry.md) keeps the delay in Redis, which is what `process_refund` does.
A delayed delivery goes into the sorted set `refunds.delayed` under its due time, with the retry count raised, and the original is acknowledged.
As the subscription reads, it moves every due entry back onto the stream under a new id.
The delay survives a restart of the service.
A sweep runs once per read, so a delay ends within one [`block`](../components/subscriptions/block.md) interval of its due time, five seconds by default.
The scores are wall-clock milliseconds, so the clocks of the service's hosts need to agree.
A `ttl` expires an abandoned set and has to be longer than the longest delay, or entries are lost before they are due.

A [claiming](../components/streams/claiming.md) subscription is the third way, and it needs no set.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#priority"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#priority"
```

</TabItem>
</Tabs>

A retry leaves the entry in the group's pending list, and the subscription's own next read takes it back once it has been idle `min_idle`.
Nothing is copied, and the entry keeps its id.
The only wait this offers is `min_idle`, so a shorter delay rounds up to it and a longer one is cut down to it.
`process_priority_refund` sets `min_idle` to the backoff for that reason.
A claiming subscription that names a `DurableZset` keeps each delay as asked instead.

## The cap and the dead-letter stream

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) counts every delivery of one refund, the first included.
The fifth delivery is not retried: [`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) publishes it to `refunds.dlq`, payload and headers as they arrived.
The dead letter is written the way the refund came in, so a refund from a stream lands on the stream `refunds.dlq`.
A cap without a destination rejects the delivery at the cap, and a destination without a cap takes every retry on the first one.
A dropped refund settles and goes nowhere; `refunds.dlq` holds only the refunds that ran out of attempts.

The cap reads one count, and where it comes from depends on the subscription.
A claiming subscription reads the [delivery count](../components/acknowledgement/counts.md) Redis keeps for the entry.
That count includes the deliveries of a worker that stopped, so a refund that crashed a worker counts against the cap too.
Every other subscription reads the retry-count header on the copies the runtime publishes, and the delay set raises it on every round as well.

A [seek](../components/positions/seek.md) that replays entries delivers them again.
On a claiming subscription the replays add to the delivery count and count against the cap; the retry-count header does not move.

## Reading the dead letters

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`refunds.dlq` is an ordinary stream, and `review` reads it through a group of its own.
A refund that came through copies carries the retry-count header, 5 here.
`review` logs the refund for a person to look at.

## Patterns name their retry destination

A copy goes to the address the subscription reports: the stream key, the list key or the channel.
A [Pub/Sub pattern](../components/pubsub/pattern.md) reads every channel its glob matches and names no single channel.
Its registration names where a copy goes with [`out_retry`](../components/acknowledgement/out-retry.md), as `.out_retry(Publish::default()).to("events.retry")`, and the service does not start without it.

## Testing the delays

The tests in the example file run the service in process on a paused clock.
`tb.advance(BACKOFF)` lets the 30 seconds pass at once, for the delay set and for the claiming subscription alike.
[Testing a Redis service](testing.md) covers the harness.
