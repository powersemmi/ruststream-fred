---
sidebar_position: 3
title: Envelopes
description: "An envelope is how a message's headers travel beside its payload; the broker names one, and a descriptor or a publish policy names its own."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Envelopes

An envelope is how a message's headers travel beside its payload; the broker names one, and a descriptor or a publish policy names its own.

A list element and a channel message are one value, and a stream entry is a map of fields.
An envelope is the layout of the headers and the payload in them.
[`RedisBroker::envelope`](api:ruststream_fred::RedisBroker::envelope) sets the broker's envelope, and every subscription and publish policy uses it unless it names its own.
A subscription names its own at the mount site with [`RedisEnvelopeExt::envelope`](api:ruststream_fred::envelope::RedisEnvelopeExt::envelope): `b.include(handler.envelope(FastStream))`.
On the manual path the descriptor names it: `RedisList::new("jobs").envelope(FastStream)`.
A publish policy names its own with `envelope(..)`: `RedisListPublish::new().envelope(FastStream)`.
The envelope is a type, so the choice costs nothing per message.
A delivery finds its payload without reading the headers, and reads them the first time a handler asks.

| Envelope | List element, channel message | Stream entry |
| --- | --- | --- |
| [`envelope::RustStream`](api:ruststream_fred::envelope::RustStream), the default | a binary frame that opens with a marker; a message without headers is its bare payload | the payload in `_payload`, each header in `h:<name>` |
| [`FastStream`](api:ruststream_fred::envelope::FastStream) | faststream's `BinaryMessageFormatV1` | the same frame in `__data__` |
| [`Framed`](api:ruststream_fred::envelope::Framed) | a `{headers, payload}` document written by a codec, its payload in CloudEvents' convention | the document in `_payload` |
| [`NoOp`](api:ruststream_fred::envelope::NoOp) | the payload alone | the payload in `_payload` |

`envelope::RustStream` writes the bytes every earlier release wrote, so services on different releases read each other.
A binary frame written by 0.7.1 or an earlier release has no marker and reads as its payload, frame bytes included, so services that share a list or a channel upgrade together.
`FastStream` shares lists, channels and streams with a faststream service in both directions.
Its header names travel as they are, so faststream's own headers arrive as `correlation_id` and `reply_to`.
`Framed` with `JsonCodec` writes text that `redis-cli` and RedisInsight show as it is.
Its payload follows the convention of CloudEvents' JSON format.
A payload that is a JSON text is embedded as the value it is and reads back byte for byte: `{"headers":{"event":"created"},"payload":{"id":7}}`.
Any other payload travels as standard base64: `{"headers":{"event":"created"},"payload_base64":"CAc="}`.
With CBOR or msgpack the payload is a byte string in `payload`.
A header is a text field, and a header whose value is not text refuses the publish.
The document of the `codec(..)` framing of 0.7.2 and earlier releases carried its payload as a string, so `Framed` reads it as a JSON string; services that share such a list switch together, and the list is drained first.

A value that is not one of the envelope's own arrives as the payload with no headers, whatever its first bytes are.
A stream entry without the envelope's field arrives as one JSON object of its fields.
A payload that only looks like an envelope is written so that it reads back unchanged.
A subscription and the publishers that write its name use one envelope.
Two subscriptions that read one name through different envelopes stop the service at startup.

`NoOp` has no room for headers, so a delivery read through it carries none of the message's.
The headers a stream read itself reports, the delivery count and the idle time of a claiming read, stay.
A publish through it that carries a header fails, and the error names the header and the destination.
A retry copy the runtime publishes carries its retry count in a header, so a retry through `NoOp` fails the same way.

## Example

A faststream shop appends orders to a stream, and the broker reads its frame.
Jobs go to a list as JSON documents, and a price feed publishes bare JSON.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_subscriptions_envelopes.rs#envelopes"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_subscriptions_envelopes.rs#envelopes"
```

</TabItem>
</Tabs>

## An envelope of your own

An envelope of the service's own implements [`Envelope`](api:ruststream_fred::envelope::Envelope).
`locate` finds the payload in a value, `headers` reads the headers, and `seal` writes a message.
A stream entry keeps the sealed value in `_payload` unless the envelope overrides `entry_field` or the other entry methods.
A broker whose own envelope is `Framed` or one of the service's own registers itself with the test harness beside the tests: `ruststream::register_testable_broker!(RedisBroker<MyEnvelope>);`.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_subscriptions_envelopes.rs#custom"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_subscriptions_envelopes.rs#custom"
```

</TabItem>
</Tabs>
