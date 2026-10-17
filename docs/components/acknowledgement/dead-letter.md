---
sidebar_position: 3
title: dead_letter
description: "max_attempts caps deliveries and dead_letter names where a spent message goes, written with its own form's command."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# dead_letter

[`max_attempts(n)`](api:ruststream::runtime::Mounting::max_attempts) caps deliveries and [`dead_letter(name)`](api:ruststream::runtime::Mounting::dead_letter) names where a spent message goes, written with its own form's command.

Both are steps of the mount chain, and they read the same on a stream, a list and a channel.
`max_attempts(n)` counts every delivery of one message, the first one included.
When the `n`-th delivery asks for a retry, the runtime settles it and publishes it to the dead-letter name instead, payload and headers as they arrived.
The name is written the way the source form writes: `XADD` from a stream, `LPUSH` from a list, `PUBLISH` from a channel.
A cap without a dead-letter name drops the spent delivery and logs a warning that names the subscription.
A dead-letter name without a cap carries every retry away on the first one.
Under a cap an immediate retry travels as a copy the runtime publishes, so that the count moves with the message in [`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER).
A `claiming` subscription reads Redis's own [delivery count](./counts.md) instead and appends nothing.
The dead-letter key is an ordinary key of the same broker, and a subscription reads it like any other.
A name one subscription reads as one Redis type and another writes as a second type stops the service at startup, and the error names both.

## Example

A payment the gateway keeps rejecting goes to `payments.dlq` after five deliveries; a refund goes to `refunds.failed` after three.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_acknowledgement_dead_letter.rs#declaration"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_acknowledgement_dead_letter.rs#declaration"
```

</TabItem>
</Tabs>

An operator's subscription reads the dead-letter stream:

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_acknowledgement_dead_letter.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_acknowledgement_dead_letter.rs#dead_letters"
```

</TabItem>
</Tabs>
