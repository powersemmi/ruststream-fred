---
sidebar_position: 3
title: EntryId
description: "EntryId is a stream entry's milliseconds-sequence id; the EntryId key binds the id a delivery was read at."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# EntryId

[`EntryId`](api:ruststream_fred::EntryId) is a stream entry's `<milliseconds>-<sequence>` id; the [`EntryId`](api:ruststream_fred::context::keys::EntryId) key binds the id a delivery was read at.

The server assigns the id at `XADD`.
Its first half, [`milliseconds`](api:ruststream_fred::EntryId::milliseconds), is the server clock at that moment.
The second, [`sequence`](api:ruststream_fred::EntryId::sequence), tells apart the entries added within one millisecond.
Ids compare the way the stream orders its entries.
`EntryId::new(milliseconds, sequence)` builds one, and parsing a string checks it, so a value is always a well-formed id.
The milliseconds half alone parses too: `"1700000000000"` is `1700000000000-0`.
`to_string` writes the form Redis takes.
[`previous`](api:ruststream_fred::EntryId::previous) is the id right below, and `EntryId::ZERO` is below every entry.
`Ctx<keys::EntryId>` binds the id of the delivery, and [`RedisGroupPosition::after(id)`](./index.md) moves a group past it.
A batch spans many entries, so a batch context carries no entry id.

## Example

Each receipt names the stream entry it billed and when the server added it.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/redis_positions_entry_id.rs#entry-id"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/redis_positions_entry_id.rs#entry-id"
```

</TabItem>
</Tabs>
