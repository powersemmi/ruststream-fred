---
sidebar_position: 13
description: "Exchange messages with Redis clients in other languages: the stream entry layout, the list and channel frames, and the JSON envelope."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Interoperating with other clients

This guide shows what a RustStream service reads and writes in Redis, so that producers and consumers in other languages share its streams, lists and channels.

The example is an order service.
A shop written in another language appends orders to a stream, and a mailer written in another language pops emails off a list.

## Reading a stream entry

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/interop.rs#read"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/interop.rs#read"
```

</TabItem>
</Tabs>

A Redis stream entry is a set of fields.
A RustStream service keeps the message body in the field `_payload` and each header in a field named `h:` followed by the header's name.
A producer in any language writes an order the service reads like this:

```bash
redis-cli XADD orders '*' _payload '{"id":1,"email":"ann@example.com"}' h:trace-id 4bf92f35
```

The body is what the codec decodes: JSON with the default codec, so the producer writes the order as JSON.
A header field is optional; an entry with `_payload` alone arrives with no headers.
Fields with other names are ignored, so a producer can add its own.
An entry without `_payload` is one another program wrote, and its fields become the body as one JSON object.
Each value is a string when it is valid UTF-8 and an array of numbers otherwise; the `h:` fields stay headers:

```bash
redis-cli XADD orders '*' id 1 email ann@example.com
# the body: {"email":"ann@example.com","id":"1"}
```

A consumer in another language reads entries the service writes the same way: `_payload` holds the body, and the `h:` fields hold the headers.

## Writing a list value

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust title="src/main.rs" reference="examples/guide/interop.rs#write"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust title="src/main.rs" reference="examples/manual/guide/interop.rs#write"
```

</TabItem>
</Tabs>

A list entry and a Pub/Sub message are one value, so the headers travel in a frame around the body.
By default the frame is binary:

| Bytes | Meaning |
| --- | --- |
| 4 | the number of headers, a big-endian `u32` |
| 4, then the name | for each header: the name's length and the name |
| 4, then the value | the value's length and the value |
| the rest | the body |

A [codec](../components/subscriptions/framing.md) on the publish policy writes an envelope in that codec instead.
With `JsonCodec` the mailer pops a JSON object:

```bash
redis-cli RPOP emails
# {"headers":{},"payload":"{\"to\":\"ann@example.com\",\"subject\":\"Order 1 received\"}"}
```

`payload` is the body, here the email as JSON text, so the mailer parses the envelope and then its `payload`.
A body or a header that is valid UTF-8 is written as a string, and any other bytes as an array of numbers.
The same envelope shows readable values in a tool like RedisInsight.

## Reading a list value or a channel message

A subscription reads the frame its publisher writes: [`RedisList::codec`](api:ruststream_fred::list::RedisList::codec) and [`RedisPubSub::codec`](api:ruststream_fred::pubsub::RedisPubSub::codec) set the envelope's codec, and without one the subscription reads the binary frame.
A subscription with a codec also reads binary frames, which a service leaves behind when it switches.

A value that is neither, such as one another client pushes or publishes as plain JSON, arrives as the body with no headers:

```bash
redis-cli LPUSH jobs '{"id":7}'
redis-cli PUBLISH prices '{"sku":"tea","cents":450}'
```

So a producer in another language writes plain values unless it needs headers, and then it writes the JSON envelope.

## Testing

The test in the example files runs in process: the order arrives as the shop writes it, and the value on `emails` is the JSON envelope, byte for byte.
A live test reads both from a server with a second `fred` client, as another program would: the newest entry of `orders` holds the order under `_payload`, and the newest value of `emails` is the envelope.
