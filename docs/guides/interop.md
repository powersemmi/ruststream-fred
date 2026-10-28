---
sidebar_position: 13
description: "Exchange messages with Redis clients in other languages: the stream entry layout, the list and channel frames, the JSON document, and faststream's frame."
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
A message without headers is written as the bare body.
By default the frame is binary, and it opens with a marker header named `ruststream-frame` with an empty value:

| Bytes | Meaning |
| --- | --- |
| 4 | the number of headers plus one for the marker, a big-endian `u32` |
| 4, `ruststream-frame`, 4 | the marker: the name's length 16, the name, and the value's length 0 |
| 4, then the name | for each header: the name's length and the name |
| 4, then the value | the value's length and the value |
| the rest | the body |

A body without headers that carries the marker at the place a frame carries it is written in a frame, so it reads back unchanged.

An [envelope](../components/subscriptions/envelopes.md) named on the publish policy writes another layout.
[`Framed`](api:ruststream_fred::envelope::Framed) with `JsonCodec` writes a JSON document, and the mailer pops a JSON object:

```bash
redis-cli RPOP emails
# {"headers":{},"payload":{"to":"ann@example.com","subject":"Order 1 received"}}
```

`payload` is the body, here the email embedded as the JSON value it is, so one parse reads the whole document.
A body that is not a JSON text travels as base64 in `payload_base64` instead, the convention of CloudEvents' JSON format, and a header is a string.
The same document shows readable values in a tool like RedisInsight.

## Reading a list value or a channel message

A subscription reads the envelope its publisher writes: the mount site names it with [`RedisEnvelopeExt::envelope`](api:ruststream_fred::envelope::RedisEnvelopeExt::envelope), as `b.include(handler.envelope(Framed::new(JsonCodec)))`, and without one the subscription reads the broker's.
The JSON document is read only in its exact shape: an object with `headers` and `payload` and no other field.

A value that is not the envelope's own, such as one another client pushes or publishes as plain JSON, arrives as the body with no headers, whatever its first bytes are:

```bash
redis-cli LPUSH jobs '{"id":7}'
redis-cli PUBLISH prices '{"sku":"tea","cents":450}'
```

So a producer in another language writes plain values unless it needs headers, and then it writes the JSON document.

A binary frame written by 0.7.1 or an earlier release has no marker, so a current subscription reads it as the body, frame bytes included.
All services that share a list or a channel upgrade together, and a list is drained of the old values first.
A subscription of an earlier release reads the new frame, and sees the marker as one more header.

## Sharing with a faststream service

A broker built with [`envelope(FastStream)`](api:ruststream_fred::RedisBroker::envelope) reads and writes [faststream](https://faststream.ag2.ai)'s `BinaryMessageFormatV1`, the frame a faststream service writes to Redis:

| Bytes | Meaning |
| --- | --- |
| 8 | the identity header `\x89BIN\r\n\x1a\n` |
| 2 | the version, 1 |
| 4, 4 | where the header block starts and where the body starts, big-endian `u32`s |
| 2 | at the header block: the number of headers, a big-endian `u16` |
| 2, then the name; 2, then the value | for each header: the name's length and the name, the value's length and the value |
| the rest | at the body's start: the body |

A list element and a channel message are the frame, and a stream entry holds it in the field `__data__`.
Every message is framed, headers or none, as faststream frames it.
The header names travel as they are: faststream's own headers arrive as `correlation_id` and `reply_to`, and a header this service sets reaches a faststream handler under the name it was given.
A value without the identity header arrives as the body with no headers.

## Testing

The test in the example files runs in process: the order arrives as the shop writes it, and the value on `emails` is the JSON document, byte for byte.
A live test reads both from a server with a second `fred` client, as another program would: the newest entry of `orders` holds the order under `_payload`, and the newest value of `emails` is the document.
