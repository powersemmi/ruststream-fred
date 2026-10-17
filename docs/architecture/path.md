---
title: Path
sidebar_position: 3
description: "The way of one stream entry through a RustStream service: the append, the group read, the delivery, the handler, the reply and the XACK."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The path of one stream entry that is answered, from the append to its acknowledgement",
  "text": "The diagram as text",
  "append": ["XADD", "a _payload field and h: fields"],
  "read": ["XREADGROUP", "up to 64 entries, on the read client"],
  "entry": ["RedisMessage", "fields become payload and headers"],
  "dispatch": ["Dispatch", "the runtime's loop or its workers"],
  "decode": ["Decode", "the payload into the handler's type"],
  "body": ["Handler", "async fn under #[subscriber]"],
  "reply": ["Reply", "XADD on a pool connection"],
  "settle": ["Settlement", "the outcome decides"],
  "ack": ["XACK", "on a pool connection"]
};

# Path

The way of one stream entry through a RustStream service: the append, the group read, the
delivery, the handler, the reply and the `XACK`.

<Diagram name="path" words={words} />

A read returns up to 64 entries in one round trip, and the subscription hands them out one at a
time before it reads again. A batch handler reads as many entries as its batch size instead. When
the stream has nothing new, the read blocks for the subscription's `block` interval, five seconds
by default.

Each entry becomes a [`RedisMessage`](api:ruststream_fred::RedisMessage): the `_payload` field is
the payload, and each `h:` field a header. The runtime decodes the payload and calls the handler.

The reply is encoded and published with `XADD` on a pool connection, and the delivery settles
after it. A list read and a Pub/Sub message take the same way from the delivery on, with the
form's own settlement, which [Settlement](settlement.md) draws.
