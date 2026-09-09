# Dead-letter and poison cap

A message that never processes is redelivered forever, and `nack(requeue = false)` discards it
without a trace. Two settings, both off by default, bound that on a stream and on a reliable list.

`dead_letter(key)` copies a dropped or poisoned message to the named key instead of discarding it,
within the same transport family: stream to stream, list to list. `max_deliveries(n)` stops
redelivering after `n` attempts and dead-letters the message, or discards it when no dead-letter key
is set.

The copy carries the `x-dead-letter-reason` header (`dropped` or `max-deliveries`) and is written
before the original is acked, so a crash leaves a duplicate rather than a loss.

=== "Redis Stream"

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_dead_letter.rs:handler"
    ```

=== "Redis List"

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_list_dead_letter.rs:handler"
    ```

The cap counts both ways a message poisons a subscription: the framework's retry-count header, which
the `nack` and republish loop raises, and on the Streams reclaim path the native Redis delivery
count. A reclaimed delivery also carries the `redis-delivery-count` and `redis-idle-ms` headers, so
a handler can branch or dead-letter the message itself.

Simple List and Pub/Sub cannot ack, so they have no dead-letter path.
