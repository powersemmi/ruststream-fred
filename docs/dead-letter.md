# Dead-letter and poison cap

By default a failing message is redelivered forever and a `nack(requeue = false)` discards it. Two
opt-in settings bound that: `dead_letter(key)` copies dropped and poison messages to the named key
(same transport family, stream to stream or list to list) instead of discarding them, and
`max_deliveries(n)` caps the delivery count.

The copy is tagged with the `x-dead-letter-reason` header (`dropped` or `max-deliveries`) and written
before the original is acked, so a crash leaves a duplicate rather than a loss.

=== "Redis Stream"

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_dead_letter.rs:handler"
    ```

=== "Redis List"

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_list_dead_letter.rs:handler"
    ```

`max_deliveries(n)` caps the delivery count. It is checked against both the framework retry-count
header (the `nack`/republish loop) and, on the Streams reclaim path, the native Redis Streams delivery
count, so a message poisoning either way is caught. Reclaimed deliveries also carry
`redis-delivery-count` and `redis-idle-ms` headers, so a handler can branch or dead-letter manually.

Simple List and Pub/Sub cannot ack, so they have no dead-letter path.
