# Transactions

A transaction sends a group of publishes as one `MULTI` / `EXEC` block, in publish order, so
subscribers see the whole group or none of it. The stream publisher offers both kinds the framework
defines, on standalone and sentinel. They differ in who owns the buffer.

## Borrowed: one transaction on the handle

`begin_transaction` claims the handle's transaction and starts buffering, `commit` flushes the
buffer, and `abort` discards it. A clone of the handle works with the same open transaction.

The usual shape is a batch handler whose replies are committed together. A slice parameter is what
makes a handler a batch handler, and the mount site names the batch size (see
[Batches](streams.md#batches)). `.out_reply(TransactionalPublish)` names the policy the replies
leave through, and `.transactional()` after it puts one batch's replies in one `MULTI` / `EXEC`
block.

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:batch"
```

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:mount"
```

A second `begin_transaction` while one is open returns an error and leaves the open transaction
untouched. So does a `commit` or an `abort` with nothing open.

## Owned: a transaction value per call

`publisher.transaction()` returns a `RedisTransaction` with a buffer of its own. Any number of them
can be open on one handle, settling one never touches another, and the handle keeps publishing
directly meanwhile. `commit` and `abort` consume the value, so a double commit and a publish after
settling do not compile.

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:owned"
```

Dropping an unsettled transaction discards the buffer like an abort and writes a warning to the log.
A commit that returns an error has consumed the transaction and its buffer is gone: recover by
redelivering the inputs, not by resubmitting the buffer.

`publisher.owned_transaction()` buffers values instead of bytes: it encodes each one with the
default codec.

## What Redis does and does not guarantee

Two properties apply to both kinds, because both are `MULTI` / `EXEC`:

- **No cluster.** A `MULTI` block cannot span hash slots, so opening a transaction on a cluster
  returns an error instead of committing a group that is not atomic.
- **No rollback.** A command that returns an error at *runtime* inside `EXEC` leaves the commands
  before it committed. For a group of `XADD`s against stream keys that happens when Redis runs out
  of memory or the key holds a non-stream type. A command the server refuses to *queue* discards the
  whole block instead.

Pipelining is a throughput tool, not a transaction: the list publisher sends its `LPUSH` and
`PEXPIRE` in one round trip, and they commit separately.
