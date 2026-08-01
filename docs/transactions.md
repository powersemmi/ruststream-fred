# Transactions

The stream publisher offers both framework transaction kinds on standalone and sentinel. Both hold
the buffer client-side while the transaction is open and commit it as one `MULTI` / `EXEC` block, in
publish order, so subscribers see the whole batch or none of it. They differ only in who owns that
buffer.

## Borrowed: one transaction on the handle

`begin_transaction` claims the handle's transaction and starts buffering, `commit` flushes the
buffer, and `abort` discards it. Clones of a handle share the same open transaction.

The idiomatic way to use it is a batch-publishing handler wired with a `.transactional()` publisher:
every reply of one batch is committed together.

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:batch"
```

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:mount"
```

Misuse errors rather than passing silently: a second `begin_transaction` while one is open leaves
the open one untouched and reports it, and a `commit` or `abort` with no open transaction is an
error.

## Owned: a transaction value per call

`publisher.transaction()` returns a `RedisTransaction` that owns its own buffer, so any number can
be open on one handle at a time and the handle keeps publishing directly meanwhile. Settling one
never touches another, and `commit` / `abort` consume the value, which makes a double commit or a
publish after settling a compile error rather than a runtime check.

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:owned"
```

Dropping an unsettled transaction discards the buffer like an abort and logs a warning, because a
vanishing buffer is almost always a missing `commit`. A failed commit has still consumed the
transaction and its buffer is lost: recovery is redelivery of the inputs, not resubmission of the
buffer.

`TypedPublisher::transaction()` is the typed sugar over this kind: it encodes each value with the
publisher's codec before buffering.

## What Redis does and does not guarantee

Two properties apply to both kinds, because both are `MULTI` / `EXEC`:

- **No cluster.** A `MULTI` block cannot span hash slots, so a cluster publisher rejects either kind
  with an error instead of pretending to be atomic.
- **No rollback.** A command that fails at *runtime* inside `EXEC` does not undo the commands before
  it - Redis has no rollback. For a block of `XADD`s against stream keys that is practically limited
  to running out of memory or a key holding a non-stream type. A command the server refuses to
  *queue* discards the whole block instead.

Pipelining is a separate, broker-specific throughput tool (the list publisher batches its `LPUSH`
and `PEXPIRE` that way): it saves round trips, it is not a transaction, and nothing here commits
through one.
