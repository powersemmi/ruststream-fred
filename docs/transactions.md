# Transactions

The stream publisher offers both framework transaction kinds on standalone and sentinel. They differ
in who owns the buffer. Cluster offers neither (buffered keys may hash to different nodes), so
opening a transaction returns an error there.

## Borrowed: one transaction on the handle

`begin_transaction` starts buffering on the publisher handle, `commit` flushes the buffer in publish
order through a single `fred` pipeline, and `abort` discards it. Clones of a handle share the same
open transaction.

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

The owned commit flushes its buffer as one `MULTI` / `EXEC` block, so the batch becomes visible
atomically; the borrowed pipeline batches the writes without that guarantee. Dropping an unsettled
transaction discards the buffer like an abort and logs a warning, because a vanishing buffer is
almost always a missing `commit`. A failed commit has still consumed the transaction and its buffer
is lost: recovery is redelivery of the inputs, not resubmission of the buffer.

`TypedPublisher::transaction()` is the typed sugar over the same kind: it encodes each value with
the publisher's codec before buffering.
