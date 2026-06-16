# Transactions

On standalone and sentinel the stream publisher is transactional (`begin_transaction` buffers,
`commit` flushes the whole batch in publish order through a single `fred` pipeline, `abort`
discards). The idiomatic way to use it is a batch-publishing handler wired with a `.transactional()`
publisher: every reply is committed atomically.

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:batch"
```

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:mount"
```

Cluster does not offer this (buffered keys may hash to different nodes), so the transaction returns
an error there.
