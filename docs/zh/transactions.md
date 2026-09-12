# 事务 { #transactions }

一个事务把一组发布作为一个 `MULTI` / `EXEC` 块按发布顺序发出，订阅者因此要么看到整组，要么一条也
看不到。流的发布者在 standalone 和 sentinel 上提供框架定义的两种事务。两者的区别在于谁持有缓冲。

## 借用式：句柄上的一个事务 { #borrowed-one-transaction-on-the-handle }

`begin_transaction` 占住句柄的那个事务并开始缓冲，`commit` 把缓冲刷出去，`abort` 把它丢掉。句柄的
克隆操作的是同一个打开着的事务。

常见的写法是一个批量处理器，它的应答一起提交。把处理器变成批量处理器的是切片参数，批次大小由挂载点
写明（见[批次](streams.md#batches)）。`.out(Reply, TransactionalPublish)` 写明应答经由哪个策略发出，
其后的 `.transactional()` 把一个批次的应答放进一个 `MULTI` / `EXEC` 块。

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:batch"
```

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:mount"
```

已经有一个事务打开时再调 `begin_transaction` 会返回错误，并且不动那个打开着的事务。什么都没打开时
调 `commit` 或 `abort` 也一样。

## 持有式：每次调用一个事务值 { #owned-a-transaction-value-per-call }

`publisher.transaction()` 返回一个 `RedisTransaction`，它自己带着缓冲。一个句柄上可以同时打开任意
多个，结算其中一个绝不会碰到另一个，与此同时句柄本身还照常直接发布。`commit` 和 `abort` 消费这个值，
因此两次提交、以及结算之后再发布，都无法通过编译。

```rust
--8<-- "crates/ruststream-fred/examples/fred_transaction.rs:owned"
```

丢弃一个还没结算的事务时，它会像 abort 一样扔掉缓冲，并往日志里写一条警告。返回了错误的 `commit` 已经
消费掉事务，它的缓冲也不在了：恢复的办法是重新投递输入消息，不是重新提交缓冲。

`publisher.owned_transaction()` 缓冲的是值，不是字节：它用默认编解码器逐个编码。

## Redis 保证什么，不保证什么 { #what-redis-does-and-does-not-guarantee }

有两条性质对两种事务都成立，因为两者都是 `MULTI` / `EXEC`：

- **不支持集群。** 一个 `MULTI` 块不能跨哈希槽，因此在集群上打开事务会返回错误，而不是提交一组并
  不原子的命令。
- **没有回滚。**在 `EXEC` 内部*运行期*返回错误的命令，会让它之前的命令保持已提交。对一组写向流键的
  `XADD` 来说，这发生在 Redis 内存耗尽、或者该键上放着非流类型的时候。服务器拒绝*入队*的命令则是
  另一回事：它让整个块作废。

流水线是提升吞吐的手段，不是事务：列表的发布者把 `LPUSH` 和 `PEXPIRE` 放在一次往返里发出，它们各自
分别提交。
