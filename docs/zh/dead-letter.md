# 死信与投递次数上限 { #dead-letter-and-poison-cap }

永远处理不掉的消息会无休止地重新投递下去，而 `nack(requeue = false)` 把它丢得不留痕迹。两项设置给这
件事设上界，它们默认都关闭，适用于流和可靠模式的列表。

`dead_letter(key)` 把丢弃掉的或已中毒的消息复制到写明的键，而不是直接扔掉，并且限定在同一个传输族之内：
流到流，列表到列表。`max_deliveries(n)` 在 `n` 次尝试之后停止重新投递，把消息送进死信；没有设死信
键时则丢弃它。

副本上带着 `x-dead-letter-reason` 消息头（`dropped` 或 `max-deliveries`），并且它在对原件做 ack
之前就写出，因此崩溃留下的是一份重复，而不是一次丢失。

=== "Redis Stream"

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_dead_letter.rs:handler"
    ```

=== "Redis List"

    ```rust
    --8<-- "crates/ruststream-fred/examples/fred_list_dead_letter.rs:handler"
    ```

这个上限把消息毒害订阅的两条途径一起计入：框架的重试次数消息头，由 `nack` 加重新发布这个循环递增；
以及 Streams 回收路径上 Redis 自带的投递次数。回收来的投递还带着 `redis-delivery-count` 和
`redis-idle-ms` 两个消息头，处理器因此可以自己分支处理，或者自己把消息送进死信。

简单模式的列表和 Pub/Sub 无法 ack，因此它们没有死信这条路径。
