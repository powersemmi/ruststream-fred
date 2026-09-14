# 生成出来的文档 { #the-generated-document }

RustStream 服务把自己描述成一份 AsyncAPI 文档，而这个 crate 往里补上只有 Redis 知道的那部分：一个
频道的消息由哪种结构承载，以及这种结构是怎么配置的。

在两个 crate 上都打开这个特性：

```toml
ruststream = { version = ">=0.7.0-rc.6, <0.8.0", features = ["macros", "asyncapi"] }
ruststream-fred = { version = "0.7", features = ["asyncapi"] }
```

AsyncAPI 规范里有一个 `redis` 绑定，它的四个对象全是空的：里面没有消费者组、读取模式或投递模式的
字段。绑定的键取自一个封闭列表，因此这个 crate 在它旁边写一个扩展 `x-ruststream-redis`，规范允许
它出现在绑定所在的同一层。

流的订阅报告自己经由哪个消费者组和哪个消费者读取、自己的读取模式，以及两种会认领条目的模式所用的
空闲阈值：

```json
--8<-- "crates/ruststream-fred/tests/fixtures/asyncapi_stream_channel.json"
```

这就是 `channels.orders.bindings.x-ruststream-redis` 下面的内容，crate 里有一个测试拿这份文件来核
对生成的文档。

列表报告自己是否 ack（`reliable`）、这期间未完成的条目躺在哪个处理中列表上，以及它的消息头装在什
么信封里。频道报告自己的投递模式，以及它的地址是不是一个通配模式。发布者从另一侧报告同一套词汇：
`PUBLISH` 发出时用的投递模式、列表推入时重新设上的过期时间、它写出的信封。

每个值都只取自描述符或策略本身，因为文档在任何连接建立之前就已经构建好。由此有两个结论值得记住。
Redis 的服务器版本不会写进文档：客户端是在握手时才知道它的，而握手还没发生。任何凭据也到不了文档
里，这一点很重要，因为文档是拿来发布和共享的：服务器那一项写的是客户端拨过去的主机和端口，协议
头、`user:password@`、数据库序号和查询串都已剥掉。
