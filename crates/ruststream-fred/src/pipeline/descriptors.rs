//! The pipelined subscription sources under names of their own.
//!
//! A service does not build these: `.pipeline()` and `.atomic()` at the mount site produce them
//! from the descriptor the attribute named. The names are what the builder's type and the
//! compiler's messages show.

use super::{Atomic, Pipelined, Plain};
use crate::pubsub::{RedisPubSub, RedisPubSubPattern};
use crate::{RedisList, RedisStream};

/// A stream subscription with a window: what `.pipeline()` at the mount site makes of a [`RedisStream`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Event { id: u64 }
///
/// #[subscriber(RedisStream::new("orders").group("workers"))]
/// async fn record(event: &Event, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     if pipeline.incr(format!("seen:{}", event.id)).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // The source of this registration is a `RedisStreamPipeline`.
///             b.include(record.pipeline());
///         },
///     )
/// }
/// # }
/// ```
pub type RedisStreamPipeline = Pipelined<RedisStream, Plain>;

/// A stream subscription with an atomic window: what `.pipeline().atomic()` at the mount site makes of a [`RedisStream`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Event { id: u64 }
///
/// #[subscriber(RedisStream::new("{orders}").group("workers"))]
/// async fn record(event: &Event, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     if pipeline.incr(format!("seen:{}", event.id)).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // The source of this registration is a `RedisStreamAtomic`.
///             b.include(record.pipeline().atomic());
///         },
///     )
/// }
/// # }
/// ```
pub type RedisStreamAtomic = Pipelined<RedisStream, Atomic>;

/// A list subscription with a window: what `.pipeline()` at the mount site makes of a [`RedisList`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::list::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Event { id: u64 }
///
/// #[subscriber(RedisList::new("jobs").reliable())]
/// async fn record(event: &Event, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     if pipeline.incr(format!("seen:{}", event.id)).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // The source of this registration is a `RedisListPipeline`.
///             b.include(record.pipeline());
///         },
///     )
/// }
/// # }
/// ```
pub type RedisListPipeline = Pipelined<RedisList, Plain>;

/// A list subscription with an atomic window: what `.pipeline().atomic()` at the mount site makes of a [`RedisList`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::list::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Event { id: u64 }
///
/// #[subscriber(RedisList::new("{jobs}").reliable())]
/// async fn record(event: &Event, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     if pipeline.incr(format!("seen:{}", event.id)).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // The source of this registration is a `RedisListAtomic`.
///             b.include(record.pipeline().atomic());
///         },
///     )
/// }
/// # }
/// ```
pub type RedisListAtomic = Pipelined<RedisList, Atomic>;

/// A Pub/Sub channel subscription with a window: what `.pipeline()` at the mount site makes of a [`RedisPubSub`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::pubsub::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Event { id: u64 }
///
/// #[subscriber(RedisPubSub::new("events"))]
/// async fn record(event: &Event, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     if pipeline.incr(format!("seen:{}", event.id)).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // The source of this registration is a `RedisPubSubPipeline`.
///             b.include(record.pipeline());
///         },
///     )
/// }
/// # }
/// ```
pub type RedisPubSubPipeline = Pipelined<RedisPubSub, Plain>;

/// A Pub/Sub channel subscription with an atomic window: what `.pipeline().atomic()` at the mount site makes of a [`RedisPubSub`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::pubsub::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Event { id: u64 }
///
/// #[subscriber(RedisPubSub::new("events"))]
/// async fn record(event: &Event, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     if pipeline.incr(format!("seen:{}", event.id)).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // The source of this registration is a `RedisPubSubAtomic`.
///             b.include(record.pipeline().atomic());
///         },
///     )
/// }
/// # }
/// ```
pub type RedisPubSubAtomic = Pipelined<RedisPubSub, Atomic>;

/// A Pub/Sub pattern subscription with a window: what `.pipeline()` at the mount site makes of a [`RedisPubSubPattern`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::pubsub::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Event { id: u64 }
///
/// #[subscriber(RedisPubSubPattern::new("events.*"))]
/// async fn record(event: &Event, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     if pipeline.incr(format!("seen:{}", event.id)).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // The source of this registration is a `RedisPubSubPatternPipeline`.
///             b.include(record.pipeline());
///         },
///     )
/// }
/// # }
/// ```
pub type RedisPubSubPatternPipeline = Pipelined<RedisPubSubPattern, Plain>;

/// A Pub/Sub pattern subscription with an atomic window: what `.pipeline().atomic()` at the mount site makes of a [`RedisPubSubPattern`].
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::pubsub::prelude::*;
/// # #[derive(serde::Deserialize)]
/// # struct Event { id: u64 }
///
/// #[subscriber(RedisPubSubPattern::new("events.*"))]
/// async fn record(event: &Event, Ctx(pipeline): Ctx<keys::Pipeline>) -> HandlerOutcome {
///     if pipeline.incr(format!("seen:{}", event.id)).await.is_err() {
///         return HandlerOutcome::retry();
///     }
///     HandlerOutcome::ack()
/// }
///
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("events", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // The source of this registration is a `RedisPubSubPatternAtomic`.
///             b.include(record.pipeline().atomic());
///         },
///     )
/// }
/// # }
/// ```
pub type RedisPubSubPatternAtomic = Pipelined<RedisPubSubPattern, Atomic>;
