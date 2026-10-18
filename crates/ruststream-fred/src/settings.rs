//! Redis subscription options at the mount site.
//!
//! The framework passes exactly one number down to a broker's subscriber: the batch size, named by
//! the core's own `batch(n)` step. Everything else about how a read forms is this crate's
//! vocabulary, and [`RedisSubscribeExt`] is where it chains - after the size, on the same line.
//!
//! There is deliberately no publisher-side twin over the core's `MapPublisher` hook. A subscription
//! needs one because its source is written inside `#[subscriber(..)]`, so a mount site has no other
//! way to reach `block(..)`; a publish policy is constructed in the `.out(marker, policy)` call
//! itself, where every option it carries (`RedisPubSubPublish::mode`, `RedisListPublish::ttl`) is
//! already one chain away. A twin would be a second spelling for the same setting, and its
//! envelope-framing method would be shadowed by the chain's own `.codec(..)`, which encodes the
//! reply rather than the frame around it.

use std::time::Duration;

use ruststream::runtime::{Declared, SubscriberBuilder, SubscriberSettings};

#[cfg(doc)]
use crate::list::RedisList;
#[cfg(doc)]
use crate::stream::RedisStream;

/// The Redis subscription options a mount site names, chained after the core's settings steps.
///
/// Import it (or glob a form's prelude) to reach [`block`](Self::block) on a builder over a
/// [`RedisStream`] or a [`RedisList`], before or after the window `.pipeline()` opens and the
/// position `start_at(..)` names. The trait
/// is bound to those sources, so the method does not appear on a builder for another broker or
/// for a form that has no blocking read.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use std::time::Duration;
///
/// use ruststream_fred::stream::prelude::*;
/// # use serde::Deserialize;
/// # #[derive(Deserialize)]
/// # struct Order {
/// #     amount: u64,
/// # }
///
/// #[subscriber(RedisStream::new("orders").group("workers"))]
/// async fn bill(orders: &[Order]) -> HandlerOutcome {
///     let total: u64 = orders.iter().map(|order| order.amount).sum();
///     tracing::info!(orders = orders.len(), total, "billed");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // The size is the core's word, the block is Redis's, and they read in that order.
///             b.include(bill.batch(nonzero!(6)).block(Duration::from_secs(5)));
///         },
///     )
/// }
/// # }
/// ```
pub trait RedisSubscribeExt {
    /// How long one read blocks waiting for entries, overriding what the descriptor named.
    ///
    /// On a stream this is the `XREADGROUP` server-side `BLOCK` (and, in reclaim mode, the poll
    /// interval between empty `XAUTOCLAIM` scans); on a list it is the `BRPOP` / `BLMOVE` timeout.
    /// Both default to five seconds.
    #[must_use]
    fn block(self, block: Duration) -> Self;
}

impl<Def, Source, State, DefCodec> RedisSubscribeExt
    for SubscriberBuilder<Def, Source, State, DefCodec>
where
    Def: Declared,
    Source: blocking::Blocks,
{
    fn block(self, block: Duration) -> Self {
        self.map_source(|source| source.block(block))
    }
}

mod blocking {
    use std::time::Duration;

    use ruststream::StartAt;

    use crate::list::RedisList;
    use crate::pipeline::Pipelined;
    use crate::stream::RedisStream;

    /// A source with a blocking read, which is what [`block`](super::RedisSubscribeExt::block)
    /// reaches into wherever the mount chain has wrapped it.
    pub trait Blocks {
        fn block(self, block: Duration) -> Self;
    }

    impl Blocks for RedisStream {
        fn block(self, block: Duration) -> Self {
            Self::block(self, block)
        }
    }

    impl Blocks for RedisList {
        fn block(self, block: Duration) -> Self {
            Self::block(self, block)
        }
    }

    /// The same option on a source whose window `.pipeline()` has opened, so the base settings
    /// read in either order around it.
    impl<Descriptor: Blocks, Mode> Blocks for Pipelined<Descriptor, Mode> {
        fn block(self, block: Duration) -> Self {
            self.map(|descriptor| descriptor.block(block))
        }
    }

    /// The same option after the core's `start_at(..)`, which wraps the source it positions.
    impl<Source: Blocks, Position> Blocks for StartAt<Source, Position> {
        fn block(self, block: Duration) -> Self {
            self.map_inner(|source| source.block(block))
        }
    }
}
