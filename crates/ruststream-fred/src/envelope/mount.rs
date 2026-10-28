//! The mount-site step that names a subscription's envelope on the macro path.

use ruststream::StartAt;
use ruststream::runtime::{Declared, SubscriberBuilder, SubscriberSettings};

use super::Envelope;
use crate::pipeline::Pipelined;

/// Names the envelope a subscription reads through at the mount site, instead of the broker's.
///
/// `#[subscriber(..)]` takes a descriptor's type from its constructor, so a descriptor written
/// there keeps the broker's envelope, and the mount site names another with
/// `b.include(handler.envelope(NoOp))`. The step reaches the descriptor through a window that
/// `.pipeline()` opened and a position `start_at(..)` named, so it reads in any order with them.
/// The manual path names the envelope on the descriptor itself, with its own `envelope(..)`.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::list::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[subscriber(RedisList::new("jobs").reliable())]
/// async fn run_job(job: &Job) -> HandlerOutcome {
///     tracing::info!(job.id, "running");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // A faststream service fills this queue.
///             b.include(run_job.envelope(FastStream));
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
pub trait RedisEnvelopeExt: Sized {
    /// The registration once its subscription reads through `Named`.
    type Enveloped<Named: Envelope>;

    /// Reads the subscription through `envelope` instead of the broker's.
    #[must_use]
    fn envelope<Named: Envelope>(self, envelope: Named) -> Self::Enveloped<Named>;
}

impl<Decl, Def, Source, State, DefCodec> RedisEnvelopeExt for Decl
where
    Decl: Declared<Settings = SubscriberBuilder<Def, Source, State, DefCodec>>,
    Def: Declared,
    Source: enveloping::Enveloping,
{
    type Enveloped<Named: Envelope> =
        SubscriberBuilder<Def, Source::Enveloped<Named>, State, DefCodec>;

    fn envelope<Named: Envelope>(self, envelope: Named) -> Self::Enveloped<Named> {
        self.declare()
            .map_source(|source| source.enveloped(envelope))
    }
}

pub(crate) mod enveloping {
    use super::{Envelope, Pipelined, StartAt};
    use crate::{RedisList, RedisPubSub, RedisPubSubPattern, RedisStream};

    /// A source whose envelope the mount site names, wherever the mount chain has wrapped it.
    #[diagnostic::on_unimplemented(
        message = "`.envelope(..)` names the envelope of a `RedisStream`, `RedisList`, \
                   `RedisPubSub` or `RedisPubSubPattern` subscription, and this one is `{Self}`",
        label = "no envelope of ruststream-fred applies here"
    )]
    pub trait Enveloping {
        type Enveloped<Named: Envelope>;

        fn enveloped<Named: Envelope>(self, envelope: Named) -> Self::Enveloped<Named>;
    }

    macro_rules! descriptor {
        ($descriptor:ident) => {
            impl<Env> Enveloping for $descriptor<Env> {
                type Enveloped<Named: Envelope> = $descriptor<Named>;

                fn enveloped<Named: Envelope>(self, envelope: Named) -> $descriptor<Named> {
                    self.envelope(envelope)
                }
            }
        };
    }

    descriptor!(RedisStream);
    descriptor!(RedisList);
    descriptor!(RedisPubSub);
    descriptor!(RedisPubSubPattern);

    /// The same step on a source whose window `.pipeline()` has opened.
    impl<Descriptor: Enveloping, Mode> Enveloping for Pipelined<Descriptor, Mode> {
        type Enveloped<Named: Envelope> = Pipelined<Descriptor::Enveloped<Named>, Mode>;

        fn enveloped<Named: Envelope>(self, envelope: Named) -> Self::Enveloped<Named> {
            Pipelined::wrap(self.into_descriptor().enveloped(envelope))
        }
    }

    /// The same step after the core's `start_at(..)`, which wraps the source it positions.
    impl<Source: Enveloping, Position> Enveloping for StartAt<Source, Position> {
        type Enveloped<Named: Envelope> = StartAt<Source::Enveloped<Named>, Position>;

        fn enveloped<Named: Envelope>(self, envelope: Named) -> Self::Enveloped<Named> {
            self.map_inner(|source| source.enveloped(envelope))
        }
    }
}
