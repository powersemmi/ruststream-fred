//! Which of Redis's three command families reaches a name this service reads.
//!
//! A stream is written with `XADD`, a list with `LPUSH`, a channel with `PUBLISH`, and a write of
//! the wrong family is either refused (`WRONGTYPE`) or lands where nobody reads it. A subscription
//! records its name here when it opens, and its dead-letter destination with it, so the broker's
//! default publisher, the one a retry copy and a dead-letter move leave through when the mount site
//! names none, writes each name with the family of the subscription it belongs to.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::{Debug, Formatter};
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::envelope::Framing;
use crate::error::RedisError;
use crate::pubsub::PubSubMode;

/// How a name is written: the publish of the form that reads it, in the envelope it reads.
#[derive(Clone)]
pub(crate) enum Route {
    /// `XADD`, the family of a name nothing here records.
    Stream { framing: Framing },
    /// `LPUSH`, framed the way the list subscription reads.
    List { framing: Framing },
    /// `PUBLISH` or `SPUBLISH`, framed the way the channel subscription reads.
    Channel { mode: PubSubMode, framing: Framing },
}

impl Route {
    /// A stream in the broker's own envelope.
    pub(crate) const STREAM: Self = Self::Stream {
        framing: Framing::Own,
    };

    /// What a name written this way is, for the refusal that names two of them.
    const fn noun(&self) -> &'static str {
        match self {
            Self::Stream { .. } => "stream",
            Self::List { .. } => "list",
            Self::Channel { .. } => "Pub/Sub channel",
        }
    }

    /// Whether a publish written this way reaches a subscription that reads `other`: the same
    /// family and the same envelope, and for a channel the same mode.
    fn writes_like(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Stream { framing: a }, Self::Stream { framing: b })
            | (Self::List { framing: a }, Self::List { framing: b }) => a == b,
            (
                Self::Channel {
                    mode: mode_a,
                    framing: a,
                },
                Self::Channel {
                    mode: mode_b,
                    framing: b,
                },
            ) => mode_a == mode_b && a == b,
            _ => false,
        }
    }

    /// Whether the default publisher has to read the table to write this name: every route but a
    /// stream in the broker's own envelope.
    const fn foreign(&self) -> bool {
        !matches!(
            self,
            Self::Stream {
                framing: Framing::Own
            }
        )
    }
}

impl Debug for Route {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stream { framing } | Self::List { framing } => {
                write!(f, "{} framed by `{framing:?}`", self.noun())
            }
            Self::Channel { mode, framing } => {
                let mode = match mode {
                    PubSubMode::Classic => "classic",
                    PubSubMode::Sharded => "sharded",
                };
                write!(f, "{mode} {} framed by `{framing:?}`", self.noun())
            }
        }
    }
}

/// The names this connection's subscriptions read or dead-letter to, each with its family.
///
/// Written when a subscription opens and read on every publish through the default policy. A
/// service that reads nothing but streams records no list or channel, and its publishes skip the
/// table on one relaxed load.
#[derive(Debug, Default)]
pub(crate) struct Routes {
    table: RwLock<HashMap<String, Held>>,
    /// Set once a list or a channel is recorded: before that every name is a stream.
    foreign: AtomicBool,
}

/// A recorded route and the subscriptions that wrote it, opened or still opening: a name goes
/// only when the last of them failed to open.
#[derive(Debug)]
struct Held {
    route: Route,
    holders: usize,
}

impl Routes {
    /// Records a subscription's own name and its dead-letter destination with `route`, when it
    /// opens: both or neither.
    ///
    /// The first subscription to write a name keeps its route: two subscriptions that write one
    /// name the same way share it. What this call recorded is let go of again unless the returned
    /// [`Recorded`] is kept, so a subscription that fails to open leaves nothing behind.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::InvalidOptions`] when another subscription already reads or
    /// dead-letters to either name another way (another Redis type, framing or Pub/Sub mode),
    /// since one of the two would receive writes it cannot read.
    pub(crate) fn record_subscription(
        &self,
        name: &str,
        dead_letter: Option<&str>,
        route: &Route,
    ) -> Result<Recorded<'_>, RedisError> {
        let mut routes = self.table.write().expect("redis route table poisoned");
        let names = std::iter::once(name).chain(dead_letter);
        for written in names.clone() {
            if let Some(Held { route: held, .. }) = routes.get(written)
                && !held.writes_like(route)
            {
                return Err(RedisError::InvalidOptions(format!(
                    "the subscription on `{name}` writes `{written}` as a {route:?}, but this \
                     service already reads or dead-letters to `{written}` as a {held:?}: one of \
                     the two would receive writes it cannot read, so give one of them another \
                     name or the same settings",
                )));
            }
        }
        let mut recorded = Recorded {
            routes: self,
            added: Vec::new(),
        };
        for written in names {
            match routes.entry(written.to_owned()) {
                Entry::Vacant(vacant) => {
                    if route.foreign() {
                        self.foreign.store(true, Ordering::Release);
                    }
                    vacant.insert(Held {
                        route: route.clone(),
                        holders: 1,
                    });
                }
                Entry::Occupied(mut occupied) => occupied.get_mut().holders += 1,
            }
            recorded.added.push(written.to_owned());
        }
        drop(routes);
        Ok(recorded)
    }

    /// Lets go of the names a subscription that did not open wrote: a name another subscription
    /// shares, opened or still opening, stays.
    fn forget(&self, names: &[String]) {
        // A poisoned table is left as it is: a destructor does not panic.
        if let Ok(mut routes) = self.table.write() {
            for name in names {
                if let Entry::Occupied(mut held) = routes.entry(name.clone()) {
                    held.get_mut().holders -= 1;
                    if held.get().holders == 0 {
                        held.remove();
                    }
                }
            }
        }
    }

    /// How `name` is written: the recorded family, or `XADD` for a name nothing here reads.
    pub(crate) fn route(&self, name: &str) -> Route {
        // A relaxed load suffices: a route is recorded while its subscription opens, before any
        // delivery of it exists to be answered or retried.
        if !self.foreign.load(Ordering::Relaxed) {
            return Route::STREAM;
        }
        self.table
            .read()
            .expect("redis route table poisoned")
            .get(name)
            .map_or(Route::STREAM, |held| held.route.clone())
    }
}

/// The names one subscription wrote to the table while it opens. Dropped, it lets go of them;
/// [`keep`](Self::keep) once the subscription has opened.
#[must_use = "the routes are forgotten again unless kept"]
pub(crate) struct Recorded<'a> {
    routes: &'a Routes,
    added: Vec<String>,
}

impl Recorded<'_> {
    /// The subscription opened: its routes stay.
    pub(crate) fn keep(mut self) {
        self.added.clear();
    }
}

impl Drop for Recorded<'_> {
    fn drop(&mut self) {
        if !self.added.is_empty() {
            self.routes.forget(&self.added);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIST: Route = Route::List {
        framing: Framing::RustStream,
    };

    #[test]
    fn a_name_nothing_reads_is_a_stream() {
        assert!(matches!(
            Routes::default().route("orders"),
            Route::Stream {
                framing: Framing::Own
            }
        ));
    }

    #[test]
    fn a_failed_open_keeps_a_route_another_subscription_shares() {
        let routes = Routes::default();
        let channel = Route::Channel {
            mode: PubSubMode::Classic,
            framing: Framing::RustStream,
        };
        let failing = routes
            .record_subscription("events", Some("events.dead"), &channel)
            .expect("record");
        routes
            .record_subscription("events", Some("events.dead"), &channel)
            .expect("a second subscription shares the channel")
            .keep();
        drop(failing);
        assert!(matches!(routes.route("events"), Route::Channel { .. }));
        assert!(matches!(routes.route("events.dead"), Route::Channel { .. }));
    }

    #[test]
    fn a_recorded_name_keeps_its_family() {
        let routes = Routes::default();
        routes
            .record_subscription("jobs", None, &LIST)
            .expect("record")
            .keep();
        routes
            .record_subscription("jobs", None, &LIST)
            .expect("a second list on the same key shares it")
            .keep();
        assert!(matches!(routes.route("jobs"), Route::List { .. }));
    }

    #[test]
    fn a_name_read_in_two_families_is_refused() {
        let routes = Routes::default();
        routes
            .record_subscription("jobs", None, &LIST)
            .expect("record")
            .keep();
        let err = routes
            .record_subscription("orders", Some("jobs"), &Route::STREAM)
            .err()
            .expect("a list key cannot be a stream too");
        let text = err.to_string();
        assert!(text.contains("`orders`") && text.contains("stream") && text.contains("list"));
        assert!(
            matches!(
                routes.route("orders"),
                Route::Stream {
                    framing: Framing::Own
                }
            ),
            "a refused subscription records neither of its names"
        );
    }

    #[test]
    fn a_channel_read_in_two_modes_is_refused() {
        let routes = Routes::default();
        let channel = |mode| Route::Channel {
            mode,
            framing: Framing::RustStream,
        };
        routes
            .record_subscription("events", None, &channel(PubSubMode::Classic))
            .expect("record")
            .keep();
        let err = routes
            .record_subscription("events", None, &channel(PubSubMode::Sharded))
            .err()
            .expect("a sharded subscription never hears a classic publish");
        assert!(err.to_string().contains("sharded"), "got {err}");
    }

    #[test]
    fn a_subscription_that_does_not_open_forgets_its_routes() {
        let routes = Routes::default();
        drop(
            routes
                .record_subscription("jobs", Some("jobs.dead"), &LIST)
                .expect("record"),
        );
        assert!(matches!(
            routes.route("jobs"),
            Route::Stream {
                framing: Framing::Own
            }
        ));
        assert!(matches!(
            routes.route("jobs.dead"),
            Route::Stream {
                framing: Framing::Own
            }
        ));
        routes
            .record_subscription("jobs", None, &Route::STREAM)
            .expect("the name is free again")
            .keep();
    }
}
