//! Which of Redis's three command families reaches a name this service reads.
//!
//! A stream is written with `XADD`, a list with `LPUSH`, a channel with `PUBLISH`, and a write of
//! the wrong family is either refused (`WRONGTYPE`) or lands where nobody reads it. A subscription
//! records its name here when it opens, and its dead-letter destination with it, so the broker's
//! default publisher, the one a retry copy and a dead-letter move leave through when the mount site
//! names none, writes each name with the family of the subscription it belongs to.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::{Debug, Display, Formatter};
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::envelope::SharedEnvelope;
use crate::error::RedisError;
use crate::pubsub::PubSubMode;

/// How a name is written: the publish of the form that reads it.
#[derive(Clone)]
pub(crate) enum Route {
    /// `XADD`, the family of a name nothing here records.
    Stream,
    /// `LPUSH`, framed the way the list subscription unframes.
    List { envelope: Option<SharedEnvelope> },
    /// `PUBLISH` or `SPUBLISH`, framed the way the channel subscription unframes.
    Channel {
        mode: PubSubMode,
        envelope: Option<SharedEnvelope>,
    },
}

impl Route {
    /// What a name written this way is, for the refusal that names two of them.
    const fn noun(&self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::List { .. } => "list",
            Self::Channel { .. } => "Pub/Sub channel",
        }
    }

    /// Whether a publish written this way reaches a subscription that reads `other`: the same
    /// family, and for a list or a channel the same framing, and for a channel the same mode.
    fn writes_like(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Stream, Self::Stream) => true,
            (Self::List { envelope: a }, Self::List { envelope: b }) => {
                framing(a.as_ref()) == framing(b.as_ref())
            }
            (
                Self::Channel {
                    mode: mode_a,
                    envelope: a,
                },
                Self::Channel {
                    mode: mode_b,
                    envelope: b,
                },
            ) => mode_a == mode_b && framing(a.as_ref()) == framing(b.as_ref()),
            _ => false,
        }
    }
}

/// How a list or channel value is framed: raw bytes, or the envelope codec's type.
fn framing(envelope: Option<&SharedEnvelope>) -> Option<&'static str> {
    envelope.map(|codec| codec.framing())
}

impl Debug for Route {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stream => f.write_str(self.noun()),
            Self::List { envelope } => write!(f, "list framed {}", Framing(envelope)),
            Self::Channel { mode, envelope } => {
                let mode = match mode {
                    PubSubMode::Classic => "classic",
                    PubSubMode::Sharded => "sharded",
                };
                write!(f, "{mode} {} framed {}", self.noun(), Framing(envelope))
            }
        }
    }
}

struct Framing<'a>(&'a Option<SharedEnvelope>);

impl Display for Framing<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match framing(self.0.as_ref()) {
            None => f.write_str("as raw bytes"),
            Some(codec) => write!(f, "by `{codec}`"),
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
    table: RwLock<HashMap<String, Route>>,
    /// Set once a list or a channel is recorded: before that every name is a stream.
    foreign: AtomicBool,
}

impl Routes {
    /// Records a subscription's own name and its dead-letter destination with `route`, when it
    /// opens: both or neither.
    ///
    /// The first subscription to write a name keeps its route: two subscriptions that write one
    /// name the same way share it. What this call added is forgotten again unless the returned
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
            if let Some(held) = routes.get(written)
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
            if let Entry::Vacant(vacant) = routes.entry(written.to_owned()) {
                if !matches!(route, Route::Stream) {
                    self.foreign.store(true, Ordering::Release);
                }
                vacant.insert(route.clone());
                recorded.added.push(written.to_owned());
            }
        }
        Ok(recorded)
    }

    /// Forgets names a subscription that did not open had added.
    fn forget(&self, names: &[String]) {
        // A poisoned table is left as it is: a destructor does not panic.
        if let Ok(mut routes) = self.table.write() {
            for name in names {
                routes.remove(name);
            }
        }
    }

    /// How `name` is written: the recorded family, or `XADD` for a name nothing here reads.
    pub(crate) fn route(&self, name: &str) -> Route {
        // A relaxed load suffices: a route is recorded while its subscription opens, before any
        // delivery of it exists to be answered or retried.
        if !self.foreign.load(Ordering::Relaxed) {
            return Route::Stream;
        }
        self.table
            .read()
            .expect("redis route table poisoned")
            .get(name)
            .cloned()
            .unwrap_or(Route::Stream)
    }
}

/// The names one subscription added to the table while it opens. Dropped, it forgets them;
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

    #[test]
    fn a_name_nothing_reads_is_a_stream() {
        assert!(matches!(Routes::default().route("orders"), Route::Stream));
    }

    #[test]
    fn a_recorded_name_keeps_its_family() {
        let routes = Routes::default();
        routes
            .record_subscription("jobs", None, &Route::List { envelope: None })
            .expect("record")
            .keep();
        routes
            .record_subscription("jobs", None, &Route::List { envelope: None })
            .expect("a second list on the same key shares it")
            .keep();
        assert!(matches!(routes.route("jobs"), Route::List { .. }));
    }

    #[test]
    fn a_name_read_in_two_families_is_refused() {
        let routes = Routes::default();
        routes
            .record_subscription("jobs", None, &Route::List { envelope: None })
            .expect("record")
            .keep();
        let err = routes
            .record_subscription("orders", Some("jobs"), &Route::Stream)
            .err()
            .expect("a list key cannot be a stream too");
        let text = err.to_string();
        assert!(text.contains("`orders`") && text.contains("stream") && text.contains("list"));
        assert!(
            matches!(routes.route("orders"), Route::Stream),
            "a refused subscription records neither of its names"
        );
    }

    #[test]
    fn a_channel_read_in_two_modes_is_refused() {
        let routes = Routes::default();
        let channel = |mode| Route::Channel {
            mode,
            envelope: None,
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
                .record_subscription("jobs", Some("jobs.dead"), &Route::List { envelope: None })
                .expect("record"),
        );
        assert!(matches!(routes.route("jobs"), Route::Stream));
        assert!(matches!(routes.route("jobs.dead"), Route::Stream));
        routes
            .record_subscription("jobs", None, &Route::Stream)
            .expect("the name is free again")
            .keep();
    }
}
