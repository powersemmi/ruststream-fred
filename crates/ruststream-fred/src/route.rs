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

    const fn same_family(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Stream, Self::Stream)
                | (Self::List { .. }, Self::List { .. })
                | (Self::Channel { .. }, Self::Channel { .. })
        )
    }
}

impl Debug for Route {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.noun())
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
    /// Records that `name` is written the way `route` says, for the subscription `owner`.
    ///
    /// The first subscription to name a family keeps it: two subscriptions of one family on one
    /// name share it.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::InvalidOptions`] when another subscription already reads or
    /// dead-letters to `name` in another family, since one of the two would receive writes it
    /// cannot read.
    pub(crate) fn record(&self, owner: &str, name: &str, route: Route) -> Result<(), RedisError> {
        let mut routes = self.table.write().expect("redis route table poisoned");
        match routes.entry(name.to_owned()) {
            Entry::Vacant(vacant) => {
                if !matches!(route, Route::Stream) {
                    self.foreign.store(true, Ordering::Release);
                }
                vacant.insert(route);
                Ok(())
            }
            Entry::Occupied(held) if held.get().same_family(&route) => Ok(()),
            Entry::Occupied(held) => Err(RedisError::InvalidOptions(format!(
                "the subscription on `{owner}` writes `{name}` as a {}, but this service already \
                 reads or dead-letters to `{name}` as a {}: a name is one Redis type, so give one \
                 of the two another name",
                route.noun(),
                held.get().noun(),
            ))),
        }
    }

    /// Records a subscription's own name and its dead-letter destination with `route`, when it
    /// opens.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::InvalidOptions`] when either name is already written another way.
    pub(crate) fn record_subscription(
        &self,
        name: &str,
        dead_letter: Option<&str>,
        route: &Route,
    ) -> Result<(), RedisError> {
        self.record(name, name, route.clone())?;
        if let Some(dead_letter) = dead_letter {
            self.record(name, dead_letter, route.clone())?;
        }
        Ok(())
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
            .record("jobs", "jobs", Route::List { envelope: None })
            .expect("record");
        routes
            .record("jobs.retry", "jobs", Route::List { envelope: None })
            .expect("a second list on the same key shares it");
        assert!(matches!(routes.route("jobs"), Route::List { .. }));
    }

    #[test]
    fn a_name_read_in_two_families_is_refused() {
        let routes = Routes::default();
        routes
            .record("jobs", "jobs", Route::List { envelope: None })
            .expect("record");
        let err = routes
            .record("orders", "jobs", Route::Stream)
            .expect_err("a list key cannot be a stream too");
        let text = err.to_string();
        assert!(text.contains("`orders`") && text.contains("stream") && text.contains("list"));
    }
}
