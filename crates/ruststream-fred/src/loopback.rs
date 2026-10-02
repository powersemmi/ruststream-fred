//! What a connection and its handles hold of the in-process server the test harness connects in
//! place of a real one.
//!
//! Each type here has one field under the `testing` feature and none without it, so a production
//! build carries three zero-sized types and no branch to a second transport: the compile-time
//! assertions below hold the promise. Every call that reaches the in-process server is itself
//! compiled only with the feature.

#[cfg(feature = "testing")]
use std::sync::Arc;
#[cfg(feature = "testing")]
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(feature = "testing")]
use ruststream::testing::Coordinator;

#[cfg(feature = "testing")]
use crate::in_process::{ReaderId, Server};

/// The in-process server a connection speaks to, when the harness connected it in process.
#[derive(Clone, Debug, Default)]
pub(crate) struct Loopback {
    #[cfg(feature = "testing")]
    server: Option<Arc<Server>>,
}

/// One subscription's registration with the in-process server: what its reads wait on and what
/// its deliveries are counted against. It leaves the server when dropped.
#[derive(Debug, Default)]
pub(crate) struct Tap {
    #[cfg(feature = "testing")]
    reader: Option<(Arc<Server>, ReaderId)>,
}

/// One delivery the test harness counts in flight: released when the delivery is dropped, which
/// a settle reaches once its commands have run.
///
/// Public in name only, for the settle side of a window: the module that holds it is private.
#[derive(Debug, Default)]
#[cfg_attr(
    not(feature = "testing"),
    allow(
        unreachable_pub,
        reason = "a window's settle side names it under `testing` only"
    )
)]
pub struct InFlight {
    #[cfg(feature = "testing")]
    coordinator: Option<Coordinator>,
}

// The zero-cost promise of the in-process mode, held by the compiler: a build without it gives
// every hook no size at all.
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Loopback>() == 0);
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<Tap>() == 0);
#[cfg(not(feature = "testing"))]
const _: () = assert!(size_of::<InFlight>() == 0);

impl Tap {
    /// The guard of one delivery this subscription hands out.
    #[cfg_attr(
        not(feature = "testing"),
        allow(
            clippy::unused_self,
            reason = "the registration is read under `testing` only"
        )
    )]
    pub(crate) fn delivered(&self) -> InFlight {
        #[cfg(feature = "testing")]
        {
            InFlight {
                coordinator: self
                    .reader
                    .as_ref()
                    .and_then(|(server, _)| server.coordinator()),
            }
        }
        #[cfg(not(feature = "testing"))]
        {
            InFlight {}
        }
    }
}

#[cfg(feature = "testing")]
impl Loopback {
    /// A connection to `server`.
    pub(crate) const fn to(server: Arc<Server>) -> Self {
        Self {
            server: Some(server),
        }
    }

    /// The in-process server, on a connection the harness made in process.
    pub(crate) const fn server(&self) -> Option<&Arc<Server>> {
        self.server.as_ref()
    }

    /// The wall clock as the connection's server keeps it, in epoch milliseconds: the in-process
    /// server's own clock, which a paused test clock moves, or the system's.
    pub(crate) fn now_ms(&self) -> u64 {
        self.server
            .as_ref()
            .map_or_else(system_ms, |server| server.now_ms())
    }
}

#[cfg(feature = "testing")]
fn system_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(feature = "testing")]
impl Tap {
    /// A registration of `reader` with `server`.
    pub(crate) const fn registered(server: Arc<Server>, reader: ReaderId) -> Self {
        Self {
            reader: Some((server, reader)),
        }
    }

    /// Whether the subscription reads from the in-process server.
    pub(crate) const fn in_process(&self) -> bool {
        self.reader.is_some()
    }

    /// Waits until a read of this subscription would find something, in place of the blocking
    /// read the server would hold open. Returns at once on a real connection.
    pub(crate) async fn readable(&self) {
        if let Some((server, reader)) = &self.reader {
            server.readable(*reader).await;
        }
    }

    /// Releases `n` deliveries the subscription received and will not hand out.
    pub(crate) fn discard(&self, n: usize) {
        if let Some((server, _)) = &self.reader {
            server.released(n);
        }
    }
}

#[cfg(feature = "testing")]
impl Drop for Tap {
    fn drop(&mut self) {
        if let Some((server, reader)) = self.reader.take() {
            server.detach(reader);
        }
    }
}

#[cfg(feature = "testing")]
impl Drop for InFlight {
    fn drop(&mut self) {
        if let Some(coordinator) = self.coordinator.take() {
            coordinator.consumed();
        }
    }
}
