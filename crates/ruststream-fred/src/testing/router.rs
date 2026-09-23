//! Subscription registry and fanout for the in-memory Redis simulator.
//!
//! [`KeyRouter`] keeps the set of live subscriptions keyed by [`SubscriptionId`]. Every
//! [`KeyRouter::publish`] copies the delivery to every subscription of the publish's own form whose
//! name matches exactly (Redis has no wildcard keys) and appends a snapshot to a per-name log so
//! test code can assert on observed traffic via [`KeyRouter::published`].
//!
//! The router keeps Redis's namespaces apart as a server does. A stream and a list are keys of a
//! type, fixed by the first write or by the subscription reading them, and a write of the other
//! type is refused the way `WRONGTYPE` refuses it. A channel is not a key: a stream or a list
//! written under the name a channel subscription reads, or a `PUBLISH` to a name only a stream or
//! a list reads, is refused rather than written where nobody reads it.
//!
//! Subscriptions are removed explicitly through [`KeyRouter::unsubscribe`]; the test subscriber
//! wrapper calls this from its `Drop` impl so dropping a subscriber stops fanout.

use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use ruststream::{HeaderMap, RawMessage, testing::Coordinator};
use tokio::sync::mpsc;

use crate::route::Route;

/// The namespace a name is read or written in: the command family of one of this crate's forms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Form {
    /// A stream key: `XADD` writes it, a consumer group reads it.
    Stream,
    /// A list key: `LPUSH` writes it, a pop reads it.
    List,
    /// A Pub/Sub channel: `PUBLISH` reaches its subscribers, and nothing is stored.
    Channel,
}

impl Form {
    /// The family a recorded route writes in.
    pub(crate) const fn of(route: &Route) -> Self {
        match route {
            Route::Stream => Self::Stream,
            Route::List { .. } => Self::List,
            Route::Channel { .. } => Self::Channel,
        }
    }

    /// The command a publish of this form issues on a server.
    const fn command(self) -> &'static str {
        match self {
            Self::Stream => "XADD",
            Self::List => "LPUSH",
            Self::Channel => "PUBLISH",
        }
    }

    /// What a name read or written in this form is.
    const fn noun(self) -> &'static str {
        match self {
            Self::Stream => "stream",
            Self::List => "list",
            Self::Channel => "Pub/Sub channel",
        }
    }
}

/// Opaque handle identifying one subscription inside a [`KeyRouter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SubscriptionId(u64);

/// Single delivery handed to a matching subscriber.
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub(crate) subject: String,
    pub(crate) payload: Bytes,
    pub(crate) headers: HeaderMap,
}

pub(crate) type DeliverySender = mpsc::UnboundedSender<Delivery>;
pub(crate) type DeliveryReceiver = mpsc::UnboundedReceiver<Delivery>;

struct Subscription {
    key: String,
    form: Form,
    sender: DeliverySender,
}

#[derive(Default)]
struct RouterState {
    subscriptions: HashMap<SubscriptionId, Subscription>,
    log: HashMap<String, Vec<RawMessage>>,
    /// The type each written key took on its first write, as a server keeps it.
    keys: HashMap<String, Form>,
}

impl RouterState {
    /// Refuses a publish of `form` to `name` that a server would refuse, or would accept and leave
    /// where no subscription of this broker reads it.
    fn admit(&self, name: &str, form: Form) -> Result<(), String> {
        let written = if form == Form::Channel {
            None
        } else {
            self.keys.get(name).copied()
        };
        let mut read_as = self
            .subscriptions
            .values()
            .filter(|sub| sub.key == name)
            .map(|sub| sub.form);
        let other = read_as.clone().find(|read| *read != form);
        let same = read_as.any(|read| read == form);
        let command = form.command();
        if let Some(held) = written.filter(|held| *held != form) {
            return Err(wrong_type(command, name, held));
        }
        match other {
            // A key write against a key another subscription reads as the other key type.
            Some(held) if form != Form::Channel && held != Form::Channel => {
                Err(wrong_type(command, name, held))
            }
            Some(read) if !same => Err(format!(
                "{command} to `{name}` reaches nobody: this broker reads `{name}` as a {}, and a \
                 {} write is not delivered there",
                read.noun(),
                form.noun(),
            )),
            _ => Ok(()),
        }
    }
}

/// The refusal a server answers a command against a key of the other type with.
fn wrong_type(command: &str, name: &str, held: Form) -> String {
    format!(
        "WRONGTYPE {command} to `{name}`, which holds a {}: Redis refuses an operation against a \
         key holding the wrong kind of value",
        held.noun(),
    )
}

/// In-memory stream-key router with exact-match semantics.
#[derive(Default)]
pub(crate) struct KeyRouter {
    state: Mutex<RouterState>,
    next_id: AtomicU64,
}

impl KeyRouter {
    /// Registers a subscription against `key` and returns the channel pair the subscriber will use,
    /// together with the [`SubscriptionId`] needed to unsubscribe.
    ///
    /// The returned [`DeliverySender`] is the same one fanout uses, so subscribers can re-send a
    /// delivery into their own queue to implement `nack(requeue = true)`.
    pub(crate) fn subscribe(
        &self,
        key: String,
        form: Form,
    ) -> (SubscriptionId, DeliverySender, DeliveryReceiver) {
        let (tx, rx) = mpsc::unbounded_channel();
        let id = SubscriptionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        self.state
            .lock()
            .expect("redis test router mutex poisoned")
            .subscriptions
            .insert(
                id,
                Subscription {
                    key,
                    form,
                    sender: tx.clone(),
                },
            );
        (id, tx, rx)
    }

    /// Removes a subscription. No-op if the id is unknown (e.g. double-drop of the subscriber).
    pub(crate) fn unsubscribe(&self, id: SubscriptionId) {
        self.state
            .lock()
            .expect("redis test router mutex poisoned")
            .subscriptions
            .remove(&id);
    }

    /// Fans out `delivery` to every subscription of `form` reading `subject`, and records it in the
    /// published log.
    ///
    /// `form` is `None` for a message the harness injects as an external producer would, which
    /// reaches every subscription on the name whatever it reads the name as.
    ///
    /// # Errors
    ///
    /// Returns the refusal, as the text a server's error would carry, when a server would refuse
    /// the write or leave it where no subscription of this broker reads it; nothing is delivered
    /// or logged then.
    ///
    /// When a harness [`Coordinator`] is installed, every live enqueue into a subscriber channel is
    /// counted with [`Coordinator::enqueued`], so the harness can drive the in-process reaction to
    /// quiescence; the matching [`Coordinator::consumed`] fires when the delivery is settled (see the
    /// `Drop` impl on [`RedisTestMessage`](super::RedisTestMessage)).
    pub(crate) fn publish(
        &self,
        subject: String,
        payload: Bytes,
        headers: HeaderMap,
        coordinator: Option<&Coordinator>,
        form: Option<Form>,
    ) -> Result<(), String> {
        let mut to_notify: Vec<DeliverySender> = Vec::new();
        {
            let mut state = self.state.lock().expect("redis test router mutex poisoned");
            if let Some(form) = form {
                state.admit(&subject, form)?;
                if form != Form::Channel {
                    state.keys.insert(subject.clone(), form);
                }
            }
            let snapshot =
                RawMessage::new(subject.clone(), payload.clone()).with_headers(headers.clone());
            state.log.entry(subject.clone()).or_default().push(snapshot);
            for sub in state.subscriptions.values() {
                if sub.key == subject && form.is_none_or(|form| form == sub.form) {
                    to_notify.push(sub.sender.clone());
                }
            }
            drop(state);
        }

        let delivery = Delivery {
            subject,
            payload,
            headers,
        };
        for tx in to_notify {
            // Count every live enqueue so the harness can drive to quiescence; the redelivered copy
            // is consumed (and decremented) in turn.
            if tx.send(delivery.clone()).is_ok()
                && let Some(coordinator) = coordinator
            {
                coordinator.enqueued();
            }
        }
        Ok(())
    }

    /// Returns every message published to `subject`, in publish order. Backs the
    /// [`TestableBroker::published`](ruststream::testing::TestableBroker::published) view and, through
    /// it, the free [`expect_published`](ruststream::testing::expect_published) helper.
    pub(crate) fn published(&self, subject: &str) -> Vec<RawMessage> {
        self.state
            .lock()
            .expect("redis test router mutex poisoned")
            .log
            .get(subject)
            .cloned()
            .unwrap_or_default()
    }

    /// Drops every subscription and clears the published log. Used by broker shutdown.
    pub(crate) fn clear(&self) {
        let mut state = self.state.lock().expect("redis test router mutex poisoned");
        state.subscriptions.clear();
        state.log.clear();
        state.keys.clear();
    }
}

impl std::fmt::Debug for KeyRouter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock().expect("redis test router mutex poisoned");
        f.debug_struct("KeyRouter")
            .field("subscriptions", &state.subscriptions.len())
            .field("logged_keys", &state.log.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_headers() -> HeaderMap {
        HeaderMap::new()
    }

    #[tokio::test]
    async fn exact_key_delivers_to_matching_subscription_only() {
        let router = KeyRouter::default();
        let (_id_a, _tx_a, mut rx_a) = router.subscribe("orders".to_owned(), Form::Stream);
        let (_id_b, _tx_b, mut rx_b) = router.subscribe("events".to_owned(), Form::Stream);

        router
            .publish(
                "orders".into(),
                Bytes::from_static(b"o1"),
                no_headers(),
                None,
                Some(Form::Stream),
            )
            .expect("publish");

        let got = rx_a.recv().await.expect("delivered");
        assert_eq!(got.payload.as_ref(), b"o1");
        assert!(
            rx_b.try_recv().is_err(),
            "events subscription should be untouched"
        );
    }

    #[tokio::test]
    async fn unsubscribe_stops_delivery() {
        let router = KeyRouter::default();
        let (id, _tx, mut rx) = router.subscribe("orders".to_owned(), Form::Stream);
        router.unsubscribe(id);

        router
            .publish(
                "orders".into(),
                Bytes::from_static(b"x"),
                no_headers(),
                None,
                Some(Form::Stream),
            )
            .expect("publish");

        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn published_log_records_in_publish_order() {
        let router = KeyRouter::default();
        router
            .publish(
                "events".into(),
                Bytes::from_static(b"a"),
                no_headers(),
                None,
                Some(Form::Stream),
            )
            .expect("publish");
        router
            .publish(
                "events".into(),
                Bytes::from_static(b"b"),
                no_headers(),
                None,
                Some(Form::Stream),
            )
            .expect("publish");
        let messages = router.published("events");
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].payload(), b"a");
        assert_eq!(messages[1].payload(), b"b");
        assert!(router.published("absent").is_empty());
    }
}
