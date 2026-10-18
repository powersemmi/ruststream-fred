//! The service of the Replay and seeking a stream guide: a payments service that rebuilds a
//! balance view from the oldest retained entry on every start, and a notifier that skips a flood
//! of alerts.
//!
//! ```text
//! docker run -p 6379:6379 redis:8.4
//! cargo run --example guide_replay --features testing -- run
//! ```

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "ledger")]
struct LedgerEntry {
    seq: u64,
    account: String,
    amount_cents: i64,
}

/// An alert for a customer; an operator appends one with `halt` set to stop a flood.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "alerts")]
struct Alert {
    id: u64,
    halt: bool,
}

// --8<-- [start:rebuild]
/// Rebuilds the balance view from the oldest entry the stream retains, on every start.
#[subscriber(
    RedisStream::new("ledger").group("balances"),
    start_at(RedisGroupPosition::beginning())
)]
async fn rebuild(entry: &LedgerEntry) -> HandlerOutcome {
    tracing::info!(
        entry.seq,
        entry.account,
        entry.amount_cents,
        "balance updated"
    );
    HandlerOutcome::ack()
}
// --8<-- [end:rebuild]

// --8<-- [start:skip]
/// Sends alerts. On a halt marker the group moves to the tail, so the alerts queued behind the
/// marker are never sent, by this consumer or any other of `notifier`.
#[subscriber(RedisStream::new("alerts").group("notifier"))]
async fn notify(alert: &Alert, Ctx(seeker): Ctx<keys::SeekHandle>) -> HandlerOutcome {
    if !alert.halt {
        tracing::info!(alert.id, "alert sent");
        return HandlerOutcome::ack();
    }
    tracing::warn!(
        group = seeker.group(),
        "halt marker, the backlog is skipped"
    );
    match seeker.seek(RedisGroupPosition::end()).await {
        Ok(()) => HandlerOutcome::ack(),
        Err(_) => HandlerOutcome::retry(),
    }
}
// --8<-- [end:skip]

// --8<-- [start:app]
fn service(broker: RedisBroker) -> impl App {
    RustStream::new(AppInfo::new("payments", "0.1.0")).with_broker(broker, |b| {
        b.include(rebuild);
        b.include(notify);
    })
}
// --8<-- [end:app]

#[ruststream::app]
fn app() -> impl App {
    service(RedisBroker::standalone("redis://localhost:6379"))
}

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::time::{SystemTime, UNIX_EPOCH};

    use futures::future::join_all;
    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Alert, LedgerEntry, app, live, service};

    fn entry(seq: u64) -> LedgerEntry {
        LedgerEntry {
            seq,
            account: "acme".to_owned(),
            amount_cents: 1250,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_view_reads_every_entry() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&entry(1))
            .publish()
            .await?;
        tb.broker::<RedisBroker>()
            .message(&entry(2))
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("ledger")
            .assert_called(2)
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_halt_marker_skips_the_alerts_behind_it() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        let broker = tb.broker::<RedisBroker>();

        // The marker and the two alerts behind it wait in the stream together.
        let flood: Vec<Alert> = [(1, false), (0, true), (2, false), (3, false)]
            .into_iter()
            .map(|(id, halt)| Alert { id, halt })
            .collect();
        for published in join_all(flood.iter().map(|alert| broker.message(alert).publish())).await {
            published?;
        }
        broker
            .message(&Alert { id: 4, halt: false })
            .publish()
            .await?;

        let handled: Vec<u64> = broker
            .subscriber("alerts")
            .received::<Alert>()
            .iter()
            .map(|alert| alert.id)
            .collect();
        assert_eq!(handled, [1, 0, 4]);
        tb.shutdown().await?;
        Ok(())
    }

    /// The second start rewinds the group, so its first delivery is an entry from before the
    /// restart, not the one published after it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn every_start_rebuilds_from_the_oldest_entry() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("REDIS_TEST_URL") else {
            return Ok(());
        };
        let seq = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos())?;

        let first = TestApp::start_live(service(RedisBroker::standalone(url.as_str()))).await?;
        first
            .broker::<RedisBroker>()
            .message(&entry(seq))
            .publish()
            .await?;
        first.shutdown().await?;

        let tb = TestApp::start_live(service(RedisBroker::standalone(url.as_str()))).await?;
        tb.broker::<RedisBroker>()
            .message(&entry(seq + 1))
            .publish()
            .await?;

        let rebuilt = tb
            .broker::<RedisBroker>()
            .subscriber("ledger")
            .received::<LedgerEntry>();
        assert_ne!(rebuilt.first().map(|entry| entry.seq), Some(seq + 1));
        tb.shutdown().await?;
        Ok(())
    }
}
