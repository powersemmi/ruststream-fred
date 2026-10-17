//! Redis Pub/Sub subscriptions: one channel, every channel a glob matches, and the buffer a
//! subscription holds while its handler is busy.
//!
//! ```text
//! cargo test --example redis_pubsub --features testing
//! cargo run --example redis_pubsub --features testing -- run
//! ```

use ruststream_fred::pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
struct Tick {
    symbol: String,
    cents: i64,
}

// --8<-- [start:channel]
/// Every price published on `prices` while the service is subscribed.
#[subscriber(RedisPubSub::new("prices"))]
async fn on_price(tick: &Tick) -> HandlerOutcome {
    tracing::info!(symbol = %tick.symbol, tick.cents, "price");
    HandlerOutcome::ack()
}
// --8<-- [end:channel]

// --8<-- [start:pattern]
/// Every channel whose name matches `quotes.*`: `quotes.eu`, `quotes.us` and any added later.
#[subscriber(RedisPubSubPattern::new("quotes.*"))]
async fn on_quote(tick: &Tick) -> HandlerOutcome {
    tracing::info!(symbol = %tick.symbol, tick.cents, "quote");
    HandlerOutcome::ack()
}
// --8<-- [end:pattern]

// --8<-- [start:buffer]
/// A burst of trades waits in a buffer of 1024 messages while the handler works through it.
#[subscriber(RedisPubSub::new("trades").buffer(nonzero!(1024)))]
async fn on_trade(tick: &Tick) -> HandlerOutcome {
    tracing::info!(symbol = %tick.symbol, tick.cents, "trade");
    HandlerOutcome::ack()
}
// --8<-- [end:buffer]

// --8<-- [start:app]
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("market", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(on_price);
            // A glob is not a channel a `PUBLISH` can name, so the registration names the one
            // a retry copy goes to. The pattern matches it, so the copy comes back here.
            b.include(on_quote)
                .out_retry(Publish::default())
                .to("quotes.retry");
            b.include(on_trade);
        },
    )
}
// --8<-- [end:app]

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::pubsub::prelude::*;

    use super::{Tick, app};

    fn tick(symbol: &str, cents: i64) -> Tick {
        Tick {
            symbol: symbol.to_owned(),
            cents,
        }
    }

    #[tokio::test]
    async fn a_channel_subscription_reads_its_channel() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.broker::<RedisBroker>()
            .message(&tick("ACME", 1_250))
            .to("prices")
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("prices")
            .assert_called_once()
            .with(&tick("ACME", 1_250));
        Ok(())
    }

    #[tokio::test]
    async fn a_pattern_reads_every_channel_it_matches() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for channel in ["quotes.eu", "quotes.us"] {
            tb.broker::<RedisBroker>()
                .message(&tick("ACME", 1_300))
                .to(channel)
                .publish()
                .await?;
        }

        tb.broker::<RedisBroker>()
            .subscriber("quotes.*")
            .assert_called(2)
            .with(&tick("ACME", 1_300));
        Ok(())
    }

    #[tokio::test]
    async fn a_buffered_subscription_reads_a_burst() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        for cents in [100, 101, 102] {
            tb.broker::<RedisBroker>()
                .message(&tick("ACME", cents))
                .to("trades")
                .publish()
                .await?;
        }

        let redis = tb.broker::<RedisBroker>();
        let trades = redis.subscriber("trades");
        assert_eq!(
            trades.received::<Tick>(),
            [tick("ACME", 100), tick("ACME", 101), tick("ACME", 102)]
        );
        Ok(())
    }
}
