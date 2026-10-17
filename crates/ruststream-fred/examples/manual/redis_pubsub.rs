//! The service of `examples/redis_pubsub.rs` written without the macros: each handler implements
//! `Handle`, and `subscriber(descriptor, handler)` mounts it on its channel or pattern.
//!
//! ```text
//! cargo test --example manual_redis_pubsub --features testing
//! cargo run --example manual_redis_pubsub --features testing -- run
//! ```

use std::future::{Future, ready};
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::{self, JsonSchema};
use ruststream_fred::pubsub::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
struct Tick {
    symbol: String,
    cents: i64,
}

// --8<-- [start:channel]
/// Every price published on `prices` while the service is subscribed.
struct OnPrice;

impl Handle<Tick> for OnPrice {
    fn handle(
        &self,
        tick: &Tick,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(symbol = %tick.symbol, tick.cents, "price");
        ready(Ok(()))
    }
}

fn prices() -> RedisPubSub {
    RedisPubSub::new("prices")
}
// --8<-- [end:channel]

// --8<-- [start:pattern]
/// Every channel whose name matches `quotes.*`: `quotes.eu`, `quotes.us` and any added later.
struct OnQuote;

impl Handle<Tick> for OnQuote {
    fn handle(
        &self,
        tick: &Tick,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(symbol = %tick.symbol, tick.cents, "quote");
        ready(Ok(()))
    }
}

fn quotes() -> RedisPubSubPattern {
    RedisPubSubPattern::new("quotes.*")
}
// --8<-- [end:pattern]

// --8<-- [start:buffer]
/// A burst of trades waits in a buffer of 1024 messages while the handler works through it.
struct OnTrade;

impl Handle<Tick> for OnTrade {
    fn handle(
        &self,
        tick: &Tick,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(symbol = %tick.symbol, tick.cents, "trade");
        ready(Ok(()))
    }
}

fn trades() -> RedisPubSub {
    RedisPubSub::new("trades").buffer(nonzero!(1024))
}
// --8<-- [end:buffer]

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("market", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include(subscriber(prices(), OnPrice).build());
            // A glob is not a channel a `PUBLISH` can name, so the registration names the one
            // a retry copy goes to. The pattern matches it, so the copy comes back here.
            b.include(subscriber(quotes(), OnQuote).build())
                .out_retry(Publish::default())
                .to("quotes.retry");
            b.include(subscriber(trades(), OnTrade).build());
        },
    )
}
// --8<-- [end:app]

fn main() -> ExitCode {
    run_main(app)
}

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
