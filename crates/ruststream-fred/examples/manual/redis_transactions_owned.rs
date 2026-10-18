//! The service of `examples/redis_transactions_owned.rs` written without `#[subscriber]`: the
//! seeding hook is the same; the reader is a manual subscriber.
//!
//! ```text
//! cargo test --example manual_redis_transactions_owned --features testing
//! cargo run --example manual_redis_transactions_owned -- run
//! ```

use std::error::Error;
use std::future::{Future, ready};
use std::io;
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream::schemars::JsonSchema;
use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
#[outgoing(name = "prices")]
struct Price {
    sku: String,
    cents: u64,
}

struct ListPrice;

impl Handle<Price> for ListPrice {
    fn handle(
        &self,
        price: &Price,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(price.sku, price.cents, "price listed");
        ready(Ok(()))
    }
}

// --8<-- [start:owned_transaction]
/// Writes the opening prices as one block: readers of `prices` see the whole list or none of it.
async fn seed_prices<P: OwnedTransactions>(
    publisher: P,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut seed = publisher.owned_transaction().await?;
    for (sku, cents) in [("tea", 450), ("cake", 600)] {
        let price = Price {
            sku: sku.to_owned(),
            cents,
        };
        seed.publish("prices", &price).await?;
    }
    seed.commit().await?;
    Ok(())
}

/// The service on `broker`: a standalone or a sentinel deployment, where `MULTI` / `EXEC` runs.
fn app_on(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("catalog", "0.1.0")).with_broker(broker, |b| {
        let list_price = subscriber(RedisStream::new("prices").group("catalog"), ListPrice);
        b.include(list_price.build());
        // Runs once the broker is connected and the subscription is open.
        b.after_startup(TransactionalPublish, async move |publisher| {
            seed_prices(publisher).await.map_err(io::Error::other)
        });
    })
}
// --8<-- [end:owned_transaction]

fn app() -> impl App<State = ()> {
    app_on(RedisBroker::standalone("redis://localhost:6379"))
}

fn main() -> ExitCode {
    run_main(app)
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Price, app, app_on};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_opening_prices_arrive_together() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;
        tb.settle().await?;

        let skus: Vec<String> = tb
            .broker::<RedisBroker>()
            .subscriber("prices")
            .assert_called(2)
            .received::<Price>()
            .into_iter()
            .map(|price| price.sku)
            .collect();
        assert_eq!(skus, ["tea", "cake"]);
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cluster_refuses_the_transaction_at_startup() {
        let cluster = RedisBroker::cluster(["redis://localhost:7000"]);
        let Err(refused) = TestApp::start(app_on(cluster)).await else {
            panic!("a cluster opened a MULTI / EXEC block");
        };
        assert!(
            format!("{refused:?}").contains("only supported on standalone and sentinel"),
            "{refused:?}",
        );
    }
}
