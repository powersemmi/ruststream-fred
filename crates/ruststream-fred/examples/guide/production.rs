//! The service of the Running in production guide: an orders service that reads its Redis
//! address and ACL credentials from the environment, on a standalone server or a sentinel set.
//!
//! ```text
//! REDIS_URL=redis://redis.internal:6379 REDIS_USERNAME=orders-svc REDIS_PASSWORD=... \
//!     cargo run --example guide_production --features testing -- run
//! ```

// The sentinel broker is the alternative a deployment picks; its test uses it.
#![cfg_attr(not(test), allow(dead_code))]

use std::env;

use ruststream_fred::stream::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:broker]
/// Where Redis is and who the service is, as the deployment provides them.
struct Deployment {
    url: String,
    username: String,
    password: String,
}

impl Deployment {
    fn from_env() -> Self {
        let read = |name: &str| env::var(name).unwrap_or_default();
        Self {
            url: read("REDIS_URL"),
            username: read("REDIS_USERNAME"),
            password: read("REDIS_PASSWORD"),
        }
    }
}

fn broker(deployment: Deployment) -> RedisBroker {
    RedisBroker::standalone(deployment.url)
        .credentials(deployment.username, deployment.password)
        .pool(8)
}
// --8<-- [end:broker]

// --8<-- [start:sentinel]
/// The primary `orders` as two sentinels report it, with the ACL user the deployment provides.
fn sentinel_broker(username: String, password: String) -> RedisBroker {
    RedisBroker::sentinel(
        "orders",
        ["sentinel-1.internal:26379", "sentinel-2.internal:26379"],
    )
    .credentials(username, password)
    .pool(8)
}
// --8<-- [end:sentinel]

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "confirmations")]
struct Confirmation {
    order_id: u64,
}

// --8<-- [start:service]
#[subscriber(RedisStream::new("orders").group("orders-svc"), reply)]
async fn confirm(order: &Order) -> Confirmation {
    tracing::info!(order.id, "order confirmed");
    Confirmation { order_id: order.id }
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(confirm).out_reply(Publish);
    })
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(broker(Deployment::from_env()))
}
// --8<-- [end:service]

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Confirmation, Deployment, Order, broker, live, sentinel_broker, service};

    fn deployment(url: &str, password: &str) -> Deployment {
        Deployment {
            url: url.to_owned(),
            username: "worker".to_owned(),
            password: password.to_owned(),
        }
    }

    /// The body every mode runs.
    async fn confirms_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<RedisBroker>()
            .message(&Order { id: 1 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .published::<Confirmation>("confirmations")
            .assert_called_once()
            .with(&Confirmation { order_id: 1 });
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_production_configuration_runs_in_process() -> Result<(), Box<dyn Error>> {
        let broker = broker(deployment("redis://redis.internal:6379", "s3cret-pa55"));
        confirms_an_order(TestApp::start(service(broker)).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_sentinel_configuration_runs_in_process() -> Result<(), Box<dyn Error>> {
        let broker = sentinel_broker("worker".to_owned(), "s3cret-pa55".to_owned());
        confirms_an_order(TestApp::start(service(broker)).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_acl_user_authenticates() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("REDIS_AUTH_TEST_URL") else {
            return Ok(());
        };
        let broker = broker(deployment(&url, "workerpass"));
        confirms_an_order(TestApp::start_live(service(broker)).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_wrong_password_does_not_start() {
        let Some(url) = live::url("REDIS_AUTH_TEST_URL") else {
            return;
        };
        let broker = broker(deployment(&url, "not-the-password"));

        assert!(TestApp::start_live(service(broker)).await.is_err());
    }
}
