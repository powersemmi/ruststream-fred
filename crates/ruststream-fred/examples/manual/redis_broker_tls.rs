//! The service of `examples/redis_broker_tls.rs` without the macros: the handler is a named type.
//!
//! ```text
//! cargo test --example manual_redis_broker_tls --features testing,tls-rustls
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::env;
use std::error::Error;
use std::future::{Future, ready};

use ruststream::schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use ruststream_fred::stream::prelude::*;

#[derive(Debug, PartialEq, Deserialize, Serialize, JsonSchema)]
#[schemars(crate = "ruststream::schemars")]
struct Order {
    id: u64,
}

// `#[outgoing(name = "orders")]` by hand: an order names the stream it is added to.
impl OutgoingDestination for Order {
    type Form = FixedName;
    const DESTINATION: &'static str = "orders";
}

impl MessageHeaders for Order {
    type Contract = NoHeaders;
}

struct Take;

impl Handle<Order> for Take {
    fn handle(
        &self,
        order: &Order,
        _outs: &(),
        _ctx: &mut Context<'_>,
    ) -> impl Future<Output = Result<(), HandlerOutcome>> {
        tracing::info!(order.id, "order taken");
        ready(Ok(()))
    }
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(subscriber(RedisStream::new("orders").group("workers"), Take).build());
    })
}

// --8<-- [start:broker]
/// The cluster over TLS, logged in as the ACL user `worker`.
fn broker(tls: TlsConnector, password: &str) -> RedisBroker {
    RedisBroker::cluster(["redis-1.internal:6380", "redis-2.internal:6380"])
        .credentials("worker", password)
        .tls(tls)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // The system's trust roots, and no client certificate.
    let tls = TlsConnector::default_rustls()?;
    let password = env::var("REDIS_PASSWORD")?;
    service(broker(tls, &password)).run().await?;
    Ok(())
}
// --8<-- [end:broker]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use fred::rustls::{ClientConfig, RootCertStore, crypto};
    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, broker, service};

    /// A connector with no trust roots: the in-process mode never reaches the handshake that
    /// would refuse every certificate, and the test does not depend on the machine's roots.
    fn connector() -> Result<TlsConnector, Box<dyn Error>> {
        let provider = Arc::new(crypto::aws_lc_rs::default_provider());
        let config = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth();
        Ok(config.into())
    }

    #[tokio::test]
    async fn a_service_with_tls_takes_an_order() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(service(broker(connector()?, "s3cr3t"))).await?;
        tb.broker::<RedisBroker>()
            .message(&Order { id: 6 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 6 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
