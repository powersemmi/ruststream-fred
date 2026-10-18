//! The service of the Interoperating with other clients guide: an order service that reads
//! orders a producer in another language appends to a stream, and pushes emails onto a list a
//! worker in another language pops.
//!
//! ```text
//! docker run -p 6379:6379 redis:8.4
//! cargo run --example guide_interop --features testing -- run
//! redis-cli XADD orders '*' _payload '{"id":1,"email":"ann@example.com"}'
//! redis-cli LPOP emails
//! ```

use ruststream::codec::JsonCodec;
use ruststream_fred::prelude::*;
use serde::{Deserialize, Serialize};

// --8<-- [start:read]
/// An order as the shop's producer appends it: `XADD orders * _payload '{"id":1,...}'`.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "orders")]
struct Order {
    id: u64,
    email: String,
}

/// An email for the mailer, a worker written in another language.
#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "emails")]
struct Email {
    to: String,
    subject: String,
}

#[subscriber(RedisStream::new("orders").group("mail"), reply)]
async fn take_order(order: &Order) -> Email {
    tracing::info!(order.id, "order taken");
    Email {
        to: order.email.clone(),
        subject: format!("Order {} received", order.id),
    }
}
// --8<-- [end:read]

// --8<-- [start:write]
#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(RedisBroker::standalone("redis://localhost:6379"))
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("shop", "0.1.0")).with_broker(broker, |b| {
        // The envelope in JSON: a value the mailer reads with any JSON parser.
        b.include(take_order)
            .out_reply(list::Publish::default().codec(JsonCodec));
    })
}
// --8<-- [end:write]

#[cfg(test)]
#[path = "../../tests/live/mod.rs"]
mod live;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::error::Error;

    use fred::clients::Client;
    use fred::interfaces::{ClientLike, ListInterface, StreamsInterface};
    use fred::types::config::Config;
    use ruststream::testing::TestApp;
    use ruststream_fred::prelude::*;

    use super::{Order, app, live, service};

    /// The mailer's value: the envelope, with the email as JSON text in `payload`.
    const EMAIL: &[u8] =
        br#"{"headers":{},"payload":"{\"to\":\"ann@example.com\",\"subject\":\"Order 1 received\"}"}"#;

    fn order() -> Order {
        Order {
            id: 1,
            email: "ann@example.com".to_owned(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_order_becomes_an_email_any_json_parser_reads() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        tb.broker::<RedisBroker>()
            .message(&order())
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&order());
        tb.broker::<RedisBroker>()
            .published::<()>("emails")
            .assert_called_once()
            .with_raw(EMAIL);
        tb.shutdown().await?;
        Ok(())
    }

    /// What another client reads off a server: the order under `_payload`, the email as the
    /// envelope.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn another_client_reads_both_values() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("REDIS_TEST_URL") else {
            return Ok(());
        };
        let other = Client::new(Config::from_url(&url)?, None, None, None);
        other.init().await?;

        let tb = TestApp::start_live(service(RedisBroker::standalone(url.as_str()))).await?;
        tb.broker::<RedisBroker>()
            .message(&order())
            .publish()
            .await?;
        tb.shutdown().await?;

        // The newest entry of the stream and the newest value of the list are this test's.
        let entries: Vec<(String, HashMap<String, String>)> =
            other.xrevrange("orders", "+", "-", Some(1)).await?;
        let payloads: Vec<Option<&String>> = entries
            .iter()
            .map(|(_, fields)| fields.get("_payload"))
            .collect();
        assert_eq!(
            payloads,
            [Some(&r#"{"id":1,"email":"ann@example.com"}"#.to_owned())]
        );
        let email: Option<Vec<u8>> = other.lindex("emails", 0).await?;
        assert_eq!(email.as_deref(), Some(EMAIL));
        other.quit().await?;
        Ok(())
    }
}
