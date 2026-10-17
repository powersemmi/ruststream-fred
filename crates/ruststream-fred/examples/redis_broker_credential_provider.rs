//! A credential provider: the client asks it for the user and password on every `AUTH` or
//! `HELLO`, so a rotated password reaches a running service. The in-process mode runs no
//! handshake, so its test proves the service starts and works with the provider; the live test
//! authenticates through it against the stand's password-protected server and skips without one.
//!
//! ```text
//! cargo test --example redis_broker_credential_provider --features testing,credential-provider
//! just brokers-up
//! REDIS_AUTH_TEST_URL=redis://127.0.0.1:6385 \
//!     cargo test --example redis_broker_credential_provider --features testing,credential-provider
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use fred::error::{Error as FredError, ErrorKind};
use fred::types::config::Server;
use serde::{Deserialize, Serialize};
use tokio::task;

use ruststream_fred::CredentialProvider;
use ruststream_fred::stream::prelude::*;

#[cfg(test)]
#[path = "../tests/live/mod.rs"]
mod live;

#[derive(Debug, PartialEq, Deserialize, Serialize, Outgoing)]
#[outgoing(name = "rotated.orders")]
struct Order {
    id: u64,
}

#[subscriber(RedisStream::new("rotated.orders").group("workers"))]
async fn take(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "order taken");
    HandlerOutcome::ack()
}

fn service(broker: RedisBroker) -> impl App<State = ()> {
    RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(broker, |b| {
        b.include(take);
    })
}

// --8<-- [start:provider]
/// The password of the ACL user `worker`, read from a secret file the platform rotates.
#[derive(Debug)]
struct RotatedSecret {
    path: PathBuf,
}

#[async_trait]
impl CredentialProvider for RotatedSecret {
    async fn fetch(
        &self,
        _server: Option<&Server>,
    ) -> Result<(Option<String>, Option<String>), FredError> {
        let path = self.path.clone();
        // A file read blocks, so it runs off the connection's task.
        let read = task::spawn_blocking(move || fs::read_to_string(path))
            .await
            .map_err(|err| FredError::new(ErrorKind::Auth, err.to_string()))?;
        let password = read.map_err(|err| FredError::new(ErrorKind::Auth, err.to_string()))?;
        Ok((Some("worker".to_owned()), Some(password.trim().to_owned())))
    }

    /// Logs in again every ten minutes, with whatever the file holds by then.
    fn refresh_interval(&self) -> Option<Duration> {
        Some(Duration::from_mins(10))
    }
}

fn broker(url: &str, secret: PathBuf) -> RedisBroker {
    RedisBroker::standalone(url).credential_provider(Arc::new(RotatedSecret { path: secret }))
}

#[ruststream::app]
fn app() -> impl App<State = ()> {
    service(broker(
        "redis://localhost:6379",
        PathBuf::from("/run/secrets/redis-password"),
    ))
}
// --8<-- [end:provider]

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::{env, fs, process};

    use ruststream::testing::TestApp;
    use ruststream_fred::stream::prelude::*;

    use super::{Order, app, broker, live, service};

    /// The body both modes run.
    async fn takes_an_order(tb: TestApp<()>) -> Result<(), Box<dyn Error>> {
        tb.broker::<RedisBroker>()
            .message(&Order { id: 12 })
            .publish()
            .await?;

        tb.broker::<RedisBroker>()
            .subscriber("rotated.orders")
            .assert_called_once()
            .with(&Order { id: 12 })
            .settled(HandlerOutcome::ack());
        tb.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn in_process() -> Result<(), Box<dyn Error>> {
        takes_an_order(TestApp::start(app()).await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_provider_logs_in_over_static_credentials() -> Result<(), Box<dyn Error>> {
        let Some(url) = live::url("REDIS_AUTH_TEST_URL") else {
            return Ok(());
        };
        let secret = env::temp_dir().join(format!("redis-password-{}", process::id()));
        fs::write(&secret, "workerpass\n")?;
        // A wrong static pair: the server accepts the login only if the provider's pair wins.
        let broker = broker(&url, secret.clone()).credentials("worker", "not-the-password");
        let started = TestApp::start_live(service(broker)).await;
        // The login is done once the service has started.
        fs::remove_file(&secret)?;
        takes_an_order(started?).await
    }
}
