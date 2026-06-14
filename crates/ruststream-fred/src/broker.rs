//! The [`RedisBroker`]: the entry point of the `fred` integration.

use std::sync::Arc;

use fred::clients::{Client, Pool};
use fred::interfaces::{ClientLike, EventInterface, PubsubInterface, StreamsInterface};
use fred::types::config::{Config, ServerConfig};
use ruststream::{Broker, DescribeServer, ServerSpec, Subscribe};
use tokio::sync::OnceCell;

use crate::{
    error::RedisError,
    list::{RedisList, RedisListPublisher, RedisListSubscriber},
    publisher::RedisPublisher,
    pubsub::{PubSubMode, RedisPubSub, RedisPubSubPublisher, RedisPubSubSubscriber},
    stream::RedisStream,
    subscriber::RedisSubscriber,
};

/// Default `fred` connection-pool size when the caller does not set one.
const DEFAULT_POOL_SIZE: usize = 4;

/// How the broker should connect, recorded synchronously and resolved into a `fred` config at
/// [`Broker::connect`] time so construction stays I/O- and failure-free.
#[derive(Debug, Clone)]
enum Topology {
    /// A single server, addressed by URL (`redis://host:port`).
    Standalone(String),
    /// A Redis Cluster, addressed by one or more `host:port` seed nodes.
    Cluster(Vec<String>),
    /// Sentinel-managed replication: the monitored primary's `service` name plus the `host:port`
    /// of each sentinel.
    Sentinel { service: String, hosts: Vec<String> },
    /// A pool supplied already-connected via [`RedisBroker::from_pool`]; no config to build.
    Preconnected,
}

/// Parses a `host:port` address (tolerating a `redis://` / `rediss://` scheme prefix) into the
/// `(host, port)` pair `fred`'s server-config constructors expect. Falls back to `default_port`
/// when no port is given.
fn parse_server(addr: &str, default_port: u16) -> Result<(String, u16), RedisError> {
    let trimmed = addr
        .trim()
        .trim_start_matches("rediss://")
        .trim_start_matches("redis://");
    let (host, port) = match trimmed.rsplit_once(':') {
        Some((host, port)) => {
            let port = port.parse::<u16>().map_err(|_| {
                RedisError::Connect(format!("invalid port in redis address `{addr}`").into())
            })?;
            (host, port)
        }
        None => (trimmed, default_port),
    };
    if host.is_empty() {
        return Err(RedisError::Connect(
            format!("missing host in redis address `{addr}`").into(),
        ));
    }
    Ok((host.to_owned(), port))
}

fn parse_servers(addrs: &[String], default_port: u16) -> Result<Vec<(String, u16)>, RedisError> {
    if addrs.is_empty() {
        return Err(RedisError::Connect("no redis addresses provided".into()));
    }
    addrs
        .iter()
        .map(|addr| parse_server(addr, default_port))
        .collect()
}

/// A Redis broker handle backed by a `fred` connection [`Pool`].
///
/// Construct it synchronously with [`RedisBroker::standalone`] and let the runtime connect it at
/// startup, or eagerly with [`RedisBroker::connect`] / [`RedisBroker::from_pool`]. The handle is
/// cheap to clone, and clones share one pool. Subscriptions are opened through
/// [`RedisBroker::subscribe`] with a [`RedisStream`] descriptor.
///
/// # Lazy connection
///
/// [`standalone`](Self::standalone) performs no I/O: it only records the server address. The pool
/// is opened by [`Broker::connect`], which the runtime calls once at startup, so a Redis service
/// can be built with the synchronous `#[ruststream::app]` macro. Publishers handed out before
/// `connect` resolve the shared pool on first use; operations that need it before `connect` return
/// [`RedisError::NotConnected`].
///
/// # Examples
///
/// ```no_run
/// use ruststream_fred::{RedisBroker, RedisStream};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let broker = RedisBroker::connect("redis://localhost:6379").await?;
/// let publisher = broker.publisher();
/// let sub = broker.subscribe(RedisStream::new("orders").group("workers")).await?;
/// # let _ = (publisher, sub);
/// broker.shutdown_pool().await;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct RedisBroker {
    pool: Arc<OnceCell<Pool>>,
    topology: Topology,
    pool_size: usize,
    default_group: Option<String>,
}

impl std::fmt::Debug for RedisBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisBroker")
            .field("topology", &self.topology)
            .field("pool_size", &self.pool_size)
            .field("default_group", &self.default_group)
            .finish_non_exhaustive()
    }
}

impl RedisBroker {
    /// Creates a standalone-topology broker that connects to `url` when [`Broker::connect`] runs.
    ///
    /// Synchronous and performs no I/O, so it slots into the `#[ruststream::app]` builder; the
    /// connection is opened lazily at startup. See the [type docs](Self#lazy-connection).
    #[must_use]
    pub fn standalone(url: impl Into<String>) -> Self {
        Self::with_topology(Topology::Standalone(url.into()))
    }

    /// Creates a Redis Cluster broker from one or more `host:port` seed nodes.
    ///
    /// Only one reachable node is needed; `fred` discovers the rest of the cluster on connect.
    /// Synchronous and performs no I/O. See the [type docs](Self#lazy-connection).
    #[must_use]
    pub fn cluster(nodes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::with_topology(Topology::Cluster(
            nodes.into_iter().map(Into::into).collect(),
        ))
    }

    /// Creates a Sentinel-backed broker that tracks the primary named `service`, discovering it
    /// through the given sentinel `host:port` addresses.
    ///
    /// Synchronous and performs no I/O. See the [type docs](Self#lazy-connection).
    #[must_use]
    pub fn sentinel(
        service: impl Into<String>,
        sentinels: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::with_topology(Topology::Sentinel {
            service: service.into(),
            hosts: sentinels.into_iter().map(Into::into).collect(),
        })
    }

    fn with_topology(topology: Topology) -> Self {
        Self {
            pool: Arc::new(OnceCell::new()),
            topology,
            pool_size: DEFAULT_POOL_SIZE,
            default_group: None,
        }
    }

    /// Sets the connection-pool size. Defaults to 4.
    #[must_use]
    pub const fn pool(mut self, size: usize) -> Self {
        self.pool_size = size;
        self
    }

    /// Sets a broker-wide default consumer group, enabling the bare-string `#[subscriber("key")]`
    /// form (Redis Streams always read through a group). Without it a bare-string subscription
    /// returns [`RedisError::InvalidOptions`]; name the group per subscription with
    /// [`RedisStream::group`] instead.
    #[must_use]
    pub fn default_group(mut self, group: impl Into<String>) -> Self {
        self.default_group = Some(group.into());
        self
    }

    /// Connects to a standalone Redis server eagerly, returning an already-connected broker.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::Connect`] when the URL is invalid or the connection cannot be
    /// established.
    pub async fn connect(url: impl Into<String>) -> Result<Self, RedisError> {
        let broker = Self::standalone(url);
        Broker::connect(&broker).await?;
        Ok(broker)
    }

    /// Wraps an already-connected `fred` pool. Useful for advanced configuration (TLS, cluster,
    /// sentinel, custom performance and reconnection policies).
    #[must_use]
    pub fn from_pool(pool: Pool) -> Self {
        Self {
            pool: Arc::new(OnceCell::new_with(Some(pool))),
            topology: Topology::Preconnected,
            pool_size: DEFAULT_POOL_SIZE,
            default_group: None,
        }
    }

    /// Builds the `fred` config for this broker's topology.
    fn build_config(&self) -> Result<Config, RedisError> {
        match &self.topology {
            Topology::Standalone(url) => {
                Config::from_url(url).map_err(|err| RedisError::Connect(Box::new(err)))
            }
            Topology::Cluster(nodes) => {
                let hosts = parse_servers(nodes, 6379)?;
                Ok(Config {
                    server: ServerConfig::new_clustered(hosts),
                    ..Config::default()
                })
            }
            Topology::Sentinel { service, hosts } => {
                let hosts = parse_servers(hosts, 26379)?;
                Ok(Config {
                    server: ServerConfig::new_sentinel(hosts, service.clone()),
                    ..Config::default()
                })
            }
            // A preconnected pool never reaches connect()'s init path.
            Topology::Preconnected => Err(RedisError::NotConnected),
        }
    }

    /// The connected pool, or [`RedisError::NotConnected`] when `connect` has not run yet.
    fn connected(&self) -> Result<Pool, RedisError> {
        self.pool.get().cloned().ok_or(RedisError::NotConnected)
    }

    /// Returns a clone of the underlying pool. Useful for advanced operations not covered by the
    /// wrapper.
    ///
    /// # Panics
    ///
    /// Panics if the broker has not connected yet (built with [`standalone`](Self::standalone) and
    /// [`Broker::connect`] not run). Call it after startup, or build with [`connect`](Self::connect)
    /// / [`from_pool`](Self::from_pool).
    #[must_use]
    pub fn pool_handle(&self) -> Pool {
        self.pool
            .get()
            .cloned()
            .expect("RedisBroker::pool_handle() called before connect()")
    }

    /// Opens a stream subscription described by `def`.
    ///
    /// Ensures the consumer group exists (`XGROUP CREATE ... MKSTREAM`, ignoring an
    /// already-existing group) before returning the subscriber.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::NotConnected`] when the broker has not connected,
    /// [`RedisError::InvalidOptions`] when `def` names no consumer group, or
    /// [`RedisError::Subscribe`] when the group cannot be created.
    pub async fn subscribe(&self, def: RedisStream) -> Result<RedisSubscriber, RedisError> {
        let pool = self.connected()?;
        let group = def.group_or_err()?.to_owned();
        let consumer = def.consumer_or_auto();
        ensure_group(&pool, def.key(), &group, def.start().as_id()).await?;
        Ok(RedisSubscriber::new(
            pool,
            def.key().to_owned(),
            group,
            consumer,
            def.count_or_default(),
            def.block_or_default(),
            def.mode(),
            def.poison_policy(),
            def.delay_config(),
        ))
    }

    /// Returns a publisher bound to this broker.
    ///
    /// It may be created before [`Broker::connect`] (for example inside the `with_broker` builder);
    /// it resolves the shared pool when it first publishes.
    #[must_use]
    pub fn publisher(&self) -> RedisPublisher {
        RedisPublisher::new(Arc::clone(&self.pool), self.supports_transactions())
    }

    /// Whether this topology can offer multi-key transactions. Cluster cannot (buffered keys may
    /// hash to different nodes), so its publishers reject `begin_transaction`.
    const fn supports_transactions(&self) -> bool {
        !matches!(self.topology, Topology::Cluster(_))
    }

    /// Builds and connects a dedicated `fred` client (used for Pub/Sub, which needs an isolated
    /// message stream and channel state per subscriber).
    async fn new_client(&self) -> Result<Client, RedisError> {
        let config = self.build_config()?;
        let client = Client::new(config, None, None, None);
        client
            .init()
            .await
            .map_err(|err| RedisError::Connect(Box::new(err)))?;
        Ok(client)
    }

    /// Opens a Pub/Sub subscription described by `def` on a dedicated client.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::InvalidOptions`] for an invalid mode/pattern combination,
    /// [`RedisError::Connect`] when the dedicated client cannot connect, or
    /// [`RedisError::Subscribe`] when the subscribe command fails.
    pub async fn subscribe_pubsub(
        &self,
        def: RedisPubSub,
    ) -> Result<RedisPubSubSubscriber, RedisError> {
        def.validate()?;
        let codec = def.codec_handle();
        let client = self.new_client().await?;
        let channel = def.channel().to_owned();
        let result = match (def.delivery_mode(), def.is_pattern()) {
            (PubSubMode::Classic, true) => client.psubscribe(channel).await,
            (PubSubMode::Classic, false) => client.subscribe(channel).await,
            (PubSubMode::Sharded, _) => client.ssubscribe(channel).await,
        };
        result.map_err(RedisError::subscribe)?;
        let rx = client.message_rx();
        Ok(RedisPubSubSubscriber::new(client, rx, codec))
    }

    /// Opens a list (work-queue) subscription described by `def`.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::NotConnected`] when the broker has not connected, or
    /// [`RedisError::InvalidOptions`] when `def` names a recovery ZSET without a `min_idle`.
    #[allow(
        clippy::unused_async,
        reason = "async for parity with the other subscribe methods and the SubscriptionSource shape"
    )]
    pub async fn subscribe_list(&self, def: RedisList) -> Result<RedisListSubscriber, RedisError> {
        let pool = self.connected()?;
        let recovery = def.recovery_config()?;
        Ok(RedisListSubscriber::new(
            pool,
            def.key().to_owned(),
            def.is_reliable(),
            def.processing_or_default(),
            def.block_or_default(),
            def.codec_handle(),
            def.poison_policy(),
            recovery,
        ))
    }

    /// Returns a Pub/Sub publisher (classic mode by default; override with
    /// [`RedisPubSubPublisher::mode`]).
    #[must_use]
    pub fn pubsub_publisher(&self) -> RedisPubSubPublisher {
        RedisPubSubPublisher::new(Arc::clone(&self.pool), PubSubMode::Classic)
    }

    /// Returns a list publisher (`LPUSH`).
    #[must_use]
    pub fn list_publisher(&self) -> RedisListPublisher {
        RedisListPublisher::new(Arc::clone(&self.pool))
    }

    /// Closes the underlying pool. A no-op if the broker never connected.
    pub async fn shutdown_pool(&self) {
        if let Some(pool) = self.pool.get() {
            let _ = pool.quit().await;
        }
    }
}

/// Creates the consumer group, treating an already-existing group as success.
async fn ensure_group(
    pool: &Pool,
    key: &str,
    group: &str,
    start_id: &str,
) -> Result<(), RedisError> {
    let result: Result<String, fred::error::Error> =
        pool.xgroup_create(key, group, start_id, true).await;
    match result {
        Ok(_) => Ok(()),
        // BUSYGROUP: the group already exists, which is the steady-state case.
        Err(err) if err.details().contains("BUSYGROUP") => Ok(()),
        Err(err) => Err(RedisError::subscribe(err)),
    }
}

impl Broker for RedisBroker {
    type Error = RedisError;

    async fn connect(&self) -> Result<(), Self::Error> {
        self.pool
            .get_or_try_init(|| async {
                let config = self.build_config()?;
                let pool = Pool::new(config, None, None, None, self.pool_size)
                    .map_err(|err| RedisError::Connect(Box::new(err)))?;
                pool.init()
                    .await
                    .map_err(|err| RedisError::Connect(Box::new(err)))?;
                Ok(pool)
            })
            .await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        self.shutdown_pool().await;
        Ok(())
    }
}

// By-name subscription capability for the bare string `#[subscriber("key")]` form. Redis Streams
// always read through a consumer group, so this requires a broker-wide default group.
#[allow(clippy::use_self)]
impl Subscribe for RedisBroker {
    type Subscriber = RedisSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        let group = self.default_group.clone().ok_or_else(|| {
            RedisError::InvalidOptions(format!(
                "bare-string subscription on `{name}` needs a broker-wide default group: \
                 call RedisBroker::default_group(name), or subscribe with \
                 RedisStream::new(name).group(group)"
            ))
        })?;
        RedisBroker::subscribe(self, RedisStream::new(name).group(group)).await
    }
}

/// `DescribeServer` reports the configured Redis address (the first seed for cluster/sentinel).
impl DescribeServer for RedisBroker {
    fn describe_server(&self) -> ServerSpec {
        let host = match &self.topology {
            Topology::Standalone(url) => url
                .trim_start_matches("rediss://")
                .trim_start_matches("redis://")
                .to_owned(),
            Topology::Cluster(nodes) => nodes.first().cloned().unwrap_or_default(),
            Topology::Sentinel { hosts, .. } => hosts.first().cloned().unwrap_or_default(),
            Topology::Preconnected => String::new(),
        };
        ServerSpec::new(host, "redis")
    }
}

#[cfg(test)]
mod tests {
    use ruststream::{OutgoingMessage, Publisher};

    use super::*;

    // `standalone` records the address without connecting, so operations needing the connection
    // fail cleanly until `Broker::connect` runs. No server required.
    #[tokio::test]
    async fn standalone_does_not_connect() {
        let broker = RedisBroker::standalone("redis://127.0.0.1:6379");

        let publish_err = broker
            .publisher()
            .publish(OutgoingMessage::new("orders", b"{}".as_slice()))
            .await
            .unwrap_err();
        assert!(matches!(publish_err, RedisError::NotConnected));

        let subscribe_err = broker
            .subscribe(RedisStream::new("orders").group("g"))
            .await
            .unwrap_err();
        assert!(matches!(subscribe_err, RedisError::NotConnected));
    }

    #[tokio::test]
    async fn bare_string_subscription_needs_default_group() {
        let broker = RedisBroker::standalone("redis://127.0.0.1:6379");
        let err = Subscribe::subscribe(&broker, "orders").await.unwrap_err();
        assert!(matches!(err, RedisError::InvalidOptions(msg) if msg.contains("default group")));
    }

    #[test]
    fn describe_server_reports_redis() {
        let broker = RedisBroker::standalone("redis://localhost:6379");
        let spec = broker.describe_server();
        assert_eq!(spec.protocol, "redis");
        assert_eq!(spec.host, "localhost:6379");
    }
}
