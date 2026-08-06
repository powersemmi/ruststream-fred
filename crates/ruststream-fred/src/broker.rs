//! The broker ladder: [`RedisBroker`] -> [`ConnectedRedisBroker`] -> [`ClosedRedisBroker`].
//!
//! Construction is synchronous and I/O-free: the named constructors only record the topology and
//! its options. All network work happens in the consuming [`Broker::connect`], and the connected
//! form owns the live `fred` pool, so subscriptions and publishers exist only once a connection
//! does.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use fred::clients::{Client, Pool};
use fred::interfaces::{ClientLike, EventInterface, PubsubInterface, StreamsInterface};
#[cfg(feature = "credential-provider")]
use fred::types::config::CredentialProvider;
#[cfg(any(
    feature = "tls-rustls",
    feature = "tls-rustls-ring",
    feature = "tls-native-tls"
))]
use fred::types::config::TlsConfig;
use fred::types::config::{Config, ServerConfig};
use ruststream::{Broker, ConnectedBroker, DescribeServer, ServerSpec, Subscribe};

use crate::{
    error::RedisError,
    list::{RedisList, RedisListPublish, RedisListPublisher, RedisListSubscriber},
    publisher::RedisPublisher,
    pubsub::{
        PubSubMode, RedisPubSub, RedisPubSubPublish, RedisPubSubPublisher, RedisPubSubSubscriber,
    },
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
    /// A pool supplied already-connected via [`RedisBroker::from_pool`]; `connect` adopts it
    /// instead of dialing.
    Preconnected(Pool),
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

/// Authentication and TLS settings recorded on the broker and folded into the `fred` [`Config`]
/// at connect time, on every topology. Fields with no value are left untouched, so credentials
/// supplied through a standalone `redis://user:pass@host` URL survive unless overridden here.
#[derive(Clone, Default)]
struct AuthConfig {
    /// ACL username for the data nodes (`Config.username`).
    username: Option<String>,
    /// Password for the data nodes (`Config.password`).
    password: Option<String>,
    /// ACL username for authenticating to the sentinels, distinct from the data-node username.
    #[cfg(feature = "sentinel-auth")]
    sentinel_username: Option<String>,
    /// Password for authenticating to the sentinels, distinct from the data-node password.
    #[cfg(feature = "sentinel-auth")]
    sentinel_password: Option<String>,
    /// Explicit TLS configuration (`Config.tls`).
    #[cfg(any(
        feature = "tls-rustls",
        feature = "tls-rustls-ring",
        feature = "tls-native-tls"
    ))]
    tls: Option<TlsConfig>,
    /// Dynamic/rotating credential provider (`Config.credential_provider`).
    #[cfg(feature = "credential-provider")]
    credential_provider: Option<Arc<dyn CredentialProvider>>,
}

// Redacts secrets: passwords never appear, and TLS / credential-provider show only presence. The
// usernames are identifiers (not secrets) and are kept to aid debugging.
impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("AuthConfig");
        s.field("username", &self.username);
        s.field("password", &self.password.as_ref().map(|_| "<redacted>"));
        #[cfg(feature = "sentinel-auth")]
        {
            s.field("sentinel_username", &self.sentinel_username);
            s.field(
                "sentinel_password",
                &self.sentinel_password.as_ref().map(|_| "<redacted>"),
            );
        }
        #[cfg(any(
            feature = "tls-rustls",
            feature = "tls-rustls-ring",
            feature = "tls-native-tls"
        ))]
        s.field("tls", &self.tls.as_ref().map(|_| "<configured>"));
        #[cfg(feature = "credential-provider")]
        s.field(
            "credential_provider",
            &self.credential_provider.as_ref().map(|_| "<configured>"),
        );
        s.finish()
    }
}

/// An unconnected Redis broker: the recorded topology and its options, no I/O performed yet.
///
/// Build it with [`standalone`](Self::standalone), [`cluster`](Self::cluster),
/// [`sentinel`](Self::sentinel), or [`from_pool`](Self::from_pool). All four are synchronous and
/// perform no I/O, so a Redis service composes with the synchronous `#[ruststream::app]` builder;
/// the runtime dials once at startup through the consuming [`Broker::connect`], which yields the
/// [`ConnectedRedisBroker`] every subscription and publisher is reached from.
///
/// # Examples
///
/// ```no_run
/// use ruststream::{Broker, ConnectedBroker};
/// use ruststream_fred::{RedisBroker, RedisStream};
///
/// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
/// let connected = RedisBroker::standalone("redis://localhost:6379").connect().await?;
/// let publisher = connected.publisher();
/// let sub = connected.subscribe(RedisStream::new("orders").group("workers")).await?;
/// # let _ = (publisher, sub);
/// let _closed = connected.shutdown().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
#[must_use]
pub struct RedisBroker {
    topology: Topology,
    pool_size: usize,
    default_group: Option<String>,
    auth: AuthConfig,
}

impl RedisBroker {
    /// Creates a standalone-topology broker that connects to `url` when [`Broker::connect`] runs.
    pub fn standalone(url: impl Into<String>) -> Self {
        Self::with_topology(Topology::Standalone(url.into()))
    }

    /// Creates a Redis Cluster broker from one or more `host:port` seed nodes.
    ///
    /// Only one reachable node is needed; `fred` discovers the rest of the cluster on connect.
    pub fn cluster(nodes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self::with_topology(Topology::Cluster(
            nodes.into_iter().map(Into::into).collect(),
        ))
    }

    /// Creates a Sentinel-backed broker that tracks the primary named `service`, discovering it
    /// through the given sentinel `host:port` addresses.
    pub fn sentinel(
        service: impl Into<String>,
        sentinels: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self::with_topology(Topology::Sentinel {
            service: service.into(),
            hosts: sentinels.into_iter().map(Into::into).collect(),
        })
    }

    /// Wraps an already-connected `fred` pool. Useful for advanced configuration (TLS, cluster,
    /// sentinel, custom performance and reconnection policies).
    ///
    /// [`Broker::connect`] adopts the pool instead of dialing; the config it was built from is
    /// reused for the dedicated clients Pub/Sub subscriptions need.
    pub fn from_pool(pool: Pool) -> Self {
        Self::with_topology(Topology::Preconnected(pool))
    }

    fn with_topology(topology: Topology) -> Self {
        Self {
            topology,
            pool_size: DEFAULT_POOL_SIZE,
            default_group: None,
            auth: AuthConfig::default(),
        }
    }

    /// Sets the connection-pool size. Defaults to 4.
    pub const fn pool(mut self, size: usize) -> Self {
        self.pool_size = size;
        self
    }

    /// Sets a broker-wide default consumer group, enabling the bare-string `#[subscriber("key")]`
    /// form (Redis Streams always read through a group). Without it a bare-string subscription
    /// returns [`RedisError::InvalidOptions`]; name the group per subscription with
    /// [`RedisStream::group`] instead.
    pub fn default_group(mut self, group: impl Into<String>) -> Self {
        self.default_group = Some(group.into());
        self
    }

    /// Sets the ACL `username` and `password` used to authenticate on connect, applied on every
    /// topology (standalone, cluster, sentinel).
    ///
    /// This maps onto `fred`'s `Config.username` / `Config.password`, so authentication works
    /// beyond the standalone `redis://user:pass@host` URL, which the bare `cluster` / `sentinel`
    /// seed lists cannot express. Credentials set here override any in a standalone URL.
    ///
    /// For a password-only `AUTH` (the legacy `requirepass`, no ACL user) use
    /// [`password`](Self::password).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream_fred::RedisBroker;
    ///
    /// let broker = RedisBroker::cluster(["10.0.0.1:6379"]).credentials("worker", "s3cr3t");
    /// # let _ = broker;
    /// ```
    pub fn credentials(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth.username = Some(username.into());
        self.auth.password = Some(password.into());
        self
    }

    /// Sets a password-only `AUTH` (no ACL username; the legacy `requirepass` form), on every
    /// topology. Use [`credentials`](Self::credentials) for an ACL user plus password.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream_fred::RedisBroker;
    ///
    /// let broker = RedisBroker::sentinel("mymaster", ["10.0.0.1:26379"]).password("s3cr3t");
    /// # let _ = broker;
    /// ```
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.auth.password = Some(password.into());
        self
    }

    /// Sets the TLS configuration used on connect, on every topology. Accepts a `fred`
    /// [`TlsConfig`] or anything convertible into one (for example a `TlsConnector`).
    ///
    /// Available behind the `tls-rustls`, `tls-rustls-ring`, or `tls-native-tls` feature; a
    /// standalone broker can also enable TLS through a `rediss://` / `valkeys://` URL. The
    /// `fred` re-exports [`TlsConfig`](crate::TlsConfig) / [`TlsConnector`](crate::TlsConnector)
    /// provide `default_rustls()` / `default_native_tls()` shorthands for system-trust setups.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream_fred::{RedisBroker, TlsConfig};
    ///
    /// fn build(tls: TlsConfig) -> RedisBroker {
    ///     RedisBroker::cluster(["10.0.0.1:6379"]).tls(tls)
    /// }
    /// ```
    #[cfg(any(
        feature = "tls-rustls",
        feature = "tls-rustls-ring",
        feature = "tls-native-tls"
    ))]
    pub fn tls(mut self, tls: impl Into<TlsConfig>) -> Self {
        self.auth.tls = Some(tls.into());
        self
    }

    /// Sets distinct credentials for authenticating to the sentinel nodes, separate from the
    /// data-node [`credentials`](Self::credentials). Only meaningful on the sentinel topology.
    ///
    /// Available behind the `sentinel-auth` feature.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream_fred::RedisBroker;
    ///
    /// let broker = RedisBroker::sentinel("mymaster", ["10.0.0.1:26379"])
    ///     .credentials("worker", "data-pass")
    ///     .sentinel_credentials("sentinel-user", "sentinel-pass");
    /// # let _ = broker;
    /// ```
    #[cfg(feature = "sentinel-auth")]
    pub fn sentinel_credentials(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth.sentinel_username = Some(username.into());
        self.auth.sentinel_password = Some(password.into());
        self
    }

    /// Sets a password-only credential for authenticating to the sentinel nodes. Use
    /// [`sentinel_credentials`](Self::sentinel_credentials) for an ACL user plus password.
    ///
    /// Available behind the `sentinel-auth` feature.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ruststream_fred::RedisBroker;
    ///
    /// let broker = RedisBroker::sentinel("mymaster", ["10.0.0.1:26379"])
    ///     .sentinel_password("sentinel-pass");
    /// # let _ = broker;
    /// ```
    #[cfg(feature = "sentinel-auth")]
    pub fn sentinel_password(mut self, password: impl Into<String>) -> Self {
        self.auth.sentinel_password = Some(password.into());
        self
    }

    /// Sets a dynamic credential provider that supplies (and can rotate) the username/password on
    /// each `AUTH` / `HELLO`, for IAM-style auth. Takes precedence over static
    /// [`credentials`](Self::credentials).
    ///
    /// Available behind the `credential-provider` feature.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use std::sync::Arc;
    /// use ruststream_fred::{CredentialProvider, RedisBroker};
    ///
    /// fn build(provider: Arc<dyn CredentialProvider>) -> RedisBroker {
    ///     RedisBroker::standalone("redis://localhost:6379").credential_provider(provider)
    /// }
    /// ```
    #[cfg(feature = "credential-provider")]
    pub fn credential_provider(mut self, provider: Arc<dyn CredentialProvider>) -> Self {
        self.auth.credential_provider = Some(provider);
        self
    }

    /// Builds the `fred` config for this broker's topology, then folds in the auth/TLS settings.
    fn build_config(&self) -> Result<Config, RedisError> {
        let mut config = match &self.topology {
            Topology::Standalone(url) => {
                Config::from_url(url).map_err(|err| RedisError::Connect(Box::new(err)))?
            }
            Topology::Cluster(nodes) => {
                let hosts = parse_servers(nodes, 6379)?;
                Config {
                    server: ServerConfig::new_clustered(hosts),
                    ..Config::default()
                }
            }
            Topology::Sentinel { service, hosts } => {
                let hosts = parse_servers(hosts, 26379)?;
                Config {
                    server: ServerConfig::new_sentinel(hosts, service.clone()),
                    ..Config::default()
                }
            }
            // A caller-supplied pool carries the config it was built from; reusing it keeps the
            // Pub/Sub path (which dials a dedicated client per subscription) available.
            Topology::Preconnected(pool) => pool.next().client_config(),
        };
        self.apply_auth(&mut config);
        Ok(config)
    }

    /// Folds the recorded auth/TLS settings into `config`. Each setting is applied only when set,
    /// so credentials carried by a standalone URL survive unless explicitly overridden.
    fn apply_auth(&self, config: &mut Config) {
        if self.auth.username.is_some() {
            config.username.clone_from(&self.auth.username);
        }
        if self.auth.password.is_some() {
            config.password.clone_from(&self.auth.password);
        }
        #[cfg(any(
            feature = "tls-rustls",
            feature = "tls-rustls-ring",
            feature = "tls-native-tls"
        ))]
        if self.auth.tls.is_some() {
            config.tls.clone_from(&self.auth.tls);
        }
        #[cfg(feature = "credential-provider")]
        if self.auth.credential_provider.is_some() {
            config
                .credential_provider
                .clone_from(&self.auth.credential_provider);
        }
        #[cfg(feature = "sentinel-auth")]
        if let ServerConfig::Sentinel {
            username, password, ..
        } = &mut config.server
        {
            if self.auth.sentinel_username.is_some() {
                username.clone_from(&self.auth.sentinel_username);
            }
            if self.auth.sentinel_password.is_some() {
                password.clone_from(&self.auth.sentinel_password);
            }
        }
    }

    /// Whether this topology can offer multi-key transactions. Cluster cannot (buffered keys may
    /// hash to different nodes), so its publishers reject `begin_transaction`.
    const fn supports_transactions(&self) -> bool {
        !matches!(self.topology, Topology::Cluster(_))
    }
}

impl Broker for RedisBroker {
    type Error = RedisError;
    type Connected = ConnectedRedisBroker;

    /// Opens (or adopts) the connection pool, consuming the unconnected form.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::Connect`] when the recorded topology cannot be turned into a `fred`
    /// config or the pool cannot reach the server.
    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let config = self.build_config()?;
        let transactions_supported = self.supports_transactions();
        let pool = if let Topology::Preconnected(pool) = &self.topology {
            pool.clone()
        } else {
            let pool = Pool::new(config.clone(), None, None, None, self.pool_size)
                .map_err(|err| RedisError::Connect(Box::new(err)))?;
            pool.init()
                .await
                .map_err(|err| RedisError::Connect(Box::new(err)))?;
            pool
        };
        Ok(ConnectedRedisBroker {
            core: Arc::new(RedisCore {
                pool,
                config,
                default_group: self.default_group,
                transactions_supported,
                closed: AtomicBool::new(false),
            }),
        })
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
            Topology::Preconnected(_) => String::new(),
        };
        ServerSpec::new(host, "redis")
    }
}

/// The live connection shared by the connected broker and every handle derived from it.
pub(crate) struct RedisCore {
    pool: Pool,
    /// The config the pool was built from; Pub/Sub subscriptions dial their dedicated client
    /// from it.
    config: Config,
    default_group: Option<String>,
    transactions_supported: bool,
    /// Flipped by [`ConnectedBroker::shutdown`]. The ladder makes owner-side misuse a compile
    /// error, but publishers handed out before the shutdown alias the connection and outlive it,
    /// so their operations check this flag rather than issuing a command against a dead pool.
    closed: AtomicBool,
}

impl RedisCore {
    /// The live pool, or [`RedisError::ShutDown`] once the connection was torn down.
    pub(crate) fn pool(&self) -> Result<Pool, RedisError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(RedisError::ShutDown);
        }
        Ok(self.pool.clone())
    }

    pub(crate) const fn transactions_supported(&self) -> bool {
        self.transactions_supported
    }
}

impl std::fmt::Debug for RedisCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisCore")
            .field("pool", &self.pool)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

/// The typed witness that [`Broker::connect`] succeeded: it owns the live `fred` pool.
///
/// Every subscription and every publisher is reached from here, so "not connected" is not
/// representable. [`ConnectedBroker::shutdown`] consumes it, which makes a publish or subscribe
/// after shutdown a compile error for the owner of the handle.
#[derive(Debug)]
pub struct ConnectedRedisBroker {
    core: Arc<RedisCore>,
}

impl ConnectedRedisBroker {
    /// Opens a stream subscription described by `def`.
    ///
    /// Ensures the consumer group exists (`XGROUP CREATE ... MKSTREAM`, ignoring an
    /// already-existing group) before returning the subscriber.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::ShutDown`] when the connection was already torn down,
    /// [`RedisError::InvalidOptions`] when `def` names no consumer group, or
    /// [`RedisError::Subscribe`] when the group cannot be created.
    pub async fn subscribe(&self, def: RedisStream) -> Result<RedisSubscriber, RedisError> {
        let pool = self.core.pool()?;
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

    /// Opens a Pub/Sub subscription described by `def` on a dedicated client.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::InvalidOptions`] for an invalid mode/pattern combination,
    /// [`RedisError::ShutDown`] when the connection was already torn down,
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
    /// Returns [`RedisError::ShutDown`] when the connection was already torn down, or
    /// [`RedisError::InvalidOptions`] when `def` names a recovery ZSET without a `min_idle`.
    #[allow(
        clippy::unused_async,
        reason = "async for parity with the other subscribe methods and the SubscriptionSource shape"
    )]
    pub async fn subscribe_list(&self, def: RedisList) -> Result<RedisListSubscriber, RedisError> {
        let pool = self.core.pool()?;
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

    /// Returns a stream publisher (`XADD`) bound to this connection.
    #[must_use]
    pub fn publisher(&self) -> RedisPublisher {
        RedisPublisher::new(Arc::clone(&self.core))
    }

    /// Returns a Pub/Sub publisher configured by `publish` (mode and envelope codec).
    #[must_use]
    pub fn pubsub_publisher(&self, publish: RedisPubSubPublish) -> RedisPubSubPublisher {
        RedisPubSubPublisher::new(Arc::clone(&self.core), publish)
    }

    /// Returns a list publisher (`LPUSH`) configured by `publish` (envelope codec and key TTL).
    #[must_use]
    pub fn list_publisher(&self, publish: RedisListPublish) -> RedisListPublisher {
        RedisListPublisher::new(Arc::clone(&self.core), publish)
    }

    /// Returns a clone of the underlying pool, for advanced operations not covered by the
    /// wrapper.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::ShutDown`] once the connection was torn down.
    pub fn pool_handle(&self) -> Result<Pool, RedisError> {
        self.core.pool()
    }

    /// Builds and connects a dedicated `fred` client (used for Pub/Sub, which needs an isolated
    /// message stream and channel state per subscriber).
    async fn new_client(&self) -> Result<Client, RedisError> {
        // The dedicated client is a second connection to the same server: refuse to dial one for
        // a connection whose owner already shut down.
        let _ = self.core.pool()?;
        let client = Client::new(self.core.config.clone(), None, None, None);
        client
            .init()
            .await
            .map_err(|err| RedisError::Connect(Box::new(err)))?;
        Ok(client)
    }
}

impl ConnectedBroker for ConnectedRedisBroker {
    type Error = RedisError;
    type Closed = ClosedRedisBroker;

    /// Closes every pooled connection and marks the shared connection dead, so publishers handed
    /// out earlier report [`RedisError::ShutDown`] instead of running against a closed pool.
    ///
    /// # Errors
    ///
    /// Returns [`RedisError::Connect`] when the `QUIT` roundtrip fails.
    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        self.core.closed.store(true, Ordering::Release);
        let connections_closed = self.core.pool.clients().len();
        self.core
            .pool
            .quit()
            .await
            .map_err(|err| RedisError::Connect(Box::new(err)))?;
        Ok(ClosedRedisBroker { connections_closed })
    }
}

/// The terminal witness returned by shutting down a [`ConnectedRedisBroker`].
///
/// It has no publish or subscribe surface; it carries the teardown diagnostics as plain data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedRedisBroker {
    connections_closed: usize,
}

impl ClosedRedisBroker {
    /// How many pooled connections the teardown closed.
    #[must_use]
    pub const fn connections_closed(&self) -> usize {
        self.connections_closed
    }
}

// By-name subscription capability for the bare string `#[subscriber("key")]` form. Redis Streams
// always read through a consumer group, so this requires a broker-wide default group.
#[allow(
    clippy::use_self,
    reason = "the type name disambiguates the inherent subscribe from this trait method"
)]
impl Subscribe for ConnectedRedisBroker {
    type Subscriber = RedisSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        let group = self.core.default_group.clone().ok_or_else(|| {
            RedisError::InvalidOptions(format!(
                "bare-string subscription on `{name}` needs a broker-wide default group: \
                 call RedisBroker::default_group(name), or subscribe with \
                 RedisStream::new(name).group(group)"
            ))
        })?;
        ConnectedRedisBroker::subscribe(self, RedisStream::new(name).group(group)).await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_server_reports_redis() {
        let broker = RedisBroker::standalone("redis://localhost:6379");
        let spec = broker.describe_server();
        assert_eq!(spec.protocol, "redis");
        assert_eq!(spec.host.as_deref(), Some("localhost:6379"));
    }

    // Credentials must reach the fred config on every topology, not just the standalone URL.
    #[test]
    fn credentials_apply_to_all_topologies() {
        let brokers = [
            RedisBroker::standalone("redis://localhost:6379").credentials("alice", "s3cr3t"),
            RedisBroker::cluster(["127.0.0.1:7000"]).credentials("alice", "s3cr3t"),
            RedisBroker::sentinel("mymaster", ["127.0.0.1:26379"]).credentials("alice", "s3cr3t"),
        ];
        for broker in brokers {
            let config = broker.build_config().expect("config builds");
            assert_eq!(config.username.as_deref(), Some("alice"));
            assert_eq!(config.password.as_deref(), Some("s3cr3t"));
        }
    }

    #[test]
    fn password_only_sets_password_without_username() {
        let config = RedisBroker::cluster(["127.0.0.1:7000"])
            .password("requirepass")
            .build_config()
            .expect("config builds");
        assert_eq!(config.username, None);
        assert_eq!(config.password.as_deref(), Some("requirepass"));
    }

    // Programmatic credentials win over a standalone URL's userinfo.
    #[test]
    fn programmatic_credentials_override_standalone_url() {
        let config = RedisBroker::standalone("redis://urluser:urlpass@localhost:6379")
            .credentials("acluser", "aclpass")
            .build_config()
            .expect("config builds");
        assert_eq!(config.username.as_deref(), Some("acluser"));
        assert_eq!(config.password.as_deref(), Some("aclpass"));
    }

    // Without an override the URL's credentials are left untouched.
    #[test]
    fn url_credentials_preserved_without_override() {
        let config = RedisBroker::standalone("redis://urluser:urlpass@localhost:6379")
            .build_config()
            .expect("config builds");
        assert_eq!(config.username.as_deref(), Some("urluser"));
        assert_eq!(config.password.as_deref(), Some("urlpass"));
    }

    #[test]
    fn debug_redacts_password() {
        let broker =
            RedisBroker::standalone("redis://localhost:6379").credentials("alice", "s3cr3t");
        let rendered = format!("{broker:?}");
        assert!(
            !rendered.contains("s3cr3t"),
            "password must not appear in Debug output: {rendered}"
        );
        // The username is an identifier, not a secret, and is kept for debugging.
        assert!(
            rendered.contains("alice"),
            "expected username in: {rendered}"
        );
    }

    #[cfg(feature = "sentinel-auth")]
    #[test]
    fn sentinel_credentials_apply_to_sentinel_server() {
        let config = RedisBroker::sentinel("mymaster", ["127.0.0.1:26379"])
            .credentials("datauser", "datapass")
            .sentinel_credentials("sentineluser", "sentinelpass")
            .build_config()
            .expect("config builds");
        // Data-node credentials sit on the top-level config.
        assert_eq!(config.username.as_deref(), Some("datauser"));
        let ServerConfig::Sentinel {
            username, password, ..
        } = &config.server
        else {
            panic!("expected a sentinel server config");
        };
        assert_eq!(username.as_deref(), Some("sentineluser"));
        assert_eq!(password.as_deref(), Some("sentinelpass"));
    }

    #[cfg(feature = "credential-provider")]
    #[derive(Debug)]
    struct StaticCredentials;

    #[cfg(feature = "credential-provider")]
    #[async_trait::async_trait]
    impl CredentialProvider for StaticCredentials {
        async fn fetch(
            &self,
            _server: Option<&fred::types::config::Server>,
        ) -> Result<(Option<String>, Option<String>), fred::error::Error> {
            Ok((Some("rotating".into()), Some("token".into())))
        }
    }

    #[cfg(feature = "credential-provider")]
    #[test]
    fn credential_provider_is_applied() {
        let provider: Arc<dyn CredentialProvider> = Arc::new(StaticCredentials);
        let config = RedisBroker::cluster(["127.0.0.1:7000"])
            .credential_provider(provider)
            .build_config()
            .expect("config builds");
        assert!(config.credential_provider.is_some());
    }
}
