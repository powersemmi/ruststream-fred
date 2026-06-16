//! TLS examples for [`RedisBroker`].
//!
//! Enable one of the TLS features (`tls-rustls`, `tls-rustls-ring`, or `tls-native-tls`) to use
//! these builders.
//!
//! ```text
//! cargo run --example fred_tls --features tls-rustls
//! ```

use ruststream_fred::{RedisBroker, TlsConnector};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:tls]
    // System trust roots, no client certificate. The same TlsConnector works on every topology.
    let tls = TlsConnector::default_rustls()?;
    let _broker = RedisBroker::cluster(["10.0.0.1:6379"]).tls(tls);
    // --8<-- [end:tls]

    // Standalone brokers can also enable TLS via a rediss:// / valkeys:// URL.
    let _broker = RedisBroker::standalone("rediss://localhost:6379");

    Ok(())
}
