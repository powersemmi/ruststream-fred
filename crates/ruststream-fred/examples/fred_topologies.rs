//! The three topologies [`RedisBroker`] connects to.
//!
//! Each constructor is synchronous and does no I/O: it records the addresses, and the runtime
//! dials them when the app starts.

use ruststream_fred::RedisBroker;

fn main() {
    // --8<-- [start:topologies]
    // A single server, addressed by URL.
    let _standalone = RedisBroker::standalone("redis://localhost:6379");

    // A cluster: one reachable seed node is enough, the rest is discovered.
    let _cluster = RedisBroker::cluster(["127.0.0.1:7000", "127.0.0.1:7001"]);

    // Sentinel: the monitored primary's name, then the sentinels that watch it.
    let _sentinel = RedisBroker::sentinel("mymaster", ["127.0.0.1:26379"]);
    // --8<-- [end:topologies]
}
