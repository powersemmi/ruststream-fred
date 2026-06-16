//! Authentication examples for [`RedisBroker`].
//!
//! `RedisBroker` carries credentials independently of the connection URL, so the same builder
//! works for standalone URLs, cluster seed lists, and sentinel seed lists.

use ruststream_fred::RedisBroker;

fn main() {
    // --8<-- [start:credentials]
    // ACL username + password on a cluster. Bare seed lists cannot encode credentials, so the
    // builder is the only way to authenticate a cluster or sentinel topology.
    let _broker = RedisBroker::cluster(["10.0.0.1:6379"]).credentials("worker", "s3cr3t");
    // --8<-- [end:credentials]

    // --8<-- [start:password]
    // Password-only AUTH (legacy requirepass, no ACL user) on a sentinel topology.
    let _broker = RedisBroker::sentinel("mymaster", ["10.0.0.1:26379"]).password("s3cr3t");
    // --8<-- [end:password]
}
