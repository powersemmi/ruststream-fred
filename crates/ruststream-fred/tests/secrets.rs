//! A service logs its broker with `{:?}`; a password never reaches that output, wherever the
//! service put it, and every other setting does.

use ruststream_fred::RedisBroker;

/// Asserts that `printed` keeps `shown` and leaks none of `hidden`.
fn assert_redacted(printed: &str, shown: &[&str], hidden: &[&str]) {
    for value in shown {
        assert!(printed.contains(value), "{value} is shown: {printed}");
    }
    for value in hidden {
        assert!(!printed.contains(value), "{value} leaks: {printed}");
    }
}

#[test]
fn a_standalone_url_prints_no_password() {
    let broker = RedisBroker::standalone("redis://alice:url-secret@cache.internal:6379/2");
    assert_redacted(
        &format!("{broker:?}"),
        &["alice", "cache.internal:6379/2"],
        &["url-secret"],
    );
}

#[test]
fn a_password_only_url_prints_no_password() {
    let broker = RedisBroker::standalone("rediss://:only-secret@cache.internal:6380");
    assert_redacted(
        &format!("{broker:?}"),
        &["cache.internal:6380"],
        &["only-secret"],
    );
}

/// A password holding an `@` of its own is where a split on the first separator goes wrong.
#[test]
fn a_password_with_an_at_sign_prints_no_part_of_it() {
    let broker = RedisBroker::standalone("redis://alice:p@ss-secret@cache.internal:6379");
    assert_redacted(
        &format!("{broker:?}"),
        &["alice", "cache.internal:6379"],
        &["ss-secret", "p@ss", "p%40ss"],
    );
}

/// `fred` reads a sentinel URL's own password from its query.
#[test]
fn a_sentinel_url_prints_no_sentinel_password() {
    let broker = RedisBroker::standalone(
        "redis-sentinel://alice:data-secret@sentinel.internal:26379/0\
         ?sentinelServiceName=mymaster&sentinelUsername=watcher&sentinelPassword=watch-secret",
    );
    assert_redacted(
        &format!("{broker:?}"),
        &["alice", "mymaster", "watcher", "sentinelPassword"],
        &["data-secret", "watch-secret"],
    );
}

#[test]
fn seed_addresses_print_no_password() {
    let cluster = RedisBroker::cluster(["redis://bob:seed-secret@node1:7000", "node2:7001"]);
    assert_redacted(
        &format!("{cluster:?}"),
        &["bob", "node1:7000", "node2:7001"],
        &["seed-secret"],
    );

    let sentinel = RedisBroker::sentinel("mymaster", ["carol:seed-secret@sentinel1:26379"]);
    assert_redacted(
        &format!("{sentinel:?}"),
        &["mymaster", "carol", "sentinel1:26379"],
        &["seed-secret"],
    );
}

#[test]
fn credentials_set_on_the_broker_print_no_password() {
    let broker = RedisBroker::cluster(["node1:7000"]).credentials("alice", "acl-secret");
    assert_redacted(&format!("{broker:?}"), &["alice"], &["acl-secret"]);

    let broker = RedisBroker::sentinel("mymaster", ["sentinel1:26379"]).password("legacy-secret");
    assert_redacted(&format!("{broker:?}"), &["mymaster"], &["legacy-secret"]);
}

#[cfg(feature = "sentinel-auth")]
#[test]
fn sentinel_credentials_print_no_password() {
    let broker = RedisBroker::sentinel("mymaster", ["sentinel1:26379"])
        .sentinel_credentials("watcher", "watch-secret");
    assert_redacted(&format!("{broker:?}"), &["watcher"], &["watch-secret"]);
}
