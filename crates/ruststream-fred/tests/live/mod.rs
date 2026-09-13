//! The gate the live suites share: when a run is supposed to reach a server, a skip is a defect.
//!
//! A live test skips when its topology's URL is unset, which is what keeps the suites usable
//! during development: `cargo test` on a laptop with no stand passes. The same skip in CI is a
//! lie, because the job started the servers first, and a test that returns before its first
//! assertion reports `ok` exactly like one that ran. The broker job therefore sets
//! `RUSTSTREAM_REQUIRE_LIVE`, and under that flag every skip becomes a failure naming the
//! variable it wanted.

/// The variable a job sets to say it stood the servers up, so skipping past them is a defect.
pub(crate) const REQUIRE_LIVE: &str = "RUSTSTREAM_REQUIRE_LIVE";

/// Whether this run is required to reach a live server.
fn required() -> bool {
    std::env::var(REQUIRE_LIVE).is_ok_and(|value| !value.is_empty())
}

/// The server URL from `name`, or `None` to skip the test.
///
/// # Panics
///
/// Panics when [`REQUIRE_LIVE`] is set and `name` is not: a job that started a server and then
/// lost its address is a broken job, and the tests behind that address would have passed without
/// ever reaching it.
pub(crate) fn url(name: &str) -> Option<String> {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Some(value),
        _ => {
            assert!(
                !required(),
                "{REQUIRE_LIVE} is set, so this suite must run, but {name} is unset or empty",
            );
            None
        }
    }
}
