//! {{project-name}} - a RustStream service over Redis Pub/Sub.
//!
//! Handlers live in `orders`, wiring in `routes`; `#[ruststream::app]` generates `main`, so there
//! is no runtime boilerplate to maintain:
//!
//! - `cargo run -- run` (or `ruststream run`) starts the service until interrupted.
//! - `cargo run -- asyncapi gen` (or `ruststream asyncapi gen`) prints the AsyncAPI document.
//!
//! `RedisBroker::standalone` is synchronous and does no I/O, so it slots into the builder; the
//! runtime opens the connection once at startup. Pub/Sub is fire-and-forget: no durability, no
//! consumer groups, no acknowledgement. Start a Redis server first, for example
//! `docker run -p 6379:6379 redis:7`.

mod orders;
mod routes;

use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream_fred::RedisBroker;

/// Builds the service: one Redis broker with the events router mounted.
#[ruststream::app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("{{project-name}}", "0.1.0")).with_broker(
        RedisBroker::standalone("redis://localhost:6379"),
        |b| {
            b.include_router(routes::events());
        },
    )
}
