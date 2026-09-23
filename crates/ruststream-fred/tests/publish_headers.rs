//! What carrying a header map through a publish costs.
//!
//! The publisher is handed the map the transforms filled, and the only copy of it a publish may
//! make is the one the published log keeps. Address equality cannot say whether a map was moved
//! or copied - its values are `Bytes`, which keep their data pointer across a clone - so the
//! measurement is this thread's allocation count around one publish.
#![cfg(feature = "testing")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use bytes::Bytes;
use ruststream::{Broker, BytesMut, ConnectedBroker, HeaderMap, OutgoingMessage, Publisher};
use ruststream_fred::testing::{RedisTestBroker, RedisTestPublisher};

/// Counts this thread's allocations. A thread-local count rather than a global one: the test
/// binary runs other tests beside this one, and their allocations are none of this
/// measurement's business.
struct Counting;

thread_local! {
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.with(|count| count.set(count.get() + 1));
        // SAFETY: the layout is the caller's, forwarded unchanged to the system allocator.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the pointer and layout are the caller's, forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// What this thread has allocated so far.
fn allocations() -> usize {
    ALLOCATIONS.with(Cell::get)
}

fn two_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("x-tenant", Bytes::from_static(b"acme"));
    headers.insert("x-region", Bytes::from_static(b"eu-central-1"));
    headers
}

/// The allocations one publish to `key` costs, with the log entry for that key already in place:
/// the first message under a name grows tables that later ones do not.
async fn one_publish(publisher: &RedisTestPublisher, key: &str, headers: HeaderMap) -> usize {
    let warmup =
        OutgoingMessage::produced(key, BytesMut::from(&b"{}"[..])).with_headers(headers.clone());
    publisher.publish(warmup, None).await.expect("publish");

    let measured = OutgoingMessage::produced(key, BytesMut::from(&b"{}"[..])).with_headers(headers);
    let before = allocations();
    publisher.publish(measured, None).await.expect("publish");
    allocations() - before
}

/// A publish carrying two headers costs exactly one copy of the map more than a publish carrying
/// none, and that copy is the published log's snapshot. A publisher that cloned the map on the
/// way in would cost two.
#[tokio::test]
async fn a_publish_copies_the_header_map_once() {
    let broker = RedisTestBroker::new().connect().await.expect("connect");
    let publisher = broker.publisher();

    let headers = two_headers();
    // The first map copied in this process brings the hash table's own machinery up; what a copy
    // costs from then on is the table.
    drop(headers.clone());
    let before = allocations();
    let copy = headers.clone();
    let one_copy = allocations() - before;
    drop(copy);

    let bare = one_publish(&publisher, "events.plain", HeaderMap::new()).await;
    let carried = one_publish(&publisher, "events.other", headers).await;

    assert_eq!(
        carried - bare,
        one_copy,
        "the publisher hands the map it was given to the router; only the published log keeps a \
         copy of its own",
    );
    broker.shutdown().await.expect("shutdown");
}
