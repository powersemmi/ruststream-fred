//! An envelope as the broker's route table holds it, beside envelopes of other types.
//!
//! The default publisher writes each name the way the subscription reading it reads, and which
//! envelope that is depends on a subscription that opens at runtime, so the table holds a value
//! that names the envelope instead of its type. Every value it can hold is written by a direct
//! call: the broker's own envelope through the publisher's type parameter, and each built-in
//! envelope by its variant. An envelope that is neither, a service's own type on a subscription
//! of a broker with another envelope, is written only through a policy that names it.

use std::any::{Any, TypeId, type_name};
#[cfg(feature = "testing")]
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

#[cfg(feature = "testing")]
use bytes::Bytes;
use ruststream::HeaderMap;
#[cfg(feature = "cbor")]
use ruststream::codec::CborCodec;
#[cfg(feature = "json")]
use ruststream::codec::JsonCodec;
#[cfg(feature = "msgpack")]
use ruststream::codec::MsgpackCodec;

#[cfg(any(feature = "json", feature = "cbor", feature = "msgpack"))]
use super::Framed;
#[cfg(feature = "testing")]
use super::{Body, EntryBody};
use super::{Envelope, FastStream, NoOp, RustStream};
use crate::error::RedisError;

/// The envelope a subscription recorded for the names it reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Framing {
    /// The broker's own envelope, whatever its type.
    Own,
    RustStream,
    FastStream,
    NoOp,
    #[cfg(feature = "json")]
    FramedJson,
    #[cfg(feature = "cbor")]
    FramedCbor,
    #[cfg(feature = "msgpack")]
    FramedMsgpack,
    /// A service's own envelope on a subscription of a broker with another one: the default
    /// publisher has no type to write it with.
    Unrouted(Unrouted),
}

/// The type of an envelope the default publisher does not write.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Unrouted(&'static str);

impl Debug for Unrouted {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

/// Runs `$call` on the envelope behind `$framing`, by type, the broker's own being `$own`; an
/// unrouted envelope answers `$unrouted`.
macro_rules! dispatch {
    ($framing:expr, $own:expr, $envelope:ident => $call:expr, $unrouted:ident => $refusal:expr) => {
        match $framing {
            Framing::Own => {
                let $envelope = $own;
                $call
            }
            Framing::RustStream => {
                let $envelope = &RustStream;
                $call
            }
            Framing::FastStream => {
                let $envelope = &FastStream;
                $call
            }
            Framing::NoOp => {
                let $envelope = &NoOp;
                $call
            }
            #[cfg(feature = "json")]
            Framing::FramedJson => {
                let $envelope = &Framed::new(JsonCodec);
                $call
            }
            #[cfg(feature = "cbor")]
            Framing::FramedCbor => {
                let $envelope = &Framed::new(CborCodec);
                $call
            }
            #[cfg(feature = "msgpack")]
            Framing::FramedMsgpack => {
                let $envelope = &Framed::new(MsgpackCodec);
                $call
            }
            Framing::Unrouted($unrouted) => $refusal,
        }
    };
}

impl Framing {
    /// How the table holds `envelope` on a broker whose own envelope is `Own`.
    pub(crate) fn of<Own: Envelope, Env: Envelope>(envelope: &Env) -> Self {
        if TypeId::of::<Env>() == TypeId::of::<Own>() {
            return Self::Own;
        }
        let any: &dyn Any = envelope;
        if any.is::<RustStream>() {
            return Self::RustStream;
        }
        if any.is::<FastStream>() {
            return Self::FastStream;
        }
        if any.is::<NoOp>() {
            return Self::NoOp;
        }
        #[cfg(feature = "json")]
        if any.is::<Framed<JsonCodec>>() {
            return Self::FramedJson;
        }
        #[cfg(feature = "cbor")]
        if any.is::<Framed<CborCodec>>() {
            return Self::FramedCbor;
        }
        #[cfg(feature = "msgpack")]
        if any.is::<Framed<MsgpackCodec>>() {
            return Self::FramedMsgpack;
        }
        Self::Unrouted(Unrouted(type_name::<Env>()))
    }

    /// The value `payload` and `headers` make for `destination`.
    pub(crate) fn seal<Own: Envelope>(
        self,
        own: &Own,
        destination: &str,
        payload: &[u8],
        headers: &HeaderMap,
    ) -> Result<Vec<u8>, RedisError> {
        let mut value = Vec::with_capacity(payload.len());
        dispatch!(self, own, envelope => envelope.seal(payload, headers, &mut value)
            .map_err(|err| RedisError::seal(destination, err)),
            unrouted => Err(refusal(destination, unrouted)))?;
        Ok(value)
    }

    /// The stream entry `payload` and `headers` make for `destination`.
    pub(crate) fn seal_entry<Own: Envelope>(
        self,
        own: &Own,
        destination: &str,
        payload: Vec<u8>,
        headers: &HeaderMap,
    ) -> Result<Vec<(String, Vec<u8>)>, RedisError> {
        dispatch!(self, own, envelope => envelope.seal_entry(payload, headers)
            .map_err(|err| RedisError::seal(destination, err)),
            unrouted => Err(refusal(destination, unrouted)))
    }

    /// A value read through this envelope, as payload and headers; an unrouted envelope reads
    /// it whole.
    #[cfg(feature = "testing")]
    // The same call reaches the broker's envelope, which may not be `Copy`, and the built-in ones,
    // which are; a clone of a unit struct is free.
    #[allow(
        clippy::clone_on_copy,
        reason = "one call for every envelope the macro expands"
    )]
    pub(crate) fn open<Own: Envelope>(self, own: &Own, value: Bytes) -> (Bytes, HeaderMap) {
        dispatch!(self, own, envelope => open(envelope.clone(), value),
            _unrouted => (value, HeaderMap::new()))
    }

    /// An entry read through this envelope, as payload and headers.
    #[cfg(feature = "testing")]
    // The same call reaches the broker's envelope, which may not be `Copy`, and the built-in ones,
    // which are; a clone of a unit struct is free.
    #[allow(
        clippy::clone_on_copy,
        reason = "one call for every envelope the macro expands"
    )]
    pub(crate) fn open_entry<Own: Envelope>(
        self,
        own: &Own,
        fields: HashMap<String, Vec<u8>>,
    ) -> (Bytes, HeaderMap) {
        dispatch!(self, own, envelope => open_entry(envelope.clone(), fields),
            _unrouted => open_entry(NoOp, fields))
    }
}

/// The refusal of a publish the default publisher cannot write.
#[cold]
fn refusal(destination: &str, unrouted: Unrouted) -> RedisError {
    RedisError::Unrouted {
        destination: destination.to_owned(),
        envelope: unrouted.0,
    }
}

/// A value read through `envelope`, as payload and headers.
#[cfg(feature = "testing")]
fn open<Env: Envelope>(envelope: Env, value: Bytes) -> (Bytes, HeaderMap) {
    let body = Body::open(envelope, value);
    (
        Bytes::copy_from_slice(body.payload()),
        body.headers("").clone(),
    )
}

/// An entry read through `envelope`, as payload and headers.
#[cfg(feature = "testing")]
fn open_entry<Env: Envelope>(
    envelope: Env,
    fields: HashMap<String, Vec<u8>>,
) -> (Bytes, HeaderMap) {
    let body = EntryBody::open(envelope, fields, None);
    (
        Bytes::copy_from_slice(body.payload()),
        body.headers("").clone(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{Location, SealError};

    /// A custom envelope, as a service defines one.
    #[derive(Debug, Clone, Copy)]
    struct Custom;

    impl Envelope for Custom {
        const CONTENT_TYPE: &'static str = "text/plain";

        fn locate(&self, _value: &[u8]) -> Location {
            Location::Bare
        }

        fn headers(&self, _value: &[u8]) -> Option<HeaderMap> {
            Some(HeaderMap::new())
        }

        fn seal(
            &self,
            payload: &[u8],
            _: &HeaderMap,
            value: &mut Vec<u8>,
        ) -> Result<(), SealError> {
            value.extend_from_slice(payload);
            Ok(())
        }
    }

    #[test]
    fn the_brokers_own_envelope_and_the_built_ins_are_held_by_variant() {
        assert_eq!(Framing::of::<FastStream, _>(&FastStream), Framing::Own);
        assert_eq!(Framing::of::<Custom, _>(&Custom), Framing::Own);
        assert_eq!(
            Framing::of::<RustStream, _>(&FastStream),
            Framing::FastStream
        );
        assert_eq!(Framing::of::<RustStream, _>(&NoOp), Framing::NoOp);
        assert_eq!(
            Framing::of::<FastStream, _>(&RustStream),
            Framing::RustStream
        );
        #[cfg(feature = "json")]
        assert_eq!(
            Framing::of::<RustStream, _>(&Framed::new(JsonCodec)),
            Framing::FramedJson
        );
    }

    /// A service's own envelope beside a broker with another is written only through a policy
    /// that names it: the default publisher refuses the name, and the error names both.
    #[test]
    fn a_services_own_envelope_beside_another_broker_envelope_is_refused_by_name() {
        let framing = Framing::of::<RustStream, _>(&Custom);

        let refused = framing.seal(&RustStream, "jobs", b"{}", &HeaderMap::new());

        let text = refused
            .expect_err("the default publisher cannot write it")
            .to_string();
        assert!(text.contains("`jobs`") && text.contains("Custom"), "{text}");
    }

    #[test]
    fn a_held_envelope_writes_as_its_type_does() {
        let mut headers = HeaderMap::new();
        headers.insert("tenant", "acme");
        let mut direct = Vec::new();
        Envelope::seal(&FastStream, b"{}", &headers, &mut direct).expect("the frame");

        let held = Framing::FastStream
            .seal(&RustStream, "jobs", b"{}", &headers)
            .expect("the frame");

        assert_eq!(held, direct);
    }
}
