//! An envelope as the broker's route table holds it, beside envelopes of other types.
//!
//! The default publisher writes each name the way the subscription reading it reads, and which
//! envelope that is depends on a subscription that opens at runtime: the table cannot hold the
//! type itself. The built-in envelopes are told apart by a variant and written with a direct call;
//! any other envelope sits behind a shared trait object, a dynamic call on the default publisher's
//! path to a name read through it. A publish through a policy names its envelope by type and never
//! comes here.

use std::any::{Any, type_name};
#[cfg(feature = "testing")]
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[cfg(feature = "testing")]
use bytes::Bytes;
use ruststream::HeaderMap;

#[cfg(feature = "testing")]
use super::{Body, EntryBody};
use super::{Envelope, FastStream, NoOp, RustStream, SealError};

/// An envelope a subscription recorded for the names it reads.
#[derive(Clone)]
pub(crate) enum Framing {
    RustStream,
    FastStream,
    NoOp,
    Other(Arc<dyn Erased>),
}

/// The object-safe face of an [`Envelope`].
pub(crate) trait Erased: Send + Sync {
    fn name(&self) -> &'static str;
    fn seal(
        &self,
        payload: &[u8],
        headers: &HeaderMap,
        value: &mut Vec<u8>,
    ) -> Result<(), SealError>;
    fn seal_entry(
        &self,
        payload: Vec<u8>,
        headers: &HeaderMap,
    ) -> Result<Vec<(String, Vec<u8>)>, SealError>;
    #[cfg(feature = "testing")]
    fn open(&self, value: Bytes) -> (Bytes, HeaderMap);
    #[cfg(feature = "testing")]
    fn open_entry(&self, fields: HashMap<String, Vec<u8>>) -> (Bytes, HeaderMap);
}

impl<Env: Envelope> Erased for Env {
    fn name(&self) -> &'static str {
        type_name::<Env>()
    }

    fn seal(
        &self,
        payload: &[u8],
        headers: &HeaderMap,
        value: &mut Vec<u8>,
    ) -> Result<(), SealError> {
        Envelope::seal(self, payload, headers, value)
    }

    fn seal_entry(
        &self,
        payload: Vec<u8>,
        headers: &HeaderMap,
    ) -> Result<Vec<(String, Vec<u8>)>, SealError> {
        Envelope::seal_entry(self, payload, headers)
    }

    #[cfg(feature = "testing")]
    fn open(&self, value: Bytes) -> (Bytes, HeaderMap) {
        open(self.clone(), value)
    }

    #[cfg(feature = "testing")]
    fn open_entry(&self, fields: HashMap<String, Vec<u8>>) -> (Bytes, HeaderMap) {
        open_entry(self.clone(), fields)
    }
}

/// A value read through `envelope`, as payload and headers.
#[cfg(feature = "testing")]
pub(crate) fn open<Env: Envelope>(envelope: Env, value: Bytes) -> (Bytes, HeaderMap) {
    let body = Body::open(envelope, value);
    (
        Bytes::copy_from_slice(body.payload()),
        body.headers().clone(),
    )
}

/// An entry read through `envelope`, as payload and headers.
#[cfg(feature = "testing")]
pub(crate) fn open_entry<Env: Envelope>(
    envelope: Env,
    fields: HashMap<String, Vec<u8>>,
) -> (Bytes, HeaderMap) {
    let body = EntryBody::open(envelope, fields, None);
    (
        Bytes::copy_from_slice(body.payload()),
        body.headers().clone(),
    )
}

/// Runs `$call` on the envelope behind `$framing`, by type for the built-in ones.
macro_rules! dispatch {
    ($framing:expr, $envelope:ident => $call:expr) => {
        match $framing {
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
            Framing::Other(other) => {
                let $envelope = &**other;
                $call
            }
        }
    };
}

impl Framing {
    /// How the table holds `envelope`.
    pub(crate) fn of<Env: Envelope>(envelope: &Env) -> Self {
        let any: &dyn Any = envelope;
        if any.is::<RustStream>() {
            Self::RustStream
        } else if any.is::<FastStream>() {
            Self::FastStream
        } else if any.is::<NoOp>() {
            Self::NoOp
        } else {
            Self::Other(Arc::new(envelope.clone()))
        }
    }

    /// The envelope's type, which tells two framings apart.
    pub(crate) fn name(&self) -> &'static str {
        dispatch!(self, envelope => Erased::name(envelope))
    }

    /// The value `payload` and `headers` make.
    pub(crate) fn seal(&self, payload: &[u8], headers: &HeaderMap) -> Result<Vec<u8>, SealError> {
        let mut value = Vec::with_capacity(payload.len());
        dispatch!(self, envelope => Erased::seal(envelope, payload, headers, &mut value))?;
        Ok(value)
    }

    /// The stream entry `payload` and `headers` make.
    pub(crate) fn seal_entry(
        &self,
        payload: Vec<u8>,
        headers: &HeaderMap,
    ) -> Result<Vec<(String, Vec<u8>)>, SealError> {
        dispatch!(self, envelope => Erased::seal_entry(envelope, payload, headers))
    }

    /// A value read through this envelope, as payload and headers.
    #[cfg(feature = "testing")]
    pub(crate) fn open(&self, value: Bytes) -> (Bytes, HeaderMap) {
        dispatch!(self, envelope => Erased::open(envelope, value))
    }

    /// An entry read through this envelope, as payload and headers.
    #[cfg(feature = "testing")]
    pub(crate) fn open_entry(&self, fields: HashMap<String, Vec<u8>>) -> (Bytes, HeaderMap) {
        dispatch!(self, envelope => Erased::open_entry(envelope, fields))
    }
}

impl Debug for Framing {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl PartialEq for Framing {
    fn eq(&self, other: &Self) -> bool {
        self.name() == other.name()
    }
}

#[cfg(test)]
mod tests {
    use ruststream::codec::JsonCodec;

    use super::*;
    use crate::envelope::Framed;

    #[test]
    fn the_built_in_envelopes_are_held_by_variant() {
        assert!(matches!(Framing::of(&RustStream), Framing::RustStream));
        assert!(matches!(Framing::of(&FastStream), Framing::FastStream));
        assert!(matches!(Framing::of(&NoOp), Framing::NoOp));
        assert!(matches!(
            Framing::of(&Framed::new(JsonCodec)),
            Framing::Other(_)
        ));
    }

    /// Two framings are one when their envelopes are of one type.
    #[test]
    fn framings_compare_by_type() {
        assert_eq!(Framing::of(&FastStream), Framing::FastStream);
        assert_eq!(
            Framing::of(&Framed::new(JsonCodec)),
            Framing::of(&Framed::new(JsonCodec))
        );
        assert_ne!(Framing::of(&Framed::new(JsonCodec)), Framing::RustStream);
    }

    #[test]
    fn a_held_envelope_writes_as_its_type_does() {
        let mut headers = HeaderMap::new();
        headers.insert("tenant", "acme");
        let framed = Framed::new(JsonCodec);
        let mut direct = Vec::new();
        Envelope::seal(&framed, b"{}", &headers, &mut direct).expect("the document");

        let held = Framing::of(&framed)
            .seal(b"{}", &headers)
            .expect("the document");

        assert_eq!(held, direct);
    }
}
