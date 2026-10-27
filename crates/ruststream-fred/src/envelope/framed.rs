//! Headers and payload as one document through a `RustStream` codec.

use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};

use bytes::Bytes;
use ruststream::HeaderMap;
use ruststream::codec::Codec;
use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use super::{Envelope, Location, SealError};

/// Headers and payload as one `{headers, payload}` document, written through a `RustStream` codec.
///
/// With the JSON codec the value is text that `redis-cli` and `RedisInsight` show as it is. A
/// field whose bytes are valid UTF-8 is written as text and any other bytes as the byte sequence
/// itself, so a payload arrives exactly as it left, and the CBOR and msgpack codecs carry the
/// document the same way. A value that is not a document in exactly this shape, one with a field
/// of its own beside the two included, is delivered as the payload with no headers. The
/// document is the one the `codec(..)` framing of earlier releases wrote.
///
/// Reading the payload decodes the document once, and reading the headers decodes it again.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::codec::JsonCodec;
/// use ruststream_fred::list::prelude::*;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// #[derive(Serialize, Outgoing)]
/// #[outgoing(name = "jobs")]
/// struct Job {
///     order: u64,
/// }
///
/// #[subscriber(RedisList::new("orders"), reply)]
/// async fn plan(order: &Order) -> Job {
///     Job { order: order.id }
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("orders", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             // The job is pushed as a JSON document a person reads in `redis-cli`.
///             b.include(plan.envelope(Framed::new(JsonCodec)))
///                 .out_reply(Publish::new().envelope(Framed::new(JsonCodec)));
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Framed<C>(C);

impl<C> Framed<C> {
    /// The document written through `codec`.
    pub const fn new(codec: C) -> Self {
        Self(codec)
    }
}

impl<C> Envelope for Framed<C>
where
    C: Codec + Clone + Debug + 'static,
{
    const CONTENT_TYPE: &'static str = C::CONTENT_TYPE;

    fn locate(&self, value: &[u8]) -> Location {
        self.0
            .decode::<Probe>(value)
            .map_or(Location::Bare, |probe| {
                Location::Decoded(probe.payload.into_vec())
            })
    }

    fn headers(&self, value: &[u8]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if let Ok(document) = self.0.decode::<Document>(value) {
            for (name, header) in document.headers {
                headers.insert(name, header.into_bytes());
            }
        }
        headers
    }

    fn seal(
        &self,
        payload: &[u8],
        headers: &HeaderMap,
        value: &mut Vec<u8>,
    ) -> Result<(), SealError> {
        let document = Document {
            headers: headers
                .iter()
                .map(|(name, header)| (name.to_owned(), Content::from_bytes(header)))
                .collect(),
            payload: Content::from_bytes(payload),
        };
        let encoded = self
            .0
            .encode(&document)
            .map_err(|err| SealError::Encode(Box::new(err)))?;
        value.extend_from_slice(&encoded);
        Ok(())
    }
}

/// One document field: text when its bytes are valid UTF-8, the bytes themselves otherwise.
///
/// The representation is untagged, so a text field is serialized as the plain string it always
/// was and nothing distinguishes a document of text fields from one an earlier release wrote.
/// Bytes that are not text keep their own form (a JSON array of numbers, a byte string in a
/// binary codec) instead of passing through a lossy conversion to text.
#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum Content {
    Text(String),
    Bytes(Vec<u8>),
}

impl Content {
    fn from_bytes(bytes: &[u8]) -> Self {
        std::str::from_utf8(bytes).map_or_else(
            |_| Self::Bytes(bytes.to_vec()),
            |text| Self::Text(text.to_owned()),
        )
    }

    /// Takes the bytes over without a copy: text stays the buffer it arrived in.
    pub(crate) fn from_vec(bytes: Vec<u8>) -> Self {
        String::from_utf8(bytes).map_or_else(|err| Self::Bytes(err.into_bytes()), Self::Text)
    }

    fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Text(text) => text.into_bytes(),
            Self::Bytes(bytes) => bytes,
        }
    }

    fn into_bytes(self) -> Bytes {
        Bytes::from(self.into_vec())
    }
}

/// The document, read only in the exact shape it is written in, so a value another client wrote
/// that only resembles it (a `payload` field alone, or fields of its own beside the two) is the
/// plain payload it is.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    headers: BTreeMap<String, Content>,
    payload: Content,
}

/// The document with its headers skipped over, which is what finding the payload reads.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Probe {
    #[allow(dead_code, reason = "present for the shape check, never read")]
    headers: AnyMap,
    payload: Content,
}

/// A map whose entries are skipped over without being decoded.
struct AnyMap;

impl<'de> Deserialize<'de> for AnyMap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Skip;

        impl<'de> Visitor<'de> for Skip {
            type Value = AnyMap;

            fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.write_str("a map of headers")
            }

            fn visit_map<Entries: MapAccess<'de>>(
                self,
                mut entries: Entries,
            ) -> Result<AnyMap, Entries::Error> {
                while entries.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(AnyMap)
            }
        }

        deserializer.deserialize_map(Skip)
    }
}

#[cfg(test)]
mod tests {
    use ruststream::codec::{CborCodec, JsonCodec, MsgpackCodec};

    use super::*;

    fn sample_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");
        headers.insert("correlation-id", "abc-1");
        headers
    }

    fn sealed<C: Codec + Clone + Debug + 'static>(
        envelope: &Framed<C>,
        payload: &[u8],
        headers: &HeaderMap,
    ) -> Vec<u8> {
        let mut value = Vec::new();
        envelope
            .seal(payload, headers, &mut value)
            .expect("the codec writes the document");
        value
    }

    fn payload_of<C: Codec + Clone + Debug + 'static>(
        envelope: &Framed<C>,
        value: &[u8],
    ) -> Vec<u8> {
        match envelope.locate(value) {
            Location::Bare => value.to_vec(),
            Location::Decoded(payload) => payload,
            Location::Within(range) => value[range].to_vec(),
        }
    }

    #[test]
    fn a_document_round_trips_and_is_readable() {
        let json = Framed::new(JsonCodec);
        let framed = sealed(&json, br#"{"id":1}"#, &sample_headers());
        let text = String::from_utf8(framed.clone()).expect("utf8");
        assert!(text.contains("\"payload\""));
        assert!(text.contains("application/json"));

        assert_eq!(payload_of(&json, &framed), br#"{"id":1}"#);
        assert_eq!(
            json.headers(&framed).content_type(),
            Some("application/json")
        );
    }

    #[test]
    fn a_value_that_is_not_a_document_is_bare() {
        assert_eq!(Framed::new(JsonCodec).locate(b"not-json"), Location::Bare);
    }

    /// The document is read only in the exact shape this crate writes: a JSON value another
    /// client wrote with a `payload` field and nothing else, or with fields of its own, is the
    /// payload it is.
    #[test]
    fn a_json_value_shaped_almost_like_the_document_is_bare() {
        for foreign in [
            br#"{"payload":"x"}"#.as_slice(),
            br#"{"headers":{},"payload":"x","id":7}"#.as_slice(),
        ] {
            assert_eq!(Framed::new(JsonCodec).locate(foreign), Location::Bare);
        }
    }

    /// The wire form of a text payload is the one the `codec(..)` framing of earlier releases
    /// wrote, so values already in Redis keep reading.
    #[test]
    fn a_text_document_keeps_its_wire_form() {
        let framed = sealed(&Framed::new(JsonCodec), br#"{"id":1}"#, &sample_headers());
        let text = String::from_utf8(framed).expect("utf8");

        assert!(text.contains(r#""payload":"{\"id\":1}""#), "{text}");
        assert!(
            text.contains(r#""content-type":"application/json""#),
            "{text}"
        );
    }

    #[test]
    fn a_document_from_an_earlier_release_still_reads() {
        let json = Framed::new(JsonCodec);
        let wire = br#"{"headers":{"content-type":"application/json"},"payload":"{\"id\":1}"}"#;

        assert_eq!(payload_of(&json, wire), br#"{"id":1}"#);
        assert_eq!(json.headers(wire).content_type(), Some("application/json"));
    }

    /// A header value is written the way the payload is, so a binary one survives too.
    #[test]
    fn a_header_value_that_is_not_text_round_trips() {
        let json = Framed::new(JsonCodec);
        let mut headers = HeaderMap::new();
        headers.insert("signature", Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]));

        let framed = sealed(&json, b"{}", &headers);

        assert_eq!(
            json.headers(&framed).get("signature").map(<[u8]>::to_vec),
            Some(vec![0xde, 0xad, 0xbe, 0xef])
        );
    }

    /// The document carries whatever codec a service picked, so the untagged text-or-bytes field
    /// has to survive each one, not only JSON.
    #[test]
    fn every_codec_round_trips_bytes_that_are_not_text() {
        let blob: &[u8] = &[0xff, 0x00, 0xfe];

        let json = Framed::new(JsonCodec);
        let framed = sealed(&json, blob, &sample_headers());
        assert_eq!(payload_of(&json, &framed), blob, "json");
        assert_eq!(
            json.headers(&framed).content_type(),
            Some("application/json")
        );

        let cbor = Framed::new(CborCodec);
        let framed = sealed(&cbor, blob, &sample_headers());
        assert_eq!(payload_of(&cbor, &framed), blob, "cbor");
        assert_eq!(
            cbor.headers(&framed).content_type(),
            Some("application/json")
        );

        let msgpack = Framed::new(MsgpackCodec);
        let framed = sealed(&msgpack, blob, &sample_headers());
        assert_eq!(payload_of(&msgpack, &framed), blob, "msgpack");
        assert_eq!(
            msgpack.headers(&framed).content_type(),
            Some("application/json")
        );
    }

    #[test]
    fn the_content_type_is_the_codecs() {
        assert_eq!(
            <Framed<JsonCodec> as Envelope>::CONTENT_TYPE,
            "application/json"
        );
    }
}
