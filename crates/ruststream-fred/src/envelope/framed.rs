//! Headers and payload as one document through a `RustStream` codec, the payload in the
//! convention of the `CloudEvents` JSON format.

use std::collections::BTreeMap;
use std::fmt::{self, Debug, Formatter};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ruststream::HeaderMap;
use ruststream::codec::Codec;
use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;

use super::{Envelope, Location, SealError};

/// The media type of a JSON codec, which writes the document as JSON text.
const JSON: &str = "application/json";

/// Headers and payload as one document, `{headers, payload}`, written through a `RustStream`
/// codec.
///
/// The payload follows the convention of the `CloudEvents` JSON format. With a JSON codec a payload
/// that is a JSON text is embedded as the JSON value it is, and any other bytes travel as standard
/// base64 in `payload_base64`; exactly one of the two fields is present:
///
/// ```text
/// {"headers":{"event":"created"},"payload":{"id":7}}
/// {"headers":{"event":"created"},"payload_base64":"CAc="}
/// ```
///
/// With a binary codec (CBOR, msgpack) the payload is always a byte string in `payload`. A header
/// is a text field of `headers`, and a header whose value is not text refuses the publish. A value
/// that is not a document in exactly this shape is delivered as the payload with no headers.
///
/// A JSON payload reads back byte for byte, and finding it costs one scan of the document. A
/// publish through a JSON codec checks that the payload is a JSON text, one scan of it, and
/// encodes a payload that is not in base64. A broker whose own envelope is `Framed` registers
/// itself with the test harness beside its tests:
/// `ruststream::register_testable_broker!(RedisBroker<Framed<JsonCodec>>);`.
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
        if C::CONTENT_TYPE == JSON {
            return locate_json(value);
        }
        self.0
            .decode::<BinaryProbe>(value)
            .map_or(Location::Bare, |probe| Location::Decoded(probe.payload.0))
    }

    fn headers(&self, value: &[u8]) -> Option<HeaderMap> {
        let parsed = if C::CONTENT_TYPE == JSON {
            serde_json::from_slice::<JsonHeaders>(value)
                .ok()
                .map(|block| block.headers)
        } else {
            self.0
                .decode::<BinaryHeaders>(value)
                .ok()
                .map(|block| block.headers)
        }?;
        let mut headers = HeaderMap::new();
        for (name, text) in parsed {
            headers.insert(name, text);
        }
        Some(headers)
    }

    fn seal(
        &self,
        payload: &[u8],
        headers: &HeaderMap,
        value: &mut Vec<u8>,
    ) -> Result<(), SealError> {
        let headers = text_headers(headers)?;
        if C::CONTENT_TYPE == JSON {
            seal_json(payload, &headers, value);
            return Ok(());
        }
        let document = BinaryDocument {
            headers,
            payload: ByteString(payload),
        };
        let encoded = self
            .0
            .encode(&document)
            .map_err(|err| SealError::Encode(Box::new(err)))?;
        value.extend_from_slice(&encoded);
        Ok(())
    }
}

/// The headers as the text fields of the document; a value that is not text has no place there.
fn text_headers(headers: &HeaderMap) -> Result<BTreeMap<&str, &str>, SealError> {
    headers
        .iter()
        .map(|(name, value)| {
            std::str::from_utf8(value)
                .map(|text| (name, text))
                .map_err(|_| {
                    SealError::Encode(
                        format!(
                            "the header `{name}` is not text, and a document holds text headers"
                        )
                        .into(),
                    )
                })
        })
        .collect()
}

/// Writes the JSON document straight into `value`: the payload embedded as is when it is a JSON
/// text, its base64 otherwise. Neither form needs escaping, so no intermediate buffer is built.
fn seal_json(payload: &[u8], headers: &BTreeMap<&str, &str>, value: &mut Vec<u8>) {
    value.reserve(payload.len() + 32);
    value.extend_from_slice(br#"{"headers":"#);
    serde_json::to_writer(&mut *value, headers).expect("a map of strings serializes");
    // A JSON text with whitespace around it would read back without that whitespace, so only a
    // payload that is the value alone is embedded.
    let embeddable = serde_json::from_slice::<&RawValue>(payload)
        .is_ok_and(|raw| raw.get().len() == payload.len());
    if embeddable {
        value.extend_from_slice(br#","payload":"#);
        value.extend_from_slice(payload);
    } else {
        value.extend_from_slice(br#","payload_base64":""#);
        let start = value.len();
        value.resize(
            start + base64::encoded_len(payload.len(), true).unwrap_or(0),
            0,
        );
        let written = STANDARD
            .encode_slice(payload, &mut value[start..])
            .expect("the buffer is sized for the encoding");
        value.truncate(start + written);
        value.push(b'"');
    }
    value.push(b'}');
}

/// Where the payload of a JSON document is: the embedded value, a range of the document, or the
/// bytes its base64 decodes to.
fn locate_json(value: &[u8]) -> Location {
    let Ok(probe) = serde_json::from_slice::<JsonProbe<'_>>(value) else {
        return Location::Bare;
    };
    match (probe.payload, probe.payload_base64) {
        (Some(raw), None) => {
            let text = raw.get().as_bytes();
            // The value borrows the document, so its offset there is the distance between the two.
            let start = text.as_ptr() as usize - value.as_ptr() as usize;
            Location::Within(start..start + text.len())
        }
        (None, Some(encoded)) => STANDARD
            .decode(encoded)
            .map_or(Location::Bare, Location::Decoded),
        _ => Location::Bare,
    }
}

/// The JSON document with its headers skipped over, which is what finding the payload reads.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JsonProbe<'a> {
    #[allow(dead_code, reason = "present for the shape check, never read")]
    headers: AnyMap,
    #[serde(borrow, default)]
    payload: Option<&'a RawValue>,
    #[serde(borrow, default)]
    payload_base64: Option<&'a str>,
}

/// The binary document with its headers skipped over.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BinaryProbe {
    #[allow(dead_code, reason = "present for the shape check, never read")]
    headers: AnyMap,
    payload: ByteBuf,
}

/// The headers of a JSON document, its payload skipped over.
#[derive(Deserialize)]
struct JsonHeaders {
    headers: BTreeMap<String, String>,
}

/// The headers of a binary document, its payload skipped over. Both fields are named, since a
/// binary codec may write a struct as the sequence of its fields.
#[derive(Deserialize)]
struct BinaryHeaders {
    headers: BTreeMap<String, String>,
    #[allow(
        dead_code,
        reason = "present for a codec that reads fields by position"
    )]
    payload: IgnoredAny,
}

/// The document a binary codec writes.
#[derive(Serialize)]
struct BinaryDocument<'a> {
    headers: BTreeMap<&'a str, &'a str>,
    payload: ByteString<'a>,
}

/// Bytes a binary codec writes as a byte string.
struct ByteString<'a>(&'a [u8]);

impl Serialize for ByteString<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

/// A byte string a binary codec read.
struct ByteBuf(Vec<u8>);

impl<'de> Deserialize<'de> for ByteBuf {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Bytes;

        impl Visitor<'_> for Bytes {
            type Value = ByteBuf;

            fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.write_str("a byte string")
            }

            fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<ByteBuf, E> {
                Ok(ByteBuf(bytes.to_vec()))
            }

            fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<ByteBuf, E> {
                Ok(ByteBuf(bytes))
            }
        }

        deserializer.deserialize_byte_buf(Bytes)
    }
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
    use bytes::Bytes;
    use ruststream::codec::{CborCodec, JsonCodec, MsgpackCodec};

    use super::*;

    fn tenant() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("event", "created");
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
    ) -> Option<Vec<u8>> {
        match envelope.locate(value) {
            Location::Bare => None,
            Location::Decoded(payload) => Some(payload),
            Location::Within(range) => Some(value[range].to_vec()),
        }
    }

    /// A JSON payload is embedded as the JSON value it is, the way the `CloudEvents` JSON format
    /// carries `data`, and reads back byte for byte, whitespace included.
    #[test]
    fn a_json_payload_is_embedded_as_a_value_and_reads_back_byte_exact() {
        let json = Framed::new(JsonCodec);
        let payload = br#"{ "id": 7,  "tags":["a"] }"#;

        let framed = sealed(&json, payload, &tenant());

        assert_eq!(
            framed,
            br#"{"headers":{"event":"created"},"payload":{ "id": 7,  "tags":["a"] }}"#
        );
        assert_eq!(
            payload_of(&json, &framed).as_deref(),
            Some(payload.as_slice())
        );
        assert_eq!(
            json.headers(&framed)
                .expect("the header block parses")
                .get_str("event"),
            Some("created")
        );
    }

    /// Bytes that are not a JSON text travel as standard base64 in `payload_base64`, as the
    /// `CloudEvents` `data_base64` does.
    #[test]
    fn a_payload_that_is_not_json_travels_as_base64() {
        let json = Framed::new(JsonCodec);
        let payload: &[u8] = &[0x08, 0x07];

        let framed = sealed(&json, payload, &tenant());

        assert_eq!(
            framed,
            br#"{"headers":{"event":"created"},"payload_base64":"CAc="}"#
        );
        assert_eq!(payload_of(&json, &framed).as_deref(), Some(payload));
    }

    /// Text that is not a JSON value is no exception: it is bytes, so it is base64 too, and so is
    /// a JSON text with whitespace around it, which the document would not keep.
    #[test]
    fn plain_text_travels_as_base64() {
        let json = Framed::new(JsonCodec);

        let framed = sealed(&json, b"hello", &HeaderMap::new());
        assert_eq!(framed, br#"{"headers":{},"payload_base64":"aGVsbG8="}"#);

        let framed = sealed(&json, b" 7\n", &HeaderMap::new());
        assert_eq!(framed, br#"{"headers":{},"payload_base64":"IDcK"}"#);
        assert_eq!(
            payload_of(&json, &framed).as_deref(),
            Some(b" 7\n".as_slice())
        );
    }

    /// A binary codec carries the payload as a byte string in `payload`, whatever the bytes are.
    #[test]
    fn a_binary_codec_carries_the_payload_as_a_byte_string() {
        let cbor = Framed::new(CborCodec);
        let framed = sealed(&cbor, br#"{"id":7}"#, &tenant());

        let decoded: ciborium::Value = ciborium::from_reader(framed.as_slice()).expect("cbor");
        let payload = decoded
            .as_map()
            .and_then(|map| map.iter().find(|(key, _)| key.as_text() == Some("payload")))
            .map(|(_, value)| value.clone());
        assert_eq!(
            payload,
            Some(ciborium::Value::Bytes(br#"{"id":7}"#.to_vec()))
        );
        assert_eq!(
            payload_of(&cbor, &framed).as_deref(),
            Some(br#"{"id":7}"#.as_slice())
        );
        assert_eq!(
            cbor.headers(&framed)
                .expect("the header block parses")
                .get_str("event"),
            Some("created")
        );

        let msgpack = Framed::new(MsgpackCodec);
        let framed = sealed(&msgpack, &[0xff, 0x00], &tenant());
        assert_eq!(
            payload_of(&msgpack, &framed).as_deref(),
            Some([0xff, 0x00].as_slice())
        );
        assert_eq!(
            msgpack
                .headers(&framed)
                .expect("the header block parses")
                .get_str("event"),
            Some("created")
        );
    }

    /// The document is recognised only in its exact shape: the headers and exactly one of the two
    /// payload fields. Anything else is a record of another client's, delivered whole.
    #[test]
    fn a_document_of_another_shape_is_a_foreign_record() {
        let json = Framed::new(JsonCodec);
        for foreign in [
            br#"{"headers":{},"payload":{"id":7},"payload_base64":"CAc="}"#.as_slice(),
            br#"{"headers":{},"payload":{"id":7},"id":7}"#.as_slice(),
            br#"{"headers":{}}"#.as_slice(),
            br#"{"payload":{"id":7}}"#.as_slice(),
            br#"{"headers":{},"payload_base64":"not base64!"}"#.as_slice(),
            b"not-json".as_slice(),
        ] {
            assert_eq!(
                json.locate(foreign),
                Location::Bare,
                "{:?}",
                String::from_utf8_lossy(foreign)
            );
        }
    }

    /// A header value is text in the document; a value that is not refuses the publish by name.
    #[test]
    fn a_header_value_that_is_not_text_is_refused_by_name() {
        let mut headers = HeaderMap::new();
        headers.insert("signature", Bytes::from_static(&[0xff, 0x00]));

        let refused = Framed::new(JsonCodec).seal(b"{}", &headers, &mut Vec::new());

        assert!(
            matches!(
                &refused,
                Err(SealError::Encode(err)) if err.to_string().contains("`signature`")
            ),
            "{refused:?}"
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
