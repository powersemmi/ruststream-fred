//! Headers and payload as one document through a `RustStream` codec, the payload in the
//! convention of the `CloudEvents` JSON format.

mod binary;
#[cfg(feature = "json")]
mod json;

use std::any::type_name;
use std::fmt::{Debug, Formatter};

use ruststream::HeaderMap;
#[cfg(feature = "cbor")]
use ruststream::codec::CborCodec;
use ruststream::codec::Codec;
#[cfg(feature = "msgpack")]
use ruststream::codec::MsgpackCodec;

use super::{Envelope, Location, SealError};

/// Headers and payload as one `{headers, payload}` document, written through a codec.
///
/// The payload follows the convention of the `CloudEvents` JSON format. With `JsonCodec` a payload
/// that is a JSON text is embedded as the JSON value it is, and any other bytes travel as standard
/// base64 in `payload_base64`; exactly one of the two fields is present:
///
/// ```text
/// {"headers":{"event":"created"},"payload":{"id":7}}
/// {"headers":{"event":"created"},"payload_base64":"CAc="}
/// ```
///
/// With `CborCodec`, `MsgpackCodec` or a service's own codec the payload is a byte string in
/// `payload`. The codec is a [`FrameCodec`]: the framework's codecs are, under this crate's
/// `json`, `cbor` and `msgpack` features. A header is a text field of `headers`, and a header
/// whose value is not text refuses the publish with the header named. A value that is not a
/// document in exactly this shape is delivered as the payload with no headers.
///
/// A JSON payload reads back byte for byte, and finding it costs one scan of the document. A
/// publish through `JsonCodec` checks that the payload is a JSON text, one scan of it, and encodes
/// a payload that is not in base64.
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
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct Framed<Format>(Format);

impl<Format> Framed<Format> {
    /// The document written through `codec`.
    pub const fn new(codec: Format) -> Self {
        Self(codec)
    }
}

impl<Format> Debug for Framed<Format> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Framed")
            .field(&type_name::<Format>())
            .finish()
    }
}

/// A codec [`Framed`] writes its document in.
///
/// The provided methods write the payload as a byte string, so a service's own codec implements
/// the trait with an empty body. `JsonCodec` overrides them with the `CloudEvents` convention.
/// Every header value is text by the time [`frame`](Self::frame) runs.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream::codec::{CborCodec, Codec, CodecError};
/// use ruststream::BytesMut;
/// use ruststream_fred::envelope::FrameCodec;
/// use ruststream_fred::list::prelude::*;
/// use serde::Serialize;
/// use serde::de::DeserializeOwned;
///
/// /// CBOR under the media type an analytics team registered for its documents.
/// #[derive(Debug, Clone, Copy, Default)]
/// pub struct AnalyticsCbor;
///
/// impl Codec for AnalyticsCbor {
///     const CONTENT_TYPE: &'static str = "application/vnd.acme.analytics+cbor";
///
///     fn encode<T: Serialize>(&self, value: &T) -> Result<BytesMut, CodecError> {
///         CborCodec.encode(value)
///     }
///
///     fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T, CodecError> {
///         CborCodec.decode(bytes)
///     }
/// }
///
/// impl FrameCodec for AnalyticsCbor {}
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     let broker =
///         RedisBroker::standalone("redis://localhost:6379").envelope(Framed::new(AnalyticsCbor));
///     RustStream::new(AppInfo::new("analytics", "0.1.0")).with_broker(broker, |_| {})
/// }
/// # }
/// # fn main() {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a codec `Framed` writes documents in",
    note = "enable this crate's `json`, `cbor` or `msgpack` feature for the framework's codec, or \
            implement `ruststream_fred::envelope::FrameCodec` for a codec of the service"
)]
pub trait FrameCodec: Codec + Clone + 'static {
    /// Writes the document of `payload` and `headers`, appended to `value`.
    ///
    /// # Errors
    ///
    /// Returns [`SealError::Encode`] when the codec cannot write the document.
    fn frame(
        &self,
        headers: &HeaderMap,
        payload: &[u8],
        value: &mut Vec<u8>,
    ) -> Result<(), SealError> {
        binary::frame(self, headers, payload, value)
    }

    /// Where the payload of a document sits, or [`Location::Bare`] for a value that is no
    /// document.
    fn frame_body(&self, value: &[u8]) -> Location {
        binary::body(self, value)
    }

    /// The headers of a document, `None` when they do not parse.
    fn frame_headers(&self, value: &[u8]) -> Option<HeaderMap> {
        binary::headers(self, value)
    }
}

#[cfg(feature = "cbor")]
impl FrameCodec for CborCodec {}

#[cfg(feature = "msgpack")]
impl FrameCodec for MsgpackCodec {}

impl<Format: FrameCodec> Envelope for Framed<Format> {
    const CONTENT_TYPE: &'static str = Format::CONTENT_TYPE;

    fn locate(&self, value: &[u8]) -> Location {
        self.0.frame_body(value)
    }

    fn headers(&self, value: &[u8]) -> Option<HeaderMap> {
        self.0.frame_headers(value)
    }

    fn seal(
        &self,
        payload: &[u8],
        headers: &HeaderMap,
        value: &mut Vec<u8>,
    ) -> Result<(), SealError> {
        if let Some((name, _)) = headers
            .iter()
            .find(|(_, header)| std::str::from_utf8(header).is_err())
        {
            return Err(SealError::Encode(
                format!("the header `{name}` is not text, and a document holds text headers")
                    .into(),
            ));
        }
        self.0.frame(headers, payload, value)
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

    fn sealed<C: FrameCodec>(envelope: &Framed<C>, payload: &[u8], headers: &HeaderMap) -> Vec<u8> {
        let mut value = Vec::new();
        envelope
            .seal(payload, headers, &mut value)
            .expect("the codec writes the document");
        value
    }

    fn payload_of<C: FrameCodec>(envelope: &Framed<C>, value: &[u8]) -> Option<Vec<u8>> {
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

    /// A codec of the service's own, CBOR under a JSON media type here, takes the byte-string
    /// form: the document follows the codec's `FrameCodec`, never its media type.
    #[test]
    fn a_services_own_codec_takes_the_byte_string_form() {
        #[derive(Debug, Clone, Copy)]
        struct JsonNamedCbor;

        impl Codec for JsonNamedCbor {
            const CONTENT_TYPE: &'static str = "application/json";

            fn encode<T: serde::Serialize>(
                &self,
                value: &T,
            ) -> Result<ruststream::BytesMut, ruststream::codec::CodecError> {
                CborCodec.encode(value)
            }

            fn decode<T: serde::de::DeserializeOwned>(
                &self,
                bytes: &[u8],
            ) -> Result<T, ruststream::codec::CodecError> {
                CborCodec.decode(bytes)
            }
        }

        impl FrameCodec for JsonNamedCbor {}

        let own = Framed::new(JsonNamedCbor);
        let framed = sealed(&own, br#"{"id":7}"#, &tenant());

        let decoded: ciborium::Value = ciborium::from_reader(framed.as_slice()).expect("cbor");
        assert!(decoded.is_map(), "the document is CBOR, not JSON text");
        assert_eq!(
            payload_of(&own, &framed).as_deref(),
            Some(br#"{"id":7}"#.as_slice())
        );
        assert_eq!(
            own.headers(&framed)
                .expect("the header block parses")
                .get_str("event"),
            Some("created")
        );
        assert_eq!(
            <Framed<JsonNamedCbor> as Envelope>::CONTENT_TYPE,
            "application/json"
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
