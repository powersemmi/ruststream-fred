//! Header-carrying framing for transports whose value is a single opaque blob (Pub/Sub, lists).
//!
//! Streams keep headers as native entry fields, but a Pub/Sub message or list entry is one value, so
//! headers need a frame around the payload. Two framings, chosen per publisher/subscriber:
//!
//! * **No codec (default)** - a compact binary frame ([`binary_encode`]) that opens with a marker
//!   header; the wire value is not human-readable. A message without headers is written as its
//!   bare payload, and a bare payload that carries the marker is framed, so it reads back
//!   unchanged.
//! * **A codec** - the `{headers, payload}` envelope is serialized with a [`Codec`], so with the
//!   JSON codec the wire value is readable JSON (e.g. in `RedisInsight`).
//!
//! Both framings are lossless. In the envelope a field whose bytes are valid UTF-8 is written as
//! text, which is what makes the JSON form readable, and any other bytes are written as the byte
//! sequence itself ([`Content`]), so a payload arrives exactly as it left. A text field is on the
//! wire in the form it has always had, so an envelope written by an earlier version reads back
//! unchanged.
//!
//! A subscriber that carries a codec reads either framing: a value that does not parse as an
//! envelope in its exact shape is tried as a binary frame. A value that is neither (one a raw
//! external client published) is delivered as the payload with empty headers, whatever its bytes
//! look like. A binary frame of a release before the marker is such a value too.

use std::any::type_name;
use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use ruststream::HeaderMap;
use ruststream::codec::Codec;
use serde::{Deserialize, Serialize};

/// A shared, object-safe envelope codec. `None` selects the binary framing.
pub(crate) type SharedEnvelope = Arc<dyn EnvelopeCodec>;

/// Object-safe wrapper so the broker can hold a codec without the generic `Codec` methods (which
/// make `Codec` itself not `dyn`-compatible). Implemented for every [`Codec`] via a blanket impl.
pub(crate) trait EnvelopeCodec: Send + Sync {
    fn encode(&self, payload: &[u8], headers: &HeaderMap) -> Vec<u8>;
    fn decode(&self, bytes: &[u8]) -> (Bytes, HeaderMap);
    /// The codec's type, which says how a value is framed: two subscriptions on one name must
    /// frame it the same way.
    fn framing(&self) -> &'static str;
    /// The media type of the framed value, for the generated `AsyncAPI` document.
    #[cfg(feature = "asyncapi")]
    fn content_type(&self) -> &'static str;
}

impl<C: Codec> EnvelopeCodec for C {
    fn framing(&self) -> &'static str {
        type_name::<C>()
    }

    #[cfg(feature = "asyncapi")]
    fn content_type(&self) -> &'static str {
        C::CONTENT_TYPE
    }

    fn encode(&self, payload: &[u8], headers: &HeaderMap) -> Vec<u8> {
        let envelope = Envelope::from_parts(payload, headers);
        Codec::encode(self, &envelope)
            .map_or_else(|_| binary_encode(payload, headers), |b| b.to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> (Bytes, HeaderMap) {
        // A value that is not an envelope may still be a binary frame: `encode` writes one when the
        // codec cannot serialize, and a service that switches framings leaves such values behind in
        // a list. `binary_decode` delivers anything that is neither as a raw payload.
        Codec::decode::<Envelope>(self, bytes)
            .map_or_else(|_| binary_decode(bytes), Envelope::into_parts)
    }
}

/// Frames `payload` and `headers` for the wire, using `codec` if set else the binary framing.
pub(crate) fn frame(
    codec: Option<&SharedEnvelope>,
    payload: &[u8],
    headers: &HeaderMap,
) -> Vec<u8> {
    codec.map_or_else(
        || binary_encode(payload, headers),
        |codec| codec.encode(payload, headers),
    )
}

/// Unframes a wire value back into payload and headers, using `codec` if set else the binary
/// framing.
pub(crate) fn unframe(codec: Option<&SharedEnvelope>, bytes: &[u8]) -> (Bytes, HeaderMap) {
    codec.map_or_else(|| binary_decode(bytes), |codec| codec.decode(bytes))
}

/// One envelope field: text when its bytes are valid UTF-8, the bytes themselves otherwise.
///
/// The representation is untagged, so a text field is serialized as the plain string it always
/// was and nothing distinguishes an envelope of text fields from one an earlier version wrote.
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

    fn into_bytes(self) -> Bytes {
        match self {
            Self::Text(text) => Bytes::from(text.into_bytes()),
            Self::Bytes(bytes) => Bytes::from(bytes),
        }
    }
}

/// The codec-serialized envelope. Readable where the data is text, lossless where it is not.
///
/// It is read only in the exact shape it is written in, so a value another client wrote that only
/// resembles it (a `payload` field alone, or fields of its own beside the two) is the plain payload
/// it is.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    headers: BTreeMap<String, Content>,
    payload: Content,
}

impl Envelope {
    fn from_parts(payload: &[u8], headers: &HeaderMap) -> Self {
        let headers = headers
            .iter()
            .map(|(name, value)| (name.to_string(), Content::from_bytes(value)))
            .collect();
        Self {
            headers,
            payload: Content::from_bytes(payload),
        }
    }

    fn into_parts(self) -> (Bytes, HeaderMap) {
        let mut headers = HeaderMap::new();
        for (name, value) in self.headers {
            headers.insert(name, value.into_bytes());
        }
        (self.payload.into_bytes(), headers)
    }
}

// --- Binary framing (the default, lossless) ----------------------------------------------------
//
// ```text
// [u32 header_count + 1]
// [u32 16]["ruststream-frame"][u32 0]          the marker, a header with an empty value
// repeated: [u32 name_len][name][u32 value_len][value]
// [payload ... to end]
// ```
// All lengths big-endian. A message without headers is written as the bare payload, unless the
// payload carries the marker where a frame does: then it is framed, so it reads back unchanged.
// A value without the marker is a plain payload, whatever its bytes look like.

/// The marker as it sits on the wire, right after the header count: a header named
/// `ruststream-frame` (16 bytes) with an empty value.
const MARKER_HEADER: &[u8] = b"\x00\x00\x00\x10ruststream-frame\x00\x00\x00\x00";

/// Where the marker ends in a frame, and the shortest value that can carry one.
const MARKED: usize = 4 + MARKER_HEADER.len();

/// Whether `bytes` carry the marker where a frame carries it.
fn is_marked(bytes: &[u8]) -> bool {
    bytes.get(4..MARKED) == Some(MARKER_HEADER)
}

/// Big-endian length prefix, saturating at `u32::MAX` (lengths that large are not real messages).
fn len_prefix(n: usize) -> [u8; 4] {
    u32::try_from(n).unwrap_or(u32::MAX).to_be_bytes()
}

fn binary_encode(payload: &[u8], headers: &HeaderMap) -> Vec<u8> {
    if headers.is_empty() && !is_marked(payload) {
        return payload.to_vec();
    }
    let mut buf = Vec::with_capacity(MARKED + payload.len());
    buf.extend_from_slice(&len_prefix(headers.len().saturating_add(1)));
    buf.extend_from_slice(MARKER_HEADER);
    for (name, value) in headers.iter() {
        let name = name.as_bytes();
        buf.extend_from_slice(&len_prefix(name.len()));
        buf.extend_from_slice(name);
        buf.extend_from_slice(&len_prefix(value.len()));
        buf.extend_from_slice(value);
    }
    buf.extend_from_slice(payload);
    buf
}

fn binary_decode(bytes: &[u8]) -> (Bytes, HeaderMap) {
    try_binary_decode(bytes).unwrap_or_else(|| (Bytes::copy_from_slice(bytes), HeaderMap::new()))
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> Option<usize> {
    let end = pos.checked_add(4)?;
    let raw = bytes.get(*pos..end)?;
    *pos = end;
    Some(u32::from_be_bytes(raw.try_into().ok()?) as usize)
}

fn read_slice<'a>(bytes: &'a [u8], pos: &mut usize, len: usize) -> Option<&'a [u8]> {
    let end = pos.checked_add(len)?;
    let slice = bytes.get(*pos..end)?;
    *pos = end;
    Some(slice)
}

fn try_binary_decode(bytes: &[u8]) -> Option<(Bytes, HeaderMap)> {
    if !is_marked(bytes) {
        return None;
    }
    let mut pos = 0;
    let count = read_u32(bytes, &mut pos)?.checked_sub(1)?;
    pos = MARKED;
    read_headers(bytes, pos, count)
}

/// Reads `count` headers from `pos` on, and the payload after them.
fn read_headers(bytes: &[u8], mut pos: usize, count: usize) -> Option<(Bytes, HeaderMap)> {
    let mut headers = HeaderMap::new();
    for _ in 0..count {
        let name_len = read_u32(bytes, &mut pos)?;
        let name = read_slice(bytes, &mut pos, name_len)?;
        let name = std::str::from_utf8(name).ok()?;
        let value_len = read_u32(bytes, &mut pos)?;
        let value = read_slice(bytes, &mut pos, value_len)?;
        headers.insert(name.to_owned(), Bytes::copy_from_slice(value));
    }
    Some((Bytes::copy_from_slice(&bytes[pos..]), headers))
}

/// Reads a frame this crate wrote, in the form of the current release or of one before it, which
/// framed every value and had no marker.
///
/// Only for values no other client writes (the delay queue's members): elsewhere a value without
/// the marker is a plain payload, since a foreign value can parse as the old frame.
pub(crate) fn unframe_own(bytes: &[u8]) -> Option<(Bytes, HeaderMap)> {
    if is_marked(bytes) {
        return try_binary_decode(bytes);
    }
    let mut pos = 0;
    let count = read_u32(bytes, &mut pos)?;
    read_headers(bytes, pos, count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ruststream::codec::{CborCodec, JsonCodec, MsgpackCodec};

    fn sample_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");
        headers.insert("correlation-id", "abc-1");
        headers
    }

    #[test]
    fn binary_round_trips() {
        let framed = frame(None, b"{}", &sample_headers());
        let (payload, decoded) = unframe(None, &framed);
        assert_eq!(payload.as_ref(), b"{}");
        assert_eq!(decoded.content_type(), Some("application/json"));
        assert_eq!(decoded.correlation_id(), Some("abc-1"));
    }

    #[test]
    fn binary_raw_value_falls_back_to_payload() {
        let (payload, headers) = unframe(None, b"hi");
        assert_eq!(payload.as_ref(), b"hi");
        assert_eq!(headers, HeaderMap::new());
    }

    /// A value another client wrote is not read as a frame, however its bytes begin: four zero
    /// bytes were a frame with no headers, and their loss went unnoticed.
    #[test]
    fn a_foreign_value_that_parses_as_the_old_frame_arrives_whole() {
        let foreign: &[u8] = &[0, 0, 0, 0, 0xab, 0xcd];
        for codec in [None, Some(Arc::new(JsonCodec) as SharedEnvelope)] {
            let (payload, headers) = unframe(codec.as_ref(), foreign);
            assert_eq!(payload.as_ref(), foreign);
            assert_eq!(headers, HeaderMap::new());
        }
    }

    /// A value with a count and a header in front, the way a released version framed it, is now
    /// a plain value too: only the marker makes a frame.
    #[test]
    fn a_frame_without_the_marker_is_a_plain_value() {
        let mut old = Vec::new();
        old.extend_from_slice(&1u32.to_be_bytes());
        old.extend_from_slice(&1u32.to_be_bytes());
        old.extend_from_slice(b"k");
        old.extend_from_slice(&1u32.to_be_bytes());
        old.extend_from_slice(b"v");
        old.extend_from_slice(b"body");

        let (payload, headers) = unframe(None, &old);

        assert_eq!(payload.as_ref(), old.as_slice());
        assert_eq!(headers, HeaderMap::new());
    }

    /// With no headers there is nothing to frame: the value is the payload, readable by any
    /// client.
    #[test]
    fn a_message_without_headers_is_written_plain() {
        assert_eq!(frame(None, b"hello", &HeaderMap::new()), b"hello");
    }

    /// A plain payload that begins the way a frame begins is framed, so it reads back unchanged
    /// instead of being taken apart.
    #[test]
    fn a_payload_that_looks_like_a_frame_reads_back_unchanged() {
        let lookalike = frame(None, b"inner", &sample_headers());

        let written = frame(None, &lookalike, &HeaderMap::new());
        assert_ne!(
            written, lookalike,
            "the lookalike is framed, not written plain"
        );
        let (payload, headers) = unframe(None, &written);

        assert_eq!(payload.as_ref(), lookalike.as_slice());
        assert_eq!(headers, HeaderMap::new());
    }

    /// The envelope is read only in the exact shape this crate writes: a JSON value another
    /// client wrote with a `payload` field and nothing else, or with fields of its own, is the
    /// payload it is.
    #[test]
    fn a_json_value_shaped_almost_like_the_envelope_arrives_whole() {
        let codec: SharedEnvelope = Arc::new(JsonCodec);
        for foreign in [
            br#"{"payload":"x"}"#.as_slice(),
            br#"{"headers":{},"payload":"x","id":7}"#.as_slice(),
        ] {
            let (payload, headers) = unframe(Some(&codec), foreign);
            assert_eq!(payload.as_ref(), foreign);
            assert_eq!(headers, HeaderMap::new());
        }
    }

    #[test]
    fn codec_round_trips_and_is_readable() {
        let codec: SharedEnvelope = Arc::new(JsonCodec);
        let framed = frame(Some(&codec), br#"{"id":1}"#, &sample_headers());
        // The wire form is readable JSON with the payload and headers as text.
        let text = String::from_utf8(framed.clone()).expect("utf8");
        assert!(text.contains("\"payload\""));
        assert!(text.contains("application/json"));

        let (payload, decoded) = unframe(Some(&codec), &framed);
        assert_eq!(payload.as_ref(), br#"{"id":1}"#);
        assert_eq!(decoded.content_type(), Some("application/json"));
    }

    #[test]
    fn codec_decode_of_raw_value_falls_back() {
        let codec: SharedEnvelope = Arc::new(JsonCodec);
        let (payload, headers) = unframe(Some(&codec), b"not-json");
        assert_eq!(payload.as_ref(), b"not-json");
        assert_eq!(headers, HeaderMap::new());
    }

    /// Bytes that are not text (a lone 0xff is invalid UTF-8, and a NUL byte survives the trip
    /// only if nothing re-encodes it) come back byte for byte through the readable envelope.
    #[test]
    fn codec_round_trips_a_payload_that_is_not_text() {
        let codec: SharedEnvelope = Arc::new(JsonCodec);
        let blob: &[u8] = &[0xff, 0x00, 0x1f, 0xfe, b'{'];

        let framed = frame(Some(&codec), blob, &sample_headers());
        let (payload, decoded) = unframe(Some(&codec), &framed);

        assert_eq!(payload.as_ref(), blob);
        assert_eq!(decoded.content_type(), Some("application/json"));
    }

    /// A header value is framed the same way as the payload, so a binary one survives too.
    #[test]
    fn codec_round_trips_a_header_value_that_is_not_text() {
        let codec: SharedEnvelope = Arc::new(JsonCodec);
        let mut headers = HeaderMap::new();
        headers.insert("signature", Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef]));

        let framed = frame(Some(&codec), b"{}", &headers);
        let (payload, decoded) = unframe(Some(&codec), &framed);

        assert_eq!(payload.as_ref(), b"{}");
        assert_eq!(
            decoded.get("signature").map(<[u8]>::to_vec),
            Some(vec![0xde, 0xad, 0xbe, 0xef])
        );
    }

    /// The wire form of a text payload is the one earlier versions wrote, so values already in
    /// Redis (and subscribers on an older release) keep reading.
    #[test]
    fn a_text_envelope_keeps_its_wire_form() {
        let codec: SharedEnvelope = Arc::new(JsonCodec);
        let framed = frame(Some(&codec), br#"{"id":1}"#, &sample_headers());
        let text = String::from_utf8(framed).expect("utf8");

        assert!(text.contains(r#""payload":"{\"id\":1}""#), "{text}");
        assert!(
            text.contains(r#""content-type":"application/json""#),
            "{text}"
        );
    }

    #[test]
    fn a_text_envelope_from_an_earlier_version_still_decodes() {
        let codec: SharedEnvelope = Arc::new(JsonCodec);
        let wire = br#"{"headers":{"content-type":"application/json"},"payload":"{\"id\":1}"}"#;

        let (payload, decoded) = unframe(Some(&codec), wire);

        assert_eq!(payload.as_ref(), br#"{"id":1}"#);
        assert_eq!(decoded.content_type(), Some("application/json"));
    }

    /// A subscriber configured with a codec still reads a binary frame, which is what `encode`
    /// falls back to when the codec cannot serialize, and what a list holds after a switch of
    /// framings. Headers survive instead of the whole frame arriving as an opaque payload.
    #[test]
    fn a_codec_subscriber_reads_a_binary_frame() {
        let codec: SharedEnvelope = Arc::new(JsonCodec);
        let framed = frame(None, b"{}", &sample_headers());

        let (payload, decoded) = unframe(Some(&codec), &framed);

        assert_eq!(payload.as_ref(), b"{}");
        assert_eq!(decoded.content_type(), Some("application/json"));
    }

    /// The envelope carries whatever codec a service picked, so the untagged text-or-bytes field
    /// has to survive each one, not only JSON.
    #[test]
    fn every_codec_round_trips_bytes_that_are_not_text() {
        let blob: &[u8] = &[0xff, 0x00, 0xfe];

        for (name, codec) in [
            ("json", Arc::new(JsonCodec) as SharedEnvelope),
            ("cbor", Arc::new(CborCodec) as SharedEnvelope),
            ("msgpack", Arc::new(MsgpackCodec) as SharedEnvelope),
        ] {
            let framed = frame(Some(&codec), blob, &sample_headers());
            let (payload, decoded) = unframe(Some(&codec), &framed);

            assert_eq!(
                payload.as_ref(),
                blob,
                "the {name} envelope lost the payload"
            );
            assert_eq!(
                decoded.content_type(),
                Some("application/json"),
                "the {name} envelope lost a header"
            );
        }
    }
}
