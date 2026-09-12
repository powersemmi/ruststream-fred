//! Header-carrying framing for transports whose value is a single opaque blob (Pub/Sub, lists).
//!
//! Streams keep headers as native entry fields, but a Pub/Sub message or list entry is one value, so
//! headers need a frame around the payload. Two framings, chosen per publisher/subscriber:
//!
//! * **No codec (default)** - a compact binary frame ([`binary_encode`]); the wire value is not
//!   human-readable.
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
//! envelope is tried as a binary frame. A value that is neither (one a raw external client
//! published) is delivered as the payload with empty headers.

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
}

impl<C: Codec> EnvelopeCodec for C {
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
enum Content {
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

    fn into_bytes(self) -> Bytes {
        match self {
            Self::Text(text) => Bytes::from(text.into_bytes()),
            Self::Bytes(bytes) => Bytes::from(bytes),
        }
    }
}

/// The codec-serialized envelope. Readable where the data is text, lossless where it is not.
#[derive(Serialize, Deserialize)]
struct Envelope {
    #[serde(default)]
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
// [u32 header_count]
// repeated: [u32 name_len][name][u32 value_len][value]
// [payload ... to end]
// ```
// All lengths big-endian.

/// Big-endian length prefix, saturating at `u32::MAX` (lengths that large are not real messages).
fn len_prefix(n: usize) -> [u8; 4] {
    u32::try_from(n).unwrap_or(u32::MAX).to_be_bytes()
}

fn binary_encode(payload: &[u8], headers: &HeaderMap) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + payload.len());
    buf.extend_from_slice(&len_prefix(headers.len()));
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
    let mut pos = 0;
    let count = read_u32(bytes, &mut pos)?;
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
        assert!(headers.is_empty());
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
        assert!(headers.is_empty());
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
