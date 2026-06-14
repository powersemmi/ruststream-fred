//! Header-carrying framing for transports whose value is a single opaque blob (Pub/Sub, lists).
//!
//! Streams keep headers as native entry fields, but a Pub/Sub message or list entry is one value, so
//! headers need a frame around the payload. Two framings, chosen per publisher/subscriber:
//!
//! * **No codec (default)** - a compact, lossless binary frame ([`binary_encode`]). It never
//!   corrupts arbitrary payload bytes, but the on-the-wire value is not human-readable.
//! * **A codec** - the `{headers, payload}` envelope is serialized with a [`Codec`], so with the
//!   JSON codec the wire value is readable JSON (e.g. in `RedisInsight`). This path treats headers
//!   and payload as UTF-8 text (the JSON/text case it exists for); for binary data use the default.
//!
//! [`decode`](unframe) is tolerant: a value that is not a well-formed frame (for example one a raw
//! external client published) is delivered as the payload with empty headers.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use ruststream::Headers;
use ruststream::codec::Codec;
use serde::{Deserialize, Serialize};

/// A shared, object-safe envelope codec. `None` selects the binary framing.
pub(crate) type SharedEnvelope = Arc<dyn EnvelopeCodec>;

/// Object-safe wrapper so the broker can hold a codec without the generic `Codec` methods (which
/// make `Codec` itself not `dyn`-compatible). Implemented for every [`Codec`] via a blanket impl.
pub(crate) trait EnvelopeCodec: Send + Sync {
    fn encode(&self, payload: &[u8], headers: &Headers) -> Vec<u8>;
    fn decode(&self, bytes: &[u8]) -> (Bytes, Headers);
}

impl<C: Codec> EnvelopeCodec for C {
    fn encode(&self, payload: &[u8], headers: &Headers) -> Vec<u8> {
        let envelope = Envelope::from_parts(payload, headers);
        Codec::encode(self, &envelope)
            .map_or_else(|_| binary_encode(payload, headers), |b| b.to_vec())
    }

    fn decode(&self, bytes: &[u8]) -> (Bytes, Headers) {
        Codec::decode::<Envelope>(self, bytes).map_or_else(
            |_| (Bytes::copy_from_slice(bytes), Headers::new()),
            Envelope::into_parts,
        )
    }
}

/// Frames `payload` and `headers` for the wire, using `codec` if set else the binary framing.
pub(crate) fn frame(codec: Option<&SharedEnvelope>, payload: &[u8], headers: &Headers) -> Vec<u8> {
    codec.map_or_else(
        || binary_encode(payload, headers),
        |codec| codec.encode(payload, headers),
    )
}

/// Unframes a wire value back into payload and headers, using `codec` if set else the binary
/// framing.
pub(crate) fn unframe(codec: Option<&SharedEnvelope>, bytes: &[u8]) -> (Bytes, Headers) {
    codec.map_or_else(|| binary_decode(bytes), |codec| codec.decode(bytes))
}

/// The codec-serialized envelope. Headers and payload are UTF-8 text so the JSON form stays
/// readable; non-UTF-8 bytes are replaced (use the binary framing for binary data).
#[derive(Serialize, Deserialize)]
struct Envelope {
    #[serde(default)]
    headers: BTreeMap<String, String>,
    payload: String,
}

impl Envelope {
    fn from_parts(payload: &[u8], headers: &Headers) -> Self {
        let headers = headers
            .iter()
            .map(|(name, value)| {
                (
                    name.to_string(),
                    String::from_utf8_lossy(value).into_owned(),
                )
            })
            .collect();
        Self {
            headers,
            payload: String::from_utf8_lossy(payload).into_owned(),
        }
    }

    fn into_parts(self) -> (Bytes, Headers) {
        let mut headers = Headers::new();
        for (name, value) in self.headers {
            headers.insert(name, Bytes::from(value.into_bytes()));
        }
        (Bytes::from(self.payload.into_bytes()), headers)
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

fn binary_encode(payload: &[u8], headers: &Headers) -> Vec<u8> {
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

fn binary_decode(bytes: &[u8]) -> (Bytes, Headers) {
    try_binary_decode(bytes).unwrap_or_else(|| (Bytes::copy_from_slice(bytes), Headers::new()))
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

fn try_binary_decode(bytes: &[u8]) -> Option<(Bytes, Headers)> {
    let mut pos = 0;
    let count = read_u32(bytes, &mut pos)?;
    let mut headers = Headers::new();
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
    use ruststream::codec::JsonCodec;

    fn sample_headers() -> Headers {
        let mut headers = Headers::new();
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
}
