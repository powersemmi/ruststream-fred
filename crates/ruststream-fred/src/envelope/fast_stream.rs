//! faststream's `BinaryMessageFormatV1`, the frame a faststream service writes to Redis.
//!
//! ```text
//! [8 bytes "\x89BIN\r\n\x1a\n"][u16 version = 1]
//! [u32 headers_start][u32 data_start]
//! at headers_start: [u16 header_count], repeated: [u16 name_len][name][u16 value_len][value]
//! at data_start: [payload ... to end]
//! ```
//!
//! All numbers are big-endian. A stream entry keeps the frame in its `__data__` field.

use bytes::Bytes;
use ruststream::HeaderMap;

use super::{Envelope, Location, SealError};

/// The identity header every frame opens with.
const IDENTITY: &[u8] = b"\x89BIN\r\n\x1a\n";

/// The one version of the format.
const VERSION: u16 = 1;

/// Where the header block starts in a frame this crate writes: right after the two offsets.
const HEADERS_START: usize = IDENTITY.len() + 2 + 4 + 4;

/// The stream entry field faststream keeps the frame in.
const DATA_FIELD: &str = "__data__";

/// faststream's `BinaryMessageFormatV1`, so a `RustStream` service and a faststream service share
/// lists, channels and streams in both directions.
///
/// Every message is framed, headers or none, the way faststream frames it; on a stream the frame
/// is the entry's `__data__` field. A value without the frame's identity header is delivered as the
/// payload with no headers. Header names and values travel byte for byte: faststream's own
/// headers (`correlation_id`, `reply_to`) arrive under faststream's names. A header name or value
/// longer than 65535 bytes does not fit the format, and its publish fails.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Order {
///     id: u64,
/// }
///
/// // A faststream service publishes the orders; this one reads them from the same stream.
/// #[subscriber(RedisStream::new("orders").group("billing"))]
/// async fn bill(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "billed");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("billing", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379").envelope(FastStream),
///         |b| {
///             b.include(bill);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FastStream;

impl Envelope for FastStream {
    const CONTENT_TYPE: &'static str = "application/octet-stream";

    fn locate(&self, value: &[u8]) -> Location {
        if value.get(..IDENTITY.len()) != Some(IDENTITY) {
            return Location::Bare;
        }
        let mut pos = IDENTITY.len();
        let version = read_u16(value, &mut pos);
        let headers_start = read_u32(value, &mut pos);
        let data_start = read_u32(value, &mut pos);
        match (version, headers_start, data_start) {
            (Some(VERSION), Some(headers), Some(data))
                if headers >= HEADERS_START && headers <= data && data <= value.len() =>
            {
                Location::Within(data..value.len())
            }
            _ => Location::Bare,
        }
    }

    /// The header block is read within its bounds, the offsets the frame names; a block that runs
    /// past them, or a name that is not text, does not parse.
    fn headers(&self, value: &[u8]) -> Option<HeaderMap> {
        let mut pos = IDENTITY.len() + 2;
        let start = read_u32(value, &mut pos)?;
        let end = read_u32(value, &mut pos)?;
        let block = value.get(start..end)?;
        let mut pos = 0;
        let count = read_u16(block, &mut pos)?;
        let mut headers = HeaderMap::new();
        for _ in 0..count {
            let name = std::str::from_utf8(read_string(block, &mut pos)?).ok()?;
            let header = read_string(block, &mut pos)?;
            headers.insert(name.to_owned(), Bytes::copy_from_slice(header));
        }
        Some(headers)
    }

    fn seal(
        &self,
        payload: &[u8],
        headers: &HeaderMap,
        value: &mut Vec<u8>,
    ) -> Result<(), SealError> {
        let count = u16::try_from(headers.len())
            .map_err(|_| too_long(&format!("{} headers", headers.len())))?;
        let block: usize = headers
            .iter()
            .map(|(name, header)| 4 + name.len() + header.len())
            .sum();
        let data_start = HEADERS_START + 2 + block;
        let data_offset = u32::try_from(data_start)
            .map_err(|_| too_long(&format!("a header block of {block} bytes")))?;
        value.reserve(data_start + payload.len());
        value.extend_from_slice(IDENTITY);
        value.extend_from_slice(&VERSION.to_be_bytes());
        value.extend_from_slice(
            &u32::try_from(HEADERS_START)
                .unwrap_or(u32::MAX)
                .to_be_bytes(),
        );
        value.extend_from_slice(&data_offset.to_be_bytes());
        value.extend_from_slice(&count.to_be_bytes());
        for (name, header) in headers.iter() {
            write_string(value, name.as_bytes(), name)?;
            write_string(value, header, name)?;
        }
        value.extend_from_slice(payload);
        Ok(())
    }

    fn entry_field(&self) -> &'static str {
        DATA_FIELD
    }
}

fn too_long(what: &str) -> SealError {
    SealError::Encode(format!("{what} do not fit faststream's binary frame").into())
}

/// Writes one length-prefixed string of the header block; `header` names it in the refusal.
fn write_string(value: &mut Vec<u8>, bytes: &[u8], header: &str) -> Result<(), SealError> {
    let len = u16::try_from(bytes.len()).map_err(|_| {
        too_long(&format!(
            "the header `{header}`, {} bytes long, does",
            bytes.len()
        ))
    })?;
    value.extend_from_slice(&len.to_be_bytes());
    value.extend_from_slice(bytes);
    Ok(())
}

fn read_u16(bytes: &[u8], pos: &mut usize) -> Option<u16> {
    let end = pos.checked_add(2)?;
    let raw = bytes.get(*pos..end)?;
    *pos = end;
    Some(u16::from_be_bytes(raw.try_into().ok()?))
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> Option<usize> {
    let end = pos.checked_add(4)?;
    let raw = bytes.get(*pos..end)?;
    *pos = end;
    Some(u32::from_be_bytes(raw.try_into().ok()?) as usize)
}

fn read_string<'a>(bytes: &'a [u8], pos: &mut usize) -> Option<&'a [u8]> {
    let len = usize::from(read_u16(bytes, pos)?);
    let end = pos.checked_add(len)?;
    let slice = bytes.get(*pos..end)?;
    *pos = end;
    Some(slice)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What faststream 0.7.8 writes for the payload `{"id":7}` with the header `tenant: acme` and
    /// the correlation id `c-1`: `BinaryMessageFormatV1.encode(message=b'{"id":7}',
    /// headers={"tenant": "acme"}, correlation_id="c-1", reply_to=None)`.
    const FASTSTREAM_FRAME: &str = "8942494e0d0a1a0a000100000012000000370002000e636f7272656c6174\
                                    696f6e5f69640003632d31000674656e616e74000461636d657b226964223a\
                                    377d";

    fn faststream_frame() -> Vec<u8> {
        (0..FASTSTREAM_FRAME.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&FASTSTREAM_FRAME[at..at + 2], 16).expect("hex"))
            .collect()
    }

    fn payload_of(value: &[u8]) -> &[u8] {
        match FastStream.locate(value) {
            Location::Bare => value,
            Location::Within(range) => &value[range],
            Location::Decoded(_) => unreachable!("the frame is never decoded"),
        }
    }

    #[test]
    fn a_faststream_frame_reads_as_its_payload_and_headers() {
        let frame = faststream_frame();

        assert_eq!(payload_of(&frame), br#"{"id":7}"#);
        let headers = FastStream.headers(&frame).expect("the header block parses");
        assert_eq!(headers.get_str("correlation_id"), Some("c-1"));
        assert_eq!(headers.get_str("tenant"), Some("acme"));
        assert_eq!(headers.len(), 2);
    }

    /// The frame this crate writes is the one faststream writes for the same message, byte for
    /// byte, so faststream reads it.
    #[test]
    fn a_sealed_message_is_the_frame_faststream_writes() {
        let mut headers = HeaderMap::new();
        headers.insert("correlation_id", "c-1");
        headers.insert("tenant", "acme");
        let mut value = Vec::new();

        FastStream
            .seal(br#"{"id":7}"#, &headers, &mut value)
            .expect("the frame carries the message");

        assert_eq!(value, faststream_frame());
    }

    /// faststream frames every message, so a message without headers is a frame too, and a
    /// payload that begins like one is inside a frame of its own.
    #[test]
    fn a_message_without_headers_and_a_lookalike_payload_are_framed() {
        let lookalike = faststream_frame();
        let mut value = Vec::new();

        FastStream
            .seal(&lookalike, &HeaderMap::new(), &mut value)
            .expect("the frame carries the message");

        assert_eq!(payload_of(&value), lookalike.as_slice());
        assert_eq!(
            FastStream.headers(&value).expect("the header block parses"),
            HeaderMap::new()
        );
    }

    #[test]
    fn a_value_without_the_identity_header_is_bare() {
        assert_eq!(FastStream.locate(br#"{"id":7}"#), Location::Bare);
    }

    /// The identity header alone does not make a frame: a version faststream has not written, or
    /// an offset past the end, leaves the value whole.
    #[test]
    fn a_frame_of_another_version_or_cut_short_is_bare() {
        let mut other = faststream_frame();
        other[IDENTITY.len() + 1] = 2;
        assert_eq!(FastStream.locate(&other), Location::Bare);

        let frame = faststream_frame();
        assert_eq!(FastStream.locate(&frame[..40]), Location::Bare);
    }

    #[test]
    fn a_header_too_long_for_the_frame_is_refused() {
        let mut headers = HeaderMap::new();
        headers.insert("trace", vec![b'x'; usize::from(u16::MAX) + 1]);

        let refused = FastStream.seal(b"{}", &headers, &mut Vec::new());

        assert!(
            matches!(&refused, Err(SealError::Encode(err)) if err.to_string().contains("`trace`")),
            "{refused:?}"
        );
    }

    #[test]
    fn a_stream_entry_keeps_the_frame_in_the_data_field() {
        let fields = FastStream
            .seal_entry(br#"{"id":7}"#.to_vec(), &HeaderMap::new())
            .expect("the frame carries the message");

        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].0, "__data__");
        assert_eq!(payload_of(&fields[0].1), br#"{"id":7}"#);
    }
}
