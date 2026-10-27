//! This crate's own layout, the default: a binary frame on lists and channels, a field per header
//! on streams.
//!
//! ```text
//! [u32 header_count + 1]
//! [u32 16]["ruststream-frame"][u32 0]          the marker, a header with an empty value
//! repeated: [u32 name_len][name][u32 value_len][value]
//! [payload ... to end]
//! ```
//!
//! All lengths are big-endian. A message without headers is written as the bare payload, unless
//! the payload carries the marker where a frame does: then it is framed, so it reads back
//! unchanged. A value without the marker is a plain payload, whatever its bytes look like.
//!
//! A stream entry keeps the payload in the field `_payload` and each header in a field
//! `h:<name>`.

use bytes::Bytes;
use ruststream::HeaderMap;

use super::{EntryFields, Envelope, Location, PAYLOAD_FIELD, SealError};

/// Prefix of a stream field that holds a header.
const HEADER_PREFIX: &str = "h:";

/// The marker as it sits on the wire, right after the header count: a header named
/// `ruststream-frame` (16 bytes) with an empty value.
const MARKER_HEADER: &[u8] = b"\x00\x00\x00\x10ruststream-frame\x00\x00\x00\x00";

/// Where the marker ends in a frame, and the shortest value that can carry one.
const MARKED: usize = 4 + MARKER_HEADER.len();

/// This crate's own layout, the default envelope of every broker.
///
/// On a list or a channel the headers travel in a compact binary frame that opens with a marker;
/// a message without headers is written as its bare payload, and a value without the marker is
/// delivered as the payload with no headers. On a stream the payload is the field `_payload` and
/// each header a field `h:<name>`. These are the bytes every earlier release wrote, so services on
/// different releases read each other.
///
/// The type shares its name with the framework's application type, so it is named through this
/// module: `envelope::RustStream`.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::envelope;
/// use ruststream_fred::list::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// // The broker reads faststream's frame; this one list keeps the crate's own.
/// #[subscriber(RedisList::<envelope::RustStream>::new_in("jobs"))]
/// async fn run_job(job: &Job) -> HandlerOutcome {
///     tracing::info!(job.id, "running");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379").envelope(FastStream),
///         |b| {
///             b.include(run_job);
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RustStream;

impl Envelope for RustStream {
    const CONTENT_TYPE: &'static str = "application/octet-stream";

    fn locate(&self, value: &[u8]) -> Location {
        if !is_marked(value) {
            return Location::Bare;
        }
        let mut pos = 0;
        let Some(count) = read_u32(value, &mut pos).and_then(|n| n.checked_sub(1)) else {
            return Location::Bare;
        };
        pos = MARKED;
        for _ in 0..count {
            for _ in 0..2 {
                let skipped = read_u32(value, &mut pos).and_then(|len| skip(value, &mut pos, len));
                if skipped.is_none() {
                    return Location::Bare;
                }
            }
        }
        Location::Within(pos..value.len())
    }

    fn headers(&self, value: &[u8]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        let mut pos = 0;
        let Some(count) = read_u32(value, &mut pos).and_then(|n| n.checked_sub(1)) else {
            return headers;
        };
        pos = MARKED;
        read_headers(value, &mut pos, count, &mut headers);
        headers
    }

    fn seal(
        &self,
        payload: &[u8],
        headers: &HeaderMap,
        value: &mut Vec<u8>,
    ) -> Result<(), SealError> {
        if headers.is_empty() && !is_marked(payload) {
            value.extend_from_slice(payload);
            return Ok(());
        }
        value.reserve(MARKED + payload.len());
        value.extend_from_slice(&len_prefix(headers.len().saturating_add(1)));
        value.extend_from_slice(MARKER_HEADER);
        for (name, header) in headers.iter() {
            let name = name.as_bytes();
            value.extend_from_slice(&len_prefix(name.len()));
            value.extend_from_slice(name);
            value.extend_from_slice(&len_prefix(header.len()));
            value.extend_from_slice(header);
        }
        value.extend_from_slice(payload);
        Ok(())
    }

    /// The body field holds the payload as it is, and the frame stays out of it.
    fn seal_entry(
        &self,
        payload: Vec<u8>,
        headers: &HeaderMap,
    ) -> Result<Vec<(String, Vec<u8>)>, SealError> {
        let mut fields = Vec::with_capacity(1 + headers.len());
        fields.push((PAYLOAD_FIELD.to_owned(), payload));
        for (name, value) in headers.iter() {
            fields.push((format!("{HEADER_PREFIX}{name}"), value.to_vec()));
        }
        Ok(fields)
    }

    fn locate_entry(&self, value: &[u8]) -> Location {
        Location::Within(0..value.len())
    }

    /// Every `h:` field is a header; a field outside that namespace is a newer producer's and is
    /// left alone.
    fn entry_headers(&self, _value: &[u8], fields: &EntryFields) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (key, value) in fields.iter() {
            if let Some(name) = key.strip_prefix(HEADER_PREFIX) {
                headers.insert(name.to_owned(), Bytes::copy_from_slice(value));
            }
        }
        headers
    }
}

/// Whether `bytes` carry the marker where a frame carries it.
fn is_marked(bytes: &[u8]) -> bool {
    bytes.get(4..MARKED) == Some(MARKER_HEADER)
}

/// Big-endian length prefix, saturating at `u32::MAX` (lengths that large are not real messages).
fn len_prefix(n: usize) -> [u8; 4] {
    u32::try_from(n).unwrap_or(u32::MAX).to_be_bytes()
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

fn skip(bytes: &[u8], pos: &mut usize, len: usize) -> Option<()> {
    read_slice(bytes, pos, len).map(drop)
}

/// Reads `count` headers from `pos` on into `headers`, and leaves `pos` where the payload starts.
/// `None` when the frame ends early or a name is not text.
fn read_headers(
    bytes: &[u8],
    pos: &mut usize,
    count: usize,
    headers: &mut HeaderMap,
) -> Option<()> {
    for _ in 0..count {
        let name_len = read_u32(bytes, pos)?;
        let name = std::str::from_utf8(read_slice(bytes, pos, name_len)?).ok()?;
        let value_len = read_u32(bytes, pos)?;
        let value = read_slice(bytes, pos, value_len)?;
        headers.insert(name.to_owned(), Bytes::copy_from_slice(value));
    }
    Some(())
}

/// Reads a frame this crate wrote, in the form of the current release or of one before it, which
/// framed every value and had no marker.
///
/// Only for values no other client writes (the delay queue's members): elsewhere a value without
/// the marker is a plain payload, since a foreign value can parse as the old frame.
pub(crate) fn unframe_own(bytes: &[u8]) -> Option<(Bytes, HeaderMap)> {
    let mut pos = 0;
    let mut count = read_u32(bytes, &mut pos)?;
    if is_marked(bytes) {
        count = count.checked_sub(1)?;
        pos = MARKED;
    }
    let mut headers = HeaderMap::new();
    read_headers(bytes, &mut pos, count, &mut headers)?;
    Some((Bytes::copy_from_slice(&bytes[pos..]), headers))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");
        headers.insert("correlation-id", "abc-1");
        headers
    }

    fn sealed(payload: &[u8], headers: &HeaderMap) -> Vec<u8> {
        let mut value = Vec::new();
        RustStream
            .seal(payload, headers, &mut value)
            .expect("the frame carries any message");
        value
    }

    fn payload_of(value: &[u8]) -> &[u8] {
        match RustStream.locate(value) {
            Location::Bare => value,
            Location::Within(range) => &value[range],
            Location::Decoded(_) => unreachable!("the frame is never decoded"),
        }
    }

    /// The bytes earlier releases wrote for this message, built by hand from the layout.
    fn released_frame() -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&2u32.to_be_bytes());
        frame.extend_from_slice(MARKER_HEADER);
        frame.extend_from_slice(&6u32.to_be_bytes());
        frame.extend_from_slice(b"tenant");
        frame.extend_from_slice(&4u32.to_be_bytes());
        frame.extend_from_slice(b"acme");
        frame.extend_from_slice(b"{}");
        frame
    }

    #[test]
    fn a_frame_round_trips() {
        let framed = sealed(b"{}", &sample_headers());

        assert_eq!(payload_of(&framed), b"{}");
        let headers = RustStream.headers(&framed);
        assert_eq!(headers.content_type(), Some("application/json"));
        assert_eq!(headers.correlation_id(), Some("abc-1"));
    }

    /// The default layout is the wire format of earlier releases, byte for byte, both ways.
    #[test]
    fn the_frame_is_the_one_earlier_releases_wrote() {
        let mut headers = HeaderMap::new();
        headers.insert("tenant", "acme");

        assert_eq!(sealed(b"{}", &headers), released_frame());
        assert_eq!(payload_of(&released_frame()), b"{}");
        assert_eq!(
            RustStream.headers(&released_frame()).get_str("tenant"),
            Some("acme")
        );
    }

    #[test]
    fn a_raw_value_is_bare() {
        assert_eq!(RustStream.locate(b"hi"), Location::Bare);
    }

    /// A value another client wrote is not read as a frame, however its bytes begin: four zero
    /// bytes were a frame with no headers, and their loss went unnoticed.
    #[test]
    fn a_foreign_value_that_parses_as_the_old_frame_is_bare() {
        assert_eq!(RustStream.locate(&[0, 0, 0, 0, 0xab, 0xcd]), Location::Bare);
    }

    /// A value with a count and a header in front, the way a release before the marker framed
    /// it, is a plain value too: only the marker makes a frame.
    #[test]
    fn a_frame_without_the_marker_is_bare() {
        let mut old = Vec::new();
        old.extend_from_slice(&1u32.to_be_bytes());
        old.extend_from_slice(&1u32.to_be_bytes());
        old.extend_from_slice(b"k");
        old.extend_from_slice(&1u32.to_be_bytes());
        old.extend_from_slice(b"v");
        old.extend_from_slice(b"body");

        assert_eq!(RustStream.locate(&old), Location::Bare);
    }

    /// A frame cut short is not one: the whole value is the payload.
    #[test]
    fn a_marked_value_cut_short_is_bare() {
        let framed = released_frame();

        assert_eq!(RustStream.locate(&framed[..MARKED + 6]), Location::Bare);
    }

    /// With no headers there is nothing to frame: the value is the payload, readable by any
    /// client.
    #[test]
    fn a_message_without_headers_is_written_plain() {
        assert_eq!(sealed(b"hello", &HeaderMap::new()), b"hello");
    }

    /// A plain payload that begins the way a frame begins is framed, so it reads back unchanged
    /// instead of being taken apart.
    #[test]
    fn a_payload_that_looks_like_a_frame_reads_back_unchanged() {
        let lookalike = sealed(b"inner", &sample_headers());

        let written = sealed(&lookalike, &HeaderMap::new());

        assert_ne!(
            written, lookalike,
            "the lookalike is framed, not written plain"
        );
        assert_eq!(payload_of(&written), lookalike.as_slice());
        assert_eq!(RustStream.headers(&written), HeaderMap::new());
    }

    /// A stream entry keeps the payload in `_payload` and each header in an `h:` field, the
    /// layout every release wrote.
    #[test]
    fn an_entry_is_the_body_field_and_a_field_per_header() {
        let mut headers = HeaderMap::new();
        headers.insert("tenant", "acme");

        let fields = RustStream
            .seal_entry(b"{}".to_vec(), &headers)
            .expect("an entry carries any message");

        assert_eq!(
            fields,
            vec![
                ("_payload".to_owned(), b"{}".to_vec()),
                ("h:tenant".to_owned(), b"acme".to_vec()),
            ]
        );
    }

    #[test]
    fn an_entry_reads_its_header_fields() {
        let mut rest = std::collections::HashMap::new();
        rest.insert("h:tenant".to_owned(), b"acme".to_vec());
        rest.insert("not-a-header".to_owned(), b"x".to_vec());

        let headers = RustStream.entry_headers(b"{}", &EntryFields::new(rest));

        assert_eq!(headers.get_str("tenant"), Some("acme"));
        assert_eq!(headers.len(), 1);
    }

    /// The body field is all payload: the frame never reaches a stream entry.
    #[test]
    fn an_entry_body_is_all_payload() {
        let lookalike = sealed(b"inner", &sample_headers());

        assert_eq!(
            RustStream.locate_entry(&lookalike),
            Location::Within(0..lookalike.len())
        );
    }

    #[test]
    fn a_delay_member_of_either_release_reads_back() {
        let framed = sealed(b"body", &sample_headers());
        let (payload, headers) = unframe_own(&framed).expect("a marked frame");
        assert_eq!(payload.as_ref(), b"body");
        assert_eq!(headers.correlation_id(), Some("abc-1"));

        let mut old = Vec::new();
        old.extend_from_slice(&1u32.to_be_bytes());
        old.extend_from_slice(&1u32.to_be_bytes());
        old.extend_from_slice(b"k");
        old.extend_from_slice(&1u32.to_be_bytes());
        old.extend_from_slice(b"v");
        old.extend_from_slice(b"body");
        let (payload, headers) = unframe_own(&old).expect("an unmarked frame of this crate's");
        assert_eq!(payload.as_ref(), b"body");
        assert_eq!(headers.get_str("k"), Some("v"));
    }
}
