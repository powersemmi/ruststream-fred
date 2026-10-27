//! A delivery's value as it arrived, with its payload located and its headers read on demand.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::OnceLock;

use bytes::Bytes;
use ruststream::HeaderMap;

use super::{Content, EntryFields, Envelope, Location};
use crate::message::{DELIVERY_COUNT_HEADER, IDLE_MS_HEADER};

/// Where a delivery's payload is: all of the value, a range of it, or decoded out of it.
#[derive(Debug)]
enum Payload {
    Whole,
    Within(Range<usize>),
    Decoded(Vec<u8>),
}

impl Payload {
    /// What `location` comes to in `value`: a range past its end is no envelope of the value's.
    fn of(location: Location, value: &[u8]) -> Self {
        match location {
            Location::Within(range) if value.get(range.clone()).is_some() => Self::Within(range),
            Location::Decoded(payload) => Self::Decoded(payload),
            Location::Bare | Location::Within(_) => Self::Whole,
        }
    }

    fn slice<'a>(&'a self, value: &'a [u8]) -> &'a [u8] {
        match self {
            Self::Whole => value,
            Self::Within(range) => &value[range.clone()],
            Self::Decoded(payload) => payload,
        }
    }

    const fn is_whole(&self) -> bool {
        matches!(self, Self::Whole)
    }
}

/// A list element or a channel message: the value as it arrived, kept whole for the headers, a
/// settle that names it, and a retry that sends it on.
pub(crate) struct Body<Env> {
    value: Bytes,
    payload: Payload,
    headers: OnceLock<HeaderMap>,
    envelope: Env,
}

impl<Env: Envelope> Body<Env> {
    /// Finds the payload in `value`; the headers stay unread.
    pub(crate) fn open(envelope: Env, value: Bytes) -> Self {
        let payload = Payload::of(envelope.locate(&value), &value);
        let headers = if payload.is_whole() {
            OnceLock::from(HeaderMap::new())
        } else {
            OnceLock::new()
        };
        Self {
            value,
            payload,
            headers,
            envelope,
        }
    }

    pub(crate) fn payload(&self) -> &[u8] {
        self.payload.slice(&self.value)
    }

    /// Reads the headers on the first call and keeps them.
    pub(crate) fn headers(&self) -> &HeaderMap {
        self.headers
            .get_or_init(|| self.envelope.headers(&self.value))
    }

    /// The value as it arrived.
    pub(crate) const fn value(&self) -> &Bytes {
        &self.value
    }

    /// Hands the value over, for a settle that names it.
    pub(crate) fn into_value(self) -> Bytes {
        self.value
    }

    /// Whether the headers have been read.
    #[cfg(test)]
    pub(crate) fn headers_read(&self) -> bool {
        self.headers.get().is_some() && !self.payload.is_whole()
    }

    pub(crate) fn len(&self) -> usize {
        self.payload().len()
    }
}

/// What a claiming read reports about an entry beside its fields, which a handler reads as two
/// headers.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Meta {
    pub(crate) delivery_count: u64,
    pub(crate) idle_ms: u64,
}

/// A stream entry: the envelope's field as it arrived, the entry's other fields, and what the read
/// reported about it.
pub(crate) struct EntryBody<Env> {
    value: Bytes,
    payload: Payload,
    fields: EntryFields,
    meta: Option<Meta>,
    headers: OnceLock<HeaderMap>,
    envelope: Env,
}

impl<Env: Envelope> EntryBody<Env> {
    /// Takes the envelope's field out of `fields` and finds the payload in it; the headers stay
    /// unread.
    ///
    /// An entry without that field is one another producer wrote, with a layout of its own: its
    /// fields become the payload as one JSON object, sorted by name, each value a string when it
    /// is text and an array of bytes otherwise, and it carries no headers. An entry of the
    /// envelope's own costs nothing for it: the object is built only when the field is missing.
    pub(crate) fn open(
        envelope: Env,
        mut fields: HashMap<String, Vec<u8>>,
        meta: Option<Meta>,
    ) -> Self {
        let (value, payload, fields) = match fields.remove(envelope.entry_field()) {
            Some(value) => {
                let payload = Payload::of(envelope.locate_entry(&value), &value);
                (Bytes::from(value), payload, EntryFields::new(fields))
            }
            None => (
                foreign_payload(fields),
                Payload::Whole,
                EntryFields::default(),
            ),
        };
        Self {
            value,
            payload,
            fields,
            meta,
            headers: OnceLock::new(),
            envelope,
        }
    }

    pub(crate) fn payload(&self) -> &[u8] {
        self.payload.slice(&self.value)
    }

    /// Reads the headers on the first call and keeps them, with the read's counts beside them.
    pub(crate) fn headers(&self) -> &HeaderMap {
        self.headers.get_or_init(|| {
            let mut headers = if self.payload.is_whole() {
                HeaderMap::new()
            } else {
                self.envelope.entry_headers(&self.value, &self.fields)
            };
            if let Some(meta) = self.meta {
                headers.insert(DELIVERY_COUNT_HEADER, meta.delivery_count.to_string());
                headers.insert(IDLE_MS_HEADER, meta.idle_ms.to_string());
            }
            headers
        })
    }

    /// The envelope the entry was read through.
    pub(crate) const fn envelope(&self) -> &Env {
        &self.envelope
    }

    /// Whether the headers have been read.
    #[cfg(test)]
    pub(crate) fn headers_read(&self) -> bool {
        self.headers.get().is_some()
    }

    pub(crate) fn len(&self) -> usize {
        self.payload().len()
    }
}

/// The JSON object a foreign entry's fields make.
fn foreign_payload(fields: HashMap<String, Vec<u8>>) -> Bytes {
    let object: BTreeMap<String, Content> = fields
        .into_iter()
        .map(|(name, value)| (name, Content::from_vec(value)))
        .collect();
    Bytes::from(
        serde_json::to_vec(&object).expect("a map of string keys to strings and bytes serializes"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envelope::{FastStream, NoOp, RustStream};

    fn framed(headers: &HeaderMap, payload: &[u8]) -> Bytes {
        let mut value = Vec::new();
        RustStream
            .seal(payload, headers, &mut value)
            .expect("the frame carries any message");
        Bytes::from(value)
    }

    fn tenant() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("tenant", "acme");
        headers
    }

    /// Finding the payload leaves the headers unread; the first read parses them, and they stay.
    #[test]
    fn the_headers_are_read_on_the_first_ask() {
        let body = Body::open(RustStream, framed(&tenant(), b"{}"));

        assert_eq!(body.payload(), b"{}");
        assert!(!body.headers_read(), "finding the payload read the headers");
        assert_eq!(body.headers().get_str("tenant"), Some("acme"));
        assert!(body.headers_read());
    }

    #[test]
    fn a_bare_value_is_the_payload_with_no_headers() {
        let body = Body::open(FastStream, Bytes::from_static(b"plain"));

        assert_eq!(body.payload(), b"plain");
        assert_eq!(body.headers(), &HeaderMap::new());
    }

    #[test]
    fn an_entry_reads_its_header_fields_on_the_first_ask() {
        let mut fields = HashMap::new();
        fields.insert("_payload".to_owned(), b"{}".to_vec());
        fields.insert("h:tenant".to_owned(), b"acme".to_vec());
        let body = EntryBody::open(RustStream, fields, None);

        assert_eq!(body.payload(), b"{}");
        assert!(!body.headers_read(), "finding the payload read the headers");
        assert_eq!(body.headers().get_str("tenant"), Some("acme"));
    }

    /// An entry another producer wrote carries no field of the envelope's (a faststream entry
    /// read through this crate's own layout), so every field it has becomes the payload, as one
    /// JSON object, instead of the message arriving empty.
    #[test]
    fn an_entry_without_the_envelope_field_arrives_as_its_fields() {
        let mut fields = HashMap::new();
        fields.insert("__data__".to_owned(), br#"{"id":1}"#.to_vec());
        fields.insert("source".to_owned(), b"faststream".to_vec());
        fields.insert("h:tenant".to_owned(), b"acme".to_vec());

        let body = EntryBody::open(RustStream, fields, None);

        assert_eq!(
            body.payload(),
            br#"{"__data__":"{\"id\":1}","h:tenant":"acme","source":"faststream"}"#,
        );
        assert_eq!(body.headers(), &HeaderMap::new());
    }

    /// A field value that is not text keeps its bytes, as the codec document keeps them.
    #[test]
    fn a_foreign_field_that_is_not_text_keeps_its_bytes() {
        let mut fields = HashMap::new();
        fields.insert("blob".to_owned(), vec![0xff, 0x00]);

        let body = EntryBody::open(RustStream, fields, None);

        assert_eq!(body.payload(), br#"{"blob":[255,0]}"#);
    }

    /// A claiming read's counts reach the handler as headers, through any envelope, a foreign
    /// entry included.
    #[test]
    fn the_counts_of_a_claim_are_headers() {
        let mut fields = HashMap::new();
        fields.insert("_payload".to_owned(), b"{}".to_vec());
        let meta = Some(Meta {
            delivery_count: 3,
            idle_ms: 40,
        });

        let body = EntryBody::open(NoOp, fields, meta);

        assert_eq!(body.headers().get_str(DELIVERY_COUNT_HEADER), Some("3"));
        assert_eq!(body.headers().get_str(IDLE_MS_HEADER), Some("40"));
    }
}
