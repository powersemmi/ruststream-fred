//! Encoding between `RustStream` messages and Redis stream entry fields.
//!
//! A stream entry is a flat map of field name to value. We reserve one field for the message body
//! and store every header under a `h:` prefix, so a round-trip through `XADD` / `XREADGROUP`
//! preserves both payload and headers without a second serialization format.

use std::collections::{BTreeMap, HashMap};

use bytes::Bytes;
use ruststream::HeaderMap;

use crate::envelope::Content;

/// Field holding the message body.
pub(crate) const PAYLOAD_FIELD: &str = "_payload";
/// Prefix marking a stream field as a serialized header.
pub(crate) const HEADER_PREFIX: &str = "h:";

/// Builds the `XADD` field list for a payload and its headers.
///
/// The body arrives owned because the client keeps it: a publish hands over the buffer the
/// framework wrote and it becomes the field's value.
pub(crate) fn fields_for_publish(payload: Vec<u8>, headers: &HeaderMap) -> Vec<(String, Vec<u8>)> {
    let mut fields = Vec::with_capacity(1 + headers.len());
    fields.push((PAYLOAD_FIELD.to_owned(), payload));
    for (name, value) in headers.iter() {
        fields.push((format!("{HEADER_PREFIX}{name}"), value.to_vec()));
    }
    fields
}

/// Reconstructs the payload and headers from a decoded stream entry's fields.
///
/// Unknown fields (neither the reserved body nor a `h:`-prefixed header) are ignored so a newer
/// producer can add fields without breaking an older consumer.
///
/// An entry without the body field is one another producer wrote (a `FastStream` entry keeps its
/// message under `__data__`, a plain `XADD` any field map). Its fields outside the `h:` namespace
/// become the payload as one JSON object, sorted by name, each value a string when it is text and
/// an array of bytes otherwise; nothing of the entry is lost. An entry of this crate's own costs
/// nothing for it: the object is built only when the body field is missing.
pub(crate) fn parts_from_fields(fields: HashMap<String, Vec<u8>>) -> (Bytes, HeaderMap) {
    let mut payload = None;
    let mut headers = HeaderMap::new();
    let mut foreign = Vec::new();
    for (key, value) in fields {
        if key == PAYLOAD_FIELD {
            payload = Some(Bytes::from(value));
        } else if let Some(name) = key.strip_prefix(HEADER_PREFIX) {
            headers.insert(name.to_owned(), Bytes::from(value));
        } else {
            foreign.push((key, value));
        }
    }
    let payload = payload.unwrap_or_else(|| foreign_payload(foreign));
    (payload, headers)
}

/// The JSON object a foreign entry's fields make.
fn foreign_payload(fields: Vec<(String, Vec<u8>)>) -> Bytes {
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
    use bytes::BytesMut;

    use super::*;

    // A copy and a hand-over carry the same bytes, so only the address tells them apart.
    #[test]
    fn the_body_field_is_the_buffer_the_framework_wrote() {
        let payload = BytesMut::from(&br#"{"id":1}"#[..]);
        let written_at = payload.as_ptr();

        let fields = fields_for_publish(Vec::from(payload), &HeaderMap::new());

        let (name, value) = &fields[0];
        assert_eq!(name, PAYLOAD_FIELD);
        assert_eq!(
            value.as_ptr(),
            written_at,
            "the entry's body is the buffer the framework wrote, not a second one copied from it",
        );
    }

    #[test]
    fn round_trips_payload_and_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("content-type", "application/json");
        headers.insert("correlation-id", "abc-1");

        let fields = fields_for_publish(b"{}".to_vec(), &headers);
        let map: HashMap<String, Vec<u8>> = fields.into_iter().collect();
        let (payload, decoded) = parts_from_fields(map);

        assert_eq!(payload.as_ref(), b"{}");
        assert_eq!(decoded.content_type(), Some("application/json"));
        assert_eq!(decoded.correlation_id(), Some("abc-1"));
    }

    /// An entry another producer wrote carries no body field of this crate's (a `FastStream` entry
    /// keeps its message under `__data__`), so every field it has becomes the payload, as one
    /// JSON object, instead of the message arriving empty.
    #[test]
    fn an_entry_without_the_body_field_arrives_as_its_fields() {
        let mut map = HashMap::new();
        map.insert("__data__".to_owned(), br#"{"id":1}"#.to_vec());
        map.insert("source".to_owned(), b"faststream".to_vec());

        let (payload, headers) = parts_from_fields(map);

        assert_eq!(
            payload.as_ref(),
            br#"{"__data__":"{\"id\":1}","source":"faststream"}"#,
        );
        assert_eq!(headers, HeaderMap::new());
    }

    /// A field value that is not text keeps its bytes, as the envelope keeps them.
    #[test]
    fn a_foreign_field_that_is_not_text_keeps_its_bytes() {
        let mut map = HashMap::new();
        map.insert("blob".to_owned(), vec![0xff, 0x00]);

        let (payload, _) = parts_from_fields(map);

        assert_eq!(payload.as_ref(), br#"{"blob":[255,0]}"#);
    }

    /// The `h:` namespace is this crate's header convention, and the delivery metadata the
    /// subscription adds rides it, so a foreign entry keeps those as headers, not payload.
    #[test]
    fn a_foreign_entry_keeps_header_fields_as_headers() {
        let mut map = HashMap::new();
        map.insert("order".to_owned(), b"7".to_vec());
        map.insert(format!("{HEADER_PREFIX}x-delivery-count"), b"2".to_vec());

        let (payload, headers) = parts_from_fields(map);

        assert_eq!(payload.as_ref(), br#"{"order":"7"}"#);
        assert_eq!(headers.get_str("x-delivery-count"), Some("2"));
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let mut map = HashMap::new();
        map.insert(PAYLOAD_FIELD.to_owned(), b"body".to_vec());
        map.insert("not-a-header".to_owned(), b"x".to_vec());
        let (payload, headers) = parts_from_fields(map);
        assert_eq!(payload.as_ref(), b"body");
        assert_eq!(headers, HeaderMap::new());
    }
}
