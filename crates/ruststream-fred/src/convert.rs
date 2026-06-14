//! Encoding between `RustStream` messages and Redis stream entry fields.
//!
//! A stream entry is a flat map of field name to value. We reserve one field for the message body
//! and store every header under a `h:` prefix, so a round-trip through `XADD` / `XREADGROUP`
//! preserves both payload and headers without a second serialization format.

use std::collections::HashMap;

use bytes::Bytes;
use ruststream::Headers;

/// Field holding the message body.
pub(crate) const PAYLOAD_FIELD: &str = "_payload";
/// Prefix marking a stream field as a serialized header.
pub(crate) const HEADER_PREFIX: &str = "h:";

/// Builds the `XADD` field list for a payload and its headers.
pub(crate) fn fields_for_publish(payload: &[u8], headers: &Headers) -> Vec<(String, Vec<u8>)> {
    let mut fields = Vec::with_capacity(1 + headers.len());
    fields.push((PAYLOAD_FIELD.to_owned(), payload.to_vec()));
    for (name, value) in headers.iter() {
        fields.push((format!("{HEADER_PREFIX}{name}"), value.to_vec()));
    }
    fields
}

/// Reconstructs the payload and headers from a decoded stream entry's fields.
///
/// Unknown fields (neither the reserved body nor a `h:`-prefixed header) are ignored so a newer
/// producer can add fields without breaking an older consumer.
pub(crate) fn parts_from_fields(fields: HashMap<String, Vec<u8>>) -> (Bytes, Headers) {
    let mut payload = Bytes::new();
    let mut headers = Headers::new();
    for (key, value) in fields {
        if key == PAYLOAD_FIELD {
            payload = Bytes::from(value);
        } else if let Some(name) = key.strip_prefix(HEADER_PREFIX) {
            headers.insert(name.to_owned(), Bytes::from(value));
        }
    }
    (payload, headers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_payload_and_headers() {
        let mut headers = Headers::new();
        headers.insert("content-type", "application/json");
        headers.insert("correlation-id", "abc-1");

        let fields = fields_for_publish(b"{}", &headers);
        let map: HashMap<String, Vec<u8>> = fields.into_iter().collect();
        let (payload, decoded) = parts_from_fields(map);

        assert_eq!(payload.as_ref(), b"{}");
        assert_eq!(decoded.content_type(), Some("application/json"));
        assert_eq!(decoded.correlation_id(), Some("abc-1"));
    }

    #[test]
    fn unknown_fields_are_ignored() {
        let mut map = HashMap::new();
        map.insert(PAYLOAD_FIELD.to_owned(), b"body".to_vec());
        map.insert("not-a-header".to_owned(), b"x".to_vec());
        let (payload, headers) = parts_from_fields(map);
        assert_eq!(payload.as_ref(), b"body");
        assert!(headers.is_empty());
    }
}
