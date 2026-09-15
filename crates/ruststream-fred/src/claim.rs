//! Decoding the reply of `XREADGROUP ... CLAIM`, the single-read claim-and-read of Redis 8.4.
//!
//! Without `CLAIM` an entry is `[id, fields]` and `fred`'s typed `xreadgroup` parses it. With
//! `CLAIM` every entry carries two more elements, its idle time and its delivery count, so the
//! typed reply no longer fits and the subscription reads the frame itself.
//!
//! The frame arrives in whichever protocol the client negotiated. Under RESP3 the reply is a map
//! of stream key to entries; under RESP2 it is an array of `[key, entries]` pairs, which `fred`
//! hands over converted element by element (a bulk string becomes a blob string, an integer a
//! number). Both shapes are accepted here, because the protocol version is the client's setting
//! and not the subscription's.

use std::collections::HashMap;

use fred::types::Resp3Frame;

use crate::error::RedisError;

/// One entry of a claiming read: its id and fields, plus the two counters `CLAIM` adds.
///
/// A fresh entry off the tail reports zero for both; a claimed one reports how long it had been
/// pending and how many times it has now been delivered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ClaimedEntry {
    pub(crate) id: String,
    pub(crate) fields: HashMap<String, Vec<u8>>,
    pub(crate) idle_ms: u64,
    pub(crate) delivery_count: u64,
}

/// Reads the entries `key` returned, or none when the blocking read timed out.
///
/// # Errors
///
/// Returns [`RedisError::Stream`] when the server answered with an error, or when the reply does
/// not have the shape `XREADGROUP ... CLAIM` documents.
pub(crate) fn decode_reply(frame: &Resp3Frame, key: &str) -> Result<Vec<ClaimedEntry>, RedisError> {
    match frame {
        // A read that timed out with nothing to claim and nothing new.
        Resp3Frame::Null => Ok(Vec::new()),
        Resp3Frame::SimpleError { data, .. } => Err(server_error(data)),
        Resp3Frame::BlobError { data, .. } => Err(server_error(&String::from_utf8_lossy(data))),
        // RESP3: a map of stream key to that key's entries.
        Resp3Frame::Map { data, .. } => {
            for (name, entries) in data {
                if frame_bytes(name) == Some(key.as_bytes()) {
                    return decode_entries(entries);
                }
            }
            Ok(Vec::new())
        }
        // RESP2: an array of `[key, entries]` pairs.
        Resp3Frame::Array { data, .. } => {
            for pair in data {
                let Resp3Frame::Array { data: pair, .. } = pair else {
                    return Err(malformed(
                        "a stream of the reply is not a [key, entries] pair",
                    ));
                };
                let [name, entries] = pair.as_slice() else {
                    return Err(malformed(
                        "a stream of the reply is not a [key, entries] pair",
                    ));
                };
                if frame_bytes(name) == Some(key.as_bytes()) {
                    return decode_entries(entries);
                }
            }
            Ok(Vec::new())
        }
        _ => Err(malformed(
            "the reply is neither a map nor an array of streams",
        )),
    }
}

/// Reads one stream's entries.
fn decode_entries(frame: &Resp3Frame) -> Result<Vec<ClaimedEntry>, RedisError> {
    match frame {
        Resp3Frame::Null => Ok(Vec::new()),
        Resp3Frame::Array { data, .. } => data.iter().map(decode_entry).collect(),
        _ => Err(malformed("the entries of a stream are not an array")),
    }
}

/// Reads one entry: `[id, fields, idle-ms, delivery-count]`.
fn decode_entry(frame: &Resp3Frame) -> Result<ClaimedEntry, RedisError> {
    let Resp3Frame::Array { data, .. } = frame else {
        return Err(malformed("an entry is not an array"));
    };
    let [id, fields, idle, count] = data.as_slice() else {
        return Err(malformed(&format!(
            "an entry of a CLAIM read has four elements [id, fields, idle-ms, delivery-count]; \
             this one has {}",
            data.len()
        )));
    };
    let id = frame_bytes(id)
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .ok_or_else(|| malformed("an entry id is not a string"))?;
    Ok(ClaimedEntry {
        id: id.to_owned(),
        fields: decode_fields(fields)?,
        idle_ms: frame_u64(idle).ok_or_else(|| malformed("an entry idle time is not a number"))?,
        delivery_count: frame_u64(count)
            .ok_or_else(|| malformed("an entry delivery count is not a number"))?,
    })
}

/// Reads an entry's field map.
///
/// The server sends a flat `[name, value, ...]` array under both protocols. A map is accepted as
/// well, so a future server that sends the RESP3 shape for it needs no change here.
fn decode_fields(frame: &Resp3Frame) -> Result<HashMap<String, Vec<u8>>, RedisError> {
    match frame {
        Resp3Frame::Array { data, .. } => {
            if data.len() % 2 != 0 {
                return Err(malformed(
                    "the fields of an entry are an odd-length array, so one name has no value",
                ));
            }
            data.as_chunks::<2>()
                .0
                .iter()
                .map(|[name, value]| field_pair(name, value))
                .collect()
        }
        Resp3Frame::Map { data, .. } => data
            .iter()
            .map(|(name, value)| field_pair(name, value))
            .collect(),
        _ => Err(malformed(
            "the fields of an entry are neither an array nor a map",
        )),
    }
}

/// Reads one field name and its value.
fn field_pair(name: &Resp3Frame, value: &Resp3Frame) -> Result<(String, Vec<u8>), RedisError> {
    let name = frame_bytes(name)
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .ok_or_else(|| malformed("a field name is not a string"))?;
    let value = frame_bytes(value).ok_or_else(|| malformed("a field value is not a string"))?;
    Ok((name.to_owned(), value.to_vec()))
}

/// The bytes of any string-shaped frame, whichever protocol carried it.
fn frame_bytes(frame: &Resp3Frame) -> Option<&[u8]> {
    match frame {
        Resp3Frame::BlobString { data, .. }
        | Resp3Frame::VerbatimString { data, .. }
        | Resp3Frame::SimpleString { data, .. }
        | Resp3Frame::ChunkedString(data) => Some(data),
        _ => None,
    }
}

/// A counter the server sent as a number, or as the string a RESP2 client may see.
fn frame_u64(frame: &Resp3Frame) -> Option<u64> {
    match frame {
        Resp3Frame::Number { data, .. } => u64::try_from(*data).ok(),
        other => frame_bytes(other)
            .and_then(|raw| std::str::from_utf8(raw).ok())
            .and_then(|text| text.parse().ok()),
    }
}

fn server_error(details: &str) -> RedisError {
    RedisError::Stream(format!("XREADGROUP with CLAIM was refused: {details}").into())
}

fn malformed(what: &str) -> RedisError {
    RedisError::Stream(format!("unexpected XREADGROUP CLAIM reply: {what}").into())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn blob(text: &str) -> Resp3Frame {
        Resp3Frame::BlobString {
            data: Bytes::copy_from_slice(text.as_bytes()),
            attributes: None,
        }
    }

    fn number(value: i64) -> Resp3Frame {
        Resp3Frame::Number {
            data: value,
            attributes: None,
        }
    }

    fn array(data: Vec<Resp3Frame>) -> Resp3Frame {
        Resp3Frame::Array {
            data,
            attributes: None,
        }
    }

    /// `[id, [name, value, ..], idle, count]`, the entry shape `CLAIM` returns.
    fn entry(id: &str, idle: i64, count: i64) -> Resp3Frame {
        array(vec![
            blob(id),
            array(vec![blob("_payload"), blob("body")]),
            number(idle),
            number(count),
        ])
    }

    /// The RESP2 shape: an array of `[key, entries]` pairs.
    fn resp2_reply(key: &str, entries: Vec<Resp3Frame>) -> Resp3Frame {
        array(vec![array(vec![blob(key), array(entries)])])
    }

    /// The RESP3 shape: a map of key to entries.
    fn resp3_reply(key: &str, entries: Vec<Resp3Frame>) -> Resp3Frame {
        let mut map = HashMap::new();
        map.insert(blob(key), array(entries));
        Resp3Frame::Map {
            data: map,
            attributes: None,
        }
    }

    /// The two protocols differ in the outer shape only, so both have to decode to the same
    /// entries: the client's protocol setting is not the subscription's business.
    #[test]
    fn both_protocol_shapes_decode_to_the_same_entries() {
        let entries = vec![entry("1-0", 800, 3), entry("2-0", 0, 0)];
        let expected = vec![
            ClaimedEntry {
                id: "1-0".to_owned(),
                fields: [("_payload".to_owned(), b"body".to_vec())].into(),
                idle_ms: 800,
                delivery_count: 3,
            },
            ClaimedEntry {
                id: "2-0".to_owned(),
                fields: [("_payload".to_owned(), b"body".to_vec())].into(),
                idle_ms: 0,
                delivery_count: 0,
            },
        ];

        assert_eq!(
            decode_reply(&resp2_reply("orders", entries.clone()), "orders").expect("resp2 decodes"),
            expected
        );
        assert_eq!(
            decode_reply(&resp3_reply("orders", entries), "orders").expect("resp3 decodes"),
            expected
        );
    }

    /// A blocking read that found nothing answers with a null, not with an empty stream.
    #[test]
    fn a_timed_out_read_decodes_to_no_entries() {
        assert!(
            decode_reply(&Resp3Frame::Null, "orders")
                .expect("a null is a legal reply")
                .is_empty()
        );
    }

    /// The subscription asked for one key, so entries of another are not its own.
    #[test]
    fn entries_of_another_key_are_not_returned() {
        let reply = resp2_reply("other", vec![entry("1-0", 0, 0)]);
        assert!(
            decode_reply(&reply, "orders")
                .expect("a reply naming another key is well formed")
                .is_empty()
        );
    }

    /// The counters are what this mode exists for: a shape that does not carry them is reported,
    /// never silently read as a plain two-element entry with the counters defaulted to zero.
    #[test]
    fn an_entry_without_the_two_counters_is_refused() {
        let plain = array(vec![blob("1-0"), array(vec![blob("_payload"), blob("b")])]);
        let err = decode_reply(&resp2_reply("orders", vec![plain]), "orders")
            .expect_err("an entry with no counters must be refused");
        assert!(
            format!("{err}").contains("four elements"),
            "the error has to name the shape it wanted: {err}"
        );
    }

    /// The server refuses the option on a release that does not have it. The startup check exists
    /// so this never reaches a running subscription, but a read that meets it reports the server's
    /// own words rather than a decoding failure.
    #[test]
    fn a_server_error_is_reported_as_itself() {
        let frame = Resp3Frame::SimpleError {
            data: "ERR syntax error".into(),
            attributes: None,
        };
        let err = decode_reply(&frame, "orders").expect_err("an error frame is an error");
        assert!(format!("{err}").contains("ERR syntax error"), "got {err}");
    }

    /// A RESP2 client can see a counter as a bulk string; it is the same counter.
    #[test]
    fn a_counter_sent_as_a_string_is_read_as_a_number() {
        let entry = array(vec![
            blob("1-0"),
            array(vec![blob("_payload"), blob("body")]),
            blob("800"),
            blob("3"),
        ]);
        let decoded = decode_reply(&resp2_reply("orders", vec![entry]), "orders").expect("decodes");
        assert_eq!(decoded[0].idle_ms, 800);
        assert_eq!(decoded[0].delivery_count, 3);
    }

    /// Headers travel as ordinary fields, so every field of the entry has to survive the decode.
    #[test]
    fn every_field_of_an_entry_survives() {
        let entry = array(vec![
            blob("1-0"),
            array(vec![
                blob("_payload"),
                blob("body"),
                blob("h:content-type"),
                blob("application/json"),
            ]),
            number(0),
            number(0),
        ]);
        let decoded = decode_reply(&resp2_reply("orders", vec![entry]), "orders").expect("decodes");
        assert_eq!(decoded[0].fields.len(), 2);
        assert_eq!(
            decoded[0].fields.get("h:content-type").map(Vec::as_slice),
            Some(b"application/json".as_slice())
        );
    }
}
