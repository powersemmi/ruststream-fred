//! The document with its payload as a byte string, what a [`FrameCodec`](super::FrameCodec)
//! writes unless it overrides it.

use std::collections::BTreeMap;
use std::fmt::{self, Formatter};

use ruststream::HeaderMap;
use ruststream::codec::Codec;
use serde::de::{self, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::envelope::{Location, SealError};

/// The headers as the text fields of a document; [`Framed`](super::Framed) checked they are text.
pub(super) fn text_headers(headers: &HeaderMap) -> BTreeMap<&str, &str> {
    headers
        .iter()
        .filter_map(|(name, value)| Some((name, std::str::from_utf8(value).ok()?)))
        .collect()
}

pub(super) fn frame<Format: Codec>(
    codec: &Format,
    headers: &HeaderMap,
    payload: &[u8],
    value: &mut Vec<u8>,
) -> Result<(), SealError> {
    let document = Document {
        headers: text_headers(headers),
        payload: ByteString(payload),
    };
    let encoded = codec
        .encode(&document)
        .map_err(|err| SealError::Encode(Box::new(err)))?;
    value.extend_from_slice(&encoded);
    Ok(())
}

pub(super) fn body<Format: Codec>(codec: &Format, value: &[u8]) -> Location {
    codec
        .decode::<Probe>(value)
        .map_or(Location::Bare, |probe| Location::Decoded(probe.payload.0))
}

pub(super) fn headers<Format: Codec>(codec: &Format, value: &[u8]) -> Option<HeaderMap> {
    let block = codec.decode::<Headers>(value).ok()?;
    let mut headers = HeaderMap::new();
    for (name, text) in block.headers {
        headers.insert(name, text);
    }
    Some(headers)
}

/// The document a binary codec writes.
#[derive(Serialize)]
struct Document<'a> {
    headers: BTreeMap<&'a str, &'a str>,
    payload: ByteString<'a>,
}

/// The document with its headers skipped over.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Probe {
    #[allow(dead_code, reason = "present for the shape check, never read")]
    headers: AnyMap,
    payload: ByteBuf,
}

/// The headers of a document, its payload skipped over. Both fields are named, since a codec may
/// write a struct as the sequence of its fields.
#[derive(Deserialize)]
struct Headers {
    headers: BTreeMap<String, String>,
    #[allow(
        dead_code,
        reason = "present for a codec that reads fields by position"
    )]
    payload: IgnoredAny,
}

/// Bytes a codec writes as a byte string.
struct ByteString<'a>(&'a [u8]);

impl Serialize for ByteString<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0)
    }
}

/// A byte string a codec read.
struct ByteBuf(Vec<u8>);

impl<'de> Deserialize<'de> for ByteBuf {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Bytes;

        impl Visitor<'_> for Bytes {
            type Value = ByteBuf;

            fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.write_str("a byte string")
            }

            fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<ByteBuf, E> {
                Ok(ByteBuf(bytes.to_vec()))
            }

            fn visit_byte_buf<E: de::Error>(self, bytes: Vec<u8>) -> Result<ByteBuf, E> {
                Ok(ByteBuf(bytes))
            }
        }

        deserializer.deserialize_byte_buf(Bytes)
    }
}

/// A map whose entries are skipped over without being decoded.
pub(super) struct AnyMap;

impl<'de> Deserialize<'de> for AnyMap {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Skip;

        impl<'de> Visitor<'de> for Skip {
            type Value = AnyMap;

            fn expecting(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.write_str("a map of headers")
            }

            fn visit_map<Entries: MapAccess<'de>>(
                self,
                mut entries: Entries,
            ) -> Result<AnyMap, Entries::Error> {
                while entries.next_entry::<IgnoredAny, IgnoredAny>()?.is_some() {}
                Ok(AnyMap)
            }
        }

        deserializer.deserialize_map(Skip)
    }
}
