//! `JsonCodec`'s document: the payload in the convention of the `CloudEvents` JSON format.

use std::collections::BTreeMap;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ruststream::HeaderMap;
use ruststream::codec::JsonCodec;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::FrameCodec;
use super::binary::{AnyMap, text_headers};
use crate::envelope::{Location, SealError};

impl FrameCodec for JsonCodec {
    /// Writes the document straight into `value`: the payload embedded as is when it is a JSON
    /// text, its base64 otherwise. Neither form needs escaping, so no intermediate buffer is
    /// built.
    fn frame(
        &self,
        headers: &HeaderMap,
        payload: &[u8],
        value: &mut Vec<u8>,
    ) -> Result<(), SealError> {
        // A JSON text with whitespace around it would read back without that whitespace, so only
        // a payload that is the value alone is embedded.
        let embeddable = serde_json::from_slice::<&RawValue>(payload)
            .is_ok_and(|raw| raw.get().len() == payload.len());
        value.reserve(payload.len() + 32);
        value.extend_from_slice(br#"{"headers":"#);
        serde_json::to_writer(&mut *value, &text_headers(headers))
            .map_err(|err| SealError::Encode(Box::new(err)))?;
        if embeddable {
            value.extend_from_slice(br#","payload":"#);
            value.extend_from_slice(payload);
        } else {
            value.extend_from_slice(br#","payload_base64":""#);
            let start = value.len();
            value.resize(
                start + base64::encoded_len(payload.len(), true).unwrap_or(0),
                0,
            );
            let written = STANDARD
                .encode_slice(payload, &mut value[start..])
                .map_err(|err| SealError::Encode(Box::new(err)))?;
            value.truncate(start + written);
            value.push(b'"');
        }
        value.push(b'}');
        Ok(())
    }

    /// The embedded value is a range of the document, found without a copy; base64 is decoded.
    fn frame_body(&self, value: &[u8]) -> Location {
        let Ok(probe) = serde_json::from_slice::<Probe<'_>>(value) else {
            return Location::Bare;
        };
        match (probe.payload, probe.payload_base64) {
            (Some(raw), None) => {
                let text = raw.get().as_bytes();
                // The value borrows the document, so its offset there is the distance between
                // the two.
                let start = text.as_ptr() as usize - value.as_ptr() as usize;
                Location::Within(start..start + text.len())
            }
            (None, Some(encoded)) => STANDARD
                .decode(encoded)
                .map_or(Location::Bare, Location::Decoded),
            _ => Location::Bare,
        }
    }

    fn frame_headers(&self, value: &[u8]) -> Option<HeaderMap> {
        let block = serde_json::from_slice::<Headers>(value).ok()?;
        let mut headers = HeaderMap::new();
        for (name, text) in block.headers {
            headers.insert(name, text);
        }
        Some(headers)
    }
}

/// The JSON document with its headers skipped over, which is what finding the payload reads.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Probe<'a> {
    #[allow(dead_code, reason = "present for the shape check, never read")]
    headers: AnyMap,
    #[serde(borrow, default)]
    payload: Option<&'a RawValue>,
    #[serde(borrow, default)]
    payload_base64: Option<&'a str>,
}

/// The headers of a JSON document, its payload skipped over.
#[derive(Deserialize)]
struct Headers {
    headers: BTreeMap<String, String>,
}
