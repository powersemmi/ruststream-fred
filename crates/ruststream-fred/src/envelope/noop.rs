//! No envelope: the value is the payload.

use ruststream::HeaderMap;

use super::{Envelope, Location, SealError};

/// No envelope at all: the value is the payload, as a client that knows nothing of headers writes
/// and reads it.
///
/// A delivery read through `NoOp` carries none of the message's headers; the headers a stream read
/// itself reports, the delivery count and the idle time of a claiming read, stay. A publish through
/// it that carries a header fails with an error naming the header and the destination, instead of
/// losing the header: a retry copy the runtime publishes carries its retry count in a header, so
/// a retry through a `NoOp` policy fails the same way. On a stream the payload is the field `_payload`.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::stream::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Reading {
///     celsius: f64,
/// }
///
/// // A sensor gateway `XADD`s bare JSON into `_payload`, with no headers.
/// #[subscriber(RedisStream::new("readings").group("dashboard"))]
/// async fn show(reading: &Reading) -> HandlerOutcome {
///     tracing::info!(reading.celsius, "reading");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("dashboard", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             b.include(show.envelope(NoOp));
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoOp;

impl Envelope for NoOp {
    const CONTENT_TYPE: &'static str = "application/octet-stream";

    fn locate(&self, _value: &[u8]) -> Location {
        Location::Bare
    }

    fn headers(&self, _value: &[u8]) -> HeaderMap {
        HeaderMap::new()
    }

    fn seal(
        &self,
        payload: &[u8],
        headers: &HeaderMap,
        value: &mut Vec<u8>,
    ) -> Result<(), SealError> {
        refuse_headers(headers)?;
        value.extend_from_slice(payload);
        Ok(())
    }

    /// The payload becomes the field as it is, without a copy.
    fn seal_entry(
        &self,
        payload: Vec<u8>,
        headers: &HeaderMap,
    ) -> Result<Vec<(String, Vec<u8>)>, SealError> {
        refuse_headers(headers)?;
        Ok(vec![(self.entry_field().to_owned(), payload)])
    }
}

fn refuse_headers(headers: &HeaderMap) -> Result<(), SealError> {
    headers.iter().next().map_or(Ok(()), |(name, _)| {
        Err(SealError::HeaderNotCarried {
            header: name.to_owned(),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_value_is_the_payload_both_ways() {
        let mut value = Vec::new();
        NoOp.seal(b"{}", &HeaderMap::new(), &mut value)
            .expect("a message without headers");

        assert_eq!(value, b"{}");
        assert_eq!(NoOp.locate(&value), Location::Bare);
        assert_eq!(NoOp.headers(&value), HeaderMap::new());
    }

    #[test]
    fn a_header_is_refused_by_name_on_both_forms() {
        let mut headers = HeaderMap::new();
        headers.insert("tenant", "acme");

        let value = NoOp.seal(b"{}", &headers, &mut Vec::new());
        let entry = NoOp.seal_entry(b"{}".to_vec(), &headers);

        assert!(
            matches!(&value, Err(SealError::HeaderNotCarried { header }) if header == "tenant"),
            "{value:?}"
        );
        assert!(
            matches!(&entry, Err(SealError::HeaderNotCarried { header }) if header == "tenant"),
            "{entry:?}"
        );
    }

    #[test]
    fn an_entry_is_the_payload_field() {
        let fields = NoOp
            .seal_entry(b"{}".to_vec(), &HeaderMap::new())
            .expect("a message without headers");

        assert_eq!(fields, vec![("_payload".to_owned(), b"{}".to_vec())]);
    }
}
