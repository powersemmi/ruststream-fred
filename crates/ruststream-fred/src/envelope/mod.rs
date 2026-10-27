//! How a message's headers travel beside its payload in a Redis value.
//!
//! A list element and a channel message are one value, and a stream entry is a map of fields, so
//! the headers need a layout around the payload. That layout is an [`Envelope`], a type of this
//! crate, chosen like a codec: the broker's default with [`RedisBroker::envelope`], overridden on
//! a subscription descriptor or a publish policy with its own `envelope(..)`. The choice is a type,
//! so it costs nothing per message.
//!
//! - [`RustStream`]: this crate's own layout and the default, the same bytes earlier releases
//!   wrote.
//! - [`FastStream`]: faststream's `BinaryMessageFormatV1`, in the `__data__` field of a stream
//!   entry, so a service shares lists, channels and streams with a faststream service.
//! - [`Framed`]: one document of headers and payload through a `RustStream` codec, readable as
//!   text with JSON.
//! - [`NoOp`]: the payload as it is, with no headers.
//!
//! The [Envelopes](https://powersemmi.github.io/ruststream-fred/latest/components/subscriptions/envelopes/)
//! page shows each one in a service, and an envelope of a service's own.
//!
//! [`RedisBroker::envelope`]: crate::RedisBroker::envelope

mod body;
mod erased;
mod fast_stream;
mod framed;
mod mount;
mod noop;
mod rust_stream;

use std::any::TypeId;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::fmt::Debug;
use std::ops::Range;

use ruststream::HeaderMap;
use thiserror::Error;

pub(crate) use body::{Body, EntryBody, Meta};
pub(crate) use erased::Framing;
pub use fast_stream::FastStream;
pub use framed::Framed;
pub use mount::RedisEnvelopeExt;
pub use noop::NoOp;
pub use rust_stream::RustStream;
pub(crate) use rust_stream::unframe_own;

/// The stream entry field this crate keeps a payload in, and the field an envelope keeps its value
/// in unless it names another.
pub(crate) const PAYLOAD_FIELD: &str = "_payload";

/// The layout that carries a message's headers beside its payload in a Redis value.
///
/// The value form is a list element and a channel message: [`seal`](Self::seal) writes it,
/// [`locate`](Self::locate) finds the payload in it without reading the headers, and
/// [`headers`](Self::headers) reads them when a handler asks. A delivery calls `locate` once and
/// `headers` at most once, so a subscription that never reads headers never parses them. The
/// entry form is a stream entry: by default the sealed value in one field,
/// [`entry_field`](Self::entry_field), which an envelope with a layout of fields of its own
/// overrides with the `*_entry` methods.
///
/// An envelope is recognisable: `locate` answers [`Location::Bare`] for a value that is not one
/// of its own, and such a value is delivered as the payload with no headers. `seal` writes a
/// payload that only looks like an envelope so that it reads back unchanged.
///
/// The type is the choice, and a value of it is held by the broker, by each subscription and by
/// each delivery, so an envelope is a small value, usually a unit struct. A broker whose own
/// envelope is [`Framed`] or one of the service's own registers itself with the test harness once,
/// beside the tests: `ruststream::register_testable_broker!(RedisBroker<MyEnvelope>);`.
///
/// # Examples
///
/// A JSON line with the headers under `meta`, the layout another system on the same list writes:
///
/// ```
/// # mod demo {
/// use std::collections::BTreeMap;
///
/// use ruststream::HeaderMap;
/// use ruststream_fred::envelope::{Envelope, Location, SealError};
/// use ruststream_fred::list::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Debug, Clone, Copy, Default)]
/// struct MetaLine;
///
/// #[derive(Deserialize)]
/// struct Line {
///     meta: BTreeMap<String, String>,
///     body: String,
/// }
///
/// impl Envelope for MetaLine {
///     const CONTENT_TYPE: &'static str = "application/json";
///
///     fn locate(&self, value: &[u8]) -> Location {
///         serde_json::from_slice::<Line>(value)
///             .map_or(Location::Bare, |line| Location::Decoded(line.body.into_bytes()))
///     }
///
///     fn headers(&self, value: &[u8]) -> HeaderMap {
///         let mut headers = HeaderMap::new();
///         if let Ok(line) = serde_json::from_slice::<Line>(value) {
///             for (name, text) in line.meta {
///                 headers.insert(name, text);
///             }
///         }
///         headers
///     }
///
///     fn seal(
///         &self,
///         payload: &[u8],
///         headers: &HeaderMap,
///         value: &mut Vec<u8>,
///     ) -> Result<(), SealError> {
///         let meta: BTreeMap<&str, String> = headers
///             .iter()
///             .map(|(name, text)| (name, String::from_utf8_lossy(text).into_owned()))
///             .collect();
///         let body = String::from_utf8_lossy(payload);
///         serde_json::to_writer(value, &serde_json::json!({ "meta": meta, "body": body }))
///             .map_err(|err| SealError::Encode(Box::new(err)))
///     }
/// }
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// #[subscriber(RedisList::new("jobs"))]
/// async fn run_job(job: &Job) -> HandlerOutcome {
///     tracing::info!(job.id, "running");
///     HandlerOutcome::ack()
/// }
///
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("jobs", "0.1.0")).with_broker(
///         RedisBroker::standalone("redis://localhost:6379"),
///         |b| {
///             b.include(run_job.envelope(MetaLine));
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
pub trait Envelope: Clone + Debug + Send + Sync + 'static {
    /// The media type of the value this envelope writes, which the generated `AsyncAPI` document
    /// reports.
    const CONTENT_TYPE: &'static str;

    /// Where the payload lies in `value`, found without reading the headers.
    fn locate(&self, value: &[u8]) -> Location;

    /// The headers of a `value` that [`locate`](Self::locate) recognised, read when a handler
    /// first asks for them.
    fn headers(&self, value: &[u8]) -> HeaderMap;

    /// Writes `payload` and `headers` as one value, appended to `value`.
    ///
    /// # Errors
    ///
    /// Returns [`SealError`] when the envelope cannot carry the message: a header on an envelope
    /// with no room for headers, or a payload its layout cannot hold.
    fn seal(
        &self,
        payload: &[u8],
        headers: &HeaderMap,
        value: &mut Vec<u8>,
    ) -> Result<(), SealError>;

    /// The stream entry field the sealed value sits in. `_payload` unless an envelope names
    /// another.
    fn entry_field(&self) -> &'static str {
        PAYLOAD_FIELD
    }

    /// Writes a stream entry's fields: by default the sealed value in
    /// [`entry_field`](Self::entry_field).
    ///
    /// # Errors
    ///
    /// Returns [`SealError`] where [`seal`](Self::seal) does.
    fn seal_entry(
        &self,
        payload: Vec<u8>,
        headers: &HeaderMap,
    ) -> Result<Vec<(String, Vec<u8>)>, SealError> {
        let mut value = Vec::with_capacity(payload.len());
        self.seal(&payload, headers, &mut value)?;
        Ok(vec![(self.entry_field().to_owned(), value)])
    }

    /// Where the payload lies in the value of [`entry_field`](Self::entry_field): by default
    /// where [`locate`](Self::locate) finds it.
    fn locate_entry(&self, value: &[u8]) -> Location {
        self.locate(value)
    }

    /// The headers of a stream entry whose value [`locate_entry`](Self::locate_entry) recognised.
    /// `fields` are the entry's other fields. By default the headers of the value.
    fn entry_headers(&self, value: &[u8], fields: &EntryFields) -> HeaderMap {
        let _ = fields;
        self.headers(value)
    }
}

/// Where an [`Envelope`] found the payload in a value.
///
/// # Examples
///
/// An envelope whose value is a tenant line and the payload after it: the payload is a range of
/// the value, and a value without the line is no envelope of its own.
///
/// ```
/// use ruststream::HeaderMap;
/// use ruststream_fred::envelope::{Envelope, Location, SealError};
///
/// #[derive(Debug, Clone, Copy)]
/// struct TenantLine;
///
/// impl Envelope for TenantLine {
///     const CONTENT_TYPE: &'static str = "text/plain";
///
///     fn locate(&self, value: &[u8]) -> Location {
///         match value.iter().position(|byte| *byte == b'\n') {
///             Some(end) if value.starts_with(b"tenant:") => {
///                 Location::Within(end + 1..value.len())
///             }
///             _ => Location::Bare,
///         }
///     }
///
///     fn headers(&self, value: &[u8]) -> HeaderMap {
///         let mut headers = HeaderMap::new();
///         if let Some(line) = value.split(|byte| *byte == b'\n').next() {
///             headers.insert("tenant", line[b"tenant:".len()..].to_vec());
///         }
///         headers
///     }
///
///     fn seal(
///         &self,
///         payload: &[u8],
///         headers: &HeaderMap,
///         value: &mut Vec<u8>,
///     ) -> Result<(), SealError> {
///         if let Some(tenant) = headers.get("tenant") {
///             value.extend_from_slice(b"tenant:");
///             value.extend_from_slice(tenant);
///             value.push(b'\n');
///         }
///         value.extend_from_slice(payload);
///         Ok(())
///     }
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Location {
    /// The value is not an envelope: all of it is the payload, and it carries no headers.
    Bare,
    /// The payload is this range of the value, which stays as it arrived.
    Within(Range<usize>),
    /// The payload had to be decoded out of the value, as a codec's document is.
    Decoded(Vec<u8>),
}

/// Why an [`Envelope`] could not write a message.
///
/// A publish through the envelope fails with it, inside
/// [`RedisError::Seal`](crate::RedisError::Seal) with the destination named.
///
/// # Examples
///
/// An envelope with room for one header, the tenant, refuses any other by name:
///
/// ```
/// use ruststream::HeaderMap;
/// use ruststream_fred::envelope::{Envelope, Location, SealError};
///
/// #[derive(Debug, Clone, Copy)]
/// struct TenantOnly;
///
/// impl Envelope for TenantOnly {
///     const CONTENT_TYPE: &'static str = "application/octet-stream";
///
///     fn locate(&self, value: &[u8]) -> Location {
///         let tenant = value.first().map_or(0, |len| usize::from(*len));
///         Location::Within(1 + tenant..value.len())
///     }
///
///     fn headers(&self, value: &[u8]) -> HeaderMap {
///         let tenant = value.first().map_or(0, |len| usize::from(*len));
///         let mut headers = HeaderMap::new();
///         headers.insert("tenant", value[1..1 + tenant].to_vec());
///         headers
///     }
///
///     fn seal(
///         &self,
///         payload: &[u8],
///         headers: &HeaderMap,
///         value: &mut Vec<u8>,
///     ) -> Result<(), SealError> {
///         if let Some((name, _)) = headers.iter().find(|(name, _)| *name != "tenant") {
///             return Err(SealError::HeaderNotCarried { header: name.to_owned() });
///         }
///         let tenant = headers.get("tenant").unwrap_or_default();
///         let len = u8::try_from(tenant.len()).map_err(|err| SealError::Encode(Box::new(err)))?;
///         value.push(len);
///         value.extend_from_slice(tenant);
///         value.extend_from_slice(payload);
///         Ok(())
///     }
/// }
/// ```
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SealError {
    /// The envelope has no room for headers, and the message carries this one.
    #[error("the envelope carries no headers, and the message has the header `{header}`")]
    HeaderNotCarried {
        /// The first header the message carries.
        header: String,
    },
    /// The layout cannot hold this message, or its codec failed to write it.
    #[error("the envelope could not write the message: {0}")]
    Encode(#[source] Box<dyn StdError + Send + Sync>),
}

/// The fields of a stream entry other than the one an [`Envelope`] keeps its value in.
///
/// # Examples
///
/// ```
/// use ruststream::HeaderMap;
/// use ruststream_fred::envelope::{EntryFields, Envelope, Location, SealError};
///
/// /// A stream entry whose `tenant` field is a header beside the payload in `body`.
/// #[derive(Debug, Clone, Copy)]
/// struct TenantEntry;
///
/// impl Envelope for TenantEntry {
///     const CONTENT_TYPE: &'static str = "application/octet-stream";
///
///     fn locate(&self, value: &[u8]) -> Location {
///         Location::Within(0..value.len())
///     }
///
///     fn headers(&self, _value: &[u8]) -> HeaderMap {
///         HeaderMap::new()
///     }
///
///     fn seal(
///         &self,
///         payload: &[u8],
///         _headers: &HeaderMap,
///         value: &mut Vec<u8>,
///     ) -> Result<(), SealError> {
///         value.extend_from_slice(payload);
///         Ok(())
///     }
///
///     fn entry_field(&self) -> &'static str {
///         "body"
///     }
///
///     fn entry_headers(&self, _value: &[u8], fields: &EntryFields) -> HeaderMap {
///         let mut headers = HeaderMap::new();
///         if let Some(tenant) = fields.get("tenant") {
///             headers.insert("tenant", tenant.to_vec());
///         }
///         headers
///     }
/// }
/// ```
#[derive(Debug, Default)]
pub struct EntryFields(HashMap<String, Vec<u8>>);

impl EntryFields {
    pub(crate) const fn new(fields: HashMap<String, Vec<u8>>) -> Self {
        Self(fields)
    }

    /// The value of the field `name`.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&[u8]> {
        self.0.get(name).map(Vec::as_slice)
    }

    /// Every field, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &[u8])> {
        self.0
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_slice()))
    }
}

/// The envelope a descriptor or a publish policy takes from its broker, until it names its own.
///
/// It is the default type of every descriptor's and every policy's envelope parameter, so
/// `RedisList::new("jobs")` writes and reads what the broker's
/// [`envelope`](crate::RedisBroker::envelope) names.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::list::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// // The descriptor names no envelope, so it reads faststream's frame, the broker's.
/// #[subscriber(RedisList::new("jobs"))]
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
pub struct Inherited;

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::Inherited {}
    impl<Env: super::Envelope> Sealed for Env {}
}

/// What a descriptor or a publish policy names as its envelope: [`Inherited`], or an [`Envelope`].
///
/// Resolved against the broker's own envelope `Default` when the descriptor or the policy meets
/// the connected broker. Implemented for [`Inherited`] and for every envelope, and for nothing
/// else.
///
/// # Examples
///
/// ```
/// # mod demo {
/// use ruststream_fred::list::prelude::*;
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Job {
///     id: u64,
/// }
///
/// // The subscription's own envelope wins over the broker's.
/// #[subscriber(RedisList::new("raw-jobs"))]
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
///             b.include(run_job.envelope(NoOp));
///         },
///     )
/// }
/// # }
/// # fn main() {}
/// ```
pub trait EnvelopeChoice<Default: Envelope>:
    sealed::Sealed + Clone + Send + Sync + 'static
{
    /// The envelope this choice comes to on a broker whose own is `Default`.
    type Envelope: Envelope;

    /// The envelope this choice comes to, given the broker's own.
    fn resolve(self, default: &Default) -> Self::Envelope;
}

impl<Default: Envelope> EnvelopeChoice<Default> for Inherited {
    type Envelope = Default;

    fn resolve(self, default: &Default) -> Default {
        default.clone()
    }
}

impl<Env: Envelope, Default: Envelope> EnvelopeChoice<Default> for Env {
    type Envelope = Env;

    fn resolve(self, _default: &Default) -> Env {
        self
    }
}

/// How a stream subscription reading through `envelope` records its names: `None` when it reads
/// through its broker's own `Default`, which the default publisher writes without the table.
pub(crate) fn stream_framing<Default: Envelope, Env: Envelope>(envelope: &Env) -> Option<Framing> {
    (TypeId::of::<Env>() != TypeId::of::<Default>()).then(|| Framing::of(envelope))
}
