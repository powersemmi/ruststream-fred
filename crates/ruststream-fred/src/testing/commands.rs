//! The stand-in's answer to the `fred` commands a handler issues: `fred`'s mock layer hands every
//! command sent through the stand-in's pool here, and the writes a subscription can observe are
//! applied to the router the way a server applies them.
//!
//! `XADD`, `LPUSH` / `RPUSH` and `PUBLISH` / `SPUBLISH` reach the subscriptions of their own form,
//! under the router's namespace rules, so a write a server would refuse is refused here with the
//! error the server's reply would carry. Every other command is answered as a server answers a
//! command it queued, which is all a handler that does not read the result can tell.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::sync::Weak;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use fred::error::{Error, ErrorKind};
use fred::mocks::{MockCommand, Mocks};
use fred::types::Value;
use ruststream::HeaderMap;

use crate::convert::parts_from_fields;
use crate::envelope::unframe;
use crate::testing::broker::TestBrokerState;
use crate::testing::router::Form;

/// The `fred` mock layer of the stand-in's pool.
pub(crate) struct StandInCommands {
    /// Weak, because the state owns the pool this layer serves.
    state: Weak<TestBrokerState>,
    /// The sequence part of the entry ids an `XADD` answers with.
    next_id: AtomicU64,
}

impl Debug for StandInCommands {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StandInCommands").finish_non_exhaustive()
    }
}

impl StandInCommands {
    pub(crate) const fn new(state: Weak<TestBrokerState>) -> Self {
        Self {
            state,
            next_id: AtomicU64::new(1),
        }
    }

    /// Hands one write to the router, as the subscription-visible effect of `command`.
    fn apply(
        state: &TestBrokerState,
        form: Form,
        key: String,
        (payload, headers): (Bytes, HeaderMap),
    ) -> Result<(), Error> {
        state
            .router
            .publish(
                key,
                payload,
                headers,
                state.coordinator().as_ref(),
                Some(form),
            )
            .map_err(|refusal| Error::new(ErrorKind::Unknown, refusal))
    }

    fn xadd(&self, state: &TestBrokerState, args: &[Value]) -> Result<Value, Error> {
        let key = text(args.first())?;
        let fields = xadd_fields(args)?;
        Self::apply(state, Form::Stream, key, parts_from_fields(fields))?;
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        Ok(Value::String(format!("0-{id}").into()))
    }

    fn push(state: &TestBrokerState, args: &[Value]) -> Result<Value, Error> {
        let key = text(args.first())?;
        let values = args.get(1..).unwrap_or_default();
        for value in values {
            Self::apply(
                state,
                Form::List,
                key.clone(),
                unframe(None, &bytes(value)?),
            )?;
        }
        Ok(Value::Integer(
            i64::try_from(values.len()).unwrap_or(i64::MAX),
        ))
    }

    fn publish(state: &TestBrokerState, args: &[Value]) -> Result<Value, Error> {
        let channel = text(args.first())?;
        let message = bytes(args.get(1).unwrap_or(&Value::Null))?;
        Self::apply(state, Form::Channel, channel, unframe(None, &message))?;
        Ok(Value::Integer(1))
    }
}

impl Mocks for StandInCommands {
    fn process_command(&self, command: MockCommand) -> Result<Value, Error> {
        let Some(state) = self.state.upgrade() else {
            return Err(Error::new(ErrorKind::Canceled, "the broker was shut down"));
        };
        state
            .alive()
            .map_err(|err| Error::new(ErrorKind::Canceled, err.to_string()))?;
        match &*command.cmd {
            "XADD" => self.xadd(&state, &command.args),
            "LPUSH" | "RPUSH" => Self::push(&state, &command.args),
            "PUBLISH" | "SPUBLISH" => Self::publish(&state, &command.args),
            "MULTI" => Ok(Value::String("OK".into())),
            "EXEC" => Ok(Value::Array(Vec::new())),
            _ => Ok(Value::Queued),
        }
    }
}

/// A key or channel argument.
fn text(value: Option<&Value>) -> Result<String, Error> {
    let bytes = bytes(value.unwrap_or(&Value::Null))?;
    String::from_utf8(bytes).map_err(|_| Error::new(ErrorKind::InvalidArgument, "a non-text key"))
}

/// The bytes of a value argument.
fn bytes(value: &Value) -> Result<Vec<u8>, Error> {
    match value {
        Value::Bytes(bytes) => Ok(bytes.to_vec()),
        Value::String(text) => Ok(text.as_bytes().to_vec()),
        Value::Integer(number) => Ok(number.to_string().into_bytes()),
        Value::Double(number) => Ok(number.to_string().into_bytes()),
        _ => Err(Error::new(
            ErrorKind::InvalidArgument,
            "a value the stand-in cannot read as bytes",
        )),
    }
}

/// The field map of an `XADD`: past the key, the trim and `NOMKSTREAM` options and the id, the
/// rest alternates field and value.
fn xadd_fields(args: &[Value]) -> Result<HashMap<String, Vec<u8>>, Error> {
    let mut at = 1;
    loop {
        let word = text(args.get(at))?.to_ascii_uppercase();
        match word.as_str() {
            "NOMKSTREAM" => at += 1,
            "MAXLEN" | "MINID" => {
                at += 1;
                if matches!(text(args.get(at))?.as_str(), "~" | "=") {
                    at += 1;
                }
                at += 1;
            }
            "LIMIT" => at += 2,
            // The id; the pairs follow it.
            _ => break,
        }
    }
    let mut fields = HashMap::new();
    for pair in args.get(at + 1..).unwrap_or_default().chunks(2) {
        let [field, value] = pair else {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "an XADD field without a value",
            ));
        };
        fields.insert(text(Some(field))?, bytes(value)?);
    }
    Ok(fields)
}
