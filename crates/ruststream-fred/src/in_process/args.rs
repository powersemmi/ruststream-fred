//! Reading a command's arguments and writing its reply, the way a Redis server parses and answers
//! them.

use bytes::Bytes;
use fred::error::{Error, ErrorKind};
use fred::types::Value;

/// A command's arguments, read front to back.
pub(crate) struct Args<'a> {
    command: &'a str,
    values: &'a [Value],
    at: usize,
}

impl<'a> Args<'a> {
    pub(crate) const fn new(command: &'a str, values: &'a [Value]) -> Self {
        Self {
            command,
            values,
            at: 0,
        }
    }

    /// How many arguments are left.
    pub(crate) const fn left(&self) -> usize {
        self.values.len() - self.at
    }

    /// The argument after the next one, without taking either.
    pub(crate) fn peek(&self) -> Option<&'a Value> {
        self.values.get(self.at)
    }

    /// Whether the next argument is `word`, ignoring case; takes it when it is.
    pub(crate) fn word(&mut self, word: &str) -> bool {
        let hit = self
            .peek()
            .and_then(|value| text_of(value).ok())
            .is_some_and(|text| text.eq_ignore_ascii_case(word));
        if hit {
            self.at += 1;
        }
        hit
    }

    fn next(&mut self) -> Result<&'a Value, Error> {
        let value = self.values.get(self.at).ok_or_else(|| self.arity())?;
        self.at += 1;
        Ok(value)
    }

    pub(crate) fn bytes(&mut self) -> Result<Bytes, Error> {
        bytes_of(self.next()?)
    }

    pub(crate) fn text(&mut self) -> Result<String, Error> {
        text_of(self.next()?)
    }

    pub(crate) fn int(&mut self) -> Result<i64, Error> {
        int_of(self.next()?)
    }

    pub(crate) fn float(&mut self) -> Result<f64, Error> {
        float_of(self.next()?)
    }

    /// The rest of the arguments, as bytes.
    pub(crate) fn rest(&mut self) -> Result<Vec<Bytes>, Error> {
        let mut out = Vec::with_capacity(self.left());
        while self.left() > 0 {
            out.push(self.bytes()?);
        }
        Ok(out)
    }

    /// Refuses the command when an argument is left over.
    pub(crate) fn done(&self) -> Result<(), Error> {
        if self.left() == 0 {
            Ok(())
        } else {
            Err(syntax())
        }
    }

    /// The refusal of a command called with too few arguments.
    pub(crate) fn arity(&self) -> Error {
        server_error(format!(
            "ERR wrong number of arguments for '{}' command",
            self.command.to_ascii_lowercase()
        ))
    }
}

/// An error reply of the server.
pub(crate) fn server_error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Unknown, message.into())
}

pub(crate) fn syntax() -> Error {
    server_error("ERR syntax error")
}

pub(crate) fn wrong_type() -> Error {
    server_error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

pub(crate) fn not_integer() -> Error {
    server_error("ERR value is not an integer or out of range")
}

pub(crate) fn bytes_of(value: &Value) -> Result<Bytes, Error> {
    match value {
        Value::Bytes(bytes) => Ok(bytes.clone()),
        Value::String(text) => Ok(text.inner().clone()),
        Value::Integer(number) => Ok(Bytes::from(number.to_string())),
        Value::Double(number) => Ok(Bytes::from(float_text(*number))),
        Value::Boolean(flag) => Ok(Bytes::from_static(if *flag { b"1" } else { b"0" })),
        _ => Err(syntax()),
    }
}

pub(crate) fn text_of(value: &Value) -> Result<String, Error> {
    String::from_utf8(bytes_of(value)?.to_vec()).map_err(|_| syntax())
}

pub(crate) fn int_of(value: &Value) -> Result<i64, Error> {
    match value {
        Value::Integer(number) => Ok(*number),
        other => text_of(other)?.parse().map_err(|_| not_integer()),
    }
}

pub(crate) fn float_of(value: &Value) -> Result<f64, Error> {
    let parsed = match value {
        Value::Double(number) => Some(*number),
        #[allow(
            clippy::cast_precision_loss,
            reason = "a score is a double on the server too"
        )]
        Value::Integer(number) => Some(*number as f64),
        other => {
            let text = text_of(other)?;
            match text.to_ascii_lowercase().as_str() {
                "inf" | "+inf" => Some(f64::INFINITY),
                "-inf" => Some(f64::NEG_INFINITY),
                text => text.parse().ok(),
            }
        }
    };
    parsed
        .filter(|number: &f64| !number.is_nan())
        .ok_or_else(|| server_error("ERR value is not a valid float"))
}

/// A double as the server writes it.
pub(crate) fn float_text(number: f64) -> String {
    if number.is_infinite() {
        return if number > 0.0 { "inf" } else { "-inf" }.to_owned();
    }
    if number.fract() == 0.0 && number.abs() < 1e17 {
        #[allow(
            clippy::cast_possible_truncation,
            reason = "an integral double below 1e17 fits an i64"
        )]
        return (number as i64).to_string();
    }
    number.to_string()
}

pub(crate) fn ok() -> Value {
    Value::String("OK".into())
}

pub(crate) fn int(number: usize) -> Value {
    Value::Integer(i64::try_from(number).unwrap_or(i64::MAX))
}

pub(crate) fn bulk(bytes: Bytes) -> Value {
    Value::Bytes(bytes)
}
