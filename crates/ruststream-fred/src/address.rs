//! A server address a service configures, kept as it wrote it and printed without its password.

use std::borrow::Cow;
use std::fmt;
use std::ops::Deref;

use url::Url;

/// What a password prints as. It needs no percent-encoding in a URL's user info or its query, so
/// the printed address reads like the one the service wrote.
const REDACTED: &str = "***";

/// What an address that does not parse prints as when it may hold a credential.
const HIDDEN: &str = "<unparsed address hidden>";

/// The scheme a scheme-less address (a `host:port` seed) is read under.
const SEED_SCHEME: &str = "redis://";

/// A Redis address as the service gave it: a `redis://` URL or a `host:port` seed.
#[derive(Clone)]
pub(crate) struct Address(String);

impl Address {
    pub(crate) const fn new(address: String) -> Self {
        Self(address)
    }
}

impl Deref for Address {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

// A broker is printed into logs, and a password that reaches a log has left the service.
impl fmt::Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&redacted(&self.0), f)
    }
}

/// `address` with every password it carries replaced: the user info's password, and any query
/// parameter named for a password (`fred` reads a sentinel's own from `sentinelPassword`). The
/// user name, host, port, database and other parameters stay as written.
pub(crate) fn redacted(address: &str) -> Cow<'_, str> {
    let trimmed = address.trim();
    let seed = !trimmed.contains("://");
    let parsed = if seed {
        Url::parse(&format!("{SEED_SCHEME}{trimmed}"))
    } else {
        Url::parse(trimmed)
    };
    let Ok(mut url) = parsed else {
        return unparsed(address);
    };
    let has_password = url.password().is_some();
    if has_password && url.set_password(Some(REDACTED)).is_err() {
        return Cow::Borrowed(HIDDEN);
    }
    let has_query_password = url.query_pairs().any(|(key, _)| names_password(&key));
    if has_query_password {
        let pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(key, value)| {
                let value = if names_password(&key) {
                    REDACTED.to_owned()
                } else {
                    value.into_owned()
                };
                (key.into_owned(), value)
            })
            .collect();
        url.query_pairs_mut().clear().extend_pairs(pairs);
    }
    if !has_password && !has_query_password {
        return Cow::Borrowed(address);
    }
    let printed: String = url.into();
    if seed {
        Cow::Owned(printed[SEED_SCHEME.len()..].to_owned())
    } else {
        Cow::Owned(printed)
    }
}

fn names_password(key: &str) -> bool {
    key.to_ascii_lowercase().contains("password")
}

/// An address the URL parser refuses (a port that is not a number, say) still reaches an error
/// message and a log. Its user info is split at the last `@` before the path, as the parser would
/// split it, and the password after the user name's `:` is replaced; anything else that may hold a
/// password hides the whole address.
fn unparsed(address: &str) -> Cow<'_, str> {
    if names_password(address) {
        return Cow::Borrowed(HIDDEN);
    }
    let authority_start = address.find("://").map_or(0, |scheme| scheme + 3);
    let rest = &address[authority_start..];
    let authority_len = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let Some(at) = rest[..authority_len].rfind('@') else {
        return if rest.contains('@') {
            Cow::Borrowed(HIDDEN)
        } else {
            Cow::Borrowed(address)
        };
    };
    let Some(colon) = rest[..at].find(':') else {
        return Cow::Borrowed(address);
    };
    let password_start = authority_start + colon + 1;
    let password_end = authority_start + at;
    Cow::Owned(format!(
        "{}{REDACTED}{}",
        &address[..password_start],
        &address[password_end..]
    ))
}

#[cfg(test)]
mod tests {
    use super::redacted;

    #[test]
    fn an_address_without_a_password_prints_as_written() {
        for address in [
            "redis://localhost:6379",
            "redis://alice@cache:6379/2?timeout=5s",
            "10.0.0.1:7000",
            "[::1]:6379",
        ] {
            assert_eq!(redacted(address), address);
        }
    }

    #[test]
    fn an_unparsed_address_keeps_everything_but_its_password() {
        assert_eq!(
            redacted("redis://alice:p@ss@node1:notaport/0"),
            "redis://alice:***@node1:notaport/0"
        );
        assert_eq!(redacted("redis://alice@[bad"), "redis://alice@[bad");
        assert_eq!(redacted("redis://[bad"), "redis://[bad");
    }

    #[test]
    fn an_unparsed_address_that_may_hold_a_password_elsewhere_is_hidden() {
        assert_eq!(
            redacted("redis://[bad?sentinelPassword=secret"),
            "<unparsed address hidden>"
        );
        assert_eq!(
            redacted("redis://[bad/path@secret"),
            "<unparsed address hidden>"
        );
    }

    #[test]
    fn a_seed_keeps_its_scheme_less_form() {
        assert_eq!(
            redacted("carol:secret@sentinel1:26379"),
            "carol:***@sentinel1:26379"
        );
    }
}
