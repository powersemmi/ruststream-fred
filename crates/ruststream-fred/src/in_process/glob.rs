//! The glob a `PSUBSCRIBE` pattern is, matched the way Redis matches it.
//!
//! `*` matches any run of bytes (a `.` or a `/` included), `?` one byte, `[abc]` and `[a-z]` one
//! byte of a set, `[^abc]` one byte outside it, and `\` makes the next byte literal.

/// Whether `pattern` matches the whole of `text`.
pub(crate) fn matches(pattern: &[u8], text: &[u8]) -> bool {
    let Some((&first, rest)) = pattern.split_first() else {
        return text.is_empty();
    };
    match first {
        b'*' => {
            // A run of stars matches what one does.
            let rest = trim_stars(rest);
            if rest.is_empty() {
                return true;
            }
            (0..=text.len()).any(|at| matches(rest, &text[at..]))
        }
        b'?' => text
            .split_first()
            .is_some_and(|(_, text)| matches(rest, text)),
        b'[' => {
            let Some((&byte, text)) = text.split_first() else {
                return false;
            };
            let (hit, rest) = class(rest, byte);
            hit && matches(rest, text)
        }
        b'\\' if !rest.is_empty() => text
            .split_first()
            .is_some_and(|(&byte, text)| byte == rest[0] && matches(&rest[1..], text)),
        literal => text
            .split_first()
            .is_some_and(|(&byte, text)| byte == literal && matches(rest, text)),
    }
}

fn trim_stars(mut pattern: &[u8]) -> &[u8] {
    while let Some((b'*', rest)) = pattern.split_first() {
        pattern = rest;
    }
    pattern
}

/// Matches `byte` against the class opening `pattern` (past its `[`), and returns the pattern
/// after the class. An unclosed class runs to the end of the pattern, as it does in Redis.
fn class(pattern: &[u8], byte: u8) -> (bool, &[u8]) {
    let (negated, mut at) = match pattern.first() {
        Some(b'^') => (true, 1),
        _ => (false, 0),
    };
    let mut hit = false;
    while at < pattern.len() && pattern[at] != b']' {
        if pattern[at] == b'\\' && at + 1 < pattern.len() {
            hit |= pattern[at + 1] == byte;
            at += 2;
        } else if at + 2 < pattern.len() && pattern[at + 1] == b'-' && pattern[at + 2] != b']' {
            let (low, high) = (
                pattern[at].min(pattern[at + 2]),
                pattern[at].max(pattern[at + 2]),
            );
            hit |= (low..=high).contains(&byte);
            at += 3;
        } else {
            hit |= pattern[at] == byte;
            at += 1;
        }
    }
    let rest = pattern.get(at + 1..).unwrap_or_default();
    (hit != negated, rest)
}

#[cfg(test)]
mod tests {
    use super::matches;

    #[test]
    fn a_star_crosses_every_separator() {
        assert!(matches(b"orders.*", b"orders.eu"));
        assert!(matches(b"orders.*", b"orders.eu.north"));
        assert!(matches(b"*", b""));
        assert!(!matches(b"orders.*", b"payments.eu"));
    }

    #[test]
    fn a_question_mark_is_one_byte() {
        assert!(matches(b"h?llo", b"hello"));
        assert!(!matches(b"h?llo", b"hllo"));
    }

    #[test]
    fn classes_ranges_negation_and_escapes() {
        assert!(matches(b"h[ae]llo", b"hallo"));
        assert!(!matches(b"h[ae]llo", b"hillo"));
        assert!(matches(b"h[^e]llo", b"hallo"));
        assert!(!matches(b"h[^e]llo", b"hello"));
        assert!(matches(b"h[a-b]llo", b"hbllo"));
        assert!(matches(b"a\\*b", b"a*b"));
        assert!(!matches(b"a\\*b", b"axb"));
    }
}
