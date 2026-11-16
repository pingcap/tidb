// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//! Go `strconv.Unquote` and `strconv.UnquoteChar` (`strconv/quote.go`).
//! Go strings are bytes, so the input and the result are bytes: a `\x`
//! escape can produce bytes that are not UTF-8.

/// Go `strconv.ErrSyntax` from [`unquote`] and [`unquote_char`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SyntaxError;

impl std::fmt::Display for SyntaxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(crate::ERR_SYNTAX)
    }
}

impl std::error::Error for SyntaxError {}

fn unhex(b: u8) -> Option<u32> {
    match b {
        b'0'..=b'9' => Some(u32::from(b - b'0')),
        b'a'..=b'f' => Some(u32::from(b - b'a' + 10)),
        b'A'..=b'F' => Some(u32::from(b - b'A' + 10)),
        _ => None,
    }
}

/// Go `utf8.DecodeRune`: the rune and its width; `U+FFFD` and 1 for an
/// invalid encoding.
fn decode_rune(s: &[u8]) -> (u32, usize) {
    let end = s.len().min(4);
    for n in 1..=end {
        if let Ok(t) = std::str::from_utf8(&s[..n]) {
            if let Some(c) = t.chars().next() {
                return (c as u32, n);
            }
        }
    }
    (0xFFFD, 1)
}

/// Go `UnquoteChar`: the first character or escape of `s`, whether it is
/// a multibyte rune, and the rest of `s`.
///
/// # Errors
/// [`SyntaxError`].
pub fn unquote_char(s: &[u8], quote: u8) -> Result<(u32, bool, &[u8]), SyntaxError> {
    let Some(&c) = s.first() else { return Err(SyntaxError) };
    if c == quote && (quote == b'\'' || quote == b'"') {
        return Err(SyntaxError);
    }
    if c >= 0x80 {
        let (r, size) = decode_rune(s);
        return Ok((r, true, &s[size..]));
    }
    if c != b'\\' {
        return Ok((u32::from(c), false, &s[1..]));
    }
    if s.len() <= 1 {
        return Err(SyntaxError);
    }
    let c = s[1];
    let mut s = &s[2..];
    let mut multibyte = false;
    let value = match c {
        b'a' => 7,
        b'b' => 8,
        b'f' => 12,
        b'n' => 10,
        b'r' => 13,
        b't' => 9,
        b'v' => 11,
        b'x' | b'u' | b'U' => {
            let n = match c {
                b'x' => 2,
                b'u' => 4,
                _ => 8,
            };
            if s.len() < n {
                return Err(SyntaxError);
            }
            let mut v: u32 = 0;
            for &b in &s[..n] {
                v = v << 4 | unhex(b).ok_or(SyntaxError)?;
            }
            s = &s[n..];
            if c != b'x' {
                if char::from_u32(v).is_none() {
                    return Err(SyntaxError);
                }
                multibyte = true;
            }
            v
        }
        b'0'..=b'7' => {
            let mut v = u32::from(c - b'0');
            if s.len() < 2 {
                return Err(SyntaxError);
            }
            for &b in &s[..2] {
                if !(b'0'..=b'7').contains(&b) {
                    return Err(SyntaxError);
                }
                v = (v << 3) | u32::from(b - b'0');
            }
            s = &s[2..];
            if v > 255 {
                return Err(SyntaxError);
            }
            v
        }
        b'\\' => u32::from(b'\\'),
        b'\'' | b'"' => {
            if c != quote {
                return Err(SyntaxError);
            }
            u32::from(c)
        }
        _ => return Err(SyntaxError),
    };
    Ok((value, multibyte, s))
}

/// Go `Unquote`: the contents of a single-, double- or back-quoted Go
/// string literal.
///
/// # Errors
/// [`SyntaxError`].
pub fn unquote(input: &[u8]) -> Result<Vec<u8>, SyntaxError> {
    if input.len() < 2 {
        return Err(SyntaxError);
    }
    let quote = input[0];
    let Some(end) = input[1..].iter().position(|&b| b == quote) else { return Err(SyntaxError) };
    let end = end + 2;
    match quote {
        b'`' => {
            if end != input.len() {
                return Err(SyntaxError);
            }
            Ok(input[1..end - 1].iter().copied().filter(|&b| b != b'\r').collect())
        }
        b'"' | b'\'' => {
            let head = &input[..end];
            if !head.contains(&b'\\') && !head.contains(&b'\n') {
                let inner = &input[1..end - 1];
                let valid = if quote == b'"' {
                    std::str::from_utf8(inner).is_ok()
                } else {
                    let (r, n) = decode_rune(inner);
                    !inner.is_empty() && 1 + n + 1 == end && (r != 0xFFFD || n != 1)
                };
                if valid {
                    return if end == input.len() { Ok(inner.to_vec()) } else { Err(SyntaxError) };
                }
            }
            let mut buf = Vec::with_capacity(3 * end / 2);
            let mut rest = &input[1..];
            while let Some(&first) = rest.first() {
                if first == quote {
                    break;
                }
                if first == b'\n' {
                    return Err(SyntaxError);
                }
                let (r, multibyte, rem) = unquote_char(rest, quote)?;
                rest = rem;
                if r < 0x80 || !multibyte {
                    buf.push(r as u8);
                } else {
                    let c = char::from_u32(r).unwrap_or('\u{FFFD}');
                    let mut tmp = [0; 4];
                    buf.extend_from_slice(c.encode_utf8(&mut tmp).as_bytes());
                }
                if quote == b'\'' {
                    break;
                }
            }
            if rest.first() != Some(&quote) {
                return Err(SyntaxError);
            }
            if rest.len() > 1 {
                return Err(SyntaxError);
            }
            Ok(buf)
        }
        _ => Err(SyntaxError),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `quote_test.go`'s `unquotetests` and `misquoted` (a sample).
    #[test]
    fn unquote_matches_go() {
        for (input, want) in [
            (r#""""#, &b""[..]),
            (r#""a""#, b"a"),
            (r#""abc""#, b"abc"),
            (r#""☺""#, "☺".as_bytes()),
            (r#""hello world""#, b"hello world"),
            (r#""\xFF""#, b"\xFF"),
            (r#""\377""#, b"\xFF"),
            (r#""\u1234""#, "\u{1234}".as_bytes()),
            (r#""\U00010111""#, "\u{10111}".as_bytes()),
            (r#""\a\b\f\n\r\t\v\\\"""#, b"\x07\x08\x0c\n\r\t\x0b\\\""),
            ("'a'", b"a"),
            (r"'\''", b"'"),
            ("`a`", b"a"),
            ("`\\n`", b"\\n"),
            ("`a\rb`", b"ab"),
        ] {
            assert_eq!(unquote(input.as_bytes()).unwrap(), want, "{input}");
        }
        for bad in [
            "", "\"", "\"a", "\"'", "b\"", "\"\\\"", "\"\\9\"", "\"\\19\"", "\"\\129\"", "'\\'", "'\\9'", "'ab'",
            "\"\\x1\"", "\"\\z\"", "`", "`xxx", "``x\r", "\"\n\"", "'\\\"'", "\"\\'\"", "\"a\"b", "\"\\400\"",
        ] {
            assert!(unquote(bad.as_bytes()).is_err(), "{bad:?}");
        }
    }
}
