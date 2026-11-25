// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.

//! Go `strconv.Unquote` and `strconv.UnquoteChar` (`strconv/quote.go`).
//! Go strings are bytes, so the input and the result are bytes: a `\x`
//! escape can produce bytes that are not UTF-8.

use std::borrow::Cow;

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
    let digit = b.wrapping_sub(b'0');
    if digit < 10 {
        return Some(u32::from(digit));
    }
    let letter = (b | 0x20).wrapping_sub(b'a');
    (letter < 6).then_some(u32::from(letter) + 10)
}

/// Go `utf8.DecodeRune`: the rune and its width; `U+FFFD` and 1 for an
/// invalid encoding.
fn decode_rune(s: &[u8]) -> (u32, usize) {
    match utf8_iter::ErrorReportingUtf8Chars::new(s).next() {
        Some(Ok(ch)) => (ch as u32, ch.len_utf8()),
        _ => (0xFFFD, 1),
    }
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
#[inline]
pub fn unquote(input: &[u8]) -> Result<Vec<u8>, SyntaxError> {
    unquote_view(input).map(Cow::into_owned)
}

/// Decodes a Go string literal, borrowing its contents when they need no changes.
///
/// # Errors
/// Returns [`SyntaxError`] if the input is not a valid quoted literal.
pub fn unquote_view(input: &[u8]) -> Result<Cow<'_, [u8]>, SyntaxError> {
    let (&quote, body) = input.split_first().ok_or(SyntaxError)?;
    let body = body.strip_suffix(&[quote]).ok_or(SyntaxError)?;
    match quote {
        b'`' => unquote_raw(body),
        b'"' => unquote_string(body),
        b'\'' if body.is_empty() => Ok(Cow::Borrowed(body)),
        b'\'' if body.first() != Some(&b'\n') => {
            let (rune, multibyte, rest) = unquote_char(body, quote)?;
            if !rest.is_empty() {
                return Err(SyntaxError);
            }
            if body[0] != b'\\' && (rune != 0xfffd || body.len() != 1) {
                return Ok(Cow::Borrowed(body));
            }
            let mut result = Vec::with_capacity(body.len());
            append_rune(&mut result, rune, multibyte);
            Ok(Cow::Owned(result))
        }
        _ => Err(SyntaxError),
    }
}

fn unquote_raw(body: &[u8]) -> Result<Cow<'_, [u8]>, SyntaxError> {
    let Some(first) = memchr::memchr2(b'`', b'\r', body) else {
        return Ok(Cow::Borrowed(body));
    };
    let (prefix, rest) = body.split_at(first);
    if rest[0] == b'`' || memchr::memchr(b'`', &rest[1..]).is_some() {
        return Err(SyntaxError);
    }
    let mut result = Vec::with_capacity(body.len() - 1);
    result.extend_from_slice(prefix);
    result.extend(rest[1..].iter().copied().filter(|&byte| byte != b'\r'));
    Ok(Cow::Owned(result))
}

fn unquote_string(mut body: &[u8]) -> Result<Cow<'_, [u8]>, SyntaxError> {
    let first = memchr::memchr3(b'"', b'\\', b'\n', body);
    if first.is_none() && (body.is_ascii() || std::str::from_utf8(body).is_ok()) {
        return Ok(Cow::Borrowed(body));
    }
    if first.is_some_and(|index| body[index] != b'\\') {
        return Err(SyntaxError);
    }

    let mut result = Vec::with_capacity(body.len());
    let mut end = first.unwrap_or(body.len());
    loop {
        let (literal, rest) = body.split_at(end);
        append_literal(&mut result, literal);
        match rest.first() {
            None => return Ok(Cow::Owned(result)),
            Some(b'\\') => {
                let (rune, multibyte, rest) = unquote_char(rest, b'"')?;
                append_rune(&mut result, rune, multibyte);
                body = rest;
                // Escapes often occur next to each other. Check the next few
                // bytes directly before searching the rest in blocks.
                let (head, tail) = body.split_at(body.len().min(4));
                end = head
                    .iter()
                    .position(|&byte| matches!(byte, b'"' | b'\\' | b'\n'))
                    .unwrap_or_else(|| {
                        head.len() + memchr::memchr3(b'"', b'\\', b'\n', tail).unwrap_or(tail.len())
                    });
            }
            Some(_) => return Err(SyntaxError),
        }
    }
}

fn append_literal(result: &mut Vec<u8>, literal: &[u8]) {
    match literal {
        [] => {}
        [byte] if byte.is_ascii() => result.push(*byte),
        _ if std::str::from_utf8(literal).is_ok() => result.extend_from_slice(literal),
        _ => {
            for chunk in literal.utf8_chunks() {
                result.extend_from_slice(chunk.valid().as_bytes());
                // Go's DecodeRune consumes one byte per invalid encoding.
                for _ in chunk.invalid() {
                    result.extend_from_slice("\u{FFFD}".as_bytes());
                }
            }
        }
    }
}

fn append_rune(result: &mut Vec<u8>, rune: u32, multibyte: bool) {
    if rune < 0x80 || !multibyte {
        result.push(rune as u8);
    } else {
        let rune = char::from_u32(rune).expect("unquote_char returned a Unicode scalar");
        result.extend_from_slice(rune.encode_utf8(&mut [0; 4]).as_bytes());
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
            ("''", b""),
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
        assert_eq!(unquote_char(b"\xf0\x9f", b'"'), Ok((0xFFFD, true, &b"\x9f"[..])));
        for bad in [
            "", "\"", "\"a", "\"'", "b\"", "\"\\\"", "\"\\9\"", "\"\\19\"", "\"\\129\"", "'\\'", "'\\9'", "'ab'",
            "\"\\x1\"", "\"\\z\"", "`", "`xxx", "``x\r", "\"\n\"", "'\\\"'", "\"\\'\"", "\"a\"b", "\"\\400\"",
        ] {
            assert!(unquote(bad.as_bytes()).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn unquote_view_borrows_unchanged_content() {
        for input in [b"\"plain\"".as_slice(), b"'\"'", b"`\xff`"] {
            let Cow::Borrowed(body) = unquote_view(input).unwrap() else {
                panic!("unchanged content should borrow the input");
            };
            assert_eq!(body.as_ptr(), input[1..].as_ptr());
            assert_eq!(body, &input[1..input.len() - 1]);
        }
        for (input, expected) in [
            (b"\"\\xFF\"".as_slice(), b"\xff".as_slice()),
            (b"'\xff'", "\u{fffd}".as_bytes()),
            (b"`a\rb`", b"ab"),
        ] {
            let Cow::Owned(body) = unquote_view(input).unwrap() else {
                panic!("decoded content should own the output");
            };
            assert_eq!(body, expected);
        }
    }

    #[test]
    fn unquote_copies_literal_runs_around_escapes() {
        let mut input = vec![b'"'];
        let prefix = "a".repeat(4096);
        input.extend_from_slice(prefix.as_bytes());
        input.extend_from_slice(b"\xe2\x82\\xFF\\\"\xc3\xa9\x80\\n\"");
        let mut expected = prefix.into_bytes();
        expected.extend_from_slice("\u{fffd}\u{fffd}".as_bytes());
        expected.extend_from_slice(b"\xff\"\xc3\xa9");
        expected.extend_from_slice("\u{fffd}\n".as_bytes());
        assert_eq!(unquote(&input).unwrap(), expected);

        input.pop();
        assert_eq!(unquote(&input), Err(SyntaxError));
        input.extend_from_slice(b"\n\"");
        assert_eq!(unquote(&input), Err(SyntaxError));
        input.truncate(4097);
        input.extend_from_slice(b"\"suffix\"");
        assert_eq!(unquote(&input), Err(SyntaxError));
    }
}
