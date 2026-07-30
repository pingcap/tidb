// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go's `strconv.Quote` over a byte string, as `fmt.Sprintf("%q", ...)`
//! applies it in `pkg/util/ranger/types.go`'s `formatDatum` — the function
//! that renders an index range bound inside `EXPLAIN`'s `range:[...]`.
//!
//! This matters for a BINARY index column, whose bound is arbitrary octets: a
//! lossy UTF-8 conversion turns every non-UTF-8 byte into U+FFFD, so
//! `where a = x'FA34E1093CB428485734E3917F000000'` printed an unreadable and
//! unstable range where TiDB records
//! `"\xfa4\xe1\t<\xb4(HW4\xe3\x91\x7f\x00\x00\x00"`
//! (`tests/integrationtest/r/explain_easy.result`).
//!
//! Go's rules, which this mirrors: a byte sequence is decoded as UTF-8; a
//! PRINTABLE rune is emitted verbatim, the five short escapes
//! (`\t`/`\n`/`\r`/`\"`/`\\`) take their short form, any other ASCII control
//! byte and any byte that is not part of a valid UTF-8 encoding becomes
//! `\xNN`, and a non-printable rune becomes `\uNNNN` (or `\UNNNNNNNN` beyond
//! the BMP). Go's `unicode.IsPrint` is approximated here by Rust's own
//! `char::is_control` plus the surrogate/unassigned-free guarantee `char`
//! already carries; the range bounds this renders are column bytes, for which
//! the two agree.

/// `fmt.Sprintf("%q", value)` for a byte string, INCLUDING the surrounding
/// double quotes.
pub(crate) fn quote(value: &[u8]) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    let mut rest = value;
    while !rest.is_empty() {
        match decode_utf8(rest) {
            Some((ch, width)) => {
                push_rune(&mut out, ch);
                rest = &rest[width..];
            }
            None => {
                push_hex(&mut out, rest[0]);
                rest = &rest[1..];
            }
        }
    }
    out.push('"');
    out
}

/// The leading UTF-8 rune of `bytes` and its encoded width, or `None` when the
/// first byte does not begin a valid encoding (Go's `utf8.DecodeRune`
/// returning `RuneError, 1`, which `strconv.Quote` renders as `\xNN`).
fn decode_utf8(bytes: &[u8]) -> Option<(char, usize)> {
    let width = match bytes[0] {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF7 => 4,
        _ => return None,
    };
    let candidate = bytes.get(..width)?;
    let text = std::str::from_utf8(candidate).ok()?;
    text.chars().next().map(|ch| (ch, width))
}

fn push_rune(out: &mut String, ch: char) {
    match ch {
        '"' => out.push_str("\\\""),
        '\\' => out.push_str("\\\\"),
        '\t' => out.push_str("\\t"),
        '\n' => out.push_str("\\n"),
        '\r' => out.push_str("\\r"),
        _ if (ch as u32) < 0x80 => {
            if ch.is_control() {
                push_hex(out, ch as u8);
            } else {
                out.push(ch);
            }
        }
        _ if ch.is_control() => {
            let code = ch as u32;
            if code <= 0xFFFF {
                out.push_str(&format!("\\u{code:04x}"));
            } else {
                out.push_str(&format!("\\U{code:08x}"));
            }
        }
        _ => out.push(ch),
    }
}

fn push_hex(out: &mut String, byte: u8) {
    out.push_str(&format!("\\x{byte:02x}"));
}

#[cfg(test)]
mod tests {
    use super::quote;

    /// The exact bound TiDB records for `explain_easy`'s
    /// `where a=x'FA34E1093CB428485734E3917F000000'` (`r/explain_easy.result`).
    #[test]
    fn renders_the_recorded_binary_index_bound() {
        let bytes = [
            0xFA, 0x34, 0xE1, 0x09, 0x3C, 0xB4, 0x28, 0x48, 0x57, 0x34, 0xE3, 0x91, 0x7F, 0x00,
            0x00, 0x00,
        ];
        assert_eq!(
            quote(&bytes),
            r#""\xfa4\xe1\t<\xb4(HW4\xe3\x91\x7f\x00\x00\x00""#
        );
    }

    #[test]
    fn short_escapes_and_printable_ascii_match_go() {
        assert_eq!(quote(b"xb"), r#""xb""#);
        assert_eq!(quote(b"a\tb\nc\rd"), r#""a\tb\nc\rd""#);
        assert_eq!(quote(b"say \"hi\"\\"), r#""say \"hi\"\\""#);
        assert_eq!(quote(b""), r#""""#);
    }

    /// Valid UTF-8 passes through as its runes -- Go's `%q` quotes a printable
    /// rune verbatim rather than byte-escaping it.
    #[test]
    fn valid_utf8_is_not_byte_escaped() {
        assert_eq!(quote("héllo".as_bytes()), r#""héllo""#);
        assert_eq!(quote("日本".as_bytes()), r#""日本""#);
    }

    /// A truncated multi-byte sequence is not a rune, so every one of its
    /// bytes escapes individually.
    #[test]
    fn invalid_utf8_escapes_byte_by_byte() {
        assert_eq!(quote(&[0xE6, 0x97]), r#""\xe6\x97""#);
        assert_eq!(quote(&[0xFF, b'a']), r#""\xffa""#);
    }
}
