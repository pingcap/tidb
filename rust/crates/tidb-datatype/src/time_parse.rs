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

//! Temporal string parsing, ported from Go `pkg/types`.
//!
//! This is the first slice of TiDB's datetime parser: `ParseDateFormat`
//! tokenizes a date/time literal into its numeric fields, the step every
//! `DATE`/`DATETIME`/`TIMESTAMP` string parse begins with. The richer
//! `parseDatetime` (field interpretation, timezone, fsp) builds on this.

/// Go `isDigit`.
const fn is_digit(c: u8) -> bool {
    c.is_ascii_digit()
}

/// Go `isPunctuation`: an ASCII punctuation character (printable, non-alnum).
const fn is_punctuation(c: u8) -> bool {
    matches!(c, 0x21..=0x2F | 0x3A..=0x40 | 0x5B..=0x60 | 0x7B..=0x7E)
}

/// Go `isValidSeparator`: punctuation is a valid separator anywhere; space and
/// `T` (and the other ASCII whitespace) separate only between the date and time
/// (`prevParts == 2`); after five parts any non-digit ends the field.
const fn is_valid_separator(c: u8, prev_parts: usize) -> bool {
    if is_punctuation(c) {
        return true;
    }
    if prev_parts == 2 && matches!(c, b'T' | b' ' | b'\t' | b'\n' | 0x0b | 0x0c | b'\r') {
        return true;
    }
    prev_parts > 4 && !is_digit(c)
}

/// Faithful port of Go `types.ParseDateFormat`: splits a date/time literal into
/// its numeric field strings, or returns `None` (Go's `nil`) when the literal
/// does not begin with a digit or contains an out-of-place non-digit.
///
/// The literal must start with a digit; punctuation separators are consumed
/// (including runs), a single space/`T` splits date from time, and the trailing
/// field is taken verbatim (so `"2011-11-11x"` yields `["2011","11","11x"]`,
/// exactly as Go leaves the last byte unexamined). Fields are lifted with
/// `from_utf8_lossy`: every valid (ASCII) literal round-trips exactly, and a
/// stray non-ASCII byte — only reachable through the `prev_parts > 4` rule —
/// fails downstream numeric parsing identically to Go's raw-byte string.
#[must_use]
pub fn parse_date_format(format: &str) -> Option<Vec<String>> {
    let format = format.trim();
    let bytes = format.as_bytes();
    if bytes.is_empty() {
        return None;
    }
    // Date format must start with a number.
    if !is_digit(bytes[0]) {
        return None;
    }

    let mut seps: Vec<String> = Vec::with_capacity(6);
    let mut start = 0usize;
    let mut i = 1usize;
    // Go: `for i := 1; i < len(format)-1; i++` — the final byte is never
    // examined and always joins the trailing field.
    while i + 1 < bytes.len() {
        if is_valid_separator(bytes[i], seps.len()) {
            let prev_parts = seps.len();
            seps.push(String::from_utf8_lossy(&bytes[start..i]).into_owned());
            start = i + 1;
            // Consume further consecutive separators.
            let mut j = i + 1;
            while j < bytes.len() {
                if !is_valid_separator(bytes[j], prev_parts) {
                    break;
                }
                start += 1;
                i += 1;
                j += 1;
            }
            i += 1;
            continue;
        }
        if !is_digit(bytes[i]) {
            return None;
        }
        i += 1;
    }
    seps.push(String::from_utf8_lossy(&bytes[start..]).into_owned());
    Some(seps)
}

#[cfg(test)]
mod tests {
    use super::parse_date_format;

    fn parts(items: &[&str]) -> Option<Vec<String>> {
        Some(items.iter().map(|s| (*s).to_string()).collect())
    }

    /// TiDB `TestParseDateFormat` (`pkg/types/time_test.go`).
    #[test]
    fn go_parse_date_format_vectors() {
        let cases: &[(&str, Option<Vec<String>>)] = &[
            (
                "2011-11-11 10:10:10.123456",
                parts(&["2011", "11", "11", "10", "10", "10", "123456"]),
            ),
            (
                "  2011-11-11 10:10:10.123456  ",
                parts(&["2011", "11", "11", "10", "10", "10", "123456"]),
            ),
            ("2011-11-11 10", parts(&["2011", "11", "11", "10"])),
            (
                "2011-11-11T10:10:10.123456",
                parts(&["2011", "11", "11", "10", "10", "10", "123456"]),
            ),
            (
                "2011:11:11T10:10:10.123456",
                parts(&["2011", "11", "11", "10", "10", "10", "123456"]),
            ),
            (
                "2011-11-11  10:10:10",
                parts(&["2011", "11", "11", "10", "10", "10"]),
            ),
            ("xx2011-11-11 10:10:10", None),
            ("T10:10:10", None),
            ("2011-11-11x", parts(&["2011", "11", "11x"])),
            ("xxx 10:10:10", None),
            (
                "2022-02-01\n16:33:00",
                parts(&["2022", "02", "01", "16", "33", "00"]),
            ),
            (
                "2022-02-01\x0c16:33:00",
                parts(&["2022", "02", "01", "16", "33", "00"]),
            ),
            (
                "2022-02-01\x0b16:33:00",
                parts(&["2022", "02", "01", "16", "33", "00"]),
            ),
            (
                "2022-02-01\r16:33:00",
                parts(&["2022", "02", "01", "16", "33", "00"]),
            ),
            (
                "2022-02-01\t16:33:00",
                parts(&["2022", "02", "01", "16", "33", "00"]),
            ),
        ];
        for (input, expected) in cases {
            assert_eq!(
                parse_date_format(input),
                *expected,
                "parse_date_format({input:?})"
            );
        }
    }
}
