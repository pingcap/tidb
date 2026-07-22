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

//! `CHAR(N)` length admission, the string-column subset of Go
//! `types.ProduceStrWithSpecifiedTp`.

use std::{error::Error, fmt};

use crate::collation::{decode_rune, go_rune_count};

/// A value longer than a `CHAR(N)` column admits in strict `sql_mode`.
///
/// Mirrors Go `types.ErrDataTooLong`: the length is a CHARACTER count (`flen`),
/// not a byte count.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DataTooLongError {
    /// The column's declared character length.
    pub flen: usize,
    /// The value's actual character length.
    pub char_len: usize,
}

impl fmt::Display for DataTooLongError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Data Too Long, field len {}, data len {}",
            self.flen, self.char_len
        )
    }
}

impl Error for DataTooLongError {}

/// Admits a string into a non-binary `utf8mb4` `CHAR(flen)` column in strict
/// `sql_mode`.
///
/// Ports the `CHAR` path of Go `types.ProduceStrWithSpecifiedTp`: the limit is
/// measured in CHARACTERS (runes), not bytes, so a value of at most `flen`
/// characters is admitted unchanged even when it is more than `flen` bytes wide.
/// A longer value whose overflow past `flen` characters is entirely trailing
/// ASCII whitespace is truncated to `flen` characters — `CHAR` ignores trailing
/// spaces — and any other over-length value is [`DataTooLongError`] (strict
/// `sql_mode`, TiDB's default; the non-strict truncate-with-warning behavior is
/// not modeled here).
pub fn produce_char_value(value: &[u8], flen: usize) -> Result<Vec<u8>, DataTooLongError> {
    // A byte length within `flen` can never exceed `flen` characters — the fast
    // path Go takes with the same `len(s) > flen` guard.
    if value.len() <= flen {
        return Ok(value.to_vec());
    }
    let char_len = go_rune_count(value);
    if char_len <= flen {
        return Ok(value.to_vec());
    }

    // Byte offset just past the `flen`-th character: the truncation point.
    let mut offset = 0;
    let mut characters = 0;
    while characters < flen && offset < value.len() {
        offset += decode_rune(&value[offset..]).map_or(1, |(_, width)| width);
        characters += 1;
    }
    let overflow = &value[offset..];

    // Go trims the overflow of `" \t\n\r"` and drops it when nothing remains; an
    // all-whitespace overflow is truncated away rather than rejected.
    let all_whitespace = overflow
        .iter()
        .all(|byte| matches!(byte, b' ' | b'\t' | b'\n' | b'\r'));
    if all_whitespace {
        Ok(value[..offset].to_vec())
    } else {
        Err(DataTooLongError { flen, char_len })
    }
}

#[cfg(test)]
mod tests {
    use super::{produce_char_value, DataTooLongError};

    #[test]
    fn a_value_within_the_character_limit_is_admitted_unchanged() {
        assert_eq!(produce_char_value(b"hello", 5), Ok(b"hello".to_vec()));
        assert_eq!(produce_char_value(b"hi", 5), Ok(b"hi".to_vec()));
        assert_eq!(produce_char_value(b"", 0), Ok(Vec::new()));
    }

    #[test]
    fn the_limit_counts_characters_not_bytes() {
        // Three 3-byte characters (9 bytes) fit in CHAR(3) even though the byte
        // length exceeds the character limit.
        let three_chars = "€€€".as_bytes();
        assert_eq!(three_chars.len(), 9);
        assert_eq!(
            produce_char_value(three_chars, 3),
            Ok(three_chars.to_vec())
        );
        // A fourth character overflows.
        let four_chars = "€€€€".as_bytes();
        assert_eq!(
            produce_char_value(four_chars, 3),
            Err(DataTooLongError {
                flen: 3,
                char_len: 4
            })
        );
    }

    #[test]
    fn a_value_one_character_too_long_is_rejected() {
        assert_eq!(
            produce_char_value(b"abcdef", 5),
            Err(DataTooLongError {
                flen: 5,
                char_len: 6
            })
        );
    }

    #[test]
    fn overflow_that_is_only_trailing_whitespace_is_truncated_not_rejected() {
        // CHAR ignores trailing spaces: "abc   " into CHAR(3) keeps "abc".
        assert_eq!(produce_char_value(b"abc   ", 3), Ok(b"abc".to_vec()));
        assert_eq!(produce_char_value(b"abc\t\n\r", 3), Ok(b"abc".to_vec()));
        // But a non-space character in the overflow is still too long.
        assert_eq!(
            produce_char_value(b"abc  x", 3),
            Err(DataTooLongError {
                flen: 3,
                char_len: 6
            })
        );
    }
}
