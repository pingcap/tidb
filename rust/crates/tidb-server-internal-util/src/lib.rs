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

//! Go `pkg/server/internal/util` (`util.go`): the length-encoded and
//! NUL-terminated field parsers the protocol reads with.

use std::io;

/// Decodes one MySQL length-encoded integer from the front of `bytes`.
///
/// Ported from `util.ParseLengthEncodedInt` (`pkg/server/internal/util`).
/// Returns `(value, is_null, consumed)`. `0xfb` is the NULL marker; `0xfc`,
/// `0xfd`, `0xfe` introduce 2, 3, and 8 little-endian bytes; any other first
/// byte is itself the value. Returns `None` on truncation (Go's `io.EOF`),
/// which every caller maps to [`BinaryParamError::MalformedPacket`].
#[must_use]
pub fn parse_length_encoded_int(bytes: &[u8]) -> Option<(u64, bool, usize)> {
    let first = *bytes.first()?;
    match first {
        0xfb => Some((0, true, 1)),
        0xfc => {
            let b = bytes.get(..3)?;
            Some((u64::from(b[1]) | (u64::from(b[2]) << 8), false, 3))
        }
        0xfd => {
            let b = bytes.get(..4)?;
            Some((
                u64::from(b[1]) | (u64::from(b[2]) << 8) | (u64::from(b[3]) << 16),
                false,
                4,
            ))
        }
        0xfe => {
            let b = bytes.get(..9)?;
            let value = u64::from(b[1])
                | (u64::from(b[2]) << 8)
                | (u64::from(b[3]) << 16)
                | (u64::from(b[4]) << 24)
                | (u64::from(b[5]) << 32)
                | (u64::from(b[6]) << 40)
                | (u64::from(b[7]) << 48)
                | (u64::from(b[8]) << 56);
            Some((value, false, 9))
        }
        // 0-250 are the value; 0xff is undefined and reaches here as its value.
        _ => Some((u64::from(first), false, 1)),
    }
}

/// Decodes one MySQL length-encoded byte slice from the front of `bytes`.
///
/// Ported from Go `ParseLengthEncodedBytes` (`pkg/server/internal/util`).
/// Returns `(value, is_null, consumed)`. `0xfb` is the NULL marker; `0x00`
/// through `0xfa` are the value's own length prefix; any other first byte is
/// parsed via [`parse_length_encoded_int`]. Returns `UnexpectedEof` on
/// truncation (Go's `io.EOF`).
pub fn parse_length_encoded_bytes(
    bytes: &[u8],
) -> Result<(Option<Vec<u8>>, bool, usize), io::Error> {
    let (length, is_null, consumed) = parse_length_encoded_int(bytes)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"))?;
    if length == 0 {
        return Ok((None, is_null, consumed));
    }

    let length =
        usize::try_from(length).map_err(|_| io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"))?;
    let end = consumed
        .checked_add(length)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"))?;
    let value = bytes
        .get(consumed..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "EOF"))?;
    Ok((Some(value.to_vec()), false, end))
}

/// Splits a null-terminated byte string into the prefix and the remainder.
///
/// Ported from Go `ParseNullTermString` (`pkg/server/internal/util`). When no
/// terminator is present, the prefix is empty and the remainder is the entire
/// input, matching the Go helper's `(nil, input)` result.
#[must_use]
pub fn parse_null_term_string(bytes: &[u8]) -> (&[u8], &[u8]) {
    match bytes.iter().position(|byte| *byte == 0) {
        Some(off) => (&bytes[..off], &bytes[off + 1..]),
        None => (&[], bytes),
    }
}
