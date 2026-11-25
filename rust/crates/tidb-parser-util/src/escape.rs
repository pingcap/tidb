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

//! MySQL byte unescaping from `pkg/parser/util/escape.go`.

/// Returns the replacement bytes for the byte following a MySQL backslash.
///
/// `\%` and `\_` retain the backslash because they remain pattern escapes;
/// every other unrecognized escape discards the backslash.
pub fn unescape_char(byte: u8) -> Vec<u8> {
    if matches!(byte, b'%' | b'_') {
        vec![b'\\', byte]
    } else {
        vec![unescape_byte(byte)]
    }
}

/// Appends a decoded backslash escape to an existing string-literal buffer.
#[inline]
pub fn append_unescaped_char(output: &mut Vec<u8>, byte: u8) {
    if matches!(byte, b'%' | b'_') {
        output.extend_from_slice(&[b'\\', byte]);
    } else {
        output.push(unescape_byte(byte));
    }
}

fn unescape_byte(byte: u8) -> u8 {
    match byte {
        b'n' => b'\n',
        b'0' => 0,
        b'b' => 8,
        b'Z' => 26,
        b'r' => b'\r',
        b't' => b'\t',
        _ => byte,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go: pkg/parser/util/escape_test.go, TestUnescapeChar.
    #[test]
    fn unescape_char_go_table() {
        let cases: &[(u8, &[u8])] = &[
            // Standard single-byte escapes
            (b'n', b"\n"),
            (b'0', &[0]),
            (b'b', &[8]),
            (b'Z', &[26]),
            (b'r', b"\r"),
            (b't', b"\t"),
            // Preserve both backslash and character
            (b'%', b"\\%"),
            (b'_', b"\\_"),
            // Self-escaping characters (backslash removed)
            (b'\\', b"\\"),
            (b'\'', b"'"),
            (b'"', b"\""),
            // Any other character just returns itself (backslash removed)
            (b'a', b"a"),
            (b'z', b"z"),
            (b'1', b"1"),
            (b' ', b" "),
        ];
        for (input, want) in cases {
            assert_eq!(&unescape_char(*input), want, "UnescapeChar({input:?})");
        }
    }
}
