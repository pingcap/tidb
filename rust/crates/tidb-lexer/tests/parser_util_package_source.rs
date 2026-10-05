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

//! Direct source tests for `pkg/parser/util/escape.go`.

use tidb_lexer::unescape_char;

#[test]
fn unescape_char_matches_every_go_vector() {
    let cases: &[(u8, &[u8])] = &[
        (b'n', b"\n"),
        (b'0', &[0]),
        (b'b', &[8]),
        (b'Z', &[26]),
        (b'r', b"\r"),
        (b't', b"\t"),
        (b'%', b"\\%"),
        (b'_', b"\\_"),
        (b'\\', b"\\"),
        (b'\'', b"'"),
        (b'"', b"\""),
        (b'a', b"a"),
        (b'z', b"z"),
        (b'1', b"1"),
        (b' ', b" "),
    ];
    for (input, expected) in cases {
        assert_eq!(unescape_char(*input), *expected, "input {input:#04x}");
    }
}

#[test]
fn every_byte_has_the_exact_go_result() {
    for byte in u8::MIN..=u8::MAX {
        let actual = unescape_char(byte);
        let expected = match byte {
            b'n' => vec![b'\n'],
            b'0' => vec![0],
            b'b' => vec![8],
            b'Z' => vec![26],
            b'r' => vec![b'\r'],
            b't' => vec![b'\t'],
            b'%' | b'_' => vec![b'\\', byte],
            _ => vec![byte],
        };
        assert_eq!(actual, expected, "input {byte:#04x}");
    }
}
