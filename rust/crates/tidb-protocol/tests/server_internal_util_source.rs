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

#![allow(missing_docs)]

use std::io::ErrorKind;

use tidb_protocol::{parse_length_encoded_bytes, parse_null_term_string};

#[test]
fn parse_length_encoded_bytes_matches_go_contract() {
    let cases = [
        (&[0xfb][..], (None, true, 1)),
        (&[0x00][..], (None, false, 1)),
        (
            &[0x03, b'a', b'b', b'c'][..],
            (Some(b"abc".to_vec()), false, 4),
        ),
    ];

    for (input, expected) in cases {
        assert_eq!(parse_length_encoded_bytes(input).unwrap(), expected);
    }

    for input in [&[0x01][..], &[0xfe][..], &[0xfc, 0x01][..]] {
        let error = parse_length_encoded_bytes(input).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnexpectedEof);
        assert_eq!(error.to_string(), "EOF");
    }
}

#[test]
fn parse_null_term_string_matches_go_contract() {
    let cases = [
        (&b"abc\0def"[..], &b"abc"[..], &b"def"[..]),
        (&b"\0def"[..], &b""[..], &b"def"[..]),
        (&b"def\0hig\0k"[..], &b"def"[..], &b"hig\0k"[..]),
        (&b"abcdef"[..], &b""[..], &b"abcdef"[..]),
    ];

    for (input, expected_prefix, expected_remain) in cases {
        let (prefix, remain) = parse_null_term_string(input);
        assert_eq!(prefix, expected_prefix);
        assert_eq!(remain, expected_remain);
    }
}
