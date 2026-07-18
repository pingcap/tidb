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

//! Direct source-row coverage for `pkg/util/codec/bytes.go` and its owned Go
//! byte-codec tests.

use std::cmp::Ordering;

use tidb_codec::{
    decode_bytes, decode_bytes_desc, decode_compact_bytes, encode_bytes, encode_bytes_desc,
    encode_bytes_ext, encode_compact_bytes, encoded_bytes_len, CodecError,
};

#[test]
fn bytes_codec_source_vectors_are_exact() {
    let rows: &[(&[u8], &[u8], bool)] = &[
        (&[], &[0, 0, 0, 0, 0, 0, 0, 0, 247], false),
        (&[], &[255, 255, 255, 255, 255, 255, 255, 255, 8], true),
        (&[0], &[0, 0, 0, 0, 0, 0, 0, 0, 248], false),
        (&[0], &[255, 255, 255, 255, 255, 255, 255, 255, 7], true),
        (&[1, 2, 3], &[1, 2, 3, 0, 0, 0, 0, 0, 250], false),
        (
            &[1, 2, 3],
            &[254, 253, 252, 255, 255, 255, 255, 255, 5],
            true,
        ),
        (&[1, 2, 3, 0], &[1, 2, 3, 0, 0, 0, 0, 0, 251], false),
        (
            &[1, 2, 3, 0],
            &[254, 253, 252, 255, 255, 255, 255, 255, 4],
            true,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7],
            &[1, 2, 3, 4, 5, 6, 7, 0, 254],
            false,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7],
            &[254, 253, 252, 251, 250, 249, 248, 255, 1],
            true,
        ),
        (
            &[0, 0, 0, 0, 0, 0, 0, 0],
            &[0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
            false,
        ),
        (
            &[0, 0, 0, 0, 0, 0, 0, 0],
            &[
                255, 255, 255, 255, 255, 255, 255, 255, 0, 255, 255, 255, 255, 255, 255, 255, 255,
                8,
            ],
            true,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8],
            &[1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
            false,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8],
            &[
                254, 253, 252, 251, 250, 249, 248, 247, 0, 255, 255, 255, 255, 255, 255, 255, 255,
                8,
            ],
            true,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248],
            false,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[
                254, 253, 252, 251, 250, 249, 248, 247, 0, 246, 255, 255, 255, 255, 255, 255, 255,
                7,
            ],
            true,
        ),
    ];

    for &(input, expected, descending) in rows {
        assert_eq!(encoded_bytes_len(input.len()), expected.len());
        let mut encoded = vec![42];
        if descending {
            encode_bytes_desc(&mut encoded, input);
        } else {
            encode_bytes(&mut encoded, input);
        }
        assert_eq!(&encoded[1..], expected);
        assert_eq!(encoded[0], 42);

        let with_leftover = [expected, &[99, 100]].concat();
        let (remain, decoded) = if descending {
            decode_bytes_desc(&with_leftover).unwrap()
        } else {
            decode_bytes(&with_leftover).unwrap()
        };
        assert_eq!(remain, [99, 100]);
        assert_eq!(decoded, input);
    }
}

#[test]
fn bytes_codec_source_error_rows_and_descending_twins_fail() {
    let malformed: &[(&[u8], CodecError)] = &[
        (&[1, 2, 3, 4], CodecError::InsufficientBytes),
        (&[0, 0, 0, 0, 0, 0, 0, 247], CodecError::InsufficientBytes),
        (
            &[0, 0, 0, 0, 0, 0, 0, 0, 246],
            CodecError::InvalidEncoding("invalid bytes marker"),
        ),
        (
            &[0, 0, 0, 0, 0, 0, 0, 1, 247],
            CodecError::InvalidEncoding("invalid bytes padding"),
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 0],
            CodecError::InvalidEncoding("invalid bytes marker"),
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 255, 1],
            CodecError::InsufficientBytes,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8],
            CodecError::InsufficientBytes,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 255],
            CodecError::InsufficientBytes,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 0],
            CodecError::InvalidEncoding("invalid bytes marker"),
        ),
    ];

    for (input, expected) in malformed {
        assert_eq!(decode_bytes(input), Err(expected.clone()));
        let descending: Vec<u8> = input.iter().map(|byte| !byte).collect();
        assert_eq!(decode_bytes_desc(&descending), Err(expected.clone()));
    }
}

#[test]
fn bytes_ext_source_rows_preserve_raw_and_comparable_modes() {
    let rows: &[(&[u8], &[u8])] = &[
        (&[], &[0, 0, 0, 0, 0, 0, 0, 0, 247]),
        (&[1, 2, 3], &[1, 2, 3, 0, 0, 0, 0, 0, 250]),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248],
        ),
    ];

    for &(input, expected) in rows {
        let mut raw = vec![42];
        encode_bytes_ext(&mut raw, input, true);
        assert_eq!(&raw[1..], input);

        let mut comparable = vec![42];
        encode_bytes_ext(&mut comparable, input, false);
        assert_eq!(&comparable[1..], expected);
    }
}

#[test]
fn compact_bytes_close_source_round_trips_and_boundaries() {
    let rows: &[(&[u8], &[u8])] = &[
        (&[], &[0]),
        (&[0, 1], &[4, 0, 1]),
        (&[0xff, 0xff], &[4, 0xff, 0xff]),
        (&[1, 0], &[4, 1, 0]),
        (b"abc", &[6, b'a', b'b', b'c']),
        (
            b"hello world",
            &[
                22, b'h', b'e', b'l', b'l', b'o', b' ', b'w', b'o', b'r', b'l', b'd',
            ],
        ),
    ];

    for &(input, expected) in rows {
        let mut encoded = vec![42];
        encode_compact_bytes(&mut encoded, input);
        assert_eq!(&encoded[1..], expected);

        let with_leftover = [expected, &[99, 100]].concat();
        let (remain, decoded) = decode_compact_bytes(&with_leftover).unwrap();
        assert_eq!(remain, [99, 100]);
        assert_eq!(decoded, input);
    }

    assert_eq!(
        decode_compact_bytes(&[]),
        Err(CodecError::InsufficientBytes)
    );
    assert_eq!(
        decode_compact_bytes(&[0x80]),
        Err(CodecError::InsufficientBytes)
    );
    assert_eq!(
        decode_compact_bytes(&[0x80; 10]),
        Err(CodecError::InvalidEncoding("varint larger than 64 bits"))
    );
    assert_eq!(
        decode_compact_bytes(&[1]),
        Err(CodecError::InsufficientBytes)
    );
    assert_eq!(
        decode_compact_bytes(&[6, 1, 2]),
        Err(CodecError::InsufficientBytes)
    );
}

#[test]
fn compact_bytes_are_explicitly_not_mem_comparable() {
    let mut shorter = Vec::new();
    let mut longer = Vec::new();
    encode_compact_bytes(&mut shorter, &[0xff]);
    encode_compact_bytes(&mut longer, &[0, 0]);
    assert_eq!([0xff].as_slice().cmp(&[0, 0]), Ordering::Greater);
    assert_eq!(shorter.cmp(&longer), Ordering::Less);
}

#[test]
fn compact_bytes_length_prefix_transitions_are_exact() {
    let rows: &[(usize, &[u8])] = &[
        (63, &[126]),
        (64, &[128, 1]),
        (8_191, &[254, 127]),
        (8_192, &[128, 128, 1]),
    ];

    for &(length, prefix) in rows {
        let input = vec![42; length];
        let mut encoded = Vec::new();
        encode_compact_bytes(&mut encoded, &input);
        assert_eq!(&encoded[..prefix.len()], prefix);
        let (remain, decoded) = decode_compact_bytes(&encoded).unwrap();
        assert!(remain.is_empty());
        assert_eq!(decoded, input);
    }
}
