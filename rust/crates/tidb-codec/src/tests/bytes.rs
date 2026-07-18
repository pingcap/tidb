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

use crate::*;
use std::cmp::Ordering;

#[test]
fn comparable_bytes_source_rows_round_trip_and_order() {
    let examples: &[(&[u8], Option<&[u8]>)] = &[
        (&[], Some(&[0, 0, 0, 0, 0, 0, 0, 0, 247])),
        (&[1, 2, 3], Some(&[1, 2, 3, 0, 0, 0, 0, 0, 250])),
        (&[1, 2, 3, 0], Some(&[1, 2, 3, 0, 0, 0, 0, 0, 251])),
        (&[0, 1], None),
        (&[0xff, 0xff], None),
        (&[1, 0], None),
        (b"abc", None),
        (b"hello world", None),
    ];
    for (input, expected) in examples {
        let mut encoded = Vec::new();
        encode_bytes(&mut encoded, input);
        if let Some(expected) = expected {
            assert_eq!(&encoded, expected);
        }
        let (remain, decoded) = decode_bytes(&encoded).unwrap();
        assert!(remain.is_empty());
        assert_eq!(&decoded, input);

        let mut descending = Vec::new();
        encode_bytes_desc(&mut descending, input);
        let (remain, decoded) = decode_bytes_desc(&descending).unwrap();
        assert!(remain.is_empty());
        assert_eq!(&decoded, input);
    }

    let comparisons: &[(&[u8], &[u8], Ordering)] = &[
        (&[], &[0], Ordering::Less),
        (&[0], &[0], Ordering::Equal),
        (&[0xff], &[0], Ordering::Greater),
        (&[0xff], &[0xff, 0], Ordering::Less),
        (b"a", b"b", Ordering::Less),
        (b"a", &[0], Ordering::Greater),
        (&[0], &[1], Ordering::Less),
        (&[0, 1], &[0, 0], Ordering::Greater),
        (&[0, 0, 0], &[0, 0], Ordering::Greater),
        (&[0; 8], &[0; 9], Ordering::Less),
        (&[1, 2, 3, 0], &[1, 2, 3], Ordering::Greater),
        (&[1, 3, 3, 4], &[1, 3, 3, 5], Ordering::Less),
        (
            &[1, 2, 3, 4, 5, 6, 7],
            &[1, 2, 3, 4, 5, 6, 7, 8],
            Ordering::Less,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 9],
            &[1, 2, 3, 4, 5, 6, 7, 8],
            Ordering::Greater,
        ),
        (
            &[1, 2, 3, 4, 5, 6, 7, 8, 0],
            &[1, 2, 3, 4, 5, 6, 7, 8],
            Ordering::Greater,
        ),
    ];
    for (left, right, expected) in comparisons {
        let mut left_encoded = Vec::new();
        let mut right_encoded = Vec::new();
        encode_bytes(&mut left_encoded, left);
        encode_bytes(&mut right_encoded, right);
        assert_eq!(left_encoded.cmp(&right_encoded), *expected);

        left_encoded.clear();
        right_encoded.clear();
        encode_bytes_desc(&mut left_encoded, left);
        encode_bytes_desc(&mut right_encoded, right);
        assert_eq!(left_encoded.cmp(&right_encoded), expected.reverse());
    }
}
