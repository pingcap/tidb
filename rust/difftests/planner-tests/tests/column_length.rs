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

//! Dependency-closed vectors for `pkg/planner/util/path.go`.
//!
//! The direct Go anchor is `TestCompareCol2Len` at
//! `pkg/planner/util/path_test.go:30`.

use tidb_planner::column_length::{compare_col2_len, Col2Len, UNSPECIFIED_LENGTH};

#[test]
fn source_compare_col2_len_preserves_dominance_and_incomparability() {
    let cases = [
        (
            Col2Len::from_pairs([
                (1, UNSPECIFIED_LENGTH),
                (2, UNSPECIFIED_LENGTH),
                (3, UNSPECIFIED_LENGTH),
            ]),
            Col2Len::from_pairs([(1, UNSPECIFIED_LENGTH), (2, 10)]),
            (1, true),
        ),
        (
            Col2Len::from_pairs([(1, 5)]),
            Col2Len::from_pairs([(1, 10), (2, UNSPECIFIED_LENGTH)]),
            (-1, true),
        ),
        (
            Col2Len::from_pairs([(1, UNSPECIFIED_LENGTH), (2, UNSPECIFIED_LENGTH)]),
            Col2Len::from_pairs([(1, UNSPECIFIED_LENGTH), (2, 5), (3, UNSPECIFIED_LENGTH)]),
            (-1, false),
        ),
        (
            Col2Len::from_pairs([(1, UNSPECIFIED_LENGTH), (2, 10)]),
            Col2Len::from_pairs([(1, UNSPECIFIED_LENGTH), (2, 5), (3, UNSPECIFIED_LENGTH)]),
            (-1, false),
        ),
        (
            Col2Len::from_pairs([(1, UNSPECIFIED_LENGTH), (2, 10)]),
            Col2Len::from_pairs([(1, UNSPECIFIED_LENGTH), (2, 10)]),
            (0, true),
        ),
        (
            Col2Len::from_pairs([(1, UNSPECIFIED_LENGTH), (2, UNSPECIFIED_LENGTH)]),
            Col2Len::from_pairs([(1, UNSPECIFIED_LENGTH), (2, 10)]),
            (-1, false),
        ),
    ];
    for (left, right, expected) in cases {
        assert_eq!(compare_col2_len(&left, &right), expected);
    }
}

#[test]
fn source_unspecified_length_is_longer_when_columns_are_shared() {
    let full = Col2Len::from_pairs([(7, UNSPECIFIED_LENGTH)]);
    let prefix = Col2Len::from_pairs([(7, 4)]);
    // Equal column sets with different lengths are intentionally incomparable
    // in the Go implementation; the caller applies the remaining criteria.
    assert_eq!(compare_col2_len(&full, &prefix), (-1, false));
    assert_eq!(compare_col2_len(&prefix, &full), (1, false));
}
