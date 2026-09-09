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

//! Port of `pkg/planner/util/path_test.go::TestCompareCol2Len`
//! (`pkg/planner.part22` item 1275 on `origin/master`).
//!
//! `CompareCol2Len` (pkg/planner/util/path.go:521-558) orders two Col2Len maps
//! by SIZE first; on a tie it stays comparable only when every shared column
//! matches length and neither map holds a column the other lacks. The
//! dominance probe (`Col2Len.dominate`, path.go:507-519, fed by
//! `compareLength` :491-504) treats `-1` (`types.UnspecifiedLength`) as the
//! LONGEST length possible, so `-1` on the dominating side never loses.

use tidb_planner::column_length::{compare_col2_len, Col2Len, UNSPECIFIED_LENGTH};

fn col2len(pairs: &[(i64, i64)]) -> Col2Len {
    Col2Len::from_pairs(pairs.iter().copied())
}

/// GO PORT of `pkg/planner/util/path_test.go:30 TestCompareCol2Len`.
///
/// The six-row table at path_test.go:35-79 in source order; each row pins one
/// `(res, comparable)` pair equal to Go's two return values.
#[test]
fn compare_col2_len_orders_by_size_then_dominance() {
    // Row 1 (:36-41): {1:-1, 2:-1, 3:-1} vs {1:-1, 2:10}. Bigger map wins and
    // dominates: every probed entry of the smaller map is no longer than its
    // counterpart (-1 outranks the concrete 10).
    let result = compare_col2_len(
        &col2len(&[(1, UNSPECIFIED_LENGTH), (2, UNSPECIFIED_LENGTH), (3, UNSPECIFIED_LENGTH)]),
        &col2len(&[(1, UNSPECIFIED_LENGTH), (2, 10)]),
    );
    assert_eq!(result, (1, true));

    // Row 2 (:42-47): {1:5} vs {1:10, 2:-1}. Fewer columns ranks -1, but the
    // bigger map dominates when probed with ITS OWN complement's entries:
    // Go calls c2.dominate(c1), iterating c1 = {1:5}; 5 < 10 keeps it inside.
    // (`dominate` iterates the ARGUMENT — path.go:510-517.)
    let result = compare_col2_len(
        &col2len(&[(1, 5)]),
        &col2len(&[(1, 10), (2, UNSPECIFIED_LENGTH)]),
    );
    assert_eq!(result, (-1, true));

    // Row 3 (:48-53): {1:-1, 2:-1} vs {1:-1, 2:5, 3:-1}. Bigger side cannot
    // dominate: probing argument entry 2:-1 against value 5 makes -1 lose.
    let result = compare_col2_len(
        &col2len(&[(1, UNSPECIFIED_LENGTH), (2, UNSPECIFIED_LENGTH)]),
        &col2len(&[(1, UNSPECIFIED_LENGTH), (2, 5), (3, UNSPECIFIED_LENGTH)]),
    );
    assert_eq!(result, (-1, false));

    // Row 4 (:54-59): {1:-1, 2:10} vs {1:-1, 2:5, 3:-1}. Same shape as row 3;
    // now the concrete 10 > 5 breaks dominance instead.
    let result = compare_col2_len(
        &col2len(&[(1, UNSPECIFIED_LENGTH), (2, 10)]),
        &col2len(&[(1, UNSPECIFIED_LENGTH), (2, 5), (3, UNSPECIFIED_LENGTH)]),
    );
    assert_eq!(result, (-1, false));

    // Row 5 (:60-65): identical maps tie and stay comparable.
    let result = compare_col2_len(
        &col2len(&[(1, UNSPECIFIED_LENGTH), (2, 10)]),
        &col2len(&[(1, UNSPECIFIED_LENGTH), (2, 10)]),
    );
    assert_eq!(result, (0, true));

    // Row 6 (:66-71): {1:-1, 2:-1} vs {1:-1, 2:10}: smaller map loses without
    // dominance — -1 held by the SMALLER side can never beat concrete 10.
    let result = compare_col2_len(
        &col2len(&[(1, UNSPECIFIED_LENGTH), (2, UNSPECIFIED_LENGTH)]),
        &col2len(&[(1, UNSPECIFIED_LENGTH), (2, 10)]),
    );
    assert_eq!(result, (-1, false));
}
