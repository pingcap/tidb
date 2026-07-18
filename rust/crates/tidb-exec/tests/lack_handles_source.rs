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

//! Source-backed tests for ordered missing-handle detection.

use std::collections::BTreeSet;

use tidb_exec::lack_handles::get_lack_handles;

#[test]
fn lack_handles_consumes_matches_and_preserves_expected_order() {
    // Source: pkg/executor/distsql.go:2259-2280.
    // Direct Go coverage: pkg/executor/distsql_test.go:140
    // (TestGetLackHandles).
    let expected: Vec<i64> = (1..=10).collect();
    let mut obtained: BTreeSet<i64> = expected.iter().copied().collect();
    assert!(get_lack_handles(&expected, &mut obtained).is_empty());
    assert!(obtained.is_empty());

    let mut obtained = BTreeSet::from([1, 5, 10]);
    assert_eq!(
        get_lack_handles(&expected, &mut obtained),
        vec![2, 3, 4, 6, 7, 8, 9]
    );
    assert_eq!(obtained, BTreeSet::from([10]));
}

#[test]
fn lack_handles_preserves_duplicate_expected_keys() {
    let expected = [1, 1, 2];
    let mut obtained = BTreeSet::from([1]);
    assert_eq!(get_lack_handles(&expected, &mut obtained), vec![1, 2]);
    assert!(obtained.is_empty());
}
