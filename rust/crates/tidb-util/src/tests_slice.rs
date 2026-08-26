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

//! Ports of `pkg/util/slice` unit tests from Go (`slice_test.go`).

use crate::slice::all_of;

/// Go: pkg/util/slice/slice_test.go TestSlice
///
/// Pins `AllOf` over the same table-driven cases as the Go test: an empty
/// slice is vacuously true, a slice with any odd element is false.
#[test]
fn slice_all_of_matches_go_table_cases() {
    let tests: [(&[i32], bool); 4] = [
        (&[], true),
        (&[1, 2, 3], false),
        (&[1, 3], false),
        (&[2, 2, 4], true),
    ];

    for (values, expected) in tests {
        assert_eq!(all_of(values, |value| value % 2 == 0), expected);
    }
}
