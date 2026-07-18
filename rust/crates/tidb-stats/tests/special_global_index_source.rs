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

//! Source-backed tests for special global-index classification.

use tidb_stats::{is_special_global_index, IndexColumnInfo};

#[test]
fn source_regular_and_local_indexes_are_not_special() {
    assert!(!is_special_global_index(
        true,
        &[IndexColumnInfo::regular()]
    ));
    assert!(!is_special_global_index(
        false,
        &[IndexColumnInfo::regular()]
    ));
    assert!(!is_special_global_index(
        false,
        &[IndexColumnInfo::virtual_generated()]
    ));
    assert!(!is_special_global_index(
        false,
        &[IndexColumnInfo::prefix()]
    ));
}

#[test]
fn source_virtual_or_prefix_global_indexes_are_special() {
    assert!(is_special_global_index(
        true,
        &[IndexColumnInfo::virtual_generated()]
    ));
    assert!(is_special_global_index(true, &[IndexColumnInfo::prefix()]));
}

#[test]
fn source_any_column_fact_is_enough_and_empty_is_not_special() {
    assert!(is_special_global_index(
        true,
        &[IndexColumnInfo::regular(), IndexColumnInfo::prefix()]
    ));
    assert!(is_special_global_index(
        true,
        &[
            IndexColumnInfo::regular(),
            IndexColumnInfo::virtual_generated()
        ]
    ));
    assert!(!is_special_global_index(true, &[]));
}
