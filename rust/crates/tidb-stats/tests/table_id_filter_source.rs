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

//! Source-backed tests for table-ID filter formatting.

use tidb_stats::build_in_table_ids_string;

#[test]
fn source_table_id_filter_preserves_order_and_decimal_formatting() {
    assert_eq!(build_in_table_ids_string(&[5, 2, 7]), "table_id in (5,2,7)");
    assert_eq!(
        build_in_table_ids_string(&[-3, 0, 12]),
        "table_id in (-3,0,12)"
    );
}

#[test]
fn source_table_id_filter_keeps_empty_and_signed_boundaries() {
    assert_eq!(build_in_table_ids_string(&[]), "table_id in ()");
    assert_eq!(
        build_in_table_ids_string(&[i64::MIN, i64::MAX]),
        "table_id in (-9223372036854775808,9223372036854775807)"
    );
}
