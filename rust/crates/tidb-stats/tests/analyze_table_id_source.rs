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

//! Source-backed tests for analyze table/partition identity.

use tidb_stats::{AnalyzeTableId, NON_PARTITION_TABLE_ID};

#[test]
fn source_non_partition_identity_uses_table_id() {
    let table = AnalyzeTableId::new(42, NON_PARTITION_TABLE_ID);
    assert_eq!(table.statistics_id(), 42);
    assert!(!table.is_partition_table());
    assert_eq!(table.display_string(), "-1 => 42");
}

#[test]
fn source_partition_identity_uses_partition_id() {
    let partition = AnalyzeTableId::new(42, 1001);
    assert_eq!(partition.statistics_id(), 1001);
    assert!(partition.is_partition_table());
    assert_eq!(partition.display_string(), "1001 => 42");
}

#[test]
fn source_equals_compares_both_ids_and_nil_shape() {
    let first = AnalyzeTableId::new(42, 1001);
    assert!(first.equals(first));
    assert!(first.equals(AnalyzeTableId::new(42, 1001)));
    assert!(!first.equals(AnalyzeTableId::new(42, 1002)));
    assert!(!first.equals(AnalyzeTableId::new(43, 1001)));

    assert!(AnalyzeTableId::equals_optional(None, None));
    assert!(!AnalyzeTableId::equals_optional(Some(&first), None));
    assert!(!AnalyzeTableId::equals_optional(None, Some(&first)));
    assert!(AnalyzeTableId::equals_optional(Some(&first), Some(&first)));
}
