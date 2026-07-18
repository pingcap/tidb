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

//! Source-backed tests for DDL physical statistics ID selection.

use tidb_stats::physical_ids_for_stats_ddl;

#[test]
fn source_physical_ids_keep_non_partitioned_tables_distinct() {
    assert_eq!(physical_ids_for_stats_ddl(100, None, false), vec![100]);
    assert_eq!(physical_ids_for_stats_ddl(100, None, true), vec![100]);
}

#[test]
fn source_physical_ids_preserve_partition_order_and_dynamic_global_id() {
    let partitions = [21, 7, 42];
    assert_eq!(
        physical_ids_for_stats_ddl(100, Some(&partitions), false),
        vec![21, 7, 42]
    );
    assert_eq!(
        physical_ids_for_stats_ddl(100, Some(&partitions), true),
        vec![21, 7, 42, 100]
    );
}

#[test]
fn source_physical_ids_keep_empty_partition_definitions() {
    let empty: [i64; 0] = [];
    assert!(physical_ids_for_stats_ddl(100, Some(&empty), false).is_empty());
    assert_eq!(
        physical_ids_for_stats_ddl(100, Some(&empty), true),
        vec![100]
    );
}
