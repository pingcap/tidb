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

//! Source-backed tests for static-partitioned auto-analyze SQL metadata.

use tidb_stats::{
    gen_sql_for_analyze_static_partition, gen_sql_for_analyze_static_partition_index,
    has_newly_added_static_partition_index, static_partition_analyze_type,
    static_partition_table_id, ANALYZE_STATIC_PARTITION, ANALYZE_STATIC_PARTITION_INDEX,
};

#[test]
fn source_static_partition_sql_uses_ordered_placeholders() {
    let (sql, params) = gen_sql_for_analyze_static_partition("test_schema", "test_table", "p0");
    assert_eq!(sql, "analyze table %n.%n partition %n");
    assert_eq!(
        params,
        vec![
            "test_schema".to_owned(),
            "test_table".to_owned(),
            "p0".to_owned(),
        ]
    );
    assert_eq!(static_partition_analyze_type(0), ANALYZE_STATIC_PARTITION);
    assert!(!has_newly_added_static_partition_index(0));
}

#[test]
fn source_static_partition_index_sql_appends_index_and_selects_kind() {
    let (sql, params) =
        gen_sql_for_analyze_static_partition_index("test_schema", "test_table", "p0", "idx");
    assert_eq!(sql, "analyze table %n.%n partition %n index %n");
    assert_eq!(
        params,
        vec![
            "test_schema".to_owned(),
            "test_table".to_owned(),
            "p0".to_owned(),
            "idx".to_owned(),
        ]
    );
    assert_eq!(
        static_partition_analyze_type(1),
        ANALYZE_STATIC_PARTITION_INDEX
    );
    assert!(has_newly_added_static_partition_index(usize::MAX));
}

#[test]
fn source_static_partition_queue_key_is_the_physical_partition_id() {
    assert_eq!(static_partition_table_id(42), 42);
    assert_eq!(static_partition_table_id(-7), -7);
}
