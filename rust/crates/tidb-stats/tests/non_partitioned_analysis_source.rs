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

//! Source-backed tests for non-partitioned auto-analyze SQL metadata.

use tidb_stats::{
    analyze_type, gen_sql_for_analyze_index, gen_sql_for_analyze_table, has_newly_added_index,
    ANALYZE_INDEX, ANALYZE_TABLE,
};

#[test]
fn source_table_sql_uses_ordered_identifier_placeholders() {
    let (sql, params) = gen_sql_for_analyze_table("test_schema", "test_table");
    assert_eq!(sql, "analyze table %n.%n");
    assert_eq!(
        params,
        vec!["test_schema".to_owned(), "test_table".to_owned()]
    );
    assert_eq!(analyze_type(0), ANALYZE_TABLE);
    assert!(!has_newly_added_index(0));
}

#[test]
fn source_index_sql_appends_index_placeholder_and_selects_kind() {
    let (sql, params) = gen_sql_for_analyze_index("test_schema", "test_table", "test_index");
    assert_eq!(sql, "analyze table %n.%n index %n");
    assert_eq!(
        params,
        vec![
            "test_schema".to_owned(),
            "test_table".to_owned(),
            "test_index".to_owned(),
        ]
    );
    assert_eq!(analyze_type(1), ANALYZE_INDEX);
    assert!(has_newly_added_index(1));
    assert!(has_newly_added_index(usize::MAX));
}
