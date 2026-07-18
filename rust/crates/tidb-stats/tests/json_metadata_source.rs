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

//! Source-backed tests for statistics JSON ordering metadata.

use tidb_stats::{JsonPredicateColumn, JsonTable, TIDB_GLOBAL_STATS};

#[test]
fn source_json_table_sort_orders_predicate_columns_by_id() {
    let mut table = JsonTable {
        predicate_columns: vec![
            JsonPredicateColumn::new(9),
            JsonPredicateColumn::new(2),
            JsonPredicateColumn::new(-1),
            JsonPredicateColumn::new(2),
        ],
    };
    table.sort();
    assert_eq!(
        table
            .predicate_columns
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>(),
        vec![-1, 2, 2, 9]
    );
}

#[test]
fn source_json_table_sort_empty_is_a_noop_and_global_marker_is_stable() {
    let mut table = JsonTable::default();
    table.sort();
    assert!(table.predicate_columns.is_empty());
    assert_eq!(TIDB_GLOBAL_STATS, "global");
}
