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

use tidb_stats::{
    JsonBucket, JsonCmSketch, JsonCmSketchRow, JsonCmSketchTopN, JsonColumn, JsonFmSketch,
    JsonHistogram, JsonPredicateColumn, JsonTable, TIDB_GLOBAL_STATS,
};

#[test]
fn source_json_table_sort_orders_predicate_columns_by_id() {
    let mut table = JsonTable {
        predicate_columns: Some(vec![
            JsonPredicateColumn::new(9),
            JsonPredicateColumn::new(2),
            JsonPredicateColumn::new(-1),
            JsonPredicateColumn::new(2),
        ]),
        ..JsonTable::default()
    };
    table.sort();
    assert_eq!(
        table
            .predicate_columns
            .as_ref()
            .expect("predicate columns")
            .iter()
            .map(|column| column.id)
            .collect::<Vec<_>>(),
        vec![-1, 2, 2, 9]
    );
}

#[test]
fn source_json_column_memory_is_the_sum_of_proto_sizes() {
    let column = JsonColumn {
        histogram: Some(JsonHistogram {
            ndv: 3,
            buckets: Some(vec![Some(JsonBucket {
                count: 5,
                lower_bound: Some("YQ==".to_owned()),
                upper_bound: Some("YmM=".to_owned()),
                repeats: 1,
                ndv: Some(2),
            })]),
        }),
        cm_sketch: Some(JsonCmSketch {
            rows: Some(vec![Some(JsonCmSketchRow {
                counters: Some(vec![1, 128]),
            })]),
            top_n: Some(vec![Some(JsonCmSketchTopN {
                data: Some("eg==".to_owned()),
                count: 9,
            })]),
            default_value: 4,
        }),
        fm_sketch: Some(JsonFmSketch {
            mask: 1,
            hashset: Some(vec![1, 128]),
        }),
        ..JsonColumn::default()
    };

    assert_eq!(column.total_memory_usage(), 40);
}

#[test]
fn source_json_table_sort_empty_is_a_noop_and_global_marker_is_stable() {
    let mut table = JsonTable::default();
    table.sort();
    assert!(table.predicate_columns.is_none());
    assert_eq!(TIDB_GLOBAL_STATS, "global");
}

#[test]
fn source_zero_json_table_preserves_go_nil_and_zero_fields() {
    assert_eq!(
        serde_json::to_string(&JsonTable::default()).unwrap(),
        r#"{"columns":null,"indices":null,"partitions":null,"database_name":"","table_name":"","predicate_columns":null,"count":0,"modify_count":0,"version":0,"is_historical_stats":false}"#
    );
}
