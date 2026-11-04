// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go's statistics-save statement batching (`saveTopNToStorage`,
//! `saveBucketsToStorage`) and the statement-local `TableSizeStats` of the
//! information-schema size readers.

use tidb_datatype::Datum;
use tidb_model::column::ColumnInfo;
use tidb_model::table_info::TableInfo;
use tidb_model::SchemaState;
use tidb_stats::cmsketch::TopN;
use tidb_stats::histogram::{Bucket, Histogram};
use tidb_stats_handle_storage::cluster_stats_load::{
    ClusterStatsItem, ColumnLength, TableRowCount, TableSizeStats,
};
use tidb_stats_handle_storage::cluster_stats_write::{save_buckets_sql, save_topn_sql};

/// One histogram at the default `ANALYZE` shape: 256 buckets and 100 TopN
/// entries.
fn full_histogram(id: i64, is_index: bool) -> ClusterStatsItem {
    let buckets = (0..256_i64)
        .map(|index| Bucket {
            count: (index + 1) * 40,
            repeat: 3,
            ndv: 30,
            lower_bound: Datum::Int(index * 100),
            upper_bound: Datum::Int(index * 100 + 99),
        })
        .collect();
    let mut topn = TopN::new(100);
    for value in 0..100_u8 {
        topn.append(&[3, 0, 0, 0, 0, 0, 0, 0, value], 7);
    }
    ClusterStatsItem {
        id,
        is_index,
        stats_ver: 2,
        flag: 0,
        load_status: tidb_stats::StatsLoadedStatus::full_load(),
        histogram: Histogram {
            id,
            ndv: 8000,
            null_count: 0,
            last_update_version: 440_000_000_000_000_000,
            tot_col_size: 40_000,
            correlation: 0.9,
            buckets,
            ..Default::default()
        },
        topn: Some(topn),
        cms: None,
        fm_sketch: None,
    }
}

/// Go `saveTopNToStorage` / `saveBucketsToStorage` batch their INSERTs at
/// ten rows or the escaped 1 MiB boundary.
#[test]
fn stats_item_inserts_use_go_statement_batches() {
    let item = full_histogram(1, false);
    assert_eq!(save_topn_sql(4242, &item).len(), 10);
    assert_eq!(save_buckets_sql(4242, &item).expect("buckets build").len(), 26);

    let mut oversized = full_histogram(1, false);
    let mut oversized_topn = TopN::new(2);
    oversized_topn.append(&vec![0; 600_000], 1);
    oversized_topn.append(&vec![0; 600_000], 2);
    oversized.topn = Some(oversized_topn);
    oversized.histogram.buckets = vec![
        Bucket {
            count: 1,
            repeat: 1,
            ndv: 1,
            lower_bound: Datum::Bytes(vec![0; 300_000]),
            upper_bound: Datum::Bytes(vec![0; 300_000]),
        },
        Bucket {
            count: 2,
            repeat: 1,
            ndv: 1,
            lower_bound: Datum::Bytes(vec![0; 300_000]),
            upper_bound: Datum::Bytes(vec![0; 300_000]),
        },
    ];
    assert_eq!(save_topn_sql(4242, &oversized).len(), 2);
    assert_eq!(save_buckets_sql(4242, &oversized).expect("buckets build").len(), 2);
}

/// Go's `TableSizeStats` is statement-local and clamps a negative persisted
/// variable-column size to zero. A later statement starts with empty maps
/// rather than observing a process-wide row-cache value.
#[test]
fn table_size_stats_are_statement_local_and_clamp_negative_sizes() {
    let table = TableInfo {
        id: 4242,
        columns: vec![
            ColumnInfo {
                id: 1,
                offset: 0,
                field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                state: SchemaState::PUBLIC,
                ..ColumnInfo::default()
            },
            ColumnInfo {
                id: 2,
                offset: 1,
                field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Varchar),
                state: SchemaState::PUBLIC,
                ..ColumnInfo::default()
            },
        ]
        .into(),
        ..TableInfo::default()
    };
    let stats = TableSizeStats::from_rows(
        vec![TableRowCount {
            table_id: 4242,
            count: 7,
        }],
        vec![ColumnLength {
            table_id: 4242,
            histogram_id: 2,
            total_size: -9,
        }],
    );
    assert_eq!(stats.estimate_data_length(&table), (7, 8, 56, 0));

    let next_statement = TableSizeStats::default();
    assert_eq!(next_statement.get_table_rows(4242), 0);
    assert_eq!(next_statement.estimate_data_length(&table), (0, 0, 0, 0));
}
