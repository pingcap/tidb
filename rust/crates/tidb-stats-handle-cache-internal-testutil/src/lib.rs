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

//! Go `pkg/statistics/handle/cache/internal/testutil`.

use std::mem::size_of;
use std::sync::Arc;

use tidb_stats::{
    Bucket, CmsSketch, Column, ColumnInfo, HistColl, Histogram, Index, IndexInfo,
    StatsLoadedStatus, Table, TopN,
};

fn histogram() -> (Histogram, i64) {
    let histogram = Histogram::new(0, 10, 0, 0, 1, 0);
    let memory_usage = size_of::<Histogram>().wrapping_add(
        histogram
            .buckets
            .capacity()
            .wrapping_mul(size_of::<Bucket>()),
    );
    (histogram, memory_usage as i64)
}

fn top_n() -> TopN {
    let mut top_n = TopN::new(1);
    top_n.append(&[], 1);
    top_n
}

fn column(id: i64, with_cms: bool, with_top_n: bool, with_hist: bool) -> Column {
    let (histogram, histogram_memory_usage) = if with_hist {
        histogram()
    } else {
        (Histogram::default(), 0)
    };
    Column {
        cmsketch: with_cms.then(|| CmsSketch::new(1, 1)),
        top_n: with_top_n.then(top_n),
        info: Some(ColumnInfo {
            id,
            ..ColumnInfo::default()
        }),
        histogram,
        stats_loaded_status: StatsLoadedStatus::full_load(),
        histogram_memory_usage,
        ..Column::default()
    }
}

fn index(id: i64, with_cms: bool, with_top_n: bool, with_hist: bool) -> Index {
    let (histogram, histogram_memory_usage) = if with_hist {
        histogram()
    } else {
        (Histogram::default(), 0)
    };
    Index {
        cmsketch: with_cms.then(|| CmsSketch::new(1, 1)),
        top_n: with_top_n.then(top_n),
        info: Some(IndexInfo {
            id,
            ..IndexInfo::default()
        }),
        histogram,
        stats_loaded_status: StatsLoadedStatus::full_load(),
        histogram_memory_usage,
        ..Index::default()
    }
}

/// Go `NewMockStatisticsTable`.
#[must_use]
pub fn new_mock_statistics_table(
    columns: isize,
    indices: isize,
    with_cms: bool,
    with_top_n: bool,
    with_hist: bool,
) -> Arc<Table> {
    let hist_coll = HistColl::new(0, 0, 0, 0, 0);
    for id in 1..=columns {
        let id = id as i64;
        hist_coll.set_column(id, column(id, with_cms, with_top_n, with_hist));
    }
    for id in 1..=indices {
        let id = id as i64;
        hist_coll.set_index(id, index(id, with_cms, with_top_n, with_hist));
    }
    Arc::new(Table {
        existence_map: None,
        hist_coll,
        version: 0,
        last_analyze_version: 0,
        last_stats_hist_version: 0,
        table_info_update_ts: 0,
        is_pk_handle: false,
    })
}

/// Go `MockTableAppendColumn`.
pub fn mock_table_append_column(table: &Table) {
    let id = table.hist_coll.column_count() as i64 + 1;
    table.hist_coll.set_column(
        id,
        Column {
            cmsketch: Some(CmsSketch::new(1, 1)),
            info: Some(ColumnInfo {
                id,
                ..ColumnInfo::default()
            }),
            ..Column::default()
        },
    );
}

/// Go `MockTableAppendIndex`.
pub fn mock_table_append_index(table: &Table) {
    let id = table.hist_coll.index_count() as i64 + 1;
    table.hist_coll.set_index(
        id,
        Index {
            cmsketch: Some(CmsSketch::new(1, 1)),
            info: Some(IndexInfo {
                id,
                ..IndexInfo::default()
            }),
            ..Index::default()
        },
    );
}
