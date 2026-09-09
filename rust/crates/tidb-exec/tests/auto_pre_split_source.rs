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

//! Focused source regressions for Go `pkg/ddl/index_auto_presplit_test.go`.

use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, UNSPECIFIED_LENGTH};
use tidb_model::column::ColumnInfo;
use tidb_model::go_runtime::GoShared;
use tidb_model::index::{IndexColumn, IndexInfo};
use tidb_model::table_info::TableInfo;
use tidb_stats::{Bucket, Column, Histogram, StatsLoadedStatus, TopN, VERSION_1, VERSION_2};

use tidb_exec::auto_pre_split::{
    plan_auto_pre_split_index_keys, AutoPreSplitConfig, AutoPreSplitPlan,
};

fn table_and_index() -> (TableInfo, IndexInfo) {
    let mut column = ColumnInfo::default();
    column.id = 1;
    column.offset = 0;
    column.name = CiString::new("b");
    column.field_type = FieldType::new(FieldTypeCode::LongLong);
    let mut table = TableInfo::default();
    table.id = 42;
    table.columns = tidb_model::GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(column))]);

    let index_column = IndexColumn {
        name: CiString::new("b"),
        offset: 0,
        length: UNSPECIFIED_LENGTH,
        ..IndexColumn::default()
    };
    let mut index = IndexInfo::default();
    index.id = 7;
    index.name = CiString::new("idx_b");
    index.columns = tidb_model::GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(index_column))]);
    table.indices = tidb_model::GoSharedPointerSlice::from_handles(vec![Some(GoShared::new(index.clone()))]);
    (table, index)
}

fn stats(top_n: Option<TopN>, stats_version: i64, null_count: i64) -> Column {
    let mut histogram = Histogram::new(1, 2, null_count, 0, 2, 0);
    histogram.buckets.push(Bucket {
        count: 5,
        repeat: 5,
        ndv: 1,
        lower_bound: Datum::new_int(10),
        upper_bound: Datum::new_int(10),
    });
    histogram.buckets.push(Bucket {
        count: 25,
        repeat: 20,
        ndv: 1,
        lower_bound: Datum::new_int(20),
        upper_bound: Datum::new_int(20),
    });
    Column {
        histogram,
        top_n,
        stats_version,
        stats_loaded_status: StatsLoadedStatus::full_load(),
        ..Column::default()
    }
}

#[test]
fn auto_pre_split_merges_topn_and_histogram_before_sampling_keys() {
    let (table, index) = table_and_index();
    let mut top_n = TopN::new(1);
    top_n.append(&tidb_codec::encode_key(&[Datum::new_int(10)]).unwrap(), 15);
    let column = stats(Some(top_n), VERSION_2, 0);
    let plan = plan_auto_pre_split_index_keys(
        &table,
        &index,
        100,
        Some(100),
        Some(&column),
        AutoPreSplitConfig {
            min_table_rows: 10,
            boundary_ratio_step: 0.5,
            ..AutoPreSplitConfig::default()
        },
    )
    .unwrap();
    let AutoPreSplitPlan::Planned(keys) = plan else {
        panic!("expected planned AUTO split");
    };
    let end = tidb_codec::table_key::encode_index_seek_key(table.id, index.id + 1, &[]);
    assert!(keys.contains(&end));
    assert_eq!(keys.len(), 2, "one internal boundary plus the index end");
}

#[test]
fn auto_pre_split_skips_unreliable_or_unsafe_statistics() {
    let (table, index) = table_and_index();
    let v1 = stats(None, VERSION_1, 0);
    assert_eq!(
        plan_auto_pre_split_index_keys(
            &table,
            &index,
            100,
            Some(100),
            Some(&v1),
            AutoPreSplitConfig {
                min_table_rows: 10,
                ..AutoPreSplitConfig::default()
            },
        )
        .unwrap(),
        AutoPreSplitPlan::Skipped("leading column stats version 1 is not Analyze V2".to_owned())
    );
    let negative_nulls = stats(None, VERSION_2, -1);
    let error = plan_auto_pre_split_index_keys(
        &table,
        &index,
        100,
        Some(100),
        Some(&negative_nulls),
        AutoPreSplitConfig {
            min_table_rows: 10,
            ..AutoPreSplitConfig::default()
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("negative null count"));
}
