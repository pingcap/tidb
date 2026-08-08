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

use std::sync::{Arc, RwLock};

use tidb_datatype::Datum;
use tidb_stats::{
    Bucket, ColAndIdxExistenceMap, Column, ColumnInfo, CopyIntent, HistColl, Histogram, Index,
    IndexInfo, StatsLoadedStatus, Table, TopN, ALL_EVICTED,
};

fn column(id: i64, count: i64, status: StatsLoadedStatus) -> Column {
    Column {
        info: Some(ColumnInfo {
            id,
            name: format!("c{id}"),
            primary_key: false,
        }),
        histogram: Histogram {
            id,
            buckets: vec![Bucket {
                count,
                repeat: 1,
                ndv: 1,
                lower_bound: Datum::Int(id),
                upper_bound: Datum::Int(id),
            }],
            ..Histogram::default()
        },
        stats_loaded_status: status,
        stats_version: 1,
        ..Column::default()
    }
}

fn index(id: i64, count: i64, mv_index: bool, status: StatsLoadedStatus) -> Index {
    Index {
        info: Some(IndexInfo {
            id,
            name: format!("i{id}"),
            columns: vec![format!("c{id}")],
            mv_index,
        }),
        histogram: Histogram {
            id,
            buckets: vec![Bucket {
                count,
                repeat: 1,
                ndv: 1,
                lower_bound: Datum::Bytes(vec![id as u8]),
                upper_bound: Datum::Bytes(vec![id as u8]),
            }],
            ..Histogram::default()
        },
        stats_loaded_status: status,
        stats_version: 1,
        ..Index::default()
    }
}

fn table() -> Table {
    let coll = HistColl::new(1, 100, 20, 2, 2);
    coll.set_column(2, column(2, 80, StatsLoadedStatus::full_load()));
    coll.set_index(3, index(3, 80, false, StatsLoadedStatus::full_load()));
    let mut existence = ColAndIdxExistenceMap::new(2, 2);
    existence.insert_column(2, true);
    existence.insert_index(3, true);
    Table {
        existence_map: Some(Arc::new(RwLock::new(existence))),
        hist_coll: coll,
        version: 4,
        last_analyze_version: 5,
        last_stats_hist_version: 6,
        table_info_update_ts: 7,
        is_pk_handle: true,
    }
}

#[test]
fn source_histcoll_stable_order_and_early_stop_match() {
    let coll = HistColl::new(1, 10, 0, 3, 3);
    for id in [9, 2, 5] {
        coll.set_column(id, column(id, id, StatsLoadedStatus::full_load()));
        coll.set_index(id, index(id, id, false, StatsLoadedStatus::full_load()));
    }
    let column_ids: Vec<i64> = coll
        .stable_columns()
        .iter()
        .map(|column| column.read().unwrap().histogram.id)
        .collect();
    let index_ids: Vec<i64> = coll
        .stable_indices()
        .iter()
        .map(|index| index.read().unwrap().histogram.id)
        .collect();
    assert_eq!(column_ids, [2, 5, 9]);
    assert_eq!(index_ids, [2, 5, 9]);

    let mut visits = 0;
    coll.for_each_column(|_, _| {
        visits += 1;
        true
    });
    assert_eq!(visits, 1);
}

#[test]
fn test_copy_as_preserves_all_five_go_intents() {
    for (intent, columns_shared, indices_shared, existence_shared) in [
        (CopyIntent::MetaOnly, true, true, true),
        (CopyIntent::ColumnMapWritable, false, true, false),
        (CopyIntent::IndexMapWritable, true, false, false),
        (CopyIntent::BothMapsWritable, false, false, false),
        (CopyIntent::AllDataWritable, false, false, false),
    ] {
        let original = table();
        let copied = original.copy_as(intent);
        copied
            .hist_coll
            .set_column(99, column(99, 1, StatsLoadedStatus::default()));
        copied
            .hist_coll
            .set_index(99, index(99, 1, false, StatsLoadedStatus::default()));
        assert_eq!(original.hist_coll.get_column(99).is_some(), columns_shared);
        assert_eq!(original.hist_coll.get_index(99).is_some(), indices_shared);
        assert_eq!(
            Arc::ptr_eq(
                original.existence_map.as_ref().unwrap(),
                copied.existence_map.as_ref().unwrap(),
            ),
            existence_shared
        );
        assert!(!copied.hist_coll.cannot_trigger_load);
        assert!(copied.hist_coll.idx_to_col_unique_ids.is_empty());
        assert!(!copied.is_pk_handle);
    }

    let original = table();
    let copied = original.copy_as(CopyIntent::AllDataWritable);
    copied
        .hist_coll
        .get_column(2)
        .unwrap()
        .write()
        .unwrap()
        .histogram
        .buckets[0]
        .count = 999;
    assert_eq!(
        original
            .hist_coll
            .get_column(2)
            .unwrap()
            .read()
            .unwrap()
            .histogram
            .buckets[0]
            .count,
        80
    );
}

#[test]
fn source_delete_updates_data_and_existence_maps() {
    let table = table();
    table.delete_column(2);
    table.delete_index(3);
    assert!(table.hist_coll.get_column(2).is_none());
    assert!(table.hist_coll.get_index(3).is_none());
    let map = table.existence_map.as_ref().unwrap().read().unwrap();
    assert!(!map.has(2, false));
    assert!(!map.has(3, true));
}

#[test]
fn source_table_memory_aggregates_components_and_tracking() {
    let table = table();
    {
        let column = table.hist_coll.get_column(2).unwrap();
        let mut column = column.write().unwrap();
        column.histogram_memory_usage = 11;
        let mut top_n = TopN::new(1);
        top_n.append(&[1], 1);
        column.top_n = Some(top_n);
    }
    {
        let index = table.hist_coll.get_index(3).unwrap();
        let mut index = index.write().unwrap();
        index.histogram_memory_usage = 13;
        let mut top_n = TopN::new(1);
        top_n.append(&[2], 1);
        index.top_n = Some(top_n);
    }

    let usage = table.memory_usage();
    assert_eq!(usage.table_id, 1);
    assert_eq!(usage.columns_mem_usage.len(), 1);
    assert_eq!(usage.indices_mem_usage.len(), 1);
    assert_eq!(
        usage.total_mem_usage,
        usage.columns_mem_usage[&2]
            .total_memory_usage()
            .wrapping_add(usage.indices_mem_usage[&3].total_memory_usage())
    );
    assert_eq!(
        usage.total_tracking_mem_usage(),
        usage.columns_mem_usage[&2]
            .tracking_mem_usage()
            .wrapping_add(usage.indices_mem_usage[&3].tracking_mem_usage())
    );
}

#[test]
fn source_table_memory_uses_go_int64_wrapping_boundaries() {
    let mut usage = tidb_stats::TableMemoryUsage::default();
    usage.columns_mem_usage.insert(
        1,
        tidb_stats::ColumnMemUsage {
            histogram_mem_usage: i64::MAX,
            ..tidb_stats::ColumnMemUsage::default()
        },
    );
    usage.indices_mem_usage.insert(
        2,
        tidb_stats::IndexMemUsage {
            histogram_mem_usage: 1,
            ..tidb_stats::IndexMemUsage::default()
        },
    );
    assert_eq!(usage.total_tracking_mem_usage(), i64::MIN);
}

#[test]
fn source_bootstrap_pre_scalar_and_drop_evicted_match() {
    let coll = HistColl::new(1, 10, 0, 1, 1);
    let mut col = column(1, 2, StatsLoadedStatus::full_load());
    col.histogram.buckets.push(Bucket {
        count: 3,
        repeat: 1,
        ndv: 1,
        lower_bound: Datum::Int(2),
        upper_bound: Datum::Int(2),
    });
    col.top_n = Some(TopN::default());
    coll.set_column(1, col);
    coll.set_index(1, index(1, 2, false, StatsLoadedStatus::all_evicted()));
    coll.set_all_indices_full_load_for_bootstrap();
    assert!(coll.get_index(1).unwrap().read().unwrap().is_full_load());
    coll.calculate_pre_scalar_counts();
    assert_eq!(
        coll.get_column(1)
            .unwrap()
            .read()
            .unwrap()
            .histogram
            .buckets[1]
            .count,
        5
    );
    coll.drop_evicted();
    let col = coll.get_column(1).unwrap();
    assert_eq!(col.read().unwrap().evicted_status(), ALL_EVICTED);
}

#[test]
fn source_load_needed_truth_tables_match() {
    let table = table();
    let (_, needed, analyzed) = table.column_load_needed(2, true);
    assert!(!needed && analyzed);
    let (missing, needed, analyzed) = table.column_load_needed(20, false);
    assert!(missing.is_none() && !needed && !analyzed);

    table
        .existence_map
        .as_ref()
        .unwrap()
        .write()
        .unwrap()
        .insert_column(20, true);
    let (_, needed, analyzed) = table.column_load_needed(20, false);
    assert!(needed && analyzed);

    table
        .hist_coll
        .get_column(2)
        .unwrap()
        .write()
        .unwrap()
        .stats_loaded_status = StatsLoadedStatus::all_evicted();
    assert!(table.column_load_needed(2, true).1);
    assert!(!table.column_load_needed(2, false).1);

    table
        .existence_map
        .as_ref()
        .unwrap()
        .write()
        .unwrap()
        .insert_index(30, true);
    assert!(table.index_load_needed(30).1);
    table
        .hist_coll
        .get_index(3)
        .unwrap()
        .write()
        .unwrap()
        .stats_loaded_status = StatsLoadedStatus::all_evicted();
    assert!(table.index_load_needed(3).1);
}

#[test]
fn source_health_outdated_analysis_and_mv_scaling_match() {
    let table = table();
    assert_eq!(table.hist_coll.analyze_row_count(), 80.0);
    assert_eq!(table.stats_healthy(), (75, true));
    assert!(!table.is_outdated(0.25));
    assert!(table.is_outdated(0.249));
    assert!(table.is_initialized());
    assert!(table.is_analyzed());
    assert!(table.is_eligible_for_analysis(100));
    assert!(!table.is_eligible_for_analysis(101));

    let mut mv = index(8, 160, true, StatsLoadedStatus::full_load());
    mv.top_n = None;
    assert_eq!(
        table.hist_coll.scaled_realtime_and_modify_count(Some(&mv)),
        (200, 40)
    );
    assert_eq!(
        table.hist_coll.scaled_realtime_and_modify_count(None),
        (100, 20)
    );

    assert_eq!(
        table
            .index_starting_with_column("c3")
            .unwrap()
            .read()
            .unwrap()
            .item_id(),
        3
    );
    assert_eq!(
        table
            .column_by_name("c2")
            .unwrap()
            .read()
            .unwrap()
            .item_id(),
        2
    );
}
