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
    IndexInfo, PseudoColumnInfo, PseudoIndexInfo, PseudoTableInfo, QueryColumn, QueryIndexInfo,
    QueryTableInfo, StatsLoadedStatus, Table, TopN, ALL_EVICTED, PSEUDO_ROW_COUNT, PSEUDO_VERSION,
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
fn source_pseudo_table_filters_schema_and_optionally_fills_histograms() {
    let info = PseudoTableInfo {
        id: 42,
        pk_is_handle: true,
        columns: vec![
            PseudoColumnInfo {
                info: ColumnInfo {
                    id: 1,
                    name: "pk".to_owned(),
                    primary_key: true,
                },
                public: true,
                hidden: false,
            },
            PseudoColumnInfo {
                info: ColumnInfo {
                    id: 2,
                    name: "hidden".to_owned(),
                    primary_key: false,
                },
                public: true,
                hidden: true,
            },
            PseudoColumnInfo {
                info: ColumnInfo {
                    id: 3,
                    name: "nonpublic".to_owned(),
                    primary_key: false,
                },
                public: false,
                hidden: false,
            },
        ],
        indices: vec![
            PseudoIndexInfo {
                info: IndexInfo {
                    id: 4,
                    name: "public".to_owned(),
                    ..IndexInfo::default()
                },
                public: true,
            },
            PseudoIndexInfo {
                info: IndexInfo {
                    id: 5,
                    name: "nonpublic".to_owned(),
                    ..IndexInfo::default()
                },
                public: false,
            },
        ],
    };

    let metadata_only = tidb_stats::pseudo_table(&info, false, false);
    assert!(metadata_only.hist_coll.pseudo);
    assert!(metadata_only.hist_coll.cannot_trigger_load);
    assert_eq!(metadata_only.hist_coll.realtime_count, PSEUDO_ROW_COUNT);
    assert_eq!(metadata_only.version, PSEUDO_VERSION);
    assert_eq!(metadata_only.hist_coll.column_count(), 0);
    assert_eq!(metadata_only.hist_coll.index_count(), 0);
    let existence = metadata_only
        .existence_map
        .as_ref()
        .unwrap()
        .read()
        .unwrap();
    assert!(existence.has(1, false));
    assert!(!existence.has(2, false));
    assert!(!existence.has(3, false));
    assert!(existence.has(4, true));
    assert!(!existence.has(5, true));
    drop(existence);

    let filled = tidb_stats::pseudo_table(&info, true, true);
    assert!(!filled.hist_coll.cannot_trigger_load);
    assert_eq!(filled.hist_coll.column_count(), 1);
    assert_eq!(filled.hist_coll.index_count(), 1);
    let pk = filled.hist_coll.get_column(1).unwrap();
    let pk = pk.read().unwrap();
    assert!(pk.is_handle);
    assert_eq!(pk.physical_id, 42);
    assert_eq!(pk.histogram.id, 1);
    assert_eq!(
        filled
            .hist_coll
            .get_index(4)
            .unwrap()
            .read()
            .unwrap()
            .physical_id,
        42
    );
}

#[test]
fn source_id_to_unique_id_rekeys_only_requested_columns_and_shares_payloads() {
    let mut coll = HistColl::new(8, 90, 4, 2, 1);
    coll.set_column(1, column(1, 10, StatsLoadedStatus::full_load()));
    coll.set_column(2, column(2, 20, StatsLoadedStatus::full_load()));
    coll.set_index(3, index(3, 30, false, StatsLoadedStatus::full_load()));
    coll.pseudo = true;
    let mapped = coll.id_to_unique_id(&[
        QueryColumn {
            id: 2,
            unique_id: 102,
        },
        QueryColumn {
            id: 99,
            unique_id: 199,
        },
    ]);
    assert_eq!(mapped.physical_id, 8);
    assert_eq!(mapped.realtime_count, 90);
    assert_eq!(mapped.modify_count, 4);
    assert!(mapped.pseudo);
    assert_eq!(mapped.column_count(), 1);
    assert_eq!(mapped.index_count(), 0);
    let shared = mapped.get_column(102).unwrap();
    assert!(Arc::ptr_eq(&shared, &coll.get_column(2).unwrap()));
}

#[test]
fn source_generate_query_maps_keeps_partial_prefix_and_sorts_index_ids() {
    let coll = HistColl::new(8, 90, 4, 2, 4);
    coll.set_column(10, column(10, 10, StatsLoadedStatus::full_load()));
    for id in [9, 3, 7] {
        coll.set_index(id, index(id, 30, id == 7, StatsLoadedStatus::full_load()));
    }
    let table_info = QueryTableInfo {
        column_ids: vec![10, 20],
        indices: vec![
            QueryIndexInfo {
                id: 9,
                column_offsets: vec![0],
                mv_index: false,
            },
            QueryIndexInfo {
                id: 3,
                // The missing second planner column stops the loop but Go
                // retains the non-empty prefix and therefore the index.
                column_offsets: vec![0, 1],
                mv_index: false,
            },
            QueryIndexInfo {
                id: 7,
                column_offsets: vec![0],
                mv_index: true,
            },
        ],
    };
    let mapped = coll.generate_from_column_info(
        &table_info,
        &[QueryColumn {
            id: 10,
            unique_id: 100,
        }],
        |index, _| index.mv_index.then_some(vec![100]),
    );
    assert_eq!(mapped.column_count(), 1);
    assert_eq!(mapped.index_count(), 3);
    assert_eq!(mapped.idx_to_col_unique_ids[&3], [100]);
    assert_eq!(mapped.col_unique_id_to_idx_ids[&100], [3, 7, 9]);
    assert_eq!(mapped.unique_id_to_col_info_id[&100], 10);
    assert_eq!(mapped.mv_idx_to_columns[&7], [100]);
}

#[test]
fn source_get_stats_info_preserves_alias_or_deep_copy_contract() {
    let table = table();
    assert!(table.stats_info(999, false, false).is_none());
    assert!(table.stats_info(999, true, true).is_none());

    let shared = table.stats_info(2, false, false).unwrap();
    shared.with_components_mut(|histogram, cmsketch, top_n, fm_sketch| {
        histogram.ndv = 55;
        *cmsketch = None;
        *top_n = None;
        *fm_sketch = None;
    });
    assert_eq!(
        table
            .hist_coll
            .get_column(2)
            .unwrap()
            .read()
            .unwrap()
            .histogram
            .ndv,
        55
    );

    let copied = table.stats_info(3, true, true).unwrap();
    copied.with_components_mut(|histogram, _, _, _| histogram.ndv = 77);
    copied.with_components(|histogram, _, _, _| assert_eq!(histogram.ndv, 77));
    assert_ne!(
        table
            .hist_coll
            .get_index(3)
            .unwrap()
            .read()
            .unwrap()
            .histogram
            .ndv,
        77
    );
}

#[test]
#[should_panic]
fn source_index_start_lookup_panics_for_empty_column_metadata() {
    let coll = HistColl::new(1, 1, 0, 0, 1);
    coll.set_index(
        1,
        Index {
            info: Some(IndexInfo {
                id: 1,
                name: "broken".to_owned(),
                columns: Vec::new(),
                mv_index: false,
            }),
            ..Index::default()
        },
    );
    let table = Table {
        existence_map: None,
        hist_coll: coll,
        version: 0,
        last_analyze_version: 0,
        last_stats_hist_version: 0,
        table_info_update_ts: 0,
        is_pk_handle: false,
    };
    let _ = table.index_starting_with_column("x");
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
