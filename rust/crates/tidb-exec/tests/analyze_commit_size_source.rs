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

//! An `ANALYZE` of a real table must fit the transaction it commits in.
//!
//! A toy table's statistics are a handful of rows, so the size of the write
//! only becomes a question at the shape a real table produces: the default
//! `ANALYZE` builds 256 buckets and 100 TopN entries *per histogram*, and a
//! table with a few columns and an index has six of them. Go saves all of one
//! table's histograms in one transaction
//! (`pkg/statistics/handle/storage/stats_read_writer.go:141` wraps
//! `SaveAnalyzeResultToStorage` in a single `util.FlagWrapTxn` `BEGIN
//! PESSIMISTIC` ... `COMMIT`), and enforces no mutation *count* on it at all --
//! only the byte limits. This test states the same for this node's path.

use std::collections::{BTreeMap, HashMap};

use tidb_ast::CiString;
use tidb_datatype::{Datum, Time, TimeType};
use tidb_exec::cluster_catalog::{
    load_cluster_catalog, ClusterCatalogError, MetaPairs, MetaSnapshot,
};
use tidb_exec::cluster_predicate_column::ColumnStatsTimeInfo;
use tidb_exec::cluster_predicate_column::{
    load_column_stats_usage, load_column_stats_usage_for_table,
};
use tidb_exec::cluster_stats_load::{ClusterStatsItem, ClusterStatsLoader, ClusterTableStats};
use tidb_exec::cluster_stats_dump::{
    load_table_stats_payload, table_historical_stats_to_json,
    table_stats_to_json_from_loaded,
};
use tidb_exec::cluster_stats_write::{
    count_outdated_historical_stats, load_analyze_options, load_stats_gc_candidates,
    load_stats_gc_timestamp, plan_analyze_options_write, plan_column_stats_usage_dump,
    plan_column_stats_usage_write, plan_delete_table_stats, plan_get_predicate_columns,
    plan_historical_stats_data_delete_for_table, plan_historical_stats_data_write,
    plan_historical_stats_meta_delete_for_table, plan_historical_stats_meta_write,
    plan_independent_index_stats_write, plan_loaded_stats_item_write,
    plan_loaded_stats_meta_write, plan_loaded_stats_usage_write,
    plan_outdated_historical_data_delete, plan_outdated_historical_meta_delete,
    plan_change_global_stats_id, plan_partial_stats_write, plan_partition_stats_write,
    plan_stats_delta_updates, plan_stats_gc_timestamp_write, plan_stats_item_delete,
    plan_stats_meta_version_refresh, plan_stats_write, plan_update_stats_version,
};
use tidb_exec::mysql_bootstrap::{plan_mysql_bootstrap, BootstrapEnvironment, BootstrapWrite};
use tidb_exec::mysql_system_tables::{scan_system_table, SystemRow, SystemTableView};
use tidb_exec::real_tikv_analyze::ANALYZE_MAX_MUTATIONS;
use tidb_executor::analyze::{AnalyzeColumnChoice, AnalyzeOptionOverrides};
use tidb_model::column::ColumnInfo;
use tidb_model::table_info::TableInfo;
use tidb_model::SchemaState;
use tidb_stats::cmsketch::{CmsSketch, TopN};
use tidb_stats::histogram::{Bucket, Histogram};
use tidb_stats::{FmSketch, JsonPredicateColumn, JsonTable, MAX_SKETCH_SIZE};
use tidb_stats_handle_usage::{DeltaUpdate, TableDelta};
use tidb_txnkv::transaction::{
    OptimisticMutationKind, MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};

#[derive(Default)]
struct MetaStore {
    pairs: BTreeMap<Vec<u8>, Vec<u8>>,
    scans: Vec<Vec<u8>>,
}

impl MetaSnapshot for MetaStore {
    fn get(&mut self, raw_key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        Ok(self.pairs.get(raw_key).cloned())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        self.scans.push(prefix.to_vec());
        Ok(self
            .pairs
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

fn apply_mutations(
    store: &mut MetaStore,
    mutations: &[tidb_txnkv::transaction::OptimisticMutation],
) {
    for mutation in mutations {
        match mutation.kind() {
            OptimisticMutationKind::MetaDelete
            | OptimisticMutationKind::Delete
            | OptimisticMutationKind::IndexDelete => {
                store.pairs.remove(mutation.key());
            }
            _ => {
                store
                    .pairs
                    .insert(mutation.key().to_vec(), mutation.value().to_vec());
            }
        }
    }
}

fn apply(store: &mut MetaStore, write: &BootstrapWrite) {
    for mutation in &write.mutations {
        match mutation.kind() {
            OptimisticMutationKind::MetaDelete
            | OptimisticMutationKind::Delete
            | OptimisticMutationKind::IndexDelete => {
                store.pairs.remove(mutation.key());
            }
            _ => {
                store
                    .pairs
                    .insert(mutation.key().to_vec(), mutation.value().to_vec());
            }
        }
    }
}

fn now() -> Time {
    Time::from_date_checked(2026, 7, 29, 6, 12, 55, 0, TimeType::Timestamp, 0)
        .expect("a fixed calendar date is a valid timestamp")
}

fn bootstrapped() -> MetaStore {
    let mut store = MetaStore::default();
    let write = plan_mysql_bootstrap(
        &mut store,
        467_996_279_696_261_139,
        &BootstrapEnvironment {
            system_tz: "Asia/Shanghai".to_owned(),
            new_collation_enabled: true,
            cluster_id: 7_667_705_271_188_879_689,
            current_timestamp: now(),
            ddl_table_version: 0,
        },
    )
    .expect("a fresh keyspace bootstraps");
    apply(&mut store, &write);
    store
}

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
        },
        topn: Some(topn),
        cms: None,
        fm_sketch: None,
    }
}

#[test]
fn a_real_table_s_six_histograms_fit_one_analyze_transaction() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let stats = ClusterTableStats {
        table_id: 4242,
        version: 440_000_000_000_000_000,
        snapshot: 440_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: (1..=4).map(|id| full_histogram(id, false)).collect(),
        indexes: (1..=2).map(|id| full_histogram(id, true)).collect(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &stats, now())
        .expect("a full-sized analyze result plans");
    let planned = plan.mutations.len();
    // The shape that made this a defect rather than a hypothetical: six
    // histograms of the default size are already past the bounded path's
    // generic ceiling, so an ANALYZE that declared that ceiling as its budget
    // worked on toy tables and hard-failed on real ones.
    assert!(
        planned > MAX_OPTIMISTIC_MUTATIONS,
        "the test's own premise is gone: {planned} mutations no longer exceed \
         the generic ceiling {MAX_OPTIMISTIC_MUTATIONS}"
    );
    // ... so an ANALYZE must not declare that ceiling as its own budget. It
    // declares Go's, which is no count bound at all.
    assert_ne!(
        ANALYZE_MAX_MUTATIONS, MAX_OPTIMISTIC_MUTATIONS,
        "an ANALYZE that declares the generic budget refuses its own \
         {planned}-mutation plan"
    );
    assert_eq!(ANALYZE_MAX_MUTATIONS, usize::MAX);
    // What the plan is actually held to now, and what Go is held to as well.
    let bytes: usize = plan
        .mutations
        .iter()
        .map(|mutation| mutation.key().len() + mutation.value().len())
        .sum();
    assert!(
        bytes <= MAX_OPTIMISTIC_TRANSACTION_BYTES,
        "an ANALYZE of a real table plans {bytes} bytes, over the transaction \
         byte budget {MAX_OPTIMISTIC_TRANSACTION_BYTES}"
    );
}

/// The analyze snapshot and `last_stats_histograms_version` columns round-trip:
/// Go stores `AnalyzeResults.Snapshot` beside the write version, and stamps
/// the histogram version with the same analyze start TS. The loader must keep
/// all three identities distinct even though this fixture gives them one TSO.
#[test]
fn the_analyze_version_round_trips_through_the_stored_row() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let stats = ClusterTableStats {
        table_id: 4242,
        version: 440_000_000_000_000_000,
        snapshot: 440_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10,
        columns: vec![],
        indexes: vec![],
    };
    let plan =
        plan_stats_write(&mut store, &catalog, &stats, now()).expect("a small analyze plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    assert_eq!(
        loader
            .load_meta(&mut store, 4242)
            .expect("the row reads back"),
        Some((
            440_000_000_000_000_000,
            440_000_000_000_000_000,
            0,
            10,
            440_000_000_000_000_000
        )),
        "version, snapshot, modify_count, count, last_analyze_version"
    );
}

/// Pinned Go `SaveAnalyzeResultToStorage` keeps FM sketches only for physical
/// partition results. Ordinary cache loads do not fetch them; the `loadAll`
/// path used by partition-to-global merge does, and a global write removes
/// any stale row instead of persisting a global sketch.
#[test]
fn partition_fm_sketch_round_trips_only_for_global_merge() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let expected = FmSketch::from_raw_parts(3, MAX_SKETCH_SIZE, [4, 8, 12]);
    let mut column = full_histogram(1, false);
    column.fm_sketch = Some(expected.clone());
    let stats = ClusterTableStats {
        table_id: 4242,
        version: 440_000_000_000_000_000,
        snapshot: 440_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: vec![column],
        indexes: vec![],
    };
    let plan = plan_partition_stats_write(&mut store, &catalog, &stats, now())
        .expect("partition analyze plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let column_types = BTreeMap::from([(
        1,
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
    )]);
    let ordinary = loader
        .load_table(&mut store, stats.table_id, &column_types)
        .expect("ordinary statistics load")
        .expect("stats_meta exists");
    assert!(ordinary.columns[0].fm_sketch.is_none());

    let merge_input = loader
        .load_table_with_fm(&mut store, stats.table_id, &column_types)
        .expect("merge-input statistics load")
        .expect("stats_meta exists");
    assert_eq!(merge_input.columns[0].fm_sketch.as_ref(), Some(&expected));

    let global =
        plan_stats_write(&mut store, &catalog, &stats, now()).expect("global analyze plans");
    apply_mutations(&mut store, &global.mutations);
    let merge_input = loader
        .load_table_with_fm(&mut store, stats.table_id, &column_types)
        .expect("merge-input statistics reload")
        .expect("stats_meta exists");
    assert!(merge_input.columns[0].fm_sketch.is_none());
}

/// Pinned async global-stat preparation probes histogram existence by item
/// kind, then each worker phase reads exactly one
/// `(table_id, is_index, hist_id)` payload at a time.
#[test]
fn async_global_stats_loads_each_payload_by_item() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let expected = FmSketch::from_raw_parts(3, MAX_SKETCH_SIZE, [8, 16, 24]);
    let mut expected_cms = CmsSketch::new(2, 8);
    expected_cms.insert_bytes_by_count(b"x", 5);
    let mut column = full_histogram(1, false);
    column.fm_sketch = Some(expected.clone());
    let stats = ClusterTableStats {
        table_id: 4242,
        version: 440_000_000_000_000_000,
        snapshot: 440_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: vec![column],
        indexes: Vec::new(),
    };
    let plan = plan_partition_stats_write(&mut store, &catalog, &stats, now())
        .expect("partition analyze plans");
    apply_mutations(&mut store, &plan.mutations);
    let mut cms_item = full_histogram(2, false);
    cms_item.stats_ver = 1;
    cms_item.cms = Some(expected_cms);
    let plan = plan_loaded_stats_item_write(
        &mut store,
        &catalog,
        stats.table_id,
        stats.row_count as i64,
        &cms_item,
        stats.version,
        now(),
    )
    .expect("a version-1 CMSketch load plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    store.scans.clear();
    assert!(loader
        .has_histogram_rows(&mut store, stats.table_id, false)
        .expect("column histogram existence probe succeeds"));
    assert!(!loader
        .has_histogram_rows(&mut store, stats.table_id, true)
        .expect("index histogram existence probe succeeds"));
    assert_eq!(
        loader
            .load_fm_sketch(&mut store, stats.table_id, false, 1)
            .expect("one FM sketch loads")
            .as_ref(),
        Some(&expected)
    );
    assert!(loader
        .load_fm_sketch(&mut store, stats.table_id, false, 2)
        .expect("a missing FM sketch is not an error")
        .is_none());
    assert_eq!(
        loader
            .load_item_cmsketch(&mut store, stats.table_id, false, 2)
            .expect("one CMSketch loads")
            .expect("the CMSketch exists")
            .total_count(),
        5
    );
    let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
    assert_eq!(
        loader
            .load_item_histogram(&mut store, stats.table_id, false, 1, Some(&field_type))
            .expect("one histogram loads")
            .expect("the histogram exists")
            .buckets
            .len(),
        256
    );
    assert_eq!(
        loader
            .load_item_topn(&mut store, stats.table_id, false, 1)
            .expect("one TopN loads")
            .as_ref()
            .map(TopN::num),
        Some(100)
    );

    let column_kind = tidb_codec::encode_key(&[Datum::Int(stats.table_id), Datum::Int(0)])
        .expect("column-kind prefix encodes");
    let index_kind = tidb_codec::encode_key(&[Datum::Int(stats.table_id), Datum::Int(1)])
        .expect("index-kind prefix encodes");
    let column_item =
        tidb_codec::encode_key(&[Datum::Int(stats.table_id), Datum::Int(0), Datum::Int(1)])
            .expect("column-item prefix encodes");
    let cms_item_prefix =
        tidb_codec::encode_key(&[Datum::Int(stats.table_id), Datum::Int(0), Datum::Int(2)])
            .expect("CMS-item prefix encodes");
    assert_eq!(
        store.scans,
        vec![
            tidb_codec::encode_row_key(
                tidb_metadef::system::STATS_HISTOGRAMS_TABLE_ID,
                &column_kind,
            ),
            tidb_codec::encode_row_key(
                tidb_metadef::system::STATS_HISTOGRAMS_TABLE_ID,
                &index_kind,
            ),
            tidb_codec::encode_row_key(tidb_metadef::system::STATS_FMSKETCH_TABLE_ID, &column_item,),
            tidb_codec::encode_row_key(
                tidb_metadef::system::STATS_FMSKETCH_TABLE_ID,
                &cms_item_prefix,
            ),
            tidb_codec::encode_row_key(
                tidb_metadef::system::STATS_HISTOGRAMS_TABLE_ID,
                &cms_item_prefix,
            ),
            tidb_codec::encode_row_key(
                tidb_metadef::system::STATS_HISTOGRAMS_TABLE_ID,
                &column_item,
            ),
            tidb_codec::encode_row_key(tidb_metadef::system::STATS_BUCKETS_TABLE_ID, &column_item,),
            tidb_codec::table_key::encode_index_seek_key(
                tidb_metadef::system::STATS_TOP_NTABLE_ID,
                1,
                &column_item,
            ),
        ]
    );
}

/// Go `TestShowHistogramsLoadStatus`: a leased cache initializes analyzed
/// histograms from metadata only, so their payload is absent and their load
/// state is `allEvicted` until sync-load requests that item.
#[test]
fn lite_load_keeps_metadata_and_evicts_the_histogram_payload() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let stats = ClusterTableStats {
        table_id: 4242,
        version: 440_000_000_000_000_000,
        snapshot: 440_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: vec![full_histogram(1, false)],
        indexes: vec![full_histogram(2, true)],
    };
    let plan = plan_stats_write(&mut store, &catalog, &stats, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let column_types = BTreeMap::from([(
        1,
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
    )]);
    let loaded = loader
        .load_table_lite(&mut store, 4242, &column_types)
        .expect("lite statistics load")
        .expect("stats_meta exists");

    for item in loaded.columns.iter().chain(&loaded.indexes) {
        assert_eq!(item.histogram.ndv, 8000);
        assert_eq!(item.histogram.last_update_version, stats.version);
        assert!(item.histogram.buckets.is_empty());
        assert!(item.topn.is_none());
        assert!(item.cms.is_none());
        assert_eq!(item.load_status.status_to_string(), "allEvicted");
    }
}

/// Pinned Go `StatsCacheImpl.Update` does not evict an item merely because a
/// table refreshes. It preserves an unchanged resident histogram, and when a
/// newer histogram replaces a resident one it reloads that item in full.
#[test]
fn cache_update_preserves_and_refreshes_resident_histogram_payload() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let old_stats = ClusterTableStats {
        table_id: 4242,
        version: 440_000_000_000_000_000,
        snapshot: 440_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: vec![full_histogram(1, false)],
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &old_stats, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let column_types = BTreeMap::from([(
        1,
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
    )]);
    let table_info = TableInfo {
        id: old_stats.table_id,
        columns: vec![ColumnInfo {
            id: 1,
            name: CiString::new("a"),
            field_type: column_types[&1].clone(),
            state: SchemaState::PUBLIC,
            ..ColumnInfo::default()
        }]
        .into(),
        ..TableInfo::default()
    };
    let resident = loader
        .load_table(&mut store, old_stats.table_id, &column_types)
        .expect("full statistics load")
        .expect("stats_meta exists")
        .to_statistics_table(&table_info);
    let unchanged = loader
        .load_statistics_table_for_update(
            &mut store,
            &table_info,
            &column_types,
            Some(resident.as_ref()),
        )
        .expect("cache update")
        .expect("stats_meta exists");
    let unchanged_column = unchanged.hist_coll.get_column(1).expect("column exists");
    let unchanged_column = unchanged_column.read().unwrap();
    assert_eq!(unchanged_column.histogram.buckets.len(), 256);
    assert_eq!(unchanged_column.top_n.as_ref().map(TopN::num), Some(100));
    assert!(unchanged_column.is_full_load());

    let next_version = old_stats.version + 1;
    let mut changed = full_histogram(1, false);
    changed.histogram.last_update_version = next_version;
    changed.histogram.buckets[0].count = 41;
    let new_stats = ClusterTableStats {
        version: next_version,
        snapshot: next_version,
        last_analyze_version: next_version,
        last_stats_hist_version: next_version,
        row_count: 10_241,
        columns: vec![changed],
        ..old_stats
    };
    let plan = plan_stats_write(&mut store, &catalog, &new_stats, now()).expect("reanalyze plans");
    apply_mutations(&mut store, &plan.mutations);
    let refreshed = loader
        .load_statistics_table_for_update(
            &mut store,
            &table_info,
            &column_types,
            Some(resident.as_ref()),
        )
        .expect("cache refresh")
        .expect("stats_meta exists");
    let item = refreshed.hist_coll.get_column(1).expect("column exists");
    let item = item.read().unwrap();
    assert_eq!(refreshed.version, next_version);
    assert_eq!(item.histogram.last_update_version, next_version);
    assert_eq!(item.histogram.buckets[0].count, 41);
    assert_eq!(item.top_n.as_ref().map(TopN::num), Some(100));
    assert!(item.is_full_load());
}

/// Go's `CMSketchAndTopNFromStorageWithHighPriority` reaches TopN through
/// `INDEX tbl(table_id, is_index, hist_id)`. A table statistics load must not
/// scan the cluster-wide `stats_top_n` record range.
#[test]
fn topn_load_uses_the_declared_secondary_index() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let stats = ClusterTableStats {
        table_id: 4242,
        version: 440_000_000_000_000_000,
        snapshot: 440_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: vec![full_histogram(1, false)],
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &stats, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);
    store.scans.clear();

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let column_types = BTreeMap::from([(
        1,
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
    )]);
    let loaded = loader
        .load_table(&mut store, stats.table_id, &column_types)
        .expect("full statistics load")
        .expect("stats_meta exists");
    assert_eq!(loaded.columns[0].topn.as_ref().map(TopN::num), Some(100));

    let encoded_prefix = tidb_codec::encode_key(&[Datum::Int(stats.table_id)]).unwrap();
    let index_prefix = tidb_codec::table_key::encode_index_seek_key(
        tidb_metadef::system::STATS_TOP_NTABLE_ID,
        1,
        &encoded_prefix,
    );
    assert!(store.scans.contains(&index_prefix));
    assert!(!store.scans.contains(&tidb_codec::gen_table_record_prefix(
        tidb_metadef::system::STATS_TOP_NTABLE_ID,
    )));
}

/// Go `readStatsForOneItem` reads only the requested histogram item: one
/// clustered histogram prefix, one clustered bucket prefix, and one TopN
/// secondary-index prefix.
#[test]
fn one_item_load_uses_only_that_items_key_ranges() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let stats = ClusterTableStats {
        table_id: 4242,
        version: 440_000_000_000_000_000,
        snapshot: 440_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: vec![full_histogram(1, false), full_histogram(2, false)],
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &stats, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);
    store.scans.clear();

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
    let loaded = loader
        .load_item(
            &mut store,
            stats.table_id,
            false,
            1,
            Some(&field_type),
            true,
        )
        .expect("one item loads")
        .expect("the histogram exists");
    assert_eq!(loaded.id, 1);
    assert_eq!(loaded.histogram.buckets.len(), 256);
    assert_eq!(loaded.topn.as_ref().map(TopN::num), Some(100));
    assert_eq!(loaded.load_status.status_to_string(), "allLoaded");

    let item = [Datum::Int(stats.table_id), Datum::Int(0), Datum::Int(1)];
    let encoded_item = tidb_codec::encode_key(&item).unwrap();
    let histogram_prefix = tidb_codec::encode_row_key(
        tidb_metadef::system::STATS_HISTOGRAMS_TABLE_ID,
        &encoded_item,
    );
    let bucket_prefix =
        tidb_codec::encode_row_key(tidb_metadef::system::STATS_BUCKETS_TABLE_ID, &encoded_item);
    let topn_prefix = tidb_codec::table_key::encode_index_seek_key(
        tidb_metadef::system::STATS_TOP_NTABLE_ID,
        1,
        &encoded_item,
    );
    assert_eq!(
        store.scans,
        vec![histogram_prefix, bucket_prefix, topn_prefix]
    );
}

/// Go `SaveColOrIdxStatsToStorage` replaces only the loaded object's payload,
/// preserves unrelated histograms, stores the CMSketch without embedded
/// TopN, and clears stale buckets/TopN rows for that object.
#[test]
fn loaded_stats_item_write_replaces_only_the_named_histogram() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let initial = ClusterTableStats {
        table_id,
        version: 440_000_000_000_000_000,
        snapshot: 439_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 7,
        row_count: 10_240,
        columns: vec![full_histogram(1, false), full_histogram(2, false)],
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &initial, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);

    let version = initial.version + 10;
    let mut loaded = full_histogram(1, false);
    loaded.stats_ver = 1;
    loaded.histogram.last_update_version = version;
    loaded.histogram.buckets.truncate(2);
    loaded.histogram.ndv = 17;
    let mut topn = TopN::new(1);
    topn.append(b"replacement", 9);
    loaded.topn = Some(topn);
    let mut cms = CmsSketch::new(2, 16);
    cms.insert_bytes_by_count(b"replacement", 9);
    loaded.cms = Some(cms);
    let plan =
        plan_loaded_stats_item_write(&mut store, &catalog, table_id, 55, &loaded, version, now())
            .expect("LOAD STATS item plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let column_types = BTreeMap::from([
        (
            1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        ),
        (
            2,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        ),
    ]);
    let stored = loader
        .load_table(&mut store, table_id, &column_types)
        .expect("statistics reload")
        .expect("stats_meta exists");
    assert_eq!(stored.version, version);
    assert_eq!(stored.snapshot, 0, "REPLACE leaves snapshot at its default");
    assert_eq!(stored.modify_count, 0);
    assert_eq!(stored.row_count, 55);
    let replaced = stored.column(1).expect("loaded column remains");
    assert_eq!(replaced.histogram.ndv, 17);
    assert_eq!(replaced.histogram.buckets.len(), 2);
    assert_eq!(replaced.topn.as_ref().map(TopN::num), Some(1));
    assert_eq!(
        replaced
            .cms
            .as_ref()
            .map(|sketch| sketch.query_bytes(b"replacement")),
        Some(9)
    );
    let untouched = stored.column(2).expect("unmentioned column remains");
    assert_eq!(untouched.histogram.buckets.len(), 256);
    assert_eq!(untouched.topn.as_ref().map(TopN::num), Some(100));
}

/// Pinned `deleteHistStatsFromKV` advances the table versions and removes
/// exactly one column's histogram payload plus its predicate-usage row.
#[test]
fn stats_item_gc_removes_only_the_dropped_column_in_one_plan() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4244;
    let stats = ClusterTableStats {
        table_id,
        version: 440_000_000_000_000_000,
        snapshot: 439_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: vec![full_histogram(1, false), full_histogram(2, false)],
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &stats, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);
    let usage = HashMap::from([
        (usage_item(table_id, 1), ColumnStatsTimeInfo::default()),
        (usage_item(table_id, 2), ColumnStatsTimeInfo::default()),
    ]);
    let plan = plan_column_stats_usage_write(&mut store, &catalog, &usage, now())
        .expect("predicate usage plans");
    apply_mutations(&mut store, &plan.mutations);

    let gc_version = stats.version + 10;
    let plan = plan_stats_item_delete(&mut store, &catalog, table_id, 1, false, gc_version)
        .expect("column GC plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let column_types = BTreeMap::from([
        (
            1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        ),
        (
            2,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        ),
    ]);
    let stored = loader
        .load_table(&mut store, table_id, &column_types)
        .expect("statistics reload")
        .expect("stats_meta remains");
    assert_eq!(stored.version, gc_version);
    assert!(stored.column(1).is_none());
    assert!(stored.column(2).is_some());
    let usage = load_column_stats_usage_for_table(
        &mut store,
        &catalog,
        &tidb_datatype::SessionTimeZone::utc(),
        table_id,
    )
    .expect("predicate usage reloads");
    assert!(!usage.contains_key(&usage_item(table_id, 1)));
    assert!(usage.contains_key(&usage_item(table_id, 2)));
}

/// Pinned `DeleteTableStatsFromKV` keeps zeroed histogram metadata for soft
/// DROP STATS and removes it for hard table GC while retaining stats_meta for
/// the second GC phase.
#[test]
fn table_stats_delete_preserves_go_soft_and_hard_phases() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4245;
    let stats = ClusterTableStats {
        table_id,
        version: 440_000_000_000_000_000,
        snapshot: 439_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: vec![full_histogram(1, false)],
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &stats, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);
    let column_types = BTreeMap::from([(
        1,
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
    )]);
    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");

    let soft_version = stats.version + 10;
    let plan = plan_delete_table_stats(&mut store, &catalog, &[table_id], true, soft_version)
        .expect("soft DROP STATS plans");
    apply_mutations(&mut store, &plan.mutations);
    let soft = loader
        .load_table(&mut store, table_id, &column_types)
        .expect("soft-deleted statistics reload")
        .expect("stats_meta remains");
    assert_eq!(soft.version, soft_version);
    let histogram = soft.column(1).expect("soft delete retains histogram metadata");
    assert_eq!(histogram.histogram.ndv, 0);
    assert!(histogram.histogram.buckets.is_empty());
    assert!(histogram.topn.is_none());

    let hard_version = soft_version + 10;
    let plan = plan_delete_table_stats(&mut store, &catalog, &[table_id], false, hard_version)
        .expect("hard table GC plans");
    apply_mutations(&mut store, &plan.mutations);
    let hard = loader
        .load_table(&mut store, table_id, &column_types)
        .expect("hard-deleted statistics reload")
        .expect("stats_meta remains for phase two");
    assert_eq!(hard.version, hard_version);
    assert!(hard.column(1).is_none());
}

/// Pinned `GCStats` scans a half-open metadata-version window and persists
/// its upper bound in the single `mysql.tidb` row used by the next pass.
#[test]
fn stats_gc_window_and_timestamp_are_half_open_and_persistent() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    for (table_id, version) in [(4250, 100), (4251, 200)] {
        let stats = ClusterTableStats {
            table_id,
            version,
            snapshot: 0,
            last_analyze_version: version,
            last_stats_hist_version: version,
            modify_count: 0,
            row_count: 1,
            columns: Vec::new(),
            indexes: Vec::new(),
        };
        let plan = plan_stats_write(&mut store, &catalog, &stats, now()).expect("stats plan");
        apply_mutations(&mut store, &plan.mutations);
    }

    assert_eq!(
        load_stats_gc_candidates(&mut store, &catalog, 100, 200)
            .expect("GC window loads"),
        vec![4250]
    );
    assert_eq!(
        load_stats_gc_timestamp(&mut store, &catalog).expect("missing timestamp is zero"),
        0
    );
    for version in [200, 300] {
        let plan = plan_stats_gc_timestamp_write(&mut store, &catalog, version, now())
            .expect("GC timestamp upsert plans");
        apply_mutations(&mut store, &plan.mutations);
        assert_eq!(
            load_stats_gc_timestamp(&mut store, &catalog).expect("GC timestamp reloads"),
            version
        );
    }
}

/// Pinned Go reads raw options, filters stale LIST IDs through current table
/// metadata, and REPLACEs all seven columns so newly omitted values clear.
#[test]
fn persisted_analyze_options_round_trip_and_replace_raw_values() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table = TableInfo {
        id: 4242,
        columns: vec![
            ColumnInfo {
                id: 1,
                name: CiString::new("a"),
                offset: 0,
                state: SchemaState::PUBLIC,
                ..ColumnInfo::default()
            },
            ColumnInfo {
                id: 2,
                name: CiString::new("b"),
                offset: 1,
                state: SchemaState::PUBLIC,
                ..ColumnInfo::default()
            },
        ]
        .into(),
        ..TableInfo::default()
    };
    assert!(load_analyze_options(&mut store, &catalog, &table, table.id)
        .expect("missing row is readable")
        .is_none());

    let first = plan_analyze_options_write(
        &mut store,
        &catalog,
        &table,
        table.id,
        AnalyzeOptionOverrides {
            num_buckets: Some(7),
            num_topn: Some(0),
            num_samples: None,
            sample_rate: Some(0.25),
        },
        &AnalyzeColumnChoice::Explicit(vec!["a".to_owned(), "gone".to_owned()]),
        now(),
    )
    .expect("first option row plans");
    apply_mutations(&mut store, &first.mutations);
    assert_eq!(
        load_analyze_options(&mut store, &catalog, &table, table.id)
            .expect("saved row loads")
            .expect("saved row exists"),
        tidb_exec::cluster_stats_write::PersistedAnalyzeOptions {
            raw: AnalyzeOptionOverrides {
                num_buckets: Some(7),
                num_topn: Some(0),
                num_samples: None,
                sample_rate: Some(0.25),
            },
            columns: AnalyzeColumnChoice::Explicit(vec!["a".to_owned()]),
        }
    );

    let second = plan_analyze_options_write(
        &mut store,
        &catalog,
        &table,
        table.id,
        AnalyzeOptionOverrides {
            num_samples: Some(9),
            ..AnalyzeOptionOverrides::default()
        },
        &AnalyzeColumnChoice::Predicate,
        now(),
    )
    .expect("replacement option row plans");
    apply_mutations(&mut store, &second.mutations);
    assert_eq!(
        load_analyze_options(&mut store, &catalog, &table, table.id)
            .expect("replacement loads")
            .expect("replacement exists"),
        tidb_exec::cluster_stats_write::PersistedAnalyzeOptions {
            raw: AnalyzeOptionOverrides {
                num_samples: Some(9),
                ..AnalyzeOptionOverrides::default()
            },
            columns: AnalyzeColumnChoice::Predicate,
        }
    );
}

/// Go `SaveAnalyzeResultToStorage` iterates only the histograms returned by a
/// partial-column analyze, replacing their payload without deleting older
/// statistics for unselected columns.
#[test]
fn partial_analyze_write_preserves_unselected_histograms() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let initial = ClusterTableStats {
        table_id,
        version: 440_000_000_000_000_000,
        snapshot: 439_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: vec![full_histogram(1, false), full_histogram(2, false)],
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &initial, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);

    let mut replacement = full_histogram(1, false);
    replacement.histogram.buckets.truncate(2);
    replacement.histogram.ndv = 17;
    let partial = ClusterTableStats {
        version: initial.version + 1,
        last_analyze_version: initial.version + 1,
        last_stats_hist_version: initial.version + 1,
        columns: vec![replacement],
        ..initial
    };
    let plan = plan_partial_stats_write(&mut store, &catalog, &partial, now())
        .expect("partial analyze plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let column_types = BTreeMap::from([
        (
            1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        ),
        (
            2,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        ),
    ]);
    let stored = loader
        .load_table(&mut store, table_id, &column_types)
        .expect("statistics reload")
        .expect("stats_meta exists");
    assert_eq!(stored.column(1).expect("selected column").histogram.ndv, 17);
    assert_eq!(
        stored
            .column(1)
            .expect("selected column")
            .histogram
            .buckets
            .len(),
        2
    );
    assert_eq!(
        stored
            .column(2)
            .expect("unselected column remains")
            .histogram
            .buckets
            .len(),
        256
    );
}

/// Pinned Go's `ForMVIndexOrGlobalIndex` path never lets an independently
/// scanned index overwrite table-level row metadata.
#[test]
fn independent_index_write_preserves_existing_meta_values() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let initial = ClusterTableStats {
        table_id,
        version: 440_000_000_000_000_000,
        snapshot: 439_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 37,
        row_count: 10_240,
        columns: vec![full_histogram(1, false)],
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &initial, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);

    let next_version = initial.version + 1;
    let mut index = full_histogram(7, true);
    index.histogram.last_update_version = next_version;
    let independent = ClusterTableStats {
        table_id,
        version: next_version,
        snapshot: next_version,
        last_analyze_version: next_version,
        last_stats_hist_version: next_version,
        modify_count: 0,
        row_count: 99_999,
        columns: Vec::new(),
        indexes: vec![index],
    };
    let (plan, inserted_meta) =
        plan_independent_index_stats_write(&mut store, &catalog, &independent, now())
            .expect("independent index analyze plans");
    assert!(!inserted_meta);
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let stored = loader
        .load_table(
            &mut store,
            table_id,
            &BTreeMap::from([(
                1,
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            )]),
        )
        .expect("statistics reload")
        .expect("stats_meta exists");
    assert_eq!(stored.version, next_version);
    assert_eq!(stored.last_stats_hist_version, next_version);
    assert_eq!(stored.row_count, initial.row_count);
    assert_eq!(stored.modify_count, initial.modify_count);
    assert_eq!(stored.snapshot, initial.snapshot);
    assert!(stored.index(7).is_some());
    assert!(stored.column(1).is_some());
}

#[test]
fn independent_index_write_creates_zero_count_zero_snapshot_meta() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let version = 440_000_000_000_000_001;
    let stats = ClusterTableStats {
        table_id: 4242,
        version,
        snapshot: version,
        last_analyze_version: version,
        last_stats_hist_version: version,
        modify_count: 91,
        row_count: 99_999,
        columns: Vec::new(),
        indexes: vec![full_histogram(7, true)],
    };
    let (plan, inserted_meta) =
        plan_independent_index_stats_write(&mut store, &catalog, &stats, now())
            .expect("independent index analyze plans");
    assert!(inserted_meta);
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let stored = loader
        .load_table(&mut store, stats.table_id, &BTreeMap::new())
        .expect("statistics reload")
        .expect("stats_meta exists");
    assert_eq!(stored.version, version);
    assert_eq!(stored.last_stats_hist_version, version);
    assert_eq!(stored.row_count, 0);
    assert_eq!(stored.modify_count, 0);
    assert_eq!(stored.snapshot, 0);
    assert!(stored.index(7).is_some());
}

/// Pinned Go's negative-count branch uses UPDATE rather than REPLACE: only
/// the two version markers move, while count/modify/snapshot stay intact.
#[test]
fn loaded_stats_negative_count_preserves_existing_meta_values() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let initial = ClusterTableStats {
        table_id,
        version: 440_000_000_000_000_000,
        snapshot: 439_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 7,
        row_count: 10_240,
        columns: Vec::new(),
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &initial, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);

    let version = initial.version + 1;
    let item = full_histogram(1, false);
    let plan =
        plan_loaded_stats_item_write(&mut store, &catalog, table_id, -1, &item, version, now())
            .expect("negative-count item plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    assert_eq!(
        loader.load_meta(&mut store, table_id).expect("meta loads"),
        Some((
            version,
            initial.snapshot,
            initial.modify_count,
            initial.row_count,
            version,
        ))
    );
}

/// Go's final `SaveMetaToStorage` is an upsert update, not the per-item
/// `REPLACE`; when a dump contains no matching histogram it updates the
/// counters/version without resetting the prior analyze snapshot.
#[test]
fn loaded_stats_final_meta_update_preserves_unnamed_columns() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let initial = ClusterTableStats {
        table_id,
        version: 440_000_000_000_000_000,
        snapshot: 439_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 7,
        row_count: 10_240,
        columns: Vec::new(),
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &initial, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);

    let version = initial.version + 1;
    let plan = plan_loaded_stats_meta_write(&mut store, &catalog, table_id, 55, 3, version, now())
        .expect("LOAD STATS final meta plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    assert_eq!(
        loader.load_meta(&mut store, table_id).expect("meta loads"),
        Some((version, initial.snapshot, 3, 55, version))
    );
}

#[test]
fn slow_save_version_refresh_changes_only_the_two_go_columns() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let initial = ClusterTableStats {
        table_id,
        version: 440_000_000_000_000_000,
        snapshot: 439_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 7,
        row_count: 10_240,
        columns: Vec::new(),
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &initial, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);

    let refreshed = initial.version + 1;
    let plan = plan_stats_meta_version_refresh(&mut store, &catalog, table_id, refreshed)
        .expect("the slow-save metadata refresh plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    assert_eq!(
        loader.load_meta(&mut store, table_id).expect("meta loads"),
        Some((
            refreshed,
            initial.snapshot,
            initial.modify_count,
            initial.row_count,
            refreshed,
        ))
    );
}

#[test]
fn stats_delta_updates_match_go_positive_negative_and_locked_rows() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let initial = plan_loaded_stats_meta_write(&mut store, &catalog, table_id, 10, 2, 100, now())
        .expect("initial meta plans");
    apply_mutations(&mut store, &initial.mutations);

    let positive = plan_stats_delta_updates(
        &mut store,
        &catalog,
        &[DeltaUpdate {
            table_id,
            delta: TableDelta {
                delta: 3,
                count: 4,
                init_time: None,
            },
            is_locked: false,
        }],
        101,
        now(),
    )
    .expect("positive delta plans");
    apply_mutations(&mut store, &positive.mutations);
    let negative = plan_stats_delta_updates(
        &mut store,
        &catalog,
        &[DeltaUpdate {
            table_id,
            delta: TableDelta {
                delta: -20,
                count: 5,
                init_time: None,
            },
            is_locked: false,
        }],
        102,
        now(),
    )
    .expect("negative delta plans");
    apply_mutations(&mut store, &negative.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    assert_eq!(
        loader.load_meta(&mut store, table_id).expect("meta loads"),
        Some((102, 0, 11, 0, 100))
    );

    let locked_id = 4343;
    let locked = plan_stats_delta_updates(
        &mut store,
        &catalog,
        &[DeltaUpdate {
            table_id: locked_id,
            delta: TableDelta {
                delta: -2,
                count: 3,
                init_time: None,
            },
            is_locked: true,
        }],
        103,
        now(),
    )
    .expect("locked delta plans");
    apply_mutations(&mut store, &locked.mutations);
    let locked_table = catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == "mysql")
        .and_then(|database| {
            database
                .tables
                .iter()
                .find(|table| table.name.lowercase() == "stats_table_locked")
        })
        .expect("stats_table_locked exists");
    let view = SystemTableView::project(
        "mysql.stats_table_locked",
        locked_table,
        &["table_id", "version", "modify_count", "count"],
    );
    let row = scan_system_table(&mut store, &view)
        .expect("locked rows scan")
        .into_iter()
        .map(|(key, value)| SystemRow::parse(&view, &key, &value).expect("lock row decodes"))
        .find(|row| row.i64("table_id").unwrap() == Some(locked_id))
        .expect("lock row exists");
    assert_eq!(row.u64("version").unwrap(), Some(103));
    assert_eq!(row.i64("modify_count").unwrap(), Some(3));
    assert_eq!(row.i64("count").unwrap(), Some(-2));
}

#[test]
fn stats_delta_update_keeps_go_row_delta_when_modify_count_is_zero() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4244;
    let initial = plan_loaded_stats_meta_write(&mut store, &catalog, table_id, 10, 2, 100, now())
        .expect("initial meta plans");
    apply_mutations(&mut store, &initial.mutations);

    let plan = plan_stats_delta_updates(
        &mut store,
        &catalog,
        &[DeltaUpdate {
            table_id,
            delta: TableDelta {
                delta: 3,
                count: 0,
                init_time: None,
            },
            is_locked: false,
        }],
        101,
        now(),
    )
    .expect("row-only delta plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    assert_eq!(
        loader.load_meta(&mut store, table_id).expect("meta loads"),
        Some((101, 0, 2, 13, 100))
    );
}

#[test]
fn update_stats_version_refreshes_meta_and_histograms_like_go() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    for table_id in [4245, 4246] {
        let stats = ClusterTableStats {
            table_id,
            version: 100,
            snapshot: 90,
            last_analyze_version: 100,
            last_stats_hist_version: 100,
            modify_count: 2,
            row_count: 10,
            columns: vec![full_histogram(1, false)],
            indexes: vec![],
        };
        let plan = plan_stats_write(&mut store, &catalog, &stats, now()).expect("stats plan");
        apply_mutations(&mut store, &plan.mutations);
    }

    let plan = plan_update_stats_version(&mut store, &catalog, 200)
        .expect("the two Go update statements plan");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let column_types = BTreeMap::from([(
        1,
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
    )]);
    for table_id in [4245, 4246] {
        let loaded = loader
            .load_table(&mut store, table_id, &column_types)
            .expect("stats load")
            .expect("stats exist");
        assert_eq!(loaded.version, 200);
        assert_eq!(loaded.columns[0].histogram.last_update_version, 200);
        assert_eq!(loaded.snapshot, 90);
    }
}

#[test]
fn change_global_stats_id_moves_exactly_the_six_go_tables() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let from = 4247;
    let to = 4248;
    let mut column = full_histogram(1, false);
    column.fm_sketch = Some(FmSketch::from_raw_parts(2, MAX_SKETCH_SIZE, [1, 2, 3]));
    let stats = ClusterTableStats {
        table_id: from,
        version: 100,
        snapshot: 90,
        last_analyze_version: 100,
        last_stats_hist_version: 100,
        modify_count: 2,
        row_count: 10,
        columns: vec![column],
        indexes: vec![],
    };
    let stats_plan = plan_partition_stats_write(&mut store, &catalog, &stats, now())
        .expect("partition stats plan");
    apply_mutations(&mut store, &stats_plan.mutations);
    let usage_plan = plan_loaded_stats_usage_write(
        &mut store,
        &catalog,
        from,
        &[Some(JsonPredicateColumn {
            id: 1,
            last_used_at: Some("2026-08-29 01:02:03.000000".to_owned()),
            last_analyzed_at: None,
        })],
        now(),
    )
    .expect("usage plan");
    apply_mutations(&mut store, &usage_plan.mutations);

    let plan = plan_change_global_stats_id(&mut store, &catalog, from, to)
        .expect("six Go table updates plan");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let column_types = BTreeMap::from([(
        1,
        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
    )]);
    assert!(loader
        .load_table_with_fm(&mut store, from, &column_types)
        .expect("old ID lookup")
        .is_none());
    let moved = loader
        .load_table_with_fm(&mut store, to, &column_types)
        .expect("new ID lookup")
        .expect("all statistics moved");
    assert_eq!(moved.columns[0].histogram.buckets.len(), 256);
    assert_eq!(moved.columns[0].topn.as_ref().map(TopN::num), Some(100));
    assert!(moved.columns[0].fm_sketch.is_some());
    assert_eq!(
        load_column_stats_usage_for_table(
            &mut store,
            &catalog,
            &tidb_datatype::SessionTimeZone::utc(),
            to,
        )
        .expect("moved usage loads")
        .len(),
        1
    );
    assert!(load_column_stats_usage_for_table(
        &mut store,
        &catalog,
        &tidb_datatype::SessionTimeZone::utc(),
        from,
    )
    .expect("old usage lookup")
    .is_empty());
}

/// Pinned Go `SaveColumnStatsUsageForTable` runs one transaction for the
/// complete slice and REPLACEs all four values, so an explicit nil timestamp
/// clears an older stored value instead of preserving it.
#[test]
fn loaded_stats_usage_replaces_timestamps_in_one_plan() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let first = plan_loaded_stats_usage_write(
        &mut store,
        &catalog,
        table_id,
        &[Some(JsonPredicateColumn {
            id: 7,
            last_used_at: Some("2026-08-29 01:02:03.123456".to_owned()),
            last_analyzed_at: Some("2026-08-28 04:05:06.000007".to_owned()),
        })],
        now(),
    )
    .expect("predicate usage plans");
    apply_mutations(&mut store, &first.mutations);

    let replacement = plan_loaded_stats_usage_write(
        &mut store,
        &catalog,
        table_id,
        &[
            None,
            Some(JsonPredicateColumn {
                id: 7,
                last_used_at: None,
                last_analyzed_at: Some("2026-08-30 08:09:10.000011".to_owned()),
            }),
        ],
        now(),
    )
    .expect("replacement usage plans");
    apply_mutations(&mut store, &replacement.mutations);

    let table = catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == "mysql")
        .and_then(|database| {
            database
                .tables
                .iter()
                .find(|table| table.name.lowercase() == "column_stats_usage")
        })
        .expect("column_stats_usage exists");
    let view = SystemTableView::project(
        "mysql.column_stats_usage",
        table,
        &["table_id", "column_id", "last_used_at", "last_analyzed_at"],
    );
    let rows = scan_system_table(&mut store, &view).expect("usage rows scan");
    let row = rows
        .iter()
        .map(|(key, value)| {
            let timezone = tidb_datatype::SessionTimeZone::utc();
            SystemRow::parse_in_timezone(&view, key, value, Some(&timezone))
                .expect("usage row decodes")
        })
        .find(|row| {
            row.i64("table_id").unwrap() == Some(table_id)
                && row.i64("column_id").unwrap() == Some(7)
        })
        .expect("replacement row exists");
    assert!(row.stored_datum("last_used_at").unwrap().unwrap().is_null());
    let expected = Time::from_date_checked(2026, 8, 30, 8, 9, 10, 0, TimeType::Timestamp, 0)
        .expect("a fixed calendar timestamp is valid");
    assert_eq!(
        row.datum("last_analyzed_at").unwrap(),
        Some(&Datum::Time(expected))
    );
}

#[test]
fn column_stats_usage_write_does_not_filter_the_table_item_kind() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let usage = HashMap::from([(
        tidb_model::TableItemID {
            table_id,
            id: 99,
            is_index: true,
            is_sync_load_failed: false,
        },
        ColumnStatsTimeInfo::default(),
    )]);
    let plan = plan_column_stats_usage_write(&mut store, &catalog, &usage, now())
        .expect("Go writes every supplied TableItemID");
    apply_mutations(&mut store, &plan.mutations);

    let loaded = load_column_stats_usage_for_table(
        &mut store,
        &catalog,
        &tidb_datatype::SessionTimeZone::utc(),
        table_id,
    )
    .expect("usage reloads");
    assert!(loaded.keys().any(|item| item.id == 99));
}

fn usage_item(table_id: i64, column_id: i64) -> tidb_model::TableItemID {
    tidb_model::TableItemID {
        table_id,
        id: column_id,
        is_index: false,
        is_sync_load_failed: false,
    }
}

fn dump_usage(store: &mut MetaStore, item: tidb_model::TableItemID, used_at: Time) {
    let catalog = load_cluster_catalog(&mut *store).expect("the bootstrapped catalog loads");
    let plan = plan_column_stats_usage_dump(store, &catalog, &[(item, used_at)], now())
        .expect("column usage dump plans");
    apply_mutations(store, &plan.mutations);
}

#[test]
fn predicate_usage_first_touch_creates_row() {
    let mut store = bootstrapped();
    let item = usage_item(4243, 11);
    dump_usage(&mut store, item, now());
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let usage = load_column_stats_usage_for_table(
        &mut store,
        &catalog,
        &tidb_datatype::SessionTimeZone::utc(),
        item.table_id,
    )
    .expect("usage reloads");
    assert_eq!(usage[&item].last_used_at, Some(now()));
}

#[test]
fn predicate_usage_no_bump_within_throttle() {
    let mut store = bootstrapped();
    let item = usage_item(4244, 12);
    let first = Time::from_date_checked(2026, 7, 29, 6, 0, 0, 0, TimeType::Timestamp, 0)
        .expect("fixed timestamp");
    let shortly_after = Time::from_date_checked(2026, 7, 29, 6, 10, 0, 0, TimeType::Timestamp, 0)
        .expect("fixed timestamp");
    dump_usage(&mut store, item, first);
    dump_usage(&mut store, item, shortly_after);
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let usage = load_column_stats_usage_for_table(
        &mut store,
        &catalog,
        &tidb_datatype::SessionTimeZone::utc(),
        item.table_id,
    )
    .expect("usage reloads");
    assert_eq!(usage[&item].last_used_at, Some(first));
}

#[test]
fn predicate_usage_bump_after_old_stored_value() {
    let mut store = bootstrapped();
    let item = usage_item(4245, 13);
    let old = Time::from_date_checked(2000, 1, 1, 0, 0, 0, 0, TimeType::Timestamp, 0)
        .expect("fixed timestamp");
    dump_usage(&mut store, item, old);
    dump_usage(&mut store, item, now());
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let usage = load_column_stats_usage_for_table(
        &mut store,
        &catalog,
        &tidb_datatype::SessionTimeZone::utc(),
        item.table_id,
    )
    .expect("usage reloads");
    assert_eq!(usage[&item].last_used_at, Some(now()));
}

/// Pinned predicatecolumn reads project UTC instants into the requested
/// location, while `GetPredicateColumns` deletes dropped-column rows and
/// returns only non-NULL `last_used_at` IDs from that same transaction.
#[test]
fn predicate_column_load_and_cleanup_match_the_pinned_storage_contract() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let plan = plan_loaded_stats_usage_write(
        &mut store,
        &catalog,
        table_id,
        &[
            Some(JsonPredicateColumn {
                id: 7,
                last_used_at: Some("2026-08-29 01:02:03".to_owned()),
                last_analyzed_at: None,
            }),
            Some(JsonPredicateColumn {
                id: 8,
                last_used_at: Some("2026-08-29 02:03:04".to_owned()),
                last_analyzed_at: None,
            }),
        ],
        now(),
    )
    .expect("usage plans");
    apply_mutations(&mut store, &plan.mutations);

    let plus_eight = tidb_datatype::SessionTimeZone::Fixed {
        name: "+08:00".to_owned(),
        offset_secs: 8 * 60 * 60,
    };
    let usage =
        load_column_stats_usage(&mut store, &catalog, &plus_eight).expect("all usage loads");
    let item = |id| tidb_model::TableItemID {
        table_id,
        id,
        is_index: false,
        is_sync_load_failed: false,
    };
    assert_eq!(
        usage[&item(7)].last_used_at,
        Some(
            Time::from_date_checked(2026, 8, 29, 9, 2, 3, 0, TimeType::Timestamp, 0)
                .expect("fixed timestamp")
        )
    );

    let (columns, cleanup) = plan_get_predicate_columns(&mut store, &catalog, table_id, &[7])
        .expect("cleanup and predicate read plan");
    assert_eq!(columns, vec![7]);
    assert!(!cleanup.is_empty());
    apply_mutations(&mut store, &cleanup.mutations);
    let remaining = load_column_stats_usage_for_table(
        &mut store,
        &catalog,
        &tidb_datatype::SessionTimeZone::utc(),
        table_id,
    )
    .expect("table usage reloads");
    assert!(remaining.contains_key(&item(7)));
    assert!(!remaining.contains_key(&item(8)));
}

/// Pinned Go history recording selects the current meta row by both table ID
/// and exact version before replacing `(table_id, version)` history.
#[test]
fn loaded_stats_history_requires_the_exact_current_meta_version() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let version = 440_000_000_000_000_000;
    let meta = plan_loaded_stats_meta_write(&mut store, &catalog, table_id, 55, 3, version, now())
        .expect("LOAD STATS final meta plans");
    apply_mutations(&mut store, &meta.mutations);

    assert!(plan_historical_stats_meta_write(
        &mut store,
        &catalog,
        table_id,
        version - 1,
        "load stats",
        now(),
    )
    .is_err());
    let history = plan_historical_stats_meta_write(
        &mut store,
        &catalog,
        table_id,
        version,
        "load stats",
        now(),
    )
    .expect("the exact version plans history");
    apply_mutations(&mut store, &history.mutations);

    let table = catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == "mysql")
        .and_then(|database| {
            database
                .tables
                .iter()
                .find(|table| table.name.lowercase() == "stats_meta_history")
        })
        .expect("stats_meta_history exists");
    let view = SystemTableView::project(
        "mysql.stats_meta_history",
        table,
        &["table_id", "modify_count", "count", "version", "source"],
    );
    let rows = scan_system_table(&mut store, &view).expect("history rows scan");
    let timezone = tidb_datatype::SessionTimeZone::utc();
    let row = rows
        .iter()
        .map(|(key, value)| {
            SystemRow::parse_in_timezone(&view, key, value, Some(&timezone))
                .expect("history row decodes")
        })
        .find(|row| row.i64("table_id").unwrap() == Some(table_id))
        .expect("history row exists");
    assert_eq!(row.i64("modify_count").unwrap(), Some(3));
    assert_eq!(row.i64("count").unwrap(), Some(55));
    assert_eq!(row.u64("version").unwrap(), Some(version));
    assert_eq!(
        row.bytes("source").unwrap().as_deref(),
        Some(b"load stats".as_slice())
    );
}

/// Pinned `history.RecordHistoricalStatsToStorage` stores the gzip-framed
/// JSON blocks under `(table_id, version, seq_no)` and returns the dump's
/// statistics version.
#[test]
fn historical_stats_data_round_trips_through_stats_history() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let json = JsonTable {
        database_name: "test".to_owned(),
        table_name: "t".to_owned(),
        count: 55,
        modify_count: 3,
        version: 440_000_000_000_000_000,
        ..JsonTable::default()
    };
    let (version, plan) =
        plan_historical_stats_data_write(&mut store, &catalog, table_id, &json, now())
            .expect("historical statistics data plans");
    assert_eq!(version, json.version);
    apply_mutations(&mut store, &plan.mutations);

    let table = catalog
        .databases
        .iter()
        .find(|database| database.info.name.lowercase() == "mysql")
        .and_then(|database| {
            database
                .tables
                .iter()
                .find(|table| table.name.lowercase() == "stats_history")
        })
        .expect("stats_history exists");
    let view = SystemTableView::project(
        "mysql.stats_history",
        table,
        &["table_id", "stats_data", "seq_no", "version"],
    );
    let timezone = tidb_datatype::SessionTimeZone::utc();
    let mut blocks = scan_system_table(&mut store, &view)
        .expect("history rows scan")
        .into_iter()
        .map(|(key, value)| {
            SystemRow::parse_in_timezone(&view, &key, &value, Some(&timezone))
                .expect("history row decodes")
        })
        .filter(|row| row.i64("table_id").unwrap() == Some(table_id))
        .map(|row| {
            assert_eq!(row.u64("version").unwrap(), Some(version));
            (
                row.i64("seq_no").unwrap().expect("sequence number"),
                row.bytes("stats_data")
                    .unwrap()
                    .expect("compressed statistics block"),
            )
        })
        .collect::<Vec<_>>();
    blocks.sort_by_key(|(sequence, _)| *sequence);
    let restored = tidb_executor::load_stats::blocks_to_json_table(
        &blocks.into_iter().map(|(_, block)| block).collect::<Vec<_>>(),
    )
    .expect("historical statistics JSON restores");
    assert_eq!(restored, json);
}

/// Pinned `TableHistoricalStatsToJSON` independently selects metadata and
/// payload versions. A later delta-flush meta row overlays its counts on the
/// newest older histogram dump and marks the reconstructed JSON historical.
#[test]
fn historical_stats_reader_selects_meta_and_data_versions_independently() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let data_version = 440_000_000_000_000_000;
    let data = JsonTable {
        database_name: "test".to_owned(),
        table_name: "t".to_owned(),
        count: 10,
        modify_count: 0,
        version: data_version,
        ..JsonTable::default()
    };
    let meta = plan_loaded_stats_meta_write(
        &mut store,
        &catalog,
        table_id,
        data.count,
        data.modify_count,
        data_version,
        now(),
    )
    .expect("initial metadata plans");
    apply_mutations(&mut store, &meta.mutations);
    let history_meta = plan_historical_stats_meta_write(
        &mut store,
        &catalog,
        table_id,
        data_version,
        "analyze",
        now(),
    )
    .expect("initial metadata history plans");
    apply_mutations(&mut store, &history_meta.mutations);
    let (_, history_data) =
        plan_historical_stats_data_write(&mut store, &catalog, table_id, &data, now())
            .expect("initial data history plans");
    apply_mutations(&mut store, &history_data.mutations);

    let meta_version = data_version + 10;
    let meta = plan_loaded_stats_meta_write(
        &mut store,
        &catalog,
        table_id,
        17,
        7,
        meta_version,
        now(),
    )
    .expect("newer metadata plans");
    apply_mutations(&mut store, &meta.mutations);
    let history_meta = plan_historical_stats_meta_write(
        &mut store,
        &catalog,
        table_id,
        meta_version,
        "flush stats",
        now(),
    )
    .expect("newer metadata history plans");
    apply_mutations(&mut store, &history_meta.mutations);

    let restored = table_historical_stats_to_json(
        &mut store,
        &catalog,
        table_id,
        meta_version,
    )
    .expect("historical statistics read succeeds")
    .expect("historical statistics exist");
    assert_eq!(restored.version, data_version);
    assert_eq!(restored.count, 17);
    assert_eq!(restored.modify_count, 7);
    assert!(restored.is_historical_stats);
    assert_eq!(
        table_historical_stats_to_json(
            &mut store,
            &catalog,
            table_id,
            data_version - 1,
        )
        .expect("an older snapshot is valid"),
        None
    );
}

/// Pinned `ClearOutdatedHistoryStats` selects expiry from metadata, then
/// removes metadata and payload rows through separate bounded statements.
#[test]
fn outdated_historical_stats_deletes_only_rows_at_or_before_the_cutoff() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4243;
    let cutoff = now();
    let newer = Time::from_date_checked(2027, 7, 29, 6, 12, 55, 0, TimeType::Timestamp, 0)
        .expect("a fixed calendar date is a valid timestamp");

    for (version, create_time) in [(1, cutoff), (2, newer)] {
        let meta = plan_loaded_stats_meta_write(
            &mut store,
            &catalog,
            table_id,
            version,
            0,
            version as u64,
            create_time,
        )
        .expect("metadata plans");
        apply_mutations(&mut store, &meta.mutations);
        let history_meta = plan_historical_stats_meta_write(
            &mut store,
            &catalog,
            table_id,
            version as u64,
            "analyze",
            create_time,
        )
        .expect("metadata history plans");
        apply_mutations(&mut store, &history_meta.mutations);
        let json = JsonTable {
            database_name: "test".to_owned(),
            table_name: "t".to_owned(),
            version: version as u64,
            count: version,
            ..JsonTable::default()
        };
        let (_, history_data) =
            plan_historical_stats_data_write(&mut store, &catalog, table_id, &json, create_time)
                .expect("data history plans");
        apply_mutations(&mut store, &history_data.mutations);
    }

    assert_eq!(
        count_outdated_historical_stats(&mut store, &catalog, cutoff)
            .expect("outdated metadata count succeeds"),
        1
    );
    for deletion in [
        plan_outdated_historical_meta_delete(&mut store, &catalog, cutoff, 1),
        plan_outdated_historical_data_delete(&mut store, &catalog, cutoff, 1),
    ] {
        let deletion = deletion.expect("bounded historical deletion plans");
        apply_mutations(&mut store, &deletion.mutations);
    }
    assert_eq!(
        count_outdated_historical_stats(&mut store, &catalog, cutoff)
            .expect("outdated metadata count succeeds"),
        0
    );
    let restored = table_historical_stats_to_json(&mut store, &catalog, table_id, u64::MAX)
        .expect("newer historical statistics read succeeds")
        .expect("newer historical statistics remain");
    assert_eq!(restored.version, 2);

    for deletion in [
        plan_historical_stats_data_delete_for_table(&mut store, &catalog, table_id),
        plan_historical_stats_meta_delete_for_table(&mut store, &catalog, table_id),
    ] {
        let deletion = deletion.expect("dropped-table historical deletion plans");
        apply_mutations(&mut store, &deletion.mutations);
    }
    assert_eq!(
        table_historical_stats_to_json(&mut store, &catalog, table_id, u64::MAX)
            .expect("a deleted table has no historical read error"),
        None
    );
}

/// Go `TableStatsToJSON(..., snapshot=0)` loads histogram payload first, then
/// refreshes stats meta and predicate usage in a second restricted
/// transaction. A metadata change between those reads must therefore appear
/// in the dump without replacing the already-loaded histogram payload.
#[test]
fn live_stats_dump_refreshes_meta_after_loading_payload() {
    let mut store = bootstrapped();
    let catalog = load_cluster_catalog(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let initial = ClusterTableStats {
        table_id,
        version: 440_000_000_000_000_000,
        snapshot: 439_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: vec![full_histogram(1, false)],
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &initial, now()).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);
    let table_info = TableInfo {
        id: table_id,
        name: CiString::new("t"),
        columns: vec![ColumnInfo {
            id: 1,
            name: CiString::new("a"),
            field_type: tidb_datatype::FieldType::new(
                tidb_datatype::FieldTypeCode::LongLong,
            ),
            state: SchemaState::PUBLIC,
            ..ColumnInfo::default()
        }]
        .into(),
        ..TableInfo::default()
    };
    let payload = load_table_stats_payload(&mut store, &catalog, &table_info, table_id)
        .expect("payload loads")
        .expect("stats exist");

    let refreshed_version = initial.version + 1;
    let plan = plan_loaded_stats_meta_write(
        &mut store,
        &catalog,
        table_id,
        55,
        3,
        refreshed_version,
        now(),
    )
    .expect("newer metadata plans");
    apply_mutations(&mut store, &plan.mutations);

    let json = table_stats_to_json_from_loaded(
        &mut store,
        &catalog,
        "test",
        &table_info,
        table_id,
        payload,
    )
    .expect("dump renders")
    .expect("stats exist");
    assert_eq!(json.version, refreshed_version);
    assert_eq!(json.count, 55);
    assert_eq!(json.modify_count, 3);
    let columns = json.columns.expect("column map");
    assert_eq!(columns.len(), 1, "the first read's payload remains");
    assert_eq!(
        columns["a"]
            .as_ref()
            .expect("column dump")
            .histogram
            .as_ref()
            .expect("histogram dump")
            .buckets
            .as_ref()
            .expect("bucket dump")
            .len(),
        256
    );
}
