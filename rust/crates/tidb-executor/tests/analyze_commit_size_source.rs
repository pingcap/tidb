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

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};

use tidb_ast::CiString;
use tidb_datatype::{Datum, Time, TimeType};
use tidb_infoschema::cluster_catalog::{
    load_info_schema, ClusterCatalogError, MetaPairs, MetaSnapshot,
};
use tidb_stats_handle_usage_predicatecolumn::cluster_predicate_column::ColStatsTimeInfo;
use tidb_stats_handle_usage_predicatecolumn::cluster_predicate_column::load_column_stats_usage_for_table;
use tidb_stats_handle_storage::cluster_stats_dump::{
    load_table_stats_payload, table_historical_stats_to_json, table_stats_to_json_from_loaded,
};
use tidb_stats_handle_storage::cluster_stats_load::{
    ClusterStatsItem, ClusterStatsLoader, ClusterTableStats, ColumnLength, TableRowCount,
    TableSizeStats,
};
use tidb_stats_handle_storage::cluster_stats_write::{insert_table_stats_statements, plan_column_stats_usage_write, plan_insert_table_stats_statement, plan_partition_stats_write, plan_stats_write, save_buckets_sql, save_topn_sql, InsertTableStatsStatement};
use tidb_stats_handle_history::history_stats::{historical_stats_data_blocks, plan_historical_stats_data_block, plan_historical_stats_meta_lock, plan_historical_stats_meta_replace};
use tidb_syssession::stats_table_rows::{locate, set, StatsRows, StatsWriteError, StatsWritePlan};
use tidb_stats_handle_usage::session_stats_collect::plan_stats_delta_statement;
use tidb_stats_handle_usage::session_stats_collect::stats_delta_statements;
use tidb_stats_handle_usage::session_stats_collect::StatsDeltaStatement;
use tidb_stats_handle_autoanalyze::analyze_jobs::plan_delete_analyze_jobs;
use tidb_stats_handle_autoanalyze::analyze_jobs::plan_finish_analyze_job;
use tidb_stats_handle_autoanalyze::analyze_jobs::plan_update_analyze_job_progress;
use tidb_stats_handle_autoanalyze::analyze_jobs::plan_start_analyze_job;
use tidb_stats_handle_autoanalyze::analyze_jobs::plan_insert_analyze_job;
use tidb_session::mysql_bootstrap::{plan_mysql_bootstrap, BootstrapEnvironment, BootstrapWrite};
use tidb_syssession::mysql_system_tables::{
    scan_system_table, SystemRow, SystemTableError, SystemTableView,
};
use tidb_stats_handle_syncload::real_tikv_stats::{StatsTarget};
use tidb_stats_handle::bootstrap::{init_stats_lite, load_initial_stats_snapshot_with_memory_limits, InitialStatsLoad};
use tidb_model::column::ColumnInfo;
use tidb_model::index::IndexInfo;
use tidb_model::table_info::TableInfo;
use tidb_model::SchemaState;
use tidb_stats::cmsketch::{CmsSketch, TopN};
use tidb_stats::histogram::{Bucket, Histogram};
use tidb_stats::{FmSketch, MAX_SKETCH_SIZE};
use tidb_stats_util::JsonTable;
use tidb_stats_handle_cache::{StatsTableRowCache, StatsTableRowSizeSource, TableHistId};
use tidb_stats_handle_usage::DeltaUpdate;
use tidb_variable::table_delta::TableDelta;
use tidb_txnkv::transaction::BufferMutationOp;

#[derive(Default)]
struct MetaStore {
    pairs: BTreeMap<Vec<u8>, Vec<u8>>,
    scans: Vec<Vec<u8>>,
    fail_scan_prefix: Option<Vec<u8>>,
    /// The node's row-ID allocators Go's internal INSERTs draw from; each
    /// reservation commits on its own, outside the planned write set.
    row_ids: std::sync::Arc<tidb_table::table_from_meta::LocalTableAutoIds>,
}

impl MetaSnapshot for MetaStore {
    fn get(&mut self, raw_key: &[u8]) -> Result<Option<Vec<u8>>, ClusterCatalogError> {
        Ok(self.pairs.get(raw_key).cloned())
    }

    fn scan_prefix(&mut self, prefix: &[u8]) -> Result<MetaPairs, ClusterCatalogError> {
        self.scans.push(prefix.to_vec());
        if self.fail_scan_prefix.as_deref() == Some(prefix) {
            return Err(ClusterCatalogError::Snapshot(
                "injected statistics scan failure".to_owned(),
            ));
        }
        Ok(self
            .pairs
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }
}

fn apply_mutations(store: &mut MetaStore, mutations: &[tidb_txnkv::transaction::BufferMutation]) {
    for mutation in mutations {
        match mutation.kind() {
            BufferMutationOp::Lock => {}
            BufferMutationOp::Delete => {
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

fn apply_historical_stats_meta_statements(
    store: &mut MetaStore,
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    table_id: i64,
    version: u64,
    source: &str,
    now: Time,
) -> Result<(), StatsWriteError> {
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let ((modify_count, count), lock) =
        plan_historical_stats_meta_lock(store, catalog, table_id, version)?;
    apply_mutations(store, &lock.mutations);
    let replace = plan_historical_stats_meta_replace(
        store,
        catalog,
        table_id,
        modify_count,
        count,
        version,
        source,
        now,
        Some(&*row_ids),
    )?;
    apply_mutations(store, &replace.mutations);
    Ok(())
}

fn apply_historical_stats_data_statements(
    store: &mut MetaStore,
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    table_id: i64,
    json: &JsonTable,
    now: Time,
) -> Result<u64, StatsWriteError> {
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let (version, blocks) = historical_stats_data_blocks(json)?;
    for (sequence, block) in blocks.iter().enumerate() {
        let plan = plan_historical_stats_data_block(
            store, catalog, table_id, version, sequence, block, now,
            Some(&*row_ids),
        )?;
        apply_mutations(store, &plan.mutations);
    }
    Ok(version)
}

fn apply_stats_delta_statements(
    store: &mut MetaStore,
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    updates: &[DeltaUpdate],
    version: u64,
    now: Time,
) -> Result<(), StatsWriteError> {
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    for statement in stats_delta_statements(updates) {
        let plan =
            plan_stats_delta_statement(store, catalog, &statement, version, now, Some(&*row_ids))?;
        apply_mutations(store, &plan.mutations);
    }
    Ok(())
}

/// Seeds `mysql.stats_meta` for the planners under test: the row's
/// version, counts and histogram version set, its other columns kept.
fn seed_stats_meta(
    store: &mut MetaStore,
    catalog: &tidb_infoschema::SessionExtendedInfoSchema,
    table_id: i64,
    count: i64,
    modify_count: i64,
    version: u64,
    now: Time,
    row_ids: Option<&dyn tidb_table::table_from_meta::TableAutoIds>,
) -> Result<StatsWritePlan, StatsWriteError> {
    let mut plan = StatsWritePlan::default();
    let table = locate(catalog, "stats_meta")?;
    let table = &*table.read();
    let mut rows = StatsRows::open(store, table, &["table_id"], table_id)?;
    let identity = vec![format!("{:?}", Datum::Int(table_id))];
    let mut values = match rows.existing_values(&identity) {
        Some(values) => values.clone(),
        None => tidb_syssession::system_row_write::defaults_row(table, now)?,
    };
    set(table, &mut values, "table_id", Datum::Int(table_id));
    set(table, &mut values, "version", Datum::UInt(version));
    set(table, &mut values, "count", Datum::Int(count));
    set(table, &mut values, "modify_count", Datum::Int(modify_count));
    set(table, &mut values, "last_stats_histograms_version", Datum::UInt(version));
    rows.store_row(catalog, &values, &mut plan, row_ids)?;
    Ok(plan)
}

fn apply(store: &mut MetaStore, write: &BootstrapWrite) {
    for mutation in &write.mutations {
        match mutation.kind() {
            BufferMutationOp::Delete => {
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

/// The id the bootstrap gave `mysql.<name>`, read by name from the catalog
/// (it is a reserved id under the next-gen kernel and an ordinary one under
/// the classic kernel).
fn system_table_id(catalog: &tidb_infoschema::SessionExtendedInfoSchema, name: &str) -> i64 {
    catalog
        .table_info_by_name("mysql", name)
        .unwrap_or_else(|| panic!("mysql.{name} exists"))
        .1
        .read().id
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

fn analyze_job_rows(store: &mut MetaStore) -> Vec<SystemRow<'static>> {
    let catalog = load_info_schema(store).expect("the bootstrapped catalog loads");
    let table = catalog
        .schema_info_by_name("mysql")
        .and_then(|database| {
            database
                .read().deprecated_tables
                .iter_deref()
                .find(|table| table.read().name.lowercase() == "analyze_jobs")
        })
        .expect("mysql.analyze_jobs exists")
        .read()
        .clone();
    let view = Box::leak(Box::new(SystemTableView::project(
        "mysql.analyze_jobs",
        Box::leak(Box::new(table)),
        &[
            "id",
            "table_schema",
            "table_name",
            "partition_name",
            "job_info",
            "processed_rows",
            "state",
            "process_id",
            "update_time",
        ],
    )));
    let timezone = Box::leak(Box::new(tidb_datatype::SessionTimeZone::utc()));
    scan_system_table(store, view)
        .expect("analyze jobs scan")
        .into_iter()
        .map(|(key, value)| {
            SystemRow::parse_in_timezone(view, &key, &value, Some(timezone)).unwrap()
        })
        .collect()
}

/// Pinned `InsertAnalyzeJob`, `StartAnalyzeJob`, `FinishAnalyzeJob`, and
/// `DeleteAnalyzeJobs` share one durable row and delete only timestamps
/// strictly older than the supplied cutoff.
#[test]
fn analyze_job_lifecycle_and_timestamp_cleanup_match_go() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let (job_id, insert) = plan_insert_analyze_job(
        &mut store,
        &catalog,
        "test",
        "t",
        "",
        &vec![b'x'; 70_000],
        "127.0.0.1:4000",
        77,
        now(),
        Some(&*row_ids),
    )
    .expect("pending job plans");
    assert_eq!(job_id, 1);
    apply_mutations(&mut store, &insert.mutations);
    let running_at =
        Time::from_date_checked(2026, 7, 29, 6, 13, 0, 0, TimeType::Timestamp, 0).unwrap();
    let catalog = load_info_schema(&mut store).unwrap();
    let start = plan_start_analyze_job(&mut store, &catalog, job_id, running_at, Some(&*row_ids)).unwrap();
    apply_mutations(&mut store, &start.mutations);
    let catalog = load_info_schema(&mut store).unwrap();
    let progress =
        plan_update_analyze_job_progress(&mut store, &catalog, job_id, 8, running_at, Some(&*row_ids))
            .unwrap();
    apply_mutations(&mut store, &progress.mutations);
    let finished_at =
        Time::from_date_checked(2026, 7, 29, 6, 13, 5, 0, TimeType::Timestamp, 0).unwrap();
    let catalog = load_info_schema(&mut store).unwrap();
    let finish =
        plan_finish_analyze_job(&mut store, &catalog, job_id, 12, None, finished_at, Some(&*row_ids)).unwrap();
    apply_mutations(&mut store, &finish.mutations);

    let rows = analyze_job_rows(&mut store);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].u64("id").unwrap(), Some(1));
    assert!(matches!(
        rows[0].datum("state").unwrap(),
        Some(Datum::Enum(value, _)) if value.name_bytes() == b"finished"
    ));
    assert_eq!(rows[0].i64("processed_rows").unwrap(), Some(20));
    assert_eq!(rows[0].datum("process_id").unwrap(), None);
    assert_eq!(rows[0].text("job_info").unwrap().unwrap().len(), 65_535);

    let catalog = load_info_schema(&mut store).unwrap();
    let keep = plan_delete_analyze_jobs(&mut store, &catalog, &finished_at).unwrap();
    assert!(keep.is_empty(), "equal update_time is retained");
    let after = Time::from_date_checked(2026, 7, 29, 6, 13, 6, 0, TimeType::Timestamp, 0).unwrap();
    let delete = plan_delete_analyze_jobs(&mut store, &catalog, &after).unwrap();
    apply_mutations(&mut store, &delete.mutations);
    assert!(analyze_job_rows(&mut store).is_empty());
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

#[test]
fn a_real_table_s_six_histograms_fit_one_analyze_transaction() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
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
    let plan = plan_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids))
        .expect("a full-sized analyze result plans");
    let planned = plan.mutations.len();
    // Every bucket and TopN row of the six 256-bucket, 100-value histograms
    // is in the one plan. A clustered statistics row is one mutation: Go
    // writes no entry for a clustered table's primary index.
    assert!(planned >= 6 * (256 + 100), "{planned}");
    let bytes: usize = plan
        .mutations
        .iter()
        .map(|mutation| mutation.key().len() + mutation.value().len())
        .sum();
    assert!(bytes as u64 <= tidb_txnkv::txn_total_size_limit());
}

/// The analyze snapshot and `last_stats_histograms_version` columns round-trip:
/// Go stores `AnalyzeResults.Snapshot` beside the write version, and stamps
/// the histogram version with the same analyze start TS. The loader must keep
/// all three identities distinct even though this fixture gives them one TSO.
#[test]
fn the_analyze_version_round_trips_through_the_stored_row() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
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
        plan_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids)).expect("a small analyze plans");
    apply_mutations(&mut store, &plan.mutations);

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    assert_eq!(
        loader
            .stats_meta_count_and_modify_count(&mut store, 4242)
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

/// Go `TableSizeStats` issues one restricted read against `stats_meta` and one
/// against non-index `stats_histograms`. The cluster adapter must read those
/// durable rows directly, deduplicate its `IN` IDs, and never substitute the
/// separately loaded planner-statistics snapshot.
#[test]
fn table_size_stats_source_reads_requested_storage_rows() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    for (table_id, row_count, column_id) in [(4242, 10, 11), (5151, 20, 12)] {
        let stats = ClusterTableStats {
            table_id,
            version: 440_000_000_000_000_000 + table_id as u64,
            snapshot: 440_000_000_000_000_000,
            last_analyze_version: 440_000_000_000_000_000,
            last_stats_hist_version: 440_000_000_000_000_000,
            modify_count: 0,
            row_count,
            columns: vec![full_histogram(column_id, false)],
            indexes: vec![full_histogram(21, true)],
        };
        let plan = plan_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids))
            .expect("statistics rows plan");
        apply_mutations(&mut store, &plan.mutations);
    }

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    store.scans.clear();
    assert_eq!(
        loader
            .load_table_row_counts(&mut store, &[5151, 4242, 5151])
            .expect("row counts load"),
        vec![
            TableRowCount {
                table_id: 4242,
                count: 10,
            },
            TableRowCount {
                table_id: 5151,
                count: 20,
            },
        ]
    );
    assert_eq!(
        loader
            .load_column_lengths(&mut store, &[5151, 4242, 5151])
            .expect("column lengths load"),
        vec![
            ColumnLength {
                table_id: 4242,
                histogram_id: 11,
                total_size: 40_000,
            },
            ColumnLength {
                table_id: 5151,
                histogram_id: 12,
                total_size: 40_000,
            },
        ],
        "index histogram rows are excluded"
    );
    assert_eq!(
        store.scans.len(),
        4,
        "two unique IDs produce two keyed scans for each restricted read"
    );
}

/// Go `cache.TableRowStatsCache.UpdateByID` copies both restricted reads into
/// the cache only when both succeed, and the information-schema reader then
/// warns over a failed refresh and serves the PREVIOUS cached values
/// (`executor/infoschema_reader.go:671` + `handle/cache/stats_table_row_cache.go`).
/// This pins that contract against the real encoded `mysql.stats_*` rows: a
/// failed refresh must leave stale values in place instead of zeroing them.
#[test]
fn the_table_row_size_cache_serves_previous_values_after_a_failed_refresh() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    for (table_id, row_count, column_id) in [(4242, 10, 11), (5151, 20, 12)] {
        let stats = ClusterTableStats {
            table_id,
            version: 440_000_000_000_000_000 + table_id as u64,
            snapshot: 440_000_000_000_000_000,
            last_analyze_version: 440_000_000_000_000_000,
            last_stats_hist_version: 440_000_000_000_000_000,
            modify_count: 0,
            row_count,
            columns: vec![full_histogram(column_id, false)],
            indexes: vec![full_histogram(21, true)],
        };
        let plan = plan_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids))
            .expect("statistics rows plan");
        apply_mutations(&mut store, &plan.mutations);
    }

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let cache = StatsTableRowCache::new();
    {
        let store = RefCell::new(&mut store);
        let source = LoaderSizeSource {
            loader: &loader,
            store: &store,
        };
        cache
            .update_by_id(&source, &[4242, 5151])
            .expect("the first refresh succeeds");
    }
    assert_eq!(cache.get_table_rows(4242), 10);
    assert_eq!(cache.get_table_rows(5151), 20);
    assert_eq!(cache.get_col_length((4242, 11)), 40_000);

    // Fail exactly the `stats_meta` scan the row-count read performs, the way
    // a lost restricted read fails in production.
    let meta_scan_prefix = {
        store.scans.clear();
        loader
            .load_table_row_counts(&mut store, &[4242])
            .expect("the probe read succeeds");
        let prefix = store
            .scans
            .first()
            .expect("the row-count read scans by prefix")
            .clone();
        store.scans.clear();
        prefix
    };
    store.fail_scan_prefix = Some(meta_scan_prefix);
    {
        let store = RefCell::new(&mut store);
        let source = LoaderSizeSource {
            loader: &loader,
            store: &store,
        };
        cache
            .update_by_id(&source, &[4242, 5151])
            .expect_err("the failed restricted read refuses the refresh");
    }
    assert_eq!(
        cache.get_table_rows(4242),
        10,
        "a failed refresh serves the previous cached row count, not zero"
    );
    assert_eq!(
        cache.get_table_rows(5151),
        20,
        "every cached table keeps its previous value across the failed refresh"
    );
    assert_eq!(
        cache.get_col_length((4242, 11)),
        40_000,
        "the column-length map is likewise untouched"
    );
}

/// The refresh source the size cache consumes over one loaded catalog: the
/// same two restricted reads Go's `UpdateByID` runs, here against the test's
/// own `MetaStore`.
struct LoaderSizeSource<'a> {
    loader: &'a ClusterStatsLoader,
    store: &'a RefCell<&'a mut MetaStore>,
}

impl StatsTableRowSizeSource for LoaderSizeSource<'_> {
    type Error = SystemTableError;

    fn read_row_counts(&self, ids: &[i64]) -> Result<Vec<(i64, u64)>, Self::Error> {
        Ok(self
            .loader
            .load_table_row_counts(&mut **self.store.borrow_mut(), ids)?
            .into_iter()
            .map(|row| (row.table_id, row.count))
            .collect())
    }

    fn read_column_lengths(&self, ids: &[i64]) -> Result<Vec<(TableHistId, u64)>, Self::Error> {
        Ok(self
            .loader
            .load_column_lengths(&mut **self.store.borrow_mut(), ids)?
            .into_iter()
            .map(|row| {
                let total_size =
                    u64::try_from(row.total_size.max(0)).expect("nonnegative i64 fits in u64");
                ((row.table_id, row.histogram_id), total_size)
            })
            .collect())
    }
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

/// Pinned Go `SaveAnalyzeResultToStorage` keeps FM sketches only for physical
/// partition results. Ordinary cache loads do not fetch them; the `loadAll`
/// path used by partition-to-global merge does, and a global write removes
/// any stale row instead of persisting a global sketch.
#[test]
fn partition_fm_sketch_round_trips_only_for_global_merge() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
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
    let plan = plan_partition_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids))
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
        plan_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids)).expect("global analyze plans");
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
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let expected = FmSketch::from_raw_parts(3, MAX_SKETCH_SIZE, [8, 16, 24]);
    let mut expected_cms = CmsSketch::new(2, 8);
    expected_cms.insert_bytes_by_count(b"x", 5);
    let mut column = full_histogram(1, false);
    column.fm_sketch = Some(expected.clone());
    let mut cms_item = full_histogram(2, false);
    cms_item.stats_ver = 1;
    cms_item.cms = Some(expected_cms);
    let stats = ClusterTableStats {
        table_id: 4242,
        version: 440_000_000_000_000_000,
        snapshot: 440_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: vec![column, cms_item],
        indexes: Vec::new(),
    };
    let plan = plan_partition_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids))
        .expect("partition analyze plans");
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
            tidb_tablecodec::table_key::encode_row_key(
                system_table_id(&catalog, "stats_histograms"),
                &column_kind,
            ),
            tidb_tablecodec::table_key::encode_row_key(
                system_table_id(&catalog, "stats_histograms"),
                &index_kind,
            ),
            tidb_tablecodec::table_key::encode_row_key(system_table_id(&catalog, "stats_fm_sketch"), &column_item,),
            tidb_tablecodec::table_key::encode_row_key(
                system_table_id(&catalog, "stats_fm_sketch"),
                &cms_item_prefix,
            ),
            tidb_tablecodec::table_key::encode_row_key(
                system_table_id(&catalog, "stats_histograms"),
                &cms_item_prefix,
            ),
            tidb_tablecodec::table_key::encode_row_key(
                system_table_id(&catalog, "stats_histograms"),
                &column_item,
            ),
            tidb_tablecodec::table_key::encode_row_key(system_table_id(&catalog, "stats_buckets"), &column_item,),
            tidb_tablecodec::table_key::encode_index_seek_key(
                system_table_id(&catalog, "stats_top_n"),
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
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
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
    let plan = plan_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids)).expect("analyze plans");
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

/// Complete observable inventory of pinned
/// `pkg/statistics/handle/handletest/initstats`: targeted/repeated/all-table
/// loads, stale dropped-table filtering, physical partition IDs, lite versus
/// non-lite payload shape, and completion through the highest physical ID.
#[test]
fn initial_stats_matches_go_table_scope_and_payload_shapes() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let version = 440_000_000_000_000_000;
    let current_ids = [4242, 4243, 4250, 4251];
    let dropped_id = 4999;

    for table_id in current_ids.into_iter().chain([dropped_id]) {
        let stats = ClusterTableStats {
            table_id,
            version,
            snapshot: version,
            last_analyze_version: version,
            last_stats_hist_version: version,
            modify_count: 0,
            row_count: 5,
            columns: vec![full_histogram(1, false)],
            indexes: vec![full_histogram(2, true)],
        };
        let plan =
            plan_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids)).expect("analyze plans");
        apply_mutations(&mut store, &plan.mutations);
    }

    let target = |table_id| {
        let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        StatsTarget {
            physical_id: table_id,
            table: TableInfo {
                id: table_id,
                columns: vec![ColumnInfo {
                    id: 1,
                    name: CiString::new("a"),
                    field_type: field_type.clone(),
                    state: SchemaState::PUBLIC,
                    ..ColumnInfo::default()
                }]
                .into(),
                indices: vec![IndexInfo {
                    id: 2,
                    name: CiString::new("idx_a"),
                    state: SchemaState::PUBLIC,
                    ..IndexInfo::default()
                }]
                .into(),
                ..TableInfo::default()
            },
            column_types: BTreeMap::from([(1, field_type)]),
        }
    };
    // 4250/4251 stand for the two physical partition definitions. The stale
    // dropped table deliberately has persisted stats but no current target.
    let targets = current_ids.map(target);
    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");

    let one = init_stats_lite(
        &mut store,
        &loader,
        &targets,
        &[4242],
        InitialStatsLoad::Lite,
    )
    .expect("one lite table loads");
    assert_eq!(one.keys().copied().collect::<Vec<_>>(), [4242]);

    let repeated = init_stats_lite(
        &mut store,
        &loader,
        &targets,
        &[4242, 4243],
        InitialStatsLoad::Lite,
    )
    .expect("an already loaded ID may be requested again");
    assert_eq!(repeated.keys().copied().collect::<Vec<_>>(), [4242, 4243]);

    let lite =
        init_stats_lite(&mut store, &loader, &targets, &[], InitialStatsLoad::Lite)
            .expect("all current physical tables load");
    assert_eq!(lite.keys().copied().collect::<Vec<_>>(), current_ids);
    assert!(!lite.contains_key(&dropped_id));
    for state in lite.values() {
        let table = state.loaded().expect("analyzed table is loaded");
        let column = table
            .hist_coll
            .get_column(1)
            .expect("column metadata exists");
        assert!(column.read().unwrap().is_all_evicted());
        let index = table.hist_coll.get_index(2).expect("index metadata exists");
        assert!(index.read().unwrap().is_all_evicted());
    }

    let non_lite = load_initial_stats_snapshot_with_memory_limits(
        &mut store,
        &loader,
        &targets,
        &[],
        InitialStatsLoad::IndexFull,
        u64::MAX,
        0,
    )
    .expect("non-lite initialization loads every current table");
    assert_eq!(non_lite.keys().next_back().copied(), Some(4251));
    for state in non_lite.values() {
        let table = state.loaded().expect("analyzed table is loaded");
        let column = table
            .hist_coll
            .get_column(1)
            .expect("column metadata exists");
        assert!(column.read().unwrap().is_all_evicted());
        let index = table.hist_coll.get_index(2).expect("index exists");
        assert!(index.read().unwrap().is_full_load());
    }

    let histogram_stage = loader
        .init_stats_histograms_concurrently(
            &mut store,
            targets[0].table.id,
            &targets[0].column_types,
        )
        .expect("histogram stage loads")
        .expect("stats_meta exists")
        .to_statistics_table(&targets[0].table);
    let topn_stage = loader
        .init_stats_top_n_concurrently(&mut store, targets[0].table.id, &targets[0].column_types)
        .expect("TopN stage loads")
        .expect("stats_meta exists")
        .to_statistics_table(&targets[0].table);
    let histogram_memory = histogram_stage.memory_usage().total_mem_usage;
    let topn_memory = topn_stage.memory_usage().total_mem_usage;
    assert!(topn_memory > histogram_memory);

    let memory_limited = load_initial_stats_snapshot_with_memory_limits(
        &mut store,
        &loader,
        &targets[..1],
        &[],
        InitialStatsLoad::IndexFull,
        u64::MAX,
        topn_memory,
    )
    .expect("memory-limited initialization completes");
    let table = memory_limited[&4242]
        .loaded()
        .expect("analyzed table is loaded");
    let index = table.hist_coll.get_index(2).expect("index exists");
    let index = index.read().unwrap();
    assert!(index
        .top_n
        .as_ref()
        .is_some_and(|topn| topn.total_count() > 0));
    assert!(index.histogram.buckets.is_empty());
    assert!(!index.is_full_load());
}

#[test]
fn initial_stats_handles_missing_histograms_and_topn_without_buckets() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let empty_id = 4260;
    let topn_only_id = 4261;
    let initial =
        seed_stats_meta(&mut store, &catalog, empty_id, 6, 6, 100, now(), Some(&*row_ids))
            .expect("metadata-only table plans");
    apply_mutations(&mut store, &initial.mutations);

    let mut topn_only = full_histogram(2, true);
    topn_only.histogram.buckets.clear();
    let stats = ClusterTableStats {
        table_id: topn_only_id,
        version: 101,
        snapshot: 101,
        last_analyze_version: 101,
        last_stats_hist_version: 101,
        modify_count: 0,
        row_count: 6,
        columns: Vec::new(),
        indexes: vec![topn_only],
    };
    let plan = plan_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids)).expect("TopN plans");
    apply_mutations(&mut store, &plan.mutations);

    let target = |table_id| StatsTarget {
        physical_id: table_id,
        table: TableInfo {
            id: table_id,
            columns: vec![ColumnInfo {
                id: 1,
                name: CiString::new("a"),
                field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
                state: SchemaState::PUBLIC,
                ..ColumnInfo::default()
            }]
            .into(),
            indices: vec![IndexInfo {
                id: 2,
                name: CiString::new("idx_a"),
                state: SchemaState::PUBLIC,
                ..IndexInfo::default()
            }]
            .into(),
            ..TableInfo::default()
        },
        column_types: BTreeMap::from([(
            1,
            tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
        )]),
    };
    let targets = [target(empty_id), target(topn_only_id)];
    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let loaded = load_initial_stats_snapshot_with_memory_limits(
        &mut store,
        &loader,
        &targets,
        &[],
        InitialStatsLoad::IndexFull,
        u64::MAX,
        0,
    )
    .expect("initial statistics load");

    let empty = loaded[&empty_id]
        .loaded()
        .expect("stats_meta makes the table non-pseudo");
    assert_eq!(empty.hist_coll.realtime_count, 6);
    assert_eq!(empty.hist_coll.index_count(), 0);
    assert_eq!(empty.hist_coll.column_count(), 0);
    let existence = empty
        .existence_map
        .as_ref()
        .expect("schema metadata has an existence map")
        .read()
        .unwrap();
    assert!(existence.has(1, false));
    assert!(!existence.has_analyzed(1, false));
    assert!(existence.has(2, true));
    assert!(!existence.has_analyzed(2, true));

    let topn_only = loaded[&topn_only_id]
        .loaded()
        .expect("analyzed table loads");
    let index = topn_only.hist_coll.get_index(2).expect("index loads");
    let index = index.read().unwrap();
    assert!(index.is_full_load());
    assert!(index.histogram.buckets.is_empty());
    assert_eq!(index.top_n.as_ref().expect("TopN loads").total_count(), 700);
}

/// Pinned Go `StatsCacheImpl.Update` does not evict an item merely because a
/// table refreshes. It preserves an unchanged resident histogram, and when a
/// newer histogram replaces a resident one it reloads that item in full.
#[test]
fn cache_update_preserves_and_refreshes_resident_histogram_payload() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
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
    let plan =
        plan_stats_write(&mut store, &catalog, &old_stats, now(), Some(&*row_ids)).expect("analyze plans");
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
            table_info.id,
            &table_info,
            &column_types,
            Some(resident.as_ref()),
            std::time::Duration::from_secs(3),
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
    let plan =
        plan_stats_write(&mut store, &catalog, &new_stats, now(), Some(&*row_ids)).expect("reanalyze plans");
    apply_mutations(&mut store, &plan.mutations);
    let refreshed = loader
        .load_statistics_table_for_update(
            &mut store,
            table_info.id,
            &table_info,
            &column_types,
            Some(resident.as_ref()),
            std::time::Duration::from_secs(3),
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
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
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
    let plan = plan_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids)).expect("analyze plans");
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
    let index_prefix = tidb_tablecodec::table_key::encode_index_seek_key(
        system_table_id(&catalog, "stats_top_n"),
        1,
        &encoded_prefix,
    );
    assert!(store.scans.contains(&index_prefix));
    assert!(!store.scans.contains(&tidb_tablecodec::table_key::gen_table_record_prefix(
        system_table_id(&catalog, "stats_top_n"),
    )));
}

/// Go `readStatsForOneItem` reads only the requested histogram item: one
/// clustered histogram prefix, one clustered bucket prefix, and one TopN
/// secondary-index prefix.
#[test]
fn one_item_load_uses_only_that_items_key_ranges() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
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
    let plan = plan_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids)).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);
    store.scans.clear();

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let field_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
    let loaded = loader
        .read_stats_for_one_item(
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
    let histogram_prefix = tidb_tablecodec::table_key::encode_row_key(
        system_table_id(&catalog, "stats_histograms"),
        &encoded_item,
    );
    let bucket_prefix =
        tidb_tablecodec::table_key::encode_row_key(system_table_id(&catalog, "stats_buckets"), &encoded_item);
    let topn_prefix = tidb_tablecodec::table_key::encode_index_seek_key(
        system_table_id(&catalog, "stats_top_n"),
        1,
        &encoded_item,
    );
    assert_eq!(
        store.scans,
        vec![histogram_prefix, bucket_prefix, topn_prefix]
    );
}

/// Go `ReadColumnDistributionStats` loads metadata, TopN, and buckets from
/// one caller-owned snapshot.  It reads TopN before buckets, returns no
/// partial column after either failure, and rejects a corrupt negative
/// `null_count` before touching either payload table.
#[test]
fn column_distribution_read_is_atomic_ordered_and_validated() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let stats = ClusterTableStats {
        table_id: 4242,
        version: 440_000_000_000_000_000,
        snapshot: 439_000_000_000_000_000,
        last_analyze_version: 440_000_000_000_000_000,
        last_stats_hist_version: 440_000_000_000_000_000,
        modify_count: 0,
        row_count: 10_240,
        columns: vec![full_histogram(1, false)],
        indexes: Vec::new(),
    };
    let plan = plan_stats_write(&mut store, &catalog, &stats, now(), Some(&*row_ids)).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);
    let table = TableInfo {
        id: stats.table_id,
        columns: vec![ColumnInfo {
            id: 1,
            name: CiString::new("b"),
            offset: 0,
            field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
            state: SchemaState::PUBLIC,
            ..ColumnInfo::default()
        }]
        .into(),
        ..TableInfo::default()
    };
    let column_type = table
        .columns
        .iter_deref()
        .next()
        .expect("the test table has one column")
        .read()
        .field_type
        .clone();
    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    let item = [Datum::Int(stats.table_id), Datum::Int(0), Datum::Int(1)];
    let encoded_item = tidb_codec::encode_key(&item).unwrap();
    let histogram_prefix = tidb_tablecodec::table_key::encode_row_key(
        system_table_id(&catalog, "stats_histograms"),
        &encoded_item,
    );
    let topn_prefix = tidb_tablecodec::table_key::encode_index_seek_key(
        system_table_id(&catalog, "stats_top_n"),
        1,
        &encoded_item,
    );
    let bucket_prefix =
        tidb_tablecodec::table_key::encode_row_key(system_table_id(&catalog, "stats_buckets"), &encoded_item);

    store.scans.clear();
    store.fail_scan_prefix = Some(topn_prefix.clone());
    let error = loader
        .load_column_distribution_stats(&mut store, stats.table_id, 1, &table, &column_type)
        .expect_err("a TopN read failure aborts the whole column");
    assert!(error
        .to_string()
        .contains("injected statistics scan failure"));
    assert_eq!(
        store.scans,
        vec![histogram_prefix.clone(), topn_prefix.clone()]
    );

    store.scans.clear();
    store.fail_scan_prefix = None;
    let loaded = loader
        .load_column_distribution_stats(&mut store, stats.table_id, 1, &table, &column_type)
        .expect("the complete distribution loads")
        .expect("the histogram row exists");
    assert_eq!(loaded.top_n.as_ref().map(TopN::num), Some(100));
    assert_eq!(loaded.histogram.buckets.len(), 256);
    assert!(loaded.stats_loaded_status.is_full_load());
    assert_eq!(
        store.scans,
        vec![histogram_prefix.clone(), topn_prefix, bucket_prefix]
    );

    let mut version_one = stats.clone();
    version_one.columns[0].stats_ver = tidb_stats::VERSION_1;
    let plan = plan_stats_write(&mut store, &catalog, &version_one, now(), Some(&*row_ids))
        .expect("the Analyze V1 fixture plans");
    apply_mutations(&mut store, &plan.mutations);
    store.scans.clear();
    let loaded = loader
        .load_column_distribution_stats(&mut store, stats.table_id, 1, &table, &column_type)
        .expect("Analyze V1 metadata loads")
        .expect("the histogram row exists");
    assert!(loaded.top_n.is_none());
    assert!(loaded.histogram.buckets.is_empty());
    assert!(loaded.stats_loaded_status.is_full_load());
    assert_eq!(store.scans, vec![histogram_prefix.clone()]);

    let mut corrupt = stats.clone();
    corrupt.columns[0].histogram.null_count = -1;
    let plan = plan_stats_write(&mut store, &catalog, &corrupt, now(), Some(&*row_ids))
        .expect("the stored-corruption fixture plans");
    apply_mutations(&mut store, &plan.mutations);
    store.scans.clear();
    let error = loader
        .load_column_distribution_stats(&mut store, stats.table_id, 1, &table, &column_type)
        .expect_err("negative null_count is rejected");
    assert!(error.to_string().contains("negative null count -1"));
    assert_eq!(store.scans, vec![histogram_prefix]);
}

#[test]
fn insert_table_stats_uses_go_statement_order_and_histogram_placeholders() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let physical_id = 4243;
    let table = TableInfo {
        id: physical_id,
        columns: vec![
            ColumnInfo {
                id: 11,
                name: CiString::new("a"),
                ..ColumnInfo::default()
            },
            ColumnInfo {
                id: 12,
                name: CiString::new("b"),
                ..ColumnInfo::default()
            },
        ]
        .into(),
        indices: vec![IndexInfo {
            id: 21,
            name: CiString::new("idx_a"),
            ..IndexInfo::default()
        }]
        .into(),
        ..TableInfo::default()
    };
    let statements = insert_table_stats_statements(&table, physical_id);
    assert_eq!(
        statements,
        vec![
            InsertTableStatsStatement::Meta { physical_id },
            InsertTableStatsStatement::Histogram {
                physical_id,
                is_index: false,
                hist_id: 11,
            },
            InsertTableStatsStatement::Histogram {
                physical_id,
                is_index: false,
                hist_id: 12,
            },
            InsertTableStatsStatement::Histogram {
                physical_id,
                is_index: true,
                hist_id: 21,
            },
        ]
    );

    let version = 440_000_000_000_000_001;
    for statement in &statements {
        let plan = plan_insert_table_stats_statement(
            &mut store,
            &catalog,
            statement,
            version,
            now(),
            Some(&*row_ids),
        )
        .expect("InsertTableStats2KV statement plans");
        apply_mutations(&mut store, &plan.mutations);
    }
    let loader = ClusterStatsLoader::locate(&catalog).expect("stats tables locate");
    let loaded = loader
        .load_table(&mut store, physical_id, &BTreeMap::new())
        .expect("placeholder statistics load")
        .expect("stats_meta exists");
    assert_eq!(loaded.version, version);
    assert_eq!(loaded.last_stats_hist_version, version);
    assert_eq!(
        loaded
            .columns
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        vec![11, 12]
    );
    assert_eq!(
        loaded
            .indexes
            .iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        vec![21]
    );

    for statement in &statements {
        let ignored = plan_insert_table_stats_statement(
            &mut store,
            &catalog,
            statement,
            version + 1,
            now(),
            Some(&*row_ids),
        )
        .expect("existing InsertTableStats2KV rows are ignored");
        assert!(ignored.mutations.is_empty());
    }
}

#[test]
fn stats_delta_updates_use_go_statement_order() {
    let updates = [
        DeltaUpdate {
            table_id: 1,
            delta: TableDelta {
                delta: -2,
                count: 3,
                init_time: None,
            },
            is_locked: true,
        },
        DeltaUpdate {
            table_id: 2,
            delta: TableDelta {
                delta: 4,
                count: 5,
                init_time: None,
            },
            is_locked: false,
        },
        DeltaUpdate {
            table_id: 3,
            delta: TableDelta {
                delta: -6,
                count: 7,
                init_time: None,
            },
            is_locked: false,
        },
    ];
    let statements = stats_delta_statements(&updates);
    assert!(matches!(
        statements.as_slice(),
        [
            StatsDeltaStatement::LockLocked(_),
            StatsDeltaStatement::LockUnlocked(_),
            StatsDeltaStatement::UpsertLocked(_),
            StatsDeltaStatement::UpsertUnlockedPositive(_),
            StatsDeltaStatement::UpsertUnlockedNegative(_),
        ]
    ));
}

#[test]
fn stats_delta_lock_statements_lock_only_selected_existing_rows() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let existing_id = 4241;
    let initial =
        seed_stats_meta(&mut store, &catalog, existing_id, 10, 2, 100, now(), Some(&*row_ids))
            .expect("initial meta plans");
    apply_mutations(&mut store, &initial.mutations);

    let plan = plan_stats_delta_statement(
        &mut store,
        &catalog,
        &StatsDeltaStatement::LockUnlocked(vec![existing_id, 999_999]),
        101,
        now(),
        Some(&*row_ids),
    )
    .expect("locking SELECT plans");
    assert_eq!(plan.mutations.len(), 1);
    assert_eq!(plan.mutations[0].kind(), BufferMutationOp::Lock);
}

#[test]
fn stats_delta_negative_multirow_insert_keeps_go_duplicate_key_order() {
    let mut store = bootstrapped();
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4240;
    apply_stats_delta_statements(
        &mut store,
        &catalog,
        &[
            DeltaUpdate {
                table_id,
                delta: TableDelta {
                    delta: -5,
                    count: 3,
                    init_time: None,
                },
                is_locked: false,
            },
            DeltaUpdate {
                table_id,
                delta: TableDelta {
                    delta: -3,
                    count: 4,
                    init_time: None,
                },
                is_locked: false,
            },
        ],
        101,
        now(),
    )
    .expect("negative multi-row statement plans");

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    assert_eq!(
        loader.stats_meta_count_and_modify_count(&mut store, table_id).expect("meta loads"),
        Some((101, 0, 7, 2, 0))
    );
}

#[test]
fn stats_delta_updates_match_go_positive_negative_and_locked_rows() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let initial =
        seed_stats_meta(&mut store, &catalog, table_id, 10, 2, 100, now(), Some(&*row_ids))
            .expect("initial meta plans");
    apply_mutations(&mut store, &initial.mutations);

    apply_stats_delta_statements(
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
    .expect("positive delta statements plan");
    apply_stats_delta_statements(
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
    .expect("negative delta statements plan");

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    assert_eq!(
        loader.stats_meta_count_and_modify_count(&mut store, table_id).expect("meta loads"),
        Some((102, 0, 11, 0, 100))
    );

    let locked_id = 4343;
    apply_stats_delta_statements(
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
    .expect("locked delta statements plan");
    let locked_table = catalog
        .schema_info_by_name("mysql")
        .and_then(|database| {
            database
                .read().deprecated_tables
                .iter_deref()
                .find(|table| table.read().name.lowercase() == "stats_table_locked")
        })
        .expect("stats_table_locked exists");
    let locked_table = locked_table.read();
    let view = SystemTableView::project(
        "mysql.stats_table_locked",
        &locked_table,
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
fn stats_delta_batch_assigns_one_transaction_version_to_every_table() {
    let mut store = bootstrapped();
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let updates = [4246, 4247].map(|table_id| DeltaUpdate {
        table_id,
        delta: TableDelta {
            delta: 3,
            count: 3,
            init_time: None,
        },
        is_locked: false,
    });
    apply_stats_delta_statements(&mut store, &catalog, &updates, 101, now())
        .expect("one dump batch plans");

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    for table_id in [4246, 4247] {
        assert_eq!(
            loader.stats_meta_count_and_modify_count(&mut store, table_id).expect("meta loads"),
            Some((101, 0, 3, 3, 0))
        );
    }
}

#[test]
fn stats_delta_update_keeps_go_row_delta_when_modify_count_is_zero() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4244;
    let initial =
        seed_stats_meta(&mut store, &catalog, table_id, 10, 2, 100, now(), Some(&*row_ids))
            .expect("initial meta plans");
    apply_mutations(&mut store, &initial.mutations);

    apply_stats_delta_statements(
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
    .expect("row-only delta statements plan");

    let loader = ClusterStatsLoader::locate(&catalog).expect("the stats tables locate");
    assert_eq!(
        loader.stats_meta_count_and_modify_count(&mut store, table_id).expect("meta loads"),
        Some((101, 0, 2, 13, 100))
    );
}

#[test]
fn column_stats_usage_write_does_not_filter_the_table_item_kind() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let usage = HashMap::from([(
        tidb_model::TableItemID {
            table_id,
            id: 99,
            is_index: true,
            is_sync_load_failed: false,
        },
        ColStatsTimeInfo::default(),
    )]);
    let plan = plan_column_stats_usage_write(&mut store, &catalog, &usage, now(), Some(&*row_ids))
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

/// Pinned predicatecolumn reads project UTC instants into the requested
/// location.
#[test]
fn predicate_column_load_matches_the_pinned_storage_contract() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let utc = |text: &str| {
        tidb_datatype::parse_time(
            text,
            TimeType::Timestamp,
            tidb_datatype::MAX_FSP,
            false,
            false,
            false,
            &chrono::Utc,
        )
        .expect("the UTC instant parses")
        .time
    };
    let usage = HashMap::from([
        (
            usage_item(table_id, 7),
            ColStatsTimeInfo {
                last_used_at: Some(utc("2026-08-29 01:02:03")),
                last_analyzed_at: None,
            },
        ),
        (
            usage_item(table_id, 8),
            ColStatsTimeInfo {
                last_used_at: Some(utc("2026-08-29 02:03:04")),
                last_analyzed_at: None,
            },
        ),
    ]);
    let plan = plan_column_stats_usage_write(&mut store, &catalog, &usage, now(), Some(&*row_ids))
        .expect("usage plans");
    apply_mutations(&mut store, &plan.mutations);

    let plus_eight = tidb_datatype::SessionTimeZone::Fixed {
        name: "+08:00".to_owned(),
        offset_secs: 8 * 60 * 60,
    };
    let usage =
        load_column_stats_usage_for_table(&mut store, &catalog, &plus_eight, table_id)
            .expect("table usage loads");
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
}

/// Both predicatecolumn SELECTs wrap timestamps in `CONVERT_TZ`. Pinned Go's
/// implementation returns SQL NULL for an incomplete date, so loading keeps
/// a NULL timestamp.
#[test]
fn predicate_column_invalid_zero_timestamp_matches_convert_tz_null() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4244;
    let item = usage_item(table_id, 10);
    let zero = Time::from_date_checked(0, 0, 0, 0, 0, 0, 0, TimeType::Timestamp, 0)
        .expect("zero timestamp");
    let usage = HashMap::from([(
        item,
        ColStatsTimeInfo {
            last_used_at: Some(zero),
            last_analyzed_at: Some(zero),
        },
    )]);
    let plan = plan_column_stats_usage_write(&mut store, &catalog, &usage, now(), Some(&*row_ids))
        .expect("predicate usage plans");
    apply_mutations(&mut store, &plan.mutations);

    let view = SystemTableView::locate(
        &catalog,
        "column_stats_usage",
        &["table_id", "column_id", "last_used_at", "last_analyzed_at"],
    )
    .expect("usage table locates");
    let stored = scan_system_table(&mut store, &view)
        .expect("usage rows scan")
        .into_iter()
        .map(|(key, value)| {
            let timezone = tidb_datatype::SessionTimeZone::utc();
            SystemRow::parse_in_timezone(&view, &key, &value, Some(&timezone))
                .expect("usage row decodes")
        })
        .find(|row| {
            row.i64("table_id").unwrap() == Some(table_id)
                && row.i64("column_id").unwrap() == Some(item.id)
        })
        .expect("usage row exists");
    assert!(stored
        .stored_datum("last_used_at")
        .unwrap()
        .unwrap()
        .is_null());
    assert!(stored
        .stored_datum("last_analyzed_at")
        .unwrap()
        .unwrap()
        .is_null());

    let loaded = load_column_stats_usage_for_table(
        &mut store,
        &catalog,
        &tidb_datatype::SessionTimeZone::utc(),
        table_id,
    )
    .expect("usage reloads");
    assert_eq!(loaded[&item], ColStatsTimeInfo::default());
}

/// Pinned Go history recording selects the current meta row by both table ID
/// and exact version before replacing `(table_id, version)` history.
#[test]
fn loaded_stats_history_requires_the_exact_current_meta_version() {
    let mut store = bootstrapped();
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let version = 440_000_000_000_000_000;
    let meta =
        seed_stats_meta(&mut store, &catalog, table_id, 55, 3, version, now(), Some(&*row_ids))
            .expect("LOAD STATS final meta plans");
    apply_mutations(&mut store, &meta.mutations);

    assert!(plan_historical_stats_meta_lock(&mut store, &catalog, table_id, version - 1).is_err());
    let ((modify_count, count), lock) =
        plan_historical_stats_meta_lock(&mut store, &catalog, table_id, version)
            .expect("the exact version plans the locking select");
    assert_eq!(
        lock.mutations.first().map(|mutation| mutation.kind()),
        Some(BufferMutationOp::Lock)
    );
    apply_mutations(&mut store, &lock.mutations);
    let replace = plan_historical_stats_meta_replace(
        &mut store,
        &catalog,
        table_id,
        modify_count,
        count,
        version,
        "load stats",
        now(),
        Some(&*row_ids),
    )
    .expect("the exact version plans the history replace");
    apply_mutations(&mut store, &replace.mutations);

    let table = catalog
        .schema_info_by_name("mysql")
        .and_then(|database| {
            database
                .read().deprecated_tables
                .iter_deref()
                .find(|table| table.read().name.lowercase() == "stats_meta_history")
        })
        .expect("stats_meta_history exists");
    let table = table.read();
    let view = SystemTableView::project(
        "mysql.stats_meta_history",
        &table,
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
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let table_id = 4242;
    let json = JsonTable {
        database_name: "test".to_owned(),
        table_name: "t".to_owned(),
        count: 55,
        modify_count: 3,
        version: 440_000_000_000_000_000,
        ..JsonTable::default()
    };
    let version =
        apply_historical_stats_data_statements(&mut store, &catalog, table_id, &json, now())
            .expect("historical statistics data statements plan");
    assert_eq!(version, json.version);

    let table = catalog
        .schema_info_by_name("mysql")
        .and_then(|database| {
            database
                .read().deprecated_tables
                .iter_deref()
                .find(|table| table.read().name.lowercase() == "stats_history")
        })
        .expect("stats_history exists");
    let table = table.read();
    let view = SystemTableView::project(
        "mysql.stats_history",
        &table,
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
    let restored = tidb_stats_handle_storage::json::blocks_to_json_table(
        &blocks
            .into_iter()
            .map(|(_, block)| block)
            .collect::<Vec<_>>(),
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
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
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
    let meta = seed_stats_meta(
        &mut store,
        &catalog,
        table_id,
        data.count,
        data.modify_count,
        data_version,
        now(),
        Some(&*row_ids),
    )
    .expect("initial metadata plans");
    apply_mutations(&mut store, &meta.mutations);
    apply_historical_stats_meta_statements(
        &mut store,
        &catalog,
        table_id,
        data_version,
        "analyze",
        now(),
    )
    .expect("initial metadata history statements plan");
    apply_historical_stats_data_statements(&mut store, &catalog, table_id, &data, now())
        .expect("initial data history statements plan");

    let meta_version = data_version + 10;
    let meta = seed_stats_meta(
        &mut store,
        &catalog,
        table_id,
        17,
        7,
        meta_version,
        now(),
        Some(&*row_ids),
    )
    .expect("newer metadata plans");
    apply_mutations(&mut store, &meta.mutations);
    apply_historical_stats_meta_statements(
        &mut store,
        &catalog,
        table_id,
        meta_version,
        "flush stats",
        now(),
    )
    .expect("newer metadata history statements plan");

    let restored = table_historical_stats_to_json(&mut store, &catalog, table_id, meta_version)
        .expect("historical statistics read succeeds")
        .expect("historical statistics exist");
    assert_eq!(restored.version, data_version);
    assert_eq!(restored.count, 17);
    assert_eq!(restored.modify_count, 7);
    assert!(restored.is_historical_stats);
    assert_eq!(
        table_historical_stats_to_json(&mut store, &catalog, table_id, data_version - 1,)
            .expect("an older snapshot is valid"),
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
    let row_ids = std::sync::Arc::clone(&store.row_ids);
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
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
    let plan =
        plan_stats_write(&mut store, &catalog, &initial, now(), Some(&*row_ids)).expect("analyze plans");
    apply_mutations(&mut store, &plan.mutations);
    let table_info = TableInfo {
        id: table_id,
        name: CiString::new("t"),
        columns: vec![ColumnInfo {
            id: 1,
            name: CiString::new("a"),
            field_type: tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
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
    let plan = seed_stats_meta(
        &mut store,
        &catalog,
        table_id,
        55,
        3,
        refreshed_version,
        now(),
        Some(&*row_ids),
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

/// Go's internal INSERT allocates `mysql.*` row ids from the node's live
/// allocator, whose reservation commits in its own transaction: a writer
/// given the node's allocators plans no allocator-key write of its own, so a
/// rolled-back statement never returns ids and concurrent writers never
/// conflict on that key.
#[test]
fn stats_writes_allocate_from_the_node_allocator_like_go() {
    let mut store = bootstrapped();
    let catalog = load_info_schema(&mut store).expect("the bootstrapped catalog loads");
    let allocators = tidb_table::table_from_meta::LocalTableAutoIds::default();
    let mut job_ids = Vec::new();
    for _ in 0..2 {
        let (job_id, insert) = {
            plan_insert_analyze_job(
                &mut store,
                &catalog,
                "test",
                "t",
                "",
                b"analyze table",
                "127.0.0.1:4000",
                77,
                now(),
                Some(&allocators),
            )
            .expect("pending job plans")
        };
        assert!(
            insert
                .mutations
                .iter()
                .all(|mutation| mutation.key().first() == Some(&b't')),
            "the plan writes only rows, never the allocator's meta key"
        );
        apply_mutations(&mut store, &insert.mutations);
        job_ids.push(job_id);
    }
    assert_eq!(job_ids, vec![1, 2]);
}
