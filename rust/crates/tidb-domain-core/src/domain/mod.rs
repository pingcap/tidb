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

//! Go `pkg/domain`'s `Domain` as this node builds it: the catalog, statistics,
//! privilege, binding and DDL owners, the background workers and the system
//! sessions they run internal SQL on. The connection's store half, which
//! sessions hold, is the parent module.

pub mod ddl_notifier;
mod ru_stats_writer;
pub mod workload_learning;
pub mod plan_replayer_owner;
mod telemetry;
pub mod schema_lease;
pub mod stats_maintenance;

use std::collections::HashSet;
use std::sync::{Arc, Condvar, Mutex, Weak};
use tidb_stats_handle_storage::stats_read_writer::StatsSessionPool as _;
use std::time::{Duration, SystemTime};

use chrono::{Datelike, Timelike, Utc};
use tidb_infoschema_issyncer::catalog_watch::InfoCache;
use tidb_infoschema::SessionExtendedInfoSchema;
use tidb_infoschema_issyncer::real_tikv_catalog::SnapshotMetaSnapshot;
use tidb_infoschema_isvalidator::SchemaValidator;
use tidb_store_driver_error::sql_query_error::SqlQueryError;
use tidb_table::table_from_meta::TableAutoIds;
use tidb_ddl::worker::ClusterDdl;
use tidb_domain::global_sysvars::GlobalSysvars;
use tidb_domain::historical_stats::{
    HistoricalStatsMetrics, HistoricalStatsWorker, InfoSchemaView, SessionInfoSchema,
    StatsHandle as HistoricalStatsHandle, TableMeta,
};
use tidb_stats_handle_syncload::stats_watch::SharedStats;
use tidb_sessmgr::ProcessRegistry;
use tidb_stats_handle_cache::table_size_stats::TableStorageStatsProvider;
use tidb_privilege::PrivilegeRegistry;
use tidb_stats_handle_cache::{StatsTableRowSizeSource, TableHistId};

use crate::bdr_role_store;
use crate::cluster_account_seam::ClusterAccountWriter;
use crate::cluster_session::{
    cluster_session_catalog, cluster_session_catalog_with_templates, SkippedTable,
};
use crate::cluster_sysvar_seam::ClusterSysvarWriter;
use crate::cluster_transactions::ClusterTransactions;
use crate::ddl_job_history_store;
use crate::sys_session::{stats_session_error, system_process_trackers};
use crate::util::{is_analyze_table_sql, sql_string_literal};

pub struct ClusterDataLockWaits {
    transactions: Arc<dyn ClusterTransactions>,
}

/// The node's store client for the TiFlash system tables.
pub struct ClusterTiFlashSystemTables {
    pub transactions: Arc<dyn ClusterTransactions>,
}

impl tidb_kv::tiflash_system_table::TiFlashSystemTableClient for ClusterTiFlashSystemTables {
    fn get_tiflash_system_table(&self, address: &str, sql: &str, timeout: Duration) -> Result<Vec<u8>, String> {
        self.transactions.get_tiflash_system_table(address, sql, timeout)
    }
}

pub struct ClusterStatisticsItemLoader {
    /// Go `statsHandle.SPool()`'s owner.
    pub domain: Weak<Domain>,
    pub catalog: Arc<InfoCache>,
    pub stats: Arc<SharedStats>,
    pub global_vars: GlobalSysvars,
}

/// The domain loading resources outlive each connection's schema catalog.
pub struct ClusterStatisticsLoading {
    pub loader: Arc<ClusterStatisticsItemLoader>,
    pub workers: Arc<tidb_stats_handle_syncload::sync_load::StatisticsLoadWorkers>,
    pub view: Arc<tidb_stats_handle::StatisticsView>,
}

impl ClusterStatisticsLoading {
    /// The statistics handle one session plans from: the domain's view, and
    /// the storage loader behind the synchronous-load workers.
    pub fn handle(&self) -> Arc<tidb_stats_handle::StatsHandle> {
        let handle = Arc::new(tidb_stats_handle::StatsHandle::default());
        handle.set_statistics_view(Arc::clone(&self.view));
        handle.set_item_loader(self.loader.clone(), Arc::clone(&self.workers));
        handle
    }
}

pub(crate) struct ClusterStatisticsSource(Arc<SharedStats>);

impl tidb_stats_handle::StatisticsSource for ClusterStatisticsSource {
    fn table(&self, physical_id: i64) -> Option<Arc<tidb_stats::Table>> {
        self.0
            .load()
            .get(&physical_id)
            .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
            .cloned()
    }

    fn get_physical_table_stats(
        &self,
        physical_id: i64,
        table: &tidb_stats::PseudoStatsTable,
    ) -> Option<Arc<tidb_stats::Table>> {
        self.0
            .get_physical_table_stats(physical_id, table.partitioned, table.cacheable, || {
                tidb_stats::pseudo_table(&table.info.read(), false, true)
            })
    }
}

pub struct ClusterColumnStatsUsageProvider {
    /// Go `statsHandle.SPool()`'s owner.
    pub domain: Weak<Domain>,
}

pub struct ClusterTableStorageStatsProvider {
    /// Go `ExecWithOpts`' restricted session: the domain's system sessions.
    pub domain: Weak<Domain>,
}

/// Go `TableRowStatsCache.UpdateByID`'s two restricted reads, each its own
/// statement on one of the domain's system sessions.
struct SessionSizeReads<'a> {
    pool: &'a dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
}

impl SessionSizeReads<'_> {
    fn rows(&self, sql: &str) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        let mut rows = Vec::new();
        self.pool
            .call_with_sctx(
                &mut |sctx| {
                    (rows, _) = tidb_stats_handle_util::exec_rows(sctx, sql, &[])?;
                    Ok(())
                },
                &[],
            )
            .map_err(|error| error.message)?;
        Ok(rows)
    }
}

/// Go `buildInTableIDsString`.
fn in_table_ids(ids: &[i64]) -> String {
    let ids: Vec<String> = ids.iter().map(i64::to_string).collect();
    format!("table_id in ({})", ids.join(","))
}

/// Go `row.GetInt64(i)`.
fn row_i64(row: &[tidb_datatype::Datum], index: usize) -> i64 {
    match row.get(index) {
        Some(tidb_datatype::Datum::Int(value)) => *value,
        Some(tidb_datatype::Datum::UInt(value)) => *value as i64,
        _ => 0,
    }
}

impl StatsTableRowSizeSource for SessionSizeReads<'_> {
    type Error = String;

    /// Go `getRowCountTables`.
    fn read_row_counts(&self, ids: &[i64]) -> Result<Vec<(i64, u64)>, Self::Error> {
        let sql = if ids.is_empty() {
            "select table_id, count from mysql.stats_meta".to_owned()
        } else {
            format!("select table_id, count from mysql.stats_meta where {}", in_table_ids(ids))
        };
        Ok(self
            .rows(&sql)?
            .iter()
            .map(|row| (row_i64(row, 0), row_i64(row, 1) as u64))
            .collect())
    }

    /// Go `getColLengthTables`.
    fn read_column_lengths(&self, ids: &[i64]) -> Result<Vec<(TableHistId, u64)>, Self::Error> {
        let sql = if ids.is_empty() {
            "select table_id, hist_id, tot_col_size from mysql.stats_histograms where is_index = 0"
                .to_owned()
        } else {
            format!(
                "select table_id, hist_id, tot_col_size from mysql.stats_histograms where is_index = 0 and {}",
                in_table_ids(ids)
            )
        };
        Ok(self
            .rows(&sql)?
            .iter()
            .map(|row| ((row_i64(row, 0), row_i64(row, 1)), row_i64(row, 2).max(0) as u64))
            .collect())
    }
}

impl TableStorageStatsProvider for ClusterTableStorageStatsProvider {
    fn load_table_row_stats(
        &self,
        physical_ids: &[i64],
    ) -> Result<tidb_stats_handle_cache::table_size_stats::TableRowStatsSnapshot, String> {
        let cache = &*tidb_stats_handle_cache::TABLE_ROW_STATS_CACHE;
        // Go `updateStatsCacheIfNeed` + `TableRowStatsCache.UpdateByID`
        // (`executor/infoschema_reader.go:671`): refresh the cache for every
        // physical ID the statement's schema holds first. A failed read only
        // warns -- the reader then serves the cache's previous values.
        let domain = self.domain.upgrade();
        let pool = domain.as_ref().map(|domain| domain.advanced_sys_session_pool());
        let source = SessionSizeReads {
            pool: &crate::sys_session::StatsPool {
                session_pool: pool.as_ref(),
            },
        };
        if let Err(error) = cache.update_by_id(&source, physical_ids) {
            // Go `updateStatsCacheIfNeed`.
            tidb_logutil::bg_logger().warn(
                "cannot update stats cache for tables",
                &[tidb_logutil::zap_error(error)],
            );
        }
        let (rows, lengths) = cache.snapshot();
        Ok(tidb_stats_handle_cache::table_size_stats::TableRowStatsSnapshot {
            table_rows: rows.into_iter().collect(),
            column_lengths: lengths.into_iter().collect(),
        })
    }
}

pub struct ClusterApproximateTableCountProvider {
    pub transactions: Arc<dyn ClusterTransactions>,
    pub cache: Arc<Mutex<tidb_pdhelper::ApproximateTableCountCache>>,
    pub cleanup_tx: std::sync::mpsc::SyncSender<bool>,
    pub cleanup_worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl ClusterApproximateTableCountProvider {
    pub(crate) fn new(transactions: Arc<dyn ClusterTransactions>) -> Self {
        let cache = Arc::new(Mutex::new(
            tidb_pdhelper::ApproximateTableCountCache::new(
                1_048_576,
                Duration::from_secs(30),
            ),
        ));
        let (cleanup_tx, cleanup_rx) = std::sync::mpsc::sync_channel(1);
        let cleanup_cache = Arc::clone(&cache);
        let cleanup_worker = std::thread::Builder::new()
            .name("pd-helper-cleanup".to_owned())
            .spawn(move || loop {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let wait = cleanup_cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .next_expiration_delay(now)
                    .max(Duration::from_micros(1));
                match cleanup_rx.recv_timeout(wait) {
                    Ok(true) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    Ok(false) => {}
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default();
                        cleanup_cache
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .delete_expired(now);
                    }
                }
            })
            .expect("PD helper cleanup worker must start");
        Self {
            transactions,
            cache,
            cleanup_tx,
            cleanup_worker: Mutex::new(Some(cleanup_worker)),
        }
    }
}

impl Drop for ClusterApproximateTableCountProvider {
    fn drop(&mut self) {
        let _ = self.cleanup_tx.send(true);
        if let Some(worker) = self
            .cleanup_worker
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = worker.join();
        }
    }
}

impl tidb_pdhelper::ApproximateTableCountProvider
    for ClusterApproximateTableCountProvider
{
    fn approximate_table_count(
        &self,
        resource_group: &str,
        physical_id: i64,
        database: &str,
        table: &str,
        partition: &str,
    ) -> (f64, bool) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let key = tidb_pdhelper::approximate_table_count_key(
            physical_id,
            database,
            table,
            partition,
        );
        if let Some(count) = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key, now)
        {
            return (count, true);
        }
        // Go's concurrent cache releases its lock before invoking PD or
        // restricted SQL, so concurrent misses may load the same key twice.
        let loaded = match self.transactions.record_region_stats(physical_id) {
            Ok(Some(stats)) if stats.count > 2 => (stats.storage_keys as f64, true),
            Ok(Some(_)) => {
                exact_physical_row_count(self.transactions.as_ref(), resource_group, physical_id)
                    .map_or((0.0, false), |count| (count as f64, true))
            }
            Ok(None) | Err(_) => (0.0, false),
        };
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key, now, loaded.0);
        let _ = self.cleanup_tx.try_send(false);
        loaded
    }
}

pub struct ClusterAnalyzeStatusProvider {
    /// The domain whose system sessions run Go's restricted query.
    pub domain: Weak<Domain>,
    pub approximate_counts: Arc<ClusterApproximateTableCountProvider>,
}

impl tidb_stats_handle_autoanalyze::analyze_jobs::AnalyzeStatusProvider for ClusterAnalyzeStatusProvider {
    fn load_analyze_status(&self) -> Result<Vec<tidb_stats::AnalyzeStatusJob>, String> {
        // Go `dataForAnalyzeStatusHelper`'s restricted query; privilege
        // filtering happens later in the session, so invisible rows still
        // occupy the limit, as in Go.
        const MAX_ANALYZE_JOBS: i64 = 30;
        let session_pool = self.domain.upgrade().map(|domain| domain.advanced_sys_session_pool());
        let mut rows = Vec::new();
        crate::sys_session::StatsPool {
            session_pool: session_pool.as_ref(),
        }
        .call_with_sctx(
            &mut |sctx| {
                rows = tidb_stats_handle_util::exec_rows(
                    sctx,
                    "SELECT table_schema, table_name, partition_name, job_info, processed_rows, CONVERT_TZ(start_time, @@TIME_ZONE, '+00:00'), CONVERT_TZ(end_time, @@TIME_ZONE, '+00:00'), state, fail_reason, instance, process_id FROM mysql.analyze_jobs ORDER BY update_time DESC LIMIT %?",
                    &[tidb_sqlescape::SqlArg::Signed(MAX_ANALYZE_JOBS)],
                )?
                .0;
                Ok(())
            },
            &[],
        )
        .map_err(|error| error.message)?;
        Ok(rows
            .iter()
            .map(|row| {
                let null = |index: usize| row.get(index).is_none_or(tidb_datatype::Datum::is_null);
                let text = |index: usize| {
                    row.get(index)
                        .and_then(tidb_datatype::Datum::as_raw_bytes)
                        .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                        .unwrap_or_default()
                };
                let time = |index: usize| match row.get(index) {
                    Some(tidb_datatype::Datum::Time(value)) => Some(*value),
                    _ => None,
                };
                tidb_stats::AnalyzeStatusJob {
                    table_schema: text(0),
                    table_name: text(1),
                    partition_name: text(2),
                    job_info: text(3),
                    processed_rows: row.get(4).map_or(0, tidb_datatype::Datum::get_int64),
                    start_time: time(5),
                    end_time: time(6),
                    state: match row.get(7) {
                        Some(tidb_datatype::Datum::Enum(value, _)) => value.name().to_utf8_lossy_go(),
                        _ => String::new(),
                    },
                    fail_reason: (!null(8)).then(|| text(8)),
                    instance: text(9),
                    process_id: (!null(10))
                        .then(|| row.get(10).map_or(0, tidb_datatype::Datum::get_int64) as u64),
                }
            })
            .collect())
    }

    fn approximate_table_count(
        &self,
        resource_group: &str,
        physical_id: i64,
        database: &str,
        table: &str,
        partition: &str,
    ) -> i64 {
        tidb_pdhelper::ApproximateTableCountProvider::approximate_table_count(
            self.approximate_counts.as_ref(),
            resource_group,
            physical_id,
            database,
            table,
            partition,
        )
        .0 as i64
    }
}

pub(crate) fn exact_physical_row_count(
    transactions: &dyn ClusterTransactions,
    resource_group: &str,
    physical_id: i64,
) -> Result<i64, String> {
    const PAGE_SIZE: usize = 1_024;
    let prefix = tidb_txnkv::Key::from_bytes(tidb_tablecodec::table_key::gen_table_record_prefix(
        physical_id,
    ));
    let end = prefix.prefix_next();
    let mut cursor = prefix;
    let mut snapshot = transactions.open_snapshot(resource_group)?;
    let mut count = 0_i64;
    loop {
        let page = snapshot
            .scan(&cursor, &end, Some(PAGE_SIZE))
            .map_err(|error| error.to_string())?;
        count = count.saturating_add(i64::try_from(page.len()).unwrap_or(i64::MAX));
        if page.len() < PAGE_SIZE {
            return Ok(count);
        }
        cursor = tidb_txnkv::Key::from_bytes(page.last().expect("full page is nonempty").0.clone())
            .next();
    }
}

impl tidb_stats_handle_usage_predicatecolumn::cluster_predicate_column::ColumnStatsUsageProvider for ClusterColumnStatsUsageProvider {
    fn load_column_stats_usage(
        &self,
        location: &tidb_datatype::SessionTimeZone,
    ) -> Result<
        std::collections::HashMap<
            tidb_model::TableItemID,
            (Option<tidb_datatype::Time>, Option<tidb_datatype::Time>),
        >,
        String,
    > {
        // Go `statsUsageImpl.LoadColumnStatsUsage`.
        let session_pool = self.domain.upgrade().map(|domain| domain.advanced_sys_session_pool());
        let mut usage = std::collections::HashMap::new();
        crate::sys_session::StatsPool {
            session_pool: session_pool.as_ref(),
        }
        .call_with_sctx(
            &mut |sctx| {
                usage = tidb_stats_handle_usage_predicatecolumn::predicate_column::load_column_stats_usage(
                    sctx, location,
                )?;
                Ok(())
            },
            &[],
        )
        .map_err(|error| error.message)?;
        Ok(usage
            .into_iter()
            .map(|(item, times)| (item, (times.last_used_at, times.last_analyzed_at)))
            .collect())
    }
}

impl tidb_stats_handle_syncload::sync_load::StatisticsItemLoader for ClusterStatisticsItemLoader {
    fn load_items(
        &self,
        items: &[tidb_model::StatsLoadItem],
    ) -> Result<Vec<(i64, Arc<tidb_stats::Table>)>, String> {
        let catalog = self.catalog.get_latest();
        let domain = self.domain.upgrade();
        let pool = domain.as_ref().map(|domain| domain.advanced_sys_session_pool());
        let pool = crate::sys_session::StatsPool {
            session_pool: pool.as_ref(),
        };
        let skipped_column_types = self
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_ANALYZE_SKIP_COLUMN_TYPES)
            .map(|value| tidb_variable::varsutil::parse_analyze_skip_column_types(&value))
            .unwrap_or_default();
        for requested in items {
            let item = requested.table_item_id;
            let Some(table) = catalog
                .stored_schemas()
                .iter()
                .flat_map(|database| database.read().deprecated_tables.iter_deref())
                .find(|table| table.read().id == item.table_id)
            else {
                continue;
            };
            let table = table.read();
            if item.is_index
                && !table
                    .indices
                    .iter_deref()
                    .any(|index| index.read().id == item.id)
            {
                continue;
            }
            let column_type = (!item.is_index)
                .then(|| {
                    table.cols().iter_deref().find_map(|column| {
                        let column = column.read();
                        (column.id == item.id).then(|| column.field_type.clone())
                    })
                })
                .flatten();
            if !item.is_index && column_type.is_none() {
                continue;
            }
            if column_type.as_ref().is_some_and(|field_type| {
                skipped_column_types.contains(tidb_datatype::type_to_str(
                    field_type.code(),
                    field_type.charset_name(),
                ))
            }) {
                continue;
            }
            let current = self.stats.load();
            let Some(current) = current
                .get(&item.table_id)
                .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
            else {
                continue;
            };
            if item.is_index {
                if !current.index_load_needed(item.id).1 {
                    continue;
                }
            } else {
                let (_, load_needed, analyzed) =
                    current.column_load_needed(item.id, requested.full_load);
                if !load_needed {
                    continue;
                }
                if !analyzed {
                    self.stats.publish_empty_column(item.table_id, item.id, &table);
                    continue;
                }
            }
            let started = std::time::Instant::now();
            // Go `handleOneItemTask`: `SPool().WithSession`, the statement
            // context at high priority.
            let mut loaded = None;
            pool.call_with_sctx(
                &mut |sctx| {
                    loaded = tidb_stats_handle_syncload::real_tikv_stats::read_stats_for_one_item(
                        sctx,
                        item.table_id,
                        item.is_index,
                        item.id,
                        column_type.as_ref(),
                        requested.full_load,
                    )?;
                    Ok(())
                },
                &[],
            )
            .map_err(|error| error.message)?;
            if let Some(loaded) = loaded {
                // Go observes only a successful read with histogram metadata,
                // before cache publication. Skips and failed attempts do not
                // contribute to read latency.
                tidb_stats_handle_metrics::READ_STATS_HISTOGRAM
                    .observe(started.elapsed().as_millis() as f64);
                self.stats.update_item(item.table_id, loaded, &table);
            }
        }
        // The domain publication above is authoritative for every session.
        Ok(Vec::new())
    }
}

impl tidb_kv::lock_waits::DataLockWaitsProvider for ClusterDataLockWaits {
    fn lock_waits(&self) -> Result<Vec<tidb_kv::lock_waits::DataLockWait>, String> {
        self.transactions.lock_waits().map(|entries| {
            entries
                .into_iter()
                .map(|entry| tidb_kv::lock_waits::DataLockWait {
                    txn: entry.txn,
                    wait_for_txn: entry.wait_for_txn,
                    key: entry.key,
                    resource_group_tag: entry.resource_group_tag,
                })
                .collect()
        })
    }
}

/// Go `domain.Domain`: the node's schema image and its follower, the
/// statistics handle, the DDL and transaction clients, the privilege and
/// process registries and the background workers -- shared by every session
/// it opens, one cluster-backed [`Session`] per authenticated connection.
/// Go `createSessionFunc`: how the session package opens one internal
/// session over this domain for its system-session pool.
pub type SysSessionOpener =
    fn(&Arc<Domain>, u64) -> Result<Arc<dyn crate::sys_session::DomainSysSession>, String>;

/// Go `Domain.bindHandle` (an `atomic.Pointer`), shared with the sessions
/// that read it later.
pub type BindingHandleSlot =
    Arc<std::sync::OnceLock<Arc<dyn tidb_bindinfo::cluster_binding_seam::ClusterBindings>>>;

pub struct Domain {
    /// The session package's opener of this domain's internal sessions.
    pub(crate) sys_session_opener: SysSessionOpener,
    pub global_config_syncer: Option<Arc<tidb_domain_globalconfigsync::GlobalConfigSyncer>>,
    /// Go `Domain.bindHandle`: stored once by [`Self::init_binding_handle`];
    /// a session reads it when it needs it, so one opened earlier sees it.
    bindings: BindingHandleSlot,
    pub binding_session_factory: Weak<Domain>,
    /// The domain as the schema builder reaches it once [`Self::shared`] has
    /// made it: Go `Builder.factory` takes the domain's internal sessions.
    schema_builder_domain: Arc<std::sync::OnceLock<Weak<Domain>>>,
    /// The `SHOW STATUS` providers this domain registered as it started its
    /// DDL and binding handle, unregistered when it closes.
    statistics: std::sync::Mutex<Vec<Arc<dyn tidb_variable::statusvar::Statistics>>>,
    /// The write/read capability every connection's statements open their
    /// snapshots and publish their commits through.
    pub transactions: Arc<dyn ClusterTransactions>,
    /// Storage-backed wait-for graph exposed by `DATA_LOCK_WAITS`.
    pub data_lock_waits: Arc<ClusterDataLockWaits>,
    /// The route a stored-schema change this node can express takes.
    pub ddl: Arc<dyn ClusterDdl>,
    /// The route a stored-account change takes; see
    /// [`crate::cluster_account_seam`].
    pub accounts: Arc<dyn ClusterAccountWriter>,
    /// The route a `SET GLOBAL` change takes; see
    /// [`crate::cluster_sysvar_seam`].
    pub sysvars: Arc<dyn ClusterSysvarWriter>,
    /// The cluster catalog, republished whole by the reload thread and by a
    /// DDL's own inline reload. A connection takes one `Arc` per statement, so
    /// no session ever sees a half-updated catalog.
    pub catalog: Arc<InfoCache>,
    /// Go's one `privilege.Manager` per `Domain`, here loaded from the
    /// cluster's own `mysql.*`.
    pub privileges: PrivilegeRegistry,
    /// Go's one `sessmgr.Manager` per TiDB instance: what `SHOW PROCESSLIST`
    /// reads and `KILL` reaches into.
    pub processes: ProcessRegistry,
    /// Go `kv.Storage.GetMPPClient()`: the TiFlash MPP client this node's
    /// sessions serve columnar reads with, when the node has one.
    pub mpp_client: Option<Arc<dyn tidb_txnkv::MppClient>>,
    /// Go `kv.Storage.GetClient()`: the coprocessor client this node's
    /// sessions send built requests through, when it was given one. `None`
    /// keeps every scan on the raw key/value path.
    pub kv_client: Option<Arc<dyn tidb_txnkv::Client>>,
    /// This node's server-info syncer, which
    /// `information_schema.TIDB_SERVERS_INFO` reads. `None` leaves that
    /// table empty, which is the honest answer for a node that never
    /// established an identity.
    pub server_info: Option<Arc<tidb_domain_serverinfo::serverinfo_syncer::Syncer>>,
    server_identity: Option<Arc<tidb_domain::server_id::ServerIdAuthority>>,
    /// Go `Domain.connIDAllocator`: the node's one connection-ID allocator,
    /// shared with the SQL listener and drawn from by internal processes
    /// (auto-analyze's `NextConnID`).
    connection_ids: Arc<dyn tidb_globalconn::Allocator + Send + Sync>,
    pub cluster_topology: Option<Arc<tidb_domain::cluster_topology::ClusterTopology>>,
    /// What the session layer runs this node's sessions with, beside the
    /// domain: Go's `createSessionWithOpt` and `cmd/tidb-server`'s main wire
    /// these into each session, and Go's domain holds none of them. The
    /// session layer owns their type; see [`Domain::session_services`].
    session_services: std::sync::OnceLock<Arc<dyn std::any::Any + Send + Sync>>,
    /// Go's one process-wide `GlobalVarsAccessor`.
    pub global_vars: GlobalSysvars,
    /// The tables of the boot catalog no session can include, kept so the
    /// node reports them once at startup instead of per connection.
    boot_skipped: Vec<SkippedTable>,
    /// This node's loaded tables' `mysql.stats_*`, republished whole by the
    /// stats reload thread [`run_cluster_session_node`] owns. Plumbing only:
    /// the estimator that will read this is a parallel unit.
    pub stats: Arc<SharedStats>,
    /// Go's per-`tidb-server` auto-increment allocators. Held HERE, above the
    /// per-session catalogs, because a reserved id range must outlive the
    /// `TableCommon` that was handing it out; see [`crate::cluster_auto_id_seam`].
    pub auto_ids: Arc<dyn TableAutoIds>,
    /// Process-owned spill authority inherited by every connection.
    pub spill_storage: Option<Arc<tidb_spill_storage::SpillStorage>>,
    /// Process-wide memory admission authority installed before listener bind.
    pub mem_arbitrator: Option<Arc<tidb_memory::MemArbitrator>>,
    /// The catalog versions live statements and transactions still read at
    /// -- the MDL gate the schema-sync acknowledger consults before telling
    /// a Go DDL owner this node has the new schema. See
    /// [`tidb_ddl::schema_sync::SchemaPinRegistry`].
    pub schema_pins: Arc<tidb_ddl::schema_sync::SchemaPinRegistry>,
    /// The node's schema validator (Go `Domain.SchemaValidator`), which
    /// every commit asks whether its schema version is still in force. `None`
    /// is a tier with no cluster schema to follow, which commits unchecked.
    pub schema_validator: Option<Arc<SchemaValidator>>,
    /// One live planner view over the domain's canonical statistics.
    pub statistics_view: Arc<tidb_stats_handle::StatisticsView>,
    /// Go Domain starts one statistics-load pool, shared by user and internal sessions.
    pub stats_load_workers: std::sync::OnceLock<Arc<tidb_stats_handle_syncload::sync_load::StatisticsLoadWorkers>>,
    /// The store's regions every session of this node reads (Go
    /// `kv.SplittableStore` and the region cache).
    pub region_store: Option<Arc<dyn tidb_kv::region_map::RegionStore>>,
    /// Go `executor.TakeSnapshot`, which the workload repository installs
    /// when the server's main sets it up (`SetupRepository`).
    pub take_snapshot: std::sync::OnceLock<std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync>>,
    /// Go `Domain.etcdClient`: the node's one etcd client, when it has PD.
    etcd_client: std::sync::OnceLock<Arc<tidb_pd_client::EtcdClient>>,
    /// Go Domain/StatsHandle's one node-global usage implementation.
    pub stats_usage: Arc<tidb_stats_handle_usage::StatsUsageHandle>,
    /// Go Domain's index-GC and positive-lease usage-dump workers.
    pub stats_usage_workers: std::sync::OnceLock<StatsUsageWorkers>,
    /// Go Domain's statistics owner, used by analyze-job history GC and
    /// dead-instance cleanup.
    pub stats_owner: Option<Arc<dyn tidb_owner::Manager>>,
    /// Go Domain's capacity-200 advanced system-session pool. It is installed
    /// after stable `Arc` ownership because its factory retains only a weak
    /// reference back to the domain-shaped session factory.
    pub(crate) advanced_sys_session_pool:
        std::sync::OnceLock<Arc<crate::sys_session::DomainSysSessionPool>>,
    /// Pinned Go Domain's durable DDL notifier, registered before the stats
    /// owner campaigns and driven by that owner's lifecycle.
    ddl_notifier: std::sync::OnceLock<Arc<tidb_ddl_notifier::DdlNotifier>>,
    /// Go `analyzeJobsCleanupWorker`, installed after stable `Arc` ownership.
    pub analyze_jobs_cleanup_worker: std::sync::OnceLock<AnalyzeJobsCleanupWorker>,
    /// Go `Domain.requestUnitsWriterLoop`.
    ru_stats_writer: std::sync::OnceLock<ru_stats_writer::RuStatsWriterWorker>,
    workload_learning_worker: std::sync::OnceLock<workload_learning::WorkloadLearningWorker>,
    /// Go `TelemetryLoop`'s goroutine.
    telemetry_worker: std::sync::OnceLock<telemetry::TelemetryWorker>,
    /// The cluster's BDR role in TiKV meta, which every session's ADMIN
    /// BDR ROLE statements read and write.
    pub bdr_role_store: std::sync::OnceLock<Arc<dyn tidb_meta::BdrRoleStore>>,
    pub ddl_job_history_store:
        std::sync::OnceLock<Arc<dyn tidb_ddl::ddl_history::DdlJobHistoryStore>>,
    /// Go `autoAnalyzeWorker`, installed after stable `Arc` ownership.
    pub auto_analyze_worker: std::sync::OnceLock<AutoAnalyzeWorker>,
    /// Go Domain's capacity-16 historical-statistics mailbox.
    pub historical_stats_worker: Arc<HistoricalStatsWorker<ClusterHistoricalInfoSchema>>,
    /// Go `StartHistoricalStatsWorker`, installed after stable `Arc` ownership.
    historical_stats_runtime: std::sync::OnceLock<HistoricalStatsRuntime>,
    /// Go `planReplayerHandle` and the dump-file GC loop.
    plan_replayer: std::sync::OnceLock<plan_replayer_owner::PlanReplayerRuntime>,
    /// Go `Domain.LoadSigningCertLoop`'s reload thread.
    signing_cert_loop: std::sync::OnceLock<SigningCertLoop>,
    /// Go `Domain.expensiveQueryHandle`, built over the domain's exit
    /// (`expensivequery.NewExpensiveQueryHandle(do.exit)`).
    expensive_query_handle: Arc<tidb_expensivequery::Handle>,
    /// Go `do.exit` as the expensive-query handle waits on it: dropped when
    /// the domain closes.
    expensive_query_exit: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    /// Go's process-global approximate table-count cache.
    pub approximate_table_counts: Arc<ClusterApproximateTableCountProvider>,
    /// Go StatsHandle's one auto-analyze priority queue. The refresher creates
    /// it lazily because the source needs a weak reference to this factory.
    auto_analyze_priority_queue: Arc<
        std::sync::OnceLock<
            Arc<tidb_stats_handle_autoanalyze_priorityqueue::AnalysisPriorityQueue>,
        >,
    >,
    /// Go StatsHandle's one refresher over the priority queue.
    auto_analyze_refresher:
        std::sync::OnceLock<Arc<Mutex<tidb_stats_handle_autoanalyze_refresher::Refresher>>>,
}

/// Go `SelectAnalyzeJobsOnCurrentInstanceSQL`.
const SELECT_ANALYZE_JOBS_ON_CURRENT_INSTANCE_SQL: &str = "SELECT id, process_id
		FROM mysql.analyze_jobs
		WHERE instance = %?
		AND state IN ('pending', 'running')
		AND update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)";
/// Go `SelectAnalyzeJobsSQL`.
const SELECT_ANALYZE_JOBS_SQL: &str = "SELECT id, instance
		FROM mysql.analyze_jobs
		WHERE state IN ('pending', 'running')
		AND update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)";
/// Go `BatchUpdateAnalyzeJobSQL`.
const BATCH_UPDATE_ANALYZE_JOB_SQL: &str = "UPDATE mysql.analyze_jobs
            SET state = 'failed',
            fail_reason = 'The TiDB Server has either shut down or the analyze query was terminated during the analyze job execution',
            process_id = NULL
            WHERE id IN (%?)";

/// Go `t.UTC().Format(types.TimeFormat)` of a UTC time.
fn go_time_format(time: tidb_datatype::Time) -> Result<String, String> {
    time.date_format("%Y-%m-%d %H:%i:%s")
        .map_err(|error| error.to_string())
}

/// Go `row.GetUint64(i)`.
fn datum_u64(datum: &tidb_datatype::Datum) -> Option<u64> {
    match datum {
        tidb_datatype::Datum::UInt(value) => Some(*value),
        tidb_datatype::Datum::Int(value) => Some(*value as u64),
        _ => None,
    }
}

/// Go `row.GetString(i)`.
fn datum_text(datum: &tidb_datatype::Datum) -> String {
    String::from_utf8_lossy(datum.go_bytes()).into_owned()
}

/// `BatchUpdateAnalyzeJobSQL` over `job_ids` (quoted, as Go's `[]string`
/// argument is), logged as Go logs it.
fn batch_fail_analyze_jobs(
    sctx: &dyn crate::sys_session::DomainSysSession,
    job_ids: &[String],
    message: &str,
) -> Result<(), tidb_sqlexec::SqlExecError> {
    if job_ids.is_empty() {
        return Ok(());
    }
    let ids: Vec<&[u8]> = job_ids.iter().map(|id| id.as_bytes()).collect();
    tidb_stats_handle_util::exec_rows(
        sctx,
        BATCH_UPDATE_ANALYZE_JOB_SQL,
        &[tidb_sqlescape::SqlArg::Strings(&ids)],
    )?;
    tidb_stats_handle_logutil::stats_logger().info(
        message,
        &[tidb_log::Field::new(
            "jobIDs",
            tidb_log::Value::Str(format!("{job_ids:?}")),
        )],
    );
    Ok(())
}

impl Domain {
    /// Binds the session layer's services to this node, once.
    pub fn set_session_services(
        &self,
        services: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), Arc<dyn std::any::Any + Send + Sync>> {
        self.session_services.set(services)
    }

    /// The session layer's services, as the type it bound them with.
    pub fn session_services<T: std::any::Any + Send + Sync>(&self) -> Option<Arc<T>> {
        Arc::clone(self.session_services.get()?).downcast().ok()
    }

    /// The node's privilege registry.
    pub fn privileges(&self) -> PrivilegeRegistry {
        self.privileges.clone()
    }

    /// The node's server-info syncer.
    pub fn server_info(&self) -> Option<Arc<tidb_domain_serverinfo::serverinfo_syncer::Syncer>> {
        self.server_info.clone()
    }

    /// The node's GLOBAL variable cache.
    pub fn global_vars(&self) -> GlobalSysvars {
        self.global_vars.clone()
    }
}

impl Domain {
    /// Install the one domain notification queue shared by every session.
    pub fn with_global_config_syncer(
        mut self,
        syncer: Arc<tidb_domain_globalconfigsync::GlobalConfigSyncer>,
    ) -> Self {
        self.global_config_syncer = Some(syncer);
        self
    }

    /// Returns Go Domain's single advanced internal-session pool, whose
    /// sessions [`Self::sys_session_opener`] opens.
    pub fn advanced_sys_session_pool(
        self: &Arc<Self>,
    ) -> Arc<crate::sys_session::DomainSysSessionPool> {
        Arc::clone(self.advanced_sys_session_pool.get_or_init(|| {
            let domain = Arc::downgrade(self);
            let open = self.sys_session_opener;
            let next_connection_id = Arc::new(std::sync::atomic::AtomicU64::new(1_u64 << 61));
            Arc::new(tidb_syssession::AdvancedSessionPool::new(200, move || {
                let domain = domain.upgrade().ok_or_else(|| {
                    tidb_syssession::SysSessionError::new("cluster session factory is stopped")
                })?;
                let connection_id =
                    next_connection_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                open(&domain, connection_id).map_err(tidb_syssession::SysSessionError::new)
            }))
        }))
    }

    /// The factory behind the `Arc` its internal sessions (restricted SQL,
    /// the binding and statistics maintenance) are opened through: Go's
    /// domain owns its session pool for its whole life.
    #[must_use]
    pub fn shared(mut self) -> Arc<Self> {
        Arc::new_cyclic(|factory| {
            self.binding_session_factory = factory.clone();
            let _ = self.schema_builder_domain.set(factory.clone());
            register_gc_tidb_table_hooks(&self.global_vars, factory);
            self
        })
    }

    /// Go `Builder.factory` for this domain's schema images.
    #[must_use]
    pub fn cached_table_session_factory(&self) -> Box<crate::cluster_session::CachedTableSessionFactory> {
        cached_table_session_factory(Arc::clone(&self.schema_builder_domain))
    }

    /// Go `Domain.InitBindingHandle`'s `do.bindHandle.Store`: the node-owned
    /// global-binding authority, stored once.
    ///
    /// # Panics
    ///
    /// A second store (Go stores it once, in `bootstrapSessionImpl`).
    pub fn init_binding_handle(
        &self,
        bindings: Arc<dyn tidb_bindinfo::cluster_binding_seam::ClusterBindings>,
    ) {
        // Go `NewBindingHandle`: `variable.RegisterStatistics(h)`.
        self.register_statistics(Arc::new(tidb_bindinfo::binding_stat::BindingStatistics(bindings.cache())));
        assert!(
            self.bindings.set(bindings).is_ok(),
            "the binding handle is stored once"
        );
    }

    /// Go `Domain.BindingHandle`.
    #[must_use]
    pub fn binding_handle(&self) -> Option<&Arc<dyn tidb_bindinfo::cluster_binding_seam::ClusterBindings>> {
        self.bindings.get()
    }

    /// The slot [`Self::init_binding_handle`] fills, for a reader that
    /// outlives a borrow of the domain (a session's commit hooks).
    #[must_use]
    pub fn binding_handle_slot(&self) -> BindingHandleSlot {
        Arc::clone(&self.bindings)
    }

    /// Binds the factory to an authority that has already read the cluster's
    /// catalog and accounts.
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        transactions: Arc<dyn ClusterTransactions>,
        ddl: Arc<dyn ClusterDdl>,
        accounts: Arc<dyn ClusterAccountWriter>,
        sysvars: Arc<dyn ClusterSysvarWriter>,
        catalog: Arc<InfoCache>,
        privileges: PrivilegeRegistry,
        global_vars: GlobalSysvars,
        stats: Arc<SharedStats>,
        auto_ids: Arc<dyn TableAutoIds>,
        sys_session_opener: SysSessionOpener,
    ) -> Self {
        global_vars.set_external_timestamp_provider(Arc::new(ClusterExternalTimestamp(
            Arc::clone(&transactions),
        )));
        let boot_skipped = cluster_session_catalog(
            &catalog.get_latest(),
            &stats.load(),
            auto_ids.as_ref(),
        )
        .skipped;
        // Go's loader builds each version's `InfoSchema` before `InfoCache`
        // holds it; the domain owns the auto-id allocators the build needs.
        let schema_builder_domain = Arc::new(std::sync::OnceLock::new());
        catalog.install_schema_builder(Box::new({
            let auto_ids = Arc::clone(&auto_ids);
            let cached_table_session = cached_table_session_factory(Arc::clone(&schema_builder_domain));
            move |loaded, previous| {
                let built = cluster_session_catalog_with_templates(
                    loaded,
                    None,
                    auto_ids.as_ref(),
                    previous,
                    Some(cached_table_session.as_ref()),
                );
                let mut schema = built.catalog;
                schema.set_skipped_tables(built.skipped);
                schema
            }
        }));
        let stats_usage = Arc::new(tidb_stats_handle_usage::StatsUsageHandle::new());
        stats_usage.start_worker();
        let data_lock_waits = Arc::new(ClusterDataLockWaits {
            transactions: Arc::clone(&transactions),
        });
        let (expensive_query_exit, expensive_query_exited) = std::sync::mpsc::channel();
        let historical_stats_worker =
            Arc::new(HistoricalStatsWorker::new(ClusterHistoricalInfoSchema {
                catalog: Arc::clone(&catalog),
            }));
        let approximate_table_counts = Arc::new(ClusterApproximateTableCountProvider::new(
            Arc::clone(&transactions),
        ));
        Self {
            sys_session_opener,
            transactions,
            bindings: BindingHandleSlot::default(),
            statistics: std::sync::Mutex::default(),
            global_config_syncer: None,
            binding_session_factory: Weak::new(),
            schema_builder_domain,
            data_lock_waits,
            ddl,
            accounts,
            sysvars,
            catalog,
            privileges,
            processes: ProcessRegistry::default(),
            auto_ids,
            mpp_client: None,
            kv_client: None,
            server_info: None,
            server_identity: None,
            connection_ids: crate::connection_ids::new_connection_id_allocator(
                &tidb_config::config_tree::Config::default(),
                None,
            ),
            cluster_topology: None,
            session_services: std::sync::OnceLock::new(),
            global_vars,
            boot_skipped,
            statistics_view: Arc::new(tidb_stats_handle::StatisticsView::new(Arc::new(
                ClusterStatisticsSource(Arc::clone(&stats)),
            ))),
            stats,
            spill_storage: None,
            mem_arbitrator: None,
            schema_pins: Arc::new(tidb_ddl::schema_sync::SchemaPinRegistry::default()),
            schema_validator: None,
            stats_load_workers: std::sync::OnceLock::new(),
            region_store: None,
            take_snapshot: std::sync::OnceLock::new(),
            etcd_client: std::sync::OnceLock::new(),
            stats_usage,
            stats_usage_workers: std::sync::OnceLock::new(),
            stats_owner: None,
            advanced_sys_session_pool: std::sync::OnceLock::new(),
            ddl_notifier: std::sync::OnceLock::new(),
            analyze_jobs_cleanup_worker: std::sync::OnceLock::new(),
            ru_stats_writer: std::sync::OnceLock::new(),
            workload_learning_worker: std::sync::OnceLock::new(),
            telemetry_worker: std::sync::OnceLock::new(),
            bdr_role_store: std::sync::OnceLock::new(),
            ddl_job_history_store: std::sync::OnceLock::new(),
            auto_analyze_worker: std::sync::OnceLock::new(),
            historical_stats_worker,
            historical_stats_runtime: std::sync::OnceLock::new(),
            plan_replayer: std::sync::OnceLock::new(),
            signing_cert_loop: std::sync::OnceLock::new(),
            expensive_query_handle: Arc::new(tidb_expensivequery::Handle::new(expensive_query_exited)),
            expensive_query_exit: Mutex::new(Some(expensive_query_exit)),
            approximate_table_counts,
            auto_analyze_priority_queue: Arc::new(std::sync::OnceLock::new()),
            auto_analyze_refresher: std::sync::OnceLock::new(),
        }
    }

    /// Returns Go StatsHandle's single auto-analyze priority queue.
    pub fn auto_analyze_priority_queue(
        self: &Arc<Self>,
        stats_lease: Duration,
    ) -> Arc<tidb_stats_handle_autoanalyze_priorityqueue::AnalysisPriorityQueue> {
        Arc::clone(self.auto_analyze_priority_queue.get_or_init(|| {
            let source: Arc<dyn tidb_stats_handle_autoanalyze_priorityqueue::PriorityQueueSource> =
                Arc::new(ClusterPriorityQueueSource {
                    factory: Arc::downgrade(self),
                    stats_lease,
                    session_pool: self.advanced_sys_session_pool(),
                });
            tidb_stats_handle_autoanalyze_priorityqueue::AnalysisPriorityQueue::new(source)
        }))
    }




    /// Returns Go StatsHandle's single auto-analyze refresher.
    pub fn auto_analyze_refresher(
        self: &Arc<Self>,
        stats_lease: Duration,
    ) -> Arc<Mutex<tidb_stats_handle_autoanalyze_refresher::Refresher>> {
        Arc::clone(self.auto_analyze_refresher.get_or_init(|| {
            let concurrency = self
                .global_vars
                .get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_CONCURRENCY)
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(tidb_vardef::defaults::DEF_TIDB_AUTO_ANALYZE_CONCURRENCY as usize);
            Arc::new(Mutex::new(
                tidb_stats_handle_autoanalyze_refresher::Refresher::new(
                    self.auto_analyze_priority_queue(stats_lease),
                    concurrency,
                ),
            ))
        }))
    }

    fn auto_analyze_refresh_parameters(
        &self,
    ) -> Result<tidb_stats_handle_autoanalyze_refresher::RefreshParameters, String> {
        let get = |name| {
            self.global_vars
                .get(name)
                .map_err(|error| format!("{error:?}"))
        };
        let prune_mode = get(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)?;
        Ok(tidb_stats_handle_autoanalyze_refresher::RefreshParameters {
            auto_analyze_ratio: get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_RATIO)?,
            prune_mode: if prune_mode.eq_ignore_ascii_case("static") {
                tidb_stats_handle_autoanalyze_priorityqueue::PartitionPruneMode::Static
            } else {
                tidb_stats_handle_autoanalyze_priorityqueue::PartitionPruneMode::Dynamic
            },
            start_time: get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_START_TIME)?,
            end_time: get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_END_TIME)?,
            max_concurrency: get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_CONCURRENCY)?
                .parse()
                .map_err(|error| format!("invalid auto analyze concurrency: {error}"))?,
        })
    }

    /// Starts Go `Domain.StartHistoricalStatsWorker`.
    /// Go `Domain.SetupWorkloadBasedLearningWorker`: the read-table-cost
    /// worker, started after the statistics loops as Go's bootstrap does.
    pub fn setup_workload_based_learning_worker(self: &Arc<Self>) {
        if self.workload_learning_worker.get().is_some() {
            return;
        }
        let _ = self
            .workload_learning_worker
            .set(workload_learning::WorkloadLearningWorker::start(self));
    }

    /// Go `Domain.TelemetryLoop`, which `BootstrapSession` starts only when
    /// the configuration enables telemetry.
    pub fn telemetry_loop(self: &Arc<Self>) {
        if self.telemetry_worker.get().is_some() {
            return;
        }
        if let Some(worker) = telemetry::TelemetryWorker::start(self) {
            let _ = self.telemetry_worker.set(worker);
        }
    }

    pub fn start_historical_stats_worker(self: &Arc<Self>) {
        if !tidb_domain::historical_stats::enable_dump_historical_stats()
            || self.historical_stats_runtime.get().is_some()
        {
            return;
        }
        let handle = Arc::new(ClusterHistoricalStatsHandle {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            global_vars: self.global_vars.clone(),
            auto_ids: Arc::clone(&self.auto_ids),
            domain: self.binding_session_factory.clone(),
        });
        let _ = self
            .historical_stats_runtime
            .set(HistoricalStatsRuntime::start(
                Arc::clone(&self.historical_stats_worker),
                handle,
            ));
    }

    /// Go `Domain.LoadSigningCertLoop`: binds the session-token signing
    /// certificate and reloads it periodically so a rotated one is picked up.
    pub fn load_signing_cert_loop(&self, signing_cert: &str, signing_key: &str) {
        tidb_sessionstates::set_cert_path(signing_cert);
        tidb_sessionstates::set_key_path(signing_key);
        let _ = self.signing_cert_loop.get_or_init(SigningCertLoop::start);
    }

    /// Go `statsReadWriter.DumpStatsToJSON` without a history executor
    /// (`DumpStatsToJSONBySnapshot` at snapshot 0): a partitioned table's
    /// partitions are dumped under static pruning or `dump_partition_stats`,
    /// its global stats under `global`.
    pub fn dump_stats_to_json(
        &self,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        dump_partition_stats: bool,
    ) -> Result<Option<tidb_stats_util::JsonTable>, String> {
        let handle = ClusterHistoricalStatsHandle {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            global_vars: self.global_vars.clone(),
            auto_ids: Arc::clone(&self.auto_ids),
            domain: self.binding_session_factory.clone(),
        };
        // Go `util.GetCurrentPruneMode`.
        let prune_mode = self
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .map_err(|error| format!("{error:?}"))?;
        let is_dynamic_mode = prune_mode.eq_ignore_ascii_case("dynamic");
        let Some(partition) = table.get_partition_info() else {
            return handle.table_json(database_name, table, table.id);
        };
        let mut partitions = std::collections::BTreeMap::new();
        if !is_dynamic_mode || dump_partition_stats {
            for definition in partition.read().definitions.snapshot() {
                if let Some(json) = handle.table_json(database_name, table, definition.id)? {
                    partitions.insert(definition.name.lowercase().to_owned(), Some(json));
                }
            }
        }
        if let Some(global) = handle.table_json(database_name, table, table.id)? {
            partitions.insert(tidb_stats_util::TIDB_GLOBAL_STATS.to_owned(), Some(global));
        }
        Ok(Some(tidb_stats_util::JsonTable {
            database_name: database_name.to_owned(),
            table_name: table.name.lowercase().to_owned(),
            partitions: Some(partitions),
            ..tidb_stats_util::JsonTable::default()
        }))
    }

    /// Pinned Go `StatsHandle.DumpHistoricalStatsBySnapshot` over the live
    /// shared catalog and restricted cluster transactions.
    pub fn dump_historical_stats_by_snapshot(
        &self,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        snapshot_ts: u64,
    ) -> Result<(Option<tidb_stats_util::JsonTable>, Vec<String>), String> {
        ClusterHistoricalStatsHandle {
            transactions: Arc::clone(&self.transactions),
            catalog: Arc::clone(&self.catalog),
            global_vars: self.global_vars.clone(),
            auto_ids: Arc::clone(&self.auto_ids),
            domain: self.binding_session_factory.clone(),
        }
        .dump_historical_stats_by_snapshot(database_name, table, snapshot_ts)
    }

    /// Go `statsGCImpl.ClearOutdatedHistoryStats` on one of the statistics
    /// handle's sessions.
    pub fn clear_outdated_history_stats(&self) -> Result<(), String> {
        let pool = self.stats_session_pool()?;
        let historical_stats_duration = self.historical_stats_duration()?;
        tidb_stats_handle_util::call_with_sctx(
            pool.as_ref(),
            |sctx| tidb_stats_handle_storage::gc::clear_outdated_history_stats(sctx, historical_stats_duration),
            &[],
        )
        .map_err(|error| error.to_string())
    }

    /// Go `statsHandle.SPool()`: the domain's system sessions.
    fn stats_session_pool(&self) -> Result<Arc<crate::sys_session::DomainSysSessionPool>, String> {
        self.binding_session_factory
            .upgrade()
            .map(|domain| domain.advanced_sys_session_pool())
            .ok_or_else(|| "cluster session factory is stopped".to_owned())
    }

    /// Go `statsAnalyze.DeleteAnalyzeJobs`.
    pub fn delete_analyze_jobs(&self, cutoff: tidb_datatype::Time) -> Result<(), String> {
        let pool = self.stats_session_pool()?;
        let update_time = go_time_format(cutoff)?;
        tidb_stats_handle_util::call_with_sctx(
            pool.as_ref(),
            |sctx| {
                tidb_stats_handle_util::exec_rows(
                    sctx,
                    "DELETE FROM mysql.analyze_jobs WHERE update_time < CONVERT_TZ(%?, '+00:00', @@TIME_ZONE)",
                    &[tidb_sqlescape::SqlArg::from(update_time.as_str())],
                )?;
                Ok(())
            },
            &[],
        )
        .map_err(|error| error.to_string())
    }

    /// Go `statsAnalyze.CleanupCorruptedAnalyzeJobsOnCurrentInstance` /
    /// `CleanupCorruptedAnalyzeJobsOnCurrentInstance`: this instance's
    /// pending or running jobs whose process no longer runs ANALYZE are
    /// failed, in one transaction.
    pub fn cleanup_corrupted_analyze_jobs_on_current_instance(
        &self,
        cutoff: tidb_datatype::Time,
    ) -> Result<(), String> {
        let syncer = self
            .server_info
            .as_ref()
            .ok_or_else(|| "server info is not initialized".to_owned())?;
        let local = syncer.get_local_server_info();
        let instance = tidb_domain_serverinfo::serverinfo_syncer::join_host_port(
            &local.static_info.ip,
            local.static_info.port,
        );
        let current_running_process_ids = self
            .processes
            .snapshot()
            .into_iter()
            .filter_map(|process| {
                process
                    .info
                    .as_deref()
                    .is_some_and(is_analyze_table_sql)
                    .then_some(process.id)
            })
            .collect::<HashSet<_>>();
        let pool = self.stats_session_pool()?;
        let ten_minutes_ago = go_time_format(cutoff)?;
        tidb_stats_handle_util::call_with_sctx(
            pool.as_ref(),
            |sctx| {
                use tidb_sqlescape::SqlArg;
                let (rows, _) = tidb_stats_handle_util::exec_rows(
                    sctx,
                    SELECT_ANALYZE_JOBS_ON_CURRENT_INSTANCE_SQL,
                    &[SqlArg::from(instance.as_str()), SqlArg::from(ten_minutes_ago.as_str())],
                )?;
                let job_ids: Vec<String> = rows
                    .iter()
                    .filter(|row| {
                        row.get(1).and_then(datum_u64).is_some_and(|process_id| {
                            !current_running_process_ids.contains(&process_id)
                        })
                    })
                    .map(|row| row.first().and_then(datum_u64).unwrap_or(0).to_string())
                    .collect();
                batch_fail_analyze_jobs(sctx, &job_ids, "clean up the potentially corrupted analyze jobs from current instance")
            },
            &[tidb_stats_handle_util::FLAG_WRAP_TXN],
        )
        .map_err(|error| error.to_string())
    }

    /// Go `statsAnalyze.CleanupCorruptedAnalyzeJobsOnDeadInstances`: the
    /// pending or running jobs of instances no longer registered are failed,
    /// in one transaction.
    pub fn cleanup_corrupted_analyze_jobs_on_dead_instances(
        &self,
        cutoff: tidb_datatype::Time,
    ) -> Result<(), String> {
        let syncer = self
            .server_info
            .as_ref()
            .ok_or_else(|| "server info is not initialized".to_owned())?;
        let pool = self.stats_session_pool()?;
        let ten_minutes_ago = go_time_format(cutoff)?;
        tidb_stats_handle_util::call_with_sctx(
            pool.as_ref(),
            |sctx| {
                use tidb_sqlescape::SqlArg;
                let (rows, _) = tidb_stats_handle_util::exec_rows(
                    sctx,
                    SELECT_ANALYZE_JOBS_SQL,
                    &[SqlArg::from(ten_minutes_ago.as_str())],
                )?;
                if rows.is_empty() {
                    return Ok(());
                }
                let instances = syncer
                    .get_all_server_info()
                    .map_err(crate::sys_session::stats_session_error)?
                    .into_values()
                    .map(|info| {
                        tidb_domain_serverinfo::serverinfo_syncer::join_host_port(
                            &info.static_info.ip,
                            info.static_info.port,
                        )
                    })
                    .collect::<HashSet<_>>();
                let job_ids: Vec<String> = rows
                    .iter()
                    .filter(|row| {
                        let instance = row.get(1).map(datum_text).unwrap_or_default();
                        !instances.contains(&instance)
                    })
                    .map(|row| row.first().and_then(datum_u64).unwrap_or(0).to_string())
                    .collect();
                batch_fail_analyze_jobs(sctx, &job_ids, "clean up the potentially corrupted analyze jobs from dead instances")
            },
            &[tidb_stats_handle_util::FLAG_WRAP_TXN],
        )
        .map_err(|error| error.to_string())
    }

    /// Go `statsGCImpl.GCStats`: `storage.GCStats` on one of the statistics
    /// handle's sessions, against the latest schema.
    pub fn gc_stats(&self, stats_lease: Duration, ddl_lease: Duration) -> Result<(), String> {
        let result = self.gc_stats_on_pool(stats_lease.max(ddl_lease));
        self.check_auto_analyze_windows_at(Utc::now());
        result
    }

    fn gc_stats_on_pool(&self, lease: Duration) -> Result<(), String> {
        let pool = self.stats_session_pool()?;
        let catalog = self.catalog.get_latest();
        let historical_stats_duration = self.historical_stats_duration()?;
        tidb_stats_handle_util::call_with_sctx(
            pool.as_ref(),
            |sctx| {
                tidb_stats_handle_storage::gc::gc_stats(
                    sctx,
                    lease,
                    historical_stats_duration,
                    &|physical_id| ClusterHistoricalStatsHandle::physical_table(&catalog, physical_id),
                )
            },
            &[],
        )
        .map_err(|error| error.to_string())
    }

    /// Go `vardef.HistoricalStatsDuration`.
    fn historical_stats_duration(&self) -> Result<Duration, String> {
        let configured = self
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_HISTORICAL_STATS_DURATION)
            .map_err(|error| format!("read tidb_historical_stats_duration failed: {error:?}"))?;
        let nanos = serde_json::from_value::<tidb_config_configtypes::Duration>(
            serde_json::Value::String(configured),
        )
        .map_err(|error| error.to_string())?
        .0;
        Ok(Duration::from_nanos(u64::try_from(nanos).unwrap_or(0)))
    }

    pub fn check_auto_analyze_windows_at(&self, now: chrono::DateTime<Utc>) {
        let Ok(parameters) = self.auto_analyze_refresh_parameters() else {
            return;
        };
        let Ok((start, end)) = tidb_stats_handle_autoanalyze_exec::parse_auto_analysis_window(
            &parameters.start_time,
            &parameters.end_time,
        ) else {
            return;
        };
        let window =
            tidb_stats_handle_autoanalyze_priorityqueue::AutoAnalysisTimeWindow::new(start, end);
        if window.is_within_time_window(now) {
            return;
        }
        for process_id in tidb_stats_handle_util::GLOBAL_AUTO_ANALYZE_PROCESS_LIST.all() {
            // Go `Domain.CheckAutoAnalyzeWindows`.
            tidb_stats_handle_logutil::stats_logger().warn(
                "Kill auto analyze process because it exceeded the window",
                &[
                    tidb_log::Field::new("processID", tidb_log::Value::U64(process_id)),
                    tidb_log::Field::new(
                        "now",
                        tidb_log::Value::Str(tidb_log::format_time(
                            &now.with_timezone(&chrono::Local).fixed_offset(),
                        )),
                    ),
                    tidb_log::Field::new(
                        "start",
                        tidb_log::Value::Str(parameters.start_time.clone()),
                    ),
                    tidb_log::Field::new("end", tidb_log::Value::Str(parameters.end_time.clone())),
                ],
            );
            self.processes.kill(process_id, true);
        }
    }

    /// Starts Go Domain's usage workers after the factory has stable `Arc`
    /// ownership. Index GC runs for every lease; delta and column dumps run
    /// only for a positive lease.
    pub fn start_stats_usage_workers(self: &Arc<Self>, lease: crate::node_config::StatsLease) {
        if self.stats_usage_workers.get().is_some() {
            return;
        }
        let _ = self
            .stats_usage_workers
            .set(StatsUsageWorkers::start(self, lease));
    }

    /// Starts pinned Go `analyzeJobsCleanupWorker`. Go starts it only when
    /// `statsLease > 0`; zero and negative leases return before this worker.
    pub fn start_analyze_jobs_cleanup_worker(
        self: &Arc<Self>,
        lease: crate::node_config::StatsLease,
    ) {
        let crate::node_config::StatsLease::Positive(lease) = lease else {
            return;
        };
        if self.analyze_jobs_cleanup_worker.get().is_some() {
            return;
        }
        let _ = self
            .analyze_jobs_cleanup_worker
            .set(AnalyzeJobsCleanupWorker::start(self, lease));
    }

    /// Starts Go `Domain.requestUnitsWriterLoop` (`pkg/domain/domain.go:830`)
    /// over this node's PD ResourceManager, meta store and internal-session
    /// pool. Go starts it unconditionally beside the other domain loops,
    /// over the store's PD client.
    pub fn start_ru_stats_writer<S>(
        self: &Arc<Self>,
        pd: tidb_txnkv::driver::tikv_store::TikvStore,
        opener: Arc<tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>>,
        timeout: Duration,
    ) where
        S: tidb_store_driver_txn::transaction::StoreClient + 'static,
    {
        if self.ru_stats_writer.get().is_some() {
            return;
        }
        let deps = ru_stats_writer::ClusterRuStatsDeps::new(
            pd,
            opener,
            self.advanced_sys_session_pool(),
            timeout,
        );
        let _ = self
            .ru_stats_writer
            .set(ru_stats_writer::RuStatsWriterWorker::start(self, deps));
    }

    /// Installs the TiKV meta store that holds the cluster's BDR role, so
    /// sessions opened from now on answer ADMIN SET / UNSET / SHOW BDR ROLE
    /// from it.
    pub fn install_bdr_role_store<S>(
        &self,
        opener: Arc<tidb_store_driver_txn::transaction::RealOptimisticTransactionOpener<S>>,
        timeout: Duration,
    ) where
        S: tidb_store_driver_txn::transaction::StoreClient + 'static,
    {
        let _ = self
            .bdr_role_store
            .set(Arc::new(bdr_role_store::ClusterBdrRoleStore::new(
                Arc::clone(&opener),
                timeout,
            )));
        // Go reads the DDL jobs through the same meta store.
        let _ = self.ddl_job_history_store.set(Arc::new(
            ddl_job_history_store::ClusterDdlJobHistoryStore::new(opener, timeout),
        ));
    }

    /// Starts pinned Go `autoAnalyzeWorker`. Like Go, only a positive stats
    /// lease starts it; every tick checks the process switch and ownership.
    pub fn start_auto_analyze_worker(self: &Arc<Self>, lease: crate::node_config::StatsLease) {
        let crate::node_config::StatsLease::Positive(lease) = lease else {
            return;
        };
        if self.auto_analyze_worker.get().is_some() {
            return;
        }
        let _ = self
            .auto_analyze_worker
            .set(AutoAnalyzeWorker::start(self, lease));
    }

    pub fn handle_auto_analyze_tick(
        self: &Arc<Self>,
        run_auto_analyze: bool,
        stats_lease: Duration,
    ) {
        let is_owner = self
            .stats_owner
            .as_ref()
            .is_some_and(|owner| owner.is_owner());
        if !run_auto_analyze || !is_owner {
            if let Some(refresher) = self.auto_analyze_refresher.get() {
                refresher
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .close_priority_queue();
            }
            return;
        }
        let Ok(parameters) = self.auto_analyze_refresh_parameters() else {
            return;
        };
        let refresher = self.auto_analyze_refresher(stats_lease);
        let _ = refresher
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .analyze_highest_priority_tables(&parameters);
    }

    /// Go `statsUsageImpl.DumpColStatsUsageToKV`: each batch of Go
    /// `DumpColStatsUsageEntries` one statement on a statistics session.
    pub fn dump_col_stats_usage_to_kv(&self) -> Result<(), String> {
        let pool = self.stats_session_pool()?;
        let mut pending = self
            .stats_usage
            .session_stats_list()
            .begin_column_stats_usage_dump();
        let mut entries = pending.entries();
        // Go sorts the entries to keep one order and fewer deadlocks.
        entries.sort_by_key(|(item, _)| (item.table_id, item.id));
        persist_column_stats_usage_batches(&mut pending, &entries, |batch| {
            tidb_stats_handle_util::call_with_sctx(
                pool.as_ref(),
                |sctx| tidb_stats_handle_usage::session_stats_collect::dump_col_stats_usage_batch(sctx, batch),
                &[],
            )
            .map_err(|error| error.to_string())
        })
    }

    /// Go `statsUsageImpl.DumpStatsDeltaToKV`.
    pub fn dump_stats_delta_to_kv(&self, force_dump: bool, target_table_ids: &[i64]) -> Result<(), String> {
        let pool = self.stats_session_pool().ok();
        Self::dump_stats_delta_to_kv_parts(
            self.stats_usage.as_ref(),
            self.catalog.as_ref(),
            self.stats.as_ref(),
            &self.global_vars,
            &crate::sys_session::StatsPool {
                session_pool: pool.as_ref(),
            },
            force_dump,
            target_table_ids,
        )
    }

    /// Go `statsUsageImpl.DumpStatsDeltaToKV` over the handle's parts.
    pub fn dump_stats_delta_to_kv_parts(
        stats_usage: &tidb_stats_handle_usage::StatsUsageHandle,
        catalog_store: &InfoCache,
        stats_store: &SharedStats,
        global_vars: &GlobalSysvars,
        stats_pool: &dyn tidb_stats_handle_storage::stats_read_writer::StatsSessionPool,
        force_dump: bool,
        target_table_ids: &[i64],
    ) -> Result<(), String> {
        let mut pending = stats_usage.session_stats_list().begin_table_delta_dump();
        let table_ids = pending.collect_pending_stats_delta_table_ids(target_table_ids);
        for batch in table_ids.chunks(tidb_stats_handle_usage::DUMP_DELTA_BATCH_SIZE) {
            let batch_time = SystemTime::now();
            let catalog = catalog_store.get_latest();
            let stats = stats_store.load();
            let mut original_updates = Vec::new();
            let mut parents = std::collections::HashMap::new();
            for &table_id in batch {
                pending.initialize_time(table_id, batch_time);
                let Some((database, parent_id)) = stats_delta_table(&catalog, table_id) else {
                    continue;
                };
                if matches!(
                    database.as_str(),
                    "information_schema"
                        | "performance_schema"
                        | "metrics_schema"
                        | "mysql"
                        | "sys"
                        | "workload_schema"
                ) {
                    continue;
                }
                let Some(delta) = pending.get(table_id) else {
                    continue;
                };
                let realtime_count = stats
                    .get(&table_id)
                    .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
                    .map(|table| table.hist_coll.realtime_count);
                if !tidb_stats_handle_usage::need_dump_stats_delta(
                    force_dump,
                    delta,
                    batch_time,
                    realtime_count,
                ) {
                    continue;
                }
                if let Some(parent_id) = parent_id {
                    parents.insert(table_id, parent_id);
                }
                original_updates.push(tidb_stats_handle_usage::DeltaUpdate {
                    table_id,
                    delta,
                    is_locked: false,
                });
            }
            if original_updates.is_empty() {
                continue;
            }
            // Go: the whole batch in one wrapped transaction.
            let mut stats_version = 0;
            let mut updates = Vec::new();
            stats_pool
                .call_with_sctx(
                    &mut |sctx| {
                        stats_version = tidb_stats_handle_util::get_start_ts(sctx)?;
                        // Go `statsHandle.GetLockedTables`, on a session of its own.
                        let mut locked = Default::default();
                        stats_pool
                            .call_with_sctx(
                                &mut |sctx| {
                                    locked = tidb_stats_handle_lockstats::session_stats_lock::query_locked_tables(sctx)?;
                                    Ok(())
                                },
                                &[],
                            )
                            .map_err(|error| crate::sys_session::stats_session_error(error.message))?;
                        let locked: std::collections::HashSet<i64> = std::mem::take(&mut locked).into_iter().collect();
                        updates = tidb_stats_handle_usage::prepare_delta_updates(
                            original_updates.clone(),
                            |table_id| parents.get(&table_id).copied(),
                            &locked,
                        );
                        tidb_stats_handle_storage::update::update_stats_meta(sctx, stats_version, &updates)
                    },
                    &[tidb_stats_handle_util::FLAG_WRAP_TXN],
                )
                .map_err(|error| error.message)?;
            for update in original_updates {
                pending.mark_persisted(update.table_id);
            }
            let historical_stats_enabled = global_vars
                .get(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
                .is_ok_and(|value| tidb_vardef::option_values::tidb_opt_on(&value));
            if historical_stats_enabled {
                // Go `RecordHistoricalStatsMeta(statsVersion, "flush stats",
                // false, unlockedTableIDs...)`: only initialized tables.
                let targeted: Vec<i64> = updates
                    .iter()
                    .filter(|update| {
                        !update.is_locked
                            && stats
                                .get(&update.table_id)
                                .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
                                .is_some_and(|table| table.is_initialized())
                    })
                    .map(|update| update.table_id)
                    .collect();
                tidb_stats_handle_history::history_stats::record_historical_stats_meta_on(
                    stats_pool,
                    stats_version,
                    "flush stats",
                    &targeted,
                );
            }
        }
        Ok(())
    }

    fn gc_index_usage(&self) -> Result<(), String> {
        let catalog = self.catalog.get_latest();
        self.stats_usage.gc_index_usage(|table_id| {
            catalog
                .stored_schemas()
                .iter()
                .flat_map(|database| database.read().deprecated_tables.iter_deref())
                .find(|table| table.read().id == table_id)
                .map(|table| Arc::new(table.read().clone()))
        });
        Ok(())
    }

    /// Installs Go `executor.TakeSnapshot`, once.
    pub fn set_take_snapshot(
        &self,
        take_snapshot: std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync>,
    ) -> Result<(), std::sync::Arc<dyn Fn() -> Result<(), String> + Send + Sync>> {
        self.take_snapshot.set(take_snapshot)
    }

    /// Installs Go `Domain.etcdClient`, once.
    pub fn install_etcd_client(&self, client: Arc<tidb_pd_client::EtcdClient>) {
        let _ = self.etcd_client.set(client);
    }

    /// Go `Domain.GetEtcdClient`.
    pub fn etcd_client(&self) -> Option<Arc<tidb_pd_client::EtcdClient>> {
        self.etcd_client.get().cloned()
    }

    /// Replaces the factory's pin registry with the node-owned one the
    /// schema-sync acknowledger reads.
    #[must_use]
    pub fn with_schema_pins(mut self, pins: Arc<tidb_ddl::schema_sync::SchemaPinRegistry>) -> Self {
        self.schema_pins = pins;
        self
    }

    /// Binds the node's schema validator, so every session's commit carries
    /// Go's schema lease check.
    pub fn with_schema_validator(mut self, validator: Arc<SchemaValidator>) -> Self {
        self.schema_validator = Some(validator);
        self
    }

    pub fn with_spill_storage(
        mut self,
        spill_storage: Arc<tidb_spill_storage::SpillStorage>,
    ) -> Self {
        self.spill_storage = Some(spill_storage);
        self
    }

    /// Installs the process memory authority inherited by every session.
    #[must_use]
    pub fn with_mem_arbitrator(
        mut self,
        arbitrator: Arc<tidb_memory::MemArbitrator>,
    ) -> Self {
        self.mem_arbitrator = Some(arbitrator);
        self
    }

    /// This node's loaded tables' statistics. The consuming estimator is a
    /// parallel unit; this is the supply line it will read from.
    #[must_use]
    pub fn stats(&self) -> &Arc<SharedStats> {
        &self.stats
    }

    /// Gives this node's sessions the store's TiFlash MPP client (Go
    /// `kv.Storage.GetMPPClient()`), which serves their TiFlash reads.
    #[must_use]
    pub fn with_mpp_client(mut self, mpp: Arc<dyn tidb_txnkv::MppClient>) -> Self {
        self.mpp_client = Some(mpp);
        self
    }

    /// Gives this node's sessions the store's coprocessor client (Go
    /// `kv.Storage.GetClient()`), which serves their base-table scans, so a
    /// `WHERE` is evaluated at the region instead of after the range's bytes
    /// have crossed the network.
    ///
    /// The staged-write half is untouched: a session's uncommitted rows are
    /// merged client-side and re-tested by the same predicate (see
    /// the executor's `remote_scan`).
    #[must_use]
    pub fn with_kv_client(mut self, client: Arc<dyn tidb_txnkv::Client>) -> Self {
        self.kv_client = Some(client);
        self
    }

    /// Hands every session the store's regions (Go `kv.SplittableStore`
    /// and the region cache).
    #[must_use]
    pub fn with_region_store(
        mut self,
        regions: Arc<dyn tidb_kv::region_map::RegionStore>,
    ) -> Self {
        self.region_store = Some(regions);
        self
    }

    /// Shares the leased numeric identity with connection allocation and admission.
    #[must_use]
    pub fn with_server_identity(
        mut self,
        identity: Option<Arc<tidb_domain::server_id::ServerIdAuthority>>,
    ) -> Self {
        self.server_identity = identity;
        self
    }

    /// Installs the node's connection-ID allocator, built from the node's
    /// configuration and server identity (see
    /// [`crate::connection_ids::new_connection_id_allocator`]).
    #[must_use]
    pub fn with_connection_ids(
        mut self,
        connection_ids: Arc<dyn tidb_globalconn::Allocator + Send + Sync>,
    ) -> Self {
        self.connection_ids = connection_ids;
        self
    }

    /// Go `Domain.NextConnID` / `ReleaseConnID`, over the node's one
    /// connection-ID allocator.
    pub fn next_conn_id(
        &self,
    ) -> Arc<dyn tidb_globalconn::Allocator + Send + Sync> {
        Arc::clone(&self.connection_ids)
    }

    /// Binds server information for publication and cluster discovery.
    #[must_use]
    pub fn with_server_info(mut self, syncer: Arc<tidb_domain_serverinfo::serverinfo_syncer::Syncer>) -> Self {
        self.cluster_topology = Some(Arc::new(
            tidb_domain::cluster_topology::ClusterTopology::new(Arc::clone(&syncer), None),
        ));
        // Go `ddl.Start`: `variable.RegisterStatistics(d)`, whose id is this
        // server's.
        self.register_statistics(Arc::new(tidb_ddl::stat::DdlStatistics {
            id: syncer.get_local_server_info().static_info.id.clone(),
            catalog: Arc::clone(&self.catalog),
        }));
        self.server_info = Some(syncer);
        self
    }

    /// Go `variable.RegisterStatistics`, kept for the domain's close.
    fn register_statistics(&self, statistics: Arc<dyn tidb_variable::statusvar::Statistics>) {
        tidb_variable::statusvar::register_statistics(Arc::clone(&statistics));
        self.statistics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(statistics);
    }

    /// Shares topology with every session.
    #[must_use]
    pub fn with_cluster_topology(
        mut self,
        topology: Arc<tidb_domain::cluster_topology::ClusterTopology>,
    ) -> Self {
        self.cluster_topology = Some(topology);
        self
    }

    /// Binds Go Domain's `/tidb/stats/owner` manager.
    #[must_use]
    pub fn with_stats_owner(mut self, owner: Arc<dyn tidb_owner::Manager>) -> Self {
        self.stats_owner = Some(owner);
        self
    }

    /// Starts Go Domain's statistics-owner campaign after the factory has
    /// reached its stable process lifetime.
    pub fn campaign_stats_owner(
        self: &Arc<Self>,
        stats_lease: crate::node_config::StatsLease,
    ) -> Result<(), String> {
        let owner = self
            .stats_owner
            .as_ref()
            .ok_or_else(|| "statistics owner is not initialized".to_owned())?;
        let notifier = Arc::clone(self.ddl_notifier.get_or_init(|| {
            ddl_notifier::build_notifier(
                self,
                stats_lease.reload_interval().unwrap_or(Duration::ZERO),
            )
        }));
        owner.set_listener(Arc::new(tidb_owner::ListenersWrapper::new(vec![
            notifier as Arc<dyn tidb_owner::Listener>,
        ])));
        owner.campaign_owner(&[])
    }

    /// The boot catalog's tables this node cannot serve, with their reasons.
    #[must_use]
    pub fn boot_skipped_tables(&self) -> &[SkippedTable] {
        &self.boot_skipped
    }

    /// The schema version this node has followed the cluster to.
    #[must_use]
    pub fn followed_schema_version(&self) -> i64 {
        self.catalog.get_latest().schema_meta_version()
    }

    /// The catalog this node currently serves, for the status server's
    /// `/schema` routes. Go answers those from `GetLatest()` per request, so
    /// this reads the live pointer rather than a captured snapshot.
    #[must_use]
    pub fn catalog_snapshot(&self) -> tidb_infoschema::SessionExtendedInfoSchema {
        (*self.catalog.get_latest()).clone()
    }

    /// The process list of every connection this factory has open.
    #[must_use]
    pub fn processes(&self) -> ProcessRegistry {
        self.processes.clone()
    }
}

pub struct StatsUsageWorkers {
    pub stop: Arc<UsageWorkerStop>,
    pub threads: Vec<std::thread::JoinHandle<()>>,
    pub flush_on_drop: bool,
}

pub struct AnalyzeJobsCleanupWorker {
    stop: Arc<UsageWorkerStop>,
    thread: Option<std::thread::JoinHandle<()>>,
}

pub struct AutoAnalyzeWorker {
    stop: Arc<UsageWorkerStop>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl AutoAnalyzeWorker {
    fn start(factory: &Arc<Domain>, stats_lease: Duration) -> Self {
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let weak = Arc::downgrade(factory);
        let running = Arc::clone(&stop);
        let thread = tidb_worker_stack::sql_worker_thread("auto-analyze")
            .spawn(move || loop {
                if running.wait(stats_lease) {
                    return;
                }
                let Some(factory) = weak.upgrade() else {
                    return;
                };
                factory.handle_auto_analyze_tick(
                    tidb_vardef::RUN_AUTO_ANALYZE.load(std::sync::atomic::Ordering::SeqCst),
                    stats_lease,
                );
            })
            .expect("auto-analyze worker spawns");
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for AutoAnalyzeWorker {
    fn drop(&mut self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.stop.wake.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl AnalyzeJobsCleanupWorker {
    fn start(factory: &Arc<Domain>, stats_lease: Duration) -> Self {
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let weak = Arc::downgrade(factory);
        let running = Arc::clone(&stop);
        let thread = tidb_worker_stack::sql_worker_thread("analyze-jobs-cleanup")
            .spawn(move || {
                let gc_interval = Duration::from_secs(60 * 60);
                let cleanup_interval = stats_lease.saturating_mul(100);
                let mut next_gc = std::time::Instant::now() + gc_interval;
                let mut next_cleanup = std::time::Instant::now() + cleanup_interval;
                loop {
                    let now = std::time::Instant::now();
                    let next = next_gc.min(next_cleanup);
                    if running.wait(next.saturating_duration_since(now)) {
                        return;
                    }
                    let Some(factory) = weak.upgrade() else {
                        return;
                    };
                    let now = std::time::Instant::now();
                    if now >= next_gc {
                        next_gc = now + gc_interval;
                        if factory
                            .stats_owner
                            .as_ref()
                            .is_some_and(|owner| owner.is_owner())
                        {
                            match timestamp_before(Duration::from_secs(7 * 24 * 60 * 60)) {
                                Ok(cutoff) => {
                                    if let Err(error) = factory.delete_analyze_jobs(cutoff) {
                                        log_gc_analyze_history_failed(error);
                                    }
                                }
                                Err(error) => log_gc_analyze_history_failed(error),
                            }
                        }
                    }
                    if now >= next_cleanup {
                        next_cleanup = now + cleanup_interval;
                        match timestamp_before(Duration::from_secs(10 * 60)) {
                            Ok(cutoff) => {
                                if let Err(error) = factory
                                    .cleanup_corrupted_analyze_jobs_on_current_instance(cutoff)
                                {
                                    log_cleanup_analyze_jobs_current_instance_failed(error);
                                }
                                if factory
                                    .stats_owner
                                    .as_ref()
                                    .is_some_and(|owner| owner.is_owner())
                                {
                                    if let Err(error) = factory
                                        .cleanup_corrupted_analyze_jobs_on_dead_instances(cutoff)
                                    {
                                        tidb_logutil::bg_logger().warn(
                                            "cleanup analyze jobs on dead instances failed",
                                            &[tidb_logutil::zap_error(error)],
                                        );
                                    }
                                }
                            }
                            Err(error) => log_cleanup_analyze_jobs_current_instance_failed(error),
                        }
                    }
                }
            })
            .expect("analyze-jobs cleanup worker spawns");
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

// Go `Domain.analyzeJobsCleanupWorker`'s lines.
pub(crate) fn log_gc_analyze_history_failed(error: impl std::fmt::Display) {
    tidb_logutil::bg_logger().warn(
        "gc analyze history failed",
        &[tidb_logutil::zap_error(error)],
    );
}

pub(crate) fn log_cleanup_analyze_jobs_current_instance_failed(error: impl std::fmt::Display) {
    tidb_logutil::bg_logger().warn(
        "cleanup analyze jobs on current instance failed",
        &[tidb_logutil::zap_error(error)],
    );
}

impl Drop for AnalyzeJobsCleanupWorker {
    fn drop(&mut self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.stop.wake.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(crate) fn timestamp_before(duration: Duration) -> Result<tidb_datatype::Time, String> {
    let nanoseconds = i64::try_from(duration.as_nanos())
        .map_err(|_| "analyze-job cleanup duration is too large".to_owned())?;
    let delta = tidb_datatype::MySqlDuration::from_nanoseconds(-nanoseconds, 0)
        .map_err(|error| error.to_string())?;
    tidb_datatype::current_time::utc_now_timestamp()
        .add_duration(delta)
        .map_err(|error| error.to_string())
}

pub struct UsageWorkerStop {
    pub stopped: Mutex<bool>,
    pub wake: Condvar,
}

impl UsageWorkerStop {
    pub fn wait(&self, interval: Duration) -> bool {
        let stopped = self
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *stopped {
            return true;
        }
        *self
            .wake
            .wait_timeout(stopped, interval)
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0
    }
}

impl StatsUsageWorkers {
    fn start(factory: &Arc<Domain>, lease: crate::node_config::StatsLease) -> Self {
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let weak = Arc::downgrade(factory);
        let mut threads = vec![spawn_usage_worker(
            "index-usage-gc",
            Weak::clone(&weak),
            Arc::clone(&stop),
            Duration::from_secs(30 * 60),
            Domain::gc_index_usage,
        )];
        let mut flush_on_drop = false;
        if let crate::node_config::StatsLease::Positive(lease) = lease {
            flush_on_drop = true;
            let jitter = || {
                Duration::from_nanos(tidb_fastrand::uint64_n(
                    Duration::from_secs(60).as_nanos() as u64,
                ))
            };
            threads.push(spawn_usage_worker(
                "column-stats-usage-dump",
                Weak::clone(&weak),
                Arc::clone(&stop),
                lease.saturating_mul(100).saturating_add(jitter()),
                |factory| factory.dump_col_stats_usage_to_kv(),
            ));
            threads.push(spawn_usage_worker(
                "stats-delta-dump",
                Weak::clone(&weak),
                Arc::clone(&stop),
                lease.saturating_mul(20).saturating_add(jitter()),
                |factory| factory.dump_stats_delta_to_kv(false, &[]),
            ));
        }
        Self {
            stop,
            threads,
            flush_on_drop,
        }
    }

    /// Stops and joins every usage worker before Go's shutdown delta flush.
    pub fn stop_before_shutdown_flush(self) -> bool {
        let flush = self.flush_on_drop;
        drop(self);
        flush
    }
}

impl Drop for StatsUsageWorkers {
    fn drop(&mut self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.stop.wake.notify_all();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl Drop for Domain {
    fn drop(&mut self) {
        // Go `Domain.Close`: `close(do.exit)` stops the expensive-query handle.
        drop(
            self.expensive_query_exit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
        // Go `ddl.close` and `Domain.Close`: `variable.UnregisterStatistics`.
        for statistics in self
            .statistics
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            tidb_variable::statusvar::unregister_statistics(statistics);
        }
        // Positive-lease maintenance has already joined. For zero/negative
        // leases this is Go's quitStatsOwner lifetime instead.
        if let Some(owner) = &self.stats_owner {
            owner.close();
        }
        if let Some(refresher) = self.auto_analyze_refresher.get() {
            refresher
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .close();
        } else if let Some(queue) = self.auto_analyze_priority_queue.get() {
            queue.close();
        }
        let flush_stats = self
            .stats_usage_workers
            .take()
            .is_some_and(StatsUsageWorkers::stop_before_shutdown_flush);
        if flush_stats {
            if let Err(error) = self.dump_stats_delta_to_kv(true, &[]) {
                // Go `Handle.FlushStats`.
                tidb_stats_handle_logutil::stats_logger().warn(
                    "dump stats delta fail",
                    &[tidb_logutil::zap_error(error)],
                );
            }
        }
    }
}

#[derive(Clone)]
pub struct ClusterHistoricalInfoSchema {
    pub catalog: Arc<InfoCache>,
}

pub struct ClusterHistoricalSchemaView {
    catalog: SessionExtendedInfoSchema,
}

impl InfoSchemaView for ClusterHistoricalSchemaView {
    fn table_by_id(&self, table_id: i64) -> Option<TableMeta> {
        self.catalog
            .stored_schemas()
            .iter()
            .flat_map(|database| database.read().deprecated_tables.iter_deref())
            .find(|table| table.read().id == table_id)
            .map(|table| {
                let table = table.read();
                TableMeta {
                    id: table.id,
                    name: table.name.original().to_owned(),
                }
            })
    }

    fn find_table_by_partition_id(&self, partition_id: i64) -> Option<TableMeta> {
        self.catalog
            .stored_schemas()
            .iter()
            .flat_map(|database| database.read().deprecated_tables.iter_deref())
            .find(|table| {
                let table = table.read();
                table.get_partition_info().is_some_and(|partition| {
                    partition
                        .read()
                        .definitions
                        .snapshot()
                        .iter()
                        .any(|definition| definition.id == partition_id)
                })
            })
            .map(|table| {
                let table = table.read();
                TableMeta {
                    id: table.id,
                    name: table.name.original().to_owned(),
                }
            })
    }

    fn schema_by_table(&self, table: &TableMeta) -> Option<String> {
        self.catalog.stored_schemas().iter().find_map(|database| {
            database
                .read().deprecated_tables
                .iter_deref()
                .any(|candidate| candidate.read().id == table.id)
                .then(|| database.read().name.original().to_owned())
        })
    }
}

impl SessionInfoSchema for ClusterHistoricalInfoSchema {
    type View = ClusterHistoricalSchemaView;

    fn info_schema(&self) -> Self::View {
        ClusterHistoricalSchemaView {
            catalog: (*self.catalog.get_latest()).clone(),
        }
    }
}

pub struct ClusterHistoricalStatsHandle {
    pub transactions: Arc<dyn ClusterTransactions>,
    pub catalog: Arc<InfoCache>,
    pub global_vars: GlobalSysvars,
    /// The node's live row-ID allocators for new rows.
    pub auto_ids: Arc<dyn TableAutoIds>,
    /// Go `statsHandle.SPool()`'s owner: the domain whose system sessions
    /// run the handle's statements.
    pub domain: Weak<Domain>,
}

impl ClusterHistoricalStatsHandle {
    pub fn commit_stats_plan(
        &self,
        build: impl FnOnce(
            &mut SnapshotMetaSnapshot,
            u64,
        ) -> Result<tidb_syssession::stats_table_rows::StatsWritePlan, String>,
    ) -> Result<(), String> {
        let snapshot = self.transactions.open_snapshot("default")?;
        let read_ts = snapshot.start_ts();
        let mut meta = SnapshotMetaSnapshot::new(snapshot);
        let plan = build(&mut meta, read_ts)?;
        self.transactions
            .commit_optimistic_mutations(
                plan.mutations,
                read_ts,
                "default",
                crate::session_transaction::restricted_transaction_options(&self.global_vars),
            )
            .map_err(|error| error.message)
    }

    fn physical_table<'catalog>(
        catalog: &'catalog SessionExtendedInfoSchema,
        physical_id: i64,
    ) -> Option<tidb_model::GoShared<tidb_model::table_info::TableInfo>> {
        catalog
            .stored_schemas()
            .iter()
            .flat_map(|database| database.read().deprecated_tables.iter_deref())
            .find(|table| {
                let table = table.read();
                table.id == physical_id
                    || table.get_partition_info().is_some_and(|partition| {
                        partition
                            .read()
                            .definitions
                            .snapshot()
                            .iter()
                            .any(|definition| definition.id == physical_id)
                    })
            })
    }

    /// Go `statsReadWriter.tableHistoricalStatsToJSON`.
    fn table_historical_json(
        &self,
        physical_id: i64,
        snapshot_ts: u64,
    ) -> Result<Option<tidb_stats_util::JsonTable>, String> {
        let session_pool = self.domain.upgrade().map(|domain| domain.advanced_sys_session_pool());
        let mut table = None;
        crate::sys_session::StatsPool {
            session_pool: session_pool.as_ref(),
        }
        .call_with_sctx(
            &mut |sctx| {
                table = tidb_stats_handle_storage::json::table_historical_stats_to_json(
                    sctx,
                    physical_id,
                    snapshot_ts,
                )?;
                Ok(())
            },
            &[tidb_stats_handle_util::FLAG_WRAP_TXN],
        )
        .map_err(|error| error.message)?;
        Ok(table)
    }

    /// Go `statsReadWriter.TableStatsToJSON` (snapshot 0): the full
    /// statistics in one wrapped transaction, then the current meta counters
    /// and predicate-column usage in another.
    fn table_json(
        &self,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        physical_id: i64,
    ) -> Result<Option<tidb_stats_util::JsonTable>, String> {
        let domain = self.domain.upgrade();
        let session_pool = domain.as_ref().map(|domain| domain.advanced_sys_session_pool());
        let pool = crate::sys_session::StatsPool {
            session_pool: session_pool.as_ref(),
        };
        let cached = domain.as_ref().and_then(|domain| domain.stats.get(physical_id));
        let mut tbl = None;
        pool.call_with_sctx(
            &mut |sctx| {
                // `loadAll` reads every item, so the lease decides nothing.
                tbl = tidb_stats_handle_storage::read::table_stats_from_storage(
                    sctx,
                    0,
                    table,
                    physical_id,
                    true,
                    Duration::ZERO,
                    cached.as_deref(),
                )?;
                Ok(())
            },
            &[tidb_stats_handle_util::FLAG_WRAP_TXN],
        )
        .map_err(|error| error.message)?;
        let Some(mut tbl) = tbl else {
            return Ok(None);
        };
        let mut json = None;
        pool.call_with_sctx(
            &mut |sctx| {
                let (version, modify_count, realtime_count) =
                    tidb_stats_handle_storage::read::stats_meta_by_table_id_from_storage(
                        sctx,
                        physical_id,
                        0,
                    )?;
                tbl.version = version;
                tbl.hist_coll.modify_count = modify_count;
                tbl.hist_coll.realtime_count = realtime_count;
                // Not shown in the session, so always UTC.
                let col_stats_usage =
                    tidb_stats_handle_usage_predicatecolumn::predicate_column::load_column_stats_usage_for_table(
                        sctx,
                        &tidb_datatype::SessionTimeZone::utc(),
                        physical_id,
                    )?;
                let predicate_columns = col_stats_usage
                    .into_iter()
                    .map(|(id, usage)| {
                        Some(tidb_stats_util::JsonPredicateColumn {
                            id: id.id,
                            last_used_at: usage.last_used_at.map(|value| value.to_string()),
                            last_analyzed_at: usage.last_analyzed_at.map(|value| value.to_string()),
                        })
                    })
                    .collect();
                json = Some(
                    tidb_stats_handle_storage::json::gen_json_table_from_stats(
                        database_name,
                        table.name.lowercase(),
                        &tbl,
                        Some(predicate_columns),
                    )
                    .map_err(|error| Box::new(error) as tidb_sqlexec::SqlExecError)?,
                );
                Ok(())
            },
            &[],
        )
        .map_err(|error| error.message)?;
        Ok(json)
    }

    fn historical_json(
        &self,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        physical_id: i64,
        is_partition: bool,
    ) -> Result<Option<tidb_stats_util::JsonTable>, String> {
        if is_partition || table.get_partition_info().is_none() {
            return self.table_json(database_name, table, physical_id);
        }
        let partition = table.get_partition_info().expect("partition checked above");
        let mut partitions = std::collections::BTreeMap::new();
        for definition in partition.read().definitions.snapshot() {
            if let Some(json) = self.table_json(database_name, table, definition.id)? {
                partitions.insert(definition.name.lowercase().to_owned(), Some(json));
            }
        }
        if let Some(global) = self.table_json(database_name, table, table.id)? {
            partitions.insert(tidb_stats_util::TIDB_GLOBAL_STATS.to_owned(), Some(global));
        }
        Ok(Some(tidb_stats_util::JsonTable {
            database_name: database_name.to_owned(),
            table_name: table.name.lowercase().to_owned(),
            partitions: Some(partitions),
            ..tidb_stats_util::JsonTable::default()
        }))
    }

    fn historical_or_latest_json(
        &self,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        physical_id: i64,
        snapshot_ts: u64,
    ) -> Result<(Option<tidb_stats_util::JsonTable>, bool), String> {
        if let Some(historical) = self.table_historical_json(physical_id, snapshot_ts)? {
            return Ok((Some(historical), false));
        }
        Ok((
            self.table_json(database_name, table, physical_id)?,
            snapshot_ts != 0,
        ))
    }

    /// Pinned Go `statsReadWriter.DumpHistoricalStatsBySnapshot`, including
    /// its feature gate, per-physical-table fallback names, and dump metrics.
    fn dump_historical_stats_by_snapshot(
        &self,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        snapshot_ts: u64,
    ) -> Result<(Option<tidb_stats_util::JsonTable>, Vec<String>), String> {
        let enabled = self
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
            .map_err(|error| format!("check tidb_enable_historical_stats failed: {error:?}"))?;
        if !tidb_vardef::option_values::tidb_opt_on(&enabled) {
            return Err("tidb_enable_historical_stats should be enabled".to_owned());
        }
        let result =
            self.dump_historical_stats_by_snapshot_inner(database_name, table, snapshot_ts);
        if result.is_ok() {
            tidb_stats_handle_metrics::dump_historical_stats_success_counter().inc();
        } else {
            tidb_stats_handle_metrics::dump_historical_stats_failed_counter().inc();
        }
        result
    }

    fn dump_historical_stats_by_snapshot_inner(
        &self,
        database_name: &str,
        table: &tidb_model::table_info::TableInfo,
        snapshot_ts: u64,
    ) -> Result<(Option<tidb_stats_util::JsonTable>, Vec<String>), String> {
        let Some(partition) = table.get_partition_info() else {
            let (json, fallback) = self.historical_or_latest_json(
                database_name,
                table,
                table.id,
                snapshot_ts,
            )?;
            return Ok((
                json,
                fallback
                    .then(|| format!("{database_name}.{}", table.name.original()))
                    .into_iter()
                    .collect(),
            ));
        };

        let mut partitions = std::collections::BTreeMap::new();
        let mut fallbacks = Vec::new();
        for definition in partition.read().definitions.snapshot() {
            let (json, fallback) = self.historical_or_latest_json(
                database_name,
                table,
                definition.id,
                snapshot_ts,
            )?;
            if fallback {
                fallbacks.push(format!(
                    "{database_name}.{} {}",
                    table.name.original(),
                    definition.name.original()
                ));
            }
            partitions.insert(definition.name.lowercase().to_owned(), json);
        }
        let (global, fallback) =
            self.historical_or_latest_json(database_name, table, table.id, snapshot_ts)?;
        if fallback {
            fallbacks.push(format!("{database_name}.{} global", table.name.original()));
        }
        if global.is_some() {
            partitions.insert(tidb_stats_util::TIDB_GLOBAL_STATS.to_owned(), global);
        }
        Ok((
            Some(tidb_stats_util::JsonTable {
                database_name: database_name.to_owned(),
                table_name: table.name.lowercase().to_owned(),
                partitions: Some(partitions),
                ..tidb_stats_util::JsonTable::default()
            }),
            fallbacks,
        ))
    }
}

impl HistoricalStatsHandle for ClusterHistoricalStatsHandle {
    fn check_historical_stats_enable(&self) -> Result<bool, String> {
        self.global_vars
            .get(tidb_vardef::tidb_vars::TIDB_ENABLE_HISTORICAL_STATS)
            .map(|value| tidb_vardef::option_values::tidb_opt_on(&value))
            .map_err(|error| format!("{error:?}"))
    }

    fn record_historical_stats_to_storage(
        &self,
        database_name: &str,
        table: &TableMeta,
        physical_id: i64,
        is_partition: bool,
    ) -> Result<u64, String> {
        let catalog = self.catalog.get_latest();
        let table_info = catalog
            .stored_schemas()
            .iter()
            .flat_map(|database| database.read().deprecated_tables.iter_deref())
            .find(|candidate| candidate.read().id == table.id)
            .ok_or_else(|| format!("cannot get table by id {}", table.id))?;
        let table_info = table_info.read();
        let Some(json) = self.historical_json(
            database_name,
            &table_info,
            physical_id,
            is_partition,
        )?
        else {
            // Go `RecordHistoricalStatsToStorage`.
            tidb_stats_handle_logutil::stats_logger().warn(
                "no stats data to record",
                &[
                    tidb_log::Field::new("dbName", tidb_log::Value::Str(database_name.to_string())),
                    tidb_log::Field::new("tableName", tidb_log::Value::Str(table.name.clone())),
                ],
            );
            return Ok(0);
        };
        // Go `handleutil.CallWithSCtx(sh.statsHandle.SPool(), ...,
        // FlagWrapTxn)`.
        let pool = self
            .domain
            .upgrade()
            .map(|domain| domain.advanced_sys_session_pool())
            .ok_or_else(|| "cluster session factory is stopped".to_owned())?;
        let mut version = 0;
        tidb_stats_handle_util::call_with_sctx(
            pool.as_ref(),
            |sctx| {
                version = tidb_stats_handle_history::history_stats::record_historical_stats_to_storage(
                    sctx,
                    physical_id,
                    &json,
                )?;
                Ok(())
            },
            &[tidb_stats_handle_util::FLAG_WRAP_TXN],
        )
        .map_err(|error| error.to_string())?;
        Ok(version)
    }
}

/// The thread of Go `Domain.LoadSigningCertLoop`, stopped when the node is
/// (Go's `do.exit`).
pub(crate) struct SigningCertLoop {
    exit: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl SigningCertLoop {
    fn start() -> Self {
        let (exit, exited) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::Builder::new()
            .name("load-signing-cert-loop".to_owned())
            .spawn(move || {
                while let Err(std::sync::mpsc::RecvTimeoutError::Timeout) =
                    exited.recv_timeout(tidb_sessionstates::get_load_cert_interval())
                {
                    tidb_sessionstates::reload_signing_cert();
                }
            })
            .ok();
        Self {
            exit: Some(exit),
            thread,
        }
    }
}

impl Drop for SigningCertLoop {
    fn drop(&mut self) {
        drop(self.exit.take());
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(crate) struct HistoricalStatsRuntime {
    worker: Arc<HistoricalStatsWorker<ClusterHistoricalInfoSchema>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

pub(crate) struct ClusterHistoricalStatsMetrics;

impl HistoricalStatsMetrics for ClusterHistoricalStatsMetrics {
    fn inc_generate_failed(&self) {
        tidb_stats_handle_metrics::domain_metrics::generate_historical_stats_failed_counter().inc();
    }

    fn inc_generate_success(&self) {
        tidb_stats_handle_metrics::domain_metrics::generate_historical_stats_success_counter()
            .inc();
    }
}

impl HistoricalStatsRuntime {
    fn start(
        worker: Arc<HistoricalStatsWorker<ClusterHistoricalInfoSchema>>,
        handle: Arc<ClusterHistoricalStatsHandle>,
    ) -> Self {
        let running = Arc::clone(&worker);
        let thread = tidb_worker_stack::sql_worker_thread("historical-stats-worker")
            .spawn(move || {
                while let Some(table_id) = running.recv_historical_stats_table() {
                    if let Err(error) = running.dump_historical_stats(
                        table_id,
                        handle.as_ref(),
                        &ClusterHistoricalStatsMetrics,
                    ) {
                        // Go `Domain.StartHistoricalStatsWorker`.
                        tidb_logutil::bg_logger().warn(
                            "dump historical stats failed",
                            &[
                                tidb_logutil::zap_error(error),
                                tidb_log::Field::new("tableID", tidb_log::Value::I64(table_id)),
                            ],
                        );
                    }
                }
            })
            .expect("spawning historical statistics worker");
        Self {
            worker,
            thread: Some(thread),
        }
    }
}

impl Drop for HistoricalStatsRuntime {
    fn drop(&mut self) {
        self.worker.close_table_channel();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub(crate) fn spawn_usage_worker(
    name: &str,
    factory: Weak<Domain>,
    stop: Arc<UsageWorkerStop>,
    interval: Duration,
    action: fn(&Domain) -> Result<(), String>,
) -> std::thread::JoinHandle<()> {
    let worker_name = name.to_owned();
    let log_name = worker_name.clone();
    tidb_worker_stack::sql_worker_thread(worker_name)
        .spawn(move || {
            while !stop.wait(interval) {
                let Some(factory) = factory.upgrade() else {
                    return;
                };
                if let Err(error) = action(&factory) {
                    log_usage_worker_failure(&log_name, error);
                }
            }
        })
        .expect("spawning statistics usage worker")
}

// Go `subscriber.handle`'s best-effort DROP SCHEMA line: one entry whichever
// half of `delayedDeleteStats4PhysicalID` failed.
pub fn log_stats_gc_version_failed(physical_id: i64, error: String) {
    tidb_stats_handle_logutil::stats_logger().error(
        "Failed to update stats meta version for gc",
        &[
            tidb_log::Field::new("tableID", tidb_log::Value::I64(physical_id)),
            tidb_logutil::zap_error(error),
        ],
    );
}

// The failure line of the Go worker each usage worker ports:
// `indexUsageWorker`, `dumpColStatsUsageWorker`, `deltaUpdateTickerWorker`.
pub(crate) fn log_usage_worker_failure(name: &str, error: String) {
    let error = [tidb_logutil::zap_error(error)];
    match name {
        "index-usage-gc" => {
            tidb_stats_handle_logutil::stats_logger().error("gc index usage failed", &error);
        }
        "column-stats-usage-dump" => {
            tidb_logutil::bg_logger().warn("dump column stats usage failed", &error);
        }
        _ => tidb_logutil::bg_logger().warn("dump stats delta failed", &error),
    }
}

pub fn persist_column_stats_usage_batches(
    pending: &mut tidb_stats_handle_usage::ColumnStatsUsageDump<'_>,
    entries: &[(tidb_model::TableItemID, SystemTime)],
    mut persist: impl FnMut(&[(tidb_model::TableItemID, SystemTime)]) -> Result<(), String>,
) -> Result<(), String> {
    for batch in entries.chunks(tidb_stats_handle_usage::BATCH_INSERT_SIZE) {
        persist(batch)?;
    }
    pending.mark_persisted(entries.iter().map(|(item, _)| *item));
    Ok(())
}

pub fn system_time_timestamp(value: SystemTime) -> Result<tidb_datatype::Time, String> {
    let value: chrono::DateTime<chrono::Utc> = value.into();
    tidb_datatype::Time::from_date_checked(
        value.year(),
        i32::try_from(value.month()).expect("month fits in i32"),
        i32::try_from(value.day()).expect("day fits in i32"),
        i32::try_from(value.hour()).expect("hour fits in i32"),
        i32::try_from(value.minute()).expect("minute fits in i32"),
        i32::try_from(value.second()).expect("second fits in i32"),
        0,
        tidb_datatype::TimeType::Timestamp,
        6,
    )
    .map_err(|error| error.to_string())
}

pub(crate) fn stats_delta_table(catalog: &SessionExtendedInfoSchema, physical_id: i64) -> Option<(String, Option<i64>)> {
    for database in &catalog.stored_schemas() {
        for table in database.read().deprecated_tables.iter_deref() {
            let table = table.read();
            if table.id == physical_id {
                return Some((database.read().name.lowercase().to_owned(), None));
            }
            if table.partition.as_ref().is_some_and(|partition| {
                partition
                    .read()
                    .definitions
                    .snapshot()
                    .iter()
                    .any(|definition| definition.id == physical_id)
            }) {
                return Some((database.read().name.lowercase().to_owned(), Some(table.id)));
            }
        }
    }
    None
}

pub(crate) fn table_and_partition_ids(table: &tidb_model::table_info::TableInfo) -> Vec<i64> {
    let mut ids = vec![table.id];
    if let Some(partition) = &table.partition {
        ids.extend(
            partition
                .read()
                .definitions
                .snapshot()
                .into_iter()
                .map(|definition| definition.id),
        );
    }
    ids
}

pub fn stats_physical_ids(table: &tidb_model::table_info::TableInfo, dynamic: bool) -> Vec<i64> {
    let Some(partition) = &table.partition else {
        return vec![table.id];
    };
    let mut ids = partition
        .read()
        .definitions
        .snapshot()
        .into_iter()
        .map(|definition| definition.id)
        .collect::<Vec<_>>();
    if dynamic {
        ids.push(table.id);
    }
    ids
}

/// Go `TiDBDriver.OpenCtx`'s store-side steps: the session a server
/// connection holds, and the process authorities the driver hands it.
impl Domain {

    pub fn install_tls_manager(
        &self,
        manager: Arc<dyn tidb_sessmgr::TlsManager>,
        variables: Vec<(&'static str, String)>,
    ) {
        self.processes.set_tls_manager(manager);
        for (name, value) in variables {
            self.global_vars.set_sys_var(name, value);
        }
    }

    pub fn server_identity(&self) -> Option<Arc<tidb_domain::server_id::ServerIdAuthority>> {
        self.server_identity.clone()
    }

    /// Go `Domain.ExpensiveQueryHandle`.
    pub fn expensive_query_handle(&self) -> Arc<tidb_expensivequery::Handle> {
        Arc::clone(&self.expensive_query_handle)
    }

    pub fn session_manager(&self) -> Arc<dyn tidb_sessmgr::processinfo::SessionManager> {
        Arc::new(self.processes.clone())
    }
}

pub struct ClusterGlobalIds {
    pub transactions: Arc<dyn ClusterTransactions>,
    pub global_vars: GlobalSysvars,
}

impl std::fmt::Debug for ClusterGlobalIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterGlobalIds")
    }
}

impl GlobalIdGenerator for ClusterGlobalIds {
    fn gen_global_id(&self) -> Result<i64, tidb_driver_error::DriverError> {
        let allocate_once = || -> Result<i64, SqlQueryError> {
            let snapshot = self
                .transactions
                .open_snapshot("default")
                .map_err(SqlQueryError::unknown)?;
            let read_ts = snapshot.start_ts();
            let (id, mutation) = tidb_ddl_jobsubmit::reserve_global_id(
                &mut SnapshotMetaSnapshot::new(snapshot),
            )
            .map_err(|error| SqlQueryError::unknown(error.to_string()))?;
            self.transactions.commit_optimistic_mutations(
                vec![mutation],
                read_ts,
                "default",
                crate::session_transaction::restricted_transaction_options(&self.global_vars),
            )?;
            Ok(id)
        };
        let mut attempt = 0;
        loop {
            match allocate_once() {
                Err(error) if error.code == 9007 && attempt + 1 < tidb_txnkv::MAX_RETRY_COUNT => {
                    std::thread::sleep(tidb_txnkv::retry_backoff_delay(attempt));
                    attempt += 1;
                }
                result => {
                    return result.map_err(|error| {
                        tidb_driver_error::DriverError::Mysql(
                            tidb_terror::MysqlError::from_parts(
                                error.code,
                                error.state,
                                error.message,
                            )
                            .with_identity_opt(error.identity),
                        )
                    })
                }
            }
        }
    }
}

pub struct ClusterPriorityQueueSource {
    pub factory: Weak<Domain>,
    pub stats_lease: Duration,
    pub session_pool: Arc<crate::sys_session::DomainSysSessionPool>,
}

impl ClusterPriorityQueueSource {
    fn factory(&self) -> Result<Arc<Domain>, String> {
        self.factory
            .upgrade()
            .ok_or_else(|| "cluster session factory is stopped".to_owned())
    }

    pub fn scalar(&self, sql: &str) -> Result<Option<tidb_datatype::Datum>, String> {
        let mut value = None;
        tidb_stats_handle_util::call_with_sctx(
            self.session_pool.as_ref(),
            |context| {
                value = context
                    .scalar(sql)
                    .map_err(|error| stats_session_error(error))?;
                Ok(())
            },
            &[],
        )
        .map_err(|error| error.to_string())?;
        Ok(value)
    }
}

impl tidb_stats_handle_autoanalyze_priorityqueue::PriorityQueueSource
    for ClusterPriorityQueueSource
{
    fn next_check_version_with_offset(&self) -> u64 {
        self.factory()
            .map(|factory| {
                factory
                    .stats
                    .get_next_check_version_with_offset(self.stats_lease)
            })
            .unwrap_or(0)
    }

    fn queue_inventory(
        &self,
    ) -> Result<tidb_stats_handle_autoanalyze_priorityqueue::QueueInventory, String> {
        use tidb_stats_handle_autoanalyze_priorityqueue::{
            InventoryTable, PartitionIdAndName, PartitionPruneMode, QueueInventory,
        };

        let factory = self.factory()?;
        let catalog = factory.catalog.get_latest();
        let statistics = factory.stats.load();
        let snapshot = factory.transactions.open_snapshot("default")?;
        let current_ts = snapshot.start_ts();
        // Go `statsHandle.GetLockedTables` on a statistics session.
        let locked_table_ids = {
            let pool = factory.stats_session_pool()?;
            let mut locked = std::collections::BTreeSet::new();
            tidb_stats_handle_util::call_with_sctx(
                pool.as_ref(),
                |sctx| {
                    locked = tidb_stats_handle_lockstats::session_stats_lock::query_locked_tables(sctx)?;
                    Ok(())
                },
                &[],
            )
            .map_err(|error| error.to_string())?;
            locked.into_iter().collect::<std::collections::HashSet<i64>>()
        };
        let ratio = factory
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_RATIO)
            .map_err(|error| format!("{error:?}"))?;
        let prune_mode = factory
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_PARTITION_PRUNE_MODE)
            .map_err(|error| format!("{error:?}"))?;
        let requested_version = factory
            .global_vars
            .get(tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION)
            .map_err(|error| format!("{error:?}"))?
            .parse::<i32>()
            .map_err(|error| error.to_string())?;
        let mut tables = Vec::new();
        for database in &catalog.stored_schemas() {
            for table in database.read().deprecated_tables.iter_deref() {
                let table = table.read();
                let global_stats = statistics
                    .get(&table.id)
                    .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
                    .map(|stats| stats.as_ref().clone());
                let mut partition_stats = std::collections::HashMap::new();
                if let Some(partition) = table.get_partition_info() {
                    for definition in partition.read().definitions.snapshot() {
                        if let Some(stats) = statistics
                            .get(&definition.id)
                            .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
                        {
                            partition_stats.insert(
                                PartitionIdAndName::new(definition.name.original(), definition.id),
                                stats.as_ref().clone(),
                            );
                        }
                    }
                }
                tables.push(InventoryTable {
                    schema_name: database.read().name.original().to_owned(),
                    table: Arc::new(table.clone()),
                    global_stats,
                    partition_stats,
                });
            }
        }
        Ok(QueueInventory {
            tables,
            locked_table_ids,
            prune_mode: if prune_mode.eq_ignore_ascii_case("static") {
                PartitionPruneMode::Static
            } else {
                PartitionPruneMode::Dynamic
            },
            auto_analyze_ratio: tidb_stats_handle_autoanalyze_exec::parse_auto_analyze_ratio(
                &ratio,
            ),
            requested_version,
            current_ts,
            auto_analyze_min_count: tidb_stats::DEFAULT_AUTO_ANALYZE_MIN_COUNT,
        })
    }
}

impl tidb_stats_handle_autoanalyze_priorityqueue::AnalysisJobContext
    for ClusterPriorityQueueSource
{
    fn lookup_table(
        &self,
        table_id: i64,
    ) -> tidb_stats_handle_autoanalyze_priorityqueue::TableLookup {
        let Ok(factory) = self.factory() else {
            return tidb_stats_handle_autoanalyze_priorityqueue::TableLookup::TableMissing;
        };
        let catalog = factory.catalog.get_latest();
        for database in &catalog.stored_schemas() {
            if let Some(table) = database
                .read().deprecated_tables
                .iter_deref()
                .find(|table| table.read().id == table_id)
            {
                return tidb_stats_handle_autoanalyze_priorityqueue::TableLookup::Found {
                    schema_name: database.read().name.original().to_owned(),
                    table: Arc::new(table.read().clone()),
                };
            }
        }
        tidb_stats_handle_autoanalyze_priorityqueue::TableLookup::TableMissing
    }

    fn last_failed_analysis_duration(
        &self,
        schema: &str,
        table: &str,
        partitions: &[String],
    ) -> Result<i64, String> {
        let schema = sql_string_literal(schema);
        let table = sql_string_literal(table);
        let sql = if partitions.is_empty() {
            format!(
                "SELECT TIMESTAMPDIFF(SECOND, start_time, CURRENT_TIMESTAMP) FROM mysql.analyze_jobs WHERE table_schema = {schema} AND table_name = {table} AND state = 'failed' AND partition_name = '' ORDER BY id DESC LIMIT 1"
            )
        } else {
            let partitions = partitions
                .iter()
                .map(|partition| sql_string_literal(partition))
                .collect::<Vec<_>>()
                .join(",");
            format!(
                "SELECT MIN(TIMESTAMPDIFF(SECOND, aj.start_time, CURRENT_TIMESTAMP)) AS min_duration FROM (SELECT MAX(id) AS max_id FROM mysql.analyze_jobs WHERE table_schema = {schema} AND table_name = {table} AND state = 'failed' AND partition_name IN ({partitions}) GROUP BY partition_name) AS latest_failures JOIN mysql.analyze_jobs aj ON aj.id = latest_failures.max_id"
            )
        };
        match self.scalar(&sql)? {
            None | Some(tidb_datatype::Datum::Null) => Ok(
                tidb_stats_handle_autoanalyze_priorityqueue::last_failed_analysis_duration(None),
            ),
            Some(tidb_datatype::Datum::Int(seconds)) => Ok(
                tidb_stats_handle_autoanalyze_priorityqueue::last_failed_analysis_duration(Some(
                    seconds,
                )),
            ),
            Some(value) => Err(format!("invalid last failed analysis duration {value:?}")),
        }
    }

    fn average_analysis_duration(
        &self,
        schema: &str,
        table: &str,
        partitions: &[String],
    ) -> Result<i64, String> {
        let schema = sql_string_literal(schema);
        let table = sql_string_literal(table);
        let predicate = if partitions.is_empty() {
            "partition_name = ''".to_owned()
        } else {
            let partitions = partitions
                .iter()
                .map(|partition| sql_string_literal(partition))
                .collect::<Vec<_>>()
                .join(",");
            format!("partition_name IN ({partitions})")
        };
        let sql = format!(
            "SELECT AVG(TIMESTAMPDIFF(SECOND, start_time, end_time)) AS avg_duration FROM (SELECT start_time, end_time FROM mysql.analyze_jobs WHERE table_schema = {schema} AND table_name = {table} AND state = 'finished' AND fail_reason IS NULL AND {predicate} ORDER BY id DESC LIMIT 5) AS recent_analyses"
        );
        match self.scalar(&sql)? {
            None | Some(tidb_datatype::Datum::Null) => {
                Ok(tidb_stats_handle_autoanalyze_priorityqueue::average_analysis_duration(None))
            }
            Some(value) => {
                let seconds = value.to_f64().map_err(|error| error.to_string())?.value;
                Ok(
                    tidb_stats_handle_autoanalyze_priorityqueue::average_analysis_duration(Some(
                        seconds,
                    )),
                )
            }
        }
    }

    fn auto_analyze(
        &self,
        stats_version: i32,
        need_version_rewrite_warning: bool,
        sql: &str,
        arguments: &[String],
    ) -> bool {
        let mut analyzed = false;
        tidb_stats_handle_util::call_with_sctx(
            self.session_pool.as_ref(),
            |context| {
                // Go hands the stats handle `Domain.NextConnID` and
                // `ReleaseConnID` as its auto-analyze process-ID generator, so
                // the ID names this node's server ID and `KILL` routes it here.
                let factory = self.factory().map_err(stats_session_error)?;
                let ids = factory.next_conn_id();
                let released = Arc::clone(&ids);
                let generator = tidb_stats_handle_util::Generator::new(
                    move || ids.next_id(),
                    move |id| released.release(id),
                );
                let (track, untrack) = system_process_trackers(factory.processes());
                let arguments = arguments
                    .iter()
                    .map(|argument| tidb_sqlescape::SqlArg::from(argument.as_str()))
                    .collect::<Vec<_>>();
                analyzed = tidb_stats_handle_autoanalyze_exec::auto_analyze(
                    context,
                    &generator,
                    track,
                    untrack,
                    stats_version,
                    need_version_rewrite_warning,
                    sql,
                    &arguments,
                );
                Ok(())
            },
            &[],
        )
        .is_ok()
            && analyzed
    }

    fn auto_analyze_partition_batch_size(&self) -> usize {
        self.factory()
            .ok()
            .and_then(|factory| {
                factory
                    .global_vars
                    .get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_PARTITION_BATCH_SIZE)
                    .ok()
            })
            .and_then(|value| value.parse().ok())
            .unwrap_or(tidb_vardef::defaults::DEF_TIDB_AUTO_ANALYZE_PARTITION_BATCH_SIZE as usize)
    }
}

struct ClusterExternalTimestamp(Arc<dyn ClusterTransactions>);
impl std::fmt::Debug for ClusterExternalTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ClusterExternalTimestamp")
    }
}

/// Go `kv.RunInNewTxn(sctx.GetStore(), ..., m.GenGlobalID())`: one id from
/// meta's global allocator, in its own transaction on the session's store
/// (the local temporary table DDL's ids, `newTemporaryTableFromTableInfo`).
pub trait GlobalIdGenerator: Send + Sync {
    /// One id from meta's global allocator.
    fn gen_global_id(&self) -> Result<i64, tidb_driver_error::DriverError>;
}

impl tidb_variable::vars::ExternalTimestampProvider for ClusterExternalTimestamp {
    fn get(&self) -> Result<u64, String> {
        self.0.external_timestamp(None)
    }
    fn set(&self, ts: u64) -> Result<(), String> {
        self.0.external_timestamp(Some(ts)).map(|_| ())
    }
}

impl Domain {
    /// This node's DDL notifier, owning: what Go's
    /// `statstestutil.HandleNextDDLEventWithTxn` runs until the statistics
    /// subscriber has handled every pending event. The caller stops it.
    pub fn owned_ddl_notifier_for_test(self: &Arc<Self>) -> Arc<tidb_ddl_notifier::DdlNotifier> {
        let notifier = ddl_notifier::build_notifier(self, std::time::Duration::ZERO);
        tidb_owner::Listener::on_become_owner(notifier.as_ref());
        notifier
    }
}

/// Go `Builder.factory` as `tableFromMeta` uses it: an internal session from
/// the domain's pool, never returned (Go keeps it with the cached table), as
/// its SQL executor. The session is taken at the executor's first statement:
/// images are built before the domain that owns the pool is running.
fn cached_table_session_factory(
    domain: Arc<std::sync::OnceLock<Weak<Domain>>>,
) -> Box<crate::cluster_session::CachedTableSessionFactory> {
    Box::new(move || {
        Ok(Arc::new(CachedTableSessionExecutor {
            domain: Arc::clone(&domain),
            session: std::sync::OnceLock::new(),
        }) as Arc<dyn tidb_sqlexec::SqlExecutor>)
    })
}

/// One cached table's internal session, taken from the pool at first use.
struct CachedTableSessionExecutor {
    domain: Arc<std::sync::OnceLock<Weak<Domain>>>,
    session: std::sync::OnceLock<Arc<dyn tidb_sqlexec::SqlExecutor>>,
}

impl CachedTableSessionExecutor {
    fn executor(&self) -> tidb_sqlexec::Result<&Arc<dyn tidb_sqlexec::SqlExecutor>> {
        if let Some(executor) = self.session.get() {
            return Ok(executor);
        }
        let domain = self
            .domain
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| crate::sys_session::stats_session_error("the domain is not running"))?;
        let session = domain.advanced_sys_session_pool().get()?;
        let executor = session.with_session_context(|context| {
            Ok::<_, tidb_syssession::SysSessionError>(tidb_syssession::SessionContext::sql_executor(
                context,
            ))
        })?;
        Ok(self.session.get_or_init(|| executor))
    }
}

impl tidb_sqlexec::SqlExecutor for CachedTableSessionExecutor {
    fn execute(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
    ) -> tidb_sqlexec::Result<Vec<Box<dyn tidb_sqlexec::RecordSet>>> {
        self.executor()?.execute(context, sql)
    }

    fn execute_internal(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        sql: &str,
        arguments: &[tidb_sqlescape::SqlArg<'_>],
    ) -> tidb_sqlexec::Result<Option<Box<dyn tidb_sqlexec::RecordSet>>> {
        self.executor()?.execute_internal(context, sql, arguments)
    }

    fn execute_stmt(
        &self,
        context: &dyn tidb_sqlexec::ExecutionContext,
        statement: &tidb_ast::Stmt,
    ) -> tidb_sqlexec::Result<Option<Box<dyn tidb_sqlexec::RecordSet>>> {
        self.executor()?.execute_stmt(context, statement)
    }
}

/// Go `sysvar.go`'s GC variables: their GetGlobal / SetGlobal read and write
/// the `tikv_gc_*` rows of `mysql.tidb` (`getTiDBTableValue` /
/// `setTiDBTableValue`), which the GC worker reads.
fn register_gc_tidb_table_hooks(globals: &tidb_domain::global_sysvars::GlobalSysvars, domain: &Weak<Domain>) {
    use tidb_vardef::tidb_vars::{
        TIDB_GC_CONCURRENCY, TIDB_GC_ENABLE, TIDB_GC_LIFETIME, TIDB_GC_RUN_INTERVAL,
        TIDB_GC_SCAN_LOCK_MODE,
    };
    let tidb_table = Arc::new(TiDBTableValues(domain.clone()));
    let getter = |key: &'static str, default: &'static str| {
        let tidb_table = Arc::clone(&tidb_table);
        Arc::new(move || tidb_table.get_tidb_table_value_or(key, default))
            as tidb_domain::global_sysvars::GetGlobalHook
    };
    let setter = |key: &'static str, comment: &'static str| {
        let tidb_table = Arc::clone(&tidb_table);
        Arc::new(move |value: &str| tidb_table.set_tidb_table_value(key, value, comment))
            as tidb_domain::global_sysvars::SetGlobalHook
    };
    globals.register_get_global_hook(TIDB_GC_ENABLE, getter("tikv_gc_enable", "ON"));
    globals.register_set_global_hook(TIDB_GC_ENABLE, setter("tikv_gc_enable", "Current GC enable status"));
    globals.register_get_global_hook(TIDB_GC_RUN_INTERVAL, getter("tikv_gc_run_interval", "10m0s"));
    globals.register_set_global_hook(
        TIDB_GC_RUN_INTERVAL,
        setter("tikv_gc_run_interval", "GC run interval, at least 10m, in Go format."),
    );
    globals.register_get_global_hook(TIDB_GC_LIFETIME, getter("tikv_gc_life_time", "10m0s"));
    globals.register_set_global_hook(
        TIDB_GC_LIFETIME,
        setter(
            "tikv_gc_life_time",
            "All versions within life time will not be collected by GC, at least 10m, in Go format.",
        ),
    );
    globals.register_get_global_hook(TIDB_GC_CONCURRENCY, {
        let tidb_table = Arc::clone(&tidb_table);
        Arc::new(move || {
            if tidb_table.get_tidb_table_value_or("tikv_gc_auto_concurrency", "ON") == "ON" {
                // The convention for "AUTO".
                return "-1".to_owned();
            }
            tidb_table.get_tidb_table_value_or("tikv_gc_concurrency", "-1")
        })
    });
    globals.register_set_global_hook(TIDB_GC_CONCURRENCY, {
        let tidb_table = Arc::clone(&tidb_table);
        Arc::new(move |value: &str| {
            let auto_concurrency = if value == "-1" { "ON" } else { "OFF" };
            // Both the auto switch and the concurrency.
            tidb_table.set_tidb_table_value(
                "tikv_gc_auto_concurrency",
                auto_concurrency,
                "Let TiDB pick the concurrency automatically. If set false, tikv_gc_concurrency will be used",
            )?;
            tidb_table.set_tidb_table_value(
                "tikv_gc_concurrency",
                value,
                "How many goroutines used to do GC parallel, [1, 256], default 2",
            )
        })
    });
    globals.register_get_global_hook(TIDB_GC_SCAN_LOCK_MODE, getter("tikv_gc_scan_lock_mode", "LEGACY"));
    globals.register_set_global_hook(
        TIDB_GC_SCAN_LOCK_MODE,
        setter("tikv_gc_scan_lock_mode", "Mode of scanning locks, \"physical\" or \"legacy\""),
    );
}

/// Go `GlobalVarAccessor.GetTiDBTableValue` / `SetTiDBTableValue`, on one of
/// the domain's internal sessions.
struct TiDBTableValues(Weak<Domain>);

impl TiDBTableValues {
    fn with_session<T>(
        &self,
        run: impl FnOnce(&dyn crate::sys_session::DomainSysSession) -> Result<T, String>,
    ) -> Result<T, String> {
        let domain = self.0.upgrade().ok_or_else(|| "the domain is not running".to_owned())?;
        domain
            .advanced_sys_session_pool()
            .with_session(|lease| {
                lease.with_session_context(|context| {
                    run(context).map_err(tidb_syssession::SysSessionError::new)
                })
            })
            .map_err(|error| error.to_string())
    }

    /// Go `getTiDBTableValue`: `true`/`false` read as ON/OFF; the default
    /// when the row cannot be read.
    fn get_tidb_table_value_or(&self, name: &str, default: &str) -> String {
        let sql = tidb_sqlescape::escape_sql_text(
            "SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME=%?",
            &[tidb_sqlescape::SqlArg::String(name.as_bytes())],
        );
        let value = sql
            .map_err(|error| error.to_string())
            .and_then(|sql| {
                self.with_session(|session| session.query_internal(&sql))
            })
            .ok()
            .and_then(|rows| rows.into_iter().next())
            .and_then(|row| row.into_iter().next())
            .and_then(|value| match value {
                tidb_datatype::Datum::String(text) => {
                    Some(String::from_utf8_lossy(text.bytes()).into_owned())
                }
                tidb_datatype::Datum::Bytes(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
                _ => None,
            });
        match value {
            Some(value) if value.eq_ignore_ascii_case("true") => "ON".to_owned(),
            Some(value) if value.eq_ignore_ascii_case("false") => "OFF".to_owned(),
            Some(value) => value,
            None => default.to_owned(),
        }
    }

    /// Go `setTiDBTableValue`: ON/OFF stored as `true`/`false`.
    fn set_tidb_table_value(&self, name: &str, value: &str, comment: &str) -> Result<(), String> {
        let value = if value.eq_ignore_ascii_case("ON") {
            "true"
        } else if value.eq_ignore_ascii_case("OFF") {
            "false"
        } else {
            value
        };
        let sql = tidb_sqlescape::escape_sql_text(
            "REPLACE INTO mysql.tidb (variable_name, variable_value, comment) VALUES (%?, %?, %?)",
            &[
                tidb_sqlescape::SqlArg::String(name.as_bytes()),
                tidb_sqlescape::SqlArg::String(value.as_bytes()),
                tidb_sqlescape::SqlArg::String(comment.as_bytes()),
            ],
        )
        .map_err(|error| error.to_string())?;
        self.with_session(|session| session.execute_internal_write(&sql).map(drop))
    }
}
