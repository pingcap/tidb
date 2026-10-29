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

//! The route an `ANALYZE TABLE` takes when the statistics live in the
//! cluster, not in this process.
//!
//! This is `ClusterDdl`'s shape rather than
//! `ClusterAccountWriter`'s, and the difference says something real. An
//! account statement runs *through the session driver* against a scratch
//! registry, because the driver is what knows that `GRANT SELECT ON *.*`
//! means. Nothing in the driver knows what `ANALYZE TABLE` means -- the
//! statement's whole meaning is reading rows and building histograms, which
//! is [`crate::cluster_analyze`]'s -- so the seam is one method that runs
//! it, exactly as the DDL seam is one method that applies a catalog change.
//!
//! # The order, and what a failure leaves behind
//!
//! 1. resolve the catalog and previous statistics at the sampling TSO;
//! 2. collect regional samples using Go's configured RC/SI analyze policy;
//! 3. build histograms and persist `mysql.stats_*` in a save transaction;
//! 4. reload this node's own [`SharedStats`] after the commit.
//!
//! Nothing the node serves from is touched until the 2PC commits. A statement
//! rejected before execution never reaches storage; a commit rejected by a
//! write conflict with a Go TiDB's own concurrent `ANALYZE` leaves the
//! cluster's statistics exactly as that other node wrote them, and the client
//! is told. There is no rollback path to get wrong.
//!
//! Step 4 is deliberately not a failure of the statement. The rows are
//! durable; a node that could not refresh its own copy is a node whose next
//! reload tick finds them, which is precisely how
//! `RealClusterDdl::refresh_catalog` treats the same situation.
//!
//! `RealClusterDdl::refresh_catalog`: crate::domain::cluster_session_node::RealClusterDdl
//! [`SharedStats`]: tidb_stats_handle_syncload::stats_watch::SharedStats

use std::sync::Arc;
use std::time::Duration;
use tidb_txnkv::transaction::StoreClient;

use crate::cluster_analyze::AnalyzeStatement;
use tidb_infoschema::cluster_catalog::load_cluster_catalog;
use tidb_stats_handle_storage::cluster_stats_load::ClusterStatsLoader;
use crate::real_tikv_analyze::{
    commit_cluster_analyze, record_global_history_enabled, resolve_cluster_analyze_statement,
    save_cluster_analyze_options, AnalyzeJobLifecycle, ApproximateTableCountProvider,
    ClusterAnalyzeReport,
};
use tidb_infoschema_issyncer::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_stats_handle_storage::real_tikv_load_stats::{
    commit_cluster_load_stats, ClusterLoadStatsCommitError, ClusterLoadStatsReport,
};
use tidb_stats_handle_syncload::real_tikv_stats::{update_stats_cache_from_cluster, StatsTarget};
use tidb_stats_handle_syncload::stats_watch::SharedStats;
use tidb_txnkv::transaction::RealOptimisticTransactionOpener;
use tidb_sqlkiller::SqlKiller;

use crate::real_tikv_analyze::cluster_analyze_error;
use tidb_store_driver::sql_query_error::SqlQueryError;

/// This node's one route to the cluster's stored statistics.
///
/// The seam exists so the routing decision -- what a session does with an
/// `ANALYZE TABLE`, and what happens when it fails -- is exercised without a
/// cluster. The production implementation is [`RealClusterAnalyze`].
pub trait ClusterAnalyze: Send + Sync {
    /// Analyzes one table and stores its statistics.
    fn execute(
        &self,
        statement: &AnalyzeStatement,
        resource_group: &str,
        approximate_counts: &dyn ApproximateTableCountProvider,
        killer: &SqlKiller,
        historical_stats_enabled: &dyn Fn() -> bool,
        jobs: &dyn AnalyzeJobLifecycle,
    ) -> Result<ClusterAnalyzeReport, SqlQueryError>;

    /// Go `AnalyzeExec.Next`'s closing `statsHandle.Update(ctx, infoSchema,
    /// tableAndPartitionIDs...)`: once per statement, after every table's
    /// results are saved, over the physical ids of all of them. Its failure
    /// is the statement's error.
    fn update_stats_cache(&self, _physical_ids: Vec<i64>) -> Result<(), SqlQueryError> {
        Ok(())
    }

    /// Loads one client-transferred JSON statistics dump into cluster storage.
    fn load_stats(
        &self,
        _json: &tidb_stats::JsonTable,
        _historical_stats_enabled: bool,
    ) -> Result<ClusterLoadStatsReport, SqlQueryError> {
        Err(SqlQueryError::unknown(
            "this statistics writer does not support LOAD STATS",
        ))
    }

    /// Go `executeDropStats`: soft-deletes every named physical id's
    /// statistics from storage, then refreshes this node's cache.
    fn drop_stats(&self, _physical_ids: &[i64]) -> Result<(), SqlQueryError> {
        Err(SqlQueryError::unknown(
            "this statistics writer does not support DROP STATS",
        ))
    }
}

/// The production analyzer: one real transaction per table, the optimistic
/// 2PC, then this node's own statistics reload.
pub struct RealClusterAnalyze<S = tikv_client::PdRpcClient>
where
    S: StoreClient,
{
    opener: Arc<RealOptimisticTransactionOpener<S>>,
    /// This node's LIVE statistics -- the snapshot every session's estimator
    /// reads. Republished only after a commit.
    stats: Arc<SharedStats>,
    timeout: Duration,
    stats_lease: Duration,
    /// The node's live row-ID allocators, which the statistics rows this
    /// analyzer writes allocate from (Go's internal INSERT).
    row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
}

impl<S> RealClusterAnalyze<S>
where
    S: StoreClient,
{
    /// Binds the analyzer to an already-connected authority and the live
    /// statistics a successful analysis republishes into.
    #[must_use]
    pub fn new(
        opener: Arc<RealOptimisticTransactionOpener<S>>,
        stats: Arc<SharedStats>,
        timeout: Duration,
        stats_lease: Duration,
        row_ids: Arc<dyn tidb_table::table_from_meta::TableAutoIds>,
    ) -> Self {
        Self {
            opener,
            stats,
            timeout,
            stats_lease,
            row_ids,
        }
    }

    /// Runs Go's targeted cache update after ANALYZE or LOAD STATS.
    fn refresh_stats(&self, physical_ids: Vec<i64>) -> Result<(), String> {
        let (targets, loader) = self.stats_targets()?;
        match update_stats_cache_from_cluster(
            &self.opener,
            self.timeout,
            &targets,
            &self.stats,
            &loader,
            self.stats_lease,
            physical_ids,
        ) {
            Ok(_) => {
                let receipt = self.stats.receipt();
                // Not log content (Go logs no success here):
                // rust/scripts/run-realtikv-analyze.sh reads this JSON line.
                eprintln!(
                    "{{\"event\":\"stats_reloaded_after_analyze\",\"loaded\":{},\"pseudo\":{}}}",
                    receipt.loaded, receipt.pseudo
                );
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    /// Every non-system table in the cluster, with the column types its
    /// stored bounds decode against.
    fn stats_targets(&self) -> Result<(Vec<StatsTarget>, ClusterStatsLoader), String> {
        let mut transaction = self
            .opener
            .begin_read_only()
            .map_err(|error| error.to_string())?;
        let result = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
            let loader = ClusterStatsLoader::locate(&catalog).map_err(|error| error.to_string())?;
            let targets = catalog
                .databases
                .iter()
                .flat_map(|database| database.tables.iter())
                .flat_map(StatsTarget::for_table)
                .collect();
            (targets, loader)
        };
        transaction
            .finish_without_writes()
            .map_err(|error| error.to_string())?;
        Ok(result)
    }
}

impl<S> ClusterAnalyze for RealClusterAnalyze<S>
where
    S: StoreClient,
{
    fn execute(
        &self,
        statement: &AnalyzeStatement,
        resource_group: &str,
        approximate_counts: &dyn ApproximateTableCountProvider,
        killer: &SqlKiller,
        historical_stats_enabled: &dyn Fn() -> bool,
        jobs: &dyn AnalyzeJobLifecycle,
    ) -> Result<ClusterAnalyzeReport, SqlQueryError> {
        let statement = resolve_cluster_analyze_statement(
            &self.opener,
            statement,
            self.timeout,
            resource_group,
            approximate_counts,
            &self.stats,
        )
        .map_err(cluster_analyze_error)?;
        let logical_table_id = statement.logical_table_id();
        let record_history = || historical_stats_enabled();
        let record_global_history = || {
            let initialized = self
                .stats
                .load()
                .get(&logical_table_id)
                .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
                .is_some_and(|table| table.is_initialized());
            record_global_history_enabled(record_history(), initialized)
        };
        let mut report = commit_cluster_analyze(
            &self.opener,
            &statement,
            resource_group,
            self.timeout,
            self.stats_lease,
            killer,
            &record_history,
            &record_global_history,
            jobs,
            &self.row_ids,
        )
        .map_err(cluster_analyze_error)?;
        if let Err(error) = save_cluster_analyze_options(
            &self.opener,
            &statement,
            self.timeout,
            &self.row_ids,
        ) {
            report.option_save_warning = Some(error.to_string());
        }
        Ok(report)
    }

    fn update_stats_cache(&self, physical_ids: Vec<i64>) -> Result<(), SqlQueryError> {
        self.refresh_stats(physical_ids)
            .map_err(SqlQueryError::unknown)
    }

    fn load_stats(
        &self,
        json: &tidb_stats::JsonTable,
        historical_stats_enabled: bool,
    ) -> Result<ClusterLoadStatsReport, SqlQueryError> {
        let current = self.stats.load();
        // Go `RecordHistoricalStatsMeta` through the stats handle; failures
        // are logged and ignored there, and ignored here.
        let record_history = |table_id: i64, version: u64, source: &str| {
            let _ = tidb_stats_handle_history::history_stats::record_historical_stats_meta(
                &self.opener,
                table_id,
                version,
                source,
                self.timeout,
                Some(self.row_ids.as_ref()),
            );
        };
        let report = commit_cluster_load_stats(
            &self.opener,
            json,
            self.timeout,
            historical_stats_enabled.then_some(&record_history as &(dyn Fn(i64, u64, &str) + Sync)),
            |table_id| {
                current
                    .get(&table_id)
                    .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)
                    .is_some()
            },
            0,
            &self.row_ids,
        )
        .map_err(cluster_load_stats_error)?;
        // As after ANALYZE, pinned Go returns `StatsHandle.Update` failure
        // from `LoadStatsFromJSON`; durable writes do not turn this into a
        // warning.
        self.refresh_stats(report.physical_ids().to_vec())
            .map_err(SqlQueryError::unknown)?;
        Ok(report)
    }

    fn drop_stats(&self, physical_ids: &[i64]) -> Result<(), SqlQueryError> {
        tidb_stats_handle_storage::real_tikv_load_stats::commit_cluster_drop_stats(
            &self.opener,
            physical_ids,
            self.timeout,
            Some(self.row_ids.as_ref()),
        )
        .map_err(cluster_load_stats_error)?;
        // Go `executeDropStats` ends in `StatsHandle.Update`, and returns its
        // failure: the rows are durable either way.
        self.refresh_stats(physical_ids.to_vec())
            .map_err(SqlQueryError::unknown)
    }
}

fn cluster_load_stats_error(error: ClusterLoadStatsCommitError) -> SqlQueryError {
    match error {
        ClusterLoadStatsCommitError::Undetermined(_) => SqlQueryError::result_undetermined(),
        ClusterLoadStatsCommitError::Commit(error) => tidb_store_driver::sql_query_error::lock_sql_error(&error),
        ClusterLoadStatsCommitError::Other(detail) => SqlQueryError::unknown(detail),
    }
}
