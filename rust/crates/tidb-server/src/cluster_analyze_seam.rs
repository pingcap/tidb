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
//! This is [`ClusterDdl`]'s shape rather than
//! [`ClusterAccountWriter`]'s, and the difference says something real. An
//! account statement runs *through the session driver* against a scratch
//! registry, because the driver is what knows that `GRANT SELECT ON *.*`
//! means. Nothing in the driver knows what `ANALYZE TABLE` means -- the
//! statement's whole meaning is reading rows and building histograms, which
//! is [`tidb_exec::cluster_analyze`]'s -- so the seam is one method that runs
//! it, exactly as the DDL seam is one method that applies a catalog change.
//!
//! # The order, and what a failure leaves behind
//!
//! 1. one transaction, one `start_ts`;
//! 2. read the catalog, the table's previous `mysql.stats_meta` row, and
//!    every one of the table's rows, all at that timestamp;
//! 3. build the histograms and plan the `mysql.stats_*` mutations;
//! 4. commit;
//! 5. only then reload this node's own [`SharedStats`], so its planner
//!    estimates from what it just wrote.
//!
//! Nothing the node serves from is touched until the 2PC commits. A statement
//! that cannot analyze the table -- a partitioned one, a prefix index -- never
//! reaches storage; a commit rejected by a write conflict with a Go TiDB's
//! own concurrent `ANALYZE` leaves the cluster's statistics exactly as that
//! other node wrote them, and the client is told. There is no rollback path
//! to get wrong.
//!
//! Step 5 is deliberately not a failure of the statement. The rows are
//! durable; a node that could not refresh its own copy is a node whose next
//! reload tick finds them, which is precisely how
//! [`RealClusterDdl::refresh_catalog`] treats the same situation.
//!
//! [`ClusterDdl`]: crate::cluster_session_node::ClusterDdl
//! [`ClusterAccountWriter`]: crate::cluster_account_seam::ClusterAccountWriter
//! [`RealClusterDdl::refresh_catalog`]: crate::cluster_session_node::RealClusterDdl
//! [`SharedStats`]: tidb_exec::stats_watch::SharedStats

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use tidb_exec::cluster_analyze::AnalyzeStatement;
use tidb_exec::cluster_catalog::load_cluster_catalog;
use tidb_exec::cluster_stats_load::column_types_of;
use tidb_exec::real_tikv_analyze::{commit_cluster_analyze, ClusterAnalyzeReport};
use tidb_exec::real_tikv_catalog::TransactionMetaSnapshot;
use tidb_exec::real_tikv_stats::load_stats_snapshot_from_cluster;
use tidb_exec::stats_watch::SharedStats;
use tidb_txnkv::transaction::RealOptimisticTransactionOpener;

/// One table's physical ID paired with the declared types its stored
/// histogram bounds decode against -- the shape
/// [`load_stats_snapshot_from_cluster`] takes.
type StatsTarget = (i64, BTreeMap<i64, tidb_datatype::FieldType>);

/// This node's one route to the cluster's stored statistics.
///
/// The seam exists so the routing decision -- what a session does with an
/// `ANALYZE TABLE`, and what happens when it fails -- is exercised without a
/// cluster. The production implementation is [`RealClusterAnalyze`].
pub trait ClusterAnalyze: Send + Sync {
    /// Analyzes one table and stores its statistics.
    fn execute(&self, statement: &AnalyzeStatement) -> Result<ClusterAnalyzeReport, String>;
}

/// The production analyzer: one real transaction per table, the optimistic
/// 2PC, then this node's own statistics reload.
pub struct RealClusterAnalyze {
    opener: Arc<RealOptimisticTransactionOpener>,
    /// This node's LIVE statistics -- the snapshot every session's estimator
    /// reads. Republished only after a commit.
    stats: Arc<SharedStats>,
    timeout: Duration,
}

impl RealClusterAnalyze {
    /// Binds the analyzer to an already-connected authority and the live
    /// statistics a successful analysis republishes into.
    #[must_use]
    pub fn new(
        opener: Arc<RealOptimisticTransactionOpener>,
        stats: Arc<SharedStats>,
        timeout: Duration,
    ) -> Self {
        Self {
            opener,
            stats,
            timeout,
        }
    }

    /// Reloads every table's statistics into this node's live snapshot.
    ///
    /// Whole-snapshot rather than one-table, because that is the only shape
    /// [`SharedStats`] publishes, and because a snapshot assembled from two
    /// timestamps is exactly what
    /// [`load_stats_snapshot_from_cluster`] exists to prevent. A failure is
    /// a warning: the rows are durable and the reload tick will find them.
    fn refresh_stats(&self) {
        let targets = match self.stats_targets() {
            Ok(targets) => targets,
            Err(error) => return warn_reload_failed(&error),
        };
        match load_stats_snapshot_from_cluster(&self.opener, self.timeout, &targets) {
            Ok(snapshot) => {
                let receipt = tidb_exec::stats_watch::receipt_of(&snapshot);
                self.stats.store(snapshot);
                eprintln!(
                    "{{\"event\":\"stats_reloaded_after_analyze\",\"loaded\":{},\"pseudo\":{}}}",
                    receipt.loaded, receipt.pseudo
                );
            }
            Err(error) => warn_reload_failed(&error.to_string()),
        }
    }

    /// Every non-system table in the cluster, with the column types its
    /// stored bounds decode against.
    fn stats_targets(&self) -> Result<Vec<StatsTarget>, String> {
        let mut transaction = self
            .opener
            .begin_read_only()
            .map_err(|error| error.to_string())?;
        let targets = {
            let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, self.timeout);
            let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
            catalog
                .databases
                .iter()
                .flat_map(|database| database.tables.iter())
                .map(|table| (table.id, column_types_of(table)))
                .collect()
        };
        transaction
            .finish_without_writes()
            .map_err(|error| error.to_string())?;
        Ok(targets)
    }
}

impl ClusterAnalyze for RealClusterAnalyze {
    fn execute(&self, statement: &AnalyzeStatement) -> Result<ClusterAnalyzeReport, String> {
        let report = commit_cluster_analyze(&self.opener, statement, self.timeout)?;
        self.refresh_stats();
        Ok(report)
    }
}

fn warn_reload_failed(error: &str) {
    eprintln!(
        "{{\"event\":\"stats_reload_after_analyze_failed\",\"level\":\"warning\",\"error\":{}}}",
        serde_json::to_string(error).unwrap_or_else(|_| "\"unprintable\"".to_owned())
    );
}
