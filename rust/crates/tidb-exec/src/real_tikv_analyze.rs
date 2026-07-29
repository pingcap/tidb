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

//! One `ANALYZE TABLE`, as one real transaction.
//!
//! Everything the statement does happens at one `start_ts`: the catalog it
//! resolves the table in, the previous statistics it reads its sample rate
//! from, the rows it samples, and the `mysql.stats_*` rows it stores. That is
//! not tidiness -- it is the correctness argument. `mysql.stats_meta.count`
//! and the histograms must describe the *same* rows, and a second timestamp
//! anywhere in between would let a concurrent `INSERT` land between the count
//! and the buckets, giving the planner a row count its histogram cannot
//! account for.
//!
//! The stamp on every stored row is that same `start_ts`, which is also what
//! makes a Go TiDB's concurrent `ANALYZE` a plain write conflict at prewrite:
//! both write the same keys, and exactly one of them commits.

use std::time::Duration;

use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, RealOptimisticTransactionOpener, MAX_OPTIMISTIC_MUTATIONS,
    MAX_OPTIMISTIC_TRANSACTION_BYTES,
};

use crate::cluster_analyze::{analyze_table, lower_analyze, AnalyzeError, AnalyzeStatement};
use crate::cluster_catalog::load_cluster_catalog;
use crate::cluster_stats_load::ClusterStatsLoader;
use crate::cluster_stats_write::plan_stats_write;
use crate::mysql_bootstrap::utc_now_timestamp;
use crate::mysql_system_tables::SystemTableError;
use crate::real_tikv_catalog::TransactionMetaSnapshot;

/// Parses and admits one text-protocol statement as an `ANALYZE TABLE`.
///
/// `None` means the statement is not one -- including when it does not parse
/// at all, so an unparsable statement is still reported by the query path
/// that owns the same text. This mirrors
/// [`crate::real_tikv_ddl::prepare_cluster_ddl`]: the server never parses SQL
/// itself, so the dependency direction stays
/// `tidb-server -> tidb-exec -> tidb-txnkv`.
pub fn prepare_cluster_analyze(
    sql: &str,
    default_schema: &str,
) -> Result<Option<Vec<AnalyzeStatement>>, AnalyzeError> {
    let Ok(statement) = tidb_parser::parse(sql) else {
        return Ok(None);
    };
    lower_analyze(&statement, default_schema)
}

/// What one committed `ANALYZE TABLE` did.
#[derive(Clone, Debug)]
pub struct ClusterAnalyzeReport {
    /// The physical table the statistics belong to.
    pub table_id: i64,
    /// The TSO every stored row is stamped with, and the transaction's
    /// `start_ts`.
    pub version: u64,
    /// Rows read from the table.
    pub scanned_rows: i64,
    /// Rows the sampler kept.
    pub sampled_rows: i64,
    /// The Bernoulli sample rate in force.
    pub sample_rate: f64,
    /// Histograms stored, across columns and indexes.
    pub histogram_count: usize,
    /// Buckets stored.
    pub bucket_count: usize,
    /// TopN entries stored.
    pub topn_count: usize,
}

/// Runs and commits one `ANALYZE TABLE`.
pub fn commit_cluster_analyze(
    opener: &RealOptimisticTransactionOpener,
    statement: &AnalyzeStatement,
    timeout: Duration,
) -> Result<ClusterAnalyzeReport, String> {
    let call = UnaryCallContext::with_timeout(timeout);
    let mut transaction = opener
        .begin(MAX_OPTIMISTIC_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
        .map_err(|error| error.to_string())?;
    let start_ts = transaction.start_ts();
    let planned = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot).map_err(|error| error.to_string())?;
        let table = catalog
            .databases
            .iter()
            .find(|database| {
                database.info.name.lowercase() == statement.schema.to_lowercase().as_str()
            })
            .and_then(|database| {
                database.tables.iter().find(|stored| {
                    stored.name.lowercase() == statement.table.to_lowercase().as_str()
                })
            })
            .ok_or_else(|| {
                SystemTableError::Missing {
                    name: format!("{}.{}", statement.schema, statement.table),
                }
                .to_string()
            })?
            .clone();
        // The sample rate Go derives from `RealtimeCount`: what the table's
        // previous `ANALYZE` (or delta flush) said it holds.
        let realtime_count = ClusterStatsLoader::locate(&catalog)
            .ok()
            .and_then(|loader| {
                loader
                    .load_table(
                        &mut snapshot,
                        table.id,
                        &crate::cluster_stats_load::column_types_of(&table),
                    )
                    .ok()
                    .flatten()
            })
            .map(|stats| i64::try_from(stats.row_count).unwrap_or(i64::MAX));
        let report = analyze_table(
            &mut snapshot,
            &table,
            &statement.options,
            realtime_count,
            start_ts,
        )
        .map_err(|error: AnalyzeError| error.to_string())?;
        let write = plan_stats_write(&mut snapshot, &catalog, &report.stats, utc_now_timestamp())
            .map_err(|error| error.to_string())?;
        (report, write)
    };
    let (report, write) = planned;

    if write.is_empty() {
        transaction
            .finish_without_writes()
            .map_err(|error| error.to_string())?;
        return Err("this ANALYZE produced no statistics to store".to_owned());
    }
    let receipt = ClusterAnalyzeReport {
        table_id: report.stats.table_id,
        version: start_ts,
        scanned_rows: report.scanned_rows,
        sampled_rows: report.sampled_rows,
        sample_rate: report.sample_rate,
        histogram_count: write.histogram_count,
        bucket_count: write.bucket_count,
        topn_count: write.topn_count,
    };
    match transaction
        .commit(write.mutations, &call)
        .map_err(|error| error.to_string())?
    {
        OptimisticCommitOutcome::Committed(_) => Ok(receipt),
        other => Err(format!(
            "the statistics were not committed: {:?}",
            other.state()
        )),
    }
}
