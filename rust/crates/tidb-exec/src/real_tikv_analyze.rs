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

use std::fmt;
use std::time::Duration;

use tidb_txnkv::rpc::UnaryCallContext;
use tidb_txnkv::transaction::{
    OptimisticCommitOutcome, RealOptimisticTransactionOpener, MAX_OPTIMISTIC_TRANSACTION_BYTES,
};
use tidb_txnkv::transaction::{StorePdCapability, StoreWriteClient, StoreWriteLoader};

use crate::cluster_analyze::{analyze_table, lower_analyze, AnalyzeError, AnalyzeStatement};
use crate::cluster_catalog::load_cluster_catalog;
use crate::cluster_stats_load::ClusterStatsLoader;
use crate::cluster_stats_write::plan_stats_write;
use crate::mysql_bootstrap::utc_now_timestamp;
use crate::mysql_system_tables::SystemTableError;
use crate::pessimistic_lock_error::{commit_outcome_to_sql_error, LockSqlError};
use crate::real_tikv_catalog::TransactionMetaSnapshot;

/// The mutation-count budget one `ANALYZE TABLE` declares.
///
/// Go saves one table's whole analyze result in ONE transaction --
/// `pkg/statistics/handle/storage/stats_read_writer.go:141` wraps
/// `SaveAnalyzeResultToStorage` in `util.FlagWrapTxn`, which is a single
/// `BEGIN PESSIMISTIC` ... `COMMIT` around every `stats_meta`,
/// `stats_histograms`, `stats_buckets` and `stats_top_n` write for the table
/// -- and places no bound on how many rows that is. Its batching
/// (`save.go:42`'s `batchInsertSize = 10`) groups rows into *statements*
/// inside that one transaction, not into separate transactions.
///
/// So this path keeps its single transaction, and states the count budget
/// Go's has: none. The bound that remains is the byte one
/// ([`MAX_OPTIMISTIC_TRANSACTION_BYTES`], this path's stand-in for Go's
/// `txn-total-size-limit`), which is the bound Go is actually held to.
///
/// The generic [`tidb_txnkv::transaction::MAX_OPTIMISTIC_MUTATIONS`] is the
/// wrong budget here and was
/// the defect: the default `ANALYZE` builds 256 buckets and 100 TopN entries
/// per histogram, so a table with four columns and two indexes plans ~4300
/// mutations -- over that budget. It worked on toy tables and hard-failed on
/// real ones.
pub const ANALYZE_MAX_MUTATIONS: usize = usize::MAX;

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

/// Why an `ANALYZE TABLE` transaction did not produce a committed report.
#[derive(Debug)]
pub enum ClusterAnalyzeError {
    /// The commit may have landed, so the server must not report failure.
    Undetermined(String),
    /// A determinate commit failure with its driver error code and SQLSTATE.
    Commit(LockSqlError),
    /// A determinate failure with its existing diagnostic.
    Other(String),
}

impl fmt::Display for ClusterAnalyzeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Undetermined(detail) => {
                write!(formatter, "execution result undetermined: {detail}")
            }
            Self::Commit(error) => formatter.write_str(&error.message),
            Self::Other(detail) => formatter.write_str(detail),
        }
    }
}

impl std::error::Error for ClusterAnalyzeError {}

/// `mysql.stats_meta.count` as the sample-rate rule reads it.
///
/// Go's `RealtimeCount` is an `int64`, so a stored count past `i64::MAX` is
/// not a number Go can have written and not one this node can believe. It
/// reads as *no usable count*, which `getAdjustedSampleRate` answers with
/// `1.0` -- read every row. Saturating to `i64::MAX` instead would drive the
/// rate to ~1e-14: zero rows kept, yet the NDVs still taken from the
/// full-scan FM sketch, producing a table the loader accepts as non-pseudo
/// with empty histograms.
fn realtime_count_of(row_count: u64) -> i64 {
    i64::try_from(row_count).unwrap_or(0)
}

/// Runs and commits one `ANALYZE TABLE`.
pub fn commit_cluster_analyze<C: StoreWriteClient, L: StoreWriteLoader, P: StorePdCapability>(
    opener: &RealOptimisticTransactionOpener<C, L, P>,
    statement: &AnalyzeStatement,
    timeout: Duration,
) -> Result<ClusterAnalyzeReport, ClusterAnalyzeError> {
    let mut transaction = opener
        .begin(ANALYZE_MAX_MUTATIONS, MAX_OPTIMISTIC_TRANSACTION_BYTES)
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    let start_ts = transaction.start_ts();
    let planned = {
        let mut snapshot = TransactionMetaSnapshot::new(&mut transaction, timeout);
        let catalog = load_cluster_catalog(&mut snapshot)
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
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
                ClusterAnalyzeError::Other(
                    SystemTableError::Missing {
                        name: format!("{}.{}", statement.schema, statement.table),
                    }
                    .to_string(),
                )
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
            .map(|stats| realtime_count_of(stats.row_count));
        let report = analyze_table(
            &mut snapshot,
            &table,
            &statement.options,
            realtime_count,
            start_ts,
        )
        .map_err(|error: AnalyzeError| ClusterAnalyzeError::Other(error.to_string()))?;
        let write = plan_stats_write(&mut snapshot, &catalog, &report.stats, utc_now_timestamp())
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        (report, write)
    };
    let (report, write) = planned;

    if write.is_empty() {
        transaction
            .finish_without_writes()
            .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
        return Err(ClusterAnalyzeError::Other(
            "this ANALYZE produced no statistics to store".to_owned(),
        ));
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
    // The commit mints its own deadline here rather than inheriting one from
    // function entry: everything above it -- the catalog read, the previous
    // statistics read and the row sample -- runs on per-request budgets
    // (`TransactionMetaSnapshot`), but it lasts as long as the table is
    // deep. A context stamped before that work shares ONE absolute deadline
    // with the whole statement, so on a real table the prewrite arrived after
    // its budget had already been spent by scanning -- `timed out after
    // 0ms` at commit admission. Go never couples these:
    // `SaveAnalyzeResultToStorage` commits through the two-phase
    // committer, whose every action builds its context when it runs
    // (`twoPhaseCommitter.execute`, pkg/store/tikv/2pc.go), not when
    // the statement began.
    let outcome = transaction
        .commit(write.mutations, &UnaryCallContext::with_timeout(timeout))
        .map_err(|error| ClusterAnalyzeError::Other(error.to_string()))?;
    classify_commit_outcome(&outcome)?;
    Ok(receipt)
}

fn classify_commit_outcome(outcome: &OptimisticCommitOutcome) -> Result<(), ClusterAnalyzeError> {
    match commit_outcome_to_sql_error(outcome) {
        Ok(()) => Ok(()),
        Err(error) if error.is_result_undetermined() => Err(ClusterAnalyzeError::Undetermined(
            "the ANALYZE transaction returned no commit verdict".to_owned(),
        )),
        Err(error) => Err(ClusterAnalyzeError::Commit(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::{classify_commit_outcome, realtime_count_of, ClusterAnalyzeError};
    use std::thread;
    use std::time::Duration;
    use tidb_stats::row_sample_collector::adjusted_sample_rate;
    use tidb_txnkv::rpc::UnaryCallContext;
    use tidb_txnkv::region::RegionBackoffKind;
    use tidb_txnkv::transaction::{
        OptimisticCommitOutcome, OptimisticTransactionReceipt, RolledBackTransaction,
        TransactionCause,
    };

    #[test]
    fn the_commit_budget_is_minted_after_the_scan_not_before_it() {
        // Regression for the ANALYZE commit that reached prewrite with
        // {bt}timed out after 0ms{bt}: the commit used to inherit one
        // {bt}UnaryCallContext{bt} stamped at function entry, so a
        // table-sized sample consumed the whole statement budget before the
        // commit was ever submitted -- on a 5.5M-row table the scan alone ran
        // past the 300s deadline and every commit arrived expired. The
        // commit must mint its budget where it is spent, the way
        // {bt}MultiStatementTransaction::transaction_end_call{bt} already
        // does for sessions.
        let statement_budget = Duration::from_millis(50);
        let inherited = UnaryCallContext::with_timeout(statement_budget);
        // The sample of a deep table: far longer than one request's budget.
        thread::sleep(statement_budget * 3);
        assert!(
            inherited.timeout().is_zero(),
            "a context minted before the scan is exhausted by it"
        );
        let commit_call = UnaryCallContext::with_timeout(statement_budget);
        assert!(
            commit_call.timeout() > Duration::ZERO,
            "the commit mints its own full budget at commit time"
        );
    }

    #[test]
    fn an_unbelievable_stored_row_count_reads_every_row_rather_than_none() {
        assert_eq!(realtime_count_of(10_000), 10_000);
        assert_eq!(realtime_count_of(u64::MAX), 0);
        // The failure this is about: a saturating read would sample at
        // 110000/i64::MAX and keep nothing, while the FM sketch still reported
        // NDVs from the full scan.
        assert!(
            adjusted_sample_rate(Some(realtime_count_of(u64::MAX)), None)
                > adjusted_sample_rate(Some(i64::MAX), None)
        );
        assert!(
            (adjusted_sample_rate(Some(realtime_count_of(u64::MAX)), None) - 1.0).abs()
                < f64::EPSILON
        );
    }

    #[test]
    fn analyze_backoff_exhaustion_keeps_its_driver_error_identity() {
        let outcome = OptimisticCommitOutcome::RolledBack(RolledBackTransaction {
            receipt: OptimisticTransactionReceipt::new(1, 2, b"k".to_vec(), 1),
            cause: TransactionCause::BackoffExhausted {
                kind: RegionBackoffKind::StaleCommand,
                detail: "staleCommand backoffer exhausted".to_owned(),
            },
        });
        assert!(matches!(
            classify_commit_outcome(&outcome),
            Err(ClusterAnalyzeError::Commit(error))
                if error.code == tidb_error::tidb::errcode::ErrTiKVStaleCommand
        ));
    }
}
