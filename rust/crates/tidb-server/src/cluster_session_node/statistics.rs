//! `ANALYZE TABLE` on the convergence node: who may run one, how much memory
//! its sampling may use, and what each analysed table reports.
//!
//! The statement is not answered by the session driver at all -- statistics
//! are stored cluster state, so it is routed to
//! [`ClusterAnalyze`](crate::cluster_analyze_seam::ClusterAnalyze), one
//! transaction per named table. Mirrors Go's split across
//! `pkg/planner/core/planbuilder.go` (`requireInsertAndSelectPriv`, the
//! privilege gate below) and `pkg/executor/analyze.go` with
//! `variable.SetMemQuotaAnalyze` (`pkg/executor/select.go:141`, the quota
//! below).

use tidb_exec::cluster_analyze::{AnalyzeStatement, SampleMemoryQuota, MEM_QUOTA_ANALYZE_VARIABLE};
use tidb_session::privilege::GlobalPriv;

use crate::sql_node::{QuerySession, SqlQueryError, WriteOutcome};

use super::{ClusterServerSession, ER_TABLEACCESS_DENIED_ERROR};

impl ClusterServerSession {
    /// Performs one `ANALYZE TABLE`, one table at a time.
    ///
    /// Each table is its own transaction, which is what Go does too: an
    /// `ANALYZE TABLE t1, t2` is two analyses, and holding one transaction
    /// open across both would make the second table's row count describe the
    /// moment the first was read.
    ///
    /// An open transaction is committed first, for the same reason a DDL or
    /// an account statement commits one: MySQL and Go both commit implicitly
    /// before a statement that changes stored state outside it.
    pub(super) fn run_analyze(
        &mut self,
        tables: &[AnalyzeStatement],
    ) -> Result<WriteOutcome, SqlQueryError> {
        if self.explicit.is_some() || self.session.in_transaction() {
            self.control_transaction("COMMIT")?;
        }
        // Go checks the privileges of EVERY named table before running any
        // of them: `buildAnalyze` appends the visitInfo for each and
        // `CheckPrivilege` runs over the whole plan, so `ANALYZE TABLE ok, no`
        // stores nothing at all.
        for statement in tables {
            self.require_analyze_privileges(statement)?;
        }
        // Go's analyze memory quota is process-wide and read at execution:
        // `variable.SetMemQuotaAnalyze` drives one `GlobalAnalyzeMemoryTracker`
        // (`pkg/executor/select.go:141`), so the value in force is whatever
        // `SET GLOBAL tidb_mem_quota_analyze` last stored. Its default, `-1`,
        // is no bound.
        let memory_quota = self.analyze_memory_quota();
        for statement in tables {
            let mut statement = statement.clone();
            statement.options.memory_quota = memory_quota;
            let statement = &statement;
            let report = self.analyze.execute(statement)?;
            eprintln!(
                "{{\"event\":\"cluster_table_analyzed\",\"schema\":{},\"table\":{},\
                 \"table_id\":{},\"version\":{},\"scanned_rows\":{},\"sampled_rows\":{},\
                 \"sample_rate\":{},\"histograms\":{},\"buckets\":{},\"topn\":{}}}",
                serde_json::to_string(&statement.schema).unwrap_or_else(|_| "\"\"".to_owned()),
                serde_json::to_string(&statement.table).unwrap_or_else(|_| "\"\"".to_owned()),
                report.table_id,
                report.version,
                report.scanned_rows,
                report.sampled_rows,
                report.sample_rate,
                report.histogram_count,
                report.bucket_count,
                report.topn_count,
            );
        }
        // Go answers `ANALYZE TABLE` with an OK packet carrying no rows.
        Ok(WriteOutcome {
            affected_rows: 0,
            last_insert_id: 0,
        })
    }

    /// Go's privilege gate on `ANALYZE TABLE`: INSERT *and* SELECT on the
    /// table.
    ///
    /// `pkg/planner/core/planbuilder.go:3205` calls
    /// `requireInsertAndSelectPriv(as.TableNames)`, which appends
    /// `mysql.InsertPriv` and then `mysql.SelectPriv` for each table, each
    /// carrying its own `ErrTableaccessDenied`. INSERT is appended first, so
    /// an account holding neither is told about INSERT -- captured from a
    /// real TiDB, for a user with no privileges and for a SELECT-only user
    /// alike:
    ///
    /// ```text
    /// ERROR 1142 (42000): INSERT command denied to user 'zzlow'@'%' for table 'zzt'
    /// ```
    ///
    /// This is not a formality on a read: the TopN entries an `ANALYZE`
    /// writes into `mysql.stats_top_n` are ACTUAL COLUMN VALUES, readable by
    /// anyone who can read the statistics.
    fn require_analyze_privileges(
        &self,
        statement: &AnalyzeStatement,
    ) -> Result<(), SqlQueryError> {
        for required in [GlobalPriv::Insert, GlobalPriv::Select] {
            if self
                .session
                .has_table_privilege(&statement.schema, &statement.table, required)
            {
                continue;
            }
            let (user, host) = self.session.authenticated_identity().unwrap_or(("", ""));
            return Err(SqlQueryError::new(
                ER_TABLEACCESS_DENIED_ERROR,
                *b"42000",
                format!(
                    "{} command denied to user '{user}'@'{host}' for table '{}'",
                    required.print_name(),
                    statement.table
                ),
            ));
        }
        Ok(())
    }

    /// `tidb_mem_quota_analyze` as this node currently holds it.
    ///
    /// A variable that is missing or unreadable is Go's default: no bound. It
    /// is not a reason to refuse an `ANALYZE`, since Go runs every one of them
    /// unbounded by default anyway.
    fn analyze_memory_quota(&self) -> SampleMemoryQuota {
        self.session
            .vars()
            .get_global(MEM_QUOTA_ANALYZE_VARIABLE)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .map_or_else(
                SampleMemoryQuota::unlimited,
                SampleMemoryQuota::from_setting,
            )
    }
}
