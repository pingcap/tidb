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

//! `ANALYZE TABLE`: the `AdminStmt::AnalyzeTable` arm of
//! [`crate::Session::dispatch_admin_stmt`].
//!
//! What an `ANALYZE` computes is [`tidb_executor::analyze`]'s, shared with the
//! cluster tier; what it means for THIS session is here: which tables the
//! statement names, and where the result is published.
//!
//! # Why this used to be refused, and what changed
//!
//! Statistics are cluster state, so a session over cluster storage routes the
//! statement to a node that can write `mysql.stats_*`
//! (`Session::statement_stored_state_change` still says so, and
//! `tidb_server`'s convergence node still routes it there). An IN-PROCESS
//! session has no cluster to write to and no peer to tell -- its catalog IS
//! the whole world -- so the honest answer is not a refusal but the analysis
//! itself, published into the same catalog the planner reads from. Until it
//! was, every `EXPLAIN` in this tier printed `stats:pseudo` even for a table
//! the script had just analyzed, and every `ANALYZE` answered "not supported".
//!
//! # What is deliberately not here
//!
//! * **Privileges.** Go gates `ANALYZE` on INSERT *and* SELECT for each named
//!   table (`planbuilder.go`'s `requireInsertAndSelectPriv`), which the
//!   convergence node enforces. This tier applies no table privileges to
//!   ordinary reads either, so a check on this one statement would be the
//!   only one and would refuse scripts nothing else refuses.
//! * **Transaction interaction.** The analysis runs against the catalog THIS
//!   statement sees, so inside an explicit transaction it reads the
//!   transaction's rows and a `ROLLBACK` discards the statistics with them.
//!   Captured: TiDB's survive the rollback, because its `ANALYZE` writes
//!   through an INTERNAL session and the statistics were never the rolling-back
//!   transaction's to discard. Named rather than papered over -- making the
//!   write escape the transaction would also make the READ escape it, and
//!   sampling rows the statement cannot see is the worse of the two errors.
//!   Pinned by `tests_analyze::analyze_inside_a_transaction_rolls_back_with_it`.

use std::sync::Arc;

use tidb_executor::analyze::kv::analyze_kv_table;
use tidb_executor::analyze::{
    lower_analyze_admin, AnalyzeOptions, AnalyzeStatement, SampleMemoryQuota,
    MEM_QUOTA_ANALYZE_VARIABLE,
};
use tidb_executor::{DriverError, SchemaErrorKind, TableEntry};

use crate::{Session, StmtOutput};

impl Session {
    /// Runs `ANALYZE TABLE t [, u ...]`, one table at a time.
    ///
    /// Returns `None` when the statement is not an `ANALYZE` this arm owns, so
    /// the caller falls through to the rest of the admin dispatch.
    pub(crate) fn analyze_stmt(
        &mut self,
        admin: &tidb_ast::AdminStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        let Some(tables) = lower_analyze_admin(admin, &self.current_db)
            .map_err(|error| DriverError::unsupported(error.to_string()))?
        else {
            return Ok(None);
        };
        let memory_quota = self.analyze_memory_quota();
        for statement in &tables {
            let mut options = statement.options;
            options.memory_quota = memory_quota;
            self.analyze_one_table(statement, &options)?;
        }
        // Go answers `ANALYZE TABLE` with an OK packet carrying no rows.
        Ok(Some(StmtOutput::Affected(0)))
    }

    /// Analyzes one named table and publishes its statistics.
    fn analyze_one_table(
        &mut self,
        statement: &AnalyzeStatement,
        options: &AnalyzeOptions,
    ) -> Result<(), DriverError> {
        let schema = statement.schema.clone();
        let name = statement.table.clone();
        let zone = self.statement_context(false).session_zone();
        self.with_catalog_mut(|catalog| {
            let table_id = match catalog.table_in(&schema, &name) {
                Some(TableEntry::Kv(kv)) => kv.table_id,
                // Go raises 1146 for a name that is not a table, and
                // `ErrAnalyzeMissColumn`-adjacent refusals for a view or a
                // sequence; both are "there is nothing here to analyze", and
                // naming the object is what a caller can act on.
                Some(_) => {
                    return Err(DriverError::unsupported(format!(
                        "`{schema}`.`{name}` is not a table whose rows this node can analyze"
                    )))
                }
                None => {
                    return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(
                        name.clone(),
                    )))
                }
            };
            // Go's `getAdjustedSampleRate` reads the CURRENT
            // `mysql.stats_meta.count`, which here is whatever the last
            // analysis published.
            let realtime_count = catalog
                .table_statistics(table_id)
                .map(|statistics| statistics.row_count);
            let Some(TableEntry::Kv(table)) = catalog.table_mut_in(&schema, &name) else {
                // The lookup two lines up already resolved it; a catalog that
                // changed in between is not reachable from one statement.
                return Err(DriverError::CatalogPoisoned);
            };
            let statistics = analyze_kv_table(table, options, realtime_count, &zone)
                .map_err(|error| DriverError::unsupported(error.to_string()))?;
            catalog.set_table_statistics(table_id, Arc::new(statistics));
            Ok(())
        })
    }

    /// Go's analyze memory quota, as one `ANALYZE` reads it.
    ///
    /// `tidb_mem_quota_analyze` is process-wide and read at execution time
    /// (`variable.SetMemQuotaAnalyze` drives one `GlobalAnalyzeMemoryTracker`,
    /// `pkg/executor/select.go:141`), so the value in force is whatever the
    /// last `SET GLOBAL` stored. Its default, `-1`, is no bound.
    fn analyze_memory_quota(&self) -> SampleMemoryQuota {
        self.vars()
            .get_global(MEM_QUOTA_ANALYZE_VARIABLE)
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .map_or_else(
                SampleMemoryQuota::unlimited,
                SampleMemoryQuota::from_setting,
            )
    }
}
