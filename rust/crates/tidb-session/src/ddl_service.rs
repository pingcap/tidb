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

//! The session's one DDL orchestration (Milestone 3a of
//! `rust/docs/structural-alignment-execplan.md`).
//!
//! Go has a single DDL path: the executor checks privileges, commits the open
//! transaction, builds a job, hands it to the `ddl` package and then copies the
//! job's outcome (warnings, the last-DDL record, a dropped current database)
//! back onto the session. Here the statement kinds that reach the cluster's
//! stored schema are lowered to a [`DdlStatement`] and published through a
//! [`DdlService`]; this module owns everything around that call, so that the
//! production node, and later the in-process tier, differ only in the service
//! they hand in and in how they commit the open transaction.

use tidb_exec::cluster_ddl::DdlStatement;
use tidb_exec::real_tikv_ddl::{ClusterDdlReport, DdlWarningLevel};
use tidb_executor::{DriverError, StmtContext};

use crate::Session;

/// Go `ddl.DDL.DoDDLJob` for one lowered statement: publishes the change,
/// then brings the caller's own catalog up to it before answering.
///
/// The two halves are one method because a caller that published without
/// refreshing would answer the next statement from a catalog it knows to be
/// stale. `job_context` is what Go's submitter copies from the issuing
/// session into the job: the statement text (`Job.Query`), SQL mode and
/// `tidb_cdc_write_source`.
pub trait DdlService {
    /// Runs one lowered DDL statement to completion.
    fn execute(
        &self,
        statement: &DdlStatement,
        job_context: &StmtContext,
    ) -> Result<ClusterDdlReport, DriverError>;
}

impl Session {
    /// Go's preprocessor name resolution for one table reference: no current
    /// database is 1046 and a missing table 1146. It runs before the
    /// planner's privilege check, so a missing table answers 1146 even to a
    /// user who holds no privilege on it.
    pub fn require_table_exists(&mut self, path: &[String]) -> Result<(), DriverError> {
        let current_db = self.current_db.clone();
        let (database, name) = tidb_executor::driver::split_table_path_pub(path, &current_db)?;
        let (database, name) = (database.to_owned(), name.to_owned());
        self.with_catalog_mut(|catalog| {
            if catalog.table_in(&database, &name).is_some() {
                Ok(())
            } else {
                Err(DriverError::Schema(
                    tidb_executor::SchemaErrorKind::UnknownTable(format!("{database}.{name}")),
                ))
            }
        })
    }

    /// Go `executor.AlterTable`'s checks before its specification loop: the
    /// table must exist (1146) and be a base table (1347), for a caller that
    /// answers a specification refusal itself.
    pub fn alter_table_target_precheck(
        &mut self,
        alter: &tidb_ast::AlterTableStmt,
    ) -> Result<(), DriverError> {
        let current_db = self.current_db.clone();
        let (database, name) =
            tidb_executor::driver::split_table_path_pub(&alter.name, &current_db)?;
        let (database, name) = (database.to_owned(), name.to_owned());
        self.with_catalog_mut(|catalog| match catalog.table_in(&database, &name) {
            None => Err(DriverError::Schema(
                tidb_executor::SchemaErrorKind::UnknownTable(format!("{database}.{name}")),
            )),
            Some(tidb_executor::TableEntry::View(_) | tidb_executor::TableEntry::Sequence(_)) => {
                Err(DriverError::Schema(
                    tidb_executor::SchemaErrorKind::WrongObject {
                        name: format!("{database}.{name}"),
                        expected: "BASE TABLE",
                    },
                ))
            }
            Some(_) => Ok(()),
        })
    }

    /// The checks Go makes before its implicit-commit boundary: the DDL's
    /// privileges and snapshot rules are decided while the connection's
    /// transaction is still intact, so a denial neither publishes staged
    /// writes nor reaches the schema authority.
    pub fn begin_ddl_statement(&mut self, parsed: &tidb_ast::Stmt) -> Result<(), DriverError> {
        self.validate_snapshot_statement(parsed)?;
        self.require_statement_table_privileges(parsed)
    }

    /// Everything after the implicit commit: the reference checks, the table
    /// lock job, the service call, and the copy of the outcome back onto the
    /// session. The caller has already run [`Self::begin_ddl_statement`] and
    /// committed any open transaction.
    pub fn run_ddl_statement(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
        statement: &DdlStatement,
        service: &dyn DdlService,
    ) -> Result<(), DriverError> {
        self.validate_cached_table_ddl(parsed)?;
        self.validate_persistent_ddl_references(statement)?;
        let lock_effect = self.cluster_ddl_lock_effect(statement);
        self.begin_cluster_lock_job(statement);
        // Go `Job.Query` is the statement's original text.
        let job_context = self.ddl_statement_context().with_ddl_query(sql);
        let report = service.execute(statement, &job_context);
        // Go appends plan-time warnings to the session's statement context
        // before it submits the job, so they precede the job's own.
        if let Some(context) = statement.statement_context() {
            self.drain_context_warnings(context);
        }
        let report = report?;
        self.finish_cluster_lock_job(statement);
        let (created_id, applied, warnings) = match report {
            ClusterDdlReport::Applied {
                created_id,
                warnings,
                ..
            } => (created_id, true, warnings),
            ClusterDdlReport::AlreadySatisfied { warnings, .. } => (None, false, warnings),
        };
        // Go `recordLastDDLInfo`: a statement that ran a job leaves its query
        // and the job's sequence number on the session.
        if applied {
            self.record_last_ddl_info(sql);
        }
        self.finish_cluster_ddl_lock_effect(lock_effect, created_id);
        // Go raises an applied job's `job.Warning` via `AppendWarning` and an
        // `IF [NOT] EXISTS` suppression via `AppendNote`, on the session's
        // own statement context so `SHOW WARNINGS` reports what the change
        // did differently from what was written.
        for (level, code, warning) in warnings {
            match level {
                DdlWarningLevel::Note => self.append_routed_note(code, warning),
                DdlWarningLevel::Warning => self.append_routed_warning(code, warning),
            }
        }
        // Go `dropSchema` invalidates the session's current database when the
        // DROP removes it: the next unqualified statement answers 1046
        // (`ErrNoDB`) until a fresh `USE`.
        if let DdlStatement::DropDatabase { name, .. } = statement {
            if self.current_database().eq_ignore_ascii_case(name) {
                self.deselect_database();
            }
        }
        Ok(())
    }
}
