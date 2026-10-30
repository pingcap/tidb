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

//! The session's one DDL orchestration.
//!
//! Go has a single DDL path: the executor checks privileges, commits the open
//! transaction, builds a job, hands it to the `ddl` package and then copies the
//! job's outcome (warnings, the last-DDL record, a dropped current database)
//! back onto the session. Here the statement kinds that reach the cluster's
//! stored schema are lowered to a [`DdlStatement`] and published through a
//! [`DdlService`]; this module owns everything around that call, so that the
//! production node, and later the in-process tier, differ only in the service
//! they hand in and in how they commit the open transaction.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_ddl::cluster_ddl::DdlStatement;
use tidb_ddl::real_tikv_ddl::ClusterDdlReport;
use tidb_driver_error::DriverError;
use tidb_sessionctx::StmtContext;

use crate::Session;
use tidb_hack::GoCaseCompare;
use tidb_hack::go_to_lower;

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

    /// Go's DDL executor entry refusals for a cached table
    /// (`ErrOptOnCacheTable`), against the latest schema the DDL holds.
    fn check_cached_table_ddl(
        &self,
        stmt: &tidb_ast::Stmt,
        current_db: &str,
    ) -> Result<(), DriverError>;

    /// Go `infoschema.GetTiFlashStoreCount(ctx.GetStore())`.
    fn tiflash_store_count(&self) -> Result<u64, String>;
}

impl Session {
    /// Go's preprocessor name resolution for a DDL (`tableByName`, masked
    /// as a privilege error for an account that holds nothing on the table,
    /// and `checkCreateTableGrammar`'s LIKE source), part of `Compile`: it
    /// precedes the planner's privilege check
    /// ([`Session::compile_statement`]).
    pub(crate) fn ddl_preprocess_table_names(
        &mut self,
        stmt: &tidb_ast::Stmt,
    ) -> Result<(), DriverError> {
        use tidb_ast::{AlterTableAction, DdlStmt};
        let tidb_ast::Stmt::Ddl(ddl) = stmt else {
            return Ok(());
        };
        match ddl.as_ref() {
            // `resolveAlterTableStmt` sets `inCreateOrDropTable` for a RENAME
            // or FOREIGN KEY spec, which skips resolution.
            DdlStmt::AlterTable(alter)
                if !alter.actions.iter().any(|action| {
                    matches!(
                        action,
                        AlterTableAction::RenameTable { .. } | AlterTableAction::AddForeignKey(_)
                    )
                }) =>
            {
                self.preprocess_table_by_name(&alter.name, "ALTER")?;
            }
            // `stmtType()`: CREATE INDEX is TypeCreate; DROP INDEX and
            // TRUNCATE fall to its default, "SELECT".
            DdlStmt::CreateIndex(create) => {
                self.preprocess_table_by_name(&create.table, "CREATE")?
            }
            DdlStmt::DropIndex(drop) => self.preprocess_table_by_name(&drop.table, "SELECT")?,
            DdlStmt::TruncateTable(path) => self.preprocess_table_by_name(path, "SELECT")?,
            // `checkCreateTableGrammar` resolves the LIKE source with no
            // privilege masking.
            DdlStmt::CreateTable(create) => {
                if let Some(like) = &create.like_table {
                    self.require_table_exists(like)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// The executor's target checks Go makes before a DDL is built into a
    /// job, after `Compile` ([`Session::compile_statement`]): `SchemaByName`
    /// for CREATE TABLE, the LIKE source's kind, ALTER's base-table check.
    /// Lowering builds the table and validates its definitions, so running
    /// it first answered build errors where Go answers 1049/1347.
    pub fn ddl_target_checks(&mut self, stmt: &tidb_ast::Stmt) -> Result<(), DriverError> {
        use tidb_ast::DdlStmt;
        let tidb_ast::Stmt::Ddl(ddl) = stmt else {
            return Ok(());
        };
        match ddl.as_ref() {
            DdlStmt::AlterTable(alter) => self.alter_table_target_precheck(alter)?,
            DdlStmt::CreateTable(create) => {
                let current_db = self.vars.current_db.clone();
                let (database, _) =
                    tidb_infoschema::split_table_path_pub(&create.name, &current_db)?;
                let database = database.to_owned();
                let source = create
                    .like_table
                    .as_ref()
                    .map(|like| -> Result<(String, String), DriverError> {
                        let (schema, name) =
                            tidb_infoschema::split_table_path_pub(like, &current_db)?;
                        Ok((schema.to_owned(), name.to_owned()))
                    })
                    .transpose()?;
                self.with_catalog_mut(|catalog| {
                    // Go `executor.CreateTable`'s `SchemaByName` (1049)
                    // precedes `BuildTableInfoWithStmt`.
                    if !catalog.has_database(&database) {
                        return Err(DriverError::from(
                            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                                .generate_with_stack_by_args(&[(database.clone()).into()]),
                        ));
                    }
                    // Go `BuildTableInfoWithLike`: the source must be a base
                    // table; the message names the TARGET schema.
                    if let Some((schema, name)) = &source {
                        if let Some(
                            tidb_infoschema::TableEntry::View(_)
                            | tidb_infoschema::TableEntry::Sequence(_),
                        ) = catalog.table_in(schema, name)
                        {
                            return Err(DriverError::from(
                                tidb_infoschema::infoschema_errors::ERR_WRONG_OBJECT
                                    .generate_with_stack_by_args(&[
                                        (&database).into(),
                                        (&name).into(),
                                        ("BASE TABLE").into(),
                                    ]),
                            ));
                        }
                    }
                    Ok(())
                })?;
            }
            _ => {}
        }
        Ok(())
    }

    /// Go `preprocessor.tableByName`: 1046 without a schema; a missing table
    /// is 1146, unless the account holds no privilege at all on it, which
    /// answers 1142 naming the statement type and the table as written so a
    /// stranger cannot probe which tables exist.
    fn preprocess_table_by_name(
        &mut self,
        path: &[String],
        verb: &'static str,
    ) -> Result<(), DriverError> {
        let current_db = self.vars.current_db.clone();
        let (database, name) = tidb_infoschema::split_table_path_pub(path, &current_db)?;
        let (database, name) = (go_to_lower(database), name.to_owned());
        // The session's infoschema Go resolves through carries its LOCAL
        // temporary tables (`AttachLocalTemporaryTableInfoSchema`).
        let exists = self.is_local_temporary_table(&database, &name)
            || self.with_catalog_mut(|catalog| Ok(catalog.table_in(&database, &name).is_some()))?;
        if exists {
            return Ok(());
        }
        if let Some((_, user, host)) = self.privilege_context() {
            if !self.has_any_scoped_privilege(&database, &name, tidb_privilege::any_priv_mask()) {
                return Err(DriverError::from(
                    tidb_plannererrors::ERR_TABLEACCESS_DENIED.generate_with_stack_by_args(
                        &[
                            verb.into(),
                            user.to_owned().into(),
                            host.to_owned().into(),
                            name.into(),
                        ],
                    ),
                ));
            }
        }
        Err(DriverError::from(
            tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
        ))
    }

    /// Go's preprocessor name resolution for one table reference: no current
    /// database is 1046 and a missing table 1146. It runs before the
    /// planner's privilege check, so a missing table answers 1146 even to a
    /// user who holds no privilege on it.
    pub fn require_table_exists(&mut self, path: &[String]) -> Result<(), DriverError> {
        let current_db = self.vars.current_db.clone();
        let (database, name) = tidb_infoschema::split_table_path_pub(path, &current_db)?;
        let (database, name) = (database.to_owned(), name.to_owned());
        self.with_catalog_mut(|catalog| {
            if catalog.table_in(&database, &name).is_some() {
                Ok(())
            } else {
                Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                        .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
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
        let current_db = self.vars.current_db.clone();
        let (database, name) =
            tidb_infoschema::split_table_path_pub(&alter.name, &current_db)?;
        let (database, name) = (database.to_owned(), name.to_owned());
        self.with_catalog_mut(|catalog| match catalog.table_in(&database, &name) {
            None => Err(DriverError::from(
                tidb_infoschema::infoschema_errors::ERR_TABLE_NOT_EXISTS
                    .generate_with_stack_by_args(&[(&database).into(), (&name).into()]),
            )),
            Some(tidb_infoschema::TableEntry::View(_) | tidb_infoschema::TableEntry::Sequence(_)) => {
                Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_WRONG_OBJECT.generate_with_stack_by_args(
                        &[(&database).into(), (&name).into(), ("BASE TABLE").into()],
                    ),
                ))
            }
            Some(_) => Ok(()),
        })
    }

    /// The check Go makes between `Compile` and its implicit-commit
    /// boundary (`ExecStmt`'s `tidb_snapshot` write refusal), while the
    /// connection's transaction is still intact, so a refusal neither
    /// publishes staged writes nor reaches the schema authority. The
    /// staleness check precedes `Compile` and the privileges are `Compile`'s
    /// ([`Session::compile_statement`]).
    pub fn begin_ddl_statement(&mut self, parsed: &tidb_ast::Stmt) -> Result<(), DriverError> {
        self.validate_snapshot_write(parsed)
    }

    /// Everything after the implicit commit: the reference checks, the table
    /// lock job, the service call, and the copy of the outcome back onto the
    /// session. The caller has already run [`Self::begin_ddl_statement`] and
    /// committed any open transaction. Answers the id an applied job
    /// created, which Go's executor pre-splits after a CREATE TABLE.
    pub fn run_ddl_statement(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
        statement: &DdlStatement,
        service: &dyn DdlService,
    ) -> Result<Option<i64>, DriverError> {
        // Go `executeDropDatabase`: the bootstrap schema is never dropped,
        // with or without IF EXISTS.
        if let DdlStatement::DropDatabase { name, .. } = statement {
            if name.go_lower_eq("mysql") {
                return Err(DriverError::from(
                    (&tidb_dbterror::ERR_FORBIDDEN_DDL)
                        .generate_with_stack("Drop 'mysql' database is forbidden".to_owned()),
                ));
            }
        }
        service.check_cached_table_ddl(parsed, &self.vars.current_db)?;
        self.validate_persistent_ddl_references(statement)?;
        let lock_effect = self.cluster_ddl_lock_effect(statement);
        self.begin_cluster_lock_job(statement);
        // Go `Job.Query` is the statement's original text.
        let job_context = self.ddl_statement_context().with_ddl_query(sql);
        let report = service.execute(statement, &job_context);
        // Go appends plan-time warnings to the session's statement context
        // before it submits the job, so they precede the job's own.
        if let Some(context) = statement.statement_context() {
            for warning in context.take_warnings() {
                self.append_sql_warning(warning);
            }
            self.drain_fold_stash();
        }
        // What the executor functions noted while building the job (Go's
        // `StmtCtx.AppendNote` as each runs) stays whether or not the
        // statement then fails.
        for warning in job_context.take_warnings() {
            self.append_sql_warning(warning);
        }
        // So do the hypothetical indexes and TiFlash replicas Go's
        // `addHypoIndexIntoCtx`, `dropHypoIndexFromCtx` and
        // `setHypoTiFlashReplica` wrote into `SessionVars`.
        if let Some(indexes) = job_context.take_hypo_indexes() {
            self.hypo_indexes = std::sync::Arc::new(indexes);
        }
        let hypo_replicas = job_context.take_hypo_tiflash_replicas();
        if !hypo_replicas.is_empty() {
            let replicas = std::sync::Arc::make_mut(&mut self.hypo_tiflash_replicas);
            for (schema, table, add) in hypo_replicas {
                replicas.set(&schema, &table, add);
            }
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
        for warning in warnings {
            self.append_sql_warning(warning);
        }
        // Go `executeDropDatabase`: dropping the current database clears it
        // (the next unqualified statement answers 1046 until a fresh `USE`)
        // and resets `character_set_database` / `collation_database` to the
        // server defaults.
        if let DdlStatement::DropDatabase { name, .. } = statement {
            if self.current_database().go_lower_eq(name) {
                self.deselect_database();
                self.vars
                    .set_system("character_set_database", tidb_mysql::DefaultCharset.to_owned())?;
                self.vars
                    .set_system("collation_database", tidb_mysql::DefaultCollationName.to_owned())?;
            }
        }
        Ok(created_id)
    }
}
