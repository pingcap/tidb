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
//! stored schema call their Go-named executor methods through a
//! [`DdlService`]; this module owns everything around that call, so that the
//! production node, and later the in-process tier, differ only in the service
//! they hand in and in how they commit the open transaction.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_ddl::real_tikv_ddl::ClusterDdlReport;
use tidb_driver_error::DriverError;
use tidb_sessionctx::StmtContext;

use crate::Session;
use tidb_hack::GoCaseCompare;
use tidb_hack::go_to_lower;

/// Go `ddl.Executor` for one parsed statement: publishes the change,
/// then brings the caller's own catalog up to it before answering.
///
/// The two halves are one method because a caller that published without
/// refreshing would answer the next statement from a catalog it knows to be
/// stale. `job_context` is what Go's submitter copies from the issuing
/// session into the job: the statement text (`Job.Query`), SQL mode and
/// `tidb_cdc_write_source`.
pub trait DdlService {
    /// Go `executor.AlterTableSetTiFlashReplica` for a database-level change.
    fn set_tiflash_replica(
        &self,
        schema: &str,
        table: &str,
        count: u64,
        labels: &[String],
        context: &StmtContext,
    ) -> Result<ClusterDdlReport, DriverError>;

    /// Go `executor.CreateView` with the preprocessed view definition.
    fn create_view(
        &self,
        schema: &str,
        name: &str,
        or_replace: bool,
        info: &tidb_model::TableInfo,
        context: &StmtContext,
    ) -> Result<ClusterDdlReport, DriverError>;

    /// Go `executor.DoDDLJobWrapper`.
    fn do_ddl_job_wrapper(
        &self,
        context: &StmtContext,
        job: tidb_ddl::JobWrapper,
    ) -> Result<ClusterDdlReport, DriverError>;

    /// Go `DDLExec.Next` calling the DDL executor for the parsed statement.
    fn execute_ddl(
        &self,
        statement: &tidb_ast::Stmt,
        current_db: &str,
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

    /// Go `ddl.CancelJobs` / `PauseJobs` / `ResumeJobs` on a system session
    /// (`CommandDDLJobsExec.Open`'s `GetSysSession`): one error per id,
    /// `None` where the command succeeded.
    ///
    /// # Errors
    ///
    /// The command's transaction error.
    fn command_ddl_jobs(
        &self,
        kind: tidb_ast::AdminDdlJobControlKind,
        job_ids: &[i64],
    ) -> Result<Vec<Option<String>>, DriverError> {
        let _ = kind;
        Ok(tidb_executor::admin::ddl_jobs_not_found(job_ids))
    }
}

impl Session {
    /// Go `preprocessor.Enter`: inPrepare sets inCreateOrDropTable for
    /// ALTER TABLE and CREATE INDEX, so neither records a resolved name.
    pub(crate) fn ddl_names_skipped_in_prepare(&self, stmt: &tidb_ast::Stmt) -> bool {
        (self.in_prepare || self.binary_prepared_execution)
            && matches!(stmt, tidb_ast::Stmt::Ddl(ddl)
                if matches!(&**ddl, tidb_ast::DdlStmt::AlterTable(_) | tidb_ast::DdlStmt::CreateIndex(_)))
    }

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
        if self.ddl_names_skipped_in_prepare(stmt) {
            return Ok(());
        }
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
    /// The executor builds the table and validates its definitions, so running
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
                    if !catalog.schema_exists(&database) {
                        return Err(DriverError::from(
                            tidb_infoschema::infoschema_errors::ERR_DATABASE_NOT_EXISTS
                                .gen_with_stack_by_args(&[(database.clone()).into()]),
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
                                    .gen_with_stack_by_args(&[
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
            if !self.has_any_scoped_privilege(&database, &name, tidb_privilege::all_priv_mask()) {
                return Err(DriverError::from(
                    tidb_plannererrors::ERR_TABLEACCESS_DENIED.gen_with_stack_by_args(
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
                .gen_with_stack_by_args(&[(&database).into(), (&name).into()]),
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
                        .gen_with_stack_by_args(&[(&database).into(), (&name).into()]),
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
                    .gen_with_stack_by_args(&[(&database).into(), (&name).into()]),
            )),
            Some(tidb_infoschema::TableEntry::View(_) | tidb_infoschema::TableEntry::Sequence(_)) => {
                Err(DriverError::from(
                    tidb_infoschema::infoschema_errors::ERR_WRONG_OBJECT.gen_with_stack_by_args(
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

    /// Go `DDLExec.Next`: the statement calls its DDL executor method
    /// after `NewTxnInStmt`, with the session context carried through the job.
    pub fn run_executor_statement(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
        service: &dyn DdlService,
    ) -> Result<Option<i64>, DriverError> {
        let current_db = self.vars.current_db.clone();
        self.run_executor_call(sql, parsed, service, None, |service, context| {
            service.execute_ddl(parsed, &current_db, context)
        })
    }

    /// Go `executor.ModifySchemaSetTiFlashReplica` calling its per-table method.
    pub fn run_set_tiflash_replica(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
        schema: &str,
        table: &str,
        count: u64,
        labels: &[String],
        service: &dyn DdlService,
    ) -> Result<Option<i64>, DriverError> {
        self.run_executor_call(sql, parsed, service, None, |service, context| {
            service.set_tiflash_replica(schema, table, count, labels, context)
        })
    }

    /// Go `DDLExec.executeCreateView` calling the owning DDL method.
    pub fn run_create_view(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
        schema: &str,
        name: &str,
        or_replace: bool,
        info: &tidb_model::TableInfo,
        service: &dyn DdlService,
    ) -> Result<Option<i64>, DriverError> {
        self.run_executor_call(sql, parsed, service, None, |service, context| {
            service.create_view(schema, name, or_replace, info, context)
        })
    }

    /// Go `executor.DoDDLJobWrapper` with the statement's session effects.
    pub fn run_ddl_job_wrapper(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
        job: tidb_ddl::JobWrapper,
        service: &dyn DdlService,
    ) -> Result<Option<i64>, DriverError> {
        let locks = match &job.args {
            tidb_model::JobArgsValue::LockTables(Some(args)) => Some(args.read().clone()),
            _ => None,
        };
        let result =
            self.run_executor_call(sql, parsed, service, locks.as_ref(), |service, context| {
                service.do_ddl_job_wrapper(context, job)
            });
        result
    }

    /// Go `DDLExec.Next`: common context and session effects around an executor call.
    fn run_executor_call(
        &mut self,
        sql: &str,
        parsed: &tidb_ast::Stmt,
        service: &dyn DdlService,
        locks: Option<&tidb_model::LockTablesArgs>,
        execute: impl FnOnce(&dyn DdlService, &StmtContext) -> Result<ClusterDdlReport, DriverError>,
    ) -> Result<Option<i64>, DriverError> {
        let drop_database = match parsed {
            tidb_ast::Stmt::Ddl(ddl) => match &**ddl {
                tidb_ast::DdlStmt::DropDatabase { name, .. } => Some(name.as_str()),
                _ => None,
            },
            _ => None,
        };
        if drop_database.is_some_and(|name| name.go_lower_eq("mysql")) {
            return Err(DriverError::from(
                tidb_dbterror::ERR_FORBIDDEN_DDL
                    .gen_with_stack("Drop 'mysql' database is forbidden".to_owned()),
            ));
        }
        service.check_cached_table_ddl(parsed, &self.vars.current_db)?;
        self.validate_executor_ddl_references(parsed)?;
        if let Some(args) = locks {
            self.begin_lock_tables_args(args);
        }
        let lock_effect = self.cluster_executor_lock_effect(parsed);
        let context = self.ddl_statement_context().with_ddl_query(sql);
        let report = execute(service, &context);
        self.finish_ddl_statement_context(&context);
        self.drain_fold_stash();
        let (created_id, applied, warnings) = match report? {
            ClusterDdlReport::Applied {
                created_id,
                warnings,
                ..
            } => (created_id, true, warnings),
            ClusterDdlReport::AlreadySatisfied { warnings, .. } => (None, false, warnings),
        };
        if let Some(args) = locks {
            self.finish_lock_tables_args(args);
        }
        self.finish_cluster_ddl_lock_effect(lock_effect, created_id);
        if applied {
            self.record_last_ddl_info(sql);
        }
        for warning in warnings {
            self.append_sql_warning(warning);
        }
        if drop_database.is_some_and(|name| self.current_database().go_lower_eq(name)) {
            self.deselect_database();
            self.vars.set_system(
                "character_set_database",
                tidb_mysql::DefaultCharset.to_owned(),
            )?;
            self.vars.set_system(
                "collation_database",
                tidb_mysql::DefaultCollationName.to_owned(),
            )?;
        }
        Ok(created_id)
    }

    /// Go `executor` changes the issuing statement's context while building its job.
    fn finish_ddl_statement_context(&mut self, job_context: &StmtContext) {
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
    }
}
