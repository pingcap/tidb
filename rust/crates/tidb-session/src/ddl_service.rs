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
//! [`tidb_executor::ddl_exec::DdlService`]; `DDLExec.Next` around that call is
//! `tidb_executor::ddl_exec::DdlExecSession`, whose session state this module
//! implements, so that the
//! production node, and later the in-process tier, differ only in the service
//! they hand in and in how they commit the open transaction.

use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use tidb_driver_error::DriverError;
use tidb_sessionctx::StmtContext;

use crate::Session;
use tidb_hack::go_to_lower;

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
}

/// Go `DDLExec.Next`'s session.
impl tidb_executor::ddl_exec::DdlExecSession for Session {
    type LockEffect = crate::table_lock::ClusterDdlLockEffect;

    fn current_database(&self) -> &str {
        Session::current_database(self)
    }

    fn validate_executor_ddl_references(&mut self, parsed: &tidb_ast::Stmt) -> Result<(), DriverError> {
        Session::validate_executor_ddl_references(self, parsed)
    }

    fn cluster_executor_lock_effect(&self, parsed: &tidb_ast::Stmt) -> Self::LockEffect {
        Session::cluster_executor_lock_effect(self, parsed)
    }

    fn finish_cluster_ddl_lock_effect(&mut self, effect: Self::LockEffect, created_id: Option<i64>) {
        Session::finish_cluster_ddl_lock_effect(self, effect, created_id);
    }

    fn ddl_statement_context(&self) -> StmtContext {
        ResetContextOfStmt::ddl_statement_context(self)
    }

    fn drain_fold_stash(&mut self) {
        Session::drain_fold_stash(self);
    }

    fn record_last_ddl_info(&mut self, query: &str) {
        Session::record_last_ddl_info(self, query);
    }

    fn append_sql_warning(&mut self, warning: tidb_context::SqlWarn) {
        Session::append_sql_warning(self, warning);
    }

    fn deselect_database(&mut self) {
        Session::deselect_database(self);
    }

    fn set_system_var(&mut self, name: &str, value: String) -> Result<(), DriverError> {
        self.vars.set_system(name, value)?;
        Ok(())
    }

    fn set_hypo_indexes(&mut self, indexes: tidb_variable::hypo_index::HypoIndexes) {
        self.hypo_indexes = std::sync::Arc::new(indexes);
    }

    fn set_hypo_tiflash_replica(&mut self, schema: &str, table: &str, add: bool) {
        std::sync::Arc::make_mut(&mut self.hypo_tiflash_replicas).set(schema, table, add);
    }
}
