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

//! Go `pkg/executor/ddl.go`: `DDLExec`.
//!
//! It lives in the session crate rather than `tidb-executor` because what
//! its `Next` reads and changes instead of (or beside) a DDL job is session
//! state the executor crate cannot reach: the LOCAL temporary tables
//! (`SessionVars.LocalTemporaryTables`, through
//! [`crate::temptable::TemporaryTableDdl`]), the hypothetical indexes and
//! TiFlash replicas (`SessionVars.HypoIndexes` / `HypoTiFlashReplicas`) and
//! the transaction `sessiontxn.NewTxnInStmt` ends. The job itself is run by
//! the session's node (`Session::run_ddl_statement` over its `DdlService`);
//! the node learns from [`Session::ddl_exec_next`] whether `Next` needs one,
//! and from [`Session::ddl_preserves_transaction`] whether `Next` returns
//! before `NewTxnInStmt`.

use tidb_ast::{DdlStmt, Stmt};
use tidb_executor::DriverError;

use crate::{SchemaErrorKind, Session, StmtOutput};

/// Where Go `DDLExec.Next` takes one DDL statement.
pub(crate) enum DdlExecNext<'a> {
    /// Before `NewTxnInStmt`: `createSessionTemporaryTable`.
    CreateSessionTemporaryTable(&'a tidb_ast::CreateTableStmt),
    /// Before `NewTxnInStmt`: a `DROP TEMPORARY TABLE` that names tables
    /// which are not the session's LOCAL temporary ones. They are one
    /// `ErrTableDropExists` (a Note under `IF EXISTS`), and nothing is
    /// dropped.
    DropTemporaryNonLocal {
        names: Vec<Vec<String>>,
        if_exists: bool,
    },
    /// Before `NewTxnInStmt`: a DROP naming only LOCAL temporary tables,
    /// which `dropLocalTemporaryTables` drops without a job.
    DropLocalTemporaryTables(Vec<Vec<String>>),
    /// After `NewTxnInStmt`: `executeTruncateTable` of a LOCAL temporary
    /// table, `TruncateLocalTemporaryTable`.
    TruncateLocalTemporaryTable(&'a [String]),
    /// After `NewTxnInStmt`: `ErrUnsupportedLocalTempTableDDL` naming the
    /// statement (`executeAlterTable`, `executeCreateIndex`,
    /// `executeDropIndex`, `executeRenameTable`).
    UnsupportedLocalTempTableDdl(&'static str),
    /// After `NewTxnInStmt`: the job drops `persistent` (the DROP without
    /// its LOCAL temporary tables), and once it succeeds
    /// `dropLocalTemporaryTables` drops `local`.
    DropTableJob {
        persistent: tidb_ast::DropTableStmt,
        local: Vec<Vec<String>>,
    },
    /// After `NewTxnInStmt`: the DDL executor's, which runs the statement's
    /// job or changes the session's hypothetical indexes and TiFlash
    /// replicas.
    Job,
}

impl DdlExecNext<'_> {
    /// Whether `Next` returns before `sessiontxn.NewTxnInStmt`, which keeps
    /// the user's transaction open.
    pub(crate) fn returns_before_new_txn(&self) -> bool {
        matches!(
            self,
            Self::CreateSessionTemporaryTable(_)
                | Self::DropTemporaryNonLocal { .. }
                | Self::DropLocalTemporaryTables(_)
        )
    }
}

impl Session {
    /// Go `DDLExec.Next`'s statement switch: where it takes `ddl`, judged
    /// against the session's LOCAL temporary tables (`getLocalTemporaryTable`).
    pub(crate) fn ddl_exec_next<'a>(&self, ddl: &'a DdlStmt) -> DdlExecNext<'a> {
        let refuse = |path: &[String], statement| {
            self.is_local_temporary_table_path(path)
                .then_some(DdlExecNext::UnsupportedLocalTempTableDdl(statement))
        };
        let next = match ddl {
            DdlStmt::CreateTable(create)
                if create.temporary == tidb_ast::CreateTableTemporary::Local =>
            {
                Some(DdlExecNext::CreateSessionTemporaryTable(create))
            }
            DdlStmt::DropTable(drop) => Some(self.drop_table_next(drop)),
            DdlStmt::TruncateTable(path) => self
                .is_local_temporary_table_path(path)
                .then_some(DdlExecNext::TruncateLocalTemporaryTable(path)),
            DdlStmt::AlterTable(alter) => refuse(&alter.name, "ALTER TABLE"),
            DdlStmt::CreateIndex(create) => refuse(&create.table, "CREATE INDEX"),
            DdlStmt::DropIndex(drop) => refuse(&drop.table, "DROP INDEX"),
            // `executeRenameTable` checks every source before any job.
            DdlStmt::RenameTable(rename) => rename
                .pairs
                .iter()
                .find_map(|(from, _)| refuse(from, "RENAME TABLE")),
            _ => None,
        };
        next.unwrap_or(DdlExecNext::Job)
    }

    /// Go `DDLExec.Next`'s `DropTableStmt` case: the LOCAL temporary tables
    /// come out of the statement, walked backwards so the rest keep their
    /// order; `DROP TEMPORARY TABLE` must name nothing else; a DROP left
    /// naming nothing returns without a job.
    fn drop_table_next<'a>(&self, drop: &tidb_ast::DropTableStmt) -> DdlExecNext<'a> {
        let (persistent, local) = tidb_executor::split_drop_table_targets(drop, |path| {
            self.is_local_temporary_table_path(path)
        });
        if drop.temporary == tidb_ast::DropTemporary::Local && !persistent.names.is_empty() {
            return DdlExecNext::DropTemporaryNonLocal {
                names: persistent.names,
                if_exists: drop.if_exists,
            };
        }
        if persistent.names.is_empty() {
            return DdlExecNext::DropLocalTemporaryTables(local);
        }
        if local.is_empty() {
            return DdlExecNext::Job;
        }
        DdlExecNext::DropTableJob { persistent, local }
    }

    /// Whether Go `DDLExec.Next` returns before `sessiontxn.NewTxnInStmt`
    /// for `stmt`, keeping the user's transaction: a LOCAL temporary CREATE,
    /// any `DROP TEMPORARY TABLE`, and a DROP of LOCAL temporary tables only.
    pub(crate) fn ddl_preserves_transaction(&self, stmt: &Stmt) -> bool {
        matches!(stmt, Stmt::Ddl(ddl) if self.ddl_exec_next(ddl).returns_before_new_txn())
    }

    /// Go `DDLExec.Next` for the DDL the session answers: a LOCAL temporary
    /// table's and the refusals of the DDL one cannot take. `None` for a job, which the session's node
    /// runs. The caller has already ended the transaction where `Next`
    /// calls `NewTxnInStmt` (see [`Self::ddl_preserves_transaction`]).
    pub(crate) fn ddl_exec(&mut self, stmt: &Stmt) -> Result<Option<StmtOutput>, DriverError> {
        let Stmt::Ddl(ddl) = stmt else {
            return Ok(None);
        };
        match self.ddl_exec_next(ddl) {
            DdlExecNext::CreateSessionTemporaryTable(create) => {
                self.create_session_temporary_table(create)?;
            }
            DdlExecNext::DropTemporaryNonLocal { names, if_exists } => {
                let current_db = self.vars.current_db.clone();
                let names = names
                    .iter()
                    .map(|path| {
                        tidb_executor::driver::split_table_path_pub(path, &current_db)
                            .map(|(schema, name)| format!("{schema}.{name}"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let error = DriverError::Schema(SchemaErrorKind::BadTable(names.join(",")));
                if !if_exists {
                    return Err(error);
                }
                self.append_suppressed(error);
            }
            DdlExecNext::DropLocalTemporaryTables(local) => {
                self.drop_local_temporary_tables(&local)?;
            }
            DdlExecNext::TruncateLocalTemporaryTable(path) => {
                let current_db = self.vars.current_db.clone();
                let (schema, table) =
                    tidb_executor::driver::split_table_path_pub(path, &current_db)?;
                self.temporary_table_ddl()
                    .truncate_local_temporary_table(schema, table)?;
            }
            DdlExecNext::UnsupportedLocalTempTableDdl(statement) => {
                return Err(DriverError::UnsupportedLocalTempTableDDL(statement));
            }
            DdlExecNext::DropTableJob { .. } => return Ok(None),
            DdlExecNext::Job => return Ok(None),
        }
        Ok(Some(StmtOutput::Affected(0)))
    }

    /// Go `DDLExec.dropLocalTemporaryTables`.
    pub(crate) fn drop_local_temporary_tables(
        &mut self,
        local: &[Vec<String>],
    ) -> Result<(), DriverError> {
        let current_db = self.vars.current_db.clone();
        for path in local {
            let (schema, name) = tidb_executor::driver::split_table_path_pub(path, &current_db)?;
            self.temporary_table_ddl()
                .drop_local_temporary_table(schema, name)?;
        }
        Ok(())
    }

    /// Go `DDLExec.createSessionTemporaryTable`.
    fn create_session_temporary_table(
        &mut self,
        create: &tidb_ast::CreateTableStmt,
    ) -> Result<(), DriverError> {
        let current_db = self.vars.current_db.clone();
        let (schema, table) =
            tidb_executor::driver::split_table_path_pub(&create.name, &current_db)?;
        let info_schema = self.info_schema.clone().ok_or_else(|| {
            DriverError::unsupported("a LOCAL temporary table needs the session's infoschema")
        })?;
        // Go's preprocessor resolves the LIKE source first.
        let like = match &create.like_table {
            Some(source) => {
                let (source_schema, source_table) =
                    tidb_executor::driver::split_table_path_pub(source, &current_db)?;
                let source = tidb_ddl::cluster_ddl::create_object::resolve_like_source(
                    &info_schema,
                    source_schema,
                    source_table,
                    create.temporary,
                )
                .map_err(|error| ddl_error(&error))?;
                Some((source_schema, source))
            }
            None => None,
        };
        let Some((database, _)) = info_schema.schema_by_name(schema) else {
            return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                schema.to_owned(),
            )));
        };
        if self.is_local_temporary_table(schema, table) {
            let exists =
                DriverError::Schema(SchemaErrorKind::TableExists(format!("{schema}.{table}")));
            if create.if_not_exists {
                self.append_suppressed(exists);
                return Ok(());
            }
            return Err(exists);
        }
        let ctx = self.ddl_statement_context();
        let mut warnings = Vec::new();
        let built = tidb_ddl::cluster_ddl::create_object::build_session_temporary_table_info(
            database,
            create,
            table,
            like,
            &ctx,
            &mut warnings,
        );
        // What the build warned about (a discarded CHECK, a LINEAR
        // partitioning, a truncated key part), in the order it filed them.
        self.drain_eval_warnings(&ctx);
        let info = built.map_err(|error| ddl_error(&error))?;
        for warning in warnings {
            self.append_sql_warning(warning);
        }
        self.temporary_table_ddl()
            .create_local_temporary_table(database, info)
    }
}

/// A DDL package error as the statement's error.
pub(crate) fn ddl_error(error: &tidb_ddl::cluster_ddl::DdlPlanError) -> DriverError {
    DriverError::Mysql(tidb_executor::MysqlError::from_sql_error(
        error.to_sql_error(),
    ))
}
