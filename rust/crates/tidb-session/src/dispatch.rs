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

//! Statement dispatch: the arms that send one parsed statement to the executor
//! entry point that answers it.
//!
//! This is the body of Go's `session.ExecuteStmt` after the statement-boundary
//! bookkeeping [`Session::run_with_columns`] does around it. The order of the
//! doors matters and is Go's: the schema/account arm runs first (it carries
//! DDL's implicit commit), then transaction control and `SET`, then the parsed
//! dispatch over query/DML/DDL.

use tidb_ast::{DdlStmt, DmlStmt, SessionStmt, Stmt};
use tidb_executor::{Catalog, DriverError, SchemaErrorKind};

use crate::warnings::UNSUPPORTED_CREATE_PARTITION_CODE;
use crate::{infoschema, statement_kind_of, Session, StatementKind, StmtOutput, WarningLevel};
use crate::{CHECK_CONSTRAINT_IS_OFF_CODE, CHECK_CONSTRAINT_IS_OFF_MESSAGE};

/// Names the AST variant behind a refused statement, for a "not supported
/// yet" message that says WHAT it refused instead of just that it did.
///
/// Every statement enum here derives `Debug`, and a derived `Debug` for an
/// enum always starts with the bare variant name (`ShowMasterStatus`, or
/// `Explain(ExplainStmt { .. })` for one carrying a payload) -- so taking the
/// leading identifier out of `{:?}` names the variant without a second,
/// separately-maintained match arm per statement kind.
pub(crate) fn variant_name<T: std::fmt::Debug>(value: &T) -> String {
    format!("{value:?}")
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// Names a statement one layer deeper than [`Stmt`]'s own variant: naming the
/// wrapper alone ("Ddl", "Admin") would repeat what the caller already knows
/// it saw a DDL or admin statement, so this names the DDL/ADMIN/SESSION kind
/// it wraps instead -- the same depth `explain_stmt`'s own "EXPLAIN of a WITH
/// clause" / "EXPLAIN of a set operation" messages already name theirs at.
pub(crate) fn stmt_kind_name(stmt: &Stmt) -> String {
    match stmt {
        Stmt::Query(query) => variant_name(&**query),
        Stmt::Dml(dml) => variant_name(&**dml),
        Stmt::Ddl(ddl) => variant_name(&**ddl),
        Stmt::Admin(admin) => variant_name(&**admin),
        Stmt::Session(session) => variant_name(&**session),
    }
}

impl Session {
    /// Applies `USE`, `CREATE DATABASE`, `DROP DATABASE`, `SHOW DATABASES`
    /// and `SHOW TABLES`.
    ///
    /// Returns `Some(output)` for those statements and `None` for anything
    /// else, so a caller can dispatch without re-parsing.
    ///
    /// This is also where DDL's IMPLICIT COMMIT lives, because every DDL
    /// statement passes through here before reaching its own arm --
    /// see the `Stmt::Ddl` arm below.
    pub fn apply_schema_statement(&mut self, sql: &str) -> Result<Option<StmtOutput>, DriverError> {
        let stmt = self.parse(sql)?;
        self.apply_schema_stmt(&stmt)
    }

    /// [`Self::apply_schema_statement`] over a statement this session already
    /// parsed, so a caller holding the parse does not pay for a second one.
    /// The text form is the wrapper above; the two share this body, so there
    /// is one answer to "is this a schema statement", not two.
    pub(crate) fn apply_schema_stmt(
        &mut self,
        stmt: &Stmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        if matches!(stmt, Stmt::Ddl(_)) {
            // Go commits the open transaction before running any DDL
            // (`session.ExecuteStmt`, which calls `sessiontxn`'s
            // `OnStmtStart` -> `checkBeforeNewTxn` for a DDL node), so the
            // DDL and everything staged before it are already durable when
            // it starts. Captured from TiDB: after
            // `INSERT; BEGIN; INSERT; TRUNCATE TABLE d; ROLLBACK` the table
            // is EMPTY -- the ROLLBACK takes nothing back, because the
            // TRUNCATE committed the insert that preceded it -- and the same
            // `ALTER TABLE ... AUTO_INCREMENT` sequence leaves the in-
            // transaction row stored.
            //
            // Doing this before the DDL runs is also what keeps the DDL off
            // the transaction's WORKING COPY of the catalog: with no open
            // transaction, `with_catalog_mut` reaches the shared catalog, so
            // a TRUNCATE's counter reset lands on the table that survives
            // rather than on a copy about to be discarded.
            self.commit()?;
        }
        match stmt {
            Stmt::Session(session_stmt) => match &**session_stmt {
                SessionStmt::Use(name) => {
                    self.use_database(name)?;
                    Ok(Some(StmtOutput::Affected(0)))
                }
                SessionStmt::SetRole(set_role) => Ok(Some(self.set_role_stmt(set_role)?)),
                SessionStmt::SetDefaultRole(set_default) => {
                    Ok(Some(self.set_default_role_stmt(set_default)?))
                }
                _ => Ok(None),
            },
            Stmt::Ddl(ddl) => match &**ddl {
                tidb_ast::DdlStmt::CreateDatabase {
                    if_not_exists,
                    name,
                    options,
                } => {
                    if !options.is_empty() {
                        return Err(DriverError::Unsupported(
                            "database charset and collation options are not supported yet",
                        ));
                    }
                    let created =
                        self.with_catalog_mut(|catalog| Ok(catalog.create_database(name)))?;
                    // Go raises ErrDBCreateExists unless IF NOT EXISTS.
                    if !created && !*if_not_exists {
                        return Err(DriverError::Schema(SchemaErrorKind::DatabaseExists(
                            name.clone(),
                        )));
                    }
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::DdlStmt::DropDatabase { if_exists, name } => {
                    let dropped =
                        self.with_catalog_mut(|catalog| Ok(catalog.drop_database(name)))?;
                    // Go raises ErrDBDropExists unless IF EXISTS.
                    if !dropped && !*if_exists {
                        return Err(DriverError::Schema(SchemaErrorKind::UnknownDatabase(
                            name.clone(),
                        )));
                    }
                    // Dropping the current database leaves the session with
                    // none selected, which is Go's ErrNoDB state for the next
                    // unqualified statement.
                    if dropped && self.current_db.eq_ignore_ascii_case(name) {
                        self.current_db.clear();
                    }
                    Ok(Some(StmtOutput::Affected(0)))
                }
                tidb_ast::DdlStmt::CreateUser {
                    if_not_exists,
                    users,
                    tls_options,
                    resource_options,
                    password_options,
                    comment_or_attribute,
                    resource_group,
                } => Ok(Some(self.create_user_stmt(
                    *if_not_exists,
                    users,
                    tls_options,
                    resource_options,
                    password_options,
                    comment_or_attribute,
                    resource_group,
                )?)),
                tidb_ast::DdlStmt::DropUser {
                    is_role,
                    if_exists,
                    users,
                } => Ok(Some(self.drop_user_stmt(*is_role, *if_exists, users)?)),
                tidb_ast::DdlStmt::AlterUser(alter) => Ok(Some(self.alter_user_stmt(alter)?)),
                tidb_ast::DdlStmt::RenameUser { pairs } => Ok(Some(self.rename_user_stmt(pairs)?)),
                tidb_ast::DdlStmt::CreateRole {
                    if_not_exists,
                    roles,
                } => Ok(Some(self.create_role_stmt(*if_not_exists, roles)?)),
                _ => Ok(None),
            },
            Stmt::Admin(admin) => self.dispatch_admin_stmt(admin),
            _ => Ok(None),
        }
    }

    /// Runs a `SELECT` whose `FROM` names an `information_schema` table.
    ///
    /// The virtual rows are materialized into a scratch catalog and then run
    /// through the ordinary plan, so `WHERE`, `ORDER BY`, `LIMIT`, expressions
    /// and aggregates all behave as they do over a stored table. Go reaches
    /// the same place differently -- its memory tables are real tables to the
    /// planner -- but the requirement is the same: a predicate over a virtual
    /// table must filter it.
    ///
    /// Returns `None` when the statement is an ordinary one, so the caller
    /// falls through to the storage path.
    ///
    /// DEFERRED (documented): a join between a virtual table and a stored one,
    /// because the scratch catalog holds only the virtual side. Such a
    /// statement is rejected rather than answered from half the data.
    fn run_information_schema_select(
        &mut self,
        select: &tidb_ast::SelectStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        let Some(join) = &select.from else {
            return Ok(None);
        };
        let tidb_ast::JoinNode::Table(table_ref) = &join.left else {
            return Ok(None);
        };
        // `information_schema.X`, or a bare `X` while that schema is current.
        let (schema, table_name) = match table_ref.name.as_slice() {
            [name] => (self.current_db.clone(), name.clone()),
            [schema, name] => (schema.clone(), name.clone()),
            _ => return Ok(None),
        };
        if !infoschema::is_information_schema(&schema) {
            return Ok(None);
        }
        if join.right.is_some() {
            return Err(DriverError::Unsupported(
                "joining an information_schema table is not supported yet",
            ));
        }
        let Some(columns) = infoschema::table_schema(&table_name) else {
            return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                "{schema}.{table_name}"
            ))));
        };
        // `PROCESSLIST` is session/registry state, not catalog state, so it
        // is built directly rather than through `infoschema::table_rows`,
        // which only ever sees the catalog.
        let rows = if table_name.eq_ignore_ascii_case("PROCESSLIST") {
            self.process_list_table_rows()
        } else if table_name.eq_ignore_ascii_case("USER_PRIVILEGES") {
            self.user_privileges_table_rows()
        } else {
            self.with_catalog_mut(|catalog| {
                Ok(infoschema::table_rows(&table_name, catalog).unwrap_or_default())
            })?
        };

        // A scratch catalog holding just this table, so the ordinary plan runs
        // over it.
        let mut scratch = Catalog::default();
        scratch.register_mem_in(
            infoschema::INFORMATION_SCHEMA,
            &table_name,
            tidb_executor::MemTable { columns, rows },
        );
        let ctx = self.statement_context(false);
        let (columns, rows) = tidb_executor::run_select_meta_stmt(
            select,
            &scratch,
            infoschema::INFORMATION_SCHEMA,
            &ctx,
        )?;
        self.drain_eval_warnings(&ctx);
        Ok(Some(StmtOutput::Rows { columns, rows }))
    }

    pub(crate) fn execute_statement(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        // One parse serves every door below. `sql_mode` is what decides how a
        // statement lexes, and nothing between here and execution changes it
        // -- the `SET` that could is itself one of these doors, and it returns
        // before the next statement is read -- so re-parsing the same text
        // under the same mode could only ever produce the same tree. The four
        // parses this replaces cost ~6 us of a ~13.5 us `SELECT 1`.
        //
        // It is also the statement boundary: Go gives every statement a fresh
        // `StatementContext`, so what `SHOW WARNINGS` reports always belongs
        // to the statement before it, and the OK/EOF packet's warning count
        // starts over here too.
        let mut stmt = self.parse_at_statement_boundary(sql)?;
        // The non-prepared plan cache reads the SAME parse every door below
        // uses. It only decides whether this statement's plan would already
        // have been there; it never replaces the planning that follows.
        self.probe_non_prepared_plan_cache(&stmt);
        // USE / CREATE DATABASE / DROP DATABASE / SHOW DATABASES / SHOW TABLES.
        if let Some(output) = self.apply_schema_stmt(&stmt)? {
            return Ok(output);
        }
        // BEGIN / COMMIT / ROLLBACK and SET both have their own entry points
        // for the wire front, which answers them with an OK packet carrying
        // a status flag. Routing them here too makes `run` the single door
        // every statement can go through, which is what a client expects of
        // one connection.
        if self.control_transaction_stmt(&stmt)?.is_some() {
            return Ok(StmtOutput::Affected(0));
        }
        if self.apply_set_stmt(&stmt)?.is_some() {
            return Ok(StmtOutput::Affected(0));
        }
        // SQL-level prepared statements are answered before variable binding,
        // because a `USING` entry is a variable whose VALUE this statement
        // must read itself (Go's `usingParam.Eval`) rather than one to
        // substitute into the `EXECUTE`'s own text. `EXECUTE` re-enters this
        // same function with the bound statement, so the inner statement takes
        // every door -- schema, transaction control, dispatch -- that a
        // directly written one does.
        if let Stmt::Session(session_stmt) = &stmt {
            match &**session_stmt {
                SessionStmt::Prepare { name, source } => {
                    self.prepare_statement(name, source)?;
                    return Ok(StmtOutput::Affected(0));
                }
                SessionStmt::Execute { name, using } => {
                    return self.execute_prepared_statement(name, using);
                }
                SessionStmt::Deallocate(name) => {
                    self.deallocate_prepared_statement(name)?;
                    return Ok(StmtOutput::Affected(0));
                }
                _ => {}
            }
        }
        // `@@x` / `@x` read the session's own state, so they are bound before
        // the statement reaches the driver.
        // A `SET_VAR` hint overlays the session BEFORE anything reads a
        // variable, which is where Go applies it too: the optimizer installs
        // it, and expression rewriting -- the `@@x` reads below -- happens
        // after.
        self.apply_set_var_hints(&stmt);
        self.bind_variables(&mut stmt)?;
        self.try_add_extra_limit(&mut stmt);
        // The mode the DDL arms below re-parse under: the one in force NOW,
        // taken before execution so a statement is lexed exactly once per
        // meaning.
        let sql_mode = self.scanner_sql_mode();
        // Only an allocating INSERT sets it; every other statement reports 0.
        self.statement_insert_id = 0;
        // Go sets the `InSelectStmt`/`In*Stmt` bits here, before execution,
        // so a statement that FAILS still classifies itself for the next
        // statement's `ROW_COUNT()` (captured: a failed SELECT leaves -1, a
        // failed INSERT leaves 0).
        self.statement_kind = statement_kind_of(&stmt);
        // With autocommit OFF a read or a write joins a transaction rather
        // than standing alone; DDL is left out because it commits the open
        // transaction instead of joining it.
        if self.statement_kind != StatementKind::Other {
            self.begin_implicit_transaction()?;
        }
        // Go's preprocessor runs before planning, so a gated clause is
        // refused before any table is touched.
        if let Stmt::Query(query) = &stmt {
            self.check_noop_functions(query)?;
            self.check_query_clauses(query)?;
        }
        // Go raises ErrNoDB where an unqualified NAME is resolved, not for
        // every statement: `SELECT 1` and `SELECT DATABASE()` both run with
        // no database selected (captured). The driver's own
        // `split_table_path` raises it at the resolution point, which is
        // where Go's does.
        match &stmt {
            Stmt::Query(query) => {
                let tidb_ast::QueryStmt::Select(select) = &**query else {
                    // A set operation runs through its own fold.
                    let tidb_ast::QueryStmt::SetOpr(set_opr) = &**query else {
                        unreachable!("a query is a SELECT or a set operation")
                    };
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(false);
                    let (columns, rows) = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_set_opr_stmt(set_opr, catalog, &current_db, &ctx)
                    })?;
                    self.drain_eval_warnings(&ctx);
                    return Ok(StmtOutput::Rows { columns, rows });
                };
                // An information_schema table is virtual: its rows are
                // computed from the catalog rather than read from storage.
                if let Some(output) = self.run_information_schema_select(select)? {
                    return Ok(output);
                }
                let current_db = self.current_db.clone();
                let ctx = self.statement_context(false);
                let (columns, rows) = self.with_catalog_mut(|catalog| {
                    tidb_executor::run_select_meta_stmt(select, catalog, &current_db, &ctx)
                })?;
                self.drain_eval_warnings(&ctx);
                Ok(StmtOutput::Rows { columns, rows })
            }
            Stmt::Dml(dml) => match &**dml {
                DmlStmt::Insert(_) => {
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(true);
                    let result = self.with_staged_catalog(|catalog| {
                        tidb_executor::run_insert_reporting(sql, catalog, &current_db, &ctx)
                    });
                    self.drain_eval_warnings(&ctx);
                    // Go `session.LastInsertID()`, the OK packet's field:
                    // `StmtCtx.LastInsertID` when the statement PUBLISHED an
                    // allocated id, `StmtCtx.InsertID` -- the last explicit
                    // value -- otherwise. Both come off the same context the
                    // publication above reads, so the wire and
                    // `LAST_INSERT_ID()` cannot drift apart: what differs is
                    // only the fallback Go itself applies.
                    //
                    // Captured from TiDB: an allocating insert reports the id
                    // on both; `INSERT INTO t (id,v) VALUES (50,2)` reports 50
                    // on the wire while `LAST_INSERT_ID()` stays where it was;
                    // an `INSERT IGNORE` whose only row is a duplicate burns
                    // an id but reports 0 on the wire.
                    // The publication itself is promoted at the statement
                    // boundary by `publish_statement_status`, off the same
                    // cell this reads -- one channel, two readers.
                    self.statement_insert_id = ctx
                        .published_last_insert_id()
                        .unwrap_or_else(|| ctx.given_insert_id());
                    let (affected, _) = result?;
                    Ok(StmtOutput::Affected(affected))
                }
                DmlStmt::Update(_) => {
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(true);
                    let output = self.with_staged_catalog(|catalog| {
                        Ok(StmtOutput::Affected(tidb_executor::run_update_in(
                            sql,
                            catalog,
                            &current_db,
                            &ctx,
                        )?))
                    });
                    self.drain_eval_warnings(&ctx);
                    output
                }
                DmlStmt::Delete(_) => {
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(true);
                    let output = self.with_staged_catalog(|catalog| {
                        Ok(StmtOutput::Affected(tidb_executor::run_delete_in(
                            sql,
                            catalog,
                            &current_db,
                            &ctx,
                        )?))
                    });
                    self.drain_eval_warnings(&ctx);
                    output
                }
                other => Err(DriverError::UnsupportedKind(format!(
                    "this DML statement kind ({}) is not supported yet",
                    variant_name(other)
                ))),
            },
            Stmt::Ddl(ddl) => match &**ddl {
                DdlStmt::RenameTable(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_rename_table_in(sql, catalog, &current_db, sql_mode)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::TruncateTable(_) => {
                    let current_db = self.current_db.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_truncate_table_in(sql, catalog, &current_db, sql_mode)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::CreateIndex(_) => {
                    let current_db = self.current_db.clone();
                    // An index backfill WRITES the entries it computes, so it
                    // evaluates at the write level: captured from TiDB,
                    // `alter table t add index i((100/a))` over a row with
                    // `a = 0` is 1365 under the default SQL mode and succeeds
                    // under `sql_mode = ''`, exactly as the INSERT of such a
                    // row does.
                    let ctx = self.statement_context(true);
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_create_index_in(sql, catalog, &current_db, &ctx)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::DropIndex(_) => {
                    let current_db = self.current_db.clone();
                    // `DROP INDEX` REBUILDS each entry's key to delete it, so
                    // it needs the session's `@@time_zone` for the same reason
                    // writing the entry did.
                    let ctx = self.statement_context(true);
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_index_in(sql, catalog, &current_db, &ctx)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::AlterTable(_) => {
                    let current_db = self.current_db.clone();
                    // `ADD INDEX` backfills, so the same write level applies.
                    let ctx = self.statement_context(true);
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_alter_table_in(sql, catalog, &current_db, &ctx)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::DropTable(_) => {
                    let current_db = self.current_db.clone();
                    let foreign_key_checks = self.foreign_key_checks();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_table_in(
                            sql,
                            catalog,
                            &current_db,
                            sql_mode,
                            foreign_key_checks,
                        )?;
                        // MySQL answers DDL with a zero affected-row count.
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::CreateTable(create) => {
                    let current_db = self.current_db.clone();
                    let foreign_key_checks = self.foreign_key_checks();
                    let enable_check_constraint = self.enable_check_constraint();
                    // The session's own evaluation context, which carries
                    // `@@time_zone`: `CREATE TABLE` folds a column `DEFAULT`
                    // and a RANGE partition bound at DDL time, and both can
                    // read the zone.
                    let ctx = self.statement_context(false);
                    // Go `pkg/ddl/create_table.go` and `add_column.go` warn
                    // once per CHECK constraint they discard, before the
                    // table is built; the constraint itself never reaches
                    // the stored `TableInfo`.
                    let discarded_checks = if enable_check_constraint {
                        0
                    } else {
                        tidb_executor::check_constraint_count(create)
                    };
                    let done = self.with_catalog_mut(|catalog| {
                        Ok(StmtOutput::Done(tidb_executor::run_create_table_in(
                            sql,
                            catalog,
                            &current_db,
                            sql_mode,
                            foreign_key_checks,
                            enable_check_constraint,
                            &ctx,
                        )?))
                    });
                    if done.is_ok() {
                        for _ in 0..discarded_checks {
                            self.append_warning(
                                WarningLevel::Warning,
                                CHECK_CONSTRAINT_IS_OFF_CODE,
                                CHECK_CONSTRAINT_IS_OFF_MESSAGE.to_owned(),
                            );
                        }
                        // Go accepts `LINEAR HASH` and builds the plain
                        // non-linear table, warning that it did so.
                        if let Some(message) = tidb_executor::linear_partitioning_warning(create) {
                            self.append_warning(
                                WarningLevel::Warning,
                                UNSUPPORTED_CREATE_PARTITION_CODE,
                                message,
                            );
                        }
                    }
                    done
                }
                DdlStmt::CreateView(create) => {
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(false);
                    let create = create.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_create_view_in(&create, catalog, &current_db, &ctx)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                // Go answers every sequence DDL with a zero affected-row
                // count, as it does every other DDL.
                DdlStmt::CreateSequence(create) => {
                    let current_db = self.current_db.clone();
                    let create = create.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_create_sequence_in(&create, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::AlterSequence(alter) => {
                    let current_db = self.current_db.clone();
                    let alter = alter.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_alter_sequence_in(&alter, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::DropSequence(drop) => {
                    let current_db = self.current_db.clone();
                    let drop = drop.clone();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_sequence_in(&drop, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                DdlStmt::DropView { if_exists, names } => {
                    let current_db = self.current_db.clone();
                    let (if_exists, names) = (*if_exists, names.clone());
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_view_in(if_exists, &names, catalog, &current_db)?;
                        Ok(StmtOutput::Affected(0))
                    })
                }
                other => Err(DriverError::UnsupportedKind(format!(
                    "this DDL statement kind ({}) is not supported yet",
                    variant_name(other)
                ))),
            },
            Stmt::Admin(admin) => Err(DriverError::UnsupportedKind(format!(
                "this statement kind (ADMIN {}) is not supported yet",
                variant_name(&**admin)
            ))),
            Stmt::Session(session) => Err(DriverError::UnsupportedKind(format!(
                "this statement kind ({}) is not supported yet",
                variant_name(&**session)
            ))),
        }
    }

    /// The query clauses this tier parses but cannot execute.
    ///
    /// `INTO OUTFILE` writes a server-side file, which this seed has no path
    /// for; Go returns an empty result set after writing the file, so
    /// executing the query and returning rows instead would be silently
    /// wrong. It is refused rather than ignored.
    ///
    /// ACCEPTED WITH A DEFERRAL (documented): `FOR UPDATE`. TiDB's default
    /// `tidb_txn_mode` is pessimistic, where the clause takes row locks at
    /// read time; this seed's transactions are optimistic, where TiDB itself
    /// takes no read-time lock and resolves the conflict at COMMIT -- which
    /// is exactly what this seed does. The rows returned therefore match;
    /// what is missing is the pessimistic lock, not the result. `OF t`,
    /// `NOWAIT`, `SKIP LOCKED` and `WAIT n` all only shape that missing
    /// lock's waiting behavior, so they are accepted for the same reason.
    fn check_query_clauses(&self, query: &tidb_ast::QueryStmt) -> Result<(), DriverError> {
        let into_outfile = match query {
            tidb_ast::QueryStmt::Select(select) => select.into_outfile.is_some(),
            tidb_ast::QueryStmt::SetOpr(_) => false,
        };
        if into_outfile {
            return Err(DriverError::Unsupported(
                "SELECT ... INTO OUTFILE is not supported yet",
            ));
        }
        Ok(())
    }
}
