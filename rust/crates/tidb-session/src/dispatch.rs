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

/// Every `information_schema` base table a top-level join tree references.
///
/// The ordinary executor resolves bare names against `current_db`; mirror
/// that here so `USE information_schema; SELECT ... FROM tables` materializes
/// the same virtual source as its qualified spelling.
fn information_schema_tables_in_join(
    node: &tidb_ast::JoinNode,
    current_db: &str,
    out: &mut Vec<String>,
) {
    match node {
        tidb_ast::JoinNode::Table(table) => match table.name.as_slice() {
            [name] if infoschema::is_information_schema(current_db) => out.push(name.clone()),
            [schema, name] if infoschema::is_information_schema(schema) => out.push(name.clone()),
            _ => {}
        },
        tidb_ast::JoinNode::Join(join) => {
            information_schema_tables_in_join(&join.left, current_db, out);
            if let Some(right) = &join.right {
                information_schema_tables_in_join(right, current_db, out);
            }
        }
        tidb_ast::JoinNode::Derived { subquery, .. } => {
            information_schema_tables_in_query(subquery, current_db, out);
        }
    }
}

/// Continues [`information_schema_tables_in_join`] across a derived query's
/// SELECT or nested set-operation terms.
fn information_schema_tables_in_query(
    query: &tidb_ast::QueryStmt,
    current_db: &str,
    out: &mut Vec<String>,
) {
    match query {
        tidb_ast::QueryStmt::Select(select) => {
            if let Some(with) = &select.with {
                for cte in &with.ctes {
                    information_schema_tables_in_query(&cte.query, current_db, out);
                }
            }
            if let Some(join) = &select.from {
                information_schema_tables_in_join(&join.left, current_db, out);
                if let Some(right) = &join.right {
                    information_schema_tables_in_join(right, current_db, out);
                }
            }
        }
        tidb_ast::QueryStmt::SetOpr(set_opr) => {
            if let Some(with) = &set_opr.with {
                for cte in &with.ctes {
                    information_schema_tables_in_query(&cte.query, current_db, out);
                }
            }
            for term in &set_opr.terms {
                match &term.body {
                    tidb_ast::SetOprTermBody::Select(select) => {
                        if let Some(join) = &select.from {
                            information_schema_tables_in_join(&join.left, current_db, out);
                            if let Some(right) = &join.right {
                                information_schema_tables_in_join(right, current_db, out);
                            }
                        }
                    }
                    tidb_ast::SetOprTermBody::Nested(nested) => {
                        information_schema_tables_in_query(
                            &tidb_ast::QueryStmt::SetOpr(nested.clone()),
                            current_db,
                            out,
                        );
                    }
                }
            }
        }
    }
}

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
                    if options.iter().any(|option| {
                        !matches!(
                            option,
                            tidb_ast::DatabaseOption::CharacterSet(_)
                                | tidb_ast::DatabaseOption::Collate(_)
                        )
                    }) {
                        return Err(DriverError::unsupported(
                            "this CREATE DATABASE option is not supported yet",
                        ));
                    }
                    let charset = tidb_executor::resolve_database_charset(options)?;
                    let created = self.with_catalog_mut(|catalog| {
                        Ok(catalog.create_database_with_charset(name, charset))
                    })?;
                    // Go raises ErrDBCreateExists unless IF NOT EXISTS, and
                    // under IF NOT EXISTS files that same error as a note:
                    // `Note | 1007 | Can't create database 'test'; database
                    // exists`, captured from `gorun`.
                    if !created {
                        let existing =
                            DriverError::Schema(SchemaErrorKind::DatabaseExists(name.clone()));
                        if !*if_not_exists {
                            return Err(existing);
                        }
                        self.append_suppressed(existing);
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
    fn run_information_schema_select(
        &mut self,
        select: &tidb_ast::SelectStmt,
    ) -> Result<Option<StmtOutput>, DriverError> {
        let Some(join) = &select.from else {
            let Some(with) = &select.with else {
                return Ok(None);
            };
            let mut table_names = Vec::new();
            for cte in &with.ctes {
                information_schema_tables_in_query(&cte.query, &self.current_db, &mut table_names);
            }
            if table_names.is_empty() {
                return Ok(None);
            }
            let ctx = self.statement_context(false);
            let scratch = self.materialize_information_schema_catalog(table_names, &ctx)?;
            let current_db = self.current_db.clone();
            let (columns, rows) =
                tidb_executor::run_select_meta_stmt(select, &scratch, &current_db, &ctx)?;
            self.drain_eval_warnings(&ctx);
            return Ok(Some(StmtOutput::Rows { columns, rows }));
        };
        let mut table_names = Vec::new();
        if let Some(with) = &select.with {
            for cte in &with.ctes {
                information_schema_tables_in_query(&cte.query, &self.current_db, &mut table_names);
            }
        }
        information_schema_tables_in_join(&join.left, &self.current_db, &mut table_names);
        if let Some(right) = &join.right {
            information_schema_tables_in_join(right, &self.current_db, &mut table_names);
        }
        if table_names.is_empty() {
            return Ok(None);
        }
        let ctx = self.statement_context(false);
        let scratch = self.materialize_information_schema_catalog(table_names, &ctx)?;
        let current_db = self.current_db.clone();
        let (columns, rows) =
            tidb_executor::run_select_meta_stmt(select, &scratch, &current_db, &ctx)?;
        self.drain_eval_warnings(&ctx);
        Ok(Some(StmtOutput::Rows { columns, rows }))
    }

    /// Clones this statement's real catalog and overlays each referenced
    /// virtual table with its computed rows.  The result is deliberately a
    /// normal [`Catalog`]: every query shape then reaches exactly the same
    /// executor and optimizer paths as stored tables do.
    fn materialize_information_schema_catalog(
        &mut self,
        mut table_names: Vec<String>,
        ctx: &tidb_executor::StmtContext,
    ) -> Result<Catalog, DriverError> {
        table_names.sort_unstable_by_key(|name| name.to_ascii_lowercase());
        table_names.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
        let mut materialized = Vec::with_capacity(table_names.len());
        for table_name in table_names {
            let Some(columns) = infoschema::table_schema(&table_name) else {
                return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                    "{}.{}",
                    infoschema::INFORMATION_SCHEMA,
                    table_name
                ))));
            };
            let rows = if table_name.eq_ignore_ascii_case("PROCESSLIST") {
                self.process_list_table_rows()
            } else if table_name.eq_ignore_ascii_case("DEADLOCKS") {
                if !self.has_process_privilege() {
                    return Err(DriverError::SpecificAccessDenied("PROCESS".to_owned()));
                }
                self.deadlock_history_table_rows()?
            } else if table_name.eq_ignore_ascii_case("USER_PRIVILEGES") {
                self.user_privileges_table_rows()
            } else if table_name.eq_ignore_ascii_case("TIDB_SERVERS_INFO") {
                self.tidb_servers_info_table_rows()
            } else if table_name.eq_ignore_ascii_case("CLUSTER_INFO") {
                self.cluster_info_table_rows()
            } else {
                let visibility = self.schema_visibility();
                self.with_catalog_mut(|catalog| {
                    Ok(
                        infoschema::table_rows(&table_name, catalog, &visibility, ctx)
                            .unwrap_or_default(),
                    )
                })?
            };
            materialized.push((table_name, columns, rows));
        }
        self.with_catalog_mut(|catalog| {
            let mut scratch = catalog.clone();
            for (table_name, columns, rows) in materialized {
                scratch.register_mem_in(
                    infoschema::INFORMATION_SCHEMA,
                    &table_name,
                    tidb_executor::MemTable { columns, rows },
                );
            }
            Ok(scratch)
        })
    }

    pub(crate) fn execute_statement(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        let stmt = self.parse_at_statement_boundary(sql)?;
        self.execute_parsed_statement(sql, stmt, false, None)
    }

    pub(crate) fn execute_prepared_ast(
        &mut self,
        sql: &str,
        stmt: Stmt,
        cached_point_get: Option<tidb_executor::PreparedPointGetExecution>,
    ) -> Result<StmtOutput, DriverError> {
        self.begin_prepared_statement_boundary(&stmt);
        self.execute_parsed_statement(sql, stmt, true, cached_point_get)
    }

    /// Executes the subset Go serves through `ExecStmt.PointGet`: the cached
    /// plan has already passed the statement-shape, schema, autocommit,
    /// stale-read, binding, and hint gates. Go skips rebuilding visitInfo for
    /// this reused executor; this path likewise avoids revisiting the AST.
    pub(crate) fn execute_cached_prepared_point_get(
        &mut self,
        cached: tidb_executor::PreparedPointGetExecution,
    ) -> Result<StmtOutput, DriverError> {
        self.begin_cached_prepared_query_boundary();
        // `dirty_content` only gates scan/access-path planning. This cached
        // executor owns one handle read and the admission gate already refuses
        // an open transaction, so walking every catalog table cannot affect
        // its result.
        self.refuse_pinned_historical_read()?;
        self.statement_insert_id = 0;
        self.statement_kind = StatementKind::Select;

        let current_db = self.current_db.clone();
        let ctx = self.prepared_point_get_context();
        let result = self.with_catalog_mut(|catalog| {
            tidb_executor::run_prepared_point_get(&cached, catalog, &current_db, &ctx)
        })?;
        let Some((columns, rows)) = result else {
            return Err(DriverError::unsupported(
                "prepared point-get cache was invalidated during the statement",
            ));
        };
        self.found_in_plan_cache = true;
        Ok(StmtOutput::Rows { columns, rows })
    }

    fn execute_parsed_statement(
        &mut self,
        sql: &str,
        mut stmt: Stmt,
        prepared: bool,
        cached_point_get: Option<tidb_executor::PreparedPointGetExecution>,
    ) -> Result<StmtOutput, DriverError> {
        // Go `SelectInto` with `SelectIntoVars`: the query runs as itself and
        // its one row lands in the named user variables. Intercepted at this
        // one door so text and prepared spellings share the rules: more than
        // one row is 1172, a row with the wrong width is 1222, and zero rows
        // leave every variable untouched (MySQL warns 1329; the OK is still
        // an OK).
        if let Stmt::Query(query) = &mut stmt {
            if let tidb_ast::QueryStmt::Select(select) = &mut **query {
                if !select.into_vars.is_empty() {
                    let names = std::mem::take(&mut select.into_vars);
                    let output =
                        self.execute_parsed_statement(sql, stmt, prepared, cached_point_get)?;
                    let StmtOutput::Rows { rows, .. } = output else {
                        return Err(DriverError::unsupported(
                            "SELECT INTO expected a row-producing query",
                        ));
                    };
                    if rows.len() > 1 {
                        return Err(DriverError::SelectIntoMoreThanOneRow);
                    }
                    if let Some(row) = rows.first() {
                        if row.len() != names.len() {
                            return Err(DriverError::SelectIntoColumnMismatch);
                        }
                        let mut vars = self.user_vars.borrow_mut();
                        for (name, value) in names.iter().zip(row.iter()) {
                            vars.insert(name.to_ascii_lowercase(), value.clone());
                        }
                    }
                    return Ok(StmtOutput::Affected(u64::from(!rows.is_empty())));
                }
            }
        }
        // Go hands every statement that is not continuing an open transaction
        // a FRESH membuffer, so `session.HasDirtyContent` answers false for
        // every table at this point -- and `BEGIN` therefore starts from an
        // empty one. A transaction here is a private catalog copy instead, so
        // the staged-write marks have to be told where that boundary is; this
        // is the only door a statement arrives through, so it is the only
        // place that has to say. See `Catalog::clear_dirty_content`.
        if !self.in_transaction() {
            self.lock_catalog()?.clear_dirty_content();
        }
        // The non-prepared plan cache reads the SAME parse every door below
        // uses. It only decides whether this statement's plan would already
        // have been there; it never replaces the planning that follows.
        if !prepared {
            self.probe_non_prepared_plan_cache(&stmt);
        }
        // `apply_schema_stmt` dispatches administrative statements early.
        // EXPLAIN is the one such wrapper whose inner query/DML can own
        // `SET_VAR`, so install that direct-AST overlay before the early
        // dispatch builds the target plan. The ordinary query/DML path below
        // applies its own hints after its early control-statement doors.
        if matches!(&stmt, Stmt::Admin(admin) if matches!(&**admin, tidb_ast::AdminStmt::Explain(_)))
        {
            self.apply_set_var_hints(&stmt);
        }
        // Database DDL is answered by `apply_schema_stmt` below, before the
        // ordinary planner door. Its Go visitInfo must therefore be checked
        // here, while the statement is still side-effect free. Table DDL is
        // checked here too so both early schema arms and later executor arms
        // have one pre-commit privilege boundary.
        if matches!(stmt, Stmt::Ddl(_)) {
            self.require_statement_table_privileges(&stmt)?;
        }
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
        // A pinned historical read must not silently answer from the present.
        // The check sits BELOW the `SET` and transaction-control doors so the
        // session can always pin, unpin, and roll back; every statement that
        // would READ or WRITE is above it and is refused.
        self.refuse_pinned_historical_read()?;
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
        // Go's row-id shard generator belongs to the TRANSACTION, so a
        // statement that IS its own transaction starts a fresh run. Inside an
        // explicit `BEGIN`/`COMMIT` the run continues across statements,
        // which is what makes `tidb_shard_allocate_step` count rows rather
        // than statements.
        if !self.in_transaction() {
            self.row_id_shards.borrow_mut().end_run();
        }
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
        // Go's `CheckPrivilege` runs on the `visitInfo` the planner
        // collected, so it too refuses before any table is touched. This is
        // the single seam for it, because every statement whose privileges
        // are table-scoped reaches here: the account statements
        // `apply_schema_stmt` answers earlier demand their own, statement-
        // specific privileges instead.
        self.require_statement_table_privileges(&stmt)?;
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
                    let mut table_names = Vec::new();
                    information_schema_tables_in_query(query, &current_db, &mut table_names);
                    let (columns, rows) = if table_names.is_empty() {
                        self.with_catalog_mut(|catalog| {
                            tidb_executor::run_set_opr_stmt(set_opr, catalog, &current_db, &ctx)
                        })?
                    } else {
                        let scratch =
                            self.materialize_information_schema_catalog(table_names, &ctx)?;
                        tidb_executor::run_set_opr_stmt(set_opr, &scratch, &current_db, &ctx)?
                    };
                    self.drain_eval_warnings(&ctx);
                    return Ok(StmtOutput::Rows { columns, rows });
                };
                // An information_schema table is virtual: its rows are
                // computed from the catalog rather than read from storage.
                if let Some(output) = self.run_information_schema_select(select)? {
                    return Ok(output);
                }
                // Go plans a matched SQL binding's hints onto the statement
                // before optimizing it (`planner.optimize`), so the binding
                // decides the access path the same way a hint written in the
                // query would. See `crate::binding`.
                let bound = self.bind_statement_hints(&stmt);
                let select = match &bound {
                    Some(Stmt::Query(query)) => match query.as_ref() {
                        tidb_ast::QueryStmt::Select(bound) => bound,
                        tidb_ast::QueryStmt::SetOpr(_) => select,
                    },
                    _ => select,
                };
                if let Some(cached) = cached_point_get.as_ref() {
                    let current_db = self.current_db.clone();
                    let ctx = self.prepared_point_get_context();
                    let result = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_prepared_point_get(cached, catalog, &current_db, &ctx)
                    })?;
                    let Some((columns, rows)) = result else {
                        return Err(DriverError::unsupported(
                            "prepared point-get cache was invalidated during the statement",
                        ));
                    };
                    self.found_in_plan_cache = true;
                    return Ok(StmtOutput::Rows { columns, rows });
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
                DmlStmt::Insert(insert) => {
                    let current_db = self.current_db.clone();
                    let enable_strict_not_null_check = !matches!(
                        self.vars
                            .get_system(tidb_vardef::tidb_vars::TIDB_ENABLE_STRICT_NOT_NULL_CHECK)
                            .as_deref(),
                        Ok("OFF" | "off" | "0")
                    );
                    // Go `ResetContextOfStmt`'s `*ast.InsertStmt` arm. The class
                    // is what `StmtContext::push_down_flags` turns into the
                    // statement-kind bit of any coprocessor request this
                    // statement's read half issues, and `IgnoreErr` is the
                    // `IGNORE` modifier Go reads off this same AST to downgrade
                    // every value-level error to a warning.
                    let ctx = self
                        .statement_context_ignoring(true, insert.ignore)
                        .with_statement_class(tidb_executor::StatementClass::Insert)
                        .with_single_insert_bad_null_policy(
                            insert.rows.len() == 1,
                            enable_strict_not_null_check,
                        );
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
                DmlStmt::Update(update) => {
                    let current_db = self.current_db.clone();
                    // Go `ResetUpdateStmtCtx`, which applies the same
                    // `!strictSQLMode || stmt.IgnoreErr` rule the INSERT arm
                    // does; the class is what `StmtContext::push_down_flags`
                    // turns into the statement-kind bit of any coprocessor
                    // request this statement's read half issues.
                    let ctx = self
                        .statement_context_ignoring(true, update.ignore)
                        .with_statement_class(tidb_executor::StatementClass::UpdateOrDelete);
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
                DmlStmt::Delete(delete) => {
                    let current_db = self.current_db.clone();
                    // Go `ResetDeleteStmtCtx`, which applies the same
                    // `!strictSQLMode || stmt.IgnoreErr` rule the INSERT arm
                    // does; the class is what `StmtContext::push_down_flags`
                    // turns into the statement-kind bit of any coprocessor
                    // request this statement's read half issues.
                    let ctx = self
                        .statement_context_ignoring(true, delete.ignore)
                        .with_statement_class(tidb_executor::StatementClass::UpdateOrDelete);
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
                other => Err(DriverError::unsupported(format!(
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
                DdlStmt::CreateIndex(create) => {
                    let if_not_exists = create.if_not_exists;
                    let current_db = self.current_db.clone();
                    // An index backfill WRITES the entries it computes, so it
                    // evaluates at the write level: captured from TiDB,
                    // `alter table t add index i((100/a))` over a row with
                    // `a = 0` is 1365 under the default SQL mode and succeeds
                    // under `sql_mode = ''`, exactly as the INSERT of such a
                    // row does.
                    let ctx = self.statement_context(true);
                    let result = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_create_index_in(sql, catalog, &current_db, &ctx)?;
                        Ok(StmtOutput::Affected(0))
                    });
                    // A DDL action raises notes of its own -- an `IF EXISTS`
                    // that skipped something files the error it swallowed as
                    // Go's `Note`, from inside the executor. Draining after
                    // the call rather than after a successful one is what Go
                    // does too: the buffer belongs to the statement, not to
                    // its outcome.
                    self.drain_eval_warnings(&ctx);
                    // Go `executor.go`'s `CreateIndex`:
                    //
                    //     if dbterror.ErrDupKeyName.Equal(err) && ifNotExists {
                    //         ctx.GetSessionVars().StmtCtx.AppendNote(err)
                    //         return nil
                    //     }
                    //
                    // The note carries the SUPPRESSED ERROR itself, so its
                    // code and text are 1061's and cannot drift from it --
                    // which is exactly what `append_suppressed` files. Only
                    // 1061 is swallowed: `IF NOT EXISTS` says the index may
                    // already be there, not that any failure is acceptable.
                    match result {
                        Err(DriverError::DuplicateKeyName(name)) if if_not_exists => {
                            self.append_suppressed(DriverError::DuplicateKeyName(name));
                            Ok(StmtOutput::Affected(0))
                        }
                        other => other,
                    }
                }
                DdlStmt::DropIndex(_) => {
                    let current_db = self.current_db.clone();
                    // `DROP INDEX` REBUILDS each entry's key to delete it, so
                    // it needs the session's `@@time_zone` for the same reason
                    // writing the entry did.
                    let ctx = self.statement_context(true);
                    let result = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_index_in(sql, catalog, &current_db, &ctx)?;
                        Ok(StmtOutput::Affected(0))
                    });
                    // A DDL action raises notes of its own -- an `IF EXISTS`
                    // that skipped something files the error it swallowed as
                    // Go's `Note`, from inside the executor. Draining after
                    // the call rather than after a successful one is what Go
                    // does too: the buffer belongs to the statement, not to
                    // its outcome.
                    self.drain_eval_warnings(&ctx);
                    result
                }
                DdlStmt::AlterTable(alter) => {
                    // `CHECK` constraints reach ALTER TABLE through the same
                    // `tidb_enable_check_constraint` model CREATE TABLE uses
                    // (see `tidb_executor::run_create_table_in`): with the
                    // variable ON, Go STORES and enforces an added
                    // constraint, none of which is modelled, so it is refused
                    // with the same reason rather than silently discarded.
                    // `ALTER CONSTRAINT` is NOT in that gate: Go answers 3940
                    // for it when the variable is on, which this tier can
                    // always say honestly because no table here holds one.
                    let discarded_checks = tidb_executor::discarded_check_constraint_actions(alter);
                    if self.enable_check_constraint() {
                        if tidb_executor::added_check_constraint_actions(alter) > 0 {
                            return Err(DriverError::unsupported(
                                "CHECK constraints are only modelled with \
                                 tidb_enable_check_constraint off",
                            ));
                        }
                        if let Some(name) = alter.actions.iter().find_map(|action| match action {
                            tidb_ast::AlterTableAction::AlterCheck(alter) => Some(&alter.name),
                            _ => None,
                        }) {
                            return Err(DriverError::CheckConstraintNotExists(name.clone()));
                        }
                    }
                    let current_db = self.current_db.clone();
                    // `ADD INDEX` backfills, so the same write level applies.
                    let ctx = self.statement_context(true);
                    let result = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_alter_table_in(sql, catalog, &current_db, &ctx)?;
                        Ok(StmtOutput::Affected(0))
                    });
                    // One `tidb_enable_check_constraint is off` warning per
                    // discarded action, the same rule CREATE TABLE follows.
                    if result.is_ok() {
                        for _ in 0..discarded_checks {
                            self.append_warning(
                                WarningLevel::Warning,
                                CHECK_CONSTRAINT_IS_OFF_CODE,
                                CHECK_CONSTRAINT_IS_OFF_MESSAGE.to_owned(),
                            );
                        }
                    }
                    // A DDL action raises notes of its own -- an `IF EXISTS`
                    // that skipped something files the error it swallowed as
                    // Go's `Note`, from inside the executor. Draining after
                    // the call rather than after a successful one is what Go
                    // does too: the buffer belongs to the statement, not to
                    // its outcome.
                    self.drain_eval_warnings(&ctx);
                    result
                }
                DdlStmt::DropTable(_) => {
                    let current_db = self.current_db.clone();
                    let foreign_key_checks = self.foreign_key_checks();
                    let missing = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_table_in(
                            sql,
                            catalog,
                            &current_db,
                            sql_mode,
                            foreign_key_checks,
                        )
                    })?;
                    // `IF EXISTS` does not silence the missing names, it
                    // demotes them: Go files one `Note 1051` per name it could
                    // not drop. Captured from `gorun`,
                    // `drop table if exists nosuchA, nosuchB` leaves TWO.
                    for name in missing {
                        self.append_suppressed(DriverError::Schema(SchemaErrorKind::BadTable(
                            name,
                        )));
                    }
                    // MySQL answers DDL with a zero affected-row count.
                    Ok(StmtOutput::Affected(0))
                }
                DdlStmt::CreateTable(create) => {
                    let current_db = self.current_db.clone();
                    let foreign_key_checks = self.foreign_key_checks();
                    let enable_check_constraint = self.enable_check_constraint();
                    // `@@tidb_enable_clustered_index` decides whether a
                    // declared primary key becomes the ROW HANDLE or an
                    // ordinary unique index, so it has to be read from this
                    // session before the table is built.
                    let clustered_index_mode = self.clustered_index_mode();
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
                            tidb_executor::CreateTableSettings {
                                sql_mode,
                                foreign_key_checks,
                                enable_check_constraint,
                                clustered_index_mode,
                            },
                            &ctx,
                        )?))
                    });
                    // `Done(false)` is `IF NOT EXISTS` finding the table
                    // already there. Go does not pass over that silently: it
                    // files the `ErrTableExists` it did not raise as a note --
                    // `Note | 1050 | Table 'test.tt' already exists`, captured
                    // from `gorun`.
                    if let Ok(StmtOutput::Done(false)) = &done {
                        let (database, name) = self.split_table_path(&create.name)?;
                        self.append_suppressed(DriverError::Schema(SchemaErrorKind::TableExists(
                            format!("{database}.{name}"),
                        )));
                    }
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
                    // What the BUILD itself warned about -- Go's
                    // `buildIndexColumns` files 1071 here when a non-strict
                    // mode truncates an over-long key part. It comes AFTER the
                    // discarded-CHECK notes, which is the order captured over
                    // a table that triggers both.
                    self.drain_eval_warnings(&ctx);
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
                // A placement policy is a schema object, not a table
                // attribute: Go handles it through its own executor methods
                // (`ddl/executor.go:6802` onward) rather than the table DDL
                // pipeline, and so does this.
                DdlStmt::CreatePlacementPolicy(create) => {
                    let create = (**create).clone();
                    let context = self.ddl_statement_context();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_create_placement_policy(catalog, &create, &context)
                    })?;
                    self.drain_context_warnings(&context);
                    Ok(StmtOutput::Done(false))
                }
                DdlStmt::AlterPlacementPolicy(alter) => {
                    let alter = (**alter).clone();
                    let context = self.ddl_statement_context();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_alter_placement_policy(catalog, &alter, &context)
                    })?;
                    self.drain_context_warnings(&context);
                    Ok(StmtOutput::Done(false))
                }
                DdlStmt::DropPlacementPolicy(drop) => {
                    let drop = (**drop).clone();
                    let context = self.ddl_statement_context();
                    self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_placement_policy(catalog, &drop, &context)
                    })?;
                    self.drain_context_warnings(&context);
                    Ok(StmtOutput::Done(false))
                }
                DdlStmt::DropView { if_exists, names } => {
                    let current_db = self.current_db.clone();
                    let (if_exists, names) = (*if_exists, names.clone());
                    let missing = self.with_catalog_mut(|catalog| {
                        tidb_executor::run_drop_view_in(if_exists, &names, catalog, &current_db)
                    })?;
                    // Same demotion as `DROP TABLE IF EXISTS`, same code:
                    // a view that was not there is `Note 1051`.
                    for name in missing {
                        self.append_suppressed(DriverError::Schema(SchemaErrorKind::BadTable(
                            name,
                        )));
                    }
                    Ok(StmtOutput::Affected(0))
                }
                other => Err(DriverError::unsupported(format!(
                    "this DDL statement kind ({}) is not supported yet",
                    variant_name(other)
                ))),
            },
            Stmt::Admin(admin) => Err(DriverError::unsupported(format!(
                "this statement kind (ADMIN {}) is not supported yet",
                variant_name(&**admin)
            ))),
            Stmt::Session(session) => Err(DriverError::unsupported(format!(
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
            return Err(DriverError::unsupported(
                "SELECT ... INTO OUTFILE is not supported yet",
            ));
        }
        Ok(())
    }
}

impl Session {
    /// Refuses every statement that would read or write while the session has
    /// pinned a historical timestamp.
    ///
    /// Go answers such a statement from the PAST: `tidb_snapshot` makes the
    /// session read at that timestamp (`SnapshotTS`), and a negative
    /// `tidb_read_staleness` reads `now() - staleness`
    /// (`CalculateAsOfTsExpr`). Both need MVCC history and a timestamp oracle,
    /// which this tier's store has neither of -- so the honest answer is the
    /// same refusal the bounded planners already give a stale read
    /// (`tidb-planner`'s `UnsupportedReadOnlyFeature::StaleRead`), not the
    /// CURRENT rows under a historical name. Answering from the present is
    /// the one outcome a client cannot detect.
    ///
    /// `tidb_read_staleness` is Go's `int` seconds, at most 0; `tidb_snapshot`
    /// is Go's timestamp string, empty when nothing is pinned.
    fn refuse_pinned_historical_read(&mut self) -> Result<(), DriverError> {
        if let Ok(snapshot) = self.vars.get_system(tidb_vardef::tidb_vars::TIDB_SNAPSHOT) {
            if !snapshot.is_empty() {
                return Err(DriverError::unsupported(
                    "reading at @@tidb_snapshot is not supported yet",
                ));
            }
        }
        if let Ok(staleness) = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_READ_STALENESS)
        {
            if staleness
                .trim()
                .parse::<i64>()
                .is_ok_and(|value| value != 0)
            {
                return Err(DriverError::unsupported(
                    "reading at @@tidb_read_staleness is not supported yet",
                ));
            }
        }
        Ok(())
    }
}
