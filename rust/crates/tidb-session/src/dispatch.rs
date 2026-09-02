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
use crate::{
    infoschema, privilege, statement_kind_of, Session, StatementKind, StmtOutput, WarningLevel,
};
use crate::{CHECK_CONSTRAINT_IS_OFF_CODE, CHECK_CONSTRAINT_IS_OFF_MESSAGE};

fn sem_table_option(option: &tidb_ast::TableOption) -> tidb_util::sem_v2::TableOptionType {
    match option {
        tidb_ast::TableOption::Ttl { .. } => tidb_util::sem_v2::TableOptionType::Ttl,
        tidb_ast::TableOption::TtlEnable(_) => tidb_util::sem_v2::TableOptionType::TtlEnable,
        tidb_ast::TableOption::TtlJobInterval(_) => {
            tidb_util::sem_v2::TableOptionType::TtlJobInterval
        }
        _ => tidb_util::sem_v2::TableOptionType::Other,
    }
}

fn sem_stmt_kind(stmt: &Stmt) -> tidb_util::sem_v2::StmtKind {
    use tidb_util::sem_v2::{AlterTableSpec, AlterTableType, StmtKind};

    match stmt {
        Stmt::Query(query) => match query.as_ref() {
            tidb_ast::QueryStmt::Select(select) => StmtKind::Select {
                select_into: select.into_outfile.is_some(),
            },
            tidb_ast::QueryStmt::SetOpr(_) => StmtKind::Other,
        },
        Stmt::Dml(dml) => {
            let mut dml = dml.as_ref();
            while let tidb_ast::DmlStmt::With { statement, .. } = dml {
                dml = statement;
            }
            match dml {
                tidb_ast::DmlStmt::ImportInto(import) => match &import.source {
                    tidb_ast::ImportSource::File { path, .. } => StmtKind::ImportInto {
                        from_select: false,
                        path: path.clone(),
                    },
                    tidb_ast::ImportSource::Select { .. } => StmtKind::ImportInto {
                        from_select: true,
                        path: String::new(),
                    },
                },
                tidb_ast::DmlStmt::LoadData(load) => StmtKind::LoadData {
                    file_loc_client: load.local,
                    path: load.path.clone(),
                },
                _ => StmtKind::Other,
            }
        }
        Stmt::Ddl(ddl) => match ddl.as_ref() {
            tidb_ast::DdlStmt::CreateTable(create) => StmtKind::CreateTable {
                options: create.table_options.iter().map(sem_table_option).collect(),
            },
            tidb_ast::DdlStmt::AlterTable(alter) => StmtKind::AlterTable {
                specs: alter
                    .actions
                    .iter()
                    .map(|action| match action {
                        tidb_ast::AlterTableAction::RemoveTtl(_) => AlterTableSpec {
                            tp: AlterTableType::RemoveTtl,
                            options: Vec::new(),
                        },
                        tidb_ast::AlterTableAction::SetTableOptions { options } => AlterTableSpec {
                            tp: AlterTableType::Option,
                            options: options.iter().map(sem_table_option).collect(),
                        },
                        tidb_ast::AlterTableAction::SetAttributes(_) => AlterTableSpec {
                            tp: AlterTableType::Attributes,
                            options: Vec::new(),
                        },
                        tidb_ast::AlterTableAction::Partition(
                            tidb_ast::AlterPartitionAction::SetAttributes { .. },
                        ) => AlterTableSpec {
                            tp: AlterTableType::PartitionAttributes,
                            options: Vec::new(),
                        },
                        _ => AlterTableSpec {
                            tp: AlterTableType::Other,
                            options: Vec::new(),
                        },
                    })
                    .collect(),
            },
            _ => StmtKind::Other,
        },
        Stmt::Admin(_) | Stmt::Session(_) => StmtKind::Other,
    }
}

fn sem_stmt_view(stmt: &Stmt) -> tidb_util::sem_v2::StmtView {
    tidb_util::sem_v2::StmtView {
        sem_command: stmt.sem_command().to_owned(),
        kind: sem_stmt_kind(stmt),
    }
}

pub(crate) fn filter_sem_restricted_hints(stmt: &mut Stmt) -> Vec<String> {
    struct Filter {
        warnings: Vec<String>,
    }

    impl Filter {
        fn retain(&mut self, hints: &mut Vec<tidb_ast::Hint>) {
            hints.retain(|hint| {
                match tidb_util::sem_v2::is_restricted_hint(&hint.name.to_ascii_lowercase()) {
                    Ok(()) => true,
                    Err(warning) => {
                        self.warnings.push(warning);
                        false
                    }
                }
            });
        }
    }

    impl tidb_ast::Visitor for Filter {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(select) = node.downcast_mut::<tidb_ast::SelectStmt>() {
                self.retain(&mut select.hints);
            } else if let Some(insert) = node.downcast_mut::<tidb_ast::InsertStmt>() {
                self.retain(&mut insert.hints);
            } else if let Some(update) = node.downcast_mut::<tidb_ast::UpdateStmt>() {
                self.retain(&mut update.hints);
            } else if let Some(delete) = node.downcast_mut::<tidb_ast::DeleteStmt>() {
                self.retain(&mut delete.hints);
            }
            false
        }

        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }

    let mut filter = Filter {
        warnings: Vec::new(),
    };
    tidb_ast::Visitable::accept(stmt, &mut filter);
    filter.warnings
}

/// A planner-owned SELECT tree offered to the ordinary statement executor.
/// `used` records whether the schema still matched while the catalog was
/// locked; a moved schema falls through to ordinary physical planning.
struct RetainedSelectPlan<'a> {
    physical: &'a mut tidb_planner::physical::PhysicalPlan,
    schema_version: u64,
    used: &'a mut bool,
}

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

fn cached_dml_plan<'a>(
    plan: Option<&'a mut tidb_planner::physical::PhysicalPlan>,
    operator: &str,
) -> Result<Option<&'a mut tidb_planner::physical::PhysicalPlan>, DriverError> {
    let Some(plan) = plan else {
        return Ok(None);
    };
    let tidb_planner::physical::PhysicalPlan::Dml(dml) = &*plan else {
        return Err(DriverError::unsupported(
            "prepared DML execution received a non-DML physical root",
        ));
    };
    if !dml.go_operator.eq_ignore_ascii_case(operator) {
        return Err(DriverError::unsupported(format!(
            "prepared {operator} execution received a {} physical root",
            dml.go_operator
        )));
    }
    Ok(Some(plan))
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
        let local_temporary_create = matches!(
            stmt,
            Stmt::Ddl(ddl)
                if matches!(
                    ddl.as_ref(),
                    tidb_ast::DdlStmt::CreateTable(create)
                        if create.temporary == tidb_ast::CreateTableTemporary::Local
                )
        );
        if matches!(stmt, Stmt::Ddl(_)) && !local_temporary_create {
            // Go commits the open transaction before ordinary DDL
            // (`session.ExecuteStmt`, which calls `sessiontxn`'s
            // `OnStmtStart` -> `checkBeforeNewTxn` for a DDL node). LOCAL
            // `CREATE TEMPORARY TABLE` is the exception: `DDLExec.Next`
            // returns through `createSessionTemporaryTable` before
            // `NewTxnInStmt`, retaining the user's transaction. For ordinary
            // DDL, everything staged before it is already durable when it
            // starts. Captured from TiDB: after
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
            let current_db = self.current_db.clone();
            let (columns, rows) = self.run_information_schema_query(
                &tidb_ast::QueryStmt::Select(Box::new(select.clone())),
                table_names,
                &current_db,
                &ctx,
            )?;
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
        let current_db = self.current_db.clone();
        let (columns, rows) = self.run_information_schema_query(
            &tidb_ast::QueryStmt::Select(Box::new(select.clone())),
            table_names,
            &current_db,
            &ctx,
        )?;
        self.drain_eval_warnings(&ctx);
        Ok(Some(StmtOutput::Rows { columns, rows }))
    }

    fn run_information_schema_query(
        &mut self,
        query: &tidb_ast::QueryStmt,
        mut table_names: Vec<String>,
        current_db: &str,
        ctx: &tidb_executor::StmtContext,
    ) -> Result<tidb_executor::SelectMeta, DriverError> {
        table_names.sort_unstable_by_key(|name| name.to_ascii_lowercase());
        table_names.dedup_by(|left, right| left.eq_ignore_ascii_case(right));

        // Go resolves and prunes the memory-table scan before its executor
        // performs the restricted statistics reads. Plan against schema-only
        // virtual tables so the real rows are generated exactly once below.
        let planning_catalog = self.information_schema_planning_catalog(&table_names)?;
        let mut physical =
            tidb_executor::plan_query_meta_stmt(query, &planning_catalog, current_db, ctx)?;
        let needs_storage_stats =
            tidb_executor::physical_plan_needs_table_storage_statistics(&physical);
        let needs_column_lengths =
            tidb_executor::physical_plan_needs_table_storage_column_lengths(&physical);
        let scratch = self.materialize_information_schema_catalog(
            table_names,
            ctx,
            needs_storage_stats,
            needs_column_lengths,
        )?;
        tidb_executor::run_query_meta_stmt_with_physical(
            query,
            Some(&mut physical),
            &scratch,
            current_db,
            ctx,
        )
    }

    fn information_schema_planning_catalog(
        &mut self,
        table_names: &[String],
    ) -> Result<Catalog, DriverError> {
        let mut schemas = Vec::with_capacity(table_names.len());
        for table_name in table_names {
            let Some(columns) = infoschema::table_schema(table_name) else {
                return Err(DriverError::Schema(SchemaErrorKind::UnknownTable(format!(
                    "{}.{}",
                    infoschema::INFORMATION_SCHEMA,
                    table_name
                ))));
            };
            schemas.push((table_name.clone(), columns));
        }
        self.with_catalog_mut(|catalog| {
            let mut scratch = catalog.clone();
            for (table_name, columns) in schemas {
                scratch.register_mem_in(
                    infoschema::INFORMATION_SCHEMA,
                    &table_name,
                    tidb_executor::MemTable {
                        columns,
                        rows: Vec::new(),
                    },
                );
            }
            Ok(scratch)
        })
    }

    /// Clones this statement's real catalog and overlays each referenced
    /// virtual table with its computed rows.  The result is deliberately a
    /// normal [`Catalog`]: every query shape then reaches exactly the same
    /// executor and optimizer paths as stored tables do.
    fn materialize_information_schema_catalog(
        &mut self,
        mut table_names: Vec<String>,
        ctx: &tidb_executor::StmtContext,
        needs_storage_stats: bool,
        needs_column_lengths: bool,
    ) -> Result<Catalog, DriverError> {
        table_names.sort_unstable_by_key(|name| name.to_ascii_lowercase());
        table_names.dedup_by(|left, right| left.eq_ignore_ascii_case(right));
        let mut storage_statistics = None;
        let mut storage_statistics_failed = false;
        if needs_storage_stats
            && table_names.iter().any(|name| {
                name.eq_ignore_ascii_case("TABLES") || name.eq_ignore_ascii_case("PARTITIONS")
            })
        {
            if let Some(provider) = &self.table_storage_stats {
                match provider.load_table_storage_statistics(
                    &self.active_resource_group,
                    needs_column_lengths,
                ) {
                    Ok(statistics) => storage_statistics = Some(statistics),
                    // Pinned `buildTableSizeStats` logs the restricted-read
                    // error and returns nil, so all size getters report zero
                    // for this statement.
                    Err(error) => {
                        storage_statistics_failed = true;
                        eprintln!(
                        "{{\"event\":\"information_schema_stats_refresh_failed\",\"error\":{}}}",
                        serde_json::to_string(&error)
                            .unwrap_or_else(|_| "\"unprintable\"".to_owned())
                        )
                    }
                }
            }
        }
        let mut scratch = self.with_catalog_mut(|catalog| Ok(catalog.clone()))?;
        if storage_statistics_failed {
            scratch.clear_table_storage_statistics();
        } else if let Some(statistics) = storage_statistics {
            for table in statistics {
                scratch.set_table_storage_statistics(
                    table.table_id,
                    table.table,
                    &table.partitions,
                );
            }
        }
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
            } else if table_name.eq_ignore_ascii_case("TIDB_INDEX_USAGE") {
                let visibility = self.schema_visibility();
                let collector = std::sync::Arc::clone(&self.index_usage_collector);
                infoschema::tidb_index_usage_rows(&scratch, &visibility, collector.as_ref())
            } else if table_name.eq_ignore_ascii_case("TIDB_STATEMENTS_STATS") {
                self.tidb_statements_stats_table_rows(&columns)
            } else if table_name.eq_ignore_ascii_case("TIDB_TRX") {
                self.tidb_trx_table_rows()
            } else if table_name.eq_ignore_ascii_case("DATA_LOCK_WAITS") {
                self.data_lock_waits_table_rows()?
            } else if table_name.eq_ignore_ascii_case("CLIENT_ERRORS_SUMMARY_GLOBAL")
                || table_name.eq_ignore_ascii_case("CLIENT_ERRORS_SUMMARY_BY_USER")
                || table_name.eq_ignore_ascii_case("CLIENT_ERRORS_SUMMARY_BY_HOST")
            {
                self.client_errors_summary_table_rows(&table_name)?
            } else if table_name.eq_ignore_ascii_case("MEMORY_USAGE") {
                memory_usage_table_rows()
            } else if table_name.eq_ignore_ascii_case("MEMORY_USAGE_OPS_HISTORY") {
                tidb_util::servermemorylimit::GLOBAL_MEMORY_OPS_HISTORY_MANAGER.get_rows()
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
                infoschema::table_rows(&table_name, &scratch, &visibility, ctx).unwrap_or_default()
            };
            materialized.push((table_name, columns, rows));
        }
        for (table_name, columns, rows) in materialized {
            scratch.register_mem_in(
                infoschema::INFORMATION_SCHEMA,
                &table_name,
                tidb_executor::MemTable { columns, rows },
            );
        }
        Ok(scratch)
    }

    /// Go `stmtSummaryRetriever.initSummaryRowsReader` for the cumulative
    /// `TIDB_STATEMENTS_STATS` table used by the workload repository.
    fn tidb_statements_stats_table_rows(
        &self,
        columns: &[(String, tidb_datatype::FieldType)],
    ) -> Vec<Vec<tidb_datatype::Datum>> {
        use tidb_ast::CiString;
        use tidb_model::ColumnInfo;
        use tidb_parser::auth::UserIdentity;
        use tidb_stmtsummary::reader::StmtSummaryReader;

        let columns = columns
            .iter()
            .enumerate()
            .map(|(offset, (name, _))| ColumnInfo {
                id: i64::try_from(offset).expect("information-schema column count fits i64"),
                name: CiString::new(name),
                offset: i64::try_from(offset).expect("information-schema column count fits i64"),
                ..ColumnInfo::default()
            })
            .collect();
        let user = self.login_user.as_deref().map(|login| {
            let (username, hostname) = login.split_once('@').unwrap_or((login, ""));
            let (auth_username, auth_hostname) = self.current_identity().unwrap_or(("", ""));
            UserIdentity {
                username: username.to_owned(),
                hostname: hostname.to_owned(),
                current_user: false,
                auth_username: auth_username.to_owned(),
                auth_hostname: auth_hostname.to_owned(),
                auth_plugin: String::new(),
            }
        });
        StmtSummaryReader::new(
            user,
            self.has_process_privilege(),
            columns,
            String::new(),
            self.session_time_zone(),
        )
        .get_stmt_summary_cumulative_rows()
    }

    /// Pinned Go `tidbTrxTableRetriever.retrieve` for this node.
    fn tidb_trx_table_rows(&self) -> Vec<Vec<tidb_datatype::Datum>> {
        use chrono::{DateTime, Local};
        use tidb_datatype::{core_time_from_datetime, Collation, Datum, MysqlEnum, Time, TimeType};

        let Some(process) = self.process.as_ref() else {
            return Vec::new();
        };
        let login_username = self
            .login_user
            .as_deref()
            .and_then(|identity| identity.split_once('@').map(|(user, _)| user));
        let has_process = self.has_process_privilege();
        process
            .registry()
            .transaction_snapshot()
            .into_iter()
            .filter(|transaction| {
                has_process
                    || login_username.is_none_or(|username| username == transaction.user.as_str())
            })
            .map(|transaction| {
                let current_sql_digest_text =
                    transaction
                        .current_sql_digest
                        .as_deref()
                        .and_then(|digest| {
                            tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP
                                .normalized_sql_for_digest(digest)
                        });
                let start = DateTime::from_timestamp_millis((transaction.start_ts >> 18) as i64)
                    .map(DateTime::<Local>::from)
                    .map(|value| {
                        Datum::new_time(
                            Time::new(core_time_from_datetime(value), TimeType::Timestamp, 6)
                                .expect("fsp 6 is valid"),
                        )
                    })
                    .unwrap_or(Datum::Null);
                let waiting_start = transaction
                    .waiting_start
                    .map(DateTime::<Local>::from)
                    .map(|value| {
                        Datum::new_time(
                            Time::new(core_time_from_datetime(value), TimeType::Timestamp, 6)
                                .expect("fsp 6 is valid"),
                        )
                    })
                    .unwrap_or(Datum::Null);
                let waiting_time = transaction
                    .waiting_start
                    .map(|started| {
                        Datum::Real(
                            chrono::Utc::now()
                                .signed_duration_since(started)
                                .num_microseconds()
                                .unwrap_or(0) as f64
                                / 1_000_000.0,
                        )
                    })
                    .unwrap_or(Datum::Null);
                let state_index = match transaction.state {
                    "Idle" => 1,
                    "Running" => 2,
                    "LockWaiting" => 3,
                    "Committing" => 4,
                    "RollingBack" => 5,
                    _ => 1,
                };
                vec![
                    Datum::UInt(transaction.start_ts),
                    start,
                    transaction
                        .current_sql_digest
                        .clone()
                        .map(Datum::new_string)
                        .unwrap_or(Datum::Null),
                    current_sql_digest_text
                        .map(Datum::new_string)
                        .unwrap_or(Datum::Null),
                    Datum::new_enum(
                        MysqlEnum::new(transaction.state, state_index),
                        Collation::Utf8Mb4Bin,
                    ),
                    waiting_start,
                    Datum::UInt(transaction.mem_buffer_keys),
                    Datum::Int(transaction.mem_buffer_bytes),
                    Datum::UInt(transaction.session_id),
                    Datum::new_string(transaction.user),
                    Datum::new_string(transaction.db),
                    Datum::new_string(
                        serde_json::to_string(&transaction.all_sql_digests)
                            .expect("SQL digest strings serialize"),
                    ),
                    Datum::new_string(
                        transaction
                            .related_table_ids
                            .iter()
                            .map(i64::to_string)
                            .collect::<Vec<_>>()
                            .join(","),
                    ),
                    waiting_time,
                ]
            })
            .collect()
    }

    /// Pinned Go `dataLockWaitsTableRetriever.retrieve` for pessimistic waits.
    fn data_lock_waits_table_rows(
        &mut self,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        use std::fmt::Write as _;
        use tidb_datatype::Datum;
        use tidb_stmtsummary::statement_summary::STMT_SUMMARY_BY_DIGEST_MAP;

        if !self.has_process_privilege() {
            return Err(DriverError::SpecificAccessDenied("PROCESS".to_owned()));
        }
        let Some(provider) = self.data_lock_waits.as_ref().map(std::sync::Arc::clone) else {
            return Ok(Vec::new());
        };
        let waits = provider.lock_waits().map_err(DriverError::unsupported)?;
        let key_info = self.with_catalog_mut(|catalog| {
            Ok(waits
                .iter()
                .map(|wait| {
                    tidb_executor::keydecoder::decode_key(&wait.key, catalog)
                        .ok()
                        .and_then(|decoded| serde_json::to_vec(&decoded).ok())
                        .map(Datum::Bytes)
                        .unwrap_or(Datum::Null)
                })
                .collect::<Vec<_>>())
        })?;

        Ok(waits
            .into_iter()
            .zip(key_info)
            .map(|(wait, key_info)| {
                let mut key_hex = String::with_capacity(wait.key.len() * 2);
                for byte in &wait.key {
                    write!(&mut key_hex, "{byte:02X}")
                        .expect("writing hexadecimal to String cannot fail");
                }
                let digest = tidb_txnkv::decode_resource_group_tag(&wait.resource_group_tag)
                    .ok()
                    .flatten()
                    .map(|bytes| {
                        let mut hex = String::with_capacity(bytes.len() * 2);
                        for byte in bytes {
                            write!(&mut hex, "{byte:02x}")
                                .expect("writing hexadecimal to String cannot fail");
                        }
                        hex
                    });
                let digest_text = digest
                    .as_deref()
                    .and_then(|digest| STMT_SUMMARY_BY_DIGEST_MAP.normalized_sql_for_digest(digest))
                    .map(Datum::new_string)
                    .unwrap_or(Datum::Null);
                vec![
                    Datum::new_string(key_hex),
                    key_info,
                    Datum::UInt(wait.txn),
                    Datum::UInt(wait.wait_for_txn),
                    digest.map(Datum::new_string).unwrap_or(Datum::Null),
                    digest_text,
                ]
            })
            .collect())
    }

    /// Pinned Go `memtableRetriever.setDataForClientErrorsSummary`.
    fn client_errors_summary_table_rows(
        &self,
        table_name: &str,
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, DriverError> {
        use chrono::{DateTime, Local};
        use tidb_datatype::{core_time_from_datetime, CoreTime, Datum, Time, TimeType};
        use tidb_error::tidb::infoschema::{self, ErrorStats};

        fn text(value: &str) -> Datum {
            Datum::new_string(value.as_bytes())
        }

        fn count(value: isize) -> Datum {
            Datum::Int(i64::try_from(value).expect("client error count fits i64"))
        }

        fn timestamp(value: Option<std::time::SystemTime>) -> Datum {
            let time = match value {
                Some(value) => {
                    let local: DateTime<Local> = value.into();
                    Time::new(core_time_from_datetime(local), TimeType::Timestamp, 0)
                }
                None => Time::new(CoreTime::from_raw(0), TimeType::Timestamp, 0),
            }
            .expect("fsp 0 is valid for client-error timestamps");
            Datum::new_time(time)
        }

        fn message(code: u16) -> &'static str {
            tidb_error::mysql::message_by_code(code)
                .or_else(|| tidb_error::tidb::message_by_code(code))
                .map_or("", |message| message.raw)
        }

        fn summary_cells(code: u16, summary: &infoschema::ErrorSummary) -> Vec<Datum> {
            vec![
                Datum::Int(i64::from(code)),
                text(message(code)),
                count(summary.error_count),
                count(summary.warning_count),
                timestamp(Some(summary.first_seen)),
                timestamp(summary.last_seen),
            ]
        }

        fn append_scoped(rows: &mut Vec<Vec<Datum>>, scope: &str, stats: ErrorStats) {
            for (code, summary) in stats {
                let mut row = Vec::with_capacity(7);
                row.push(text(scope));
                row.extend(summary_cells(code, &summary));
                rows.push(row);
            }
        }

        let has_process = self.has_process_privilege();
        if !has_process
            && (table_name.eq_ignore_ascii_case("CLIENT_ERRORS_SUMMARY_GLOBAL")
                || table_name.eq_ignore_ascii_case("CLIENT_ERRORS_SUMMARY_BY_HOST"))
        {
            return Err(DriverError::SpecificAccessDenied("PROCESS".to_owned()));
        }

        let mut rows = Vec::new();
        if table_name.eq_ignore_ascii_case("CLIENT_ERRORS_SUMMARY_GLOBAL") {
            for (code, summary) in infoschema::global_stats() {
                rows.push(summary_cells(code, &summary));
            }
        } else if table_name.eq_ignore_ascii_case("CLIENT_ERRORS_SUMMARY_BY_HOST") {
            for (host, stats) in infoschema::host_stats() {
                append_scoped(&mut rows, &host, stats);
            }
        } else {
            let login_username = self
                .login_user
                .as_deref()
                .and_then(|identity| identity.split_once('@').map(|(user, _)| user));
            for (user, stats) in infoschema::user_stats() {
                if !has_process && login_username.is_some_and(|login| login != user) {
                    continue;
                }
                append_scoped(&mut rows, &user, stats);
            }
        }
        Ok(rows)
    }

    pub(crate) fn execute_statement(&mut self, sql: &str) -> Result<StmtOutput, DriverError> {
        let stmt = self.parse_at_statement_boundary(sql)?;
        self.execute_parsed_statement(sql, stmt, false)
    }

    /// Executes a statement tree already parsed and bound by the prepared
    /// protocol path.  The surrounding session lifecycle is still applied by
    /// `run_with_columns_using`; this seam only avoids reparsing the SQL text.
    pub(crate) fn execute_statement_parsed(
        &mut self,
        stmt: Stmt,
        sql: &str,
    ) -> Result<StmtOutput, DriverError> {
        self.execute_prepared_ast(sql, stmt)
    }

    pub(crate) fn execute_prepared_ast(
        &mut self,
        sql: &str,
        stmt: Stmt,
    ) -> Result<StmtOutput, DriverError> {
        self.begin_prepared_statement_boundary(&stmt);
        self.execute_parsed_statement(sql, stmt, true)
    }

    /// Executes the subset Go serves through a prepared `PointGetPlan`. The
    /// plan has already passed the statement-shape, schema, stale-read,
    /// binding, and hint gates, and each call creates fresh mutable execution
    /// state. The execution carries the complete binding-aware cache-key hit
    /// result rather than receiving a protocol-local readiness flag.
    pub fn execute_prepared_point_get(
        &mut self,
        execution: tidb_executor::PreparedPointGetExecution,
    ) -> Result<Option<StmtOutput>, DriverError> {
        let cache_hit = execution.cache_hit();
        self.active_resource_group.clone_from(&self.resource_group);
        self.begin_cached_prepared_query_boundary();
        let plan = execution.plan();
        self.require_named_table_privilege(
            plan.names().0,
            plan.names().1,
            privilege::GlobalPriv::Select,
        )?;
        // `dirty_content` only gates scan/access-path planning. This cached
        // executor owns one handle read and the admission gate already refuses
        // an open transaction, so walking every catalog table cannot affect
        // its result.
        self.statement_insert_id = 0;
        self.statement_kind = StatementKind::Select;

        if self.in_transaction() {
            let names = [(plan.names().0.to_owned(), plan.names().1.to_owned())];
            self.record_mdl_related_table_names(&names);
        }
        let current_db = self.current_db.clone();
        let ctx = self.prepared_point_get_context();
        let stmt_ctx = self.statement_context(false);
        let result = self.with_catalog_mut(|catalog| {
            tidb_executor::run_prepared_point_get(&execution, catalog, &current_db, &ctx, &stmt_ctx)
        })?;
        let Some((columns, rows)) = result else {
            return Ok(None);
        };
        self.found_in_plan_cache = cache_hit;
        Ok(Some(StmtOutput::Rows { columns, rows }))
    }

    /// Executes a prepared SELECT through the ordinary statement and
    /// executor funnel, offering the retained physical tree at the same seam
    /// where a fresh plan is handed to the executor builder.
    pub fn execute_prepared_select(
        &mut self,
        execution: &tidb_executor::PreparedSelectExecution,
        sql: &str,
    ) -> Result<StmtOutput, DriverError> {
        let mut used = false;
        let output = execution
            .with_plan(|statement, physical| {
                self.run_with_columns_using(sql, false, |session| {
                    session.begin_prepared_statement_boundary(statement);
                    session.execute_parsed_statement_with_select_plan(
                        sql,
                        statement.clone(),
                        true,
                        physical,
                        execution.schema_version(),
                        &mut used,
                    )
                })
                .map(|(output, _)| output)
            })
            .ok_or_else(|| {
                DriverError::unsupported(
                    "prepared SELECT plan generation changed before executor construction",
                )
            })??;
        self.found_in_plan_cache = execution.cache_hit() && used;
        Ok(output)
    }

    /// Executes a bound prepared DML statement through the ordinary statement
    /// funnel, then publishes whether Go's complete cache key was reused.
    /// Cache hits and misses therefore share privilege checks, metadata locks,
    /// resource-group selection, statement context, and the DML executor.
    pub fn execute_cached_prepared_dml(
        &mut self,
        execution: &tidb_executor::PreparedDmlExecution,
        sql: &str,
    ) -> Result<StmtOutput, DriverError> {
        let output = execution
            .with_plan(|statement, physical| {
                self.run_with_columns_using(sql, false, |session| {
                    session.begin_prepared_statement_boundary(statement);
                    session.execute_parsed_statement_with_dml_plan(
                        sql,
                        statement.clone(),
                        true,
                        physical,
                    )
                })
                .map(|(output, _)| output)
            })
            .ok_or_else(|| {
                DriverError::unsupported(
                    "cached DML plan generation changed before executor construction",
                )
            })??;
        self.found_in_plan_cache = execution.cache_hit();
        Ok(output)
    }

    /// Records one cached point read's table on the transaction's metadata-
    /// lock map when it runs INSIDE an explicit transaction. The ordinary
    /// funnel does this from its single preprocess walk; the cached arm skips
    /// that walk (which is its whole point), and a read under autocommit
    /// records nothing anyway (`IsAutoCommitTxn && IsReadOnly`). Skipping the
    /// RECORD here would let a DDL slip past a live transaction's reader.

    /// Go `preprocess.go:2243-2270`: as a statement's tables are bound, each
    /// is recorded on the transaction as `table id -> the LATEST domain
    /// schema version` at first use, and the metadata-lock gate blocks a DDL
    /// job while some live map holds one of its tables below the job's
    /// version (`RemoveLockDDLJobs`). Go records at planner resolution; this
    /// port records at the same statement funnel from the parsed names,
    /// which is why a name that resolves to no stored table (a view -- Go
    /// would record its BASE tables) reports `record_unresolved` and the
    /// gate falls back to blocking conservatively.
    ///
    /// Go's exemption is kept verbatim: a READ-ONLY statement under
    /// autocommit records nothing (`IsAutoCommitTxn && IsReadOnly` returns
    /// before the store), so the hot point-read path pays nothing here
    /// either.
    fn record_mdl_related_tables(&mut self, stmt: &Stmt, names: &[(String, String)]) {
        let Some(sink) = self.mdl_related_tables.clone() else {
            return;
        };
        // `SELECT ... FOR UPDATE` is not read-only in Go, but this tier does
        // not take its row locks yet either (a named gap); classifying every
        // Query as read-only here is exact for the statements this node
        // actually serves, and errs toward recording MORE once it isn't.
        let read_only = matches!(stmt, Stmt::Query(_));
        if read_only && !self.in_transaction() && self.is_autocommit() {
            return;
        }
        let version = self.cluster_schema_version_now();
        let current_db = self.current_database().to_owned();
        let catalog = match self.lock_catalog() {
            Ok(catalog) => catalog,
            Err(_) => {
                sink.record_unresolved();
                return;
            }
        };
        for (db, table) in names {
            let db = if db.is_empty() { &current_db } else { db };
            match catalog.stored_table_id(db, table) {
                Some(table_id) => {
                    sink.record_table(table_id, version);
                    if let Some(process) = &self.process {
                        process
                            .registry()
                            .transaction_related_table(process.id(), table_id);
                    }
                }
                None => sink.record_unresolved(),
            }
        }
    }

    /// [`Self::record_mdl_related_tables`] for a statement whose single table
    /// name the cached plan already carries. A cached point read is a
    /// SELECT: under autocommit it records nothing (the read-only rule
    /// above), and inside an explicit transaction it records exactly that
    /// one table.
    fn record_mdl_related_table_names(&mut self, names: &[(String, String)]) {
        if !self.in_transaction() {
            return;
        }
        let Some(sink) = self.mdl_related_tables.clone() else {
            return;
        };
        let version = self.cluster_schema_version_now();
        let current_db = self.current_database().to_owned();
        let catalog = match self.lock_catalog() {
            Ok(catalog) => catalog,
            Err(_) => {
                sink.record_unresolved();
                return;
            }
        };
        for (db, table) in names {
            let db = if db.is_empty() { &current_db } else { db };
            match catalog.stored_table_id(db, table) {
                Some(table_id) => {
                    sink.record_table(table_id, version);
                    if let Some(process) = &self.process {
                        process
                            .registry()
                            .transaction_related_table(process.id(), table_id);
                    }
                }
                None => sink.record_unresolved(),
            }
        }
    }

    /// Runs a statement whose table refs carry `AS OF TIMESTAMP`, against
    /// the store's state as of that timestamp. `Ok(None)` means the
    /// statement carries no as-of clause and takes its ordinary path.
    ///
    /// Go's rules, from `preprocess.go` and `staleread/util.go`: every
    /// as-of expression in one statement must name the same instant (Go
    /// errors on a mix; this port refuses it the same way), the resolved
    /// snapshot serves every read, and the statement's transaction is the
    /// stale one -- `@@tidb_current_ts` is the as-of timestamp for its
    /// duration, and `LastTxnInfo` records the start-only shape a read-only
    /// transaction leaves (`setLastTxnInfoBeforeTxnEnd`).
    fn execute_as_of_statement(&mut self, stmt: &Stmt) -> Result<Option<StmtOutput>, DriverError> {
        struct StripAsOf {
            taken: Vec<tidb_ast::Expr>,
        }
        impl tidb_ast::Visitor for StripAsOf {
            fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
                if let Some(table_ref) = node.downcast_mut::<tidb_ast::TableRef>() {
                    if let Some(expr) = table_ref.as_of.take() {
                        self.taken.push(*expr);
                    }
                }
                false
            }
            fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
                true
            }
        }
        let mut stripped = stmt.clone();
        let mut visitor = StripAsOf { taken: Vec::new() };
        tidb_ast::Visitable::accept(&mut stripped, &mut visitor);
        if visitor.taken.is_empty() {
            return Ok(None);
        }
        let mut resolved: Option<u64> = None;
        for expr in &visitor.taken {
            let ts = self.resolve_as_of_ts(expr)?;
            match resolved {
                None => resolved = Some(ts),
                Some(previous) if previous == ts => {}
                Some(_) => {
                    // Go refuses a statement naming two different instants.
                    return Err(DriverError::Txn(crate::TxnErrorKind::AsOf(
                        "can not set different time in the as of".to_owned(),
                    )));
                }
            }
        }
        let ts = resolved.expect("at least one as-of expression");
        let output = self.run_statement_as_of(ts, stripped);
        Some(output).transpose()
    }

    /// Executes one already-stripped statement against the snapshot at `ts`,
    /// through the same transaction overlay every in-transaction statement
    /// uses -- so the read path is the ordinary one, only the catalog is
    /// historical.
    fn run_statement_as_of(&mut self, ts: u64, stripped: Stmt) -> Result<StmtOutput, DriverError> {
        self.open_stale_transaction(ts)?;
        let outcome = self.execute_parsed_statement_no_as_of(stripped);
        // The stale statement's transaction ends with the statement: Go's
        // read-only end leaves the start-only `LastTxnInfo` record and the
        // published timestamp goes with it.
        self.discard_stale_statement_transaction();
        outcome
    }

    /// [`Self::execute_parsed_statement`] minus the as-of interception, for
    /// the stale execution itself (its statement is already stripped and its
    /// transaction already open).
    fn execute_parsed_statement_no_as_of(&mut self, stmt: Stmt) -> Result<StmtOutput, DriverError> {
        self.execute_parsed_statement_inner("", stmt, false, None, None)
    }

    fn execute_parsed_statement(
        &mut self,
        sql: &str,
        stmt: Stmt,
        prepared: bool,
    ) -> Result<StmtOutput, DriverError> {
        self.execute_parsed_statement_with_optional_physical_plan(sql, stmt, prepared, None, None)
    }

    fn execute_parsed_statement_with_select_plan(
        &mut self,
        sql: &str,
        stmt: Stmt,
        prepared: bool,
        physical: &mut tidb_planner::physical::PhysicalPlan,
        schema_version: u64,
        used: &mut bool,
    ) -> Result<StmtOutput, DriverError> {
        self.execute_parsed_statement_with_optional_physical_plan(
            sql,
            stmt,
            prepared,
            Some(RetainedSelectPlan {
                physical,
                schema_version,
                used,
            }),
            None,
        )
    }

    fn execute_parsed_statement_with_dml_plan(
        &mut self,
        sql: &str,
        stmt: Stmt,
        prepared: bool,
        physical: &mut tidb_planner::physical::PhysicalPlan,
    ) -> Result<StmtOutput, DriverError> {
        self.execute_parsed_statement_with_optional_physical_plan(
            sql,
            stmt,
            prepared,
            None,
            Some(physical),
        )
    }

    fn execute_parsed_statement_with_optional_physical_plan(
        &mut self,
        sql: &str,
        mut stmt: Stmt,
        prepared: bool,
        select_plan: Option<RetainedSelectPlan<'_>>,
        dml_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
    ) -> Result<StmtOutput, DriverError> {
        if tidb_util::sem_v2::is_enabled()
            && tidb_util::sem_v2::is_restricted_sql(&sem_stmt_view(&stmt))
            && !self.has_dynamic_privilege("RESTRICTED_SQL_ADMIN", false)
        {
            let statement = if stmt.text().is_empty() {
                sql.to_owned()
            } else {
                String::from_utf8_lossy(stmt.text()).into_owned()
            };
            return Err(DriverError::NotSupportedWithSem(statement));
        }
        for warning in filter_sem_restricted_hints(&mut stmt) {
            self.append_warning(WarningLevel::Warning, 1105, warning);
        }
        // Go's `Preprocess` walks the AST once per statement and answers
        // every table-shaped question from that pass (`preprocess.go`); the
        // three consumers below share this one walk instead of each cloning
        // and re-walking the statement.
        let scan = crate::binding::scan_statement_tables(&mut stmt);
        self.record_mdl_related_tables(&stmt, &scan.names);
        // A statement whose table references carry `AS OF TIMESTAMP` runs
        // against the store's history -- Go's stale statement
        // (`StalenessTxnContextProvider` for one statement). Intercepted at
        // this one funnel so text and prepared spellings share the rules.
        if scan.has_as_of && !self.in_transaction() {
            if let Some(output) = self.execute_as_of_statement(&stmt)? {
                return Ok(output);
            }
        }
        // For the autocommit `LastTxnInfo` decision below: a table-reading
        // SELECT activates a transaction in Go (start-only record); one that
        // reads no stored table never takes a timestamp and leaves the
        // record alone (`setLastTxnInfoBeforeTxnEnd`'s `StartTS == 0` skip).
        let query_reads_stored_table =
            matches!(stmt, Stmt::Query(_)) && !self.in_transaction() && {
                let current_db = self.current_database().to_owned();
                match self.lock_catalog() {
                    Ok(catalog) => scan.names.iter().any(|(db, table)| {
                        let db = if db.is_empty() { &current_db } else { db };
                        catalog.stored_table_id(db, table).is_some()
                    }),
                    Err(_) => false,
                }
            };
        let was_autocommit_statement = !self.in_transaction();
        let output =
            self.execute_parsed_statement_inner(sql, stmt, prepared, select_plan, dml_plan)?;
        // Go's autocommit statement is its own transaction; its end writes
        // `LastTxnInfo` exactly as an explicit one's would -- the full
        // commit record for a statement that published, the start-only one
        // for a read that activated, and nothing for `SELECT 1`.
        if was_autocommit_statement && !self.in_transaction() {
            match &output {
                StmtOutput::Affected(_) | StmtOutput::Done(_) => {
                    let (start_ts, commit_ts) = {
                        let shared = self.lock_catalog()?;
                        let start_ts = shared.allocate_tso();
                        let commit_ts = shared.allocate_tso();
                        shared.record_commit(commit_ts);
                        (start_ts, commit_ts)
                    };
                    self.set_last_txn_info_committed(start_ts, commit_ts);
                }
                StmtOutput::Rows { .. } if query_reads_stored_table => {
                    let start_ts = self.lock_catalog()?.allocate_tso();
                    self.set_last_txn_info_started(start_ts);
                }
                _ => {}
            }
        }
        Ok(output)
    }

    /// Runs Go's `matchAgainstToLike` before physical planning. Prepared
    /// cache misses and ordinary statements both call this method, so the
    /// cache retains the same rewritten tree the normal executor receives.
    pub(crate) fn rewrite_fts_for_planning(&self, stmt: &mut Stmt) {
        let enabled = self
            .vars
            .get_system(tidb_vardef::tidb_vars::TIDB_OPT_ENABLE_ALTERNATIVE_LOGICAL_PLANS)
            .is_ok_and(|value| value.eq_ignore_ascii_case("on") || value == "1");
        if !enabled {
            return;
        }

        let current_db = self.current_db.clone();
        // The context takes its catalog-backed snapshots, so construct it
        // before holding the catalog guard used by every resolved-type probe.
        let ctx = self.statement_context(false);
        let Ok(catalog) = self.lock_catalog() else {
            return;
        };
        let columns_are_strings = |select: &tidb_ast::SelectStmt, columns: &[Vec<String>]| {
            tidb_executor::fts_columns_are_strings(select, columns, &catalog, &current_db, &ctx)
        };
        struct FtsRewriter<'a> {
            columns_are_strings: &'a tidb_executor::fts_like_rewrite::ColumnsAreStrings<'a>,
        }
        impl tidb_ast::Visitor for FtsRewriter<'_> {
            fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
                if let Some(select) = node.downcast_mut::<tidb_ast::SelectStmt>() {
                    tidb_executor::fts_like_rewrite::rewrite_select_fts(
                        select,
                        self.columns_are_strings,
                    );
                }
                false
            }

            fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
                true
            }
        }
        use tidb_ast::Visitable as _;
        stmt.accept(&mut FtsRewriter {
            columns_are_strings: &columns_are_strings,
        });
    }

    fn execute_parsed_statement_inner(
        &mut self,
        sql: &str,
        mut stmt: Stmt,
        prepared: bool,
        mut select_plan: Option<RetainedSelectPlan<'_>>,
        dml_plan: Option<&mut tidb_planner::physical::PhysicalPlan>,
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
                    let output = self.execute_parsed_statement(sql, stmt, prepared)?;
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
                        let mut vars = self
                            .user_vars
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
            // The same boundary is where a GLOBAL temporary table empties.
            // Its rows live in Go's `TxnCtx.TemporaryTables`, which is built
            // fresh for each transaction and whose keys `temporaryTableKV
            // Filter` strips before commit, so nothing a previous transaction
            // wrote is ever readable again -- `ON COMMIT DELETE ROWS` is that
            // and nothing more, which is also why TiDB refuses
            // `ON COMMIT PRESERVE ROWS` outright. In autocommit every
            // statement is its own transaction, so clearing here covers both
            // shapes: the statement before this one was the transaction that
            // ended, and a `BEGIN` arriving here starts from empty.
            self.discard_global_temporary_rows();
        }
        // Go parameterizes a non-prepared statement before optimization, then
        // sends the retained marker-bearing statement through the same plan
        // cache as PREPARE. Keep the candidate beside this ordinary statement
        // funnel; privilege, binding, transaction, and context setup below
        // remain shared whether the physical plan hits or misses.
        let non_prepared = (!prepared)
            .then(|| self.parameterize_non_prepared_select(&stmt))
            .flatten();
        // `apply_schema_stmt` dispatches administrative statements early.
        // EXPLAIN is the one such wrapper whose inner query/DML can own
        // `SET_VAR`, so install that direct-AST overlay before the early
        // dispatch builds the target plan. The ordinary query/DML path below
        // applies its own hints after its early control-statement doors.
        if matches!(&stmt, Stmt::Admin(admin) if matches!(&**admin, tidb_ast::AdminStmt::Explain(_)))
        {
            self.apply_set_var_hints(&stmt)?;
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
        self.apply_set_var_hints(&stmt)?;
        // Go first applies the statement's own StmtHints, then tries the
        // matched binding and applies that binding's StmtHints before
        // optimizing the replacement tree. Prepared execution has already
        // performed this match in order to construct its binding-aware cache
        // key, so only an ordinary statement matches here.
        let binding_sql = if !prepared && matches!(stmt, Stmt::Query(_) | Stmt::Dml(_)) {
            if let Some((bound, bind_sql)) = self.bind_statement_hints_with_sql(&stmt) {
                self.apply_set_var_hints(&bound)?;
                stmt = bound;
                Some(bind_sql)
            } else {
                None
            }
        } else {
            None
        };
        // Go applies the effective statement's RESOURCE_GROUP hint only
        // after binding selection.  Activating the original tree first can
        // leak its group (and warning) when a binding supplies another hint.
        self.activate_statement_resource_group(&stmt);
        self.bind_variables(&mut stmt)?;
        self.try_add_extra_limit(&mut stmt);
        // The mode the DDL arms below re-parse under: the one in force NOW,
        // taken before execution so a statement is lexed exactly once per
        // meaning.
        let sql_mode = self.scanner_sql_mode();
        // Only an allocating INSERT sets it; every other statement reports 0.
        self.statement_insert_id = 0;
        // The Apply channel describes THIS statement's plan.
        self.planned_apply
            .store(false, std::sync::atomic::Ordering::Relaxed);
        self.rewrite_fts_for_planning(&mut stmt);
        // Go's row-id shard generator belongs to the TRANSACTION, so a
        // statement that IS its own transaction starts a fresh run. Inside an
        // explicit `BEGIN`/`COMMIT` the run continues across statements,
        // which is what makes `tidb_shard_allocate_step` count rows rather
        // than statements.
        if !self.in_transaction() {
            self.row_id_shards
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .end_run();
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
                    let tidb_ast::QueryStmt::SetOpr(_) = &**query else {
                        unreachable!("a query is a SELECT or a set operation")
                    };
                    let current_db = self.current_db.clone();
                    let ctx = self.statement_context(false);
                    let mut table_names = Vec::new();
                    information_schema_tables_in_query(query, &current_db, &mut table_names);
                    let (columns, rows) = if table_names.is_empty() {
                        self.with_catalog_mut(|catalog| {
                            let physical = select_plan.as_mut().and_then(|retained| {
                                (retained.schema_version == catalog.metadata_version()).then(|| {
                                    *retained.used = true;
                                    &mut *retained.physical
                                })
                            });
                            tidb_executor::run_query_meta_stmt_with_physical(
                                query,
                                physical,
                                catalog,
                                &current_db,
                                &ctx,
                            )
                        })?
                    } else {
                        self.run_information_schema_query(query, table_names, &current_db, &ctx)?
                    };
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
                if let Some(parameterized) = non_prepared.as_ref() {
                    let mut effective_parameterized = parameterized.statement.clone();
                    if binding_sql.is_some() {
                        let binding_hints = crate::binding::collect_hints(&stmt);
                        crate::binding::bind_hints(&mut effective_parameterized, &binding_hints);
                    }
                    if let Some(execution) = self.bind_non_prepared_select(
                        parameterized,
                        &effective_parameterized,
                        binding_sql.as_deref(),
                    ) {
                        let cache_hit = execution.cache_hit();
                        let schema_version = execution.schema_version();
                        let result = execution.with_plan(|statement, physical| {
                            let Stmt::Query(query) = statement else {
                                unreachable!("a retained SELECT owns a query statement")
                            };
                            let tidb_ast::QueryStmt::Select(select) = query.as_ref() else {
                                unreachable!("a retained SELECT owns a SELECT query")
                            };
                            self.with_catalog_mut(|catalog| {
                                (schema_version == catalog.metadata_version())
                                    .then(|| {
                                        tidb_executor::run_select_meta_stmt_with_physical(
                                            select,
                                            Some(physical),
                                            catalog,
                                            &current_db,
                                            &ctx,
                                        )
                                    })
                                    .transpose()
                            })
                        });
                        if let Some(Some((columns, rows))) = result.transpose()? {
                            self.found_in_plan_cache = cache_hit;
                            self.drain_eval_warnings(&ctx);
                            return Ok(StmtOutput::Rows { columns, rows });
                        }
                    }
                }
                let (columns, rows) = self.with_catalog_mut(|catalog| {
                    let physical = select_plan.as_mut().and_then(|retained| {
                        (retained.schema_version == catalog.metadata_version()).then(|| {
                            *retained.used = true;
                            &mut *retained.physical
                        })
                    });
                    tidb_executor::run_select_meta_stmt_with_physical(
                        select,
                        physical,
                        catalog,
                        &current_db,
                        &ctx,
                    )
                })?;
                self.drain_eval_warnings(&ctx);
                Ok(StmtOutput::Rows { columns, rows })
            }
            Stmt::Dml(dml) => match &**dml {
                DmlStmt::Insert(insert) => {
                    let physical_plan = cached_dml_plan(dml_plan, "Insert")?;
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
                    if insert.source.is_some() {
                        self.write_sli.set_invalid();
                    }
                    let result =
                        self.with_staged_catalog_for_path(&insert.table, &current_db, |catalog| {
                            // Go executes the statement the protocol BOUND
                            // (`pkg/server`'s `statement`/`executableParams`
                            // carry the values): re-parsing `sql` here would run
                            // a tree whose markers never met their execute-time
                            // values, which is a wrong answer for every binary-
                            // protocol write.
                            tidb_executor::run_insert_stmt_with_physical(
                                insert,
                                catalog,
                                &current_db,
                                &ctx,
                                physical_plan,
                            )
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
                    let physical_plan = cached_dml_plan(dml_plan, "Update")?;
                    let current_db = self.current_db.clone();
                    // Go `ResetUpdateStmtCtx`, which applies the same
                    // `!strictSQLMode || stmt.IgnoreErr` rule the INSERT arm
                    // does; the class is what `StmtContext::push_down_flags`
                    // turns into the statement-kind bit of any coprocessor
                    // request this statement's read half issues.
                    let ctx = self
                        .statement_context_for_update_read(update.ignore)
                        .with_statement_class(tidb_executor::StatementClass::UpdateOrDelete);
                    let output = match &update.kind {
                        tidb_ast::UpdateKind::Single(table_ref) => self
                            .with_staged_catalog_for_path(
                                &table_ref.name,
                                &current_db,
                                |catalog| {
                                    // Bound AST, not SQL text: the text still
                                    // carries the markers the binary protocol
                                    // already replaced. See the INSERT arm.
                                    Ok(StmtOutput::Affected(
                                        tidb_executor::run_update_stmt_with_physical(
                                            update,
                                            catalog,
                                            &current_db,
                                            &ctx,
                                            physical_plan,
                                        )?,
                                    ))
                                },
                            ),
                        tidb_ast::UpdateKind::Multi { .. } => self.with_staged_catalog(|catalog| {
                            Ok(StmtOutput::Affected(
                                tidb_executor::run_update_stmt_with_physical(
                                    update,
                                    catalog,
                                    &current_db,
                                    &ctx,
                                    physical_plan,
                                )?,
                            ))
                        }),
                    };
                    self.drain_eval_warnings(&ctx);
                    output
                }
                DmlStmt::Delete(delete) => {
                    let physical_plan = cached_dml_plan(dml_plan, "Delete")?;
                    let current_db = self.current_db.clone();
                    // Go `ResetDeleteStmtCtx`, which applies the same
                    // `!strictSQLMode || stmt.IgnoreErr` rule the INSERT arm
                    // does; the class is what `StmtContext::push_down_flags`
                    // turns into the statement-kind bit of any coprocessor
                    // request this statement's read half issues.
                    let ctx = self
                        .statement_context_for_update_read(delete.ignore)
                        .with_statement_class(tidb_executor::StatementClass::UpdateOrDelete);
                    let output = match &delete.kind {
                        tidb_ast::DeleteKind::Single(table_ref) => self
                            .with_staged_catalog_for_path(
                                &table_ref.name,
                                &current_db,
                                |catalog| {
                                    // Bound AST, not SQL text -- see the
                                    // UPDATE arm.
                                    Ok(StmtOutput::Affected(
                                        tidb_executor::run_delete_stmt_with_physical(
                                            delete,
                                            catalog,
                                            &current_db,
                                            &ctx,
                                            physical_plan,
                                        )?,
                                    ))
                                },
                            ),
                        tidb_ast::DeleteKind::Multi { .. } => self.with_staged_catalog(|catalog| {
                            Ok(StmtOutput::Affected(
                                tidb_executor::run_delete_stmt_with_physical(
                                    delete,
                                    catalog,
                                    &current_db,
                                    &ctx,
                                    physical_plan,
                                )?,
                            ))
                        }),
                    };
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
                    let discarded_checks = if self.enable_check_constraint() {
                        0
                    } else {
                        tidb_executor::discarded_check_constraint_actions(alter)
                    };
                    let current_db = self.current_db.clone();
                    // `ADD INDEX` backfills, so the same write level applies.
                    let ctx = self.statement_context(true).with_ddl_query(sql);
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

fn memory_usage_table_rows() -> Vec<Vec<tidb_datatype::Datum>> {
    use std::sync::atomic::Ordering;

    use tidb_datatype::{core_time_from_datetime, CoreTime, Datum, Time, TimeType};

    let stats = tidb_util::memory::read_mem_stats();
    let current_ops = tidb_util::servermemorylimit::IS_KILLING
        .load(Ordering::SeqCst)
        .then(|| Datum::new_string("shrink"))
        .unwrap_or(Datum::Null);
    let session_kill_last = tidb_util::servermemorylimit::SESSION_KILL_LAST
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .map(|value| {
            Datum::new_time(
                Time::new(core_time_from_datetime(value), TimeType::DateTime, 0)
                    .expect("fsp 0 is valid"),
            )
        })
        .unwrap_or(Datum::Null);
    let zero_datetime = Datum::new_time(
        Time::new(CoreTime::default(), TimeType::DateTime, 0).expect("fsp 0 is valid"),
    );
    vec![vec![
        Datum::new_int(
            tidb_util::memory::mem_total()
                .ok()
                .and_then(|value| i64::try_from(value).ok())
                .unwrap_or(0),
        ),
        Datum::new_int(
            i64::try_from(tidb_util::memory::SERVER_MEMORY_LIMIT.load(Ordering::SeqCst))
                .unwrap_or(i64::MAX),
        ),
        Datum::new_int(stats.heap_inuse),
        Datum::new_int(
            i64::try_from(tidb_util::servermemorylimit::MEMORY_MAX_USED.load(Ordering::SeqCst))
                .unwrap_or(i64::MAX),
        ),
        current_ops,
        session_kill_last,
        Datum::new_int(tidb_util::servermemorylimit::SESSION_KILL_TOTAL.load(Ordering::SeqCst)),
        zero_datetime,
        Datum::new_int(0),
        Datum::new_int(0),
        Datum::new_int(0),
    ]]
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
