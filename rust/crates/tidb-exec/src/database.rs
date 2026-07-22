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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Top-level statement coordination: constructing a [`Database`], resetting
//! per-statement session state, and routing each statement family to its
//! physical owner.

use std::time::{SystemTime, UNIX_EPOCH};

use tidb_ast::{DmlStmt, Expr, HintKind, JoinNode, QueryStmt, Stmt};

use crate::result_schema::{
    is_bounded_catalog_count_column_select, resolve_catalog_count_column_select_field,
};
use crate::session_runtime::parse_strict_bool_value;
use crate::session_settings::SqlSelectLimit;
use crate::{
    CatalogSchemaError, CatalogTableSchema, Database, ExecError, Outcome, StatementStatus,
};

/// Only the DML forms this seed actually executes have a source-backed
/// affected-row result. Parser-only DML boundaries deliberately leave the
/// prior `ROW_COUNT()` value alone rather than fabricate statement metadata.
fn dml_publishes_row_count(dml: &DmlStmt) -> bool {
    match dml {
        DmlStmt::Insert(_) => true,
        DmlStmt::Update(update) => update.order_by.is_empty() && update.limit.is_none(),
        DmlStmt::Delete(delete) => delete.order_by.is_empty() && delete.limit.is_none(),
        DmlStmt::With { .. }
        | DmlStmt::Batch(_)
        | DmlStmt::DistributeTable(_)
        | DmlStmt::ImportInto(_)
        | DmlStmt::LoadData(_) => false,
    }
}

impl Database {
    /// Creates an empty database.
    pub fn new() -> Self {
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("host clock must not precede the Unix epoch")
            .as_nanos() as i64;
        Self {
            transaction: crate::transaction::TransactionState::new(),
            tidb_retry_limit: 10,
            sql_select_limit: SqlSelectLimit::UNLIMITED,
            rng: std::rc::Rc::new(std::cell::RefCell::new(tidb_expr::MysqlRng::new_with_seed(
                now_nanos,
            ))),
            ..Self::default()
        }
    }

    /// Runs one statement, mutating catalog state or returning a result set.
    pub fn run(&mut self, stmt: &Stmt) -> Result<Outcome, ExecError> {
        self.run_with_optional_statement_status(stmt, None)
    }

    /// Runs one statement while publishing source diagnostics directly into
    /// the connection-owned statement status.
    pub(crate) fn run_with_statement_status(
        &mut self,
        stmt: &Stmt,
        status: &mut StatementStatus,
    ) -> Result<Outcome, ExecError> {
        self.run_with_optional_statement_status(stmt, Some(status))
    }

    fn run_with_optional_statement_status(
        &mut self,
        stmt: &Stmt,
        status: Option<&mut StatementStatus>,
    ) -> Result<Outcome, ExecError> {
        // TiDB caches its statement timestamp in StatementContext. Clearing
        // before dispatch makes every SessionState requested during this one
        // top-level statement share one instant, then starts fresh for the
        // next statement.
        *self.statement_clock.borrow_mut() = None;
        self.statement_rngs.borrow_mut().clear();
        // `SetExecutor`'s statement-context reset promotes a previous
        // `LAST_INSERT_ID(expr)` write BEFORE the following statement is
        // evaluated (`pkg/executor/select.go:1224-1229`). This cell stays
        // outside rollback snapshots: an expression write survives a later
        // failed statement and transaction rollback, just like TiDB's own
        // session status.
        if let Some(last_insert_id) = self.statement_last_insert_id.borrow_mut().take() {
            self.previous_last_insert_id = last_insert_id;
        }
        match stmt {
            // `ROW_COUNT()` must still see the preceding statement while a
            // SELECT is being evaluated. TiDB then publishes -1 for every
            // SELECT, including one that fails, after statement completion.
            Stmt::Query(query) => {
                let result = self.run_query(query);
                self.previous_affected_rows = -1;
                result
            }
            // Every currently executable INSERT/UPDATE/DELETE path returns
            // its source-shaped affected count. A failing executable DML
            // publishes zero; parser-only DML boundaries preserve the prior
            // value because this seed has no source-backed execution status
            // for them.
            Stmt::Dml(dml) if dml_publishes_row_count(dml) => {
                let result = self.run_dml(dml);
                self.previous_affected_rows = result.as_ref().map_or(0, |count| *count);
                result.map(|_| Outcome::Done)
            }
            Stmt::Dml(dml) => self.run_dml(dml).map(|_| Outcome::Done),
            Stmt::Admin(admin) => {
                let result = self.run_admin(admin);
                self.previous_affected_rows = 0;
                result
            }
            Stmt::Session(session) => {
                let result = self.run_session(session, status);
                self.previous_affected_rows = 0;
                result
            }
            Stmt::Ddl(ddl) => {
                let result = self.run_ddl(ddl);
                self.previous_affected_rows = 0;
                result
            }
        }
    }

    fn run_query(&mut self, query: &QueryStmt) -> Result<Outcome, ExecError> {
        // TiDB lazily creates the non-autocommit transaction only when a
        // statement needs table state.  In particular, `SELECT 1` stays
        // transaction-free while `SELECT * FROM t` opens the same rollback
        // boundary as DML; see TestTxnLazyInitialize above the helper.
        if crate::transaction::query_reads_base_table(query) {
            self.transaction.ensure_implicit(&self.tables);
        }
        match query {
            QueryStmt::Select(select) => self.run_select(select),
            QueryStmt::SetOpr(setopr) if setopr.with.is_some() => {
                Err(ExecError::Unsupported("WITH before set operation"))
            }
            QueryStmt::SetOpr(setopr) => {
                let rows = self.setopr(setopr, None)?;
                Ok(Outcome::Rows(if setopr.limit.is_some() {
                    rows
                } else {
                    self.apply_sql_select_limit(rows)
                }))
            }
        }
    }

    /// Applies `sql_select_limit` precisely where TiDB's
    /// `TryAddExtraLimit` injects its synthetic limit: only at the outer
    /// SELECT/set-operation statement, only when no explicit LIMIT was
    /// written. The u64 value never crosses through a signed value or string;
    /// conversion to usize is solely the in-memory result-vector bound.
    fn apply_sql_select_limit(&self, mut rows: crate::ResultSet) -> crate::ResultSet {
        if self.sql_select_limit != SqlSelectLimit::UNLIMITED {
            rows.rows
                .truncate(usize::try_from(self.sql_select_limit.value()).unwrap_or(usize::MAX));
        }
        rows
    }

    fn select_with_session_limit(
        &self,
        select: &tidb_ast::SelectStmt,
    ) -> Result<Outcome, ExecError> {
        let rows = self.select(select, None)?;
        Ok(Outcome::Rows(if select.limit.is_some() {
            rows
        } else {
            self.apply_sql_select_limit(rows)
        }))
    }

    /// Executes the one source-backed `SET_VAR` overlay represented by this
    /// seed. TiDB applies an approved SET_VAR during statement execution and
    /// restores its prior session value before the next statement, including
    /// when the statement itself fails. Preserve every other parsed hint as
    /// the current no-effect parser surface rather than pretending it has an
    /// executor implementation.
    fn run_select(&mut self, select: &tidb_ast::SelectStmt) -> Result<Outcome, ExecError> {
        self.validate_bounded_catalog_count_column(select)?;
        let scoped_value = select
            .hints
            .iter()
            .filter_map(|hint| match &hint.kind {
                HintKind::SetVar { var_name, value }
                    if var_name.eq_ignore_ascii_case("sql_safe_updates") =>
                {
                    Some(value)
                }
                _ => None,
            })
            // TiDB retains the first SET_VAR for a duplicate variable name
            // and warns about the rest. This executor has no warning channel,
            // so preserve the source value semantics without inventing one.
            .next();

        let Some(scoped_value) = scoped_value else {
            return self.select_with_session_limit(select);
        };
        let scoped_value = parse_strict_bool_value(
            &Expr::String(scoped_value.clone()),
            "SET_VAR(sql_safe_updates) value",
        )?;
        let saved = self.sql_safe_updates;
        self.sql_safe_updates = scoped_value;
        let result = self.select_with_session_limit(select);
        self.sql_safe_updates = saved;
        result
    }

    /// Binds the one admitted table-backed aggregate against this statement
    /// attempt's catalog snapshot before the generic evaluator can see it.
    /// Keeping the check inside `run_select` preserves the outer dispatcher's
    /// clock/RNG reset, pending LAST_INSERT_ID promotion, and failed-SELECT
    /// `ROW_COUNT() = -1` behavior.
    fn validate_bounded_catalog_count_column(
        &self,
        select: &tidb_ast::SelectStmt,
    ) -> Result<(), ExecError> {
        if !is_bounded_catalog_count_column_select(select) {
            return Ok(());
        }
        let from = select.from.as_ref().expect("bounded COUNT requires FROM");
        let JoinNode::Table(table_ref) = &from.left else {
            unreachable!("bounded COUNT requires one table")
        };
        let table_key = crate::catalog::table_key(&table_ref.name);
        let table = self
            .tables
            .get(&table_key)
            .ok_or_else(|| ExecError::UnknownTable(table_key.clone()))?;
        let schema = CatalogTableSchema::from_columns("", table_key, &table.cols, &table.col_types)
            .map_err(|_| ExecError::Unsupported("bounded catalog COUNT schema"))?;
        resolve_catalog_count_column_select_field(select, &schema, tidb_datatype::Collation::Binary)
            .map(|_| ())
            .map_err(|error| match error {
                CatalogSchemaError::MissingColumn { column } => ExecError::UnknownColumn(column),
                CatalogSchemaError::UnknownQualifier { qualifier } => {
                    ExecError::UnknownColumn(qualifier)
                }
                _ => ExecError::Unsupported("bounded catalog COUNT binding"),
            })
    }

    fn run_dml(&mut self, dml: &DmlStmt) -> Result<i64, ExecError> {
        // Non-transactional DML is a client/server orchestration protocol:
        // choosing a shard key, repeatedly rewriting and executing DML, and
        // DRY RUN result production. Do not run its ordinary inner DML or
        // open an implicit transaction while that protocol is absent.
        if matches!(dml, DmlStmt::Batch(_)) {
            return Err(ExecError::Unsupported("BATCH DML"));
        }
        // IMPORT INTO's parser surface is intentionally complete even though
        // external-file/query import needs TiDB's distributed import-job
        // protocol. Reject before opening an implicit transaction or touching
        // any session-visible state.
        if matches!(dml, DmlStmt::ImportInto(_)) {
            return Err(ExecError::Unsupported("IMPORT INTO"));
        }
        // LOAD DATA needs client/server/external-file transport, row decoding,
        // duplicate-key behavior, and TiDB's distributed import pipeline.
        // Keep its parser surface truthful, but reject before opening an
        // implicit transaction or touching catalog data.
        if matches!(dml, DmlStmt::LoadData(_)) {
            return Err(ExecError::Unsupported("LOAD DATA"));
        }
        if matches!(dml, DmlStmt::DistributeTable(_)) {
            return Err(ExecError::Unsupported("DISTRIBUTE TABLE"));
        }
        if matches!(dml, DmlStmt::Insert(insert) if !insert.returning.is_empty())
            || matches!(dml, DmlStmt::Update(update) if !update.returning.is_empty())
            || matches!(dml, DmlStmt::Delete(delete) if !delete.returning.is_empty())
        {
            return Err(ExecError::Unsupported("DML RETURNING"));
        }
        // UPDATE/DELETE ordering and row limiting are observable mutation
        // semantics. This executor currently scans/mutates every matching
        // row in storage order, so it must reject these parsed tails before
        // opening a transaction rather than silently applying a different
        // set of writes.
        match dml {
            DmlStmt::With { .. } => return Err(ExecError::Unsupported("WITH DML")),
            DmlStmt::Update(update) if !update.order_by.is_empty() || update.limit.is_some() => {
                return Err(ExecError::Unsupported("UPDATE ORDER BY/LIMIT"));
            }
            DmlStmt::Delete(delete) if !delete.order_by.is_empty() || delete.limit.is_some() => {
                return Err(ExecError::Unsupported("DELETE ORDER BY/LIMIT"));
            }
            _ => {}
        }
        self.transaction.ensure_implicit(&self.tables);
        // TiDB treats every executable DML statement atomically. The seed's
        // catalog is mutated directly (rather than through TiKV's mem-buffer),
        // so retain a per-statement catalog image around the lower-level
        // executor. This intentionally excludes session state and the
        // auto-increment allocator: source semantics consume auto IDs and
        // retain statement status even when the data mutation later fails.
        let statement_tables = self.tables.clone();
        let result = match dml {
            DmlStmt::With { .. } => unreachable!("WITH DML rejected before transaction mutation"),
            DmlStmt::Insert(insert) => self.insert(insert),
            DmlStmt::Update(update) => self.update(update),
            DmlStmt::Delete(delete) => self.delete(delete),
            DmlStmt::Batch(_) => unreachable!("BATCH rejected before transaction mutation"),
            DmlStmt::ImportInto(_) => {
                unreachable!("IMPORT INTO rejected before transaction mutation")
            }
            DmlStmt::LoadData(_) => unreachable!("LOAD DATA rejected before transaction mutation"),
            DmlStmt::DistributeTable(_) => {
                unreachable!("DISTRIBUTE TABLE rejected before transaction mutation")
            }
        };
        if result.is_err() {
            self.tables = statement_tables;
        }
        result
    }
}
