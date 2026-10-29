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

//! Compatibility name scopes for DML assignments and scalar rewriting.
//! Logical source construction belongs to `tidb-planner::plan_builder`;
//! physical row execution belongs to `driver::physical_builder`.

use tidb_datatype::{Datum, FieldType};
use tidb_driver_error::DriverError;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::ColumnResolver;
use tidb_infoschema::{split_table_path, TableEntry};
use tidb_hack::GoCaseCompare;
use tidb_hack::GoToLower;

/// An internal qualifier that parsed SQL can never produce. Plain EXPLAIN
/// uses it for Go's `ScalarSubQueryExpr`: a typed, non-constant planner value
/// that is visible to expression rewriting but is not part of an executor row.
pub const SCALAR_QUERY_SCOPE: &str = "\0scalar_subquery";

#[derive(Clone)]
pub struct PlanColumn {
    pub name: String,
    pub field_type: FieldType,
    pub unique_id: i64,
    /// The default plain-EXPLAIN branch evaluates scalar subqueries once.
    pub value: Option<Datum>,
}

/// The joined `FROM` scope: every table's columns concatenated left to right,
/// which is the row layout [`JoinExec`] produces.
///
/// `NATURAL`/`USING` coalescing does not change that row layout at all -- it
/// is expressed here, as the two pieces of naming a coalesced join adds on
/// top of it (see [`coalesce_common_columns`]).
#[derive(Clone)]
pub struct FromScope {
    pub tables: Vec<FromTable>,
    /// Planner-only values registered by plain EXPLAIN. Their synthetic row
    /// offsets follow the physical source width, but they deliberately do not
    /// contribute to [`FromScope::width`] or wildcard expansion because no
    /// executor ever produces them.
    pub plan_columns: Vec<PlanColumn>,
    pub constant_context: Option<tidb_sessionctx::StmtContext>,
    /// The statement's session `time_zone`, which [`ScopeResolver`] publishes
    /// to the expression rewriter as [`ColumnResolver::time_zone`] -- Go's
    /// `ctx.Location()`, reached while BUILDING an expression (the
    /// `TIMESTAMP 'lit'` fold rounds and offset-normalizes in it). It rides
    /// on the scope because the scope is the one value every rewrite over
    /// this `FROM` already receives; the statement build points set it from
    /// `StmtContext::session_zone` and every derived scope clones it along.
    pub zone: tidb_datatype::SessionTimeZone,
    pub like_default_escape: u8,
    pub no_unsigned_subtraction: bool,
    pub div_precision_increment: u32,
    /// The row offsets a `NATURAL`/`USING` join coalesced AWAY: the inner
    /// side's copy of each common column, which stays reachable through its
    /// own table's qualifier (`SELECT u2.id`) but is invisible to `*` and to
    /// an unqualified name -- Go's `FullNames[...].Redundant`. Empty for
    /// every other `FROM` shape, which is what makes this a no-op there.
    pub coalesced: Vec<usize>,
    /// The row offsets `*` expands to, in order, when that is NOT plain row
    /// order: a coalesced join puts the common columns first and a RIGHT
    /// join reports its two sides right-then-left. Empty means row order.
    pub star: Vec<usize>,
}

impl Default for FromScope {
    /// An empty scope in UTC -- the same zone a fresh session's
    /// `StmtContext` answers before any `SET time_zone`. Statement build
    /// points that HAVE a context overwrite the zone from it; only tests and
    /// scopes for statements that never fold a temporal literal rely on this
    /// default.
    fn default() -> Self {
        Self {
            tables: Vec::new(),
            plan_columns: Vec::new(),
            constant_context: None,
            coalesced: Vec::new(),
            star: Vec::new(),
            zone: tidb_datatype::SessionTimeZone::utc(),
            like_default_escape: b'\\',
            no_unsigned_subtraction: false,
            div_precision_increment: 4,
        }
    }
}

impl FromScope {
    pub fn for_statement(ctx: &tidb_sessionctx::StmtContext) -> Self {
        Self {
            constant_context: Some(ctx.clone()),
            zone: ctx.session_zone(),
            like_default_escape: ctx.like_default_escape(),
            no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
            div_precision_increment: ctx.div_precision_increment(),
            ..Self::default()
        }
    }

    pub fn width(&self) -> usize {
        self.tables.iter().map(|t| t.columns.len()).sum()
    }

    /// The column at a row offset, with the name it answers to.
    pub fn column_at(&self, offset: usize) -> Option<&(String, FieldType)> {
        self.tables
            .iter()
            .find(|t| (t.offset..t.offset + t.columns.len()).contains(&offset))
            .and_then(|t| t.columns.get(offset - t.offset))
    }

    /// Every column an unqualified `*` expands to, in display order (Go's
    /// `unfoldWildStar` over the join's own output names).
    pub fn star_columns(&self) -> Vec<(usize, String, FieldType)> {
        let offsets: Vec<usize> = if self.star.is_empty() {
            (0..self.width()).collect()
        } else {
            self.star.clone()
        };
        offsets
            .into_iter()
            .filter_map(|offset| {
                self.column_at(offset)
                    .map(|(name, ft)| (offset, name.clone(), ft.clone()))
            })
            .collect()
    }
}

/// The scope exposed by one base table.
pub fn single_table_scope(
    visible: &str,
    database: Option<String>,
    columns: Vec<(String, FieldType)>,
) -> FromScope {
    FromScope {
        tables: vec![FromTable {
            name: visible.to_owned(),
            database,
            columns,
            offset: 0,
        }],
        ..FromScope::default()
    }
}

/// Resolves a column reference against the joined `FROM` scope.
///
/// A qualified `t.a` binds to table `t`'s column; an unqualified `a` binds to
/// the one table that has such a column, and is rejected as ambiguous when
/// several do -- MySQL's `ERROR 1052 (23000): Column 'a' in field list is
/// ambiguous`, which Go raises from `expression.buildColumn`.
///
/// A column a `NATURAL`/`USING` join coalesced away ([`FromScope::coalesced`])
/// is skipped by the unqualified lookup and only by it: that is exactly what
/// makes `id` unambiguous after `u1 JOIN u2 USING (id)` while `u2.id` still
/// names the right side's own value (captured from Go, which reports the pair
/// as two distinct columns for `SELECT u1.id, u2.id`).
pub struct ScopeResolver<'a> {
    pub scope: &'a FromScope,
}

impl ScopeResolver<'_> {
    /// Go `expression.ColumnFullName(db, table, column)` -- the `OrigName` a
    /// resolved column carries and the only text the 1260 `GROUP_CONCAT`
    /// truncation message renders. The rewritten `Expression` keeps only an
    /// index and a unique id, so the name has to be read here, where the
    /// scope still knows which table the reference bound to.
    pub fn orig_name(&self, path: &[String]) -> Option<String> {
        let (index, _, _) = self.resolve(path)?;
        let table =
            self.scope.tables.iter().find(|table| {
                (table.offset..table.offset + table.columns.len()).contains(&index)
            })?;
        let column = table.columns.get(index - table.offset)?;
        let database = table.database.as_deref()?;
        Some(format!(
            "{}.{}.{}",
            database.go_to_lower(),
            table.name.go_to_lower(),
            column.0.go_to_lower()
        ))
    }
}

impl ColumnResolver for ScopeResolver<'_> {
    fn user_vars(&self) -> Option<&tidb_variable::user_vars::UserVars> {
        self.scope.constant_context.as_ref()?.user_vars()
    }

    fn param_value(&self, order: usize) -> Result<Datum, tidb_expr::EvalError> {
        tidb_expr::Columns::param_value(
            self.scope
                .constant_context
                .as_ref()
                .ok_or(tidb_expr::EvalError::Unsupported(
                    "unbound prepared parameter",
                ))?,
            order,
        )
    }

    /// The zone the scope's build point took from the statement's
    /// `StmtContext` -- see [`FromScope::zone`].
    fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
        self.scope.zone.clone()
    }

    fn date_modes(&self) -> tidb_datatype::DateModes {
        self.scope
            .constant_context
            .as_ref()
            .map(tidb_expr::Columns::date_modes)
            .unwrap_or(tidb_datatype::DateModes::TIDB_DEFAULT_SQL_MODE)
    }

    fn connection_charset_info(&self) -> (&str, &str) {
        match &self.scope.constant_context {
            Some(ctx) => ctx.connection_charset_info(),
            None => tidb_expr::collation_derive::connection_charset_info(),
        }
    }

    fn tidb_info_len(&self) -> usize {
        match &self.scope.constant_context {
            Some(ctx) => ctx.tidb_info_len(),
            None => tidb_printer::get_tidb_info().len(),
        }
    }

    fn like_default_escape(&self) -> u8 {
        self.scope.like_default_escape
    }

    fn no_unsigned_subtraction(&self) -> bool {
        self.scope.no_unsigned_subtraction
    }

    fn div_precision_increment(&self) -> u32 {
        self.scope.div_precision_increment
    }

    fn current_database(&self) -> Option<String> {
        self.scope
            .constant_context
            .as_ref()
            .and_then(tidb_expr::Columns::current_database)
    }

    fn fold_constant(&self, expression: &mut Expression, mode: tidb_expr::ConstantFoldMode) {
        // Join-rebuild scopes lose their statement context; Go's planner
        // always has one. Construct a minimal context from the scope's own
        // fields so deterministic functions over constants (DATE_ADD over
        // literals, string ops, ...) still fold at build time.
        match &self.scope.constant_context {
            Some(ctx) => tidb_expr::fold_constant_in_mode(expression, ctx, mode),
            None if mode != tidb_expr::ConstantFoldMode::Disabled => {
                tidb_expr::derive_constant_null_flag(expression);
                let minimal = tidb_sessionctx::StmtContext::for_query();
                tidb_expr::fold_constant_in_mode(expression, &minimal, mode);
                // go appends the fold-time warnings to StmtCtx.warnings; the
                // minimal context is otherwise discarded, so its warnings
                // route through the fold stash the statement driver drains
                // into the session buffer (captured: ADDDATE('abc', INTERVAL
                // 1 DAY) warns `Incorrect datetime value: 'abc'`).
                for warning in minimal.take_warnings() {
                    tidb_expr::constant_fold::record_fold_sql_warn(warning);
                }
            }
            None => {}
        }
    }

    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
        if let [scope, name] = path {
            if scope == SCALAR_QUERY_SCOPE {
                let physical_width = self.scope.width();
                return self
                    .scope
                    .plan_columns
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name == *name)
                    .map(|(offset, column)| {
                        (
                            physical_width + offset,
                            column.field_type.clone(),
                            column.unique_id,
                        )
                    });
            }
        }
        let (schema, qualifier, name) = match path {
            [name] => (None, None, name),
            [table, name] => (None, Some(table), name),
            // `db.t.a` is how a view's stored definition names its columns.
            [schema, table, name] => (Some(schema), Some(table), name),
            _ => return None,
        };
        let mut found: Option<(usize, FieldType)> = None;
        for table in &self.scope.tables {
            if let Some(q) = qualifier {
                if !q.go_lower_eq(&table.name) {
                    continue;
                }
            }
            if let Some(schema) = schema {
                // An aliased or synthetic source carries no schema, so a
                // schema-qualified reference cannot name it.
                match &table.database {
                    Some(db) if db.go_lower_eq(schema) => {}
                    _ => continue,
                }
            }
            for (i, (candidate, ft)) in table.columns.iter().enumerate() {
                if candidate.go_lower_eq(name) {
                    if qualifier.is_none() && self.scope.coalesced.contains(&(table.offset + i)) {
                        continue;
                    }
                    if found.is_some() {
                        // Ambiguous across tables: MySQL errors rather than
                        // picking one.
                        return None;
                    }
                    found = Some((table.offset + i, ft.clone()));
                }
            }
        }
        let (index, ft) = found?;
        Some((index, ft, (index + 1) as i64))
    }

    fn orig_name(&self, path: &[String]) -> Option<String> {
        Self::orig_name(self, path)
    }

    fn resolve_constant(&self, path: &[String]) -> Option<Expression> {
        let [scope, name] = path else {
            return None;
        };
        if scope != SCALAR_QUERY_SCOPE {
            return None;
        }
        let column = self
            .scope
            .plan_columns
            .iter()
            .find(|column| column.name == *name)?;
        let mut constant =
            tidb_expr::constant::Constant::new(column.value.clone()?, column.field_type.clone());
        constant.subquery_ref_id = -column.unique_id;
        Some(Expression::Constant(constant))
    }

    fn has_resolved_constants(&self) -> bool {
        self.scope
            .plan_columns
            .iter()
            .any(|column| column.value.is_some())
    }
}

/// The table a single-table `UPDATE`/`DELETE` targets.
pub fn single_table_name(
    table_ref: &tidb_ast::TableRef,
    current_db: &str,
) -> Result<(String, String), DriverError> {
    let (database, name) = split_table_path(&table_ref.name, current_db)?;
    Ok((database.to_owned(), name.to_owned()))
}

/// Go `buildDataSource`'s extra handle column, for a table that has one.
///
/// A table whose primary key IS the handle reports that column instead, and a
/// clustered common handle builds `HandleCols` from the primary index -- in
/// both cases `_tidb_rowid` names nothing, which is why TiDB answers
/// "Unknown column" for it there. Only a HEAP table gets the extra column.
pub fn extra_handle_column(entry: &TableEntry) -> Option<(String, FieldType)> {
    let TableEntry::Kv(kv) = entry else {
        return None;
    };
    if kv.pk_handle_offset().is_some() || !kv.common_handle_offsets().is_empty() {
        return None;
    }
    Some((
        tidb_model::column::EXTRA_HANDLE_NAME.to_owned(),
        FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
            .with_flags(tidb_datatype::FieldTypeFlags::NOT_NULL),
    ))
}

/// One table in a query's `FROM`: the name a qualifier must match (its alias
/// when it has one, as in Go's `TableSource`), its columns, and the offset of
/// its first column in the joined row.
#[derive(Clone, Debug)]
pub struct FromTable {
    pub name: String,
    /// The schema the table lives in, when a `db.t.column` reference may name
    /// it. `None` for a source that cannot be schema-qualified: an aliased
    /// table (MySQL's alias replaces the whole path) or a synthetic scope.
    pub database: Option<String>,
    pub columns: Vec<(String, FieldType)>,
    pub offset: usize,
}
