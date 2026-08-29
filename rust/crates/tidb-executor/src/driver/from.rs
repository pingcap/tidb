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

//! `FROM`-clause plan construction: base tables, joins, derived tables,
//! lateral joins and views.
//!
//! Mirrors Go's `PlanBuilder.buildResultSetNode` family (`planner/core/
//! logical_plan_builder.go`): each `ResultSetNode` shape becomes an executor
//! plus the [`FromScope`] that names its output columns for the rewriter.

use super::*;
pub(crate) type MaterializedRelation = (Vec<(String, FieldType)>, Vec<Vec<Datum>>);

/// An internal qualifier that parsed SQL can never produce. Plain EXPLAIN
/// uses it for Go's `ScalarSubQueryExpr`: a typed, non-constant planner value
/// that is visible to expression rewriting but is not part of an executor row.
pub(crate) const SCALAR_QUERY_SCOPE: &str = "\0scalar_subquery";

#[derive(Clone)]
pub(crate) struct PlanColumn {
    pub(crate) name: String,
    pub(crate) field_type: FieldType,
    pub(crate) unique_id: i64,
    /// The default plain-EXPLAIN branch evaluates scalar subqueries once.
    pub(crate) value: Option<Datum>,
}

/// The joined `FROM` scope: every table's columns concatenated left to right,
/// which is the row layout [`JoinExec`] produces.
///
/// `NATURAL`/`USING` coalescing does not change that row layout at all -- it
/// is expressed here, as the two pieces of naming a coalesced join adds on
/// top of it (see [`coalesce_common_columns`]).
#[derive(Clone)]
pub(crate) struct FromScope {
    pub(crate) tables: Vec<FromTable>,
    /// Planner-only values registered by plain EXPLAIN. Their synthetic row
    /// offsets follow the physical source width, but they deliberately do not
    /// contribute to [`FromScope::width`] or wildcard expansion because no
    /// executor ever produces them.
    pub(crate) plan_columns: Vec<PlanColumn>,
    pub(crate) constant_context: Option<crate::StmtContext>,
    /// The statement's session `time_zone`, which [`ScopeResolver`] publishes
    /// to the expression rewriter as [`ColumnResolver::time_zone`] -- Go's
    /// `ctx.Location()`, reached while BUILDING an expression (the
    /// `TIMESTAMP 'lit'` fold rounds and offset-normalizes in it). It rides
    /// on the scope because the scope is the one value every rewrite over
    /// this `FROM` already receives; the statement build points set it from
    /// `StmtContext::session_zone` and every derived scope clones it along.
    pub(crate) zone: tidb_expr::SessionTimeZone,
    pub(crate) like_default_escape: u8,
    pub(crate) no_unsigned_subtraction: bool,
    pub(crate) div_precision_increment: u32,
    /// The row offsets a `NATURAL`/`USING` join coalesced AWAY: the inner
    /// side's copy of each common column, which stays reachable through its
    /// own table's qualifier (`SELECT u2.id`) but is invisible to `*` and to
    /// an unqualified name -- Go's `FullNames[...].Redundant`. Empty for
    /// every other `FROM` shape, which is what makes this a no-op there.
    pub(crate) coalesced: Vec<usize>,
    /// The row offsets `*` expands to, in order, when that is NOT plain row
    /// order: a coalesced join puts the common columns first and a RIGHT
    /// join reports its two sides right-then-left. Empty means row order.
    pub(crate) star: Vec<usize>,
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
            zone: tidb_expr::SessionTimeZone::utc(),
            like_default_escape: b'\\',
            no_unsigned_subtraction: false,
            div_precision_increment: 4,
        }
    }
}

impl FromScope {
    pub(crate) fn for_statement(ctx: &crate::StmtContext) -> Self {
        Self {
            constant_context: Some(ctx.clone()),
            zone: ctx.session_zone(),
            like_default_escape: ctx.like_default_escape(),
            no_unsigned_subtraction: ctx.no_unsigned_subtraction(),
            div_precision_increment: ctx.div_precision_increment(),
            ..Self::default()
        }
    }

    /// Every column of the scope in row order.
    pub(crate) fn column_list(&self) -> Vec<(String, FieldType)> {
        self.tables
            .iter()
            .flat_map(|t| t.columns.iter().cloned())
            .collect()
    }

    pub(crate) fn width(&self) -> usize {
        self.tables.iter().map(|t| t.columns.len()).sum()
    }

    /// The column at a row offset, with the name it answers to.
    pub(crate) fn column_at(&self, offset: usize) -> Option<&(String, FieldType)> {
        self.tables
            .iter()
            .find(|t| (t.offset..t.offset + t.columns.len()).contains(&offset))
            .and_then(|t| t.columns.get(offset - t.offset))
    }

    /// How a row offset is written when it must be named unambiguously:
    /// `table.column`, the form a coalesced join's synthesized equality uses
    /// to reach the side it means.
    pub(crate) fn qualified_path(&self, offset: usize) -> Option<Vec<String>> {
        let table = self
            .tables
            .iter()
            .find(|t| (t.offset..t.offset + t.columns.len()).contains(&offset))?;
        let (name, _) = table.columns.get(offset - table.offset)?;
        Some(vec![table.name.clone(), name.clone()])
    }

    /// Every column an unqualified `*` expands to, in display order (Go's
    /// `unfoldWildStar` over the join's own output names).
    pub(crate) fn star_columns(&self) -> Vec<(usize, String, FieldType)> {
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
pub(crate) fn single_table_scope(
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
pub(crate) struct ScopeResolver<'a> {
    pub(crate) scope: &'a FromScope,
}

impl ScopeResolver<'_> {
    /// Go `expression.ColumnFullName(db, table, column)` -- the `OrigName` a
    /// resolved column carries and the only text the 1260 `GROUP_CONCAT`
    /// truncation message renders. The rewritten `Expression` keeps only an
    /// index and a unique id, so the name has to be read here, where the
    /// scope still knows which table the reference bound to.
    pub(crate) fn orig_name(&self, path: &[String]) -> Option<String> {
        let (index, _, _) = self.resolve(path)?;
        let table =
            self.scope.tables.iter().find(|table| {
                (table.offset..table.offset + table.columns.len()).contains(&index)
            })?;
        let column = table.columns.get(index - table.offset)?;
        let database = table.database.as_deref()?;
        Some(format!(
            "{}.{}.{}",
            database.to_lowercase(),
            table.name.to_lowercase(),
            column.0.to_lowercase()
        ))
    }
}

/// A resolver over `scope`, for the modules that build their own expressions.
pub(crate) fn scope_resolver(scope: &FromScope) -> impl ColumnResolver + '_ {
    ScopeResolver { scope }
}

impl ColumnResolver for ScopeResolver<'_> {
    /// The zone the scope's build point took from the statement's
    /// `StmtContext` -- see [`FromScope::zone`].
    fn time_zone(&self) -> tidb_expr::SessionTimeZone {
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
            None => tidb_util::printer::get_tidb_info(
                &tidb_util::versioninfo::VersionInfo::build_default(),
            )
            .len(),
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
                let minimal = crate::StmtContext::for_query();
                tidb_expr::fold_constant_in_mode(expression, &minimal, mode);
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
                if !q.eq_ignore_ascii_case(&table.name) {
                    continue;
                }
            }
            if let Some(schema) = schema {
                // An aliased or synthetic source carries no schema, so a
                // schema-qualified reference cannot name it.
                match &table.database {
                    Some(db) if db.eq_ignore_ascii_case(schema) => {}
                    _ => continue,
                }
            }
            for (i, (candidate, ft)) in table.columns.iter().enumerate() {
                if candidate.eq_ignore_ascii_case(name) {
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

/// A derived table's MATERIALIZED relation: the alias it answers to, its
/// column list and its rows.
///
/// Split out of [`build_derived_source`] because a multi-table write's `FROM`
/// needs the rows themselves rather than an executor over them (see
/// `multi_dml`, whose joined row carries a per-table row identity beside the
/// values). Both callers therefore apply ONE reading of the two rules a
/// derived table's NAME and SHAPE must satisfy -- Go's
/// `ErrDerivedMustHaveAlias` (1248) and `ErrDupFieldName` (1060) -- instead of
/// two that can drift.
type DerivedSourceRelation<'a> = (&'a str, Vec<(String, FieldType)>, Vec<Vec<Datum>>);

pub(crate) fn derived_source_relation<'a>(
    subquery: &QueryStmt,
    alias: Option<&'a str>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<DerivedSourceRelation<'a>, DriverError> {
    let alias = alias.filter(|alias| !alias.is_empty());
    let Some(alias) = alias else {
        return Err(DriverError::DerivedMustHaveAlias);
    };
    let (columns, rows) = super::run_query_stmt(subquery, catalog, current_db, ctx)?;
    for (index, (name, _)) in columns.iter().enumerate() {
        if columns[..index]
            .iter()
            .any(|(earlier, _)| earlier.eq_ignore_ascii_case(name))
        {
            return Err(DriverError::DuplicateColumnName(name.clone()));
        }
    }
    Ok((alias, columns, rows))
}

/// Applies a derived table's `(c1, c2, ...)` alias column list.
///
/// The list renames the subquery's own output columns positionally, and a
/// length disagreement is Go's `ErrViewWrongList` (1353) -- captured, the same
/// error a `CREATE VIEW v (a, b) AS SELECT 1` mismatch reports.
pub(crate) fn rename_derived_columns(
    columns: &mut [(String, FieldType)],
    names: &[String],
) -> Result<(), DriverError> {
    if names.is_empty() {
        return Ok(());
    }
    if names.len() != columns.len() {
        return Err(DriverError::ViewWrongList);
    }
    for (column, name) in columns.iter_mut().zip(names) {
        column.0 = name.clone();
    }
    Ok(())
}

/// The names a `SELECT`'s fields give the relation they produce, when they can
/// be read off the statement alone.
///
/// The lateral path needs the names BEFORE it can run anything, and the run it
/// uses to settle the column TYPES has the correlated columns replaced by
/// literals -- which would rename `SELECT t.a` to the literal's own text. This
/// applies the same naming rule the plain select path uses (an alias, else a
/// column reference's bare name, else the restored expression), and gives up
/// (`None`) on a `*` field, whose width is not known from the statement.
pub(crate) fn derived_field_names(select: &tidb_ast::SelectStmt) -> Option<Vec<String>> {
    select
        .fields
        .fields()
        .iter()
        .enumerate()
        .map(|(field_index, field)| match field {
            SelectField::Expr { expr, alias } => Some(alias.clone().unwrap_or_else(|| {
                crate::driver::default_field_display_name(&select.fields, field_index, expr)
            })),
            _ => None,
        })
        .collect()
}

/// [`derived_field_names`], widened to a `QueryStmt`: a set operation's output
/// is named after its LEFTMOST `SELECT` term, the same rule Go's `buildSetOpr`
/// uses when it derives the result schema from the first child.
pub(crate) fn derived_field_names_query(query: &QueryStmt) -> Option<Vec<String>> {
    match query {
        QueryStmt::Select(select) => derived_field_names(select),
        QueryStmt::SetOpr(set_opr) => {
            let first = set_opr.terms.first()?;
            match &first.body {
                tidb_ast::SetOprTermBody::Select(select) => derived_field_names(select),
                tidb_ast::SetOprTermBody::Nested(nested) => {
                    derived_field_names_query(&QueryStmt::SetOpr(nested.clone()))
                }
            }
        }
    }
}

/// A type-carrying stand-in value for a column of type `ft`.
///
/// Type inference over the probe run needs a datum of the right KIND, and
/// nothing more -- so this is the neutral value of that kind, never a bound
/// that arithmetic in the subquery could overflow. Types with no obvious
/// neutral value fall back to NULL, which is what the probe used before types
/// were carried at all.
pub(crate) fn probe_datum(ft: &FieldType) -> Datum {
    match tidb_datatype::get_min_value(ft) {
        Datum::Int(_) => Datum::Int(0),
        Datum::UInt(_) => Datum::UInt(0),
        Datum::Real(_) => Datum::Real(0.0),
        Datum::Float32(_) => Datum::Float32(0.0),
        Datum::Decimal(_) => Datum::new_decimal(tidb_datatype::Decimal::from_signed_literal("0")),
        other => other,
    }
}

pub(crate) const MAX_VIEW_DEPTH: usize = 32;

thread_local! {
    static VIEW_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

pub(crate) struct ViewDepthGuard;

impl ViewDepthGuard {
    /// Enters one view body, refusing to go past [`MAX_VIEW_DEPTH`].
    pub(crate) fn enter(qualified: &str) -> Result<ViewDepthGuard, DriverError> {
        VIEW_DEPTH.with(|depth| {
            if depth.get() >= MAX_VIEW_DEPTH {
                return Err(DriverError::Schema(SchemaErrorKind::ViewInvalid(
                    qualified.to_owned(),
                )));
            }
            depth.set(depth.get() + 1);
            Ok(ViewDepthGuard)
        })
    }
}

impl Drop for ViewDepthGuard {
    fn drop(&mut self) {
        VIEW_DEPTH.with(|depth| depth.set(depth.get() - 1));
    }
}

/// Runs a view's stored `SELECT` and presents its rows as a `FROM` source.
///
/// Go rewrites the reference into a derived table over the view's plan; the
/// rows here are materialized instead, which is the same result for a reader
/// (the outer `WHERE`, joins and `ORDER BY` all apply to the view's output
/// either way) and differs only in that nothing is pushed into the view.
///

pub(crate) fn view_source_relation(
    view: &ViewDef,
    database: &str,
    name: &str,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<MaterializedRelation, DriverError> {
    let qualified = format!("{database}.{name}");
    let _guard = ViewDepthGuard::enter(&qualified)?;
    let invalid = || DriverError::Schema(SchemaErrorKind::ViewInvalid(qualified.clone()));
    let (body_columns, rows) =
        run_select_meta_in(&view.select_sql, catalog, database, ctx).map_err(|_| invalid())?;
    if body_columns.len() != view.columns.len() {
        return Err(invalid());
    }
    let columns = view
        .columns
        .iter()
        .zip(&body_columns)
        .map(|((name, _), (_, ft))| (name.clone(), ft.clone()))
        .collect();
    Ok((columns, rows))
}

/// One common column of a `NATURAL`/`USING` join: the row offset that stays
/// visible under the shared name, and the one that is coalesced away.
pub(crate) struct CommonColumn {
    pub(crate) visible: usize,
    pub(crate) redundant: usize,
}

/// Go `PlanBuilder.coalesceCommonColumns`, as the naming half of a join.
///
/// The whole of `NATURAL JOIN` and `JOIN ... USING` is this: an ordinary join
/// whose `ON` is `l.c = r.c` for every common column `c`, plus a rule about
/// which names the result answers to. Nothing about the ROW changes -- it is
/// still the left side's columns followed by the right side's -- so this
/// returns the common pairs and rewrites only `scope`'s [`FromScope::star`]
/// and [`FromScope::coalesced`], and every consumer downstream (`*`, name
/// resolution, `ONLY_FULL_GROUP_BY`, pruning) reads the scope it always did.
///
/// Captured from Go, and the reason the two orders below are separate:
///
/// * the common columns come FIRST, ordered by the LEFT side's own column
///   order -- not by the order the `USING` list writes them (`m1 JOIN m2
///   USING (b, a)` and `USING (a, b)` both report `a, b, ...`);
/// * a RIGHT join reports right-then-left throughout, so its common columns
///   take the RIGHT side's order and the surviving copy is the RIGHT side's
///   column. That is Go's `leftPlan, rightPlan = rightPlan, leftPlan` swap,
///   and it is what makes the survivor always the OUTER (row-preserving)
///   side, whose value is never the NULL-padded one.
///
/// `using` empty means `NATURAL`: every common name participates.
pub(crate) fn coalesce_common_columns(
    scope: &mut FromScope,
    left_visible: Vec<(usize, String, FieldType)>,
    right_visible: Vec<(usize, String, FieldType)>,
    join_tp: tidb_ast::JoinType,
    using: &[String],
) -> Result<Vec<CommonColumn>, DriverError> {
    // The RIGHT-join mirror: from here on "left" means the outer side.
    let (outer, inner) = match join_tp {
        tidb_ast::JoinType::Right => (right_visible, left_visible),
        _ => (left_visible, right_visible),
    };
    let lower = |name: &str| name.to_ascii_lowercase();
    let filter: Vec<String> = using.iter().map(|name| lower(name)).collect();
    let named = |name: &str| filter.is_empty() || filter.iter().any(|f| f == &lower(name));

    // Go checks ambiguity BEFORE matching: a name that a side offers twice
    // (which only a join can produce) has no single column to coalesce.
    // Without a `USING` filter the check applies to the common names only,
    // with one it applies to every name the filter mentions.
    let ambiguous = |side: &[(usize, String, FieldType)], name: &str| {
        side.iter()
            .filter(|(_, candidate, _)| candidate.eq_ignore_ascii_case(name))
            .count()
            > 1
    };
    let in_both = |name: &str| {
        outer.iter().any(|(_, n, _)| n.eq_ignore_ascii_case(name))
            && inner.iter().any(|(_, n, _)| n.eq_ignore_ascii_case(name))
    };
    for (_, name, _) in outer.iter().chain(inner.iter()) {
        // `NATURAL` has only the common names to coalesce, so only those can
        // be ambiguous; `USING` answers for every name it lists.
        if !named(name) || (filter.is_empty() && !in_both(name)) {
            continue;
        }
        if ambiguous(&outer, name) || ambiguous(&inner, name) {
            return Err(DriverError::AmbiguousColumnInClause {
                column: lower(name),
                clause: "from clause".to_owned(),
            });
        }
    }

    let mut common: Vec<CommonColumn> = Vec::new();
    let mut taken: Vec<String> = Vec::new();
    for (offset, name, _) in &outer {
        // `USING (a, a)`: Go's filter is a set, so a name coalesces once.
        if !named(name) || taken.contains(&lower(name)) {
            continue;
        }
        let Some((inner_offset, ..)) = inner.iter().find(|(_, n, _)| n.eq_ignore_ascii_case(name))
        else {
            continue;
        };
        taken.push(lower(name));
        common.push(CommonColumn {
            visible: *offset,
            redundant: *inner_offset,
        });
    }

    // A `USING` name neither side pairs up is Go's ErrUnknownColumn against
    // the `from clause` -- the same 1054 a missing column anywhere reports.
    if let Some(missing) = filter.iter().find(|name| !taken.contains(name)) {
        return Err(DriverError::UnknownColumnInClause {
            column: missing.clone(),
            clause: "from clause".to_owned(),
        });
    }

    // Display order: the common columns, then the outer side's remaining
    // columns, then the inner side's.
    let remaining = |side: &[(usize, String, FieldType)], common: &[CommonColumn]| -> Vec<usize> {
        side.iter()
            .map(|(offset, ..)| *offset)
            .filter(|offset| {
                !common
                    .iter()
                    .any(|c| c.visible == *offset || c.redundant == *offset)
            })
            .collect()
    };
    scope.star = common
        .iter()
        .map(|c| c.visible)
        .chain(remaining(&outer, &common))
        .chain(remaining(&inner, &common))
        .collect();
    scope.coalesced.extend(common.iter().map(|c| c.redundant));
    Ok(common)
}

/// The table a single-table `UPDATE`/`DELETE` targets.
pub(crate) fn single_table_name(
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
pub(crate) fn extra_handle_column(entry: &TableEntry) -> Option<(String, FieldType)> {
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
