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

/// The joined `FROM` scope: every table's columns concatenated left to right,
/// which is the row layout [`JoinExec`] produces.
#[derive(Clone, Debug, Default)]
pub(crate) struct FromScope {
    pub(crate) tables: Vec<FromTable>,
}

impl FromScope {
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
}

/// Resolves a column reference against the joined `FROM` scope.
///
/// A qualified `t.a` binds to table `t`'s column; an unqualified `a` binds to
/// the one table that has such a column, and is rejected as ambiguous when
/// several do -- MySQL's `ERROR 1052 (23000): Column 'a' in field list is
/// ambiguous`, which Go raises from `expression.buildColumn`.
pub(crate) struct ScopeResolver<'a> {
    pub(crate) scope: &'a FromScope,
}

/// A resolver over `scope`, for the modules that build their own expressions.
pub(crate) fn scope_resolver(scope: &FromScope) -> impl ColumnResolver + '_ {
    ScopeResolver { scope }
}

impl ColumnResolver for ScopeResolver<'_> {
    fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
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
}

/// Builds the `FROM` scope and the executor that produces its rows.
///
/// Go's `buildJoin` builds a left-deep tree of `LogicalJoin`s over the
/// `FROM` list; this walks the same tree, so `a JOIN b JOIN c` nests as
/// `(a JOIN b) JOIN c` and the row layout is `a`'s columns, then `b`'s, then
/// `c`'s.
///
/// A `LATERAL` derived table on the right of a join is not a join at all but
/// an Apply, so it leaves this path early -- see `build_lateral_join`.
///
/// DEFERRED (documented): `USING`, `NATURAL`, and `STRAIGHT_JOIN`'s ordering
/// guarantee.
///
/// `trace` records the operator each branch commits to, so `EXPLAIN` prints
/// the FROM clause the driver actually built rather than a second guess at
/// it. A shape the recorder has never printed (a derived table, a lateral
/// join) marks the trace refused instead of inventing a node for it.
pub(crate) fn build_from(
    node: &JoinNode,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    match node {
        JoinNode::Table(table_ref) => {
            // A `db.t` reference resolves in that schema; a bare `t` resolves
            // in the session's current one (Go's name resolution).
            let (database, name) = split_table_path(&table_ref.name, current_db)?;
            let entry = catalog
                .get_in(database, name)
                .ok_or(DriverError::Unsupported("table not found in catalog"))?;
            // A table alias replaces the name for qualification, as in Go.
            let visible = table_ref.alias.clone().unwrap_or_else(|| name.to_owned());
            // Every base-table read starts as a whole-table scan; the fast
            // paths in `run_select_stmt` REPLACE this node at the same
            // moment they replace the executor it describes.
            if let Some(trace) = trace.as_deref_mut() {
                trace.table_full_scan(&visible);
            }
            if let TableEntry::View(view) = entry {
                let (exec, scope) = build_view_source(
                    view,
                    database,
                    name,
                    visible,
                    table_ref.alias.is_none(),
                    catalog,
                    ctx,
                )?;
                return Ok((meter(exec, trace), scope));
            }
            let columns = entry.column_list();
            let schema_columns: Vec<Column> = columns
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            let schema = Schema::new(schema_columns);
            let exec: Box<dyn Executor> = match entry {
                TableEntry::Mem(mem) => Box::new(MemTableSourceExec::new(
                    ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                    mem.rows.clone(),
                )),
                TableEntry::Kv(kv) => Box::new(TableScanExec::new(
                    ExecutorMeta::new(schema, 0, INIT_CAP, MAX_CHUNK_SIZE),
                    kv.clone(),
                )),
                // Handled above, before the columns were taken.
                TableEntry::View(_) => unreachable!("views take the branch above"),
            };
            let scope = FromScope {
                tables: vec![FromTable {
                    name: visible,
                    // An alias replaces the whole path, so `db.t.col` no
                    // longer names the table once it is aliased.
                    database: table_ref.alias.is_none().then(|| database.to_owned()),
                    columns,
                    offset: 0,
                }],
            };
            Ok((meter(exec, trace), scope))
        }
        JoinNode::Join(join) => build_join(join, catalog, current_db, ctx, trace),
        JoinNode::Derived {
            subquery,
            alias,
            lateral,
            column_names,
        } => {
            // A `LATERAL` in the LEFTMOST position has no preceding table to
            // correlate with, so it is an ordinary derived table -- the same
            // reading Go's `buildLateralJoin` reaches with an empty outer
            // schema (captured: `SELECT * FROM LATERAL (SELECT 1) x` runs).
            // Its alias column list still renames positionally.
            let _ = lateral;
            // EXPLAIN has never described a derived table (its plan is a
            // subtree this recorder does not enter), and this refactor does
            // not widen that surface.
            if let Some(trace) = trace {
                trace.refuse("derived tables are not supported yet");
            }
            let (exec, mut scope) =
                build_derived_source(subquery, alias.as_deref(), catalog, current_db, ctx)?;
            rename_derived_columns(&mut scope.tables[0].columns, column_names)?;
            Ok((exec, scope))
        }
    }
}

/// Runs a derived table's subquery and presents its rows as a `FROM` source.
///
/// Go plans the subquery and wraps it in a `LogicalProjection` the outer query
/// reads by alias; the rows are materialized here instead, which is the same
/// result for a reader. The derived table's column names are the subquery's
/// own result-field names, and only the alias qualifies them -- `db.alias.col`
/// and a base table's name are both Go's `ErrUnknownColumn` once the subquery
/// is behind an alias.
pub(crate) fn build_derived_source(
    subquery: &QueryStmt,
    alias: Option<&str>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    // Captured from Go: an alias-less derived table is ErrDerivedMustHaveAlias
    // in a plain SELECT and in a view body alike.
    let alias = alias.filter(|alias| !alias.is_empty());
    let Some(alias) = alias else {
        return Err(DriverError::DerivedMustHaveAlias);
    };
    let (columns, rows) = match subquery {
        QueryStmt::Select(select) => run_select_stmt(select, catalog, current_db, ctx)?,
        QueryStmt::SetOpr(set_opr) => run_set_opr_stmt(set_opr, catalog, current_db, ctx)?,
    };
    // A derived table is a named relation, so its columns must be uniquely
    // named: Go's ErrDupFieldName, which `(SELECT * FROM t JOIN s ...)` hits
    // whenever the joined tables share a column name.
    for (index, (name, _)) in columns.iter().enumerate() {
        if columns[..index]
            .iter()
            .any(|(earlier, _)| earlier.eq_ignore_ascii_case(name))
        {
            return Err(DriverError::DuplicateColumnName(name.clone()));
        }
    }
    let schema_columns: Vec<Column> = columns
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let exec: Box<dyn Executor> = Box::new(MemTableSourceExec::new(
        ExecutorMeta::new(Schema::new(schema_columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
        rows,
    ));
    let scope = FromScope {
        tables: vec![FromTable {
            name: alias.to_owned(),
            // An alias is the only qualifier a derived table answers to.
            database: None,
            columns,
            offset: 0,
        }],
    };
    Ok((exec, scope))
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
        .map(|field| match field {
            SelectField::Expr { expr, alias } => Some(match (alias, expr) {
                (Some(alias), _) => alias.clone(),
                (None, tidb_ast::Expr::Column(path)) => {
                    path.last().cloned().unwrap_or_else(|| expr.restore())
                }
                (None, _) => expr.restore(),
            }),
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

/// Builds a `LATERAL` derived table as an Apply over the tables preceding it.
///
/// Go's `buildLateralJoin` makes this a `LogicalApply` with `InnerJoin`: the
/// left side's columns are the outer schema the subquery's correlated columns
/// bind against, and the subquery is re-run per outer row. That is exactly
/// this crate's correlated-subquery machinery, with the one difference that a
/// derived table yields a RELATION rather than a scalar, so
/// [`crate::apply::LateralApplyExec`] concatenates every inner row onto the
/// outer row instead of appending one value.
///
/// A `LATERAL` over a set operation (`UNION`/`EXCEPT`/`INTERSECT`) walks the
/// same path: [`collect_correlated_columns_query`] and
/// [`bind_subquery_columns_query`] widen the collector and the binder to a
/// `QueryStmt`, so every term of the set operation is re-run per outer row
/// exactly like a lone `SELECT`'s clauses are, and the result's column names
/// come from the set operation's leftmost term (`derived_field_names_query`),
/// matching Go's `buildSetOpr`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_lateral_join(
    join: &tidb_ast::Join,
    left_exec: Box<dyn Executor>,
    left_scope: FromScope,
    subquery: &QueryStmt,
    alias: Option<&str>,
    column_names: &[String],
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    // Go's own rejections, in `buildLateralJoin`'s order.
    if join.natural {
        return Err(DriverError::InvalidLateralJoin(
            "NATURAL JOIN is not supported with LATERAL",
        ));
    }
    if !join.using.is_empty() {
        return Err(DriverError::InvalidLateralJoin(
            "USING clause is not supported with LATERAL",
        ));
    }
    match join.tp {
        tidb_ast::JoinType::Left => {
            return Err(DriverError::InvalidLateralJoin(
                "LEFT JOIN is not supported with LATERAL",
            ))
        }
        tidb_ast::JoinType::Right => {
            return Err(DriverError::InvalidLateralJoin(
                "RIGHT JOIN is not supported with LATERAL",
            ))
        }
        // Comma syntax (which the parser spells `CrossJoin`) and an explicit
        // INNER JOIN are the shapes Go plans.
        tidb_ast::JoinType::Cross => {}
    }
    let alias = alias.filter(|alias| !alias.is_empty());
    let Some(alias) = alias else {
        return Err(DriverError::DerivedMustHaveAlias);
    };
    // The columns the subquery's correlated references name in the left scope.
    // A set-operation subquery's terms are walked the same as a lone
    // `SELECT`'s clauses -- each term is re-run per outer row exactly like a
    // plain `SELECT` would be.
    let mut correlated = Vec::new();
    collect_correlated_columns_query(
        subquery,
        &left_scope,
        catalog,
        current_db,
        &mut correlated,
        ctx,
    );

    // The inner relation's shape must be fixed before the first outer row, so
    // it is settled by one probe run with every correlated column bound to a
    // stand-in value -- the same trick `subquery_result_type` uses for a
    // scalar Apply, except the stand-in must carry the outer column's OWN
    // type: a bare NULL would make `SELECT t.a + u.z` infer the type of
    // `NULL + NULL` rather than of two BIGINTs. The probe's VALUES are
    // discarded; only the field types (and, when the statement does not state
    // them, the names) survive.
    let probe_resolver = ScopeResolver { scope: &left_scope };
    let probes: Vec<(Vec<String>, Datum)> = correlated
        .iter()
        .map(|path| {
            let datum = probe_resolver
                .resolve(path)
                .map_or(Datum::Null, |(_, ft, _)| probe_datum(&ft));
            (path.clone(), datum)
        })
        .collect();
    let typed = bind_subquery_columns_query(subquery, &probes)?;
    let (probe_columns, _) = run_query_stmt(&typed, catalog, current_db, ctx)?;
    let mut columns: Vec<(String, FieldType)> = match derived_field_names_query(subquery) {
        Some(names) if names.len() == probe_columns.len() => names
            .into_iter()
            .zip(&probe_columns)
            .map(|(name, (_, ft))| (name, ft.clone()))
            .collect(),
        _ => probe_columns,
    };
    // A derived table is a named relation, so duplicate column names are Go's
    // ErrDupFieldName -- the same check `build_derived_source` makes.
    for (index, (name, _)) in columns.iter().enumerate() {
        if columns[..index]
            .iter()
            .any(|(earlier, _)| earlier.eq_ignore_ascii_case(name))
        {
            return Err(DriverError::DuplicateColumnName(name.clone()));
        }
    }
    rename_derived_columns(&mut columns, column_names)?;

    let left_width = left_scope.width();
    let mut scope = left_scope.clone();
    scope.tables.push(FromTable {
        name: alias.to_owned(),
        // An alias is the only qualifier a derived table answers to.
        database: None,
        columns: columns.clone(),
        offset: left_width,
    });

    let inner_width = columns.len();
    let subquery = subquery.clone();
    let correlated_paths = correlated;
    let outer_scope = left_scope;
    // The callback outlives this borrow of the catalog, so it owns a snapshot
    // (see ApplyExec::new).
    let inner_catalog = catalog.clone();
    let inner_db = current_db.to_owned();
    let inner_ctx = ctx.clone();
    let runner: crate::apply::LateralRunner = Box::new(move |values: &[Datum]| {
        let mut bindings = Vec::with_capacity(correlated_paths.len());
        let resolver = ScopeResolver {
            scope: &outer_scope,
        };
        for path in &correlated_paths {
            let (index, _, _) = resolver
                .resolve(path)
                .ok_or(ExecError::Unsupported("unresolved correlated column"))?;
            let value = values
                .get(index)
                .cloned()
                .ok_or(ExecError::Unsupported("correlated column out of range"))?;
            bindings.push((path.clone(), value));
        }
        let bound = bind_subquery_columns_query(&subquery, &bindings)
            .map_err(|e| ExecError::Unsupported(driver_error_text(&e)))?;
        let (_, rows) =
            run_query_stmt(&bound, &inner_catalog, &inner_db, &inner_ctx).map_err(|e| match e {
                DriverError::Exec(exec) => exec,
                other => ExecError::Unsupported(driver_error_text(&other)),
            })?;
        Ok(rows)
    });

    let schema_columns: Vec<Column> = scope
        .column_list()
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    debug_assert_eq!(schema_columns.len(), left_width + inner_width);
    let schema = Schema::new(schema_columns);
    let mut exec: Box<dyn Executor> = Box::new(crate::apply::LateralApplyExec::new(
        ExecutorMeta::new(schema.clone(), 6, INIT_CAP, MAX_CHUNK_SIZE),
        left_exec,
        runner,
    ));
    // `JOIN LATERAL (...) x ON <cond>`: the inner join is already produced by
    // the Apply, so the ON condition is simply a filter over its rows.
    if let Some(on) = &join.on {
        let resolver = ScopeResolver { scope: &scope };
        let predicate = rewrite_expr_resolved(on, &resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        exec = Box::new(SelectionExec::new(
            ExecutorMeta::new(schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
            vec![predicate],
            exec,
            ctx.clone(),
        ));
    }
    Ok((exec, scope))
}

/// How deep a view may nest before the reference is called invalid. A view
/// whose body reads itself (which `CREATE OR REPLACE` can build) would
/// otherwise recurse forever.
///
/// DIVERGENCE (documented): MySQL caps nesting at 61 and reports
/// `ER_VIEW_RECURSIVE` (1462); this reports `ErrViewInvalid` (1356), the same
/// error the other broken-view cases report.
pub(crate) const MAX_VIEW_DEPTH: usize = 32;

thread_local! {
    /// How many view bodies the current statement is inside.
    static VIEW_DEPTH: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Decrements the view-nesting depth however the body's evaluation ends.
pub(crate) struct ViewDepthGuard;

impl ViewDepthGuard {
    /// Enters one view body, refusing to go past [`MAX_VIEW_DEPTH`].
    fn enter(qualified: &str) -> Result<ViewDepthGuard, DriverError> {
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
/// The body's own failure is Go's `ErrViewInvalid`: the definition ran once
/// already, when the view was created, so anything that stops it running now
/// is a schema change underneath it.
pub(crate) fn build_view_source(
    view: &ViewDef,
    database: &str,
    name: &str,
    visible: String,
    alias_free: bool,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    let qualified = format!("{database}.{name}");
    let _guard = ViewDepthGuard::enter(&qualified)?;
    let invalid = || DriverError::Schema(SchemaErrorKind::ViewInvalid(qualified.clone()));
    // The definition is stored schema-qualified, so it resolves in the view's
    // own schema rather than the reader's.
    let (body_columns, rows) =
        run_select_meta_in(&view.select_sql, catalog, database, ctx).map_err(|_| invalid())?;
    if body_columns.len() != view.columns.len() {
        return Err(invalid());
    }
    // The view's own column names win over the body's, which is what a
    // `CREATE VIEW v (a2) AS SELECT a ...` column list means.
    let columns: Vec<(String, FieldType)> = view
        .columns
        .iter()
        .zip(&body_columns)
        .map(|((name, _), (_, ft))| (name.clone(), ft.clone()))
        .collect();
    let schema_columns: Vec<Column> = columns
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let exec: Box<dyn Executor> = Box::new(MemTableSourceExec::new(
        ExecutorMeta::new(Schema::new(schema_columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
        rows,
    ));
    let scope = FromScope {
        tables: vec![FromTable {
            name: visible,
            database: alias_free.then(|| database.to_owned()),
            columns,
            offset: 0,
        }],
    };
    Ok((exec, scope))
}

/// Builds one join node (or passes through the single-table wrapper).
pub(crate) fn build_join(
    join: &tidb_ast::Join,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<(Box<dyn Executor>, FromScope), DriverError> {
    let (left_exec, left_scope) =
        build_from(&join.left, catalog, current_db, ctx, trace.as_deref_mut())?;
    let Some(right_node) = &join.right else {
        // The single-table wrapper the parser always produces.
        return Ok((left_exec, left_scope));
    };
    if let JoinNode::Derived {
        subquery,
        alias,
        lateral: true,
        column_names,
    } = right_node
    {
        // An Apply, not a join: a shape the plan recorder does not print.
        if let Some(trace) = trace.as_deref_mut() {
            trace.refuse("LATERAL derived tables are not supported yet");
        }
        return build_lateral_join(
            join,
            left_exec,
            left_scope,
            subquery,
            alias.as_deref(),
            column_names,
            catalog,
            current_db,
            ctx,
        );
    }
    if join.natural || !join.using.is_empty() {
        return Err(DriverError::Unsupported(
            "NATURAL and USING joins are not supported yet",
        ));
    }
    let (right_exec, right_scope) =
        build_from(right_node, catalog, current_db, ctx, trace.as_deref_mut())?;

    // The joined scope: the right tables' columns follow the left's.
    let left_width = left_scope.width();
    let mut scope = left_scope;
    for table in right_scope.tables {
        scope.tables.push(FromTable {
            name: table.name,
            database: table.database,
            columns: table.columns,
            offset: table.offset + left_width,
        });
    }

    let column_list = scope.column_list();
    let schema_columns: Vec<Column> = column_list
        .iter()
        .enumerate()
        .map(|(i, (_, ft))| {
            let mut col = Column::new((i + 1) as i64, ft.clone());
            col.index = i as i64;
            col
        })
        .collect();
    let meta = ExecutorMeta::new(Schema::new(schema_columns), 6, INIT_CAP, MAX_CHUNK_SIZE);

    let conditions = match &join.on {
        Some(expr) => {
            let resolver = ScopeResolver { scope: &scope };
            vec![rewrite_expr_resolved(expr, &resolver)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?]
        }
        None => Vec::new(),
    };
    let kind = match join.tp {
        tidb_ast::JoinType::Cross => JoinKind::Inner,
        tidb_ast::JoinType::Left => JoinKind::Left,
        tidb_ast::JoinType::Right => JoinKind::Right,
    };
    // The condition split the executor will run on, so EXPLAIN's
    // `equal:[...]`/`other cond:` and the hash table's own keys are one
    // decision rather than two that can drift.
    let split = crate::hash_join::split_equi(&conditions, left_width);
    // Go's stats-less build side: the inner (non-preserved) child, which is
    // the left one only for a RIGHT join. See `join.rs`'s module doc.
    let build_is_left = kind == JoinKind::Right;
    let exec: Box<dyn Executor> = Box::new(JoinExec::new(
        meta,
        kind,
        conditions,
        left_exec,
        right_exec,
        ctx.clone(),
    ));
    if let Some(trace) = trace.as_deref_mut() {
        if trace
            .join(join, &scope, current_db, &split.equal_mask, build_is_left)
            .is_err()
        {
            trace.refuse("this join's plan is not supported yet");
        }
    }
    Ok((meter(exec, trace), scope))
}

/// Meters `exec` for the node the trace just recorded, when there is one.
fn meter(exec: Box<dyn Executor>, trace: Option<&mut PlanTrace>) -> Box<dyn Executor> {
    match trace {
        Some(trace) => trace.meter(exec),
        None => exec,
    }
}

/// The table a single-table `UPDATE`/`DELETE` targets.
pub(crate) fn single_table_name(
    table_ref: &tidb_ast::TableRef,
    current_db: &str,
) -> Result<(String, String), DriverError> {
    let (database, name) = split_table_path(&table_ref.name, current_db)?;
    Ok((database.to_owned(), name.to_owned()))
}
