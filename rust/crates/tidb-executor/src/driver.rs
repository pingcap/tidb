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

//! A minimal query driver: parse a SQL string, rewrite its expressions, wire the
//! executors, and run it -- the first end-to-end parse -> plan -> execute of a
//! SQL string.
//!
//! SCOPE: `SELECT <exprs | *> [FROM <table>] [WHERE <pred>] [ORDER BY ...]
//! [LIMIT ...]` over a single in-memory [`Catalog`] table or the implicit dual
//! row. It parses via `tidb-parser`, resolves `FROM` against the catalog,
//! rewrites fields/predicates/by-items through
//! [`tidb_expr::rewriter::rewrite_expr_resolved`] (columns bound by the
//! [`TableResolver`]), and wires `MemTableSource|TableDual ->
//! [Selection] -> [Sort] -> Projection -> [Limit]`.
//!
//! DEFERRED (documented): joins and derived tables, `db.t` qualification
//! (single-schema catalog), ordering by select alias/position, and everything
//! the rewriter does not yet handle. The real storage-backed `TableReaderExec`
//! replaces [`MemTableSourceExec`] when storage/tablecodec integration lands.

use crate::access_path::{HandleSourceExec, IndexRangeSourceExec};
use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::hash_agg::{AggFunc, AggKind, HashAggExec};
use crate::join::{JoinExec, JoinKind};
use crate::kv_table::{IndexRange, KvTable, TableHandle, TableScanExec};
use crate::limit::LimitExec;
use crate::mem_quota;
use crate::mem_table::MemTableSourceExec;
use crate::plan_trace::{PlanTrace, Qualifier};
use crate::predicate_pushdown::{
    PushedScanFilter, ScanComparison, ScanComparisonOp, ScanPredicate,
};
use crate::projection::ProjectionExec;
use crate::selection::SelectionExec;
use crate::sort::{SortByItem, SortExec};
use crate::table_dual::TableDualExec;
use crate::topn::TopNExec;
use std::collections::HashMap;
use std::sync::Arc;
use tidb_ast::{JoinNode, QueryStmt, SelectField, SelectFieldList, Stmt};
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::builtin_compare::refine_comparisons;
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::{rewrite_expr_resolved, ColumnResolver};

/// The name an unaliased field takes: a column reference keeps its column
/// name, anything else keeps the text it was WRITTEN with -- Go's
/// `SelectField.Text`, backed here by the parser-recorded per-field source
/// span (see `tidb_ast::SelectFieldList::text`). `count(*)` therefore names
/// the column `count(*)` even though `expr` itself restores as `COUNT(1)`
/// (the parser lowers a bare `*` argument to the AST literal `1`, matching
/// the same lowering Go's own hand-written parser performs -- see
/// `pkg/parser/expr_func_parser.go`'s `parseAggregateFuncCall`). A user who
/// writes `count(1)` literally still gets `count(1)`, since both cases read
/// the same original bytes; nothing here special-cases the star string.
///
/// Falls back to `expr.restore()` when the parser recorded no source text
/// for this field (for example a field synthesized by a rewrite pass rather
/// than parsed from source).
pub(crate) fn default_field_display_name(
    fields: &SelectFieldList,
    index: usize,
    expr: &tidb_ast::Expr,
) -> String {
    if let tidb_ast::Expr::Column(path) = expr {
        return path.last().cloned().unwrap_or_default();
    }
    // Go `getInnerFromParenthesesAndUnaryPlus`: the literal test looks through
    // parentheses and a unary `+`, so `(null)` and `+1` are named as their
    // inner literal.
    let inner = inner_field_expr(expr);
    // Go `buildProjectionFieldNameFromExpressions`: a literal gets special
    // handling. This tier corrects the one case that diverges -- a bare NULL
    // literal is named `NULL` (Go's `types.KindNull` arm), not the lowercase
    // `null` its source text carried -- and leaves every other literal named by
    // the text it was written with, as before.
    if is_value_literal(inner) {
        if matches!(inner, tidb_ast::Expr::Null) {
            return "NULL".to_owned();
        }
        return fields
            .text(index)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .map_or_else(|| expr.restore(), str::to_owned);
    }
    // Non-literal: named by its source text with MySQL special-result-field
    // comment markers removed -- Go's
    // `SpecFieldPattern.ReplaceAllStringFunc(field.Text(), TrimComment)`, which
    // drops every `*/` and `/*!<version>` marker. A `/*+ hint */` therefore
    // keeps `/*+ hint ` in the label because only the closing `*/` matches.
    let text = fields
        .text(index)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
        .map_or_else(|| expr.restore(), str::to_owned);
    strip_spec_field_comment_markers(&text)
}

/// Go `getInnerFromParenthesesAndUnaryPlus`: strips enclosing parentheses and
/// leading unary `+` to reach the expression that decides the column name.
fn inner_field_expr(expr: &tidb_ast::Expr) -> &tidb_ast::Expr {
    match expr {
        tidb_ast::Expr::Paren(inner) | tidb_ast::Expr::Unary(tidb_ast::UnaryOp::Plus, inner) => {
            inner_field_expr(inner)
        }
        other => other,
    }
}

/// Whether `expr` is one of Go's `driver.ValueExpr` literals, which
/// `buildProjectionFieldNameFromExpressions` names by a rule of their own
/// rather than by the field's source text.
fn is_value_literal(expr: &tidb_ast::Expr) -> bool {
    matches!(
        expr,
        tidb_ast::Expr::Null
            | tidb_ast::Expr::Int(_)
            | tidb_ast::Expr::Decimal(_)
            | tidb_ast::Expr::Float(_)
            | tidb_ast::Expr::Hex(_)
            | tidb_ast::Expr::Bit(_)
            | tidb_ast::Expr::String(_)
            | tidb_ast::Expr::RawString(_)
            | tidb_ast::Expr::Bool(_)
    )
}

/// Go `SpecFieldPattern.ReplaceAllStringFunc(text, TrimComment)`: removes each
/// MySQL special-result-field comment marker -- a closing `*/`, and an opening
/// `/*!` optionally followed by a 5-6 digit (optionally `M`-prefixed) version
/// -- leaving the rest of the text, including a `/*+ ...` optimizer-hint
/// opener, untouched.
fn strip_spec_field_comment_markers(text: &str) -> String {
    let bytes = text.as_bytes();
    // Only ASCII marker sequences are removed from valid UTF-8, so copying the
    // surviving bytes and reinterpreting them never splits a code point.
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        // A closing `*/`.
        if bytes[i] == b'*' && i + 1 < bytes.len() && bytes[i + 1] == b'/' {
            i += 2;
            continue;
        }
        // An opening `/*!`, with an optional `M` and a 5-6 digit version, all
        // of which Go's `SpecFieldPattern` consumes as one match and
        // `TrimComment` drops.
        if bytes[i] == b'/' && i + 2 < bytes.len() && bytes[i + 1] == b'*' && bytes[i + 2] == b'!' {
            let mut j = i + 3;
            if j < bytes.len() && bytes[j] == b'M' {
                j += 1;
            }
            let digit_start = j;
            while j < bytes.len() && j - digit_start < 6 && bytes[j].is_ascii_digit() {
                j += 1;
            }
            // The version group is 5-6 digits, or absent; a 1-4 digit run is
            // not a version, so the marker is still just `/*!`.
            if j - digit_start == 0 || (5..=6).contains(&(j - digit_start)) {
                i = j;
                continue;
            }
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| text.to_owned())
}
use tidb_expr::schema::Schema;

pub(crate) mod access;
mod agg_build;
mod agg_select;
mod catalog;
mod clause_resolve;
mod dml;
mod errors;
mod from;
pub(crate) mod funcdep;
mod grouping;
mod having;
pub(crate) mod index_join_decision;
pub mod infoschema_meta;
mod join_reorder;
pub(crate) mod leaf_demand;
mod merge_decision;
mod multi_dml;
mod only_full_group_by;
mod outer_join_elimination;
mod params;
mod point_get_key;
mod predicate_push_down;
mod recursive_cte;
mod set_opr;
mod subquery;
#[cfg(test)]
mod tests;
mod through_proj;
mod write_cast;

// Re-exported flat, so every caller inside and outside this module keeps
// naming these as `driver::…` exactly as before the split.
pub(crate) use access::*;
pub(crate) use agg_build::*;
pub(crate) use agg_select::*;
pub use catalog::*;
pub(crate) use clause_resolve::*;
pub use dml::*;
pub(crate) use from::*;
pub(crate) use grouping::*;
pub use params::*;
pub use set_opr::*;
pub(crate) use subquery::*;
pub(crate) use write_cast::*;

pub use errors::{DriverError, MysqlError, SchemaErrorKind, TxnErrorKind, VarErrorKind};

const INIT_CAP: usize = 1;
const MAX_CHUNK_SIZE: usize = 1024;

/// Parses and runs a `FROM`-less `SELECT`, returning its rows as `Datum`s.
pub fn run_select(sql: &str) -> Result<Vec<Vec<Datum>>, DriverError> {
    run_select_on(sql, &Catalog::default(), &crate::StmtContext::for_query())
}

/// Parses and runs a single-table (or `FROM`-less) `SELECT` against `catalog`,
/// returning its rows as `Datum`s.
pub fn run_select_on(
    sql: &str,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    run_select_meta_on(sql, catalog, ctx).map(|(_, rows)| rows)
}

/// A `SELECT` result with metadata: the output columns as `(name, type)`, then
/// the rows.
pub type SelectMeta = (Vec<(String, FieldType)>, Vec<Vec<Datum>>);

/// Like [`run_select_on`], but also returns the result-column metadata the
/// wire protocol needs: one `(name, type)` per output column.
///
/// Naming follows Go's result-field resolution in spirit, simplified for the
/// seed driver: an `AS` alias wins; a plain column reference uses the column's
/// own name; any other expression uses the text it was WRITTEN with (Go's
/// `SelectField.Text`, see [`default_field_display_name`]), falling back to
/// its restored text when the parser recorded no source span for the field;
/// `*` expands to the table's column names.
pub fn run_select_meta_on(
    sql: &str,
    catalog: &Catalog,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_select_meta_in(sql, catalog, DEFAULT_DATABASE, ctx)
}

/// [`run_select_meta_on`] resolving unqualified names in `current_db`.
pub fn run_select_meta_in(
    sql: &str,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    let stmt = ctx.parse(sql)?;

    let select = match &stmt {
        Stmt::Query(query) => match &**query {
            QueryStmt::Select(select) => select,
            QueryStmt::SetOpr(set_opr) => {
                return run_set_opr_stmt(set_opr, catalog, current_db, ctx)
            }
        },
        _ => return Err(DriverError::unsupported("only SELECT is supported")),
    };
    run_select_stmt(select, catalog, current_db, ctx)
}

/// Materializes a `WITH` clause's CTEs into `catalog`, so the query that
/// follows resolves them like ordinary tables.
///
/// Go plans a non-recursive CTE as its own subtree the outer query reads from
/// (`buildWith`), and a later CTE may reference an earlier one; materializing
/// them in written order gives that.
///
/// `WITH RECURSIVE` reaches the same loop: `RECURSIVE` is a clause-level flag
/// that only PERMITS a CTE to name itself, so it is passed through to
/// [`recursive_cte::materialize_cte_body`] rather than branched on here.
fn materialize_ctes(
    with: &tidb_ast::WithClause,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Catalog, DriverError> {
    // The scratch catalog carries the real tables too, since the CTE bodies
    // and the outer query both read them.
    let mut scratch = catalog.clone();
    for cte in &with.ctes {
        // Each CTE sees the ones already materialized, which is what lets a
        // later one reference an earlier one.
        let (columns, rows) = recursive_cte::materialize_cte_body(
            &cte.name,
            &cte.columns,
            &cte.query,
            &scratch,
            current_db,
            ctx,
            with.recursive,
        )?;
        scratch.register_mem_in(current_db, &cte.name, MemTable { columns, rows });
    }
    Ok(scratch)
}

/// Runs a `QueryStmt` of either shape against the catalog: the same dispatch
/// [`build_derived_source`] makes over a derived table's subquery, factored
/// out so the lateral-over-set-operation path can share it.
pub(crate) fn run_query_stmt(
    query: &QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    match query {
        QueryStmt::Select(select) => run_select_stmt(select, catalog, current_db, ctx),
        QueryStmt::SetOpr(set_opr) => run_set_opr_stmt(set_opr, catalog, current_db, ctx),
    }
}

/// Runs one parsed `SELECT` against the catalog, for a caller that has
/// already rewritten the statement (session-variable binding, for instance)
/// and must not go back through SQL text.
pub fn run_select_meta_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_select_stmt(select, catalog, current_db, ctx)
}

/// Go `restoreSchemaIfChanged`, for a scope whose leaves the join reorder
/// moved.
///
/// A reorder changes the ROW layout and nothing else: every column still
/// answers to the same name, so only `*` -- which expands in row order -- can
/// see the difference. Recording the written order in `FromScope::star` is
/// therefore the whole restore.
///
/// A no-op unless every group leaf became exactly one scope table, which is
/// what [`crate::driver::join_reorder`] only ever produces; a coalesced scope
/// already owns `star` and is never reordered.
fn restore_written_order(scope: &mut FromScope, written_order: &[usize]) {
    if scope.tables.len() != written_order.len() || !scope.star.is_empty() {
        return;
    }
    let mut star = Vec::with_capacity(scope.width());
    for position in written_order {
        let Some(table) = scope.tables.get(*position) else {
            return;
        };
        star.extend(table.offset..table.offset + table.columns.len());
    }
    scope.star = star;
}

/// The name a source operator's `access object` prints: the alias the FROM
/// clause gave the table, which is what Go prints too.
fn source_table_name<'a>(scope: &'a FromScope, table: &'a str) -> &'a str {
    match scope.tables.first() {
        Some(first) => &first.name,
        None => table,
    }
}

/// Runs one parsed `SELECT` against the catalog.
fn run_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_select_traced(select, catalog, current_db, ctx, None)
}

/// [`run_select_stmt`], recording the plan it builds into `trace`.
///
/// This is the one control flow that decides a `SELECT`'s shape, so it is
/// also the only place that describes one: each site that commits to an
/// executor records the matching node (see [`crate::plan_trace`]), and in
/// `EXPLAIN ANALYZE` mode the executor is metered so the node's `actRows` is
/// the count that operator really produced. A plan-only trace stops before
/// the drain below, so plain `EXPLAIN` yields no result row.
pub(crate) fn run_select_traced(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
) -> Result<SelectMeta, DriverError> {
    // The statement as written, which the plan text is rendered from: the
    // rewrites below (CTE materialization, subquery folding, window
    // hoisting) change what is EXECUTED, not what the user asked for.
    let traced_select = select;
    // A WITH clause's CTEs are materialized first, then the query runs against
    // a catalog that contains them.
    let with_catalog;
    let catalog = match &select.with {
        Some(with) => {
            with_catalog = materialize_ctes(with, catalog, current_db, ctx)?;
            &with_catalog
        }
        None => catalog,
    };
    // Uncorrelated subqueries are evaluated now and folded into literals, so
    // everything below plans against ordinary expressions (Go's
    // handleScalarSubquery for the non-Apply case).
    let folded;
    let select = if select_has_uncorrelated_subquery(select, catalog, current_db, ctx) {
        let outer = select_outer_scope(select, catalog, current_db, ctx);
        folded = fold_select_subqueries(select, &outer, catalog, current_db, ctx)?;
        &folded
    } else {
        select
    };
    // Go's `tidb_opt_join_reorder_through_proj`: a derived table whose
    // projection sits directly on a join dissolves into the statement, so the
    // relations under it become leaves of THIS join group and the reorder can
    // move them. Both of Go's gates and this tier's own are in
    // `driver::through_proj`; every default session gets `None` here.
    let inlined;
    let select = match through_proj::inline(select, catalog, current_db, ctx) {
        Some(rewritten) => {
            inlined = rewritten;
            &inlined
        }
        None => select,
    };
    // Go's `rule_join_elimination`: an outer join whose null-producing side
    // nobody reads and which cannot duplicate an outer row is dropped, so the
    // inner table is never accessed at all. See
    // `driver::outer_join_elimination` for the half of Go's rule this
    // implements and for every shape it refuses.
    let unjoined;
    let select = match outer_join_elimination::eliminate(select, catalog, current_db) {
        Some(rewritten) => {
            unjoined = rewritten;
            &unjoined
        }
        None => select,
    };

    // Go's `buildSelect` pushes this block's `/*+ ... */` hints and its
    // deferred `popTableHints` reports the ones no `DataSource` of the block
    // claimed, as 1815. It runs whether or not there is a `FROM` -- a hint on
    // a `FROM`-less select names nothing and is reported too. Captured.
    crate::index_hints::report_comment_index_hints(select, catalog, current_db, ctx);
    // Resolve FROM: none -> table-dual; otherwise the (possibly joined) tables.
    let (mut from_source, mut scope): (Option<Box<dyn Executor>>, FromScope) = match &select.from {
        None => {
            if let Some(trace) = trace.as_deref_mut() {
                trace.table_dual();
            }
            (
                None,
                FromScope {
                    zone: ctx.session_zone(),
                    ..FromScope::default()
                },
            )
        }
        Some(join) => {
            // Go raises `ErrKeyDoesNotExist` (1176) from
            // `getPossibleAccessPaths`, once per `DataSource` and before any
            // path is costed -- so a `FORCE INDEX` naming an index no table
            // has fails the statement whether or not that table is the one
            // the access-path decision would have narrowed. Doing it here,
            // over the whole join tree, is what makes it independent of which
            // table that turns out to be.
            crate::index_hints::validate_join_index_hints(join, catalog, current_db)?;
            // Go's `rule_predicate_push_down`: the `WHERE` equalities are
            // offered to the joins below, so a comma join does not have to
            // build the cross product the filter would then throw away. See
            // `driver::predicate_push_down`.
            let offered = predicate_push_down::offered_conjuncts(select.where_clause.as_ref());
            // Go's `rule_column_pruning`: what every `DataSource` below still
            // has to produce, which is the input its access-path costing
            // needs (`isCoveringIndex`). A `FROM` of ONE base table is
            // deliberately excluded -- `commit_fast_path_source` below costs
            // that table's paths WITH its `WHERE`, and a second, condition-
            // blind choice here could only be the worse of the two.
            let wanted = access::single_kv_table(&select.from, catalog, current_db)
                .is_none()
                .then(|| leaf_demand::LeafDemand::of_select(select));
            let demand = leaf_demand::FromDemand {
                offered: &offered,
                columns: wanted.as_ref(),
            };
            // Go's `join_reorder` rule, which runs on the logical plan
            // between predicate pushdown and physical planning. It only ever
            // fires for a session that raised
            // `tidb_opt_join_reorder_threshold`; see
            // `driver::join_reorder`'s module doc for why that bounds it.
            let reordered =
                join_reorder::reorder(join, select.where_clause.as_ref(), catalog, current_db, ctx);
            let planned = reordered.as_ref().map_or(join, |plan| &plan.join);
            let (exec, mut scope) = build_join(
                planned,
                catalog,
                current_db,
                ctx,
                trace.as_deref_mut(),
                Some(select),
                demand,
                &tidb_planner::physical_property::PhysicalProperty::default(),
            )?;
            // Go's `restoreSchemaIfChanged`: the reordered join's schema is
            // the new leaf order, and the statement's output must stay the
            // written one. Go wraps a `Projection`; here the scope carries
            // the display order (`FromScope::star`), which is the same escape
            // hatch a `RIGHT JOIN` uses.
            if let Some(plan) = &reordered {
                restore_written_order(&mut scope, &plan.written_order);
            }
            (Some(exec), scope)
        }
    };

    // The access-path decision and the work handed down to it live in
    // `driver::access`; `index_order` is set when the committed source emits
    // rows in an index's order, which is what lets a `LIMIT` under a matching
    // `ORDER BY` stop the scan early.
    let index_order = commit_fast_path_source(
        select,
        catalog,
        current_db,
        &scope,
        &mut from_source,
        trace.as_deref_mut(),
        ctx,
    )?;
    // Go's `rule_partition_processor` runs after the access path is chosen
    // and BEFORE anything above the scan is built, which is exactly here: the
    // leaf is final (renamed or replaced by whichever path won) and nothing
    // sits over it yet. It only ever fires under `@@tidb_partition_prune_mode
    // = 'static'`; the shipped `dynamic` leaves the one scan alone.
    if ctx.static_partition_prune() {
        if let (Some(trace), Some(table)) = (
            trace.as_deref_mut(),
            crate::driver::access::sole_kv_table(&select.from, catalog, current_db),
        ) {
            trace.partition_union(&crate::driver::access::surviving_partition_names(
                select,
                crate::driver::access::sole_table_ref(&select.from),
                &table,
                &ctx.session_zone(),
            ));
        }
    }
    // Column pruning: over a single base-table scan the fast paths left
    // alone, narrow the scan -- and with it the scope -- to the columns the
    // statement actually reads.
    prune_scan_columns(select, &mut scope, &mut from_source);

    // The column resolver for this query's scope.
    let resolver = ScopeResolver { scope: &scope };

    // GROUPING() reads which grouping set produced a row, so it means nothing
    // without WITH ROLLUP: Go rejects it with ErrInvalidGroupFuncUse (1111),
    // whether or not the query groups at all.
    if !select.rollup && select_has_grouping(select) {
        return Err(DriverError::InvalidGroupFuncUse);
    }

    // A window function outside the select list / ORDER BY is Go's
    // ErrWindowInvalidWindowFuncUse (3593), whichever path runs below.
    crate::window::reject_windows_outside_select_list(select)?;

    // Aggregate path, Go `PlanBuilder.detectSelectAgg`: GROUP BY, or any
    // select field, HAVING or ORDER BY expression CONTAINING an aggregate
    // call -- not merely one that IS an aggregate call. `IF(1=1, COUNT(*), 0)`
    // is an aggregate query, and answering it on the non-aggregate path leaves
    // the aggregate node for the expression rewriter to refuse.
    let is_aggregate = !select.group_by.is_empty()
        || select.fields.fields().iter().any(|f| match f {
            SelectField::Expr { expr, .. } => expr.has_aggregate_flag(),
            SelectField::Wildcard { .. } => false,
        })
        || select
            .having
            .as_ref()
            .is_some_and(tidb_ast::Expr::has_aggregate_flag)
        || select
            .order_by
            .iter()
            .any(|item| item.expr.has_aggregate_flag());
    if is_aggregate {
        return run_aggregate_select(
            select,
            traced_select,
            from_source,
            &resolver,
            catalog,
            current_db,
            ctx,
            trace,
        );
    }

    // `SELECT DISTINCT ... ORDER BY`, for the queries that never reach the
    // aggregate pipeline. The aggregate path runs the same check itself, after
    // ONLY_FULL_GROUP_BY, which is the order Go's two builders impose.
    only_full_group_by::check_order_by_in_distinct(select, resolver.scope, ctx)?;

    // Source: the table rows (matrix- or TiKV-byte-backed), or one virtual row
    // from a table-dual.
    let (mut source, source_schema): (Box<dyn Executor>, Schema) = match from_source {
        Some(exec) => {
            let schema = exec.schema().clone();
            (exec, schema)
        }
        None => {
            let exec: Box<dyn Executor> = Box::new(TableDualExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 0, INIT_CAP, MAX_CHUNK_SIZE),
                1,
            ));
            let exec = match trace.as_deref_mut() {
                Some(trace) => trace.meter(exec),
                None => exec,
            };
            (exec, Schema::new(vec![]))
        }
    };
    // The plan text quotes the statement as written, against the FROM scope
    // the driver just built.
    let qualify = Qualifier {
        db: current_db,
        scope: &scope,
    };

    // Optional WHERE: a selection over the source rows. A correlated
    // subquery in the predicate first becomes an Apply below the selection,
    // appending the column the rewritten predicate reads (Go's plan shape).
    // The scope the rows above the WHERE have: the FROM tables, plus the
    // column a correlated WHERE subquery's Apply appends.
    let mut current_scope = scope.clone();
    // Predicate push-down: over a single base table, offer the source the
    // conjuncts it can apply itself; only the residual needs a `Selection`.
    let executed_where =
        negotiate_scan_filter(select, &scope, &mut source, ctx, trace.as_deref_mut());
    // LIMIT push-down: offer the source the row cap, when nothing between it
    // and the `LimitExec` can add, drop or reorder a row.
    offer_scan_limit(
        select,
        executed_where.as_ref(),
        index_order.as_ref(),
        &resolver,
        &mut source,
    );
    // `keep order`: whether the source's own walk order is the answer's, which
    // is what decides whether an index lookup reorders its handle batch.
    offer_keep_order(select, index_order.as_ref(), &resolver, &mut source);

    // A `WHERE` whose conjuncts all moved into the scan still records its
    // `Selection`, over the predicate as written, and meters the filtered
    // rows the scan now emits.
    if executed_where.is_none() && select.where_clause.is_some() {
        if let Some(trace) = trace.as_deref_mut() {
            if let Some(written) = &traced_select.where_clause {
                trace.selection(
                    written,
                    &qualify,
                    select_stats_selectivity(select, catalog, current_db, &scope),
                );
                source = trace.meter(source);
            }
        }
    }
    if let Some(predicate) = &executed_where {
        let mut correlated = None;
        let appended = scope.width();
        let predicate = extract_correlated_subquery(
            predicate,
            &scope,
            catalog,
            current_db,
            appended,
            &mut correlated,
            ctx,
        )?;
        let (predicate_resolver, predicate_scope);
        let mut source_schema = source_schema;
        if let Some(correlated) = correlated {
            // The Apply's schema is the source's columns plus the subquery's.
            let mut applied = scope.clone();
            let mut value_type = FieldType::new(FieldTypeCode::LongLong);
            if matches!(correlated.kind, SubqueryKind::Scalar) {
                value_type = subquery_result_type(&correlated, &scope, catalog, current_db, ctx)
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            }
            applied.tables.push(FromTable {
                name: String::new(),
                database: None,
                columns: vec![(format!("__apply_{appended}"), value_type)],
                offset: appended,
                func_deps: Default::default(),
            });
            let columns: Vec<Column> = applied
                .column_list()
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            let apply_schema = Schema::new(columns);
            let inner_scope = scope.clone();
            // The apply callback outlives this borrow of the catalog, so it
            // owns a snapshot (see ApplyExec::new).
            let inner_catalog = catalog.clone();
            let inner_db = current_db.to_owned();
            // The statement context is a handle, so the callback shares the
            // one warning buffer the statement reports.
            let inner_ctx = ctx.clone();
            let runner: crate::apply::InnerRunner = Box::new(move |values: &[Datum]| {
                run_correlated_subquery(
                    &correlated,
                    values,
                    &inner_scope,
                    &inner_catalog,
                    &inner_db,
                    &inner_ctx,
                )
                .map_err(|e| match e {
                    DriverError::Exec(exec) => exec,
                    DriverError::SubqueryReturnsMoreThanOneRow => {
                        ExecError::SubqueryReturnsMoreThanOneRow
                    }
                    other => ExecError::unsupported(driver_error_text(&other)),
                })
            });
            source = Box::new(crate::apply::ApplyExec::new(
                ExecutorMeta::new(apply_schema.clone(), 7, INIT_CAP, MAX_CHUNK_SIZE),
                source,
                runner,
                ctx.statement_memory(),
                // The outer side here is the statement's SOURCE, never an
                // aggregation, so Go's deselected-default-row case cannot
                // arise: there is no mismatch row to pad.
                None,
            ));
            source_schema = apply_schema;
            current_scope = applied;
            predicate_scope = current_scope.clone();
        } else {
            predicate_scope = scope.clone();
        }
        predicate_resolver = ScopeResolver {
            scope: &predicate_scope,
        };
        let mut pred = rewrite_expr_resolved(&predicate, &predicate_resolver)
            .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
        refine_comparisons(&mut pred, ctx);
        source = Box::new(SelectionExec::new(
            ExecutorMeta::new(source_schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
            vec![pred],
            source,
            ctx.clone(),
        ));
        if let Some(trace) = trace.as_deref_mut() {
            // An Apply below this selection (a correlated subquery in the
            // WHERE) adds an executor the recorder has never printed, so it
            // stays out of the trace rather than changing the shape EXPLAIN
            // reports.
            if let Some(written) = &traced_select.where_clause {
                trace.selection(
                    written,
                    &qualify,
                    select_stats_selectivity(select, catalog, current_db, &scope),
                );
                source = trace.meter(source);
            }
        }
    }

    // Window functions: the source rows are materialized here, each window
    // call is computed over them (see `crate::window`), and its values are
    // appended as one synthetic source column per call. Every `Expr::Window`
    // in the select list / ORDER BY is then rewritten to read that column, so
    // everything below -- projection, outer ORDER BY, DISTINCT, LIMIT -- runs
    // unchanged, and the outer ORDER BY sorts the already-computed values.
    let window_rewritten;
    let select = if crate::window::select_has_window(select) {
        let calls = crate::window::collect_window_calls(select)?;
        let source_types: Vec<FieldType> = current_scope
            .column_list()
            .into_iter()
            .map(|(_, field_type)| field_type)
            .collect();
        let rows = drain_executor_rows(source, &source_types)?;
        let (rows, scope_with_windows) =
            crate::window::compute_windows(&calls, rows, &current_scope, ctx)?;
        let columns: Vec<Column> = scope_with_windows
            .column_list()
            .iter()
            .enumerate()
            .map(|(i, (_, ft))| {
                let mut col = Column::new((i + 1) as i64, ft.clone());
                col.index = i as i64;
                col
            })
            .collect();
        source = Box::new(MemTableSourceExec::new(
            ExecutorMeta::new(Schema::new(columns), 0, INIT_CAP, MAX_CHUNK_SIZE),
            rows,
        ));
        current_scope = scope_with_windows;
        window_rewritten = crate::window::rewrite_windows(select, &calls);
        &window_rewritten
    } else {
        select
    };

    // A correlated subquery in the SELECT list becomes an Apply above the
    // WHERE's selection, appending the column the rewritten field reads --
    // the same shape the WHERE path builds, and Go's plan for
    // `handleScalarSubquery` when the subquery cannot be folded. It sits
    // ABOVE the filter, so the inner query runs only for the rows the WHERE
    // kept, as Go's plan does.
    let mut projected: Vec<(SelectField, Option<String>)> = Vec::new();
    for (field_index, field) in select.fields.fields().iter().enumerate() {
        let SelectField::Expr { expr, alias } = field else {
            projected.push((field.clone(), None));
            continue;
        };
        let name = alias
            .clone()
            .unwrap_or_else(|| default_field_display_name(&select.fields, field_index, expr));
        let mut correlated = None;
        let appended = current_scope.width();
        let rewritten = extract_correlated_subquery(
            expr,
            &current_scope,
            catalog,
            current_db,
            appended,
            &mut correlated,
            ctx,
        )?;
        if let Some(correlated) = correlated {
            let mut value_type = FieldType::new(FieldTypeCode::LongLong);
            if matches!(correlated.kind, SubqueryKind::Scalar) {
                value_type =
                    subquery_result_type(&correlated, &current_scope, catalog, current_db, ctx)
                        .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            }
            let inner_scope = current_scope.clone();
            current_scope.tables.push(FromTable {
                name: String::new(),
                database: None,
                columns: vec![(format!("__apply_{appended}"), value_type)],
                offset: appended,
                func_deps: Default::default(),
            });
            let columns: Vec<Column> = current_scope
                .column_list()
                .iter()
                .enumerate()
                .map(|(i, (_, ft))| {
                    let mut col = Column::new((i + 1) as i64, ft.clone());
                    col.index = i as i64;
                    col
                })
                .collect();
            let apply_schema = Schema::new(columns);
            // The callback outlives this borrow of the catalog, so it owns a
            // snapshot (see ApplyExec::new); the context is a handle, so the
            // inner query's warnings reach the statement's one buffer.
            let inner_catalog = catalog.clone();
            let inner_db = current_db.to_owned();
            let inner_ctx = ctx.clone();
            let runner: crate::apply::InnerRunner = Box::new(move |values: &[Datum]| {
                run_correlated_subquery(
                    &correlated,
                    values,
                    &inner_scope,
                    &inner_catalog,
                    &inner_db,
                    &inner_ctx,
                )
                .map_err(|e| match e {
                    DriverError::Exec(exec) => exec,
                    DriverError::SubqueryReturnsMoreThanOneRow => {
                        ExecError::SubqueryReturnsMoreThanOneRow
                    }
                    other => ExecError::unsupported(driver_error_text(&other)),
                })
            });
            source = Box::new(crate::apply::ApplyExec::new(
                ExecutorMeta::new(apply_schema, 7, INIT_CAP, MAX_CHUNK_SIZE),
                source,
                runner,
                ctx.statement_memory(),
                // The outer side here is the statement's SOURCE, never an
                // aggregation, so Go's deselected-default-row case cannot
                // arise: there is no mismatch row to pad.
                None,
            ));
        }
        projected.push((
            SelectField::Expr {
                expr: rewritten,
                alias: alias.clone(),
            },
            Some(name),
        ));
    }
    // HAVING, for every query the aggregate pipeline never sees. Go builds it
    // as a `LogicalSelection` ABOVE the select list's `Projection`, so it
    // filters projected rows and may name only what the projection outputs;
    // this tier evaluates the projection last, so the same filter is a
    // `SelectionExec` over source rows with each name replaced by the field
    // it names. See [`having::build_plain_having`] -- until it existed the
    // clause was silently DROPPED here.
    if let Some(having) = &select.having {
        source = having::build_plain_having(
            having,
            &projected,
            &scope,
            &mut current_scope,
            source,
            catalog,
            current_db,
            ctx,
        )?;
    }
    let projected_fields: Vec<SelectField> =
        projected.iter().map(|(field, _)| field.clone()).collect();
    let resolver = ScopeResolver {
        scope: &current_scope,
    };

    // Rewrite each projected field into an evaluable expression; `*` expands to
    // every table column in order (Go's unfoldWildStar).
    let mut exprs: Vec<Expression> = Vec::new();
    let mut names: Vec<String> = Vec::new();
    for (field, name) in &projected {
        match field {
            SelectField::Expr { expr, .. } => {
                let mut rewritten = rewrite_expr_resolved(expr, &resolver)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                refine_comparisons(&mut rewritten, ctx);
                exprs.push(rewritten);
                names.push(name.clone().unwrap_or_default());
            }
            SelectField::Wildcard(qualifier) => {
                if scope.tables.is_empty() {
                    return Err(DriverError::unsupported(
                        "`*` is not supported in a FROM-less SELECT",
                    ));
                }
                // `*` expands to every column of every FROM table in order,
                // `t.*` to one table's (Go's unfoldWildStar). A coalesced
                // join reorders the former and hides the duplicates from it;
                // `t.*` normally is untouched, so `u2.*` still reports `u2`'s
                // own copy of a `USING` column (captured from Go).
                //
                // The exception is Go's `fullSchema == nil` fallback, which
                // hides the redundant copy from `t.*` as well -- see
                // [`FromScope::qualified_star_is_output_only`] for the shape
                // that produces it and the captures.
                let hidden: &[usize] = if current_scope.qualified_star_is_output_only {
                    &current_scope.coalesced
                } else {
                    &[]
                };
                let selected: Vec<&FromTable> = match qualifier.last() {
                    None => {
                        for (index, name, ft) in scope.star_columns() {
                            let mut col = Column::new((index + 1) as i64, ft);
                            col.index = index as i64;
                            exprs.push(Expression::Column(col));
                            names.push(name);
                        }
                        continue;
                    }
                    Some(q) => {
                        let matching: Vec<&FromTable> = scope
                            .tables
                            .iter()
                            .filter(|t| t.name.eq_ignore_ascii_case(q))
                            .collect();
                        if matching.is_empty() {
                            return Err(DriverError::unsupported(
                                "`t.*` qualifier does not match a FROM table",
                            ));
                        }
                        matching
                    }
                };
                for table in selected {
                    for (i, (name, ft)) in table.columns.iter().enumerate() {
                        let index = table.offset + i;
                        if hidden.contains(&index) {
                            continue;
                        }
                        let mut col = Column::new((index + 1) as i64, ft.clone());
                        col.index = index as i64;
                        exprs.push(Expression::Column(col));
                        names.push(name.clone());
                    }
                }
            }
        }
    }

    // Output schema: one column per field, typed by the expression's static type.
    let out_columns: Vec<Column> = exprs
        .iter()
        .enumerate()
        .map(|(i, expr)| {
            let field_type = expr
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
            let mut col = Column::new((i + 1) as i64, field_type);
            col.index = i as i64;
            col
        })
        .collect();
    let out_schema = Schema::new(out_columns);
    let ret_types: Vec<FieldType> = out_schema
        .columns
        .iter()
        .map(|c| c.ret_type.clone().expect("output column has a type"))
        .collect();

    // ORDER BY: a sort below the projection, with by-items resolved against
    // the SELECT list first and the SOURCE schema second -- Go's own
    // resolution order, which is why ordering by a column that is not
    // projected still works while an alias shadows one that is.
    //
    // Whether the `LIMIT` below was already consumed by a fused `TopN`.
    let mut fused_topn = false;
    if !select.order_by.is_empty() {
        let mut by_items = Vec::with_capacity(select.order_by.len());
        for item in &select.order_by {
            let resolved = substitute_output_aliases(&item.expr, &projected_fields, true)?;
            let mut expr = rewrite_expr_resolved(&resolved, &resolver).map_err(|e| {
                order_by_column_error(&resolved).unwrap_or(DriverError::Exec(ExecError::Eval(e)))
            })?;
            refine_comparisons(&mut expr, ctx);
            by_items.push(SortByItem {
                expr,
                desc: item.desc,
            });
        }
        let sort_schema = source.schema().clone();
        // Go's `topn_push_down` rule fuses the `LIMIT` into the `Sort`: the
        // `LogicalLimit` becomes a by-item-less `LogicalTopN`, pushes through
        // the projection, and the `LogicalSort` hands it its by-items
        // (`logical_sort.go` `PushDownTopN`). The result is one bounded
        // operator where the projection then evaluates only the rows that
        // survived -- which is also why a projection expression that errors
        // on a discarded row no longer fails the statement, as in Go.
        //
        // `SELECT DISTINCT` is the one case that must NOT fuse: this tier's
        // dedup sits BETWEEN the sort and the limit, so a bounded sort would
        // drop rows before they were deduplicated. Go's own plan puts the
        // TopN ABOVE the aggregation (captured: `TopN_9 | root` over
        // `HashAgg_18`), a position this build order cannot express.
        let fused_limit = if select.distinct {
            None
        } else {
            select.limit.as_ref()
        };
        if let Some(limit) = fused_limit {
            let count = eval_limit_bound(&limit.count)?;
            let offset = match &limit.offset {
                Some(expr) => eval_limit_bound(expr)?,
                None => 0,
            };
            source = Box::new(TopNExec::new(
                ExecutorMeta::new(sort_schema, 3, INIT_CAP, MAX_CHUNK_SIZE),
                by_items,
                source,
                ctx.clone(),
                offset,
                count,
                ctx.statement_memory(),
            ));
            fused_topn = true;
            if let Some(trace) = trace.as_deref_mut() {
                trace.topn(&traced_select.order_by, &qualify, offset, count);
                source = trace.meter(source);
            }
        } else {
            source = Box::new(SortExec::new(
                ExecutorMeta::new(sort_schema, 3, INIT_CAP, MAX_CHUNK_SIZE),
                by_items,
                source,
                ctx.clone(),
                ctx.statement_memory(),
            ));
            if let Some(trace) = trace.as_deref_mut() {
                trace.sort(&traced_select.order_by, &qualify);
                source = trace.meter(source);
            }
        }
    }

    // Projection of the rewritten fields.
    let mut root: Box<dyn Executor> = Box::new(ProjectionExec::new(
        ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
        exprs,
        source,
        ctx.clone(),
    ));
    if let Some(trace) = trace.as_deref_mut() {
        trace.projection(traced_select.fields.fields(), &qualify);
        root = trace.meter(root);
    }

    // SELECT DISTINCT: Go `buildDistinct` builds an aggregation grouping by
    // every projected column, with a FIRST_ROW aggregate per column, which is
    // exactly a deduplication. It sits above the projection and below LIMIT.
    if select.distinct {
        let all: Vec<usize> = (0..out_schema.columns.len()).collect();
        root = Box::new(distinct_over(root, &out_schema, &all, ctx));
        if let Some(trace) = trace.as_deref_mut() {
            trace.distinct(traced_select.fields.fields(), &qualify);
            root = trace.meter(root);
        }
    }

    // LIMIT [offset,] count: both bounds must be non-negative integer literals
    // (as in SQL; Go validates the same in the planner). A fused `TopN`
    // already applied this window.
    if let Some(limit) = select.limit.as_ref().filter(|_| !fused_topn) {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        let limit_schema = root.schema().clone();
        root = Box::new(LimitExec::new(
            ExecutorMeta::new(limit_schema, 4, INIT_CAP, MAX_CHUNK_SIZE),
            offset,
            count,
            root,
        ));
        if let Some(trace) = trace.as_deref_mut() {
            trace.limit(offset, count);
            root = trace.meter(root);
        }
    }

    // Plain `EXPLAIN`: the pipeline is built and recorded, then dropped
    // undrained -- no row of the result is ever produced.
    if trace.as_deref().is_some_and(PlanTrace::is_plan_only) {
        return Ok((names.into_iter().zip(ret_types).collect(), Vec::new()));
    }

    root.open()?;
    let mut req = root.new_chunk();
    let mut rows: Vec<Vec<Datum>> = Vec::new();
    loop {
        root.next(&mut req)?;
        let n = req.num_rows();
        if n == 0 {
            break;
        }
        for r in 0..n {
            let row = req.get_row(r);
            let values = ret_types
                .iter()
                .enumerate()
                .map(|(c, ft)| row.get_datum(c, ft))
                .collect();
            rows.push(values);
        }
    }
    root.close()?;
    let columns = names.into_iter().zip(ret_types).collect();
    Ok((columns, rows))
}

/// Go turns a subquery's result `Datum` into an `expression.Constant`; the
/// same value has to travel back through the AST here, so it becomes the
/// literal that parses to it.
/// A byte string as a literal expression: readable text stays a string, and
/// anything that is not UTF-8 becomes a hex literal so no byte is lost.
pub(crate) fn bytes_to_literal(bytes: &[u8]) -> tidb_ast::Expr {
    match std::str::from_utf8(bytes) {
        Ok(text) => tidb_ast::Expr::String(text.to_owned()),
        Err(_) => tidb_ast::Expr::Hex(hex_digits(bytes)),
    }
}

/// The lowercase, even-length hex digits an `Expr::Hex` carries.
fn hex_digits(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn datum_to_literal(value: &Datum) -> Result<tidb_ast::Expr, DriverError> {
    use tidb_ast::Expr;
    Ok(match value {
        Datum::Null => Expr::Null,
        Datum::Int(v) => {
            // A negative literal is a unary minus over a positive one, which
            // is how the parser itself represents it.
            if *v < 0 {
                Expr::Unary(
                    tidb_ast::UnaryOp::Minus,
                    Box::new(Expr::Int(v.unsigned_abs().to_string())),
                )
            } else {
                Expr::Int(v.to_string())
            }
        }
        Datum::UInt(v) => Expr::Int(v.to_string()),
        Datum::Real(v) => Expr::Float(*v),
        Datum::Decimal(d) => Expr::Decimal(d.to_string()),
        // A byte string that is not UTF-8 becomes a hex literal, which is
        // lossless where a lossy string conversion would corrupt it.
        Datum::String(s) => bytes_to_literal(s.bytes()),
        Datum::Bytes(b) => bytes_to_literal(b),
        Datum::BinaryLiteral(literal) | Datum::Bit(literal) => {
            Expr::Hex(hex_digits(literal.as_bytes()))
        }
        _ => {
            return Err(DriverError::unsupported(
                "this subquery result kind is not supported yet",
            ))
        }
    })
}

/// One table in a query's `FROM`: the name a qualifier must match (its alias
/// when it has one, as in Go's `TableSource`), its columns, and the offset of
/// its first column in the joined row.
#[derive(Clone, Debug)]
pub(crate) struct FromTable {
    pub(crate) name: String,
    /// The schema the table lives in, when a `db.t.column` reference may name
    /// it. `None` for a source that cannot be schema-qualified: an aliased
    /// table (MySQL's alias replaces the whole path) or a synthetic scope.
    pub(crate) database: Option<String>,
    pub(crate) columns: Vec<(String, FieldType)>,
    pub(crate) offset: usize,
    /// Go `DataSource.ExtractFD`'s contribution for this source: the keys in
    /// both strengths and the generated columns' dependencies, as offsets
    /// local to the source: the primary key and each UNIQUE index, split by
    /// whether every member is `NOT NULL`, plus the generated columns'
    /// dependencies. A derived table, a view or a synthetic scope has none.
    pub(crate) func_deps: funcdep::TableFuncDeps,
}

/// Opens `exec`, drains every row as datums of `types`, and closes it.
fn drain_executor_rows(
    mut exec: Box<dyn Executor>,
    types: &[FieldType],
) -> Result<Vec<Vec<Datum>>, DriverError> {
    exec.open()?;
    let mut rows = Vec::new();
    let mut req = exec.new_chunk();
    loop {
        exec.next(&mut req)?;
        let n = req.num_rows();
        if n == 0 {
            break;
        }
        for r in 0..n {
            let row = req.get_row(r);
            rows.push(
                types
                    .iter()
                    .enumerate()
                    .map(|(c, ft)| row.get_datum(c, ft))
                    .collect(),
            );
        }
    }
    exec.close()?;
    Ok(rows)
}

/// Go `buildDistinct(child, length)`: an aggregation grouping by the columns
/// of `schema` at `key_indices`, carrying EVERY column of `schema` through a
/// `FIRST_ROW` aggregate.
///
/// The two sets differ where Go's projection appended `ORDER BY` carriers
/// past the select list's own `oldLen` columns: those ride through the dedup
/// so the sort above can still read them, but they do not take part in the
/// grouping (`logical_plan_builder.go:1973-1990`).
///
/// The hash aggregation emits groups in first-seen order, so a sort below it
/// still orders the deduplicated rows -- the first row of each group is the
/// one the sort put first.
fn distinct_over(
    child: Box<dyn Executor>,
    schema: &Schema,
    key_indices: &[usize],
    ctx: &crate::StmtContext,
) -> HashAggExec<crate::StmtContext> {
    let group_by: Vec<Expression> = key_indices
        .iter()
        .map(|index| Expression::Column(schema.columns[*index].clone()))
        .collect();
    let agg_funcs: Vec<AggFunc> = schema
        .columns
        .iter()
        .map(|column| AggFunc::new(AggKind::FirstRow, Some(Expression::Column(column.clone()))))
        .collect();
    HashAggExec::new(
        ExecutorMeta::new(schema.clone(), 5, INIT_CAP, MAX_CHUNK_SIZE),
        group_by,
        agg_funcs,
        child,
        ctx.clone(),
        ctx.statement_memory(),
    )
}

/// Evaluates a `LIMIT` bound, which must be a non-negative integer literal.
pub(crate) fn eval_limit_bound(expr: &tidb_ast::Expr) -> Result<u64, DriverError> {
    match expr {
        tidb_ast::Expr::Int(text) => text
            .parse::<u64>()
            .map_err(|_| DriverError::unsupported("LIMIT bound must be a non-negative integer")),
        _ => Err(DriverError::unsupported(
            "LIMIT bound must be an integer literal",
        )),
    }
}

#[cfg(test)]
mod field_name_tests {
    use super::{run_select_meta_on, strip_spec_field_comment_markers, Catalog};

    /// Go `SpecFieldPattern` + `TrimComment`: every closing `*/` and opening
    /// `/*!<version>` marker is removed, and a `/*+ ...` hint opener is kept.
    #[test]
    fn strip_spec_field_comment_markers_matches_go_spec_field_pattern() {
        // The optimizer-hint case the recorded corpus needs: the `*/` goes,
        // the `/*+` stays, and the two surrounding spaces remain.
        assert_eq!(
            strip_spec_field_comment_markers("(select /*+ INL_JOIN(x2) */ x2.a from t)"),
            "(select /*+ INL_JOIN(x2)  x2.a from t)"
        );
        // A MySQL version comment `/*!40101 ... */` loses both markers.
        assert_eq!(
            strip_spec_field_comment_markers("a /*!40101 + 1 */"),
            "a  + 1 "
        );
        // A bare `/*!` with no version still goes.
        assert_eq!(strip_spec_field_comment_markers("/*! x */"), " x ");
        // Text with neither marker is untouched, multibyte included.
        assert_eq!(strip_spec_field_comment_markers("列 + 1"), "列 + 1");
    }

    /// Go `buildProjectionFieldNameFromExpressions`: a bare NULL literal is
    /// named `NULL`, whatever case the source text used.
    #[test]
    fn a_null_literal_column_is_named_uppercase_null() {
        let ctx = crate::StmtContext::for_query();
        let (columns, _) = run_select_meta_on("select null", &Catalog::default(), &ctx).unwrap();
        assert_eq!(columns[0].0, "NULL");
        // A parenthesized / unary-plus NULL reaches the same rule.
        let (columns, _) = run_select_meta_on("select (null)", &Catalog::default(), &ctx).unwrap();
        assert_eq!(columns[0].0, "NULL");
    }
}
