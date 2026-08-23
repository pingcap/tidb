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
use crate::hash_agg::{AggFunc, AggKind, HashAggExec, StreamAggExec};
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
use crate::remote_scan::{PushdownPartialAggregate, PushdownTopN, PushdownTopNOrder};
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
///
/// # The literal switch
///
/// Go's literal handling is a switch on the `driver.ValueExpr`'s DATUM KIND,
/// not on its source text, and every arm below is that switch:
///
/// * `KindString` names the column by the literal's VALUE, not its text, with
///   leading non-graphic characters trimmed (`mysql.RangeGraph`), so
///   `select '\t   col'` is named `col` and `select ('\N')` is named `N`.
/// * `KindNull` is named `NULL`, whatever case the source used.
/// * `KindBinaryLiteral` (a `0x`/`b''` literal) keeps its source text.
/// * `KindInt64` carrying `IsBooleanFlag` -- a `TRUE`/`FALSE` keyword -- is
///   named `TRUE` or `FALSE` by its VALUE, so `select false` is `FALSE`.
/// * adjacent string literals use the decoded value of the first token, via
///   [`SelectFieldList::projection_offset`].
/// * every other literal keeps its source text with `\t\n +(` trimmed from
///   the left and `\t\n )` from the right, so `select +1` is named `1`.
pub fn default_field_display_name(
    fields: &SelectFieldList,
    index: usize,
    expr: &tidb_ast::Expr,
) -> String {
    // Go `getInnerFromParenthesesAndUnaryPlus`: parentheses and a unary `+`
    // are looked through before anything else is asked, because Go asks its
    // questions of the REWRITTEN expression and the rewriter drops both --
    // `(a)` rewrites to `a`, and `unaryOpToExpression`'s `opcode.Plus` arm
    // returns without touching the stack ("expression (+ a) is equal to a").
    // So `select (a)` and `select +a` are named `a`, like `select a`.
    let inner = inner_field_expr(expr);
    if let tidb_ast::Expr::Column(path) = inner {
        return path.last().cloned().unwrap_or_default();
    }
    let text = || {
        fields
            .text(index)
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .map_or_else(|| expr.restore(), str::to_owned)
    };
    // Go: `NAME_CONST` names the column by its FIRST argument's value, which
    // MySQL documents as the function's whole purpose. Go evaluates that
    // argument with `evalAstExpr`; `preprocess.go` has already refused every
    // call whose first argument is not a literal, so a literal is the only
    // shape that reaches the name.
    if let tidb_ast::Expr::Func { name, args, .. } = inner {
        if name.eq_ignore_ascii_case("name_const") && args.len() == 2 {
            if let Some(label) = literal_label_value(&args[0]) {
                return label;
            }
        }
    }
    // Go asks `field.Expr` -- the PARSED node -- whether this field is a
    // literal, never the rewritten expression: its test is
    // `innerExpr.(*driver.ValueExpr)`, and a `driver.ValueExpr` exists only
    // where the SOURCE wrote a literal. This tier's `expr` has been through
    // passes that substitute literals INTO the tree -- variable binding turns
    // `@@warning_count` into its value, subquery folding turns `(select 1)`
    // into `1` -- so `is_value_literal(inner)` alone would name those columns
    // `0` and `select 1` instead of `@@warning_count` and `(select 1)`. Both
    // were measured as regressions before
    // `SelectFieldList::written_literal` recorded the parse-time answer.
    if fields.written_literal(index) && is_value_literal(inner) {
        return literal_field_display_name(fields, index, inner, &text());
    }
    // Non-literal: named by its source text with MySQL special-result-field
    // comment markers removed -- Go's
    // `SpecFieldPattern.ReplaceAllStringFunc(field.Text(), TrimComment)`, which
    // drops every `*/` and `/*!<version>` marker. A `/*+ hint */` therefore
    // keeps `/*+ hint ` in the label because only the closing `*/` matches.
    strip_spec_field_comment_markers(&text())
}

/// Go `buildProjectionFieldNameFromExpressions`'s literal switch, over the
/// literal `expr` and the field's own source `text`.
/// Wraps a rewrite failure with the resolving clause's name: an
/// [`EvalError::UnknownColumn`] becomes Go's `ErrBadField` shape
/// (`Unknown column '<name>' in '<clause>'`, `clauseMsg`), everything else
/// keeps its own diagnostic.
fn eval_error_in_clause(error: tidb_expr::EvalError, clause: &'static str) -> DriverError {
    match error {
        tidb_expr::EvalError::UnknownColumn(column) => DriverError::UnknownColumnInClause {
            column,
            clause: clause.to_owned(),
        },
        other => DriverError::Exec(ExecError::Eval(other)),
    }
}

fn literal_field_display_name(
    fields: &SelectFieldList,
    index: usize,
    expr: &tidb_ast::Expr,
    text: &str,
) -> String {
    match expr {
        // `types.KindString`: the VALUE names the column, with leading
        // non-graphic characters trimmed.
        tidb_ast::Expr::String(value) | tidb_ast::Expr::RawString(value) => {
            let value = match fields.projection_offset(index) {
                Some(offset) => value
                    .get(..offset)
                    .expect("parser projection offset is a decoded string boundary"),
                None => value,
            };
            trim_leading_non_graphic(value).to_owned()
        }
        // `types.KindNull`.
        tidb_ast::Expr::Null => "NULL".to_owned(),
        // `types.KindBinaryLiteral`: "Don't rewrite BIT literal or HEX
        // literals" -- the source text is kept exactly, untrimmed.
        tidb_ast::Expr::Hex(_) | tidb_ast::Expr::Bit(_) => text.to_owned(),
        // `types.KindInt64` carrying `mysql.IsBooleanFlag`: the `TRUE` and
        // `FALSE` keywords are int64 literals whose flag says they were
        // written as booleans, and they are named by that value rather than
        // by the text (so `select FaLsE` is named `FALSE`).
        tidb_ast::Expr::Bool(value) => {
            if *value {
                "TRUE".to_owned()
            } else {
                "FALSE".to_owned()
            }
        }
        // The `default` arm: every remaining numeric literal keeps its source
        // text with the unary-plus/parenthesis wrapper trimmed off both ends.
        _ => text
            .trim_start_matches(['\t', '\n', ' ', '+', '('])
            .trim_end_matches(['\t', '\n', ' ', ')'])
            .to_owned(),
    }
}

/// The value a literal argument contributes as a column label -- Go's
/// `evalAstExpr(...)` followed by `Datum.ToString()`, reached only from
/// `NAME_CONST`'s first argument, which `preprocess.go` guarantees is a
/// literal.
fn literal_label_value(expr: &tidb_ast::Expr) -> Option<String> {
    match expr {
        tidb_ast::Expr::String(value) | tidb_ast::Expr::RawString(value) => Some(value.clone()),
        tidb_ast::Expr::Int(text) | tidb_ast::Expr::Decimal(text) => Some(text.clone()),
        _ => None,
    }
}

/// Go `strings.TrimLeftFunc(projName, func(r rune) bool { return
/// !unicode.IsOneOf(mysql.RangeGraph, r) })`: drops leading characters that
/// are not "graphic" in MySQL's sense.
///
/// `tidb-mysql` owns the exact source category tables, so this does not depend
/// on the Unicode version bundled with Rust.
fn trim_leading_non_graphic(value: &str) -> &str {
    value.trim_start_matches(|c: char| !tidb_mysql::is_range_graph(c))
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

pub mod access;
mod agg_build;
mod agg_predicate_pushdown;
mod agg_select;
mod catalog;
mod clause_resolve;
mod correlated_agg_decorrelate;
mod decorrelate_exists;
mod derived_agg_pruning;
pub(crate) use derived_agg_pruning::has_pruned_row_count;
mod derived_projection_pushdown;
mod dml;
mod errors;
mod from;
pub(crate) mod funcdep;
mod grouping;
mod having;
pub(crate) mod index_join_decision;
pub mod infoschema_meta;
pub(crate) mod join_key_cast;
pub(crate) mod join_method_hints;
pub(crate) mod join_reorder;
pub(crate) mod join_search;
pub(crate) mod leaf_access;
pub(crate) mod leaf_demand;
pub mod legacy_stats;
pub(crate) mod merge_decision;
mod multi_dml;
mod only_full_group_by;
mod outer_join_elimination;
pub(crate) mod outer_join_simplify;
mod params;
pub(crate) mod point_get_key;
mod predicate_push_down;
mod recursive_cte;
mod set_opr;
mod subquery;
#[cfg(test)]
mod tests;
mod through_proj;
pub(crate) mod write_cast;

// Re-exported flat, so every caller inside and outside this module keeps
// naming these as `driver::…` exactly as before the split.
pub(crate) use access::*;
pub(crate) use agg_build::*;
pub(crate) use agg_select::*;
pub use catalog::*;
pub(crate) use clause_resolve::*;
pub use dml::*;
pub use dml::run_fast_prepared_update;
pub(crate) use from::*;
pub(crate) use grouping::*;
pub use params::*;
pub use set_opr::*;
pub(crate) use subquery::*;
pub(crate) use write_cast::*;

pub use errors::{DriverError, MysqlError, SchemaErrorKind, TxnErrorKind, VarErrorKind};

const INIT_CAP: usize = 1;
const MAX_CHUNK_SIZE: usize = 1024;

fn limit_init_cap(count: u64) -> usize {
    usize::try_from(count)
        .unwrap_or(usize::MAX)
        .min(MAX_CHUNK_SIZE)
}

#[cfg(test)]
mod limit_chunk_capacity_tests {
    use super::{limit_init_cap, MAX_CHUNK_SIZE};

    #[test]
    fn limit_initial_capacity_is_count_capped_at_max_chunk_size() {
        assert_eq!(limit_init_cap(0), 0);
        assert_eq!(limit_init_cap(7), 7);
        assert_eq!(limit_init_cap(u64::MAX), MAX_CHUNK_SIZE);
    }
}

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
        let table = recursive_cte::materialize_cte_body(
            &cte.name,
            &cte.columns,
            &cte.query,
            &scratch,
            current_db,
            ctx,
            with.recursive,
        )?;
        scratch.register_cte_in(current_db, &cte.name, table);
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

pub(crate) fn append_correlated_apply(
    source: Box<dyn Executor>,
    outer_scope: &FromScope,
    correlated: CorrelatedSubquery,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<(Box<dyn Executor>, FromScope, Schema), DriverError> {
    let appended = outer_scope.width();
    let value_type = if matches!(correlated.kind, SubqueryKind::Scalar) {
        subquery_result_type(&correlated, outer_scope, catalog, current_db, ctx)
            .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
    } else {
        FieldType::new(FieldTypeCode::LongLong)
    };
    let mut applied = outer_scope.clone();
    applied.tables.push(FromTable {
        name: String::new(),
        database: None,
        columns: vec![(format!("__apply_{appended}"), value_type)],
        offset: appended,
        func_deps: Default::default(),
        physical: None,
    });
    let columns = applied
        .column_list()
        .iter()
        .enumerate()
        .map(|(index, (_, field_type))| {
            let mut column = Column::new((index + 1) as i64, field_type.clone());
            column.index = index as i64;
            column
        })
        .collect();
    let schema = Schema::new(columns);
    let cache_columns = correlated_column_indices(&correlated, outer_scope)?;
    let inner_scope = outer_scope.clone();
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
        .map_err(|error| match error {
            DriverError::Exec(exec) => exec,
            DriverError::SubqueryReturnsMoreThanOneRow => ExecError::SubqueryReturnsMoreThanOneRow,
            other => ExecError::unsupported(driver_error_text(&other)),
        })
    });
    let source = Box::new(
        crate::apply::ApplyExec::new(
            ExecutorMeta::new(schema.clone(), 7, INIT_CAP, MAX_CHUNK_SIZE),
            source,
            runner,
            ctx.statement_memory(),
            None,
        )
        .with_cache(
            ctx.apply_cache_capacity(),
            cache_columns,
            ctx.session_zone(),
        ),
    );
    Ok((source, applied, schema))
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

/// Plans one parsed `SELECT` and returns only its result-column metadata.
///
/// The ordinary planner still builds the exact executor pipeline, including
/// access-path selection and output type derivation, but a plan-only trace
/// stops before the pipeline is opened or drained.
pub fn plan_select_meta_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Vec<(String, FieldType)>, DriverError> {
    let mut trace = PlanTrace::planning();
    let (columns, _) = run_select_traced(
        select,
        catalog,
        current_db,
        ctx,
        Some(&mut trace),
        &tidb_planner::physical_property::PhysicalProperty::default(),
        false,
    )?;
    Ok(columns)
}

/// The read-free decision shared by SQL execution and EXPLAIN for the narrow
/// clustered-handle point path.
pub(crate) struct FastPointGetPlan {
    pub(crate) visible: String,
    pub(crate) table: KvTable,
    pub(crate) handle: Option<TableHandle>,
    output_offsets: Vec<usize>,
    output_columns: Vec<(String, FieldType)>,
}

pub(crate) fn plan_fast_point_get(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<FastPointGetPlan>, DriverError> {
    if select.with.is_some()
        || select.distinct
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || !select.windows.is_empty()
        || select.limit.is_some()
        || select.lock.is_some()
        || select.into_outfile.is_some()
        || select.calc_found_rows
    {
        return Ok(None);
    }
    let Some(table_ref) = access::single_table_ref(&select.from) else {
        return Ok(None);
    };
    let Some(table) = access::single_kv_table(&select.from, catalog, current_db) else {
        return Ok(None);
    };
    let hints = crate::index_hints::single_table_scan_hints(
        select,
        Some(table_ref),
        &table,
        current_db,
        ctx,
    )?;
    if !hints.allows_table() || !hints.allows_common_primary() {
        return Ok(None);
    }
    let visible = table_ref
        .alias
        .clone()
        .unwrap_or_else(|| table.name.clone());
    let columns = table.visible_columns();
    let point = access::try_point_get(
        &access::PointPlanStmt::of_select(select),
        &table,
        &columns
            .iter()
            .map(|column| (column.name.clone(), column.field_type.clone()))
            .collect::<Vec<_>>(),
        &ctx.session_zone(),
    )?;
    let Some(handle) = point else {
        return Ok(None);
    };
    let mut output_offsets = Vec::new();
    let mut output_columns = Vec::new();
    for field in select.fields.fields() {
        match field {
            tidb_ast::SelectField::Wildcard(_) => {
                for (offset, column) in columns.iter().enumerate() {
                    output_offsets.push(offset);
                    output_columns.push((column.name.clone(), column.field_type.clone()));
                }
            }
            tidb_ast::SelectField::Expr { expr, alias } => {
                let tidb_ast::Expr::Column(path) = expr else {
                    return Ok(None);
                };
                let Some(name) = path.last() else {
                    return Ok(None);
                };
                let Some((offset, column)) = columns
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name.eq_ignore_ascii_case(name))
                else {
                    return Ok(None);
                };
                output_offsets.push(offset);
                output_columns.push((
                    alias.clone().unwrap_or_else(|| column.name.clone()),
                    column.field_type.clone(),
                ));
            }
        }
    }
    if output_offsets.is_empty() {
        return Ok(None);
    }

    Ok(Some(FastPointGetPlan {
        visible,
        table,
        handle: handle.handle,
        output_offsets,
        output_columns,
    }))
}

/// Executes the narrow point-read shape without rebuilding the general
/// logical/physical plan.  Go's prepared point-get executor keeps this small
/// path on the connection and only rebinds the handle for each EXECUTE; the
/// ordinary Rust driver used to parse, optimize, and construct the complete
/// executor tree for every YCSB read.
///
/// This is deliberately a conservative fast path.  It admits only a single
/// clustered-handle equality, a projection made solely of base columns, and
/// no clause whose semantics need an executor stage.  Anything outside that
/// shape returns `Ok(None)` and remains on the general planner path.
pub fn run_fast_point_get(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<SelectMeta>, DriverError> {
    let Some(plan) = plan_fast_point_get(select, catalog, current_db, ctx)? else {
        return Ok(None);
    };
    let FastPointGetPlan {
        mut table,
        handle,
        output_offsets,
        output_columns,
        ..
    } = plan;
    let Some(handle) = handle else {
        return Ok(Some((output_columns, Vec::new())));
    };
    let row = table
        .get_row_by_handle(&handle, &ctx.session_zone())
        .map_err(|error| DriverError::Parse(format!("row decode failed: {error:?}")))?;
    let rows = row
        .map(|row| {
            output_offsets
                .into_iter()
                .map(|offset| row.get(offset).cloned().unwrap_or(Datum::Null))
                .collect::<Vec<_>>()
        })
        .into_iter()
        .collect();
    Ok(Some((output_columns, rows)))
}

/// Executes the prepared clustered-handle point-read shape using the retained
/// AST and execute values directly.  This is the binary-protocol equivalent of
/// [`run_fast_point_get`]; unlike the literal entry point it recognizes a
/// `?` marker and therefore avoids cloning the complete prepared AST merely to
/// install one key value.
pub fn run_fast_prepared_point_get(
    select: &tidb_ast::SelectStmt,
    params: &[Datum],
    catalog: &mut Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<SelectMeta>, DriverError> {
    if select.with.is_some()
        || select.distinct
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || !select.windows.is_empty()
        || select.limit.is_some()
        || select.lock.is_some()
        || select.into_outfile.is_some()
        || select.calc_found_rows
    {
        return Ok(None);
    }
    let Some(table_ref) = access::single_table_ref(&select.from) else {
        return Ok(None);
    };
    let (database, table_name) = match table_ref.name.as_slice() {
        [name] if !current_db.is_empty() => (current_db, name.as_str()),
        [database, name] => (database.as_str(), name.as_str()),
        _ => return Ok(None),
    };
    let Some(TableEntry::Kv(table)) = catalog.get_mut_in(database, table_name) else {
        return Ok(None);
    };
    let columns = table.visible_columns();
    let point = access::try_prepared_common_handle_point_get_path(
        select,
        table,
        params,
        &ctx.session_zone(),
    )?;
    let Some(handle) = point else {
        return Ok(None);
    };

    let mut output_offsets = Vec::new();
    let mut output_columns = Vec::new();
    for field in select.fields.fields() {
        match field {
            tidb_ast::SelectField::Wildcard(_) => {
                for (offset, column) in columns.iter().enumerate() {
                    output_offsets.push(offset);
                    output_columns.push((column.name.clone(), column.field_type.clone()));
                }
            }
            tidb_ast::SelectField::Expr { expr, alias } => {
                let tidb_ast::Expr::Column(path) = expr else {
                    return Ok(None);
                };
                let Some(name) = path.last() else {
                    return Ok(None);
                };
                let Some((offset, column)) = columns
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name.eq_ignore_ascii_case(name))
                else {
                    return Ok(None);
                };
                output_offsets.push(offset);
                output_columns.push((
                    alias.clone().unwrap_or_else(|| column.name.clone()),
                    column.field_type.clone(),
                ));
            }
        }
    }
    if output_offsets.is_empty() {
        return Ok(None);
    }
    let row = table
        .get_row_by_handle(&handle, &ctx.session_zone())
        .map_err(|error| DriverError::Parse(format!("row decode failed: {error:?}")))?;
    let rows = row
        .map(|row| {
            output_offsets
                .into_iter()
                .map(|offset| row.get(offset).cloned().unwrap_or(Datum::Null))
                .collect::<Vec<_>>()
        })
        .into_iter()
        .collect();
    Ok(Some((output_columns, rows)))
}

/// Executes the same prepared clustered-handle point shape with the narrow
/// decode context used by the prepared point-get cache.  The binary protocol
/// can reach this path before a complete planner context is needed; keeping
/// it narrow avoids rebuilding session-wide metadata (notably
/// `TIDB_DECODE_KEY`) for every YCSB execute.
pub fn run_fast_prepared_point_get_with_decode_context(
    select: &tidb_ast::SelectStmt,
    params: &[Datum],
    catalog: &mut Catalog,
    current_db: &str,
    context: &crate::kv_table::PreparedPointGetDecodeContext,
) -> Result<Option<SelectMeta>, DriverError> {
    if select.with.is_some()
        || select.distinct
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || !select.windows.is_empty()
        || select.limit.is_some()
        || select.lock.is_some()
        || select.into_outfile.is_some()
        || select.calc_found_rows
    {
        return Ok(None);
    }
    let Some(table_ref) = access::single_table_ref(&select.from) else {
        return Ok(None);
    };
    let (database, table_name) = match table_ref.name.as_slice() {
        [name] if !current_db.is_empty() => (current_db, name.as_str()),
        [database, name] => (database.as_str(), name.as_str()),
        _ => return Ok(None),
    };
    let Some(TableEntry::Kv(table)) = catalog.get_mut_in(database, table_name) else {
        return Ok(None);
    };
    let columns = table.visible_columns();
    let Some(handle) = access::try_prepared_common_handle_point_get_path(
        select,
        table,
        params,
        context.zone(),
    )?
    else {
        return Ok(None);
    };

    let mut output_offsets = Vec::new();
    let mut output_columns = Vec::new();
    for field in select.fields.fields() {
        match field {
            tidb_ast::SelectField::Wildcard(_) => {
                for (offset, column) in columns.iter().enumerate() {
                    output_offsets.push(offset);
                    output_columns.push((column.name.clone(), column.field_type.clone()));
                }
            }
            tidb_ast::SelectField::Expr { expr, alias } => {
                let tidb_ast::Expr::Column(path) = expr else {
                    return Ok(None);
                };
                let Some(name) = path.last() else {
                    return Ok(None);
                };
                let Some((offset, column)) = columns
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name.eq_ignore_ascii_case(name))
                else {
                    return Ok(None);
                };
                output_offsets.push(offset);
                output_columns.push((
                    alias.clone().unwrap_or_else(|| column.name.clone()),
                    column.field_type.clone(),
                ));
            }
        }
    }
    if output_offsets.is_empty() {
        return Ok(None);
    }
    let decoder = crate::kv_table::PreparedPointGetRowDecoder::new_with_handles(
        columns,
        table.pk_handle_offset(),
        table.common_handle_offsets(),
        &output_offsets,
    )
    .map_err(|error| DriverError::Parse(format!("point row decoder failed: {error:?}")))?;
    let row = table
        .get_prepared_point_row(&handle, &decoder, context)
        .map_err(|error| DriverError::Parse(format!("row decode failed: {error:?}")))?;
    let rows = row.into_iter().collect();
    Ok(Some((output_columns, rows)))
}

/// The read-free decision shared by SQL execution and EXPLAIN for YCSB E's
/// one-row clustered-handle range.
pub(crate) struct FastSingleRowScanPlan {
    pub(crate) visible: String,
    pub(crate) ranges: Vec<IndexRange>,
    pub(crate) pseudo: bool,
    table: KvTable,
    output_offsets: Vec<usize>,
    output_columns: Vec<(String, FieldType)>,
}

pub(crate) fn plan_fast_single_row_scan(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<FastSingleRowScanPlan>, DriverError> {
    let Some(limit) = select.limit.as_ref() else {
        return Ok(None);
    };
    if limit.offset.is_some()
        || !matches!(&limit.count, tidb_ast::Expr::Int(value) if value == "1")
        || select.with.is_some()
        || select.distinct
        || !select.group_by.is_empty()
        || select.having.is_some()
        || !select.order_by.is_empty()
        || !select.windows.is_empty()
        || select.lock.is_some()
        || select.into_outfile.is_some()
        || select.calc_found_rows
    {
        return Ok(None);
    }
    let Some(where_clause) = select.where_clause.as_ref() else {
        return Ok(None);
    };
    let Some(table_ref) = access::single_table_ref(&select.from) else {
        return Ok(None);
    };
    let Some(table) = access::single_kv_table(&select.from, catalog, current_db) else {
        return Ok(None);
    };
    let hints = crate::index_hints::single_table_scan_hints(
        select,
        Some(table_ref),
        &table,
        current_db,
        ctx,
    )?;
    if !hints.allows_table() || !hints.allows_common_primary() {
        return Ok(None);
    }
    let visible = table_ref
        .alias
        .clone()
        .unwrap_or_else(|| table.name.clone());
    if table.common_handle_offsets().len() != 1
        || table.partition().is_some()
        || table.has_dirty_content()
    {
        return Ok(None);
    }
    let built = crate::handle_range::build_handle_ranges(&table, where_clause, &ctx.session_zone())
        .ok_or_else(|| DriverError::unsupported("the WHERE is not a clustered-handle range"))?;
    if !built.residual.is_empty() || built.ranges.is_empty() {
        return Ok(None);
    }

    let columns = table.visible_columns();
    let mut output_offsets = Vec::new();
    let mut output_columns = Vec::new();
    for field in select.fields.fields() {
        match field {
            tidb_ast::SelectField::Wildcard(_) => {
                for (offset, column) in columns.iter().enumerate() {
                    output_offsets.push(offset);
                    output_columns.push((column.name.clone(), column.field_type.clone()));
                }
            }
            tidb_ast::SelectField::Expr { expr, alias } => {
                let tidb_ast::Expr::Column(path) = expr else {
                    return Ok(None);
                };
                let Some(name) = path.last() else {
                    return Ok(None);
                };
                let Some((offset, column)) = columns
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name.eq_ignore_ascii_case(name))
                else {
                    return Ok(None);
                };
                output_offsets.push(offset);
                output_columns.push((
                    alias.clone().unwrap_or_else(|| column.name.clone()),
                    column.field_type.clone(),
                ));
            }
        }
    }
    if output_offsets.is_empty() {
        return Ok(None);
    }

    let pseudo = catalog
        .table_statistics(table.table_id)
        .is_none_or(|statistics| statistics.pseudo);
    Ok(Some(FastSingleRowScanPlan {
        visible,
        ranges: built.ranges,
        pseudo,
        table,
        output_offsets,
        output_columns,
    }))
}

/// Executes the bounded clustered-key range shape used by YCSB workload E
/// without constructing a full table-reader/coprocessor DAG for every
/// prepared execute. The range builder remains the source of truth for key
/// semantics; this path only changes how the already-proven first row is
/// fetched. Any residual predicate, secondary index, staged write, or wider
/// limit falls back to the general planner.
pub fn run_fast_single_row_scan(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<Option<SelectMeta>, DriverError> {
    let Some(plan) = plan_fast_single_row_scan(select, catalog, current_db, ctx)? else {
        return Ok(None);
    };
    let FastSingleRowScanPlan {
        mut table,
        ranges,
        output_offsets,
        output_columns,
        ..
    } = plan;
    // The proven one-row shape already carries the complete clustered-key
    // range. Use the storage seam's bounded max-ts primitive directly: a DAG
    // cop request would create a transport and response stream for one row,
    // which is measurably slower than the same TiKV range seek. This remains
    // fail-closed because the shape gate above rejects every residual,
    // partitioned, dirty, or non-clustered table.
    let rows = table
        .first_row_in_handle_ranges(None, &ranges, &ctx.session_zone())
        .map_err(|error| DriverError::Parse(format!("row decode failed: {error:?}")))?
        .map(|(_, row)| {
            output_offsets
                .into_iter()
                .map(|offset| row.get(offset).cloned().unwrap_or(Datum::Null))
                .collect::<Vec<_>>()
        })
        .into_iter()
        .collect();
    Ok(Some((output_columns, rows)))
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

/// The columns left in Go's `restoreSchemaIfChanged` projection after column
/// pruning. `written_order` maps each original leaf to its new scope table.
fn restored_join_projection_fields(
    scope: &FromScope,
    written_order: &[usize],
    demand: &leaf_demand::LeafDemand,
    planned: &tidb_ast::Join,
    catalog: &Catalog,
    current_db: &str,
) -> Option<(Vec<crate::driver::merge_decision::RelColumn>, Vec<String>)> {
    if scope.tables.len() != written_order.len()
        || written_order
            .iter()
            .enumerate()
            .all(|(at, moved)| at == *moved)
    {
        return None;
    }
    let root = JoinNode::Join(Box::new(planned.clone()));
    let mut columns = Vec::new();
    let mut fields = Vec::new();
    for position in written_order {
        let table = scope.tables.get(*position)?;
        for local in demand.needed(&table.name, &table.columns) {
            let offset = table.offset + local;
            let column = crate::driver::merge_decision::RelColumn {
                relation: table.name.clone(),
                column: table.columns.get(local)?.0.clone(),
            };
            fields.push(
                crate::driver::merge_decision::physical_column_trace_name(
                    &root, &column, catalog, current_db,
                )
                .unwrap_or_else(|| format!("Column#{offset}")),
            );
            columns.push(column);
        }
    }
    let actual = scope
        .tables
        .iter()
        .flat_map(|table| {
            demand
                .needed(&table.name, &table.columns)
                .into_iter()
                .filter_map(|local| {
                    Some(crate::driver::merge_decision::RelColumn {
                        relation: table.name.clone(),
                        column: table.columns.get(local)?.0.clone(),
                    })
                })
        })
        .collect::<Vec<_>>();
    (!fields.is_empty() && actual != columns).then_some((columns, fields))
}

/// The grouped columns' positions in a SELECT's own result row. A grouped
/// StreamAgg emits in this order; selected carriers preserve it through the
/// final projection.
fn grouped_select_output_order(select: &tidb_ast::SelectStmt) -> Option<Vec<usize>> {
    select
        .group_by
        .iter()
        .map(|group| {
            let tidb_ast::Expr::Column(group_path) = &group.expr else {
                return None;
            };
            select.fields.fields().iter().position(|field| {
                matches!(field,
                    tidb_ast::SelectField::Expr {
                        expr: tidb_ast::Expr::Column(path),
                        ..
                    } if path == group_path)
            })
        })
        .collect()
}

/// The name a source operator's `access object` prints: the alias the FROM
/// clause gave the table, which is what Go prints too.
fn source_table_name<'a>(scope: &'a FromScope, table: &'a str) -> &'a str {
    match scope.tables.first() {
        Some(first) => &first.name,
        None => table,
    }
}

/// Whether the select list is the source table's complete row in source
/// order. Go eliminates this identity projection even when the columns were
/// written explicitly (`SELECT id, k, c, pad`), while retaining a projection
/// for a proper subset such as `SELECT c`.
fn projects_entire_single_table_in_order(select: &tidb_ast::SelectStmt, scope: &FromScope) -> bool {
    let [table] = scope.tables.as_slice() else {
        return false;
    };
    let fields = select.fields.fields();
    if let [SelectField::Wildcard(qualifier)] = fields {
        return qualifier
            .last()
            .is_none_or(|name| table.name.eq_ignore_ascii_case(name));
    }
    if fields.len() != table.columns.len() {
        return false;
    }
    let resolver = ScopeResolver { scope };
    fields.iter().enumerate().all(|(expected, field)| {
        let SelectField::Expr {
            expr: tidb_ast::Expr::Column(path),
            ..
        } = field
        else {
            return false;
        };
        resolver
            .resolve(path)
            .is_some_and(|(offset, _, _)| offset == expected)
    })
}

/// The source-order schema Go keeps on an ordered double-read below the
/// select-list projection. Only distinct plain columns are admitted because
/// their positional scope can be remapped without inventing expression ids.
fn ordered_lookup_projection(
    select: &tidb_ast::SelectStmt,
    scope: &FromScope,
) -> Option<(Vec<usize>, Vec<SelectField>)> {
    let resolver = ScopeResolver { scope };
    let mut columns = Vec::new();
    for field in select.fields.fields() {
        let SelectField::Expr {
            expr: tidb_ast::Expr::Column(path),
            ..
        } = field
        else {
            return None;
        };
        let (offset, _, _) = resolver.resolve(path)?;
        columns.push((offset, field.clone()));
    }
    columns.sort_by_key(|(offset, _)| *offset);
    let offsets = columns
        .iter()
        .map(|(offset, _)| *offset)
        .collect::<Vec<_>>();
    if offsets.windows(2).any(|pair| pair[0] == pair[1]) {
        return None;
    }
    let selected = select
        .fields
        .fields()
        .iter()
        .filter_map(|field| match field {
            SelectField::Expr {
                expr: tidb_ast::Expr::Column(path),
                ..
            } => resolver.resolve(path).map(|(offset, _, _)| offset),
            _ => None,
        })
        .collect::<Vec<_>>();
    (offsets != selected).then(|| {
        (
            offsets,
            columns.into_iter().map(|(_, field)| field).collect(),
        )
    })
}

/// Runs one parsed `SELECT` against the catalog.
fn run_select_stmt(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_select_traced(
        select,
        catalog,
        current_db,
        ctx,
        None,
        &tidb_planner::physical_property::PhysicalProperty::default(),
        false,
    )
}

/// [`run_select_stmt`], recording the plan it builds into `trace`.
///
/// This is the one control flow that decides a `SELECT`'s shape, so it is
/// also the only place that describes one: each site that commits to an
/// executor records the matching node (see [`crate::plan_trace`]), and in
/// `EXPLAIN ANALYZE` mode the executor is metered so the node's `actRows` is
/// the count that operator really produced. A plan-only trace stops before
/// the drain below, so plain `EXPLAIN` yields no result row.
/// `required` is the order this `SELECT`'s OWN OUTPUT is asked for, in its
/// result-field offsets -- non-empty only when a join above a DERIVED TABLE
/// requires one of it (`from::build_from`'s `Derived` arm). Go's
/// `PhysicalProjection.exhaustPhysicalPlans` maps it through the select list
/// onto the `FROM`, which is what
/// [`merge_decision::from_required_prop`] does below.
pub(crate) fn run_select_traced(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    trace: Option<&mut PlanTrace>,
    required: &tidb_planner::physical_property::PhysicalProperty,
    parent_duplicate_agnostic: bool,
) -> Result<SelectMeta, DriverError> {
    run_select_traced_with_delivery(
        select,
        catalog,
        current_db,
        ctx,
        trace,
        required,
        None,
        None,
        parent_duplicate_agnostic,
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AggregationChoice {
    Auto,
    Hash,
    Stream,
}

/// [`run_select_traced`] plus the order the built SELECT output actually
/// retains. Derived-table materialization uses this receipt instead of
/// predicting order from catalog properties after execution.
pub(super) fn run_select_traced_with_delivery(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
    required: &tidb_planner::physical_property::PhysicalProperty,
    output_delivered: Option<&mut from::Delivered>,
    deferred_exec: Option<&mut Option<Box<dyn Executor>>>,
    parent_duplicate_agnostic: bool,
) -> Result<SelectMeta, DriverError> {
    let query_source_frame_depth = trace.as_deref().map(PlanTrace::query_source_frame_depth);
    let result = run_select_traced_with_delivery_choice(
        select,
        catalog,
        current_db,
        ctx,
        trace.as_deref_mut(),
        required,
        output_delivered,
        deferred_exec,
        parent_duplicate_agnostic,
        AggregationChoice::Auto,
    );
    if let (Some(trace), Some(depth)) = (trace, query_source_frame_depth) {
        trace.truncate_query_source_frames(depth);
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn run_select_traced_with_delivery_choice(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    trace: Option<&mut PlanTrace>,
    required: &tidb_planner::physical_property::PhysicalProperty,
    output_delivered: Option<&mut from::Delivered>,
    deferred_exec: Option<&mut Option<Box<dyn Executor>>>,
    parent_duplicate_agnostic: bool,
    aggregation_choice: AggregationChoice,
) -> Result<SelectMeta, DriverError> {
    // Every nested SELECT -- a derived table, a subquery, a view -- plans by
    // recursing through here, and a debug build's planner frames run to
    // hundreds of KB per level (this function's alone is ~276 KB). Go never
    // aborts on that depth because a goroutine's stack grows on demand;
    // `maybe_grow` is that semantics for a Rust thread. The red zone must
    // cover one full level of the planner -- this frame plus `build_from`,
    // `build_join_with_choice` and the aggregation builder -- before the
    // next check runs, hence 2 MB, with 16 MB segments so growth is rare.
    stacker::maybe_grow(2 * 1024 * 1024, 16 * 1024 * 1024, move || {
        run_select_traced_with_delivery_choice_inner(
            select,
            catalog,
            current_db,
            ctx,
            trace,
            required,
            output_delivered,
            deferred_exec,
            parent_duplicate_agnostic,
            aggregation_choice,
        )
    })
}

#[allow(clippy::too_many_arguments)]
fn run_select_traced_with_delivery_choice_inner(
    select: &tidb_ast::SelectStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
    mut trace: Option<&mut PlanTrace>,
    required: &tidb_planner::physical_property::PhysicalProperty,
    mut output_delivered: Option<&mut from::Delivered>,
    mut deferred_exec: Option<&mut Option<Box<dyn Executor>>>,
    parent_duplicate_agnostic: bool,
    aggregation_choice: AggregationChoice,
) -> Result<SelectMeta, DriverError> {
    // Go enumerates HashAgg before StreamAgg for an empty required property,
    // rebuilds the child under each candidate's property, and keeps the first
    // candidate on an exact tie. Build both plans without executing or tracing
    // them, then rebuild only the cheaper one into the caller's real output.
    if aggregation_choice == AggregationChoice::Auto
        && !select.group_by.is_empty()
        && required.is_sort_item_empty()
    {
        // Go's logical statistics collection runs once before physical
        // aggregation alternatives are compared. Each speculative branch
        // must therefore start from the same shared residency snapshot;
        // otherwise a derived source can make a later branch use a different
        // NDV and change its join order/cardinality.
        let stats_checkpoint = catalog.statistics_load_checkpoint();
        let plan_only = trace.as_deref().is_some_and(PlanTrace::is_plan_only);
        let mut stream_delivered = from::Delivered::new();
        let mut stream_exec = None;
        let mut stream_trace = plan_only.then(PlanTrace::planning);
        let stream = run_select_traced_with_delivery_choice(
            select,
            catalog,
            current_db,
            ctx,
            stream_trace.as_mut(),
            required,
            Some(&mut stream_delivered),
            Some(&mut stream_exec),
            parent_duplicate_agnostic,
            AggregationChoice::Stream,
        );
        catalog.restore_statistics_load_checkpoint(&stats_checkpoint);
        let mut hash_delivered = from::Delivered::new();
        let mut hash_exec = None;
        let mut hash_trace = plan_only.then(PlanTrace::planning);
        let hash = run_select_traced_with_delivery_choice(
            select,
            catalog,
            current_db,
            ctx,
            hash_trace.as_mut(),
            required,
            Some(&mut hash_delivered),
            Some(&mut hash_exec),
            parent_duplicate_agnostic,
            AggregationChoice::Hash,
        );
        catalog.restore_statistics_load_checkpoint(&stats_checkpoint);
        let costed = |result: &Result<SelectMeta, DriverError>,
                      delivered: &from::Delivered,
                      stream: bool| {
            result.as_ref().ok()?;
            let candidate = delivered.candidate.as_ref()?;
            if stream
                != matches!(
                    candidate,
                    tidb_planner::candidate_cost::Candidate::StreamAgg { .. }
                )
            {
                return None;
            }
            Some(tidb_planner::candidate_cost::evaluate(
                candidate,
                &tidb_planner::candidate_cost::CostEnv::default(),
                tidb_planner::task_type::TaskType::Root,
            ))
        };
        let stream_cost = costed(&stream, &stream_delivered, true);
        let hash_cost = costed(&hash, &hash_delivered, false);
        let chosen = match (&stream_cost, &hash_cost) {
            (Some(stream), Some(hash)) => {
                if tidb_planner::candidate_cost::prefer(stream, hash) {
                    Some(AggregationChoice::Stream)
                } else {
                    Some(AggregationChoice::Hash)
                }
            }
            (Some(_), None) => Some(AggregationChoice::Stream),
            (None, Some(_)) => Some(AggregationChoice::Hash),
            (None, None) => None,
        };
        if let Some(chosen) = chosen {
            return run_select_traced_with_delivery_choice(
                select,
                catalog,
                current_db,
                ctx,
                trace,
                required,
                output_delivered,
                deferred_exec,
                parent_duplicate_agnostic,
                chosen,
            );
        }
    }
    // Go invokes `TryFastPlan` before `PlanBuilder`: a complete point read does
    // not materialize CTEs, run logical rewrites, derive statistics, or build a
    // full-scan source first. Only the statement-level call is eligible; a
    // derived SELECT is optimized as part of its parent's ordinary plan.
    if output_delivered.is_none() && deferred_exec.is_none() && required.is_sort_item_empty() {
        if let Some(result) =
            try_fast_point_select(select, catalog, current_db, ctx, trace.as_deref_mut())?
        {
            return Ok(result);
        }
    }
    // Only a derived-table caller asks for an output-order receipt. Go's
    // projection elimination can then map the outer relation directly onto
    // an aggregation's schema, while a top-level SELECT retains the visible
    // Projection that restores function-first partial aggregate outputs.
    let derived_output = output_delivered.is_some();
    if let Some(delivered) = output_delivered.as_deref_mut() {
        delivered.clear();
    }
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
    // Go's expression rewriter turns a positive, uncorrelated IN predicate
    // into an inner join against the DISTINCT subquery output before the
    // logical rules run. Ordinary execution and every EXPLAIN mode share this
    // path; the remaining subquery forms keep their Apply/fold semantics.
    let in_subquery_rewritten;
    let select = match subquery::rewrite_filter_in_subqueries(select, catalog, current_db, ctx)? {
        Some(rewritten) => {
            in_subquery_rewritten = rewritten;
            &in_subquery_rewritten
        }
        None => select,
    };
    // Go's DecorrelateSolver turns equality-correlated scalar aggregations
    // into ordinary joins and aggregations. This executor has no retained
    // logical-plan tree, so the proof-shaped equivalent rewrites the AST
    // before every later optimizer rule and before plan tracing.
    let decorrelated;
    let decorrelated_aggregate;
    let select = match correlated_agg_decorrelate::rewrite(select, catalog, current_db, ctx) {
        Some(rewritten) => {
            decorrelated = rewritten;
            decorrelated_aggregate = true;
            &decorrelated
        }
        None => {
            decorrelated_aggregate = false;
            select
        }
    };
    // Ordinary execution and EXPLAIN ANALYZE evaluate uncorrelated subqueries
    // now. Plain EXPLAIN follows Go's `ExplainNonEvaledSubQuery` branch: it
    // plans each scalar/EXISTS child into a separate root and keeps a typed
    // planner placeholder in the outer expression.
    let pre_subquery_select = select;
    let plan_only = trace.as_deref().is_some_and(PlanTrace::is_plan_only);
    let mut go_logical_plan_columns = if plan_only {
        trace.as_deref().map(|trace| {
            subquery::reserve_go_logical_plan_columns(select, catalog, current_db, trace)
        })
    } else {
        None
    };
    let mut plan_columns = Vec::new();
    let planned;
    let folded;
    let select = if subquery::select_has_subquery(select) {
        let outer = select_outer_scope(select, catalog, current_db, ctx);
        if plan_only {
            planned = subquery::plan_select_subqueries(
                select,
                &outer,
                catalog,
                current_db,
                ctx,
                trace.as_deref_mut().expect("plan-only trace exists"),
            )?;
            plan_columns.extend(planned.columns.iter().cloned());
            &planned.select
        } else {
            folded = fold_select_subqueries(select, &outer, catalog, current_db, ctx)?;
            &folded
        }
    } else {
        select
    };
    if let Some(columns) = go_logical_plan_columns.as_mut() {
        columns.finish_after_subqueries(
            pre_subquery_select,
            trace
                .as_deref_mut()
                .expect("plan-column allocation requires a plan-only trace"),
        );
    }
    // Runtime-only literal folding stays invisible. Plain EXPLAIN instead
    // prints the ScalarQueryCol placeholders Go put into the logical plan.
    let traced_select = if plan_only {
        select
    } else {
        pre_subquery_select
    };
    // Go expands a derived table's wildcard while constructing its logical
    // Projection. Projection elimination then sees the expanded expressions,
    // so a `SELECT * FROM (a JOIN b)` identity projection can disappear
    // before join reorder even when through-projection reordering is OFF.
    let expanded_derived_wildcards;
    let select =
        match derived_projection_pushdown::expand_derived_wildcards(select, catalog, current_db) {
            Some(rewritten) => {
                expanded_derived_wildcards = rewritten;
                &expanded_derived_wildcards
            }
            None => select,
        };
    // Go's AggregationPushDownSolver runs before join reorder. Its
    // Aggregation -> Projection arm substitutes aggregate arguments and group
    // items through a simple derived projection, then removes that projection.
    // The outer Projection and ORDER BY remain above the aggregation; the
    // returned statement is only the AST adapter used to build that lower
    // aggregation input in this plan-tree-free driver.
    let aggregation_inputs_pushed;
    let aggregation_semantic_scope;
    let aggregate_projection_pushed;
    let select = match through_proj::push_aggregation_inputs_through_projection(
        select, catalog, current_db, ctx,
    ) {
        Some(rewritten) => {
            aggregation_inputs_pushed = true;
            aggregation_semantic_scope = Some(rewritten.semantic_scope);
            aggregate_projection_pushed = rewritten.select;
            &aggregate_projection_pushed
        }
        None => {
            aggregation_inputs_pushed = false;
            aggregation_semantic_scope = None;
            select
        }
    };
    // Go's projection elimination removes bare-column projections before join
    // reorder. Projections that still compute expressions dissolve only when
    // `tidb_opt_join_reorder_through_proj` is enabled; both cases share the
    // same name-preserving AST splice.
    let inlined;
    let inlined_computed_output;
    let select = match through_proj::inline(select, catalog, current_db, ctx) {
        Some(rewritten) => {
            inlined_computed_output = rewritten.computed_output_restored;
            inlined = rewritten.select;
            &inlined
        }
        None => {
            inlined_computed_output = false;
            select
        }
    };
    // Move leaf-local predicates through those derived projections before
    // pruning their now-unread outputs. Join equalities and multi-relation
    // predicates remain at the join that executes them.
    let derived_predicates_pushed;
    let select = match derived_projection_pushdown::push_local_predicates_into_derived(
        select, catalog, current_db, ctx,
    ) {
        Some(rewritten) => {
            derived_predicates_pushed = rewritten;
            &derived_predicates_pushed
        }
        None => select,
    };
    // Go substitutes predicates through a pass-through derived projection
    // before simplifying NULL-rejecting outer joins. The TPCC consistency
    // checks use that chain to expose both leaf ranges and a local inner
    // merge join; the proof-shaped rewrite refuses every other derived form.
    let globally_counted;
    let select = match derived_projection_pushdown::fuse_global_count(select, catalog, current_db) {
        Some(rewritten) => {
            globally_counted = rewritten;
            &globally_counted
        }
        None => select,
    };
    // Go's `LogicalAggregation.PruneColumns` reaching a derived table: an
    // ungrouped aggregation nobody reads a column of keeps only the `count(1)`
    // that carries its row. See `driver::derived_agg_pruning`.
    let unaggregated;
    let grouped_derived_output_pruned;
    let select = match derived_agg_pruning::prune(select) {
        Some(rewritten) => {
            grouped_derived_output_pruned =
                derived_agg_pruning::is_single_grouped_derived(&rewritten);
            unaggregated = rewritten;
            &unaggregated
        }
        None => {
            grouped_derived_output_pruned = false;
            select
        }
    };
    // Go's `rule_join_elimination`: an outer join whose null-producing side
    // nobody reads and which cannot duplicate an outer row is dropped, so the
    // inner table is never accessed at all. See
    // `driver::outer_join_elimination` for the half of Go's rule this
    // implements and for every shape it refuses.
    let unjoined;
    let select = match outer_join_elimination::eliminate(
        select,
        catalog,
        current_db,
        parent_duplicate_agnostic,
    ) {
        Some(rewritten) => {
            unjoined = rewritten;
            &unjoined
        }
        None => select,
    };
    // The AST optimizer passes above can expose a derived table's predicates
    // in this query block after the initial subquery fold has run. Rewrite
    // those newly visible nodes now, in Go's expression-rewriter order:
    // uncorrelated subqueries become constants while correlated ones remain
    // for the Apply/decorrelation path below.
    let late_planned;
    let late_folded;
    let select = if subquery::select_has_subquery(select) {
        let outer = select_outer_scope(select, catalog, current_db, ctx);
        if plan_only {
            late_planned = subquery::plan_select_subqueries(
                select,
                &outer,
                catalog,
                current_db,
                ctx,
                trace.as_deref_mut().expect("plan-only trace exists"),
            )?;
            plan_columns.extend(late_planned.columns.iter().cloned());
            &late_planned.select
        } else {
            late_folded = fold_select_subqueries(select, &outer, catalog, current_db, ctx)?;
            &late_folded
        }
    } else {
        select
    };
    // Derived-table fusion can pull a decorrelated scalar-SUM shape into a
    // caller after this recursive call's initial rewrite returned `None`.
    // Recognize the resulting invariant shape here so physical plan details
    // keep using catalog table/column names throughout the pulled plan.
    let physical_source_names = decorrelated_aggregate
        || correlated_agg_decorrelate::is_pulled_scalar_sum(select)
        || correlated_agg_decorrelate::is_pulled_scalar_sum_wrapper(select)
        || (decorrelate_exists::has_top_level_exists(select.where_clause.as_ref())
            && access::single_kv_table(&select.from, catalog, current_db).is_none());
    let distinct_eliminated = agg_select::distinct_can_be_eliminated(select, catalog, current_db);

    // Go's `buildSelect` pushes this block's `/*+ ... */` hints and its
    // deferred `popTableHints` reports the ones no `DataSource` of the block
    // claimed, as 1815. It runs whether or not there is a `FROM` -- a hint on
    // a `FROM`-less select names nothing and is reported too. Captured.
    crate::index_hints::report_comment_index_hints(select, catalog, current_db, ctx);
    // Resolve FROM: none -> table-dual; otherwise the (possibly joined) tables.
    let mut join_consumed_where = false;
    // `Some` means a join group proved how the written WHERE splits.  The
    // inner option is its cross-leaf residue; `Some(None)` means leaf filters
    // and join equalities consumed the whole predicate.
    let mut join_residual_where: Option<Option<tidb_ast::Expr>> = None;
    let mut aggregation_order = None;
    let mut grouped_logical_rows = None;
    let mut distinct_logical_rows = None;
    let mut joined_logical_rows = None;
    let mut restored_join_output = None;
    let mut aggregate_join_projection = None;
    // The ORDER this select's `FROM` source was asked for, in the sole
    // table's own column offsets -- carried out of the arm below so
    // `commit_fast_path_source` can refuse to replace the ordered scan with
    // an unordered path (see its `required_order` doc).
    let mut source_required_order: Option<Vec<usize>> = None;
    let (mut from_source, mut scope, mut from_delivered): (
        Option<Box<dyn Executor>>,
        FromScope,
        from::Delivered,
    ) = match &select.from {
        None => {
            if let Some(trace) = trace.as_deref_mut() {
                trace.table_dual();
            }
            (None, FromScope::for_statement(ctx), from::Delivered::new())
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
            let simplified = outer_join_simplify::simplify(
                join,
                select.where_clause.as_ref(),
                catalog,
                current_db,
            );
            let logical_join = simplified.as_ref().unwrap_or(join);
            // Go's `rule_predicate_push_down`: the `WHERE` equalities are
            // offered to the joins below, so a comma join does not have to
            // build the cross product the filter would then throw away. See
            // `driver::predicate_push_down`.
            let offered = predicate_push_down::offered_conjuncts(select.where_clause.as_ref());
            let pushdown = predicate_push_down::plan(
                logical_join,
                select.where_clause.as_ref(),
                catalog,
                current_db,
                false,
            );
            // Go's aggregation elimination runs before physical property
            // enforcement. Once a unique group becomes a projection, the
            // source no longer owes a group-key order; asking for it here
            // would make the eventual scan claim `keep order:true` for work
            // the executable plan does not require.
            aggregation_order = (aggregation_choice != AggregationChoice::Hash
                && !physical_source_names
                && !decorrelate_exists::has_top_level_exists(select.where_clause.as_ref())
                && !agg_select::aggregation_can_be_eliminated(select, catalog, current_db))
            .then(|| {
                merge_decision::aggregation_order(
                    select,
                    logical_join,
                    catalog,
                    current_db,
                    &offered,
                )
            })
            .flatten();
            // Go's `rule_column_pruning`: what every `DataSource` below still
            // has to produce, which is the input its access-path costing
            // needs (`isCoveringIndex`). A `FROM` of ONE base table is
            // deliberately excluded -- `commit_fast_path_source` below costs
            // that table's paths WITH its `WHERE`, and a second, condition-
            // blind choice here could only be the worse of the two. A
            // grouping order is the exception: it is a non-empty physical
            // property that has to reach the leaf before the aggregate is
            // built, so the leaf must cost its WHERE-constrained ordered path
            // here instead of waiting for the later empty-property fast path.
            let plan_at_leaf = access::single_kv_table(&select.from, catalog, current_db).is_none()
                || aggregation_order.is_some();
            // The statement-wide name walk, always taken: `wanted` below is
            // withheld from a single-KV-table `SELECT` because that leaf is
            // costed later, and a question about which names the statement
            // WRITES cannot be deferred with it.
            let all_names = leaf_demand::LeafDemand::of_select(select);
            let wanted = plan_at_leaf.then(|| all_names.clone());
            let output_wanted = leaf_demand::LeafDemand::of_select_output(select);
            // The estimate owner: every relation of this `FROM` with the row
            // count `derive_stats` derives for it, read off the statement,
            // the catalog and the statistics. It is built here, beside the
            // reorder that costs the same models, because both need the join
            // group as WRITTEN -- and NOT off `PlanTrace`, which exists only
            // under `EXPLAIN`. See `driver::join_search`.
            // A plain single-KV-table SELECT is costed later by
            // `commit_fast_path_source`, with its original WHERE intact. If
            // RowSource consumes that predicate here while column demand is
            // deliberately absent, the later point/range chooser sees no
            // key condition and the statement degenerates to a full scan.
            // A grouped stream plan is the exception above: its non-empty
            // property has already committed leaf planning at this site.
            // A plain single-table SELECT has one statistics owner below:
            // `commit_fast_path_source` derives the DataSource rows while it
            // costs the access paths. Running `row_source` here as well used
            // to repeat the full selectivity/ranger pass, which is especially
            // visible for large IN predicates. Joins still need the complete
            // relation map before they are built, while GROUP BY / DISTINCT
            // need its per-expression NDV estimates; retain those cases.
            let needs_row_source_estimate = plan_at_leaf
                || !select.group_by.is_empty()
                || select.distinct
                || select.fields.fields().iter().any(|field| {
                    matches!(field, SelectField::Expr { expr, .. } if expr.has_aggregate_flag())
                })
                || select
                    .having
                    .as_ref()
                    .is_some_and(tidb_ast::Expr::has_aggregate_flag)
                || select
                    .order_by
                    .iter()
                    .any(|item| item.expr.has_aggregate_flag());
            let row_source = needs_row_source_estimate
                .then(|| {
                    join_reorder::row_source(
                        logical_join,
                        select.where_clause.as_ref(),
                        catalog,
                        current_db,
                        ctx,
                    )
                })
                .flatten();
            grouped_logical_rows = row_source
                .as_ref()
                .and_then(|rows| rows.grouped_rows(&select.group_by));
            if select.distinct {
                let expressions = select
                    .fields
                    .fields()
                    .iter()
                    .map(|field| match field {
                        SelectField::Expr { expr, .. } => Some(expr),
                        SelectField::Wildcard(_) => None,
                    })
                    .collect::<Option<Vec<_>>>();
                distinct_logical_rows = expressions.as_deref().and_then(|expressions| {
                    row_source
                        .as_ref()
                        .and_then(|rows| rows.grouped_expression_rows(expressions))
                });
            }
            joined_logical_rows = row_source.as_ref().and_then(|rows| rows.root_rows());
            if joined_logical_rows.is_none() {
                joined_logical_rows = join_reorder::sole_derived_rows(
                    join,
                    select.where_clause.as_ref(),
                    catalog,
                    current_db,
                    ctx,
                );
            }
            // Estimating the logical rows above is side-effect free. Only
            // hand the source to leaf planning when this pass owns the
            // physical access decision; otherwise the empty-property fast
            // path below must remain the sole owner of predicate consumption.
            let row_source = plan_at_leaf.then_some(row_source).flatten();
            // Go's `SetPreferredJoinTypeAndOrder`: the statement's own join
            // hints, which decide at some sites which physical families are
            // enumerated AT ALL. See `driver::join_method_hints`.
            let join_hints =
                join_method_hints::JoinMethodHints::of_select_reporting(select, Some(ctx));
            let demand = leaf_demand::FromDemand {
                offered: &offered,
                pushdown: Some(&pushdown),
                columns: wanted.as_ref(),
                all_names: Some(&all_names),
                output_columns: Some(&output_wanted),
                rows: row_source.as_ref(),
                join_hints: (!join_hints.is_empty()).then_some(&join_hints),
                physical_source_names,
                plan_columns: &plan_columns,
                runtime_lookup: None,
                // A MULTI-table `FROM` fans its partitioned leaves out in the
                // leaf builder; a sole KV table is fanned out below instead,
                // after its fast paths have replaced the scan. Exactly one of
                // the two must fire -- see `FromDemand::partition_fan_out`.
                partition_fan_out: access::sole_kv_table(&select.from, catalog, current_db)
                    .is_none(),
            };
            // Go's `join_reorder` rule, which runs on the logical plan
            // between predicate pushdown and physical planning. It only ever
            // fires for a session that raised
            // `tidb_opt_join_reorder_threshold`; see
            // `driver::join_reorder`'s module doc for why that bounds it.
            let reordered = join_reorder::reorder(
                logical_join,
                select,
                select.where_clause.as_ref(),
                catalog,
                current_db,
                ctx,
            );
            if let (Some(reordered), Some(rows)) = (&reordered, row_source.as_ref()) {
                grouped_logical_rows = rows
                    .grouped_rows_for_join(&reordered.join, &select.group_by)
                    .or(grouped_logical_rows);
                if select.distinct {
                    let expressions = select
                        .fields
                        .fields()
                        .iter()
                        .map(|field| match field {
                            SelectField::Expr { expr, .. } => Some(expr),
                            SelectField::Wildcard(_) => None,
                        })
                        .collect::<Option<Vec<_>>>();
                    distinct_logical_rows = expressions.as_deref().and_then(|expressions| {
                        rows.grouped_expression_rows_for_join(&reordered.join, expressions)
                    });
                }
                joined_logical_rows = rows
                    .root_rows_for_join(&reordered.join)
                    .or(joined_logical_rows);
            }
            let planned = reordered.as_ref().map_or(logical_join, |plan| &plan.join);
            let parent_required = merge_decision::from_required_prop(
                select, planned, required, catalog, current_db, &offered,
            );
            let aggregation_required = aggregation_order
                .as_ref()
                .and_then(|order| order.required_for(planned, catalog, current_db, &offered));
            let semi_join_required = tidb_planner::physical_property::PhysicalProperty::default();
            let source_required =
                if decorrelate_exists::has_top_level_exists(select.where_clause.as_ref()) {
                    &semi_join_required
                } else {
                    aggregation_required.as_ref().unwrap_or(&parent_required)
                };
            if !source_required.is_sort_item_empty() {
                source_required_order = Some(
                    source_required
                        .sort_items
                        .iter()
                        .map(|item| item.col as usize)
                        .collect(),
                );
            }
            let (exec, mut scope, delivered) = build_join(
                planned,
                catalog,
                current_db,
                ctx,
                trace.as_deref_mut(),
                Some(select),
                demand,
                // The order this select's OUTPUT was asked for, carried down
                // onto the `FROM` it is projected from -- read off `planned`
                // rather than the written join, because a reorder renumbers
                // the leaves and the offsets are the reordered ones.
                source_required,
            )?;
            // Read the residue only after both leaves committed their
            // physical paths and reported which local filters those paths
            // actually accepted.
            if let Some(rows) = row_source.as_ref() {
                let residual = rows.residual_where();
                join_consumed_where = select.where_clause.is_some() && residual.is_none();
                join_residual_where = Some(residual);
            }
            join_consumed_where |= exec.consumes_where();
            // Go's `restoreSchemaIfChanged`: the reordered join's schema is
            // the new leaf order, and the statement's output must stay the
            // written one. Go wraps a `Projection`; here the scope carries
            // the display order (`FromScope::star`), which is the same escape
            // hatch a `RIGHT JOIN` uses.
            if let Some(plan) = &reordered {
                let aggregate_path = !select.group_by.is_empty()
                    || select.fields.fields().iter().any(|field| {
                        matches!(field, SelectField::Expr { expr, .. } if expr.has_aggregate_flag())
                    })
                    || select
                        .having
                        .as_ref()
                        .is_some_and(tidb_ast::Expr::has_aggregate_flag)
                    || select
                        .order_by
                        .iter()
                        .any(|item| item.expr.has_aggregate_flag());
                // The final projection absorbs a restore introduced by
                // through-projection inlining. Every other independently
                // reordered join first restores its original compact schema;
                // the physical projection eliminator may remove the SELECT's
                // projection later only when it copies that schema exactly.
                if !inlined_computed_output {
                    if let Some((columns, fields)) = restored_join_projection_fields(
                        &scope,
                        &plan.written_order,
                        &output_wanted,
                        &plan.join,
                        catalog,
                        current_db,
                    ) {
                        if aggregate_path {
                            let sources = columns
                                .iter()
                                .map(|column| from::scope_offset_of(&scope, column))
                                .collect::<Option<Vec<_>>>();
                            if let Some(sources) = sources {
                                aggregate_join_projection =
                                    Some(agg_select::JoinOutputProjection { sources, fields });
                            }
                        } else {
                            if let Some(trace) = trace.as_deref_mut() {
                                trace.join_reorder_projection(&fields);
                            }
                            restored_join_output = Some(columns);
                        }
                    }
                }
                restore_written_order(&mut scope, &plan.written_order);
            }
            (Some(exec), scope, delivered)
        }
    };
    if let Some(columns) = go_logical_plan_columns.take() {
        columns.finish_after_source(
            pre_subquery_select,
            trace
                .as_deref_mut()
                .expect("plan-column allocation requires a plan-only trace"),
        );
    }
    scope.plan_columns = plan_columns;

    // Predicate pushdown through a join is not all-or-nothing.  Build the
    // remaining pipeline from the exact cross-leaf residue: leaf-local
    // conjuncts are already installed by `build_from`, and join equalities by
    // `build_join`.  The traced statement follows the same rewrite so EXPLAIN
    // describes the executor tree that actually runs.
    //
    // Keep the pre-pushdown logical statement for semantic checks. Go runs
    // ONLY_FULL_GROUP_BY while the Selection still owns its predicates, so a
    // null-rejecting WHERE can wake an outer join's conditional FDs even when
    // execution later pushes that predicate into a child access path.
    let semantic_select = traced_select;
    let residual_select;
    let residual_traced_select;
    let (select, traced_select) = match join_residual_where {
        Some(residual) => {
            residual_select = {
                let mut rewritten = select.clone();
                rewritten.where_clause = residual.clone();
                rewritten
            };
            residual_traced_select = {
                let mut rewritten = traced_select.clone();
                rewritten.where_clause = residual;
                rewritten
            };
            (&residual_select, &residual_traced_select)
        }
        None => (select, traced_select),
    };

    // The access-path decision and the work handed down to it live in
    // `driver::access`; `index_order` is set when the committed source emits
    // rows in an index's order, which is what lets a `LIMIT` under a matching
    // `ORDER BY` stop the scan early.
    // A grouped StreamAgg's non-empty property was already costed and
    // committed by the leaf builder. Re-entering the empty-property chooser
    // here loses both its range predicate (already marked consumed) and its
    // required order, replacing the real `[w_id,w_id]` path with an
    // `IndexFullScan`. Go keeps the task returned for the requested property.
    // Go `LogicalAggregation.PredicatePushDown`
    // (`pkg/planner/core/operator/logicalop/logical_aggregation.go:106`,
    // `splitCondForAggregation` `:631`): a HAVING conjunct whose columns are
    // all grouping columns descends below the aggregation, so the
    // `DataSource` builds its ranges and prunes its partitions from the
    // WHERE *and* that HAVING. The access view is the only thing that
    // changes here -- the HAVING `Selection` above the aggregation still
    // evaluates ([`having`]), which filters exactly the groups the pushed
    // predicate already removed whole.
    let grouped_access_select = agg_predicate_pushdown::for_access(select);
    let access_select = grouped_access_select.as_ref().unwrap_or(select);
    let access_path = if aggregation_order.is_some() {
        AccessPathCommit::default()
    } else {
        commit_fast_path_source(
            access_select,
            catalog,
            current_db,
            &scope,
            &mut from_source,
            trace.as_deref_mut(),
            ctx,
            source_required_order.as_deref(),
        )?
    };
    let AccessPathCommit {
        mut index_order,
        candidate: access_candidate,
        direct_output,
        direct_output_offsets,
        cop_projection_offsets,
        filtered_cop_projection_offsets,
        consumed_where,
        handle_range_residual,
        access_residual,
        logical_rows,
        reader_ready,
        order_satisfied,
    } = access_path;
    // For the plain single-table case `row_source` deliberately deferred this
    // number to the access-path owner above. Both values are Go's logical
    // DataSource row count; sharing it here keeps downstream Selection/TopN
    // estimates unchanged without a second ranger pass.
    if joined_logical_rows.is_none() {
        joined_logical_rows = logical_rows;
    }
    let consumed_where = consumed_where || join_consumed_where;
    let grouped_stream_ordered = aggregation_order
        .as_ref()
        .is_some_and(|order| order.is_delivered_by(&from_delivered, &scope));
    if grouped_stream_ordered {
        if let (Some(delivered), Some(order)) = (
            output_delivered.as_deref_mut(),
            grouped_select_output_order(traced_select),
        ) {
            delivered.push(order);
        }
    }
    // Go's `TryFastPlan` makes a simple select list part of PointGetPlan or
    // BatchPointGetPlan itself. The lookup source already emits exactly that
    // schema, and the equality/IN predicate was fully consumed by the key,
    // so returning it here is the real one-node executor tree Go explains --
    // not a display-only suppression of Selection and Projection.
    if let Some(columns) = direct_output {
        let direct_ready = match direct_output_offsets {
            Some(offsets) => {
                let accepted = from_source
                    .as_mut()
                    .and_then(|source| source.table_access())
                    .is_some_and(|access| access.accept_column_prune(&offsets));
                if accepted {
                    if let Some(trace) = trace.as_deref_mut() {
                        trace.cop_table_projection(
                            traced_select.fields.fields(),
                            &Qualifier {
                                db: current_db,
                                scope: &scope,
                                catalog: Some(catalog),
                            },
                            logical_rows,
                        );
                    }
                }
                accepted
            }
            None => true,
        };
        if direct_ready {
            if trace.as_deref().is_some_and(PlanTrace::is_plan_only) {
                return Ok((columns, Vec::new()));
            }
            let types: Vec<FieldType> = columns
                .iter()
                .map(|(_, field_type)| field_type.clone())
                .collect();
            let source = from_source
                .take()
                .expect("a direct access plan installed its source");
            let rows = drain_executor_rows(source, &types, &ctx.statement_memory())?;
            return Ok((columns, rows));
        }
    }
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
            let surviving = crate::driver::access::surviving_partitions(
                select,
                crate::driver::access::sole_table_ref(&select.from),
                &table,
                &ctx.session_zone(),
            );
            let estimates =
                crate::driver::access::surviving_partition_estimates(catalog, &surviving);
            let names = surviving
                .iter()
                .map(|(name, _)| name.clone())
                .collect::<Vec<_>>();
            // Go `makeUnionAllChildren` builds NO union when pruning left
            // nothing -- it returns a `LogicalTableDual{RowCount: 0}` -- and
            // an unpartitioned table simply never reaches that rule. The two
            // reach `surviving` as the same empty vector, so the table's own
            // partitioning is what separates them.
            if names.is_empty() {
                if table.partition().is_some() {
                    trace.pruned_away_table_dual();
                }
            } else {
                trace.partition_union(&names, &estimates);
            }
        }
    }
    // An exact ordered handle range still has root work to do, so it cannot
    // take the early-return path above. When its ORDER BY reads only the
    // simple projected columns, the scan can nevertheless emit that narrow
    // schema from the cop task. Mutate the scope together with the real scan
    // offer so every expression built below resolves against the executor's
    // actual row layout.
    let mut cop_projection_ready = false;
    if let (Some(offsets), Some(source)) = (cop_projection_offsets, from_source.as_mut()) {
        if source
            .table_access()
            .is_some_and(|access| access.accept_column_prune(&offsets))
        {
            scope = crate::column_prune::pruned_scope(&scope, &offsets);
            if let Some(trace) = trace.as_deref_mut() {
                trace.cop_table_projection(
                    traced_select.fields.fields(),
                    &Qualifier {
                        db: current_db,
                        scope: &scope,
                        catalog: Some(catalog),
                    },
                    logical_rows,
                );
            }
            cop_projection_ready = true;
        }
    }
    let mut full_row_projection = projects_entire_single_table_in_order(select, &scope);

    // Column pruning: over a single base-table scan the fast paths left
    // alone, narrow the scan -- and with it the scope -- to the columns the
    // statement actually reads.
    let scope_before_prune = scope.clone();
    let general_prune_offsets = crate::column_prune::prunable_columns(select, &scope);
    prune_scan_columns(select, &mut scope, &mut from_source);
    if let Some(keep) = general_prune_offsets
        .as_deref()
        .filter(|keep| keep.len() < scope_before_prune.width() && scope.width() == keep.len())
    {
        if let Some(order) = index_order.as_mut() {
            order.remap_columns(keep);
        }
    }
    // A filtered cop projection is deliberately NOT offered as ordinary
    // column pruning: its residual predicate still needs columns outside the
    // final SELECT list. Translate the final projection into the wider,
    // generally-pruned scan layout and offer it only after the scan accepts
    // that predicate below.
    let filtered_cop_projection_offsets = filtered_cop_projection_offsets.and_then(|offsets| {
        let scan_offsets = match general_prune_offsets.as_ref() {
            Some(general) if scope.width() == general.len() => general,
            _ if scope.width() == scope_before_prune.width() => {
                return Some(offsets);
            }
            _ => return None,
        };
        offsets
            .into_iter()
            .map(|offset| scan_offsets.iter().position(|kept| *kept == offset))
            .collect()
    });

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
        let grouped_stream_physical_order = aggregation_order
            .as_ref()
            .and_then(|order| order.physical_group_offsets(&scope));
        let resolver = ScopeResolver { scope: &scope };
        let semantic_scope = aggregation_semantic_scope.as_ref().unwrap_or(&scope);
        return run_aggregate_select(
            select,
            traced_select,
            semantic_select,
            from_source,
            &resolver,
            semantic_scope,
            catalog,
            current_db,
            ctx,
            consumed_where,
            logical_rows,
            joined_logical_rows,
            grouped_logical_rows,
            grouped_stream_ordered,
            grouped_stream_physical_order,
            derived_output,
            grouped_derived_output_pruned || physical_source_names,
            aggregation_inputs_pushed,
            from_delivered.semi_join,
            aggregate_join_projection,
            from_delivered.candidate.clone().or(access_candidate),
            output_delivered.as_deref_mut(),
            deferred_exec.as_deref_mut(),
            trace,
        );
    }

    // `SELECT DISTINCT ... ORDER BY`, for the queries that never reach the
    // aggregate pipeline. The aggregate path runs the same check itself, after
    // ONLY_FULL_GROUP_BY, which is the order Go's two builders impose.
    only_full_group_by::check_order_by_in_distinct(select, &scope, ctx)?;

    // Source: the table rows (matrix- or TiKV-byte-backed), or one virtual row
    // from a table-dual.
    let mut source: Box<dyn Executor> = match from_source {
        Some(exec) => exec,
        None => {
            let exec: Box<dyn Executor> = Box::new(TableDualExec::new(
                ExecutorMeta::new(Schema::new(vec![]), 0, INIT_CAP, MAX_CHUNK_SIZE),
                1,
            ));
            match trace.as_deref_mut() {
                Some(trace) => trace.meter(exec),
                None => exec,
            }
        }
    };
    // The plan text quotes the statement as written, against the FROM scope
    // the driver just built.
    let filter_scope = scope.clone();
    let qualify = Qualifier {
        db: current_db,
        scope: &filter_scope,
        catalog: Some(catalog),
    };

    // Optional WHERE: a selection over the source rows. A correlated
    // subquery in the predicate first becomes an Apply below the selection,
    // appending the column the rewritten predicate reads (Go's plan shape).
    // The scope the rows above the WHERE have: the FROM tables, plus the
    // column a correlated WHERE subquery's Apply appends.
    // Predicate push-down: over a single base table, offer the source the
    // conjuncts it can apply itself; only the residual needs a `Selection`.
    let (executed_where, pushed_where) = negotiate_scan_filter(
        select,
        &scope,
        &mut source,
        ctx,
        consumed_where,
        trace.as_deref_mut(),
    );
    let cop_filtered_projection_ready = handle_range_residual.is_some()
        && executed_where.is_none()
        && select.limit.is_none()
        && !select.order_by.is_empty()
        && filtered_cop_projection_offsets
            .as_ref()
            .is_some_and(|offsets| {
                source
                    .table_access()
                    .is_some_and(|access| access.accept_post_filter_projection(offsets))
            });
    if cop_filtered_projection_ready {
        scope = crate::column_prune::pruned_scope(
            &scope,
            filtered_cop_projection_offsets
                .as_deref()
                .expect("a ready filtered projection has offsets"),
        );
    }
    let resolver = ScopeResolver { scope: &scope };
    let mut current_scope = scope.clone();
    // `keep order`: whether the source's own walk order is the answer's, which
    // is what decides whether an index lookup reorders its handle batch.
    let order_from_access = offer_keep_order(select, index_order.as_ref(), &resolver, &mut source);
    // A DESCENDING order is discharged only by a source that actually
    // REVERSES its walk -- Go reads the range backwards (`desc` on the
    // scan). The index sources do (see `IndexRangeSourceExec`); the table
    // scan walks the record keys forward only, so its handle-order claim
    // stands for ASC alone. Without this demotion a `WHERE id > 1 ORDER BY
    // id DESC LIMIT 2` dropped its sort, capped the FORWARD walk, and
    // answered the two SMALLEST ids.
    let descending_order = select.order_by.first().is_some_and(|item| item.desc);
    let order_satisfied = order_satisfied && (!descending_order || order_from_access);
    if order_satisfied && order_from_access {
        if let Some(limit) = select.limit.as_ref() {
            let count = eval_limit_bound(&limit.count)?;
            let offset = match &limit.offset {
                Some(offset) => eval_limit_bound(offset)?,
                None => 0,
            };
            // A residual filter can reject rows inside the lookup task. Keep
            // one bounded amount of read-ahead so a moderately selective
            // predicate normally fills the root window without serializing
            // several BatchGet round trips; the source still emits at most
            // what the unchanged root Limit consumes.
            let window = offset.saturating_add(count).saturating_mul(3);
            if window > 0 {
                let _ = source
                    .table_access()
                    .is_some_and(|access| access.accept_lookup_batch_size(window));
            }
        }
    }
    let residual_on_index = access_residual.as_ref().is_none_or(|residual| {
        index_order
            .as_ref()
            .is_some_and(|order| order.residual_uses_only_index(residual, &resolver))
    });
    // When the lookup residual is covered by the index, let the source ask
    // TiKV to evaluate it while producing handles. The row-level probe stays
    // installed for correctness if the backend refuses the request.
    let index_filter_ready = access_residual.is_some()
        && residual_on_index
        && source
            .table_access()
            .is_some_and(|access| access.accept_index_filter());
    // A non-covering lookup still materializes the table row before the
    // residual is evaluated. Go keeps the SELECT * Projection above that
    // lookup even when it is an identity projection; retaining it here also
    // keeps the row shape distinct from the covering/index-only paths.
    if access_residual.is_some() && !residual_on_index {
        full_row_projection = false;
    }
    if order_satisfied {
        if let Some(trace) = trace.as_deref_mut() {
            trace.keep_order(select.order_by.first().is_some_and(|item| item.desc));
        }
    }
    let embedded_lookup_limit = (order_satisfied && order_from_access && residual_on_index)
        .then(|| {
            offer_embedded_lookup_limit(
                select,
                if residual_on_index {
                    executed_where.as_ref()
                } else {
                    access_residual.as_ref().or(executed_where.as_ref())
                },
                index_order.as_ref(),
                &resolver,
                &mut source,
            )
        })
        .flatten();
    // LIMIT push-down: an ordered double read consumes its offset from the
    // handle stream. Other scans take the ordinary `offset + count` cap.
    let scan_limit_pushed = embedded_lookup_limit.is_some()
        || ((select.order_by.is_empty() || order_satisfied)
            && offer_scan_limit(
                select,
                if residual_on_index {
                    executed_where.as_ref()
                } else {
                    access_residual.as_ref().or(executed_where.as_ref())
                },
                index_order.as_ref(),
                &resolver,
                &mut source,
            ));

    // A `WHERE` whose conjuncts all moved into the scan still records its
    // `Selection`, over the predicate as written, and meters the filtered
    // rows the scan now emits.
    if !consumed_where && executed_where.is_none() && select.where_clause.is_some() {
        if let Some(trace) = trace.as_deref_mut() {
            if let Some(written) = &traced_select.where_clause {
                let predicate = access_residual
                    .as_ref()
                    .or(handle_range_residual.as_ref())
                    .unwrap_or(written);
                if access_residual.is_some() || handle_range_residual.is_some() {
                    let resolver = ScopeResolver {
                        scope: &filter_scope,
                    };
                    let mut physical = rewrite_expr_resolved(predicate, &resolver)
                        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                    refine_comparisons(&mut physical, ctx)
                        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                    trace.residual_selection(
                        predicate,
                        Some(std::slice::from_ref(&physical)),
                        &qualify,
                        logical_rows,
                        crate::driver::access::select_predicate_stats_selectivity_in_session(
                            select,
                            predicate,
                            catalog,
                            current_db,
                            &filter_scope,
                            ctx.default_string_match_selectivity(),
                        ),
                    );
                } else if grouped_derived_output_pruned {
                    let resolver = ScopeResolver {
                        scope: &filter_scope,
                    };
                    let mut physical = rewrite_expr_resolved(predicate, &resolver)
                        .map_err(|e| eval_error_in_clause(e, "where clause"))?;
                    refine_comparisons(&mut physical, ctx)
                        .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
                    if !trace.physical_selection(
                        &physical,
                        predicate,
                        select_stats_selectivity(select, catalog, current_db, &filter_scope),
                    ) {
                        trace.refuse(
                            "a pruned derived aggregation's Selection is not printable yet",
                        );
                    }
                } else {
                    trace.selection(
                        predicate,
                        Some(&pushed_where),
                        &qualify,
                        select_stats_selectivity(select, catalog, current_db, &filter_scope),
                    );
                }
                if cop_filtered_projection_ready
                    && !trace
                        .cop_selection_projection_reader(traced_select.fields.fields(), &qualify)
                {
                    trace.refuse("cop Selection/Projection child is not a bare table scan");
                }
                if !residual_on_index {
                    if !trace.lookup_probe_selection(logical_rows) {
                        trace.refuse("probe residual child is not an IndexLookUp");
                    } else if let Some(count) = select
                        .limit
                        .as_ref()
                        .and_then(|limit| eval_limit_bound(&limit.count).ok())
                    {
                        trace.cap_lookup_output(count);
                    }
                }
                source = trace.meter(source);
            }
        }
    }
    if let Some(predicate) = &executed_where {
        let decorrelated = decorrelate_exists::decorrelate_where(
            source,
            &current_scope,
            predicate,
            select,
            std::mem::replace(&mut from_delivered, from::Delivered::new()),
            catalog,
            current_db,
            ctx,
            joined_logical_rows,
            None,
            trace.as_deref_mut(),
        )?;
        source = decorrelated.source;
        current_scope = decorrelated.scope;
        let source_schema = source.schema().clone();
        from_delivered = decorrelated.delivered;
        if let Some(mut predicate) = decorrelated.residual {
            let selection_written = predicate.clone();
            let mut source_schema = source_schema;
            loop {
                let mut correlated = None;
                predicate = extract_correlated_subquery(
                    &predicate,
                    &current_scope,
                    catalog,
                    current_db,
                    current_scope.width(),
                    &mut correlated,
                    ctx,
                )?;
                let Some(correlated) = correlated else { break };
                (source, current_scope, source_schema) = append_correlated_apply(
                    source,
                    &current_scope,
                    correlated,
                    catalog,
                    current_db,
                    ctx,
                )?;
            }
            let predicate_resolver = ScopeResolver {
                scope: &current_scope,
            };
            let mut pred = rewrite_expr_resolved(&predicate, &predicate_resolver)
                .map_err(|e| eval_error_in_clause(e, "where clause"))?;
            refine_comparisons(&mut pred, ctx)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            let explained_where = trace.is_some().then(|| {
                let mut predicates = pushed_where;
                predicates.push(pred.clone());
                predicates
            });
            let physical_trace_predicate = physical_source_names.then(|| pred.clone());
            source = Box::new(SelectionExec::new(
                ExecutorMeta::new(source_schema, 1, INIT_CAP, MAX_CHUNK_SIZE),
                vec![pred],
                source,
                ctx.clone(),
                ctx.statement_memory(),
            ));
            if let Some(trace) = trace.as_deref_mut() {
                // An Apply below this selection (a correlated subquery in the
                // WHERE) adds an executor the recorder has never printed, so it
                // stays out of the trace rather than changing the shape EXPLAIN
                // reports.
                let stats = physical_source_names
                    .then_some(crate::plan_trace::SELECTIVITY_FACTOR)
                    .or_else(|| {
                        select_stats_selectivity(select, catalog, current_db, &filter_scope)
                    });
                if let Some(predicate) = &physical_trace_predicate {
                    let column_names =
                        physical_source_column_names(select, &current_scope, catalog, current_db);
                    if !trace.physical_selection_with_columns(
                        predicate,
                        &selection_written,
                        stats,
                        &column_names,
                    ) {
                        trace.refuse(
                            "a pruned derived aggregation's Selection is not printable yet",
                        );
                    }
                } else {
                    trace.selection(
                        &selection_written,
                        explained_where.as_deref(),
                        &qualify,
                        stats,
                    );
                }
                source = trace.meter(source);
            }
        }
    }

    // A covering scan whose access ranges consumed the whole predicate is
    // already the cop task Go places below IndexReader/TableReader. Record
    // that boundary before root sorting and projection are added.
    if reader_ready {
        if let Some(trace) = trace.as_deref_mut() {
            trace.scan_reader();
        }
    }

    let mut limit_before_projection = false;
    let mut select_lock_traced = false;
    if let Some((offset, count)) = embedded_lookup_limit {
        if let Some(trace) = trace.as_deref_mut() {
            if !trace.embedded_lookup_limit(offset, count, logical_rows) {
                trace.refuse("embedded ordered Limit child is not an IndexLookUp");
            }
        }
        limit_before_projection = true;
        if let Some((offsets, fields)) = ordered_lookup_projection(select, &current_scope) {
            let inner_resolver = ScopeResolver {
                scope: &current_scope,
            };
            let expressions = fields
                .iter()
                .map(|field| match field {
                    SelectField::Expr { expr, .. } => rewrite_expr_resolved(expr, &inner_resolver),
                    SelectField::Wildcard(_) => unreachable!("projection gate rejects wildcards"),
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| eval_error_in_clause(error, "field list"))?;
            let columns = expressions
                .iter()
                .enumerate()
                .map(|(index, expression)| {
                    let field_type = expression
                        .static_type()
                        .cloned()
                        .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                    let mut column = Column::new(index as i64 + 1, field_type);
                    column.index = index as i64;
                    column
                })
                .collect();
            source = Box::new(ProjectionExec::new(
                ExecutorMeta::new(Schema::new(columns), 2, INIT_CAP, MAX_CHUNK_SIZE),
                expressions,
                source,
                ctx.clone(),
            ));
            current_scope = crate::column_prune::pruned_scope(&current_scope, &offsets);
            if let Some(trace) = trace.as_deref_mut() {
                if select.lock.is_some() {
                    trace.select_lock();
                    select_lock_traced = true;
                }
                trace.projection(&fields, &qualify);
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
    let visible_fields = select.fields.clone();
    let window_rewritten;
    let select = if crate::window::select_has_window(select) {
        let calls = crate::window::collect_window_calls(select)?;
        let source_types: Vec<FieldType> = current_scope
            .column_list()
            .into_iter()
            .map(|(_, field_type)| field_type)
            .collect();
        let rows = drain_executor_rows(source, &source_types, &ctx.statement_memory())?;
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
        let SelectField::Expr {
            expr: visible_expr, ..
        } = &visible_fields.fields()[field_index]
        else {
            unreachable!("window rewriting preserves the select-field shape")
        };
        let name = alias.clone().unwrap_or_else(|| {
            default_field_display_name(&visible_fields, field_index, visible_expr)
        });
        let mut rewritten = expr.clone();
        loop {
            let mut correlated = None;
            rewritten = extract_correlated_subquery(
                &rewritten,
                &current_scope,
                catalog,
                current_db,
                current_scope.width(),
                &mut correlated,
                ctx,
            )?;
            let Some(correlated) = correlated else { break };
            let (applied, widened_scope, _) = append_correlated_apply(
                source,
                &current_scope,
                correlated,
                catalog,
                current_db,
                ctx,
            )?;
            source = applied;
            current_scope = widened_scope;
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
                    .map_err(|e| eval_error_in_clause(e, "field list"))?;
                refine_comparisons(&mut rewritten, ctx)
                    .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
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

    // A plain projection preserves each input order through its bare-column
    // outputs. Report that ACTUAL mapping to a parent join so its merge-plan
    // verification reads the executor just built rather than a catalog
    // promise. Clauses with their own physical ordering semantics stay
    // fail-closed here, matching `merge_decision::order_preserving_source`.
    let projection_sources = exprs
        .iter()
        .map(|expr| {
            expr.as_column()
                .and_then(|column| usize::try_from(column.index).ok())
        })
        .collect::<Vec<_>>();
    if (!select.distinct || distinct_eliminated)
        && select.group_by.is_empty()
        && select.having.is_none()
        && select.order_by.is_empty()
        && select.limit.is_none()
        && select.windows.is_empty()
    {
        if let Some(delivered) = output_delivered.as_deref_mut() {
            delivered.extend(crate::driver::merge_decision::project_delivered_orders(
                &from_delivered,
                &projection_sources,
            ));
        }
    }
    // Go folds an increasing bare-column subset into the child join's pruned
    // output schema. ProjectionExec remains this executor's implementation of
    // that pruning, but it is not a separate physical Projection in Go's
    // plan tree.
    let order_by_covered_by_projection = select.order_by.iter().all(|item| {
        substitute_output_aliases(&item.expr, &projected_fields, true)
            .ok()
            .and_then(|resolved| rewrite_expr_resolved(&resolved, &resolver).ok())
            .and_then(|expression| {
                expression
                    .as_column()
                    .and_then(|column| usize::try_from(column.index).ok())
            })
            .is_some_and(|offset| projection_sources.contains(&Some(offset)))
    });
    let simple_projection = (!select.distinct || distinct_eliminated)
        && select.group_by.is_empty()
        && select.having.is_none()
        && order_by_covered_by_projection
        && select.limit.is_none()
        && select.windows.is_empty();
    let direct_column_projection = projection_sources.iter().all(Option::is_some)
        && projection_sources
            .iter()
            .flatten()
            .copied()
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            == projection_sources.len();
    let logical_direct_column_projection = select.fields.fields().iter().all(|field| {
        matches!(
            field,
            SelectField::Expr {
                expr: tidb_ast::Expr::Column(_),
                ..
            }
        )
    }) || matches!(
        select.fields.fields(),
        [SelectField::Wildcard(qualifier)]
            if qualifier.last().is_none()
                && projection_sources.len() == current_scope.width()
                && projection_sources
                    .iter()
                    .enumerate()
                    .all(|(offset, source)| *source == Some(offset))
    );
    let derived_column_prune = derived_output
        && simple_projection
        && direct_column_projection
        && projection_sources.iter().all(Option::is_some)
        && projection_sources.windows(2).all(|pair| {
            pair[0]
                .zip(pair[1])
                .is_some_and(|(left, right)| left < right)
        });
    // LogicalJoin.PruneColumns narrows the join schema to direct parent
    // columns, and its InlineProjection makes the remaining physical
    // projection an identity. The executor still uses ProjectionExec to map
    // its positional row, but Go does not print a separate operator for it.
    let joined_column_prune = current_scope.tables.len() > 1
        && simple_projection
        && direct_column_projection
        && logical_direct_column_projection
        && !inlined_computed_output
        && restored_join_output.as_ref().is_none_or(|restored| {
            projection_sources.len() == restored.len()
                && projection_sources
                    .iter()
                    .zip(restored)
                    .all(|(projected, restored)| {
                        *projected == crate::driver::from::scope_offset_of(&current_scope, restored)
                    })
        });
    let logical_column_prune =
        !inlined_computed_output && (derived_column_prune || joined_column_prune);
    // A derived SELECT whose DISTINCT and Projection were both eliminated has
    // no physical operator above its access path. Preserve that complete task
    // for a parent join's cost comparison. AccessPathCommit already costs its
    // complete WHERE even when the executable Selection remains above the scan.
    if let Some(delivered) = output_delivered.as_deref_mut() {
        delivered.candidate = logical_column_prune
            .then(|| from_delivered.candidate.clone().or_else(|| access_candidate.clone()))
            .flatten();
        // A COMPUTED simple projection is Go's `PhysicalProjection` over the
        // child task, priced by `getPlanCostVer24PhysicalProjection` -- child
        // cost plus a per-row expression CPU term. Delivering it lets the
        // parent join PRICE the site: without a receipt, `build_join`'s
        // `unpriced_merge` arm keeps a structural merge preference, and a
        // committed merge then forces an index join by elimination below
        // (measured: `planner/core/join_reorder_through_projection`'s five
        // recorded divergences, all of them `dt`-shaped derived tables of
        // exactly this projection). `simple_projection` plus an empty ORDER
        // BY bounds the claim to selects whose ONLY root operator is that
        // projection -- a Sort, Limit, Aggregation or DISTINCT above it would
        // make this receipt understate the task.
        if delivered.candidate.is_none()
            && !logical_column_prune
            && simple_projection
            && select.order_by.is_empty()
        {
            if let Some(child) = from_delivered.candidate.clone().or_else(|| access_candidate.clone())
            {
                let input_rows = tidb_planner::candidate_cost::evaluate(
                    &child,
                    &tidb_planner::candidate_cost::CostEnv::default(),
                    tidb_planner::task_type::TaskType::Root,
                )
                .rows;
                delivered.candidate = Some(tidb_planner::candidate_cost::Candidate::Projection {
                    child: Box::new(child),
                    input_rows,
                    exprs: exprs
                        .iter()
                        .map(|expr| matches!(expr, Expression::ScalarFunction(_)))
                        .collect(),
                });
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
    let direct_distinct_candidate = if select.distinct && !distinct_eliminated && exprs.len() == 1 {
        exprs[0].as_column().map(|_| exprs[0].clone())
    } else {
        None
    };
    let mut deferred_distinct_sort: Option<Vec<SortByItem>> = None;

    // ORDER BY: a sort below the projection, with by-items resolved against
    // the SELECT list first and the SOURCE schema second -- Go's own
    // resolution order, which is why ordering by a column that is not
    // projected still works while an alias shadows one that is.
    //
    // Whether the `LIMIT` below was already consumed by a fused `TopN`.
    let mut fused_topn = false;
    if !select.order_by.is_empty() && !order_satisfied {
        let mut by_items = Vec::with_capacity(select.order_by.len());
        for item in &select.order_by {
            let resolved = substitute_output_aliases(&item.expr, &projected_fields, true)?;
            let mut expr = rewrite_expr_resolved(&resolved, &resolver).map_err(|e| {
                order_by_column_error(&resolved).unwrap_or(DriverError::Exec(ExecError::Eval(e)))
            })?;
            refine_comparisons(&mut expr, ctx)
                .map_err(|e| DriverError::Exec(ExecError::Eval(e)))?;
            by_items.push(SortByItem {
                expr,
                desc: item.desc,
            });
        }
        let defer_sort = direct_distinct_candidate
            .as_ref()
            .and_then(Expression::as_column)
            .is_some_and(|selected| {
                by_items.iter().all(|item| {
                    item.expr
                        .as_column()
                        .is_some_and(|order| order.index == selected.index)
                })
            });
        if defer_sort {
            deferred_distinct_sort = Some(
                by_items
                    .iter()
                    .map(|item| SortByItem {
                        expr: Expression::Column(out_schema.columns[0].clone()),
                        desc: item.desc,
                    })
                    .collect(),
            );
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
        // A direct one-column `SELECT DISTINCT` defers this ordering until
        // after its HashAgg exists below. That is where Go attaches TopN: the
        // aggregate must deduplicate before the bounded ordering discards any
        // rows.
        let fused_limit = if select.distinct && !distinct_eliminated {
            None
        } else {
            select.limit.as_ref()
        };
        if defer_sort {
            // Go places this Sort above buildDistinct's HashAgg. It is built
            // below after the direct distinct executor exists.
        } else if let Some(limit) = fused_limit {
            let count = eval_limit_bound(&limit.count)?;
            let offset = match &limit.offset {
                Some(expr) => eval_limit_bound(expr)?,
                None => 0,
            };
            let remote_topn_ready = offset.checked_add(count).is_some_and(|cap| {
                let order_by: Option<Vec<PushdownTopNOrder>> = by_items
                    .iter()
                    .map(|item| {
                        Some(PushdownTopNOrder {
                            offset: usize::try_from(item.expr.as_column()?.index).ok()?,
                            desc: item.desc,
                        })
                    })
                    .collect();
                let Some(order_by) = order_by else {
                    return false;
                };
                source.table_access().is_some_and(|access| {
                    access.accept_remote_topn(&PushdownTopN {
                        order_by,
                        limit: cap,
                    })
                })
            });
            // A non-covering index lookup has its own Go shape:
            // `TopN(Build) -> Selection -> IndexRangeScan` under the lookup.
            // The source retains the equivalent local root TopN when the
            // backend cannot expose a cop request, but the physical contract
            // and result bound are still the same.
            let index_topn_ready =
                if !order_satisfied && index_filter_ready && index_order.is_some() {
                    let order_by = by_items
                        .iter()
                        .map(|item| {
                            Some((
                                usize::try_from(item.expr.as_column()?.index).ok()?,
                                item.desc,
                            ))
                        })
                        .collect::<Option<Vec<_>>>();
                    order_by.is_some_and(|order_by| {
                        offset.checked_add(count).is_some_and(|cap| {
                            source
                                .table_access()
                                .is_some_and(|access| access.accept_index_top_n(&order_by, cap))
                        })
                    })
                } else {
                    false
                };
            if remote_topn_ready || index_topn_ready {
                let cap = offset
                    .checked_add(count)
                    .expect("a ready remote TopN has a bounded cap");
                source = Box::new(TopNExec::new(
                    ExecutorMeta::new(sort_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
                    by_items.clone(),
                    source,
                    ctx.clone(),
                    0,
                    cap,
                    ctx.statement_memory(),
                ));
                if let Some(trace) = trace.as_deref_mut() {
                    let pushed = if index_topn_ready {
                        trace.pushed_topn_lookup(
                            &traced_select.order_by,
                            &qualify,
                            offset,
                            count,
                            logical_rows,
                        )
                    } else {
                        trace.pushed_topn_reader(&traced_select.order_by, &qualify, cap)
                    };
                    if pushed {
                        source = trace.meter(source);
                    } else {
                        trace.refuse("cop TopN child is not a table scan or pushed Selection");
                    }
                }
            }
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
                if !index_topn_ready {
                    trace.topn(&traced_select.order_by, &qualify, offset, count);
                }
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

    // An ordered IndexLookUp embeds the complete SQL window and has no root
    // Limit. Table and covering-index scans keep the equivalent root Limit
    // above their pushed `offset + count` cap.
    if order_satisfied && scan_limit_pushed && (!select.distinct || distinct_eliminated) {
        if let Some(limit) = select.limit.as_ref() {
            let count = eval_limit_bound(&limit.count)?;
            let offset = match &limit.offset {
                Some(expr) => eval_limit_bound(expr)?,
                None => 0,
            };
            if embedded_lookup_limit.is_none() {
                let cap = offset.saturating_add(count);
                let limit_schema = source.schema().clone();
                source = Box::new(LimitExec::new(
                    ExecutorMeta::new(limit_schema, 4, INIT_CAP, MAX_CHUNK_SIZE),
                    offset,
                    count,
                    source,
                ));
                if let Some(trace) = trace.as_deref_mut() {
                    if !trace.pushed_limit_reader(0, cap) {
                        trace.refuse("pushed ordered Limit child is not a bare table scan");
                    }
                    trace.limit(offset, count);
                    source = trace.meter(source);
                }
                limit_before_projection = true;
            }
        }
    }

    // The cluster-session transaction seam collects the raw keys consumed by
    // a locking read and issues their TiKV pessimistic lock. It is transparent
    // to row values, so the executor chain needs no row-transforming wrapper,
    // but the physical plan retains Go's SelectLock at this point.
    if select.lock.is_some() && !select_lock_traced {
        if let Some(trace) = trace.as_deref_mut() {
            trace.select_lock();
        }
    }

    let direct_distinct_input = direct_distinct_candidate
        .filter(|_| select.order_by.is_empty() || deferred_distinct_sort.is_some());

    let partial_distinct = direct_distinct_input.as_ref().is_some_and(|input| {
        let Some(input_offset) = input
            .as_column()
            .and_then(|column| usize::try_from(column.index).ok())
        else {
            return false;
        };
        let aggregate = PushdownPartialAggregate::GroupBy {
            input_offset,
            output_type: out_schema.columns[0]
                .ret_type
                .clone()
                .expect("distinct output has a type"),
        };
        source
            .table_access()
            .is_some_and(|access| access.accept_partial_aggregate(&aggregate, ctx))
    });

    // A direct one-column DISTINCT groups the source expression itself. Go
    // absorbs its FIRST_ROW output projection into HashAgg, and places a
    // valid ORDER BY on that output above the aggregate.
    let projection_elision_candidate =
        cop_projection_ready || cop_filtered_projection_ready || full_row_projection;
    // A correlated subquery in HAVING appends its Apply result to the source
    // row after the identity-projection decision above was made.  The final
    // projection is no longer an identity in that case: it has to trim the
    // private Apply column before the row reaches the client.  Keep elision
    // fail-closed on the physical output width so Row::GetDatumRow retains
    // Go's one-field-type-per-chunk-column invariant.
    let projection_elided =
        projection_elision_candidate && source.schema().columns.len() == out_schema.columns.len();
    // Only EXPLAIN needs a second view of the executable expression tree.
    // Ordinary execution moves the sole copy into ProjectionExec, so the
    // TPCC hot path pays no clone for physical-expression rendering.
    let projection_trace_exprs = (trace.is_some()
        && direct_distinct_input.is_none()
        && !projection_elided
        && !logical_column_prune)
        .then(|| exprs.clone());
    let projection_trace_columns = projection_trace_exprs.as_ref().map(|_| {
        (0..current_scope.width())
            .map(|offset| {
                let path = current_scope.qualified_path(offset)?;
                let [relation, column] = path.as_slice() else {
                    return None;
                };
                let key = crate::driver::merge_decision::RelColumn {
                    relation: relation.clone(),
                    column: column.clone(),
                };
                let from = select.from.as_ref()?;
                crate::driver::merge_decision::physical_column_trace_name(
                    &from.left, &key, catalog, current_db,
                )
                .or_else(|| {
                    from.right.as_ref().and_then(|right| {
                        crate::driver::merge_decision::physical_column_trace_name(
                            right, &key, catalog, current_db,
                        )
                    })
                })
            })
            .collect::<Vec<_>>()
    });
    let mut root: Box<dyn Executor> = if let Some(input) = direct_distinct_input.as_ref() {
        let input = if partial_distinct {
            Expression::Column(source.schema().columns[0].clone())
        } else {
            input.clone()
        };
        let aggregate = HashAggExec::new(
            ExecutorMeta::new(out_schema.clone(), 5, INIT_CAP, MAX_CHUNK_SIZE),
            vec![input.clone()],
            vec![AggFunc::new(AggKind::FirstRow, Some(input.clone()))],
            source,
            ctx.clone(),
            ctx.statement_memory(),
        );
        let mut aggregate: Box<dyn Executor> = Box::new(aggregate);
        if let Some(trace) = trace.as_deref_mut() {
            if partial_distinct {
                if !trace.partial_hash_agg(
                    traced_select.fields.fields(),
                    &qualify,
                    distinct_logical_rows,
                ) {
                    trace.refuse("partial HashAgg child is not a bare table scan");
                }
            } else {
                trace.scan_reader();
            }
            if partial_distinct {
                trace.final_distinct(traced_select.fields.fields(), &qualify);
            } else if physical_source_names {
                let column_names =
                    physical_source_column_names(select, &current_scope, catalog, current_db);
                if !trace.physical_distinct(
                    std::slice::from_ref(&input),
                    &column_names,
                    distinct_logical_rows,
                ) {
                    trace.refuse("a projection-eliminated DISTINCT is not printable yet");
                }
            } else {
                trace.distinct(
                    traced_select.fields.fields(),
                    &qualify,
                    distinct_logical_rows,
                );
            }
            aggregate = trace.meter(aggregate);
        }
        aggregate
    } else if projection_elided {
        source
    } else {
        Box::new(ProjectionExec::new(
            ExecutorMeta::new(out_schema.clone(), 2, INIT_CAP, MAX_CHUNK_SIZE),
            exprs,
            source,
            ctx.clone(),
        ))
    };
    if direct_distinct_input.is_none() && !projection_elided && !logical_column_prune {
        if let Some(trace) = trace.as_deref_mut() {
            let physical = projection_trace_exprs
                .as_deref()
                .is_some_and(|expressions| {
                    trace.physical_real_projection(
                        expressions,
                        projection_trace_columns.as_deref().unwrap_or(&[]),
                        reader_ready.then_some(logical_rows).flatten(),
                    )
                });
            if !physical {
                if reader_ready {
                    trace.projection_at_rows(traced_select.fields.fields(), &qualify, logical_rows);
                } else {
                    trace.projection(traced_select.fields.fields(), &qualify);
                }
            }
            root = trace.meter(root);
        }
    }

    // SELECT DISTINCT: Go `buildDistinct` builds an aggregation grouping by
    // every projected column, with a FIRST_ROW aggregate per column, which is
    // exactly a deduplication. It sits above the projection and below LIMIT.
    if select.distinct && !distinct_eliminated && direct_distinct_input.is_none() {
        let all: Vec<usize> = (0..out_schema.columns.len()).collect();
        root = Box::new(distinct_over(root, &out_schema, &all, ctx));
        if let Some(trace) = trace.as_deref_mut() {
            trace.distinct(
                traced_select.fields.fields(),
                &qualify,
                distinct_logical_rows,
            );
            root = trace.meter(root);
        }
    }

    if let Some(by_items) = deferred_distinct_sort {
        if let Some(limit) = select.limit.as_ref() {
            let count = eval_limit_bound(&limit.count)?;
            let offset = match &limit.offset {
                Some(expr) => eval_limit_bound(expr)?,
                None => 0,
            };
            root = Box::new(TopNExec::new(
                ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
                by_items,
                root,
                ctx.clone(),
                offset,
                count,
                ctx.statement_memory(),
            ));
            fused_topn = true;
            if let Some(trace) = trace.as_deref_mut() {
                trace.topn(&traced_select.order_by, &qualify, offset, count);
                root = trace.meter(root);
            }
        } else {
            root = Box::new(SortExec::new(
                ExecutorMeta::new(out_schema.clone(), 3, INIT_CAP, MAX_CHUNK_SIZE),
                by_items,
                root,
                ctx.clone(),
                ctx.statement_memory(),
            ));
            if let Some(trace) = trace.as_deref_mut() {
                trace.sort(&traced_select.order_by, &qualify);
                root = trace.meter(root);
            }
        }
    }

    // LIMIT [offset,] count: both bounds must be non-negative integer literals
    // (as in SQL; Go validates the same in the planner). A fused `TopN`
    // already applied this window.
    if let Some(limit) = select
        .limit
        .as_ref()
        .filter(|_| !fused_topn && !limit_before_projection)
    {
        let count = eval_limit_bound(&limit.count)?;
        let offset = match &limit.offset {
            Some(expr) => eval_limit_bound(expr)?,
            None => 0,
        };
        let limit_schema = root.schema().clone();
        root = Box::new(LimitExec::new(
            ExecutorMeta::new(limit_schema, 4, limit_init_cap(count), MAX_CHUNK_SIZE),
            offset,
            count,
            root,
        ));
        if let Some(trace) = trace.as_deref_mut() {
            trace.limit(offset, count);
            root = trace.meter(root);
        }
    }

    if let Some(deferred) = deferred_exec {
        *deferred = Some(root);
        return Ok((names.into_iter().zip(ret_types).collect(), Vec::new()));
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
        next_executor(root.as_mut(), &mut req, &ctx.statement_memory())?;
        let n = req.num_rows();
        if n == 0 {
            break;
        }
        for r in 0..n {
            rows.push(req.get_row(r).get_datum_row(&ret_types));
        }
    }
    root.close()?;
    let columns = names.into_iter().zip(ret_types).collect();
    Ok((columns, rows))
}

/// Source-table identities behind a derived relation's physical columns.
/// Aggregate and computed outputs deliberately remain `None`, so EXPLAIN
/// prints their internal `Column#N` identity while projection-only columns
/// retain the same base-table names as Go's logical `Column.UniqueID`.
fn physical_source_column_names(
    select: &tidb_ast::SelectStmt,
    scope: &FromScope,
    catalog: &Catalog,
    current_db: &str,
) -> Vec<Option<String>> {
    let Some(from) = &select.from else {
        return vec![None; scope.width()];
    };
    (0..scope.width())
        .map(|offset| {
            let path = scope.qualified_path(offset)?;
            let [.., relation, column] = path.as_slice() else {
                return None;
            };
            let column = crate::driver::merge_decision::RelColumn {
                relation: relation.clone(),
                column: column.clone(),
            };
            crate::driver::merge_decision::physical_column_trace_name(
                &from.left, &column, catalog, current_db,
            )
            .or_else(|| {
                from.right.as_ref().and_then(|right| {
                    crate::driver::merge_decision::physical_column_trace_name(
                        right, &column, catalog, current_db,
                    )
                })
            })
        })
        .collect()
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
    /// The PHYSICAL table behind `name`, when `name` is an alias.
    ///
    /// Resolution and PRINTING want different identities, and conflating them
    /// printed aliases where Go never does. An alias replaces the whole path
    /// for resolution (`db.t.col` stops naming an aliased `t`), but Go's
    /// `Column` carries `OrigName`/`OrigTblName` -- the base table -- and
    /// every `ExplainInfo` prints THAT: TPC-H q7's `nation n1, nation n2`
    /// records as `tpch50.nation.n_name` for both sides
    /// (`tests/integrationtest/r/tpch.result:419`), while the access object
    /// still shows `table:n1`. `None` when the visible name IS the table's
    /// own name, or when the relation has no single base table (a derived
    /// table, a synthetic scope).
    pub(crate) physical: Option<String>,
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
    memory: &crate::StatementMemory,
) -> Result<Vec<Vec<Datum>>, DriverError> {
    exec.open()?;
    let mut rows = Vec::new();
    let mut req = exec.new_chunk();
    loop {
        next_executor(exec.as_mut(), &mut req, memory)?;
        let n = req.num_rows();
        if n == 0 {
            break;
        }
        for r in 0..n {
            rows.push(req.get_row(r).get_datum_row(types));
        }
    }
    exec.close()?;
    Ok(rows)
}

fn next_executor(
    exec: &mut dyn Executor,
    req: &mut tidb_chunk::chunk::Chunk,
    memory: &crate::StatementMemory,
) -> Result<(), DriverError> {
    memory.check()?;
    exec.next(req)?;
    memory.check()?;
    Ok(())
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

    /// The label of the one field of `sql`.
    fn label_of(sql: &str) -> String {
        let ctx = crate::StmtContext::for_query();
        let (columns, _) = run_select_meta_on(sql, &Catalog::default(), &ctx).unwrap();
        columns[0].0.clone()
    }

    /// Go's `types.KindString` arm: a string literal names its column by its
    /// VALUE, not by the text it was written with, and leading non-graphic
    /// characters are trimmed (`strings.TrimLeftFunc` over `mysql.RangeGraph`).
    #[test]
    fn a_string_literal_column_is_named_by_its_decoded_value() {
        // The quotes go, and an escape is decoded: `'\N'` is the value `N`.
        assert_eq!(label_of("select 'abc'"), "abc");
        assert_eq!(label_of(r"select '\N'"), "N");
        assert_eq!(label_of(r#"select "\N""#), "N");
        // Leading whitespace and controls are trimmed; TRAILING ones are not.
        assert_eq!(label_of(r"select '\t   col'"), "col");
        assert_eq!(label_of(r"select ' \r\n  .col'"), ".col");
        assert_eq!(label_of("select 'abc   '"), "abc   ");
        assert_eq!(label_of("select '  abc   123   '"), "abc   123   ");
        // Multi-byte graphic characters survive the trim, both letters (`Lo`)
        // and symbols (`So`), which are both members of `mysql.RangeGraph`.
        assert_eq!(label_of(r"select '\n\t   中文 col'"), "中文 col");
        assert_eq!(label_of("select '   \u{1f606}col'"), "\u{1f606}col");
        assert_eq!(label_of("select '\u{200b}col'"), "col");
        // The value rule is reached through parentheses and a unary plus.
        assert_eq!(label_of("select (('abc'))"), "abc");
        assert_eq!(label_of(r#"select +"aaa""#), "aaa");
        // Adjacent literals concatenate as a value but retain the decoded
        // byte length of the first token for the result-column name.
        assert_eq!(label_of("select 'a' 'b'"), "a");
        assert_eq!(label_of("select 'é' 'x'"), "é");
    }

    /// Go's `default` arm: a numeric literal keeps its SOURCE TEXT, with the
    /// unary-plus/parenthesis wrapper trimmed from both ends -- so the trim,
    /// not a re-render of the value, is what makes `+0.999` into `0.999`.
    #[test]
    fn a_numeric_literal_column_is_named_by_its_trimmed_source_text() {
        assert_eq!(label_of("select +1"), "1");
        assert_eq!(label_of("select +0"), "0");
        assert_eq!(label_of("select +0.999"), "0.999");
        // A unary MINUS is not stripped by `getInnerFromParenthesesAndUnaryPlus`,
        // so `+(-9)` is not a literal at all and keeps its whole text.
        assert_eq!(label_of("select +(-9)"), "+(-9)");
        assert_eq!(label_of("select +(-0.001)"), "+(-0.001)");
    }

    /// Go's `types.KindInt64` arm reads `mysql.IsBooleanFlag`: the `TRUE` and
    /// `FALSE` keywords are named by their VALUE, uppercased, rather than by
    /// the text. Read from `buildProjectionFieldNameFromExpressions`; no
    /// recorded `.result` in the 257-topic corpus selects a bare boolean
    /// keyword, so the Go source is the only oracle for this arm.
    #[test]
    fn a_boolean_keyword_column_is_named_true_or_false() {
        assert_eq!(label_of("select true"), "TRUE");
        assert_eq!(label_of("select FaLsE"), "FALSE");
        // A plain `1` carries no boolean flag and takes the text arm.
        assert_eq!(label_of("select 1"), "1");
    }

    /// Go's `types.KindBinaryLiteral` arm: "Don't rewrite BIT literal or HEX
    /// literals" -- the source text is kept exactly, with no trim.
    #[test]
    fn a_hex_or_bit_literal_column_keeps_its_source_text() {
        assert_eq!(label_of("select 0x41"), "0x41");
        assert_eq!(label_of("select b'101'"), "b'101'");
    }

    /// Go: `NAME_CONST(name, value)` names its column by `name`, which MySQL
    /// documents as the whole point of the function.
    #[test]
    fn name_const_names_its_column_by_its_first_argument() {
        assert_eq!(label_of("select name_const('test_int', 1)"), "test_int");
        assert_eq!(label_of("SELECT NAME_CONST('come', -1)"), "come");
    }

    /// Go's `unaryOpToExpression`: "expression (+ a) is equal to a", and
    /// parentheses vanish the same way, so both are named like the bare
    /// column.
    #[test]
    fn a_parenthesized_or_unary_plus_column_is_named_like_the_column() {
        let mut catalog = Catalog::default();
        crate::run_create_table_on("CREATE TABLE t (a INT)", &mut catalog).unwrap();
        let ctx = crate::StmtContext::for_query();
        for sql in [
            "select (a) from t",
            "select + a from t",
            "select all + a from t",
        ] {
            let (columns, _) = run_select_meta_on(sql, &catalog, &ctx).unwrap();
            assert_eq!(columns[0].0, "a", "{sql}");
        }
    }

    /// The literal arms must read what the SOURCE WROTE, not what the tree
    /// holds by the time the name is chosen: this tier substitutes a
    /// variable's value and folds a scalar subquery INTO the field's
    /// expression, and both of those land a literal where the user wrote
    /// something else. Both cases below were measured as regressions when the
    /// rewritten expression was asked instead of
    /// `SelectFieldList::written_literal`.
    #[test]
    fn a_folded_literal_is_not_a_written_literal() {
        // `(select 1)` folds to the literal `1`; naming it by the `default`
        // arm's trim would have produced `select 1`.
        assert_eq!(label_of("select (select 1)"), "(select 1)");
    }
}
