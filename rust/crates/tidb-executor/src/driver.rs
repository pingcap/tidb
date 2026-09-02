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
//! Storage-backed reads use [`TableScanExec`] and [`crate::remote_scan`];
//! [`MemTableSourceExec`] remains the source for catalog-backed in-memory
//! tables.

use crate::executor::{ExecError, Executor, ExecutorMeta};
use crate::kv_table::{KvTable, TableHandle};
use crate::mem_quota;
use std::collections::HashMap;
use std::sync::Arc;
use tidb_ast::{JoinNode, QueryStmt, SelectField, SelectFieldList, Stmt};
use tidb_datatype::{Datum, FieldType, FieldTypeCode};
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
mod catalog;
pub(crate) use catalog::sync_load;
mod dml;
mod errors;
mod from;
mod index_usage_reporter;
pub mod infoschema_meta;
mod multi_dml;
mod params;
pub(crate) mod physical_builder;
pub(crate) mod planner_bridge;
pub(crate) mod point_get_key;
pub(crate) mod set_opr;
mod subquery;
#[cfg(test)]
mod tests;
pub(crate) mod write_cast;

// Re-exported flat, so every caller inside and outside this module keeps
// naming these as `driver::…` exactly as before the split.
pub(crate) use access::*;
pub use catalog::*;
pub use dml::*;
pub(crate) use from::*;
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

    let query = match &stmt {
        Stmt::Query(query) => query.as_ref(),
        _ => return Err(DriverError::unsupported("only SELECT is supported")),
    };
    run_query_stmt(query, catalog, current_db, ctx)
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
    set_opr::validate_query_usage(query)?;
    let mut physical = optimize_query_stmt(query, catalog, current_db, ctx)?;
    physical_builder::execute_query(query, &mut physical, catalog, ctx)
}

/// Go `planner.optimize`: try the complete fast physical plan first, then
/// build and optimize the ordinary logical tree exactly once.
pub(crate) fn optimize_query_stmt(
    query: &QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_planner::physical::PhysicalPlan, DriverError> {
    if let QueryStmt::Select(select) = query {
        if let Some(plan) = access::try_fast_point_physical_plan(select, catalog, current_db, ctx)?
        {
            return Ok(plan);
        }
    }
    planner_bridge::physical_query_plan(query, catalog, current_db, ctx)
        .map_err(planner_error_to_driver)
}

pub(super) fn run_physical_set_opr_stmt(
    set_opr: &tidb_ast::SetOprStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_query_stmt(
        &QueryStmt::SetOpr(Box::new(set_opr.clone())),
        catalog,
        current_db,
        ctx,
    )
}

pub(super) fn planner_error_to_driver(error: tidb_planner::plan_base::PlanError) -> DriverError {
    match error.kind() {
        tidb_planner::plan_base::PlanErrorKind::UnknownDatabase(database) => {
            DriverError::Schema(SchemaErrorKind::UnknownDatabase(database.clone()))
        }
        tidb_planner::plan_base::PlanErrorKind::UnknownTable(table) => {
            DriverError::Schema(SchemaErrorKind::UnknownTable(table.clone()))
        }
        tidb_planner::plan_base::PlanErrorKind::UnknownPartition { partition, table } => {
            DriverError::UnknownPartition {
                partition: partition.clone(),
                table: table.clone(),
            }
        }
        tidb_planner::plan_base::PlanErrorKind::PartitionClauseOnNonpartitioned => {
            DriverError::PartitionClauseOnNonpartitioned
        }
        tidb_planner::plan_base::PlanErrorKind::KeyNotExists { key, table } => {
            DriverError::KeyNotExists {
                key: key.clone(),
                table: table.clone(),
            }
        }
        tidb_planner::plan_base::PlanErrorKind::Internal => {
            DriverError::unsupported(error.to_string())
        }
        tidb_planner::plan_base::PlanErrorKind::WrongNumberOfColumnsInSelect => {
            DriverError::WrongNumberOfColumnsInSelect
        }
        tidb_planner::plan_base::PlanErrorKind::ViewWrongList => DriverError::ViewWrongList,
        tidb_planner::plan_base::PlanErrorKind::CteRecursiveRequiresUnion(name) => {
            DriverError::CteRecursiveRequiresUnion(name.clone())
        }
        tidb_planner::plan_base::PlanErrorKind::CteRecursiveRequiresNonRecursiveFirst(name) => {
            DriverError::CteRecursiveRequiresNonRecursiveFirst(name.clone())
        }
        tidb_planner::plan_base::PlanErrorKind::CteRecursiveForbidsAggregation(name) => {
            DriverError::CteRecursiveForbidsAggregation(name.clone())
        }
        tidb_planner::plan_base::PlanErrorKind::CteRecursiveForbiddenJoinOrder(name) => {
            DriverError::CteRecursiveForbiddenJoinOrder(name.clone())
        }
        tidb_planner::plan_base::PlanErrorKind::NotSupportedYet(feature) => {
            DriverError::NotSupportedYet(feature.clone().into())
        }
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
    run_select_meta_stmt_with_physical(select, None, catalog, current_db, ctx)
}

/// Runs one parsed `SELECT` through the ordinary executor builder, optionally
/// consuming a physical tree already selected by the planner. Go's fresh and
/// cached plans both enter `executorBuilder.build`; this is that common seam.
pub fn run_select_meta_stmt_with_physical(
    select: &tidb_ast::SelectStmt,
    physical: Option<&mut tidb_planner::physical::PhysicalPlan>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    run_query_meta_stmt_with_physical(
        &QueryStmt::Select(Box::new(select.clone())),
        physical,
        catalog,
        current_db,
        ctx,
    )
}

/// Runs one parsed query through the ordinary physical executor builder,
/// optionally consuming a cache-rebuilt tree selected by the planner.
pub fn run_query_meta_stmt_with_physical(
    query: &QueryStmt,
    physical: Option<&mut tidb_planner::physical::PhysicalPlan>,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    match physical {
        Some(physical) => physical_builder::execute_query(query, physical, catalog, ctx),
        None => run_query_stmt(query, catalog, current_db, ctx),
    }
}

/// Builds the ordinary physical query tree without opening an executor.
///
/// Information-schema execution uses this to inspect the resolved, pruned
/// `PhysicalMemTable.Columns` before Go's retriever decides whether its
/// process-wide table-size cache needs a fresh restricted read.
pub fn plan_query_meta_stmt(
    query: &QueryStmt,
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> Result<tidb_planner::physical::PhysicalPlan, DriverError> {
    planner_bridge::physical_query_plan(query, catalog, current_db, ctx)
        .map_err(planner_error_to_driver)
}

/// Answers Go `memtableRetriever.updateStatsCacheIfNeed` from the physical
/// memory-table columns left after logical column pruning.
#[must_use]
pub fn physical_plan_needs_table_storage_statistics(
    plan: &tidb_planner::physical::PhysicalPlan,
) -> bool {
    if let tidb_planner::physical::PhysicalPlan::CTE(cte) = plan {
        if physical_plan_needs_table_storage_statistics(&cte.seed_plan)
            || cte
                .recursive_plan
                .as_deref()
                .is_some_and(physical_plan_needs_table_storage_statistics)
        {
            return true;
        }
    }
    if let tidb_planner::physical::PhysicalPlan::MemTable(scan) = plan {
        let table_uses_cache = scan.table_name.eq_ignore_ascii_case("TABLES")
            || scan.table_name.eq_ignore_ascii_case("PARTITIONS");
        if table_uses_cache
            && scan.columns.iter().any(|column| {
                matches!(
                    column.name.to_ascii_uppercase().as_str(),
                    "TABLE_ROWS" | "AVG_ROW_LENGTH" | "DATA_LENGTH" | "INDEX_LENGTH"
                )
            })
        {
            return true;
        }
    }
    plan.children()
        .iter()
        .any(|child| physical_plan_needs_table_storage_statistics(child))
}

/// Answers Go `buildTableSizeStats`'s `needColLength` decision after logical
/// column pruning. `TABLE_ROWS` needs only `mysql.stats_meta`; the variable-
/// width size columns additionally require the more expensive histogram read.
#[must_use]
pub fn physical_plan_needs_table_storage_column_lengths(
    plan: &tidb_planner::physical::PhysicalPlan,
) -> bool {
    if let tidb_planner::physical::PhysicalPlan::CTE(cte) = plan {
        if physical_plan_needs_table_storage_column_lengths(&cte.seed_plan)
            || cte
                .recursive_plan
                .as_deref()
                .is_some_and(physical_plan_needs_table_storage_column_lengths)
        {
            return true;
        }
    }
    if let tidb_planner::physical::PhysicalPlan::MemTable(scan) = plan {
        let table_uses_stats = scan.table_name.eq_ignore_ascii_case("TABLES")
            || scan.table_name.eq_ignore_ascii_case("PARTITIONS");
        if table_uses_stats
            && scan.columns.iter().any(|column| {
                matches!(
                    column.name.to_ascii_uppercase().as_str(),
                    "AVG_ROW_LENGTH" | "DATA_LENGTH" | "INDEX_LENGTH"
                )
            })
        {
            return true;
        }
    }
    plan.children()
        .iter()
        .any(|child| physical_plan_needs_table_storage_column_lengths(child))
}

/// Resolves every column named by one `MATCH` expression against the SELECT's
/// complete FROM scope and returns whether Go's LIKE fallback may treat them
/// as strings. This is the same resolved-type question
/// `expressionRewriter.matchAgainstToLike` asks; aliases, joins, derived
/// tables, and database qualifiers therefore use the ordinary planner's name
/// resolution instead of an AST-only single-table approximation.
#[must_use]
pub fn fts_columns_are_strings(
    select: &tidb_ast::SelectStmt,
    columns: &[Vec<String>],
    catalog: &Catalog,
    current_db: &str,
    ctx: &crate::StmtContext,
) -> bool {
    let scope = subquery::select_outer_scope(select, catalog, current_db, ctx);
    let resolver = from::ScopeResolver { scope: &scope };
    columns.iter().all(|path| {
        resolver.resolve(path).is_some_and(|(_, field_type, _)| {
            field_type.eval_type() == tidb_datatype::EvalType::String
        })
    })
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
    let physical = planner_bridge::physical_select_plan(select, catalog, current_db, ctx)
        .map_err(planner_error_to_driver)?;
    physical_builder::planned_result_columns(select, &physical)
}

/// Opens a finished physical executor tree, drains every row, and closes it.
fn drain_root_executor(
    mut root: Box<dyn Executor>,
    columns: Vec<(String, FieldType)>,
    ctx: &crate::StmtContext,
) -> Result<SelectMeta, DriverError> {
    let ret_types: Vec<FieldType> = columns.iter().map(|(_, ty)| ty.clone()).collect();
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

/// Evaluates a `LIMIT` bound, which must be a non-negative integer literal or
/// an execute-time integer parameter. Go keeps a `ParamMarkerExpr` in the
/// prepared AST and resolves its value during execution; the Rust prepared
/// path installs that same datum on [`tidb_ast::Expr::ParamMarker`].
pub(crate) fn eval_limit_bound(expr: &tidb_ast::Expr) -> Result<u64, DriverError> {
    match expr {
        tidb_ast::Expr::Int(text) => text
            .parse::<u64>()
            .map_err(|_| DriverError::unsupported("LIMIT bound must be a non-negative integer")),
        tidb_ast::Expr::ParamMarker {
            value: Some(Datum::Int(value)),
            ..
        } if *value >= 0 => Ok(*value as u64),
        tidb_ast::Expr::ParamMarker {
            value: Some(Datum::UInt(value)),
            ..
        } => Ok(*value),
        _ => Err(DriverError::unsupported(
            "LIMIT bound must be an integer literal",
        )),
    }
}

#[cfg(test)]
mod limit_bound_tests {
    use super::eval_limit_bound;
    use tidb_ast::Expr;
    use tidb_datatype::Datum;

    #[test]
    fn accepts_execute_time_integer_parameter() {
        let marker = Expr::ParamMarker {
            offset: 0,
            order: 0,
            in_execute: true,
            value: Some(Datum::Int(7)),
            projection_offset: 0,
        };
        assert_eq!(eval_limit_bound(&marker).unwrap(), 7);
    }

    #[test]
    fn rejects_unbound_or_negative_parameter() {
        let unbound = Expr::ParamMarker {
            offset: 0,
            order: 0,
            in_execute: false,
            value: None,
            projection_offset: 0,
        };
        assert!(eval_limit_bound(&unbound).is_err());
        let negative = Expr::ParamMarker {
            offset: 0,
            order: 0,
            in_execute: true,
            value: Some(Datum::Int(-1)),
            projection_offset: 0,
        };
        assert!(eval_limit_bound(&negative).is_err());
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
