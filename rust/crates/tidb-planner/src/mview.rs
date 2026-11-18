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

//! Go master `pkg/planner/mview`'s COMPLETE DELTA APPLY half
//! (`BuildCompleteDiffSource` and its helpers): the diff-source SELECT that
//! full-outer-joins the recomputed materialized view definition to the
//! view's current rows, and the layout its executor reads.
//!
//! Go assembles the SELECT as an AST around the parsed definition; this port
//! writes the same statement as SQL around the restored definition and parses
//! it once, which yields the same tree up to the parentheses around the
//! payload comparison Go's `NOT` wraps.

use tidb_ast::{Expr, QueryStmt, SelectField, SelectStmt, Stmt};
use tidb_model::{ColumnInfo, TableInfo};

/// Go `diffRecomputedAlias`.
pub const DIFF_RECOMPUTED_ALIAS: &str = "__mvd_recomputed";
/// Go `diffCurrentAlias`.
pub const DIFF_CURRENT_ALIAS: &str = "__mvd_current";
/// Go `diffOpColName`.
pub const DIFF_OP_COL_NAME: &str = "__mvd_op";
/// Go `diffHandlePrefix`.
pub const DIFF_HANDLE_PREFIX: &str = "__mvd_h_";
/// Go `diffOldRowPrefix`.
pub const DIFF_OLD_ROW_PREFIX: &str = "__mvd_o_";
/// Go `diffNewRowPrefix`.
pub const DIFF_NEW_ROW_PREFIX: &str = "__mvd_n_";

/// Go `mviewCompleteDeltaDiffOpInsert` / `Delete` / `Update`.
pub const DIFF_OP_INSERT: i64 = 1;
pub const DIFF_OP_DELETE: i64 = 2;
pub const DIFF_OP_UPDATE: i64 = 3;

/// Go master `buildCompleteDiffHandleCols`' answer: where the current row's
/// handle sits in the diff source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiffHandleCols {
    /// Go `NewIntHandleCols` over the integer primary key column.
    IntPk { source_offset: usize },
    /// Go `NewCommonHandlesColsWithoutColsAlign` over the primary index
    /// columns, in index order.
    Common { source_offsets: Vec<usize> },
    /// Go `NewIntHandleCols` over `_tidb_rowid`.
    RowId { source_offset: usize },
}

/// Go master `CompleteDiffBuildResult`.
#[derive(Clone, Debug)]
pub struct CompleteDiffBuildResult {
    /// Go `DiffSourceSelect`.
    pub diff_source_select: SelectStmt,
    /// Go `SourceColumnCount`.
    pub source_column_count: usize,
    /// Go `MVTableID`.
    pub mv_table_id: i64,
    /// Go `MVColumnCount`.
    pub mv_column_count: usize,
    /// Go `OpColOffset`.
    pub op_col_offset: usize,
    /// Go `MarkerMVOffset`.
    pub marker_mv_offset: usize,
    /// Go `GroupKeyMVOffsets`.
    pub group_key_mv_offsets: Vec<usize>,
    /// Go `CurrentHandleCols`.
    pub current_handle_cols: DiffHandleCols,
    /// Go `CurrentRowOffsets`.
    pub current_row_offsets: Vec<usize>,
    /// Go `RecomputedRowOffsets`.
    pub recomputed_row_offsets: Vec<usize>,
}

impl CompleteDiffBuildResult {
    /// Go master `CompleteDiffBuildResult.ValidateSourceLayout`.
    ///
    /// # Errors
    ///
    /// The layout and a source schema of `schema_len` columns disagree.
    pub fn validate_source_layout(&self, schema_len: usize) -> Result<(), String> {
        if self.mv_column_count == 0 {
            return Err("complete diff build result: MVColumnCount is invalid".to_owned());
        }
        if self.source_column_count == 0 {
            return Err("complete diff build result: SourceColumnCount is invalid".to_owned());
        }
        if schema_len > 0 && schema_len != self.source_column_count {
            return Err(format!(
                "complete diff build result: schema length mismatch: got {schema_len}, expected {}",
                self.source_column_count
            ));
        }
        let mut expected = 1 + self.mv_column_count * 2;
        if matches!(self.current_handle_cols, DiffHandleCols::RowId { .. }) {
            expected += 1;
        }
        if self.source_column_count != expected {
            return Err(format!(
                "complete diff build result: source column count mismatch: got {}, expected {expected}",
                self.source_column_count
            ));
        }
        if self.current_row_offsets.len() != self.mv_column_count
            || self.recomputed_row_offsets.len() != self.mv_column_count
        {
            return Err("complete diff build result: row offset length mismatch".to_owned());
        }
        Ok(())
    }
}

fn escape(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

fn qual_col(alias: &str, column: &str) -> String {
    format!("{}.{}", escape(alias), escape(column))
}

/// Go master `extractGroupKeyOffsetsFromMVSelect`.
fn extract_group_key_offsets_from_mv_select(select: &SelectStmt) -> Result<Vec<usize>, String> {
    if select.group_by.is_empty() {
        return Err("materialized view definition must have GROUP BY".to_owned());
    }
    let mut offset_by_name = std::collections::HashMap::new();
    for (index, field) in select.fields.fields().iter().enumerate() {
        if let SelectField::Expr { expr: Expr::Column(path), .. } = field {
            if let Some(name) = path.last() {
                offset_by_name.insert(tidb_hack::go_to_lower(name), index);
            }
        }
    }
    let mut offsets = Vec::with_capacity(select.group_by.len());
    for item in &select.group_by {
        let Expr::Column(path) = &item.expr else {
            return Err("GROUP BY expression must be a column name for mview".to_owned());
        };
        let name = path.last().cloned().unwrap_or_default();
        let Some(&offset) = offset_by_name.get(&tidb_hack::go_to_lower(&name)) else {
            return Err(format!("GROUP BY column {name} must appear in SELECT list"));
        };
        offsets.push(offset);
    }
    Ok(offsets)
}

/// Go master `pickCompleteDiffMarkerColumn`: the first visible NOT NULL
/// column.
fn pick_complete_diff_marker_column(mv: &TableInfo, columns: &[ColumnInfo]) -> Result<usize, String> {
    columns
        .iter()
        .position(|column| !column.hidden && tidb_mysql::types::has_not_null_flag(column.field_type.flags() as usize))
        .ok_or_else(|| format!("materialized view {} has no visible NOT NULL column for diff marker", mv.name.original()))
}

/// Go master `buildCompleteDiffHandleCols`.
fn build_complete_diff_handle_cols(
    mv: &TableInfo,
    columns: &[ColumnInfo],
    current_row_offsets: &[usize],
    row_id_handle_offset: Option<usize>,
) -> Result<DiffHandleCols, String> {
    let name = mv.name.original();
    if mv.pk_is_handle {
        let pk = columns
            .iter()
            .find(|column| tidb_mysql::types::has_pri_key_flag(column.field_type.flags() as usize))
            .ok_or_else(|| format!("mv table {name} has PKIsHandle but no primary key column"))?;
        let offset = usize::try_from(pk.offset).ok().filter(|&offset| offset < current_row_offsets.len());
        let offset = offset.ok_or_else(|| format!("mv table {name} primary key column offset {} is invalid", pk.offset))?;
        return Ok(DiffHandleCols::IntPk { source_offset: current_row_offsets[offset] });
    }
    if mv.is_common_handle {
        let indices: Vec<tidb_model::IndexInfo> = mv.indices.iter_deref().map(|index| index.read().clone()).collect();
        let primary = indices
            .iter()
            .find(|index| index.primary)
            .ok_or_else(|| format!("mv table {name} has common handle but no primary index"))?;
        let mut source_offsets = Vec::with_capacity(primary.columns.len());
        for column in primary.columns.iter_deref() {
            let column_offset = column.read().offset;
            let offset = usize::try_from(column_offset).ok().filter(|&offset| offset < columns.len());
            let offset =
                offset.ok_or_else(|| format!("mv table {name} primary index column offset {column_offset} is invalid"))?;
            source_offsets.push(current_row_offsets[offset]);
        }
        return Ok(DiffHandleCols::Common { source_offsets });
    }
    row_id_handle_offset
        .map(|source_offset| DiffHandleCols::RowId { source_offset })
        .ok_or_else(|| format!("mv table {name} rowid handle offset is invalid"))
}

/// Go master `BuildCompleteDiffSource`: the diff-source SELECT and its
/// layout for COMPLETE DELTA APPLY.
///
/// ```text
/// SELECT <diff op>, [current._tidb_rowid AS <rowid handle>],
///        current.<all MV columns>, recomputed.<all MV columns>
/// FROM (<MV definition SQL>) AS recomputed
/// FULL OUTER JOIN <mv table> AS current
///   ON recomputed.<group key> [= | <=>] current.<group key> AND ...
/// WHERE recomputed.<marker> IS NULL OR current.<marker> IS NULL
///    OR NOT <old/new payload equality>
/// ```
///
/// `db_name` is the view's schema (Go `dbNameByTableID`); `parse` parses one
/// statement under the session's parser configuration (Go
/// `parseSelectFromSQL`).
///
/// # Errors
///
/// The view is malformed for a diff, or a statement fails to parse.
pub fn build_complete_diff_source(
    mv: &TableInfo,
    db_name: &str,
    parse: &dyn Fn(&str) -> Result<Stmt, String>,
) -> Result<CompleteDiffBuildResult, String> {
    let name = mv.name.original();
    let Some(info) = mv.materialized_view.as_ref().map(|info| info.read().clone()) else {
        return Err(format!("table {name} is not a materialized view"));
    };
    if info.sql_content.is_empty() {
        return Err(format!("materialized view {name} has empty SQLContent"));
    }
    let columns: Vec<ColumnInfo> = mv.columns.iter_deref().map(|column| column.read().clone()).collect();
    let mut stmt = parse(&info.sql_content)?;
    let Stmt::Query(query) = &mut stmt else {
        return Err("expected select statement".to_owned());
    };
    let QueryStmt::Select(select) = &mut **query else {
        return Err("expected select statement".to_owned());
    };
    // Go `validateCompleteDiffSelectFields`.
    if select.fields.fields().len() != columns.len() {
        return Err(format!(
            "materialized view {name} select output count {} does not match mv column count {}",
            select.fields.fields().len(),
            columns.len()
        ));
    }
    for (index, field) in select.fields.fields_mut().iter_mut().enumerate() {
        let SelectField::Expr { alias, .. } = field else {
            return Err(format!("materialized view {name} select field {index} is nil"));
        };
        *alias = Some(columns[index].name.original().to_owned());
    }
    let group_key_offsets = extract_group_key_offsets_from_mv_select(select)?;
    if group_key_offsets.is_empty() {
        return Err(format!("materialized view {name} has empty group key offsets"));
    }
    if let Some(&bad) = group_key_offsets.iter().find(|&&offset| offset >= columns.len()) {
        return Err(format!("materialized view {name} has invalid group key offset {bad}"));
    }
    let recomputed_sql = stmt.restore();

    let marker_offset = pick_complete_diff_marker_column(mv, &columns)?;
    let column_name = |offset: usize| columns[offset].name.original().to_owned();
    let not_null = |offset: usize| tidb_mysql::types::has_not_null_flag(columns[offset].field_type.flags() as usize);
    let compare = |offset: usize| {
        let op = if not_null(offset) { "=" } else { "<=>" };
        let column = column_name(offset);
        format!(
            "{} {op} {}",
            qual_col(DIFF_RECOMPUTED_ALIAS, &column),
            qual_col(DIFF_CURRENT_ALIAS, &column)
        )
    };
    let join_cond = group_key_offsets.iter().map(|&offset| compare(offset)).collect::<Vec<_>>().join(" AND ");
    // Go `buildCompleteDiffPayloadEqExpr`: the non-group-key columns.
    let payload: Vec<String> = (0..columns.len())
        .filter(|offset| !group_key_offsets.contains(offset))
        .map(compare)
        .collect();
    let payload_eq = if payload.is_empty() { "1".to_owned() } else { payload.join(" AND ") };
    let marker = column_name(marker_offset);
    let recomputed_marker = qual_col(DIFF_RECOMPUTED_ALIAS, &marker);
    let current_marker = qual_col(DIFF_CURRENT_ALIAS, &marker);
    let where_expr =
        format!("{recomputed_marker} IS NULL OR {current_marker} IS NULL OR NOT ({payload_eq})");

    let use_row_id = !mv.pk_is_handle && !mv.is_common_handle;
    // Go `buildCompleteDiffOpExpr`.
    let mut fields = vec![format!(
        "CASE WHEN {current_marker} IS NULL THEN {DIFF_OP_INSERT} WHEN {recomputed_marker} IS NULL THEN {DIFF_OP_DELETE} ELSE {DIFF_OP_UPDATE} END AS {}",
        escape(DIFF_OP_COL_NAME)
    )];
    let row_id_handle_offset = use_row_id.then(|| {
        fields.push(format!(
            "{} AS {}",
            qual_col(DIFF_CURRENT_ALIAS, tidb_model::column::EXTRA_HANDLE_NAME),
            escape(&format!("{DIFF_HANDLE_PREFIX}{}", tidb_model::column::EXTRA_HANDLE_NAME))
        ));
        fields.len() - 1
    });
    let mut current_row_offsets = Vec::with_capacity(columns.len());
    for offset in 0..columns.len() {
        let column = column_name(offset);
        current_row_offsets.push(fields.len());
        fields.push(format!(
            "{} AS {}",
            qual_col(DIFF_CURRENT_ALIAS, &column),
            escape(&format!("{DIFF_OLD_ROW_PREFIX}{column}"))
        ));
    }
    let mut recomputed_row_offsets = Vec::with_capacity(columns.len());
    for offset in 0..columns.len() {
        let column = column_name(offset);
        recomputed_row_offsets.push(fields.len());
        fields.push(format!(
            "{} AS {}",
            qual_col(DIFF_RECOMPUTED_ALIAS, &column),
            escape(&format!("{DIFF_NEW_ROW_PREFIX}{column}"))
        ));
    }
    let current_handle_cols = build_complete_diff_handle_cols(mv, &columns, &current_row_offsets, row_id_handle_offset)?;

    let current_hint_table = format!("{}.{}", escape(db_name), escape(DIFF_CURRENT_ALIAS));
    let sql = format!(
        "SELECT /*+ READ_FROM_STORAGE(TIFLASH[{current_hint_table}]), HASH_JOIN_PROBE({current_hint_table}) */ {} FROM ({recomputed_sql}) AS {} FULL OUTER JOIN {}.{} AS {} ON {join_cond} WHERE {where_expr}",
        fields.join(", "),
        escape(DIFF_RECOMPUTED_ALIAS),
        escape(db_name),
        escape(name),
        escape(DIFF_CURRENT_ALIAS),
    );
    let Stmt::Query(query) = parse(&sql)? else {
        return Err("complete diff: diff source select is not a query".to_owned());
    };
    let QueryStmt::Select(diff_source_select) = &*query else {
        return Err("complete diff: diff source select is not a select".to_owned());
    };
    let diff_source_select = diff_source_select.clone();
    let result = CompleteDiffBuildResult {
        diff_source_select: *diff_source_select,
        source_column_count: fields.len(),
        mv_table_id: mv.id,
        mv_column_count: columns.len(),
        op_col_offset: 0,
        marker_mv_offset: marker_offset,
        group_key_mv_offsets: group_key_offsets,
        current_handle_cols,
        current_row_offsets,
        recomputed_row_offsets,
    };
    result.validate_source_layout(0)?;
    Ok(result)
}

// ---------------------------------------------------------------------------
// Go master `mview.Build`: FAST refresh's merge source.
//
// SQL shape (Go's `buildFromLocal` doc):
//
//   SELECT <delta payload cols>, <mv cols>, [mv._tidb_rowid AS __mview_mv_rowid]
//   FROM (<stage-1 delta aggregation on mlog>) AS delta
//   LEFT JOIN <mv table> AS mv ON delta.<group_key_1> [= | <=>] mv.<group_key_1> AND ...
//
// As for the complete diff source, the statement is written as SQL and
// parsed once; the stage-1 `SUM_INT` aggregates, which no statement can
// spell, are renamed on the parsed tree.

/// Go `deltaTableAlias`.
pub const DELTA_TABLE_ALIAS: &str = "delta";
/// Go `mvTableAlias`.
pub const MV_TABLE_ALIAS: &str = "mv";
/// Go `deltaCntStarName`.
pub const DELTA_CNT_STAR_NAME: &str = "__mview_delta_cnt_star";
/// Go `mvRowIDName`.
pub const MV_ROW_ID_NAME: &str = "__mview_mv_rowid";

/// Go master `AggKind`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggKind {
    /// Go `AggCountStar`.
    CountStar,
    /// Go `AggCount`.
    Count,
    /// Go `AggSum`.
    Sum,
    /// Go `AggMin`.
    Min,
    /// Go `AggMax`.
    Max,
}

/// Go master `AggInfo`: one aggregate output column and the merge-source
/// offsets it is updated from (see Go's doc on `Dependencies`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AggInfo {
    /// Go `Kind`.
    pub kind: AggKind,
    /// Go `MVOffset`.
    pub mv_offset: usize,
    /// Go `ArgColName`: the base column argument, lowercase; empty for
    /// COUNT(*) and an expression argument.
    pub arg_col_name: String,
    /// Go `ArgNotNull`.
    pub arg_not_null: bool,
    /// Go `Dependencies`.
    pub dependencies: Vec<usize>,
}

/// Go master `aggColInfo`.
#[derive(Clone, Debug)]
struct AggColInfo {
    info: AggInfo,
    delta_name: String,
    added_count_delta_name: String,
    removed_value_delta: String,
    removed_count_delta: String,
    /// The argument with its qualifiers and parentheses stripped.
    arg_expr: Option<Expr>,
}

/// Go master `BuildOptions`: the commit-ts window `(from_ts, to_ts]` of the
/// log rows read; a zero `to_ts` leaves the statement snapshot the bound.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BuildOptions {
    /// Go `FromTS`.
    pub from_ts: u64,
    /// Go `ToTS`.
    pub to_ts: u64,
}

/// Go master `BuildResult`.
#[derive(Clone, Debug)]
pub struct BuildResult {
    /// Go `MergeSourceSelect`.
    pub merge_source_select: SelectStmt,
    /// Go `SourceColumnCount`.
    pub source_column_count: usize,
    /// Go `MVTableID`.
    pub mv_table_id: i64,
    /// Go `BaseTableID`.
    pub base_table_id: i64,
    /// Go `MLogTableID`.
    pub mlog_table_id: i64,
    /// Go `MVColumnCount`.
    pub mv_column_count: usize,
    /// Go `DeltaColumnCount`.
    pub delta_column_count: usize,
    /// Go `MVTablePKCols`: the view's handle in the merge source.
    pub mv_table_pk_cols: DiffHandleCols,
    /// Go `GroupKeyMVOffsets`.
    pub group_key_mv_offsets: Vec<usize>,
    /// Go `GroupKeyBaseCols`.
    pub group_key_base_cols: Vec<String>,
    /// Go `CountStarMVOffset`.
    pub count_star_mv_offset: usize,
    /// Go `AggInfos`.
    pub agg_infos: Vec<AggInfo>,
    /// Go `FullUpdateLookupTemplateSelect`, as SQL with the inner base
    /// table's index hint left at [`FULL_UPDATE_INDEX_HINT_SLOT`] (Go's
    /// builder sets it with `SetFullUpdateLookupIndexHint` once it has
    /// validated the supporting indexes): the MIN / MAX fallback, absent
    /// without MIN / MAX.
    pub full_update_lookup_template_sql: Option<String>,
    /// Go `FullUpdateLookupColumnCount`.
    pub full_update_lookup_column_count: usize,
    /// Go `FullUpdateLookupMVOffsets`.
    pub full_update_lookup_mv_offsets: Vec<usize>,
}

/// Where the full-update lookup template's inner base table takes its
/// `USE INDEX` hint.
pub const FULL_UPDATE_INDEX_HINT_SLOT: &str = "/*__mview_full_update_index_hint__*/";

/// Go `fullUpdateOuterAlias`.
const FULL_UPDATE_OUTER_ALIAS: &str = "full_outer";
/// Go `fullUpdateInnerAlias`.
const FULL_UPDATE_INNER_ALIAS: &str = "full_inner";

/// Go master `SetFullUpdateLookupIndexHint`: the template with its inner
/// base table bound to `index_names`.
#[must_use]
pub fn set_full_update_lookup_index_hint(template_sql: &str, index_names: &[String]) -> String {
    let hint = format!("USE INDEX ({})", index_names.iter().map(|name| escape(name)).collect::<Vec<_>>().join(", "));
    template_sql.replace(FULL_UPDATE_INDEX_HINT_SLOT, &hint)
}

/// Go master `stripColumnQualifier` and `stripAllParentheses` over a parsed
/// expression.
fn strip_column_qualifiers_and_parentheses(expr: &mut Expr) {
    struct Stripper;
    impl tidb_ast::Visitor for Stripper {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(Expr::Column(path)) = node.downcast_mut::<Expr>() {
                if path.len() > 1 {
                    let name = path.pop().unwrap_or_default();
                    *path = vec![name];
                }
            }
            false
        }
        fn leave(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(expr) = node.downcast_mut::<Expr>() {
                while let Expr::Paren(inner) = expr {
                    let inner = std::mem::replace(&mut **inner, Expr::Int("0".to_owned()));
                    *expr = inner;
                }
            }
            true
        }
    }
    tidb_ast::Visitable::accept(expr, &mut Stripper);
}

/// Go master `isCountStarOrOneArg`.
fn is_count_star_or_one_arg(arg: Option<&Expr>) -> bool {
    match arg {
        None => true,
        Some(Expr::Int(digits)) => digits.trim() == "1",
        _ => false,
    }
}

/// Go master `extractAggInfosFromMVSelect`.
fn extract_agg_infos_from_mv_select(select: &SelectStmt) -> Result<(Vec<AggColInfo>, bool), String> {
    let mut agg_cols = Vec::new();
    let mut has_min_max = false;
    for (index, field) in select.fields.fields().iter().enumerate() {
        let SelectField::Expr { expr: Expr::Aggregate { name, distinct, args, .. }, .. } = field else {
            continue;
        };
        let column_arg = || match args.first() {
            Some(Expr::Column(path)) => path.last().map(|name| tidb_hack::go_to_lower(name)).unwrap_or_default(),
            _ => String::new(),
        };
        let stripped = || {
            args.first().cloned().map(|mut arg| {
                strip_column_qualifiers_and_parentheses(&mut arg);
                arg
            })
        };
        let plain = |kind, delta_name: String| AggColInfo {
            info: AggInfo { kind, mv_offset: index, arg_col_name: String::new(), arg_not_null: false, dependencies: Vec::new() },
            delta_name,
            added_count_delta_name: String::new(),
            removed_value_delta: String::new(),
            removed_count_delta: String::new(),
            arg_expr: None,
        };
        match tidb_hack::go_to_lower(name).as_str() {
            "count" => {
                if args.len() != 1 {
                    return Err("COUNT must have exactly one argument for mview stage-1".to_owned());
                }
                if *distinct {
                    return Err("COUNT(DISTINCT ...) is not supported in mview stage-1".to_owned());
                }
                if is_count_star_or_one_arg(args.first()) {
                    agg_cols.push(plain(AggKind::CountStar, DELTA_CNT_STAR_NAME.to_owned()));
                    continue;
                }
                let mut col = plain(AggKind::Count, format!("__mview_delta_cnt_{index}"));
                col.info.arg_col_name = column_arg();
                col.arg_expr = stripped();
                agg_cols.push(col);
            }
            "sum" => {
                if args.len() != 1 {
                    return Err("SUM must have exactly one argument for mview stage-1".to_owned());
                }
                if *distinct {
                    return Err("SUM(DISTINCT ...) is not supported in mview stage-1".to_owned());
                }
                let mut col = plain(AggKind::Sum, format!("__mview_delta_sum_{index}"));
                col.info.arg_col_name = column_arg();
                col.arg_expr = stripped();
                agg_cols.push(col);
            }
            lower @ ("max" | "min") => {
                let upper = if lower == "max" { "MAX" } else { "MIN" };
                if !matches!(args.first(), Some(Expr::Column(_))) {
                    return Err(format!("{upper} argument must be a column name for mview"));
                }
                has_min_max = true;
                let kind = if lower == "max" { AggKind::Max } else { AggKind::Min };
                let mut col = plain(kind, format!("__mview_{lower}_in_added_{index}"));
                col.info.arg_col_name = column_arg();
                col.added_count_delta_name = format!("__mview_{lower}_cnt_in_added_{index}");
                col.removed_value_delta = format!("__mview_{lower}_in_removed_{index}");
                col.removed_count_delta = format!("__mview_{lower}_cnt_in_removed_{index}");
                col.arg_expr = stripped();
                agg_cols.push(col);
            }
            _ => {
                let written = match field {
                    SelectField::Expr { expr: Expr::Aggregate { written: Some(written), .. }, .. } => written.clone(),
                    _ => name.clone(),
                };
                return Err(format!("unsupported aggregate function {written} in mview stage-1"));
            }
        }
    }
    Ok((agg_cols, has_min_max))
}

/// Go master `mapSumToCountExprDependencies` and
/// `mapMinMaxToCountExprDependencies`: the first COUNT(expr) structurally
/// equal to each nullable SUM / MIN / MAX argument (required for SUM).
fn map_to_count_expr_dependencies(
    agg_cols: &[AggColInfo],
    arg_not_null_by_offset: &std::collections::HashMap<usize, bool>,
    kinds: &[AggKind],
    required: bool,
) -> Result<std::collections::HashMap<usize, usize>, String> {
    let mut matched = std::collections::HashMap::new();
    for (index, col) in agg_cols.iter().enumerate() {
        if !kinds.contains(&col.info.kind) || arg_not_null_by_offset.get(&col.info.mv_offset).copied().unwrap_or(false) {
            continue;
        }
        let Some(arg) = &col.arg_expr else {
            return Err(format!("{:?} aggregate argument is nil at mv offset {}", col.info.kind, col.info.mv_offset));
        };
        let found = agg_cols
            .iter()
            .position(|candidate| candidate.info.kind == AggKind::Count && candidate.arg_expr.as_ref() == Some(arg));
        match found {
            Some(count) => {
                matched.insert(index, count);
            }
            None if required => {
                return Err(format!(
                    "SUM expression {} at mv offset {} requires matching COUNT(expr) in SELECT list",
                    arg.restore(),
                    col.info.mv_offset
                ))
            }
            None => {}
        }
    }
    Ok(matched)
}

/// Go master `validateAggDependencies`.
fn validate_agg_dependencies(
    agg_infos: &[AggInfo],
    mv_column_offset_base: usize,
    mv_column_count: usize,
    schema_len: usize,
) -> Result<(), String> {
    let mv_column_end = mv_column_offset_base + mv_column_count;
    for info in agg_infos {
        if info.mv_offset >= mv_column_count {
            return Err(format!("invalid mv offset {} for {:?}", info.mv_offset, info.kind));
        }
        if let Some(&bad) = info.dependencies.iter().find(|&&dep| dep >= schema_len) {
            return Err(format!("invalid dependency offset {bad} for {:?} at mv offset {}", info.kind, info.mv_offset));
        }
        let deps = &info.dependencies;
        let in_delta = |dep: usize| dep < mv_column_offset_base;
        let in_mv = |dep: usize| dep >= mv_column_offset_base && dep < mv_column_end;
        let valid = match info.kind {
            AggKind::CountStar | AggKind::Count => deps.len() == 1 && in_delta(deps[0]),
            AggKind::Sum => matches!(deps.len(), 1 | 2) && in_delta(deps[0]) && (deps.len() == 1 || in_mv(deps[1])),
            AggKind::Min | AggKind::Max => {
                matches!(deps.len(), 4 | 5) && deps[..4].iter().all(|&dep| in_delta(dep)) && (deps.len() == 4 || in_mv(deps[4]))
            }
        };
        if !valid {
            return Err(format!("{:?} at mv offset {} has invalid dependencies {:?}", info.kind, info.mv_offset, deps));
        }
    }
    Ok(())
}

/// Go master `Build` (`buildLocal` + `buildFromLocal`): the merge source of
/// one FAST refresh window and the layout its executor merges by. `mv` is
/// the view in `db_name`; `table_by_id` answers its base table and log.
///
/// # Errors
///
/// The view, its base table or log is malformed for FAST refresh, the
/// definition uses what FAST cannot merge, or a statement fails to parse.
pub fn build(
    mv: &TableInfo,
    db_name: &str,
    table_by_id: &dyn Fn(i64) -> Option<TableInfo>,
    opt: BuildOptions,
    parse: &dyn Fn(&str) -> Result<Stmt, String>,
) -> Result<BuildResult, String> {
    let name = mv.name.original();
    // Stage 0 (Go `buildLocal`): the view, its one base table and its log.
    let Some(info) = mv.materialized_view.as_ref().map(|info| info.read().clone()) else {
        return Err(format!("table {name} is not a materialized view"));
    };
    if info.base_table_ids.len() != 1 {
        return Err(format!("materialized view {name} has invalid base table list size {}", info.base_table_ids.len()));
    }
    let base_table_id = info.base_table_ids[0];
    let base = table_by_id(base_table_id).ok_or_else(|| format!("base table id {base_table_id} not found in infoschema"))?;
    let mlog_table_id = base.materialized_view_base.as_ref().map_or(0, |base| base.read().mlog_id);
    if mlog_table_id == 0 {
        return Err(format!("base table {} has no materialized view log", base.name.original()));
    }
    let mlog = table_by_id(mlog_table_id)
        .ok_or_else(|| format!("materialized view log table id {mlog_table_id} not found in infoschema"))?;
    let Some(mlog_info) = mlog.materialized_view_log.as_ref().map(|info| info.read().clone()) else {
        return Err(format!("table {} is not a materialized view log", mlog.name.original()));
    };
    if mlog_info.base_table_id != base_table_id {
        return Err(format!(
            "materialized view log {} does not belong to base table id {base_table_id}",
            mlog.name.original()
        ));
    }
    let has_column = |table: &TableInfo, column: &str| {
        let column = tidb_hack::go_to_lower(column);
        table.columns.iter_deref().any(|candidate| candidate.read().name.lowercase() == column)
    };
    for required in [
        tidb_model::table::MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME,
        tidb_model::table::MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME,
    ] {
        if !has_column(&mlog, required) {
            return Err(format!("materialized view log {} missing required column {required}", mlog.name.original()));
        }
    }

    // Stage 1: the definition's group keys and aggregates.
    let Stmt::Query(query) = parse(&info.sql_content)? else {
        return Err("expected select statement".to_owned());
    };
    let QueryStmt::Select(mv_select) = &*query else {
        return Err("expected select statement".to_owned());
    };
    let group_key_offsets = extract_group_key_offsets_from_mv_select(mv_select)?;
    if group_key_offsets.is_empty() {
        return Err("materialized view definition has empty GROUP BY".to_owned());
    }
    let (agg_cols, has_min_max) = extract_agg_infos_from_mv_select(mv_select)?;
    let columns: Vec<ColumnInfo> = mv.columns.iter_deref().map(|column| column.read().clone()).collect();
    if columns.len() != mv_select.fields.fields().len() {
        return Err(format!(
            "mv columns count {} does not match mv query output {}",
            columns.len(),
            mv_select.fields.fields().len()
        ));
    }
    let count_star_mv_offset = agg_cols
        .iter()
        .find(|col| col.info.kind == AggKind::CountStar)
        .map(|col| col.info.mv_offset)
        .ok_or_else(|| "materialized view definition must include COUNT(*) for mview".to_owned())?;

    // Go `inferAggArgNotNullByOffset`.
    let base_not_null: std::collections::HashMap<String, bool> = base
        .columns
        .iter_deref()
        .map(|column| {
            let column = column.read();
            (column.name.lowercase().to_owned(), tidb_mysql::types::has_not_null_flag(column.field_type.flags() as usize))
        })
        .collect();
    let arg_not_null: std::collections::HashMap<usize, bool> = agg_cols
        .iter()
        .filter(|col| !col.info.arg_col_name.is_empty() && base_not_null.get(&col.info.arg_col_name).copied().unwrap_or(false))
        .map(|col| (col.info.mv_offset, true))
        .collect();

    // Go `groupKeyBaseColExprAtOffset`.
    let group_key_base_col = |mv_offset: usize| -> Result<String, String> {
        match mv_select.fields.fields().get(mv_offset) {
            Some(SelectField::Expr { expr: Expr::Column(path), .. }) => Ok(path.last().cloned().unwrap_or_default()),
            Some(_) => Err(format!("mv field at offset {mv_offset} is not a group key column")),
            None => Err(format!("invalid mv offset {mv_offset}")),
        }
    };

    // Stage 2a (Go `buildMLogDeltaSelect`).
    let old_new = escape(tidb_model::table::MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME);
    let sum_int = tidb_expression_aggregation::names::SUM_INT;
    let mut renames: Vec<(String, &'static str, &'static str)> = vec![(DELTA_CNT_STAR_NAME.to_owned(), "sum", sum_int)];
    let mut delta_fields = Vec::new();
    for &mv_offset in &group_key_offsets {
        delta_fields.push(format!("{} AS {}", escape(&group_key_base_col(mv_offset)?), escape(columns[mv_offset].name.original())));
    }
    delta_fields.push(format!("SUM({old_new}) AS {}", escape(DELTA_CNT_STAR_NAME)));
    for col in &agg_cols {
        match col.info.kind {
            AggKind::CountStar => {}
            AggKind::Count => {
                let arg = col.arg_expr.as_ref().ok_or("COUNT aggregate argument is nil for mview")?.restore();
                delta_fields.push(format!("SUM(IF({arg} IS NOT NULL, {old_new}, 0)) AS {}", escape(&col.delta_name)));
                renames.push((col.delta_name.clone(), "sum", sum_int));
            }
            AggKind::Sum => {
                let arg = col.arg_expr.as_ref().ok_or("SUM aggregate argument is nil for mview")?.restore();
                delta_fields.push(format!("SUM(IF({old_new} = 1, {arg}, -({arg}))) AS {}", escape(&col.delta_name)));
            }
            AggKind::Min | AggKind::Max => {
                let (func, count) = if col.info.kind == AggKind::Max {
                    ("MAX", tidb_expression_aggregation::names::MAX_COUNT)
                } else {
                    ("MIN", tidb_expression_aggregation::names::MIN_COUNT)
                };
                if col.info.arg_col_name.is_empty() {
                    return Err(format!("{func} aggregate argument column is empty for mview"));
                }
                let arg = escape(&col.info.arg_col_name);
                let added = format!("IF({old_new} = 1, {arg}, NULL)");
                let removed = format!("IF({old_new} = -1, {arg}, NULL)");
                delta_fields.push(format!("{func}({added}) AS {}", escape(&col.delta_name)));
                delta_fields.push(format!("{func}({added}) AS {}", escape(&col.added_count_delta_name)));
                delta_fields.push(format!("{func}({removed}) AS {}", escape(&col.removed_value_delta)));
                delta_fields.push(format!("{func}({removed}) AS {}", escape(&col.removed_count_delta)));
                let lower = if col.info.kind == AggKind::Max { "max" } else { "min" };
                renames.push((col.added_count_delta_name.clone(), lower, count));
                renames.push((col.removed_count_delta.clone(), lower, count));
            }
        }
    }
    let mut delta_where = format!("{} > {}", escape(tidb_model::column::EXTRA_COMMIT_TS_NAME), opt.from_ts);
    if opt.to_ts > 0 {
        delta_where.push_str(&format!(" AND {} <= {}", escape(tidb_model::column::EXTRA_COMMIT_TS_NAME), opt.to_ts));
    }
    if let Some(mv_where) = &mv_select.where_clause {
        let mut mv_where = mv_where.clone();
        strip_column_qualifiers_and_parentheses(&mut mv_where);
        delta_where.push_str(&format!(" AND ({})", mv_where.restore()));
    }
    let delta_group_by = group_key_offsets
        .iter()
        .map(|&mv_offset| group_key_base_col(mv_offset).map(|column| escape(&column)))
        .collect::<Result<Vec<_>, _>>()?
        .join(", ");
    let mlog_table = format!("{}.{}", escape(db_name), escape(mlog.name.original()));
    let delta_sql = format!(
        "SELECT /*+ READ_FROM_STORAGE(TIFLASH[{mlog_table}]) */ {} FROM {mlog_table} WHERE {delta_where} GROUP BY {delta_group_by}",
        delta_fields.join(", ")
    );

    // Stage 2b (Go `buildMergeSourceSelect`).
    let join_on = group_key_offsets
        .iter()
        .map(|&mv_offset| {
            let column = columns[mv_offset].name.original();
            let op = if tidb_mysql::types::has_not_null_flag(columns[mv_offset].field_type.flags() as usize) { "=" } else { "<=>" };
            format!("{} {op} {}", qual_col(DELTA_TABLE_ALIAS, column), qual_col(MV_TABLE_ALIAS, column))
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let mut delta_columns: Vec<String> = vec![DELTA_CNT_STAR_NAME.to_owned()];
    for col in &agg_cols {
        match col.info.kind {
            AggKind::CountStar => {}
            AggKind::Min | AggKind::Max => delta_columns.extend([
                col.delta_name.clone(),
                col.added_count_delta_name.clone(),
                col.removed_value_delta.clone(),
                col.removed_count_delta.clone(),
            ]),
            _ => delta_columns.push(col.delta_name.clone()),
        }
    }
    let mut fields: Vec<String> =
        delta_columns.iter().map(|name| format!("{} AS {}", qual_col(DELTA_TABLE_ALIAS, name), escape(name))).collect();
    for (offset, column) in columns.iter().enumerate() {
        let alias = if group_key_offsets.contains(&offset) { DELTA_TABLE_ALIAS } else { MV_TABLE_ALIAS };
        fields.push(format!("{} AS {}", qual_col(alias, column.name.original()), escape(column.name.original())));
    }
    let row_id_handle_offset = (!mv.pk_is_handle && !mv.is_common_handle).then(|| {
        fields.push(format!(
            "{} AS {}",
            qual_col(MV_TABLE_ALIAS, tidb_model::column::EXTRA_HANDLE_NAME),
            escape(MV_ROW_ID_NAME)
        ));
        fields.len() - 1
    });
    let merge_sql = format!(
        "SELECT {} FROM ({delta_sql}) AS {} LEFT JOIN {}.{} AS {} ON {join_on}",
        fields.join(", "),
        escape(DELTA_TABLE_ALIAS),
        escape(db_name),
        escape(name),
        escape(MV_TABLE_ALIAS),
    );
    // Go `buildFullUpdateLookupTemplateSelect` (MIN / MAX only): an
    // IndexJoin of one probe group-key tuple (outer) against the group's
    // recomputation from the base table (inner).
    let (full_update_lookup_template_sql, full_update_lookup_mv_offsets) = if has_min_max {
        let base_table = format!("{}.{}", escape(db_name), escape(base.name.original()));
        let mv_where = mv_select.where_clause.as_ref().map(|mv_where| {
            let mut mv_where = mv_where.clone();
            strip_column_qualifiers_and_parentheses(&mut mv_where);
            format!(" WHERE {}", mv_where.restore())
        });
        let group_key_expr = |mv_offset: usize| match mv_select.fields.fields().get(mv_offset) {
            Some(SelectField::Expr { expr: expr @ Expr::Column(_), .. }) => Ok(expr.restore()),
            Some(_) => Err(format!("mv field at offset {mv_offset} is not a group key column")),
            None => Err(format!("invalid mv offset {mv_offset}")),
        };
        // Go `buildFullUpdateLookupOuterSelect`.
        let mut outer_fields = Vec::with_capacity(group_key_offsets.len());
        for &mv_offset in &group_key_offsets {
            outer_fields.push(format!("{} AS {}", group_key_expr(mv_offset)?, escape(&format!("__mview_full_outer_gk_{mv_offset}"))));
        }
        let outer_sql =
            format!("SELECT {} FROM {base_table}{} LIMIT 1", outer_fields.join(", "), mv_where.clone().unwrap_or_default());
        // Go `buildFullUpdateLookupInnerSelect`.
        let agg_by_mv_offset: std::collections::HashMap<usize, &AggColInfo> =
            agg_cols.iter().map(|col| (col.info.mv_offset, col)).collect();
        let mut inner_fields = Vec::with_capacity(columns.len());
        for (mv_offset, column) in columns.iter().enumerate() {
            if group_key_offsets.contains(&mv_offset) {
                inner_fields.push(format!("{} AS {}", group_key_expr(mv_offset)?, escape(&group_key_base_col(mv_offset)?)));
                continue;
            }
            let col = agg_by_mv_offset
                .get(&mv_offset)
                .ok_or_else(|| format!("mv offset {mv_offset} is neither group key nor aggregate"))?;
            // Full-update fallback only recomputes MIN/MAX aggregates.
            let func = match col.info.kind {
                AggKind::Max => "MAX",
                AggKind::Min => "MIN",
                _ => continue,
            };
            inner_fields.push(format!("{func}({}) AS {}", escape(&col.info.arg_col_name), escape(column.name.original())));
        }
        let inner_group_by = group_key_offsets.iter().map(|&mv_offset| group_key_expr(mv_offset)).collect::<Result<Vec<_>, _>>()?;
        let inner_sql = format!(
            "SELECT {} FROM {base_table} {FULL_UPDATE_INDEX_HINT_SLOT}{} GROUP BY {}",
            inner_fields.join(", "),
            mv_where.unwrap_or_default(),
            inner_group_by.join(", ")
        );
        // Group keys can be NULL, so full-update lookup must use null-safe equality.
        let mut on = Vec::with_capacity(group_key_offsets.len());
        for &mv_offset in &group_key_offsets {
            on.push(format!(
                "{} <=> {}",
                qual_col(FULL_UPDATE_OUTER_ALIAS, &format!("__mview_full_outer_gk_{mv_offset}")),
                qual_col(FULL_UPDATE_INNER_ALIAS, &group_key_base_col(mv_offset)?)
            ));
        }
        let mut fields = Vec::with_capacity(columns.len());
        let mut mv_offsets = Vec::with_capacity(columns.len());
        for (mv_offset, column) in columns.iter().enumerate() {
            if agg_by_mv_offset.get(&mv_offset).is_some_and(|col| !matches!(col.info.kind, AggKind::Min | AggKind::Max)) {
                continue;
            }
            let out = if group_key_offsets.contains(&mv_offset) { group_key_base_col(mv_offset)? } else { column.name.original().to_owned() };
            fields.push(format!("{} AS {}", qual_col(FULL_UPDATE_INNER_ALIAS, &out), escape(column.name.original())));
            mv_offsets.push(mv_offset);
        }
        // Keep the template stable on index-join path; the hint names the
        // view's schema, as an internal session's current database may be empty.
        let hint = format!(
            "/*+ INL_JOIN({db}.{FULL_UPDATE_OUTER_ALIAS}, {db}.{FULL_UPDATE_INNER_ALIAS}) */",
            db = escape(db_name)
        );
        let sql = format!(
            "SELECT {hint} {} FROM ({outer_sql}) AS {} JOIN ({inner_sql}) AS {} ON {}",
            fields.join(", "),
            escape(FULL_UPDATE_OUTER_ALIAS),
            escape(FULL_UPDATE_INNER_ALIAS),
            on.join(" AND ")
        );
        (Some(sql), mv_offsets)
    } else {
        (None, Vec::new())
    };

    let Stmt::Query(query) = parse(&merge_sql)? else {
        return Err("mview: merge source select is not a query".to_owned());
    };
    let QueryStmt::Select(merge_select) = &*query else {
        return Err("mview: merge source select is not a select".to_owned());
    };
    let mut merge_select = (**merge_select).clone();
    rename_internal_aggregates(&mut merge_select, &renames);

    // Go `buildFromLocal`'s dependencies.
    let delta_count = delta_columns.len();
    let expected_len = columns.len() + delta_count + usize::from(row_id_handle_offset.is_some());
    let sum_to_count = map_to_count_expr_dependencies(&agg_cols, &arg_not_null, &[AggKind::Sum], true)?;
    let min_max_to_count = map_to_count_expr_dependencies(&agg_cols, &arg_not_null, &[AggKind::Min, AggKind::Max], false)?;
    let mut agg_infos = Vec::with_capacity(agg_cols.len());
    for (index, col) in agg_cols.iter().enumerate() {
        let mut info = col.info.clone();
        info.arg_not_null = arg_not_null.get(&info.mv_offset).copied().unwrap_or(false);
        let mut deps = Vec::with_capacity(2);
        let delta_offset = delta_columns
            .iter()
            .position(|name| *name == col.delta_name)
            .ok_or_else(|| format!("internal error: delta column {} not found in output", col.delta_name))?;
        deps.push(delta_offset);
        if info.kind == AggKind::Sum && !info.arg_not_null {
            let count = sum_to_count.get(&index).ok_or_else(|| {
                format!("internal error: SUM at mv offset {} has no COUNT(expr) dependency", info.mv_offset)
            })?;
            deps.push(delta_count + agg_cols[*count].info.mv_offset);
        }
        if matches!(info.kind, AggKind::Min | AggKind::Max) {
            for name in [&col.added_count_delta_name, &col.removed_value_delta, &col.removed_count_delta] {
                deps.push(delta_columns.iter().position(|delta| delta == name).ok_or_else(|| {
                    format!("internal error: delta column {name} not found in output")
                })?);
            }
            if let Some(count) = min_max_to_count.get(&index) {
                deps.push(delta_count + agg_cols[*count].info.mv_offset);
            }
        }
        info.dependencies = deps;
        agg_infos.push(info);
    }
    validate_agg_dependencies(&agg_infos, delta_count, columns.len(), expected_len)?;
    let mv_offsets: Vec<usize> = (0..columns.len()).map(|offset| delta_count + offset).collect();
    let mv_table_pk_cols = build_complete_diff_handle_cols(mv, &columns, &mv_offsets, row_id_handle_offset)?;
    let group_key_base_cols = group_key_offsets
        .iter()
        .map(|&mv_offset| group_key_base_col(mv_offset).map(|column| tidb_hack::go_to_lower(&column)))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(BuildResult {
        merge_source_select: merge_select,
        source_column_count: expected_len,
        mv_table_id: mv.id,
        base_table_id,
        mlog_table_id,
        mv_column_count: columns.len(),
        delta_column_count: delta_count,
        mv_table_pk_cols,
        group_key_mv_offsets: group_key_offsets,
        group_key_base_cols,
        count_star_mv_offset,
        agg_infos,
        full_update_lookup_column_count: full_update_lookup_mv_offsets.len(),
        full_update_lookup_template_sql,
        full_update_lookup_mv_offsets,
    })
}

/// Go `aggSumInt` / `aggMaxCount` / `aggMinCount`: the stage-1 aggregate
/// under each alias of `renames` becomes Go's internal aggregate (`SUM_INT`,
/// `MAX_COUNT`, `MIN_COUNT`), which no statement can spell; the alias names
/// the aggregate it was parsed as and the one it becomes.
fn rename_internal_aggregates(select: &mut SelectStmt, renames: &[(String, &'static str, &'static str)]) {
    struct Renamer<'a>(&'a [(String, &'static str, &'static str)]);
    impl tidb_ast::Visitor for Renamer<'_> {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(SelectField::Expr { expr: Expr::Aggregate { name, written, .. }, alias: Some(alias), .. }) =
                node.downcast_mut::<SelectField>()
            {
                if let Some((_, _, to)) =
                    self.0.iter().find(|(candidate, from, _)| candidate == alias && name.eq_ignore_ascii_case(from))
                {
                    *name = to.to_uppercase();
                    *written = Some((*to).to_owned());
                }
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    tidb_ast::Visitable::accept(select, &mut Renamer(renames));
}

/// Go `oracle.ExtractPhysical`.
const fn tso_physical(ts: u64) -> i64 {
    (ts >> 18) as i64
}

/// Go master `SplitMLogCommitTSFilterSelectivity` (`planner/mview/stats.go`):
/// the active FAST refresh's mlog commit-ts filters split off `conds`, with
/// their selectivity over the retained log window. `None` leaves `conds` to
/// the ordinary estimate.
#[must_use]
pub fn split_mlog_commit_ts_filter_selectivity(
    eval: &dyn tidb_exprctx::Columns,
    mlog_table_info: Option<&TableInfo>,
    schema: &tidb_expr::schema::Schema,
    conds: &[tidb_expr::expression::Expression],
) -> Option<(Vec<tidb_expr::expression::Expression>, f64)> {
    let mlog_table_info = mlog_table_info?;
    mlog_table_info.materialized_view_log.as_ref()?;
    let estimation = eval.mlog_commit_ts_estimation()?;
    // The planner may see other mlog tables; the retained window only belongs to the active refresh mlog.
    if mlog_table_info.id != estimation.mlog_table_id {
        return None;
    }
    let commit_ts_col = schema
        .columns
        .iter()
        .find(|col| col.id == tidb_model::column::EXTRA_COMMIT_TS_ID)?;
    let mut filter = MLogCommitTSFilterWindow::default();
    let mut remaining = Vec::with_capacity(conds.len());
    for cond in conds {
        if !filter.add_cond(eval, commit_ts_col, cond) {
            remaining.push(cond.clone());
        }
    }
    if !filter.has_bound() {
        return None;
    }
    let selectivity = estimate_mlog_commit_ts_selectivity(&estimation, mlog_table_info, &filter)?;
    Some((remaining, selectivity))
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BoundOp {
    Gt,
    Ge,
    Lt,
    Le,
}

/// Go master `mlogCommitTSFilterWindow`.
#[derive(Default)]
struct MLogCommitTSFilterWindow {
    lower_tso: Option<u64>,
    upper_tso: Option<u64>,
}

impl MLogCommitTSFilterWindow {
    fn has_bound(&self) -> bool {
        self.lower_tso.is_some() || self.upper_tso.is_some()
    }

    fn add_cond(
        &mut self,
        eval: &dyn tidb_exprctx::Columns,
        commit_ts_col: &tidb_expr::column::Column,
        expr: &tidb_expr::expression::Expression,
    ) -> bool {
        let tidb_expr::expression::Expression::ScalarFunction(sf) = expr else {
            return false;
        };
        let op = match sf.func_name.lowercase() {
            "gt" => BoundOp::Gt,
            "ge" => BoundOp::Ge,
            "lt" => BoundOp::Lt,
            "le" => BoundOp::Le,
            _ => return false,
        };
        let [left, right] = sf.get_args() else {
            return false;
        };
        let Some((op, mut value)) = extract_mlog_commit_ts_filter_bound(eval, commit_ts_col, op, left, right) else {
            return false;
        };
        // The selectivity estimate uses physical TSO milliseconds and models the
        // interval as (lower, upper]. Normalize an inclusive SQL lower bound to the
        // preceding millisecond. Retained lower bounds, including the UpdateTS
        // fallback, must not be adjusted here.
        if op == BoundOp::Ge {
            value = normalize_mlog_commit_ts_inclusive_lower(value);
        }
        match op {
            BoundOp::Gt | BoundOp::Ge => {
                if self.lower_tso.is_none_or(|lower| value > lower) {
                    self.lower_tso = Some(value);
                }
            }
            BoundOp::Lt | BoundOp::Le => {
                if self.upper_tso.is_none_or(|upper| value < upper) {
                    self.upper_tso = Some(value);
                }
            }
        }
        true
    }
}

/// Go master `extractMLogCommitTSFilterBound`.
fn extract_mlog_commit_ts_filter_bound(
    eval: &dyn tidb_exprctx::Columns,
    commit_ts_col: &tidb_expr::column::Column,
    op: BoundOp,
    left: &tidb_expr::expression::Expression,
    right: &tidb_expr::expression::Expression,
) -> Option<(BoundOp, u64)> {
    use tidb_expr::expression::Expression;
    let constant = |expr| match tidb_expr::expr_util::predicates::get_uint64_from_constant(expr, eval) {
        Some((value, false)) => Some(value),
        _ => None,
    };
    if matches!(left, Expression::Column(_)) && commit_ts_col.equal_column(left) {
        return Some((op, constant(right)?));
    }
    if matches!(right, Expression::Column(_)) && commit_ts_col.equal_column(right) {
        let value = constant(left)?;
        let flipped = match op {
            BoundOp::Gt => BoundOp::Lt,
            BoundOp::Ge => BoundOp::Le,
            BoundOp::Lt => BoundOp::Gt,
            BoundOp::Le => BoundOp::Ge,
        };
        return Some((flipped, value));
    }
    None
}

/// Go master `normalizeMLogCommitTSInclusiveLower`.
fn normalize_mlog_commit_ts_inclusive_lower(value: u64) -> u64 {
    let physical = tso_physical(value);
    if physical == 0 {
        return value;
    }
    // Go `oracle.ComposeTS(physical-1, 0)`.
    ((physical - 1) as u64) << 18
}

/// Go master `estimateMLogCommitTSSelectivity`.
fn estimate_mlog_commit_ts_selectivity(
    estimation: &tidb_exprctx::MLogCommitTSEstimation,
    mlog_table_info: &TableInfo,
    filter: &MLogCommitTSFilterWindow,
) -> Option<f64> {
    if estimation.retained_upper_tso == 0 {
        return None;
    }
    let mut retained_lower_tso = estimation.retained_lower_tso;
    if retained_lower_tso == 0 {
        retained_lower_tso = mlog_table_info.update_ts;
    }
    if retained_lower_tso == 0 {
        return None;
    }
    let retained_lower_from_fallback = estimation.retained_lower_tso == 0;

    // Missing filter bounds are unbounded on that side, so use the retained mlog window boundary.
    let filter_lower_tso = filter.lower_tso.map_or(retained_lower_tso, |lower| lower.max(retained_lower_tso));
    let filter_upper_tso = filter
        .upper_tso
        .map_or(estimation.retained_upper_tso, |upper| upper.min(estimation.retained_upper_tso));
    if filter_lower_tso >= filter_upper_tso {
        if retained_lower_from_fallback && filter.lower_tso.is_none_or(|lower| lower <= retained_lower_tso) {
            return None;
        }
        return Some(0.0);
    }

    // This is a physical-time estimate. Logical TSO parts and strict/inclusive bound differences
    // are too fine-grained for this planner-only cardinality hint.
    let retained_duration = tso_physical(estimation.retained_upper_tso) - tso_physical(retained_lower_tso);
    let filter_duration = tso_physical(filter_upper_tso) - tso_physical(filter_lower_tso);
    if retained_duration <= 0 || filter_duration <= 0 {
        return None;
    }
    Some((filter_duration as f64 / retained_duration as f64).clamp(0.0, 1.0))
}

/// Go master `planner/mview/stats_test.go`.
#[cfg(test)]
mod mlog_commit_ts_stats_tests {
    use super::{
        estimate_mlog_commit_ts_selectivity, extract_mlog_commit_ts_filter_bound, normalize_mlog_commit_ts_inclusive_lower,
        split_mlog_commit_ts_filter_selectivity, BoundOp, MLogCommitTSFilterWindow,
    };
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::constant::Constant;
    use tidb_expr::expression::Expression;
    use tidb_exprctx::{Columns, MLogCommitTSEstimation, NoColumns};
    use tidb_model::TableInfo;

    /// Go `oracle.ComposeTS`.
    const fn compose_ts(physical: u64, logical: u64) -> u64 {
        (physical << 18) | logical
    }

    fn commit_ts_col(unique_id: i64) -> Column {
        let mut column = Column::new(unique_id, tidb_model::ColumnInfo::new_extra_commit_ts_col_info().field_type);
        column.id = tidb_model::column::EXTRA_COMMIT_TS_ID;
        column
    }

    fn constant(value: u64) -> Expression {
        Expression::Constant(Constant::new(Datum::UInt(value), FieldType::new(FieldTypeCode::LongLong)))
    }

    fn function(name: &str, left: Expression, right: Expression) -> Expression {
        tidb_expr::new_function_base(&NoColumns, name, FieldType::new(FieldTypeCode::Tiny), vec![left, right]).unwrap()
    }

    /// Go's `mlogStatsTestContext`: a planning context carrying the window.
    struct EstimationContext(MLogCommitTSEstimation);

    impl Columns for EstimationContext {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn mlog_commit_ts_estimation(&self) -> Option<MLogCommitTSEstimation> {
            Some(self.0)
        }
    }

    #[test]
    fn extract_mlog_commit_ts_filter_bound_flips_a_right_hand_column() {
        let col = commit_ts_col(1);
        let other = Expression::Column(commit_ts_col(2));
        let column = Expression::Column(col.clone());
        let cases = [
            ("column on left", BoundOp::Gt, &column, &constant(42), Some(BoundOp::Gt)),
            ("column on right", BoundOp::Gt, &constant(42), &column, Some(BoundOp::Lt)),
            ("column on right greater or equal", BoundOp::Ge, &constant(42), &column, Some(BoundOp::Le)),
            ("column on right less than", BoundOp::Lt, &constant(42), &column, Some(BoundOp::Gt)),
            ("column on right inclusive", BoundOp::Le, &constant(42), &column, Some(BoundOp::Ge)),
            ("different commit ts column", BoundOp::Gt, &other, &constant(42), None),
        ];
        for (name, op, left, right, expected) in cases {
            let got = extract_mlog_commit_ts_filter_bound(&NoColumns, &col, op, left, right);
            assert!(got.map(|(op, _)| op) == expected, "{name}");
            if let Some((_, value)) = got {
                assert_eq!(value, 42, "{name}");
            }
        }
    }

    #[test]
    fn normalize_mlog_commit_ts_inclusive_lower_drops_one_millisecond() {
        assert_eq!(normalize_mlog_commit_ts_inclusive_lower(compose_ts(1000, 123)), compose_ts(999, 0));
        assert_eq!(normalize_mlog_commit_ts_inclusive_lower(compose_ts(0, 123)), compose_ts(0, 123));
    }

    #[test]
    fn mlog_commit_ts_filter_window_normalizes_inclusive_lower() {
        let col = commit_ts_col(1);
        let cond = function("ge", Expression::Column(col.clone()), constant(compose_ts(1000, 123)));
        let mut filter = MLogCommitTSFilterWindow::default();
        assert!(filter.add_cond(&NoColumns, &col, &cond));
        assert_eq!(filter.lower_tso, Some(compose_ts(999, 0)));

        filter.upper_tso = Some(compose_ts(998, 0));
        let estimation = MLogCommitTSEstimation { retained_upper_tso: compose_ts(2000, 0), ..Default::default() };
        let table = TableInfo { update_ts: compose_ts(999, 0), ..TableInfo::default() };
        assert_eq!(estimate_mlog_commit_ts_selectivity(&estimation, &table, &filter), None);
    }

    #[test]
    fn estimate_mlog_commit_ts_selectivity_disjoint_lower() {
        let window = |lower: Option<u64>, upper: Option<u64>| MLogCommitTSFilterWindow { lower_tso: lower, upper_tso: upper };
        let cases = [
            (
                "fallback lower is not proven empty",
                MLogCommitTSEstimation { retained_upper_tso: compose_ts(20, 0), ..Default::default() },
                compose_ts(15, 0),
                window(None, Some(compose_ts(14, 0))),
                None,
            ),
            (
                "retained lower proves empty",
                MLogCommitTSEstimation {
                    retained_lower_tso: compose_ts(15, 0),
                    retained_upper_tso: compose_ts(20, 0),
                    ..Default::default()
                },
                0,
                window(None, Some(compose_ts(14, 0))),
                Some(0.0),
            ),
            (
                "sql lower proves empty despite fallback retained lower",
                MLogCommitTSEstimation { retained_upper_tso: compose_ts(20, 0), ..Default::default() },
                compose_ts(10, 0),
                window(Some(compose_ts(15, 0)), Some(compose_ts(14, 0))),
                Some(0.0),
            ),
            (
                "fallback lower equal upper is not proven empty",
                MLogCommitTSEstimation { retained_upper_tso: compose_ts(20, 0), ..Default::default() },
                compose_ts(15, 0),
                window(None, Some(compose_ts(15, 0))),
                None,
            ),
        ];
        for (name, estimation, update_ts, filter, expected) in cases {
            let table = TableInfo { update_ts, ..TableInfo::default() };
            assert_eq!(estimate_mlog_commit_ts_selectivity(&estimation, &table, &filter), expected, "{name}");
        }
    }

    /// Go master `TestDeriveStatsByFilterUsesMLogCommitTSSelectivity`'s
    /// split half (the derivation half is in `stats/derive_stats_tests.rs`).
    #[test]
    fn split_mlog_commit_ts_filter_selectivity_over_the_retained_window() {
        let table = TableInfo {
            id: 100,
            materialized_view_log: Some(tidb_model::GoShared::new(tidb_model::table::MaterializedViewLogInfo::default())),
            ..TableInfo::default()
        };
        let col = commit_ts_col(1);
        let schema = tidb_expr::schema::Schema::new(vec![col.clone()]);
        let ctx = EstimationContext(MLogCommitTSEstimation {
            mlog_table_id: 100,
            retained_lower_tso: compose_ts(10, 0),
            retained_upper_tso: compose_ts(110, 0),
        });
        let conds = vec![
            function("gt", Expression::Column(col.clone()), constant(compose_ts(20, 0))),
            function("le", Expression::Column(col), constant(compose_ts(50, 0))),
        ];
        let (remaining, selectivity) = split_mlog_commit_ts_filter_selectivity(&ctx, Some(&table), &schema, &conds).unwrap();
        assert!(remaining.is_empty());
        assert!((selectivity - 0.3).abs() < 0.0001);
        // Another mlog table, or no window, keeps the ordinary estimate.
        let other = TableInfo { id: 101, ..table.clone() };
        assert!(split_mlog_commit_ts_filter_selectivity(&ctx, Some(&other), &schema, &conds).is_none());
        assert!(split_mlog_commit_ts_filter_selectivity(&NoColumns, Some(&table), &schema, &conds).is_none());
    }
}
