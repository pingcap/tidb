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
