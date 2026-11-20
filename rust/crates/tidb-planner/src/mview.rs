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

//! Go master `pkg/planner/mview` (`mview.go`, `stats.go`): the merge source
//! of a FAST refresh window (`Build`), the diff source of COMPLETE DELTA
//! APPLY (`BuildCompleteDiffSource`), the layouts their executors read, and
//! the mlog commit-ts selectivity.
//!
//! Go assembles each SELECT as an AST. This port writes each Go builder's
//! statement as SQL and parses it; where Go nests one builder's SELECT in
//! another (the stage-1 delta in the merge source, the probe and lookup
//! sides in the full-update template, the definition in the diff source),
//! the derived table's subquery is the inner builder's own tree. The
//! internal aggregates no statement can spell (Go `aggSumInt`,
//! `aggMaxCount`, `aggMinCount`) are set on the parsed tree.

use std::collections::{HashMap, HashSet};

use tidb_ast::{Expr, JoinNode, QueryStmt, SelectField, SelectStmt, Stmt};
use tidb_model::{ColumnInfo, TableInfo};
use tidb_planner_util::handle_cols::{
    new_common_handles_cols_without_cols_align, new_int_handle_cols, HandleCols,
};

/// Go `deltaTableAlias`.
pub const DELTA_TABLE_ALIAS: &str = "delta";
/// Go `mvTableAlias`.
pub const MV_TABLE_ALIAS: &str = "mv";
/// Go `fullUpdateOuterAlias`.
const FULL_UPDATE_OUTER_ALIAS: &str = "full_outer";
/// Go `fullUpdateInnerAlias`.
const FULL_UPDATE_INNER_ALIAS: &str = "full_inner";
/// Go `deltaCntStarName`.
pub const DELTA_CNT_STAR_NAME: &str = "__mview_delta_cnt_star";
/// Go `mvRowIDName`.
pub const MV_ROW_ID_NAME: &str = "__mview_mv_rowid";
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

/// The diff op `buildCompleteDiffOpExpr` gives a row the view lacks.
pub const DIFF_OP_INSERT: i64 = 1;
/// The diff op of a row the recomputation lacks.
pub const DIFF_OP_DELETE: i64 = 2;
/// The diff op of a row whose payload changed.
pub const DIFF_OP_UPDATE: i64 = 3;

/// Go `planctx.PlanContext` as this package uses it: the session's parser
/// (`parseSelectFromSQL` parses under its configuration and connection
/// charset).
pub type Parse<'a> = &'a dyn Fn(&str) -> Result<Stmt, String>;

/// Go `infoschema.InfoSchema` as this package reads it.
pub trait InfoSchema {
    /// Go `TableInfoByID`.
    fn table_info_by_id(&self, id: i64) -> Option<TableInfo>;
    /// Go `TableItemByID(id).DBName.O`.
    fn table_item_db_name(&self, id: i64) -> Option<String>;
}

impl InfoSchema for tidb_infoschema::SessionExtendedInfoSchema {
    fn table_info_by_id(&self, id: i64) -> Option<TableInfo> {
        tidb_infoschema::SessionExtendedInfoSchema::table_info_by_id(self, id).map(|table| table.read().clone_like_go())
    }

    fn table_item_db_name(&self, id: i64) -> Option<String> {
        self.schema_name_by_table_id(id)
    }
}

/// Go master `FindVisibleIndexesWithPrefixCoveringColumns`: every usable key
/// layout for the MIN/MAX full-update lookup.
#[must_use]
pub fn find_visible_indexes_with_prefix_covering_columns(base_table_info: &TableInfo, group_by_cols: &[String]) -> Vec<String> {
    tidb_mviewutil::find_visible_indexes_with_prefix_covering_columns(base_table_info, group_by_cols)
}

/// Go master `SetFullUpdateLookupIndexHint`: binds the full-update lookup's
/// inner scan to the supporting indexes.
///
/// # Errors
///
/// The template is not the full-update lookup shape, or `index_names` is
/// empty.
pub fn set_full_update_lookup_index_hint(sel: &mut SelectStmt, index_names: &[String]) -> Result<(), String> {
    let base_table_name = full_update_lookup_inner_base_table_name(sel)?;
    if index_names.is_empty() {
        return Err("mview full-update lookup template: no index hint names".to_owned());
    }
    base_table_name.hints = vec![tidb_ast::IndexHint {
        kind: tidb_ast::IndexHintKind::Use,
        scope: tidb_ast::IndexHintScope::All,
        indexes: index_names.to_vec(),
    }];
    Ok(())
}

/// Go master `fullUpdateLookupInnerBaseTableName`.
fn full_update_lookup_inner_base_table_name(sel: &mut SelectStmt) -> Result<&mut tidb_ast::TableRef, String> {
    let Some(from) = sel.from.as_mut() else {
        return Err("mview full-update lookup template: invalid select for index hint".to_owned());
    };
    let Some(JoinNode::Derived { subquery, .. }) = from.right.as_mut() else {
        return Err("mview full-update lookup template: inner table source not found for index hint".to_owned());
    };
    let QueryStmt::Select(inner_sel) = &mut **subquery else {
        return Err("mview full-update lookup template: inner select not found for index hint".to_owned());
    };
    let Some(inner_from) = inner_sel.from.as_mut() else {
        return Err("mview full-update lookup template: inner select not found for index hint".to_owned());
    };
    let JoinNode::Table(base_table_name) = &mut inner_from.left else {
        return Err("mview full-update lookup template: inner base table source not found for index hint".to_owned());
    };
    Ok(base_table_name)
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

/// Go master `BuildResult`: the merge source `Build` produces and the layout
/// the executor merges by. The merge source's output is the delta columns,
/// then the view's columns, then the optional `_tidb_rowid` handle; every
/// offset in `agg_infos` is into that layout.
#[derive(Clone, Debug)]
pub struct BuildResult {
    /// Go `MergeSourceSelect`.
    pub merge_source_select: SelectStmt,
    /// Go `SourceColumnCount`.
    pub source_column_count: usize,
    /// Go `FullUpdateLookupTemplateSelect`: the MIN/MAX fallback's
    /// outer+inner IndexJoin template, absent without MIN/MAX.
    pub full_update_lookup_template_select: Option<SelectStmt>,
    /// Go `FullUpdateLookupColumnCount`.
    pub full_update_lookup_column_count: usize,
    /// Go `FullUpdateLookupMVOffsets`.
    pub full_update_lookup_mv_offsets: Vec<usize>,
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
    pub mv_table_pk_cols: HandleCols,
    /// Go `GroupKeyMVOffsets`.
    pub group_key_mv_offsets: Vec<usize>,
    /// Go `GroupKeyBaseCols`.
    pub group_key_base_cols: Vec<String>,
    /// Go `CountStarMVOffset`.
    pub count_star_mv_offset: usize,
    /// Go `AggInfos`.
    pub agg_infos: Vec<AggInfo>,
}

/// Go master `CompleteDiffBuildResult`: the diff source of COMPLETE DELTA
/// APPLY. Its output is the diff op, the optional `_tidb_rowid` handle, the
/// current row image and the recomputed row image, each in the view's
/// column order.
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
    pub current_handle_cols: HandleCols,
    /// Go `CurrentRowOffsets`.
    pub current_row_offsets: Vec<usize>,
    /// Go `RecomputedRowOffsets`.
    pub recomputed_row_offsets: Vec<usize>,
}

impl CompleteDiffBuildResult {
    /// Go master `ValidateSourceLayout`: the layout against a concrete
    /// schema length (zero: none given).
    ///
    /// # Errors
    ///
    /// The layout is inconsistent, or disagrees with `schema_len`.
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
        if (0..self.current_handle_cols.num_cols())
            .any(|i| self.current_handle_cols.get_col(i).id == tidb_model::column::EXTRA_HANDLE_ID)
        {
            expected += 1;
        }
        if self.source_column_count != expected {
            return Err(format!(
                "complete diff build result: unexpected column count: got {}, expected {expected}",
                self.source_column_count
            ));
        }
        if self.op_col_offset >= self.source_column_count {
            return Err(format!("complete diff build result: invalid OpColOffset {}", self.op_col_offset));
        }
        if self.marker_mv_offset >= self.mv_column_count {
            return Err(format!("complete diff build result: invalid MarkerMVOffset {}", self.marker_mv_offset));
        }
        if self.group_key_mv_offsets.is_empty() {
            return Err("complete diff build result: GroupKeyMVOffsets is empty".to_owned());
        }
        for (i, &off) in self.group_key_mv_offsets.iter().enumerate() {
            if off >= self.mv_column_count {
                return Err(format!("complete diff build result: invalid GroupKeyMVOffsets[{i}]={off}"));
            }
        }
        if self.current_row_offsets.len() != self.mv_column_count {
            return Err(format!(
                "complete diff build result: CurrentRowOffsets length {} != MVColumnCount {}",
                self.current_row_offsets.len(),
                self.mv_column_count
            ));
        }
        for (i, &off) in self.current_row_offsets.iter().enumerate() {
            if off >= self.source_column_count {
                return Err(format!("complete diff build result: invalid CurrentRowOffsets[{i}]={off}"));
            }
        }
        if self.recomputed_row_offsets.len() != self.mv_column_count {
            return Err(format!(
                "complete diff build result: RecomputedRowOffsets length {} != MVColumnCount {}",
                self.recomputed_row_offsets.len(),
                self.mv_column_count
            ));
        }
        for (i, &off) in self.recomputed_row_offsets.iter().enumerate() {
            if off >= self.source_column_count {
                return Err(format!("complete diff build result: invalid RecomputedRowOffsets[{i}]={off}"));
            }
        }
        for i in 0..self.current_handle_cols.num_cols() {
            let index = self.current_handle_cols.get_col(i).index;
            if index < 0 || index as usize >= self.source_column_count {
                return Err(format!("complete diff build result: handle column {i} has invalid index {index}"));
            }
        }
        Ok(())
    }
}

/// Go master `DeltaColumn`: one delta payload column of the merge source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeltaColumn {
    /// Go `Name`.
    pub name: String,
    /// Go `Offset`.
    pub offset: usize,
}

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

impl std::fmt::Display for AggKind {
    /// Go's `%v` of an `AggKind`: its `iota` value (the type has no
    /// `String`).
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::CountStar => 0,
            Self::Count => 1,
            Self::Sum => 2,
            Self::Min => 3,
            Self::Max => 4,
        };
        write!(f, "{value}")
    }
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
    arg_expr: Option<Expr>,
}

/// Go master `buildLocalResult`: the parsed definition and everything
/// derived from it, before the merge source is assembled.
struct BuildLocalResult {
    mv_select: SelectStmt,
    mv_db_name: String,
    mview_table: TableInfo,
    mview_columns: Vec<ColumnInfo>,
    base_table_id: i64,
    mlog_table_id: i64,
    base_table: TableInfo,
    mlog_table: TableInfo,
    group_key_set: HashSet<usize>,
    group_key_offs: Vec<usize>,
    agg_cols: Vec<AggColInfo>,
    has_min_max: bool,
    count_star_mv_offset: usize,
}

/// Go master `Build`: the merge source and its layout for one FAST refresh
/// window (`buildLocal`, then `buildFromLocal`).
///
/// # Errors
///
/// The view, its source or log is malformed for FAST refresh, the
/// definition uses what FAST cannot merge, or a statement fails to parse.
pub fn build(
    sctx: Parse<'_>,
    is: &dyn InfoSchema,
    mv: &TableInfo,
    opt: BuildOptions,
    agg_arg_not_null_by_offset: Option<HashMap<usize, bool>>,
) -> Result<BuildResult, String> {
    let local = build_local(sctx, is, mv)?;
    build_from_local(sctx, local, opt, agg_arg_not_null_by_offset)
}

/// Go master `FastRefreshSource`: the source object (a physical table or a
/// ready materialized view) and its log one FAST refresh reads.
#[derive(Debug, Clone)]
pub struct FastRefreshSource {
    /// Go `BaseTable`.
    pub base_table: TableInfo,
    /// Go `MLogTable`.
    pub mlog_table: TableInfo,
    /// Go `BaseTableID`.
    pub base_table_id: i64,
    /// Go `MLogTableID`.
    pub mlog_table_id: i64,
}

/// Go master `ResolveFastRefreshSource`: validates the source object and the
/// log metadata a single-source FAST refresh needs.
///
/// # Errors
///
/// The view, its source or the source's log is missing, not public, not
/// ready or malformed.
pub fn resolve_fast_refresh_source(is: &dyn InfoSchema, mv: &TableInfo) -> Result<FastRefreshSource, String> {
    let Some(info) = mv.materialized_view.as_ref().map(|info| info.read().clone()) else {
        return Err(format!("table {} is not a materialized view", mv.name.original()));
    };
    if info.base_table_ids.len() != 1 {
        return Err(format!(
            "materialized view {} has invalid base table list size {}",
            mv.name.original(),
            info.base_table_ids.len()
        ));
    }
    let base_table_id = info.base_table_ids[0];
    let base_table = is
        .table_info_by_id(base_table_id)
        .ok_or_else(|| format!("base table id {base_table_id} not found in infoschema"))?;
    if base_table.state != tidb_model::SchemaState::PUBLIC {
        return Err(format!("source object {} is not public", base_table.name.original()));
    }
    if base_table.materialized_view.as_ref().is_some_and(|source| {
        !tidb_model::table::MaterializedViewInfo::get_init_build_state(Some(&source.read())).is_ready()
    }) {
        return Err(format!("source materialized view {} is not ready", base_table.name.original()));
    }
    let mlog_table_id = base_table.materialized_view_base.as_ref().map_or(0, |base| base.read().mlog_id);
    if mlog_table_id == 0 {
        return Err(format!("base table {} has no materialized view log", base_table.name.original()));
    }
    let mlog_table = is
        .table_info_by_id(mlog_table_id)
        .ok_or_else(|| format!("materialized view log table id {mlog_table_id} not found in infoschema"))?;
    if mlog_table.state != tidb_model::SchemaState::PUBLIC {
        return Err(format!("materialized view log {} is not public", mlog_table.name.original()));
    }
    let Some(mlog_info) = mlog_table.materialized_view_log.as_ref().map(|info| info.read().clone()) else {
        return Err(format!("table {} is not a materialized view log", mlog_table.name.original()));
    };
    if mlog_info.base_table_id != base_table_id {
        return Err(format!(
            "materialized view log {} does not belong to base table id {base_table_id}",
            mlog_table.name.original()
        ));
    }
    for required in [
        tidb_model::table::MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME,
        tidb_model::table::MATERIALIZED_VIEW_LOG_DML_TYPE_COLUMN_NAME,
    ] {
        if !has_column(&mlog_table, required) {
            return Err(format!(
                "materialized view log {} missing required column {required}",
                mlog_table.name.original()
            ));
        }
    }
    Ok(FastRefreshSource { base_table, mlog_table, base_table_id, mlog_table_id })
}

/// Go master `buildLocal`: validates the view, its source and log, parses
/// the definition, and derives the layout.
fn build_local(sctx: Parse<'_>, is: &dyn InfoSchema, mv: &TableInfo) -> Result<BuildLocalResult, String> {
    // Stage 0: validate MV/MLog metadata and locate all required tables.
    let fast_source = resolve_fast_refresh_source(is, mv)?;

    // Stage 1: parse MV definition and derive merge key/aggregate layout from it.
    let mv_db_name = db_name_by_table_id(is, mv.id)?;
    let sql_content = mv.materialized_view.as_ref().map(|info| info.read().sql_content.clone()).unwrap_or_default();
    let mv_sel = parse_select_from_sql(sctx, &sql_content)?;

    let group_key_offsets = extract_group_key_offsets_from_mv_select(&mv_sel)?;
    if group_key_offsets.is_empty() {
        return Err("materialized view definition has empty GROUP BY".to_owned());
    }
    // Fast membership check when deciding whether an MV output column is a group key.
    let group_key_set: HashSet<usize> = group_key_offsets.iter().copied().collect();

    let (agg_cols, has_min_max) = extract_agg_infos_from_mv_select(&mv_sel)?;

    let mview_columns: Vec<ColumnInfo> = mv.columns.iter_deref().map(|column| column.read().clone()).collect();
    if mview_columns.len() != mv_sel.fields.fields().len() {
        // This should never happen for valid MV metadata. Keep it as a guard to avoid mismatched join schema.
        return Err(format!(
            "mv columns count {} does not match mv query output {}",
            mview_columns.len(),
            mv_sel.fields.fields().len()
        ));
    }

    let Some(count_star_mv_offset) =
        agg_cols.iter().find(|ac| ac.info.kind == AggKind::CountStar).map(|ac| ac.info.mv_offset)
    else {
        return Err("materialized view definition must include COUNT(*) for mview".to_owned());
    };

    Ok(BuildLocalResult {
        mv_select: mv_sel,
        mv_db_name,
        mview_table: mv.clone_like_go(),
        mview_columns,
        base_table_id: fast_source.base_table_id,
        mlog_table_id: fast_source.mlog_table_id,
        base_table: fast_source.base_table,
        mlog_table: fast_source.mlog_table,
        group_key_set,
        group_key_offs: group_key_offsets,
        agg_cols,
        has_min_max,
        count_star_mv_offset,
    })
}

/// Go master `buildFromLocal`: the merge-source SQL and the dependency
/// layout.
///
/// ```text
/// SELECT <delta payload cols>, <mv cols>, [mv._tidb_rowid AS __mview_mv_rowid]
/// FROM (<stage-1 delta aggregation on mlog>) AS delta
/// LEFT JOIN <mv table> AS mv
///   ON delta.<group_key_1> [= | <=>] mv.<group_key_1> AND ...
/// ```
fn build_from_local(
    sctx: Parse<'_>,
    local: BuildLocalResult,
    opt: BuildOptions,
    agg_arg_not_null_by_offset: Option<HashMap<usize, bool>>,
) -> Result<BuildResult, String> {
    let agg_arg_not_null_by_offset =
        agg_arg_not_null_by_offset.or_else(|| infer_agg_arg_not_null_by_offset(&local)).unwrap_or_default();
    let not_null = |mv_offset: usize| agg_arg_not_null_by_offset.get(&mv_offset).copied().unwrap_or(false);

    // Stage 2: build merge source SQL in two steps:
    //   1) aggregate mlog rows into per-group deltas
    //   2) left join those deltas with current MV snapshot
    let delta_sel = build_m_log_delta_select(
        sctx,
        &local.mv_db_name,
        &local.mlog_table,
        &local.mv_select,
        &local.mview_columns,
        &local.group_key_offs,
        &local.agg_cols,
        opt,
    )?;
    let mut full_update_sel = None;
    let mut full_update_column_count = 0;
    let mut full_update_mv_offsets = Vec::new();
    if local.has_min_max {
        let (sel, mv_offsets) = build_full_update_lookup_template_select(
            sctx,
            &local.mv_db_name,
            &local.base_table,
            &local.mv_select,
            &local.mview_columns,
            &local.group_key_set,
            &local.group_key_offs,
            &local.agg_cols,
        )?;
        full_update_column_count = sel.fields.fields().len();
        if mv_offsets.len() != full_update_column_count {
            return Err(format!(
                "mview: full-update lookup template mv-offset mapping length mismatch: got {}, expected {full_update_column_count}",
                mv_offsets.len()
            ));
        }
        full_update_sel = Some(sel);
        full_update_mv_offsets = mv_offsets;
    }

    let (merge_sel, delta_columns, row_id_handle_offset) = build_merge_source_select(
        sctx,
        &local.mv_db_name,
        &local.mview_table,
        &local.mview_columns,
        &local.group_key_set,
        &local.group_key_offs,
        delta_sel,
        &local.agg_cols,
    )?;

    let mut expected_len = local.mview_columns.len() + delta_columns.len();
    let mv_column_offset_base = delta_columns.len();
    if row_id_handle_offset.is_some() {
        expected_len += 1;
    }

    // TODO: Revisit dependency-check ownership. We currently validate/derive aggregate dependencies
    // in planner mview, but this may be moved to another stage (for example, MV creation-time checks)
    // after we evaluate end-to-end guarantees and maintenance cost.
    let sum_to_count_expr_idx = map_sum_to_count_expr_dependencies(&local.agg_cols, &agg_arg_not_null_by_offset)?;
    let min_max_to_count_expr_idx = map_min_max_to_count_expr_dependencies(&local.agg_cols, &agg_arg_not_null_by_offset)?;

    // Patch delta offsets into AggInfos so executor can read delta payload by offset directly.
    let delta_offset_by_name: HashMap<&str, usize> =
        delta_columns.iter().map(|dc| (dc.name.as_str(), dc.offset)).collect();
    let delta_offset = |name: &str| {
        delta_offset_by_name
            .get(name)
            .copied()
            .ok_or_else(|| format!("internal error: delta column {name} not found in output"))
    };
    let mut out_agg_infos = Vec::with_capacity(local.agg_cols.len());
    for (i, ac) in local.agg_cols.iter().enumerate() {
        let mut di = ac.info.clone();
        di.arg_not_null = not_null(di.mv_offset);
        let mut deps = Vec::with_capacity(5);
        if !ac.delta_name.is_empty() {
            deps.push(delta_offset(&ac.delta_name)?);
        }
        match di.kind {
            AggKind::Sum => {
                if !not_null(di.mv_offset) {
                    let Some(&count_idx) = sum_to_count_expr_idx.get(&i) else {
                        return Err(format!(
                            "internal error: SUM at mv offset {} has no COUNT(expr) dependency",
                            di.mv_offset
                        ));
                    };
                    deps.push(mv_column_offset_base + local.agg_cols[count_idx].info.mv_offset);
                }
            }
            AggKind::Max | AggKind::Min => {
                deps.push(delta_offset(&ac.added_count_delta_name)?);
                deps.push(delta_offset(&ac.removed_value_delta)?);
                deps.push(delta_offset(&ac.removed_count_delta)?);
                if let Some(&count_idx) = min_max_to_count_expr_idx.get(&i) {
                    deps.push(mv_column_offset_base + local.agg_cols[count_idx].info.mv_offset);
                }
            }
            AggKind::CountStar | AggKind::Count => {}
        }
        di.dependencies = deps;
        out_agg_infos.push(di);
    }
    // TODO: See the TODO above; dependency validation location may be adjusted in future.
    validate_agg_dependencies(&out_agg_infos, mv_column_offset_base, local.mview_columns.len(), expected_len)?;

    let mv_table_pk_cols = build_mv_table_pk_handle_cols(&local.mview_table, mv_column_offset_base, row_id_handle_offset)?;
    let group_key_base_cols = group_key_base_col_names_at_offsets(&local.mv_select, &local.group_key_offs)?;

    Ok(BuildResult {
        merge_source_select: merge_sel,
        source_column_count: expected_len,
        full_update_lookup_template_select: full_update_sel,
        full_update_lookup_column_count: full_update_column_count,
        full_update_lookup_mv_offsets: full_update_mv_offsets,
        mv_table_id: local.mview_table.id,
        base_table_id: local.base_table_id,
        mlog_table_id: local.mlog_table_id,
        mv_column_count: local.mview_columns.len(),
        delta_column_count: delta_columns.len(),
        mv_table_pk_cols,
        group_key_mv_offsets: local.group_key_offs.clone(),
        group_key_base_cols,
        count_star_mv_offset: local.count_star_mv_offset,
        agg_infos: out_agg_infos,
    })
}

/// Go `expression.Column{ID, RetType, Index}`.
fn handle_column(id: i64, ret_type: &tidb_datatype::FieldType, index: usize) -> tidb_expr::column::Column {
    let mut col = tidb_expr::column::Column::default();
    col.id = id;
    col.ret_type = Some(ret_type.clone());
    col.index = index as i64;
    col
}

/// Go master `buildMVTablePKHandleCols`: the view's handle columns at their
/// merge-source positions.
fn build_mv_table_pk_handle_cols(
    mv: &TableInfo,
    mv_column_offset_base: usize,
    row_id_handle_offset: Option<usize>,
) -> Result<HandleCols, String> {
    let columns: Vec<ColumnInfo> = mv.columns.iter_deref().map(|column| column.read().clone()).collect();
    let mv_offset_by_col_id: HashMap<i64, usize> =
        columns.iter().enumerate().map(|(i, col)| (col.id, i)).collect();
    let name = mv.name.original();
    if mv.pk_is_handle {
        let Some(pk_col) = mv.get_pk_col_info().map(|col| col.read().clone()) else {
            return Err(format!("mv table {name} has PKIsHandle but no primary key column"));
        };
        let Some(&mv_offset) = mv_offset_by_col_id.get(&pk_col.id) else {
            return Err(format!("mv table {name} primary key column id {} not found in mv columns", pk_col.id));
        };
        return Ok(new_int_handle_cols(handle_column(pk_col.id, &pk_col.field_type, mv_column_offset_base + mv_offset)));
    }
    if mv.is_common_handle {
        let Some(pk_idx) = tidb_table::tables::scan_from_infos::find_primary_index(mv) else {
            return Err(format!("mv table {name} has common handle but no primary index"));
        };
        let mut cols = Vec::new();
        for idx_col in pk_idx.read().columns.iter_deref() {
            let offset = idx_col.read().offset;
            let Some(col) = usize::try_from(offset).ok().and_then(|offset| columns.get(offset)) else {
                return Err(format!(
                    "mv table {name} primary index {} has invalid column offset {offset}",
                    pk_idx.read().name.original()
                ));
            };
            let Some(&mv_offset) = mv_offset_by_col_id.get(&col.id) else {
                return Err(format!("mv table {name} primary key column id {} not found in mv columns", col.id));
            };
            cols.push(handle_column(col.id, &col.field_type, mv_column_offset_base + mv_offset));
        }
        return Ok(new_common_handles_cols_without_cols_align(
            tidb_model::GoShared::new(mv.clone_like_go()),
            pk_idx,
            cols,
        ));
    }
    let Some(row_id_handle_offset) = row_id_handle_offset else {
        return Err(format!(
            "mv table {name} uses extra row-id handle but merge-source output has no _tidb_rowid column"
        ));
    };
    let extra_handle_col = ColumnInfo::new_extra_handle_col_info();
    Ok(new_int_handle_cols(handle_column(extra_handle_col.id, &extra_handle_col.field_type, row_id_handle_offset)))
}

/// Go master `inferAggArgNotNullByOffset`: the aggregates whose base column
/// argument is NOT NULL, which need no COUNT(expr) dependency.
fn infer_agg_arg_not_null_by_offset(local: &BuildLocalResult) -> Option<HashMap<usize, bool>> {
    if local.base_table.columns.is_empty() || local.agg_cols.is_empty() {
        return None;
    }
    let base_col_not_null: HashMap<String, bool> = local
        .base_table
        .columns
        .iter_deref()
        .map(|c| {
            let c = c.read();
            (c.name.lowercase().to_owned(), tidb_mysql::types::has_not_null_flag(c.field_type.flags() as usize))
        })
        .collect();
    let out: HashMap<usize, bool> = local
        .agg_cols
        .iter()
        .filter(|ac| !ac.info.arg_col_name.is_empty() && base_col_not_null.get(&ac.info.arg_col_name).copied().unwrap_or(false))
        .map(|ac| (ac.info.mv_offset, true))
        .collect();
    (!out.is_empty()).then_some(out)
}

/// Go master `parseSelectFromSQL`.
fn parse_select_from_sql(sctx: Parse<'_>, sql: &str) -> Result<SelectStmt, String> {
    match sctx(sql)? {
        Stmt::Query(query) => match &*query {
            QueryStmt::Select(sel) => Ok((**sel).clone()),
            other => Err(format!("expected select statement, got {}", query_type_name(other))),
        },
        other => Err(format!("expected select statement, got {}", stmt_type_name(&other))),
    }
}

/// Go's `%T` of a non-SELECT query statement.
fn query_type_name(query: &QueryStmt) -> &'static str {
    match query {
        QueryStmt::Select(_) => "*ast.SelectStmt",
        _ => "*ast.SetOprStmt",
    }
}

/// Go's `%T` of a statement that is not a query: its variant's AST type.
fn stmt_type_name(stmt: &Stmt) -> String {
    let debug = format!("{stmt:?}");
    let variant = debug.split(|c: char| !c.is_alphanumeric()).next().unwrap_or_default();
    format!("*ast.{variant}Stmt")
}

/// Go master `parseExprFromSQL`.
fn parse_expr_from_sql(sctx: Parse<'_>, expr_sql: &str) -> Result<Expr, String> {
    let sel = parse_select_from_sql(sctx, &format!("select {expr_sql}"))?;
    match sel.fields.fields() {
        [SelectField::Expr { expr, .. }] => Ok(expr.clone()),
        _ => Err("failed to parse expression: expected one select field".to_owned()),
    }
}

/// Go master `cloneExprByRestore`.
fn clone_expr_by_restore(sctx: Parse<'_>, expr: &Expr) -> Result<Expr, String> {
    parse_expr_from_sql(sctx, &restore_expr_strict(expr))
}

/// Go master `restoreExprStrict` (`format.DefaultRestoreFlags`).
fn restore_expr_strict(expr: &Expr) -> String {
    expr.restore()
}

/// Go master `extractGroupKeyOffsetsFromMVSelect`: the GROUP BY columns'
/// offsets in the SELECT list, the join keys between deltas and view rows.
fn extract_group_key_offsets_from_mv_select(sel: &SelectStmt) -> Result<Vec<usize>, String> {
    if sel.group_by.is_empty() {
        return Err("materialized view definition must have GROUP BY".to_owned());
    }
    let mut offset_by_col_name = HashMap::with_capacity(8);
    for (i, f) in sel.fields.fields().iter().enumerate() {
        let SelectField::Expr { expr: Expr::Column(path), .. } = f else {
            continue;
        };
        if let Some(name) = path.last() {
            offset_by_col_name.insert(tidb_hack::go_to_lower(name), i);
        }
    }
    let mut group_offsets = Vec::with_capacity(sel.group_by.len());
    for item in &sel.group_by {
        let Expr::Column(path) = &item.expr else {
            return Err("GROUP BY expression must be a column name for mview".to_owned());
        };
        let name = path.last().cloned().unwrap_or_default();
        let Some(&off) = offset_by_col_name.get(&tidb_hack::go_to_lower(&name)) else {
            return Err(format!("GROUP BY column {name} must appear in SELECT list"));
        };
        group_offsets.push(off);
    }
    Ok(group_offsets)
}

/// Go master `extractAggInfosFromMVSelect`: the supported aggregates of the
/// SELECT list and their stage-1 delta columns.
fn extract_agg_infos_from_mv_select(sel: &SelectStmt) -> Result<(Vec<AggColInfo>, bool), String> {
    let mut agg_cols = Vec::new();
    let mut has_min_max = false;
    for (i, f) in sel.fields.fields().iter().enumerate() {
        let SelectField::Expr { expr: Expr::Aggregate { name, written, distinct, args }, .. } = f else {
            continue;
        };
        let new = |kind, arg_col_name: String, delta_name: String| AggColInfo {
            info: AggInfo { kind, mv_offset: i, arg_col_name, arg_not_null: false, dependencies: Vec::new() },
            delta_name,
            added_count_delta_name: String::new(),
            removed_value_delta: String::new(),
            removed_count_delta: String::new(),
            arg_expr: None,
        };
        let arg_col_name = || match args.first() {
            Some(Expr::Column(path)) => path.last().map(|name| tidb_hack::go_to_lower(name)).unwrap_or_default(),
            _ => String::new(),
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
                    agg_cols.push(new(AggKind::CountStar, String::new(), DELTA_CNT_STAR_NAME.to_owned()));
                    continue;
                }
                let mut ac = new(AggKind::Count, arg_col_name(), format!("__mview_delta_cnt_{i}"));
                ac.arg_expr = args.first().map(strip_column_qualifier);
                agg_cols.push(ac);
            }
            "sum" => {
                if args.len() != 1 {
                    return Err("SUM must have exactly one argument for mview stage-1".to_owned());
                }
                if *distinct {
                    return Err("SUM(DISTINCT ...) is not supported in mview stage-1".to_owned());
                }
                let mut ac = new(AggKind::Sum, arg_col_name(), format!("__mview_delta_sum_{i}"));
                ac.arg_expr = args.first().map(strip_column_qualifier);
                agg_cols.push(ac);
            }
            lower @ ("max" | "min") => {
                let (kind, upper) = if lower == "max" { (AggKind::Max, "MAX") } else { (AggKind::Min, "MIN") };
                if !matches!(args.first(), Some(Expr::Column(_))) {
                    return Err(format!("{upper} argument must be a column name for mview"));
                }
                has_min_max = true;
                let mut ac = new(kind, arg_col_name(), format!("__mview_{lower}_in_added_{i}"));
                ac.added_count_delta_name = format!("__mview_{lower}_cnt_in_added_{i}");
                ac.removed_value_delta = format!("__mview_{lower}_in_removed_{i}");
                ac.removed_count_delta = format!("__mview_{lower}_cnt_in_removed_{i}");
                ac.arg_expr = args.first().map(strip_column_qualifier);
                agg_cols.push(ac);
            }
            _ => {
                let f = written.clone().unwrap_or_else(|| name.clone());
                return Err(format!("unsupported aggregate function {f} in mview stage-1"));
            }
        }
    }
    Ok((agg_cols, has_min_max))
}

/// Go master `mapSumToCountExprDependencies`: for every nullable SUM(expr),
/// the first structurally matching COUNT(expr), which preserves SUM(NULL)
/// semantics.
fn map_sum_to_count_expr_dependencies(
    agg_cols: &[AggColInfo],
    sum_arg_not_null_by_offset: &HashMap<usize, bool>,
) -> Result<HashMap<usize, usize>, String> {
    let mut sum_to_count_idx = HashMap::new();
    for (i, ac) in agg_cols.iter().enumerate() {
        if ac.info.kind != AggKind::Sum {
            continue;
        }
        if sum_arg_not_null_by_offset.get(&ac.info.mv_offset).copied().unwrap_or(false) {
            continue;
        }
        let Some(arg_expr) = &ac.arg_expr else {
            return Err(format!("SUM aggregate argument is nil at mv offset {}", ac.info.mv_offset));
        };
        let match_idx = agg_cols.iter().position(|cand| {
            cand.info.kind == AggKind::Count && cand.arg_expr.as_ref().is_some_and(|cand| expr_structural_equal(arg_expr, cand))
        });
        let Some(match_idx) = match_idx else {
            return Err(format!(
                "SUM expression {} at mv offset {} requires matching COUNT(expr) in SELECT list",
                restore_expr(Some(arg_expr)),
                ac.info.mv_offset
            ));
        };
        sum_to_count_idx.insert(i, match_idx);
    }
    Ok(sum_to_count_idx)
}

/// Go master `mapMinMaxToCountExprDependencies`: an optional first matching
/// COUNT(expr) for a nullable MIN/MAX(expr).
fn map_min_max_to_count_expr_dependencies(
    agg_cols: &[AggColInfo],
    agg_arg_not_null_by_offset: &HashMap<usize, bool>,
) -> Result<HashMap<usize, usize>, String> {
    let mut min_max_to_count_idx = HashMap::new();
    for (i, ac) in agg_cols.iter().enumerate() {
        if ac.info.kind != AggKind::Max && ac.info.kind != AggKind::Min {
            continue;
        }
        if agg_arg_not_null_by_offset.get(&ac.info.mv_offset).copied().unwrap_or(false) {
            continue;
        }
        let Some(arg_expr) = &ac.arg_expr else {
            return Err(format!("{} aggregate argument is nil at mv offset {}", ac.info.kind, ac.info.mv_offset));
        };
        let match_idx = agg_cols.iter().position(|cand| {
            cand.info.kind == AggKind::Count && cand.arg_expr.as_ref().is_some_and(|cand| expr_structural_equal(arg_expr, cand))
        });
        if let Some(match_idx) = match_idx {
            min_max_to_count_idx.insert(i, match_idx);
        }
    }
    Ok(min_max_to_count_idx)
}

/// Go's `%v` of an `[]int`.
fn go_int_slice(values: &[usize]) -> String {
    format!("[{}]", values.iter().map(usize::to_string).collect::<Vec<_>>().join(" "))
}

/// Go master `validateAggDependencies`: each aggregate's dependency layout,
/// and every offset inside the merge-source schema.
fn validate_agg_dependencies(
    agg_infos: &[AggInfo],
    mv_column_offset_base: usize,
    mv_column_count: usize,
    schema_len: usize,
) -> Result<(), String> {
    let mv_column_end = mv_column_offset_base + mv_column_count;
    for ai in agg_infos {
        if ai.mv_offset >= mv_column_count {
            return Err(format!("invalid mv offset {} for {}", ai.mv_offset, ai.kind));
        }
        for &dep in &ai.dependencies {
            if dep >= schema_len {
                return Err(format!("invalid dependency offset {dep} for {} at mv offset {}", ai.kind, ai.mv_offset));
            }
        }
        let deps = &ai.dependencies;
        match ai.kind {
            AggKind::CountStar | AggKind::Count => {
                if deps.len() != 1 {
                    return Err(format!(
                        "{} at mv offset {} expects dependencies [self_delta], got {}",
                        ai.kind,
                        ai.mv_offset,
                        go_int_slice(deps)
                    ));
                }
                if deps[0] >= mv_column_offset_base {
                    return Err(format!("{} at mv offset {} has invalid self_delta offset {}", ai.kind, ai.mv_offset, deps[0]));
                }
            }
            AggKind::Sum => {
                // [self_delta] when SUM argument is NOT NULL; otherwise [self_delta, matched_count_expr_mv].
                if deps.len() != 1 && deps.len() != 2 {
                    return Err(format!(
                        "SUM at mv offset {} expects dependencies [self_delta] or [self_delta, matched_count_expr_mv], got {}",
                        ai.mv_offset,
                        go_int_slice(deps)
                    ));
                }
                if deps[0] >= mv_column_offset_base {
                    return Err(format!("SUM at mv offset {} has invalid self_delta offset {}", ai.mv_offset, deps[0]));
                }
                if deps.len() == 2 && (deps[1] < mv_column_offset_base || deps[1] >= mv_column_end) {
                    return Err(format!(
                        "SUM at mv offset {} has invalid matched_count_expr_mv offset {}",
                        ai.mv_offset, deps[1]
                    ));
                }
            }
            AggKind::Max | AggKind::Min => {
                // [added_val, added_cnt, removed_val, removed_cnt] + optional [matched_count_expr_mv] for nullable arg.
                if deps.len() != 4 && deps.len() != 5 {
                    return Err(format!(
                        "{} at mv offset {} expects dependencies [added_val, added_cnt, removed_val, removed_cnt] or [added_val, added_cnt, removed_val, removed_cnt, matched_count_expr_mv], got {}",
                        ai.kind,
                        ai.mv_offset,
                        go_int_slice(deps)
                    ));
                }
                for (dep_pos, &dep) in deps.iter().take(4).enumerate() {
                    if dep >= mv_column_offset_base {
                        return Err(format!(
                            "{} at mv offset {} has invalid delta dependency[{dep_pos}] offset {dep}",
                            ai.kind, ai.mv_offset
                        ));
                    }
                }
                if deps.len() == 5 && (deps[4] < mv_column_offset_base || deps[4] >= mv_column_end) {
                    return Err(format!(
                        "{} at mv offset {} has invalid matched_count_expr_mv offset {}",
                        ai.kind, ai.mv_offset, deps[4]
                    ));
                }
            }
        }
        if deps.is_empty() {
            return Err(format!("{} at mv offset {} has empty Dependencies", ai.kind, ai.mv_offset));
        }
    }
    Ok(())
}

/// Go master `isCountStarOrOneArg`.
fn is_count_star_or_one_arg(arg: Option<&Expr>) -> bool {
    match arg {
        None => true,
        Some(Expr::Int(digits)) => digits.trim() == "1",
        _ => false,
    }
}

/// Go master `stripColumnQualifier`: the expression without column
/// qualifiers or parentheses.
fn strip_column_qualifier(expr: &Expr) -> Expr {
    struct ColumnQualifierStripper;
    impl tidb_ast::Visitor for ColumnQualifierStripper {
        fn enter(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(Expr::Column(path)) = node.downcast_mut::<Expr>() {
                if path.len() > 1 {
                    let name = path.pop().unwrap_or_default();
                    *path = vec![name];
                }
            }
            false
        }
        fn leave(&mut self, _node: &mut dyn std::any::Any) -> bool {
            true
        }
    }
    let mut expr = expr.clone();
    tidb_ast::Visitable::accept(&mut expr, &mut ColumnQualifierStripper);
    strip_all_parentheses(expr)
}

/// Go master `exprStructuralEqual`: equality up to the outer parentheses
/// (and the parser's positions, which the tree does not compare).
fn expr_structural_equal(lhs: &Expr, rhs: &Expr) -> bool {
    strip_parentheses(lhs) == strip_parentheses(rhs)
}

/// Go master `stripParentheses`: the outer parentheses removed.
fn strip_parentheses(mut expr: &Expr) -> &Expr {
    while let Expr::Paren(inner) = expr {
        expr = inner;
    }
    expr
}

/// Go master `restoreExpr`.
fn restore_expr(expr: Option<&Expr>) -> String {
    expr.map_or_else(|| "<nil>".to_owned(), Expr::restore)
}

/// Go master `stripAllParentheses`.
fn strip_all_parentheses(mut expr: Expr) -> Expr {
    struct ParenthesesStripper;
    impl tidb_ast::Visitor for ParenthesesStripper {
        fn enter(&mut self, _node: &mut dyn std::any::Any) -> bool {
            false
        }
        fn leave(&mut self, node: &mut dyn std::any::Any) -> bool {
            if let Some(expr) = node.downcast_mut::<Expr>() {
                while let Expr::Paren(inner) = expr {
                    let inner = std::mem::replace(&mut **inner, Expr::Null);
                    *expr = inner;
                }
            }
            true
        }
    }
    tidb_ast::Visitable::accept(&mut expr, &mut ParenthesesStripper);
    while let Expr::Paren(inner) = expr {
        expr = *inner;
    }
    expr
}

/// The MV WHERE predicate as the stage-1, probe and lookup selects read it:
/// cloned, unqualified, without parentheses (Go `cloneExprByRestore`, then
/// `columnQualifierStripper` and `stripAllParentheses`).
fn stripped_mv_where(sctx: Parse<'_>, mv_sel: &SelectStmt) -> Result<Option<Expr>, String> {
    mv_sel
        .where_clause
        .as_ref()
        .map(|mv_where| clone_expr_by_restore(sctx, mv_where).map(|mv_where| strip_column_qualifier(&mv_where)))
        .transpose()
}

/// The internal aggregates (Go `aggSumInt`, `aggMaxCount`, `aggMinCount`)
/// of the select fields aliased `renames`: each was parsed as the
/// aggregate it renames.
fn set_internal_aggregates(sel: &mut SelectStmt, renames: &[(String, &'static str)]) {
    for field in sel.fields.fields_mut() {
        let SelectField::Expr { expr: Expr::Aggregate { name, written, .. }, alias: Some(alias), .. } = field else {
            continue;
        };
        if let Some((_, to)) = renames.iter().find(|(candidate, _)| candidate == alias) {
            *name = to.to_uppercase();
            *written = Some((*to).to_owned());
        }
    }
}

/// The derived table `alias` of `sel`'s FROM, whose subquery becomes `sub`.
fn set_derived_table(sel: &mut SelectStmt, alias: &str, sub: SelectStmt) -> Result<(), String> {
    let Some(from) = sel.from.as_mut() else {
        return Err(format!("mview: derived table {alias} not found"));
    };
    let mut nodes = vec![&mut from.left];
    if let Some(right) = from.right.as_mut() {
        nodes.push(right);
    }
    for node in nodes {
        if let JoinNode::Derived { subquery, alias: Some(name), .. } = node {
            if name == alias {
                **subquery = QueryStmt::Select(Box::new(sub));
                return Ok(());
            }
        }
    }
    Err(format!("mview: derived table {alias} not found"))
}

/// Go master `buildMLogDeltaSelect`: the stage-1 aggregation of the log rows
/// inside `(FromTS, ToTS]`.
///
/// ```text
/// SELECT <group keys>, SUM_INT(old_new) AS __mview_delta_cnt_star,
///        SUM_INT(IF(<count arg> IS NOT NULL, old_new, 0)) AS __mview_delta_cnt_<i>,
///        SUM(IF(old_new = 1, <sum arg>, -<sum arg>)) AS __mview_delta_sum_<i>,
///        MAX/MIN(IF(old_new = 1, <arg>, NULL)), MAX/MIN_COUNT(...), ...
/// FROM <db>.<mlog>
/// WHERE _tidb_commit_ts > FromTS [AND _tidb_commit_ts <= ToTS] AND <MV WHERE>
/// GROUP BY <group keys>
/// ```
#[allow(clippy::too_many_arguments)]
fn build_m_log_delta_select(
    sctx: Parse<'_>,
    db_name: &str,
    mlog_table: &TableInfo,
    mv_sel: &SelectStmt,
    mv_cols: &[ColumnInfo],
    group_key_offsets: &[usize],
    agg_cols: &[AggColInfo],
    opt: BuildOptions,
) -> Result<SelectStmt, String> {
    let build_m_log_where = || -> Result<String, String> {
        let ts_col = col_expr(tidb_model::column::EXTRA_COMMIT_TS_NAME);
        let mut r#where = format!("{ts_col} > {}", opt.from_ts);
        if opt.to_ts > 0 {
            r#where = and_expr(&r#where, &format!("{ts_col} <= {}", opt.to_ts));
        }
        if let Some(mv_where) = stripped_mv_where(sctx, mv_sel)? {
            r#where = and_expr(&r#where, &mv_where.restore());
        }
        Ok(r#where)
    };

    let mut group_key_base_col_by_mv_offset = HashMap::with_capacity(group_key_offsets.len());
    let mut group_by = Vec::with_capacity(group_key_offsets.len());
    for &mv_offset in group_key_offsets {
        let base_col = group_key_base_col_expr_at_offset(mv_sel, mv_offset)?;
        group_by.push(col_expr(&base_col));
        group_key_base_col_by_mv_offset.insert(mv_offset, base_col);
    }

    let mut phase1_fields = Vec::with_capacity(group_key_offsets.len() + 1 + agg_cols.len() + 1);
    for &mv_offset in group_key_offsets {
        phase1_fields.push(field(
            &col_expr(&group_key_base_col_by_mv_offset[&mv_offset]),
            mv_cols[mv_offset].name.original(),
        ));
    }
    let old_new_col = col_expr(tidb_model::table::MATERIALIZED_VIEW_LOG_OLD_NEW_COLUMN_NAME);
    let sum_int = tidb_expression_aggregation::names::SUM_INT;
    let mut internal = vec![(DELTA_CNT_STAR_NAME.to_owned(), sum_int)];
    phase1_fields.push(field(&format!("SUM({old_new_col})"), DELTA_CNT_STAR_NAME));

    for ac in agg_cols {
        match ac.info.kind {
            AggKind::CountStar => {}
            AggKind::Count => {
                let Some(arg) = &ac.arg_expr else {
                    return Err("COUNT aggregate argument is nil for mview".to_owned());
                };
                let arg_expr = clone_expr_by_restore(sctx, arg)?.restore();
                let cond = format!("{arg_expr} IS NOT NULL");
                phase1_fields.push(field(&format!("SUM({})", if_expr(&cond, &old_new_col, "0")), &ac.delta_name));
                internal.push((ac.delta_name.clone(), sum_int));
            }
            AggKind::Sum => {
                let Some(arg) = &ac.arg_expr else {
                    return Err("SUM aggregate argument is nil for mview".to_owned());
                };
                let arg_expr = clone_expr_by_restore(sctx, arg)?.restore();
                let added_cond = format!("{old_new_col} = 1");
                phase1_fields.push(field(
                    &format!("SUM({})", if_expr(&added_cond, &arg_expr, &format!("-({arg_expr})"))),
                    &ac.delta_name,
                ));
            }
            AggKind::Max | AggKind::Min => {
                let (func, count, upper) = if ac.info.kind == AggKind::Max {
                    ("MAX", tidb_expression_aggregation::names::MAX_COUNT, "MAX")
                } else {
                    ("MIN", tidb_expression_aggregation::names::MIN_COUNT, "MIN")
                };
                if ac.info.arg_col_name.is_empty() {
                    return Err(format!("{upper} aggregate argument column is empty for mview"));
                }
                let arg_col = col_expr(&ac.info.arg_col_name);
                let added_arg = if_expr(&format!("{old_new_col} = 1"), &arg_col, "NULL");
                let removed_arg = if_expr(&format!("{old_new_col} = -1"), &arg_col, "NULL");
                phase1_fields.push(field(&format!("{func}({added_arg})"), &ac.delta_name));
                phase1_fields.push(field(&format!("{func}({added_arg})"), &ac.added_count_delta_name));
                phase1_fields.push(field(&format!("{func}({removed_arg})"), &ac.removed_value_delta));
                phase1_fields.push(field(&format!("{func}({removed_arg})"), &ac.removed_count_delta));
                internal.push((ac.added_count_delta_name.clone(), count));
                internal.push((ac.removed_count_delta.clone(), count));
            }
        }
    }

    let mlog = qual_table(db_name, mlog_table.name.original());
    let phase1_where = build_m_log_where()?;
    let sql = format!(
        "SELECT /*+ {} */ {} FROM {mlog} WHERE {phase1_where} GROUP BY {}",
        build_read_from_storage_tiflash_hint(db_name, mlog_table.name.original()),
        phase1_fields.join(", "),
        group_by.join(", ")
    );
    let mut sel = parse_select_from_sql(sctx, &sql)?;
    set_internal_aggregates(&mut sel, &internal);
    Ok(sel)
}

/// Go master `buildMergeSourceSelect`: the stage-1 deltas LEFT JOIN the
/// view's current rows, with the delta payload first, then the view's
/// columns (group keys from the delta side), then the optional row-id
/// handle. Nullable group keys join by `<=>`, NOT NULL ones by `=`.
#[allow(clippy::too_many_arguments)]
fn build_merge_source_select(
    sctx: Parse<'_>,
    db_name: &str,
    mv: &TableInfo,
    mv_cols: &[ColumnInfo],
    group_key_set: &HashSet<usize>,
    group_key_offsets: &[usize],
    delta_sel: SelectStmt,
    agg_cols: &[AggColInfo],
) -> Result<(SelectStmt, Vec<DeltaColumn>, Option<usize>), String> {
    // Build join conditions on group keys. Nullable MV keys need <=> to preserve NULL-group
    // semantics; NOT NULL MV keys can use regular equality.
    let mut on_expr: Option<String> = None;
    for &mv_offset in group_key_offsets {
        let mv_col_info = &mv_cols[mv_offset];
        let col_name = mv_col_info.name.original();
        let op = if tidb_mysql::types::has_not_null_flag(mv_col_info.field_type.flags() as usize) { "=" } else { "<=>" };
        let cmp = binary(op, &qual_col_expr(DELTA_TABLE_ALIAS, col_name), &qual_col_expr(MV_TABLE_ALIAS, col_name));
        on_expr = Some(match on_expr {
            None => cmp,
            Some(on_expr) => and_expr(&on_expr, &cmp),
        });
    }
    let Some(on_expr) = on_expr else {
        return Err("empty join key for mview".to_owned());
    };

    // SELECT: delta columns first, then MV columns.
    let mut fields = Vec::with_capacity(mv_cols.len() + 1 + agg_cols.len() + 1);
    let mut delta_columns: Vec<DeltaColumn> = Vec::with_capacity(1 + agg_cols.len());
    let mut add_delta_col = |name: &str, fields: &mut Vec<String>| {
        let offset = delta_columns.len();
        delta_columns.push(DeltaColumn { name: name.to_owned(), offset });
        fields.push(field(&qual_col_expr(DELTA_TABLE_ALIAS, name), name));
    };
    // delta_cnt_star is always included.
    add_delta_col(DELTA_CNT_STAR_NAME, &mut fields);
    for ac in agg_cols {
        match ac.info.kind {
            AggKind::CountStar => {}
            AggKind::Max | AggKind::Min => {
                for name in [&ac.delta_name, &ac.added_count_delta_name, &ac.removed_value_delta, &ac.removed_count_delta] {
                    add_delta_col(name, &mut fields);
                }
            }
            AggKind::Count | AggKind::Sum => add_delta_col(&ac.delta_name, &mut fields),
        }
    }
    for (i, col) in mv_cols.iter().enumerate() {
        let alias = if group_key_set.contains(&i) { DELTA_TABLE_ALIAS } else { MV_TABLE_ALIAS };
        fields.push(field(&qual_col_expr(alias, col.name.original()), col.name.original()));
    }
    // Keep extra row-id handle outside delta payload so delta offsets and MV offsets stay stable.
    let mut row_id_handle_offset = None;
    if !mv.pk_is_handle && !mv.is_common_handle {
        row_id_handle_offset = Some(fields.len());
        fields.push(field(&qual_col_expr(MV_TABLE_ALIAS, tidb_model::column::EXTRA_HANDLE_NAME), MV_ROW_ID_NAME));
    }

    // Use delta as left side so every changed group survives even when MV row doesn't exist yet.
    let sql = format!(
        "SELECT {} FROM (SELECT 1) AS {} LEFT JOIN {} AS {} ON {on_expr}",
        fields.join(", "),
        escape(DELTA_TABLE_ALIAS),
        qual_table(db_name, mv.name.original()),
        escape(MV_TABLE_ALIAS),
    );
    let mut sel = parse_select_from_sql(sctx, &sql)?;
    set_derived_table(&mut sel, DELTA_TABLE_ALIAS, delta_sel)?;
    Ok((sel, delta_columns, row_id_handle_offset))
}

/// Go master `buildFullUpdateLookupTemplateSelect`: the MIN/MAX fallback's
/// template, an outer probe-key side IndexJoin-ed (`INL_JOIN`) to the
/// group's recomputation from the base table.
///
/// ```text
/// SELECT <group keys and MIN/MAX columns in MV layout>
/// FROM (<outer probe select>) AS full_outer
/// JOIN (<inner recomputation select>) AS full_inner
///   ON full_outer.__mview_full_outer_gk_<i> <=> full_inner.<base_group_key_i> AND ...
/// ```
#[allow(clippy::too_many_arguments)]
fn build_full_update_lookup_template_select(
    sctx: Parse<'_>,
    db_name: &str,
    base_table: &TableInfo,
    mv_sel: &SelectStmt,
    mv_cols: &[ColumnInfo],
    group_key_set: &HashSet<usize>,
    group_key_offsets: &[usize],
    agg_cols: &[AggColInfo],
) -> Result<(SelectStmt, Vec<usize>), String> {
    let (outer_sel, outer_gk_alias_by_mv_offset) =
        build_full_update_lookup_outer_select(sctx, db_name, base_table, mv_sel, group_key_offsets)?;
    let mut group_key_base_col_by_mv_offset = HashMap::with_capacity(group_key_offsets.len());
    for &mv_offset in group_key_offsets {
        group_key_base_col_by_mv_offset.insert(mv_offset, group_key_base_col_expr_at_offset(mv_sel, mv_offset)?);
    }
    let agg_kind_by_mv_offset: HashMap<usize, AggKind> =
        agg_cols.iter().map(|ac| (ac.info.mv_offset, ac.info.kind)).collect();
    let inner_sel = build_full_update_lookup_inner_select(
        sctx,
        db_name,
        base_table,
        mv_sel,
        mv_cols,
        group_key_set,
        group_key_offsets,
        &group_key_base_col_by_mv_offset,
        agg_cols,
    )?;

    let mut on_expr: Option<String> = None;
    for mv_offset in group_key_offsets {
        let outer_gk = qual_col_expr(FULL_UPDATE_OUTER_ALIAS, &outer_gk_alias_by_mv_offset[mv_offset]);
        let inner_gk = qual_col_expr(FULL_UPDATE_INNER_ALIAS, &group_key_base_col_by_mv_offset[mv_offset]);
        // Group keys can be NULL, so full-update lookup must use null-safe equality.
        let cmp = binary("<=>", &outer_gk, &inner_gk);
        on_expr = Some(match on_expr {
            None => cmp,
            Some(on_expr) => and_expr(&on_expr, &cmp),
        });
    }
    let Some(on_expr) = on_expr else {
        return Err("mview: empty group key offsets for full-update lookup template".to_owned());
    };

    let mut fields = Vec::with_capacity(mv_cols.len());
    let mut mv_offsets = Vec::with_capacity(mv_cols.len());
    for (mv_offset, mv_col) in mv_cols.iter().enumerate() {
        // Full-update fallback only returns group keys and MIN/MAX aggregate columns.
        if agg_kind_by_mv_offset.get(&mv_offset).is_some_and(|kind| *kind != AggKind::Min && *kind != AggKind::Max) {
            continue;
        }
        let out_col_name = group_key_base_col_by_mv_offset
            .get(&mv_offset)
            .cloned()
            .unwrap_or_else(|| mv_col.name.original().to_owned());
        fields.push(field(&qual_col_expr(FULL_UPDATE_INNER_ALIAS, &out_col_name), mv_col.name.original()));
        mv_offsets.push(mv_offset);
    }

    // Keep the template stable on index-join path so planbuilder can extract
    // inner child + range/key mapping as executor rebuild metadata.
    // Internal MV refresh planning may run on a system session with empty
    // CurrentDB, so attach DBName explicitly to keep this join hint matchable.
    let sql = format!(
        "SELECT /*+ INL_JOIN({}, {}) */ {} FROM (SELECT 1) AS {} JOIN (SELECT 1) AS {} ON {on_expr}",
        qual_table(db_name, FULL_UPDATE_OUTER_ALIAS),
        qual_table(db_name, FULL_UPDATE_INNER_ALIAS),
        fields.join(", "),
        escape(FULL_UPDATE_OUTER_ALIAS),
        escape(FULL_UPDATE_INNER_ALIAS),
    );
    let mut sel = parse_select_from_sql(sctx, &sql)?;
    set_derived_table(&mut sel, FULL_UPDATE_OUTER_ALIAS, outer_sel)?;
    set_derived_table(&mut sel, FULL_UPDATE_INNER_ALIAS, inner_sel)?;
    Ok((sel, mv_offsets))
}

/// Go master `buildFullUpdateLookupOuterSelect`: the probe side, one tuple
/// of group keys the executor refills per changed group.
///
/// ```text
/// SELECT <group_key_i> AS __mview_full_outer_gk_<mv_offset_i>
/// FROM <db>.<base_table> WHERE <MV WHERE predicate> LIMIT 1
/// ```
fn build_full_update_lookup_outer_select(
    sctx: Parse<'_>,
    db_name: &str,
    base_table: &TableInfo,
    mv_sel: &SelectStmt,
    group_key_offsets: &[usize],
) -> Result<(SelectStmt, HashMap<usize, String>), String> {
    if group_key_offsets.is_empty() {
        return Err("mview: empty group key offsets for full-update lookup template".to_owned());
    }
    let mut fields = Vec::with_capacity(group_key_offsets.len());
    let mut group_key_alias_by_mv_offset = HashMap::with_capacity(group_key_offsets.len());
    for &mv_offset in group_key_offsets {
        let base_col = group_key_base_col_expr_at_offset(mv_sel, mv_offset)?;
        let alias = format!("__mview_full_outer_gk_{mv_offset}");
        fields.push(field(&col_expr(&base_col), &alias));
        group_key_alias_by_mv_offset.insert(mv_offset, alias);
    }
    let r#where = stripped_mv_where(sctx, mv_sel)?.map(|r#where| format!(" WHERE {}", r#where.restore()));
    // Outer side only provides one probe key tuple; runtime will refill real keys per changed group.
    let sql = format!(
        "SELECT {} FROM {}{} LIMIT 1",
        fields.join(", "),
        qual_table(db_name, base_table.name.original()),
        r#where.unwrap_or_default()
    );
    Ok((parse_select_from_sql(sctx, &sql)?, group_key_alias_by_mv_offset))
}

/// Go master `buildFullUpdateLookupInnerSelect`: the recomputation of one
/// group's keys and MIN/MAX columns from the base table.
///
/// ```text
/// SELECT <group_key_i> AS <base_group_key_i>, MIN/MAX(<arg>) AS <mv_agg_col>
/// FROM <db>.<base_table> WHERE <MV WHERE predicate> GROUP BY <group keys>
/// ```
#[allow(clippy::too_many_arguments)]
fn build_full_update_lookup_inner_select(
    sctx: Parse<'_>,
    db_name: &str,
    base_table: &TableInfo,
    mv_sel: &SelectStmt,
    mv_cols: &[ColumnInfo],
    group_key_set: &HashSet<usize>,
    group_key_offsets: &[usize],
    group_key_base_col_by_mv_offset: &HashMap<usize, String>,
    agg_cols: &[AggColInfo],
) -> Result<SelectStmt, String> {
    if group_key_offsets.is_empty() {
        return Err("mview: empty group key offsets for full-update lookup template".to_owned());
    }
    let agg_by_mv_offset: HashMap<usize, &AggColInfo> = agg_cols.iter().map(|ac| (ac.info.mv_offset, ac)).collect();
    let mut fields = Vec::with_capacity(mv_cols.len());
    for (i, mv_col) in mv_cols.iter().enumerate() {
        if group_key_set.contains(&i) {
            let base_col = group_key_base_col_expr_at_offset(mv_sel, i)?;
            fields.push(field(&col_expr(&base_col), &group_key_base_col_by_mv_offset[&i]));
            continue;
        }
        let Some(ac) = agg_by_mv_offset.get(&i) else {
            return Err(format!("mv offset {i} is neither group key nor aggregate"));
        };
        // Full-update fallback only recomputes MIN/MAX aggregates.
        if ac.info.kind != AggKind::Min && ac.info.kind != AggKind::Max {
            continue;
        }
        fields.push(field(&build_full_update_agg_expr(sctx, ac)?, mv_col.name.original()));
    }
    let r#where = stripped_mv_where(sctx, mv_sel)?.map(|r#where| format!(" WHERE {}", r#where.restore()));
    let mut group_by = Vec::with_capacity(group_key_offsets.len());
    for &mv_offset in group_key_offsets {
        group_by.push(col_expr(&group_key_base_col_expr_at_offset(mv_sel, mv_offset)?));
    }
    let sql = format!(
        "SELECT {} FROM {}{} GROUP BY {}",
        fields.join(", "),
        qual_table(db_name, base_table.name.original()),
        r#where.unwrap_or_default(),
        group_by.join(", ")
    );
    parse_select_from_sql(sctx, &sql)
}

/// Go master `buildFullUpdateAggExpr`.
fn build_full_update_agg_expr(sctx: Parse<'_>, ac: &AggColInfo) -> Result<String, String> {
    match ac.info.kind {
        AggKind::CountStar => Ok("COUNT(1)".to_owned()),
        AggKind::Count => {
            let Some(arg) = &ac.arg_expr else {
                return Err("COUNT aggregate argument is nil for full-update".to_owned());
            };
            Ok(format!("COUNT({})", clone_expr_by_restore(sctx, arg)?.restore()))
        }
        AggKind::Sum => {
            let Some(arg) = &ac.arg_expr else {
                return Err("SUM aggregate argument is nil for full-update".to_owned());
            };
            Ok(format!("SUM({})", clone_expr_by_restore(sctx, arg)?.restore()))
        }
        AggKind::Max => {
            if ac.info.arg_col_name.is_empty() {
                return Err("MAX aggregate argument column is empty for full-update".to_owned());
            }
            Ok(format!("MAX({})", col_expr(&ac.info.arg_col_name)))
        }
        AggKind::Min => {
            if ac.info.arg_col_name.is_empty() {
                return Err("MIN aggregate argument column is empty for full-update".to_owned());
            }
            Ok(format!("MIN({})", col_expr(&ac.info.arg_col_name)))
        }
    }
}

/// Go master `groupKeyBaseColExprAtOffset`: the base column (by its
/// written name, unqualified) of one group-key output.
fn group_key_base_col_expr_at_offset(mv_sel: &SelectStmt, mv_offset: usize) -> Result<String, String> {
    match mv_sel.fields.fields().get(mv_offset) {
        None => Err(format!("invalid mv offset {mv_offset}")),
        Some(SelectField::Expr { expr: Expr::Column(path), .. }) => Ok(path.last().cloned().unwrap_or_default()),
        Some(_) => Err(format!("mv field at offset {mv_offset} is not a group key column")),
    }
}

/// Go master `groupKeyBaseColNamesAtOffsets`: lowercase.
fn group_key_base_col_names_at_offsets(mv_sel: &SelectStmt, group_key_offsets: &[usize]) -> Result<Vec<String>, String> {
    group_key_offsets
        .iter()
        .map(|&mv_offset| group_key_base_col_expr_at_offset(mv_sel, mv_offset).map(|name| tidb_hack::go_to_lower(&name)))
        .collect()
}

/// Go master `dbNameByTableID`.
fn db_name_by_table_id(is: &dyn InfoSchema, table_id: i64) -> Result<String, String> {
    match is.table_item_db_name(table_id) {
        None => Err(format!("table id {table_id} not found in infoschema")),
        Some(name) if name.is_empty() => Err(format!("table id {table_id} has empty schema name in infoschema")),
        Some(name) => Ok(name),
    }
}

/// Go master `hasColumn`.
fn has_column(t: &TableInfo, col_name: &str) -> bool {
    let col_name = tidb_hack::go_to_lower(col_name);
    t.columns.iter_deref().any(|c| c.read().name.lowercase() == col_name)
}

/// A back-quoted identifier.
fn escape(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Go `ast.TableName{Schema, Name}`.
fn qual_table(db_name: &str, table: &str) -> String {
    format!("{}.{}", escape(db_name), escape(table))
}

/// Go master `qualColExpr`.
fn qual_col_expr(table: &str, col: &str) -> String {
    format!("{}.{}", escape(table), escape(col))
}

/// Go master `colExpr`.
fn col_expr(col: &str) -> String {
    escape(col)
}

/// Go master `binary`.
fn binary(op: &str, l: &str, r: &str) -> String {
    format!("{l} {op} {r}")
}

/// Go master `andExpr`.
fn and_expr(l: &str, r: &str) -> String {
    format!("{l} AND {r}")
}

/// Go master `orExpr`.
fn or_expr(l: &str, r: &str) -> String {
    format!("{l} OR {r}")
}

/// Go `ast.SelectField{Expr, AsName}`.
fn field(expr: &str, as_name: &str) -> String {
    format!("{expr} AS {}", escape(as_name))
}

/// Go master `buildReadFromStorageTiFlashHint`.
fn build_read_from_storage_tiflash_hint(db_name: &str, table_name: &str) -> String {
    format!("READ_FROM_STORAGE(TIFLASH[{}])", qual_table(db_name, table_name))
}

/// Go master `buildHashJoinProbeHint`.
fn build_hash_join_probe_hint(db_name: &str, table_name: &str) -> String {
    format!("HASH_JOIN_PROBE({})", qual_table(db_name, table_name))
}

/// Go master `ifExpr`.
fn if_expr(cond: &str, true_expr: &str, false_expr: &str) -> String {
    format!("IF({cond}, {true_expr}, {false_expr})")
}

/// Go master `validateCompleteDiffSelectFields`. (Go's nil field list has
/// no Rust form: a parsed SELECT always has its list.)
fn validate_complete_diff_select_fields(mv_sel: &SelectStmt, mv: &TableInfo) -> Result<(), String> {
    let columns = mv.columns.len();
    if mv_sel.fields.fields().len() != columns {
        return Err(format!(
            "materialized view {} select output count {} does not match mv column count {columns}",
            mv.name.original(),
            mv_sel.fields.fields().len()
        ));
    }
    Ok(())
}

/// Go master `BuildCompleteDiffSource`: the diff-source SELECT and its
/// layout for COMPLETE DELTA APPLY, a full outer join of the recomputed
/// definition and the view's current rows.
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
/// # Errors
///
/// The view is malformed for a diff, or a statement fails to parse.
pub fn build_complete_diff_source(
    sctx: Parse<'_>,
    is: &dyn InfoSchema,
    mv: &TableInfo,
) -> Result<CompleteDiffBuildResult, String> {
    let name = mv.name.original();
    let Some(info) = mv.materialized_view.as_ref().map(|info| info.read().clone()) else {
        return Err(format!("table {name} is not a materialized view"));
    };
    if info.sql_content.is_empty() {
        return Err(format!("materialized view {name} has empty SQLContent"));
    }
    let mut mv_sel = parse_select_from_sql(sctx, &info.sql_content)?;
    validate_complete_diff_select_fields(&mv_sel, mv)?;
    let mv_columns: Vec<ColumnInfo> = mv.columns.iter_deref().map(|column| column.read().clone()).collect();
    for (i, f) in mv_sel.fields.fields_mut().iter_mut().enumerate() {
        let SelectField::Expr { alias, .. } = f else {
            return Err(format!("materialized view {name} select field {i} is nil"));
        };
        *alias = Some(mv_columns[i].name.original().to_owned());
    }

    let group_key_offsets = extract_group_key_offsets_from_mv_select(&mv_sel)?;
    if group_key_offsets.is_empty() {
        return Err(format!("materialized view {name} has empty group key offsets"));
    }
    let mut group_key_set = HashSet::with_capacity(group_key_offsets.len());
    for &off in &group_key_offsets {
        if off >= mv_columns.len() {
            return Err(format!("materialized view {name} has invalid group key offset {off}"));
        }
        group_key_set.insert(off);
    }

    let (marker_offset, marker_col) = pick_complete_diff_marker_column(mv)?;
    let db_name = db_name_by_table_id(is, mv.id)?;

    let not_null = |col: &ColumnInfo| tidb_mysql::types::has_not_null_flag(col.field_type.flags() as usize);
    let mut join_cond: Option<String> = None;
    for &off in &group_key_offsets {
        let col_name = mv_columns[off].name.original();
        let op = if not_null(&mv_columns[off]) { "=" } else { "<=>" };
        let cmp = binary(
            op,
            &qual_col_expr(DIFF_RECOMPUTED_ALIAS, col_name),
            &qual_col_expr(DIFF_CURRENT_ALIAS, col_name),
        );
        join_cond = Some(match join_cond {
            None => cmp,
            Some(join_cond) => and_expr(&join_cond, &cmp),
        });
    }
    let Some(join_cond) = join_cond else {
        return Err(format!("materialized view {name} has empty join condition"));
    };

    let recomputed_marker_expr = qual_col_expr(DIFF_RECOMPUTED_ALIAS, marker_col.name.original());
    let current_marker_expr = qual_col_expr(DIFF_CURRENT_ALIAS, marker_col.name.original());

    let payload_eq = build_complete_diff_payload_eq_expr(mv, &group_key_set, DIFF_RECOMPUTED_ALIAS, DIFF_CURRENT_ALIAS)
        .unwrap_or_else(|| "1".to_owned());
    let payload_changed = format!("NOT ({payload_eq})");
    let where_expr = or_expr(
        &or_expr(&format!("{recomputed_marker_expr} IS NULL"), &format!("{current_marker_expr} IS NULL")),
        &payload_changed,
    );

    let use_row_id = !mv.pk_is_handle && !mv.is_common_handle;
    let mut fields = Vec::with_capacity(2 + mv_columns.len() * 2);
    let diff_op_expr = build_complete_diff_op_expr(&recomputed_marker_expr, &current_marker_expr);
    fields.push(field(&diff_op_expr, DIFF_OP_COL_NAME));

    let mut row_id_handle_offset = None;
    if use_row_id {
        row_id_handle_offset = Some(fields.len());
        fields.push(field(
            &qual_col_expr(DIFF_CURRENT_ALIAS, tidb_model::column::EXTRA_HANDLE_NAME),
            &format!("{DIFF_HANDLE_PREFIX}{}", tidb_model::column::EXTRA_HANDLE_NAME),
        ));
    }

    let mut current_row_offsets = Vec::with_capacity(mv_columns.len());
    for col in &mv_columns {
        current_row_offsets.push(fields.len());
        fields.push(field(
            &qual_col_expr(DIFF_CURRENT_ALIAS, col.name.original()),
            &format!("{DIFF_OLD_ROW_PREFIX}{}", col.name.original()),
        ));
    }
    let mut recomputed_row_offsets = Vec::with_capacity(mv_columns.len());
    for col in &mv_columns {
        recomputed_row_offsets.push(fields.len());
        fields.push(field(
            &qual_col_expr(DIFF_RECOMPUTED_ALIAS, col.name.original()),
            &format!("{DIFF_NEW_ROW_PREFIX}{}", col.name.original()),
        ));
    }

    let current_handle_cols = build_complete_diff_handle_cols(mv, &current_row_offsets, row_id_handle_offset)?;

    let sql = format!(
        "SELECT /*+ {}, {} */ {} FROM (SELECT 1) AS {} FULL OUTER JOIN {} AS {} ON {join_cond} WHERE {where_expr}",
        build_read_from_storage_tiflash_hint(&db_name, DIFF_CURRENT_ALIAS),
        build_hash_join_probe_hint(&db_name, DIFF_CURRENT_ALIAS),
        fields.join(", "),
        escape(DIFF_RECOMPUTED_ALIAS),
        qual_table(&db_name, name),
        escape(DIFF_CURRENT_ALIAS),
    );
    let mut diff_sel = parse_select_from_sql(sctx, &sql)?;
    set_derived_table(&mut diff_sel, DIFF_RECOMPUTED_ALIAS, mv_sel)?;

    let res = CompleteDiffBuildResult {
        diff_source_select: diff_sel,
        source_column_count: fields.len(),
        mv_table_id: mv.id,
        mv_column_count: mv_columns.len(),
        op_col_offset: 0,
        marker_mv_offset: marker_offset,
        group_key_mv_offsets: group_key_offsets,
        current_handle_cols,
        current_row_offsets,
        recomputed_row_offsets,
    };
    res.validate_source_layout(0)?;
    Ok(res)
}

/// Go master `pickCompleteDiffMarkerColumn`: the first visible NOT NULL
/// column.
fn pick_complete_diff_marker_column(mv: &TableInfo) -> Result<(usize, ColumnInfo), String> {
    for (i, col) in mv.columns.iter_deref().enumerate() {
        let col = col.read();
        if col.hidden {
            continue;
        }
        if tidb_mysql::types::has_not_null_flag(col.field_type.flags() as usize) {
            return Ok((i, col.clone()));
        }
    }
    Err(format!("materialized view {} has no visible NOT NULL column for diff marker", mv.name.original()))
}

/// Go master `buildCompleteDiffHandleCols`: the view's handle columns at
/// their current-row-image positions.
fn build_complete_diff_handle_cols(
    mv: &TableInfo,
    current_row_offsets: &[usize],
    row_id_handle_offset: Option<usize>,
) -> Result<HandleCols, String> {
    let name = mv.name.original();
    let columns: Vec<ColumnInfo> = mv.columns.iter_deref().map(|column| column.read().clone()).collect();
    if mv.pk_is_handle {
        let Some(pk_col) = mv.get_pk_col_info().map(|col| col.read().clone()) else {
            return Err(format!("mv table {name} has PKIsHandle but no primary key column"));
        };
        let Some(&index) = usize::try_from(pk_col.offset).ok().and_then(|offset| current_row_offsets.get(offset)) else {
            return Err(format!("mv table {name} primary key column offset {} is invalid", pk_col.offset));
        };
        return Ok(new_int_handle_cols(handle_column(pk_col.id, &pk_col.field_type, index)));
    }
    if mv.is_common_handle {
        let Some(pk_idx) = tidb_table::tables::scan_from_infos::find_primary_index(mv) else {
            return Err(format!("mv table {name} has common handle but no primary index"));
        };
        let mut cols = Vec::new();
        for idx_col in pk_idx.read().columns.iter_deref() {
            let offset = idx_col.read().offset;
            let Some(slot) = usize::try_from(offset).ok().filter(|&offset| offset < columns.len()) else {
                return Err(format!("mv table {name} primary index column offset {offset} is invalid"));
            };
            let col = &columns[slot];
            cols.push(handle_column(col.id, &col.field_type, current_row_offsets[slot]));
        }
        return Ok(new_common_handles_cols_without_cols_align(
            tidb_model::GoShared::new(mv.clone_like_go()),
            pk_idx,
            cols,
        ));
    }
    let Some(row_id_handle_offset) = row_id_handle_offset else {
        return Err(format!("mv table {name} rowid handle offset is invalid"));
    };
    let extra_col = ColumnInfo::new_extra_handle_col_info();
    Ok(new_int_handle_cols(handle_column(extra_col.id, &extra_col.field_type, row_id_handle_offset)))
}

/// Go master `buildCompleteDiffOpExpr`: current missing is INSERT,
/// recomputed missing is DELETE, both present (payload changed) is UPDATE.
fn build_complete_diff_op_expr(recomputed_marker_expr: &str, current_marker_expr: &str) -> String {
    format!(
        "CASE WHEN {current_marker_expr} IS NULL THEN {DIFF_OP_INSERT} WHEN {recomputed_marker_expr} IS NULL THEN {DIFF_OP_DELETE} ELSE {DIFF_OP_UPDATE} END"
    )
}

/// Go master `buildCompleteDiffPayloadEqExpr`: the non-group-key columns
/// compared between the two sides (`<=>` when nullable).
fn build_complete_diff_payload_eq_expr(
    mv: &TableInfo,
    group_key_set: &HashSet<usize>,
    recomputed_alias: &str,
    current_alias: &str,
) -> Option<String> {
    let mut expr: Option<String> = None;
    for (i, col) in mv.columns.iter_deref().enumerate() {
        if group_key_set.contains(&i) {
            continue;
        }
        let col = col.read();
        let col_name = col.name.original();
        let op = if tidb_mysql::types::has_not_null_flag(col.field_type.flags() as usize) { "=" } else { "<=>" };
        let eq = binary(op, &qual_col_expr(recomputed_alias, col_name), &qual_col_expr(current_alias, col_name));
        expr = Some(match expr {
            None => eq,
            Some(expr) => and_expr(&expr, &eq),
        });
    }
    expr
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
    fn test_extract_m_log_commit_ts_filter_bound() {
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
    fn test_normalize_m_log_commit_ts_inclusive_lower() {
        assert_eq!(normalize_mlog_commit_ts_inclusive_lower(compose_ts(1000, 123)), compose_ts(999, 0));
        assert_eq!(normalize_mlog_commit_ts_inclusive_lower(compose_ts(0, 123)), compose_ts(0, 123));
    }

    #[test]
    fn test_m_log_commit_ts_filter_window_normalizes_inclusive_lower() {
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
    fn test_estimate_m_log_commit_ts_selectivity_disjoint_lower() {
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
