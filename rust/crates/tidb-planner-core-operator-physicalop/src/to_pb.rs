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

//! Go's `ToPB(ctx *base.BuildPBContext, storeType kv.StoreType)` of the
//! physical operators (`physical_selection.go`, `physical_projection.go`,
//! `physical_limit.go`, `physical_topn.go`, `physical_table_scan.go`,
//! `physical_exchange_sender.go`, `physical_exchange_receiver.go`), over
//! `expression`'s `ExpressionsToPBList`, `ProjectionExpressionsToPBList`,
//! `SortByItemToPB` and `ToPBFieldTypeWithCheck`. An operator Go gives no
//! `ToPB` keeps the base body's refusal.
//!
//! Go's `PbConverter` asks the `kv.Client` whether each expression type is
//! supported; this port's expression lowering decides from its pushdown
//! catalog instead (register row X01), so the context carries no client.

use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_expr::expr_to_pb::{self, PbConverter};
use tidb_expr::schema::Schema;
use tidb_kv::store_type::StoreType;
use tidb_planner_core_base::PlanError;
use tidb_proto::tipb;

use crate::{
    PhysicalExchangeReceiver, PhysicalExchangeSender, PhysicalHashAgg, PhysicalHashJoin, PhysicalLimit, PhysicalPlan, PhysicalProjection,
    PhysicalSelection, PhysicalSort, PhysicalStreamAgg, PhysicalTableScan, PhysicalTopN, PhysicalWindow,
};

/// Go `planctx.BuildPBContext` (`base.BuildPBContext`): what building a
/// `tipb.Executor` from a plan reads.
pub struct BuildPbContext<'a> {
    /// Go `ExprCtx`, whose evaluation context renders expressions and
    /// carries the statement's time zone.
    pub expr_ctx: &'a dyn tidb_exprctx::Columns,
    /// Go `TiFlashFastScan`.
    pub tiflash_fast_scan: bool,
    /// Go `TiFlashFineGrainedShuffleBatchSize`.
    pub tiflash_fine_grained_shuffle_batch_size: u64,
    /// Go `GroupConcatMaxLen`.
    pub group_concat_max_len: u64,
    /// Go `InExplainStmt`.
    pub in_explain_stmt: bool,
    /// Go `p.SCtx().GetSessionVars().StmtCtx.UseDynamicPartitionPrune()`,
    /// which Go's `ToPB` reads off the plan's session.
    pub use_dynamic_partition_prune: bool,
}

/// Go `PbConverter.ExprToPB`.
fn expr_to_pb(ctx: &BuildPbContext<'_>, expr: &Expression, _input: &Schema) -> Option<tipb::Expr> {
    PbConverter::new(ctx.expr_ctx, true, &ctx.expr_ctx.time_zone()).expr_to_pb(expr)
}

/// Go `plannererrors.ErrInternal.GenWithStack("expression %v cannot be
/// pushed down", expr.StringWithCtx(ctx, errors.RedactLogDisable))`.
fn cannot_be_pushed_down(ctx: &BuildPbContext<'_>, expr: &Expression) -> PlanError {
    PlanError::internal(format!("expression {} cannot be pushed down", expression_string(ctx, expr)))
}

/// Go `expr.StringWithCtx(evalCtx, errors.RedactLogDisable)`.
fn expression_string(ctx: &BuildPbContext<'_>, expr: &Expression) -> String {
    tidb_expr::explain::physical_expression_text_with_columns(
        ctx.expr_ctx,
        expr,
        &[],
        tidb_expr::explain::ExpressionTextStyle::StringWithCtx,
    )
    .unwrap_or_default()
}

/// Go `expression.ExpressionsToPBList`.
///
/// # Errors
/// An expression has no PB form.
pub fn expressions_to_pb_list(
    ctx: &BuildPbContext<'_>,
    exprs: &[Expression],
    input: &Schema,
) -> Result<Vec<tipb::Expr>, PlanError> {
    exprs.iter().map(|expr| expr_to_pb(ctx, expr, input).ok_or_else(|| cannot_be_pushed_down(ctx, expr))).collect()
}

/// Go `expression.GroupingSets.ToPB` (`GroupingSet.ToPB` per set).
///
/// # Errors
/// A grouping expression has no PB form.
pub fn grouping_sets_to_pb(
    ctx: &BuildPbContext<'_>,
    gss: &tidb_expr::grouping_sets::GroupingSets,
    input: &Schema,
) -> Result<Vec<tipb::GroupingSet>, PlanError> {
    gss.iter()
        .map(|gs| {
            let mut res = tipb::GroupingSet::default();
            for g_exprs in gs.iter() {
                let g_exprs_pb = expressions_to_pb_list(ctx, g_exprs, input)?;
                res.grouping_exprs.push(tipb::GroupingExpr { grouping_expr: g_exprs_pb });
            }
            Ok(res)
        })
        .collect()
}

/// Go `expression.ProjectionExpressionsToPBList`: a top-level column is
/// converted without the type check (`columnToPBExpr(column, false)`), since
/// it implies no calculation.
///
/// # Errors
/// An expression has no PB form.
pub fn projection_expressions_to_pb_list(
    ctx: &BuildPbContext<'_>,
    exprs: &[Expression],
    input: &Schema,
) -> Result<Vec<tipb::Expr>, PlanError> {
    exprs
        .iter()
        .map(|expr| {
            let v = match expr {
                Expression::Column(column) => {
                    PbConverter::new(ctx.expr_ctx, true, &ctx.expr_ctx.time_zone()).column_to_pb_expr(column, false)
                }
                _ => expr_to_pb(ctx, expr, input),
            };
            v.ok_or_else(|| cannot_be_pushed_down(ctx, expr))
        })
        .collect()
}

/// Go `expression.SortByItemToPB`.
fn sort_by_item_to_pb(ctx: &BuildPbContext<'_>, expr: &Expression, desc: bool, input: &Schema) -> Option<tipb::ByItem> {
    let e = expr_to_pb(ctx, expr, input)?;
    Some(tipb::ByItem { expr: Some(e), desc: Some(desc), ..tipb::ByItem::default() })
}

/// Go `expression.ToPBFieldTypeWithCheck`, as a planner error.
fn to_pb_field_type_with_check(ft: &tidb_datatype::FieldType, store_type: StoreType) -> Result<tipb::FieldType, PlanError> {
    expr_to_pb::to_pb_field_type_with_check(ft, store_type).map_err(PlanError::internal)
}

/// Go `util.GetPushDownCtxFromBuildPBContext`: the push-down context over the
/// operator's input row.
struct PushDownCtx<'a, 'b> {
    ctx: &'a BuildPbContext<'b>,
    input: &'a Schema,
}

impl expr_to_pb::PushDownContext for PushDownCtx<'_, '_> {
    fn expr_to_pb(&self, expr: &Expression) -> Option<tipb::Expr> {
        expr_to_pb(self.ctx, expr, self.input)
    }
    fn expr_string(&self, expr: &Expression) -> String {
        expression_string(self.ctx, expr)
    }
    fn group_concat_max_len(&self) -> u64 {
        self.ctx.group_concat_max_len
    }
}

/// Go's zero `tipb.Executor`: gogoproto always sends its non-nullable
/// fine-grained shuffle fields, zero included.
fn go_executor() -> tipb::Executor {
    tipb::Executor {
        fine_grained_shuffle_stream_count: Some(0),
        fine_grained_shuffle_batch_size: Some(0),
        ..tipb::Executor::default()
    }
}

fn child_schema(plan: &PhysicalPlan) -> Result<&Schema, PlanError> {
    plan.children()
        .first()
        .and_then(PhysicalPlan::schema)
        .ok_or_else(|| PlanError::internal(format!("plan {} has no child schema", plan.explain_id(false))))
}

fn executor_id(plan: &PhysicalPlan) -> String {
    plan.explain_id(false)
}

fn child_to_pb(plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<Box<tipb::Executor>, PlanError> {
    let child = plan
        .children()
        .first()
        .ok_or_else(|| PlanError::internal(format!("plan {} has no child", plan.explain_id(false))))?;
    Ok(Box::new(child.to_pb(ctx, store_type)?))
}

impl PhysicalPlan {
    /// Go `ToPB(ctx, storeType)`: the operator's `tipb.Executor`, its
    /// children included where Go's tree form (TiFlash) carries them.
    ///
    /// # Errors
    /// The operator, or an expression it carries, has no PB form.
    pub fn to_pb(&self, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        match self {
            Self::Selection(p) => p.to_pb(self, ctx, store_type),
            Self::Projection(p) => p.to_pb(self, ctx, store_type),
            Self::Limit(p) => p.to_pb(self, ctx, store_type),
            Self::TopN(p) => p.to_pb(self, ctx, store_type),
            Self::TableScan(p) => p.to_pb(self, ctx, store_type),
            Self::IndexScan(p) => p.to_pb(self),
            Self::ExchangeSender(p) => p.to_pb(self, ctx, store_type),
            Self::ExchangeReceiver(p) => p.to_pb(self, ctx),
            Self::HashAgg(p) => p.to_pb(self, ctx, store_type),
            Self::StreamAgg(p) => p.to_pb(self, ctx, store_type),
            Self::HashJoin(p) => p.to_pb(self, ctx, store_type),
            Self::Sort(p) => p.to_pb(self, ctx, store_type),
            Self::Window(p) => p.to_pb(self, ctx, store_type),
            Self::Expand(p) => p.to_pb(self, ctx, store_type),
            Self::LocalIndexLookUp(p) => p.to_pb(store_type),
            Self::CTESink(p) => p.to_pb(self, ctx, store_type),
            Self::CTESource(p) => p.to_pb(self, store_type),
            // Go's base body is exactly this error.
            _ => Err(PlanError::internal(format!("plan {} fails converts to PB", self.explain_id(false)))),
        }
    }
}

impl crate::PhysicalLocalIndexLookUp {
    /// Go `PhysicalLocalIndexLookUp.ToPB`.
    fn to_pb(&self, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        if store_type != StoreType::TiKv {
            return Err(PlanError::internal(format!("unsupported store type {store_type:?} for LocalIndexLookUp")));
        }
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeIndexLookUp as i32),
            index_lookup: Some(tipb::IndexLookUp {
                index_handle_offsets: self.index_handle_offsets.clone(),
                ..tipb::IndexLookUp::default()
            }),
            ..go_executor()
        })
    }
}

impl PhysicalSelection {
    /// Go `PhysicalSelection.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        let conditions = expressions_to_pb_list(ctx, &self.conditions, child_schema(plan)?)?;
        let mut sel_exec = tipb::Selection { conditions, ..tipb::Selection::default() };
        let mut executor_id = String::new();
        if store_type == StoreType::TiFlash {
            sel_exec.child = Some(child_to_pb(plan, ctx, store_type)?);
            executor_id = self::executor_id(plan);
        }
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeSelection as i32),
            selection: Some(Box::new(sel_exec)),
            executor_id: Some(executor_id),
            ..go_executor()
        })
    }
}

impl PhysicalProjection {
    /// Go `PhysicalProjection.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        let exprs = projection_expressions_to_pb_list(ctx, &self.exprs, child_schema(plan)?)?;
        let mut proj_exec = tipb::Projection { exprs, ..tipb::Projection::default() };
        if !(store_type == StoreType::TiFlash || store_type == StoreType::TiKv) {
            return Err(PlanError::internal(format!(
                "the projection can only be pushed down to TiFlash or TiKV now, not {}",
                store_type.name()
            )));
        }
        proj_exec.child = Some(child_to_pb(plan, ctx, store_type)?);
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeProjection as i32),
            projection: Some(Box::new(proj_exec)),
            executor_id: Some(executor_id(plan)),
            ..go_executor()
        })
    }
}

impl crate::expand::PhysicalExpand {
    /// Go `PhysicalExpand.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        if !self.level_exprs.is_empty() {
            return self.to_pb_v2(plan, ctx, store_type);
        }
        let grouping_sets_pb = grouping_sets_to_pb(ctx, &self.grouping_sets, child_schema(plan)?)?;
        let mut expand = tipb::Expand { grouping_sets: grouping_sets_pb, ..tipb::Expand::default() };
        let mut executor_id = String::new();
        if store_type == StoreType::TiFlash {
            expand.child = Some(child_to_pb(plan, ctx, store_type)?);
            executor_id = self::executor_id(plan);
        }
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeExpand as i32),
            expand: Some(Box::new(expand)),
            executor_id: Some(executor_id),
            ..go_executor()
        })
    }

    /// Go `PhysicalExpand.toPBV2`.
    fn to_pb_v2(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        let input = child_schema(plan)?;
        let mut proj_exprs_pb = Vec::with_capacity(self.level_exprs.len());
        for exprs in &self.level_exprs {
            let expressions_pb = expressions_to_pb_list(ctx, exprs, input)?;
            proj_exprs_pb.push(tipb::ExprSlice { exprs: expressions_pb });
        }
        let mut expand2 = tipb::Expand2 {
            proj_exprs: proj_exprs_pb,
            generated_output_names: self.extra_grouping_col_names.clone(),
            ..tipb::Expand2::default()
        };
        let mut executor_id = String::new();
        if store_type == StoreType::TiFlash {
            expand2.child = Some(child_to_pb(plan, ctx, store_type)?);
            executor_id = self::executor_id(plan);
        }
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeExpand2 as i32),
            expand2: Some(Box::new(expand2)),
            executor_id: Some(executor_id),
            ..go_executor()
        })
    }
}

/// Go's `PrefixCol` (an `*expression.Column` of the child's schema), which
/// this port keeps by its unique id.
fn prefix_column(plan: &PhysicalPlan, prefix_col: Option<i64>) -> Result<Option<Expression>, PlanError> {
    let Some(unique_id) = prefix_col else {
        return Ok(None);
    };
    let column = child_schema(plan)?
        .columns
        .iter()
        .find(|column| column.unique_id == unique_id)
        .ok_or_else(|| PlanError::internal(format!("plan {} has no prefix column {unique_id}", plan.explain_id(false))))?;
    Ok(Some(Expression::Column(column.clone())))
}

impl PhysicalLimit {
    /// Go `PhysicalLimit.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        let input = child_schema(plan)?;
        let mut limit_exec = tipb::Limit { limit: Some(self.count), ..tipb::Limit::default() };
        for item in &self.partition_by {
            limit_exec.partition_by.extend(sort_by_item_to_pb(ctx, &Expression::Column(item.col.clone()), item.desc, input));
        }
        // init partial order params
        if let Some(prefix_col) = prefix_column(plan, self.prefix_col)? {
            limit_exec.truncate_key_expr = expressions_to_pb_list(ctx, std::slice::from_ref(&prefix_col), input)?;
        }
        let mut executor_id = String::new();
        if store_type == StoreType::TiFlash {
            limit_exec.child = Some(child_to_pb(plan, ctx, store_type)?);
            executor_id = self::executor_id(plan);
        }
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeLimit as i32),
            limit: Some(Box::new(limit_exec)),
            executor_id: Some(executor_id),
            ..go_executor()
        })
    }
}

impl PhysicalTopN {
    /// Go `PhysicalTopN.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        let input = child_schema(plan)?;
        let mut top_n_exec = tipb::TopN { limit: Some(self.count), ..tipb::TopN::default() };
        for item in &self.by_items {
            top_n_exec.order_by.extend(sort_by_item_to_pb(ctx, &item.expr, item.desc, input));
        }
        for item in &self.partition_by {
            top_n_exec.partition_by.extend(sort_by_item_to_pb(ctx, &Expression::Column(item.col.clone()), item.desc, input));
        }
        let mut executor_id = String::new();
        if store_type == StoreType::TiFlash {
            top_n_exec.child = Some(child_to_pb(plan, ctx, store_type)?);
            executor_id = self::executor_id(plan);
        }
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeTopN as i32),
            top_n: Some(Box::new(top_n_exec)),
            executor_id: Some(executor_id),
            ..go_executor()
        })
    }
}

impl PhysicalTableScan {
    /// Go `PhysicalTableScan.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        let table = self
            .table
            .as_ref()
            .ok_or_else(|| PlanError::internal(format!("plan {} has no table", plan.explain_id(false))))?
            .read();
        if store_type == StoreType::TiFlash
            && table.get_partition_info().is_some()
            && self.is_mpp_or_batch_cop
            && ctx.use_dynamic_partition_prune
        {
            return self.partition_table_scan_to_pb_for_flash(plan, ctx, &table);
        }
        let mut ts_exec = tidb_table::tables::scan_from_infos::build_table_scan_from_infos(
            &table,
            &self.columns,
            self.store_type == StoreType::TiFlash,
        );
        ts_exec.desc = Some(self.desc);
        ts_exec.keep_order = Some(self.keep_order);
        ts_exec.is_fast_scan = Some(ctx.tiflash_fast_scan);

        if !self.late_materialization_filter_condition.is_empty() {
            ts_exec.pushed_down_filter_conditions =
                expressions_to_pb_list(ctx, &self.late_materialization_filter_condition, schema_of(plan)?)?;
        }
        ts_exec.used_columnar_indexes =
            self.used_columnar_indexes.iter().map(|idx| idx.query_info.clone()).collect();
        // Go `int32(p.maxWaitTimeMs)`, zero without runtime filters.
        ts_exec.max_wait_time_ms = Some(0);
        if self.is_partition {
            ts_exec.table_id = Some(self.physical_table_id);
        }
        let mut executor_id = String::new();
        if store_type == StoreType::TiFlash {
            executor_id = self::executor_id(plan);
        }
        tidb_table::tables::scan_from_infos::set_pb_columns_default_value(
            &ctx.expr_ctx.time_zone(),
            &mut ts_exec.columns,
            &self.columns,
            table.name.original(),
        )
        .map_err(PlanError::internal)?;
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            tbl_scan: Some(ts_exec),
            executor_id: Some(executor_id),
            ..go_executor()
        })
    }
}

impl crate::PhysicalIndexScan {
    /// Go `PhysicalIndexScan.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan) -> Result<tipb::Executor, PlanError> {
        let no = |what: &str| PlanError::internal(format!("plan {} has no {what}", plan.explain_id(false)));
        let table = self.table.as_ref().ok_or_else(|| no("table"))?.read();
        let index = self.index.as_ref().ok_or_else(|| no("index"))?.read();
        let columns = schema_of(plan)?
            .columns
            .iter()
            .map(|col| match col.id {
                tidb_model::column::EXTRA_HANDLE_ID => {
                    Ok(tidb_model::GoShared::new(tidb_model::ColumnInfo::new_extra_handle_col_info()))
                }
                tidb_model::column::EXTRA_PHYS_TBL_ID => {
                    Ok(tidb_model::GoShared::new(tidb_model::ColumnInfo::new_extra_phys_tbl_id_col_info()))
                }
                id => tidb_model::column::find_column_info_by_id(&table.columns, id).ok_or_else(|| no("column")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let pk_col_ids = if self.need_common_handle {
            tidb_table::tables::scan_from_infos::try_get_common_pk_column_ids(&table)
        } else {
            Vec::new()
        };
        let idx_exec = tipb::IndexScan {
            table_id: Some(if self.is_partition { self.physical_table_id } else { table.id }),
            index_id: Some(index.id),
            columns: tidb_util::misc::columns_to_proto(&columns, table.pk_is_handle, true, false),
            desc: Some(self.desc),
            primary_column_ids: pk_col_ids,
            unique: Some(check_cover_index(&index, &self.ranges)),
            ..tipb::IndexScan::default()
        };
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeIndexScan as i32),
            idx_scan: Some(idx_exec),
            ..go_executor()
        })
    }
}

/// Go `checkCoverIndex`: whether the scan is a unique get -- a unique index
/// whose every range pins all its columns to non-NULL values.
fn check_cover_index(idx: &tidb_model::IndexInfo, ranges: &[tidb_ranger::types::Range]) -> bool {
    // If the index is (c1, c2) but the query range only contains c1, it is not a unique get.
    if !idx.unique {
        return false;
    }
    ranges.iter().all(|rg| {
        // a unique index may have duplicated rows with NULLs, so we cannot set the unique attribute to true when the range has NULL
        rg.low_val.len() == idx.columns.len()
            && !rg.low_val.iter().any(tidb_datatype::Datum::is_null)
            && !rg.high_val.iter().any(tidb_datatype::Datum::is_null)
    })
}

fn schema_of(child: &PhysicalPlan) -> Result<&Schema, PlanError> {
    child.schema().ok_or_else(|| PlanError::internal(format!("plan {} has no schema", child.explain_id(false))))
}

impl PhysicalTableScan {
    /// Go `partitionTableScanToPBForFlash`.
    fn partition_table_scan_to_pb_for_flash(
        &self,
        plan: &PhysicalPlan,
        ctx: &BuildPbContext<'_>,
        table: &tidb_model::TableInfo,
    ) -> Result<tipb::Executor, PlanError> {
        let mut pts_exec =
            tidb_table::tables::scan_from_infos::build_partition_table_scan_from_infos(table, &self.columns, ctx.tiflash_fast_scan);

        if !self.late_materialization_filter_condition.is_empty() {
            pts_exec.pushed_down_filter_conditions =
                expressions_to_pb_list(ctx, &self.late_materialization_filter_condition, schema_of(plan)?)?;
        }
        pts_exec.used_columnar_indexes =
            self.used_columnar_indexes.iter().map(|idx| idx.query_info.clone()).collect();
        // Go `int32(p.maxWaitTimeMs)`, zero without runtime filters.
        pts_exec.max_wait_time_ms = Some(0);
        pts_exec.desc = Some(self.desc);

        let executor_id = executor_id(plan);
        tidb_table::tables::scan_from_infos::set_pb_columns_default_value(
            &ctx.expr_ctx.time_zone(),
            &mut pts_exec.columns,
            &self.columns,
            table.name.original(),
        )
        .map_err(PlanError::internal)?;
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypePartitionTableScan as i32),
            partition_table_scan: Some(pts_exec),
            executor_id: Some(executor_id),
            ..go_executor()
        })
    }
}

/// Go `kv.MPPTask.ToPB().Marshal()` of each task.
fn encoded_tasks(tasks: &[crate::MppTask]) -> Vec<Vec<u8>> {
    use prost::Message;
    tasks.iter().map(|task| task.to_meta().encode_to_vec()).collect()
}

impl PhysicalExchangeSender {
    /// Go `PhysicalExchangeSender.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        let child = child_to_pb(plan, ctx, StoreType::TiFlash)?;
        let encoded_task = encoded_tasks(&self.target_tasks);

        let input = child_schema(plan)?;
        let mut hash_cols = Vec::with_capacity(self.hash_cols.len());
        let mut hash_col_types = Vec::with_capacity(self.hash_cols.len());
        for col in &self.hash_cols {
            hash_cols.push(Expression::Column(col.col.clone()));
            let ret_type = col.col.ret_type.as_ref().ok_or_else(|| PlanError::internal("a hash column has no type"))?;
            let mut tp = to_pb_field_type_with_check(ret_type, store_type)?;
            tp.collate = Some(col.collate_id);
            hash_col_types.push(tp);
        }
        let schema = plan.schema().ok_or_else(|| PlanError::internal("an exchange sender has no schema"))?;
        let mut all_field_types = Vec::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let ret_type = column.ret_type.as_ref().ok_or_else(|| PlanError::internal("a sender column has no type"))?;
            all_field_types.push(to_pb_field_type_with_check(ret_type, store_type)?);
        }
        let hash_col_pb = expressions_to_pb_list(ctx, &hash_cols, input)?;
        let ec_exec = tipb::ExchangeSender {
            tp: Some(match self.exchange_type {
                crate::ExchangeType::PassThrough => tipb::ExchangeType::PassThrough,
                crate::ExchangeType::Broadcast => tipb::ExchangeType::Broadcast,
                crate::ExchangeType::Hash => tipb::ExchangeType::Hash,
            } as i32),
            encoded_task_meta: encoded_task,
            partition_keys: hash_col_pb,
            child: Some(child),
            types: hash_col_types,
            all_field_types,
            compression: Some(self.compression_mode.to_tipb_compression_mode()),
            ..tipb::ExchangeSender::default()
        };
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeExchangeSender as i32),
            exchange_sender: Some(Box::new(ec_exec)),
            executor_id: Some(executor_id(plan)),
            fine_grained_shuffle_stream_count: Some(self.base.tiflash_fine_grained_shuffle_stream_count),
            fine_grained_shuffle_batch_size: Some(ctx.tiflash_fine_grained_shuffle_batch_size),
            ..go_executor()
        })
    }
}

/// The `tipb.FieldType`s of `plan`'s schema (Go's `fieldTypes` loop over
/// `p.Schema().Columns` with `ToPBFieldTypeWithCheck`).
fn schema_field_types(plan: &PhysicalPlan, store_type: StoreType) -> Result<Vec<tipb::FieldType>, PlanError> {
    let schema = plan
        .schema()
        .ok_or_else(|| PlanError::internal(format!("plan {} has no schema", plan.explain_id(false))))?;
    schema
        .columns
        .iter()
        .map(|column| {
            let ret_type = column.ret_type.as_ref().ok_or_else(|| PlanError::internal("a column has no type"))?;
            to_pb_field_type_with_check(ret_type, store_type)
        })
        .collect()
}

impl crate::PhysicalCTESink {
    /// Go `PhysicalCTESink.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        let child = child_to_pb(plan, ctx, store_type)?;
        let field_types = schema_field_types(plan, store_type)?;
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeCteSink as i32),
            cte_sink: Some(Box::new(tipb::CteSink {
                cte_id: self.id_for_storage as u32,
                cte_source_num: self.cte_source_num,
                cte_sink_num: self.cte_sink_num,
                child: Some(child),
                field_types,
            })),
            executor_id: Some(executor_id(plan)),
            ..go_executor()
        })
    }
}

impl crate::PhysicalCTESource {
    /// Go `PhysicalCTESource.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        let field_types = schema_field_types(plan, store_type)?;
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeCteSource as i32),
            cte_source: Some(tipb::CteSource {
                cte_id: self.id_for_storage as u32,
                cte_source_num: self.cte_source_num,
                cte_sink_num: self.cte_sink_num,
                field_types,
            }),
            executor_id: Some(executor_id(plan)),
            ..go_executor()
        })
    }
}

impl PhysicalExchangeReceiver {
    /// Go `PhysicalExchangeReceiver.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>) -> Result<tipb::Executor, PlanError> {
        let encoded_task = encoded_tasks(&self.tasks);
        let schema = plan.schema().ok_or_else(|| PlanError::internal("an exchange receiver has no schema"))?;
        let mut field_types = Vec::with_capacity(schema.columns.len());
        for column in &schema.columns {
            let ret_type = column.ret_type.as_ref().ok_or_else(|| PlanError::internal("a receiver column has no type"))?;
            field_types.push(to_pb_field_type_with_check(ret_type, StoreType::TiFlash)?);
        }
        let ec_exec = tipb::ExchangeReceiver { encoded_task_meta: encoded_task, field_types, ..tipb::ExchangeReceiver::default() };
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeExchangeReceiver as i32),
            exchange_receiver: Some(ec_exec),
            executor_id: Some(executor_id(plan)),
            fine_grained_shuffle_stream_count: Some(self.base.tiflash_fine_grained_shuffle_stream_count),
            fine_grained_shuffle_batch_size: Some(ctx.tiflash_fine_grained_shuffle_batch_size),
            ..go_executor()
        })
    }
}

/// The aggregation's group-by items and functions (Go's shared head of
/// `PhysicalHashAgg.ToPB` and `PhysicalStreamAgg.ToPB`).
fn aggregation_to_pb(
    plan: &PhysicalPlan,
    ctx: &BuildPbContext<'_>,
    store_type: StoreType,
    group_by_items: &[Expression],
    agg_funcs: &[tidb_expression_aggregation::AggFuncDesc],
) -> Result<tipb::Aggregation, PlanError> {
    let input = child_schema(plan)?;
    let group_by_exprs = expressions_to_pb_list(ctx, group_by_items, input)?;
    // gogoproto's non-nullable `streamed` is always sent; neither Go
    // `ToPB` sets it.
    let mut agg_exec = tipb::Aggregation { group_by: group_by_exprs, streamed: Some(false), ..tipb::Aggregation::default() };
    let push_down_ctx = PushDownCtx { ctx, input };
    for agg_func in agg_funcs {
        let agg = tidb_expression_aggregation::agg_func_to_pb_expr(&push_down_ctx, agg_func, store_type)
            .map_err(PlanError::internal)?;
        agg_exec.agg_func.push(agg);
    }
    Ok(agg_exec)
}

impl PhysicalHashAgg {
    /// Go `PhysicalHashAgg.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        let mut agg_exec = aggregation_to_pb(plan, ctx, store_type, &self.group_by_items, &self.agg_funcs)?;
        let mut executor_id = String::new();
        if store_type == StoreType::TiFlash {
            agg_exec.child = Some(child_to_pb(plan, ctx, store_type)?);
            executor_id = self::executor_id(plan);
            // If p.TiflashPreAggMode is empty, means no need to consider preagg mode.
            // For example it's the the second stage of hashagg.
            if !self.tiflash_pre_agg_mode.is_empty() {
                let (pre_agg_mode_val, ok) = tidb_variable::session_vars::to_tipb_tiflash_pre_agg_mode(&self.tiflash_pre_agg_mode);
                if !ok {
                    return Err(PlanError::internal(format!(
                        "unexpected tiflash pre agg mode: {}",
                        self.tiflash_pre_agg_mode
                    )));
                }
                agg_exec.pre_agg_mode = Some(pre_agg_mode_val as i32);
            }
        }
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeAggregation as i32),
            aggregation: Some(Box::new(agg_exec)),
            executor_id: Some(executor_id),
            fine_grained_shuffle_stream_count: Some(self.base.tiflash_fine_grained_shuffle_stream_count),
            fine_grained_shuffle_batch_size: Some(ctx.tiflash_fine_grained_shuffle_batch_size),
            ..go_executor()
        })
    }
}

impl PhysicalStreamAgg {
    /// Go `PhysicalStreamAgg.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        let mut agg_exec = aggregation_to_pb(plan, ctx, store_type, &self.group_by_items, &self.agg_funcs)?;
        let mut executor_id = String::new();
        if store_type == StoreType::TiFlash {
            agg_exec.child = Some(child_to_pb(plan, ctx, store_type)?);
            executor_id = self::executor_id(plan);
        }
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeStreamAgg as i32),
            aggregation: Some(Box::new(agg_exec)),
            executor_id: Some(executor_id),
            ..go_executor()
        })
    }
}

impl PhysicalHashJoin {
    /// Go `PhysicalHashJoin.ToPB`. The join's keys and side conditions are
    /// resolved against their own side's child, its other conditions against
    /// both children's schemas.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        use tidb_planner_core_base::LogicalJoinType;

        // Go `GetNAJoinKeys`: the columns of each null-aware condition.
        let na_join_keys = |side: usize| -> Vec<Expression> {
            self.na_equal_conditions.iter().map(|condition| condition.get_args()[side].clone()).collect()
        };
        if !self.left_join_keys.is_empty() && !self.na_equal_conditions.is_empty() {
            return Err(PlanError::internal("join key and na join key can not both exist"));
        }

        let is_null_aware_semi_join = !self.na_equal_conditions.is_empty();
        let (left_keys, right_keys) = if is_null_aware_semi_join {
            (na_join_keys(0), na_join_keys(1))
        } else {
            (
                self.left_join_keys.iter().cloned().map(Expression::Column).collect::<Vec<_>>(),
                self.right_join_keys.iter().cloned().map(Expression::Column).collect::<Vec<_>>(),
            )
        };

        let children = plan.children();
        let [left_child, right_child] = children else {
            return Err(PlanError::internal(format!("plan {} is not a two-child join", plan.explain_id(false))));
        };
        let l_children = left_child.to_pb(ctx, store_type)?;
        let r_children = right_child.to_pb(ctx, store_type)?;

        let left_schema = schema_of(left_child)?;
        let right_schema = schema_of(right_child)?;
        let merged = tidb_expr::schema::merge_schema(Some(left_schema), Some(right_schema))
            .ok_or_else(|| PlanError::internal("a join has no merged schema"))?;
        let left = expressions_to_pb_list(ctx, &left_keys, left_schema)?;
        let right = expressions_to_pb_list(ctx, &right_keys, right_schema)?;

        let left_conditions = expressions_to_pb_list(ctx, &self.left_conditions, left_schema)?;
        let right_conditions = expressions_to_pb_list(ctx, &self.right_conditions, right_schema)?;

        let mut other_conditions_in_join = Vec::new();
        let mut other_eq_conditions_from_in = Vec::new();
        // For anti join, equal conditions from `in` clause requires additional processing,
        // for example, treat `null` as true.
        if matches!(
            self.join_type,
            LogicalJoinType::AntiSemi | LogicalJoinType::AntiLeftOuterSemi | LogicalJoinType::LeftOuterSemi
        ) {
            for condition in &self.other_conditions {
                if tidb_expr::expression::is_eq_cond_from_in(condition) {
                    other_eq_conditions_from_in.push(condition.clone());
                } else {
                    other_conditions_in_join.push(condition.clone());
                }
            }
        } else {
            other_conditions_in_join = self.other_conditions.clone();
        }
        let other_conditions = expressions_to_pb_list(ctx, &other_conditions_in_join, &merged)?;
        let other_eq_conditions = expressions_to_pb_list(ctx, &other_eq_conditions_from_in, &merged)?;

        let pb_join_type = match self.join_type {
            LogicalJoinType::LeftOuter => tipb::JoinType::TypeLeftOuterJoin,
            LogicalJoinType::RightOuter => tipb::JoinType::TypeRightOuterJoin,
            LogicalJoinType::Semi => tipb::JoinType::TypeSemiJoin,
            LogicalJoinType::AntiSemi => tipb::JoinType::TypeAntiSemiJoin,
            LogicalJoinType::LeftOuterSemi => tipb::JoinType::TypeLeftOuterSemiJoin,
            LogicalJoinType::AntiLeftOuterSemi => tipb::JoinType::TypeAntiLeftOuterSemiJoin,
            _ => tipb::JoinType::TypeInnerJoin,
        };

        let equal_conditions = if is_null_aware_semi_join { &self.na_equal_conditions } else { &self.equal_conditions };
        let mut probe_filed_types = Vec::with_capacity(equal_conditions.len());
        let mut build_filed_types = Vec::with_capacity(equal_conditions.len());
        for equal_condition in equal_conditions {
            let mut ret_type = equal_condition
                .ret_type
                .clone()
                .ok_or_else(|| PlanError::internal("an equal condition has no type"))?;
            let (chs, coll) = equal_condition.collation.charset_and_collation();
            ret_type.set_charset_name(chs);
            ret_type.set_collation_name(coll);
            let ty = to_pb_field_type_with_check(&ret_type, store_type)?;
            probe_filed_types.push(ty.clone());
            build_filed_types.push(ty);
        }

        // runtime filter: this port plans none (Go's empty list).
        let join = tipb::Join {
            join_type: Some(pb_join_type as i32),
            join_exec_type: Some(tipb::JoinExecType::TypeHashJoin as i32),
            inner_idx: Some(self.inner_child_idx as i64),
            left_join_keys: left,
            right_join_keys: right,
            probe_types: probe_filed_types,
            build_types: build_filed_types,
            left_conditions,
            right_conditions,
            other_conditions,
            other_eq_conditions_from_in: other_eq_conditions,
            children: vec![l_children, r_children],
            is_null_aware_semi_join: Some(is_null_aware_semi_join),
            ..tipb::Join::default()
        };

        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeJoin as i32),
            join: Some(join),
            executor_id: Some(executor_id(plan)),
            fine_grained_shuffle_stream_count: Some(self.base.tiflash_fine_grained_shuffle_stream_count),
            fine_grained_shuffle_batch_size: Some(ctx.tiflash_fine_grained_shuffle_batch_size),
            ..go_executor()
        })
    }
}

impl PhysicalSort {
    /// Go `PhysicalSort.ToPB`: only a partial sort is pushed.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        if !self.is_partial_sort {
            return Err(PlanError::internal(format!(
                "sort {} can't convert to pb, because it isn't a partial sort",
                plan.explain_id(false)
            )));
        }

        let input = child_schema(plan)?;
        let mut sort_exec = tipb::Sort::default();
        for item in &self.by_items {
            sort_exec.by_items.extend(sort_by_item_to_pb(ctx, &item.expr, item.desc, input));
        }
        sort_exec.is_partial_sort = Some(self.is_partial_sort);

        sort_exec.child = Some(child_to_pb(plan, ctx, store_type)?);
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeSort as i32),
            sort: Some(Box::new(sort_exec)),
            executor_id: Some(executor_id(plan)),
            fine_grained_shuffle_stream_count: Some(self.base.tiflash_fine_grained_shuffle_stream_count),
            fine_grained_shuffle_batch_size: Some(ctx.tiflash_fine_grained_shuffle_batch_size),
            ..go_executor()
        })
    }
}

/// Go `FrameBound.ToPB` (`logical_window.go`).
fn frame_bound_to_pb(
    ctx: &BuildPbContext<'_>,
    fb: &tidb_planner_core_operator_logicalop::window::FrameBound,
    input: &Schema,
) -> Result<tipb::WindowFrameBound, PlanError> {
    use tidb_planner_core_operator_logicalop::window::{BoundType, RangeCmpDataType};
    let mut pb_bound = tipb::WindowFrameBound {
        r#type: Some(match fb.bound_type {
            BoundType::Following => tipb::WindowBoundType::Following,
            BoundType::Preceding => tipb::WindowBoundType::Preceding,
            BoundType::CurrentRow => tipb::WindowBoundType::CurrentRow,
        } as i32),
        unbounded: Some(fb.unbounded),
        offset: Some(fb.num),
        ..tipb::WindowFrameBound::default()
    };

    if fb.is_explicit_range {
        let range_frame = expressions_to_pb_list(ctx, &fb.calc_funcs, input)?;
        pb_bound.frame_range = range_frame.into_iter().next();
        pb_bound.cmp_data_type = Some(match fb.cmp_data_type {
            RangeCmpDataType::Int => tipb::RangeCmpDataType::Int,
            RangeCmpDataType::Float => tipb::RangeCmpDataType::Float,
            RangeCmpDataType::Decimal => tipb::RangeCmpDataType::Decimal,
            RangeCmpDataType::DateTime => tipb::RangeCmpDataType::DateTime,
            RangeCmpDataType::Duration => tipb::RangeCmpDataType::Duration,
        } as i32);
    }
    Ok(pb_bound)
}

impl PhysicalWindow {
    /// Go `PhysicalWindow.ToPB`.
    fn to_pb(&self, plan: &PhysicalPlan, ctx: &BuildPbContext<'_>, store_type: StoreType) -> Result<tipb::Executor, PlanError> {
        use tidb_planner_core_operator_logicalop::window::FrameType;
        let input = child_schema(plan)?;
        let push_down_ctx = PushDownCtx { ctx, input };
        let mut window_exec = tipb::Window::default();

        // Go appends `WindowFuncToPBExpr`'s nil for a function with no PB
        // form, which its marshalling cannot encode; the planner pushes only
        // functions `CanPushDownToTiFlash` accepts.
        window_exec.func_desc = self
            .window_func_descs
            .iter()
            .map(|desc| {
                tidb_expression_aggregation::window_func_to_pb_expr(&push_down_ctx, desc)
                    .ok_or_else(|| PlanError::internal(format!("window function {} has no PB form", desc.base.name)))
            })
            .collect::<Result<_, _>>()?;
        for item in &self.partition_by {
            window_exec.partition_by.extend(sort_by_item_to_pb(ctx, &Expression::Column(item.col.clone()), item.desc, input));
        }
        for item in &self.order_by {
            window_exec.order_by.extend(sort_by_item_to_pb(ctx, &Expression::Column(item.col.clone()), item.desc, input));
        }

        if let Some(frame) = &self.frame {
            let mut pb_frame = tipb::WindowFrame {
                r#type: Some(match frame.frame_type {
                    FrameType::Rows => tipb::WindowFrameType::Rows,
                    FrameType::Ranges => tipb::WindowFrameType::Ranges,
                    FrameType::Groups => tipb::WindowFrameType::Groups,
                } as i32),
                ..tipb::WindowFrame::default()
            };
            if let Some(start) = &frame.start {
                pb_frame.start = Some(frame_bound_to_pb(ctx, start, input)?);
            }
            if let Some(end) = &frame.end {
                pb_frame.end = Some(frame_bound_to_pb(ctx, end, input)?);
            }
            window_exec.frame = Some(pb_frame);
        }

        window_exec.child = Some(child_to_pb(plan, ctx, store_type)?);
        Ok(tipb::Executor {
            tp: Some(tipb::ExecType::TypeWindow as i32),
            window: Some(Box::new(window_exec)),
            executor_id: Some(executor_id(plan)),
            fine_grained_shuffle_stream_count: Some(self.base.tiflash_fine_grained_shuffle_stream_count),
            fine_grained_shuffle_batch_size: Some(ctx.tiflash_fine_grained_shuffle_batch_size),
            ..go_executor()
        })
    }
}
