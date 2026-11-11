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

//! Go `pkg/planner/core/fts_resolve_index.go`: the four rules that resolve
//! `FTS_MATCH_WORD()` to a full-text index scan.
//!
//! Go's `FullTextIndexPlanVisitor` walks the plan keeping the stack of a
//! `DataSource`'s ancestors and hands each `DataSource` to the rule. Here the
//! stack is the `DataSource`'s child-index path from the root: ancestor `n`
//! is the node at the path with its last `n + 1` steps dropped.

use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::column::Column;
use tidb_expr::expression::Expression;
use tidb_model::column::VIRTUAL_COL_FTS_SCORE_ID;
use tidb_expr::fts_helper::{contains_full_text_search_fn, interpret_full_text_search_expr, FtsInfo};
use tidb_planner_core_base::PlanError;
use tidb_planner_core_operator_logicalop::data_source::{DataSource, FtsPushDown};
use tidb_planner_core_operator_logicalop::plan_context::{LogicalOptRule, RuleContext};
use tidb_planner_core_operator_logicalop::LogicalPlan;
use tidb_proto::tipb;

/// Go `maxFTSTopK`.
const MAX_FTS_TOP_K: u32 = u32::MAX;

/// Go `ftsMatchWordDirtyTxnErrMsg`.
const FTS_MATCH_WORD_DIRTY_TXN_ERR_MSG: &str =
    "FTS_MATCH_WORD() cannot be used in a transaction with uncommitted changes";

/// Go `ftsScoreType`.
fn fts_score_type() -> FieldType {
    let mut tp = FieldType::new(FieldTypeCode::Float);
    tp.set_flag(FieldTypeFlags::NOT_NULL);
    tp
}

fn wrong_usage(message: &str) -> PlanError {
    PlanError::from(tidb_plannererrors::ERR_WRONG_USAGE.fast_gen(message, &[]))
}

/// The child-index path of every `DataSource` Go's `visit` reaches, in the
/// order it reaches them.
fn data_source_paths(plan: &LogicalPlan, path: &mut Vec<usize>, paths: &mut Vec<Vec<usize>>) {
    if matches!(plan, LogicalPlan::DataSource(_)) {
        paths.push(path.clone());
        return;
    }
    for (i, child) in plan.children().iter().enumerate() {
        path.push(i);
        data_source_paths(child, path, paths);
        path.pop();
    }
}

fn node_at<'a>(root: &'a mut LogicalPlan, path: &[usize]) -> &'a mut LogicalPlan {
    path.iter()
        .fold(root, |node, &i| &mut node.base_mut().children_mut()[i])
}

fn data_source_at<'a>(root: &'a mut LogicalPlan, path: &[usize]) -> &'a mut DataSource {
    match node_at(root, path) {
        LogicalPlan::DataSource(ds) => ds,
        _ => unreachable!("the path was collected at a DataSource"),
    }
}

/// Go `FullTextIndexPlanVisitor.visit` driving one rule's
/// `onEnterDataSource`.
#[allow(clippy::result_large_err)]
fn visit(
    mut plan: LogicalPlan,
    mut on_enter_data_source: impl FnMut(&mut LogicalPlan, &[usize]) -> Result<bool, PlanError>,
) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
    let mut paths = Vec::new();
    data_source_paths(&plan, &mut Vec::new(), &mut paths);
    let mut changed = false;
    for path in &paths {
        match on_enter_data_source(&mut plan, path) {
            Ok(child_changed) => changed |= child_changed,
            Err(err) => return Err((plan, err)),
        }
    }
    Ok((plan, changed))
}

/// Go `FullTextIndexResolverWhere`: resolves an `FTS_MATCH_WORD()` predicate
/// and pushes it down to the table scan. It runs before predicate pushdown.
pub struct FullTextIndexResolverWhere;

impl LogicalOptRule for FullTextIndexResolverWhere {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        if !ctx.eval_context.fts_function_is_used() {
            return Ok((plan, false));
        }
        visit(plan, |root, path| resolve_where(ctx, root, path))
    }

    fn name(&self) -> &'static str {
        "fts_resolve_index_where"
    }
}

/// Go `FullTextIndexResolverWhere.onEnterDataSource`.
fn resolve_where(ctx: &RuleContext<'_>, root: &mut LogicalPlan, path: &[usize]) -> Result<bool, PlanError> {
    let Some((&ds_idx, selection_path)) = path.split_last() else {
        return Ok(false);
    };
    let LogicalPlan::Selection(plan_selection) = node_at(root, selection_path) else {
        return Ok(false);
    };
    let Some((idx, fts_info)) = plan_selection
        .conditions
        .iter()
        .enumerate()
        .find_map(|(idx, expr)| interpret_full_text_search_expr(expr).map(|info| (idx, info)))
    else {
        return Ok(false);
    };
    let LogicalPlan::DataSource(ds) = &mut plan_selection.base.children_mut()[ds_idx] else {
        unreachable!("the path was collected at a DataSource")
    };

    let Some(matching_index) = find_matching_full_text_index(ds, &fts_info) else {
        return Err(wrong_usage("Full text search can only be used with a matching fulltext index"));
    };

    let (index_id, query_tokenizer) = {
        let index = matching_index.read();
        let tokenizer = index
            .full_text_info
            .as_ref()
            .map(|info| info.read().parser_type.clone());
        (index.id, tokenizer)
    };
    ds.fts_push_down = Some(FtsPushDown {
        index_info: matching_index,
        query_info: tipb::FtsQueryInfo {
            query_type: Some(tipb::FtsQueryType::NoScore as i32),
            index_id: Some(index_id),
            columns: vec![tidb_util::misc::column_to_proto(&fts_info.column.to_info(), false, false)],
            column_names: vec![fts_info.column.orig_name.clone()],
            query_text: Some(fts_info.query),
            query_tokenizer,
            top_k: Some(MAX_FTS_TOP_K),
            query_func: Some(tipb::ScalarFuncSig::FtsMatchWord as i32),
            ..Default::default()
        },
    });
    ds.columns.push(tidb_model::GoShared::new(tidb_model::ColumnInfo {
        name: tidb_ast::CiString::new("_FTS_SCORE"),
        id: VIRTUAL_COL_FTS_SCORE_ID,
        field_type: fts_score_type(),
        ..Default::default()
    }));
    let mut score = Column::new(ctx.column_allocator.alloc(), fts_score_type());
    score.id = VIRTUAL_COL_FTS_SCORE_ID;
    score.orig_name = "_FTS_SCORE".to_owned();
    let mut schema = ds.base.base.schema().cloned().unwrap_or_default();
    schema.columns.push(score);
    ds.base.base.set_schema(Some(schema));

    plan_selection.conditions.remove(idx);
    if plan_selection.conditions.is_empty() {
        remove_selection_node(root, selection_path);
    }
    Ok(true)
}

/// Go `findMatchingFullTextIndex`.
fn find_matching_full_text_index(
    ds: &DataSource,
    fts_info: &FtsInfo,
) -> Option<tidb_model::GoShared<tidb_model::IndexInfo>> {
    let table_info = ds.table_info.as_ref()?.read();
    table_info.indices.handles().into_iter().flatten().find(|idx| {
        let idx = idx.read();
        if idx.full_text_info.is_none() || !idx.is_public() || idx.columns.len() != 1 {
            return false;
        }
        let offset = idx.columns.get(0).map(|column| column.read().offset);
        offset
            .and_then(|offset| table_info.columns.get(offset as usize))
            .is_some_and(|column| column.read().id == fts_info.column.id)
    })
}

/// Go `removeSelectionNode`: the selection's children take its place under
/// its parent; a selection that is the root stays.
fn remove_selection_node(root: &mut LogicalPlan, selection_path: &[usize]) {
    let Some((&idx_to_remove, parent_path)) = selection_path.split_last() else {
        return;
    };
    let children = node_at(root, parent_path).base_mut().children_mut();
    let mut plan_selection = children.remove(idx_to_remove);
    let selection_children = plan_selection.base_mut().take_children();
    children.splice(idx_to_remove..idx_to_remove, selection_children);
}

/// Go `FullTextIndexResolverTopN`: resolves a matching
/// `ORDER BY FTS_MATCH_WORD() LIMIT` after the TopN rewrite.
pub struct FullTextIndexResolverTopN;

impl LogicalOptRule for FullTextIndexResolverTopN {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        if !ctx.eval_context.fts_function_is_used() {
            return Ok((plan, false));
        }
        visit(plan, resolve_topn)
    }

    fn name(&self) -> &'static str {
        "fts_resolve_index_topn"
    }
}

/// Whether Go's `ftsInfo` names the search `queryInfo` runs.
fn matches_query(fts_info: &FtsInfo, query_info: &tipb::FtsQueryInfo) -> bool {
    query_info.columns[0].column_id == Some(fts_info.column.id)
        && query_info.query_text.as_deref() == Some(fts_info.query.as_str())
}

/// Go `FullTextIndexResolverTopN.onEnterDataSource`.
fn resolve_topn(root: &mut LogicalPlan, path: &[usize]) -> Result<bool, PlanError> {
    let ds = data_source_at(root, path);
    if ds.fts_push_down.is_none() {
        return Ok(false);
    }
    let Some((_, parent0_path)) = path.split_last() else {
        return Ok(false);
    };
    let (has_selection, topn_path) = if matches!(node_at(root, parent0_path), LogicalPlan::Selection(_)) {
        let Some((_, parent1_path)) = parent0_path.split_last() else {
            return Ok(false);
        };
        (true, parent1_path)
    } else {
        (false, parent0_path)
    };
    let LogicalPlan::TopN(plan_topn) = node_at(root, topn_path) else {
        return Ok(false);
    };
    let Some(first) = plan_topn.by_items.first() else {
        return Ok(false);
    };
    let Some(order_by_info) = interpret_full_text_search_expr(&first.expr) else {
        return Ok(false);
    };
    let (desc, by_items, top_k) = (
        first.desc,
        plan_topn.by_items.len(),
        plan_topn.offset.saturating_add(plan_topn.count),
    );

    let ds = data_source_at(root, path);
    let has_pushed_down_conds = !ds.pushed_down_conds.is_empty();
    let score = ds.base.base.schema().and_then(|schema| schema.columns.last().cloned());
    let Some(FtsPushDown { query_info, .. }) = &mut ds.fts_push_down else {
        return Ok(false);
    };
    if query_info.columns.is_empty() || !matches_query(&order_by_info, query_info) {
        return Err(wrong_usage("'FTS_MATCH_WORD()' in ORDER BY must match the one in WHERE"));
    }
    query_info.query_type = Some(tipb::FtsQueryType::WithScore as i32);
    if !has_selection && !has_pushed_down_conds && desc && by_items == 1 {
        query_info.top_k = Some(u32::try_from(top_k).unwrap_or(MAX_FTS_TOP_K));
    }
    let score = score.expect("the WHERE rule appended the score column");
    let LogicalPlan::TopN(plan_topn) = node_at(root, topn_path) else {
        unreachable!("matched above")
    };
    plan_topn.by_items[0].expr = Expression::Column(score);
    Ok(true)
}

/// Go `FullTextIndexResolverProjection`: resolves matching `FTS_MATCH_WORD()`
/// expressions in the SELECT fields to the score column the scan produces.
pub struct FullTextIndexResolverProjection;

impl LogicalOptRule for FullTextIndexResolverProjection {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        if !ctx.eval_context.fts_function_is_used() {
            return Ok((plan, false));
        }
        visit(plan, resolve_projection)
    }

    fn name(&self) -> &'static str {
        "fts_resolve_index_projection"
    }
}

/// Go `FullTextIndexResolverProjection.onEnterDataSource`.
fn resolve_projection(root: &mut LogicalPlan, path: &[usize]) -> Result<bool, PlanError> {
    let ds = data_source_at(root, path);
    let Some(FtsPushDown { query_info, .. }) = &ds.fts_push_down else {
        return Ok(false);
    };
    let query_info = query_info.clone();
    let score_column = ds.base.base.schema().and_then(|schema| schema.columns.last().cloned());

    let (mut seen_selection, mut seen_topn) = (false, false);
    let mut projection_path = None;
    for parents in (0..path.len()).rev() {
        match node_at(root, &path[..parents]) {
            LogicalPlan::Selection(_) => {
                if seen_topn || seen_selection {
                    return Ok(false);
                }
                seen_selection = true;
            }
            LogicalPlan::TopN(_) => {
                if seen_topn {
                    return Ok(false);
                }
                seen_topn = true;
            }
            LogicalPlan::Projection(_) => {
                projection_path = Some(&path[..parents]);
                break;
            }
            _ => return Ok(false),
        }
    }
    let Some(projection_path) = projection_path else {
        return Ok(false);
    };
    if query_info.columns.is_empty() {
        return Ok(false);
    }
    let LogicalPlan::Projection(plan_projection) = node_at(root, projection_path) else {
        unreachable!("matched above")
    };
    let mut matched_projections = Vec::new();
    for (i, expr) in plan_projection.exprs.iter().enumerate() {
        let Some(fts_info) = interpret_full_text_search_expr(expr) else {
            continue;
        };
        if !matches_query(&fts_info, &query_info) {
            return Err(wrong_usage("'FTS_MATCH_WORD()' in SELECT must match the one in WHERE"));
        }
        matched_projections.push(i);
    }
    if matched_projections.is_empty() {
        return Ok(false);
    }

    let score_column = score_column.expect("the WHERE rule appended the score column");
    let mut schema = plan_projection.base.base.schema().cloned().unwrap_or_default();
    for idx in matched_projections {
        plan_projection.exprs[idx] = Expression::Column(score_column.clone());
        schema.columns[idx].ret_type = Some(fts_score_type());
    }
    plan_projection.base.base.set_schema(Some(schema));
    if let Some(FtsPushDown { query_info, .. }) = &mut data_source_at(root, path).fts_push_down {
        query_info.query_type = Some(tipb::FtsQueryType::WithScore as i32);
    }
    Ok(true)
}

/// Go `FullTextIndexResolverRejectRemaining`: rejects the `FTS_MATCH_WORD()`
/// uses the pushdown rules left in the plan.
pub struct FullTextIndexResolverRejectRemaining;

impl LogicalOptRule for FullTextIndexResolverRejectRemaining {
    #[allow(clippy::result_large_err)]
    fn optimize(
        &self,
        ctx: &RuleContext<'_>,
        plan: LogicalPlan,
    ) -> Result<(LogicalPlan, bool), (LogicalPlan, PlanError)> {
        if !ctx.eval_context.fts_function_is_used() {
            return Ok((plan, false));
        }
        match reject_remaining(&plan) {
            Ok(()) => Ok((plan, false)),
            Err(err) => Err((plan, err)),
        }
    }

    fn name(&self) -> &'static str {
        "fts_resolve_reject_remaining"
    }
}

const MUST_BE_USED_ALONE: &str = "Currently 'FTS_MATCH_WORD()' must be used alone. It cannot be placed inside any other function or expression as a parameter, or used multiple times. A valid example: SELECT * FROM <TABLE> WHERE FTS_MATCH_WORD(...)";

/// Go `containsFullTextSearchFn`.
fn any_contains_full_text_search_fn(exprs: &[Expression]) -> bool {
    exprs.iter().any(contains_full_text_search_fn)
}

/// Go `FullTextIndexResolverRejectRemaining.visit`.
fn reject_remaining(plan: &LogicalPlan) -> Result<(), PlanError> {
    match plan {
        LogicalPlan::Projection(p) => {
            for expr in &p.exprs {
                if interpret_full_text_search_expr(expr).is_some() {
                    return Err(wrong_usage("'FTS_MATCH_WORD()' in SELECT requires a matching 'FTS_MATCH_WORD()' in WHERE. A valid example: SELECT FTS_MATCH_WORD(...) FROM <TABLE> WHERE FTS_MATCH_WORD(...)"));
                }
                if contains_full_text_search_fn(expr) {
                    return Err(wrong_usage("'FTS_MATCH_WORD()' in SELECT must not be wrapped in expressions. A valid example: SELECT FTS_MATCH_WORD(...) FROM <TABLE> WHERE FTS_MATCH_WORD(...)"));
                }
            }
        }
        LogicalPlan::UnionScan(p) => {
            if any_contains_full_text_search_fn(&p.conditions) {
                return Err(wrong_usage(FTS_MATCH_WORD_DIRTY_TXN_ERR_MSG));
            }
        }
        LogicalPlan::DataSource(p) => {
            if any_contains_full_text_search_fn(&p.pushed_down_conds) {
                return Err(wrong_usage(MUST_BE_USED_ALONE));
            }
        }
        LogicalPlan::Selection(p) => {
            if any_contains_full_text_search_fn(&p.conditions) {
                if matches!(p.base.children(), [LogicalPlan::UnionScan(_)]) {
                    return Err(wrong_usage(FTS_MATCH_WORD_DIRTY_TXN_ERR_MSG));
                }
                return Err(wrong_usage(MUST_BE_USED_ALONE));
            }
        }
        LogicalPlan::TopN(p) => {
            if let Some(i) = p.by_items.iter().position(|item| contains_full_text_search_fn(&item.expr)) {
                if i > 0 {
                    return Err(wrong_usage("FTS_MATCH_WORD() must be used as the first item in ORDER BY. A valid example: SELECT * FROM <TABLE> WHERE FTS_MATCH_WORD(...) ORDER BY FTS_MATCH_WORD(...) LIMIT .."));
                }
                return Err(wrong_usage("Unsupported 'FTS_MATCH_WORD()' usage. It must be used with a WHERE clause and must be used alone. A valid example: SELECT * FROM <TABLE> WHERE FTS_MATCH_WORD(...) ORDER BY FTS_MATCH_WORD(...) LIMIT .."));
            }
        }
        LogicalPlan::Sort(p) => {
            if p.by_items.iter().any(|item| contains_full_text_search_fn(&item.expr)) {
                return Err(wrong_usage("Currently 'FTS_MATCH_WORD()' in ORDER BY without a LIMIT clause is not supported, try specify a very large LIMIT as a workaround"));
            }
        }
        tidb_planner_core_operator_logicalop::base_arms![
            Join,
            Apply,
            Aggregation,
            Limit,
            UnionAll,
            PartitionUnionAll,
            Window,
            CTE,
            CTETable,
            MaxOneRow,
            Lock,
            Sequence,
            TiKVSingleGather,
            TableScan,
            IndexScan,
            TableDual,
            Expand,
            MemTable,
            Show,
            ShowDDLJobs,
        ] => {}
    }
    plan.children().iter().try_for_each(reject_remaining)
}
