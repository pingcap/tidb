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

//! The aggregate partial/final split: Go `BuildFinalModeAggregation`
//! (`pkg/planner/core/operator/physicalop/base_physical_agg.go:600`) and its
//! helpers `RemoveUnnecessaryFirstRow` (`:460`), `genFirstRowAggForGroupBy`
//! (`:441`), and `computePartialCursorOffset` (`:507`).
//!
//! Go splits one complete aggregation into a PARTIAL half that the
//! coprocessor runs next to the scan and a FINAL half that merges partial
//! results at the root. The split rewrites the descriptors: `avg` becomes
//! `count`+`sum` below and a division above, a distinct argument becomes a
//! pushed group-by column, and the final half's arguments become the
//! partial half's output schema columns.
//!
//! Go keys `firstRowFuncMap` by descriptor POINTER; descriptors here are
//! values, so the map is `partial index -> final index`, carried alongside
//! the two halves and consumed by [`remove_unnecessary_first_row`] exactly
//! where Go consumes the pointer map.

use std::collections::{HashMap, HashSet};

use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};
use tidb_expr::aggregation::{
    names, need_count, need_value, AggFuncDesc, AggFunctionMode, ByItems,
};
use tidb_expr::column::Column;
use tidb_expr::constant::Constant;
use tidb_expr::expression::Expression;
use tidb_expr::schema::Schema;
use tidb_expr::Columns;

use crate::cardinality::ndv::GroupNdv;
use crate::expression_rewriter::ColumnIdAllocator;
use crate::stats_info::StatsInfo;

/// Go `AggInfo` (`base_physical_agg.go:592`): the descriptor triple either
/// half of a split carries.
#[derive(Clone, Debug, Default)]
pub struct AggInfo {
    /// Go `AggFuncs`.
    pub agg_funcs: Vec<AggFuncDesc>,
    /// Go `GroupByItems`.
    pub group_by_items: Vec<Expression>,
    /// Go `Schema`.
    pub schema: Schema,
}

/// The split's product: both halves plus the firstrow pairing Go returns as
/// `firstRowFuncMap` (partial-index -> final-index here, pointer map there).
#[derive(Debug)]
pub struct FinalModeSplit {
    /// The half pushed toward the source.
    pub partial: AggInfo,
    /// The half kept at the root.
    pub final_agg: AggInfo,
    /// Which partial `firstrow` merges into which final `firstrow`.
    pub first_row_func_map: HashMap<usize, usize>,
}

/// The three physical aggregation stages Go builds for a scalar single
/// `COUNT(DISTINCT column)` aggregate. The partial stage keeps the distinct
/// key in its group-by, the middle stage evaluates the local distinct/count
/// merge, and the final stage gathers the middle results into one worker.
#[derive(Debug)]
pub struct ThreeStageAggSplit {
    /// The source-side partial aggregate.
    pub partial: crate::physical::PhysicalPlan,
    /// The hash-partitioned middle aggregate.
    pub middle: crate::physical::PhysicalPlan,
    /// The single-partition final aggregate.
    pub final_agg: crate::physical::PhysicalPlan,
    /// The columns used by the partial-to-middle hash exchange.
    pub partition_cols: Vec<crate::physical_property::MppPartitionColumn>,
}

/// The physical pieces Go's multi-distinct scalar MPP rewrite inserts around
/// the ordinary partial/final split: Expand plus the conditional projection
/// below the partial aggregate, followed by middle and final aggregates.
#[derive(Debug)]
pub struct MultiDistinctThreeStageAggSplit {
    /// Replicates each input row once per distinct argument set.
    pub expand: crate::physical::PhysicalPlan,
    /// Selects each ordinary aggregate's target grouping set.
    pub partial_projection: crate::physical::PhysicalPlan,
    /// The source-side partial aggregate.
    pub partial: crate::physical::PhysicalPlan,
    /// The hash-partitioned middle aggregate.
    pub middle: crate::physical::PhysicalPlan,
    /// The single-partition final aggregate.
    pub final_agg: crate::physical::PhysicalPlan,
    /// The columns used by the partial-to-middle hash exchange.
    pub partition_cols: Vec<crate::physical_property::MppPartitionColumn>,
}

/// Go `BasePhysicalAgg.canUse3Stage4SingleDistinctAgg` (`base_physical_agg.go:403`).
///
/// The three-stage rewrite is deliberately narrow: one distinct COUNT, no
/// grouping set, no aggregate-local ordering, complete input descriptors, and
/// column arguments only. Unsupported shapes stay on Go's ordinary scalar
/// two-stage path.
#[must_use]
pub fn can_use_three_stage_single_distinct(
    agg_funcs: &[AggFuncDesc],
    group_by_items: &[Expression],
) -> bool {
    if !group_by_items.is_empty() {
        return false;
    }
    let mut distinct_count = 0;
    for function in agg_funcs {
        if function.has_distinct {
            distinct_count += 1;
            if distinct_count > 1 || function.name() != names::COUNT {
                return false;
            }
            if function
                .base
                .args
                .iter()
                .any(|argument| !matches!(argument, Expression::Column(_)))
            {
                return false;
            }
        } else if function.base.args.len() > 1 {
            return false;
        }
        if !function.order_by_items.is_empty() || function.mode != AggFunctionMode::Complete {
            return false;
        }
        // Go's middle-stage construction reads the one ordinary argument as
        // a column. Refuse a non-column here instead of producing an invalid
        // remap for a descriptor that the source implementation cannot use.
        if !function.has_distinct
            && function
                .base
                .args
                .first()
                .is_some_and(|argument| !matches!(argument, Expression::Column(_)))
        {
            return false;
        }
    }
    distinct_count == 1
}

/// The grouping sets used by Go's multi-distinct three-stage rewrite. Each
/// set is the ordered list of column arguments of one `COUNT(DISTINCT ...)`.
pub type DistinctGroupingSets = Vec<Vec<Column>>;

/// Go `BasePhysicalAgg.canUse3Stage4MultiDistinctAgg`'s admission checks.
///
/// The multi-distinct feature is session-gated and intentionally narrow: no
/// GROUP BY, at least two distinct COUNT descriptors, simple column arguments
/// for the distinct descriptors, complete input modes, and no aggregate-local
/// ordering. Duplicate or overlapping grouping sets remain refused exactly as
/// Go's current implementation does.
#[must_use]
pub fn can_use_three_stage_multi_distinct(
    agg_funcs: &[AggFuncDesc],
    group_by_items: &[Expression],
    enable_3_stage_distinct_agg: bool,
    enable_3_stage_multi_distinct_agg: bool,
) -> Option<DistinctGroupingSets> {
    if !enable_3_stage_distinct_agg
        || !enable_3_stage_multi_distinct_agg
        || !group_by_items.is_empty()
    {
        return None;
    }
    let mut grouping_sets: DistinctGroupingSets = Vec::new();
    for function in agg_funcs {
        if function.has_distinct {
            if function.name() != names::COUNT
                || function
                    .base
                    .args
                    .iter()
                    .any(|argument| !matches!(argument, Expression::Column(_)))
            {
                return None;
            }
            grouping_sets.push(
                function
                    .base
                    .args
                    .iter()
                    .filter_map(|argument| match argument {
                        Expression::Column(column) => Some(column.clone()),
                        _ => None,
                    })
                    .collect(),
            );
        } else if function.base.args.len() > 1 {
            return None;
        }
        if !function.order_by_items.is_empty() || function.mode != AggFunctionMode::Complete {
            return None;
        }
    }
    if grouping_sets.len() <= 1 {
        return None;
    }
    let mut seen = HashSet::new();
    if grouping_sets.iter().any(|set| {
        let mut ids: Vec<_> = set.iter().map(|column| column.unique_id).collect();
        ids.sort_unstable();
        !seen.insert(ids)
    }) {
        return None;
    }
    // Go's GroupingSets.Merge rejects layouts that share a column: the
    // subsequent NeedCloneColumn check would require duplicating that column
    // in Expand, which the three-stage path deliberately does not implement.
    if grouping_sets.iter().enumerate().any(|(index, left)| {
        grouping_sets[index + 1..].iter().any(|right| {
            left.iter().any(|column| {
                right
                    .iter()
                    .any(|other| other.unique_id == column.unique_id)
            })
        })
    }) {
        return None;
    }
    let mut marked = agg_funcs.to_vec();
    if !mark_three_stage_grouping_ids(&mut marked, &grouping_sets) {
        return None;
    }
    Some(grouping_sets)
}

/// Assigns Go's one-based `GroupingID` to every distinct and ordinary
/// aggregate descriptor. The caller uses the returned descriptors as a
/// validation-only copy before applying the same IDs to the physical plan.
#[must_use]
pub fn mark_three_stage_grouping_ids(
    agg_funcs: &mut [AggFuncDesc],
    grouping_sets: &[Vec<Column>],
) -> bool {
    let all_grouping_ids: HashSet<_> = grouping_sets
        .iter()
        .flat_map(|set| set.iter().map(|column| column.unique_id))
        .collect();
    for function in agg_funcs {
        if function.has_distinct {
            // Distinct descriptors were admitted only when every argument is
            // a simple column, so preserve their grouping-set position.
            let argument_ids: HashSet<_> = function
                .base
                .args
                .iter()
                .filter_map(|argument| match argument {
                    Expression::Column(column) => Some(column.unique_id),
                    _ => None,
                })
                .collect();
            let Some(index) = grouping_sets.iter().position(|set| {
                set.iter()
                    .map(|column| column.unique_id)
                    .collect::<HashSet<_>>()
                    == argument_ids
            }) else {
                return false;
            };
            function.grouping_id = (index + 1) as i32;
            continue;
        }
        // GroupingSets.TargetOne chooses the first layout whose null-filled
        // columns do not intersect the ordinary aggregate's dependencies.
        // A constant aggregate therefore targets the first layout, as does a
        // column absent from every distinct grouping set.
        let argument_ids: HashSet<_> = function
            .base
            .args
            .iter()
            .flat_map(tidb_expr::expr_util::extract_columns)
            .map(|column| column.unique_id)
            .collect();
        let Some(index) = grouping_sets.iter().position(|set| {
            let set_ids: HashSet<_> = set.iter().map(|column| column.unique_id).collect();
            argument_ids.iter().all(|column_id| {
                set_ids.contains(column_id) || !all_grouping_ids.contains(column_id)
            })
        }) else {
            return false;
        };
        function.grouping_id = (index + 1) as i32;
    }
    true
}

/// Go `adjust3StagePhaseAgg`'s single-distinct branch (`task.go:1840`).
///
/// `new_partial_aggregate_mpp` has already produced the source partial and
/// the ordinary final descriptor. This function clones that final descriptor
/// into the middle phase, changes the distinct COUNT to a SUM in the final
/// phase, and remaps ordinary aggregate arguments through the middle schema.
pub fn adjust_three_stage_single_distinct(
    partial: crate::physical::PhysicalPlan,
    final_agg: crate::physical::PhysicalPlan,
    column_ids: &ColumnIdAllocator,
    plan_ids: &crate::plan_base::PlanIdAllocator,
) -> Result<Option<ThreeStageAggSplit>, crate::plan_base::PlanError> {
    use crate::physical::{PhysicalHashAgg, PhysicalPlan};

    let PhysicalPlan::HashAgg(mut final_hash) = final_agg else {
        return Ok(None);
    };
    let PhysicalPlan::HashAgg(partial_hash) = &partial else {
        return Ok(None);
    };
    let Some(distinct_pos) = final_hash
        .agg_funcs
        .iter()
        .position(|function| function.has_distinct)
    else {
        return Ok(None);
    };
    if final_hash
        .agg_funcs
        .iter()
        .filter(|function| function.has_distinct)
        .count()
        != 1
    {
        return Ok(None);
    }

    let mut partition_cols = Vec::with_capacity(partial_hash.group_by_items.len());
    for item in &partial_hash.group_by_items {
        let Expression::Column(column) = item else {
            return Ok(None);
        };
        let collate_id = column
            .get_static_type()
            .map(|field_type| {
                crate::physical_property::collate_id_for_partition(field_type.collation_name())
            })
            .unwrap_or(-1);
        partition_cols.push(crate::physical_property::MppPartitionColumn {
            col: column.clone(),
            collate_id,
        });
    }
    if partition_cols.is_empty() {
        return Ok(None);
    }

    // The source Clone allocates a fresh plan identity for the middle phase;
    // its child is attached later by `attach_plan_to_task`.
    let mut middle_hash = PhysicalHashAgg {
        base: final_hash.base.clone(),
        agg_funcs: final_hash.agg_funcs.clone(),
        group_by_items: final_hash.group_by_items.clone(),
        mpp_run_mode: final_hash.mpp_run_mode,
        mpp_partition_cols: final_hash.mpp_partition_cols.clone(),
        enable_3_stage_distinct_agg: final_hash.enable_3_stage_distinct_agg,
        enable_3_stage_multi_distinct_agg: final_hash.enable_3_stage_multi_distinct_agg,
        tiflash_pre_agg_mode: final_hash.tiflash_pre_agg_mode.clone(),
    };
    middle_hash.base.base.set_id(plan_ids.alloc());
    middle_hash.base.set_children_req_props(vec![Some(
        crate::physical_property::PhysicalProperty::default(),
    )]);

    let mut middle_schema = Schema::default();
    let mut ordinary_arg_map = std::collections::HashMap::new();
    for (index, function) in middle_hash.agg_funcs.iter_mut().enumerate() {
        let output = Column::new(column_ids.alloc(), function.base.ret_type.clone());
        if index != distinct_pos {
            let Some(Expression::Column(argument)) = function.base.args.first() else {
                return Ok(None);
            };
            ordinary_arg_map.insert(argument.unique_id, output.clone());
            function.mode = AggFunctionMode::Partial2;
        } else {
            function.mode = AggFunctionMode::Partial1;
        }
        middle_schema.columns.push(output);
    }
    middle_hash
        .base
        .base
        .set_schema(Some(middle_schema.clone()));

    for (index, function) in final_hash.agg_funcs.iter_mut().enumerate() {
        if index == distinct_pos {
            // Go's middle distinct result is merged by SUM at the final
            // single-partition stage.
            function.base.name = names::SUM.to_owned();
            function.has_distinct = false;
            function.base.args = vec![Expression::Column(middle_schema.columns[index].clone())];
        } else {
            let mut remapped = Vec::with_capacity(function.base.args.len());
            for argument in &function.base.args {
                let Expression::Column(column) = argument else {
                    return Ok(None);
                };
                let Some(mapped) = ordinary_arg_map.get(&column.unique_id) else {
                    return Ok(None);
                };
                remapped.push(Expression::Column(mapped.clone()));
            }
            function.base.args = remapped;
        }
        function.mode = AggFunctionMode::Final;
    }
    final_hash.base.set_children_req_props(vec![Some(
        crate::physical_property::PhysicalProperty::default(),
    )]);

    Ok(Some(ThreeStageAggSplit {
        partial,
        middle: PhysicalPlan::HashAgg(middle_hash),
        final_agg: PhysicalPlan::HashAgg(final_hash),
        partition_cols,
    }))
}

/// Go `adjust3StagePhaseAgg`'s grouping-set branch
/// (`task.go:1905-2030`). It materializes the grouping-set expansion and the
/// conditional projection below the partial aggregate, then rewrites the
/// middle and final descriptors over the same output-column contract.
pub fn adjust_three_stage_multi_distinct(
    partial: crate::physical::PhysicalPlan,
    final_agg: crate::physical::PhysicalPlan,
    child: &crate::physical::PhysicalPlan,
    grouping_sets: &[Vec<Column>],
    column_ids: &ColumnIdAllocator,
    plan_ids: &crate::plan_base::PlanIdAllocator,
) -> Result<Option<MultiDistinctThreeStageAggSplit>, crate::plan_base::PlanError> {
    use crate::physical::expand::PhysicalExpand;
    use crate::physical::{BasePhysicalPlan, PhysicalHashAgg, PhysicalPlan, PhysicalProjection};
    use tidb_expr::expr_util::{FunctionBuilder, RealFunctionBuilder};

    let PhysicalPlan::HashAgg(mut final_hash) = final_agg else {
        return Ok(None);
    };
    let PhysicalPlan::HashAgg(mut partial_hash) = partial else {
        return Ok(None);
    };
    let child_schema = child.schema().cloned().unwrap_or_default();
    let mut grouping_ids = HashSet::new();
    for set in grouping_sets {
        grouping_ids.extend(set.iter().map(|column| column.unique_id));
    }

    let mut gid_type = FieldType::new(FieldTypeCode::LongLong);
    gid_type.set_flags(gid_type.flags() | FieldTypeFlags::UNSIGNED | FieldTypeFlags::NOT_NULL);
    let mut grouping_id_col = Column::new(column_ids.alloc(), gid_type.clone());
    grouping_id_col.index = child_schema.columns.len() as i64;

    let mut expand_schema_columns = child_schema.columns.clone();
    expand_schema_columns.push(grouping_id_col.clone());
    let expand_schema = Schema::new(expand_schema_columns.clone());
    let levels = grouping_sets
        .iter()
        .enumerate()
        .map(|(offset, grouping_set)| {
            let grouping_set_ids: HashSet<_> =
                grouping_set.iter().map(|column| column.unique_id).collect();
            let mut level = Vec::with_capacity(expand_schema_columns.len());
            for column in &child_schema.columns {
                if grouping_ids.contains(&column.unique_id)
                    && !grouping_set_ids.contains(&column.unique_id)
                {
                    let mut null = Constant::new_null();
                    null.ret_type = column.ret_type.clone();
                    level.push(Expression::Constant(null));
                } else {
                    level.push(Expression::Column(column.clone()));
                }
            }
            let mut gid = Constant::new(
                tidb_datatype::Datum::UInt((offset + 1) as u64),
                gid_type.clone(),
            );
            gid.ret_type = Some(gid_type.clone());
            level.push(Expression::Constant(gid));
            level
        })
        .collect::<Vec<_>>();
    let mut expand_base = BasePhysicalPlan::new(plan_ids, "Expand", child.query_block_offset());
    expand_base.base.set_stats(
        child
            .stats_info()
            .map(|stats| stats.scale(grouping_sets.len() as f64, 1.0)),
    );
    expand_base.base.set_schema(Some(expand_schema.clone()));
    let expand = PhysicalPlan::Expand(PhysicalExpand {
        base: expand_base,
        level_exprs: levels,
        extra_grouping_col_names: vec!["gid".to_owned()],
    });

    partial_hash
        .group_by_items
        .push(Expression::Column(grouping_id_col.clone()));
    if let Some(schema) = partial_hash.base.base.schema().cloned() {
        let mut schema = schema;
        schema.columns.push(grouping_id_col.clone());
        partial_hash.base.base.set_schema(Some(schema));
    }
    scale_three_stage_multi_distinct_stats(
        &mut partial_hash,
        grouping_sets,
        &grouping_id_col,
        child.stats_info(),
    );

    let eval_ctx = tidb_expr::ZonedNoColumns(tidb_expr::SessionTimeZone::utc());
    let builder = RealFunctionBuilder::new(&eval_ctx);
    let mut projection_exprs = expand_schema
        .columns
        .iter()
        .cloned()
        .map(Expression::Column)
        .collect::<Vec<_>>();
    let mut projection_columns = expand_schema.columns.clone();
    for function in &mut partial_hash.agg_funcs {
        if function.has_distinct {
            continue;
        }
        let Some(argument) = function.base.args.first().cloned() else {
            return Ok(None);
        };
        let Some(grouping_id) = (function.grouping_id > 0).then_some(function.grouping_id) else {
            return Ok(None);
        };
        let mut null = Constant::new_null();
        null.ret_type = argument.static_type().cloned();
        let condition = builder
            .new_function(
                "eq",
                None,
                vec![
                    Expression::Column(grouping_id_col.clone()),
                    Expression::Constant(Constant::new(
                        tidb_datatype::Datum::UInt(grouping_id as u64),
                        gid_type.clone(),
                    )),
                ],
            )
            .map_err(|error| crate::plan_base::PlanError::internal(error.to_string()))?;
        let case_when = builder
            .new_function(
                "case",
                argument.static_type().cloned(),
                vec![condition, argument, Expression::Constant(null)],
            )
            .map_err(|error| crate::plan_base::PlanError::internal(error.to_string()))?;
        let mut case_column = Column::new(
            column_ids.alloc(),
            case_when
                .static_type()
                .cloned()
                .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
        );
        case_column.index = projection_exprs.len() as i64;
        projection_exprs.push(case_when);
        projection_columns.push(case_column.clone());
        function.base.args[0] = Expression::Column(case_column);
    }
    let mut projection_base =
        BasePhysicalPlan::new(plan_ids, "Projection", child.query_block_offset());
    projection_base.base.set_stats(child.stats_info().cloned());
    projection_base
        .base
        .set_schema(Some(Schema::new(projection_columns)));
    let partial_projection = PhysicalPlan::Projection(PhysicalProjection {
        base: projection_base,
        exprs: projection_exprs,
        ..Default::default()
    });

    let mut middle_hash = PhysicalHashAgg {
        base: final_hash.base.clone(),
        agg_funcs: final_hash.agg_funcs.clone(),
        group_by_items: final_hash.group_by_items.clone(),
        mpp_run_mode: final_hash.mpp_run_mode,
        mpp_partition_cols: final_hash.mpp_partition_cols.clone(),
        enable_3_stage_distinct_agg: final_hash.enable_3_stage_distinct_agg,
        enable_3_stage_multi_distinct_agg: final_hash.enable_3_stage_multi_distinct_agg,
        tiflash_pre_agg_mode: final_hash.tiflash_pre_agg_mode.clone(),
    };
    middle_hash.base.base.set_id(plan_ids.alloc());
    middle_hash.base.set_children_req_props(vec![Some(
        crate::physical_property::PhysicalProperty::default(),
    )]);

    let mut middle_schema = Schema::default();
    let mut ordinary_arg_map = HashMap::new();
    for function in &mut middle_hash.agg_funcs {
        let output = Column::new(column_ids.alloc(), function.base.ret_type.clone());
        if function.has_distinct {
            function.mode = AggFunctionMode::Partial1;
        } else {
            let Some(Expression::Column(argument)) = function.base.args.first() else {
                return Ok(None);
            };
            ordinary_arg_map.insert(argument.unique_id, output.clone());
            function.mode = AggFunctionMode::Partial2;
        }
        middle_schema.columns.push(output);
    }
    middle_hash
        .base
        .base
        .set_schema(Some(middle_schema.clone()));

    for (index, function) in final_hash.agg_funcs.iter_mut().enumerate() {
        if function.has_distinct {
            function.base.name = names::SUM.to_owned();
            function.has_distinct = false;
            function.base.args = vec![Expression::Column(middle_schema.columns[index].clone())];
        } else {
            let mut remapped = Vec::with_capacity(function.base.args.len());
            for argument in &function.base.args {
                let Expression::Column(column) = argument else {
                    return Ok(None);
                };
                let Some(mapped) = ordinary_arg_map.get(&column.unique_id) else {
                    return Ok(None);
                };
                remapped.push(Expression::Column(mapped.clone()));
            }
            function.base.args = remapped;
        }
        function.mode = AggFunctionMode::Final;
        function.grouping_id = 0;
    }
    final_hash.base.set_children_req_props(vec![Some(
        crate::physical_property::PhysicalProperty::default(),
    )]);

    let partition_cols = partial_hash
        .group_by_items
        .iter()
        .filter_map(|item| match item {
            Expression::Column(column) => Some(crate::physical_property::MppPartitionColumn {
                collate_id: column
                    .get_static_type()
                    .map(|field_type| {
                        crate::physical_property::collate_id_for_partition(
                            field_type.collation_name(),
                        )
                    })
                    .unwrap_or(-1),
                col: column.clone(),
            }),
            _ => None,
        })
        .collect();
    Ok(Some(MultiDistinctThreeStageAggSplit {
        expand,
        partial_projection,
        partial: PhysicalPlan::HashAgg(partial_hash),
        middle: PhysicalPlan::HashAgg(middle_hash),
        final_agg: PhysicalPlan::HashAgg(final_hash),
        partition_cols,
    }))
}

/// Mirrors Go `scaleStats4GroupingSets`: the Expand rows are replicated, but
/// the partial aggregate's output cardinality is the sum of the NDV for each
/// grouping layout. Existing composite NDVs are adjusted for the grouping
/// columns that become NULL in each layout and all other profile metadata is
/// retained.
fn scale_three_stage_multi_distinct_stats(
    partial: &mut crate::physical::PhysicalHashAgg,
    grouping_sets: &[Vec<Column>],
    grouping_id_col: &Column,
    child_stats: Option<&StatsInfo>,
) {
    let Some(child_stats) = child_stats else {
        return;
    };
    let grouping_ids: HashSet<_> = grouping_sets
        .iter()
        .flat_map(|set| set.iter().map(|column| column.unique_id))
        .collect();
    let mut normal_group_ids = Vec::new();
    for item in &partial.group_by_items {
        for column in tidb_expr::expr_util::extract_columns(item) {
            if column.unique_id != grouping_id_col.unique_id
                && !grouping_ids.contains(&column.unique_id)
            {
                normal_group_ids.push(column.unique_id);
            }
        }
    }

    let sum_ndv = grouping_sets
        .iter()
        .map(|set| {
            let mut ids: Vec<_> = set.iter().map(|column| column.unique_id).collect();
            ids.extend(normal_group_ids.iter().copied());
            crate::cardinality::derive_stats::estimate_cols_ndv_with_matched_len(&ids, child_stats)
                .0
        })
        .sum::<f64>();
    let col_ndvs: Vec<(i64, f64)> = partial
        .base
        .base
        .stats_info()
        .map(|stats| stats.col_ndvs().keys().map(|id| (*id, sum_ndv)).collect())
        .unwrap_or_default();

    let group_ndvs = partial
        .base
        .base
        .stats_info()
        .map(|stats| {
            stats
                .group_ndvs()
                .iter()
                .map(|group| {
                    let mut ndv = group.ndv;
                    let mut intersection_ids = Vec::new();
                    for (index, column_id) in group.columns.iter().enumerate() {
                        if grouping_ids.contains(column_id) {
                            let before_len = intersection_ids.len();
                            intersection_ids.extend_from_slice(&group.columns[index..]);
                            let (increment, _) = crate::cardinality::derive_stats::
                                estimate_cols_ndv_with_matched_len(
                                    &intersection_ids,
                                    child_stats,
                                );
                            ndv += increment;
                            intersection_ids.truncate(before_len);
                        }
                        intersection_ids.push(*column_id);
                    }
                    GroupNdv {
                        columns: group.columns.clone(),
                        ndv,
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let Some(old_stats) = partial.base.base.stats_info() else {
        return;
    };
    let mut new_stats = StatsInfo::new(sum_ndv, col_ndvs)
        .with_stats_version(old_stats.stats_version())
        .with_group_ndvs(group_ndvs);
    if let Some(hist_coll) = old_stats.hist_coll().cloned() {
        new_stats = new_stats.with_hist_coll(hist_coll);
    }
    partial.base.base.set_stats(Some(new_stats));
}

/// Go `BasePhysicalAgg.ConvertAvgForMPP` (`base_physical_agg.go:209`).
///
/// TiFlash's MPP aggregation contract exposes AVG as a COUNT/SUM pair and
/// reconstructs the original AVG in a projection above the aggregate.  This
/// mutates the supplied physical HashAgg to the pair and returns that
/// projection.  A non-AVG aggregate returns `None` without changing the
/// plan.
pub fn convert_avg_for_mpp(
    plan: &mut crate::physical::PhysicalPlan,
    alloc: &ColumnIdAllocator,
    plan_ids: &crate::plan_base::PlanIdAllocator,
) -> Result<Option<crate::physical::PhysicalProjection>, crate::plan_base::PlanError> {
    use crate::physical::{BasePhysicalPlan, PhysicalPlan, PhysicalProjection};
    use tidb_expr::aggregation::names;
    use tidb_expr::expr_util::{FunctionBuilder, RealFunctionBuilder};

    let PhysicalPlan::HashAgg(agg) = plan else {
        return Ok(None);
    };
    let Some(original_schema) = agg.base.base.schema().cloned() else {
        return Ok(None);
    };
    if !agg
        .agg_funcs
        .iter()
        .any(|function| function.name() == names::AVG)
    {
        return Ok(None);
    }

    let ctx = tidb_expr::ZonedNoColumns(tidb_expr::SessionTimeZone::utc());
    let builder = RealFunctionBuilder::new(&ctx);
    let mut new_schema = Schema {
        columns: Vec::with_capacity(original_schema.columns.len() + agg.agg_funcs.len()),
        pk_or_uk: original_schema.pk_or_uk.clone(),
        nullable_uk: original_schema.nullable_uk.clone(),
    };
    let mut new_agg_funcs = Vec::with_capacity(agg.agg_funcs.len() * 2);
    let mut projection_exprs = Vec::with_capacity(original_schema.columns.len());

    for (index, function) in agg.agg_funcs.iter().enumerate() {
        let Some(output_column) = original_schema.columns.get(index) else {
            return Ok(None);
        };
        if function.name() != names::AVG {
            new_agg_funcs.push(function.clone());
            new_schema.columns.push(output_column.clone());
            projection_exprs.push(Expression::Column(output_column.clone()));
            continue;
        }

        let mut count = function.clone();
        count.base.name = names::COUNT.to_owned();
        if count.base.type_infer(&ctx).is_err() {
            return Ok(None);
        }
        let count_column = Column::new(alloc.alloc(), count.base.ret_type.clone());

        let mut sum = function.clone();
        sum.base.name = names::SUM.to_owned();
        if sum
            .base
            .type_infer_4_avg_sum(&function.base.ret_type)
            .is_err()
        {
            return Ok(None);
        }
        let mut sum_column = output_column.clone();
        sum_column.ret_type = Some(sum.base.ret_type.clone());

        new_agg_funcs.push(count);
        new_agg_funcs.push(sum);
        new_schema.columns.push(count_column.clone());
        new_schema.columns.push(sum_column.clone());

        let count_expr = Expression::Column(count_column.clone());
        let zero = Expression::Constant(tidb_expr::constant::Constant::new_zero());
        let condition = builder
            .new_function("eq", None, vec![count_expr.clone(), zero])
            .map_err(|error| crate::plan_base::PlanError::internal(error.to_string()))?;
        let denominator = builder
            .new_function(
                "case",
                None,
                vec![
                    condition,
                    Expression::Constant(tidb_expr::constant::Constant::new_one()),
                    count_expr,
                ],
            )
            .map_err(|error| crate::plan_base::PlanError::internal(error.to_string()))?;
        let quotient = builder
            .new_function(
                "div",
                Some(
                    output_column
                        .ret_type
                        .clone()
                        .unwrap_or_else(|| function.base.ret_type.clone()),
                ),
                vec![Expression::Column(sum_column), denominator],
            )
            .map_err(|error| crate::plan_base::PlanError::internal(error.to_string()))?;
        projection_exprs.push(quotient);
    }
    for output_column in original_schema.columns.iter().skip(agg.agg_funcs.len()) {
        projection_exprs.push(Expression::Column(output_column.clone()));
    }

    agg.agg_funcs = new_agg_funcs;
    agg.base.base.set_schema(Some(new_schema));
    let mut projection_base =
        BasePhysicalPlan::new(plan_ids, "Projection", agg.base.base.query_block_offset());
    projection_base
        .base
        .set_stats(agg.base.base.stats_info().cloned());
    projection_base
        .base
        .set_schema(Some(original_schema.clone()));
    if let Some(prop) = agg.base.child_req_prop(0) {
        projection_base.set_children_req_props(vec![Some(prop.clone_essential_fields())]);
    }
    Ok(Some(PhysicalProjection {
        base: projection_base,
        exprs: projection_exprs,
        calculate_no_delay: false,
        avoid_column_evaluator: false,
    }))
}

/// Go `genFirstRowAggForGroupBy` (`:441`): one `firstrow(item)` per group-by
/// item, for the TiDB-cop case whose executor does not output group-by
/// values.
pub fn gen_first_row_agg_for_group_by(
    ctx: &impl Columns,
    group_by_items: &[Expression],
) -> Result<Vec<AggFuncDesc>, tidb_expr::aggregation::AggDescError> {
    let mut agg_funcs = Vec::with_capacity(group_by_items.len());
    for group_by in group_by_items {
        agg_funcs.push(AggFuncDesc::new(
            ctx,
            names::FIRST_ROW,
            vec![group_by.clone()],
            false,
        )?);
    }
    Ok(agg_funcs)
}

/// Go `computePartialCursorOffset` (`:507`).
fn compute_partial_cursor_offset(name: &str) -> usize {
    let mut offset = 0;
    if need_count(name) {
        offset += 1;
    }
    if need_value(name) {
        offset += 1;
    }
    if name == names::APPROX_COUNT_DISTINCT {
        offset += 1;
    }
    offset
}

/// Go `RemoveUnnecessaryFirstRow` (`:460`): a partial `firstrow(expr)` whose
/// argument already IS a group-by item is dropped — its value arrives through
/// the group-by schema — and the FINAL firstrow's argument is redirected to
/// the final group-by column. Returns the surviving partial functions;
/// `final_agg_funcs` is mutated exactly where Go writes through
/// `firstRowFuncMap`.
pub fn remove_unnecessary_first_row(
    final_agg_funcs: &mut [AggFuncDesc],
    final_gby_items: &[Expression],
    partial_agg_funcs: Vec<AggFuncDesc>,
    partial_gby_items: &[Expression],
    partial_schema: &mut Schema,
    first_row_func_map: &HashMap<usize, usize>,
) -> Vec<AggFuncDesc> {
    let mut partial_cursor = 0;
    let mut new_agg_funcs = Vec::with_capacity(partial_agg_funcs.len());
    for (partial_index, agg_func) in partial_agg_funcs.into_iter().enumerate() {
        if agg_func.name() == names::FIRST_ROW {
            let mut can_optimize = false;
            for (j, gby_expr) in partial_gby_items.iter().enumerate() {
                if j >= final_gby_items.len() {
                    // After distinct push, the partial group-by can be longer
                    // than the final one (`select a, count(distinct a)`); the
                    // root task's firstrow cannot be removed.
                    break;
                }
                // A constant group-by item is skipped: for
                // `SELECT DISTINCT SQRT(1)` the `firstrow(SQRT(1))` must
                // stay.
                if matches!(gby_expr, Expression::Constant(_)) {
                    continue;
                }
                if gby_expr.equal(&agg_func.base.args[0]) {
                    can_optimize = true;
                    if let Some(final_index) = first_row_func_map.get(&partial_index) {
                        final_agg_funcs[*final_index].base.args[0] = final_gby_items[j].clone();
                    }
                    break;
                }
            }
            if can_optimize {
                partial_schema.columns.remove(partial_cursor);
                continue;
            }
        }
        partial_cursor += compute_partial_cursor_offset(agg_func.name());
        new_agg_funcs.push(agg_func);
    }
    new_agg_funcs
}

/// Go's `types.NewFieldType(mysql.TypeLonglong)` count column: `Flen` 21,
/// binary charset and collation.
fn count_column_type() -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::LongLong);
    ft.set_flen(21);
    ft.set_charset_name("binary");
    ft.set_collation_name("binary");
    ft
}

/// Go's `mysql.TypeString` sketch column for `approx_count_distinct`:
/// binary charset/collation plus `NotNullFlag`.
fn approx_count_distinct_column_type() -> FieldType {
    let mut ft = FieldType::new(FieldTypeCode::String);
    ft.set_charset_name("binary");
    ft.set_collation_name("binary");
    ft.set_flags(ft.flags() | tidb_datatype::FieldTypeFlags::NOT_NULL);
    ft
}

/// The mutable split state Go's `getDistinctExpr` closure captures.
struct DistinctState<'a> {
    partial: &'a mut AggInfo,
    partial_gby_schema: &'a mut Schema,
    partial_cursor: &'a mut usize,
}

/// Go `getDistinctExpr` (`:668`): route one distinct argument through the
/// partial group-by, allocating a column for it if it is not one already,
/// and — outside cop, or for group_concat order-by items — a `firstrow` to
/// carry it.
fn get_distinct_expr(
    ctx: &impl Columns,
    alloc: &ColumnIdAllocator,
    state: &mut DistinctState<'_>,
    distinct_arg: &Expression,
    only_add_first_row: bool,
    partial_is_cop: bool,
) -> Expression {
    let mut ret: Option<Expression> = None;
    // 1. An argument already in the group-by list, with the exact type,
    //    reuses that group-by column.
    for (j, gby_expr) in state.partial.group_by_items.iter().enumerate() {
        let types_equal = match (gby_expr.static_type(), distinct_arg.static_type()) {
            (Some(left), Some(right)) => left.equal(right),
            _ => false,
        };
        if gby_expr.equal(distinct_arg) && types_equal {
            ret = Some(Expression::Column(
                state.partial_gby_schema.columns[j].clone(),
            ));
            break;
        }
    }
    if ret.is_none() {
        let gby_col = if let Expression::Column(col) = distinct_arg {
            col.clone()
        } else {
            Column::new(
                alloc.alloc(),
                distinct_arg
                    .static_type()
                    .cloned()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
            )
        };
        // 2. Add the group-by item if needed.
        if !only_add_first_row {
            state.partial.group_by_items.push(distinct_arg.clone());
            state.partial_gby_schema.append([gby_col.clone()]);
            ret = Some(Expression::Column(gby_col.clone()));
        }
        // 3. Add `firstrow()` if needed: a cop partial outputs group-by
        //    values through its schema, so the firstrow is redundant there;
        //    group_concat order-by items always take one.
        if !partial_is_cop || only_add_first_row {
            let first_row =
                AggFuncDesc::new(ctx, names::FIRST_ROW, vec![distinct_arg.clone()], false)
                    .unwrap_or_else(|error| {
                        // Go: `panic("NewAggFuncDesc FirstRow meets error: " + ...)`.
                        panic!("NewAggFuncDesc FirstRow meets error: {error:?}")
                    });
            let mut new_col = gby_col;
            new_col.ret_type = Some(first_row.base.ret_type.clone());
            state.partial.agg_funcs.push(first_row);
            state.partial.schema.append([new_col.clone()]);
            if only_add_first_row {
                ret = Some(Expression::Column(new_col));
            }
            *state.partial_cursor += 1;
        }
    }
    ret.expect("getDistinctExpr always resolves through one of its three steps")
}

/// Go `BuildFinalModeAggregation` (`:600`). `None` is Go's
/// `partial == nil, final == original` return: the aggregation must run in
/// ONE phase (group_concat with order-by but no distinct, or a failed
/// avg-split type inference).
///
/// `partial_is_cop` and `is_mpp_task` carry Go's meanings; the live callers
/// in this port pass `(true, false)`, but every branch both flags gate is
/// ported.
#[allow(clippy::too_many_lines)] // Go's own comment: "This for loop is ugly".
pub fn build_final_mode_aggregation(
    ctx: &impl Columns,
    alloc: &ColumnIdAllocator,
    original: &AggInfo,
    partial_is_cop: bool,
    is_mpp_task: bool,
) -> Option<FinalModeSplit> {
    let mut first_row_func_map: HashMap<usize, usize> =
        HashMap::with_capacity(original.agg_funcs.len());
    let mut partial = AggInfo {
        agg_funcs: Vec::with_capacity(original.agg_funcs.len()),
        group_by_items: original.group_by_items.clone(),
        schema: Schema::default(),
    };
    let mut partial_cursor = 0usize;
    let mut final_agg = AggInfo {
        agg_funcs: Vec::with_capacity(original.agg_funcs.len()),
        group_by_items: Vec::with_capacity(original.group_by_items.len()),
        schema: original.schema.clone(),
    };

    let mut partial_gby_schema = Schema::default();
    // Add group-by columns: an expression that is not already a column gets a
    // fresh plan column carrying its type.
    for gby_expr in &partial.group_by_items {
        let gby_col = if let Expression::Column(col) = gby_expr {
            col.clone()
        } else {
            Column::new(
                alloc.alloc(),
                gby_expr
                    .static_type()
                    .cloned()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong)),
            )
        };
        partial_gby_schema.append([gby_col.clone()]);
        final_agg.group_by_items.push(Expression::Column(gby_col));
    }

    for (i, agg_func) in original.agg_funcs.iter().enumerate() {
        // Go builds `finalAggFunc` field by field; the base fields land at
        // the end of the loop body exactly as Go's trailing assignments do.
        let mut final_name = agg_func.name().to_owned();
        let mut final_mode = AggFunctionMode::Complete;
        let mut final_has_distinct = false;
        let mut final_order_by_items = agg_func.order_by_items.clone();
        let mut final_grouping_id = 0;
        let mut args: Vec<Expression> = Vec::with_capacity(agg_func.base.args.len());
        if agg_func.has_distinct {
            // eg: SELECT COUNT(DISTINCT a), SUM(b) FROM t GROUP BY c —
            // the cop half groups by (c, a) and the root keeps the distinct
            // aggregate over the pushed column.
            let mut state = DistinctState {
                partial: &mut partial,
                partial_gby_schema: &mut partial_gby_schema,
                partial_cursor: &mut partial_cursor,
            };
            let arg_count = agg_func.base.args.len();
            for (j, distinct_arg) in agg_func.base.args.iter().enumerate() {
                // The last arg of group_concat is the separator: it goes to
                // the final half untouched.
                if agg_func.name() == names::GROUP_CONCAT && j + 1 == arg_count {
                    args.push(distinct_arg.clone());
                    continue;
                }
                args.push(get_distinct_expr(
                    ctx,
                    alloc,
                    &mut state,
                    distinct_arg,
                    false,
                    partial_is_cop,
                ));
            }

            let mut by_items = Vec::with_capacity(agg_func.order_by_items.len());
            for by_item in &agg_func.order_by_items {
                by_items.push(ByItems::new(
                    get_distinct_expr(ctx, alloc, &mut state, &by_item.expr, true, partial_is_cop),
                    by_item.desc,
                ));
            }

            if is_mpp_task && agg_func.grouping_id > 0 {
                // Keep the groupingID, else the split final aggregate loses
                // its grouping info.
                final_grouping_id = agg_func.grouping_id;
            }

            final_order_by_items = by_items;
            final_has_distinct = agg_func.has_distinct;
            // If the original mode is already partial (an Agg above a
            // PartitionUnion), the final becomes Partial2.
            if agg_func.mode == AggFunctionMode::Complete {
                final_mode = AggFunctionMode::Complete;
            } else if matches!(
                agg_func.mode,
                AggFunctionMode::Partial1 | AggFunctionMode::Partial2
            ) {
                final_mode = AggFunctionMode::Partial2;
            }
        } else {
            if agg_func.name() == names::GROUP_CONCAT && !agg_func.order_by_items.is_empty() {
                // group_concat with order-by but without distinct runs in one
                // phase only.
                return None;
            }
            // The variance/stddev family keeps a (count, sum, variance)
            // partial state that neither `NeedCount` nor `NeedValue` exposes,
            // so a two-phase split would leave the final descriptor without
            // an argument. Run it in one phase, as the executor's
            // `AggState::update` requires.
            if matches!(
                agg_func.name(),
                names::VAR_POP | names::VAR_SAMP | names::STDDEV_POP | names::STDDEV_SAMP
            ) {
                return None;
            }
            if need_count(&final_name) {
                if is_mpp_task && final_name == names::COUNT {
                    // For MPP the final count() merges by sum().
                    final_name = names::SUM.to_owned();
                } else {
                    partial
                        .schema
                        .append([Column::new(alloc.alloc(), count_column_type())]);
                    args.push(Expression::Column(
                        partial.schema.columns[partial_cursor].clone(),
                    ));
                    partial_cursor += 1;
                }
            }
            if final_name == names::APPROX_COUNT_DISTINCT {
                partial.schema.append([Column::new(
                    alloc.alloc(),
                    approx_count_distinct_column_type(),
                )]);
                args.push(Expression::Column(
                    partial.schema.columns[partial_cursor].clone(),
                ));
                partial_cursor += 1;
            }
            if need_value(&final_name) {
                // Go's max_count/min_count partial state is [count,
                // extrema value].  The extrema column must retain the
                // original argument type (including charset/collation), not
                // the aggregate's count-shaped return type; the final
                // descriptor compares this column as the same value type.
                let value_ret_type =
                    if matches!(final_name.as_str(), names::MAX_COUNT | names::MIN_COUNT) {
                        agg_func
                            .base
                            .args
                            .first()
                            .and_then(Expression::static_type)
                            .cloned()
                            .unwrap_or_else(|| {
                                original.schema.columns[i]
                                    .ret_type
                                    .clone()
                                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
                            })
                    } else {
                        original.schema.columns[i]
                            .ret_type
                            .clone()
                            .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong))
                    };
                partial
                    .schema
                    .append([Column::new(alloc.alloc(), value_ret_type)]);
                args.push(Expression::Column(
                    partial.schema.columns[partial_cursor].clone(),
                ));
                partial_cursor += 1;
            }
            if agg_func.name() == names::AVG {
                let mut cnt_agg = agg_func.clone();
                cnt_agg.base.name = names::COUNT.to_owned();
                if cnt_agg.base.type_infer(ctx).is_err() {
                    // must not happen (Go's comment) — one phase.
                    return None;
                }
                partial.schema.columns[partial_cursor - 2].ret_type =
                    Some(cnt_agg.base.ret_type.clone());
                // Deep clone, to avoid sharing the arguments.
                let mut sum_agg = agg_func.clone();
                sum_agg.base.name = names::SUM.to_owned();
                if sum_agg
                    .base
                    .type_infer_4_avg_sum(&agg_func.base.ret_type)
                    .is_err()
                {
                    return None;
                }
                partial.schema.columns[partial_cursor - 1].ret_type =
                    Some(sum_agg.base.ret_type.clone());
                partial.agg_funcs.push(cnt_agg);
                partial.agg_funcs.push(sum_agg);
            } else if agg_func.name() == names::APPROX_COUNT_DISTINCT
                || agg_func.name() == names::GROUP_CONCAT
            {
                let mut new_agg_func = agg_func.clone();
                new_agg_func.base.ret_type = partial.schema.columns[partial_cursor - 1]
                    .ret_type
                    .clone()
                    .unwrap_or_else(|| FieldType::new(FieldTypeCode::LongLong));
                partial.agg_funcs.push(new_agg_func);
                if agg_func.name() == names::GROUP_CONCAT {
                    // Append the trailing separator arg.
                    args.push(agg_func.base.args[agg_func.base.args.len() - 1].clone());
                }
            } else {
                // Other descriptors just split into two identical halves.
                let partial_func_desc = agg_func.clone();
                if agg_func.name() == names::FIRST_ROW {
                    first_row_func_map.insert(partial.agg_funcs.len(), i);
                }
                partial.agg_funcs.push(partial_func_desc);
            }

            if agg_func.mode == AggFunctionMode::Complete {
                final_mode = AggFunctionMode::Final;
            } else if matches!(
                agg_func.mode,
                AggFunctionMode::Partial1 | AggFunctionMode::Partial2
            ) {
                final_mode = AggFunctionMode::Partial2;
            }
            if is_mpp_task && agg_func.grouping_id > 0 {
                final_grouping_id = agg_func.grouping_id;
            }
        }

        final_agg.agg_funcs.push(AggFuncDesc {
            base: tidb_expr::aggregation::BaseFuncDesc {
                name: final_name,
                args,
                ret_type: agg_func.base.ret_type.clone(),
            },
            mode: final_mode,
            has_distinct: final_has_distinct,
            order_by_items: final_order_by_items,
            grouping_id: final_grouping_id,
        });
    }
    partial
        .schema
        .append(partial_gby_schema.columns.iter().cloned());
    if partial_is_cop {
        for func in &mut partial.agg_funcs {
            func.mode = AggFunctionMode::Partial1;
        }
    }
    Some(FinalModeSplit {
        partial,
        final_agg,
        first_row_func_map,
    })
}

/// Go `ContainVirtualColumn` (`pkg/expression/util.go:1635`): a column
/// backed by a generating expression cannot be pushed.
fn contain_virtual_column(exprs: &[Expression]) -> bool {
    exprs.iter().any(|expr| match expr {
        Expression::Column(col) => col.virtual_expr.is_some(),
        Expression::ScalarFunction(func) => contain_virtual_column(func.get_args()),
        _ => false,
    })
}

/// Go `ContainCorrelatedColumn` (`pkg/expression/util.go:1652`). Note Go's
/// switch does NOT look inside Constants — neither does this.
fn contain_correlated_column(exprs: &[Expression]) -> bool {
    exprs.iter().any(|expr| match expr {
        Expression::CorrelatedColumn(_) => true,
        Expression::ScalarFunction(func) => contain_correlated_column(func.get_args()),
        _ => false,
    })
}

/// Go `CheckAggCanPushCop` (`base_physical_agg.go:522`) for the TiKV store.
///
/// Ported checks, in Go's order: virtual/correlated columns in arguments,
/// `CheckAggPushDown` (with the default empty push-down blacklist Go reads
/// from the session), argument pushability, order-by-item pushability, and
/// the same two group-by checks. Go's final `AggFuncToPBExpr != nil` probe
/// includes the client request capability check from `pkg/kv/checker.go`:
/// having a PB mapping alone does not make JSON or variance aggregates
/// pushable. Go's refusal WARNING
/// (`Aggregation can not be pushed to ...`) narrows with the same unported
/// statement-context channel the other attach refusals name.
#[must_use]
pub fn check_agg_can_push_cop_tikv(
    agg_funcs: &[AggFuncDesc],
    group_by_items: &[Expression],
) -> bool {
    let blacklist = HashMap::new();
    for agg_func in agg_funcs {
        if contain_virtual_column(&agg_func.base.args)
            || contain_correlated_column(&agg_func.base.args)
        {
            return false;
        }
        if !tidb_expr::aggregation::check_agg_push_down(
            agg_func,
            tidb_expr::infer_pushdown::PushDownStore::TiKv,
            &blacklist,
        ) {
            return false;
        }
        // Go passes `aggFunc.Name == ast.AggFuncSum` as `canEnumPush`, a
        // TiFlash-only enum carve-out (`canExprPushDown`); for TiKV the
        // plain check is the same function.
        if !crate::pushdown::can_exprs_push_down_tikv(&agg_func.base.args) {
            return false;
        }
        if !agg_func.order_by_items.is_empty() {
            let exprs: Vec<Expression> = agg_func
                .order_by_items
                .iter()
                .map(|item| item.expr.clone())
                .collect();
            if !crate::pushdown::can_exprs_push_down_tikv(&exprs) {
                return false;
            }
        }
        // AggFuncToPBExpr asks RequestTypeSupportedChecker before encoding.
        // Keep its aggregate set here; CheckAggPushDown applies store limits.
        if !matches!(
            agg_func.name(),
            names::COUNT
                | names::FIRST_ROW
                | names::MAX
                | names::MIN
                | names::SUM
                | names::AVG
                | names::SUM_INT
                | names::MAX_COUNT
                | names::MIN_COUNT
                | names::BIT_XOR
                | names::BIT_AND
                | names::BIT_OR
                | names::APPROX_COUNT_DISTINCT
                | names::GROUP_CONCAT
        ) {
            return false;
        }
    }
    if contain_virtual_column(group_by_items) {
        return false;
    }
    crate::pushdown::can_exprs_push_down_tikv(group_by_items)
}

/// Go `checkCanPushDownToMPP` together with the TiFlash branch of
/// `CheckAggCanPushCop`.  TiFlash MPP currently permits DISTINCT only for
/// COUNT and GROUP_CONCAT, and does not admit APPROX_COUNT_DISTINCT.
#[must_use]
pub fn check_agg_can_push_mpp(agg_funcs: &[AggFuncDesc], group_by_items: &[Expression]) -> bool {
    let blacklist = HashMap::new();
    for agg_func in agg_funcs {
        if contain_virtual_column(&agg_func.base.args)
            || contain_correlated_column(&agg_func.base.args)
        {
            return false;
        }
        if agg_func.has_distinct
            && agg_func.name() != names::COUNT
            && agg_func.name() != names::GROUP_CONCAT
        {
            return false;
        }
        if agg_func.name() == names::APPROX_COUNT_DISTINCT
            || !tidb_expr::aggregation::check_agg_push_down(
                agg_func,
                tidb_expr::infer_pushdown::PushDownStore::TiFlash,
                &blacklist,
            )
            || !crate::pushdown::can_exprs_push_down_tiflash(&agg_func.base.args)
        {
            return false;
        }
        let order_by_exprs: Vec<Expression> = agg_func
            .order_by_items
            .iter()
            .map(|item| item.expr.clone())
            .collect();
        if !crate::pushdown::can_exprs_push_down_tiflash(&order_by_exprs) {
            return false;
        }
    }
    !contain_virtual_column(group_by_items)
        && crate::pushdown::can_exprs_push_down_tiflash(group_by_items)
}

/// Go `BasePhysicalAgg.NewPartialAggregate` (`base_physical_agg.go:279`) for
/// a TiKV cop task: `(None, plan)` is Go's `(nil, p.Self)` — the aggregate
/// stays whole — and `(Some(partial), final)` is the split, the partial
/// keeping the original plan's id and stats (Go mutates `p` into it) and the
/// final sharing those stats above it and receiving a fresh physical-plan ID.
///
/// The `copTaskType == kv.TiDB` firstrow-appending arm is not reachable: the
/// only caller hands TiKV cop tasks. Go's expression context is consulted
/// ONLY for argument types during `TypeInfer` on this path, which is why a
/// column-free context is exact here.
pub fn new_partial_aggregate(
    ctx: &impl Columns,
    alloc: &ColumnIdAllocator,
    plan: crate::physical::PhysicalPlan,
    plan_ids: &crate::plan_base::PlanIdAllocator,
) -> Result<
    (
        Option<crate::physical::PhysicalPlan>,
        crate::physical::PhysicalPlan,
    ),
    crate::plan_base::PlanError,
> {
    new_partial_aggregate_with_mode(ctx, alloc, plan, plan_ids, false)
}

/// Go `BasePhysicalAgg.NewPartialAggregate(kv.TiFlash, true)` for an MPP
/// fragment.  The MPP split keeps DISTINCT forms that TiFlash supports and
/// changes a final COUNT merge into SUM through
/// [`build_final_mode_aggregation`].
pub fn new_partial_aggregate_mpp(
    ctx: &impl Columns,
    alloc: &ColumnIdAllocator,
    plan: crate::physical::PhysicalPlan,
    plan_ids: &crate::plan_base::PlanIdAllocator,
) -> Result<
    (
        Option<crate::physical::PhysicalPlan>,
        crate::physical::PhysicalPlan,
    ),
    crate::plan_base::PlanError,
> {
    new_partial_aggregate_for_store(ctx, alloc, plan, plan_ids, true, true)
}

/// Go `NewPartialAggregate(kv.TiFlash, false)` for the MPP-TiDB mode: the
/// partial runs in TiFlash, but its final descriptor remains a normal COUNT
/// merge because TiDB, rather than TiFlash, owns that final stage.
pub fn new_partial_aggregate_mpp_tidb(
    ctx: &impl Columns,
    alloc: &ColumnIdAllocator,
    plan: crate::physical::PhysicalPlan,
    plan_ids: &crate::plan_base::PlanIdAllocator,
) -> Result<
    (
        Option<crate::physical::PhysicalPlan>,
        crate::physical::PhysicalPlan,
    ),
    crate::plan_base::PlanError,
> {
    new_partial_aggregate_for_store(ctx, alloc, plan, plan_ids, true, false)
}

fn new_partial_aggregate_with_mode(
    ctx: &impl Columns,
    alloc: &ColumnIdAllocator,
    plan: crate::physical::PhysicalPlan,
    plan_ids: &crate::plan_base::PlanIdAllocator,
    is_mpp: bool,
) -> Result<
    (
        Option<crate::physical::PhysicalPlan>,
        crate::physical::PhysicalPlan,
    ),
    crate::plan_base::PlanError,
> {
    new_partial_aggregate_for_store(ctx, alloc, plan, plan_ids, false, is_mpp)
}

fn new_partial_aggregate_for_store(
    ctx: &impl Columns,
    alloc: &ColumnIdAllocator,
    plan: crate::physical::PhysicalPlan,
    plan_ids: &crate::plan_base::PlanIdAllocator,
    is_tiflash: bool,
    is_mpp: bool,
) -> Result<
    (
        Option<crate::physical::PhysicalPlan>,
        crate::physical::PhysicalPlan,
    ),
    crate::plan_base::PlanError,
> {
    use crate::physical::PhysicalPlan;
    let (agg_funcs, group_by_items, is_stream, mpp_run_mode) = match &plan {
        PhysicalPlan::HashAgg(agg) => (
            &agg.agg_funcs,
            &agg.group_by_items,
            false,
            Some(agg.mpp_run_mode),
        ),
        PhysicalPlan::StreamAgg(agg) => (&agg.agg_funcs, &agg.group_by_items, true, None),
        _ => {
            return Err(crate::plan_base::PlanError::internal(
                "NewPartialAggregate over a non-aggregate plan",
            ))
        }
    };
    let can_push = if is_tiflash {
        check_agg_can_push_mpp(agg_funcs, group_by_items)
    } else {
        check_agg_can_push_cop_tikv(agg_funcs, group_by_items)
    };
    if !can_push {
        return Ok((None, plan));
    }
    // Go: with `tidb_opt_distinct_agg_push_down` OFF (the default,
    // `AllowDistinctAggPushDown == false`), `NewPartialAggregate` never
    // splits a DISTINCT aggregation — `applyLogicalAggregationHint` only
    // prefers a root-task plan for it. The live master capture for
    // `count(distinct from_address)` over `idx_from` is a single-phase
    // root HashAgg over a plain IndexReader: no cop partial stage.
    if !is_tiflash && !is_mpp && agg_funcs.iter().any(|function| function.has_distinct) {
        return Ok((None, plan));
    }
    let original = AggInfo {
        agg_funcs: agg_funcs.clone(),
        group_by_items: group_by_items.clone(),
        schema: plan.schema().cloned().unwrap_or_default(),
    };
    let Some(mut split) = build_final_mode_aggregation(ctx, alloc, &original, true, is_mpp) else {
        return Ok((None, plan));
    };
    // A stream aggregate whose split grew the group-by (a pushed distinct
    // argument) cannot keep its order contract: stay whole.
    if is_stream && split.partial.group_by_items.len() != split.final_agg.group_by_items.len() {
        return Ok((None, plan));
    }
    // Remove unnecessary FirstRow.
    let partial_funcs = std::mem::take(&mut split.partial.agg_funcs);
    split.partial.agg_funcs = remove_unnecessary_first_row(
        &mut split.final_agg.agg_funcs,
        &split.final_agg.group_by_items,
        partial_funcs,
        &split.partial.group_by_items,
        &mut split.partial.schema,
        &split.first_row_func_map,
    );
    // Go mutates `p` into the partial half (same plan id, same stats) and
    // Init's a NEW final of the same kind above it, with
    // `ExpectedCnt: math.MaxFloat64` and `p`'s stats.
    let mut partial = plan.clone();
    let mut final_base =
        crate::physical::BasePhysicalPlan::new(plan_ids, plan.tp(), plan.query_block_offset());
    final_base.base.set_stats(plan.stats_info().cloned());
    final_base.set_children_req_props(vec![Some(
        crate::physical_property::PhysicalProperty::default(),
    )]);
    let mut final_plan = match &plan {
        PhysicalPlan::HashAgg(_) => PhysicalPlan::HashAgg(crate::physical::PhysicalHashAgg {
            base: final_base,
            agg_funcs: Vec::new(),
            group_by_items: Vec::new(),
            mpp_run_mode: mpp_run_mode.unwrap_or_default(),
            ..Default::default()
        }),
        PhysicalPlan::StreamAgg(_) => PhysicalPlan::StreamAgg(crate::physical::PhysicalStreamAgg {
            base: final_base,
            agg_funcs: Vec::new(),
            group_by_items: Vec::new(),
        }),
        _ => unreachable!("the aggregate variant was checked above"),
    };
    match (&mut partial, &mut final_plan) {
        (PhysicalPlan::HashAgg(part), PhysicalPlan::HashAgg(fin)) => {
            part.agg_funcs = split.partial.agg_funcs;
            part.group_by_items = split.partial.group_by_items;
            part.base.base.set_schema(Some(split.partial.schema));
            fin.agg_funcs = split.final_agg.agg_funcs;
            fin.group_by_items = split.final_agg.group_by_items;
            fin.base.base.set_schema(Some(split.final_agg.schema));
        }
        (PhysicalPlan::StreamAgg(part), PhysicalPlan::StreamAgg(fin)) => {
            part.agg_funcs = split.partial.agg_funcs;
            part.group_by_items = split.partial.group_by_items;
            part.base.base.set_schema(Some(split.partial.schema));
            fin.agg_funcs = split.final_agg.agg_funcs;
            fin.group_by_items = split.final_agg.group_by_items;
            fin.base.base.set_schema(Some(split.final_agg.schema));
        }
        _ => unreachable!("both halves clone from the same variant"),
    }
    Ok((Some(partial), final_plan))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_expr::{SessionTimeZone, ZonedNoColumns};

    fn ctx() -> ZonedNoColumns {
        ZonedNoColumns(SessionTimeZone::utc())
    }

    fn bigint_col(unique_id: i64) -> Column {
        Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong))
    }

    fn agg(name: &str, arg: &Column, distinct: bool) -> AggFuncDesc {
        AggFuncDesc::new(
            &ctx(),
            name,
            vec![Expression::Column(arg.clone())],
            distinct,
        )
        .expect("descriptor")
    }

    #[test]
    fn cop_aggregation_requires_client_request_support() {
        let a = bigint_col(1);
        for name in [
            names::JSON_ARRAYAGG,
            names::JSON_OBJECTAGG,
            names::VAR_POP,
            names::VAR_SAMP,
            names::STDDEV_POP,
            names::STDDEV_SAMP,
        ] {
            let args = if name == names::JSON_OBJECTAGG {
                vec![Expression::Column(a.clone()), Expression::Column(a.clone())]
            } else {
                vec![Expression::Column(a.clone())]
            };
            let descriptor = AggFuncDesc::new(&ctx(), name, args, false).unwrap();
            assert!(!check_agg_can_push_cop_tikv(&[descriptor], &[]), "{name}");
        }
        for name in [names::COUNT, names::SUM, names::AVG, names::MIN, names::MAX] {
            assert!(
                check_agg_can_push_cop_tikv(&[agg(name, &a, false)], &[]),
                "{name}"
            );
        }
    }

    /// `select count(b) from t group by a`, cop split: the partial half
    /// keeps `count(b)` in Partial1 mode, its schema is the count column
    /// then the group-by column, and the final half counts THE COUNT COLUMN
    /// in Final mode over the aliased group-by column.
    #[test]
    fn a_plain_count_splits_into_partial1_below_and_final_above() {
        let a = bigint_col(1);
        let b = bigint_col(2);
        let alloc = ColumnIdAllocator::new();
        alloc.alloc(); // ids 1, 2 are taken by the test columns
        alloc.alloc();
        let count = agg(names::COUNT, &b, false);
        let original = AggInfo {
            schema: Schema::new(vec![Column::new(10, count.base.ret_type.clone())]),
            agg_funcs: vec![count],
            group_by_items: vec![Expression::Column(a.clone())],
        };
        let split = build_final_mode_aggregation(&ctx(), &alloc, &original, true, false)
            .expect("two-phase");

        assert_eq!(split.partial.agg_funcs.len(), 1);
        assert_eq!(split.partial.agg_funcs[0].name(), names::COUNT);
        assert_eq!(split.partial.agg_funcs[0].mode, AggFunctionMode::Partial1);
        // Schema: [count-out, group-by a].
        assert_eq!(split.partial.schema.columns.len(), 2);
        assert_eq!(split.partial.schema.columns[1].unique_id, a.unique_id);

        assert_eq!(split.final_agg.agg_funcs.len(), 1);
        let final_count = &split.final_agg.agg_funcs[0];
        assert_eq!(final_count.name(), names::COUNT);
        assert_eq!(final_count.mode, AggFunctionMode::Final);
        // Its argument IS the partial count output column.
        let Expression::Column(arg) = &final_count.base.args[0] else {
            panic!("final count must read the partial output column");
        };
        assert_eq!(arg.unique_id, split.partial.schema.columns[0].unique_id);
        // The final group-by is the group-by COLUMN, reused as-is.
        let Expression::Column(gby) = &split.final_agg.group_by_items[0] else {
            panic!("column group-by stays a column");
        };
        assert_eq!(gby.unique_id, a.unique_id);
    }

    /// `avg(b)` splits into partial `count(b)` + `sum(b)`; the final avg
    /// reads both partial output columns (Go's NeedCount + NeedValue pair).
    #[test]
    fn an_avg_splits_into_count_plus_sum_below() {
        let b = bigint_col(1);
        let alloc = ColumnIdAllocator::new();
        alloc.alloc();
        let avg = agg(names::AVG, &b, false);
        let original = AggInfo {
            schema: Schema::new(vec![Column::new(10, avg.base.ret_type.clone())]),
            agg_funcs: vec![avg],
            group_by_items: Vec::new(),
        };
        let split = build_final_mode_aggregation(&ctx(), &alloc, &original, true, false)
            .expect("two-phase");
        let names_below: Vec<&str> = split
            .partial
            .agg_funcs
            .iter()
            .map(AggFuncDesc::name)
            .collect();
        assert_eq!(names_below, [names::COUNT, names::SUM]);
        assert_eq!(split.final_agg.agg_funcs[0].name(), names::AVG);
        assert_eq!(split.final_agg.agg_funcs[0].base.args.len(), 2);
    }

    #[test]
    fn mpp_avg_conversion_exposes_count_sum_and_division_projection() {
        let input = bigint_col(1);
        let avg = agg(names::AVG, &input, false);
        let output = Column::new(20, avg.base.ret_type.clone());
        let plan_ids = crate::plan_base::PlanIdAllocator::new();
        let column_ids = ColumnIdAllocator::new();
        let mut base = crate::physical::BasePhysicalPlan::new(&plan_ids, "HashAgg", 0);
        base.base
            .set_schema(Some(Schema::new(vec![output.clone()])));
        base.set_children_req_props(vec![Some(
            crate::physical_property::PhysicalProperty::default(),
        )]);
        let mut plan = crate::physical::PhysicalPlan::HashAgg(crate::physical::PhysicalHashAgg {
            base,
            agg_funcs: vec![avg],
            group_by_items: Vec::new(),
            ..Default::default()
        });

        let projection = convert_avg_for_mpp(&mut plan, &column_ids, &plan_ids)
            .expect("AVG conversion")
            .expect("AVG must be rewritten for MPP");
        let crate::physical::PhysicalPlan::HashAgg(agg) = plan else {
            panic!("AVG conversion must retain a HashAgg");
        };
        assert_eq!(
            agg.agg_funcs
                .iter()
                .map(AggFuncDesc::name)
                .collect::<Vec<_>>(),
            [names::COUNT, names::SUM]
        );
        assert_eq!(agg.base.base.schema().expect("MPP schema").len(), 2);
        assert_eq!(projection.exprs.len(), 1);
        let Expression::ScalarFunction(function) = &projection.exprs[0] else {
            panic!("AVG projection must divide SUM by COUNT");
        };
        assert_eq!(function.func_name.lowercase(), "div");
    }

    /// `select a, count(b) group by a`: the partial `firstrow(a)` duplicates
    /// the group-by output, so `remove_unnecessary_first_row` drops it, its
    /// schema column goes too, and the FINAL firstrow's argument is
    /// redirected to the final group-by column (Go `:460`'s example).
    #[test]
    fn a_group_by_first_row_is_removed_and_redirected() {
        let a = bigint_col(1);
        let b = bigint_col(2);
        let alloc = ColumnIdAllocator::new();
        alloc.alloc();
        alloc.alloc();
        let first_row = agg(names::FIRST_ROW, &a, false);
        let count = agg(names::COUNT, &b, false);
        let original = AggInfo {
            schema: Schema::new(vec![
                Column::new(10, first_row.base.ret_type.clone()),
                Column::new(11, count.base.ret_type.clone()),
            ]),
            agg_funcs: vec![first_row, count],
            group_by_items: vec![Expression::Column(a.clone())],
        };
        let mut split = build_final_mode_aggregation(&ctx(), &alloc, &original, true, false)
            .expect("two-phase");
        assert_eq!(split.partial.agg_funcs.len(), 2);
        let schema_before = split.partial.schema.columns.len();

        let partial_funcs = std::mem::take(&mut split.partial.agg_funcs);
        let kept = remove_unnecessary_first_row(
            &mut split.final_agg.agg_funcs,
            &split.final_agg.group_by_items,
            partial_funcs,
            &split.partial.group_by_items,
            &mut split.partial.schema,
            &split.first_row_func_map,
        );
        // Only the count survives below.
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].name(), names::COUNT);
        assert_eq!(split.partial.schema.columns.len(), schema_before - 1);
        // The final firstrow now reads the final group-by column.
        let final_first_row = &split.final_agg.agg_funcs[0];
        assert_eq!(final_first_row.name(), names::FIRST_ROW);
        assert!(final_first_row.base.args[0].equal(&split.final_agg.group_by_items[0]));
    }

    /// `count(distinct a) group by c`, cop split (Go's own worked example at
    /// `:657`): the cop half groups by (c, a) with NO aggregate functions,
    /// and the root keeps `count(distinct ...)` over the pushed column.
    #[test]
    fn a_distinct_count_pushes_its_argument_into_the_partial_group_by() {
        let a = bigint_col(1);
        let c = bigint_col(2);
        let alloc = ColumnIdAllocator::new();
        alloc.alloc();
        alloc.alloc();
        let count = agg(names::COUNT, &a, true);
        let original = AggInfo {
            schema: Schema::new(vec![Column::new(10, count.base.ret_type.clone())]),
            agg_funcs: vec![count],
            group_by_items: vec![Expression::Column(c.clone())],
        };
        let split = build_final_mode_aggregation(&ctx(), &alloc, &original, true, false)
            .expect("two-phase");
        // A cop partial outputs group-by values through its schema: no
        // firstrow is added, so no partial functions at all.
        assert!(split.partial.agg_funcs.is_empty());
        assert_eq!(split.partial.group_by_items.len(), 2);
        assert!(split.partial.group_by_items[1].equal(&Expression::Column(a.clone())));
        let final_count = &split.final_agg.agg_funcs[0];
        assert!(final_count.has_distinct);
        assert_eq!(final_count.mode, AggFunctionMode::Complete);
        // Its argument is the group-by column for a, not a itself... which
        // for a plain column IS the same column reference.
        assert!(final_count.base.args[0].equal(&Expression::Column(a)));
    }

    /// group_concat with order-by but WITHOUT distinct runs in one phase
    /// only: Go returns `(nil, original)`, here `None`.
    #[test]
    fn a_sorted_group_concat_without_distinct_refuses_to_split() {
        let a = bigint_col(1);
        let b = bigint_col(2);
        let alloc = ColumnIdAllocator::new();
        alloc.alloc();
        alloc.alloc();
        let mut concat = agg(names::GROUP_CONCAT, &a, false);
        concat
            .order_by_items
            .push(ByItems::new(Expression::Column(b), false));
        let original = AggInfo {
            schema: Schema::new(vec![Column::new(10, concat.base.ret_type.clone())]),
            agg_funcs: vec![concat],
            group_by_items: Vec::new(),
        };
        assert!(build_final_mode_aggregation(&ctx(), &alloc, &original, true, false).is_none());
    }

    #[test]
    fn multi_distinct_admission_matches_go_grouping_set_targeting() {
        let a = bigint_col(1);
        let b = bigint_col(2);
        let c = bigint_col(3);
        let funcs = vec![
            agg(names::COUNT, &a, true),
            agg(names::COUNT, &b, true),
            agg(names::COUNT, &c, false),
        ];
        let grouping_sets = can_use_three_stage_multi_distinct(&funcs, &[], true, true)
            .expect("two independent distinct layouts are admissible");
        let mut marked = funcs.clone();
        assert!(mark_three_stage_grouping_ids(&mut marked, &grouping_sets));
        assert_eq!(
            marked
                .iter()
                .map(|function| function.grouping_id)
                .collect::<Vec<_>>(),
            [1, 2, 1]
        );
        assert!(can_use_three_stage_multi_distinct(&funcs, &[], false, true).is_none());
        assert!(can_use_three_stage_multi_distinct(&funcs, &[], true, false).is_none());

        let overlapping = vec![
            AggFuncDesc::new(
                &ctx(),
                names::COUNT,
                vec![Expression::Column(a), Expression::Column(b.clone())],
                true,
            )
            .expect("multi-column distinct descriptor"),
            agg(names::COUNT, &b, true),
        ];
        assert!(can_use_three_stage_multi_distinct(&overlapping, &[], true, true).is_none());
    }
}
