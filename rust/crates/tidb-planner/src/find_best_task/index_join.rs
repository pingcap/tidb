// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go's IndexJoin inner-property flow. Secondary path statistics/selection and
//! family enumeration remain open; no reduced unhinted search is activated.

use super::{dispatch::DispatchContext, LogicalJoinType};
use crate::{
    logical::{DataSource, LogicalJoin, LogicalPlan},
    physical::{BasePhysicalJoin, BasePhysicalPlan, PhysicalIndexJoin, PhysicalPlan},
    physical_property::{IndexJoinRuntimeProp, PhysicalProperty},
    plan_base::{PlanError, PlanIdAllocator},
    task::Task,
    task_type::TaskType,
};
use tidb_expr::simple_expr::extract_columns_from_expressions;

pub mod candidate;
pub mod path;
mod table;

/// Current statement's ranger inputs for native IndexJoin path construction.
pub struct IndexJoinRangerSettings<'a> {
    /// Current statement bindings, shared by all alternatives in this search.
    pub eval_expression: &'a crate::ranger::points::ExpressionEvaluator<'a>,
    /// Go SessionVars.RangeMaxSize (64 MiB by default).
    pub range_max_size: i64,
    /// Go RegardNULLAsPoint.
    pub regard_null_as_point: bool,
    /// Go OptPrefixIndexSingleScan.
    pub opt_prefix_index_single_scan: bool,
    /// Whether parameter-dependent range definitions must survive cache reuse.
    pub use_plan_cache: bool,
    /// Go RecordRangeFallback events, including rejected alternatives.
    pub range_fallbacks: std::cell::RefCell<Vec<i64>>,
    /// Go's failed range-build diagnostics; a failed candidate is not selected.
    pub range_errors: std::cell::RefCell<Vec<PlanError>>,
}

impl Default for IndexJoinRangerSettings<'_> {
    fn default() -> Self {
        Self {
            eval_expression: &crate::ranger::points::evaluate_static,
            range_max_size: 64 * 1024 * 1024,
            regard_null_as_point: true,
            opt_prefix_index_single_scan: true,
            use_plan_cache: false,
            range_fallbacks: std::cell::RefCell::new(Vec::new()),
            range_errors: std::cell::RefCell::new(Vec::new()),
        }
    }
}

/// Go `admitIndexJoinInnerChildPattern`, including its aggregate group-key rule.
pub fn admits_inner(
    plan: &LogicalPlan,
    property: &IndexJoinRuntimeProp,
    multi_pattern: bool,
) -> bool {
    match plan {
        LogicalPlan::DataSource(ds) => {
            ds.prefer_store_type & crate::logical::data_source::PREFER_TIFLASH == 0
        }
        LogicalPlan::Selection(_) | LogicalPlan::Projection(_) => multi_pattern,
        LogicalPlan::Join(join) => multi_pattern && join.join_type == LogicalJoinType::Inner,
        LogicalPlan::Aggregation(agg) => {
            if !multi_pattern {
                return false;
            }
            let groups = extract_columns_from_expressions(&agg.group_by_items, None);
            let mut child = plan;
            let schema = loop {
                if let LogicalPlan::DataSource(ds) = child {
                    break ds.base.base.schema();
                }
                let [next] = child.children() else {
                    return false;
                };
                child = next;
            };
            let Some(schema) = schema else {
                return false;
            };
            property
                .inner_join_keys()
                .iter()
                .filter(|key| schema.contains(key))
                .all(|key| {
                    groups
                        .iter()
                        .any(|column| column.unique_id == key.unique_id)
                })
        }
        LogicalPlan::UnionScan(_) => true,
        _ => false,
    }
}

/// Go `constructIndexJoinStatic`, the regular family member. The caller still
/// enumerates outer sides, access-path classes and the other IndexJoin families.
pub fn construct_index_join_static(
    join: &LogicalJoin,
    property: &PhysicalProperty,
    outer_idx: usize,
    table_range_scan: bool,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
    ordering_ratio: f64,
) -> Result<PhysicalPlan, PlanError> {
    if outer_idx > 1 {
        return Err(PlanError::internal("invalid IndexJoin outer index"));
    }
    let outer = join
        .base
        .children()
        .get(outer_idx)
        .ok_or_else(|| PlanError::internal("IndexJoin outer child missing"))?;
    let outer_schema = outer
        .schema()
        .ok_or_else(|| PlanError::internal("IndexJoin outer schema missing"))?;
    if property.index_join_prop.is_some()
        || !property.all_same_order().0
        || !property.sort_items.iter().all(|item| {
            outer_schema
                .columns
                .iter()
                .any(|col| col.unique_id == item.col)
        })
    {
        return Err(PlanError::internal(
            "IndexJoin cannot satisfy the required outer order",
        ));
    }
    let outer_rows = outer
        .stats_info()
        .ok_or_else(|| PlanError::internal("IndexJoin outer statistics missing"))?
        .row_count();
    let stats = join
        .base
        .base
        .stats_info()
        .ok_or_else(|| PlanError::internal("IndexJoin statistics missing"))?;
    let (left, right, null_eq, _) = join.get_join_keys();
    let (outer_keys, inner_keys) = if outer_idx == 0 {
        (left, right)
    } else {
        (right, left)
    };
    let lookup = IndexJoinRuntimeProp::new(
        join.other_conditions.clone(),
        outer_keys.clone(),
        inner_keys.clone(),
        if outer_rows > 0.0 {
            join.equal_cond_out_cnt / outer_rows
        } else {
            0.0
        },
        table_range_scan,
    );
    let mut requirements = [PhysicalProperty::default(), PhysicalProperty::default()];
    for requirement in &mut requirements {
        requirement.cte_producer_status = property.cte_producer_status;
        requirement.no_cop_push_down = property.no_cop_push_down;
    }
    requirements[outer_idx].sort_items = property.sort_items.clone();
    requirements[outer_idx].expected_cnt = crate::physical::merge_join::child_expected_count(
        property,
        outer_rows,
        stats.row_count(),
        ordering_ratio,
    );
    requirements[1 - outer_idx].index_join_prop = Some(lookup);
    let mut base =
        BasePhysicalPlan::new(allocator, "IndexJoin", join.base.base.query_block_offset());
    base.base.set_schema(join.base.base.schema().cloned());
    base.base.set_stats(Some(
        stats.scale_by_expect_cnt(property.expected_cnt, skew_ratio),
    ));
    base.set_children_req_props(requirements.into_iter().map(Some).collect());
    Ok(PhysicalPlan::IndexJoin(PhysicalIndexJoin {
        join: BasePhysicalJoin {
            base,
            inner_child_idx: 1 - outer_idx,
            join_type: join.join_type,
            left_conditions: join.left_conditions.clone(),
            right_conditions: join.right_conditions.clone(),
            other_conditions: join.other_conditions.clone(),
            is_null_eq: null_eq,
            default_values: join.default_values.clone(),
            ..Default::default()
        },
        outer_join_keys: outer_keys,
        inner_join_keys: inner_keys,
        equal_conditions: join.equal_conditions.clone(),
        from_decorrelated_apply: join.from_decorrelated_apply && outer_idx == 0,
        ..Default::default()
    }))
}

/// Go `getBestIndexJoinInnerTaskByProp`.
pub(super) fn find_inner_task(
    ds: &DataSource,
    prop: &PhysicalProperty,
    ctx: &DispatchContext<'_>,
) -> Result<Task, PlanError> {
    let lookup = prop.index_join_prop.as_ref().expect("IndexJoin property");
    if !lookup.table_range_scan() {
        return Err(PlanError::internal(
            "native IndexJoin secondary path statistics/selection is not implemented",
        ));
    }
    let task = table::build_table_task(ds, prop, ctx)?;
    if !task.invalid() && prop.task_tp == TaskType::Root {
        task.into_root_task()
    } else {
        Ok(task)
    }
}
