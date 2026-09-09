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

//! Go physicalop.GetMergeJoin: enumerate natural and enforced key orders.

use super::{BasePhysicalJoin, BasePhysicalPlan, PhysicalMergeJoin, PhysicalPlan};
use crate::find_best_task::LogicalJoinType;
use crate::logical::{
    functional_dependencies::{self, FdContext},
    LogicalJoin,
};
use crate::physical_property::{PhysicalProperty, SortItem, TaskType};
use crate::plan_base::{PlanError, PlanIdAllocator};
use crate::plan_builder::from::join_hint_flags as hint;
use tidb_datatype::{EvalType, FieldTypeCode};
use tidb_expr::{column::Column, expression::Expression};
use tidb_funcdep::ColSet;

/// Merge-specific statement settings.
#[derive(Clone, Copy, Debug)]
pub struct MergeJoinSettings {
    /// Go OptOrderingIdxSelRatio, recorded in the plan's relevant settings.
    pub ordering_index_selectivity_ratio: f64,
}
impl Default for MergeJoinSettings {
    fn default() -> Self {
        Self {
            ordering_index_selectivity_ratio: 0.01,
        }
    }
}
/// Candidate plans and the setting dependency read by CalcChildExpectedCnt.
pub struct MergeJoinCandidates {
    /// Natural child orders first, followed by the enforced candidate.
    pub plans: Vec<PhysicalPlan>,
    /// Go RecordRelevantOptVar(TiDBOptOrderingIdxSelRatio).
    pub uses_ordering_index_selectivity: bool,
}

/// Native candidates, driven by the logical join's complete FD derivation.
pub fn get_merge_joins(
    join: &LogicalJoin,
    property: &PhysicalProperty,
    fd_context: &FdContext<'_>,
    settings: MergeJoinSettings,
    disable_hash_join: bool,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
    warnings: &mut Vec<String>,
) -> Result<MergeJoinCandidates, PlanError> {
    let (left, right, null_eq, has_null_eq) = join.get_join_keys();
    let constants = functional_dependencies::join(join, fd_context, false)?.constant_cols();
    let mut result = MergeJoinCandidates {
        plans: Vec::new(),
        uses_ordering_index_selectivity: false,
    };
    if has_null_eq
        || left.iter().chain(&right).any(|key| {
            key.ret_type
                .as_ref()
                .is_some_and(|ft| matches!(ft.code(), FieldTypeCode::Enum | FieldTypeCode::Set))
        })
    {
        return Ok(result);
    }
    let stats = join
        .base
        .base
        .stats_info()
        .ok_or_else(|| PlanError::internal("merge join statistics have not been derived"))?;
    let [left_child, right_child] = join.base.children() else {
        return Err(PlanError::internal("merge join requires two children"));
    };
    let (_, desc) = property.all_same_order();
    for order in &join.left_properties {
        let offsets = order
            .iter()
            .map_while(|column| {
                left.iter()
                    .position(|key| key.unique_id == column.unique_id)
            })
            .collect::<Vec<_>>();
        if offsets.len() < left.len() || left.is_empty() {
            continue;
        }
        let left_keys = order[..offsets.len()].to_vec();
        let right_keys = offsets
            .iter()
            .map(|&offset| right[offset].clone())
            .collect::<Vec<_>>();
        let prefix = join
            .right_properties
            .iter()
            .map(|order| {
                order
                    .iter()
                    .zip(&right_keys)
                    .take_while(|(column, key)| column.unique_id == key.unique_id)
                    .count()
            })
            .max()
            .unwrap_or(0);
        if prefix < offsets.len() || prefix == 0 || !compatible_collations(&left_keys, &right_keys)
        {
            continue;
        }
        let Some(mut requirements) = child_requirements(
            join.join_type,
            property,
            &left_keys,
            &right_keys,
            &constants,
        ) else {
            continue;
        };
        if property.expected_cnt < stats.row_count() {
            result.uses_ordering_index_selectivity |= !property.is_sort_item_empty();
            for (requirement, child) in requirements.iter_mut().zip([left_child, right_child]) {
                let rows = child
                    .stats_info()
                    .ok_or_else(|| {
                        PlanError::internal("merge join child statistics have not been derived")
                    })?
                    .row_count();
                requirement.expected_cnt = child_expected_count(
                    property,
                    rows,
                    stats.row_count(),
                    settings.ordering_index_selectivity_ratio,
                );
            }
        }
        let other = residuals(join, &offsets);
        result.plans.push(candidate(
            join,
            property,
            allocator,
            skew_ratio,
            left_keys,
            right_keys,
            offsets.iter().map(|&offset| null_eq[offset]).collect(),
            other,
            requirements,
            desc,
        )?);
    }
    if join.prefer_join_type & hint::NO_MERGE_JOIN != 0 {
        if join.prefer_join_type & hint::MERGE_JOIN == 0 {
            result.plans.clear();
            return Ok(result);
        }
        warnings.push(
            "Some MERGE_JOIN and NO_MERGE_JOIN hints conflict, NO_MERGE_JOIN is ignored".into(),
        );
    }
    if join.prefer_join_type & (hint::MERGE_JOIN | hint::NO_HASH_JOIN) != 0 || disable_hash_join {
        if let Some(plan) = enforced(
            join, property, allocator, skew_ratio, &left, &right, &null_eq,
        )? {
            result.plans.push(plan);
        }
    }
    Ok(result)
}

fn compatible_collations(left: &[Column], right: &[Column]) -> bool {
    left.iter()
        .zip(right)
        .all(|(left, right)| match (&left.ret_type, &right.ret_type) {
            (Some(left), Some(right))
                if left.eval_type() == EvalType::String
                    && right.eval_type() == EvalType::String =>
            {
                left.charset_name() == right.charset_name()
                    && left.collation_name() == right.collation_name()
            }
            _ => true,
        })
}

/// Go isSortPropCompatibleWithJoinKeys skips only FD-proven constants.
pub fn compatible_order(items: &[SortItem], keys: &[Column], constants: &ColSet) -> bool {
    let mut position = 0;
    for item in items {
        while position < keys.len()
            && keys[position].unique_id != item.col
            && constants.has(keys[position].unique_id)
        {
            position += 1;
        }
        if position == keys.len() || keys[position].unique_id != item.col {
            return false;
        }
        position += 1;
    }
    true
}
fn child_requirements(
    kind: LogicalJoinType,
    property: &PhysicalProperty,
    left: &[Column],
    right: &[Column],
    constants: &ColSet,
) -> Option<[PhysicalProperty; 2]> {
    let (same, desc) = property.all_same_order();
    if !property.is_sort_item_empty() {
        if !same {
            return None;
        }
        let l = compatible_order(&property.sort_items, left, constants);
        let r = compatible_order(&property.sort_items, right, constants);
        if (!l && !r)
            || (r && kind == LogicalJoinType::LeftOuter)
            || (l && kind == LogicalJoinType::RightOuter)
        {
            return None;
        }
    }
    let mut result = [
        requirement(left, desc, false),
        requirement(right, desc, false),
    ];
    for item in &mut result {
        item.cte_producer_status = property.cte_producer_status;
        item.no_cop_push_down = property.no_cop_push_down;
    }
    Some(result)
}
fn requirement(keys: &[Column], desc: bool, enforced: bool) -> PhysicalProperty {
    PhysicalProperty::new(
        TaskType::Root,
        &keys.iter().map(|key| key.unique_id).collect::<Vec<_>>(),
        desc,
        f64::MAX,
        enforced,
    )
}
fn residuals(join: &LogicalJoin, offsets: &[usize]) -> Vec<Expression> {
    let mut result = join.other_conditions.clone();
    result.extend(
        join.equal_conditions
            .iter()
            .enumerate()
            .filter(|(index, _)| !offsets.contains(index))
            .map(|(_, expr)| Expression::ScalarFunction(expr.clone())),
    );
    result
}
fn candidate(
    join: &LogicalJoin,
    property: &PhysicalProperty,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
    left: Vec<Column>,
    right: Vec<Column>,
    null_eq: Vec<bool>,
    other: Vec<Expression>,
    requirements: [PhysicalProperty; 2],
    desc: bool,
) -> Result<PhysicalPlan, PlanError> {
    let stats = join
        .base
        .base
        .stats_info()
        .ok_or_else(|| PlanError::internal("merge join statistics have not been derived"))?;
    let mut base =
        BasePhysicalPlan::new(allocator, "MergeJoin", join.base.base.query_block_offset());
    base.base.set_schema(join.base.base.schema().cloned());
    base.base.set_stats(Some(
        stats.scale_by_expect_cnt(property.expected_cnt, skew_ratio),
    ));
    base.set_children_req_props(requirements.into_iter().map(Some).collect());
    Ok(PhysicalPlan::MergeJoin(PhysicalMergeJoin {
        join: BasePhysicalJoin {
            base,
            join_type: join.join_type,
            inner_child_idx: 0,
            left_conditions: join.left_conditions.clone(),
            right_conditions: join.right_conditions.clone(),
            other_conditions: other,
            left_join_keys: left,
            right_join_keys: right,
            is_null_eq: null_eq,
            default_values: join.default_values.clone(),
            ..Default::default()
        },
        desc,
    }))
}
fn enforced(
    join: &LogicalJoin,
    property: &PhysicalProperty,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
    left: &[Column],
    right: &[Column],
    null_eq: &[bool],
) -> Result<Option<PhysicalPlan>, PlanError> {
    let (same, desc) = property.all_same_order();
    if !same {
        return Ok(None);
    }
    let mut offsets = Vec::new();
    for item in &property.sort_items {
        let Some(index) = left
            .iter()
            .zip(right)
            .position(|(left, right)| left.unique_id == item.col || right.unique_id == item.col)
        else {
            return Ok(None);
        };
        if (join.join_type == LogicalJoinType::LeftOuter && right[index].unique_id == item.col)
            || (join.join_type == LogicalJoinType::RightOuter && left[index].unique_id == item.col)
        {
            return Ok(None);
        }
        if !offsets.contains(&index) {
            offsets.push(index);
        }
    }
    for index in 0..left.len() {
        if !offsets.contains(&index) {
            offsets.push(index);
        }
    }
    let mut l = offsets
        .iter()
        .map(|&index| left[index].clone())
        .collect::<Vec<_>>();
    let mut r = offsets
        .iter()
        .map(|&index| right[index].clone())
        .collect::<Vec<_>>();
    let mut nulls = offsets
        .iter()
        .map(|&index| null_eq[index])
        .collect::<Vec<_>>();
    let other = if compatible_collations(&l, &r) {
        join.other_conditions.clone()
    } else {
        l.clear();
        r.clear();
        nulls.clear();
        residuals(join, &[])
    };
    let mut requirements = [requirement(&l, desc, true), requirement(&r, desc, true)];
    for item in &mut requirements {
        item.no_cop_push_down = property.no_cop_push_down;
    }
    candidate(
        join,
        property,
        allocator,
        skew_ratio,
        l,
        r,
        nulls,
        other,
        requirements,
        desc,
    )
    .map(Some)
}

/// Go CalcChildExpectedCnt; the ordering penalty is absent for unordered output.
pub fn child_expected_count(
    property: &PhysicalProperty,
    child: f64,
    estimated: f64,
    ordering_ratio: f64,
) -> f64 {
    let ordered = !property.is_sort_item_empty();
    let ratio = if ordered { ordering_ratio } else { 0.0 };
    if property.expected_cnt < estimated
        || (ordered
            && ratio > 0.0
            && child > estimated
            && property.expected_cnt < child
            && estimated > 0.0)
    {
        let penalty = if ordered && ratio > 0.0 {
            ((child - estimated) * ratio).max(0.0)
        } else {
            0.0
        };
        child * (property.expected_cnt / estimated) + penalty
    } else {
        f64::MAX
    }
}
