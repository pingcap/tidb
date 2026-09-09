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

//! Go `getHashJoins`, `getHashJoin`, and `NewPhysicalHashJoin`. The logical
//! tree owns predicates and statistics; candidates own child requirements.
//! Index/merge/MPP candidate enumeration is separate, not replaced by this list.

use crate::find_best_task::{hash_join_shapes, LogicalJoinType};
use crate::logical::LogicalJoin;
use crate::physical_property::{PhysicalProperty, TaskType};
use crate::plan_base::{PlanError, PlanIdAllocator};
use crate::plan_builder::from::join_hint_flags as hint;

use super::{BasePhysicalJoin, BasePhysicalPlan, PhysicalHashJoin, PhysicalPlan};

/// Current session/runtime inputs read by Go's hash-join enumeration.
#[derive(Clone, Copy, Debug)]
pub struct HashJoinSettings {
    /// `HashJoinConcurrency()`.
    pub concurrency: usize,
    /// `UseHashJoinV2 && IsHashJoinV2Supported()` for the executing runtime.
    pub use_hash_join_v2: bool,
    /// `DisableHashJoin`.
    pub disable_hash_join: bool,
}

impl Default for HashJoinSettings {
    fn default() -> Self {
        // The live Rust runtime remains v1-shaped; the session must pass its
        // current supported version rather than infer it from a plan shape.
        Self {
            concurrency: 5,
            use_hash_join_v2: false,
            disable_hash_join: false,
        }
    }
}

/// Go's `(joins, forced)` return from `getHashJoins`.
pub struct HashJoinCandidates {
    /// Candidates in source order, which breaks equal-cost ties.
    pub plans: Vec<PhysicalPlan>,
    /// A HASH_JOIN or applicable build/probe hint selected this family.
    pub forced: bool,
}

/// Enumerate hash joins without planning, opening or retaining their children.
pub fn get_hash_joins(
    join: &LogicalJoin,
    property: &PhysicalProperty,
    settings: HashJoinSettings,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
    warnings: &mut Vec<String>,
) -> Result<HashJoinCandidates, PlanError> {
    if !property.is_sort_item_empty() {
        return Ok(HashJoinCandidates {
            plans: Vec::new(),
            forced: false,
        });
    }
    let hints = join.prefer_join_type;
    let mut force_left = hints & (hint::LEFT_AS_HJ_BUILD | hint::RIGHT_AS_HJ_PROBE) != 0;
    let mut force_right = hints & (hint::RIGHT_AS_HJ_BUILD | hint::LEFT_AS_HJ_PROBE) != 0;
    if force_left && force_right {
        warnings.push("Conflicting HASH_JOIN_BUILD and HASH_JOIN_PROBE hints detected. Both sides cannot be specified to use the same table. Please review the hints".into());
        force_left = false;
        force_right = false;
    }
    let (left_keys, _, _, has_null_eq) = join.get_join_keys();
    let (left_na_keys, _) = join.get_na_join_keys();
    let semi_outer_build = settings.use_hash_join_v2
        && !left_keys.is_empty()
        && !has_null_eq
        && left_na_keys.is_empty();
    if (force_left || force_right)
        && (matches!(
            join.join_type,
            LogicalJoinType::LeftOuterSemi | LogicalJoinType::AntiLeftOuterSemi
        ) || (matches!(
            join.join_type,
            LogicalJoinType::Semi | LogicalJoinType::AntiSemi
        ) && !semi_outer_build))
    {
        let name = match join.join_type {
            LogicalJoinType::Semi => "semi join",
            LogicalJoinType::AntiSemi => "anti semi join",
            LogicalJoinType::LeftOuterSemi => "left outer semi join",
            LogicalJoinType::AntiLeftOuterSemi => "anti left outer semi join",
            _ => unreachable!(),
        };
        warnings.push(if matches!(join.join_type, LogicalJoinType::Semi | LogicalJoinType::AntiSemi) {
            format!("The HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for {name} with hash join version 1. Please remove these hints")
        } else {
            format!("HASH_JOIN_BUILD and HASH_JOIN_PROBE hints are not supported for {name} because the build side is fixed. Please remove these hints")
        });
        force_left = false;
        force_right = false;
    }
    let forced = hints & hint::HASH_JOIN != 0 || force_left || force_right;
    let skip = settings.disable_hash_join || hints & hint::NO_HASH_JOIN != 0;
    if skip && !forced {
        return Ok(HashJoinCandidates {
            plans: Vec::new(),
            forced: false,
        });
    }
    if skip {
        warnings.push("A conflict between the HASH_JOIN hint and the NO_HASH_JOIN hint, or the tidb_opt_enable_hash_join system variable, the HASH_JOIN hint will take precedence.".into());
    }
    let stats = join
        .base
        .base
        .stats_info()
        .ok_or_else(|| PlanError::internal("hash join statistics have not been derived"))?;
    let children = join.base.children();
    let [left, right] = children else {
        return Err(PlanError::internal(
            "hash join requires two logical children",
        ));
    };
    let child_stats = [left.stats_info(), right.stats_info()];
    let mut plans = Vec::with_capacity(2);
    for shape in hash_join_shapes(join.join_type, force_left, force_right, semi_outer_build) {
        let mut requirements = [PhysicalProperty::default(), PhysicalProperty::default()];
        for requirement in &mut requirements {
            requirement.task_tp = TaskType::Root;
            requirement.expected_cnt = f64::MAX;
            requirement.cte_producer_status = property.cte_producer_status;
            requirement.no_cop_push_down = property.no_cop_push_down;
        }
        let outer = 1 - shape.inner_idx;
        if property.expected_cnt < stats.row_count() {
            let outer_stats = child_stats[outer].ok_or_else(|| {
                PlanError::internal("hash join child statistics have not been derived")
            })?;
            let scale = property.expected_cnt / stats.row_count();
            requirements[outer].expected_cnt = outer_stats.row_count() * scale;
        }
        // Go getHashJoin tries pushing the enclosing lookup requirement to
        // each child separately. The other child's scan remains independent.
        for lookup_child in 0..if property.index_join_prop.is_some() {
            2
        } else {
            1
        } {
            let mut requirements = requirements.clone();
            requirements[lookup_child].index_join_prop = property.index_join_prop.clone();
            let mut base =
                BasePhysicalPlan::new(allocator, "HashJoin", join.base.base.query_block_offset());
            base.base.set_schema(join.base.base.schema().cloned());
            base.base.set_stats(Some(
                stats.scale_by_expect_cnt(property.expected_cnt, skew_ratio),
            ));
            base.set_children_req_props(requirements.into_iter().map(Some).collect());
            plans.push(PhysicalPlan::HashJoin(new_physical_hash_join(
                join,
                base,
                shape.inner_idx,
                shape.use_outer_to_build,
                settings.concurrency,
            )));
        }
    }
    Ok(HashJoinCandidates { plans, forced })
}

/// Go NewPhysicalHashJoin, also embedded by PhysicalApply without hash-family
/// hint admission or ordering rejection.
pub(super) fn new_physical_hash_join(
    join: &LogicalJoin,
    base: BasePhysicalPlan,
    inner_child_idx: usize,
    use_outer_to_build: bool,
    concurrency: usize,
) -> PhysicalHashJoin {
    let (left_join_keys, right_join_keys, is_null_eq, _) = join.get_join_keys();
    let (left_na_join_keys, right_na_join_keys) = join.get_na_join_keys();
    PhysicalHashJoin {
        use_outer_to_build,
        concurrency,
        equal_conditions: join.equal_conditions.clone(),
        na_equal_conditions: join.na_eq_conditions.clone(),
        join: BasePhysicalJoin {
            base,
            join_type: join.join_type,
            inner_child_idx,
            left_conditions: join.left_conditions.clone(),
            right_conditions: join.right_conditions.clone(),
            other_conditions: join.other_conditions.clone(),
            left_join_keys,
            right_join_keys,
            left_na_join_keys,
            right_na_join_keys,
            is_null_eq,
            default_values: join.default_values.clone(),
        },
    }
}
