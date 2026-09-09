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

//! Go exhaustPhysicalPlans4LogicalApply, before optimizeByShuffle chooses workers.
use super::hash_join::new_physical_hash_join;
use super::merge_join::child_expected_count;
use super::{BasePhysicalPlan, PhysicalApply, PhysicalPlan};
use crate::{
    logical::LogicalApply,
    physical_property::{PhysicalProperty, TaskType},
    plan_base::{PlanError, PlanIdAllocator},
};

/// Statement settings read by Apply candidate construction.
#[derive(Clone, Copy, Debug)]
pub struct ApplySettings {
    /// Go MemQuotaApplyCache.
    pub cache_capacity: i64,
    /// Go RiskGroupNDVSkewRatio, used by the canonical NDV estimator.
    pub group_ndv_skew_ratio: f64,
}
impl Default for ApplySettings {
    fn default() -> Self {
        Self {
            cache_capacity: tidb_vardef::defaults::DEF_TIDB_MEM_QUOTA_APPLY_CACHE,
            group_ndv_skew_ratio: 0.0,
        }
    }
}
/// Candidate plus Go's RecordRelevantOptVar dependency from child costing.
pub struct ApplyCandidates {
    pub plans: Vec<PhysicalPlan>,
    pub uses_ordering_index_selectivity: bool,
}
/// Apply preserves outer order; the inner plan is reopened for outer bindings.
pub fn get_apply(
    apply: &LogicalApply,
    property: &PhysicalProperty,
    settings: ApplySettings,
    ordering_ratio: f64,
    hash_concurrency: usize,
    allocator: &PlanIdAllocator,
    skew_ratio: f64,
) -> Result<ApplyCandidates, PlanError> {
    let [outer, _inner] = apply.join.base.children() else {
        return Err(PlanError::internal("Apply requires two children"));
    };
    let schema = outer
        .schema()
        .ok_or_else(|| PlanError::internal("Apply outer schema is missing"))?;
    let mut result = ApplyCandidates {
        plans: vec![],
        uses_ordering_index_selectivity: false,
    };
    if property.task_tp == TaskType::Mpp
        || property
            .sort_items
            .iter()
            .any(|item| !schema.columns.iter().any(|col| col.unique_id == item.col))
    {
        return Ok(result);
    }
    let stats = apply
        .join
        .base
        .base
        .stats_info()
        .ok_or_else(|| PlanError::internal("Apply statistics have not been derived"))?;
    let mut cache_hit_ratio = 0.0;
    if stats.row_count() != 0.0 {
        let ids = apply
            .cor_cols
            .iter()
            .map(|col| col.column.unique_id)
            .collect::<Vec<_>>();
        let ndvs = stats
            .col_ndvs()
            .iter()
            .map(|(id, ndv)| (*id, *ndv))
            .collect::<Vec<_>>();
        let (ndv, _) = crate::cardinality::ndv::estimate_cols_ndv_with_matched_len(
            &ids,
            &ndvs,
            stats.row_count(),
            stats.group_ndvs(),
            settings.group_ndv_skew_ratio,
        );
        cache_hit_ratio = 1.0 - ndv / stats.row_count();
    }
    let outer_expected = if property.is_sort_item_empty() {
        f64::MAX
    } else {
        result.uses_ordering_index_selectivity = true;
        let outer_rows = outer
            .stats_info()
            .ok_or_else(|| PlanError::internal("Apply outer statistics have not been derived"))?
            .row_count();
        child_expected_count(property, outer_rows, stats.row_count(), ordering_ratio)
    };
    let mut outer_prop = PhysicalProperty::default();
    outer_prop.sort_items = property.sort_items.clone();
    outer_prop.expected_cnt = outer_expected;
    outer_prop.cte_producer_status = property.cte_producer_status;
    outer_prop.no_cop_push_down = true;
    let mut inner_prop = PhysicalProperty::default();
    inner_prop.expected_cnt = f64::MAX;
    inner_prop.cte_producer_status = property.cte_producer_status;
    inner_prop.no_cop_push_down = property.no_cop_push_down;
    let mut base = BasePhysicalPlan::new(
        allocator,
        "Apply",
        apply.join.base.base.query_block_offset(),
    );
    base.base.set_schema(apply.join.base.base.schema().cloned());
    base.base.set_stats(Some(
        stats.scale_by_expect_cnt(property.expected_cnt, skew_ratio),
    ));
    base.set_children_req_props(vec![Some(outer_prop), Some(inner_prop)]);
    result.plans.push(PhysicalPlan::Apply(PhysicalApply {
        hash_join: new_physical_hash_join(&apply.join, base, 1, false, hash_concurrency),
        can_use_cache: cache_hit_ratio > 0.1 && settings.cache_capacity > 0,
        outer_schema: apply.cor_cols.clone(),
        no_decorrelate: apply.no_decorrelate,
        ..Default::default()
    }));
    Ok(result)
}
