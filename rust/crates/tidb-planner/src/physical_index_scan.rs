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

//! Live PhysicalIndexScan statistics and cost hand-off from
//! `pkg/planner/core/operator/physicalop/physical_index_scan.go`.
//!
//! The source physical scan receives the already-derived index row count,
//! feeds it into the index-scan cost formula, and then reaches task comparison
//! through `find_best_task.go`.  This bounded bridge makes that path a real
//! planner `PlanNode`, rather than leaving the cardinality/cost result as a
//! standalone model.  Range construction, physical properties, task
//! attachment, and all non-index alternatives remain external owners.

use crate::{
    cardinality::index_range_policy::IndexRangeShape,
    cardinality::live_index_optimizer::{live_index_choice, LiveIndexCandidate, LiveIndexChoice},
    plan::PlanNode,
    scan_pushdown::{
        IndexPushdownMetadataError, ResolvedIndexDescriptor, TiKvIndexScanSpec,
        ValidatedIndexPushdown,
    },
};

/// The source plan-codec type assigned by `PhysicalIndexScan.Init`.
pub const PLAN_TYPE: &str = "IndexScan";

/// A physical index scan whose statistics and cost are ready for task choice.
///
/// `PlanNode` is the existing planner-to-executor metadata hand-off. Its
/// estimated rows are the source-provided `AccessPath.CountAfterAccess` (or
/// an exact upstream `ExpectedCnt` adjustment) used by the cost choice.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalIndexScanPlan {
    plan: PlanNode,
    choice: LiveIndexChoice,
    ranges: Vec<IndexRangeShape>,
    pushdown: Option<ValidatedIndexPushdown>,
}

impl PhysicalIndexScanPlan {
    /// Builds the source physical index scan from its resolved cost candidate
    /// and precomputed source row count.
    #[must_use]
    pub fn init(
        plan_id: i32,
        query_block_offset: i32,
        candidate: &LiveIndexCandidate,
        count_after_access: f64,
    ) -> Self {
        let choice = live_index_choice(candidate, count_after_access);
        Self {
            plan: PlanNode::new(PLAN_TYPE, plan_id, query_block_offset)
                .with_estimated_rows(choice.rows),
            choice,
            ranges: candidate.ranges.clone(),
            pushdown: None,
        }
    }

    /// Validates and attaches schema-resolved TiKV data for directly built
    /// physical plans.
    pub fn try_with_pushdown(
        mut self,
        descriptor: ResolvedIndexDescriptor,
        pushdown: TiKvIndexScanSpec,
    ) -> Result<Self, IndexPushdownMetadataError> {
        let validated = ValidatedIndexPushdown::new(self.index_id(), descriptor, pushdown)?;
        self.pushdown = Some(validated);
        Ok(self)
    }

    /// Attaches metadata already validated by `IndexAccessPath`.
    pub(crate) fn with_validated_pushdown(mut self, pushdown: ValidatedIndexPushdown) -> Self {
        self.pushdown = Some(pushdown);
        self
    }

    /// Returns the planner-to-executor physical-plan metadata hand-off.
    #[must_use]
    pub const fn plan(&self) -> &PlanNode {
        &self.plan
    }

    /// Returns the source index identity used by the deterministic tie-breaker.
    #[must_use]
    pub const fn index_id(&self) -> i64 {
        self.choice.index_id
    }

    /// Returns the derived physical scan row count.
    #[must_use]
    pub const fn estimated_rows(&self) -> f64 {
        self.choice.rows
    }

    /// Returns the source index scan cost, including its index-ID tie-breaker.
    #[must_use]
    pub const fn cost(&self) -> f64 {
        self.choice.cost
    }

    /// Returns normalized source range endpoints used by `checkCoverIndex`.
    #[must_use]
    pub fn ranges(&self) -> &[IndexRangeShape] {
        &self.ranges
    }

    /// Returns the pre-resolved TiKV payload, if the schema owner supplied it.
    #[must_use]
    pub const fn pushdown(&self) -> Option<&TiKvIndexScanSpec> {
        match self.pushdown.as_ref() {
            Some(pushdown) => Some(pushdown.spec()),
            None => None,
        }
    }

    /// Keeps the current scan unless the challenger has strictly lower cost.
    ///
    /// This is the bounded index branch of `compareTaskCost`: equal costs
    /// retain the existing task, including when callers intentionally provide
    /// candidates with the same source index ID.
    #[must_use]
    pub fn choose_lower_cost(current: Self, challenger: Self) -> Self {
        if challenger.cost() < current.cost() {
            challenger
        } else {
            current
        }
    }
}
