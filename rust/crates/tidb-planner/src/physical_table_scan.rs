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

//! Dependency-closed physical TiKV table scan.

use crate::{plan::PlanNode, scan_pushdown::TiKvTableScanSpec};

/// The source plan-codec type assigned by `PhysicalTableScan.Init`.
pub const PLAN_TYPE: &str = "TableScan";

/// A physical table scan carrying its pre-resolved TiKV pushdown payload.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalTableScanPlan {
    plan: PlanNode,
    pushdown: TiKvTableScanSpec,
}

impl PhysicalTableScanPlan {
    /// Creates a physical table scan ready for bounded DAG lowering.
    #[must_use]
    pub fn init(plan_id: i32, query_block_offset: i32, pushdown: TiKvTableScanSpec) -> Self {
        Self {
            plan: PlanNode::new(PLAN_TYPE, plan_id, query_block_offset),
            pushdown,
        }
    }

    /// Returns physical-plan metadata.
    #[must_use]
    pub const fn plan(&self) -> &PlanNode {
        &self.plan
    }

    /// Returns the pre-resolved TiKV scan payload.
    #[must_use]
    pub const fn pushdown(&self) -> &TiKvTableScanSpec {
        &self.pushdown
    }
}
