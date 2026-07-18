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

use crate::{
    access_path::{
        ResolvedTableDescriptor, ResolvedTableScanKind, TableScanExplainIdSuffix,
        ValidatedTablePushdown,
    },
    plan::PlanNode,
    scan_pushdown::TiKvTableScanSpec,
};

/// The source plan-codec type assigned by `PhysicalTableScan.Init`.
pub const PLAN_TYPE: &str = "TableScan";

/// A physical table scan carrying its pre-resolved TiKV pushdown payload.
#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalTableScanPlan {
    plan: PlanNode,
    pushdown: TiKvTableScanSpec,
    descriptor: Option<ResolvedTableDescriptor>,
}

// `PhysicalTableScanPlan` can contain only no row estimate or a finite
// nonnegative estimate admitted by `try_with_source_estimated_rows`; therefore
// its `PartialEq` remains reflexive even though `PlanNode` generally permits
// arbitrary `f64` estimates.
impl Eq for PhysicalTableScanPlan {}

impl PhysicalTableScanPlan {
    /// Creates a raw physical table scan ready for bounded DAG lowering.
    ///
    /// DAG construction needs only the pre-resolved protobuf payload. This
    /// constructor deliberately does not invent the planner-only table
    /// descriptor required by TableReader conversion.
    #[must_use]
    pub fn init(plan_id: i32, query_block_offset: i32, pushdown: TiKvTableScanSpec) -> Self {
        Self {
            plan: PlanNode::new(PLAN_TYPE, plan_id, query_block_offset),
            pushdown,
            descriptor: None,
        }
    }

    /// Creates the planner-owned scan after access-path descriptor validation.
    #[must_use]
    pub(crate) fn from_validated_pushdown(
        plan_id: i32,
        query_block_offset: i32,
        pushdown: ValidatedTablePushdown,
    ) -> Self {
        Self {
            plan: PlanNode::new(PLAN_TYPE, plan_id, query_block_offset),
            descriptor: Some(pushdown.descriptor()),
            pushdown: pushdown.spec().clone(),
        }
    }

    /// Attaches source `CountAfterAccess` after validating the table-task
    /// boundary.
    pub(crate) fn try_with_source_estimated_rows(mut self, rows: f64) -> Option<Self> {
        if !rows.is_finite() || rows < 0.0 {
            return None;
        }
        self.plan = self.plan.with_estimated_rows(rows);
        Some(self)
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

    /// Returns the source-authoritative table descriptor retained by the
    /// physical scan.
    #[must_use]
    pub const fn descriptor(&self) -> Option<ResolvedTableDescriptor> {
        self.descriptor
    }

    /// Returns the source-resolved full/range scan kind.
    #[must_use]
    pub const fn scan_kind(&self) -> Option<ResolvedTableScanKind> {
        match self.descriptor {
            Some(descriptor) => Some(descriptor.scan_kind()),
            None => None,
        }
    }

    /// Returns Go's bounded `PhysicalTableScan.ExplainID` result.
    #[must_use]
    pub fn explain_id(&self) -> Option<String> {
        let descriptor = self.descriptor?;
        let plan_type = descriptor.scan_kind().plan_type();
        Some(match descriptor.explain_id_suffix() {
            TableScanExplainIdSuffix::IncludePlanId => {
                format!("{plan_type}_{}", self.plan.id())
            }
            TableScanExplainIdSuffix::Omit => plan_type.to_owned(),
        })
    }

    /// Returns whether the source table uses a common handle.
    #[must_use]
    pub const fn is_common_handle(&self) -> Option<bool> {
        match self.descriptor {
            Some(descriptor) => Some(descriptor.is_common_handle()),
            None => None,
        }
    }

    /// Returns source `CountAfterAccess` attached by the table-task builder.
    #[must_use]
    pub const fn estimated_rows(&self) -> Option<f64> {
        self.plan.estimated_rows()
    }
}
