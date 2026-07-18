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

//! Dependency-closed physical-plan metadata.
//!
//! TiDB's Go `base.Plan` is the hand-off between optimization and execution:
//! an executor needs the operator type, stable plan ID, query-block offset,
//! estimated rows, and child relationships before it can choose a concrete
//! execution implementation.  The full Go plan also owns expressions,
//! schemas, session context, cost, and storage protobuf conversion.  Those
//! owners are not in the seed workspace, so this module carries only the
//! source-shaped metadata contract and does not pretend to execute a plan.

/// A physical-plan node's source-visible metadata and owned child tree.
///
/// This mirrors the dependency-closed portion of `baseimpl.Plan` and
/// `BasePhysicalPlan`.  `operator` intentionally remains a string: Go's
/// operator catalog is generated and evolves independently of this seed
/// crate, so inventing a Rust enum here would silently create a second source
/// of truth.  Unsupported operators can therefore remain explicit at the
/// eventual executor dispatch boundary.
#[derive(Clone, Debug, PartialEq)]
pub struct PlanNode {
    operator: String,
    id: i32,
    query_block_offset: i32,
    estimated_rows: Option<f64>,
    children: Vec<PlanNode>,
}

impl PlanNode {
    /// Creates a node with no estimated-row statistic.
    #[must_use]
    pub fn new(operator: impl Into<String>, id: i32, query_block_offset: i32) -> Self {
        Self {
            operator: operator.into(),
            id,
            query_block_offset,
            estimated_rows: None,
            children: Vec::new(),
        }
    }

    /// Sets the optional `StatsInfo.RowCount` value and returns the node.
    #[must_use]
    pub fn with_estimated_rows(mut self, estimated_rows: f64) -> Self {
        self.estimated_rows = Some(estimated_rows);
        self
    }

    /// Sets the owned child plans and returns the node.
    #[must_use]
    pub fn with_children(mut self, children: impl IntoIterator<Item = PlanNode>) -> Self {
        self.children = children.into_iter().collect();
        self
    }

    /// Returns the source operator type (`Plan.TP`).
    #[must_use]
    pub fn operator(&self) -> &str {
        &self.operator
    }

    /// Returns the source plan ID (`Plan.ID`).
    #[must_use]
    pub const fn id(&self) -> i32 {
        self.id
    }

    /// Returns the source query-block offset.
    #[must_use]
    pub const fn query_block_offset(&self) -> i32 {
        self.query_block_offset
    }

    /// Returns the optional estimated row count.
    #[must_use]
    pub const fn estimated_rows(&self) -> Option<f64> {
        self.estimated_rows
    }

    /// Returns the children in executor order.
    #[must_use]
    pub fn children(&self) -> &[PlanNode] {
        &self.children
    }

    /// Formats the source `ExplainID` contract.
    ///
    /// Go omits the numeric suffix when `StmtCtx.IgnoreExplainIDSuffix` is
    /// enabled; otherwise it appends `_` and the plan ID.  This method does
    /// not add operator-specific formatting or normalize a digest.
    #[must_use]
    pub fn explain_id(&self, ignore_suffix: bool) -> String {
        if ignore_suffix {
            self.operator.clone()
        } else {
            format!("{}_{}", self.operator, self.id)
        }
    }

    /// Produces a pre-order metadata view for executor dispatch and tests.
    ///
    /// The view deliberately omits child ownership and expressions.  It is a
    /// read-only selector, not a replacement for the full physical plan.
    #[must_use]
    pub fn metadata_preorder(&self) -> Vec<PlanNodeMetadata> {
        let mut result = Vec::new();
        self.visit_preorder(&mut |node| result.push(node.metadata()));
        result
    }

    fn visit_preorder(&self, visitor: &mut impl FnMut(&Self)) {
        visitor(self);
        for child in &self.children {
            child.visit_preorder(visitor);
        }
    }

    fn metadata(&self) -> PlanNodeMetadata {
        PlanNodeMetadata {
            operator: self.operator.clone(),
            id: self.id,
            query_block_offset: self.query_block_offset,
            estimated_rows: self.estimated_rows,
            child_count: self.children.len(),
        }
    }
}

/// Read-only metadata used by the planner→executor dispatch seam.
#[derive(Clone, Debug, PartialEq)]
pub struct PlanNodeMetadata {
    /// Source operator type (`Plan.TP`).
    pub operator: String,
    /// Source plan ID (`Plan.ID`).
    pub id: i32,
    /// Source query-block offset.
    pub query_block_offset: i32,
    /// Source estimated row count, when statistics have been derived.
    pub estimated_rows: Option<f64>,
    /// Number of direct children in the physical plan.
    pub child_count: usize,
}
