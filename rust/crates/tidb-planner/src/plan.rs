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

//! An EXPLAIN-only metadata view of a plan. NOT the plan representation.
//!
//! DEMOTED. This module predates the plan tree and is no longer a candidate
//! for it: [`crate::logical::LogicalPlan`] and [`crate::physical::PhysicalPlan`]
//! are the plan representation, over [`crate::plan_base::BasePlan`]. Two
//! rival trees would be two sources of truth, so this one is narrowed to what
//! it is actually used for — the flat, string-typed row set that an
//! `EXPLAIN` renderer and a metadata-dispatch seam consume.
//!
//! Its `operator: String` is the tell: a plan node whose type is a string
//! cannot be matched on, which is exactly why the real tree is a closed enum.
//!
//! NEW CODE SHOULD NOT BUILD ON THIS. It is retained because
//! `difftests/planner-tests/tests/plan.rs` pins its metadata pre-order
//! output; a follow-up batch that rewrites that difftest against
//! [`crate::physical::PhysicalPlan::walk_preorder`] can delete this module
//! outright. Nothing in `crates/` reads it. (`tidb-executor` has its own,
//! unrelated, crate-private `plan_trace::PlanNode`.)

/// A flat metadata row set for EXPLAIN rendering, with its own child tree.
///
/// This is NOT `baseimpl.Plan`; see the module header. The plan tree's base
/// is [`crate::plan_base::BasePlan`], whose `tp` carries the same operator
/// name that `operator` does here.
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
