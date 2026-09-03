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

//! Union-all dual elimination from
//! `pkg/planner/core/rule_eliminate_unionall_dual_item.go`.
//!
//! The Go rule removes zero-row table-dual branches from a `LogicalUnionAll`.
//! It also removes a projection whose first child is a zero-row dual, replaces
//! an empty union with a schema-preserving zero-row dual, and then recursively
//! applies the same operation to the remaining tree.  This module keeps those
//! semantics over a small source-shaped plan adapter; logical operator
//! construction, schemas, and SQL execution remain external planner owners.

/// The source node kinds needed by the union-all dual-elimination rule.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum UnionAllNodeKind {
    /// A logical UNION ALL whose children are concatenated in order.
    UnionAll,
    /// A table-dual node carrying the source row-count boundary.
    TableDual {
        /// The source row-count boundary used to identify an empty dual.
        row_count: i32,
    },
    /// A projection node, which may have a table-dual child.
    Projection,
    /// Any other logical operator.
    Other,
}

/// Minimal owned plan tree used to exercise source rule semantics.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct UnionAllPlan {
    kind: UnionAllNodeKind,
    children: Vec<Self>,
    schema: Vec<u64>,
}

impl UnionAllPlan {
    /// Creates a node with an empty source schema.
    #[must_use]
    pub fn new(kind: UnionAllNodeKind) -> Self {
        Self {
            kind,
            children: Vec::new(),
            schema: Vec::new(),
        }
    }

    /// Creates a node with children and an empty source schema.
    #[must_use]
    pub fn with_children(kind: UnionAllNodeKind, children: Vec<Self>) -> Self {
        Self {
            kind,
            children,
            schema: Vec::new(),
        }
    }

    /// Creates a node with an explicit schema and children.
    #[must_use]
    pub fn with_schema(kind: UnionAllNodeKind, schema: Vec<u64>, children: Vec<Self>) -> Self {
        Self {
            kind,
            children,
            schema,
        }
    }

    /// Returns this node's source kind.
    #[must_use]
    pub fn kind(&self) -> &UnionAllNodeKind {
        &self.kind
    }

    /// Returns this node's ordered children.
    #[must_use]
    pub fn children(&self) -> &[Self] {
        &self.children
    }

    /// Returns this node's source schema identity.
    #[must_use]
    pub fn schema(&self) -> &[u64] {
        &self.schema
    }
}

/// Source-shaped logical optimizer for `union_all_eliminate_dual_item`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct EliminateUnionAllDualItem;

impl EliminateUnionAllDualItem {
    /// Applies recursive union-all dual elimination and reports whether any
    /// branch was removed or an empty union was replaced.
    #[must_use]
    pub fn optimize(self, plan: UnionAllPlan) -> (UnionAllPlan, bool) {
        eliminate_union_all_dual_item(plan)
    }

    /// Returns the source rule registry name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        "union_all_eliminate_dual_item"
    }
}

/// Applies the source recursive rewrite over an owned structural plan.
#[must_use]
pub fn eliminate_union_all_dual_item(mut plan: UnionAllPlan) -> (UnionAllPlan, bool) {
    let mut changed = false;

    if matches!(plan.kind, UnionAllNodeKind::UnionAll) {
        let mut retained = Vec::with_capacity(plan.children.len());
        for child in plan.children.drain(..) {
            if is_zero_row_dual(&child) || is_projection_over_zero_row_dual(&child) {
                changed = true;
            } else {
                retained.push(child);
            }
        }

        if retained.is_empty() {
            let schema = plan.schema;
            return (
                UnionAllPlan::with_schema(
                    UnionAllNodeKind::TableDual { row_count: 0 },
                    schema,
                    Vec::new(),
                ),
                true,
            );
        }
        plan.children = retained;
    }

    let mut rewritten_children = Vec::with_capacity(plan.children.len());
    for child in plan.children.drain(..) {
        let (rewritten, child_changed) = eliminate_union_all_dual_item(child);
        changed |= child_changed;
        rewritten_children.push(rewritten);
    }
    plan.children = rewritten_children;
    (plan, changed)
}

fn is_zero_row_dual(plan: &UnionAllPlan) -> bool {
    matches!(plan.kind, UnionAllNodeKind::TableDual { row_count: 0 })
}

fn is_projection_over_zero_row_dual(plan: &UnionAllPlan) -> bool {
    matches!(plan.kind, UnionAllNodeKind::Projection)
        && plan.children.first().is_some_and(is_zero_row_dual)
}
