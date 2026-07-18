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

//! Resolve-expand traversal from
//! `pkg/planner/core/rule_resolve_grouping_expand.go`.
//!
//! The Go rule performs a post-order walk and invokes
//! `LogicalExpand.GenLevelProjections` only after all children have been
//! visited.  This module keeps that ordering and the source's append-style
//! level-generation count over a structural adapter; real grouping-set
//! expressions, schemas, GID/GPos columns, and planner errors remain external.

/// Structural node kinds needed by the resolve-expand traversal.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ExpandNodeKind {
    /// A LogicalExpand with caller-owned grouping-set and generated-level
    /// counts.
    Expand {
        /// Number of grouping sets supplied by the caller.
        grouping_set_count: usize,
        /// Number of generated levels already appended to the node.
        generated_level_count: usize,
    },
    /// Any non-expand logical operator.
    Other,
}

/// Minimal owned plan tree for source-shaped post-order traversal.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExpandPlan {
    kind: ExpandNodeKind,
    children: Vec<Self>,
}

impl ExpandPlan {
    /// Creates a non-expand leaf.
    #[must_use]
    pub fn other() -> Self {
        Self {
            kind: ExpandNodeKind::Other,
            children: Vec::new(),
        }
    }

    /// Creates an expand leaf with no generated level projections yet.
    #[must_use]
    pub fn expand(grouping_set_count: usize) -> Self {
        Self::expand_with_generated(grouping_set_count, 0, Vec::new())
    }

    /// Creates an expand node with caller-owned generated-level state and
    /// children.
    #[must_use]
    pub fn expand_with_generated(
        grouping_set_count: usize,
        generated_level_count: usize,
        children: Vec<Self>,
    ) -> Self {
        Self {
            kind: ExpandNodeKind::Expand {
                grouping_set_count,
                generated_level_count,
            },
            children,
        }
    }

    /// Creates a non-expand node with ordered children.
    #[must_use]
    pub fn other_with_children(children: Vec<Self>) -> Self {
        Self {
            kind: ExpandNodeKind::Other,
            children,
        }
    }

    /// Returns the node kind.
    #[must_use]
    pub fn kind(&self) -> &ExpandNodeKind {
        &self.kind
    }

    /// Returns ordered children after traversal.
    #[must_use]
    pub fn children(&self) -> &[Self] {
        &self.children
    }

    /// Returns the current generated-level count for an expand node.
    #[must_use]
    pub fn generated_level_count(&self) -> Option<usize> {
        match self.kind {
            ExpandNodeKind::Expand {
                generated_level_count,
                ..
            } => Some(generated_level_count),
            ExpandNodeKind::Other => None,
        }
    }
}

/// Source-shaped resolve-expand logical optimization rule.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct ResolveExpand;

impl ResolveExpand {
    /// Performs the source post-order traversal.  The source wrapper reports
    /// no direct plan-change flag even though expand metadata is populated.
    #[must_use]
    pub fn optimize(self, plan: ExpandPlan) -> (ExpandPlan, bool) {
        (resolve_expand(plan), false)
    }

    /// Returns the source rule registry name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        "resolve_expand"
    }
}

/// Recursively visits children first and then generates levels on Expand.
#[must_use]
pub fn resolve_expand(mut plan: ExpandPlan) -> ExpandPlan {
    let mut rewritten_children = Vec::with_capacity(plan.children.len());
    for child in plan.children.drain(..) {
        rewritten_children.push(resolve_expand(child));
    }
    plan.children = rewritten_children;

    if let ExpandNodeKind::Expand {
        grouping_set_count,
        ref mut generated_level_count,
    } = plan.kind
    {
        *generated_level_count += grouping_set_count;
    }
    plan
}
