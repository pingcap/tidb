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

//! Sequence push-down traversal from `pkg/planner/core/rule_push_down_sequence.go`.
//!
//! The source walks logical plans, merges nested Sequence CTE lists, pushes a
//! Sequence through DataSource/CTE and unary operators, and attaches it above
//! multi-child or childless operators. `SequencePlan` is a dependency-closed
//! structural adapter; real logical operators, session metadata, and CTE
//! execution remain external planner owners.

/// Source node categories needed by the sequence traversal.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum SequenceNodeKind {
    /// A LogicalSequence containing CTE children and a main-query child.
    Sequence,
    /// A DataSource leaf through which the sequence can be pushed.
    DataSource,
    /// A LogicalCTE leaf through which the sequence can be pushed.
    Cte,
    /// Any other logical operator; child count controls traversal behavior.
    Operator,
}

/// Minimal owned plan tree used to exercise source sequence traversal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SequencePlan {
    kind: SequenceNodeKind,
    children: Vec<Self>,
}

impl SequencePlan {
    /// Creates a node with no children.
    #[must_use]
    pub const fn new(kind: SequenceNodeKind) -> Self {
        Self {
            kind,
            children: Vec::new(),
        }
    }

    /// Creates a node with the supplied source-ordered children.
    #[must_use]
    pub fn with_children(kind: SequenceNodeKind, children: impl IntoIterator<Item = Self>) -> Self {
        Self {
            kind,
            children: children.into_iter().collect(),
        }
    }

    /// Returns this node's source category.
    #[must_use]
    pub const fn kind(&self) -> SequenceNodeKind {
        self.kind
    }

    /// Returns children in source order.
    #[must_use]
    pub fn children(&self) -> &[Self] {
        &self.children
    }
}

/// Source-shaped logical optimization rule for pushing down sequences.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct PushDownSequenceSolver;

impl PushDownSequenceSolver {
    /// Traverses a structural plan tree and reports the source false
    /// direct-change flag.
    #[must_use]
    pub fn optimize(&self, plan: SequencePlan) -> (SequencePlan, bool) {
        (push_down_sequence(plan), false)
    }

    /// Returns the source rule registry name.
    #[must_use]
    pub const fn name(self) -> &'static str {
        "push_down_sequence"
    }
}

/// Applies the source recursive sequence traversal to a structural plan.
#[must_use]
pub fn push_down_sequence(plan: SequencePlan) -> SequencePlan {
    recursive_optimize(None, plan)
}

fn recursive_optimize(
    pushed_sequence: Option<SequencePlan>,
    mut plan: SequencePlan,
) -> SequencePlan {
    if plan.kind != SequenceNodeKind::Sequence && pushed_sequence.is_none() {
        let children = std::mem::take(&mut plan.children);
        plan.children = children
            .into_iter()
            .map(|child| recursive_optimize(None, child))
            .collect();
        return plan;
    }

    match plan.kind {
        SequenceNodeKind::Sequence => {
            assert!(
                !plan.children.is_empty(),
                "source LogicalSequence must retain its main-query child"
            );
            let child_len = plan.children.len();
            let main_query = plan.children[child_len - 1].clone();
            match pushed_sequence {
                None => recursive_optimize(Some(plan), main_query),
                Some(pushed) => {
                    assert!(
                        !pushed.children.is_empty(),
                        "pushed source LogicalSequence must retain its main-query child"
                    );
                    let pushed_len = pushed.children.len();
                    let mut all_children = pushed.children[..pushed_len - 1].to_vec();
                    all_children.extend_from_slice(&plan.children[..child_len - 1]);
                    all_children.push(main_query.clone());
                    let merged =
                        SequencePlan::with_children(SequenceNodeKind::Sequence, all_children);
                    recursive_optimize(Some(merged), main_query)
                }
            }
        }
        SequenceNodeKind::DataSource | SequenceNodeKind::Cte => {
            let mut pushed = pushed_sequence.expect("leaf traversal requires a pushed sequence");
            let last = pushed.children.len() - 1;
            pushed.children[last] = recursive_optimize(None, plan);
            pushed
        }
        SequenceNodeKind::Operator => {
            let child_len = plan.children.len();
            let mut pushed =
                pushed_sequence.expect("operator traversal requires a pushed sequence");
            let last = pushed.children.len() - 1;
            if child_len != 1 {
                pushed.children[last] = plan;
                pushed
            } else {
                let child = plan.children[0].clone();
                plan.children[0] = recursive_optimize(Some(pushed), child);
                plan
            }
        }
    }
}
