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

//! Dependency-closed vectors for
//! `pkg/planner/core/rule_push_down_sequence.go`.
//!
//! The direct Go regression anchor is
//! `pkg/planner/core/issuetest/panicrisk_tier2_test.go:60
//! TestPushDownSequenceWithTableDual`. These structural vectors cover nested
//! sequence merging, DataSource/CTE push-through, unary descent, multi-child
//! and childless attachment, and the source false change flag without
//! fabricating logical operators or SQL execution.

use tidb_planner::push_down_sequence::{PushDownSequenceSolver, SequenceNodeKind, SequencePlan};

fn leaf(kind: SequenceNodeKind) -> SequencePlan {
    SequencePlan::new(kind)
}

fn sequence(children: impl IntoIterator<Item = SequencePlan>) -> SequencePlan {
    SequencePlan::with_children(SequenceNodeKind::Sequence, children)
}

fn operator(children: impl IntoIterator<Item = SequencePlan>) -> SequencePlan {
    SequencePlan::with_children(SequenceNodeKind::Operator, children)
}

#[test]
fn nested_sequences_merge_ctes_and_push_through_data_source() {
    let plan = sequence([
        leaf(SequenceNodeKind::Cte),
        sequence([
            leaf(SequenceNodeKind::Cte),
            leaf(SequenceNodeKind::DataSource),
        ]),
    ]);
    let result = tidb_planner::push_down_sequence::push_down_sequence(plan);
    assert_eq!(result.kind(), SequenceNodeKind::Sequence);
    assert_eq!(result.children().len(), 3);
    assert_eq!(result.children()[0].kind(), SequenceNodeKind::Cte);
    assert_eq!(result.children()[1].kind(), SequenceNodeKind::Cte);
    assert_eq!(result.children()[2].kind(), SequenceNodeKind::DataSource);
}

#[test]
fn unary_descent_and_multi_child_attachment_preserve_sequence_boundary() {
    let plan = sequence([
        leaf(SequenceNodeKind::Cte),
        operator([leaf(SequenceNodeKind::Operator)]),
    ]);
    let result = tidb_planner::push_down_sequence::push_down_sequence(plan);
    assert_eq!(result.kind(), SequenceNodeKind::Operator);
    assert_eq!(result.children().len(), 1);
    assert_eq!(result.children()[0].kind(), SequenceNodeKind::Sequence);

    let plan = sequence([
        leaf(SequenceNodeKind::Cte),
        operator([
            leaf(SequenceNodeKind::Operator),
            leaf(SequenceNodeKind::Operator),
        ]),
    ]);
    let result = tidb_planner::push_down_sequence::push_down_sequence(plan);
    assert_eq!(result.kind(), SequenceNodeKind::Sequence);
    assert_eq!(result.children().len(), 2);
    assert_eq!(result.children()[1].kind(), SequenceNodeKind::Operator);
    assert_eq!(result.children()[1].children().len(), 2);
}

#[test]
fn childless_operator_is_attached_without_indexing_a_missing_child() {
    let plan = sequence([
        leaf(SequenceNodeKind::Cte),
        leaf(SequenceNodeKind::Operator),
    ]);
    let (result, changed) = PushDownSequenceSolver.optimize(plan);
    assert_eq!(result.kind(), SequenceNodeKind::Sequence);
    assert_eq!(result.children().len(), 2);
    assert_eq!(result.children()[1].kind(), SequenceNodeKind::Operator);
    assert!(!changed);
}

#[test]
fn source_rule_name_is_stable() {
    assert_eq!(PushDownSequenceSolver.name(), "push_down_sequence");
}
