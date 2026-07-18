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
//! `pkg/planner/core/rule_resolve_grouping_expand.go`.
//!
//! The Go integration anchor is `TestRollupExpand` at
//! `pkg/planner/core/logical_plans_test.go:2473`.  These tests isolate the
//! source post-order traversal and append-style level generation; grouping-set
//! expressions, schema/GID construction, SQL output, and optimizer errors
//! remain external.

use tidb_planner::resolve_grouping_expand::{ExpandNodeKind, ExpandPlan, ResolveExpand};

#[test]
fn visits_nested_children_before_generating_expand_levels() {
    let input = ExpandPlan::other_with_children(vec![ExpandPlan::expand_with_generated(
        3,
        0,
        vec![ExpandPlan::expand(2)],
    )]);

    let (rewritten, changed) = ResolveExpand.optimize(input);
    assert!(!changed);

    let outer = &rewritten.children()[0];
    assert_eq!(
        outer.kind(),
        &ExpandNodeKind::Expand {
            grouping_set_count: 3,
            generated_level_count: 3,
        }
    );
    assert_eq!(outer.children()[0].generated_level_count(), Some(2));
}

#[test]
fn appends_levels_to_existing_generation_state() {
    let input = ExpandPlan::expand_with_generated(4, 1, Vec::new());
    let (rewritten, changed) = ResolveExpand.optimize(input);

    assert!(!changed);
    assert_eq!(rewritten.generated_level_count(), Some(5));
}

#[test]
fn leaves_non_expand_nodes_and_child_order_unchanged() {
    let input = ExpandPlan::other_with_children(vec![
        ExpandPlan::other(),
        ExpandPlan::expand(0),
        ExpandPlan::other(),
    ]);
    let (rewritten, changed) = ResolveExpand.optimize(input);

    assert!(!changed);
    assert!(matches!(rewritten.kind(), ExpandNodeKind::Other));
    assert_eq!(rewritten.children().len(), 3);
    assert!(matches!(
        rewritten.children()[0].kind(),
        ExpandNodeKind::Other
    ));
    assert_eq!(rewritten.children()[1].generated_level_count(), Some(0));
    assert!(matches!(
        rewritten.children()[2].kind(),
        ExpandNodeKind::Other
    ));
}

#[test]
fn source_rule_name_is_stable() {
    assert_eq!(ResolveExpand.name(), "resolve_expand");
}
