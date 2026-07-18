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
//! `pkg/planner/core/rule_eliminate_unionall_dual_item.go`.
//!
//! The Go integration anchor is `TestUnion` at
//! `pkg/planner/core/logical_plans_test.go:1599`.  These vectors isolate the
//! source branch filtering, schema-preserving empty-union replacement, and
//! recursive changed-flag behavior; logical operator construction, SQL plan
//! output, and executor integration remain external.

use tidb_planner::eliminate_unionall_dual_item::{
    EliminateUnionAllDualItem, UnionAllNodeKind, UnionAllPlan,
};

fn dual(row_count: i32) -> UnionAllPlan {
    UnionAllPlan::new(UnionAllNodeKind::TableDual { row_count })
}

fn projection(child: UnionAllPlan) -> UnionAllPlan {
    UnionAllPlan::with_children(UnionAllNodeKind::Projection, vec![child])
}

#[test]
fn removes_direct_and_projected_zero_row_duals_before_recursing() {
    let input = UnionAllPlan::with_children(
        UnionAllNodeKind::Other,
        vec![UnionAllPlan::with_schema(
            UnionAllNodeKind::UnionAll,
            vec![11, 22],
            vec![dual(0), projection(dual(0)), dual(1)],
        )],
    );

    let (rewritten, changed) = EliminateUnionAllDualItem.optimize(input);
    assert!(changed);

    let union = &rewritten.children()[0];
    assert_eq!(union.kind(), &UnionAllNodeKind::UnionAll);
    assert_eq!(union.schema(), &[11, 22]);
    assert_eq!(union.children(), &[dual(1)]);
}

#[test]
fn replaces_all_zero_row_union_branches_with_schema_preserving_dual() {
    let input = UnionAllPlan::with_schema(
        UnionAllNodeKind::UnionAll,
        vec![7, 9],
        vec![dual(0), projection(dual(0))],
    );

    let (rewritten, changed) = EliminateUnionAllDualItem.optimize(input);
    assert!(changed);
    assert_eq!(
        rewritten.kind(),
        &UnionAllNodeKind::TableDual { row_count: 0 }
    );
    assert_eq!(rewritten.schema(), &[7, 9]);
    assert!(rewritten.children().is_empty());
}

#[test]
fn recursive_rewrite_preserves_nonzero_duals_and_safe_empty_projections() {
    let input = UnionAllPlan::with_children(
        UnionAllNodeKind::Other,
        vec![
            projection(UnionAllPlan::new(UnionAllNodeKind::Other)),
            UnionAllPlan::with_children(
                UnionAllNodeKind::UnionAll,
                vec![dual(1), projection(dual(0))],
            ),
        ],
    );

    let (rewritten, changed) = EliminateUnionAllDualItem.optimize(input);
    assert!(changed);
    assert_eq!(rewritten.children()[0].children().len(), 1);
    let union = &rewritten.children()[1];
    assert_eq!(union.kind(), &UnionAllNodeKind::UnionAll);
    assert_eq!(union.children(), &[dual(1)]);
}

#[test]
fn source_rule_name_is_stable() {
    assert_eq!(
        EliminateUnionAllDualItem.name(),
        "union_all_eliminate_dual_item"
    );
}
