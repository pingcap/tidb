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

//! Dependency-closed vectors for `pkg/planner/memo/expr_iterator.go`.
//!
//! The direct Go anchors are `TestNewExprIterFromGroupElem` at line 29,
//! `TestExprIterNext` at line 77, `TestExprIterReset` at line 130, and
//! `TestExprIterWithEngineType` at line 207 in
//! `pkg/planner/memo/expr_iterator_test.go`.

use tidb_planner::expr_iterator::{
    new_expr_iter_from_group, new_expr_iter_from_group_elem, Group, GroupExpression,
};
use tidb_planner::pattern::{new_pattern, Operand};
use tidb_planner::pattern_engine::{EngineType, EngineTypeSet};

fn group(engine: EngineType, operands: &[Operand]) -> Group {
    let mut value = Group::new(engine);
    for &operand in operands {
        value.insert(GroupExpression::new(operand));
    }
    value
}

fn join_group(left: Group, right: Group) -> Group {
    let mut root = Group::new(EngineType::TiDb);
    root.insert(
        GroupExpression::new(Operand::Join)
            .with_child(left)
            .with_child(right),
    );
    root
}

#[test]
fn source_group_element_constructor_matches_root_and_children() {
    let root = join_group(
        group(
            EngineType::TiDb,
            &[Operand::Selection, Operand::Projection, Operand::Limit],
        ),
        group(
            EngineType::TiDb,
            &[Operand::Limit, Operand::Selection, Operand::Projection],
        ),
    );
    let pattern = new_pattern(Operand::Join, EngineTypeSet::ALL);
    let iter = new_expr_iter_from_group_elem(&root, 0, &pattern).expect("root match");
    assert!(iter.matched());
    assert_eq!(iter.current().expect("current").operand, Operand::Join);
    assert_eq!(iter.current().expect("current").children.len(), 0);
}

#[test]
fn source_next_enumerates_cartesian_child_matches() {
    let root = join_group(
        group(
            EngineType::TiDb,
            &[Operand::Projection, Operand::Limit, Operand::Projection],
        ),
        group(
            EngineType::TiDb,
            &[Operand::Selection, Operand::Limit, Operand::Selection],
        ),
    );
    let pattern = tidb_planner::pattern::build_pattern(
        Operand::Join,
        EngineTypeSet::ALL,
        [
            new_pattern(Operand::Projection, EngineTypeSet::ALL),
            new_pattern(Operand::Selection, EngineTypeSet::ALL),
        ],
    );
    let mut iter = new_expr_iter_from_group(&root, &pattern).expect("join matches");
    assert_eq!(iter.len(), 4);
    let mut count = 0;
    while iter.matched() {
        let current = iter.current().expect("current match");
        assert_eq!(current.children.len(), 2);
        count += 1;
        iter.advance();
    }
    assert_eq!(count, 4);
    assert!(!iter.matched());
}

#[test]
fn source_reset_returns_to_first_match() {
    let root = join_group(
        group(
            EngineType::TiDb,
            &[Operand::Projection, Operand::Projection],
        ),
        group(EngineType::TiDb, &[Operand::Selection]),
    );
    let pattern = tidb_planner::pattern::build_pattern(
        Operand::Join,
        EngineTypeSet::ALL,
        [
            new_pattern(Operand::Projection, EngineTypeSet::ALL),
            new_pattern(Operand::Selection, EngineTypeSet::ALL),
        ],
    );
    let mut iter = new_expr_iter_from_group(&root, &pattern).expect("join matches");
    assert_eq!(iter.len(), 2);
    assert!(iter.advance());
    assert!(iter.matched());
    assert!(!iter.advance());
    assert!(!iter.matched());
    assert!(iter.reset());
    assert!(iter.matched());
}

#[test]
fn source_engine_filters_nested_groups() {
    let mut root = Group::new(EngineType::TiDb);
    let mut tikv = group(EngineType::TiKv, &[Operand::Limit, Operand::Projection]);
    let mut tiflash = group(EngineType::TiFlash, &[Operand::Limit, Operand::Projection]);
    tikv.insert(GroupExpression::new(Operand::Selection));
    tiflash.insert(GroupExpression::new(Operand::Selection));
    root.insert(GroupExpression::new(Operand::TiKvSingleGather).with_child(tikv));
    root.insert(GroupExpression::new(Operand::TiKvSingleGather).with_child(tiflash));

    let pattern = tidb_planner::pattern::build_pattern(
        Operand::TiKvSingleGather,
        EngineTypeSet::TIDB_ONLY,
        [new_pattern(Operand::Limit, EngineTypeSet::TIKV_ONLY)],
    );
    let iter = new_expr_iter_from_group(&root, &pattern).expect("TiKV match");
    assert_eq!(iter.len(), 1);
}
