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

//! Focused source-shaped tests for ranger CNF/DNF detachment.

use tidb_planner::range_detacher::{
    detach_cnf_predicates, detach_dnf_predicates, AccessDecision, RangeAtom, RangeAtomKind,
    RangePredicate,
};

fn atom(identity: u32, decision: AccessDecision) -> RangePredicate {
    RangePredicate::atom(RangeAtom::new(
        identity,
        RangeAtomKind::Comparison,
        decision,
    ))
}

#[test]
fn cnf_keeps_filter_only_and_reserved_access_conditions() {
    let access = atom(1, AccessDecision::access());
    let filter = atom(2, AccessDecision::filter());
    let approximate = atom(3, AccessDecision::access_and_reserve());

    let result = detach_cnf_predicates(&[access.clone(), filter.clone(), approximate.clone()]);
    assert_eq!(result.access_conditions(), &[access, approximate.clone()]);
    assert_eq!(result.filter_conditions(), &[filter, approximate]);
}

#[test]
fn dnf_detaches_each_and_branch_and_marks_any_residual() {
    let left_access = atom(1, AccessDecision::access());
    let left_filter = atom(2, AccessDecision::filter());
    let right_access = atom(3, AccessDecision::access());
    let right_reserved = atom(4, AccessDecision::access_and_reserve());
    let condition = RangePredicate::or([
        RangePredicate::and([left_access.clone(), left_filter.clone()]),
        RangePredicate::and([right_access.clone(), right_reserved.clone()]),
    ]);

    let result = detach_cnf_predicates(std::slice::from_ref(&condition));
    assert_eq!(
        result.access_conditions(),
        &[RangePredicate::or([
            left_access.clone(),
            RangePredicate::and([right_access.clone(), right_reserved.clone()]),
        ])]
    );
    assert_eq!(result.filter_conditions(), &[condition]);

    let dnf = detach_dnf_predicates(&[
        RangePredicate::and([left_access, left_filter]),
        RangePredicate::and([right_access, right_reserved]),
    ]);
    assert!(dnf.has_residual());
}

#[test]
fn dnf_with_unusable_branch_cannot_supply_partial_access() {
    let access = atom(1, AccessDecision::access());
    let filter = atom(2, AccessDecision::filter());
    let result = detach_dnf_predicates(&[access, RangePredicate::and([filter])]);
    assert!(result.access_conditions().is_empty());
    assert!(result.has_residual());
}

#[test]
fn nested_boolean_nodes_use_checker_decision_as_a_whole() {
    let left = atom(1, AccessDecision::access());
    let right = atom(2, AccessDecision::access_and_reserve());
    let nested = RangePredicate::and([left.clone(), right.clone()]);
    let result = detach_cnf_predicates(std::slice::from_ref(&nested));
    assert_eq!(result.access_conditions(), std::slice::from_ref(&nested));
    assert_eq!(result.filter_conditions(), std::slice::from_ref(&nested));

    let denied = RangePredicate::and([left, atom(3, AccessDecision::filter())]);
    let result = detach_cnf_predicates(std::slice::from_ref(&denied));
    assert!(result.access_conditions().is_empty());
    assert_eq!(result.filter_conditions(), std::slice::from_ref(&denied));
}
