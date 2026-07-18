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
//! `pkg/planner/core/rule_eliminate_empty_selection.go`.
//!
//! The source integration anchor is `TestEmptySelectionEliminator` at
//! `pkg/planner/core/casetest/rule/rule_eliminate_empty_selection_test.go:23`.
//! These tests isolate the wrapper callback, stable name, and unchanged flag;
//! logical selection tree mutation and SQL testdata remain external.

use tidb_planner::eliminate_empty_selection::{EmptySelectionEliminator, EmptySelectionPlan};

#[derive(Debug, Eq, PartialEq)]
struct MockPlan {
    callback_count: u8,
}

impl EmptySelectionPlan for MockPlan {
    type Output = Self;

    fn eliminate_empty_selections(mut self) -> Self::Output {
        self.callback_count += 1;
        self
    }
}

#[test]
fn source_rule_calls_recursive_plan_callback_and_keeps_false_flag() {
    let (plan, changed) = EmptySelectionEliminator.optimize(MockPlan { callback_count: 0 });
    assert_eq!(plan, MockPlan { callback_count: 1 });
    assert!(!changed);
}

#[test]
fn source_rule_name_is_stable() {
    assert_eq!(EmptySelectionEliminator.name(), "eliminate_empty_selection");
}
