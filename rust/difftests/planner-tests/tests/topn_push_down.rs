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

//! Dependency-closed vectors for `pkg/planner/core/rule_topn_push_down.go`.
//!
//! The source integration anchor is `TestTopNPushDown` at
//! `pkg/planner/core/logical_plans_test.go:1640`. These tests isolate the
//! wrapper's callback, nil parent, stable name, and unchanged flag; SQL plan
//! construction and TopN placement remain external.

use tidb_planner::topn_push_down::{PushDownTopNOptimizer, TopNPushDownPlan};

#[derive(Debug, Eq, PartialEq)]
struct MockPlan {
    callback_count: u8,
    received_parent: bool,
}

impl TopNPushDownPlan for MockPlan {
    type Output = Self;

    fn push_down_top_n(mut self, top_n: Option<Self::Output>) -> Self::Output {
        self.callback_count += 1;
        self.received_parent = top_n.is_some();
        self
    }
}

#[test]
fn source_rule_calls_plan_with_nil_parent_and_false_change_flag() {
    let (plan, changed) = PushDownTopNOptimizer.optimize(MockPlan {
        callback_count: 0,
        received_parent: true,
    });
    assert_eq!(
        plan,
        MockPlan {
            callback_count: 1,
            received_parent: false,
        }
    );
    assert!(!changed);
}

#[test]
fn source_rule_name_is_stable() {
    assert_eq!(PushDownTopNOptimizer.name(), "topn_push_down");
}
