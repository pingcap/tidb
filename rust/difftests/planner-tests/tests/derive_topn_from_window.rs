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
//! `pkg/planner/core/rule_derive_topn_from_window.go`.
//!
//! The source integration anchor is `TestDerivedTopNSuite` at
//! `pkg/planner/core/casetest/rule/rule_derive_topn_from_window_test.go:30`.
//! These tests isolate the wrapper callback, stable name, and unchanged flag;
//! SQL plan construction, window semantics, and MPP storage placement remain
//! external.

use tidb_planner::derive_topn_from_window::{DeriveTopNFromWindow, DeriveTopNPlan};

#[derive(Debug, Eq, PartialEq)]
struct MockPlan {
    callback_count: u8,
}

impl DeriveTopNPlan for MockPlan {
    type Output = Self;

    fn derive_top_n(mut self) -> Self::Output {
        self.callback_count += 1;
        self
    }
}

#[test]
fn source_rule_calls_derive_callback_and_keeps_false_change_flag() {
    let (plan, changed) = DeriveTopNFromWindow.optimize(MockPlan { callback_count: 0 });
    assert_eq!(plan, MockPlan { callback_count: 1 });
    assert!(!changed);
}

#[test]
fn source_rule_name_is_stable() {
    assert_eq!(DeriveTopNFromWindow.name(), "derive_topn_from_window");
}
