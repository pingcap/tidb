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

//! Dependency-closed vectors for cascades rule-set filtering.
//!
//! The Go anchor is `TestAppliedRuleSet` at
//! `pkg/planner/cascades/old/optimize_test.go:212`.

use tidb_planner::rule_set::{OperandRules, RuleMask};

#[test]
fn applied_rule_mask_keeps_source_order() {
    let mask = RuleMask::from_ids(&[0, 65]);
    assert_eq!(mask.filter(&[65, 2, 0]), vec![65, 0]);
}

#[test]
fn intermediate_apply_uses_only_special_rules() {
    let rules = OperandRules::new(vec![1, 2], vec![7]);
    assert_eq!(rules.filter(true), &[7]);
    assert_eq!(rules.filter(false), &[1, 2]);
}
