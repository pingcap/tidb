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
//! `pkg/planner/cascades/rule/rule_type.go`.
//!
//! The Go rule package has no direct `rule_type_test.go`; the surrounding
//! cascades rule tests exercise the type through rule construction. These
//! vectors pin the source `iota` sequence and its exact String fallback while
//! keeping the rule/memo runtime boundary explicit.

use tidb_planner::rule_type::RuleType;

#[test]
fn source_iota_values_round_trip() {
    let known = [
        RuleType::DefaultNone,
        RuleType::XfJoinToApply,
        RuleType::XfDeCorrelateSimpleApply,
        RuleType::XfPullCorrPredFromProj,
        RuleType::XfPullCorrPredFromSel,
        RuleType::XfPullCorrPredFromDs,
        RuleType::XfPullCorrPredFromSort,
        RuleType::XfPullCorrPredFromLimit,
        RuleType::XfPullCorrPredFromMax1Row,
        RuleType::XfPullCorrPredFromAgg1,
        RuleType::XfPullCorrPredFromAgg2,
        RuleType::XfMaximumRuleLength,
    ];
    for (raw, rule) in known.into_iter().enumerate() {
        let raw = raw as i32;
        assert_eq!(RuleType::from_raw(raw), rule);
        assert_eq!(rule.raw(), raw);
    }
}

#[test]
fn source_string_labels_preserve_join_special_case_and_fallback() {
    assert_eq!(RuleType::XfJoinToApply.as_str(), "join_to_apply");
    assert_eq!(RuleType::XfJoinToApply.to_string(), "join_to_apply");

    for raw in [0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, -1, 99] {
        let rule = RuleType::from_raw(raw);
        assert_eq!(rule.as_str(), "default_none");
        assert_eq!(rule.to_string(), "default_none");
        assert_eq!(rule.raw(), raw);
    }
}
