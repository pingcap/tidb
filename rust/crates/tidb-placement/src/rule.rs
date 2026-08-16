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

//! Go `rule.go`: building placement rules from a constraint string.

use std::sync::LazyLock;

use regex::Regex;

use crate::constraints::{
    new_constraints, new_constraints_from_yaml, pre_check_dict_constraint_str,
};
use crate::errors::{PlacementError, PlacementErrorKind};
use crate::pd::{LabelConstraint, PeerRoleType, Rule};
use crate::yaml_lite::unmarshal_strict_string_int_map;

/// Go `attributePrefix`.
pub(crate) const ATTRIBUTE_PREFIX: &str = "#";
/// Go `attributeEvictLeader`: used to evict the leader from a store.
pub(crate) const ATTRIBUTE_EVICT_LEADER: &str = "evict-leader";

/// Go `RuleBuilder`: builds the rules from a constraint string.
#[derive(Clone, Debug, Default)]
pub struct RuleBuilder {
    role: PeerRoleType,
    replicas_num: u64,
    skip_check_replicas_consistent: bool,
    constraint_str: String,
}

impl RuleBuilder {
    /// Go `NewRuleBuilder`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Go `SetRole`: sets the role of the rule.
    #[must_use]
    pub fn set_role(mut self, role: PeerRoleType) -> Self {
        self.role = role;
        self
    }

    /// Go `SetReplicasNum`: sets the replicas number in the rule.
    #[must_use]
    pub const fn set_replicas_num(mut self, num: u64) -> Self {
        self.replicas_num = num;
        self
    }

    /// Go `SetSkipCheckReplicasConsistent`.
    #[must_use]
    pub const fn set_skip_check_replicas_consistent(mut self, skip: bool) -> Self {
        self.skip_check_replicas_consistent = skip;
        self
    }

    /// Go `SetConstraintStr`: sets the constraint string.
    #[must_use]
    pub fn set_constraint_str(mut self, constraint_str: &str) -> Self {
        self.constraint_str = constraint_str.to_owned();
        self
    }

    /// Go `BuildRulesWithDictConstraintsOnly`: constructs the rules from a
    /// yaml-compatible representation of 'dict' constraints.
    ///
    /// # Errors
    ///
    /// Propagates [`new_rules_with_dict_constraints`].
    pub fn build_rules_with_dict_constraints_only(&self) -> Result<Vec<Rule>, PlacementError> {
        new_rules_with_dict_constraints(&self.role, &self.constraint_str)
    }

    /// Go `BuildRules`: constructs the rules from a yaml-compatible
    /// representation of 'array' or 'dict' constraints.
    ///
    /// Refer to
    /// <https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-24-placement-rules-in-sql.md>.
    ///
    /// # Errors
    ///
    /// Propagates [`new_rules`], and returns `ErrInvalidConstraintsReplicas`
    /// when the dict constraints' replica counts do not add up to the
    /// requested number.
    pub fn build_rules(&self) -> Result<Vec<Rule>, PlacementError> {
        let rules = new_rules(&self.role, self.replicas_num, &self.constraint_str)?;
        // Check if replicas is consistent.
        if self.skip_check_replicas_consistent {
            return Ok(rules);
        }
        let total_cnt: i64 = rules.iter().map(|rule| rule.count).sum();
        if self.replicas_num != 0 && self.replicas_num != total_cnt as u64 {
            return Err(PlacementError::wrap(
                PlacementErrorKind::InvalidConstraintsReplicas,
                format!(
                    "count of replicas in dict constrains is {total_cnt}, but got {}",
                    self.replicas_num
                ),
            ));
        }
        Ok(rules)
    }
}

/// Go `NewRule`: constructs a rule from role, count, and constraints. It is
/// here to make the behavior of creating new rules consistent.
#[must_use]
pub fn new_rule(role: PeerRoleType, replicas: u64, constraints: Vec<LabelConstraint>) -> Rule {
    Rule {
        role,
        count: replicas as i64,
        label_constraints: constraints,
        ..Rule::default()
    }
}

/// Go `wrongSeparatorRegexp`.
static WRONG_SEPARATOR_REGEXP: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r#"[^"':]+:\d"#).expect("the Go source's literal pattern is valid RE2")
});

/// Go `getYamlMapFormatError`.
fn get_yaml_map_format_error(input: &str) -> Option<PlacementError> {
    if !input.contains(':') {
        return Some(PlacementError::new(
            PlacementErrorKind::InvalidConstraintsMappingNoColonFound,
        ));
    }
    if WRONG_SEPARATOR_REGEXP.is_match(input) {
        return Some(PlacementError::new(
            PlacementErrorKind::InvalidConstraintsMappingWrongSeparator,
        ));
    }
    None
}

/// Go `newRules`: constructs the rules from a yaml-compatible representation of
/// 'array' or 'dict' constraints.
///
/// Refer to
/// <https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-24-placement-rules-in-sql.md>.
///
/// # Errors
///
/// Returns `ErrInvalidConstraintsReplicas` for a zero replica count with a
/// non-empty constraint, `ErrInvalidConstraintsFormat` when the string is
/// neither an array nor a dict, and otherwise propagates the dict path.
pub(crate) fn new_rules(
    role: &PeerRoleType,
    replicas: u64,
    constraint_str: &str,
) -> Result<Vec<Rule>, PlacementError> {
    let constraint_bytes = constraint_str.as_bytes();
    match new_constraints_from_yaml(constraint_bytes) {
        Ok(constraints) => {
            if replicas == 0 {
                if !constraint_str.is_empty() {
                    return Err(PlacementError::wrap(
                        PlacementErrorKind::InvalidConstraintsReplicas,
                        format!(
                            "count of replicas should be positive, but got {replicas}, constraint {constraint_str}"
                        ),
                    ));
                }
                return Ok(Vec::new());
            }
            Ok(vec![new_rule(role.clone(), replicas, constraints)])
        }
        Err(array_err) => {
            // Check if it is dict constraints.
            match unmarshal_strict_string_int_map(constraint_bytes) {
                Err(map_err) => Err(PlacementError::wrap(
                    PlacementErrorKind::InvalidConstraintsFormat,
                    format!(
                        "should be [constraint1, ...] (error {array_err}), {{constraint1: cnt1, ...}} (error {map_err}), or any yaml compatible representation"
                    ),
                )),
                Ok(_) => new_rules_with_dict_constraints(role, constraint_str),
            }
        }
    }
}

/// Go `newRulesWithDictConstraints`: constructs the rules from a
/// yaml-compatible representation of 'dict' constraints.
///
/// # Errors
///
/// Returns `ErrInvalidConstraintsFormat` when the string is not a dict, the
/// mapping-separator errors for a malformed `key: count` pair,
/// `ErrInvalidConstraintsMapcnt` for a non-positive count, and otherwise
/// propagates [`new_constraints`].
pub(crate) fn new_rules_with_dict_constraints(
    role: &PeerRoleType,
    constraint_str: &str,
) -> Result<Vec<Rule>, PlacementError> {
    let mut rules = Vec::new();
    let constraints = unmarshal_strict_string_int_map(constraint_str.as_bytes()).map_err(
        |map_err| {
            PlacementError::wrap(
                PlacementErrorKind::InvalidConstraintsFormat,
                format!(
                    "should be [constraint1, ...] or {{constraint1: cnt1, ...}}, error {map_err}, or any yaml compatible representation"
                ),
            )
        },
    )?;

    for (labels, cnt) in &constraints {
        if *cnt <= 0 {
            if let Some(err) = get_yaml_map_format_error(constraint_str) {
                return Err(err);
            }
            return Err(PlacementError::wrap(
                PlacementErrorKind::InvalidConstraintsMapcnt,
                format!("count of labels '{labels}' should be positive, but got {cnt}"),
            ));
        }
    }

    for (labels, cnt) in constraints {
        let (lbs, override_role) = pre_check_dict_constraint_str(&labels, role)?;
        let label_constraints = new_constraints(&lbs)?;
        if cnt == 0 {
            return Err(PlacementError::wrap(
                PlacementErrorKind::InvalidConstraintsReplicas,
                format!("count of replicas should be positive, but got {cnt}"),
            ));
        }
        rules.push(new_rule(override_role, cnt as u64, label_constraints));
    }
    Ok(rules)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{new_rule, new_rules};
    use crate::constraint::new_constraint_direct;
    use crate::constraints::new_constraints_direct;
    use crate::errors::PlacementErrorKind;
    use crate::pd::{LabelConstraintOp, PeerRoleType, Rule};

    /// Go `matchRules` (`rule_test.go`): an order-insensitive rule-set
    /// comparison, shared with the bundle tests.
    pub(crate) fn match_rules(expected: &[Rule], got: &[Rule], prefix: &str) {
        assert_eq!(got.len(), expected.len(), "{prefix}");
        for (index, rule) in expected.iter().enumerate() {
            assert!(
                got.contains(rule),
                "{prefix}\n\ncan not found {index} rule\n{rule:?}\n{got:?}"
            );
        }
    }

    /// Go `TestClone` (`rule_test.go`).
    #[test]
    fn test_clone() {
        let rule = Rule {
            id: "434".to_owned(),
            ..Rule::default()
        };
        let mut new_rule_value = rule.clone_rule();
        new_rule_value.id = "121".to_owned();

        assert_eq!(
            Rule {
                id: "434".to_owned(),
                ..Rule::default()
            },
            rule
        );
        assert_eq!(
            Rule {
                id: "121".to_owned(),
                ..Rule::default()
            },
            new_rule_value
        );
    }

    /// Go `TestNewRuleAndNewRules` (`rule_test.go`).
    #[test]
    fn test_new_rule_and_new_rules() {
        struct TestCase {
            name: &'static str,
            input: &'static str,
            replicas: u64,
            output: Vec<Rule>,
            err: Option<PlacementErrorKind>,
        }
        let mut tests = Vec::new();

        tests.push(TestCase {
            name: "empty constraints",
            input: "",
            replicas: 3,
            output: vec![new_rule(
                PeerRoleType::VOTER,
                3,
                new_constraints_direct(vec![]),
            )],
            err: None,
        });

        tests.push(TestCase {
            name: "zero replicas",
            input: "",
            replicas: 0,
            output: Vec::new(),
            err: None,
        });

        tests.push(TestCase {
            name: "normal list constraints",
            input: r#"["+zone=sh", "+region=sh"]"#,
            replicas: 3,
            output: vec![new_rule(
                PeerRoleType::VOTER,
                3,
                new_constraints_direct(vec![
                    new_constraint_direct("zone", LabelConstraintOp::IN, &["sh"]),
                    new_constraint_direct("region", LabelConstraintOp::IN, &["sh"]),
                ]),
            )],
            err: None,
        });

        tests.push(TestCase {
            name: "normal dict constraints",
            input: r#"{"+zone=sh,-zone=bj":2, "+zone=sh": 1}"#,
            replicas: 0,
            output: vec![
                new_rule(
                    PeerRoleType::VOTER,
                    2,
                    new_constraints_direct(vec![
                        new_constraint_direct("zone", LabelConstraintOp::IN, &["sh"]),
                        new_constraint_direct("zone", LabelConstraintOp::NOT_IN, &["bj"]),
                    ]),
                ),
                new_rule(
                    PeerRoleType::VOTER,
                    1,
                    new_constraints_direct(vec![new_constraint_direct(
                        "zone",
                        LabelConstraintOp::IN,
                        &["sh"],
                    )]),
                ),
            ],
            err: None,
        });

        tests.push(TestCase {
            name: "normal dict constraints, with count",
            input: "{'+zone=sh,-zone=bj':2, '+zone=sh': 1}",
            replicas: 0,
            output: vec![
                new_rule(
                    PeerRoleType::VOTER,
                    2,
                    new_constraints_direct(vec![
                        new_constraint_direct("zone", LabelConstraintOp::IN, &["sh"]),
                        new_constraint_direct("zone", LabelConstraintOp::NOT_IN, &["bj"]),
                    ]),
                ),
                new_rule(
                    PeerRoleType::VOTER,
                    1,
                    new_constraints_direct(vec![new_constraint_direct(
                        "zone",
                        LabelConstraintOp::IN,
                        &["sh"],
                    )]),
                ),
            ],
            err: None,
        });

        tests.push(TestCase {
            name: "zero count in dict constraints",
            input: r#"{"+zone=sh,-zone=bj":0, "+zone=sh": 1}"#,
            replicas: 0,
            output: Vec::new(),
            err: Some(PlacementErrorKind::InvalidConstraintsMapcnt),
        });

        tests.push(TestCase {
            name: "invalid list constraints",
            input: r#"["ne=sh", "+zone=sh"]"#,
            replicas: 3,
            output: Vec::new(),
            err: Some(PlacementErrorKind::InvalidConstraintsFormat),
        });

        tests.push(TestCase {
            name: "invalid dict constraints",
            input: r#"{+ne=sh,-zone=bj:1, "+zone=sh": 4"#,
            replicas: 0,
            output: Vec::new(),
            err: Some(PlacementErrorKind::InvalidConstraintsFormat),
        });

        tests.push(TestCase {
            name: "invalid dict constraints",
            input: r#"{"nesh,-zone=bj":1, "+zone=sh": 4}"#,
            replicas: 0,
            output: Vec::new(),
            err: Some(PlacementErrorKind::InvalidConstraintFormat),
        });

        tests.push(TestCase {
            name: "invalid dict separator",
            input: "{+region=us-east-2:2}",
            replicas: 0,
            output: Vec::new(),
            err: Some(PlacementErrorKind::InvalidConstraintsMappingWrongSeparator),
        });

        tests.push(TestCase {
            name: "normal dict constraint with evict leader attribute",
            input: r#"{"+zone=sh,-zone=bj":2, "+zone=sh,#evict-leader": 1}"#,
            replicas: 0,
            output: vec![
                new_rule(
                    PeerRoleType::VOTER,
                    2,
                    new_constraints_direct(vec![
                        new_constraint_direct("zone", LabelConstraintOp::IN, &["sh"]),
                        new_constraint_direct("zone", LabelConstraintOp::NOT_IN, &["bj"]),
                    ]),
                ),
                new_rule(
                    PeerRoleType::FOLLOWER,
                    1,
                    new_constraints_direct(vec![new_constraint_direct(
                        "zone",
                        LabelConstraintOp::IN,
                        &["sh"],
                    )]),
                ),
            ],
            err: None,
        });

        tests.push(TestCase {
            name: "invalid constraints with invalid format",
            input: r#"{"+zone=sh,-zone=bj":2, "+zone=sh,evict-leader": 1}"#,
            replicas: 0,
            output: Vec::new(),
            err: Some(PlacementErrorKind::InvalidConstraintFormat),
        });

        tests.push(TestCase {
            name: "invalid constraints with undetermined attribute",
            input: r#"{"+zone=sh,-zone=bj":2, "+zone=sh,#reject-follower": 1}"#,
            replicas: 0,
            output: Vec::new(),
            err: Some(PlacementErrorKind::UnsupportedConstraint),
        });

        for test in tests {
            let comment = format!("[{}]", test.name);
            let result = new_rules(&PeerRoleType::VOTER, test.replicas, test.input);
            match test.err {
                None => match_rules(
                    &test.output,
                    &result.unwrap_or_else(|err| panic!("{comment}: {err}")),
                    &comment,
                ),
                Some(kind) => assert!(
                    result.as_ref().is_err_and(|err| err.is(kind)),
                    "{comment}\n{result:?}\n{}",
                    kind.text()
                ),
            }
        }
    }
}
