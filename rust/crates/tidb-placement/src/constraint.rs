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

//! Go `constraint.go`: one `{+|-}key=value` label constraint.

use crate::common::{ENGINE_LABEL_KEY, ENGINE_LABEL_TIFLASH};
use crate::errors::{PlacementError, PlacementErrorKind};
use crate::pd::{LabelConstraint, LabelConstraintOp};

/// Go `NewConstraint`: creates a constraint from a string.
///
/// # Errors
///
/// Returns `ErrInvalidConstraintFormat` when the label is not
/// `{+|-}key=value`, and `ErrUnsupportedConstraint` for `+engine=tiflash`,
/// which TiDB refuses to schedule through a placement rule.
pub fn new_constraint(label: &str) -> Result<LabelConstraint, PlacementError> {
    let invalid = || PlacementError::wrap(PlacementErrorKind::InvalidConstraintFormat, label);

    if label.len() < 4 {
        return Err(invalid());
    }

    let op = match label.as_bytes()[0] {
        b'+' => LabelConstraintOp::IN,
        b'-' => LabelConstraintOp::NOT_IN,
        _ => return Err(invalid()),
    };

    let key_value: Vec<&str> = label[1..].split('=').collect();
    if key_value.len() != 2 {
        return Err(invalid());
    }

    let key = key_value[0].trim();
    if key.is_empty() {
        return Err(invalid());
    }

    let value = key_value[1].trim();
    if value.is_empty() {
        return Err(invalid());
    }

    // Does not allow adding a rule of tiflash.
    if op == LabelConstraintOp::IN
        && key == ENGINE_LABEL_KEY
        && value.to_lowercase() == ENGINE_LABEL_TIFLASH
    {
        return Err(PlacementError::wrap(
            PlacementErrorKind::UnsupportedConstraint,
            label,
        ));
    }

    Ok(LabelConstraint {
        key: key.to_owned(),
        op,
        values: value.split(',').map(str::to_owned).collect(),
    })
}

/// Go `NewConstraintDirect`: creates a constraint from its parts directly.
#[must_use]
pub fn new_constraint_direct(key: &str, op: LabelConstraintOp, values: &[&str]) -> LabelConstraint {
    LabelConstraint {
        key: key.to_owned(),
        op,
        values: values.iter().copied().map(str::to_owned).collect(),
    }
}

/// Go `RestoreConstraint`: converts a constraint back to its source string.
///
/// # Errors
///
/// Returns `ErrInvalidConstraintFormat` unless the constraint holds exactly one
/// value and an operation that has a `+`/`-` spelling.
pub fn restore_constraint(constraint: &LabelConstraint) -> Result<String, PlacementError> {
    let mut builder = String::new();
    if constraint.values.len() != 1 {
        return Err(PlacementError::wrap(
            PlacementErrorKind::InvalidConstraintFormat,
            format!(
                "constraint should have exactly one label value, got {}",
                go_string_slice(&constraint.values)
            ),
        ));
    }
    if constraint.op == LabelConstraintOp::IN {
        builder.push('+');
    } else if constraint.op == LabelConstraintOp::NOT_IN {
        builder.push('-');
    } else {
        return Err(PlacementError::wrap(
            PlacementErrorKind::InvalidConstraintFormat,
            format!("disallowed operation '{}'", constraint.op.as_str()),
        ));
    }
    builder.push_str(&constraint.key);
    builder.push('=');
    builder.push_str(&constraint.values[0]);
    Ok(builder)
}

/// Go's `%v` rendering of a `[]string`.
fn go_string_slice(values: &[String]) -> String {
    format!("[{}]", values.join(" "))
}

/// Go `ConstraintCompatibility`: the return type of `ConstraintCompatibleWith`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConstraintCompatibility {
    /// Go `ConstraintCompatible`: the two constraints can coexist.
    Compatible,
    /// Go `ConstraintIncompatible`: no store can satisfy both.
    Incompatible,
    /// Go `ConstraintDuplicated`: the two constraints say the same thing.
    Duplicated,
}

/// Go `ConstraintCompatibleWith`: checks whether two constraints are
/// compatible.
#[must_use]
pub fn constraint_compatible_with(
    constraint: &LabelConstraint,
    other: &LabelConstraint,
) -> ConstraintCompatibility {
    let same_key = constraint.key == other.key;
    if !same_key {
        return ConstraintCompatibility::Compatible;
    }

    let same_op = constraint.op == other.op;
    let mut same_val = true;
    for (index, value) in constraint.values.iter().enumerate() {
        if index < other.values.len() && *value != other.values[index] {
            same_val = false;
            break;
        }
    }
    // No following cases:
    // 1. duplicated constraint, skip it
    // 2. no instance can meet: +dc=sh, -dc=sh
    // 3. can not match multiple instances: +dc=sh, +dc=bj
    if same_op && same_val {
        ConstraintCompatibility::Duplicated
    } else if (!same_op && same_val)
        || (same_op && !same_val && constraint.op == LabelConstraintOp::IN)
    {
        ConstraintCompatibility::Incompatible
    } else {
        ConstraintCompatibility::Compatible
    }
}

#[cfg(test)]
mod tests {
    use super::{
        constraint_compatible_with, new_constraint, restore_constraint, ConstraintCompatibility,
    };
    use crate::errors::PlacementErrorKind;
    use crate::pd::{LabelConstraint, LabelConstraintOp};

    /// Go `TestNewFromYaml` (`constraint_test.go`).
    #[test]
    fn test_new_from_yaml() {
        crate::constraints::new_constraints_from_yaml(b"[]").expect("empty array");
        crate::constraints::new_constraints_from_yaml(b"]").expect_err("not an array");
    }

    /// Go `TestNewConstraint` (`constraint_test.go`).
    #[test]
    fn test_new_constraint() {
        struct TestCase {
            name: &'static str,
            input: &'static str,
            label: LabelConstraint,
            err: Option<PlacementErrorKind>,
        }
        let ok = |key: &str, op: LabelConstraintOp, value: &str| LabelConstraint {
            key: key.to_owned(),
            op,
            values: vec![value.to_owned()],
        };
        let tests = vec![
            TestCase {
                name: "normal",
                input: "+zone=bj",
                label: ok("zone", LabelConstraintOp::IN, "bj"),
                err: None,
            },
            TestCase {
                name: "normal with spaces",
                input: "-  dc  =  sh  ",
                label: ok("dc", LabelConstraintOp::NOT_IN, "sh"),
                err: None,
            },
            TestCase {
                name: "not tiflash",
                input: "-engine  =  tiflash  ",
                label: ok("engine", LabelConstraintOp::NOT_IN, "tiflash"),
                err: None,
            },
            TestCase {
                name: "not tiflash_compute",
                input: "-engine  =  tiflash_compute  ",
                label: ok("engine", LabelConstraintOp::NOT_IN, "tiflash_compute"),
                err: None,
            },
            TestCase {
                name: "disallow tiflash",
                input: "+engine=Tiflash",
                label: LabelConstraint::default(),
                err: Some(PlacementErrorKind::UnsupportedConstraint),
            },
            // invalid
            TestCase {
                name: "invalid length",
                input: ",,,",
                label: LabelConstraint::default(),
                err: Some(PlacementErrorKind::InvalidConstraintFormat),
            },
            TestCase {
                name: "invalid, lack = 1",
                input: "+    ",
                label: LabelConstraint::default(),
                err: Some(PlacementErrorKind::InvalidConstraintFormat),
            },
            TestCase {
                name: "invalid, lack = 2",
                input: "+000",
                label: LabelConstraint::default(),
                err: Some(PlacementErrorKind::InvalidConstraintFormat),
            },
            TestCase {
                name: "invalid op",
                input: "0000",
                label: LabelConstraint::default(),
                err: Some(PlacementErrorKind::InvalidConstraintFormat),
            },
            TestCase {
                name: "empty key 1",
                input: "+ =zone1",
                label: LabelConstraint::default(),
                err: Some(PlacementErrorKind::InvalidConstraintFormat),
            },
            TestCase {
                name: "empty key 2",
                input: "+  =   z",
                label: LabelConstraint::default(),
                err: Some(PlacementErrorKind::InvalidConstraintFormat),
            },
            TestCase {
                name: "empty value 1",
                input: "+zone=",
                label: LabelConstraint::default(),
                err: Some(PlacementErrorKind::InvalidConstraintFormat),
            },
            TestCase {
                name: "empty value 2",
                input: "+z  =   ",
                label: LabelConstraint::default(),
                err: Some(PlacementErrorKind::InvalidConstraintFormat),
            },
        ];

        for test in tests {
            let result = new_constraint(test.input);
            match test.err {
                None => assert_eq!(test.label, result.expect(test.name), "{}", test.name),
                Some(kind) => assert!(
                    result.as_ref().is_err_and(|err| err.is(kind)),
                    "{}: {result:?}",
                    test.name
                ),
            }
        }
    }

    /// Go `TestRestoreConstraint` (`constraint_test.go`).
    #[test]
    fn test_restore_constraint() {
        struct TestCase {
            name: &'static str,
            input: LabelConstraint,
            output: &'static str,
            err: Option<PlacementErrorKind>,
        }
        let mut tests = Vec::new();

        tests.push(TestCase {
            name: "normal, op in",
            input: new_constraint("+zone=bj").expect("valid"),
            output: "+zone=bj",
            err: None,
        });
        tests.push(TestCase {
            name: "normal with spaces, op in",
            input: new_constraint("+  zone = bj  ").expect("valid"),
            output: "+zone=bj",
            err: None,
        });
        tests.push(TestCase {
            name: "normal with spaces, op not in",
            input: new_constraint("-  zone = bj  ").expect("valid"),
            output: "-zone=bj",
            err: None,
        });
        tests.push(TestCase {
            name: "no values",
            input: LabelConstraint {
                op: LabelConstraintOp::IN,
                key: "dc".to_owned(),
                values: Vec::new(),
            },
            output: "",
            err: Some(PlacementErrorKind::InvalidConstraintFormat),
        });
        tests.push(TestCase {
            name: "multiple values",
            input: LabelConstraint {
                op: LabelConstraintOp::IN,
                key: "dc".to_owned(),
                values: vec!["dc1".to_owned(), "dc2".to_owned()],
            },
            output: "",
            err: Some(PlacementErrorKind::InvalidConstraintFormat),
        });
        tests.push(TestCase {
            name: "invalid op",
            input: LabelConstraint {
                op: LabelConstraintOp::from("["),
                key: "dc".to_owned(),
                values: Vec::new(),
            },
            output: "",
            err: Some(PlacementErrorKind::InvalidConstraintFormat),
        });

        for test in tests {
            let result = restore_constraint(&test.input);
            match test.err {
                None => assert_eq!(test.output, result.expect(test.name), "{}", test.name),
                Some(kind) => assert!(
                    result.as_ref().is_err_and(|err| err.is(kind)),
                    "{}: {result:?}",
                    test.name
                ),
            }
        }
    }

    /// Go `TestCompatibleWith` (`constraint_test.go`).
    #[test]
    fn test_compatible_with() {
        let cases = [
            (
                "case 2",
                "+zone=sh",
                "-zone=sh",
                ConstraintCompatibility::Incompatible,
            ),
            (
                "case 3",
                "+zone=bj",
                "+zone=sh",
                ConstraintCompatibility::Incompatible,
            ),
            (
                "case 1",
                "+zone=sh",
                "+zone=sh",
                ConstraintCompatibility::Duplicated,
            ),
            (
                "normal 1",
                "+zone=sh",
                "+dc=sh",
                ConstraintCompatibility::Compatible,
            ),
            (
                "normal 2",
                "-zone=sh",
                "-zone=bj",
                ConstraintCompatibility::Compatible,
            ),
        ];

        for (name, first, second, output) in cases {
            let i1 = new_constraint(first).expect("valid");
            let i2 = new_constraint(second).expect("valid");
            assert_eq!(output, constraint_compatible_with(&i1, &i2), "{name}");
        }
    }
}
