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

//! Go `constraints.go`: sets of label constraints and their fingerprint.

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use sha2::{Digest as _, Sha256};

use crate::constraint::{
    constraint_compatible_with, new_constraint, restore_constraint, ConstraintCompatibility,
};
use crate::errors::{PlacementError, PlacementErrorKind};
use crate::pd::{LabelConstraint, PeerRoleType};
use crate::rule::{ATTRIBUTE_EVICT_LEADER, ATTRIBUTE_PREFIX};
use crate::yaml_lite::unmarshal_strict_string_slice;

/// Go `NewConstraints`: checks each label and builds the constraints.
///
/// # Errors
///
/// Propagates [`new_constraint`]'s format errors, and returns
/// `ErrConflictingConstraints` when two labels cannot hold at once.
pub fn new_constraints(labels: &[String]) -> Result<Vec<LabelConstraint>, PlacementError> {
    if labels.is_empty() {
        return Ok(Vec::new());
    }

    let mut constraints = Vec::with_capacity(labels.len());
    for label in labels {
        let label = new_constraint(label.trim())?;
        add_constraint(&mut constraints, label)?;
    }
    Ok(constraints)
}

/// Go `preCheckDictConstraintStr`: splits one dict key into labels, letting an
/// `#evict-leader` attribute override the role.
///
/// # Errors
///
/// Returns `ErrUnsupportedConstraint` for any other `#attribute`.
pub(crate) fn pre_check_dict_constraint_str(
    label_str: &str,
    role: &PeerRoleType,
) -> Result<(Vec<String>, PeerRoleType), PlacementError> {
    let mut override_role = role.clone();
    let mut new_labels = Vec::new();
    for label in label_str.split(',') {
        if let Some(attribute) = label.strip_prefix(ATTRIBUTE_PREFIX) {
            if attribute == ATTRIBUTE_EVICT_LEADER {
                if *role == PeerRoleType::VOTER {
                    override_role = PeerRoleType::FOLLOWER;
                }
            } else {
                return Err(PlacementError::wrap(
                    PlacementErrorKind::UnsupportedConstraint,
                    format!("unsupported attribute '{label}'"),
                ));
            }
            continue;
        }
        new_labels.push(label.to_owned());
    }
    Ok((new_labels, override_role))
}

/// Go `NewConstraintsFromYaml`: parses the raw 'array' constraints and calls
/// [`new_constraints`].
///
/// Refer to
/// <https://github.com/pingcap/tidb/blob/master/docs/design/2020-06-24-placement-rules-in-sql.md>.
///
/// # Errors
///
/// Returns `ErrInvalidConstraintsFormat` when the input is not a YAML array,
/// then propagates [`new_constraints`].
pub fn new_constraints_from_yaml(input: &[u8]) -> Result<Vec<LabelConstraint>, PlacementError> {
    let constraints = unmarshal_strict_string_slice(input)
        .map_err(|_| PlacementError::new(PlacementErrorKind::InvalidConstraintsFormat))?;
    new_constraints(&constraints)
}

/// Go `NewConstraintsDirect`: a helper for creating new constraints from
/// individual constraints.
#[must_use]
pub fn new_constraints_direct(constraints: Vec<LabelConstraint>) -> Vec<LabelConstraint> {
    constraints
}

/// Go `RestoreConstraints`: converts label constraints to a string.
///
/// # Errors
///
/// Propagates [`restore_constraint`].
pub fn restore_constraints(constraints: &[LabelConstraint]) -> Result<String, PlacementError> {
    let mut builder = String::new();
    for (index, constraint) in constraints.iter().enumerate() {
        if index > 0 {
            builder.push(',');
        }
        builder.push('"');
        builder.push_str(&restore_constraint(constraint)?);
        builder.push('"');
    }
    Ok(builder)
}

/// Go `AddConstraint`: adds a new label constraint, validating it against all
/// existing constraints. Note that it does not validate the single constraint
/// on its own.
///
/// # Errors
///
/// Returns `ErrConflictingConstraints` when the new label contradicts one that
/// is already present.
pub fn add_constraint(
    constraints: &mut Vec<LabelConstraint>,
    label: LabelConstraint,
) -> Result<(), PlacementError> {
    let mut pass = true;

    for existing in constraints.iter() {
        match constraint_compatible_with(&label, existing) {
            ConstraintCompatibility::Compatible => continue,
            ConstraintCompatibility::Duplicated => {
                pass = false;
                continue;
            }
            ConstraintCompatibility::Incompatible => {
                let first = restore_constraint(&label).unwrap_or_else(|err| err.to_string());
                let second = restore_constraint(existing).unwrap_or_else(|err| err.to_string());
                return Err(PlacementError::wrap(
                    PlacementErrorKind::ConflictingConstraints,
                    format!("'{first}' and '{second}'"),
                ));
            }
        }
    }

    if pass {
        constraints.push(label);
    }
    Ok(())
}

/// Go `ConstraintsFingerPrint`: a unique string for the constraint set.
#[must_use]
pub fn constraints_finger_print(constraints: &[LabelConstraint]) -> String {
    let mut copied: Vec<String> = constraints.iter().map(constraint_to_string).collect();
    copied.sort();
    let combined_constraints = copied.concat();

    // Calculate the SHA256 hash of the concatenated constraints.
    let hash = Sha256::digest(combined_constraints.as_bytes());

    // Encode the hash as a base64 string.
    STANDARD.encode(hash)
}

/// Go `constraintToString`.
fn constraint_to_string(constraint: &LabelConstraint) -> String {
    // Sort the values in the constraint.
    let mut sorted_values = constraint.values.clone();
    sorted_values.sort();
    format!(
        "{}|{}|{}",
        constraint.key,
        constraint.op.as_str(),
        sorted_values.join(",")
    )
}

#[cfg(test)]
mod tests {
    use super::{add_constraint, new_constraints, restore_constraints};
    use crate::constraint::new_constraint;
    use crate::errors::PlacementErrorKind;
    use crate::pd::{LabelConstraint, LabelConstraintOp};

    fn labels(values: &[&str]) -> Vec<String> {
        values.iter().copied().map(str::to_owned).collect()
    }

    /// Go `TestNewConstraints` (`constraints_test.go`).
    #[test]
    fn test_new_constraints() {
        new_constraints(&[]).expect("nil labels");
        new_constraints(&labels(&[])).expect("empty labels");

        let err = new_constraints(&labels(&["+zonesh"])).expect_err("bad format");
        assert!(err.is(PlacementErrorKind::InvalidConstraintFormat), "{err}");

        let err = new_constraints(&labels(&["+zone=sh", "-zone=sh"])).expect_err("conflict");
        assert!(err.is(PlacementErrorKind::ConflictingConstraints), "{err}");
    }

    /// Go `TestAdd` (`constraints_test.go`).
    #[test]
    fn test_add() {
        struct TestCase {
            name: &'static str,
            labels: Vec<LabelConstraint>,
            label: LabelConstraint,
            err: Option<PlacementErrorKind>,
        }
        let mut tests = Vec::new();

        let base = new_constraints(&labels(&["+zone=sh"])).expect("valid");
        tests.push(TestCase {
            name: "always false match",
            labels: base.clone(),
            label: new_constraint("-zone=sh").expect("valid"),
            err: Some(PlacementErrorKind::ConflictingConstraints),
        });

        let label = new_constraint("+zone=sh").expect("valid");
        tests.push(TestCase {
            name: "duplicated constraints, skip",
            labels: base.clone(),
            label: label.clone(),
            err: None,
        });

        let mut with_conflict = base.clone();
        with_conflict.push(LabelConstraint {
            op: LabelConstraintOp::NOT_IN,
            key: "zone".to_owned(),
            values: vec!["sh".to_owned()],
        });
        tests.push(TestCase {
            name: "duplicated constraints should not stop conflicting constraints check",
            labels: with_conflict,
            label: label.clone(),
            err: Some(PlacementErrorKind::ConflictingConstraints),
        });

        tests.push(TestCase {
            name: "invalid label in operand",
            labels: base.clone(),
            label: LabelConstraint {
                op: LabelConstraintOp::from("["),
                ..LabelConstraint::default()
            },
            err: None,
        });

        tests.push(TestCase {
            name: "invalid label in operator",
            labels: vec![LabelConstraint {
                op: LabelConstraintOp::from("["),
                ..LabelConstraint::default()
            }],
            label,
            err: None,
        });

        tests.push(TestCase {
            name: "invalid label in both, same key",
            labels: vec![LabelConstraint {
                op: LabelConstraintOp::from("["),
                key: "dc".to_owned(),
                ..LabelConstraint::default()
            }],
            label: LabelConstraint {
                op: LabelConstraintOp::from("]"),
                key: "dc".to_owned(),
                ..LabelConstraint::default()
            },
            err: Some(PlacementErrorKind::ConflictingConstraints),
        });

        tests.push(TestCase {
            name: "normal",
            labels: base,
            label: new_constraint("-zone=bj").expect("valid"),
            err: None,
        });

        for mut test in tests {
            let result = add_constraint(&mut test.labels, test.label.clone());
            match test.err {
                None => {
                    result.unwrap_or_else(|err| panic!("{}: {err}", test.name));
                    assert_eq!(Some(&test.label), test.labels.last(), "{}", test.name);
                }
                Some(kind) => assert!(
                    result.as_ref().is_err_and(|err| err.is(kind)),
                    "{}: {result:?}",
                    test.name
                ),
            }
        }
    }

    /// Go `TestRestoreConstraints` (`constraints_test.go`).
    #[test]
    fn test_restore_constraints() {
        struct TestCase {
            name: &'static str,
            input: Vec<LabelConstraint>,
            output: &'static str,
            err: Option<PlacementErrorKind>,
        }
        let mut tests = vec![TestCase {
            name: "normal1",
            input: Vec::new(),
            output: "",
            err: None,
        }];

        tests.push(TestCase {
            name: "normal2",
            input: vec![
                new_constraint("+zone=bj").expect("valid"),
                new_constraint("-zone=sh").expect("valid"),
            ],
            output: r#""+zone=bj","-zone=sh""#,
            err: None,
        });

        tests.push(TestCase {
            name: "error",
            input: vec![LabelConstraint {
                op: LabelConstraintOp::from("["),
                key: "dc".to_owned(),
                values: vec!["dc1".to_owned()],
            }],
            output: "",
            err: Some(PlacementErrorKind::InvalidConstraintFormat),
        });

        for test in tests {
            let result = restore_constraints(&test.input);
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
}
