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

//! Go `pkg/util/ranger/detacher.go`, on the whole-file track: separating
//! the conditions that can BUILD ranges from the conditions that must stay
//! as filters.
//!
//! Landed here first: the COLUMN layer — the mutually recursive CNF/DNF
//! walk (`detachColumnCNFConditions`/`detachColumnDNFConditions`) whose
//! contract is Go's own: a CNF conjunct is an access condition when the
//! checker admits it (staying a filter too when reserved); an OR arm that
//! cannot contribute poisons its WHOLE disjunction into the filters; and a
//! rebuilt DNF/CNF re-composes only the contributing arms. Its entry
//! points `DetachCondsForColumn` and `ExtractAccessConditionsForColumn`
//! are what `BuildColumnRange`'s callers detach with.
//!
//! The INDEX layer (`rangeDetacher`, eq/in extraction, CNF/DNF index
//! detachment, the shard-index GC-column family) continues in this file
//! with `ranger.go`'s multi-column append machinery, their shared
//! consumer.

use tidb_expr::expr_util::normal_form::{flatten_cnf_conditions, flatten_dnf_conditions};
use tidb_expr::expr_util::predicates::contains;
use tidb_expr::expression::Expression;
use tidb_expr::simple_expr::{compose_cnf_condition, compose_dnf_condition};

use super::checker::{ConditionChecker, UNSPECIFIED_LENGTH};

/// Go `detachColumnCNFConditions`: `(access_conditions,
/// filter_conditions)`.
pub fn detach_column_cnf_conditions(
    conditions: &[Expression],
    checker: &ConditionChecker<'_>,
) -> (Vec<Expression>, Vec<Expression>) {
    let mut access_conditions = Vec::new();
    let mut filter_conditions = Vec::new();
    for cond in conditions {
        if let Expression::ScalarFunction(sf) = cond {
            if sf.func_name.lowercase() == "or" {
                let dnf_items = flatten_dnf_conditions(sf);
                let (column_dnf_items, has_residual) =
                    detach_column_dnf_conditions(&dnf_items, checker);
                // A DNF with an unresolvable arm rides whole into the
                // filters.
                if has_residual {
                    filter_conditions.push(cond.clone());
                }
                if column_dnf_items.is_empty() {
                    continue;
                }
                if let Some(rebuilt) = compose_dnf_condition(column_dnf_items) {
                    access_conditions.push(rebuilt);
                }
                continue;
            }
        }
        let (is_access_cond, should_reserve) = checker.check(cond);
        if !is_access_cond {
            filter_conditions.push(cond.clone());
            continue;
        }
        access_conditions.push(cond.clone());
        if should_reserve {
            filter_conditions.push(cond.clone());
        }
    }
    (access_conditions, filter_conditions)
}

/// Go `detachColumnDNFConditions`: `(access_conditions, has_residual)`.
/// One arm with NO access condition kills the whole disjunction
/// (`(nil, true)`).
pub fn detach_column_dnf_conditions(
    conditions: &[Expression],
    checker: &ConditionChecker<'_>,
) -> (Vec<Expression>, bool) {
    let mut has_residual_conditions = false;
    let mut access_conditions = Vec::new();
    for cond in conditions {
        if let Expression::ScalarFunction(sf) = cond {
            if sf.func_name.lowercase() == "and" {
                let cnf_items = flatten_cnf_conditions(sf);
                let (column_cnf_items, others) =
                    detach_column_cnf_conditions(&cnf_items, checker);
                if !others.is_empty() {
                    has_residual_conditions = true;
                }
                // One part of the DNF with no access condition: no range.
                if column_cnf_items.is_empty() {
                    return (Vec::new(), true);
                }
                if let Some(rebuilt) = compose_cnf_condition(column_cnf_items) {
                    access_conditions.push(rebuilt);
                }
                continue;
            }
        }
        let (is_access_cond, should_reserve) = checker.check(cond);
        if !is_access_cond {
            return (Vec::new(), true);
        }
        access_conditions.push(cond.clone());
        if should_reserve {
            has_residual_conditions = true;
        }
    }
    (access_conditions, has_residual_conditions)
}

/// Go `removeConditions`: `conditions` minus the semantic occurrences of
/// `conds_to_remove` (`expression.Contains`).
#[must_use]
pub fn remove_conditions(
    conditions: &[Expression],
    conds_to_remove: &[Expression],
) -> Vec<Expression> {
    conditions
        .iter()
        .filter(|cond| !contains(conds_to_remove, cond))
        .cloned()
        .collect()
}

/// Go `AppendConditionsIfNotExist`.
#[must_use]
pub fn append_conditions_if_not_exist(
    mut conditions: Vec<Expression>,
    conds_to_append: &[Expression],
) -> Vec<Expression> {
    let missing: Vec<Expression> = conds_to_append
        .iter()
        .filter(|cond| !contains(&conditions, cond))
        .cloned()
        .collect();
    conditions.extend(missing);
    conditions
}

/// Go `ExtractAccessConditionsForColumn`: the access conditions only, no
/// filter split (a flat filter over the checker, unlike the CNF/DNF walk).
#[must_use]
pub fn extract_access_conditions_for_column(
    conds: &[Expression],
    col: &tidb_expr::column::Column,
    opt_prefix_index_single_scan: bool,
) -> Vec<Expression> {
    let checker = ConditionChecker {
        checker_col: Some(col),
        length: UNSPECIFIED_LENGTH,
        opt_prefix_index_single_scan,
    };
    conds
        .iter()
        .filter(|expr| checker.check(expr).0)
        .cloned()
        .collect()
}

/// Go `DetachCondsForColumn`.
#[must_use]
pub fn detach_conds_for_column(
    conds: &[Expression],
    col: &tidb_expr::column::Column,
    opt_prefix_index_single_scan: bool,
) -> (Vec<Expression>, Vec<Expression>) {
    let checker = ConditionChecker {
        checker_col: Some(col),
        length: UNSPECIFIED_LENGTH,
        opt_prefix_index_single_scan,
    };
    detach_column_cnf_conditions(conds, &checker)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{Datum, FieldType, FieldTypeCode};
    use tidb_expr::column::Column;
    use tidb_expr::scalar_function::ScalarFunction;

    fn int_column(unique_id: i64) -> Column {
        Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong))
    }

    fn int_const(v: i64) -> Expression {
        Expression::Constant(tidb_expr::constant::Constant::new(
            Datum::Int(v),
            FieldType::new(FieldTypeCode::LongLong),
        ))
    }

    fn func(name: &str, args: Vec<Expression>) -> Expression {
        Expression::ScalarFunction(ScalarFunction::new(
            tidb_ast::CiString::new(name),
            FieldType::new(FieldTypeCode::LongLong),
            args,
        ))
    }

    /// The CNF walk: conditions over the checked column become access
    /// conditions; conditions over OTHER columns stay filters.
    #[test]
    fn cnf_detach_splits_by_column() {
        let a = int_column(1);
        let b = int_column(2);
        let conds = vec![
            func("gt", vec![Expression::Column(a.clone()), int_const(1)]),
            func("lt", vec![Expression::Column(b.clone()), int_const(5)]),
            func("eq", vec![Expression::Column(a.clone()), int_const(3)]),
        ];
        let (access, filters) = detach_conds_for_column(&conds, &a, true);
        assert_eq!(access.len(), 2);
        assert_eq!(filters.len(), 1);
        // The other column's condition is the filter.
        assert!(matches!(&filters[0], Expression::ScalarFunction(sf)
            if sf.func_name.lowercase() == "lt"));
    }

    /// Go's DNF poisoning: `(a > 1 OR b > 2)` cannot serve `a` — one arm
    /// over another column kills the disjunction into the filters; a
    /// same-column DNF passes whole.
    #[test]
    fn a_foreign_arm_poisons_its_disjunction() {
        let a = int_column(1);
        let b = int_column(2);
        let mixed_or = func(
            "or",
            vec![
                func("gt", vec![Expression::Column(a.clone()), int_const(1)]),
                func("gt", vec![Expression::Column(b.clone()), int_const(2)]),
            ],
        );
        let (access, filters) = detach_conds_for_column(&[mixed_or], &a, true);
        assert!(access.is_empty());
        assert_eq!(filters.len(), 1);

        let same_or = func(
            "or",
            vec![
                func("gt", vec![Expression::Column(a.clone()), int_const(5)]),
                func("eq", vec![Expression::Column(a.clone()), int_const(1)]),
            ],
        );
        let (access, filters) = detach_conds_for_column(&[same_or], &a, true);
        assert_eq!(access.len(), 1);
        assert!(filters.is_empty());
        // The rebuilt DNF is still an OR over both arms.
        assert!(matches!(&access[0], Expression::ScalarFunction(sf)
            if sf.func_name.lowercase() == "or" && sf.args.len() == 2));
    }

    /// A CNF inside a DNF arm: `(a > 1 AND b > 2) OR a = 5` — the AND arm
    /// keeps its `a` half as access and reports the `b` half as residual,
    /// so the whole DNF ALSO rides into filters.
    #[test]
    fn a_partial_and_arm_keeps_access_and_reserves() {
        let a = int_column(1);
        let b = int_column(2);
        let or = func(
            "or",
            vec![
                func(
                    "and",
                    vec![
                        func("gt", vec![Expression::Column(a.clone()), int_const(1)]),
                        func("gt", vec![Expression::Column(b.clone()), int_const(2)]),
                    ],
                ),
                func("eq", vec![Expression::Column(a.clone()), int_const(5)]),
            ],
        );
        let (access, filters) = detach_conds_for_column(&[or], &a, true);
        assert_eq!(access.len(), 1, "the rebuilt (a>1) OR (a=5)");
        assert_eq!(filters.len(), 1, "the original OR stays as the filter");
    }

    /// `removeConditions` / `AppendConditionsIfNotExist` round trip.
    #[test]
    fn condition_set_helpers_use_semantic_equality() {
        let a = int_column(1);
        let gt = func("gt", vec![Expression::Column(a.clone()), int_const(1)]);
        let lt = func("lt", vec![Expression::Column(a.clone()), int_const(9)]);
        let removed = remove_conditions(&[gt.clone(), lt.clone()], &[gt.clone()]);
        assert_eq!(removed.len(), 1);
        let appended = append_conditions_if_not_exist(removed, &[gt.clone(), lt]);
        assert_eq!(appended.len(), 2, "the existing lt does not duplicate");
    }

    /// `ExtractAccessConditionsForColumn` keeps only what the checker
    /// admits, with no filter bookkeeping.
    #[test]
    fn extraction_is_the_flat_admission_filter() {
        let a = int_column(1);
        let b = int_column(2);
        let conds = vec![
            func("ge", vec![Expression::Column(a.clone()), int_const(0)]),
            func("eq", vec![Expression::Column(b), int_const(1)]),
        ];
        let access = extract_access_conditions_for_column(&conds, &a, true);
        assert_eq!(access.len(), 1);
    }
}
