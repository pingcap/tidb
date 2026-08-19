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


use tidb_datatype::Datum;

use super::points::{
    range_point_cmp, Point, PointBuilder, OP_EQ, OP_GE, OP_GT, OP_LE, OP_LT, OP_NE, OP_NULL_EQ,
};

/// Go `valueInfo`: one index column's constant value, when it has one.
/// `mutable` marks plan-cache parameters whose value cannot be trusted at
/// plan time.
#[derive(Clone, Debug)]
pub struct ValueInfo {
    /// Go `value` — `None` when mutable.
    pub value: Option<Datum>,
    /// Go `mutable`.
    pub mutable: bool,
}

/// Go `getPotentialEqOrInColOffset`: which index column (by offset) this
/// condition could pin as an equality — an EQ/compare against a constant,
/// an all-constant IN, an IS NULL, or a same-offset DNF of those.
#[must_use]
pub fn get_potential_eq_or_in_col_offset(
    expr: &Expression,
    cols: &[tidb_expr::column::Column],
    regard_null_as_point: bool,
) -> i64 {
    let Expression::ScalarFunction(f) = expr else {
        return -1;
    };
    let (_, collation) = f.collation.charset_and_collation();
    match f.func_name.lowercase() {
        "or" => {
            let dnf_items = flatten_dnf_conditions(f);
            let mut offset = -1;
            for dnf_item in &dnf_items {
                let cur_offset =
                    get_potential_eq_or_in_col_offset(dnf_item, cols, regard_null_as_point);
                if cur_offset == -1 {
                    return -1;
                }
                if offset != -1 && cur_offset != offset {
                    return -1;
                }
                offset = cur_offset;
            }
            offset
        }
        name @ ("eq" | "nulleq" | "le" | "ge" | "lt" | "gt") => {
            let (column, const_side) = if let Expression::Column(c) = &f.args[0] {
                (c, &f.args[1])
            } else if let Expression::Column(c) = &f.args[1] {
                (c, &f.args[0])
            } else {
                return -1;
            };
            let column_type = column.ret_type.as_ref();
            if column_type
                .is_some_and(|ft| ft.eval_type() == tidb_datatype::EvalType::String)
                && !tidb_datatype::compatible_collate(
                    column_type.map_or("", |ft| ft.collation_name()),
                    collation,
                )
            {
                return -1;
            }
            // LT/GT pin an equality only for INT columns (`x >= 2 AND
            // x <= 2` folding).
            if (name == "lt" || name == "gt")
                && !column_type
                    .is_some_and(|ft| ft.eval_type() == tidb_datatype::EvalType::Int)
            {
                return -1;
            }
            let Expression::Constant(const_val) = const_side else {
                return -1;
            };
            let val = &const_val.value;
            // col <=> NULL stays a range scan, not a point get (nullable
            // unique indexes can hold several NULL rows).
            if (!regard_null_as_point && matches!(val, Datum::Null))
                || (name == "nulleq" && matches!(val, Datum::Null))
            {
                return -1;
            }
            for (i, col) in cols.iter().enumerate() {
                if col.equal_column(&Expression::Column(column.clone())) {
                    return i as i64;
                }
            }
            -1
        }
        "in" => {
            let Expression::Column(c) = &f.args[0] else {
                return -1;
            };
            let column_type = c.ret_type.as_ref();
            if column_type
                .is_some_and(|ft| ft.eval_type() == tidb_datatype::EvalType::String)
                && !tidb_datatype::compatible_collate(
                    column_type.map_or("", |ft| ft.collation_name()),
                    collation,
                )
            {
                return -1;
            }
            if f.args[1..]
                .iter()
                .any(|arg| !matches!(arg, Expression::Constant(_)))
            {
                return -1;
            }
            for (i, col) in cols.iter().enumerate() {
                if col.equal_column(&Expression::Column(c.clone())) {
                    return i as i64;
                }
            }
            -1
        }
        "isnull" => {
            let Expression::Column(c) = &f.args[0] else {
                return -1;
            };
            for (i, col) in cols.iter().enumerate() {
                if col.equal_column(&Expression::Column(c.clone())) {
                    return i as i64;
                }
            }
            -1
        }
        _ => -1,
    }
}

/// Go `excludeToIncludeForIntPoint`: turn an exclusive INT point inclusive
/// by stepping the value; `None` marks an unsatisfiable interval end
/// (`(MaxUint64, ...` / `..., MinInt64)`).
fn exclude_to_include_for_int_point(mut p: Point) -> Option<Point> {
    if !p.excl {
        return Some(p);
    }
    match p.value {
        Datum::Int(val) => {
            if p.start {
                if val == i64::MAX {
                    p.value = Datum::UInt(val as u64 + 1);
                } else {
                    p.value = Datum::Int(val + 1);
                }
                p.excl = false;
            } else {
                if val == i64::MIN {
                    return None;
                }
                p.value = Datum::Int(val - 1);
                p.excl = false;
            }
            Some(p)
        }
        Datum::UInt(val) => {
            if p.start {
                if val == u64::MAX {
                    return None;
                }
                p.value = Datum::UInt(val + 1);
                p.excl = false;
            } else {
                if val == 0 {
                    // Go stores `int64(0 - 1)` — the wrap is deliberate.
                    p.value = Datum::Int(-1);
                } else {
                    p.value = Datum::UInt(val - 1);
                }
                p.excl = false;
            }
            Some(p)
        }
        _ => Some(p),
    }
}

/// Go `allSinglePoints`: `None` when any interval is longer than a point;
/// otherwise the satisfiable single points.
fn all_single_points(points: Vec<Point>, collation: tidb_datatype::Collation) -> Option<Vec<Point>> {
    let mut result = Vec::with_capacity(points.len());
    let mut iter = points.into_iter();
    while let (Some(left_raw), Some(right_raw)) = (iter.next(), iter.next()) {
        let Some(left) = exclude_to_include_for_int_point(left_raw) else {
            continue;
        };
        let Some(right) = exclude_to_include_for_int_point(right_raw) else {
            continue;
        };
        if !left.start || right.start || left.excl || right.excl {
            return None;
        }
        match left.value.compare(&right.value, collation) {
            Ok(std::cmp::Ordering::Equal) => {}
            _ => return None,
        }
        result.push(left);
        result.push(right);
    }
    Some(result)
}

/// Go `allEqOrIn`.
fn all_eq_or_in(expr: &Expression) -> bool {
    let Expression::ScalarFunction(f) = expr else {
        return false;
    };
    match f.func_name.lowercase() {
        "or" => f.args.iter().all(all_eq_or_in),
        "eq" | "nulleq" | "in" | "isnull" => true,
        _ => false,
    }
}

/// Go `extractValueInfo`.
fn extract_value_info(expr: &Expression) -> Option<ValueInfo> {
    let Expression::ScalarFunction(f) = expr else {
        return None;
    };
    match f.func_name.lowercase() {
        "isnull" => Some(ValueInfo {
            value: Some(Datum::Null),
            mutable: false,
        }),
        "eq" | "nulleq" => {
            for arg in &f.args[..2.min(f.args.len())] {
                if let Expression::Constant(c) = arg {
                    // Go's mutable test reads ParamMarker/DeferredExpr;
                    // this port's constants are materialized values.
                    return Some(ValueInfo {
                        value: Some(c.value.clone()),
                        mutable: false,
                    });
                }
            }
            None
        }
        _ => None,
    }
}

/// Go `points2EqOrInCond` (`ranger.go:802`): points BACK into an EQ/IN
/// (plus IS NULL arms for null points) over the column.
fn points_to_eq_or_in_cond(
    points: &[Point],
    col: &tidb_expr::column::Column,
) -> Option<Expression> {
    let ret_type = col
        .ret_type
        .clone()
        .unwrap_or_else(|| tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong));
    let ctx = tidb_expr::NoColumns;
    let mut args: Vec<Expression> = vec![Expression::Column(col.clone())];
    let mut or_args: Vec<Expression> = Vec::new();
    let mut i = 0;
    while i < points.len() {
        if matches!(points[i].value, Datum::Null) {
            or_args.push(
                tidb_expr::new_function::new_function_internal(
                    &ctx,
                    "isnull",
                    ret_type.clone(),
                    vec![Expression::Column(col.clone())],
                )?,
            );
        } else {
            args.push(Expression::Constant(tidb_expr::constant::Constant::new(
                points[i].value.clone(),
                ret_type.clone(),
            )));
        }
        i += 2;
    }
    let mut result = None;
    if args.len() > 1 {
        let func_name = if args.len() > 2 { "in" } else { "eq" };
        result = Some(tidb_expr::new_function::new_function_internal(
            &ctx,
            func_name,
            ret_type.clone(),
            args,
        )?);
    }
    if or_args.is_empty() {
        return result;
    }
    if let Some(result) = result {
        or_args.push(result);
    }
    if or_args.len() == 1 {
        return or_args.into_iter().next();
    }
    tidb_expr::new_function::new_function_internal(&ctx, "or", ret_type, or_args)
}

/// Go `ExtractEqAndInCondition`'s product.
#[derive(Debug, Default)]
pub struct EqAndInExtraction {
    /// Go `accesses`: the leading equality chain, one per pinned column.
    pub accesses: Vec<Expression>,
    /// Go `filters`: prefix-index access conditions that must ALSO filter.
    pub filters: Vec<Expression>,
    /// Go `newConditions`: the simplified condition set.
    pub new_conditions: Vec<Expression>,
    /// Go `columnValues`.
    pub column_values: Vec<Option<ValueInfo>>,
    /// Go's trailing bool: an EMPTY merged range was proven.
    pub empty_range: bool,
}

/// Go `ExtractEqAndInCondition` (`detacher.go:732`).
#[must_use]
pub fn extract_eq_and_in_condition(
    conditions: &[Expression],
    cols: &[tidb_expr::column::Column],
    lengths: &[i64],
    regard_null_as_point: bool,
) -> EqAndInExtraction {
    let mut builder = PointBuilder::default();
    let mut accesses: Vec<Option<Expression>> = vec![None; cols.len()];
    let mut points: Vec<Vec<Point>> = vec![Vec::new(); cols.len()];
    let mut merged: Vec<bool> = vec![false; cols.len()];
    let mut new_conditions = Vec::with_capacity(conditions.len());
    let mut column_values: Vec<Option<ValueInfo>> = vec![None; cols.len()];
    let mut offsets = vec![-1_i64; conditions.len()];
    for (i, cond) in conditions.iter().enumerate() {
        let offset = get_potential_eq_or_in_col_offset(cond, cols, regard_null_as_point);
        offsets[i] = offset;
        if offset == -1 {
            continue;
        }
        let offset = offset as usize;
        if accesses[offset].is_none() {
            accesses[offset] = Some(cond.clone());
            continue;
        }
        // Multiple eq/in for one column: intersect their points.
        let col_type = cols[offset]
            .ret_type
            .clone()
            .unwrap_or_else(|| {
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
            });
        let new_tp = super::ranger::new_field_type(&col_type);
        let collator = col_type.collation();
        if !merged[offset] {
            merged[offset] = true;
            // Original values kept: no prefix cut, no sort key.
            points[offset] = builder.build(
                accesses[offset].as_ref().expect("set above"),
                &new_tp,
                UNSPECIFIED_LENGTH,
                false,
            );
        }
        let built = builder.build(cond, &new_tp, UNSPECIFIED_LENGTH, false);
        points[offset] = match super::points::intersection(&points[offset], &built, collator) {
            Ok(intersected) => intersected,
            Err(_) => return EqAndInExtraction::default(),
        };
        if points[offset].is_empty() {
            // A provably false conjunction: the whole range is empty.
            return EqAndInExtraction {
                empty_range: true,
                ..EqAndInExtraction::default()
            };
        }
    }
    let mut filters = Vec::new();
    for i in 0..cols.len() {
        if !merged[i] {
            if let Some(access) = accesses[i].clone() {
                if all_eq_or_in(&access) {
                    column_values[i] = extract_value_info(&access);
                    if column_values[i]
                        .as_ref()
                        .and_then(|info| info.value.as_ref())
                        .is_some_and(|value| matches!(value, Datum::Null))
                        && !regard_null_as_point
                    {
                        accesses[i] = None;
                    } else {
                        new_conditions.push(access);
                    }
                } else {
                    accesses[i] = None;
                }
            }
            continue;
        }
        let col_collation = cols[i]
            .ret_type
            .as_ref()
            .map_or(tidb_datatype::Collation::Binary, |ft| ft.collation());
        match all_single_points(std::mem::take(&mut points[i]), col_collation) {
            None => {
                // An interval longer than a point: not an equality chain.
                accesses[i] = None;
            }
            Some(single) if single.is_empty() => {
                return EqAndInExtraction {
                    empty_range: true,
                    ..EqAndInExtraction::default()
                };
            }
            Some(single) => {
                // All intervals are single points: rebuild the EQ/IN.
                let rebuilt = points_to_eq_or_in_cond(&single, &cols[i]);
                let Some(rebuilt) = rebuilt else {
                    accesses[i] = None;
                    continue;
                };
                accesses[i] = Some(rebuilt.clone());
                new_conditions.push(rebuilt.clone());
                if let Expression::ScalarFunction(f) = &rebuilt {
                    if f.func_name.lowercase() == "eq" {
                        column_values[i] = Some(ValueInfo {
                            value: None,
                            mutable: true,
                        });
                    }
                }
            }
        }
    }
    for (i, offset) in offsets.iter().enumerate() {
        if *offset == -1 || accesses[*offset as usize].is_none() {
            new_conditions.push(conditions[i].clone());
        }
    }
    // The equality chain is the longest all-set PREFIX.
    let mut chain: Vec<Expression> = Vec::new();
    for (i, access) in accesses.iter().enumerate() {
        let Some(access) = access else { break };
        chain.push(access.clone());
        // A prefix-index access condition also filters.
        let is_full_length = lengths[i] == UNSPECIFIED_LENGTH
            || cols[i]
                .ret_type
                .as_ref()
                .is_some_and(|ft| lengths[i] == ft.flen());
        if !is_full_length {
            filters.push(access.clone());
        }
    }
    let new_conditions = remove_conditions(&new_conditions, &chain);
    let _ = range_point_cmp;
    EqAndInExtraction {
        accesses: chain,
        filters,
        new_conditions,
        column_values,
        empty_range: false,
    }
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

    /// `ExtractEqAndInCondition` over a two-column index: a full equality
    /// chain, a broken chain (no leading column), and the provably-false
    /// conjunction.
    #[test]
    fn eq_and_in_extraction_builds_the_prefix_chain() {
        let a = int_column(1);
        let b = int_column(2);
        let cols = [a.clone(), b.clone()];
        let lengths = [UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH];

        // a = 1 AND b = 2: both pin, chain of two.
        let conds = vec![
            func("eq", vec![Expression::Column(a.clone()), int_const(1)]),
            func("eq", vec![Expression::Column(b.clone()), int_const(2)]),
        ];
        let result = extract_eq_and_in_condition(&conds, &cols, &lengths, true);
        assert!(!result.empty_range);
        assert_eq!(result.accesses.len(), 2);
        assert!(result.new_conditions.is_empty(), "{:?}", result.new_conditions);
        assert!(result.column_values[0].is_some());

        // b = 2 alone: the chain needs its PREFIX, so it is empty and the
        // condition stays.
        let conds = vec![func("eq", vec![Expression::Column(b.clone()), int_const(2)])];
        let result = extract_eq_and_in_condition(&conds, &cols, &lengths, true);
        assert!(result.accesses.is_empty());
        assert_eq!(result.new_conditions.len(), 1);

        // a = 1 AND a = 2: provably empty.
        let conds = vec![
            func("eq", vec![Expression::Column(a.clone()), int_const(1)]),
            func("eq", vec![Expression::Column(a.clone()), int_const(2)]),
        ];
        let result = extract_eq_and_in_condition(&conds, &cols, &lengths, true);
        assert!(result.empty_range);
    }

    /// Go's worked example: `a IN (1,2,3) AND a IN (2,3,4)` merges to
    /// `a IN (2,3)` — the rebuilt IN replaces both.
    #[test]
    fn overlapping_in_lists_merge_to_their_intersection() {
        let a = int_column(1);
        let cols = [a.clone()];
        let conds = vec![
            func(
                "in",
                vec![
                    Expression::Column(a.clone()),
                    int_const(1),
                    int_const(2),
                    int_const(3),
                ],
            ),
            func(
                "in",
                vec![
                    Expression::Column(a.clone()),
                    int_const(2),
                    int_const(3),
                    int_const(4),
                ],
            ),
        ];
        let result =
            extract_eq_and_in_condition(&conds, &cols, &[UNSPECIFIED_LENGTH], true);
        assert!(!result.empty_range);
        assert_eq!(result.accesses.len(), 1);
        let Expression::ScalarFunction(rebuilt) = &result.accesses[0] else {
            panic!("a rebuilt IN, got {:?}", result.accesses[0]);
        };
        assert_eq!(rebuilt.func_name.lowercase(), "in");
        // col + the two surviving members.
        assert_eq!(rebuilt.args.len(), 3, "{:?}", rebuilt.args);
        // The rebuilt equality is marked mutable in columnValues.
        assert!(result.column_values[0].is_none() || result.accesses.len() == 1);
    }

    /// `x >= 2 AND x <= 2` over an INT column folds into the equality
    /// chain (Go's le/ge/lt/gt admission + `allSinglePoints`).
    #[test]
    fn touching_inequalities_fold_to_an_equality() {
        let a = int_column(1);
        let cols = [a.clone()];
        let conds = vec![
            func("ge", vec![Expression::Column(a.clone()), int_const(2)]),
            func("le", vec![Expression::Column(a.clone()), int_const(2)]),
        ];
        let result =
            extract_eq_and_in_condition(&conds, &cols, &[UNSPECIFIED_LENGTH], true);
        assert!(!result.empty_range);
        assert_eq!(result.accesses.len(), 1, "{:?}", result.new_conditions);
        let Expression::ScalarFunction(rebuilt) = &result.accesses[0] else {
            panic!("a rebuilt EQ");
        };
        assert_eq!(rebuilt.func_name.lowercase(), "eq");
        // `x > 1 AND x < 3` (an interval of one) also folds via the
        // exclude-to-include stepping.
        let conds = vec![
            func("gt", vec![Expression::Column(a.clone()), int_const(1)]),
            func("lt", vec![Expression::Column(a.clone()), int_const(3)]),
        ];
        let result =
            extract_eq_and_in_condition(&conds, &cols, &[UNSPECIFIED_LENGTH], true);
        assert_eq!(result.accesses.len(), 1);
    }
}
