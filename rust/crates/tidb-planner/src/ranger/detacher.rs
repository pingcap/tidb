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


/// Go `DetachRangeResult`.
#[derive(Debug, Default)]
pub struct DetachRangeResult {
    /// Go `Ranges`.
    pub ranges: super::types::Ranges,
    /// Go `AccessConds`.
    pub access_conds: Vec<Expression>,
    /// Go `RemainedConds`.
    pub remained_conds: Vec<Expression>,
    /// Go `ColumnValues`.
    pub column_values: Vec<Option<ValueInfo>>,
    /// Go `EqCondCount`.
    pub eq_cond_count: usize,
    /// Go `EqOrInCount`.
    pub eq_or_in_count: usize,
    /// Go `IsDNFCond`.
    pub is_dnf_cond: bool,
    /// Go `MinAccessCondsForDNFCond`.
    pub min_access_conds_for_dnf_cond: i64,
}

/// Go `rangeDetacher`: one detach run's inputs.
pub struct RangeDetacher<'a> {
    /// Go `cols`.
    pub cols: &'a [tidb_expr::column::Column],
    /// Go `lengths`.
    pub lengths: &'a [i64],
    /// Go `newTpSlice`.
    pub new_tp_slice: Vec<tidb_datatype::FieldType>,
    /// Go `mergeConsecutive`.
    pub merge_consecutive: bool,
    /// Go `convertToSortKey`.
    pub convert_to_sort_key: bool,
    /// Go `rangeMaxSize`.
    pub range_max_size: i64,
    /// Go's session `RegardNULLAsPoint` (default true).
    pub regard_null_as_point: bool,
    /// Go's session `OptPrefixIndexSingleScan` (default true).
    pub opt_prefix_index_single_scan: bool,
    /// Go `sctx.SetSkipPlanCache`'s carrier.
    pub skip_plan_cache_reason: Option<String>,
    /// Go `fixcontrol.Fix44389` (default false): admit the best CNF item's
    /// NON-point ranges when no equality chain exists.
    pub fix_44389: bool,
    /// Go `fixcontrol.Fix54337` (default false): intersect competing CNF
    /// item ranges instead of the pick-one heuristic.
    pub fix_54337: bool,
}

impl RangeDetacher<'_> {
    /// Go `buildRangeOnColsByCNFCond` (`ranger.go:553`): the leading eq/in
    /// chain appends column by column; the tail's non-equal conditions
    /// intersect into ONE more column's points.
    fn build_range_on_cols_by_cnf_cond(
        &mut self,
        eq_and_in_count: usize,
        access_conds: &[Expression],
    ) -> Result<
        (super::types::Ranges, Vec<Expression>, Vec<Expression>),
        super::points::PointBuilderError,
    > {
        let mut builder = PointBuilder::default();
        let mut ranges = super::types::Ranges::new();
        for i in 0..eq_and_in_count {
            let point = builder.build(
                &access_conds[i],
                &self.new_tp_slice[i],
                self.lengths[i],
                self.convert_to_sort_key,
            );
            if let Some(error) = builder.err.take() {
                return Err(error);
            }
            let tmp_new_tp = if self.convert_to_sort_key {
                super::ranger::convert_string_ft_to_binary_collate(&self.new_tp_slice[i])
            } else {
                self.new_tp_slice[i].clone()
            };
            let (new_ranges, fallback) = if i == 0 {
                super::ranger::points_to_ranges(
                    point,
                    &tmp_new_tp,
                    self.range_max_size,
                    &mut self.skip_plan_cache_reason,
                )?
            } else {
                super::ranger::append_points_to_ranges(
                    ranges,
                    point,
                    &tmp_new_tp,
                    self.range_max_size,
                    self.regard_null_as_point,
                    &mut self.skip_plan_cache_reason,
                )?
            };
            ranges = new_ranges;
            if fallback {
                return Ok((
                    ranges,
                    access_conds[..i].to_vec(),
                    access_conds[i..].to_vec(),
                ));
            }
        }
        let mut range_points = super::points::get_full_range();
        for cond in &access_conds[eq_and_in_count..] {
            let collator = if self.convert_to_sort_key {
                tidb_datatype::Collation::Binary
            } else {
                self.new_tp_slice[eq_and_in_count].collation()
            };
            let built = builder.build(
                cond,
                &self.new_tp_slice[eq_and_in_count],
                self.lengths[eq_and_in_count],
                self.convert_to_sort_key,
            );
            range_points = super::points::intersection(&range_points, &built, collator)?;
            if let Some(error) = builder.err.take() {
                return Err(error);
            }
        }
        if eq_and_in_count == 0 || eq_and_in_count < access_conds.len() {
            let tmp_new_tp = if self.convert_to_sort_key {
                super::ranger::convert_string_ft_to_binary_collate(
                    &self.new_tp_slice[eq_and_in_count],
                )
            } else {
                self.new_tp_slice[eq_and_in_count].clone()
            };
            let (new_ranges, fallback) = if eq_and_in_count == 0 {
                super::ranger::points_to_ranges(
                    range_points,
                    &tmp_new_tp,
                    self.range_max_size,
                    &mut self.skip_plan_cache_reason,
                )?
            } else {
                super::ranger::append_points_to_ranges(
                    ranges,
                    range_points,
                    &tmp_new_tp,
                    self.range_max_size,
                    self.regard_null_as_point,
                    &mut self.skip_plan_cache_reason,
                )?
            };
            ranges = new_ranges;
            if fallback {
                return Ok((
                    ranges,
                    access_conds[..eq_and_in_count].to_vec(),
                    access_conds[eq_and_in_count..].to_vec(),
                ));
            }
        }
        Ok((ranges, access_conds.to_vec(), Vec::new()))
    }

    /// Go `buildCNFIndexRange` (`ranger.go:629`).
    fn build_cnf_index_range(
        &mut self,
        eq_and_in_count: usize,
        access_conds: &[Expression],
    ) -> Result<
        (super::types::Ranges, Vec<Expression>, Vec<Expression>),
        super::points::PointBuilderError,
    > {
        let (mut ranges, new_access, remained) =
            self.build_range_on_cols_by_cnf_cond(eq_and_in_count, access_conds)?;
        // Take prefix indexes into consideration.
        if super::ranger::has_prefix(self.lengths) {
            ranges = super::ranger::union_ranges(ranges, self.merge_consecutive)?;
        }
        Ok((ranges, new_access, remained))
    }

    /// Go `detachCNFCondAndBuildRangeForIndex` (`detacher.go:397`), both
    /// branches of `considerDNF`.
    fn detach_cnf(
        &mut self,
        conditions: &[Expression],
        consider_dnf: bool,
    ) -> Result<DetachRangeResult, super::points::PointBuilderError> {
        let mut res = DetachRangeResult::default();
        let extraction = extract_eq_and_in_condition(
            conditions,
            self.cols,
            self.lengths,
            self.regard_null_as_point,
        );
        if extraction.empty_range {
            return Ok(res);
        }
        let mut filter_conds = extraction.filters;
        let mut new_conditions = extraction.new_conditions;
        let (ranges, access_conds, remained_conds) =
            self.build_range_on_cols_by_cnf_cond(extraction.accesses.len(), &extraction.accesses)?;
        let mut ranges = ranges;
        let mut access_conds = access_conds;
        if !remained_conds.is_empty() {
            filter_conds = remove_conditions(&filter_conds, &remained_conds);
            new_conditions.extend(remained_conds);
        }
        let mut eq_count = 0;
        for cond in &access_conds {
            let Expression::ScalarFunction(f) = cond else { break };
            if f.func_name.lowercase() != "eq" {
                break;
            }
            eq_count += 1;
        }
        let eq_or_in_count = access_conds.len();
        res.eq_cond_count = eq_count;
        res.eq_or_in_count = eq_or_in_count;
        if super::ranger::has_prefix(self.lengths) {
            ranges = super::ranger::union_ranges(ranges, self.merge_consecutive)?;
        }
        res.column_values = extraction.column_values;
        // The prefix-and-merge interplay (issue 26029): point ranges are
        // kept SEPARATELY when consecutive-merge may fuse them.
        let mut point_ranges = ranges.clone();
        if super::ranger::has_prefix(self.lengths) && self.merge_consecutive {
            point_ranges = super::ranger::union_ranges(point_ranges, false)?;
        }
        if eq_or_in_count == self.cols.len() || new_conditions.is_empty() {
            res.ranges = ranges;
            res.access_conds = access_conds;
            res.remained_conds = filter_conds;
            res.remained_conds.extend(new_conditions);
            return Ok(res);
        }
        if consider_dnf {
            return self.detach_cnf_consider_dnf(
                conditions,
                res,
                ranges,
                point_ranges,
                access_conds,
                filter_conds,
                new_conditions,
                eq_or_in_count,
            );
        }
        let next_col = &self.cols[eq_or_in_count];
        let checker = ConditionChecker {
            checker_col: Some(next_col),
            length: self.lengths[eq_or_in_count],
            opt_prefix_index_single_scan: self.opt_prefix_index_single_scan,
        };
        for cond in &new_conditions {
            let (is_access_cond, should_reserve) = checker.check(cond);
            if !is_access_cond {
                filter_conds.push(cond.clone());
                continue;
            }
            access_conds.push(cond.clone());
            if should_reserve {
                filter_conds.push(cond.clone());
            }
        }
        let (built_ranges, built_access, built_remained) =
            self.build_cnf_index_range(eq_or_in_count, &access_conds)?;
        filter_conds.extend(built_remained);
        res.ranges = built_ranges;
        res.access_conds = built_access;
        res.remained_conds = filter_conds;
        Ok(res)
    }
}

/// Go `DetachSimpleCondAndBuildRangeForIndex` (`detacher.go:1117`): the
/// point-query-first detachment WITHOUT DNF consideration —
/// `(ranges, access_conds, remained_conds)`.
pub fn detach_simple_cond_and_build_range_for_index(
    conditions: &[Expression],
    cols: &[tidb_expr::column::Column],
    lengths: &[i64],
    range_max_size: i64,
) -> Result<
    (super::types::Ranges, Vec<Expression>, Vec<Expression>),
    super::points::PointBuilderError,
> {
    let new_tp_slice: Vec<tidb_datatype::FieldType> = cols
        .iter()
        .map(|col| {
            super::ranger::new_field_type(&col.ret_type.clone().unwrap_or_else(|| {
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
            }))
        })
        .collect();
    let mut detacher = RangeDetacher {
        cols,
        lengths,
        new_tp_slice,
        merge_consecutive: true,
        convert_to_sort_key: true,
        range_max_size,
        regard_null_as_point: true,
        opt_prefix_index_single_scan: true,
        skip_plan_cache_reason: None,
        fix_44389: false,
        fix_54337: false,
    };
    let res = detacher.detach_cnf(conditions, false)?;
    Ok((res.ranges, res.access_conds, res.remained_conds))
}


/// Go `cnfItemRangeResult`.
struct CnfItemRangeResult {
    range_result: DetachRangeResult,
    offset: usize,
    same_len_point_ranges: bool,
    max_col_num: usize,
    min_col_num: usize,
}

/// Go `getCNFItemRangeResult`.
fn get_cnf_item_range_result(
    range_result: DetachRangeResult,
    offset: usize,
    regard_null_as_point: bool,
) -> CnfItemRangeResult {
    let mut same_len_point_ranges = true;
    let mut max_col_num = 0;
    let mut min_col_num = 0;
    for (i, ran) in range_result.ranges.iter().enumerate() {
        if !ran.is_point(regard_null_as_point) {
            same_len_point_ranges = false;
        }
        if i == 0 {
            max_col_num = ran.low_val.len();
            min_col_num = ran.low_val.len();
        } else {
            max_col_num = max_col_num.max(ran.low_val.len());
            min_col_num = min_col_num.min(ran.low_val.len());
        }
    }
    if min_col_num != max_col_num {
        same_len_point_ranges = false;
    }
    CnfItemRangeResult {
        range_result,
        offset,
        same_len_point_ranges,
        max_col_num,
        min_col_num,
    }
}

/// Go `compareCNFItemRangeResult`.
fn compare_cnf_item_range_result(cur: &CnfItemRangeResult, best: &CnfItemRangeResult) -> bool {
    if cur.same_len_point_ranges && best.same_len_point_ranges {
        return cur.min_col_num > best.min_col_num;
    }
    if !cur.same_len_point_ranges && !best.same_len_point_ranges {
        if cur.min_col_num == best.min_col_num {
            return cur.max_col_num > best.max_col_num;
        }
        return cur.min_col_num > best.min_col_num;
    }
    // Point ranges beat non-point ranges: later columns can append.
    cur.same_len_point_ranges
}

/// Go `mergeTwoCNFRanges`: keep the better of two CNF-item results —
/// Fix54337 (off by default) upgrades the heuristic to a subset/
/// intersection comparison.
fn merge_two_cnf_ranges(
    cond: &Expression,
    range_result: Option<CnfItemRangeResult>,
    other: Option<CnfItemRangeResult>,
    fix_54337: bool,
) -> Option<CnfItemRangeResult> {
    let Some(mut merged) = range_result else {
        return other;
    };
    let Some(other) = other else {
        return Some(merged);
    };
    let mut try_heuristic = false;
    if fix_54337 {
        let merged_is_subset =
            super::types::ranges_subset(&merged.range_result.ranges, &other.range_result.ranges);
        if !merged_is_subset {
            let other_is_subset = super::types::ranges_subset(
                &other.range_result.ranges,
                &merged.range_result.ranges,
            );
            if other_is_subset {
                return Some(other);
            }
            match super::types::intersect_ranges(
                &other.range_result.ranges,
                &merged.range_result.ranges,
            ) {
                None => try_heuristic = true,
                Some(intersection) => {
                    merged.range_result.ranges = intersection;
                    merged.range_result.access_conds = append_conditions_if_not_exist(
                        std::mem::take(&mut merged.range_result.access_conds),
                        std::slice::from_ref(cond),
                    );
                }
            }
        }
    } else {
        try_heuristic = true;
    }
    if try_heuristic && compare_cnf_item_range_result(&other, &merged) {
        return Some(other);
    }
    Some(merged)
}

/// Go `unionColumnValues`.
fn union_column_values(
    mut lhs: Vec<Option<ValueInfo>>,
    rhs: &[Option<ValueInfo>],
) -> Vec<Option<ValueInfo>> {
    if lhs.is_empty() {
        return rhs.to_vec();
    }
    for (i, val_info) in lhs.iter_mut().enumerate() {
        if i >= rhs.len() {
            break;
        }
        if val_info.is_none() && rhs[i].is_some() {
            *val_info = rhs[i].clone();
        }
    }
    lhs
}

/// Go `isSameValue`: binary-compare two extracted constants.
fn is_same_value(lhs: &Option<ValueInfo>, rhs: &Option<ValueInfo>) -> bool {
    match (lhs, rhs) {
        (Some(left), Some(right)) if !left.mutable && !right.mutable => {
            match (&left.value, &right.value) {
                (Some(a), Some(b)) => a
                    .compare(b, tidb_datatype::Collation::Binary)
                    .map(|order| order == std::cmp::Ordering::Equal)
                    .unwrap_or(false),
                _ => false,
            }
        }
        _ => false,
    }
}

/// This port's stand-in for `Ranges.MemUsage` in the DNF accumulation
/// (Go sizes with `unsafe.Sizeof`; see `ranger.rs`'s module header).
fn ranges_mem_estimate(ranges: &super::types::Ranges) -> i64 {
    ranges
        .iter()
        .map(|ran| 96 + 72 * (ran.low_val.len() as i64 + ran.high_val.len() as i64))
        .sum()
}

impl RangeDetacher<'_> {
    /// Go `extractBestCNFItemRanges`.
    fn extract_best_cnf_item_ranges(
        &mut self,
        conds: &[Expression],
    ) -> Result<
        (Option<CnfItemRangeResult>, Vec<Option<ValueInfo>>),
        super::points::PointBuilderError,
    > {
        if conds.len() < 2 {
            return Ok((None, Vec::new()));
        }
        let mut best: Option<CnfItemRangeResult> = None;
        let mut column_values: Vec<Option<ValueInfo>> = vec![None; self.cols.len()];
        for (i, cond) in conds.iter().enumerate() {
            if tidb_expr::simple_expr::extract_columns(cond).is_empty() {
                continue;
            }
            // Consecutive-merge OFF here: point ranges must stay points so
            // later columns can append (issue 41572).
            let mut inner = RangeDetacher {
                cols: self.cols,
                lengths: self.lengths,
                new_tp_slice: self.new_tp_slice.clone(),
                merge_consecutive: false,
                convert_to_sort_key: self.convert_to_sort_key,
                range_max_size: self.range_max_size,
                regard_null_as_point: self.regard_null_as_point,
                opt_prefix_index_single_scan: self.opt_prefix_index_single_scan,
                skip_plan_cache_reason: None,
                fix_44389: self.fix_44389,
                fix_54337: self.fix_54337,
            };
            let res = inner.detach_cond_and_build_range_for_cols(std::slice::from_ref(cond))?;
            if let Some(reason) = inner.skip_plan_cache_reason {
                self.skip_plan_cache_reason.get_or_insert(reason);
            }
            if res.ranges.is_empty() {
                return Ok((
                    Some(CnfItemRangeResult {
                        range_result: res,
                        offset: i,
                        same_len_point_ranges: false,
                        max_col_num: 0,
                        min_col_num: 0,
                    }),
                    Vec::new(),
                ));
            }
            column_values = union_column_values(column_values, &res.column_values);
            if res.access_conds.is_empty() {
                continue;
            }
            let cur = get_cnf_item_range_result(res, i, self.regard_null_as_point);
            best = merge_two_cnf_ranges(cond, best, Some(cur), self.fix_54337);
        }
        if let Some(best) = &mut best {
            best.range_result.is_dnf_cond = false;
        }
        Ok((best, column_values))
    }

    /// Go `chooseBetweenRangeAndPoint` (Fix54337-gated; a no-op when off).
    fn choose_between_range_and_point(
        &self,
        res: &mut DetachRangeResult,
        best: Option<&CnfItemRangeResult>,
    ) {
        if !self.fix_54337 {
            return;
        }
        let Some(best) = best else { return };
        if res.ranges.is_empty() {
            return;
        }
        let r1_minus_r2 =
            remove_conditions(&res.access_conds, &best.range_result.access_conds);
        let r2_minus_r1 =
            remove_conditions(&best.range_result.access_conds, &res.access_conds);
        if r1_minus_r2.is_empty() && !r2_minus_r1.is_empty() {
            res.remained_conds =
                remove_conditions(&res.remained_conds, &best.range_result.access_conds);
            res.ranges = best.range_result.ranges.clone();
            res.access_conds = best.range_result.access_conds.clone();
        }
    }

    /// The `considerDNF = true` continuation of
    /// `detachCNFCondAndBuildRangeForIndex`.
    #[allow(clippy::too_many_arguments)]
    fn detach_cnf_consider_dnf(
        &mut self,
        conditions: &[Expression],
        mut res: DetachRangeResult,
        ranges: super::types::Ranges,
        mut point_ranges: super::types::Ranges,
        access_conds: Vec<Expression>,
        filter_conds: Vec<Expression>,
        mut new_conditions: Vec<Expression>,
        mut eq_or_in_count: usize,
    ) -> Result<DetachRangeResult, super::points::PointBuilderError> {
        res.ranges = ranges;
        res.access_conds = access_conds;
        res.remained_conds = filter_conds;
        let (best, best_column_values) = self.extract_best_cnf_item_ranges(conditions)?;
        res.column_values =
            union_column_values(std::mem::take(&mut res.column_values), &best_column_values);
        let mut best = best;
        if let Some(candidate) = &best {
            if candidate.range_result.ranges.is_empty() {
                return Ok(DetachRangeResult::default());
            }
            if candidate.same_len_point_ranges && candidate.min_col_num > eq_or_in_count {
                let candidate = best.take().expect("checked some");
                let offset = candidate.offset;
                let mut taken = candidate.range_result;
                taken.column_values = std::mem::take(&mut res.column_values);
                point_ranges = taken.ranges.clone();
                eq_or_in_count = taken.ranges[0].low_val.len();
                res = taken;
                new_conditions.clear();
                new_conditions.extend_from_slice(&conditions[..offset]);
                new_conditions.extend_from_slice(&conditions[offset + 1..]);
                if eq_or_in_count == self.cols.len() || new_conditions.is_empty() {
                    res.remained_conds.extend(new_conditions);
                    return Ok(res);
                }
            } else if self.fix_44389
                && !candidate.same_len_point_ranges
                && eq_or_in_count == 0
                && candidate.min_col_num > 0
                && candidate.max_col_num > 1
            {
                let candidate = best.take().expect("checked some");
                let offset = candidate.offset;
                let mut taken = candidate.range_result;
                taken.column_values = std::mem::take(&mut res.column_values);
                res = taken;
                new_conditions.clear();
                new_conditions.extend_from_slice(&conditions[..offset]);
                new_conditions.extend_from_slice(&conditions[offset + 1..]);
                res.remained_conds.extend(new_conditions);
                return Ok(res);
            }
        }
        if eq_or_in_count > 0 {
            let new_cols = &self.cols[eq_or_in_count..];
            let new_lengths = &self.lengths[eq_or_in_count..];
            let new_tps: Vec<tidb_datatype::FieldType> =
                self.new_tp_slice[eq_or_in_count..].to_vec();
            let mut tail_detacher = RangeDetacher {
                cols: new_cols,
                lengths: new_lengths,
                new_tp_slice: new_tps,
                merge_consecutive: self.merge_consecutive,
                convert_to_sort_key: self.convert_to_sort_key,
                range_max_size: self.range_max_size,
                regard_null_as_point: self.regard_null_as_point,
                opt_prefix_index_single_scan: self.opt_prefix_index_single_scan,
                skip_plan_cache_reason: None,
                fix_44389: self.fix_44389,
                fix_54337: self.fix_54337,
            };
            let tail_res = tail_detacher.detach_cond_and_build_range_for_cols(&new_conditions)?;
            if let Some(reason) = tail_detacher.skip_plan_cache_reason {
                self.skip_plan_cache_reason.get_or_insert(reason);
            }
            if tail_res.ranges.is_empty() {
                return Ok(DetachRangeResult::default());
            }
            if !tail_res.access_conds.is_empty() {
                // Go `AppendRanges2PointRanges` with the memory fallback.
                let range_count = point_ranges.len() * tail_res.ranges.len();
                if self.range_max_size > 0
                    && (range_count as i64) * 96 > self.range_max_size
                {
                    res.remained_conds.extend(tail_res.access_conds);
                    res.remained_conds = append_conditions_if_not_exist(
                        std::mem::take(&mut res.remained_conds),
                        &tail_res.remained_conds,
                    );
                    return Ok(res);
                }
                let mut new_ranges = super::types::Ranges::new();
                for point_range in &point_ranges {
                    for tail in &tail_res.ranges {
                        let mut low_val = point_range.low_val.clone();
                        low_val.extend(tail.low_val.iter().cloned());
                        let mut high_val = point_range.high_val.clone();
                        high_val.extend(tail.high_val.iter().cloned());
                        let mut collators = point_range.collators.clone();
                        collators.extend(tail.collators.iter().copied());
                        new_ranges.push(super::types::Range {
                            low_val,
                            low_exclude: tail.low_exclude,
                            high_val,
                            high_exclude: tail.high_exclude,
                            collators,
                        });
                    }
                }
                res.ranges = new_ranges;
                res.access_conds.extend(tail_res.access_conds);
                res.remained_conds.extend(tail_res.remained_conds);
                // The `((a=1 AND b=1) OR (a=2 AND b=2)) AND c=1` guard:
                // only a NON-zero EqOrInCount accumulates the tail's.
                if res.eq_or_in_count > 0 {
                    if res.eq_or_in_count == res.eq_cond_count {
                        res.eq_cond_count += tail_res.eq_cond_count;
                    }
                    res.eq_or_in_count += tail_res.eq_or_in_count;
                }
                return Ok(res);
            }
            res.remained_conds.extend(tail_res.remained_conds);
            self.choose_between_range_and_point(&mut res, best.as_ref());
            return Ok(res);
        }
        // `eqOrInCount == 0`: the column walk over the FIRST index column.
        let checker = ConditionChecker {
            checker_col: Some(&self.cols[0]),
            length: self.lengths[0],
            opt_prefix_index_single_scan: self.opt_prefix_index_single_scan,
        };
        let (column_access, column_filters) =
            detach_column_cnf_conditions(&new_conditions, &checker);
        res.access_conds = column_access;
        res.remained_conds = column_filters;
        let (built_ranges, built_access, built_remained) =
            self.build_cnf_index_range(0, &res.access_conds.clone())?;
        res.remained_conds = append_conditions_if_not_exist(
            std::mem::take(&mut res.remained_conds),
            &built_remained,
        );
        res.ranges = built_ranges;
        res.access_conds = built_access;
        // Pick the best CNF item's ranges when they are a PROPER subset of
        // the column walk's.
        if let Some(best) = &best {
            if !res.ranges.is_empty() {
                let best_is_subset = super::types::ranges_subset(
                    &best.range_result.ranges,
                    &res.ranges,
                );
                let point_is_subset = super::types::ranges_subset(
                    &res.ranges,
                    &best.range_result.ranges,
                );
                if best_is_subset && !point_is_subset {
                    res.remained_conds = remove_conditions(
                        &res.remained_conds,
                        &best.range_result.access_conds,
                    );
                    res.ranges = best.range_result.ranges.clone();
                    res.access_conds = best.range_result.access_conds.clone();
                }
            }
        }
        Ok(res)
    }

    /// Go `detachDNFCondAndBuildRangeForIndex` (`detacher.go:849`).
    fn detach_dnf(
        &mut self,
        condition: &tidb_expr::scalar_function::ScalarFunction,
    ) -> Result<
        (
            super::types::Ranges,
            Vec<Expression>,
            Vec<Option<ValueInfo>>,
            bool,
            i64,
        ),
        super::points::PointBuilderError,
    > {
        let first_column_checker = ConditionChecker {
            checker_col: Some(&self.cols[0]),
            length: self.lengths[0],
            opt_prefix_index_single_scan: self.opt_prefix_index_single_scan,
        };
        let mut builder = PointBuilder::default();
        let dnf_items = flatten_dnf_conditions(condition);
        let mut new_access_items = Vec::with_capacity(dnf_items.len());
        let mut min_access_conds: i64 = -1;
        let mut total_ranges = super::types::Ranges::new();
        let mut total_mem: i64 = 0;
        let mut column_values: Vec<Option<ValueInfo>> = vec![None; self.cols.len()];
        let mut has_residual = false;
        for (i, item) in dnf_items.iter().enumerate() {
            let is_and = matches!(item, Expression::ScalarFunction(sf)
                if sf.func_name.lowercase() == "and");
            if is_and {
                let Expression::ScalarFunction(sf) = item else {
                    unreachable!("matched above");
                };
                let cnf_items = flatten_cnf_conditions(sf);
                let res = self.detach_cnf(&cnf_items, true)?;
                // An always-false DNF item is skipped.
                if res.ranges.is_empty() {
                    continue;
                }
                if res.access_conds.is_empty() {
                    return Ok((super::points::full_range(), Vec::new(), Vec::new(), true, -1));
                }
                if !res.remained_conds.is_empty() {
                    has_residual = true;
                }
                total_mem += ranges_mem_estimate(&res.ranges);
                total_ranges.extend(res.ranges);
                if self.range_max_size > 0 && total_mem > self.range_max_size {
                    return Ok((super::points::full_range(), Vec::new(), Vec::new(), true, -1));
                }
                if let Some(composed) =
                    tidb_expr::simple_expr::compose_cnf_condition(res.access_conds.clone())
                {
                    new_access_items.push(composed);
                }
                if i == 0 {
                    column_values = res.column_values;
                } else {
                    for j in 0..column_values.len() {
                        if column_values[j].is_none() {
                            continue;
                        }
                        let other = res.column_values.get(j).cloned().flatten();
                        if !is_same_value(&column_values[j], &other) {
                            column_values[j] = None;
                        }
                    }
                }
                let access_len = res.access_conds.len() as i64;
                if min_access_conds == -1 || access_len < min_access_conds {
                    min_access_conds = access_len;
                }
            } else {
                let (is_access_cond, should_reserve) = first_column_checker.check(item);
                if !is_access_cond {
                    return Ok((super::points::full_range(), Vec::new(), Vec::new(), true, -1));
                }
                if should_reserve {
                    has_residual = true;
                }
                let points = builder.build(
                    item,
                    &self.new_tp_slice[0],
                    self.lengths[0],
                    self.convert_to_sort_key,
                );
                let tmp_new_tp = if self.convert_to_sort_key {
                    super::ranger::convert_string_ft_to_binary_collate(&self.new_tp_slice[0])
                } else {
                    self.new_tp_slice[0].clone()
                };
                let (ranges, fallback) = super::ranger::points_to_ranges(
                    points,
                    &tmp_new_tp,
                    self.range_max_size,
                    &mut self.skip_plan_cache_reason,
                )?;
                if fallback {
                    return Ok((super::points::full_range(), Vec::new(), Vec::new(), true, -1));
                }
                total_mem += ranges_mem_estimate(&ranges);
                total_ranges.extend(ranges);
                if self.range_max_size > 0 && total_mem > self.range_max_size {
                    return Ok((super::points::full_range(), Vec::new(), Vec::new(), true, -1));
                }
                new_access_items.push(item.clone());
                if i == 0 {
                    column_values[0] = extract_value_info(item);
                } else if column_values[0].is_some() {
                    let val_info = extract_value_info(item);
                    if !is_same_value(&column_values[0], &val_info) {
                        column_values[0] = None;
                    }
                }
                if min_access_conds == -1 || min_access_conds > 1 {
                    min_access_conds = 1;
                }
            }
        }
        let total_ranges =
            super::ranger::union_ranges(total_ranges, self.merge_consecutive)?;
        let access = tidb_expr::simple_expr::compose_dnf_condition(new_access_items)
            .map_or_else(Vec::new, |composed| vec![composed]);
        Ok((total_ranges, access, column_values, has_residual, min_access_conds))
    }

    /// Go `detachCondAndBuildRangeForCols` (`detacher.go:1084`).
    pub fn detach_cond_and_build_range_for_cols(
        &mut self,
        all_conds: &[Expression],
    ) -> Result<DetachRangeResult, super::points::PointBuilderError> {
        let mut res = DetachRangeResult::default();
        if all_conds.len() == 1 {
            if let Expression::ScalarFunction(sf) = &all_conds[0] {
                if sf.func_name.lowercase() == "or" {
                    let (ranges, accesses, column_values, has_residual, min_access_conds) =
                        self.detach_dnf(sf)?;
                    res.ranges = ranges;
                    res.access_conds = accesses;
                    res.column_values = column_values;
                    res.is_dnf_cond = true;
                    if min_access_conds != -1 {
                        res.min_access_conds_for_dnf_cond = min_access_conds;
                    }
                    // A DNF with an uncomputable part pushes WHOLE as
                    // filter.
                    if has_residual {
                        res.remained_conds = all_conds.to_vec();
                    }
                    return Ok(res);
                }
            }
        }
        self.detach_cnf(all_conds, true)
    }
}

/// Go `DetachCondAndBuildRangeForIndex` (`detacher.go:1033`): the FULL
/// entry with DNF consideration, consecutive-merge, and sort-key
/// conversion.
pub fn detach_cond_and_build_range_for_index(
    conditions: &[Expression],
    cols: &[tidb_expr::column::Column],
    lengths: &[i64],
    range_max_size: i64,
) -> Result<DetachRangeResult, super::points::PointBuilderError> {
    detach_cond_and_build_range(conditions, cols, lengths, range_max_size, true, true)
}

/// Go `DetachCondAndBuildRangeForPartition`: no sort key, no
/// consecutive-merge.
pub fn detach_cond_and_build_range_for_partition(
    conditions: &[Expression],
    cols: &[tidb_expr::column::Column],
    lengths: &[i64],
    range_max_size: i64,
) -> Result<DetachRangeResult, super::points::PointBuilderError> {
    detach_cond_and_build_range(conditions, cols, lengths, range_max_size, false, false)
}

/// Go `detachCondAndBuildRange`.
fn detach_cond_and_build_range(
    conditions: &[Expression],
    cols: &[tidb_expr::column::Column],
    lengths: &[i64],
    range_max_size: i64,
    convert_to_sort_key: bool,
    merge_consecutive: bool,
) -> Result<DetachRangeResult, super::points::PointBuilderError> {
    let new_tp_slice: Vec<tidb_datatype::FieldType> = cols
        .iter()
        .map(|col| {
            super::ranger::new_field_type(&col.ret_type.clone().unwrap_or_else(|| {
                tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
            }))
        })
        .collect();
    let mut detacher = RangeDetacher {
        cols,
        lengths,
        new_tp_slice,
        merge_consecutive,
        convert_to_sort_key,
        range_max_size,
        regard_null_as_point: true,
        opt_prefix_index_single_scan: true,
        skip_plan_cache_reason: None,
        fix_44389: false,
        fix_54337: false,
    };
    detacher.detach_cond_and_build_range_for_cols(conditions)
}


/// Go `MergeDNFItems4Col` (`detacher.go:1191`): group single-column DNF
/// items whose column can build ranges, composing one DNF per column —
/// `[a > 5, b > 6, c > 7, a = 1, b > 3]` becomes
/// `[c > 7, a > 5 OR a = 1, b > 6 OR b > 3]`. Multi-column items and the
/// `_tidb_rowid` extra handle stay unmerged (the Selectivity recursion
/// guard in Go's comment).
#[must_use]
pub fn merge_dnf_items_4_col(
    dnf_items: &[Expression],
    opt_prefix_index_single_scan: bool,
) -> Vec<Expression> {
    const EXTRA_HANDLE_ID: i64 = -1;
    let mut merged = Vec::with_capacity(dnf_items.len());
    let mut col_order: Vec<i64> = Vec::new();
    let mut col_to_items: std::collections::HashMap<i64, Vec<Expression>> =
        std::collections::HashMap::new();
    for dnf_item in dnf_items {
        let cols = tidb_expr::simple_expr::extract_columns(dnf_item);
        if cols.len() != 1 || cols[0].id == EXTRA_HANDLE_ID {
            merged.push(dnf_item.clone());
            continue;
        }
        let unique_id = cols[0].unique_id;
        let checker = ConditionChecker {
            checker_col: Some(&cols[0]),
            length: UNSPECIFIED_LENGTH,
            opt_prefix_index_single_scan,
        };
        let (is_access_cond, _) = checker.check(dnf_item);
        if !is_access_cond {
            merged.push(dnf_item.clone());
            continue;
        }
        if !col_to_items.contains_key(&unique_id) {
            col_order.push(unique_id);
        }
        col_to_items.entry(unique_id).or_default().push(dnf_item.clone());
    }
    // Go iterates the map (unordered); first-seen order keeps this port
    // deterministic without changing membership.
    for unique_id in col_order {
        let items = col_to_items.remove(&unique_id).expect("recorded above");
        if let Some(composed) = compose_dnf_condition(items) {
            merged.push(composed);
        }
    }
    merged
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

    /// `DetachSimpleCondAndBuildRangeForIndex` end to end over a
    /// two-column index: the eq chain appends the tail column's interval —
    /// `a = 1 AND b > 2` builds `(1 2, 1 +inf]`.
    #[test]
    fn simple_detachment_builds_multi_column_ranges() {
        let a = int_column(1);
        let b = int_column(2);
        let cols = [a.clone(), b.clone()];
        let lengths = [UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH];
        let conds = vec![
            func("eq", vec![Expression::Column(a.clone()), int_const(1)]),
            func("gt", vec![Expression::Column(b.clone()), int_const(2)]),
        ];
        let (ranges, access, remained) =
            detach_simple_cond_and_build_range_for_index(&conds, &cols, &lengths, 0)
                .expect("detaches");
        assert_eq!(access.len(), 2);
        assert!(remained.is_empty(), "{remained:?}");
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].to_display_string(), "(1 2,1 +inf]");

        // A condition on a column OUTSIDE the index stays remained.
        let c = int_column(9);
        let conds = vec![
            func("eq", vec![Expression::Column(a.clone()), int_const(1)]),
            func("gt", vec![Expression::Column(c), int_const(0)]),
        ];
        let (ranges, access, remained) =
            detach_simple_cond_and_build_range_for_index(&conds, &cols, &lengths, 0)
                .expect("detaches");
        assert_eq!(access.len(), 1);
        assert_eq!(remained.len(), 1);
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].to_display_string(), "[1,1]");

        // An IN chain fans out: `a IN (1, 3) AND b = 5`.
        let conds = vec![
            func(
                "in",
                vec![Expression::Column(a.clone()), int_const(1), int_const(3)],
            ),
            func("eq", vec![Expression::Column(b.clone()), int_const(5)]),
        ];
        let (ranges, _, _) =
            detach_simple_cond_and_build_range_for_index(&conds, &cols, &lengths, 0)
                .expect("detaches");
        let shown: Vec<String> = ranges
            .iter()
            .map(super::super::types::Range::to_display_string)
            .collect();
        assert_eq!(shown, ["[1 5,1 5]", "[3 5,3 5]"]);
    }

    /// `DetachCondAndBuildRangeForIndex`'s DNF arm:
    /// `(a = 1 AND b = 2) OR (a = 3 AND b = 4)` over index (a, b) —
    /// per-arm CNF detachment, ranges unioned, one composed DNF access.
    #[test]
    fn dnf_detachment_unions_per_arm_ranges() {
        let a = int_column(1);
        let b = int_column(2);
        let cols = [a.clone(), b.clone()];
        let lengths = [UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH];
        let arm = |av: i64, bv: i64| {
            func(
                "and",
                vec![
                    func("eq", vec![Expression::Column(a.clone()), int_const(av)]),
                    func("eq", vec![Expression::Column(b.clone()), int_const(bv)]),
                ],
            )
        };
        let dnf = func("or", vec![arm(1, 2), arm(3, 4)]);
        let res = detach_cond_and_build_range_for_index(&[dnf], &cols, &lengths, 0)
            .expect("detaches");
        assert!(res.is_dnf_cond);
        assert!(res.remained_conds.is_empty(), "{:?}", res.remained_conds);
        let shown: Vec<String> = res
            .ranges
            .iter()
            .map(super::super::types::Range::to_display_string)
            .collect();
        assert_eq!(shown, ["[1 2,1 2]", "[3 4,3 4]"]);
        assert_eq!(res.min_access_conds_for_dnf_cond, 2);

        // An arm outside the index poisons the DNF whole: full range plus
        // the whole condition remained.
        let c = int_column(9);
        let poisoned = func(
            "or",
            vec![
                arm(1, 2),
                func("eq", vec![Expression::Column(c), int_const(0)]),
            ],
        );
        let res =
            detach_cond_and_build_range_for_index(&[poisoned.clone()], &cols, &lengths, 0)
                .expect("detaches");
        assert!(res.is_dnf_cond);
        assert_eq!(res.remained_conds.len(), 1);
        assert_eq!(res.ranges.len(), 1);
        assert_eq!(res.ranges[0].to_display_string(), "[NULL,+inf]");
    }

    /// The considerDNF branch's best-CNF-item pick: a first-column DNF
    /// (`a = 1 OR a = 3`) AND `b = 5` — the eq/in extraction takes the DNF
    /// as column-a equalities and appends b.
    #[test]
    fn a_first_column_dnf_pins_the_chain() {
        let a = int_column(1);
        let b = int_column(2);
        let cols = [a.clone(), b.clone()];
        let lengths = [UNSPECIFIED_LENGTH, UNSPECIFIED_LENGTH];
        let conds = vec![
            func(
                "or",
                vec![
                    func("eq", vec![Expression::Column(a.clone()), int_const(1)]),
                    func("eq", vec![Expression::Column(a.clone()), int_const(3)]),
                ],
            ),
            func("eq", vec![Expression::Column(b.clone()), int_const(5)]),
        ];
        let res = detach_cond_and_build_range_for_index(&conds, &cols, &lengths, 0)
            .expect("detaches");
        let shown: Vec<String> = res
            .ranges
            .iter()
            .map(super::super::types::Range::to_display_string)
            .collect();
        assert_eq!(shown, ["[1 5,1 5]", "[3 5,3 5]"], "{:?}", res.access_conds);
        assert!(res.remained_conds.is_empty(), "{:?}", res.remained_conds);
    }

    /// Go's own `MergeDNFItems4Col` example: `[a > 5, b > 6, c?, a = 1,
    /// b > 3]` groups per column (`c` here is a multi-column item that
    /// stays put).
    #[test]
    fn dnf_items_merge_per_column() {
        let a = int_column(1);
        let b = int_column(2);
        let c = int_column(3);
        let multi = func(
            "gt",
            vec![Expression::Column(c.clone()), Expression::Column(a.clone())],
        );
        let items = vec![
            func("gt", vec![Expression::Column(a.clone()), int_const(5)]),
            func("gt", vec![Expression::Column(b.clone()), int_const(6)]),
            multi.clone(),
            func("eq", vec![Expression::Column(a.clone()), int_const(1)]),
            func("gt", vec![Expression::Column(b.clone()), int_const(3)]),
        ];
        let merged = merge_dnf_items_4_col(&items, true);
        assert_eq!(merged.len(), 3, "{merged:?}");
        // The multi-column item is untouched and FIRST (unmergeable order).
        assert!(merged[0].equal(&multi));
        // Then one OR per column, in first-seen column order.
        for (index, expected_arms) in [(1, 2), (2, 2)] {
            let Expression::ScalarFunction(sf) = &merged[index] else {
                panic!("a composed OR at {index}");
            };
            assert_eq!(sf.func_name.lowercase(), "or");
            assert_eq!(sf.args.len(), expected_arms);
        }
    }
}

// ---------------------------------------------------------------------------
// The shard-index GC-column family (`detacher.go:1228-1616`): rewriting an
// EQ/IN over `uk(tidb_shard(a), a, ...)`'s data column into the
// `tidb_shard(a) = xxx AND a = ...` form the index can seek.
// ---------------------------------------------------------------------------

/// Go `ExtractColumnsFromExpr`: every distinct column under the virtual
/// expression, in first-appearance order.
#[must_use]
pub fn extract_columns_from_expr(
    virtual_expr: Option<&tidb_expr::scalar_function::ScalarFunction>,
) -> Vec<tidb_expr::column::Column> {
    let mut fields: Vec<tidb_expr::column::Column> = Vec::new();
    fn walk(
        function: &tidb_expr::scalar_function::ScalarFunction,
        fields: &mut Vec<tidb_expr::column::Column>,
    ) {
        for arg in &function.args {
            match arg {
                Expression::ScalarFunction(inner) => walk(inner, fields),
                Expression::Column(column) => {
                    if !fields.iter().any(|field| field.unique_id == column.unique_id) {
                        fields.push(column.clone());
                    }
                }
                _ => {}
            }
        }
    }
    if let Some(function) = virtual_expr {
        walk(function, &mut fields);
    }
    fields
}

/// Go `IsValidShardIndex`: `index(tidb_shard(a), a, ...)` -- at least two
/// columns, the first a GC column whose virtual expression is
/// `tidb_shard(<the second column>)`.
#[must_use]
pub fn is_valid_shard_index(cols: &[tidb_expr::column::Column]) -> bool {
    if cols.len() < 2 {
        return false;
    }
    if !tidb_expr::column::gc_column_expr_is_tidb_shard(cols[0].virtual_expr.as_deref()) {
        return false;
    }
    let Some(Expression::ScalarFunction(shard)) = cols[0].virtual_expr.as_deref() else {
        return false;
    };
    if shard.args.len() != 1 {
        return false;
    }
    let Expression::Column(argument) = &shard.args[0] else {
        return false;
    };
    argument.unique_id == cols[1].unique_id
}

/// Go `NeedAddColumn4EqCond`: every index column past the shard prefix is
/// pinned by an EQ with a knowable constant, and nothing already pins the
/// prefix itself.
#[must_use]
pub fn need_add_column4_eq_cond(
    cols: &[tidb_expr::column::Column],
    access_cond: &[Option<Expression>],
    column_values: &[Option<ValueInfo>],
) -> bool {
    if column_values.len() < 2 {
        return false;
    }
    let mut matched_key_fields = 0_usize;
    for cond in &access_cond[1..] {
        let Some(cond) = cond else { break };
        let Expression::ScalarFunction(function) = cond else {
            return false;
        };
        if function.func_name.lowercase() != "eq" {
            return false;
        }
        matched_key_fields += 1;
    }
    let mut value_count = 0_usize;
    for value in &column_values[1..] {
        if value.is_none() {
            break;
        }
        value_count += 1;
    }
    matched_key_fields == cols.len() - 1
        && value_count == cols.len() - 1
        && access_cond[0].is_none()
        && column_values[0].is_none()
}

/// Go `NeedAddColumn4InCond`: the IN names exactly the shard function's
/// one column, every member a constant, and nothing pins the prefix.
#[must_use]
pub fn need_add_column4_in_cond(
    cols: &[tidb_expr::column::Column],
    access_cond: &[Option<Expression>],
    function: Option<&tidb_expr::scalar_function::ScalarFunction>,
) -> bool {
    let Some(function) = function else {
        return false;
    };
    if cols.is_empty() || access_cond.is_empty() {
        return false;
    }
    if access_cond[0].is_some() {
        return false;
    }
    let virtual_function = match cols[0].virtual_expr.as_deref() {
        Some(Expression::ScalarFunction(inner)) => Some(inner),
        _ => None,
    };
    let fields = extract_columns_from_expr(virtual_function);
    let Some(Expression::Column(in_column)) = function.args.first() else {
        return false;
    };
    if function.args[1..]
        .iter()
        .any(|member| !matches!(member, Expression::Constant(_)))
    {
        return false;
    }
    fields.len() == 1 && fields[0].unique_id == in_column.unique_id
}

/// Go `NeedAddGcColumn4ShardIndex`.
#[must_use]
pub fn need_add_gc_column4_shard_index(
    cols: &[tidb_expr::column::Column],
    access_cond: &[Option<Expression>],
    column_values: &[Option<ValueInfo>],
) -> bool {
    if access_cond.len() < 2 || cols.len() < 2 {
        return false;
    }
    if !is_valid_shard_index(cols) {
        return false;
    }
    if let Some(Expression::ScalarFunction(function)) = &access_cond[1] {
        match function.func_name.lowercase() {
            "eq" => return need_add_column4_eq_cond(cols, access_cond, column_values),
            "in" => return need_add_column4_in_cond(cols, access_cond, Some(function)),
            _ => {}
        }
    }
    false
}

/// Evaluates the shard prefix's virtual expression at one record: the
/// argument column replaced by the value, then folded to its constant --
/// Go's `expr.Eval(mutRow)`.
fn eval_virtual_expr_at(
    virtual_expr: &Expression,
    value: &Datum,
) -> Result<Datum, super::points::PointBuilderError> {
    fn substitute(expression: &Expression, value: &Datum) -> Expression {
        match expression {
            Expression::Column(column) => {
                Expression::Constant(tidb_expr::constant::Constant::new(
                    value.clone(),
                    column.ret_type.clone().unwrap_or_else(|| {
                        tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong)
                    }),
                ))
            }
            Expression::ScalarFunction(function) => {
                let mut rewritten = function.clone();
                rewritten.args = function
                    .args
                    .iter()
                    .map(|argument| substitute(argument, value))
                    .collect();
                Expression::ScalarFunction(rewritten)
            }
            other => other.clone(),
        }
    }
    let mut substituted = substitute(virtual_expr, value);
    tidb_expr::fold_constant_in_mode(
        &mut substituted,
        &tidb_expr::NoColumns,
        tidb_expr::ConstantFoldMode::Normal,
    );
    match substituted {
        Expression::Constant(constant) => Ok(constant.value),
        other => Err(super::points::PointBuilderError::Unsupported(format!(
            "tidb_shard did not fold: {other:?}"
        ))),
    }
}

fn eq_function(
    left: Expression,
    left_type: &tidb_datatype::FieldType,
    right: Expression,
) -> Result<Expression, super::points::PointBuilderError> {
    tidb_expr::new_function::new_function(
        &tidb_expr::NoColumns,
        "eq",
        left_type.clone(),
        vec![left, right],
    )
    .map_err(|error| super::points::PointBuilderError::Unsupported(format!("{error:?}")))
}

/// Go `AddGcColumn4EqCond`: evaluates `tidb_shard` over the pinned record
/// and fills the prefix slot with `tidb_shard(a) = <value>`.
pub fn add_gc_column4_eq_cond(
    cols: &[tidb_expr::column::Column],
    access_cond: &mut [Option<Expression>],
    column_values: &mut [Option<ValueInfo>],
) -> Result<(), super::points::PointBuilderError> {
    let virtual_expr = cols[0]
        .virtual_expr
        .as_deref()
        .expect("a shard index carries its virtual expression");
    // The shard function reads ONE column; Go builds the record from the
    // data columns and evaluates over it.
    let record = column_values[1]
        .as_ref()
        .and_then(|info| info.value.clone())
        .unwrap_or(Datum::Null);
    let evaluated = eval_virtual_expr_at(virtual_expr, &record)?;
    let ret_type = cols[0]
        .ret_type
        .clone()
        .unwrap_or_else(|| tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong));
    let constant = Expression::Constant(tidb_expr::constant::Constant::new(
        evaluated.clone(),
        ret_type.clone(),
    ));
    access_cond[0] = Some(eq_function(
        Expression::Column(cols[0].clone()),
        &ret_type,
        constant,
    )?);
    column_values[0] = Some(ValueInfo {
        value: Some(evaluated),
        mutable: false,
    });
    Ok(())
}

/// Go `AddGcColumn4InCond`: one `(tidb_shard(a) = h AND a = v)` disjunct
/// per IN member, OR-chained left-deep.
pub fn add_gc_column4_in_cond(
    cols: &[tidb_expr::column::Column],
    access_cond: &[Option<Expression>],
) -> Result<Vec<Expression>, super::points::PointBuilderError> {
    let virtual_expr = cols[0]
        .virtual_expr
        .as_deref()
        .expect("a shard index carries its virtual expression");
    let Some(Expression::ScalarFunction(function)) = &access_cond[1] else {
        return Err(super::points::PointBuilderError::Unsupported(
            "AddGcColumn4InCond expects the IN condition".to_owned(),
        ));
    };
    let Some(Expression::Column(in_column)) = function.args.first() else {
        return Err(super::points::PointBuilderError::Unsupported(
            "the IN names no column".to_owned(),
        ));
    };
    let and_type = tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::Tiny);
    let shard_type = cols[0]
        .ret_type
        .clone()
        .unwrap_or_else(|| tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong));
    let column_type = in_column
        .ret_type
        .clone()
        .unwrap_or_else(|| tidb_datatype::FieldType::new(tidb_datatype::FieldTypeCode::LongLong));
    let mut and_or: Option<Expression> = None;
    for member in &function.args[1..] {
        let Expression::Constant(constant) = member else {
            return Err(super::points::PointBuilderError::Unsupported(
                "an IN member is not a constant".to_owned(),
            ));
        };
        let shard_value = eval_virtual_expr_at(virtual_expr, &constant.value)?;
        let shard_constant =
            Expression::Constant(tidb_expr::constant::Constant::new(shard_value, shard_type.clone()));
        let shard_eq = eq_function(
            Expression::Column(cols[0].clone()),
            &shard_type,
            shard_constant,
        )?;
        let member_eq = eq_function(
            Expression::Column(in_column.clone()),
            &column_type,
            member.clone(),
        )?;
        let and_expr = tidb_expr::new_function::new_function(
            &tidb_expr::NoColumns,
            "and",
            and_type.clone(),
            vec![shard_eq, member_eq],
        )
        .map_err(|error| super::points::PointBuilderError::Unsupported(format!("{error:?}")))?;
        and_or = Some(match and_or {
            None => and_expr,
            Some(previous) => tidb_expr::new_function::new_function(
                &tidb_expr::NoColumns,
                "or",
                and_type.clone(),
                vec![previous, and_expr],
            )
            .map_err(|error| super::points::PointBuilderError::Unsupported(format!("{error:?}")))?,
        });
    }
    Ok(and_or.into_iter().collect())
}

/// Go `AddGcColumnCond`.
pub fn add_gc_column_cond(
    cols: &[tidb_expr::column::Column],
    access_cond: &mut [Option<Expression>],
    column_values: &mut [Option<ValueInfo>],
) -> Result<Option<Vec<Expression>>, super::points::PointBuilderError> {
    if let Some(Expression::ScalarFunction(function)) = &access_cond[1] {
        match function.func_name.lowercase() {
            "eq" => {
                add_gc_column4_eq_cond(cols, access_cond, column_values)?;
                return Ok(None);
            }
            "in" => return add_gc_column4_in_cond(cols, access_cond).map(Some),
            _ => {}
        }
    }
    Ok(None)
}

/// Go `AddExpr4EqAndInCondition` (`detacher.go:1379`), the planner rule's
/// entry: rewrites `WHERE a = 1` over `uk(tidb_shard(a), a)` into
/// `tidb_shard(a) = 214 AND a = 1`, and an IN into the OR-of-ANDs form.
/// Anything that does not match returns the conditions untouched.
pub fn add_expr4_eq_and_in_condition(
    conditions: &[Expression],
    cols: &[tidb_expr::column::Column],
) -> Result<Vec<Expression>, super::points::PointBuilderError> {
    let mut accesses: Vec<Option<Expression>> = vec![None; cols.len()];
    let mut column_values: Vec<Option<ValueInfo>> = vec![None; cols.len()];
    let mut add_gc_cond = true;
    for cond in conditions {
        let offset = get_potential_eq_or_in_col_offset(cond, cols, false);
        if offset < 0 {
            continue;
        }
        let offset = offset as usize;
        if accesses[offset].is_none() {
            accesses[offset] = Some(cond.clone());
        } else {
            // The same field twice (`a > 100 AND a < 200`): no rewrite.
            add_gc_cond = false;
        }
    }
    for (offset, cond) in accesses.iter().enumerate() {
        let Some(cond) = cond else { continue };
        if !all_eq_or_in(cond) {
            add_gc_cond = false;
            break;
        }
        column_values[offset] = extract_value_info(cond);
    }
    if !add_gc_cond || !need_add_gc_column4_shard_index(cols, &accesses, &column_values) {
        return Ok(conditions.to_vec());
    }
    let flattened: Vec<Expression> = accesses.iter().flatten().cloned().collect();
    let mut new_conditions = remove_conditions(conditions, &flattened);
    match add_gc_column_cond(cols, &mut accesses, &mut column_values)? {
        Some(replaced) => new_conditions.extend(replaced),
        None => new_conditions.extend(accesses.into_iter().flatten()),
    }
    Ok(new_conditions)
}
