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

//! Go `pkg/expression/util.go`, the column-extraction family (`:82`-`:544`).
//!
//! `ExtractColumns` (`:127`), `ExtractCorColumns` (`:140`) and
//! `ExtractColumnsFromExpressions` (`:164`) are NOT here: `simple_expr.rs`
//! ported them first, and this module's parent re-exports them.

use crate::column::{Column, CorrelatedColumn};
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use std::collections::{BTreeMap, BTreeSet};

/// Go `FilterOutInPlace` (`util.go:94`): partitions `input` into the elements
/// the filter REJECTS (returned first, matching Go's `remained`) and the ones
/// it accepts (`filteredOut`).
///
/// Go's name reads backwards on purpose: `filter` selects what is filtered
/// OUT, so `remained` is the complement.
#[must_use]
pub fn filter_out_in_place(
    input: Vec<Expression>,
    filter: &dyn Fn(&Expression) -> bool,
) -> (Vec<Expression>, Vec<Expression>) {
    let mut remained = Vec::with_capacity(input.len());
    let mut filtered_out = Vec::new();
    for expr in input {
        if filter(&expr) {
            filtered_out.push(expr);
        } else {
            remained.push(expr);
        }
    }
    (remained, filtered_out)
}

/// Go `extractDependentColumns` (`util.go:111`).
fn extract_dependent_columns_into(result: &mut Vec<Column>, expr: &Expression) {
    match expr {
        Expression::Column(column) => {
            result.push(column.clone());
            if let Some(virtual_expr) = column.virtual_expr.as_deref() {
                extract_dependent_columns_into(result, virtual_expr);
            }
        }
        Expression::ScalarFunction(function) => {
            for arg in function.get_args() {
                extract_dependent_columns_into(result, arg);
            }
        }
        _ => {}
    }
}

/// Go `ExtractDependentColumns` (`util.go:105`): every column under `expr`,
/// DESCENDING into a virtual column's generating expression.
///
/// This is what separates it from `ExtractColumns`: a virtual column
/// contributes both itself and the columns it is computed from. No
/// deduplication and no sort -- Go appends, so a column reached twice appears
/// twice, in walk order.
#[must_use]
pub fn extract_dependent_columns(expr: &Expression) -> Vec<Column> {
    let mut result = Vec::new();
    extract_dependent_columns_into(&mut result, expr);
    result
}

/// Go `extractColumns` (`util.go:263`), the map-collecting walk.
fn extract_columns_into_map(
    result: &mut BTreeMap<i64, Column>,
    expr: &Expression,
    filter: Option<&dyn Fn(&Column) -> bool>,
) {
    match expr {
        Expression::Column(column) => {
            if filter.is_none_or(|keep| keep(column)) {
                result.insert(column.unique_id, column.clone());
            }
        }
        Expression::ScalarFunction(function) => {
            for arg in function.get_args() {
                extract_columns_into_map(result, arg, filter);
            }
        }
        _ => {}
    }
}

/// Go `extractColumnsSlices` (`util.go:276`), the append-collecting walk.
fn extract_columns_into_slice(
    result: &mut Vec<Column>,
    expr: &Expression,
    filter: Option<&dyn Fn(&Column) -> bool>,
) {
    match expr {
        Expression::Column(column) => {
            if filter.is_none_or(|keep| keep(column)) {
                result.push(column.clone());
            }
        }
        Expression::ScalarFunction(function) => {
            for arg in function.get_args() {
                extract_columns_into_slice(result, arg, filter);
            }
        }
        _ => {}
    }
}

/// Go `extractColumnsSet` (`util.go:290`), the id-set-collecting walk.
fn extract_columns_into_set(
    result: &mut BTreeSet<i64>,
    expr: &Expression,
    filter: Option<&dyn Fn(&Column) -> bool>,
) {
    match expr {
        Expression::Column(column) => {
            if filter.is_none_or(|keep| keep(column)) {
                result.insert(column.unique_id);
            }
        }
        Expression::ScalarFunction(function) => {
            for arg in function.get_args() {
                extract_columns_into_set(result, arg, filter);
            }
        }
        _ => {}
    }
}

/// Go `ExtractColumnsMapFromExpressions` (`util.go:181`): the same walk as
/// `ExtractColumnsFromExpressions`, keeping the `UniqueID -> Column` map
/// instead of a sorted slice.
///
/// Go returns a nil map for an empty input; an empty `BTreeMap` is the same
/// thing to every reader (`len`, lookup, iteration).
#[must_use]
pub fn extract_columns_map_from_expressions(
    filter: Option<&dyn Fn(&Column) -> bool>,
    exprs: &[Expression],
) -> BTreeMap<i64, Column> {
    let mut result = BTreeMap::new();
    if exprs.is_empty() {
        return result;
    }
    for expr in exprs {
        extract_columns_into_map(&mut result, expr, filter);
    }
    result
}

/// Go `ExtractColumnsMapFromExpressionsWithReusedMap` (`util.go:210`):
/// accumulates into a caller-owned map.
///
/// Go pairs this with a `sync.Pool` of maps (`util.go:199`, `:204`); the pool
/// is an allocator, not a semantic, and is named as a boundary in this
/// module's parent rather than reproduced. The `&mut` parameter is the part
/// that carries meaning: results ACCUMULATE across calls and are not cleared.
pub fn extract_columns_map_from_expressions_with_reused_map(
    result: &mut BTreeMap<i64, Column>,
    filter: Option<&dyn Fn(&Column) -> bool>,
    exprs: &[Expression],
) {
    if exprs.is_empty() {
        return;
    }
    for expr in exprs {
        extract_columns_into_map(result, expr, filter);
    }
}

/// Go `ExtractAllColumnsFromExpressionsInUsedSlices` (`util.go:223`):
/// appends into `reuse`, then sorts by `UniqueID` and compacts.
///
/// Go's `slices.CompactFunc` removes only ADJACENT duplicates, which the
/// preceding sort makes equivalent to full deduplication -- but the sort is
/// `SortFunc`, i.e. UNSTABLE, so which of several equal-id columns survives is
/// not defined by Go either. `sort_unstable_by_key` + `dedup_by_key` is the
/// same pair of operations.
#[must_use]
pub fn extract_all_columns_from_expressions_in_used_slices(
    mut reuse: Vec<Column>,
    filter: Option<&dyn Fn(&Column) -> bool>,
    exprs: &[Expression],
) -> Vec<Column> {
    if exprs.is_empty() {
        return Vec::new();
    }
    for expr in exprs {
        extract_columns_into_slice(&mut reuse, expr, filter);
    }
    reuse.sort_unstable_by_key(|column| column.unique_id);
    reuse.dedup_by_key(|column| column.unique_id);
    reuse
}

/// Go `ExtractAllColumnsFromExpressions` (`util.go:240`): like
/// `ExtractColumnsFromExpressions` but WITHOUT deduplication and without a
/// sort -- the columns arrive in walk order, repeats included.
#[must_use]
pub fn extract_all_columns_from_expressions(
    exprs: &[Expression],
    filter: Option<&dyn Fn(&Column) -> bool>,
) -> Vec<Column> {
    if exprs.is_empty() {
        return Vec::new();
    }
    let mut result = Vec::new();
    for expr in exprs {
        extract_columns_into_slice(&mut result, expr, filter);
    }
    result
}

/// Go `ExtractColumnsSetFromExpressions` (`util.go:253`): accumulates column
/// `UniqueID`s into a caller-owned set.
///
/// `// boundary:` Go `intset.FastIntSet` -- see this module's parent.
pub fn extract_columns_set_from_expressions(
    result: &mut BTreeSet<i64>,
    filter: Option<&dyn Fn(&Column) -> bool>,
    exprs: &[Expression],
) {
    if exprs.is_empty() {
        return;
    }
    for expr in exprs {
        extract_columns_into_set(result, expr, filter);
    }
}

/// Go `extractColumnSet` (`util.go:515`).
fn extract_column_set_into(expr: &Expression, set: &mut BTreeSet<i64>) {
    match expr {
        Expression::Column(column) => {
            set.insert(column.unique_id);
        }
        Expression::ScalarFunction(function) => {
            for arg in function.get_args() {
                extract_column_set_into(arg, set);
            }
        }
        _ => {}
    }
}

/// Go `ExtractColumnSet` (`util.go:507`): the distinct `UniqueID`s of the
/// columns in `exprs`.
///
/// Unlike [`extract_columns_set_from_expressions`] this one takes no filter,
/// which is the only difference between the two in Go as well.
#[must_use]
pub fn extract_column_set(exprs: &[Expression]) -> BTreeSet<i64> {
    let mut set = BTreeSet::new();
    for expr in exprs {
        extract_column_set_into(expr, &mut set);
    }
    set
}

/// Go `extractColumnsAndCorColumns` (`util.go:391`): appends columns AND the
/// `Column` embedded inside each correlated column.
fn extract_columns_and_cor_columns_into(result: &mut Vec<Column>, expr: &Expression) {
    match expr {
        Expression::Column(column) => result.push(column.clone()),
        Expression::CorrelatedColumn(cor) => result.push(cor.column.clone()),
        Expression::ScalarFunction(function) => {
            for arg in function.get_args() {
                extract_columns_and_cor_columns_into(result, arg);
            }
        }
        _ => {}
    }
}

/// Go `ExtractColumnsAndCorColumnsFromExpressions` (`util.go:499`): appends
/// into `result`, no deduplication, walk order.
#[must_use]
pub fn extract_columns_and_cor_columns_from_expressions(
    mut result: Vec<Column>,
    list: &[Expression],
) -> Vec<Column> {
    for expr in list {
        extract_columns_and_cor_columns_into(&mut result, expr);
    }
    result
}

/// Go `SetExprColumnInOperand` (`util.go:527`): marks every column under
/// `expr` as the inner operand of a rewritten `[NOT] IN (subquery)`.
///
/// Go clones a `*Column` before setting the flag (the column may be shared)
/// but MUTATES a `*ScalarFunction` in place and clears its hash-code cache.
/// Taking `expr` by value gives the same observable result -- the caller's
/// other references, if any, are untouched -- without needing Go's
/// clone/mutate split.
#[must_use]
pub fn set_expr_column_in_operand(expr: Expression) -> Expression {
    match expr {
        Expression::Column(mut column) => {
            column.in_operand = true;
            Expression::Column(column)
        }
        Expression::ScalarFunction(mut function) => {
            let args = std::mem::take(&mut function.args);
            function.args = args.into_iter().map(set_expr_column_in_operand).collect();
            // Go's `CleanHashCode()`: the args changed, so the cached code is
            // stale. `ScalarFunction::new` is the crate's way to rebuild with a
            // fresh cache; mutating through the public `args` field here means
            // the private cache must be invalidated the same way.
            function.clean_hash_code();
            Expression::ScalarFunction(function)
        }
        other => other,
    }
}

/// Go `FindUpperBound` (`util.go:316`): recognizes `column < constant` or
/// `column <= constant` and returns the column with the largest integer the
/// column may take.
///
/// `None` when the expression is not of that form. `col < v` yields `v - 1`,
/// which is Go's own arithmetic and, as in Go, is not guarded against
/// underflow at `i64::MIN`; `wrapping_sub` reproduces Go's two's-complement
/// wrap rather than panicking in a Rust debug build.
#[must_use]
pub fn find_upper_bound(expr: &Expression) -> Option<(Column, i64)> {
    let Expression::ScalarFunction(function) = expr else {
        return None;
    };
    let args = function.get_args();
    if args.len() != 2 {
        return None;
    }
    let name = function.func_name.lowercase();
    if name != "lt" && name != "le" {
        return None;
    }
    let (Expression::Column(column), Expression::Constant(constant)) = (&args[0], &args[1]) else {
        return None;
    };
    // Go's `constant.Value.GetValue().(int64)` is a type ASSERTION: it matches
    // `KindInt64` only, so an unsigned or decimal bound is not a bound here.
    let tidb_datatype::Datum::Int(value) = constant.value else {
        return None;
    };
    if name == "lt" {
        Some((column.clone(), value.wrapping_sub(1)))
    } else {
        Some((column.clone(), value))
    }
}

/// Go `extractEquivalenceColumns` (`util.go:337`).
fn extract_equivalence_columns_into(result: &mut Vec<[Expression; 2]>, expr: &Expression) {
    let Expression::ScalarFunction(function) = expr else {
        return;
    };
    let name = function.func_name.lowercase();
    let args = function.get_args();

    // `a = b` and `a <=> b`; the latter is also true when both sides are NULL.
    if name == "eq" || name == "nulleq" {
        if args.len() == 2 {
            push_equivalence_pair(result, &args[0], &args[1]);
        }
        return;
    }
    // For a non-EQ function there is nothing to descend into: `(a=b or c=d)`
    // asserts no equivalence at all.
    if name == "in" {
        // Only `col IN (exactly one element)` asserts an equivalence.
        if args.len() == 2 {
            push_equivalence_pair(result, &args[0], &args[1]);
        }
    }
}

/// The three `col/col`, `col/func`, `func/col` shapes Go appends for an
/// equivalence, in Go's order. Go writes them as three sequential `if`s over
/// the same pair, so a `col = col` pair matches only the first.
fn push_equivalence_pair(result: &mut Vec<[Expression; 2]>, left: &Expression, right: &Expression) {
    if matches!(left, Expression::Column(_)) && matches!(right, Expression::Column(_)) {
        result.push([left.clone(), right.clone()]);
    }
    if matches!(left, Expression::Column(_)) && matches!(right, Expression::ScalarFunction(_)) {
        result.push([left.clone(), right.clone()]);
    }
    if matches!(right, Expression::Column(_)) && matches!(left, Expression::ScalarFunction(_)) {
        result.push([right.clone(), left.clone()]);
    }
}

/// Go `ExtractEquivalenceColumns` (`util.go:304`): the column equivalences
/// asserted by a list of CNF conjuncts.
///
/// `exprs` are CNF items, so an equality only asserts an equivalence at the
/// TOP level of each item -- the walk deliberately does not descend.
#[must_use]
pub fn extract_equivalence_columns(
    mut result: Vec<[Expression; 2]>,
    exprs: &[Expression],
) -> Vec<[Expression; 2]> {
    for expr in exprs {
        extract_equivalence_columns_into(&mut result, expr);
    }
    result
}

/// Go `extractConstantEqColumnsOrScalar` (`util.go:414`).
fn extract_constant_eq_columns_or_scalar_into(result: &mut Vec<Expression>, expr: &Expression) {
    let Expression::ScalarFunction(function) = expr else {
        return;
    };
    let name = function.func_name.lowercase();
    let args = function.get_args();

    if name == "eq" || name == "nulleq" {
        if args.len() != 2 {
            return;
        }
        // Go runs eight sequential `if`s over the same pair: {column, scalar}
        // on one side against {constant, correlated column} on the other. A
        // correlated column counts as a constant here because it is fixed for
        // the duration of one inner-query execution.
        for (side, other) in [(&args[0], &args[1]), (&args[1], &args[0])] {
            let other_is_const = matches!(
                other,
                Expression::Constant(_) | Expression::CorrelatedColumn(_)
            );
            if other_is_const
                && matches!(side, Expression::Column(_) | Expression::ScalarFunction(_))
            {
                result.push(side.clone());
            }
        }
        return;
    }
    if name == "in" {
        // `col IN (all the SAME constant)` makes `col` constant. Go's guard is
        // value equality against the first list element, so `a IN (1, '1')`
        // qualifies while `a IN (1, '2')` does not.
        if args.len() < 2 {
            return;
        }
        let guard = &args[1];
        let mut all_args_is_const = true;
        for (index, arg) in args[1..].iter().enumerate() {
            if !matches!(arg, Expression::Constant(_)) {
                all_args_is_const = false;
                break;
            }
            if index == 0 {
                continue;
            }
            // `// boundary:` Go `Expression.Equal(ctx, v)` compares constants
            // through a collator. `Expression::equal` in this crate is
            // documented as context-free and reports `false` for two
            // constants, so this test is CONSERVATIVE: a list Go accepts may
            // be rejected here, never the reverse. Rejecting only loses an
            // optimization, so the narrowing is safe in the direction it errs.
            if !guard.equal(arg) {
                all_args_is_const = false;
                break;
            }
        }
        if all_args_is_const
            && matches!(
                args[0],
                Expression::Column(_) | Expression::ScalarFunction(_)
            )
        {
            result.push(args[0].clone());
        }
    }
    // For a non-EQ function there is nothing to descend into.
}

/// Go `ExtractConstantEqColumnsOrScalar` (`util.go:406`): the columns and
/// scalar functions that a list of CNF conjuncts pins to a constant.
#[must_use]
pub fn extract_constant_eq_columns_or_scalar(
    mut result: Vec<Expression>,
    exprs: &[Expression],
) -> Vec<Expression> {
    for expr in exprs {
        extract_constant_eq_columns_or_scalar_into(&mut result, expr);
    }
    result
}

/// Go `IsColOpCol` (`util.go:2370`): the two columns of a `col op col`
/// condition, or `None`.
#[must_use]
pub fn is_col_op_col(sf: &ScalarFunction) -> Option<(&Column, &Column)> {
    let args = sf.get_args();
    if args.len() != 2 {
        return None;
    }
    match (&args[0], &args[1]) {
        (Expression::Column(left), Expression::Column(right)) => Some((left, right)),
        _ => None,
    }
}

/// Go `ExtractColumnsFromColOpCol` (`util.go:2381`): the two columns of a
/// condition the caller has ALREADY established is `col op col`.
///
/// Go type-asserts without the `, ok` form and panics otherwise; returning
/// `None` is the same contract expressed without a panic, and
/// [`is_col_op_col`] is the check Go expects the caller to have made.
#[must_use]
pub fn extract_columns_from_col_op_col(sf: &ScalarFunction) -> Option<(&Column, &Column)> {
    is_col_op_col(sf)
}

/// Go `ExtractCorColumns` over a batch: the correlated columns of every
/// expression in `exprs`, concatenated in walk order without deduplication.
///
/// Go has no batch form; the planner rules write the loop inline. It is here
/// so a downstream rule does not have to.
#[must_use]
pub fn extract_cor_columns_from_expressions(exprs: &[Expression]) -> Vec<CorrelatedColumn> {
    exprs
        .iter()
        .flat_map(crate::simple_expr::extract_cor_columns)
        .collect()
}
