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

//! Tests for [`super`].
//!
//! Two kinds, labelled per test:
//!
//! - **GO PORT** -- transcribed from `pkg/expression/util_test.go`. The
//!   assertions are Go's; only the expression-building helpers differ, because
//!   Go's `newFunctionWithMockCtx` runs the real `NewFunction` this crate does
//!   not have yet (see [`super::builder`]).
//! - **NEW COVERAGE** -- written here. Go exercises most of the extraction and
//!   normal-form surface only indirectly, through planner testkit cases that
//!   need a running optimizer, so there is no unit test to port. These test
//!   the ported semantics directly.
//!
//! Tree equality is asserted through `HashCode`, not `Expression::equal`:
//! `expression.rs` documents the latter as context-free, reporting `false` for
//! two constants and for two scalar functions, which would make most of these
//! assertions vacuous. `HashCode` is structural and context-free by
//! construction, which is exactly what these shape assertions need.

use super::builder::PreservingFunctionBuilder;
use super::*;
use crate::column::{Column, CorrelatedColumn};
use crate::constant::Constant;
use crate::context::NoColumns;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode, FieldTypeFlags};

fn int_type() -> FieldType {
    FieldType::new(FieldTypeCode::LongLong)
}

fn string_type() -> FieldType {
    FieldType::new(FieldTypeCode::VarString)
}

/// Go `newColumn(id)`: a column whose `UniqueID` and `Index` are both `id`.
fn col(id: i64) -> Expression {
    let mut column = Column::new(id, int_type());
    column.index = id;
    column.orig_name = format!("t.c{id}");
    Expression::Column(column)
}

fn cor_col(id: i64, data: Option<Datum>) -> Expression {
    let mut column = Column::new(id, int_type());
    column.index = id;
    Expression::CorrelatedColumn(CorrelatedColumn { column, data })
}

/// Go `newLonglong(v)`.
fn int_const(v: i64) -> Expression {
    Expression::Constant(Constant::new(Datum::Int(v), int_type()))
}

fn str_const(v: &str) -> Expression {
    Expression::Constant(Constant::new(
        Datum::Bytes(v.as_bytes().to_vec()),
        string_type(),
    ))
}

/// Go `newFunctionWithMockCtx(name, args...)`, minus the type inference the
/// real `NewFunction` performs -- see this module's header.
fn func(name: &str, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(CiString::new(name), int_type(), args))
}

fn shape(expr: &Expression) -> Vec<u8> {
    expr.clone().hash_code().to_vec()
}

fn same_shape(left: &Expression, right: &Expression) -> bool {
    shape(left) == shape(right)
}

fn opts<'a>(builder: &'a PreservingFunctionBuilder) -> SubstituteOptions<'a> {
    SubstituteOptions::new(builder)
}

// ---------------------------------------------------------------- extraction

/// NEW COVERAGE: `ExtractColumns` deduplicates by `UniqueID` and sorts.
#[test]
fn extract_columns_dedups_and_sorts_by_unique_id() {
    let expr = func(
        "and",
        vec![
            func("eq", vec![col(3), col(1)]),
            func("eq", vec![col(1), int_const(7)]),
        ],
    );
    let ids: Vec<i64> = extract_columns(&expr).iter().map(|c| c.unique_id).collect();
    assert_eq!(ids, vec![1, 3]);
}

/// NEW COVERAGE: `ExtractDependentColumns` descends into a virtual column's
/// generating expression and does NOT deduplicate, which is what separates it
/// from `ExtractColumns`.
#[test]
fn extract_dependent_columns_descends_into_virtual_expr() {
    let mut virt = Column::new(10, int_type());
    virt.index = 10;
    virt.virtual_expr = Some(Box::new(func("plus", vec![col(1), col(2)])));
    let expr = func("eq", vec![Expression::Column(virt), col(1)]);

    let ids: Vec<i64> = extract_dependent_columns(&expr)
        .iter()
        .map(|c| c.unique_id)
        .collect();
    // 10, then its generators 1 and 2, then the outer 1 again -- walk order,
    // repeats included.
    assert_eq!(ids, vec![10, 1, 2, 1]);
}

/// NEW COVERAGE: `ExtractColumnSet` collects distinct ids across a batch.
#[test]
fn extract_column_set_collects_distinct_ids() {
    let exprs = vec![
        func("eq", vec![col(2), col(5)]),
        func("gt", vec![col(2), int_const(1)]),
    ];
    let set = extract_column_set(&exprs);
    assert_eq!(set.into_iter().collect::<Vec<_>>(), vec![2, 5]);
}

/// NEW COVERAGE: the batch extractors differ exactly in deduplication.
#[test]
fn extract_all_columns_keeps_duplicates_while_dedup_form_does_not() {
    let exprs = vec![func("eq", vec![col(1), col(1)])];
    assert_eq!(extract_all_columns_from_expressions(&exprs, None).len(), 2);
    assert_eq!(
        extract_all_columns_from_expressions_in_used_slices(Vec::new(), None, &exprs).len(),
        1
    );
}

/// NEW COVERAGE: the filter is applied while walking, so a rejected column is
/// never collected.
#[test]
fn extract_columns_from_expressions_applies_filter() {
    let exprs = vec![func("eq", vec![col(1), col(2)])];
    let keep_even = |c: &Column| c.unique_id % 2 == 0;
    let cols = extract_columns_from_expressions(&exprs, Some(&keep_even));
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].unique_id, 2);
}

/// NEW COVERAGE: `ExtractCorColumns` does NOT deduplicate -- Go appends.
#[test]
fn extract_cor_columns_keeps_every_occurrence() {
    let expr = func(
        "and",
        vec![
            cor_col(1, Some(Datum::Int(1))),
            cor_col(1, Some(Datum::Int(1))),
        ],
    );
    assert_eq!(extract_cor_columns(&expr).len(), 2);
}

/// NEW COVERAGE: `ExtractEquivalenceColumns` reads only the TOP level of each
/// CNF item -- an equality nested under `OR` asserts nothing.
#[test]
fn extract_equivalence_columns_ignores_nested_disjunction() {
    let top = func("eq", vec![col(1), col(2)]);
    let nested = func(
        "or",
        vec![
            func("eq", vec![col(3), col(4)]),
            func("eq", vec![col(5), col(6)]),
        ],
    );
    let pairs = extract_equivalence_columns(Vec::new(), &[top, nested]);
    assert_eq!(pairs.len(), 1);
    assert!(same_shape(&pairs[0][0], &col(1)));
    assert!(same_shape(&pairs[0][1], &col(2)));
}

/// NEW COVERAGE: `ExtractEquivalenceColumns` also records `col <=> func`, and
/// normalizes it so the COLUMN is always first.
#[test]
fn extract_equivalence_columns_puts_column_first() {
    let expr = func(
        "nulleq",
        vec![func("plus", vec![col(9), int_const(1)]), col(1)],
    );
    let pairs = extract_equivalence_columns(Vec::new(), std::slice::from_ref(&expr));
    assert_eq!(pairs.len(), 1);
    assert!(matches!(pairs[0][0], Expression::Column(_)));
    assert!(matches!(pairs[0][1], Expression::ScalarFunction(_)));
}

/// NEW COVERAGE: a correlated column counts as a constant for
/// `ExtractConstantEqColumnsOrScalar`, because it is fixed for one execution.
#[test]
fn extract_constant_eq_treats_correlated_column_as_constant() {
    let exprs = vec![
        func("eq", vec![col(1), int_const(5)]),
        func("eq", vec![cor_col(7, Some(Datum::Int(2))), col(2)]),
        func("gt", vec![col(3), int_const(5)]),
    ];
    let found = extract_constant_eq_columns_or_scalar(Vec::new(), &exprs);
    let ids: Vec<i64> = found
        .iter()
        .filter_map(|e| e.as_column().map(|c| c.unique_id))
        .collect();
    // `gt` is not an equality, so column 3 is not pinned.
    assert_eq!(ids, vec![1, 2]);
}

/// GO PORT of `TestFilterOutInPlace` (`util_test.go:344`).
#[test]
fn go_filter_out_in_place() {
    let conditions = vec![
        func("eq", vec![col(0), col(1)]),
        func("eq", vec![col(1), col(2)]),
        func("or", vec![int_const(1), col(0)]),
    ];
    let is_logic_or = |e: &Expression| matches!(e, Expression::ScalarFunction(f) if f.func_name.lowercase() == "or");
    let (remained, filtered) = filter_out_in_place(conditions, &is_logic_or);
    assert_eq!(remained.len(), 2);
    for expr in &remained {
        let Expression::ScalarFunction(f) = expr else {
            panic!("expected a scalar function")
        };
        assert_eq!(f.func_name.lowercase(), "eq");
    }
    assert_eq!(filtered.len(), 1);
    let Expression::ScalarFunction(f) = &filtered[0] else {
        panic!("expected a scalar function")
    };
    assert_eq!(f.func_name.lowercase(), "or");
}

/// GO PORT of `TestSetExprColumnInOperand` (`util_test.go:190`).
#[test]
fn go_set_expr_column_in_operand() {
    let marked = set_expr_column_in_operand(col(0));
    assert!(marked.as_column().expect("a column").in_operand);

    let fun = set_expr_column_in_operand(func("abs", vec![col(0)]));
    let Expression::ScalarFunction(f) = &fun else {
        panic!("expected a scalar function")
    };
    assert!(
        f.get_args()[0]
            .as_column()
            .expect("the argument is a column")
            .in_operand
    );
}

/// NEW COVERAGE: marking is observable through structural equality.
///
/// It is deliberately NOT observable through `HashCode`: a column's hash code
/// is its flag plus `UniqueID` only (see `column.rs`), so `InOperand` does not
/// enter it. Go's `CleanHashCode` call in `SetExprColumnInOperand` is
/// nonetheless required, because the same in-place mutation path is shared
/// with substitutions that DO change the code.
#[test]
fn set_expr_column_in_operand_is_visible_in_structural_equality() {
    let original = Column::new(0, int_type());
    let marked = set_expr_column_in_operand(Expression::Column(original.clone()));
    let marked = marked.as_column().expect("still a column");
    assert!(marked.in_operand);
    assert!(!original.equals(marked));
}

/// NEW COVERAGE: `FindUpperBound` turns `<` into an inclusive bound and
/// rejects everything else.
#[test]
fn find_upper_bound_recognizes_lt_and_le_only() {
    let (_, bound) = find_upper_bound(&func("lt", vec![col(1), int_const(10)])).expect("lt bound");
    assert_eq!(bound, 9);
    let (_, bound) = find_upper_bound(&func("le", vec![col(1), int_const(10)])).expect("le bound");
    assert_eq!(bound, 10);
    assert!(find_upper_bound(&func("gt", vec![col(1), int_const(10)])).is_none());
    // Constant on the left is not the recognized shape.
    assert!(find_upper_bound(&func("lt", vec![int_const(10), col(1)])).is_none());
    // A non-integer bound is not a bound.
    assert!(find_upper_bound(&func("lt", vec![col(1), str_const("10")])).is_none());
}

// --------------------------------------------------------------- normal form

/// NEW COVERAGE: `SplitCNFItems` flattens nesting and leaves a non-`AND`
/// expression as a one-element CNF.
#[test]
fn split_cnf_and_dnf_items() {
    let nested = func("and", vec![func("and", vec![col(1), col(2)]), col(3)]);
    assert_eq!(split_cnf_items(&nested).len(), 3);
    // Splitting on the wrong connective yields the single item.
    assert_eq!(split_dnf_items(&nested).len(), 1);

    let dnf = func("or", vec![func("or", vec![col(1), col(2)]), col(3)]);
    assert_eq!(split_dnf_items(&dnf).len(), 3);
    assert_eq!(split_cnf_items(&dnf).len(), 1);

    // A leaf is a one-element normal form, never empty.
    assert_eq!(split_cnf_items(&col(1)).len(), 1);
}

/// NEW COVERAGE: `FlattenDNFConditions` on Go's own doc example.
#[test]
fn flatten_dnf_and_cnf_conditions() {
    let dnf = func(
        "or",
        vec![
            func("or", vec![col(1), col(2)]),
            func("or", vec![col(3), col(4)]),
        ],
    );
    let Expression::ScalarFunction(f) = &dnf else {
        panic!("expected a scalar function")
    };
    assert_eq!(flatten_dnf_conditions(f).len(), 4);
    // Flattening on the other connective does not descend.
    assert_eq!(flatten_cnf_conditions(f).len(), 2);
}

/// NEW COVERAGE: composition and splitting round-trip.
#[test]
fn compose_then_split_round_trips() {
    let items = vec![col(1), col(2), col(3)];
    let composed = compose_cnf_condition(items.clone()).expect("three items compose");
    let split = split_cnf_items(&composed);
    assert_eq!(split.len(), 3);
    for (left, right) in split.iter().zip(&items) {
        assert!(same_shape(left, right));
    }
    // An empty list is Go's nil.
    assert!(compose_cnf_condition(Vec::new()).is_none());
    // A single condition passes through untouched.
    let single = compose_dnf_condition(vec![col(1)]).expect("one item");
    assert!(same_shape(&single, &col(1)));
}

/// NEW COVERAGE: `ExtractFiltersFromDNFs` lifts the conjunct common to every
/// disjunct and leaves the remainder behind.
#[test]
fn extract_filters_from_dnfs_lifts_common_conjunct() {
    let common = func("eq", vec![col(1), int_const(1)]);
    let left = func(
        "and",
        vec![common.clone(), func("eq", vec![col(2), int_const(2)])],
    );
    let right = func(
        "and",
        vec![common.clone(), func("eq", vec![col(3), int_const(3)])],
    );
    let dnf = func("or", vec![left, right]);

    let result = extract_filters_from_dnfs(vec![dnf]);
    // The remainder replaces the DNF in place, and the lifted filter is
    // appended after it.
    assert_eq!(result.len(), 2);
    assert!(same_shape(&result[1], &common));
    let Expression::ScalarFunction(remainder) = &result[0] else {
        panic!("the remainder is still a disjunction")
    };
    assert_eq!(remainder.func_name.lowercase(), "or");
}

/// NEW COVERAGE: when a disjunct consists ONLY of lifted conjuncts, the whole
/// DNF is implied by them and is deleted rather than replaced.
#[test]
fn extract_filters_from_dnfs_deletes_fully_covered_dnf() {
    let common = func("eq", vec![col(1), int_const(1)]);
    let dnf = func(
        "or",
        vec![
            common.clone(),
            func(
                "and",
                vec![common.clone(), func("eq", vec![col(2), int_const(2)])],
            ),
        ],
    );
    let result = extract_filters_from_dnfs(vec![dnf]);
    assert_eq!(result.len(), 1);
    assert!(same_shape(&result[0], &common));
}

/// NEW COVERAGE: a DNF with nothing in common is returned untouched.
#[test]
fn extract_filters_from_dnfs_leaves_disjoint_dnf_alone() {
    let dnf = func(
        "or",
        vec![
            func("eq", vec![col(1), int_const(1)]),
            func("eq", vec![col(2), int_const(2)]),
        ],
    );
    let result = extract_filters_from_dnfs(vec![dnf.clone()]);
    assert_eq!(result.len(), 1);
    assert!(same_shape(&result[0], &dnf));
}

/// NEW COVERAGE: `ExprFromSchema` on Go's own definition -- constants and
/// correlated columns are always "from" the schema.
#[test]
fn expr_from_schema_covers_constants_and_correlated_columns() {
    let schema =
        crate::schema::Schema::new(vec![Column::new(1, int_type()), Column::new(2, int_type())]);
    assert!(expr_from_schema(&func("eq", vec![col(1), col(2)]), &schema));
    assert!(!expr_from_schema(
        &func("eq", vec![col(1), col(9)]),
        &schema
    ));
    assert!(expr_from_schema(&int_const(1), &schema));
    assert!(expr_from_schema(&cor_col(99, None), &schema));
}

/// NEW COVERAGE: `DeriveRelaxedFiltersFromDNF` on Go's own doc example.
#[test]
fn derive_relaxed_filters_from_dnf_matches_doc_example() {
    // Schema of t1 holds column 1 only; column 2 belongs to t2.
    let schema = crate::schema::Schema::new(vec![Column::new(1, int_type())]);
    let t1a1 = func("eq", vec![col(1), int_const(1)]);
    let t2a1 = func("eq", vec![col(2), int_const(1)]);
    let t1a2 = func("eq", vec![col(1), int_const(2)]);
    let t2a2 = func("eq", vec![col(2), int_const(2)]);

    // `(t1.a=1 and t2.a=1) or (t1.a=2 and t2.a=2)` -> `t1.a=1 or t1.a=2`.
    let expr = func(
        "or",
        vec![
            func("and", vec![t1a1.clone(), t2a1.clone()]),
            func("and", vec![t1a2.clone(), t2a2]),
        ],
    );
    let relaxed = derive_relaxed_filters_from_dnf(&expr, &schema).expect("a relaxed filter");
    let expected = compose_dnf_condition(vec![t1a1.clone(), t1a2]).expect("two items");
    assert!(same_shape(&relaxed, &expected));

    // `t1.a=1 or t2.a=1` -> nothing: the second disjunct is unconstrained.
    let expr = func("or", vec![t1a1, t2a1]);
    assert!(derive_relaxed_filters_from_dnf(&expr, &schema).is_none());
}

// -------------------------------------------------------------- substitution

/// NEW COVERAGE: `ColumnSubstitute` replaces a schema column by its
/// projection expression.
#[test]
fn column_substitute_replaces_schema_column() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let schema = crate::schema::Schema::new(vec![Column::new(1, int_type())]);
    let new_exprs = vec![col(7)];

    let expr = func("lt", vec![col(1), int_const(10)]);
    let result = column_substitute(&expr, &schema, &new_exprs, &opts);
    let expected = func("lt", vec![col(7), int_const(10)]);
    assert!(same_shape(&result, &expected));
}

/// NEW COVERAGE: a column outside the schema is left alone, and the outcome
/// reports no substitution.
#[test]
fn column_substitute_leaves_foreign_column_alone() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let schema = crate::schema::Schema::new(vec![Column::new(1, int_type())]);
    let expr = func("lt", vec![col(9), int_const(10)]);
    let outcome = column_substitute_impl(&expr, &schema, &[col(7)], false, &opts);
    assert!(!outcome.substituted);
    assert!(same_shape(&outcome.expr, &expr));
}

/// NEW COVERAGE: Go's `EQ` special case -- a substitution that puts the
/// constant first is rebuilt with the constant on the RIGHT, so plan output
/// stays stable.
#[test]
fn column_substitute_keeps_eq_constant_on_the_right() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let schema = crate::schema::Schema::new(vec![Column::new(1, int_type())]);
    // `eq(c1, c2)` with c1 -> a constant would yield `eq(const, c2)`.
    let expr = func("eq", vec![col(1), col(2)]);
    let result = column_substitute(&expr, &schema, &[int_const(5)], &opts);
    let Expression::ScalarFunction(f) = &result else {
        panic!("expected a scalar function")
    };
    assert!(matches!(f.get_args()[0], Expression::Column(_)));
    assert!(matches!(f.get_args()[1], Expression::Constant(_)));
}

/// NEW COVERAGE: `InOperand` propagates onto the substituted expression, which
/// is what keeps `[NOT] IN (subquery)` rewrites correct.
#[test]
fn column_substitute_propagates_in_operand() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let mut column = Column::new(1, int_type());
    column.in_operand = true;
    let schema = crate::schema::Schema::new(vec![Column::new(1, int_type())]);

    let result = column_substitute(
        &Expression::Column(column),
        &schema,
        &[func("abs", vec![col(7)])],
        &opts,
    );
    let Expression::ScalarFunction(f) = &result else {
        panic!("expected a scalar function")
    };
    assert!(
        f.get_args()[0]
            .as_column()
            .expect("a column argument")
            .in_operand
    );
}

/// GO PORT of `TestSubstituteCorCol2Constant` (`util_test.go:240`), reduced to
/// the parts that do not need the real `NewFunction`.
///
/// Go builds `((cast(corCol1) + corCol2) + 1)` from two correlated columns
/// bound to 1 and asserts it folds to the constant 3. That fold needs
/// arithmetic evaluation, which this crate has; what it does NOT have is Go's
/// cast construction, so the `cast` wrapper is dropped and the sum is over the
/// two correlated columns directly.
#[test]
fn go_substitute_cor_col_2_constant() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let cor1 = cor_col(1, Some(Datum::Int(1)));
    let cor2 = cor_col(2, Some(Datum::Int(1)));
    let plus = func("plus", vec![cor1, cor2]);
    let plus2 = func("plus", vec![plus, int_const(1)]);

    let result = substitute_cor_col_2_constant(&plus2, &NoColumns, &opts)
        .expect("a wholly correlated tree folds");
    let Expression::Constant(constant) = &result else {
        panic!("expected the tree to fold to a constant, got {result:?}")
    };
    assert_eq!(constant.value, Datum::Int(3));

    // A plain column is returned unchanged -- it is not correlated.
    let column = col(1);
    let result = substitute_cor_col_2_constant(&column, &NoColumns, &opts).expect("no failure");
    assert!(same_shape(&result, &column));

    // With a real column mixed in, the tree cannot fold to a constant.
    let plus3 = func("plus", vec![plus2, col(1)]);
    let result = substitute_cor_col_2_constant(&plus3, &NoColumns, &opts).expect("no failure");
    assert!(matches!(result, Expression::ScalarFunction(_)));
}

/// GO PORT of `TestPopRowFirstArg` (`util_test.go:202`).
#[test]
fn go_pop_row_first_arg() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let row = func("row", vec![col(1), col(2), col(3)]);
    let popped = pop_row_first_arg(&row, &opts)
        .expect("no build failure")
        .expect("a row pops");
    let Expression::ScalarFunction(f) = &popped else {
        panic!("expected a scalar function")
    };
    assert_eq!(f.get_args().len(), 2);

    // NEW COVERAGE: a two-element row pops to the bare remaining element,
    // because a one-element row is not a row in MySQL.
    let row = func("row", vec![col(1), col(2)]);
    let popped = pop_row_first_arg(&row, &opts)
        .expect("no build failure")
        .expect("a row pops");
    assert!(same_shape(&popped, &col(2)));

    // A non-row is Go's nil return.
    assert!(pop_row_first_arg(&col(1), &opts)
        .expect("no build failure")
        .is_none());
}

/// NEW COVERAGE: `EvaluateExprWithNull` replaces schema columns by NULL and
/// leaves foreign ones intact.
#[test]
fn evaluate_expr_with_null_nulls_only_schema_columns() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let schema = crate::schema::Schema::new(vec![Column::new(1, int_type())]);
    let expr = func("eq", vec![col(1), col(9)]);

    let result = evaluate_expr_with_null(&expr, &schema, false, &NoColumns, &opts)
        .expect("no build failure");
    let Expression::ScalarFunction(f) = &result else {
        panic!("expected a scalar function")
    };
    assert!(
        matches!(&f.get_args()[0], Expression::Constant(c) if c.value.is_null()),
        "the schema column became NULL"
    );
    assert!(
        matches!(f.get_args()[1], Expression::Column(_)),
        "the foreign column survived"
    );
}

/// NEW COVERAGE: under null-rejection checking, a NULL that came from the
/// nulled schema is neutralized inside `AND` so it cannot decide a result the
/// real row might not have produced.
#[test]
fn evaluate_expr_with_null_neutralizes_schema_null_in_and() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let schema = crate::schema::Schema::new(vec![Column::new(1, int_type())]);
    // `col1 AND col9`: col1 nulls from the schema, col9 stays unknown.
    let expr = func("and", vec![col(1), col(9)]);

    let result =
        evaluate_expr_with_null(&expr, &schema, true, &NoColumns, &opts).expect("no build failure");
    let Expression::ScalarFunction(f) = &result else {
        panic!("expected a scalar function")
    };
    // The schema NULL became 1, the AND identity.
    assert!(
        matches!(&f.get_args()[0], Expression::Constant(c) if c.value == Datum::Int(1)),
        "expected the schema NULL to be neutralized, got {:?}",
        f.get_args()[0]
    );
}

/// NEW COVERAGE: `BuildNotNullExpr` wraps as `NOT(ISNULL(x))`.
#[test]
fn build_not_null_expr_wraps_in_not_isnull() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let built = build_not_null_expr(col(1), &opts).expect("no build failure");
    let Expression::ScalarFunction(outer) = &built else {
        panic!("expected a scalar function")
    };
    assert_eq!(outer.func_name.lowercase(), "not");
    let Expression::ScalarFunction(inner) = &outer.get_args()[0] else {
        panic!("expected a nested scalar function")
    };
    assert_eq!(inner.func_name.lowercase(), "isnull");
}

// ------------------------------------------------------------- NOT push-down

/// GO PORT of `TestPushDownNot` (`util_test.go:265`), first case:
/// `!((a=1||a=1)&&a=1)` becomes `(a!=1&&a!=1)||a!=1`.
///
/// Go's expected value reads `orFunc2 = (a!=1&&a!=1)||a!=1`; the comment above
/// it says `||a=1`, but the code builds `neFunc`.
#[test]
fn go_push_down_not_de_morgan() {
    let builder = PreservingFunctionBuilder;
    let eq = func("eq", vec![col(1), int_const(1)]);
    let or = func("or", vec![eq.clone(), eq.clone()]);
    let and = func("and", vec![or, eq]);
    let not = func("not", vec![and]);
    let original = not.clone();

    let ne = func("ne", vec![col(1), int_const(1)]);
    let and2 = func("and", vec![ne.clone(), ne.clone()]);
    let expected = func("or", vec![and2, ne]);

    let result = push_down_not(&not, &builder);
    assert!(
        same_shape(&result, &expected),
        "expected De Morgan's law to flip the tree"
    );
    // Go asserts the input is not mutated.
    assert!(same_shape(&not, &original));
}

/// GO PORT of `TestPushDownNot`, the double-negation cases (issue 15725).
#[test]
fn go_push_down_not_double_negation() {
    let builder = PreservingFunctionBuilder;

    // `not not (a=1)` optimizes to `a=1`.
    let eq = func("eq", vec![col(1), int_const(1)]);
    let not2 = func("not", vec![func("not", vec![eq.clone()])]);
    assert!(same_shape(&push_down_not(&not2, &builder), &eq));

    // `not not not (a > 1)` optimizes to `a <= 1`.
    let gt = func("gt", vec![col(1), int_const(1)]);
    let not3 = func("not", vec![func("not", vec![func("not", vec![gt])])]);
    let expected = func("le", vec![col(1), int_const(1)]);
    assert!(same_shape(&push_down_not(&not3, &builder), &expected));

    // `not not not not (a <= 1)` optimizes back to `a <= 1`.
    let le = func("le", vec![col(1), int_const(1)]);
    let mut not4 = le.clone();
    for _ in 0..4 {
        not4 = func("not", vec![not4]);
    }
    assert!(same_shape(&push_down_not(&not4, &builder), &le));
}

/// GO PORT of `TestPushDownNot`, the cases where the truth wrapper survives:
/// `not not a` becomes `a is true`, because `a` is not itself a truth value.
#[test]
fn go_push_down_not_wraps_non_predicate_in_is_true() {
    let builder = PreservingFunctionBuilder;

    let not2 = func("not", vec![func("not", vec![col(1)])]);
    let expected = func("istrue_with_null", vec![col(1)]);
    assert!(same_shape(&push_down_not(&not2, &builder), &expected));

    // `not not not a` becomes `not (a is true)`.
    let not3 = func("not", vec![func("not", vec![func("not", vec![col(1)])])]);
    let expected = func("not", vec![func("istrue_with_null", vec![col(1)])]);
    assert!(same_shape(&push_down_not(&not3, &builder), &expected));
}

/// NEW COVERAGE: `GetExprInsideIsTruth` undoes exactly the wrapper the
/// push-down adds.
#[test]
fn get_expr_inside_is_truth_unwraps_both_spellings() {
    let inner = func("eq", vec![col(1), int_const(1)]);
    let wrapped = func(
        "istrue",
        vec![func("istrue_with_null", vec![inner.clone()])],
    );
    assert!(same_shape(get_expr_inside_is_truth(&wrapped), &inner));
    // An unwrapped expression is returned as is.
    assert!(same_shape(get_expr_inside_is_truth(&inner), &inner));
}

/// NEW COVERAGE: `ContainOuterNot` on Go's own doc examples.
#[test]
fn contain_outer_not_matches_doc_examples() {
    // `not(0+(a=1 and b=2))` -- the NOT encloses arithmetic, so true.
    let inner = func(
        "plus",
        vec![
            int_const(0),
            func(
                "and",
                vec![
                    func("eq", vec![col(1), int_const(1)]),
                    func("eq", vec![col(2), int_const(2)]),
                ],
            ),
        ],
    );
    assert!(contain_outer_not(&func("not", vec![inner])));

    // `not(a) and not(b)` -- each NOT sits directly on a leaf, so false.
    let expr = func(
        "and",
        vec![func("not", vec![col(1)]), func("not", vec![col(2)])],
    );
    assert!(!contain_outer_not(&expr));
}

// ------------------------------------------------------------ cast removal

/// NEW COVERAGE: `noPrecisionLossCastCompatible` accepts a widening integer
/// cast and rejects a narrowing one and a signedness change.
#[test]
fn no_precision_loss_cast_compatible_integer_rules() {
    let big = FieldType::new(FieldTypeCode::LongLong);
    let small = FieldType::new(FieldTypeCode::Long);
    assert!(no_precision_loss_cast_compatible(&big, &small));
    assert!(!no_precision_loss_cast_compatible(&small, &big));

    let mut unsigned_big = FieldType::new(FieldTypeCode::LongLong);
    unsigned_big.add_flags(FieldTypeFlags::UNSIGNED);
    assert!(!no_precision_loss_cast_compatible(&unsigned_big, &small));

    // CHAR is excluded entirely -- its padding changes the stored form.
    let char_type = FieldType::new(FieldTypeCode::String);
    assert!(!no_precision_loss_cast_compatible(&char_type, &char_type));
}

/// NEW COVERAGE: `EliminateNoPrecisionLossCast` unwraps a lossless cast on
/// either side of a comparison, and leaves a lossy one alone.
#[test]
fn eliminate_no_precision_loss_cast_unwraps_lossless_cast() {
    let builder = PreservingFunctionBuilder;
    let mut column = Column::new(1, FieldType::new(FieldTypeCode::Long));
    column.index = 1;
    let column = Expression::Column(column);

    let cast = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("cast"),
        FieldType::new(FieldTypeCode::LongLong),
        vec![column.clone()],
    ));
    let expr = func("eq", vec![cast.clone(), int_const(5)]);
    let result = eliminate_no_precision_loss_cast(&expr, &builder);
    assert!(same_shape(
        &result,
        &func("eq", vec![column.clone(), int_const(5)])
    ));

    // Cast on the right side is unwrapped too.
    let expr = func("eq", vec![int_const(5), cast]);
    let result = eliminate_no_precision_loss_cast(&expr, &builder);
    assert!(same_shape(
        &result,
        &func("eq", vec![int_const(5), column.clone()])
    ));

    // A NARROWING cast is left alone: removing it would change the comparison.
    let narrowing = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("cast"),
        FieldType::new(FieldTypeCode::Tiny),
        vec![column],
    ));
    let expr = func("eq", vec![narrowing, int_const(5)]);
    assert!(same_shape(
        &eliminate_no_precision_loss_cast(&expr, &builder),
        &expr
    ));
}

/// NEW COVERAGE: the `IN` arm needs every list element constant.
#[test]
fn eliminate_no_precision_loss_cast_in_list_requires_constants() {
    let builder = PreservingFunctionBuilder;
    let mut column = Column::new(1, FieldType::new(FieldTypeCode::Long));
    column.index = 1;
    let column = Expression::Column(column);
    let cast = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("cast"),
        FieldType::new(FieldTypeCode::LongLong),
        vec![column.clone()],
    ));

    let expr = func("in", vec![cast.clone(), int_const(1), int_const(2)]);
    let result = eliminate_no_precision_loss_cast(&expr, &builder);
    assert!(same_shape(
        &result,
        &func("in", vec![column, int_const(1), int_const(2)])
    ));

    // A non-constant element blocks the rewrite.
    let expr = func("in", vec![cast, int_const(1), col(9)]);
    assert!(same_shape(
        &eliminate_no_precision_loss_cast(&expr, &builder),
        &expr
    ));
}

// ------------------------------------------------------------ tree predicates

/// GO PORT of `TestDisableParseJSONFlag4Expr` (`util_test.go:400`).
#[test]
fn go_disable_parse_json_flag_4_expr() {
    // A column keeps the flag: its RetType points into the infoschema.
    let mut column = Column::new(1, int_type());
    column
        .ret_type
        .as_mut()
        .expect("a typed column")
        .add_flags(FieldTypeFlags::PARSE_TO_JSON);
    let mut expr = Expression::Column(column);
    disable_parse_json_flag_4_expr(&mut expr);
    assert!(
        expr.static_type().expect("a type").flags() & FieldTypeFlags::PARSE_TO_JSON != 0,
        "a column keeps the flag"
    );

    // A correlated column keeps it too.
    let mut column = Column::new(1, int_type());
    column
        .ret_type
        .as_mut()
        .expect("a typed column")
        .add_flags(FieldTypeFlags::PARSE_TO_JSON);
    let mut expr = Expression::CorrelatedColumn(CorrelatedColumn { column, data: None });
    disable_parse_json_flag_4_expr(&mut expr);
    assert!(expr.static_type().expect("a type").flags() & FieldTypeFlags::PARSE_TO_JSON != 0);

    // A scalar function loses it.
    let mut ret_type = int_type();
    ret_type.add_flags(FieldTypeFlags::PARSE_TO_JSON);
    let mut expr = Expression::ScalarFunction(ScalarFunction::new(
        CiString::new("abs"),
        ret_type,
        vec![col(1)],
    ));
    disable_parse_json_flag_4_expr(&mut expr);
    assert!(expr.static_type().expect("a type").flags() & FieldTypeFlags::PARSE_TO_JSON == 0);
}

/// GO PORT of `TestGetUint64FromConstant` (`util_test.go:155`), minus the
/// `ParamMarker` case -- see the narrowing on
/// [`super::predicates::get_uint64_from_constant`].
#[test]
fn go_get_uint64_from_constant() {
    let null = Expression::Constant(Constant::new(Datum::Null, int_type()));
    assert_eq!(get_uint64_from_constant(&null, &NoColumns), Some((0, true)));

    // A negative signed value is not usable.
    let negative = Expression::Constant(Constant::new(Datum::Int(-1), int_type()));
    assert_eq!(get_uint64_from_constant(&negative, &NoColumns), None);

    let one = Expression::Constant(Constant::new(Datum::Int(1), int_type()));
    assert_eq!(get_uint64_from_constant(&one, &NoColumns), Some((1, false)));

    let unsigned = Expression::Constant(Constant::new(Datum::UInt(1), int_type()));
    assert_eq!(
        get_uint64_from_constant(&unsigned, &NoColumns),
        Some((1, false))
    );

    // A deferred expression is evaluated.
    let mut deferred = Constant::new(Datum::Null, int_type());
    deferred.deferred_expr = Some(Box::new(int_const(1)));
    assert_eq!(
        get_uint64_from_constant(&Expression::Constant(deferred), &NoColumns),
        Some((1, false))
    );

    // A non-constant is not usable.
    assert_eq!(get_uint64_from_constant(&col(1), &NoColumns), None);
}

/// GO PORT of `TestGetStrIntFromConstant` (`util_test.go:213`).
#[test]
fn go_get_str_int_from_constant() {
    // A non-constant is an error.
    assert!(get_string_from_constant(&col(1), &NoColumns).is_err());

    // NULL reads as "is null".
    let null = Expression::Constant(Constant::new(
        Datum::Null,
        FieldType::new(FieldTypeCode::Null),
    ));
    assert_eq!(
        get_string_from_constant(&null, &NoColumns).expect("no error"),
        None
    );

    assert_eq!(
        get_string_from_constant(&int_const(1), &NoColumns).expect("no error"),
        Some("1".to_owned())
    );

    // `GetIntFromConstant` goes through the string form, so a non-numeric
    // string is "is null" and NOT an error.
    assert_eq!(
        get_int_from_constant(&str_const("abc"), &NoColumns).expect("no error"),
        None
    );
    assert_eq!(
        get_int_from_constant(&str_const("123"), &NoColumns).expect("no error"),
        Some(123)
    );
}

/// GO PORT of `TestProjectionBenefitsFromPushedDown` (`util_test.go:483`).
#[test]
fn go_projection_benefits_from_pushed_down() {
    // Pure column refs benefit only when they PRUNE.
    assert!(projection_benefits_from_pushed_down(&[col(0), col(1)], 5));
    assert!(!projection_benefits_from_pushed_down(&[col(0), col(1)], 2));

    // The JSON functions TiKV evaluates well.
    let json_exprs = vec![
        col(0),
        func("json_extract", vec![col(1), str_const("$.a")]),
        func("json_depth", vec![col(1)]),
        func("json_length", vec![col(1)]),
        func("json_type", vec![col(1)]),
        func("json_valid", vec![col(1)]),
        func("json_contains", vec![col(1), str_const("1")]),
        func("json_contains_path", vec![col(1), str_const("one")]),
        func("json_keys", vec![col(1)]),
        func("json_search", vec![col(1), str_const("one")]),
        func("json_memberof", vec![str_const("1"), col(1)]),
        func("json_overlaps", vec![col(1), col(2)]),
    ];
    assert!(projection_benefits_from_pushed_down(&json_exprs, 3));

    // A bare JSON_UNQUOTE does not benefit...
    let bare_unquote = vec![func("json_unquote", vec![col(1)])];
    assert!(!projection_benefits_from_pushed_down(&bare_unquote, 3));

    // ...but the `->>` spelling does.
    let arrow = vec![func(
        "json_unquote",
        vec![func(
            "cast",
            vec![func("json_extract", vec![col(1), str_const("$.a")])],
        )],
    )];
    assert!(projection_benefits_from_pushed_down(&arrow, 3));

    // Anything else blocks the push-down.
    assert!(!projection_benefits_from_pushed_down(
        &[func("abs", vec![col(1)])],
        3
    ));
}

/// NEW COVERAGE: `IsRuntimeConstExpr` counts a correlated column but not a
/// plain column, and rejects an unfoldable call.
#[test]
fn is_runtime_const_expr_rules() {
    assert!(is_runtime_const_expr(&int_const(1)));
    assert!(is_runtime_const_expr(&cor_col(1, None)));
    assert!(!is_runtime_const_expr(&col(1)));
    assert!(is_runtime_const_expr(&func(
        "plus",
        vec![int_const(1), cor_col(2, None)]
    )));
    assert!(!is_runtime_const_expr(&func("rand", vec![])));
    assert!(!is_runtime_const_expr(&func(
        "plus",
        vec![int_const(1), col(2)]
    )));
}

/// NEW COVERAGE: the three "does re-evaluation change the answer" predicates
/// disagree in exactly the ways Go's tables make them.
#[test]
fn mutability_predicates_disagree_as_the_tables_dictate() {
    let rand = func("rand", vec![]);
    let now = func("now", vec![]);
    let abs = func("abs", vec![col(1)]);

    // RAND is both unfoldable and mutable.
    assert!(check_non_deterministic(&rand));
    assert!(is_mutable_effects_expr(&rand));
    assert!(!is_immutable_func(&rand));

    // NOW is mutable but NOT in the unfoldable table.
    assert!(!check_non_deterministic(&now));
    assert!(is_mutable_effects_expr(&now));
    assert!(!is_immutable_func(&now));

    // ABS is neither.
    assert!(!check_non_deterministic(&abs));
    assert!(!is_mutable_effects_expr(&abs));
    assert!(is_immutable_func(&abs));

    // A COLUMN is "immutable" here: the predicate asks about re-evaluation,
    // not constancy.
    assert!(is_immutable_func(&col(1)));
}

/// NEW COVERAGE: a deferred constant is inspected through its wrapped
/// expression.
#[test]
fn is_mutable_effects_expr_sees_through_deferred_constant() {
    let mut constant = Constant::new(Datum::Int(1), int_type());
    constant.deferred_expr = Some(Box::new(func("rand", vec![])));
    assert!(is_mutable_effects_expr(&Expression::Constant(constant)));
}

/// NEW COVERAGE: `RemoveDupExprs` drops repeats but never a mutable one.
#[test]
fn remove_dup_exprs_keeps_mutable_repeats() {
    let eq = func("eq", vec![col(1), int_const(1)]);
    let deduped = remove_dup_exprs(vec![eq.clone(), eq.clone(), col(2)]);
    assert_eq!(deduped.len(), 2);

    let rand = func("rand", vec![]);
    let kept = remove_dup_exprs(vec![rand.clone(), rand]);
    assert_eq!(
        kept.len(),
        2,
        "evaluating RAND twice is not evaluating it once"
    );
}

/// NEW COVERAGE: `CheckFuncInExpr` finds a nested call by name.
#[test]
fn check_func_in_expr_finds_nested_call() {
    let expr = func("and", vec![func("abs", vec![col(1)]), col(2)]);
    assert!(check_func_in_expr(&expr, "abs"));
    assert!(!check_func_in_expr(&expr, "sleep"));
}

/// NEW COVERAGE: `GetRowLen` and `CheckArgsNotMultiColumnRow`.
#[test]
fn row_length_helpers() {
    assert_eq!(get_row_len(&col(1)), 1);
    assert_eq!(get_row_len(&func("row", vec![col(1), col(2)])), 2);

    assert!(check_args_not_multi_column_row(&[col(1), int_const(2)]).is_ok());
    assert_eq!(
        check_args_not_multi_column_row(&[col(1), func("row", vec![col(2), col(3)])]),
        Err(1)
    );
}

/// NEW COVERAGE: `ContainVirtualColumn` and `ContainCorrelatedColumn`.
#[test]
fn contain_virtual_and_correlated_columns() {
    let mut virt = Column::new(10, int_type());
    virt.virtual_expr = Some(Box::new(col(1)));
    let with_virtual = func("abs", vec![Expression::Column(virt)]);
    assert!(contain_virtual_column(std::slice::from_ref(&with_virtual)));
    assert!(!contain_virtual_column(&[col(1)]));

    let with_cor = func("abs", vec![cor_col(1, None)]);
    assert!(contain_correlated_column(std::slice::from_ref(&with_cor)));
    assert!(!contain_correlated_column(&[col(1)]));
}

/// NEW COVERAGE: `MaybeOverOptimized4PlanCache` fires only when the plan cache
/// is on AND a lazy constant is present.
#[test]
fn maybe_over_optimized_needs_both_cache_and_lazy_constant() {
    let mut param = Constant::new(Datum::Int(1), int_type());
    param.param_marker = Some(crate::constant::ParamMarker { order: 0 });
    let with_param = func("eq", vec![col(1), Expression::Constant(param)]);
    let plain = func("eq", vec![col(1), int_const(1)]);

    assert!(maybe_over_optimized_4_plan_cache(
        true,
        std::slice::from_ref(&with_param)
    ));
    assert!(!maybe_over_optimized_4_plan_cache(
        false,
        std::slice::from_ref(&with_param)
    ));
    assert!(!maybe_over_optimized_4_plan_cache(true, &[plain]));
}

/// NEW COVERAGE: `RemoveMutableConst` evaluates the deferred expression into
/// the value and drops both mutability markers.
#[test]
fn remove_mutable_const_freezes_the_value() {
    let mut constant = Constant::new(Datum::Null, int_type());
    constant.deferred_expr = Some(Box::new(int_const(42)));
    constant.param_marker = Some(crate::constant::ParamMarker { order: 0 });
    let mut exprs = vec![func("abs", vec![Expression::Constant(constant)])];

    remove_mutable_const(&mut exprs, &NoColumns).expect("evaluation succeeds");

    let Expression::ScalarFunction(f) = &exprs[0] else {
        panic!("expected a scalar function")
    };
    let Expression::Constant(constant) = &f.get_args()[0] else {
        panic!("expected a constant")
    };
    assert_eq!(constant.value, Datum::Int(42));
    assert!(constant.deferred_expr.is_none());
    assert!(constant.param_marker.is_none());
}

/// NEW COVERAGE: `ExprsHasSideEffects` finds `SET @var` and `SLEEP` nested.
#[test]
fn exprs_has_side_effects_finds_nested_calls() {
    assert!(exprs_has_side_effects(&[func(
        "and",
        vec![col(1), func("sleep", vec![int_const(1)])]
    )]));
    assert!(expr_has_set_var_or_sleep(&func(
        "setvar",
        vec![str_const("a")]
    )));
    assert!(!exprs_has_side_effects(&[func("abs", vec![col(1)])]));
}

/// NEW COVERAGE: `IsConstNull` recognizes `col <op> NULL` and excludes a
/// deferred null, which is not yet known to be null.
#[test]
fn is_const_null_excludes_deferred_null() {
    let null = Expression::Constant(Constant::new(Datum::Null, int_type()));
    assert!(is_const_null(&func("eq", vec![col(1), null.clone()])));
    assert!(!is_const_null(&func("plus", vec![col(1), null])));

    let mut deferred = Constant::new(Datum::Null, int_type());
    deferred.deferred_expr = Some(Box::new(int_const(1)));
    assert!(!is_const_null(&func(
        "eq",
        vec![col(1), Expression::Constant(deferred)]
    )));
}

/// NEW COVERAGE: `IsColOpCol` needs both sides to be columns.
#[test]
fn is_col_op_col_needs_two_columns() {
    let Expression::ScalarFunction(both) = func("eq", vec![col(1), col(2)]) else {
        panic!("expected a scalar function")
    };
    assert!(is_col_op_col(&both).is_some());
    assert!(extract_columns_from_col_op_col(&both).is_some());

    let Expression::ScalarFunction(mixed) = func("eq", vec![col(1), int_const(2)]) else {
        panic!("expected a scalar function")
    };
    assert!(is_col_op_col(&mixed).is_none());
}

/// NEW COVERAGE: `checkCollationStrictness` -- a stricter collation is
/// acceptable, a weaker one is not.
#[test]
fn check_collation_strictness_direction() {
    use super::traits::check_collation_strictness;
    // Same group.
    assert!(check_collation_strictness(
        "utf8mb4_general_ci",
        "utf8_general_ci"
    ));
    // general_ci (1) -> bin (3) and binary (4) are stricter.
    assert!(check_collation_strictness(
        "utf8mb4_general_ci",
        "utf8mb4_bin"
    ));
    assert!(check_collation_strictness("utf8mb4_general_ci", "binary"));
    // The reverse is weaker.
    assert!(!check_collation_strictness(
        "utf8mb4_bin",
        "utf8mb4_general_ci"
    ));
    // An unlisted collation is never acceptable.
    assert!(!check_collation_strictness("utf8mb4_bin", "gbk_bin"));
}

// ------------------------------------------------------------------- folding

/// NEW COVERAGE: `FoldConstant` reduces a wholly constant tree to its value.
#[test]
fn fold_constant_reduces_constant_tree() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let expr = func("plus", vec![int_const(2), int_const(3)]);
    let folded = fold_constant(&expr, &NoColumns, &opts);
    let Expression::Constant(constant) = &folded else {
        panic!("expected a folded constant, got {folded:?}")
    };
    assert_eq!(constant.value, Datum::Int(5));
}

/// NEW COVERAGE: an unfoldable call is never folded, even with constant
/// arguments.
#[test]
fn fold_constant_refuses_unfoldable_call() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let expr = func("rand", vec![]);
    assert!(matches!(
        fold_constant(&expr, &NoColumns, &opts),
        Expression::ScalarFunction(_)
    ));
}

/// NEW COVERAGE: a tree containing a column does not fold.
#[test]
fn fold_constant_leaves_non_constant_tree() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let expr = func("plus", vec![col(1), int_const(3)]);
    assert!(matches!(
        fold_constant(&expr, &NoColumns, &opts),
        Expression::ScalarFunction(_)
    ));
}

/// NEW COVERAGE: `ISNULL(x)` collapses to 0 for an `x` the type system
/// declares NOT NULL, with no evaluation at all (Go's `isNullHandler`).
#[test]
fn fold_constant_is_null_handler_uses_not_null_flag() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let mut ret_type = int_type();
    ret_type.add_flags(FieldTypeFlags::NOT_NULL);
    let mut column = Column::new(1, ret_type);
    column.index = 1;

    let expr = func("isnull", vec![Expression::Column(column)]);
    let folded = fold_constant(&expr, &NoColumns, &opts);
    let Expression::Constant(constant) = &folded else {
        panic!("expected a folded constant, got {folded:?}")
    };
    assert_eq!(constant.value, Datum::Int(0));

    // Without the flag it stays unfolded: the column may still be NULL.
    let expr = func("isnull", vec![col(1)]);
    assert!(matches!(
        fold_constant(&expr, &NoColumns, &opts),
        Expression::ScalarFunction(_)
    ));
}

/// NEW COVERAGE: `IF` folds to the branch a constant condition selects, and
/// stays put when the condition is not constant (Go's `ifFoldHandler`).
#[test]
fn fold_constant_if_handler_selects_a_branch() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);

    let taken = func("if", vec![int_const(1), int_const(10), int_const(20)]);
    let Expression::Constant(constant) = fold_constant(&taken, &NoColumns, &opts) else {
        panic!("expected the then-branch")
    };
    assert_eq!(constant.value, Datum::Int(10));

    let not_taken = func("if", vec![int_const(0), int_const(10), int_const(20)]);
    let Expression::Constant(constant) = fold_constant(&not_taken, &NoColumns, &opts) else {
        panic!("expected the else-branch")
    };
    assert_eq!(constant.value, Datum::Int(20));

    // A NULL condition takes the else branch, as `EvalInt`'s isNull does.
    let null_cond = func(
        "if",
        vec![
            Expression::Constant(Constant::new(Datum::Null, int_type())),
            int_const(10),
            int_const(20),
        ],
    );
    let Expression::Constant(constant) = fold_constant(&null_cond, &NoColumns, &opts) else {
        panic!("expected the else-branch")
    };
    assert_eq!(constant.value, Datum::Int(20));

    // A non-constant condition leaves the IF alone.
    let unknown = func("if", vec![col(1), int_const(10), int_const(20)]);
    assert!(matches!(
        fold_constant(&unknown, &NoColumns, &opts),
        Expression::ScalarFunction(_)
    ));
}

/// NEW COVERAGE: `IFNULL` returns the first argument when it is not NULL, and
/// the second when it is (Go's `ifNullFoldHandler`).
#[test]
fn fold_constant_ifnull_handler() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);

    let first_wins = func("ifnull", vec![int_const(7), int_const(9)]);
    let Expression::Constant(constant) = fold_constant(&first_wins, &NoColumns, &opts) else {
        panic!("expected the first argument")
    };
    assert_eq!(constant.value, Datum::Int(7));

    let second_wins = func(
        "ifnull",
        vec![
            Expression::Constant(Constant::new(Datum::Null, int_type())),
            int_const(9),
        ],
    );
    let Expression::Constant(constant) = fold_constant(&second_wins, &NoColumns, &opts) else {
        panic!("expected the second argument")
    };
    assert_eq!(constant.value, Datum::Int(9));
}

/// NEW COVERAGE: `CASE` returns the first constant-true body, and the trailing
/// ELSE when every condition is false (Go's `caseWhenHandler`).
#[test]
fn fold_constant_case_when_handler() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);

    // CASE WHEN 0 THEN 1 WHEN 1 THEN 2 ELSE 3 END -> 2.
    let expr = func(
        "case",
        vec![
            int_const(0),
            int_const(1),
            int_const(1),
            int_const(2),
            int_const(3),
        ],
    );
    let Expression::Constant(constant) = fold_constant(&expr, &NoColumns, &opts) else {
        panic!("expected the second body")
    };
    assert_eq!(constant.value, Datum::Int(2));

    // CASE WHEN 0 THEN 1 ELSE 3 END -> 3.
    let expr = func("case", vec![int_const(0), int_const(1), int_const(3)]);
    let Expression::Constant(constant) = fold_constant(&expr, &NoColumns, &opts) else {
        panic!("expected the else body")
    };
    assert_eq!(constant.value, Datum::Int(3));

    // A non-constant condition stops the walk immediately.
    let expr = func("case", vec![col(1), int_const(1), int_const(3)]);
    assert!(matches!(
        fold_constant(&expr, &NoColumns, &opts),
        Expression::ScalarFunction(_)
    ));
}

/// NEW COVERAGE: a deferred constant folds to its evaluated value while
/// KEEPING the deferral, so plan-cache reuse still re-evaluates it.
#[test]
fn fold_constant_evaluates_deferred_constant() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let mut constant = Constant::new(Datum::Null, int_type());
    constant.deferred_expr = Some(Box::new(int_const(5)));

    let folded = fold_constant(&Expression::Constant(constant), &NoColumns, &opts);
    let Expression::Constant(constant) = &folded else {
        panic!("expected a constant")
    };
    assert_eq!(constant.value, Datum::Int(5));
    assert!(constant.deferred_expr.is_some());
}

/// NEW COVERAGE: folding stamps NOT NULL on a non-null result, which is what
/// `SHOW COLUMNS` on a view reads.
#[test]
fn fold_constant_sets_not_null_flag() {
    let builder = PreservingFunctionBuilder;
    let opts = opts(&builder);
    let expr = func("plus", vec![int_const(1), int_const(1)]);
    let folded = fold_constant(&expr, &NoColumns, &opts);
    assert!(
        folded.static_type().expect("a type").flags() & FieldTypeFlags::NOT_NULL != 0,
        "a non-null folded value is stamped NOT NULL"
    );
}

// ------------------------------------------------------- normalized explain

/// NEW COVERAGE: normalization replaces every literal by `?`, which is what
/// makes two statements differing only in literals share a digest.
#[test]
fn explain_normalized_info_replaces_constants() {
    let expr = func("eq", vec![col(1), int_const(42)]);
    assert_eq!(explain_normalized_info(&expr), "eq(t.c1, ?)");
}

/// NEW COVERAGE: a column with no original name prints as `?`.
#[test]
fn explain_normalized_info_unnamed_column() {
    let expr = Expression::Column(Column::new(1, int_type()));
    assert_eq!(explain_normalized_info(&expr), "?");
}

/// NEW COVERAGE: an `IN` list collapses to `...` in the in-list form, so
/// `a IN (1,2)` and `a IN (1,2,3)` share a digest.
#[test]
fn explain_normalized_info_4_in_list_collapses_the_list() {
    let two = func("in", vec![col(1), int_const(1), int_const(2)]);
    let three = func("in", vec![col(1), int_const(1), int_const(2), int_const(3)]);
    assert_eq!(explain_normalized_info_4_in_list(&two), "in(...)");
    assert_eq!(
        explain_normalized_info_4_in_list(&two),
        explain_normalized_info_4_in_list(&three)
    );
    // The plain form does NOT collapse it.
    assert_eq!(explain_normalized_info(&two), "in(t.c1, ?, ?)");
}

/// NEW COVERAGE: the sorted list is order-independent, which is the property
/// plan digests rely on.
#[test]
fn sorted_explain_normalized_expression_list_is_order_independent() {
    let a = func("eq", vec![col(1), int_const(1)]);
    let b = func("gt", vec![col(2), int_const(2)]);
    assert_eq!(
        sorted_explain_normalized_expression_list(&[a.clone(), b.clone()]),
        sorted_explain_normalized_expression_list(&[b, a])
    );
}

// ------------------------------------------------------- the closed boundary

/// NEW COVERAGE: [`RealFunctionBuilder`] closes the [`super::builder`]
/// boundary by routing rebuilds through the crate's real `NewFunction`.
///
/// The observable difference from [`PreservingFunctionBuilder`] is that Go's
/// construction FOLDS the node it just built, so a substitution that makes a
/// subtree wholly constant comes back as a `Constant` rather than as a
/// still-unfolded call.
#[test]
fn real_function_builder_folds_what_preserving_does_not() {
    let schema = crate::schema::Schema::new(vec![Column::new(1, int_type())]);
    // `c1 + 1` with `c1 -> 2` is `2 + 1`, which Go folds to 3 during
    // construction.
    let expr = func("plus", vec![col(1), int_const(1)]);
    let new_exprs = vec![int_const(2)];

    let preserving = PreservingFunctionBuilder;
    let result = column_substitute(&expr, &schema, &new_exprs, &opts(&preserving));
    assert!(
        matches!(result, Expression::ScalarFunction(_)),
        "the deferring builder leaves the call unfolded"
    );

    let real = super::builder::RealFunctionBuilder::new(&NoColumns);
    let real_opts = SubstituteOptions::new(&real);
    let result = column_substitute(&expr, &schema, &new_exprs, &real_opts);
    let Expression::Constant(constant) = &result else {
        panic!("the real builder folds during construction, got {result:?}")
    };
    assert_eq!(constant.value, Datum::Int(3));
}

/// NEW COVERAGE: the real builder also applies Go's arity check, so a rewrite
/// that would produce a malformed call reports a failure instead of building
/// one.
#[test]
fn real_function_builder_rejects_bad_arity() {
    use super::builder::FunctionBuilder;
    let real = super::builder::RealFunctionBuilder::new(&NoColumns);
    // `ISNULL` takes exactly one argument.
    assert!(real
        .new_function("isnull", None, vec![col(1), col(2)])
        .is_err());
    assert!(real.new_function("isnull", None, vec![col(1)]).is_ok());
}
