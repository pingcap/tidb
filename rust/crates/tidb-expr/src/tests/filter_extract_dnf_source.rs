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

//! GO PORT of `pkg/expression/integration_test/integration_test.go:1766`
//! `TestFilterExtractFromDNF` (batch part10).
//!
//! The Go test builds `select * from t where <expr>` through the full planner,
//! runs `expression.PushDownNot` (`util.go:1141`) over the selection's
//! conditions, then `expression.ExtractFiltersFromDNFs` (`util.go:1204`),
//! sorts by `HashCode`, and pins `StringifyExpressionsWithCtx` output
//! (`expression.go:1334`). The Rust side carries the two pure transforms in
//! this crate (`expr_util::push_not::push_down_not`,
//! `expr_util::normal_form::extract_filters_from_dnfs`), so each Go case is
//! rebuilt here as an already-resolved tree over the same three columns
//! (`test.t.a/b/c`) — the plan-building steps the Go harness performs are what
//! this evaluator cannot see, not the transforms under test.

use crate::expr_util::normal_form::extract_filters_from_dnfs;
use crate::expr_util::push_not::push_down_not;
use crate::expr_util::builder::PreservingFunctionBuilder;
use crate::column::Column;
use crate::constant::Constant;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;
use tidb_datatype::{Datum, FieldType, FieldTypeCode};

/// A column standing for one of the Go test table's columns. `UniqueID` plays
/// the role of `pkg/expression.Column.UniqueID`; `orig_name` is what both
/// `StringifyExpressionsWithCtx` and this local renderer print.
fn table_column(unique_id: i64, name: &str) -> Expression {
    let mut column = Column::new(unique_id, FieldType::new(FieldTypeCode::LongLong));
    column.orig_name = format!("test.t.{name}").to_string();
    Expression::Column(column)
}

fn int_const(v: i64) -> Expression {
    Expression::Constant(Constant::new(Datum::Int(v), FieldType::new(FieldTypeCode::LongLong)))
}

/// `newFunctionWithMockCtx` shape: no inference, direct name + args.
fn f(name: &str, args: Vec<Expression>) -> Expression {
    Expression::ScalarFunction(ScalarFunction::new(
        CiString::new(name),
        FieldType::new(FieldTypeCode::LongLong),
        args,
    ))
}

fn eq(column: &Expression, v: i64) -> Expression {
    f("eq", vec![column.clone(), int_const(v)])
}

fn gt(column: &Expression, v: i64) -> Expression {
    f("gt", vec![column.clone(), int_const(v)])
}

fn lt(column: &Expression, v: i64) -> Expression {
    f("lt", vec![column.clone(), int_const(v)])
}

fn and(args: Vec<Expression>) -> Expression {
    f("and", args)
}

fn or(args: Vec<Expression>) -> Expression {
    f("or", args)
}

/// Local stand-in for `StringifyExpressionsWithCtx`
/// (`expression.go:1334`) restricted to the node shapes these cases contain:
/// a plain `1` constant renders bare (`strconv.FormatInt` through
/// `types.Datum.StringWithCtx` would print identically here) and functions
/// render `name(arg, arg)` space-comma-separated as Go does.
fn render(expr: &Expression) -> String {
    match expr {
        Expression::Column(column) => column.orig_name.clone(),
        Expression::Constant(constant) => match &constant.value {
            Datum::Int(value) => value.to_string(),
            other => panic!("test only pins integer constants, got {other:?}"),
        },
        Expression::ScalarFunction(function) => {
            let mut buffer = format!("{}(", function.func_name.lowercase());
            let args = function.get_args();
            for (index, arg) in args.iter().enumerate() {
                buffer.push_str(&render(arg));
                if index + 1 < args.len() {
                    buffer.push_str(", ");
                }
            }
            buffer.push(')');
            buffer
        }
        other => panic!("test only pins column/const/func trees, got {other:?}"),
    }
}

/// Go `expression.StringifyExpressionsWithCtx(ctx, conds)` on a slice:
/// bracketed, space-separated.
fn render_list(exprs: &[Expression]) -> String {
    let items: Vec<String> = exprs.iter().map(render).collect();
    format!("[{}]", items.join(" "))
}

/// The whole Go body minus the planner plumbing: PushDownNot, extract, sort
/// by HashCode, stringify. Sorting replicates
/// `bytes.Compare(afterFunc[i].HashCode(), afterFunc[j].HashCode())`.
fn push_down_extract_and_render(condition: Expression) -> String {
    let builder = PreservingFunctionBuilder;
    let pushed = push_down_not(&condition, &builder);
    let mut after = extract_filters_from_dnfs(vec![pushed]);
    // Go's HashCode is `[retType + operands...]`; Rust's [`Expression::hash_code`
    // mirrors it but borrows mutably (it caches), so take a mutable copy.
    after.sort_by_key(|expr| {
        let mut copy = expr.clone();
        copy.hash_code().to_vec()
    });
    render_list(&after)
}

#[test]
fn test_filter_extract_from_dnf_case_table() {
    // Column handles for the `t(a int, b int, c int)` table the Go harness
    // plans against; UniqueIDs only need to be stable inside the case.
    let a = table_column(1, "a");
    let b = table_column(2, "b");
    let c = table_column(3, "c");

    // Case 1: repeated identical disjuncts collapse to the common conjunct.
    assert_eq!(
        push_down_extract_and_render(or(vec![
            eq(&a, 1),
            eq(&a, 1),
            eq(&a, 1),
        ])),
        "[eq(test.t.a, 1)]"
    );

    // Case 2: `(a=1 or a=1 or (a=1 and b=1))` — every disjunct implies a=1,
    // so the DNF is deleted outright and only the extraction remains.
    assert_eq!(
        push_down_extract_and_render(or(vec![
            eq(&a, 1),
            eq(&a, 1),
            and(vec![eq(&a, 1), eq(&b, 1)]),
        ])),
        "[eq(test.t.a, 1)]"
    );

    // Case 3: no conjunct occurs in EVERY disjunct, so the condition passes
    // through untouched. `tidbparser` groups `X or Y or Z` as
    // `or(or(X, Y), Z)` (binary left-nested), which is why the expected
    // string nests the `or`s even though nothing was rewritten.
    assert_eq!(
        push_down_extract_and_render(or(vec![
            or(vec![and(vec![eq(&a, 1), eq(&a, 1)]), eq(&a, 1)]),
            eq(&b, 1),
        ])),
        "[or(or(and(eq(test.t.a, 1), eq(test.t.a, 1)), eq(test.t.a, 1)), eq(test.t.b, 1))]"
    );

    // Case 4: a=1 lifted out of every disjunct; the right-associated remainder
    // stays behind. Note the extracted filter prints FIRST because the Go
    // harness sorts by HashCode before stringifying.
    assert_eq!(
        push_down_extract_and_render(or(vec![
            and(vec![eq(&a, 1), eq(&b, 2)]),
            and(vec![eq(&a, 1), eq(&b, 3)]),
            and(vec![eq(&a, 1), eq(&b, 4)]),
        ])),
        "[eq(test.t.a, 1) or(eq(test.t.b, 2), or(eq(test.t.b, 3), eq(test.t.b, 4)))]"
    );

    // Case 5: `{a,b,c}` / `{a,b}` / `{a,b,c>2,c<3}` share exactly {a=1,
    // b=1}; because the LAST disjunct is fully consumed (`c=1`, `c>2`,
    // `c<3` are not shared), Go's `onlyNeedExtracted` path DELETES the whole
    // DNF instead of leaving a remainder -- so only the two lifted filters,
    // hash-sorted, survive.
    assert_eq!(
        push_down_extract_and_render(or(vec![
            and(vec![eq(&a, 1), eq(&b, 1), eq(&c, 1)]),
            and(vec![eq(&a, 1), eq(&b, 1)]),
            and(vec![eq(&a, 1), eq(&b, 1), gt(&c, 2), lt(&c, 3)]),
        ])),
        "[eq(test.t.a, 1) eq(test.t.b, 1)]"
    );
}
