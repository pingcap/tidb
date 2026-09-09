// Copyright 2026 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Sources: `pkg/planner/core/exhaust_physical_plans_test.go` and the range
//! portion of `integration_test.go::TestPlanCacheForIndexJoinRangeFallback`.
//! Expressions below are the already-rewritten nodes from Go's test inputs.

use super::*;
use tidb_ast::CiString;
use tidb_datatype::FieldTypeCode;
use tidb_expr::{constant::Constant, expression::ScalarFunction};

/// Go `TestPlanCacheForIndexJoinRangeFallback`: longer replacement parameters
/// exceed the planning quota, but rebuilding a cached range has no quota.
/// Session cache admission, warnings and EXPLAIN remain integration work.
#[test]
fn plan_cache_for_index_join_range_fallback() {
    let columns = [col(1), col(3)];
    let schema = Schema::new(columns.to_vec());
    let lookup = IndexJoinRuntimeProp::new(vec![], vec![col(6)], vec![col(1)], 1.0, false);
    let mut args = vec![c(3)];
    for order in 0..3 {
        let mut constant = Constant::new(
            Datum::new_collation_string("", Collation::Utf8Mb4Bin),
            col(3).ret_type.unwrap(),
        );
        constant.param_marker = Some(tidb_expr::constant::ParamMarker { order });
        args.push(Expression::Constant(constant));
    }
    let conditions = [f("in", args)];
    let evaluate = |values: &[&str], expression: &Expression| match expression {
        Expression::Constant(constant) => match &constant.param_marker {
            Some(marker) => Ok(Datum::new_collation_string(
                values[marker.order as usize],
                Collation::Utf8Mb4Bin,
            )),
            None => constant.eval(),
        },
        _ => crate::ranger::points::evaluate_static(expression),
    };
    let short = |constant: &Expression| evaluate(&["a", "b", "c"], constant);
    let long = |constant: &Expression| evaluate(&["aaaaaa", "bbbbbb", "cccccc"], constant);
    let fallback = std::cell::Cell::new(false);
    let record = |_: i64| fallback.set(true);
    let builder = IndexJoinPathRangeBuilder {
        columns: &columns,
        lengths: &[-1, -1],
        lookup: &lookup,
        inner_schema: &schema,
        pushed_conditions: &conditions,
        eval_expression: &short,
        range_max_size: 1260,
        record_range_fallback: &record,
        regard_null_as_point: true,
        opt_prefix_index_single_scan: true,
    };
    let cached = builder.build_for_plan_cache().unwrap().0.unwrap();
    assert!(!fallback.get());
    assert_eq!(cached.ranges.len(), 3);
    assert_eq!(cached.used_columns(), 2);
    let normal_long = IndexJoinPathRangeBuilder {
        eval_expression: &long,
        ..builder
    }
    .build(false)
    .unwrap()
    .0
    .unwrap();
    assert!(fallback.get());
    assert_eq!(normal_long.used_columns(), 1);

    let template = cached.range_template.as_ref().unwrap();
    let rebuilt = template.rebuild(&cached.ranges, &long).unwrap();
    assert_eq!(rebuilt.len(), 3);
    for (range, text) in rebuilt.iter().zip(["aaaaaa", "bbbbbb", "cccccc"]) {
        assert_eq!(
            range.low_val,
            vec![Datum::Null, Datum::Bytes(text.as_bytes().to_vec())]
        );
        assert_eq!(range.high_val, range.low_val);
    }
    let again = template.rebuild(&rebuilt, &long).unwrap();
    assert_eq!(
        again
            .iter()
            .map(|range| range.to_display_string())
            .collect::<Vec<_>>(),
        rebuilt
            .iter()
            .map(|range| range.to_display_string())
            .collect::<Vec<_>>()
    );
}

fn col(id: i64) -> Column {
    let mut ty = FieldType::new(if [3, 5, 8].contains(&id) {
        FieldTypeCode::Varchar
    } else {
        FieldTypeCode::LongLong
    });
    if ty.is_string() {
        ty.set_charset_name(if id == 5 { "ascii" } else { "utf8mb4" });
        ty.set_collation_name(if id == 5 { "ascii_bin" } else { "utf8mb4_bin" });
    }
    Column::new(id, ty)
}

fn c(id: i64) -> Expression {
    Expression::Column(col(id))
}
fn n(value: i64) -> Expression {
    Expression::Constant(Constant::new(
        Datum::Int(value),
        FieldType::new(FieldTypeCode::LongLong),
    ))
}
fn s(value: &str) -> Expression {
    Expression::Constant(Constant::new(
        Datum::Bytes(value.as_bytes().to_vec()),
        col(3).ret_type.unwrap(),
    ))
}
fn f(op: &str, args: Vec<Expression>) -> Expression {
    let ty = if op == "concat" {
        col(3).ret_type.unwrap()
    } else {
        FieldType::new(FieldTypeCode::LongLong)
    };
    let mut expression =
        Expression::ScalarFunction(ScalarFunction::new(CiString::new(op), ty, args));
    tidb_expr::rewriter::derive_tree_collation(&mut expression).unwrap();
    expression
}
fn eq() -> Expression {
    f("eq", vec![c(1), n(1)])
}
fn bounds(id: i64, upper: &str) -> Vec<Expression> {
    vec![f("gt", vec![c(id), s("a")]), f("lt", vec![c(id), s(upper)])]
}
fn compare() -> Vec<Expression> {
    vec![
        f("gt", vec![c(3), c(8)]),
        f("lt", vec![c(3), f("concat", vec![c(8), s("ab")])]),
    ]
}
fn in_prefix() -> Vec<Expression> {
    vec![
        f("in", vec![c(1), n(1), n(2), n(3)]),
        f("in", vec![c(3), s("a"), s("b"), s("c")]),
    ]
}

fn build(
    keys: &[i64],
    pushed: &[Expression],
    other: Vec<Expression>,
    quota: i64,
    rebuild: bool,
) -> Option<IndexJoinPathRanges> {
    build_with_fallback(keys, pushed, other, quota, rebuild, &|_| {})
}

fn build_with_fallback(
    keys: &[i64],
    pushed: &[Expression],
    other: Vec<Expression>,
    quota: i64,
    rebuild: bool,
    record: &dyn Fn(i64),
) -> Option<IndexJoinPathRanges> {
    let columns: Vec<_> = (1..=5).map(col).collect();
    let schema = Schema::new(columns.clone());
    let lookup = IndexJoinRuntimeProp::new(
        other,
        keys.iter().copied().map(col).collect(),
        keys.iter().copied().map(col).collect(),
        1.0,
        false,
    );
    IndexJoinPathRangeBuilder {
        columns: &columns,
        lengths: &[-1, -1, 2, -1, 2],
        lookup: &lookup,
        inner_schema: &schema,
        pushed_conditions: pushed,
        eval_expression: &crate::ranger::points::evaluate_static,
        range_max_size: quota,
        record_range_fallback: record,
        regard_null_as_point: true,
        opt_prefix_index_single_scan: true,
    }
    .build(rebuild)
    .unwrap()
    .0
}

fn shown(result: &Option<IndexJoinPathRanges>) -> String {
    let items = result.as_ref().map_or_else(Vec::new, |r| {
        r.ranges
            .iter()
            .map(Range::to_display_string)
            .collect::<Vec<_>>()
    });
    format!("[{}]", items.join(" "))
}

fn same_exprs(actual: &[Expression], expected: &[Expression]) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "actual {actual:?}, expected {expected:?}"
    );
    for (actual, expected) in actual.iter().zip(expected) {
        assert!(
            actual.equal(expected),
            "actual {actual:?}, expected {expected:?}"
        );
    }
}

/// Go `TestIndexJoinAnalyzeLookUpFilters`.
#[test]
fn index_join_analyze_lookup_filters() {
    let in_ranges = "[[1 NULL \"a\",1 NULL \"a\"] [1 NULL \"b\",1 NULL \"b\"] [1 NULL \"c\",1 NULL \"c\"] [2 NULL \"a\",2 NULL \"a\"] [2 NULL \"b\",2 NULL \"b\"] [2 NULL \"c\",2 NULL \"c\"] [3 NULL \"a\",3 NULL \"a\"] [3 NULL \"b\",3 NULL \"b\"] [3 NULL \"c\",3 NULL \"c\"]]";
    let dynamic = vec![
        f("gt", vec![c(4), c(9)]),
        f("lt", vec![c(4), f("plus", vec![c(9), n(100)])]),
    ];
    let mut prefix_bounds = vec![eq()];
    prefix_bounds.extend(bounds(3, "aaaaaa"));
    let mut ascii_bounds = vec![eq()];
    ascii_bounds.extend(bounds(5, "aaaaaa"));
    let mut unicode_bounds = vec![eq()];
    unicode_bounds.extend(bounds(3, "一二三"));
    let mut extra_access = in_prefix();
    extra_access.extend(dynamic.clone());
    let mut compare_access = vec![eq()];
    compare_access.extend(compare());
    let cases = vec![
        (
            vec![1, 3],
            vec![],
            vec![],
            "[[NULL,NULL]]".to_owned(),
            vec![0, -1, -1, -1, -1],
            vec![],
            vec![],
            0,
        ),
        (
            vec![3],
            vec![eq()],
            vec![],
            "[]".to_owned(),
            vec![],
            vec![],
            vec![],
            0,
        ),
        (
            vec![2],
            vec![eq()],
            vec![],
            "[[1 NULL,1 NULL]]".to_owned(),
            vec![-1, 0, -1, -1, -1],
            vec![eq()],
            vec![],
            0,
        ),
        (
            vec![2],
            vec![eq()],
            compare(),
            "[[1 NULL NULL,1 NULL NULL]]".to_owned(),
            vec![-1, 0, -1, -1, -1],
            compare_access,
            vec![],
            2,
        ),
        (
            vec![2],
            prefix_bounds.clone(),
            vec![],
            "[(1 NULL \"a\",1 NULL \"aa\"]]".to_owned(),
            vec![-1, 0, -1, -1, -1],
            prefix_bounds,
            bounds(3, "aaaaaa"),
            0,
        ),
        (
            vec![2, 3, 4],
            ascii_bounds.clone(),
            vec![],
            "[(1 NULL NULL NULL \"a\",1 NULL NULL NULL \"aa\"]]".to_owned(),
            vec![-1, 0, 1, 2, -1],
            ascii_bounds,
            bounds(5, "aaaaaa"),
            0,
        ),
        (
            vec![2],
            in_prefix(),
            vec![],
            in_ranges.to_owned(),
            vec![-1, 0, -1, -1, -1],
            in_prefix(),
            vec![in_prefix()[1].clone()],
            0,
        ),
        (
            vec![2],
            in_prefix(),
            dynamic,
            in_ranges
                .replace("\",", "\" NULL,")
                .replace("\"]", "\" NULL]"),
            vec![-1, 0, -1, -1, -1],
            extra_access,
            vec![in_prefix()[1].clone()],
            2,
        ),
        (
            vec![1, 3],
            vec![f("gt", vec![c(2), n(1)])],
            vec![],
            "[(NULL 1,NULL +inf]]".to_owned(),
            vec![0, -1, -1, -1, -1],
            vec![f("gt", vec![c(2), n(1)])],
            vec![],
            0,
        ),
        (
            vec![2],
            unicode_bounds.clone(),
            vec![],
            "[(1 NULL \"a\",1 NULL \"一二\"]]".to_owned(),
            vec![-1, 0, -1, -1, -1],
            unicode_bounds,
            bounds(3, "一二三"),
            0,
        ),
    ];
    for (keys, pushed, other, ranges, offsets, accesses, remained, comparisons) in cases {
        let result = build(&keys, &pushed, other, 0, false);
        assert_eq!(shown(&result), ranges);
        if let Some(result) = result {
            assert_eq!(result.index_to_key, offsets);
            same_exprs(&result.accesses, &accesses);
            same_exprs(&result.remained, &remained);
            assert_eq!(
                result
                    .compare_filters
                    .as_ref()
                    .map_or(0, |m| m.op_args.len()),
                comparisons
            );
        }
    }
    // Go's fifth case has a cast around c for c < g + 10. The cast means
    // this is not a bare next-index-column comparison.
    let other = vec![
        f("gt", vec![c(3), c(8)]),
        f(
            "lt",
            vec![f("cast", vec![c(3)]), f("plus", vec![c(8), n(10)])],
        ),
    ];
    let result = build(&[2], &[eq()], other, 0, false).unwrap();
    assert_eq!(result.compare_filters.unwrap().op_types, ["gt"]);
    assert_eq!(result.ranges[0].width(), 3);
}

/// Go `TestRangeFallbackForAnalyzeLookUpFilters`.
#[test]
fn range_fallback_for_analyze_lookup_filters() {
    let scenarios = [
        (
            vec![2, 4],
            vec![
                f("in", vec![c(1), n(1), n(3)]),
                f("in", vec![c(3), s("aaa"), s("bbb")]),
            ],
            vec![],
            vec![
                (4, 4, 2, 1, 0),
                (3, 4, 2, 1, 0),
                (2, 2, 1, 1, 0),
                (0, 0, 0, 0, 0),
            ],
        ),
        (
            vec![1],
            vec![f("in", vec![c(2), n(1), n(3), n(5)])],
            compare(),
            vec![(3, 3, 3, 0, 2), (2, 3, 1, 0, 0), (1, 1, 0, 1, 0)],
        ),
        (
            vec![2],
            vec![
                f("in", vec![c(1), n(1), n(3)]),
                f("gt", vec![c(3), s("aaa")]),
                f("lt", vec![c(3), s("bbb")]),
            ],
            vec![],
            vec![(3, 2, 3, 2, 0), (2, 2, 1, 2, 0)],
        ),
    ];
    for (keys, pushed, other, outputs) in scenarios {
        let mut quota = 0;
        for (step, (width, count, access, residual, comparisons)) in outputs.into_iter().enumerate()
        {
            let fallback = std::cell::Cell::new(false);
            let result =
                build_with_fallback(&keys, &pushed, other.clone(), quota, false, &|limit| {
                    assert_eq!(limit, quota);
                    fallback.set(true);
                });
            assert_eq!(fallback.get(), step > 0);
            if count == 0 {
                assert!(result.is_none());
                break;
            }
            let result = result.unwrap();
            assert_eq!(
                (
                    result.used_columns(),
                    result.ranges.len(),
                    result.accesses.len(),
                    result.remained.len(),
                    result
                        .compare_filters
                        .as_ref()
                        .map_or(0, |m| m.op_args.len())
                ),
                (width, count, access, residual, comparisons)
            );
            quota = ranges_mem_usage(&result.ranges) - 1;
        }
    }
    let result = build_with_fallback(
        &[1, 3],
        &[
            f("in", vec![c(2), n(1), n(3)]),
            f("in", vec![c(4), n(2), n(4)]),
        ],
        vec![],
        1,
        true,
        &|_| panic!("Go rebuild mode must not trigger range fallback"),
    );
    assert_eq!(shown(&result),"[[NULL 1 NULL 2,NULL 1 NULL 2] [NULL 1 NULL 4,NULL 1 NULL 4] [NULL 3 NULL 2,NULL 3 NULL 2] [NULL 3 NULL 4,NULL 3 NULL 4]]");
}
