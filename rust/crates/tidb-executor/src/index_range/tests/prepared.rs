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

use super::*;

#[test]
fn prepared_range_endpoints_use_current_typed_parameters() {
    let statement = tidb_parser::parse("SELECT * FROM t WHERE a >= ? + 1 AND a <= ?").unwrap();
    let original = statement.restore();
    let tidb_ast::Stmt::Query(query) = &statement else {
        panic!("query")
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("select")
    };
    let predicate = select.where_clause.as_ref().unwrap();
    let index = columns(&["a"]);
    let fields = vec![("a".to_owned(), index[0].field_type.clone())];
    for (parameters, expected) in [
        (vec![Datum::Int(2), Datum::Int(5)], "[3,5]"),
        (vec![Datum::Int(6), Datum::Int(9)], "[7,9]"),
        (vec![Datum::Int(7), Datum::Int(2)], ""),
        (vec![Datum::Null, Datum::Int(5)], ""),
    ] {
        let ctx = crate::StmtContext::for_query().with_prepared_params(parameters.into());
        let resolver = crate::driver::TableResolver {
            table_name: "t",
            columns: &fields,
            zone: ctx.session_zone(),
            no_unsigned_subtraction: false,
            div_precision_increment: 4,
            constant_context: ctx,
        };
        let ranges = detach_cond_and_build_range_for_index(&index, predicate, &resolver)
            .expect("prepared conditions must produce current access ranges");
        assert_eq!(render(&ranges.ranges), expected);
        assert!(ranges.residual.is_empty());
    }
    assert_eq!(statement.restore(), original);
}

#[test]
fn prepared_range_shapes_preserve_current_values_and_residuals() {
    let ints = |values: &[i64]| values.iter().copied().map(Datum::Int).collect::<Vec<_>>();
    for (names, predicate, parameters, expected, residuals, native_expected, native_residuals) in [
        (
            vec!["a"],
            "a IN (?,?,?)",
            ints(&[5, 2, 2]),
            "[2,2], [5,5]",
            0,
            "[2,2], [5,5]",
            0,
        ),
        (
            vec!["a"],
            "a = ? OR a = ?",
            ints(&[2, 5]),
            "[2,2], [5,5]",
            0,
            "[2,2], [5,5]",
            0,
        ),
        (
            vec!["a", "b"],
            "(a,b) IN ((?,?),(?,?))",
            ints(&[1, 4, 2, 5]),
            "[1 4,1 4], [2 5,2 5]",
            0,
            "[1 4,1 4], [2 5,2 5]",
            0,
        ),
        (
            vec!["a"],
            "a >= ? AND b = ?",
            ints(&[2, 9]),
            "[2,+inf]",
            1,
            "[2,+inf]",
            1,
        ),
        // The AST-only helper can prove this is empty. Comparison rewriting
        // casts both sides to DOUBLE, so the native ranger keeps a full range
        // and the rewritten predicate as a residual filter.
        (
            vec!["a"],
            "a = ?",
            vec![Datum::Null],
            "",
            0,
            "[NULL,+inf]",
            1,
        ),
        (
            vec!["s"],
            "s = ?",
            vec![Datum::Bytes(b"Ab".to_vec())],
            "[\"Ab\",\"Ab\"]",
            0,
            "[\"Ab\",\"Ab\"]",
            0,
        ),
        (
            vec!["s"],
            "s LIKE ?",
            vec![Datum::new_collation_string(
                b"ab%".to_vec(),
                Collation::DEFAULT,
            )],
            "[\"ab\",\"ac\")",
            1,
            "[\"ab\",\"ac\")",
            1,
        ),
    ] {
        let statement = tidb_parser::parse(&format!("SELECT * FROM t WHERE {predicate}")).unwrap();
        let tidb_ast::Stmt::Query(query) = &statement else {
            panic!("query")
        };
        let tidb_ast::QueryStmt::Select(select) = &**query else {
            panic!("select")
        };
        let predicate = select.where_clause.as_ref().unwrap();
        let index = columns(&names);
        let fields = columns(&["a", "b", "s"])
            .into_iter()
            .map(|column| (column.name, column.field_type))
            .collect::<Vec<_>>();
        let ctx = crate::StmtContext::for_query().with_prepared_params(parameters.into());
        let resolver = crate::driver::TableResolver {
            table_name: "t",
            columns: &fields,
            zone: ctx.session_zone(),
            no_unsigned_subtraction: false,
            div_precision_increment: 4,
            constant_context: ctx,
        };
        let ranges = detach_cond_and_build_range_for_index(&index, predicate, &resolver)
            .expect("prepared range shape");
        assert_eq!(render(&ranges.ranges), expected, "{predicate:?}");
        assert_eq!(ranges.residual.len(), residuals, "{predicate:?}");
        let expression = rewrite_expr_resolved(predicate, &resolver).unwrap();
        let conditions = tidb_expr::expr_util::split_cnf_items(&expression);
        let native_columns: Vec<_> = index
            .iter()
            .map(|column| resolver.resolve_column(&[column.name.clone()]).unwrap())
            .collect();
        let lengths: Vec<_> = index.iter().map(|column| column.prefix_len).collect();
        let native = tidb_planner::ranger::detacher::detach_cond_and_build_range_for_index_in(
            &conditions,
            &native_columns,
            &lengths,
            0,
            &|expression| resolver.eval_constant(expression),
        )
        .unwrap();
        let native_ranges: Vec<_> = native
            .ranges
            .iter()
            .map(|range| IndexRange {
                low: range.low_val.clone(),
                high: range.high_val.clone(),
                low_exclusive: range.low_exclude,
                high_exclusive: range.high_exclude,
            })
            .collect();
        assert_eq!(
            render(&native_ranges),
            native_expected,
            "compiled {predicate:?}"
        );
        assert_eq!(
            native.remained_conds.len(),
            native_residuals,
            "compiled {predicate:?}"
        );
    }
}

#[test]
fn range_constant_evaluation_preserves_context_and_rejects_row_values() {
    let fields = vec![(
        "a".to_owned(),
        FieldType::new(tidb_datatype::FieldTypeCode::LongLong),
    )];
    for (offset_secs, expected) in [(0, 946684800), (28800, 946656000)] {
        let ctx =
            crate::StmtContext::for_query().with_time_zone(tidb_datatype::SessionTimeZone::Fixed {
                name: String::new(),
                offset_secs,
            });
        let resolver = crate::driver::TableResolver {
            table_name: "t",
            columns: &fields,
            zone: ctx.session_zone(),
            no_unsigned_subtraction: false,
            div_precision_increment: 4,
            constant_context: ctx,
        };
        for (text, expected) in [
            (
                "UNIX_TIMESTAMP('2000-01-01 00:00:00')",
                Some(Datum::Int(expected)),
            ),
            ("a + 1", None),
            ("RAND()", None),
            ("?", None),
        ] {
            let statement = tidb_parser::parse(&format!("SELECT 1 WHERE {text}")).unwrap();
            let tidb_ast::Stmt::Query(query) = &statement else {
                panic!("query")
            };
            let tidb_ast::QueryStmt::Select(select) = &**query else {
                panic!("select")
            };
            assert_eq!(
                constant_value(select.where_clause.as_ref().unwrap(), &resolver),
                expected
            );
        }
    }
}

#[test]
fn statistics_ranges_use_current_prepared_parameters() {
    let statement = tidb_parser::parse("SELECT * FROM t WHERE a IN (?,?)").unwrap();
    let original = statement.restore();
    let tidb_ast::Stmt::Query(query) = &statement else {
        panic!("query");
    };
    let tidb_ast::QueryStmt::Select(select) = &**query else {
        panic!("select");
    };
    let predicate = select.where_clause.as_ref().unwrap();
    let index = columns(&["a"]);
    let fields = vec![("a".to_owned(), index[0].field_type.clone())];
    for (parameters, expected) in [
        (vec![Datum::Int(2), Datum::Int(5)], "[2,2], [5,5]"),
        (vec![Datum::Int(7), Datum::Int(9)], "[7,7], [9,9]"),
    ] {
        let ctx = crate::StmtContext::for_query().with_prepared_params(parameters.into());
        let resolver = crate::driver::TableResolver {
            table_name: "t",
            columns: &fields,
            zone: ctx.session_zone(),
            no_unsigned_subtraction: false,
            div_precision_increment: 4,
            constant_context: ctx,
        };
        let (built, _, _) = detach_conjuncts_and_build_range_for_index_with_context(
            &index,
            &[predicate],
            &resolver,
            RangeContext::default(),
            &index,
        )
        .unwrap()
        .expect("current parameters must produce statistics ranges");
        assert_eq!(render(&built.ranges), expected);
        assert!(built.residual.is_empty());
        let (column_built, fallback) = detach_conds_for_column_with_context(
            &index[0],
            &[predicate],
            &resolver,
            RangeContext::default(),
        )
        .expect("bound column statistics parameters must build");
        assert!(!fallback);
        assert_eq!(render(&column_built.ranges), expected);
    }
    assert_eq!(statement.restore(), original);
}

#[test]
fn column_statistics_ranges_preserve_conversion_errors() {
    let statement = tidb_parser::parse("SELECT * FROM t WHERE a = ?").unwrap();
    let tidb_ast::Stmt::Query(query) = statement else {
        panic!("query");
    };
    let tidb_ast::QueryStmt::Select(select) = &*query else {
        panic!("select");
    };
    let predicate = select.where_clause.as_ref().unwrap();
    let column = columns(&["a"]).remove(0);
    let fields = vec![(column.name.clone(), column.field_type.clone())];
    let ctx = crate::StmtContext::for_query()
        .with_prepared_params(vec![Datum::new_raw(b"unsupported")].into());
    let resolver = crate::driver::TableResolver {
        table_name: "t",
        columns: &fields,
        zone: ctx.session_zone(),
        no_unsigned_subtraction: false,
        div_precision_increment: 4,
        constant_context: ctx,
    };

    let error = detach_conds_for_column_with_context(
        &column,
        &[predicate],
        &resolver,
        RangeContext::default(),
    )
    .expect_err("Go BuildColumnRange returns an error for this unsupported value");
    assert!(matches!(
        error,
        tidb_planner::ranger::points::PointBuilderError::Value(_)
    ));
}

#[test]
fn column_statistics_like_ranges_use_go_collation_keys() {
    let statement = tidb_parser::parse("SELECT * FROM t WHERE s LIKE 'ab%'").unwrap();
    let tidb_ast::Stmt::Query(query) = statement else {
        panic!("query");
    };
    let tidb_ast::QueryStmt::Select(select) = &*query else {
        panic!("select");
    };
    let predicate = select.where_clause.as_ref().unwrap();
    let field_type = FieldType::new(tidb_datatype::FieldTypeCode::VarString)
        .with_collation(Collation::Utf8Mb4GeneralCi);
    let column = RangeColumn::whole("s".to_owned(), field_type.clone());
    let fields = vec![(column.name.clone(), field_type)];
    let ctx = crate::StmtContext::for_query();
    let resolver = crate::driver::TableResolver {
        table_name: "t",
        columns: &fields,
        zone: ctx.session_zone(),
        no_unsigned_subtraction: false,
        div_precision_increment: 4,
        constant_context: ctx,
    };

    let (built, _) = detach_conds_for_column_with_context(
        &column,
        &[predicate],
        &resolver,
        RangeContext::default(),
    )
    .expect("Go BuildColumnRange builds a prefix LIKE range in collation-key space");
    assert_eq!(built.access_count, 1);
    assert_eq!(
        built.ranges,
        vec![IndexRange {
            low: vec![Datum::Bytes(vec![0, b'A', 0, b'B'])],
            high: vec![Datum::Bytes(vec![0, b'A', 0, b'C'])],
            low_exclusive: false,
            high_exclusive: true,
        }]
    );
}

#[test]
fn column_statistics_range_preserves_go_in_evaluation_error_policy() {
    let column = columns(&["a"]).remove(0);
    let fields = vec![(column.name.clone(), column.field_type.clone())];
    for (condition, expect_error) in [
        ("a IN (?)", true),
        ("a IN ((?))", true),
        ("a = ?", false),
        ("a IN (?) OR a = 1", true),
        ("a IN (?) OR b = 1", false),
        ("a IN (?) OR a IN (b + 1)", false),
        ("a IN (?) AND b = 1", true),
        ("a IN (b + 1)", false),
    ] {
        let statement = tidb_parser::parse(&format!("SELECT * FROM t WHERE {condition}")).unwrap();
        let tidb_ast::Stmt::Query(query) = statement else {
            panic!("query");
        };
        let tidb_ast::QueryStmt::Select(select) = &*query else {
            panic!("select");
        };
        let predicate = select.where_clause.as_ref().unwrap();
        let ctx = crate::StmtContext::for_query();
        let resolver = crate::driver::TableResolver {
            table_name: "t",
            columns: &fields,
            zone: ctx.session_zone(),
            no_unsigned_subtraction: false,
            div_precision_increment: 4,
            constant_context: ctx,
        };
        let result = detach_conds_for_column_with_context(
            &column,
            &[predicate],
            &resolver,
            RangeContext::default(),
        );
        if expect_error {
            assert!(matches!(
                result,
                Err(tidb_planner::ranger::points::PointBuilderError::Unsupported(_))
            ));
        } else {
            let (built, _) = result.expect("Go comparison bounds decline range evaluation errors");
            assert_eq!(built.access_count, 0);
        }
    }
}
