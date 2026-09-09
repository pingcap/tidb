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
    for (names, predicate, parameters, expected, residuals) in [
        (
            vec!["a"],
            "a IN (?,?,?)",
            ints(&[5, 2, 2]),
            "[2,2], [5,5]",
            0,
        ),
        (
            vec!["a"],
            "a = ? OR a = ?",
            ints(&[2, 5]),
            "[2,2], [5,5]",
            0,
        ),
        (
            vec!["a", "b"],
            "(a,b) IN ((?,?),(?,?))",
            ints(&[1, 4, 2, 5]),
            "[1 4,1 4], [2 5,2 5]",
            0,
        ),
        (vec!["a"], "a >= ? AND b = ?", ints(&[2, 9]), "[2,+inf]", 1),
        (vec!["a"], "a = ?", vec![Datum::Null], "", 0),
        (
            vec!["s"],
            "s = ?",
            vec![Datum::Bytes(b"Ab".to_vec())],
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
            &|constant| resolver.eval_constant(&Expression::Constant(constant.clone())),
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
        assert_eq!(render(&native_ranges), expected, "compiled {predicate:?}");
        assert_eq!(
            native.remained_conds.len(),
            residuals,
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
