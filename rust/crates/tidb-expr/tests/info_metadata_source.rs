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

//! Source-contract cases from `pkg/expression/builtin_info.go` and
//! `tests/integrationtest/r/expression/charset_and_collation.result`.

use tidb_datatype::{Collation, Datum, FieldType, FieldTypeCode};
use tidb_expr::{
    rewriter::{rewrite_expr, rewrite_expr_resolved, ColumnResolver},
    NoColumns,
};

fn parse_expr(sql_expr: &str) -> tidb_ast::Expr {
    let statement = tidb_parser::parse(&format!("SELECT {sql_expr}")).expect("parses");
    let tidb_ast::Stmt::Query(query) = statement else {
        panic!("expected a query")
    };
    let tidb_ast::QueryStmt::Select(select) = &*query else {
        panic!("expected a SELECT")
    };
    let tidb_ast::SelectField::Expr { expr, .. } = &select.fields.fields()[0] else {
        panic!("expected an expression field")
    };
    expr.clone()
}

fn rewrite(sql_expr: &str) -> tidb_expr::expression::Expression {
    rewrite_expr(&parse_expr(sql_expr)).expect("rewrites")
}

fn eval(sql_expr: &str) -> Datum {
    let expression = rewrite(sql_expr);
    let mut chunk = tidb_chunk::chunk::Chunk::new_empty(&[]);
    chunk.set_num_virtual_rows(1);
    expression
        .eval(&NoColumns, chunk.get_row(0))
        .expect("evaluates")
}

fn text(value: Datum) -> String {
    value.sql_string().expect("metadata functions return text")
}

#[test]
fn charset_and_collation_read_the_arguments_static_type() {
    assert_eq!(rewrite("charset(null)").static_type().unwrap().flen(), 64);
    assert_eq!(rewrite("collation(null)").static_type().unwrap().flen(), 64);
    assert_eq!(text(eval("charset(null)")), "binary");
    assert_eq!(text(eval("collation(null)")), "binary");
    assert_eq!(text(eval("charset(2)")), "binary");
    assert_eq!(text(eval("collation(2)")), "binary");
    assert_eq!(text(eval("charset('a')")), "utf8mb4");
    assert_eq!(text(eval("collation('a')")), "utf8mb4_bin");
    assert_eq!(text(eval("charset(_latin1'a')")), "latin1");
    assert_eq!(text(eval("collation(_latin1'a')")), "latin1_bin");
    assert_eq!(text(eval("charset(N'a')")), "utf8");
    assert_eq!(text(eval("collation(N'a')")), "utf8_bin");
    let introduced = rewrite("_latin1'a'");
    let introduced_type = introduced.static_type().expect("introduced string type");
    assert!(introduced_type.has_flag(tidb_datatype::FieldTypeFlags::UNDERSCORE_CHARSET));
    assert_eq!(introduced_type.flen(), 1);
    assert!(rewrite("_utf8mb4'a'")
        .static_type()
        .expect("default introduced string type")
        .has_flag(tidb_datatype::FieldTypeFlags::UNDERSCORE_CHARSET));
    assert_eq!(
        tidb_expr::eval(&parse_expr("_latin1'a'")).expect("AST value evaluates"),
        Datum::new_collation_string(b"a".to_vec(), Collation::Latin1Bin)
    );
    assert_eq!(
        text(eval("collation('a' collate utf8mb4_general_ci)")),
        "utf8mb4_general_ci"
    );
    assert_eq!(
        text(tidb_expr::eval(&parse_expr("charset('a')")).expect("AST evaluates")),
        "utf8mb4"
    );
}

#[test]
fn coercibility_reads_expression_metadata_without_evaluating_to_null() {
    assert_eq!(eval("coercibility(1)"), Datum::Int(5));
    assert_eq!(eval("coercibility(null)"), Datum::Int(6));
    assert_eq!(eval("coercibility('abc')"), Datum::Int(4));
    assert_eq!(eval("coercibility(version())"), Datum::Int(3));
    assert_eq!(eval("coercibility(concat(null, 'abcde'))"), Datum::Int(4));
    assert_eq!(
        eval("coercibility('a' collate utf8mb4_general_ci)"),
        Datum::Int(0)
    );
    assert_eq!(
        tidb_expr::eval(&parse_expr("coercibility(null)")).expect("AST evaluates"),
        Datum::Int(6)
    );
}

#[test]
fn column_metadata_survives_the_table_backed_rewrite_path() {
    #[derive(Clone)]
    struct Resolver(FieldType);

    impl ColumnResolver for Resolver {
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            (path.last().is_some_and(|name| name == "c")).then(|| (0, self.0.clone(), 1))
        }

        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            tidb_datatype::SessionTimeZone::utc()
        }
    }

    let field_type =
        FieldType::new(FieldTypeCode::Varchar).with_collation(Collation::Utf8Mb4GeneralCi);
    let resolver = Resolver(field_type.clone());
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(&[field_type], 1);
    chunk.append_string(0, "ignored row value");

    let evaluate = |sql_expr: &str| {
        rewrite_expr_resolved(&parse_expr(sql_expr), &resolver)
            .expect("rewrites with column metadata")
            .eval(&NoColumns, chunk.get_row(0))
            .expect("evaluates")
    };

    assert_eq!(text(evaluate("charset(c)")), "utf8mb4");
    assert_eq!(text(evaluate("collation(c)")), "utf8mb4_general_ci");
    assert_eq!(evaluate("coercibility(c)"), Datum::Int(2));
}
