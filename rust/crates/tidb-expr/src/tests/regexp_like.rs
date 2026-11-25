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

//! Rewritten-expression coverage for `REGEXP_LIKE`.

use super::{chunk_e, e};

/// Scalar rows from `pkg/expression/builtin_regexp_test.go:210`
/// (`TestRegexpLike`), folded at build time and evaluated per row.
#[test]
fn regexp_like_source_vectors_reach_the_rewritten_evaluator() {
    for (expr, want) in [
        ("regexp_like('a', 'a')", "INT:1"),
        ("regexp_like('b', 'a')", "INT:0"),
        ("regexp_like('abc', 'AbC', 'i')", "INT:1"),
        ("regexp_like('good\\nday', '^day', 'm')", "INT:1"),
        ("regexp_like(NULL, 'a')", "NULL"),
        ("regexp_like('a', NULL)", "NULL"),
        ("regexp_like('a', 'a', NULL)", "NULL"),
    ] {
        assert_eq!(e(expr), want, "AST: {expr}");
        assert_eq!(chunk_e(expr), want, "rewritten: {expr}");
    }

    assert!(chunk_e("regexp_like('abc', 'abc', 'p')").contains("Invalid match type"));
}

#[test]
fn regexp_like_with_column_patterns_matches_each_row() {
    use crate::{
        constant::Constant,
        expression::{Column, Expression},
        Datum, NoColumns,
    };
    use tidb_chunk::chunk::Chunk;
    use tidb_datatype::{FieldType, FieldTypeCode};

    let string_type = FieldType::new(FieldTypeCode::VarString);
    let column = |index| {
        let mut column = Column::new(index, string_type.clone());
        column.index = index;
        Expression::Column(column)
    };
    let function = crate::new_function::new_function_base(
        &NoColumns,
        "regexp_like",
        FieldType::new(FieldTypeCode::LongLong),
        vec![column(0), column(1)],
    )
    .unwrap();
    let constant = crate::new_function::new_function_base(
        &NoColumns,
        "regexp_like",
        FieldType::new(FieldTypeCode::LongLong),
        vec![
            column(0),
            Expression::Constant(Constant::new(
                Datum::new_string_datum(r"\p{Tai_Yo}+"),
                string_type.clone(),
            )),
        ],
    )
    .unwrap();
    let rows = [
        ("prefixſKtail", "(?i)sk", 1),
        ("prefixſKtail", "(?-i)sk", 0),
        ("abc", r"\W+", 0),
        ("abc-1", r"\W+", 1),
        ("猫Σ", r"\p{Greek}+", 1),
        ("\u{1e6c0}", r"\p{Tai_Yo}+", 1),
    ];
    let mut chunk = Chunk::new_chunk_with_capacity(
        &[string_type.clone(), string_type],
        rows.len(),
    );
    for (text, pattern, _) in rows {
        chunk.append_string(0, text);
        chunk.append_string(1, pattern);
    }
    for (index, (text, _, expected)) in rows.into_iter().enumerate() {
        assert_eq!(
            function.eval(&NoColumns, chunk.get_row(index)).unwrap(),
            Datum::Int(expected)
        );
        assert_eq!(
            constant.eval(&NoColumns, chunk.get_row(index)).unwrap(),
            Datum::Int(i64::from(text == "\u{1e6c0}"))
        );
    }
}

/// Positional regexp functions from `pkg/expression/builtin_regexp_test.go`.
/// Their scalar implementations already share the Go algorithms; this test
/// proves the rewritten SQL path reaches them with the source argument casts.
#[test]
fn positional_regexp_source_vectors_reach_the_rewritten_evaluator() {
    for (expr, want) in [
        ("regexp_substr('abc abd abe', 'ab.', 1, 2)", "STR:abd"),
        ("regexp_substr('你好啊', '好', 2)", "STR:好"),
        ("regexp_instr('abc abd abe', 'ab.', 3, 2)", "INT:9"),
        ("regexp_instr('你好啊', '好', 2)", "INT:2"),
        ("regexp_replace('abc abd', 'ab.', 'X')", "STR:X X"),
        ("regexp_replace('abc abd', 'ab.', 'X', 1, 2)", "STR:abc X"),
    ] {
        assert_eq!(e(expr), want, "AST: {expr}");
        assert_eq!(chunk_e(expr), want, "rewritten: {expr}");
    }
}
