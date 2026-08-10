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

//! Row-for-row translations of `pkg/expression/collation_test.go`'s
//! `TestDeriveCollation` and `TestInferCollation` tables, including the
//! helper constructors they build their arguments with.

use tidb_datatype::{Datum, EvalType, FieldType, FieldTypeCode};

use super::{derive_collation, infer_collation};
use crate::{
    column::Column,
    constant::Constant,
    expr_collation::{Coercibility, ExprCollation, Repertoire},
    expression::Expression,
};

const UTF8MB4: &str = "utf8mb4";
const UTF8MB4_BIN: &str = "utf8mb4_bin";
const UTF8MB4_GENERAL_CI: &str = "utf8mb4_general_ci";
const UTF8MB4_UNICODE_CI: &str = "utf8mb4_unicode_ci";
const GBK: &str = "gbk";
const GBK_BIN: &str = "gbk_bin";
const BIN: &str = "binary";
const ASCII_CHARSET: &str = "ascii";
const ASCII_BIN: &str = "ascii_bin";
const LATIN1: &str = "latin1";
const LATIN1_BIN: &str = "latin1_bin";

fn string_field_type(charset: &str, collation: &str) -> FieldType {
    FieldType::parser(FieldTypeCode::String)
        .with_charset_name(charset)
        .with_collation_name(collation)
}

fn int_field_type() -> FieldType {
    FieldType::parser(FieldTypeCode::Long)
        .with_charset_name(BIN)
        .with_collation_name(BIN)
}

/// Go `newConstString`.
fn new_const_string(
    value: &str,
    coercibility: Coercibility,
    charset: &str,
    collation: &str,
) -> Expression {
    let mut constant = Constant::new(
        Datum::new_string(value),
        string_field_type(charset, collation),
    );
    constant.collation.set_coercibility(coercibility);
    // Go derives the literal's repertoire from its bytes: any byte >= 0x80
    // makes it UNICODE.
    constant
        .collation
        .set_repertoire(if value.bytes().any(|byte| byte >= 0x80) {
            Repertoire::UNICODE
        } else {
            Repertoire::ASCII
        });
    Expression::Constant(constant)
}

/// Go `newExpression`: a constant with an explicitly chosen repertoire.
fn new_expression(
    value: &str,
    coercibility: Coercibility,
    repertoire: Repertoire,
    charset: &str,
    collation: &str,
) -> Expression {
    let mut constant = Constant::new(
        Datum::new_string(value),
        string_field_type(charset, collation),
    );
    constant.collation.set_coercibility(coercibility);
    constant.collation.set_repertoire(repertoire);
    Expression::Constant(constant)
}

/// Go `newColString`.
fn new_col_string(charset: &str, collation: &str) -> Expression {
    let mut column = Column::new(0, string_field_type(charset, collation));
    column.collation.set_coercibility(Coercibility::IMPLICIT);
    column
        .collation
        .set_repertoire(if charset == ASCII_CHARSET {
            Repertoire::ASCII
        } else {
            Repertoire::UNICODE
        });
    Expression::Column(column)
}

/// Go `newColJSON`: a JSON column, with the zero coercibility state.
fn new_col_json() -> Expression {
    Expression::Column(Column::new(
        0,
        FieldType::parser(FieldTypeCode::Json)
            .with_charset_name(BIN)
            .with_collation_name(BIN),
    ))
}

/// Go `newConstInt`.
fn new_const_int(coercibility: Coercibility) -> Expression {
    let mut constant = Constant::new(Datum::Int(1), int_field_type());
    constant.collation.set_coercibility(coercibility);
    constant.collation.set_repertoire(Repertoire::ASCII);
    Expression::Constant(constant)
}

/// Go `newColInt`.
fn new_col_int(coercibility: Coercibility) -> Expression {
    let mut column = Column::new(0, int_field_type());
    column.collation.set_coercibility(coercibility);
    column.collation.set_repertoire(Repertoire::ASCII);
    Expression::Column(column)
}

fn collation(
    coer: Coercibility,
    repe: Repertoire,
    charset: &str,
    collation: &str,
) -> ExprCollation {
    ExprCollation {
        coer,
        repe,
        charset: charset.to_owned(),
        collation: collation.to_owned(),
    }
}

/// One row of Go's table: the function names it applies to, the arguments,
/// the declared result eval type, and the expected `ExprCollation` (`None`
/// where Go expects an error).
struct Row {
    functions: &'static [&'static str],
    args: Vec<Expression>,
    ret_type: EvalType,
    expected: Option<ExprCollation>,
}

struct InferRow {
    exprs: Vec<Expression>,
    expected: Option<ExprCollation>,
}

/// The whole `TestDeriveCollation` table.
///
/// Function names are this crate's rewriter spellings, which map 1:1 onto
/// Go's `ast.*` constants (`ast.InsertFunc` is `insert_func`, `ast.SHA` is
/// `sha1`, and so on).
fn go_table() -> Vec<Row> {
    let explicit = Coercibility::EXPLICIT;
    let implicit = Coercibility::IMPLICIT;
    let coercible = Coercibility::COERCIBLE;
    let numeric = Coercibility::NUMERIC;
    let ascii = Repertoire::ASCII;
    let unicode = Repertoire::UNICODE;

    vec![
        Row {
            functions: &["left", "right", "repeat", "substr", "substring", "mid"],
            args: vec![
                new_const_string("a", coercible, UTF8MB4, UTF8MB4_BIN),
                new_const_int(explicit),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(coercible, ascii, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["trim", "ltrim", "rtrim"],
            args: vec![new_const_string("a", coercible, UTF8MB4, UTF8MB4_BIN)],
            ret_type: EvalType::String,
            expected: Some(collation(coercible, ascii, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["substring_index"],
            args: vec![
                new_const_string("a", coercible, UTF8MB4, UTF8MB4_BIN),
                new_const_string("啊", explicit, GBK, GBK_BIN),
                new_const_int(explicit),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(coercible, ascii, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["replace", "translate"],
            args: vec![
                new_const_string("a", explicit, UTF8MB4, UTF8MB4_BIN),
                new_const_string("啊", explicit, GBK, GBK_BIN),
                new_const_string("ㅂ", explicit, BIN, BIN),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(explicit, ascii, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["insert_func"],
            args: vec![
                new_const_string("a", explicit, UTF8MB4, UTF8MB4_BIN),
                new_const_int(explicit),
                new_const_int(explicit),
                new_const_string("ㅂ", explicit, BIN, BIN),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(explicit, unicode, BIN, BIN)),
        },
        Row {
            functions: &["insert_func"],
            args: vec![
                new_const_string("a", implicit, UTF8MB4, UTF8MB4_BIN),
                new_const_int(explicit),
                new_const_int(explicit),
                new_const_string("啊", implicit, GBK, GBK_BIN),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(implicit, unicode, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["insert_func"],
            args: vec![
                new_const_string("ㅂ", implicit, UTF8MB4, UTF8MB4_BIN),
                new_const_int(explicit),
                new_const_int(explicit),
                new_const_string("啊", explicit, GBK, GBK_BIN),
            ],
            ret_type: EvalType::String,
            expected: None,
        },
        Row {
            functions: &["lpad", "rpad"],
            args: vec![
                new_const_string("ㅂ", implicit, UTF8MB4, UTF8MB4_BIN),
                new_const_int(explicit),
                new_const_string("啊", explicit, GBK, GBK_BIN),
            ],
            ret_type: EvalType::String,
            expected: None,
        },
        Row {
            functions: &["lpad", "rpad"],
            args: vec![
                new_const_string("ㅂ", implicit, UTF8MB4, UTF8MB4_BIN),
                new_const_int(explicit),
                new_const_string("啊", implicit, GBK, GBK_BIN),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(implicit, unicode, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["find_in_set", "regexp"],
            args: vec![
                new_col_string(UTF8MB4, UTF8MB4_GENERAL_CI),
                new_col_string(UTF8MB4, UTF8MB4_UNICODE_CI),
            ],
            ret_type: EvalType::Int,
            expected: None,
        },
        Row {
            functions: &["field"],
            args: vec![
                new_col_string(UTF8MB4, UTF8MB4_BIN),
                new_col_string(UTF8MB4, UTF8MB4_UNICODE_CI),
            ],
            ret_type: EvalType::Int,
            expected: Some(collation(implicit, unicode, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["field"],
            args: vec![new_col_int(implicit), new_col_int(implicit)],
            ret_type: EvalType::Int,
            expected: Some(collation(numeric, ascii, BIN, BIN)),
        },
        Row {
            functions: &["locate", "instr", "position"],
            args: vec![new_col_int(numeric), new_col_int(numeric)],
            ret_type: EvalType::Int,
            expected: Some(collation(numeric, ascii, BIN, BIN)),
        },
        Row {
            functions: &["format", "sha2"],
            args: vec![new_col_int(numeric), new_col_int(numeric)],
            ret_type: EvalType::String,
            expected: Some(collation(coercible, ascii, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["space", "to_base64", "uuid", "hex", "md5", "sha1"],
            args: vec![new_col_int(numeric)],
            ret_type: EvalType::String,
            expected: Some(collation(coercible, ascii, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["ge", "le", "gt", "lt", "eq", "ne", "nulleq", "strcmp"],
            args: vec![
                new_col_string(ASCII_CHARSET, ASCII_BIN),
                new_col_string(GBK, GBK_BIN),
            ],
            ret_type: EvalType::Int,
            expected: Some(collation(numeric, ascii, GBK, GBK_BIN)),
        },
        Row {
            functions: &["ge", "le", "gt", "lt", "eq", "ne", "nulleq", "strcmp"],
            args: vec![
                new_col_string(LATIN1, LATIN1_BIN),
                new_col_string(GBK, GBK_BIN),
            ],
            ret_type: EvalType::Int,
            expected: None,
        },
        Row {
            functions: &["bin", "from_base64", "oct", "unhex", "weight_string"],
            args: vec![new_col_string(LATIN1, LATIN1_BIN)],
            ret_type: EvalType::String,
            expected: Some(collation(coercible, unicode, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &[
                "ascii",
                "bit_length",
                "char_length",
                "character_length",
                "length",
                "octet_length",
                "ord",
            ],
            args: vec![new_col_string(LATIN1, LATIN1_BIN)],
            ret_type: EvalType::Int,
            expected: Some(collation(numeric, ascii, BIN, BIN)),
        },
        Row {
            functions: &["export_set", "elt", "make_set"],
            args: vec![
                new_col_int(explicit),
                new_col_string(UTF8MB4, UTF8MB4_BIN),
                new_col_string(UTF8MB4, UTF8MB4_UNICODE_CI),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(implicit, unicode, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["export_set", "elt", "make_set"],
            args: vec![
                new_col_int(explicit),
                new_col_json(),
                new_col_string(UTF8MB4, UTF8MB4_UNICODE_CI),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(implicit, unicode, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["concat", "concat_ws", "coalesce", "greatest", "least"],
            args: vec![new_col_string(GBK, GBK_BIN), new_col_json()],
            ret_type: EvalType::String,
            expected: Some(collation(implicit, unicode, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["concat", "concat_ws", "coalesce", "greatest", "least"],
            args: vec![new_col_json(), new_col_string(BIN, BIN)],
            ret_type: EvalType::String,
            expected: Some(collation(implicit, unicode, BIN, BIN)),
        },
        Row {
            functions: &["concat", "concat_ws", "coalesce", "in", "greatest", "least"],
            args: vec![
                new_const_string("a", coercible, UTF8MB4, UTF8MB4_BIN),
                new_col_string(UTF8MB4, UTF8MB4_BIN),
                new_col_string(UTF8MB4, UTF8MB4_UNICODE_CI),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(implicit, unicode, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["lower", "lcase", "reverse", "upper", "ucase", "quote"],
            args: vec![new_const_string("a", coercible, UTF8MB4, UTF8MB4_BIN)],
            ret_type: EvalType::String,
            expected: Some(collation(coercible, ascii, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["lower", "lcase", "reverse", "upper", "ucase", "quote"],
            args: vec![new_col_json()],
            ret_type: EvalType::String,
            expected: Some(collation(implicit, unicode, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["if"],
            args: vec![
                new_col_int(explicit),
                new_col_string(UTF8MB4, UTF8MB4_BIN),
                new_col_string(UTF8MB4, UTF8MB4_UNICODE_CI),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(implicit, unicode, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["ifnull"],
            args: vec![
                new_col_string(UTF8MB4, UTF8MB4_BIN),
                new_col_string(UTF8MB4, UTF8MB4_UNICODE_CI),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(implicit, unicode, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["like"],
            args: vec![
                new_col_string(UTF8MB4, UTF8MB4_BIN),
                new_const_string("like", explicit, UTF8MB4, UTF8MB4_BIN),
                new_const_string("\\", explicit, UTF8MB4, UTF8MB4_BIN),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(numeric, ascii, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["date_format", "time_format"],
            args: vec![
                new_const_string("2020-02-02", explicit, UTF8MB4, UTF8MB4_GENERAL_CI),
                new_const_string("%Y %M %D", explicit, UTF8MB4, UTF8MB4_UNICODE_CI),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(explicit, ascii, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &["date_format", "time_format"],
            args: vec![
                new_const_string("2020-02-02", explicit, UTF8MB4, UTF8MB4_GENERAL_CI),
                new_const_string("%Y %M %D", coercible, UTF8MB4, UTF8MB4_UNICODE_CI),
            ],
            ret_type: EvalType::String,
            expected: Some(collation(coercible, ascii, UTF8MB4, UTF8MB4_BIN)),
        },
        Row {
            functions: &[
                "database",
                "user",
                "current_user",
                "version",
                "current_role",
                "tidb_version",
                "current_resource_group",
            ],
            args: vec![],
            ret_type: EvalType::String,
            expected: Some(collation(
                Coercibility::SYSCONST,
                unicode,
                UTF8MB4,
                UTF8MB4_BIN,
            )),
        },
        Row {
            functions: &["cast"],
            args: vec![new_col_int(explicit)],
            ret_type: EvalType::String,
            expected: Some(collation(explicit, ascii, UTF8MB4, UTF8MB4_BIN)),
        },
    ]
}

/// The `TestDeriveCollation` rows this crate answers Go's way today.
///
/// `derive_collation` documents `date_format`/`time_format`, `cast` and `case`
/// as deferred; those rows are asserted separately below so the deferral is a
/// named work item rather than a missing row.
#[test]
fn go_test_derive_collation() {
    for (index, row) in go_table().into_iter().enumerate() {
        if is_deferred_row(row.functions) {
            continue;
        }
        assert_row(index, &row);
    }
}

/// Rows whose functions `derive_collation` has not implemented yet. Each one
/// asserts Go's answer, so making the function work turns this green rather
/// than requiring the row to be written from scratch.
#[test]
#[ignore = "derive_collation defers date_format/time_format and cast to their own arms"]
fn go_test_derive_collation_deferred_functions() {
    for (index, row) in go_table().into_iter().enumerate() {
        if is_deferred_row(row.functions) {
            assert_row(index, &row);
        }
    }
}

/// `pkg/expression/collation_test.go::TestInferCollation`.
#[test]
fn go_test_infer_collation() {
    let tests = vec![
        // same charset.
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_general_ci",
                ),
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_unicode_ci",
                ),
            ],
            expected: Some(collation(
                Coercibility::EXPLICIT,
                Repertoire::UNICODE,
                "utf8mb4",
                "utf8mb4_unicode_ci",
            )),
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_unicode_ci",
                ),
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_unicode_ci",
                ),
            ],
            expected: Some(collation(
                Coercibility::EXPLICIT,
                Repertoire::UNICODE,
                "utf8mb4",
                "utf8mb4_unicode_ci",
            )),
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_general_ci",
                ),
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_unicode_ci",
                ),
            ],
            expected: None,
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_general_ci",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_unicode_ci",
                ),
            ],
            expected: Some(collation(
                Coercibility::NONE,
                Repertoire::UNICODE,
                "utf8mb4",
                "utf8mb4_bin",
            )),
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_general_ci",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_bin",
                ),
            ],
            expected: Some(collation(
                Coercibility::IMPLICIT,
                Repertoire::UNICODE,
                "utf8mb4",
                "utf8mb4_bin",
            )),
        },
        // Regression test: utf8mb4_0900_bin is a binary collation and should win
        // over non-bin collations at the same coercibility (same as utf8mb4_bin).
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_0900_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_unicode_ci",
                ),
            ],
            expected: Some(collation(
                Coercibility::IMPLICIT,
                Repertoire::UNICODE,
                "utf8mb4",
                "utf8mb4_0900_bin",
            )),
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_unicode_ci",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_0900_bin",
                ),
            ],
            expected: Some(collation(
                Coercibility::IMPLICIT,
                Repertoire::UNICODE,
                "utf8mb4",
                "utf8mb4_0900_bin",
            )),
        },
        // binary charset with non-binary charset.
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::NUMERIC,
                    Repertoire::UNICODE,
                    "binary",
                    "binary",
                ),
                new_expression(
                    "a",
                    Coercibility::COERCIBLE,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_bin",
                ),
            ],
            expected: Some(collation(
                Coercibility::COERCIBLE,
                Repertoire::UNICODE,
                "utf8mb4",
                "utf8mb4_bin",
            )),
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::COERCIBLE,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::NUMERIC,
                    Repertoire::UNICODE,
                    "binary",
                    "binary",
                ),
            ],
            expected: Some(collation(
                Coercibility::COERCIBLE,
                Repertoire::UNICODE,
                "utf8mb4",
                "utf8mb4_bin",
            )),
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "binary",
                    "binary",
                ),
            ],
            expected: Some(collation(
                Coercibility::EXPLICIT,
                Repertoire::UNICODE,
                "binary",
                "binary",
            )),
        },
        // different charset, one of them is utf8mb4
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_unicode_ci",
                ),
            ],
            expected: Some(collation(
                Coercibility::IMPLICIT,
                Repertoire::UNICODE,
                "utf8mb4",
                "utf8mb4_unicode_ci",
            )),
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_unicode_ci",
                ),
            ],
            expected: Some(collation(
                Coercibility::EXPLICIT,
                Repertoire::UNICODE,
                "utf8mb4",
                "utf8mb4_unicode_ci",
            )),
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_unicode_ci",
                ),
            ],
            expected: None,
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "latin1",
                    "latin1",
                ),
            ],
            expected: None,
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "latin1",
                    "latin1",
                ),
            ],
            expected: None,
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "latin1",
                    "latin1",
                ),
            ],
            expected: None,
        },
        // different charset, one of them is CoercibilityCoercible
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::COERCIBLE,
                    Repertoire::UNICODE,
                    "latin1",
                    "latin1",
                ),
            ],
            expected: Some(collation(
                Coercibility::IMPLICIT,
                Repertoire::UNICODE,
                "gbk",
                "gbk_bin",
            )),
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::COERCIBLE,
                    Repertoire::UNICODE,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "latin1",
                    "latin1",
                ),
            ],
            expected: Some(collation(
                Coercibility::IMPLICIT,
                Repertoire::UNICODE,
                "latin1",
                "latin1",
            )),
        },
        // different charset, one of them is ASCII
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::ASCII,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "latin1",
                    "latin1",
                ),
            ],
            expected: Some(collation(
                Coercibility::IMPLICIT,
                Repertoire::UNICODE,
                "latin1",
                "latin1",
            )),
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::ASCII,
                    "latin1",
                    "latin1",
                ),
            ],
            expected: Some(collation(
                Coercibility::IMPLICIT,
                Repertoire::UNICODE,
                "gbk",
                "gbk_bin",
            )),
        },
        // 3 expressions.
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "latin1",
                    "latin1",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "binary",
                    "binary",
                ),
            ],
            expected: None,
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "latin1",
                    "latin1",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_bin",
                ),
            ],
            expected: None,
        },
        InferRow {
            exprs: vec![
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "gbk",
                    "gbk_bin",
                ),
                new_expression(
                    "a",
                    Coercibility::EXPLICIT,
                    Repertoire::UNICODE,
                    "latin1",
                    "latin1",
                ),
                new_expression(
                    "a",
                    Coercibility::IMPLICIT,
                    Repertoire::UNICODE,
                    "utf8mb4",
                    "utf8mb4_bin",
                ),
            ],
            expected: None,
        },
    ];

    for (index, test) in tests.into_iter().enumerate() {
        let derived = infer_collation(&test.exprs);
        match test.expected {
            Some(expected) => assert_eq!(derived.as_ref(), Some(&expected), "Number: {index}"),
            None => assert!(
                derived.is_none(),
                "Number: {index}: expected an error, got {derived:?}"
            ),
        }
    }
}

fn is_deferred_row(functions: &[&str]) -> bool {
    functions
        .iter()
        .any(|name| matches!(*name, "date_format" | "time_format" | "cast"))
}

fn assert_row(index: usize, row: &Row) {
    for function in row.functions {
        let derived = derive_collation(function, &row.args, row.ret_type);
        match &row.expected {
            Some(expected) => assert_eq!(
                derived.as_ref().ok(),
                Some(expected),
                "Number: {index}, function: {function}"
            ),
            None => assert!(
                derived.is_err(),
                "Number: {index}, function: {function}: expected an error, got {derived:?}"
            ),
        }
    }
}
