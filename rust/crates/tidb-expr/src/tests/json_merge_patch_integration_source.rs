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

//! GO PORT of `pkg/expression/integration_test/integration_test.go`
//! `TestBuiltinFuncJSONMergePatch_InColumn` (`integration_test.go:2342`) and
//! `TestBuiltinFuncJSONMergePatch_InExpression` (`integration_test.go:2412`)
//! (batch part10).
//!
//! The Go harness runs the two-argument call first through stored `j JSON` /
//! `vc VARCHAR(5000)` columns and then through plain session parameters; both
//! reach the same `builtinJSONMergePatchSig` evaluation. Here the In-column
//! shape is evaluated over a one-row chunk whose columns carry exactly those
//! field types, and the in-expression shape through the constant rewrite tier,
//! so the string→document parse, the SQL-NULL truncation rules and the RFC 7396
//! merge are all code paths under test rather than duplicated logic.
//!
//! Expected documents are normalized exactly like Go's own assertion does:
//! `types.ParseBinaryJSONFromString(tt.expected).String()`
//! (`integration_test.go:2404`, `2507`) turns each expectation into canonical
//! spacing before comparison.

use super::*;
use crate::context::NoColumns;
use crate::rewriter::{rewrite_expr, rewrite_expr_resolved, ColumnResolver};
use tidb_ast::{QueryStmt, SelectField, Stmt};
use tidb_datatype::{BinaryJSON, FieldType, FieldTypeCode};

/// The top-level select-field expression of `sql`, parsed once.
fn parse_field_expr(sql: &str) -> tidb_ast::Expr {
    let stmt = tidb_parser::parse(sql).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("not select")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("no expr field")
    };
    expr.clone()
}

/// In-expression tier: document-string constants (or SQL NULL), evaluated
/// against an empty row.
fn eval_in_expression(args: &[Option<&str>]) -> Result<Datum, EvalError> {
    let mut call = String::from("json_merge_patch(");
    for (index, arg) in args.iter().enumerate() {
        if index > 0 {
            call.push_str(", ");
        }
        match arg {
            // NULL is Go's `types.NewDatum(nil)`; everything else binds as the
            // same UTF-8 text the Go driver passes.
            None => call.push_str("NULL"),
            Some(text) => {
                call.push('\'');
                call.push_str(text);
                call.push('\'');
            }
        }
    }
    call.push(')');
    let rewritten = rewrite_expr(&parse_field_expr(&format!("select {call}")))?;
    rewritten.eval(&NoColumns, tidb_chunk::row::Row::empty())
}

/// Stored-column tier: `j` (JSON column, pre-parsed on write) and `vc`
/// (VARCHAR column, raw text), evaluated over that one-row chunk.
fn eval_in_column(j: Option<&str>, vc: Option<&str>) -> Result<Datum, EvalError> {
    struct ColumnsCtx;
    impl ColumnResolver for ColumnsCtx {
        // The resolver's `(index, ret_type, unique_id)` triple places `j` at
        // chunk offset 0 and `vc` at 1.
        fn resolve(&self, path: &[String]) -> Option<(usize, FieldType, i64)> {
            match path.last()?.as_str() {
                "j" => Some((0, FieldType::new(FieldTypeCode::Json), 1)),
                "vc" => Some((1, FieldType::new(FieldTypeCode::VarString), 2)),
                _ => None,
            }
        }

        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            tidb_datatype::SessionTimeZone::utc()
        }
    }

    let rewritten = rewrite_expr_resolved(
        &parse_field_expr("select json_merge_patch(j, vc)"),
        &ColumnsCtx,
    )?;
    let json_ft = FieldType::new(FieldTypeCode::Json);
    let var_ft = FieldType::new(FieldTypeCode::VarString);
    let mut chunk = tidb_chunk::chunk::Chunk::new_with_capacity(&[json_ft, var_ft], 1);
    match j {
        // A document inserted into a JSON column arrives pre-parsed; an
        // invalid document could never have entered this column at all.
        Some(text) => chunk.append_json(0, &BinaryJSON::parse(text).expect("stored doc")),
        None => chunk.append_null(0),
    }
    match vc {
        Some(text) => chunk.append_string(1, text.to_owned()),
        None => chunk.append_null(1),
    }
    rewritten.eval(&NoColumns, chunk.get_row(0))
}

/// Canonicalizes one Go expected-document literal exactly like the Go test's
/// `types.ParseBinaryJSONFromString(expected).String()`.
fn canonical(expected: &str) -> String {
    BinaryJSON::parse(expected)
        .expect("expected literals are valid JSON documents")
        .to_string()
}

/// Asserts a successful evaluation equals Go's canonical rendering; `None`
/// stands for the Go table's `nil` expectation (a `<nil>` result cell).
fn assert_document_or_nil(datum: Datum, expected: Option<&str>, context: &str) {
    let rendered = match datum {
        Datum::Null => None,
        Datum::String(value) => Some(value.as_utf8().expect("utf8 document").to_owned()),
        other => panic!("{context}: merge patch should answer text or NULL, got {other:?}"),
    };
    match (expected, rendered) {
        (None, None) => {}
        (Some(want), Some(got)) => assert_eq!(got, canonical(want), "{context}"),
        (expected, got) => panic!("{context}: expected {expected:?}, got {got:?}"),
    }
}

#[test]
fn test_builtin_func_json_merge_patch_in_column_table() {
    // One entry per `{input [2]any, expected any}` literal of
    // TestBuiltinFuncJSONMergePatch_InColumn: `(j, vc, expected)` where a
    // `None` operand stands for the Go `nil` SQL value.
    let rows: &[(&str, &str, &str)] = &[
        (r#"{"a":"b"}"#, r#"{"a":"c"}"#, r#"{"a": "c"}"#),
        (r#"{"a":"b"}"#, r#"{"b":"c"}"#, r#"{"a": "b", "b": "c"}"#),
        (r#"{"a":"b"}"#, r#"{"a":null}"#, "{}"),
        (r#"{"a":"b", "b":"c"}"#, r#"{"a":null}"#, r#"{"b": "c"}"#),
        (r#"{"a":["b"]}"#, r#"{"a":"c"}"#, r#"{"a": "c"}"#),
        (r#"{"a":"c"}"#, r#"{"a":["b"]}"#, r#"{"a": ["b"]}"#),
        (
            r#"{"a":{"b":"c"}}"#,
            r#"{"a":{"b":"d","c":null}}"#,
            r#"{"a": {"b": "d"}}"#,
        ),
        (r#"{"a":[{"b":"c"}]}"#, r#"{"a": [1]}"#, r#"{"a": [1]}"#),
        (r#"["a","b"]"#, r#"["c","d"]"#, r#"["c", "d"]"#),
        (r#"{"a":"b"}"#, r#"["c"]"#, r#"["c"]"#),
        (r#"{"a":"foo"}"#, "null", "null"),
        (r#"{"a":"foo"}"#, r#""bar""#, r#""bar""#),
        (r#"{"e":null}"#, r#"{"a":1}"#, r#"{"e": null, "a": 1}"#),
        ("[1,2]", r#"{"a":"b","c":null}"#, r#"{"a": "b"}"#),
        ("{}", r#"{"a":{"bb":{"ccc":null}}}"#, r#"{"a": {"bb": {}}}"#),
        // RFC 7396 Example Document
        (
            r#"{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}"#,
            r#"{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}"#,
            r#"{"title":"Hello!","author":{"givenName":"John"},"tags":["example"],"content":"This will be unchanged","phoneNumber":"+01-123-456-7890"}"#,
        ),
        // From MySQL example cases: document kinds reaching the merge.
        (r#"{"a":"foo"}"#, "true", "true"),
        (r#"{"a":"foo"}"#, "false", "false"),
        (r#"{"a":"foo"}"#, "123", "123"),
        (r#"{"a":"foo"}"#, "123.1", "123.1"),
        (r#"{"a":"foo"}"#, "[1,2,3]", "[1,2,3]"),
        (r#"null"#, r#"{"a":1}"#, r#"{"a":1}"#),
        (r#"{"a":1}"#, "null", "null"),
    ];

    for (index, (j, vc, expected)) in rows.iter().enumerate() {
        let datum =
            eval_in_column(Some(j), Some(vc)).unwrap_or_else(|err| panic!("row {index}: {err:?}"));
        assert_document_or_nil(
            datum,
            Some(expected),
            &format!("row {index} ({j:?}, {vc:?})"),
        );
    }

    // The three SQL-NULL truncation rows: target NULL, value NULL, and both.
    assert_document_or_nil(
        eval_in_column(None, Some(r#"{"a":1}"#)).expect("row: j NULL"),
        None,
        "j NULL",
    );
    assert_document_or_nil(
        eval_in_column(Some(r#"{"a":1}"#), None).expect("row: vc NULL"),
        None,
        "vc NULL",
    );
    assert_document_or_nil(
        eval_in_column(None, None).expect("row: both NULL"),
        None,
        "both NULL",
    );

    // Invalid JSON text reaching the runtime parse raises ErrInvalidJSONText
    // (3140): the failure row of the In-column table.
    let err =
        eval_in_column(Some(r#"{"a":1}"#), Some("[1]}")).expect_err("invalid vc document errors");
    assert!(
        matches!(err, EvalError::Json(crate::context::JsonError::InvalidText)),
        "{err:?}"
    );
}

#[test]
fn test_builtin_func_json_merge_patch_in_expression_table() {
    // One entry per `{input []any, expected any}` literal of
    // TestBuiltinFuncJSONMergePatch_InExpression (`integration_test.go:2412`):
    // every argument is either a document string or the Go `nil`.
    let rows: &[(&[Option<&str>], Option<&str>)] = &[
        (
            &[Some(r#"{"a":"b"}"#), Some(r#"{"a":"c"}"#)],
            Some(r#"{"a": "c"}"#),
        ),
        (
            &[Some(r#"{"a":"b"}"#), Some(r#"{"b":"c"}"#)],
            Some(r#"{"a": "b", "b": "c"}"#),
        ),
        (&[Some(r#"{"a":"b"}"#), Some(r#"{"a":null}"#)], Some("{}")),
        (
            &[Some(r#"{"a":"b", "b":"c"}"#), Some(r#"{"a":null}"#)],
            Some(r#"{"b": "c"}"#),
        ),
        (
            &[Some(r#"{"a":["b"]}"#), Some(r#"{"a":"c"}"#)],
            Some(r#"{"a": "c"}"#),
        ),
        (
            &[Some(r#"{"a":"c"}"#), Some(r#"{"a":["b"]}"#)],
            Some(r#"{"a": ["b"]}"#),
        ),
        (
            &[
                Some(r#"{"a":{"b":"c"}}"#),
                Some(r#"{"a":{"b":"d","c":null}}"#),
            ],
            Some(r#"{"a": {"b": "d"}}"#),
        ),
        (
            &[Some(r#"{"a":[{"b":"c"}]}"#), Some(r#"{"a": [1]}"#)],
            Some(r#"{"a": [1]}"#),
        ),
        (
            &[Some(r#"["a","b"]"#), Some(r#"["c","d"]"#)],
            Some(r#"["c", "d"]"#),
        ),
        (&[Some(r#"{"a":"b"}"#), Some(r#"["c"]"#)], Some(r#"["c"]"#)),
        (&[Some(r#"{"a":"foo"}"#), Some("null")], Some("null")),
        (
            &[Some(r#"{"a":"foo"}"#), Some(r#""bar""#)],
            Some(r#""bar""#),
        ),
        (
            &[Some(r#"{"e":null}"#), Some(r#"{"a":1}"#)],
            Some(r#"{"e": null, "a": 1}"#),
        ),
        (
            &[Some("[1,2]"), Some(r#"{"a":"b","c":null}"#)],
            Some(r#"{"a": "b"}"#),
        ),
        (
            &[Some("{}"), Some(r#"{"a":{"bb":{"ccc":null}}}"#)],
            Some(r#"{"a": {"bb": {}}}"#),
        ),
        // RFC 7396 Example Document
        (
            &[
                Some(
                    r#"{"title":"Goodbye!","author":{"givenName":"John","familyName":"Doe"},"tags":["example","sample"],"content":"This will be unchanged"}"#,
                ),
                Some(
                    r#"{"title":"Hello!","phoneNumber":"+01-123-456-7890","author":{"familyName":null},"tags":["example"]}"#,
                ),
            ],
            Some(
                r#"{"title":"Hello!","author":{"givenName":"John"},"tags":["example"],"content":"This will be unchanged","phoneNumber":"+01-123-456-7890"}"#,
            ),
        ),
        // test cases: SQL NULL is distinct from a JSON null document.
        (&[None, Some("1")], Some("1")),
        (&[Some("1"), None], None),
        (&[None, Some("null")], Some("null")),
        (&[Some("null"), None], None),
        (&[None, Some("true")], Some("true")),
        (&[Some("true"), None], None),
        (&[None, Some("false")], Some("false")),
        (&[Some("false"), None], None),
        (&[None, Some("[1,2,3]")], Some("[1,2,3]")),
        (&[Some("[1,2,3]"), None], None),
        (&[None, Some(r#"{"a":"foo"}"#)], None),
        (&[Some(r#"{"a":"foo"}"#), None], None),
        (
            &[
                Some(r#"{"a":"foo"}"#),
                Some(r#"{"a":null}"#),
                Some(r#"{"b":"123"}"#),
                Some(r#"{"c":1}"#),
            ],
            Some(r#"{"b":"123","c":1}"#),
        ),
        (
            &[
                Some(r#"{"a":"foo"}"#),
                Some(r#"{"a":null}"#),
                Some(r#"{"c":1}"#),
            ],
            Some(r#"{"c":1}"#),
        ),
        (
            &[Some(r#"{"a":"foo"}"#), Some(r#"{"a":null}"#), Some("true")],
            Some("true"),
        ),
        (
            &[
                Some(r#"{"a":"foo"}"#),
                Some(r#"{"d":1}"#),
                Some(r#"{"a":{"bb":{"ccc":null}}}"#),
            ],
            Some(r#"{"a":{"bb":{}},"d":1}"#),
        ),
        (
            &[Some("null"), Some("true"), Some("[1,2,3]")],
            Some("[1,2,3]"),
        ),
        // From mysql Example Test Cases: two operands placed in every slot.
        (
            &[None, Some("null"), Some("[1,2,3]"), Some(r#"{"a":1}"#)],
            Some(r#"{"a": 1}"#),
        ),
        (
            &[Some("null"), None, Some("[1,2,3]"), Some(r#"{"a":1}"#)],
            Some(r#"{"a": 1}"#),
        ),
        (
            &[Some("null"), Some("[1,2,3]"), None, Some(r#"{"a":1}"#)],
            None,
        ),
        (
            &[Some("null"), Some("[1,2,3]"), Some(r#"{"a":1}"#), None],
            None,
        ),
        (
            &[None, Some("null"), Some(r#"{"a":1}"#), Some("[1,2,3]")],
            Some("[1,2,3]"),
        ),
        (
            &[Some("null"), None, Some(r#"{"a":1}"#), Some("[1,2,3]")],
            Some("[1,2,3]"),
        ),
        (
            &[Some("null"), Some(r#"{"a":1}"#), None, Some("[1,2,3]")],
            Some("[1,2,3]"),
        ),
        (
            &[Some("null"), Some(r#"{"a":1}"#), Some("[1,2,3]"), None],
            None,
        ),
        (
            &[None, Some("null"), Some(r#"{"a":1}"#), Some("true")],
            Some("true"),
        ),
        (
            &[Some("null"), None, Some(r#"{"a":1}"#), Some("true")],
            Some("true"),
        ),
        (
            &[Some("null"), Some(r#"{"a":1}"#), None, Some("true")],
            Some("true"),
        ),
        (
            &[Some("null"), Some(r#"{"a":1}"#), Some("true"), None],
            None,
        ),
        // Non-object last item replaces the whole chain.
        (
            &[
                Some("true"),
                Some("false"),
                Some("[]"),
                Some("{}"),
                Some("null"),
            ],
            Some("null"),
        ),
        (
            &[
                Some("false"),
                Some("[]"),
                Some("{}"),
                Some("null"),
                Some("true"),
            ],
            Some("true"),
        ),
        (
            &[
                Some("true"),
                Some("[]"),
                Some("{}"),
                Some("null"),
                Some("false"),
            ],
            Some("false"),
        ),
        (
            &[
                Some("true"),
                Some("false"),
                Some("{}"),
                Some("null"),
                Some("[]"),
            ],
            Some("[]"),
        ),
        (
            &[
                Some("true"),
                Some("false"),
                Some("{}"),
                Some("null"),
                Some("1"),
            ],
            Some("1"),
        ),
        (
            &[
                Some("true"),
                Some("false"),
                Some("{}"),
                Some("null"),
                Some("1.8"),
            ],
            Some("1.8"),
        ),
        (
            &[
                Some("true"),
                Some("false"),
                Some("{}"),
                Some("null"),
                Some(r#""112""#),
            ],
            Some(r#""112""#),
        ),
        (&[Some(r#"{"a":"foo"}"#), None], None),
        (&[None, Some(r#"{"a":"foo"}"#)], None),
        (&[Some(r#"{"a":"foo"}"#), Some("false")], Some("false")),
        (&[Some(r#"{"a":"foo"}"#), Some("123")], Some("123")),
        (&[Some(r#"{"a":"foo"}"#), Some("123.1")], Some("123.1")),
        (&[Some(r#"{"a":"foo"}"#), Some("[1,2,3]")], Some("[1,2,3]")),
        (&[Some("null"), Some(r#"{"a":1}"#)], Some(r#"{"a":1}"#)),
        (&[Some(r#"{"a":1}"#), Some("null")], Some("null")),
        (
            &[
                Some(r#"{"a":"foo"}"#),
                Some(r#"{"a":null}"#),
                Some(r#"{"b":"123"}"#),
                Some(r#"{"c":1}"#),
            ],
            Some(r#"{"b":"123","c":1}"#),
        ),
        (
            &[
                Some(r#"{"a":"foo"}"#),
                Some(r#"{"a":null}"#),
                Some(r#"{"c":1}"#),
            ],
            Some(r#"{"c":1}"#),
        ),
        (
            &[Some(r#"{"a":"foo"}"#), Some(r#"{"a":null}"#), Some("true")],
            Some("true"),
        ),
        (
            &[
                Some(r#"{"a":"foo"}"#),
                Some(r#"{"d":1}"#),
                Some(r#"{"a":{"bb":{"ccc":null}}}"#),
            ],
            Some(r#"{"a":{"bb":{}},"d":1}"#),
        ),
    ];

    for (index, (args, expected)) in rows.iter().enumerate() {
        let datum =
            eval_in_expression(args).unwrap_or_else(|err| panic!("row {index} {args:?}: {err:?}"));
        assert_document_or_nil(datum, *expected, &format!("row {index} {args:?}"));
    }

    // Invalid json text raises ErrInvalidJSONText (3140) wherever it appears
    // in the argument list -- the three failure rows of the Go table.
    for args in [
        &[Some(r#"{"a":1}"#), Some("[1]}")][..],
        &[Some(r#"{{"a":1}"#), Some("[1]"), Some("null")][..],
        &[Some(r#"{"a":1}"#), Some("jjj"), Some("null")][..],
    ] {
        let err = eval_in_expression(args).expect_err("invalid document errors");
        assert!(
            matches!(err, EvalError::Json(crate::context::JsonError::InvalidText)),
            "{args:?}: {err:?}"
        );
    }
}
