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
// See the License for the specific language governing permissions and
// limitations under the License.

//! Source-first completion of `pkg/expression/builtin_string_test.go::TestConvert`
//! (:850) on `origin/master`. The binary-literal value rows were already
//! pinned by `tests::convert_using_invalid_binary_literal_is_null_in_both_evaluators`;
//! this module pins the remaining halves — the build-time RESULT TYPE claims
//! (charset + default collation + BINARY flag per target charset) and the
//! unknown-charset error table — re-derived from `builtinConvertSig` and its
//! function class (`pkg/expression/builtin_string.go`).

use super::*;
use tidb_ast::{QueryStmt, SelectField, Stmt};
use tidb_datatype::FieldTypeFlags;

/// Rewrites one CONVERT expression through the chunk tier and hands back the
/// resulting ScalarFunction's static result type. Go's test inspects exactly
/// this artifact: `f.getRetTp()` after `fc.getFunction`.
fn rewritten_convert_result_type(expr_text: &str) -> tidb_datatype::FieldType {
    struct NoneResolver;
    impl crate::rewriter::ColumnResolver for NoneResolver {
        fn resolve(&self, _: &[String]) -> Option<(usize, tidb_datatype::FieldType, i64)> {
            None
        }
        fn time_zone(&self) -> tidb_datatype::SessionTimeZone {
            tidb_datatype::SessionTimeZone::utc()
        }
    }
    let stmt = tidb_parser::parse(&format!("select {expr_text}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not a query")
    };
    let QueryStmt::Select(select) = query.into_inner() else {
        panic!("not select")
    };
    let SelectField::Expr { expr, .. } = &select.fields[0] else {
        panic!("no expression")
    };
    match crate::rewriter::rewrite_expr_resolved(expr, &NoneResolver).expect("rewrite") {
        crate::expression::Expression::ScalarFunction(function) => function
            .get_static_type()
            .cloned()
            .expect("CONVERT carries a result type"),
        other => panic!("expected a scalar function rewrite, got {other:?}"),
    }
}

/// GO PORT of `pkg/expression/builtin_string_test.go:850 TestConvert`'s
/// result-type table.
///
/// For every row Go requires `retType.GetCharset() == strings.ToLower(cs)`,
/// the charset's DEFAULT collation, and the BINARY flag to appear exactly on
/// the binary targets (`mysql.HasBinaryFlag`). The converted VALUE rows for
/// the two binary-literal inputs live in the earlier test; this pins the TYPE
/// metadata each of Go's six rows asserts alongside those values.
#[test]
fn convert_using_result_type_carries_target_charset_metadata() {
    // {"haha", "utf8"} / {"haha", "ascii"}: character targets carry their
    // default collation (utf8→utf8_bin, ascii→ascii_bin) and NO binary flag.
    let utf8_type = rewritten_convert_result_type("convert('haha' using utf8)");
    assert_eq!(utf8_type.charset_name(), "utf8");
    assert_eq!(utf8_type.collation_name(), "utf8_bin");
    assert!(!utf8_type.has_flag(FieldTypeFlags::BINARY));

    let ascii_type = rewritten_convert_result_type("convert('haha' using ascii)");
    assert_eq!(ascii_type.charset_name(), "ascii");
    assert_eq!(ascii_type.collation_name(), "ascii_bin");
    assert!(!ascii_type.has_flag(FieldTypeFlags::BINARY));

    // {"haha", "binary"} / {"haha", "bInAry"}: mixed-case target names fold
    // like Go's strings.ToLower, and binary is flagged.
    for target in ["binary", "bInAry"] {
        let binary_type =
            rewritten_convert_result_type(&format!("convert('haha' using {target})"));
        assert_eq!(binary_type.charset_name(), "binary", "{target}");
        assert_eq!(binary_type.collation_name(), "binary", "{target}");
        assert!(binary_type.has_flag(FieldTypeFlags::BINARY), "{target}");
    }

    // The binary-literal rows: values already verified by the earlier test;
    // their TYPES must be retagged identically ({"0x7e","BiNarY"} → '~'
    // binary-flagged; {"0xe4b8ade696870a","uTf8"} → '中文\n' unflagged).
    let bin_literal_to_binary =
        rewritten_convert_result_type("convert(0x7e using bInArY)");
    assert_eq!(bin_literal_to_binary.charset_name(), "binary");
    assert!(bin_literal_to_binary.has_flag(FieldTypeFlags::BINARY));
    let bin_literal_to_utf8 =
        rewritten_convert_result_type("convert(0xe4b8ade696870a using uTf8)");
    assert_eq!(bin_literal_to_utf8.charset_name(), "utf8");
    assert!(!bin_literal_to_utf8.has_flag(FieldTypeFlags::BINARY));
}

/// GO PORT of `pkg/expression/builtin_string_test.go:893 TestConvert`'s error
/// table plus its evaluation-time mutation row.
///
/// Both targets must be rejected BEFORE any signature evaluates. Where that
/// rejection lands differs by layer in this stack: `cp866` is a recognized
/// lexer charset but an UNSUPPORTED conversion encoding, so it fails at
/// rewrite/eval time exactly like Go's getFunction (`[expression:1115]Unknown
/// character set`); `wrongcharset` is rejected at PARSE time by the Rust
/// parser's own canonical-charset gate — one layer earlier than Go's build
/// step, same observable refusal before any evaluation.
#[test]
fn convert_using_unknown_charset_fails_before_evaluation() {
    // cp866: both evaluators refuse through the shared encoding gate.
    let ast = e("convert('haha' using cp866)");
    assert!(ast.contains("unknown character set"), "AST tier: {ast}");
    let rewritten = chunk_e("convert('haha' using cp866)");
    assert!(
        rewritten.contains("unknown character set"),
        "rewritten tier: {rewritten}"
    );

    // wrongcharset: refused by the parser itself, mirroring MySQL's
    // parse-time unknown-charset syntax check.
    assert!(
        tidb_parser::parse("select convert('haha' using wrongcharset)").is_err(),
        "wrongcharset must fail parsing"
    );
}

/// go-parity-gap: TestConvert's last block mutates an ALREADY-BUILT
/// builtinConvertSig's FieldType at runtime and expects the evaluation to fail
/// with the unknown-charset error; the Rust tier exposes no such post-build
/// mutation seam, so that half has no faithful carrier here.
#[test]
#[ignore = "go-parity-gap: no post-build FieldType mutation seam to reproduce wrongFunction.tp.SetCharset"]
fn convert_runtime_charset_mutation_gap() {}
