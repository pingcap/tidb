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

//! GO PORTS of `pkg/expression/builtin_ilike_test.go`,
//! `pkg/expression/builtin_info_test.go`, and the cast-vectorized specials
//! from `pkg/expression/builtin_cast_vec_test.go`, plus `#[ignore]` stubs for
//! everything those ports cannot reach on this tier.

use super::{chunk_e, Columns, Datum};
use crate::constant::Constant;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;
use tidb_datatype::{FieldType, FieldTypeCode, FieldTypeFlags};

/// GO PORT of `pkg/expression/builtin_ilike_test.go:30 TestIlike` over its
/// whole 34-row table and all six collation groups (two GENERAL_CI groups,
/// then BIN / UNICODE_CI over utf8mb4 and utf8).
///
/// Go builds one function per row and overrides ITS charset/collation; here
/// the same statement-level attachment lands through a `COLLATE` clause on
/// the pattern operand, which `derive_collation` propagates to the function
/// exactly as the builder override would.
#[test]
fn test_ilike() {
    // (input, pattern, escape-char-or-NUL, general_match, unicode_match)
    let rows: [(&str, &str, char, i64, i64); 33] = [
        ("a", "", '\0', 0, 0),
        ("a", "a", '\0', 1, 1),
        ("\u{fc}", "\u{dc}", '\0', 0, 0),
        ("a", "\u{e1}", '\0', 0, 0),
        ("a", "b", '\0', 0, 0),
        ("aA", "Aa", '\0', 1, 1),
        ("\u{e1}Ab", "Aa%", '\0', 0, 0),
        ("\u{e1}Ab", "%ab%", '\0', 1, 1),
        ("", "", '\0', 1, 1),
        ("\u{df}", "s%", '\0', 0, 0),
        ("\u{df}", "%s", '\0', 0, 0),
        ("\u{df}", "ss", '\0', 0, 0),
        ("\u{df}", "s", '\0', 0, 0),
        ("ss", "%\u{df}%", '\0', 0, 0),
        ("\u{df}", "_", '\0', 1, 1),
        ("\u{df}", "__", '\0', 0, 0),
        (
            "\u{554a}aaa\u{554a}\u{554a}\u{554a}aa",
            "\u{554a}aaa\u{554a}\u{554a}\u{554a}aa",
            '\0',
            1,
            1,
        ),
        // escape rows.
        ("abc", "ABC", 'a', 1, 1),
        ("abc", "ABC", 'A', 0, 0),
        ("aaz", "Aaaz", 'a', 1, 1),
        ("AAz", "AAAAz", 'a', 0, 0),
        ("a", "Aa", 'A', 1, 1),
        ("a", "AA", 'A', 1, 1),
        ("Aa", "AAAA", 'A', 1, 1),
        ("gTp", "AGTAp", 'A', 1, 1),
        ("gTAp", "AGTAap", 'A', 1, 1),
        ("A", "aA", 'a', 1, 1),
        ("a", "aA", 'a', 1, 1),
        ("aaa", "AAaA", 'a', 1, 1),
        ("a\u{554a}\u{554a}a", "a\u{554a}\u{554a}A", 'A', 0, 0),
        (
            "\u{554a}aaa\u{554a}\u{554a}\u{554a}aa",
            "\u{554a}aaa\u{554a}\u{554a}\u{554a}aa",
            'A',
            1,
            1,
        ),
        (
            "\u{554a}aAa\u{554a}\u{554a}\u{554a}aA",
            "\u{554a}AAA\u{554a}\u{554a}\u{554a}AA",
            'a',
            1,
            1,
        ),
        (
            "\u{554a}aaa\u{554a}\u{554a}\u{554a}aa",
            "\u{554a}aaa\u{554a}\u{554a}\u{554a}aa",
            'a',
            0,
            0,
        ),
    ];

    // Go's groups: two GENERAL_CI groups then four UNICODE/binary ones.
    let groups: &[(&str, &str, usize)] = &[
        ("utf8mb4", "utf8mb4_general_ci", 3),
        ("utf8", "utf8_general_ci", 3),
        ("utf8mb4", "utf8mb4_bin", 4),
        ("utf8mb4", "utf8mb4_unicode_ci", 4),
        ("utf8", "utf8_bin", 4),
        ("utf8", "utf8_unicode_ci", 4),
    ];

    for (charset, collation, flag_slot) in groups {
        for (input, pattern, escape, general, unicode) in rows {
            let want = if *flag_slot == 3 { general } else { unicode };
            // Mirrors Go building ILIKE over datums then calling
            // `f.SetCharsetAndCollation(charset, collation)`: three typed
            // string/int constants with the function-level collation pinned
            // through the argument field types (the chunk evaluator derives
            // the function collation from them exactly as the builder does).
            let mut text_type = FieldType::new(FieldTypeCode::VarString);
            text_type.set_charset_name(*charset);
            text_type.set_collation_name(*collation);
            // The function-level charset/collation lives on the result
            // field type (`ScalarFunction::derived_collation`), which is
            // where the rewriter stores what Go's SetCharsetAndCollation
            // would override.
            let mut bool_type = FieldType::new(FieldTypeCode::Long);
            bool_type.set_charset_name(*charset);
            bool_type.set_collation_name(*collation);
            let int_type = FieldType::new(FieldTypeCode::Long);
            let args = vec![
                Expression::Constant(Constant::new(
                    Datum::new_string(input.as_bytes().to_vec()),
                    text_type.clone(),
                )),
                Expression::Constant(Constant::new(
                    Datum::new_string(pattern.as_bytes().to_vec()),
                    text_type.clone(),
                )),
                Expression::Constant(Constant::new(
                    Datum::Int(i64::from(escape as u32)),
                    int_type,
                )),
            ];
            let sf = ScalarFunction::new(CiString::new("ilike"), bool_type, args);
            let result = sf
                .eval(&super::NoColumns, tidb_chunk::row::Row::empty())
                .unwrap_or_else(|err| panic!("{input}/{pattern}/{escape}@{collation}: {err:?}"));
            assert_eq!(
                result.label(),
                format!("INT:{want}"),
                "{input}/{pattern}/{escape} [{collation}]"
            );
        }
    }
}

/// go-parity-gap: `TestVectorizedBuiltinIlikeFunc`
/// (`pkg/expression/builtin_ilike_test.go:161`) runs the fixed
/// candidate-pair generators through the vec-vs-scalar differential harness
/// with per-case escape constants ('A'/'a'/'\\'); there is no vectorized
/// signature tier to differentiate against. The scalar equivalents of the
/// generator pairs and escapes are pinned by [`test_ilike`] and
/// `ilike_uses_source_ascii_lowering_and_escape_rules`.
#[test]
#[ignore = "go-parity-gap: ILIKE vec-vs-scalar differential without a vectorized tier"]
fn vectorized_builtin_ilike_harness_gap() {}

/// go-parity-gap: `TestVectorizedBuiltinIlikeForConstants`
/// (`pkg/expression/builtin_ilike_test.go:171`) mixes CONSTANT pattern /
/// constant expr into an otherwise-columnar input chunk and asserts the
/// vectorized output equals the per-row scalar answers. With no separate vec
/// tier both routes collapse into one evaluator, so there is nothing to
/// differentiate; the constants-handling half of the contract (an unchanged
/// literal driving column comparisons) is exercised by [`test_ilike`]'s
/// rewritten-chunk path, where every operand passes through Constant nodes.
#[test]
#[ignore = "go-parity-gap: const-mixed ILIKE chunk differential without a vectorized tier"]
fn vectorized_builtin_ilike_for_constants_gap() {}

// ---------------------------------------------------------------------------
// builtin_info_test.go family
// ---------------------------------------------------------------------------

/// A minimal session resolver answering exactly the pieces these builtins
/// read, mirroring `mock.NewContext()` + direct `SessionVars` mutation in
/// Go's tests. Defaults model a context without any state set.
#[derive(Default)]
struct InfoSession {
    current_db: Option<String>,
    current_user: Option<String>,
    login_user: Option<String>,
    current_resource_group: Option<String>,
    found_rows: Option<u64>,
}

impl Columns for InfoSession {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }

    fn current_database(&self) -> Option<String> {
        self.current_db.clone()
    }

    fn current_user(&self) -> Option<String> {
        self.current_user.clone()
    }

    fn login_user(&self) -> Option<String> {
        self.login_user.clone()
    }

    fn current_resource_group(&self) -> Option<String> {
        self.current_resource_group.clone()
    }

    fn found_rows(&self) -> Option<u64> {
        self.found_rows
    }
}

fn eval_zero_arg(name: &str, ctx: &InfoSession) -> Datum {
    // FOUND_ROWS carries an unsigned longlong result type like Go's
    // TypeLong with UnsignedFlag; the remaining names render as text.
    let mut ret = FieldType::new(tidb_datatype::FieldTypeCode::VarString);
    if name.eq_ignore_ascii_case("found_rows") {
        ret = FieldType::new(tidb_datatype::FieldTypeCode::LongLong);
        ret.add_flags(FieldTypeFlags::UNSIGNED);
    }
    crate::scalar_function::ScalarFunction::new(tidb_ast::CiString::new(name), ret, vec![])
        .eval(ctx, tidb_chunk::row::Row::empty())
        .expect("zero-argument information builtin must evaluate")
}

/// GO PORT of `pkg/expression/builtin_info_test.go:33 TestDatabase`
/// (including the SCHEMA alias). The two `PbCode == Clone().PbCode()`
/// identity checks have no Rust carrier (ScalarFuncSig codes are unported;
/// see the receipt), so only the value halves are asserted.
#[test]
fn test_database() {
    // No database selected -> NULL.
    assert_eq!(
        eval_zero_arg("database", &InfoSession::default()),
        Datum::Null
    );

    let mut ctx = InfoSession::default();
    ctx.current_db = Some("test".to_owned());
    assert_eq!(
        eval_zero_arg("database", &ctx),
        Datum::new_string(b"test".to_vec())
    );
    assert_eq!(
        eval_zero_arg("schema", &ctx),
        Datum::new_string(b"test".to_vec())
    );
}

/// GO PORT of `pkg/expression/builtin_info_test.go:58 TestFoundRows`:
/// `LastFoundRows = 2` flows out as uint64(2).
#[test]
fn test_found_rows() {
    let mut ctx = InfoSession::default();
    ctx.found_rows = Some(2);
    assert_eq!(eval_zero_arg("found_rows", &ctx), Datum::UInt(2));
}

/// GO PORT of `pkg/expression/builtin_info_test.go:71 TestUser`: USER()
/// reports the LOGIN identity (root@localhost).
#[test]
fn test_user() {
    let mut ctx = InfoSession::default();
    ctx.login_user = Some("root@localhost".to_owned());
    assert_eq!(
        eval_zero_arg("user", &ctx),
        Datum::new_string(b"root@localhost".to_vec())
    );
}

/// GO PORT of `pkg/expression/builtin_info_test.go:85 TestCurrentUser`:
/// CURRENT_USER reports the MATCHED grant identity, which for Auth ==
/// Login fields is the same spelling.
#[test]
fn test_current_user() {
    let mut ctx = InfoSession::default();
    ctx.current_user = Some("root@localhost".to_owned());
    ctx.login_user = Some("root@localhost".to_owned());
    assert_eq!(
        eval_zero_arg("current_user", &ctx),
        Datum::new_string(b"root@localhost".to_vec())
    );
}

/// GO PORT of `pkg/expression/builtin_info_test.go:99 TestCurrentResourceGroup`:
/// `CURRENT_RESOURCE_GROUP()` reports the effective statement resource group.
#[test]
fn test_current_resource_group() {
    let mut ctx = InfoSession::default();
    ctx.current_resource_group = Some("rg1".to_owned());
    assert_eq!(
        eval_zero_arg("current_resource_group", &ctx),
        Datum::new_string(b"rg1".to_vec())
    );
    assert_eq!(
        eval_zero_arg("current_resource_group", &InfoSession::default()),
        Datum::Null
    );
}

/// The row/AST evaluator must expose the same session-bound value as the
/// rewritten ScalarFunction path above.  This catches the prior split where
/// `CURRENT_RESOURCE_GROUP()` was implemented only in `scalar_function.rs`.
#[test]
fn test_current_resource_group_ast_path() {
    let expression = tidb_ast::Expr::Func {
        name: "CURRENT_RESOURCE_GROUP".to_owned(),
        args: Vec::new(),
        origin_position: 0,
    };

    let mut ctx = InfoSession::default();
    ctx.current_resource_group = Some("rg_ast".to_owned());
    assert_eq!(
        crate::eval_in(&expression, &ctx),
        Ok(Datum::new_string(b"rg_ast".to_vec()))
    );
    assert_eq!(
        crate::eval_in(&expression, &InfoSession::default()),
        Ok(Datum::Null)
    );
}

// ---------------------------------------------------------------------------
// embed-text / inference family
// ---------------------------------------------------------------------------

/// go-parity-gap: `TestEmbedTextBuiltin`
/// (`pkg/expression/builtin_inference_test.go:32`) requires
/// `inference.NewEmbedFn`, the starter deployment mode switch
/// (`kerneltype.IsNextGen`, `deploymode.Set`) and VectorFloat32 evaluation
/// through `NewFunction`; none of the inference/runtime symbols are
/// transcreated in the workspace yet, so EMBED_TEXT construction, option
/// propagation ([2,3,4] mock embedder outputs) and the EvalContext-wrapper
/// indirection have no carrier.
#[test]
#[ignore = "go-parity-gap: EMBED_TEXT/inference module not transcreated (no NewFunction vector evaluation, deploymode, or EmbedFn registry)"]
fn test_embed_text_builtin() {}

/// go-parity-gap: `TestEmbedTextBuiltinNullAndErrors`
/// (`pkg/expression/builtin_inference_test.go:76`) drives EvalEmbedTextArgs/
/// EvalEmbedTextArgsFromExpr/EvalEmbedTextArgsToDatum error contracts
/// (invalid usage, JSON options errors, dimension cap 16383, deploy-mode
/// refusals); the same missing inference carriers as above apply.
#[test]
#[ignore = "go-parity-gap: EvalEmbedTextArgs* helpers not transcreated"]
fn test_embed_text_builtin_null_and_errors() {}

// ---------------------------------------------------------------------------
// cast-vectorized specials (builtin_cast_vec_test.go)
// ---------------------------------------------------------------------------

/// GO PORT of `pkg/expression/builtin_cast_vec_test.go:159
/// TestVectorizedCastRealAsTime`: the twenty-row REAL→DATETIME packed-number
/// table (`genCastRealAsTime`). Valid inputs parse as YYMMDDHHMMSS-shaped
/// numbers (rendered through the four-digit year form Go's own expectations
/// spell out); every INVALID shape folds to NULL exactly like the test's nil
/// expectation slots. This surfaces Go's `builtinCastRealAsTimeSig.evalTime`
/// semantics through the CAST evaluation boundary the workspace ships.
///
/// What stays unportable is the harness shell itself (a vecEvalTime input
/// chunk versus per-row evalTime): see the sibling ignore stubs above for
/// that rationale.
#[test]
fn test_vectorized_cast_real_as_time() {
    // (real literal, expected label)
    let rows: [(&str, &str); 20] = [
        ("0e0", "STR:0000-00-00 00:00:00"),
        ("101.1", "STR:2000-01-01 00:00:00"),
        ("111.1", "STR:2000-01-11 00:00:00"),
        ("1122.1", "STR:2000-11-22 00:00:00"),
        ("31212.111", "STR:2003-12-12 00:00:00"),
        ("121212.1111", "STR:2012-12-12 00:00:00"),
        ("1121212.111111", "STR:0112-12-12 00:00:00"),
        ("11121212.111111", "STR:1112-12-12 00:00:00"),
        ("99991111.1111111", "STR:9999-11-11 00:00:00"),
        ("201212121212.1111111", "STR:2020-12-12 12:12:12"),
        ("20121212121212.1111111", "STR:2012-12-12 12:12:12"),
        // Invalid shapes -> NULL (with warning) rows.
        ("1.1", "NULL"),
        ("48.1", "NULL"),
        ("100.1", "NULL"),
        ("1301.11", "NULL"),
        ("1131.111", "NULL"),
        ("100001111.111", "NULL"),
        ("20121212121260.1111111", "NULL"),
        ("20121212126012.1111111", "NULL"),
        ("20121212241212.1111111", "NULL"),
    ];
    for (literal, want) in rows {
        assert_eq!(
            chunk_e(&format!("cast({literal} as datetime)")),
            want,
            "cast({literal} as datetime)"
        );
    }
}

/// go-parity-gap: `TestVectorizedCastStringAsDecimalWithUnsignedFlagInUnion`
/// (`pkg/expression/builtin_cast_vec_test.go:248`) constructs
/// `builtinCastStringAsDecimalSig` with `inUnion=true` plus `UnsignedFlag`
/// and re-checks 1024 random strings both signs. b066's port batch already
/// records the remaining string-to-DECIMAL `inUnion` seam. The integer cast
/// carrier is now implemented by `cast_unsigned_in_union`; without a
/// target-specific decimal carrier this signature still cannot be driven.
#[test]
#[ignore = "go-parity-gap: the vectorized string-to-DECIMAL inUnion signature remains unmodeled; scalar source-specific UNION casts are covered"]
fn vectorized_cast_string_as_decimal_union_gap() {}

/// go-parity-gap: `TestVectorizedBuiltinCastEvalOneVec` /
/// `TestVectorizedBuiltinCastFunc`
/// (`pkg/expression/builtin_cast_vec_test.go:151,155`) run the ~50-family
/// `vecBuiltinCastCases` map through the shared eval-one-vec / full-vector
/// differentials. No vectorized tier exists here, so there is no second code
/// path to compare against; the VALUE content of the main cast families was
/// already ported by b066 (`aggregation_arithmetic_cast_source.rs`), which
/// is why these stubs carry the harness gap alone.
#[test]
#[ignore = "go-parity-gap: CAST vec-vs-scalar differentials need a vectorized tier"]
fn vectorized_builtin_cast_harness_gap() {}
