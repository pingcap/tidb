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

//! Source-first ports of `pkg/expression.part5`'s IN-family extras:
//! `builtin_other_vec_test.go::TestInDecimal`, the collation tail row of
//! `builtin_other_test.go::TestInFunc`, and the representable arms of the
//! generated vectorized-IN harnesses
//! (`builtin_other_vec_generated_test.go::TestVectorizedBuiltinOtherEvalOneVecGenerated`
//! / `TestVectorizedBuiltinOtherFuncGenerated`). Expectations are re-derived
//! from the Go sources on `origin/master`.

use super::*;
use crate::column::Column;
use crate::expression::Expression;
use crate::scalar_function::ScalarFunction;
use tidb_ast::CiString;
use tidb_chunk::chunk::Chunk;
use tidb_datatype::{Decimal, FieldType, FieldTypeCode as C};

/// GO PORT of `pkg/expression/builtin_other_vec_test.go:66 TestInDecimal`.
///
/// Go builds an ETDecimal `col0 in (col1)` over 1024 chunk rows where every
/// pair holds the SAME numeric value at DIFFERENT scales (`v` vs `v+"00"`),
/// first requiring the two digits-frac counts to differ, and then requires the
/// result to be `1` for every row — DECIMAL comparison is numeric, so scale
/// never decides membership. Go fills with `rand.Intn`; deterministic sweeps
/// keep the same invariant reproducible.
#[test]
fn in_decimal_across_scales_compares_numerically() {
    let ft = FieldType::new(C::NewDecimal);
    // Deterministic value/scale sweep standing in for Go's random generator:
    // quotients 0..=255 x fractional parts of increasing width, so operand
    // scales always differ while values stay equal.
    let mut left = Vec::new();
    let mut right = Vec::new();
    for quotient in 0..256_u64 {
        for frac_width in [1_u32, 3, 5] {
            let digits = "123456789".repeat(3);
            let base = format!("{quotient}.{}", &digits[..frac_width as usize]);
            let padded = format!("{base}00");
            let d0 = Decimal::parse_mysql(&base).0;
            let d1 = Decimal::parse_mysql(&padded).0;
            // Precondition Go asserts per row: the scales differ...
            assert_ne!(
                d0.precision_and_frac().1,
                d1.precision_and_frac().1,
                "{base} vs {padded}"
            );
            // ...and the trailing zeros leave the numeric VALUE untouched,
            // which is what makes each Int(1) below (the Go test's actual
            // assertion) a real numeric-equality result rather than a
            // trivially-true same-datum comparison.
            left.push(Datum::Decimal(d0));
            right.push(Datum::Decimal(d1));
        }
    }

    let mut input = Chunk::new_with_capacity(&[ft.clone(), ft.clone()], left.len());
    let mut column = Column::new(0, ft.clone());
    column.index = 0;
    let lhs = Expression::Column(column);
    let mut probe = Column::new(1, ft);
    probe.index = 1;
    let rhs = Expression::Column(probe);

    for (low, high) in left.iter().zip(right.iter()) {
        input.append_datum(0, low);
        input.append_datum(1, high);
    }

    let in_func = ScalarFunction::new(
        CiString::new("in"),
        FieldType::new(C::LongLong),
        vec![lhs.clone(), rhs.clone()],
    );

    for (index, (_low, high)) in left.iter().zip(right.iter()).enumerate() {
        let row = input.get_row(index);
        let lhs_value = lhs.eval(&crate::context::NoColumns, row).unwrap();
        assert_eq!(
            in_func.eval(&crate::context::NoColumns, row).unwrap(),
            Datum::Int(1),
            "row {index}: {lhs_value:?} in ({high:?}) must compare equal across scales"
        );
    }
}

/// GO PORT of `pkg/expression/builtin_other_test.go:277 TestInFunc`'s closing
/// collation row. Go compares two datums whose OWN collation is
/// `utf8_general_ci`: `'a'` against `'Á'`, expecting a MATCH (result Int 1) —
/// TiDB's general_ci weight table folds accents, not only case. Both operands
/// carry that collation themselves rather than inheriting one.
///
/// The port raises each side with an explicit COLLATE instead of setting per-
/// datum metadata. That makes the four-byte spelling the faithful carrier:
/// general_ci weights are shared across the utf8/utf8mb4 names, but attaching
/// the LEGACY `utf8_general_ci` name to a utf8mb4-typed literal is refused as
/// a charset/collation mix here — exactly Go's 1267 illegal-mix rule for
/// cross-charset operands. (Withdrawn first draft: this test initially used
/// `utf8_general_ci` on both sides; the rewritten tier answered
/// CollationCharsetMismatch and the AST tier fell back to uncollated binary
/// comparison, so those spellings do NOT reproduce the Go datum shapes and
/// were replaced by the utf8mb4 rows below.)
#[test]
fn in_func_collation_row_general_ci_folds_case_and_accents() {
    // Accent folding gives a MATCH on the rewritten/chunk tier. The AST-value
    // tier is NOT asserted here: its COLLATE arm re-tags nothing and it has no
    // collation derivation at all, so an explicit-COLLATE comparison there
    // degrades to binary equality ("INT:0") — the SAME documented boundary
    // recorded by tests::weight_string_and_load_file_source_vectors.
    assert_eq!(
        chunk_e("'a' collate utf8mb4_general_ci in ('Á' collate utf8mb4_general_ci)"),
        "INT:1"
    );
    // The binary form does NOT fold, proving the row above exercises the
    // collation path instead of raw byte equality.
    assert_eq!(
        chunk_e("'a' collate utf8mb4_bin in ('Á' collate utf8mb4_bin)"),
        "INT:0"
    );
}

/// GO PORT of the representable INT/STRING/DECIMAL arms of
/// `pkg/expression/builtin_other_vec_generated_test.go:285/289`
/// (`TestVectorizedBuiltinOtherEvalOneVecGenerated` /
/// `TestVectorizedBuiltinOtherFuncGenerated`). Those harnesses evaluate
/// three-item IN lists over generated rows per eval type; `inGener` draws
/// signed small integers, their decimal stringifications (`FormatInt`), and
/// `randNum * 100000` decimals. The loop pins both tiers on deterministic
/// boundary rows drawn from those same domains.
#[test]
fn generated_in_harness_int_string_decimal_arms_agree_across_tiers() {
    // ETInt arm: negatives, zero, positives within [-9, 9].
    for sql in [
        "7 in (7, -9, 9)",
        "-9 in (-9, -8, 0)",
        "0 in (1, 2, 0)",
        "10 not in (9, -9, 0)",
        "NULL in (NULL, 1)",
    ] {
        assert_eq!(e(sql), chunk_e(sql), "{sql}");
    }
    // ETString arm: strconv.FormatInt renderings of the same domain.
    for sql in [
        "'9' in ('9', '-9', '0')",
        "'-3' in ('3', '-3')",
        "'12' in ('12', '11', '10')",
        "NULL in ('a')",
    ] {
        assert_eq!(e(sql), chunk_e(sql), "{sql}");
    }
    // ETDecimal arm: randNum * 100000 keeps six fraction digits.
    for sql in [
        "900000.000000 in (900000.000000, 800000.000000)",
        "100000.000000 in (100000.0)",
        "0.000000 in (0.00, 0.0)",
        "NULL in (100000.000000)",
    ] {
        assert_eq!(e(sql), chunk_e(sql), "{sql}");
    }
    // Cross-check two exact membership decisions per type so agreement cannot
    // hide a shared regression to constant true/false.
    assert_eq!(e("7 in (7, -9, 9)"), "INT:1");
    assert_eq!(e("'-3' in ('3', '-3')"), "INT:1");
    assert_eq!(e("100000.000000 in (100000.0)"), "INT:1");
    assert_eq!(e("10 not in (9, -9, 0)"), "INT:1");
}

/// GO PORT of the DATETIME/TIMESTAMP, DURATION, and JSON arms of
/// `vecBuiltinOtherGeneratedCases`.
///
/// Go's `inFunctionClass` casts every argument to the first argument's eval
/// type, then dispatches to `builtinInTimeSig`, `builtinInDurationSig`, or
/// `builtinInJSONSig`. The rewritten Rust evaluator now keeps those typed
/// signatures on the same path: temporal values compare by `Time`, duration
/// values by `MySqlDuration`, and JSON values by binary-JSON ordering, all
/// with the source's three-valued NULL result.
#[test]
fn generated_in_harness_temporal_duration_json_arms() {
    // DATETIME / TIMESTAMP signatures compare the temporal core, not the
    // formatted string or numeric context. The matching value is the second
    // list member; a NULL-only miss preserves the Go hasNull result.
    for sql in [
        "cast('2019-11-02 22:00:05' as datetime) in (cast('2019-11-02 22:00:04' as datetime), cast('2019-11-02 22:00:05' as datetime))",
        "cast('2019-11-02 22:00:05' as datetime) in (cast('2019-11-02 22:00:04' as datetime), NULL)",
    ] {
        let want = if sql.ends_with("datetime))") {
            "INT:1"
        } else {
            "NULL"
        };
        assert_eq!(chunk_e(sql), want, "{sql}");
    }
    // A string list member is cast to the first DATETIME argument's domain
    // before comparison, just as `newBaseBuiltinFuncWithTp` does in Go.
    assert_eq!(
        chunk_e(
            "cast('2019-11-02 22:00:05' as datetime) in ('2019-11-02 22:00:05', '2019-11-02 22:00:04')"
        ),
        "INT:1"
    );

    // DURATION is the regression that the generic comparison ladder could
    // lose: it must compare the actual elapsed duration, not its numeric
    // fallback. The non-match plus NULL row exercises Go's hasNull bit.
    assert_eq!(
        chunk_e("cast('00:00:01' as time) in (cast('00:00:02' as time), cast('00:00:01' as time))"),
        "INT:1"
    );
    assert_eq!(
        chunk_e("cast('00:00:01' as time) in (cast('00:00:02' as time), NULL)"),
        "NULL"
    );

    // JSON membership uses CompareBinaryJSON's type precedence and structural
    // equality. Objects with the same members in a different key order are
    // equal, while a different scalar plus NULL remains NULL.
    assert_eq!(
        chunk_e(
            r#"cast('{"key":1,"other":[2]}' as json) in (cast('{"other":[2],"key":1}' as json), cast('{"key":2}' as json))"#
        ),
        "INT:1"
    );
    assert_eq!(
        chunk_e("cast('1' as json) in (cast('2' as json), NULL)"),
        "NULL"
    );
    // The JSON signature disables ParseToJSONFlag on list expressions. A
    // plain string therefore remains a JSON string value, not a parsed JSON
    // number; this distinguishes the typed signature from generic JSON
    // comparison, which would parse both documents and incorrectly match.
    assert_eq!(chunk_e("cast('1' as json) in ('1', '2')"), "INT:0");
}
