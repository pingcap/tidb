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

//! Go's `types.ETString` ARGUMENT declaration, at the value boundary
//! (`crate::arg_eval_type`) -- the third rung of the layer whose first two are
//! `datetime.rs` and `etint_argument.rs`, kept in its own file so none of the
//! three grows toward the size ratchet.
//!
//! # Recorded witnesses, and Go-derived values
//!
//! The corpus DOES speak for this rung, at
//! `t/expression/builtin.test:1589-1590` (issue #50850):
//!
//! ```sql
//! create table t3 (col1 double NOT NULL, col2 bit(8) NOT NULL);
//! insert into t3 (col1, col2) values (2306.9705216860984, x'31'), ... (9546.629394674586, x'ff'), ...;
//! select hex(r) as r0 from (select ELT(2, col1, col2) as r from t3 group by ELT(2, col1, col2)) as t order by r0;
//! ```
//!
//! whose recording (`r/expression/builtin.result:3338-3347`) is the ten bit
//! values `01 31 34 5D 65 A5 A6 B1 D5 FF` -- the BIT column's RAW BYTES, four
//! of which are not valid UTF-8 on their own. This tier used to raise a hard
//! evaluation error on that statement and the replay counted it OUT OF DOMAIN;
//! it is now compared and matched. Everything else below is GO-DERIVED,
//! captured statement by statement from real TiDB through `gorun`
//! (`rust/difftests/gorun`) over
//!
//! ```sql
//! create table t (e enum('a','b','c'), s set('a','b'), v varbinary(4), b bit(8));
//! insert into t values ('b','a,b', unhex('61'), b'00000011');
//! ```

use super::*;
use crate::arg_eval_type::{eval_string, wrap_string_args};
use tidb_datatype::{BinaryLiteral, Collation, MysqlEnum, MysqlSet};

/// Both evaluator tiers impose the layer, so every expression assertion is
/// made twice. A tier that stopped calling `wrap_string_args` would otherwise
/// keep passing on the other one's evidence.
fn both(expr: &str) -> (String, String) {
    (e(expr), chunk_e(expr))
}

/// A `types.ETString` argument of a NON-string kind is CAST, not refused.
/// Captured: `quote(65)` is `'65'`, `elt(1,65)` is `65` and `ltrim(65)` is
/// `65`; `hex(quote(65))` is `27363527`.
#[test]
fn a_non_string_etstring_argument_is_cast_before_the_signature_runs() {
    for (expr, want) in [
        ("quote(65)", "STR:'65'"),
        ("elt(1, 65)", "STR:65"),
        ("ltrim(65)", "STR:65"),
        ("rtrim(65)", "STR:65"),
        // The selector is `types.ETInt`, so a STRING selector is parsed by
        // the other rung's cast, not by this one. Captured:
        // `elt('2abc','x','y','z')` is `y`.
        ("elt('2abc','x','y','z')", "STR:y"),
    ] {
        let (row, chunk) = both(expr);
        assert_eq!(row, want, "{expr}");
        assert_eq!(chunk, want, "{expr} (chunk tier)");
    }
}

/// The mask is the `argTps...` TAIL, and `QUOTE`/`LTRIM`/`RTRIM` each declare
/// `types.ETString, types.ETString` -- the FIRST of which is the RETURN type.
/// A mask shifted by one would cast nothing at all for those three (they are
/// one-argument functions), and `ELT`'s would cast its integer SELECTOR.
///
/// The zero entries are the measured NON-members: five names whose
/// `types.ETString` is chosen by the arguments' own types (so what varies is
/// the SIGNATURE), two whose verifier makes the declared cast unreachable,
/// and `JSON_TYPE`, whose `types.ETString` is its RETURN type over a
/// `types.ETJson` argument. See `crate::arg_eval_type::string_arg_mask`'s doc
/// for the quoted Go behind each.
#[test]
fn the_string_mask_positions_follow_the_argtps_tail() {
    // Position 0 is the SELECTOR and is `types.ETInt`; every other position
    // is `types.ETString`, for any arity.
    assert_eq!(
        wrap_string_args(
            "ELT",
            vec![Datum::Int(2), Datum::Int(7), Datum::Int(8)],
            &[],
            &NoColumns,
        )
        .unwrap(),
        vec![
            Datum::Int(2),
            Datum::new_string("7"),
            Datum::new_string("8"),
        ]
    );
    // ...and the ETInt rung owns that selector, so the two masks are
    // complementary rather than overlapping.
    assert_eq!(
        crate::arg_eval_type::int_arg_mask("ELT"),
        1,
        "the selector is the ETInt rung's"
    );

    for one_argument in ["QUOTE", "LTRIM", "RTRIM"] {
        assert_eq!(
            wrap_string_args(one_argument, vec![Datum::Int(7)], &[], &NoColumns).unwrap(),
            vec![Datum::new_string("7")],
            "{one_argument}"
        );
    }

    for non_member in [
        "FIELD",
        "GREATEST",
        "LEAST",
        "INTERVAL",
        "JSON_VALID",
        "JSON_TYPE",
        "JSON_QUOTE",
        "JSON_UNQUOTE",
        "TRIM",
        "CONCAT",
    ] {
        assert_eq!(
            wrap_string_args(non_member, vec![Datum::Int(7)], &[], &NoColumns).unwrap(),
            vec![Datum::Int(7)],
            "{non_member} must not be routed"
        );
    }
}

/// `ELT` accepts an UNBOUNDED argument list, and Go declares its
/// `types.ETString` tail with a LOOP. The mask's top bit therefore covers
/// every position from 31 upward -- without that, argument 35 would either be
/// left un-cast (and the body would refuse it) or shift past the mask's width
/// and PANIC.
#[test]
fn the_masks_top_bit_covers_an_unbounded_argument_list() {
    let mut vals = vec![Datum::Int(35)];
    vals.extend((0..40).map(Datum::Int));
    let out = wrap_string_args("ELT", vals, &[], &NoColumns).unwrap();
    assert_eq!(out[0], Datum::Int(35), "the selector stays an integer");
    assert_eq!(out[31], Datum::new_string("30"));
    assert_eq!(out[40], Datum::new_string("39"));
    // And the body reads the selected one, 35 positions in.
    assert_eq!(
        crate::string_fn::elt(&out).unwrap(),
        Datum::new_string("34")
    );
}

/// `WrapWithCastAsString`'s early return is `EvalType() == types.ETString`,
/// and `FieldType.EvalType` puts ENUM and SET there. Those two therefore
/// reach the signature UNTOUCHED and are read as their NAME -- the exact
/// opposite of the `types.ETInt` rung, where the same values arrive as their
/// ordinal. Captured: `quote(e)` is `'b'`, `ltrim(e)` is `b`, `elt(1,e)` is
/// `b` and `elt(1,s)` is `a,b`.
///
/// BIT is the hybrid that is NOT string-typed (`mysql.TypeBit` is
/// `types.ETInt`), so it takes the cast and lands on
/// `castAsStringFunctionClass`'s own hybrid arm, whose value is the RAW
/// BYTES. Captured: `hex(ltrim(b))` and `hex(elt(1,b))` are both `03`, not
/// the digit `3`'s `33`. Answering `33` here is the tempting wrong reading,
/// and it is what a cast built from `Datum::to_i64` would give.
#[test]
fn the_string_typed_hybrids_take_the_early_return_and_bit_does_not() {
    let enum_value = Datum::Enum(MysqlEnum::new("b", 2), Collation::Utf8Mb4Bin);
    let set_value = Datum::Set(MysqlSet::new("a,b", 3), Collation::Utf8Mb4Bin);
    let literal = Datum::BinaryLiteral(BinaryLiteral::from_uint(3, None));
    for value in [
        enum_value.clone(),
        set_value.clone(),
        literal.clone(),
        Datum::new_string("kept"),
        Datum::new_bytes(vec![0xFF]),
        Datum::Null,
    ] {
        let out = wrap_string_args("QUOTE", vec![value.clone()], &[None], &NoColumns).unwrap();
        assert_eq!(out[0], value, "{value:?} must survive the cast verbatim");
    }
    // The NAME, never the ordinal.
    assert_eq!(eval_string(&enum_value).unwrap().unwrap(), b"b");
    assert_eq!(eval_string(&set_value).unwrap().unwrap(), b"a,b");

    // BIT alone is rewritten, and to its bytes.
    let bit = Datum::Bit(BinaryLiteral::from_uint(3, None));
    let out = wrap_string_args("LTRIM", vec![bit], &[None], &NoColumns).unwrap();
    assert_eq!(out[0], Datum::new_bytes(vec![3]));
}

/// The reader is Go's `EvalString`, which returns a Go `string`: a BYTE
/// sequence that is never validated as UTF-8. Every one of these was a hard
/// evaluation error before the rung, and every expected value is captured
/// from real TiDB.
///
/// `QUOTE` is the one that does NOT simply pass the bytes through: Go's
/// `Quote` opens with `runes := []rune(str)`, so each malformed byte becomes
/// one U+FFFD. `27EFBFBD27` is `'` U+FFFD `'` -- five bytes out of a
/// one-byte argument, which no byte-preserving implementation can produce and
/// no UTF-8-refusing one can reach.
#[test]
fn an_etstring_argument_is_read_as_bytes_not_as_utf8() {
    let binary = Datum::new_bytes(vec![0xFF]);
    let bit = Datum::Bit(BinaryLiteral::from_uint(0xFF, None));
    for value in [binary, bit] {
        let quoted = wrap_string_args("QUOTE", vec![value.clone()], &[None], &NoColumns).unwrap();
        assert_eq!(
            crate::string_fn::quote(&quoted).unwrap(),
            Datum::new_bytes(vec![b'\'', 0xEF, 0xBF, 0xBD, b'\'']),
            "quote({value:?})"
        );

        let trimmed = wrap_string_args("LTRIM", vec![value.clone()], &[None], &NoColumns).unwrap();
        assert_eq!(
            crate::builtin_ext::string2::dispatch("LTRIM", &trimmed, &NoColumns)
                .unwrap()
                .unwrap(),
            Datum::new_bytes(vec![0xFF]),
            "ltrim({value:?})"
        );

        let selected = wrap_string_args(
            "ELT",
            vec![Datum::Int(1), value.clone()],
            &[None, None],
            &NoColumns,
        )
        .unwrap();
        assert_eq!(
            crate::string_fn::elt(&selected).unwrap(),
            Datum::new_bytes(vec![0xFF]),
            "elt(1,{value:?})"
        );
    }
}

/// `LTRIM`/`RTRIM` strip only U+0020, and the byte scan must not eat a byte
/// that merely CONTAINS `0x20` in a multi-byte sequence -- it cannot, because
/// UTF-8 continuation bytes all have the high bit set, and that is exactly
/// why Go can use `strings.TrimLeft` on an unvalidated string. The
/// leading/trailing asymmetry is the other half: `LTRIM` must not touch the
/// tail and `RTRIM` must not touch the head.
#[test]
fn the_space_scan_is_one_sided_and_byte_exact() {
    let cases = [
        ("LTRIM", "  a  ", "a  "),
        ("RTRIM", "  a  ", "  a"),
        // All-space and empty are the two lengths at which an off-by-one in
        // the `take_while` count would run off the slice.
        ("LTRIM", "   ", ""),
        ("RTRIM", "   ", ""),
        ("LTRIM", "", ""),
        ("RTRIM", "", ""),
        // A multi-byte character adjacent to the stripped run.
        ("LTRIM", " 中 ", "中 "),
        ("RTRIM", " 中 ", " 中"),
    ];
    for (name, input, want) in cases {
        let vals =
            wrap_string_args(name, vec![Datum::new_string(input)], &[None], &NoColumns).unwrap();
        assert_eq!(
            crate::builtin_ext::string2::dispatch(name, &vals, &NoColumns)
                .unwrap()
                .unwrap(),
            Datum::new_string(want),
            "{name}({input:?})"
        );
    }
}

/// `FIELD` is the measured NON-member, and this is what it costs to get it
/// wrong in the other direction. Go picks the SIGNATURE from
/// `isAllString`/`isAllNumber` over the arguments' `EvalType()`s, and that
/// switch puts ENUM/SET under `types.ETString` and BIT under `types.ETInt`.
/// Captured over the table in this module's doc:
///
/// ```text
/// field(e,'b')      -> 1      field(s,'a,b')  -> 1      field(x'61','a') -> 1
/// field(e,2)        -> 1      field(e,'b',2)  -> 2      field(e,b)       -> 0
/// field(b,3)        -> 1      field(b,'3')    -> 1      field(v,'a')     -> 1
/// ```
///
/// The first three are the ones this tier answered `0` to: they are
/// all-string lists whose hybrids fell through to the REAL signature. The
/// next three are the guard against over-application -- a MIXED list must
/// still take the real signature, where an ENUM compares as its ORDINAL, so
/// `field(e,2)` is `1` and `field(e,b)` is `0`.
#[test]
fn field_reads_a_hybrid_by_its_own_eval_type() {
    let e = Datum::Enum(MysqlEnum::new("b", 2), Collation::Utf8Mb4Bin);
    let s = Datum::Set(MysqlSet::new("a,b", 3), Collation::Utf8Mb4Bin);
    let b = Datum::Bit(BinaryLiteral::from_uint(3, None));
    let hex = Datum::BinaryLiteral(BinaryLiteral::from_uint(0x61, None));
    let cases: [(Vec<Datum>, i64); 9] = [
        (vec![e.clone(), Datum::new_string("b")], 1),
        (vec![s.clone(), Datum::new_string("a,b")], 1),
        (vec![hex, Datum::new_string("a")], 1),
        (vec![e.clone(), Datum::Int(2)], 1),
        (vec![e.clone(), Datum::new_string("b"), Datum::Int(2)], 2),
        (vec![e, b.clone()], 0),
        (vec![b.clone(), Datum::Int(3)], 1),
        (vec![b, Datum::new_string("3")], 1),
        (
            vec![Datum::new_bytes(b"a".to_vec()), Datum::new_string("a")],
            1,
        ),
    ];
    for (vals, want) in cases {
        assert_eq!(
            crate::string_fn::field(&vals, &NoColumns).unwrap(),
            Datum::Int(want),
            "field({vals:?})"
        );
    }
}

/// The layer's contract, the same one `eval_int` states: a position the mask
/// named is a position the body may assume was cast, so an un-cast datum
/// arriving there means an evaluator entry point skipped the wrap. Refusing
/// is what makes that visible; a silent rendering would hide it.
#[test]
fn an_uncast_etstring_argument_is_refused_rather_than_re_derived() {
    assert!(eval_string(&Datum::Int(1)).is_err());
    assert!(eval_string(&Datum::Real(1.0)).is_err());
    assert!(eval_string(&Datum::Bit(BinaryLiteral::from_uint(3, None))).is_err());
    assert_eq!(eval_string(&Datum::Null).unwrap(), None);
}
