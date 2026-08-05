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

//! Go's `types.ETInt` ARGUMENT declaration, at the value boundary
//! (`crate::arg_eval_type`) -- the second rung of the same layer
//! `datetime.rs`'s `an_etdatetime_argument_is_cast_before_the_signature_runs`
//! covers, kept in its own file so neither grows toward the size ratchet.
//!
//! NO RECORDED ROW EXISTS for any assertion here. `tests/integrationtest/r/**`
//! calls `ROUND`/`TRUNCATE` only with literal integer scales and `INSERT`/
//! `LOCATE`/`MAKE_SET` only with literal integer positions, which is why the
//! whole-corpus survey does not move on this change. Every expected value is
//! therefore GO-DERIVED, captured statement by statement from real TiDB
//! through `gorun` (`rust/difftests/gorun`) and pinned absolutely.

use super::*;
use crate::arg_eval_type::wrap_int_args;
use tidb_datatype::{BinaryLiteral, Collation, MysqlEnum, MysqlSet};

/// Both evaluator tiers impose the layer, so every value assertion is made
/// twice. A tier that stopped calling `wrap_int_args` would otherwise keep
/// passing on the other one's evidence.
fn both(expr: &str) -> (String, String) {
    (e(expr), chunk_e(expr))
}

/// The `types.ETInt` argument of a NON-integer kind is CAST, not refused and
/// not read as NULL. Captured: `round(1.2345,'2')` is `1.23`,
/// `truncate(1.2345,'2')` is `1.23`, `insert('abcdef',cast(2 as unsigned),2,'X')`
/// is `aXdef`, `locate('b','abcdef','2')` is `2` and `make_set('3','a','b','c')`
/// is `a,b`.
#[test]
fn a_non_integer_etint_argument_is_cast_before_the_signature_runs() {
    for (expr, want) in [
        ("round(1.2345,'2')", "DEC:1.23"),
        ("truncate(1.2345,'2')", "DEC:1.23"),
        // Before this rung the whole call was NULL: `str_insert` matched
        // `Datum::Int` only, so an UNSIGNED position fell off the pattern.
        (
            "insert('abcdef', cast(2 as unsigned), 2, 'X')",
            "STR:aXdef",
        ),
        ("locate('b','abcdef','2')", "INT:2"),
        ("make_set('3','a','b','c')", "STR:a,b"),
    ] {
        let (row, chunk) = both(expr);
        assert_eq!(row, want, "{expr}");
        assert_eq!(chunk, want, "{expr} (chunk tier)");
    }
}

/// The BOUNDARY of the cast itself. Go's `WrapWithCastAsInt` builds an
/// ordinary `CAST(x AS SIGNED)`, whose string arm takes the leading integer
/// RUN (no `.`, no exponent) and whose decimal arm rounds HALF-UP. Captured:
/// `round(3.14,'abc')` is `3` (scale 0, not NULL and not an error),
/// `round(3.14,2.5)` is `3.140` (scale 3) and `round(3.14,2.4)` is `3.14`
/// (scale 2) -- the two decimals straddle the half-up boundary, so a cast
/// that truncated instead of rounding would collapse them onto one answer.
#[test]
fn the_int_cast_boundary_is_the_ordinary_cast_as_signed() {
    for (expr, want) in [
        ("round(3.14,'abc')", "DEC:3"),
        ("round(3.14,2.5)", "DEC:3.140"),
        ("round(3.14,2.4)", "DEC:3.14"),
        // A leading-run parse, not a full parse: `'2x'` is `2`.
        ("round(3.14159,'2x')", "DEC:3.14"),
    ] {
        let (row, chunk) = both(expr);
        assert_eq!(row, want, "{expr}");
        assert_eq!(chunk, want, "{expr} (chunk tier)");
    }
}

/// The NEGATIVE boundary of the same cast, which is the one place a wrong
/// sign is silent rather than loud: `TRUNCATE`'s scale is a SHIFT, so a
/// string `'-1'` that reached the signature uncast would read as scale `0`
/// and leave the value alone. Captured: `truncate(1234.5678,'-2')` is `1200`
/// and `round(1234.5678,'-2')` is `1200`.
#[test]
fn a_negative_string_scale_still_shifts_left() {
    for (expr, want) in [
        ("truncate(1234.5678,'-2')", "DEC:1200"),
        ("round(1234.5678,'-2')", "DEC:1200"),
    ] {
        let (row, chunk) = both(expr);
        assert_eq!(row, want, "{expr}");
        assert_eq!(chunk, want, "{expr} (chunk tier)");
    }
}

/// The HYBRID short-circuit, at the layer's own boundary because no constant
/// expression can carry an ENUM or a SET. `castAsIntFunctionClass` opens with
/// `if args[0].GetType(ctx).Hybrid() || IsBinaryLiteral(args[0]) { sig =
/// &builtinCastIntAsIntSig{bf} }` (`builtin_cast.go:146-147`), so all four
/// hybrids reach the signature as their ORDINAL or BIT integer rather than as
/// a parse of their displayed text.
///
/// Captured from real TiDB over `h(e enum('x','y','z'), s set('a','b','c'),
/// b bit(8))` holding `('y','a,c',b'00000011')`: `make_set(e,'p','q','r')` is
/// `q` (ordinal 2), `make_set(s,'p','q','r')` is `p,r` (bits 5),
/// `make_set(b,'p','q','r')` is `p,q` (bits 3) and
/// `make_set(b'00000011','p','q','r')` is `p,q` as well.
///
/// The SET row is the one that must be measured rather than reasoned: Go
/// gives `mysql.TypeEnum` the `EnumSetAsIntFlag` and `mysql.TypeSet` NOTHING,
/// so a reading that stopped at `WrapWithCastAsInt`'s own body would send the
/// SET through a STRING cast of `a,c` and answer `0`.
#[test]
fn every_hybrid_etint_argument_reads_as_its_ordinal() {
    let cases = [
        (Datum::Enum(MysqlEnum::new("y", 2), Collation::Utf8Mb4Bin), 2),
        (Datum::Set(MysqlSet::new("a,c", 5), Collation::Utf8Mb4Bin), 5),
        (Datum::Bit(BinaryLiteral::from_uint(3, None)), 3),
        (Datum::BinaryLiteral(BinaryLiteral::from_uint(3, None)), 3),
    ];
    for (value, want) in cases {
        let out = wrap_int_args("MAKE_SET", vec![value.clone()], &[None], &NoColumns).unwrap();
        assert_eq!(out[0], Datum::Int(want), "{value:?}");
    }
}

/// `OCT` is the measured NON-member: its `types.ETInt` `argTps` entry is
/// chosen by the argument's own type, so what varies is the SIGNATURE and the
/// two signatures disagree about the same hybrid value. Captured over the
/// same table: `oct(e)` is `0` and `oct(s)` is `0` -- `builtinOctStringSig`
/// parsing the text `y` and `a,c` -- while `oct(b)` is `3` and
/// `oct(b'01000001')` is `101`. Routing `OCT` through the mask would give
/// the ENUM its ordinal and break the first two.
#[test]
fn oct_is_signature_selected_and_not_a_member_of_the_layer() {
    assert_eq!(crate::arg_eval_type::int_arg_mask("OCT"), 0);

    let oct = |value: Datum| crate::string_fn::oct(&[value]).unwrap().label();
    assert_eq!(
        oct(Datum::Enum(MysqlEnum::new("y", 2), Collation::Utf8Mb4Bin)),
        "STR:0"
    );
    assert_eq!(
        oct(Datum::Set(MysqlSet::new("a,c", 5), Collation::Utf8Mb4Bin)),
        "STR:0"
    );
    assert_eq!(oct(Datum::Bit(BinaryLiteral::from_uint(3, None))), "STR:3");
    // `b'01000001'` is the byte `A`; the INTEGER signature renders 65 as
    // octal `101`, while a text parse of `A` would be `0`.
    assert_eq!(
        oct(Datum::BinaryLiteral(BinaryLiteral::from_uint(65, None))),
        "STR:101"
    );
    // The string signature keeps its own contract for every non-hybrid,
    // non-integer source: captured `oct('A')` is `0` and `oct(1.2345)` is `1`.
    assert_eq!(oct(Datum::new_string("A".to_string())), "STR:0");
    assert_eq!(e("oct(1.2345)"), "STR:1");
    // The two BOUNDARIES that separate `builtinOctStringSig` from the
    // `types.ETInt` cast by VALUE, not just by classification -- routing
    // `OCT` through the mask would move both. Captured: `oct('')` is NULL
    // where `CAST('' AS SIGNED)` is `0`, and `oct('18446744073709551616')`
    // is `1777777777777777777777` (`strconv.ParseUint`'s `ErrRange` pinned
    // at `MaxUint64`) where `CAST(... AS SIGNED)` saturates at `i64::MAX`,
    // i.e. `777777777777777777777`.
    assert_eq!(e("oct('')"), "NULL");
    assert_eq!(
        e("oct('18446744073709551616')"),
        "STR:1777777777777777777777"
    );
}

