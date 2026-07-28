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

//! Shared expression-test helpers and non-math families. Math-source tests live
//! in `math.rs`, so extending `builtin_math.go` coverage does not touch this root.

use super::*;
use tidb_ast::{QueryStmt, SelectField, Stmt};

mod compare;
mod control;
mod math;

/// Parses and evaluates a constant expression to its label.
pub(super) fn e(expr: &str) -> String {
    let stmt = tidb_parser::parse(&format!("select {expr}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(s) = query.into_inner() else {
        panic!("not select")
    };
    match &s.fields[0] {
        SelectField::Expr { expr, .. } => match eval(expr) {
            Ok(v) => v.label(),
            Err(err) => format!("{err:?}"),
        },
        _ => panic!("no expr"),
    }
}

/// Parses and evaluates a constant expression to its raw `Datum`.
pub(super) fn v(expr: &str) -> Datum {
    let stmt = tidb_parser::parse(&format!("select {expr}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("not query")
    };
    let QueryStmt::Select(s) = query.into_inner() else {
        panic!("not select")
    };
    match &s.fields[0] {
        SelectField::Expr { expr, .. } => eval(expr).expect("eval"),
        _ => panic!("no expr"),
    }
}

#[test]
fn avg() {
    // AVG grows the sum's scale by 4 (MySQL's div_precision_increment)
    // and ROUNDS to it, unlike DIV's exact truncation.
    assert_eq!(avg_of(v("6"), 3).unwrap().label(), "DEC:2.0000"); // Int sum promotes to scale 0
    assert_eq!(avg_of(v("8.00"), 3).unwrap().label(), "DEC:2.666667"); // scale 2+4=6, rounds up
    assert_eq!(avg_of(v("4.0000"), 2).unwrap().label(), "DEC:2.00000000"); // scale 4+4=8, exact
    assert!(matches!(
        avg_of(Datum::new_string("x".to_string()), 1),
        Err(EvalError::Unsupported(_))
    ));
}

/// `BIN` receives an implicit `ETInt` argument in
/// `pkg/expression/builtin_string.go`. Keep the coercion boundary explicit:
/// the Go source-table test's non-empty string, integer, float, negative and
/// NULL rows all reach this same result domain. `TestBin`'s empty direct-datum
/// row is intentionally not recorded as source coverage yet: it expects
/// `NULL`, while `builtinCastStringAsIntSig` and the SQL oracle both produce
/// zero. The local Go test cannot currently execute because the arm64 linker
/// fails before running it, so the source-table contradiction remains visible
/// instead of being hidden by a synthetic special case.
#[test]
fn bin_follows_tidb_implicit_integer_coercion() {
    for (input, want) in [
        ("'10'", "STR:1010"),
        ("'10.2'", "STR:1010"),
        ("'10aa'", "STR:1010"),
        ("'10.2aa'", "STR:1010"),
        ("'aaa'", "STR:0"),
        ("''", "STR:0"),
        ("10", "STR:1010"),
        // SQL decimal literals use TiDB's decimal-to-int half-up path.
        ("10.0", "STR:1010"),
        (
            "-1",
            "STR:1111111111111111111111111111111111111111111111111111111111111111",
        ),
        (
            "'-1'",
            "STR:1111111111111111111111111111111111111111111111111111111111111111",
        ),
        ("null", "NULL"),
    ] {
        assert_eq!(e(&format!("bin({input})")), want, "BIN({input})");
    }

    // A Go `float64` table datum selects `builtinCastRealAsIntSig`, whose
    // ties-to-even rule is distinct from decimal's half-up rule.
    assert_eq!(
        string_fn::bin(&[Datum::Real(10.0)]).unwrap(),
        Datum::new_string("1010".to_string())
    );
    assert_eq!(
        string_fn::bin(&[Datum::Real(10.5)]).unwrap(),
        Datum::new_string("1010".to_string())
    );
    assert_eq!(
        string_fn::bin(&[Datum::new_bytes(b"10".to_vec())]).unwrap(),
        Datum::new_string("1010".to_string())
    );
    assert_eq!(
        string_fn::bin(&[Datum::new_bytes(vec![0xff])]).unwrap(),
        Datum::new_string("0".to_string())
    );
    assert_eq!(
        string_fn::bin(&[]),
        Err(EvalError::Unsupported("bad BIN arity"))
    );
}

/// Complete representable rows from `TestOrd` in
/// `pkg/expression/builtin_string_test.go`.  Text values fold the first
/// UTF-8 character's bytes, while binary values use only their first raw byte;
/// session charset conversion (the GBK rows in the source table) remains an
/// explicit partial boundary.
#[test]
fn ord_source_vectors_preserve_utf8_and_binary_bytes() {
    for (expr, want) in [
        ("ord('2')", "INT:50"),
        ("ord(2)", "INT:50"),
        ("ord('23')", "INT:50"),
        ("ord(23)", "INT:50"),
        ("ord(2.3)", "INT:50"),
        ("ord(NULL)", "NULL"),
        ("ord('')", "INT:0"),
        ("ord('你好')", "INT:14990752"),
        ("ord('にほん')", "INT:14909867"),
        ("ord('한국')", "INT:15570332"),
        ("ord('👍')", "INT:4036989325"),
        ("ord('א')", "INT:55184"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
    assert_eq!(
        string_fn::ord(&[Datum::new_bytes(vec![0xe4, 0xbd, 0xa0])]).unwrap(),
        Datum::Int(0xe4)
    );
    assert_eq!(
        string_fn::ord(&[Datum::new_collation_string(
            vec![0xe4, 0xbd, 0xa0],
            tidb_datatype::Collation::Binary,
        )])
        .unwrap(),
        Datum::Int(0xe4)
    );
    assert_eq!(
        string_fn::ord(&[Datum::new_bytes(vec![0xff])]).unwrap(),
        Datum::Int(0xff)
    );
    assert_eq!(
        string_fn::ord(&[]),
        Err(EvalError::Unsupported("bad ORD arity"))
    );
}

/// Source scalar rows from `pkg/expression/builtin_string_test.go:1436
/// TestHexFunc`. `HEX` has two normal signatures: numbers use ETInt
/// conversion, while strings and hex literals preserve bytes. Session charset
/// conversion and the source's injected-error datum remain outside this
/// value-only evaluator.
#[test]
fn hex_source_vectors_preserve_numeric_and_byte_signatures() {
    for (input, want) in [
        ("'abc'", "STR:616263"),
        ("'你好'", "STR:E4BDA0E5A5BD"),
        ("12", "STR:C"),
        // Go's source table uses untyped Go floats. Scientific SQL literals
        // select this seed's matching Float value domain rather than Decimal.
        ("12.3e0", "STR:C"),
        ("12.8e0", "STR:D"),
        ("-1", "STR:FFFFFFFFFFFFFFFF"),
        ("-12.3e0", "STR:FFFFFFFFFFFFFFF4"),
        ("-12.8e0", "STR:FFFFFFFFFFFFFFF3"),
        ("0x0c", "STR:0C"),
        ("0x12", "STR:12"),
        ("null", "NULL"),
        ("'🀁'", "STR:F09F8081"),
        (
            "'一忒(๑•ㅂ•)و✧'",
            "STR:E4B880E5BF9228E0B991E280A2E38582E280A229D988E29CA7",
        ),
    ] {
        assert_eq!(e(&format!("hex({input})")), want, "HEX({input})");
    }
}

/// Scalar rows from `pkg/expression/builtin_string_test.go:1508
/// TestUnhexFunc`. Go returns binary bytes even when the payload also happens
/// to be valid UTF-8; retaining `Bytes` rather than upgrading it to `String`
/// preserves that build-time type boundary.
#[test]
fn unhex_source_vectors_preserve_odd_digit_left_padding() {
    for (input, want) in [
        ("'4D7953514C'", Datum::new_bytes(b"MySQL".to_vec())),
        ("'1267'", Datum::new_bytes(b"\x12g".to_vec())),
        ("'126'", Datum::new_bytes(b"\x01&".to_vec())),
        ("''", Datum::new_bytes(Vec::new())),
        ("1267", Datum::new_bytes(b"\x12g".to_vec())),
        ("126", Datum::new_bytes(b"\x01&".to_vec())),
        ("1267.3", Datum::Null),
        ("'string'", Datum::Null),
        ("'你好'", Datum::Null),
        ("null", Datum::Null),
    ] {
        assert_eq!(v(&format!("unhex({input})")), want, "UNHEX({input})");
    }

    assert_eq!(v("unhex('FF00')"), Datum::new_bytes(vec![0xff, 0]));
    // Go's ETString evaluator accepts arbitrary bytes.  Invalid UTF-8 is
    // therefore an invalid hex payload and returns NULL, rather than leaking
    // a Rust decoding error before `hex.DecodeString` gets to decide.
    assert_eq!(
        string_fn::unhex(&[Datum::new_bytes(vec![0xff])]),
        Ok(Datum::Null)
    );
}

/// Default-charset rows from `pkg/expression/builtin_string_test.go:1548
/// TestBitLength`. The current UTF-8 String domain deliberately does not
/// pretend to model the source table's GBK connection-charset conversion.
#[test]
fn bit_length_source_vectors_preserve_utf8_byte_count() {
    for (input, want) in [
        ("'hi'", "INT:16"),
        ("'你好'", "INT:48"),
        ("''", "INT:0"),
        ("'一二三'", "INT:72"),
        ("'一二三!'", "INT:80"),
    ] {
        assert_eq!(
            e(&format!("bit_length({input})")),
            want,
            "BIT_LENGTH({input})"
        );
    }

    // Go's len(val) counts binary bytes without UTF-8 validation.
    assert_eq!(
        string_fn::bit_length(&[Datum::new_bytes(vec![0xff, 0x00])]).unwrap(),
        Datum::Int(16)
    );
}

/// Full UTF-8-value-domain vector from
/// `pkg/expression/builtin_string_test.go:2071 TestOct`.  The three source
/// binary-literal rows deliberately remain outside this table: a bit literal
/// selects Go's ETInt signature from its original AST shape, while this seed
/// has no byte-string/AST-context value domain for `b'11111111'`.
#[test]
fn oct_source_vectors_preserve_distinct_string_and_integer_signatures() {
    for (expr, want) in [
        ("oct('-2.7')", "STR:1777777777777777777776"),
        ("oct(-1.5e0)", "STR:1777777777777777777777"),
        ("oct(-1)", "STR:1777777777777777777777"),
        ("oct('0')", "STR:0"),
        ("oct('1')", "STR:1"),
        ("oct('8')", "STR:10"),
        ("oct('12')", "STR:14"),
        ("oct('20')", "STR:24"),
        ("oct('100')", "STR:144"),
        ("oct('1024')", "STR:2000"),
        ("oct('2048')", "STR:4000"),
        ("oct(1.0e0)", "STR:1"),
        ("oct(9.5e0)", "STR:11"),
        ("oct(13)", "STR:15"),
        ("oct(1025)", "STR:2001"),
        ("oct('8a8')", "STR:10"),
        ("oct('abc')", "STR:0"),
        (
            "oct('9999999999999999999999999')",
            "STR:1777777777777777777777",
        ),
        (
            "oct('-9999999999999999999999999')",
            "STR:1777777777777777777777",
        ),
        ("oct(null)", "NULL"),
        // Regression from builtinOctStringSig / issue #59446, also present
        // in tests/integrationtest/t/expression/builtin.test.
        ("oct('')", "NULL"),
        // The same upstream fixture keeps this distinct from an empty input:
        // whitespace makes a nonempty value whose numeric prefix is empty.
        ("oct(' ')", "STR:0"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }

    // `builtinOctStringSig` also receives binary values produced by another
    // ETString function.  Go's byte-prefix parser sees an invalid leading
    // byte as an empty numeric prefix and returns zero; it does not reject the
    // value merely because the byte sequence is not UTF-8.
    assert_eq!(e("oct(unhex('FF'))"), "STR:0");
    assert_eq!(
        string_fn::oct(&[Datum::new_bytes(vec![0xff])]),
        Ok(Datum::new_string("0".to_string()))
    );
}

#[test]
fn any_value_source_vectors_preserve_value_labels() {
    // pkg/expression/builtin_miscellaneous_test.go:240 TestAnyValue
    assert_eq!(e("any_value(null)"), "NULL");
    assert_eq!(e("any_value(1234)"), "INT:1234");
    assert_eq!(e("any_value(-153)"), "INT:-153");
    assert_eq!(e("any_value(cast(3.1415926 as double))"), "FLOAT:3.1415926");
    assert_eq!(e("any_value('Hello, World')"), "STR:Hello, World");
}

#[test]
fn unary_minus_source_vectors_preserve_uint_overflow_domain() {
    // pkg/expression/builtin_op_test.go:30 TestUnary
    assert_eq!(e("-9223372036854775809"), "DEC:-9223372036854775809");
    assert_eq!(e("-9223372036854775810"), "DEC:-9223372036854775810");
    assert_eq!(e("-9223372036854775808"), "INT:-9223372036854775808");
    assert_eq!(e("-(-9223372036854775808)"), "DEC:9223372036854775808");
}

#[test]
fn like_source_vectors_preserve_default_escape_semantics() {
    // pkg/expression/builtin_like_test.go:30 TestLike
    for (expr, want) in [
        ("'a' like ''", "INT:0"),
        ("'a' like 'a'", "INT:1"),
        ("'a' like 'b'", "INT:0"),
        ("'aA' like 'Aa'", "INT:0"),
        ("'aAb' like 'Aa%'", "INT:0"),
        ("'aAb' like 'aA_'", "INT:1"),
        ("'baab' like 'b_%b'", "INT:1"),
        ("'baab' like 'b%_b'", "INT:1"),
        ("'bab' like 'b_%b'", "INT:1"),
        ("'bab' like 'b%_b'", "INT:1"),
        ("'bb' like 'b_%b'", "INT:0"),
        ("'bb' like 'b%_b'", "INT:0"),
        ("'baabccc' like 'b_%b%'", "INT:1"),
        ("'a' like '\\\\a'", "INT:1"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

#[test]
fn reverse_source_vectors_preserve_scalar_string_coercion() {
    // pkg/expression/builtin_string_test.go:689 TestReverse
    assert_eq!(e("reverse(null)"), "NULL");
    assert_eq!(e("reverse('abc')"), "STR:cba");
    assert_eq!(e("reverse('LIKE')"), "STR:EKIL");
    assert_eq!(e("reverse(123)"), "STR:321");
    assert_eq!(e("reverse('')"), "STR:");
}

/// Default-charset scalar rows from
/// `pkg/expression/builtin_string_test.go:107 TestASCII`. `ASCII` consumes
/// the first encoded byte, so the UTF-8 input intentionally returns `228`
/// (the first byte of `你`) rather than its Unicode scalar value. Connection
/// charset conversion and the source's injected-error datum require runtime
/// state outside this value-only evaluator; their omission is recorded in the
/// partial ledger evidence, not disguised by a Rust-only rule.
#[test]
fn ascii_source_vectors_preserve_first_byte_and_string_coercion() {
    for (expr, want) in [
        ("ascii('2')", "INT:50"),
        ("ascii(2)", "INT:50"),
        ("ascii('23')", "INT:50"),
        ("ascii(23)", "INT:50"),
        ("ascii(2.3)", "INT:50"),
        ("ascii(null)", "NULL"),
        ("ascii('')", "INT:0"),
        ("ascii('你好')", "INT:228"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }

    // Go's EvalString also accepts binary values. Keep this raw-byte case
    // separate from the SQL source table because the parser does not expose
    // an invalid-UTF-8 string literal in this seed value domain.
    assert_eq!(
        string_fn::ascii(&[Datum::new_bytes(vec![0xff, 0x00])]).unwrap(),
        Datum::Int(255)
    );
}

/// Currently representable scalar rows from
/// `pkg/expression/builtin_string_test.go::TestLengthAndOctetLength`. The
/// source runs the identical table through both function names.
#[test]
fn length_and_octet_length_source_vectors_count_evaluated_bytes() {
    for function in ["length", "octet_length"] {
        for (argument, want) in [
            ("'abc'", "INT:3"),
            ("'你好'", "INT:6"),
            ("1", "INT:1"),
            ("3.14", "INT:4"),
            ("123.123", "INT:7"),
            ("0x01", "INT:1"),
            ("null", "NULL"),
        ] {
            let expression = format!("{function}({argument})");
            assert_eq!(e(&expression), want, "{expression}");
        }

        // A binary cast can retain an incomplete UTF-8 suffix.  LENGTH and
        // OCTET_LENGTH must count the raw bytes selected by Go's
        // `builtinLengthSig`, rather than trying to decode or count runes.
        let expression = format!("{function}(cast('你好world' as binary(5)))");
        assert_eq!(e(&expression), "INT:5", "{expression}");
    }
}

/// Default-charset scalar rows from
/// `pkg/expression/builtin_string_test.go:1635 TestCharLength`. Go selects
/// `builtinCharLengthUTF8Sig` for these non-binary arguments and counts runes
/// after ETString coercion. The source's second loop explicitly mutates the
/// argument FieldType to binary, selecting `builtinCharLengthBinarySig`; that
/// build-time distinction is covered by `build::tests`. This context-free AST
/// evaluator has no general type-inference pass, so it deliberately constructs
/// the deterministic default character signature instead.
#[test]
fn char_length_source_vectors_preserve_utf8_rune_count_and_coercion() {
    for (expr, want) in [
        ("char_length('33')", "INT:2"),
        ("char_length('你好')", "INT:2"),
        ("char_length(33)", "INT:2"),
        ("char_length(3.14)", "INT:4"),
        ("char_length(null)", "NULL"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

/// Public `eval` regression for Go's build-time `IsBinaryStr` selection.
/// These expected values were checked through the real `gorun` query path;
/// notably the identical UTF-8 bytes count as three for every binary source
/// form and one after an explicit character cast.
#[test]
fn char_length_public_eval_uses_source_field_type() {
    for (expression, want) in [
        ("char_length('你')", "INT:1"),
        ("char_length(0xE4BDA0)", "INT:3"),
        ("char_length(b'111001001011110110100000')", "INT:3"),
        ("char_length(cast('你' as binary))", "INT:3"),
        ("char_length(unhex('E4BDA0'))", "INT:3"),
        ("char_length(char(228,189,160))", "INT:3"),
        ("char_length(from_base64('5L2g'))", "INT:3"),
        ("char_length(0xF0288C28)", "INT:4"),
        ("char_length(unhex('F0288C28'))", "INT:4"),
        ("char_length(cast(0xE4BDA0 as char))", "INT:1"),
        ("char_length(elt(1, 0xE4BDA0, 'x'))", "INT:3"),
        ("character_length((0xE4BDA0))", "INT:3"),
    ] {
        assert_eq!(e(expression), want, "{expression}");
    }
}

#[test]
fn char_length_rejects_unresolved_field_type_before_runtime_datum() {
    struct RuntimeBytes;

    impl Columns for RuntimeBytes {
        fn get(&self, _: &[String]) -> Option<Datum> {
            Some(Datum::new_bytes("你".as_bytes().to_vec()))
        }
    }

    let expression = Expr::Func {
        name: "CHAR_LENGTH".to_string(),
        args: vec![Expr::Column(vec!["binary_col".to_string()])],
        origin_position: 0,
    };
    assert_eq!(
        eval_in(&expression, &RuntimeBytes),
        Err(EvalError::Unsupported(
            "unresolved CHAR_LENGTH argument FieldType"
        ))
    );
}

#[test]
fn elt_source_vectors_preserve_selector_and_result_coercion() {
    // pkg/expression/builtin_string_test.go:2443 TestElt
    assert_eq!(e("elt(1, 'Hej', 'ej', 'Heja', 'hej', 'foo')"), "STR:Hej");
    assert_eq!(e("elt(9, 'Hej', 'ej', 'Heja', 'hej', 'foo')"), "NULL");
    assert_eq!(
        e("elt(-1, 'Hej', 'ej', 'Heja', 'ej', 'hej', 'foo')"),
        "NULL"
    );
    assert_eq!(e("elt(0, 2, 3, 11, 1)"), "NULL");
    assert_eq!(e("elt(3, 2, 3, 11, 1)"), "STR:11");
    assert_eq!(e("elt(1.1e0, '2.1', '3.1', '11.1', '1.1')"), "STR:2.1");
}

#[test]
fn quote_source_vectors_preserve_byte_exact_escaping() {
    // pkg/expression/builtin_string_test.go:2528 TestQuote
    for (expr, want) in [
        ("quote(x'446f6e5c277421')", "STR:'Don\\\\\\'t!'"),
        ("quote(x'446f6e2774')", "STR:'Don\\'t'"),
        ("quote(x'446f6e22')", "STR:'Don\"'"),
        ("quote(x'446f6e5c22')", "STR:'Don\\\\\"'"),
        ("quote(x'5c27')", "STR:'\\\\\\''"),
        ("quote(x'5c22')", "STR:'\\\\\"'"),
        ("quote('萌萌哒(๑•ᴗ•๑)😊')", "STR:'萌萌哒(๑•ᴗ•๑)😊'"),
        ("quote('㍿㌍㍑㌫')", "STR:'㍿㌍㍑㌫'"),
        ("quote(x'001a')", "STR:'\\0\\Z'"),
        ("quote(null)", "STR:NULL"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

#[test]
fn make_set_source_vectors_preserve_signed_bit_masks() {
    // pkg/expression/builtin_string_test.go:2045 TestMakeSet
    assert_eq!(e("make_set(1, 'a', 'b', 'c')"), "STR:a");
    assert_eq!(
        e("make_set(5, 'hello', 'nice', 'world')"),
        "STR:hello,world"
    );
    assert_eq!(
        e("make_set(5, 'hello', 'nice', null, 'world')"),
        "STR:hello"
    );
    assert_eq!(e("make_set(0, 'a', 'b', 'c')"), "STR:");
    assert_eq!(e("make_set(null, 'a', 'b', 'c')"), "NULL");
    assert_eq!(
        e("make_set(-100, 'hello', 'nice', 'abc', 'world')"),
        "STR:abc,world"
    );
    assert_eq!(
        e("make_set(-1, 'hello', 'nice', 'abc', 'world')"),
        "STR:hello,nice,abc,world"
    );
}

/// Regression: `bits` evaluating to the UNSIGNED domain -- a bitwise OR's
/// result, confirmed via `gorun` (`MAKE_SET(1|4,'a','b','c')` is `'a,c'`) --
/// used to fall through `make_set`'s `Datum::Int`-only match and answer
/// `NULL` instead of reading the same bit pattern. More set bits than
/// strings (`31` against 3 arguments) simply has nothing to match past the
/// last one, confirmed via the same run.
#[test]
fn make_set_reads_unsigned_bits_too() {
    assert_eq!(e("make_set(1|4, 'a', 'b', 'c')"), "STR:a,c");
    assert_eq!(e("make_set(31, 'a', 'b', 'c')"), "STR:a,b,c");
}

#[test]
fn field_source_vectors_preserve_numeric_and_string_comparison_modes() {
    // pkg/expression/builtin_string_test.go:1712 TestField
    for (expr, want) in [
        ("field('ej', 'Hej', 'ej', 'Heja', 'hej', 'foo')", "INT:2"),
        ("field('fo', 'Hej', 'ej', 'Heja', 'hej', 'foo')", "INT:0"),
        (
            "field('ej', 'Hej', 'ej', 'Heja', 'ej', 'hej', 'foo')",
            "INT:2",
        ),
        ("field(1, 2, 3, 11, 1)", "INT:4"),
        ("field(null, 2, 3, 11, 1)", "INT:0"),
        ("field(1.1e0, 2.1e0, 3.1e0, 11.1e0, 1.1e0)", "INT:4"),
        ("field(1.1e0, '2.1', '3.1', '11.1', '1.1')", "INT:4"),
        ("field('1.1a', 2.1e0, 3.1e0, 11.1e0, 1.1e0)", "INT:4"),
        ("field(1.10, 0, 11e-1)", "INT:2"),
        ("field('abc', 0, 1, 11.1e0, 1.1e0)", "INT:1"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

#[test]
fn field_mixed_arguments_select_one_real_signature() {
    // pkg/expression/builtin_string.go:2822 fieldFunctionClass selects
    // builtinFieldRealSig for a list containing both strings and integers.
    // Every argument is therefore compared through EvalReal: '1' and '01'
    // are equal numerically, even though they are different text values.
    assert_eq!(e("field('1', '01', 1)"), "INT:1");
    assert_eq!(e("field('1', '1x', 1)"), "INT:1");
}

#[test]
fn pad_source_vectors_preserve_unicode_empty_and_overflow_rules() {
    // pkg/expression/builtin_string_test.go:1747 TestLpad and :1789 TestRpad
    for (expr, want) in [
        ("lpad('hi', 5, '?')", "STR:???hi"),
        ("lpad('hi', 1, '?')", "STR:h"),
        ("lpad('hi', 0, '?')", "STR:"),
        ("lpad('hi', -1, '?')", "NULL"),
        ("lpad('hi', 1, '')", "STR:h"),
        ("lpad('hi', 5, '')", "STR:"),
        ("lpad('hi', 5, 'ab')", "STR:abahi"),
        ("lpad('hi', 6, 'ab')", "STR:ababhi"),
        ("lpad('中文', 5, '字符')", "STR:字符字中文"),
        ("lpad('中文', 1, 'a')", "STR:中"),
        ("lpad('中文', -5, '字符')", "NULL"),
        ("lpad('中文', 10, '')", "STR:"),
        ("lpad('1', 4611686018427387904, '1')", "NULL"),
        ("rpad('hi', 5, '?')", "STR:hi???"),
        ("rpad('hi', 1, '?')", "STR:h"),
        ("rpad('hi', 0, '?')", "STR:"),
        ("rpad('hi', -1, '?')", "NULL"),
        ("rpad('hi', 1, '')", "STR:h"),
        ("rpad('hi', 5, '')", "STR:"),
        ("rpad('hi', 5, 'ab')", "STR:hiaba"),
        ("rpad('hi', 6, 'ab')", "STR:hiabab"),
        ("rpad('中文', 5, '字符')", "STR:中文字符字"),
        ("rpad('中文', 1, 'a')", "STR:中"),
        ("rpad('中文', -5, '字符')", "NULL"),
        ("rpad('中文', 10, '')", "STR:"),
        ("rpad('1', 4611686018427387904, '1')", "NULL"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

#[test]
fn right_and_rpad_sig_source_vectors_preserve_scalar_boundaries() {
    // `TestStringRight` (pkg/expression/builtin_string_test.go:2719) is the
    // character-valued RIGHT path; the binary assertion below keeps its
    // separate byte signature visible without pretending a Go chunk column
    // or session field type exists in this evaluator.
    for (expr, want) in [
        ("right('helloworld', 5)", "STR:world"),
        ("right('helloworld', 10)", "STR:helloworld"),
        ("right('helloworld', 11)", "STR:helloworld"),
        ("right('helloworld', -1)", "STR:"),
        ("right('', 2)", "STR:"),
        ("right(NULL, 2)", "NULL"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
    assert_eq!(v("right(unhex('6162FF'), 1)"), Datum::new_bytes(vec![0xff]));
    assert_eq!(
        string_fn::str_take(
            &[
                Datum::new_collation_string(
                    vec![0x61, 0x62, 0xff],
                    tidb_datatype::Collation::Binary,
                ),
                Datum::Int(1),
            ],
            false,
        )
        .unwrap(),
        Datum::new_bytes(vec![0xff])
    );

    // The scalar value row in `TestRpadSig` (source line 1831) is the same
    // rune-based path as the existing RPAD table; the warning-producing
    // max-packet/vectorized row remains an explicit partial boundary.
    assert_eq!(e("rpad('abc', 6, '123')"), "STR:abc123");
    assert_eq!(e("rpad(NULL, 6, '123')"), "NULL");
    assert_eq!(e("rpad('abc', 6, NULL)"), "NULL");
    assert_eq!(
        string_fn::pad(
            &[
                Datum::new_bytes(b"ab".to_vec()),
                Datum::Int(3),
                Datum::new_bytes(vec![0xff]),
            ],
            false,
        )
        .unwrap(),
        Datum::new_bytes(vec![b'a', b'b', 0xff])
    );
    assert_eq!(
        string_fn::pad(&[], false),
        Err(EvalError::Unsupported("bad LPAD/RPAD arguments"))
    );
}

#[test]
fn repeat_source_vectors_preserve_uint_count_and_packet_boundary() {
    // pkg/expression/builtin_string_test.go:495 TestRepeat
    assert_eq!(e("repeat('a', 2)"), "STR:aa");
    for (expr, expected_len) in [
        ("repeat('a', 16777217)", 16_777_217usize),
        ("repeat('a', 16777216)", 16_777_216usize),
    ] {
        match v(expr) {
            Datum::String(value) => {
                assert_eq!(value.bytes().len(), expected_len, "{expr}");
                assert_eq!(value.bytes().first(), Some(&b'a'));
                assert_eq!(value.bytes().last(), Some(&b'a'));
            }
            other => panic!("{expr} returned {other:?}"),
        }
    }
    assert_eq!(e("repeat('a', -1)"), "STR:");
    assert_eq!(e("repeat('a', 0)"), "STR:");
    assert_eq!(e("repeat('a', cast(0 as unsigned))"), "STR:");
    assert_eq!(e("repeat(null, 2)"), "NULL");
    assert_eq!(e("repeat('a', null)"), "NULL");

    // `TestRepeatSig` constructs a custom 1000-byte max_allowed_packet and
    // checks warning counts.  The value-only evaluator has no warning/session
    // channel; its default 64 MiB boundary still preserves the representable
    // positive and Unicode repeat rows from that source table.
    assert_eq!(e("repeat('a', 6)"), "STR:aaaaaa");
    assert_eq!(v("repeat('毅', 6)").label(), "STR:毅毅毅毅毅毅");
    assert_eq!(
        v("repeat('毅', 334)").label().len(),
        "STR:".len() + 334 * "毅".len()
    );
    assert_eq!(v("repeat('a', 2147483647)"), Datum::Null);
}

#[test]
fn arithmetic() {
    assert_eq!(e("1 + 2 * 3"), "INT:7");
    assert_eq!(e("(1 + 2) * 3"), "INT:9");
    assert_eq!(e("7 DIV 2"), "INT:3");
    assert_eq!(e("7 MOD 3"), "INT:1");
    assert_eq!(e("- -5"), "INT:5");
    assert_eq!(e("~0"), "UINT:18446744073709551615");
}

#[test]
fn comparisons() {
    assert_eq!(e("1 = 1"), "INT:1");
    assert_eq!(e("2 < 1"), "INT:0");
    assert_eq!(e("1 <> 2"), "INT:1");
    assert_eq!(e("5 <=> 5"), "INT:1");
    // `types.CompareInt` retains both signedness bits: negative signed
    // values sort below every UInt, while same raw bits are not equal.
    assert_eq!(e("18446744073709551615 > -1"), "INT:1");
    assert_eq!(e("18446744073709551615 = -1"), "INT:0");
}

#[test]
fn builtin_functions() {
    assert_eq!(e("abs(-5)"), "INT:5");
    assert_eq!(e("sign(-3)"), "INT:-1");
    assert_eq!(e("least(3, 1, 2)"), "INT:1");
    assert_eq!(e("greatest(1, 2, 3)"), "INT:3");
    assert_eq!(e("least(5, 3, NULL, 1)"), "NULL");
    assert_eq!(e("coalesce(NULL, NULL, 7)"), "INT:7");
    assert_eq!(e("if(0, 10, 20)"), "INT:20");
    assert_eq!(e("if(NULL, 10, 20)"), "INT:20");
    assert_eq!(e("ifnull(NULL, 5)"), "INT:5");
    assert_eq!(e("nullif(3, 3)"), "NULL");
    assert_eq!(e("nullif(3, 4)"), "INT:3");
    // Nested calls fold too.
    assert_eq!(e("greatest(abs(-1), sign(-4), 0)"), "INT:1");
}

#[test]
fn string_functions() {
    assert_eq!(e("'hello'"), "STR:hello");
    assert_eq!(e("concat('a', 'b', 'c')"), "STR:abc");
    assert_eq!(e("concat('x', NULL)"), "NULL");
    assert_eq!(e("length('héllo')"), "INT:6"); // bytes
    assert_eq!(e("length(unhex('FF00'))"), "INT:2"); // arbitrary bytes
    assert_eq!(e("octet_length(unhex('FF00'))"), "INT:2");
    assert_eq!(e("char_length('héllo')"), "INT:5"); // chars
    assert_eq!(e("upper('abc')"), "STR:ABC");
    assert_eq!(e("left('hello', 3)"), "STR:hel");
    assert_eq!(e("right('hello', 2)"), "STR:lo");
    assert_eq!(e("substring('hello', 2, 3)"), "STR:ell");
    assert_eq!(e("concat('n=', 5)"), "STR:n=5"); // int coerced
    assert_eq!(e("if(1, 'yes', 'no')"), "STR:yes");
}

/// Complete representable rows from `TestLower` and `TestUpper` in
/// `pkg/expression/builtin_string_test.go`.  The seed evaluator has the
/// default UTF-8 text signature and an explicit binary signature: session
/// charset conversion (including GBK) is a separate metadata boundary, while
/// raw binary bytes must remain unchanged rather than being UTF-8-decoded.
#[test]
fn lower_upper_source_vectors_preserve_case_and_binary_boundaries() {
    for (expr, want) in [
        ("lower(NULL)", "NULL"),
        ("lower('ab')", "STR:ab"),
        ("lower(1)", "STR:1"),
        ("lower('one week’s time TEST')", "STR:one week’s time test"),
        (
            "lower(\"one week's time TEST\")",
            "STR:one week's time test",
        ),
        ("lower('ABC测试DEF')", "STR:abc测试def"),
        ("lower('ABCテストDEF')", "STR:abcテストdef"),
        ("upper(NULL)", "NULL"),
        ("upper('ab')", "STR:AB"),
        ("upper(1)", "STR:1"),
        ("upper('one week’s time TEST')", "STR:ONE WEEK’S TIME TEST"),
        (
            "upper(\"one week's time TEST\")",
            "STR:ONE WEEK'S TIME TEST",
        ),
        ("upper('abc测试def')", "STR:ABC测试DEF"),
        ("upper('abcテストdef')", "STR:ABCテストDEF"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
    assert_eq!(
        string_fn::case_convert(&[Datum::new_bytes(vec![b'A', 0xff])], false).unwrap(),
        Datum::new_bytes(vec![b'A', 0xff])
    );
    assert_eq!(
        string_fn::case_convert(&[Datum::new_bytes(vec![b'a', 0xff])], true).unwrap(),
        Datum::new_bytes(vec![b'a', 0xff])
    );
    assert!(string_fn::case_convert(&[], false).is_err());
}

/// Complete representable rows from `TestStrcmp` in
/// `pkg/expression/builtin_string_test.go`.  The Go source uses the selected
/// string collation; this seed keeps the default byte-wise comparison while
/// preserving ETString numeric coercion and arbitrary binary input.  Session
/// collation changes and the injected harness error remain explicit partial
/// boundaries.
#[test]
fn strcmp_source_vectors_preserve_coercion_and_nulls() {
    for (expr, want) in [
        ("strcmp('123', '123')", "INT:0"),
        ("strcmp('123', '1')", "INT:1"),
        ("strcmp('1', '123')", "INT:-1"),
        ("strcmp('123', '45')", "INT:-1"),
        ("strcmp(123, '123')", "INT:0"),
        ("strcmp('12.34', 12.34)", "INT:0"),
        ("strcmp(NULL, '123')", "NULL"),
        ("strcmp('123', NULL)", "NULL"),
        ("strcmp('', '123')", "INT:-1"),
        ("strcmp('123', '')", "INT:1"),
        ("strcmp('', '')", "INT:0"),
        ("strcmp('', NULL)", "NULL"),
        ("strcmp(NULL, '')", "NULL"),
        ("strcmp(NULL, NULL)", "NULL"),
        ("strcmp('123 ', '123')", "INT:0"),
        ("strcmp(123, '123 ')", "INT:0"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
    assert_eq!(
        string_fn::strcmp(&[Datum::new_bytes(vec![0xff]), Datum::new_bytes(vec![0x00]),]).unwrap(),
        Datum::Int(1)
    );
    assert_eq!(
        string_fn::strcmp(&[
            Datum::new_bytes(b"a".to_vec()),
            Datum::new_bytes(b"a ".to_vec()),
        ])
        .unwrap(),
        Datum::Int(-1)
    );
    assert_eq!(
        string_fn::strcmp(&[
            Datum::new_string("a ".to_string()),
            Datum::new_bytes(b"a".to_vec()),
        ])
        .unwrap(),
        Datum::Int(1)
    );
    assert_eq!(
        string_fn::strcmp(&[Datum::new_string("a".to_string())]),
        Err(EvalError::Unsupported("bad STRCMP arity"))
    );
}

/// Complete representable rows from `TestLeft` and `TestRight` in
/// `pkg/expression/builtin_string_test.go`.  Numeric-prefix count coercion,
/// Unicode character slicing, NULL propagation, and binary byte slicing are
/// all scalar value behavior; the injected Go error datum and FieldType/
/// warning state remain outside this evaluator.
#[test]
fn left_right_source_vectors_preserve_count_and_byte_boundaries() {
    for (expr, want) in [
        ("left('abcde', 3)", "STR:abc"),
        ("left('abcde', 0)", "STR:"),
        ("left('abcde', 1.2)", "STR:a"),
        ("left('abcde', 1.9)", "STR:ab"),
        ("left('abcde', -1)", "STR:"),
        ("left('abcde', 100)", "STR:abcde"),
        ("left('abcde', NULL)", "NULL"),
        ("left(NULL, 3)", "NULL"),
        ("left('abcde', '3')", "STR:abc"),
        ("left('abcde', 'a')", "STR:"),
        ("left(1234, 3)", "STR:123"),
        ("left(12.34, 3)", "STR:12."),
        ("right('abcde', 3)", "STR:cde"),
        ("right('abcde', 0)", "STR:"),
        ("right('abcde', 1.2)", "STR:e"),
        ("right('abcde', 1.9)", "STR:de"),
        ("right('abcde', -1)", "STR:"),
        ("right('abcde', 100)", "STR:abcde"),
        ("right('abcde', NULL)", "NULL"),
        ("right(NULL, 1)", "NULL"),
        ("right('abcde', '3')", "STR:cde"),
        ("right('abcde', 'a')", "STR:"),
        ("right(1234, 3)", "STR:234"),
        ("right(12.34, 3)", "STR:.34"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
    assert_eq!(v("left(unhex('0102'), 1)"), Datum::new_bytes(vec![0x01]));
    assert_eq!(v("right(unhex('0102'), 1)"), Datum::new_bytes(vec![0x02]));
    assert_eq!(e("left('你好世界', 2)"), "STR:你好");
    assert_eq!(e("right('你好世界', 2)"), "STR:世界");
}

/// Complete representable rows from `TestReplace` in
/// `pkg/expression/builtin_string_test.go`.  REPLACE evaluates every operand
/// through Go's byte-preserving `EvalString`; the direct binary assertion
/// keeps invalid UTF-8 and embedded NULs on that same path instead of forcing
/// a Rust text decode.
#[test]
fn replace_source_vectors_preserve_byte_coercion() {
    for (expr, want) in [
        (
            "replace('www.mysql.com', 'mysql', 'pingcap')",
            "STR:www.pingcap.com",
        ),
        ("replace('www.mysql.com', 'w', 1)", "STR:111.mysql.com"),
        ("replace(1234, 2, 55)", "STR:15534"),
        ("replace('', 'a', 'b')", "STR:"),
        ("replace('abc', '', 'd')", "STR:abc"),
        ("replace('aaa', 'a', '')", "STR:"),
        ("replace(NULL, 'a', 'b')", "NULL"),
        ("replace('a', NULL, 'b')", "NULL"),
        ("replace('a', 'b', NULL)", "NULL"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
    assert_eq!(
        string_fn::replace(&[
            Datum::new_bytes(vec![0xff, 0x00, b'a']),
            Datum::new_bytes(vec![0xff]),
            Datum::new_bytes(vec![0xfe, b'b']),
        ])
        .unwrap(),
        Datum::new_bytes(vec![0xfe, b'b', 0x00, b'a'])
    );
    assert!(string_fn::replace(&[
        Datum::new_string("abc".to_string()),
        Datum::new_string("a".to_string()),
    ])
    .is_err());
}

/// Complete representable rows from `TestSubstringIndex` in
/// `pkg/expression/builtin_string_test.go`.  The source signature is
/// `ETString, ETString, ETInt`, so string/decimal counts use the shared
/// numeric-prefix/rounding coercion rather than an integer-literal-only path.
#[test]
fn substring_index_source_vectors_preserve_count_and_bytes() {
    for (expr, want) in [
        (
            "substring_index('www.pingcap.com', '.', 2)",
            "STR:www.pingcap",
        ),
        (
            "substring_index('www.pingcap.com', '.', -2)",
            "STR:pingcap.com",
        ),
        ("substring_index('www.pingcap.com', '.', 0)", "STR:"),
        (
            "substring_index('www.pingcap.com', '.', 100)",
            "STR:www.pingcap.com",
        ),
        (
            "substring_index('www.pingcap.com', '.', -100)",
            "STR:www.pingcap.com",
        ),
        ("substring_index('www.pingcap.com', 'd', 0)", "STR:"),
        (
            "substring_index('www.pingcap.com', 'd', 1)",
            "STR:www.pingcap.com",
        ),
        (
            "substring_index('www.pingcap.com', 'd', -1)",
            "STR:www.pingcap.com",
        ),
        ("substring_index('www.pingcap.com', '', 0)", "STR:"),
        ("substring_index('www.pingcap.com', '', 1)", "STR:"),
        ("substring_index('www.pingcap.com', '', -1)", "STR:"),
        ("substring_index('www.pingcap.com', '', NULL)", "NULL"),
        ("substring_index('', '.', 0)", "STR:"),
        ("substring_index('', '.', 1)", "STR:"),
        ("substring_index('', '.', -1)", "STR:"),
        ("substring_index(NULL, '.', 1)", "NULL"),
        ("substring_index('www.pingcap.com', NULL, 1)", "NULL"),
        ("substring_index('www.pingcap.com', '.', NULL)", "NULL"),
        (
            "substring_index('www.pingcap.com', '.', '2')",
            "STR:www.pingcap",
        ),
        (
            "substring_index('www.pingcap.com', '.', 2.5)",
            "STR:www.pingcap.com",
        ),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
    assert_eq!(
        string_fn::substring_index(&[
            Datum::new_bytes(b"a\xffb\xffc".to_vec()),
            Datum::new_bytes(vec![0xff]),
            Datum::Int(-2),
        ])
        .unwrap(),
        Datum::new_bytes(b"b\xffc".to_vec())
    );
    assert_eq!(
        string_fn::substring_index(&[
            Datum::new_string("a".to_string()),
            Datum::new_string(".".to_string()),
        ]),
        Err(EvalError::Unsupported("bad SUBSTRING_INDEX arity"))
    );
    assert_eq!(
        string_fn::substring_index(&[
            Datum::new_string("a.b.c".to_string()),
            Datum::new_string(".".to_string()),
            Datum::UInt(i64::MAX as u64 + 1),
        ])
        .unwrap(),
        Datum::new_string("a.b.c".to_string())
    );
    assert_eq!(
        string_fn::substring_index(&[
            Datum::new_string("a.b.c".to_string()),
            Datum::new_string(".".to_string()),
            Datum::Int(i64::MIN),
        ])
        .unwrap(),
        Datum::new_string("a.b.c".to_string())
    );
}

/// Complete scalar rows from `TestTrim`.  TRIM removes repeated whole byte
/// prefixes/suffixes, and only ASCII space is implicit for the one-argument
/// form; tabs, CR, and LF remain ordinary payload bytes exactly as in Go.
#[test]
fn trim_source_vectors_preserve_direction_and_whole_remstr() {
    use tidb_ast::TrimDirection;
    for (expr, want) in [
        ("trim('   bar   ')", "STR:bar"),
        ("trim('')", "STR:"),
        ("trim(NULL)", "NULL"),
        ("trim('x' from 'xxxbarxxx')", "STR:bar"),
        ("trim('x' from 'bar')", "STR:bar"),
        ("trim('' from '   bar   ')", "STR:   bar   "),
        ("trim('x' from '')", "STR:"),
        ("trim(NULL from 'bar')", "NULL"),
        ("trim('x' from NULL)", "NULL"),
        ("trim(leading 'x' from 'xxxbarxxx')", "STR:barxxx"),
        ("trim(trailing 'xyz' from 'barxxyz')", "STR:barx"),
        ("trim(both 'x' from 'xxxbarxxx')", "STR:bar"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
    assert_eq!(
        string_fn::trim_value(
            Some(b"\t   bar   \n".to_vec()),
            Some(b" ".to_vec()),
            TrimDirection::Both,
            false,
        ),
        Datum::new_string(b"\t   bar   \n".to_vec())
    );
    assert_eq!(
        string_fn::trim_value(
            Some(b"\r   bar   \t".to_vec()),
            Some(b" ".to_vec()),
            TrimDirection::Both,
            false,
        ),
        Datum::new_string(b"\r   bar   \t".to_vec())
    );
    assert_eq!(
        string_fn::trim_value(
            Some(b"   \tbar\n     ".to_vec()),
            Some(b" ".to_vec()),
            TrimDirection::Both,
            false,
        ),
        Datum::new_string(b"\tbar\n".to_vec())
    );
    assert_eq!(
        string_fn::trim_value(
            Some(b"xxxbarxxx".to_vec()),
            Some(b"x".to_vec()),
            TrimDirection::Leading,
            false,
        ),
        Datum::new_string("barxxx".to_string())
    );
    assert_eq!(
        string_fn::trim_value(
            Some(b"barxxyz".to_vec()),
            Some(b"xyz".to_vec()),
            TrimDirection::Trailing,
            false,
        ),
        Datum::new_string("barx".to_string())
    );
    assert_eq!(
        string_fn::trim_value(
            Some(b"\x20\xff\x20".to_vec()),
            Some(vec![0x20]),
            TrimDirection::Both,
            true,
        ),
        Datum::new_bytes(vec![0xff])
    );
    assert_eq!(
        string_fn::trim_value(Some(b"bar".to_vec()), None, TrimDirection::Both, false,),
        Datum::Null
    );
    assert_eq!(
        string_fn::trim_value(Some(b"bar".to_vec()), None, TrimDirection::Leading, false,),
        Datum::Null
    );
}

/// Scalar and binary rows from `pkg/expression/builtin_string_test.go:169
/// TestConcat`.  Go's `EvalString` boundary is byte-preserving: numeric
/// values stringify, any `NULL` propagates, and a binary/hex argument keeps
/// invalid UTF-8 octets in the result.  Date/time and injected-error rows in
/// the source table require value domains this seed evaluator does not yet
/// expose; the direct byte test keeps that boundary explicit instead of
/// decoding arbitrary bytes through UTF-8.
#[test]
fn concat_source_vectors_preserve_scalar_and_binary_coercion() {
    for (expr, want) in [
        ("concat(null)", "NULL"),
        (
            "concat('a', 'b', 1, 2, 1.1, 1.2, cast(1.1 as decimal(3, 1)))",
            "STR:ab121.11.21.1",
        ),
        ("concat('a', 'b', null, 'c')", "NULL"),
        ("concat(0xFF, 'a')", "STR_HEX:FF61"),
        ("concat('a', unhex('FF00'))", "STR_HEX:61FF00"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
    assert_eq!(
        string_fn::concat(&[Datum::new_bytes(vec![0xff]), Datum::new_string("a")]).unwrap(),
        Datum::new_string(vec![0xff, b'a'])
    );
    assert!(string_fn::concat(&[]).is_err());
}

/// Scalar and separator rows from
/// `pkg/expression/builtin_string_test.go:273 TestConcatWS`.  `NULL` as the
/// separator propagates; later `NULL` values are skipped while empty strings
/// remain real fields.  The separator and values use the same byte-preserving
/// `EvalString` coercion as CONCAT.
#[test]
fn concat_ws_source_vectors_preserve_separator_and_null_rules() {
    for (expr, want) in [
        ("concat_ws(null, null)", "NULL"),
        ("concat_ws(null, 'a', 'b')", "NULL"),
        (
            "concat_ws(',', 'a', 'b', 'hello', '$^%')",
            "STR:a,b,hello,$^%",
        ),
        ("concat_ws('|', 'a', null, 'b', 'c')", "STR:a|b|c"),
        ("concat_ws(',', 'a', ',', 'b', 'c')", "STR:a,,,b,c"),
        (
            "concat_ws(',', 'a', 'b', 1, 2, 1.1, 0.11, cast(1.1 as decimal(3, 1)))",
            "STR:a,b,1,2,1.1,0.11,1.1",
        ),
        ("concat_ws(0x2c, 0x61, 'b')", "STR:a,b"),
        ("concat_ws(',', 'a', '')", "STR:a,"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
    assert_eq!(
        string_fn::concat_ws(&[
            Datum::new_bytes(vec![b'|']),
            Datum::new_bytes(vec![0xff]),
            Datum::Null,
            Datum::new_string("b"),
        ])
        .unwrap(),
        Datum::new_string(vec![0xff, b'|', b'b'])
    );
    assert!(string_fn::concat_ws(&[]).is_err());
    assert!(string_fn::concat_ws(&[Datum::new_string(",")]).is_err());
}

#[test]
fn concat_signature_source_rows_preserve_scalar_results() {
    // `TestConcatSig` (pkg/expression/builtin_string_test.go:225) uses
    // chunk-column metadata to exercise max_allowed_packet warnings. Its
    // ordinary scalar rows still belong to the same byte-preserving CONCAT
    // evaluator and are asserted here; warning/session state stays partial.
    for (expr, want) in [
        ("concat('a', 'b')", "STR:ab"),
        ("concat('中', 'a')", "STR:中a"),
        ("concat('中文', 'a')", "STR:中文a"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }

    // `TestConcatWSSig` (source line 345) has the same vectorized warning
    // boundary. The value-domain rows are separator joins with Unicode text.
    for (expr, want) in [
        ("concat_ws(',', 'a', 'b')", "STR:a,b"),
        ("concat_ws(',', '中', 'a')", "STR:中,a"),
        ("concat_ws(',', '中文', 'a')", "STR:中文,a"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

#[test]
fn instr_source_vectors_preserve_string_coercion_and_nulls() {
    // pkg/expression/builtin_string_test.go:1968 TestInstr. INSTR(str,
    // substr) shares the same 1-indexed character position contract as the
    // two-argument LOCATE path, but this test exercises the public dispatch.
    for (expr, want) in [
        ("instr('foobarbar', 'bar')", "INT:4"),
        ("instr('xbar', 'foobar')", "INT:0"),
        ("instr(123456234, 234)", "INT:2"),
        ("instr(123456, 567)", "INT:0"),
        ("instr(1e10, 1e2)", "INT:1"),
        ("instr(1.234, '.234')", "INT:2"),
        ("instr(1.234, '')", "INT:1"),
        ("instr('', 123)", "INT:0"),
        ("instr('', '')", "INT:1"),
        ("instr('中文美好', '美好')", "INT:3"),
        ("instr('中文美好', '世界')", "INT:0"),
        ("instr('中文abc', 'a')", "INT:3"),
        ("instr('live long and prosper', 'long')", "INT:6"),
        ("instr('not binary string', 'binary')", "INT:5"),
        ("instr('upper case', 'upper')", "INT:1"),
        ("instr('UPPER CASE', 'CASE')", "INT:7"),
        ("instr('中文abc', 'abc')", "INT:3"),
        ("instr('foobar', NULL)", "NULL"),
        ("instr(NULL, 'foobar')", "NULL"),
        ("instr(NULL, NULL)", "NULL"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
    // Valid binary bytes still use the same position in this value domain;
    // invalid-byte/session collation signatures remain explicit boundaries.
    assert_eq!(e("instr(unhex('666f6f626172'), unhex('626172'))"), "INT:4");
}

#[test]
fn predicates() {
    // IN with a match / no match / negation.
    assert_eq!(e("2 in (1, 2, 3)"), "INT:1");
    assert_eq!(e("5 in (1, 2, 3)"), "INT:0");
    assert_eq!(e("5 not in (1, 2, 3)"), "INT:1");
    assert_eq!(e("'b' in ('a', 'b')"), "INT:1");
    // IN three-valued logic: a NULL in the list (no match) is NULL, but a
    // real match still wins over the NULL.
    assert_eq!(e("5 in (1, NULL, 3)"), "NULL");
    assert_eq!(e("1 in (1, NULL)"), "INT:1");
    assert_eq!(e("NULL in (1, 2)"), "NULL");
    // BETWEEN and NOT BETWEEN.
    assert_eq!(e("5 between 1 and 10"), "INT:1");
    assert_eq!(e("5 between 6 and 10"), "INT:0");
    assert_eq!(e("5 not between 6 and 10"), "INT:1");
    assert_eq!(e("NULL between 1 and 10"), "NULL");
    // IS always resolves to TRUE/FALSE, never NULL.
    assert_eq!(e("NULL is null"), "INT:1");
    assert_eq!(e("1 is null"), "INT:0");
    assert_eq!(e("1 is not null"), "INT:1");
    assert_eq!(e("1 is true"), "INT:1");
    assert_eq!(e("0 is true"), "INT:0");
    assert_eq!(e("NULL is true"), "INT:0");
    assert_eq!(e("NULL is not true"), "INT:1");
    assert_eq!(e("0 is false"), "INT:1");
}

/// Bounded value-only slice of `pkg/expression/builtin_other_test.go`'s
/// `TestRowFunc`/`TestInFunc` rows.  The Go function-class test constructs a
/// four-argument row signature (it does not evaluate a bare row), while the
/// production Rust evaluator intentionally gives `ROW(...)` meaning only as
/// a comparison/`IN` operand.  Keeping the row inside those SQL contexts
/// proves the same source shape without inventing a standalone row value
/// domain.  Temporal, duration, JSON, collation-metadata, and vectorized
/// signatures remain explicit partial boundaries below.
#[test]
fn builtin_other_row_and_in_source_vectors() {
    assert_eq!(
        e("row('1', 1.2, true, 120) = row('1', 1.2, true, 120)"),
        "INT:1"
    );
    assert_eq!(e("row(1, 2) <> row(1, 3)"), "INT:1");

    // Integer, unsigned-boundary, float, decimal, string, and NULL rows
    // from TestInFunc all stay in the seed Datum domain.
    for (expr, want) in [
        ("1 in (1, 2, 3)", "INT:1"),
        ("1 in (0, 2, 3)", "INT:0"),
        ("1 in (NULL, 2, 3)", "NULL"),
        ("NULL in (NULL, 2, 3)", "NULL"),
        (
            "18446744073709551615 in (18446744073709551615, 2, 3)",
            "INT:1",
        ),
        ("-1 in (18446744073709551615, 2, 3)", "INT:0"),
        ("1.1e0 in (1.1e0, 1.2e0, 1.3e0)", "INT:1"),
        ("1.1e0 in (1.2e0, 1.3e0)", "INT:0"),
        ("123.121 in (123.122, 123.123)", "INT:0"),
        ("123.121 in (123.122, 123.121)", "INT:1"),
        ("'1.1' in ('1.1', '1.2', '1.3')", "INT:1"),
        ("'1.1' in ('1.2', '1.3')", "INT:0"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

/// The source `TestTypeConversion` asks `BuildGetVarFunction` to convert an
/// integer user variable to DECIMAL and DOUBLE.  Rust's seed evaluator does
/// not yet expose Go's build-time FieldType/function-class seam, so this test
/// keeps the same stored-user-variable values and verifies the production
/// CAST conversion path explicitly.  The evidence remains PARTIAL until
/// typed user-variable retrieval is represented by the session contract.
#[test]
fn builtin_other_type_conversion_source_scalars() {
    struct UserVar(Datum);

    impl Columns for UserVar {
        fn get(&self, _: &[String]) -> Option<Datum> {
            None
        }

        fn get_uservar(&self, name: &str) -> Option<Datum> {
            (name.eq_ignore_ascii_case("a")).then(|| self.0.clone())
        }
    }

    fn eval_user_expr(sql: &str, resolver: &UserVar) -> String {
        let stmt = tidb_parser::parse(&format!("select {sql}")).expect("parse");
        let Stmt::Query(query) = stmt else {
            panic!("not query")
        };
        let QueryStmt::Select(s) = query.into_inner() else {
            panic!("not select")
        };
        let SelectField::Expr { expr, .. } = &s.fields[0] else {
            panic!("not expression")
        };
        eval_in(expr, resolver).expect("evaluate").label()
    }

    let resolver = UserVar(Datum::Int(3));
    assert_eq!(eval_user_expr("cast(@a as decimal)", &resolver), "DEC:3");
    assert_eq!(eval_user_expr("cast(@a as double)", &resolver), "FLOAT:3");
}

#[test]
fn like_predicate() {
    // Anchored, prefix, suffix, and infix `%`.
    assert_eq!(e("'abc' like 'abc'"), "INT:1");
    assert_eq!(e("'abc' like 'a%'"), "INT:1");
    assert_eq!(e("'abc' like '%c'"), "INT:1");
    assert_eq!(e("'abc' like '%b%'"), "INT:1");
    assert_eq!(e("'abc' like 'a%d'"), "INT:0");
    // `_` matches exactly one character.
    assert_eq!(e("'abc' like 'a_c'"), "INT:1");
    assert_eq!(e("'abc' like 'a_'"), "INT:0");
    assert_eq!(e("'ac' like 'a_c'"), "INT:0");
    // Case-sensitive (utf8mb4_bin), and NOT LIKE.
    assert_eq!(e("'abc' like 'ABC'"), "INT:0");
    assert_eq!(e("'abc' not like 'a%'"), "INT:0");
    assert_eq!(e("'abc' not like 'x%'"), "INT:1");
    // NULL operands.
    assert_eq!(e("NULL like 'a%'"), "NULL");
    assert_eq!(e("'abc' like NULL"), "NULL");
    // Empty pattern matches only the empty string.
    assert_eq!(e("'' like ''"), "INT:1");
    assert_eq!(e("'a' like ''"), "INT:0");
    // A non-string operand, on EITHER side, is implicitly stringified
    // the same way `Datum::sql_string` renders it -- including a
    // `DECIMAL`'s declared scale (`12.50` stringifies to `"12.50"`,
    // not simplified to `"12.5"`).
    assert_eq!(e("2 like '2'"), "INT:1");
    assert_eq!(e("123 like '12%'"), "INT:1");
    assert_eq!(e("12.50 like '12.5'"), "INT:0");
    assert_eq!(e("12.50 like '12.50'"), "INT:1");
    assert_eq!(e("1.5e2 like '150'"), "INT:1");
    assert_eq!(e("-5 like '-5'"), "INT:1");
    assert_eq!(e("'2' like 2"), "INT:1");
    assert_eq!(e("123 like 12"), "INT:0");

    // The source matcher compiles an explicit ESCAPE byte exactly like
    // `pkg/util/stringutil.CompilePattern`: a custom byte quotes the next
    // character, while ESCAPE '' disables quoting completely.  A trailing
    // escape byte is retained as a literal (the Go compiler leaves the
    // escape rune in place when there is no following rune).
    assert_eq!(e("'a' like '+a' escape '+'"), "INT:1");
    assert_eq!(e("'a+' like 'a+' escape '+'"), "INT:1");
    assert_eq!(e("'a+' like 'a++' escape '+'"), "INT:1");
    assert_eq!(e("'a' like 'a\\\\'"), "INT:0");
    assert_eq!(e("'a\\\\' like 'a\\\\'"), "INT:1");
    assert_eq!(e("'a' like 'a\\\\' escape ''"), "INT:0");
    assert_eq!(e("'a\\\\' like 'a\\\\' escape ''"), "INT:1");
}

#[test]
fn three_valued_logic() {
    assert_eq!(e("1 AND NULL"), "NULL");
    assert_eq!(e("0 AND NULL"), "INT:0");
    assert_eq!(e("1 OR NULL"), "INT:1");
    assert_eq!(e("0 OR NULL"), "NULL");
    assert_eq!(e("NOT NULL"), "NULL");
    assert_eq!(e("NULL <=> NULL"), "INT:1");
    assert_eq!(e("NULL <=> 1"), "INT:0");
    assert_eq!(e("NULL + 1"), "NULL");
}

/// Complete scalar source tables from `TestLogicAnd` (lines 127-146) and
/// `TestLogicOr` (lines 346-369) in `pkg/expression/builtin_op_test.go`.
/// The final injected-error row in each Go table uses an `errors.New` datum;
/// the seed evaluator deliberately has no error-valued `Datum`, so its
/// propagation contract is tracked as an explicit model boundary in the
/// porting ledger rather than fabricated as a SQL literal.
#[test]
fn logic_and_or_follow_tidb_source_tables() {
    for (sql, want) in [
        ("1 AND 1", "INT:1"),
        ("1 AND 0", "INT:0"),
        ("0 AND 1", "INT:0"),
        ("0 AND 0", "INT:0"),
        ("2 AND -1", "INT:1"),
        ("'a' AND '0'", "INT:0"),
        ("'a' AND '1'", "INT:0"),
        ("'1a' AND '0'", "INT:0"),
        ("'1a' AND '1'", "INT:1"),
        ("0 AND NULL", "INT:0"),
        ("NULL AND 0", "INT:0"),
        ("NULL AND 1", "NULL"),
        ("0.001 AND 0", "INT:0"),
        ("0.001 AND 1", "INT:1"),
        ("NULL AND 0.000", "INT:0"),
        ("NULL AND 0.001", "NULL"),
        ("0.000001 AND 0", "INT:0"),
        ("0.000001 AND 1", "INT:1"),
        ("0.000000 AND NULL", "INT:0"),
        ("0.000001 AND NULL", "NULL"),
        ("1 OR 1", "INT:1"),
        ("1 OR 0", "INT:1"),
        ("0 OR 1", "INT:1"),
        ("0 OR 0", "INT:0"),
        ("2 OR -1", "INT:1"),
        ("'a' OR '0'", "INT:0"),
        ("'a' OR '1'", "INT:1"),
        ("'1a' OR '0'", "INT:1"),
        ("'1a' OR '1'", "INT:1"),
        ("'0.0a' OR 0", "INT:0"),
        ("'0.0001a' OR 0", "INT:1"),
        ("1 OR NULL", "INT:1"),
        ("NULL OR 1", "INT:1"),
        ("NULL OR 0", "NULL"),
        ("0.000 OR 0", "INT:0"),
        ("0.001 OR 0", "INT:1"),
        ("NULL OR 0.000", "NULL"),
        ("NULL OR 0.001", "INT:1"),
        ("0.000000 OR 0", "INT:0"),
        ("0.000000 OR 1", "INT:1"),
        ("0.000000 OR NULL", "NULL"),
        ("0.000001 OR 0", "INT:1"),
        ("0.000001 OR 1", "INT:1"),
        ("0.000001 OR NULL", "INT:1"),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
    assert!(tidb_parser::parse("select 1 AND").is_err());
    assert!(tidb_parser::parse("select 1 OR").is_err());
}

/// Source vectors from `TestLogicXor` in
/// `pkg/expression/builtin_op_test.go`: binary logical operators use ETInt
/// numeric-prefix coercion for strings, unlike ordinary string comparison.
#[test]
fn logic_xor_coerces_strings_and_preserves_three_valued_null() {
    for (sql, want) in [
        ("'a' XOR '0'", "INT:0"),
        ("'a' XOR '1'", "INT:1"),
        ("'1a' XOR '0'", "INT:1"),
        ("'1a' XOR '1'", "INT:0"),
        ("0.5000 XOR 0.4999", "INT:0"),
        ("0.5000 XOR 1.0", "INT:0"),
        ("0.4999 XOR 1.0", "INT:0"),
        ("NULL XOR 0.000", "NULL"),
        ("NULL XOR 0.001", "NULL"),
        ("0.000001 XOR 1", "INT:0"),
        ("0.000000 XOR NULL", "NULL"),
        ("0.000001 XOR NULL", "NULL"),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
    // This guard proves ordinary string operators retain collation comparison
    // semantics after logical dispatch moved ahead of that branch.
    assert_eq!(e("'a' = '0'"), "INT:0");
    assert!(tidb_parser::parse("select 1 XOR").is_err());
}

/// Scalar rows from `pkg/expression/builtin_op_test.go`'s bitwise and unary
/// operator tables (`TestLeftShift`, `TestRightShift`, `TestBitXor`,
/// `TestBitOr`, `TestBitAnd`, `TestBitNeg`, and `TestUnaryNot`).  The Go
/// tables also inject an `errors.New` datum; the SQL evaluator has no error
/// value variant, so those rows remain an explicit non-SQL boundary.  Every
/// representable literal row is kept here so the source table cannot quietly
/// regress to a hand-picked happy path.
#[test]
fn bitwise_and_unary_source_vectors_match_tidb() {
    for (sql, want) in [
        // TestLeftShift.
        ("123 << 2", "UINT:492"),
        ("-123 << 2", "UINT:18446744073709551124"),
        ("NULL << 1", "NULL"),
        // TestRightShift.
        ("123 >> 2", "UINT:30"),
        ("-123 >> 2", "UINT:4611686018427387873"),
        ("NULL >> 1", "NULL"),
        // TestBitXor.
        ("123 ^ 321", "UINT:314"),
        ("-123 ^ 321", "UINT:18446744073709551300"),
        ("NULL ^ 1", "NULL"),
        // TestBitOr.
        ("123 | 321", "UINT:379"),
        ("-123 | 321", "UINT:18446744073709551557"),
        ("NULL | 1", "NULL"),
        // TestBitAnd.
        ("123 & 321", "UINT:65"),
        ("-123 & 321", "UINT:257"),
        ("NULL & 1", "NULL"),
        // TestBitNeg.
        ("~123", "UINT:18446744073709551492"),
        ("~-123", "UINT:122"),
        ("~NULL", "NULL"),
        // TestUnaryNot's numeric, string-prefix, decimal, and NULL rows.
        ("NOT 1", "INT:0"),
        ("NOT 0", "INT:1"),
        ("NOT 123", "INT:0"),
        ("NOT -123", "INT:0"),
        ("NOT '123'", "INT:0"),
        ("NOT 0.3e0", "INT:0"),
        ("NOT '0.3'", "INT:0"),
        ("NOT 0.3", "INT:0"),
        ("NOT NULL", "NULL"),
        // `!` is the alternate spelling of the same AST unary operator.
        ("!0", "INT:1"),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
    assert!(tidb_parser::parse("select 1 <<").is_err());
    assert!(tidb_parser::parse("select 1 ^").is_err());
}

/// Scalar rows from `TestIsTrueOrFalse` in
/// `pkg/expression/builtin_op_test.go`.  TiDB builds these predicates over
/// the same numeric-prefix coercion as `EvalReal`: malformed text is zero,
/// while a non-zero decimal/real/string is true.  Typed duration/time/JSON
/// rows in the Go table require FieldType/session value domains and remain
/// explicit partial coverage rather than being replaced with SQL literals.
#[test]
fn is_true_and_false_source_vectors_use_numeric_prefix_truthiness() {
    for (sql, want) in [
        ("-12 IS TRUE", "INT:1"),
        ("-12 IS FALSE", "INT:0"),
        ("12 IS TRUE", "INT:1"),
        ("12 IS FALSE", "INT:0"),
        ("0 IS TRUE", "INT:0"),
        ("0 IS FALSE", "INT:1"),
        ("0.0e0 IS TRUE", "INT:0"),
        ("0.0e0 IS FALSE", "INT:1"),
        ("'aaa' IS TRUE", "INT:0"),
        ("'aaa' IS FALSE", "INT:1"),
        ("'' IS TRUE", "INT:0"),
        ("'' IS FALSE", "INT:1"),
        ("'0.3' IS TRUE", "INT:1"),
        ("'0.3' IS FALSE", "INT:0"),
        ("0.3e0 IS TRUE", "INT:1"),
        ("0.3e0 IS FALSE", "INT:0"),
        ("0.3 IS TRUE", "INT:1"),
        ("0.3 IS FALSE", "INT:0"),
        ("NULL IS TRUE", "INT:0"),
        ("NULL IS FALSE", "INT:0"),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
}

#[test]
fn bitwise_and_div_by_zero() {
    assert_eq!(e("7 & 3"), "UINT:3");
    assert_eq!(e("1 << 4"), "UINT:16");
    assert_eq!(e("100 >> 2"), "UINT:25");
    // DIV / MOD by zero are NULL in MySQL.
    assert_eq!(e("10 DIV 0"), "NULL");
    assert_eq!(e("10 MOD 0"), "NULL");
}

#[test]
fn out_of_domain_is_unsupported() {
    // A user variable reference is NEVER an error (confirmed via
    // `gorun`: an unset/session-less `@x` reads as `NULL`, unlike
    // `@@sysvar`'s deliberately narrower, error-on-unknown-name
    // domain) — `eval`'s stateless `NoColumns` resolver has no
    // session at all, which `Columns::get_uservar`'s own default
    // collapses to the SAME `NULL` a real session's genuinely-unset
    // variable would give, not an error.
    assert_eq!(
        {
            let stmt = tidb_parser::parse("select @x").unwrap();
            let Stmt::Query(query) = stmt else {
                unreachable!()
            };
            let QueryStmt::Select(s) = query.into_inner() else {
                unreachable!()
            };
            let SelectField::Expr { expr, .. } = &s.fields[0] else {
                unreachable!()
            };
            eval(expr).unwrap()
        },
        Datum::Null
    );
    // An unsupported expression construct (a window function outside
    // its own evaluation context) is still a genuine error.
    assert!(matches!(
        {
            let stmt = tidb_parser::parse("select row_number() over ()").unwrap();
            let Stmt::Query(query) = stmt else {
                unreachable!()
            };
            let QueryStmt::Select(s) = query.into_inner() else {
                unreachable!()
            };
            let SelectField::Expr { expr, .. } = &s.fields[0] else {
                unreachable!()
            };
            eval(expr)
        },
        Err(EvalError::Unsupported(_))
    ));
}

#[test]
fn decimals() {
    // The literal's own scale is preserved verbatim; leading zeros in the
    // integer part are stripped; a numerically zero value never carries a
    // sign — MyDecimal's canonical string form.
    assert_eq!(e("3.14"), "DEC:3.14");
    assert_eq!(e("3.140"), "DEC:3.140");
    assert_eq!(e("010.500"), "DEC:10.500");
    assert_eq!(e("-0.0"), "DEC:0.0");
    assert_eq!(e(".5"), "DEC:0.5");
    assert_eq!(e("5."), "DEC:5");
    // Exact arithmetic: no float rounding error, unlike a binary float.
    assert_eq!(e("0.1 + 0.2"), "DEC:0.3");
    assert_eq!(e("3.14 + 2.1"), "DEC:5.24"); // scale = max(2, 1)
    assert_eq!(e("3.14 - 5"), "DEC:-1.86"); // Int promotes to decimal(0)
    assert_eq!(e("1.5 * 2"), "DEC:3.0"); // scale = 1 + 0
    assert_eq!(e("3.140 - 3.14"), "DEC:0.000"); // zero result: no sign
                                                // Equality ignores scale; ordering does not.
    assert_eq!(e("1.5 = 1.50"), "INT:1");
    assert_eq!(e("1.5 < 2"), "INT:1");
    // Truthiness (nonzero is truthy) is shared by NOT / IS TRUE|FALSE /
    // AND / OR / XOR.
    assert_eq!(e("not 0.0"), "INT:1");
    assert_eq!(e("0.0 is false"), "INT:1");
    assert_eq!(e("0.0 and 1"), "INT:0");
    // Builtins.
    assert_eq!(e("abs(-3.14)"), "DEC:3.14");
    assert_eq!(e("sign(-3.14)"), "INT:-1");
    assert_eq!(e("nullif(3.14, 3.140)"), "NULL"); // equal despite differing scale
    assert_eq!(e("least(1.5, 2.5, 0.5)"), "DEC:0.5");

    // DIV/MOD: exact via unsigned long division, truncating toward zero.
    // DIV's result is an Int; MOD's is a Decimal at max(scale_a, scale_b),
    // sign of the dividend.
    assert_eq!(e("3.14 div 2"), "INT:1");
    assert_eq!(e("3.14 mod 2"), "DEC:1.14");
    assert_eq!(e("-3.14 div 2"), "INT:-1"); // truncates toward zero, not floor
    assert_eq!(e("-3.14 mod 2"), "DEC:-1.14"); // remainder sign follows the dividend
    assert_eq!(e("5 div 2.5"), "INT:2"); // Int promotes to decimal
    assert_eq!(e("2.5 mod 5"), "DEC:2.5");
    assert_eq!(e("10 div 0.0"), "NULL"); // division by (decimal) zero
    assert_eq!(e("10 mod 0.0"), "NULL");
    // `/` grows the result scale by 4 past the DIVIDEND's own scale
    // (MySQL's div_precision_increment), regardless of the divisor's
    // own scale — confirmed via `goeval`, not assumed.
    assert_eq!(e("3.14 / 2"), "DEC:1.570000"); // scale 2+4=6
    assert_eq!(e("5 / 2.5"), "DEC:2.0000"); // scale 0+4=4, divisor's scale ignored
    assert_eq!(e("1.5 / 0"), "NULL"); // division by zero

    // Bitwise/shift: rounds to the nearest i64 first, ties away from
    // zero (not round-half-to-even), then applies the integer operator.
    assert_eq!(e("~3.14"), "UINT:18446744073709551612");
    assert_eq!(e("~3.5"), "UINT:18446744073709551611");
    assert_eq!(e("~2.5"), "UINT:18446744073709551612");
    assert_eq!(e("3.14 & 5"), "UINT:1");
    assert_eq!(e("3.14 << 1"), "UINT:6");
    assert_eq!(e("-1.5 & 3"), "UINT:2");
}

#[test]
fn floats() {
    // A literal round-trips via Rust's own f64 Display — confirmed
    // (by direct comparison, not assumed) to match Go's
    // strconv.FormatFloat(f, 'f', -1, 64) byte for byte across a wide
    // value range, so no custom formatting is needed, unlike Decimal.
    assert_eq!(e("1.5e2"), "FLOAT:150");
    assert_eq!(e("-1.5e2"), "FLOAT:-150");
    assert_eq!(e("-0.0e0"), "FLOAT:-0");
    // An Int or Decimal operand promotes to Float — the OPPOSITE
    // direction from how Decimal dominates Int (Float dominates
    // Decimal instead) — confirmed via goeval, not assumed: even a
    // Decimal-looking literal like `3.14` promotes once a Float is
    // anywhere in the expression.
    assert_eq!(e("1.5e2 + 1"), "FLOAT:151");
    assert_eq!(e("1.5e2 + 3.14"), "FLOAT:153.14");
    assert_eq!(e("1.5e2 / 2"), "FLOAT:75");
    assert_eq!(e("1.5e2 / 0"), "NULL");
    // A float literal that would overflow to infinity is rejected at
    // PARSE time (confirmed via godump restore: real TiDB rejects
    // `1e400`, the boundary is exactly f64::MAX), so an in-domain
    // Float value here is always finite by construction; an
    // ARITHMETIC result that overflows is instead a genuine
    // evaluation error (confirmed via goeval: `1e300 * 1e300`
    // errors, never silently becomes IEEE-754 infinity) — this is
    // the one case the differential corpus can't itself assert
    // (`ERR` goldens are skipped, not compared), so it's covered
    // directly here instead.
    assert_eq!(e("1e300 * 1e300"), "FloatOverflow");
    assert_eq!(e("1e-300 * 1e-300"), "FLOAT:0"); // underflow to zero is fine
                                                 // Bitwise/shift rounds to the nearest i64 first — but TIES TO
                                                 // EVEN, the OPPOSITE tie-breaking rule from Decimal's own `~`
                                                 // (ties away from zero) — confirmed via goeval, not assumed.
    assert_eq!(e("~2.5e0"), "UINT:18446744073709551613");
    assert_eq!(e("~3.5e0"), "UINT:18446744073709551611");
    // DIV truncates toward zero to an Int result, same as Int/Decimal.
    assert_eq!(e("1.007e2 div 3"), "INT:33");
    assert_eq!(e("-1.007e2 div 3"), "INT:-33");
    // LEAST/GREATEST promote their RESULT to the widest argument type
    // (a real bug caught by the differential corpus on the first
    // attempt, not assumed correct): the winning argument `2` is a
    // bare Int literal, but the result is still Float because
    // ANOTHER argument was Float.
    assert_eq!(e("least(1.5e2, 3.14, 2)"), "FLOAT:2");
    assert_eq!(e("greatest(1.5e2, 3.14, 2)"), "FLOAT:150");
    // NULLIF's equality reuses the same cross-type promotion, unlike
    // a hand-rolled same-type-only check.
    assert_eq!(e("nullif(150, 1.5e2)"), "NULL");
    assert_eq!(e("sign(0.0e0)"), "INT:0"); // unlike IEEE-754 signum, never 0
}

/// A [`Columns`] fixture with a fixed clock but no columns — for
/// testing `NOW()`/`CURRENT_TIMESTAMP()` and friends directly, since
/// `e`/`v`'s `NoColumns` has no session by design (see
/// `now_current_timestamp` below for that boundary case). Fields:
/// `(utc_secs, nanos, tz_offset_seconds)`.
struct FixedClock(i64, u32, i32);
impl Columns for FixedClock {
    fn get(&self, _: &[String]) -> Option<Datum> {
        None
    }
    fn now(&self) -> Option<(i64, u32, i32)> {
        Some((self.0, self.1, self.2))
    }
}

fn e_at(expr: &str, clock: &FixedClock) -> String {
    let stmt = tidb_parser::parse(&format!("select {expr}")).expect("parse");
    let Stmt::Query(query) = stmt else {
        panic!("expected Query")
    };
    let QueryStmt::Select(s) = query.into_inner() else {
        panic!("expected Select")
    };
    let SelectField::Expr { expr, .. } = &s.fields[0] else {
        panic!("expected expr field")
    };
    match eval_in(expr, clock) {
        Ok(v) => v.label(),
        Err(e) => format!("{e:?}"),
    }
}

#[test]
fn case_when() {
    // Simple form: WHEN compares `value = cond` via ordinary `=`.
    assert_eq!(e("case 1 when 1 then 'a' when 2 then 'b' end"), "STR:a");
    assert_eq!(e("case 2 when 1 then 'a' when 2 then 'b' end"), "STR:b");
    // No match and no ELSE is NULL.
    assert_eq!(e("case 3 when 1 then 'a' when 2 then 'b' end"), "NULL");
    assert_eq!(e("case 3 when 1 then 'a' else 'c' end"), "STR:c");
    // Searched form: no compare value, each WHEN is truthiness-tested
    // directly -- the same three-valued logic IF/WHERE already use.
    assert_eq!(e("case when 1=1 then 10 else 20 end"), "INT:10");
    assert_eq!(e("case when 1=0 then 10 else 20 end"), "INT:20");
    assert_eq!(e("case when 1=0 then 10 end"), "NULL");
    // A NULL searched condition is neither true nor false -- ELSE wins.
    assert_eq!(e("case when null then 1 else 2 end"), "INT:2");
    // A NULL simple-CASE value never matches ANY WHEN (NULL = x is
    // NULL, matching ordinary `=` propagation) -- confirmed via
    // goeval, not assumed: `CASE NULL WHEN NULL THEN 1 ELSE 2 END`
    // is `2`, not `1`.
    assert_eq!(e("case null when null then 1 else 2 end"), "INT:2");
    assert_eq!(e("case null when 1 then 'a' else 'b' end"), "STR:b");
    // The FIRST matching WHEN wins, even if a later one would also
    // match.
    assert_eq!(
        e("case when 1=1 then 1 when 1=1 then 2 else 3 end"),
        "INT:1"
    );
    assert_eq!(e("case 1 when 1 then 10 when 1 then 20 end"), "INT:10");
    // LAZY evaluation: only the taken branch is ever evaluated,
    // matching real MySQL's short-circuit CASE -- a load-bearing SQL
    // idiom for guarding against errors. `1/0` in the untaken branch
    // must NOT raise `IntOverflow`/division-by-zero here.
    assert_eq!(e("case when 1=0 then 1/0 else 5 end"), "INT:5");
    assert_eq!(e("case 1 when 2 then 1/0 else 5 end"), "INT:5");
    // Nests, and composes with ordinary operators.
    assert_eq!(
        e("case when 1=1 then case when 2=2 then 'nested' else 'no' end else 'outer' end"),
        "STR:nested"
    );
    assert_eq!(e("1 + case when 1=1 then 10 else 20 end"), "INT:11");
}

#[test]
fn now_current_timestamp() {
    // `NoColumns` (used by plain constant-expression `eval`) has no
    // session clock by design -- this evaluator never falls back to
    // the live wall clock, which would be non-deterministic.
    assert_eq!(
        e("now()"),
        "Unsupported(\"no session clock (SET timestamp)\")"
    );
    assert_eq!(
        e("current_timestamp"),
        "Unsupported(\"no session clock (SET timestamp)\")"
    );

    // 1700000000.123456 (Unix epoch, UTC) -- matches the exact value
    // probed via `gorun` for the `rust/difftests/corpus/table/
    // now_current_timestamp.txt` topic.
    let clock = FixedClock(1_700_000_000, 123_456_000, 0);
    assert_eq!(e_at("now()", &clock), "STR:2023-11-14 22:13:20");
    // NOW and CURRENT_TIMESTAMP are true synonyms; CURRENT_TIMESTAMP
    // also parses with no `()` at all.
    assert_eq!(e_at("current_timestamp", &clock), "STR:2023-11-14 22:13:20");
    assert_eq!(
        e_at("current_timestamp()", &clock),
        "STR:2023-11-14 22:13:20"
    );
    // The fractional part TRUNCATES (never rounds) to the requested
    // 0-6 precision.
    assert_eq!(e_at("now(3)", &clock), "STR:2023-11-14 22:13:20.123");
    assert_eq!(e_at("now(6)", &clock), "STR:2023-11-14 22:13:20.123456");
    // TestNowAndUTCTimestamp's exact invalid source values: only 0-6 is a
    // valid precision.
    assert_eq!(
        e_at("now(8)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
    assert_eq!(
        e_at("now(-2)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
    // Additional boundary neighbors retain the same contract.
    assert_eq!(
        e_at("now(7)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
    assert_eq!(
        e_at("now(-1)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
}

#[test]
fn curdate_curtime() {
    // A nonzero `time_zone` offset (+05:30, matching the epoch/offset
    // probed via `gorun`) shifts CURDATE/CURTIME's local rendering.
    let clock = FixedClock(1_700_000_000, 654_321_000, 19_800);
    assert_eq!(e_at("curdate()", &clock), "STR:2023-11-15");
    assert_eq!(e_at("current_date", &clock), "STR:2023-11-15");
    assert_eq!(e_at("current_date()", &clock), "STR:2023-11-15");
    assert_eq!(e_at("curtime()", &clock), "STR:03:43:20");
    assert_eq!(e_at("current_time", &clock), "STR:03:43:20");
    // TestCurrentTime's explicit precision table.
    assert_eq!(e_at("current_time(3)", &clock), "STR:03:43:20.654");
    assert_eq!(e_at("current_time(6)", &clock), "STR:03:43:20.654321");
    // Go's parser rejects a signed precision before execution sees it.
    assert!(tidb_parser::parse("select current_time(-1)").is_err());
    assert_eq!(
        e_at("current_time(7)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
    assert_eq!(e_at("curtime(3)", &clock), "STR:03:43:20.654");
    // CURDATE takes no argument at all -- confirmed via `godump
    // restore`: `CURDATE(1)` is a real parse error, not just an
    // out-of-range value.
    assert!(tidb_parser::parse("select curdate(1)").is_err());

    // A genuine SPLIT rule, confirmed via `gorun`: the 0-arg form
    // TRUNCATES, but an EXPLICIT argument (even literally `0`)
    // ROUNDS. UTC offset 0 here isolates the effect from the
    // time_zone shift above.
    let utc = FixedClock(1_700_000_000, 654_321_000, 0);
    assert_eq!(e_at("curtime()", &utc), "STR:22:13:20"); // truncates
    assert_eq!(e_at("curtime(0)", &utc), "STR:22:13:21"); // rounds up
}

#[test]
fn utc_date_time_timestamp() {
    // The RAW UTC clock, ignoring `time_zone` entirely -- with a
    // nonzero offset, `UTC_TIMESTAMP()` still reports the SAME value
    // it would at offset 0 (confirmed via `gorun`).
    let clock = FixedClock(1_700_000_000, 654_321_000, 19_800);
    assert_eq!(e_at("utc_date()", &clock), "STR:2023-11-14");
    // UTC_TIMESTAMP() ALWAYS ROUNDS (ties away from zero), for BOTH
    // the 0-arg and explicit-arg forms alike -- unlike NOW's uniform
    // truncation, and unlike CURTIME/UTC_TIME's 0-arg/explicit-arg
    // split. Confirmed via reading `evalUTCTimestampWithFsp` in
    // `pkg/expression/builtin_time.go`, not assumed.
    assert_eq!(e_at("utc_timestamp()", &clock), "STR:2023-11-14 22:13:21");
    assert_eq!(e_at("utc_timestamp(0)", &clock), "STR:2023-11-14 22:13:21");
    assert_eq!(
        e_at("utc_timestamp(3)", &clock),
        "STR:2023-11-14 22:13:20.654"
    );
    assert_eq!(
        e_at("utc_timestamp(6)", &clock),
        "STR:2023-11-14 22:13:20.654321"
    );
    assert_eq!(
        e_at("utc_timestamp(8)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
    // Signed precision is a parse error in Go's FuncDatetimePrecListOpt.
    assert!(tidb_parser::parse("select utc_timestamp(-2)").is_err());
    // UTC_TIME has the SAME 0-arg-truncates/explicit-arg-rounds split
    // as CURTIME.
    assert_eq!(e_at("utc_time()", &clock), "STR:22:13:20");
    assert_eq!(e_at("utc_time(0)", &clock), "STR:22:13:21");
    assert_eq!(e_at("utc_time(3)", &clock), "STR:22:13:20.654");
    assert_eq!(e_at("utc_time(6)", &clock), "STR:22:13:20.654321");
    assert!(tidb_parser::parse("select utc_time(-1)").is_err());
    assert_eq!(
        e_at("utc_time(7)", &clock),
        "Unsupported(\"bad fractional-seconds-precision argument\")"
    );
}

#[test]
fn date_parts() {
    // A DATE/DATETIME value is a plain string; these extract its
    // calendar components directly, ignoring any time-of-day part.
    assert_eq!(e("year('2021-03-15')"), "INT:2021");
    assert_eq!(e("month('2021-03-15')"), "INT:3");
    assert_eq!(e("day('2021-03-15')"), "INT:15");
    assert_eq!(e("dayofmonth('2021-03-15')"), "INT:15");
    assert_eq!(e("quarter('2021-03-15')"), "INT:1");
    assert_eq!(e("quarter('2021-12-31')"), "INT:4");
    assert_eq!(e("year('2021-03-15 10:30:00')"), "INT:2021");
    // Lenient separators (any run of non-digit characters) and
    // whitespace trimming, matching real TiDB's own leniency.
    assert_eq!(e("year('2021-3-5')"), "INT:2021"); // no zero-padding required
    assert_eq!(e("year('  2021-01-15  ')"), "INT:2021");
    assert_eq!(e("year('2021/01/15')"), "INT:2021");
    // Calendar validation: month 1-12, day valid for that month/year.
    assert_eq!(e("year('not a date')"), "NULL");
    assert_eq!(e("year('2021-13-01')"), "NULL"); // no month 13
    assert_eq!(e("year('2021-01-32')"), "NULL"); // no day 32
    assert_eq!(e("year('2021-02-30')"), "NULL"); // Feb never has 30 days
    assert_eq!(e("year('2020-02-29')"), "INT:2020"); // 2020 is a leap year
    assert_eq!(e("year('2021-02-29')"), "NULL"); // 2021 is not
    assert_eq!(e("year('10:30:00')"), "NULL"); // no date part at all
    assert_eq!(e("year(NULL)"), "NULL");

    // A bare, separator-less digit run of EXACTLY 6 or 8 digits (an
    // integer literal argument coerces to this same string form) is a
    // SEPARATE positional YYMMDD/YYYYMMDD reading, confirmed via
    // `goeval` -- NOT the same algorithm as the lenient
    // separator-based path above.
    assert_eq!(e("year(20240315)"), "INT:2024");
    assert_eq!(e("month(20240315)"), "INT:3");
    assert_eq!(e("day(20240315)"), "INT:15");
    assert_eq!(e("quarter(20240315)"), "INT:1");
    assert_eq!(e("year('20240315')"), "INT:2024"); // quoted string, same reading
                                                   // The 6-digit form's 2-digit year is CENTURY-PIVOTED: 00-69 ->
                                                   // 2000-2069, 70-99 -> 1970-1999 (real MySQL/TiDB convention,
                                                   // confirmed via `goeval`, not invented).
    assert_eq!(e("year(240315)"), "INT:2024");
    assert_eq!(e("year(690101)"), "INT:2069"); // pivot boundary
    assert_eq!(e("year(700101)"), "INT:1970"); // pivot boundary, other side
                                               // The SAME century pivot applies to a separator-based date's own
                                               // 1- or 2-digit year (a 1-digit year is indistinguishable from a
                                               // 2-digit one once parsed, and pivots identically) -- but NOT to
                                               // a 3-or-more-digit year, which is taken LITERALLY even when its
                                               // own value happens to be under 100 (confirmed via `goeval`, a
                                               // real asymmetry that couldn't be guessed from the value alone).
    assert_eq!(e("year('24-03-15')"), "INT:2024");
    assert_eq!(e("year('99-03-15')"), "INT:1999");
    assert_eq!(e("year('1-03-15')"), "INT:2001");
    assert_eq!(e("year('099-03-15')"), "INT:99");
    // Calendar validation still applies after century-pivoting.
    assert_eq!(e("year(20241332)"), "NULL"); // no month 13
    assert_eq!(e("year(230229)"), "NULL"); // 2023 (pivoted) is not a leap year

    // DATEDIFF: day count between two dates' DATE parts, ignoring any
    // time-of-day component and honoring leap years.
    assert_eq!(e("datediff('2021-03-15', '2021-03-10')"), "INT:5");
    assert_eq!(e("datediff('2021-03-10', '2021-03-15')"), "INT:-5");
    assert_eq!(e("datediff('2021-01-01', '2020-01-01')"), "INT:366"); // 2020 is a leap year
    assert_eq!(
        e("datediff('2021-03-15 23:59:59', '2021-03-15 00:00:01')"),
        "INT:0"
    ); // same calendar day, time ignored
    assert_eq!(e("datediff('2021-03-15', '2021-03-15')"), "INT:0");
    assert_eq!(e("datediff('not a date', '2021-01-01')"), "NULL");
    assert_eq!(e("datediff('2021-01-01', NULL)"), "NULL");

    // DAYOFYEAR: 1-based day count within the year, leap-year aware.
    assert_eq!(e("dayofyear('2021-01-01')"), "INT:1");
    assert_eq!(e("dayofyear('2021-12-31')"), "INT:365");
    assert_eq!(e("dayofyear('2020-12-31')"), "INT:366"); // 2020 is a leap year
    assert_eq!(e("dayofyear('2020-02-29')"), "INT:60");

    // DAYOFWEEK (1=Sunday..7=Saturday) / WEEKDAY (0=Monday..6=Sunday)
    // over a full week starting 2021-01-01, a Friday.
    assert_eq!(e("dayofweek('2021-01-01')"), "INT:6"); // Friday
    assert_eq!(e("dayofweek('2021-01-03')"), "INT:1"); // Sunday
    assert_eq!(e("dayofweek('2021-01-04')"), "INT:2"); // Monday
    assert_eq!(e("weekday('2021-01-01')"), "INT:4"); // Friday
    assert_eq!(e("weekday('2021-01-04')"), "INT:0"); // Monday
    assert_eq!(e("weekday('2021-02-30')"), "NULL"); // invalid calendar date

    // TO_DAYS: an absolute day number (days_from_civil plus a fixed
    // offset solved from real TiDB's own answer); ignores time-of-day.
    assert_eq!(e("to_days('1970-01-01')"), "INT:719528"); // days_from_civil's own epoch
    assert_eq!(e("to_days('2021-01-01')"), "INT:738156");
    assert_eq!(e("to_days('2021-03-15 10:30:00')"), "INT:738229");
    assert_eq!(e("to_days('not a date')"), "NULL");
    assert_eq!(e("to_days(NULL)"), "NULL");

    // FROM_DAYS: the inverse of TO_DAYS. Outside the valid range
    // (year 0001-9999) real TiDB returns the "zero date" string.
    assert_eq!(e("from_days(719528)"), "STR:1970-01-01");
    assert_eq!(e("from_days(738156)"), "STR:2021-01-01");
    assert_eq!(e("from_days(366)"), "STR:0001-01-01"); // lower boundary
    assert_eq!(e("from_days(3652424)"), "STR:9999-12-31"); // upper boundary
    assert_eq!(e("from_days(365)"), "STR:0000-00-00"); // just below the valid range
    assert_eq!(e("from_days(0)"), "STR:0000-00-00");
    assert_eq!(e("from_days(NULL)"), "NULL");

    // DATE_ADD/DATE_SUB with INTERVAL n DAY: exact day arithmetic, so
    // month/year rollover and leap days are handled correctly for free.
    assert_eq!(
        e("date_add('2021-01-01', interval 5 day)"),
        "STR:2021-01-06"
    );
    assert_eq!(
        e("date_sub('2021-01-01', interval 5 day)"),
        "STR:2020-12-27"
    );
    assert_eq!(
        e("date_add('2021-01-31', interval 1 day)"),
        "STR:2021-02-01"
    ); // month rollover
    assert_eq!(
        e("date_add('2020-02-28', interval 1 day)"),
        "STR:2020-02-29"
    ); // leap day
    assert_eq!(
        e("date_add('2021-01-01', interval -5 day)"),
        "STR:2020-12-27"
    ); // negative interval = subtraction
       // `date_expr + INTERVAL n unit` / `date_expr - INTERVAL n unit`
       // desugar to `DATE_ADD`/`DATE_SUB` at PARSE time (`tidb-parser`'s
       // own `fold_interval_arith`), so evaluation here needs no new
       // logic at all -- confirmed end-to-end (not just restore-checked)
       // against `gorun`.
    assert_eq!(e("'2020-01-01' + interval 5 day"), "STR:2020-01-06");
    assert_eq!(e("'2020-01-01' - interval 5 day"), "STR:2019-12-27");
    // A time-of-day suffix is preserved verbatim in the output.
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval 5 day)"),
        "STR:2021-01-06 10:30:00"
    );
    assert_eq!(e("date_add('not a date', interval 5 day)"), "NULL");
    assert_eq!(e("date_add(NULL, interval 5 day)"), "NULL");
    assert_eq!(e("date_add('2021-01-01', interval NULL day)"), "NULL");
    // A decimal interval value rounds to the nearest day, ties away
    // from zero (matching Decimal::round_to_i64's existing rule).
    assert_eq!(
        e("date_add('2021-01-10', interval 5.5 day)"),
        "STR:2021-01-16"
    );
    assert_eq!(
        e("date_add('2021-01-10', interval -5.5 day)"),
        "STR:2021-01-04"
    );
    // DAY's year-range boundary: a computed year of exactly 0 is the
    // "zero date" string; any other out-of-range year is NULL. A real
    // bug in an earlier version of this function never checked this.
    assert_eq!(e("date_add('9999-12-31', interval 1 day)"), "NULL");
    assert_eq!(
        e("date_add('0001-01-01', interval -1 day)"),
        "STR:0000-00-00"
    );
    assert_eq!(e("date_add('0001-01-01', interval -367 day)"), "NULL");

    // DATE_ADD/DATE_SUB with INTERVAL n MONTH/YEAR: calendar-field
    // arithmetic, clamping the day to the target month's own length
    // rather than overflowing into the next month.
    assert_eq!(
        e("date_add('2021-01-31', interval 1 month)"),
        "STR:2021-02-28"
    );
    // The clamp applies once against the FINAL target month, not
    // iteratively re-clamped one month at a time (would give 03-28).
    assert_eq!(
        e("date_add('2021-01-31', interval 2 month)"),
        "STR:2021-03-31"
    );
    assert_eq!(
        e("date_add('2020-01-31', interval 1 month)"),
        "STR:2020-02-29"
    ); // leap year: clamps to 29, not 28
    assert_eq!(
        e("date_add('2021-01-31', interval -1 month)"),
        "STR:2020-12-31"
    );
    assert_eq!(
        e("date_add('2020-02-29', interval 1 year)"),
        "STR:2021-02-28"
    ); // leap day, target year not leap
    assert_eq!(
        e("date_add('2021-01-31 10:30:00', interval 1 month)"),
        "STR:2021-02-28 10:30:00"
    );
    assert_eq!(e("date_add('2021-01-31', interval NULL month)"), "NULL");
    // A decimal amount rounds to the nearest whole unit first, ties
    // away from zero, then the (rounded) calendar arithmetic applies.
    assert_eq!(
        e("date_add('2021-01-31', interval 1.5 month)"),
        "STR:2021-03-31"
    );
    // MONTH/YEAR's year-range boundary: the same rule as DAY's.
    assert_eq!(e("date_add('9999-12-01', interval 1 month)"), "NULL");
    assert_eq!(
        e("date_add('0001-02-01', interval -2 month)"),
        "STR:0000-00-00"
    );
    assert_eq!(e("date_add('0003-06-15', interval -4 year)"), "NULL");

    // DATE_ADD/DATE_SUB with INTERVAL n WEEK: exact day arithmetic,
    // WEEK being DAY with the (already-rounded) amount pre-multiplied
    // by 7.
    assert_eq!(
        e("date_add('2021-01-01', interval 1 week)"),
        "STR:2021-01-08"
    );
    assert_eq!(
        e("date_sub('2021-01-01', interval 1 week)"),
        "STR:2020-12-25"
    );
    // A fractional WEEK amount rounds to the nearest whole WEEK FIRST,
    // then multiplies by 7 (not the reverse order: round(1.5*7)=11
    // would give Jan 12, not Jan 15).
    assert_eq!(
        e("date_add('2021-01-01', interval 1.5 week)"),
        "STR:2021-01-15"
    );
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval 1 week)"),
        "STR:2021-01-08 10:30:00"
    );
    assert_eq!(e("date_add('2021-01-01', interval NULL week)"), "NULL");
    assert_eq!(e("date_add('9999-12-25', interval 1 week)"), "NULL");

    // DATE_ADD/DATE_SUB with INTERVAL n HOUR/MINUTE/SECOND: unlike
    // DAY/WEEK/MONTH/YEAR, these ALWAYS render a time-of-day
    // component, treating a DATE-only input as midnight.
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval 5 hour)"),
        "STR:2021-01-01 15:30:00"
    );
    assert_eq!(
        e("date_add('2021-01-01', interval 5 hour)"),
        "STR:2021-01-01 05:00:00"
    );
    // Overflow carries into the day (and, via civil_from_days, month).
    assert_eq!(
        e("date_add('2021-01-01 22:00:00', interval 5 hour)"),
        "STR:2021-01-02 03:00:00"
    );
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval -15 hour)"),
        "STR:2020-12-31 19:30:00"
    );
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval 5 minute)"),
        "STR:2021-01-01 10:35:00"
    );
    assert_eq!(
        e("date_add('2021-01-01 10:30:45', interval 20 second)"),
        "STR:2021-01-01 10:31:05"
    ); // second->minute carry
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval 1.5 hour)"),
        "STR:2021-01-01 12:30:00"
    );
    assert_eq!(
        e("date_add('2021-01-01 10:30:00', interval NULL hour)"),
        "NULL"
    );
    assert_eq!(
        e("date_add('9999-12-31 23:00:00', interval 2 hour)"),
        "NULL"
    );
    // The zero-date special case replaces ONLY the date portion; the
    // computed time still shows through.
    assert_eq!(
        e("date_add('0001-01-01 00:00:00', interval -1 hour)"),
        "STR:0000-00-00 23:00:00"
    );
    assert_eq!(
        e("date_add('0001-01-01', interval -1 second)"),
        "STR:0000-00-00 23:59:59"
    );
}

#[test]
fn hour_minute_second() {
    // Real TiDB's own two-path algorithm (confirmed via `goeval`, not
    // assumed), depending on whether the argument contains a `:`.
    //
    // WITH a `:`: an optional `[DATE ]` prefix followed by a required
    // `H:M[:S]` time-of-day (`S` defaults to `0`). `H` may exceed 23 --
    // TiDB's `TIME` domain is elapsed-time, not wall-clock.
    assert_eq!(e("hour('10:30:45')"), "INT:10");
    assert_eq!(e("minute('10:30:45')"), "INT:30");
    assert_eq!(e("second('10:30:45')"), "INT:45");
    assert_eq!(e("hour('2024-01-15 10:30:45')"), "INT:10");
    assert_eq!(e("minute('2024-01-15 10:30:45')"), "INT:30");
    assert_eq!(e("hour('100:30:45')"), "INT:100"); // elapsed time, not wall-clock
    assert_eq!(e("hour('10:30')"), "INT:10"); // seconds default to 0
    assert_eq!(e("second('10:30')"), "INT:0");
    assert_eq!(e("hour('1:2:3')"), "INT:1"); // single-digit components
    assert_eq!(e("hour(' 10:30:45 ')"), "INT:10"); // whitespace trimmed
                                                   // A negative sign is stripped before parsing -- HOUR/MINUTE/SECOND
                                                   // always return a NON-NEGATIVE magnitude, never the sign itself.
    assert_eq!(e("hour('-10:30:45')"), "INT:10");
    assert_eq!(e("minute('-10:30:45')"), "INT:30");
    // A JUNK date-like prefix invalidates the WHOLE value, not just
    // gets ignored.
    assert_eq!(e("hour('junk 10:30:45')"), "NULL");
    // `838:59:59` is TiDB's real documented `TIME` maximum; an `H`
    // exceeding it clamps the WHOLE value to exactly `838:59:59` --
    // not just the hour component -- even when `M`/`S` were
    // individually valid.
    assert_eq!(e("hour('838:59:59')"), "INT:838");
    assert_eq!(e("hour('900:30:15')"), "INT:838");
    assert_eq!(e("minute('900:30:15')"), "INT:59"); // NOT 30
    assert_eq!(e("second('900:30:15')"), "INT:59"); // NOT 15
                                                    // An out-of-range `M`/`S` invalidates the WHOLE value, regardless
                                                    // of `H`'s own magnitude.
    assert_eq!(e("hour('839:70:80')"), "NULL");
    assert_eq!(e("minute('839:70:80')"), "NULL");
    assert_eq!(e("second('839:70:80')"), "NULL");
    assert_eq!(e("hour(NULL)"), "NULL");
    assert_eq!(e("hour('not a time')"), "NULL");

    // WITHOUT a `:` (including a bare `DATE`-only value, NOT `0` --
    // a genuinely surprising real behavior): the value's OWN leading
    // digit run decodes as a right-aligned HHMMSS number, the SAME
    // rule an integer literal already uses.
    assert_eq!(e("hour(103045)"), "INT:10");
    assert_eq!(e("hour(-103045)"), "INT:10"); // sign stripped here too
    assert_eq!(e("minute(-103045)"), "INT:30");
    assert_eq!(e("hour(103045.789)"), "INT:10"); // fractional part truncated
    assert_eq!(e("second(103045.789)"), "INT:45");
    // A bare DATE (no `:` at all) takes ONLY its leading digit run
    // ('2024', stopping at the first non-digit '-') -- NOT the
    // calendar date's own values.
    assert_eq!(e("hour('2024-01-15')"), "INT:0");
    assert_eq!(e("minute('2024-01-15')"), "INT:20");
    assert_eq!(e("second('2024-01-15')"), "INT:24");
    assert_eq!(e("minute('2024-02-15')"), "INT:20"); // same leading run, rest ignored
    assert_eq!(e("hour('12')"), "INT:0");
    assert_eq!(e("minute('12')"), "INT:0");
    assert_eq!(e("second('12')"), "INT:12");
    assert_eq!(e("hour('12abc')"), "INT:0"); // digit run stops at the first non-digit
    assert_eq!(e("hour('abc123')"), "NULL"); // must START with a digit
    assert_eq!(e("hour('-')"), "NULL"); // sign with no digits at all
    assert_eq!(e("hour('')"), "NULL");
    // The SAME `0..=59`-for-`M`/`S`-or-invalid rule applies to the
    // decoded digit run too.
    assert_eq!(e("hour(999999999)"), "NULL");
    assert_eq!(e("minute(999999999)"), "NULL");
    assert_eq!(e("second(999999999)"), "NULL");
}

#[test]
fn extract() {
    // `EXTRACT(unit FROM expr)` is sugar for calling the SAME
    // single-argument function `unit` already names -- every simple
    // unit this project's evaluator already supports as a standalone
    // function works identically through `EXTRACT`.
    assert_eq!(e("extract(year from '2024-03-15')"), "INT:2024");
    assert_eq!(e("extract(month from '2024-03-15')"), "INT:3");
    assert_eq!(e("extract(day from '2024-03-15')"), "INT:15");
    assert_eq!(e("extract(quarter from '2024-03-15')"), "INT:1");
    assert_eq!(e("extract(hour from '2024-03-15 10:30:45')"), "INT:10");
    assert_eq!(e("extract(minute from '2024-03-15 10:30:45')"), "INT:30");
    assert_eq!(e("extract(second from '2024-03-15 10:30:45')"), "INT:45");
    // NULL propagates, same as an ordinary function call.
    assert_eq!(e("extract(year from NULL)"), "NULL");
    // The unit keyword is case-insensitive (lexed as an ordinary
    // keyword token, then canonically uppercased by the parser).
    assert_eq!(e("extract(YeAr from '2024-03-15')"), "INT:2024");
    // `WEEK` is now a differentially verified standalone function, so its
    // EXTRACT spelling takes the exact same path. (The no-mode spelling uses
    // the evaluator's documented default-week-format capability boundary.)
    assert_eq!(e("extract(week from '2024-03-15')"), "INT:10");
    // A compound unit remains unsupported until it has a faithful standalone
    // evaluator rather than being silently mis-evaluated by a partial parser.
    assert_eq!(
        e("extract(day_hour from '2024-03-15 10:30:45')"),
        "Unsupported(\"unsupported function\")"
    );
}

/// `CAST(... AS type)` / `CONVERT(...)` evaluation — one `(expr, want)`
/// pair per rule confirmed via `goeval` (see `crate::cast`'s own doc for
/// the rules themselves); table-driven since there's no shared setup
/// between cases, unlike most of this file's other tests.
#[test]
fn cast_and_convert() {
    let cases: &[(&str, &str)] = &[
        ("cast('123' as signed)", "INT:123"),
        ("cast(1.5 as signed)", "INT:2"),
        ("cast(1.9 as signed)", "INT:2"),
        ("cast(-1.9 as signed)", "INT:-2"),
        ("cast('abc' as signed)", "INT:0"),
        ("cast(NULL as signed)", "NULL"),
        ("cast(-1 as unsigned)", "UINT:18446744073709551615"),
        ("cast(1 as unsigned)", "UINT:1"),
        ("cast(-1.5 as unsigned)", "UINT:0"),
        ("cast('123.45' as decimal)", "DEC:123"),
        ("cast('123.45' as decimal(10,2))", "DEC:123.45"),
        ("cast(1 as decimal(5,2))", "DEC:1.00"),
        ("cast(123 as char)", "STR:123"),
        ("cast(123.45 as char)", "STR:123.45"),
        ("cast('  123  ' as char)", "STR:  123  "),
        ("cast(1 as char(1))", "STR:1"),
        ("cast('hello' as char(3))", "STR:hel"),
        ("cast('2021-01-01' as date)", "STR:2021-01-01"),
        ("cast('2021-01-01 10:30:00' as date)", "STR:2021-01-01"),
        ("cast('2021-01-01' as datetime)", "STR:2021-01-01 00:00:00"),
        ("cast('not a date' as date)", "NULL"),
        ("cast(NULL as date)", "NULL"),
        ("cast('2021-01-01' as year)", "INT:2021"),
        ("cast(2021 as year)", "INT:2021"),
        ("cast('99' as year)", "INT:99"),
        ("cast(1 as double)", "FLOAT:1"),
        ("cast('1.5' as double)", "FLOAT:1.5"),
        ("cast(1 as float)", "FLOAT:1"),
        ("cast(1 as binary)", "STR:1"),
        ("cast('hi' as binary(5))", "STR:hi\0\0\0"),
        // `TestCastFunctions` truncates BINARY by bytes (`str[:5]`), not
        // UTF-8 characters.  The fifth byte lands inside `好`; the raw
        // result remains observable through Datum's lossless hex label.
        ("cast('你好world' as binary(5))", "STR_HEX:E4BDA0E5A5"),
        ("cast('hi' as binary)", "STR:hi"),
        ("cast(123 as binary)", "STR:123"),
        ("convert('123', signed)", "INT:123"),
        ("convert('hello' using utf8)", "STR:hello"),
        ("cast(true as signed)", "INT:1"),
        ("cast(3.5 as decimal)", "DEC:4"),
        ("cast(-5 as unsigned)", "UINT:18446744073709551611"),
        ("cast(-100 as unsigned)", "UINT:18446744073709551516"),
        ("cast('-5' as unsigned)", "UINT:18446744073709551611"),
        ("cast('hi' as char(5))", "STR:hi"),
        // `CHAR(N) CHARSET binary` restores identically to `BINARY(N)`
        // (confirmed via `godump restore`), but does NOT evaluate the
        // same way — it stays a plain truncating `CHAR` cast (no
        // right-padding), confirmed directly via `goeval`: `LENGTH(CAST(
        // 'hi' AS CHAR(5) CHARSET binary))` is `2`, not `5`. `charset` is
        // ignored entirely at evaluation time (see `crate::cast`'s own
        // `CastType::Char` arm), so this is really just re-confirming
        // `cast('hi' as char(5))`'s own behavior above under a
        // charset-qualified spelling that could easily be mistaken for
        // `binary(5)`'s padding behavior instead.
        ("cast('hi' as char(5) charset binary)", "STR:hi"),
        ("cast(99 as year)", "INT:99"),
        ("cast(0 as year)", "INT:0"),
        ("cast(2000 as year)", "INT:2000"),
        ("cast(123456 as decimal(5,2))", "DEC:999.99"),
        ("cast(123.456 as decimal(5,2))", "DEC:123.46"),
        ("cast(-123.456 as decimal(5,2))", "DEC:-123.46"),
        ("cast('  42abc' as signed)", "INT:42"),
        ("cast('   4.5e1  ' as signed)", "INT:4"),
        ("cast(1e300 as signed)", "INT:9223372036854775807"),
        ("cast(1 as unsigned) + 1", "UINT:2"),
        (
            "cast('9223372036854775807' as unsigned) + 1",
            "UINT:9223372036854775808",
        ),
        ("cast(1.5 as decimal)", "DEC:2"),
        ("cast(-1 as char)", "STR:-1"),
        ("cast(-5 as year)", "INT:-5"),
        ("cast(2.5e0 as signed)", "INT:2"),
        ("cast(-2.5e0 as signed)", "INT:-2"),
        ("cast(0.5e0 as unsigned)", "UINT:0"),
        ("cast('3.5abc' as decimal)", "DEC:4"),
        ("cast('3.5e1abc' as double)", "FLOAT:35"),
        ("cast('1e2' as decimal)", "DEC:100"),
        ("cast('10:30:00' as time)", "Unsupported(\"CAST AS TIME\")"),
        ("cast('{}' as json)", "Unsupported(\"CAST AS JSON\")"),
    ];
    for (expr, want) in cases {
        assert_eq!(&e(expr), want, "expr: {expr}");
    }
}

/// `expr COLLATE name` is a pure passthrough — the value is unaffected,
/// unlike `CONVERT ... USING`'s own stringification (see
/// `tidb_ast::Expr::Collate`'s own doc). Confirmed via `gorun`: real TiDB
/// itself treats it identically since this crate models no collation
/// domain at all.
#[test]
fn collate_expr() {
    let cases: &[(&str, &str)] = &[
        ("'a' collate utf8mb4_bin", "STR:a"),
        ("'a' collate utf8mb4_bin = 'a'", "INT:1"),
        (
            "'a' collate utf8mb4_bin collate utf8mb4_general_ci",
            "STR:a",
        ),
    ];
    for (expr, want) in cases {
        assert_eq!(&e(expr), want, "expr: {expr}");
    }
}

/// `expr [NOT] REGEXP pattern` — case-sensitive (`utf8mb4_bin`,
/// matching this crate's own established collation convention), a
/// substring/partial match (no implicit `^`/`$` anchoring), `NULL`
/// from either operand propagates, and a non-string operand is
/// coerced the SAME way `LIKE` already does — all confirmed via
/// `gorun`. See `crate::regexp::regexp_match`'s own doc for the
/// empty-pattern/malformed-pattern error rules, also exercised here.
#[test]
fn regexp_expr_eval() {
    let cases: &[(&str, &str)] = &[
        ("'abc' regexp 'a.c'", "INT:1"),
        ("'ABC' regexp 'a.c'", "INT:0"), // case-sensitive
        ("'abc' regexp 'xyz'", "INT:0"),
        ("'abc' not regexp 'a.c'", "INT:0"),
        ("'abc' not regexp 'xyz'", "INT:1"),
        ("null regexp 'a.c'", "NULL"),
        ("'abc' regexp null", "NULL"),
        ("5 regexp '5'", "INT:1"),             // non-string operand coerced
        ("'abc123' regexp '[0-9]+'", "INT:1"), // substring match, no anchors needed
        ("'hello world' regexp '^hello'", "INT:1"),
        ("'hello world' regexp 'world$'", "INT:1"),
        (
            "'abc' regexp '['",
            "Unsupported(\"invalid regular expression pattern\")",
        ),
        (
            "'abc' regexp ''",
            "Unsupported(\"empty regular expression pattern\")",
        ),
    ];
    for (expr, want) in cases {
        assert_eq!(&e(expr), want, "expr: {expr}");
    }
}

/// The original two-argument `[NOT] REGEXP` function is registered separately
/// from `REGEXP_LIKE` in Go (`pkg/expression/builtin_like_test.go:64
/// TestRegexp`).  Keep every successful source row running through the real
/// parser and `Expr::Regexp` dispatch as well as through the leaf builder
/// tests in `regexp.rs`.
#[test]
fn regexp_source_rows_through_dispatch() {
    let rows: &[(&str, &str)] = &[
        ("'a' regexp '^$'", "INT:0"),
        ("'a' regexp 'a'", "INT:1"),
        ("'b' regexp 'a'", "INT:0"),
        ("'aA' regexp 'aA'", "INT:1"),
        ("'a' regexp '.'", "INT:1"),
        ("'ab' regexp '^.$'", "INT:0"),
        ("'b' regexp '..'", "INT:0"),
        ("'aab' regexp '.ab'", "INT:1"),
        ("'abcd' regexp '.*'", "INT:1"),
        ("'a' not regexp 'a'", "INT:0"),
        ("'a' not regexp 'b'", "INT:1"),
    ];
    for (expr, want) in rows {
        assert_eq!(&e(expr), want, "expression: {expr}");
    }
}

/// `MATCH(col, ...) AGAINST(expr [modifier])` evaluates as `Unsupported` —
/// no fulltext index or scoring is modelled at all (see
/// `tidb_ast::Expr::MatchAgainst`'s own doc for the same "parse/restore
/// fidelity only" boundary `Expr::Regexp` already established).
#[test]
fn match_against_unsupported() {
    let cases: &[(&str, &str)] = &[
        (
            "match(a) against('x')",
            "Unsupported(\"unsupported expression\")",
        ),
        (
            "match(a) against('x' in boolean mode)",
            "Unsupported(\"unsupported expression\")",
        ),
    ];
    for (expr, want) in cases {
        assert_eq!(&e(expr), want, "expr: {expr}");
    }
}
