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

//! Focused tests for translated `pkg/expression/builtin_math.go` behavior.

use super::e;
use crate::math_fn::{conv, conv_valid_prefix, crc32};
use crate::Datum;

fn call_conv(n: Datum, from: i64, to: i64) -> Datum {
    conv(&[n, Datum::Int(from), Datum::Int(to)]).expect("CONV must evaluate")
}

/// Exact helper and evaluator vectors from `TestConv` in
/// `pkg/expression/builtin_math_test.go`.
#[test]
fn conv_matches_go_prefix_and_base_semantics() {
    assert_eq!(conv_valid_prefix("-123456D1f", 5), "-1234");
    assert_eq!(conv_valid_prefix("+12azD", 16), "12a");
    assert_eq!(conv_valid_prefix("+", 12), "");

    let cases = [
        ("a", 16, 2, Datum::new_string("1010".to_string())),
        ("6E", 18, 8, Datum::new_string("172".to_string())),
        ("-17", 10, -18, Datum::new_string("-H".to_string())),
        (
            "-17",
            10,
            18,
            Datum::new_string("2D3FGB0B9CG4BD1H".to_string()),
        ),
        ("+18aZ", 7, 36, Datum::new_string("1".to_string())),
        (
            "18446744073709551615",
            -10,
            16,
            Datum::new_string("7FFFFFFFFFFFFFFF".to_string()),
        ),
        ("12F", -10, 16, Datum::new_string("C".to_string())),
        ("  FF ", 16, 10, Datum::new_string("255".to_string())),
        ("TIDB", 10, 8, Datum::new_string("0".to_string())),
        ("aa", 10, 2, Datum::new_string("0".to_string())),
        (" A", -10, 16, Datum::new_string("0".to_string())),
        ("a6a", 10, 8, Datum::new_string("0".to_string())),
        ("a6a", 1, 8, Datum::Null),
    ];
    for (n, from, to, want) in cases {
        assert_eq!(call_conv(Datum::new_string(n.to_string()), from, to), want);
    }
    assert_eq!(call_conv(Datum::Null, 10, 10), Datum::Null);
}

/// UTF-8 source vectors from `pkg/expression/builtin_math_test.go`'s
/// `TestCRC32`. The GBK-only vectors belong to the executor charset domain,
/// which this scalar `String` value intentionally does not model.
#[test]
fn crc32_matches_go_utf8_source_vectors() {
    let cases = [
        (Datum::new_string("".to_string()), 0),
        (Datum::Int(-1), 808_273_962),
        (Datum::new_string("-1".to_string()), 808_273_962),
        (Datum::new_string("mysql".to_string()), 2_501_908_538),
        (Datum::new_string("MySQL".to_string()), 3_259_397_556),
        (Datum::new_string("hello".to_string()), 907_060_870),
        (Datum::new_string("一二三".to_string()), 1_785_250_883),
        (Datum::new_string("一".to_string()), 2_416_838_398),
    ];
    for (input, want) in cases {
        assert_eq!(crc32(&[input]), Ok(Datum::UInt(want)));
    }
    assert_eq!(crc32(&[Datum::Null]), Ok(Datum::Null));
}

/// Full scalar vector from `pkg/expression/builtin_math_test.go:35`
/// `TestAbs`, including the distinct unsigned signature.
#[test]
fn abs_source_vectors_preserve_uint() {
    for (expr, want) in [
        ("abs(null)", "NULL"),
        ("abs(1)", "INT:1"),
        ("abs(cast(1 as unsigned))", "UINT:1"),
        ("abs(-1)", "INT:1"),
        ("abs(3.14e0)", "FLOAT:3.14"),
        ("abs(-3.14e0)", "FLOAT:3.14"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

/// Source edge from `builtinAbsIntSig.evalInt`: the signed minimum has no
/// positive `BIGINT` representation, so Go returns `ErrOverflow` instead of
/// wrapping back to the same negative value.  This row is not present in the
/// original `TestAbs` table, but is part of the source implementation's
/// observable contract and guards the Rust `checked_abs` boundary.
#[test]
fn abs_signed_minimum_reports_overflow() {
    assert_eq!(e("abs(-9223372036854775808)"), "IntOverflow");
}

/// Exact representable source table from `TestSign` in
/// `pkg/expression/builtin_math_test.go:642`.  The Go signature always
/// returns a signed integer, while its argument builder still selects the
/// real coercion path for strings.  Keep every source row here so a future
/// change cannot preserve only the already-covered string-prefix examples
/// while dropping `NULL`, fractional values, or the `UInt64` boundary.
#[test]
fn sign_matches_go_source_table() {
    for (expr, want) in [
        ("sign(null)", "NULL"),
        ("sign(1)", "INT:1"),
        ("sign(0)", "INT:0"),
        ("sign(-1)", "INT:-1"),
        ("sign(0.4e0)", "INT:1"),
        ("sign(-0.4e0)", "INT:-1"),
        ("sign('1')", "INT:1"),
        ("sign('-1')", "INT:-1"),
        ("sign('1a')", "INT:1"),
        ("sign('-1a')", "INT:-1"),
        ("sign('a')", "INT:0"),
        ("sign(cast(9223372036854775808 as unsigned))", "INT:1"),
    ] {
        assert_eq!(e(expr), want, "{expr}");
    }
}

#[test]
fn math_functions() {
    assert_eq!(e("sqrt(4)"), "FLOAT:2");
    assert_eq!(e("sqrt(null)"), "NULL");
    assert_eq!(e("pow(2, 10)"), "FLOAT:1024");
    assert_eq!(e("power(2, 10)"), "FLOAT:1024");
    assert_eq!(e("exp(0)"), "FLOAT:1");
    assert_eq!(e("ln(1)"), "FLOAT:0");
    assert_eq!(e("log(10)"), "FLOAT:2.302585092994046"); // LOG(x), one arg, is LN
    assert_eq!(e("log(2, 8)"), "FLOAT:3"); // LOG(base, x), two args
    assert_eq!(e("log2(8)"), "FLOAT:3");
    assert_eq!(e("log10(100)"), "FLOAT:2");
    assert_eq!(e("pi()"), "FLOAT:3.141592653589793");
    assert_eq!(e("pi(1)"), "Unsupported(\"bad function arity\")");
    // SQRT/LN/LOG/LOG2/LOG10 return NULL for an out-of-domain
    // argument — MySQL's own explicit domain check (confirmed via
    // goeval, not assumed) — the OPPOSITE failure mode from POW/EXP
    // below.
    assert_eq!(e("sqrt(-1)"), "NULL");
    assert_eq!(e("ln(0)"), "NULL");
    assert_eq!(e("ln(-1)"), "NULL");
    assert_eq!(e("log2(-1)"), "NULL");
    assert_eq!(e("log10(0)"), "NULL");
    assert_eq!(e("log(1, 8)"), "NULL"); // log base 1 is undefined
    assert_eq!(e("log(-2, 8)"), "NULL"); // negative base
    assert_eq!(e("log(2, -8)"), "NULL"); // negative x
                                         // POW/EXP have no such domain check; a NaN (complex) or
                                         // overflowing result is instead a genuine evaluation ERROR —
                                         // this is the one case the differential corpus can't itself
                                         // assert (`ERR` goldens are skipped, not compared), so it's
                                         // covered directly here.
    assert_eq!(e("pow(-2, 0.5)"), "FloatOverflow"); // sqrt(-2), NaN
    assert_eq!(e("pow(2, 2000)"), "FloatOverflow"); // overflows to infinity
    assert_eq!(e("pow(0, -1)"), "FloatOverflow"); // 1/0, also infinity
    assert_eq!(e("exp(1000)"), "FloatOverflow");
    assert_eq!(e("exp(-1000)"), "FLOAT:0"); // underflow to zero is fine
    assert_eq!(e("pow(0, 0)"), "FLOAT:1"); // not an error, matches f64::powf
}

/// Exact scalar result/error vectors from `TestExp` in
/// `pkg/expression/builtin_math_test.go`. The production test also counts the
/// ETReal truncation warning for `EXP('tidb')`; this value-only evaluator has
/// no statement warning channel, but must still return the same numeric value.
#[test]
fn exp_matches_go_source_vectors_and_arity() {
    for (sql, want) in [
        ("exp(null)", "NULL"),
        ("exp(1)", "FLOAT:2.718281828459045"),
        ("exp(1.23e0)", "FLOAT:3.4212295362896734"),
        ("exp(-1.23e0)", "FLOAT:0.2922925776808594"),
        ("exp(0)", "FLOAT:1"),
        ("exp('0')", "FLOAT:1"),
        ("exp('tidb')", "FLOAT:1"),
        ("exp(-1000)", "FLOAT:0"),
        ("exp(100000)", "FloatOverflow"),
        ("exp(1, 2)", "Unsupported(\"bad function arity\")"),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
}

#[test]
fn trig_functions() {
    assert_eq!(e("sin(0)"), "FLOAT:0");
    assert_eq!(e("cos(0)"), "FLOAT:1");
    assert_eq!(e("tan(0)"), "FLOAT:0");
    assert_eq!(e("asin(1)"), "FLOAT:1.5707963267948966");
    assert_eq!(e("acos(1)"), "FLOAT:0");
    assert_eq!(e("atan(1)"), "FLOAT:0.7853981633974483");
    assert_eq!(e("cot(1)"), "FLOAT:0.6420926159343308");
    assert_eq!(e("radians(180)"), e("pi()"));
    assert_eq!(e("degrees(pi())"), "FLOAT:180");
    // ASIN/ACOS return NULL outside [-1, 1] — MySQL's own explicit
    // domain check (confirmed via goeval, not assumed), mirroring
    // SQRT's own — the OPPOSITE failure mode from COT below.
    assert_eq!(e("asin(2)"), "NULL");
    assert_eq!(e("asin(-2)"), "NULL");
    assert_eq!(e("acos(2)"), "NULL");
    // ATAN(y, x) (2 args) is exactly ATAN2(y, x) — same argument
    // order, confirmed via goeval, not assumed.
    assert_eq!(e("atan(1, 2)"), e("atan2(1, 2)"));
    // COT/TAN etc. have no explicit domain check; a genuine division
    // by zero (`COT(0)` is `1/tan(0)` = `1/0`) is instead a real
    // evaluation ERROR through the same `finite_float` check POW/EXP
    // already use — this is the one case the differential corpus
    // can't itself assert (`ERR` goldens are skipped, not compared),
    // so it's covered directly here.
    assert_eq!(e("cot(0)"), "FloatOverflow");
}

/// Full scalar result table from the transcendental portions of
/// `pkg/expression/builtin_math_test.go` (`TestDegrees` through `TestCot`).
///
/// The Go tests also assert statement warning counts for malformed string
/// prefixes.  This constant-expression evaluator has no warning channel, so
/// those rows are still checked for their value while the warning side effect
/// remains an explicit boundary in the coverage evidence.  Keeping the
/// source rows here (instead of a handful of representative calls) is
/// important: several of the functions differ only at NULL/domain and
/// negative-angle boundaries.
fn assert_source_math_value(sql: &str, want: &str) {
    if let Some(want_float) = want.strip_prefix("FLOAT:") {
        let got = e(sql);
        let got_float = got
            .strip_prefix("FLOAT:")
            .unwrap_or_else(|| panic!("{sql}: expected float result, got {got}"));
        let got_float = got_float
            .parse::<f64>()
            .unwrap_or_else(|_| panic!("{sql}: invalid Rust float result {got}"));
        let want_float = want_float
            .parse::<f64>()
            .unwrap_or_else(|_| panic!("{sql}: invalid source float oracle {want}"));
        assert_eq!(got_float, want_float, "{sql}: got {got}, want {want}");
    } else {
        assert_eq!(e(sql), want, "{sql}");
    }
}

#[test]
fn transcendental_source_vectors() {
    for (sql, want) in [
        // TestDegrees
        ("degrees(null)", "NULL"),
        ("degrees(0)", "FLOAT:0"),
        ("degrees(1)", "FLOAT:57.29577951308232"),
        ("degrees(1e0)", "FLOAT:57.29577951308232"),
        ("degrees(3.141592653589793e0)", "FLOAT:180"),
        ("degrees(-1.5707963267948966e0)", "FLOAT:-90"),
        ("degrees('')", "FLOAT:0"),
        ("degrees('-2')", "FLOAT:-114.59155902616465"),
        ("degrees('abc')", "FLOAT:0"),
        ("degrees('+1abc')", "FLOAT:57.29577951308232"),
        // TestSqrt
        ("sqrt(null)", "NULL"),
        ("sqrt(1)", "FLOAT:1"),
        ("sqrt(4e0)", "FLOAT:2"),
        ("sqrt('4')", "FLOAT:2"),
        ("sqrt('9')", "FLOAT:3"),
        ("sqrt('-16')", "NULL"),
        // TestPi
        ("pi()", "FLOAT:3.141592653589793"),
        // TestRadians
        ("radians(null)", "NULL"),
        ("radians(0)", "FLOAT:0"),
        ("radians(180e0)", "FLOAT:3.141592653589793"),
        ("radians(-360)", "FLOAT:-6.283185307179586"),
        ("radians('180')", "FLOAT:3.141592653589793"),
        ("radians(1e308)", "FLOAT:1.7453292519943295e306"),
        ("radians(23)", "FLOAT:0.4014257279586958"),
        ("radians('notNum')", "FLOAT:0"),
        // TestSin
        ("sin(null)", "NULL"),
        ("sin(0)", "FLOAT:0"),
        ("sin(3.141592653589793e0)", "FLOAT:1.2246467991473532e-16"),
        ("sin(-3.141592653589793e0)", "FLOAT:-1.2246467991473532e-16"),
        ("sin(1.5707963267948966e0)", "FLOAT:1"),
        ("sin(-1.5707963267948966e0)", "FLOAT:-1"),
        ("sin(0.5235987755982988e0)", "FLOAT:0.49999999999999994"),
        ("sin(-0.5235987755982988e0)", "FLOAT:-0.49999999999999994"),
        ("sin(6.283185307179586e0)", "FLOAT:-2.4492935982947064e-16"),
        ("sin('adfsdfgs')", "FLOAT:0"),
        ("sin('0.000')", "FLOAT:0"),
        // TestCos
        ("cos(null)", "NULL"),
        ("cos(0)", "FLOAT:1"),
        ("cos(3.141592653589793e0)", "FLOAT:-1"),
        ("cos(-3.141592653589793e0)", "FLOAT:-1"),
        ("cos(1.5707963267948966e0)", "FLOAT:6.123233995736766e-17"),
        ("cos(-1.5707963267948966e0)", "FLOAT:6.123233995736766e-17"),
        ("cos('0.000')", "FLOAT:1"),
        ("cos('sdfgsfsdf')", "FLOAT:1"),
        // TestAcos
        ("acos(null)", "NULL"),
        ("acos(1e0)", "FLOAT:0"),
        ("acos(2e0)", "NULL"),
        ("acos(-1e0)", "FLOAT:3.141592653589793"),
        ("acos(-2e0)", "NULL"),
        ("acos('tidb')", "FLOAT:1.5707963267948966"),
        // TestAsin
        ("asin(null)", "NULL"),
        ("asin(1e0)", "FLOAT:1.5707963267948966"),
        ("asin(2e0)", "NULL"),
        ("asin(-1e0)", "FLOAT:-1.5707963267948966"),
        ("asin(-2e0)", "NULL"),
        ("asin('tidb')", "FLOAT:0"),
        // TestAtan
        ("atan(null)", "NULL"),
        ("atan(null, null)", "NULL"),
        ("atan(1e0)", "FLOAT:0.7853981633974483"),
        ("atan(-1e0)", "FLOAT:-0.7853981633974483"),
        ("atan(0e0, -2e0)", "FLOAT:3.141592653589793"),
        ("atan('tidb')", "FLOAT:0"),
        // TestTan
        ("tan(null)", "NULL"),
        ("tan(0)", "FLOAT:0"),
        ("tan(0.7853981633974483e0)", "FLOAT:0.9999999999999999"),
        ("tan(-0.7853981633974483e0)", "FLOAT:-0.9999999999999999"),
        ("tan(2.356194490192345e0)", "FLOAT:-1.0000000000000002"),
        ("tan('0.000')", "FLOAT:0"),
        ("tan('sdfgsdfg')", "FLOAT:0"),
        // TestCot (the source's COT(0) and COT('tidb') error rows are
        // asserted by `trig_functions`; query goldens intentionally omit
        // ERR values).
        ("cot(null)", "NULL"),
        ("cot(-1e0)", "FLOAT:-0.6420926159343308"),
        ("cot(1e0)", "FLOAT:0.6420926159343308"),
        ("cot(0.7853981633974483e0)", "FLOAT:1.0000000000000002"),
        ("cot(1.5707963267948966e0)", "FLOAT:6.123233995736766e-17"),
        ("cot(3.141592653589793e0)", "FLOAT:-8165619676597685"),
    ] {
        assert_source_math_value(sql, want);
    }
}

#[test]
fn ceil_floor() {
    // Int is unchanged; CEIL rounds toward +infinity, FLOOR toward
    // -infinity.
    assert_eq!(e("ceil(3)"), "INT:3");
    assert_eq!(e("ceil(3.14)"), "INT:4");
    assert_eq!(e("ceil(-3.14)"), "INT:-3");
    assert_eq!(e("ceiling(3.14)"), "INT:4"); // alias
    assert_eq!(e("floor(3.14)"), "INT:3");
    assert_eq!(e("floor(-3.14)"), "INT:-4");
    // Go's TestCeil/TestFloor give string arguments their ETReal signature:
    // numeric prefixes are retained, invalid text is zero, and the result
    // remains FLOAT rather than taking the Decimal result path above.
    assert_eq!(e("ceil('1.23')"), "FLOAT:2");
    assert_eq!(e("ceil('-1.23')"), "FLOAT:-1");
    assert_eq!(e("ceil('tidb')"), "FLOAT:0");
    assert_eq!(e("ceil('1tidb')"), "FLOAT:1");
    // Go's float64 table rows select the real signature even when the
    // numeric value is written with a decimal point.  Keep those rows
    // separate from the SQL DECIMAL literals above so the result domain
    // cannot silently regress to INT/DECIMAL.
    assert_eq!(e("ceil(1.23e0)"), "FLOAT:2");
    assert_eq!(e("ceil(-1.23e0)"), "FLOAT:-1");
    assert_eq!(e("floor('1.23')"), "FLOAT:1");
    assert_eq!(e("floor('-1.23')"), "FLOAT:-2");
    assert_eq!(e("floor('-1.b23')"), "FLOAT:-1");
    assert_eq!(e("floor('abce')"), "FLOAT:0");
    assert_eq!(e("floor(1)"), "INT:1");
    assert_eq!(e("floor(1.23e0)"), "FLOAT:1");
    assert_eq!(e("floor(-1.23e0)"), "FLOAT:-2");
    assert_eq!(e("ceil(null)"), "NULL");
    assert_eq!(e("floor(null)"), "NULL");
    assert_eq!(e("ceil(3.00)"), "INT:3"); // already an integer value
    assert_eq!(e("ceil(-0.0e0)"), "FLOAT:-0"); // Float ceil(-0.0) stays -0.0
                                               // A Decimal argument collapses to Int (real MySQL's own BIGINT
                                               // return type, confirmed via goeval, not assumed) — the OPPOSITE
                                               // convention from a Float argument, which stays Float.
    assert_eq!(e("ceil(1.5e2)"), "FLOAT:150");
    assert_eq!(e("ceil(3.7e0)"), "FLOAT:4");
    assert_eq!(e("floor(3.7e0)"), "FLOAT:3");
    // When the exact Decimal ceiling/floor exceeds i64's range, it
    // stays Decimal rather than erroring or silently truncating —
    // computed on the digit string directly, not via f64, so it's
    // exact even here (confirmed via goeval, not assumed).
    assert_eq!(
        e("ceil(99999999999999999999.5)"),
        "DEC:100000000000000000000"
    );
    assert_eq!(
        e("floor(-99999999999999999999.5)"),
        "DEC:-100000000000000000000"
    );
    assert_eq!(e("ceil(9223372036854775807.5)"), "DEC:9223372036854775808");
    // Go's getEvalTp4FloorAndCeil uses the declared integer width, not the
    // rounded value's magnitude: 18 integer digits still return BIGINT,
    // while the 19-digit boundary above remains DECIMAL.
    assert_eq!(e("ceil(999999999999999999.5)"), "INT:1000000000000000000");
    // A scale-zero DECIMAL whose integer part is 19 digits still selects the
    // decimal return signature, even when the exact result fits i64.  This
    // is distinct from the INT literal with the same value above.
    assert_eq!(e("ceil(9223372036854775807.0)"), "DEC:9223372036854775807");
    assert_eq!(
        e("floor(-9223372036854775808.0)"),
        "DEC:-9223372036854775808"
    );
    // TestSign's string vectors prove SIGN uses the same ETReal numeric
    // prefix coercion, not the previous unsupported-string fallback.
    assert_eq!(e("sign('1a')"), "INT:1");
    assert_eq!(e("sign('-1a')"), "INT:-1");
    assert_eq!(e("sign('a')"), "INT:0");
}

#[test]
fn round_truncate() {
    // NULL propagates from either argument.
    assert_eq!(e("round(null)"), "NULL");
    assert_eq!(e("round(3.14, null)"), "NULL");
    assert_eq!(e("truncate(null, 2)"), "NULL");
    // Int stays Int for the 1-arg form (a plain passthrough); TRUNCATE
    // has no 1-arg form at all.
    assert_eq!(e("round(5)"), "INT:5");
    assert_eq!(e("truncate(5)"), "Unsupported(\"bad function arity\")");
    // Decimal NEVER collapses to Int (unlike CEIL/FLOOR) and rounds
    // ties AWAY from zero.
    assert_eq!(e("round(3.14159)"), "DEC:3");
    assert_eq!(e("round(2.5)"), "DEC:3");
    assert_eq!(e("round(-2.5)"), "DEC:-3");
    // Float rounds ties TO EVEN -- the OPPOSITE tie-breaking rule from
    // Decimal for the exact same numeric value.
    assert_eq!(e("round(2.5e0)"), "FLOAT:2");
    assert_eq!(e("round(3.5e0)"), "FLOAT:4");
    assert_eq!(e("round(-2.5e0)"), "FLOAT:-2");
    // A negative `d` rounds/truncates into the integer part.
    assert_eq!(e("round(12345, -2)"), "INT:12300");
    assert_eq!(e("truncate(12345, 2)"), "INT:12345"); // d >= 0: no-op on Int
    assert_eq!(e("truncate(-12345, -2)"), "INT:-12300");
    // Go's builtinTruncateIntSig/builtinTruncateUintSig treat an unsigned
    // scale as an already-nonnegative value and return the input unchanged;
    // this is distinct from a signed negative scale that zeroes digits.
    assert_eq!(e("truncate(12345, cast(2 as unsigned))"), "INT:12345");
    assert_eq!(
        e("truncate(cast(12345 as unsigned), cast(2 as unsigned))"),
        "UINT:12345"
    );
    assert_eq!(
        e("truncate(12345, cast(18446744073709551615 as unsigned))"),
        "INT:12345"
    );
    assert_eq!(e("round(3.14159, -1)"), "DEC:0"); // rounds past every digit
                                                  // ROUND rounds the first cut digit; TRUNCATE always drops it.
    assert_eq!(e("round(3.14159, 2)"), "DEC:3.14");
    assert_eq!(e("truncate(3.999, 0)"), "DEC:3");
    // A positive `d` clamps to DECIMAL's max scale (30), matching real
    // MySQL -- confirmed via goeval, not assumed.
    assert_eq!(
        e("round(3.14159, 100)"),
        "DEC:3.141590000000000000000000000000"
    );
    // A non-integer scale argument is out of this evaluator's scope.
    assert_eq!(
        e("round(3.14, 2.5)"),
        "Unsupported(\"non-integer scale argument\")"
    );
}

/// Complete value/error table from `pkg/expression/builtin_math_test.go:247
/// TestLog`, including both arities, domain NULLs, and MySQL numeric-prefix
/// coercion.  TiDB records conversion/domain warnings in the Go statement
/// context; this value-only ring deliberately claims only the returned value.
#[test]
fn log_source_vectors() {
    for (sql, want) in [
        ("log(null)", "NULL"),
        ("log(null, null)", "NULL"),
        ("log(100)", "FLOAT:4.605170185988092"),
        ("log(100e0)", "FLOAT:4.605170185988092"),
        ("log(10, 100)", "FLOAT:2"),
        ("log(10e0, 100e0)", "FLOAT:2"),
        ("log(-1e0)", "NULL"),
        ("log(2e0, -1e0)", "NULL"),
        ("log(-1e0, 2e0)", "NULL"),
        ("log(1e0, 2e0)", "NULL"),
        ("log(0.5e0, 0.25e0)", "FLOAT:2"),
        ("log('abc')", "NULL"),
    ] {
        assert_source_math_value(sql, want);
    }
}

/// Complete scalar table from `TestLog2` (`builtin_math_test.go:290`).
#[test]
fn log2_source_vectors() {
    for (sql, want) in [
        ("log2(null)", "NULL"),
        ("log2(16)", "FLOAT:4"),
        ("log2(16e0)", "FLOAT:4"),
        ("log2(5)", "FLOAT:2.321928094887362"),
        ("log2(-1)", "NULL"),
        ("log2('4abc')", "FLOAT:2"),
        ("log2('abc')", "NULL"),
    ] {
        assert_source_math_value(sql, want);
    }
}

/// Complete scalar table from `TestLog10` (`builtin_math_test.go:328`).
#[test]
fn log10_source_vectors() {
    for (sql, want) in [
        ("log10(null)", "NULL"),
        ("log10(100)", "FLOAT:2"),
        ("log10(100e0)", "FLOAT:2"),
        ("log10(101)", "FLOAT:2.0043213737826426"),
        ("log10(-1)", "NULL"),
        ("log10('100abc')", "FLOAT:2"),
        ("log10('abc')", "NULL"),
    ] {
        assert_source_math_value(sql, want);
    }
}

/// Complete scalar/error table from `TestPow` (`builtin_math_test.go:387`).
/// The source's string rows are warning-producing numeric-prefix coercions;
/// only the resulting value is compared here.  Overflow remains an explicit
/// evaluator error, matching TiDB's `ErrOverflow` path.
#[test]
fn pow_source_vectors() {
    for (sql, want) in [
        ("pow(1, 3)", "FLOAT:1"),
        ("pow(2, 2)", "FLOAT:4"),
        ("pow(4, 0.5e0)", "FLOAT:2"),
        ("pow(4, -2)", "FLOAT:0.0625"),
        ("pow('test', 'test')", "FLOAT:1"),
        ("pow(1, 'test')", "FLOAT:1"),
        ("pow(10, 700)", "FloatOverflow"),
    ] {
        assert_eq!(e(sql), want, "{sql}");
    }
}

/// Value rows from `TestRound` (`builtin_math_test.go:434`).  Go's table uses
/// untyped float64 values for the first group, so the `e0` suffix deliberately
/// selects the Rust `Datum::Real` path; the decimal rows retain the source
/// `MyDecimal` half-up behavior.
#[test]
fn round_source_vectors() {
    for (sql, want) in [
        ("round(-1.23e0)", "FLOAT:-1"),
        ("round(-1.23e0, 0)", "FLOAT:-1"),
        ("round(-1.58e0)", "FLOAT:-2"),
        ("round(1.58e0)", "FLOAT:2"),
        ("round(1.298e0, 1)", "FLOAT:1.3"),
        ("round(1.298e0)", "FLOAT:1"),
        ("round(1.298e0, 0)", "FLOAT:1"),
        ("round(-1.5e0, 0)", "FLOAT:-2"),
        ("round(1.5e0, 0)", "FLOAT:2"),
        ("round(23.298e0, -1)", "FLOAT:20"),
        ("round(-1.23)", "DEC:-1"),
        ("round(-1.23, 1)", "DEC:-1.2"),
        ("round(-1.58)", "DEC:-2"),
        ("round(1.58)", "DEC:2"),
        ("round(1.58, 1)", "DEC:1.6"),
        ("round(23.298, -1)", "DEC:20"),
        ("round(null, 2)", "NULL"),
        ("round(1, -2012)", "INT:0"),
        ("round(1, -201299999999999)", "INT:0"),
    ] {
        assert_source_math_value(sql, want);
    }
}

/// Value rows from `TestTruncate` (`builtin_math_test.go:488`).  The NaN
/// cases in the Go test require a session-created IEEE value and are outside
/// this SQL constant parser; all finite and integer/decimal boundary rows
/// remain executable here.
#[test]
fn truncate_source_vectors() {
    for (sql, want) in [
        ("truncate(-1.23e0, 0)", "FLOAT:-1"),
        ("truncate(1.58e0, 0)", "FLOAT:1"),
        ("truncate(1.298e0, 1)", "FLOAT:1.2"),
        ("truncate(123.2e0, -1)", "FLOAT:120"),
        ("truncate(123.2e0, 100)", "FLOAT:123.2"),
        ("truncate(123.2e0, -100)", "FLOAT:0"),
        (
            "truncate(1.797693134862315708145274237317043567981e+308, 2)",
            "FLOAT:1.7976931348623157e308",
        ),
        ("truncate(-1.23, 0)", "DEC:-1"),
        ("truncate(-1.23, 1)", "DEC:-1.2"),
        ("truncate(-11.23, -1)", "DEC:-10"),
        ("truncate(1.58, 0)", "DEC:1"),
        ("truncate(1.58, 1)", "DEC:1.5"),
        ("truncate(11.58, -1)", "DEC:10"),
        ("truncate(23.298, -1)", "DEC:20"),
        ("truncate(23.298, -100)", "DEC:0"),
        (
            "truncate(23.298, 100)",
            "DEC:23.298000000000000000000000000000",
        ),
        ("truncate(null, 2)", "NULL"),
        (
            "truncate(cast(9223372036854775808 as unsigned), -10)",
            "UINT:9223372030000000000",
        ),
        (
            "truncate(9223372036854775807, -7)",
            "INT:9223372036850000000",
        ),
        (
            "truncate(cast(18446744073709551615 as unsigned), -10)",
            "UINT:18446744070000000000",
        ),
    ] {
        assert_source_math_value(sql, want);
    }
}
