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

use super::Decimal;

/// Vectors selected from TiDB's `pkg/types/mydecimal_test.go`: preserve
/// literal scale, canonicalize zero, and compare values independent of
/// their rendered scale.
#[test]
fn go_literal_and_comparison_vectors() {
    assert_eq!(Decimal::from_literal("010.500").to_string(), "10.500");
    assert_eq!(
        Decimal::from_literal(".00012345000098765").to_string(),
        "0.00012345000098765"
    );
    assert_eq!(Decimal::from_literal("0.0").negate().to_string(), "0.0");
    assert_eq!(Decimal::from_literal("1.5"), Decimal::from_literal("1.50"));
    assert!(Decimal::from_literal("1.1").negate() > Decimal::from_literal("1.2").negate());
}

/// Exact arithmetic and the division/remainder sign rules correspond to
/// `TestAddMyDecimal`, `TestMulMyDecimal`, and `TestDivModMyDecimal`.
#[test]
fn go_arithmetic_vectors() {
    assert_eq!(
        Decimal::from_literal(".00012345000098765")
            .add(&Decimal::from_literal("123.45"))
            .to_string(),
        "123.45012345000098765"
    );
    assert_eq!(
        Decimal::from_literal("123.456")
            .negate()
            .mul(&Decimal::from_literal("98765.4321"))
            .to_string(),
        "-12193185.1853376"
    );
    let (quotient, remainder) = Decimal::from_literal("3.14")
        .negate()
        .div_rem(&Decimal::from_literal("2"))
        .unwrap();
    assert_eq!(quotient, -1);
    assert_eq!(remainder.to_string(), "-1.14");
    assert_eq!(
        Decimal::from_literal("3.14")
            .true_div(&Decimal::from_literal("2"), 6)
            .unwrap()
            .to_string(),
        "1.570000"
    );
}

/// `DecimalDiv` stores whole base-1e9 fraction words while exposing only
/// `resultFrac`. This vector is the source-observed shape behind
/// `pkg/executor/test/executor_test.go:TestDecimalDivPrecisionIncrement`:
/// each scalar division displays seven digits, but AVG consumes the
/// hidden ninth digit from each operand before exposing fourteen digits.
#[test]
fn division_keeps_word_precision_beyond_display_scale() {
    let eight = Decimal::from_int(8);
    let nine = Decimal::from_int(9);
    let seven = Decimal::from_int(7);
    let first = eight.true_div(&seven, 7).unwrap();
    let second = nine.true_div(&seven, 7).unwrap();
    assert_eq!(first.to_string(), "1.1428571");
    assert_eq!(second.to_string(), "1.2857143");

    let avg = first.add(&second).div_round(2, 14);
    assert_eq!(avg.to_string(), "1.21428571350000");
}

/// `TestRoundWithTruncate` and DECIMAL column coercion both rely on
/// half-away-from-zero rounding before the precision range check.
#[test]
fn go_rounding_and_precision_vectors() {
    assert_eq!(
        Decimal::from_literal("15.5")
            .negate()
            .round_to_scale(0)
            .to_string(),
        "-16"
    );
    assert_eq!(
        Decimal::from_literal("15.9")
            .negate()
            .truncate_to_scale(0)
            .to_string(),
        "-15"
    );
    assert!(Decimal::from_literal("99.995")
        .fit_precision_scale(4, 2)
        .is_none());
    assert_eq!(
        Decimal::from_literal("99.994")
            .fit_precision_scale(4, 2)
            .unwrap()
            .to_string(),
        "99.99"
    );
}

#[test]
fn storage_codec_accessors_preserve_hidden_division_precision() {
    let value = Decimal::from_literal("8")
        .true_div(&Decimal::from_literal("7"), 7)
        .unwrap();

    assert_eq!(value.to_string(), "1.1428571");
    assert_eq!(value.scale(), 7);
    assert_eq!(value.storage_scale(), 9);
    assert_eq!(value.coefficient_digits(), "1142857142");
    assert!(!value.is_negative());
}

use crate::decimal::{decimal_bin_size, DecimalCodecError, DecimalCodecWarning};

/// Parses a signed decimal literal for the codec tests: `from_literal` takes
/// sign-free text (the AST carries negation separately), so strip a leading `-`
/// and negate.
fn parse_signed(input: &str) -> Decimal {
    match input.strip_prefix('-') {
        Some(rest) => Decimal::from_literal(rest).negate(),
        None => Decimal::from_literal(input),
    }
}

fn to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Byte-exact vectors captured from TiDB's own `MyDecimal.ToBin`
/// (`pkg/types/mydecimal_test.go` `TestToBinFromBin` inputs, plus its illegal
/// precision/frac error cases): the Rust codec must emit the identical
/// memcmp-comparable bytes and the same soft truncation/overflow signal.
#[test]
fn go_to_bin_byte_vectors() {
    type Want = Result<Option<DecimalCodecWarning>, DecimalCodecError>;
    let ok = Ok(None);
    let trunc = Ok(Some(DecimalCodecWarning::Truncated));
    let over = Ok(Some(DecimalCodecWarning::Overflow));
    let bad: Want = Err(DecimalCodecError::BadNumber);
    let cases: &[(&str, i32, i32, &str, Want)] = &[
        ("-10.55", 4, 2, "75c8", ok),
        ("0.0123456789012345678912345", 30, 25, "80000000bc614e35b7bf870087fdd9", ok),
        ("12345", 5, 0, "803039", ok),
        ("12345", 10, 3, "800030390000", ok),
        ("123.45", 10, 3, "8000007b01c2", ok),
        ("-123.45", 20, 10, "7fffffff84e52d8b7fff", ok),
        (".00012345000098765", 15, 14, "800001e23a000062", trunc),
        (".00012345000098765", 22, 20, "800001e23a000f120200", ok),
        (".12345000098765", 30, 20, "8000000000075bb2903ade57d000", ok),
        ("-.000000012345000098765", 30, 20, "7ffffffffffffffff3eb6fb75db3", trunc),
        ("1234500009876.5", 30, 5, "80000000000004d21dcd8b9400c350", ok),
        ("111111111.11", 10, 2, "80a98ac70b", over),
        ("000000000.01", 7, 3, "8000000a", ok),
        ("123.4", 10, 2, "8000007b28", ok),
        ("1000", 3, 0, "8000", over),
        ("0.1", 1, 1, "81", ok),
        ("0.100", 1, 1, "81", trunc),
        ("0.1000", 1, 1, "81", trunc),
        ("0.10000", 1, 1, "81", trunc),
        ("0.100000", 1, 1, "81", trunc),
        ("0.1000000", 1, 1, "81", trunc),
        ("0.10", 1, 1, "81", trunc),
        (
            "0000000000000000000000000000000000000000000.000000000000123000000000000000",
            15,
            15,
            "8000000000007b",
            trunc,
        ),
        ("00000000000000000000000000000.00000000000012300", 15, 15, "8000000000007b", trunc),
        (
            "0000000000000000000000000000000000000000000.0000000000001234000000000000000",
            16,
            16,
            "80000000000004d2",
            trunc,
        ),
        ("00000000000000000000000000000.000000000000123400", 16, 16, "80000000000004d2", trunc),
        ("0.1", 2, 2, "8a", ok),
        ("0.10", 3, 3, "8064", ok),
        ("0.1", 3, 1, "8001", ok),
        ("0.0000000000001234", 32, 17, "800000000000000000000000003034", ok),
        ("0.0000000000001234", 20, 20, "800000000001e20800", ok),
        ("1", 82, 1, "", bad),
        ("1", -1, 1, "", bad),
        ("1", 10, 31, "", bad),
        ("1", 10, -1, "", bad),
    ];
    for (input, precision, frac, hex, want) in cases {
        let got = parse_signed(input).to_bin(*precision, *frac);
        match want {
            Ok(expected_warning) => {
                let (bytes, warning) = got
                    .unwrap_or_else(|e| panic!("{input} to_bin({precision},{frac}) failed: {e:?}"));
                assert_eq!(
                    to_hex(&bytes),
                    *hex,
                    "{input} to_bin({precision},{frac}) bytes"
                );
                assert_eq!(
                    warning, *expected_warning,
                    "{input} to_bin({precision},{frac}) warning"
                );
            }
            Err(expected) => {
                assert_eq!(
                    got.map(|(b, w)| (to_hex(&b), w)),
                    Err(*expected),
                    "{input} to_bin({precision},{frac}) should be a hard error"
                );
            }
        }
    }
}

/// `DecimalBinSize` vectors from TiDB `TestDecimalBinSize`.
#[test]
fn go_decimal_bin_size_vectors() {
    assert_eq!(decimal_bin_size(3, 1), Ok(2));
    assert_eq!(decimal_bin_size(-1, 0), Err(DecimalCodecError::BadNumber));
    assert_eq!(decimal_bin_size(3, 5), Err(DecimalCodecError::BadNumber));
}

/// The complete `TestToBinFromBin` round trip from TiDB
/// `pkg/types/mydecimal_test.go`: `FromString -> ToBin -> FromBin -> ToString`
/// must reproduce Go's exact rendered output (and `ToBin`'s soft
/// truncation/overflow), and `FromBin` must not error on any of these.
#[test]
fn go_to_bin_from_bin_round_trip() {
    let none = None;
    let trunc = Some(DecimalCodecWarning::Truncated);
    let over = Some(DecimalCodecWarning::Overflow);
    // (input, precision, frac, rendered output, ToBin warning)
    let cases: &[(&str, i32, i32, &str, Option<DecimalCodecWarning>)] = &[
        ("-10.55", 4, 2, "-10.55", none),
        ("0.0123456789012345678912345", 30, 25, "0.0123456789012345678912345", none),
        ("12345", 5, 0, "12345", none),
        ("12345", 10, 3, "12345.000", none),
        ("123.45", 10, 3, "123.450", none),
        ("-123.45", 20, 10, "-123.4500000000", none),
        (".00012345000098765", 15, 14, "0.00012345000098", trunc),
        (".00012345000098765", 22, 20, "0.00012345000098765000", none),
        (".12345000098765", 30, 20, "0.12345000098765000000", none),
        ("-.000000012345000098765", 30, 20, "-0.00000001234500009876", trunc),
        ("1234500009876.5", 30, 5, "1234500009876.50000", none),
        ("111111111.11", 10, 2, "11111111.11", over),
        ("000000000.01", 7, 3, "0.010", none),
        ("123.4", 10, 2, "123.40", none),
        ("1000", 3, 0, "0", over),
        ("0.1", 1, 1, "0.1", none),
        ("0.100", 1, 1, "0.1", trunc),
        ("0.1000", 1, 1, "0.1", trunc),
        ("0.10000", 1, 1, "0.1", trunc),
        ("0.100000", 1, 1, "0.1", trunc),
        ("0.1000000", 1, 1, "0.1", trunc),
        ("0.10", 1, 1, "0.1", trunc),
        (
            "0000000000000000000000000000000000000000000.000000000000123000000000000000",
            15,
            15,
            "0.000000000000123",
            trunc,
        ),
        ("00000000000000000000000000000.00000000000012300", 15, 15, "0.000000000000123", trunc),
        (
            "0000000000000000000000000000000000000000000.0000000000001234000000000000000",
            16,
            16,
            "0.0000000000001234",
            trunc,
        ),
        ("00000000000000000000000000000.000000000000123400", 16, 16, "0.0000000000001234", trunc),
        ("0.1", 2, 2, "0.10", none),
        ("0.10", 3, 3, "0.100", none),
        ("0.1", 3, 1, "0.1", none),
        ("0.0000000000001234", 32, 17, "0.00000000000012340", none),
        ("0.0000000000001234", 20, 20, "0.00000000000012340000", none),
    ];
    for (input, precision, frac, output, tobin_warn) in cases {
        let (bytes, warn) = parse_signed(input)
            .to_bin(*precision, *frac)
            .unwrap_or_else(|e| panic!("{input} to_bin({precision},{frac}) failed: {e:?}"));
        assert_eq!(warn, *tobin_warn, "{input} to_bin({precision},{frac}) warning");
        let (decoded, _bin_size, from_warn) = Decimal::from_bin(&bytes, *precision, *frac)
            .unwrap_or_else(|e| panic!("{input} from_bin({precision},{frac}) failed: {e:?}"));
        assert_eq!(from_warn, None, "{input} from_bin should not warn");
        assert_eq!(
            decoded.to_string(),
            *output,
            "{input} round-trip render at ({precision},{frac})"
        );
    }

    // TestToBinFromBin errTests: FromBin at illegal precision/frac. The bytes
    // come from ToBin(1,0) of zero; only the FromBin disposition is asserted.
    let (zero_bytes, _) = Decimal::from_literal("0").to_bin(1, 0).unwrap();
    assert!(
        matches!(
            Decimal::from_bin(&zero_bytes, 82, 1),
            Ok((_, _, Some(DecimalCodecWarning::Truncated)))
        ),
        "from_bin(_, 82, 1) should soft-truncate"
    );
    for (prec, frac) in [(-1, 1), (10, 31), (10, -1)] {
        assert_eq!(
            Decimal::from_bin(&zero_bytes, prec, frac).map(|_| ()),
            Err(DecimalCodecError::BadNumber),
            "from_bin(_, {prec}, {frac}) should be a hard error"
        );
    }
}

/// TiDB `TestRoundWithHalfEven` (which actually passes `ModeHalfUp` — round
/// half away from zero) from `pkg/types/mydecimal_test.go`. Exercises
/// `round_to_scale` across positive, zero, and negative scales, including full
/// carry propagation (`999999999` at scale -9 grows to `1000000000`).
#[test]
fn go_round_half_up_vectors() {
    let cases: &[(&str, i32, &str)] = &[
        ("123456789.987654321", 1, "123456790.0"),
        ("15.1", 0, "15"),
        ("15.5", 0, "16"),
        ("15.9", 0, "16"),
        ("-15.1", 0, "-15"),
        ("-15.5", 0, "-16"),
        ("-15.9", 0, "-16"),
        ("15.1", 1, "15.1"),
        ("-15.1", 1, "-15.1"),
        ("15.17", 1, "15.2"),
        ("15.4", -1, "20"),
        ("-15.4", -1, "-20"),
        ("5.4", -1, "10"),
        (".999", 0, "1"),
        ("999999999", -9, "1000000000"),
    ];
    for (input, scale, output) in cases {
        assert_eq!(
            parse_signed(input).round_to_scale(*scale).to_string(),
            *output,
            "round_to_scale({scale}) of {input}"
        );
    }
}

/// TiDB `TestRoundWithTruncate` (`ModeTruncate`) from
/// `pkg/types/mydecimal_test.go`. Exercises `truncate_to_scale` across positive,
/// zero, and negative scales.
///
/// `TestRoundWithCeil` (`ModeCeiling`) is deliberately NOT ported: the Go test
/// carries a `//TODO:fix me` and asserts `-15.1 -> -16`, i.e. Go's `ModeCeiling`
/// rounds the MAGNITUDE away from zero for negatives (a known Go bug), whereas
/// the Rust `ceil_floor` implements true ceiling (`-15.1 -> -15`, toward +inf,
/// the correct SQL `CEILING` semantics). Porting the Go vectors would encode
/// that bug; surfaced here rather than silently skipped. (Go `ModeCeiling` also
/// takes an arbitrary scale, which `ceil_floor` — scale-0 only — does not yet
/// model; a general ceiling-round is a separate follow-up if a caller needs it.)
#[test]
fn go_round_truncate_vectors() {
    let cases: &[(&str, i32, &str)] = &[
        ("123456789.987654321", 1, "123456789.9"),
        ("15.1", 0, "15"),
        ("15.5", 0, "15"),
        ("15.9", 0, "15"),
        ("-15.1", 0, "-15"),
        ("-15.5", 0, "-15"),
        ("-15.9", 0, "-15"),
        ("15.1", 1, "15.1"),
        ("-15.1", 1, "-15.1"),
        ("15.17", 1, "15.1"),
        ("15.4", -1, "10"),
        ("-15.4", -1, "-10"),
        ("5.4", -1, "0"),
        (".999", 0, "0"),
        ("999999999", -9, "0"),
    ];
    for (input, scale, output) in cases {
        assert_eq!(
            parse_signed(input).truncate_to_scale(*scale).to_string(),
            *output,
            "truncate_to_scale({scale}) of {input}"
        );
    }
}

/// TiDB `TestMulMyDecimal`. `Decimal::mul` matches Go `DecimalMul` on every
/// in-range product (result scale = frac_a + frac_b, exact digits).
///
/// The two boundary vectors of the Go test are an INTENTIONAL, surfaced
/// divergence, not a bug: the Rust `Decimal` is arbitrary-precision and keeps
/// the EXACT product (clamping to a `DECIMAL(p,s)` column happens later at
/// storage — see `fit_precision_scale`), whereas Go's bounded nine-word
/// `MyDecimal` clamps inside `Mul` itself, so it reports `ErrTruncated` (a
/// tiny*huge product past 30 fraction digits) / `ErrOverflow` (a product past
/// 81 integer digits) and returns a clamped value. Both reach the same stored
/// SQL result after column coercion; only the raw primitive differs. The
/// overflow direction is asserted concretely below.
#[test]
fn go_mul_vectors() {
    let cases: &[(&str, &str, &str)] = &[
        ("12", "10", "120"),
        ("-123.456", "98765.4321", "-12193185.1853376"),
        ("-123456000000", "98765432100000", "-12193185185337600000000000"),
        ("123456", "987654321", "121931851853376"),
        ("123456", "9876543210", "1219318518533760"),
        ("123", "0.01", "1.23"),
        ("123", "0", "0"),
        ("0.5999991229316", "0.918755041726043", "0.5512522192246113614062276588"),
        ("0.5999991229317", "0.918755041726042", "0.5512522192247026369112773314"),
        ("0.000", "-1", "0.000"),
    ];
    for (a, b, product) in cases {
        assert_eq!(
            parse_signed(a).mul(&parse_signed(b)).to_string(),
            *product,
            "{a} * {b}"
        );
    }
    // Arbitrary-precision divergence from Go's bounded `Mul`: 10^60 * 10^60
    // yields the exact 10^120 rather than Go's clamped `ErrOverflow` -> "0".
    let big = format!("1{}", "0".repeat(60));
    let exact = format!("1{}", "0".repeat(120));
    assert_eq!(
        parse_signed(&big).mul(&parse_signed(&big)).to_string(),
        exact,
        "10^60 * 10^60 is the exact 10^120 (Go's bounded Mul reports ErrOverflow)"
    );
}

/// `Decimal::round_to_u64_saturating` — the `CAST(... AS UNSIGNED)` decimal path,
/// `Round(0, ModeHalfUp)` then Go `MyDecimal.ToUint`. Its defining property over
/// the signed path is that magnitudes in `(i64::MAX, u64::MAX]` — the upper half
/// of `UNSIGNED BIGINT` — survive instead of saturating at `i64::MAX`.
#[test]
fn go_round_to_u64_unsigned_cast_vectors() {
    // In-range positives, rounded half away from zero (ModeHalfUp).
    assert_eq!(Decimal::from_literal("5").round_to_u64_saturating(), 5);
    assert_eq!(Decimal::from_literal("5.4").round_to_u64_saturating(), 5);
    assert_eq!(Decimal::from_literal("5.6").round_to_u64_saturating(), 6);
    assert_eq!(Decimal::from_literal("2.5").round_to_u64_saturating(), 3);
    assert_eq!(Decimal::from_literal("0.5").round_to_u64_saturating(), 1);
    assert_eq!(Decimal::from_literal("0").round_to_u64_saturating(), 0);

    // The upper half of UNSIGNED BIGINT, which the old i64-routed path lost:
    // i64::MAX is 9223372036854775807; u64::MAX is 18446744073709551615.
    assert_eq!(
        Decimal::from_literal("9223372036854775808").round_to_u64_saturating(),
        9_223_372_036_854_775_808,
        "one past i64::MAX is kept, not saturated down to i64::MAX"
    );
    assert_eq!(
        Decimal::from_literal("10000000000000000000").round_to_u64_saturating(),
        10_000_000_000_000_000_000
    );
    assert_eq!(
        Decimal::from_literal("18446744073709551615").round_to_u64_saturating(),
        u64::MAX
    );

    // Positive overflow past u64::MAX saturates to MaxUint64 (Go ToUint ErrOverflow).
    assert_eq!(
        Decimal::from_literal("18446744073709551616").round_to_u64_saturating(),
        u64::MAX
    );
    assert_eq!(
        Decimal::from_literal("99999999999999999999999999").round_to_u64_saturating(),
        u64::MAX
    );

    // A negative value is ToUint's ErrOverflow, which the cast reports as 0 —
    // whether it rounds to a nonzero magnitude or down to zero. (`from_literal`
    // takes sign-free text, so a negative is built with `negate`.)
    assert_eq!(
        Decimal::from_literal("5.4").negate().round_to_u64_saturating(),
        0
    );
    assert_eq!(
        Decimal::from_literal("5.6").negate().round_to_u64_saturating(),
        0
    );
    assert_eq!(
        Decimal::from_literal("2.5").negate().round_to_u64_saturating(),
        0
    );
    assert_eq!(
        Decimal::from_literal("0.4").negate().round_to_u64_saturating(),
        0
    );
    assert_eq!(
        Decimal::from_literal("99999999999999999999999999")
            .negate()
            .round_to_u64_saturating(),
        0
    );
}
