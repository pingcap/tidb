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

use super::{Decimal, DecimalIntegerWarning, DecimalParseError};

/// Exact source `TestFromInt`, `TestFromUint`, `TestFromFloat`, `TestToInt`,
/// and `TestToUint` conversion tables.
#[test]
fn source_decimal_integer_and_float_conversion_tables() {
    for (input, output) in [
        (-12_345, "-12345"),
        (-1, "-1"),
        (1, "1"),
        (-9_223_372_036_854_775_807, "-9223372036854775807"),
        (i64::MIN, "-9223372036854775808"),
    ] {
        assert_eq!(Decimal::from_int(input).to_string(), output);
    }
    for (input, output) in [
        (12_345, "12345"),
        (0, "0"),
        (u64::MAX, "18446744073709551615"),
    ] {
        assert_eq!(Decimal::from_uint(input).to_string(), output);
    }
    for (input, output, warning) in [
        (
            "18446744073709551615",
            i64::MAX,
            Some(DecimalIntegerWarning::Overflow),
        ),
        ("-1", -1, None),
        ("1", 1, None),
        ("-1.23", -1, Some(DecimalIntegerWarning::Truncated)),
        ("-9223372036854775807", -9_223_372_036_854_775_807, None),
        ("-9223372036854775808", i64::MIN, None),
        (
            "9223372036854775808",
            i64::MAX,
            Some(DecimalIntegerWarning::Overflow),
        ),
        (
            "-9223372036854775809",
            i64::MIN,
            Some(DecimalIntegerWarning::Overflow),
        ),
    ] {
        assert_eq!(
            Decimal::from_signed_literal(input).to_i64_trunc(),
            (output, warning),
            "{input}"
        );
    }
    for (input, output, warning) in [
        ("12345", 12_345, None),
        ("0", 0, None),
        ("18446744073709551615", u64::MAX, None),
        (
            "18446744073709551616",
            u64::MAX,
            Some(DecimalIntegerWarning::Overflow),
        ),
        ("-1", 0, Some(DecimalIntegerWarning::Overflow)),
        ("1.23", 1, Some(DecimalIntegerWarning::Truncated)),
        (
            "9999999999999999999999999.000",
            u64::MAX,
            Some(DecimalIntegerWarning::Overflow),
        ),
    ] {
        assert_eq!(
            Decimal::from_signed_literal(input).to_u64_trunc(),
            (output, warning),
            "{input}"
        );
    }
    for (value, output) in [
        (12_345.0, "12345"),
        (123.45, "123.45"),
        (-123.45, "-123.45"),
        (0.000_123_450_000_987_65, "0.00012345000098765"),
        (1_234_500_009_876.5, "1234500009876.5"),
    ] {
        assert_eq!(Decimal::from_f64(value).unwrap().to_string(), output);
    }
}

/// Exact source `TestMaxDecimal` and `TestMaxOrMinMyDecimal` rows.
#[test]
fn source_max_decimal_tables() {
    for (precision, frac, output) in [
        (1, 1, "0.9"),
        (1, 0, "9"),
        (2, 1, "9.9"),
        (4, 2, "99.99"),
        (6, 3, "999.999"),
        (8, 4, "9999.9999"),
        (10, 5, "99999.99999"),
        (12, 6, "999999.999999"),
        (14, 7, "9999999.9999999"),
        (16, 8, "99999999.99999999"),
        (18, 9, "999999999.999999999"),
        (20, 10, "9999999999.9999999999"),
        (20, 20, "0.99999999999999999999"),
        (20, 0, "99999999999999999999"),
        (40, 20, "99999999999999999999.99999999999999999999"),
    ] {
        assert_eq!(
            Decimal::max_or_min(false, precision, frac).to_string(),
            output
        );
    }
    for (negative, precision, frac, output) in [
        (true, 2, 1, "-9.9"),
        (false, 1, 1, "0.9"),
        (true, 1, 0, "-9"),
        (false, 0, 0, "0"),
        (false, 4, 2, "99.99"),
    ] {
        assert_eq!(
            Decimal::max_or_min(negative, precision, frac).to_string(),
            output
        );
    }
}

/// Exact source `TestMarshalMyDecimal` cases using Go's word-buffer JSON
/// persistence shape rather than a rendered decimal string.
#[test]
fn test_marshal_my_decimal() {
    for input in [
        "12345",
        "12345.",
        ".00012345000098765",
        ".12345000098765",
        "-.000000012345000098765",
        "123E-2",
    ] {
        let expanded = crate::convert_scientific_notation(input).unwrap();
        let original = Decimal::from_signed_literal(&expanded);
        let encoded = original.mysql_json_value();
        let decoded = Decimal::from_mysql_json_value(&encoded).unwrap();
        assert_eq!(original, decoded, "{input}: {encoded}");
        let object = encoded.as_object().unwrap();
        assert!(object.get("DigitsInt").is_some());
        assert!(object.get("DigitsFrac").is_some());
        assert!(object.get("ResultFrac").is_some());
        assert_eq!(object.get("WordBuf").unwrap().as_array().unwrap().len(), 9);
    }
}

/// Complete equivalence groups from source `TestToHashKey`, including the
/// `ToHashKey` prefix versus `ToBin(PrecisionAndFrac)` relation.
#[test]
fn test_to_hash_key() {
    let groups: &[&[&str]] = &[
        &[
            "1.1",
            "1.1000",
            "1.1000000",
            "1.10000000000",
            "01.1",
            "0001.1",
            "001.1000000",
        ],
        &[
            "-1.1",
            "-1.1000",
            "-1.1000000",
            "-1.10000000000",
            "-01.1",
            "-0001.1",
            "-001.1000000",
        ],
        &[
            ".1",
            "0.1",
            "0.10",
            "000000.1",
            ".10000",
            "0000.10000",
            "000000000000000000.1",
        ],
        &[
            "0",
            "0000",
            ".0",
            ".00000",
            "00000.00000",
            "-0",
            "-0000",
            "-.0",
            "-.00000",
            "-00000.00000",
        ],
        &[
            ".123456789123456789",
            ".1234567891234567890",
            ".12345678912345678900",
            ".123456789123456789000",
            ".1234567891234567890000",
            "0.123456789123456789",
            ".1234567891234567890000000000",
            "0000000.123456789123456789000",
        ],
        &[
            "12345",
            "012345",
            "0012345",
            "0000012345",
            "0000000012345",
            "00000000000012345",
            "12345.",
            "12345.00",
            "12345.000000000",
            "000012345.0000",
        ],
        &[
            "123E5",
            "12300000",
            "00123E5",
            "000000123E5",
            "12300000.00000000",
        ],
        &[
            "123E-2",
            "1.23",
            "00000001.23",
            "1.2300000000000000",
            "000000001.23000000000000",
        ],
    ];
    for group in groups {
        let keys: Vec<Vec<u8>> = group
            .iter()
            .map(|input| {
                let expanded = crate::convert_scientific_notation(input).unwrap();
                Decimal::from_signed_literal(&expanded)
                    .to_hash_key()
                    .unwrap()
                    .0
            })
            .collect();
        assert!(keys.iter().all(|key| key == &keys[0]), "{group:?}");
    }

    for group in groups {
        for input in *group {
            let expanded = crate::convert_scientific_notation(input).unwrap();
            let decimal = Decimal::from_signed_literal(&expanded);
            let (hash_key, warning) = decimal.to_hash_key().unwrap();
            assert_eq!(warning, None, "{input}");
            assert_eq!(decimal.hash_key_size().unwrap(), hash_key.len(), "{input}");
            let (precision, frac) = decimal.precision_and_frac();
            assert!(!decimal.to_bin(precision, frac).unwrap().0.is_empty());
        }
    }
}

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
        (
            "0.0123456789012345678912345",
            30,
            25,
            "80000000bc614e35b7bf870087fdd9",
            ok,
        ),
        ("12345", 5, 0, "803039", ok),
        ("12345", 10, 3, "800030390000", ok),
        ("123.45", 10, 3, "8000007b01c2", ok),
        ("-123.45", 20, 10, "7fffffff84e52d8b7fff", ok),
        (".00012345000098765", 15, 14, "800001e23a000062", trunc),
        (".00012345000098765", 22, 20, "800001e23a000f120200", ok),
        (
            ".12345000098765",
            30,
            20,
            "8000000000075bb2903ade57d000",
            ok,
        ),
        (
            "-.000000012345000098765",
            30,
            20,
            "7ffffffffffffffff3eb6fb75db3",
            trunc,
        ),
        (
            "1234500009876.5",
            30,
            5,
            "80000000000004d21dcd8b9400c350",
            ok,
        ),
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
        (
            "00000000000000000000000000000.00000000000012300",
            15,
            15,
            "8000000000007b",
            trunc,
        ),
        (
            "0000000000000000000000000000000000000000000.0000000000001234000000000000000",
            16,
            16,
            "80000000000004d2",
            trunc,
        ),
        (
            "00000000000000000000000000000.000000000000123400",
            16,
            16,
            "80000000000004d2",
            trunc,
        ),
        ("0.1", 2, 2, "8a", ok),
        ("0.10", 3, 3, "8064", ok),
        ("0.1", 3, 1, "8001", ok),
        (
            "0.0000000000001234",
            32,
            17,
            "800000000000000000000000003034",
            ok,
        ),
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
        (
            "0.0123456789012345678912345",
            30,
            25,
            "0.0123456789012345678912345",
            none,
        ),
        ("12345", 5, 0, "12345", none),
        ("12345", 10, 3, "12345.000", none),
        ("123.45", 10, 3, "123.450", none),
        ("-123.45", 20, 10, "-123.4500000000", none),
        (".00012345000098765", 15, 14, "0.00012345000098", trunc),
        (".00012345000098765", 22, 20, "0.00012345000098765000", none),
        (".12345000098765", 30, 20, "0.12345000098765000000", none),
        (
            "-.000000012345000098765",
            30,
            20,
            "-0.00000001234500009876",
            trunc,
        ),
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
        (
            "00000000000000000000000000000.00000000000012300",
            15,
            15,
            "0.000000000000123",
            trunc,
        ),
        (
            "0000000000000000000000000000000000000000000.0000000000001234000000000000000",
            16,
            16,
            "0.0000000000001234",
            trunc,
        ),
        (
            "00000000000000000000000000000.000000000000123400",
            16,
            16,
            "0.0000000000001234",
            trunc,
        ),
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
        assert_eq!(
            warn, *tobin_warn,
            "{input} to_bin({precision},{frac}) warning"
        );
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

/// Exact port of TiDB `TestRoundWithCeil`. The source's current
/// `ModeCeiling` behavior rounds discarded magnitude away from zero, including
/// the documented negative-value bug; transcreation preserves that behavior.
#[test]
fn go_round_with_ceil() {
    let cases: &[(&str, i32, &str)] = &[
        ("123456789.987654321", 1, "123456790.0"),
        ("15.1", 0, "16"),
        ("15.5", 0, "16"),
        ("15.9", 0, "16"),
        ("-15.1", 0, "-16"),
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
            parse_signed(input)
                .round_ceiling_to_scale(*scale)
                .to_string(),
            *output,
            "round_ceiling_to_scale({scale}) of {input}"
        );
    }
}

/// Exact TiDB `TestMulMyDecimal`, including the fixed nine-word buffer's
/// truncation and overflow outcomes.
#[test]
fn go_mul_vectors() {
    let cases: &[(&str, &str, &str, Option<DecimalCodecWarning>)] = &[
        ("12", "10", "120", None),
        ("-123.456", "98765.4321", "-12193185.1853376", None),
        (
            "-123456000000",
            "98765432100000",
            "-12193185185337600000000000",
            None,
        ),
        ("123456", "987654321", "121931851853376", None),
        ("123456", "9876543210", "1219318518533760", None),
        ("123", "0.01", "1.23", None),
        ("123", "0", "0", None),
        (
            "-0.0000000000000000000000000000000000000000000000000017382578996420603",
            "-13890436710184412000000000000000000000000000000000000000000000000000000000000",
            "0.000000000000000000000000000000",
            Some(DecimalCodecWarning::Truncated),
        ),
        (
            "1000000000000000000000000000000000000000000000000000000000000",
            "1000000000000000000000000000000000000000000000000000000000000",
            "0",
            Some(DecimalCodecWarning::Overflow),
        ),
        (
            "0.5999991229316",
            "0.918755041726043",
            "0.5512522192246113614062276588",
            None,
        ),
        (
            "0.5999991229317",
            "0.918755041726042",
            "0.5512522192247026369112773314",
            None,
        ),
        ("0.000", "-1", "0.000", None),
    ];
    for (a, b, product, warning) in cases {
        let (actual, actual_warning) = parse_signed(a).mul_mysql(&parse_signed(b));
        assert_eq!(actual_warning, *warning, "{a} * {b} warning");
        assert_eq!(actual.to_string(), *product, "{a} * {b}");
    }
}

/// Exact source `TestShiftMyDecimal`, including the temporary two-word buffer
/// cases that distinguish truncation from overflow.
#[test]
fn test_shift_my_decimal() {
    for (input, shift, output) in [
        ("123.123", 1, "1231.23"),
        ("123457189.123123456789000", 1, "1234571891.23123456789"),
        ("123457189.123123456789000", 8, "12345718912312345.6789"),
        ("123457189.123123456789000", 9, "123457189123123456.789"),
        ("123457189.123123456789000", 10, "1234571891231234567.89"),
        (
            "123457189.123123456789000",
            17,
            "12345718912312345678900000",
        ),
        (
            "123457189.123123456789000",
            18,
            "123457189123123456789000000",
        ),
        (
            "123457189.123123456789000",
            19,
            "1234571891231234567890000000",
        ),
        (
            "123457189.123123456789000",
            26,
            "12345718912312345678900000000000000",
        ),
        (
            "123457189.123123456789000",
            27,
            "123457189123123456789000000000000000",
        ),
        (
            "123457189.123123456789000",
            28,
            "1234571891231234567890000000000000000",
        ),
        ("123", 1, "1230"),
        ("123", 10, "1230000000000"),
        (".123", 1, "1.23"),
        (".123", 10, "1230000000"),
        (".123", 14, "12300000000000"),
        ("000.000", 1000, "0"),
        ("123.123", -1, "12.3123"),
        (
            "123987654321.123456789000",
            -14,
            "0.00123987654321123456789",
        ),
    ] {
        let decimal = Decimal::from_signed_literal(input);
        let (shifted, warning) = decimal.shift_mysql(shift);
        assert_eq!(warning, None, "{input} shift {shift}");
        assert_eq!(shifted.to_string(), output, "{input} shift {shift}");
    }
    let decimal = Decimal::from_int(1);
    let (shifted, warning) = decimal.shift_mysql(1000);
    assert_eq!(warning, Some(DecimalCodecWarning::Overflow));
    assert_eq!(shifted, decimal);

    for (input, shift, output, warning) in [
        ("123.123", -2, "1.23123", None),
        ("123.123", -15, "0.000000000000123123", None),
        (
            "123.123",
            -16,
            "0.000000000000012312",
            Some(DecimalCodecWarning::Truncated),
        ),
        (
            "123.123",
            -20,
            "0.000000000000000001",
            Some(DecimalCodecWarning::Truncated),
        ),
        ("123.123", -21, "0", Some(DecimalCodecWarning::Truncated)),
        (".000000000123", 27, "123000000000000000", None),
        (
            ".000000000123",
            28,
            "0.000000000123",
            Some(DecimalCodecWarning::Overflow),
        ),
        (
            "123456789.987654321",
            -1,
            "12345678.998765432",
            Some(DecimalCodecWarning::Truncated),
        ),
        (
            "123456789.987654321",
            -8,
            "1.234567900",
            Some(DecimalCodecWarning::Truncated),
        ),
        ("123456789.987654321", -9, "0.123456789987654321", None),
        (
            "123456789.987654321",
            1,
            "1234567900",
            Some(DecimalCodecWarning::Truncated),
        ),
        ("123456789.987654321", 9, "123456789987654321", None),
        (
            "123456789.987654321",
            10,
            "123456789.987654321",
            Some(DecimalCodecWarning::Overflow),
        ),
    ] {
        let decimal = Decimal::from_signed_literal(input);
        let (shifted, actual_warning) = decimal.shift_mysql_with_word_limit(shift, 2);
        assert_eq!(actual_warning, warning, "{input} shift {shift}");
        assert_eq!(shifted.to_string(), output, "{input} shift {shift}");
    }
}

/// Exact source `TestFromStringMyDecimal`, including exponent best-effort
/// parsing and the test-only one-word buffer.
#[test]
fn test_from_string_my_decimal() {
    for (input, output, error) in [
        ("12345", "12345", None),
        ("12345.", "12345", None),
        (
            "123.45.",
            "123.45",
            Some(DecimalParseError::Truncated),
        ),
        (
            "-123.45.",
            "-123.45",
            Some(DecimalParseError::Truncated),
        ),
        (".00012345000098765", "0.00012345000098765", None),
        (".12345000098765", "0.12345000098765", None),
        (
            "-.000000012345000098765",
            "-0.000000012345000098765",
            None,
        ),
        ("1234500009876.5", "1234500009876.5", None),
        ("123E5", "12300000", None),
        ("123E-2", "1.23", None),
        (
            "1e1073741823",
            "999999999999999999999999999999999999999999999999999999999999999999999999999999999",
            Some(DecimalParseError::Overflow),
        ),
        (
            "-1e1073741823",
            "-999999999999999999999999999999999999999999999999999999999999999999999999999999999",
            Some(DecimalParseError::Overflow),
        ),
        (
            "1e18446744073709551620",
            "0",
            Some(DecimalParseError::BadNumber),
        ),
        ("1e", "1", Some(DecimalParseError::Truncated)),
        ("1e001", "10", None),
        ("1e00", "1", None),
        ("1eabc", "1", Some(DecimalParseError::Truncated)),
        ("1e 1dddd ", "10", Some(DecimalParseError::Truncated)),
        ("1e - 1", "1", Some(DecimalParseError::Truncated)),
        ("1e -1", "0.1", None),
        (
            "0.00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
            "0.000000000000000000000000000000000000000000000000000000000000000000000000",
            Some(DecimalParseError::Truncated),
        ),
        ("1asf", "1", Some(DecimalParseError::Truncated)),
        ("1.1.1.1.1", "1.1", Some(DecimalParseError::Truncated)),
        ("1  1", "1", Some(DecimalParseError::Truncated)),
        ("1  ", "1", None),
    ] {
        let (decimal, actual_error) = Decimal::parse_mysql(input);
        assert_eq!(actual_error, error, "{input}");
        assert_eq!(decimal.to_string(), output, "{input}");
    }

    for (input, output, error) in [
        (
            "123450000098765",
            "98765",
            Some(DecimalParseError::Overflow),
        ),
        (
            "123450.000098765",
            "123450",
            Some(DecimalParseError::Truncated),
        ),
    ] {
        let (decimal, actual_error) = Decimal::parse_mysql_with_word_limit(input, 1);
        assert_eq!(actual_error, error, "{input}");
        assert_eq!(decimal.to_string(), output, "{input}");
    }
}

/// Complete source `TestAddMyDecimal`, `TestSubMyDecimal`, and
/// `TestDivModMyDecimal` row sets.
#[test]
fn source_bounded_add_sub_div_mod_tables() {
    for (left, right, output) in [
        (".00012345000098765", "123.45", "123.45012345000098765"),
        (".1", ".45", "0.55"),
        (
            "1234500009876.5",
            ".00012345000098765",
            "1234500009876.50012345000098765",
        ),
        ("9999909999999.5", ".555", "9999910000000.055"),
        ("99999999", "1", "100000000"),
        ("989999999", "1", "990000000"),
        ("999999999", "1", "1000000000"),
        ("12345", "123.45", "12468.45"),
        ("-12345", "-123.45", "-12468.45"),
        ("-12345", "123.45", "-12221.55"),
        ("12345", "-123.45", "12221.55"),
        ("123.45", "-12345", "-12221.55"),
        ("-123.45", "12345", "12221.55"),
        ("5", "-6.0", "-1.0"),
        ("-1234.1234", "1234.1234", "0.0000"),
    ] {
        let (actual, warning) = parse_signed(left).add_mysql(&parse_signed(right));
        assert_eq!(warning, None, "{left} + {right}");
        assert_eq!(actual.to_string(), output, "{left} + {right}");
    }
    let large_left = format!("2{}", "1".repeat(71));
    let large_right = "8".repeat(81);
    let large_output = format!("8888888890{}", "9".repeat(71));
    let (actual, warning) =
        Decimal::from_literal(&large_left).add_mysql(&Decimal::from_literal(&large_right));
    assert_eq!(warning, None);
    assert_eq!(actual.to_string(), large_output);

    for (left, right, output) in [
        (".00012345000098765", "123.45", "-123.44987654999901235"),
        (
            "1234500009876.5",
            ".00012345000098765",
            "1234500009876.49987654999901235",
        ),
        ("9999900000000.5", ".555", "9999899999999.945"),
        ("1111.5551", "1111.555", "0.0001"),
        (".555", ".555", "0.000"),
        ("10000000", "1", "9999999"),
        ("1000001000", ".1", "1000000999.9"),
        ("1000000000", ".1", "999999999.9"),
        ("12345", "123.45", "12221.55"),
        ("-12345", "-123.45", "-12221.55"),
        ("123.45", "12345", "-12221.55"),
        ("-123.45", "-12345", "12221.55"),
        ("-12345", "123.45", "-12468.45"),
        ("12345", "-123.45", "12468.45"),
        ("12.12", "12.12", "0.00"),
    ] {
        let (actual, warning) = parse_signed(left).sub_mysql(&parse_signed(right));
        assert_eq!(warning, None, "{left} - {right}");
        assert_eq!(actual.to_string(), output, "{left} - {right}");
    }

    for (left, right, output) in [
        ("120", "10", "12.000000000"),
        ("123", "0.01", "12300.000000000"),
        ("120", "100000000000.00000", "0.000000001200000000"),
        ("-12193185.1853376", "98765.4321", "-123.456000000000000000"),
        ("121931851853376", "987654321", "123456.000000000"),
        ("0", "987", "0.00000"),
        ("1", "3", "0.333333333"),
        ("1.000000000000", "3", "0.333333333333333333"),
        ("1", "1", "1.000000000"),
        (
            "0.0123456789012345678912345",
            "9999999999",
            "0.000000000001234567890246913578148141",
        ),
        ("10.333000000", "12.34500", "0.837019036046982584042122316"),
        ("10.000000000060", "2", "5.000000000030000000"),
        ("51", "0.003430", "14868.804664723032069970"),
    ] {
        let actual = parse_signed(left)
            .div_mysql(&parse_signed(right), 5)
            .unwrap();
        assert_eq!(actual.storage_string(), output, "{left} / {right}");
    }
    assert!(Decimal::from_int(123)
        .div_mysql(&Decimal::from_int(0), 5)
        .is_none());

    for (left, right, output) in [
        ("1", "1", "1.0000"),
        ("1.00", "1", "1.000000"),
        ("1", "1.000", "1.0000"),
        ("2", "3", "0.6667"),
        ("51", "0.003430", "14868.8047"),
        ("0.000", "0.1", "0.0000000"),
    ] {
        let actual = parse_signed(left)
            .div_mysql(&parse_signed(right), 4)
            .unwrap();
        assert_eq!(actual.to_string(), output, "{left} / {right}");
    }

    for (left, right, output) in [
        ("234", "10", "4"),
        ("234.567", "10.555", "2.357"),
        ("-234.567", "10.555", "-2.357"),
        ("234.567", "-10.555", "2.357"),
        ("99999999999999999999999999999999999999", "3", "0"),
        ("51", "0.003430", "0.002760"),
        ("0.0000000001", "1.0", "0.0000000001"),
        ("0.000", "0.1", "0.000"),
        ("1", "2.0", "1.0"),
        ("1.0", "2", "1.0"),
        ("2.23", "3", "2.23"),
    ] {
        let actual = parse_signed(left).rem_mysql(&parse_signed(right)).unwrap();
        assert_eq!(actual.to_string(), output, "{left} % {right}");
    }
}

/// Complete source `TestToFloat` input table. Rust and Go both require the
/// nearest IEEE-754 binary64 value, so numeric equality is the contract.
#[test]
fn test_to_float() {
    for (input, expected) in [
        ("12345", "12345"),
        ("123.45", "123.45"),
        ("-123.45", "-123.45"),
        ("0.00012345000098765", "0.00012345000098765"),
        ("1234500009876.5", "1234500009876.5"),
        ("1e39", "1e39"),
        ("1e-39", "1e-39"),
        ("1e00", "1"),
        ("1e001", "10"),
        ("-9223372036854775807", "-9223372036854775807"),
        ("-9223372036854775808", "-9223372036854775808"),
        ("18446744073709551615", "18446744073709551615"),
        ("123456789.987654321", "123456789.987654321"),
        ("1", "1"),
        ("+1", "1"),
        ("1e23", "1e+23"),
        ("1E23", "1e+23"),
        ("100000000000000000000000", "1e+23"),
        ("123456700", "1.234567e+08"),
        ("99999999999999974834176", "9.999999999999997e+22"),
        ("100000000000000000000001", "1.0000000000000001e+23"),
        ("100000000000000008388608", "1.0000000000000001e+23"),
        ("100000000000000016777215", "1.0000000000000001e+23"),
        ("100000000000000016777216", "1.0000000000000003e+23"),
        ("-1", "-1"),
        ("-0.1", "-0.1"),
        ("-0", "-0"),
        ("1e-20", "1e-20"),
        ("625e-3", "0.625"),
        ("0", "0"),
        ("22.222222222222222", "22.22222222222222"),
        (
            "1.00000000000000011102230246251565404236316680908203125",
            "1",
        ),
        (
            "1.00000000000000011102230246251565404236316680908203124",
            "1",
        ),
        (
            "1.00000000000000011102230246251565404236316680908203126",
            "1.0000000000000002",
        ),
        (
            "1.00000000000000033306690738754696212708950042724609375",
            "1.0000000000000004",
        ),
        ("1090544144181609348671888949248", "1.0905441441816093e+30"),
        ("1090544144181609348835077142190", "1.0905441441816094e+30"),
    ] {
        let (decimal, error) = Decimal::parse_mysql(input);
        assert_eq!(error, None, "{input}");
        assert_eq!(
            decimal.to_f64(),
            expected.parse::<f64>().unwrap(),
            "{input}"
        );
    }
}

#[test]
fn source_from_parquet_array_two_complement_and_scale() {
    let mut positive = [0x04, 0xd2];
    let (decimal, warning) = Decimal::from_parquet_array(&mut positive, 2);
    assert_eq!(warning, None);
    assert_eq!(decimal.to_string(), "12.34");
    assert_eq!(positive, [0x04, 0xd2]);

    let mut negative = [0xfb, 0x2e];
    let (decimal, warning) = Decimal::from_parquet_array(&mut negative, 2);
    assert_eq!(warning, None);
    assert_eq!(decimal.to_string(), "-12.34");
    assert_eq!(negative, [0x04, 0xd2]);

    let mut integer = [0x04, 0xd2];
    let (decimal, warning) = Decimal::from_parquet_array(&mut integer, -2);
    assert_eq!(warning, None);
    assert_eq!(decimal.to_string(), "123400");
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
        Decimal::from_literal("5.4")
            .negate()
            .round_to_u64_saturating(),
        0
    );
    assert_eq!(
        Decimal::from_literal("5.6")
            .negate()
            .round_to_u64_saturating(),
        0
    );
    assert_eq!(
        Decimal::from_literal("2.5")
            .negate()
            .round_to_u64_saturating(),
        0
    );
    assert_eq!(
        Decimal::from_literal("0.4")
            .negate()
            .round_to_u64_saturating(),
        0
    );
    assert_eq!(
        Decimal::from_literal("99999999999999999999999999")
            .negate()
            .round_to_u64_saturating(),
        0
    );
}
