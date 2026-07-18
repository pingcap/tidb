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
