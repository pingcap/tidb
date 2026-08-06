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

//! LOCKDOWN INVENTORY: `pkg/types/vector_functions.go` -> `vector.rs`.
//!
//! This extends the existing `vector.rs` lockdown; it does not reopen or
//! replace the `pkg/types/vector.go` inventory. Every Go function and every
//! control-flow rule in the owning source has one PORTED row below. There are
//! no DECLINED or UNREACHABLE rules in this unit and no source-owned Go test
//! file: repository search finds these operations only in `vector_functions.go`,
//! while `vector_test.go`'s Compare cases are already owned by
//! `vector_inventory.rs`.
//!
//! The source hash is intentionally strict. Any function, branch, arithmetic
//! expression, error order, or comment-level contract change in Go fails this
//! inventory until a reviewer reclassifies it. A separate declaration scan
//! makes missing functions legible, and the symbol gate compiles a reference
//! to every Rust symbol named by a PORTED function row.
//!
//! Go returns `(ZeroVectorFloat32, error)` from failed Add/Sub/Mul operations.
//! Rust's native `Result` carries no value on `Err`, so no partial result can
//! escape; that is the same caller-visible success/error partition rather than
//! a silent omission. Go's open multiplication-underflow note is fully classified:
//! absence of an underflow rejection is source behavior, pinned by a boundary
//! test, and leaves no unclassified Rust work.
//!
//! The multiplication accumulators use `f32::mul_add`, not `sum + x * y`.
//! Direct Go probes on the owning implementation measured fused results for
//! L2 squared distance, inner product, and all three cosine accumulators; the
//! exact witnesses below fail with Rust's separately rounded expression.

use std::cmp::Ordering;

use sha2::{Digest, Sha256};

use crate::vector::{
    deserialize_vector_float32, VectorError, VectorFloat32, CHECK_IDENTICAL_DIMS_SYMBOL,
};

#[allow(dead_code)] // Kept explicit so future DECLINED/UNREACHABLE rows are representable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    Ported,
    Declined,
    Unreachable,
}

type Row = (&'static str, Verdict, &'static str);

const GO_SOURCE_SHA256: &str = "a379639b2f3dce2ec6655962f998b83ff386de9b2046cf1a3230e3891d9f853b";

const FUNCTIONS: &[Row] = &[
    (
        "(a VectorFloat32) checkIdenticalDims",
        Verdict::Ported,
        "VectorFloat32::check_identical_dims",
    ),
    (
        "(a VectorFloat32) L2SquaredDistance",
        Verdict::Ported,
        "VectorFloat32::l2_squared_distance",
    ),
    (
        "(a VectorFloat32) L2Distance",
        Verdict::Ported,
        "VectorFloat32::l2_distance",
    ),
    (
        "(a VectorFloat32) InnerProduct",
        Verdict::Ported,
        "VectorFloat32::inner_product",
    ),
    (
        "(a VectorFloat32) NegativeInnerProduct",
        Verdict::Ported,
        "VectorFloat32::negative_inner_product",
    ),
    (
        "(a VectorFloat32) CosineDistance",
        Verdict::Ported,
        "VectorFloat32::cosine_distance",
    ),
    (
        "(a VectorFloat32) L1Distance",
        Verdict::Ported,
        "VectorFloat32::l1_distance",
    ),
    (
        "(a VectorFloat32) L2Norm",
        Verdict::Ported,
        "VectorFloat32::l2_norm",
    ),
    (
        "(a VectorFloat32) Add",
        Verdict::Ported,
        "VectorFloat32::add",
    ),
    (
        "(a VectorFloat32) Sub",
        Verdict::Ported,
        "VectorFloat32::sub",
    ),
    (
        "(a VectorFloat32) Mul",
        Verdict::Ported,
        "VectorFloat32::mul",
    ),
    (
        "(a VectorFloat32) Compare",
        Verdict::Ported,
        "VectorFloat32::compare",
    ),
];

const BRANCHES: &[Row] = &[
    (
        "check_dims.equal",
        Verdict::Ported,
        "equal_dimensions_succeed",
    ),
    (
        "check_dims.mismatch",
        Verdict::Ported,
        "every_fallible_operation_preserves_dimension_error",
    ),
    (
        "l2_squared.dimension_error",
        Verdict::Ported,
        "every_fallible_operation_preserves_dimension_error",
    ),
    (
        "l2_squared.float32_fused_accumulation",
        Verdict::Ported,
        "source_precision_and_empty_boundaries",
    ),
    (
        "l2_squared.empty",
        Verdict::Ported,
        "source_precision_and_empty_boundaries",
    ),
    (
        "l2.dimension_error",
        Verdict::Ported,
        "every_fallible_operation_preserves_dimension_error",
    ),
    (
        "l2.sqrt_after_squared_distance",
        Verdict::Ported,
        "equal_dimensions_succeed",
    ),
    (
        "inner.dimension_error",
        Verdict::Ported,
        "every_fallible_operation_preserves_dimension_error",
    ),
    (
        "inner.float32_fused_accumulation",
        Verdict::Ported,
        "source_precision_and_empty_boundaries",
    ),
    (
        "negative_inner.dimension_error",
        Verdict::Ported,
        "every_fallible_operation_preserves_dimension_error",
    ),
    (
        "negative_inner.multiply_by_negative_one",
        Verdict::Ported,
        "source_precision_and_empty_boundaries",
    ),
    (
        "cosine.dimension_error",
        Verdict::Ported,
        "every_fallible_operation_preserves_dimension_error",
    ),
    (
        "cosine.float32_fused_dot_and_norms",
        Verdict::Ported,
        "cosine_nan_in_range_and_clamp_boundaries",
    ),
    (
        "cosine.nan_after_zero_division",
        Verdict::Ported,
        "cosine_nan_in_range_and_clamp_boundaries",
    ),
    (
        "cosine.similarity_above_one",
        Verdict::Ported,
        "cosine_nan_in_range_and_clamp_boundaries",
    ),
    (
        "cosine.similarity_below_negative_one",
        Verdict::Ported,
        "cosine_nan_in_range_and_clamp_boundaries",
    ),
    (
        "cosine.similarity_in_range",
        Verdict::Ported,
        "cosine_nan_in_range_and_clamp_boundaries",
    ),
    (
        "l1.dimension_error",
        Verdict::Ported,
        "every_fallible_operation_preserves_dimension_error",
    ),
    (
        "l1.negative_difference",
        Verdict::Ported,
        "l1_sign_and_float32_accumulation_boundaries",
    ),
    (
        "l1.nonnegative_difference",
        Verdict::Ported,
        "l1_sign_and_float32_accumulation_boundaries",
    ),
    (
        "l1.float32_accumulation",
        Verdict::Ported,
        "l1_sign_and_float32_accumulation_boundaries",
    ),
    (
        "l2_norm.float64_accumulation",
        Verdict::Ported,
        "source_precision_and_empty_boundaries",
    ),
    (
        "add.dimension_error",
        Verdict::Ported,
        "every_fallible_operation_preserves_dimension_error",
    ),
    (
        "add.success",
        Verdict::Ported,
        "elementwise_success_boundaries",
    ),
    (
        "add.infinity",
        Verdict::Ported,
        "elementwise_error_boundaries",
    ),
    ("add.nan", Verdict::Ported, "elementwise_error_boundaries"),
    (
        "sub.dimension_error",
        Verdict::Ported,
        "every_fallible_operation_preserves_dimension_error",
    ),
    (
        "sub.success",
        Verdict::Ported,
        "elementwise_success_boundaries",
    ),
    (
        "sub.infinity",
        Verdict::Ported,
        "elementwise_error_boundaries",
    ),
    ("sub.nan", Verdict::Ported, "elementwise_error_boundaries"),
    (
        "mul.dimension_error",
        Verdict::Ported,
        "every_fallible_operation_preserves_dimension_error",
    ),
    (
        "mul.success",
        Verdict::Ported,
        "elementwise_success_boundaries",
    ),
    (
        "mul.infinity",
        Verdict::Ported,
        "elementwise_error_boundaries",
    ),
    ("mul.nan", Verdict::Ported, "elementwise_error_boundaries"),
    (
        "mul.underflow_is_not_rejected",
        Verdict::Ported,
        "elementwise_error_boundaries",
    ),
    (
        "elementwise.infinity_precedes_nan",
        Verdict::Ported,
        "elementwise_error_boundaries",
    ),
    (
        "compare.first_less",
        Verdict::Ported,
        "compare_order_and_nan_boundaries",
    ),
    (
        "compare.first_greater",
        Verdict::Ported,
        "compare_order_and_nan_boundaries",
    ),
    (
        "compare.shorter_prefix",
        Verdict::Ported,
        "compare_order_and_nan_boundaries",
    ),
    (
        "compare.longer_prefix",
        Verdict::Ported,
        "compare_order_and_nan_boundaries",
    ),
    (
        "compare.equal",
        Verdict::Ported,
        "compare_order_and_nan_boundaries",
    ),
    (
        "compare.nan_falls_through_ordered_checks",
        Verdict::Ported,
        "compare_order_and_nan_boundaries",
    ),
];

fn go_symbols(source: &str) -> Vec<String> {
    let mut symbols = Vec::new();
    for line in source.lines() {
        let Some(declaration) = line.trim_start().strip_prefix("func ") else {
            continue;
        };
        let after_receiver = declaration
            .strip_prefix('(')
            .expect("all vector_functions declarations have receivers");
        let receiver_end = after_receiver.find(") ").expect("Go receiver terminates");
        let receiver = &declaration[..receiver_end + 2];
        let name = &after_receiver[receiver_end + 2..];
        symbols.push(format!(
            "{receiver} {}",
            name.split_once('(').expect("Go function has arguments").0
        ));
    }
    symbols.sort();
    symbols
}

fn inventory_symbols() -> Vec<String> {
    let mut symbols = FUNCTIONS
        .iter()
        .map(|(symbol, _, _)| (*symbol).to_owned())
        .collect::<Vec<_>>();
    symbols.sort();
    symbols
}

fn raw_vector(values: &[u32]) -> VectorFloat32 {
    let mut bytes = Vec::with_capacity(4 + values.len() * 4);
    bytes.extend_from_slice(&(values.len() as u32).to_le_bytes());
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    deserialize_vector_float32(&bytes).unwrap().0
}

fn finite(values: &[f32]) -> VectorFloat32 {
    VectorFloat32::must_create(values.to_vec())
}

fn error_message(result: Result<VectorFloat32, VectorError>) -> String {
    result.unwrap_err().to_string()
}

#[test]
fn vector_functions_go_source_and_function_list_are_still_current() {
    let source = include_str!("../../../../pkg/types/vector_functions.go");
    let digest = Sha256::digest(source.as_bytes());
    let actual = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    assert_eq!(actual, GO_SOURCE_SHA256);
    assert_eq!(go_symbols(source), inventory_symbols());
}

#[test]
fn every_ported_vector_functions_symbol_still_compiles() {
    let _: fn(&VectorFloat32, &VectorFloat32) -> Result<(), VectorError> =
        CHECK_IDENTICAL_DIMS_SYMBOL;
    let _ = VectorFloat32::l2_squared_distance;
    let _ = VectorFloat32::l2_distance;
    let _ = VectorFloat32::inner_product;
    let _ = VectorFloat32::negative_inner_product;
    let _ = VectorFloat32::cosine_distance;
    let _ = VectorFloat32::l1_distance;
    let _ = VectorFloat32::l2_norm;
    let _ = VectorFloat32::add;
    let _ = VectorFloat32::sub;
    let _ = VectorFloat32::mul;
    let _ = VectorFloat32::compare;
}

#[test]
fn inventory_has_no_unclassified_or_empty_reason() {
    for (_, verdict, reason) in FUNCTIONS.iter().chain(BRANCHES) {
        assert!(matches!(
            verdict,
            Verdict::Ported | Verdict::Declined | Verdict::Unreachable
        ));
        assert!(!reason.is_empty());
    }
}

#[test]
fn equal_dimensions_succeed() {
    let left = finite(&[1.0, 2.0, 3.0]);
    let right = finite(&[4.0, 5.0, 6.0]);
    assert_eq!(CHECK_IDENTICAL_DIMS_SYMBOL(&left, &right), Ok(()));
    assert_eq!(left.l2_squared_distance(&right), Ok(27.0));
    assert_eq!(left.l2_distance(&right), Ok(27_f64.sqrt()));
    assert_eq!(left.inner_product(&right), Ok(32.0));
    assert_eq!(left.negative_inner_product(&right), Ok(-32.0));
}

#[test]
fn every_fallible_operation_preserves_dimension_error() {
    let one = finite(&[1.0]);
    let empty = finite(&[]);
    let expected = "vectors have different dimensions: 1 and 0";
    assert_eq!(
        CHECK_IDENTICAL_DIMS_SYMBOL(&one, &empty)
            .unwrap_err()
            .to_string(),
        expected
    );
    assert_eq!(
        one.l2_squared_distance(&empty).unwrap_err().to_string(),
        expected
    );
    assert_eq!(one.l2_distance(&empty).unwrap_err().to_string(), expected);
    assert_eq!(one.inner_product(&empty).unwrap_err().to_string(), expected);
    assert_eq!(
        one.negative_inner_product(&empty).unwrap_err().to_string(),
        expected
    );
    assert_eq!(
        one.cosine_distance(&empty).unwrap_err().to_string(),
        expected
    );
    assert_eq!(one.l1_distance(&empty).unwrap_err().to_string(), expected);
    assert_eq!(error_message(one.add(&empty)), expected);
    assert_eq!(error_message(one.sub(&empty)), expected);
    assert_eq!(error_message(one.mul(&empty)), expected);
}

#[test]
fn source_precision_and_empty_boundaries() {
    let empty = finite(&[]);
    assert_eq!(empty.l2_squared_distance(&empty), Ok(0.0));
    assert_eq!(empty.l2_distance(&empty), Ok(0.0));
    assert_eq!(empty.inner_product(&empty), Ok(0.0));
    assert_eq!(
        empty.negative_inner_product(&empty).unwrap().to_bits(),
        (-0.0_f64).to_bits()
    );
    assert_eq!(empty.l2_norm(), 0.0);
    assert_eq!(empty.l1_distance(&empty), Ok(0.0));

    let large = finite(&[1.0e10, 1.0, -1.0e10]);
    let ones = finite(&[1.0, 1.0, 1.0]);
    let zero = finite(&[0.0, 0.0, 0.0]);
    assert_eq!(
        large.inner_product(&ones).unwrap().to_bits(),
        0x0000_0000_0000_0000
    );
    assert_eq!(
        large.l2_squared_distance(&zero).unwrap().to_bits(),
        0x4425_af1d_8000_0000
    );
    assert_eq!(
        finite(&[f32::MAX]).l2_norm().to_bits(),
        0x47ef_ffff_e000_0000
    );

    let fma_left =
        finite(&[0x3f19_5369, 0x3f4f_683f, 0x3f31_e6da, 0x3f68_17ea].map(f32::from_bits));
    let fma_right =
        finite(&[0xbf0e_8969, 0xbf6d_129f, 0x3d34_aeec, 0xbf3b_8526].map(f32::from_bits));
    assert_eq!(
        fma_left.inner_product(&fma_right).unwrap().to_bits(),
        0xbffb_79a5_a000_0000
    );

    let l2_left = finite(&[0xbf4c_0f50, 0xbf79_5134, 0xbe9c_7145, 0xbedb_0ab1].map(f32::from_bits));
    let l2_right =
        finite(&[0xbe39_f208, 0xbeb6_57d8, 0xbd34_7473, 0xbece_c1a3].map(f32::from_bits));
    assert_eq!(
        l2_left.l2_squared_distance(&l2_right).unwrap().to_bits(),
        0x3fea_8ad0_4000_0000
    );
}

#[test]
fn cosine_nan_in_range_and_clamp_boundaries() {
    let empty = finite(&[]);
    assert!(empty.cosine_distance(&empty).unwrap().is_nan());
    assert_eq!(
        finite(&[1.0, 0.0]).cosine_distance(&finite(&[0.0, 1.0])),
        Ok(1.0)
    );
    assert_eq!(
        finite(&[1.0, 2.0, 3.0])
            .cosine_distance(&finite(&[4.0, 5.0, 6.0]))
            .unwrap()
            .to_bits(),
        0x3f99_fa1b_fbc6_1940
    );
    assert_eq!(
        finite(&[0x3f19_5369, 0x3f4f_683f, 0x3f31_e6da, 0x3f68_17ea].map(f32::from_bits),)
            .cosine_distance(&finite(
                &[0xbf0e_8969, 0xbf6d_129f, 0x3d34_aeec, 0xbf3b_8526].map(f32::from_bits),
            ))
            .unwrap()
            .to_bits(),
        0x3ffd_cf5d_0a98_bf80
    );

    // Direct Go probe: raw similarity is 1.0000000000000056 before clamping.
    let a = finite(&[2.071_945_5e11, -3.510_359e-8]);
    let b = finite(&[2.071_945_7e11, -3.510_359_3e-8]);
    assert_eq!(a.cosine_distance(&b).unwrap().to_bits(), 0);

    // Negating the same witness produces the symmetric below--1 branch.
    let negative_b = finite(&[-2.071_945_7e11, 3.510_359_3e-8]);
    assert_eq!(a.cosine_distance(&negative_b).unwrap(), 2.0);
}

#[test]
fn l1_sign_and_float32_accumulation_boundaries() {
    assert_eq!(
        finite(&[-2.0, 3.0]).l1_distance(&finite(&[1.0, 1.0])),
        Ok(5.0)
    );
    let large = finite(&[1.0e10, 1.0, -1.0e10]);
    assert_eq!(
        large
            .l1_distance(&finite(&[0.0, 0.0, 0.0]))
            .unwrap()
            .to_bits(),
        0x4212_a05f_2000_0000
    );
}

#[test]
fn elementwise_success_boundaries() {
    let left = finite(&[1.0, 2.0, 3.0]);
    let right = finite(&[4.0, 5.0, 6.0]);
    assert_eq!(left.add(&right).unwrap().elements(), [5.0, 7.0, 9.0]);
    assert_eq!(right.sub(&left).unwrap().elements(), [3.0, 3.0, 3.0]);
    assert_eq!(left.mul(&right).unwrap().elements(), [4.0, 10.0, 18.0]);
    let empty = finite(&[]);
    assert!(empty.add(&empty).unwrap().is_empty());
    assert!(empty.sub(&empty).unwrap().is_empty());
    assert!(empty.mul(&empty).unwrap().is_empty());
}

#[test]
fn elementwise_error_boundaries() {
    let maximum = finite(&[f32::MAX]);
    assert_eq!(
        error_message(maximum.add(&maximum)),
        "value out of range: overflow"
    );
    assert_eq!(
        error_message(maximum.sub(&finite(&[-f32::MAX]))),
        "value out of range: overflow"
    );
    assert_eq!(
        error_message(maximum.mul(&finite(&[2.0]))),
        "value out of range: overflow"
    );

    let positive_inf = raw_vector(&[f32::INFINITY.to_bits()]);
    let negative_inf = raw_vector(&[f32::NEG_INFINITY.to_bits()]);
    let nan = raw_vector(&[f32::NAN.to_bits()]);
    assert_eq!(
        error_message(positive_inf.add(&negative_inf)),
        "value out of range: NaN"
    );
    assert_eq!(
        error_message(positive_inf.sub(&positive_inf)),
        "value out of range: NaN"
    );
    assert_eq!(
        error_message(positive_inf.mul(&finite(&[0.0]))),
        "value out of range: NaN"
    );

    let mixed_left = raw_vector(&[f32::MAX.to_bits(), f32::INFINITY.to_bits()]);
    let mixed_right = raw_vector(&[f32::MAX.to_bits(), f32::NEG_INFINITY.to_bits()]);
    assert_eq!(
        error_message(mixed_left.add(&mixed_right)),
        "value out of range: overflow"
    );

    // The Go source deliberately does not reject multiplication underflow.
    assert_eq!(
        finite(&[f32::from_bits(1)])
            .mul(&finite(&[0.5]))
            .unwrap()
            .elements()[0]
            .to_bits(),
        0
    );

    // Keep the NaN fixture live so a future constructor restriction cannot
    // silently erase the source deserialization boundary used above.
    assert!(nan.elements()[0].is_nan());
}

#[test]
fn compare_order_and_nan_boundaries() {
    assert_eq!(finite(&[0.0]).compare(&finite(&[1.0])), Ordering::Less);
    assert_eq!(
        finite(&[2.0, -100.0]).compare(&finite(&[1.0, 100.0])),
        Ordering::Greater
    );
    assert_eq!(finite(&[1.0]).compare(&finite(&[1.0, 0.0])), Ordering::Less);
    assert_eq!(
        finite(&[1.0, 0.0]).compare(&finite(&[1.0])),
        Ordering::Greater
    );
    assert_eq!(finite(&[1.0]).compare(&finite(&[1.0])), Ordering::Equal);

    let nan_one = raw_vector(&[f32::NAN.to_bits(), 1.0_f32.to_bits()]);
    assert_eq!(nan_one.compare(&finite(&[0.0, 1.0])), Ordering::Equal);
    let nan_two = raw_vector(&[f32::NAN.to_bits(), 2.0_f32.to_bits()]);
    assert_eq!(nan_two.compare(&finite(&[0.0, 1.0])), Ordering::Greater);
}
