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

//! LOCKDOWN INVENTORY: `pkg/types/vector.go` -> `vector.rs`.
//!
//! This is the source-owned boundary for the VECTOR value representation. The
//! arithmetic methods sharing `VectorFloat32` come from
//! `pkg/types/vector_functions.go`; they are intentionally not evidence for
//! this source file and need their own inventory before another change lands.
//!
//! Every Go declaration from `vector.go` has exactly one verdict below. The
//! gate derives the list with the same declaration-shaped scan every time the
//! Rust tests run. An added, removed, or renamed Go function therefore cannot
//! become a silent omission, and the second gate compiles a reference to every
//! Rust symbol named by a PORTED row.
//!
//! `DECLINED` is not a TODO. The source and measurement are the reason:
//!
//! * `init` says `"VectorFloat32 only supports little endian"`. Rust's wire
//!   conversion is explicitly little-endian on every supported target, so it
//!   has no host-endian startup assertion to port.
//! * `Elements` returns `nil` when `l == 0`. Rust's borrowed slice has no nil
//!   value; `&[]` is the equivalent iterable, length-zero representation.
//! * `ZeroCopySerialize` says `"without memory copy"` and returns Go's backing
//!   bytes. This crate forbids unsafe code and owns aligned `f32`s, so returning
//!   an alias would either violate Rust ownership or need unsafe casts.
//! * The `PeekBytesAsVectorFloat32` overflow branch is declined deliberately.
//!   A direct Go probe over `00 00 00 40` returned nil error and a vector whose
//!   `Len()` is 1073741824 despite carrying only the four-byte header, because
//!   `uint32` multiplication wraps. Rust rejects it rather than constructing a
//!   slice it cannot represent safely. `malformed_wrapped_vector_length_is_rejected`
//!   pins that boundary.

use super::{
    check_vector_dim_valid, deserialize_vector_float32, peek_vector_float32, VectorError,
    VectorFloat32,
};

#[allow(dead_code)] // This source file has no unreachable Go declaration today.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    Ported,
    Declined,
    Unreachable,
}

type Row = (&'static str, Verdict, &'static str);

const FUNCTIONS: &[Row] = &[
    (
        "init",
        Verdict::Declined,
        "host-endian startup assertion; see module doc",
    ),
    (
        "CreateVectorFloat32",
        Verdict::Ported,
        "VectorFloat32::create",
    ),
    (
        "MustCreateVectorFloat32",
        Verdict::Ported,
        "VectorFloat32::must_create",
    ),
    ("InitVectorFloat32", Verdict::Ported, "VectorFloat32::init"),
    (
        "CheckVectorDimValid",
        Verdict::Ported,
        "check_vector_dim_valid",
    ),
    (
        "(v VectorFloat32) CheckDimsFitColumn",
        Verdict::Ported,
        "VectorFloat32::check_dims_fit_column; None is Go UnspecifiedLength",
    ),
    (
        "(v VectorFloat32) Len",
        Verdict::Ported,
        "VectorFloat32::len",
    ),
    (
        "(v VectorFloat32) Elements",
        Verdict::Declined,
        "Go's nil empty slice; see module doc",
    ),
    (
        "(v VectorFloat32) TruncatedString",
        Verdict::Ported,
        "VectorFloat32::truncated_string",
    ),
    (
        "(v VectorFloat32) String",
        Verdict::Ported,
        "Display for VectorFloat32",
    ),
    (
        "(v VectorFloat32) ZeroCopySerialize",
        Verdict::Declined,
        "source says without memory copy; see module doc",
    ),
    (
        "(v VectorFloat32) SerializeTo",
        Verdict::Ported,
        "VectorFloat32::serialize_to",
    ),
    (
        "(v VectorFloat32) SerializedSize",
        Verdict::Ported,
        "VectorFloat32::serialized_size",
    ),
    (
        "(v VectorFloat32) EstimatedMemUsage",
        Verdict::Ported,
        "VectorFloat32::estimated_mem_usage",
    ),
    (
        "PeekBytesAsVectorFloat32",
        Verdict::Declined,
        "normal framing is peek_vector_float32; wrapped uint32 branch is declined",
    ),
    (
        "ZeroCopyDeserializeVectorFloat32",
        Verdict::Declined,
        "normal value decoding is deserialize_vector_float32; zero-copy alias is declined",
    ),
    (
        "ParseVectorFloat32",
        Verdict::Ported,
        "VectorFloat32::parse",
    ),
    (
        "(v VectorFloat32) Clone",
        Verdict::Ported,
        "VectorFloat32::clone",
    ),
    (
        "(v VectorFloat32) IsZeroValue",
        Verdict::Ported,
        "VectorFloat32::is_zero_value",
    ),
];

const GO_TESTS: &[Row] = &[
    (
        "TestVectorEndianess",
        Verdict::Ported,
        "original_vector_endianness_zero_parse_compare_serialize_and_datum_rows",
    ),
    (
        "TestZeroVector",
        Verdict::Ported,
        "original_vector_endianness_zero_parse_compare_serialize_and_datum_rows",
    ),
    (
        "TestVectorParse",
        Verdict::Ported,
        "original_vector_endianness_zero_parse_compare_serialize_and_datum_rows",
    ),
    ("TestVectorDatum", Verdict::Ported, "go_test_vector_datum"),
    (
        "TestVectorCompare",
        Verdict::Ported,
        "original_vector_endianness_zero_parse_compare_serialize_and_datum_rows",
    ),
    (
        "TestVectorSerialize",
        Verdict::Ported,
        "original_vector_endianness_zero_parse_compare_serialize_and_datum_rows",
    ),
];

fn go_symbols(source: &str, test_only: bool) -> Vec<String> {
    let mut symbols = Vec::new();
    for line in source.lines() {
        let Some(declaration) = line.trim_start().strip_prefix("func ") else {
            continue;
        };
        if test_only && !declaration.starts_with("Test") {
            continue;
        }
        let symbol = if let Some(after_receiver) = declaration.strip_prefix('(') {
            let receiver_end = after_receiver.find(") ").expect("Go receiver terminates");
            let receiver = &declaration[..receiver_end + 2];
            let name = &after_receiver[receiver_end + 2..];
            format!(
                "{receiver} {}",
                name.split_once('(').expect("Go function has args").0
            )
        } else {
            declaration
                .split_once('(')
                .expect("Go function has args")
                .0
                .to_owned()
        };
        symbols.push(symbol);
    }
    symbols.sort();
    symbols
}

fn inventory_symbols(rows: &[Row]) -> Vec<String> {
    let mut symbols = rows
        .iter()
        .map(|(symbol, _, _)| (*symbol).to_owned())
        .collect::<Vec<_>>();
    symbols.sort();
    symbols
}

#[test]
fn vector_go_function_list_is_still_current() {
    let source = include_str!("../../../../pkg/types/vector.go");
    assert_eq!(go_symbols(source, false), inventory_symbols(FUNCTIONS));
}

#[test]
fn vector_go_test_list_is_still_current() {
    let source = include_str!("../../../../pkg/types/vector_test.go");
    assert_eq!(go_symbols(source, true), inventory_symbols(GO_TESTS));
}

#[test]
fn every_ported_vector_symbol_still_compiles() {
    let _: fn(Vec<f32>) -> Result<VectorFloat32, VectorError> = VectorFloat32::create;
    let _: fn(Vec<f32>) -> VectorFloat32 = VectorFloat32::must_create;
    let _ = VectorFloat32::init;
    let _ = check_vector_dim_valid;
    let _ = VectorFloat32::check_dims_fit_column;
    let _ = VectorFloat32::len;
    let _ = VectorFloat32::truncated_string;
    let _ = VectorFloat32::serialize_to;
    let _ = VectorFloat32::serialized_size;
    let _ = VectorFloat32::estimated_mem_usage;
    let _ = VectorFloat32::parse;
    let _ = VectorFloat32::clone;
    let _ = VectorFloat32::is_zero_value;
    let _ = deserialize_vector_float32;
    let _ = peek_vector_float32;
    let vector = VectorFloat32::default();
    assert_eq!(vector.to_string(), "[]");
}

#[test]
fn inventory_has_no_unclassified_or_empty_reason() {
    for (_, verdict, reason) in FUNCTIONS.iter().chain(GO_TESTS) {
        assert!(matches!(
            verdict,
            Verdict::Ported | Verdict::Declined | Verdict::Unreachable
        ));
        assert!(!reason.is_empty());
    }
}
