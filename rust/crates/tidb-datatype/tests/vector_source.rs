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

//! Source-backed boundary tests for Go `pkg/types/vector.go`.

use std::{cmp::Ordering, collections::BTreeMap, fs, path::PathBuf};

use sha2::{Digest, Sha256};
use tidb_datatype::{
    check_vector_dim_valid, deserialize_vector_float32, peek_vector_float32, VectorError,
    VectorFloat32, MAX_VECTOR_DIMENSION,
};

const GO_SOURCE_SHA256: &str = "3f833eb599672f37fa3e056c2d85b578c5bd1a378155c7b64ddccc454aaa6e61";
const GO_TEST_SHA256: &str = "167de06e188ae18c58fede0569064b7b156b49fe6873faf7c55d89974584b31a";
const INVENTORY_SHA256: &str = "03a8259dcb51e14a8b2c3c601aff467593fdb564e4a6cd5617b71aca3d1971d3";
const RUST_MODULE_SHA256: &str = "c2eb1433c1787eb5607a349dc19e2e84dbd4a761e518112a82d88493c59c65fb";
const INVENTORY: &str = include_str!("../src/vector.inventory.tsv");

type DeserializeVectorFn =
    for<'a> fn(&'a [u8]) -> Result<(VectorFloat32, &'a [u8]), VectorError>;

const EXPECTED_IDS: [&str; 84] = [
    "D01", "D02", "R01", "R02", "F01", "B01", "B02", "F02", "B03", "B04", "B05", "B06",
    "F03", "B07", "B08", "F04", "B09", "R03", "F05", "B10", "B11", "B12", "F06", "B13",
    "B14", "B15", "F07", "F08", "B16", "B17", "F09", "B18", "B19", "B20", "R04", "F10",
    "B21", "B22", "R05", "F11", "R06", "F12", "R07", "F13", "F14", "F15", "B23", "R08",
    "B24", "B25", "B26", "F16", "B27", "R09", "B28", "B29", "B30", "B31", "F17", "B32",
    "B33", "B34", "B35", "B36", "B37", "B38", "B39", "B40", "B41", "B42", "B43", "B44",
    "B45", "F18", "R10", "F19", "B46", "B47", "T01", "T02", "T03", "T04", "T05", "T06",
];

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..")
}

fn sha256(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

fn inventory_rows() -> Vec<Vec<&'static str>> {
    INVENTORY
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#') && !line.starts_with("id\t"))
        .map(|line| line.split('\t').collect())
        .collect()
}

fn vector_text(dimensions: usize) -> String {
    if dimensions == 0 {
        return "[]".to_owned();
    }
    format!("[{}0]", "0,".repeat(dimensions - 1))
}

#[test]
fn vector_go_source_lockdown_inventory_and_symbols() {
    let root = repo_root();
    assert_eq!(
        sha256(fs::read(root.join("pkg/types/vector.go")).unwrap()),
        GO_SOURCE_SHA256,
        "owning Go source drifted"
    );
    assert_eq!(
        sha256(fs::read(root.join("pkg/types/vector_test.go")).unwrap()),
        GO_TEST_SHA256,
        "owning Go test drifted"
    );
    assert_eq!(sha256(INVENTORY), INVENTORY_SHA256, "inventory drifted");
    assert_eq!(
        sha256(fs::read(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/vector.rs")).unwrap()),
        RUST_MODULE_SHA256,
        "owned Rust module drifted"
    );

    let rows = inventory_rows();
    assert!(rows.iter().all(|row| row.len() == 6));
    assert_eq!(
        rows.iter().map(|row| row[0]).collect::<Vec<_>>(),
        EXPECTED_IDS
    );

    let allowed_statuses = ["PORTED", "DECLINED", "UNREACHABLE"];
    let mut statuses = BTreeMap::new();
    let ported_symbols = [
        "VectorFloat32",
        "VectorFloat32::default",
        "VectorFloat32::serialize",
        "VectorFloat32::create",
        "VectorFloat32::must_create",
        "VectorFloat32::init",
        "check_vector_dim_valid",
        "VectorFloat32::check_dims_fit_column",
        "VectorFloat32::len",
        "VectorFloat32::elements_mut",
        "VectorFloat32::truncated_string",
        "ToString::to_string",
        "VectorFloat32::serialize_to",
        "VectorFloat32::serialized_size",
        "VectorFloat32::estimated_mem_usage",
        "peek_vector_float32",
        "deserialize_vector_float32",
        "VectorFloat32::parse",
        "Clone::clone",
        "VectorFloat32::is_zero_value",
        "vector_wire_format_and_decode_rules_match_source",
        "vector_clone_zero_and_memory_rules_match_source",
        "vector_text_parse_and_format_rules_match_source",
    ];
    for row in &rows {
        assert!(
            allowed_statuses.contains(&row[3]),
            "invalid status: {row:?}"
        );
        assert!(!row[5].is_empty(), "missing evidence: {row:?}");
        *statuses.entry(row[3]).or_insert(0usize) += 1;
        if row[3] == "PORTED" {
            assert!(
                ported_symbols.contains(&row[4]),
                "PORTED row has no gated symbol: {row:?}"
            );
        } else {
            assert_eq!(row[4], "-", "non-PORTED row claims a symbol: {row:?}");
        }
    }
    assert_eq!(statuses.get("PORTED"), Some(&69));
    assert_eq!(statuses.get("DECLINED"), Some(&9));
    assert_eq!(statuses.get("UNREACHABLE"), Some(&6));

    let _: fn(Vec<f32>) -> Result<VectorFloat32, VectorError> = VectorFloat32::create;
    let _: fn(Vec<f32>) -> VectorFloat32 = VectorFloat32::must_create;
    let _: fn(usize) -> VectorFloat32 = VectorFloat32::init;
    let _: fn(isize) -> Result<(), VectorError> = check_vector_dim_valid;
    let _: fn(&VectorFloat32, Option<usize>) -> Result<(), VectorError> =
        VectorFloat32::check_dims_fit_column;
    let _: fn(&VectorFloat32) -> usize = VectorFloat32::len;
    let _: for<'a> fn(&'a mut VectorFloat32) -> &'a mut [f32] = VectorFloat32::elements_mut;
    let _: fn(&VectorFloat32) -> String = VectorFloat32::truncated_string;
    let _: fn(&VectorFloat32) -> String = ToString::to_string;
    let _: fn(&VectorFloat32, &mut Vec<u8>) = VectorFloat32::serialize_to;
    let _: fn(&VectorFloat32) -> usize = VectorFloat32::serialized_size;
    let _: fn(&VectorFloat32) -> usize = VectorFloat32::estimated_mem_usage;
    let _: fn(&[u8]) -> Result<usize, VectorError> = peek_vector_float32;
    let _: DeserializeVectorFn = deserialize_vector_float32;
    let _: fn(&str) -> Result<VectorFloat32, VectorError> = VectorFloat32::parse;
    let _: fn(&VectorFloat32) -> VectorFloat32 = Clone::clone;
    let _: fn(&VectorFloat32) -> bool = VectorFloat32::is_zero_value;
    let _: fn() = vector_constructor_and_dimension_rules_match_source;
    let _: fn() = vector_text_parse_and_format_rules_match_source;
    let _: fn() = vector_wire_format_and_decode_rules_match_source;
    let _: fn() = vector_clone_zero_and_memory_rules_match_source;
    let _ = std::mem::size_of::<VectorFloat32>();
}

fn wire(bits: &[u32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(4 + bits.len() * 4);
    bytes.extend_from_slice(&(bits.len() as u32).to_le_bytes());
    for value in bits {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

#[test]
fn vector_constructor_and_dimension_rules_match_source() {
    assert_eq!(
        check_vector_dim_valid(-1).unwrap_err().to_string(),
        "dimensions for type vector must be at least 0"
    );
    assert!(check_vector_dim_valid(0).is_ok());
    assert!(check_vector_dim_valid(MAX_VECTOR_DIMENSION as isize).is_ok());
    let dimension = MAX_VECTOR_DIMENSION + 1;
    assert_eq!(
        check_vector_dim_valid(dimension as isize)
            .unwrap_err()
            .to_string(),
        "vector cannot have more than 16383 dimensions"
    );
    let vector = VectorFloat32::create(vec![0.0; dimension]).unwrap();
    assert_eq!(vector.len(), dimension);

    for (value, message) in [
        (f32::NAN, "NaN not allowed in vector"),
        (f32::INFINITY, "infinite value not allowed in vector"),
        (f32::NEG_INFINITY, "infinite value not allowed in vector"),
    ] {
        assert_eq!(
            VectorFloat32::create(vec![value])
                .unwrap_err()
                .to_string(),
            message
        );
    }
    assert!(std::panic::catch_unwind(|| VectorFloat32::must_create(vec![f32::NAN])).is_err());
    assert_eq!(
        VectorFloat32::must_create(vec![1.0, 2.0]).elements(),
        [1.0, 2.0]
    );

    let mut source = vec![1.0, 2.0];
    let copied = VectorFloat32::create(source.as_slice()).unwrap();
    source[0] = 9.0;
    assert_eq!(copied.elements(), [1.0, 2.0]);

    let mut initialized = VectorFloat32::init(2);
    assert_eq!(initialized.elements(), [0.0, 0.0]);
    initialized.elements_mut().copy_from_slice(&[1.1, 2.2]);
    assert_eq!(initialized.len(), 2);
    assert!(initialized.check_dims_fit_column(None).is_ok());
    assert!(initialized.check_dims_fit_column(Some(2)).is_ok());
    for expected in [0, 3] {
        assert_eq!(
            initialized
                .check_dims_fit_column(Some(expected))
                .unwrap_err()
                .to_string(),
            format!("vector has 2 dimensions, does not fit VECTOR({expected})")
        );
    }
}

#[test]
fn vector_text_parse_and_format_rules_match_source() {
    for text in [
        "abc",
        "null",
        "  null\t",
        "NULL",
        "\"json_str\"",
        "123",
        "[123",
        "123]",
        "[123,]",
        "[NaN]",
        "[Infinity]",
        "[-Infinity]",
        "[null]",
        "[true]",
        "[\"1\"]",
        "[[1]]",
        "[1,2,3]extra",
        "[1] null",
    ] {
        assert_eq!(
            VectorFloat32::parse(text).unwrap_err().to_string(),
            format!("Invalid vector text: {text}")
        );
    }

    let empty = VectorFloat32::parse("[]").unwrap();
    assert!(empty.is_zero_value());
    assert_eq!(empty.to_string(), "[]");
    let ordinary = VectorFloat32::parse("[1.1, 2.2, 3.3]   ").unwrap();
    assert_eq!(ordinary.elements(), [1.1, 2.2, 3.3]);
    assert_eq!(ordinary.to_string(), "[1.1,2.2,3.3]");

    let maximum = VectorFloat32::parse("[3.4028234663852886e38]").unwrap();
    assert_eq!(maximum.elements(), [f32::MAX]);

    assert_eq!(
        VectorFloat32::parse("[3.402823466385289e38]")
            .unwrap_err()
            .to_string(),
        "value 3.402823466385289e+38 out of range for float32"
    );

    for text in ["[1e9999]", "[-1e9999]"] {
        assert_eq!(
            VectorFloat32::parse(text).unwrap_err().to_string(),
            format!("Invalid vector text: {text}")
        );
    }
    assert_eq!(
        VectorFloat32::parse("[1e-9999]").unwrap().elements(),
        [0.0]
    );

    let signed_and_extreme =
        VectorFloat32::parse("[-0,0,1e-45,3.4028234663852886e38]").unwrap();
    assert_eq!(signed_and_extreme.elements()[0].to_bits(), (-0.0_f32).to_bits());
    assert_eq!(signed_and_extreme.elements()[1].to_bits(), 0.0_f32.to_bits());
    assert_eq!(signed_and_extreme.elements()[2].to_bits(), 1);
    assert_eq!(signed_and_extreme.elements()[3], f32::MAX);
    assert_eq!(
        signed_and_extreme.to_string(),
        "[-0,0,0.000000000000000000000000000000000000000000001,340282350000000000000000000000000000000]"
    );

    assert_eq!(
        VectorFloat32::parse("[-1e39, 1e39]")
            .unwrap_err()
            .to_string(),
        "value -1e+39 out of range for float32"
    );
    assert_eq!(
        VectorFloat32::parse(&vector_text(MAX_VECTOR_DIMENSION))
            .unwrap()
            .len(),
        MAX_VECTOR_DIMENSION
    );
    assert_eq!(
        VectorFloat32::parse(&vector_text(MAX_VECTOR_DIMENSION + 1))
            .unwrap_err()
            .to_string(),
        "vector cannot have more than 16383 dimensions"
    );

    let mut formatted = VectorFloat32::init(11);
    formatted.elements_mut().copy_from_slice(&[
        -0.0,
        0.0,
        f32::from_bits(1),
        -f32::from_bits(1),
        1e-5,
        1e-4,
        1.1,
        12.34,
        1e20,
        f32::MAX,
        -f32::MAX,
    ]);
    assert_eq!(
        formatted.to_string(),
        "[-0,0,0.000000000000000000000000000000000000000000001,-0.000000000000000000000000000000000000000000001,0.00001,0.0001,1.1,12.34,100000000000000000000,340282350000000000000000000000000000000,-340282350000000000000000000000000000000]"
    );
    assert_eq!(
        formatted.truncated_string(),
        "[-0,0,1.4e-45,-1.4e-45,1e-05,(6 more)...]"
    );
    for (dimension, expected) in [
        (0, "[]"),
        (1, "[1.2]"),
        (5, "[1.2,2.2,3.2,4.2,5.2]"),
        (6, "[1.2,2.2,3.2,4.2,5.2,(1 more)...]"),
    ] {
        let mut value = VectorFloat32::init(dimension);
        for (index, element) in value.elements_mut().iter_mut().enumerate() {
            *element = (index + 1) as f32 + 0.25;
        }
        assert_eq!(value.truncated_string(), expected);
    }
}

#[test]
fn vector_wire_format_and_decode_rules_match_source() {
    let mut vector = VectorFloat32::init(2);
    vector.elements_mut().copy_from_slice(&[1.1, 2.2]);
    assert_eq!(
        vector.serialize(),
        [2, 0, 0, 0, 0xcd, 0xcc, 0x8c, 0x3f, 0xcd, 0xcc, 0x0c, 0x40]
    );
    let mut prefixed = vec![9, 8, 7];
    vector.serialize_to(&mut prefixed);
    assert_eq!(
        prefixed,
        [
            9, 8, 7, 2, 0, 0, 0, 0xcd, 0xcc, 0x8c, 0x3f, 0xcd, 0xcc, 0x0c, 0x40
        ]
    );
    assert_eq!(vector.serialized_size(), 12);

    for bytes in [&[][..], &[0xf1, 0xfc][..]] {
        assert_eq!(
            peek_vector_float32(bytes).unwrap_err().to_string(),
            format!("bad VectorFloat32 value header (len={})", bytes.len())
        );
        assert!(deserialize_vector_float32(bytes).is_err());
    }
    assert_eq!(
        peek_vector_float32(&[1, 0, 0, 0])
            .unwrap_err()
            .to_string(),
        "bad VectorFloat32 value (len=4, expected=8)"
    );

    let mut serialized = vector.serialize();
    serialized.extend_from_slice(&[1, 2, 3, 4]);
    assert_eq!(peek_vector_float32(&serialized).unwrap(), 12);
    let (round_trip, remaining) = deserialize_vector_float32(&serialized).unwrap();
    assert_eq!(round_trip, vector);
    assert_eq!(remaining, [1, 2, 3, 4]);

    let large_wire = wire(&vec![0; MAX_VECTOR_DIMENSION + 1]);
    let (large, remaining) = deserialize_vector_float32(&large_wire).unwrap();
    assert_eq!(large.len(), MAX_VECTOR_DIMENSION + 1);
    assert!(remaining.is_empty());

    for bits in [0x7fc0_0000, 0x7f80_0000, 0xff80_0000] {
        let bytes = wire(&[bits]);
        let (decoded, remaining) = deserialize_vector_float32(&bytes).unwrap();
        assert_eq!(decoded.elements()[0].to_bits(), bits);
        assert!(remaining.is_empty());
    }

    let bytes = wire(&[f32::NAN.to_bits()]);
    let (nan, _) = deserialize_vector_float32(&bytes).unwrap();
    let zero = VectorFloat32::init(1);
    assert_eq!(nan.compare(&zero), Ordering::Equal);
    assert_eq!(zero.compare(&nan), Ordering::Equal);
    assert_eq!(nan.compare(&nan), Ordering::Equal);

    for header in [0x4000_0000_u32, 0xffff_ffff] {
        assert_eq!(
            peek_vector_float32(&header.to_le_bytes())
                .unwrap_err()
                .to_string(),
            "bad VectorFloat32 value size overflow"
        );
    }
}

#[test]
fn vector_clone_zero_and_memory_rules_match_source() {
    let zero = VectorFloat32::default();
    assert!(zero.is_zero_value());
    assert!(zero.is_empty());
    assert!(zero.elements().is_empty());
    assert_eq!(zero.serialize(), [0, 0, 0, 0]);
    assert_eq!(zero.serialized_size(), 4);
    assert_eq!(zero.to_string(), "[]");

    let mut original = VectorFloat32::must_create(vec![1.0, 2.0]);
    assert!(!original.is_zero_value());
    let mut cloned = original.clone();
    cloned.elements_mut()[0] = 9.0;
    assert_eq!(original.elements(), [1.0, 2.0]);
    assert_eq!(cloned.elements(), [9.0, 2.0]);
    original.elements_mut()[1] = 7.0;
    assert_eq!(cloned.elements(), [9.0, 2.0]);

    assert_eq!(
        original.estimated_mem_usage(),
        std::mem::size_of::<VectorFloat32>() + original.serialized_size()
    );
    if std::mem::size_of::<usize>() == 8 {
        assert_eq!(original.estimated_mem_usage(), 36);
    }
}

#[test]
fn vector_declined_and_unreachable_seams_are_explicit() {
    let rows = inventory_rows();
    let items = |status| {
        rows.iter()
            .filter(|row| row[3] == status)
            .map(|row| row[2])
            .collect::<Vec<_>>()
    };
    assert_eq!(
        items("DECLINED"),
        [
            "VectorFloat32 stores header and elements in one data []byte",
            "init() host-endian guard",
            "VectorFloat32.ZeroCopySerialize()",
            "ZeroCopySerialize returns bytes aliasing the Go vector",
            "PeekBytesAsVectorFloat32 lets uint32 size arithmetic wrap to zero or four",
            "ZeroCopyDeserializeVectorFloat32 aliases the caller's input bytes",
            "ZeroCopyDeserializeVectorFloat32 accepts wrapped malformed headers",
            "TestVectorDatum",
            "TestVectorCompare",
        ]
    );
    assert_eq!(
        items("UNREACHABLE"),
        [
            "init returns on a little-endian host",
            "init panics on a non-little-endian host",
            "InitVectorFloat32 negative dims panic while constructing the Go byte slice",
            "Elements returns nil for a zero-dimensional Go vector",
            "ParseVectorFloat32 ReadArray NaN error branch",
            "ParseVectorFloat32 ReadArray infinity error branch",
        ]
    );

    let module = fs::read_to_string(
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/vector.rs"),
    )
    .unwrap();
    assert!(module.contains("elements: Vec<f32>"));
    assert!(module.contains("to_le_bytes()"));
    assert!(module.contains("from_le_bytes"));
    assert!(module.contains("checked_mul(4)"));
}
