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

//! Go-authoritative JSON binary-format vectors.
//!
//! Every hex string in the fixture was produced by
//! `rust/difftests/transaction-tests/fixtures/generate_json_vectors.go`
//! running against this repository's Go tree: the binary form
//! (`TypeCode` followed by `Value`) that `types.ParseBinaryJSONFromString`
//! builds, which is the exact byte sequence TiDB stores inside a JSON
//! datum. The self-round-trip suite (parse ours, encode ours) cannot
//! detect a symmetric encoder divergence from TiDB; this file can.

use tidb_datatype::BinaryJSON;

const FIXTURE: &str =
    include_str!("../../../difftests/transaction-tests/fixtures/json_vectors.hex");

/// The documents, in the fixture's order. Keep this list in sync with
/// `generate_json_vectors.go`.
const DOCUMENTS: &[&str] = &[
    "null",
    "true",
    "false",
    "1",
    "-1",
    "18446744073709551615",
    "1.5",
    "-2.25",
    "\"hello\"",
    "\"中文 ✓ escaped \\u00e9\"",
    "[]",
    "{}",
    "[1, -2, 3.5, \"x\", true, null, [], {\"k\": \"v\"}]",
    "{\"b\": 1, \"a\": 2}",
    "{\"k\": [{\"nested\": true}, 255]}",
    "{\"z\": {\"y\": {\"x\": 1}}}",
    "\"\"",
];

fn fixture_hex(name: &str) -> &'static str {
    let prefix = format!("{name}=");
    FIXTURE
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("fixture has no {name} entry"))
}

fn unhex(text: &str) -> Vec<u8> {
    assert!(text.len().is_multiple_of(2), "odd hex length");
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).expect("hex digit");
            let low = (pair[1] as char).to_digit(16).expect("hex digit");
            ((high << 4) | low) as u8
        })
        .collect()
}

#[test]
fn json_binary_encoding_matches_go_byte_for_byte() {
    for (index, document) in DOCUMENTS.iter().enumerate() {
        let name = format!("doc{index:02}");
        let expected = unhex(fixture_hex(&name));
        let parsed = BinaryJSON::parse(document)
            .unwrap_or_else(|error| panic!("{name} ({document}) does not parse: {error:?}"));
        assert_eq!(
            parsed.encoded(),
            expected,
            "{name}: binary form of {document} diverges from Go"
        );
    }
}

#[test]
fn json_type_codes_carry_go_leading_byte() {
    // The leading byte is Go's TypeCode: 0x04 literal, 0x09 int64,
    // 0x0a uint64, 0x0b float64, 0x0c string, 0x01 object, 0x03 array.
    let codes = [
        ("null", 0x04u8),
        ("true", 0x04),
        ("false", 0x04),
        ("1", 0x09),
        ("-1", 0x09),
        ("18446744073709551615", 0x0a),
        ("1.5", 0x0b),
        ("\"hello\"", 0x0c),
        ("[]", 0x03),
        ("{}", 0x01),
    ];
    for (document, code) in codes {
        let parsed = BinaryJSON::parse(document).expect("parses");
        assert_eq!(parsed.type_code(), code, "type code of {document}");
    }
}
