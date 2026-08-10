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

//! Go-generated text-protocol vectors.
//!
//! This is the tier that actually serves clients: every value a `SELECT`
//! returns over the text protocol passes through `FormatValueText`, so a
//! divergence here is a wrong answer on the wire, not an internal
//! representation detail. Each row is what `pkg/format/textrow`'s own
//! formatter emitted for the same column shape and the same value, driven
//! through a real `chunk.Row` -- never derived from this crate's output.
//!
//! Fixture: `generate_textrow_vectors.go` beside the `.tsv`. Its columns are
//! type code, flag, decimal, `Table == ""`, the input, then `OK <hex>` or
//! `ERR`.
//!
//! The `ERR` rows matter as much as the `OK` ones: Go's formatter has a
//! `default: return nil, ErrInvalidType` arm, and this crate must refuse
//! exactly the same shapes rather than inventing a rendering for them.

use tidb_protocol::{format_text_value, TextColumn, TextScalar};

const FIXTURE: &str =
    include_str!("../../../difftests/transaction-tests/fixtures/textrow_vectors.tsv");

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|index| u8::from_str_radix(&text[index..index + 2], 16).expect("hex pair"))
        .collect()
}

#[test]
fn format_text_value_go_vectors() {
    let mut rows = 0_usize;
    for line in FIXTURE.lines().filter(|line| !line.trim().is_empty()) {
        let fields: Vec<&str> = line.split('\t').collect();
        assert!(fields.len() >= 6, "malformed fixture line: {line}");
        let type_code: u8 = fields[0].parse().expect("type code");
        let flag: u16 = fields[1].parse().expect("flag");
        let decimal: u8 = fields[2].parse().expect("decimal");
        let table_is_empty = fields[3] == "1";
        let input = fields[4];
        let status = fields[5];
        let want = fields.get(6).copied().unwrap_or("");

        let column = TextColumn {
            type_code,
            flag,
            decimal,
            table_is_empty,
        };

        // The input encoding keeps floats as their exact BIT PATTERN, so no
        // decimal round trip stands between Go's value and this one.
        let (tag, payload) = input.split_once(':').expect("tagged input");
        let bytes;
        let decimal_text;
        let value = match tag {
            "t" | "dur" => {
                bytes = payload.as_bytes().to_vec();
                TextScalar::Temporal(&bytes)
            }
            "enum" | "set" | "json" => {
                bytes = if tag == "json" {
                    br#"{"k": [1, "x", null]}"#.to_vec()
                } else {
                    payload.as_bytes().to_vec()
                };
                TextScalar::Bytes(&bytes)
            }
            "i" => TextScalar::Signed(payload.parse().expect("i64")),
            "u" => TextScalar::Unsigned(payload.parse().expect("u64")),
            "f64" => TextScalar::Float {
                value: f64::from_bits(u64::from_str_radix(payload, 16).expect("f64 bits")),
                bit_size: 64,
            },
            "f32" => TextScalar::Float {
                value: f64::from(f32::from_bits(
                    u32::from_str_radix(payload, 16).expect("f32 bits"),
                )),
                bit_size: 32,
            },
            "d" => {
                decimal_text = payload.as_bytes().to_vec();
                TextScalar::Decimal(&decimal_text)
            }
            "b" => {
                bytes = unhex(payload);
                TextScalar::Bytes(&bytes)
            }
            other => panic!("unknown input tag {other}"),
        };

        let got = format_text_value(column, value);
        let label = format!("type={type_code} flag={flag} dec={decimal} input={input}");
        match status {
            "OK" => {
                let formatted = got
                    .unwrap_or_else(|error| panic!("{label}: Go formatted it, we errored {error}"))
                    .unwrap_or_else(|| panic!("{label}: Go formatted it, we answered NULL"));
                assert_eq!(hex(&formatted), want, "{label}");
            }
            "ERR" => assert!(got.is_err(), "{label}: Go refused this shape, we did not"),
            other => panic!("unknown status {other}"),
        }
        rows += 1;
    }
    assert_eq!(rows, 1516, "fixture row count");
}
