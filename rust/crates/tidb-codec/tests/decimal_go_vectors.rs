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

//! Natural-precision vectors from `pkg/util/codec/codec_test.go::TestDecimal`.

use tidb_codec::{decode_one, encode_key};
use tidb_datatype::{Datum, Decimal};

const FIXTURE: &str =
    include_str!("../../../difftests/transaction-tests/fixtures/decimal_keys.hex");

#[test]
fn natural_precision_decimal_keys_are_go_byte_exact() {
    let literals = [
        "1234.00", "1234", "12.34", "12.340", "0.1234", "0.0", "0", "-0.0", "-0.0000", "-1234.00",
        "-1234", "-12.34", "-12.340", "-0.1234",
    ];

    for (index, literal) in literals.into_iter().enumerate() {
        let decimal = parse_decimal(literal);
        let encoded = encode_key(&[Datum::new_decimal(decimal.clone())]).unwrap();
        assert_eq!(encoded, fixture(&format!("decimal_{index}")), "{literal}");

        let (remain, decoded) = decode_one(&encoded).unwrap();
        assert!(remain.is_empty());
        let decoded = decoded.as_decimal().unwrap();
        assert_eq!(decoded, &decimal);
        assert_eq!(decoded.storage_scale(), decimal.storage_scale());
    }
}

fn parse_decimal(literal: &str) -> Decimal {
    match literal.strip_prefix('-') {
        Some(magnitude) => Decimal::from_literal(magnitude).negate(),
        None => Decimal::from_literal(literal),
    }
}

fn fixture(name: &str) -> Vec<u8> {
    let prefix = format!("{name}=");
    let hex = FIXTURE
        .lines()
        .find_map(|line| line.strip_prefix(&prefix))
        .unwrap_or_else(|| panic!("fixture has no {name} entry"));
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
        .collect()
}

fn nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        _ => panic!("non-hex fixture byte {byte}"),
    }
}
