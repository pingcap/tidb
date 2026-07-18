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

// Tuple DISTINCT state translated from
// `pkg/expression/aggregation/util.go::distinctChecker`.

use std::collections::HashSet;

use tidb_datatype::Datum;

/// Stores previously encoded tuples and reports whether the next tuple is new.
///
/// Go's checker reuses one `codec.EncodeValue` buffer and stores the resulting
/// byte key in an `MVMap`. The Rust shape preserves that ownership and reuse,
/// using `HashSet` because this consumer needs membership, not multiple values
/// per key. String and bytes datums deliberately share one raw-byte tag, as
/// Go `EncodeValue` does; collation metadata must not change DISTINCT identity.
#[derive(Debug, Default)]
pub(crate) struct DistinctChecker {
    existing_keys: HashSet<Vec<u8>>,
    key: Vec<u8>,
}

impl DistinctChecker {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Returns `true` only for the tuple's first occurrence.
    pub(crate) fn check(&mut self, values: &[Datum]) -> bool {
        self.key.clear();
        for value in values {
            encode_value(&mut self.key, value);
        }
        self.existing_keys.insert(self.key.clone())
    }
}

fn encode_value(out: &mut Vec<u8>, value: &Datum) {
    match value {
        Datum::Null => out.push(0),
        Datum::MinNotNull => out.push(1),
        Datum::MaxValue => out.push(250),
        Datum::Int(value) => {
            out.push(8);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Datum::UInt(value) => {
            out.push(9);
            out.extend_from_slice(&value.to_le_bytes());
        }
        Datum::Real(value) => {
            out.push(5);
            let bits = value.to_bits();
            let comparable = if *value >= 0.0 {
                bits | (1_u64 << 63)
            } else {
                !bits
            };
            out.extend_from_slice(&comparable.to_be_bytes());
        }
        Datum::Decimal(value) => {
            out.push(6);
            let mut coefficient = value.coefficient_digits();
            let mut scale = value.storage_scale();
            while scale > 0 && coefficient.ends_with('0') {
                coefficient = &coefficient[..coefficient.len() - 1];
                scale -= 1;
            }
            let coefficient = coefficient.trim_start_matches('0');
            let coefficient = if coefficient.is_empty() {
                scale = 0;
                "0"
            } else {
                coefficient
            };
            out.push(u8::from(value.is_negative()));
            out.extend_from_slice(&scale.to_le_bytes());
            encode_bytes(out, coefficient.as_bytes());
        }
        Datum::String(value) => {
            out.push(2);
            encode_bytes(out, value.bytes());
        }
        Datum::Bytes(value) => {
            out.push(2);
            encode_bytes(out, value);
        }
    }
}

fn encode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}
