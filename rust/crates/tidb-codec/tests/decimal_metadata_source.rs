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

//! Source-shaped coverage for `MyDecimal.DecimalPeak` metadata framing.

use tidb_codec::{decode_decimal, encode_decimal_fixed, inspect_decimal, CodecError};
use tidb_datatype::Decimal;

#[test]
fn decimal_metadata_preserves_precision_scale_length_and_remainder() {
    // Go source: pkg/types/mydecimal.go::DecimalPeak and
    // pkg/util/codec/decimal.go::DecodeDecimal. The first two bytes are the
    // declared precision/frac, followed by the packed coefficient.
    let decimal = Decimal::from_literal("123.4500").negate();
    let mut encoded = Vec::new();
    encode_decimal_fixed(&mut encoded, &decimal, 10, 4).expect("exact fixed decimal");
    encoded.extend_from_slice(&[0xaa, 0xbb]);

    let (remain, metadata) = inspect_decimal(&encoded).expect("decimal metadata");
    assert_eq!(remain, &[0xaa, 0xbb]);
    assert_eq!(metadata.precision(), 10);
    assert_eq!(metadata.scale(), 4);
    assert_eq!(metadata.payload_len(), 7);

    let (decoded_remain, decoded, precision, scale) = decode_decimal(&encoded).unwrap();
    assert_eq!(decoded_remain, &[0xaa, 0xbb]);
    assert_eq!(decoded, decimal);
    assert_eq!((precision, scale), (10, 4));
}

#[test]
fn decimal_metadata_rejects_shape_and_physical_length_errors() {
    assert_eq!(
        inspect_decimal(&[10, 4, 0]),
        Err(CodecError::InsufficientBytes)
    );
    assert_eq!(
        inspect_decimal(&[3, 5, 0, 0]),
        Err(CodecError::DecimalOutOfRange)
    );
}
