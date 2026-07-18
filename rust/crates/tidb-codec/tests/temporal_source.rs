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

//! Source-derived checks for `pkg/types/time.go`'s packed payload and
//! `pkg/util/codec/codec.go::EncodeMySQLTime`.

use tidb_codec::{
    decode_packed_time, decode_value, encode_packed_time, CodecError, RawValue, VALUE_UINT_FLAG,
};
use tidb_datatype::{PackedTime, PackedTimeError};

#[test]
fn packed_time_uses_go_calendar_bit_layout_and_round_trips_parts() {
    let value = PackedTime::from_parts(2011, 1, 1, 0, 0, 0, 0).unwrap();
    assert_eq!(value.raw(), 0x1988_0200_0000_0000);
    assert_eq!(
        value.parts(),
        tidb_datatype::PackedTimeParts {
            year: 2011,
            month: 1,
            day: 1,
            hour: 0,
            minute: 0,
            second: 0,
            microsecond: 0,
        }
    );

    let fractional = PackedTime::from_parts(2000, 6, 1, 10, 11, 12, 999_999).unwrap();
    assert_eq!(fractional.parts().microsecond, 999_999);

    let mut encoded = vec![VALUE_UINT_FLAG];
    encode_packed_time(&mut encoded, fractional);
    encoded.extend_from_slice(&[0xaa, 0xbb]);
    let (remain, raw) = decode_value(&encoded).unwrap();
    assert_eq!(remain, &[0xaa, 0xbb]);
    assert_eq!(raw.flag, VALUE_UINT_FLAG);
    assert_eq!(raw.packed_time().unwrap(), fractional);
}

#[test]
fn packed_time_order_and_zero_match_source_codec_rows() {
    // `pkg/util/codec/codec_test.go::TestTime`: zero and the original
    // 2011/2000/2001 ordering rows all compare on the packed uint payload.
    let zero = PackedTime::ZERO;
    let year_2000 = PackedTime::from_parts(2000, 10, 10, 0, 0, 0, 0).unwrap();
    let year_2001 = PackedTime::from_parts(2001, 10, 10, 0, 0, 0, 0).unwrap();
    let year_2011 = PackedTime::from_parts(2011, 10, 10, 0, 0, 0, 0).unwrap();
    assert!(zero.is_zero());
    assert!(year_2000 < year_2001);
    assert!(year_2001 < year_2011);

    let mut buffer = Vec::new();
    encode_packed_time(&mut buffer, zero);
    assert_eq!(buffer, [0_u8; 8]);
    let encoded_with_remainder = [buffer.as_slice(), &[1, 2]].concat();
    let (remainder, decoded) = decode_packed_time(&encoded_with_remainder).unwrap();
    assert_eq!(remainder, &[1, 2]);
    assert_eq!(decoded, zero);
}

#[test]
fn packed_time_rejects_only_physical_boundary_errors() {
    assert_eq!(
        decode_packed_time(&[0; 7]),
        Err(CodecError::InsufficientBytes)
    );
    assert_eq!(
        PackedTime::from_parts(10_000, 1, 1, 0, 0, 0, 0),
        Err(PackedTimeError::OutOfRange("year"))
    );
    assert_eq!(
        RawValue {
            flag: 7,
            payload: &[0; 8],
        }
        .packed_time(),
        Err(CodecError::InvalidEncoding(
            "packed temporal value needs uint tag"
        ))
    );
}
