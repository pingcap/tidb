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

//! Source-derived checks for `types.PeekBytesAsJSON` and `codec.DecodeOne`.

use tidb_codec::{
    decode_json, decode_value, peek_json_len, CodecError, RawJsonTemporalKind,
    JSON_TYPE_CODE_ARRAY, JSON_TYPE_CODE_DATE, JSON_TYPE_CODE_DATETIME, JSON_TYPE_CODE_DURATION,
    JSON_TYPE_CODE_OBJECT, JSON_TYPE_CODE_OPAQUE, JSON_TYPE_CODE_STRING, JSON_TYPE_CODE_TIMESTAMP,
    VALUE_JSON_FLAG,
};
use tidb_datatype::PackedTime;

#[test]
fn json_value_boundary_preserves_primitive_and_remainder() {
    // `pkg/types/json_binary.go` stores a string as type + uvarint length +
    // bytes. `pkg/util/codec/codec.go::EncodeValue` prepends jsonFlag.
    let encoded = [
        VALUE_JSON_FLAG,
        JSON_TYPE_CODE_STRING,
        3,
        b'a',
        b'b',
        b'c',
        0xaa,
    ];
    let (remain, raw) = decode_value(&encoded).unwrap();
    assert_eq!(remain, &[0xaa]);
    let json = raw.json().unwrap();
    assert_eq!(json.type_code(), JSON_TYPE_CODE_STRING);
    assert_eq!(json.value(), &[3, b'a', b'b', b'c']);
}

#[test]
fn json_container_and_opaque_boundaries_follow_source_sizes() {
    // Empty large object: element-count=0, total-size=8 (the two uint32
    // header fields). The size excludes the type byte.
    let object = [JSON_TYPE_CODE_OBJECT, 0, 0, 0, 0, 8, 0, 0, 0];
    assert_eq!(peek_json_len(&object).unwrap(), object.len());
    let object_with_remainder = [object.as_slice(), &[1, 2]].concat();
    let (remain, decoded) = decode_json(&object_with_remainder).unwrap();
    assert_eq!(remain, &[1, 2]);
    assert_eq!(decoded.type_code(), JSON_TYPE_CODE_OBJECT);
    assert_eq!(decoded.value(), &object[1..]);

    // Opaque values are type + opaque type id + uvarint length + bytes.
    let opaque = [JSON_TYPE_CODE_OPAQUE, 0xf5, 3, b'x', b'y', b'z'];
    let opaque_with_remainder = [opaque.as_slice(), &[9]].concat();
    let (remain, decoded) = decode_json(&opaque_with_remainder).unwrap();
    assert_eq!(remain, &[9]);
    assert_eq!(decoded.type_code(), JSON_TYPE_CODE_OPAQUE);
    assert_eq!(decoded.value(), &opaque[1..]);
}

#[test]
fn json_duration_keeps_go_nanoseconds_and_fsp_fields() {
    // `pkg/types/json_binary.go` duration ::= uint64 uint32, both little
    // endian under jsonEndian. It is not SQL rounding or timezone conversion.
    let mut duration = vec![JSON_TYPE_CODE_DURATION];
    duration.extend_from_slice(&(-1_234_567_i64).to_le_bytes());
    duration.extend_from_slice(&6_u32.to_le_bytes());
    let duration_with_remainder = [duration.as_slice(), &[0xee]].concat();
    let (remain, decoded) = decode_json(&duration_with_remainder).unwrap();
    assert_eq!(remain, &[0xee]);
    assert_eq!(decoded.duration().unwrap(), (-1_234_567, 6));

    let mut encoded = vec![VALUE_JSON_FLAG];
    encoded.extend_from_slice(&duration);
    let encoded_with_remainder = [encoded, vec![0xdd]].concat();
    let (remain, raw) = decode_value(&encoded_with_remainder).unwrap();
    assert_eq!(remain, &[0xdd]);
    assert_eq!(raw.json().unwrap().type_code(), JSON_TYPE_CODE_DURATION);
}

#[test]
fn json_temporal_keeps_type_code_and_packed_calendar_bits() {
    // `pkg/types/json_binary.go::GetTimeWithFsp` reads date, datetime, and
    // timestamp values as little-endian packed CoreTime. FSP and timezone
    // are deliberately absent from BinaryJSON.
    let packed = PackedTime::from_parts(2026, 7, 16, 12, 34, 56, 789_000).unwrap();
    let cases = [
        (JSON_TYPE_CODE_DATE, RawJsonTemporalKind::Date),
        (JSON_TYPE_CODE_DATETIME, RawJsonTemporalKind::Datetime),
        (JSON_TYPE_CODE_TIMESTAMP, RawJsonTemporalKind::Timestamp),
    ];
    for (type_code, kind) in cases {
        let encoded = [&[type_code][..], &packed.raw().to_le_bytes(), &[0xaa][..]].concat();
        let (remain, decoded) = decode_json(&encoded).unwrap();
        assert_eq!(remain, &[0xaa]);
        let temporal = decoded.temporal().unwrap();
        assert_eq!(temporal.kind(), kind);
        assert_eq!(temporal.packed_time(), packed);
    }
    assert_eq!(
        decode_json(&[JSON_TYPE_CODE_DATE, 0]),
        Err(CodecError::InsufficientBytes)
    );
    let duration = [JSON_TYPE_CODE_DURATION, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let (_, duration) = decode_json(&duration).unwrap();
    assert_eq!(
        duration.temporal(),
        Err(CodecError::InvalidEncoding(
            "JSON payload is not a temporal value"
        ))
    );
}

#[test]
fn json_boundaries_reject_unknown_or_short_physical_payloads() {
    assert_eq!(
        peek_json_len(&[JSON_TYPE_CODE_ARRAY, 0, 0, 0]),
        Err(CodecError::InsufficientBytes)
    );
    assert_eq!(
        peek_json_len(&[JSON_TYPE_CODE_OBJECT, 0, 0, 0, 0, 7, 0, 0, 0]),
        Err(CodecError::InvalidEncoding(
            "JSON container size is smaller than its header"
        ))
    );
    assert_eq!(
        peek_json_len(&[0xff]),
        Err(CodecError::InvalidEncoding("unknown JSON type code"))
    );
}
