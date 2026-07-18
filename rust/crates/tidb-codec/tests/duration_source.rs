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

//! Source-shaped coverage for the fixed duration branch of `EncodeValue`.

use tidb_codec::{
    decode_duration, decode_value, encode_duration, CodecError, RawDuration, VALUE_DURATION_FLAG,
};

#[test]
fn duration_value_uses_signed_comparable_int_and_max_fsp() {
    // Go source: pkg/util/codec/codec.go::{EncodeValue, DecodeOne}. The
    // duration flag is followed by EncodeInt(int64(Duration.Duration)); the
    // decoder consumes eight bytes and assigns types.MaxFsp (6).
    let samples = [0_i64, 1_000_000_000, -1_234_567, i64::MIN, i64::MAX];
    for nanoseconds in samples {
        let mut payload = Vec::new();
        encode_duration(&mut payload, nanoseconds);
        assert_eq!(payload.len(), 8);
        let mut framed = payload;
        framed.push(0xee);
        let (remain, decoded) = decode_duration(&framed).expect("duration payload");
        assert_eq!(remain, &[0xee]);
        assert_eq!(decoded, RawDuration::from_nanoseconds(nanoseconds));
        assert_eq!(decoded.nanoseconds(), nanoseconds);
        assert_eq!(decoded.fsp(), 6);
    }
}

#[test]
fn duration_value_boundary_preserves_tag_and_remainder() {
    let mut encoded = vec![VALUE_DURATION_FLAG];
    encode_duration(&mut encoded, -42);
    encoded.extend_from_slice(&[0xaa, 0xbb]);

    let (remain, raw) = decode_value(&encoded).expect("duration value");
    assert_eq!(remain, &[0xaa, 0xbb]);
    assert_eq!(raw.flag, VALUE_DURATION_FLAG);
    assert_eq!(
        raw.decode_datum(),
        Err(CodecError::UnsupportedValueTag(VALUE_DURATION_FLAG))
    );
    let (payload_remain, decoded) = decode_duration(raw.payload).expect("duration payload");
    assert!(payload_remain.is_empty());
    assert_eq!(decoded.nanoseconds(), -42);
    assert_eq!(decoded.fsp(), 6);
}

#[test]
fn duration_value_rejects_only_short_physical_payloads() {
    assert_eq!(decode_duration(&[0; 7]), Err(CodecError::InsufficientBytes));
}

#[test]
fn duration_components_follow_go_split_duration_clock_rules() {
    // Go source: pkg/types/time.go::splitDuration and
    // pkg/types/time_test.go::TestDurationClock. The parser itself remains
    // outside this codec leaf; these values are the exact durations produced
    // by the source table before the clock accessors are checked.
    let hour = 60_i64 * 60 * 1_000_000_000;
    let minute = 60_i64 * 1_000_000_000;
    let second = 1_000_000_000_i64;
    let cases: [(i64, u64, u8, u8, u32); 3] = [
        (
            11 * hour + 11 * minute + 11 * second + 110_000 * 1_000,
            11,
            11,
            11,
            110_000,
        ),
        (
            35 * hour + 11 * minute + 11 * second + 11 * 1_000,
            35,
            11,
            11,
            11,
        ),
        (
            -(11 * hour + 11 * minute + 11 * second + 11 * 1_000),
            11,
            11,
            11,
            11,
        ),
    ];
    for (nanoseconds, hours, minutes, seconds, microseconds) in cases {
        let parts = RawDuration::from_nanoseconds(nanoseconds).parts();
        assert_eq!(parts.hours(), hours);
        assert_eq!(parts.minutes(), minutes);
        assert_eq!(parts.seconds(), seconds);
        assert_eq!(parts.microseconds(), microseconds);
        assert_eq!(parts.sign(), if nanoseconds < 0 { -1 } else { 1 });
    }
}
