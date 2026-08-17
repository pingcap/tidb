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

//! Complete transcreation of Go `pkg/util/split.go`: region pre-split key
//! interpolation for `SPLIT TABLE ... REGIONS`.
//!
//! Go computes evenly-spaced split keys between a `lower` and `upper` byte
//! bound by skipping their longest shared prefix, treating the remaining
//! bytes (right-padded to 8 bytes, with `0x00` for `lower` and `0xff` for
//! `upper`) as big-endian `uint64`s, dividing the range by the requested
//! region count, and re-appending the shared prefix to each stepped value.
//! `binary.BigEndian.PutUint64`/`.Uint64` become `u64::to_be_bytes`/
//! `u64::from_be_bytes` — bit-for-bit equivalent on the wire.

/// Go `GetValuesList`: gets `num` values between `lower` and `upper`, used to
/// split `num` regions. Only `num - 1` interior keys are needed to divide the
/// range into `num` regions, so that many values are appended to
/// `values_list` and returned.
pub fn get_values_list(
    lower: &[u8],
    upper: &[u8],
    num: usize,
    mut values_list: Vec<Vec<u8>>,
) -> Vec<Vec<u8>> {
    let common_prefix_idx = longest_common_prefix_len(lower, upper);
    let step = get_step_value(
        &lower[common_prefix_idx..],
        &upper[common_prefix_idx..],
        num,
    );
    let mut start_v = get_uint64_from_bytes(&lower[common_prefix_idx..], 0);
    // To get `num` regions, only need to split `num-1` idx keys.
    for _ in 0..num.saturating_sub(1) {
        let mut value = Vec::with_capacity(common_prefix_idx + 8);
        value.extend_from_slice(&lower[..common_prefix_idx]);
        start_v = start_v.wrapping_add(step);
        value.extend_from_slice(&start_v.to_be_bytes());
        values_list.push(value);
    }
    values_list
}

/// Go's private `longestCommonPrefixLen`: gets the longest common prefix
/// byte length.
fn longest_common_prefix_len(s1: &[u8], s2: &[u8]) -> usize {
    s1.iter().zip(s2.iter()).take_while(|(a, b)| a == b).count()
}

/// Go's private `getStepValue`: gets the step between the lower and upper
/// value. `step = (upper - lower) / num`. Converts each byte slice to a
/// `uint64` first.
fn get_step_value(lower: &[u8], upper: &[u8], num: usize) -> u64 {
    let lower_uint = get_uint64_from_bytes(lower, 0);
    let upper_uint = get_uint64_from_bytes(upper, 0xff);
    // Go divides by an `int` converted to `uint64`; `num` is always >= 1 in
    // every call site (`GetValuesList` requires at least one region), so
    // this mirrors Go's unchecked `/ uint64(num)` without a saturating guard.
    (upper_uint - lower_uint) / num as u64
}

/// Go's private `getUint64FromBytes`: gets a `uint64` from the `bs` byte
/// slice. If `len(bs) < 8`, right-pads with `pad` first.
fn get_uint64_from_bytes(bs: &[u8], pad: u8) -> u64 {
    if bs.len() < 8 {
        let mut buf = [pad; 8];
        buf[..bs.len()].copy_from_slice(bs);
        u64::from_be_bytes(buf)
    } else {
        // Go's `binary.BigEndian.Uint64` reads only the first 8 bytes of a
        // longer slice; mirror that instead of requiring an exact length.
        u64::from_be_bytes(bs[..8].try_into().unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Go `TestLongestCommonPrefixLen` (pkg/util/split_test.go).
    #[test]
    fn test_longest_common_prefix_len() {
        let cases: [(&[u8], &[u8], usize); 8] = [
            (b"", b"", 0),
            (b"", b"a", 0),
            (b"a", b"", 0),
            (b"a", b"a", 1),
            (b"ab", b"a", 1),
            (b"a", b"ab", 1),
            (b"b", b"ab", 0),
            (b"ba", b"ab", 0),
        ];
        for (s1, s2, l) in cases {
            assert_eq!(longest_common_prefix_len(s1, s2), l);
        }
    }

    // Go `TestGetStepValue` (pkg/util/split_test.go).
    #[test]
    fn test_get_step_value() {
        let cases: [(&[u8], &[u8], usize, u64); 7] = [
            (b"", b"", 0, u64::MAX),
            (
                &[0],
                &[128],
                0,
                u64::from_be_bytes([128, 255, 255, 255, 255, 255, 255, 255]),
            ),
            (
                b"a",
                b"z",
                0,
                u64::from_be_bytes([b'z' - b'a', 255, 255, 255, 255, 255, 255, 255]),
            ),
            (
                b"abc",
                b"z",
                0,
                u64::from_be_bytes([b'z' - b'a', 255 - b'b', 255 - b'c', 255, 255, 255, 255, 255]),
            ),
            (
                b"abc",
                b"xyz",
                0,
                u64::from_be_bytes([
                    b'x' - b'a',
                    b'y' - b'b',
                    b'z' - b'c',
                    255,
                    255,
                    255,
                    255,
                    255,
                ]),
            ),
            (
                b"abc",
                b"axyz",
                1,
                u64::from_be_bytes([b'x' - b'b', b'y' - b'c', b'z', 255, 255, 255, 255, 255]),
            ),
            (
                b"abc0123456",
                b"xyz01234",
                0,
                u64::from_be_bytes([b'x' - b'a', b'y' - b'b', b'z' - b'c', 0, 0, 0, 0, 0]),
            ),
        ];

        for (lower, upper, l, v) in cases {
            let common = longest_common_prefix_len(lower, upper);
            assert_eq!(common, l);
            let v0 = get_step_value(&lower[common..], &upper[common..], 1);
            assert_eq!(v0, v);
        }
    }

    // New coverage (no Go equivalent in split_test.go): pins
    // `get_uint64_from_bytes`'s padding contract directly, since Go's own
    // tests only exercise it indirectly through `getStepValue`.
    #[test]
    fn get_uint64_from_bytes_pads_short_input() {
        assert_eq!(get_uint64_from_bytes(&[], 0), 0);
        assert_eq!(get_uint64_from_bytes(&[], 0xff), u64::MAX);
        assert_eq!(
            get_uint64_from_bytes(&[1, 2], 0),
            u64::from_be_bytes([1, 2, 0, 0, 0, 0, 0, 0])
        );
        assert_eq!(
            get_uint64_from_bytes(&[1, 2], 0xff),
            u64::from_be_bytes([1, 2, 255, 255, 255, 255, 255, 255])
        );
    }

    // New coverage: `get_uint64_from_bytes` on an exact 8-byte slice reads it
    // as-is, matching `binary.BigEndian.Uint64`'s behavior for `len(bs) == 8`.
    #[test]
    fn get_uint64_from_bytes_exact_length_reads_directly() {
        let bytes = [1u8, 2, 3, 4, 5, 6, 7, 8];
        assert_eq!(get_uint64_from_bytes(&bytes, 0), u64::from_be_bytes(bytes));
    }

    // New coverage: end-to-end `get_values_list` on inputs with no shared
    // prefix (they differ in their very first byte, so
    // `longest_common_prefix_len` is 0 and the whole 8 bytes are the
    // "tail"), to pin the full call chain — prefix skip, step, and
    // re-prefixing — together against hand-computed expected keys.
    #[test]
    fn get_values_list_splits_num_minus_one_keys() {
        let lower = [0u8; 8];
        let upper = [0xffu8, 0, 0, 0, 0, 0, 0, 0];
        let result = get_values_list(&lower, &upper, 10, Vec::new());
        assert_eq!(result.len(), 9);

        let lower_uint = u64::from_be_bytes(lower);
        let upper_uint = u64::from_be_bytes(upper);
        let step = (upper_uint - lower_uint) / 10;
        for (i, value) in result.iter().enumerate() {
            let expected = step * (i as u64 + 1);
            assert_eq!(value.as_slice(), expected.to_be_bytes());
        }
    }

    // New coverage: a shared prefix is preserved verbatim on every returned
    // key, matching Go's `value = append(value, lower[:commonPrefixIdx]...)`.
    // `lower`/`upper` share the "ta" prefix and differ starting at index 2,
    // so `longest_common_prefix_len` is exactly 2.
    #[test]
    fn get_values_list_preserves_common_prefix() {
        let lower = [b't', b'a', 0, 0, 0, 0, 0, 0, 0, 0];
        let upper = [b't', b'a', 0xff, 0, 0, 0, 0, 0, 0, 0];
        let result = get_values_list(&lower, &upper, 4, Vec::new());
        assert_eq!(result.len(), 3);
        for value in &result {
            assert_eq!(&value[..2], b"ta");
            assert_eq!(value.len(), 2 + 8);
        }
    }

    // New coverage: `values_list` accumulates onto a caller-supplied vector
    // rather than replacing it, matching Go's `valuesList` parameter being
    // both read (only for its initial contents) and returned.
    #[test]
    fn get_values_list_appends_to_existing_vec() {
        let seed = vec![vec![9u8]];
        let lower = 0u64.to_be_bytes();
        let upper = 10u64.to_be_bytes();
        let result = get_values_list(&lower, &upper, 2, seed);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], vec![9u8]);
    }
}
