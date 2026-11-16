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

//! Go `pkg/util/split.go`: evenly spaced keys between two keys.

/// Go `GetValuesList`: `num - 1` keys evenly spaced between `lower` and
/// `upper`, over the eight bytes after their common prefix, appended to
/// `values_list`.
pub fn get_values_list(lower: &[u8], upper: &[u8], num: usize, values_list: &mut Vec<Vec<u8>>) {
    let common_prefix_idx = longest_common_prefix_len(lower, upper);
    let step = get_step_value(&lower[common_prefix_idx..], &upper[common_prefix_idx..], num);
    let mut start_v = get_uint64_from_bytes(&lower[common_prefix_idx..], 0);
    // To get `num` regions, only need to split `num-1` idx keys.
    for _ in 1..num {
        let mut value = Vec::with_capacity(common_prefix_idx + 8);
        value.extend_from_slice(&lower[..common_prefix_idx]);
        start_v = start_v.wrapping_add(step);
        value.extend_from_slice(&start_v.to_be_bytes());
        values_list.push(value);
    }
}

/// Go `longestCommonPrefixLen`.
fn longest_common_prefix_len(s1: &[u8], s2: &[u8]) -> usize {
    s1.iter().zip(s2).take_while(|(a, b)| a == b).count()
}

/// Go `getStepValue`: `(upper - lower) / num` over their first eight bytes.
fn get_step_value(lower: &[u8], upper: &[u8], num: usize) -> u64 {
    let lower_uint = get_uint64_from_bytes(lower, 0);
    let upper_uint = get_uint64_from_bytes(upper, 0xff);
    upper_uint.wrapping_sub(lower_uint) / num as u64
}

/// Go `getUint64FromBytes`: the first eight bytes, padded with `pad`.
fn get_uint64_from_bytes(bs: &[u8], pad: u8) -> u64 {
    let mut buf = [pad; 8];
    let len = bs.len().min(8);
    buf[..len].copy_from_slice(&bs[..len]);
    u64::from_be_bytes(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestLongestCommonPrefixLen`.
    #[test]
    fn test_longest_common_prefix_len() {
        let cases = [("", "", 0), ("", "a", 0), ("a", "", 0), ("a", "a", 1), ("ab", "a", 1), ("a", "ab", 1), ("b", "ab", 0), ("ba", "ab", 0)];
        for (s1, s2, l) in cases {
            assert_eq!(longest_common_prefix_len(s1.as_bytes(), s2.as_bytes()), l);
        }
    }

    /// Go `TestGetStepValue`.
    #[test]
    fn test_get_step_value() {
        let be = |b: [u8; 8]| u64::from_be_bytes(b);
        let cases: [(&[u8], &[u8], usize, u64); 7] = [
            (b"", b"", 0, u64::MAX),
            (&[0], &[128], 0, be([128, 255, 255, 255, 255, 255, 255, 255])),
            (b"a", b"z", 0, be([b'z' - b'a', 255, 255, 255, 255, 255, 255, 255])),
            (b"abc", b"z", 0, be([b'z' - b'a', 255 - b'b', 255 - b'c', 255, 255, 255, 255, 255])),
            (b"abc", b"xyz", 0, be([b'x' - b'a', b'y' - b'b', b'z' - b'c', 255, 255, 255, 255, 255])),
            (b"abc", b"axyz", 1, be([b'x' - b'b', b'y' - b'c', b'z', 255, 255, 255, 255, 255])),
            (b"abc0123456", b"xyz01234", 0, be([b'x' - b'a', b'y' - b'b', b'z' - b'c', 0, 0, 0, 0, 0])),
        ];
        for (lower, upper, l, v) in cases {
            assert_eq!(longest_common_prefix_len(lower, upper), l);
            assert_eq!(get_step_value(&lower[l..], &upper[l..], 1), v);
        }
    }
}
