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
    if num == 1 {
        return;
    }
    let common_prefix_idx = longest_common_prefix_len(lower, upper);
    let (prefix, lower) = lower.split_at(common_prefix_idx);
    let (mut start_v, step) = get_start_and_step(lower, &upper[common_prefix_idx..], num);
    // To get `num` regions, only need to split `num-1` idx keys.
    values_list.reserve(num - 1);
    for _ in 1..num {
        let mut value = Vec::with_capacity(prefix.len() + 8);
        value.extend_from_slice(prefix);
        start_v = start_v.wrapping_add(step);
        value.extend_from_slice(&start_v.to_be_bytes());
        values_list.push(value);
    }
}

/// Go `longestCommonPrefixLen`.
fn longest_common_prefix_len(s1: &[u8], s2: &[u8]) -> usize {
    let len = s1.len().min(s2.len());
    let (left_chunks, left_tail) = s1[..len].as_chunks::<8>();
    let (right_chunks, right_tail) = s2[..len].as_chunks::<8>();
    for (index, (left, right)) in left_chunks.iter().zip(right_chunks).enumerate() {
        let differing_bits = u64::from_be_bytes(*left) ^ u64::from_be_bytes(*right);
        if differing_bits != 0 {
            return index * 8 + differing_bits.leading_zeros() as usize / 8;
        }
    }
    left_chunks.len() * 8
        + left_tail
            .iter()
            .zip(right_tail)
            .position(|(left, right)| left != right)
            .unwrap_or(left_tail.len())
}

/// Go's initial value and `getStepValue` over the first eight suffix bytes.
fn get_start_and_step(lower: &[u8], upper: &[u8], num: usize) -> (u64, u64) {
    let start = get_uint64_from_bytes(lower, 0);
    let end = get_uint64_from_bytes(upper, 0xff);
    let step = end.wrapping_sub(start) / num as u64;
    (start, step)
}

/// Go `getUint64FromBytes`: the first eight bytes, padded with `pad`.
fn get_uint64_from_bytes(bs: &[u8], pad: u8) -> u64 {
    if let Some(first) = bs.first_chunk::<8>() {
        return u64::from_be_bytes(*first);
    }
    let mut buf = [pad; 8];
    buf[..bs.len()].copy_from_slice(bs);
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
        let left = [0; 66];
        let mut right = left;
        for i in 0..right.len() {
            right[i] = 1;
            assert_eq!(longest_common_prefix_len(&left, &right), i);
            right[i] = 0;
        }
        assert_eq!(longest_common_prefix_len(&left, &right[..65]), 65);
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
            assert_eq!(get_start_and_step(&lower[l..], &upper[l..], 1).1, v);
        }
    }
}
