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

use std::cmp::Ordering;

/// Compares corresponding unsigned integer elements.
pub fn vec_compare_uu(left: &[u64], right: &[u64], result: &mut [i64]) {
    assert!(right.len() >= left.len() && result.len() >= left.len());
    for ((left, right), output) in left.iter().zip(right).zip(result) {
        *output = ordering_i64(left.cmp(right));
    }
}

/// Compares corresponding signed integer elements.
pub fn vec_compare_ii(left: &[i64], right: &[i64], result: &mut [i64]) {
    assert!(right.len() >= left.len() && result.len() >= left.len());
    for ((left, right), output) in left.iter().zip(right).zip(result) {
        *output = ordering_i64(left.cmp(right));
    }
}

/// Compares corresponding unsigned-left and signed-right integer elements.
pub fn vec_compare_ui(left: &[u64], right: &[i64], result: &mut [i64]) {
    assert!(right.len() >= left.len() && result.len() >= left.len());
    for ((left, right), output) in left.iter().zip(right).zip(result) {
        *output = compare_int(*left as i64, true, *right, false);
    }
}

/// Compares corresponding signed-left and unsigned-right integer elements.
pub fn vec_compare_iu(left: &[i64], right: &[u64], result: &mut [i64]) {
    assert!(right.len() >= left.len() && result.len() >= left.len());
    for ((left, right), output) in left.iter().zip(right).zip(result) {
        *output = compare_int(*left, false, *right as i64, true);
    }
}

/// Compares two integer bit patterns with independent signedness.
pub fn compare_int(left: i64, left_unsigned: bool, right: i64, right_unsigned: bool) -> i64 {
    let ordering = match (left_unsigned, right_unsigned) {
        (true, true) => (left as u64).cmp(&(right as u64)),
        (true, false) if right < 0 || (left as u64) > i64::MAX as u64 => Ordering::Greater,
        (true, false) => left.cmp(&right),
        (false, true) if left < 0 || (right as u64) > i64::MAX as u64 => Ordering::Less,
        (false, true) | (false, false) => left.cmp(&right),
    };
    ordering_i64(ordering)
}

const fn ordering_i64(ordering: Ordering) -> i64 {
    match ordering {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vec_compare_int_and_uint() {
        let ascending_i: Vec<i64> = (0..10).collect();
        let descending_i: Vec<i64> = (0..10).rev().collect();
        let ascending_u: Vec<u64> = (0..10).collect();
        let descending_u: Vec<u64> = (0..10).rev().collect();
        let expected = [-1, -1, -1, -1, -1, 1, 1, 1, 1, 1];
        let mut result = [0; 10];

        vec_compare_uu(&ascending_u, &descending_u, &mut result);
        assert_eq!(result, expected);
        vec_compare_ii(&ascending_i, &descending_i, &mut result);
        assert_eq!(result, expected);
        vec_compare_iu(&ascending_i, &descending_u, &mut result);
        assert_eq!(result, expected);
        vec_compare_ui(&ascending_u, &descending_i, &mut result);
        assert_eq!(result, expected);

        vec_compare_uu(&ascending_u, &ascending_u, &mut result);
        assert_eq!(result, [0; 10]);
        vec_compare_ii(&ascending_i, &ascending_i, &mut result);
        assert_eq!(result, [0; 10]);
        vec_compare_iu(&ascending_i, &ascending_u, &mut result);
        assert_eq!(result, [0; 10]);

        let negative: Vec<i64> = (0..10).map(|value| -value).collect();
        vec_compare_iu(&negative, &descending_u, &mut result);
        assert_eq!(result, [-1; 10]);
        let ascending_negative: Vec<i64> = (0..10).map(|value| value - 9).collect();
        vec_compare_ui(&ascending_u, &ascending_negative, &mut result);
        assert_eq!(result, [1; 10]);

        let too_large = [i64::MAX as u64 + 1; 10];
        vec_compare_iu(&ascending_i, &too_large, &mut result);
        assert_eq!(result, [-1; 10]);
        vec_compare_ui(&too_large, &ascending_i, &mut result);
        assert_eq!(result, [1; 10]);
    }

    #[test]
    fn compare_int_preserves_signed_unsigned_boundaries() {
        assert_eq!(compare_int(-1, false, 1, true), -1);
        assert_eq!(compare_int(1, true, -1, false), 1);
        assert_eq!(compare_int(i64::MIN, true, i64::MAX, false), 1);
        assert_eq!(compare_int(i64::MAX, false, i64::MIN, true), -1);
        assert_eq!(compare_int(-1, true, -1, true), 0);
    }
}
