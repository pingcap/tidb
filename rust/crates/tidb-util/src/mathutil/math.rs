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

use num_traits::{PrimInt, ToPrimitive};

/// Architecture-specific maximum signed integer.
pub const MAX_INT: isize = isize::MAX;
/// Architecture-specific minimum signed integer.
pub const MIN_INT: isize = isize::MIN;
/// Architecture-specific maximum unsigned integer.
pub const MAX_UINT: usize = usize::MAX;
/// Architecture-specific integer width.
pub const INT_BITS: u32 = usize::BITS;

const UINT_SIZE_TABLE: [u64; 21] = [
    0,
    9,
    99,
    999,
    9_999,
    99_999,
    999_999,
    9_999_999,
    99_999_999,
    999_999_999,
    9_999_999_999,
    99_999_999_999,
    999_999_999_999,
    9_999_999_999_999,
    99_999_999_999_999,
    999_999_999_999_999,
    9_999_999_999_999_999,
    99_999_999_999_999_999,
    999_999_999_999_999_999,
    9_999_999_999_999_999_999,
    u64::MAX,
];

/// Returns the source two's-complement absolute value, including the wrapped
/// `i64::MIN` result.
#[must_use]
pub fn abs(value: i64) -> i64 {
    let sign = value >> 63;
    (value ^ sign).wrapping_sub(sign)
}

/// Efficiently returns the decimal character length of a `u64`.
#[must_use]
pub fn str_len_of_uint64_fast(value: u64) -> usize {
    for (index, limit) in UINT_SIZE_TABLE.iter().enumerate().skip(1) {
        if value <= *limit {
            return index;
        }
    }
    unreachable!("u64::MAX terminates the source lookup table")
}

/// Efficiently returns the decimal character length of an `i64`.
#[must_use]
pub fn str_len_of_int64_fast(value: i64) -> usize {
    usize::from(value < 0) + str_len_of_uint64_fast(abs(value) as u64)
}

/// Reports whether a value is neither NaN nor infinity.
#[must_use]
pub fn is_finite(value: f64) -> bool {
    value.is_finite()
}

/// Restricts a partially ordered value to the source interval.
///
/// A NaN compares neither above nor below the bounds and is therefore returned
/// unchanged, matching Go's ordered-float behavior.
pub fn clamp<T: PartialOrd>(value: T, minimum: T, maximum: T) -> T {
    if value >= maximum {
        maximum
    } else if value <= minimum {
        minimum
    } else {
        value
    }
}

/// Returns the smallest power of two greater than or equal to `value`.
///
/// The caller retains the Go precondition that `value` is positive and the
/// result does not overflow.
#[must_use]
pub fn next_power_of_two(mut value: i64) -> i64 {
    if value & value.wrapping_sub(1) == 0 {
        return value;
    }
    value = value.wrapping_mul(2);
    while value & value.wrapping_sub(1) != 0 {
        value &= value.wrapping_sub(1);
    }
    value
}

/// Divides `total` into at most `batches` positive, near-equal parts.
///
/// # Panics
///
/// Like the source, panics for a zero divisor or a batch count that cannot be
/// represented as an allocation capacity.
#[must_use]
pub fn divide_2_batches<T>(mut total: T, batches: T) -> Vec<T>
where
    T: PrimInt + ToPrimitive,
{
    let capacity = batches
        .to_usize()
        .expect("batch count cannot be represented as a capacity");
    let mut result = Vec::with_capacity(capacity);
    let quotient = total / batches;
    let mut remainder = total % batches;
    while total > T::zero() {
        let mut size = quotient;
        if remainder > T::zero() {
            size = size + T::one();
            remainder = remainder - T::one();
        }
        crate::intest::assert_with_message(size > T::zero(), "size should be positive");
        result.push(size);
        total = total - size;
    }
    result
}

#[cfg(test)]
#[allow(non_snake_case)]
mod tests {
    use super::*;

    #[test]
    fn TestStrLenOfUint64Fast() {
        let mut value = 0x9e37_79b9_7f4a_7c15_u64;
        for _ in 0..1_000_000 {
            value ^= value << 13;
            value ^= value >> 7;
            value ^= value << 17;
            assert_eq!(str_len_of_uint64_fast(value), value.to_string().len());
        }

        let values = [
            0,
            1,
            12,
            123,
            1_234,
            12_345,
            123_456,
            1_234_567,
            12_345_678,
            123_456_789,
            1_234_567_890,
            1_234_567_891,
            12_345_678_912,
            123_456_789_123,
            1_234_567_891_234,
            12_345_678_912_345,
            123_456_789_123_456,
            1_234_567_891_234_567,
            12_345_678_912_345_678,
            123_456_789_123_456_789,
            123_456_789_123_457_890,
            u64::MAX,
        ];
        for value in values {
            assert_eq!(str_len_of_uint64_fast(value), value.to_string().len());
        }
    }

    #[test]
    fn TestClamp() {
        assert_eq!(clamp(100, 1, 3), 3);
        assert_eq!(clamp(2.0_f64, 1.0, 3.0), 2.0);
        assert_eq!(clamp(0.0_f32, 1.0, 3.0), 1.0);
        assert_eq!(clamp(0, 1, 1), 1);
        assert_eq!(clamp(100, 1, 1), 1);
        assert_eq!(clamp("aa", "ab", "xy"), "ab");
        assert_eq!(clamp("yy", "ab", "xy"), "xy");
        assert_eq!(clamp("ab", "ab", "ab"), "ab");
    }

    #[test]
    fn TestNextPowerOfTwo() {
        assert_eq!(next_power_of_two(1), 1);
        assert_eq!(next_power_of_two(3), 4);
        assert_eq!(next_power_of_two(255), 256);
        assert_eq!(next_power_of_two(1024), 1024);
        assert_eq!(next_power_of_two(0xabcd_1234), 0x1_0000_0000);
    }

    #[test]
    fn TestDivide2Batches() {
        assert_eq!(divide_2_batches(0_i32, 1), Vec::<i32>::new());
        assert_eq!(divide_2_batches(1, 1), vec![1]);
        assert_eq!(divide_2_batches(1, 3), vec![1]);
        assert_eq!(divide_2_batches(2, 2), vec![1, 1]);
        assert_eq!(divide_2_batches(2, 10), vec![1, 1]);
        assert_eq!(divide_2_batches(10, 1), vec![10]);
        assert_eq!(divide_2_batches(10, 2), vec![5, 5]);
        assert_eq!(divide_2_batches(10, 3), vec![4, 3, 3]);
        assert_eq!(divide_2_batches(10, 4), vec![3, 3, 2, 2]);
        assert_eq!(divide_2_batches(10, 5), vec![2, 2, 2, 2, 2]);
    }

    #[test]
    fn source_uncovered_boundaries_remain_exact() {
        assert_eq!(abs(i64::MIN), i64::MIN);
        assert_eq!(str_len_of_int64_fast(i64::MIN), 20);
        assert!(is_finite(0.0));
        assert!(!is_finite(f64::NAN));
        assert!(!is_finite(f64::INFINITY));
        assert!(clamp(f64::NAN, 1.0, 3.0).is_nan());
        assert_eq!(INT_BITS, usize::BITS);
        assert_eq!(
            (MAX_INT, MIN_INT, MAX_UINT),
            (isize::MAX, isize::MIN, usize::MAX)
        );
    }
}
