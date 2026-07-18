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

//! Direct translation of `pkg/types/overflow_test.go`.

use super::overflow::{
    add_duration, add_int64, add_integer, add_uint64, div_int64, div_int_with_uint,
    div_uint_with_int, mul_int64, mul_integer, mul_uint64, sub_int64, sub_int_with_uint,
    sub_uint64, sub_uint_with_int, OverflowType,
};

fn assert_u64(result: Result<u64, impl std::fmt::Debug>, want: u64, overflow: bool) {
    assert_eq!(result.is_err(), overflow);
    if !overflow {
        assert_eq!(result.unwrap(), want);
    }
}

#[test]
fn test_add() {
    for (lhs, rhs, want, overflow) in [
        (u64::MAX, 1, 0, true),
        (u64::MAX, 0, u64::MAX, false),
        (1, 1, 2, false),
    ] {
        assert_u64(add_uint64(lhs, rhs), want, overflow);
    }
    for (lhs, rhs, want, overflow) in [
        (i64::MAX, 1, 0, true),
        (i64::MAX, 0, i64::MAX, false),
        (0, i64::MIN, i64::MIN, false),
        (-1, i64::MIN, 0, true),
        (i64::MAX, i64::MIN, -1, false),
        (1, 1, 2, false),
        (1, -1, 0, false),
    ] {
        let result = add_int64(lhs, rhs);
        assert_eq!(result.is_err(), overflow);
        let duration = add_duration(lhs, rhs);
        assert_eq!(duration.is_err(), overflow);
        if !overflow {
            assert_eq!(result.unwrap(), want);
            assert_eq!(duration.unwrap(), want);
        }
    }
    for (lhs, rhs, want, overflow) in [
        (u64::MAX, i64::MIN, i64::MAX as u64, false),
        (i64::MAX as u64, i64::MIN, 0, true),
        (0, -1, 0, true),
        (1, -1, 0, false),
        (0, 1, 1, false),
        (1, 1, 2, false),
    ] {
        assert_u64(add_integer(lhs, rhs), want, overflow);
    }
}

#[test]
fn test_sub() {
    for (lhs, rhs, want, overflow) in [
        (u64::MAX, 1, u64::MAX - 1, false),
        (u64::MAX, 0, u64::MAX, false),
        (0, u64::MAX, 0, true),
        (0, 1, 0, true),
        (1, u64::MAX, 0, true),
        (1, 1, 0, false),
    ] {
        assert_u64(sub_uint64(lhs, rhs), want, overflow);
    }
    for (lhs, rhs, want, overflow) in [
        (i64::MIN, 0, i64::MIN, false),
        (i64::MIN, 1, 0, true),
        (i64::MAX, -1, 0, true),
        (0, i64::MIN, 0, true),
        (-1, i64::MIN, i64::MAX, false),
        (i64::MIN, i64::MAX, 0, true),
        (i64::MIN, i64::MIN, 0, false),
        (i64::MIN, -i64::MAX, -1, false),
        (1, 1, 0, false),
    ] {
        let result = sub_int64(lhs, rhs);
        assert_eq!(result.is_err(), overflow);
        if !overflow {
            assert_eq!(result.unwrap(), want);
        }
    }
    for (lhs, rhs, want, overflow) in [
        (0, i64::MIN, (i64::MIN).unsigned_abs(), false),
        (0, 1, 0, true),
        (u64::MAX, i64::MIN, 0, true),
        (i64::MAX as u64, i64::MIN, 2 * i64::MAX as u64 + 1, false),
        (u64::MAX, -1, 0, true),
        (0, -1, 1, false),
        (1, 1, 0, false),
    ] {
        assert_u64(sub_uint_with_int(lhs, rhs), want, overflow);
    }
    for (lhs, rhs, want, overflow) in [
        (i64::MIN, 0, 0, true),
        (i64::MAX, 0, i64::MAX as u64, false),
        (i64::MAX, u64::MAX, 0, true),
        (-1, 0, 0, true),
        (1, 1, 0, false),
    ] {
        assert_u64(sub_int_with_uint(lhs, rhs), want, overflow);
    }
    assert_eq!(
        sub_int_with_uint(-1, 0)
            .expect_err("negative signed minuend must overflow")
            .to_string(),
        "BIGINT UNSIGNED value is out of range in '(-1, 0)'"
    );
}

#[test]
fn test_mul() {
    for (lhs, rhs, want, overflow) in [
        (u64::MAX, 1, u64::MAX, false),
        (u64::MAX, 0, 0, false),
        (u64::MAX, 2, 0, true),
        (1, 1, 1, false),
    ] {
        assert_u64(mul_uint64(lhs, rhs), want, overflow);
    }
    for (lhs, rhs, want, overflow) in [
        (i64::MAX, 1, i64::MAX, false),
        (i64::MIN, 1, i64::MIN, false),
        (i64::MAX, -1, -i64::MAX, false),
        (i64::MIN, -1, 0, true),
        (i64::MIN, 0, 0, false),
        (i64::MAX, 0, 0, false),
        (i64::MAX, i64::MAX, 0, true),
        (i64::MAX, i64::MIN, 0, true),
        (i64::MIN / 10, 11, 0, true),
        (1, 1, 1, false),
    ] {
        let result = mul_int64(lhs, rhs);
        assert_eq!(result.is_err(), overflow);
        if !overflow {
            assert_eq!(result.unwrap(), want);
        }
    }
    for (lhs, rhs, want, overflow) in [
        (u64::MAX, 0, 0, false),
        (0, -1, 0, false),
        (1, -1, 0, true),
        (u64::MAX, -1, 0, true),
        (u64::MAX, 10, 0, true),
        (1, 1, 1, false),
    ] {
        assert_u64(mul_integer(lhs, rhs), want, overflow);
    }
}

#[test]
fn test_div() {
    for (lhs, rhs, want, overflow) in [
        (i64::MAX, 1, i64::MAX, false),
        (i64::MIN, 1, i64::MIN, false),
        (i64::MIN, -1, 0, true),
        (i64::MAX, -1, -i64::MAX, false),
        (1, -1, -1, false),
        (-1, 1, -1, false),
        (-1, 2, 0, false),
        (i64::MIN, 2, i64::MIN / 2, false),
    ] {
        let result = div_int64(lhs, rhs);
        assert_eq!(result.is_err(), overflow);
        if !overflow {
            assert_eq!(result.unwrap(), want);
        }
    }
    for (lhs, rhs, want, overflow) in [
        (0, -1, 0, false),
        (1, -1, 0, true),
        (i64::MAX as u64, i64::MIN, 0, false),
        (i64::MAX as u64, -1, 0, true),
        (100, 20, 5, false),
    ] {
        assert_u64(div_uint_with_int(lhs, rhs), want, overflow);
    }
    let error = div_int_with_uint(i64::MIN, i64::MAX as u64)
        .expect_err("negative signed dividend must overflow");
    assert_eq!(error.kind(), OverflowType::BigIntUnsigned);
    assert_eq!(
        error.to_string(),
        "BIGINT UNSIGNED value is out of range in '(-9223372036854775808, 9223372036854775807)'"
    );
    assert_eq!(div_int_with_uint(0, 1).unwrap(), 0);
    assert_eq!(div_int_with_uint(-1, i64::MAX as u64).unwrap(), 0);
}
