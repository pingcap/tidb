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

//! Public package contract for Go `pkg/util/intset`.

use std::any::Any;
use std::panic::{catch_unwind, AssertUnwindSafe};

use tidb_util::intset::{FastIntSet, MAX_INT, MIN_INT};

fn panic_text(payload: &(dyn Any + Send)) -> Option<&str> {
    payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
}

#[test]
fn public_representation_and_sentinel_contracts_match_go() {
    let mut set = FastIntSet::default();
    assert!(set.is_empty());
    assert!(!set.only1_zero());

    set.insert(0);
    assert!(set.only1_zero());
    set.insert(63);
    assert_eq!(set.len(), 2);
    assert_eq!(set.get_small_uint64().unwrap(), 1 | (1 << 63));

    set.insert(-2);
    set.insert(64);
    set.insert(MAX_INT);
    assert_eq!(set.sorted_array(), [-2, 0, 63, 64, MAX_INT]);
    assert_eq!(set.next(MIN_INT), (0, true));
    assert_eq!(set.next(64), (64, true));
    assert_eq!(set.next(MAX_INT), (MAX_INT, false));
    assert_eq!(set.iter().collect::<Vec<_>>(), [-2, 0, 63, 64]);

    let mut visited = Vec::new();
    set.for_each(|value| visited.push(value));
    assert_eq!(visited, [-2, 0, 63, 64]);
    assert_eq!(set.to_string(), "(-2,0,63,64)");
    assert_eq!(
        set.get_small_uint64().unwrap_err(),
        "set contains large values, cannot get small uint64"
    );

    set.clear();
    assert!(set.is_empty());
    assert!(set.get_small_uint64().is_err());

    let target = FastIntSet::new(&[1]);
    set.copy_from(&target);
    assert_eq!(set.len(), 0);
    assert!(set.has(1));
    assert!(set.sorted_array().is_empty());
    assert!(set.equals(&target));
    assert!(set.get_small_uint64().is_err());
}

#[test]
fn public_copy_and_set_algebra_match_go() {
    let original = FastIntSet::of([1, 2, 64, 65]);
    let mut copied = original.copy();
    copied.remove(64);
    assert!(original.has(64));
    assert!(!copied.has(64));

    let small = FastIntSet::new(&[1, 2]);
    let mut large = FastIntSet::new(&[1, 2, 64]);
    large.remove(64);
    assert_eq!(small, large);
    assert!(small.equals(&large));
    assert!(small.subset_of(&large));
    assert!(large.subset_of(&small));
    assert!(large.intersects(&FastIntSet::new(&[2])));
    assert!(!small.intersects(&FastIntSet::new(&[3])));

    let rhs = FastIntSet::new(&[2, 65, 66]);
    assert_eq!(original.union(&rhs).sorted_array(), [1, 2, 64, 65, 66]);
    assert_eq!(original.intersection(&rhs).sorted_array(), [2, 65]);
    assert_eq!(original.difference(&rhs).sorted_array(), [1, 64]);

    let mut mutated = original.copy();
    mutated.union_with(&rhs);
    assert_eq!(mutated.sorted_array(), [1, 2, 64, 65, 66]);
    mutated.copy_from(&original);
    mutated.intersection_with(&rhs);
    assert_eq!(mutated.sorted_array(), [2, 65]);
    mutated.copy_from(&original);
    mutated.difference_with(&rhs);
    assert_eq!(mutated.sorted_array(), [1, 64]);
}

#[test]
fn public_range_shift_error_and_format_contracts_match_go() {
    let empty = FastIntSet::default();
    for delta in [MIN_INT, -65, -1, 0, 1, 65, MAX_INT] {
        assert!(empty.shift(delta).is_empty(), "delta {delta}");
    }
    assert_eq!(
        FastIntSet::new(&[1]).shift(MAX_INT).sorted_array(),
        [MIN_INT]
    );
    assert_eq!(FastIntSet::new(&[1]).shift(MIN_INT).sorted_array(), [1]);
    assert_eq!(
        FastIntSet::new(&[MAX_INT - 1]).shift(2).sorted_array(),
        [MIN_INT]
    );

    let mut full_small = FastIntSet::default();
    full_small.add_range(0, 63);
    assert_eq!(full_small.get_small_uint64().unwrap(), u64::MAX);

    let mut mixed = FastIntSet::default();
    mixed.add_range(-2, 2);
    for value in [7, 8, 10, 11, 12] {
        mixed.insert(value);
    }
    assert_eq!(mixed.to_string(), "(-2,-1,0-2,7,8,10-12)");

    let panic = catch_unwind(AssertUnwindSafe(|| {
        FastIntSet::default().add_range(1, 0);
    }))
    .expect_err("invalid range must panic");
    assert_eq!(
        panic_text(panic.as_ref()),
        Some("invalid range when adding range to FastIntSet")
    );
}
