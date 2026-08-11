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

//! Public contract for Go `pkg/util/disjointset`.

use std::panic::{catch_unwind, AssertUnwindSafe};

use tidb_util::disjointset::{Set, SimpleIntSet};

#[test]
fn dense_roots_union_direction_and_reset_match_go() {
    let mut set = SimpleIntSet::new(4);
    set.union(0, 1);
    set.union(2, 1);
    assert_eq!(
        (0..4).map(|index| set.find_root(index)).collect::<Vec<_>>(),
        [1, 1, 1, 3]
    );

    set.clear();
    assert!(set.is_empty());
    assert!(catch_unwind(AssertUnwindSafe(|| set.find_root(0))).is_err());

    set.grow_new_int_set(3);
    assert_eq!(set.len(), 3);
    assert_eq!(
        (0..3).map(|index| set.find_root(index)).collect::<Vec<_>>(),
        [0, 1, 2]
    );
}

#[test]
fn sparse_indices_root_values_and_invalid_index_match_go() {
    let mut set = Set::new(0);
    let b = set.find_root("b");
    let a = set.find_root("a");
    assert_eq!((b, a, set.len()), (0, 1, 2));
    assert!(!set.in_same_group("b", "a"));

    set.union("b", "a");
    assert_eq!(set.find_root("a"), 0);
    assert_eq!(set.find_value(a), Some("b"));

    set.union("c", "b");
    assert_eq!(set.find_root("a"), 2);
    assert_eq!(set.find_value(a), Some("c"));

    let len = set.len();
    set.union("a", "a");
    assert_eq!(set.find_root("a"), 2);
    assert_eq!(set.len(), len);
    assert!(catch_unwind(AssertUnwindSafe(|| set.find_value(99))).is_err());
}
