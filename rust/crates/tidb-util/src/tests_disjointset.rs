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

//! Ports of `pkg/util/disjointset` unit tests from Go (`int_set_test.go`,
//! `set_test.go`).

use crate::disjointset::{Set, SimpleIntSet};

/// Go: pkg/util/disjointset/int_set_test.go TestIntDisjointSet
#[test]
fn int_disjoint_set_unions_and_roots() {
    let mut set = SimpleIntSet::new(10);
    // Go asserts parent has length 10 and parent[i] == i initially; the
    // equivalent public-API observation is that every element is its own root.
    for i in 0..10 {
        assert_eq!(set.find_root(i), i);
    }
    set.union(0, 1);
    set.union(1, 3);
    set.union(4, 2);
    set.union(2, 6);
    set.union(3, 5);
    set.union(7, 8);
    set.union(9, 6);
    let root_1 = set.find_root(1);
    assert_eq!(set.find_root(0), root_1);
    assert_eq!(set.find_root(3), root_1);
    assert_eq!(set.find_root(5), root_1);
    let root_4 = set.find_root(4);
    let root_2 = set.find_root(2);
    assert_eq!(root_2, root_4);
    assert_eq!(set.find_root(6), root_4);
    assert_eq!(set.find_root(9), root_2);
    assert_eq!(set.find_root(7), set.find_root(8));
}

/// Go: pkg/util/disjointset/set_test.go TestDisjointSet
#[test]
fn disjoint_set_sparse_string_elements() {
    let mut set = Set::new(10);
    assert!(!set.in_same_group("a", "b"));
    // First InSameGroup call inserts "a" and "b" as singletons.
    assert_eq!(set.len(), 2);
    set.union("a", "b");
    assert!(set.in_same_group("a", "b"));
    assert!(!set.in_same_group("a", "c"));
    assert_eq!(set.len(), 3);
    assert!(!set.in_same_group("b", "c"));
    assert_eq!(set.len(), 3);
    set.union("b", "c");
    assert!(set.in_same_group("a", "c"));
    assert!(set.in_same_group("b", "c"));
    set.union("d", "e");
    set.union("e", "f");
    set.union("f", "g");
    assert_eq!(set.len(), 7);
    assert!(!set.in_same_group("a", "d"));
    assert!(set.in_same_group("d", "g"));
    assert!(!set.in_same_group("c", "g"));
    set.union("a", "g");
    assert!(set.in_same_group("a", "d"));
    assert!(set.in_same_group("b", "g"));
    assert!(set.in_same_group("c", "f"));
    assert!(set.in_same_group("a", "e"));
    assert!(set.in_same_group("b", "c"));
}
