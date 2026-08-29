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

//! Sparse generic disjoint set.

use std::collections::HashMap;
use std::hash::Hash;

/// Disjoint set for sparse or non-integer element domains.
pub struct Set<T> {
    parent: Vec<isize>,
    value_to_index: HashMap<T, isize>,
    values: Vec<T>,
}

impl<T> Set<T>
where
    T: Clone + Eq + Hash,
{
    /// Creates an empty sparse disjoint set with the requested capacity.
    pub fn new(capacity: isize) -> Self {
        let capacity = usize::try_from(capacity).expect("negative disjoint set size");
        Self {
            parent: Vec::with_capacity(capacity),
            value_to_index: HashMap::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
        }
    }

    fn find_root_original_value(&mut self, value: T) -> isize {
        if let Some(index) = self.value_to_index.get(&value).copied() {
            return self.find_root_internal(index);
        }

        let index = self.parent.len() as isize;
        self.parent.push(index);
        self.value_to_index.insert(value.clone(), index);
        self.values.push(value);
        index
    }

    fn find_root_internal(&mut self, index: isize) -> isize {
        let position = usize::try_from(index).expect("negative disjoint set index");
        let parent = self.parent[position];
        if parent == index {
            return index;
        }
        let root = self.find_root_internal(parent);
        self.parent[position] = root;
        root
    }

    /// Returns whether two values belong to the same group.
    ///
    /// Missing values are inserted as singleton groups, matching Go.
    pub fn in_same_group(&mut self, a: T, b: T) -> bool {
        self.find_root_original_value(a) == self.find_root_original_value(b)
    }

    /// Joins the two groups, preserving `a`'s root as the new root.
    pub fn union(&mut self, a: T, b: T) {
        let root_a = self.find_root_original_value(a);
        let root_b = self.find_root_original_value(b);
        if root_a != root_b {
            self.parent[usize::try_from(root_b).expect("negative disjoint set index")] = root_a;
        }
    }

    /// Finds the integer root for a value, inserting a missing singleton.
    pub fn find_root(&mut self, value: T) -> isize {
        self.find_root_original_value(value)
    }

    /// Finds the original value associated with an index's current root.
    ///
    /// Panics when `index` is outside the inserted domain.
    pub fn find_val(&mut self, index: isize) -> Option<T> {
        let root = self.find_root_internal(index);
        self.values
            .get(usize::try_from(root).expect("negative disjoint set index"))
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::Set;

    /// Go `pkg/util/disjointset/set_test.go` `TestDisjointSet`.
    #[test]
    fn test_disjoint_set() {
        let mut set = Set::new(10);
        assert!(!set.in_same_group("a", "b"));
        assert_eq!(set.parent.len(), 2);
        set.union("a", "b");
        assert!(set.in_same_group("a", "b"));
        assert!(!set.in_same_group("a", "c"));
        assert_eq!(set.parent.len(), 3);
        assert!(!set.in_same_group("b", "c"));
        assert_eq!(set.parent.len(), 3);
        set.union("b", "c");
        assert!(set.in_same_group("a", "c"));
        assert!(set.in_same_group("b", "c"));
        set.union("d", "e");
        set.union("e", "f");
        set.union("f", "g");
        assert_eq!(set.parent.len(), 7);
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
}
