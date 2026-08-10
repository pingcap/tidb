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
#[derive(Clone, Debug)]
pub struct Set<T> {
    parent: Vec<usize>,
    value_to_index: HashMap<T, usize>,
    values: Vec<T>,
}

impl<T> Set<T>
where
    T: Clone + Eq + Hash,
{
    /// Creates an empty sparse disjoint set with the requested capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            parent: Vec::with_capacity(capacity),
            value_to_index: HashMap::with_capacity(capacity),
            values: Vec::with_capacity(capacity),
        }
    }

    fn find_root_original_value(&mut self, value: T) -> usize {
        if let Some(index) = self.value_to_index.get(&value).copied() {
            return self.find_root_internal(index);
        }

        let index = self.parent.len();
        self.parent.push(index);
        self.value_to_index.insert(value.clone(), index);
        self.values.push(value);
        index
    }

    fn find_root_internal(&mut self, index: usize) -> usize {
        let parent = self.parent[index];
        if parent == index {
            return index;
        }
        let root = self.find_root_internal(parent);
        self.parent[index] = root;
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
            self.parent[root_b] = root_a;
        }
    }

    /// Finds the integer root for a value, inserting a missing singleton.
    pub fn find_root(&mut self, value: T) -> usize {
        self.find_root_original_value(value)
    }

    /// Finds the original value associated with an index's current root.
    ///
    /// Panics when `index` is outside the inserted domain.
    pub fn find_value(&mut self, index: usize) -> Option<T> {
        let root = self.find_root_internal(index);
        self.values.get(root).cloned()
    }

    /// Returns the number of values inserted into the set.
    #[must_use]
    pub fn len(&self) -> usize {
        self.parent.len()
    }

    /// Returns whether no value has been inserted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.parent.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::Set;

    #[test]
    fn union_and_path_compression() {
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

    #[test]
    fn root_value_and_missing_value_boundaries_match_source() {
        let mut set = Set::new(0);
        let a = set.find_root("a");
        let b = set.find_root("b");
        assert_eq!(set.find_value(a), Some("a"));
        assert_eq!(set.find_value(b), Some("b"));
        set.union("a", "b");
        assert_eq!(set.find_value(b), Some("a"));
    }
}
