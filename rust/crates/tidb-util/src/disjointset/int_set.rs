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

//! Dense integer disjoint set from `pkg/util/disjointset/int_set.go`.

/// Disjoint set for continuous, non-negative integer elements.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SimpleIntSet {
    parent: Vec<usize>,
}

impl SimpleIntSet {
    /// Creates a set containing the integers in `0..size`.
    ///
    /// Like Go's `make([]int, size)`, a negative size is invalid.
    #[must_use]
    pub fn new(size: isize) -> Self {
        assert!(size >= 0, "disjoint set size must be non-negative");
        Self {
            parent: (0..size as usize).collect(),
        }
    }

    /// Joins the sets containing `a` and `b`, using `b`'s root as successor.
    pub fn union(&mut self, a: usize, b: usize) {
        let root_a = self.find_root(a);
        let root_b = self.find_root(b);
        self.parent[root_a] = root_b;
    }

    /// Finds the representative element and compresses the traversed path.
    pub fn find_root(&mut self, element: usize) -> usize {
        let parent = self.parent[element];
        if element == parent {
            return element;
        }
        let root = self.find_root(parent);
        self.parent[element] = root;
        root
    }

    /// Removes every element while retaining the allocation for reuse.
    pub fn clear(&mut self) {
        self.parent.clear();
    }

    /// Resets the set to contain the integers in `0..size`.
    pub fn grow_new_int_set(&mut self, size: isize) {
        assert!(size >= 0, "disjoint set size must be non-negative");
        let size = size as usize;
        self.parent.clear();
        self.parent.reserve(size);
        self.parent.extend(0..size);
    }

    /// Returns the number of elements in this dense set.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.parent.len()
    }

    /// Returns whether this dense set contains no elements.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.parent.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::SimpleIntSet;

    #[test]
    #[allow(non_snake_case)]
    fn TestIntDisjointSet() {
        let mut set = SimpleIntSet::new(10);
        assert_eq!(set.parent.len(), 10);
        for (index, parent) in set.parent.iter().copied().enumerate() {
            assert_eq!(index, parent);
        }
        set.union(0, 1);
        set.union(1, 3);
        set.union(4, 2);
        set.union(2, 6);
        set.union(3, 5);
        set.union(7, 8);
        set.union(9, 6);
        let root_0 = set.find_root(0);
        let root_1 = set.find_root(1);
        let root_3 = set.find_root(3);
        let root_5 = set.find_root(5);
        let root_2 = set.find_root(2);
        let root_4 = set.find_root(4);
        let root_6 = set.find_root(6);
        let root_9 = set.find_root(9);
        let root_7 = set.find_root(7);
        let root_8 = set.find_root(8);
        assert_eq!(root_0, root_1);
        assert_eq!(root_3, root_1);
        assert_eq!(root_5, root_1);
        assert_eq!(root_2, root_4);
        assert_eq!(root_6, root_4);
        assert_eq!(root_9, root_2);
        assert_eq!(root_7, root_8);
    }

    #[test]
    fn clear_and_grow_replace_the_complete_dense_domain() {
        let mut set = SimpleIntSet::new(3);
        set.union(0, 2);
        set.clear();
        assert!(set.is_empty());
        set.grow_new_int_set(5);
        assert_eq!(set.parent, [0, 1, 2, 3, 4]);
    }

    #[test]
    #[should_panic(expected = "disjoint set size must be non-negative")]
    fn negative_size_matches_go_make_panic_boundary() {
        let _ = SimpleIntSet::new(-1);
    }
}
