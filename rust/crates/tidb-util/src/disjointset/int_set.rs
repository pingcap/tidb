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

//! Dense integer disjoint set.

/// Disjoint set for continuous, non-negative integer elements.
pub struct SimpleIntSet {
    parent: Vec<isize>,
}

impl SimpleIntSet {
    /// Creates a set containing the integers in `0..size`.
    pub fn new(size: isize) -> Self {
        let size = usize::try_from(size).expect("negative disjoint set size");
        Self {
            parent: (0..size).map(|value| value as isize).collect(),
        }
    }

    /// Joins the sets containing `a` and `b`, using `b`'s root as successor.
    pub fn union(&mut self, a: isize, b: isize) {
        let root_a = self.find_root(a);
        let root_b = self.find_root(b);
        self.parent[usize::try_from(root_a).expect("negative disjoint set index")] = root_b;
    }

    /// Finds the representative element and compresses the traversed path.
    pub fn find_root(&mut self, element: isize) -> isize {
        let index = usize::try_from(element).expect("negative disjoint set index");
        let parent = self.parent[index];
        if element == parent {
            return element;
        }
        let root = self.find_root(parent);
        self.parent[index] = root;
        root
    }

    /// Removes every element while retaining the allocation for reuse.
    pub fn clear(&mut self) {
        self.parent.clear();
    }

    /// Resets the set to contain the integers in `0..size`.
    pub fn grow_new_int_set(&mut self, size: isize) {
        let size = usize::try_from(size).expect("negative disjoint set size");
        self.parent.clear();
        self.parent.reserve(size);
        self.parent.extend((0..size).map(|value| value as isize));
    }
}

#[cfg(test)]
mod tests {
    use super::SimpleIntSet;
    use crate::disjointset::Set;
    use std::panic::{catch_unwind, AssertUnwindSafe};

    /// Go `pkg/util/disjointset/int_set_test.go` `TestIntDisjointSet`.
    #[test]
    fn test_int_disjoint_set() {
        let mut set = SimpleIntSet::new(10);
        assert_eq!(set.parent.len(), 10);
        for (index, parent) in set.parent.iter().copied().enumerate() {
            assert_eq!(index as isize, parent);
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
    fn signed_sizes_and_indexes_match_go() {
        assert!(catch_unwind(|| SimpleIntSet::new(-1)).is_err());
        assert!(catch_unwind(|| Set::<i32>::new(-1)).is_err());

        let mut dense = SimpleIntSet::new(1);
        assert!(catch_unwind(AssertUnwindSafe(|| dense.find_root(-1))).is_err());

        let mut sparse = Set::new(1);
        sparse.find_root(1);
        assert!(catch_unwind(AssertUnwindSafe(|| sparse.find_val(-1))).is_err());
    }
}
