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
use std::hash::{BuildHasher, Hash, Hasher};

use foldhash::fast::{FoldHasher, RandomState};
use hashbrown::{HashTable, hash_table::Entry};

use crate::int_set::find_root;

enum ValueIndex<T> {
    Samples {
        table: HashTable<(u64, isize)>,
        hasher: RandomState,
    },
    Map(HashMap<T, isize, RandomState>),
}

/// Samples long byte writes so that comparing the candidate key is cheaper
/// than first hashing the whole key. The fallback map hashes entire keys.
struct KeySample(FoldHasher);

impl Hasher for KeySample {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        if bytes.len() > 64 {
            let first = u64::from_ne_bytes(*bytes.first_chunk().unwrap());
            let last = u64::from_ne_bytes(*bytes.last_chunk().unwrap());
            self.0.write_u64(first ^ last.rotate_left(25));
            self.0.write_usize(bytes.len());
        } else {
            self.0.write(bytes);
        }
    }

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.0.write_u8(value);
    }

    #[inline]
    fn write_isize(&mut self, value: isize) {
        self.0.write_isize(value);
    }

    #[inline]
    fn write_usize(&mut self, value: usize) {
        self.0.write_usize(value);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.0.finish()
    }
}

/// Disjoint set for sparse or non-integer element domains.
pub struct Set<T> {
    parent: Vec<isize>,
    value_to_index: ValueIndex<T>,
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
            value_to_index: ValueIndex::Samples {
                table: HashTable::with_capacity(capacity),
                hasher: RandomState::default(),
            },
            values: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    fn find_root_original_value(&mut self, value: T) -> isize {
        let index = self.parent.len() as isize;
        match &mut self.value_to_index {
            ValueIndex::Samples { table, hasher } => {
                if matches!(self.values.as_slice(), [only] if *only == value) {
                    return 0;
                }
                let mut sample = KeySample(hasher.build_hasher());
                value.hash(&mut sample);
                let sample = sample.finish();
                match table.entry(sample, |&(stored, _)| stored == sample, |&(stored, _)| stored) {
                    Entry::Vacant(entry) => {
                        entry.insert((sample, index));
                    }
                    Entry::Occupied(entry) => {
                        let &(_, existing) = entry.get();
                        if self.values[existing as usize] == value {
                            return self.find_root_internal(existing);
                        }
                        let hasher = *hasher;
                        self.insert_with_full_hashes(&value, hasher, index);
                    }
                }
            }
            ValueIndex::Map(map) => {
                if let Some(existing) = map.get(&value).copied() {
                    return self.find_root_internal(existing);
                }
                map.insert(value.clone(), index);
            }
        }

        self.parent.push(index);
        self.values.push(value);
        index
    }

    /// Promote after the first sample collision so shared prefixes and
    /// suffixes cannot produce an unbounded chain of full-key comparisons.
    #[cold]
    fn insert_with_full_hashes(&mut self, value: &T, hasher: RandomState, index: isize) {
        let capacity = self.values.capacity().max(self.values.len() + 1);
        let mut map = HashMap::with_capacity_and_hasher(capacity, hasher);
        map.extend(self.values.iter().cloned().zip(0..));
        map.insert(value.clone(), index);
        self.value_to_index = ValueIndex::Map(map);
    }

    fn find_root_internal(&mut self, index: isize) -> isize {
        find_root(&mut self.parent, index)
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

    #[test]
    fn sample_collisions_preserve_values_and_representatives() {
        let mut set = Set::new(16);
        let prefix = "shared prefix".repeat(8);
        let suffix = "shared suffix".repeat(8);
        let first = format!("{prefix}1{suffix}");
        let first_copy = first.clone();
        let second = format!("{prefix}2{suffix}");
        let third = format!("{prefix}3{suffix}");

        assert_eq!(set.find_root(first.as_str()), 0);
        assert_eq!(set.find_root(first_copy.as_str()), 0);
        assert_eq!(set.find_root(second.as_str()), 1);
        assert!(matches!(set.value_to_index, super::ValueIndex::Map(_)));
        set.union(second.as_str(), first.as_str());
        assert_eq!(set.find_root(first.as_str()), 1);
        assert_eq!(set.find_val(0), Some(second.as_str()));
        assert_eq!(set.find_root(third.as_str()), 2);
        assert!(!set.in_same_group(second.as_str(), third.as_str()));
    }

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
