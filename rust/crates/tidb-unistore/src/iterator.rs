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

//! Go `iterator.go`: a cursor over the entries of a [`MemStore`].

use crate::arena::ArenaAddr;
use crate::lockstore::MemStore;

/// Go `Iterator`: iterates the entries in the [`MemStore`].
///
/// Like Go's, it keeps its own copy of the current key and value rather than
/// pointing into the arena, so a write between two positioning calls cannot
/// invalidate it. Rust adds the borrow of the store, which is what makes the
/// single-writer discipline checkable: no write can happen while an iterator
/// is alive.
#[derive(Debug)]
pub struct Iterator<'a> {
    ls: &'a MemStore,
    key: Vec<u8>,
    val: Vec<u8>,
}

impl MemStore {
    /// Go `NewIterator`: a new [`Iterator`] for the lock store.
    #[must_use]
    pub fn new_iterator(&self) -> Iterator<'_> {
        Iterator {
            ls: self,
            key: Vec::new(),
            val: Vec::new(),
        }
    }
}

impl Iterator<'_> {
    /// Go `Valid`: whether the iterator is positioned at a valid node.
    #[must_use]
    pub fn valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Go `Key`: the key at the current position.
    #[must_use]
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Go `Value`: the value at the current position.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        &self.val
    }

    /// Go `Next`: moves to the next entry.
    pub fn next(&mut self) {
        let (e, _) = self.ls.find_greater(&self.key, false);
        self.set_key_value(e);
    }

    /// Go `Prev`: moves to the previous entry.
    pub fn prev(&mut self) {
        // find <. No equality allowed.
        let (e, _) = self.ls.find_less(&self.key, false);
        self.set_key_value(e);
    }

    /// Go `Seek`: locates the first entry with a key `>= seek_key`.
    pub fn seek(&mut self, seek_key: &[u8]) {
        let (e, _) = self.ls.find_greater(seek_key, true); // find >=.
        self.set_key_value(e);
    }

    /// Go `SeekForPrev`: locates the last entry with a key `<= target`.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        let (e, _) = self.ls.find_less(target, true); // find <=.
        self.set_key_value(e);
    }

    /// Go `SeekForExclusivePrev`: locates the last entry with a key
    /// `< target`.
    pub fn seek_for_exclusive_prev(&mut self, target: &[u8]) {
        let (e, _) = self.ls.find_less(target, false);
        self.set_key_value(e);
    }

    /// Go `SeekToFirst`: locates the first entry.
    pub fn seek_to_first(&mut self) {
        let e = self.ls.get_next(self.ls.head(), 0);
        self.set_key_value(e);
    }

    /// Go `SeekToLast`: locates the last entry.
    pub fn seek_to_last(&mut self) {
        let e = self.ls.find_last();
        self.set_key_value(e);
    }

    /// Go `setKeyValue`.
    fn set_key_value(&mut self, e: ArenaAddr) {
        let ls = self.ls;
        self.key.clear();
        self.val.clear();
        if e.is_null() {
            return;
        }
        self.key.extend_from_slice(ls.key_of(e));
        self.val.extend_from_slice(ls.value_of(e));
    }
}

#[cfg(test)]
mod tests {
    use crate::lockstore::{Hint, MemStore};
    use crate::testutil::num_to_key;

    /// Builds the list Go's `TestIterator` builds: keys `ls…10` through
    /// `ls…990` in steps of 10, each value the key repeated ten times.
    fn build(seed: u64) -> MemStore {
        let mut ls = MemStore::with_seed(1 << 10, seed);
        let mut hint = Hint::new();
        let mut i = 10;
        while i < 1000 {
            let key = num_to_key(i);
            let val = key.repeat(10);
            ls.put_with_hint(&key, &val, &mut hint);
            i += 10;
        }
        ls
    }

    fn check_key(it: &super::Iterator<'_>, n: usize) {
        assert!(it.valid());
        assert_eq!(it.key(), num_to_key(n).as_slice());
        assert_eq!(it.value(), it.key().repeat(10).as_slice());
    }

    /// Go `TestIterator`, ignored for the same reason Go skips it: its
    /// `require.Len(t, ls.getArena().blocks, 33)` assertion depends on the
    /// random node heights, which is TiDB issue #26235. Every other assertion
    /// in it is covered, seeded and therefore stable, by
    /// [`test_iterator_positioning`] below.
    #[test]
    #[ignore = "Go skips this: unstable arena-block assertion, TiDB #26235"]
    fn test_iterator() {
        let ls = build(1);
        assert_eq!(ls.arena().blocks.len(), 33);
    }

    /// Go `TestIterator` minus the unstable block-count assertion, which is
    /// everything the test actually checks about iteration. The store is
    /// seeded so the shape of the list is fixed run to run.
    #[test]
    fn test_iterator_positioning() {
        let ls = build(0x5EED);
        let mut it = ls.new_iterator();
        it.seek_to_first();
        check_key(&it, 10);
        it.next();
        check_key(&it, 20);
        it.seek_to_first();
        check_key(&it, 10);
        it.seek_to_last();
        check_key(&it, 990);
        it.seek(&num_to_key(11));
        check_key(&it, 20);
        it.seek(&num_to_key(989));
        check_key(&it, 990);
        it.seek(&num_to_key(0));
        check_key(&it, 10);

        it.seek(&num_to_key(2000));
        assert!(!it.valid());
        it.seek(&num_to_key(500));
        check_key(&it, 500);
        it.prev();
        check_key(&it, 490);
        it.seek_for_prev(&num_to_key(100));
        check_key(&it, 100);
        it.seek_for_prev(&num_to_key(99));
        check_key(&it, 90);

        it.seek_for_prev(&num_to_key(2000));
        check_key(&it, 990);
    }

    /// Not a Go test: `SeekForExclusivePrev` has no Go coverage at all, and it
    /// is the only positioning method whose equality handling differs from its
    /// neighbour `SeekForPrev`.
    #[test]
    fn test_seek_for_exclusive_prev() {
        let ls = build(0x5EED);
        let mut it = ls.new_iterator();
        it.seek_for_exclusive_prev(&num_to_key(100));
        check_key(&it, 90);
        it.seek_for_exclusive_prev(&num_to_key(101));
        check_key(&it, 100);
        it.seek_for_exclusive_prev(&num_to_key(10));
        assert!(!it.valid());
    }
}
