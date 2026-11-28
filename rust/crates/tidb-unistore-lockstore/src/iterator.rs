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

use crate::arena::{ArenaAddr, NULL_ARENA_ADDR};
use crate::{MemStore, ReadView};

/// Go `Iterator`: iterates the entries in the [`MemStore`].
///
/// The current key and value are copied out of the arena as the iterator
/// moves (Go hands out slices of arena memory a later reuse may overwrite).
pub struct Iterator<'a> {
    ls: &'a MemStore,
    view: ReadView<'a>,
    current: ArenaAddr,
    key: Vec<u8>,
    val: Vec<u8>,
}

impl std::fmt::Debug for Iterator<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Iterator")
            .field("current", &self.current)
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl MemStore {
    /// Go `NewIterator`: a new [`Iterator`] for the lock store.
    #[must_use]
    pub fn new_iterator(&self) -> Iterator<'_> {
        Iterator {
            ls: self,
            view: self.read_view(),
            current: NULL_ARENA_ADDR,
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
        let e = if self.current.is_null() {
            // Go searches after the empty key when there is no current node.
            self.ls.find_greater(&mut self.view, b"", false).0
        } else {
            self.ls.get_next(&mut self.view, self.current, 0)
        };
        self.set_key_value(e);
    }

    /// Go `Prev`: moves to the previous entry.
    pub fn prev(&mut self) {
        // find <. No equality allowed.
        let key = std::mem::take(&mut self.key);
        let (e, _) = self.ls.find_less(&mut self.view, &key, false);
        self.key = key;
        self.set_key_value(e);
    }

    /// Go `Seek`: locates the first entry with a key `>= seek_key`.
    pub fn seek(&mut self, seek_key: &[u8]) {
        let (e, _) = self.ls.find_greater(&mut self.view, seek_key, true); // find >=.
        self.set_key_value(e);
    }

    /// Go `SeekForPrev`: locates the last entry with a key `<= target`.
    pub fn seek_for_prev(&mut self, target: &[u8]) {
        let (e, _) = self.ls.find_less(&mut self.view, target, true); // find <=.
        self.set_key_value(e);
    }

    /// Go `SeekForExclusivePrev`: locates the last entry with a key
    /// `< target`.
    pub fn seek_for_exclusive_prev(&mut self, target: &[u8]) {
        let (e, _) = self.ls.find_less(&mut self.view, target, false);
        self.set_key_value(e);
    }

    /// Go `SeekToFirst`: locates the first entry.
    pub fn seek_to_first(&mut self) {
        let head = self.ls.head;
        let e = self.ls.get_next(&mut self.view, head, 0);
        self.set_key_value(e);
    }

    /// Go `SeekToLast`: locates the last entry.
    pub fn seek_to_last(&mut self) {
        let e = self.ls.find_last(&mut self.view);
        self.set_key_value(e);
    }

    /// Go `setKeyValue`.
    fn set_key_value(&mut self, e: ArenaAddr) {
        self.current = e;
        if e.is_null() {
            self.key.clear();
            self.val.clear();
        } else {
            let nodes = self.view.nodes(e);
            nodes.key(e, &mut self.key);
            nodes.value(e, &mut self.val);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Hint, MemStore};
    use crate::testutil::num_to_key;

    /// Builds the list Go's `TestIterator` builds: keys `ls…10` through
    /// `ls…990` in steps of 10, each value the key repeated ten times.
    fn build(seed: u64) -> MemStore {
        let ls = MemStore::with_seed(1 << 10, seed);
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
        assert_eq!(ls.block_count(), 33);
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

    #[test]
    fn next_preserves_empty_key_and_end_behavior() {
        let ls = MemStore::with_seed(256, 42);
        ls.put(b"", b"first");
        ls.put(b"a", b"second");
        ls.put(b"b", b"");

        let mut it = ls.new_iterator();
        it.next();
        assert_eq!((it.key(), it.value()), (&b"a"[..], &b"second"[..]));
        it.seek_to_first();
        assert!(!it.valid());
        assert_eq!((it.key(), it.value()), (&b""[..], &b"first"[..]));
        it.next();
        assert_eq!(it.key(), b"a");
        it.next();
        assert!(it.valid());
        assert_eq!((it.key(), it.value()), (&b"b"[..], &b""[..]));
        it.next();
        assert!(!it.valid());
        assert_eq!((it.key(), it.value()), (&b""[..], &b""[..]));
        it.next();
        assert_eq!(it.key(), b"a");
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
