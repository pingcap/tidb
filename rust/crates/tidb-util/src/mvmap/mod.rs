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

//! Complete transcreation of Go `pkg/util/mvmap` (`mvmap.go`, `fnv.go`).
//!
//! [`MVMap`] stores multiple values for a given key with minimum GC overhead.
//! Keys and values live packed in growable byte arenas ([`DataStore`]), and the
//! per-entry metadata lives in a parallel entry arena ([`EntryStore`]); a hash
//! table maps `fnv_hash64(key)` to the head of a singly linked list of entries
//! chained through [`EntryAddr`]. Both hash collisions and repeated puts of the
//! same key share one chain — [`DataStore::get`] filters by comparing the stored
//! key bytes.
//!
//! The arena/address layout is preserved exactly, since [`Iter`] walks the entry
//! arena in insertion order and [`MVMap::get`] depends on the chain plus the
//! byte-equality filter. Go's returned `[][]byte` alias the internal arenas;
//! the Rust equivalent returns `&[u8]` borrowed from `&self`.

mod fnv;

use fnv::fnv_hash64;
use std::collections::HashMap;

const MAX_DATA_SLICE_LEN: u32 = 64 * 1024;
const MAX_ENTRY_SLICE_LEN: u32 = 8 * 1024;

#[derive(Clone, Copy, Default, PartialEq, Eq)]
struct EntryAddr {
    slice_idx: u32,
    offset: u32,
}

#[derive(Clone, Copy, Default)]
struct DataAddr {
    slice_idx: u32,
    offset: u32,
}

#[derive(Clone, Copy, Default)]
struct Entry {
    addr: DataAddr,
    key_len: u32,
    val_len: u32,
    next: EntryAddr,
}

/// A growable byte arena holding packed `key||value` records.
#[derive(Default)]
struct DataStore {
    slices: Vec<Vec<u8>>,
    slice_idx: u32,
    slice_len: u32,
}

impl DataStore {
    fn put(&mut self, key: &[u8], value: &[u8]) -> DataAddr {
        let data_len = (key.len() + value.len()) as u32;
        if self.slice_len != 0 && self.slice_len + data_len > MAX_DATA_SLICE_LEN {
            self.slices.push(Vec::with_capacity(
                (MAX_DATA_SLICE_LEN as usize).max(data_len as usize),
            ));
            self.slice_len = 0;
            self.slice_idx += 1;
        }
        let addr = DataAddr {
            slice_idx: self.slice_idx,
            offset: self.slice_len,
        };
        let slice = &mut self.slices[self.slice_idx as usize];
        slice.extend_from_slice(key);
        slice.extend_from_slice(value);
        self.slice_len += data_len;
        addr
    }

    fn get(&self, e: &Entry, key: &[u8]) -> Option<&[u8]> {
        let slice = &self.slices[e.addr.slice_idx as usize];
        let key_offset = e.addr.offset as usize;
        let val_offset = key_offset + e.key_len as usize;
        if key != &slice[key_offset..val_offset] {
            return None;
        }
        Some(&slice[val_offset..val_offset + e.val_len as usize])
    }

    fn get_entry_data(&self, e: &Entry) -> (&[u8], &[u8]) {
        let slice = &self.slices[e.addr.slice_idx as usize];
        let key_offset = e.addr.offset as usize;
        let key_end = key_offset + e.key_len as usize;
        let val_end = key_end + e.val_len as usize;
        (&slice[key_offset..key_end], &slice[key_end..val_end])
    }
}

/// A growable arena holding [`Entry`] records.
#[derive(Default)]
struct EntryStore {
    slices: Vec<Vec<Entry>>,
    slice_idx: u32,
    slice_len: u32,
}

impl EntryStore {
    fn put(&mut self, e: Entry) -> EntryAddr {
        if self.slice_len == MAX_ENTRY_SLICE_LEN {
            self.slices
                .push(Vec::with_capacity(MAX_ENTRY_SLICE_LEN as usize));
            self.slice_len = 0;
            self.slice_idx += 1;
        }
        let addr = EntryAddr {
            slice_idx: self.slice_idx,
            offset: self.slice_len,
        };
        let slice = &mut self.slices[self.slice_idx as usize];
        slice.push(e);
        self.slice_len += 1;
        addr
    }

    fn get(&self, addr: EntryAddr) -> Entry {
        self.slices[addr.slice_idx as usize][addr.offset as usize]
    }
}

/// Stores multiple values for a given key with minimum GC overhead.
///
/// A given key can store multiple values. It is not thread-safe and should only
/// be used from a single thread.
pub struct MVMap {
    hash_table: HashMap<u64, EntryAddr>,
    entry_store: EntryStore,
    data_store: DataStore,
    length: usize,
}

// A derived `Default` would leave the arenas empty and skip the reserved first
// entry, producing an `MVMap` that panics on the first `put`. `Default` must be
// the same valid, initialized map as `new`.
impl Default for MVMap {
    fn default() -> Self {
        Self::new()
    }
}

impl MVMap {
    /// Creates a new multi-value map.
    #[must_use]
    pub fn new() -> Self {
        let mut m = MVMap {
            hash_table: HashMap::new(),
            entry_store: EntryStore {
                slices: vec![Vec::with_capacity(64)],
                slice_idx: 0,
                slice_len: 0,
            },
            data_store: DataStore {
                slices: vec![Vec::with_capacity(1024)],
                slice_idx: 0,
                slice_len: 0,
            },
            length: 0,
        };
        // Append the first empty entry, so the zero EntryAddr can represent null.
        m.entry_store.put(Entry::default());
        m
    }

    /// Puts the key/value pair into the map. If the key already exists, the old
    /// value is not overwritten; values are stored in a list.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let hash_key = fnv_hash64(key);
        let old_entry_addr = self.hash_table.get(&hash_key).copied().unwrap_or_default();
        let data_addr = self.data_store.put(key, value);
        let e = Entry {
            addr: data_addr,
            key_len: key.len() as u32,
            val_len: value.len() as u32,
            next: old_entry_addr,
        };
        let new_entry_addr = self.entry_store.put(e);
        self.hash_table.insert(hash_key, new_entry_addr);
        self.length += 1;
    }

    /// Gets the values of `key` and appends them to `values`, returning the
    /// grown vector. The returned slices borrow the map's internal arenas.
    #[must_use]
    pub fn get<'a>(&'a self, key: &[u8], mut values: Vec<&'a [u8]>) -> Vec<&'a [u8]> {
        let hash_key = fnv_hash64(key);
        let mut entry_addr = self.hash_table.get(&hash_key).copied().unwrap_or_default();
        while entry_addr != EntryAddr::default() {
            let e = self.entry_store.get(entry_addr);
            entry_addr = e.next;
            if let Some(val) = self.data_store.get(&e, key) {
                values.push(val);
            }
        }
        // Keep the order of input: the chain is walked newest-first.
        values.reverse();
        values
    }

    /// Returns the number of values in the map. The number of keys may be less
    /// than this if the same key is put more than once.
    #[must_use]
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.length
    }

    /// Creates an iterator over the map's key/value pairs in insertion order.
    #[must_use]
    pub fn new_iterator(&self) -> Iter<'_> {
        // The first entry is empty, so init entry_cur to 1.
        Iter {
            m: self,
            slice_cur: 0,
            entry_cur: 1,
        }
    }
}

/// Iterates over an [`MVMap`] (Go's `Iterator`).
pub struct Iter<'a> {
    m: &'a MVMap,
    slice_cur: usize,
    entry_cur: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    /// Returns the next key/value pair, or `None` when exhausted.
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.slice_cur >= self.m.entry_store.slices.len() {
                return None;
            }
            let entry_slice = &self.m.entry_store.slices[self.slice_cur];
            if self.entry_cur >= entry_slice.len() {
                self.slice_cur += 1;
                self.entry_cur = 0;
                continue;
            }
            let entry = entry_slice[self.entry_cur];
            self.entry_cur += 1;
            return Some(self.m.data_store.get_entry_data(&entry));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::MVMap;

    fn as_str(b: &[u8]) -> &str {
        std::str::from_utf8(b).unwrap()
    }

    // Go `TestMVMap`.
    #[test]
    fn mvmap() {
        let mut m = MVMap::new();
        m.put(b"abc", b"abc1");
        m.put(b"abc", b"abc2");
        m.put(b"def", b"def1");
        m.put(b"def", b"def2");

        let v = m.get(b"abc", Vec::new());
        assert_eq!(
            v.iter().map(|b| as_str(b)).collect::<Vec<_>>(),
            vec!["abc1", "abc2"]
        );
        let v = m.get(b"def", Vec::new());
        assert_eq!(
            v.iter().map(|b| as_str(b)).collect::<Vec<_>>(),
            vec!["def1", "def2"]
        );
        assert_eq!(m.len(), 4);

        let results = [
            ("abc", "abc1"),
            ("abc", "abc2"),
            ("def", "def1"),
            ("def", "def2"),
        ];
        let mut it = m.new_iterator();
        for expected in results {
            let (key, val) = it.next().unwrap();
            assert_eq!((as_str(key), as_str(val)), expected);
        }
        assert_eq!(it.next(), None);
    }

    // Go `BenchmarkMVMapPut` body, run as a functional test. The larger item
    // count also exercises the entry- and data-arena slice rollover paths.
    #[test]
    fn bench_mvmap_put() {
        const N: u64 = 10000;
        let mut m = MVMap::new();
        for i in 0..N {
            let buffer = i.to_be_bytes();
            m.put(&buffer, &buffer);
        }
        assert_eq!(m.len(), N as usize);
    }

    // Go `BenchmarkMVMapGet` body, run as a functional test; it preserves the
    // benchmark's inner correctness check (each distinct key maps to exactly one
    // value equal to the key).
    #[test]
    fn bench_mvmap_get() {
        const N: u64 = 10000;
        let mut m = MVMap::new();
        for i in 0..N {
            let buffer = i.to_be_bytes();
            m.put(&buffer, &buffer);
        }
        for i in 0..N {
            let buffer = i.to_be_bytes();
            let val = m.get(&buffer, Vec::new());
            assert!(val.len() == 1 && val[0] == &buffer[..]);
        }
    }
}
