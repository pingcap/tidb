// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A fixed-seed multiply-xor hasher for read-mostly in-process tables.
//!
//! Statement execution probes several name-keyed registries per statement --
//! the sysvar registry, the lexer's keyword sets. Those keys are short,
//! lowercase-after-normalization literals whose hash inputs an adversary does
//! not choose (they are engine constants or parser output), so the collision
//! resistance SipHash sells is not needed; what its 4-8 byte-mixing rounds
//! cost per probe IS felt (`std`'s RandomState also reseeds per process for
//! DoS resistance that these static tables cannot benefit from). This is the
//! `rustc-hash`/FxHash construction: one multiply-rotate per word of input.
//! Tables built with [`FxBuildHasher`] must never key on attacker-controlled
//! input.

use std::hash::{BuildHasherDefault, Hasher};

/// The FxHasher rotate-multiply constants (rustc-hash), 64-bit.
const SEED64: u64 = 0x51_7c_c1_b7_27_22_0a_95;

/// One multiply per 8 bytes, two per 4-byte tail: the whole state lives in
/// one register, so a short string hashes without touching memory beyond its
/// own bytes.
#[derive(Clone, Default)]
pub struct FxHasher {
    hash: u64,
}

impl FxHasher {
    #[inline]
    fn add_to_hash(&mut self, word: u64) {
        self.hash = (self.hash.rotate_left(5) ^ word).wrapping_mul(SEED64);
    }
}

impl Hasher for FxHasher {
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let mut chunks = bytes.chunks_exact(8);
        for chunk in &mut chunks {
            self.add_to_hash(u64::from_le_bytes(chunk.try_into().unwrap()));
        }
        let tail = chunks.remainder();
        if !tail.is_empty() {
            let mut last = [0u8; 8];
            last[..tail.len()].copy_from_slice(tail);
            self.add_to_hash(u64::from_le_bytes(last));
        }
    }

    #[inline]
    fn write_u8(&mut self, value: u8) {
        self.add_to_hash(u64::from(value));
    }

    #[inline]
    fn write_u64(&mut self, value: u64) {
        self.add_to_hash(value);
    }

    #[inline]
    fn finish(&self) -> u64 {
        self.hash
    }
}

/// The builder the read-mostly tables use.
pub type FxBuildHasher = BuildHasherDefault<FxHasher>;

/// `HashMap` behind [`FxBuildHasher`].
pub type FxHashMap<K, V> = std::collections::HashMap<K, V, FxBuildHasher>;

/// `HashSet` behind [`FxBuildHasher`].
pub type FxHashSet<K> = std::collections::HashSet<K, FxBuildHasher>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn same_key_hashes_stably_within_a_process() {
        let mut first = FxHasher::default();
        first.write(b"sql_mode");
        let mut second = FxHasher::default();
        second.write(b"sql_mode");
        assert_eq!(first.finish(), second.finish());
    }

    #[test]
    fn different_keys_hash_differently() {
        let mut a = FxHasher::default();
        a.write(b"autocommit");
        let mut b = FxHasher::default();
        b.write(b"autocommiy");
        assert_ne!(a.finish(), b.finish());
    }

    #[test]
    fn works_as_a_std_table() {
        let mut table: FxHashMap<&str, u32> = FxHashMap::default();
        table.insert("a", 1);
        table.insert("bcdefgh", 2);
        assert_eq!(table.get("a"), Some(&1));
        assert_eq!(table.get("bcdefgh"), Some(&2));
        assert_eq!(table.get("z"), None);

        let set: FxHashSet<u64> = [1u64, 2, 3].into_iter().collect();
        assert!(set.contains(&2));

        // A long key exercises the 8-byte chunk loop plus tail.
        let long = "validate_password_special_char_count";
        let mut map: HashMap<&str, u8, FxBuildHasher> = HashMap::default();
        map.insert(long, 1);
        assert_eq!(map.get(long), Some(&1));
    }
}
