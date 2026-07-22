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

//! Primitive FNV-1a hashing from `pkg/planner/cascades/base/hash_equaler.go`.
//!
//! The source hashes planner metadata incrementally without allocating an
//! intermediate byte encoding. This leaf preserves the primitive update order,
//! byte-length string framing, reset/cache lifecycle, and 64-bit wrapping while
//! leaving cascades object implementations and equality dispatch external.

use tidb_hash::IHasher;

/// Planner hasher contract, adapted to the parser-owned primitive authority.
///
/// Rust has no stable trait alias that can add planner-only methods, so this
/// object-safe facade preserves the existing planner API while its
/// implementation delegates every primitive operation to [`IHasher`].
pub trait Hasher {
    /// Hashes a boolean as zero or one.
    fn hash_bool(&mut self, value: bool);
    /// Hashes a signed machine-sized integer on TiDB's 64-bit target.
    fn hash_int(&mut self, value: i64);
    /// Hashes a signed 64-bit integer.
    fn hash_int64(&mut self, value: i64);
    /// Hashes an unsigned 64-bit integer.
    fn hash_uint64(&mut self, value: u64);
    /// Hashes the IEEE-754 bit representation of a float.
    fn hash_float64(&mut self, value: f64);
    /// Hashes a source rune represented as a signed 32-bit code point.
    fn hash_rune(&mut self, value: i32);
    /// Hashes a UTF-8 string with its byte length followed by its runes.
    fn hash_string(&mut self, value: &str);
    /// Hashes one byte as a rune.
    fn hash_byte(&mut self, value: u8);
    /// Hashes a byte slice with its length followed by its bytes.
    fn hash_bytes(&mut self, value: &[u8]);
    /// Resets the digest and reuses the cache allocation.
    fn reset(&mut self);
    /// Returns the current digest.
    fn sum64(&self) -> u64;
    /// Replaces the reusable planner encoding cache.
    fn set_cache(&mut self, cache: Vec<u8>);
    /// Returns the reusable planner encoding cache.
    fn cache(&self) -> &[u8];
}

const OFFSET64: u64 = 14_695_981_039_346_656_037;
const PRIME64: u64 = 1_099_511_628_211;

/// Marker written for a nil pointer/interface field.
pub const NIL_FLAG: u8 = 0;
/// Marker written before the contents of a non-nil pointer/interface field.
pub const NOT_NIL_FLAG: u8 = 1;

/// The 64-bit digest accumulated by the planner hasher.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Hash64a(u64);

impl Hash64a {
    /// Creates a digest from its raw value.
    #[must_use]
    pub const fn new(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw digest value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

/// FNV-1a planner hasher with a reusable byte cache.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HashEqualer {
    hash64a: Hash64a,
    cache: Vec<u8>,
}

impl HashEqualer {
    fn absorb(&mut self, value: u64) {
        self.hash64a.0 ^= value;
        self.hash64a.0 = self.hash64a.0.wrapping_mul(PRIME64);
    }
}

/// Creates a planner hasher initialized to the FNV-1a offset basis.
#[must_use]
pub fn new_hash_equaler() -> HashEqualer {
    HashEqualer {
        hash64a: Hash64a::new(OFFSET64),
        cache: Vec::new(),
    }
}

impl IHasher for HashEqualer {
    fn hash_bool(&mut self, value: bool) {
        self.absorb(if value { 1 } else { 0 });
    }

    fn hash_int(&mut self, value: i64) {
        self.absorb(value as u64);
    }

    fn hash_int64(&mut self, value: i64) {
        self.absorb(value as u64);
    }

    fn hash_uint64(&mut self, value: u64) {
        self.absorb(value);
    }

    fn hash_float64(&mut self, value: f64) {
        self.absorb(value.to_bits());
    }

    fn hash_rune(&mut self, value: i32) {
        self.absorb(value as u64);
    }

    fn hash_string(&mut self, value: &[u8]) {
        IHasher::hash_int(self, value.len() as i64);
        let mut remaining = value;
        while !remaining.is_empty() {
            match std::str::from_utf8(remaining) {
                Ok(valid) => {
                    for character in valid.chars() {
                        IHasher::hash_rune(self, character as i32);
                    }
                    break;
                }
                Err(error) => {
                    let valid_length = error.valid_up_to();
                    let valid = std::str::from_utf8(&remaining[..valid_length])
                        .expect("Utf8Error::valid_up_to always ends at a character boundary");
                    for character in valid.chars() {
                        IHasher::hash_rune(self, character as i32);
                    }
                    // Go's range over a malformed string yields RuneError and
                    // advances one byte for every invalid encoding.
                    IHasher::hash_rune(self, '\u{fffd}' as i32);
                    remaining = &remaining[valid_length + 1..];
                }
            }
        }
    }

    fn hash_byte(&mut self, value: u8) {
        IHasher::hash_rune(self, i32::from(value));
    }

    fn hash_bytes(&mut self, value: &[u8]) {
        IHasher::hash_int(self, value.len() as i64);
        for &byte in value {
            IHasher::hash_byte(self, byte);
        }
    }

    fn reset(&mut self) {
        self.hash64a = Hash64a::new(OFFSET64);
        self.cache.clear();
    }

    fn sum64(&self) -> u64 {
        self.hash64a.raw()
    }
}

impl Hasher for HashEqualer {
    fn hash_bool(&mut self, value: bool) {
        IHasher::hash_bool(self, value);
    }

    fn hash_int(&mut self, value: i64) {
        IHasher::hash_int(self, value);
    }

    fn hash_int64(&mut self, value: i64) {
        IHasher::hash_int64(self, value);
    }

    fn hash_uint64(&mut self, value: u64) {
        IHasher::hash_uint64(self, value);
    }

    fn hash_float64(&mut self, value: f64) {
        IHasher::hash_float64(self, value);
    }

    fn hash_rune(&mut self, value: i32) {
        IHasher::hash_rune(self, value);
    }

    fn hash_string(&mut self, value: &str) {
        IHasher::hash_string(self, value.as_bytes());
    }

    fn hash_byte(&mut self, value: u8) {
        IHasher::hash_byte(self, value);
    }

    fn hash_bytes(&mut self, value: &[u8]) {
        IHasher::hash_bytes(self, value);
    }

    fn reset(&mut self) {
        IHasher::reset(self);
    }

    fn sum64(&self) -> u64 {
        IHasher::sum64(self)
    }

    fn set_cache(&mut self, cache: Vec<u8>) {
        self.cache = cache;
    }

    fn cache(&self) -> &[u8] {
        &self.cache
    }
}
