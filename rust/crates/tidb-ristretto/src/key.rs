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

//! Native substitute for z.KeyToHash and Go's runtime-dependent MemHash.
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

/// Supported Go key representations. Strings and bytes hash identically.
#[derive(Clone, Copy, Debug)]
pub enum KeyRef<'a> {
    /// Integer identity, including two's-complement signed keys.
    Integer(u64),
    /// String or byte-slice contents.
    Bytes(&'a [u8]),
}

/// A supported cache key. Integer hashes preserve all 64 bits.
pub trait Key {
    /// Borrow the canonical key representation.
    fn key_ref(&self) -> KeyRef<'_>;
}
macro_rules! integers {
    ($($t:ty),*) => { $(impl Key for $t {
        fn key_ref(&self) -> KeyRef<'_> { KeyRef::Integer(*self as u64) }
    })* };
}
integers!(u8, u32, u64, usize, i32, i64, isize);
impl Key for str {
    fn key_ref(&self) -> KeyRef<'_> {
        KeyRef::Bytes(self.as_bytes())
    }
}
impl Key for String {
    fn key_ref(&self) -> KeyRef<'_> {
        self.as_str().key_ref()
    }
}
impl Key for [u8] {
    fn key_ref(&self) -> KeyRef<'_> {
        KeyRef::Bytes(self)
    }
}
impl Key for Vec<u8> {
    fn key_ref(&self) -> KeyRef<'_> {
        self.as_slice().key_ref()
    }
}

pub(crate) fn hash(key: KeyRef<'_>, seed: &RandomState) -> (u64, u64) {
    match key {
        KeyRef::Integer(value) => (value, 0),
        KeyRef::Bytes(bytes) => {
            let mut primary = seed.build_hasher();
            primary.write(bytes);
            (primary.finish(), xxhash_rust::xxh64::xxh64(bytes, 0))
        }
    }
}
