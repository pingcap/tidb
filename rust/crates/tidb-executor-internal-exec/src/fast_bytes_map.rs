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

//! The byte-key hash map the hash join and the hash aggregate key their
//! buckets by: FNV-1a over an already-encoded key, as Go's executors hash
//! their encoded group and join keys.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

/// Go's hash join feeds encoded key columns to `hash/fnv.New64`. The complete
/// encoded key is still compared after a bucket hit, so this hash is only a
/// fast bucket selector and collisions cannot change SQL results.
#[derive(Default)]
pub struct FastBytesHasher {
    hash: u64,
    initialized: bool,
}

impl Hasher for FastBytesHasher {
    fn finish(&self) -> u64 {
        if self.initialized {
            self.hash
        } else {
            0xcbf29ce484222325
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = if self.initialized {
            self.hash
        } else {
            self.initialized = true;
            0xcbf29ce484222325
        };
        for byte in bytes {
            hash = hash.wrapping_mul(0x100000001b3);
            hash ^= u64::from(*byte);
        }
        self.hash = hash;
    }
}

pub type FastBytesMap<V> = HashMap<Vec<u8>, V, BuildHasherDefault<FastBytesHasher>>;
