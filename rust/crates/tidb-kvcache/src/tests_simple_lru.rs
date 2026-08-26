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

//! Test-port coverage for Go `pkg/util/kvcache/simple_lru_test.go`.
//!
//! The eight Go test functions (`TestPut`, `TestZeroQuota`, `TestOOMGuard`,
//! `TestGet`, `TestDelete`, `TestDeleteAll`, `TestValues`,
//! `TestPutProfileName`) are already translated as the integration tests in
//! `crates/tidb-kvcache/tests/simple_lru_test.rs`; this module carries only
//! the assertion that crate cannot express yet.

use crate::{CacheKey, SimpleLruCache};

/// Mock key mirroring Go `mockCacheKey`: identity is the raw hash bytes.
#[derive(Clone, Debug)]
struct Key {
    key: i64,
    hash: Vec<u8>,
}

impl Key {
    fn new(key: i64) -> Self {
        Self {
            key,
            hash: (key as u64).to_be_bytes().to_vec(),
        }
    }
}

impl CacheKey for Key {
    fn hash_bytes(&self) -> &[u8] {
        &self.hash
    }
}

/// From Go `TestGet` (pkg/util/kvcache/simple_lru_test.go): after filling a
/// cache of capacity 3 with keys 0..5, `lru.Peek(keys[2])` must find the entry
/// *without* promoting it — the list front afterwards is still keys[4].
#[test]
#[ignore = "go-parity-gap: SimpleLruCache has no Peek (non-promoting lookup); adding it requires a lib.rs API change outside this batch's edit scope"]
fn get_peek_does_not_promote_entry() {
    let mut lru = SimpleLruCache::new(3);
    for i in 0..5 {
        lru.put(Key::new(i), i);
    }
    // With Peek available this would assert:
    //   assert_eq!(lru.peek(&Key::new(2)), Some(&2));
    //   assert_eq!(lru.keys()[0].key, 4); // front unchanged by the peek
    let _ = lru.get(&Key::new(4)); // placeholder use so the fixture stays valid
}
