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

//! Source-backed tests for the memory-budgeted apply cache.

use tidb_exec::apply_cache::{apply_cache_kv_mem, ApplyCache};

#[test]
fn apply_cache_memory_charge_and_lru_eviction_match_source() {
    // Source: pkg/executor/internal/applycache/apply_cache.go:35-43,
    // 76-101.
    // Direct Go coverage: pkg/executor/internal/applycache/apply_cache_test.go:30
    // (TestApplyCache), quota=100 and three 100-byte key/value entries.
    let mut cache = ApplyCache::new(100);
    let keys: Vec<Vec<u8>> = (0..3).map(|i| vec![b'0' + i; 100]).collect();
    for key in &keys {
        assert_eq!(apply_cache_kv_mem(key, 0), 100);
    }

    assert!(cache.set(keys[0].clone(), 0_i64, 0));
    assert_eq!(cache.get(&keys[0]), Some(&0));
    assert_eq!(cache.memory_consumed(), 100);

    assert!(cache.set(keys[1].clone(), 1_i64, 0));
    assert_eq!(cache.get(&keys[1]), Some(&1));
    assert!(cache.set(keys[2].clone(), 2_i64, 0));
    assert_eq!(cache.get(&keys[2]), Some(&2));

    // key 0 was touched before key 1 and key 2 arrived; key 0 and key 1 are
    // therefore evicted by the two subsequent 100-byte insertions.
    assert_eq!(cache.get(&keys[0]), None);
    assert_eq!(cache.get(&keys[1]), None);
    assert_eq!(cache.get(&keys[2]), Some(&2));
    assert_eq!(cache.memory_consumed(), 100);
}

#[test]
fn apply_cache_rejects_an_item_larger_than_quota_without_mutation() {
    // Source: pkg/executor/internal/applycache/apply_cache.go:86-101.
    // Direct Go coverage: pkg/executor/internal/applycache/apply_cache_test.go:30
    // (TestApplyCache), Set's oversize admission boundary is source-owned even
    // though the fixture uses exactly-quota entries.
    let mut cache = ApplyCache::new(100);
    assert!(!cache.set(vec![b'x'; 101], 7_i64, 0));
    assert!(cache.is_empty());
    assert_eq!(cache.memory_consumed(), 0);
}
