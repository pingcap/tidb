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

//! Transcreation of Go `pkg/util/generic/sync_map.go`.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::RwLock;

/// The generic version of a mutex-guarded map (Go's `SyncMap`).
///
/// Go pairs a bare `map[K]V` with a separate `sync.RWMutex`; Rust's
/// [`RwLock`] owns the map, so the two are one field and no unsynchronized
/// access is representable.
pub struct SyncMap<K, V> {
    item: RwLock<HashMap<K, V>>,
}

impl<K: Eq + Hash, V> SyncMap<K, V> {
    /// Returns a new `SyncMap` preallocating room for `capacity` entries.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            item: RwLock::new(HashMap::with_capacity(capacity)),
        }
    }

    /// Stores a value.
    pub fn store(&self, key: K, value: V) {
        self.item.write().unwrap().insert(key, value);
    }

    /// Loads the value for a key, returning `None` when absent.
    ///
    /// Go returns `(V, bool)` where the value is `V`'s zero value on a miss;
    /// [`Option`] fuses those into the presence signal and drops the zero-value
    /// special case.
    #[must_use]
    pub fn load(&self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        self.item.read().unwrap().get(key).cloned()
    }

    /// Deletes the value for a key, returning the previous value if any.
    pub fn delete(&self, key: &K) -> Option<V> {
        self.item.write().unwrap().remove(key)
    }

    /// Returns all the keys in the map.
    #[must_use]
    pub fn keys(&self) -> Vec<K>
    where
        K: Clone,
    {
        self.item.read().unwrap().keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::SyncMap;

    // Go `TestSyncMap`.
    #[test]
    fn sync_map() {
        let sm = SyncMap::<i64, String>::new(10);
        sm.store(1, "a".to_string());
        sm.store(2, "b".to_string());
        // Load an existing key.
        assert_eq!(sm.load(&1), Some("a".to_string()));
        // Load a non-existent key.
        assert_eq!(sm.load(&3), None);
        // Overwrite the value.
        sm.store(1, "c".to_string());
        assert_eq!(sm.load(&1), Some("c".to_string()));
        // Drop an existing key.
        sm.delete(&1);
        assert_eq!(sm.load(&1), None);
        // Drop a non-existent key.
        sm.delete(&3);
        assert_eq!(sm.keys(), vec![2]);
        assert_eq!(sm.load(&3), None);

        // Test the keys() method.
        let sm = SyncMap::<i64, String>::new(10);
        sm.store(2, "b".to_string());
        sm.store(1, "a".to_string());
        sm.store(3, "c".to_string());
        let mut keys = sm.keys();
        keys.sort_unstable();
        assert_eq!(keys, vec![1, 2, 3]);
    }
}
