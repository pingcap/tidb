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

//! Table-scoped snapshot cache from `pkg/kv/cachedb.go`.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::RwLock;

use crate::{GetOptions, Getter, Key};

/// Source per-table cache capacity.
pub const TABLE_CACHE_CAPACITY_BYTES: usize = 100 * 1024 * 1024;
const MAX_CACHE_KEY_BYTES: usize = u16::MAX as usize;
const MAX_CACHE_ENTRY_BYTES: usize = TABLE_CACHE_CAPACITY_BYTES / 1024 - 24;

/// Cache read/write failures.
#[derive(Debug)]
pub enum CacheDbError<E> {
    /// Snapshot read failure.
    Snapshot(E),
    /// Freecache rejects keys larger than `uint16`.
    KeyTooLarge,
    /// Freecache rejects entries larger than one quarter of a segment.
    EntryTooLarge,
}

impl<E: fmt::Display> fmt::Display for CacheDbError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(error) => error.fmt(formatter),
            Self::KeyTooLarge => formatter.write_str("The key is larger than 65535"),
            Self::EntryTooLarge => {
                formatter.write_str("The entry size is larger than 1/1024 of cache size")
            }
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for CacheDbError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Snapshot(error) => Some(error),
            Self::KeyTooLarge | Self::EntryTooLarge => None,
        }
    }
}

#[derive(Debug, Default)]
struct TableCache {
    entries: HashMap<Key, Vec<u8>>,
    insertion_order: VecDeque<Key>,
    bytes: usize,
}

impl TableCache {
    fn get(&self, key: &Key) -> Option<Vec<u8>> {
        self.entries.get(key).cloned()
    }

    fn set<E>(&mut self, key: Key, value: Vec<u8>) -> Result<(), CacheDbError<E>> {
        if key.as_bytes().len() > MAX_CACHE_KEY_BYTES {
            return Err(CacheDbError::KeyTooLarge);
        }
        let entry_bytes = key.as_bytes().len().saturating_add(value.len());
        if entry_bytes > MAX_CACHE_ENTRY_BYTES {
            return Err(CacheDbError::EntryTooLarge);
        }

        if let Some(previous) = self.entries.remove(&key) {
            self.bytes = self
                .bytes
                .saturating_sub(key.as_bytes().len().saturating_add(previous.len()));
            self.insertion_order.retain(|candidate| candidate != &key);
        }
        while self.bytes.saturating_add(entry_bytes) > TABLE_CACHE_CAPACITY_BYTES {
            let Some(oldest) = self.insertion_order.pop_front() else {
                break;
            };
            if let Some(previous) = self.entries.remove(&oldest) {
                self.bytes = self
                    .bytes
                    .saturating_sub(oldest.as_bytes().len().saturating_add(previous.len()));
            }
        }
        self.bytes = self.bytes.saturating_add(entry_bytes);
        self.insertion_order.push_back(key.clone());
        self.entries.insert(key, value);
        Ok(())
    }
}

/// A cache between a transaction buffer and its snapshot.
#[derive(Debug, Default)]
pub struct CacheDb {
    tables: RwLock<HashMap<i64, TableCache>>,
}

/// Cache between a transaction buffer and persistent storage.
pub trait MemManager {
    /// Gets from the table cache first and populates it from the snapshot on a
    /// miss.
    fn union_get<S>(
        &self,
        table_id: i64,
        snapshot: &mut S,
        key: &Key,
    ) -> Result<Vec<u8>, CacheDbError<S::Error>>
    where
        S: Getter;

    /// Releases one table cache.
    fn delete(&self, table_id: i64);
}

impl CacheDb {
    /// Creates an empty cache database.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets from a table cache first, otherwise reads and caches the snapshot.
    pub fn union_get<S>(
        &self,
        table_id: i64,
        snapshot: &mut S,
        key: &Key,
    ) -> Result<Vec<u8>, CacheDbError<S::Error>>
    where
        S: Getter,
    {
        if let Some(value) = self
            .tables
            .read()
            .expect("cache lock poisoned")
            .get(&table_id)
            .and_then(|table| table.get(key))
        {
            return Ok(value);
        }

        let value = snapshot
            .get(key, GetOptions::default())
            .map_err(CacheDbError::Snapshot)?
            .value;
        self.tables
            .write()
            .expect("cache lock poisoned")
            .entry(table_id)
            .or_default()
            .set::<S::Error>(key.clone(), value.clone())?;
        Ok(value)
    }

    /// Releases one table cache.
    pub fn delete(&self, table_id: i64) {
        self.tables
            .write()
            .expect("cache lock poisoned")
            .remove(&table_id);
    }
}

impl MemManager for CacheDb {
    fn union_get<S>(
        &self,
        table_id: i64,
        snapshot: &mut S,
        key: &Key,
    ) -> Result<Vec<u8>, CacheDbError<S::Error>>
    where
        S: Getter,
    {
        Self::union_get(self, table_id, snapshot, key)
    }

    fn delete(&self, table_id: i64) {
        Self::delete(self, table_id);
    }
}

/// Creates an empty cache database.
#[must_use]
pub fn new_cache_db() -> CacheDb {
    CacheDb::new()
}
