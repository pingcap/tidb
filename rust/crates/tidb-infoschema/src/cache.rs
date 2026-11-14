// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/infoschema/cache.go`: known schema ranges, including empty diffs
//! and unknown commit timestamps. Its owner serializes mutations.
use crate::SessionExtendedInfoSchema;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

fn count(action: &str, kind: &str) {
    tidb_metrics::domain::INFOCACHE_COUNTERS
        .with_label_values(&[action, kind])
        .inc();
}

/// Go `schemaAndTimestamp`.
#[derive(Clone, Debug)]
struct SchemaAndTimestamp {
    infoschema: Arc<SessionExtendedInfoSchema>,
    timestamp: u64,
}
/// Go `InfoCache`'s version/timestamp index. `catalog_watch::InfoCache` owns
/// this index and the reload lock, schema builder, and shared V2 Data.
#[derive(Debug)]
pub struct InfoCache {
    cache: BTreeMap<i64, SchemaAndTimestamp>,
    capacity: usize,
    empty_schema_versions: BTreeSet<i64>,
    first_known_schema_version: i64,
}
impl InfoCache {
    /// Go `NewCache`.
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: BTreeMap::new(),
            capacity,
            empty_schema_versions: BTreeSet::new(),
            first_known_schema_version: 0,
        }
    }
    /// Go `InfoCache.ReSize`.
    pub fn resize(&mut self, capacity: usize) {
        self.capacity = capacity;
        while self.cache.len() > capacity {
            self.cache.pop_first();
        }
    }
    /// Go `InfoCache.Size` / `Len`.
    pub fn len(&self) -> usize {
        self.cache.len()
    }
    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }
    /// Go `InfoCache.Reset`.
    pub fn reset(&mut self, capacity: usize) {
        self.cache.clear();
        self.capacity = capacity;
    }
    /// Go `InfoCache.GetLatest`.
    pub fn get_latest(&self) -> Option<Arc<SessionExtendedInfoSchema>> {
        count("get", "latest");
        self.cache.last_key_value().map(|(_, v)| {
            count("hit", "latest");
            v.infoschema.clone()
        })
    }
    /// Go `InfoCache.GetByVersion`.
    pub fn get_by_version(&self, version: i64) -> Option<Arc<SessionExtendedInfoSchema>> {
        count("get", "version");
        let latest = *self.cache.last_key_value()?.0;
        let (found, item) = self.cache.range(..=version).next_back()?;
        (*found == version || (*found != latest && *found >= self.first_known_schema_version)).then(
            || {
                count("hit", "version");
                item.infoschema.clone()
            },
        )
    }
    /// Go `InfoCache.GetBySnapshotTS` / `getSchemaByTimestampNoLock`.
    pub fn get_by_snapshot_ts(&self, ts: u64) -> Option<Arc<SessionExtendedInfoSchema>> {
        count("get", "ts");
        let mut previous: Option<(i64, u64)> = None;
        for (&version, item) in self.cache.iter().rev() {
            if item.timestamp != 0 && ts >= item.timestamp {
                let valid = match previous {
                    None => true,
                    Some((last_version, last_ts)) => {
                        last_ts > ts
                            && (version + 1..last_version)
                                .all(|v| self.empty_schema_versions.contains(&v))
                    }
                };
                return valid.then(|| {
                    count("hit", "ts");
                    item.infoschema.clone()
                });
            }
            previous = Some((version, item.timestamp));
        }
        None
    }
    /// Go `InfoCache.Insert`.
    pub fn insert(&mut self, schema: Arc<SessionExtendedInfoSchema>, ts: u64) -> bool {
        let version = schema.schema_meta_version();
        if let Some(item) = self.cache.get_mut(&version) {
            if item.infoschema.is_v2() == schema.is_v2() {
                if ts > 0 && item.timestamp == 0 {
                    item.timestamp = ts;
                } else if schema.is_v2() {
                    item.infoschema = schema;
                }
                return true;
            }
            // V1/V2 changes replace the entry; mode switches normally use Upsert.
            *item = SchemaAndTimestamp {
                infoschema: schema,
                timestamp: ts,
            };
            return true;
        }
        if self.capacity == 0
            || (self.cache.len() >= self.capacity
                && self
                    .cache
                    .first_key_value()
                    .is_some_and(|(v, _)| *v > version))
        {
            return false;
        }
        let first = self.cache.is_empty();
        self.cache.insert(
            version,
            SchemaAndTimestamp {
                infoschema: schema,
                timestamp: ts,
            },
        );
        if first {
            self.first_known_schema_version = version;
        }
        while self.cache.len() > self.capacity {
            self.cache.pop_first();
        }
        true
    }
    /// Go `InfoCache.Upsert`: reset known history on a V1/V2 switch. Existing
    /// readers own their Arcs; Rust never mutates a previously published image.
    pub fn upsert(&mut self, schema: Arc<SessionExtendedInfoSchema>, ts: u64) {
        self.cache.clear();
        self.first_known_schema_version = schema.schema_meta_version();
        self.cache.insert(
            schema.schema_meta_version(),
            SchemaAndTimestamp {
                infoschema: schema,
                timestamp: ts,
            },
        );
    }
    /// Go `InfoCache.InsertEmptySchemaVersion`.
    pub fn insert_empty_schema_version(&mut self, version: i64) {
        self.empty_schema_versions.insert(version);
        while self.empty_schema_versions.len() > self.capacity {
            self.empty_schema_versions.pop_first();
        }
    }
    /// Go `InfoCache.GetEmptySchemaVersions`.
    pub fn empty_schema_versions(&self) -> &BTreeSet<i64> {
        &self.empty_schema_versions
    }
    /// Rebuild retained images when the domain installs its table factory.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut Arc<SessionExtendedInfoSchema>> {
        self.cache.values_mut().map(|v| &mut v.infoschema)
    }
    pub fn keys(&self) -> impl Iterator<Item = &i64> {
        self.cache.keys()
    }
    pub fn contains_key(&self, version: &i64) -> bool {
        self.cache.contains_key(version)
    }
    pub fn get(&self, version: &i64) -> Option<&Arc<SessionExtendedInfoSchema>> {
        self.cache.get(version).map(|v| &v.infoschema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn schema(version: i64) -> Arc<SessionExtendedInfoSchema> {
        let mut s = SessionExtendedInfoSchema::memory_schema_scratch();
        s.set_schema_meta_version(version);
        Arc::new(s)
    }
    fn version(s: Option<Arc<SessionExtendedInfoSchema>>) -> Option<i64> {
        s.map(|s| s.schema_meta_version())
    }
    /// Go `TestNewCache`, test/cachetest/cache_test.go.
    #[test]
    fn test_new_cache() {
        let ic = InfoCache::new(16);
        assert_eq!(ic.capacity, 16);
        assert!(ic.is_empty());
    }
    /// Go `TestGetLatest`, test/cachetest/cache_test.go.
    #[test]
    fn test_get_latest() {
        let mut ic = InfoCache::new(16);
        assert!(ic.get_latest().is_none());
        for v in [1, 2, 0] {
            ic.insert(schema(v), v as u64);
        }
        assert_eq!(version(ic.get_latest()), Some(2));
    }
    /// Go `TestInsert`.
    #[test]
    fn test_insert() {
        let mut ic = InfoCache::new(3);
        ic.insert(schema(2), 2);
        assert_eq!(version(ic.get_by_snapshot_ts(10)), Some(2));
        assert!(ic.get_by_snapshot_ts(0).is_none());
        ic.insert(schema(5), 5);
        assert!(ic.get_by_snapshot_ts(2).is_none());
        ic.insert(schema(0), 0);
        assert_eq!(version(ic.get_by_version(0)), Some(0));
        ic.insert(schema(6), 6);
        assert!(ic.get_by_version(0).is_none());
        assert_eq!(version(ic.get_by_snapshot_ts(5)), Some(5));
        ic.insert(schema(3), 3);
        assert!(ic.get_by_version(2).is_none());
        assert!(!ic.insert(schema(2), 2));
        assert!(ic.insert(schema(5), 5));
        assert_eq!(version(ic.get_by_snapshot_ts(10)), Some(6));
        assert_eq!(ic.len(), 3);
    }
    /// Go `TestGetByVersion` plus cache.go's Upsert historical-gap example.
    #[test]
    fn test_get_by_version() {
        let mut ic = InfoCache::new(2);
        ic.insert(schema(1), 1);
        ic.insert(schema(3), 3);
        assert_eq!(version(ic.get_by_version(1)), Some(1));
        assert_eq!(version(ic.get_by_version(2)), Some(1));
        assert_eq!(version(ic.get_by_version(3)), Some(3));
        assert!(ic.get_by_version(0).is_none());
        assert!(ic.get_by_version(4).is_none());
        ic.upsert(schema(56), 56);
        ic.insert(schema(51), 51);
        assert!(ic.get_by_version(55).is_none());
        assert_eq!(version(ic.get_by_version(51)), Some(51));
    }
    /// Go `TestGetByTimestamp`.
    #[test]
    fn test_get_by_timestamp() {
        let mut ic = InfoCache::new(16);
        ic.insert(schema(1), 1);
        ic.insert(schema(3), 3);
        assert!(ic.get_by_snapshot_ts(2).is_none());
        ic.insert(schema(2), 0);
        for ts in 0..3 {
            assert!(ic.get_by_snapshot_ts(ts).is_none());
        }
        assert_eq!(version(ic.get_by_snapshot_ts(4)), Some(3));
        ic.insert(schema(2), 2);
        for ts in 1..=3 {
            assert_eq!(version(ic.get_by_snapshot_ts(ts)), Some(ts as i64));
        }
    }
    /// Go `TestReSize`.
    #[test]
    fn test_re_size() {
        let mut ic = InfoCache::new(2);
        ic.insert(schema(1), 1);
        ic.insert(schema(2), 2);
        ic.resize(3);
        assert!(ic.insert(schema(3), 3));
        assert_eq!(ic.len(), 3);
        ic.resize(1);
        assert_eq!(ic.len(), 1);
        assert!(ic.get_by_version(2).is_none());
        assert!(!ic.insert(schema(2), 2));
        assert!(ic.insert(schema(4), 4));
        assert!(ic.get_by_version(3).is_none());
    }
    /// Go `TestCacheWithSchemaTsZero`.
    #[test]
    fn test_cache_with_schema_ts_zero() {
        let mut ic = InfoCache::new(16);
        for v in 1..=8 {
            ic.insert(schema(v), v as u64);
        }
        ic.insert(schema(9), 0);
        for ts in 1..=7 {
            assert_eq!(version(ic.get_by_snapshot_ts(ts)), Some(ts as i64));
        }
        for ts in 8..=10 {
            assert!(ic.get_by_snapshot_ts(ts).is_none());
        }
        for v in 10..=16 {
            ic.insert(schema(v), v as u64);
        }
        for ts in 8..=9 {
            assert!(ic.get_by_snapshot_ts(ts).is_none());
        }
        ic.insert(schema(9), 9);
        for ts in 1..=16 {
            assert_eq!(version(ic.get_by_snapshot_ts(ts)), Some(ts as i64));
        }
        ic.insert(schema(17), 17);
        assert!(ic.get_by_snapshot_ts(1).is_none());
        let mut ic = InfoCache::new(16);
        ic.insert(schema(100), 100);
        for v in 1..=16 {
            ic.insert(schema(v), v as u64);
        }
        assert!(ic.get_by_snapshot_ts(1).is_none());
        assert!(ic.get_by_snapshot_ts(16).is_none());
        for ts in 2..=15 {
            assert_eq!(version(ic.get_by_snapshot_ts(ts)), Some(ts as i64));
        }
        for v in 85..100 {
            ic.insert(schema(v), v as u64);
        }
        for ts in 85..=100 {
            assert_eq!(version(ic.get_by_snapshot_ts(ts)), Some(ts as i64));
        }
        let mut ic = InfoCache::new(16);
        for v in 1..=8 {
            ic.insert(schema(v), v as u64);
        }
        ic.insert(schema(10), 10);
        assert!(ic.get_by_snapshot_ts(8).is_none());
        assert!(ic.get_by_snapshot_ts(9).is_none());
        ic.insert_empty_schema_version(9);
        assert_eq!(version(ic.get_by_snapshot_ts(8)), Some(8));
        assert_eq!(version(ic.get_by_snapshot_ts(9)), Some(8));
    }
    /// Go `TestCacheEmptySchemaVersion`.
    #[test]
    fn test_cache_empty_schema_version() {
        let mut ic = InfoCache::new(16);
        for v in 0..20 {
            ic.insert_empty_schema_version(v);
        }
        assert_eq!(ic.empty_schema_versions(), &(4..20).collect());
    }
}
