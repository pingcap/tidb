// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::future::Future;

use crate::proto::kvrpcpb;
use crate::BoundRange;
use crate::Key;
use crate::KvPair;
use crate::Result;
use crate::Value;
use crate::ValueEntry;

use super::transaction::Mutation;

/// A caching layer which buffers reads and writes in a transaction.
pub struct Buffer {
    primary_key: Option<Key>,
    entry_map: BTreeMap<Key, BufferEntry>,
    is_pessimistic: bool,
    snapshot_cache_hits: usize,
}

impl Buffer {
    pub fn new(is_pessimistic: bool) -> Buffer {
        Buffer {
            primary_key: None,
            entry_map: BTreeMap::new(),
            is_pessimistic,
            snapshot_cache_hits: 0,
        }
    }

    /// Get the primary key of the buffer.
    pub fn get_primary_key(&self) -> Option<Key> {
        self.primary_key.clone()
    }

    /// Discard values cached by snapshot reads while retaining transaction mutations.
    pub(crate) fn clear_cached_reads(&mut self) {
        self.entry_map
            .retain(|_, entry| !matches!(entry, BufferEntry::Cached(_)));
    }

    /// Return the number of source snapshot-cache hits observed by this
    /// buffer. Clearing cached reads deliberately preserves this counter.
    pub(crate) fn snapshot_cache_hit_count(&self) -> usize {
        self.snapshot_cache_hits
    }

    /// Return the number of entries retained solely by snapshot reads.
    /// Cached misses count as entries, matching client-go's snapshot cache.
    pub(crate) fn snapshot_cache_size(&self) -> usize {
        self.entry_map
            .values()
            .filter(|entry| matches!(entry, BufferEntry::Cached(_)))
            .count()
    }

    pub(crate) fn snapshot_cache(&self) -> BTreeMap<Key, ValueEntry> {
        self.entry_map
            .iter()
            .filter_map(|(key, entry)| match entry {
                BufferEntry::Cached(Some(value)) => Some((key.clone(), value.clone())),
                BufferEntry::Cached(None) => Some((key.clone(), ValueEntry::default())),
                _ => None,
            })
            .collect()
    }

    pub(crate) fn update_snapshot_cache(
        &mut self,
        keys: impl IntoIterator<Item = Key>,
        values: &BTreeMap<Key, ValueEntry>,
    ) {
        for key in keys {
            let value = values.get(&key).cloned();
            match self.entry_map.get(&key) {
                None | Some(BufferEntry::Cached(_)) => {
                    self.entry_map.insert(key, BufferEntry::Cached(value));
                }
                // Snapshot instances are read-only, so source cache mutation
                // never meets a write/lock entry. Preserve a transaction's
                // higher-priority local state if this internal buffer is
                // reused outside that facade.
                Some(_) => {}
            }
        }
    }

    pub(crate) fn clean_snapshot_cache(&mut self, keys: impl IntoIterator<Item = Key>) {
        for key in keys {
            if matches!(self.entry_map.get(&key), Some(BufferEntry::Cached(_))) {
                self.entry_map.remove(&key);
            }
        }
    }

    /// Set the primary key if it is not set
    pub fn primary_key_or(&mut self, key: &Key) {
        self.primary_key.get_or_insert_with(|| key.clone());
    }

    /// Get a value from the buffer.
    /// If the returned value is None, it means the key doesn't exist in buffer yet.
    pub fn get(&self, key: &Key) -> Option<Value> {
        match self.get_from_mutations(key) {
            MutationValue::Determined(value) => value,
            MutationValue::Undetermined => None,
        }
    }

    /// Get a value from the buffer. If the value is not present, run `f` to get
    /// the value.
    pub async fn get_or_else<F, Fut>(&mut self, key: Key, f: F) -> Result<Option<Value>>
    where
        F: FnOnce(Key) -> Fut,
        Fut: Future<Output = Result<Option<Value>>>,
    {
        self.get_or_else_with_cache(key, true, f).await
    }

    /// Get a value from the buffer, optionally retaining a fetched read in
    /// the local cache.
    pub async fn get_or_else_with_cache<F, Fut>(
        &mut self,
        key: Key,
        cache_result: bool,
        f: F,
    ) -> Result<Option<Value>>
    where
        F: FnOnce(Key) -> Fut,
        Fut: Future<Output = Result<Option<Value>>>,
    {
        let cached = matches!(self.entry_map.get(&key), Some(BufferEntry::Cached(_)));
        let value = self.get_from_mutations(&key);
        if cached {
            self.snapshot_cache_hits += 1;
        }
        match value {
            MutationValue::Determined(value) => Ok(value),
            MutationValue::Undetermined => {
                let value = f(key.clone()).await?;
                if cache_result {
                    self.update_cache(key, value.clone());
                }
                Ok(value)
            }
        }
    }

    /// Fetch a snapshot entry while retaining its optional commit timestamp.
    /// A cached non-empty entry without a commit timestamp cannot satisfy a
    /// later `ReturnCommitTs` read and is fetched again, as in client-go.
    pub async fn get_snapshot_entry_or_else<F, Fut>(
        &mut self,
        key: Key,
        return_commit_ts: bool,
        cache_result: bool,
        f: F,
    ) -> Result<Option<ValueEntry>>
    where
        F: FnOnce(Key) -> Fut,
        Fut: Future<Output = Result<Option<ValueEntry>>>,
    {
        if let Some(BufferEntry::Cached(value)) = self.entry_map.get(&key) {
            let eligible = value
                .as_ref()
                .is_none_or(|entry| !return_commit_ts || entry.commit_ts > 0);
            if eligible {
                self.snapshot_cache_hits += 1;
                return Ok(value.as_ref().map(|entry| {
                    let mut entry = entry.clone();
                    if !return_commit_ts {
                        entry.commit_ts = 0;
                    }
                    entry
                }));
            }

            let value = f(key.clone()).await?;
            if cache_result {
                self.update_snapshot_cache_entry(key, value.clone());
            }
            return Ok(value);
        }

        match self.get_from_mutations(&key) {
            MutationValue::Determined(value) => Ok(value.map(|value| ValueEntry::new(value, 0))),
            MutationValue::Undetermined => {
                let value = f(key.clone()).await?;
                if cache_result {
                    self.update_snapshot_cache_entry(key, value.clone());
                }
                Ok(value.map(|mut entry| {
                    if !return_commit_ts {
                        entry.commit_ts = 0;
                    }
                    entry
                }))
            }
        }
    }

    /// Get multiple values from the buffer. If any are not present, run `f` to
    /// get the missing values.
    ///
    /// only used for snapshot read (i.e. not for `batch_get_for_update`)
    pub async fn batch_get_or_else<F, Fut>(
        &mut self,
        keys: impl Iterator<Item = Key>,
        f: F,
    ) -> Result<impl Iterator<Item = KvPair>>
    where
        F: FnOnce(Box<dyn Iterator<Item = Key> + Send>) -> Fut,
        Fut: Future<Output = Result<Vec<KvPair>>>,
    {
        self.batch_get_or_else_with_cache(keys, true, f).await
    }

    /// Get multiple values from the buffer, optionally retaining fetched
    /// values and misses in the local cache.
    pub async fn batch_get_or_else_with_cache<F, Fut>(
        &mut self,
        keys: impl Iterator<Item = Key>,
        cache_results: bool,
        f: F,
    ) -> Result<impl Iterator<Item = KvPair>>
    where
        F: FnOnce(Box<dyn Iterator<Item = Key> + Send>) -> Fut,
        Fut: Future<Output = Result<Vec<KvPair>>>,
    {
        // Partition the keys into those we have buffered and those we have to
        // get from the store. A cached miss is still a source snapshot-cache
        // hit, even though it produces no output pair.
        let mut undetermined_keys = Vec::new();
        let mut cached_results = Vec::new();
        for key in keys {
            match self.entry_map.get(&key) {
                Some(BufferEntry::Cached(value)) => {
                    self.snapshot_cache_hits += 1;
                    if let Some(value) = value {
                        cached_results.push(KvPair(key, value.value.clone()));
                    }
                }
                Some(entry) => match entry.get_value() {
                    MutationValue::Determined(Some(value)) => {
                        cached_results.push(KvPair(key, value));
                    }
                    MutationValue::Determined(None) => {}
                    MutationValue::Undetermined => undetermined_keys.push(key),
                },
                None => undetermined_keys.push(key),
            }
        }

        let fetched_results = if undetermined_keys.is_empty() {
            Vec::new()
        } else {
            f(Box::new(undetermined_keys.clone().into_iter())).await?
        };
        if cache_results {
            let fetched_by_key: HashMap<_, _> = fetched_results
                .iter()
                .map(|pair| (pair.0.clone(), pair.1.clone()))
                .collect();
            for key in undetermined_keys {
                self.update_cache(key.clone(), fetched_by_key.get(&key).cloned());
            }
        }

        Ok(cached_results.into_iter().chain(fetched_results))
    }

    /// Batch variant of [`Self::get_snapshot_entry_or_else`]. The returned
    /// map excludes missing keys, while the cache retains those misses.
    pub async fn batch_get_snapshot_entries_or_else<F, Fut>(
        &mut self,
        keys: impl Iterator<Item = Key>,
        return_commit_ts: bool,
        cache_results: bool,
        f: F,
    ) -> Result<BTreeMap<Key, ValueEntry>>
    where
        F: FnOnce(Box<dyn Iterator<Item = Key> + Send>) -> Fut,
        Fut: Future<Output = Result<BTreeMap<Key, ValueEntry>>>,
    {
        let mut cached_results = BTreeMap::new();
        let mut undetermined_keys = Vec::new();
        for key in keys {
            match self.entry_map.get(&key) {
                Some(BufferEntry::Cached(value)) => {
                    let eligible = value
                        .as_ref()
                        .is_none_or(|entry| !return_commit_ts || entry.commit_ts > 0);
                    if eligible {
                        self.snapshot_cache_hits += 1;
                        if let Some(value) = value {
                            let mut value = value.clone();
                            if !return_commit_ts {
                                value.commit_ts = 0;
                            }
                            cached_results.insert(key, value);
                        }
                    } else {
                        undetermined_keys.push(key);
                    }
                }
                Some(entry) => match entry.get_value() {
                    MutationValue::Determined(Some(value)) => {
                        cached_results.insert(key, ValueEntry::new(value, 0));
                    }
                    MutationValue::Determined(None) => {}
                    MutationValue::Undetermined => undetermined_keys.push(key),
                },
                None => undetermined_keys.push(key),
            }
        }

        let fetched_results = if undetermined_keys.is_empty() {
            BTreeMap::new()
        } else {
            f(Box::new(undetermined_keys.clone().into_iter())).await?
        };
        if cache_results {
            for key in undetermined_keys {
                self.update_snapshot_cache_entry(key.clone(), fetched_results.get(&key).cloned());
            }
        }

        cached_results.extend(fetched_results.into_iter().map(|(key, mut value)| {
            if !return_commit_ts {
                value.commit_ts = 0;
            }
            (key, value)
        }));
        Ok(cached_results)
    }

    /// Run `f` to fetch entries in `range` from TiKV. Combine them with mutations in local buffer. Returns the results.
    pub async fn scan_and_fetch<F, Fut>(
        &mut self,
        range: BoundRange,
        limit: u32,
        update_cache: bool,
        reverse: bool,
        f: F,
    ) -> Result<impl Iterator<Item = KvPair>>
    where
        F: FnOnce(BoundRange, u32) -> Fut,
        Fut: Future<Output = Result<Vec<KvPair>>>,
    {
        // read from local buffer
        let mutation_range = self.entry_map.range(range.clone());

        // fetch from TiKV
        // fetch more entries because some of them may be deleted.
        let deleted_count = u32::try_from(
            mutation_range
                .clone()
                .filter(|(_, m)| matches!(m, BufferEntry::Del))
                .count(),
        )
        .unwrap_or(u32::MAX);
        let redundant_limit = limit.saturating_add(deleted_count);

        let mut results = f(range, redundant_limit)
            .await?
            .into_iter()
            .map(|pair| pair.into())
            .collect::<HashMap<Key, Value>>();

        // override using local data
        for (k, m) in mutation_range {
            match m {
                BufferEntry::Put(v) => {
                    results.insert(k.clone(), v.clone());
                }
                BufferEntry::Del => {
                    results.remove(k);
                }
                _ => {}
            }
        }

        // update local buffer
        if update_cache {
            for (k, v) in &results {
                self.update_cache(k.clone(), Some(v.clone()));
            }
        }

        let mut res = results
            .into_iter()
            .map(|(k, v)| KvPair::new(k, v))
            .collect::<Vec<_>>();

        // TODO: use `BTreeMap` instead of `HashMap` to avoid sorting.
        if reverse {
            res.sort_unstable_by(|a, b| b.key().cmp(a.key()));
        } else {
            res.sort_unstable_by(|a, b| a.key().cmp(b.key()));
        }

        Ok(res.into_iter().take(limit as usize))
    }

    /// Lock the given key if necessary.
    pub fn lock(&mut self, key: Key) {
        self.primary_key.get_or_insert_with(|| key.clone());
        let value = self
            .entry_map
            .entry(key)
            // Mutated keys don't need a lock.
            .or_insert(BufferEntry::Locked(None));
        // But values which we have only read, but not written, do.
        if let BufferEntry::Cached(v) = value {
            *value = BufferEntry::Locked(Some(v.take()))
        }
    }

    /// Unlock the given key if locked.
    pub fn unlock(&mut self, key: &Key) {
        if let Some(value) = self.entry_map.get_mut(key) {
            if let BufferEntry::Locked(v) = value {
                if let Some(v) = v {
                    *value = BufferEntry::Cached(v.take());
                } else {
                    self.entry_map.remove(key);
                }
            }
        }
    }

    /// Put a value into the buffer (does not write through).
    pub fn put(&mut self, key: Key, value: Value) {
        let mut entry = self.entry_map.entry(key.clone());
        match entry {
            Entry::Occupied(ref mut o)
                if matches!(o.get(), BufferEntry::Insert(_))
                    || matches!(o.get(), BufferEntry::CheckNotExist) =>
            {
                o.insert(BufferEntry::Insert(value));
            }
            _ => self.insert_entry(key, BufferEntry::Put(value)),
        }
    }

    /// Mark a value as Insert mutation into the buffer (does not write through).
    pub fn insert(&mut self, key: Key, value: Value) {
        let mut entry = self.entry_map.entry(key.clone());
        match entry {
            Entry::Occupied(ref mut o) if matches!(o.get(), BufferEntry::Del) => {
                o.insert(BufferEntry::Put(value));
            }
            _ => self.insert_entry(key, BufferEntry::Insert(value)),
        }
    }

    /// Mark a value as deleted.
    pub fn delete(&mut self, key: Key) {
        let is_pessimistic = self.is_pessimistic;
        let mut entry = self.entry_map.entry(key.clone());

        match entry {
            Entry::Occupied(ref mut o)
                if !is_pessimistic
                    && (matches!(o.get(), BufferEntry::Insert(_))
                        || matches!(o.get(), BufferEntry::CheckNotExist)) =>
            {
                o.insert(BufferEntry::CheckNotExist);
            }
            _ => self.insert_entry(key, BufferEntry::Del),
        }
    }

    pub(crate) fn mutate(&mut self, m: Mutation) {
        match m {
            Mutation::Put(key, value) => self.put(key, value),
            Mutation::Delete(key) => self.delete(key),
        };
    }

    /// Converts the buffered mutations to the proto buffer version
    pub fn to_proto_mutations(&self) -> Vec<kvrpcpb::Mutation> {
        self.entry_map
            .iter()
            .filter_map(|(key, mutation)| mutation.to_proto_with_key(key))
            .collect()
    }

    pub fn get_write_size(&self) -> usize {
        self.entry_map
            .iter()
            .map(|(k, v)| match v {
                BufferEntry::Put(val) | BufferEntry::Insert(val) => val.len() + k.len(),
                BufferEntry::Del => k.len(),
                _ => 0,
            })
            .sum()
    }

    fn get_from_mutations(&self, key: &Key) -> MutationValue {
        self.entry_map
            .get(key)
            .map(BufferEntry::get_value)
            .unwrap_or(MutationValue::Undetermined)
    }

    fn update_cache(&mut self, key: Key, value: Option<Value>) {
        self.update_cache_entry(key, value.map(|value| ValueEntry::new(value, 0)));
    }

    fn update_cache_entry(&mut self, key: Key, value: Option<ValueEntry>) {
        match self.entry_map.get(&key) {
            Some(BufferEntry::Locked(None)) => {
                self.entry_map.insert(key, BufferEntry::Locked(Some(value)));
            }
            None => {
                self.entry_map.insert(key, BufferEntry::Cached(value));
            }
            Some(BufferEntry::Cached(v)) | Some(BufferEntry::Locked(Some(v))) => {
                assert!(&value == v);
            }
            Some(BufferEntry::Put(v)) => {
                assert!(value.as_ref().map(|entry| &entry.value) == Some(v))
            }
            Some(BufferEntry::Del) => {
                assert!(value.is_none());
            }
            Some(BufferEntry::Insert(v)) => {
                assert!(value.as_ref().map(|entry| &entry.value) == Some(v))
            }
            Some(BufferEntry::CheckNotExist) => {
                assert!(value.is_none());
            }
        }
    }

    fn update_snapshot_cache_entry(&mut self, key: Key, value: Option<ValueEntry>) {
        match self.entry_map.get(&key) {
            None | Some(BufferEntry::Cached(_)) => {
                self.entry_map.insert(key, BufferEntry::Cached(value));
            }
            Some(_) => {}
        }
    }

    fn insert_entry(&mut self, key: impl Into<Key>, entry: BufferEntry) {
        let key = key.into();
        if !matches!(entry, BufferEntry::Cached(_) | BufferEntry::CheckNotExist) {
            self.primary_key.get_or_insert_with(|| key.clone());
        }
        self.entry_map.insert(key, entry);
    }
}

// The state of a key-value pair in the buffer.
// It includes two kinds of state:
//
// Mutations:
//   - `Put`
//   - `Del`
//   - `Insert`
//   - `CheckNotExist`, a constraint to ensure the key doesn't exist. See https://github.com/pingcap/tidb/pull/14968.
// Cache of read requests:
//   - `Cached`, generated by normal read requests
//   - `ReadLockCached`, generated by lock commands (`lock_keys`, `get_for_update`) and optionally read requests
//
#[derive(Debug, Clone)]
enum BufferEntry {
    // The value has been read from the server. None means there is no entry.
    // Also means the entry isn't locked.
    Cached(Option<ValueEntry>),
    // Key is locked.
    //
    // Cached value:
    //   - Outer Option: Whether there is cached value
    //   - Inner Option: Whether the value is empty
    //   - Note: The cache is not what the lock request reads, but what normal read (`get`) requests read.
    //
    // In optimistic transaction:
    //   The key is locked by `lock_keys`.
    //   It means letting the server check for conflicts when committing
    //
    // In pessimistic transaction:
    //   The key is locked by `get_for_update` or `batch_get_for_update`
    Locked(Option<Option<ValueEntry>>),
    // Value has been written.
    Put(Value),
    // Value has been deleted.
    Del,
    // Key should be check not exists before.
    Insert(Value),
    // Key should be check not exists before.
    CheckNotExist,
}

impl BufferEntry {
    fn to_proto_with_key(&self, key: &Key) -> Option<kvrpcpb::Mutation> {
        let mut pb = kvrpcpb::Mutation::default();
        match self {
            BufferEntry::Cached(_) => return None,
            BufferEntry::Put(v) => {
                pb.op = kvrpcpb::Op::Put.into();
                pb.value.clone_from(v);
            }
            BufferEntry::Del => pb.op = kvrpcpb::Op::Del.into(),
            BufferEntry::Locked(_) => pb.op = kvrpcpb::Op::Lock.into(),
            BufferEntry::Insert(v) => {
                pb.op = kvrpcpb::Op::Insert.into();
                pb.value.clone_from(v);
            }
            BufferEntry::CheckNotExist => pb.op = kvrpcpb::Op::CheckNotExists.into(),
        };
        pb.key = key.clone().into();
        Some(pb)
    }

    fn get_value(&self) -> MutationValue {
        match self {
            BufferEntry::Cached(value) => {
                MutationValue::Determined(value.as_ref().map(|entry| entry.value.clone()))
            }
            BufferEntry::Put(value) => MutationValue::Determined(Some(value.clone())),
            BufferEntry::Del => MutationValue::Determined(None),
            BufferEntry::Locked(None) => MutationValue::Undetermined,
            BufferEntry::Locked(Some(value)) => {
                MutationValue::Determined(value.as_ref().map(|entry| entry.value.clone()))
            }
            BufferEntry::Insert(value) => MutationValue::Determined(Some(value.clone())),
            BufferEntry::CheckNotExist => MutationValue::Determined(None),
        }
    }
}

// The state of a value as known by the buffer.
#[derive(Eq, PartialEq, Debug)]
enum MutationValue {
    // The buffer can determine the value.
    Determined(Option<Value>),
    // The buffer cannot determine the value.
    Undetermined,
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use futures::future::ready;

    use super::*;
    use crate::internal_err;

    #[test]
    fn set_and_get_from_buffer() {
        let mut buffer = Buffer::new(false);
        buffer.put(b"key1".to_vec().into(), b"value1".to_vec());
        buffer.put(b"key2".to_vec().into(), b"value2".to_vec());
        assert_eq!(
            block_on(
                buffer.get_or_else(b"key1".to_vec().into(), move |_| ready(Err(internal_err!(
                    ""
                ))))
            )
            .unwrap()
            .unwrap(),
            b"value1".to_vec()
        );

        buffer.delete(b"key2".to_vec().into());
        buffer.put(b"key1".to_vec().into(), b"value".to_vec());
        assert_eq!(
            block_on(buffer.batch_get_or_else(
                vec![b"key2".to_vec().into(), b"key1".to_vec().into()].into_iter(),
                move |_| ready(Ok(vec![])),
            ))
            .unwrap()
            .collect::<Vec<_>>(),
            vec![KvPair(Key::from(b"key1".to_vec()), b"value".to_vec(),),]
        );
    }

    #[test]
    fn insert_and_get_from_buffer() {
        let mut buffer = Buffer::new(false);
        buffer.insert(b"key1".to_vec().into(), b"value1".to_vec());
        buffer.insert(b"key2".to_vec().into(), b"value2".to_vec());
        assert_eq!(
            block_on(
                buffer.get_or_else(b"key1".to_vec().into(), move |_| ready(Err(internal_err!(
                    ""
                ))))
            )
            .unwrap()
            .unwrap(),
            b"value1".to_vec()
        );

        buffer.delete(b"key2".to_vec().into());
        buffer.insert(b"key1".to_vec().into(), b"value".to_vec());
        assert_eq!(
            block_on(buffer.batch_get_or_else(
                vec![b"key2".to_vec().into(), b"key1".to_vec().into()].into_iter(),
                move |_| ready(Ok(vec![])),
            ))
            .unwrap()
            .collect::<Vec<_>>(),
            vec![KvPair(Key::from(b"key1".to_vec()), b"value".to_vec()),]
        );
    }

    #[test]
    fn repeat_reads_are_cached() {
        let k1: Key = b"key1".to_vec().into();
        let k1_ = k1.clone();
        let k2: Key = b"key2".to_vec().into();
        let k2_ = k2.clone();
        let v1: Value = b"value1".to_vec();
        let v1_ = v1.clone();
        let v1__ = v1.clone();
        let v2: Value = b"value2".to_vec();
        let v2_ = v2.clone();

        let mut buffer = Buffer::new(false);
        let r1 = block_on(buffer.get_or_else(k1.clone(), move |_| ready(Ok(Some(v1_)))));
        let r2 = block_on(buffer.get_or_else(k1.clone(), move |_| ready(Err(internal_err!("")))));
        assert_eq!(r1.unwrap().unwrap(), v1);
        assert_eq!(r2.unwrap().unwrap(), v1);

        let mut buffer = Buffer::new(false);
        let r1 = block_on(
            buffer.batch_get_or_else(vec![k1.clone(), k2.clone()].into_iter(), move |_| {
                ready(Ok(vec![(k1_, v1__).into(), (k2_, v2_).into()]))
            }),
        );
        let r2 = block_on(buffer.get_or_else(k2.clone(), move |_| ready(Err(internal_err!("")))));
        let r3 = block_on(
            buffer.batch_get_or_else(vec![k1.clone(), k2.clone()].into_iter(), move |_| {
                ready(Ok(vec![]))
            }),
        );
        assert_eq!(
            r1.unwrap().collect::<Vec<_>>(),
            vec![
                KvPair(k1.clone(), v1.clone()),
                KvPair(k2.clone(), v2.clone())
            ]
        );
        assert_eq!(r2.unwrap().unwrap(), v2);
        assert_eq!(
            r3.unwrap().collect::<Vec<_>>(),
            vec![KvPair(k1, v1), KvPair(k2, v2)]
        );
    }

    #[test]
    fn source_snapshot_cache_observability_counts_values_and_misses() {
        let key: Key = b"missing".to_vec().into();
        let mut buffer = Buffer::new(false);

        let first = block_on(
            buffer.batch_get_or_else(vec![key.clone()].into_iter(), |_| ready(Ok(Vec::new()))),
        )
        .unwrap()
        .collect::<Vec<_>>();
        let second = block_on(buffer.batch_get_or_else(vec![key].into_iter(), |_| {
            ready(Err(internal_err!("cached snapshot miss must avoid fetch")))
        }))
        .unwrap()
        .collect::<Vec<_>>();

        assert!(first.is_empty());
        assert!(second.is_empty());
        assert_eq!(buffer.snapshot_cache_hit_count(), 1);
        assert_eq!(buffer.snapshot_cache_size(), 1);
        buffer.clear_cached_reads();
        assert_eq!(buffer.snapshot_cache_hit_count(), 1);
        assert_eq!(buffer.snapshot_cache_size(), 0);
    }

    #[test]
    fn source_snapshot_cache_mutation_preserves_entries_and_cached_misses() {
        let present: Key = b"present".to_vec().into();
        let missing: Key = b"missing".to_vec().into();
        let mut buffer = Buffer::new(false);
        let values = BTreeMap::from([(present.clone(), ValueEntry::new(b"first".to_vec(), 41))]);

        buffer.update_snapshot_cache(vec![present.clone(), missing.clone()], &values);
        assert_eq!(
            buffer.snapshot_cache(),
            BTreeMap::from([
                (present.clone(), ValueEntry::new(b"first".to_vec(), 41)),
                (missing.clone(), ValueEntry::default()),
            ])
        );

        let replacement =
            BTreeMap::from([(present.clone(), ValueEntry::new(b"second".to_vec(), 42))]);
        buffer.update_snapshot_cache(vec![present.clone()], &replacement);
        assert_eq!(
            buffer.snapshot_cache().get(&present),
            Some(&ValueEntry::new(b"second".to_vec(), 42))
        );
        buffer.clean_snapshot_cache(vec![present]);
        assert_eq!(
            buffer.snapshot_cache(),
            BTreeMap::from([(missing, ValueEntry::default())])
        );
    }

    #[test]
    fn scan_and_fetch_redundant_limit_does_not_overflow() {
        let mut buffer = Buffer::new(false);
        buffer.delete(b"key1".to_vec().into());

        let range: BoundRange = (..).into();
        let res =
            block_on(
                buffer.scan_and_fetch(range, u32::MAX, false, false, |_, redundant_limit| {
                    assert_eq!(redundant_limit, u32::MAX);
                    ready(Ok(Vec::<KvPair>::new()))
                }),
            )
            .unwrap()
            .collect::<Vec<_>>();

        assert!(res.is_empty());
    }

    // Check that multiple writes to the same key combine in the correct way.
    #[test]
    fn state_machine() {
        let mut buffer = Buffer::new(false);

        macro_rules! assert_entry {
            ($key: ident, $p: pat) => {
                assert!(matches!(buffer.entry_map.get(&$key), Some(&$p),))
            };
        }

        macro_rules! assert_entry_none {
            ($key: ident) => {
                assert!(buffer.entry_map.get(&$key).is_none())
            };
        }

        // Insert + Delete = CheckNotExists
        let key: Key = b"key1".to_vec().into();
        buffer.insert(key.clone(), b"value1".to_vec());
        buffer.delete(key.clone());
        assert_entry!(key, BufferEntry::CheckNotExist);

        // CheckNotExists + Delete = CheckNotExists
        buffer.delete(key.clone());
        assert_entry!(key, BufferEntry::CheckNotExist);

        // CheckNotExists + Put = Insert
        buffer.put(key.clone(), b"value2".to_vec());
        assert_entry!(key, BufferEntry::Insert(_));

        // Insert + Put = Insert
        let key: Key = b"key2".to_vec().into();
        buffer.insert(key.clone(), b"value1".to_vec());
        buffer.put(key.clone(), b"value2".to_vec());
        assert_entry!(key, BufferEntry::Insert(_));

        // Delete + Insert = Put
        let key: Key = b"key3".to_vec().into();
        buffer.delete(key.clone());
        buffer.insert(key.clone(), b"value1".to_vec());
        assert_entry!(key, BufferEntry::Put(_));

        // Lock + Unlock = None
        let key: Key = b"key4".to_vec().into();
        buffer.lock(key.clone());
        buffer.unlock(&key);
        assert_entry_none!(key);

        // Cached + Lock + Unlock = Cached
        let key: Key = b"key5".to_vec().into();
        let val: Value = b"value5".to_vec();
        let val_ = val.clone();
        let r = block_on(buffer.get_or_else(key.clone(), move |_| ready(Ok(Some(val_)))));
        assert_eq!(r.unwrap().unwrap(), val);
        buffer.lock(key.clone());
        buffer.unlock(&key);
        assert_entry!(key, BufferEntry::Cached(Some(_)));
        assert_eq!(
            block_on(buffer.get_or_else(key, move |_| ready(Err(internal_err!("")))))
                .unwrap()
                .unwrap(),
            val
        );
    }
}
