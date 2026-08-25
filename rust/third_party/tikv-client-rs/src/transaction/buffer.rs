// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Bound;

use crate::kv::{FlagsOp, KeyFlags};
use crate::proto::kvrpcpb;
use crate::request::{EncodeKeyspace, KeyMode, Keyspace, TruncateKeyspace};
use crate::BoundRange;
use crate::Key;
use crate::KvPair;
use crate::Result;
use crate::Value;
use crate::ValueEntry;

use super::transaction::{Mutation, MutationFlags, MutationOptions};
use super::unionstore::MemDb;

const SNAPSHOT_CACHE_SIZE_LIMIT: i64 = 10 << 30;

fn snapshot_cache_entry_size(key: &Key, value: &Option<ValueEntry>) -> i64 {
    key.len() as i64
        + value
            .as_ref()
            .map_or(std::mem::size_of::<ValueEntry>() as i64, |value| {
                value.size() as i64
            })
}

fn key_in_range(key: &Key, range: &BoundRange) -> bool {
    let after_start = match &range.from {
        Bound::Included(start) => key >= start,
        Bound::Excluded(start) => key > start,
        Bound::Unbounded => true,
    };
    let before_end = match &range.to {
        Bound::Included(end) => key <= end,
        Bound::Excluded(end) => key < end,
        Bound::Unbounded => true,
    };
    after_start && before_end
}

fn lock_operation(flags: KeyFlags) -> kvrpcpb::Op {
    if flags.has_locked_in_share_mode() {
        kvrpcpb::Op::SharedLock
    } else {
        kvrpcpb::Op::Lock
    }
}

fn assertion_from_flags(flags: KeyFlags) -> super::MutationAssertion {
    if flags.has_assert_exist() {
        super::MutationAssertion::Exist
    } else if flags.has_assert_not_exist() {
        super::MutationAssertion::NotExist
    } else if flags.has_assert_unknown() {
        super::MutationAssertion::Unknown
    } else {
        super::MutationAssertion::None
    }
}

fn mutation_flags_from_key_flags(flags: KeyFlags) -> MutationFlags {
    MutationFlags {
        options: MutationOptions::default()
            .assertion(assertion_from_flags(flags))
            .need_constraint_check_in_prewrite(flags.has_need_constraint_check_in_prewrite()),
        pessimistic_locked: flags.has_locked(),
        shared_locked: flags.has_locked_in_share_mode(),
        presume_key_not_exists: flags.has_presume_key_not_exists(),
    }
}

pub(crate) fn proto_mutations_from_memdb(
    memdb: &MemDb,
    keyspace: Keyspace,
    is_pessimistic: bool,
) -> Vec<kvrpcpb::Mutation> {
    let physical_key = |key: &[u8]| {
        Key::from(key.to_vec())
            .encode_keyspace(keyspace, KeyMode::Txn)
            .into()
    };
    let mut mutations = Vec::with_capacity(memdb.len());
    let mut iterator = memdb.iter_with_flags(None, None);
    while iterator.valid() {
        let flags = iterator.flags();
        let mut mutation = kvrpcpb::Mutation {
            key: physical_key(iterator.key()),
            assertion: assertion_from_flags(flags).to_proto() as i32,
            ..Default::default()
        };
        let operation = if !iterator.has_value() {
            flags.has_locked().then(|| lock_operation(flags))
        } else if !iterator.value().is_empty() {
            mutation.value = iterator.value().to_vec();
            Some(if flags.has_presume_key_not_exists() {
                kvrpcpb::Op::Insert
            } else {
                kvrpcpb::Op::Put
            })
        } else if !is_pessimistic && flags.has_presume_key_not_exists() {
            Some(kvrpcpb::Op::CheckNotExists)
        } else if flags.has_newly_inserted() {
            flags.has_locked().then(|| lock_operation(flags))
        } else {
            Some(kvrpcpb::Op::Del)
        };
        if let Some(operation) = operation {
            mutation.op = operation as i32;
            mutations.push(mutation);
        }
        iterator
            .next()
            .expect("owned MemDB iterator advances deterministically");
    }
    mutations
}

/// Returns the first and last logical keys in a flushed MemDB generation.
///
/// Client-go derives the range from its immutable MemDB generation before it
/// builds the flush mutations. Rust's MemDB key domain is logical, so retaining
/// those logical bounds lets the later region lookup encode them exactly once.
pub(crate) fn memdb_key_bounds(memdb: &MemDb) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut iterator = memdb.iter_with_flags(None, None);
    if !iterator.valid() {
        return None;
    }
    let first = iterator.key().to_vec();
    let mut last = first.clone();
    while iterator.valid() {
        last = iterator.key().to_vec();
        iterator
            .next()
            .expect("a stable flushed MemDB iterator remains valid");
    }
    Some((first, last))
}

/// A caching layer which buffers reads and writes in a transaction.
pub struct Buffer {
    /// The transaction's single authoritative mutation store. Keys are kept
    /// logical here, like client-go's MemBuffer; physical keyspace prefixes
    /// are added only when constructing TiKV requests.
    memdb: MemDb,
    keyspace: Keyspace,
    primary_key: Option<Key>,
    /// Snapshot and pessimistic-lock returned-value cache only. Transaction
    /// mutations and all key flags live exclusively in `memdb`.
    entry_map: BTreeMap<Key, BufferEntry>,
    is_pessimistic: bool,
    snapshot_cache_hits: usize,
    snapshot_cache_size_bytes: i64,
}

impl Buffer {
    pub fn new(is_pessimistic: bool) -> Buffer {
        Self::new_with_keyspace(is_pessimistic, Keyspace::Disable)
    }

    pub(crate) fn new_with_keyspace(is_pessimistic: bool, keyspace: Keyspace) -> Buffer {
        Buffer {
            memdb: MemDb::new(),
            keyspace,
            primary_key: None,
            entry_map: BTreeMap::new(),
            is_pessimistic,
            snapshot_cache_hits: 0,
            snapshot_cache_size_bytes: 0,
        }
    }

    pub(crate) fn set_pessimistic(&mut self, pessimistic: bool) {
        self.is_pessimistic = pessimistic;
    }

    /// Get the primary key of the buffer.
    pub fn get_primary_key(&self) -> Option<Key> {
        self.primary_key.clone().or_else(|| {
            self.memdb
                .managed_pipelined_metadata()
                .primary_key
                .map(Key::from)
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.memdb.len()
    }

    pub(crate) fn memory_footprint(&self) -> u64 {
        self.memdb.memory_footprint()
    }

    /// Returns the exact MemDB committed by this transaction. Its public key
    /// surface is logical for both API V1 and V2, matching client-go.
    pub(crate) fn mem_buffer(&mut self) -> &mut MemDb {
        &mut self.memdb
    }

    pub(crate) fn memdb_memory_hook_is_set(&self) -> bool {
        self.memdb.memory_hook_is_set()
    }

    /// Discard values cached by snapshot reads while retaining transaction mutations.
    pub(crate) fn clear_cached_reads(&mut self) {
        self.entry_map
            .retain(|_, entry| !matches!(entry, BufferEntry::Cached(_)));
        // SetSnapshotTS drops the source cache map but deliberately leaves its
        // cachedSize accounting untouched, just as it preserves hitCnt.
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
        self.update_snapshot_cache_with_limit(keys, values, SNAPSHOT_CACHE_SIZE_LIMIT);
    }

    fn update_snapshot_cache_with_limit(
        &mut self,
        keys: impl IntoIterator<Item = Key>,
        values: &BTreeMap<Key, ValueEntry>,
        size_limit: i64,
    ) {
        for key in keys {
            let value = values.get(&key).cloned();
            if !matches!(self.memdb_value(&key), MutationValue::Undetermined) {
                continue;
            }
            match self.entry_map.get(&key) {
                None | Some(BufferEntry::Cached(_)) => {
                    self.update_source_snapshot_cache_entry(key, value);
                }
                // Snapshot instances are read-only, so source cache mutation
                // never meets a write/lock entry. Preserve a transaction's
                // higher-priority local state if this internal buffer is
                // reused outside that facade.
                Some(_) => {}
            }
        }
        self.evict_unneeded_snapshot_cache(values, size_limit);
    }

    pub(crate) fn clean_snapshot_cache(&mut self, keys: impl IntoIterator<Item = Key>) {
        for key in keys {
            // Preserve client-go's probe-visible accounting exactly: CleanCache
            // charges the key even when it is absent, and an existing entry
            // additionally charges only its ValueEntry size.
            self.snapshot_cache_size_bytes -= key.len() as i64;
            if let Some(BufferEntry::Cached(value)) = self.entry_map.remove(&key) {
                self.snapshot_cache_size_bytes -= value
                    .as_ref()
                    .map_or(std::mem::size_of::<ValueEntry>() as i64, |value| {
                        value.size() as i64
                    });
            }
        }
    }

    /// Set the primary key if it is not set
    pub fn primary_key_or(&mut self, key: &Key) {
        self.primary_key.get_or_insert_with(|| key.clone());
    }

    fn logical_key(&self, key: &Key) -> Vec<u8> {
        let key = key.clone().truncate_keyspace(self.keyspace);
        <&[u8]>::from(&key).to_vec()
    }

    fn physical_key(&self, key: &[u8]) -> Key {
        Key::from(key.to_vec()).encode_keyspace(self.keyspace, KeyMode::Txn)
    }

    fn memdb_value(&self, key: &Key) -> MutationValue {
        match self.memdb.get_readonly(&self.logical_key(key)) {
            Ok(value) if value.is_empty() => MutationValue::Determined(None),
            Ok(value) => MutationValue::Determined(Some(value)),
            Err(_) => MutationValue::Undetermined,
        }
    }

    /// Return the source PipelinedMemDB read precedence as a tri-state:
    /// `None` means the remote buffer tier is still required, while
    /// `Some(None)` is a buffered delete or cached remote miss.
    pub(crate) fn pipelined_value(&self, key: &Key) -> Option<Option<Value>> {
        match self.memdb_value(key) {
            MutationValue::Determined(value) => Some(value),
            MutationValue::Undetermined => self
                .memdb
                .managed_batch_get_cached_value(&self.logical_key(key)),
        }
    }

    pub(crate) fn cache_pipelined_batch_get(
        &mut self,
        keys: impl IntoIterator<Item = Key>,
        values: &BTreeMap<Key, Value>,
    ) {
        let keys = keys
            .into_iter()
            .map(|key| self.logical_key(&key))
            .collect::<Vec<_>>();
        let values = values
            .iter()
            .map(|(key, value)| (self.logical_key(key), value.clone()))
            .collect::<BTreeMap<_, _>>();
        self.memdb.cache_managed_batch_get(keys, &values);
    }

    fn memdb_flags(&self, key: &Key) -> KeyFlags {
        self.memdb
            .get_flags_readonly(&self.logical_key(key))
            .unwrap_or_default()
    }

    fn memdb_has_flag(&self, predicate: impl Fn(KeyFlags) -> bool) -> bool {
        let mut iterator = self.memdb.iter_with_flags(None, None);
        while iterator.valid() {
            if predicate(iterator.flags()) {
                return true;
            }
            iterator
                .next()
                .expect("owned MemDB iterator advances deterministically");
        }
        false
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
        match self.memdb_value(&key) {
            MutationValue::Determined(value) => Ok(value),
            MutationValue::Undetermined => {
                if let Some(entry) = self.entry_map.get(&key) {
                    if matches!(entry, BufferEntry::Cached(_)) {
                        self.snapshot_cache_hits += 1;
                    }
                    if let MutationValue::Determined(value) = entry.get_value() {
                        return Ok(value);
                    }
                }
                let value = f(key.clone()).await?;
                if cache_result {
                    self.update_point_snapshot_cache_entry(
                        key,
                        value.clone().map(|value| ValueEntry::new(value, 0)),
                    );
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
        if let MutationValue::Determined(value) = self.memdb_value(&key) {
            return Ok(value.map(|value| ValueEntry::new(value, 0)));
        }

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
                self.update_point_snapshot_cache_entry(key, value.clone());
            }
            return Ok(value);
        }

        match self
            .entry_map
            .get(&key)
            .map(BufferEntry::get_value)
            .unwrap_or(MutationValue::Undetermined)
        {
            MutationValue::Determined(value) => Ok(value.map(|value| ValueEntry::new(value, 0))),
            MutationValue::Undetermined => {
                let value = f(key.clone()).await?;
                if cache_result {
                    self.update_point_snapshot_cache_entry(key, value.clone());
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
            match self.memdb_value(&key) {
                MutationValue::Determined(Some(value)) => {
                    cached_results.push(KvPair(key, value));
                }
                MutationValue::Determined(None) => {}
                MutationValue::Undetermined => match self.entry_map.get(&key) {
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
                },
            }
        }

        let fetched_results = if undetermined_keys.is_empty() {
            Vec::new()
        } else {
            f(Box::new(undetermined_keys.clone().into_iter())).await?
        };
        if cache_results {
            let fetched_by_key = fetched_results
                .iter()
                .map(|pair| (pair.0.clone(), ValueEntry::new(pair.1.clone(), 0)))
                .collect::<BTreeMap<_, _>>();
            self.update_snapshot_cache(undetermined_keys, &fetched_by_key);
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
            match self.memdb_value(&key) {
                MutationValue::Determined(Some(value)) => {
                    cached_results.insert(key, ValueEntry::new(value, 0));
                }
                MutationValue::Determined(None) => {}
                MutationValue::Undetermined => match self.entry_map.get(&key) {
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
                },
            }
        }

        let fetched_results = if undetermined_keys.is_empty() {
            BTreeMap::new()
        } else {
            f(Box::new(undetermined_keys.clone().into_iter())).await?
        };
        if cache_results {
            self.update_snapshot_cache(undetermined_keys, &fetched_results);
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
        if self.memdb.is_managed_pipelined() {
            return Err(crate::Error::StringError(
                "pipelined memdb does not support Iter".to_owned(),
            ));
        }
        // Collect the authoritative MemDB view before awaiting the remote
        // fetch. The owned ART iterator includes tombstones and direct writes
        // made through Transaction::get_mem_buffer().
        let mut local_mutations = Vec::new();
        let mut iterator = self.memdb.iter_with_flags(None, None);
        while iterator.valid() {
            if iterator.has_value() {
                let key = self.physical_key(iterator.key());
                if key_in_range(&key, &range) {
                    local_mutations.push((key, iterator.value().to_vec()));
                }
            }
            iterator
                .next()
                .expect("owned MemDB iterator advances deterministically");
        }

        // fetch from TiKV
        // fetch more entries because some of them may be deleted.
        let deleted_count = u32::try_from(
            local_mutations
                .iter()
                .filter(|(_, value)| value.is_empty())
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
        for (key, value) in local_mutations {
            if value.is_empty() {
                results.remove(&key);
            } else {
                results.insert(key, value);
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
        self.lock_with_returned_value(key, false, None)
            .expect("exclusive lock cannot violate lock-mode invariants");
    }

    pub(crate) fn lock_with_returned_value(
        &mut self,
        key: Key,
        shared: bool,
        returned: Option<&crate::ReturnedValue>,
    ) -> std::result::Result<(), &'static str> {
        let returned_exists = returned.map(|value| value.exists);
        let returned = returned.map(|value| {
            value
                .exists
                .then(|| ValueEntry::new(value.value.clone(), 0))
        });
        if !shared {
            self.primary_key.get_or_insert_with(|| key.clone());
        } else if self.primary_key.is_none() {
            return Err("pessimistic lock in share mode requires primary key to be selected");
        }
        let current_flags = self.memdb_flags(&key);
        if current_flags.has_locked_in_share_mode() && !shared {
            return Err("upgrading a shared lock to an exclusive lock is not supported");
        }
        let effective_shared =
            shared && (!current_flags.has_locked() || current_flags.has_locked_in_share_mode());
        let cache_key = key.clone();
        let entry = self.entry_map.entry(key);
        match entry {
            Entry::Vacant(entry) => {
                if effective_shared {
                    entry.insert(BufferEntry::SharedLocked(returned));
                } else {
                    entry.insert(BufferEntry::Locked(returned));
                }
            }
            Entry::Occupied(mut entry) => match entry.get_mut() {
                BufferEntry::Cached(value) => {
                    self.snapshot_cache_size_bytes = self
                        .snapshot_cache_size_bytes
                        .saturating_sub(snapshot_cache_entry_size(&cache_key, value));
                    let value = returned.or_else(|| Some(value.take()));
                    if effective_shared {
                        entry.insert(BufferEntry::SharedLocked(value));
                    } else {
                        entry.insert(BufferEntry::Locked(value));
                    }
                }
                BufferEntry::SharedLocked(value) if !effective_shared => {
                    let value = value.take();
                    entry.insert(BufferEntry::Locked(value));
                }
                BufferEntry::Locked(value) if effective_shared => {
                    let value = value.take();
                    entry.insert(BufferEntry::SharedLocked(value));
                }
                BufferEntry::Locked(_) | BufferEntry::SharedLocked(_) => {}
            },
        }
        let logical_key = self.logical_key(&cache_key);
        let value_exists = returned_exists.unwrap_or(true);
        let value_flag = if value_exists {
            FlagsOp::SetKeyLockedValueExists
        } else {
            FlagsOp::SetKeyLockedValueNotExists
        };
        let mode_flag = if effective_shared {
            FlagsOp::SetKeyLockedInShareMode
        } else {
            FlagsOp::SetKeyLockedInExclusiveMode
        };
        self.memdb.update_flags(
            &logical_key,
            &[
                FlagsOp::SetKeyLocked,
                FlagsOp::DelNeedCheckExists,
                value_flag,
                mode_flag,
            ],
        );
        Ok(())
    }

    /// Mark a pessimistically acquired shared lock. Shared locks never select
    /// the primary key; an exclusive primary must already exist.
    #[cfg(test)]
    pub(crate) fn lock_shared(&mut self, key: Key) -> std::result::Result<(), &'static str> {
        self.lock_with_returned_value(key, true, None)
    }

    pub(crate) fn has_shared_locks(&self) -> bool {
        self.memdb_has_flag(KeyFlags::has_locked_in_share_mode)
    }

    pub(crate) fn is_shared_locked(&self, key: &Key) -> bool {
        self.memdb_flags(key).has_locked_in_share_mode()
    }

    pub(crate) fn is_locked(&self, key: &Key) -> bool {
        self.memdb_flags(key).has_locked()
    }

    pub(crate) fn pessimistic_lock_keys(&self) -> BTreeSet<Vec<u8>> {
        let mut keys = BTreeSet::new();
        let mut iterator = self.memdb.iter_with_flags(None, None);
        while iterator.valid() {
            if iterator.flags().has_locked() {
                keys.insert(<Key as Into<Vec<u8>>>::into(
                    self.physical_key(iterator.key()),
                ));
            }
            iterator
                .next()
                .expect("owned MemDB iterator advances deterministically");
        }
        keys
    }

    /// Unlock the given key if locked.
    pub fn unlock(&mut self, key: &Key) {
        let logical_key = self.logical_key(key);
        let preserve_constraint = self
            .memdb
            .get_flags_readonly(&logical_key)
            .is_ok_and(KeyFlags::has_need_constraint_check_in_prewrite);
        let mut operations = vec![
            FlagsOp::DelKeyLocked,
            FlagsOp::SetKeyLockedInExclusiveMode,
            FlagsOp::SetKeyLockedValueNotExists,
        ];
        if preserve_constraint {
            operations.push(FlagsOp::SetNeedConstraintCheckInPrewrite);
        }
        self.memdb.update_flags(&logical_key, &operations);
        let remove_flags_only = self
            .memdb
            .get_flags_readonly(&logical_key)
            .is_ok_and(|flags| flags.bits() == 0)
            && self.memdb.get_readonly(&logical_key).is_err();
        if remove_flags_only {
            self.memdb.remove_from_buffer(&logical_key);
        }
        if let Some(value) = self.entry_map.get_mut(key) {
            if let BufferEntry::Locked(v) | BufferEntry::SharedLocked(v) = value {
                if let Some(v) = v {
                    let cached = v.take();
                    self.snapshot_cache_size_bytes = self
                        .snapshot_cache_size_bytes
                        .saturating_add(snapshot_cache_entry_size(key, &cached));
                    *value = BufferEntry::Cached(cached);
                } else {
                    self.entry_map.remove(key);
                }
            }
        }
    }

    /// Put a value into the buffer (does not write through).
    pub fn put(&mut self, key: Key, value: Value) -> Result<()> {
        let logical_key = self.logical_key(&key);
        self.remove_cached_entry(&key);
        self.primary_key_or(&key);
        self.memdb
            .set(&logical_key, &value)
            .map_err(|error| crate::Error::StringError(error.to_string()))
    }

    /// Mark a value as Insert mutation into the buffer (does not write through).
    pub fn insert(&mut self, key: Key, value: Value) -> Result<()> {
        let logical_key = self.logical_key(&key);
        let was_plain_tombstone = self
            .memdb
            .get_readonly(&logical_key)
            .is_ok_and(|value| value.is_empty())
            && self
                .memdb
                .get_flags_readonly(&logical_key)
                .is_ok_and(|flags| !flags.has_presume_key_not_exists());
        self.remove_cached_entry(&key);
        self.primary_key_or(&key);
        let result = if was_plain_tombstone {
            self.memdb.set(&logical_key, &value)
        } else {
            self.memdb
                .set_with_flags(&logical_key, &value, &[FlagsOp::SetPresumeKeyNotExists])
        };
        result.map_err(|error| crate::Error::StringError(error.to_string()))
    }

    /// Mark a value as deleted.
    pub fn delete(&mut self, key: Key) -> Result<()> {
        let logical_key = self.logical_key(&key);
        self.remove_cached_entry(&key);
        self.primary_key_or(&key);
        self.memdb
            .delete(&logical_key)
            .map_err(|error| crate::Error::StringError(error.to_string()))
    }

    pub(crate) fn mutate(&mut self, m: Mutation) -> Result<()> {
        match m {
            Mutation::Put(key, value) => self.put(key, value),
            Mutation::Delete(key) => self.delete(key),
        }
    }

    /// Converts the buffered mutations to the proto buffer version
    pub fn to_proto_mutations(&self) -> Vec<kvrpcpb::Mutation> {
        proto_mutations_from_memdb(&self.memdb, self.keyspace, self.is_pessimistic)
    }

    /// Returns every value-bearing MemDB entry in source iteration order for
    /// `KvFilter`. This deliberately includes tombstones that mutation
    /// lowering may later omit, matching client-go's `initKeysAndMutations`.
    pub(crate) fn value_entries_for_filter(&self) -> Vec<(Vec<u8>, Value, MutationFlags)> {
        let mut entries = Vec::with_capacity(self.memdb.len());
        let mut iterator = self.memdb.iter_with_flags(None, None);
        while iterator.valid() {
            if iterator.has_value() {
                entries.push((
                    iterator.key().to_vec(),
                    iterator.value().to_vec(),
                    mutation_flags_from_key_flags(iterator.flags()),
                ));
            }
            iterator
                .next()
                .expect("owned MemDB iterator advances deterministically");
        }
        entries
    }

    pub(crate) fn set_mutation_options(
        &mut self,
        key: &Key,
        options: MutationOptions,
    ) -> Result<()> {
        let logical_key = self.logical_key(key);
        if self.memdb.get_flags_readonly(&logical_key).is_err() {
            return Err(crate::Error::StringError(
                "mutation options require an existing buffered mutation".to_owned(),
            ));
        }
        let assertion = match options.mutation_assertion() {
            super::MutationAssertion::None => FlagsOp::SetAssertNone,
            super::MutationAssertion::Exist => FlagsOp::SetAssertExist,
            super::MutationAssertion::NotExist => FlagsOp::SetAssertNotExist,
            super::MutationAssertion::Unknown => FlagsOp::SetAssertUnknown,
        };
        let constraint = if options.needs_constraint_check_in_prewrite() {
            FlagsOp::SetNeedConstraintCheckInPrewrite
        } else {
            FlagsOp::DelNeedConstraintCheckInPrewrite
        };
        self.memdb
            .update_flags(&logical_key, &[assertion, constraint]);
        Ok(())
    }

    pub(crate) fn mutation_flags(&self, key: &Key) -> MutationFlags {
        mutation_flags_from_key_flags(self.memdb_flags(key))
    }

    pub(crate) fn constraint_check_keys(&self) -> BTreeSet<Vec<u8>> {
        let mut keys = BTreeSet::new();
        let mut iterator = self.memdb.iter_with_flags(None, None);
        while iterator.valid() {
            if iterator.flags().has_need_constraint_check_in_prewrite() {
                keys.insert(self.physical_key(iterator.key()).into());
            }
            iterator
                .next()
                .expect("owned MemDB iterator advances deterministically");
        }
        keys
    }

    pub(crate) fn assertion_failure(
        &self,
        key: &Key,
        start_timestamp: u64,
    ) -> Option<kvrpcpb::AssertionFailed> {
        let flags = self.memdb_flags(key);
        if !flags.has_locked() {
            return None;
        }
        let exists = flags.has_locked_value_exists();
        let assertion = assertion_from_flags(flags);
        let failed = match assertion {
            super::MutationAssertion::Exist => !exists,
            super::MutationAssertion::NotExist => exists,
            super::MutationAssertion::None | super::MutationAssertion::Unknown => false,
        };
        failed.then(|| kvrpcpb::AssertionFailed {
            start_ts: start_timestamp,
            key: <&[u8]>::from(key).to_vec(),
            assertion: assertion.to_proto() as i32,
            ..Default::default()
        })
    }

    pub fn get_write_size(&self) -> usize {
        self.memdb.size()
    }

    fn get_from_mutations(&self, key: &Key) -> MutationValue {
        match self.memdb_value(key) {
            MutationValue::Undetermined => self
                .entry_map
                .get(key)
                .map(BufferEntry::get_value)
                .unwrap_or(MutationValue::Undetermined),
            determined => determined,
        }
    }

    fn update_cache(&mut self, key: Key, value: Option<Value>) {
        self.update_cache_entry(key, value.map(|value| ValueEntry::new(value, 0)));
    }

    fn update_cache_entry(&mut self, key: Key, value: Option<ValueEntry>) {
        if !matches!(self.memdb_value(&key), MutationValue::Undetermined) {
            return;
        }
        match self.entry_map.get(&key) {
            Some(BufferEntry::Locked(None)) => {
                self.entry_map.insert(key, BufferEntry::Locked(Some(value)));
            }
            Some(BufferEntry::SharedLocked(None)) => {
                self.entry_map
                    .insert(key, BufferEntry::SharedLocked(Some(value)));
            }
            None => {
                self.replace_cached_entry(key, value);
            }
            Some(BufferEntry::Cached(v))
            | Some(BufferEntry::Locked(Some(v)))
            | Some(BufferEntry::SharedLocked(Some(v))) => {
                assert!(&value == v);
            }
        }
    }

    fn update_point_snapshot_cache_entry(&mut self, key: Key, value: Option<ValueEntry>) {
        self.update_point_snapshot_cache_entry_with_limit(key, value, SNAPSHOT_CACHE_SIZE_LIMIT);
    }

    fn update_point_snapshot_cache_entry_with_limit(
        &mut self,
        key: Key,
        value: Option<ValueEntry>,
        size_limit: i64,
    ) {
        if !matches!(self.memdb_value(&key), MutationValue::Undetermined) {
            return;
        }
        match self.entry_map.get(&key) {
            None | Some(BufferEntry::Cached(_)) => {
                let needed = BTreeMap::from([(key.clone(), value.clone().unwrap_or_default())]);
                self.update_source_snapshot_cache_entry(key, value);
                self.evict_unneeded_snapshot_cache(&needed, size_limit);
            }
            Some(_) => {}
        }
    }

    fn replace_cached_entry(&mut self, key: Key, value: Option<ValueEntry>) {
        if let Some(BufferEntry::Cached(old)) = self.entry_map.get(&key) {
            self.snapshot_cache_size_bytes = self
                .snapshot_cache_size_bytes
                .saturating_sub(snapshot_cache_entry_size(&key, old));
        }
        self.snapshot_cache_size_bytes = self
            .snapshot_cache_size_bytes
            .saturating_add(snapshot_cache_entry_size(&key, &value));
        self.entry_map.insert(key, BufferEntry::Cached(value));
    }

    fn update_source_snapshot_cache_entry(&mut self, key: Key, value: Option<ValueEntry>) {
        // UpdateSnapshotCache adds the key and new ValueEntry, then subtracts
        // only the old ValueEntry on replacement. This intentionally retains
        // client-go's cumulative key-byte accounting.
        self.snapshot_cache_size_bytes += snapshot_cache_entry_size(&key, &value);
        if let Some(BufferEntry::Cached(old)) = self.entry_map.get(&key) {
            self.snapshot_cache_size_bytes -= old
                .as_ref()
                .map_or(std::mem::size_of::<ValueEntry>() as i64, |value| {
                    value.size() as i64
                });
        }
        self.entry_map.insert(key, BufferEntry::Cached(value));
    }

    fn remove_cached_entry(&mut self, key: &Key) {
        if let Some(BufferEntry::Cached(value)) = self.entry_map.remove(key) {
            self.snapshot_cache_size_bytes = self
                .snapshot_cache_size_bytes
                .saturating_sub(snapshot_cache_entry_size(key, &value));
        }
    }

    fn evict_unneeded_snapshot_cache(
        &mut self,
        needed: &BTreeMap<Key, ValueEntry>,
        size_limit: i64,
    ) {
        if self.snapshot_cache_size_bytes < size_limit {
            return;
        }
        let candidates = self
            .entry_map
            .iter()
            .filter(|(key, entry)| {
                matches!(entry, BufferEntry::Cached(_)) && !needed.contains_key(*key)
            })
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in candidates {
            self.remove_cached_entry(&key);
            if self.snapshot_cache_size_bytes < size_limit {
                break;
            }
        }
    }

    fn insert_entry(&mut self, key: impl Into<Key>, entry: BufferEntry) {
        let key = key.into();
        if matches!(self.entry_map.get(&key), Some(BufferEntry::Cached(_))) {
            self.remove_cached_entry(&key);
        }
        self.entry_map.insert(key, entry);
    }
}

// The state of a key-value pair in the buffer.
// It includes two kinds of state:
//
// This map contains only the cache of read requests. Mutation values and
// flags live in the authoritative MemDB above.
//
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
    // A pessimistic shared lock. It is prewritten as `SharedLock` and cannot
    // become the transaction primary.
    SharedLocked(Option<Option<ValueEntry>>),
}

impl BufferEntry {
    fn get_value(&self) -> MutationValue {
        match self {
            BufferEntry::Cached(value) => {
                MutationValue::Determined(value.as_ref().map(|entry| entry.value.clone()))
            }
            BufferEntry::Locked(None) => MutationValue::Undetermined,
            BufferEntry::Locked(Some(value)) => {
                MutationValue::Determined(value.as_ref().map(|entry| entry.value.clone()))
            }
            BufferEntry::SharedLocked(None) => MutationValue::Undetermined,
            BufferEntry::SharedLocked(Some(value)) => {
                MutationValue::Determined(value.as_ref().map(|entry| entry.value.clone()))
            }
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
        buffer
            .put(b"key1".to_vec().into(), b"value1".to_vec())
            .unwrap();
        buffer
            .put(b"key2".to_vec().into(), b"value2".to_vec())
            .unwrap();
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

        buffer.delete(b"key2".to_vec().into()).unwrap();
        buffer
            .put(b"key1".to_vec().into(), b"value".to_vec())
            .unwrap();
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
        buffer
            .insert(b"key1".to_vec().into(), b"value1".to_vec())
            .unwrap();
        buffer
            .insert(b"key2".to_vec().into(), b"value2".to_vec())
            .unwrap();
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

        buffer.delete(b"key2".to_vec().into()).unwrap();
        buffer
            .insert(b"key1".to_vec().into(), b"value".to_vec())
            .unwrap();
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
    fn ordinary_write_preserves_assertion_and_clears_constraint_check() {
        let key = Key::from(b"key".to_vec());
        let mut buffer = Buffer::new(false);
        buffer.put(key.clone(), b"value1".to_vec()).unwrap();
        buffer
            .set_mutation_options(
                &key,
                MutationOptions::default()
                    .assertion(super::super::MutationAssertion::Exist)
                    .need_constraint_check_in_prewrite(true),
            )
            .unwrap();

        buffer.put(key.clone(), b"value2".to_vec()).unwrap();

        let flags = buffer.mutation_flags(&key);
        assert_eq!(flags.assertion(), super::super::MutationAssertion::Exist);
        assert!(!flags.needs_constraint_check_in_prewrite());
        assert_eq!(
            buffer.to_proto_mutations()[0].assertion,
            kvrpcpb::Assertion::Exist as i32
        );
    }

    #[test]
    fn local_unlock_clears_only_lock_metadata() {
        let key = Key::from(b"key".to_vec());
        let mut buffer = Buffer::new(true);
        buffer.put(key.clone(), b"value".to_vec()).unwrap();
        buffer.lock(key.clone());
        buffer
            .set_mutation_options(
                &key,
                MutationOptions::default()
                    .assertion(super::super::MutationAssertion::Exist)
                    .need_constraint_check_in_prewrite(true),
            )
            .unwrap();

        buffer.unlock(&key);

        let flags = buffer.mutation_flags(&key);
        assert!(!flags.is_pessimistic_locked());
        assert!(flags.needs_constraint_check_in_prewrite());
        assert_eq!(flags.assertion(), super::super::MutationAssertion::Exist);
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
    fn source_buffer_batch_getter_local_precedence_delete_and_commit_ts() {
        let keys = ["a", "b", "c", "d", "e"]
            .into_iter()
            .map(|key| Key::from(key.as_bytes().to_vec()))
            .collect::<Vec<_>>();
        let mut buffer = Buffer::new(false);
        buffer.insert(keys[0].clone(), b"a1".to_vec()).unwrap();
        buffer.delete(keys[1].clone()).unwrap();

        let fetched = block_on(buffer.batch_get_snapshot_entries_or_else(
            keys.clone().into_iter(),
            true,
            true,
            |missing| {
                assert_eq!(missing.collect::<Vec<_>>(), keys[2..].to_vec());
                ready(Ok(BTreeMap::from([
                    (keys[2].clone(), ValueEntry::new(b"c".to_vec(), 3)),
                    (keys[3].clone(), ValueEntry::new(b"d".to_vec(), 4)),
                ])))
            },
        ))
        .unwrap();
        assert_eq!(
            fetched,
            BTreeMap::from([
                (keys[0].clone(), ValueEntry::new(b"a1".to_vec(), 0)),
                (keys[2].clone(), ValueEntry::new(b"c".to_vec(), 3)),
                (keys[3].clone(), ValueEntry::new(b"d".to_vec(), 4)),
            ])
        );

        let without_commit_ts = block_on(buffer.batch_get_snapshot_entries_or_else(
            keys.clone().into_iter(),
            false,
            true,
            |_| {
                ready(Err(internal_err!(
                    "all snapshot entries and misses are cached"
                )))
            },
        ))
        .unwrap();
        assert_eq!(
            without_commit_ts,
            BTreeMap::from([
                (keys[0].clone(), ValueEntry::new(b"a1".to_vec(), 0)),
                (keys[2].clone(), ValueEntry::new(b"c".to_vec(), 0)),
                (keys[3].clone(), ValueEntry::new(b"d".to_vec(), 0)),
            ])
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
        let accounted_size = buffer.snapshot_cache_size_bytes;
        buffer.clear_cached_reads();
        assert_eq!(buffer.snapshot_cache_hit_count(), 1);
        assert_eq!(buffer.snapshot_cache_size(), 0);
        assert_eq!(buffer.snapshot_cache_size_bytes, accounted_size);
    }

    #[test]
    fn source_len_size_and_memory_exclude_snapshot_cache_but_include_flag_only_entries() {
        let cached: Key = b"cached".to_vec().into();
        let missing: Key = b"missing".to_vec().into();
        let mut buffer = Buffer::new(true);
        buffer.update_snapshot_cache(
            vec![cached.clone(), missing],
            &BTreeMap::from([(cached, ValueEntry::new(b"snapshot".to_vec(), 7))]),
        );
        assert_eq!(buffer.len(), 0);
        assert_eq!(buffer.get_write_size(), 0);
        assert_eq!(buffer.memory_footprint(), 0);

        buffer.lock(b"lock".to_vec().into());
        buffer
            .insert(b"check".to_vec().into(), b"value".to_vec())
            .unwrap();
        buffer.delete(b"check".to_vec().into()).unwrap();
        buffer
            .put(b"put".to_vec().into(), b"value".to_vec())
            .unwrap();

        assert_eq!(buffer.len(), 3);
        assert_eq!(buffer.get_write_size(), 4 + 5 + 3 + 5);
        // MemDB retains the overwritten insert value in its value-log
        // history, so memory can exceed the current logical write size.
        assert_eq!(buffer.memory_footprint(), 4 + 5 + 3 + 5 + 5);
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
    fn source_snapshot_cache_limit_evicts_only_entries_unneeded_by_current_fill() {
        let old: Key = b"old".to_vec().into();
        let current: Key = b"current".to_vec().into();
        let old_value = ValueEntry::new(b"old-value".to_vec(), 1);
        let current_value = ValueEntry::new(b"current-value".to_vec(), 2);
        let mut buffer = Buffer::new(false);
        buffer.update_snapshot_cache_with_limit(
            vec![old.clone()],
            &BTreeMap::from([(old.clone(), old_value)]),
            i64::MAX,
        );
        let current_size = snapshot_cache_entry_size(&current, &Some(current_value.clone()));

        buffer.update_snapshot_cache_with_limit(
            vec![current.clone()],
            &BTreeMap::from([(current.clone(), current_value.clone())]),
            current_size + 1,
        );

        assert_eq!(
            buffer.snapshot_cache(),
            BTreeMap::from([(current, current_value)])
        );
        assert_eq!(buffer.snapshot_cache_size_bytes, current_size);
    }

    #[test]
    fn source_point_get_cache_limit_preserves_the_current_missing_key() {
        let old: Key = b"old".to_vec().into();
        let current: Key = b"current-missing".to_vec().into();
        let mut buffer = Buffer::new(false);
        buffer.update_point_snapshot_cache_entry_with_limit(
            old.clone(),
            Some(ValueEntry::new(b"old-value".to_vec(), 1)),
            i64::MAX,
        );
        let current_size = snapshot_cache_entry_size(&current, &None);

        buffer.update_point_snapshot_cache_entry_with_limit(
            current.clone(),
            None,
            current_size + 1,
        );

        assert_eq!(
            buffer.snapshot_cache(),
            BTreeMap::from([(current, ValueEntry::default())])
        );
        assert_eq!(buffer.snapshot_cache_size_bytes, current_size);
    }

    #[test]
    fn source_snapshot_cache_replacement_and_clean_keep_go_accounting() {
        let present: Key = b"present".to_vec().into();
        let absent: Key = b"absent".to_vec().into();
        let first = Some(ValueEntry::new(b"first".to_vec(), 1));
        let second = Some(ValueEntry::new(b"second".to_vec(), 2));
        let mut buffer = Buffer::new(false);

        buffer.update_point_snapshot_cache_entry_with_limit(
            present.clone(),
            first.clone(),
            i64::MAX,
        );
        buffer.update_point_snapshot_cache_entry_with_limit(
            present.clone(),
            second.clone(),
            i64::MAX,
        );

        // UpdateSnapshotCache subtracts only the old value on replacement,
        // so the key bytes remain cumulatively charged.
        assert_eq!(
            buffer.snapshot_cache_size_bytes,
            snapshot_cache_entry_size(&present, &second) + present.len() as i64
        );

        buffer.clean_snapshot_cache(vec![present.clone(), absent.clone()]);
        assert_eq!(
            buffer.snapshot_cache_size_bytes,
            present.len() as i64 - absent.len() as i64
        );
        assert!(buffer.snapshot_cache().is_empty());
    }

    #[test]
    fn scan_and_fetch_redundant_limit_does_not_overflow() {
        let mut buffer = Buffer::new(false);
        buffer.delete(b"key1".to_vec().into()).unwrap();

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

        macro_rules! assert_operation {
            ($key: ident, $operation: expr) => {
                assert_eq!(
                    buffer
                        .to_proto_mutations()
                        .into_iter()
                        .find(|mutation| mutation.key == <&[u8]>::from(&$key))
                        .and_then(|mutation| kvrpcpb::Op::try_from(mutation.op).ok()),
                    Some($operation),
                )
            };
        }

        macro_rules! assert_entry_none {
            ($key: ident) => {
                assert!(buffer.entry_map.get(&$key).is_none())
            };
        }

        // Insert + Delete = CheckNotExists
        let key: Key = b"key1".to_vec().into();
        buffer.insert(key.clone(), b"value1".to_vec()).unwrap();
        buffer.delete(key.clone()).unwrap();
        assert_operation!(key, kvrpcpb::Op::CheckNotExists);

        // CheckNotExists + Delete = CheckNotExists
        buffer.delete(key.clone()).unwrap();
        assert_operation!(key, kvrpcpb::Op::CheckNotExists);

        // CheckNotExists + Put = Insert
        buffer.put(key.clone(), b"value2".to_vec()).unwrap();
        assert_operation!(key, kvrpcpb::Op::Insert);

        // Insert + Put = Insert
        let key: Key = b"key2".to_vec().into();
        buffer.insert(key.clone(), b"value1".to_vec()).unwrap();
        buffer.put(key.clone(), b"value2".to_vec()).unwrap();
        assert_operation!(key, kvrpcpb::Op::Insert);

        // Delete + Insert = Put
        let key: Key = b"key3".to_vec().into();
        buffer.delete(key.clone()).unwrap();
        buffer.insert(key.clone(), b"value1".to_vec()).unwrap();
        assert_operation!(key, kvrpcpb::Op::Put);

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
        assert!(matches!(
            buffer.entry_map.get(&key),
            Some(BufferEntry::Cached(Some(_)))
        ));
        assert_eq!(
            block_on(buffer.get_or_else(key, move |_| ready(Err(internal_err!("")))))
                .unwrap()
                .unwrap(),
            val
        );
    }

    #[test]
    fn source_transaction_uses_the_exact_staged_memdb_for_reads_flags_and_commit() {
        let key = Key::from(b"direct".to_vec());
        let mut buffer = Buffer::new(false);

        let discarded = buffer.mem_buffer().staging();
        buffer
            .mem_buffer()
            .set_with_flags(
                b"direct",
                b"discarded",
                &[
                    FlagsOp::SetPresumeKeyNotExists,
                    FlagsOp::SetAssertNotExist,
                    FlagsOp::SetNeedConstraintCheckInPrewrite,
                ],
            )
            .unwrap();
        assert_eq!(buffer.get(&key), Some(b"discarded".to_vec()));
        assert_eq!(
            buffer.to_proto_mutations()[0].op,
            kvrpcpb::Op::Insert as i32
        );
        buffer.mem_buffer().cleanup(discarded);
        assert_eq!(buffer.get(&key), None);
        assert!(buffer.to_proto_mutations().is_empty());

        let committed = buffer.mem_buffer().staging();
        buffer
            .mem_buffer()
            .set_with_flags(
                b"direct",
                b"committed",
                &[
                    FlagsOp::SetPresumeKeyNotExists,
                    FlagsOp::SetAssertNotExist,
                    FlagsOp::SetNeedConstraintCheckInPrewrite,
                ],
            )
            .unwrap();
        buffer.mem_buffer().release(committed);

        let mutation = &buffer.to_proto_mutations()[0];
        assert_eq!(mutation.key, b"direct");
        assert_eq!(mutation.value, b"committed");
        assert_eq!(mutation.op, kvrpcpb::Op::Insert as i32);
        assert_eq!(mutation.assertion, kvrpcpb::Assertion::NotExist as i32);
        assert!(buffer
            .constraint_check_keys()
            .contains(b"direct".as_slice()));
        assert_eq!(buffer.len(), 1);
        assert_eq!(
            buffer.get_write_size(),
            b"direct".len() + b"committed".len()
        );
    }

    #[test]
    fn source_api_v2_memdb_surface_keeps_logical_keys_and_commit_encodes_once() {
        let keyspace = Keyspace::try_enable(42).unwrap();
        let mut buffer = Buffer::new_with_keyspace(false, keyspace);
        buffer.mem_buffer().set(b"key", b"value").unwrap();

        let physical = Key::from(b"key".to_vec()).encode_keyspace(keyspace, KeyMode::Txn);
        assert_eq!(buffer.get(&physical), Some(b"value".to_vec()));
        assert_eq!(buffer.mem_buffer().get(b"key").unwrap(), b"value");

        let mutation = &buffer.to_proto_mutations()[0];
        assert_eq!(mutation.key, [b'x', 0, 0, 42, b'k', b'e', b'y']);
        assert_eq!(mutation.value, b"value");
    }

    #[test]
    fn source_direct_memdb_lock_flags_are_authoritative() {
        let mut buffer = Buffer::new(true);
        buffer.mem_buffer().update_flags(
            b"shared",
            &[
                FlagsOp::SetKeyLocked,
                FlagsOp::SetKeyLockedInShareMode,
                FlagsOp::SetKeyLockedValueExists,
            ],
        );

        assert!(buffer.is_locked(&Key::from(b"shared".to_vec())));
        assert!(buffer.has_shared_locks());
        assert_eq!(
            buffer.to_proto_mutations()[0].op,
            kvrpcpb::Op::SharedLock as i32
        );
        assert!(buffer
            .pessimistic_lock_keys()
            .contains(b"shared".as_slice()));
    }
}
