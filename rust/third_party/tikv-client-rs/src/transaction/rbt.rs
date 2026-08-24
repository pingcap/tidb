//! Safe ordered-map implementation of client-go's rollbackable RBT memdb.
//!
//! The Go implementation uses an arena-backed red-black tree for allocation
//! efficiency. Rust's `BTreeMap` supplies the same ordered-key contract
//! safely; this module retains the observable value-log, staging, flag, and
//! snapshot semantics rather than the allocator representation.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::error::{EntryTooLargeError, KeyTooLargeError, StaticError, TransactionTooLargeError};
use crate::kv::{apply_flags_ops, FlagsOp, KeyFlags};

pub(crate) const MAX_KEY_LEN: usize = u16::MAX as usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct RbtHandle(u64);

#[derive(Clone, Debug)]
struct Entry {
    value: Option<Vec<u8>>,
    flags: KeyFlags,
    /// Previous values, newest last. Flags deliberately are not versioned:
    /// client-go's flags map is non-rollbackable.
    history: Vec<Option<Vec<u8>>>,
    /// Position of the value-log record that backs `value`, when it was
    /// written in a staging buffer. It lets equal-sized writes in the active
    /// stage overwrite in place, matching client-go's value-log behavior.
    value_log_undo_index: Option<usize>,
    handle: RbtHandle,
}

#[derive(Clone, Debug)]
struct Undo {
    key: Vec<u8>,
    old_value: Option<Option<Vec<u8>>>,
    old_history_len: usize,
    old_value_log_undo_index: Option<usize>,
}

#[derive(Clone, Copy, Debug)]
struct Stage {
    undo_start: usize,
}

/// Source-compatible transaction buffer with rollbackable values and durable
/// key flags. It is intentionally crate-private until the parent unionstore
/// package consumes it.
#[derive(Default)]
pub(crate) struct Rbt {
    entries: BTreeMap<Vec<u8>, Entry>,
    handles: BTreeMap<RbtHandle, Vec<u8>>,
    undo: Vec<Undo>,
    stages: Vec<Stage>,
    next_handle: u64,
    entry_size_limit: u64,
    buffer_size_limit: u64,
    logical_size: usize,
    dirty: bool,
    values_discarded: bool,
    last_key: Option<Vec<u8>>,
    cache_hits: u64,
    cache_misses: u64,
    memory_hook: Option<Arc<dyn Fn(u64) + Send + Sync>>,
}

impl Rbt {
    pub(crate) fn new() -> Self {
        Self {
            entry_size_limit: u64::MAX,
            buffer_size_limit: u64::MAX,
            ..Self::default()
        }
    }

    pub(crate) fn is_staging(&self) -> bool {
        !self.stages.is_empty()
    }

    pub(crate) fn staging(&mut self) -> usize {
        self.stages.push(Stage {
            undo_start: self.undo.len(),
        });
        self.stages.len()
    }

    pub(crate) fn release(&mut self, handle: usize) {
        if handle == 0 {
            return;
        }
        assert_eq!(handle, self.stages.len(), "cannot release staging buffer");
        if handle == 1 && self.undo.len() != self.stages[0].undo_start {
            self.dirty = true;
        }
        self.stages.pop();
    }

    pub(crate) fn cleanup(&mut self, handle: usize) {
        if handle == 0 || handle > self.stages.len() {
            return;
        }
        assert_eq!(handle, self.stages.len(), "cannot cleanup staging buffer");
        let start = self.stages.pop().unwrap().undo_start;
        while self.undo.len() > start {
            let undo = self.undo.pop().unwrap();
            self.revert(undo);
        }
        self.notify_memory_change();
    }

    fn revert(&mut self, undo: Undo) {
        let current_value_len = self
            .entries
            .get(&undo.key)
            .and_then(|entry| entry.value.as_ref())
            .map_or(0, Vec::len);
        match undo.old_value {
            Some(old_value) => {
                let old_value_len = old_value.as_ref().map_or(0, Vec::len);
                if let Some(entry) = self.entries.get_mut(&undo.key) {
                    entry.value = old_value;
                    entry.history.truncate(undo.old_history_len);
                    entry.value_log_undo_index = undo.old_value_log_undo_index;
                    self.logical_size = self.logical_size + old_value_len - current_value_len;
                }
            }
            None => {
                let persistent = self
                    .entries
                    .get(&undo.key)
                    .map(|entry| entry.flags.and_persistent())
                    .unwrap_or_default();
                if persistent.bits() == 0 {
                    if let Some(entry) = self.entries.remove(&undo.key) {
                        self.handles.remove(&entry.handle);
                        self.logical_size -= undo.key.len() + current_value_len;
                    }
                } else if let Some(entry) = self.entries.get_mut(&undo.key) {
                    entry.value = None;
                    entry.history.clear();
                    entry.value_log_undo_index = None;
                    entry.flags = persistent;
                    self.logical_size -= current_value_len;
                }
            }
        }
    }

    pub(crate) fn checkpoint(&self) -> usize {
        self.undo.len()
    }

    pub(crate) fn revert_to_checkpoint(&mut self, checkpoint: usize) {
        assert!(checkpoint <= self.undo.len(), "invalid RBT checkpoint");
        while self.undo.len() > checkpoint {
            let undo = self.undo.pop().unwrap();
            self.revert(undo);
        }
        self.notify_memory_change();
    }

    pub(crate) fn reset(&mut self) {
        self.entries.clear();
        self.handles.clear();
        self.undo.clear();
        self.stages.clear();
        self.next_handle = 0;
        self.logical_size = 0;
        self.dirty = false;
        self.values_discarded = false;
        self.last_key = None;
        self.notify_memory_change();
    }

    pub(crate) fn discard_values(&mut self) {
        self.values_discarded = true;
    }

    pub(crate) fn set_entry_size_limit(&mut self, entry_limit: u64, buffer_limit: u64) {
        self.entry_size_limit = entry_limit;
        self.buffer_size_limit = buffer_limit;
    }

    pub(crate) fn entry_size_limit(&self) -> (u64, u64) {
        (self.entry_size_limit, self.buffer_size_limit)
    }

    pub(crate) fn set_memory_footprint_change_hook(
        &mut self,
        hook: Arc<dyn Fn(u64) + Send + Sync>,
    ) {
        self.memory_hook = Some(hook);
    }

    pub(crate) fn memory_hook_is_set(&self) -> bool {
        self.memory_hook.is_some()
    }

    /// Native memory accounting is payload based; Go exposes allocated arena
    /// capacity, which has no safe `BTreeMap` equivalent.
    pub(crate) fn memory_footprint(&self) -> u64 {
        self.entries
            .iter()
            .map(|(key, entry)| {
                let values = if self.values_discarded {
                    0
                } else {
                    entry.value.as_ref().map_or(0, Vec::len)
                        + entry.history.iter().flatten().map(Vec::len).sum::<usize>()
                };
                (key.len() + values) as u64
            })
            .sum()
    }

    fn notify_memory_change(&self) {
        if let Some(hook) = &self.memory_hook {
            hook(self.memory_footprint());
        }
    }

    fn entry_mut(&mut self, key: &[u8]) -> (&mut Entry, bool) {
        if self.last_key.as_deref() == Some(key) {
            self.cache_hits += 1;
        } else {
            self.cache_misses += 1;
            self.last_key = Some(key.to_vec());
        }
        let key = key.to_vec();
        let existed = self.entries.contains_key(&key);
        if !existed {
            let handle = RbtHandle(self.next_handle);
            self.next_handle += 1;
            self.handles.insert(handle, key.clone());
            self.entries.insert(
                key.clone(),
                Entry {
                    value: None,
                    flags: KeyFlags::default(),
                    history: Vec::new(),
                    value_log_undo_index: None,
                    handle,
                },
            );
            self.logical_size += key.len();
            self.notify_memory_change();
        }
        (self.entries.get_mut(&key).unwrap(), existed)
    }

    fn entry(&mut self, key: &[u8]) -> Option<&Entry> {
        if self.last_key.as_deref() == Some(key) {
            self.cache_hits += 1;
        } else {
            self.cache_misses += 1;
            self.last_key = Some(key.to_vec());
        }
        self.entries.get(key)
    }

    pub(crate) fn set(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        operations: &[FlagsOp],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        assert!(!self.values_discarded, "vlog is reset");
        if key.len() > MAX_KEY_LEN {
            return Err(Box::new(KeyTooLargeError {
                key_size: key.len(),
            }));
        }
        if let Some(value) = value {
            let size = (key.len() + value.len()) as u64;
            if size > self.entry_size_limit {
                return Err(Box::new(EntryTooLargeError {
                    limit: self.entry_size_limit,
                    size,
                }));
            }
        }
        if !self.is_staging() {
            self.dirty = true;
        }
        let existed_before = self.entries.contains_key(key);
        let old = self.entries.get(key).map(|entry| entry.value.clone());
        let old_history_len = self.entries.get(key).map_or(0, |entry| entry.history.len());
        let old_value_len = old.as_ref().and_then(Option::as_ref).map_or(0, Vec::len);
        let old_value_log_undo_index = self
            .entries
            .get(key)
            .and_then(|entry| entry.value_log_undo_index);
        let staging = self.is_staging();
        let stage_start = self.stages.last().map(|stage| stage.undo_start);
        let can_modify_value = value.is_some_and(|value| {
            self.entries.get(key).is_some_and(|entry| {
                entry.value.as_ref().is_some_and(|old_value| {
                    !old_value.is_empty()
                        && old_value.len() == value.len()
                        && stage_start.is_none_or(|start| {
                            entry
                                .value_log_undo_index
                                .is_some_and(|index| index >= start)
                        })
                })
            })
        });
        let appended_value_log_index = if value.is_some() && staging && !can_modify_value {
            self.undo.push(Undo {
                key: key.to_vec(),
                old_value: if existed_before { old } else { None },
                old_history_len,
                old_value_log_undo_index,
            });
            Some(self.undo.len() - 1)
        } else {
            None
        };
        let mut flags_ops = Vec::with_capacity(operations.len() + 1);
        if value.is_some() {
            flags_ops.push(FlagsOp::DelNeedConstraintCheckInPrewrite);
        }
        flags_ops.extend_from_slice(operations);
        let persistent_flags;
        {
            let (entry, _) = self.entry_mut(key);
            entry.flags = apply_flags_ops(entry.flags, &flags_ops);
            persistent_flags = entry.flags.and_persistent().bits() != 0;
            if let Some(value) = value {
                if !can_modify_value {
                    entry.history.push(entry.value.clone());
                    entry.value_log_undo_index = appended_value_log_index;
                }
                entry.value = Some(value.to_vec());
                self.logical_size = self.logical_size + value.len() - old_value_len;
            }
        }
        if persistent_flags {
            self.dirty = true;
        }
        if value.is_some() {
            if self.size() as u64 > self.buffer_size_limit {
                return Err(Box::new(TransactionTooLargeError { size: self.size() }));
            }
            self.notify_memory_change();
        }
        Ok(())
    }

    pub(crate) fn get(&mut self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        assert!(!self.values_discarded, "vlog is resetted");
        self.entry(key)
            .and_then(|entry| entry.value.clone())
            .ok_or(StaticError::NotExist)
    }

    pub(crate) fn select_value_history(
        &mut self,
        key: &[u8],
        mut predicate: impl FnMut(&[u8]) -> bool,
    ) -> Result<Option<Vec<u8>>, StaticError> {
        let entry = self.entry(key).ok_or(StaticError::NotExist)?;
        let mut values = entry
            .value
            .iter()
            .chain(entry.history.iter().rev().flatten());
        Ok(values.find(|value| predicate(value)).cloned())
    }

    pub(crate) fn flags(&mut self, key: &[u8]) -> Result<KeyFlags, StaticError> {
        self.entry(key)
            .map(|entry| entry.flags)
            .ok_or(StaticError::NotExist)
    }

    /// Apply non-rollbackable flag operations to a key, creating a flags-only
    /// entry when needed. This is the native equivalent of
    /// `RBTIterator.UpdateFlags` without tying a Rust iterator's lifetime to a
    /// mutable map borrow.
    pub(crate) fn update_flags(&mut self, key: &[u8], operations: &[FlagsOp]) {
        let persistent_flags;
        {
            let (entry, _) = self.entry_mut(key);
            entry.flags = apply_flags_ops(entry.flags, operations);
            persistent_flags = entry.flags.and_persistent().bits() != 0;
        }
        if persistent_flags {
            self.dirty = true;
        }
    }

    pub(crate) fn remove_from_buffer(&mut self, key: &[u8]) {
        assert!(!self.values_discarded, "vlog is resetted");
        if self.entry(key).is_some() {
            let entry = self.entries.remove(key).expect("entry checked above");
            self.handles.remove(&entry.handle);
            self.logical_size -= key.len() + entry.value.as_ref().map_or(0, Vec::len);
        }
    }

    pub(crate) fn key_by_handle(&self, handle: RbtHandle) -> Option<&[u8]> {
        self.handles.get(&handle).map(Vec::as_slice)
    }

    pub(crate) fn value_by_handle(&self, handle: RbtHandle) -> Option<&[u8]> {
        if self.values_discarded {
            return None;
        }
        self.handles
            .get(&handle)
            .and_then(|key| self.entries.get(key))
            .and_then(|entry| entry.value.as_deref())
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.len()
    }

    pub(crate) fn size(&self) -> usize {
        self.logical_size
    }

    pub(crate) fn dirty(&self) -> bool {
        self.dirty
    }

    pub(crate) fn cache_hit_count(&self) -> u64 {
        self.cache_hits
    }

    pub(crate) fn cache_miss_count(&self) -> u64 {
        self.cache_misses
    }

    pub(crate) fn inspect_stage(
        &self,
        handle: usize,
        mut function: impl FnMut(&[u8], KeyFlags, &[u8]),
    ) {
        let stage = self.stages[handle - 1];
        let keys: BTreeSet<&[u8]> = self.undo[stage.undo_start..]
            .iter()
            .map(|undo| undo.key.as_slice())
            .collect();
        for key in keys {
            if let Some(entry) = self.entries.get(key) {
                if let Some(value) = entry.value.as_deref() {
                    function(key, entry.flags, value);
                }
            }
        }
    }

    pub(crate) fn iter(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> RbtIterator {
        RbtIterator::new(self, start, end, false, false)
    }

    pub(crate) fn iter_reverse(
        &self,
        start_exclusive: Option<&[u8]>,
        lower_bound: Option<&[u8]>,
    ) -> RbtIterator {
        RbtIterator::new(self, lower_bound, start_exclusive, true, false)
    }

    pub(crate) fn iter_with_flags(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> RbtIterator {
        RbtIterator::new(self, start, end, false, true)
    }

    pub(crate) fn iter_reverse_with_flags(&self, start_exclusive: Option<&[u8]>) -> RbtIterator {
        RbtIterator::new(self, None, start_exclusive, true, true)
    }

    pub(crate) fn snapshot(&self) -> RbtSnapshot {
        let mut entries = self.entries.clone();
        if let Some(stage) = self.stages.first() {
            for undo in self.undo[stage.undo_start..].iter().rev() {
                match &undo.old_value {
                    Some(value) => {
                        if let Some(entry) = entries.get_mut(&undo.key) {
                            entry.value = value.clone();
                            entry.history.truncate(undo.old_history_len);
                            entry.value_log_undo_index = undo.old_value_log_undo_index;
                        }
                    }
                    None => {
                        entries.remove(&undo.key);
                    }
                }
            }
        }
        RbtSnapshot { entries }
    }
}

/// Ordered iterator with copied keys/values. A live Go iterator is invalidated
/// by writes; this native iterator instead preserves a stable traversal view.
type IteratorItem = (Vec<u8>, Option<Vec<u8>>, KeyFlags, RbtHandle);

pub(crate) struct RbtIterator {
    items: Vec<IteratorItem>,
    index: usize,
}

impl RbtIterator {
    fn new(
        db: &Rbt,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
        include_flags: bool,
    ) -> Self {
        let mut items: Vec<_> = db
            .entries
            .iter()
            .filter(|(key, entry)| {
                lower.is_none_or(|lower| key.as_slice() >= lower)
                    && upper.is_none_or(|upper| key.as_slice() < upper)
                    && (include_flags || entry.value.is_some())
            })
            .map(|(key, entry)| (key.clone(), entry.value.clone(), entry.flags, entry.handle))
            .collect();
        if reverse {
            items.reverse();
        }
        Self { items, index: 0 }
    }

    pub(crate) fn valid(&self) -> bool {
        self.index < self.items.len()
    }

    pub(crate) fn key(&self) -> &[u8] {
        &self.items[self.index].0
    }

    pub(crate) fn value(&self) -> Option<&[u8]> {
        self.items[self.index].1.as_deref()
    }

    pub(crate) fn flags(&self) -> KeyFlags {
        self.items[self.index].2
    }

    pub(crate) fn handle(&self) -> RbtHandle {
        self.items[self.index].3
    }

    pub(crate) fn has_value(&self) -> bool {
        self.value().is_some()
    }

    pub(crate) fn next(&mut self) {
        self.index += 1;
    }
}

#[derive(Clone)]
pub(crate) struct RbtSnapshot {
    entries: BTreeMap<Vec<u8>, Entry>,
}

impl RbtSnapshot {
    pub(crate) fn get(&self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        self.entries
            .get(key)
            .and_then(|entry| entry.value.clone())
            .ok_or(StaticError::NotExist)
    }

    pub(crate) fn iter(&self, start: Option<&[u8]>, end: Option<&[u8]>) -> RbtIterator {
        RbtIterator::new(
            &Rbt {
                entries: self.entries.clone(),
                ..Rbt::new()
            },
            start,
            end,
            false,
            false,
        )
    }

    pub(crate) fn iter_reverse(
        &self,
        start_exclusive: Option<&[u8]>,
        lower_bound: Option<&[u8]>,
    ) -> RbtIterator {
        RbtIterator::new(
            &Rbt {
                entries: self.entries.clone(),
                ..Rbt::new()
            },
            lower_bound,
            start_exclusive,
            true,
            false,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(number: u32) -> [u8; 4] {
        number.to_be_bytes()
    }

    #[test]
    fn original_discard_staging_and_iterator_cases() {
        let mut db = Rbt::new();
        let base = db.staging();
        for number in 0..10_000 {
            let key = key(number);
            db.set(&key, Some(&key), &[]).unwrap();
        }
        let size = db.size();
        let replacement = db.staging();
        for number in 0..10_000 {
            let key = key(number);
            db.set(&key, Some(&(number + 1).to_be_bytes()), &[])
                .unwrap();
        }
        db.cleanup(replacement);
        assert_eq!(db.len(), 10_000);
        assert_eq!(db.size(), size);
        for number in 0..10_000 {
            let key = key(number);
            assert_eq!(db.get(&key).unwrap(), key);
        }
        let mut iterator = db.iter(None, None);
        for number in 0..10_000 {
            assert!(iterator.valid());
            assert_eq!(iterator.key(), key(number));
            assert_eq!(iterator.value().unwrap(), key(number));
            iterator.next();
        }
        assert!(!iterator.valid());
        db.cleanup(base);
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn flags_remain_non_rollbackable_but_only_persistent_flags_keep_new_keys() {
        let mut db = Rbt::new();
        let stage = db.staging();
        for number in 0..100 {
            let key = key(number);
            let operations = if number % 2 == 0 {
                vec![FlagsOp::SetPresumeKeyNotExists, FlagsOp::SetKeyLocked]
            } else {
                vec![FlagsOp::SetPresumeKeyNotExists]
            };
            db.set(&key, Some(&key), &operations).unwrap();
        }
        db.cleanup(stage);
        assert_eq!(db.len(), 50);
        for number in 0..100 {
            let key = key(number);
            assert!(db.get(&key).is_err());
            if number % 2 == 0 {
                let flags = db.flags(&key).unwrap();
                assert!(flags.has_locked());
                assert!(!flags.has_presume_key_not_exists());
            } else {
                assert!(db.flags(&key).is_err());
            }
        }
        assert_eq!(db.len(), 50);
        let mut without_locked = Vec::new();
        for number in 0..100 {
            let key = key(number);
            db.set(&key, None, &[FlagsOp::DelKeyLocked]).unwrap();
            without_locked.push(key);
        }
        for key in without_locked {
            assert!(db.get(&key).is_err());
            assert!(!db.flags(&key).unwrap().has_locked());
        }
        assert_eq!(db.len(), 100);
    }

    #[test]
    fn snapshots_histories_limits_handles_and_flag_iterator_match_source_contracts() {
        let mut db = Rbt::new();
        db.set(b"a", Some(b"one"), &[]).unwrap();
        let snapshot = db.snapshot();
        let stage = db.staging();
        db.set(b"a", Some(b"two"), &[]).unwrap();
        db.set(b"b", Some(b"three"), &[FlagsOp::SetKeyLocked])
            .unwrap();
        let staged_snapshot = db.snapshot();
        assert_eq!(
            db.select_value_history(b"a", |value| value == b"one")
                .unwrap(),
            Some(b"one".to_vec())
        );
        assert_eq!(snapshot.get(b"a").unwrap(), b"one");
        assert!(snapshot.get(b"b").is_err());
        assert_eq!(staged_snapshot.get(b"a").unwrap(), b"one");
        assert!(staged_snapshot.get(b"b").is_err());
        let handle = db.iter(None, None).handle();
        assert_eq!(db.key_by_handle(handle), Some(&b"a"[..]));
        assert_eq!(db.value_by_handle(handle), Some(&b"two"[..]));
        db.cleanup(stage);
        assert_eq!(db.get(b"a").unwrap(), b"one");
        assert!(db.get(b"b").is_err());
        assert!(db.flags(b"b").unwrap().has_locked());

        db.set_entry_size_limit(3, 3);
        assert!(db.set(b"long", Some(b"v"), &[]).is_err());
        db.set_entry_size_limit(u64::MAX, 1);
        assert!(db.set(b"c", Some(b"v"), &[]).is_err());
        let mut flags = db.iter_with_flags(None, None);
        assert!(flags.valid());
        assert!(flags.has_value());
        flags.next();
        assert!(flags.valid());
        assert!(!flags.has_value());
        let mut reverse_flags = db.iter_reverse_with_flags(None);
        assert!(reverse_flags.valid());
        reverse_flags.next();
    }

    #[test]
    fn bounds_reverse_memory_hook_discard_and_cache_stats_are_observable() {
        let mut db = Rbt::new();
        let observed = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let hook_observed = observed.clone();
        db.set_memory_footprint_change_hook(Arc::new(move |memory| {
            hook_observed.store(memory, std::sync::atomic::Ordering::Release);
        }));
        db.set(b"a", Some(b"1"), &[]).unwrap();
        db.set(b"b", Some(b"2"), &[]).unwrap();
        db.set(b"c", Some(b"3"), &[]).unwrap();
        assert!(db.memory_hook_is_set());
        assert!(observed.load(std::sync::atomic::Ordering::Acquire) > 0);
        let mut forward = db.iter(Some(b"b"), Some(b"c"));
        assert_eq!(forward.key(), b"b");
        forward.next();
        assert!(!forward.valid());
        let mut reverse = db.iter_reverse(Some(b"c"), Some(b"a"));
        assert_eq!(reverse.key(), b"b");
        reverse.next();
        assert_eq!(reverse.key(), b"a");
        let _ = db.get(b"a");
        let _ = db.get(b"a");
        assert!(db.cache_hit_count() > 0);
        assert!(db.cache_miss_count() > 0);
        let handle = db.iter(None, None).handle();
        let size_before_discard = db.size();
        db.discard_values();
        assert_eq!(db.size(), size_before_discard);
        assert!(db.iter(None, None).valid());
        assert!(db.value_by_handle(handle).is_none());
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| db.get(b"a")));
        assert!(panic.is_err());
    }

    #[test]
    fn equal_sized_writes_only_retain_history_when_the_value_log_appends() {
        let mut db = Rbt::new();
        db.set(b"history", Some(b"one"), &[]).unwrap();
        db.set(b"history", Some(b"two"), &[]).unwrap();
        assert_eq!(
            db.select_value_history(b"history", |value| value == b"one")
                .unwrap(),
            None
        );
        db.set(b"history", Some(b"three"), &[]).unwrap();
        assert_eq!(
            db.select_value_history(b"history", |value| value == b"two")
                .unwrap(),
            Some(b"two".to_vec())
        );
    }

    #[test]
    fn checkpoints_stage_inspection_and_native_flag_updates_preserve_source_semantics() {
        let mut db = Rbt::new();
        let first = db.staging();
        db.set(b"a", Some(b"one"), &[]).unwrap();
        let checkpoint = db.checkpoint();
        db.set(b"a", Some(b"two"), &[]).unwrap();
        db.set(b"b", Some(b"three"), &[]).unwrap();
        let second = db.staging();
        db.set(b"c", Some(b"four"), &[]).unwrap();

        let mut inspected = Vec::new();
        db.inspect_stage(second, |key, _, value| {
            inspected.push((key.to_vec(), value.to_vec()));
        });
        assert_eq!(inspected, vec![(b"c".to_vec(), b"four".to_vec())]);
        db.revert_to_checkpoint(checkpoint);
        // `a` was written in the active outer stage before the checkpoint.
        // client-go overwrites equal-sized values in that stage's log entry,
        // so rolling the later checkpoint back retains "two".
        assert_eq!(db.get(b"a").unwrap(), b"two");
        assert!(db.get(b"b").is_err());
        assert!(db.get(b"c").is_err());
        db.cleanup(second);
        db.update_flags(b"flags", &[FlagsOp::SetKeyLocked]);
        assert!(db.flags(b"flags").unwrap().has_locked());
        assert!(db.get(b"flags").is_err());
        db.cleanup(first);
        assert!(db.flags(b"flags").unwrap().has_locked());
        assert_eq!(db.entry_size_limit(), (u64::MAX, u64::MAX));
        db.reset();
        assert_eq!(db.len(), 0);
        assert!(!db.dirty());
    }

    #[test]
    fn empty_bounds_handles_tombstones_and_snapshot_reverse_are_source_compatible() {
        let mut db = Rbt::new();
        assert!(db.get(&[0]).is_err());
        assert!(!db.iter(None, None).valid());
        assert!(!db.iter_reverse(None, None).valid());

        db.set(b"a", Some(&[]), &[]).unwrap();
        db.set(b"b", Some(b"value"), &[]).unwrap();
        let handle = db.iter(None, None).handle();
        assert_eq!(db.key_by_handle(handle), Some(&b"a"[..]));
        assert_eq!(db.value_by_handle(handle), Some(&b""[..]));
        let snapshot = db.snapshot();
        db.remove_from_buffer(b"a");
        assert!(db.key_by_handle(handle).is_none());
        assert_eq!(snapshot.get(b"a").unwrap(), b"");
        let mut reverse = snapshot.iter_reverse(None, Some(b"a"));
        assert!(reverse.valid());
        assert_eq!(reverse.key(), b"b");
        reverse.next();
        assert_eq!(reverse.key(), b"a");

        let too_large_key = vec![0; MAX_KEY_LEN + 1];
        let error = db.set(&too_large_key, Some(b"x"), &[]).unwrap_err();
        assert!(error.downcast_ref::<KeyTooLargeError>().is_some());
    }
}
