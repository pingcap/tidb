//! Safe ordered-map implementation of client-go's rollbackable ART memdb.
//!
//! client-go uses an adaptive radix tree backed by manually managed arenas.
//! This module maps its externally observable transaction-buffer semantics to
//! a `BTreeMap`: value-log rollback, non-rollbackable flags, durable handles,
//! ordered bounds, and iterator invalidation are retained without exposing
//! unsafe nodes or arena addresses.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use crate::error::{EntryTooLargeError, KeyTooLargeError, StaticError, TransactionTooLargeError};
use crate::kv::{apply_flags_ops, FlagsOp, KeyFlags};

use super::rbt::MAX_KEY_LEN;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct ArtHandle(u64);

#[derive(Clone, Debug)]
struct Entry {
    value: Option<Vec<u8>>,
    flags: KeyFlags,
    history: Vec<Option<Vec<u8>>>,
    value_log_undo_index: Option<usize>,
    handle: ArtHandle,
    deleted: bool,
}

#[derive(Clone, Debug)]
struct Undo {
    key: Vec<u8>,
    old_value: Option<Option<Vec<u8>>>,
    old_history_len: usize,
    old_value_log_undo_index: Option<usize>,
    old_deleted: bool,
}

#[derive(Clone, Copy, Debug)]
struct Stage {
    undo_start: usize,
}

fn contribution(key_len: usize, value: Option<&[u8]>, deleted: bool) -> usize {
    if deleted {
        0
    } else {
        key_len + value.map_or(0, <[u8]>::len)
    }
}

/// Native mapping of client-go's ART transaction buffer.
#[derive(Default)]
pub(crate) struct Art {
    entries: BTreeMap<Vec<u8>, Entry>,
    handles: BTreeMap<ArtHandle, Vec<u8>>,
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
    write_sequence: Arc<AtomicU64>,
    snapshot_sequence: Arc<AtomicU64>,
}

impl Art {
    pub(crate) fn new() -> Self {
        Self {
            entry_size_limit: u64::MAX,
            buffer_size_limit: u64::MAX,
            write_sequence: Arc::new(AtomicU64::new(0)),
            snapshot_sequence: Arc::new(AtomicU64::new(0)),
            ..Self::default()
        }
    }

    fn bump_write_sequence(&self) {
        self.write_sequence.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn write_sequence(&self) -> u64 {
        self.write_sequence.load(Ordering::Acquire)
    }

    pub(crate) fn snapshot_sequence(&self) -> u64 {
        self.snapshot_sequence.load(Ordering::Acquire)
    }

    pub(crate) fn snapshot_sequence_counter(&self) -> Arc<AtomicU64> {
        self.snapshot_sequence.clone()
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
        self.bump_write_sequence();
        if handle == 1 {
            self.snapshot_sequence.fetch_add(1, Ordering::Release);
            if self.undo.len() != self.stages[0].undo_start {
                self.dirty = true;
            }
        }
        self.stages.pop();
    }

    pub(crate) fn cleanup(&mut self, handle: usize) {
        if handle == 0 || handle > self.stages.len() {
            return;
        }
        assert_eq!(handle, self.stages.len(), "cannot cleanup staging buffer");
        self.bump_write_sequence();
        if handle == 1 {
            self.snapshot_sequence.fetch_add(1, Ordering::Release);
        }
        let start = self.stages.pop().unwrap().undo_start;
        while self.undo.len() > start {
            let undo = self.undo.pop().unwrap();
            self.revert(undo);
        }
        self.notify_memory_change();
    }

    fn revert(&mut self, undo: Undo) {
        let Some(entry) = self.entries.get_mut(&undo.key) else {
            return;
        };
        let key_len = undo.key.len();
        let before = contribution(key_len, entry.value.as_deref(), entry.deleted);
        match undo.old_value {
            Some(old_value) => {
                if undo.old_deleted && old_value.is_none() {
                    let persistent = entry.flags.and_persistent();
                    entry.value = None;
                    entry.history.clear();
                    entry.value_log_undo_index = None;
                    if persistent.bits() == 0 {
                        entry.deleted = true;
                        entry.flags = KeyFlags::default();
                    } else {
                        entry.deleted = false;
                        entry.flags = persistent;
                    }
                } else {
                    entry.value = old_value;
                    entry.history.truncate(undo.old_history_len);
                    entry.value_log_undo_index = undo.old_value_log_undo_index;
                    entry.deleted = undo.old_deleted;
                }
            }
            None => {
                entry.value = None;
                entry.history.clear();
                entry.value_log_undo_index = None;
                let persistent = entry.flags.and_persistent();
                if persistent.bits() == 0 {
                    if !entry.deleted {
                        entry.deleted = true;
                    }
                    entry.flags = KeyFlags::default();
                } else {
                    entry.flags = persistent;
                }
            }
        }
        let after = contribution(key_len, entry.value.as_deref(), entry.deleted);
        self.logical_size = self.logical_size + after - before;
    }

    pub(crate) fn checkpoint(&self) -> usize {
        self.undo.len()
    }

    /// Native checkpoint positions corresponding to client-go's `Stages`.
    pub(crate) fn stages(&self) -> Vec<usize> {
        self.stages.iter().map(|stage| stage.undo_start).collect()
    }

    pub(crate) fn revert_to_checkpoint(&mut self, checkpoint: usize) {
        assert!(checkpoint <= self.undo.len(), "invalid ART checkpoint");
        while self.undo.len() > checkpoint {
            let undo = self.undo.pop().unwrap();
            self.revert(undo);
        }
        self.bump_write_sequence();
        if self
            .stages
            .first()
            .is_none_or(|stage| stage.undo_start < checkpoint)
        {
            self.snapshot_sequence.fetch_add(1, Ordering::Release);
        }
        self.notify_memory_change();
    }

    pub(crate) fn reset(&mut self) {
        self.entries.clear();
        self.handles.clear();
        self.undo.clear();
        self.stages.clear();
        self.next_handle = 0;
        self.dirty = false;
        self.values_discarded = false;
        self.logical_size = 0;
        self.last_key = None;
        self.snapshot_sequence.fetch_add(1, Ordering::Release);
        self.bump_write_sequence();
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

    /// Builds an empty generation with the same limits and memory hook.
    ///
    /// Pipelined transactions rotate the active ART before dispatching a
    /// flush. The new generation must retain caller-installed limits and
    /// accounting hooks without cloning any buffered entries or stage state.
    pub(crate) fn empty_generation(&self) -> Self {
        let mut next = Self::new();
        next.set_entry_size_limit(self.entry_size_limit, self.buffer_size_limit);
        next.memory_hook.clone_from(&self.memory_hook);
        next
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

    pub(crate) fn take_memory_hook(&mut self) -> Option<Arc<dyn Fn(u64) + Send + Sync>> {
        self.memory_hook.take()
    }

    /// Native payload accounting; client-go reports arena capacities.
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
            let handle = ArtHandle(self.next_handle);
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
                    deleted: false,
                },
            );
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

        self.bump_write_sequence();
        if !self.is_staging() {
            self.dirty = true;
            self.snapshot_sequence.fetch_add(1, Ordering::Release);
        }
        let existed = self.entries.contains_key(key);
        let old_value = self.entries.get(key).map(|entry| entry.value.clone());
        let old_history_len = self.entries.get(key).map_or(0, |entry| entry.history.len());
        let old_value_log_undo_index = self
            .entries
            .get(key)
            .and_then(|entry| entry.value_log_undo_index);
        let old_deleted = self.entries.get(key).is_some_and(|entry| entry.deleted);
        let was_live = existed && !old_deleted;
        let old_contribution = contribution(
            key.len(),
            old_value.as_ref().and_then(Option::as_deref),
            !was_live,
        );
        let stage_start = self.stages.last().map(|stage| stage.undo_start);
        let can_modify_value = value.is_some_and(|value| {
            self.entries.get(key).is_some_and(|entry| {
                entry.value.as_ref().is_some_and(|old| {
                    !old.is_empty()
                        && old.len() == value.len()
                        && stage_start.is_none_or(|start| {
                            entry
                                .value_log_undo_index
                                .is_some_and(|index| index >= start)
                        })
                })
            })
        });
        let appended_value_log_index = if value.is_some() && self.is_staging() && !can_modify_value
        {
            self.undo.push(Undo {
                key: key.to_vec(),
                old_value: if existed { old_value } else { None },
                old_history_len,
                old_value_log_undo_index,
                old_deleted,
            });
            Some(self.undo.len() - 1)
        } else {
            None
        };

        let persistent;
        {
            let (entry, _) = self.entry_mut(key);
            if entry.deleted {
                entry.deleted = false;
            }
            let mut flag_ops = Vec::with_capacity(operations.len() + 1);
            if value.is_some() {
                flag_ops.push(FlagsOp::DelNeedConstraintCheckInPrewrite);
            }
            flag_ops.extend_from_slice(operations);
            entry.flags = apply_flags_ops(entry.flags, &flag_ops);
            persistent = entry.flags.and_persistent().bits() != 0;
            if let Some(value) = value {
                if !can_modify_value {
                    entry.history.push(entry.value.clone());
                    entry.value_log_undo_index = appended_value_log_index;
                }
                entry.value = Some(value.to_vec());
            }
        }
        let new_value = self
            .entries
            .get(key)
            .and_then(|entry| entry.value.as_deref());
        let new_contribution = contribution(key.len(), new_value, false);
        self.logical_size = self.logical_size + new_contribution - old_contribution;
        if persistent {
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
        assert!(!self.values_discarded, "vlog is reset");
        self.entry(key)
            .filter(|entry| !entry.deleted)
            .and_then(|entry| entry.value.clone())
            .ok_or(StaticError::NotExist)
    }

    /// Read without touching the source-compatible last-key cache. This is
    /// used by the immutable flushing generation of a pipelined MemDB.
    pub(crate) fn get_readonly(&self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        assert!(!self.values_discarded, "vlog is reset");
        self.entries
            .get(key)
            .filter(|entry| !entry.deleted)
            .and_then(|entry| entry.value.clone())
            .ok_or(StaticError::NotExist)
    }

    pub(crate) fn flags(&mut self, key: &[u8]) -> Result<KeyFlags, StaticError> {
        self.entry(key)
            .filter(|entry| !entry.deleted)
            .map(|entry| entry.flags)
            .ok_or(StaticError::NotExist)
    }

    pub(crate) fn flags_readonly(&self, key: &[u8]) -> Result<KeyFlags, StaticError> {
        self.entries
            .get(key)
            .filter(|entry| !entry.deleted)
            .map(|entry| entry.flags)
            .ok_or(StaticError::NotExist)
    }

    pub(crate) fn select_value_history(
        &mut self,
        key: &[u8],
        mut predicate: impl FnMut(&[u8]) -> bool,
    ) -> Result<Option<Vec<u8>>, StaticError> {
        let entry = self
            .entry(key)
            .filter(|entry| !entry.deleted && entry.value.is_some())
            .ok_or(StaticError::NotExist)?;
        Ok(entry
            .value
            .iter()
            .chain(entry.history.iter().rev().flatten())
            .find(|value| predicate(value))
            .cloned())
    }

    /// Native replacement for Go's iterator-borrowed `UpdateFlags`.
    ///
    /// Go can only invoke this on the iterator's current, live leaf. Rust
    /// cannot retain that mutable map borrow in a safe iterator, so callers
    /// pass the current key explicitly and an invalid/missing key panics.
    pub(crate) fn update_flags(&mut self, key: &[u8], operations: &[FlagsOp]) {
        let entry = self
            .entries
            .get_mut(key)
            .filter(|entry| !entry.deleted)
            .expect("ART iterator flag update requires a live key");
        entry.flags = apply_flags_ops(entry.flags, operations);
        if entry.flags.and_persistent().bits() != 0 {
            self.dirty = true;
        }
    }

    pub(crate) fn key_by_handle(&self, handle: ArtHandle) -> Option<&[u8]> {
        self.handles.get(&handle).map(Vec::as_slice)
    }

    pub(crate) fn value_by_handle(&self, handle: ArtHandle) -> Option<&[u8]> {
        if self.values_discarded {
            return None;
        }
        self.handles
            .get(&handle)
            .and_then(|key| self.entries.get(key))
            .filter(|entry| !entry.deleted)
            .and_then(|entry| entry.value.as_deref())
    }

    pub(crate) fn len(&self) -> usize {
        self.entries.values().filter(|entry| !entry.deleted).count()
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
            if let Some(entry) = self.entries.get(key).filter(|entry| !entry.deleted) {
                if let Some(value) = entry.value.as_deref() {
                    function(key, entry.flags, value);
                }
            }
        }
    }

    pub(crate) fn iter(&self, lower: Option<&[u8]>, upper: Option<&[u8]>) -> ArtIterator {
        ArtIterator::new(self, lower, upper, false, false)
    }

    pub(crate) fn iter_reverse(&self, upper: Option<&[u8]>, lower: Option<&[u8]>) -> ArtIterator {
        ArtIterator::new(self, lower, upper, true, false)
    }

    pub(crate) fn iter_with_flags(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> ArtIterator {
        ArtIterator::new(self, lower, upper, false, true)
    }

    pub(crate) fn iter_reverse_with_flags(&self, upper: Option<&[u8]>) -> ArtIterator {
        ArtIterator::new(self, None, upper, true, true)
    }

    pub(crate) fn snapshot(&self) -> ArtSnapshot {
        let mut entries = self.entries.clone();
        if let Some(stage) = self.stages.first() {
            for undo in self.undo[stage.undo_start..].iter().rev() {
                if let Some(entry) = entries.get_mut(&undo.key) {
                    match &undo.old_value {
                        Some(value) => {
                            entry.value = value.clone();
                            entry.history.truncate(undo.old_history_len);
                            entry.value_log_undo_index = undo.old_value_log_undo_index;
                            entry.deleted = undo.old_deleted;
                        }
                        None => {
                            entries.remove(&undo.key);
                        }
                    }
                }
            }
        }
        ArtSnapshot { entries }
    }

    /// Removes a record completely from the test buffer.
    ///
    /// Pinned client-go's ART still panics here, although its RBT backend and
    /// public `MemBuffer` contract implement the operation. Rust keeps ART as
    /// the source-default backend and supplies the missing test-only behavior.
    pub(crate) fn remove_from_buffer(&mut self, key: &[u8]) {
        assert!(!self.values_discarded, "vlog is resetted");
        self.bump_write_sequence();
        let Some(entry) = self.entries.remove(key) else {
            return;
        };
        self.handles.remove(&entry.handle);
        self.logical_size = self.logical_size.saturating_sub(contribution(
            key.len(),
            entry.value.as_deref(),
            entry.deleted,
        ));
        if self.last_key.as_deref() == Some(key) {
            self.last_key = None;
        }
        self.notify_memory_change();
    }
}

type IteratorItem = (Vec<u8>, Option<Vec<u8>>, KeyFlags, ArtHandle);

pub(crate) struct ArtIterator {
    items: Vec<IteratorItem>,
    index: usize,
    expected_sequence: u64,
    write_sequence: Arc<AtomicU64>,
}

impl ArtIterator {
    fn new(
        tree: &Art,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
        include_flags: bool,
    ) -> Self {
        // ART's Go implementation uses `len(bound) == 0` to mean unbounded,
        // unlike RBT which distinguishes a non-nil empty upper bound.
        let lower = lower.filter(|bound| !bound.is_empty());
        let upper = upper.filter(|bound| !bound.is_empty());
        let mut items: Vec<_> = tree
            .entries
            .iter()
            .filter(|(key, entry)| {
                !entry.deleted
                    && lower.is_none_or(|lower| key.as_slice() >= lower)
                    && upper.is_none_or(|upper| key.as_slice() < upper)
                    && (include_flags || entry.value.is_some())
            })
            .map(|(key, entry)| (key.clone(), entry.value.clone(), entry.flags, entry.handle))
            .collect();
        if reverse {
            items.reverse();
        }
        Self {
            items,
            index: 0,
            expected_sequence: tree.write_sequence(),
            write_sequence: tree.write_sequence.clone(),
        }
    }

    fn check_sequence(&self) {
        assert_eq!(
            self.expected_sequence,
            self.write_sequence.load(Ordering::Acquire),
            "ART iterator invalidated by write"
        );
    }

    pub(crate) fn valid(&self) -> bool {
        self.check_sequence();
        self.index < self.items.len()
    }
    pub(crate) fn key(&self) -> &[u8] {
        self.check_sequence();
        &self.items[self.index].0
    }
    pub(crate) fn value(&self) -> Option<&[u8]> {
        self.check_sequence();
        self.items[self.index].1.as_deref()
    }
    pub(crate) fn flags(&self) -> KeyFlags {
        self.check_sequence();
        self.items[self.index].2
    }
    pub(crate) fn handle(&self) -> ArtHandle {
        self.check_sequence();
        self.items[self.index].3
    }
    pub(crate) fn has_value(&self) -> bool {
        self.value().is_some()
    }

    pub(crate) fn next(&mut self) -> Result<(), &'static str> {
        self.check_sequence();
        if self.index >= self.items.len() {
            return Err("Art: iterator is finished");
        }
        self.index += 1;
        Ok(())
    }

    pub(crate) fn close(self) {}
}

#[derive(Clone)]
pub(crate) struct ArtSnapshot {
    entries: BTreeMap<Vec<u8>, Entry>,
}

impl ArtSnapshot {
    pub(crate) fn get(&self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        self.entries
            .get(key)
            .filter(|entry| !entry.deleted)
            .and_then(|entry| entry.value.clone())
            .ok_or(StaticError::NotExist)
    }

    pub(crate) fn iter(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
    ) -> SnapshotIterator {
        let lower = lower.filter(|bound| !bound.is_empty());
        let upper = upper.filter(|bound| !bound.is_empty());
        let mut items: Vec<_> = self
            .entries
            .iter()
            .filter(|(key, entry)| {
                !entry.deleted
                    && entry.value.is_some()
                    && lower.is_none_or(|lower| key.as_slice() >= lower)
                    && upper.is_none_or(|upper| key.as_slice() < upper)
            })
            .map(|(key, entry)| (key.clone(), entry.value.clone().unwrap()))
            .collect();
        if reverse {
            items.reverse();
        }
        SnapshotIterator { items, index: 0 }
    }

    pub(crate) fn close(self) {}
}

pub(crate) struct SnapshotIterator {
    items: Vec<(Vec<u8>, Vec<u8>)>,
    index: usize,
}

impl SnapshotIterator {
    pub(crate) fn valid(&self) -> bool {
        self.index < self.items.len()
    }
    pub(crate) fn key(&self) -> &[u8] {
        &self.items[self.index].0
    }
    pub(crate) fn value(&self) -> &[u8] {
        &self.items[self.index].1
    }
    pub(crate) fn next(&mut self) -> Result<(), &'static str> {
        if !self.valid() {
            return Err("Art: iterator is finished");
        }
        self.index += 1;
        Ok(())
    }

    pub(crate) fn close(self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_key_shapes_node_capacities_and_bounds_are_ordered() {
        let mut tree = Art::new();
        for i in 0..256u16 {
            let key = [i as u8];
            tree.set(&key, Some(&key), &[]).unwrap();
            assert_eq!(tree.get(&key).unwrap(), key);
        }
        for capacity in [4usize, 16, 48, 256] {
            let mut forward = tree.iter(Some(&[0]), Some(&[capacity as u8]));
            for value in 0..capacity as u8 {
                assert!(forward.valid());
                assert_eq!(forward.key(), [value]);
                forward.next().unwrap();
            }
            assert!(!forward.valid());
            assert!(forward.next().is_err());
        }
        let mut empty_bounds = tree.iter(Some(b""), Some(b""));
        assert_eq!(empty_bounds.key(), [0]);
        empty_bounds.next().unwrap();
        assert_eq!(empty_bounds.key(), [1]);
        let mut reverse = tree.iter_reverse(Some(&[8]), Some(&[4]));
        for value in (4..8).rev() {
            assert_eq!(reverse.key(), [value]);
            reverse.next().unwrap();
        }
        assert!(!reverse.valid());
    }

    #[test]
    fn source_prefix_flags_handles_and_discard_contracts_hold() {
        let mut tree = Art::new();
        for key in [b"a".as_slice(), b"aa", b"aaa", &[1, 1, 1], &[1, 1, 2]] {
            tree.set(key, Some(key), &[]).unwrap();
            assert_eq!(tree.get(key).unwrap(), key);
        }
        tree.set(b"locked", Some(b"v"), &[FlagsOp::SetKeyLocked])
            .unwrap();
        tree.set(b"flag-only", None, &[FlagsOp::SetAssertNone])
            .unwrap();
        assert!(tree.flags(b"locked").unwrap().has_locked());
        assert!(!tree.flags(b"flag-only").unwrap().has_assertion_flags());
        assert!(tree.get(b"flag-only").is_err());
        tree.update_flags(b"locked", &[FlagsOp::DelKeyLocked]);
        assert!(!tree.flags(b"locked").unwrap().has_locked());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tree.update_flags(b"missing", &[FlagsOp::SetKeyLocked])
        }))
        .is_err());
        let handle = tree.iter_with_flags(None, None).handle();
        let key = tree.key_by_handle(handle).unwrap().to_vec();
        let value = tree.get(&key).unwrap();
        assert_eq!(tree.value_by_handle(handle), Some(value.as_slice()));
        let size = tree.size();
        tree.discard_values();
        assert_eq!(tree.size(), size);
        assert!(tree.value_by_handle(handle).is_none());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tree.get(b"a"))).is_err());
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| tree.set(
                b"x",
                Some(b"y"),
                &[]
            )))
            .is_err()
        );
    }

    #[test]
    fn staging_history_persistent_flags_and_handles_match_art() {
        let mut tree = Art::new();
        let stage = tree.staging();
        tree.set(b"ephemeral", Some(b"one"), &[]).unwrap();
        tree.set(b"persistent", Some(b"two"), &[FlagsOp::SetKeyLocked])
            .unwrap();
        let ephemeral = tree
            .iter_with_flags(Some(b"ephemeral"), Some(b"f"))
            .handle();
        tree.cleanup(stage);
        assert!(tree.get(b"ephemeral").is_err());
        assert_eq!(tree.key_by_handle(ephemeral), Some(&b"ephemeral"[..]));
        assert!(tree.value_by_handle(ephemeral).is_none());
        assert!(tree.flags(b"ephemeral").is_err());
        assert!(tree.flags(b"persistent").unwrap().has_locked());
        assert_eq!(tree.len(), 1);

        let non_persistent = tree.staging();
        tree.set(
            b"non-persistent",
            Some(b"value"),
            &[FlagsOp::SetPresumeKeyNotExists],
        )
        .unwrap();
        tree.cleanup(non_persistent);
        tree.set(b"non-persistent", Some(b"new"), &[]).unwrap();
        assert!(!tree
            .flags(b"non-persistent")
            .unwrap()
            .has_presume_key_not_exists());

        let root = tree.staging();
        tree.set(b"history", Some(b"one"), &[]).unwrap();
        let checkpoint = tree.checkpoint();
        tree.set(b"history", Some(b"two"), &[]).unwrap();
        assert_eq!(
            tree.select_value_history(b"history", |v| v == b"one")
                .unwrap(),
            None
        );
        tree.set(b"history", Some(b"three"), &[]).unwrap();
        assert_eq!(
            tree.select_value_history(b"history", |v| v == b"two")
                .unwrap(),
            Some(b"two".to_vec())
        );
        tree.revert_to_checkpoint(checkpoint);
        assert_eq!(tree.get(b"history").unwrap(), b"two");
        tree.cleanup(root);
    }

    #[test]
    fn ordinary_iterators_invalidate_but_snapshots_remain_stable() {
        let mut tree = Art::new();
        tree.set(b"a", Some(b"one"), &[]).unwrap();
        let snapshot = tree.snapshot();
        let iterator = tree.iter(None, None);
        tree.set(b"b", Some(b"two"), &[]).unwrap();
        assert!(std::panic::catch_unwind(|| iterator.valid()).is_err());
        assert_eq!(snapshot.get(b"a").unwrap(), b"one");
        assert!(snapshot.get(b"b").is_err());
        let mut snapshot_iterator = snapshot.iter(None, None, false);
        assert_eq!(snapshot_iterator.key(), b"a");
        snapshot_iterator.next().unwrap();
        assert!(!snapshot_iterator.valid());
    }

    #[test]
    fn limits_hooks_cache_stage_inspection_and_reset_are_source_compatible() {
        let mut tree = Art::new();
        let observed = Arc::new(AtomicU64::new(0));
        let result = observed.clone();
        tree.set_memory_footprint_change_hook(Arc::new(move |memory| {
            result.store(memory, Ordering::Release);
        }));
        tree.set_entry_size_limit(2, u64::MAX);
        assert!(tree.set(b"ab", Some(b"c"), &[]).is_err());
        tree.set_entry_size_limit(u64::MAX, 1);
        assert!(tree.set(b"a", Some(b"b"), &[]).is_err());
        tree.set_entry_size_limit(u64::MAX, u64::MAX);
        let stage = tree.staging();
        tree.set(b"a", Some(b"b"), &[]).unwrap();
        let mut inspected = Vec::new();
        tree.inspect_stage(stage, |key, _, value| {
            inspected.push((key.to_vec(), value.to_vec()))
        });
        assert_eq!(inspected, vec![(b"a".to_vec(), b"b".to_vec())]);
        let _ = tree.get(b"a");
        let _ = tree.get(b"a");
        assert!(tree.cache_hit_count() > 0);
        assert!(tree.cache_miss_count() > 0);
        assert!(tree.memory_hook_is_set());
        assert!(observed.load(Ordering::Acquire) > 0);
        let snapshot_sequence = tree.snapshot_sequence();
        tree.release(stage);
        assert!(tree.snapshot_sequence() > snapshot_sequence);
        tree.reset();
        assert_eq!(tree.len(), 0);
        assert!(!tree.dirty());
    }

    #[test]
    fn hundred_thousand_decimal_keys_and_long_common_prefixes_are_retrievable() {
        let mut tree = Art::new();
        for number in 0..100_000 {
            let key = format!("{number:010}").into_bytes();
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        for number in [0, 1, 9_999, 42_000, 99_999] {
            let key = format!("{number:010}").into_bytes();
            assert_eq!(tree.get(&key).unwrap(), key);
        }
        let prefix = vec![0; 64];
        let mut first = prefix.clone();
        first.push(1);
        let mut second = prefix;
        second.push(2);
        tree.set(&first, Some(b"one"), &[]).unwrap();
        tree.set(&second, Some(b"two"), &[]).unwrap();
        assert_eq!(tree.get(&first).unwrap(), b"one");
        assert_eq!(tree.get(&second).unwrap(), b"two");
    }

    #[test]
    fn snapshot_iterators_are_stable_and_shareable_after_tree_writes() {
        let mut tree = Art::new();
        for value in 0..48u8 {
            let key = [0, value];
            tree.set(&key, Some(&key), &[]).unwrap();
        }
        let snapshot = Arc::new(tree.snapshot());
        tree.set(&[0, 48], Some(&[0, 48]), &[]).unwrap();
        let mut joins = Vec::new();
        for _ in 0..100 {
            let snapshot = snapshot.clone();
            joins.push(std::thread::spawn(move || {
                let mut iterator = snapshot.iter(None, None, false);
                let mut count = 0;
                while iterator.valid() {
                    assert_eq!(iterator.key(), iterator.value());
                    iterator.next().unwrap();
                    count += 1;
                }
                count
            }));
        }
        for join in joins {
            assert_eq!(join.join().unwrap(), 48);
        }
    }
}
