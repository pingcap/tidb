//! Native core of client-go's `internal/unionstore` package.
//!
//! This layer adapts the rollbackable ART index to MemBuffer-style set/delete
//! operations and merges it with an immutable snapshot. Pipelined flushing
//! and batched snapshots build on these owned iterator primitives.

use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fmt;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc, Arc,
};
use std::time::{Duration, Instant};

use crate::error::{KeyExistsError, StaticError};
use crate::kv::{FlagsOp, GetOption, KeyFlags, ValueEntry};

use super::art::{Art, ArtIterator, ArtSnapshot, SnapshotIterator};
use super::rbt::{Rbt, RbtIterator, RbtSnapshot};

/// Source-compatible tombstone predicate: an empty value deletes a key from a
/// union view while remaining visible in its mutation buffer.
pub const fn is_tombstone(value: &[u8]) -> bool {
    value.is_empty()
}

pub trait KvIterator {
    fn valid(&self) -> bool;
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
    fn next(&mut self) -> Result<(), &'static str>;

    /// Rust iterators own their backing data, so dropping one releases it.
    /// This no-op preserves client-go's explicit `Iterator.Close` surface.
    fn close(&mut self) {}
}

struct ArtBufferIterator(ArtIterator);

impl KvIterator for ArtBufferIterator {
    fn valid(&self) -> bool {
        self.0.valid()
    }

    fn key(&self) -> &[u8] {
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        self.0.value().unwrap_or_default()
    }

    fn next(&mut self) -> Result<(), &'static str> {
        self.0.next()
    }
}

struct ArtSnapshotBufferIterator(SnapshotIterator);

impl KvIterator for ArtSnapshotBufferIterator {
    fn valid(&self) -> bool {
        self.0.valid()
    }

    fn key(&self) -> &[u8] {
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        self.0.value()
    }

    fn next(&mut self) -> Result<(), &'static str> {
        self.0.next()
    }
}

struct VecIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    index: usize,
}

impl VecIterator {
    fn new(entries: impl Iterator<Item = (Vec<u8>, Vec<u8>)>, reverse: bool) -> Self {
        let mut entries: Vec<_> = entries.collect();
        if reverse {
            entries.reverse();
        }
        Self { entries, index: 0 }
    }
}

impl KvIterator for VecIterator {
    fn valid(&self) -> bool {
        self.index < self.entries.len()
    }

    fn key(&self) -> &[u8] {
        &self.entries[self.index].0
    }

    fn value(&self) -> &[u8] {
        &self.entries[self.index].1
    }

    fn next(&mut self) -> Result<(), &'static str> {
        if !self.valid() {
            return Err("iterator is finished");
        }
        self.index += 1;
        Ok(())
    }
}

/// Iterator returned by source-compatible unsupported snapshot operations on
/// `PipelinedMemDB`: it is initially valid and reports its stored error from
/// `next`, exactly like client-go's `errIterator`.
struct ErrorIterator {
    error: &'static str,
}

impl KvIterator for ErrorIterator {
    fn valid(&self) -> bool {
        true
    }

    fn key(&self) -> &[u8] {
        &[]
    }

    fn value(&self) -> &[u8] {
        &[]
    }

    fn next(&mut self) -> Result<(), &'static str> {
        Err(self.error)
    }
}

/// MemDB-facing adapter for the source-default ART index.
pub struct MemDb {
    art: Art,
}

impl Default for MemDb {
    fn default() -> Self {
        Self::new()
    }
}

impl MemDb {
    pub fn new() -> Self {
        Self { art: Art::new() }
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        self.art.get(key)
    }

    /// MemBuffer implements `Getter`, but client-go guarantees the local
    /// buffer never supplies a commit timestamp, regardless of read options.
    pub fn get_entry(&mut self, key: &[u8], _: &[GetOption]) -> Result<ValueEntry, StaticError> {
        self.get(key).map(|value| ValueEntry::new(value, 0))
    }

    pub fn get_readonly(&self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        self.art.get_readonly(key)
    }

    /// Source `BatchGet`: absent keys are omitted, and an empty buffer avoids
    /// per-key lookups entirely.
    pub fn batch_get(&mut self, keys: &[Vec<u8>]) -> BTreeMap<Vec<u8>, Vec<u8>> {
        if self.len() == 0 {
            return BTreeMap::new();
        }
        keys.iter()
            .filter_map(|key| self.get(key).ok().map(|value| (key.clone(), value)))
            .collect()
    }

    pub fn batch_get_entries(
        &mut self,
        keys: &[Vec<u8>],
        _: &[GetOption],
    ) -> BTreeMap<Vec<u8>, ValueEntry> {
        self.batch_get(keys)
            .into_iter()
            .map(|(key, value)| (key, ValueEntry::new(value, 0)))
            .collect()
    }

    pub fn get_flags(&mut self, key: &[u8]) -> Result<KeyFlags, StaticError> {
        self.art.flags(key)
    }

    pub fn get_flags_readonly(&self, key: &[u8]) -> Result<KeyFlags, StaticError> {
        self.art.flags_readonly(key)
    }

    pub fn set(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if value.is_empty() {
            return Err(Box::new(StaticError::CannotSetNilValue));
        }
        self.art.set(key, Some(value), &[])
    }

    pub fn set_with_flags(
        &mut self,
        key: &[u8],
        value: &[u8],
        operations: &[FlagsOp],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if value.is_empty() {
            return Err(Box::new(StaticError::CannotSetNilValue));
        }
        self.art.set(key, Some(value), operations)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.art.set(key, Some(&[]), &[])
    }

    pub fn delete_with_flags(
        &mut self,
        key: &[u8],
        operations: &[FlagsOp],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.art.set(key, Some(&[]), operations)
    }

    pub fn update_flags(&mut self, key: &[u8], operations: &[FlagsOp]) {
        self.art.set(key, None, operations).unwrap();
    }

    pub fn iter(&self, lower: Option<&[u8]>, upper: Option<&[u8]>) -> Box<dyn KvIterator> {
        Box::new(ArtBufferIterator(self.art.iter(lower, upper)))
    }

    pub fn iter_reverse(&self, upper: Option<&[u8]>, lower: Option<&[u8]>) -> Box<dyn KvIterator> {
        Box::new(ArtBufferIterator(self.art.iter_reverse(upper, lower)))
    }

    pub fn staging(&mut self) -> usize {
        self.art.staging()
    }

    pub fn cleanup(&mut self, handle: usize) {
        self.art.cleanup(handle);
    }

    pub fn release(&mut self, handle: usize) {
        self.art.release(handle);
    }

    pub fn snapshot(&self) -> MemDbSnapshot {
        MemDbSnapshot {
            snapshot: self.art.snapshot(),
            expected_sequence: self.art.snapshot_sequence(),
            sequence: self.art.snapshot_sequence_counter(),
        }
    }

    /// Deprecated source `SnapshotGetter` mapping; callers receive an owned,
    /// validity-checked snapshot rather than a borrowed getter.
    pub fn snapshot_getter(&self) -> MemDbSnapshot {
        self.snapshot()
    }

    pub fn get_memdb(&mut self) -> &mut Self {
        self
    }

    pub fn snapshot_iter(
        &mut self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Box<dyn KvIterator> {
        self.snapshot().iter(lower, upper, false)
    }

    pub fn snapshot_iter_reverse(
        &self,
        upper: Option<&[u8]>,
        lower: Option<&[u8]>,
    ) -> Box<dyn KvIterator> {
        self.snapshot().iter(lower, upper, true)
    }

    pub fn batched_snapshot_iter(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
    ) -> BatchedSnapshotIterator {
        self.snapshot().batched_iter(lower, upper, reverse)
    }

    pub fn for_each_in_snapshot_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
        function: impl FnMut(&[u8], &[u8]) -> Result<bool, &'static str>,
    ) -> Result<(), &'static str> {
        self.snapshot().for_each(lower, upper, reverse, function)
    }

    pub fn len(&self) -> usize {
        self.art.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size(&self) -> usize {
        self.art.size()
    }

    pub fn dirty(&self) -> bool {
        self.art.dirty()
    }

    pub fn cache_hit_count(&self) -> u64 {
        self.art.cache_hit_count()
    }

    pub fn cache_miss_count(&self) -> u64 {
        self.art.cache_miss_count()
    }

    pub fn is_staging(&self) -> bool {
        self.art.is_staging()
    }

    pub fn set_entry_size_limit(&mut self, entry_limit: u64, buffer_limit: u64) {
        self.art.set_entry_size_limit(entry_limit, buffer_limit);
    }

    pub fn checkpoint(&self) -> usize {
        self.art.checkpoint()
    }

    pub fn revert_to_checkpoint(&mut self, checkpoint: usize) {
        self.art.revert_to_checkpoint(checkpoint);
    }

    pub fn inspect_stage(&self, handle: usize, function: impl FnMut(&[u8], KeyFlags, &[u8])) {
        self.art.inspect_stage(handle, function);
    }

    /// Removes a record completely from the test buffer.
    pub fn remove_from_buffer(&mut self, key: &[u8]) {
        self.art.remove_from_buffer(key)
    }

    pub fn set_memory_footprint_change_hook(&mut self, hook: Arc<dyn Fn(u64) + Send + Sync>) {
        self.art.set_memory_footprint_change_hook(hook);
    }

    pub fn memory_hook_is_set(&self) -> bool {
        self.art.memory_hook_is_set()
    }

    pub fn memory_footprint(&self) -> u64 {
        self.art.memory_footprint()
    }

    pub fn reset(&mut self) {
        self.art.reset();
    }

    pub fn select_value_history(
        &mut self,
        key: &[u8],
        predicate: impl FnMut(&[u8]) -> bool,
    ) -> Result<Option<Vec<u8>>, StaticError> {
        self.art.select_value_history(key, predicate)
    }

    pub fn flush(&self, _: bool) -> Result<bool, String> {
        Ok(false)
    }

    pub fn flush_wait(&self) -> Result<(), String> {
        Ok(())
    }

    pub fn metrics(&self) -> PipelinedMetrics {
        PipelinedMetrics {
            flush_wait_duration: Duration::ZERO,
            total_duration: Duration::ZERO,
            memdb_hit_count: 0,
            memdb_miss_count: 0,
        }
    }
}

pub struct MemDbSnapshot {
    snapshot: ArtSnapshot,
    expected_sequence: u64,
    sequence: Arc<AtomicU64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotError {
    NotExist,
    Invalidated,
}

impl From<StaticError> for SnapshotError {
    fn from(_: StaticError) -> Self {
        Self::NotExist
    }
}

impl MemDbSnapshot {
    fn check_sequence(&self) -> Result<(), &'static str> {
        if self.sequence.load(Ordering::Acquire) == self.expected_sequence {
            Ok(())
        } else {
            Err("invalid snapshot: snapshot sequence changed")
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, SnapshotError> {
        self.check_sequence()
            .map_err(|_| SnapshotError::Invalidated)?;
        self.snapshot.get(key).map_err(SnapshotError::from)
    }

    pub fn get_entry(&self, key: &[u8], _: &[GetOption]) -> Result<ValueEntry, SnapshotError> {
        self.get(key).map(|value| ValueEntry::new(value, 0))
    }

    pub fn iter(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
    ) -> Box<dyn KvIterator> {
        Box::new(ArtSnapshotBufferIterator(
            self.snapshot.iter(lower, upper, reverse),
        ))
    }

    pub fn batched_iter(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
    ) -> BatchedSnapshotIterator {
        BatchedSnapshotIterator {
            inner: self.snapshot.iter(lower, upper, reverse),
            expected_sequence: self.expected_sequence,
            sequence: self.sequence.clone(),
        }
    }

    pub fn for_each(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
        mut function: impl FnMut(&[u8], &[u8]) -> Result<bool, &'static str>,
    ) -> Result<(), &'static str> {
        // Match `SnapshotWithMutex.ForEachInSnapshotRange`: this direct
        // traversal holds an owned immutable view, whereas only getter and
        // batched iterator operations check the source snapshot sequence.
        let mut iterator = self.snapshot.iter(lower, upper, reverse);
        while iterator.valid() {
            if function(iterator.key(), iterator.value())? {
                return Ok(());
            }
            iterator.next()?;
        }
        Ok(())
    }

    /// Source snapshots release resources on `Close`; this owned Rust view has
    /// no external allocation to release before `Drop`.
    pub fn close(self) {}
}

struct RbtBufferIterator(RbtIterator);

impl KvIterator for RbtBufferIterator {
    fn valid(&self) -> bool {
        self.0.valid()
    }

    fn key(&self) -> &[u8] {
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        self.0.value().unwrap_or_default()
    }

    fn next(&mut self) -> Result<(), &'static str> {
        if !self.0.valid() {
            return Err("iterator is finished");
        }
        self.0.next();
        Ok(())
    }
}

/// Source-compatible wrapper for the optional RBT MemBuffer implementation.
/// Unlike ART, its native iterator is intentionally a stable owned traversal
/// view and its snapshots do not use a sequence-number invalidation check.
pub struct RbtMemDb {
    rbt: Rbt,
}

impl Default for RbtMemDb {
    fn default() -> Self {
        Self::new()
    }
}

impl RbtMemDb {
    pub fn new() -> Self {
        Self { rbt: Rbt::new() }
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        self.rbt.get(key)
    }

    pub fn get_entry(&mut self, key: &[u8], _: &[GetOption]) -> Result<ValueEntry, StaticError> {
        self.get(key).map(|value| ValueEntry::new(value, 0))
    }

    pub fn batch_get(&mut self, keys: &[Vec<u8>]) -> BTreeMap<Vec<u8>, Vec<u8>> {
        if self.len() == 0 {
            return BTreeMap::new();
        }
        keys.iter()
            .filter_map(|key| self.get(key).ok().map(|value| (key.clone(), value)))
            .collect()
    }

    pub fn batch_get_entries(
        &mut self,
        keys: &[Vec<u8>],
        _: &[GetOption],
    ) -> BTreeMap<Vec<u8>, ValueEntry> {
        self.batch_get(keys)
            .into_iter()
            .map(|(key, value)| (key, ValueEntry::new(value, 0)))
            .collect()
    }

    pub fn get_flags(&mut self, key: &[u8]) -> Result<KeyFlags, StaticError> {
        self.rbt.flags(key)
    }

    pub fn set(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if value.is_empty() {
            return Err(Box::new(StaticError::CannotSetNilValue));
        }
        self.rbt.set(key, Some(value), &[])
    }

    pub fn set_with_flags(
        &mut self,
        key: &[u8],
        value: &[u8],
        flags: &[FlagsOp],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if value.is_empty() {
            return Err(Box::new(StaticError::CannotSetNilValue));
        }
        self.rbt.set(key, Some(value), flags)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.rbt.set(key, Some(&[]), &[])
    }

    pub fn delete_with_flags(
        &mut self,
        key: &[u8],
        flags: &[FlagsOp],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.rbt.set(key, Some(&[]), flags)
    }

    pub fn update_flags(&mut self, key: &[u8], flags: &[FlagsOp]) {
        self.rbt.update_flags(key, flags);
    }

    pub fn iter(&self, lower: Option<&[u8]>, upper: Option<&[u8]>) -> Box<dyn KvIterator> {
        Box::new(RbtBufferIterator(self.rbt.iter(lower, upper)))
    }

    pub fn iter_reverse(&self, upper: Option<&[u8]>, lower: Option<&[u8]>) -> Box<dyn KvIterator> {
        Box::new(RbtBufferIterator(self.rbt.iter_reverse(upper, lower)))
    }

    pub fn staging(&mut self) -> usize {
        self.rbt.staging()
    }

    pub fn cleanup(&mut self, handle: usize) {
        self.rbt.cleanup(handle);
    }

    pub fn release(&mut self, handle: usize) {
        self.rbt.release(handle);
    }

    pub fn snapshot(&self) -> RbtMemDbSnapshot {
        RbtMemDbSnapshot {
            snapshot: self.rbt.snapshot(),
        }
    }

    pub fn snapshot_getter(&self) -> RbtMemDbSnapshot {
        self.snapshot()
    }

    /// Client-go's optional RBT wrapper returns a nil `*MemDB` here.
    pub fn get_memdb(&mut self) -> Option<&mut MemDb> {
        None
    }

    pub fn snapshot_iter(&self, lower: Option<&[u8]>, upper: Option<&[u8]>) -> Box<dyn KvIterator> {
        self.snapshot().iter(lower, upper, false)
    }

    pub fn snapshot_iter_reverse(
        &self,
        upper: Option<&[u8]>,
        lower: Option<&[u8]>,
    ) -> Box<dyn KvIterator> {
        self.snapshot().iter(lower, upper, true)
    }

    pub fn batched_snapshot_iter(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
    ) -> Box<dyn KvIterator> {
        self.snapshot().iter(lower, upper, reverse)
    }

    pub fn for_each_in_snapshot_range(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
        mut function: impl FnMut(&[u8], &[u8]) -> Result<bool, &'static str>,
    ) -> Result<(), &'static str> {
        let mut iterator = self.snapshot().iter(lower, upper, reverse);
        while iterator.valid() {
            if function(iterator.key(), iterator.value())? {
                return Ok(());
            }
            iterator.next()?;
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.rbt.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size(&self) -> usize {
        self.rbt.size()
    }

    pub fn dirty(&self) -> bool {
        self.rbt.dirty()
    }

    pub fn set_entry_size_limit(&mut self, entry_limit: u64, buffer_limit: u64) {
        self.rbt.set_entry_size_limit(entry_limit, buffer_limit);
    }

    pub fn checkpoint(&self) -> usize {
        self.rbt.checkpoint()
    }

    pub fn revert_to_checkpoint(&mut self, checkpoint: usize) {
        self.rbt.revert_to_checkpoint(checkpoint);
    }

    pub fn inspect_stage(&self, handle: usize, function: impl FnMut(&[u8], KeyFlags, &[u8])) {
        self.rbt.inspect_stage(handle, function);
    }

    pub fn remove_from_buffer(&mut self, key: &[u8]) {
        self.rbt.remove_from_buffer(key);
    }

    pub fn set_memory_footprint_change_hook(&mut self, hook: Arc<dyn Fn(u64) + Send + Sync>) {
        self.rbt.set_memory_footprint_change_hook(hook);
    }

    pub fn memory_hook_is_set(&self) -> bool {
        self.rbt.memory_hook_is_set()
    }

    pub fn memory_footprint(&self) -> u64 {
        self.rbt.memory_footprint()
    }

    pub fn reset(&mut self) {
        self.rbt.reset();
    }

    pub fn select_value_history(
        &mut self,
        key: &[u8],
        predicate: impl FnMut(&[u8]) -> bool,
    ) -> Result<Option<Vec<u8>>, StaticError> {
        self.rbt.select_value_history(key, predicate)
    }

    pub fn flush(&self, _: bool) -> Result<bool, String> {
        Ok(false)
    }

    pub fn flush_wait(&self) -> Result<(), String> {
        Ok(())
    }

    pub fn metrics(&self) -> PipelinedMetrics {
        PipelinedMetrics {
            flush_wait_duration: Duration::ZERO,
            total_duration: Duration::ZERO,
            memdb_hit_count: 0,
            memdb_miss_count: 0,
        }
    }
}

pub struct RbtMemDbSnapshot {
    snapshot: RbtSnapshot,
}

impl RbtMemDbSnapshot {
    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        self.snapshot.get(key)
    }

    pub fn get_entry(&self, key: &[u8], _: &[GetOption]) -> Result<ValueEntry, StaticError> {
        self.get(key).map(|value| ValueEntry::new(value, 0))
    }

    pub fn iter(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
    ) -> Box<dyn KvIterator> {
        let iterator = if reverse {
            self.snapshot.iter_reverse(upper, lower)
        } else {
            self.snapshot.iter(lower, upper)
        };
        Box::new(RbtBufferIterator(iterator))
    }

    pub fn close(self) {}
}

/// Native stable-view mapping of client-go's growing snapshot batch iterator.
/// The immutable snapshot is already owned, so it does not need to rebuild an
/// underlying unsafe ART iterator between batches; its validity still follows
/// client-go's stage-0 sequence contract.
pub struct BatchedSnapshotIterator {
    inner: SnapshotIterator,
    expected_sequence: u64,
    sequence: Arc<AtomicU64>,
}

impl BatchedSnapshotIterator {
    fn check_sequence(&self) -> Result<(), &'static str> {
        if self.sequence.load(Ordering::Acquire) == self.expected_sequence {
            Ok(())
        } else {
            Err("invalid snapshot: snapshot sequence changed")
        }
    }

    pub fn valid(&self) -> bool {
        self.check_sequence().is_ok() && self.inner.valid()
    }

    pub fn key(&self) -> &[u8] {
        self.inner.key()
    }

    pub fn value(&self) -> &[u8] {
        self.inner.value()
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<(), &'static str> {
        self.check_sequence()?;
        self.inner.next()
    }
}

/// Immutable in-process snapshot used by the native union-store adapter.
#[derive(Clone, Default)]
pub struct MapSnapshot {
    entries: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl MapSnapshot {
    pub fn insert(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.entries.insert(key.into(), value.into());
    }

    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        self.entries.get(key).cloned().ok_or(StaticError::NotExist)
    }

    /// Native form of client-go's `mockSnapshot.Get`: the test snapshot only
    /// fabricates a commit timestamp when callers explicitly request it.
    pub fn get_entry(&self, key: &[u8], options: &[GetOption]) -> Result<ValueEntry, StaticError> {
        let mut entry = ValueEntry::new(self.get(key)?, 0);
        if options.contains(&GetOption::ReturnCommitTs) {
            entry.commit_ts = 1;
            if let Some(first) = key.first() {
                entry.commit_ts = 1000 + u64::from(*first);
            }
        }
        Ok(entry)
    }

    /// Source mock snapshot batch reads omit missing keys.
    pub fn batch_get(&self, keys: &[Vec<u8>]) -> BTreeMap<Vec<u8>, Vec<u8>> {
        keys.iter()
            .filter_map(|key| self.get(key).ok().map(|value| (key.clone(), value)))
            .collect()
    }

    pub fn batch_get_entries(
        &self,
        keys: &[Vec<u8>],
        options: &[GetOption],
    ) -> BTreeMap<Vec<u8>, ValueEntry> {
        keys.iter()
            .filter_map(|key| {
                self.get_entry(key, options)
                    .ok()
                    .map(|entry| (key.clone(), entry))
            })
            .collect()
    }

    pub fn iter(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
    ) -> Box<dyn KvIterator> {
        let lower = lower.unwrap_or_default();
        let entries = self
            .entries
            .iter()
            .filter(|(key, _)| {
                key.as_slice() >= lower && upper.is_none_or(|end| key.as_slice() < end)
            })
            .map(|(key, value)| (key.clone(), value.clone()));
        Box::new(VecIterator::new(entries, reverse))
    }
}

/// A merge iterator where MemDB updates override snapshot values and empty
/// values are tombstones.
pub struct UnionIterator {
    dirty: Box<dyn KvIterator>,
    snapshot: Box<dyn KvIterator>,
    dirty_valid: bool,
    snapshot_valid: bool,
    current_is_dirty: bool,
    valid: bool,
    reverse: bool,
}

impl UnionIterator {
    pub fn new(
        dirty: Box<dyn KvIterator>,
        snapshot: Box<dyn KvIterator>,
        reverse: bool,
    ) -> Result<Self, &'static str> {
        let mut iterator = Self {
            dirty_valid: dirty.valid(),
            snapshot_valid: snapshot.valid(),
            dirty,
            snapshot,
            current_is_dirty: false,
            valid: false,
            reverse,
        };
        iterator.update_current()?;
        Ok(iterator)
    }

    fn dirty_next(&mut self) -> Result<(), &'static str> {
        self.dirty.next()?;
        self.dirty_valid = self.dirty.valid();
        Ok(())
    }

    fn snapshot_next(&mut self) -> Result<(), &'static str> {
        self.snapshot.next()?;
        self.snapshot_valid = self.snapshot.valid();
        Ok(())
    }

    fn update_current(&mut self) -> Result<(), &'static str> {
        loop {
            self.valid = true;
            match (self.dirty_valid, self.snapshot_valid) {
                (false, false) => {
                    self.valid = false;
                    return Ok(());
                }
                (false, true) => {
                    self.current_is_dirty = false;
                    return Ok(());
                }
                (true, false) => {
                    if is_tombstone(self.dirty.value()) {
                        self.dirty_next()?;
                        continue;
                    }
                    self.current_is_dirty = true;
                    return Ok(());
                }
                (true, true) => {
                    let mut ordering = self.dirty.key().cmp(self.snapshot.key());
                    if self.reverse {
                        ordering = ordering.reverse();
                    }
                    match ordering {
                        std::cmp::Ordering::Equal => {
                            let deleted = is_tombstone(self.dirty.value());
                            self.snapshot_next()?;
                            if deleted {
                                self.dirty_next()?;
                                continue;
                            }
                            self.current_is_dirty = true;
                            return Ok(());
                        }
                        std::cmp::Ordering::Greater => {
                            self.current_is_dirty = false;
                            return Ok(());
                        }
                        std::cmp::Ordering::Less => {
                            if is_tombstone(self.dirty.value()) {
                                self.dirty_next()?;
                                continue;
                            }
                            self.current_is_dirty = true;
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    pub fn valid(&self) -> bool {
        self.valid
    }

    pub fn key(&self) -> &[u8] {
        if self.current_is_dirty {
            self.dirty.key()
        } else {
            self.snapshot.key()
        }
    }

    pub fn value(&self) -> &[u8] {
        if self.current_is_dirty {
            self.dirty.value()
        } else {
            self.snapshot.value()
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<(), &'static str> {
        if !self.valid {
            return Err("iterator is finished");
        }
        if self.current_is_dirty {
            self.dirty_next()?;
        } else {
            self.snapshot_next()?;
        }
        self.update_current()
    }
}

/// Source-equivalent local-write/snapshot-read union store.
pub struct UnionStore {
    mem: MemDb,
    snapshot: MapSnapshot,
}

pub const MIN_FLUSH_KEYS: usize = 10_000;
pub const MIN_FLUSH_MEMORY: u64 = 16 * 1024 * 1024;
pub const FORCE_FLUSH_MEMORY: u64 = 128 * 1024 * 1024;

type FlushFunction = Arc<dyn Fn(u64, Arc<MemDb>) -> Result<(), PipelinedError> + Send + Sync>;
type RemoteBatchGetter =
    Arc<dyn Fn(&[Vec<u8>]) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, String> + Send + Sync>;

/// Errors surfaced by pipelined flushing.
///
/// `KeyExists` maps client-go's `ErrKeyExist`: after the server reports an
/// already-exists key, the error is enriched with the value still held in the
/// immutable flushing MemDB.
#[derive(Debug)]
pub enum PipelinedError {
    Message(String),
    KeyExists(KeyExistsError),
}

impl PipelinedError {
    pub fn message(message: impl Into<String>) -> Self {
        Self::Message(message.into())
    }
}

impl fmt::Display for PipelinedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Message(message) => formatter.write_str(message),
            Self::KeyExists(error) => error.fmt(formatter),
        }
    }
}

impl StdError for PipelinedError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::Message(_) => None,
            Self::KeyExists(error) => Some(error),
        }
    }
}

/// Native counterpart to client-go's pipelined MemDB.
///
/// The current generation remains mutable while an immutable `Arc<MemDb>` is
/// handed to one flush worker. Reads prefer mutable state, then that flushing
/// generation, then the caller-provided remote buffer getter.
pub struct PipelinedMemDb {
    mem: MemDb,
    flushing: Option<Arc<MemDb>>,
    completion: Option<mpsc::Receiver<Result<(), PipelinedError>>>,
    flush: FlushFunction,
    remote_batch_get: RemoteBatchGetter,
    accumulated_len: usize,
    accumulated_size: usize,
    generation: u64,
    entry_limit: u64,
    min_flush_keys: usize,
    min_flush_memory: u64,
    force_flush_memory: u64,
    batch_cache: Option<BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
    mem_change_hook: Option<Arc<dyn Fn(u64) + Send + Sync>>,
    is_flushing: Arc<AtomicBool>,
    flush_wait_duration: Duration,
    accumulated_cache_hits: u64,
    accumulated_cache_misses: u64,
    started_at: Instant,
}

impl PipelinedMemDb {
    pub fn new(remote_batch_get: RemoteBatchGetter, flush: FlushFunction) -> Self {
        Self {
            mem: MemDb::new(),
            flushing: None,
            completion: None,
            flush,
            remote_batch_get,
            accumulated_len: 0,
            accumulated_size: 0,
            generation: 0,
            entry_limit: u64::MAX,
            min_flush_keys: MIN_FLUSH_KEYS,
            min_flush_memory: MIN_FLUSH_MEMORY,
            force_flush_memory: FORCE_FLUSH_MEMORY,
            batch_cache: None,
            mem_change_hook: None,
            is_flushing: Arc::new(AtomicBool::new(false)),
            flush_wait_duration: Duration::ZERO,
            accumulated_cache_hits: 0,
            accumulated_cache_misses: 0,
            started_at: Instant::now(),
        }
    }

    #[cfg(test)]
    fn set_flush_thresholds(&mut self, min_keys: usize, min_memory: u64, force_memory: u64) {
        self.min_flush_keys = min_keys;
        self.min_flush_memory = min_memory;
        self.force_flush_memory = force_memory;
    }

    pub fn dirty(&self) -> bool {
        self.mem.dirty() || self.accumulated_len > 0
    }

    pub fn len(&self) -> usize {
        self.accumulated_len + self.mem.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size(&self) -> usize {
        self.accumulated_size + self.mem.size()
    }

    pub fn memory_footprint(&self) -> u64 {
        self.mem.memory_footprint()
            + self
                .flushing
                .as_ref()
                .map_or(0, |buffer| buffer.memory_footprint())
    }

    pub fn on_flushing(&self) -> bool {
        self.is_flushing.load(Ordering::Acquire)
    }

    pub fn set_entry_size_limit(&mut self, entry_limit: u64) {
        self.entry_limit = entry_limit;
        self.mem.set_entry_size_limit(entry_limit, u64::MAX);
    }

    /// The source intentionally ignores its total-buffer argument because the
    /// force-flush threshold is the buffer limit for pipelined mode.
    pub fn set_entry_size_limits(&mut self, entry_limit: u64, _: u64) {
        self.set_entry_size_limit(entry_limit);
    }

    pub fn set(
        &mut self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let result = self.mem.set(key, value);
        self.on_mem_change();
        result
    }

    pub fn set_with_flags(
        &mut self,
        key: &[u8],
        value: &[u8],
        flags: &[FlagsOp],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let result = self.mem.set_with_flags(key, value, flags);
        self.on_mem_change();
        result
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let result = self.mem.delete(key);
        self.on_mem_change();
        result
    }

    pub fn delete_with_flags(
        &mut self,
        key: &[u8],
        flags: &[FlagsOp],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let result = self.mem.delete_with_flags(key, flags);
        self.on_mem_change();
        result
    }

    pub fn get_local(&mut self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        match self.mem.get(key) {
            Ok(value) => Ok(value),
            Err(StaticError::NotExist) => self
                .flushing
                .as_ref()
                .ok_or(StaticError::NotExist)?
                .get_readonly(key),
            Err(error) => Err(error),
        }
    }

    pub fn get_flags(&mut self, key: &[u8]) -> Result<KeyFlags, StaticError> {
        match self.mem.get_flags(key) {
            Ok(flags) => Ok(flags),
            Err(StaticError::NotExist) => self
                .flushing
                .as_ref()
                .ok_or(StaticError::NotExist)?
                .get_flags_readonly(key),
            Err(error) => Err(error),
        }
    }

    pub fn update_flags(&mut self, key: &[u8], flags: &[FlagsOp]) {
        self.mem.update_flags(key, flags);
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Vec<u8>, String> {
        match self.get_local(key) {
            Ok(value) => Ok(value),
            Err(StaticError::NotExist) => {
                if let Some(cached) = self.batch_cache.as_ref().and_then(|cache| cache.get(key)) {
                    return cached.clone().ok_or_else(|| "key not found".to_owned());
                }
                (self.remote_batch_get)(&[key.to_vec()])
                    .map_err(|error| error.to_string())?
                    .remove(key)
                    .ok_or_else(|| "key not found".to_owned())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    /// Pipelined MemDB, like its source counterpart, ignores read options and
    /// returns zero commit timestamps for both local and flushed values.
    pub fn get_entry(&mut self, key: &[u8], _: &[GetOption]) -> Result<ValueEntry, String> {
        self.get(key).map(|value| ValueEntry::new(value, 0))
    }

    pub fn batch_get(&mut self, keys: &[Vec<u8>]) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, String> {
        let mut result = BTreeMap::new();
        let mut remote_keys = Vec::new();
        let mut cache_updates = BTreeMap::new();
        for key in keys {
            match self.get_local(key) {
                Ok(value) => {
                    cache_updates.insert(key.clone(), Some(value.clone()));
                    result.insert(key.clone(), value);
                }
                Err(StaticError::NotExist) => remote_keys.push(key.clone()),
                Err(error) => return Err(error.to_string()),
            }
        }
        let remote = (self.remote_batch_get)(&remote_keys)?;
        for key in remote_keys {
            match remote.get(&key) {
                Some(value) => {
                    cache_updates.insert(key.clone(), Some(value.clone()));
                    result.insert(key, value.clone());
                }
                None => {
                    cache_updates.insert(key, None);
                }
            }
        }
        self.batch_cache
            .get_or_insert_with(BTreeMap::new)
            .extend(cache_updates);
        Ok(result)
    }

    pub fn batch_get_entries(
        &mut self,
        keys: &[Vec<u8>],
        _: &[GetOption],
    ) -> Result<BTreeMap<Vec<u8>, ValueEntry>, String> {
        self.batch_get(keys).map(|entries| {
            entries
                .into_iter()
                .map(|(key, value)| (key, ValueEntry::new(value, 0)))
                .collect()
        })
    }

    pub fn staging(&mut self) -> usize {
        self.mem.staging()
    }

    pub fn cleanup(&mut self, handle: usize) {
        self.mem.cleanup(handle);
    }

    pub fn release(&mut self, handle: usize) {
        self.mem.release(handle);
    }

    /// The source forbids callers from bypassing pipelined generations.
    pub fn get_memdb(&mut self) -> ! {
        panic!("GetMemDB should not be invoked for PipelinedMemDB")
    }

    pub fn iter(
        &self,
        _: Option<&[u8]>,
        _: Option<&[u8]>,
    ) -> Result<Box<dyn KvIterator>, &'static str> {
        Err("pipelined memdb does not support Iter")
    }

    pub fn iter_reverse(
        &self,
        _: Option<&[u8]>,
        _: Option<&[u8]>,
    ) -> Result<Box<dyn KvIterator>, &'static str> {
        Err("pipelined memdb does not support IterReverse")
    }

    pub fn for_each_in_snapshot_range(
        &self,
        _: Option<&[u8]>,
        _: Option<&[u8]>,
        _: bool,
        _: impl FnMut(&[u8], &[u8]) -> Result<bool, &'static str>,
    ) -> Result<(), &'static str> {
        Err("pipelined memdb does not support ForEachInSnapshotRange")
    }

    pub fn snapshot_iter(&self, _: Option<&[u8]>, _: Option<&[u8]>) -> Box<dyn KvIterator> {
        Box::new(ErrorIterator {
            error: "SnapshotIter is not supported for PipelinedMemDB",
        })
    }

    pub fn snapshot_iter_reverse(&self, _: Option<&[u8]>, _: Option<&[u8]>) -> Box<dyn KvIterator> {
        Box::new(ErrorIterator {
            error: "SnapshotIter is not supported for PipelinedMemDB",
        })
    }

    pub fn snapshot_getter(&self) -> ! {
        panic!("SnapshotGetter is not supported for PipelinedMemDB")
    }

    pub fn get_snapshot(&self) -> ! {
        panic!("GetSnapshot is not supported for PipelinedMemDB")
    }

    pub fn remove_from_buffer(&mut self, _: &[u8]) -> ! {
        panic!("RemoveFromBuffer is not supported for PipelinedMemDB")
    }

    pub fn inspect_stage(&self, _: usize, _: impl FnMut(&[u8], KeyFlags, &[u8])) -> ! {
        panic!("InspectStage is not supported for PipelinedMemDB")
    }

    pub fn checkpoint(&self) -> ! {
        panic!("Checkpoint is not supported for PipelinedMemDB")
    }

    pub fn revert_to_checkpoint(&mut self, _: usize) -> ! {
        panic!("RevertToCheckpoint is not supported for PipelinedMemDB")
    }

    pub fn batched_snapshot_iter(&self, _: Option<&[u8]>, _: Option<&[u8]>, _: bool) -> ! {
        panic!("BatchedSnapshotIter is not supported for PipelinedMemDB")
    }

    pub fn set_memory_footprint_change_hook(&mut self, hook: Arc<dyn Fn(u64) + Send + Sync>) {
        self.mem_change_hook = Some(hook);
    }

    pub fn memory_hook_is_set(&self) -> bool {
        self.mem_change_hook.is_some()
    }

    fn on_mem_change(&self) {
        if let Some(hook) = &self.mem_change_hook {
            hook(self.memory_footprint());
        }
    }

    fn needs_flush(&self) -> bool {
        let memory = self.mem.memory_footprint();
        memory >= self.min_flush_memory
            && (self.mem.len() >= self.min_flush_keys || memory >= self.force_flush_memory)
    }

    fn wait_for_flush(&mut self) -> Result<(), PipelinedError> {
        let Some(completion) = self.completion.take() else {
            return Ok(());
        };
        let start = Instant::now();
        let result = completion
            .recv()
            .map_err(|_| PipelinedError::message("pipelined flush worker disconnected"))?;
        self.flush_wait_duration += start.elapsed();
        let result = match result {
            Err(PipelinedError::KeyExists(mut error)) => {
                if let Some(flushing) = &self.flushing {
                    if let Ok(value) = flushing.get_readonly(&error.already_exist.key) {
                        error.value = value;
                    }
                }
                Err(PipelinedError::KeyExists(error))
            }
            result => result,
        };
        self.flushing = None;
        result
    }

    /// Flushes when the configured key/memory condition is met, or
    /// unconditionally when `force` is true. A second flush waits for the
    /// prior generation when the current buffer crossed the force threshold.
    pub fn flush(&mut self, force: bool) -> Result<bool, PipelinedError> {
        self.batch_cache = None;
        if self.mem.is_staging() {
            return Err(PipelinedError::message(
                "there are stages unreleased when Flush is called",
            ));
        }
        if !force && !self.needs_flush() {
            return Ok(false);
        }
        if self.flushing.is_some() {
            if !force
                && self.is_flushing.load(Ordering::Acquire)
                && self.mem.memory_footprint() < self.force_flush_memory
            {
                return Ok(false);
            }
            self.wait_for_flush()?;
        }

        let old = std::mem::take(&mut self.mem);
        let old = Arc::new(old);
        self.accumulated_len += old.len();
        self.accumulated_size += old.size();
        self.accumulated_cache_hits += old.cache_hit_count();
        self.accumulated_cache_misses += old.cache_miss_count();
        self.flushing = Some(old.clone());
        self.mem = MemDb::new();
        self.mem.set_entry_size_limit(self.entry_limit, u64::MAX);
        self.generation += 1;
        let generation = self.generation;
        let flush = self.flush.clone();
        let is_flushing = self.is_flushing.clone();
        let flush_len = old.len();
        let flush_size = old.size();
        is_flushing.store(true, Ordering::Release);
        let (sender, receiver) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let started = Instant::now();
            let result = flush(generation, old);
            crate::stats::observe_pipelined_flush(flush_len, flush_size, started.elapsed());
            is_flushing.store(false, Ordering::Release);
            let _ = sender.send(result);
        });
        self.completion = Some(receiver);
        self.on_mem_change();
        Ok(true)
    }

    pub fn flush_wait(&mut self) -> Result<(), PipelinedError> {
        self.wait_for_flush()
    }

    pub fn metrics(&self) -> PipelinedMetrics {
        PipelinedMetrics {
            flush_wait_duration: self.flush_wait_duration,
            total_duration: self.started_at.elapsed(),
            memdb_hit_count: self.accumulated_cache_hits + self.mem.cache_hit_count(),
            memdb_miss_count: self.accumulated_cache_misses + self.mem.cache_miss_count(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PipelinedMetrics {
    pub flush_wait_duration: Duration,
    pub total_duration: Duration,
    pub memdb_hit_count: u64,
    pub memdb_miss_count: u64,
}

impl UnionStore {
    pub fn new(mem: MemDb, snapshot: MapSnapshot) -> Self {
        Self { mem, snapshot }
    }

    pub fn mem_buffer(&mut self) -> &mut MemDb {
        &mut self.mem
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        self.get_entry(key, &[]).map(|entry| entry.value)
    }

    pub fn get_entry(
        &mut self,
        key: &[u8],
        options: &[GetOption],
    ) -> Result<ValueEntry, StaticError> {
        match self.mem.get_entry(key, options) {
            Ok(entry) if is_tombstone(&entry.value) => Err(StaticError::NotExist),
            Ok(entry) => Ok(entry),
            Err(StaticError::NotExist) => self.snapshot.get_entry(key, options),
            Err(error) => Err(error),
        }
    }

    pub fn iter(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Result<UnionIterator, &'static str> {
        UnionIterator::new(
            self.mem.iter(lower, upper),
            self.snapshot.iter(lower, upper, false),
            false,
        )
    }

    pub fn iter_reverse(
        &self,
        upper: Option<&[u8]>,
        lower: Option<&[u8]>,
    ) -> Result<UnionIterator, &'static str> {
        UnionIterator::new(
            self.mem.iter_reverse(upper, lower),
            self.snapshot.iter(lower, upper, true),
            true,
        )
    }

    pub fn has_presume_key_not_exists(&mut self, key: &[u8]) -> bool {
        self.mem
            .get_flags(key)
            .is_ok_and(|flags| flags.has_presume_key_not_exists())
    }

    pub fn unmark_presume_key_not_exists(&mut self, key: &[u8]) {
        self.mem
            .update_flags(key, &[FlagsOp::DelPresumeKeyNotExists]);
    }

    /// Matches `KVUnionStore.SetEntrySizeLimit`: zero means unlimited rather
    /// than a zero-byte limit.
    pub fn set_entry_size_limit(&mut self, entry_limit: u64, buffer_limit: u64) {
        self.mem.set_entry_size_limit(
            if entry_limit == 0 {
                u64::MAX
            } else {
                entry_limit
            },
            if buffer_limit == 0 {
                u64::MAX
            } else {
                buffer_limit
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use unistore::{Mutation, MvccStore};

    trait TestMemBuffer {
        fn set_value(&mut self, key: &[u8], value: &[u8]);
        fn delete_value(&mut self, key: &[u8]);
        fn get_value(&mut self, key: &[u8]) -> Result<Vec<u8>, StaticError>;
        fn iter_all(&self) -> Box<dyn KvIterator>;
        fn staging_handle(&mut self) -> usize;
        fn cleanup_handle(&mut self, handle: usize);
        fn release_handle(&mut self, handle: usize);
        fn len_value(&self) -> usize;
        fn size_value(&self) -> usize;
    }

    impl TestMemBuffer for MemDb {
        fn set_value(&mut self, key: &[u8], value: &[u8]) {
            self.set(key, value).unwrap();
        }
        fn delete_value(&mut self, key: &[u8]) {
            self.delete(key).unwrap();
        }
        fn get_value(&mut self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
            self.get(key)
        }
        fn iter_all(&self) -> Box<dyn KvIterator> {
            self.iter(None, None)
        }
        fn staging_handle(&mut self) -> usize {
            self.staging()
        }
        fn cleanup_handle(&mut self, handle: usize) {
            self.cleanup(handle);
        }
        fn release_handle(&mut self, handle: usize) {
            self.release(handle);
        }
        fn len_value(&self) -> usize {
            self.len()
        }
        fn size_value(&self) -> usize {
            self.size()
        }
    }

    impl TestMemBuffer for RbtMemDb {
        fn set_value(&mut self, key: &[u8], value: &[u8]) {
            self.set(key, value).unwrap();
        }
        fn delete_value(&mut self, key: &[u8]) {
            self.delete(key).unwrap();
        }
        fn get_value(&mut self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
            self.get(key)
        }
        fn iter_all(&self) -> Box<dyn KvIterator> {
            self.iter(None, None)
        }
        fn staging_handle(&mut self) -> usize {
            self.staging()
        }
        fn cleanup_handle(&mut self, handle: usize) {
            self.cleanup(handle);
        }
        fn release_handle(&mut self, handle: usize) {
            self.release(handle);
        }
        fn len_value(&self) -> usize {
            self.len()
        }
        fn size_value(&self) -> usize {
            self.size()
        }
    }

    fn next_random(state: &mut u64) -> u64 {
        *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        *state
    }

    fn assert_matches_oracle(
        buffer: &mut impl TestMemBuffer,
        oracle: &BTreeMap<Vec<u8>, Vec<u8>>,
        operation: usize,
    ) {
        let mut actual = BTreeMap::new();
        let mut iterator = buffer.iter_all();
        while iterator.valid() {
            actual.insert(iterator.key().to_vec(), iterator.value().to_vec());
            iterator.next().unwrap();
        }
        if actual != *oracle {
            let unexpected = actual.keys().find(|key| !oracle.contains_key(*key));
            let missing = oracle.keys().find(|key| !actual.contains_key(*key));
            panic!(
                "iteration mismatch after operation {operation}: unexpected={unexpected:?}, missing={missing:?}"
            );
        }
        assert_eq!(
            buffer.len_value(),
            oracle.len(),
            "entry-count mismatch after operation {operation}"
        );
        assert_eq!(
            buffer.size_value(),
            oracle
                .iter()
                .map(|(key, value)| key.len() + value.len())
                .sum::<usize>()
        );
        let mut iterator = buffer.iter_all();
        for (key, value) in oracle {
            assert!(iterator.valid());
            assert_eq!(iterator.key(), key);
            assert_eq!(iterator.value(), value);
            iterator.next().unwrap();
            assert_eq!(buffer.get_value(key).unwrap(), *value);
        }
        assert!(!iterator.valid());
    }

    fn randomized_staging_oracle(buffer: &mut impl TestMemBuffer) {
        let mut random = 0x8f53_3a21_9c77_4e11;
        let mut oracle = BTreeMap::new();
        let mut stages = Vec::new();
        for operation in 0..4_000 {
            let choice = next_random(&mut random);
            if choice.is_multiple_of(19) && stages.len() < 8 {
                stages.push((buffer.staging_handle(), oracle.clone()));
            } else if choice.is_multiple_of(23) && !stages.is_empty() {
                let (handle, before) = stages.pop().unwrap();
                if choice & 1 == 0 {
                    buffer.cleanup_handle(handle);
                    oracle = before;
                } else {
                    buffer.release_handle(handle);
                }
            } else {
                let key_len = (choice as usize % 8) + 1;
                let mut key = next_random(&mut random).to_be_bytes().to_vec();
                key.truncate(key_len);
                if choice.is_multiple_of(11) {
                    buffer.delete_value(&key);
                    oracle.insert(key, Vec::new());
                } else {
                    let value_len = ((choice >> 8) as usize % 16) + 1;
                    let mut value = next_random(&mut random).to_be_bytes().to_vec();
                    value.resize(value_len, operation as u8);
                    buffer.set_value(&key, &value);
                    oracle.insert(key, value);
                }
            }
            assert_matches_oracle(buffer, &oracle, operation);
        }
        while let Some((handle, _)) = stages.pop() {
            buffer.release_handle(handle);
        }
        assert_matches_oracle(buffer, &oracle, 4_000);
    }

    /// Deterministic equivalent of client-go's non-race `TestRandomDerive`:
    /// 101 nested stages each write 512 values, and selected child stages roll
    /// back after their descendants have released into them.
    fn deeply_nested_staging_oracle(
        buffer: &mut impl TestMemBuffer,
        oracle: &mut BTreeMap<Vec<u8>, Vec<u8>>,
        depth: u16,
    ) {
        let before = oracle.clone();
        let handle = buffer.staging_handle();
        for index in 0..512u16 {
            let key = ((usize::from(depth) * 317 + usize::from(index)) % 2_048)
                .to_be_bytes()
                .to_vec();
            let value = vec![depth as u8, index as u8, (index >> 8) as u8];
            buffer.set_value(&key, &value);
            oracle.insert(key, value);
        }
        if depth < 100 {
            deeply_nested_staging_oracle(buffer, oracle, depth + 1);
        }
        if depth > 0 && depth % 7 == 3 {
            buffer.cleanup_handle(handle);
            *oracle = before;
        } else {
            buffer.release_handle(handle);
        }
    }

    fn collect(mut iterator: UnionIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut entries = Vec::new();
        while iterator.valid() {
            entries.push((iterator.key().to_vec(), iterator.value().to_vec()));
            iterator.next().unwrap();
        }
        entries
    }

    #[test]
    fn memdb_set_delete_staging_and_snapshot_match_source_contract() {
        let mut db = MemDb::new();
        assert!(db.set(b"key", b"").is_err());
        db.set(b"key", b"value").unwrap();
        assert_eq!(db.get(b"key").unwrap(), b"value");
        db.delete(b"key").unwrap();
        assert_eq!(db.get(b"key").unwrap(), b"");
        let stage = db.staging();
        db.set_with_flags(b"staged", b"value", &[FlagsOp::SetKeyLocked])
            .unwrap();
        let snapshot = db.snapshot();
        assert!(snapshot.get(b"staged").is_err());
        db.cleanup(stage);
        assert!(db.get(b"staged").is_err());
        assert!(db.get_flags(b"staged").unwrap().has_locked());
    }

    #[test]
    fn snapshot_batches_survive_staged_writes_and_invalidate_on_stage_zero_release() {
        let mut db = MemDb::new();
        db.set(b"a", b"one").unwrap();
        let stage = db.staging();
        db.set(b"a", b"two").unwrap();
        db.set(b"b", b"three").unwrap();
        let snapshot = db.snapshot();
        assert_eq!(snapshot.get(b"a").unwrap(), b"one");
        assert_eq!(snapshot.get(b"b"), Err(SnapshotError::NotExist));
        let mut forward = snapshot.batched_iter(None, None, false);
        assert!(forward.valid());
        assert_eq!(forward.key(), b"a");
        assert_eq!(forward.value(), b"one");
        forward.next().unwrap();
        assert!(!forward.valid());

        let mut range = Vec::new();
        snapshot
            .for_each(None, None, false, |key, value| {
                range.push((key.to_vec(), value.to_vec()));
                Ok(false)
            })
            .unwrap();
        assert_eq!(range, vec![(b"a".to_vec(), b"one".to_vec())]);
        db.release(stage);
        assert_eq!(snapshot.get(b"a"), Err(SnapshotError::Invalidated));
        assert!(!forward.valid());
        assert_eq!(
            forward.next(),
            Err("invalid snapshot: snapshot sequence changed")
        );
    }

    #[test]
    fn batched_snapshot_iteration_honors_forward_reverse_and_exclusive_upper_bounds() {
        let mut db = MemDb::new();
        for key in 0u8..=4 {
            db.set(&[key], &[key]).unwrap();
        }
        let stage = db.staging();
        db.set(b"staged", b"value").unwrap();
        let mut forward = db.batched_snapshot_iter(Some(&[1]), Some(&[4]), false);
        let mut forward_keys = Vec::new();
        while forward.valid() {
            forward_keys.push(forward.key().to_vec());
            forward.next().unwrap();
        }
        assert_eq!(forward_keys, vec![vec![1], vec![2], vec![3]]);

        let mut reverse = db.batched_snapshot_iter(Some(&[1]), Some(&[4]), true);
        let mut reverse_keys = Vec::new();
        while reverse.valid() {
            reverse_keys.push(reverse.key().to_vec());
            reverse.next().unwrap();
        }
        assert_eq!(reverse_keys, vec![vec![3], vec![2], vec![1]]);
        db.cleanup(stage);
    }

    #[test]
    fn rbt_memdb_adapter_covers_source_staging_iteration_and_stable_snapshot_contract() {
        let mut db = RbtMemDb::new();
        let base = db.staging();
        for number in 0..10_000u32 {
            let key = number.to_be_bytes();
            db.set(&key, &key).unwrap();
        }
        db.release(base);
        let snapshot = db.snapshot();
        let overwrite = db.staging();
        for number in 0..10_000u32 {
            let key = number.to_be_bytes();
            db.set(&key, &(number + 1).to_be_bytes()).unwrap();
        }
        db.cleanup(overwrite);
        let mut forward = db.iter(Some(&1u32.to_be_bytes()), Some(&4u32.to_be_bytes()));
        assert_eq!(forward.key(), 1u32.to_be_bytes());
        forward.next().unwrap();
        assert_eq!(forward.key(), 2u32.to_be_bytes());
        let mut reverse = db.iter_reverse(Some(&4u32.to_be_bytes()), Some(&1u32.to_be_bytes()));
        assert_eq!(reverse.key(), 3u32.to_be_bytes());
        reverse.next().unwrap();
        assert_eq!(reverse.key(), 2u32.to_be_bytes());
        assert_eq!(db.len(), 10_000);
        assert_eq!(
            snapshot.get(&9u32.to_be_bytes()).unwrap(),
            9u32.to_be_bytes()
        );
        db.delete(b"delete").unwrap();
        assert_eq!(db.get(b"delete").unwrap(), b"");
        db.update_flags(b"delete", &[FlagsOp::SetPresumeKeyNotExists]);
        assert!(db
            .get_flags(b"delete")
            .unwrap()
            .has_presume_key_not_exists());
        db.reset();
        assert_eq!(db.len(), 0);
    }

    #[test]
    fn art_and_rbt_memdb_adapters_match_the_same_randomized_staging_oracle() {
        randomized_staging_oracle(&mut MemDb::new());
        randomized_staging_oracle(&mut RbtMemDb::new());
    }

    #[test]
    fn art_memdb_survives_client_go_random_derive_depth_and_mutation_scale() {
        let mut db = MemDb::new();
        let mut oracle = BTreeMap::new();
        deeply_nested_staging_oracle(&mut db, &mut oracle, 0);
        assert_matches_oracle(&mut db, &oracle, 51_712);
    }

    #[test]
    fn source_scale_rbt_random_mutation_and_art_rbt_staging_differential_hold() {
        const COUNT: usize = 50_000;
        let mut state = 0x477d_4f41_1a19_7ad3;
        let mut rbt = RbtMemDb::new();
        let mut oracle = BTreeMap::new();
        let mut keys = Vec::with_capacity(COUNT);
        for _ in 0..COUNT {
            let key_len = (next_random(&mut state) as usize % 19) + 1;
            let mut key = next_random(&mut state).to_be_bytes().to_vec();
            key.resize(key_len, (state >> 8) as u8);
            rbt.set(&key, &key).unwrap();
            oracle.insert(key.clone(), key.clone());
            keys.push(key);
        }
        for key in keys.iter().rev() {
            if next_random(&mut state) % 100 < 35 {
                rbt.remove_from_buffer(key);
                oracle.remove(key);
            } else {
                let value_len = (next_random(&mut state) as usize % 19) + 1;
                let mut value = next_random(&mut state).to_be_bytes().to_vec();
                value.resize(value_len, (state >> 16) as u8);
                rbt.set(key, &value).unwrap();
                oracle.insert(key.clone(), value);
            }
        }
        assert_matches_oracle(&mut rbt, &oracle, COUNT * 2);

        let mut art = MemDb::new();
        let mut rbt = RbtMemDb::new();
        let mut shared_oracle = BTreeMap::new();
        for operation in 0..COUNT {
            let handle_art = art.staging();
            let handle_rbt = rbt.staging();
            let key_len = (next_random(&mut state) as usize % 19) + 1;
            let mut key = next_random(&mut state).to_be_bytes().to_vec();
            key.resize(key_len, (state >> 24) as u8);
            let value = key.clone();
            art.set(&key, &value).unwrap();
            rbt.set(&key, &value).unwrap();
            if operation % 2 == 0 {
                art.cleanup(handle_art);
                rbt.cleanup(handle_rbt);
            } else {
                art.release(handle_art);
                rbt.release(handle_rbt);
                shared_oracle.insert(key, value);
            }
            if operation % 5_000 == 0 {
                assert_matches_oracle(&mut art, &shared_oracle, operation);
                assert_matches_oracle(&mut rbt, &shared_oracle, operation);
            }
        }
        assert_matches_oracle(&mut art, &shared_oracle, COUNT);
        assert_matches_oracle(&mut rbt, &shared_oracle, COUNT);
    }

    #[test]
    fn memdb_facade_forwards_batch_snapshot_checkpoint_stage_and_metrics_contracts() {
        let mut db = MemDb::new();
        assert!(!db.flush(false).unwrap());
        assert_eq!(db.flush_wait(), Ok(()));
        assert_eq!(db.metrics().memdb_hit_count, 0);
        let memory_events = Arc::new(AtomicU64::new(0));
        db.set_memory_footprint_change_hook({
            let memory_events = memory_events.clone();
            Arc::new(move |_| {
                memory_events.fetch_add(1, Ordering::Relaxed);
            })
        });
        assert!(db.memory_hook_is_set());
        db.set(b"present", b"value").unwrap();
        db.delete(b"tombstone").unwrap();
        assert!(memory_events.load(Ordering::Relaxed) > 0);
        assert_eq!(
            db.batch_get(&[
                b"present".to_vec(),
                b"tombstone".to_vec(),
                b"missing".to_vec()
            ]),
            BTreeMap::from([
                (b"present".to_vec(), b"value".to_vec()),
                (b"tombstone".to_vec(), Vec::new()),
            ])
        );
        assert_eq!(
            db.get_entry(b"present", &[GetOption::ReturnCommitTs])
                .unwrap(),
            ValueEntry::new(b"value".to_vec(), 0)
        );
        assert_eq!(
            db.batch_get_entries(&[b"present".to_vec()], &[GetOption::ReturnCommitTs]),
            BTreeMap::from([(b"present".to_vec(), ValueEntry::new(b"value".to_vec(), 0))])
        );
        let mut snapshot = db.snapshot_iter(None, None);
        assert_eq!(snapshot.key(), b"present");
        snapshot.next().unwrap();
        assert_eq!(snapshot.key(), b"tombstone");
        db.snapshot_getter().close();
        assert_eq!(db.get_memdb().len(), 2);
        let mut scanned = Vec::new();
        db.for_each_in_snapshot_range(None, None, false, |key, value| {
            scanned.push((key.to_vec(), value.to_vec()));
            Ok(false)
        })
        .unwrap();
        assert_eq!(
            scanned,
            vec![
                (b"present".to_vec(), b"value".to_vec()),
                (b"tombstone".to_vec(), Vec::new()),
            ]
        );
        db.remove_from_buffer(b"present");
        assert_eq!(db.get(b"present"), Err(StaticError::NotExist));
        assert_eq!(db.len(), 1);

        let stage = db.staging();
        let checkpoint = db.checkpoint();
        db.set_with_flags(b"staged", b"value", &[FlagsOp::SetPresumeKeyNotExists])
            .unwrap();
        let mut staged = Vec::new();
        db.inspect_stage(stage, |key, flags, value| {
            staged.push((key.to_vec(), flags, value.to_vec()));
        });
        assert_eq!(staged.len(), 1);
        assert_eq!(staged[0].0, b"staged");
        assert!(staged[0].1.has_presume_key_not_exists());
        db.revert_to_checkpoint(checkpoint);
        assert_eq!(db.get(b"staged"), Err(StaticError::NotExist));
        db.cleanup(stage);

        db.set(b"history", b"one").unwrap();
        let history = db.staging();
        db.set(b"history", b"two").unwrap();
        assert_eq!(
            db.select_value_history(b"history", |value| value == b"one")
                .unwrap(),
            Some(b"one".to_vec())
        );
        db.cleanup(history);
    }

    #[test]
    fn rbt_memdb_facade_forwards_batch_snapshot_checkpoint_stage_and_metrics_contracts() {
        let mut db = RbtMemDb::new();
        assert!(!db.flush(false).unwrap());
        assert_eq!(db.flush_wait(), Ok(()));
        assert_eq!(db.metrics().memdb_miss_count, 0);
        let memory_events = Arc::new(AtomicU64::new(0));
        db.set_memory_footprint_change_hook({
            let memory_events = memory_events.clone();
            Arc::new(move |_| {
                memory_events.fetch_add(1, Ordering::Relaxed);
            })
        });
        assert!(db.memory_hook_is_set());
        db.set(b"present", b"value").unwrap();
        db.delete_with_flags(b"tombstone", &[FlagsOp::SetPresumeKeyNotExists])
            .unwrap();
        assert!(memory_events.load(Ordering::Relaxed) > 0);
        assert_eq!(
            db.batch_get(&[
                b"present".to_vec(),
                b"tombstone".to_vec(),
                b"missing".to_vec()
            ]),
            BTreeMap::from([
                (b"present".to_vec(), b"value".to_vec()),
                (b"tombstone".to_vec(), Vec::new()),
            ])
        );
        assert_eq!(
            db.get_entry(b"present", &[GetOption::ReturnCommitTs])
                .unwrap(),
            ValueEntry::new(b"value".to_vec(), 0)
        );
        assert_eq!(
            db.batch_get_entries(&[b"present".to_vec()], &[GetOption::ReturnCommitTs]),
            BTreeMap::from([(b"present".to_vec(), ValueEntry::new(b"value".to_vec(), 0))])
        );
        let mut snapshot = db.snapshot_iter_reverse(None, None);
        assert_eq!(snapshot.key(), b"tombstone");
        snapshot.next().unwrap();
        assert_eq!(snapshot.key(), b"present");
        db.snapshot_getter().close();
        assert!(db.get_memdb().is_none());
        let mut scanned = Vec::new();
        db.for_each_in_snapshot_range(None, None, true, |key, value| {
            scanned.push((key.to_vec(), value.to_vec()));
            Ok(false)
        })
        .unwrap();
        assert_eq!(
            scanned,
            vec![
                (b"tombstone".to_vec(), Vec::new()),
                (b"present".to_vec(), b"value".to_vec()),
            ]
        );
        db.remove_from_buffer(b"present");
        assert_eq!(db.get(b"present"), Err(StaticError::NotExist));

        let stage = db.staging();
        let checkpoint = db.checkpoint();
        db.set(b"staged", b"value").unwrap();
        let mut staged = Vec::new();
        db.inspect_stage(stage, |key, _, value| {
            staged.push((key.to_vec(), value.to_vec()));
        });
        assert_eq!(staged, vec![(b"staged".to_vec(), b"value".to_vec())]);
        db.revert_to_checkpoint(checkpoint);
        assert_eq!(db.get(b"staged"), Err(StaticError::NotExist));
        db.cleanup(stage);

        db.set(b"history", b"one").unwrap();
        let history = db.staging();
        db.set(b"history", b"two").unwrap();
        assert_eq!(
            db.select_value_history(b"history", |value| value == b"one")
                .unwrap(),
            Some(b"one".to_vec())
        );
        db.cleanup(history);
    }

    #[test]
    fn union_store_get_set_delete_and_bounds_follow_source_tests() {
        let mut snapshot = MapSnapshot::default();
        for key in [b"1", b"2", b"3"] {
            snapshot.insert(key.as_slice(), key.as_slice());
        }
        let mut store = UnionStore::new(MemDb::new(), snapshot);
        assert_eq!(store.get(b"1").unwrap(), b"1");
        store.mem_buffer().set(b"1", b"one").unwrap();
        store.mem_buffer().set(b"4", b"4").unwrap();
        store.mem_buffer().delete(b"3").unwrap();
        assert_eq!(store.get(b"1").unwrap(), b"one");
        assert!(store.get(b"3").is_err());
        assert_eq!(
            collect(store.iter(Some(b"2"), None).unwrap()),
            vec![
                (b"2".to_vec(), b"2".to_vec()),
                (b"4".to_vec(), b"4".to_vec())
            ]
        );
        assert_eq!(
            collect(store.iter_reverse(Some(b"4"), Some(b"1")).unwrap()),
            vec![
                (b"2".to_vec(), b"2".to_vec()),
                (b"1".to_vec(), b"one".to_vec())
            ]
        );
    }

    #[test]
    fn union_store_forwards_flags_limits_and_mock_snapshot_batch_reads() {
        let mut snapshot = MapSnapshot::default();
        snapshot.insert(b"snapshot", b"value");
        assert_eq!(
            snapshot.batch_get(&[b"snapshot".to_vec(), b"missing".to_vec()]),
            BTreeMap::from([(b"snapshot".to_vec(), b"value".to_vec())])
        );
        assert_eq!(
            snapshot
                .get_entry(b"snapshot", &[GetOption::ReturnCommitTs])
                .unwrap(),
            ValueEntry::new(b"value".to_vec(), 1000 + u64::from(b's'))
        );
        assert_eq!(
            snapshot.batch_get_entries(
                &[b"snapshot".to_vec(), b"missing".to_vec()],
                &[GetOption::ReturnCommitTs]
            ),
            BTreeMap::from([(
                b"snapshot".to_vec(),
                ValueEntry::new(b"value".to_vec(), 1000 + u64::from(b's'))
            )])
        );
        let mut store = UnionStore::new(MemDb::new(), snapshot);
        assert_eq!(
            store
                .get_entry(b"snapshot", &[GetOption::ReturnCommitTs])
                .unwrap(),
            ValueEntry::new(b"value".to_vec(), 1000 + u64::from(b's'))
        );
        store
            .mem_buffer()
            .set_with_flags(b"local", b"value", &[FlagsOp::SetPresumeKeyNotExists])
            .unwrap();
        assert_eq!(
            store
                .get_entry(b"local", &[GetOption::ReturnCommitTs])
                .unwrap(),
            ValueEntry::new(b"value".to_vec(), 0)
        );
        assert!(store.has_presume_key_not_exists(b"local"));
        store.unmark_presume_key_not_exists(b"local");
        assert!(!store.has_presume_key_not_exists(b"local"));

        store.set_entry_size_limit(0, 0);
        store.mem_buffer().set(b"unlimited", b"value").unwrap();
        store.set_entry_size_limit(3, 3);
        assert!(store.mem_buffer().set(b"four", b"bytes").is_err());
    }

    #[test]
    fn union_iterator_prefers_dirty_values_and_skips_tombstones_in_both_directions() {
        let dirty = Box::new(VecIterator::new(
            vec![
                (b"a".to_vec(), b"local".to_vec()),
                (b"b".to_vec(), Vec::new()),
                (b"d".to_vec(), b"new".to_vec()),
            ]
            .into_iter(),
            false,
        ));
        let snapshot = Box::new(VecIterator::new(
            vec![
                (b"a".to_vec(), b"old".to_vec()),
                (b"b".to_vec(), b"old".to_vec()),
                (b"c".to_vec(), b"old".to_vec()),
            ]
            .into_iter(),
            false,
        ));
        let mut iterator = UnionIterator::new(dirty, snapshot, false).unwrap();
        let mut values = Vec::new();
        while iterator.valid() {
            values.push((iterator.key().to_vec(), iterator.value().to_vec()));
            iterator.next().unwrap();
        }
        assert_eq!(
            values,
            vec![
                (b"a".to_vec(), b"local".to_vec()),
                (b"c".to_vec(), b"old".to_vec()),
                (b"d".to_vec(), b"new".to_vec())
            ]
        );
    }

    #[test]
    fn pipelined_flush_preserves_read_precedence_generation_and_totals() {
        let remote = Arc::new(std::sync::Mutex::new(BTreeMap::<Vec<u8>, Vec<u8>>::new()));
        let remote_get = {
            let remote = remote.clone();
            Arc::new(move |keys: &[Vec<u8>]| {
                let remote = remote.lock().unwrap();
                Ok(keys
                    .iter()
                    .filter_map(|key| remote.get(key).map(|value| (key.clone(), value.clone())))
                    .collect())
            })
        };
        let generations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let flush = {
            let remote = remote.clone();
            let generations = generations.clone();
            Arc::new(move |generation, db: Arc<MemDb>| {
                let mut iterator = db.iter(None, None);
                let mut remote = remote.lock().unwrap();
                while iterator.valid() {
                    let key = iterator.key().to_vec();
                    let value = iterator.value().to_vec();
                    if value.is_empty() {
                        remote.remove(&key);
                    } else {
                        remote.insert(key, value);
                    }
                    iterator.next().unwrap();
                }
                generations.lock().unwrap().push(generation);
                Ok(())
            })
        };
        let mut db = PipelinedMemDb::new(remote_get, flush);
        db.set_flush_thresholds(1, 1, 32);
        db.set(b"key", b"one").unwrap();
        assert!(db.flush(false).unwrap());
        assert_eq!(db.get_local(b"key").unwrap(), b"one");
        assert!(db.flush_wait().is_ok());
        assert!(!db.on_flushing());
        assert_eq!(db.get(b"key").unwrap(), b"one");
        assert_eq!(db.len(), 1);
        assert_eq!(db.size(), 6);

        db.set(b"key", b"two").unwrap();
        assert!(db.flush(true).unwrap());
        db.set(b"key", b"three").unwrap();
        assert_eq!(db.get_local(b"key").unwrap(), b"three");
        db.flush_wait().unwrap();
        assert_eq!(db.get(b"key").unwrap(), b"three");
        assert_eq!(*generations.lock().unwrap(), vec![1, 2]);
        assert_eq!(db.len(), 3);
        assert_eq!(db.size(), 20);
        assert!(db.dirty());
        assert!(db.metrics().total_duration >= Duration::ZERO);
    }

    #[test]
    fn pipelined_batch_cache_staging_and_flush_errors_follow_source_rules() {
        let remote = Arc::new(std::sync::Mutex::new(BTreeMap::from([(
            b"remote".to_vec(),
            b"value".to_vec(),
        )])));
        let remote_get = {
            let remote = remote.clone();
            Arc::new(move |keys: &[Vec<u8>]| {
                let remote = remote.lock().unwrap();
                Ok(keys
                    .iter()
                    .filter_map(|key| remote.get(key).map(|value| (key.clone(), value.clone())))
                    .collect())
            })
        };
        let flush: FlushFunction = Arc::new(|_, _| Ok(()));
        let mut db = PipelinedMemDb::new(remote_get, flush);
        let values = db
            .batch_get(&[b"remote".to_vec(), b"missing".to_vec()])
            .unwrap();
        assert_eq!(values.get(b"remote" as &[u8]), Some(&b"value".to_vec()));
        assert!(!values.contains_key(b"missing" as &[u8]));
        assert_eq!(db.get(b"remote").unwrap(), b"value");
        let stage = db.staging();
        db.set(b"staged", b"value").unwrap();
        assert!(db.flush(true).is_err());
        db.cleanup(stage);
        assert!(!db.flush(false).unwrap());
        db.update_flags(b"flag", &[FlagsOp::SetKeyLocked]);
        assert!(db.get_flags(b"flag").unwrap().has_locked());
        let empty_remote: RemoteBatchGetter = Arc::new(|_| Ok(BTreeMap::new()));
        let flush: FlushFunction = Arc::new(|_, _| Ok(()));
        let mut empty = PipelinedMemDb::new(empty_remote, flush);
        assert!(empty.flush(true).unwrap());
        empty.flush_wait().unwrap();
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn pipelined_unsupported_operations_preserve_source_error_and_panic_contracts() {
        let remote: RemoteBatchGetter = Arc::new(|_| Ok(BTreeMap::new()));
        let flush: FlushFunction = Arc::new(|_, _| Ok(()));
        let mut db = PipelinedMemDb::new(remote, flush);
        assert!(matches!(
            db.iter(None, None),
            Err("pipelined memdb does not support Iter")
        ));
        assert!(matches!(
            db.iter_reverse(None, None),
            Err("pipelined memdb does not support IterReverse")
        ));
        assert_eq!(
            db.for_each_in_snapshot_range(None, None, false, |_, _| Ok(false)),
            Err("pipelined memdb does not support ForEachInSnapshotRange")
        );
        let mut snapshot = db.snapshot_iter(None, None);
        assert!(snapshot.valid());
        assert_eq!(snapshot.key(), b"");
        assert_eq!(
            snapshot.next(),
            Err("SnapshotIter is not supported for PipelinedMemDB")
        );
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.checkpoint();
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.revert_to_checkpoint(0);
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.remove_from_buffer(b"key");
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.snapshot_getter();
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.get_snapshot();
        }))
        .is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            db.get_memdb();
        }))
        .is_err());
    }

    #[test]
    fn pipelined_flush_skip_wait_and_worker_error_match_source_scheduling() {
        let remote: RemoteBatchGetter = Arc::new(|_| Ok(BTreeMap::new()));
        let (started_sender, started_receiver) = mpsc::sync_channel(1);
        let (release_sender, release_receiver) = mpsc::sync_channel(1);
        let release_receiver = Arc::new(std::sync::Mutex::new(release_receiver));
        let flush: FlushFunction = Arc::new(move |_, _| {
            started_sender.send(()).unwrap();
            release_receiver.lock().unwrap().recv().unwrap();
            Ok(())
        });
        let mut db = PipelinedMemDb::new(remote, flush);
        db.set_flush_thresholds(1, 1, 1_024);
        db.set(b"first", b"value").unwrap();
        assert!(db.flush(false).unwrap());
        started_receiver.recv().unwrap();
        assert!(db.on_flushing());
        db.set(b"second", b"value").unwrap();
        assert!(!db.flush(false).unwrap());
        assert_eq!(db.get_local(b"second").unwrap(), b"value");
        release_sender.send(()).unwrap();
        db.flush_wait().unwrap();
        assert!(!db.on_flushing());
        assert!(db.flush(false).unwrap());
        started_receiver.recv().unwrap();
        // The same worker gate proves a forced flush waits for the preceding
        // generation before installing the next one.
        db.set(b"third", b"value").unwrap();
        release_sender.send(()).unwrap();
        assert!(db.flush(true).unwrap());
        started_receiver.recv().unwrap();
        release_sender.send(()).unwrap();
        db.flush_wait().unwrap();

        let remote: RemoteBatchGetter = Arc::new(|_| Ok(BTreeMap::new()));
        let flush: FlushFunction = Arc::new(|_, _| Err(PipelinedError::message("flush failed")));
        let mut failing = PipelinedMemDb::new(remote, flush);
        failing.set_flush_thresholds(1, 1, 1);
        failing.set(b"key", b"value").unwrap();
        assert!(failing.flush(false).unwrap());
        assert_eq!(
            failing.flush_wait().unwrap_err().to_string(),
            "flush failed"
        );

        let remote: RemoteBatchGetter = Arc::new(|_| Ok(BTreeMap::new()));
        let flush: FlushFunction = Arc::new(|_, _| {
            Err(PipelinedError::KeyExists(KeyExistsError {
                already_exist: crate::proto::kvrpcpb::AlreadyExist {
                    key: b"key".to_vec(),
                },
                value: Vec::new(),
            }))
        });
        let mut duplicate = PipelinedMemDb::new(remote, flush);
        duplicate.set_flush_thresholds(1, 1, 1);
        duplicate.set(b"key", b"flushing-value").unwrap();
        assert!(duplicate.flush(false).unwrap());
        match duplicate.flush_wait().unwrap_err() {
            PipelinedError::KeyExists(error) => {
                assert_eq!(error.already_exist.key, b"key");
                assert_eq!(error.value, b"flushing-value");
            }
            error => panic!("expected key-exists error, got {error}"),
        }
    }

    #[test]
    fn pipelined_memdb_can_use_the_reusable_unistore_crate_as_its_remote_backend() {
        let store = Arc::new(MvccStore::new());
        let remote_get = {
            let store = store.clone();
            Arc::new(move |keys: &[Vec<u8>]| {
                Ok(keys
                    .iter()
                    .filter_map(|key| store.get(key, u64::MAX).map(|value| (key.clone(), value)))
                    .collect())
            })
        };
        let flush = {
            let store = store.clone();
            Arc::new(move |generation, db: Arc<MemDb>| {
                let mut iterator = db.iter(None, None);
                let mut mutations = Vec::new();
                while iterator.valid() {
                    let key = iterator.key().to_vec();
                    let value = iterator.value().to_vec();
                    mutations.push(if value.is_empty() {
                        Mutation::Delete { key }
                    } else {
                        Mutation::Put { key, value }
                    });
                    iterator.next().unwrap();
                }
                store
                    .commit(generation * 2 + 1, generation * 2 + 2, mutations)
                    .map_err(|error| PipelinedError::message(error.to_string()))
            })
        };
        let mut db = PipelinedMemDb::new(remote_get, flush);
        db.set_flush_thresholds(1, 1, 1);
        db.set(b"key", b"value").unwrap();
        assert!(db.flush(true).unwrap());
        db.flush_wait().unwrap();
        assert_eq!(db.get(b"key").unwrap(), b"value");

        db.delete(b"key").unwrap();
        assert!(db.flush(true).unwrap());
        db.flush_wait().unwrap();
        assert_eq!(db.get(b"key"), Err("key not found".to_owned()));
    }
}
