// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go `internal/unionstore/art`: the adaptive radix tree that is
//! client-go's default MemDB.
//!
//! You can think of ART as two tree maps, key => value and key => flags. The
//! value map is rollbackable through `Staging`, `Release` and `Cleanup`; the
//! flags map is not. When `Cleanup` discards a newly added key, its
//! non-persistent flags are cleared; a key with persistent flags stays in the
//! tree without a value.

mod arena;
mod iterator;
mod node;
mod snapshot;
mod tree;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

pub(crate) use iterator::ArtIterator;
pub(crate) use snapshot::{ArtSnapshot, SnapshotIterator};
use tree::ArtTree;

use crate::error::StaticError;
use crate::kv::{FlagsOp, KeyFlags};
use crate::transaction::arena::{ArenaAddress, MemdbCheckpoint};

/// The tree Go's `*ART` names, shared with the iterators and snapshots that
/// read it.
pub(crate) type SharedTree = Arc<RwLock<ArtTree>>;

/// Go has no lock to poison: a panic inside the tree (an invalidated
/// iterator, a reset value log) leaves it readable as Go's does.
fn read(tree: &SharedTree) -> RwLockReadGuard<'_, ArtTree> {
    tree.read().unwrap_or_else(PoisonError::into_inner)
}

fn write(tree: &SharedTree) -> RwLockWriteGuard<'_, ArtTree> {
    tree.write().unwrap_or_else(PoisonError::into_inner)
}

/// Go `arena.MemKeyHandle` of an ART leaf.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ArtHandle(pub(crate) ArenaAddress);

/// Go `ART`.
pub(crate) struct Art {
    tree: SharedTree,
}

impl Default for Art {
    fn default() -> Self {
        Self::new()
    }
}

impl Art {
    /// Go `New`.
    pub(crate) fn new() -> Self {
        Self {
            tree: Arc::new(RwLock::new(ArtTree::new())),
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, ArtTree> {
        read(&self.tree)
    }

    fn write(&self) -> RwLockWriteGuard<'_, ArtTree> {
        write(&self.tree)
    }

    /// Go `ART.WriteSeqNo`.
    pub(crate) fn write_sequence(&self) -> u64 {
        self.read().write_seq_no
    }

    /// Go `ART.SnapshotSeqNo`.
    pub(crate) fn snapshot_sequence(&self) -> u64 {
        self.read().snapshot_seq_no.load(Ordering::Acquire)
    }

    pub(crate) fn snapshot_sequence_counter(&self) -> Arc<AtomicU64> {
        self.read().snapshot_seq_no.clone()
    }

    /// Go `ART.IsStaging`.
    pub(crate) fn is_staging(&self) -> bool {
        !self.read().stages.is_empty()
    }

    /// Go `ART.Staging`.
    pub(crate) fn staging(&mut self) -> usize {
        self.write().staging()
    }

    /// Go `ART.Release`.
    pub(crate) fn release(&mut self, handle: usize) {
        self.write().release(handle);
    }

    /// Go `ART.Cleanup`.
    pub(crate) fn cleanup(&mut self, handle: usize) {
        self.write().cleanup(handle);
    }

    /// Go `ART.Checkpoint`.
    pub(crate) fn checkpoint(&self) -> MemdbCheckpoint {
        self.read().checkpoint()
    }

    /// Go `ART.Stages`.
    pub(crate) fn stages(&self) -> Vec<MemdbCheckpoint> {
        self.read().stages.clone()
    }

    /// Go `ART.RevertToCheckpoint`.
    pub(crate) fn revert_to_checkpoint(&mut self, checkpoint: &MemdbCheckpoint) {
        self.write().revert_to_checkpoint(checkpoint);
    }

    /// Go `ART.Reset`.
    pub(crate) fn reset(&mut self) {
        self.write().reset();
    }

    /// Go `ART.DiscardValues`: any operation that needs a value panics after
    /// it.
    pub(crate) fn discard_values(&mut self) {
        self.write().discard_values();
    }

    /// Go `ART.SetEntrySizeLimit`.
    pub(crate) fn set_entry_size_limit(&mut self, entry_limit: u64, buffer_limit: u64) {
        self.write().set_entry_size_limit(entry_limit, buffer_limit);
    }

    /// Go `ART.GetEntrySizeLimit`.
    pub(crate) fn entry_size_limit(&self) -> (u64, u64) {
        self.read().get_entry_size_limit()
    }

    /// The next generation of a pipelined MemDB: a new tree with this one's
    /// limits and memory hook.
    pub(crate) fn empty_generation(&self) -> Self {
        let next = Self::new();
        {
            let current = self.read();
            let mut tree = next.write();
            let (entry_limit, buffer_limit) = current.get_entry_size_limit();
            tree.set_entry_size_limit(entry_limit, buffer_limit);
            tree.allocator.memory_change_hook = current.allocator.memory_change_hook.clone();
        }
        next
    }

    /// Go `ART.SetMemoryFootprintChangeHook`.
    pub(crate) fn set_memory_footprint_change_hook(
        &mut self,
        hook: Arc<dyn Fn(u64) + Send + Sync>,
    ) {
        self.write().set_memory_footprint_change_hook(hook);
    }

    /// Go `ART.MemHookSet`.
    pub(crate) fn memory_hook_is_set(&self) -> bool {
        self.read().mem_hook_set()
    }

    pub(crate) fn take_memory_hook(&mut self) -> Option<Arc<dyn Fn(u64) + Send + Sync>> {
        self.write().allocator.memory_change_hook.take()
    }

    /// Go `ART.Mem`: the capacity of the node and value arenas.
    pub(crate) fn memory_footprint(&self) -> u64 {
        self.read().mem()
    }

    /// Go `ART.Set`; a `None` value updates the flags alone.
    pub(crate) fn set(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        operations: &[FlagsOp],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.write().set(key, value, operations)
    }

    /// Go `ART.Get`.
    pub(crate) fn get(&self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        self.read().get(key).map(<[u8]>::to_vec)
    }

    /// Go `ART.GetFlags`.
    pub(crate) fn flags(&self, key: &[u8]) -> Result<KeyFlags, StaticError> {
        self.read().get_flags(key)
    }

    /// Go `ART.SelectValueHistory`.
    pub(crate) fn select_value_history(
        &self,
        key: &[u8],
        predicate: impl FnMut(&[u8]) -> bool,
    ) -> Result<Option<Vec<u8>>, StaticError> {
        self.read()
            .select_value_history(key, predicate)
            .map(|value| value.map(<[u8]>::to_vec))
    }

    /// Go `ART.GetKeyByHandle`.
    pub(crate) fn key_by_handle(&self, handle: ArtHandle) -> Vec<u8> {
        self.read().get_key_by_handle(handle.0).to_vec()
    }

    /// Go `ART.GetValueByHandle`.
    pub(crate) fn value_by_handle(&self, handle: ArtHandle) -> Option<Vec<u8>> {
        self.read().get_value_by_handle(handle.0).map(<[u8]>::to_vec)
    }

    /// Go `ART.Len`.
    pub(crate) fn len(&self) -> usize {
        self.read().len()
    }

    /// Go `ART.Size`.
    pub(crate) fn size(&self) -> usize {
        self.read().size()
    }

    /// Go `ART.Dirty`.
    pub(crate) fn dirty(&self) -> bool {
        self.read().dirty()
    }

    /// Go `ART.GetCacheHitCount`.
    pub(crate) fn cache_hit_count(&self) -> u64 {
        self.read().get_cache_hit_count()
    }

    /// Go `ART.GetCacheMissCount`.
    pub(crate) fn cache_miss_count(&self) -> u64 {
        self.read().get_cache_miss_count()
    }

    /// Go `ART.InspectStage`.
    pub(crate) fn inspect_stage(
        &self,
        handle: usize,
        function: impl FnMut(&[u8], KeyFlags, &[u8]),
    ) {
        self.read().inspect_stage(handle, function);
    }

    fn iterator(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
        include_flags: bool,
    ) -> ArtIterator {
        ArtIterator::new(self.tree.clone(), lower, upper, reverse, include_flags)
            .expect("a new ART iterator positions on its first key")
    }

    /// Go `ART.Iter`.
    pub(crate) fn iter(&self, lower: Option<&[u8]>, upper: Option<&[u8]>) -> ArtIterator {
        self.iterator(lower, upper, false, false)
    }

    /// Go `ART.IterReverse`.
    pub(crate) fn iter_reverse(&self, upper: Option<&[u8]>, lower: Option<&[u8]>) -> ArtIterator {
        self.iterator(lower, upper, true, false)
    }

    /// Go `ART.IterWithFlags`.
    pub(crate) fn iter_with_flags(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> ArtIterator {
        self.iterator(lower, upper, false, true)
    }

    /// Go `ART.IterReverseWithFlags`.
    pub(crate) fn iter_reverse_with_flags(&self, upper: Option<&[u8]>) -> ArtIterator {
        self.iterator(None, upper, true, true)
    }

    /// Go `ART.GetSnapshot`.
    pub(crate) fn snapshot(&self) -> ArtSnapshot {
        ArtSnapshot::new(self.tree.clone())
    }

    /// Go `ART.RemoveFromBuffer`, which client-go's ART does not support.
    pub(crate) fn remove_from_buffer(&mut self, _key: &[u8]) {
        panic!("unimplemented");
    }
}

#[cfg(test)]
mod source_tests;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod internal_tests;
