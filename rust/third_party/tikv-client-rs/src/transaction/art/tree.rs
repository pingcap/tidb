// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go `internal/unionstore/art/art.go`: the rollbackable adaptive
//! radix tree over the node arena and the value log.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use super::arena::ArtAllocator;
use super::node::{
    char_at, key_valid, leaf, longest_common_prefix, minimum_leaf_node, ArtNode, NodeKind,
    MAX_IN_NODE_PREFIX_LEN, MAX_KEY_LEN, NULL_ART_NODE,
};
use crate::error::{EntryTooLargeError, KeyTooLargeError, StaticError, TransactionTooLargeError};
use crate::kv::{apply_flags_ops, FlagsOp, KeyFlags};
use crate::transaction::arena::{
    ArenaAddress, KeyFlagsGetter, MemdbArena, MemdbCheckpoint, MemdbValueLog, ValueLogHeader,
    VlogMemDb, NULL_ADDRESS, NULL_U64_ADDRESS,
};

/// Go `ART`.
///
/// Go's iterators and snapshots hold a `*ART`; the Rust ones share this
/// tree behind the lock [`super::Art`] keeps.
pub(crate) struct ArtTree {
    pub(crate) allocator: ArtAllocator,
    pub(crate) root: ArtNode,
    pub(crate) stages: Vec<MemdbCheckpoint>,
    pub(crate) vlog_invalid: bool,
    dirty: bool,
    entry_size_limit: u64,
    buffer_size_limit: u64,
    len: isize,
    size: isize,

    // The traversal cache, updated by readers too, so atomic as in Go.
    last_traversed_node: AtomicU64,
    hit_count: AtomicU64,
    miss_count: AtomicU64,

    /// Go `WriteSeqNo`: every write invalidates the iterators made before it.
    pub(crate) write_seq_no: u64,
    /// Go `SnapshotSeqNo`, shared with the MemDB snapshots that check it.
    pub(crate) snapshot_seq_no: Arc<AtomicU64>,
}

impl ArtTree {
    /// Go `New`.
    pub(crate) fn new() -> Self {
        let mut allocator = ArtAllocator::default();
        allocator.init();
        Self {
            allocator,
            root: NULL_ART_NODE,
            stages: Vec::with_capacity(2),
            vlog_invalid: false,
            dirty: false,
            entry_size_limit: u64::MAX,
            buffer_size_limit: u64::MAX,
            len: 0,
            size: 0,
            last_traversed_node: AtomicU64::new(NULL_U64_ADDRESS),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
            write_seq_no: 0,
            snapshot_seq_no: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Go `ART.Get`.
    pub(crate) fn get(&self, key: &[u8]) -> Result<&[u8], StaticError> {
        assert!(!self.vlog_invalid, "vlog is reset");
        let addr = self.traverse(key);
        if addr.is_null() {
            return Err(StaticError::NotExist);
        }
        let vlog_addr = leaf::vlog_addr(self.allocator.node_data(addr));
        if vlog_addr.is_null() {
            return Err(StaticError::NotExist);
        }
        Ok(self.allocator.vlog_allocator.value(vlog_addr))
    }

    /// Go `ART.GetFlags`.
    pub(crate) fn get_flags(&self, key: &[u8]) -> Result<KeyFlags, StaticError> {
        let addr = self.traverse(key);
        if addr.is_null() {
            return Err(StaticError::NotExist);
        }
        let data = self.allocator.node_data(addr);
        if leaf::vlog_addr(data).is_null() && leaf::is_deleted(data) {
            return Err(StaticError::NotExist);
        }
        Ok(KeyFlags::from_bits(leaf::key_flags(data)))
    }

    /// Go `ART.Set`; a `None` value updates the flags alone.
    pub(crate) fn set(
        &mut self,
        key: &[u8],
        value: Option<&[u8]>,
        ops: &[FlagsOp],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        assert!(!self.vlog_invalid, "vlog is reset");
        if key.len() > MAX_KEY_LEN {
            return Err(Box::new(KeyTooLargeError {
                key_size: key.len() as isize,
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

        self.write_seq_no += 1;
        if self.stages.is_empty() {
            self.dirty = true;
            // note: there is no such usage in TiDB
            self.snapshot_seq_no.fetch_add(1, Ordering::AcqRel);
        }
        // 1. create or search the existing leaf in the tree.
        let addr = self.traverse_insert(key);
        // 2. set the value and flags.
        self.set_value(addr, value, ops);
        if self.size() as u64 > self.buffer_size_limit {
            return Err(Box::new(TransactionTooLargeError {
                size: self.size() as isize,
            }));
        }
        Ok(())
    }

    /// Go `ART.traverse(key, false)`: `search` behind the last-traversed
    /// cache.
    pub(crate) fn traverse(&self, key: &[u8]) -> ArenaAddress {
        if let Some(addr) = self.check_key_in_cache(key) {
            self.hit_count.fetch_add(1, Ordering::Relaxed);
            return addr;
        }
        self.miss_count.fetch_add(1, Ordering::Relaxed);
        let addr = self.search(key);
        if !addr.is_null() {
            self.update_last_traversed(addr);
        }
        addr
    }

    /// Go `ART.traverse(key, true)`: `recursiveInsert` behind the
    /// last-traversed cache.
    fn traverse_insert(&mut self, key: &[u8]) -> ArenaAddress {
        if let Some(addr) = self.check_key_in_cache(key) {
            self.hit_count.fetch_add(1, Ordering::Relaxed);
            return addr;
        }
        self.miss_count.fetch_add(1, Ordering::Relaxed);
        let addr = self.recursive_insert(key);
        if !addr.is_null() {
            self.update_last_traversed(addr);
        }
        addr
    }

    /// Go `ART.search`: the leaf address of `key`, or the null address.
    pub(crate) fn search(&self, key: &[u8]) -> ArenaAddress {
        let a = &self.allocator;
        let mut current = self.root;
        if current == NULL_ART_NODE {
            return NULL_ADDRESS;
        }
        let mut depth = 0u32;
        loop {
            if current.is_leaf() {
                if leaf::matches(a.node_data(current.addr), 0, key) {
                    return current.addr;
                }
                return NULL_ADDRESS;
            }
            let prefix_len = current.prefix_len(a);
            if prefix_len > 0 {
                let matched = current.match_prefix(a, key, depth);
                if matched < prefix_len.min(MAX_IN_NODE_PREFIX_LEN) {
                    return NULL_ADDRESS;
                }
                // A prefix longer than maxInNodePrefixLen matches
                // optimistically; the leaf's full key is checked at last.
                depth += prefix_len;
            }
            let (_, next) = current.find_child(
                a,
                char_at(key, depth as isize),
                !key_valid(key, depth as usize),
            );
            if next.addr.is_null() {
                return NULL_ADDRESS;
            }
            current = next;
            depth += 1;
        }
    }

    /// Go `ART.recursiveInsert`: the leaf of `key`, inserted when absent.
    fn recursive_insert(&mut self, key: &[u8]) -> ArenaAddress {
        // lazy init root node and allocator; this saves memory for read
        // only txns.
        if self.root.addr.is_null() {
            self.root = self.allocator.alloc_node4();
        }

        let mut prev_depth = 0isize;
        let mut current_depth = 0u32;
        let mut prev_node = NULL_ART_NODE;
        let mut current_node = self.root;
        loop {
            if current_node.is_leaf() {
                return self.expand_leaf_if_needed(key, current_depth, prev_node, current_node);
            }

            let prefix_len = current_node.prefix_len(&self.allocator);
            if prefix_len > 0 {
                let mismatch_idx = current_node.match_deep(&self.allocator, key, current_depth);
                if mismatch_idx < prefix_len {
                    // the prefix doesn't match: split the node.
                    return self.expand_node(
                        key,
                        current_depth,
                        mismatch_idx,
                        prev_node,
                        current_node,
                    );
                }
                current_depth += prefix_len;
            }

            // search next node
            let valid = key_valid(key, current_depth as usize);
            let c = char_at(key, current_depth as isize);
            let (_, next) = current_node.find_child(&self.allocator, c, !valid);
            if next == NULL_ART_NODE {
                // insert as leaf if there is no child.
                let new_leaf = self.allocator.alloc_leaf(key);
                if current_node.add_child(&mut self.allocator, c, !valid, new_leaf) {
                    if prev_node == NULL_ART_NODE {
                        self.root = current_node;
                    } else {
                        prev_node.replace_child(
                            &mut self.allocator,
                            char_at(key, prev_depth),
                            current_node,
                        );
                    }
                }
                return new_leaf.addr;
            }
            if !valid && next.kind == NodeKind::Leaf {
                // key is drained, return the leaf.
                return next.addr;
            }
            prev_node = current_node;
            current_node = next;
            prev_depth = current_depth as isize;
            current_depth += 1;
        }
    }

    /// Go `ART.expandLeafIfNeeded`: a leaf whose key differs becomes a node4
    /// holding it and the new leaf.
    fn expand_leaf_if_needed(
        &mut self,
        key: &[u8],
        depth: u32,
        prev_node: ArtNode,
        current_node: ArtNode,
    ) -> ArenaAddress {
        assert!(depth != 0, "Invalid expandLeafIfNeeded input with zero depth");
        let l1_key = self.allocator.leaf_key(current_node.addr);
        if leaf::matches(self.allocator.node_data(current_node.addr), depth - 1, key) {
            // same key, return the artLeaf and overwrite the value.
            return current_node.addr;
        }
        let prev_depth = (depth - 1) as isize;
        // calculate the common prefix length of new node.
        let lcp = longest_common_prefix(l1_key, key, depth);
        let child_depth = depth + lcp;
        let l1_char = char_at(l1_key, child_depth as isize);
        let l1_inplace = !key_valid(l1_key, child_depth as usize);

        let new_leaf_node = self.allocator.alloc_leaf(key);
        let mut new_an = self.allocator.alloc_node4();
        new_an.set_prefix(&mut self.allocator, &key[depth as usize..], lcp);
        new_an.add_child(&mut self.allocator, l1_char, l1_inplace, current_node);
        new_an.add_child(
            &mut self.allocator,
            char_at(key, child_depth as isize),
            !key_valid(key, child_depth as usize),
            new_leaf_node,
        );

        // swap the old leaf with the new node4.
        prev_node.replace_child(&mut self.allocator, char_at(key, prev_depth), new_an);
        new_leaf_node.addr
    }

    /// Go `ART.expandNode`: a prefix mismatch puts a new parent node4 with
    /// the shorter prefix above the node.
    fn expand_node(
        &mut self,
        key: &[u8],
        depth: u32,
        mismatch_idx: u32,
        prev_node: ArtNode,
        current_node: ArtNode,
    ) -> ArenaAddress {
        let prev_depth = depth as isize - 1;

        // set prefix for new node.
        let mut new_an = self.allocator.alloc_node4();
        new_an.set_prefix(&mut self.allocator, &key[depth as usize..], mismatch_idx);

        // update prefix for old node and move it as a child of the new node.
        let prefix_len = current_node.prefix_len(&self.allocator);
        let prefix: Vec<u8> = if prefix_len <= MAX_IN_NODE_PREFIX_LEN {
            // all the prefix is in the prefix array.
            current_node.raw_prefix(&self.allocator).to_vec()
        } else {
            // otherwise the prefix is in any leaf under the node.
            let leaf_art_node = minimum_leaf_node(&self.allocator, current_node);
            self.allocator.leaf_key(leaf_art_node.addr)
                [depth as usize..(depth + prefix_len) as usize]
                .to_vec()
        };
        let node_char = prefix[mismatch_idx as usize];
        current_node.set_prefix(
            &mut self.allocator,
            &prefix[mismatch_idx as usize + 1..],
            prefix_len - mismatch_idx - 1,
        );
        new_an.add_child(&mut self.allocator, node_char, false, current_node);

        // Insert the new artLeaf node.
        let new_leaf = self.allocator.alloc_leaf(key);
        let leaf_depth = depth + mismatch_idx;
        new_an.add_child(
            &mut self.allocator,
            char_at(key, leaf_depth as isize),
            !key_valid(key, leaf_depth as usize),
            new_leaf,
        );
        prev_node.replace_child(&mut self.allocator, char_at(key, prev_depth), new_an);
        new_leaf.addr
    }

    /// Go `ART.setValue`.
    fn set_value(&mut self, addr: ArenaAddress, value: Option<&[u8]>, ops: &[FlagsOp]) {
        let data = self.allocator.node_data(addr);
        let mut flags = KeyFlags::from_bits(leaf::key_flags(data));
        let vlog_addr = leaf::vlog_addr(data);
        if flags.bits() == 0 && vlog_addr.is_null() || leaf::is_deleted(data) {
            self.len += 1;
            self.size += leaf::key(data).len() as isize;
        }
        if value.is_some() {
            let mut flag_ops = Vec::with_capacity(ops.len() + 1);
            flag_ops.push(FlagsOp::DelNeedConstraintCheckInPrewrite);
            flag_ops.extend_from_slice(ops);
            flags = apply_flags_ops(flags, &flag_ops);
        } else {
            // an UpdateFlag operation, do not delete the
            // NeedConstraintCheckInPrewrite flag.
            flags = apply_flags_ops(flags, ops);
        }
        if flags.and_persistent().bits() != 0 {
            self.dirty = true;
        }
        leaf::set_key_flags(self.allocator.node_data_mut(addr), flags.bits());
        let Some(value) = value else {
            // value == nil means it updates flags only.
            return;
        };
        let (old_size, swapped) = self.try_swap_value(vlog_addr, value);
        if swapped {
            return;
        }
        self.size += value.len() as isize - old_size as isize;
        let v_addr = self.allocator.append_value(addr, vlog_addr, value);
        leaf::set_vlog_addr(self.allocator.node_data_mut(addr), v_addr);
    }

    /// Go `ART.trySwapValue`: whether the value was updated in place, else
    /// the old value's size.
    fn try_swap_value(&mut self, addr: ArenaAddress, value: &[u8]) -> (usize, bool) {
        if addr.is_null() {
            return (0, false);
        }
        let old_len = self.allocator.vlog_allocator.value(addr).len();
        if let Some(cp) = self.stages.last() {
            if !self.allocator.vlog_allocator.can_modify(Some(cp), addr) {
                return (old_len, false);
            }
        }
        if old_len > 0 && old_len == value.len() {
            self.allocator
                .vlog_allocator
                .value_mut(addr)
                .copy_from_slice(value);
            return (0, true);
        }
        (old_len, false)
    }

    /// Go `ART.Dirty`.
    pub(crate) fn dirty(&self) -> bool {
        self.dirty
    }

    /// Go `ART.Mem`.
    pub(crate) fn mem(&self) -> u64 {
        self.allocator.mem()
    }

    /// Go `ART.Len`.
    pub(crate) fn len(&self) -> usize {
        self.len as usize
    }

    /// Go `ART.Size`.
    pub(crate) fn size(&self) -> usize {
        self.size as usize
    }

    /// Go `ART.checkpoint`.
    pub(crate) fn checkpoint(&self) -> MemdbCheckpoint {
        self.allocator.vlog_allocator.checkpoint()
    }

    /// Go `ART.RevertToCheckpoint`.
    pub(crate) fn revert_to_checkpoint(&mut self, cp: &MemdbCheckpoint) {
        self.revert_vlog(cp);
        self.allocator.vlog_allocator.arena_mut().truncate(cp);
        self.allocator.on_mem_change();
        self.write_seq_no += 1;
        if self.stages.is_empty() || self.stages[0].is_before(cp) {
            self.snapshot_seq_no.fetch_add(1, Ordering::AcqRel);
        }
    }

    /// Go `vlogAllocator.RevertToCheckpoint(t, cp)`, with the tree's leaves
    /// and counters as the `VlogMemDB`.
    fn revert_vlog(&mut self, cp: &MemdbCheckpoint) {
        let mut reverter = LeafReverter {
            nodes: &mut self.allocator.node_allocator.arena,
            vlog: &self.allocator.vlog_allocator,
            len: &mut self.len,
            size: &mut self.size,
        };
        self.allocator
            .vlog_allocator
            .revert_to_checkpoint::<LeafView<'_>, _>(&mut reverter, cp);
    }

    /// Go `ART.Staging`.
    pub(crate) fn staging(&mut self) -> usize {
        self.stages.push(self.checkpoint());
        self.stages.len()
    }

    /// Go `ART.Release`.
    pub(crate) fn release(&mut self, h: usize) {
        if h == 0 {
            // 0 is the invalid and no-effect handle.
            return;
        }
        assert!(h == self.stages.len(), "cannot release staging buffer");
        self.write_seq_no += 1;
        if h == 1 {
            self.snapshot_seq_no.fetch_add(1, Ordering::AcqRel);
            let tail = self.checkpoint();
            if !self.stages[0].is_same_position(&tail) {
                self.dirty = true;
            }
        }
        self.stages.truncate(h - 1);
    }

    /// Go `ART.Cleanup`.
    pub(crate) fn cleanup(&mut self, h: usize) {
        if h == 0 {
            // 0 is the invalid and no-effect handle.
            return;
        }
        if h > self.stages.len() {
            return;
        }
        assert!(
            h >= self.stages.len(),
            "cannot cleanup staging buffer, h={}, len(tree.stages)={}",
            h,
            self.stages.len()
        );

        self.write_seq_no += 1;
        if h == 1 {
            self.snapshot_seq_no.fetch_add(1, Ordering::AcqRel);
        }

        let cp = self.stages[h - 1];
        if !self.vlog_invalid {
            let curr = self.checkpoint();
            if !curr.is_same_position(&cp) {
                self.revert_vlog(&cp);
                self.allocator.vlog_allocator.arena_mut().truncate(&cp);
            }
        }
        self.stages.truncate(h - 1);
        self.allocator.on_mem_change();
    }

    /// Go `ART.Reset`.
    pub(crate) fn reset(&mut self) {
        self.root = NULL_ART_NODE;
        self.stages.clear();
        self.dirty = false;
        self.vlog_invalid = false;
        self.size = 0;
        self.len = 0;
        self.allocator.node_allocator.arena.reset();
        self.allocator.on_mem_change();
        self.allocator.vlog_allocator.arena_mut().reset();
        self.allocator.on_mem_change();
        self.last_traversed_node
            .store(NULL_U64_ADDRESS, Ordering::Release);
        self.snapshot_seq_no.fetch_add(1, Ordering::AcqRel);
        self.write_seq_no += 1;
    }

    /// Go `ART.DiscardValues`: any value read after it panics.
    pub(crate) fn discard_values(&mut self) {
        self.vlog_invalid = true;
        self.allocator.vlog_allocator.arena_mut().reset();
        self.allocator.on_mem_change();
    }

    /// Go `ART.InspectStage`.
    pub(crate) fn inspect_stage(&self, handle: usize, f: impl FnMut(&[u8], KeyFlags, &[u8])) {
        let tail = self.allocator.vlog_allocator.checkpoint();
        let head = self.stages[handle - 1];
        let inspector = LeafInspector {
            nodes: &self.allocator.node_allocator.arena,
        };
        self.allocator
            .vlog_allocator
            .inspect_key_values::<LeafView<'_>, _>(&inspector, &head, &tail, f);
    }

    /// Go `ART.SelectValueHistory`.
    pub(crate) fn select_value_history(
        &self,
        key: &[u8],
        mut predicate: impl FnMut(&[u8]) -> bool,
    ) -> Result<Option<&[u8]>, StaticError> {
        let addr = self.search(key);
        if addr.is_null() {
            return Err(StaticError::NotExist);
        }
        let vlog_addr = leaf::vlog_addr(self.allocator.node_data(addr));
        if vlog_addr.is_null() {
            // A flags only key, act as value not exists
            return Err(StaticError::NotExist);
        }
        let vlog = &self.allocator.vlog_allocator;
        let result = vlog.select_value_history(vlog_addr, |addr| predicate(vlog.value(addr)));
        if result.is_null() {
            return Ok(None);
        }
        Ok(Some(vlog.value(result)))
    }

    /// Go `ART.SetMemoryFootprintChangeHook`.
    pub(crate) fn set_memory_footprint_change_hook(
        &mut self,
        hook: Arc<dyn Fn(u64) + Send + Sync>,
    ) {
        self.allocator.memory_change_hook = Some(hook);
    }

    /// Go `ART.MemHookSet`.
    pub(crate) fn mem_hook_set(&self) -> bool {
        self.allocator.memory_change_hook.is_some()
    }

    /// Go `ART.GetKeyByHandle`.
    pub(crate) fn get_key_by_handle(&self, addr: ArenaAddress) -> &[u8] {
        self.allocator.leaf_key(addr)
    }

    /// Go `ART.GetValueByHandle`.
    pub(crate) fn get_value_by_handle(&self, addr: ArenaAddress) -> Option<&[u8]> {
        if self.vlog_invalid {
            return None;
        }
        let vlog_addr = leaf::vlog_addr(self.allocator.node_data(addr));
        if vlog_addr.is_null() {
            return None;
        }
        Some(self.allocator.vlog_allocator.value(vlog_addr))
    }

    /// Go `ART.GetEntrySizeLimit`.
    pub(crate) fn get_entry_size_limit(&self) -> (u64, u64) {
        (self.entry_size_limit, self.buffer_size_limit)
    }

    /// Go `ART.SetEntrySizeLimit`.
    pub(crate) fn set_entry_size_limit(&mut self, entry_limit: u64, buffer_limit: u64) {
        self.entry_size_limit = entry_limit;
        self.buffer_size_limit = buffer_limit;
    }

    /// Go `ART.updateLastTraversed`.
    fn update_last_traversed(&self, addr: ArenaAddress) {
        self.last_traversed_node
            .store(addr.as_u64(), Ordering::Release);
    }

    /// Go `ART.checkKeyInCache`.
    fn check_key_in_cache(&self, key: &[u8]) -> Option<ArenaAddress> {
        let addr_u64 = self.last_traversed_node.load(Ordering::Acquire);
        if addr_u64 == NULL_U64_ADDRESS {
            return None;
        }
        let addr = ArenaAddress::from_u64(addr_u64);
        (self.allocator.leaf_key(addr) == key).then_some(addr)
    }

    /// Go `ART.GetCacheHitCount`.
    pub(crate) fn get_cache_hit_count(&self) -> u64 {
        self.hit_count.load(Ordering::Relaxed)
    }

    /// Go `ART.GetCacheMissCount`.
    pub(crate) fn get_cache_miss_count(&self) -> u64 {
        self.miss_count.load(Ordering::Relaxed)
    }

    /// Go `ART.getSnapshotCheckpoint`.
    pub(crate) fn get_snapshot_checkpoint(&self) -> MemdbCheckpoint {
        self.stages.first().copied().unwrap_or_else(|| self.checkpoint())
    }
}

/// Go `*artLeaf` as the vlog's `KeyFlagsGetter`.
pub(crate) struct LeafView<'a> {
    bytes: &'a [u8],
}

impl KeyFlagsGetter for LeafView<'_> {
    fn key(&self) -> &[u8] {
        leaf::key(self.bytes)
    }

    fn key_flags(&self) -> KeyFlags {
        KeyFlags::from_bits(leaf::key_flags(self.bytes))
    }
}

/// Go `ART.RevertVAddr`'s side of `VlogMemDB`, over the leaves and counters
/// the value log's revert writes while it reads the log itself.
struct LeafReverter<'a> {
    nodes: &'a mut MemdbArena,
    vlog: &'a MemdbValueLog,
    len: &'a mut isize,
    size: &'a mut isize,
}

impl<'a> VlogMemDb<LeafView<'a>> for LeafReverter<'a> {
    /// Go `ART.RevertVAddr`.
    fn revert_value_address(&mut self, header: &ValueLogHeader) {
        assert!(!header.node_address.is_null(), "revert an invalid node");
        let lf = self.nodes.data_mut(header.node_address);
        leaf::set_vlog_addr(lf, header.old_value);
        *self.size -= header.value_length as isize;
        if header.old_value.is_null() {
            let kept_flags = KeyFlags::from_bits(leaf::key_flags(lf)).and_persistent();
            if kept_flags.bits() == 0 {
                leaf::mark_delete(lf);
                *self.len -= 1;
                *self.size -= leaf::key(lf).len() as isize;
            } else {
                leaf::set_key_flags(lf, kept_flags.bits());
            }
        } else {
            *self.size += self.vlog.value(header.old_value).len() as isize;
        }
    }

    fn inspect_node(&self, _: ArenaAddress) -> (LeafView<'a>, ArenaAddress) {
        unreachable!("RevertToCheckpoint never inspects a node")
    }
}

/// Go `ART.InspectNode`'s side of `VlogMemDB`.
struct LeafInspector<'a> {
    nodes: &'a MemdbArena,
}

impl<'a> VlogMemDb<LeafView<'a>> for LeafInspector<'a> {
    fn revert_value_address(&mut self, _: &ValueLogHeader) {
        unreachable!("InspectKVInLog never reverts a value")
    }

    /// Go `ART.InspectNode`.
    fn inspect_node(&self, addr: ArenaAddress) -> (LeafView<'a>, ArenaAddress) {
        let bytes = self.nodes.data(addr);
        (LeafView { bytes }, leaf::vlog_addr(bytes))
    }
}
