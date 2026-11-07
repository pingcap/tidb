// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go `internal/unionstore/art/art_arena.go`: the node arena with
//! its per-kind free lists, and the allocator pairing it with the value log.

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use super::node::{
    init_leaf, init_node, ArtNode, NodeKind, LEAF_SIZE, NODE16_SIZE, NODE256_SIZE, NODE48_SIZE,
    NODE4_SIZE,
};
use crate::transaction::arena::{ArenaAddress, MemdbArena, MemdbValueLog};

/// Go `nodeArena`.
#[derive(Default)]
pub(crate) struct NodeArena {
    pub(crate) arena: MemdbArena,

    // The ART node will expand to a higher capacity, and the address of the
    // freed node is kept in the free list for reuse.
    free_node4: Vec<ArenaAddress>,
    free_node16: Vec<ArenaAddress>,
    free_node48: Vec<ArenaAddress>,

    // While a snapshot iterator is open, ART keeps the old versions
    // available: freed nodes go to the unused lists until it closes.
    blocked_snapshot_cnt: AtomicI64,
    is_unused_node_freeing: AtomicBool,
    unused_node4: Vec<ArenaAddress>,
    unused_node16: Vec<ArenaAddress>,
    unused_node48: Vec<ArenaAddress>,
}

/// Go `artAllocator`.
#[derive(Default)]
pub(crate) struct ArtAllocator {
    pub(crate) vlog_allocator: MemdbValueLog,
    pub(crate) node_allocator: NodeArena,
    /// Go `ART.SetMemoryFootprintChangeHook`'s hook, which Go installs on
    /// both arenas as `hook(nodeAllocator.Capacity() + vlogAllocator.Capacity())`.
    /// It lives here, beside the two arenas, because the hook reads both.
    pub(crate) memory_change_hook: Option<Arc<dyn Fn(u64) + Send + Sync>>,
}

impl ArtAllocator {
    /// Go `artAllocator.init`.
    pub(crate) fn init(&mut self) {
        self.node_allocator.free_node4 = Vec::with_capacity(1 << 4);
        self.node_allocator.free_node16 = Vec::with_capacity(1 << 3);
        self.node_allocator.free_node48 = Vec::with_capacity(1 << 2);
    }

    /// Go `ART.Mem`: the capacity of both arenas.
    pub(crate) fn mem(&self) -> u64 {
        self.node_allocator.arena.capacity() + self.vlog_allocator.arena().capacity()
    }

    /// Go `MemdbArena.OnMemChange` with the hook `SetMemoryFootprintChangeHook`
    /// installs.
    pub(crate) fn on_mem_change(&self) {
        if let Some(hook) = &self.memory_change_hook {
            hook(self.mem());
        }
    }

    /// Go `vlogAllocator.AppendValue`, whose new block fires the hook.
    pub(crate) fn append_value(
        &mut self,
        node_addr: ArenaAddress,
        old_value_addr: ArenaAddress,
        value: &[u8],
    ) -> ArenaAddress {
        let prev_blocks = self.vlog_allocator.arena().block_count();
        let addr = self
            .vlog_allocator
            .append_value(node_addr, old_value_addr, value);
        if prev_blocks != self.vlog_allocator.arena().block_count() {
            self.on_mem_change();
        }
        addr
    }

    /// Go `nodeAllocator.GetData`.
    pub(crate) fn node_data(&self, addr: ArenaAddress) -> &[u8] {
        self.node_allocator.arena.data(addr)
    }

    pub(crate) fn node_data_mut(&mut self, addr: ArenaAddress) -> &mut [u8] {
        self.node_allocator.arena.data_mut(addr)
    }

    fn alloc_node(&mut self, kind: NodeKind, size: usize) -> ArtNode {
        let free = match kind {
            NodeKind::Node4 => self.node_allocator.free_node4.pop(),
            NodeKind::Node16 => self.node_allocator.free_node16.pop(),
            NodeKind::Node48 => self.node_allocator.free_node48.pop(),
            _ => None,
        };
        let addr = match free {
            Some(addr) => {
                init_node(self.node_allocator.arena.data_mut(addr), kind);
                addr
            }
            None => {
                let (addr, data) = self.node_allocator.arena.allocate(size, true);
                init_node(data, kind);
                addr
            }
        };
        ArtNode { kind, addr }
    }

    /// Go `artAllocator.allocNode4`.
    pub(crate) fn alloc_node4(&mut self) -> ArtNode {
        self.alloc_node(NodeKind::Node4, NODE4_SIZE)
    }

    /// Go `artAllocator.allocNode16`.
    pub(crate) fn alloc_node16(&mut self) -> ArtNode {
        self.alloc_node(NodeKind::Node16, NODE16_SIZE)
    }

    /// Go `artAllocator.allocNode48`.
    pub(crate) fn alloc_node48(&mut self) -> ArtNode {
        self.alloc_node(NodeKind::Node48, NODE48_SIZE)
    }

    /// Go `artAllocator.allocNode256`: a node256 is never freed, so never
    /// reused.
    pub(crate) fn alloc_node256(&mut self) -> ArtNode {
        self.alloc_node(NodeKind::Node256, NODE256_SIZE)
    }

    fn free_node(&mut self, kind: NodeKind, addr: ArenaAddress) {
        let blocked = self.node_allocator.blocked_snapshot_cnt.load(Ordering::SeqCst) != 0;
        let list = match (kind, blocked) {
            (NodeKind::Node4, false) => &mut self.node_allocator.free_node4,
            (NodeKind::Node4, true) => &mut self.node_allocator.unused_node4,
            (NodeKind::Node16, false) => &mut self.node_allocator.free_node16,
            (NodeKind::Node16, true) => &mut self.node_allocator.unused_node16,
            (NodeKind::Node48, false) => &mut self.node_allocator.free_node48,
            (NodeKind::Node48, true) => &mut self.node_allocator.unused_node48,
            _ => panic!("node kind is never freed"),
        };
        list.push(addr);
    }

    /// Go `artAllocator.freeNode4`.
    pub(crate) fn free_node4(&mut self, addr: ArenaAddress) {
        self.free_node(NodeKind::Node4, addr);
    }

    /// Go `artAllocator.freeNode16`.
    pub(crate) fn free_node16(&mut self, addr: ArenaAddress) {
        self.free_node(NodeKind::Node16, addr);
    }

    /// Go `artAllocator.freeNode48`.
    pub(crate) fn free_node48(&mut self, addr: ArenaAddress) {
        self.free_node(NodeKind::Node48, addr);
    }

    /// Go `artAllocator.allocLeaf`.
    pub(crate) fn alloc_leaf(&mut self, key: &[u8]) -> ArtNode {
        let (addr, data) = self
            .node_allocator
            .arena
            .allocate(LEAF_SIZE + key.len(), true);
        init_leaf(data, key);
        ArtNode {
            kind: NodeKind::Leaf,
            addr,
        }
    }

    /// Go `artAllocator.getLeaf(addr).GetKey()`.
    pub(crate) fn leaf_key(&self, addr: ArenaAddress) -> &[u8] {
        super::node::leaf::key(self.node_data(addr))
    }

    /// Go `artAllocator.snapshotInc`.
    pub(crate) fn snapshot_inc(&self) {
        self.node_allocator
            .blocked_snapshot_cnt
            .fetch_add(1, Ordering::SeqCst);
    }

    /// Go `artAllocator.snapshotDec`: the last open snapshot iterator moves
    /// the unused old-version nodes into the free lists.
    pub(crate) fn snapshot_dec(&mut self) {
        let nodes = &mut self.node_allocator;
        if nodes.blocked_snapshot_cnt.fetch_sub(1, Ordering::SeqCst) - 1 != 0 {
            return;
        }
        if nodes
            .is_unused_node_freeing
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        nodes.free_node4.append(&mut nodes.unused_node4);
        nodes.free_node16.append(&mut nodes.unused_node16);
        nodes.free_node48.append(&mut nodes.unused_node48);
        nodes.is_unused_node_freeing.store(false, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(crate) fn unused_node_lens(&self) -> (usize, usize, usize) {
        (
            self.node_allocator.unused_node4.len(),
            self.node_allocator.unused_node16.len(),
            self.node_allocator.unused_node48.len(),
        )
    }
}
