// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go `internal/unionstore/art/art_node.go`: the adaptive radix
//! tree's node kinds, their layout in the node arena, and the node
//! operations the tree runs.
//!
//! Go keeps each node as a struct inside an arena block and reaches it
//! through an `unsafe.Pointer` cast. This port keeps the same bytes at the
//! same offsets (the field layout `unsafe.Sizeof` measures on 64-bit Go) and
//! reads and writes them through little-endian accessors, so every node takes
//! the arena space Go's takes and every address is Go's.

use super::arena::ArtAllocator;
use crate::transaction::arena::{ArenaAddress, NULL_ADDRESS};

/// Go `nodeKind`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub(crate) enum NodeKind {
    Invalid = 0,
    Node4 = 1,
    Node16 = 2,
    Node48 = 3,
    Node256 = 4,
    Leaf = 5,
}

impl NodeKind {
    fn from_u16(kind: u16) -> Self {
        match kind {
            1 => Self::Node4,
            2 => Self::Node16,
            3 => Self::Node48,
            4 => Self::Node256,
            5 => Self::Leaf,
            _ => Self::Invalid,
        }
    }
}

/// Go `MaxKeyLen`.
pub(crate) const MAX_KEY_LEN: usize = u16::MAX as usize;
/// Go `maxInNodePrefixLen`.
pub(crate) const MAX_IN_NODE_PREFIX_LEN: u32 = 20;
pub(crate) const NODE4_CAP: usize = 4;
pub(crate) const NODE16_CAP: usize = 16;
pub(crate) const NODE48_CAP: usize = 48;
pub(crate) const NODE256_CAP: usize = 256;
/// Go `inplaceIndex`: the in-place leaf of a node, which shares the node's
/// key and takes no child slot.
pub(crate) const INPLACE_INDEX: isize = -1;
/// Go `notExistIndex`.
pub(crate) const NOT_EXIST_INDEX: isize = -2;

/// Go `unsafe.Sizeof(node4{})` (measured).
pub(crate) const NODE4_SIZE: usize = 92;
/// Go `unsafe.Sizeof(node16{})` (measured).
pub(crate) const NODE16_SIZE: usize = 248;
/// Go `unsafe.Sizeof(node48{})` (measured).
pub(crate) const NODE48_SIZE: usize = 904;
/// Go `unsafe.Sizeof(node256{})` (measured).
pub(crate) const NODE256_SIZE: usize = 3144;
/// Go `unsafe.Sizeof(artLeaf{})` (measured); the key follows it.
pub(crate) const LEAF_SIZE: usize = 12;

// `nodeBase` field offsets.
const NODE_NUM: usize = 0;
const PREFIX_LEN: usize = 4;
const PREFIX: usize = 8;
const INPLACE_LEAF: usize = 28;
const NODE_BASE_SIZE: usize = 40;
/// `unsafe.Sizeof(artNode{})`: `kind uint16`, padding, `addr {idx, off uint32}`.
const ART_NODE_SIZE: usize = 12;
// Per-kind field offsets.
const NODE4_KEYS: usize = NODE_BASE_SIZE;
const NODE4_CHILDREN: usize = NODE4_KEYS + NODE4_CAP;
const NODE16_KEYS: usize = NODE_BASE_SIZE;
const NODE16_CHILDREN: usize = NODE16_KEYS + NODE16_CAP;
const PRESENT: usize = NODE_BASE_SIZE;
const NODE48_KEYS: usize = PRESENT + 32;
const NODE48_CHILDREN: usize = NODE48_KEYS + NODE256_CAP;
const NODE256_CHILDREN: usize = PRESENT + 32;
// `artLeaf` field offsets.
const LEAF_VLOG_ADDR: usize = 0;
const LEAF_KEY_LEN: usize = 8;
const LEAF_FLAGS: usize = 10;

/// Go `n48s` / `n48m`: the present bitmap's word shift and width.
const N48S: usize = 6;
const N48M: usize = 64;

/// Go `deleteFlag` / `flagMask` on a leaf's flags.
pub(crate) const DELETE_FLAG: u16 = 1 << 15;
pub(crate) const FLAG_MASK: u16 = !DELETE_FLAG;

/// Go `artNode`: a node's kind and its address in the node arena.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ArtNode {
    pub(crate) kind: NodeKind,
    pub(crate) addr: ArenaAddress,
}

/// Go `nullArtNode`.
pub(crate) const NULL_ART_NODE: ArtNode = ArtNode {
    kind: NodeKind::Invalid,
    addr: NULL_ADDRESS,
};

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes([bytes[offset], bytes[offset + 1]])
}

fn write_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("four bytes"))
}

fn write_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("eight bytes"))
}

fn write_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn read_addr(bytes: &[u8], offset: usize) -> ArenaAddress {
    ArenaAddress::new(read_u32(bytes, offset), read_u32(bytes, offset + 4))
}

fn write_addr(bytes: &mut [u8], offset: usize, addr: ArenaAddress) {
    let raw = addr.as_u64();
    write_u32(bytes, offset, (raw >> 32) as u32);
    write_u32(bytes, offset + 4, raw as u32);
}

fn read_art_node(bytes: &[u8], offset: usize) -> ArtNode {
    ArtNode {
        kind: NodeKind::from_u16(read_u16(bytes, offset)),
        addr: read_addr(bytes, offset + 4),
    }
}

fn write_art_node(bytes: &mut [u8], offset: usize, node: ArtNode) {
    write_u16(bytes, offset, node.kind as u16);
    write_addr(bytes, offset + 4, node.addr);
}

fn children_offset(kind: NodeKind) -> usize {
    match kind {
        NodeKind::Node4 => NODE4_CHILDREN,
        NodeKind::Node16 => NODE16_CHILDREN,
        NodeKind::Node48 => NODE48_CHILDREN,
        NodeKind::Node256 => NODE256_CHILDREN,
        NodeKind::Invalid | NodeKind::Leaf => panic!("invalid nodeBase kind"),
    }
}

/// Go `nodeBase.init()` and each node kind's `init`: the header, and the
/// present bitmap of a node48 / node256. Keys and children are left as the
/// arena holds them (a reused node keeps its stale slots, as Go's does);
/// `nodeNum` says which are live.
pub(crate) fn init_node(bytes: &mut [u8], kind: NodeKind) {
    bytes[NODE_NUM] = 0;
    write_u32(bytes, PREFIX_LEN, 0);
    write_art_node(bytes, INPLACE_LEAF, NULL_ART_NODE);
    if matches!(kind, NodeKind::Node48 | NodeKind::Node256) {
        for word in 0..4 {
            write_u64(bytes, PRESENT + word * 8, 0);
        }
    }
}

/// Go `artLeaf` initialization in `allocLeaf`: key length, no flags, no
/// value, then the key bytes.
pub(crate) fn init_leaf(bytes: &mut [u8], key: &[u8]) {
    write_addr(bytes, LEAF_VLOG_ADDR, NULL_ADDRESS);
    write_u16(bytes, LEAF_KEY_LEN, key.len() as u16);
    write_u16(bytes, LEAF_FLAGS, 0);
    bytes[LEAF_SIZE..LEAF_SIZE + key.len()].copy_from_slice(key);
}

impl ArtNode {
    /// Go `artNode.isLeaf`.
    pub(crate) fn is_leaf(self) -> bool {
        self.kind == NodeKind::Leaf
    }

    fn bytes(self, a: &ArtAllocator) -> &[u8] {
        a.node_data(self.addr)
    }

    fn bytes_mut(self, a: &mut ArtAllocator) -> &mut [u8] {
        a.node_data_mut(self.addr)
    }

    /// Go `nodeBase.nodeNum`.
    pub(crate) fn node_num(self, a: &ArtAllocator) -> u8 {
        self.bytes(a)[NODE_NUM]
    }

    fn set_node_num(self, a: &mut ArtAllocator, value: u8) {
        self.bytes_mut(a)[NODE_NUM] = value;
    }

    /// Go `nodeBase.prefixLen`.
    pub(crate) fn prefix_len(self, a: &ArtAllocator) -> u32 {
        read_u32(self.bytes(a), PREFIX_LEN)
    }

    /// Go `nodeBase.prefix[:min(prefixLen, maxInNodePrefixLen)]`.
    pub(crate) fn prefix(self, a: &ArtAllocator) -> &[u8] {
        let len = self.prefix_len(a).min(MAX_IN_NODE_PREFIX_LEN) as usize;
        &self.bytes(a)[PREFIX..PREFIX + len]
    }

    /// Go `nodeBase.inplaceLeaf`.
    pub(crate) fn inplace_leaf(self, a: &ArtAllocator) -> ArtNode {
        read_art_node(self.bytes(a), INPLACE_LEAF)
    }

    pub(crate) fn set_inplace_leaf(self, a: &mut ArtAllocator, leaf: ArtNode) {
        write_art_node(self.bytes_mut(a), INPLACE_LEAF, leaf);
    }

    /// Go `nodeBase.setPrefix`.
    pub(crate) fn set_prefix(self, a: &mut ArtAllocator, key: &[u8], prefix_len: u32) {
        let bytes = self.bytes_mut(a);
        write_u32(bytes, PREFIX_LEN, prefix_len);
        let copied = prefix_len.min(MAX_IN_NODE_PREFIX_LEN) as usize;
        bytes[PREFIX..PREFIX + copied].copy_from_slice(&key[..copied]);
    }

    /// Go `nodeBase.prefixLen = n` alone (the stored prefix bytes unchanged).
    pub(crate) fn set_prefix_len(self, a: &mut ArtAllocator, prefix_len: u32) {
        write_u32(self.bytes_mut(a), PREFIX_LEN, prefix_len);
    }

    /// Go `copy(n.prefix[:], src)`.
    pub(crate) fn copy_prefix_bytes(self, a: &mut ArtAllocator, src: &[u8]) {
        let copied = src.len().min(MAX_IN_NODE_PREFIX_LEN as usize);
        self.bytes_mut(a)[PREFIX..PREFIX + copied].copy_from_slice(&src[..copied]);
    }

    /// The stored prefix bytes, all `maxInNodePrefixLen` of them.
    pub(crate) fn raw_prefix(self, a: &ArtAllocator) -> [u8; MAX_IN_NODE_PREFIX_LEN as usize] {
        self.bytes(a)[PREFIX..PREFIX + MAX_IN_NODE_PREFIX_LEN as usize]
            .try_into()
            .expect("prefix bytes")
    }

    /// The child at slot `index` of a node4 / node16 / node48, or at key
    /// byte `index` of a node256.
    pub(crate) fn child_at_slot(self, a: &ArtAllocator, index: usize) -> ArtNode {
        read_art_node(self.bytes(a), children_offset(self.kind) + index * ART_NODE_SIZE)
    }

    fn set_child_at_slot(self, a: &mut ArtAllocator, index: usize, child: ArtNode) {
        let offset = children_offset(self.kind) + index * ART_NODE_SIZE;
        write_art_node(self.bytes_mut(a), offset, child);
    }

    /// The key byte at slot `index` of a node4 / node16, or the slot of key
    /// byte `index` in a node48.
    pub(crate) fn key_at(self, a: &ArtAllocator, index: usize) -> u8 {
        let offset = match self.kind {
            NodeKind::Node4 => NODE4_KEYS,
            NodeKind::Node16 => NODE16_KEYS,
            NodeKind::Node48 => NODE48_KEYS,
            _ => panic!("node kind has no keys"),
        };
        self.bytes(a)[offset + index]
    }

    fn set_key_at(self, a: &mut ArtAllocator, index: usize, key: u8) {
        let offset = match self.kind {
            NodeKind::Node4 => NODE4_KEYS,
            NodeKind::Node16 => NODE16_KEYS,
            NodeKind::Node48 => NODE48_KEYS,
            _ => panic!("node kind has no keys"),
        };
        self.bytes_mut(a)[offset + index] = key;
    }

    fn present(self, a: &ArtAllocator, word: usize) -> u64 {
        read_u64(self.bytes(a), PRESENT + word * 8)
    }

    fn set_present(self, a: &mut ArtAllocator, word: usize, value: u64) {
        write_u64(self.bytes_mut(a), PRESENT + word * 8, value);
    }

    /// Go `node48.present[c>>n48s] & (1 << (c % n48m)) != 0`.
    pub(crate) fn is_present(self, a: &ArtAllocator, c: u8) -> bool {
        self.present(a, usize::from(c) >> N48S) & (1u64 << (usize::from(c) % N48M)) != 0
    }

    fn mark_present(self, a: &mut ArtAllocator, c: u8) {
        let word = usize::from(c) >> N48S;
        let value = self.present(a, word) | (1u64 << (usize::from(c) % N48M));
        self.set_present(a, word, value);
    }

    /// Go `node48.nextPresentIdx` / `node256.nextPresentIdx`.
    pub(crate) fn next_present_idx(self, a: &ArtAllocator, start: usize) -> usize {
        let mut start = start;
        let mut present_offset = start >> N48S;
        while present_offset < 4 {
            let offset = start % N48M;
            start = 0;
            let mask = u64::MAX - (1u64 << offset) + 1;
            let current = self.present(a, present_offset) & mask;
            let zeros = current.trailing_zeros() as usize;
            if zeros < N48M {
                return present_offset * N48M + zeros;
            }
            present_offset += 1;
        }
        NODE256_CAP
    }

    /// Go `node48.prevPresentIdx` / `node256.prevPresentIdx`.
    pub(crate) fn prev_present_idx(self, a: &ArtAllocator, start: usize) -> isize {
        let mut start = start;
        let mut present_offset = (start >> N48S) as isize;
        while present_offset >= 0 {
            let offset = start % N48M;
            start = N48M - 1;
            let mask = if offset + 1 == 64 { u64::MAX } else { (1u64 << (offset + 1)) - 1 };
            let current = self.present(a, present_offset as usize) & mask;
            let zeros = current.leading_zeros() as usize;
            if zeros < N48M {
                return present_offset * N48M as isize + N48M as isize - (zeros as isize + 1);
            }
            present_offset -= 1;
        }
        INPLACE_INDEX
    }

    /// Go `nodeBase.match`: the mismatch index of `key[depth:]` against the
    /// node's in-node prefix.
    pub(crate) fn match_prefix(self, a: &ArtAllocator, key: &[u8], depth: u32) -> u32 {
        longest_common_prefix(&key[depth as usize..], self.prefix(a), 0)
    }

    /// Go `nodeBase.matchDeep`: the actual mismatch index, reading a leaf's
    /// key when the prefix is longer than the node stores.
    pub(crate) fn match_deep(self, a: &ArtAllocator, key: &[u8], depth: u32) -> u32 {
        let mismatch_idx = self.match_prefix(a, key, depth);
        if mismatch_idx < MAX_IN_NODE_PREFIX_LEN || self.prefix_len(a) <= MAX_IN_NODE_PREFIX_LEN {
            return mismatch_idx;
        }
        assert!(
            mismatch_idx <= MAX_IN_NODE_PREFIX_LEN,
            "matchDeep invalid state, the mismatchIdx={mismatch_idx} and maxInNodePrefixLen={MAX_IN_NODE_PREFIX_LEN}"
        );
        let leaf = minimum_leaf_node(a, self);
        let leaf_key = a.leaf_key(leaf.addr);
        longest_common_prefix(leaf_key, key, depth + MAX_IN_NODE_PREFIX_LEN) + MAX_IN_NODE_PREFIX_LEN
    }

    /// Go `artNode.findChild`: the index of the child under key byte `c`
    /// (or the in-place leaf), and the child.
    pub(crate) fn find_child(self, a: &ArtAllocator, c: u8, inplace: bool) -> (isize, ArtNode) {
        if inplace {
            return (INPLACE_INDEX, self.inplace_leaf(a));
        }
        let index = self.find_child_index(a, c);
        if index == NOT_EXIST_INDEX {
            return (NOT_EXIST_INDEX, NULL_ART_NODE);
        }
        (index, self.child_at_index(a, index as usize))
    }

    /// The child an index from [`Self::find_child_index`] names.
    pub(crate) fn child_at_index(self, a: &ArtAllocator, index: usize) -> ArtNode {
        match self.kind {
            NodeKind::Node48 => self.child_at_slot(a, usize::from(self.key_at(a, index))),
            _ => self.child_at_slot(a, index),
        }
    }

    /// Go `node4.findChild` / `node16.findChild` / `node48.findChild` /
    /// `node256.findChild`.
    pub(crate) fn find_child_index(self, a: &ArtAllocator, c: u8) -> isize {
        match self.kind {
            NodeKind::Node4 => {
                for index in 0..usize::from(self.node_num(a)) {
                    if self.key_at(a, index) == c {
                        return index as isize;
                    }
                }
                NOT_EXIST_INDEX
            }
            NodeKind::Node16 => {
                let (index, found) = self.search16(a, c);
                if found {
                    index as isize
                } else {
                    NOT_EXIST_INDEX
                }
            }
            NodeKind::Node48 | NodeKind::Node256 => {
                if self.is_present(a, c) {
                    isize::from(c)
                } else {
                    NOT_EXIST_INDEX
                }
            }
            NodeKind::Invalid | NodeKind::Leaf => NOT_EXIST_INDEX,
        }
    }

    /// Go `sort.Find(int(n16.nodeNum), cmp)` over a node16's keys: the first
    /// slot whose key is not below `c`, and whether it equals `c`.
    fn search16(self, a: &ArtAllocator, c: u8) -> (usize, bool) {
        let num = usize::from(self.node_num(a));
        let (mut low, mut high) = (0usize, num);
        while low < high {
            let mid = low + (high - low) / 2;
            if self.key_at(a, mid) < c {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        (low, low < num && self.key_at(a, low) == c)
    }

    /// Go `artNode.replaceChild`.
    pub(crate) fn replace_child(self, a: &mut ArtAllocator, c: u8, child: ArtNode) {
        let index = self.find_child_index(a, c);
        assert!(index != NOT_EXIST_INDEX, "replace child failed");
        match self.kind {
            NodeKind::Node48 => {
                let slot = usize::from(self.key_at(a, index as usize));
                self.set_child_at_slot(a, slot, child);
            }
            _ => self.set_child_at_slot(a, index as usize, child),
        }
    }

    /// Go `artNode.addChild`: whether the node grew to a higher capacity
    /// (`self` then names the grown node).
    pub(crate) fn add_child(&mut self, a: &mut ArtAllocator, c: u8, inplace: bool, child: ArtNode) -> bool {
        if inplace {
            self.set_inplace_leaf(a, child);
            return false;
        }
        match self.kind {
            NodeKind::Node4 => self.add_child4(a, c, child),
            NodeKind::Node16 => self.add_child16(a, c, child),
            NodeKind::Node48 => self.add_child48(a, c, child),
            NodeKind::Node256 => self.add_child256(a, c, child),
            NodeKind::Invalid | NodeKind::Leaf => panic!("add child failed"),
        }
    }

    fn add_child4(&mut self, a: &mut ArtAllocator, c: u8, child: ArtNode) -> bool {
        let num = self.node_num(a);
        if usize::from(num) >= NODE4_CAP {
            self.grow_node4(a);
            self.add_child16(a, c, child);
            return true;
        }
        let mut i = 0u8;
        while i < num {
            if c <= self.key_at(a, usize::from(i)) {
                debug_assert!(c != self.key_at(a, usize::from(i)), "key already exists");
                break;
            }
            i += 1;
        }
        self.shift_slots_right(a, usize::from(i), usize::from(num));
        self.set_key_at(a, usize::from(i), c);
        self.set_child_at_slot(a, usize::from(i), child);
        self.set_node_num(a, num + 1);
        false
    }

    fn add_child16(&mut self, a: &mut ArtAllocator, c: u8, child: ArtNode) -> bool {
        let num = self.node_num(a);
        if usize::from(num) >= NODE16_CAP {
            self.grow_node16(a);
            self.add_child48(a, c, child);
            return true;
        }
        let (i, found) = self.search16(a, c);
        debug_assert!(!found, "key already exists");
        self.shift_slots_right(a, i, usize::from(num));
        self.set_key_at(a, i, c);
        self.set_child_at_slot(a, i, child);
        self.set_node_num(a, num + 1);
        false
    }

    /// Go's `copy(keys[i+1:num+1], keys[i:num])` and the same for children.
    fn shift_slots_right(self, a: &mut ArtAllocator, from: usize, num: usize) {
        let mut index = num;
        while index > from {
            let key = self.key_at(a, index - 1);
            let child = self.child_at_slot(a, index - 1);
            self.set_key_at(a, index, key);
            self.set_child_at_slot(a, index, child);
            index -= 1;
        }
    }

    fn add_child48(&mut self, a: &mut ArtAllocator, c: u8, child: ArtNode) -> bool {
        let num = self.node_num(a);
        if usize::from(num) >= NODE48_CAP {
            self.grow_node48(a);
            self.add_child256(a, c, child);
            return true;
        }
        debug_assert!(!self.is_present(a, c), "key already exists");
        self.set_key_at(a, usize::from(c), num);
        self.mark_present(a, c);
        self.set_child_at_slot(a, usize::from(num), child);
        self.set_node_num(a, num + 1);
        false
    }

    fn add_child256(&mut self, a: &mut ArtAllocator, c: u8, child: ArtNode) -> bool {
        debug_assert!(!self.is_present(a, c), "key already exists");
        self.mark_present(a, c);
        self.set_child_at_slot(a, usize::from(c), child);
        let num = self.node_num(a);
        // Go's `nodeNum++` on a uint8: a full node256 wraps to 0.
        self.set_node_num(a, num.wrapping_add(1));
        false
    }

    /// Go `nodeBase.copyMeta`.
    fn copy_meta(self, a: &mut ArtAllocator, dst: ArtNode) {
        let header: [u8; NODE_BASE_SIZE] = self.bytes(a)[..NODE_BASE_SIZE].try_into().expect("node header");
        dst.bytes_mut(a)[..NODE_BASE_SIZE].copy_from_slice(&header);
    }

    /// Go `artNode.growNode4`.
    fn grow_node4(&mut self, a: &mut ArtAllocator) {
        let n16 = a.alloc_node16();
        self.copy_meta(a, n16);
        let keys: [u8; NODE4_CAP] =
            self.bytes(a)[NODE4_KEYS..NODE4_KEYS + NODE4_CAP].try_into().expect("node4 keys");
        let children: Vec<u8> =
            self.bytes(a)[NODE4_CHILDREN..NODE4_CHILDREN + NODE4_CAP * ART_NODE_SIZE].to_vec();
        let bytes = n16.bytes_mut(a);
        bytes[NODE16_KEYS..NODE16_KEYS + NODE4_CAP].copy_from_slice(&keys);
        bytes[NODE16_CHILDREN..NODE16_CHILDREN + NODE4_CAP * ART_NODE_SIZE].copy_from_slice(&children);
        a.free_node4(self.addr);
        *self = n16;
    }

    /// Go `artNode.growNode16`.
    fn grow_node16(&mut self, a: &mut ArtAllocator) {
        let n48 = a.alloc_node48();
        self.copy_meta(a, n48);
        for i in 0..self.node_num(a) {
            let ch = self.key_at(a, usize::from(i));
            n48.set_key_at(a, usize::from(ch), i);
            n48.mark_present(a, ch);
            let child = self.child_at_slot(a, usize::from(i));
            n48.set_child_at_slot(a, usize::from(i), child);
        }
        a.free_node16(self.addr);
        *self = n48;
    }

    /// Go `artNode.growNode48`.
    fn grow_node48(&mut self, a: &mut ArtAllocator) {
        let n256 = a.alloc_node256();
        self.copy_meta(a, n256);
        let mut i = self.next_present_idx(a, 0);
        while i < NODE256_CAP {
            let child = self.child_at_slot(a, usize::from(self.key_at(a, i)));
            n256.set_child_at_slot(a, i, child);
            i = self.next_present_idx(a, i + 1);
        }
        for word in 0..4 {
            let present = self.present(a, word);
            n256.set_present(a, word, present);
        }
        a.free_node48(self.addr);
        *self = n256;
    }
}

/// Go `artKey.charAt`.
pub(crate) fn char_at(key: &[u8], pos: isize) -> u8 {
    if pos < 0 || pos as usize >= key.len() {
        0
    } else {
        key[pos as usize]
    }
}

/// Go `artKey.valid`.
pub(crate) fn key_valid(key: &[u8], pos: usize) -> bool {
    pos < key.len()
}

/// Go `longestCommonPrefix`: the common prefix length of the two keys from
/// `depth`, whose bytes before it the caller guarantees equal.
pub(crate) fn longest_common_prefix(l1: &[u8], l2: &[u8], depth: u32) -> u32 {
    let limit = l1.len().min(l2.len());
    let mut index = depth as usize;
    while index < limit && l1[index] == l2[index] {
        index += 1;
    }
    (index - depth as usize) as u32
}

/// Go `minimumLeafNode`: the smallest leaf under `node`.
pub(crate) fn minimum_leaf_node(a: &ArtAllocator, node: ArtNode) -> ArtNode {
    let mut node = node;
    loop {
        match node.kind {
            NodeKind::Leaf => return node,
            NodeKind::Node4 | NodeKind::Node16 => {
                let inplace = node.inplace_leaf(a);
                if !inplace.addr.is_null() {
                    return inplace;
                }
                node = node.child_at_slot(a, 0);
            }
            NodeKind::Node48 => {
                let inplace = node.inplace_leaf(a);
                if !inplace.addr.is_null() {
                    return inplace;
                }
                let index = node.next_present_idx(a, 0);
                node = node.child_at_slot(a, usize::from(node.key_at(a, index)));
            }
            NodeKind::Node256 => {
                let inplace = node.inplace_leaf(a);
                if !inplace.addr.is_null() {
                    return inplace;
                }
                let index = node.next_present_idx(a, 0);
                node = node.child_at_slot(a, index);
            }
            NodeKind::Invalid => panic!("invalid node kind"),
        }
    }
}

/// Go `artLeaf`'s accessors over a leaf's arena bytes.
pub(crate) mod leaf {
    use super::*;

    /// Go `artLeaf.vLogAddr`.
    pub(crate) fn vlog_addr(bytes: &[u8]) -> ArenaAddress {
        read_addr(bytes, LEAF_VLOG_ADDR)
    }

    pub(crate) fn set_vlog_addr(bytes: &mut [u8], addr: ArenaAddress) {
        write_addr(bytes, LEAF_VLOG_ADDR, addr);
    }

    /// Go `artLeaf.GetKey`.
    pub(crate) fn key(bytes: &[u8]) -> &[u8] {
        let len = usize::from(read_u16(bytes, LEAF_KEY_LEN));
        &bytes[LEAF_SIZE..LEAF_SIZE + len]
    }

    /// Go `artLeaf.flags`, raw.
    pub(crate) fn raw_flags(bytes: &[u8]) -> u16 {
        read_u16(bytes, LEAF_FLAGS)
    }

    pub(crate) fn set_raw_flags(bytes: &mut [u8], flags: u16) {
        write_u16(bytes, LEAF_FLAGS, flags);
    }

    /// Go `artLeaf.GetKeyFlags`.
    pub(crate) fn key_flags(bytes: &[u8]) -> u16 {
        raw_flags(bytes) & FLAG_MASK
    }

    /// Go `artLeaf.setKeyFlags`.
    pub(crate) fn set_key_flags(bytes: &mut [u8], flags: u16) {
        set_raw_flags(bytes, flags & FLAG_MASK);
    }

    /// Go `artLeaf.markDelete`.
    pub(crate) fn mark_delete(bytes: &mut [u8]) {
        set_raw_flags(bytes, DELETE_FLAG);
    }

    /// Go `artLeaf.isDeleted`.
    pub(crate) fn is_deleted(bytes: &[u8]) -> bool {
        raw_flags(bytes) & DELETE_FLAG != 0
    }

    /// Go `artLeaf.match`.
    pub(crate) fn matches(bytes: &[u8], depth: u32, other: &[u8]) -> bool {
        key(bytes)[depth as usize..] == other[depth as usize..]
    }
}

#[cfg(test)]
#[path = "node_tests.rs"]
mod tests;
