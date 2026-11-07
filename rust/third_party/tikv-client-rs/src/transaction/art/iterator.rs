// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! client-go `internal/unionstore/art/art_iterator.go`.

use std::cmp::Ordering as CmpOrdering;

use super::arena::ArtAllocator;
use super::node::{
    char_at, key_valid, leaf, minimum_leaf_node, ArtNode, NodeKind, INPLACE_INDEX,
    MAX_IN_NODE_PREFIX_LEN, NODE256_CAP, NOT_EXIST_INDEX, NULL_ART_NODE,
};
use super::tree::ArtTree;
use super::{ArtHandle, SharedTree};
use crate::kv::KeyFlags;
use crate::transaction::arena::{ArenaAddress, BAD_ADDRESS, NULL_ADDRESS};

/// Go `Iterator`.
///
/// Go reads the current leaf through a pointer; this iterator keeps a copy
/// of the leaf's key and flags as it moves, and reads the value through the
/// tree when asked.
pub(crate) struct ArtIterator {
    tree: SharedTree,
    reverse: bool,
    valid: bool,
    include_flags: bool,
    inner: BaseIter,
    curr_addr: ArenaAddress,
    end_addr: ArenaAddress,
    curr_key: Vec<u8>,
    curr_flags: KeyFlags,
    curr_vlog_addr: ArenaAddress,
    curr_deleted: bool,
    value: Vec<u8>,

    /// only when seqNo == art.WriteSeqNo, the iterator is valid.
    seq_no: u64,
    /// ignoreSeqNo is used to ignore the seqNo check, used for snapshot
    /// iter before its full deprecation.
    pub(crate) ignore_seq_no: bool,
}

impl ArtIterator {
    /// Go `ART.iter`.
    pub(crate) fn new(
        tree: SharedTree,
        lower_bound: Option<&[u8]>,
        upper_bound: Option<&[u8]>,
        reverse: bool,
        include_flags: bool,
    ) -> Result<Self, &'static str> {
        let seq_no = super::read(&tree).write_seq_no;
        let mut it = Self {
            tree,
            reverse,
            valid: true,
            include_flags,
            inner: BaseIter::default(),
            // the default value of currAddr is not equal to any valid
            // address, so it never equals the initial endAddr.
            curr_addr: BAD_ADDRESS,
            end_addr: NULL_ADDRESS,
            curr_key: Vec::new(),
            curr_flags: KeyFlags::default(),
            curr_vlog_addr: NULL_ADDRESS,
            curr_deleted: false,
            value: Vec::new(),
            seq_no,
            ignore_seq_no: false,
        };
        {
            let tree = super::read(&it.tree);
            let (valid, inner, end_addr) =
                init(&tree, lower_bound.unwrap_or_default(), upper_bound.unwrap_or_default(), reverse);
            it.valid = valid;
            it.inner = inner;
            it.end_addr = end_addr;
        }
        if !it.valid {
            return Ok(it);
        }
        it.next()?;
        Ok(it)
    }

    pub(crate) fn tree(&self) -> &SharedTree {
        &self.tree
    }

    fn check_seq_no(&self, tree: &ArtTree) {
        if self.seq_no != tree.write_seq_no && !self.ignore_seq_no {
            panic!(
                "seqNo mismatch: it seqNo {}, art seqNo {}",
                self.seq_no, tree.write_seq_no
            );
        }
    }

    /// Go `Iterator.Valid`.
    pub(crate) fn valid(&self) -> bool {
        self.check_seq_no(&super::read(&self.tree));
        self.valid
    }

    /// Go `Iterator.Key`.
    pub(crate) fn key(&self) -> &[u8] {
        self.check_seq_no(&super::read(&self.tree));
        &self.curr_key
    }

    /// Go `Iterator.Flags`.
    pub(crate) fn flags(&self) -> KeyFlags {
        self.check_seq_no(&super::read(&self.tree));
        self.curr_flags
    }

    /// Go `Iterator.Value`: `None` for a flags-only key.
    pub(crate) fn value(&mut self) -> Option<&[u8]> {
        {
            let tree = super::read(&self.tree);
            self.check_seq_no(&tree);
            if self.curr_vlog_addr.is_null() {
                return None;
            }
            self.value.clear();
            self.value
                .extend_from_slice(tree.allocator.vlog_allocator.value(self.curr_vlog_addr));
        }
        Some(&self.value)
    }

    /// Go `Iterator.HasValue`: false if it is flags only.
    pub(crate) fn has_value(&self) -> bool {
        !self.is_flags_only()
    }

    fn is_flags_only(&self) -> bool {
        self.curr_addr != BAD_ADDRESS && self.curr_vlog_addr.is_null()
    }

    /// The current leaf's value address (Go `currLeaf.vLogAddr`).
    pub(crate) fn curr_vlog_addr(&self) -> ArenaAddress {
        self.curr_vlog_addr
    }

    /// Go `Iterator.Next`.
    pub(crate) fn next(&mut self) -> Result<(), &'static str> {
        if !self.valid {
            // iterate is finished
            return Err("Art: iterator is finished");
        }
        let tree = std::sync::Arc::clone(&self.tree);
        let tree = super::read(&tree);
        self.check_seq_no(&tree);
        if self.curr_addr == self.end_addr {
            self.valid = false;
            return Ok(());
        }

        loop {
            let next_leaf = if self.reverse {
                self.inner.prev(&tree.allocator)
            } else {
                self.inner.next(&tree.allocator)
            };
            if next_leaf.addr.is_null() {
                self.valid = false;
                return Ok(());
            }
            self.set_curr_leaf(&tree.allocator, next_leaf.addr);
            if self.curr_vlog_addr.is_null() {
                // if includeFlags is true, the iterator should return even
                // the value is null.
                if self.include_flags && !self.curr_deleted {
                    return Ok(());
                }
                if next_leaf.addr == self.end_addr {
                    self.valid = false;
                    return Ok(());
                }
                continue;
            }
            return Ok(());
        }
    }

    /// Go `Iterator.setCurrLeaf`.
    fn set_curr_leaf(&mut self, a: &ArtAllocator, node: ArenaAddress) {
        self.curr_addr = node;
        let data = a.node_data(node);
        self.curr_key.clear();
        self.curr_key.extend_from_slice(leaf::key(data));
        self.curr_flags = KeyFlags::from_bits(leaf::key_flags(data));
        self.curr_vlog_addr = leaf::vlog_addr(data);
        self.curr_deleted = leaf::is_deleted(data);
    }

    /// Go `Iterator.Handle`.
    pub(crate) fn handle(&self) -> ArtHandle {
        ArtHandle(self.curr_addr)
    }

    /// Go `Iterator.Close`.
    pub(crate) fn close(self) {}
}

/// Go `Iterator.init`: whether the iterator is valid, its positioned inner
/// iterator, and its end address.
fn init(
    tree: &ArtTree,
    lower_bound: &[u8],
    upper_bound: &[u8],
    reverse: bool,
) -> (bool, BaseIter, ArenaAddress) {
    let mut inner = BaseIter::default();
    if tree.root.addr.is_null() {
        return (false, inner, NULL_ADDRESS);
    }
    if !lower_bound.is_empty() && !upper_bound.is_empty() && lower_bound >= upper_bound {
        return (false, inner, NULL_ADDRESS);
    }

    let (start_key, end_key) = if reverse {
        (upper_bound, lower_bound)
    } else {
        (lower_bound, upper_bound)
    };

    let a = &tree.allocator;
    if start_key.is_empty() {
        inner.seek_to_first(tree.root, reverse);
    } else {
        inner.seek(a, tree.root, start_key);
    }
    if end_key.is_empty() {
        return (true, inner, NULL_ADDRESS);
    }

    let mut helper = BaseIter::default();
    helper.seek(a, tree.root, end_key);
    if inner.compare(&helper) == CmpOrdering::Equal {
        // no keys exist between start key and end key.
        return (false, inner, NULL_ADDRESS);
    }

    let end_addr = if reverse {
        helper.next(a).addr
    } else {
        helper.prev(a).addr
    };
    let cmp = inner.compare(&helper);
    if cmp == CmpOrdering::Equal {
        // the current key is valid.
        return (true, inner, end_addr);
    }
    // in asc scan, cmp > 0 means the current key is larger than the end key;
    // in desc scan, cmp < 0 means it is less than the end key.
    if (cmp == CmpOrdering::Less) == reverse || helper.idxes.is_empty() {
        return (false, inner, end_addr);
    }
    (true, inner, end_addr)
}

/// Go `baseIter`: the DFS stack of the nodes being visited and the index
/// in each.
#[derive(Default)]
pub(crate) struct BaseIter {
    idxes: Vec<isize>,
    nodes: Vec<ArtNode>,
}

impl BaseIter {
    /// Go `baseIter.seekToFirst`.
    fn seek_to_first(&mut self, root: ArtNode, reverse: bool) {
        self.nodes = vec![root];
        self.idxes = vec![if reverse {
            NODE256_CAP as isize
        } else {
            INPLACE_INDEX
        }];
    }

    /// Go `baseIter.seek`: the first node and index that is >= key.
    fn seek(&mut self, a: &ArtAllocator, root: ArtNode, key: &[u8]) {
        assert!(!key.is_empty(), "seek with empty key is not allowed");
        let mut curr = root;
        let mut depth = 0u32;
        loop {
            if curr.is_leaf() {
                if key_valid(key, depth as usize) {
                    let leaf_key = a.leaf_key(curr.addr);
                    if key > leaf_key {
                        // the seek key does not exist, and it's longer and
                        // larger than the current leaf's key.
                        *self.idxes.last_mut().expect("seek stack") += 1;
                    }
                }
                return;
            }

            self.nodes.push(curr);
            let prefix_len = curr.prefix_len(a);
            if prefix_len > 0 {
                let mismatch_idx = curr.match_deep(a, key, depth);
                if mismatch_idx < prefix_len {
                    // No leaf matches the seek key: all the children are on
                    // its right when it is smaller than the prefix, else on
                    // its left.
                    let prefix: Vec<u8> = if mismatch_idx < MAX_IN_NODE_PREFIX_LEN {
                        curr.raw_prefix(a).to_vec()
                    } else {
                        let leaf_node = minimum_leaf_node(a, curr);
                        a.leaf_key(leaf_node.addr)[depth as usize..].to_vec()
                    };
                    if (mismatch_idx + depth) as usize == key.len()
                        || key[(depth + mismatch_idx) as usize] < prefix[mismatch_idx as usize]
                    {
                        self.idxes.push(-1);
                    } else {
                        self.idxes.push(NODE256_CAP as isize);
                    }
                    return;
                }
                depth += mismatch_idx.min(prefix_len);
            }

            let c = char_at(key, depth as isize);
            let (idx, next) = curr.find_child(a, c, !key_valid(key, depth as usize));
            if next.addr.is_null() {
                self.idxes.push(seek_to_idx(a, curr, c));
                return;
            }
            self.idxes.push(idx);
            curr = next;
            depth += 1;
        }
    }

    /// Go `baseIter.compare`: the order of the two visiting paths.
    fn compare(&self, other: &BaseIter) -> CmpOrdering {
        let (l1, l2) = (self.idxes.len(), other.idxes.len());
        for i in 0..l1.min(l2) {
            match self.idxes[i].cmp(&other.idxes[i]) {
                CmpOrdering::Equal => continue,
                unequal => return unequal,
            }
        }
        l1.cmp(&l2)
    }

    /// Go `baseIter.next`: the next leaf, or the null node when drained.
    fn next(&mut self, a: &ArtAllocator) -> ArtNode {
        loop {
            let depth = self.nodes.len() - 1;
            let curr = self.nodes[depth];
            let mut idx = self.idxes[depth];
            let mut child = None;
            match curr.kind {
                NodeKind::Node4 | NodeKind::Node16 => {
                    let cap = if curr.kind == NodeKind::Node4 { 4 } else { 16 };
                    let mut drained = false;
                    if idx == INPLACE_INDEX {
                        idx = 0; // mark in-place leaf is visited
                        self.idxes[depth] = idx;
                        let inplace = curr.inplace_leaf(a);
                        if !inplace.addr.is_null() {
                            return inplace;
                        }
                    } else if idx == cap {
                        drained = true;
                    }
                    if !drained {
                        let node_num = isize::from(curr.node_num(a));
                        if idx >= 0 && idx < node_num {
                            self.idxes[depth] = idx;
                            child = Some(curr.child_at_slot(a, idx as usize));
                        } else if idx < node_num {
                            panic_for_invalid_index(idx);
                        }
                        // idx >= nodeNum means this node is drained.
                    }
                }
                NodeKind::Node48 | NodeKind::Node256 => {
                    let mut drained = false;
                    if idx == INPLACE_INDEX {
                        idx = 0; // mark in-place leaf is visited
                        self.idxes[depth] = idx;
                        let inplace = curr.inplace_leaf(a);
                        if !inplace.addr.is_null() {
                            return inplace;
                        }
                    } else if idx == NODE256_CAP as isize {
                        drained = true;
                    }
                    if !drained {
                        idx = curr.next_present_idx(a, idx as usize) as isize;
                        if idx >= 0 && idx < NODE256_CAP as isize {
                            self.idxes[depth] = idx;
                            child = Some(curr.child_at_index(a, idx as usize));
                        } else if idx != NODE256_CAP as isize {
                            panic_for_invalid_index(idx);
                        }
                        // idx == node256cap means this node is drained.
                    }
                }
                _ => panic!("invalid node type"),
            }
            if let Some(child) = child {
                if child.kind == NodeKind::Leaf {
                    self.idxes[depth] += 1;
                    return child;
                }
                self.nodes.push(child);
                self.idxes.push(INPLACE_INDEX);
                continue;
            }
            self.nodes.truncate(depth);
            self.idxes.truncate(depth);
            if depth == 0 {
                return NULL_ART_NODE;
            }
            self.idxes[depth - 1] += 1;
        }
    }

    /// Go `baseIter.prev`: the previous leaf, or the null node when drained.
    fn prev(&mut self, a: &ArtAllocator) -> ArtNode {
        loop {
            let depth = self.nodes.len() - 1;
            let curr = self.nodes[depth];
            let mut idx = self.idxes[depth] - 1;
            if idx != NOT_EXIST_INDEX {
                let mut child = None;
                match curr.kind {
                    NodeKind::Node4 | NodeKind::Node16 => {
                        idx = idx.min(isize::from(curr.node_num(a)) - 1);
                        if idx >= 0 {
                            self.idxes[depth] = idx;
                            child = Some(curr.child_at_slot(a, idx as usize));
                        } else if idx == INPLACE_INDEX {
                            self.idxes[depth] = idx;
                            let inplace = curr.inplace_leaf(a);
                            if !inplace.addr.is_null() {
                                return inplace;
                            }
                        } else {
                            panic_for_invalid_index(idx);
                        }
                    }
                    NodeKind::Node48 | NodeKind::Node256 => {
                        if idx >= 0 && !curr.is_present(a, idx as u8) {
                            // goto the previous present key; for idx < 0
                            // the in-place leaf is checked below.
                            idx = curr.prev_present_idx(a, idx as usize);
                        }
                        if idx >= 0 {
                            self.idxes[depth] = idx;
                            child = Some(curr.child_at_index(a, idx as usize));
                        } else if idx == INPLACE_INDEX {
                            self.idxes[depth] = idx;
                            let inplace = curr.inplace_leaf(a);
                            if !inplace.addr.is_null() {
                                return inplace;
                            }
                        } else {
                            panic_for_invalid_index(idx);
                        }
                    }
                    _ => panic!("invalid node type"),
                }
                if let Some(child) = child {
                    if child.kind == NodeKind::Leaf {
                        return child;
                    }
                    self.nodes.push(child);
                    self.idxes.push(NODE256_CAP as isize);
                    continue;
                }
            }
            self.nodes.truncate(depth);
            self.idxes.truncate(depth);
            if depth == 0 {
                return NULL_ART_NODE;
            }
        }
    }
}

/// Go `seekToIdx`: the index before which every child is below `char`.
fn seek_to_idx(a: &ArtAllocator, curr: ArtNode, c: u8) -> isize {
    match curr.kind {
        NodeKind::Node4 => {
            let num = usize::from(curr.node_num(a));
            let mut next_idx = 0;
            while next_idx < num && curr.key_at(a, next_idx) < c {
                next_idx += 1;
            }
            next_idx as isize
        }
        NodeKind::Node16 => {
            // Go sort.Find with a comparator never returning 0: the first
            // slot whose key is not below `c`.
            let num = usize::from(curr.node_num(a));
            let (mut low, mut high) = (0usize, num);
            while low < high {
                let mid = low + (high - low) / 2;
                if curr.key_at(a, mid) < c {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            low as isize
        }
        NodeKind::Node48 | NodeKind::Node256 => curr.next_present_idx(a, usize::from(c)) as isize,
        _ => panic!("invalid node type"),
    }
}

fn panic_for_invalid_index(idx: isize) -> ! {
    panic!("ART iterator meets an invalid index {idx}")
}

#[cfg(test)]
mod tests {
    //! client-go `art_iterator_test.go`'s tests of the inner iterator.

    use super::*;
    use crate::transaction::art::{read, Art};

    #[test]
    fn test_seek_in_exist_node() {
        for kind in [
            NodeKind::Node4,
            NodeKind::Node16,
            NodeKind::Node48,
            NodeKind::Node256,
        ] {
            let mut tree = Art::new();
            let cnt = match kind {
                NodeKind::Node4 => 4,
                NodeKind::Node16 => 16,
                NodeKind::Node48 => 48,
                _ => 49,
            };
            for i in 0..cnt {
                let key = [(2 * i) as u8];
                tree.set(&key, Some(&key), &[]).unwrap();
            }
            let tree = read(&tree.tree);
            assert_eq!(tree.root.kind, kind);
            for i in 0..cnt - 1 {
                let mut helper = BaseIter::default();
                helper.seek(&tree.allocator, tree.root, &[(2 * i + 1) as u8]);
                let expect = match kind {
                    // node4 and node16 are not indexed by char
                    NodeKind::Node4 | NodeKind::Node16 => i + 1,
                    // node48 and node256 are indexed by char
                    _ => 2 * (i + 1),
                };
                assert_eq!(helper.idxes, vec![expect]);
            }
        }
    }

    #[test]
    fn test_seek_to_idx() {
        let mut a = ArtAllocator::default();
        a.init();
        for kind in [
            NodeKind::Node4,
            NodeKind::Node16,
            NodeKind::Node48,
            NodeKind::Node256,
        ] {
            let mut node = match kind {
                NodeKind::Node4 => a.alloc_node4(),
                NodeKind::Node16 => a.alloc_node16(),
                NodeKind::Node48 => a.alloc_node48(),
                _ => a.alloc_node256(),
            };
            let lf = a.alloc_leaf(&[10]);
            node.add_child(&mut a, 10, false, lf);
            let (exist_idx, max_idx) = match kind {
                NodeKind::Node4 | NodeKind::Node16 => (0, 1),
                _ => (10, NODE256_CAP as isize),
            };
            assert_eq!(seek_to_idx(&a, node, 1), exist_idx);
            assert_eq!(seek_to_idx(&a, node, 11), max_idx);
        }
    }

    #[test]
    fn test_iter_position_compare() {
        let compare = |idx1: &[isize], idx2: &[isize]| {
            let helper1 = BaseIter {
                idxes: idx1.to_vec(),
                nodes: Vec::new(),
            };
            let helper2 = BaseIter {
                idxes: idx2.to_vec(),
                nodes: Vec::new(),
            };
            helper1.compare(&helper2)
        };
        assert_eq!(compare(&[1, 2, 3], &[1, 2, 3]), CmpOrdering::Equal);
        assert_eq!(compare(&[1, 2, 2], &[1, 2, 3]), CmpOrdering::Less);
        assert_eq!(compare(&[1, 2, 4], &[1, 2, 3]), CmpOrdering::Greater);
        assert_eq!(compare(&[1, 2, 3], &[1, 2]), CmpOrdering::Greater);
        assert_eq!(compare(&[1, 2], &[1, 2, 3]), CmpOrdering::Less);
    }
}
