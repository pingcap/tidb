// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Arena and value-log storage used by native transaction-buffer implementations.

use std::sync::Arc;

use crate::kv::KeyFlags;

const NULL_BLOCK_OFFSET: u32 = u32::MAX;
pub const MAX_BLOCK_SIZE: usize = 128 << 20;
pub const INITIAL_BLOCK_SIZE: usize = 4 * 1024;
const VALUE_LOG_HEADER_SIZE: usize = 8 + 8 + 4;

pub const NULL_ADDRESS: ArenaAddress = ArenaAddress::new(u32::MAX, u32::MAX);
pub const NULL_U64_ADDRESS: u64 = u64::MAX;
pub const BAD_ADDRESS: ArenaAddress = ArenaAddress::new(u32::MAX - 1, u32::MAX);
pub const TOMBSTONE: &[u8] = &[];

/// Stable address into an arena block.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ArenaAddress {
    block_index: u32,
    block_offset: u32,
}

impl ArenaAddress {
    pub const fn new(block_index: u32, block_offset: u32) -> Self {
        Self {
            block_index,
            block_offset,
        }
    }

    pub const fn from_u64(value: u64) -> Self {
        Self::new((value >> 32) as u32, value as u32)
    }

    pub const fn as_u64(self) -> u64 {
        ((self.block_index as u64) << 32) | self.block_offset as u64
    }

    pub const fn is_null(self) -> bool {
        self.block_index == u32::MAX || self.block_offset == u32::MAX
    }

    pub const fn to_key_handle(self) -> MemKeyHandle {
        MemKeyHandle {
            user_data: 0,
            block_index: self.block_index as u16,
            block_offset: self.block_offset,
        }
    }

    fn store(self, destination: &mut [u8]) {
        destination[..4].copy_from_slice(&self.block_index.to_le_bytes());
        destination[4..8].copy_from_slice(&self.block_offset.to_le_bytes());
    }

    fn load(source: &[u8]) -> Self {
        Self::new(
            u32::from_le_bytes(source[..4].try_into().unwrap()),
            u32::from_le_bytes(source[4..8].try_into().unwrap()),
        )
    }
}

#[derive(Default)]
struct ArenaBlock {
    buffer: Vec<u8>,
    length: usize,
}

impl ArenaBlock {
    fn with_size(size: usize) -> Self {
        Self {
            buffer: vec![0; size],
            length: 0,
        }
    }

    fn allocate(&mut self, size: usize, align: bool) -> (u32, Option<&mut [u8]>) {
        let offset = if align {
            (self.length + 7) & !7
        } else {
            self.length
        };
        let Some(new_length) = offset.checked_add(size) else {
            return (NULL_BLOCK_OFFSET, None);
        };
        if new_length > self.buffer.len() {
            return (NULL_BLOCK_OFFSET, None);
        }
        self.length = new_length;
        (offset as u32, Some(&mut self.buffer[offset..new_length]))
    }
}

/// Growable block arena with checkpoint and truncation support.
#[derive(Default)]
pub struct MemdbArena {
    block_size: usize,
    blocks: Vec<ArenaBlock>,
    capacity: u64,
    memory_change_hook: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl MemdbArena {
    pub fn allocate(&mut self, size: usize, align: bool) -> (ArenaAddress, &mut [u8]) {
        assert!(
            size <= MAX_BLOCK_SIZE,
            "alloc size is larger than max block size"
        );
        if self.blocks.is_empty() {
            self.enlarge(size, INITIAL_BLOCK_SIZE);
        }

        if self.can_allocate_in_last_block(size, align) {
            return self.allocate_in_last_block(size, align);
        }
        self.enlarge(size, self.block_size << 1);
        self.allocate_in_last_block(size, align)
    }

    fn can_allocate_in_last_block(&self, size: usize, align: bool) -> bool {
        let block = self.blocks.last().unwrap();
        let offset = if align {
            (block.length + 7) & !7
        } else {
            block.length
        };
        offset
            .checked_add(size)
            .is_some_and(|end| end <= block.buffer.len())
    }

    fn allocate_in_last_block(&mut self, size: usize, align: bool) -> (ArenaAddress, &mut [u8]) {
        let index = self.blocks.len() - 1;
        let (offset, data) = self.blocks[index].allocate(size, align);
        let data = data.expect("newly enlarged arena block must fit allocation");
        (ArenaAddress::new(index as u32, offset), data)
    }

    fn enlarge(&mut self, allocation_size: usize, mut block_size: usize) {
        while block_size <= allocation_size {
            block_size <<= 1;
        }
        self.block_size = block_size.min(MAX_BLOCK_SIZE);
        self.blocks.push(ArenaBlock::with_size(self.block_size));
        self.capacity += self.block_size as u64;
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn set_memory_change_hook(&mut self, hook: Arc<dyn Fn() + Send + Sync>) {
        self.memory_change_hook = Some(hook);
    }

    pub fn memory_hook_is_set(&self) -> bool {
        self.memory_change_hook.is_some()
    }

    pub fn on_memory_change(&self) {
        if let Some(hook) = &self.memory_change_hook {
            hook();
        }
    }

    pub fn data(&self, address: ArenaAddress) -> &[u8] {
        &self.blocks[address.block_index as usize].buffer[address.block_offset as usize..]
    }

    pub fn reset(&mut self) {
        self.blocks.clear();
        self.block_size = 0;
        self.capacity = 0;
        self.on_memory_change();
    }

    pub fn checkpoint(&self) -> MemdbCheckpoint {
        MemdbCheckpoint {
            block_size: self.block_size,
            blocks: self.blocks.len(),
            offset_in_block: self.blocks.last().map_or(0, |block| block.length),
        }
    }

    pub fn truncate(&mut self, checkpoint: &MemdbCheckpoint) {
        self.blocks.truncate(checkpoint.blocks);
        if let Some(last) = self.blocks.last_mut() {
            last.length = checkpoint.offset_in_block;
        }
        self.block_size = checkpoint.block_size;
        self.capacity = self.blocks.iter().map(|block| block.length as u64).sum();
    }
}

/// Position in a [`MemdbArena`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemdbCheckpoint {
    block_size: usize,
    blocks: usize,
    offset_in_block: usize,
}

impl MemdbCheckpoint {
    pub const fn is_same_position(&self, other: &Self) -> bool {
        self.blocks == other.blocks && self.offset_in_block == other.offset_in_block
    }

    pub const fn is_before(&self, other: &Self) -> bool {
        self.blocks < other.blocks
            || (self.blocks == other.blocks && self.offset_in_block < other.offset_in_block)
    }
}

/// Access to the key and metadata held by an index node.
pub trait KeyFlagsGetter {
    fn key(&self) -> &[u8];
    fn key_flags(&self) -> KeyFlags;
}

/// Buffer callbacks needed while traversing or reverting a value log.
pub trait VlogMemDb<G: KeyFlagsGetter> {
    fn revert_value_address(&mut self, header: &ValueLogHeader);
    fn inspect_node(&self, address: ArenaAddress) -> (G, ArenaAddress);
}

/// Header stored after each value in the value log.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ValueLogHeader {
    pub node_address: ArenaAddress,
    pub old_value: ArenaAddress,
    pub value_length: u32,
}

impl ValueLogHeader {
    fn store(self, destination: &mut [u8]) {
        destination[..4].copy_from_slice(&self.value_length.to_le_bytes());
        self.old_value.store(&mut destination[4..12]);
        self.node_address.store(&mut destination[12..20]);
    }

    fn load(source: &[u8]) -> Self {
        Self {
            value_length: u32::from_le_bytes(source[..4].try_into().unwrap()),
            old_value: ArenaAddress::load(&source[4..12]),
            node_address: ArenaAddress::load(&source[12..20]),
        }
    }
}

/// Append-only value log layered on [`MemdbArena`].
#[derive(Default)]
pub struct MemdbValueLog {
    arena: MemdbArena,
}

impl MemdbValueLog {
    pub fn arena(&self) -> &MemdbArena {
        &self.arena
    }

    pub fn arena_mut(&mut self) -> &mut MemdbArena {
        &mut self.arena
    }

    pub fn checkpoint(&self) -> MemdbCheckpoint {
        self.arena.checkpoint()
    }

    pub fn append_value(
        &mut self,
        node_address: ArenaAddress,
        old_value: ArenaAddress,
        value: &[u8],
    ) -> ArenaAddress {
        let previous_blocks = self.arena.block_count();
        let size = VALUE_LOG_HEADER_SIZE + value.len();
        let (mut address, memory) = self.arena.allocate(size, false);
        memory[..value.len()].copy_from_slice(value);
        ValueLogHeader {
            node_address,
            old_value,
            value_length: value.len() as u32,
        }
        .store(&mut memory[value.len()..]);
        address.block_offset += size as u32;
        if previous_blocks != self.arena.block_count() {
            self.arena.on_memory_change();
        }
        address
    }

    pub fn value(&self, address: ArenaAddress) -> &[u8] {
        let header_offset = address.block_offset as usize - VALUE_LOG_HEADER_SIZE;
        let block = &self.arena.blocks[address.block_index as usize].buffer;
        let value_length =
            u32::from_le_bytes(block[header_offset..header_offset + 4].try_into().unwrap())
                as usize;
        if value_length == 0 {
            return TOMBSTONE;
        }
        &block[header_offset - value_length..header_offset]
    }

    pub fn snapshot_value(
        &self,
        address: ArenaAddress,
        checkpoint: &MemdbCheckpoint,
    ) -> Option<&[u8]> {
        let selected = self.select_value_history(address, |candidate| {
            !self.can_modify(Some(checkpoint), candidate)
        });
        (!selected.is_null()).then(|| self.value(selected))
    }

    pub fn select_value_history(
        &self,
        mut address: ArenaAddress,
        mut predicate: impl FnMut(ArenaAddress) -> bool,
    ) -> ArenaAddress {
        while !address.is_null() {
            if predicate(address) {
                return address;
            }
            address = self.header_at(address).old_value;
        }
        NULL_ADDRESS
    }

    pub fn revert_to_checkpoint<G, M>(&self, database: &mut M, checkpoint: &MemdbCheckpoint)
    where
        G: KeyFlagsGetter,
        M: VlogMemDb<G>,
    {
        let mut cursor = self.checkpoint();
        while !checkpoint.is_same_position(&cursor) {
            let header = self.header_at_checkpoint(&cursor);
            database.revert_value_address(&header);
            self.move_cursor_back(&mut cursor, &header);
        }
    }

    pub fn inspect_key_values<G, M>(
        &self,
        database: &M,
        head: &MemdbCheckpoint,
        tail: &MemdbCheckpoint,
        mut function: impl FnMut(&[u8], KeyFlags, &[u8]),
    ) where
        G: KeyFlagsGetter,
        M: VlogMemDb<G>,
    {
        let mut cursor = *tail;
        while !head.is_same_position(&cursor) {
            let cursor_address =
                ArenaAddress::new((cursor.blocks - 1) as u32, cursor.offset_in_block as u32);
            let header = self.header_at(cursor_address);
            let (node, current_value) = database.inspect_node(header.node_address);
            if current_value == cursor_address {
                function(node.key(), node.key_flags(), self.value(cursor_address));
            }
            self.move_cursor_back(&mut cursor, &header);
        }
    }

    pub fn can_modify(&self, checkpoint: Option<&MemdbCheckpoint>, address: ArenaAddress) -> bool {
        let Some(checkpoint) = checkpoint else {
            return true;
        };
        if checkpoint.blocks == 0 {
            return true;
        }
        let index = address.block_index as usize;
        index > checkpoint.blocks - 1
            || (index == checkpoint.blocks - 1
                && address.block_offset as usize > checkpoint.offset_in_block)
    }

    fn header_at(&self, address: ArenaAddress) -> ValueLogHeader {
        let offset = address.block_offset as usize - VALUE_LOG_HEADER_SIZE;
        let block = &self.arena.blocks[address.block_index as usize].buffer;
        ValueLogHeader::load(&block[offset..offset + VALUE_LOG_HEADER_SIZE])
    }

    fn header_at_checkpoint(&self, checkpoint: &MemdbCheckpoint) -> ValueLogHeader {
        let offset = checkpoint.offset_in_block - VALUE_LOG_HEADER_SIZE;
        ValueLogHeader::load(
            &self.arena.blocks[checkpoint.blocks - 1].buffer
                [offset..offset + VALUE_LOG_HEADER_SIZE],
        )
    }

    fn move_cursor_back(&self, cursor: &mut MemdbCheckpoint, header: &ValueLogHeader) {
        cursor.offset_in_block -= VALUE_LOG_HEADER_SIZE + header.value_length as usize;
        if cursor.offset_in_block == 0 {
            cursor.blocks -= 1;
            if cursor.blocks > 0 {
                cursor.offset_in_block = self.arena.blocks[cursor.blocks - 1].length;
            }
        }
    }
}

/// Compact pointer to a key in a transaction buffer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemKeyHandle {
    pub user_data: u16,
    block_index: u16,
    block_offset: u32,
}

impl MemKeyHandle {
    pub const fn to_address(self) -> ArenaAddress {
        ArenaAddress::new(self.block_index as u32, self.block_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone)]
    struct Node {
        key: Vec<u8>,
        flags: KeyFlags,
    }

    impl KeyFlagsGetter for Node {
        fn key(&self) -> &[u8] {
            &self.key
        }

        fn key_flags(&self) -> KeyFlags {
            self.flags
        }
    }

    #[derive(Default)]
    struct DummyDatabase {
        reverted: Vec<ValueLogHeader>,
        nodes: Vec<(Node, ArenaAddress)>,
    }

    impl VlogMemDb<Node> for DummyDatabase {
        fn revert_value_address(&mut self, header: &ValueLogHeader) {
            self.reverted.push(*header);
        }

        fn inspect_node(&self, address: ArenaAddress) -> (Node, ArenaAddress) {
            self.nodes[address.block_offset as usize].clone()
        }
    }

    #[test]
    fn original_large_value_growth_cases() {
        let mut value_log = MemdbValueLog::default();
        value_log.append_value(ArenaAddress::new(0, 0), NULL_ADDRESS, &vec![0; 80 << 20]);
        assert_eq!(value_log.arena().block_size(), MAX_BLOCK_SIZE);
        assert_eq!(value_log.arena().block_count(), 1);

        let checkpoint = value_log.checkpoint();
        value_log.append_value(ArenaAddress::new(0, 1), NULL_ADDRESS, &vec![0; 127 << 20]);
        value_log.revert_to_checkpoint::<Node, _>(&mut DummyDatabase::default(), &checkpoint);
        assert_eq!(value_log.arena().block_size(), MAX_BLOCK_SIZE);
        assert_eq!(value_log.arena().block_count(), 2);

        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            value_log.append_value(
                ArenaAddress::new(0, 2),
                NULL_ADDRESS,
                &vec![0; MAX_BLOCK_SIZE + 1],
            );
        }));
        assert_eq!(
            panic.unwrap_err().downcast_ref::<&str>().copied(),
            Some("alloc size is larger than max block size")
        );
    }

    #[test]
    fn original_value_larger_than_current_block_case() {
        let mut value_log = MemdbValueLog::default();
        value_log.append_value(ArenaAddress::new(0, 0), NULL_ADDRESS, &[0]);
        value_log.append_value(ArenaAddress::new(0, 1), NULL_ADDRESS, &vec![0; 4096]);
        assert_eq!(value_log.arena().block_count(), 2);
        let address = value_log.append_value(ArenaAddress::new(0, 2), NULL_ADDRESS, &vec![7; 3000]);
        assert_eq!(value_log.arena().block_count(), 2);
        assert_eq!(value_log.value(address), vec![7; 3000]);
    }

    #[test]
    fn addresses_checkpoints_history_inspection_and_hooks() {
        let address = ArenaAddress::new(0x1234, 0x5678);
        assert_eq!(ArenaAddress::from_u64(address.as_u64()), address);
        assert_eq!(address.to_key_handle().to_address(), address);
        assert!(NULL_ADDRESS.is_null());
        assert!(BAD_ADDRESS.is_null());

        let calls = Arc::new(AtomicUsize::new(0));
        let mut value_log = MemdbValueLog::default();
        let hook_calls = calls.clone();
        value_log
            .arena_mut()
            .set_memory_change_hook(Arc::new(move || {
                hook_calls.fetch_add(1, Ordering::SeqCst);
            }));
        let head = value_log.checkpoint();
        let old = value_log.append_value(ArenaAddress::new(0, 0), NULL_ADDRESS, b"old");
        let tombstone = value_log.append_value(ArenaAddress::new(0, 0), old, TOMBSTONE);
        assert_eq!(value_log.value(tombstone), TOMBSTONE);
        let snapshot = value_log.checkpoint();
        let current = value_log.append_value(ArenaAddress::new(0, 0), tombstone, b"new");
        let tail = value_log.checkpoint();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            value_log.snapshot_value(current, &snapshot),
            Some(TOMBSTONE)
        );
        assert_eq!(value_log.snapshot_value(old, &head), None);
        assert!(value_log.can_modify(None, old));
        assert!(value_log.can_modify(Some(&snapshot), current));
        assert!(!value_log.can_modify(Some(&snapshot), old));
        assert!(head.is_before(&tail));

        let mut database = DummyDatabase {
            reverted: Vec::new(),
            nodes: vec![(
                Node {
                    key: b"key".to_vec(),
                    flags: KeyFlags::from_bits(1),
                },
                current,
            )],
        };
        let mut inspected = Vec::new();
        value_log.inspect_key_values::<Node, _>(&database, &head, &tail, |key, flags, value| {
            inspected.push((key.to_vec(), flags.bits(), value.to_vec()));
        });
        assert_eq!(inspected, vec![(b"key".to_vec(), 1, b"new".to_vec())]);
        value_log.revert_to_checkpoint::<Node, _>(&mut database, &snapshot);
        assert_eq!(database.reverted.len(), 1);

        value_log.arena_mut().truncate(&snapshot);
        assert!(value_log.arena().capacity() > 0);
        value_log.arena_mut().reset();
        assert_eq!(value_log.arena().block_count(), 0);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn allocator_alignment_capacity_and_truncation_match_source() {
        let mut arena = MemdbArena::default();
        let (first, first_data) = arena.allocate(3, false);
        first_data.copy_from_slice(&[1, 2, 3]);
        let checkpoint = arena.checkpoint();
        let (aligned, _) = arena.allocate(1, true);
        assert_eq!(first, ArenaAddress::new(0, 0));
        assert_eq!(aligned, ArenaAddress::new(0, 8));
        assert_eq!(arena.capacity(), INITIAL_BLOCK_SIZE as u64);
        assert_eq!(arena.data(first)[..3], [1, 2, 3]);

        arena.truncate(&checkpoint);
        assert_eq!(arena.capacity(), 3);
        let (reused, _) = arena.allocate(1, false);
        assert_eq!(reused, ArenaAddress::new(0, 3));
    }
}
