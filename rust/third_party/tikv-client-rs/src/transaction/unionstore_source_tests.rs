use super::*;

use crate::error::KeyTooLargeError;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{atomic::AtomicU64, mpsc};

type BufferError = Box<dyn std::error::Error + Send + Sync>;

trait SourceSnapshot {
    fn get_value(&self, key: &[u8]) -> Result<Vec<u8>, ()>;
    fn iter_values(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
    ) -> Box<dyn KvIterator>;
}

impl SourceSnapshot for MemDbSnapshot {
    fn get_value(&self, key: &[u8]) -> Result<Vec<u8>, ()> {
        self.get(key).map_err(|_| ())
    }

    fn iter_values(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
    ) -> Box<dyn KvIterator> {
        self.iter(lower, upper, reverse)
    }
}

impl SourceSnapshot for RbtMemDbSnapshot {
    fn get_value(&self, key: &[u8]) -> Result<Vec<u8>, ()> {
        self.get(key).map_err(|_| ())
    }

    fn iter_values(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        reverse: bool,
    ) -> Box<dyn KvIterator> {
        self.iter(lower, upper, reverse)
    }
}

trait SourceBuffer {
    type Snapshot: SourceSnapshot;

    fn set_value(&mut self, key: &[u8], value: &[u8]) -> Result<(), BufferError>;
    fn set_value_with_flags(
        &mut self,
        key: &[u8],
        value: &[u8],
        flags: &[FlagsOp],
    ) -> Result<(), BufferError>;
    fn delete_value(&mut self, key: &[u8]) -> Result<(), BufferError>;
    fn update_key_flags(&mut self, key: &[u8], flags: &[FlagsOp]);
    fn get_value(&mut self, key: &[u8]) -> Result<Vec<u8>, StaticError>;
    fn get_key_flags(&mut self, key: &[u8]) -> Result<KeyFlags, StaticError>;
    fn iter_values(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Box<dyn KvIterator>;
    fn iter_values_reverse(
        &self,
        upper: Option<&[u8]>,
        lower: Option<&[u8]>,
    ) -> Box<dyn KvIterator>;
    fn iter_values_with_flags(&self, reverse: bool) -> Box<dyn KvIterator>;
    fn staging_handle(&mut self) -> usize;
    fn cleanup_handle(&mut self, handle: usize);
    fn release_handle(&mut self, handle: usize);
    fn checkpoint_value(&self) -> usize;
    fn revert_checkpoint(&mut self, checkpoint: usize);
    fn inspect_stage_values(
        &self,
        handle: usize,
        function: &mut dyn FnMut(&[u8], KeyFlags, &[u8]),
    );
    fn snapshot_value(&self) -> Self::Snapshot;
    fn len_value(&self) -> usize;
    fn size_value(&self) -> usize;
    fn dirty_value(&self) -> bool;
    fn reset_value(&mut self);
    fn remove_value(&mut self, key: &[u8]);
    fn set_limits(&mut self, entry: u64, buffer: u64);
    fn memory_value(&self) -> u64;
    fn memory_hook_set(&self) -> bool;
    fn set_memory_hook(&mut self, hook: Arc<dyn Fn(u64) + Send + Sync>);
    fn cache_hits(&self) -> u64;
    fn cache_misses(&self) -> u64;
    fn select_history(
        &mut self,
        key: &[u8],
        predicate: &mut dyn FnMut(&[u8]) -> bool,
    ) -> Result<Option<Vec<u8>>, StaticError>;
}

impl SourceBuffer for MemDb {
    type Snapshot = MemDbSnapshot;

    fn set_value(&mut self, key: &[u8], value: &[u8]) -> Result<(), BufferError> {
        self.set(key, value)
    }
    fn set_value_with_flags(
        &mut self,
        key: &[u8],
        value: &[u8],
        flags: &[FlagsOp],
    ) -> Result<(), BufferError> {
        self.set_with_flags(key, value, flags)
    }
    fn delete_value(&mut self, key: &[u8]) -> Result<(), BufferError> {
        self.delete(key)
    }
    fn update_key_flags(&mut self, key: &[u8], flags: &[FlagsOp]) {
        self.update_flags(key, flags);
    }
    fn get_value(&mut self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        self.get(key)
    }
    fn get_key_flags(&mut self, key: &[u8]) -> Result<KeyFlags, StaticError> {
        self.get_flags(key)
    }
    fn iter_values(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Box<dyn KvIterator> {
        self.iter(lower, upper)
    }
    fn iter_values_reverse(
        &self,
        upper: Option<&[u8]>,
        lower: Option<&[u8]>,
    ) -> Box<dyn KvIterator> {
        self.iter_reverse(upper, lower)
    }
    fn iter_values_with_flags(&self, reverse: bool) -> Box<dyn KvIterator> {
        if reverse {
            self.iter_reverse_with_flags(None)
        } else {
            self.iter_with_flags(None, None)
        }
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
    fn checkpoint_value(&self) -> usize {
        self.checkpoint()
    }
    fn revert_checkpoint(&mut self, checkpoint: usize) {
        self.revert_to_checkpoint(checkpoint);
    }
    fn inspect_stage_values(
        &self,
        handle: usize,
        function: &mut dyn FnMut(&[u8], KeyFlags, &[u8]),
    ) {
        self.inspect_stage(handle, function);
    }
    fn snapshot_value(&self) -> Self::Snapshot {
        self.snapshot_getter()
    }
    fn len_value(&self) -> usize {
        self.len()
    }
    fn size_value(&self) -> usize {
        self.size()
    }
    fn dirty_value(&self) -> bool {
        self.dirty()
    }
    fn reset_value(&mut self) {
        self.reset();
    }
    fn remove_value(&mut self, key: &[u8]) {
        self.remove_from_buffer(key);
    }
    fn set_limits(&mut self, entry: u64, buffer: u64) {
        self.set_entry_size_limit(entry, buffer);
    }
    fn memory_value(&self) -> u64 {
        self.memory_footprint()
    }
    fn memory_hook_set(&self) -> bool {
        self.memory_hook_is_set()
    }
    fn set_memory_hook(&mut self, hook: Arc<dyn Fn(u64) + Send + Sync>) {
        self.set_memory_footprint_change_hook(hook);
    }
    fn cache_hits(&self) -> u64 {
        self.cache_hit_count()
    }
    fn cache_misses(&self) -> u64 {
        self.cache_miss_count()
    }
    fn select_history(
        &mut self,
        key: &[u8],
        predicate: &mut dyn FnMut(&[u8]) -> bool,
    ) -> Result<Option<Vec<u8>>, StaticError> {
        self.select_value_history(key, predicate)
    }
}

impl SourceBuffer for RbtMemDb {
    type Snapshot = RbtMemDbSnapshot;

    fn set_value(&mut self, key: &[u8], value: &[u8]) -> Result<(), BufferError> {
        self.set(key, value)
    }
    fn set_value_with_flags(
        &mut self,
        key: &[u8],
        value: &[u8],
        flags: &[FlagsOp],
    ) -> Result<(), BufferError> {
        self.set_with_flags(key, value, flags)
    }
    fn delete_value(&mut self, key: &[u8]) -> Result<(), BufferError> {
        self.delete(key)
    }
    fn update_key_flags(&mut self, key: &[u8], flags: &[FlagsOp]) {
        self.update_flags(key, flags);
    }
    fn get_value(&mut self, key: &[u8]) -> Result<Vec<u8>, StaticError> {
        self.get(key)
    }
    fn get_key_flags(&mut self, key: &[u8]) -> Result<KeyFlags, StaticError> {
        self.get_flags(key)
    }
    fn iter_values(
        &self,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
    ) -> Box<dyn KvIterator> {
        self.iter(lower, upper)
    }
    fn iter_values_reverse(
        &self,
        upper: Option<&[u8]>,
        lower: Option<&[u8]>,
    ) -> Box<dyn KvIterator> {
        self.iter_reverse(upper, lower)
    }
    fn iter_values_with_flags(&self, reverse: bool) -> Box<dyn KvIterator> {
        if reverse {
            self.iter_reverse_with_flags(None)
        } else {
            self.iter_with_flags(None, None)
        }
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
    fn checkpoint_value(&self) -> usize {
        self.checkpoint()
    }
    fn revert_checkpoint(&mut self, checkpoint: usize) {
        self.revert_to_checkpoint(checkpoint);
    }
    fn inspect_stage_values(
        &self,
        handle: usize,
        function: &mut dyn FnMut(&[u8], KeyFlags, &[u8]),
    ) {
        self.inspect_stage(handle, function);
    }
    fn snapshot_value(&self) -> Self::Snapshot {
        self.snapshot_getter()
    }
    fn len_value(&self) -> usize {
        self.len()
    }
    fn size_value(&self) -> usize {
        self.size()
    }
    fn dirty_value(&self) -> bool {
        self.dirty()
    }
    fn reset_value(&mut self) {
        self.reset();
    }
    fn remove_value(&mut self, key: &[u8]) {
        self.remove_from_buffer(key);
    }
    fn set_limits(&mut self, entry: u64, buffer: u64) {
        self.set_entry_size_limit(entry, buffer);
    }
    fn memory_value(&self) -> u64 {
        self.memory_footprint()
    }
    fn memory_hook_set(&self) -> bool {
        self.memory_hook_is_set()
    }
    fn set_memory_hook(&mut self, hook: Arc<dyn Fn(u64) + Send + Sync>) {
        self.set_memory_footprint_change_hook(hook);
    }
    fn cache_hits(&self) -> u64 {
        self.cache_hit_count()
    }
    fn cache_misses(&self) -> u64 {
        self.cache_miss_count()
    }
    fn select_history(
        &mut self,
        key: &[u8],
        predicate: &mut dyn FnMut(&[u8]) -> bool,
    ) -> Result<Option<Vec<u8>>, StaticError> {
        self.select_value_history(key, predicate)
    }
}

macro_rules! check_both_buffers {
    ($check:ident) => {{
        $check(&mut RbtMemDb::new());
        $check(&mut MemDb::new());
    }};
}

fn u32_key(number: usize) -> [u8; 4] {
    (number as u32).to_be_bytes()
}

fn derive_and_fill<B: SourceBuffer>(
    buffer: &mut B,
    start: usize,
    end: usize,
    value_base: usize,
) -> usize {
    let handle = buffer.staging_handle();
    for number in start..end {
        buffer
            .set_value(&u32_key(number), &u32_key(number + value_base))
            .unwrap();
    }
    handle
}

fn fill<B: SourceBuffer>(buffer: &mut B, count: usize) {
    let handle = derive_and_fill(buffer, 0, count, 0);
    buffer.release_handle(handle);
}

fn collect_iterator(mut iterator: Box<dyn KvIterator>) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = Vec::new();
    while iterator.valid() {
        entries.push((iterator.key().to_vec(), iterator.value().to_vec()));
        iterator.next().unwrap();
    }
    entries
}

fn decimal_key(number: usize) -> Vec<u8> {
    format!("{number:010}").into_bytes()
}

#[test]
fn source_test_get_set() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        fill(buffer, 10_000);
        for number in 0..10_000 {
            assert_eq!(buffer.get_value(&u32_key(number)).unwrap(), u32_key(number));
        }
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_iterator() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        fill(buffer, 10_000);
        let forward = collect_iterator(buffer.iter_values(None, None));
        assert_eq!(forward.len(), 10_000);
        for (number, (key, value)) in forward.iter().enumerate() {
            assert_eq!(key, &u32_key(number));
            assert_eq!(value, &u32_key(number));
        }
        let reverse = collect_iterator(buffer.iter_values_reverse(None, None));
        for (offset, (key, value)) in reverse.iter().enumerate() {
            let expected = u32_key(9_999 - offset);
            assert_eq!(key, &expected);
            assert_eq!(value, &expected);
        }
        assert_eq!(buffer.iter_values(None, Some(&u32_key(400))).count(), 400);
        assert_eq!(
            buffer
                .iter_values_reverse(None, Some(&u32_key(400)))
                .count(),
            9_600
        );
    }
    check_both_buffers!(check);
}

trait IteratorCount {
    fn count(&mut self) -> usize;
}

impl IteratorCount for Box<dyn KvIterator> {
    fn count(&mut self) -> usize {
        let mut count = 0;
        while self.valid() {
            count += 1;
            self.next().unwrap();
        }
        count
    }
}

#[test]
fn source_test_discard() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let base = derive_and_fill(buffer, 0, 10_000, 0);
        let size = buffer.size_value();
        let overwrite = derive_and_fill(buffer, 0, 10_000, 1);
        buffer.cleanup_handle(overwrite);
        assert_eq!(buffer.len_value(), 10_000);
        assert_eq!(buffer.size_value(), size);
        for number in 0..10_000 {
            assert_eq!(buffer.get_value(&u32_key(number)).unwrap(), u32_key(number));
        }
        assert_eq!(collect_iterator(buffer.iter_values(None, None)).len(), 10_000);
        assert_eq!(
            collect_iterator(buffer.iter_values_reverse(None, None)).len(),
            10_000
        );
        buffer.cleanup_handle(base);
        for number in 0..10_000 {
            assert_eq!(buffer.get_value(&u32_key(number)), Err(StaticError::NotExist));
        }
        assert!(!buffer.iter_values(None, None).valid());
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_flush_overwrite() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let base = derive_and_fill(buffer, 0, 10_000, 0);
        buffer.release_handle(base);
        let size = buffer.size_value();
        let overwrite = derive_and_fill(buffer, 0, 10_000, 1);
        buffer.release_handle(overwrite);
        assert_eq!(buffer.len_value(), 10_000);
        assert_eq!(buffer.size_value(), size);
        for number in 0..10_000 {
            assert_eq!(
                buffer.get_value(&u32_key(number)).unwrap(),
                u32_key(number + 1)
            );
        }
        let forward = collect_iterator(buffer.iter_values(None, None));
        let reverse = collect_iterator(buffer.iter_values_reverse(None, None));
        assert_eq!(forward.len(), 10_000);
        assert_eq!(reverse.len(), 10_000);
        assert_eq!(forward[0].1, u32_key(1));
        assert_eq!(reverse[0].1, u32_key(10_000));
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_complex_update() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let base = derive_and_fill(buffer, 0, 6_000, 0);
        buffer.release_handle(base);
        let update = derive_and_fill(buffer, 3_000, 9_000, 1);
        buffer.release_handle(update);
        assert_eq!(buffer.len_value(), 9_000);
        for number in 0..9_000 {
            let expected = if number < 3_000 { number } else { number + 1 };
            assert_eq!(buffer.get_value(&u32_key(number)).unwrap(), u32_key(expected));
        }
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_nested_sandbox() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let h0 = derive_and_fill(buffer, 0, 200, 0);
        let h1 = derive_and_fill(buffer, 0, 100, 1);
        let h2 = derive_and_fill(buffer, 50, 150, 2);
        let h3 = derive_and_fill(buffer, 100, 120, 3);
        let h4 = derive_and_fill(buffer, 0, 150, 4);
        buffer.cleanup_handle(h4);
        buffer.release_handle(h3);
        buffer.cleanup_handle(h2);
        buffer.release_handle(h1);
        buffer.release_handle(h0);
        for number in 0..200 {
            let expected = if number < 100 { number + 1 } else { number };
            assert_eq!(buffer.get_value(&u32_key(number)).unwrap(), u32_key(expected));
        }
        assert_eq!(collect_iterator(buffer.iter_values(None, None)).len(), 200);
        assert_eq!(
            collect_iterator(buffer.iter_values_reverse(None, None)).len(),
            200
        );
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_overwrite() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        fill(buffer, 10_000);
        let size = buffer.size_value();
        for number in (0..10_000).step_by(3) {
            buffer
                .set_value(&u32_key(number), &u32_key(number * 10))
                .unwrap();
        }
        assert_eq!(buffer.len_value(), 10_000);
        assert_eq!(buffer.size_value(), size);
        for number in 0..10_000 {
            let expected = if number % 3 == 0 { number * 10 } else { number };
            assert_eq!(buffer.get_value(&u32_key(number)).unwrap(), u32_key(expected));
        }
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_reset() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        fill(buffer, 1_000);
        buffer.reset_value();
        assert_eq!(buffer.get_value(&[0; 4]), Err(StaticError::NotExist));
        assert_eq!(buffer.get_key_flags(&[0; 4]), Err(StaticError::NotExist));
        assert!(!buffer.iter_values(None, None).valid());
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_inspect_stage() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let h1 = derive_and_fill(buffer, 0, 1_000, 0);
        let h2 = derive_and_fill(buffer, 500, 1_000, 1);
        for number in 500..1_500 {
            let mut value = Vec::from(u32_key(number + 2));
            value.push(0);
            buffer.set_value(&u32_key(number), &value).unwrap();
        }
        let h3 = derive_and_fill(buffer, 1_000, 2_000, 3);

        let mut seen_h3 = 0;
        buffer.inspect_stage_values(h3, &mut |key, _, value| {
            let key = u32::from_be_bytes(key.try_into().unwrap()) as usize;
            let value = u32::from_be_bytes(value[..4].try_into().unwrap()) as usize;
            assert!((1_000..2_000).contains(&key));
            assert_eq!(value - key, 3);
            seen_h3 += 1;
        });
        assert_eq!(seen_h3, 1_000);

        let mut seen_h2 = BTreeMap::new();
        buffer.inspect_stage_values(h2, &mut |key, _, value| {
            seen_h2.insert(key.to_vec(), value.to_vec());
        });
        assert_eq!(seen_h2.len(), 1_500);
        for number in 500..2_000 {
            let expected = if number < 1_000 { number + 2 } else { number + 3 };
            assert_eq!(seen_h2[&u32_key(number)[..]][..4], u32_key(expected));
        }

        buffer.cleanup_handle(h3);
        buffer.release_handle(h2);
        let mut seen_h1 = BTreeMap::new();
        buffer.inspect_stage_values(h1, &mut |key, _, value| {
            seen_h1.insert(key.to_vec(), value.to_vec());
        });
        assert_eq!(seen_h1.len(), 1_500);
        for number in 0..1_500 {
            let expected = if number < 500 { number } else { number + 2 };
            assert_eq!(seen_h1[&u32_key(number)[..]][..4], u32_key(expected));
        }
        buffer.release_handle(h1);
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_dirty() {
    fn check<B: SourceBuffer + Default>() {
        let mut buffer = B::default();
        buffer.set_value(&[1], &[1]).unwrap();
        assert!(buffer.dirty_value());

        let mut buffer = B::default();
        let stage = buffer.staging_handle();
        buffer.set_value(&[1], &[1]).unwrap();
        buffer.cleanup_handle(stage);
        assert!(!buffer.dirty_value());
        let stage = buffer.staging_handle();
        buffer.set_value(&[1], &[1]).unwrap();
        buffer.release_handle(stage);
        assert!(buffer.dirty_value());

        let mut buffer = B::default();
        let stage = buffer.staging_handle();
        buffer
            .set_value_with_flags(&[1], &[1], &[FlagsOp::SetKeyLocked])
            .unwrap();
        buffer.cleanup_handle(stage);
        assert!(buffer.dirty_value());

        let mut buffer = B::default();
        let stage = buffer.staging_handle();
        buffer
            .set_value_with_flags(&[1], &[1], &[FlagsOp::SetPresumeKeyNotExists])
            .unwrap();
        buffer.cleanup_handle(stage);
        assert!(!buffer.dirty_value());
    }
    check::<RbtMemDb>();
    check::<MemDb>();
}

#[test]
fn source_test_flags() {
    fn check<B: SourceBuffer + Default>(reverse: bool) {
        let mut buffer = B::default();
        let stage = buffer.staging_handle();
        for number in 0..10_000u32 {
            let key = number.to_be_bytes();
            let flags = if number % 2 == 0 {
                &[FlagsOp::SetPresumeKeyNotExists, FlagsOp::SetKeyLocked][..]
            } else {
                &[FlagsOp::SetPresumeKeyNotExists][..]
            };
            buffer.set_value_with_flags(&key, &key, flags).unwrap();
        }
        buffer.cleanup_handle(stage);
        for number in 0..10_000u32 {
            let key = number.to_be_bytes();
            assert_eq!(buffer.get_value(&key), Err(StaticError::NotExist));
            if number % 2 == 0 {
                let flags = buffer.get_key_flags(&key).unwrap();
                assert!(flags.has_locked());
                assert!(!flags.has_presume_key_not_exists());
            } else {
                assert_eq!(buffer.get_key_flags(&key), Err(StaticError::NotExist));
            }
        }
        assert_eq!(buffer.len_value(), 5_000);
        assert_eq!(buffer.size_value(), 20_000);
        assert!(!buffer.iter_values(None, None).valid());
        let mut iterator = buffer.iter_values_with_flags(reverse);
        let mut seen = 0;
        while iterator.valid() {
            assert_eq!(u32::from_be_bytes(iterator.key().try_into().unwrap()) % 2, 0);
            assert!(!iterator.has_value());
            seen += 1;
            iterator.next().unwrap();
        }
        assert_eq!(seen, 5_000);
        for number in 0..10_000u32 {
            buffer.update_key_flags(&number.to_be_bytes(), &[FlagsOp::DelKeyLocked]);
        }
        for number in 0..10_000u32 {
            let key = number.to_be_bytes();
            assert_eq!(buffer.get_value(&key), Err(StaticError::NotExist));
            assert!(!buffer.get_key_flags(&key).unwrap().has_locked());
        }
    }
    check::<RbtMemDb>(false);
    check::<MemDb>(false);
    check::<RbtMemDb>(true);
    check::<MemDb>(true);
}

#[test]
fn source_test_kv_get_set() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        for number in [0usize, 2] {
            let key = decimal_key(number);
            buffer.set_value(&key, &key).unwrap();
        }
        for number in [0usize, 2] {
            let key = decimal_key(number);
            assert_eq!(buffer.get_value(&key).unwrap(), key);
        }
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_new_iterator() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        assert!(!buffer.iter_values(None, None).valid());
        for number in [0usize, 2] {
            let key = decimal_key(number);
            buffer.set_value(&key, &key).unwrap();
        }
        for number in [0usize, 2] {
            let key = decimal_key(number);
            let mut iterator = buffer.iter_values(Some(&key), None);
            assert_eq!(iterator.key(), key);
            assert_eq!(iterator.value(), key);
        }
        let mut iterator = buffer.iter_values(Some(&decimal_key(0)), None);
        iterator.next().unwrap();
        assert_eq!(iterator.key(), decimal_key(2));
        assert!(!buffer.iter_values(Some(&decimal_key(4)), None).valid());
        assert_eq!(
            buffer.iter_values(Some(&decimal_key(1)), None).key(),
            decimal_key(2)
        );
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_iter_next_until() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        for number in [0usize, 2] {
            let key = decimal_key(number);
            buffer.set_value(&key, &key).unwrap();
        }
        let mut iterator = buffer.iter_values(None, None);
        while iterator.valid() {
            iterator.next().unwrap();
        }
        assert!(!iterator.valid());
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_basic_new_iterator() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        assert!(!buffer.iter_values(Some(b"2"), None).valid());
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_new_iterator_min() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let entries = [
            ("DATA_test_main_db_tbl_tbl_test_record__00000000000000000001", "lock-version"),
            ("DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0002", "1"),
            ("DATA_test_main_db_tbl_tbl_test_record__00000000000000000001_0003", "hello"),
            ("DATA_test_main_db_tbl_tbl_test_record__00000000000000000002", "lock-version"),
            ("DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0002", "2"),
            ("DATA_test_main_db_tbl_tbl_test_record__00000000000000000002_0003", "hello"),
        ];
        for (key, value) in entries {
            buffer.set_value(key.as_bytes(), value.as_bytes()).unwrap();
        }
        assert_eq!(collect_iterator(buffer.iter_values(None, None)).len(), 6);
        assert_eq!(
            buffer
                .iter_values(
                    Some(b"DATA_test_main_db_tbl_tbl_test_record__00000000000000000000"),
                    None,
                )
                .key(),
            b"DATA_test_main_db_tbl_tbl_test_record__00000000000000000001"
        );
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_memdb_staging() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        buffer.set_value(b"x", &[0; 2]).unwrap();
        let h1 = buffer.staging_handle();
        buffer.set_value(b"x", &[0; 3]).unwrap();
        let h2 = buffer.staging_handle();
        buffer.set_value(b"yz", &[0]).unwrap();
        assert_eq!(buffer.get_value(b"x").unwrap().len(), 3);
        buffer.release_handle(h2);
        assert_eq!(buffer.get_value(b"yz").unwrap().len(), 1);
        buffer.cleanup_handle(h1);
        assert_eq!(buffer.get_value(b"x").unwrap().len(), 2);
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_memdb_multi_level_staging() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        for depth in 0..100usize {
            assert_eq!(buffer.staging_handle(), depth + 1);
            buffer.set_value(&[0], &[depth as u8]).unwrap();
            assert_eq!(buffer.get_value(&[0]).unwrap(), [depth as u8]);
        }
        for depth in (0..100usize).rev() {
            let expected = if depth % 2 == 1 { depth - 1 } else { depth };
            if depth % 2 == 1 {
                buffer.cleanup_handle(depth + 1);
            } else {
                buffer.release_handle(depth + 1);
            }
            assert_eq!(buffer.get_value(&[0]).unwrap(), [expected as u8]);
        }
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_invalid_staging_handle() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let h1 = buffer.staging_handle();
        let h2 = buffer.staging_handle();
        assert!(catch_unwind(AssertUnwindSafe(|| buffer.release_handle(h2 + 1))).is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| buffer.release_handle(h2 - 1))).is_err());
        buffer.release_handle(0);
        buffer.release_handle(h2);
        buffer.release_handle(0);
        buffer.release_handle(h1);
        buffer.release_handle(0);

        let h1 = buffer.staging_handle();
        let h2 = buffer.staging_handle();
        buffer.cleanup_handle(h2 + 1);
        assert!(catch_unwind(AssertUnwindSafe(|| buffer.cleanup_handle(h2 - 1))).is_err());
        buffer.cleanup_handle(0);
        buffer.cleanup_handle(h2);
        buffer.cleanup_handle(0);
        buffer.cleanup_handle(h1);
        buffer.cleanup_handle(0);
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_memdb_checkpoint() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let cp1 = buffer.checkpoint_value();
        buffer.set_value(b"x", b"x").unwrap();
        let cp2 = buffer.checkpoint_value();
        buffer.set_value(b"y", b"y").unwrap();
        let stage = buffer.staging_handle();
        buffer.set_value(b"z", b"z").unwrap();
        buffer.release_handle(stage);
        for key in [b"x", b"y", b"z"] {
            assert_eq!(buffer.get_value(key).unwrap(), key);
        }
        buffer.revert_checkpoint(cp2);
        assert_eq!(buffer.get_value(b"x").unwrap(), b"x");
        assert_eq!(buffer.get_value(b"y"), Err(StaticError::NotExist));
        assert_eq!(buffer.get_value(b"z"), Err(StaticError::NotExist));
        buffer.revert_checkpoint(cp1);
        assert_eq!(buffer.get_value(b"x"), Err(StaticError::NotExist));
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_buffer_limit() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        buffer.set_limits(500, 1_000);
        assert!(buffer.set_value(b"x", &vec![0; 500]).is_err());
        buffer.set_value(b"x", &vec![0; 499]).unwrap();
        assert!(buffer.set_value(b"yz", &vec![0; 499]).is_err());
        buffer.delete_value(&vec![0; 499]).unwrap();
        assert!(buffer.delete_value(&vec![0; 500]).is_err());
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_unset_temporary_flag() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        buffer
            .set_value_with_flags(&[1], &[2], &[FlagsOp::SetNeedConstraintCheckInPrewrite])
            .unwrap();
        buffer.set_value(&[1], &[2]).unwrap();
        assert!(!buffer
            .get_key_flags(&[1])
            .unwrap()
            .has_need_constraint_check_in_prewrite());
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_snapshot_get_iter() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let mut getters = Vec::new();
        let mut iterators = Vec::new();
        let mut reverse_iterators = Vec::new();
        for value in 0..100u8 {
            buffer.set_value(&[0], &[value]).unwrap();
            buffer.set_value(&[1], &[value]).unwrap();
            let expected = value.min(50);

            let getter = buffer.snapshot_value();
            assert_eq!(getter.get_value(&[0]).unwrap(), [expected]);
            getters.push(getter);

            let snapshot = buffer.snapshot_value();
            let mut iterator = snapshot.iter_values(None, None, false);
            assert_eq!(iterator.key(), [0]);
            assert_eq!(iterator.value(), [expected]);
            iterators.push(iterator);

            let snapshot = buffer.snapshot_value();
            let mut iterator = snapshot.iter_values(None, None, true);
            assert_eq!(iterator.key(), [1]);
            assert_eq!(iterator.value(), [expected]);
            reverse_iterators.push(iterator);

            if value == 50 {
                let _ = buffer.staging_handle();
            }
        }
        for snapshot in &getters {
            assert_eq!(snapshot.get_value(&[0]).unwrap(), [50]);
        }
        for iterator in &mut iterators {
            assert_eq!(iterator.key(), [0]);
            assert_eq!(iterator.value(), [50]);
        }
        for iterator in &mut reverse_iterators {
            assert_eq!(iterator.key(), [1]);
            assert_eq!(iterator.value(), [50]);
        }

        buffer.reset_value();
        buffer.update_key_flags(&[255], &[FlagsOp::SetPresumeKeyNotExists]);
        for number in 1..50u8 {
            buffer.set_value(&[number * 2], &[number * 2]).unwrap();
        }
        let stage = buffer.staging_handle();
        for number in 0..100u8 {
            buffer.set_value(&[number], &[number * 2]).unwrap();
        }
        let snapshot = buffer.snapshot_value();
        assert_eq!(snapshot.get_value(&[2]).unwrap(), [2]);
        assert!(snapshot.get_value(&[1]).is_err());
        assert!(snapshot.get_value(&[254]).is_err());
        assert!(snapshot.get_value(&[255]).is_err());
        let forward = collect_iterator(snapshot.iter_values(None, None, false));
        let reverse = collect_iterator(snapshot.iter_values(None, None, true));
        assert_eq!(forward.len(), 49);
        assert_eq!(reverse.len(), 49);
        for (offset, (key, value)) in forward.iter().enumerate() {
            let expected = [((offset + 1) * 2) as u8];
            assert_eq!(key, &expected);
            assert_eq!(value, &expected);
        }
        buffer.release_handle(stage);
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_cleanup_keep_persistent_flag() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let stage = buffer.staging_handle();
        buffer
            .set_value_with_flags(&[1], &[1], &[FlagsOp::SetKeyLocked])
            .unwrap();
        buffer
            .set_value_with_flags(&[2], &[2], &[FlagsOp::SetPresumeKeyNotExists])
            .unwrap();
        buffer
            .set_value_with_flags(
                &[3],
                &[3],
                &[FlagsOp::SetKeyLocked, FlagsOp::SetPresumeKeyNotExists],
            )
            .unwrap();
        buffer.cleanup_handle(stage);
        for key in [1u8, 2, 3] {
            assert_eq!(buffer.get_value(&[key]), Err(StaticError::NotExist));
        }
        assert!(buffer.get_key_flags(&[1]).unwrap().has_locked());
        assert_eq!(buffer.get_key_flags(&[2]), Err(StaticError::NotExist));
        let flags = buffer.get_key_flags(&[3]).unwrap();
        assert!(flags.has_locked());
        assert!(!flags.has_presume_key_not_exists());
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_iter_no_result() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        buffer.set_value(&[1, 1], &[1, 1]).unwrap();
        for (lower, upper) in [
            (&[1, 1][..], &[1, 1][..]),
            (&[1, 0, 0][..], &[1, 0, 1][..]),
            (&[1, 0, 1][..], &[1, 0, 0][..]),
        ] {
            assert!(!buffer.iter_values(Some(lower), Some(upper)).valid());
            assert!(!buffer
                .iter_values_reverse(Some(upper), Some(lower))
                .valid());
        }
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_mem_buffer_cache() {
    fn check_delta<B: SourceBuffer>(buffer: &mut B, hit: bool, operation: impl FnOnce(&mut B)) {
        let before = (buffer.cache_hits(), buffer.cache_misses());
        operation(buffer);
        let after = (buffer.cache_hits(), buffer.cache_misses());
        assert_eq!(after.0 - before.0, u64::from(hit));
        assert_eq!(after.1 - before.1, u64::from(!hit));
    }
    fn check<B: SourceBuffer>(buffer: &mut B) {
        check_delta(buffer, false, |buffer| buffer.set_value(&[1], &[0]).unwrap());
        check_delta(buffer, true, |buffer| buffer.set_value(&[1], &[1]).unwrap());
        check_delta(buffer, false, |buffer| buffer.set_value(&[2], &[2]).unwrap());
        check_delta(buffer, true, |buffer| {
            assert_eq!(buffer.get_value(&[2]).unwrap(), [2]);
        });
        check_delta(buffer, false, |buffer| {
            assert_eq!(buffer.get_value(&[1]).unwrap(), [1]);
        });
        check_delta(buffer, true, |buffer| {
            assert_eq!(buffer.get_value(&[1]).unwrap(), [1]);
        });
        check_delta(buffer, false, |buffer| {
            assert_eq!(buffer.get_value(&[2]).unwrap(), [2]);
        });
        check_delta(buffer, true, |buffer| {
            buffer.set_value(&[2], &[2, 2]).unwrap();
        });
        check_delta(buffer, true, |buffer| {
            assert_eq!(buffer.get_value(&[2]).unwrap(), [2, 2]);
        });
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_memdb_leaf_fragmentation() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let mut stage = buffer.staging_handle();
        let mut previous = buffer.memory_value();
        for _ in 0..10 {
            for number in 0..100 {
                let key = number.to_string().repeat(256);
                buffer.set_value(key.as_bytes(), b"value").unwrap();
            }
            let current = buffer.memory_value();
            if previous != 0 {
                assert!(current <= previous);
            }
            previous = current;
            buffer.cleanup_handle(stage);
            stage = buffer.staging_handle();
        }
        buffer.cleanup_handle(stage);
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_read_only_zero_mem() {
    assert_eq!(RbtMemDb::new().memory_footprint(), 0);
    assert_eq!(MemDb::new().memory_footprint(), 0);
}

#[test]
fn source_test_key_value_oversize() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let key = vec![0; u16::MAX as usize];
        let oversized = vec![0; u16::MAX as usize + 1];
        buffer.set_value(&key, &oversized).unwrap();
        let error = buffer.set_value(&oversized, &key).unwrap_err();
        assert_eq!(
            error.downcast_ref::<KeyTooLargeError>().unwrap().key_size,
            u16::MAX as isize + 1
        );
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_set_memory_footprint_change_hook() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        assert!(!buffer.memory_hook_set());
        let observed = Arc::new(AtomicU64::new(0));
        let hook_observed = observed.clone();
        buffer.set_memory_hook(Arc::new(move |memory| {
            hook_observed.store(memory, Ordering::Release);
        }));
        assert!(buffer.memory_hook_set());
        assert_eq!(observed.load(Ordering::Acquire), 0);
        buffer.set_value(&[1], &[1]).unwrap();
        assert_ne!(observed.load(Ordering::Acquire), 0);
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_select_value_history() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        buffer.set_value(&[1], &[1]).unwrap();
        let stage = buffer.staging_handle();
        buffer.set_value(&[1], &[1, 1]).unwrap();
        assert_eq!(
            buffer
                .select_history(&[1], &mut |value| value == [1])
                .unwrap(),
            Some(vec![1])
        );
        assert_eq!(
            buffer
                .select_history(&[1], &mut |value| value == [1, 1])
                .unwrap(),
            Some(vec![1, 1])
        );
        assert_eq!(
            buffer
                .select_history(&[1], &mut |value| value == [1, 1, 1])
                .unwrap(),
            None
        );
        assert_eq!(
            buffer.select_history(&[2], &mut |_| false),
            Err(StaticError::NotExist)
        );
        buffer.cleanup_handle(stage);
        assert_eq!(
            buffer
                .select_history(&[1], &mut |value| value == [1])
                .unwrap(),
            Some(vec![1])
        );
        assert_eq!(
            buffer
                .select_history(&[1], &mut |value| value == [1, 1])
                .unwrap(),
            None
        );
    }
    check_both_buffers!(check);
}

#[test]
fn source_test_snapshot_reader_with_write() {
    fn check<B: SourceBuffer>(buffer: &mut B, count: u8) {
        for value in 0..count {
            buffer.set_value(&[0, value], &[0, value]).unwrap();
        }
        let stage = buffer.staging_handle();
        let snapshot = buffer.snapshot_value();
        let mut iterator = snapshot.iter_values(Some(&[0, 0]), Some(&[0, 255]), false);
        assert_eq!(iterator.key(), [0, 0]);
        buffer.set_value(&[0, count], &[0, count]).unwrap();
        for value in 0..count {
            buffer.set_value(&[1, value], &[1, value]).unwrap();
        }
        for value in 0..count {
            assert!(iterator.valid());
            assert_eq!(iterator.key(), [0, value]);
            iterator.next().unwrap();
        }
        assert!(!iterator.valid());
        buffer.release_handle(stage);
    }
    for count in [4, 16, 48] {
        check(&mut RbtMemDb::new(), count);
        check(&mut MemDb::new(), count);
    }
}

fn collect_batched(mut iterator: BatchedSnapshotIterator) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    while iterator.valid() {
        keys.push(iterator.key().to_vec());
        iterator.next().unwrap();
    }
    keys
}

#[test]
fn source_test_batched_snapshot_iter() {
    for count in [3u8, 17, 64] {
        for reverse in [false, true] {
            let mut db = MemDb::new();
            for value in 0..count {
                db.set(&[0, value], &[0, value]).unwrap();
            }
            let stage = db.staging();
            let snapshot = db.snapshot();
            let mut iterator = snapshot.batched_iter(Some(&[0, 0]), Some(&[0, 255]), reverse);
            assert!(iterator.valid());
            let first_key = if reverse {
                vec![0, count - 1]
            } else {
                vec![0, 0]
            };
            assert_eq!(iterator.key(), first_key);
            db.set(&[0, count], &[0, count]).unwrap();
            for value in 0..count {
                db.set(&[1, value], &[1, value]).unwrap();
            }
            let mut seen = Vec::new();
            while iterator.valid() {
                seen.push(iterator.key().to_vec());
                let key = iterator.key().to_vec();
                assert_eq!(key, iterator.value());
                iterator.next().unwrap();
            }
            let mut expected: Vec<_> = (0..count).map(|value| vec![0, value]).collect();
            if reverse {
                expected.reverse();
            }
            assert_eq!(seen, expected);
            db.release(stage);
        }
    }
}

#[test]
fn source_test_batched_snapshot_iter_edge_cases() {
    let mut db = MemDb::new();
    let stage = db.staging();
    let snapshot = db.snapshot();
    assert!(!snapshot.batched_iter(Some(&[1]), Some(&[1]), false).valid());
    assert!(!snapshot.batched_iter(Some(&[0]), Some(&[1]), false).valid());
    db.set(&[1], &[1]).unwrap();
    db.release(stage);
    let stage = db.staging();
    let snapshot = db.snapshot();
    assert_eq!(collect_batched(snapshot.batched_iter(Some(&[1]), Some(&[2]), false)), [vec![1]]);
    for value in 2..=4u8 {
        db.set(&[value], &[value]).unwrap();
    }
    db.release(stage);
    let _ = db.staging();
    let snapshot = db.snapshot();
    assert_eq!(
        collect_batched(snapshot.batched_iter(Some(&[2]), Some(&[4]), false)),
        [vec![2], vec![3]]
    );
    assert_eq!(
        collect_batched(snapshot.batched_iter(Some(&[2]), Some(&[4]), true)),
        [vec![3], vec![2]]
    );
}

#[test]
fn source_test_batched_snapshot_iter_boundary_tests() {
    let mut db = MemDb::new();
    for key in [[1, 0], [1, 2], [1, 4], [1, 6], [1, 8]] {
        db.set(&key, &key).unwrap();
    }
    let _ = db.staging();
    let snapshot = db.snapshot();
    assert_eq!(
        collect_batched(snapshot.batched_iter(Some(&[1, 2]), Some(&[1, 9]), false)),
        [vec![1, 2], vec![1, 4], vec![1, 6], vec![1, 8]]
    );
    assert_eq!(
        collect_batched(snapshot.batched_iter(Some(&[1, 0]), Some(&[1, 6]), false)),
        [vec![1, 0], vec![1, 2], vec![1, 4]]
    );
    assert_eq!(
        collect_batched(snapshot.batched_iter(Some(&[1, 0]), Some(&[1, 6]), true)),
        [vec![1, 4], vec![1, 2], vec![1, 0]]
    );
}

#[test]
fn source_test_batched_snapshot_iter_alphabetical_order() {
    let keys = [vec![2], vec![2, 1], vec![2, 1, 1], vec![2, 1, 1, 1]];
    let mut db = MemDb::new();
    for key in &keys {
        db.set(key, key).unwrap();
    }
    let _ = db.staging();
    let snapshot = db.snapshot();
    assert_eq!(
        collect_batched(snapshot.batched_iter(Some(&[2]), Some(&[3]), false)),
        keys
    );
    let mut reverse = keys.to_vec();
    reverse.reverse();
    assert_eq!(
        collect_batched(snapshot.batched_iter(Some(&[2]), Some(&[3]), true)),
        reverse
    );
}

#[test]
fn source_test_batched_snapshot_iter_batch_size_growth() {
    let mut db = MemDb::new();
    for value in 0..100u8 {
        db.set(&[3, value], &[3, value]).unwrap();
    }
    let _ = db.staging();
    let snapshot = db.snapshot();
    let forward = collect_batched(snapshot.batched_iter(Some(&[3, 0]), Some(&[3, 255]), false));
    let reverse = collect_batched(snapshot.batched_iter(Some(&[3, 0]), Some(&[3, 255]), true));
    assert_eq!(forward, (0..100u8).map(|value| vec![3, value]).collect::<Vec<_>>());
    assert_eq!(reverse, (0..100u8).rev().map(|value| vec![3, value]).collect::<Vec<_>>());
}

#[test]
fn source_test_batched_snapshot_iter_snapshot_change() {
    let mut db = MemDb::new();
    db.set(&[0], &[0]).unwrap();
    let stage = db.staging();
    let snapshot = db.snapshot();
    db.set(&[1], &[1]).unwrap();
    let mut iterator = snapshot.batched_iter(Some(&[0]), Some(&[255]), false);
    assert!(iterator.valid());
    iterator.next().unwrap();
    db.release(stage);
    let _ = db.staging();
    assert!(!iterator.valid());
    assert!(iterator.next().is_err());
}

fn collect_union(mut iterator: UnionIterator) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut entries = Vec::new();
    while iterator.valid() {
        entries.push((iterator.key().to_vec(), iterator.value().to_vec()));
        iterator.next().unwrap();
    }
    entries
}

#[test]
fn source_test_union_store_get_set() {
    let mut snapshot = MapSnapshot::default();
    snapshot.insert(b"1", b"1");
    let mut store = UnionStore::new(MemDb::new(), snapshot);

    assert_eq!(store.get_entry(b"1", &[]).unwrap(), ValueEntry::new(b"1".to_vec(), 0));
    assert_eq!(
        store
            .get_entry(b"1", &[GetOption::ReturnCommitTs])
            .unwrap(),
        ValueEntry::new(b"1".to_vec(), 1000 + u64::from(b'1'))
    );
    store.mem_buffer().set(b"1", b"2").unwrap();
    assert_eq!(store.get_entry(b"1", &[]).unwrap(), ValueEntry::new(b"2".to_vec(), 0));
    assert_eq!(
        store
            .get_entry(b"1", &[GetOption::ReturnCommitTs])
            .unwrap(),
        ValueEntry::new(b"2".to_vec(), 0)
    );
    assert_eq!(store.mem_buffer().size(), 2);
    assert_eq!(store.mem_buffer().len(), 1);
}

#[test]
fn source_test_union_store_delete() {
    let mut snapshot = MapSnapshot::default();
    snapshot.insert(b"1", b"1");
    let mut store = UnionStore::new(MemDb::new(), snapshot);
    store.mem_buffer().delete(b"1").unwrap();
    assert_eq!(store.get(b"1"), Err(StaticError::NotExist));
    store.mem_buffer().set(b"1", b"2").unwrap();
    assert_eq!(store.get(b"1").unwrap(), b"2");
}

#[test]
fn source_test_union_store_seek() {
    let mut snapshot = MapSnapshot::default();
    for key in [b"1", b"2", b"3"] {
        snapshot.insert(key.as_slice(), key.as_slice());
    }
    let mut store = UnionStore::new(MemDb::new(), snapshot);
    assert_eq!(
        collect_union(store.iter(None, None).unwrap()),
        [
            (b"1".to_vec(), b"1".to_vec()),
            (b"2".to_vec(), b"2".to_vec()),
            (b"3".to_vec(), b"3".to_vec()),
        ]
    );
    assert_eq!(
        collect_union(store.iter(Some(b"2"), None).unwrap()),
        [
            (b"2".to_vec(), b"2".to_vec()),
            (b"3".to_vec(), b"3".to_vec()),
        ]
    );
    store.mem_buffer().set(b"4", b"4").unwrap();
    assert_eq!(
        collect_union(store.iter(Some(b"2"), None).unwrap()),
        [
            (b"2".to_vec(), b"2".to_vec()),
            (b"3".to_vec(), b"3".to_vec()),
            (b"4".to_vec(), b"4".to_vec()),
        ]
    );
    store.mem_buffer().delete(b"3").unwrap();
    assert_eq!(
        collect_union(store.iter(Some(b"2"), None).unwrap()),
        [
            (b"2".to_vec(), b"2".to_vec()),
            (b"4".to_vec(), b"4".to_vec()),
        ]
    );
}

#[test]
fn source_test_union_store_iter_reverse() {
    let mut snapshot = MapSnapshot::default();
    for key in [b"1", b"2", b"3"] {
        snapshot.insert(key.as_slice(), key.as_slice());
    }
    let mut store = UnionStore::new(MemDb::new(), snapshot);
    assert_eq!(
        collect_union(store.iter_reverse(None, None).unwrap()),
        [
            (b"3".to_vec(), b"3".to_vec()),
            (b"2".to_vec(), b"2".to_vec()),
            (b"1".to_vec(), b"1".to_vec()),
        ]
    );
    assert_eq!(
        collect_union(store.iter_reverse(Some(b"3"), Some(b"1")).unwrap()),
        [
            (b"2".to_vec(), b"2".to_vec()),
            (b"1".to_vec(), b"1".to_vec()),
        ]
    );
    assert_eq!(
        collect_union(store.iter_reverse(Some(b"3"), None).unwrap()),
        [
            (b"2".to_vec(), b"2".to_vec()),
            (b"1".to_vec(), b"1".to_vec()),
        ]
    );
    store.mem_buffer().set(b"0", b"0").unwrap();
    assert_eq!(
        collect_union(store.iter_reverse(Some(b"3"), None).unwrap()),
        [
            (b"2".to_vec(), b"2".to_vec()),
            (b"1".to_vec(), b"1".to_vec()),
            (b"0".to_vec(), b"0".to_vec()),
        ]
    );
    store.mem_buffer().delete(b"1").unwrap();
    assert_eq!(
        collect_union(store.iter_reverse(Some(b"3"), None).unwrap()),
        [
            (b"2".to_vec(), b"2".to_vec()),
            (b"0".to_vec(), b"0".to_vec()),
        ]
    );
    assert_eq!(
        collect_union(store.iter_reverse(Some(b"3"), Some(b"1")).unwrap()),
        [(b"2".to_vec(), b"2".to_vec())]
    );
}

fn empty_remote_batch_getter() -> RemoteBatchGetter {
    Arc::new(|_| Ok(BTreeMap::new()))
}

fn blocking_flush() -> (
    FlushFunction,
    mpsc::Receiver<u64>,
    mpsc::SyncSender<()>,
) {
    let (started_sender, started_receiver) = mpsc::sync_channel(8);
    let (release_sender, release_receiver) = mpsc::sync_channel(8);
    let release_receiver = Arc::new(std::sync::Mutex::new(release_receiver));
    let flush = Arc::new(move |generation, _: Arc<MemDb>| {
        started_sender.send(generation).unwrap();
        release_receiver.lock().unwrap().recv().unwrap();
        Ok(())
    });
    (flush, started_receiver, release_sender)
}

#[test]
fn source_test_pipelined_flush_trigger() {
    const MIN_KEYS: usize = 4;
    const MIN_MEMORY: u64 = 40;
    const FORCE_MEMORY: u64 = 200;

    let (flush, started, release) = blocking_flush();
    let mut db = PipelinedMemDb::new(empty_remote_batch_getter(), flush);
    db.set_flush_thresholds(MIN_KEYS, MIN_MEMORY, FORCE_MEMORY);
    for number in 0..MIN_KEYS {
        db.set(number.to_string().as_bytes(), b"v").unwrap();
        assert!(!db.flush(false).unwrap());
        assert!(!db.on_flushing());
    }
    assert_eq!(db.mem.len(), MIN_KEYS);
    assert!(db.mem.memory_footprint() < MIN_MEMORY);
    assert!(started.try_recv().is_err());
    drop(release);

    let (flush, started, release) = blocking_flush();
    let mut db = PipelinedMemDb::new(empty_remote_batch_getter(), flush);
    db.set_flush_thresholds(MIN_KEYS, MIN_MEMORY, FORCE_MEMORY);
    for number in 0..MIN_KEYS - 1 {
        db.set(number.to_string().as_bytes(), &[number as u8; 20])
            .unwrap();
        assert!(!db.flush(false).unwrap());
        assert!(!db.on_flushing());
    }
    assert!(db.mem.len() < MIN_KEYS);
    assert!(db.mem.memory_footprint() >= MIN_MEMORY);
    assert!(db.mem.memory_footprint() < FORCE_MEMORY);
    assert!(started.try_recv().is_err());
    drop(release);

    let (flush, started, release) = blocking_flush();
    let mut db = PipelinedMemDb::new(empty_remote_batch_getter(), flush);
    db.set_flush_thresholds(MIN_KEYS, MIN_MEMORY, FORCE_MEMORY);
    for number in 0..MIN_KEYS {
        db.set(number.to_string().as_bytes(), &[number as u8; 20])
            .unwrap();
        let flushed = db.flush(false).unwrap();
        assert_eq!(flushed, number == MIN_KEYS - 1);
        assert_eq!(db.on_flushing(), number == MIN_KEYS - 1);
    }
    assert_eq!(started.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
    assert_eq!(db.mem.len(), 0);
    assert_eq!(db.mem.size(), 0);
    assert_eq!(db.len(), MIN_KEYS);
    assert_eq!(db.size(), db.flushing.as_ref().unwrap().size());
    release.send(()).unwrap();
    db.flush_wait().unwrap();
}

#[test]
fn source_test_pipelined_flush_skip() {
    let (flush, started, release) = blocking_flush();
    let mut db = PipelinedMemDb::new(empty_remote_batch_getter(), flush);
    db.set_flush_thresholds(2, 1, 1_000);
    for key in [b"a", b"b"] {
        db.set(key, b"value").unwrap();
    }
    assert!(db.flush(false).unwrap());
    assert_eq!(started.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
    assert!(db.on_flushing());
    assert_eq!(db.mem.len(), 0);
    for key in [b"c", b"d"] {
        db.set(key, b"value").unwrap();
    }
    assert!(!db.flush(false).unwrap());
    assert_eq!(db.mem.len(), 2);
    release.send(()).unwrap();
    db.flush_wait().unwrap();
    assert!(db.flush(false).unwrap());
    assert_eq!(started.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
    assert_eq!(db.mem.len(), 0);
    assert_eq!(db.accumulated_len, 4);
    release.send(()).unwrap();
    db.flush_wait().unwrap();
}

#[test]
fn source_test_pipelined_flush_block() {
    let (flush, started, release) = blocking_flush();
    let mut db = PipelinedMemDb::new(empty_remote_batch_getter(), flush);
    db.set_flush_thresholds(2, 1, 40);
    for key in [b"a", b"b"] {
        db.set(key, b"value").unwrap();
    }
    assert!(db.flush(false).unwrap());
    assert_eq!(started.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
    db.set(b"c", &[1; 48]).unwrap();
    assert!(db.mem.memory_footprint() >= 40);

    let (returned_sender, returned_receiver) = mpsc::sync_channel(1);
    std::thread::scope(|scope| {
        let handle = scope.spawn(|| {
            returned_sender.send(db.flush(false)).unwrap();
        });
        assert!(returned_receiver
            .recv_timeout(Duration::from_millis(100))
            .is_err());
        release.send(()).unwrap();
        assert_eq!(started.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
        assert!(returned_receiver
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap());
        handle.join().unwrap();
    });
    assert!(db.on_flushing());
    release.send(()).unwrap();
    db.flush_wait().unwrap();
}

#[test]
fn source_test_pipelined_flush_get() {
    let (flush, started, release) = blocking_flush();
    let mut db = PipelinedMemDb::new(empty_remote_batch_getter(), flush);
    db.set_flush_thresholds(2, 1, 1_000);
    db.set(b"key", b"value").unwrap();
    db.set(b"filler", b"value").unwrap();
    assert_eq!(db.get(b"key").unwrap(), b"value");
    assert!(db.flush(false).unwrap());
    assert_eq!(started.recv_timeout(Duration::from_secs(5)).unwrap(), 1);
    assert_eq!(db.mem.get(b"key"), Err(StaticError::NotExist));
    assert_eq!(db.get(b"key").unwrap(), b"value");

    release.send(()).unwrap();
    while db.on_flushing() {
        std::thread::yield_now();
    }
    db.set(b"next-a", b"value").unwrap();
    db.set(b"next-b", b"value").unwrap();
    assert!(db.flush(false).unwrap());
    assert_eq!(started.recv_timeout(Duration::from_secs(5)).unwrap(), 2);
    assert_eq!(db.get(b"key"), Err("key not found".to_owned()));
    release.send(()).unwrap();
    db.flush_wait().unwrap();
}

#[test]
fn source_test_pipelined_flush_size() {
    let flush: FlushFunction = Arc::new(|_, _| Ok(()));
    let mut db = PipelinedMemDb::new(empty_remote_batch_getter(), flush);
    db.set_flush_thresholds(4, 1, 1_000);
    let mut keys = 0;
    let mut size = 0;
    for number in 0..4 {
        let key = number.to_string().into_bytes();
        let value = vec![number as u8; 8];
        keys += 1;
        size += key.len() + value.len();
        db.set(&key, &value).unwrap();
        assert_eq!(db.len(), keys);
        assert_eq!(db.size(), size);
    }
    assert!(db.flush(false).unwrap());
    assert_eq!(db.mem.len(), 0);
    assert_eq!(db.mem.size(), 0);
    assert_eq!(db.len(), keys);
    assert_eq!(db.size(), size);
    for number in 4..8 {
        let key = number.to_string().into_bytes();
        let value = vec![number as u8; 8];
        keys += 1;
        size += key.len() + value.len();
        db.set(&key, &value).unwrap();
        assert_eq!(db.len(), keys);
        assert_eq!(db.size(), size);
    }
    assert!(db.flush(true).unwrap());
    assert_eq!(db.len(), keys);
    assert_eq!(db.size(), size);
    db.flush_wait().unwrap();
}

#[test]
fn source_test_pipelined_flush_generation() {
    let (generation_sender, generation_receiver) = mpsc::sync_channel(1);
    let flush = Arc::new(move |generation, _: Arc<MemDb>| {
        generation_sender.send(generation).unwrap();
        Ok(())
    });
    let mut db = PipelinedMemDb::new(empty_remote_batch_getter(), flush);
    for number in 0..100u64 {
        db.set(&[number as u8], &[number as u8]).unwrap();
        assert!(db.flush(true).unwrap());
        assert_eq!(
            generation_receiver
                .recv_timeout(Duration::from_secs(5))
                .unwrap(),
            number + 1
        );
    }
    db.flush_wait().unwrap();
}

#[test]
fn source_test_error_iterator() {
    fn returns_error(mut iterator: Box<dyn KvIterator>) -> bool {
        loop {
            if iterator.next().is_err() {
                return true;
            }
            if !iterator.valid() {
                return false;
            }
        }
    }
    let flush: FlushFunction = Arc::new(|_, _| Ok(()));
    let db = PipelinedMemDb::new(empty_remote_batch_getter(), flush);
    assert!(returns_error(db.snapshot_iter(None, None)));
    assert!(returns_error(db.snapshot_iter_reverse(None, None)));
}

#[test]
fn source_test_pipelined_adjust_flush_condition() {
    let flush: FlushFunction = Arc::new(|_, _| Ok(()));
    let mut db = PipelinedMemDb::new(empty_remote_batch_getter(), flush.clone());
    db.set(b"key", b"value").unwrap();
    assert!(!db.flush(false).unwrap());

    let mut db = PipelinedMemDb::new(empty_remote_batch_getter(), flush.clone());
    db.set_flush_thresholds(1, 1, u64::MAX);
    db.set(b"key", b"value").unwrap();
    assert!(db.flush(false).unwrap());
    db.flush_wait().unwrap();

    let mut db = PipelinedMemDb::new(empty_remote_batch_getter(), flush.clone());
    db.set_flush_thresholds(2, 1, u64::MAX);
    db.set(b"key", b"value").unwrap();
    assert!(!db.flush(false).unwrap());
    db.flush_wait().unwrap();

    let mut db = PipelinedMemDb::new(empty_remote_batch_getter(), flush);
    db.set_flush_thresholds(2, 1, 2);
    db.set(b"key", b"value").unwrap();
    assert!(db.flush(false).unwrap());
    db.flush_wait().unwrap();
}

#[test]
fn source_test_mem_buffer_batch_get_cache() {
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
    let (flush_done_sender, flush_done_receiver) = mpsc::sync_channel(8);
    let flush = {
        let remote = remote.clone();
        Arc::new(move |_, db: Arc<MemDb>| {
            let mut iterator = db.iter(None, None);
            let mut remote = remote.lock().unwrap();
            while iterator.valid() {
                remote.insert(iterator.key().to_vec(), iterator.value().to_vec());
                iterator.next().unwrap();
            }
            flush_done_sender.send(()).unwrap();
            Ok(())
        })
    };
    let mut db = PipelinedMemDb::new(remote_get, flush);
    let must_flush = |db: &mut PipelinedMemDb| {
        assert!(db.flush(true).unwrap());
        flush_done_receiver
            .recv_timeout(Duration::from_secs(5))
            .unwrap();
    };

    db.set(b"k1", b"v11").unwrap();
    must_flush(&mut db);
    db.set(b"k2", b"v21").unwrap();
    must_flush(&mut db);
    assert_eq!(db.get_local(b"k1"), Err(StaticError::NotExist));
    assert_eq!(db.get_local(b"k2").unwrap(), b"v21");
    assert_eq!(
        db.batch_get(&[b"k1".to_vec(), b"k2".to_vec()])
            .unwrap(),
        BTreeMap::from([
            (b"k1".to_vec(), b"v11".to_vec()),
            (b"k2".to_vec(), b"v21".to_vec()),
        ])
    );
    assert_eq!(
        db.batch_cache.as_ref().unwrap().get(b"k1" as &[u8]),
        Some(&Some(b"v11".to_vec()))
    );
    assert_eq!(
        db.batch_cache.as_ref().unwrap().get(b"k2" as &[u8]),
        Some(&Some(b"v21".to_vec()))
    );
    assert!(!db
        .batch_cache
        .as_ref()
        .unwrap()
        .contains_key(b"k3" as &[u8]));
    assert!(db.batch_get(&[b"k3".to_vec()]).unwrap().is_empty());
    assert_eq!(
        db.batch_cache.as_ref().unwrap().get(b"k3" as &[u8]),
        Some(&None)
    );

    db.delete(b"k1").unwrap();
    db.set(b"k2", b"v22").unwrap();
    db.delete(b"k3").unwrap();
    must_flush(&mut db);
    assert!(db.batch_cache.is_none());
    assert_eq!(
        db.batch_get(&[
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ])
        .unwrap(),
        BTreeMap::from([
            (b"k1".to_vec(), Vec::new()),
            (b"k2".to_vec(), b"v22".to_vec()),
            (b"k3".to_vec(), Vec::new()),
        ])
    );
    let cache = db.batch_cache.as_ref().unwrap();
    assert_eq!(cache.get(b"k1" as &[u8]), Some(&Some(Vec::new())));
    assert_eq!(cache.get(b"k2" as &[u8]), Some(&Some(b"v22".to_vec())));
    assert_eq!(cache.get(b"k3" as &[u8]), Some(&Some(Vec::new())));
    assert_eq!(cache.get(b"k4" as &[u8]), Some(&None));
    db.flush_wait().unwrap();
}

fn check_set_get_contract<B: SourceBuffer>(buffer: &mut B, entries: &[(Vec<u8>, Vec<u8>)]) {
    for (key, value) in entries {
        buffer.set_value(key, value).unwrap();
    }
    for (key, value) in entries {
        assert_eq!(buffer.get_value(key).unwrap(), *value);
    }
}

#[test]
fn source_benchmark_large_index_contract() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let entries: Vec<_> = (0..256u32)
            .map(|number| {
                let mut value = vec![0; 128];
                value[..4].copy_from_slice(&number.to_le_bytes());
                (value.clone(), value)
            })
            .collect();
        check_set_get_contract(buffer, &entries);
    }
    check_both_buffers!(check);
}

#[test]
fn source_benchmark_put_contract() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let entries: Vec<_> = (0..256u32)
            .map(|number| {
                let mut value = vec![0; 128];
                value[..4].copy_from_slice(&number.to_be_bytes());
                (value[..16].to_vec(), value)
            })
            .collect();
        check_set_get_contract(buffer, &entries);
    }
    check_both_buffers!(check);
}

#[test]
fn source_benchmark_put_random_contract() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let mut state = 0xd92f_3b71_4420_78ad;
        let entries: Vec<_> = (0..256)
            .map(|_| {
                let mut value = vec![0; 128];
                value[..8].copy_from_slice(&next_random(&mut state).to_le_bytes());
                (value[..16].to_vec(), value)
            })
            .collect();
        check_set_get_contract(buffer, &entries);
    }
    check_both_buffers!(check);
}

#[test]
fn source_benchmark_get_contract() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let entries: Vec<_> = (0..512u32)
            .map(|number| {
                let mut value = vec![0; 128];
                value[..4].copy_from_slice(&number.to_be_bytes());
                (value[..16].to_vec(), value)
            })
            .collect();
        check_set_get_contract(buffer, &entries);
        for _ in 0..4 {
            for (key, value) in &entries {
                assert_eq!(buffer.get_value(key).unwrap(), *value);
            }
        }
    }
    check_both_buffers!(check);
}

#[test]
fn source_benchmark_get_random_contract() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let mut state = 0x4dd1_937a_b840_3c2f;
        let entries: Vec<_> = (0..512)
            .map(|_| {
                let mut value = vec![0; 128];
                value[..8].copy_from_slice(&next_random(&mut state).to_le_bytes());
                (value[..16].to_vec(), value)
            })
            .collect();
        check_set_get_contract(buffer, &entries);
        for (key, value) in entries.iter().rev() {
            assert_eq!(buffer.get_value(key).unwrap(), *value);
        }
    }
    check_both_buffers!(check);
}

#[test]
fn source_benchmark_memdb_buffer_sequential_contract() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let entries: Vec<_> = (0..2_048u64)
            .map(|number| {
                let key = number.to_be_bytes().to_vec();
                (key.clone(), key)
            })
            .collect();
        check_set_get_contract(buffer, &entries);
    }
    check_both_buffers!(check);
}

#[test]
fn source_benchmark_memdb_buffer_random_contract() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let mut state = 0x87a5_19c3_0e46_2db1;
        let entries: Vec<_> = (0..2_048)
            .map(|_| {
                let key = next_random(&mut state).to_be_bytes().to_vec();
                (key.clone(), key)
            })
            .collect();
        check_set_get_contract(buffer, &entries);
    }
    check_both_buffers!(check);
}

#[test]
fn source_benchmark_memdb_iter_contract() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let entries: Vec<_> = (0..2_048u64)
            .map(|number| {
                let key = number.to_be_bytes().to_vec();
                (key.clone(), key)
            })
            .collect();
        check_set_get_contract(buffer, &entries);
        assert_eq!(collect_iterator(buffer.iter_values(None, None)), entries);
    }
    check_both_buffers!(check);
}

#[test]
fn source_benchmark_snapshot_iter_contract() {
    let entries: Vec<_> = (0..1_024u64)
        .map(|number| {
            let key = number.to_be_bytes().to_vec();
            (key.clone(), key)
        })
        .collect();

    let mut rbt = RbtMemDb::new();
    check_set_get_contract(&mut rbt, &entries);
    let _ = rbt.staging();
    assert_eq!(collect_iterator(rbt.snapshot_iter(None, None)), entries);
    assert_eq!(collect_iterator(rbt.batched_snapshot_iter(None, None, false)), entries);
    let mut rbt_for_each = Vec::new();
    rbt.for_each_in_snapshot_range(None, None, false, |key, value| {
        rbt_for_each.push((key.to_vec(), value.to_vec()));
        Ok(false)
    })
    .unwrap();
    assert_eq!(rbt_for_each, entries);

    let mut art = MemDb::new();
    check_set_get_contract(&mut art, &entries);
    let _ = art.staging();
    assert_eq!(collect_iterator(art.snapshot_iter(None, None)), entries);
    let snapshot = art.snapshot();
    let mut batched = snapshot.batched_iter(None, None, false);
    let mut art_batched = Vec::new();
    while batched.valid() {
        art_batched.push((batched.key().to_vec(), batched.value().to_vec()));
        batched.next().unwrap();
    }
    assert_eq!(art_batched, entries);
    let mut art_for_each = Vec::new();
    snapshot
        .for_each(None, None, false, |key, value| {
            art_for_each.push((key.to_vec(), value.to_vec()));
            Ok(false)
        })
        .unwrap();
    assert_eq!(art_for_each, entries);
}

#[test]
fn source_benchmark_memdb_creation_contract() {
    for _ in 0..1_000 {
        assert!(MemDb::new().is_empty());
        assert!(RbtMemDb::new().is_empty());
    }
}

#[test]
fn source_benchmark_mem_buffer_cache_contract() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        for number in 0..512u64 {
            let key = number.to_le_bytes();
            buffer.set_value(&key, &key).unwrap();
            assert_eq!(buffer.get_value(&key).unwrap(), key);
            for _ in 0..10 {
                assert_eq!(buffer.get_value(&key).unwrap(), key);
            }
        }
        assert!(buffer.cache_hits() >= 5_120);
    }
    check_both_buffers!(check);
}

#[test]
fn source_benchmark_mem_buffer_set_get_long_key_contract() {
    fn check<B: SourceBuffer>(buffer: &mut B) {
        let entries: Vec<_> = (0..128u64)
            .map(|number| {
                let mut key = vec![0; 1_024];
                key[..8].copy_from_slice(&number.to_be_bytes());
                key.reverse();
                (key.clone(), key)
            })
            .collect();
        check_set_get_contract(buffer, &entries);
    }
    check_both_buffers!(check);
}
