//! Go `allocator.go`: Lightning never allocates ids, it only records the
//! largest one it wrote so the import can rebase the table afterwards.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// Go `autoid.AllocatorType`, for the three allocators Lightning keeps.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum AllocatorType {
    /// Go `RowIDAllocType`.
    RowId,
    /// Go `AutoIncrementType`.
    AutoIncrement,
    /// Go `AutoRandomType`.
    AutoRandom,
}

/// Go `panickingAllocator`: answers `Rebase` and `Base` only.
#[derive(Debug, Default)]
pub struct PanickingAllocator {
    base: AtomicI64,
}

impl PanickingAllocator {
    /// Go `panickingAllocator.Rebase`: raises the base, never lowers it.
    pub fn rebase(&self, new_base: i64) {
        self.base.fetch_max(new_base, Ordering::SeqCst);
    }

    /// Go `panickingAllocator.Base`.
    pub fn base(&self) -> i64 {
        self.base.load(Ordering::SeqCst)
    }
}

/// Go `autoid.Allocators` built by `NewPanickingAllocatorsWithBase`.
#[derive(Clone, Debug)]
pub struct PanickingAllocators {
    sep_auto_inc: bool,
    auto_random: Arc<PanickingAllocator>,
    auto_increment: Arc<PanickingAllocator>,
    row_id: Arc<PanickingAllocator>,
}

impl PanickingAllocators {
    /// Go `NewPanickingAllocators`.
    #[must_use]
    pub fn new(sep_auto_inc: bool) -> Self {
        Self::with_base(sep_auto_inc, 0, 0, 0)
    }

    /// Go `NewPanickingAllocatorsWithBase`.
    #[must_use]
    pub fn with_base(sep_auto_inc: bool, auto_rand_base: i64, auto_incr_base: i64, auto_row_id_base: i64) -> Self {
        let alloc = |base| {
            let a = PanickingAllocator::default();
            a.rebase(base);
            Arc::new(a)
        };
        Self {
            sep_auto_inc,
            auto_random: alloc(auto_rand_base),
            auto_increment: alloc(auto_incr_base),
            row_id: alloc(auto_row_id_base),
        }
    }

    /// Go `Allocators.Get`: without `SepAutoInc` the auto-increment column
    /// shares the row-id allocator.
    #[must_use]
    pub fn get(&self, ty: AllocatorType) -> &PanickingAllocator {
        match ty {
            AllocatorType::AutoRandom => &self.auto_random,
            AllocatorType::AutoIncrement if self.sep_auto_inc => &self.auto_increment,
            AllocatorType::AutoIncrement | AllocatorType::RowId => &self.row_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Go `TestAllocator`.
    #[test]
    fn test_allocator() {
        let alloc = PanickingAllocators::new(true);
        alloc.get(AllocatorType::RowId).rebase(123);
        assert_eq!(alloc.get(AllocatorType::RowId).base(), 123);
        alloc.get(AllocatorType::RowId).rebase(100);
        assert_eq!(alloc.get(AllocatorType::RowId).base(), 123);
        alloc.get(AllocatorType::AutoIncrement).rebase(456);
        assert_eq!(alloc.get(AllocatorType::AutoIncrement).base(), 456);
        alloc.get(AllocatorType::AutoIncrement).rebase(100);
        assert_eq!(alloc.get(AllocatorType::AutoIncrement).base(), 456);
        alloc.get(AllocatorType::AutoRandom).rebase(789);
        assert_eq!(alloc.get(AllocatorType::AutoRandom).base(), 789);
        alloc.get(AllocatorType::AutoRandom).rebase(100);
        assert_eq!(alloc.get(AllocatorType::AutoRandom).base(), 789);
    }
}
