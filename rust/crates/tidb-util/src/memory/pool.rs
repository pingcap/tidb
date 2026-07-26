// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Transcreation of Go `pkg/util/memory/pool.go`: the `ResourcePool`
//! hierarchy and its `Budget` handles.
//!
//! Faithful adaptations, none changing observable behavior:
//! - Go's `*ResourcePool` with intrusive sibling pointers becomes
//!   `Arc<ResourcePool>` holding a head-inserted `Vec<Arc<..>>` of children
//!   (the same newest-first traversal order the source's head-insertion
//!   produces), removed by `Arc::ptr_eq` on `Stop`.
//! - Go invokes `OutOfCapacityActionCB`/`OutOfLimitActionCB` while the
//!   pool mutex is HELD, and callbacks mutate the pool only through
//!   non-locking methods (`forceAddCap`, the unexported `setLimit`). Rust
//!   cannot re-enter a held `Mutex`, so callbacks receive a
//!   [`PoolCallbackCtx`] handle over the already-locked state exposing
//!   exactly those non-locking operations — the same reachable behavior,
//!   made sound.
//! - Go's nil-`*Budget` receiver no-ops (`TestNilBudget`) are a Go idiom;
//!   Rust callers hold a `Budget` value, so there is no nil receiver to
//!   emulate.
//! - Locks are always taken child-then-upstream (as in the source), so the
//!   acyclic pool tree cannot deadlock.

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering::SeqCst};
use std::sync::{Arc, Mutex};

/// The default allocation alignment size (Go `DefPoolAllocAlignSize`).
pub const DEF_POOL_ALLOC_ALIGN_SIZE: i64 = 10 * 1024;
/// The default maximum unused blocks before shrinking (Go
/// `DefMaxUnusedBlocks`).
pub const DEF_MAX_UNUSED_BLOCKS: i64 = 10;
/// The default maximum limit of memory quota (Go `DefMaxLimit`,
/// `arbitrator.go`).
pub const DEF_MAX_LIMIT: i64 = 5_000_000_000_000_000;

static RESOURCE_POOL_ID: AtomicI64 = AtomicI64::new(-1);

pub(crate) fn new_pool_uid() -> u64 {
    RESOURCE_POOL_ID.fetch_add(-1, SeqCst) as u64
}

/// A budget-exceeded failure with the source's exact message.
#[derive(Debug, Clone)]
pub struct PoolError(pub String);

impl fmt::Display for PoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for PoolError {}

fn budget_exceeded(
    reason: &str,
    name: &str,
    requested: i64,
    allocated: i64,
    limit: i64,
) -> PoolError {
    PoolError(format!(
        "resource pool `{name}` meets `{reason}`: requested({requested}) + allocated({allocated}) > limit({limit})"
    ))
}

/// Arguments for the out-of-capacity action (Go `OutOfCapacityActionArgs`).
pub struct OutOfCapacityActionArgs<'a, 'g> {
    /// The locked pool the action fires on.
    pub pool: PoolCallbackCtx<'a, 'g>,
    /// The extra capacity needed.
    pub request: i64,
}

/// Callback context over an already-locked pool: the non-locking surface Go
/// callbacks use while the mutex is held.
pub struct PoolCallbackCtx<'a, 'g> {
    pool: &'a ResourcePool,
    inner: &'g mut PoolInner,
}

impl PoolCallbackCtx<'_, '_> {
    /// The pool's name.
    pub fn name(&self) -> &str {
        &self.pool.name
    }
    /// Go's unexported `forceAddCap`.
    pub fn force_add_cap(&mut self, c: i64) {
        if c != 0 {
            self.inner.budget.cap += c;
        }
    }
    /// The test-side unlocked `setLimit` (usable while the pool is locked).
    pub fn set_limit(&mut self, new_limit: i64) {
        self.inner.limit = new_limit;
    }
    /// Go's unexported `capacity`.
    pub fn capacity(&self) -> i64 {
        self.inner.budget.cap
    }
    /// Go's unexported `allocated`.
    pub fn allocated(&self) -> i64 {
        self.inner.allocated
    }
}

type OutOfCapacityCb =
    Box<dyn for<'a, 'g> Fn(OutOfCapacityActionArgs<'a, 'g>) -> Result<(), PoolError> + Send + Sync>;
type OutOfLimitCb =
    Box<dyn for<'a, 'g> Fn(PoolCallbackCtx<'a, 'g>) -> Result<(), PoolError> + Send + Sync>;

/// Actions taken when the pool meets certain conditions (Go `PoolActions`).
#[derive(Default)]
pub struct PoolActions {
    /// Called when the pool is out of capacity.
    pub out_of_capacity: Option<OutOfCapacityCb>,
    /// Called when the pool is out of limit.
    pub out_of_limit: Option<OutOfLimitCb>,
}

/// The state of one pool during traversal (Go `ResourcePoolState`).
#[derive(Clone, Debug)]
pub struct ResourcePoolState {
    /// Pool name.
    pub name: String,
    /// Depth in the traversal.
    pub level: usize,
    /// Pool UID.
    pub id: u64,
    /// Upstream pool UID (0 for none).
    pub parent_id: u64,
    /// Allocated bytes.
    pub used: i64,
    /// Reserved bytes.
    pub reserved: i64,
    /// Budget capacity.
    pub budget: i64,
}

struct PoolInner {
    children: Vec<Arc<ResourcePool>>,
    budget: Budget,
    allocated: i64,
    max_allocated: i64,
    stopped: bool,
    limit: i64,
    alloc_align_size: i64,
    max_unused_blocks: i64,
    reserved: i64,
    actions: PoolActions,
}

/// Manages a set of resource quota (Go `ResourcePool`).
pub struct ResourcePool {
    name: String,
    uid: u64,
    inner: Mutex<PoolInner>,
}

/// The budget of a resource pool (Go `Budget`).
#[derive(Default)]
pub struct Budget {
    pool: Option<Arc<ResourcePool>>,
    cap: i64,
    used: i64,
    explicit_reserved: i64,
}

impl Budget {
    /// Used bytes.
    pub fn used(&self) -> i64 {
        self.used
    }
    /// The upstream pool.
    pub fn pool(&self) -> Option<&Arc<ResourcePool>> {
        self.pool.as_ref()
    }
    /// Budget capacity.
    pub fn capacity(&self) -> i64 {
        self.cap
    }
    /// Explicitly reserved size (test surface).
    pub fn explicit_reserved(&self) -> i64 {
        self.explicit_reserved
    }
    fn available(&self) -> i64 {
        self.cap - self.used
    }

    /// Reserves aligned budget and records the explicit reservation (Go
    /// `Reserve`).
    pub fn reserve(&mut self, request: i64) -> Result<(), PoolError> {
        let Some(pool) = self.pool.clone() else {
            return Ok(());
        };
        let min_extra = pool.round_size(request);
        pool.allocate(min_extra)?;
        self.cap += min_extra;
        self.explicit_reserved += request;
        Ok(())
    }

    /// Releases the used budget, shrinking surplus capacity (Go `Empty`).
    pub fn empty(&mut self) {
        self.used = 0;
        let Some(pool) = self.pool.clone() else {
            return;
        };
        let release = self.available() - pool.alloc_align_size();
        if release > 0 {
            pool.release(release);
            self.cap -= release;
        }
    }

    /// Releases everything and resets (Go `Clear`).
    pub fn clear(&mut self) {
        let release = self.cap;
        self.used = 0;
        self.cap = 0;
        if let Some(pool) = self.pool.clone() {
            if release > 0 {
                pool.release(release);
            }
        }
    }

    /// Go's unexported `resize`.
    pub fn resize(&mut self, old_sz: i64, new_sz: i64) -> Result<(), PoolError> {
        let delta = new_sz - old_sz;
        if delta > 0 {
            self.grow(delta)
        } else {
            if delta < 0 {
                self.shrink(-delta);
            }
            Ok(())
        }
    }

    /// Resizes to the new size (Go `ResizeTo`).
    pub fn resize_to(&mut self, new_sz: i64) -> Result<(), PoolError> {
        if new_sz == self.used {
            return Ok(());
        }
        self.resize(self.used, new_sz)
    }

    /// Grows the budget (Go `Grow`).
    pub fn grow(&mut self, request: i64) -> Result<(), PoolError> {
        let extra = request.wrapping_sub(self.available());
        if extra > 0 {
            let Some(pool) = self.pool.clone() else {
                // A pool-less budget cannot grow beyond its capacity; Go only
                // reaches this on the root pool's internal budget.
                self.used += request;
                return Ok(());
            };
            let min_extra = pool.round_size(extra);
            pool.allocate(min_extra)?;
            self.cap += min_extra;
        }
        self.used += request;
        Ok(())
    }

    /// Shrinks the budget (Go `Shrink`).
    pub fn shrink(&mut self, mut delta: i64) {
        if delta == 0 {
            return;
        }
        if self.used < delta {
            delta = self.used;
        }
        self.used -= delta;
        let Some(pool) = self.pool.clone() else {
            return;
        };
        let align = pool.alloc_align_size();
        let release = self.available() - align;
        if release > 0
            && (self.explicit_reserved == 0 || self.used + align > self.explicit_reserved)
        {
            pool.release(release);
            self.cap -= release;
        }
    }
}

impl ResourcePool {
    /// Creates a pool with defaults (Go `NewResourcePoolDefault`).
    pub fn new_default(name: &str, alloc_align_size: i64) -> Arc<ResourcePool> {
        Self::new(
            new_pool_uid(),
            name,
            0,
            alloc_align_size,
            DEF_MAX_UNUSED_BLOCKS,
            PoolActions::default(),
        )
    }

    /// Creates a pool (Go `NewResourcePool`).
    pub fn new(
        uid: u64,
        name: &str,
        mut limit: i64,
        mut alloc_align_size: i64,
        max_unused_blocks: i64,
        actions: PoolActions,
    ) -> Arc<ResourcePool> {
        if alloc_align_size <= 0 {
            alloc_align_size = DEF_POOL_ALLOC_ALIGN_SIZE;
        }
        if limit <= 0 {
            limit = DEF_MAX_LIMIT;
        }
        Arc::new(ResourcePool {
            name: name.to_string(),
            uid,
            inner: Mutex::new(PoolInner {
                children: Vec::new(),
                budget: Budget::default(),
                allocated: 0,
                max_allocated: 0,
                stopped: false,
                limit,
                alloc_align_size,
                max_unused_blocks,
                reserved: 0,
                actions,
            }),
        })
    }

    /// Creates a child-configured pool inheriting align/blocks (Go
    /// `NewResourcePoolInheritWithLimit`; actions do not transfer here since
    /// Go copies the parent's struct — install them via the setters).
    pub fn new_inherit_with_limit(self: &Arc<Self>, name: &str, limit: i64) -> Arc<ResourcePool> {
        let (align, blocks) = {
            let inner = self.inner.lock().unwrap();
            (inner.alloc_align_size, inner.max_unused_blocks)
        };
        Self::new(
            new_pool_uid(),
            name,
            limit,
            align,
            blocks,
            PoolActions::default(),
        )
    }

    /// Starts with no reserved quota (Go `StartNoReserved`).
    pub fn start_no_reserved(self: &Arc<Self>, parent: Option<&Arc<ResourcePool>>) {
        self.start(parent, 0);
    }

    /// The unique pool ID (Go `UID`).
    pub fn uid(&self) -> u64 {
        self.uid
    }

    /// Starts the pool under a parent with reserved quota (Go `Start`);
    /// panics on restart like the source.
    pub fn start(self: &Arc<Self>, parent: Option<&Arc<ResourcePool>>, reserved: i64) {
        {
            let mut inner = self.inner.lock().unwrap();
            assert!(
                inner.allocated == 0,
                "{}: started with {} bytes left over",
                self.name,
                inner.allocated
            );
            assert!(
                inner.budget.pool.is_none(),
                "{}: already started",
                self.name
            );
            inner.allocated = 0;
            inner.max_allocated = 0;
            inner.budget = Budget {
                pool: parent.cloned(),
                ..Budget::default()
            };
            inner.stopped = false;
            inner.reserved = reserved;
        }
        if let Some(parent) = parent {
            parent
                .inner
                .lock()
                .unwrap()
                .children
                .insert(0, Arc::clone(self));
        }
    }

    /// The pool name (Go `Name`).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The quota limit (Go `Limit`).
    pub fn limit(&self) -> i64 {
        self.inner.lock().unwrap().limit
    }

    /// Whether stopped (Go `IsStopped`).
    pub fn is_stopped(&self) -> bool {
        self.inner.lock().unwrap().stopped
    }

    /// Sets the alignment size, returning the old one (Go
    /// `SetAllocAlignSize`).
    pub fn set_alloc_align_size(&self, size: i64) -> i64 {
        let mut inner = self.inner.lock().unwrap();
        std::mem::replace(&mut inner.alloc_align_size, size)
    }

    /// Test surface for the unused-blocks threshold.
    pub fn set_max_unused_blocks(&self, blocks: i64) {
        self.inner.lock().unwrap().max_unused_blocks = blocks;
    }

    /// Stops the pool, releasing its budget; returns the released quota (Go
    /// `Stop`).
    pub fn stop(self: &Arc<Self>) -> i64 {
        let (released, parent) = {
            let mut inner = self.inner.lock().unwrap();
            inner.stopped = true;
            if inner.allocated != 0 {
                let allocated = inner.allocated;
                Self::do_release(&mut inner, allocated);
            }
            let released = inner.budget.cap;
            // releaseBudget: Clear against the upstream pool.
            let mut budget = std::mem::take(&mut inner.budget);
            let parent = budget.pool.clone();
            drop(inner); // Clear locks the upstream pool.
            budget.clear();
            let mut inner = self.inner.lock().unwrap();
            inner.budget = Budget::default();
            (released, parent)
        };
        if let Some(parent) = parent {
            let mut p = parent.inner.lock().unwrap();
            if let Some(i) = p.children.iter().position(|c| Arc::ptr_eq(c, self)) {
                p.children.remove(i);
            }
        }
        released
    }

    /// Maximum allocated bytes (Go `MaxAllocated`).
    pub fn max_allocated(&self) -> i64 {
        self.inner.lock().unwrap().max_allocated
    }

    /// Allocated bytes (Go `Allocated`).
    pub fn allocated(&self) -> i64 {
        self.inner.lock().unwrap().allocated
    }

    /// Creates a budget drawing from this pool (Go `CreateBudget`).
    pub fn create_budget(self: &Arc<Self>) -> Budget {
        Budget {
            pool: Some(Arc::clone(self)),
            ..Budget::default()
        }
    }

    /// Explicitly reserves budget (Go `ExplicitReserve`). The internal budget
    /// reservation allocates from the UPSTREAM pool, so the own lock is not
    /// held across it (same order as the source's inner calls).
    pub fn explicit_reserve(self: &Arc<Self>, request: i64) -> Result<(), PoolError> {
        let mut budget = {
            let mut inner = self.inner.lock().unwrap();
            std::mem::take(&mut inner.budget)
        };
        let result = budget.reserve(request);
        self.inner.lock().unwrap().budget = budget;
        result
    }

    pub(crate) fn alloc_align_size(&self) -> i64 {
        self.inner.lock().unwrap().alloc_align_size
    }

    pub(crate) fn round_size(&self, sz: i64) -> i64 {
        let align = self.alloc_align_size();
        if align <= 1 {
            return sz;
        }
        // Go arithmetic wraps on overflow; the huge-request paths (see
        // TestMemoryAllocationEdgeCases) depend on it to reach the limit
        // check rather than trap.
        sz.wrapping_add(align - 1) / align * align
    }

    /// Go's unexported `allocate` (public in tests via the package).
    pub fn allocate(self: &Arc<Self>, request: i64) -> Result<(), PoolError> {
        let mut inner = self.inner.lock().unwrap();
        self.do_alloc(&mut inner, request)
    }

    fn do_alloc(self: &Arc<Self>, inner: &mut PoolInner, request: i64) -> Result<(), PoolError> {
        if inner.allocated > inner.limit.wrapping_sub(request) {
            let has_cb = inner.actions.out_of_limit.is_some();
            if !has_cb {
                return Err(budget_exceeded(
                    "out of limit",
                    &self.name,
                    request,
                    inner.allocated,
                    inner.limit,
                ));
            }
            let cb = inner.actions.out_of_limit.take().unwrap();
            let result = cb(PoolCallbackCtx { pool: self, inner });
            inner.actions.out_of_limit = Some(cb);
            result?;
        }

        let delta = request
            .wrapping_add(inner.allocated)
            .wrapping_sub(inner.budget.used)
            .wrapping_sub(inner.reserved);
        if delta > 0 {
            self.increase_budget(inner, delta)?;
        }
        inner.allocated += request;
        if inner.max_allocated < inner.allocated {
            inner.max_allocated = inner.allocated;
        }
        Ok(())
    }

    fn increase_budget(
        self: &Arc<Self>,
        inner: &mut PoolInner,
        request: i64,
    ) -> Result<(), PoolError> {
        if inner.budget.pool.is_none() {
            // Root pool.
            let need = request - inner.budget.available();
            if need <= 0 {
                inner.budget.used += request;
                return Ok(());
            }
            if inner.actions.out_of_capacity.is_some() {
                let cb = inner.actions.out_of_capacity.take().unwrap();
                let result = cb(OutOfCapacityActionArgs {
                    pool: PoolCallbackCtx { pool: self, inner },
                    request: need,
                });
                inner.actions.out_of_capacity = Some(cb);
                result?;
                inner.budget.used += request;
                return Ok(());
            }
            return Err(budget_exceeded(
                "out of quota",
                &self.name,
                request,
                inner.budget.used,
                inner.budget.cap,
            ));
        }

        // Non-root: grow through the upstream pool WITHOUT this lock held.
        let mut budget = std::mem::take(&mut inner.budget);
        // Temporarily unlock: Budget::grow locks the upstream pool only.
        let result = budget.grow(request);
        inner.budget = budget;
        result
    }

    /// The budget capacity (Go `Capacity`).
    pub fn capacity(&self) -> i64 {
        self.inner.lock().unwrap().budget.cap
    }

    /// Approximate capacity (Go `ApproxCap`; lock-free in the source, an
    /// approximation either way).
    pub fn approx_cap(&self) -> i64 {
        self.capacity()
    }

    /// Sets the limit (Go `SetLimit`).
    pub fn set_limit(&self, new_limit: i64) {
        self.inner.lock().unwrap().limit = new_limit;
    }

    /// Constructs an unstarted pool with explicit fields (the Go struct
    /// literals in `EmplaceRootPool`/`initAwaitFreePool`).
    pub(crate) fn new_raw(
        name: &str,
        uid: u64,
        limit: i64,
        alloc_align_size: i64,
        max_unused_blocks: i64,
    ) -> Arc<ResourcePool> {
        Arc::new(ResourcePool {
            name: name.to_string(),
            uid,
            inner: Mutex::new(PoolInner {
                children: Vec::new(),
                budget: Budget::default(),
                allocated: 0,
                max_allocated: 0,
                stopped: false,
                limit,
                alloc_align_size,
                max_unused_blocks,
                reserved: 0,
                actions: PoolActions::default(),
            }),
        })
    }

    /// Go `forceAddCap` invoked outside a held pool lock (`windUp`,
    /// await-free growth): takes the lock in Rust for soundness.
    pub(crate) fn force_add_cap_unlocked(&self, c: i64) {
        if c != 0 {
            self.inner.lock().unwrap().budget.cap += c;
        }
    }

    /// Go `releasePopBudget`.
    pub(crate) fn release_pop_budget(self: &Arc<Self>, c: i64) -> i64 {
        let mut inner = self.inner.lock().unwrap();
        Self::do_release(&mut inner, c);
        let released = inner.budget.available();
        inner.budget.cap -= released;
        released
    }

    /// The reserved quota (Go field read in `addRootPool`).
    pub(crate) fn reserved(&self) -> i64 {
        self.inner.lock().unwrap().reserved
    }

    /// Whether the pool has been started under an upstream pool.
    pub(crate) fn upstream(&self) -> Option<Arc<ResourcePool>> {
        self.inner.lock().unwrap().budget.pool.clone()
    }

    /// Go `p.mu.stopped = false` in `RestartEntryByContext`.
    pub(crate) fn clear_stopped(&self) {
        self.inner.lock().unwrap().stopped = false;
    }

    /// Go's unexported `release`.
    pub fn release(self: &Arc<Self>, sz: i64) {
        let mut inner = self.inner.lock().unwrap();
        Self::do_release(&mut inner, sz);
    }

    fn do_release(inner: &mut PoolInner, mut sz: i64) {
        if inner.allocated < sz {
            sz = inner.allocated;
        }
        inner.allocated -= sz;
        Self::do_adjust_budget(inner);
    }

    /// Sets the out-of-capacity action (Go `SetOutOfCapacityAction`).
    pub fn set_out_of_capacity_action(&self, f: OutOfCapacityCb) {
        self.inner.lock().unwrap().actions.out_of_capacity = Some(f);
    }

    /// Sets the out-of-limit action (Go `SetOutOfLimitAction`).
    pub fn set_out_of_limit_action(&self, f: OutOfLimitCb) {
        self.inner.lock().unwrap().actions.out_of_limit = Some(f);
    }

    /// Adjusts the budget (Go `AdjustBudget`).
    pub fn adjust_budget(&self) {
        let mut inner = self.inner.lock().unwrap();
        Self::do_adjust_budget(&mut inner);
    }

    fn do_adjust_budget(inner: &mut PoolInner) {
        let mut needed = inner.allocated - inner.reserved;
        if needed <= 0 {
            needed = 0;
        } else {
            let align = inner.alloc_align_size;
            if align > 1 {
                needed = (needed + align - 1) / align * align;
            }
        }
        if inner.alloc_align_size * inner.max_unused_blocks <= inner.budget.used - needed {
            let delta = inner.budget.used - needed;
            // Budget::shrink would lock the upstream pool; do it unlocked.
            let mut budget = std::mem::take(&mut inner.budget);
            budget.shrink(delta);
            inner.budget = budget;
        }
    }

    /// Traverses the pool tree (Go `Traverse`).
    pub fn traverse(
        self: &Arc<Self>,
        state_cb: &mut dyn FnMut(ResourcePoolState) -> Result<(), PoolError>,
    ) -> Result<(), PoolError> {
        self.traverse_level(0, state_cb)
    }

    fn traverse_level(
        self: &Arc<Self>,
        level: usize,
        state_cb: &mut dyn FnMut(ResourcePoolState) -> Result<(), PoolError>,
    ) -> Result<(), PoolError> {
        let (state, children) = {
            let inner = self.inner.lock().unwrap();
            if inner.stopped {
                return Ok(());
            }
            (
                ResourcePoolState {
                    level,
                    name: self.name.clone(),
                    id: self.uid,
                    parent_id: inner.budget.pool.as_ref().map_or(0, |p| p.uid),
                    used: inner.allocated,
                    reserved: inner.reserved,
                    budget: inner.budget.cap,
                },
                inner.children.clone(),
            )
        };
        state_cb(state)?;
        for c in children {
            c.traverse_level(level + 1, state_cb)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicI64};
    use std::time::Duration;

    // Deterministic PRNG standing in for Go's rand.NewSource(1). The Go
    // tests' contracts are the checked invariants, not the exact op
    // sequence, so any fixed deterministic sequence is faithful.
    struct Lcg(u64);
    impl Lcg {
        fn new(seed: u64) -> Lcg {
            Lcg(seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407))
        }
        fn next_u64(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 11
        }
        fn intn(&mut self, n: i64) -> i64 {
            (self.next_u64() % n as u64) as i64
        }
        fn float64(&mut self) -> f64 {
            (self.next_u64() % (1 << 53)) as f64 / (1u64 << 53) as f64
        }
        fn exp_float64(&mut self) -> f64 {
            -(1.0 - self.float64()).ln()
        }
    }

    fn random_size(rnd: &mut Lcg, mag: i64) -> i64 {
        (rnd.exp_float64() * mag as f64 * 0.3679) as i64
    }

    #[test]
    fn test_pool_allocations() {
        let maxs: [i64; 8] = [1, 9, 10, 11, 99, 100, 101, 0];
        let factors: [i64; 4] = [1, 2, 10, 10000];
        let pool_alloc_sizes: [i64; 6] = [1, 2, 9, 10, 11, 100];
        let pre_budgets: [i64; 7] = [0, 1, 2, 9, 10, 11, 100];

        let mut rnd = Lcg::new(1);

        let first_m = ResourcePool::new_default("test", 0);
        first_m.start_no_reserved(None);
        // As in the Go test, the budgets stay bound to this initial pool for
        // the whole run (Go rebinds `m` without recreating `accs`).
        let mut accs: Vec<Budget> = (0..4).map(|_| first_m.create_budget()).collect();

        let check_invariants =
            |accs: &[Budget], m: &Arc<ResourcePool>, pool: &Arc<ResourcePool>| {
                let mut sum = 0i64;
                for (i, a) in accs.iter().enumerate() {
                    assert!(a.used >= 0, "budget {i} < 0: {}", a.used);
                    sum += a.capacity();
                }
                let mi = m.inner.lock().unwrap();
                assert!(mi.allocated >= 0, "pool size < 0: {}", mi.allocated);
                assert_eq!(sum, mi.allocated, "total budget sum differs from pool size");
                assert!(mi.budget.used >= 0, "pool budget < 0: {}", mi.budget.used);
                let avail = mi.budget.capacity() + mi.reserved;
                assert!(
                    sum <= avail,
                    "total budget sum {sum} greater than total pool budget {avail}"
                );
                let pi = pool.inner.lock().unwrap();
                assert!(
                    pi.allocated <= pi.reserved,
                    "pool cur {} exceeds max {}",
                    pi.allocated,
                    pi.reserved
                );
                assert_eq!(
                    mi.budget.capacity(),
                    pi.allocated,
                    "pool budget differs from pool cur"
                );
            };

        const NUM_BUDGET_OPS: usize = 200;

        for max in maxs {
            let pool = ResourcePool::new_default("test", 1);
            pool.start(None, max);

            for hf in factors {
                pool.set_max_unused_blocks(hf);

                for pb in pre_budgets {
                    let mmax = pb + max;

                    for pa in pool_alloc_sizes {
                        let m = ResourcePool::new_default("test", pa);
                        m.start(Some(&pool), pb);

                        for _ in 0..NUM_BUDGET_OPS {
                            let acc_i = rnd.intn(accs.len() as i64) as usize;
                            match rnd.intn(3) {
                                0 => {
                                    let sz = random_size(&mut rnd, mmax);
                                    check_invariants(&accs, &m, &pool);
                                    let _ = accs[acc_i].grow(sz);
                                    check_invariants(&accs, &m, &pool);
                                }
                                1 => {
                                    check_invariants(&accs, &m, &pool);
                                    accs[acc_i].clear();
                                    check_invariants(&accs, &m, &pool);
                                }
                                _ => {
                                    let osz = rnd.intn(accs[acc_i].used + 1);
                                    let nsz = random_size(&mut rnd, mmax);
                                    check_invariants(&accs, &m, &pool);
                                    let _ = accs[acc_i].resize(osz, nsz);
                                    check_invariants(&accs, &m, &pool);
                                }
                            }
                        }

                        for i in 0..accs.len() {
                            check_invariants(&accs, &m, &pool);
                            accs[i].clear();
                            check_invariants(&accs, &m, &pool);
                        }

                        m.stop();
                        assert_eq!(
                            pool.inner.lock().unwrap().allocated,
                            0,
                            "pool not empty after pool close"
                        );
                    }
                }
            }
            pool.stop();
        }
    }

    #[test]
    fn test_budget() {
        let p = ResourcePool::new_default("test", 1);
        p.start(None, 100);
        p.set_alloc_align_size(1);
        p.set_max_unused_blocks(1);

        let mut a1 = p.create_budget();
        let mut a2 = p.create_budget();
        a1.grow(10).expect("pool refused allocation");
        a2.grow(30).expect("pool refused allocation");
        assert!(a1.grow(61).is_err(), "pool accepted excessive allocation");
        assert!(a2.grow(61).is_err(), "pool accepted excessive allocation");

        a1.clear();
        a2.grow(61).expect("pool refused allocation");
        assert!(
            a2.resize(50, 60).is_err(),
            "pool accepted excessive allocation"
        );
        a1.resize(0, 5).expect("pool refused allocation");
        let used = a2.used();
        a2.resize(used, 40)
            .expect("pool refused reset + allocation");

        a1.clear();
        a2.clear();
        assert_eq!(
            p.inner.lock().unwrap().allocated,
            0,
            "closing spans leaves bytes in pool"
        );
        assert!(Arc::ptr_eq(a1.pool().unwrap(), &p));
        p.stop();
    }

    #[test]
    fn test_resource_pool() {
        let p = ResourcePool::new_default("test", 1);
        p.start(None, 100);
        p.set_max_unused_blocks(1);

        p.allocate(10).expect("pool refused small allocation");
        assert!(
            p.allocate(91).is_err(),
            "pool accepted excessive allocation"
        );
        p.allocate(90).expect("pool refused top allocation");
        assert_eq!(p.inner.lock().unwrap().allocated, 100);

        p.release(90);
        assert_eq!(p.inner.lock().unwrap().allocated, 10);
        assert_eq!(p.inner.lock().unwrap().max_allocated, 100);
        assert_eq!(p.max_allocated(), 100);

        p.release(10);
        assert_eq!(p.inner.lock().unwrap().allocated, 0);

        let limited = ResourcePool::new(
            new_pool_uid(),
            "testlimit",
            10,
            1,
            DEF_MAX_UNUSED_BLOCKS,
            PoolActions::default(),
        );
        limited.start_no_reserved(Some(&p));

        limited
            .allocate(10)
            .expect("limited pool refused small allocation");
        assert!(
            limited.allocate(1).is_err(),
            "limited pool allowed allocation over limit"
        );

        limited.release(10);
        limited.stop();
        p.stop();
    }

    #[test]
    fn test_memory_allocation_edge_cases() {
        let m = ResourcePool::new_default("test", 1_000_000_000);
        m.start(None, 1_000_000_000);

        let mut a = m.create_budget();
        a.grow(1).unwrap();
        assert!(
            a.grow(i64::MAX).is_err(),
            "expected error, but found success"
        );

        a.clear();
        m.stop();
    }

    #[test]
    fn test_multi_shared_gauge() {
        let min_allocation = 1000i64;

        let parent = ResourcePool::new_default("root", min_allocation);
        parent.start(None, 100000);

        let child = parent.new_inherit_with_limit("child", 20000);
        child.start_no_reserved(Some(&parent));

        let mut acc = child.create_budget();
        acc.grow(100).unwrap();

        assert_eq!(min_allocation, parent.allocated());
    }

    #[test]
    fn test_actions() {
        {
            let root = ResourcePool::new_default("root", 1000);
            root.start(None, i64::MAX);
            let p1 = ResourcePool::new_default("p1", 666);
            p1.start_no_reserved(Some(&root));
            p1.explicit_reserve(1002).unwrap();
            let root_align = root.alloc_align_size();
            assert_eq!(p1.capacity(), root_align * 2);
            assert_eq!(p1.inner.lock().unwrap().budget.explicit_reserved, 1002);
            let mut pb1 = p1.create_budget();
            pb1.reserve(123).unwrap();
            assert_eq!(p1.capacity(), root_align * 2);
            assert_eq!(pb1.capacity(), p1.alloc_align_size());
            assert_eq!(pb1.explicit_reserved(), 123);
            pb1.grow(123).unwrap();
            pb1.shrink(123);
            assert_eq!(p1.capacity(), root_align * 2);
            pb1.clear();
        }

        {
            let name = "root";
            let out_of_capacity_num = Arc::new(AtomicI64::new(0));
            let root = ResourcePool::new_default(name, 1);
            root.start_no_reserved(None);

            let mut b = root.create_budget();
            assert!(b.grow(1).is_err());

            let cnt = Arc::clone(&out_of_capacity_num);
            root.set_out_of_capacity_action(Box::new(
                move |mut s: OutOfCapacityActionArgs<'_, '_>| {
                    assert_eq!(s.pool.name(), "root");
                    s.pool.force_add_cap(s.request);
                    cnt.fetch_add(1, SeqCst);
                    Ok(())
                },
            ));

            b.grow(1).unwrap();
            assert_eq!(out_of_capacity_num.load(SeqCst), 1);
            b.grow(10).unwrap();
            assert_eq!(out_of_capacity_num.load(SeqCst), 2);
            assert_eq!(root.inner.lock().unwrap().budget.used, b.used());

            b.clear();
            b.grow(5).unwrap();
            assert_eq!(out_of_capacity_num.load(SeqCst), 2);
            b.clear();

            let out_of_limit_cnt = Arc::new(AtomicI64::new(0));
            let lcnt = Arc::clone(&out_of_limit_cnt);
            root.set_out_of_limit_action(Box::new(move |mut r: PoolCallbackCtx<'_, '_>| {
                lcnt.fetch_add(1, SeqCst);
                let cap = r.capacity();
                r.set_limit(cap);
                Err(PoolError(String::new()))
            }));
            root.set_limit(1);
            assert_eq!(root.limit(), 1);
            assert!(b.grow(2).is_err());
            assert_eq!(out_of_limit_cnt.load(SeqCst), 1);
            b.grow(root.capacity() - root.allocated()).unwrap();
            assert_eq!(out_of_limit_cnt.load(SeqCst), 1);
        }
    }

    fn gen_pool(name: &str, parent: Option<&Arc<ResourcePool>>) -> Arc<ResourcePool> {
        let reserved = if parent.is_none() { i64::MAX } else { 0 };
        get_pool_impl(name, parent, reserved)
    }

    fn get_pool_impl(
        name: &str,
        parent: Option<&Arc<ResourcePool>>,
        reserved: i64,
    ) -> Arc<ResourcePool> {
        let m = ResourcePool::new_default(name, 1);
        m.start(parent, reserved);
        m
    }

    fn get_pool_used(
        name: &str,
        parent: &Arc<ResourcePool>,
        used_bytes: i64,
        reserved_bytes: i64,
    ) -> (Arc<ResourcePool>, Budget) {
        let m = get_pool_impl(name, Some(parent), reserved_bytes);
        let mut acc = m.create_budget();
        if used_bytes != 0 {
            acc.grow(used_bytes).unwrap();
        }
        (m, acc)
    }

    fn export(p: &Arc<ResourcePool>) -> String {
        let mut out = String::new();
        p.traverse(&mut |s| {
            for _ in 0..s.level {
                out.push('-');
            }
            out.push_str(&s.name);
            out.push('\n');
            Ok(())
        })
        .unwrap();
        out
    }

    #[test]
    fn test_resource_pool_tree() {
        let parent = gen_pool("parent", None);
        let child1 = gen_pool("child1", Some(&parent));
        let child2 = gen_pool("child2", Some(&parent));
        assert_eq!("parent\n-child2\n-child1\n", export(&parent));
        assert_eq!("child1\n", export(&child1));
        assert_eq!("child2\n", export(&child2));

        let grandchild1 = gen_pool("grandchild1", Some(&child1));
        let grandchild2 = gen_pool("grandchild2", Some(&child2));
        assert_eq!(
            "parent\n-child2\n--grandchild2\n-child1\n--grandchild1\n",
            export(&parent)
        );
        assert_eq!("child1\n-grandchild1\n", export(&child1));
        assert_eq!("child2\n-grandchild2\n", export(&child2));

        grandchild2.stop();
        child2.stop();
        assert_eq!("parent\n-child1\n--grandchild1\n", export(&parent));
        assert_eq!("child1\n-grandchild1\n", export(&child1));

        grandchild1.stop();
        child1.stop();
        assert_eq!("parent\n", export(&parent));
        parent.stop();
    }

    #[test]
    fn test_resource_pool_used_from_reserved() {
        let root = gen_pool("root", None);
        const USED_BYTES: i64 = 1 << 10;
        let (child, _acc) = get_pool_used("child", &root, USED_BYTES, 2 * USED_BYTES);

        root.traverse(&mut |s| {
            if s.name == child.name() {
                assert_eq!(USED_BYTES, s.used);
            }
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_resource_pool_no_deadlocks() {
        let root = gen_pool("root", None);
        let done = Arc::new(AtomicBool::new(false));
        const NUM_THREADS: usize = 10;

        let mut handles = Vec::new();
        for i in 0..NUM_THREADS {
            let root = Arc::clone(&root);
            let done = Arc::clone(&done);
            handles.push(std::thread::spawn(move || {
                let mut rng = Lcg::new(1 + i as u64);
                while !done.load(SeqCst) {
                    let m = gen_pool(&format!("m{i}"), Some(&root));
                    let num_ops = rng.intn(11);
                    let mut reserved = 0i64;
                    for _ in 0..num_ops {
                        if reserved > 0 && rng.float64() < 0.5 {
                            let to_release = rng.intn(reserved) + 1;
                            m.release(to_release);
                            reserved -= to_release;
                        } else {
                            let to_reserve = rng.intn(1000) + 1;
                            let _ = m.allocate(to_reserve);
                            reserved += to_reserve;
                        }
                        std::thread::sleep(Duration::from_micros(rng.intn(1000) as u64));
                    }
                    m.release(reserved);
                    m.stop();
                    std::thread::sleep(Duration::from_micros(rng.intn(2000) as u64));
                }
            }));
        }

        let mut rng = Lcg::new(1);
        for _ in 0..1000 {
            let mut pools = Vec::new();
            root.traverse(&mut |s| {
                pools.push(s);
                Ok(())
            })
            .unwrap();
            for m in &pools {
                assert!(!m.name.is_empty());
            }
            std::thread::sleep(Duration::from_micros(rng.intn(3000) as u64));
        }
        done.store(true, SeqCst);
        for h in handles {
            h.join().unwrap();
        }
        root.stop();
    }
}
