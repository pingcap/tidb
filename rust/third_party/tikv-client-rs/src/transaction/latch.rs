//! Client-side transaction latches.
//!
//! Latches serialize optimistic commits that touch the same keys and reject a
//! waiting transaction when an earlier transaction commits above its start
//! timestamp.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use futures::channel::oneshot;

use crate::oracle::extract_physical;

const EXPIRE_DURATION_MS: i64 = 2 * 60 * 1_000;
const CHECK_INTERVAL_MS: i64 = 60 * 1_000;
const CHECK_COUNTER: usize = 50_000;
const LATCH_LIST_COUNT: usize = 5;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcquireResult {
    Success,
    Locked,
    Stale,
}

struct LockState {
    id: u64,
    keys: Vec<Vec<u8>>,
    required_slots: Vec<usize>,
    acquired_count: usize,
    start_timestamp: u64,
    commit_timestamp: u64,
    is_stale: bool,
    notifier: Option<oneshot::Sender<()>>,
}

type SharedLock = Arc<Mutex<LockState>>;

#[derive(Default)]
struct LatchEntry {
    max_commit_timestamp: u64,
    owner: Option<u64>,
}

#[derive(Default)]
struct LatchSlot {
    entries: HashMap<Vec<u8>, LatchEntry>,
    waiting: Vec<SharedLock>,
}

struct Latches {
    slots: Vec<LatchSlot>,
    next_lock_id: u64,
}

impl Latches {
    fn new(size: usize) -> Self {
        assert!(size > 0, "latch capacity must be greater than zero");
        let size = size
            .checked_next_power_of_two()
            .expect("latch capacity is too large");
        Self {
            slots: (0..size).map(|_| LatchSlot::default()).collect(),
            next_lock_id: 0,
        }
    }

    fn new_lock(&mut self, start_timestamp: u64, mut keys: Vec<Vec<u8>>) -> SharedLock {
        keys.sort();
        self.next_lock_id = self.next_lock_id.wrapping_add(1);
        let required_slots = keys.iter().map(|key| self.slot_id(key)).collect();
        Arc::new(Mutex::new(LockState {
            id: self.next_lock_id,
            keys,
            required_slots,
            acquired_count: 0,
            start_timestamp,
            commit_timestamp: 0,
            is_stale: false,
            notifier: None,
        }))
    }

    fn slot_id(&self, key: &[u8]) -> usize {
        murmur3_sum32(key) as usize & (self.slots.len() - 1)
    }

    fn acquire(&mut self, lock: &SharedLock) -> AcquireResult {
        if lock.lock().unwrap().is_stale {
            return AcquireResult::Stale;
        }
        loop {
            let acquired_count = lock.lock().unwrap().acquired_count;
            if acquired_count == lock.lock().unwrap().required_slots.len() {
                return AcquireResult::Success;
            }
            let result = self.acquire_slot(lock);
            if result != AcquireResult::Success {
                return result;
            }
        }
    }

    fn acquire_slot(&mut self, lock: &SharedLock) -> AcquireResult {
        let (key, slot_id, start_timestamp, lock_id) = {
            let lock = lock.lock().unwrap();
            let index = lock.acquired_count;
            (
                lock.keys[index].clone(),
                lock.required_slots[index],
                lock.start_timestamp,
                lock.id,
            )
        };
        let slot = &mut self.slots[slot_id];
        if slot.entries.len() >= LATCH_LIST_COUNT {
            recycle_slot(slot, start_timestamp);
        }

        match slot.entries.get_mut(&key) {
            None => {
                slot.entries.insert(
                    key,
                    LatchEntry {
                        max_commit_timestamp: 0,
                        owner: Some(lock_id),
                    },
                );
                lock.lock().unwrap().acquired_count += 1;
                AcquireResult::Success
            }
            Some(entry) if entry.max_commit_timestamp > start_timestamp => {
                lock.lock().unwrap().is_stale = true;
                AcquireResult::Stale
            }
            Some(entry) if entry.owner.is_none() => {
                entry.owner = Some(lock_id);
                lock.lock().unwrap().acquired_count += 1;
                AcquireResult::Success
            }
            Some(_) => {
                slot.waiting.push(lock.clone());
                AcquireResult::Locked
            }
        }
    }

    fn release(&mut self, lock: &SharedLock) -> Vec<SharedLock> {
        let mut wakeups = Vec::new();
        while lock.lock().unwrap().acquired_count > 0 {
            if let Some(next) = self.release_slot(lock) {
                wakeups.push(next);
            }
        }
        wakeups
    }

    fn release_slot(&mut self, lock: &SharedLock) -> Option<SharedLock> {
        let (key, slot_id, lock_id, commit_timestamp) = {
            let mut lock = lock.lock().unwrap();
            let index = lock.acquired_count - 1;
            let values = (
                lock.keys[index].clone(),
                lock.required_slots[index],
                lock.id,
                lock.commit_timestamp,
            );
            lock.acquired_count -= 1;
            values
        };

        let slot = &mut self.slots[slot_id];
        let entry = slot
            .entries
            .get_mut(&key)
            .expect("released latch entry must exist");
        assert_eq!(entry.owner, Some(lock_id), "released latch has wrong owner");
        entry.max_commit_timestamp = entry.max_commit_timestamp.max(commit_timestamp);
        entry.owner = None;

        let waiting_index = slot.waiting.iter().position(|waiting| {
            let waiting = waiting.lock().unwrap();
            waiting.keys[waiting.acquired_count] == key
        })?;
        let next = slot.waiting.remove(waiting_index);
        let mut next_state = next.lock().unwrap();
        if entry.max_commit_timestamp > next_state.start_timestamp {
            entry.owner = Some(next_state.id);
            next_state.acquired_count += 1;
            next_state.is_stale = true;
        }
        drop(next_state);
        Some(next)
    }

    fn wakeup(&mut self, wakeups: Vec<SharedLock>) {
        for lock in wakeups {
            if self.acquire(&lock) != AcquireResult::Locked {
                if let Some(notifier) = lock.lock().unwrap().notifier.take() {
                    let _ = notifier.send(());
                }
            }
        }
    }

    fn cancel(&mut self, lock: &SharedLock) {
        let lock_id = lock.lock().unwrap().id;
        for slot in &mut self.slots {
            slot.waiting
                .retain(|waiting| waiting.lock().unwrap().id != lock_id);
        }
        let wakeups = self.release(lock);
        self.wakeup(wakeups);
        lock.lock().unwrap().notifier.take();
    }

    fn recycle(&mut self, current_timestamp: u64) {
        for slot in &mut self.slots {
            recycle_slot(slot, current_timestamp);
        }
    }
}

fn recycle_slot(slot: &mut LatchSlot, current_timestamp: u64) -> usize {
    let before = slot.entries.len();
    slot.entries.retain(|_, entry| {
        entry.owner.is_some()
            || timestamp_difference_ms(current_timestamp, entry.max_commit_timestamp)
                < EXPIRE_DURATION_MS
    });
    before - slot.entries.len()
}

struct SchedulerState {
    latches: Latches,
    closed: bool,
    last_recycle_timestamp: u64,
    unlock_counter: usize,
}

/// Schedules local transaction latches.
pub(crate) struct LatchesScheduler {
    state: Mutex<SchedulerState>,
}

impl LatchesScheduler {
    pub(crate) fn new(size: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SchedulerState {
                latches: Latches::new(size),
                closed: false,
                last_recycle_timestamp: 0,
                unlock_counter: 0,
            }),
        })
    }

    pub(crate) async fn lock(
        self: &Arc<Self>,
        start_timestamp: u64,
        keys: Vec<Vec<u8>>,
    ) -> LatchGuard {
        let (sender, receiver) = oneshot::channel();
        let lock = {
            let mut state = self.state.lock().unwrap();
            let lock = state.latches.new_lock(start_timestamp, keys);
            lock.lock().unwrap().notifier = Some(sender);
            let result = state.latches.acquire(&lock);
            if result != AcquireResult::Locked {
                lock.lock().unwrap().notifier.take();
            }
            (lock, result)
        };

        if lock.1 == AcquireResult::Locked {
            let mut pending = PendingLock {
                scheduler: self.clone(),
                lock: lock.0.clone(),
                active: true,
            };
            receiver
                .await
                .expect("latch notification sender must remain live while waiting");
            pending.active = false;
        }

        LatchGuard {
            scheduler: self.clone(),
            lock: lock.0,
        }
    }

    pub(crate) fn close(&self) {
        self.state.lock().unwrap().closed = true;
    }

    fn cancel_lock(&self, lock: &SharedLock) {
        let mut state = self.state.lock().unwrap();
        state.latches.cancel(lock);
    }

    fn unlock(&self, lock: &SharedLock) {
        let (start_timestamp, commit_timestamp) = {
            let lock = lock.lock().unwrap();
            (lock.start_timestamp, lock.commit_timestamp)
        };
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return;
        }
        let wakeups = state.latches.release(lock);
        state.latches.wakeup(wakeups);

        if commit_timestamp > start_timestamp
            && (timestamp_difference_ms(commit_timestamp, state.last_recycle_timestamp)
                > CHECK_INTERVAL_MS
                || state.unlock_counter > CHECK_COUNTER)
        {
            state.latches.recycle(commit_timestamp);
            state.last_recycle_timestamp = commit_timestamp;
            state.unlock_counter = 0;
        }
        state.unlock_counter += 1;
    }
}

impl Drop for LatchesScheduler {
    fn drop(&mut self) {
        self.close();
    }
}

struct PendingLock {
    scheduler: Arc<LatchesScheduler>,
    lock: SharedLock,
    active: bool,
}

impl Drop for PendingLock {
    fn drop(&mut self) {
        if self.active {
            self.scheduler.cancel_lock(&self.lock);
        }
    }
}

/// An acquired set of transaction latches.
pub(crate) struct LatchGuard {
    scheduler: Arc<LatchesScheduler>,
    lock: SharedLock,
}

impl LatchGuard {
    pub(crate) fn is_stale(&self) -> bool {
        self.lock.lock().unwrap().is_stale
    }

    pub(crate) fn set_commit_timestamp(&self, commit_timestamp: u64) {
        self.lock.lock().unwrap().commit_timestamp = commit_timestamp;
    }
}

impl Drop for LatchGuard {
    fn drop(&mut self) {
        self.scheduler.unlock(&self.lock);
    }
}

fn timestamp_difference_ms(left: u64, right: u64) -> i64 {
    extract_physical(left) - extract_physical(right)
}

fn murmur3_sum32(bytes: &[u8]) -> u32 {
    const C1: u32 = 0xcc9e_2d51;
    const C2: u32 = 0x1b87_3593;

    let mut hash = 0_u32;
    let mut chunks = bytes.chunks_exact(4);
    for chunk in &mut chunks {
        let mut value = u32::from_le_bytes(chunk.try_into().unwrap());
        value = value.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        hash ^= value;
        hash = hash
            .rotate_left(13)
            .wrapping_mul(5)
            .wrapping_add(0xe654_6b64);
    }

    let tail = chunks.remainder();
    let mut value = 0_u32;
    if tail.len() >= 3 {
        value ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        value ^= u32::from(tail[1]) << 8;
    }
    if let Some(first) = tail.first() {
        value ^= u32::from(*first);
        value = value.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2);
        hash ^= value;
    }

    hash ^= bytes.len() as u32;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85eb_ca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2_ae35);
    hash ^ (hash >> 16)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, UNIX_EPOCH};

    use super::*;
    use crate::oracle::system_time_to_timestamp;

    static TSO: AtomicU64 = AtomicU64::new(0);

    fn next_timestamp() -> u64 {
        TSO.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn lock_state(latches: &mut Latches, keys: &[&[u8]]) -> (u64, SharedLock) {
        let timestamp = next_timestamp();
        let keys = keys.iter().map(|key| key.to_vec()).collect();
        (timestamp, latches.new_lock(timestamp, keys))
    }

    #[test]
    fn original_wakeup_and_stale_scenario() {
        let mut latches = Latches::new(256);
        let (_, lock_a) = lock_state(&mut latches, &[b"a", b"b", b"c"]);
        let (start_b, lock_b) = lock_state(&mut latches, &[b"d", b"e", b"a", b"c"]);

        assert_eq!(latches.acquire(&lock_a), AcquireResult::Success);
        assert_eq!(latches.acquire(&lock_b), AcquireResult::Locked);

        let commit_a = next_timestamp();
        lock_a.lock().unwrap().commit_timestamp = commit_a;
        let wakeups = latches.release(&lock_a);
        assert_eq!(wakeups.len(), 1);
        assert_eq!(wakeups[0].lock().unwrap().start_timestamp, start_b);
        assert_eq!(latches.acquire(&lock_b), AcquireResult::Stale);
        assert!(latches.release(&lock_b).is_empty());

        let (_, restarted_b) = lock_state(&mut latches, &[b"d", b"e", b"a", b"c"]);
        assert_eq!(latches.acquire(&restarted_b), AcquireResult::Success);
    }

    #[test]
    fn original_first_acquire_stale_and_recycle_scenarios() {
        let mut latches = Latches::new(8);
        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let start = system_time_to_timestamp(now);
        let first = latches.new_lock(start, vec![b"a".to_vec(), b"b".to_vec()]);
        let waiting = latches.new_lock(start, vec![b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(latches.acquire(&first), AcquireResult::Success);
        assert_eq!(latches.acquire(&waiting), AcquireResult::Locked);
        first.lock().unwrap().commit_timestamp = start + 1;
        let wakeups = latches.release(&first);
        assert_eq!(wakeups.len(), 1);
        assert_eq!(latches.acquire(&waiting), AcquireResult::Stale);
        latches.release(&waiting);

        let later = latches.new_lock(start + 3, vec![b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(latches.acquire(&later), AcquireResult::Success);
        latches.release(&later);
        assert!(latches.slots.iter().any(|slot| !slot.entries.is_empty()));

        let expiration =
            system_time_to_timestamp(now + Duration::from_millis(EXPIRE_DURATION_MS as u64)) + 3;
        latches.recycle(expiration);
        assert!(latches.slots.iter().all(|slot| slot.entries.is_empty()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn scheduler_serializes_concurrent_transactions() {
        let scheduler = LatchesScheduler::new(7);
        let mut tasks = Vec::new();
        for worker in 0..10_u8 {
            let scheduler = scheduler.clone();
            tasks.push(tokio::spawn(async move {
                for sequence in 0..100_u8 {
                    let first = vec![b'a' + (worker % 4)];
                    let second = vec![b'a' + (sequence % 8)];
                    let keys = if first == second {
                        vec![first]
                    } else {
                        vec![first, second]
                    };
                    let start = next_timestamp();
                    let guard = scheduler.lock(start, keys).await;
                    if !guard.is_stale() {
                        guard.set_commit_timestamp(next_timestamp());
                    }
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn canceled_waiter_releases_its_partial_acquisitions() {
        let scheduler = LatchesScheduler::new(8);
        let owner = scheduler.lock(1, vec![b"b".to_vec()]).await;
        let waiting_scheduler = scheduler.clone();
        let waiter = tokio::spawn(async move {
            waiting_scheduler
                .lock(2, vec![b"a".to_vec(), b"b".to_vec()])
                .await
        });
        tokio::task::yield_now().await;
        waiter.abort();
        let _ = waiter.await;
        drop(owner);

        let guard = scheduler.lock(3, vec![b"a".to_vec(), b"b".to_vec()]).await;
        assert!(!guard.is_stale());
    }

    #[tokio::test]
    async fn close_is_idempotent_and_later_unlocks_are_ignored() {
        let scheduler = LatchesScheduler::new(8);
        let guard = scheduler.lock(1, vec![b"key".to_vec()]).await;
        scheduler.close();
        scheduler.close();
        drop(guard);
    }

    #[test]
    fn murmur3_and_capacity_match_source_boundaries() {
        assert_eq!(murmur3_sum32(b""), 0);
        assert_eq!(murmur3_sum32(b"hello"), 0x248b_fa47);
        assert_eq!(Latches::new(7).slots.len(), 8);
        assert_eq!(Latches::new(8).slots.len(), 8);
        assert_eq!(Latches::new(9).slots.len(), 16);

        let default_config = crate::Config::default();
        assert!(!default_config.txn_local_latches.enabled);
        assert_eq!(default_config.txn_local_latches.capacity, 0);
        let enabled = default_config.with_txn_local_latches(2_048_000);
        assert!(enabled.txn_local_latches.enabled);
        assert_eq!(enabled.txn_local_latches.capacity, 2_048_000);
    }
}
