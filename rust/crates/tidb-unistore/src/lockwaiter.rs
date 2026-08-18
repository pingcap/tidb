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

//! Go `pkg/store/mockstore/unistore/util/lockwaiter` lands as a complete
//! package: the manager that parks a pessimistic-lock waiter until the lock
//! holder commits, until the deadlock detector answers, or until it times out.
//!
//! One [`Manager`] owns a map from key hash to a queue of [`Waiter`]s. A
//! waiter blocks in [`Waiter::wait`] on its own channel; the manager pushes a
//! [`WaitResult`] into that channel from whichever thread resolved the lock.
//! Only the *oldest* waiter on a key (smallest `start_ts`) is granted the lock
//! on [`Manager::wake_up`] and leaves the queue; the rest stay queued and get a
//! [`WAKEUP_DELAY_TIMEOUT`] nudge that shortens their timer to
//! `wake_up_delay_duration` so they retry sooner.
//!
//! # Boundaries
//!
//! - `boundary: kvproto`. Go imports `github.com/pingcap/kvproto/pkg/deadlock`
//!   for `DeadlockResponse`. Of that generated type this package touches
//!   exactly four scalars — `Entry.Txn`, `Entry.WaitForTxn`, `Entry.KeyHash`
//!   and `DeadlockKeyHash` — so [`WaitForEntry`] and [`DeadlockResponse`] are
//!   declared here as plain structs holding those four `u64`s. The protobuf
//!   fields this package never reads (`Entry.Key`, `Entry.ResourceGroupTag`,
//!   `WaitChain`) are not modeled, and neither is the wire encoding; when
//!   `tidb-proto` grows the deadlock service these become type aliases.
//! - `boundary: unistore/config`. Go takes `*config.Config` and reads one
//!   field, `PessimisticTxn.WakeUpDelayDuration`. [`Config`] is that one field,
//!   with [`Config::default`] carrying `config.DefaultConf`'s value of 100ms.
//! - `boundary: logging`. Go's `log.S().Debug` / `log.Info` calls carry no
//!   behavior; this crate has no dependencies and so no logger. The only
//!   visible trace is [`Manager::wake_up`]'s `_txn` parameter, which Go passes
//!   solely to a debug line and which is kept for signature fidelity.
//! - `boundary: channels and timers`. Go's `chan WaitResult` with capacity 32
//!   is [`std::sync::mpsc::sync_channel`] with the same bound: Go's
//!   `select { case ch <- r: default: }` is `try_send` (dropped when the buffer
//!   is full), and Go's blocking send in
//!   [`Manager::wake_up_for_deadlock`] is `send`. Go's
//!   `select { case <-timer.C: ...; case r := <-ch: ... }` is
//!   `Receiver::recv_timeout`, with `timer.Stop`/`timer.Reset` expressed as a
//!   deadline that [`Waiter::wait`] moves earlier. See [`Waiter::wait`] for the
//!   one state difference this causes across repeated calls.
//! - `boundary: pointer identity`. Go's map is `map[uint64]*queue` and its
//!   waiters are `*Waiter`; both are aliased — a caller keeps a `*Waiter` the
//!   manager also holds, and `removeWaiter` compares pointers. Here that is
//!   [`Arc`] plus [`Arc::ptr_eq`], and a queue is `Arc<Queue>` so that a queue
//!   dropped from the map stays observable through an existing handle exactly
//!   as Go's `TestLockwaiterBasic` relies on.
//!
//! Go's `main_test.go` is skipped, as in the rest of this crate: it is
//! `goleak` plus TiDB's global test setup for the Go test binary.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Go `LockNoWait`: the pessimistic-lock wait time meaning "do not wait".
///
/// Part of the lock protocol shared with TiKV: `-1` is no-wait, any other
/// value is a wait in milliseconds. Go declares it as a `var` that nothing
/// assigns to.
pub const LOCK_NO_WAIT: i64 = -1;

/// The capacity of a waiter's result channel, Go's `make(chan WaitResult, 32)`.
const WAITER_CH_CAPACITY: usize = 32;

/// `boundary: unistore/config` — the slice of Go's `config.Config` this
/// package reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    /// Go `PessimisticTxn.WakeUpDelayDuration`: the delay in milliseconds
    /// before a queued (not granted) waiter gives up and retries.
    pub wake_up_delay_duration: i64,
}

impl Default for Config {
    /// Go `config.DefaultConf`: 100ms, the same value TiKV defaults to.
    fn default() -> Self {
        Self {
            wake_up_delay_duration: 100,
        }
    }
}

/// `boundary: kvproto` — `deadlock.WaitForEntry`, whole. This package reads
/// only the three scalars; [`crate::detector`] fills the diagnostic pair
/// as well, and both share this one type rather than keeping two shapes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WaitForEntry {
    /// The transaction that is blocked.
    pub txn: u64,
    /// The transaction holding the lock it is blocked on.
    pub wait_for_txn: u64,
    /// The hash of the locked key.
    pub key_hash: u64,
    /// `Key`, the locked key itself — diagnostics only.
    pub key: Vec<u8>,
    /// `ResourceGroupTag` — diagnostics only.
    pub resource_group_tag: Vec<u8>,
}

/// `boundary: kvproto` — the scalars of `deadlock.DeadlockResponse` this
/// package reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeadlockResponse {
    /// The wait edge that closed the cycle.
    pub entry: WaitForEntry,
    /// The hash of the key whose lock completes the deadlock cycle.
    pub deadlock_key_hash: u64,
}

/// Go `WakeupWaitTime`: the implementation of the variable
/// `wake-up-delay-duration`.
///
/// Not an enum: [`WAIT_TIMEOUT`], [`WAKE_UP_THIS_WAITER`] and
/// [`WAKEUP_DELAY_TIMEOUT`] are named points in an open range whose other
/// values are sleep times in milliseconds, exactly as Go's `type
/// WakeupWaitTime int` is.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct WakeupWaitTime(pub i64);

/// Go `WaitTimeout`: the wait has already timed out.
pub const WAIT_TIMEOUT: WakeupWaitTime = WakeupWaitTime(-1);
/// Go `WakeUpThisWaiter`: the lock will be granted to this waiter.
pub const WAKE_UP_THIS_WAITER: WakeupWaitTime = WakeupWaitTime(0);
/// Go `WakeupDelayTimeout`: this waiter stays queued and should retry after
/// the wake-up delay.
pub const WAKEUP_DELAY_TIMEOUT: WakeupWaitTime = WakeupWaitTime(1);

/// Go `WaitResult`: what a [`Waiter::wait`] call resolved to.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WaitResult {
    /// Set when the deadlock detector, not the lock holder, ended the wait.
    pub deadlock_resp: Option<DeadlockResponse>,
    /// [`WAIT_TIMEOUT`] means the wait already timed out, [`WAKE_UP_THIS_WAITER`]
    /// that the lock will be granted to this waiter; other values are the
    /// wake-up-delay-duration sleep time, in milliseconds.
    pub wakeup_sleep_time: WakeupWaitTime,
    /// The commit TS of the transaction that released the lock.
    pub commit_ts: u64,
}

/// Go `Waiter`: one transaction parked on one key hash.
///
/// Shared: the [`Manager`] keeps the same [`Arc`] the caller holds, and wakes
/// the waiter by pushing into the channel whose receiving end lives here.
#[derive(Debug)]
pub struct Waiter {
    /// Go `deadlineTime`: when the original `timeout` expires. A wake-up delay
    /// may pull the effective timer earlier but never past this.
    deadline_time: Instant,
    ch_tx: SyncSender<WaitResult>,
    ch_rx: Mutex<Receiver<WaitResult>>,
    wake_up_delay_duration: i64,
    start_ts: u64,
    /// Go `LockTS`: the transaction whose lock is being waited for.
    pub lock_ts: u64,
    /// Go `KeyHash`: the hash of the key being waited on.
    pub key_hash: u64,
    /// Go `CommitTs`: the commit TS learned from a wake-up delay, reported
    /// again when the shortened timer then fires. Atomic because Go's field is
    /// exported and this type is shared; only [`Waiter::wait`] writes it.
    commit_ts: AtomicU64,
}

impl Waiter {
    /// Go's `CommitTs` field read.
    #[must_use]
    pub fn commit_ts(&self) -> u64 {
        self.commit_ts.load(Ordering::SeqCst)
    }

    /// Go's unexported `startTS` field, exposed read-only because the wake-up
    /// order (oldest transaction first) is the queue's contract.
    #[must_use]
    pub fn start_ts(&self) -> u64 {
        self.start_ts
    }

    /// Go `Wait`: waits on the lock until woken by others or timed out.
    ///
    /// A [`WAKEUP_DELAY_TIMEOUT`] result does not end the wait: it records the
    /// commit TS, shortens the timer to `wake_up_delay_duration` when that
    /// lands before the deadline, and keeps waiting — so the eventual timeout
    /// reports [`WAKEUP_DELAY_TIMEOUT`] with that commit TS rather than
    /// [`WAIT_TIMEOUT`].
    ///
    /// `boundary: timers`. Go's `*time.Timer` is a field, so its fired-and-
    /// drained state survives a returned `Wait`; the deadline here is a local,
    /// so a second `wait` call after a timeout times out again immediately
    /// where Go's would block forever on a drained channel. No caller waits
    /// twice — Go's own `Manager` hands each waiter a fresh timer.
    #[must_use]
    pub fn wait(&self) -> WaitResult {
        let rx = lock(&self.ch_rx);
        // Go creates the timer with the same `timeout` used for `deadlineTime`.
        let mut timer_deadline = self.deadline_time;
        let mut wakeup_delayed = false;
        loop {
            let now = Instant::now();
            let remaining = timer_deadline.saturating_duration_since(now);
            match rx.recv_timeout(remaining) {
                Err(RecvTimeoutError::Timeout) => {
                    if wakeup_delayed {
                        return WaitResult {
                            deadlock_resp: None,
                            wakeup_sleep_time: WAKEUP_DELAY_TIMEOUT,
                            commit_ts: self.commit_ts(),
                        };
                    }
                    return WaitResult {
                        deadlock_resp: None,
                        wakeup_sleep_time: WAIT_TIMEOUT,
                        commit_ts: 0,
                    };
                }
                // Unreachable: the waiter owns a clone of the sender, so the
                // channel outlives every receive. Go has no disconnect at all
                // — nothing ever closes the channel — so this takes the same
                // exit as a timeout rather than inventing an error path.
                Err(RecvTimeoutError::Disconnected) => {
                    return WaitResult {
                        deadlock_resp: None,
                        wakeup_sleep_time: WAIT_TIMEOUT,
                        commit_ts: 0,
                    };
                }
                Ok(result) => {
                    if result.wakeup_sleep_time == WAKEUP_DELAY_TIMEOUT {
                        self.commit_ts.store(result.commit_ts, Ordering::SeqCst);
                        wakeup_delayed = true;
                        let delay_sleep_duration =
                            Duration::from_millis(self.wake_up_delay_duration.max(0) as u64);
                        let now = Instant::now();
                        if now + delay_sleep_duration < self.deadline_time {
                            // Go's `if w.timer.Stop() { w.timer.Reset(d) }`: a
                            // timer that already fired cannot be restarted, and
                            // its pending tick still ends the next iteration.
                            if now < timer_deadline {
                                timer_deadline = now + delay_sleep_duration;
                            }
                        }
                        continue;
                    }
                    return result;
                }
            }
        }
    }

    /// Go `DrainCh`: discards every buffered result without blocking.
    pub fn drain_ch(&self) {
        let rx = lock(&self.ch_rx);
        while rx.try_recv().is_ok() {}
    }
}

/// Go's unexported `queue`: the waiters on one key hash.
///
/// Go's map holds `*queue`, and its contents are guarded by the manager's
/// mutex; here the queue carries its own mutex so a handle can outlive the map
/// entry, and it is only ever locked while the manager's map lock is held.
#[derive(Debug, Default)]
struct Queue {
    waiters: Mutex<Vec<Arc<Waiter>>>,
}

impl Queue {
    /// Go `getOldestWaiter`: pops the smallest-`startTS` waiter and returns it
    /// with the waiters that stay queued.
    ///
    /// Panics on an empty queue, as Go's `q.waiters[0]` does.
    fn get_oldest_waiter(&self) -> (Arc<Waiter>, Vec<Arc<Waiter>>) {
        let mut waiters = lock(&self.waiters);
        // make the waiters in start ts order
        waiters.sort_by_key(|w| w.start_ts);
        let oldest_waiter = waiters.remove(0);
        // the remain waiters still exist in the wait queue
        (oldest_waiter, waiters.clone())
    }

    /// Go `removeWaiter`: removes the corresponding waiter from the pending
    /// array. It should be used under map lock protection.
    fn remove_waiter(&self, w: &Arc<Waiter>) {
        let mut waiters = lock(&self.waiters);
        if let Some(i) = waiters.iter().position(|waiter| Arc::ptr_eq(waiter, w)) {
            waiters.remove(i);
        }
    }

    fn len(&self) -> usize {
        lock(&self.waiters).len()
    }
}

/// Go `Manager`: the waiters manager.
#[derive(Debug)]
pub struct Manager {
    waiting_queues: Mutex<HashMap<u64, Arc<Queue>>>,
    wake_up_delay_duration: i64,
}

impl Manager {
    /// Go `NewManager`: returns a new manager reading
    /// `PessimisticTxn.WakeUpDelayDuration` out of the config.
    #[must_use]
    pub fn new(conf: &Config) -> Self {
        Self {
            waiting_queues: Mutex::new(HashMap::new()),
            wake_up_delay_duration: conf.wake_up_delay_duration,
        }
    }

    /// Go `NewWaiter`: registers and returns a new waiter on `key_hash`.
    #[must_use]
    pub fn new_waiter(
        &self,
        start_ts: u64,
        lock_ts: u64,
        key_hash: u64,
        timeout: Duration,
    ) -> Arc<Waiter> {
        // allocate memory before hold the lock.
        let (ch_tx, ch_rx) = sync_channel(WAITER_CH_CAPACITY);
        let waiter = Arc::new(Waiter {
            deadline_time: Instant::now() + timeout,
            wake_up_delay_duration: self.wake_up_delay_duration,
            ch_tx,
            ch_rx: Mutex::new(ch_rx),
            start_ts,
            lock_ts,
            key_hash,
            commit_ts: AtomicU64::new(0),
        });
        let mut queues = lock(&self.waiting_queues);
        let q = queues.entry(key_hash).or_default();
        lock(&q.waiters).push(Arc::clone(&waiter));
        drop(queues);
        waiter
    }

    /// Go `WakeUp`: wakes up the waiters waiting on the transaction.
    ///
    /// The oldest waiter per key hash leaves its queue and is granted the lock;
    /// the waiters that stay queued get a delay nudge instead.
    ///
    /// `boundary: logging` — Go passes `_txn` only to a debug log line.
    pub fn wake_up(&self, _txn: u64, commit_ts: u64, key_hashes: &[u64]) {
        let mut waiters: Vec<Arc<Waiter>> = Vec::with_capacity(8);
        let mut wake_up_delay_waiters: Vec<Arc<Waiter>> = Vec::with_capacity(8);
        {
            let mut queues = lock(&self.waiting_queues);
            for &key_hash in key_hashes {
                let Some(q) = queues.get(&key_hash).map(Arc::clone) else {
                    continue;
                };
                let (waiter, remain_waiters) = q.get_oldest_waiter();
                waiters.push(waiter);
                if remain_waiters.is_empty() {
                    queues.remove(&key_hash);
                } else {
                    wake_up_delay_waiters.extend(remain_waiters);
                }
            }
        }

        // wake up waiters
        for w in &waiters {
            // Go's `select { case w.ch <- r: default: }`: drop the result when
            // the waiter's buffer is full.
            let _ = w.ch_tx.try_send(WaitResult {
                deadlock_resp: None,
                wakeup_sleep_time: WAKE_UP_THIS_WAITER,
                commit_ts,
            });
        }
        // wake up delay waiters, this will not remove waiter from queue
        for w in &wake_up_delay_waiters {
            let _ = w.ch_tx.try_send(WaitResult {
                deadlock_resp: None,
                wakeup_sleep_time: WAKEUP_DELAY_TIMEOUT,
                commit_ts,
            });
        }
    }

    /// Go `CleanUp`: removes a waiter from the waiting queues when the wait
    /// timed out.
    pub fn clean_up(&self, w: &Arc<Waiter>) {
        {
            let mut queues = lock(&self.waiting_queues);
            if let Some(q) = queues.get(&w.key_hash).map(Arc::clone) {
                q.remove_waiter(w);
                if q.len() == 0 {
                    queues.remove(&w.key_hash);
                }
            }
        }
        w.drain_ch();
    }

    /// Go `WakeUpForDeadlock`: wakes up the waiter that the deadlock detector
    /// answered for.
    pub fn wake_up_for_deadlock(&self, resp: &DeadlockResponse) {
        let mut waiter: Option<Arc<Waiter>> = None;
        let wait_for_key_hash = resp.entry.key_hash;
        {
            let mut queues = lock(&self.waiting_queues);
            if let Some(q) = queues.get(&wait_for_key_hash).map(Arc::clone) {
                {
                    let mut waiters = lock(&q.waiters);
                    // there should be no duplicated waiters
                    let found = waiters.iter().position(|cur| {
                        cur.start_ts == resp.entry.txn && cur.key_hash == resp.entry.key_hash
                    });
                    if let Some(i) = found {
                        waiter = Some(waiters.remove(i));
                    }
                }
                if q.len() == 0 {
                    queues.remove(&wait_for_key_hash);
                }
            }
        }
        if let Some(w) = waiter {
            // Go's plain `w.ch <- ...`, blocking when the buffer is full. The
            // send cannot fail: `w` holds its own receiver alive.
            let _ = w.ch_tx.send(WaitResult {
                deadlock_resp: Some(resp.clone()),
                wakeup_sleep_time: WAKE_UP_THIS_WAITER,
                commit_ts: 0,
            });
        }
    }
}

/// Go's `sync.Mutex.Lock`, which has no poisoning: a panic while a lock is held
/// leaves the data usable in Go, so recover the guard instead of propagating.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;
    use std::thread;

    /// Go's bare `<-waiter.ch`: a blocking receive that bypasses
    /// [`Waiter::wait`]'s timer, used by `TestLockwaiterBasic`.
    fn recv(w: &Waiter) -> WaitResult {
        lock(&w.ch_rx).recv().unwrap()
    }

    fn queue_of(mgr: &Manager, key_hash: u64) -> Option<Arc<Queue>> {
        lock(&mgr.waiting_queues).get(&key_hash).map(Arc::clone)
    }

    /// Go `TestLockwaiterBasic`.
    #[test]
    fn test_lockwaiter_basic() {
        let mgr = Manager::new(&Config::default());

        let key_hash = 100u64;
        // Go passes the untyped constant 10 as a `time.Duration`, i.e. 10ns.
        // Nothing in this test waits on a timer, so the value is inert.
        let timeout = Duration::from_nanos(10);
        let _ = mgr.new_waiter(1, 2, key_hash, timeout);

        // basic check queue and waiter
        let q = queue_of(&mgr, key_hash);
        assert!(q.is_some());
        let q = q.unwrap();
        let waiter = Arc::clone(&lock(&q.waiters)[0]);
        assert_eq!(1, waiter.start_ts);
        assert_eq!(2, waiter.lock_ts);
        assert_eq!(100, waiter.key_hash);

        // check ready waiters
        let keys_hash = vec![key_hash];
        let (rdy_waiter, _) = q.get_oldest_waiter();
        assert_eq!(1, rdy_waiter.start_ts);
        assert_eq!(2, rdy_waiter.lock_ts);
        assert_eq!(100, rdy_waiter.key_hash);

        // basic wake up test
        // The queue is empty but still in the map, so this waiter joins `q`.
        let waiter = mgr.new_waiter(3, 2, key_hash, timeout);
        mgr.wake_up(2, 222, &keys_hash);
        let res = recv(&waiter);
        assert_eq!(222, res.commit_ts);
        assert_eq!(0, q.len());
        // verify queue deleted from map
        assert!(queue_of(&mgr, key_hash).is_none());

        // basic wake up for deadlock test
        let waiter = mgr.new_waiter(3, 4, key_hash, timeout);
        let resp = DeadlockResponse {
            entry: WaitForEntry {
                txn: 3,
                wait_for_txn: 4,
                key_hash,
                ..WaitForEntry::default()
            },
            deadlock_key_hash: 30192,
        };
        mgr.wake_up_for_deadlock(&resp);
        let res = recv(&waiter);
        assert!(res.deadlock_resp.is_some());
        let got = res.deadlock_resp.unwrap();
        assert_eq!(3, got.entry.txn);
        assert_eq!(4, got.entry.wait_for_txn);
        assert_eq!(key_hash, got.entry.key_hash);
        assert_eq!(30192, got.deadlock_key_hash);
        // verify queue deleted from map. Go checks key 4 — the lock TS, not the
        // key hash — which was never a map key; kept as written.
        assert!(queue_of(&mgr, 4).is_none());
    }

    /// Go `TestLockwaiterConcurrent`.
    ///
    /// Go's `wg` is a `sync.WaitGroup` the main goroutine waits on so that
    /// every waiter is registered before any wake-up is issued; that rendezvous
    /// is a [`Barrier`] of `numbers + 1` here, which additionally holds the
    /// worker threads until main arrives — harmless, since main arrives exactly
    /// when the last worker registers. Go's `endWg` is thread joins. Go's
    /// `sync.RWMutex` around `resp.DeadlockKeyHash` guards a `*DeadlockResponse`
    /// the main goroutine mutates while woken goroutines read it; a
    /// [`WaitResult`] here carries an owned copy, so there is nothing to guard
    /// and no lock. The 100ms timeout is semantic — thread `numbers-1` cleans
    /// itself out of the queue and must observe [`WAIT_TIMEOUT`] — and is kept.
    #[test]
    fn test_lockwaiter_concurrent() {
        let mgr = Arc::new(Manager::new(&Config::default()));
        let wait_for_txn = 100u64;
        let commit_ts = 199u64;
        let deadlock_key_hash = 299u64;
        let numbers = 10u64;
        let barrier = Arc::new(Barrier::new(numbers as usize + 1));

        let mut handles = Vec::with_capacity(numbers as usize);
        for num in 0..numbers {
            let mgr = Arc::clone(&mgr);
            let barrier = Arc::clone(&barrier);
            handles.push(thread::spawn(move || {
                let waiter =
                    mgr.new_waiter(num, wait_for_txn, num * 10, Duration::from_millis(100));
                // num == numbers - 1 uses CleanUp on the waiter and the result
                // will be a timeout
                if num == numbers - 1 {
                    mgr.clean_up(&waiter);
                    barrier.wait();
                    let res = waiter.wait();
                    assert_eq!(WAIT_TIMEOUT, res.wakeup_sleep_time);
                    assert_eq!(0, res.commit_ts);
                    assert!(res.deadlock_resp.is_none());
                } else {
                    barrier.wait();
                    let res = waiter.wait();
                    // even woken up by commit
                    if num % 2 == 0 {
                        assert_eq!(commit_ts, res.commit_ts);
                    } else {
                        // odd woken up by deadlock
                        assert!(res.deadlock_resp.is_some());
                        assert_eq!(
                            deadlock_key_hash,
                            res.deadlock_resp.unwrap().deadlock_key_hash
                        );
                    }
                }
            }));
        }
        barrier.wait();

        let mut resp = DeadlockResponse::default();
        for i in 0..numbers {
            if i % 2 == 0 {
                mgr.wake_up(wait_for_txn, commit_ts, &[i * 10]);
            } else {
                resp.deadlock_key_hash = deadlock_key_hash;
                resp.entry.txn = i;
                resp.entry.wait_for_txn = wait_for_txn;
                resp.entry.key_hash = i * 10;
                mgr.wake_up_for_deadlock(&resp);
            }
        }
        for h in handles {
            h.join().unwrap();
        }
    }
}
