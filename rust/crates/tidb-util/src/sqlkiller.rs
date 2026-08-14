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

//! Complete transcreation of Go `pkg/util/sqlkiller` (`sqlkiller.go`): the
//! per-query kill switch checked by executors.
//!
//! Faithful Rust adaptations, none changing observable behavior:
//! - Go's `chan struct{}` close-broadcast kill event becomes a stable
//!   [`KillEventSubscription`] over a generation-tagged `Condvar`. Both a
//!   kill and `Reset` release existing subscribers, while a subscriber
//!   created after reset waits on the new generation.
//! - The `Signal` CAS (`0 -> reason`, first signal wins) is the same
//!   `compare_exchange` on an `AtomicU32`.
//! - `logutil.BgLogger()` lines map to `tracing`; the `failpoint`
//!   random-panic injection is Go test machinery with no runtime behavior
//!   and is not ported.
//! - Function-pointer fields (`Finish`, `IsConnectionAlive`) become guarded
//!   callback slots; liveness registration tokens preserve the source's
//!   conditional compare-and-swap removal.
//!
//! The Go package ships no test; the tests below pin the contract:
//! first-signal-wins, kill-event trigger/reset/late-subscribe semantics,
//! the exact error mapping (messages come from the fixture-verified
//! `exeerrors` table), connection-alive interval gating (1s, 1ms under
//! `intest`), and `Reset`.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender};
use tidb_error::mysql::FormatArg;
use tidb_error::terror::TerrorError;

use crate::dbterror::exeerrors;
use crate::intest;

fn lock_unpoison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Kill signal types (Go `killSignal` constants). When adding a new signal,
/// the source also updates `store/driver/error/ToTiDBErr`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum KillSignal {
    /// No kill requested.
    UnspecifiedKillSignal = 0,
    /// `KILL QUERY` / connection dead.
    QueryInterrupted = 1,
    /// `max_execution_time` exceeded.
    MaxExecTimeExceeded = 2,
    /// Per-query memory quota exceeded.
    QueryMemoryExceeded = 3,
    /// Server memory limit exceeded.
    ServerMemoryExceeded = 4,
    /// Runaway-query watchdog.
    RunawayQueryExceeded = 5,
    /// Killed by the memory arbitrator.
    KilledByMemArbitrator = 6,
}

impl KillSignal {
    fn from_u32(v: u32) -> Option<KillSignal> {
        Some(match v {
            1 => KillSignal::QueryInterrupted,
            2 => KillSignal::MaxExecTimeExceeded,
            3 => KillSignal::QueryMemoryExceeded,
            4 => KillSignal::ServerMemoryExceeded,
            5 => KillSignal::RunawayQueryExceeded,
            6 => KillSignal::KilledByMemArbitrator,
            _ => return None,
        })
    }
}

#[derive(Default)]
struct KillEventState {
    generation: u64,
    triggered: bool,
    desc: String,
}

#[derive(Default)]
struct KillEventShared {
    state: Mutex<KillEventState>,
    ready: Condvar,
    /// One-shot receivers used by resource pools that must interrupt a
    /// blocking allocation as soon as this statement is killed or reset.
    waiters: Mutex<Vec<Sender<()>>>,
}

/// A stable receiver for one Go kill-event channel generation.
///
/// A subscription becomes ready when its generation is killed or reset. A
/// subscription created after reset belongs to the next generation and waits
/// independently, matching `GetKillEventChan` returning a newly allocated
/// channel after `Reset`.
#[derive(Clone)]
pub struct KillEventSubscription {
    shared: Arc<KillEventShared>,
    generation: u64,
}

impl KillEventSubscription {
    fn ready(state: &KillEventState, generation: u64) -> bool {
        state.triggered || state.generation != generation
    }

    /// Whether this subscription's channel has been closed.
    pub fn is_ready(&self) -> bool {
        let state = lock_unpoison(&self.shared.state);
        Self::ready(&state, self.generation)
    }

    /// Waits until a kill or reset closes this subscription.
    pub fn wait(&self) {
        let mut state = lock_unpoison(&self.shared.state);
        while !Self::ready(&state, self.generation) {
            state = self
                .shared
                .ready
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

type AliveFn = Arc<dyn Fn() -> bool + Send + Sync>;
type FinishFn = Box<dyn Fn() + Send + Sync>;

struct AliveProbe {
    registration: u64,
    callback: AliveFn,
}

/// Identifies one installed connection-liveness callback.
///
/// It provides the same conditional removal behavior as Go's
/// `atomic.Pointer.CompareAndSwap`: clearing an older registration cannot
/// remove a callback installed later.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionAliveRegistration(u64);

/// Kills a query (Go `SQLKiller`).
#[derive(Default)]
pub struct SqlKiller {
    signal: AtomicU32,
    /// The connection ID.
    pub conn_id: AtomicU64,
    /// Whether the query is currently writing its result set.
    pub in_write_result_set: AtomicBool,
    kill_event: Arc<KillEventShared>,
    finish: Mutex<Option<FinishFn>>,
    is_connection_alive: Mutex<Option<AliveProbe>>,
    next_alive_registration: AtomicU64,
    last_check_time: Mutex<Option<Instant>>,
}

impl SqlKiller {
    /// Returns the current kill-event subscription (Go
    /// `GetKillEventChan`).
    pub fn get_kill_event(&self) -> KillEventSubscription {
        let generation = lock_unpoison(&self.kill_event.state).generation;
        KillEventSubscription {
            shared: Arc::clone(&self.kill_event),
            generation,
        }
    }

    /// Subscribes to the current kill-event generation through a receiver.
    ///
    /// The memory arbitrator selects on this receiver while a root pool waits
    /// for capacity. A kill or the next statement's reset closes the Go
    /// channel, so both must wake the blocked allocation.
    pub fn subscribe_kill_event(&self) -> Receiver<()> {
        let (tx, rx) = bounded(1);
        let state = lock_unpoison(&self.kill_event.state);
        if state.triggered {
            let _ = tx.send(());
        } else {
            lock_unpoison(&self.kill_event.waiters).push(tx);
        }
        rx
    }

    /// Whether the kill event has been triggered (a closed Go channel).
    pub fn kill_event_triggered(&self) -> bool {
        lock_unpoison(&self.kill_event.state).triggered
    }

    /// Subscribes to and waits for the current kill event.
    pub fn wait_kill_event(&self) {
        self.get_kill_event().wait();
    }

    /// Waits up to `duration` for a real kill signal.
    ///
    /// A statement reset also wakes the underlying generation channel, but
    /// it is not itself a kill. In that case this method subscribes to the
    /// new generation and keeps waiting for the remainder of the deadline.
    #[must_use]
    pub fn wait_kill_event_timeout(&self, duration: Duration) -> bool {
        let started = Instant::now();
        loop {
            if self.get_kill_signal().is_some() {
                return true;
            }
            let elapsed = started.elapsed();
            if elapsed >= duration {
                return false;
            }
            let event = self.subscribe_kill_event();
            match event.recv_timeout(duration - elapsed) {
                Ok(()) | Err(crossbeam_channel::RecvTimeoutError::Disconnected) => continue,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => return false,
            }
        }
    }

    fn trigger_kill_event(&self) {
        let mut state = lock_unpoison(&self.kill_event.state);
        if state.triggered {
            return;
        }
        state.triggered = true;
        for waiter in lock_unpoison(&self.kill_event.waiters).drain(..) {
            let _ = waiter.send(());
        }
        self.kill_event.ready.notify_all();
    }

    fn reset_kill_event(&self) {
        let mut state = lock_unpoison(&self.kill_event.state);
        state.generation = state.generation.wrapping_add(1);
        state.triggered = false;
        state.desc.clear();
        for waiter in lock_unpoison(&self.kill_event.waiters).drain(..) {
            let _ = waiter.send(());
        }
        self.kill_event.ready.notify_all();
    }

    /// Sets the kill-event reason and sends the signal (Go
    /// `SendKillSignalWithKillEventReason`).
    pub fn send_kill_signal_with_reason(&self, signal: KillSignal, desc: &str) {
        lock_unpoison(&self.kill_event.state).desc = desc.to_string();
        self.send_kill_signal_inner(signal);
        self.trigger_kill_event();
    }

    fn send_kill_signal_inner(&self, reason: KillSignal) {
        if self
            .signal
            .compare_exchange(0, reason as u32, SeqCst, SeqCst)
            .is_ok()
        {
            let status = self.signal.load(SeqCst);
            if let Some(err) = self.kill_error(status) {
                tracing::warn!(
                    connection_id = self.conn_id.load(SeqCst),
                    reason = %err,
                    "kill initiated"
                );
            }
        }
    }

    /// Sends a kill signal to the query (Go `SendKillSignal`).
    pub fn send_kill_signal(&self, reason: KillSignal) {
        self.send_kill_signal_inner(reason);
        self.trigger_kill_event();
    }

    /// Gets the current kill signal.
    pub fn get_kill_signal(&self) -> Option<KillSignal> {
        KillSignal::from_u32(self.signal.load(SeqCst))
    }

    fn kill_event_reason(&self) -> String {
        lock_unpoison(&self.kill_event.state).desc.clone()
    }

    /// The error for a kill status (Go `getKillError`); `None` when no kill
    /// is pending.
    fn kill_error(&self, status: u32) -> Option<TerrorError> {
        let conn_id = self.conn_id.load(SeqCst);
        let by_args = |proto: &TerrorError, args: &[FormatArg]| {
            let template = proto.message().to_string();
            let formatted = proto.fast_generate(&template, args);
            proto.generate_with_stack(formatted.message().to_string())
        };
        Some(match KillSignal::from_u32(status)? {
            KillSignal::UnspecifiedKillSignal => return None,
            KillSignal::QueryInterrupted => by_args(&exeerrors::ERR_QUERY_INTERRUPTED, &[]),
            KillSignal::MaxExecTimeExceeded => by_args(&exeerrors::ERR_MAX_EXEC_TIME_EXCEEDED, &[]),
            KillSignal::QueryMemoryExceeded => by_args(
                &exeerrors::ERR_MEMORY_EXCEED_FOR_QUERY,
                &[FormatArg::from(conn_id)],
            ),
            KillSignal::ServerMemoryExceeded => by_args(
                &exeerrors::ERR_MEMORY_EXCEED_FOR_INSTANCE,
                &[FormatArg::from(conn_id)],
            ),
            KillSignal::RunawayQueryExceeded => {
                let proto = &exeerrors::ERR_RESOURCE_GROUP_QUERY_RUNAWAY_INTERRUPTED;
                let template = proto.message().to_string();
                proto.fast_generate(&template, &[FormatArg::from("runaway exceed tidb side")])
            }
            KillSignal::KilledByMemArbitrator => by_args(
                &exeerrors::ERR_QUERY_EXEC_STOPPED,
                &[
                    FormatArg::from(self.kill_event_reason().as_str()),
                    FormatArg::from(conn_id),
                ],
            ),
        })
    }

    /// Closes the result set to release resources when a killed query is
    /// stuck writing to the client (Go `FinishResultSet`).
    pub fn finish_result_set(&self) {
        let finish = lock_unpoison(&self.finish);
        if let Some(f) = finish.as_ref() {
            f();
        }
    }

    /// Sets the finish function.
    pub fn set_finish_func(&self, f: FinishFn) {
        *lock_unpoison(&self.finish) = Some(f);
    }

    /// Clears the finish function.
    pub fn clear_finish_func(&self) {
        *lock_unpoison(&self.finish) = None;
    }

    /// Installs the connection-liveness probe and returns its registration.
    pub fn set_is_connection_alive(
        &self,
        f: Box<dyn Fn() -> bool + Send + Sync>,
    ) -> ConnectionAliveRegistration {
        let registration = self
            .next_alive_registration
            .fetch_add(1, SeqCst)
            .wrapping_add(1);
        *lock_unpoison(&self.is_connection_alive) = Some(AliveProbe {
            registration,
            callback: Arc::from(f),
        });
        ConnectionAliveRegistration(registration)
    }

    /// Removes the probe only when `registration` still owns the slot.
    pub fn clear_is_connection_alive(&self, registration: ConnectionAliveRegistration) -> bool {
        let mut probe = lock_unpoison(&self.is_connection_alive);
        if probe.as_ref().map(|p| p.registration) != Some(registration.0) {
            return false;
        }
        *probe = None;
        true
    }

    /// Handles the kill signal, returning the pending kill error (Go
    /// `HandleSignal`). Also polls connection liveness at most once per
    /// second (1ms under `intest`), like the source.
    pub fn handle_signal(&self) -> Option<TerrorError> {
        let alive = lock_unpoison(&self.is_connection_alive)
            .as_ref()
            .map(|probe| Arc::clone(&probe.callback));
        if let Some(fn_alive) = alive {
            let check_dur = if intest::IN_TEST {
                Duration::from_millis(1)
            } else {
                Duration::from_secs(1)
            };
            let now = Instant::now();
            let should_check = {
                let mut last = lock_unpoison(&self.last_check_time);
                match *last {
                    None => {
                        *last = Some(now);
                        false
                    }
                    Some(prev) if now.duration_since(prev) > check_dur => {
                        *last = Some(now);
                        true
                    }
                    _ => false,
                }
            };
            if should_check && !fn_alive() {
                self.send_kill_signal_inner(KillSignal::QueryInterrupted);
            }
        }

        let status = self.signal.load(SeqCst);
        let err = self.kill_error(status);
        if status == KillSignal::ServerMemoryExceeded as u32 {
            tracing::warn!(
                conn = self.conn_id.load(SeqCst),
                "global memory controller, NeedKill signal is received successfully"
            );
        }
        err
    }

    /// Checks connection liveness immediately (Go `CheckConnectionAlive`).
    pub fn check_connection_alive(&self) {
        let alive = lock_unpoison(&self.is_connection_alive)
            .as_ref()
            .map(|probe| Arc::clone(&probe.callback));
        if let Some(fn_alive) = alive {
            if !fn_alive() {
                self.send_kill_signal_inner(KillSignal::QueryInterrupted);
            }
        }
    }

    /// Resets the killer (Go `Reset`).
    pub fn reset(&self) {
        if self.signal.load(SeqCst) != 0 {
            tracing::warn!(conn = self.conn_id.load(SeqCst), "kill finished");
        }
        self.signal.store(0, SeqCst);
        self.reset_kill_event();
        *lock_unpoison(&self.last_check_time) = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;

    #[test]
    fn first_signal_wins() {
        let killer = SqlKiller::default();
        assert_eq!(killer.get_kill_signal(), None);
        killer.send_kill_signal(KillSignal::MaxExecTimeExceeded);
        killer.send_kill_signal(KillSignal::QueryInterrupted);
        assert_eq!(
            killer.get_kill_signal(),
            Some(KillSignal::MaxExecTimeExceeded)
        );
        assert!(killer.kill_event_triggered());

        killer.reset();
        assert_eq!(killer.get_kill_signal(), None);
        assert!(!killer.kill_event_triggered());
    }

    #[test]
    fn receiver_subscribers_wake_for_kill_and_statement_reset() {
        let killer = SqlKiller::default();
        let killed = killer.subscribe_kill_event();
        killer.send_kill_signal(KillSignal::QueryInterrupted);
        assert!(killed.recv_timeout(Duration::from_millis(10)).is_ok());

        killer.reset();
        let reset = killer.subscribe_kill_event();
        killer.reset();
        assert!(reset.recv_timeout(Duration::from_millis(10)).is_ok());
    }

    #[test]
    fn kill_error_mapping() {
        let killer = SqlKiller::default();
        killer.conn_id.store(42, SeqCst);

        let cases = [
            (
                KillSignal::QueryInterrupted,
                &*exeerrors::ERR_QUERY_INTERRUPTED,
            ),
            (
                KillSignal::MaxExecTimeExceeded,
                &*exeerrors::ERR_MAX_EXEC_TIME_EXCEEDED,
            ),
            (
                KillSignal::QueryMemoryExceeded,
                &*exeerrors::ERR_MEMORY_EXCEED_FOR_QUERY,
            ),
            (
                KillSignal::ServerMemoryExceeded,
                &*exeerrors::ERR_MEMORY_EXCEED_FOR_INSTANCE,
            ),
            (
                KillSignal::RunawayQueryExceeded,
                &*exeerrors::ERR_RESOURCE_GROUP_QUERY_RUNAWAY_INTERRUPTED,
            ),
        ];
        for (signal, expected) in cases {
            killer.send_kill_signal(signal);
            let err = killer.handle_signal().expect("kill pending");
            assert_eq!(err.code(), expected.code(), "{signal:?}");
            killer.reset();
        }

        killer.send_kill_signal_with_reason(KillSignal::KilledByMemArbitrator, "oom risk");
        let err = killer.handle_signal().expect("kill pending");
        assert_eq!(err.code(), exeerrors::ERR_QUERY_EXEC_STOPPED.code());
        assert!(err.message().contains("oom risk"), "{}", err.message());
        assert!(err.message().contains("42"), "{}", err.message());

        killer.reset();
        assert!(killer.handle_signal().is_none());
    }

    #[test]
    fn connection_errors_preserve_the_unsigned_id_domain() {
        let killer = SqlKiller::default();
        killer.conn_id.store(u64::MAX, SeqCst);
        let expected = "[conn=18446744073709551615]";

        for signal in [
            KillSignal::QueryMemoryExceeded,
            KillSignal::ServerMemoryExceeded,
        ] {
            killer.send_kill_signal(signal);
            let error = killer.handle_signal().expect("kill pending");
            assert!(error.message().contains(expected), "{}", error.message());
            killer.reset();
        }

        killer.send_kill_signal_with_reason(KillSignal::KilledByMemArbitrator, "oom risk");
        let error = killer.handle_signal().expect("kill pending");
        assert!(error.message().contains(expected), "{}", error.message());
    }

    #[test]
    fn kill_event_wakes_waiters_and_late_subscribers() {
        let killer = Arc::new(SqlKiller::default());
        let waiter = {
            let killer = Arc::clone(&killer);
            std::thread::spawn(move || killer.wait_kill_event())
        };
        std::thread::sleep(Duration::from_millis(10));
        killer.send_kill_signal(KillSignal::QueryInterrupted);
        waiter.join().unwrap();

        // A subscriber arriving after the trigger returns immediately.
        killer.wait_kill_event();
    }

    #[test]
    fn reset_releases_existing_kill_event_waiters() {
        let killer = Arc::new(SqlKiller::default());
        let old_subscription = killer.get_kill_event();
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let waiter = {
            let subscription = old_subscription.clone();
            std::thread::spawn(move || {
                started_tx.send(()).unwrap();
                subscription.wait();
                done_tx.send(()).unwrap();
            })
        };

        started_rx.recv().unwrap();
        killer.reset();
        done_rx
            .recv_timeout(Duration::from_millis(100))
            .expect("reset must close the existing kill-event subscription");
        waiter.join().unwrap();
        assert!(old_subscription.is_ready());

        let new_subscription = killer.get_kill_event();
        assert!(!new_subscription.is_ready());
        killer.send_kill_signal(KillSignal::QueryInterrupted);
        new_subscription.wait();
    }

    #[test]
    fn connection_alive_gating() {
        let killer = SqlKiller::default();
        let alive = Arc::new(AtomicBool::new(true));
        let probe = Arc::clone(&alive);
        killer.set_is_connection_alive(Box::new(move || probe.load(SeqCst)));

        // First call only records the check time.
        assert!(killer.handle_signal().is_none());
        alive.store(false, SeqCst);
        // Under intest the interval is 1ms; wait past it.
        std::thread::sleep(Duration::from_millis(5));
        let err = killer.handle_signal().expect("dead connection kills");
        assert_eq!(err.code(), exeerrors::ERR_QUERY_INTERRUPTED.code());

        // The immediate check path.
        let killer = SqlKiller::default();
        killer.set_is_connection_alive(Box::new(|| false));
        killer.check_connection_alive();
        assert_eq!(killer.get_kill_signal(), Some(KillSignal::QueryInterrupted));
    }

    #[test]
    fn stale_connection_registration_cannot_clear_replacement() {
        let killer = SqlKiller::default();
        let old = killer.set_is_connection_alive(Box::new(|| true));
        let current = killer.set_is_connection_alive(Box::new(|| false));

        assert!(!killer.clear_is_connection_alive(old));
        killer.check_connection_alive();
        assert_eq!(killer.get_kill_signal(), Some(KillSignal::QueryInterrupted));

        killer.reset();
        assert!(killer.clear_is_connection_alive(current));
        killer.check_connection_alive();
        assert_eq!(killer.get_kill_signal(), None);
    }

    #[test]
    fn finish_func_lifecycle() {
        let killer = SqlKiller::default();
        let called = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&called);
        killer.set_finish_func(Box::new(move || {
            counter.fetch_add(1, SeqCst);
        }));
        killer.finish_result_set();
        killer.finish_result_set();
        assert_eq!(called.load(SeqCst), 2);
        killer.clear_finish_func();
        killer.finish_result_set();
        assert_eq!(called.load(SeqCst), 2);

        killer.in_write_result_set.store(true, SeqCst);
        assert!(killer.in_write_result_set.load(SeqCst));
        killer.in_write_result_set.store(false, SeqCst);
    }

    #[test]
    fn recovered_finish_panic_does_not_disable_the_killer() {
        let killer = SqlKiller::default();
        killer.set_finish_func(Box::new(|| panic!("finish failed")));

        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            killer.finish_result_set();
        }))
        .is_err());

        let called = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&called);
        killer.set_finish_func(Box::new(move || observed.store(true, SeqCst)));
        killer.finish_result_set();
        assert!(called.load(SeqCst));
    }
}
