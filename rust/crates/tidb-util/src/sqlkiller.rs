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
//! - Go's `chan struct{}` close-broadcast kill event becomes one native
//!   receiver per caller. Both a kill and `Reset` release every receiver,
//!   while a receiver created after reset waits for the next event.
//! - The `Signal` CAS (`0 -> reason`, first signal wins) is the same
//!   `compare_exchange` on an `AtomicU32`.
//! - `logutil.BgLogger()` lines map to `tracing`; Go's `randomPanic`
//!   injection is available under the crate's `failpoints` feature.
//! - Function-pointer fields (`Finish`, `IsConnectionAlive`) become guarded
//!   callback slots; liveness registration tokens preserve the source's
//!   conditional compare-and-swap removal.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, Sender};
use tidb_error::mysql::FormatArg;
use tidb_error::terror::TerrorError;

use crate::dbterror::exeerrors;
use crate::intest;

fn lock_unpoison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Go's raw `uint32` kill signal.
///
/// The source field is public and may contain values outside the named
/// constants, so this remains a transparent value instead of a closed enum.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(transparent)]
pub struct KillSignal(pub u32);

#[allow(non_upper_case_globals)]
impl KillSignal {
    /// No kill requested.
    pub const UnspecifiedKillSignal: Self = Self(0);
    /// `KILL QUERY` / connection dead.
    pub const QueryInterrupted: Self = Self(1);
    /// `max_execution_time` exceeded.
    pub const MaxExecTimeExceeded: Self = Self(2);
    /// Per-query memory quota exceeded.
    pub const QueryMemoryExceeded: Self = Self(3);
    /// Server memory limit exceeded.
    pub const ServerMemoryExceeded: Self = Self(4);
    /// Runaway-query watchdog.
    pub const RunawayQueryExceeded: Self = Self(5);
    /// Killed by the memory arbitrator.
    pub const KilledByMemArbitrator: Self = Self(6);

    /// Returns the source `uint32` representation.
    #[must_use]
    pub const fn raw(self) -> u32 {
        self.0
    }
}

#[derive(Default)]
struct KillEventState {
    triggered: bool,
    desc: String,
}

#[derive(Default)]
struct KillEventShared {
    state: Mutex<KillEventState>,
    waiters: Mutex<Vec<Sender<()>>>,
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
    /// Go `Signal`, shared directly with storage request cancellation.
    pub signal: AtomicU32,
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
    #[cfg(feature = "failpoints")]
    fn inject_random_panic(&self) {
        let _ = fail::eval("randomPanic", |value| {
            let Some(probability) = value.and_then(|value| value.parse::<i64>().ok()) else {
                return;
            };
            let random = ((u64::from(crate::fastrand::uint32()) << 31)
                | u64::from(crate::fastrand::uint32() >> 1)) as f64
                / (1_u64 << 63) as f64;
            if random <= probability as f64 / 1000.0 || self.conn_id.load(SeqCst) == 0 {
                return;
            }

            let status = loop {
                let value = crate::fastrand::uint32() >> 1;
                const MAX_MULTIPLE_OF_FIVE: u32 = i32::MAX as u32 - (1_u32 << 31) % 5;
                if value <= MAX_MULTIPLE_OF_FIVE {
                    break value % 5;
                }
            };
            self.signal.store(status, SeqCst);
        });
    }

    /// Returns a receiver released when the current statement is killed or
    /// reset (Go `GetKillEventChan`).
    pub fn get_kill_event_chan(&self) -> Receiver<()> {
        let (tx, rx) = bounded(1);
        let state = lock_unpoison(&self.kill_event.state);
        if state.triggered {
            let _ = tx.send(());
        } else {
            lock_unpoison(&self.kill_event.waiters).push(tx);
        }
        rx
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
    }

    fn reset_kill_event(&self) {
        let mut state = lock_unpoison(&self.kill_event.state);
        state.triggered = false;
        state.desc.clear();
        for waiter in lock_unpoison(&self.kill_event.waiters).drain(..) {
            let _ = waiter.send(());
        }
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
            .compare_exchange(0, reason.raw(), SeqCst, SeqCst)
            .is_ok()
        {
            let status = self.signal.load(SeqCst);
            let err = self
                .kill_error(status)
                .expect("a newly installed kill signal must map to an error");
            tracing::warn!(
                connection_id = self.conn_id.load(SeqCst),
                reason = %err,
                "kill initiated"
            );
        }
    }

    /// Sends a kill signal to the query (Go `SendKillSignal`).
    pub fn send_kill_signal(&self, reason: KillSignal) {
        self.send_kill_signal_inner(reason);
        self.trigger_kill_event();
    }

    /// Gets the current kill signal.
    pub fn get_kill_signal(&self) -> KillSignal {
        KillSignal(self.signal.load(SeqCst))
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
        Some(match KillSignal(status) {
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
            _ => return None,
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
        #[cfg(feature = "failpoints")]
        self.inject_random_panic();

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
            let last = *lock_unpoison(&self.last_check_time);
            let should_check = match last {
                None => {
                    *lock_unpoison(&self.last_check_time) = Some(now);
                    false
                }
                Some(prev)
                    if now
                        .checked_duration_since(prev)
                        .is_some_and(|elapsed| elapsed > check_dur) =>
                {
                    *lock_unpoison(&self.last_check_time) = Some(now);
                    true
                }
                _ => false,
            };
            if should_check && !fn_alive() {
                self.send_kill_signal_inner(KillSignal::QueryInterrupted);
            }
        }

        let status = self.signal.load(SeqCst);
        let err = self.kill_error(status);
        if status == KillSignal::ServerMemoryExceeded.raw() {
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
