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
//! - Go's `chan struct{}` close-broadcast kill event becomes a
//!   `Mutex`-guarded state with a `Condvar` broadcast: pollable via
//!   [`SqlKiller::kill_event_triggered`] and blockable via
//!   [`SqlKiller::wait_kill_event`] — the same one-shot,
//!   already-triggered-means-immediately-ready semantics as a closed
//!   channel.
//! - The `Signal` CAS (`0 -> reason`, first signal wins) is the same
//!   `compare_exchange` on an `AtomicU32`.
//! - `logutil.BgLogger()` lines map to `tracing`; the `failpoint`
//!   random-panic injection is Go test machinery with no runtime behavior
//!   and is not ported.
//! - Function-pointer fields (`Finish`, `IsConnectionAlive`) become
//!   `Mutex<Option<Box<dyn Fn ...>>>`.
//!
//! The Go package ships no test; the tests below pin the contract:
//! first-signal-wins, kill-event trigger/reset/late-subscribe semantics,
//! the exact error mapping (messages come from the fixture-verified
//! `exeerrors` table), connection-alive interval gating (1s, 1ms under
//! `intest`), and `Reset`.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::SeqCst};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use tidb_error::mysql::FormatArg;
use tidb_error::terror::TerrorError;

use crate::dbterror::exeerrors;
use crate::intest;

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
struct KillEvent {
    triggered: bool,
    desc: String,
    // Go tracks whether anyone made the channel; here subscription is
    // implicit in the Condvar, so only the trigger state matters.
}

type AliveFn = Box<dyn Fn() -> bool + Send + Sync>;
type FinishFn = Box<dyn Fn() + Send + Sync>;

/// Kills a query (Go `SQLKiller`).
#[derive(Default)]
pub struct SqlKiller {
    signal: AtomicU32,
    /// The connection ID.
    pub conn_id: AtomicU64,
    kill_event: Mutex<KillEvent>,
    kill_event_cond: Condvar,
    finish: Mutex<Option<FinishFn>>,
    is_connection_alive: Mutex<Option<AliveFn>>,
    last_check_time: Mutex<Option<Instant>>,
}

impl SqlKiller {
    /// Whether the kill event has been triggered (a closed Go channel).
    pub fn kill_event_triggered(&self) -> bool {
        self.kill_event.lock().unwrap().triggered
    }

    /// Blocks until the kill event triggers (receiving on Go's channel); an
    /// already-triggered event returns immediately.
    pub fn wait_kill_event(&self) {
        let mut event = self.kill_event.lock().unwrap();
        while !event.triggered {
            event = self.kill_event_cond.wait(event).unwrap();
        }
    }

    fn trigger_kill_event(&self) {
        let mut event = self.kill_event.lock().unwrap();
        if event.triggered {
            return;
        }
        event.triggered = true;
        self.kill_event_cond.notify_all();
    }

    fn reset_kill_event(&self) {
        let mut event = self.kill_event.lock().unwrap();
        // Go closes a still-open channel so pending receivers wake.
        event.triggered = false;
        event.desc.clear();
        self.kill_event_cond.notify_all();
    }

    /// Sets the kill-event reason and sends the signal (Go
    /// `SendKillSignalWithKillEventReason`).
    pub fn send_kill_signal_with_reason(&self, signal: KillSignal, desc: &str) {
        self.kill_event.lock().unwrap().desc = desc.to_string();
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
        self.kill_event.lock().unwrap().desc.clone()
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
                &[FormatArg::from(conn_id as i64)],
            ),
            KillSignal::ServerMemoryExceeded => by_args(
                &exeerrors::ERR_MEMORY_EXCEED_FOR_INSTANCE,
                &[FormatArg::from(conn_id as i64)],
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
                    FormatArg::from(conn_id as i64),
                ],
            ),
        })
    }

    /// Closes the result set to release resources when a killed query is
    /// stuck writing to the client (Go `FinishResultSet`).
    pub fn finish_result_set(&self) {
        let finish = self.finish.lock().unwrap();
        if let Some(f) = finish.as_ref() {
            f();
        }
    }

    /// Sets the finish function.
    pub fn set_finish_func(&self, f: FinishFn) {
        *self.finish.lock().unwrap() = Some(f);
    }

    /// Clears the finish function.
    pub fn clear_finish_func(&self) {
        *self.finish.lock().unwrap() = None;
    }

    /// Installs the connection-liveness probe.
    pub fn set_is_connection_alive(&self, f: AliveFn) {
        *self.is_connection_alive.lock().unwrap() = Some(f);
    }

    /// Handles the kill signal, returning the pending kill error (Go
    /// `HandleSignal`). Also polls connection liveness at most once per
    /// second (1ms under `intest`), like the source.
    pub fn handle_signal(&self) -> Option<TerrorError> {
        {
            let alive = self.is_connection_alive.lock().unwrap();
            if let Some(fn_alive) = alive.as_ref() {
                let check_dur = if intest::IN_TEST {
                    Duration::from_millis(1)
                } else {
                    Duration::from_secs(1)
                };
                let now = Instant::now();
                let mut last = self.last_check_time.lock().unwrap();
                match *last {
                    None => *last = Some(now),
                    Some(prev) if now.duration_since(prev) > check_dur => {
                        *last = Some(now);
                        if !fn_alive() {
                            self.send_kill_signal_inner(KillSignal::QueryInterrupted);
                        }
                    }
                    _ => {}
                }
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
        let alive = self.is_connection_alive.lock().unwrap();
        if let Some(fn_alive) = alive.as_ref() {
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
        *self.last_check_time.lock().unwrap() = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

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
    fn kill_error_mapping() {
        let killer = SqlKiller::default();
        killer.conn_id.store(42, SeqCst);

        killer.send_kill_signal(KillSignal::QueryInterrupted);
        let err = killer.handle_signal().expect("kill pending");
        assert_eq!(err.code(), exeerrors::ERR_QUERY_INTERRUPTED.code());

        killer.reset();
        killer.send_kill_signal_with_reason(KillSignal::KilledByMemArbitrator, "oom risk");
        let err = killer.handle_signal().expect("kill pending");
        assert_eq!(err.code(), exeerrors::ERR_QUERY_EXEC_STOPPED.code());
        assert!(err.message().contains("oom risk"), "{}", err.message());
        assert!(err.message().contains("42"), "{}", err.message());

        killer.reset();
        killer.send_kill_signal(KillSignal::QueryMemoryExceeded);
        let err = killer.handle_signal().expect("kill pending");
        assert!(err.message().contains("42"), "{}", err.message());

        killer.reset();
        assert!(killer.handle_signal().is_none());
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
    }
}
