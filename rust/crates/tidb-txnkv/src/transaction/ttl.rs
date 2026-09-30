// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Primary-lock keep-alive for transactions that outlive one statement.
//!
//! An optimistic transaction writes all its locks in one burst, so a TTL
//! computed at Prewrite covers the whole write phase. A pessimistic
//! transaction holds its first lock from the first locking statement until
//! commit — arbitrarily long, under the user's control — so the TTL written
//! into the lock must be extended while the transaction is alive. That is the
//! only job of this manager: extend the primary's TTL, and stop when the
//! transaction ends, when the lock is gone, or when the transaction has lived
//! longer than any lock may be trusted.
//!
//! Like client-go's ttlManager, each transaction owns a close channel, not a
//! thread. Timer waits share the execution scheduler. Synchronous timestamp
//! and heartbeat calls release the scheduler worker while they are blocked.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

/// TTL written into a managed lock, matching client-go's `ManagedLockTTL`.
pub const MANAGED_LOCK_TTL_MS: u64 = 20_000;
/// Maximum transaction lifetime a lock may be kept alive for, matching
/// client-go's `config.MaxTxnTTL` default of one hour.
pub const MAX_TXN_TTL_MS: u64 = 60 * 60 * 1_000;
/// Consecutive heartbeat failures tolerated before giving up.
pub const MAX_CONSECUTIVE_FAILURES: u32 = 10;

const TSO_LOGICAL_BITS: u32 = 18;

/// The timestamp and lock-refresh capabilities owned by a keep-alive task.
pub trait TxnHeartBeatSender {
    /// Returns a fresh real TSO used to measure transaction uptime.
    fn current_ts(&self) -> Result<u64, String>;

    /// Extends the primary lock's TTL, returning the TTL TiKV actually set.
    ///
    /// An `Err` that TiKV produced as a KeyError means the lock is gone and the
    /// heartbeat must stop; a transport error is retried.
    fn send_heart_beat(
        &mut self,
        primary: &[u8],
        start_ts: u64,
        advise_ttl_ms: u64,
    ) -> Result<u64, HeartBeatFailure>;
}

/// Why one heartbeat failed, which decides whether the loop may continue.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HeartBeatFailure {
    /// TiKV answered with a KeyError: the lock no longer exists to extend.
    Rejected(String),
    /// The attempt never produced a TiKV answer and may be retried.
    Transport(String),
}

impl std::fmt::Display for HeartBeatFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(detail) => write!(formatter, "TxnHeartBeat rejected: {detail}"),
            Self::Transport(detail) => write!(formatter, "TxnHeartBeat transport failed: {detail}"),
        }
    }
}

/// Truthful reason the keep-alive loop ended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeepAliveStop {
    /// The transaction closed the manager: the normal end.
    Closed,
    /// The transaction outlived [`MAX_TXN_TTL_MS`]; its locks may now expire,
    /// so it must no longer execute statements, only commit or roll back.
    LifetimeExceeded {
        /// Measured uptime in milliseconds at the moment the loop gave up.
        uptime_ms: u64,
    },
    /// TiKV reported the primary lock is gone.
    Rejected(String),
    /// The heartbeat failed [`MAX_CONSECUTIVE_FAILURES`] times in a row.
    ConsecutiveFailures(String),
    /// A fresh timestamp could not be allocated, so uptime is unknowable.
    TimestampFailed(String),
    /// The keep-alive session itself could not be created.
    SenderUnavailable(String),
}

/// Evidence retained after the keep-alive loop ends.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeepAliveReport {
    /// Number of heartbeats TiKV confirmed.
    pub confirmed_heart_beats: u64,
    /// TTL advised by the last confirmed heartbeat.
    pub last_advised_ttl_ms: u64,
    /// Why the loop ended.
    pub stop: KeepAliveStop,
}

/// Owner handle for one primary-lock keep-alive task.
///
/// Dropping the handle closes the loop, so a transaction that ends by any path
/// — commit, rollback, or unwinding — stops refreshing its lock.
pub struct LockKeepAlive {
    // Dropping the sender closes the receiver, including during unwinding.
    close: Option<oneshot::Sender<()>>,
    /// client-go's `lockCtx.LockExpired`, raised by the loop and read by the
    /// session BEFORE it runs the next statement; see [`Self::lock_expired`].
    lock_expired: Arc<AtomicBool>,
    worker: Option<JoinHandle<KeepAliveReport>>,
}

impl LockKeepAlive {
    /// Starts refreshing `primary`'s TTL until the returned handle is closed.
    ///
    /// The task owns its sender while sharing process-level transport and
    /// routing capabilities with other transactions.
    pub fn start<F, S>(
        primary: Vec<u8>,
        start_ts: u64,
        tick: Duration,
        make_sender: F,
    ) -> Result<Self, String>
    where
        F: FnOnce() -> Result<S, String> + Send + 'static,
        S: TxnHeartBeatSender + Send + 'static,
    {
        if primary.is_empty() {
            return Err("a keep-alive requires the transaction's primary key".to_owned());
        }
        if start_ts == 0 {
            return Err("a keep-alive requires a real nonzero start timestamp".to_owned());
        }
        if tick.is_zero() {
            return Err("a keep-alive requires a nonzero tick".to_owned());
        }
        let runtime = crate::rpc::execution_runtime()?;
        let (close, closed) = oneshot::channel();
        let lock_expired = Arc::new(AtomicBool::new(false));
        let task_expired = Arc::clone(&lock_expired);
        let worker = runtime.spawn(async move {
            keep_alive_loop(closed, &task_expired, make_sender, &primary, start_ts, tick).await
        });
        Ok(Self {
            close: Some(close),
            lock_expired,
            worker: Some(worker),
        })
    }

    /// client-go's `lockCtx.LockExpired`: whether this transaction has
    /// outlived [`MAX_TXN_TTL_MS`], so TiKV may already be letting other
    /// transactions resolve the locks it believes it holds.
    ///
    /// This is the ONE terminal reason client-go publishes to the session
    /// (`2pc.go`'s `ttlManager` sets the flag only under
    /// `uptime > maxTtl`, and only for a pessimistic transaction); a rejected
    /// or repeatedly failing heartbeat merely stops the loop, because the
    /// lock's own TTL has not necessarily run out. A session that reads
    /// `true` here must refuse every statement but `COMMIT` and `ROLLBACK`
    /// (Go `session.checkTxnAborted` -> `kv.ErrLockExpire`, 8229).
    ///
    /// It is a live flag rather than the [`KeepAliveReport`] that
    /// [`Self::close_and_wait`] answers, because the report arrives when the
    /// transaction ENDS and the whole point is to stop it executing before
    /// that.
    #[must_use]
    pub fn lock_expired(&self) -> bool {
        self.lock_expired.load(Ordering::Relaxed)
    }

    /// Signals the loop to stop without waiting for an in-flight request,
    /// matching client-go's ttlManager.close. Drop has the same behavior.
    pub fn close(self) {
        drop(self);
    }

    /// Stops the loop and explicitly waits for its final report. Validation
    /// and draining owners may await this; transaction close paths must not.
    pub async fn close_and_wait(mut self) -> KeepAliveReport {
        self.close.take();
        self.worker
            .take()
            .expect("a live handle always owns its worker")
            .await
            .expect("keep-alive task panicked")
    }
}

async fn keep_alive_loop<F, S>(
    mut closed: oneshot::Receiver<()>,
    lock_expired: &AtomicBool,
    make_sender: F,
    primary: &[u8],
    start_ts: u64,
    tick: Duration,
) -> KeepAliveReport
where
    F: FnOnce() -> Result<S, String>,
    S: TxnHeartBeatSender,
{
    let mut report = KeepAliveReport {
        confirmed_heart_beats: 0,
        last_advised_ttl_ms: 0,
        stop: KeepAliveStop::Closed,
    };
    let mut sender = match make_sender() {
        Ok(sender) => sender,
        Err(error) => {
            report.stop = KeepAliveStop::SenderUnavailable(error);
            return report;
        }
    };
    let mut ticker = tokio::time::interval_at(Instant::now() + tick, tick);
    // Go's ticker drops missed ticks rather than issuing a catch-up burst.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut consecutive_failures = 0_u32;
    loop {
        tokio::select! {
            biased;
            _ = &mut closed => break,
            _ = ticker.tick() => {}
        }
        let stop = tokio::task::block_in_place(|| {
            heartbeat_tick(
                &mut sender,
                lock_expired,
                primary,
                start_ts,
                &mut report,
                &mut consecutive_failures,
            )
        });
        if let Some(stop) = stop {
            report.stop = stop;
            break;
        }
    }
    report
}

fn heartbeat_tick<S: TxnHeartBeatSender>(
    sender: &mut S,
    lock_expired: &AtomicBool,
    primary: &[u8],
    start_ts: u64,
    report: &mut KeepAliveReport,
    consecutive_failures: &mut u32,
) -> Option<KeepAliveStop> {
    let now = match sender.current_ts() {
        Ok(now) => now,
        Err(error) => {
            return Some(KeepAliveStop::TimestampFailed(error));
        }
    };
    let uptime_ms = transaction_uptime_ms(start_ts, now);
    if uptime_ms > MAX_TXN_TTL_MS {
        // client-go `2pc.go`: "the pessimistic locks may expire if the ttl
        // manager has timed out, set `LockExpired` flag so that this
        // transaction could only commit or rollback with no more statement
        // executions". Raised BEFORE the loop returns, so the flag is
        // already up when the next statement reads it.
        lock_expired.store(true, Ordering::Relaxed);
        return Some(KeepAliveStop::LifetimeExceeded { uptime_ms });
    }
    let advised_ttl_ms = uptime_ms.saturating_add(MANAGED_LOCK_TTL_MS);
    match sender.send_heart_beat(primary, start_ts, advised_ttl_ms) {
        Ok(_) => {
            *consecutive_failures = 0;
            report.confirmed_heart_beats = report.confirmed_heart_beats.saturating_add(1);
            report.last_advised_ttl_ms = advised_ttl_ms;
        }
        Err(HeartBeatFailure::Rejected(detail)) => {
            return Some(KeepAliveStop::Rejected(detail));
        }
        Err(HeartBeatFailure::Transport(detail)) => {
            *consecutive_failures = consecutive_failures.saturating_add(1);
            if *consecutive_failures > MAX_CONSECUTIVE_FAILURES {
                return Some(KeepAliveStop::ConsecutiveFailures(detail));
            }
        }
    }
    None
}

/// Milliseconds between two TSO values, using only their physical halves.
fn transaction_uptime_ms(start_ts: u64, current_ts: u64) -> u64 {
    (current_ts >> TSO_LOGICAL_BITS).saturating_sub(start_ts >> TSO_LOGICAL_BITS)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;
    use std::time::Instant;

    use super::*;

    const TICK: Duration = Duration::from_millis(1);
    const START_TS: u64 = 1_000 << TSO_LOGICAL_BITS;

    /// Records every heartbeat and can script rejection or transport failure.
    struct ScriptedSender {
        clock_ms: u64,
        ts_calls: Arc<AtomicU64>,
        advised: Arc<Mutex<Vec<u64>>>,
        reject_after: u64,
        always_fail_transport: bool,
    }

    impl TxnHeartBeatSender for ScriptedSender {
        fn current_ts(&self) -> Result<u64, String> {
            self.ts_calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.clock_ms << TSO_LOGICAL_BITS)
        }

        fn send_heart_beat(
            &mut self,
            _primary: &[u8],
            _start_ts: u64,
            advise_ttl_ms: u64,
        ) -> Result<u64, HeartBeatFailure> {
            let attempt = {
                let mut advised = self.advised.lock().unwrap();
                advised.push(advise_ttl_ms);
                advised.len() as u64
            };
            if self.always_fail_transport {
                return Err(HeartBeatFailure::Transport("stream reset".to_owned()));
            }
            if self.reject_after > 0 && attempt > self.reject_after {
                return Err(HeartBeatFailure::Rejected("TxnLockNotFound".to_owned()));
            }
            Ok(advise_ttl_ms)
        }
    }

    /// Waits for an observable effect so the loop's terminal decision is already
    /// made before `close` is called; only then does `close` report that
    /// decision instead of `Closed`.
    fn wait_until(condition: impl Fn() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !condition() {
            assert!(Instant::now() < deadline, "keep-alive made no progress");
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn advised_ttl_tracks_measured_uptime_not_wall_clock() {
        // The advised TTL must always cover the time already elapsed, or a
        // long-running transaction's own lock expires under it.
        assert_eq!(
            transaction_uptime_ms(5 << TSO_LOGICAL_BITS, 905 << TSO_LOGICAL_BITS),
            900
        );
        // Physical-only arithmetic: logical bits must not leak into a duration.
        assert_eq!(
            transaction_uptime_ms(5 << TSO_LOGICAL_BITS, (5 << TSO_LOGICAL_BITS) + 999),
            0
        );
        // A TSO that went backwards cannot produce a wrapped, enormous uptime.
        assert_eq!(
            transaction_uptime_ms(900 << TSO_LOGICAL_BITS, 5 << TSO_LOGICAL_BITS),
            0
        );
    }

    #[test]
    fn a_rejected_heart_beat_stops_the_loop_because_the_lock_is_gone() {
        let advised = Arc::new(Mutex::new(Vec::new()));
        let sender = ScriptedSender {
            clock_ms: 1_000,
            ts_calls: Arc::new(AtomicU64::new(0)),
            advised: Arc::clone(&advised),
            reject_after: 1,
            always_fail_transport: false,
        };
        let keep_alive =
            LockKeepAlive::start(b"primary".to_vec(), START_TS, TICK, move || Ok(sender))
                .expect("keep-alive starts");
        wait_until(|| advised.lock().unwrap().len() >= 2);

        let report = futures::executor::block_on(keep_alive.close_and_wait());

        assert_eq!(report.confirmed_heart_beats, 1);
        // Uptime is zero at this clock, so the advised TTL is the managed base.
        assert_eq!(report.last_advised_ttl_ms, MANAGED_LOCK_TTL_MS);
        assert!(matches!(report.stop, KeepAliveStop::Rejected(_)));
    }

    #[test]
    fn transport_failures_are_retried_until_the_consecutive_budget_is_spent() {
        let advised = Arc::new(Mutex::new(Vec::new()));
        let sender = ScriptedSender {
            clock_ms: 1_000,
            ts_calls: Arc::new(AtomicU64::new(0)),
            advised: Arc::clone(&advised),
            reject_after: 0,
            always_fail_transport: true,
        };
        let keep_alive =
            LockKeepAlive::start(b"primary".to_vec(), START_TS, TICK, move || Ok(sender))
                .expect("keep-alive starts");
        wait_until(|| advised.lock().unwrap().len() as u64 > u64::from(MAX_CONSECUTIVE_FAILURES));

        let report = futures::executor::block_on(keep_alive.close_and_wait());

        assert_eq!(report.confirmed_heart_beats, 0);
        assert!(matches!(report.stop, KeepAliveStop::ConsecutiveFailures(_)));
        assert_eq!(
            advised.lock().unwrap().len() as u64,
            u64::from(MAX_CONSECUTIVE_FAILURES) + 1
        );
    }

    #[test]
    fn a_transaction_older_than_the_maximum_lifetime_stops_being_kept_alive() {
        let advised = Arc::new(Mutex::new(Vec::new()));
        let ts_calls = Arc::new(AtomicU64::new(0));
        let sender = ScriptedSender {
            clock_ms: 1_000 + MAX_TXN_TTL_MS + 1,
            ts_calls: Arc::clone(&ts_calls),
            advised: Arc::clone(&advised),
            reject_after: 0,
            always_fail_transport: false,
        };
        let keep_alive =
            LockKeepAlive::start(b"primary".to_vec(), START_TS, TICK, move || Ok(sender))
                .expect("keep-alive starts");
        wait_until(|| ts_calls.load(Ordering::SeqCst) >= 1);

        let report = futures::executor::block_on(keep_alive.close_and_wait());

        assert!(matches!(
            report.stop,
            KeepAliveStop::LifetimeExceeded { .. }
        ));
        // An expired transaction must not refresh a lock it no longer owns.
        assert!(advised.lock().unwrap().is_empty());
    }

    /// client-go's `lockCtx.LockExpired`: the ONE terminal reason that reaches
    /// the session while the transaction is still open.
    ///
    /// `2pc.go`'s `ttlManager` raises it under `uptime > maxTtl` and nowhere
    /// else -- a rejected heartbeat means the lock is already gone and a
    /// failing one means the network is, and in neither case has client-go
    /// concluded the transaction's locks may be resolved out from under it. So
    /// the flag has to be readable BEFORE `close`, and it has to stay down for
    /// the other three terminal reasons; a flag raised for all of them would
    /// abort transactions Go keeps running.
    #[test]
    fn only_the_lifetime_bound_raises_lock_expired_and_it_is_live() {
        let expired_after = |clock_ms: u64, reject_after: u64, fail: bool| {
            let advised = Arc::new(Mutex::new(Vec::new()));
            let ts_calls = Arc::new(AtomicU64::new(0));
            let sender = ScriptedSender {
                clock_ms,
                ts_calls: Arc::clone(&ts_calls),
                advised: Arc::clone(&advised),
                reject_after,
                always_fail_transport: fail,
            };
            let keep_alive =
                LockKeepAlive::start(b"primary".to_vec(), START_TS, TICK, move || Ok(sender))
                    .expect("keep-alive starts");
            wait_until(|| keep_alive.worker.as_ref().unwrap().is_finished());
            // Read WHILE the handle is alive: the session has to learn this
            // before the transaction ends, which is the whole point.
            let live = keep_alive.lock_expired();
            (
                live,
                futures::executor::block_on(keep_alive.close_and_wait()).stop,
            )
        };

        let (live, stop) = expired_after(1_000 + MAX_TXN_TTL_MS + 1, 0, false);
        assert!(matches!(stop, KeepAliveStop::LifetimeExceeded { .. }));
        assert!(live, "the session must be able to read this before close");

        let (live, stop) = expired_after(1_000, 1, false);
        assert!(matches!(stop, KeepAliveStop::Rejected(_)));
        assert!(!live, "a gone lock is not client-go's LockExpired");

        let (live, stop) = expired_after(1_000, 0, true);
        assert!(matches!(stop, KeepAliveStop::ConsecutiveFailures(_)));
        assert!(!live, "a failing transport is not client-go's LockExpired");
    }

    #[test]
    fn closing_a_healthy_keep_alive_is_the_normal_end() {
        let sender = ScriptedSender {
            clock_ms: 1_000,
            ts_calls: Arc::new(AtomicU64::new(0)),
            advised: Arc::new(Mutex::new(Vec::new())),
            reject_after: 0,
            always_fail_transport: false,
        };
        let keep_alive = LockKeepAlive::start(
            b"primary".to_vec(),
            START_TS,
            Duration::from_secs(3_600),
            move || Ok(sender),
        )
        .expect("keep-alive starts");

        assert_eq!(
            futures::executor::block_on(keep_alive.close_and_wait()).stop,
            KeepAliveStop::Closed
        );

        // client-go ttlManager.close closes a channel; it does not join an
        // in-flight timestamp/heartbeat request before releasing the owner.
        struct SlowSender {
            entered: std::sync::mpsc::SyncSender<()>,
            release: std::sync::mpsc::Receiver<()>,
        }
        impl TxnHeartBeatSender for SlowSender {
            fn current_ts(&self) -> Result<u64, String> {
                self.entered.send(()).unwrap();
                self.release.recv().unwrap();
                Ok(START_TS)
            }
            fn send_heart_beat(
                &mut self,
                _: &[u8],
                _: u64,
                ttl: u64,
            ) -> Result<u64, HeartBeatFailure> {
                Ok(ttl)
            }
        }
        let (entered, started) = std::sync::mpsc::sync_channel(1);
        let (release, released) = std::sync::mpsc::sync_channel(1);
        let keep_alive = LockKeepAlive::start(b"primary".to_vec(), START_TS, TICK, move || {
            Ok(SlowSender {
                entered,
                release: released,
            })
        })
        .unwrap();
        started.recv_timeout(Duration::from_secs(10)).unwrap();
        let (finished, closed) = std::sync::mpsc::sync_channel(1);
        let owner = std::thread::spawn(move || {
            drop(keep_alive);
            finished.send(()).unwrap();
        });
        let closed_without_rpc = closed.recv_timeout(Duration::from_secs(1)).is_ok();
        release.send(()).unwrap();
        owner.join().unwrap();
        assert!(
            closed_without_rpc,
            "Go closes the manager without waiting for its RPC"
        );
    }

    #[test]
    fn a_failed_session_is_reported_instead_of_silently_not_refreshing() {
        let keep_alive = LockKeepAlive::start(b"primary".to_vec(), START_TS, TICK, || {
            Err::<ScriptedSender, _>("no TiKV session".to_owned())
        })
        .expect("keep-alive starts");

        assert!(matches!(
            futures::executor::block_on(keep_alive.close_and_wait()).stop,
            KeepAliveStop::SenderUnavailable(_)
        ));
    }

    #[test]
    fn a_keep_alive_without_a_primary_start_ts_or_tick_is_refused() {
        let healthy = || {
            Ok(ScriptedSender {
                clock_ms: 1_000,
                ts_calls: Arc::new(AtomicU64::new(0)),
                advised: Arc::new(Mutex::new(Vec::new())),
                reject_after: 0,
                always_fail_transport: false,
            })
        };
        assert!(LockKeepAlive::start(Vec::new(), START_TS, TICK, healthy).is_err());
        assert!(LockKeepAlive::start(b"p".to_vec(), 0, TICK, healthy).is_err());
        assert!(LockKeepAlive::start(b"p".to_vec(), START_TS, Duration::ZERO, healthy).is_err());
    }
}
