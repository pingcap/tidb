// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.
//! Go `pkg/ingestor/ingestctrl/rate_limiter.go`.
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tidb_gocontext::{Context, CtxError};

const RATE_PER_SEC_MULTIPLIER: usize = 1000;
struct Waiter {
    weight: usize,
    ready: crossbeam_channel::Sender<()>,
}
struct StoreState {
    used: usize,
    waiters: VecDeque<Waiter>,
    tokens: f64,
    last: Instant,
}
struct PerStore {
    state: Mutex<StoreState>,
}
/// Go `ingestLimiter` with independent limits per store.
pub struct IngestLimiter {
    ctx: Context,
    max_req_in_flight: usize,
    max_req_per_sec: f64,
    limiters: Mutex<HashMap<u64, Arc<PerStore>>>,
}
/// Go `ingestLimiter.Acquire` error.
#[derive(Debug, PartialEq, Eq)]
pub enum AcquireError {
    /// Go context cancellation/deadline.
    Context(CtxError),
    /// Go `rate.WaitN` rejects requests larger than its burst.
    BurstExceeded,
}
impl std::fmt::Display for AcquireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Context(e) => e.fmt(f),
            Self::BurstExceeded => f.write_str("rate: Wait(n) exceeds limiter's burst"),
        }
    }
}
impl std::error::Error for AcquireError {}
impl IngestLimiter {
    /// Go `newIngestLimiter`.
    pub fn new(ctx: Context, max_req_in_flight: usize, max_req_per_sec: f64) -> Self {
        Self {
            ctx,
            max_req_in_flight,
            max_req_per_sec,
            limiters: Mutex::new(HashMap::new()),
        }
    }
    /// Go `ingestLimiter.NoLimit`.
    pub fn no_limit(&self) -> bool {
        self.max_req_in_flight == 0 && self.max_req_per_sec == 0.0
    }
    /// Go `ingestLimiter.Burst`.
    pub fn burst(&self) -> usize {
        if self.no_limit() {
            return isize::MAX as usize;
        }
        if self.max_req_in_flight == 0 {
            return get_rate_burst(self.max_req_per_sec);
        }
        if self.max_req_per_sec == 0.0 {
            return self.max_req_in_flight;
        }
        get_rate_burst(self.max_req_per_sec).min(self.max_req_in_flight)
    }
    /// Go `ingestLimiter.Acquire`, rate reservation followed by FIFO weighted
    /// semaphore acquisition. Cancellation restores unconsumed reservations.
    pub fn acquire(&self, store_id: u64, n: usize) -> Result<(), AcquireError> {
        if self.no_limit() {
            return Ok(());
        }
        let store = self
            .limiters
            .lock()
            .unwrap()
            .entry(store_id)
            .or_insert_with(|| {
                Arc::new(PerStore {
                    state: Mutex::new(StoreState {
                        used: 0,
                        waiters: VecDeque::new(),
                        tokens: (get_rate_burst(self.max_req_per_sec) * RATE_PER_SEC_MULTIPLIER)
                            as f64,
                        last: Instant::now(),
                    }),
                })
            })
            .clone();
        if let Some(e) = self.ctx.err() {
            return Err(AcquireError::Context(e));
        }
        if self.max_req_per_sec > 0.0 {
            let burst = (get_rate_burst(self.max_req_per_sec) * RATE_PER_SEC_MULTIPLIER) as f64;
            let weight = (n * RATE_PER_SEC_MULTIPLIER) as f64;
            if weight > burst {
                return Err(AcquireError::BurstExceeded);
            }
            let rate = (self.max_req_per_sec * RATE_PER_SEC_MULTIPLIER as f64)
                .trunc()
                .max(1.0);
            let delay = {
                let mut s = store.state.lock().unwrap();
                let now = Instant::now();
                s.tokens = (s.tokens + now.duration_since(s.last).as_secs_f64() * rate).min(burst)
                    - weight;
                s.last = now;
                Duration::from_secs_f64((-s.tokens / rate).max(0.0))
            };
            if !delay.is_zero() && self.ctx.sleep(delay) {
                let mut s = store.state.lock().unwrap();
                let now = Instant::now();
                s.tokens = (s.tokens + now.duration_since(s.last).as_secs_f64() * rate + weight)
                    .min(burst);
                s.last = now;
                return Err(AcquireError::Context(
                    self.ctx.err().unwrap_or(CtxError::Canceled),
                ));
            }
        }
        if self.max_req_in_flight == 0 {
            return Ok(());
        }
        let (ready, wait) = crossbeam_channel::bounded(1);
        {
            let mut s = store.state.lock().unwrap();
            if n <= self.max_req_in_flight.saturating_sub(s.used) && s.waiters.is_empty() {
                s.used += n;
                return Ok(());
            }
            // Go semaphore waits for cancellation without blocking the queue
            // when the request exceeds total capacity.
            if n <= self.max_req_in_flight {
                s.waiters.push_back(Waiter {
                    weight: n,
                    ready: ready.clone(),
                });
            }
        }
        crossbeam_channel::select! {
            recv(wait) -> _ => Ok(()),
            recv(self.ctx.done()) -> _ => {
                let mut s = store.state.lock().unwrap();
                if let Some(i) = s.waiters.iter().position(|w| w.ready.same_channel(&ready)) { s.waiters.remove(i); }
                else if wait.try_recv().is_ok() { s.used -= n; }
                self.notify_waiters(&mut s);
                Err(AcquireError::Context(self.ctx.err().unwrap_or(CtxError::Canceled)))
            }
        }
    }
    /// Go `ingestLimiter.Release`.
    pub fn release(&self, store_id: u64, n: usize) {
        if self.max_req_in_flight == 0 {
            return;
        }
        let Some(store) = self.limiters.lock().unwrap().get(&store_id).cloned() else {
            return;
        };
        let mut s = store.state.lock().unwrap();
        s.used = s
            .used
            .checked_sub(n)
            .expect("semaphore: released more than held");
        self.notify_waiters(&mut s);
    }
    /// Go `semaphore.Weighted.notifyWaiters`.
    fn notify_waiters(&self, s: &mut StoreState) {
        while s
            .waiters
            .front()
            .is_some_and(|w| w.weight <= self.max_req_in_flight - s.used)
        {
            let w = s.waiters.pop_front().unwrap();
            s.used += w.weight;
            let _ = w.ready.send(());
        }
    }
}
/// Go `getRateBurst`.
pub fn get_rate_burst(rate_per_sec: f64) -> usize {
    (rate_per_sec.ceil() as usize).max(1)
}

#[cfg(test)]
mod tests {
    use super::*;
    /// Go `rate_limiter_test.go:TestIngestLimiterBurst`.
    #[test]
    fn test_ingest_limiter_burst() {
        for (concurrency, rate, burst) in [
            (0, 0.0, isize::MAX as usize),
            (0, 1000.0, 1000),
            (1000, 0.0, 1000),
            (1000, 1000.0, 1000),
            (1000, 567.0, 567),
            (567, 1000.0, 567),
        ] {
            let l = IngestLimiter::new(Context::background(), concurrency, rate);
            assert_eq!(l.burst(), burst);
            assert_eq!(l.no_limit(), concurrency == 0 && rate == 0.0);
        }
    }
    /// Go `rate_limiter_test.go:TestConcurrencyLimit`.
    #[test]
    fn test_concurrency_limit() {
        let l = Arc::new(IngestLimiter::new(Context::background(), 1000, 1000.0));
        l.acquire(0, 1000).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = l.clone();
        let thread = std::thread::spawn(move || {
            worker.acquire(0, 1).unwrap();
            tx.send(()).unwrap();
        });
        assert!(rx.recv_timeout(Duration::from_millis(100)).is_err());
        l.release(0, 1);
        rx.recv_timeout(Duration::from_millis(100)).unwrap();
        thread.join().unwrap();
        l.release(0, 1000);
        l.acquire(1, 1000).unwrap();
        l.acquire(2, 1).unwrap();
        l.release(1, 1000);
        l.release(2, 1);
    }
    /// Go `rate_limiter_test.go:TestRateLimit`.
    #[test]
    fn test_rate_limit() {
        let l = IngestLimiter::new(Context::background(), 10, 10.0);
        let start = Instant::now();
        for _ in 0..10 {
            l.acquire(0, 1).unwrap();
        }
        assert!(start.elapsed() < Duration::from_millis(50));
        l.release(0, 10);
        let start = Instant::now();
        l.acquire(0, 1).unwrap();
        assert!(start.elapsed() >= Duration::from_millis(90));
        assert!(start.elapsed() < Duration::from_millis(150));
        l.acquire(1, 10).unwrap();
        l.release(1, 10);
        let start = Instant::now();
        l.acquire(2, 10).unwrap();
        assert!(start.elapsed() < Duration::from_millis(50));
        l.release(2, 10);
    }
    fn cancellation(concurrency: usize, rate: f64) {
        let (ctx, cancel) = Context::with_cancel(&Context::background());
        let l = Arc::new(IngestLimiter::new(ctx, concurrency, rate));
        l.acquire(0, 1).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let worker = l.clone();
        let thread = std::thread::spawn(move || tx.send(worker.acquire(0, 1)).unwrap());
        cancel.cancel();
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(100)).unwrap(),
            Err(AcquireError::Context(CtxError::Canceled))
        );
        thread.join().unwrap();
        l.release(0, 1);
    }
    /// Go `rate_limiter_test.go:TestContextCancelDuringConcurrencyWait`.
    #[test]
    fn test_context_cancel_during_concurrency_wait() {
        cancellation(1, 100.0);
    }
    /// Go `rate_limiter_test.go:TestContextCancelDuringRateWait`.
    #[test]
    fn test_context_cancel_during_rate_wait() {
        cancellation(100, 1.0);
    }
}
