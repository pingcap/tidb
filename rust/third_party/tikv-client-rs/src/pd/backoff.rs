// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! PD's shared `pkg/retry` owner. This is distinct from client-go's KV retry budget.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{Level, Log, Metadata, Record};
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::trace::TraceContext;
use crate::{Error, Result};

type RetryCheck = dyn Fn(&Error) -> bool + Send + Sync;
type RetryableChecker = Box<RetryCheck>;

/// A reusable constructor option, applied in order as in Go's `Option`.
pub type BackofferOption = Arc<dyn Fn(&mut Backoffer) + Send + Sync>;

#[cfg(test)]
fn with_min_log_interval(interval: i64) -> BackofferOption {
    Arc::new(move |bo| bo.log_interval = interval)
}

/// Exponential wait policy, reset after each execution, including a dropped future.
/// All durations are signed nanoseconds, matching Go's `time.Duration`.
/// Cloning copies execution state but shares the callback's captured environment,
/// as the value copy in PD's per-RPC backoffer interceptor does.
#[derive(Clone)]
pub struct Backoffer {
    base: i64,
    max: i64,
    total: i64,
    retryable_checker: Option<Arc<RetryCheck>>,
    log_interval: i64,
    next_log_time: i64,
    attempt: isize,
    next: i64,
    current_total: i64,
}

impl Backoffer {
    /// Initialize Go's exponential policy in signed nanoseconds. A non-positive
    /// total has no wait budget; a non-positive interval is immediately eligible.
    pub fn new(base: i64, max: i64, total: i64) -> Self {
        Self::new_with_options(base, max, total, &[])
    }

    /// Go `InitialBackoffer`, including ordered, reusable caller options.
    pub fn new_with_options(base: i64, max: i64, total: i64, options: &[BackofferOption]) -> Self {
        let base = base.min(max);
        let total = if total > 0 && total < base {
            base
        } else {
            total
        };
        let mut backoffer = Self {
            base,
            max,
            total,
            retryable_checker: None,
            log_interval: 0,
            next_log_time: 0,
            attempt: 0,
            next: base,
            current_total: 0,
        };
        for option in options {
            option(&mut backoffer);
        }
        backoffer
    }

    /// Preserve a supplied checker unless overwrite is requested; None clears it.
    pub fn set_retryable_checker(&mut self, checker: Option<RetryableChecker>, overwrite: bool) {
        if overwrite || self.retryable_checker.is_none() {
            self.retryable_checker = checker.map(Arc::from);
        }
    }

    /// Run at least once, checking cancellation only while waiting after a
    /// retryable failure. `context_done` yields the owning context's error,
    /// preserving cancellation/deadline identity without inventing a new context.
    /// Only completed waits consume the budget; operation time does not.
    pub async fn exec<C, F, Fut>(&mut self, context_done: C, f: F) -> Result<()>
    where
        C: Future<Output = Error>,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        self.exec_with_logger(context_done, f, log::logger()).await
    }

    async fn exec_with_logger<C, F, Fut>(
        &mut self,
        context_done: C,
        mut f: F,
        logger: &dyn Log,
    ) -> Result<()>
    where
        C: Future<Output = Error>,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let reset = ResetOnDrop(self);
        let bo = &mut *reset.0;
        tokio::pin!(context_done);
        // Go creates no timer on immediate success or a non-retryable error.
        // Pin its optional storage once, then reuse it after the first failure.
        let timer = None::<tokio::time::Sleep>;
        tokio::pin!(timer);
        loop {
            let result = f().await;
            bo.attempt = bo.attempt.wrapping_add(1);
            let error = match result {
                Ok(()) => return Ok(()),
                Err(error) => error,
            };
            if bo
                .retryable_checker
                .as_ref()
                .is_some_and(|check| !check(&error))
            {
                return Err(error);
            }
            let interval = bo.next_interval();
            bo.next_log_time = bo.next_log_time.wrapping_add(interval);
            if bo.log_interval > 0 && bo.next_log_time >= bo.log_interval {
                bo.next_log_time %= bo.log_interval;
                let metadata = Metadata::builder()
                    .level(Level::Warn)
                    .target(module_path!())
                    .build();
                if logger.enabled(&metadata) {
                    logger.log(&Record::builder().metadata(metadata).args(format_args!(
                        "[pd.backoffer] exec fn failed and retrying [fn-name={}] [retry-time={}] [error={}]",
                        function_name::<F>(), bo.attempt, error,
                    )).build());
                }
            }
            // Go Timer treats non-positive durations as immediately ready.
            let deadline = Instant::now() + Duration::from_nanos(interval.max(0) as u64);
            if let Some(timer) = timer.as_mut().as_pin_mut() {
                timer.reset(deadline);
            } else {
                timer.set(Some(tokio::time::sleep_until(deadline)));
            }
            tokio::select! {
                error = &mut context_done => return Err(error),
                _ = timer.as_mut().as_pin_mut().expect("retry timer initialized") => {
                    let _ = fail::eval("backOffExecute", |_| {
                        BACKOFF_EXECUTED.store(true, Ordering::SeqCst);
                    });
                }
            }
            if bo.total > 0 {
                bo.current_total = bo.current_total.wrapping_add(interval);
                if bo.current_total >= bo.total {
                    return Err(error);
                }
            }
        }
    }

    fn next_interval(&mut self) -> i64 {
        let interval = if self.total > 0 && self.current_total.wrapping_add(self.next) > self.total
        {
            self.total.wrapping_sub(self.current_total)
        } else {
            self.next
        };
        self.next = self.next.wrapping_mul(2);
        if self.next > self.max {
            self.next = self.max;
        }
        interval
    }

    fn reset(&mut self) {
        self.next = self.base;
        self.current_total = 0;
        self.attempt = 0;
        self.next_log_time = 0;
    }
}

struct ResetOnDrop<'a>(&'a mut Backoffer);
impl Drop for ResetOnDrop<'_> {
    fn drop(&mut self) {
        self.0.reset();
    }
}

fn duration_remainder(value: Duration, divisor: Duration) -> Duration {
    let nanos = value.as_nanos() % divisor.as_nanos();
    Duration::new(
        (nanos / 1_000_000_000) as u64,
        (nanos % 1_000_000_000) as u32,
    )
}

fn function_name<F>() -> &'static str {
    std::any::type_name::<F>().rsplit("::").next().unwrap_or("")
}

static BACKOFF_EXECUTED: AtomicBool = AtomicBool::new(false);

/// Source failpoint probe; false until `backOffExecute` fires after a wait.
#[doc(hidden)]
pub fn test_backoff_execute() -> bool {
    BACKOFF_EXECUTED.load(Ordering::SeqCst)
}

/// A caller may attach one backoffer to its existing context for PD APIs.
pub type SharedBackoffer = Arc<Mutex<Backoffer>>;
struct BackofferKey;

/// Go `FromContext`, including absent context and explicitly nil backoffers.
pub fn from_context(context: Option<&TraceContext>) -> Option<SharedBackoffer> {
    context?
        .value::<BackofferKey, Option<SharedBackoffer>>()?
        .clone()
}

/// Derive a context while preserving its other values and backoffer identity.
pub fn with_backoffer(context: &TraceContext, backoffer: Option<SharedBackoffer>) -> TraceContext {
    context.with_value::<BackofferKey, _>(backoffer)
}

/// Retry on Go's fixed ticker: ten attempts at 500ms intervals.
pub async fn with_config<C, F, Fut>(context_done: C, f: F) -> Result<()>
where
    C: Future<Output = Error>,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    retry(context_done, 10, 500_000_000, f).await
}

/// Fixed-interval retry, including the final failed attempt's wait. A canceled
/// context returns the operation's last error, unlike [`Backoffer::exec`].
/// Non-positive attempts succeed without invoking the operation. A non-positive interval panics
/// like Go's NewTicker. Slow operations do not add a fresh interval to each retry.
pub async fn retry<C, F, Fut>(
    context_done: C,
    max_times: isize,
    interval: i64,
    mut f: F,
) -> Result<()>
where
    C: Future<Output = Error>,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    assert!(interval > 0, "non-positive interval for ticker");
    let interval = Duration::from_nanos(interval as u64);
    let mut next_tick = Instant::now() + interval;
    let ticker = tokio::time::sleep_until(next_tick);
    tokio::pin!(ticker, context_done);
    let mut last_error = None;
    for _ in 0..max_times {
        let error = match f().await {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        tokio::select! {
            _ = &mut context_done => return Err(error),
            _ = &mut ticker => {}
        }
        // Go's ticker advances to the next original-phase deadline, dropping
        // missed ticks. Tokio Interval permits catch-up bursts below 5ms even
        // in Skip mode, so use Go's scheduling formula with one reusable timer.
        let now = Instant::now();
        let overdue = now.saturating_duration_since(next_tick);
        next_tick = now + interval - duration_remainder(overdue, interval);
        ticker.as_mut().reset(next_tick);
        last_error = Some(error);
    }
    last_error.map_or(Ok(()), Err)
}

#[cfg(test)]
#[path = "backoff_tests.rs"]
mod tests;
