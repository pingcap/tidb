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

type RetryableChecker = Box<dyn Fn(&Error) -> bool + Send + Sync>;

/// Exponential wait policy, reset after each execution, including a dropped future.
pub struct Backoffer {
    base: Duration,
    max: Duration,
    total: Duration,
    retryable_checker: Option<RetryableChecker>,
    log_interval: Duration,
    next_log_time: Duration,
    attempt: usize,
    next: Duration,
    current_total: Duration,
}

impl Backoffer {
    /// Initialize Go's bounded exponential policy. Zero total means no wait budget.
    pub fn new(base: Duration, max: Duration, total: Duration) -> Self {
        let base = base.min(max);
        let total = if total.is_zero() {
            total
        } else {
            total.max(base)
        };
        Self {
            base,
            max,
            total,
            retryable_checker: None,
            log_interval: Duration::ZERO,
            next_log_time: Duration::ZERO,
            attempt: 0,
            next: base,
            current_total: Duration::ZERO,
        }
    }

    /// Configure the package's minimum warning interval.
    #[doc(hidden)]
    pub fn with_min_log_interval(mut self, interval: Duration) -> Self {
        self.log_interval = interval;
        self
    }

    /// Preserve a supplied checker unless overwrite is requested; None clears it.
    pub fn set_retryable_checker(&mut self, checker: Option<RetryableChecker>, overwrite: bool) {
        if overwrite || self.retryable_checker.is_none() {
            self.retryable_checker = checker;
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
            bo.attempt += 1;
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
            bo.next_log_time = bo.next_log_time.saturating_add(interval);
            if !bo.log_interval.is_zero() && bo.next_log_time >= bo.log_interval {
                bo.next_log_time = duration_remainder(bo.next_log_time, bo.log_interval);
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
            let deadline = Instant::now() + interval;
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
            if !bo.total.is_zero() {
                bo.current_total += interval;
                if bo.current_total >= bo.total {
                    return Err(error);
                }
            }
        }
    }

    fn next_interval(&mut self) -> Duration {
        let interval = if self.total.is_zero() {
            self.next
        } else {
            self.next.min(self.total.saturating_sub(self.current_total))
        };
        // Go durations are signed. Saturation avoids a Rust overflow for
        // unrepresentable Go inputs without changing valid PD timing values.
        self.next = self.next.saturating_mul(2).min(self.max);
        interval
    }

    fn reset(&mut self) {
        self.next = self.base;
        self.current_total = Duration::ZERO;
        self.attempt = 0;
        self.next_log_time = Duration::ZERO;
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
    retry(context_done, 10, Duration::from_millis(500), f).await
}

/// Fixed-interval retry, including the final failed attempt's wait. A canceled
/// context returns the operation's last error, unlike [`Backoffer::exec`].
/// Zero attempts succeed without invoking the operation. A zero interval panics
/// like Go's NewTicker. Slow operations do not add a fresh interval to each retry.
pub async fn retry<C, F, Fut>(
    context_done: C,
    max_times: usize,
    interval: Duration,
    mut f: F,
) -> Result<()>
where
    C: Future<Output = Error>,
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    assert!(!interval.is_zero(), "non-positive interval for ticker");
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
