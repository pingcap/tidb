// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! PD's request-driven circuit breaker. Completed calls update the state that
//! admitted them; only a subsequent request can advance the current state.

use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, RwLock};

use prometheus::Counter;
use tokio::time::Instant;

use super::metrics;
use crate::trace::TraceContext;
use crate::{Error, Result};

/// Overload classification is independent of whether an operation failed.
pub type Overloading = bool;
pub const NO: Overloading = false;
pub const YES: Overloading = true;

/// Go `Settings`. Its zero value has no enabled threshold or probe policy.
/// Durations are signed nanoseconds, as in Go; changing settings does not reset a window.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Settings {
    pub error_rate_threshold_pct: u32,
    pub min_qps_for_open: u32,
    /// Observation window in signed nanoseconds (`time.Duration` in Go).
    pub error_rate_window: i64,
    /// Open-state cooldown in signed nanoseconds (`time.Duration` in Go).
    pub cool_down_interval: i64,
    pub half_open_success_count: u32,
}

/// Go's mutable named disabled configuration, distinct from zero Settings and
/// client-go's explicit 30-second region-meta configuration.
pub static ALWAYS_CLOSED_SETTINGS: RwLock<Settings> = RwLock::new(Settings {
    error_rate_threshold_pct: 0,
    min_qps_for_open: 10,
    error_rate_window: 10_000_000_000,
    cool_down_interval: 10_000_000_000,
    half_open_success_count: 1,
});

/// Source state discriminants, including Go's formatting of unknown values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StateType(pub isize);

impl StateType {
    pub const CLOSED: Self = Self(0);
    pub const OPEN: Self = Self(1);
    pub const HALF_OPEN: Self = Self(2);
}

impl fmt::Display for StateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::CLOSED => f.write_str("closed"),
            Self::OPEN => f.write_str("open"),
            Self::HALF_OPEN => f.write_str("half-open"),
            Self(other) => write!(f, "unknown state: {other}"),
        }
    }
}

/// An admitted window/probe generation. Its fields are private, as in Go.
#[derive(Debug)]
pub struct State {
    state_type: StateType,
    started: Instant,
    interval: i64,
    pending_count: u32,
    success_count: u32,
    failure_count: u32,
}

impl State {
    fn new(now: Instant, state_type: StateType, settings: &Settings) -> Self {
        let (interval, pending_count) = match state_type {
            StateType::CLOSED => (settings.error_rate_window, 0),
            StateType::OPEN => (settings.cool_down_interval, 0),
            StateType::HALF_OPEN => (0, 1),
            _ => panic!("unknown state"),
        };
        Self {
            state_type,
            started: now,
            interval,
            pending_count,
            success_count: 0,
            failure_count: 0,
        }
    }

    fn expired(&self, now: Instant) -> bool {
        // Compare elapsed nanoseconds instead of adding a signed Go duration to
        // Instant: platforms need not support Instant +/- the full i64 range.
        let elapsed = if now >= self.started {
            now.duration_since(self.started).as_nanos() as i128
        } else {
            -(self.started.duration_since(now).as_nanos() as i128)
        };
        elapsed > i128::from(self.interval)
    }

    fn on_result(&mut self, overloaded: Overloading) {
        if overloaded {
            self.failure_count = self.failure_count.wrapping_add(1);
        } else {
            self.success_count = self.success_count.wrapping_add(1);
        }
    }
}

type StateHandle = Arc<Mutex<State>>;

struct Inner {
    settings: Settings,
    state: StateHandle,
}

struct Counters {
    success: Counter,
    error: Counter,
    overload: Counter,
    fast_fail: Counter,
}

impl Counters {
    fn new(name: &str, metrics: &metrics::Metrics) -> Self {
        let metric_name = name.replace([' ', '-'], "_");
        let counters = &metrics.circuit_breaker_counters;
        Self {
            success: counters.with_label_values(&[&metric_name, "success"]),
            error: counters.with_label_values(&[&metric_name, "error"]),
            overload: counters.with_label_values(&[&metric_name, "overload"]),
            fast_fail: counters.with_label_values(&[&metric_name, "fast_fail"]),
        }
    }
}

/// Shared PD circuit breaker. No state lock is held while executing a call.
pub struct CircuitBreaker {
    name: String,
    inner: Mutex<Inner>,
    counters: Arc<RwLock<Counters>>,
}

impl CircuitBreaker {
    pub fn new(name: impl Into<String>, settings: Settings) -> Arc<Self> {
        Self::new_with_consumer(name.into(), settings, metrics::register_consumer)
    }

    fn new_with_consumer(
        name: String,
        settings: Settings,
        register: impl FnOnce(metrics::Consumer),
    ) -> Arc<Self> {
        let state = Arc::new(Mutex::new(State::new(
            Instant::now(),
            StateType::CLOSED,
            &settings,
        )));
        let counters = Arc::new(RwLock::new(Counters::new(
            &name,
            &metrics::global_metrics(),
        )));
        let rebind = counters.clone();
        let metric_name = name.clone();
        register(Box::new(move |metrics| {
            *rebind.write().unwrap_or_else(|e| e.into_inner()) =
                Counters::new(&metric_name, metrics);
        }));
        Arc::new(Self {
            name,
            inner: Mutex::new(Inner { settings, state }),
            counters,
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .settings
            .error_rate_threshold_pct
            > 0
    }

    pub fn change_settings(&self, apply: impl FnOnce(&mut Settings)) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        apply(&mut inner.settings);
        log::debug!("circuit breaker settings changed: {:?}", inner.settings);
    }

    /// Execute a synchronous call, preserving its returned error or panic.
    pub fn execute<T>(&self, call: impl FnOnce() -> (Overloading, Result<T>)) -> Result<T> {
        let admission = self.admit()?;
        let (overloaded, result) = call();
        admission.finish(overloaded, result.is_err());
        result
    }

    /// Native async execution. Dropping an admitted future records a cancelled,
    /// non-overloaded call. Unpolled futures never acquire probe admission.
    pub async fn execute_async<T, F>(&self, call: impl FnOnce() -> F) -> Result<T>
    where
        F: Future<Output = (Overloading, Result<T>)>,
    {
        let admission = self.admit()?;
        let (overloaded, result) = call().await;
        admission.finish(overloaded, result.is_err());
        result
    }

    fn admit(&self) -> Result<Admission<'_>> {
        match self.on_request(Instant::now) {
            Ok(state) => Ok(Admission {
                breaker: self,
                state,
                completed: false,
            }),
            Err(error) => {
                self.counters
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
                    .fast_fail
                    .inc();
                Err(error)
            }
        }
    }

    fn on_request(&self, clock: impl FnOnce() -> Instant) -> Result<StateHandle> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = clock(); // Sample after acquiring the owner lock, as in Go.
        let settings = inner.settings;
        let mut state = inner.state.lock().unwrap_or_else(|e| e.into_inner());
        let transition = match state.state_type {
            StateType::CLOSED if state.expired(now) => {
                let total = state.failure_count.wrapping_add(state.success_count);
                // time.Duration.Seconds splits seconds and the remainder before
                // float conversion. Go's supported 64-bit targets truncate the
                // float to an integer and retain the low 32 bits, including negatives.
                let window = settings.error_rate_window;
                let seconds = (window / 1_000_000_000) as f64
                    + (window % 1_000_000_000) as f64 / 1_000_000_000.;
                let minimum = (seconds as i64 as u32).wrapping_mul(settings.min_qps_for_open);
                if settings.error_rate_threshold_pct > 0 && total > 0 {
                    let rate = state.failure_count.wrapping_mul(100) / total;
                    if total >= minimum && rate >= settings.error_rate_threshold_pct {
                        log::error!("circuit breaker tripped and starting to fail all requests: name={} observed-err-rate-pct={} config={:?}", self.name, rate, settings);
                        Some((StateType::OPEN, false))
                    } else {
                        Some((StateType::CLOSED, true))
                    }
                } else {
                    Some((StateType::CLOSED, true))
                }
            }
            StateType::CLOSED => None,
            StateType::OPEN if settings.error_rate_threshold_pct == 0 => {
                Some((StateType::CLOSED, true))
            }
            StateType::OPEN if state.expired(now) => {
                log::info!("circuit breaker cooldown period is over. Transitioning to half-open state to test the service: name={} config={:?}", self.name, settings);
                Some((StateType::HALF_OPEN, true))
            }
            StateType::OPEN => return Err(Error::CircuitBreakerOpen),
            StateType::HALF_OPEN if settings.error_rate_threshold_pct == 0 => {
                Some((StateType::CLOSED, true))
            }
            StateType::HALF_OPEN if state.failure_count > 0 => {
                log::error!("circuit breaker goes from half-open to open again as errors persist and continue to fail all requests: name={} config={:?}", self.name, settings);
                Some((StateType::OPEN, false))
            }
            StateType::HALF_OPEN if state.success_count == settings.half_open_success_count => {
                log::info!("circuit breaker is closed and start allowing all requests: name={} config={:?}", self.name, settings);
                Some((StateType::CLOSED, true))
            }
            StateType::HALF_OPEN if state.pending_count < settings.half_open_success_count => {
                state.pending_count = state.pending_count.wrapping_add(1);
                None
            }
            StateType::HALF_OPEN => return Err(Error::CircuitBreakerOpen),
            _ => panic!("unknown state"),
        };
        drop(state);
        if let Some((kind, allowed)) = transition {
            inner.state = Arc::new(Mutex::new(State::new(now, kind, &settings)));
            if !allowed {
                return Err(Error::CircuitBreakerOpen);
            }
        }
        Ok(inner.state.clone())
    }

    fn on_result(&self, state: &StateHandle, overloaded: Overloading, error: bool) {
        let counters = self.counters.read().unwrap_or_else(|e| e.into_inner());
        if overloaded {
            counters.overload.inc();
        } else {
            counters.success.inc();
        }
        if error {
            counters.error.inc();
        }
        drop(counters);
        // Admission, setting changes and completion share Go's owner lock.
        // The generation handle only preserves a late result's destination.
        let _owner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .on_result(overloaded);
    }
}

struct Admission<'a> {
    breaker: &'a CircuitBreaker,
    state: StateHandle,
    completed: bool,
}

impl Admission<'_> {
    fn finish(mut self, overloaded: Overloading, error: bool) {
        self.completed = true;
        self.breaker.on_result(&self.state, overloaded, error);
    }
}

impl Drop for Admission<'_> {
    fn drop(&mut self) {
        if !self.completed {
            // Go's deferred panic handler observes overload and its still-nil
            // error. Ordinary async cancellation maps to a non-overload error.
            let panicking = std::thread::panicking();
            self.breaker.on_result(&self.state, panicking, !panicking);
        }
    }
}

/// Type-keyed counterpart of Go's exported CircuitBreakerKey.
pub struct CircuitBreakerKey;

pub fn from_context(context: Option<&TraceContext>) -> Option<Arc<CircuitBreaker>> {
    let context = context?;
    if let Some(value) = context.value::<CircuitBreakerKey, Option<Arc<CircuitBreaker>>>() {
        return value.clone();
    }
    context
        .value::<CircuitBreakerKey, Arc<CircuitBreaker>>()
        .cloned()
}

pub fn with_circuit_breaker(
    context: &TraceContext,
    breaker: Option<Arc<CircuitBreaker>>,
) -> TraceContext {
    context.with_value::<CircuitBreakerKey, _>(breaker)
}

#[cfg(test)]
#[path = "circuitbreaker_tests.rs"]
mod tests;
