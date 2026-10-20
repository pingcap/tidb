// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! Go `limiter.go`: the local token bucket with reservations, low-token
//! notification and reconfiguration wakeups.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use prometheus::Counter;
use tokio::sync::mpsc;

use super::context::{Context, ContextError};
use super::global_controller::log_controller_trace;
use super::util::{duration_nanos, go_duration_string};
use crate::async_util::Cancellation;
use crate::pd::errs;

/// How many tokens are consumed per second.
pub type FillRate = f64;

/// The infinite rate limit; it allows all events (even if burst is zero).
pub const INF: FillRate = f64::MAX;

/// Converts a minimum time interval between events to a fill rate.
pub fn every(interval: Duration) -> FillRate {
    if interval.is_zero() {
        return INF;
    }
    1.0 / interval.as_secs_f64()
}

/// The duration `Reservation::delay` answers for a failed reservation.
pub const INF_DURATION: Duration = Duration::from_nanos(i64::MAX as u64);

/// A message notifying the low-token state.
#[derive(Clone, Copy, Debug, Default)]
pub struct NotifyMsg {
    /// When the notification was sent; `None` is Go's zero time.
    pub start_time: Option<Instant>,
}

pub(crate) struct LimiterMetricsCollection {
    low_token_notify_counter: Counter,
}

/// A token bucket of size `burst`, refilled at `fill_rate` tokens per second.
///
/// Burst semantics differ from a plain token bucket:
///   - `burst == 0`: unlimited capacity with a rate (the controller default).
///   - `burst < 0`: unlimited capacity and the rate is ignored, as if the
///     rate were `INF`.
///   - `burst > 0`: limited capacity.
pub struct Limiter {
    pub(crate) mu: Mutex<LimiterState>,
    metrics: Option<LimiterMetricsCollection>,
}

pub(crate) struct LimiterState {
    pub(crate) fill_rate: FillRate,
    pub(crate) tokens: f64,
    pub(crate) burst: i64,
    /// The last time `tokens` was updated.
    pub(crate) last: Instant,
    pub(crate) notify_threshold: f64,
    low_tokens_notify_chan: Option<mpsc::Sender<NotifyMsg>>,
    /// The notify threshold drops to 0 after a notification, so this records
    /// whether the limiter is in the low-token state.
    pub(crate) is_low_process: bool,
    /// Limits notifications when the speed limit is already set.
    pub(crate) remaining_notify_times: i64,
    pub(crate) name: String,
    /// Closed on every reconfiguration to wake waiting token acquirers.
    reconfigured_ch: Option<Arc<Cancellation>>,
}

impl LimiterState {
    /// Keeps `last` monotonic; see https://github.com/tikv/pd/issues/8435.
    fn update_last(&mut self, t: Instant) {
        if self.last < t {
            self.last = t;
        }
    }

    /// The token count at `now`, without changing the limiter.
    pub(crate) fn get_tokens(&self, now: Instant) -> (Instant, f64) {
        let mut last = self.last;
        if now < last {
            last = now;
        }
        if self.burst < 0 {
            return (last, self.tokens);
        }
        // The new number of tokens, due to time that passed.
        let elapsed = now.saturating_duration_since(last);
        let delta = tokens_from_duration(self.fill_rate, elapsed);
        let mut tokens = self.tokens + delta;
        if self.burst > 0 {
            let burst = self.burst as f64;
            if tokens > burst {
                tokens = burst;
            }
        }
        (last, tokens)
    }

    fn is_low_tokens(&self) -> bool {
        self.burst >= 0
            && (self.is_low_process
                || (self.notify_threshold > 0.0 && self.tokens < self.notify_threshold))
    }
}

impl Limiter {
    fn lock(&self) -> MutexGuard<'_, LimiterState> {
        self.mu.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Sends a non-blocking notification and disables further ones until the
    /// next reconfiguration.
    fn notify_locked(&self, state: &mut LimiterState) {
        if state.is_low_process {
            return;
        }
        state.notify_threshold = 0.0;
        state.is_low_process = true;
        if let Some(chan) = &state.low_tokens_notify_chan {
            if chan
                .try_send(NotifyMsg {
                    start_time: Some(Instant::now()),
                })
                .is_ok()
            {
                if let Some(metrics) = &self.metrics {
                    metrics.low_token_notify_counter.inc();
                }
            }
        }
    }

    /// Go `notify`, taking the limiter lock.
    pub(crate) fn notify(&self) {
        let mut state = self.lock();
        self.notify_locked(&mut state);
    }

    fn maybe_notify(&self, state: &mut LimiterState) {
        if state.is_low_tokens() {
            self.notify_locked(state);
        }
    }

    /// Go `NewLimiter`.
    pub fn new_limiter(
        now: Instant,
        r: FillRate,
        b: i64,
        tokens: f64,
        low_tokens_notify_chan: Option<mpsc::Sender<NotifyMsg>>,
    ) -> Arc<Limiter> {
        let limiter = Limiter {
            mu: Mutex::new(LimiterState {
                fill_rate: r,
                tokens,
                burst: b,
                last: now,
                notify_threshold: 0.0,
                low_tokens_notify_chan,
                is_low_process: false,
                remaining_notify_times: 0,
                name: String::new(),
                reconfigured_ch: Some(Arc::new(Cancellation::default())),
            }),
            metrics: None,
        };
        log::debug!("new limiter, rate {r}, burst {b}, tokens {tokens}");
        Arc::new(limiter)
    }

    /// Go `NewLimiterWithCfg`.
    pub fn new_limiter_with_cfg(
        name: &str,
        now: Instant,
        cfg: TokenBucketReconfigureArgs,
        low_tokens_notify_chan: Option<mpsc::Sender<NotifyMsg>>,
    ) -> Arc<Limiter> {
        let metrics = super::metrics::global_metrics();
        let limiter = Limiter {
            mu: Mutex::new(LimiterState {
                fill_rate: cfg.new_fill_rate,
                tokens: cfg.new_tokens,
                burst: cfg.new_burst,
                last: now,
                notify_threshold: cfg.new_notify_threshold,
                low_tokens_notify_chan,
                is_low_process: false,
                remaining_notify_times: 0,
                name: name.to_owned(),
                reconfigured_ch: Some(Arc::new(Cancellation::default())),
            }),
            metrics: Some(LimiterMetricsCollection {
                low_token_notify_counter: metrics
                    .low_token_request_notify_counter
                    .with_label_values(&[name]),
            }),
        };
        log::debug!(
            "new limiter {name}, rate {}, burst {}, tokens {}",
            cfg.new_fill_rate,
            cfg.new_burst,
            cfg.new_tokens
        );
        Arc::new(limiter)
    }

    /// Go `Reserve`: a reservation for `n` tokens at `now`. It is not reserved
    /// when the wait exceeds `wait_duration`, or the context deadline when the
    /// context has one. A done context yields an unreserved reservation that
    /// carries the context error.
    pub fn reserve(
        self: &Arc<Self>,
        ctx: &Context,
        wait_duration: Duration,
        now: Instant,
        n: f64,
    ) -> Reservation {
        if let Some(error) = ctx.err() {
            return Reservation {
                err: Some(error),
                ..Reservation::default()
            };
        }
        let wait_limit = match ctx.deadline() {
            // A deadline before `now` is Go's negative wait limit: nothing,
            // not even an immediate grant, fits.
            Some(deadline) => deadline.checked_duration_since(now),
            None => Some(wait_duration),
        };
        self.reserve_n_with_limit(now, n, wait_limit)
    }

    /// Enables the notification at the given threshold.
    pub fn setup_notification_threshold(&self, threshold: f64) {
        self.lock().notify_threshold = threshold;
    }

    /// Sets the name of the limiter.
    pub fn set_name(&self, name: &str) -> &Self {
        self.lock().name = name.to_owned();
        self
    }

    /// A signal closed when the limiter is next reconfigured, so waiters wake
    /// as soon as new tokens arrive instead of sleeping blindly.
    pub fn get_reconfigured_ch(&self) -> Arc<Cancellation> {
        self.lock()
            .reconfigured_ch
            .get_or_insert_with(|| Arc::new(Cancellation::default()))
            .clone()
    }

    /// Whether the limiter is in low tokens.
    pub fn is_low_tokens(&self) -> bool {
        self.lock().is_low_tokens()
    }

    /// The burst size of the limiter.
    pub fn get_burst(&self) -> i64 {
        self.lock().burst
    }

    /// Decreases the amount of tokens currently available.
    pub fn remove_tokens(&self, now: Instant, amount: f64) {
        let mut state = self.lock();
        if state.burst < 0 || state.fill_rate == INF {
            return;
        }
        let (_, tokens) = state.get_tokens(now);
        state.update_last(now);
        state.tokens = tokens - amount;
        self.maybe_notify(&mut state);
    }

    /// Adds tokens back to the limiter.
    ///
    /// Overshooting the burst is intentional: `get_tokens` clamps on the next
    /// read. No notification is attempted because a refund only raises the
    /// token count.
    pub fn refund_tokens(&self, now: Instant, amount: f64) {
        let mut state = self.lock();
        if state.burst < 0 || state.fill_rate == INF {
            return;
        }
        let (_, tokens) = state.get_tokens(now);
        state.update_last(now);
        state.tokens = tokens + amount;
    }

    /// Modifies all settings of the limiter.
    pub fn reconfigure(
        &self,
        now: Instant,
        args: TokenBucketReconfigureArgs,
        opts: &[LimiterOption],
    ) {
        let mut state = self.lock();
        log_controller_trace(|| {
            format!(
                "[resource group controller] before reconfigure, name: {}, old-tokens: {}, old-rate: {}, old-notify-threshold: {}, old-burst: {}",
                state.name, state.tokens, state.fill_rate, state.notify_threshold, state.burst
            )
        });
        if args.new_burst < 0 {
            state.update_last(now);
            state.tokens = args.new_tokens;
        } else {
            let (_, tokens) = state.get_tokens(now);
            state.update_last(now);
            state.tokens = tokens + args.new_tokens;
        }
        state.fill_rate = args.new_fill_rate;
        state.burst = args.new_burst;
        state.notify_threshold = args.new_notify_threshold;
        for opt in opts {
            (opt.0)(&mut state);
        }
        self.maybe_notify(&mut state);
        // Wake every acquirer waiting in a retry loop.
        if let Some(reconfigured) = &state.reconfigured_ch {
            reconfigured.cancel();
        }
        state.reconfigured_ch = Some(Arc::new(Cancellation::default()));
        log_controller_trace(|| {
            format!(
                "[resource group controller] after reconfigure, name: {}, tokens: {}, rate: {}, notify-threshold: {}, burst: {}",
                state.name, state.tokens, state.fill_rate, args.new_notify_threshold, state.burst
            )
        });
    }

    /// The tokens available at `now`.
    pub fn available_tokens(&self, now: Instant) -> f64 {
        self.lock().get_tokens(now).1
    }

    /// Go `reserveN` with a non-negative maximum reservation wait.
    pub(crate) fn reserve_n(
        self: &Arc<Self>,
        now: Instant,
        n: f64,
        max_future_reserve: Duration,
    ) -> Reservation {
        self.reserve_n_with_limit(now, n, Some(max_future_reserve))
    }

    /// Go `reserveN`. `None` is a negative maximum wait.
    fn reserve_n_with_limit(
        self: &Arc<Self>,
        now: Instant,
        n: f64,
        max_future_reserve: Option<Duration>,
    ) -> Reservation {
        let mut state = self.lock();
        if state.burst < 0 || state.fill_rate == INF {
            return Reservation {
                reserved: true,
                lim: Some(self.clone()),
                tokens: n,
                time_to_act: Some(now),
                ..Reservation::default()
            };
        }
        let (last, mut tokens) = state.get_tokens(now);

        // The remaining number of tokens resulting from the request.
        tokens -= n;
        let mut wait_duration = Duration::ZERO;
        if tokens < 0.0 {
            wait_duration = duration_from_tokens(state.fill_rate, -tokens);
        }

        let reserved = max_future_reserve.is_some_and(|max| wait_duration <= max);
        let mut reservation = Reservation {
            reserved,
            lim: Some(self.clone()),
            fill_rate: state.fill_rate,
            need_wait_duration: wait_duration,
            remaining_tokens: tokens,
            ..Reservation::default()
        };
        if reserved {
            reservation.tokens = n;
            reservation.time_to_act = Some(add_saturating(now, wait_duration));
            state.update_last(now);
            state.tokens = tokens;
            self.maybe_notify(&mut state);
        } else {
            // Log when the limiter cannot reserve for a while.
            if state.last.elapsed() > RESERVE_WARN_LOG_INTERVAL {
                log::warn!(
                    "[resource group controller] cannot reserve enough tokens, need-wait-duration: {}, max-wait-duration: {}, current-ltb-tokens: {}, current-ltb-rate: {}, request-tokens: {}, notify-threshold: {}, is-low-process: {}, burst: {}, remaining-notify-times: {}, name: {}",
                    go_duration_string(duration_nanos(wait_duration)),
                    max_future_reserve
                        .map(|max| go_duration_string(duration_nanos(max)))
                        .unwrap_or_else(|| "negative".to_owned()),
                    state.tokens,
                    state.fill_rate,
                    n,
                    state.notify_threshold,
                    state.is_low_process,
                    state.burst,
                    state.remaining_notify_times,
                    state.name
                );
            }
            state.update_last(last);
            if state.fill_rate == 0.0 {
                self.notify_locked(&mut state);
            } else if state.remaining_notify_times > 0 {
                // With a positive fill rate the speed limit is already set and
                // the server has granted as much as it can, except in the case
                // of https://github.com/tikv/pd/issues/6300.
                state.remaining_notify_times -= 1;
                self.notify_locked(&mut state);
            }
        }
        reservation
    }

    /// Resets the remaining notify times to 3.
    pub fn reset_remaining_notify_times(&self) {
        self.lock().remaining_notify_times = 3;
    }

    /// Go's unexported `getTokens`, for tests.
    pub(crate) fn get_tokens(&self, now: Instant) -> (Instant, f64) {
        self.lock().get_tokens(now)
    }

    /// Go's unexported `last`, for tests.
    pub(crate) fn last(&self) -> Instant {
        self.lock().last
    }
}

/// Reconfiguration arguments of a limiter.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct TokenBucketReconfigureArgs {
    pub new_tokens: f64,
    pub new_fill_rate: f64,
    pub new_burst: i64,
    pub new_notify_threshold: f64,
}

/// Configures a limiter during reconfiguration.
pub struct LimiterOption(pub(crate) fn(&mut LimiterState));

pub(crate) fn reset_low_process() -> LimiterOption {
    LimiterOption(|state| state.is_low_process = false)
}

const RESERVE_WARN_LOG_INTERVAL: Duration = Duration::from_millis(10);

/// Events permitted by a limiter after a delay. A reservation may be
/// cancelled, which returns its tokens to the limiter.
#[derive(Clone, Default)]
pub struct Reservation {
    pub(crate) reserved: bool,
    pub(crate) lim: Option<Arc<Limiter>>,
    pub(crate) tokens: f64,
    pub(crate) time_to_act: Option<Instant>,
    pub(crate) need_wait_duration: Duration,
    /// The fill rate at reservation time; it can change later.
    pub(crate) fill_rate: FillRate,
    pub(crate) remaining_tokens: f64,
    pub(crate) err: Option<ContextError>,
}

impl Reservation {
    /// Whether the limiter can provide the requested tokens within the
    /// maximum wait. An unreserved reservation delays `INF_DURATION` and
    /// cannot be cancelled.
    pub fn reserved(&self) -> bool {
        self.reserved
    }

    /// Go `Delay`, which is `delay_from(Instant::now())`.
    pub fn delay(&self) -> Duration {
        self.delay_from(Instant::now())
    }

    /// How long the holder must wait before acting. Zero means act now;
    /// `INF_DURATION` means the tokens cannot be granted within the maximum
    /// wait.
    pub fn delay_from(&self, now: Instant) -> Duration {
        if !self.reserved {
            return INF_DURATION;
        }
        match self.time_to_act {
            Some(time_to_act) => time_to_act.saturating_duration_since(now),
            None => Duration::ZERO,
        }
    }

    /// The holder will not act; the reserved tokens return to the limiter.
    pub fn cancel_at(&self, now: Instant) {
        if !self.reserved {
            return;
        }
        let Some(lim) = &self.lim else {
            return;
        };
        let mut state = lim.lock();
        if self.tokens == 0.0 || state.burst < 0 || state.fill_rate == INF {
            return;
        }
        let (_, mut tokens) = state.get_tokens(now);
        tokens += self.tokens;
        state.update_last(now);
        state.tokens = tokens;
    }
}

/// Go `fillRate.durationFromTokens`. A float duration beyond the int64 range
/// saturates, as Go's conversion does on arm64.
pub(crate) fn duration_from_tokens(fill_rate: FillRate, tokens: f64) -> Duration {
    if fill_rate <= 0.0 {
        return INF_DURATION;
    }
    let seconds = tokens / fill_rate;
    let nanos = 1_000_000_000.0 * seconds;
    if nanos >= i64::MAX as f64 {
        return INF_DURATION;
    }
    Duration::from_nanos(nanos as u64)
}

/// Go `fillRate.tokensFromDuration`.
pub(crate) fn tokens_from_duration(fill_rate: FillRate, d: Duration) -> f64 {
    if fill_rate <= 0.0 {
        return 0.0;
    }
    d.as_secs_f64() * fill_rate
}

/// `t + d`, or the latest representable instant on the way there; Go's
/// wall-clock arithmetic does not overflow for an infinite wait.
fn add_saturating(t: Instant, d: Duration) -> Instant {
    let mut d = d;
    loop {
        if let Some(sum) = t.checked_add(d) {
            return sum;
        }
        d /= 2;
    }
}

/// Whether `error` is `ErrClientResourceGroupThrottled`.
pub(crate) fn is_throttled_error(error: &crate::Error) -> bool {
    matches!(error, crate::Error::Pd(error) if error.definition() == errs::ERR_CLIENT_RESOURCE_GROUP_THROTTLED)
}

/// Returns every reservation's tokens if the wait is abandoned, including
/// when the waiting future is dropped (Go's context cancellation).
struct CancelOnDrop<'a, 'b> {
    reservations: &'a [Option<&'b Reservation>],
    now: Instant,
    armed: bool,
}

impl CancelOnDrop<'_, '_> {
    fn cancel(&mut self) {
        self.armed = false;
        for reservation in self.reservations.iter().flatten() {
            reservation.cancel_at(self.now);
        }
    }
}

impl Drop for CancelOnDrop<'_, '_> {
    fn drop(&mut self) {
        if self.armed {
            self.cancel();
        }
    }
}

/// Processes a series of reservations so that every limiter's tokens are
/// returned if one reservation fails. Returns the waited duration and the
/// result; on error the duration is the failed reservation's needed wait.
pub async fn wait_reservations(
    ctx: &Context,
    now: Instant,
    reservations: &[Option<&Reservation>],
) -> (Duration, crate::Result<()>) {
    if reservations.is_empty() {
        return (Duration::ZERO, Ok(()));
    }
    let mut guard = CancelOnDrop {
        reservations,
        now,
        armed: false,
    };
    let mut longest_delay_duration = Duration::ZERO;
    for reservation in reservations.iter().flatten() {
        if !reservation.reserved {
            guard.cancel();
            if let Some(error) = reservation.err {
                return (reservation.need_wait_duration, Err(error.into()));
            }
            return (
                reservation.need_wait_duration,
                Err(crate::Error::Pd(errs::client_resource_group_throttled(
                    go_duration_string(duration_nanos(reservation.need_wait_duration)),
                    reservation.fill_rate,
                    reservation.remaining_tokens,
                ))),
            );
        }
        let delay = reservation.delay_from(now);
        if delay > longest_delay_duration {
            longest_delay_duration = delay;
        }
    }
    if longest_delay_duration.is_zero() {
        return (Duration::ZERO, Ok(()));
    }
    let _ = fail::eval("waitReservationsBeforeSelect", |_| ());
    guard.armed = true;
    tokio::select! {
        _ = tokio::time::sleep(longest_delay_duration) => {
            // We can proceed.
            guard.armed = false;
            (longest_delay_duration, Ok(()))
        }
        _ = ctx.done() => {
            // The context was cancelled before we could proceed. Cancelling
            // the reservations may let other events proceed sooner.
            guard.cancel();
            let error = ctx.err().unwrap_or(ContextError::Canceled);
            (Duration::ZERO, Err(error.into()))
        }
    }
}

/// Go `NewLimiter`.
pub fn new_limiter(
    now: Instant,
    r: FillRate,
    b: i64,
    tokens: f64,
    low_tokens_notify_chan: Option<mpsc::Sender<NotifyMsg>>,
) -> Arc<Limiter> {
    Limiter::new_limiter(now, r, b, tokens, low_tokens_notify_chan)
}

/// Go `NewLimiterWithCfg`.
pub fn new_limiter_with_cfg(
    name: &str,
    now: Instant,
    cfg: TokenBucketReconfigureArgs,
    low_tokens_notify_chan: Option<mpsc::Sender<NotifyMsg>>,
) -> Arc<Limiter> {
    Limiter::new_limiter_with_cfg(name, now, cfg, low_tokens_notify_chan)
}
