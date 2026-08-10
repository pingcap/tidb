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

//! Complete transcreation of Go `pkg/util/timeutil` (`errors.go`, `time.go`,
//! `time_zone.go`): TiDB's time-zone infrastructure — system-timezone
//! inference, named/offset zone parsing, and the day-time-period check.
//!
//! Go's `*time.Location` maps to [`TimeZone`]: the process-local zone
//! (`System`), an IANA zone backed by `chrono-tz`'s compiled tzdata (the
//! existing equal library — no hand-maintained timezone table), or a fixed
//! offset (`time.FixedZone`). Go's `locCache` exists to amortize
//! `time.LoadLocation`'s file I/O; `chrono-tz` resolves names from static
//! data with no I/O, so the cache is a non-observable performance artifact
//! with nothing to port.
//! Time-zone offsets reuse `tidb-datatype`'s source-compatible MySQL duration
//! parser, so compact, day-prefix, spaced, and fractional forms have one
//! authority across SQL evaluation and `ParseTimeZone`.
//!
//! `time.go`'s `Sleep(ctx, d)` maps to [`sleep`] plus [`SleepContext`]. The
//! context uses a condition variable, so cancellation wakes a sleeping thread
//! immediately without polling, and a deadline bounds the same wait just as
//! Go's `context.WithTimeout` bounds `<-timer.C`.

mod time_zone;

pub use time_zone::{
    construct_time_zone, get_system_tz, infer_system_tz, load_location, parse_time_zone,
    set_system_tz, system_location, within_day_time_period, zone, zone_name, TimeZone,
    TimeZoneError,
};

use std::sync::{Arc, Condvar, LazyLock, Mutex};
use std::time::{Duration, Instant};
use tidb_error::terror::TerrorError;
use tidb_error::tidb::errcode;

#[derive(Debug, Default)]
struct SleepContextState {
    cause: Mutex<Option<SleepError>>,
    changed: Condvar,
}

/// Why a [`SleepContext`] finished before the requested duration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SleepError {
    /// Go `context.Canceled`.
    Cancelled,
    /// Go `context.DeadlineExceeded`.
    DeadlineExceeded,
}

impl std::fmt::Display for SleepError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Cancelled => "context canceled",
            Self::DeadlineExceeded => "context deadline exceeded",
        })
    }
}

impl std::error::Error for SleepError {}

/// The cancellation/deadline subset of Go `context.Context` consumed by
/// [`sleep`]. Clones share cancellation and retain the same deadline.
#[derive(Clone, Debug, Default)]
pub struct SleepContext {
    state: Arc<SleepContextState>,
    deadline: Option<Instant>,
}

impl SleepContext {
    /// A context with no deadline, like `context.Background()`.
    #[must_use]
    pub fn background() -> Self {
        Self::default()
    }

    /// A background context canceled after `timeout`, like
    /// `context.WithTimeout(context.Background(), timeout)`.
    #[must_use]
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            state: Arc::new(SleepContextState::default()),
            deadline: Instant::now().checked_add(timeout),
        }
    }

    /// Cancels this context and wakes every current sleeper.
    pub fn cancel(&self) {
        let mut cause = self
            .state
            .cause
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if cause.is_none() {
            *cause = Some(
                if self
                    .deadline
                    .is_some_and(|deadline| deadline <= Instant::now())
                {
                    SleepError::DeadlineExceeded
                } else {
                    SleepError::Cancelled
                },
            );
        }
        drop(cause);
        self.state.changed.notify_all();
    }

    /// Whether explicit cancellation or the deadline has already fired.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.state
            .cause
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
            || self
                .deadline
                .is_some_and(|deadline| deadline <= Instant::now())
    }
}

/// Go `timeutil.Sleep`: blocks until `duration` elapses or `context` is
/// canceled/deadlined, whichever happens first.
pub fn sleep(context: &SleepContext, duration: Duration) -> Result<(), SleepError> {
    let started = Instant::now();
    let sleep_deadline = started.checked_add(duration);
    let wait_deadline = match (sleep_deadline, context.deadline) {
        (Some(sleep_deadline), Some(context_deadline)) => {
            Some(sleep_deadline.min(context_deadline))
        }
        (Some(sleep_deadline), None) => Some(sleep_deadline),
        (None, Some(context_deadline)) => Some(context_deadline),
        (None, None) => None,
    };
    let mut cause = context
        .state
        .cause
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    loop {
        if let Some(cause) = *cause {
            return Err(cause);
        }

        let now = Instant::now();
        if context.deadline.is_some_and(|context_deadline| {
            context_deadline <= now
                && sleep_deadline.is_none_or(|sleep_deadline| context_deadline <= sleep_deadline)
        }) {
            *cause = Some(SleepError::DeadlineExceeded);
            return Err(SleepError::DeadlineExceeded);
        }
        if sleep_deadline.is_some_and(|sleep_deadline| sleep_deadline <= now) {
            return Ok(());
        }

        let Some(remaining) = wait_deadline.map(|deadline| deadline.saturating_duration_since(now))
        else {
            cause = context
                .state
                .changed
                .wait(cause)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            continue;
        };
        let (next, _) = context
            .state
            .changed
            .wait_timeout(cause, remaining)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        cause = next;
    }
}

/// `ErrUnknownTimeZone` (Go `errors.go`): unknown time zone.
pub static ERR_UNKNOWN_TIME_ZONE: LazyLock<TerrorError> =
    LazyLock::new(|| crate::dbterror::CLASS_VARIABLE.new_std(errcode::ErrUnknownTimeZone));

#[cfg(test)]
mod tests {
    use super::*;

    /// Source: `pkg/util/timeutil/time_test.go::TestSleep`.
    #[test]
    fn test_sleep() {
        let context_timeout = Duration::from_millis(10);
        let sleep_time = Duration::from_secs(10);
        let context = SleepContext::with_timeout(context_timeout);
        let now = Instant::now();

        let result = sleep(&context, sleep_time);

        let since = now.elapsed();
        assert_eq!(result, Err(SleepError::DeadlineExceeded));
        assert!(since > context_timeout);
        assert!(since < sleep_time);
    }
}
