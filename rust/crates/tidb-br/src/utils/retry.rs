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

//! Go `br/pkg/utils/retry.go`: running a call under a [`BackoffStrategy`].
//! `RetryWithBackoff` (an adapter over client-go's `tikv.Backoffer`) comes
//! with the restore code that uses it.

use std::sync::Arc;
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};

use super::backoff::{BackoffStrategy, Sentinel};
use crate::errors;

/// Go `multierr`'s combined error: the errors of every attempt, shown
/// joined by `"; "`.
#[derive(Debug, Clone)]
pub struct MultiError(pub Vec<errors::Error>);

impl std::fmt::Display for MultiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, e) in self.0.iter().enumerate() {
            if i > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{e}")?;
        }
        Ok(())
    }
}

impl std::error::Error for MultiError {
    /// Go's `errors.Cause` stops at a multierr (it has no `Cause`); Go's
    /// `errors.Is` sees every error. The first error is the source here.
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

/// Go `multierr.Append`.
#[must_use]
pub fn multierr_append(all: Option<errors::Error>, err: errors::Error) -> errors::Error {
    match all {
        None => err,
        Some(all) => {
            let mut v = multierr_errors(&all);
            v.extend(multierr_errors(&err));
            Arc::new(MultiError(v))
        }
    }
}

/// Go `multierr.Errors`.
#[must_use]
pub fn multierr_errors(err: &errors::Error) -> Vec<errors::Error> {
    match err.downcast_ref::<MultiError>() {
        Some(m) => m.0.clone(),
        None => vec![err.clone()],
    }
}

/// Go `WithRetry`.
///
/// # Errors
/// Every attempt's error, combined.
pub fn with_retry(
    ctx: &Context,
    mut f: impl FnMut() -> errors::Result<()>,
    bo: &mut dyn BackoffStrategy,
) -> errors::Result<()> {
    with_retry_v2(ctx, bo, |_| f())
}

/// Go `WithRetryV2`.
///
/// # Errors
/// Every attempt's error, combined; on `ctx` done, the errors so far.
pub fn with_retry_v2<T>(
    ctx: &Context,
    bo: &mut dyn BackoffStrategy,
    mut f: impl FnMut(&Context) -> errors::Result<T>,
) -> errors::Result<T> {
    let mut all: Option<errors::Error> = None;
    while bo.remaining_attempts() > 0 {
        let err = match f(ctx) {
            Ok(v) => return Ok(v),
            Err(e) => e,
        };
        all = Some(multierr_append(all, err.clone()));
        let backoff = bo.next_backoff(&err);
        if ctx.sleep(backoff) {
            return Err(all.unwrap());
        }
    }
    // Go returns the nil `allErrors` when no attempt ran.
    Err(all.unwrap_or_else(|| errors::errorf("no attempt was made")))
}

/// Go `WithRetryReturnLastErr`.
///
/// # Errors
/// `ctx`'s error, or the last attempt's error.
pub fn with_retry_return_last_err(
    ctx: &Context,
    mut f: impl FnMut() -> errors::Result<()>,
    bo: &mut dyn BackoffStrategy,
) -> errors::Result<()> {
    if let Some(err) = ctx.err() {
        return Err(Arc::new(err));
    }
    let mut last: Option<errors::Error> = None;
    while bo.remaining_attempts() > 0 {
        let err = match f() {
            Ok(()) => return Ok(()),
            Err(e) => e,
        };
        let backoff = bo.next_backoff(&err);
        tidb_log::info(
            "retryable operation failed",
            &[
                Field::new("category", Value::Str("utils".into())),
                Field::new("error", Value::Str(err.to_string())),
                Field::new("backoff", Value::Duration(backoff.as_nanos() as i64)),
            ],
        );
        last = Some(err);
        if ctx.sleep(backoff) {
            break;
        }
    }
    last.map_or(Ok(()), Err)
}

/// Go `FallBack2CreateTable`: a batch create that failed with
/// `ErrInvalidDDLJob` falls back to creating tables one by one.
#[must_use]
pub fn fall_back_to_create_table(err: &(dyn std::error::Error + 'static)) -> bool {
    errors::cause(err)
        .downcast_ref::<tidb_terror::TerrorError>()
        .is_some_and(|e| e.code().value() == tidb_errno::errcode::ErrInvalidDDLJob as isize)
}

/// Go `verboseBackoffStrategy`.
struct VerboseBackoff<B> {
    inner: B,
    group_id: String,
}

impl<B: BackoffStrategy> BackoffStrategy for VerboseBackoff<B> {
    fn next_backoff(&mut self, err: &errors::Error) -> Duration {
        let next = self.inner.next_backoff(err);
        tidb_log::warn(
            "Encountered err, retrying.",
            &[
                Field::new("nextBackoff", Value::Str(tidb_model::go_duration::format_go_duration(next.as_nanos() as i64))),
                Field::new("err", Value::Str(err.to_string())),
                Field::new("gid", Value::Str(self.group_id.clone())),
            ],
        );
        next
    }

    fn remaining_attempts(&self) -> i32 {
        let attempt = self.inner.remaining_attempts();
        if attempt > 0 {
            tidb_log::debug(
                "Retry attempt hint.",
                &[Field::new("attempt", Value::I64(i64::from(attempt))), Field::new("gid", Value::Str(self.group_id.clone()))],
            );
        } else {
            tidb_log::warn("Retry limit exceeded.", &[Field::new("gid", Value::Str(self.group_id.clone()))]);
        }
        attempt
    }
}

/// Go `VerboseRetry`.
pub fn verbose_retry<B: BackoffStrategy + 'static>(bo: B) -> impl BackoffStrategy {
    VerboseBackoff { inner: bo, group_id: uuid::Uuid::new_v4().to_string() }
}

/// Go `failedOnErr`.
struct FailedOnErr<B> {
    inner: B,
    failed: bool,
    failed_on: Vec<Sentinel>,
}

impl<B: BackoffStrategy> BackoffStrategy for FailedOnErr<B> {
    fn next_backoff(&mut self, err: &errors::Error) -> Duration {
        let cause = errors::cause(err.as_ref());
        if self.failed_on.iter().any(|s| s.matches(cause)) {
            self.failed = true;
            return Duration::ZERO;
        }
        if !self.failed {
            return self.inner.next_backoff(err);
        }
        Duration::ZERO
    }

    fn remaining_attempts(&self) -> i32 {
        if self.failed {
            0
        } else {
            self.inner.remaining_attempts()
        }
    }
}

/// Go `GiveUpRetryOn`.
pub fn give_up_retry_on<B: BackoffStrategy + 'static>(bo: B, errs: Vec<Sentinel>) -> impl BackoffStrategy {
    FailedOnErr { inner: bo, failed: false, failed_on: errs }
}
