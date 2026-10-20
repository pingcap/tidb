// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

//! The subset of Go's `context.Context` that the controller's exported
//! limiter API observes: cancellation and an optional deadline.
//!
//! The controller's own request paths are cancelled by dropping their
//! futures; this type exists for the exported `Limiter::reserve` and
//! `wait_reservations` functions, whose Go signatures take a context and
//! whose behavior depends on its deadline and cancellation.

use std::fmt;
use std::time::{Duration, Instant};

use crate::async_util::Cancellation;

/// Go's `context.Canceled` and `context.DeadlineExceeded` sentinels.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextError {
    Canceled,
    DeadlineExceeded,
}

impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContextError::Canceled => f.write_str("context canceled"),
            ContextError::DeadlineExceeded => f.write_str("context deadline exceeded"),
        }
    }
}

impl std::error::Error for ContextError {}

impl From<ContextError> for crate::Error {
    /// `Canceled` keeps the crate's cancellation identity. A deadline uses the
    /// crate's existing representation of Go's `context.DeadlineExceeded`.
    fn from(error: ContextError) -> Self {
        match error {
            ContextError::Canceled => crate::Error::ContextCanceled,
            ContextError::DeadlineExceeded => crate::Error::GrpcAPI(
                tonic::Status::deadline_exceeded("context deadline exceeded"),
            ),
        }
    }
}

/// A cancellable context with an optional deadline, like Go's
/// `context.Context`. `Context::background()` is never done.
#[derive(Clone, Default)]
pub struct Context {
    cancellation: Option<Cancellation>,
    deadline: Option<Instant>,
}

/// Cancels the context returned with it, like Go's `context.CancelFunc`.
#[derive(Clone)]
pub struct CancelFunc(Cancellation);

impl CancelFunc {
    pub fn cancel(&self) {
        self.0.cancel();
    }
}

impl Context {
    /// Go `context.Background()`.
    pub fn background() -> Self {
        Self::default()
    }

    /// Go `context.TODO()`.
    pub fn todo() -> Self {
        Self::default()
    }

    /// Go `context.WithCancel`.
    pub fn with_cancel(parent: &Context) -> (Context, CancelFunc) {
        let cancellation = match &parent.cancellation {
            Some(parent) => parent.child(),
            None => Cancellation::default(),
        };
        (
            Context {
                cancellation: Some(cancellation.clone()),
                deadline: parent.deadline,
            },
            CancelFunc(cancellation),
        )
    }

    /// Go `context.WithDeadline`. An earlier parent deadline wins.
    pub fn with_deadline(parent: &Context, deadline: Instant) -> (Context, CancelFunc) {
        let (mut context, cancel) = Self::with_cancel(parent);
        context.deadline = Some(match parent.deadline {
            Some(existing) if existing < deadline => existing,
            _ => deadline,
        });
        (context, cancel)
    }

    /// Go `context.WithTimeout`.
    pub fn with_timeout(parent: &Context, timeout: Duration) -> (Context, CancelFunc) {
        Self::with_deadline(parent, Instant::now() + timeout)
    }

    /// Go `ctx.Deadline()`.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Go `ctx.Err()`: `None` while the context is not done.
    pub fn err(&self) -> Option<ContextError> {
        if let Some(cancellation) = &self.cancellation {
            if cancellation.is_cancelled() {
                return Some(ContextError::Canceled);
            }
        }
        match self.deadline {
            Some(deadline) if Instant::now() >= deadline => Some(ContextError::DeadlineExceeded),
            _ => None,
        }
    }

    /// Waits until the context is done, like receiving from Go's `ctx.Done()`.
    pub async fn done(&self) {
        let cancelled = async {
            match &self.cancellation {
                Some(cancellation) => cancellation.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        let expired = async {
            match self.deadline {
                Some(deadline) => {
                    tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await
                }
                None => std::future::pending::<()>().await,
            }
        };
        tokio::select! {
            _ = cancelled => {}
            _ = expired => {}
        }
    }
}
