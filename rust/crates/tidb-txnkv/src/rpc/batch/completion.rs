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

//! Completion variants carried by one client-go-shaped BatchCommands entry.

use std::fmt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use crate::rpc::unary::CancellationWaiter;
use crate::rpc::{CompletionError, CompletionRequest, UnaryCallContext};

use super::{BatchEntryCompletion, BatchInflightError, OpaqueBatchCommand};

type BatchResult = Result<OpaqueBatchCommand, BatchInflightError>;
type CancelListener = Box<dyn FnOnce() + Send + 'static>;

struct SynchronousCompletionInner {
    cancelled: bool,
    completed: bool,
    result: Option<BatchResult>,
    cancel_listeners: Vec<CancelListener>,
}

struct SynchronousCompletionState {
    inner: Mutex<SynchronousCompletionInner>,
    changed: Condvar,
}

impl CancellationWaiter for SynchronousCompletionState {
    fn wake_all(&self) {
        self.changed.notify_all();
    }
}

/// Stream-side authority for one synchronous BatchCommands response channel.
#[derive(Clone)]
pub struct SynchronousBatchCompletion {
    state: Arc<SynchronousCompletionState>,
}

impl fmt::Debug for SynchronousBatchCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self
            .state
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        formatter
            .debug_struct("SynchronousBatchCompletion")
            .field("cancelled", &inner.cancelled)
            .field("completed", &inner.completed)
            .finish()
    }
}

impl SynchronousBatchCompletion {
    fn schedule(&self, result: BatchResult) {
        let published = {
            let mut inner = self
                .state
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if inner.cancelled || inner.completed {
                false
            } else {
                inner.completed = true;
                inner.result = Some(result);
                inner.cancel_listeners.clear();
                true
            }
        };
        if published {
            self.state.changed.notify_all();
        }
    }

    fn is_cancelled(&self) -> bool {
        self.state
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .cancelled
    }

    fn on_cancel<F>(&self, listener: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let mut listener = Some(Box::new(listener) as CancelListener);
        let run_now = {
            let mut inner = self
                .state
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if inner.cancelled {
                true
            } else if inner.completed {
                false
            } else {
                inner
                    .cancel_listeners
                    .push(listener.take().expect("listener is registered once"));
                false
            }
        };
        if run_now {
            listener.expect("cancelled registration retains listener")();
        }
    }
}

/// Caller-side owner of one synchronous BatchCommands response channel.
pub struct SynchronousBatchPull {
    state: Arc<SynchronousCompletionState>,
}

impl SynchronousBatchPull {
    pub fn try_complete(&mut self) -> Result<Option<BatchResult>, CompletionError> {
        let mut inner = self
            .state
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if inner.cancelled {
            return Err(CompletionError::Cancelled);
        }
        Ok(inner.result.take())
    }

    pub fn complete(&mut self, call: &UnaryCallContext) -> Result<BatchResult, CompletionError> {
        call.cancellation().register_completion_waiter(&self.state);
        let result = loop {
            let mut inner = self
                .state
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if inner.cancelled || call.cancellation().is_cancelled() {
                break Err(CompletionError::Cancelled);
            }
            if let Some(result) = inner.result.take() {
                break Ok(result);
            }
            let remaining = call.deadline().saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break Err(CompletionError::DeadlineExceeded);
            }
            let (next, _) = self
                .state
                .changed
                .wait_timeout(inner, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            inner = next;
            drop(inner);
        };
        call.cancellation()
            .unregister_completion_waiter(&self.state);
        if result.is_err() {
            self.cancel();
        }
        result
    }

    pub fn cancel(&mut self) {
        let listeners = {
            let mut inner = self
                .state
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if inner.cancelled || inner.completed {
                Vec::new()
            } else {
                inner.cancelled = true;
                std::mem::take(&mut inner.cancel_listeners)
            }
        };
        self.state.changed.notify_all();
        for listener in listeners {
            listener();
        }
    }
}

pub fn synchronous_batch_completion_pair() -> (SynchronousBatchCompletion, SynchronousBatchPull) {
    let state = Arc::new(SynchronousCompletionState {
        inner: Mutex::new(SynchronousCompletionInner {
            cancelled: false,
            completed: false,
            result: None,
            cancel_listeners: Vec::new(),
        }),
        changed: Condvar::new(),
    });
    (
        SynchronousBatchCompletion {
            state: Arc::clone(&state),
        },
        SynchronousBatchPull { state },
    )
}

/// Go's one batch entry carries either its synchronous response channel or its
/// asynchronous callback through the same scheduler and in-flight table.
#[derive(Clone, Debug)]
pub enum BatchCommandCompletion {
    /// `RPCClient.SendRequest` response-channel completion.
    Synchronous(SynchronousBatchCompletion),
    /// `RPCClient.SendRequestAsync` callback completion.
    Asynchronous(CompletionRequest<OpaqueBatchCommand, BatchInflightError>),
}

impl From<SynchronousBatchCompletion> for BatchCommandCompletion {
    fn from(completion: SynchronousBatchCompletion) -> Self {
        Self::Synchronous(completion)
    }
}

impl From<CompletionRequest<OpaqueBatchCommand, BatchInflightError>> for BatchCommandCompletion {
    fn from(completion: CompletionRequest<OpaqueBatchCommand, BatchInflightError>) -> Self {
        Self::Asynchronous(completion)
    }
}

impl BatchCommandCompletion {
    pub(super) fn schedule(&self, result: BatchResult) {
        match self {
            Self::Synchronous(completion) => completion.schedule(result),
            Self::Asynchronous(completion) => completion.schedule(result),
        }
    }

    pub(super) fn schedule_error(&self, error: BatchInflightError) {
        self.schedule(Err(error));
    }

    pub(super) fn is_cancelled(&self) -> bool {
        match self {
            Self::Synchronous(completion) => completion.is_cancelled(),
            Self::Asynchronous(completion) => completion.is_cancelled(),
        }
    }

    pub(super) fn on_cancel<F>(&self, listener: F)
    where
        F: FnOnce() + Send + 'static,
    {
        match self {
            Self::Synchronous(completion) => completion.on_cancel(listener),
            Self::Asynchronous(completion) => completion.on_cancel(listener),
        }
    }
}

impl BatchEntryCompletion for BatchCommandCompletion {
    type Error = BatchInflightError;

    fn is_canceled(&self) -> bool {
        self.is_cancelled()
    }

    fn fail(&self, error: Self::Error) {
        self.schedule_error(error);
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::synchronous_batch_completion_pair;
    use crate::rpc::batch::{BatchCommandTag, OpaqueBatchCommand};
    use crate::rpc::{CompletionError, UnaryCallContext, UnaryCancellation};

    #[test]
    fn synchronous_response_is_delivered_directly_to_its_waiter() {
        let (completion, mut pull) = synchronous_batch_completion_pair();
        completion.schedule(Ok(OpaqueBatchCommand::new(
            BatchCommandTag::Coprocessor,
            b"response",
        )));

        let response = pull.try_complete().unwrap().unwrap().unwrap();
        assert_eq!(response.tag(), BatchCommandTag::Coprocessor);
        assert_eq!(response.body(), b"response");
    }

    #[test]
    fn caller_cancellation_wakes_a_synchronous_response_waiter() {
        let (_completion, mut pull) = synchronous_batch_completion_pair();
        let cancellation = UnaryCancellation::new();
        let call = UnaryCallContext::new(Duration::from_secs(10), cancellation.clone());
        let waiting = std::thread::spawn(move || pull.complete(&call));

        cancellation.cancel();

        assert_eq!(waiting.join().unwrap(), Err(CompletionError::Cancelled));
    }
}
