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

//! client-go batchCommandsEntry has either a one-result response channel or
//! an explicit async callback. Ordinary RPCs do not execute a callback queue.

use std::fmt;
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use tokio::sync::oneshot;

use super::{BatchEntryCompletion, BatchInflightError, OpaqueBatchCommand};
use crate::rpc::{CompletionError, CompletionNotifier, CompletionRequest, UnaryCallContext};

type Reply = Result<OpaqueBatchCommand, BatchInflightError>;
type CancelListener = Box<dyn FnOnce() + Send + 'static>;

struct ReplyState {
    sender: Option<oneshot::Sender<Reply>>,
    cancelled: bool,
    cancel_listeners: Vec<CancelListener>,
}

impl fmt::Debug for ReplyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyState")
            .field("pending", &self.sender.is_some())
            .field("cancelled", &self.cancelled)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
enum CompletionKind {
    Response(Arc<Mutex<ReplyState>>),
    Callback(CompletionRequest<OpaqueBatchCommand, BatchInflightError>),
}

/// The original response channel or explicitly requested callback carried
/// unchanged from admission through the route-scoped in-flight table.
#[derive(Clone, Debug)]
pub struct BatchCommandCompletion {
    kind: CompletionKind,
}

impl From<CompletionRequest<OpaqueBatchCommand, BatchInflightError>> for BatchCommandCompletion {
    fn from(callback: CompletionRequest<OpaqueBatchCommand, BatchInflightError>) -> Self {
        Self {
            kind: CompletionKind::Callback(callback),
        }
    }
}

impl BatchCommandCompletion {
    /// Publishes one terminal result using the entry's original delivery mode.
    pub fn schedule(&self, result: Reply) {
        match &self.kind {
            CompletionKind::Callback(callback) => callback.schedule(result),
            CompletionKind::Response(state) => {
                let sender = state
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .sender
                    .take();
                if let Some(sender) = sender {
                    let _ = sender.send(result);
                }
            }
        }
    }

    /// Publishes a transport/protocol failure through the same terminal gate.
    pub fn schedule_error(&self, error: BatchInflightError) {
        self.schedule(Err(error));
    }

    /// Whether the caller abandoned this exact request.
    pub fn is_cancelled(&self) -> bool {
        match &self.kind {
            CompletionKind::Callback(callback) => callback.is_cancelled(),
            CompletionKind::Response(state) => {
                let state = state.lock().unwrap_or_else(|p| p.into_inner());
                state.cancelled
                    || state
                        .sender
                        .as_ref()
                        .is_some_and(oneshot::Sender::is_closed)
            }
        }
    }

    pub(super) fn on_cancel(&self, listener: impl FnOnce() + Send + 'static) {
        match &self.kind {
            CompletionKind::Callback(callback) => callback.on_cancel(listener),
            CompletionKind::Response(state) => {
                let mut state = state.lock().unwrap_or_else(|p| p.into_inner());
                if state.cancelled {
                    drop(state);
                    listener();
                } else {
                    state.cancel_listeners.push(Box::new(listener));
                }
            }
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

/// Normal RPC response ownership. Tokio's one-shot carries the value and wake;
/// no callback, runnable queue, or second response publication is involved.
pub(in crate::rpc) struct BatchReply {
    receiver: Option<oneshot::Receiver<Reply>>,
    ready: Option<Result<Reply, CompletionError>>,
    state: Arc<Mutex<ReplyState>>,
}

pub(in crate::rpc) fn reply_pair() -> (BatchCommandCompletion, BatchReply) {
    let (sender, receiver) = oneshot::channel();
    let state = Arc::new(Mutex::new(ReplyState {
        sender: Some(sender),
        cancelled: false,
        cancel_listeners: Vec::new(),
    }));
    (
        BatchCommandCompletion {
            kind: CompletionKind::Response(Arc::clone(&state)),
        },
        BatchReply {
            receiver: Some(receiver),
            ready: None,
            state,
        },
    )
}

struct ReplyNotifier {
    notifier: CompletionNotifier,
    token: u64,
}

impl Wake for ReplyNotifier {
    fn wake(self: Arc<Self>) {
        self.notifier.notify(self.token);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notifier.notify(self.token);
    }
}

impl BatchReply {
    pub(in crate::rpc) fn set_notifier(&mut self, notifier: CompletionNotifier, token: u64) {
        let waker = Waker::from(Arc::new(ReplyNotifier {
            notifier: notifier.clone(),
            token,
        }));
        // Poll installs the receiver's native waker or captures an early reply.
        // Publication before registration therefore cannot lose a notification.
        if let Poll::Ready(result) = self.poll_complete(&mut Context::from_waker(&waker)) {
            self.ready = Some(result);
            notifier.notify(token);
        }
    }

    pub(in crate::rpc) fn try_complete(&mut self) -> Result<Option<Reply>, CompletionError> {
        if self.is_cancelled() {
            return Ok(None);
        }
        if let Some(result) = self.ready.take() {
            return result.map(Some);
        }
        let Some(receiver) = &mut self.receiver else {
            return Ok(None);
        };
        match receiver.try_recv() {
            Ok(result) => {
                self.receiver = None;
                Ok(Some(result))
            }
            Err(oneshot::error::TryRecvError::Empty) => Ok(None),
            Err(oneshot::error::TryRecvError::Closed) => {
                self.receiver = None;
                Err(CompletionError::AlreadyCompleted)
            }
        }
    }

    pub(in crate::rpc) fn poll_complete(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Reply, CompletionError>> {
        if self.is_cancelled() {
            return Poll::Ready(Err(CompletionError::Cancelled));
        }
        if let Some(result) = self.ready.take() {
            return Poll::Ready(result);
        }
        let Some(receiver) = &mut self.receiver else {
            return Poll::Ready(Err(CompletionError::AlreadyCompleted));
        };
        let result = std::task::ready!(Pin::new(receiver).poll(cx));
        self.receiver = None;
        Poll::Ready(result.map_err(|_| CompletionError::AlreadyCompleted))
    }

    pub(in crate::rpc) fn complete(
        &mut self,
        call: &UnaryCallContext,
    ) -> Result<Reply, CompletionError> {
        let result = (|| {
            if self.is_cancelled() || call.cancellation().is_cancelled() {
                return Err(CompletionError::Cancelled);
            }
            if call.timeout().is_zero() {
                return Err(CompletionError::DeadlineExceeded);
            }
            if let Some(result) = self.try_complete()? {
                return Ok(result);
            }
            crate::rpc::execution::wait_with_call(poll_fn(|cx| self.poll_complete(cx)), call)?
        })();
        let result = if call.cancellation().is_cancelled() {
            Err(CompletionError::Cancelled)
        } else if call.timeout().is_zero() {
            Err(CompletionError::DeadlineExceeded)
        } else {
            result
        };
        if result.is_err() {
            self.cancel();
        }
        result
    }

    fn is_cancelled(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancelled
    }

    pub(in crate::rpc) fn cancel(&mut self) {
        let (sender, listeners) = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            state.cancelled = true;
            (
                state.sender.take(),
                std::mem::take(&mut state.cancel_listeners),
            )
        };
        // Drop wakes a native waiter, and listener execution must not retain
        // the reply mutex while acquiring the route's in-flight table.
        drop(sender);
        self.receiver = None;
        self.ready = None;
        for listener in listeners {
            listener();
        }
    }
}

impl Drop for BatchReply {
    fn drop(&mut self) {
        if self.receiver.is_some() {
            self.cancel();
        }
    }
}
