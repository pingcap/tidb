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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};

use tokio::sync::oneshot;

use super::{BatchEntryCompletion, BatchInflightError, OpaqueBatchCommand};
use crate::rpc::{CompletionError, CompletionNotifier, CompletionRequest, UnaryCallContext};

type Reply = Result<OpaqueBatchCommand, BatchInflightError>;
type CancelListener = Box<dyn FnOnce() + Send + 'static>;

struct ReplyState {
    // Go batchCommandsEntry.canceled is monotonic and read on every poll.
    cancelled: AtomicBool,
    delivery: Mutex<ReplyDelivery>,
}

struct ReplyDelivery {
    sender: Option<oneshot::Sender<Reply>>,
    cancel_listeners: Vec<CancelListener>,
    snapshot_rpc: Option<crate::rpc::SnapshotRpcObservation>,
}

impl fmt::Debug for ReplyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplyState")
            .field("cancelled", &self.cancelled.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
enum CompletionKind {
    Response(Arc<ReplyState>),
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
                let sender = {
                    let mut delivery = state.delivery.lock().unwrap_or_else(|p| p.into_inner());
                    // Finish accounting under the terminal gate: a racing
                    // cancellation must not return ahead of the winning reply's
                    // stats. The stats lock protects data only and never enters
                    // transport code, so it cannot acquire this reply lock.
                    drop(delivery.snapshot_rpc.take());
                    delivery.sender.take()
                };
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
            // BatchReply owns the receiver and publishes cancellation before
            // dropping it. Delivery ownership needs no lock for this check.
            CompletionKind::Response(state) => state.cancelled.load(Ordering::Acquire),
        }
    }

    pub(super) fn on_cancel(&self, listener: impl FnOnce() + Send + 'static) {
        match &self.kind {
            CompletionKind::Callback(callback) => callback.on_cancel(listener),
            CompletionKind::Response(state) => {
                let mut delivery = state.delivery.lock().unwrap_or_else(|p| p.into_inner());
                if state.cancelled.load(Ordering::Acquire) {
                    drop(delivery);
                    listener();
                } else {
                    delivery.cancel_listeners.push(Box::new(listener));
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
    state: Arc<ReplyState>,
}

pub(in crate::rpc) fn reply_pair() -> (BatchCommandCompletion, BatchReply) {
    let (sender, receiver) = oneshot::channel();
    let state = Arc::new(ReplyState {
        cancelled: AtomicBool::new(false),
        delivery: Mutex::new(ReplyDelivery {
            sender: Some(sender),
            cancel_listeners: Vec::new(),
            snapshot_rpc: None,
        }),
    });
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
    /// Installed before transport admission, so an immediate response cannot
    /// precede its observer. Uninstrumented requests acquire no additional lock.
    pub(in crate::rpc) fn observe_snapshot_rpc(
        &mut self,
        observation: Option<crate::rpc::SnapshotRpcObservation>,
    ) {
        if let Some(observation) = observation {
            let mut delivery = self
                .state
                .delivery
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            debug_assert!(delivery.sender.is_some());
            debug_assert!(delivery.snapshot_rpc.is_none());
            delivery.snapshot_rpc = Some(observation);
        }
    }

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
        self.state.cancelled.load(Ordering::Acquire)
    }

    pub(in crate::rpc) fn cancel(&mut self) {
        let (sender, listeners) = {
            let mut state = self
                .state
                .delivery
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            drop(state.snapshot_rpc.take());
            self.state.cancelled.store(true, Ordering::Release);
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::reply_pair;
    use crate::rpc::batch::{BatchCommandTag, OpaqueBatchCommand};
    use crate::rpc::{CompletionError, UnaryCallContext, UnaryCancellation};

    #[test]
    fn synchronous_response_is_delivered_directly_to_its_waiter() {
        let (completion, mut pull) = reply_pair();
        completion.schedule(Ok(OpaqueBatchCommand::new(
            BatchCommandTag::Coprocessor,
            bytes::Bytes::from_static(b"response"),
        )));

        let response = pull.try_complete().unwrap().unwrap().unwrap();
        assert_eq!(response.tag(), BatchCommandTag::Coprocessor);
        assert_eq!(response.body(), b"response");
    }

    #[test]
    fn caller_cancellation_wakes_a_synchronous_response_waiter() {
        // client-go's canceled-entry flag is visible whether cancellation
        // happens before or after the route registers its retirement hook.
        for cancel_first in [false, true] {
            let (completion, mut pull) = reply_pair();
            let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            if cancel_first {
                pull.cancel();
            }
            let observed = calls.clone();
            completion.on_cancel(move || {
                observed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            });
            pull.cancel();
            pull.cancel();
            assert!(completion.is_cancelled());
            assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        }
        let (_completion, mut pull) = reply_pair();
        let cancellation = UnaryCancellation::new();
        let call = UnaryCallContext::new(Duration::from_secs(10), cancellation.clone());
        let waiting = std::thread::spawn(move || pull.complete(&call));

        cancellation.cancel();

        assert_eq!(waiting.join().unwrap(), Err(CompletionError::Cancelled));
    }

    #[test]
    fn snapshot_rpc_stats_stop_at_delivery_before_the_reader_polls() {
        use crate::rpc::{DirectUnaryClientError, SnapshotRpcObservation};
        use std::sync::Arc;
        use tikv_client::{SnapshotRpcCommand, SnapshotRuntimeStats};
        for fail in [false, true] {
            let stats = Arc::new(SnapshotRuntimeStats::new());
            let (completion, mut pull) = reply_pair();
            pull.observe_snapshot_rpc(SnapshotRpcObservation::start(
                Some(&stats),
                SnapshotRpcCommand::BatchGet,
            ));
            if fail {
                completion.schedule_error(super::BatchInflightError::Transport(
                    DirectUnaryClientError::Closed,
                ));
            } else {
                completion.schedule(Ok(OpaqueBatchCommand::new(
                    BatchCommandTag::BatchGet,
                    bytes::Bytes::new(),
                )));
            }
            // Delivery must finish accounting even if this ready reader waits
            // behind another request indefinitely. No wall-clock sleep needed.
            assert_eq!(stats.rpc_count(SnapshotRpcCommand::BatchGet), 1);
            let duration = stats.rpc_duration(SnapshotRpcCommand::BatchGet);
            assert_eq!(pull.try_complete().unwrap().unwrap().is_err(), fail);
            completion.schedule_error(super::BatchInflightError::Transport(
                DirectUnaryClientError::Closed,
            ));
            pull.cancel();
            drop(pull);
            assert_eq!(stats.rpc_count(SnapshotRpcCommand::BatchGet), 1);
            assert_eq!(stats.rpc_duration(SnapshotRpcCommand::BatchGet), duration);
        }
    }

    #[test]
    fn snapshot_rpc_stats_account_once_when_a_reader_cancels_or_drops() {
        use crate::rpc::SnapshotRpcObservation;
        use std::sync::Arc;
        use tikv_client::{SnapshotRpcCommand, SnapshotRuntimeStats};
        for explicit_cancel in [false, true] {
            let stats = Arc::new(SnapshotRuntimeStats::new());
            let (completion, mut pull) = reply_pair();
            pull.observe_snapshot_rpc(SnapshotRpcObservation::start(
                Some(&stats),
                SnapshotRpcCommand::BatchGet,
            ));
            if explicit_cancel {
                pull.cancel();
                pull.cancel();
                assert_eq!(stats.rpc_count(SnapshotRpcCommand::BatchGet), 1);
            }
            drop(pull);
            assert_eq!(stats.rpc_count(SnapshotRpcCommand::BatchGet), 1);
            let duration = stats.rpc_duration(SnapshotRpcCommand::BatchGet);
            completion.schedule(Ok(OpaqueBatchCommand::new(
                BatchCommandTag::BatchGet,
                bytes::Bytes::new(),
            )));
            drop(completion);
            assert_eq!(stats.rpc_count(SnapshotRpcCommand::BatchGet), 1);
            assert_eq!(stats.rpc_duration(SnapshotRpcCommand::BatchGet), duration);
        }
    }

    #[test]
    fn snapshot_rpc_stats_are_final_when_cancellation_races_delivery() {
        use crate::rpc::SnapshotRpcObservation;
        use std::sync::{Arc, Barrier};
        use tikv_client::{SnapshotRpcCommand, SnapshotRuntimeStats};
        for _ in 0..32 {
            let stats = Arc::new(SnapshotRuntimeStats::new());
            let (completion, mut pull) = reply_pair();
            pull.observe_snapshot_rpc(SnapshotRpcObservation::start(
                Some(&stats),
                SnapshotRpcCommand::BatchGet,
            ));
            let start = Arc::new(Barrier::new(2));
            let ready = Arc::clone(&start);
            let worker = std::thread::spawn(move || {
                ready.wait();
                completion.schedule(Ok(OpaqueBatchCommand::new(
                    BatchCommandTag::BatchGet,
                    bytes::Bytes::new(),
                )));
            });
            start.wait();
            pull.cancel();
            assert_eq!(stats.rpc_count(SnapshotRpcCommand::BatchGet), 1);
            let duration = stats.rpc_duration(SnapshotRpcCommand::BatchGet);
            worker.join().unwrap();
            drop(pull);
            assert_eq!(stats.rpc_count(SnapshotRpcCommand::BatchGet), 1);
            assert_eq!(stats.rpc_duration(SnapshotRpcCommand::BatchGet), duration);
        }
    }
}
