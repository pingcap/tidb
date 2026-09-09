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

//! Typed transactional commands over the retained BatchCommands transport.
//!
//! This module owns one physical attempt only. Region selection, retry,
//! transaction state, primary choice, and two-phase commit stay above it.

use std::{marker::PhantomData, sync::Arc};

use prost::Message;
use tidb_proto::{
    KvrpcBatchGetRequest, KvrpcBatchGetResponse, KvrpcBatchRollbackRequest,
    KvrpcBatchRollbackResponse, KvrpcCommitRequest, KvrpcCommitResponse, KvrpcContext,
    KvrpcGetRequest, KvrpcGetResponse, KvrpcPessimisticLockRequest, KvrpcPessimisticLockResponse,
    KvrpcPessimisticRollbackRequest, KvrpcPessimisticRollbackResponse, KvrpcPrewriteRequest,
    KvrpcPrewriteResponse, KvrpcScanRequest, KvrpcScanResponse, KvrpcTxnHeartBeatRequest,
    KvrpcTxnHeartBeatResponse,
};

use super::batch::{
    batch_get_entry, batch_rollback_entry, commit_entry, get_entry, pessimistic_lock_entry,
    pessimistic_rollback_entry, prewrite_entry, reply_pair, scan_entry, txn_heart_beat_entry,
    BatchCommandEntry, BatchCommandTag, BatchInflightError, BatchReply, BatchRequestProgress,
    BatchRoute, OpaqueBatchCommand,
};
use super::TonicCoprocessorClient;
use super::{
    CompletionError, DirectUnaryClientError, DirectUnaryConnectionError, UnaryCallContext,
};

/// Immutable identity assigned before one transaction command enters tonic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionBatchPublication {
    tag: BatchCommandTag,
    route: BatchRoute,
    request_id: u64,
}

impl TransactionBatchPublication {
    /// Publication identity for a command a store completed in-process.
    ///
    /// Go's unistore RPCClient answers every kvrpcpb command inside the
    /// process; no BatchCommands stream exists, so the receipt names the
    /// in-process address with generation zero. The identity is still
    /// irrevocable and still truthful: the store DID see the command.
    #[must_use]
    pub fn in_process(tag: BatchCommandTag, address: &str, request_id: u64) -> Self {
        Self {
            tag,
            route: BatchRoute::direct(address, 0),
            request_id,
        }
    }

    /// Exact BatchCommands oneof tag used for this command.
    #[must_use]
    pub const fn tag(&self) -> BatchCommandTag {
        self.tag
    }

    /// Scheduler-assigned request identity published into the sole in-flight table.
    #[must_use]
    pub const fn request_id(&self) -> u64 {
        self.request_id
    }

    /// Physical TiKV address carrying this attempt.
    #[must_use]
    pub fn physical_address(&self) -> &str {
        self.route.physical_address()
    }

    /// Address-local channel-pool version carrying this attempt.
    #[must_use]
    pub const fn physical_channel_version(&self) -> u64 {
        self.route.physical_channel_version()
    }

    /// Address-local BatchCommands stream generation carrying this attempt.
    #[must_use]
    pub const fn batch_stream_generation(&self) -> u64 {
        self.route.generation()
    }

    /// Logical TiKV target when the physical route is a forwarding proxy.
    #[must_use]
    pub fn forwarded_host(&self) -> Option<&str> {
        self.route.forwarded_host()
    }
}

/// One decoded response paired with the publication that produced it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionBatchResponse<R> {
    /// Exact decoded kvrpcpb response, including region and key errors.
    pub response: R,
    /// Immutable physical publication identity.
    pub publication: TransactionBatchPublication,
}

/// Pull-side owner of one typed transaction command completion.
pub struct TransactionBatchPending<R> {
    tag: BatchCommandTag,
    completion: BatchReply,
    publication: Option<TransactionBatchPublication>,
    progress: Arc<BatchRequestProgress>,
    barrier: Option<crate::rpc::transport_runtime::PublicationBarrier>,
    response: PhantomData<fn() -> R>,
}

impl<R> TransactionBatchPending<R>
where
    R: Message + Default,
{
    pub(in crate::rpc) fn entry(
        tag: BatchCommandTag,
        encoded_request: Vec<u8>,
        forwarded_host: Option<&str>,
    ) -> (BatchCommandEntry, Self) {
        let (completion, pull) = reply_pair();
        crate::rpc::batch::wire::wire_diag::note_outbound(tag);
        let mut entry =
            BatchCommandEntry::new(OpaqueBatchCommand::new(tag, encoded_request), completion);
        if let Some(forwarded_host) = forwarded_host {
            entry = entry.with_forwarded_host(forwarded_host);
        }
        let progress = entry.progress();
        (
            entry,
            Self {
                tag,
                completion: pull,
                publication: None,
                progress,
                barrier: None,
                response: PhantomData,
            },
        )
    }

    fn retain_barrier(&mut self, barrier: crate::rpc::transport_runtime::PublicationBarrier) {
        self.barrier = Some(barrier);
    }

    /// In-flight admission records identity before a response can complete.
    /// Go's entry likewise carries its identity through receive and errors.
    fn resolve_publication(&mut self) {
        if self.publication.is_some() {
            return;
        }
        if self.progress.publication_route().is_none() {
            if let Some(barrier) = self.barrier.take() {
                barrier.wait();
            }
        }
        self.capture_publication();
    }

    fn capture_publication(&mut self) {
        if self.publication.is_some() {
            return;
        }
        self.publication =
            self.progress
                .publication_route()
                .map(|route| TransactionBatchPublication {
                    tag: self.tag,
                    route: route.clone(),
                    request_id: self.progress.request_id(),
                });
    }

    /// Publication identity, available after successful in-flight admission.
    ///
    /// An explicit pre-response read waits for address-local admission only.
    #[must_use]
    pub fn publication(&mut self) -> Option<&TransactionBatchPublication> {
        self.resolve_publication();
        self.publication.as_ref()
    }

    fn map_result(
        &self,
        result: Result<OpaqueBatchCommand, BatchInflightError>,
    ) -> Result<TransactionBatchResponse<R>, DirectUnaryClientError> {
        match result {
            Ok(command) if command.tag() == self.tag => {
                let publication = self.publication.clone().ok_or_else(|| {
                    DirectUnaryClientError::InvalidRequest(format!(
                        "successful {:?} completion has no publication identity",
                        self.tag
                    ))
                })?;
                let response = R::decode(command.body()).map_err(|error| {
                    DirectUnaryClientError::InvalidRequest(format!(
                        "invalid BatchCommands {:?} response: {error}",
                        self.tag
                    ))
                })?;
                Ok(TransactionBatchResponse {
                    response,
                    publication,
                })
            }
            Ok(command) => Err(DirectUnaryClientError::InvalidRequest(format!(
                "BatchCommands {:?} attempt returned {:?}",
                self.tag,
                command.tag()
            ))),
            Err(BatchInflightError::Protocol(error)) => {
                Err(DirectUnaryClientError::InvalidRequest(format!(
                    "invalid BatchCommands {:?} envelope: {error}",
                    self.tag
                )))
            }
            Err(BatchInflightError::Transport(error)) => Err(error),
        }
    }

    /// Polls without blocking; `None` means the exact request remains pending.
    pub fn try_complete(
        &mut self,
    ) -> Result<Option<Result<TransactionBatchResponse<R>, DirectUnaryClientError>>, CompletionError>
    {
        let Some(result) = self.completion.try_complete()? else {
            return Ok(None);
        };
        // A completed response already has identity in its original entry.
        self.capture_publication();
        Ok(Some(self.map_result(result)))
    }

    /// Waits for the one terminal response or the canonical call cancellation/deadline.
    pub fn complete(
        &mut self,
        call: &UnaryCallContext,
    ) -> Result<Result<TransactionBatchResponse<R>, DirectUnaryClientError>, CompletionError> {
        let result = self.completion.complete(call)?;
        // No second acknowledgement follows the response on the success path.
        self.capture_publication();
        Ok(self.map_result(result))
    }

    pub(crate) fn poll_complete(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Result<Result<TransactionBatchResponse<R>, DirectUnaryClientError>, CompletionError>,
    > {
        let result = std::task::ready!(self.completion.poll_complete(cx))?;
        self.capture_publication();
        std::task::Poll::Ready(Ok(self.map_result(result)))
    }

    /// Cancels this exact completion without creating a response.
    pub fn cancel(&mut self) {
        self.completion.cancel();
    }
}

impl TonicCoprocessorClient {
    fn publish_transaction_command<R>(
        &mut self,
        physical_address: &str,
        entry: BatchCommandEntry,
        mut pending: TransactionBatchPending<R>,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchPending<R>, DirectUnaryClientError>
    where
        R: Message + Default,
    {
        if call.cancellation().is_cancelled() {
            pending.cancel();
            return Err(DirectUnaryClientError::CallerCancelled);
        }
        if call.timeout().is_zero() {
            pending.cancel();
            return Err(DirectUnaryClientError::Timeout {
                connection: DirectUnaryConnectionError::local_deadline(
                    physical_address,
                    0,
                    "BatchCommands deadline elapsed before transaction command admission"
                        .to_owned(),
                ),
                timeout_ms: 0,
            });
        }
        // Go sendBatchRequest waits on the response, not a publication ACK.
        let barrier =
            match self.submit_batch_commands_with_call(physical_address, vec![entry], call) {
                Ok(barrier) => barrier,
                Err(error) => {
                    pending.cancel();
                    return Err(error);
                }
            };
        pending.retain_barrier(barrier);
        Ok(pending)
    }

    /// Begins one transactional Get on an already selected TiKV route.
    pub fn begin_transaction_get(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &KvrpcGetRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchPending<KvrpcGetResponse>, DirectUnaryClientError> {
        let (entry, pending) = get_entry(request, context, forwarded_host);
        self.publish_transaction_command(physical_address, entry, pending, call)
    }

    /// Begins one transactional BatchGet for keys in one selected region.
    pub fn begin_transaction_batch_get(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &KvrpcBatchGetRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchPending<KvrpcBatchGetResponse>, DirectUnaryClientError> {
        let (entry, pending) = batch_get_entry(request, context, forwarded_host);
        self.publish_transaction_command(physical_address, entry, pending, call)
    }

    /// Begins one transactional forward Scan on an already selected TiKV route.
    ///
    /// A Scan is bounded by the serving region: TiKV stops at the region's end
    /// key even when `end_key` reaches further, so the caller advances across
    /// regions itself.
    pub fn begin_transaction_scan(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &KvrpcScanRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchPending<KvrpcScanResponse>, DirectUnaryClientError> {
        let (entry, pending) = scan_entry(request, context, forwarded_host);
        self.publish_transaction_command(physical_address, entry, pending, call)
    }

    /// Begins one transactional Prewrite on an already selected TiKV route.
    pub fn begin_transaction_prewrite(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &KvrpcPrewriteRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchPending<KvrpcPrewriteResponse>, DirectUnaryClientError> {
        let (entry, pending) = prewrite_entry(request, context, forwarded_host);
        self.publish_transaction_command(physical_address, entry, pending, call)
    }

    /// Begins one transactional Commit on an already selected TiKV route.
    pub fn begin_transaction_commit(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &KvrpcCommitRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchPending<KvrpcCommitResponse>, DirectUnaryClientError> {
        let (entry, pending) = commit_entry(request, context, forwarded_host);
        self.publish_transaction_command(physical_address, entry, pending, call)
    }

    /// Begins one transactional BatchRollback on an already selected TiKV route.
    pub fn begin_transaction_batch_rollback(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &KvrpcBatchRollbackRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchPending<KvrpcBatchRollbackResponse>, DirectUnaryClientError> {
        let (entry, pending) = batch_rollback_entry(request, context, forwarded_host);
        self.publish_transaction_command(physical_address, entry, pending, call)
    }

    /// Begins one PessimisticLock on an already selected TiKV route.
    pub fn begin_transaction_pessimistic_lock(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &KvrpcPessimisticLockRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchPending<KvrpcPessimisticLockResponse>, DirectUnaryClientError> {
        let (entry, pending) = pessimistic_lock_entry(request, context, forwarded_host);
        self.publish_transaction_command(physical_address, entry, pending, call)
    }

    /// Begins one PessimisticRollback on an already selected TiKV route.
    pub fn begin_transaction_pessimistic_rollback(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &KvrpcPessimisticRollbackRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchPending<KvrpcPessimisticRollbackResponse>, DirectUnaryClientError>
    {
        let (entry, pending) = pessimistic_rollback_entry(request, context, forwarded_host);
        self.publish_transaction_command(physical_address, entry, pending, call)
    }

    /// Begins one TxnHeartBeat on an already selected TiKV route.
    pub fn begin_transaction_heart_beat(
        &mut self,
        physical_address: &str,
        forwarded_host: Option<&str>,
        request: &KvrpcTxnHeartBeatRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Result<TransactionBatchPending<KvrpcTxnHeartBeatResponse>, DirectUnaryClientError> {
        let (entry, pending) = txn_heart_beat_entry(request, context, forwarded_host);
        self.publish_transaction_command(physical_address, entry, pending, call)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::rpc::UnaryCancellation;

    use super::*;

    #[derive(Default)]
    struct WakeCount(std::sync::atomic::AtomicUsize);

    impl std::task::Wake for WakeCount {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    #[test]
    fn completed_transaction_uses_entry_publication() {
        use crate::rpc::batch::{
            BatchInflightTable, BatchScheduler, BatchWireResponse, PendingBatchCommand,
        };
        for poll_before_response in [false, true] {
            let wake = Arc::new(WakeCount::default());
            let waker = std::task::Waker::from(Arc::clone(&wake));
            let mut cx = std::task::Context::from_waker(&waker);
            let (entry, mut pending) = TransactionBatchPending::<KvrpcGetResponse>::entry(
                BatchCommandTag::Get,
                vec![],
                Some("logical:20160"),
            );
            // There is no separate publication ACK: the original entry is enough.
            let mut scheduler = BatchScheduler::new();
            scheduler.push(entry);
            let group = scheduler
                .build_with_limit(1)
                .into_parts()
                .forwarded
                .into_values()
                .next()
                .unwrap();
            let scheduled = group.into_entries().pop().unwrap();
            let request_id = scheduled.request_id();
            let (_, request) = PendingBatchCommand::from_scheduled(scheduled);
            let route = BatchRoute::forwarded("physical:20160", "logical:20160", 17);
            let mut inflight = BatchInflightTable::new();
            inflight.publish(route.clone(), vec![request]).unwrap();
            if poll_before_response {
                assert!(pending.poll_complete(&mut cx).is_pending());
            }
            let response = BatchWireResponse::new(
                vec![OpaqueBatchCommand::new(
                    BatchCommandTag::Get,
                    KvrpcGetResponse::default().encode_to_vec(),
                )],
                vec![request_id],
                0,
                None,
                0,
            )
            .unwrap();
            assert_eq!(inflight.receive(&route, response).completed, 1);
            let std::task::Poll::Ready(Ok(Ok(response))) = pending.poll_complete(&mut cx) else {
                panic!("published response must be ready without another receipt");
            };
            assert_eq!(response.publication.route, route);
            assert_eq!(response.publication.request_id, request_id);
            assert_eq!(
                wake.0.load(std::sync::atomic::Ordering::Relaxed),
                usize::from(poll_before_response)
            );
            assert_eq!(
                Arc::strong_count(&wake),
                2,
                "completed pull must release its task waker"
            );
        }
    }

    #[test]
    fn cancellation_after_publication_cannot_erase_attempt_identity() {
        let (_, mut pending) = TransactionBatchPending::<KvrpcGetResponse>::entry(
            BatchCommandTag::Get,
            Vec::new(),
            None,
        );
        pending.publication = Some(TransactionBatchPublication {
            tag: BatchCommandTag::Get,
            route: BatchRoute::direct("127.0.0.1:20160", 7),
            request_id: 11,
        });
        let cancellation = UnaryCancellation::new();
        cancellation.cancel();
        let call = UnaryCallContext::new(Duration::from_secs(1), cancellation);

        let wake = Arc::new(WakeCount::default());
        let waker = std::task::Waker::from(Arc::clone(&wake));
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(pending.poll_complete(&mut cx).is_pending());
        assert_eq!(pending.complete(&call), Err(CompletionError::Cancelled));
        assert!(matches!(
            pending.poll_complete(&mut cx),
            std::task::Poll::Ready(Err(CompletionError::Cancelled))
        ));
        assert_eq!(wake.0.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(Arc::strong_count(&wake), 2);

        let publication = pending
            .publication()
            .expect("published cancellation retains its identity");
        assert_eq!(publication.request_id(), 11);
        assert_eq!(publication.batch_stream_generation(), 7);
    }
}
