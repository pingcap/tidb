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

use std::marker::PhantomData;

use prost::Message;
use tidb_proto::{
    KvrpcBatchRollbackRequest, KvrpcBatchRollbackResponse, KvrpcCommitRequest, KvrpcCommitResponse,
    KvrpcContext, KvrpcGetRequest, KvrpcGetResponse, KvrpcPessimisticLockRequest,
    KvrpcPessimisticLockResponse, KvrpcPessimisticRollbackRequest,
    KvrpcPessimisticRollbackResponse, KvrpcPrewriteRequest, KvrpcPrewriteResponse,
    KvrpcScanRequest, KvrpcScanResponse, KvrpcTxnHeartBeatRequest, KvrpcTxnHeartBeatResponse,
};

use super::batch::{
    batch_rollback_entry, commit_entry, get_entry, pessimistic_lock_entry,
    pessimistic_rollback_entry, prewrite_entry, scan_entry, txn_heart_beat_entry,
    BatchCommandEntry, BatchCommandTag, BatchInflightError, BatchPublicationReceipt, BatchRoute,
    OpaqueBatchCommand,
};
use super::TonicCoprocessorClient;
use super::{
    completion_pair, CompletionError, CompletionPull, CompletionRunLoop, DirectUnaryClientError,
    DirectUnaryConnectionError, UnaryCallContext,
};

/// Immutable identity assigned before one transaction command enters tonic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionBatchPublication {
    tag: BatchCommandTag,
    route: BatchRoute,
    request_id: u64,
}

impl TransactionBatchPublication {
    fn from_receipts(
        tag: BatchCommandTag,
        receipts: &[BatchPublicationReceipt],
    ) -> Result<Self, DirectUnaryClientError> {
        let [receipt] = receipts else {
            return Err(DirectUnaryClientError::InvalidRequest(format!(
                "one {tag:?} command requires exactly one BatchCommands publication receipt, got {}",
                receipts.len()
            )));
        };
        let [request_id] = receipt.request_ids() else {
            return Err(DirectUnaryClientError::InvalidRequest(format!(
                "one {tag:?} command requires exactly one published request ID, got {}",
                receipt.request_ids().len()
            )));
        };
        Ok(Self {
            tag,
            route: receipt.route().clone(),
            request_id: *request_id,
        })
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
    completion: CompletionPull<OpaqueBatchCommand, BatchInflightError>,
    publication: Option<TransactionBatchPublication>,
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
        let (completion, pull) = completion_pair(CompletionRunLoop::new(), || {});
        let mut entry =
            BatchCommandEntry::new(OpaqueBatchCommand::new(tag, encoded_request), completion);
        if let Some(forwarded_host) = forwarded_host {
            entry = entry.with_forwarded_host(forwarded_host);
        }
        (
            entry,
            Self {
                tag,
                completion: pull,
                publication: None,
                response: PhantomData,
            },
        )
    }

    fn bind_publication(
        &mut self,
        receipts: &[BatchPublicationReceipt],
    ) -> Result<(), DirectUnaryClientError> {
        if self.publication.is_some() {
            return Err(DirectUnaryClientError::InvalidRequest(format!(
                "{:?} pending attempt was bound to publication twice",
                self.tag
            )));
        }
        self.publication = Some(TransactionBatchPublication::from_receipts(
            self.tag, receipts,
        )?);
        Ok(())
    }

    /// Publication identity, available after successful in-flight admission.
    #[must_use]
    pub const fn publication(&self) -> Option<&TransactionBatchPublication> {
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
        let result = self.completion.try_complete()?;
        Ok(result.map(|result| self.map_result(result)))
    }

    /// Waits for the one terminal response or the canonical call cancellation/deadline.
    pub fn complete(
        &mut self,
        call: &UnaryCallContext,
    ) -> Result<Result<TransactionBatchResponse<R>, DirectUnaryClientError>, CompletionError> {
        let result = self.completion.complete(call)?;
        Ok(self.map_result(result))
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
        let receipts =
            match self.submit_batch_commands_with_call(physical_address, vec![entry], call) {
                Ok(receipts) => receipts,
                Err(error) => {
                    pending.cancel();
                    return Err(error);
                }
            };
        if let Err(error) = pending.bind_publication(&receipts) {
            pending.cancel();
            return Err(error);
        }
        Ok(retain_published_pending(pending, call))
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

fn retain_published_pending<R>(
    pending: TransactionBatchPending<R>,
    _call: &UnaryCallContext,
) -> TransactionBatchPending<R> {
    // Publication is the irrevocable boundary. Cancellation after the receipt
    // is bound must flow through `complete`, which cancels the physical attempt
    // while leaving its identity available to transaction cleanup or
    // undetermined-primary classification.
    debug_assert!(pending.publication.is_some());
    pending
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::rpc::UnaryCancellation;

    use super::*;

    #[test]
    fn publication_requires_one_receipt_before_completion_can_escape() {
        let error = TransactionBatchPublication::from_receipts(BatchCommandTag::Get, &[])
            .expect_err("an admitted transaction command cannot lose its publication receipt");
        assert!(matches!(error, DirectUnaryClientError::InvalidRequest(_)));
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

        let pending = retain_published_pending(pending, &call);

        let publication = pending
            .publication()
            .expect("published cancellation retains its receipt");
        assert_eq!(publication.request_id(), 11);
        assert_eq!(publication.batch_stream_generation(), 7);
    }
}
