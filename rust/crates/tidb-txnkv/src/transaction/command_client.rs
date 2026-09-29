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

//! Typed transaction commands required from the sole shared TiKV client.
//!
//! This mirrors [`crate::lock::LockRecoveryClient`]: the transaction
//! coordinator names the exact capabilities it needs from the one production
//! BatchCommands client instead of naming the concrete client type. There is
//! still exactly one production implementation, one transport, and one
//! transaction coordinator. This is not a second transaction client and not a
//! mockable transaction abstraction; it is the publication boundary of the
//! existing client, expressed as a capability so focused tests can drive the
//! coordinator's decoded-response branches that a live cluster cannot produce
//! on demand.

use prost::Message;
use tidb_proto::{
    KvrpcBatchGetRequest, KvrpcBatchGetResponse, KvrpcBatchRollbackRequest,
    KvrpcBatchRollbackResponse, KvrpcCommitRequest, KvrpcCommitResponse, KvrpcContext,
    KvrpcGetRequest, KvrpcGetResponse, KvrpcPessimisticLockRequest, KvrpcPessimisticLockResponse,
    KvrpcPessimisticRollbackRequest, KvrpcPessimisticRollbackResponse, KvrpcPrewriteRequest,
    KvrpcPrewriteResponse, KvrpcScanRequest, KvrpcScanResponse, KvrpcTxnHeartBeatRequest,
    KvrpcTxnHeartBeatResponse,
};

use crate::rpc::{
    TonicCoprocessorClient, TransactionBatchPending, TransactionBatchPublication,
    TransactionBatchResponse, UnaryCallContext,
};

/// Outcome of one transaction command relative to the publication boundary.
///
/// Publication is irrevocable: once a command is bound to a BatchCommands
/// receipt, its physical identity must survive into the receipt even when no
/// response is decoded. The three variants are the only truthful answers to
/// "did TiKV see this command, and what came back".
pub enum PublishedCommand<R> {
    /// Admission failed before the command reached BatchCommands.
    ///
    /// TiKV never saw this attempt, so it carries no publication identity.
    BeforePublication(String),
    /// The command was published but no response was decoded.
    ///
    /// The attempt may have been applied; only the caller's phase decides
    /// whether that is ambiguity or a retryable cleanup failure.
    AfterPublication {
        /// Immutable physical publication identity of the attempt.
        publication: TransactionBatchPublication,
        /// Exact completion failure text.
        error: String,
    },
    /// A decoded response, including region and key errors.
    Response(TransactionBatchResponse<R>),
}

/// One region-routed BatchGet submitted as part of a concurrent read round.
///
/// The address is borrowed from the caller's region batch; the request and
/// context are owned so all commands can be admitted before any completion is
/// awaited.
pub struct TransactionBatchGetRequest<'a> {
    /// Physical TiKV leader address selected by the region cache.
    pub address: &'a str,
    /// Region-scoped transactional BatchGet request.
    pub request: KvrpcBatchGetRequest,
    /// Region context stamped with the transaction's resolved locks.
    pub context: KvrpcContext,
}

/// One region-routed Prewrite submitted as part of a concurrent write round.
///
/// Go `twoPhaseCommitter.prewriteRegions` (`2pc.go`) admits every region
/// batch's Prewrite before waiting on any of them, so a multi-region commit
/// costs one round trip instead of one per region. The address is borrowed
/// from the caller's region batch; the request and context are owned for the
/// same reason as [`TransactionBatchGetRequest`].
pub struct TransactionPrewriteRequest<'a> {
    /// Physical TiKV leader address selected by the region cache.
    pub address: &'a str,
    /// Region-scoped Prewrite request.
    pub request: KvrpcPrewriteRequest,
    /// Region context stamped with the transaction's resolved locks.
    pub context: KvrpcContext,
}

/// One region-routed Commit submitted as part of a concurrent write round.
///
/// Go `twoPhaseCommitter.commitRegions` (`2pc.go`) admits every secondary
/// region's Commit before waiting on any of them, exactly as its prewrite
/// does. The address is borrowed from the caller's region batch; the request
/// and context are owned for the same reason as [`TransactionPrewriteRequest`].
pub struct TransactionCommitRequest<'a> {
    /// Physical TiKV leader address selected by the region cache.
    pub address: &'a str,
    /// Region-scoped Commit request.
    pub request: KvrpcCommitRequest,
    /// Region context stamped with the transaction's resolved locks.
    pub context: KvrpcContext,
}

/// Actual response of a detached commit, or its admission/transport failure.
/// Region and key errors remain in the decoded response for the observer.
pub type DetachedCommitCompletion = Result<TransactionBatchResponse<KvrpcCommitResponse>, String>;

/// One region-routed PessimisticLock submitted as part of a concurrent
/// locking round.
///
/// Go `KVTxn.LockKeys` (`client-go/txnkv/txn.lockKeys`) admits every region
/// batch's lock request before waiting on any of them, so a statement that
/// touches several regions costs one round trip instead of one per region.
/// The address is borrowed from the caller's region batch; the request and
/// context are owned for the same reason as [`TransactionPrewriteRequest`].
pub struct TransactionPessimisticLockRequest<'a> {
    /// Physical TiKV leader address selected by the region cache.
    pub address: &'a str,
    /// Region-scoped PessimisticLock request.
    pub request: KvrpcPessimisticLockRequest,
    /// Region context stamped with the transaction's resolved locks.
    pub context: KvrpcContext,
}

/// One region's pessimistic lock cleanup in a concurrently published round.
pub struct TransactionPessimisticRollbackRequest<'a> {
    /// Physical TiKV leader address selected by the region cache.
    pub address: &'a str,
    /// Region-scoped rollback request.
    pub request: KvrpcPessimisticRollbackRequest,
    /// Region context stamped with the transaction's resolved locks.
    pub context: KvrpcContext,
}

/// One admitted BatchGet whose completion can be driven independently of its
/// siblings. Dropping a transport future cancels that exact pending response.
pub type TransactionBatchGetFuture =
    futures::future::BoxFuture<'static, PublishedCommand<KvrpcBatchGetResponse>>;

/// Typed transaction commands required from the sole shared TiKV client.
///
/// Commands use an already-selected route. Synchronous methods return their
/// publication result; begin methods return independently driven completions.
pub trait TransactionCommandClient {
    /// Publishes an already encoded client-rust request. Production transport
    /// overrides this to avoid a compatibility protobuf round trip. `None`
    /// means unsupported, before publication; use the typed command instead.
    fn publish_raw_transaction<R: Message + Default>(
        &mut self,
        _address: &str,
        _tag: crate::rpc::BatchCommandTag,
        _encoded_request: Vec<u8>,
        _context: &KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Option<PublishedCommand<R>> {
        None
    }

    /// Publishes one transactional Get at the caller's snapshot timestamp.
    fn publish_transaction_get(
        &mut self,
        address: &str,
        request: &KvrpcGetRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcGetResponse>;

    /// Publishes one transactional BatchGet for keys in one selected region.
    fn publish_transaction_batch_get(
        &mut self,
        address: &str,
        request: &KvrpcBatchGetRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcBatchGetResponse>;

    /// Publishes a round of region-routed BatchGets before waiting for any
    /// response. Implementations may override this to retain all in-flight
    /// requests on the shared transport; the default preserves the original
    /// sequential behavior for alternate clients.
    fn publish_transaction_batch_gets(
        &mut self,
        requests: &[TransactionBatchGetRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<PublishedCommand<KvrpcBatchGetResponse>> {
        requests
            .iter()
            .map(|request| {
                self.publish_transaction_batch_get(
                    request.address,
                    &request.request,
                    &request.context,
                    call,
                )
            })
            .collect()
    }

    /// Admits all requests and exposes their individual completions. The
    /// snapshot owner drives them in completion order and joins retry workers
    /// before releasing its result collector, as Go asyncBatchGetByRegions does.
    fn begin_transaction_batch_gets(
        &mut self,
        requests: &[TransactionBatchGetRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<TransactionBatchGetFuture> {
        self.publish_transaction_batch_gets(requests, call)
            .into_iter()
            .map(|response| Box::pin(std::future::ready(response)) as TransactionBatchGetFuture)
            .collect()
    }

    /// Publishes one forward Scan at the caller's snapshot timestamp.
    ///
    /// TiKV answers only from the region named by `context`, so a caller that
    /// wants a whole key range keeps re-routing until the range is covered.
    fn publish_transaction_scan(
        &mut self,
        address: &str,
        request: &KvrpcScanRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcScanResponse>;

    /// Publishes one Prewrite for an immutable, region-grouped mutation batch.
    fn publish_prewrite(
        &mut self,
        address: &str,
        request: &KvrpcPrewriteRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcPrewriteResponse>;

    /// Publishes a round of region-routed Prewrites before waiting for any
    /// response, mirroring Go `twoPhaseCommitter.prewriteRegions` admitting
    /// every batch before the first completion is awaited. Implementations may
    /// override this to retain all in-flight requests on the shared transport;
    /// the default preserves sequential publication for alternate clients.
    fn publish_prewrites(
        &mut self,
        requests: &[TransactionPrewriteRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<PublishedCommand<KvrpcPrewriteResponse>> {
        requests
            .iter()
            .map(|request| {
                self.publish_prewrite(request.address, &request.request, &request.context, call)
            })
            .collect()
    }

    /// Publishes one primary or secondary Commit for a region-grouped batch.
    fn publish_commit(
        &mut self,
        address: &str,
        request: &KvrpcCommitRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcCommitResponse>;

    /// Publishes a round of region-routed Commits before waiting for any
    /// response, mirroring Go `twoPhaseCommitter.commitRegions` admitting
    /// every secondary batch concurrently. Implementations may override this
    /// to retain all in-flight requests on the shared transport; the default
    /// preserves sequential publication for alternate clients. The primary
    /// commit is NOT part of a round: Go commits the primary alone first
    /// (`commitTxn` -> `commitPrimary`), then fans the secondaries out.
    fn publish_commits(
        &mut self,
        requests: &[TransactionCommitRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<PublishedCommand<KvrpcCommitResponse>> {
        requests
            .iter()
            .map(|request| {
                self.publish_commit(request.address, &request.request, &request.context, call)
            })
            .collect()
    }

    /// Publishes one BatchRollback cleaning possibly-prewritten keys.
    fn publish_batch_rollback(
        &mut self,
        address: &str,
        request: &KvrpcBatchRollbackRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcBatchRollbackResponse>;

    /// Publishes one PessimisticLock acquiring locks at a statement's
    /// `for_update_ts`. TiKV may hold the request for its `wait_timeout`
    /// before answering, so this is the one command whose server-side latency
    /// is a protocol feature rather than a symptom.
    fn publish_pessimistic_lock(
        &mut self,
        address: &str,
        request: &KvrpcPessimisticLockRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcPessimisticLockResponse>;

    /// Publishes a round of region-routed PessimisticLocks before waiting for
    /// any response, mirroring Go `KVTxn.LockKeys` admitting every region's
    /// lock request concurrently. Implementations may override this to retain
    /// all in-flight requests on the shared transport; the default preserves
    /// sequential publication for alternate clients.
    fn publish_pessimistic_locks(
        &mut self,
        requests: &[TransactionPessimisticLockRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<PublishedCommand<KvrpcPessimisticLockResponse>> {
        requests
            .iter()
            .map(|request| {
                self.publish_pessimistic_lock(
                    request.address,
                    &request.request,
                    &request.context,
                    call,
                )
            })
            .collect()
    }

    /// Publishes one PessimisticRollback releasing acquired pessimistic locks.
    fn publish_pessimistic_rollback(
        &mut self,
        address: &str,
        request: &KvrpcPessimisticRollbackRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcPessimisticRollbackResponse>;

    /// Publishes independent region cleanups before waiting for their replies.
    /// Alternate clients retain the scalar capability's sequential behavior.
    fn publish_pessimistic_rollbacks(
        &mut self,
        requests: &[TransactionPessimisticRollbackRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<PublishedCommand<KvrpcPessimisticRollbackResponse>> {
        requests
            .iter()
            .map(|request| {
                self.publish_pessimistic_rollback(
                    request.address,
                    &request.request,
                    &request.context,
                    call,
                )
            })
            .collect()
    }

    /// Publishes one TxnHeartBeat extending the primary lock's TTL.
    fn publish_txn_heart_beat(
        &mut self,
        address: &str,
        request: &KvrpcTxnHeartBeatRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcTxnHeartBeatResponse>;
}

impl TonicCoprocessorClient {
    /// Publishes an already encoded client-rust transaction request without
    /// the tidb-proto request round trip used by the compatibility trait.
    pub(crate) fn publish_raw_transaction<R: Message + Default>(
        &mut self,
        address: &str,
        tag: crate::rpc::BatchCommandTag,
        encoded_request: Vec<u8>,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<R> {
        complete_published(
            self.begin_raw_transaction(address, tag, encoded_request, context, call)
                .map_err(|error| error.to_string()),
            call,
        )
    }
}

impl TransactionCommandClient for TonicCoprocessorClient {
    fn publish_raw_transaction<R: Message + Default>(
        &mut self,
        address: &str,
        tag: crate::rpc::BatchCommandTag,
        encoded_request: Vec<u8>,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> Option<PublishedCommand<R>> {
        Some(TonicCoprocessorClient::publish_raw_transaction(
            self,
            address,
            tag,
            encoded_request,
            context,
            call,
        ))
    }

    fn publish_transaction_get(
        &mut self,
        address: &str,
        request: &KvrpcGetRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcGetResponse> {
        complete_published(
            self.begin_transaction_get(address, None, request, context, call)
                .map_err(|error| error.to_string()),
            call,
        )
    }

    fn publish_transaction_batch_get(
        &mut self,
        address: &str,
        request: &KvrpcBatchGetRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcBatchGetResponse> {
        complete_published(
            self.begin_transaction_batch_get(address, None, request, context, call)
                .map_err(|error| error.to_string()),
            call,
        )
    }

    fn publish_transaction_batch_gets(
        &mut self,
        requests: &[TransactionBatchGetRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<PublishedCommand<KvrpcBatchGetResponse>> {
        complete_published_batch(
            requests.iter().map(|request| {
                self.begin_transaction_batch_get(
                    request.address,
                    None,
                    &request.request,
                    &request.context,
                    call,
                )
                .map_err(|error| error.to_string())
            }),
            call,
        )
    }

    fn begin_transaction_batch_gets(
        &mut self,
        requests: &[TransactionBatchGetRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<TransactionBatchGetFuture> {
        requests
            .iter()
            .map(|request| {
                let pending = self
                    .begin_transaction_batch_get(
                        request.address,
                        None,
                        &request.request,
                        &request.context,
                        call,
                    )
                    .map_err(|error| error.to_string());
                Box::pin(async move {
                    let mut pending = match pending {
                        Ok(pending) => pending,
                        Err(error) => return PublishedCommand::BeforePublication(error),
                    };
                    let completed = std::future::poll_fn(|cx| pending.poll_complete(cx)).await;
                    let error = match completed {
                        Ok(Ok(response)) => return PublishedCommand::Response(response),
                        Ok(Err(error)) => error.to_string(),
                        Err(error) => error.to_string(),
                    };
                    match pending.publication().cloned() {
                        Some(publication) => {
                            PublishedCommand::AfterPublication { publication, error }
                        }
                        None => PublishedCommand::BeforePublication(error),
                    }
                }) as TransactionBatchGetFuture
            })
            .collect()
    }

    fn publish_transaction_scan(
        &mut self,
        address: &str,
        request: &KvrpcScanRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcScanResponse> {
        complete_published(
            self.begin_transaction_scan(address, None, request, context, call)
                .map_err(|error| error.to_string()),
            call,
        )
    }

    fn publish_prewrite(
        &mut self,
        address: &str,
        request: &KvrpcPrewriteRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcPrewriteResponse> {
        complete_published(
            self.begin_transaction_prewrite(address, None, request, context, call)
                .map_err(|error| error.to_string()),
            call,
        )
    }

    fn publish_prewrites(
        &mut self,
        requests: &[TransactionPrewriteRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<PublishedCommand<KvrpcPrewriteResponse>> {
        complete_published_batch(
            requests.iter().map(|request| {
                self.begin_transaction_prewrite(
                    request.address,
                    None,
                    &request.request,
                    &request.context,
                    call,
                )
                .map_err(|error| error.to_string())
            }),
            call,
        )
    }

    fn publish_commit(
        &mut self,
        address: &str,
        request: &KvrpcCommitRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcCommitResponse> {
        complete_published(
            self.begin_transaction_commit(address, None, request, context, call)
                .map_err(|error| error.to_string()),
            call,
        )
    }

    fn publish_commits(
        &mut self,
        requests: &[TransactionCommitRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<PublishedCommand<KvrpcCommitResponse>> {
        complete_published_batch(
            requests.iter().map(|request| {
                self.begin_transaction_commit(
                    request.address,
                    None,
                    &request.request,
                    &request.context,
                    call,
                )
                .map_err(|error| error.to_string())
            }),
            call,
        )
    }

    fn publish_pessimistic_locks(
        &mut self,
        requests: &[TransactionPessimisticLockRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<PublishedCommand<KvrpcPessimisticLockResponse>> {
        complete_published_batch(
            requests.iter().map(|request| {
                self.begin_transaction_pessimistic_lock(
                    request.address,
                    None,
                    &request.request,
                    &request.context,
                    call,
                )
                .map_err(|error| error.to_string())
            }),
            call,
        )
    }

    fn publish_batch_rollback(
        &mut self,
        address: &str,
        request: &KvrpcBatchRollbackRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcBatchRollbackResponse> {
        complete_published(
            self.begin_transaction_batch_rollback(address, None, request, context, call)
                .map_err(|error| error.to_string()),
            call,
        )
    }

    fn publish_pessimistic_lock(
        &mut self,
        address: &str,
        request: &KvrpcPessimisticLockRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcPessimisticLockResponse> {
        complete_published(
            self.begin_transaction_pessimistic_lock(address, None, request, context, call)
                .map_err(|error| error.to_string()),
            call,
        )
    }

    fn publish_pessimistic_rollback(
        &mut self,
        address: &str,
        request: &KvrpcPessimisticRollbackRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcPessimisticRollbackResponse> {
        complete_published(
            self.begin_transaction_pessimistic_rollback(address, None, request, context, call)
                .map_err(|error| error.to_string()),
            call,
        )
    }

    fn publish_txn_heart_beat(
        &mut self,
        address: &str,
        request: &KvrpcTxnHeartBeatRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcTxnHeartBeatResponse> {
        complete_published(
            self.begin_transaction_heart_beat(address, None, request, context, call)
                .map_err(|error| error.to_string()),
            call,
        )
    }

    fn publish_pessimistic_rollbacks(
        &mut self,
        requests: &[TransactionPessimisticRollbackRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<PublishedCommand<KvrpcPessimisticRollbackResponse>> {
        complete_published_batch(
            requests.iter().map(|request| {
                self.begin_transaction_pessimistic_rollback(
                    request.address,
                    None,
                    &request.request,
                    &request.context,
                    call,
                )
                .map_err(|error| error.to_string())
            }),
            call,
        )
    }
}

fn complete_published_batch<R>(
    pending: impl Iterator<Item = Result<TransactionBatchPending<R>, String>>,
    call: &UnaryCallContext,
) -> Vec<PublishedCommand<R>>
where
    R: Message + Default,
{
    // Go doActionOnBatches overlaps region requests. Consume every admission
    // before waiting on any response, retaining caller order and sibling errors.
    let pending = pending.collect::<Vec<_>>();
    pending
        .into_iter()
        .map(|request| complete_published(request, call))
        .collect()
}

fn complete_published<R>(
    pending: Result<TransactionBatchPending<R>, String>,
    call: &UnaryCallContext,
) -> PublishedCommand<R>
where
    R: Message + Default,
{
    let mut pending = match pending {
        Ok(pending) => pending,
        Err(error) => return PublishedCommand::BeforePublication(error),
    };
    // Go sendBatchRequest waits directly on the response. Only a failure needs
    // the ordered publication observation to distinguish an unpublished entry
    // from an attempt that may already have been applied by TiKV.
    let completed = pending.complete(call);
    let error = match completed {
        Ok(Ok(response)) => return PublishedCommand::Response(response),
        Ok(Err(error)) => error.to_string(),
        Err(error) => error.to_string(),
    };
    match pending.publication().cloned() {
        Some(publication) => PublishedCommand::AfterPublication { publication, error },
        None => PublishedCommand::BeforePublication(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn transaction_admission_errors_remain_before_publication() {
        // Go sendBatchRequest returns connection/admission errors to its caller;
        // queuing an entry does not imply that any request reached TiKV.
        let mut client = TonicCoprocessorClient::new().unwrap();
        let address = "http://[invalid";
        macro_rules! check_batch {
            ($method:ident, $request:ident) => {
                let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
                let results = client.$method(
                    &[$request {
                        address,
                        request: Default::default(),
                        context: Default::default(),
                    }],
                    &call,
                );
                assert_eq!(results.len(), 1);
                assert!(matches!(&results[0], PublishedCommand::BeforePublication(error) if !error.is_empty()));
            };
        }
        let results = client.publish_transaction_batch_gets(
            &[TransactionBatchGetRequest {
                address,
                request: Default::default(),
                context: Default::default(),
            }],
            &UnaryCallContext::with_timeout(Duration::from_secs(2)),
        );
        assert!(
            matches!(&results[0], PublishedCommand::BeforePublication(error) if !error.is_empty())
        );
        let call = UnaryCallContext::with_timeout(Duration::from_secs(2));
        let pending = client.begin_transaction_batch_gets(
            &[TransactionBatchGetRequest {
                address,
                request: Default::default(),
                context: Default::default(),
            }],
            &call,
        );
        let results =
            crate::rpc::wait_with_call(futures::future::join_all(pending), &call).unwrap();
        assert_eq!(results.len(), 1);
        assert!(
            matches!(&results[0], PublishedCommand::BeforePublication(error) if !error.is_empty())
        );
        check_batch!(publish_prewrites, TransactionPrewriteRequest);
        check_batch!(publish_commits, TransactionCommitRequest);
        check_batch!(publish_pessimistic_locks, TransactionPessimisticLockRequest);
        check_batch!(
            publish_pessimistic_rollbacks,
            TransactionPessimisticRollbackRequest
        );
        assert!(matches!(
            client.publish_commit(
                address,
                &Default::default(),
                &Default::default(),
                &UnaryCallContext::with_timeout(Duration::from_secs(2)),
            ),
            PublishedCommand::BeforePublication(error) if !error.is_empty()
        ));
    }
}
