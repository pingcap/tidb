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
use std::time::Duration;
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

/// The deadline a detached async-commit secondary flush runs under: the same
/// store-lifetime reasoning as the coordinator's `secondary_commit_call_budget`
/// (`CommitSecondaryMaxBackoff` plus one full-length RPC). Nothing user-visible
/// waits on this — only the background thread does.
const DETACHED_SECONDARY_COMMIT_BUDGET: Duration = Duration::from_millis(41_500);

/// Detached secondary Commits that ended in a transport, region, or key error.
/// The transaction is committed regardless; the counter exists so a probe can
/// notice systematic flush failures without touching the session path.
static DETACHED_FLUSH_FAILURES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Reads the detached secondary-flush failure counter.
///
/// Go counts the same events on
/// `metrics.SecondaryLockCleanupFailureCounterCommit` (`2pc.go`, inside the
/// goroutine `spawnWithStorePool` runs for a classic commit's secondaries):
/// the committed primary has already decided the transaction, so these
/// failures never reach the caller — they surface only as a metric.
pub fn detached_flush_failures() -> u64 {
    DETACHED_FLUSH_FAILURES.load(std::sync::atomic::Ordering::Relaxed)
}

/// One queued detached flush: the requests to publish and the shared
/// transport-owner authority they must serialize against.
struct DetachedFlushJob {
    requests: Vec<OwnedTransactionCommitRequest>,
    client_authority: std::sync::Arc<std::sync::Mutex<TonicCoprocessorClient>>,
}

static DETACHED_FLUSH_QUEUE: std::sync::OnceLock<
    std::sync::mpsc::Sender<DetachedFlushJob>,
> = std::sync::OnceLock::new();

fn detached_flush_sender() -> &'static std::sync::mpsc::Sender<DetachedFlushJob> {
    DETACHED_FLUSH_QUEUE.get_or_init(|| {
        let (sender, receiver) =
            std::sync::mpsc::channel::<DetachedFlushJob>();
        // Go hands the secondary-mutation flush to the transaction's own
        // goroutine (client-go `2pc.go :: twoPhaseCommitter.execute` ->
        // `commitMutations`), so no store resource is created or torn down
        // per COMMIT. The rust equivalent of that cheap continuation is ONE
        // persistent worker draining a queue -- not an OS thread spawned and
        // joined per commit, whose arena bind/flush and scheduler entry
        // showed up as `txn-secondary-flush` samples on every commit under
        // load. Jobs run in submission order; today's per-commit threads
        // raced arbitrarily, so FIFO is not a semantic change.
        std::thread::Builder::new()
            .name("txn-secondary-flush".to_owned())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    run_detached_flush(job.requests, job.client_authority);
                }
            })
            .expect("spawn the persistent detached-secondary flusher");
        sender
    })
}

fn run_detached_flush(
    requests: Vec<OwnedTransactionCommitRequest>,
    client_authority: std::sync::Arc<std::sync::Mutex<TonicCoprocessorClient>>,
) {
    // Holding the shared authority (not a clone) keeps the
    // transport owner alive even when the transaction and its
    // runtime are already dropped; the lock serializes this flush
    // against any concurrent user of the same authority.
    let mut client = match client_authority.lock() {
        Ok(client) => client,
        Err(poisoned) => poisoned.into_inner(),
    };
    let call = UnaryCallContext::with_timeout(DETACHED_SECONDARY_COMMIT_BUDGET);
    let refs: Vec<TransactionCommitRequest> = requests
        .iter()
        .map(|request| TransactionCommitRequest {
            address: &request.address,
            request: request.request.clone(),
            context: request.context.clone(),
        })
        .collect();
    for published in client.publish_commits(&refs, &call) {
        match published {
            PublishedCommand::Response(response) => {
                if response.response.region_error.is_some()
                    || response.response.error.is_some()
                {
                    // The transaction is committed either way —
                    // Go logs and moves on; an unmaterialized
                    // secondary write record resolves through
                    // the primary lock on read.
                    DETACHED_FLUSH_FAILURES
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            _ => {
                DETACHED_FLUSH_FAILURES
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }
}

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

/// An owned [`TransactionCommitRequest`]: the shape a detached secondary
/// flush receives, because those requests must outlive the coordinator that
/// grouped them and the session that already observed the commit.
pub struct OwnedTransactionCommitRequest {
    /// Physical TiKV leader address selected by the region cache.
    pub address: String,
    /// Region-scoped Commit request.
    pub request: KvrpcCommitRequest,
    /// Region context stamped with the transaction's resolved locks.
    pub context: KvrpcContext,
}

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

/// Typed transaction commands required from the sole shared TiKV client.
///
/// Every method publishes one command on an already-selected route and
/// completes it at the publication boundary.
pub trait TransactionCommandClient {
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

    /// Publishes a round of region-routed Commits WITHOUT waiting on their
    /// responses, answering `true` only when the round was taken over.
    ///
    /// Go `twoPhaseCommitter.execute` (`2pc.go`): an async-commit transaction
    /// is committed the moment its carrying prewrite succeeds — the follow-up
    /// secondary Commits only materialize that decision, so the session is
    /// released immediately and they are flushed on a goroutine
    /// (`cleanWg.Add(1); go ...`). The caller passes the shared client
    /// authority it routed through (`SharedReadRuntime::client_handle`), so a
    /// flush that outlives its transaction still owns a live transport until
    /// it finishes. The default `false` keeps every other client awaiting
    /// inline exactly as before.
    fn publish_commits_detached(
        &mut self,
        requests: Vec<OwnedTransactionCommitRequest>,
        client_authority: std::sync::Arc<std::sync::Mutex<Self>>,
    ) -> bool
    where
        Self: Sized,
    {
        let _ = (requests, client_authority);
        false
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

    /// Publishes one TxnHeartBeat extending the primary lock's TTL.
    fn publish_txn_heart_beat(
        &mut self,
        address: &str,
        request: &KvrpcTxnHeartBeatRequest,
        context: &KvrpcContext,
        call: &UnaryCallContext,
    ) -> PublishedCommand<KvrpcTxnHeartBeatResponse>;
}

impl TransactionCommandClient for TonicCoprocessorClient {
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
        let mut results = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<Option<PublishedCommand<KvrpcBatchGetResponse>>>>();
        let mut pending = Vec::with_capacity(requests.len());

        // Admission is synchronous only up to the transport publication
        // receipt. Keeping each pending completion alive lets the worker
        // overlap requests routed to different regions (and addresses).
        for (index, request) in requests.iter().enumerate() {
            match self.begin_transaction_batch_get(
                request.address,
                None,
                &request.request,
                &request.context,
                call,
            ) {
                Ok(pending_request) => pending.push((index, pending_request)),
                Err(error) => {
                    results[index] = Some(PublishedCommand::BeforePublication(error.to_string()));
                }
            }
        }

        for (index, mut pending_request) in pending {
            let publication = pending_request
                .publication()
                .expect("Stage A binds a nonzero publication before pending escapes")
                .clone();
            results[index] = Some(match pending_request.complete(call) {
                Ok(Ok(response)) => PublishedCommand::Response(response),
                Ok(Err(error)) => PublishedCommand::AfterPublication {
                    publication,
                    error: error.to_string(),
                },
                Err(error) => PublishedCommand::AfterPublication {
                    publication,
                    error: error.to_string(),
                },
            });
        }

        results
            .into_iter()
            .map(|result| result.expect("every admitted BatchGet has a completion result"))
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
        let mut results = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<Option<PublishedCommand<KvrpcPrewriteResponse>>>>();
        let mut pending = Vec::with_capacity(requests.len());

        // Admission is synchronous only up to the transport publication
        // receipt. Keeping each pending completion alive lets the worker
        // overlap the region batches, which is what makes a multi-region
        // commit cost one round trip instead of one per region.
        for (index, request) in requests.iter().enumerate() {
            match self.begin_transaction_prewrite(
                request.address,
                None,
                &request.request,
                &request.context,
                call,
            ) {
                Ok(pending_request) => pending.push((index, pending_request)),
                Err(error) => {
                    results[index] = Some(PublishedCommand::BeforePublication(error.to_string()));
                }
            }
        }

        for (index, mut pending_request) in pending {
            let publication = pending_request
                .publication()
                .expect("Stage A binds a nonzero publication before pending escapes")
                .clone();
            results[index] = Some(match pending_request.complete(call) {
                Ok(Ok(response)) => PublishedCommand::Response(response),
                Ok(Err(error)) => PublishedCommand::AfterPublication {
                    publication,
                    error: error.to_string(),
                },
                Err(error) => PublishedCommand::AfterPublication {
                    publication,
                    error: error.to_string(),
                },
            });
        }

        results
            .into_iter()
            .map(|result| result.expect("every admitted Prewrite has a completion result"))
            .collect()
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
        let mut results = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<Option<PublishedCommand<KvrpcCommitResponse>>>>();
        let mut pending = Vec::with_capacity(requests.len());

        // Admission is synchronous only up to the transport publication
        // receipt. Keeping each pending completion alive lets the worker
        // overlap the region batches, which is what makes a multi-region
        // secondary-commit phase cost one round trip instead of one per
        // region — Go `commitRegions`.
        for (index, request) in requests.iter().enumerate() {
            match self.begin_transaction_commit(
                request.address,
                None,
                &request.request,
                &request.context,
                call,
            ) {
                Ok(pending_request) => pending.push((index, pending_request)),
                Err(error) => {
                    results[index] = Some(PublishedCommand::BeforePublication(error.to_string()));
                }
            }
        }

        for (index, mut pending_request) in pending {
            let publication = pending_request
                .publication()
                .expect("Stage A binds a nonzero publication before pending escapes")
                .clone();
            results[index] = Some(match pending_request.complete(call) {
                Ok(Ok(response)) => PublishedCommand::Response(response),
                Ok(Err(error)) => PublishedCommand::AfterPublication {
                    publication,
                    error: error.to_string(),
                },
                Err(error) => PublishedCommand::AfterPublication {
                    publication,
                    error: error.to_string(),
                },
            });
        }

        results
            .into_iter()
            .map(|result| result.expect("every admitted Commit has a completion result"))
            .collect()
    }

    fn publish_commits_detached(
        &mut self,
        requests: Vec<OwnedTransactionCommitRequest>,
        client_authority: std::sync::Arc<std::sync::Mutex<Self>>,
    ) -> bool
    where
        Self: Sized,
    {
        if requests.is_empty() {
            return true;
        }
        // Enqueue onto the persistent flusher instead of spawning an OS
        // thread per COMMIT. A send failure means the worker is gone (it is
        // never taken down once started); report the job as not accepted,
        // exactly as a spawn failure did.
        detached_flush_sender().send(DetachedFlushJob {
            requests,
            client_authority,
        })
        .is_ok()
    }

    fn publish_pessimistic_locks(
        &mut self,
        requests: &[TransactionPessimisticLockRequest<'_>],
        call: &UnaryCallContext,
    ) -> Vec<PublishedCommand<KvrpcPessimisticLockResponse>> {
        let mut results = std::iter::repeat_with(|| None)
            .take(requests.len())
            .collect::<Vec<Option<PublishedCommand<KvrpcPessimisticLockResponse>>>>();
        let mut pending = Vec::with_capacity(requests.len());

        // Admission is synchronous only up to the transport publication
        // receipt. Keeping each pending completion alive lets the worker
        // overlap the region batches, so a multi-region locking statement
        // costs one round trip instead of one per region — Go `lockKeys`.
        for (index, request) in requests.iter().enumerate() {
            match self.begin_transaction_pessimistic_lock(
                request.address,
                None,
                &request.request,
                &request.context,
                call,
            ) {
                Ok(pending_request) => pending.push((index, pending_request)),
                Err(error) => {
                    results[index] = Some(PublishedCommand::BeforePublication(error.to_string()));
                }
            }
        }

        for (index, mut pending_request) in pending {
            let publication = pending_request
                .publication()
                .expect("Stage A binds a nonzero publication before pending escapes")
                .clone();
            results[index] = Some(match pending_request.complete(call) {
                Ok(Ok(response)) => PublishedCommand::Response(response),
                Ok(Err(error)) => PublishedCommand::AfterPublication {
                    publication,
                    error: error.to_string(),
                },
                Err(error) => PublishedCommand::AfterPublication {
                    publication,
                    error: error.to_string(),
                },
            });
        }

        results
            .into_iter()
            .map(|result| result.expect("every admitted PessimisticLock has a completion result"))
            .collect()
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
    let publication = pending
        .publication()
        .expect("Stage A binds a nonzero publication before pending escapes")
        .clone();
    match pending.complete(call) {
        Ok(Ok(response)) => PublishedCommand::Response(response),
        Ok(Err(error)) => PublishedCommand::AfterPublication {
            publication,
            error: error.to_string(),
        },
        Err(error) => PublishedCommand::AfterPublication {
            publication,
            error: error.to_string(),
        },
    }
}

#[cfg(test)]
mod detached_flusher_tests {
    use super::*;

    fn flush_threads() -> usize {
        std::fs::read_dir("/proc/self/task")
            .map(|entries| {
                entries
                    .filter_map(std::result::Result::ok)
                    .filter(|entry| {
                        // /proc comm truncates to 15 chars.
                        std::fs::read_to_string(format!("{}/comm", entry.path().display()))
                            .map(|comm| comm.trim().starts_with("txn-secondary"))
                            .unwrap_or(false)
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    fn one_request() -> OwnedTransactionCommitRequest {
        OwnedTransactionCommitRequest {
            address: "127.0.0.1:9".to_owned(),
            request: KvrpcCommitRequest::default(),
            context: KvrpcContext::default(),
        }
    }

    /// Go hands the secondary flush to the transaction's goroutine, so the
    /// rust port must not create a store resource per COMMIT either: N
    /// detached publishes enqueue onto exactly ONE persistent worker.
    #[test]
    fn repeated_detached_flushes_reuse_one_worker_thread() {
        let before = flush_threads();
        let client = TonicCoprocessorClient::new().expect("client constructs without a socket");
        let authority = std::sync::Arc::new(std::sync::Mutex::new(client));
        let mut client = authority.lock().expect("fresh authority is unlocked");
        let first = client.publish_commits_detached(
            vec![one_request()],
            std::sync::Arc::clone(&authority),
        );
        let second = client.publish_commits_detached(
            vec![one_request()],
            std::sync::Arc::clone(&authority),
        );
        drop(client);
        assert!(first, "first detached publish must be accepted");
        assert!(second, "second detached publish must be accepted");
        // Both jobs reach the worker (each fails against the dead address and
        // lands in the failure counter) -- the queue really drained.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while DETACHED_FLUSH_FAILURES.load(std::sync::atomic::Ordering::Relaxed) < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "both queued flushes should have been attempted"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        let after = flush_threads();
        assert_eq!(
            after,
            before + 1,
            "exactly one persistent flusher thread, not one per commit"
        );
    }
}
