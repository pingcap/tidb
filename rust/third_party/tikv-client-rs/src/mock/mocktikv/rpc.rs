use std::any::Any;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use prost::Message;

use crate::proto::{coprocessor, debugpb, errorpb, kvrpcpb};
use crate::store::{
    BatchCoprocessorStreamRequest, BatchCoprocessorStreamResponse, CoprocessorStreamRequest,
    CoprocessorStreamResponse, KvClient, Request,
};
use crate::{Error, Result};
use unistore::{
    Action, Assertion, AssertionLevel, MockEngine, MockError, MvccInfo, Op, PessimisticAction,
    PessimisticLockKeyResultType, PessimisticWakeUpMode, PrewriteRequest, TxnMutation,
};

use super::{Cluster, Session};

pub const REQUEST_MAX_SIZE: usize = 8 * 1024 * 1024;

pub trait CoprocessorHandler: Send + Sync + 'static {
    fn handle(
        &self,
        context: &kvrpcpb::Context,
        session: &Session,
        request: &coprocessor::Request,
    ) -> coprocessor::Response;

    fn handle_batch(
        &self,
        _context: &kvrpcpb::Context,
        _session: &Session,
        _request: &coprocessor::BatchRequest,
    ) -> Result<BatchCoprocessorStreamResponse> {
        Err(Error::Unimplemented)
    }

    fn handle_stream(
        &self,
        _context: &kvrpcpb::Context,
        _session: &Session,
        _request: &coprocessor::Request,
    ) -> Result<CoprocessorStreamResponse> {
        Err(Error::Unimplemented)
    }

    fn close(&self) {}
}

#[derive(Clone)]
pub struct RpcClient {
    cluster: Cluster,
    engine: MockEngine,
    coprocessor: Option<Arc<dyn CoprocessorHandler>>,
    address: String,
    closed: Arc<AtomicBool>,
}

impl RpcClient {
    pub fn new(cluster: Cluster, engine: MockEngine) -> Self {
        Self {
            cluster,
            engine,
            coprocessor: None,
            address: String::new(),
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_coprocessor_handler(mut self, handler: Arc<dyn CoprocessorHandler>) -> Self {
        self.coprocessor = Some(handler);
        self
    }

    pub fn for_address(&self, address: impl Into<String>) -> Self {
        let mut client = self.clone();
        client.address = address.into();
        client
    }

    pub fn cluster(&self) -> &Cluster {
        &self.cluster
    }

    pub fn engine(&self) -> MockEngine {
        self.engine.clone()
    }

    pub async fn send_to(&self, address: &str, request: &dyn Request) -> Result<Box<dyn Any>> {
        self.for_address(address).dispatch_inner(request).await
    }

    pub fn close_addr(&self, _address: &str) {}

    async fn session(&self, context: Option<&kvrpcpb::Context>) -> Result<Session> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::StringError("mock RPC client is closed".to_owned()));
        }
        let address = if self.address.is_empty() {
            context
                .and_then(|context| context.peer.as_ref())
                .and_then(|peer| self.cluster.store(peer.store_id))
                .map(|store| store.address)
                .unwrap_or_default()
        } else {
            self.address.clone()
        };
        let stores = self
            .cluster
            .checked_stores_by_addr(&address)
            .map_err(Error::GrpcAPI)?;
        if stores.is_empty() {
            return Err(Error::StringError("connect fail".to_owned()));
        }
        let store = stores
            .into_iter()
            .find(|store| {
                !matches!(
                    metapb_store_state(store.state),
                    crate::proto::metapb::StoreState::Offline
                        | crate::proto::metapb::StoreState::Tombstone
                )
            })
            .ok_or_else(|| Error::StringError("connection refused".to_owned()))?;
        Ok(Session::new(
            self.cluster.clone(),
            self.engine.clone(),
            store.id,
        ))
    }

    async fn checked_session<M: Message>(
        &self,
        context: Option<&kvrpcpb::Context>,
        request: &M,
    ) -> Result<std::result::Result<Session, errorpb::Error>> {
        let mut session = self.session(context).await?;
        if let Some(error) = session.check_request(context, request.encoded_len()) {
            Ok(Err(error))
        } else {
            Ok(Ok(session))
        }
    }

    async fn dispatch_inner(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        let any = request.as_any();

        macro_rules! unary {
            ($request:ty, $handler:ident) => {
                if let Some(request) = any.downcast_ref::<$request>() {
                    return self
                        .$handler(request)
                        .await
                        .map(|response| Box::new(response) as Box<dyn Any>);
                }
            };
        }

        unary!(kvrpcpb::GetRequest, handle_get);
        unary!(kvrpcpb::ScanRequest, handle_scan);
        unary!(kvrpcpb::PrewriteRequest, handle_prewrite);
        unary!(kvrpcpb::PessimisticLockRequest, handle_pessimistic_lock);
        unary!(
            kvrpcpb::PessimisticRollbackRequest,
            handle_pessimistic_rollback
        );
        unary!(kvrpcpb::CommitRequest, handle_commit);
        unary!(kvrpcpb::CleanupRequest, handle_cleanup);
        unary!(kvrpcpb::CheckTxnStatusRequest, handle_check_txn_status);
        unary!(
            kvrpcpb::CheckSecondaryLocksRequest,
            handle_check_secondary_locks
        );
        unary!(kvrpcpb::TxnHeartBeatRequest, handle_txn_heartbeat);
        unary!(kvrpcpb::BatchGetRequest, handle_batch_get);
        unary!(kvrpcpb::BatchRollbackRequest, handle_batch_rollback);
        unary!(kvrpcpb::ScanLockRequest, handle_scan_lock);
        unary!(kvrpcpb::ResolveLockRequest, handle_resolve_lock);
        unary!(kvrpcpb::GcRequest, handle_gc);
        unary!(kvrpcpb::DeleteRangeRequest, handle_delete_range);
        unary!(kvrpcpb::RawGetRequest, handle_raw_get);
        unary!(kvrpcpb::RawBatchGetRequest, handle_raw_batch_get);
        unary!(kvrpcpb::RawPutRequest, handle_raw_put);
        unary!(kvrpcpb::RawBatchPutRequest, handle_raw_batch_put);
        unary!(kvrpcpb::RawDeleteRequest, handle_raw_delete);
        unary!(kvrpcpb::RawBatchDeleteRequest, handle_raw_batch_delete);
        unary!(kvrpcpb::RawDeleteRangeRequest, handle_raw_delete_range);
        unary!(kvrpcpb::RawScanRequest, handle_raw_scan);
        unary!(kvrpcpb::RawCasRequest, handle_raw_cas);
        unary!(kvrpcpb::RawChecksumRequest, handle_raw_checksum);
        unary!(kvrpcpb::MvccGetByKeyRequest, handle_mvcc_get_by_key);
        unary!(
            kvrpcpb::MvccGetByStartTsRequest,
            handle_mvcc_get_by_start_ts
        );
        unary!(kvrpcpb::SplitRegionRequest, handle_split_region);
        unary!(
            debugpb::GetRegionPropertiesRequest,
            handle_region_properties
        );
        unary!(coprocessor::Request, handle_coprocessor);

        if let Some(request) = any.downcast_ref::<BatchCoprocessorStreamRequest>() {
            return self
                .handle_batch_coprocessor(request)
                .await
                .map(|response| Box::new(response) as Box<dyn Any>);
        }
        if let Some(request) = any.downcast_ref::<CoprocessorStreamRequest>() {
            return self
                .handle_coprocessor_stream(request)
                .await
                .map(|response| Box::new(response) as Box<dyn Any>);
        }

        if any.is::<kvrpcpb::UnsafeDestroyRangeRequest>() {
            panic!("unimplemented");
        }
        if any.is::<kvrpcpb::RegisterLockObserverRequest>()
            || any.is::<kvrpcpb::CheckLockObserverRequest>()
            || any.is::<kvrpcpb::RemoveLockObserverRequest>()
            || any.is::<kvrpcpb::PhysicalScanLockRequest>()
        {
            return Err(Error::Unimplemented);
        }
        Err(Error::StringError(format!(
            "unsupported this request type {}",
            request.label()
        )))
    }

    async fn handle_get(&self, request: &kvrpcpb::GetRequest) -> Result<kvrpcpb::GetResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::GetResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        assert!(
            session.key_in_region(&request.key),
            "KvGet: key not in region"
        );
        Ok(
            match self.engine.get(
                &request.key,
                request.version,
                session.isolation_level(),
                session.resolved_locks(),
            ) {
                Ok(value) => kvrpcpb::GetResponse {
                    not_found: value.is_none(),
                    value: value
                        .as_ref()
                        .map_or_else(Vec::new, |value| value.0.clone()),
                    commit_ts: if request.need_commit_ts {
                        value.as_ref().map_or(0, |value| value.1)
                    } else {
                        0
                    },
                    ..Default::default()
                },
                Err(error) => kvrpcpb::GetResponse {
                    error: Some(key_error(error)),
                    ..Default::default()
                },
            },
        )
    }

    async fn handle_scan(&self, request: &kvrpcpb::ScanRequest) -> Result<kvrpcpb::ScanResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::ScanResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        let region_end = session.raw_end_key();
        let (lower, mut upper) = if request.reverse {
            assert!(
                session.key_in_region(&request.end_key),
                "KvScan: startKey not in region"
            );
            (request.end_key.as_slice(), region_end)
        } else {
            assert!(
                session.key_in_region(&request.start_key),
                "KvScan: startKey not in region"
            );
            (request.start_key.as_slice(), region_end)
        };
        if !request.end_key.is_empty()
            && !request.reverse
            && (upper.is_empty() || request.end_key.as_slice() < upper)
        {
            upper = &request.end_key;
        }
        if request.reverse
            && !request.start_key.is_empty()
            && (upper.is_empty() || request.start_key.as_slice() < upper)
        {
            upper = &request.start_key;
        }
        let pairs = self.engine.scan(
            lower,
            upper,
            request.limit as usize,
            request.version,
            session.isolation_level(),
            session.resolved_locks(),
            request.reverse,
        );
        Ok(kvrpcpb::ScanResponse {
            pairs: pairs.into_iter().map(proto_pair).collect(),
            ..Default::default()
        })
    }

    async fn handle_prewrite(
        &self,
        request: &kvrpcpb::PrewriteRequest,
    ) -> Result<kvrpcpb::PrewriteResponse> {
        if let Some(response) = prewrite_failpoint_response(request) {
            return Ok(response);
        }
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::PrewriteResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        self.cluster
            .handle_delay(
                request.start_version,
                request
                    .context
                    .as_ref()
                    .map_or(0, |context| context.region_id),
            )
            .await;
        for mutation in &request.mutations {
            assert!(
                session.key_in_region(&mutation.key),
                "KvPrewrite: key not in region"
            );
        }
        let mut source = prewrite_request(request);
        if source.use_async_commit || source.try_one_pc {
            // TiKV chooses the commit lower bound from its own clock. A delayed
            // prewrite therefore has to fall back once that bound exceeds the
            // client's MaxCommitTs; reusing the request's stale MinCommitTs
            // would leave an already-expired async-commit transaction active.
            let server_min_commit_ts = source
                .min_commit_ts
                .max(source.start_ts.saturating_add(1))
                .max(crate::oracle::system_time_to_timestamp(
                    std::time::SystemTime::now(),
                ));
            if source.max_commit_ts != 0 && server_min_commit_ts > source.max_commit_ts {
                source.use_async_commit = false;
                source.try_one_pc = false;
            } else {
                source.min_commit_ts = server_min_commit_ts;
            }
        }
        let candidate_one_pc_commit_ts = source.try_one_pc.then_some(source.min_commit_ts);
        let mut errors = if let Some(commit_ts) = candidate_one_pc_commit_ts {
            self.engine.prewrite_one_pc(&source, commit_ts)
        } else {
            self.engine.prewrite(&source)
        };
        let succeeded = errors.iter().all(Option::is_none);
        if let Some(index) = errors.iter().position(|error| {
            error.as_ref().is_some_and(|error| {
                !matches!(
                    error,
                    MockError::Locked { .. } | MockError::SharedLocked { .. }
                )
            })
        }) {
            errors = vec![errors[index].clone()];
        }
        Ok(kvrpcpb::PrewriteResponse {
            errors: errors.into_iter().flatten().map(key_error).collect(),
            min_commit_ts: if succeeded && source.use_async_commit && !source.try_one_pc {
                source.min_commit_ts.max(source.start_ts.saturating_add(1))
            } else {
                0
            },
            one_pc_commit_ts: if succeeded {
                candidate_one_pc_commit_ts.unwrap_or(0)
            } else {
                0
            },
            ..Default::default()
        })
    }

    async fn handle_pessimistic_lock(
        &self,
        request: &kvrpcpb::PessimisticLockRequest,
    ) -> Result<kvrpcpb::PessimisticLockResponse> {
        if failpoint_string("rpcPessimisticLockResult").as_deref() == Some("notLeader") {
            return Ok(kvrpcpb::PessimisticLockResponse {
                region_error: Some(errorpb::Error {
                    not_leader: Some(errorpb::NotLeader::default()),
                    ..Default::default()
                }),
                ..Default::default()
            });
        }
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::PessimisticLockResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        for mutation in &request.mutations {
            assert!(
                session.key_in_region(&mutation.key),
                "KvPessimisticLock: key not in region"
            );
        }
        self.cluster
            .handle_delay(
                request.start_version,
                request
                    .context
                    .as_ref()
                    .map_or(0, |context| context.region_id),
            )
            .await;
        let mut source = pessimistic_lock_request(request);
        let (mut errors, mut results) = self.engine.pessimistic_lock(&source);
        let had_live_lock = errors
            .iter()
            .flatten()
            .any(|error| mock_error_has_live_holder(&self.engine, error));
        if had_live_lock && request.wait_timeout > 0 {
            let shared_conflict = errors
                .iter()
                .flatten()
                .any(|error| matches!(error, MockError::SharedLocked { .. }))
                || source
                    .mutations
                    .iter()
                    .any(|mutation| mutation.op == unistore::Op::SharedPessimisticLock);
            let wait_started = tokio::time::Instant::now();
            let deadline = tokio::time::Instant::now()
                + std::time::Duration::from_millis(request.wait_timeout as u64);
            loop {
                // The real client receives wakeups/lock errors and retries the
                // request, recomputing lock TTL from transaction elapsed time.
                // The in-process mock waits inside one RPC, so reissue the
                // engine operation here and advance its TTL by the emulated
                // client-side wait. Otherwise a waiter either returns the
                // stale Locked error after wakeup or acquires an almost-expired
                // lock, diverging from client-go's RetryPushTTL behavior.
                // ForceLock owns retrying a failed per-key result. Once the
                // shared holder is gone, let the emulated server-side waiter
                // reissue the mutation and return a successful ForceLock
                // result. Returning the stale shared-lock error instead makes
                // the resolver consume the caller's lock-wait budget before
                // ForceLock gets its source-defined retry.
                let terminal_conflict = (shared_conflict
                    && source.wake_up_mode != PessimisticWakeUpMode::ForceLock)
                    || self
                        .engine
                        .transaction_was_deadlocked(request.start_version);
                if terminal_conflict {
                    if !errors
                        .iter()
                        .flatten()
                        .any(|error| mock_error_has_live_holder(&self.engine, error))
                    {
                        break;
                    }
                } else {
                    source.ttl = request.lock_ttl.saturating_add(
                        wait_started
                            .elapsed()
                            .as_millis()
                            .try_into()
                            .unwrap_or(u64::MAX),
                    );
                    (errors, results) = self.engine.pessimistic_lock(&source);
                    if !errors.iter().flatten().any(|error| {
                        matches!(
                            error,
                            MockError::Locked { .. } | MockError::SharedLocked { .. }
                        )
                    }) {
                        break;
                    }
                }
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    break;
                }
                tokio::time::sleep(
                    std::time::Duration::from_millis(5).min(deadline.duration_since(now)),
                )
                .await;
            }
        } else if request.wait_timeout >= 0
            && errors.iter().flatten().any(|error| {
                matches!(
                    error,
                    MockError::Locked { .. } | MockError::SharedLocked { .. }
                )
            })
        {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let protobuf_results: Vec<_> = results
            .iter()
            .map(|result| kvrpcpb::PessimisticLockKeyResult {
                r#type: match result.result_type {
                    PessimisticLockKeyResultType::Normal => {
                        kvrpcpb::PessimisticLockKeyResultType::LockResultNormal as i32
                    }
                    PessimisticLockKeyResultType::LockedWithConflict => {
                        kvrpcpb::PessimisticLockKeyResultType::LockResultLockedWithConflict as i32
                    }
                    PessimisticLockKeyResultType::Failed => {
                        kvrpcpb::PessimisticLockKeyResultType::LockResultFailed as i32
                    }
                },
                value: result.value.clone(),
                existence: result.existence,
                locked_with_conflict_ts: result.locked_with_conflict_ts,
                ..Default::default()
            })
            .collect();
        let mut key_errors = errors
            .into_iter()
            .flatten()
            .map(key_error)
            .collect::<Vec<_>>();
        if had_live_lock {
            for error in &mut key_errors {
                set_lock_update_duration(error, 1);
            }
        }
        let mut response = kvrpcpb::PessimisticLockResponse {
            errors: key_errors,
            ..Default::default()
        };
        if source.wake_up_mode == PessimisticWakeUpMode::ForceLock {
            response.results = protobuf_results;
        } else if response.errors.is_empty() {
            if request.return_values {
                response.values = results.iter().map(|result| result.value.clone()).collect();
                response.not_founds = results.iter().map(|result| !result.existence).collect();
            } else if request.check_existence {
                response.not_founds = results.iter().map(|result| !result.existence).collect();
            }
        }
        Ok(response)
    }

    async fn handle_pessimistic_rollback(
        &self,
        request: &kvrpcpb::PessimisticRollbackRequest,
    ) -> Result<kvrpcpb::PessimisticRollbackResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::PessimisticRollbackResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        for key in &request.keys {
            assert!(
                session.key_in_region(key),
                "KvPessimisticRollback: key not in region"
            );
        }
        let errors = self.engine.pessimistic_rollback(
            session.raw_start_key(),
            session.raw_end_key(),
            &request.keys,
            request.start_version,
            request.for_update_ts,
        );
        Ok(kvrpcpb::PessimisticRollbackResponse {
            errors: errors.into_iter().flatten().map(key_error).collect(),
            ..Default::default()
        })
    }

    async fn handle_commit(
        &self,
        request: &kvrpcpb::CommitRequest,
    ) -> Result<kvrpcpb::CommitResponse> {
        if let Some(response) = commit_failpoint_response(request)? {
            return Ok(response);
        }
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::CommitResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        for key in &request.keys {
            assert!(session.key_in_region(key), "KvCommit: key not in region");
        }
        let mut response = kvrpcpb::CommitResponse::default();
        if let Err(error) =
            self.engine
                .commit(&request.keys, request.start_version, request.commit_version)
        {
            response.error = Some(key_error(error));
        }
        if failpoint_bool("rpcCommitTimeout") {
            return Err(Error::StringError("result undetermined".to_owned()));
        }
        Ok(response)
    }

    async fn handle_cleanup(
        &self,
        request: &kvrpcpb::CleanupRequest,
    ) -> Result<kvrpcpb::CleanupResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::CleanupResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        assert!(
            session.key_in_region(&request.key),
            "KvCleanup: key not in region"
        );
        let mut response = kvrpcpb::CleanupResponse::default();
        if let Err(error) =
            self.engine
                .cleanup(&request.key, request.start_version, request.current_ts)
        {
            if let MockError::AlreadyCommitted { commit_ts } = error {
                response.commit_version = commit_ts;
            } else {
                response.error = Some(key_error(error));
            }
        }
        Ok(response)
    }

    async fn handle_check_txn_status(
        &self,
        request: &kvrpcpb::CheckTxnStatusRequest,
    ) -> Result<kvrpcpb::CheckTxnStatusResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::CheckTxnStatusResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        assert!(
            session.key_in_region(&request.primary_key),
            "KvCheckTxnStatus: key not in region"
        );
        if request.verify_is_primary {
            if let Some(lock) = self
                .engine
                .lock_info_by_key(&request.primary_key, request.lock_ts)
            {
                if lock.primary != request.primary_key {
                    return Ok(kvrpcpb::CheckTxnStatusResponse {
                        error: Some(kvrpcpb::KeyError {
                            primary_mismatch: Some(kvrpcpb::PrimaryMismatch {
                                lock_info: Some(proto_lock_info(lock)),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    });
                }
            }
        }
        Ok(
            match self.engine.check_txn_status_with_force_sync(
                &request.primary_key,
                request.lock_ts,
                request.caller_start_ts,
                request.current_ts,
                request.rollback_if_not_exist,
                request.force_sync_commit,
                request.resolving_pessimistic_lock,
            ) {
                Ok((ttl, commit_version, action)) => kvrpcpb::CheckTxnStatusResponse {
                    lock_ttl: ttl,
                    commit_version,
                    action: proto_action(action),
                    lock_info: self
                        .engine
                        .lock_info_by_key(&request.primary_key, request.lock_ts)
                        .map(proto_lock_info),
                    ..Default::default()
                },
                Err(error) => kvrpcpb::CheckTxnStatusResponse {
                    error: Some(key_error(error)),
                    ..Default::default()
                },
            },
        )
    }

    async fn handle_txn_heartbeat(
        &self,
        request: &kvrpcpb::TxnHeartBeatRequest,
    ) -> Result<kvrpcpb::TxnHeartBeatResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::TxnHeartBeatResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        assert!(
            session.key_in_region(&request.primary_lock),
            "KvTxnHeartBeat: key not in region"
        );
        Ok(
            match self.engine.txn_heartbeat(
                &request.primary_lock,
                request.start_version,
                request.advise_lock_ttl,
            ) {
                Ok(lock_ttl) => kvrpcpb::TxnHeartBeatResponse {
                    lock_ttl,
                    ..Default::default()
                },
                Err(error) => kvrpcpb::TxnHeartBeatResponse {
                    error: Some(key_error(error)),
                    ..Default::default()
                },
            },
        )
    }

    async fn handle_check_secondary_locks(
        &self,
        request: &kvrpcpb::CheckSecondaryLocksRequest,
    ) -> Result<kvrpcpb::CheckSecondaryLocksResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::CheckSecondaryLocksResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        for key in &request.keys {
            assert!(
                session.key_in_region(key),
                "KvCheckSecondaryLocks: key not in region"
            );
        }
        Ok(
            match self
                .engine
                .check_secondary_locks(&request.keys, request.start_version)
            {
                Ok((locks, commit_ts)) => kvrpcpb::CheckSecondaryLocksResponse {
                    locks: locks.into_iter().map(proto_lock_info).collect(),
                    commit_ts,
                    ..Default::default()
                },
                Err(error) => kvrpcpb::CheckSecondaryLocksResponse {
                    error: Some(key_error(error)),
                    ..Default::default()
                },
            },
        )
    }

    async fn handle_batch_get(
        &self,
        request: &kvrpcpb::BatchGetRequest,
    ) -> Result<kvrpcpb::BatchGetResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::BatchGetResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        for key in &request.keys {
            assert!(session.key_in_region(key), "KvBatchGet: key not in region");
        }
        let mut pairs: Vec<_> = self
            .engine
            .batch_get(
                &request.keys,
                request.version,
                session.isolation_level(),
                session.resolved_locks(),
            )
            .into_iter()
            .map(proto_pair)
            .collect();
        if !request.need_commit_ts {
            for pair in &mut pairs {
                pair.commit_ts = 0;
            }
        }
        Ok(kvrpcpb::BatchGetResponse {
            pairs,
            ..Default::default()
        })
    }

    async fn handle_batch_rollback(
        &self,
        request: &kvrpcpb::BatchRollbackRequest,
    ) -> Result<kvrpcpb::BatchRollbackResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::BatchRollbackResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        for key in &request.keys {
            assert!(
                session.key_in_region(key),
                "KvBatchRollback: key not in region"
            );
        }
        Ok(
            match self.engine.rollback(&request.keys, request.start_version) {
                Ok(()) => kvrpcpb::BatchRollbackResponse::default(),
                Err(error) => kvrpcpb::BatchRollbackResponse {
                    error: Some(key_error(error)),
                    ..Default::default()
                },
            },
        )
    }

    async fn handle_scan_lock(
        &self,
        request: &kvrpcpb::ScanLockRequest,
    ) -> Result<kvrpcpb::ScanLockResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::ScanLockResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        Ok(
            match self.engine.scan_locks(
                session.raw_start_key(),
                session.raw_end_key(),
                request.max_version,
            ) {
                Ok(locks) => kvrpcpb::ScanLockResponse {
                    locks: proto_scan_locks(locks),
                    ..Default::default()
                },
                Err(error) => kvrpcpb::ScanLockResponse {
                    error: Some(key_error(error)),
                    ..Default::default()
                },
            },
        )
    }

    async fn handle_resolve_lock(
        &self,
        request: &kvrpcpb::ResolveLockRequest,
    ) -> Result<kvrpcpb::ResolveLockResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::ResolveLockResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        let result = if request.txn_infos.is_empty() {
            self.engine.resolve_lock(
                session.raw_start_key(),
                session.raw_end_key(),
                request.start_version,
                request.commit_version,
            )
        } else {
            let txn_status = request
                .txn_infos
                .iter()
                .map(|info| (info.txn, info.status))
                .collect::<std::collections::HashMap<_, _>>();
            self.engine.batch_resolve_lock(
                session.raw_start_key(),
                session.raw_end_key(),
                &txn_status,
            )
        };
        Ok(match result {
            Ok(()) => kvrpcpb::ResolveLockResponse::default(),
            Err(error) => kvrpcpb::ResolveLockResponse {
                error: Some(key_error(error)),
                ..Default::default()
            },
        })
    }

    async fn handle_gc(&self, request: &kvrpcpb::GcRequest) -> Result<kvrpcpb::GcResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::GcResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        Ok(
            match self.engine.gc(
                session.raw_start_key(),
                session.raw_end_key(),
                request.safe_point,
            ) {
                Ok(()) => kvrpcpb::GcResponse::default(),
                Err(error) => kvrpcpb::GcResponse {
                    error: Some(key_error(error)),
                    ..Default::default()
                },
            },
        )
    }

    async fn handle_delete_range(
        &self,
        request: &kvrpcpb::DeleteRangeRequest,
    ) -> Result<kvrpcpb::DeleteRangeResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::DeleteRangeResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        assert!(
            session.key_in_region(&request.start_key),
            "KvDeleteRange: key not in region"
        );
        self.engine
            .delete_range(&request.start_key, &request.end_key);
        Ok(kvrpcpb::DeleteRangeResponse::default())
    }

    async fn handle_raw_get(
        &self,
        request: &kvrpcpb::RawGetRequest,
    ) -> Result<kvrpcpb::RawGetResponse> {
        let _session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::RawGetResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        let value = self.engine.raw_get(&request.cf, &request.key);
        Ok(kvrpcpb::RawGetResponse {
            not_found: value.is_none(),
            value: value.unwrap_or_default(),
            ..Default::default()
        })
    }

    async fn handle_raw_batch_get(
        &self,
        request: &kvrpcpb::RawBatchGetRequest,
    ) -> Result<kvrpcpb::RawBatchGetResponse> {
        let _session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::RawBatchGetResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        let values = self.engine.raw_batch_get(&request.cf, &request.keys);
        Ok(kvrpcpb::RawBatchGetResponse {
            pairs: request
                .keys
                .iter()
                .cloned()
                .zip(values)
                .map(|(key, value)| kvrpcpb::KvPair {
                    key,
                    value: value.unwrap_or_default(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        })
    }

    async fn handle_raw_put(
        &self,
        request: &kvrpcpb::RawPutRequest,
    ) -> Result<kvrpcpb::RawPutResponse> {
        let _session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::RawPutResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        self.engine
            .raw_put(&request.cf, request.key.clone(), request.value.clone());
        Ok(kvrpcpb::RawPutResponse::default())
    }

    async fn handle_raw_batch_put(
        &self,
        request: &kvrpcpb::RawBatchPutRequest,
    ) -> Result<kvrpcpb::RawBatchPutResponse> {
        let _session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::RawBatchPutResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        self.engine.raw_batch_put(
            &request.cf,
            request
                .pairs
                .iter()
                .map(|pair| (pair.key.clone(), pair.value.clone())),
        );
        Ok(kvrpcpb::RawBatchPutResponse::default())
    }

    async fn handle_raw_delete(
        &self,
        request: &kvrpcpb::RawDeleteRequest,
    ) -> Result<kvrpcpb::RawDeleteResponse> {
        let _session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::RawDeleteResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        self.engine.raw_delete(&request.cf, &request.key);
        Ok(kvrpcpb::RawDeleteResponse::default())
    }

    async fn handle_raw_batch_delete(
        &self,
        request: &kvrpcpb::RawBatchDeleteRequest,
    ) -> Result<kvrpcpb::RawBatchDeleteResponse> {
        let _session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::RawBatchDeleteResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        self.engine.raw_batch_delete(&request.cf, &request.keys);
        Ok(kvrpcpb::RawBatchDeleteResponse::default())
    }

    async fn handle_raw_delete_range(
        &self,
        request: &kvrpcpb::RawDeleteRangeRequest,
    ) -> Result<kvrpcpb::RawDeleteRangeResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::RawDeleteRangeResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        assert!(
            session.raw_key_in_region(&request.start_key),
            "RawDeleteRange: start key not in region"
        );
        assert!(
            session.raw_end_key_in_region(&request.end_key),
            "RawDeleteRange: end key not in region"
        );
        let _ = self
            .engine
            .raw_delete_range(&request.cf, &request.start_key, &request.end_key);
        Ok(kvrpcpb::RawDeleteRangeResponse::default())
    }

    async fn handle_raw_scan(
        &self,
        request: &kvrpcpb::RawScanRequest,
    ) -> Result<kvrpcpb::RawScanResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::RawScanResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        let (start, end) = if request.reverse {
            let lower = if request.end_key.as_slice() > session.raw_start_key() {
                request.end_key.as_slice()
            } else {
                session.raw_start_key()
            };
            (request.start_key.as_slice(), lower)
        } else {
            let upper = if !request.end_key.is_empty()
                && (session.raw_end_key().is_empty()
                    || request.end_key.as_slice() < session.raw_end_key())
            {
                request.end_key.as_slice()
            } else {
                session.raw_end_key()
            };
            (request.start_key.as_slice(), upper)
        };
        Ok(kvrpcpb::RawScanResponse {
            kvs: self
                .engine
                .raw_scan(
                    &request.cf,
                    start,
                    end,
                    request.limit as usize,
                    request.reverse,
                )
                .into_iter()
                .map(proto_pair)
                .collect(),
            ..Default::default()
        })
    }

    async fn handle_raw_cas(
        &self,
        request: &kvrpcpb::RawCasRequest,
    ) -> Result<kvrpcpb::RawCasResponse> {
        let _session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::RawCasResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        Ok(
            match self.engine.raw_compare_and_swap(
                &request.cf,
                &request.key,
                &request.previous_value,
                request.value.clone(),
            ) {
                Ok((old, succeed)) => kvrpcpb::RawCasResponse {
                    succeed,
                    previous_not_exist: false,
                    previous_value: old,
                    ..Default::default()
                },
                Err(error) => kvrpcpb::RawCasResponse {
                    error: error.to_string(),
                    ..Default::default()
                },
            },
        )
    }

    async fn handle_raw_checksum(
        &self,
        request: &kvrpcpb::RawChecksumRequest,
    ) -> Result<kvrpcpb::RawChecksumResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::RawChecksumResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        let (checksum, total_kvs, total_bytes) =
            request
                .ranges
                .iter()
                .fold((0, 0, 0), |(checksum, count, bytes), range| {
                    let end = if !range.end_key.is_empty()
                        && (session.raw_end_key().is_empty()
                            || range.end_key.as_slice() < session.raw_end_key())
                    {
                        range.end_key.as_slice()
                    } else {
                        session.raw_end_key()
                    };
                    let value = self
                        .engine
                        .raw_checksum("CF_DEFAULT", &range.start_key, end);
                    (checksum ^ value.0, count + value.1, bytes + value.2)
                });
        Ok(kvrpcpb::RawChecksumResponse {
            checksum,
            total_kvs,
            total_bytes,
            ..Default::default()
        })
    }

    async fn handle_mvcc_get_by_key(
        &self,
        request: &kvrpcpb::MvccGetByKeyRequest,
    ) -> Result<kvrpcpb::MvccGetByKeyResponse> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::MvccGetByKeyResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        assert!(
            session.key_in_region(&request.key),
            "MvccGetByKey: key not in region"
        );
        Ok(kvrpcpb::MvccGetByKeyResponse {
            info: Some(proto_mvcc_info(self.engine.mvcc_get_by_key(&request.key))),
            ..Default::default()
        })
    }

    async fn handle_mvcc_get_by_start_ts(
        &self,
        request: &kvrpcpb::MvccGetByStartTsRequest,
    ) -> Result<kvrpcpb::MvccGetByStartTsResponse> {
        let _session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::MvccGetByStartTsResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        let value = self.engine.mvcc_get_by_start_ts(request.start_ts);
        Ok(kvrpcpb::MvccGetByStartTsResponse {
            info: Some(proto_mvcc_info(value.0)),
            key: value.1,
            ..Default::default()
        })
    }

    async fn handle_split_region(
        &self,
        request: &kvrpcpb::SplitRegionRequest,
    ) -> Result<kvrpcpb::SplitRegionResponse> {
        let _session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(kvrpcpb::SplitRegionResponse {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        let mut regions = Vec::with_capacity(request.split_keys.len() + 1);
        for (index, key) in request.split_keys.iter().enumerate() {
            let mut encoded = Vec::new();
            crate::kv::codec::encode_bytes(&mut encoded, key);
            let Some((region, _, _, _)) = self.cluster.region_by_key(&encoded) else {
                continue;
            };
            if region.start_key == *key {
                continue;
            }
            if index == 0 {
                regions.push(region.clone());
            }
            let region_id = self.cluster.alloc_id();
            let peers = self.cluster.alloc_ids(region.peers.len());
            let right = self
                .cluster
                .split_raw(region.id, region_id, &encoded, &peers, peers[0]);
            regions.push(right);
        }
        Ok(kvrpcpb::SplitRegionResponse {
            regions,
            ..Default::default()
        })
    }

    async fn handle_region_properties(
        &self,
        request: &debugpb::GetRegionPropertiesRequest,
    ) -> Result<debugpb::GetRegionPropertiesResponse> {
        let (region, _) =
            self.cluster
                .region(request.region_id)
                .ok_or(Error::RegionNotFoundInResponse {
                    region_id: request.region_id,
                })?;
        let start = decode_boundary(&region.start_key);
        let end = decode_boundary(&region.end_key);
        let rows = self
            .engine
            .scan(
                &start,
                &end,
                u32::MAX as usize,
                u64::MAX,
                unistore::IsolationLevel::SnapshotIsolation,
                &[],
                false,
            )
            .len();
        Ok(debugpb::GetRegionPropertiesResponse {
            props: vec![debugpb::Property {
                name: "mvcc.num_rows".to_owned(),
                value: rows.to_string(),
            }],
        })
    }

    async fn handle_coprocessor(
        &self,
        request: &coprocessor::Request,
    ) -> Result<coprocessor::Response> {
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(coprocessor::Response {
                    region_error: Some(region_error),
                    ..Default::default()
                })
            }
        };
        let handler = self.coprocessor.as_ref().ok_or(Error::Unimplemented)?;
        Ok(handler.handle(
            request
                .context
                .as_ref()
                .unwrap_or(&kvrpcpb::Context::default()),
            &session,
            request,
        ))
    }

    async fn handle_batch_coprocessor(
        &self,
        request: &BatchCoprocessorStreamRequest,
    ) -> Result<BatchCoprocessorStreamResponse> {
        if failpoint_bool("BatchCopCancelled") {
            return Err(Error::GrpcAPI(tonic::Status::cancelled(
                "batch coprocessor request cancelled",
            )));
        }
        if failpoint_string("BatchCopRpcErr").as_deref() == Some(self.address.as_str()) {
            return Err(Error::StringError("rpc error".to_owned()));
        }
        let request = request.inner_for_mock();
        let session = self.session(request.context.as_ref()).await?;
        let handler = self.coprocessor.as_ref().ok_or(Error::Unimplemented)?;
        handler.handle_batch(
            request
                .context
                .as_ref()
                .unwrap_or(&kvrpcpb::Context::default()),
            &session,
            request,
        )
    }

    async fn handle_coprocessor_stream(
        &self,
        request: &CoprocessorStreamRequest,
    ) -> Result<CoprocessorStreamResponse> {
        let request = request.inner_for_mock();
        let session = match self
            .checked_session(request.context.as_ref(), request)
            .await?
        {
            Ok(session) => session,
            Err(region_error) => {
                return Ok(CoprocessorStreamResponse::from_first(Some(
                    coprocessor::Response {
                        region_error: Some(region_error),
                        ..Default::default()
                    },
                )))
            }
        };
        let handler = self.coprocessor.as_ref().ok_or(Error::Unimplemented)?;
        handler.handle_stream(
            request
                .context
                .as_ref()
                .unwrap_or(&kvrpcpb::Context::default()),
            &session,
            request,
        )
    }
}

#[async_trait]
impl KvClient for RpcClient {
    async fn dispatch(&self, request: &dyn Request) -> Result<Box<dyn Any>> {
        let response = self.dispatch_inner(request).await?;
        // Source failure injection happens after the mock server has handled
        // the request. This distinction is essential for ambiguity tests: the
        // prewrite lock exists even though the caller receives no response.
        if request.as_any().is::<kvrpcpb::PrewriteRequest>() && failpoint_bool("rpcPrewriteTimeout")
        {
            return Err(Error::UndeterminedError(Box::new(Error::GrpcAPI(
                tonic::Status::deadline_exceeded("injected prewrite response timeout"),
            ))));
        }
        if failpoint_bool("rpcContextCancelErr") {
            return Err(Error::StringError("context canceled".to_owned()));
        }
        Ok(response)
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            if let Some(handler) = &self.coprocessor {
                handler.close();
            }
            self.engine.close();
        }
    }
}

pub fn put_mutations(pairs: &[(&[u8], &[u8])]) -> Vec<kvrpcpb::Mutation> {
    pairs
        .iter()
        .map(|(key, value)| kvrpcpb::Mutation {
            op: kvrpcpb::Op::Put as i32,
            key: key.to_vec(),
            value: value.to_vec(),
            ..Default::default()
        })
        .collect()
}

pub fn must_prewrite(
    engine: &MockEngine,
    mutations: &[kvrpcpb::Mutation],
    primary: &str,
    start_ts: u64,
    ttl: u64,
) -> bool {
    engine
        .prewrite(&PrewriteRequest {
            mutations: mutations.iter().map(txn_mutation).collect(),
            primary: primary.as_bytes().to_vec(),
            start_ts,
            ttl,
            min_commit_ts: start_ts + 1,
            ..Default::default()
        })
        .into_iter()
        .all(|error| error.is_none())
}

fn prewrite_request(request: &kvrpcpb::PrewriteRequest) -> PrewriteRequest {
    PrewriteRequest {
        mutations: request.mutations.iter().map(txn_mutation).collect(),
        primary: request.primary_lock.clone(),
        start_ts: request.start_version,
        ttl: request.lock_ttl,
        txn_size: request.txn_size,
        for_update_ts: request.for_update_ts,
        min_commit_ts: request.min_commit_ts,
        pessimistic_actions: request
            .pessimistic_actions
            .iter()
            .map(
                |action| match kvrpcpb::prewrite_request::PessimisticAction::try_from(*action) {
                    Ok(kvrpcpb::prewrite_request::PessimisticAction::DoPessimisticCheck) => {
                        PessimisticAction::DoCheck
                    }
                    Ok(kvrpcpb::prewrite_request::PessimisticAction::DoConstraintCheck) => {
                        PessimisticAction::ConstraintCheck
                    }
                    _ => PessimisticAction::Skip,
                },
            )
            .collect(),
        for_update_ts_constraints: request
            .for_update_ts_constraints
            .iter()
            .map(|constraint| unistore::ForUpdateTsConstraint {
                index: constraint.index,
                expected_for_update_ts: constraint.expected_for_update_ts,
            })
            .collect(),
        assertion_level: match kvrpcpb::AssertionLevel::try_from(request.assertion_level) {
            Ok(kvrpcpb::AssertionLevel::Fast) => AssertionLevel::Fast,
            Ok(kvrpcpb::AssertionLevel::Strict) => AssertionLevel::Strict,
            _ => AssertionLevel::Off,
        },
        resolved_locks: request
            .context
            .as_ref()
            .map_or_else(Vec::new, |context| context.resolved_locks.clone()),
        use_async_commit: request.use_async_commit,
        try_one_pc: request.try_one_pc,
        secondaries: request.secondaries.clone(),
        max_commit_ts: request.max_commit_ts,
    }
}

fn pessimistic_lock_request(
    request: &kvrpcpb::PessimisticLockRequest,
) -> unistore::PessimisticLockRequest {
    unistore::PessimisticLockRequest {
        mutations: request.mutations.iter().map(txn_mutation).collect(),
        primary: request.primary_lock.clone(),
        start_ts: request.start_version,
        for_update_ts: request.for_update_ts,
        ttl: request.lock_ttl,
        min_commit_ts: request.min_commit_ts,
        wait_timeout: request.wait_timeout,
        return_values: request.return_values,
        check_existence: request.check_existence,
        lock_only_if_exists: request.lock_only_if_exists,
        wake_up_mode: if request.wake_up_mode
            == kvrpcpb::PessimisticLockWakeUpMode::WakeUpModeForceLock as i32
        {
            PessimisticWakeUpMode::ForceLock
        } else {
            PessimisticWakeUpMode::Normal
        },
        resource_group_tag: request
            .context
            .as_ref()
            .map_or_else(Vec::new, |context| context.resource_group_tag.clone()),
    }
}

fn txn_mutation(mutation: &kvrpcpb::Mutation) -> TxnMutation {
    TxnMutation {
        op: match kvrpcpb::Op::try_from(mutation.op).unwrap_or(kvrpcpb::Op::Put) {
            kvrpcpb::Op::Put => Op::Put,
            kvrpcpb::Op::Del => Op::Delete,
            kvrpcpb::Op::Lock => Op::Lock,
            kvrpcpb::Op::SharedLock => Op::SharedLock,
            kvrpcpb::Op::Rollback => Op::Rollback,
            kvrpcpb::Op::Insert => Op::Insert,
            kvrpcpb::Op::PessimisticLock => Op::PessimisticLock,
            kvrpcpb::Op::SharedPessimisticLock => Op::SharedPessimisticLock,
            kvrpcpb::Op::CheckNotExists => Op::CheckNotExists,
        },
        key: mutation.key.clone(),
        value: mutation.value.clone(),
        assertion: match kvrpcpb::Assertion::try_from(mutation.assertion) {
            Ok(kvrpcpb::Assertion::Exist) => Assertion::Exist,
            Ok(kvrpcpb::Assertion::NotExist) => Assertion::NotExist,
            _ => Assertion::None,
        },
    }
}

fn key_error(error: MockError) -> kvrpcpb::KeyError {
    match error {
        MockError::Locked {
            key,
            primary,
            start_ts,
            for_update_ts,
            ttl,
            txn_size,
            lock_type,
            min_commit_ts,
            use_async_commit,
            secondaries,
        } => kvrpcpb::KeyError {
            locked: Some(kvrpcpb::LockInfo {
                key,
                primary_lock: primary,
                lock_version: start_ts,
                lock_ttl: ttl,
                txn_size,
                lock_type: proto_op(lock_type),
                lock_for_update_ts: for_update_ts,
                min_commit_ts,
                use_async_commit,
                secondaries,
                ..Default::default()
            }),
            ..Default::default()
        },
        MockError::SharedLocked { key, locks } => kvrpcpb::KeyError {
            locked: Some(kvrpcpb::LockInfo {
                key,
                lock_type: kvrpcpb::Op::SharedLock as i32,
                shared_lock_infos: locks.into_iter().map(proto_lock_info).collect(),
                ..Default::default()
            }),
            ..Default::default()
        },
        MockError::KeyAlreadyExists { key } => kvrpcpb::KeyError {
            already_exist: Some(kvrpcpb::AlreadyExist { key }),
            ..Default::default()
        },
        MockError::Conflict {
            start_ts,
            conflict_start_ts,
            conflict_commit_ts,
            key,
            ..
        } => kvrpcpb::KeyError {
            conflict: Some(kvrpcpb::WriteConflict {
                start_ts,
                conflict_ts: conflict_start_ts,
                conflict_commit_ts,
                key,
                ..Default::default()
            }),
            ..Default::default()
        },
        MockError::Deadlock {
            lock_ts,
            lock_key,
            deadlock_key_hash,
            wait_chain,
        } => kvrpcpb::KeyError {
            deadlock: Some(kvrpcpb::Deadlock {
                lock_ts,
                lock_key,
                deadlock_key_hash,
                wait_chain: wait_chain
                    .into_iter()
                    .map(|entry| crate::proto::deadlock::WaitForEntry {
                        txn: entry.transaction,
                        wait_for_txn: entry.wait_for_transaction,
                        key_hash: entry.key_hash,
                        key: entry.key,
                        resource_group_tag: entry.resource_group_tag,
                        wait_time: entry.wait_time,
                    })
                    .collect(),
                ..Default::default()
            }),
            ..Default::default()
        },
        MockError::Retryable(message) => kvrpcpb::KeyError {
            retryable: format!("retryable: {message}"),
            ..Default::default()
        },
        MockError::CommitTsExpired {
            start_ts,
            attempted_commit_ts,
            key,
            min_commit_ts,
        } => kvrpcpb::KeyError {
            commit_ts_expired: Some(kvrpcpb::CommitTsExpired {
                start_ts,
                attempted_commit_ts,
                key,
                min_commit_ts,
            }),
            ..Default::default()
        },
        MockError::TxnNotFound { start_ts, primary } => kvrpcpb::KeyError {
            txn_not_found: Some(kvrpcpb::TxnNotFound {
                start_ts,
                primary_key: primary,
            }),
            ..Default::default()
        },
        MockError::AssertionFailed {
            start_ts,
            key,
            assertion,
            existing_start_ts,
            existing_commit_ts,
        } => kvrpcpb::KeyError {
            assertion_failed: Some(kvrpcpb::AssertionFailed {
                start_ts,
                key,
                assertion: match assertion {
                    Assertion::None => kvrpcpb::Assertion::None as i32,
                    Assertion::Exist => kvrpcpb::Assertion::Exist as i32,
                    Assertion::NotExist => kvrpcpb::Assertion::NotExist as i32,
                },
                existing_start_ts,
                existing_commit_ts,
            }),
            ..Default::default()
        },
        error => kvrpcpb::KeyError {
            abort: error.to_string(),
            ..Default::default()
        },
    }
}

fn proto_pair(pair: unistore::Pair) -> kvrpcpb::KvPair {
    if let Some(error) = pair.error {
        kvrpcpb::KvPair {
            error: Some(key_error(error)),
            ..Default::default()
        }
    } else {
        kvrpcpb::KvPair {
            key: pair.key,
            value: pair.value,
            commit_ts: pair.commit_ts,
            ..Default::default()
        }
    }
}

fn mock_error_has_live_holder(engine: &MockEngine, error: &MockError) -> bool {
    match error {
        MockError::Locked {
            primary, start_ts, ..
        } => mock_lock_holder_is_live(engine, primary, *start_ts),
        MockError::SharedLocked { locks, .. } => locks
            .iter()
            .any(|lock| mock_lock_holder_is_live(engine, &lock.primary, lock.start_ts)),
        _ => false,
    }
}

fn mock_lock_holder_is_live(engine: &MockEngine, primary: &[u8], start_ts: u64) -> bool {
    let info = engine.mvcc_get_by_key(primary);
    info.lock
        .as_ref()
        .is_some_and(|lock| lock.start_ts == start_ts)
        || info
            .shared_locks
            .iter()
            .any(|lock| lock.start_ts == start_ts)
}

fn set_lock_update_duration(error: &mut kvrpcpb::KeyError, duration_ms: u64) {
    let Some(lock) = error.locked.as_mut() else {
        return;
    };
    if lock.shared_lock_infos.is_empty() {
        lock.duration_to_last_update_ms = duration_ms;
    } else {
        for holder in &mut lock.shared_lock_infos {
            holder.duration_to_last_update_ms = duration_ms;
        }
    }
}

fn proto_lock_info(lock: unistore::LockInfo) -> kvrpcpb::LockInfo {
    let lock_type = match lock.lock_type {
        Op::SharedLock => kvrpcpb::Op::Lock as i32,
        Op::SharedPessimisticLock => kvrpcpb::Op::PessimisticLock as i32,
        lock_type => proto_op(lock_type),
    };
    kvrpcpb::LockInfo {
        primary_lock: lock.primary,
        lock_version: lock.start_ts,
        key: lock.key,
        lock_ttl: lock.ttl,
        txn_size: lock.txn_size,
        lock_type,
        lock_for_update_ts: lock.for_update_ts,
        min_commit_ts: lock.min_commit_ts,
        use_async_commit: lock.use_async_commit,
        secondaries: lock.secondaries,
        ..Default::default()
    }
}

fn proto_scan_locks(locks: Vec<unistore::LockInfo>) -> Vec<kvrpcpb::LockInfo> {
    let mut by_key = BTreeMap::<Vec<u8>, (Vec<kvrpcpb::LockInfo>, Vec<kvrpcpb::LockInfo>)>::new();
    for lock in locks {
        let key = lock.key.clone();
        let (ordinary, shared) = by_key.entry(key).or_default();
        if matches!(lock.lock_type, Op::SharedLock | Op::SharedPessimisticLock) {
            shared.push(proto_lock_info(lock));
        } else {
            ordinary.push(proto_lock_info(lock));
        }
    }
    by_key
        .into_iter()
        .flat_map(|(key, (mut ordinary, shared_lock_infos))| {
            if !shared_lock_infos.is_empty() {
                ordinary.push(kvrpcpb::LockInfo {
                    key,
                    lock_type: kvrpcpb::Op::SharedLock as i32,
                    shared_lock_infos,
                    ..Default::default()
                });
            }
            ordinary
        })
        .collect()
}

fn proto_mvcc_info(info: MvccInfo) -> kvrpcpb::MvccInfo {
    kvrpcpb::MvccInfo {
        lock: info.lock.map(|lock| kvrpcpb::MvccLock {
            r#type: proto_op(lock.op),
            start_ts: lock.start_ts,
            primary: lock.primary,
            short_value: if lock.value.len() <= 64 {
                lock.value
            } else {
                Vec::new()
            },
            ..Default::default()
        }),
        writes: info
            .writes
            .into_iter()
            .map(|write| kvrpcpb::MvccWrite {
                r#type: proto_op(write.write_type),
                start_ts: write.start_ts,
                commit_ts: write.commit_ts,
                short_value: write.short_value,
                ..Default::default()
            })
            .collect(),
        values: info
            .values
            .into_iter()
            .map(|value| kvrpcpb::MvccValue {
                start_ts: value.start_ts,
                value: value.value,
            })
            .collect(),
    }
}

fn proto_op(op: Op) -> i32 {
    match op {
        Op::Put => kvrpcpb::Op::Put as i32,
        Op::Delete => kvrpcpb::Op::Del as i32,
        Op::Lock => kvrpcpb::Op::Lock as i32,
        Op::Rollback => kvrpcpb::Op::Rollback as i32,
        Op::Insert => kvrpcpb::Op::Insert as i32,
        Op::PessimisticLock => kvrpcpb::Op::PessimisticLock as i32,
        Op::CheckNotExists => kvrpcpb::Op::CheckNotExists as i32,
        Op::SharedLock => kvrpcpb::Op::SharedLock as i32,
        Op::SharedPessimisticLock => kvrpcpb::Op::SharedPessimisticLock as i32,
    }
}

fn proto_action(action: Action) -> i32 {
    match action {
        Action::NoAction => kvrpcpb::Action::NoAction as i32,
        Action::TtlExpireRollback => kvrpcpb::Action::TtlExpireRollback as i32,
        Action::LockNotExistRollback => kvrpcpb::Action::LockNotExistRollback as i32,
        Action::MinCommitTsPushed => kvrpcpb::Action::MinCommitTsPushed as i32,
        Action::TtlExpirePessimisticRollback => {
            kvrpcpb::Action::TtlExpirePessimisticRollback as i32
        }
        Action::LockNotExistDoNothing => kvrpcpb::Action::LockNotExistDoNothing as i32,
    }
}

fn metapb_store_state(value: i32) -> crate::proto::metapb::StoreState {
    crate::proto::metapb::StoreState::try_from(value)
        .unwrap_or(crate::proto::metapb::StoreState::Up)
}

fn decode_boundary(encoded: &[u8]) -> Vec<u8> {
    if encoded.is_empty() {
        return Vec::new();
    }
    let mut decoded = Vec::new();
    crate::kv::codec::decode_bytes(encoded, &mut decoded)
        .expect("cluster boundary must be memcomparable");
    decoded
}

fn failpoint_string(name: &str) -> Option<String> {
    crate::util::eval_failpoint(name, |value| value.unwrap_or_default())
        .ok()
        .flatten()
}

fn failpoint_bool(name: &str) -> bool {
    crate::util::eval_failpoint(name, |value| value.unwrap_or_default() == "true")
        .ok()
        .flatten()
        .unwrap_or(false)
}

fn prewrite_failpoint_response(
    request: &kvrpcpb::PrewriteRequest,
) -> Option<kvrpcpb::PrewriteResponse> {
    if failpoint_string("rpcAllowedOnAlmostFull").as_deref() == Some("true")
        && request
            .context
            .as_ref()
            .map_or(0, |context| context.disk_full_opt)
            != kvrpcpb::DiskFullOpt::AllowedOnAlmostFull as i32
    {
        return Some(kvrpcpb::PrewriteResponse {
            region_error: Some(errorpb::Error {
                disk_full: Some(errorpb::DiskFull {
                    store_id: vec![1, 10],
                    reason: "disk almost full".to_owned(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        });
    }
    match failpoint_string("rpcPrewriteResult").as_deref() {
        Some("notLeader") => Some(kvrpcpb::PrewriteResponse {
            region_error: Some(errorpb::Error {
                not_leader: Some(errorpb::NotLeader::default()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        Some("undeterminedResult") => Some(kvrpcpb::PrewriteResponse {
            region_error: Some(errorpb::Error {
                undetermined_result: Some(errorpb::UndeterminedResult::default()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        Some("writeConflict") => Some(kvrpcpb::PrewriteResponse {
            errors: vec![kvrpcpb::KeyError {
                conflict: Some(kvrpcpb::WriteConflict {
                    start_ts: request.start_version,
                    key: request
                        .mutations
                        .first()
                        .map_or_else(Vec::new, |mutation| mutation.key.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        _ => None,
    }
}

fn commit_failpoint_response(
    request: &kvrpcpb::CommitRequest,
) -> Result<Option<kvrpcpb::CommitResponse>> {
    if failpoint_string("rpcAllowedOnAlmostFull").as_deref() == Some("true")
        && request
            .context
            .as_ref()
            .map_or(0, |context| context.disk_full_opt)
            != kvrpcpb::DiskFullOpt::AllowedOnAlmostFull as i32
    {
        return Ok(Some(kvrpcpb::CommitResponse {
            region_error: Some(errorpb::Error {
                disk_full: Some(errorpb::DiskFull {
                    store_id: vec![1, 10],
                    reason: "disk almost full".to_owned(),
                }),
                ..Default::default()
            }),
            ..Default::default()
        }));
    }
    Ok(match failpoint_string("rpcCommitResult").as_deref() {
        Some("timeout") => {
            return Err(Error::GrpcAPI(tonic::Status::deadline_exceeded(
                "injected commit response timeout",
            )))
        }
        Some("notLeader") => Some(kvrpcpb::CommitResponse {
            region_error: Some(errorpb::Error {
                not_leader: Some(errorpb::NotLeader::default()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        Some("keyError") => Some(kvrpcpb::CommitResponse {
            error: Some(kvrpcpb::KeyError::default()),
            ..Default::default()
        }),
        Some("undeterminedResult") => Some(kvrpcpb::CommitResponse {
            region_error: Some(errorpb::Error {
                undetermined_result: Some(errorpb::UndeterminedResult::default()),
                ..Default::default()
            }),
            ..Default::default()
        }),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::mocktikv::bootstrap_with_single_store;
    use crate::proto::metapb::Peer;

    struct SourceCoprocessorHandler;

    impl CoprocessorHandler for SourceCoprocessorHandler {
        fn handle(
            &self,
            _context: &kvrpcpb::Context,
            _session: &Session,
            _request: &coprocessor::Request,
        ) -> coprocessor::Response {
            coprocessor::Response {
                data: b"unary".to_vec(),
                ..Default::default()
            }
        }

        fn handle_batch(
            &self,
            _context: &kvrpcpb::Context,
            _session: &Session,
            _request: &coprocessor::BatchRequest,
        ) -> Result<BatchCoprocessorStreamResponse> {
            Ok(BatchCoprocessorStreamResponse::from_first(Some(
                coprocessor::BatchResponse {
                    data: b"batch".to_vec(),
                    ..Default::default()
                },
            )))
        }

        fn handle_stream(
            &self,
            _context: &kvrpcpb::Context,
            _session: &Session,
            _request: &coprocessor::Request,
        ) -> Result<CoprocessorStreamResponse> {
            Ok(CoprocessorStreamResponse::from_first(Some(
                coprocessor::Response {
                    data: b"stream".to_vec(),
                    ..Default::default()
                },
            )))
        }
    }

    fn context(cluster: &Cluster, store: u64, peer: u64, region: u64) -> kvrpcpb::Context {
        kvrpcpb::Context {
            region_id: region,
            region_epoch: cluster.region(region).unwrap().0.region_epoch,
            peer: Some(Peer {
                id: peer,
                store_id: store,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn scan_lock_response_keeps_key_order_when_shared_and_exclusive_locks_mix() {
        let lock = |key: &[u8], start_ts: u64, lock_type| unistore::LockInfo {
            primary: key.to_vec(),
            start_ts,
            key: key.to_vec(),
            ttl: 1,
            txn_size: 1,
            lock_type,
            for_update_ts: start_ts,
            min_commit_ts: 0,
            use_async_commit: false,
            secondaries: Vec::new(),
        };
        let response = proto_scan_locks(vec![
            lock(b"a", 1, Op::SharedPessimisticLock),
            lock(b"a", 2, Op::SharedPessimisticLock),
            lock(b"b", 3, Op::PessimisticLock),
            lock(b"c", 4, Op::SharedPessimisticLock),
        ]);

        assert_eq!(
            response
                .iter()
                .map(|lock| lock.key.as_slice())
                .collect::<Vec<_>>(),
            [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
        );
        assert_eq!(response[0].shared_lock_infos.len(), 2);
        assert!(response[1].shared_lock_infos.is_empty());
        assert_eq!(response[2].shared_lock_infos.len(), 1);
    }

    #[tokio::test]
    async fn source_rpc_matrix_routes_transactional_and_raw_requests() {
        let engine = MockEngine::new();
        let cluster = Cluster::new(engine.clone());
        let (store, peer, region) = bootstrap_with_single_store(&cluster);
        let client = RpcClient::new(cluster.clone(), engine).for_address(format!("store{store}"));
        let context = context(&cluster, store, peer, region);

        let prewrite = kvrpcpb::PrewriteRequest {
            context: Some(context.clone()),
            mutations: put_mutations(&[(b"k", b"v")]),
            primary_lock: b"k".to_vec(),
            start_version: 1,
            ..Default::default()
        };
        assert!(client
            .handle_prewrite(&prewrite)
            .await
            .unwrap()
            .errors
            .is_empty());
        assert!(client
            .handle_commit(&kvrpcpb::CommitRequest {
                context: Some(context.clone()),
                keys: vec![b"k".to_vec()],
                start_version: 1,
                commit_version: 2,
                ..Default::default()
            })
            .await
            .unwrap()
            .error
            .is_none());
        let present = client
            .handle_get(&kvrpcpb::GetRequest {
                context: Some(context.clone()),
                key: b"k".to_vec(),
                version: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(present.value, b"v");
        assert!(!present.not_found);

        let missing = client
            .handle_get(&kvrpcpb::GetRequest {
                context: Some(context.clone()),
                key: b"missing".to_vec(),
                version: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(missing.not_found);
        assert!(missing.value.is_empty());

        client
            .handle_raw_put(&kvrpcpb::RawPutRequest {
                context: Some(context.clone()),
                key: b"raw".to_vec(),
                value: b"value".to_vec(),
                cf: "default".to_owned(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            client
                .handle_raw_get(&kvrpcpb::RawGetRequest {
                    context: Some(context.clone()),
                    key: b"raw".to_vec(),
                    cf: "default".to_owned(),
                })
                .await
                .unwrap()
                .value,
            b"value"
        );

        let missing = client
            .handle_mvcc_get_by_start_ts(&kvrpcpb::MvccGetByStartTsRequest {
                context: Some(context.clone()),
                start_ts: 999,
            })
            .await
            .unwrap();
        assert!(missing.info.is_some());
        assert!(missing.key.is_empty());

        let delete = client
            .handle_raw_delete_range(&kvrpcpb::RawDeleteRangeRequest {
                context: Some(context),
                cf: "missing".to_owned(),
                start_key: b"a".to_vec(),
                end_key: b"z".to_vec(),
            })
            .await
            .unwrap();
        assert!(delete.error.is_empty());
    }

    #[tokio::test]
    async fn source_rpc_dispatches_unary_batch_and_stream_coprocessor_commands() {
        let engine = MockEngine::new();
        let cluster = Cluster::new(engine.clone());
        let (store, peer, region) = bootstrap_with_single_store(&cluster);
        let context = context(&cluster, store, peer, region);
        let client = RpcClient::new(cluster, engine)
            .with_coprocessor_handler(Arc::new(SourceCoprocessorHandler))
            .for_address(format!("store{store}"));

        let unary = client
            .dispatch(&coprocessor::Request {
                context: Some(context.clone()),
                ..Default::default()
            })
            .await
            .unwrap()
            .downcast::<coprocessor::Response>()
            .unwrap();
        assert_eq!(unary.data, b"unary");

        let stream = client
            .dispatch(&CoprocessorStreamRequest::new(coprocessor::Request {
                context: Some(context.clone()),
                ..Default::default()
            }))
            .await
            .unwrap()
            .downcast::<CoprocessorStreamResponse>()
            .unwrap();
        assert_eq!(stream.first.unwrap().data, b"stream");

        let batch = client
            .dispatch(&BatchCoprocessorStreamRequest::new(
                coprocessor::BatchRequest {
                    context: Some(context),
                    ..Default::default()
                },
            ))
            .await
            .unwrap()
            .downcast::<BatchCoprocessorStreamResponse>()
            .unwrap();
        assert_eq!(batch.first.unwrap().data, b"batch");
    }
}
