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

//! Go `pkg/store/mockstore/unistore/rpc.go`'s `RPCClient`, restated against
//! this workspace's client seam: the in-process implementor of
//! [`tidb_txnkv::DirectUnaryClient`], answering the SAME encoded
//! `coprocessor.Request` bodies the gRPC client carries to a real TiKV —
//! which is exactly Go's trick, `RPCClient` implementing client-go's
//! `tikv.Client` over the embedded store.
//!
//! This is the piece that lets the node's read path hold a store with no
//! network under it: `DirectUnaryQueryTransport<C, R>` is generic over the
//! client, and this type is a `C`.
//!
//! # Narrowings, by name
//!
//! * `rpc.go`'s non-coprocessor arms (`CmdGet`, `CmdPrewrite`, ...) dispatch
//!   TYPED requests; this seam carries only the coprocessor body
//!   (`DirectUnaryRequest.encoded_request` is an exact `coprocessor.Request`),
//!   so the KV commands ride [`crate::kv_handler::KvHandler`] directly
//!   rather than through this client.
//! * Channel generations and liveness are gRPC concerns an in-process call
//!   does not have: `close_address*` succeed as no-ops and `liveness` always
//!   answers reachable, Go's own behavior for its in-process client.
//! * The cancellation carrier is honored by CONSTRUCTION — an in-process
//!   call completes synchronously before any cancellation could land, the
//!   same window Go's in-process dispatch has.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use prost::Message;
use tidb_proto::coprocessor;
use tidb_txnkv::rpc::{BatchCommandTag, TransactionBatchPublication, TransactionBatchResponse};
use tidb_txnkv::transaction::{PublishedCommand, TransactionCommandClient};
use tidb_txnkv::{
    DirectUnaryClient, DirectUnaryClientError, DirectUnaryRequest, DirectUnaryResponse,
    UnaryCallContext,
};

use crate::kv_handler::KvHandler;
use crate::mvcc_store::MvccStore;

/// The address every in-process response reports; Go's client reports its
/// store path the same way.
pub const IN_PROCESS_ADDRESS: &str = "unistore-in-process";

/// Go `RPCClient` over the embedded store.
#[derive(Clone, Debug)]
pub struct InProcessClient {
    handler: Arc<Mutex<KvHandler>>,
    /// Monotone identity for typed-command publications, shared by clones so
    /// two receipts never claim the same request.
    request_ids: Arc<AtomicU64>,
}

impl InProcessClient {
    /// A client over a fresh store.
    #[must_use]
    pub fn new() -> Self {
        Self::over(KvHandler::default())
    }

    /// A client over an existing handler — the store a test seeded.
    #[must_use]
    pub fn over(handler: KvHandler) -> Self {
        Self {
            handler: Arc::new(Mutex::new(handler)),
            request_ids: Arc::new(AtomicU64::new(1)),
        }
    }

    /// The store beneath, for seeding and inspection.
    pub fn with_store<T>(&self, body: impl FnOnce(&mut MvccStore) -> T) -> T {
        let mut handler = self.handler.lock().expect("the store lock");
        body(&mut handler.store)
    }

    fn dispatch(
        &mut self,
        request: &DirectUnaryRequest,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        let decoded =
            coprocessor::Request::decode(request.encoded_request.as_slice()).map_err(|err| {
                DirectUnaryClientError::InvalidRequest(format!(
                    "invalid coprocessor request body: {err}"
                ))
            })?;
        let mut handler = self.handler.lock().expect("the store lock");
        let response = crate::cophandler::handle_cop_request(&mut handler.store, &decoded);
        let mut encoded = Vec::new();
        response.encode(&mut encoded).map_err(|err| {
            DirectUnaryClientError::InvalidRequest(format!(
                "coprocessor response failed to encode: {err}"
            ))
        })?;
        Ok(DirectUnaryResponse::new(encoded, IN_PROCESS_ADDRESS, 1))
    }
}

impl Default for InProcessClient {
    fn default() -> Self {
        Self::new()
    }
}

impl DirectUnaryClient for InProcessClient {
    fn send_request(
        &mut self,
        _address: &str,
        request: &DirectUnaryRequest,
        _timeout: Duration,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        self.dispatch(request)
    }

    fn send_request_with_context(
        &mut self,
        _address: &str,
        request: &DirectUnaryRequest,
        _call: &UnaryCallContext,
    ) -> Result<DirectUnaryResponse, DirectUnaryClientError> {
        // The carrier is honored by construction: the call completes
        // synchronously (module header).
        self.dispatch(request)
    }

    fn close_address(&mut self, _address: &str) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }

    fn close_address_version(
        &mut self,
        _address: &str,
        _version: u64,
    ) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }

    fn liveness(
        &self,
        _address: &str,
        _timeout: Duration,
    ) -> Result<tidb_txnkv::region::StoreLiveness, DirectUnaryClientError> {
        Ok(tidb_txnkv::region::StoreLiveness::Reachable)
    }

    fn close(&mut self) -> Result<(), DirectUnaryClientError> {
        Ok(())
    }
}

/// Go `rpc.go`'s typed lock-recovery arms, over the same handler: the four
/// calls the read path's lock resolver makes, each an in-process dispatch.
impl tidb_txnkv::lock::LockRecoveryClient for InProcessClient {
    fn check_txn_status_for_lock(
        &mut self,
        _address: &str,
        request: &tidb_proto::KvrpcCheckTxnStatusRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcCheckTxnStatusResponse, DirectUnaryClientError> {
        let mut handler = self.handler.lock().expect("the store lock");
        Ok(handler.kv_check_txn_status(request))
    }

    fn check_secondary_locks_for_lock(
        &mut self,
        _address: &str,
        request: &tidb_proto::KvrpcCheckSecondaryLocksRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcCheckSecondaryLocksResponse, DirectUnaryClientError> {
        let mut handler = self.handler.lock().expect("the store lock");
        Ok(handler.kv_check_secondary_locks(request))
    }

    fn resolve_lock_for_read(
        &mut self,
        _address: &str,
        request: &tidb_proto::KvrpcResolveLockRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcResolveLockResponse, DirectUnaryClientError> {
        let mut handler = self.handler.lock().expect("the store lock");
        Ok(handler.kv_resolve_lock(request))
    }

    fn pessimistic_rollback_for_lock(
        &mut self,
        _address: &str,
        request: &tidb_proto::KvrpcPessimisticRollbackRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> Result<tidb_proto::KvrpcPessimisticRollbackResponse, DirectUnaryClientError> {
        let mut handler = self.handler.lock().expect("the store lock");
        Ok(handler.kv_pessimistic_rollback(request))
    }
}

/// An already-finished pending: the in-process dispatch completes inside
/// `begin`, so the handle only hands the result out once. `publication`
/// stays `None` by the trait's own rule — a synchronous pending cannot
/// prove a transport publication and must not invent identity.
#[derive(Debug)]
pub struct ImmediatePending {
    result: Option<Result<DirectUnaryResponse, DirectUnaryClientError>>,
}

impl tidb_txnkv::rpc::PendingRequest for ImmediatePending {
    fn try_complete(
        &mut self,
    ) -> Result<
        Option<Result<DirectUnaryResponse, DirectUnaryClientError>>,
        tidb_txnkv::rpc::CompletionError,
    > {
        Ok(self.result.take())
    }

    fn complete(
        &mut self,
        _call: &UnaryCallContext,
    ) -> Result<Result<DirectUnaryResponse, DirectUnaryClientError>, tidb_txnkv::rpc::CompletionError>
    {
        // A once-only handle whose result was already taken: the attempt no
        // longer exists, which is the cancelled shape.
        self.result
            .take()
            .ok_or(tidb_txnkv::rpc::CompletionError::Cancelled)
    }

    fn cancel(&mut self) {
        self.result = None;
    }
}

/// Go `rpc.go`'s typed arms: `CmdGet`, `CmdScan`, `CmdPrewrite`, `CmdCommit`,
/// `CmdBatchRollback`, `CmdPessimisticLock`, `CmdPessimisticRollback`,
/// `CmdTxnHeartBeat` each dispatch the typed kvrpcpb request straight into the
/// embedded store. Every command completes synchronously, so the publication
/// receipt is minted here — the store DID see the command — with the
/// in-process address and a client-monotone request identity.
macro_rules! publish_in_process {
    ($self:ident, $request:ident, $tag:ident, $method:ident) => {{
        let response = $self
            .handler
            .lock()
            .expect("the store lock")
            .$method($request);
        let request_id = $self.request_ids.fetch_add(1, Ordering::Relaxed);
        PublishedCommand::Response(TransactionBatchResponse {
            response,
            publication: TransactionBatchPublication::in_process(
                BatchCommandTag::$tag,
                IN_PROCESS_ADDRESS,
                request_id,
            ),
        })
    }};
}

impl TransactionCommandClient for InProcessClient {
    fn publish_transaction_get(
        &mut self,
        _address: &str,
        request: &tidb_proto::KvrpcGetRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<tidb_proto::KvrpcGetResponse> {
        publish_in_process!(self, request, Get, kv_get)
    }

    fn publish_transaction_scan(
        &mut self,
        _address: &str,
        request: &tidb_proto::KvrpcScanRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<tidb_proto::KvrpcScanResponse> {
        publish_in_process!(self, request, Scan, kv_scan)
    }

    fn publish_prewrite(
        &mut self,
        _address: &str,
        request: &tidb_proto::KvrpcPrewriteRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<tidb_proto::KvrpcPrewriteResponse> {
        publish_in_process!(self, request, Prewrite, kv_prewrite)
    }

    fn publish_commit(
        &mut self,
        _address: &str,
        request: &tidb_proto::KvrpcCommitRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<tidb_proto::KvrpcCommitResponse> {
        publish_in_process!(self, request, Commit, kv_commit)
    }

    fn publish_batch_rollback(
        &mut self,
        _address: &str,
        request: &tidb_proto::KvrpcBatchRollbackRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<tidb_proto::KvrpcBatchRollbackResponse> {
        publish_in_process!(self, request, BatchRollback, kv_batch_rollback)
    }

    fn publish_pessimistic_lock(
        &mut self,
        _address: &str,
        request: &tidb_proto::KvrpcPessimisticLockRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<tidb_proto::KvrpcPessimisticLockResponse> {
        publish_in_process!(self, request, PessimisticLock, kv_pessimistic_lock)
    }

    fn publish_pessimistic_rollback(
        &mut self,
        _address: &str,
        request: &tidb_proto::KvrpcPessimisticRollbackRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<tidb_proto::KvrpcPessimisticRollbackResponse> {
        publish_in_process!(self, request, PessimisticRollback, kv_pessimistic_rollback)
    }

    fn publish_txn_heart_beat(
        &mut self,
        _address: &str,
        request: &tidb_proto::KvrpcTxnHeartBeatRequest,
        _context: &tidb_proto::KvrpcContext,
        _call: &UnaryCallContext,
    ) -> PublishedCommand<tidb_proto::KvrpcTxnHeartBeatResponse> {
        publish_in_process!(self, request, TxnHeartBeat, kv_txn_heart_beat)
    }
}

impl tidb_txnkv::rpc::AsyncRequestDispatcher for InProcessClient {
    type Pending = ImmediatePending;

    fn begin(
        &mut self,
        _physical_address: &str,
        forwarded_host: Option<&str>,
        request: &DirectUnaryRequest,
        _call: &UnaryCallContext,
    ) -> Result<Self::Pending, DirectUnaryClientError> {
        if forwarded_host.is_some() {
            // The trait's own fail-closed rule for direct-only clients.
            return Err(DirectUnaryClientError::InvalidRequest(
                "in-process client does not support request forwarding".to_owned(),
            ));
        }
        Ok(ImmediatePending {
            result: Some(self.dispatch(request)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_proto::tipb;

    // WRITTEN: rpc.go's coverage is the store integration suites.

    /// The write-seam milestone: the REAL optimistic 2PC coordinator — the
    /// same `RealOptimisticTransaction` the production node publishes
    /// through — opens over the in-process triple (client, region loader,
    /// PD capability), takes its timestamps from the embedded TSO, prewrites
    /// and commits through `TransactionCommandClient`, and the row lands in
    /// the store. Go's precedent: a transaction over mockstore runs the
    /// identical client-go committer, only the RPC hop is elided.
    #[test]
    fn the_generic_opener_commits_through_the_in_process_store() {
        use tidb_txnkv::gc_state::TxnSafePointRefresher;
        use tidb_txnkv::region::RegionCache;
        use tidb_txnkv::transaction::{OptimisticCommitOutcome, OptimisticMutation};

        let client = InProcessClient::new();
        let cache = RegionCache::new(crate::region_loader::InProcessRegionLoader);
        let authority = tidb_txnkv::SharedReadAuthority::start(client.clone(), cache)
            .expect("the read authority starts over the in-process plane");
        let gc_state = TxnSafePointRefresher::start_with_source(|| Ok(0))
            .expect("the static-zero safe point seeds");
        let opener = tidb_txnkv::transaction::RealOptimisticTransactionOpener::from_capabilities(
            authority.opener(),
            crate::tso::InProcessPd::new(),
            Duration::from_secs(1),
            gc_state,
        )
        .expect("the opener derives from in-process capabilities");

        let transaction = opener
            .begin(16, 4096)
            .expect("a timestamp comes from the embedded TSO");
        let start_ts = transaction.start_ts();
        assert!(start_ts > 0, "the TSO issued a real timestamp");

        let key = b"the-write-seam-key".to_vec();
        let value = b"the-write-seam-value".to_vec();
        let call = UnaryCallContext::with_timeout(Duration::from_secs(1));
        let outcome = transaction
            .commit(
                // The store starts empty and the coordinator prewrites at
                // `AssertionLevel_Strict`, so the first write must be an
                // Insert (`Assertion_NotExist`) — a `put_existing` here would
                // be refused as an assertion failure, exactly as in Go.
                vec![OptimisticMutation::insert(key.clone(), value.clone())
                    .expect("a valid mutation")],
                &call,
            )
            .expect("the two-phase commit completes in-process");
        let OptimisticCommitOutcome::Committed(committed) = outcome else {
            panic!("the commit must land, got a non-committed outcome");
        };
        assert!(
            committed.receipt.commit_ts > start_ts,
            "commit_ts follows start_ts"
        );

        client.with_store(|store| {
            let read = store
                .get(&key, u64::MAX)
                .expect("the committed key reads without lock errors");
            assert_eq!(read, Some(value), "the committed value is the one written");
        });
    }

    #[test]
    fn a_cop_request_travels_the_client_seam_end_to_end() {
        // The final joint: the SAME encoded body the gRPC client would carry
        // to TiKV goes through the trait and comes back as an encoded
        // coprocessor.Response with rows in it.
        use tidb_codec::table_key::{encode_row_key_with_handle, RecordHandle};
        use tidb_datatype::Datum;
        use tidb_proto::{KvrpcMutation, KvrpcOp};

        let mut client = InProcessClient::new();
        client.with_store(|store| {
            let key = encode_row_key_with_handle(5, &RecordHandle::Int(1));
            let value = tidb_codec::encode_value(&[Datum::Int(2), Datum::Int(7)]).expect("row");
            store
                .prewrite(&crate::mvcc_store::PrewriteReq {
                    mutations: vec![KvrpcMutation {
                        op: KvrpcOp::Put as i32,
                        key: key.clone(),
                        value,
                        ..KvrpcMutation::default()
                    }],
                    primary_lock: key.clone(),
                    start_version: 10,
                    ..crate::mvcc_store::PrewriteReq::default()
                })
                .expect("prewrites");
            store.commit(&[key], 10, 11).expect("commits");
        });

        let dag = tipb::DagRequest {
            executors: vec![tipb::Executor {
                tp: Some(tipb::ExecType::TypeTableScan as i32),
                tbl_scan: Some(tipb::TableScan {
                    table_id: Some(5),
                    columns: vec![tipb::ColumnInfo {
                        column_id: Some(2),
                        tp: Some(8),
                        ..tipb::ColumnInfo::default()
                    }],
                    ..tipb::TableScan::default()
                }),
                ..tipb::Executor::default()
            }],
            ..tipb::DagRequest::default()
        };
        let mut dag_data = Vec::new();
        dag.encode(&mut dag_data).expect("encodes");
        let (start, end) = tidb_codec::table_key::get_table_handle_key_range(5);
        let cop = coprocessor::Request {
            tp: crate::cophandler::REQ_TYPE_DAG,
            data: dag_data,
            ranges: vec![coprocessor::KeyRange { start, end }],
            start_ts: 20,
            ..coprocessor::Request::default()
        };
        let mut encoded_request = Vec::new();
        cop.encode(&mut encoded_request).expect("encodes");

        let request = DirectUnaryRequest {
            endpoint: tidb_txnkv::EndpointType::TiKv,
            replica_read_type: tidb_txnkv::ClientReplicaReadType::Leader,
            replica_read: false,
            stale_read: false,
            input_request_source: String::new(),
            predicted_read_bytes: 0,
            read_replica_scope: String::new(),
            txn_scope: String::new(),
            context: tidb_proto::KvrpcContext::default(),
            encoded_request,
        };
        let response = client
            .send_request(IN_PROCESS_ADDRESS, &request, Duration::from_secs(1))
            .expect("the in-process store answers");
        let decoded = coprocessor::Response::decode(response.encoded_response.as_slice())
            .expect("a coprocessor response");
        assert!(decoded.other_error.is_empty(), "{}", decoded.other_error);
        let select =
            tipb::SelectResponse::decode(decoded.data.as_slice()).expect("a select response");
        let rows = select.chunks[0].rows_data.as_deref().expect("rows");
        let datums = tidb_codec::decode(rows, 1).expect("one datum");
        assert_eq!(datums, vec![Datum::Int(7)]);
    }

    #[test]
    fn the_full_transport_constructs_over_the_in_process_pair() {
        // The decisive bound: `DirectUnaryQueryTransport::from_read_authority`
        // demands the unary core PLUS lock recovery PLUS async dispatch.
        // With all three implemented, the node's ACTUAL query transport
        // constructs over a store with no network under it — the last type
        // barrier between --store unistore and a running node.
        use crate::region_loader::InProcessRegionLoader;
        let cache = tidb_txnkv::region::RegionCache::new(InProcessRegionLoader);
        let authority: tidb_txnkv::SharedReadAuthority<InProcessClient, InProcessRegionLoader> =
            tidb_txnkv::SharedReadAuthority::start(InProcessClient::new(), cache)
                .expect("the authority starts");
        let transport = tidb_distsql::DirectUnaryQueryTransport::from_read_authority(
            &authority.opener(),
            tidb_distsql::DirectUnaryRuntimeConfig::default(),
            tidb_txnkv::lock::FixedTimestampSource::new(42),
        );
        assert!(transport.is_ok(), "{:?}", transport.err());
    }
}
