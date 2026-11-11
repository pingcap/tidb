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

//! Go `pkg/store/mockstore/unistore/rpc.go`: `RPCClient`, the embedded
//! store's TiKV client -- here client-rust's store client
//! ([`tikv_client::tikv::Client`]), as Go's is client-go's `tikv.Client`.
//! Each typed request is served by the store's handlers ([`KvHandler`]) in
//! process.
//!
//! # Narrowings, by name
//!
//! * The raw KV commands (`rawHandler`), coprocessor streams, batch
//!   coprocessor, `CmdMPPCancel` and `CmdFlush`: not ported; they answer
//!   Go's "not support" error, `Flush` client-rust's `Unimplemented`.
//! * Go's failpoints and send/response hooks: test instruments with no
//!   counterpart here.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use std::time::Duration;

use prost::Message as _;
use tidb_proto::tipb;
use tikv_client::proto::{coprocessor, debugpb, kvrpcpb, mpp};
use tikv_client::tikv::{MppStreamRequest, MppStreamResponse, Request};
use tikv_client::{Error, Result};

use crate::cluster::Cluster;
use tidb_unistore_cophandler::mpp_task::{MppCtx, MppRpcClient, MppTaskHandler};
use tidb_unistore_tikv::kv_handler::{KvHandler, SharedDbReader};

/// Go `UnistoreRPCClientSendHook`: a test's look at every request before the
/// store serves it.
pub type UnistoreRPCClientSendHook = Arc<dyn Fn(&dyn Request) + Send + Sync>;

/// Go `RPCClient`, addressed to one store (client-rust hands out a client
/// per store address; Go checks the address per request).
#[derive(Clone)]
pub struct RpcClient {
    handler: Arc<Mutex<KvHandler>>,
    cluster: Arc<Cluster>,
    addr: String,
    closed: Arc<AtomicBool>,
    send_hook: Arc<Mutex<Option<UnistoreRPCClientSendHook>>>,
}

/// Go `RPCClient` as the connector client-rust's PD client opens store
/// clients with: a client per known store address. Go checks the address on
/// every request (`GetStoreIDByAddr`); client-rust asks once per address.
#[derive(Clone, Debug)]
pub struct RpcConnect {
    rpc: RpcClient,
}

impl RpcConnect {
    /// The connector over `rpc`'s store.
    #[must_use]
    pub fn new(rpc: RpcClient) -> Self {
        Self { rpc }
    }
}

#[async_trait]
impl tikv_client::tikv::KvConnect for RpcConnect {
    type KvClient = RpcClient;

    async fn connect(&self, address: &str) -> Result<RpcClient> {
        self.rpc
            .cluster
            .region_manager()
            .get_store_id_by_addr(address)
            .map_err(Error::StringError)?;
        Ok(self.rpc.for_address(address))
    }
}

impl std::fmt::Debug for RpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RpcClient")
            .field("addr", &self.addr)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

impl RpcClient {
    /// A client over the store's handlers and cluster, addressed to no store
    /// until [`Self::for_address`].
    #[must_use]
    pub fn new(handler: Arc<Mutex<KvHandler>>, cluster: Arc<Cluster>) -> Self {
        Self {
            handler,
            cluster,
            addr: String::new(),
            closed: Arc::new(AtomicBool::new(false)),
            send_hook: Arc::new(Mutex::new(None)),
        }
    }

    /// Go `UnistoreRPCClientSendHook.Store` (with its failpoint enabled):
    /// every client sharing this store calls `hook` on each request first;
    /// `None` removes it.
    pub fn set_send_hook(&self, hook: Option<UnistoreRPCClientSendHook>) {
        *self
            .send_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = hook;
    }

    /// The same client addressed to the store at `addr`.
    #[must_use]
    pub fn for_address(&self, addr: &str) -> Self {
        Self {
            addr: addr.to_owned(),
            ..self.clone()
        }
    }

    /// The store's handlers, for seeding and inspection.
    #[must_use]
    pub fn handler(&self) -> &Arc<Mutex<KvHandler>> {
        &self.handler
    }

    /// Go `Close`: later requests fail as cancelled.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, KvHandler> {
        self.handler
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Go `Cluster.handleDelay`: sleeps the delay a test scheduled.
    async fn handle_delay(&self, start_ts: u64, context: Option<&kvrpcpb::Context>) {
        let region_id = context.map_or(0, |context| context.region_id);
        if let Some(delay) = self.cluster.take_delay(start_ts, region_id) {
            tokio::time::sleep(delay).await;
        }
    }

    /// Go `handleDebugGetRegionProperties`: the region's row count, from a
    /// whole-region scan at the newest version.
    fn handle_debug_get_region_properties(
        &self,
        req: &debugpb::GetRegionPropertiesRequest,
    ) -> Result<debugpb::GetRegionPropertiesResponse> {
        let region =
            self.cluster
                .get_region(req.region_id)
                .ok_or(Error::RegionNotFoundInResponse {
                    region_id: req.region_id,
                })?;
        let decode = |key: &[u8]| {
            tidb_codec::decode_bytes(key)
                .map(|(_, raw)| raw)
                .map_err(|error| Error::StringError(error.to_string()))
        };
        let start = decode(&region.start_key)?;
        let end = decode(&region.end_key)?;
        let scan_resp = self.lock().kv_scan(&kvrpcpb::ScanRequest {
            context: Some(kvrpcpb::Context {
                region_id: region.id,
                region_epoch: region.region_epoch,
                ..kvrpcpb::Context::default()
            }),
            start_key: start,
            end_key: end,
            version: u64::MAX,
            limit: u32::MAX,
            ..kvrpcpb::ScanRequest::default()
        });
        if let Some(region_error) = scan_resp.region_error {
            panic!("{region_error:?}");
        }
        Ok(debugpb::GetRegionPropertiesResponse {
            props: vec![debugpb::Property {
                name: "mvcc.num_rows".to_owned(),
                value: scan_resp.pairs.len().to_string(),
            }],
            ..debugpb::GetRegionPropertiesResponse::default()
        })
    }
}

impl RpcClient {
    /// Go `Server.DispatchMPPTaskWithStoreID`.
    fn dispatch_mpp_task_with_store_id(
        &self,
        req: &mpp::DispatchTaskRequest,
        store_id: u64,
    ) -> Result<mpp::DispatchTaskResponse> {
        let meta = req.meta.clone().unwrap_or_default();
        let rpc_client: Arc<dyn MppRpcClient> = Arc::new(self.clone());
        let mpp_handler = self
            .lock()
            .create_mpp_task_handler(&meta, store_id, rpc_client)
            .map_err(Error::StringError)?;
        let store_addr = self
            .cluster
            .region_manager()
            .get_store_addr_by_store_id(store_id)
            .map_err(Error::StringError)?;
        self.execute_mpp_dispatch(req, store_addr, store_id, mpp_handler);
        Ok(mpp::DispatchTaskResponse::default())
    }

    /// Go `Server.executeMPPDispatch`: the task runs on its own thread and
    /// leaves the store's task set when it ends.
    fn execute_mpp_dispatch(
        &self,
        req: &mpp::DispatchTaskRequest,
        store_addr: String,
        store_id: u64,
        handler: Arc<MppTaskHandler>,
    ) {
        let task_id = handler.meta.task_id;
        let (read_ctx, cop_req) = self.lock().mpp_dispatch_request(req, store_id, task_id);
        let db_reader = SharedDbReader::new(Arc::clone(&self.handler), read_ctx, store_id);
        let mpp_ctx = Arc::new(MppCtx {
            rpc_client: Arc::clone(&handler.rpc_client),
            store_addr,
            task_handler: Arc::clone(&handler),
        });
        let rpc = self.clone();
        std::thread::spawn(move || {
            let resp = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                tidb_unistore_cophandler::handle_mpp_dag_req(&db_reader, &cop_req, mpp_ctx)
            }))
            .unwrap_or_else(|_| {
                // Go's goroutine has no recover: its panic ends the process.
                std::process::abort()
            });
            let mut err = rpc.lock().remove_mpp_task_handler(task_id, store_id).err();
            if !resp.other_error.is_empty() {
                err = Some(resp.other_error);
            }
            *handler
                .err
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = err;
        });
    }

    /// Go `Server.EstablishMPPConnectionWithStoreID` with
    /// `mockMPPConnectStreamServer`: every packet the sender task sends the
    /// receiver, collected until the sender closes the tunnel.
    fn establish_mpp_connection_with_store_id(
        &self,
        req: &mpp::EstablishMppConnectionRequest,
        store_id: u64,
    ) -> std::result::Result<Vec<mpp::MppDataPacket>, String> {
        let sender_task_id = req.sender_meta.as_ref().map_or(0, |meta| meta.task_id);
        let max_retry_time = 5;
        let mut mpp_handler = None;
        for _ in 0..max_retry_time {
            mpp_handler = self.lock().get_mpp_task_handler(sender_task_id, store_id)?;
            if mpp_handler.is_some() {
                break;
            }
            std::thread::sleep(Duration::from_secs(1));
        }
        let Some(mpp_handler) = mpp_handler else {
            return Err("task not found".to_owned());
        };
        let tunnel = mpp_handler.handle_establish_conn(req)?;
        let mut packets = Vec::new();
        loop {
            let (chunk, err) = tunnel.recv_chunk();
            if let Some(err) = err {
                packets.push(mpp::MppDataPacket {
                    error: Some(mpp::Error {
                        msg: err,
                        ..mpp::Error::default()
                    }),
                    ..mpp::MppDataPacket::default()
                });
                break;
            }
            let Some(chunk) = chunk else {
                // todo return io.EOF error?
                break;
            };
            // Go's gogoproto encodes the non-nullable `encode_type` always.
            let res = tipb::SelectResponse {
                chunks: vec![chunk],
                encode_type: Some(tipb::EncodeType::TypeDefault as i32),
                ..tipb::SelectResponse::default()
            };
            packets.push(mpp::MppDataPacket {
                data: res.encode_to_vec(),
                ..mpp::MppDataPacket::default()
            });
        }
        Ok(packets)
    }
}

impl MppRpcClient for RpcClient {
    /// Go `RPCClient.SendRequest(ctx, addr, CmdMPPConn)` as an MPP task's
    /// exchange receiver sends it: the stream's packets.
    fn establish_mpp_conn(
        &self,
        addr: &str,
        req: mpp::EstablishMppConnectionRequest,
    ) -> std::result::Result<Vec<mpp::MppDataPacket>, String> {
        if self.closed.load(Ordering::Acquire) {
            return Err("context canceled".to_owned());
        }
        let store_id = self.cluster.get_store_id_by_addr(addr)?;
        self.establish_mpp_connection_with_store_id(&req, store_id)
    }
}

#[async_trait]
impl tikv_client::tikv::Client for RpcClient {
    /// Go `RPCClient.SendRequest`.
    async fn dispatch(&self, req: &dyn Request) -> Result<Box<dyn Any>> {
        let hook = self
            .send_hook
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(hook) = hook {
            hook(req);
        }
        if self.closed.load(Ordering::Acquire) {
            // Go answers `context.Canceled`, which stops the backoff.
            return Err(Error::ContextCanceled);
        }
        let store_id = self
            .cluster
            .get_store_id_by_addr(&self.addr)
            .map_err(Error::StringError)?;
        let any = req.as_any();

        macro_rules! serve {
            ($request:ty, $method:ident) => {
                if let Some(request) = any.downcast_ref::<$request>() {
                    let response = self.lock().$method(request);
                    return Ok(Box::new(response));
                }
            };
        }

        serve!(kvrpcpb::GetRequest, kv_get);
        serve!(kvrpcpb::ScanRequest, kv_scan);
        if let Some(request) = any.downcast_ref::<kvrpcpb::PrewriteRequest>() {
            self.handle_delay(request.start_version, request.context.as_ref())
                .await;
            return Ok(Box::new(self.lock().kv_prewrite(request)));
        }
        if let Some(request) = any.downcast_ref::<kvrpcpb::PessimisticLockRequest>() {
            self.handle_delay(request.start_version, request.context.as_ref())
                .await;
            // Go `Server.KvPessimisticLock`: a conflicting lock is waited for
            // with the store released, so its holder can finish.
            let (response, wait) = self.lock().kv_pessimistic_lock_begin(request);
            let Some(wait) = wait else {
                return Ok(Box::new(response));
            };
            let waiter = Arc::clone(wait.waiter());
            let result = tokio::task::spawn_blocking(move || waiter.wait())
                .await
                .map_err(|error| Error::StringError(error.to_string()))?;
            return Ok(Box::new(
                self.lock()
                    .kv_pessimistic_lock_finish(request, response, wait, result),
            ));
        }
        serve!(kvrpcpb::PessimisticRollbackRequest, kv_pessimistic_rollback);
        serve!(kvrpcpb::CommitRequest, kv_commit);
        serve!(kvrpcpb::CleanupRequest, kv_cleanup);
        serve!(kvrpcpb::CheckTxnStatusRequest, kv_check_txn_status);
        serve!(
            kvrpcpb::CheckSecondaryLocksRequest,
            kv_check_secondary_locks
        );
        serve!(kvrpcpb::TxnHeartBeatRequest, kv_txn_heart_beat);
        serve!(kvrpcpb::BatchGetRequest, kv_batch_get);
        serve!(kvrpcpb::BatchRollbackRequest, kv_batch_rollback);
        serve!(kvrpcpb::ScanLockRequest, kv_scan_lock);
        serve!(kvrpcpb::ResolveLockRequest, kv_resolve_lock);
        serve!(kvrpcpb::GcRequest, kv_gc);
        serve!(kvrpcpb::DeleteRangeRequest, kv_delete_range);
        serve!(coprocessor::Request, coprocessor);
        serve!(kvrpcpb::MvccGetByKeyRequest, mvcc_get_by_key);
        serve!(kvrpcpb::MvccGetByStartTsRequest, mvcc_get_by_start_ts);
        serve!(kvrpcpb::SplitRegionRequest, split_region);
        if let Some(request) = any.downcast_ref::<debugpb::GetRegionPropertiesRequest>() {
            return Ok(Box::new(self.handle_debug_get_region_properties(request)?));
        }
        serve!(kvrpcpb::StoreSafeTsRequest, get_store_safe_ts);
        if any.is::<kvrpcpb::UnsafeDestroyRangeRequest>() {
            // Go: "Pretend it was done. Unistore does not have "destroy", and
            // the keys has already been removed one-by-one before through:
            // (dr *delRange) startEmulator()".
            return Ok(Box::new(kvrpcpb::UnsafeDestroyRangeResponse::default()));
        }
        if any.is::<kvrpcpb::FlushRequest>() {
            return Err(Error::Unimplemented);
        }
        serve!(kvrpcpb::BufferBatchGetRequest, kv_buffer_batch_get);
        if let Some(request) = any.downcast_ref::<MppStreamRequest>() {
            // Go `handleEstablishMPPConnection`: the sender task's packets,
            // collected while it runs, answer as an already-received stream.
            let rpc = self.clone();
            let request = request.0.clone();
            let packets = tokio::task::spawn_blocking(move || {
                rpc.establish_mpp_connection_with_store_id(&request, store_id)
            })
            .await
            .map_err(|error| Error::StringError(error.to_string()))?
            .map_err(Error::StringError)?;
            return Ok(Box::new(MppStreamResponse::from_packets(packets)));
        }
        if let Some(request) = any.downcast_ref::<mpp::DispatchTaskRequest>() {
            return Ok(Box::new(self.dispatch_mpp_task_with_store_id(request, store_id)?));
        }
        serve!(mpp::IsAliveRequest, is_alive);
        Err(Error::StringError(format!(
            "not support this request type {}",
            req.label()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tikv_client::tikv::Client as _;

    fn client() -> RpcClient {
        let rm = Arc::new(crate::cluster::new_region_manager());
        let handler = Arc::new(Mutex::new(KvHandler::new(
            tidb_unistore_tikv::mvcc_store::MvccStore::new(),
            Arc::clone(&rm),
        )));
        RpcClient::new(handler, Arc::new(Cluster::new(rm)))
    }

    /// Go `SendRequest`: an unknown store address fails; a known one is
    /// served; an unsupported command answers Go's error.
    #[tokio::test]
    async fn send_request_checks_the_store_and_serves_typed_requests() {
        let rpc = client();
        let get = kvrpcpb::GetRequest::default();
        let error = rpc.for_address("store9").dispatch(&get).await.unwrap_err();
        assert!(error.to_string().contains("Store not match"), "{error}");

        let rpc = rpc.for_address("store1");
        let response = rpc.dispatch(&get).await.unwrap();
        let response = response.downcast::<kvrpcpb::GetResponse>().unwrap();
        assert!(
            response.region_error.unwrap().region_not_found.is_some(),
            "the request named no region"
        );

        let destroyed = rpc
            .dispatch(&kvrpcpb::UnsafeDestroyRangeRequest::default())
            .await
            .unwrap();
        assert!(destroyed
            .downcast::<kvrpcpb::UnsafeDestroyRangeResponse>()
            .is_ok());

        let error = rpc
            .dispatch(&kvrpcpb::RawGetRequest::default())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("not support this request type"),
            "{error}"
        );

        rpc.close();
        assert!(matches!(
            rpc.dispatch(&get).await.unwrap_err(),
            Error::ContextCanceled
        ));
    }

    /// Go `mockMPPConnectionClient.Recv`: the collected packets in order,
    /// the first already received, then EOF.
    #[tokio::test]
    async fn an_mpp_stream_answers_its_collected_packets_in_order() {
        let packet = |data: &[u8]| mpp::MppDataPacket {
            data: data.to_vec(),
            ..mpp::MppDataPacket::default()
        };
        let mut stream = MppStreamResponse::from_packets(vec![packet(b"a"), packet(b"b"), packet(b"c")]);
        assert_eq!(stream.first, Some(packet(b"a")));
        assert_eq!(stream.message().await.unwrap(), Some(packet(b"b")));
        assert_eq!(stream.message().await.unwrap(), Some(packet(b"c")));
        assert_eq!(stream.message().await.unwrap(), None);

        let mut empty = MppStreamResponse::from_packets(Vec::new());
        assert_eq!(empty.first, None);
        assert_eq!(empty.message().await.unwrap(), None);
    }

    fn put(handler: &Mutex<KvHandler>, handle: i64, b: i64) {
        let key = tidb_tablecodec::table_key::encode_row_key_with_handle(
            90,
            &tidb_tablecodec::table_key::RecordHandle::Int(handle),
        );
        let value =
            tidb_codec::encode_value(&[tidb_datatype::Datum::Int(2), tidb_datatype::Datum::Int(b)])
                .unwrap();
        let store = &mut handler.lock().unwrap().store;
        store
            .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                mutations: vec![tidb_proto::KvrpcMutation {
                    op: tidb_proto::KvrpcOp::Put as i32,
                    key: key.clone(),
                    value,
                    ..Default::default()
                }],
                primary_lock: key.clone(),
                start_version: 10,
                ..Default::default()
            })
            .unwrap();
        store.commit(&[key], 10, 11).unwrap();
    }

    /// Go `DispatchMPPTaskWithStoreID` and `handleEstablishMPPConnection`: a
    /// TiFlash store runs a dispatched scan task, which passes its rows to
    /// TiDB over the connection; its task id is taken until it ends, and a
    /// store `AddStore` did not add has no MPP tasks.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_tiflash_store_runs_a_dispatched_task_and_streams_its_rows() {
        let rpc = client();
        let rm = Arc::clone(rpc.cluster.region_manager());
        let tiflash_id = rm.alloc_id();
        rm.add_store(
            tiflash_id,
            "tiflash0",
            vec![tikv_client::proto::metapb::StoreLabel {
                key: "engine".to_owned(),
                value: "tiflash".to_owned(),
            }],
        );
        let region = rm
            .get_region(tidb_unistore_tikv::region_manager::BOOTSTRAP_REGION_ID)
            .unwrap();
        rm.add_peer(region.id, tiflash_id, rm.alloc_id());
        for handle in 1..=6 {
            put(&rpc.handler, handle, handle * 10);
        }
        // An uncommitted write in the range: Go's MPP table scan reads
        // committed data only (`DBReader.Scan`), never the lock.
        let locked = tidb_tablecodec::table_key::encode_row_key_with_handle(
            90,
            &tidb_tablecodec::table_key::RecordHandle::Int(7),
        );
        rpc.handler
            .lock()
            .unwrap()
            .store
            .prewrite(&tidb_unistore_tikv::mvcc_store::PrewriteReq {
                mutations: vec![tidb_proto::KvrpcMutation {
                    op: tidb_proto::KvrpcOp::Put as i32,
                    key: locked.clone(),
                    value: tidb_codec::encode_value(&[tidb_datatype::Datum::Int(2), tidb_datatype::Datum::Int(70)])
                        .unwrap(),
                    ..Default::default()
                }],
                primary_lock: locked,
                start_version: 15,
                ..Default::default()
            })
            .unwrap();

        let meta = |task_id| mpp::TaskMeta {
            task_id,
            gather_id: 1,
            start_ts: 20,
            address: "tiflash0".to_owned(),
            ..mpp::TaskMeta::default()
        };
        let (scan_task, tidb_task) = (meta(1), meta(-1));
        let column = |id| tipb::ColumnInfo {
            column_id: Some(id),
            tp: Some(8),
            pk_handle: Some(id == 1),
            ..tipb::ColumnInfo::default()
        };
        let scan = tipb::Executor {
            tp: Some(tipb::ExecType::TypeTableScan as i32),
            tbl_scan: Some(tipb::TableScan {
                table_id: Some(90),
                columns: vec![column(1), column(2)],
                ..tipb::TableScan::default()
            }),
            ..tipb::Executor::default()
        };
        let sender = tipb::Executor {
            tp: Some(tipb::ExecType::TypeExchangeSender as i32),
            exchange_sender: Some(Box::new(tipb::ExchangeSender {
                tp: Some(tipb::ExchangeType::PassThrough as i32),
                encoded_task_meta: vec![tidb_task.encode_to_vec()],
                child: Some(Box::new(scan)),
                ..tipb::ExchangeSender::default()
            })),
            ..tipb::Executor::default()
        };
        let dag = tipb::DagRequest {
            root_executor: Some(sender),
            output_offsets: vec![0, 1],
            ..tipb::DagRequest::default()
        };
        let (start, end) = tidb_tablecodec::table_key::get_table_handle_key_range(90);
        let dispatch = mpp::DispatchTaskRequest {
            meta: Some(scan_task.clone()),
            encoded_plan: dag.encode_to_vec(),
            regions: vec![coprocessor::RegionInfo {
                region_id: region.id,
                region_epoch: region.region_epoch.clone(),
                ranges: vec![coprocessor::KeyRange { start, end }],
                ..coprocessor::RegionInfo::default()
            }],
            ..mpp::DispatchTaskRequest::default()
        };

        let error = rpc
            .for_address("store1")
            .dispatch(&dispatch)
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("cannot find mpp task set for store"),
            "{error}"
        );

        let tiflash = rpc.for_address("tiflash0");
        let resp = tiflash.dispatch(&dispatch).await.unwrap();
        let resp = resp.downcast::<mpp::DispatchTaskResponse>().unwrap();
        assert_eq!(resp.error, None);
        // The task waits for TiDB's connection, so its id is still taken.
        let error = tiflash.dispatch(&dispatch).await.unwrap_err();
        assert!(error.to_string().contains("Task 1 has been created"), "{error}");

        let conn = MppStreamRequest(mpp::EstablishMppConnectionRequest {
            sender_meta: Some(scan_task.clone()),
            receiver_meta: Some(tidb_task.clone()),
            ..mpp::EstablishMppConnectionRequest::default()
        });
        let stream = tiflash.dispatch(&conn).await.unwrap();
        let mut stream = stream.downcast::<MppStreamResponse>().unwrap();
        let mut packets = Vec::from_iter(stream.first.take());
        while let Some(packet) = stream.message().await.unwrap() {
            packets.push(packet);
        }
        let mut datums = Vec::new();
        for packet in packets {
            assert_eq!(packet.error, None);
            let select = tipb::SelectResponse::decode(packet.data.as_slice()).unwrap();
            assert_eq!(select.encode_type, Some(tipb::EncodeType::TypeDefault as i32));
            for chunk in select.chunks {
                let mut rest = chunk.rows_data.as_deref().unwrap_or_default();
                while !rest.is_empty() {
                    let (remaining, datum) = tidb_codec::decode_one(rest).unwrap();
                    datums.push(datum);
                    rest = remaining;
                }
            }
        }
        let expected: Vec<tidb_datatype::Datum> = (1..=6)
            .flat_map(|h| [tidb_datatype::Datum::Int(h), tidb_datatype::Datum::Int(h * 10)])
            .collect();
        assert_eq!(datums, expected);

        let alive = tiflash.dispatch(&mpp::IsAliveRequest::default()).await.unwrap();
        assert!(alive.downcast::<mpp::IsAliveResponse>().unwrap().available);

        // The ended task leaves the store's task set.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while rpc.lock().get_mpp_task_handler(1, tiflash_id).unwrap().is_some() {
            assert!(std::time::Instant::now() < deadline, "the task stayed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        rm.remove_store(tiflash_id);
        assert!(rm.get_mpp_task_set(tiflash_id).is_none());
    }
}
