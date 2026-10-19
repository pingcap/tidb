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
//!   coprocessor and MPP, and `CmdFlush`: not ported; they answer Go's "not
//!   support" error, `Flush` client-rust's `Unimplemented`.
//! * Go's failpoints and send/response hooks: test instruments with no
//!   counterpart here.

use std::any::Any;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tikv_client::proto::{coprocessor, debugpb, kvrpcpb};
use tikv_client::tikv::Request;
use tikv_client::{Error, Result};

use crate::cluster::Cluster;
use crate::kv_handler::KvHandler;

/// Go `UnistoreRPCClientSendHook`: a test's look at every request before the
/// store serves it.
pub type SendHook = Arc<dyn Fn(&dyn Request) + Send + Sync>;

/// Go `RPCClient`, addressed to one store (client-rust hands out a client
/// per store address; Go checks the address per request).
#[derive(Clone)]
pub struct RpcClient {
    handler: Arc<Mutex<KvHandler>>,
    cluster: Arc<Cluster>,
    addr: String,
    closed: Arc<AtomicBool>,
    send_hook: Arc<Mutex<Option<SendHook>>>,
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
    pub fn set_send_hook(&self, hook: Option<SendHook>) {
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
        self.cluster
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
            return Ok(Box::new(self.lock().kv_pessimistic_lock(request)));
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
        Err(Error::StringError(format!(
            "not support this request type {}",
            req.label()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::region_manager::InProcessRegionManager;
    use tikv_client::tikv::Client as _;

    fn client() -> RpcClient {
        let rm = Arc::new(InProcessRegionManager::new());
        let handler = Arc::new(Mutex::new(KvHandler::new(
            crate::mvcc_store::MvccStore::new(),
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
}
