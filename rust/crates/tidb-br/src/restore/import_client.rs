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

//! Go `br/pkg/restore/internal/import_client`: the ImportSST client of each
//! store, one connection per store for imports and one for ingests.
//!
//! The KV-file apply, clear and batch-download RPCs serve log and compacted
//! restore and are not ported.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_proto::import_sstpb::{self, import_sst_client::ImportSstClient};
use tonic::transport::Channel;

use super::split::SplitClient;
use crate::errors::{self, Result};

/// Go `gRPCBackOffMaxDelay`.
const GRPC_BACKOFF_MAX_DELAY: Duration = Duration::from_secs(3);

/// Go `ImporterClient`, the RPCs snapshot restore sends.
pub trait ImporterClient: Send + Sync {
    /// Go `DownloadSST`.
    ///
    /// # Errors
    /// The connection or RPC error.
    fn download_sst(
        &self,
        ctx: &Context,
        store_id: u64,
        req: import_sstpb::DownloadRequest,
    ) -> Result<import_sstpb::DownloadResponse>;
    /// Go `MultiIngest`.
    ///
    /// # Errors
    /// The connection or RPC error.
    fn multi_ingest(
        &self,
        ctx: &Context,
        store_id: u64,
        req: import_sstpb::MultiIngestRequest,
    ) -> Result<import_sstpb::IngestResponse>;
    /// Go `SetDownloadSpeedLimit`.
    ///
    /// # Errors
    /// The connection or RPC error.
    fn set_download_speed_limit(
        &self,
        ctx: &Context,
        store_id: u64,
        req: import_sstpb::SetDownloadSpeedLimitRequest,
    ) -> Result<import_sstpb::SetDownloadSpeedLimitResponse>;
    /// Go `BatchDownloadLatestMVCC`.
    ///
    /// # Errors
    /// The connection or RPC error.
    fn batch_download_latest_mvcc(
        &self,
        ctx: &Context,
        store_id: u64,
        req: import_sstpb::DownloadRequest,
    ) -> Result<import_sstpb::DownloadResponse>;
    /// Go `AddForcePartitionRange`.
    ///
    /// # Errors
    /// The connection or RPC error.
    fn add_force_partition_range(
        &self,
        ctx: &Context,
        store_id: u64,
        req: import_sstpb::AddPartitionRangeRequest,
    ) -> Result<()>;
    /// Go `RemoveForcePartitionRange`.
    ///
    /// # Errors
    /// The connection or RPC error.
    fn remove_force_partition_range(
        &self,
        ctx: &Context,
        store_id: u64,
        req: import_sstpb::RemovePartitionRangeRequest,
    ) -> Result<()>;
    /// Go `CloseGrpcClient`.
    ///
    /// # Errors
    /// Never: tonic channels close when dropped.
    fn close_grpc_client(&self) -> Result<()>;

    /// Go `IsBatchDownloadLatestMVCCSupported`.
    ///
    /// # Errors
    /// A store's error other than `Unimplemented`.
    fn is_batch_download_latest_mvcc_supported(
        &self,
        ctx: &Context,
        stores: &[u64],
    ) -> Result<bool> {
        for &store_id in stores {
            match self.batch_download_latest_mvcc(
                ctx,
                store_id,
                import_sstpb::DownloadRequest::default(),
            ) {
                Ok(_) => {}
                Err(err) if errors::grpc_code(err.as_ref()) == tonic::Code::Unimplemented => {
                    return Ok(false)
                }
                Err(err) => return Err(errors::annotate(
                    err,
                    format!(
                        "failed to check BatchDownloadLatestMVCC support. (store id {store_id})"
                    ),
                )),
            }
        }
        Ok(true)
    }

    /// Go `CheckMultiIngestSupport`.
    ///
    /// # Errors
    /// A store without multi ingest, or a store's error.
    fn check_multi_ingest_support(&self, ctx: &Context, stores: &[u64]) -> Result<()> {
        for &store_id in stores {
            if let Err(err) =
                self.multi_ingest(ctx, store_id, import_sstpb::MultiIngestRequest::default())
            {
                if errors::grpc_code(err.as_ref()) == tonic::Code::Unimplemented {
                    return Err(errors::errorf(format!(
                        "tikv node doesn't support multi ingest. (store id {store_id})"
                    )));
                }
                return Err(errors::annotate(
                    err,
                    format!("failed to check multi ingest support. (store id {store_id})"),
                ));
            }
        }
        Ok(())
    }
}

/// Go `importClient`.
pub struct ImportClient {
    meta_client: Arc<dyn SplitClient>,
    conns: Mutex<HashMap<u64, Channel>>,
    ingest_conns: Mutex<HashMap<u64, Channel>>,
    security: tidb_pd_client::ClusterSecurity,
    keepalive: crate::utils::store_manager::Keepalive,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl ImportClient {
    /// Go `NewImportClient`.
    #[must_use]
    pub fn new(
        meta_client: Arc<dyn SplitClient>,
        security: tidb_pd_client::ClusterSecurity,
        keepalive: crate::utils::store_manager::Keepalive,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self {
            meta_client,
            conns: Mutex::default(),
            ingest_conns: Mutex::default(),
            security,
            keepalive,
            runtime,
        }
    }

    /// Go `createGrpcConn`: the store's peer address, else its address.
    fn create_grpc_conn(&self, ctx: &Context, store_id: u64) -> Result<Channel> {
        let store = self.meta_client.get_store(ctx, store_id)?;
        let addr = if store.peer_address.is_empty() {
            store.address
        } else {
            store.peer_address
        };
        let endpoint = tidb_pd_client::secure_endpoint(&addr, &self.security)
            .map_err(|e| errors::errorf(e.to_string()))?
            .http2_keep_alive_interval(self.keepalive.time)
            .keep_alive_timeout(self.keepalive.timeout)
            .connect_timeout(GRPC_BACKOFF_MAX_DELAY * 10);
        self.runtime
            .block_on(endpoint.connect())
            .map_err(|e| errors::errorf(e.to_string()))
    }

    /// Go `cachedConnectionFrom`.
    fn cached_connection_from(
        &self,
        ctx: &Context,
        store_id: u64,
        ingest: bool,
    ) -> Result<ImportSstClient<Channel>> {
        let caches = if ingest {
            &self.ingest_conns
        } else {
            &self.conns
        };
        let mut caches = caches.lock().unwrap();
        if let Some(conn) = caches.get(&store_id) {
            return Ok(ImportSstClient::new(conn.clone()));
        }
        let conn = self.create_grpc_conn(ctx, store_id)?;
        caches.insert(store_id, conn.clone());
        Ok(ImportSstClient::new(conn))
    }

    fn call<T, F>(
        &self,
        ctx: &Context,
        store_id: u64,
        ingest: bool,
        f: impl FnOnce(ImportSstClient<Channel>) -> F,
    ) -> Result<T>
    where
        F: std::future::Future<Output = std::result::Result<tonic::Response<T>, tonic::Status>>,
    {
        let cli = self.cached_connection_from(ctx, store_id, ingest)?;
        let done = ctx.done();
        let res = self.runtime.block_on(async move {
            let rpc = f(cli);
            tokio::select! {
                r = rpc => Some(r.map(tonic::Response::into_inner).map_err(errors::grpc)),
                () = wait_done(done) => None,
            }
        });
        res.unwrap_or_else(|| {
            Err(Arc::new(
                ctx.err().unwrap_or(tidb_gocontext::CtxError::Canceled),
            ))
        })
    }
}

/// Waits until a context's done channel is closed.
async fn wait_done(done: crossbeam_channel::Receiver<()>) {
    loop {
        if matches!(
            done.try_recv(),
            Err(crossbeam_channel::TryRecvError::Disconnected)
        ) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

impl ImporterClient for ImportClient {
    fn download_sst(
        &self,
        ctx: &Context,
        store_id: u64,
        req: import_sstpb::DownloadRequest,
    ) -> Result<import_sstpb::DownloadResponse> {
        self.call(ctx, store_id, false, |mut c| async move {
            c.download(req).await
        })
    }

    fn multi_ingest(
        &self,
        ctx: &Context,
        store_id: u64,
        req: import_sstpb::MultiIngestRequest,
    ) -> Result<import_sstpb::IngestResponse> {
        self.call(ctx, store_id, true, |mut c| async move {
            c.multi_ingest(req).await
        })
    }

    fn set_download_speed_limit(
        &self,
        ctx: &Context,
        store_id: u64,
        req: import_sstpb::SetDownloadSpeedLimitRequest,
    ) -> Result<import_sstpb::SetDownloadSpeedLimitResponse> {
        self.call(ctx, store_id, false, |mut c| async move {
            c.set_download_speed_limit(req).await
        })
    }

    fn batch_download_latest_mvcc(
        &self,
        ctx: &Context,
        store_id: u64,
        req: import_sstpb::DownloadRequest,
    ) -> Result<import_sstpb::DownloadResponse> {
        self.call(ctx, store_id, false, |mut c| async move {
            c.batch_download_latest_mvcc(req).await
        })
    }

    fn add_force_partition_range(
        &self,
        ctx: &Context,
        store_id: u64,
        req: import_sstpb::AddPartitionRangeRequest,
    ) -> Result<()> {
        self.call(ctx, store_id, false, |mut c| async move {
            c.add_force_partition_range(req).await
        })
        .map(|_| ())
    }

    fn remove_force_partition_range(
        &self,
        ctx: &Context,
        store_id: u64,
        req: import_sstpb::RemovePartitionRangeRequest,
    ) -> Result<()> {
        self.call(ctx, store_id, false, |mut c| async move {
            c.remove_force_partition_range(req).await
        })
        .map(|_| ())
    }

    fn close_grpc_client(&self) -> Result<()> {
        self.conns.lock().unwrap().clear();
        self.ingest_conns.lock().unwrap().clear();
        Ok(())
    }
}
