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

//! Go `mpp.go`'s task side (`MPPTaskHandler`, `ExchangerTunnel`) and
//! `cop_handler.go`'s `MPPCtx`: an MPP task's tunnels to the tasks that
//! receive its rows, and the client it reaches other stores' tasks with.
//! Go's channels are crossbeam channels; closing one is dropping its sender.

use std::collections::HashMap;
use std::sync::atomic::AtomicI32;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender};
use tidb_proto::mpp;
use tidb_proto::tipb;

/// Go `MPPErrTunnelNotFound`: the task is not ready, or has been deleted.
pub const MPP_ERR_TUNNEL_NOT_FOUND: i32 = 0;
/// Go `MPPErrEstablishConnMultiTimes`: the Establish requests came at least
/// twice.
pub const MPP_ERR_ESTABLISH_CONN_MULTI_TIMES: i32 = 1;
/// Go `MPPErrMPPGatherIDMismatch`: mismatched gather id, usually a bug in the
/// MPP coordinator.
pub const MPP_ERR_MPP_GATHER_ID_MISMATCH: i32 = 2;

/// Go `client.Client` as an MPP task reaches another store's task: an
/// `EstablishMPPConnection` to `addr`, answered with every packet the sender
/// task sends (Go's mock stream collects them).
pub trait MppRpcClient: Send + Sync {
    /// Go `SendRequest(ctx, addr, CmdMPPConn, ...)` and the stream it opens.
    ///
    /// # Errors
    /// The connection failed.
    fn establish_mpp_conn(
        &self,
        addr: &str,
        req: mpp::EstablishMppConnectionRequest,
    ) -> Result<Vec<mpp::MppDataPacket>, String>;
}

/// Go `MPPCtx`.
pub struct MppCtx {
    /// Go `RPCClient`.
    pub rpc_client: Arc<dyn MppRpcClient>,
    /// Go `StoreAddr`.
    pub store_addr: String,
    /// Go `TaskHandler`.
    pub task_handler: Arc<MppTaskHandler>,
}

/// Go `ExchangerTunnel`: carries data from one task to another. Only one
/// sender and one receiver use it, so the sender closes it.
pub struct ExchangerTunnel {
    data_tx: Mutex<Option<Sender<tipb::Chunk>>>,
    data_rx: Receiver<tipb::Chunk>,
    /// Go `sourceTask`: nearer to the data source.
    pub(crate) source_task: mpp::TaskMeta,
    /// Go `targetTask`: nearer to the client end, as TiDB.
    pub(crate) target_task: mpp::TaskMeta,
    connected_tx: Mutex<Option<Sender<()>>>,
    connected_rx: Receiver<()>,
    err_tx: Mutex<Option<Sender<String>>>,
    err_rx: Receiver<String>,
}

impl ExchangerTunnel {
    /// A tunnel from `source_task` to `target_task` (Go's
    /// `buildMPPExchangeSender`: a ten-chunk data channel, a one-error
    /// channel).
    pub(crate) fn new(source_task: mpp::TaskMeta, target_task: mpp::TaskMeta) -> Self {
        let (data_tx, data_rx) = crossbeam_channel::bounded(10);
        let (connected_tx, connected_rx) = crossbeam_channel::bounded(0);
        let (err_tx, err_rx) = crossbeam_channel::bounded(1);
        Self {
            data_tx: Mutex::new(Some(data_tx)),
            data_rx,
            source_task,
            target_task,
            connected_tx: Mutex::new(Some(connected_tx)),
            connected_rx,
            err_tx: Mutex::new(Some(err_tx)),
            err_rx,
        }
    }

    /// Go `tunnel.DataCh <- chunk`.
    pub(crate) fn send_chunk(&self, chunk: tipb::Chunk) {
        let tx = self.data_tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        if let Some(tx) = tx {
            let _ = tx.send(chunk);
        }
    }

    /// Go `tunnel.ErrCh <- err`.
    pub(crate) fn send_err(&self, err: String) {
        let tx = self.err_tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
        if let Some(tx) = tx {
            let _ = tx.try_send(err);
        }
    }

    /// Go `<-tunnel.connectedCh`: blocks until a receiver connected.
    pub(crate) fn wait_connected(&self) {
        let _ = self.connected_rx.recv();
    }

    /// Go `close(tunnel.ErrCh); close(tunnel.DataCh)`.
    pub(crate) fn close(&self) {
        self.err_tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        self.data_tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
    }

    /// Go `RecvChunk`: the next chunk (`None` once the sender closed the
    /// tunnel), and the error the sender left.
    pub fn recv_chunk(&self) -> (Option<tipb::Chunk>, Option<String>) {
        let chunk = self.data_rx.recv().ok();
        let err = self.err_rx.try_recv().ok();
        (chunk, err)
    }
}

/// Go `MPPTaskHandler`: one MPP task in a store.
pub struct MppTaskHandler {
    /// When a connect request comes, it names the server task (source) and
    /// the client task (target); the tunnel is found by the client task.
    tunnel_set: Mutex<HashMap<i64, Arc<ExchangerTunnel>>>,
    /// Go `Meta`.
    pub meta: mpp::TaskMeta,
    /// Go `RPCClient`.
    pub rpc_client: Arc<dyn MppRpcClient>,
    /// Go `Status`.
    pub status: AtomicI32,
    /// Go `Err`.
    pub err: Mutex<Option<String>>,
}

impl MppTaskHandler {
    /// A handler with no tunnels yet (Go `CreateMPPTaskHandler`).
    #[must_use]
    pub fn new(meta: mpp::TaskMeta, rpc_client: Arc<dyn MppRpcClient>) -> Self {
        Self {
            tunnel_set: Mutex::new(HashMap::new()),
            meta,
            rpc_client,
            status: AtomicI32::new(0),
            err: Mutex::new(None),
        }
    }

    /// Go `HandleEstablishConn`: the tunnel to the receiver, once the task
    /// registered it (ten tries, a second apart).
    ///
    /// # Errors
    /// The gather ids mismatch, or no tunnel to the receiver appeared.
    pub fn handle_establish_conn(&self, req: &mpp::EstablishMppConnectionRequest) -> Result<Arc<ExchangerTunnel>, String> {
        let meta = req.receiver_meta.clone().unwrap_or_default();
        for _ in 0..10 {
            match self.get_and_active_tunnel(req) {
                Ok(tunnel) => return Ok(tunnel),
                Err(err) if err.code == MPP_ERR_MPP_GATHER_ID_MISMATCH => return Err(err.msg),
                Err(_) => std::thread::sleep(Duration::from_secs(1)),
            }
        }
        Err(format!(
            "cannot find client task {} registered in server task {}",
            meta.task_id,
            req.sender_meta.as_ref().map_or(0, |sender| sender.task_id)
        ))
    }

    /// Go `registerTunnel`.
    pub(crate) fn register_tunnel(&self, tunnel: Arc<ExchangerTunnel>) -> Result<(), String> {
        if self.meta.gather_id != tunnel.source_task.gather_id {
            return Err("mpp gather id mismatch, maybe a bug in MPP coordinator".to_owned());
        }
        if self.meta.gather_id != tunnel.target_task.gather_id {
            return Err("mpp gather id mismatch, maybe a bug in MPP coordinator".to_owned());
        }
        let task_id = tunnel.target_task.task_id;
        let mut tunnel_set = self.tunnel_set.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if tunnel_set.contains_key(&task_id) {
            return Err(format!("task id {task_id} has been registered"));
        }
        tunnel_set.insert(task_id, tunnel);
        Ok(())
    }

    /// Go `getAndActiveTunnel`.
    fn get_and_active_tunnel(&self, req: &mpp::EstablishMppConnectionRequest) -> Result<Arc<ExchangerTunnel>, mpp::Error> {
        let receiver = req.receiver_meta.clone().unwrap_or_default();
        if self.meta.gather_id != receiver.gather_id {
            return Err(mpp::Error {
                code: MPP_ERR_MPP_GATHER_ID_MISMATCH,
                msg: "mpp gather id mismatch, maybe a bug in MPP coordinator".to_owned(),
                ..mpp::Error::default()
            });
        }
        let target_id = receiver.task_id;
        let tunnel_set = self.tunnel_set.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(tunnel) = tunnel_set.get(&target_id) {
            tunnel.connected_tx.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
            return Ok(Arc::clone(tunnel));
        }
        // We dont find this dataCh, may be task not ready or have been deleted.
        Err(mpp::Error {
            code: MPP_ERR_TUNNEL_NOT_FOUND,
            msg: "task not found, please wait for a while".to_owned(),
            ..mpp::Error::default()
        })
    }
}
