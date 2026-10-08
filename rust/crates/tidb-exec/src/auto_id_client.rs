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

//! Process-owned AutoID discovery and gRPC connections (Go ClientDiscover).

use crate::cluster_auto_id::{
    AutoIdServiceAllocRequest, AutoIdServiceRebaseRequest, AutoIdServiceRpc, AutoIdServiceRpcError,
};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;
use tidb_executor::kv_table::AutoIdCall as UnaryCallContext;
use tidb_pd_client::{secure_endpoint, ClusterSecurity, EtcdClient};
use tikv_client::proto::autoid::{self, auto_id_alloc_client::AutoIdAllocClient};
use tonic::transport::Channel;

type Error = AutoIdServiceRpcError;

/// Election lookup shared by every table's allocator.
pub trait AutoIdLeader: Send + Sync + 'static {
    /// Earliest-created election candidate, or None before election completes.
    fn leader(&self) -> Result<Option<String>, Error>;
}

/// Go WithFirstCreate over the AutoID leader election prefix.
pub struct EtcdAutoIdLeader(pub Arc<EtcdClient>);
impl AutoIdLeader for EtcdAutoIdLeader {
    fn leader(&self) -> Result<Option<String>, Error> {
        let entries = self
            .0
            .get_prefix_metadata(b"tidb/autoid/leader")
            .map_err(|e| Error::Other(e.to_string()))?;
        entries
            .into_iter()
            .min_by_key(|entry| entry.create_revision)
            .map(|entry| String::from_utf8(entry.value).map_err(|e| Error::Other(e.to_string())))
            .transpose()
    }
}

enum Command {
    Alloc(
        UnaryCallContext,
        AutoIdServiceAllocRequest,
        mpsc::Sender<Result<(i64, i64), Error>>,
    ),
    Rebase(
        UnaryCallContext,
        AutoIdServiceRebaseRequest,
        mpsc::Sender<Result<(), Error>>,
    ),
}

/// One joined runtime and generation-bound channel cache per process.
pub struct AutoIdClient {
    sender: Option<tokio::sync::mpsc::UnboundedSender<Command>>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}
impl std::fmt::Debug for AutoIdClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AutoIdClient").finish_non_exhaustive()
    }
}

struct Discovery {
    leader: Arc<dyn AutoIdLeader>,
    security: ClusterSecurity,
    cached: tokio::sync::Mutex<DiscoveryState>,
}
#[derive(Default)]
struct DiscoveryState {
    generation: u64,
    channel: Option<Channel>,
    // Keep one bounded etcd lookup alive across callers that abandon their wait.
    // Cancellation must not enqueue a fresh blocking lookup per SQL statement.
    lookup: Option<tokio::task::JoinHandle<Result<Option<String>, Error>>>,
}
impl Discovery {
    async fn client(&self) -> Result<(u64, AutoIdAllocClient<Channel>), Error> {
        let mut held = self.cached.lock().await;
        if let Some(channel) = &held.channel {
            return Ok((held.generation, AutoIdAllocClient::new(channel.clone())));
        }
        let mut delay = Duration::from_millis(5);
        let address = loop {
            if held.lookup.is_none() {
                let leader = self.leader.clone();
                held.lookup = Some(tokio::task::spawn_blocking(move || leader.leader()));
            }
            let result = held.lookup.as_mut().expect("lookup installed").await;
            held.lookup = None;
            let result = result.map_err(|e| Error::Other(e.to_string()))??;
            if let Some(address) = result {
                break address;
            }
            delay = (delay * 2).min(Duration::from_millis(100));
            tokio::time::sleep(delay).await;
        };
        let channel = secure_endpoint(&address, &self.security)
            .map_err(|e| Error::Other(e.to_string()))?
            .connect_lazy();
        held.channel = Some(channel.clone());
        Ok((held.generation, AutoIdAllocClient::new(channel)))
    }

    async fn failed(&self, generation: u64) {
        let mut held = self.cached.lock().await;
        if held.generation == generation {
            held.generation = held.generation.wrapping_add(1);
            held.channel = None;
        }
    }

    async fn alloc(
        &self,
        call: &UnaryCallContext,
        request: AutoIdServiceAllocRequest,
    ) -> Result<(i64, i64), Error> {
        let (generation, mut client) = self.client().await?;
        let response = client
            .alloc_auto_id(autoid::AutoIdRequest {
                db_id: request.db_id,
                tbl_id: request.table_id,
                is_unsigned: request.unsigned,
                n: request.n,
                increment: request.increment,
                offset: request.offset,
                keyspace: Some(autoid::auto_id_request::Keyspace::KeyspaceId(
                    request.keyspace_id,
                )),
            })
            .await;
        match response {
            Ok(response) => {
                let response = response.into_inner();
                if !response.errmsg.is_empty() {
                    return Err(Error::Other(
                        String::from_utf8_lossy(&response.errmsg).into_owned(),
                    ));
                }
                Ok((response.min, response.max))
            }
            Err(error) => {
                if !call.cancellation().is_cancelled() && !call.timeout().is_zero() {
                    self.failed(generation).await;
                }
                Err(Error::Rpc(error.to_string()))
            }
        }
    }

    async fn rebase(
        &self,
        call: &UnaryCallContext,
        request: AutoIdServiceRebaseRequest,
    ) -> Result<(), Error> {
        let (generation, mut client) = self.client().await?;
        let response = client
            .rebase(autoid::RebaseRequest {
                db_id: request.db_id,
                tbl_id: request.table_id,
                is_unsigned: request.unsigned,
                base: request.new_base,
                force: request.force,
                // Go singlePointAlloc.rebaseRPC omits keyspace on this request.
                keyspace: None,
            })
            .await;
        match response {
            Ok(response) => {
                let response = response.into_inner();
                if !response.errmsg.is_empty() {
                    return Err(Error::Other(
                        String::from_utf8_lossy(&response.errmsg).into_owned(),
                    ));
                }
                Ok(())
            }
            Err(error) => {
                if !call.cancellation().is_cancelled() && !call.timeout().is_zero() {
                    self.failed(generation).await;
                }
                Err(Error::Rpc(error.to_string()))
            }
        }
    }
}

async fn bounded<T>(
    call: &UnaryCallContext,
    future: impl std::future::Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    tokio::pin!(future);
    loop {
        if call.cancellation().is_cancelled() {
            return Err(Error::Other("AutoID request canceled".into()));
        }
        let remaining = call.timeout();
        if remaining.is_zero() {
            return Err(Error::Other("AutoID request deadline exceeded".into()));
        }
        tokio::select! {
            result = &mut future => return result,
            _ = tokio::time::sleep(remaining.min(Duration::from_millis(5))) => {},
        }
    }
}

impl AutoIdClient {
    /// Starts the process owner without dialing until the first allocation.
    pub fn new(leader: Arc<dyn AutoIdLeader>, security: ClusterSecurity) -> Result<Self, Error> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| Error::Other(e.to_string()))?;
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let worker = std::thread::Builder::new().name("tidb-autoid".into()).spawn(move || {
            runtime.block_on(async move {
                let discovery = Arc::new(Discovery { leader, security, cached: tokio::sync::Mutex::new(DiscoveryState::default()) });
                let mut requests = tokio::task::JoinSet::new();
                loop {
                    tokio::select! {
                        command = receiver.recv() => {
                            let Some(command) = command else { break; };
                            let discovery = discovery.clone();
                            requests.spawn(async move {
                                match command {
                                    Command::Alloc(call, request, reply) => { let _ = reply.send(bounded(&call, discovery.alloc(&call,request)).await); }
                                    Command::Rebase(call, request, reply) => { let _ = reply.send(bounded(&call, discovery.rebase(&call,request)).await); }
                                }
                            });
                        }
                        _ = requests.join_next(), if !requests.is_empty() => {},
                    }
                }
                requests.abort_all();
                while requests.join_next().await.is_some() {}
            });
        }).map_err(|e| Error::Other(e.to_string()))?;
        Ok(Self {
            sender: Some(sender),
            worker: Mutex::new(Some(worker)),
        })
    }
}
impl Drop for AutoIdClient {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(worker) = self
            .worker
            .get_mut()
            .expect("AutoID worker poisoned")
            .take()
        {
            let _ = worker.join();
        }
    }
}
impl AutoIdServiceRpc for AutoIdClient {
    fn alloc_auto_id(
        &self,
        call: &UnaryCallContext,
        request: AutoIdServiceAllocRequest,
    ) -> Result<(i64, i64), Error> {
        let (reply, receive) = mpsc::channel();
        self.sender
            .as_ref()
            .ok_or_else(|| Error::Other("AutoID client closed".into()))?
            .send(Command::Alloc(call.clone(), request, reply))
            .map_err(|_| Error::Other("AutoID client closed".into()))?;
        receive
            .recv()
            .map_err(|_| Error::Other("AutoID client closed".into()))?
    }
    fn rebase(
        &self,
        call: &UnaryCallContext,
        request: AutoIdServiceRebaseRequest,
    ) -> Result<(), Error> {
        let (reply, receive) = mpsc::channel();
        self.sender
            .as_ref()
            .ok_or_else(|| Error::Other("AutoID client closed".into()))?
            .send(Command::Rebase(call.clone(), request, reply))
            .map_err(|_| Error::Other("AutoID client closed".into()))?;
        receive
            .recv()
            .map_err(|_| Error::Other("AutoID client closed".into()))?
    }
}
