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

//! Go `br/pkg/utils/store_manager.go`: one gRPC connection per TiKV store,
//! dialed through PD's store address. Go's `Pool` (a round-robin pool used
//! by the log backup and restore importers) comes with the code that uses
//! it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tidb_gocontext::Context;
use tidb_log::{Field, Value};
use tonic::transport::Channel;

use crate::errors::{self, Result, ERR_FAILED_TO_CONNECT};
use crate::pdclient::PdClient;

/// Go `defaultDialTimeout`.
const DEFAULT_DIAL_TIMEOUT: Duration = Duration::from_secs(30);
/// Go `resetRetryTimes`.
const RESET_RETRY_TIMES: u64 = 3;

/// Go `keepalive.ClientParameters` (`Time`, `Timeout`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Keepalive {
    /// Go `Time`.
    pub time: Duration,
    /// Go `Timeout`.
    pub timeout: Duration,
}

/// Go `StoreManager`.
pub struct StoreManager {
    pd_client: Arc<dyn PdClient>,
    clis: Mutex<HashMap<u64, Channel>>,
    keepalive: Keepalive,
    security: tidb_pd_client::ClusterSecurity,
    runtime: Arc<tokio::runtime::Runtime>,
    /// Go `DialTimeout`; zero is the default.
    pub dial_timeout: Duration,
}

impl StoreManager {
    /// Go `NewStoreManager`. Go passes a `*tls.Config`; this passes the
    /// cluster TLS files it is built from.
    #[must_use]
    pub fn new(
        pd_client: Arc<dyn PdClient>,
        keepalive: Keepalive,
        security: tidb_pd_client::ClusterSecurity,
        runtime: Arc<tokio::runtime::Runtime>,
    ) -> Self {
        Self { pd_client, clis: Mutex::new(HashMap::new()), keepalive, security, runtime, dial_timeout: Duration::ZERO }
    }

    /// Go `GetKeepalive`.
    #[must_use]
    pub fn get_keepalive(&self) -> Keepalive {
        self.keepalive
    }

    fn get_dial_timeout(&self) -> Duration {
        if self.dial_timeout > Duration::ZERO {
            self.dial_timeout
        } else {
            DEFAULT_DIAL_TIMEOUT
        }
    }

    /// Go `PDClient`.
    #[must_use]
    pub fn pd_client(&self) -> &Arc<dyn PdClient> {
        &self.pd_client
    }

    /// Go `TLSConfig() != nil`.
    #[must_use]
    pub fn tls_enabled(&self) -> bool {
        self.security.is_tls_enabled()
    }

    /// Go `TLSConfig()`: the cluster TLS files the config is built from.
    #[must_use]
    pub fn security(&self) -> &tidb_pd_client::ClusterSecurity {
        &self.security
    }

    /// The runtime connections run on.
    #[must_use]
    pub fn runtime(&self) -> &Arc<tokio::runtime::Runtime> {
        &self.runtime
    }

    /// Go `getGrpcConnLocked`: dials the store's peer address (its address
    /// without one).
    fn get_grpc_conn(&self, ctx: &Context, store_id: u64) -> Result<Channel> {
        tidb_failpoint::inject("github.com/pingcap/tidb/br/pkg/utils/hint-get-backup-client", |v| {
            tidb_log::info(
                "failpoint hint-get-backup-client injected, process will notify the shell.",
                &[Field::new("store", Value::U64(store_id))],
            );
            let file = v.string();
            if !file.is_empty() {
                let _ = std::fs::File::create(file);
            }
            std::thread::sleep(Duration::from_secs(3));
        });
        let store = self.pd_client.get_store(ctx, store_id)?;
        let addr = if store.peer_address.is_empty() { store.address.clone() } else { store.peer_address.clone() };
        tidb_log::info(
            "StoreManager: dialing to store.",
            &[Field::new("address", Value::Str(addr.clone())), Field::new("store-id", Value::U64(store_id))],
        );
        let wrap = |err: String| -> errors::Error {
            Arc::new(
                ERR_FAILED_TO_CONNECT
                    .wrap(errors::errorf(err))
                    .gen_with_stack(format!("failed to make connection to store {store_id}")),
            )
        };
        let endpoint = tidb_pd_client::secure_endpoint(&addr, &self.security)
            .map_err(|e| wrap(e.to_string()))?
            .connect_timeout(self.get_dial_timeout())
            .http2_keep_alive_interval(self.keepalive.time)
            .keep_alive_timeout(self.keepalive.timeout);
        let timeout = self.get_dial_timeout();
        self.runtime
            .block_on(async move { tokio::time::timeout(timeout, endpoint.connect()).await })
            .map_err(|_| wrap("context deadline exceeded".into()))?
            .map_err(|e| wrap(e.to_string()))
    }

    /// Go `RemoveConn`.
    ///
    /// # Errors
    /// `ctx`'s error.
    pub fn remove_conn(&self, ctx: &Context, store_id: u64) -> Result<()> {
        if let Some(err) = ctx.err() {
            return Err(Arc::new(err));
        }
        self.clis.lock().unwrap().remove(&store_id);
        Ok(())
    }

    /// Go `TryWithConn`: `f` over the store's cached connection, dialing
    /// one first if there is none.
    ///
    /// # Errors
    /// `ctx`'s error, the dial error, or `f`'s.
    pub fn try_with_conn<T>(&self, ctx: &Context, store_id: u64, f: impl FnOnce(Channel) -> Result<T>) -> Result<T> {
        if let Some(err) = ctx.err() {
            return Err(Arc::new(err));
        }
        let mut clis = self.clis.lock().unwrap();
        if let Some(conn) = clis.get(&store_id) {
            return f(conn.clone());
        }
        let conn = self.get_grpc_conn(ctx, store_id)?;
        clis.insert(store_id, conn.clone());
        drop(clis);
        f(conn)
    }

    /// Go `WithConn`.
    ///
    /// # Errors
    /// `ctx`'s error or the dial error.
    pub fn with_conn<T>(&self, ctx: &Context, store_id: u64, f: impl FnOnce(Channel) -> T) -> Result<T> {
        self.try_with_conn(ctx, store_id, |c| Ok(f(c)))
    }

    /// Go `ResetBackupClient`: a fresh connection to the store, retried
    /// three times.
    ///
    /// # Errors
    /// `ctx`'s error or the last dial error.
    pub fn reset_backup_client(
        &self,
        ctx: &Context,
        store_id: u64,
    ) -> Result<tidb_proto::backup::backup_client::BackupClient<Channel>> {
        self.remove_conn(ctx, store_id)?;
        let mut clis = self.clis.lock().unwrap();
        let mut last = None;
        for retry in 0..RESET_RETRY_TIMES {
            match self.get_grpc_conn(ctx, store_id) {
                Ok(conn) => {
                    clis.insert(store_id, conn.clone());
                    return Ok(new_backup_client(conn));
                }
                Err(err) => {
                    tidb_log::warn(
                        "failed to reset grpc connection, retry it",
                        &[Field::new("retry time", Value::U64(retry)), Field::new("error", Value::Str(err.to_string()))],
                    );
                    std::thread::sleep(Duration::from_secs(retry + 3));
                    last = Some(err);
                }
            }
        }
        Err(last.unwrap())
    }

    /// Go `Close`.
    pub fn close(&self) {
        self.clis.lock().unwrap().clear();
    }
}

/// Go `backuppb.NewBackupClient`, without a response size limit.
#[must_use]
pub fn new_backup_client(conn: Channel) -> tidb_proto::backup::backup_client::BackupClient<Channel> {
    tidb_proto::backup::backup_client::BackupClient::new(conn).max_decoding_message_size(usize::MAX)
}
