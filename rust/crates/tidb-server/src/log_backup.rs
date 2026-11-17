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


//! Go `Domain.initLogBackup`: the log backup checkpoint advancer every TiDB
//! node runs under the `/tidb/br-stream/owner` election. Dropping the
//! handle stops the daemon loop and closes the owner manager (Go
//! `brOwnerMgr.Close` in `Domain.Close`).

use std::sync::Arc;

use tidb_br::streamhelper::advancer::{owner_id_for_log_backup, CheckpointAdvancer, OWNER_PATH, OWNER_PROMPT};
use tidb_br::streamhelper::advancer_env::{ClusterEnv, TikvLockResolver};
use tidb_br::streamhelper::daemon::OwnerDaemon;
use tidb_br::streamhelper::MetaDataClient;
use tidb_txnkv::driver::tikv_store::TikvStore;

/// The running advancer daemon.
pub(crate) struct LogBackupAdvancer {
    cancel: tidb_gocontext::CancelFunc,
    owner: Arc<dyn tidb_owner::Manager>,
    loop_thread: Option<std::thread::JoinHandle<()>>,
}

impl Drop for LogBackupAdvancer {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(t) = self.loop_thread.take() {
            let _ = t.join();
        }
        self.owner.close();
    }
}

/// Go `initLogBackup`: `None` without etcd (Go: no pd or etcd client).
///
/// # Errors
/// The owner campaign failed.
pub(crate) fn init_log_backup(
    store: &TikvStore,
    etcd: Option<Arc<tidb_pd_client::EtcdClient>>,
    security: &tidb_pd_client::ClusterSecurity,
) -> Result<Option<LogBackupAdvancer>, String> {
    let Some(etcd) = etcd else {
        tidb_log::warn("pd / etcd client not provided, won't begin Advancer.", &[]);
        return Ok(None);
    };
    let cfg = tidb_config::config_tree::config::get_global_config();
    let keepalive = tidb_br::utils::store_manager::Keepalive {
        time: std::time::Duration::from_secs(u64::from(cfg.tikv_client.grpc_keep_alive_time)),
        // Go `time.Duration(GrpcKeepAliveTimeout) * time.Second` truncates.
        timeout: std::time::Duration::from_secs(cfg.tikv_client.grpc_keep_alive_timeout as u64),
    };
    let env = ClusterEnv::tidb_env(
        Arc::new(tidb_br::pdclient::TikvPdClient::new(store.clone())),
        MetaDataClient::new(Arc::clone(&etcd) as Arc<dyn tidb_br::streamhelper::MetaStore>),
        Arc::new(TikvLockResolver::new(store)),
        keepalive,
        security.clone(),
        Arc::clone(store.runtime()),
    );
    let adv = CheckpointAdvancer::new_tidb(Arc::new(env));
    // Go `OwnerManagerForLogBackup`.
    let (ctx, cancel) = tidb_gocontext::Context::with_cancel(&tidb_gocontext::Context::background());
    let owner: Arc<dyn tidb_owner::Manager> = Arc::new(tidb_owner::OwnerManager::new(
        tidb_owner::Context::background(),
        etcd as Arc<dyn tidb_owner::OwnerStore>,
        OWNER_PROMPT,
        owner_id_for_log_backup(),
        OWNER_PATH,
    ));
    let tick = adv.config().tick_timeout();
    let daemon = OwnerDaemon::new(Arc::new(adv), Arc::clone(&owner), tick);
    let run = match daemon.begin(&ctx) {
        Ok(run) => run,
        Err(err) => {
            cancel.cancel();
            owner.close();
            return Err(err.to_string());
        }
    };
    let loop_thread = std::thread::Builder::new()
        .name("logBackupAdvancer".into())
        .spawn(run)
        .map_err(|e| e.to_string())?;
    Ok(Some(LogBackupAdvancer { cancel, owner, loop_thread: Some(loop_thread) }))
}
