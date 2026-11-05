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

//! Go `Domain.initResourceGroupsController`'s `runaway.NewRunawayManager`
//! and `Domain.Start`'s `runawayRecordFlushLoop` / `runawayWatchSyncLoop`,
//! over the domain's internal sessions, its info cache and its DDL.

use std::sync::Arc;

use tidb_resourcegroup_runaway as runaway;

use super::Domain;
use crate::sys_session::{DomainSysSession, DomainSysSessionPool};

/// Go `do.sysSessionPool` as the runaway package runs SQL on it.
struct RunawaySessions(Arc<DomainSysSessionPool>);

/// One pooled internal session: Go's `ExecuteInternal` and
/// `ExecRestrictedSQL(ExecOptionUseCurSession)` both run on it.
struct RunawaySession<'a>(&'a dyn DomainSysSession);

impl runaway::SqlSession for RunawaySession<'_> {
    fn execute_internal(
        &self,
        sql: &str,
        args: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        let sql = tidb_sqlescape::escape_sql_text(sql, args).map_err(|error| format!("{error:?}"))?;
        // Go runaway's `kv.WithInternalSourceType(ctx, kv.InternalTxnOthers)`.
        self.0.query_internal(tidb_txnkv::INTERNAL_TXN_OTHERS, &sql)
    }

    fn exec_restricted_sql(
        &self,
        sql: &str,
        args: &[tidb_sqlescape::SqlArg<'_>],
    ) -> Result<Vec<Vec<tidb_datatype::Datum>>, String> {
        self.execute_internal(sql, args)
    }
}

impl runaway::SessionPool for RunawaySessions {
    fn with_session(&self, body: &mut dyn FnMut(&dyn runaway::SqlSession)) -> Result<(), String> {
        self.0
            .with_session(|session| {
                session.with_session_context(|context| {
                    body(&RunawaySession(context));
                    Ok::<(), tidb_syssession::SysSessionError>(())
                })
            })
            .map_err(|error| error.to_string())
    }
}

/// Go `do.infoCache`.
struct RunawayInfoCache(Arc<tidb_infoschema_issyncer::catalog_watch::InfoCache>);

impl runaway::InfoCache for RunawayInfoCache {
    fn table_exists(&self, schema: &str, table: &str) -> bool {
        self.table_by_name(schema, table).is_some()
    }

    fn table_by_name(&self, schema: &str, table: &str) -> Option<tidb_model::GoShared<tidb_model::TableInfo>> {
        self.0.get_latest().table_info_by_name(schema, table).map(|(_, table)| table)
    }
}

/// Go `do.ddl`'s `OwnerManager().IsOwner()`.
struct RunawayDdl(Arc<dyn tidb_ddl::worker::ClusterDdl>);

impl runaway::DdlOwner for RunawayDdl {
    fn is_owner(&self) -> bool {
        self.0.is_owner()
    }
}

/// Go `do.runawayManager` and its two loops (plus the watch list's cleaner,
/// Go's `go watchList.Start()`).
pub struct RunawayRuntime {
    /// Go `RunawayManager()`.
    pub manager: Arc<runaway::Manager>,
    exit: Arc<runaway::ExitSignal>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

impl RunawayRuntime {
    fn start(domain: &Arc<Domain>, controller: Arc<dyn runaway::ResourceGroupsController>) -> Self {
        // Go `net.JoinHostPort(serverInfo.IP, strconv.Itoa(int(serverInfo.Port)))`.
        let server_addr = domain
            .server_info()
            .map(|syncer| {
                let info = syncer.get_local_server_info();
                tidb_domain::plan_replayer::join_host_port(&info.static_info.ip, info.static_info.port as u64)
            })
            .unwrap_or_default();
        let exit = Arc::new(runaway::ExitSignal::default());
        let manager = runaway::Manager::new(
            controller,
            &server_addr,
            Arc::new(RunawaySessions(domain.advanced_sys_session_pool())),
            Arc::clone(&exit),
            Arc::new(RunawayInfoCache(Arc::clone(&domain.catalog))),
            Arc::new(RunawayDdl(Arc::clone(&domain.ddl))),
        );
        let spawn = |name: &str, run: Box<dyn FnOnce() + Send>| {
            tidb_worker_stack::sql_worker_thread(name.to_owned()).spawn(run).expect("runaway thread spawns")
        };
        let threads = vec![
            spawn("runaway-watch-list", {
                let manager = Arc::clone(&manager);
                Box::new(move || manager.watch_list_start())
            }),
            spawn("runawayRecordFlushLoop", {
                let manager = Arc::clone(&manager);
                Box::new(move || manager.runaway_record_flush_loop())
            }),
            spawn("runawayWatchSyncLoop", {
                let manager = Arc::clone(&manager);
                Box::new(move || manager.runaway_watch_sync_loop())
            }),
        ];
        Self { manager, exit, threads }
    }
}

impl Drop for RunawayRuntime {
    fn drop(&mut self) {
        // Go `close(do.exit)`, then `do.runawayManager.Stop()`.
        self.exit.close();
        self.manager.stop();
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

impl Domain {
    /// Go `initResourceGroupsController`'s runaway manager over the started
    /// resource-groups controller, with `Domain.Start`'s loops; a second call
    /// keeps the first manager.
    pub fn start_runaway_manager(self: &Arc<Self>, controller: Arc<dyn runaway::ResourceGroupsController>) {
        let _ = self.runaway.get_or_init(|| RunawayRuntime::start(self, controller));
    }

    /// Go `RunawayManager()`, absent until started.
    pub fn runaway_manager(&self) -> Option<Arc<runaway::Manager>> {
        self.runaway.get().map(|runtime| Arc::clone(&runtime.manager))
    }
}
