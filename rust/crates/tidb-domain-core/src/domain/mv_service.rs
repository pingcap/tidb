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

//! Go master Domain's materialized view service: `RegisterMVService`'s
//! construction, `GetMVService`, `StartMVService` with its
//! `notifyMVSMetadataChange` / `watchMVSMetaChange` loops, and the
//! `domain_sysvars.go` setters.
//!
//! Go builds the service at domain init; here it is built on first use
//! (the server starts it at boot), when the domain has its stable `Arc`.
//! The service reaches system sessions through a weak domain reference, so
//! it never keeps the domain alive; the [`MvServiceGuard`] the caller keeps
//! stops it, as Go's `Domain.Close` cancels `do.ctx`.

use std::sync::{Arc, Weak};

use crossbeam_channel::{bounded, Receiver, Sender};
use tidb_gocontext::{CancelFunc, Context};
use tidb_log::{Field, Value};
use tidb_logutil::bg_logger;
use tidb_mvservice::{
    MVService, MVServiceSessionContext, MVServiceSessionPool, MvsError, ServerInfo, SystemSessionPool,
};

use super::Domain;

/// Go `mvsDDLNotifyKey`.
const MVS_DDL_NOTIFY_KEY: &str = "/tidb/mvservice/ddl";

/// The domain's MV service and Go's `do.mvService.ddlNotify`.
pub(crate) struct MvServiceState {
    service: Arc<MVService>,
    /// The DDL handler Go's `RegisterMVService` registers on the notifier;
    /// the notifier, also built on first use, takes it from here.
    ddl_handler: (tidb_ddl_notifier::HandlerId, tidb_ddl_notifier::Handler),
    ddl_notify_tx: Sender<()>,
    ddl_notify_rx: Receiver<()>,
    cancel: CancelFunc,
}

/// Stops the MV service loops when dropped (Go `Domain.Close` cancelling
/// `do.ctx` and waiting on `do.wg`).
pub struct MvServiceGuard {
    cancel: CancelFunc,
    loops: Vec<std::thread::JoinHandle<()>>,
}

impl Drop for MvServiceGuard {
    fn drop(&mut self) {
        self.cancel.cancel();
        for handle in self.loops.drain(..) {
            let _ = handle.join();
        }
    }
}

/// The domain's system sessions as the helper borrows them.
struct DomainMvSessionPool(Weak<Domain>);

impl MVServiceSessionPool for DomainMvSessionPool {
    fn with_session(&self, f: &mut dyn FnMut(&dyn MVServiceSessionContext) -> Result<(), MvsError>) -> Result<(), MvsError> {
        let domain = self.0.upgrade().ok_or_else(|| MvsError::Other("mv service: the domain is closed".to_owned()))?;
        let pool = domain.advanced_sys_session_pool();
        let mut outcome = Ok(());
        let pooled = pool.with_session(|session| {
            session.with_session_context(|context| {
                outcome = f(context.mv_service_session());
                Ok(())
            })
        });
        match (outcome, pooled) {
            (Err(err), _) => Err(err),
            (Ok(()), Err(err)) => Err(MvsError::Other(err.to_string())),
            (Ok(()), Ok(())) => Ok(()),
        }
    }
}

fn server_info_of(info: &tidb_domain_serverinfo::serverinfo::ServerInfo) -> ServerInfo {
    ServerInfo {
        id: info.static_info.id.clone(),
        ip: info.static_info.ip.clone(),
        port: info.static_info.port as u64,
        start_timestamp: info.static_info.start_timestamp,
    }
}

impl Domain {
    fn mv_service_state(self: &Arc<Self>) -> &MvServiceState {
        self.mv_service.get_or_init(|| {
            let (ctx, cancel) = Context::with_cancel(&Context::background());
            let local = Arc::downgrade(self);
            let all = Arc::downgrade(self);
            let (stats_handle_getter, sys_proc_tracker, is_auto_analyze_stopped) = mv_service_stats_seams(self);
            let (ddl_notify_tx, ddl_notify_rx) = bounded(1);
            let on_ddl_handled_tx = ddl_notify_tx.clone();
            let mut ddl_handler = None;
            let service = tidb_mvservice::register_mv_service(
                ctx,
                |id, handler| ddl_handler = Some((id, handler)),
                Arc::new(SystemSessionPool(Arc::new(DomainMvSessionPool(Arc::downgrade(self))))),
                tidb_mvservice::ServerViews {
                    local_server: Box::new(move || {
                        let domain = local.upgrade().ok_or_else(|| "the domain is closed".to_owned())?;
                        let syncer = domain.server_info().ok_or_else(|| "the server info is not ready".to_owned())?;
                        Ok(server_info_of(&syncer.get_local_server_info()))
                    }),
                    all_servers: Box::new(move || {
                        let domain = all.upgrade().ok_or_else(|| "the domain is closed".to_owned())?;
                        let syncer = domain.server_info().ok_or_else(|| "the server info is not ready".to_owned())?;
                        Ok(syncer
                            .get_all_server_info()?
                            .values()
                            .map(|info| (server_info_of(info), tidb_util_disttask::generate_exec_id(info)))
                            .collect())
                    }),
                },
                Some(stats_handle_getter),
                Some(sys_proc_tracker),
                is_auto_analyze_stopped,
                // Go `onDDLHandled`: wake the cross-server fan-out.
                Arc::new(move || {
                    let _ = on_ddl_handled_tx.try_send(());
                }),
            );
            register_mv_service_sysvar_hooks(&self.global_vars, &service);
            let ddl_handler = ddl_handler.expect("RegisterMVService registers its DDL handler");
            MvServiceState { service, ddl_handler, ddl_notify_tx, ddl_notify_rx, cancel }
        })
    }

    /// Go `GetMVService`.
    pub fn mv_service(self: &Arc<Self>) -> Arc<MVService> {
        Arc::clone(&self.mv_service_state().service)
    }

    /// The DDL handler Go's `RegisterMVService` registered, for the notifier.
    pub(crate) fn mv_service_ddl_handler(self: &Arc<Self>) -> (tidb_ddl_notifier::HandlerId, tidb_ddl_notifier::Handler) {
        let (id, handler) = &self.mv_service_state().ddl_handler;
        (*id, Arc::clone(handler))
    }

    /// Go `StartMVService`: the service loop and the cross-server DDL
    /// notification loops (the latter only with etcd).
    ///
    /// # Errors
    /// A loop thread could not start.
    pub fn start_mv_service(self: &Arc<Self>) -> Result<MvServiceGuard, String> {
        let state = self.mv_service_state();
        let mut loops = Vec::new();
        let service = Arc::clone(&state.service);
        loops.push(spawn_mv_service_loop("mvService", move || service.run())?);
        if let Some(etcd) = self.etcd_client() {
            // Go `notifyMVSMetadataChange`.
            let notify = state.ddl_notify_rx.clone();
            let exit = state.service_done();
            let put_etcd = Arc::clone(&etcd);
            loops.push(spawn_mv_service_loop("mvServiceNotify", move || loop {
                crossbeam_channel::select! {
                    recv(exit) -> _ => return,
                    recv(notify) -> message => {
                        if message.is_err() {
                            return;
                        }
                        if let Err(err) = put_etcd.put(MVS_DDL_NOTIFY_KEY.as_bytes(), b"") {
                            bg_logger().warn(
                                "notify mvservice metadata change failed",
                                &[Field::new("error", Value::Str(err.to_string()))],
                            );
                        }
                    }
                }
            })?);
            // Go `watchMVSMetaChange`: the etcd watcher's thread delivers
            // each change; it stops with the guard.
            let watched = Arc::clone(&state.service);
            match etcd.watch_key(MVS_DDL_NOTIFY_KEY, 0, move |_| watched.notify_ddl_change()) {
                Ok(watcher) => {
                    let exit = state.service_done();
                    loops.push(spawn_mv_service_loop("watchMVSMetaChange", move || {
                        let _ = exit.recv();
                        drop(watcher);
                    })?);
                }
                Err(err) => bg_logger().warn(
                    "watch MVS metadata change loop watch channel closed",
                    &[Field::new("error", Value::Str(err.to_string()))],
                ),
            }
        }
        Ok(MvServiceGuard { cancel: state.cancel.clone(), loops })
    }
}

impl MvServiceState {
    fn service_done(&self) -> Receiver<()> {
        self.service_context().done()
    }

    fn service_context(&self) -> Context {
        self.service.context()
    }
}

/// Go `do.wg.RunWithRecover(loop, recover)`: a panic is logged and counted.
fn spawn_mv_service_loop(
    name: &'static str,
    body: impl FnOnce() + Send + 'static,
) -> Result<std::thread::JoinHandle<()>, String> {
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            if let Err(panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(body)) {
                let text = panic
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_owned())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_default();
                bg_logger().error(
                    "panic in MVService domain loop",
                    &[Field::new("loop", Value::Str(name.to_owned())), Field::new("panic", Value::Str(text))],
                );
                tidb_metrics::materialized_view::MV_SERVICE_PANIC_GAUGE.inc();
            }
        })
        .map_err(|err| err.to_string())
}

/// Go `domain_sysvars.go`'s MV service setters, as the variables'
/// `SetGlobal` hooks call them.
fn register_mv_service_sysvar_hooks(globals: &tidb_domain::global_sysvars::GlobalSysvars, service: &Arc<MVService>) {
    use tidb_vardef::tidb_vars as v;
    let weak = Arc::downgrade(service);
    let hook = |apply: fn(&MVService, &str) -> Result<(), String>| {
        let weak = weak.clone();
        Arc::new(move |value: &str| -> Result<(), String> {
            match weak.upgrade() {
                Some(service) => apply(&service, value),
                None => Ok(()),
            }
        })
    };
    globals.register_set_global_hook(
        v::TIDB_MVIEW_TASK_MAX,
        hook(|service, value| {
            service.set_task_max_concurrency(value.parse::<i64>().map_err(|err| err.to_string())?);
            Ok(())
        }),
    );
    globals.register_set_global_hook(
        v::TIDB_MVIEW_TASK_REFRESH_RATIO,
        hook(|service, value| {
            service.set_refresh_task_concurrency_ratio(value.parse::<f64>().map_err(|err| err.to_string())?);
            Ok(())
        }),
    );
    globals.register_set_global_hook(
        v::TIDB_MVIEW_TASK_THRESHOLD_CPU,
        hook(|service, value| {
            let mut cfg = service.get_task_backpressure_config();
            cfg.cpu_threshold = value.parse::<f64>().map_err(|err| err.to_string())?;
            let _ = service.set_task_backpressure_config(cfg);
            Ok(())
        }),
    );
    globals.register_set_global_hook(
        v::TIDB_MVIEW_TASK_THRESHOLD_MEMORY,
        hook(|service, value| {
            let mut cfg = service.get_task_backpressure_config();
            cfg.mem_threshold = value.parse::<f64>().map_err(|err| err.to_string())?;
            let _ = service.set_task_backpressure_config(cfg);
            Ok(())
        }),
    );
    // Go stores the hours as a duration (`time.Duration(val) * time.Hour`).
    const HOUR: i64 = tidb_timer::go_time::HOUR;
    globals.register_set_global_hook(
        v::TIDB_MVIEW_REFRESH_HIST_TIME,
        hook(|service, value| {
            let _ = service.set_mview_refresh_hist_retention(value.parse::<i64>().map_err(|err| err.to_string())? * HOUR);
            Ok(())
        }),
    );
    globals.register_set_global_hook(
        v::TIDB_MLOG_PURGE_HIST_TIME,
        hook(|service, value| {
            let _ = service.set_mlog_purge_hist_retention(value.parse::<i64>().map_err(|err| err.to_string())? * HOUR);
            Ok(())
        }),
    );
}

/// Go `do.StatsHandle()` as the MV service reads it: the domain's loaded
/// statistics and its connection IDs for auto-analyze processes.
struct DomainMvStatsHandle(Weak<Domain>);

impl tidb_stats_handle_util::AutoAnalyzeProcIdGenerator for DomainMvStatsHandle {
    fn auto_analyze_proc_id(&self) -> u64 {
        self.0.upgrade().map_or(0, |domain| domain.next_conn_id().next_id())
    }

    fn release_auto_analyze_proc_id(&self, id: u64) {
        if let Some(domain) = self.0.upgrade() {
            domain.next_conn_id().release(id);
        }
    }
}

impl tidb_mvservice::MVServiceStatsHandle for DomainMvStatsHandle {
    /// Go `GetNonPseudoPhysicalTableStats`.
    fn get_non_pseudo_physical_table_stats(&self, physical_table_id: i64) -> Option<tidb_mvservice::MLogStats> {
        let domain = self.0.upgrade()?;
        let statistics = domain.stats.load();
        let table = statistics
            .get(&physical_table_id)
            .and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)?;
        if table.hist_coll.pseudo {
            return None;
        }
        Some(tidb_mvservice::MLogStats {
            pseudo: table.hist_coll.pseudo,
            stats_ver: table.hist_coll.stats_version,
            realtime_count: table.hist_coll.realtime_count,
            modify_count: table.hist_coll.modify_count,
            analyzed: tidb_stats::table_is_analyzed(table.last_analyze_version),
            analyze_row_count: table.hist_coll.analyze_row_count(),
        })
    }
}

/// Go `RegisterMVService`'s statistics arguments: `do.StatsHandle()`,
/// `&do.sysProcesses` and `do.stopAutoAnalyze.Load` (the domain gone).
fn mv_service_stats_seams(
    domain: &Arc<Domain>,
) -> (tidb_mvservice::StatsHandleGetter, tidb_mvservice::SysProcTracker, Arc<dyn Fn() -> bool + Send + Sync>) {
    let handle: Arc<dyn tidb_mvservice::MVServiceStatsHandle> = Arc::new(DomainMvStatsHandle(Arc::downgrade(domain)));
    let (track, untrack) = crate::sys_session::system_process_trackers(domain.processes());
    let weak = Arc::downgrade(domain);
    (
        Arc::new(move || Some(Arc::clone(&handle))),
        tidb_mvservice::SysProcTracker { track, untrack },
        Arc::new(move || weak.strong_count() == 0),
    )
}
