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
    MVService, MVServiceSessionContext, MVServiceSessionPool, MetricsReporter, MvsError, ServerInfo, ServiceHelper,
    SystemSessionPool, TaskBackpressureConfig,
};

use super::Domain;

/// Go `mvsDDLNotifyKey`.
const MVS_DDL_NOTIFY_KEY: &str = "/tidb/mvservice/ddl";

/// The domain's MV service and Go's `do.mvService.ddlNotify`.
pub(crate) struct MvServiceState {
    service: Arc<MVService>,
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
            let helper = ServiceHelper {
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
                analyzer: Some(Arc::new(DomainMvAnalyzer(Arc::downgrade(self)))),
                metrics: Arc::new(MetricsReporter::default()),
            };
            // Go `RegisterMVService`: the default config with the default
            // CPU / memory backpressure.
            let mut cfg = tidb_mvservice::default_mv_service_config();
            cfg.task_backpressure = TaskBackpressureConfig {
                cpu_threshold: tidb_mvservice::DEFAULT_BACKPRESSURE_CPU_THRESHOLD,
                mem_threshold: tidb_mvservice::DEFAULT_BACKPRESSURE_MEM_THRESHOLD,
                delay: tidb_mvservice::DEFAULT_TASK_BACKPRESSURE_DELAY,
            };
            let service = tidb_mvservice::new_mv_service(
                ctx,
                Arc::new(SystemSessionPool(Arc::new(DomainMvSessionPool(Arc::downgrade(self))))),
                Arc::new(helper),
                cfg,
            );
            // Trigger one startup refresh so in-memory state catches up with metadata.
            service.notify_ddl_change();
            register_mv_service_sysvar_hooks(&self.global_vars, &service);
            let (ddl_notify_tx, ddl_notify_rx) = bounded(1);
            MvServiceState { service, ddl_notify_tx, ddl_notify_rx, cancel }
        })
    }

    /// Go `GetMVService`.
    pub fn mv_service(self: &Arc<Self>) -> Arc<MVService> {
        Arc::clone(&self.mv_service_state().service)
    }

    /// Go `RegisterMVService`'s `onDDLHandled`: the DDL owner saw an MV
    /// change; fan it out to the other servers.
    pub(crate) fn on_mv_ddl_handled(self: &Arc<Self>) {
        let _ = self.mv_service_state().ddl_notify_tx.try_send(());
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

/// Go `serviceHelper`'s MV log analyze half over the domain: its statistics
/// handle, the system process tracker and `do.stopAutoAnalyze` (the
/// service context ending here).
struct DomainMvAnalyzer(Weak<Domain>);

/// Go `mlogAnalyzeTask` candidates' statistics, as `GetNonPseudoPhysicalTableStats` finds them.
fn mlog_stats(domain: &Domain, mlog_id: i64) -> Option<(tidb_mvservice::MLogStats, i32)> {
    let statistics = domain.stats.load();
    let table = statistics.get(&mlog_id).and_then(tidb_stats_handle_syncload::stats_watch::TableStatsState::loaded)?;
    if table.hist_coll.pseudo {
        return None;
    }
    Some((
        tidb_mvservice::MLogStats {
            pseudo: table.hist_coll.pseudo,
            realtime_count: table.hist_coll.realtime_count,
            modify_count: table.hist_coll.modify_count,
            analyzed: tidb_stats::table_is_analyzed(table.last_analyze_version),
            analyze_row_count: table.hist_coll.analyze_row_count(),
        },
        table.hist_coll.stats_version,
    ))
}

impl DomainMvAnalyzer {
    fn domain(&self) -> Result<Arc<Domain>, MvsError> {
        self.0.upgrade().ok_or_else(|| MvsError::Other("mv service: the domain is closed".to_owned()))
    }

    /// Go `mlogAnalyzeStopped`.
    fn mlog_analyze_stopped(domain: &Domain, ctx: &Context) -> bool {
        ctx.err().is_some()
            || !domain
                .global_vars
                .get(tidb_vardef::tidb_vars::TIDB_ENABLE_AUTO_ANALYZE)
                .map_or(true, |value| value.eq_ignore_ascii_case("ON"))
    }

    /// Go `canAnalyzeMLog`: not stopped and inside the auto-analyze window.
    fn can_analyze_mlog(
        domain: &Domain,
        ctx: &Context,
        sctx: &dyn crate::sys_session::DomainSysSession,
    ) -> bool {
        if Self::mlog_analyze_stopped(domain, ctx) {
            return false;
        }
        let parameters = tidb_stats_handle_autoanalyze_exec::get_auto_analyze_parameters(sctx);
        let get = |name: &str| parameters.get(name).cloned().unwrap_or_default();
        let Ok((start, end)) = tidb_stats_handle_autoanalyze_exec::parse_auto_analysis_window(
            &get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_START_TIME),
            &get(tidb_vardef::tidb_vars::TIDB_AUTO_ANALYZE_END_TIME),
        ) else {
            bg_logger().warn("mv service: failed to parse mlog auto analyze window", &[]);
            return false;
        };
        let now = tidb_mvservice::mvs_now().instant();
        tidb_timeutil::within_day_time_period(start.with_timezone(&chrono::Utc), end.with_timezone(&chrono::Utc), now)
            && !Self::mlog_analyze_stopped(domain, ctx)
    }

    fn with_session<T>(
        domain: &Arc<Domain>,
        f: impl FnOnce(&dyn crate::sys_session::DomainSysSession) -> Result<T, MvsError>,
    ) -> Result<T, MvsError> {
        let pool = domain.advanced_sys_session_pool();
        let mut outcome = None;
        let mut f = Some(f);
        pool.with_session(|session| {
            session.with_session_context(|context| {
                outcome = Some((f.take().expect("called once"))(context));
                Ok(())
            })
        })
        .map_err(|err| MvsError::Other(err.to_string()))?;
        outcome.unwrap_or_else(|| Err(MvsError::Other("mv service: no system session".to_owned())))
    }
}

fn sql_exec_error(err: tidb_sqlexec::SqlExecError) -> MvsError {
    MvsError::Other(err.to_string())
}

impl tidb_mvservice::MVServiceAnalyzer for DomainMvAnalyzer {
    /// Go `LoadAllTiDBMVLogAnalyzeTasks`.
    fn load_all_tidb_mv_log_analyze_tasks(
        &self,
        ctx: &Context,
    ) -> Result<std::collections::HashMap<i64, tidb_mvservice::MlogAnalyzeTask>, MvsError> {
        const FETCH_SQL: &str = "SELECT MLOG_ID FROM mysql.tidb_mlog_purge_info";
        let domain = self.domain()?;
        if Self::mlog_analyze_stopped(&domain, ctx) {
            return Ok(std::collections::HashMap::new());
        }
        Self::with_session(&domain, |sctx| {
            let mut tasks = std::collections::HashMap::new();
            if !Self::can_analyze_mlog(&domain, ctx, sctx) {
                return Ok(tasks);
            }
            let locked = tidb_stats_handle_lockstats::session_stats_lock::query_locked_tables(sctx).map_err(sql_exec_error)?;
            let mv_sctx = sctx.mv_service_session();
            let rows = mv_sctx.exec_restricted_sql(FETCH_SQL, &[])?;
            let ratio = tidb_mvservice::read_mlog_auto_analyze_ratio(mv_sctx);
            let min_count = tidb_stats::AUTO_ANALYZE_MIN_CNT.load(std::sync::atomic::Ordering::SeqCst);
            for row in rows {
                let Some(tidb_datatype::Datum::Int(mlog_id)) = row.first().cloned() else {
                    continue;
                };
                if mlog_id <= 0 || locked.contains(&mlog_id) {
                    continue;
                }
                let Some((schema_name, mlog_name)) = tidb_mvservice::resolve_mv_log_identity_by_id(mv_sctx, mlog_id)? else {
                    continue;
                };
                let Some((stats, _)) = mlog_stats(&domain, mlog_id) else {
                    continue;
                };
                let (need, reason) = tidb_mvservice::need_analyze_mlog(Some(&stats), ratio, min_count);
                if !need {
                    continue;
                }
                tasks.insert(mlog_id, tidb_mvservice::MlogAnalyzeTask { schema_name, mlog_name, reason, ratio });
            }
            Ok(tasks)
        })
    }

    /// Go `AnalyzeMVLog`: one `ANALYZE TABLE` of the log, rechecked first.
    fn analyze_mv_log(&self, ctx: &Context, mlog_id: i64) -> Result<(), MvsError> {
        const ANALYZE_MLOG_SQL: &str = "ANALYZE TABLE %n.%n";
        let domain = self.domain()?;
        if Self::mlog_analyze_stopped(&domain, ctx) {
            return Ok(());
        }
        Self::with_session(&domain, |sctx| {
            tidb_stats_handle_util::update_sctx_vars_for_stats(sctx).map_err(sql_exec_error)?;
            let mv_sctx = sctx.mv_service_session();
            let Some((schema_name, mlog_name)) = tidb_mvservice::resolve_mv_log_identity_by_id(mv_sctx, mlog_id)? else {
                return Ok(());
            };
            let locked = tidb_stats_handle_lockstats::session_stats_lock::query_locked_tables(sctx).map_err(sql_exec_error)?;
            if locked.contains(&mlog_id) {
                return Ok(());
            }
            // A task may have waited in the executor queue or for a pooled session.
            // Recheck current eligibility and controls before starting another ANALYZE.
            let ratio = tidb_mvservice::read_mlog_auto_analyze_ratio(mv_sctx);
            let Some((stats, stats_version)) = mlog_stats(&domain, mlog_id) else {
                return Ok(());
            };
            let min_count = tidb_stats::AUTO_ANALYZE_MIN_CNT.load(std::sync::atomic::Ordering::SeqCst);
            if !tidb_mvservice::need_analyze_mlog(Some(&stats), ratio, min_count).0 {
                return Ok(());
            }
            if !Self::can_analyze_mlog(&domain, ctx, sctx) {
                return Ok(());
            }
            let table_stats_ver = mv_sctx
                .global_system_var(tidb_vardef::tidb_vars::TIDB_ANALYZE_VERSION)?
                .parse::<i32>()
                .map_err(|err| MvsError::Other(err.to_string()))?;
            let need_version_rewrite_warn =
                !tidb_stats::analyze_version_matches(Some(i64::from(stats_version)), stats.pseudo, i64::from(table_stats_ver));
            let ids = domain.next_conn_id();
            let released = Arc::clone(&ids);
            let generator =
                tidb_stats_handle_util::Generator::new(move || ids.next_id(), move |id| released.release(id));
            let (track, untrack) = crate::sys_session::system_process_trackers(domain.processes());
            tidb_stats_handle_autoanalyze_exec::run_analyze_stmt(
                sctx,
                &generator,
                track,
                untrack,
                table_stats_ver,
                need_version_rewrite_warn,
                ANALYZE_MLOG_SQL,
                &[
                    tidb_sqlescape::SqlArg::String(schema_name.as_bytes()),
                    tidb_sqlescape::SqlArg::String(mlog_name.as_bytes()),
                ],
            )
            .map(|_| ())
            .map_err(sql_exec_error)
        })
    }
}
