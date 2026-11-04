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

//! Go `Domain.TelemetryLoop`: the initial report, then a report every
//! `telemetry.ReportInterval` and a sub-window rotation every
//! `telemetry.SubWindowSize`, on one internal session.

use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Instant;

use tidb_datatype::Datum;
use tidb_infoschema_issyncer::catalog_watch::InfoCache;
use tidb_log::{Field, Value};

use super::{Domain, UsageWorkerStop};
use crate::sys_session::DomainSysSession;

/// Go `ses[5]` as telemetry reads it.
struct TelemetrySession {
    session: tidb_syssession::Session<dyn DomainSysSession>,
    catalog: Arc<InfoCache>,
}

impl TelemetrySession {
    fn with_context<T>(&self, callback: impl FnOnce(&dyn DomainSysSession) -> Result<T, String>) -> Result<T, String> {
        self.session
            .with_session_context(|context| callback(context).map_err(tidb_syssession::SysSessionError::new))
            .map_err(|error| error.to_string())
    }
}

impl tidb_telemetry::Session for TelemetrySession {
    fn get_global_sys_var(&self, name: &str) -> Result<String, String> {
        self.with_context(|context| context.global_system_var(name).map_err(|error| error.to_string()))
    }

    fn cost_model_version(&self) -> i64 {
        self.with_context(|context| Ok(context.cost_model_version())).unwrap_or(0)
    }

    fn enable_paging(&self) -> bool {
        self.with_context(|context| Ok(context.enable_paging())).unwrap_or(false)
    }

    fn exec_restricted_sql(&self, sql: &str) -> Result<Vec<Vec<Datum>>, String> {
        self.with_context(|context| context.query_internal(sql))
    }

    fn refresh_txn_ctx(&mut self) -> Result<(), String> {
        // Go `RefreshTxnCtx`: commit what the session holds, then
        // `sessiontxn.NewTxn` (pessimistic for an internal session).
        self.with_context(|context| {
            context.commit_txn()?;
            context.new_txn(true)
        })
    }

    fn info_schema(&self) -> Arc<tidb_infoschema::SessionExtendedInfoSchema> {
        self.catalog.get_latest()
    }
}

/// The node's `TelemetryLoop` goroutine.
pub(super) struct TelemetryWorker {
    stop: Arc<UsageWorkerStop>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl TelemetryWorker {
    pub(super) fn start(factory: &Arc<Domain>) -> Option<Self> {
        // Go `ctx.GetSessionVars().InRestrictedSQL = true`: the pool's
        // sessions are internal.
        let session = match factory.advanced_sys_session_pool().get() {
            Ok(session) => session,
            Err(error) => {
                tidb_logutil::bg_logger().warn(
                    "Initial telemetry run failed",
                    &[Field::new("error", Value::Str(error.to_string()))],
                );
                return None;
            }
        };
        let mut session = TelemetrySession {
            session,
            catalog: Arc::clone(&factory.catalog),
        };
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let running = Arc::clone(&stop);
        let weak: Weak<Domain> = Arc::downgrade(factory);
        let thread = tidb_worker_stack::sql_worker_thread("telemetry-loop")
            .spawn(move || {
                if let Err(error) = tidb_telemetry::initial_run(&mut session) {
                    tidb_logutil::bg_logger().warn(
                        "Initial telemetry run failed",
                        &[Field::new("error", Value::Str(error))],
                    );
                }
                let mut next_report = Instant::now() + tidb_telemetry::REPORT_INTERVAL;
                let mut next_sub_window = Instant::now() + tidb_telemetry::SUB_WINDOW_SIZE;
                loop {
                    let now = Instant::now();
                    let wait = next_report.min(next_sub_window).saturating_duration_since(now);
                    if running.wait(wait) || weak.strong_count() == 0 {
                        break;
                    }
                    let now = Instant::now();
                    if now >= next_report {
                        next_report += tidb_telemetry::REPORT_INTERVAL;
                        if let Err(error) = tidb_telemetry::report_usage_data(&mut session) {
                            tidb_logutil::bg_logger().warn(
                                "TelemetryLoop retports usaged data failed",
                                &[Field::new("error", Value::Str(error))],
                            );
                        }
                    }
                    if now >= next_sub_window {
                        next_sub_window += tidb_telemetry::SUB_WINDOW_SIZE;
                        tidb_telemetry::rotate_sub_window();
                    }
                }
                tidb_logutil::bg_logger().info("TelemetryReportLoop exited.", &[]);
            })
            .expect("telemetry worker spawns");
        Some(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for TelemetryWorker {
    fn drop(&mut self) {
        *self
            .stop
            .stopped
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        self.stop.wake.notify_all();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
