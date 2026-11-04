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

//! Go `Domain.SetupWorkloadBasedLearningWorker` and `readTableCostWorker`
//! over the domain's system-session pool.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::time::Duration;

use tidb_datatype::Datum;
use tidb_sqlescape::SqlArg;
use tidb_workloadlearning::{Handle, Session, SessionPool, WlCacheWorker};

use super::{Domain, UsageWorkerStop};
use crate::sys_session::{DomainSysSession, DomainSysSessionPool};

/// Go `kv.WithInternalSourceType(ctx, kv.InternalTxnWorkloadLearning)`.
struct InternalTxnWorkloadLearning;

/// Go `do.sysSessionPool` as `workloadlearning` uses it.
pub struct SysSessionPool(pub Arc<DomainSysSessionPool>);

struct PooledSession<'a> {
    session: &'a tidb_syssession::Session<dyn DomainSysSession>,
}

impl PooledSession<'_> {
    fn with_context<T>(&self, callback: impl FnOnce(&dyn DomainSysSession) -> Result<T, String>) -> Result<T, String> {
        self.session
            .with_session_context(|context| callback(context).map_err(tidb_syssession::SysSessionError::new))
            .map_err(|error| error.to_string())
    }
}

impl Session for PooledSession<'_> {
    fn exec_restricted_sql(&self, sql: &str, args: &[SqlArg<'_>]) -> Result<Vec<Vec<Datum>>, String> {
        use tidb_sqlexec::RestrictedSqlExecutor;
        // Go passes no options, so the statement runs on its own pooled
        // session, not inside this session's transaction.
        self.session
            .exec_restricted_sql(&InternalTxnWorkloadLearning, &[], sql, args)
            .map(|(rows, _)| rows)
            .map_err(|error| error.to_string())
    }

    fn new_txn(&self) -> Result<(), String> {
        // Go `sessiontxn.NewTxn`: `EnterNewTxnDefault`, the session's
        // `tidb_txn_mode` -- pessimistic for an internal session.
        self.with_context(|context| context.new_txn(true))
    }

    fn txn_start_ts(&self) -> Result<u64, String> {
        self.with_context(|context| context.transaction_start_ts(true).map_err(|error| error.to_string()))
    }

    fn commit_txn(&self) -> Result<(), String> {
        self.with_context(|context| context.commit_txn())
    }

    fn enable_non_prepared_plan_cache(&self) {
        let _ = self.with_context(|context| {
            context.set_enable_non_prepared_plan_cache(true);
            Ok(())
        });
    }
}

impl SessionPool for SysSessionPool {
    fn with_session(&self, f: &mut dyn FnMut(&dyn Session) -> Result<(), String>) -> Result<(), String> {
        self.0
            .with_session(|session| f(&PooledSession { session }).map_err(tidb_syssession::SysSessionError::new))
            .map_err(|error| error.to_string())
    }
}

/// The node's `readTableCostWorker`.
pub(super) struct WorkloadLearningWorker {
    stop: Arc<UsageWorkerStop>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl WorkloadLearningWorker {
    pub(super) fn start(factory: &Arc<Domain>) -> Self {
        let stop = Arc::new(UsageWorkerStop {
            stopped: Mutex::new(false),
            wake: Condvar::new(),
        });
        let pool: Arc<dyn SessionPool> =
            Arc::new(SysSessionPool(factory.advanced_sys_session_pool()));
        let handle = Handle::new(Arc::clone(&pool));
        let cache = WlCacheWorker::new(pool);
        let weak: Weak<Domain> = Arc::downgrade(factory);
        let running = Arc::clone(&stop);
        // Go `time.NewTicker(vardef.WorkloadBasedLearningInterval.Load())`:
        // the interval is read once.
        let interval = Duration::from_nanos(
            u64::try_from(tidb_vardef::WORKLOAD_BASED_LEARNING_INTERVAL.load(Ordering::Relaxed)).unwrap_or(0),
        );
        let thread = tidb_worker_stack::sql_worker_thread("read-table-cost")
            .spawn(move || loop {
                if running.wait(interval) {
                    return;
                }
                let Some(factory) = weak.upgrade() else {
                    return;
                };
                let is_owner = factory.stats_owner.as_ref().is_some_and(|owner| owner.is_owner());
                if tidb_vardef::ENABLE_WORKLOAD_BASED_LEARNING.load(Ordering::Relaxed) && is_owner {
                    handle.handle_table_read_cost(&factory.catalog.get_latest());
                    cache.update_table_read_cost_cache();
                }
            })
            .expect("workload-based learning worker spawns");
        Self {
            stop,
            thread: Some(thread),
        }
    }
}

impl Drop for WorkloadLearningWorker {
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
