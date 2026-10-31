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

//! The statistics a statement reads through its context: Go's
//! `domain.GetDomain(sctx).StatsHandle()`, `StmtCtx.TableStats` and
//! `StmtCtx.StatsLoad`, which Go keeps untyped on the session context (the
//! domain as a session value, `map[int64]any`, `singleflight.Result`). The
//! session context holds them erased; these are their typed reads.

use std::collections::HashMap;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use tidb_sessionctx::{StmtContext, StmtContextData};
use tidb_stats::Table;
use tidb_stats_handle_syncload::sync_load::SyncLoadOutcome;
use tidb_stats_handle::StatsHandle;

/// Go `StmtCtx.StatsLoad`: the synchronous statistics loads one statement
/// waits for.
pub struct PendingStatisticsLoad {
    /// Go `StatsLoad.ResultCh`.
    pub receivers: Vec<Receiver<SyncLoadOutcome>>,
    pub items: Vec<tidb_model::StatsLoadItem>,
    pub timeout: Duration,
    pub started: Instant,
}

/// The statement context's statistics, typed.
pub trait StatsStmtCtx {
    /// The statistics handle this statement plans from. A context with no
    /// domain behind it gets an empty handle, so every table is Go's
    /// `PseudoTable`.
    fn stats_handle(&self) -> &Arc<StatsHandle>;

    /// Installs the domain's statistics handle.
    #[must_use]
    fn with_stats_handle(self, handle: Arc<StatsHandle>) -> Self;

    /// Go `recordTableRuntimeStats`: retain one entry for every visited
    /// logical table, including `None` when the table has no loaded stats.
    fn record_table_runtime_statistics(
        &self,
        table_ids: impl IntoIterator<Item = i64>,
        get: impl FnMut(i64) -> Option<Arc<Table>>,
    );

    /// Snapshot of Go `StmtCtx.TableStats` for plan-replayer dumping.
    fn table_runtime_statistics(&self) -> HashMap<i64, Option<Arc<Table>>>;

    /// Go `SyncLoad`'s pending requests, which `SyncWaitStatsLoad` waits for.
    fn install_pending_statistics_load(
        &self,
        receivers: Vec<Receiver<SyncLoadOutcome>>,
        items: Vec<tidb_model::StatsLoadItem>,
        timeout: Duration,
    );

    /// Takes the pending loads [`Self::install_pending_statistics_load`]
    /// installed.
    fn take_pending_statistics_load(&self) -> Option<PendingStatisticsLoad>;
}

/// [`StatsStmtCtx::with_stats_handle`] on the context's builder.
pub trait StatsStmtCtxData {
    /// [`StatsStmtCtx::with_stats_handle`].
    fn with_stats_handle(&mut self, handle: Arc<StatsHandle>) -> &mut Self;
}

fn erased(handle: Arc<StatsHandle>) -> Arc<dyn std::any::Any + Send + Sync> {
    Arc::new(handle)
}

impl StatsStmtCtx for StmtContext {
    fn stats_handle(&self) -> &Arc<StatsHandle> {
        static EMPTY: OnceLock<Arc<StatsHandle>> = OnceLock::new();
        self.stats_handle_any()
            .and_then(|handle| handle.downcast_ref::<Arc<StatsHandle>>())
            .unwrap_or_else(|| EMPTY.get_or_init(Arc::default))
    }

    fn with_stats_handle(self, handle: Arc<StatsHandle>) -> Self {
        self.with_stats_handle_any(erased(handle))
    }

    fn record_table_runtime_statistics(
        &self,
        table_ids: impl IntoIterator<Item = i64>,
        mut get: impl FnMut(i64) -> Option<Arc<Table>>,
    ) {
        self.record_table_runtime_statistics_any(table_ids, |table_id| {
            get(table_id).map(|stats| stats as Arc<dyn std::any::Any + Send + Sync>)
        });
    }

    fn table_runtime_statistics(&self) -> HashMap<i64, Option<Arc<Table>>> {
        self.table_runtime_statistics_any()
            .into_iter()
            .map(|(table_id, stats)| {
                (
                    table_id,
                    stats.and_then(|stats| stats.downcast::<Table>().ok()),
                )
            })
            .collect()
    }

    fn install_pending_statistics_load(
        &self,
        receivers: Vec<Receiver<SyncLoadOutcome>>,
        items: Vec<tidb_model::StatsLoadItem>,
        timeout: Duration,
    ) {
        self.install_pending_statistics_load_any(Box::new(receivers), items, timeout);
    }

    fn take_pending_statistics_load(&self) -> Option<PendingStatisticsLoad> {
        let pending = self.take_pending_statistics_load_any()?;
        let receivers = pending
            .receivers
            .downcast::<Vec<Receiver<SyncLoadOutcome>>>()
            .map(|receivers| *receivers)
            .unwrap_or_default();
        Some(PendingStatisticsLoad {
            receivers,
            items: pending.items,
            timeout: pending.timeout,
            started: pending.started,
        })
    }
}

impl StatsStmtCtxData for StmtContextData {
    fn with_stats_handle(&mut self, handle: Arc<StatsHandle>) -> &mut Self {
        self.with_stats_handle_any(erased(handle))
    }
}
