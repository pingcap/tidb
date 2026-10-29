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

//! The parts of Go's planner that read a statement's context and the schema
//! image together: `RequestLoadStats` / `SyncWaitStatsLoad`, and the
//! statement state a [`PlannerCatalog`] is planned under.

use tidb_stats_handle::catalog_statistics::CatalogStatistics;
use crate::StatsStmtCtx as _;
use crate::{Catalog, StmtContext};
use tidb_infoschema::PlannerCatalog;

/// Go `RequestLoadStats` and `SyncWaitStatsLoad` over a statement's context.
pub trait CatalogStatisticsLoad {
    /// Go `RequestLoadStats`: start the workers at
    /// `CollectPredicateColumnsPoint` and leave synchronous waiting to the
    /// later `SyncWaitStatsLoadPoint`.
    fn request_statistics_load(
        &self,
        usage: &tidb_stats::table_statistics::ColumnStatsUsage,
        context: &StmtContext,
    ) -> Result<(), tidb_planner::plan_base::PlanError>;

    /// Go `SyncWaitStatsLoad`, at the later logical-rule position.
    fn wait_statistics_load(
        &self,
        context: &StmtContext,
    ) -> Result<(), tidb_planner::plan_base::PlanError>;
}

impl CatalogStatisticsLoad for Catalog {
    /// Go `RequestLoadStats`: start the workers at
    /// `CollectPredicateColumnsPoint` and leave synchronous waiting to the
    /// later `SyncWaitStatsLoadPoint`.
    fn request_statistics_load(
        &self,
        usage: &tidb_stats::table_statistics::ColumnStatsUsage,
        context: &StmtContext,
    ) -> Result<(), tidb_planner::plan_base::PlanError> {
        let wait = std::time::Duration::from_millis(context.stats_load_wait_ms());
        if wait.is_zero() {
            // Go `CollectPredicateColumnsPoint` without sync load queues every
            // needed item as it is; only `SendLoadRequests` (the sync path)
            // drops the ones the cache already holds.
            for (item, full_load) in self.statistics_demand(context.stats_handle(), usage) {
                tidb_stats::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.insert(item, full_load);
            }
            return Ok(());
        }
        let items = self.statistics_load_items(context.stats_handle(), usage, true);
        if items.is_empty() {
            return Ok(());
        }
        let Some(service) = context.stats_handle().sync_load_service() else {
            return Ok(());
        };
        let receivers = service.request(&items, context.resource_group_name(), wait);
        context.install_pending_statistics_load(receivers, items, wait);
        Ok(())
    }

    /// Go `SyncWaitStatsLoad`, at the later logical-rule position.
    fn wait_statistics_load(
        &self,
        context: &StmtContext,
    ) -> Result<(), tidb_planner::plan_base::PlanError> {
        use tidb_stats_handle_metrics::{
            SYNC_LOAD_HISTOGRAM, SYNC_LOAD_TIMEOUT_TOTAL, SYNC_LOAD_TOTAL,
        };

        if context.sync_stats_failed() {
            return Ok(());
        }
        let Some(pending) = context.take_pending_statistics_load() else {
            return Ok(());
        };
        if pending.items.is_empty() {
            return Ok(());
        }
        /// Go `SyncWaitStatsLoad`'s deferred `DurationOptimizer.StatsSyncWait`.
        struct RecordStatsSyncWait<'a>(
            &'a tidb_variable::phase_durations::SessionPhaseDurations,
            std::time::Instant,
        );
        impl Drop for RecordStatsSyncWait<'_> {
            fn drop(&mut self) {
                self.0.set_stats_sync_wait(self.1.elapsed());
            }
        }
        let _record_wait =
            RecordStatsSyncWait(context.phase_durations(), std::time::Instant::now());
        let now = std::time::Instant::now();
        let deadline = now.checked_add(pending.timeout).unwrap_or(now);
        let requested_items = pending.items;
        let mut remaining_items = requested_items
            .iter()
            .map(|item| item.table_item_id)
            .collect::<std::collections::HashSet<_>>();
        let mut wait_error = None;
        for receiver in pending.receivers {
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            let result = receiver.recv_timeout(remaining);
            SYNC_LOAD_TOTAL.inc();
            match result {
                Ok(tidb_stats_handle::sync_load::SyncLoadOutcome::TransportError(error)) => {
                    tracing::warn!(error = %error, "synchronous statistics load request failed");
                }
                Ok(tidb_stats_handle::sync_load::SyncLoadOutcome::Item { item, error }) => {
                    if let Some(error) = error {
                        tracing::warn!(error = %error, "synchronous statistics item load failed");
                    }
                    // A worker error is still a delivered result. Only missing
                    // results fail Go's SyncWaitStatsLoad completion check.
                    remaining_items.remove(&item);
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    SYNC_LOAD_TIMEOUT_TOTAL.inc();
                    wait_error = Some("sync load stats timeout");
                    break;
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    wait_error = Some("sync load stats channel closed unexpectedly");
                    break;
                }
            }
        }
        if wait_error.is_none() {
            if remaining_items.is_empty() {
                SYNC_LOAD_HISTOGRAM.observe(pending.started.elapsed().as_millis() as f64);
                return Ok(());
            }
            // The singleflight timer may finish before the statement starts
            // waiting. Apply the same failure policy regardless of timer order.
            SYNC_LOAD_TIMEOUT_TOTAL.inc();
            wait_error =
                Some("sync load stats failed: some requested items are not loaded in time");
        }
        tidb_stats_handle::enqueue_sync_load_failures(&requested_items, &remaining_items);
        context.report_sync_stats_failed();
        let message = wait_error.expect("an incomplete statistics wait has an error");
        if context.stats_load_pseudo_timeout() {
            context.set_skip_plan_cache("sync-load timed out and fell back to pseudo stats");
            context.append_warning_parts(1105, message);
            return Ok(());
        }
        Err(tidb_planner::plan_base::PlanError::internal(message))
    }

}

/// The statement state a [`PlannerCatalog`] is planned under, read off the
/// statement's context.
pub(crate) trait PlannerCatalogContext: Sized {
    fn with_statement_dirty_tables(self, ctx: &StmtContext) -> Self;
    fn with_statement_privilege_verifier(self, ctx: &StmtContext) -> Self;
    fn with_statement_related_tables_for_mdl(self, ctx: &StmtContext) -> Self;
    fn with_statement_hypo_state(self, ctx: &StmtContext) -> Self;
}

impl PlannerCatalogContext for PlannerCatalog {
    fn with_statement_dirty_tables(self, ctx: &StmtContext) -> Self {
        if ctx.pessimistic_transaction() || ctx.optimistic_transaction() {
            self.with_dirty_tables(ctx.staged_writes().dirty_table_ids())
        } else {
            self
        }
    }

    fn with_statement_privilege_verifier(self, ctx: &StmtContext) -> Self {
        self.with_privilege_verifier(
            ctx.view_definer_privilege().cloned(),
            ctx.session_privilege().cloned(),
        )
    }

    fn with_statement_related_tables_for_mdl(self, ctx: &StmtContext) -> Self {
        self.with_related_tables_for_mdl(ctx.mdl_related_table().cloned())
    }

    fn with_statement_hypo_state(self, ctx: &StmtContext) -> Self {
        if ctx.in_explain_stmt() {
            self.with_explain_hypo_state(
                ctx.hypo_indexes_shared(),
                ctx.hypo_tiflash_replicas_shared(),
            )
        } else {
            self
        }
    }
}
