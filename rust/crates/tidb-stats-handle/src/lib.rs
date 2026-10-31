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

//! Go `pkg/statistics/handle`: the domain's statistics handle as the planner
//! reads it -- the loaded-table cache, the storage loader and the
//! synchronous-load workers. It is NOT part of the schema image
//! (`tidb-infoschema`); a statement reads it through its context.

use tidb_stats_handle_syncload::sync_load::{StatisticsItemLoader, StatisticsLoadWorkers};
use std::collections::HashMap;
use std::sync::Arc;

pub mod bootstrap;
pub mod catalog_statistics;
pub mod source;
pub mod table_size_stats;

pub use source::{StatisticsSource, StatisticsView};


#[derive(Default)]
pub struct StatsHandle {
    /// Go `Handle`'s statistics view over the domain's canonical cache, when
    /// a cluster node installed one; it answers instead of `values`.
    pub view: std::sync::RwLock<Option<Arc<StatisticsView>>>,
    pub values: std::sync::RwLock<HashMap<i64, Arc<tidb_stats::Table>>>,
    pub loader: std::sync::RwLock<Option<Arc<dyn StatisticsItemLoader>>>,
    pub sync_load: std::sync::RwLock<Option<Arc<tidb_stats_handle_syncload::sync_load::SyncLoadService>>>,
}

impl std::fmt::Debug for StatsHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StatsHandle")
            .field(
                "tables",
                &self
                    .values
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .len(),
            )
            .field(
                "has_loader",
                &self
                    .loader
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_some(),
            )
            .finish()
    }
}

pub fn enqueue_sync_load_failures(
    requested_items: &[tidb_model::StatsLoadItem],
    remaining_items: &std::collections::HashSet<tidb_model::TableItemID>,
) {
    for requested in requested_items {
        if !remaining_items.contains(&requested.table_item_id) {
            continue;
        }
        let mut item = requested.table_item_id;
        item.is_sync_load_failed = true;
        tidb_stats_asyncload::ASYNC_LOAD_HISTOGRAM_NEEDED_ITEMS.insert(item, requested.full_load);
    }
}



impl StatsHandle {
    /// Publishes one table's loaded statistics, which the access-path choice
    /// and `EXPLAIN`'s `estRows` then read instead of the pseudo constants.
    ///
    /// Two publishers reach here, and they must agree: the cluster session
    /// loads `mysql.stats_*` rows a Go `ANALYZE` wrote, and an in-process
    /// `ANALYZE TABLE` computes them. Both build the histograms through the
    /// same code, so a table's estimates do not depend on which storage it
    /// sits on.
    pub fn set_table_statistics(
        &self,
        table_id: i64,
        statistics: Arc<tidb_stats::Table>,
    ) {
        self.values
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(table_id, statistics);
    }

    /// Installs the domain statistics worker used by logical optimization.
    pub fn set_item_loader(
        self: &Arc<Self>,
        loader: Arc<dyn StatisticsItemLoader>,
        workers: Arc<StatisticsLoadWorkers>,
    ) {
        let service_loader = Arc::clone(&loader);
        *self
            .loader
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(loader);
        // Go's sync load reads through the handle's one loader: a newly
        // installed loader replaces the service's, over the same workers.
        *self
            .sync_load
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(
            tidb_stats_handle_syncload::sync_load::SyncLoadService::with_pool(
                service_loader,
                Arc::downgrade(self) as std::sync::Weak<dyn tidb_stats_handle_types::stats_cache::StatsCache>,
                workers,
            ),
        );
    }

    /// The synchronous-load worker service, once the domain started it.
    #[must_use]
    pub fn sync_load_service(&self) -> Option<Arc<tidb_stats_handle_syncload::sync_load::SyncLoadService>> {
        self.sync_load
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Uses the domain cache for cluster planning, independent of schema refresh.
    pub fn set_statistics_view(&self, view: Arc<StatisticsView>) {
        *self
            .view
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(view);
    }

    /// Go `Handle.GetPhysicalTableStats`: one table's cached statistics;
    /// `None` is Go's `PseudoTable`. `schema` supplies the `TableInfo` facts a
    /// domain view's cache miss reads, and is not called otherwise.
    #[must_use]
    pub fn table_statistics(
        &self,
        table_id: i64,
        schema: impl FnOnce() -> Option<Arc<tidb_stats::PseudoStatsTable>>,
    ) -> Option<Arc<tidb_stats::Table>> {
        let view = self
            .view
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        if let Some(view) = view {
            return view.table(table_id, schema()?.as_ref());
        }
        self.values
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&table_id)
            .cloned()
    }
}

impl tidb_stats_handle_types::stats_cache::StatsCache for StatsHandle {
    fn update_stats_cache(&self, tables: Vec<(i64, Arc<tidb_stats::Table>)>) {
        let mut values = self
            .values
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (table_id, statistics) in tables {
            values.insert(table_id, statistics);
        }
    }
}

#[cfg(test)]
mod sync_load_tests {
    use std::sync::Arc;

    use tidb_stats_handle_syncload::sync_load::{StatisticsItemLoader, StatisticsLoadWorkers};

    use super::StatsHandle;

    struct NoLoader;

    impl StatisticsItemLoader for NoLoader {
        fn load_items(
            &self,
            _items: &[tidb_model::StatsLoadItem],
            _resource_group: &str,
        ) -> Result<Vec<(i64, Arc<tidb_stats::Table>)>, String> {
            Ok(Vec::new())
        }
    }

    #[test]
    fn domain_catalogs_share_statistics_load_workers() {
        // Go Domain.StartLoadStatsSubWorkers owns one pool for every session
        // using its StatsHandle; opening catalogs does not start more workers.
        let loader: Arc<dyn StatisticsItemLoader> = Arc::new(NoLoader);
        let workers = StatisticsLoadWorkers::new();
        let weak_workers = Arc::downgrade(&workers);
        let first = Arc::new(StatsHandle::default());
        let second = Arc::new(StatsHandle::default());
        first.set_item_loader(Arc::clone(&loader), Arc::clone(&workers));
        second.set_item_loader(loader, workers);
        assert!(Arc::ptr_eq(
            first.sync_load_service().unwrap().pool(),
            second.sync_load_service().unwrap().pool(),
        ));
        // Go TestSysSessionPoolGoroutineLeak / TestSessionPoolClose: a
        // closed session must not retain its workers after its last view.
        drop(first);
        drop(second);
        assert!(
            weak_workers.upgrade().is_none(),
            "closed handle retains workers"
        );
    }

}
