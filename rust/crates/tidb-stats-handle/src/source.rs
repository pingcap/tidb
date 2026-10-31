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

use std::sync::Arc;

use tidb_stats::PseudoStatsTable;

/// The domain's canonical Go-compatible statistics, independent of InfoSchema.
/// Published tables are immutable: loading or evicting a payload publishes a
/// new table object, as Go's cache and the cluster SharedStats implementation do.
pub trait StatisticsSource: Send + Sync {
    /// Missing or pseudo statistics return `None`.
    fn table(&self, physical_id: i64) -> Option<Arc<tidb_stats::Table>>;

    /// Go `Handle.GetPhysicalTableStats(physicalID, tblInfo)`: [`Self::table`],
    /// where a cache miss also caches Go's `PseudoTable` for `table` when Go
    /// would. `None` still stands for that `PseudoTable`.
    fn get_physical_table_stats(
        &self,
        physical_id: i64,
        table: &PseudoStatsTable,
    ) -> Option<Arc<tidb_stats::Table>> {
        let _ = table;
        self.table(physical_id)
    }
}

/// The domain's statistics as the planner reads them: Go's `Handle` answering
/// `GetPhysicalTableStats` from the canonical cache.
pub struct StatisticsView {
    source: Arc<dyn StatisticsSource>,
}

impl std::fmt::Debug for StatisticsView {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StatisticsView")
            .finish_non_exhaustive()
    }
}

impl StatisticsView {
    /// One view is shared by every catalog in a domain, including old schemas.
    pub fn new(source: Arc<dyn StatisticsSource>) -> Self {
        Self { source }
    }

    /// Go `Handle.GetPhysicalTableStats`; `None` is Go's `PseudoTable`.
    pub fn table(
        &self,
        physical_id: i64,
        schema: &PseudoStatsTable,
    ) -> Option<Arc<tidb_stats::Table>> {
        self.source
            .get_physical_table_stats(physical_id, schema)
    }
}
