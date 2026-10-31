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

//! Go `types.StatsCache`'s write side as `syncload` reaches it.

use std::sync::Arc;

use tidb_stats::Table;

/// Go `StatsCache.UpdateStatsCache`: publishes freshly loaded tables.
pub trait StatsCache: Send + Sync {
    /// Publishes `tables`, keyed by physical table ID, replacing older ones.
    fn update_stats_cache(&self, tables: Vec<(i64, Arc<Table>)>);
}
