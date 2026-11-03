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

//! Go `statstypes.ColStatsTimeInfo` and the session's reader of
//! `SHOW COLUMN_STATS_USAGE` (Go `statsUsageImpl.LoadColumnStatsUsage`).

use tidb_datatype::Time;

/// Go `statstypes.ColStatsTimeInfo`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ColStatsTimeInfo {
    /// Most recent predicate use, in the requested session location.
    pub last_used_at: Option<Time>,
    /// Most recent analyze, in the requested session location.
    pub last_analyzed_at: Option<Time>,
}

/// Storage-backed reader used by pinned Go `SHOW COLUMN_STATS_USAGE`.
pub trait ColumnStatsUsageProvider: Send + Sync {
    /// Go `statsUsageImpl.LoadColumnStatsUsage`: the whole usage table.
    fn load_column_stats_usage(
        &self,
        location: &tidb_datatype::SessionTimeZone,
    ) -> Result<
        std::collections::HashMap<
            tidb_model::TableItemID,
            (Option<tidb_datatype::Time>, Option<tidb_datatype::Time>),
        >,
        String,
    >;
}
