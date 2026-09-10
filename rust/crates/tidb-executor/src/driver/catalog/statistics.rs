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

use std::collections::HashMap;
use std::sync::{Arc, RwLock, Weak};

use crate::access_cost::TableStatistics;

/// The domain's canonical Go-compatible statistics, independent of InfoSchema.
/// Published tables are immutable: loading or evicting a payload publishes a
/// new table object, as Go's cache and the cluster SharedStats implementation do.
pub trait StatisticsSource: Send + Sync {
    /// Missing or pseudo statistics return `None`.
    fn table(&self, physical_id: i64) -> Option<Arc<tidb_stats::Table>>;
}

/// Only the schema metadata used when converting canonical statistics.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct StatisticsSchema {
    pub columns: Vec<(i64, bool)>,
    pub indexes: Vec<(i64, usize, bool)>,
}

struct ConvertedTable {
    source: Weak<tidb_stats::Table>,
    schema: Arc<StatisticsSchema>,
    value: Arc<TableStatistics>,
}

/// Shared conversion cache, not an independently published statistics cache.
/// Every read resolves the current domain table before using a converted value.
pub struct StatisticsView {
    source: Arc<dyn StatisticsSource>,
    converted: RwLock<HashMap<i64, ConvertedTable>>,
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
        Self {
            source,
            converted: RwLock::default(),
        }
    }

    pub(super) fn table(
        &self,
        physical_id: i64,
        schema: &Arc<StatisticsSchema>,
    ) -> Option<Arc<TableStatistics>> {
        let Some(source) = self.source.table(physical_id) else {
            self.converted
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&physical_id);
            return None;
        };
        let matches = |entry: &ConvertedTable| {
            entry.source.as_ptr() == Arc::as_ptr(&source) && entry.schema == *schema
        };
        if let Some(entry) = self
            .converted
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&physical_id)
            .filter(|entry| matches(entry))
        {
            return Some(Arc::clone(&entry.value));
        }
        let mut converted = self
            .converted
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = converted.get(&physical_id).filter(|entry| matches(entry)) {
            return Some(Arc::clone(&entry.value));
        }
        // Weak references prevent address reuse while allowing obsolete raw
        // payloads to be released. Prune conversions no longer in any snapshot.
        converted.retain(|_, entry| entry.source.strong_count() != 0);
        let value = Arc::new(crate::load_stats::table_statistics_from_table_schema(
            &source,
            &schema.columns,
            &schema.indexes,
        ));
        converted.insert(
            physical_id,
            ConvertedTable {
                source: Arc::downgrade(&source),
                schema: Arc::clone(schema),
                value: Arc::clone(&value),
            },
        );
        Some(value)
    }
}
