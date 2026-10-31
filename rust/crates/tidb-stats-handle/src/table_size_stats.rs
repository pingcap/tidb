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

//! The in-process store's [`TableSizeSource`]: the estimates Go's
//! `cache.TableRowStatsCache` serves (`tidb_stats_handle_cache::table_size_stats`),
//! read from this handle's table statistics.

use tidb_infoschema::Catalog;
use tidb_stats_handle_cache::table_size_stats::{estimates_from, TableSizeSource, TableStorageEstimate};

/// The estimates of every stored table in `catalog`, from the catalog's own
/// table statistics (the in-process store's statistics handle).
#[must_use]
pub fn table_storage_estimates(
    catalog: &Catalog,
    handle: &crate::StatsHandle,
) -> Vec<TableStorageEstimate> {
    estimates_from(catalog, &CatalogTableStatistics(catalog, handle))
}

/// The in-process catalog's table statistics.
struct CatalogTableStatistics<'a>(&'a Catalog, &'a crate::StatsHandle);

impl TableSizeSource for CatalogTableStatistics<'_> {
    fn table_rows(&self, physical_id: i64) -> u64 {
        self.1
            .table_statistics(physical_id, || self.0.statistics_schema(physical_id))
            .map_or(0, |statistics| statistics.hist_coll.realtime_count.max(0) as u64)
    }

    fn column_length(&self, physical_id: i64, column_id: i64) -> u64 {
        self.1
            .table_statistics(physical_id, || self.0.statistics_schema(physical_id))
            .and_then(|statistics| {
                statistics.hist_coll.get_column(column_id).map(|column| {
                    column
                        .read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .histogram
                        .tot_col_size
                        .max(0) as u64
                })
            })
            .unwrap_or(0)
    }
}

