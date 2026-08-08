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

//! Aggregate column-statistics behavior from `pkg/statistics/column.go`.
//!
//! The Go implementation mutates a process-global asynchronous-load set from
//! `ColumnStatsIsInvalid`. This dependency-closed port returns that insertion
//! as [`ColumnValidity::load_request`], so its caller can perform the same
//! effect without hiding it behind global state.

use crate::{
    CmsSketch, ColumnMemUsage, FmSketch, Histogram, StatsLoadedStatus, TableItemId, TopN,
    ALL_EVICTED,
};

/// The column metadata used directly by `column.go`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ColumnInfo {
    /// TiDB column identifier.
    pub id: i64,
    /// Lower-cased column name.
    pub name: String,
    /// Whether the MySQL primary-key flag is present.
    pub primary_key: bool,
}

/// A column histogram and its optional sketches.
#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub cmsketch: Option<CmsSketch>,
    pub top_n: Option<TopN>,
    pub fm_sketch: Option<FmSketch>,
    pub info: Option<ColumnInfo>,
    pub histogram: Histogram,
    pub stats_loaded_status: StatsLoadedStatus,
    pub physical_id: i64,
    pub stats_version: i64,
    pub is_handle: bool,
    /// Memory already measured by the source-owned histogram representation.
    pub histogram_memory_usage: i64,
}

impl Default for Column {
    fn default() -> Self {
        Self {
            cmsketch: None,
            top_n: None,
            fm_sketch: None,
            info: None,
            histogram: Histogram::default(),
            stats_loaded_status: StatsLoadedStatus::default(),
            physical_id: 0,
            stats_version: 0,
            is_handle: false,
            histogram_memory_usage: 0,
        }
    }
}

impl Column {
    /// Go `(*Column).Copy`; all owned payloads are independent in the result.
    #[must_use]
    pub fn copy(&self) -> Self {
        self.clone()
    }

    /// Go `TotalRowCount`. Analyze-v2 requires a TopN, as the source does.
    #[must_use]
    pub fn total_row_count(&self) -> f64 {
        let histogram = self.histogram.total_row_count();
        if self.stats_version >= 2 {
            histogram
                + self
                    .top_n
                    .as_ref()
                    .expect("v2 column has no TopN")
                    .total_count() as f64
        } else {
            histogram
        }
    }

    /// Go `NotNullCount`. Analyze-v2 requires a TopN, as the source does.
    #[must_use]
    pub fn not_null_count(&self) -> f64 {
        let histogram = self.histogram.not_null_count();
        if self.stats_version >= 2 {
            histogram
                + self
                    .top_n
                    .as_ref()
                    .expect("v2 column has no TopN")
                    .total_count() as f64
        } else {
            histogram
        }
    }

    /// Go `GetIncreaseFactor`, including its zero-count identity fallback.
    #[must_use]
    pub fn increase_factor(&self, realtime_row_count: i64) -> f64 {
        let column_count = self.total_row_count();
        if column_count == 0.0 {
            1.0
        } else {
            realtime_row_count as f64 / column_count
        }
    }

    /// Go `MemoryUsage`, with the histogram component supplied by its locked
    /// source owner and all integer additions retaining Go `int64` wrapping.
    #[must_use]
    pub fn memory_usage(&self) -> ColumnMemUsage {
        let mut usage = ColumnMemUsage {
            column_id: self.info.as_ref().expect("column has no metadata").id,
            histogram_mem_usage: self.histogram_memory_usage,
            ..ColumnMemUsage::default()
        };
        let mut total = self.histogram_memory_usage;
        if let Some(cmsketch) = &self.cmsketch {
            usage.cmsketch_mem_usage = cmsketch.memory_usage() as i64;
            total = total.wrapping_add(usage.cmsketch_mem_usage);
        }
        if let Some(top_n) = &self.top_n {
            usage.topn_mem_usage = top_n.memory_usage() as i64;
            total = total.wrapping_add(usage.topn_mem_usage);
        }
        if let Some(fm_sketch) = &self.fm_sketch {
            usage.fmsketch_mem_usage = fm_sketch.memory_usage() as i64;
            total = total.wrapping_add(usage.fmsketch_mem_usage);
        }
        usage.total_mem_usage = total;
        usage
    }

    /// Go `DropUnnecessaryData`.
    pub fn drop_unnecessary_data(&mut self) {
        if self.stats_version < 2 {
            self.cmsketch = None;
        }
        self.top_n = None;
        self.histogram.buckets.clear();
        self.stats_loaded_status =
            StatsLoadedStatus::new(self.stats_loaded_status.stats_initialized(), ALL_EVICTED);
    }

    #[must_use]
    pub const fn item_id(&self) -> i64 {
        match &self.info {
            Some(info) => info.id,
            None => panic!("column has no metadata"),
        }
    }

    #[must_use]
    pub const fn is_all_evicted(&self) -> bool {
        self.stats_loaded_status.is_all_evicted()
    }

    #[must_use]
    pub const fn evicted_status(&self) -> i32 {
        self.stats_loaded_status.evicted_status()
    }

    #[must_use]
    pub const fn is_stats_initialized(&self) -> bool {
        self.stats_loaded_status.stats_initialized()
    }

    #[must_use]
    pub const fn is_full_load(&self) -> bool {
        self.stats_loaded_status.is_full_load()
    }

    #[must_use]
    pub const fn stats_version(&self) -> i64 {
        self.stats_version
    }

    #[must_use]
    pub const fn is_cms_exist(&self) -> bool {
        self.cmsketch.is_some()
    }

    #[must_use]
    pub const fn is_analyzed(&self) -> bool {
        self.stats_version > 0
    }

    /// Go `StatsAvailable`, including synthesized default-value statistics.
    #[must_use]
    pub const fn stats_available(&self) -> bool {
        self.stats_version > 0 || self.histogram.ndv > 0 || self.histogram.null_count > 0
    }

    #[must_use]
    pub const fn histogram(&self) -> &Histogram {
        &self.histogram
    }

    #[must_use]
    pub const fn top_n(&self) -> Option<&TopN> {
        self.top_n.as_ref()
    }
}

/// Go's nil-receiver behavior for `(*Column).Copy`.
#[must_use]
pub fn copy_column(column: Option<&Column>) -> Option<Column> {
    column.map(Column::copy)
}

/// Go's nil-receiver behavior for `(*Column).IsAllEvicted`.
#[must_use]
pub fn column_is_all_evicted(column: Option<&Column>) -> bool {
    column.is_none_or(Column::is_all_evicted)
}

/// Inputs owned by `planctx.PlanContext` and `HistColl` in Go.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ColumnValidityContext {
    pub has_plan_context: bool,
    pub restricted_sql: bool,
    pub has_statement_context: bool,
    pub cannot_trigger_load: bool,
    pub pseudo: bool,
    pub physical_id: i64,
    pub sync_load_failed: bool,
}

/// Pure result of Go `ColumnStatsIsInvalid` plus its asynchronous-load effect.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnValidity {
    pub invalid: bool,
    pub load_request: Option<TableItemId>,
}

#[must_use]
pub fn column_stats_validity(
    column: Option<&Column>,
    context: ColumnValidityContext,
    column_id: i64,
) -> ColumnValidity {
    if context.has_plan_context && context.restricted_sql {
        return ColumnValidity {
            invalid: true,
            load_request: None,
        };
    }

    let load_needed = column.is_none_or(|column| {
        !column.is_stats_initialized() || column.stats_loaded_status.is_load_needed()
    });
    let load_request = (context.has_plan_context
        && load_needed
        && context.has_statement_context
        && column_id > 0
        && !context.cannot_trigger_load)
        .then_some(TableItemId {
            table_id: context.physical_id,
            id: column_id,
            is_index: false,
            is_sync_load_failed: context.sync_load_failed,
        });

    let invalid = context.pseudo
        || column.is_none_or(|column| {
            column.total_row_count() == 0.0
                || (!column.stats_loaded_status.is_essential_stats_loaded()
                    && column.histogram.ndv > 0)
        });
    ColumnValidity {
        invalid,
        load_request,
    }
}

/// Go `EmptyColumn` at the dependency-closed metadata boundary.
#[must_use]
pub fn empty_column(physical_id: i64, pk_is_handle: bool, info: ColumnInfo) -> Column {
    let id = info.id;
    let is_handle = pk_is_handle && info.primary_key;
    Column {
        physical_id,
        info: Some(info),
        histogram: Histogram {
            id,
            ..Histogram::default()
        },
        is_handle,
        ..Column::default()
    }
}
