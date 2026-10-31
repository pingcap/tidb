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

use crate::histogram::Histogram;
use crate::{CmsSketch, ColumnMemUsage, FmSketch, StatsLoadedStatus, TopN, ALL_EVICTED};

/// Go `Column.Info`: the schema's shared `*model.ColumnInfo`.
pub type ColumnInfo = tidb_model::GoShared<tidb_model::ColumnInfo>;

/// A column histogram and its optional sketches.
#[derive(Clone, Debug, Default)]
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
}

impl Column {
    /// Go `(*Column).Copy`; all owned payloads are independent in the result.
    pub fn copy(&self) -> Self {
        let mut copied = self.clone();
        copied.histogram = self.histogram.copy();
        // Go `nc.Info = c.Info.Clone()`.
        copied.info = self
            .info
            .as_ref()
            .and_then(|info| tidb_model::ColumnInfo::clone_pointer(Some(&info.read())));
        copied
    }

    /// Go `TotalRowCount`: version 2 folds the TopN back in.
    pub fn total_row_count(&self) -> f64 {
        let histogram = self.histogram.total_row_count();
        if self.stats_version >= 2 {
            // Go's `(*TopN).TotalCount` is nil-safe.
            histogram + self.top_n.as_ref().map_or(0, TopN::total_count) as f64
        } else {
            histogram
        }
    }

    /// Go `NotNullCount`. Analyze-v2 requires a TopN, as the source does.
    pub fn not_null_count(&self) -> f64 {
        let histogram = self.histogram.not_null_count();
        if self.stats_version >= 2 {
            // Go's `(*TopN).TotalCount` is nil-safe.
            histogram + self.top_n.as_ref().map_or(0, TopN::total_count) as f64
        } else {
            histogram
        }
    }

    /// The payload gate from Go `ColumnStatsIsInvalid`, independent of the
    /// collection owner's load requests and restricted-SQL policy.
    #[must_use]
    pub fn is_valid_for_estimation(&self, pseudo: bool, essential_loaded: bool) -> bool {
        !pseudo && self.total_row_count() != 0.0 && (essential_loaded || self.histogram.ndv <= 0)
    }

    /// Go `mysql.HasUnsignedFlag(c.Info.GetFlag())`.
    #[must_use]
    pub fn is_unsigned(&self) -> bool {
        self.info
            .as_ref()
            .is_some_and(|info| info.read().field_type.is_unsigned())
    }

    /// Go `GetIncreaseFactor`, including its zero-count identity fallback.
    pub fn increase_factor(&self, realtime_row_count: i64) -> f64 {
        let column_count = self.total_row_count();
        if column_count == 0.0 {
            1.0
        } else {
            realtime_row_count as f64 / column_count
        }
    }

    /// Go `MemoryUsage`, with all integer additions retaining Go `int64`
    /// wrapping.
    pub fn memory_usage(&self) -> ColumnMemUsage {
        let mut usage = ColumnMemUsage {
            column_id: self.item_id(),
            histogram_mem_usage: self.histogram.memory_usage(),
            ..ColumnMemUsage::default()
        };
        let mut total = usage.histogram_mem_usage;
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
        self.histogram.drop_bucket_data();
        self.stats_loaded_status =
            StatsLoadedStatus::new(self.stats_loaded_status.stats_initialized(), ALL_EVICTED);
    }

    pub fn item_id(&self) -> i64 {
        self.info.as_ref().expect("column has no metadata").read().id
    }

    pub const fn is_all_evicted(&self) -> bool {
        self.stats_loaded_status.is_all_evicted()
    }

    pub const fn evicted_status(&self) -> i32 {
        self.stats_loaded_status.evicted_status()
    }

    pub const fn is_stats_initialized(&self) -> bool {
        self.stats_loaded_status.stats_initialized()
    }

    pub const fn is_full_load(&self) -> bool {
        self.stats_loaded_status.is_full_load()
    }

    pub const fn stats_version(&self) -> i64 {
        self.stats_version
    }

    pub const fn is_cms_exist(&self) -> bool {
        self.cmsketch.is_some()
    }

    pub const fn is_analyzed(&self) -> bool {
        self.stats_version > 0
    }

    /// Go `StatsAvailable`, including synthesized default-value statistics.
    pub const fn stats_available(&self) -> bool {
        self.stats_version > 0 || self.histogram.ndv > 0 || self.histogram.null_count > 0
    }

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
pub fn column_is_all_evicted(column: Option<&Column>) -> bool {
    column.is_none_or(Column::is_all_evicted)
}

/// Go `EmptyColumn`.
pub fn empty_column(physical_id: i64, pk_is_handle: bool, info: ColumnInfo) -> Column {
    let (id, primary_key) = {
        let info = info.read();
        (
            info.id,
            info.field_type
                .has_flag(tidb_datatype::FieldTypeFlags::PRI_KEY),
        )
    };
    let is_handle = pk_is_handle && primary_key;
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
