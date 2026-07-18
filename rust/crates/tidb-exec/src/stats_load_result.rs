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

//! Statistics-load result metadata from `stmtctx.go`.
//!
//! The statistics sync-loader sends a small value through a result channel:
//! the table/item identity and, optionally, an error. This leaf owns the
//! source `HasError` predicate and stable `ErrorMsg` rendering only. Worker
//! retries, channels, failpoints, storage reads, and live statement/session
//! attachment remain external.

/// Table or index identity carried by a statistics-load result.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatsLoadItemId {
    /// Physical table ID.
    pub table_id: i64,
    /// Column or index ID within the table.
    pub id: i64,
    /// Whether `id` names an index rather than a column.
    pub is_index: bool,
}

impl StatsLoadItemId {
    /// Creates a table-item identity.
    #[must_use]
    pub const fn new(table_id: i64, id: i64, is_index: bool) -> Self {
        Self {
            table_id,
            id,
            is_index,
        }
    }
}

/// Result metadata returned by a statistics-load worker.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatsLoadResult {
    /// Table/column/index identity.
    pub item: StatsLoadItemId,
    /// Worker error text, if loading failed.
    pub error: Option<String>,
}

impl StatsLoadResult {
    /// Creates a successful result for `item`.
    #[must_use]
    pub const fn success(item: StatsLoadItemId) -> Self {
        Self { item, error: None }
    }

    /// Creates a failed result while retaining the source error text.
    #[must_use]
    pub fn failure(item: StatsLoadItemId, error: impl Into<String>) -> Self {
        Self {
            item,
            error: Some(error.into()),
        }
    }

    /// Returns whether this result carries an error.
    pub const fn has_error(&self) -> bool {
        self.error.is_some()
    }

    /// Formats the source `ErrorMsg` payload.
    #[must_use]
    pub fn error_msg(&self) -> String {
        let Some(error) = &self.error else {
            return String::new();
        };
        format!(
            "tableID:{}, id:{}, isIndex:{}, err:{}",
            self.item.table_id, self.item.id, self.item.is_index, error
        )
    }
}
