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

//! Statistics-version metadata predicates from `pkg/statistics/histogram.go`.
//!
//! This leaf deliberately does not know how a version is persisted, selected
//! by ANALYZE, or attached to a table/column/index.  It only preserves the
//! source's version constants and raw-value predicates.

/// No statistics were collected; only metadata may be present.
pub const VERSION_0: i64 = 0;

/// Legacy statistics layout.
pub const VERSION_1: i64 = 1;

/// Current statistics layout.
pub const VERSION_2: i64 = 2;

/// Returns whether a non-zero statistics version is analyzed.
#[must_use]
pub const fn is_analyzed(stats_version: i64) -> bool {
    stats_version != VERSION_0
}

/// Returns whether column statistics are analyzed or synthesized from a
/// default value's NDV/null-count metadata.
#[must_use]
pub const fn is_column_analyzed_or_synthesized(
    stats_version: i64,
    ndv: i64,
    null_count: i64,
) -> bool {
    is_analyzed(stats_version) || ndv > 0 || null_count > 0
}
