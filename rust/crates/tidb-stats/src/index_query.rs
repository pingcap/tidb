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

//! Index byte-query precedence from `pkg/statistics/index.go`.
//!
//! `Index.QueryBytes` first accepts a matching TopN count, then a CMSketch
//! count, and finally falls back to the histogram equal-row count. This leaf
//! preserves that source order over already-resolved caller values; hashing,
//! Datum/tablecodec encoding, and the three statistics structures remain
//! external owners.

/// Chooses the source `Index.QueryBytes` result from resolved lookup values.
///
/// `topn_count` and `cms_count` are `None` when the corresponding structure is
/// absent or has no match. `histogram_count` is the caller's already-converted
/// `uint64` result from `Histogram.EqualRowCount`; it is always the final
/// fallback, matching Go's unconditional conversion after the lookup.
#[must_use]
pub const fn query_index_bytes(
    topn_count: Option<u64>,
    cms_count: Option<u64>,
    histogram_count: u64,
) -> u64 {
    match topn_count {
        Some(count) => count,
        None => match cms_count {
            Some(count) => count,
            None => histogram_count,
        },
    }
}
