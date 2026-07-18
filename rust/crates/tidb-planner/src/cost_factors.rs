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

//! Dependency-closed planner cost factors from
//! `pkg/planner/core/cost/factors_thresholds.go`.
//!
//! The Go source keeps these values as package constants plus a name-keyed
//! aggregation-factor map. This leaf preserves the exact values and the
//! map's case-sensitive lookup without introducing a second AST aggregate
//! catalog or a cost-model/session owner.

/// Default selectivity used when no more specific estimate is available.
pub const SELECTION_FACTOR: f64 = 0.8;

/// Factor used for distinct aggregation work.
pub const DISTINCT_FACTOR: f64 = 0.8;

/// Tolerance used by source floating-point comparisons.
pub const TOLERANCE_FACTOR: f64 = 0.00001;

/// Threshold at which an ordered scan avoids the descending-scan penalty.
pub const SMALL_SCAN_THRESHOLD: u64 = 10_000;

/// Fallback factor used by the source physical aggregation cost path.
pub const DEFAULT_AGGREGATION_FACTOR: f64 = 1.5;

/// Returns the source aggregation factor for a known aggregate name.
///
/// Names are the lowercase `pkg/parser/ast` aggregate constants. Returning
/// `None` for an unknown name deliberately preserves the Go map's lookup
/// semantics; callers that use the source fallback should call
/// [`aggregation_factor_or_default`].
#[must_use]
pub fn aggregation_factor(name: &str) -> Option<f64> {
    Some(match name {
        "count" | "sum" | "sum_int" | "max" | "min" | "group_concat" => 1.0,
        "avg" => 2.0,
        "firstrow" => 0.1,
        "bit_or" | "bit_xor" | "bit_and" => 0.9,
        "var_pop" | "var_samp" | "stddev_pop" | "stddev_samp" => 3.0,
        "default" => DEFAULT_AGGREGATION_FACTOR,
        _ => return None,
    })
}

/// Returns the source default when an aggregate name is absent from the map.
#[must_use]
pub fn aggregation_factor_or_default(name: &str) -> f64 {
    aggregation_factor(name).unwrap_or(DEFAULT_AGGREGATION_FACTOR)
}
