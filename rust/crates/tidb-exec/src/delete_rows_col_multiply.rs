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

//! Delete row/column work accounting from `pkg/executor/delete.go`.
//!
//! The source accumulates the number of deleted row values for RUV2 metrics
//! with an `int64` saturating upper bound. This leaf keeps that arithmetic
//! dependency-closed; DELETE execution, chunk iteration, foreign-key skips,
//! batching, and metric publication remain executor/session responsibilities.

/// Adds one delete row/column contribution using the source saturation rules.
///
/// Non-positive deltas and an already saturated total leave the total alone.
/// Positive additions that would exceed `i64::MAX` clamp to that maximum.
#[must_use]
pub fn add_delete_rows_col_multiply(total: i64, delta: i64) -> i64 {
    if delta <= 0 || total == i64::MAX {
        return total;
    }
    if total > i64::MAX - delta {
        return i64::MAX;
    }
    total + delta
}
