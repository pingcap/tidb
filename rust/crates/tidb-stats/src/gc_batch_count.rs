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

//! Statistics garbage-collection batch count from
//! `pkg/statistics/handle/storage/gc.go`.
//!
//! The Go GC loop converts a total row count into the number of fixed-size
//! batches with integer division and a positive-remainder increment. This
//! leaf keeps that arithmetic independent of the storage/session lifecycle.

/// Returns the number of batches needed for `total` items of size `batch`.
///
/// This is Go's `forCount`: division truncates toward zero, and only a
/// positive remainder rounds the quotient up. Wrapping arithmetic preserves
/// Go's signed behavior at synthetic overflow boundaries; a zero batch still
/// panics, matching Go's divide-by-zero behavior.
#[must_use]
pub fn gc_batch_count(total: i64, batch: i64) -> i64 {
    let mut result = total.wrapping_div(batch);
    if total.wrapping_rem(batch) > 0 {
        result = result.wrapping_add(1);
    }
    result
}
