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

//! Distinct-value and global-singleton estimators from
//! `pkg/statistics/estimate.go`.
//!
//! This module consumes the single [`FmSketch`](crate::FmSketch) authority.
//! It deliberately keeps the source's copied-merge shape: an estimator never
//! mutates a sketch supplied by its caller.

use std::sync::atomic::{AtomicBool, Ordering};

use crate::FmSketch;

static ENABLE_INTERNAL_CHECK: AtomicBool = AtomicBool::new(false);

/// Enables or disables TiDB-style runtime internal assertions, returning the
/// previous setting.
///
/// Debug builds correspond to Go's `intest` build tag and always assert. In a
/// release build this switch corresponds to Go `EnableInternalCheck`, making
/// the same assertions runtime-selectable instead of compiling them away.
pub fn set_internal_check_enabled(enabled: bool) -> bool {
    ENABLE_INTERNAL_CHECK.swap(enabled, Ordering::SeqCst)
}

fn intest_assert(condition: bool, message: &'static str) {
    if (cfg!(debug_assertions) || ENABLE_INTERNAL_CHECK.load(Ordering::SeqCst)) && !condition {
        panic!("assert failed, {message}");
    }
}

/// Calculates sampled NDV and scale ratio for CMSketch/TopN construction.
///
/// The scalar arguments are the fields read from Go's private `topNHelper`.
/// Keeping the estimator independent of the CMSketch builder avoids a second
/// estimator implementation while preserving the source branch order.
#[must_use]
pub(crate) fn calculate_estimate_ndv(
    sample_size: u64,
    sample_ndv: u64,
    singleton_items: u64,
    row_count: u64,
) -> (u64, u64) {
    let scale_ratio = row_count / sample_size;

    if singleton_items == sample_size {
        // The sample is unique, so the source does not scale singleton count.
        (row_count, 1)
    } else if singleton_items == 0 {
        // The source assumes the data consists only of sampled values.
        (sample_ndv, scale_ratio)
    } else {
        (
            estimate_ndv_by_gee(sample_ndv, singleton_items, sample_size, row_count),
            scale_ratio,
        )
    }
}

/// Estimates NDV with the GEE estimator used by TiDB analyze.
///
/// This is the source formula `sqrt(N/n) * f1 + d - f1`, rounded half up and
/// clamped to the observed sample NDV and the row count.
#[must_use]
pub fn estimate_ndv_by_gee(
    sample_ndv: u64,
    singleton_items: u64,
    sample_size: u64,
    row_count: u64,
) -> u64 {
    // Go's intest.Assert is enabled by its internal/unit-test mode or its
    // runtime internal-check switch. Keep the defensive zero return reachable
    // in release builds when that switch is disabled.
    intest_assert(sample_size > 0, "sampleSize should be greater than 0");
    intest_assert(sample_ndv > 0, "sampleNDV should be greater than 0");
    if sample_size == 0 || sample_ndv == 0 {
        return 0;
    }
    intest_assert(
        row_count >= sample_ndv,
        "rowCount should be greater than or equal to sampleNDV",
    );

    let singleton_items = singleton_items as f64;
    let sample_size = sample_size as f64;
    let row_count_float = row_count as f64;
    let sample_ndv_float = sample_ndv as f64;
    let estimate =
        sample_ndv_float + (f64::sqrt(row_count_float / sample_size) - 1.0) * singleton_items;
    let ndv = (estimate + 0.5) as u64;
    let ndv = ndv.max(sample_ndv);
    if row_count > 0 {
        ndv.min(row_count)
    } else {
        ndv
    }
}

/// Estimates values that occur exactly once across all supplied nodes.
///
/// `None` is the Rust representation of a nil Go `*FMSketch`. Debug builds
/// reject nil entries with the same internal invariants as the source; the
/// defensive empty/mismatched-length return remains effective in release.
#[must_use]
pub fn estimate_global_singleton_by_sketches(
    ndv_sketches: &[Option<FmSketch>],
    singleton_sketches: &[Option<FmSketch>],
) -> u64 {
    intest_assert(!ndv_sketches.is_empty(), "ndvSketches shouldn't be empty");
    intest_assert(
        ndv_sketches.len() == singleton_sketches.len(),
        "ndvSketches and singletonSketches should have the same length",
    );
    intest_assert(
        ndv_sketches.iter().all(Option::is_some),
        "ndvSketches must not contain nil entries",
    );
    intest_assert(
        singleton_sketches.iter().all(Option::is_some),
        "singletonSketches must not contain nil entries",
    );
    if ndv_sketches.is_empty() || ndv_sketches.len() != singleton_sketches.len() {
        return 0;
    }

    let middle = ndv_sketches.len() - ndv_sketches.len() / 2;
    let mut left_half_ndv = None;
    for sketch in &ndv_sketches[..middle] {
        left_half_ndv = merge_copied_fm_sketch(left_half_ndv, sketch.as_ref());
    }
    let mut right_half_ndv = None;
    for sketch in &ndv_sketches[middle..] {
        right_half_ndv = merge_copied_fm_sketch(right_half_ndv, sketch.as_ref());
    }

    let mut global_singleton = estimate_global_singleton_in_range(
        &ndv_sketches[..middle],
        &singleton_sketches[..middle],
        right_half_ndv.as_ref(),
    );
    global_singleton = global_singleton.wrapping_add(estimate_global_singleton_in_range(
        &ndv_sketches[middle..],
        &singleton_sketches[middle..],
        left_half_ndv.as_ref(),
    ));
    intest_assert(global_singleton >= 0, "globalSingleton must be positive");
    global_singleton as u64
}

fn estimate_global_singleton_in_range(
    ndv_sketches: &[Option<FmSketch>],
    singleton_sketches: &[Option<FmSketch>],
    out_of_range_ndv_sketch: Option<&FmSketch>,
) -> i64 {
    let mut global_singleton = 0_i64;
    let mut prefix_ndv_sketch = None;

    for index in 0..ndv_sketches.len() {
        let mut other = merge_copied_fm_sketch(None, prefix_ndv_sketch.as_ref());
        for sketch in &ndv_sketches[index + 1..] {
            other = merge_copied_fm_sketch(other, sketch.as_ref());
        }
        other = merge_copied_fm_sketch(other, out_of_range_ndv_sketch);

        let ndv_other = other.as_ref().map_or(0, FmSketch::ndv);
        other = merge_copied_fm_sketch(other, singleton_sketches[index].as_ref());
        let ndv_union = other.as_ref().map_or(0, FmSketch::ndv);
        // FM estimates are not monotone under merge. The source clamps every
        // node before accumulation so one noisy negative estimate cannot
        // subtract another node's real singleton contribution.
        global_singleton =
            global_singleton.wrapping_add(0_i64.max(ndv_union.wrapping_sub(ndv_other)));
        prefix_ndv_sketch = merge_copied_fm_sketch(prefix_ndv_sketch, ndv_sketches[index].as_ref());
    }

    global_singleton
}

fn merge_copied_fm_sketch(
    destination: Option<FmSketch>,
    source: Option<&FmSketch>,
) -> Option<FmSketch> {
    let Some(source) = source else {
        return destination;
    };
    match destination {
        None => Some(source.clone()),
        Some(mut destination) => {
            destination.merge(source);
            Some(destination)
        }
    }
}
