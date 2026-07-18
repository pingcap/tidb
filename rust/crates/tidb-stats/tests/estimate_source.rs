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

//! Exact source assertions for `pkg/statistics/estimate.go` and the claimed
//! `TestEstimateNDVByGEE` test in `cmsketch_test.go`.

use std::collections::HashMap;

use tidb_datatype::Datum;
use tidb_stats::cmsketch::{encode_integer_datum_value, hash_bytes};
use tidb_stats::estimate::set_internal_check_enabled;
use tidb_stats::{estimate_global_singleton_by_sketches, estimate_ndv_by_gee, FmSketch};

fn sketch(hash_values: impl IntoIterator<Item = u64>) -> Option<FmSketch> {
    let mut sketch = FmSketch::new(1_000);
    sketch.insert_hashes(hash_values);
    Some(sketch)
}

fn sketches_from_integer_samples(
    max_size: usize,
    samples: &[i64],
) -> (Option<FmSketch>, Option<FmSketch>) {
    let mut ndv = FmSketch::new(max_size);
    let mut singletons = FmSketch::new(max_size);
    let mut counts = HashMap::new();
    for &sample in samples {
        *counts.entry(sample).or_insert(0_usize) += 1;
        let encoded =
            encode_integer_datum_value(&Datum::new_int(sample)).expect("integer EncodeValue");
        ndv.insert_hash(hash_bytes(&encoded).h1);
    }
    for (&sample, &count) in &counts {
        if count == 1 {
            let encoded =
                encode_integer_datum_value(&Datum::new_int(sample)).expect("integer EncodeValue");
            singletons.insert_hash(hash_bytes(&encoded).h1);
        }
    }
    (Some(ndv), Some(singletons))
}

#[test]
fn test_estimate_ndv_by_gee_all_source_cases() {
    let cases = [
        ("applies singleton correction", 10, 3, 20, 80, 13),
        ("rounds half up", 10, 7, 20, 45, 14),
        ("keeps sample ndv as lower bound", 10, 7, 20, 10, 10),
    ];
    for (name, sample_ndv, singleton_items, sample_size, row_count, expected) in cases {
        assert_eq!(
            estimate_ndv_by_gee(sample_ndv, singleton_items, sample_size, row_count),
            expected,
            "source case {name}"
        );
    }
}

#[test]
fn test_estimate_ndv_by_gee_invalid_input_assertions() {
    set_internal_check_enabled(true);
    assert_panic_equals(
        || {
            let _ = estimate_ndv_by_gee(1, 1, 0, 1);
        },
        "assert failed, sampleSize should be greater than 0",
    );
    assert_panic_equals(
        || {
            let _ = estimate_ndv_by_gee(0, 0, 1, 1);
        },
        "assert failed, sampleNDV should be greater than 0",
    );
    assert_panic_equals(
        || {
            let _ = estimate_ndv_by_gee(10, 3, 20, 9);
        },
        "assert failed, rowCount should be greater than or equal to sampleNDV",
    );
}

#[test]
fn test_estimate_global_singleton_doc_comment_example() {
    let (a, b, c, d, e, f) = (100, 200, 300, 400, 500, 600);
    let ndv_sketches = vec![sketch([a, b, c]), sketch([b, c, d]), sketch([c, e, f])];
    let singleton_sketches = vec![sketch([a, b, c]), sketch([b, d]), sketch([e, f])];

    assert_eq!(
        estimate_global_singleton_by_sketches(&ndv_sketches, &singleton_sketches),
        4
    );
}

#[test]
fn test_estimate_global_singleton_single_node() {
    let ndv_sketches = vec![sketch([100, 200, 300])];
    let singleton_sketches = vec![sketch([100, 200, 300])];

    assert_eq!(
        estimate_global_singleton_by_sketches(&ndv_sketches, &singleton_sketches),
        3
    );
}

#[test]
fn test_estimate_global_singleton_no_overlap() {
    let ndv_sketches = vec![sketch([100, 200]), sketch([300, 400]), sketch([500, 600])];
    let singleton_sketches = vec![sketch([100, 200]), sketch([300, 400]), sketch([500, 600])];

    assert_eq!(
        estimate_global_singleton_by_sketches(&ndv_sketches, &singleton_sketches),
        6
    );
}

#[test]
fn test_estimate_global_singleton_full_overlap() {
    let ndv_sketches = vec![sketch([100, 200]), sketch([100, 200])];
    let singleton_sketches = vec![sketch([100, 200]), sketch([100, 200])];

    assert_eq!(
        estimate_global_singleton_by_sketches(&ndv_sketches, &singleton_sketches),
        0
    );
}

#[test]
fn test_estimate_global_singleton_negative_contribution_is_clamped() {
    // Exact source samples and maxSize=3. Both sketches traverse integer
    // Datum -> codec.EncodeValue -> Murmur3 before the estimator clamp.
    let (ndv_0, singleton_0) = sketches_from_integer_samples(3, &[0]);
    let (ndv_1, singleton_1) = sketches_from_integer_samples(3, &[0, 0, 0, 1, 1, 4, 7]);
    let ndv_sketches = vec![ndv_0, ndv_1];
    let singleton_sketches = vec![singleton_0, singleton_1];
    assert_eq!(
        estimate_global_singleton_by_sketches(&ndv_sketches, &singleton_sketches),
        2
    );
}

#[test]
fn test_estimate_global_singleton_nil_entry() {
    set_internal_check_enabled(true);
    let ndv_sketches = vec![None, sketch([300, 400])];
    let singleton_sketches = vec![sketch([100, 200]), sketch([300, 400])];
    assert_panic_equals(
        || {
            let _ = estimate_global_singleton_by_sketches(&ndv_sketches, &singleton_sketches);
        },
        "assert failed, ndvSketches must not contain nil entries",
    );

    let ndv_sketches = vec![sketch([100, 200]), sketch([300, 400])];
    let singleton_sketches = vec![None, sketch([300, 400])];
    assert_panic_equals(
        || {
            let _ = estimate_global_singleton_by_sketches(&ndv_sketches, &singleton_sketches);
        },
        "assert failed, singletonSketches must not contain nil entries",
    );
}

#[test]
fn test_estimate_global_singleton_empty_input() {
    set_internal_check_enabled(true);
    assert_panic_equals(
        || {
            let _ = estimate_global_singleton_by_sketches(&[], &[]);
        },
        "assert failed, ndvSketches shouldn't be empty",
    );
}

#[test]
fn test_estimate_global_singleton_mismatched_lengths() {
    set_internal_check_enabled(true);
    let ndv_sketches = vec![sketch([100])];
    let singleton_sketches = vec![sketch([100]), sketch([200])];
    assert_panic_equals(
        || {
            let _ = estimate_global_singleton_by_sketches(&ndv_sketches, &singleton_sketches);
        },
        "assert failed, ndvSketches and singletonSketches should have the same length",
    );
}

fn assert_panic_equals(function: impl FnOnce() + std::panic::UnwindSafe, expected: &str) {
    let payload = std::panic::catch_unwind(function).expect_err("source assertion must panic");
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .expect("panic payload is a string");
    assert_eq!(message, expected);
}
