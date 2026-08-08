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

use tidb_datatype::Datum;
use tidb_stats::SortedHistogramBuilder;

#[test]
fn source_first_value_and_repeats_stay_in_one_bucket() {
    let mut builder = SortedHistogramBuilder::new(2, 7, 2);
    for value in [1, 1, 1] {
        builder.iterate(Datum::Int(value)).unwrap();
    }
    let histogram = builder.histogram();
    assert_eq!(builder.count(), 3);
    assert_eq!(histogram.ndv, 1);
    assert_eq!(histogram.buckets.len(), 1);
    assert_eq!(histogram.buckets[0].count, 3);
    assert_eq!(histogram.buckets[0].repeat, 3);
    assert_eq!(histogram.buckets[0].ndv, 1);
}

#[test]
fn source_full_buckets_merge_and_double_capacity() {
    let mut builder = SortedHistogramBuilder::new(2, 7, 2);
    for value in 1..=8 {
        builder.iterate(Datum::Int(value)).unwrap();
    }
    let histogram = builder.histogram();
    assert_eq!(histogram.ndv, 8);
    assert!(histogram.buckets.len() <= 2);
    assert_eq!(histogram.buckets.last().unwrap().count, 8);
    assert_eq!(
        histogram
            .buckets
            .iter()
            .map(|bucket| bucket.ndv)
            .sum::<i64>(),
        8
    );
}

#[test]
fn source_v1_leaves_bucket_ndv_zero() {
    let mut builder = SortedHistogramBuilder::new(3, 7, 1);
    for value in 1..=6 {
        builder.iterate(Datum::Int(value)).unwrap();
    }
    assert!(builder
        .histogram()
        .buckets
        .iter()
        .all(|bucket| bucket.ndv == 0));
    assert_eq!(builder.histogram().ndv, 6);
}
