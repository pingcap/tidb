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

//! Source-backed tests for weighted reservoir selection.

use tidb_stats::WeightedReservoir;

#[test]
fn weighted_sampling_keeps_largest_weights() {
    let mut reservoir = WeightedReservoir::new(3);
    for (weight, payload) in [
        (5, "five"),
        (1, "one"),
        (7, "seven"),
        (3, "three"),
        (9, "nine"),
    ] {
        reservoir.consider(weight, payload);
    }

    let mut weights: Vec<_> = reservoir
        .samples()
        .iter()
        .map(|sample| sample.weight())
        .collect();
    weights.sort_unstable();
    assert_eq!(weights, [5, 7, 9]);
    assert_eq!(reservoir.len(), 3);
}

#[test]
fn weighted_sampling_does_not_replace_equal_minimum() {
    let mut reservoir = WeightedReservoir::new(2);
    reservoir.consider(10, "first");
    reservoir.consider(10, "second");
    reservoir.consider(10, "third");

    let payloads: Vec<_> = reservoir
        .samples()
        .iter()
        .map(|sample| *sample.payload())
        .collect();
    assert!(payloads.contains(&"first"));
    assert!(payloads.contains(&"second"));
    assert!(!payloads.contains(&"third"));
}

#[test]
fn zero_sized_weighted_sampling_is_empty() {
    let mut reservoir = WeightedReservoir::new(0);
    reservoir.consider(1, "ignored");
    assert!(reservoir.is_empty());
}
