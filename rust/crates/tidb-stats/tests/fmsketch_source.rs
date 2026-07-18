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

//! Source-backed tests for the raw-hash FM sketch boundary.
//!
//! These tests use the original Go tests' direct `insertHashValue` and merge
//! scenarios.  Datum/tablecodec hashing and tipb protobuf round trips are
//! intentionally left to their future owners rather than being approximated.

use tidb_stats::{FmSketch, MAX_SKETCH_SIZE};

#[test]
fn source_threshold_advances_mask_and_retains_zero_suffixes() {
    let mut sketch = FmSketch::new(2);
    sketch.insert_hash(1);
    sketch.insert_hash(2);
    assert_eq!(sketch.mask(), 0);
    assert_eq!(sketch.len(), 2);
    assert_eq!(sketch.ndv(), 2);

    // The third hash crosses maxSize.  Go advances mask 0 -> 1 and keeps the
    // even values (2 and 4), so the estimate remains 2 * 2.
    sketch.insert_hash(4);
    assert_eq!(sketch.mask(), 1);
    assert_eq!(sketch.len(), 2);
    assert!(sketch.contains(2));
    assert!(sketch.contains(4));
    assert!(!sketch.contains(1));
    assert_eq!(sketch.ndv(), 4);
}

#[test]
fn source_duplicate_insert_still_advances_when_threshold_is_zero() {
    // The Go method inserts into the map and checks len on every admitted
    // value; a duplicate therefore still advances a zero-sized sketch.
    let mut sketch = FmSketch::new(0);
    sketch.insert_hash(0);
    assert_eq!(sketch.mask(), 1);
    sketch.insert_hash(0);
    assert_eq!(sketch.mask(), 3);
    assert_eq!(sketch.len(), 1);
    assert_eq!(sketch.ndv(), 4);
}

#[test]
fn source_merge_raises_mask_filters_destination_and_replays_source() {
    let mut destination = FmSketch::new(10);
    destination.insert_hash(1);
    destination.insert_hash(2);

    let mut source = FmSketch::new(1);
    source.insert_hash(1);
    source.insert_hash(2);
    assert_eq!(source.mask(), 1);
    assert!(source.contains(2));
    assert!(!source.contains(1));

    destination.merge(&source);
    assert_eq!(destination.mask(), 1);
    assert_eq!(destination.len(), 1);
    assert!(destination.contains(2));
    assert_eq!(destination.ndv(), 2);
}

#[test]
fn source_copy_and_memory_shape_are_independent() {
    let mut sketch = FmSketch::new(MAX_SKETCH_SIZE);
    sketch.insert_hashes([7, 9, 7]);
    let clone = sketch.clone();

    assert_eq!(clone.mask(), sketch.mask());
    assert_eq!(clone.len(), sketch.len());
    assert_eq!(clone.ndv(), sketch.ndv());
    assert_eq!(sketch.memory_usage(), 16 + 8 * sketch.len() as u64);

    sketch.insert_hash(11);
    assert_eq!(clone.len(), 2);
    assert!(!clone.contains(11));
}
