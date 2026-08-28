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

//! Port ledger for the partition-id / key-range scanning half of
//! `pkg/ddl/ddl_test.go` (`pkg/ddl.part6` batch b105, items 301-302 of the
//! pkg/ddl enumeration).
//!
//! Both Go tests are pure-function tables over helpers that walk physical
//! table ids and TiKV key ranges while a reorganization backfill is running;
//! neither helper is transcreated yet, and neither is reachable through any
//! transcreated carrier in this workspace.

/// GO PORT of `pkg/ddl/ddl_test.go:323 TestFindNextNonTouchedPartitionID`.
///
/// Re-derived contract (pkg/ddl/index.go:3717-3741): with
/// `pi.Definitions = [1,2,3,4,5]` and `DroppingDefinitions = [2,3]` (p2/p3
/// are being reorganized into replacements), `findNextNonTouchedPartitionID`
/// walks the definitions AFTER `curr` in id order and returns the first one
/// that is NOT in `DroppingDefinitions`: curr 1/2/3 -> 4, curr 4 -> 5, curr
/// 5 -> 0 (exhausted), and a curr of 6 -- not a partition of the table at
/// all -- also answers 0. With `Definitions = [1,2,3]` and
/// `DroppingDefinitions = [2,3]`, curr 1 has no non-touched successor and
/// answers 0.
#[test]
#[ignore = "go-parity-gap: findNextNonTouchedPartitionID (pkg/ddl/index.go:3717-3741) -- the global-index backfill's partition iterator -- is not transcreated"]
fn find_next_non_touched_partition_id_skips_dropping_definitions() {}

/// GO PORT of `pkg/ddl/ddl_test.go:360 TestMergeContinuousKeyRanges`.
///
/// Re-derived contract (pkg/ddl/cluster.go:325-357 plus the
/// `keyRangeMayExclude` pair type; precondition: input sorted by start key
/// and non-overlapping, and the gap between ranges carries no data):
/// `mergeContinuousKeyRanges` keeps ONE open run (`continuousStart,
/// continuousEnd`); a non-excluded range opens the run or extends its end;
/// an excluded range FLUSHES the open run into the result and skips. After
/// the loop the open run, if any, is flushed. Seven rows:
/// `{[1,2) exclude}` -> []; `{[1,2)}` -> [[1,2)]; `{[1,2) [3,4)}` ->
/// [[1,4)] (contiguous non-excluded ranges MERGE into one run); `{[1,2)
/// [3,4)x [5,6)}` -> [[1,2) [5,6)]; `{[1,2)x [3,4)x [5,6)}` -> [[5,6)];
/// `{[1,2) [3,4)x [5,6)x}` -> [[1,2)]; `{[1,2)x [3,4) [5,6)x}` -> [[3,4)] --
/// an excluded hole SPLITS the output rather than joining its neighbours.
#[test]
#[ignore = "go-parity-gap: mergeContinuousKeyRanges/keyRangeMayExclude (pkg/ddl/cluster.go:325-355), the flashback/cluster-range folder, is not transcreated"]
fn merge_continuous_key_ranges_folds_excluded_holes_into_splits() {}
