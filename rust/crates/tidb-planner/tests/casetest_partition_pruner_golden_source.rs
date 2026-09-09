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

//! Documentary gap ports for `pkg/planner/core/casetest/partition/
//! partition_pruner_test.go` (`pkg/planner.part6` items 348–354 on
//! `origin/master`; bootstrap is `partition/main_test.go:30 TestMain`,
//! skipped-reason in the receipt; the integration_partition_test.go and
//! list_partition_integration_test.go halves are sibling files).
//!
//! The pruner family reads its inputs from the `partition_pruner` book,
//! extracts `partition:`/`table:` fields out of plan strings via
//! `coretestsdk.GetFieldValue` (`getPartitionInfoFromPlan`, :87), and asserts
//! both prune modes. Everything asserted — which partitions a constant hash
//! key lands in, list-columns tuple pruning, EXTRACT()-based range pruning,
//! <=> NULL handling — belongs to `pkg/planner/core/partition_pruning.go`,
//! which this workspace has not transcreated, and every test also needs live
//! sessions to build its tables.

/// GO PORT of `pkg/planner/core/casetest/partition/
/// partition_pruner_test.go:34 TestHashPartitionPruner`.
///
/// Re-derived contract: t1..t11 spanning hash-partition expressions
/// (`id`, `id+a`, `year(d)`, `month(d)`, `a+a+a+b`, bigint unsigned, bit(1)),
/// int-only clustered-index mode forced, get partition golden rows pinning
/// e.g. that `select * from t1 where id=7` opens TableReader over exactly
/// `partition:p7` and multi-key IN lists union the right physical tables.
#[test]
#[ignore = "go-parity-gap: constant-key hash partition resolution in pkg/planner/core/partition_pruning.go is unported; needs live session + golden book"]
fn hash_partition_pruner_pins_constants_to_their_partitions() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// partition_pruner_test.go:124 TestListColumnsPartitionPruner`.
///
/// Re-derived contract: LIST COLUMNS((b,a)) / ((id,a,b)) tuple-bucketed tables
/// (plain, unique-indexed, unpartitioned twins), must agree on: plan_tree
/// goldens for both index/no-index shapes, `getPartitionInfoFromPlan`'s sorted
/// "tN: pX" summary equaling each case's expected Pruner string, result-set
/// equality between index/plain/unpartitioned (when no `partition(pN)` is
/// written), under `tidb_regard_null_as_point=false`.
#[test]
#[ignore = "go-parity-gap: tuple-valued list-columns pruning + GetFieldValue extraction over printed plans are unported"]
fn list_columns_pruner_matches_tuple_buckets_and_unpartitioned_results() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// partition_pruner_test.go:221 TestPointGetIntHandleNotFirst`.
///
/// Re-derived contract: clustered PK `(a)` NOT first in column order; point
/// lookup `a BETWEEN 13 AND 13` still returns the row before ALTER TABLE adds
/// range partitions and again after — i.e. int-handle point get keeps working
/// once partitioned.
#[test]
#[ignore = "go-parity-gap: needs DDL re-partition plus executor result rows; int-handle point-get/range merge unported"]
fn point_get_int_handle_not_first_survives_range_partition_alter() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// partition_pruner_test.go:250 TestRangeDatePruningExtract`
/// (driver `runExtractTestCases`, :365).
///
/// Re-derived contract: for DATE/DATETIME(fsp 1|6) columns partitioned by
/// range EXTRACT(unit FROM d) whose boundaries come from evaluating EXTRACT at
/// four timestamps, DATE/DATETIME-compatible units (YEAR, QUARTER,
/// YEAR_MONTH, MONTH, DAY, DAY_HOUR..MICROSECOND with their per-unit
/// compatibility gates like WEEK being never allowed on datetime sources) prune
/// `d op '1991-04-02...'` into the recorded partition sets ("p2", "all",
/// "p0,p1,p2", ...), BETWEEN takes the last entry, invalid partitions fail
/// DDL with `[ddl:1486]Constant, random or timezone-dependent expressions`,
/// and no-fsp collations use the NoFspResult override ("p1") because
/// evaluation truncates while partition definition metadata keeps fsp.
#[test]
#[ignore = "go-parity-gap: EXTRACT-based range pruning against ddl-evaluated partition definitions spans the unported pruner + DDL execution"]
fn range_date_pruning_extract_unit_compatibility_golden() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// partition_pruner_test.go:426 TestRangeTimePruningExtract`.
///
/// Re-derived contract: TIME/TIME(fsp)/TIMESTAMP(fsp) columns reject every
/// date-oriented EXTRACT unit with `[ddl:1486]`, and only HOUR-family units
/// (HOUR, HOUR_MINUTE, HOUR_SECOND, HOUR_MICROSECOND) admit TIME columns;
/// TIMESTAMP behaves like DATETIME — all pinned through the same extract-loop
/// driver.
#[test]
#[ignore = "go-parity-gap: same unported surface as the DATE half of the driver"]
fn range_time_pruning_extract_rejects_date_units_admits_hour_family() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// partition_pruner_test.go:535 TestPartitionPrunerRegression`.
///
/// Re-derived contract: issue 59827's parenthesization bug — IN('1','2'),
/// parenthesized variants and OR-lists over list/key/range-columns partitions
/// must select the same single partitions as bare equality across static AND
/// dynamic modes, including PREPARE/EXECUTE replays; issue 61134 pins empty-string
/// IN pruning to a Point_Get; issue 61176 tables pin `<=>` null-safe lookups'
/// access objects per scheme (RANGE/LIST/KEY char; RANGE/LIST/KEY/HASH int),
/// where KEY-int with unsigned overflow differs and primary-keyed ints raise
/// `[table:1048]Column 'a' cannot be null` on NULL insert yet dual-scan
/// on `<=> NULL`.
#[test]
#[ignore = "go-parity-gap: parenthesized-IN/or/prepare partition selection, <=> range construction and TableDual-vs-IndexRangeScan choices all live in the unported pruner"]
fn partition_pruner_regression_in_parens_prepare_and_null_safe() {}

/// GO PORT of `pkg/planner/core/casetest/partition/
/// partition_pruner_test.go:747 TestCast`.
///
/// Re-derived contract: two utf8-charset range-columns(string)-partitioned
/// tables with global indexes joined on tinyint/varchar keys return exactly
/// one row (`53196 1`) through SUM+GROUP BY+HAVING — proving global-index
/// partition casts don't lose rows during cross-partition join planning.
#[test]
#[ignore = "go-parity-gap: join execution over global-index partitioned tables with binary-literal inserts needs the full executor"]
fn cast_across_global_index_partitions_keeps_join_correctness() {}
