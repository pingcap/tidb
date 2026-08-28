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

//! Port ledger for `pkg/ddl/index_modify_test.go` (`pkg/ddl.part6` batch
//! b105, items 336-360 of the pkg/ddl enumeration).
//!
//! The file's 25 Go tests are thin wrappers over two shared helpers, and the
//! wrappers' identities ARE their configurations:
//!
//! * `testAddIndex` (index_modify_test.go:170-337) creates the table, batch
//!   inserts `defaultBatchSize` rows from -10 plus ~100 discrete row groups
//!   and a MaxInt64-adjacent row, runs `alter table test_add_index add
//!   [primary|unique] key c3_index(c3)` FROM ANOTHER SESSION while the main
//!   session keeps deleting and inserting, then pins: `select c1 from
//!   test_add_index where c3 >= -10 order by c1` returns exactly the
//!   surviving keys (index and table agree through the backfill), and
//!   `admin check table` is clean. The `testShardRowID` variants pre-split
//!   regions and instead assert >= 16 regions in
//!   `show table test_add_index regions`; the `testPartition` variants stop
//!   at that select; the plain variants additionally verify the new index's
//!   meta (name, allocated id > 0) on the reloaded table.
//! * `testAddIndexRollback` (index_modify_test.go:507-573) plants duplicate
//!   or NULL c3 values, runs the add from another session, pins the exact
//!   error string, verifies the index is NOT on the reloaded table, then --
//!   after the bad rows are removed -- the same statement succeeds.
//!
//! Every one of them executes through the DDL reorg/backfill machinery
//! (online states, background session execution, admin checks), which is not
//! transcreated in this tier; hence each is a documentary gap port.

/// GO PORT of `pkg/ddl/index_modify_test.go:63 TestAddPrimaryKey1`:
/// `testAddIndex(testPlain, "create table test_add_index (c1 bigint, c2
/// bigint, c3 bigint, unique key(c1))", "primary")` -- a PRIMARY KEY added
/// over a plain (non-partitioned) table.
#[test]
#[ignore = "go-parity-gap: the testAddIndex backfill harness (concurrent session + online states + admin check) is not transcreated"]
fn add_primary_key_plain_table_backfills_all_rows() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:67 TestAddPrimaryKey2`:
/// `testAddIndex(testPartition, ... partition by range (c3) (p0..p4,
/// maxvalue), "primary")` -- the primary-key backfill over a RANGE
/// partitioned table.
#[test]
#[ignore = "go-parity-gap: the testAddIndex backfill harness over partitioned tables is not transcreated"]
fn add_primary_key_range_partitioned_backfills_all_rows() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:78 TestAddPrimaryKey3`:
/// `testAddIndex(testPartition, ... partition by hash (c3) partitions 4,
/// "primary")` -- the primary-key backfill over a HASH partitioned table.
#[test]
#[ignore = "go-parity-gap: the testAddIndex backfill harness over partitioned tables is not transcreated"]
fn add_primary_key_hash_partitioned_backfills_all_rows() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:84 TestAddPrimaryKey4`:
/// `testAddIndex(testPartition, ... partition by range columns (c3)
/// (p0..p4, maxvalue), "primary")` -- RANGE COLUMNS variant.
#[test]
#[ignore = "go-parity-gap: the testAddIndex backfill harness over partitioned tables is not transcreated"]
fn add_primary_key_range_columns_partitioned_backfills_all_rows() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:95 TestAddIndex1`:
/// `testAddIndex(testPlain, "... primary key(c1)", "")` -- a plain secondary
/// index backfill, with the full meta verification tail.
#[test]
#[ignore = "go-parity-gap: the testAddIndex backfill harness is not transcreated"]
fn add_index_plain_table_backfills_all_rows() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:100
/// TestAddIndex1WithShardRowID`: `testAddIndex(testPartition|testShardRowID,
/// "... ) SHARD_ROW_ID_BITS = 4 pre_split_regions = 4", "")` -- the
/// region-count assertion replaces the select tail.
#[test]
#[ignore = "go-parity-gap: SHARD_ROW_ID_BITS pre-split regions and the backfill harness are not transcreated"]
fn add_index_with_shard_row_id_sees_presplit_regions() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:105 TestAddIndex2`:
/// `testAddIndex(testPartition, ... primary key(c1) partition by range (c1)
/// ..., "")`.
#[test]
#[ignore = "go-parity-gap: the testAddIndex backfill harness over partitioned tables is not transcreated"]
fn add_index_range_partitioned_backfills_all_rows() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:116 TestAddIndex2WithShardRowID`:
/// shard-row-id create + RANGE partitioning.
#[test]
#[ignore = "go-parity-gap: SHARD_ROW_ID_BITS pre-split regions and the backfill harness are not transcreated"]
fn add_index_shard_row_id_range_partitioned_sees_presplit_regions() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:128 TestAddIndex3`:
/// `testAddIndex(testPartition, ... primary key(c1) partition by hash (c1)
/// partitions 4, "")`.
#[test]
#[ignore = "go-parity-gap: the testAddIndex backfill harness over partitioned tables is not transcreated"]
fn add_index_hash_partitioned_backfills_all_rows() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:134 TestAddIndex3WithShardRowID`:
/// shard-row-id create + HASH partitioning.
#[test]
#[ignore = "go-parity-gap: SHARD_ROW_ID_BITS pre-split regions and the backfill harness are not transcreated"]
fn add_index_shard_row_id_hash_partitioned_sees_presplit_regions() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:141 TestAddIndex4`:
/// `testAddIndex(testPartition, ... primary key(c1) partition by range
/// columns (c1) ..., "")`.
#[test]
#[ignore = "go-parity-gap: the testAddIndex backfill harness over partitioned tables is not transcreated"]
fn add_index_range_columns_partitioned_backfills_all_rows() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:152 TestAddIndex4WithShardRowID`:
/// shard-row-id create + RANGE COLUMNS partitioning.
#[test]
#[ignore = "go-parity-gap: SHARD_ROW_ID_BITS pre-split regions and the backfill harness are not transcreated"]
fn add_index_shard_row_id_range_columns_partitioned_sees_presplit_regions() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:164 TestAddIndex5`:
/// `testAddIndex(testClusteredIndex, "... primary key(c2, c3)", "")` --
/// clustered common-handle table, session var forced to
/// ClusteredIndexDefModeOn.
#[test]
#[ignore = "go-parity-gap: the testAddIndex backfill harness over a common-handle table is not transcreated"]
fn add_index_clustered_common_handle_backfills_all_rows() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:340
/// TestAddIndexForGeneratedColumn`: a generated `y1 year as (y + 2)` column
/// (and the issue-9311 `d1 date as (DATE_SUB(d, INTERVAL 31 DAY))` twin,
/// including a generated column over the PKIsHandle column) carries its
/// computed value through `ADD INDEX`/`DROP INDEX`, and the index reads
/// return the same values as a table scan.
#[test]
#[ignore = "go-parity-gap: generated-column index backfill and the use-index read path it verifies are not transcreated"]
fn add_index_for_generated_column_matches_table_scan() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:379 TestAnalyzeStuck`: with
/// `tidb_stats_update_during_ddl = 1` and the analyze step stalled by a
/// failpoint, `ADD INDEX` and `MODIFY COLUMN` still finish (the
/// `DefaultCumulativeTimeout` breaks the stall) and `show stats_meta`
/// eventually reports the table.
#[test]
#[ignore = "go-parity-gap: the DDL-triggered analyze step and its cumulative timeout are not transcreated"]
fn analyze_stuck_does_not_block_the_ddl_past_its_timeout() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:448
/// TestAnalyzeOwnerResignNoReRun`: a forced write-conflict on
/// `mysql.tidb_ddl_job` (processing 0->1) makes the owner resign, and the
/// `beforeAnalyzeTable` hook must fire EXACTLY once across the
/// re-election -- analyze is not re-run for an already-analyzed job.
#[test]
#[ignore = "go-parity-gap: the owner-resign/re-election path over mysql.tidb_ddl_job is not transcreated"]
fn analyze_is_not_rerun_when_the_owner_resigns() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:485 TestAddPrimaryKeyRollback1`:
/// duplicate c3 values make `alter table t1 add primary key c3_index (c3)`
/// fail with `[kv:1062]Duplicate entry '16374' for key 't1.PRIMARY'`
/// (defaultBatchSize*2-10), the index is absent afterwards, and after the
/// duplicates are removed the statement succeeds.
#[test]
#[ignore = "go-parity-gap: the rollback arm of the add-index backfill (and its concurrent harness) is not transcreated"]
fn add_primary_key_rolls_back_on_duplicate_entries() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:493 TestAddPrimaryKeyRollback2`:
/// NULL c3 values make the same statement fail with
/// `[ddl:1138]Invalid use of NULL value`, with the same
/// absent-index + retry-success tail.
#[test]
#[ignore = "go-parity-gap: the rollback arm of the add-index backfill (and its concurrent harness) is not transcreated"]
fn add_primary_key_rolls_back_on_null_values() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:500 TestAddUniqueIndexRollback`:
/// `create unique index c3_index on t1 (c3)` over duplicates fails with
/// `[kv:1062]Duplicate entry '16374' for key 't1.c3_index'`, same tail.
#[test]
#[ignore = "go-parity-gap: the rollback arm of the add-index backfill (and its concurrent harness) is not transcreated"]
fn add_unique_index_rolls_back_on_duplicate_entries() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:575 TestAddIndexWithSplitTable`:
/// over an AUTO_RANDOM(4) table, `SPLIT TABLE ... BETWEEN (MinInt64) AND
/// (MaxInt64) REGIONS 16` reports `15 1`, `tidb_wait_split_region_finish`
/// reads 1, and the concurrent ADD INDEX still converges (`admin check
/// table` clean).
#[test]
#[ignore = "go-parity-gap: SPLIT TABLE region arithmetic and the backfill harness are not transcreated"]
fn add_index_with_split_table_converges_over_autorandom_regions() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:581 TestAddIndexWithShardRowID`:
/// the shard-row-id create of the split-table pair (10 concurrent batch
/// inserters over pre-split regions, then a concurrent ADD INDEX with
/// interleaved writes).
#[test]
#[ignore = "go-parity-gap: SHARD_ROW_ID_BITS pre-split regions and the backfill harness are not transcreated"]
fn add_index_over_shard_row_id_regions_converges() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:684 TestAddAnonymousIndex`:
/// `add index (c1, c2)` names the index after its FIRST column (`c1`);
/// `drop index` with no name is a syntax error; a duplicate explicit name is
/// refused and leaves the count unchanged; anonymous duplicates auto-name
/// `c1_2`..`c1_4`; the name match is case-insensitive (`C3`/`c3`); a column
/// literally named `primary` makes anonymous names `primary_2`, `primary_3`,
/// while `add primary key(b)` takes the bare `primary` name; pre-named
/// `primary_2` indexes shift later anonymous names on (`t_primary_2`,
/// `t_primary_3`).
#[test]
#[ignore = "go-parity-gap: the anonymous-index naming ladder runs through the DDL path this tier refuses (online ADD INDEX) and needs the reload-and-inspect harness"]
fn add_anonymous_index_names_follow_go_ladder() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:744 TestAddIndexWithPK`: in BOTH
/// clustered-index modes (IntOnly and On), `add index idx(a)` and
/// `add index idx1(a, b)` over `primary key(a)` tables -- integer handle,
/// and handle-bearing `c`/`c unsigned` variants -- leave reads identical to
/// the pre-index state, and `create index idx on t (a, b)` over a
/// `primary key(a, b)` table is accepted.
#[test]
#[ignore = "go-parity-gap: the clustered-mode index adds run through the backfill harness, which is not transcreated"]
fn add_index_with_pk_reads_match_across_clustered_modes() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:795 TestAddGlobalIndex`: over a
/// RANGE partitioned table, `add unique index p_a (a) global` sets
/// `indexInfo.Global`, rows land with the partition id in the global index
/// key per `checkGlobalIndexRow` (row 1 under p0, row 2 -- whose b=11
/// belongs to p1 -- under p1), and a global PRIMARY KEY index variant is
/// covered the same way.
#[test]
#[ignore = "go-parity-gap: the global-index key encoding path (partition id in the index key) and its ADD INDEX backfill are not transcreated"]
fn add_global_index_keeps_partition_id_in_the_key() {}

/// GO PORT of `pkg/ddl/index_modify_test.go:983 TestDropIndexes` (with its
/// `testDropIndexes`/`testDropIndexesIfExists`/`testDropIndexesFromPartitionedTable`
/// helpers): dropping `drop index i1, drop index i2` /
/// `drop primary key, drop index i1` / the three-index variant while rows
/// keep changing removes exactly the named indexes from the meta; a missing
/// name in a multi-drop is `[ddl:1091]index i3 doesn't exist` unless
/// `IF EXISTS` demotes it to a Note; and the partitioned-table variant drops
/// from every partition.
#[test]
#[ignore = "go-parity-gap: the concurrent multi-index drop runs through the job queue and the online states, which are not transcreated"]
fn drop_indexes_removes_exactly_the_named_ones() {}
