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

//! Documented go-parity-gap ports of the `pkg/ddl/partition_test.go` tests
//! whose contract is the online-DDL job queue: reorganize-partition
//! rollback through admin-cancel, and DML interleaved with an in-progress
//! ADD COLUMN. The drop/truncate contract of the same Go file is ported
//! running in `tests_ddl_partition_operations_sql`. REORGANIZE and EXCHANGE
//! partition actions are not implemented in this tier either
//! (`alter_table.rs` falls to its unsupported-action arm), which the per-
//! test notes call out.

/// Go `partition_test.go:153::TestReorganizePartitionRollback` (issue
/// 42448). Cancelling a `reorganize partition p0..p4 into (pnew)` while it
/// is in `StateWriteReorganization` leaves the job in `rollback done`, the
/// table meta WITHOUT `AddingDefinitions`/`DroppingDefinitions`,
/// `show create table` byte-identical to before, and a follow-up
/// `alter table t1 add index idx_kc (k, c)` succeeding.
// go-parity-gap: REORGANIZE PARTITION and admin-cancel rollback are
// online-DDL job machinery this tier does not build.
#[test]
#[ignore]
fn reorganize_partition_cancel_rolls_back_to_original_layout() {
}

/// Go `partition_test.go:252::TestUpdateDuringAddColumn`. While the
/// hash-partitioned `alter table t1 add column c3 bigint default 9` is in
/// `StateWriteOnly`, another session's `update t1, t2 set t1.c1 = 8,
/// t2.c2 = 10 where t1.c2 = t2.c1` succeeds and reads `8 1`/`8 2` and
/// `1 10`/`2 10`; after the ALTER, rows read `8 1 9`/`8 2 9`.
// go-parity-gap: the afterWaitSchemaSynced interleaving needs the online-DDL
// job queue.
#[test]
#[ignore]
fn update_during_partitioned_add_column_reads_write_only_state() {
}

/// Go `partition_test.go:276::TestExchangePartitionMultiColumn`. `alter
/// table t exchange partition p10 with table t_np` over a
/// `partition by range columns(a1, a2)` table and a matching non-partitioned
/// table with identical column types and primary key succeeds.
// go-parity-gap: EXCHANGE PARTITION is not implemented in this tier
// (`alter_table.rs` unsupported-action arm).
#[test]
#[ignore]
fn exchange_partition_accepts_matching_multi_column_range_tables() {
}
