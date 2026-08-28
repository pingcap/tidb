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

//! Port ledger for `pkg/ddl/index_change_test.go` (`pkg/ddl.part6` batch
//! b105, items 331-334 of the pkg/ddl enumeration).
//!
//! All four Go tests observe an index-adding or index-dropping DDL job as it
//! walks the online schema states (DeleteOnly -> WriteOnly -> Public), using
//! the `afterWaitSchemaSynced` failpoint to snapshot the table at each state
//! and check which writes are visible. The online state machine is not
//! transcreated in this tier, so none of the four has a carrier.

/// GO PORT of `pkg/ddl/index_change_test.go:39 TestIndexChange`.
///
/// Re-derived contract (index_change_test.go:39-160 + the check helpers):
/// adding `index c2(c2)` on `t (c1 int primary key, c2 int)` walks
/// DeleteOnly (writes of c2 are NOT indexed), WriteOnly (writes ARE indexed,
/// checked by `checkAddWriteOnlyForAddIndex`), and Public with
/// `job.GetRowCount() == 3` (the backfill covered exactly the 3 rows);
/// dropping the index then walks WriteOnly (`checkDropWriteOnly`) and
/// DeleteOnly (`checkDropDeleteOnly`) back to None.
#[test]
#[ignore = "go-parity-gap: the add/drop-index online schema states and the afterWaitSchemaSynced failpoint seam are not transcreated"]
fn index_change_walks_every_schema_state_with_visible_writes() {}

/// GO PORT of `pkg/ddl/index_change_test.go:394 TestAddIndexRowCountUpdate`.
///
/// Re-derived contract (classic kernels only; nextgen skips): with one reorg
/// worker and fast-reorg/dist-task off, `admin show ddl jobs` reports a
/// growing `ROW_COUNT` for the running add-index job -- the `afterHandleBackfillTask`
/// hook proves a backfill task completed BEFORE the job's row count leaves 0.
#[test]
#[ignore = "go-parity-gap: the reorg backfill progress accounting (job.row_count) and its admin-show surface are not transcreated"]
fn add_index_row_count_updates_as_backfill_tasks_finish() {}

/// GO PORT of `pkg/ddl/index_change_test.go:438
/// TestFastReOrgAlwaysEnabledOnNextGen`.
///
/// Re-derived contract (nextgen kernels only; classic skips):
/// `@@global.tidb_ddl_enable_fast_reorg` reads 1 and
/// `set global tidb_ddl_enable_fast_reorg=0` is refused with "setting
/// tidb_ddl_enable_fast_reorg is not supported in the next generation of
/// TiDB".
#[test]
#[ignore = "go-parity-gap: the nextgen-kernel read-only var ratchet over tidb_ddl_enable_fast_reorg is not transcreated (Rust default build is the classic kernel)"]
fn fast_reorg_is_pinned_on_for_nextgen() {}

/// GO PORT of `pkg/ddl/index_change_test.go:449 TestReadOnlyVarsInNextGen`.
///
/// Re-derived contract (nextgen kernels only; classic skips): setting
/// `tidb_max_dist_task_nodes`, `tidb_ddl_reorg_max_write_speed`, or
/// `tidb_ddl_disk_quota` globally is refused with "<name> is not supported
/// in the next generation of TiDB".
#[test]
#[ignore = "go-parity-gap: the nextgen-kernel read-only ratchets over the dist-task/reorg vars are not transcreated (Rust default build is the classic kernel)"]
fn nextgen_pins_dist_task_and_reorg_vars_read_only() {}
