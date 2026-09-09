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

//! Ports of Go `pkg/ddl/table_split_test.go` (master): `TestTableSplit`
//! (`:38`), `TestScatterRegion` (`:95`) and `TestTableSplitPolicy` (`:169`).
//! Go exercises region pre-split/scatter through the TiKV region cache and
//! the `tidb_scatter_region` session/global variable; the split/scatter
//! execution is not transcreated in this tier (the VARIABLE definitions
//! exist in `tidb-session`'s sysvar catalog -- `tidb_scatter_region` with
//! possible values "", "table", "global" at
//! `rust/crates/tidb-session/src/sysvar/catalog/distsql_storage.rs:447` --
//! but no validator or DDL wiring is reachable here), so each test is
//! recorded as an explicit gap with the contract re-derived from the Go
//! source. Nothing is approximated.

/// Go `TestTableSplit` (`pkg/ddl/table_split_test.go:38`): with
/// `ddl.EnableSplitTableRegion` set and `tidb_scatter_region = 'table'`
/// (session-scoped for the first create, then global for a NEW session), a
/// range-partitioned table's every partition gets a region whose START KEY
/// is exactly `tablecodec.EncodeTablePrefix(partitionID)` (checked through
/// the region cache with one invalidate-and-reload), and `mysql.tidb` gets
/// its own table-prefixed region.
// go-parity-gap: no region splitter/scatter carrier and no region cache;
// EnableSplitTableRegion and the mockstore split hooks are not transcreated.
#[test]
#[ignore]
fn create_table_with_scatter_region_pre_splits_every_partition() {
}

/// Go `TestScatterRegion` (`pkg/ddl/table_split_test.go:95`): the
/// `tidb_scatter_region` variable round-trips '' / 'table' / 'global'
/// case-insensitively (`TABLE` reads back `table`), the session value
/// defaults to the global one only for sessions created AFTER a
/// `set global` (existing sessions keep ''), and invalid values ('test',
/// 'te st', '1', 0) fail with `invalid value for '<v>', it should be either
/// '', 'table' or 'global'`.
// go-parity-gap: the sysvar DEFINITION is carried in tidb-session
// (`distsql_storage.rs:447`, possible values "", "table", "global") but the
// case-normalizing validator, the global-scope session inheritance and the
// exact error message are not reachable from this tier.
#[test]
#[ignore]
fn scatter_region_variable_validates_and_inherits_globally() {
}

/// Go `TestTableSplitPolicy` (`pkg/ddl/table_split_test.go:169`): `alter
/// table t1 split between (0) and (1000000) regions 4` stores a
/// `TableSplitPolicy{Regions: 4, Lower: ["0"], Upper: ["1000000"]}` on the
/// table meta; `alter table t1 split index idx_name between ('a') and ('z')
/// regions 3` stores `RegionSplitPolicy{Regions: 3}` on the index; a CREATE
/// TABLE with inline `split between ... / split index ...` clauses carries
/// both policies; and `admin check table t1` stays green.
// go-parity-gap: no SPLIT PARTITION/index DDL carrier -- the alter specs are
// refused as unsupported by the DDL dispatch and TableInfo carries no
// split-policy field in this tier.
#[test]
#[ignore]
fn split_policies_are_stored_on_table_and_index_meta() {
}
