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

//! Ports of the `pkg/ddl/table_split_test.go` split-policy family (part12
//! items 661-665 of the package's `func Test*`/`func Benchmark*`
//! declarations sorted by file and line), read from `origin/master`.
//!
//! Go stores a `model.TableSplitPolicy` / `model.RegionSplitPolicy` on the
//! table and index meta through `ALTER TABLE ... SPLIT`/`CREATE TABLE ...
//! SPLIT` (Go `pkg/ddl/executor.go:7646` `AlterTableSetRegionSplitPolicy`
//! and `:1053`'s create-table arm, on top of `pkg/ddl/split_region.go`), and
//! the b110 batch already established that the SPLIT execution half (region
//! cache, scatter, pre-split) is not transcreated in this tier. The same
//! holds for the POLICY-STORAGE half: `tidb_model` carries
//! `RegionSplitPolicy` (`rust/crates/tidb-model/src/index.rs:223`, Go
//! `pkg/meta/model/index.go`) but the tier's CREATE/ALTER runners never
//! populate it, so each Go assertion's observable is unavailable here. Each
//! test is recorded as an explicit gap with the contract re-derived from the
//! Go source. Nothing is approximated.

/// Go `TestTableSplitPolicyForPartitionedTable`
/// (`pkg/ddl/table_split_test.go:223`): a range-partitioned CREATE TABLE
/// with `split between (0) and (10000) regions 5` plus
/// `split index idx_val between (0) and (10000) regions 3` stores
/// `TableSplitPolicy{Regions: 5}` on the table meta and
/// `RegionSplitPolicy{Regions: 3}` on `idx_val`; a later
/// `alter table t_part2 split between (0) and (10000) regions 5` stores the
/// same policy on an UNPARTITIONED table created without one; both tables
/// stay green under `admin check table`.
// go-parity-gap: no SPLIT-clause carrier — the AST parses
// `CreateTableSplit`/`AlterTableAction::SplitRegion`
// (`rust/crates/tidb-ast/src/ddl.rs`, `create_split.rs`) but the tier's
// CREATE/ALTER runners do not lower them, and `TableInfo` built here never
// carries `TableSplitPolicy`.
#[test]
#[ignore = "go-parity-gap: split policies are parsed but never stored on the table/index meta"]
fn split_policies_survive_partitioned_create_and_alter() {
    // Contract (pkg/ddl/table_split_test.go:223-268): t_part's meta has
    // TableSplitPolicy{Regions: 5} and idx_val has
    // RegionSplitPolicy{Regions: 3}; t_part2 gains TableSplitPolicy{Regions:
    // 5} from the ALTER; `admin check table` passes on both.
}

/// Go `TestTableSplitPolicyWarning` (`pkg/ddl/table_split_test.go:271`):
/// after `alter table t_warn split index idx_user_id ...` stores the index
/// policy, a LATER `alter table t_warn add index idx_status (status)` emits
/// a `Warning` whose text contains both "region split strategy" and
/// "idx_status" (Go `pkg/ddl/executor.go:7695`: "It is recommended to add a
/// region split strategy to the new index '<name>' to avoid write
/// hotspots").
// go-parity-gap: the split-index ALTER is refused before any policy is
// stored, so the follow-up ADD INDEX has no policy to warn about; the
// statement-context warning bag's Go shape (Level "Warning" entries) is not
// reachable from this tier either.
#[test]
#[ignore = "go-parity-gap: no stored split policy for the ADD INDEX warning to fire on"]
fn add_index_after_a_split_index_warns_about_region_split_strategy() {
    // Contract (pkg/ddl/table_split_test.go:271-306): idx_user_id carries
    // RegionSplitPolicy; ADD INDEX idx_status leaves exactly one Warning
    // naming "region split strategy" and "idx_status"; `admin check table`
    // stays green.
}

/// Go `TestTableSplitPolicyMultipleIndexes`
/// (`pkg/ddl/table_split_test.go:309`): a CREATE TABLE splitting the table
/// (regions 4) and two of its three indexes (`idx_user` regions 3,
/// `idx_status` regions 2) leaves `idx_created` WITHOUT a policy; a later
/// `alter table t_multi split index idx_created ... regions 5` gives only
/// that index one; `admin check table` stays green.
// go-parity-gap: same missing SPLIT-clause lowering — per-index policies are
// never stored, so neither the nil policy for idx_created nor the ALTER'd
// regions-5 policy is observable.
#[test]
#[ignore = "go-parity-gap: per-index split policies are parsed but never stored"]
fn split_policies_track_each_index_independently() {
    // Contract (pkg/ddl/table_split_test.go:309-354): indexPolicies == {
    // idx_user: {3}, idx_status: {2} } and idx_created nil; after the ALTER,
    // idx_created alone has {5}.
}

/// Go `TestTableSplitPolicyShowCreateRoundTrip`
/// (`pkg/ddl/table_split_test.go:357`): `show create table t_src` renders
/// the split clauses inside a `/*T![region_split] ... */` comment; re-executing
/// that text as `CREATE TABLE t_dst ...` reproduces both the table policy
/// (regions 4) and `idx_user_id`'s policy (regions 3) via
/// `TableInfo.FindIndexByName`.
// go-parity-gap: this tier has no SHOW CREATE TABLE renderer and the split
// clauses are never stored on the meta, so neither the comment rendering nor
// the round trip is reproducible.
#[test]
#[ignore = "go-parity-gap: no SHOW CREATE renderer and no stored split policies to round trip"]
fn split_policies_round_trip_through_show_create_text() {
    // Contract (pkg/ddl/table_split_test.go:357-383): the create SQL embeds
    // `/*T![region_split]`; t_dst rebuilds TableSplitPolicy{Regions: 4} and
    // idx_user_id's RegionSplitPolicy{Regions: 3}.
}

/// Go `TestTableSplitPolicyRejectSplitIndexPrimaryOnClustered`
/// (`pkg/ddl/table_split_test.go:386`): on a CLUSTERED table (primary key
/// `id`), both `alter table t split index `PRIMARY` between (0) and
/// (1000000) regions 4` and the same clause inline in CREATE TABLE fail with
/// `ErrForbiddenDDL` (Go `pkg/parser/mysql/errcode.go:1159`, code 8267; Go
/// `pkg/ddl/split_region.go:362` renders "SPLIT PRIMARY is only for
/// non-clustered table" against the clustered handle).
// go-parity-gap: the SPLIT clauses are refused by this tier as unsupported
// ALTER/CREATE actions with a generic 1105, never reaching Go's
// clustered-handle-specific 8267 refusal.
#[test]
#[ignore = "go-parity-gap: split-index-PRIMARY refusal (8267) unreachable; clauses die earlier as unsupported"]
fn split_index_primary_on_a_clustered_table_reports_forbidden_ddl() {
    // Contract (pkg/ddl/table_split_test.go:386-407): both the ALTER and the
    // CREATE TABLE forms fail [ddl:8267] when the table's primary key is
    // clustered.
}
