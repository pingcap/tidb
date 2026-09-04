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

//! Port of `pkg/ddl/tests/partition/global_index_version_test.go`
//! (`:30::TestGlobalIndexVersion0`, `:169::TestGlobalIndexVersion1`,
//! `:266::TestGlobalIndexVersionConstants`,
//! `:276::TestGlobalIndexTruncateAndDropPartition`,
//! `:460::TestUpdateIndexesResetsGlobalIndexVersion`).
//!
//! Go pins the `GlobalIndexVersion` key-format lifecycle of GLOBAL indexes:
//! which version a new global index gets (LEGACY 0 under the
//! `SetGlobalIndexVersion` failpoint, V1 default, V2 for unique nullable
//! columns), that local indexes stay at 0, that `UPDATE INDEXES` global↔local
//! flips reset the version, and that truncate/drop/reorganize clean only the
//! affected partition's global entries. The version constants themselves
//! (`pkg/meta/model/index.go:54-:68`: LEGACY=0, V1=1, V2=2) are transcreated
//! in `tidb-model`; the end-to-end GLOBAL-index lifecycle remains deferred
//! because a GLOBAL index cannot be created at all on this tier.

use tidb_executor::{run_create_table_on, Catalog};

/// Go `global_index_version_test.go:30::TestGlobalIndexVersion0`: with the
/// `github.com/pingcap/tidb/pkg/ddl/SetGlobalIndexVersion` failpoint forced
/// to 0 (`:63`), `create index idx_b on tp(b) global` builds a LEGACY (0)
/// global index whose entries keep duplicate `_tidb_rowid`s after the
/// EXCHANGE at `:51` — visible as `admin check` answering 8223 data
/// inconsistency (`:70`) and updates answering TiKV 8141 assertion failures
/// (`:72-:73`). Metadata checks at `:85-:110`: local `idx_a` is
/// `Global=false, version 0`; unique-nullable `idx_ab GLOBAL` is V1; a
/// clustered table's global index is LEGACY (`:129-:139`).
// go-parity-gap: no failpoints; `CREATE INDEX ... GLOBAL` answers 1105
// "this index kind is not supported yet" (measured); `admin check` has no
// 8223 global-index inconsistency mode and TiKV's 8141 assertion layer is
// absent; EXCHANGE PARTITION is 1105.
#[test]
#[ignore]
fn global_index_version_legacy_zero_under_failpoint() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE tp (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN (200))",
        &mut catalog,
    )
    .unwrap();
    // Go :49-:110 exchanges partitions, builds the forced-version-0 global
    // index and asserts the corrupted-duplicate behavior.
}

/// Go `global_index_version_test.go:169::TestGlobalIndexVersion1`: the same
/// exchange scenario with the DEFAULT version — `create index idx_b on
/// tp(b) global` now gets V1, `use index(idx_b)` and `ignore index(idx_b)`
/// both count 14 rows (`:185-:187`), admin check stays clean (`:189`), the
/// concurrent updates read consistent pairs through either index
/// (`:191-:201`), and the metadata checks at `:229-:256` pin local=0/global=V1.
// go-parity-gap: `CREATE INDEX ... GLOBAL` unported (1105, measured), the
// `_tidb_rowid` projection Go sorts on is unselectable, and the V1 default
// decision (Go `pkg/ddl/index.go`, buildIndexInfo) is not transcreated —
// this tier writes version 0 only.
#[test]
#[ignore]
fn global_index_version_v1_is_the_default_for_new_global_indexes() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE tp (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN (200))",
        &mut catalog,
    )
    .unwrap();
    // Go :181-:256 is the V1 body.
}

/// Go `global_index_version_test.go:266::TestGlobalIndexVersionConstants`:
/// `model.GlobalIndexVersionLegacy == 0`, `GlobalIndexVersionV1 == 1`,
/// `GlobalIndexVersionV2 == 2` (`pkg/meta/model/index.go:54-:68`).
#[test]
fn global_index_version_constants_are_numbered_legacy_v1_v2() {
    assert_eq!(tidb_model::index::GLOBAL_INDEX_VERSION_LEGACY, 0);
    assert_eq!(tidb_model::index::GLOBAL_INDEX_VERSION_V1, 1);
    assert_eq!(tidb_model::index::GLOBAL_INDEX_VERSION_V2, 2);
}

/// Go `global_index_version_test.go:276::TestGlobalIndexTruncateAndDropPartition`:
/// after `CREATE INDEX ... GLOBAL` on a 3-partition range table, TRUNCATE
/// PARTITION / DROP PARTITION / REORGANIZE PARTITION must leave the OTHER
/// partitions' global entries readable through `use index(idx_b)` with
/// exact counts (`:311-:417`), survive duplicate `_tidb_rowid`s produced by
/// an exchange (`:383-:408`), and honor the V2 unique-nullable global index
/// allowing NULLs across partitions (`:420-:467`).
// go-parity-gap: the whole contract is keyed on a GLOBAL index carrier
// (1105 here, measured); REORGANIZE PARTITION is likewise 1105; the V2
// unique-nullable default is unported.
#[test]
#[ignore]
fn global_index_entries_survive_truncate_drop_reorganize_of_other_partitions() {
    let mut catalog = Catalog::default();
    run_create_table_on(
        "CREATE TABLE tp_trunc (a INT, b INT, PRIMARY KEY (a) NONCLUSTERED) \
         PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (100), \
         PARTITION p1 VALUES LESS THAN (200), PARTITION p2 VALUES LESS THAN (300))",
        &mut catalog,
    )
    .unwrap();
    // Go :299-:467 is the truncate/drop/exchange/reorganize/null battery.
}

/// Go `global_index_version_test.go:460::TestUpdateIndexesResetsGlobalIndexVersion`
/// (at line 460 the test body starts; the fixture `create table t_upd_idx`
/// uses `alter table t partition by ... update indexes (idx_b global)`):
/// switching an index GLOBAL→LOCAL must reset its stored version to 0
/// (`buildTablePartitionInfo`), else the local reader mis-decodes a
/// PartitionHandle and DML errors.
// go-parity-gap: `ALTER TABLE ... PARTITION BY` with `UPDATE INDEXES` is
// unsupported 1105 (measured), and the tier's stored
// `global_index_version` has no writer to reset; the fixture cannot even
// build the partitioned-with-global-index starting state.
#[test]
#[ignore]
fn update_indexes_flip_to_local_resets_the_global_index_version() {
    let mut catalog = Catalog::default();
    run_create_table_on("create table t_upd_idx (a int, b int, index idx_b(b))", &mut catalog)
        .unwrap();
    // Go :466-:500 re-partitions with `update indexes (idx_b global)` and
    // asserts the stored version flips 0→V1→0 across the flips.
}
