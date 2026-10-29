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

//! Ports of `pkg/ddl/partition_test.go` (master snapshot). Go drives
//! `ExecutorForTest::DoDDLJobWrapper` jobs directly and inspects job
//! history; the DROP/TRUNCATE contract is pinned here over the SQL surface
//! (`alter table ... drop partition` / `truncate partition`), replacing Go's
//! history check with the partition metadata Go's `tblInfo` carries
//! (`external.GetTableByName`) and the surviving data. The
//! reorganize-rollback and failpoint-interleaved tests need the online-DDL
//! job queue and are ported as documented gaps.

use crate::support::testkit::TestKit;

// The partition definitions as (name, id) pairs, in definition order, read
// from the domain's current schema (Go `external.GetTableByName(...).Meta()`).
fn partition_defs(tk: &TestKit, table: &str) -> Vec<(String, i64)> {
    let tidb_infoschema::TableEntry::Kv(kv) = tk.get_table_by_name("test", table) else {
        panic!("table {table} is not a KV table");
    };
    let partition = kv.partition().expect("table is partitioned");
    partition
        .definitions
        .iter()
        .map(|definition| (definition.name.clone(), definition.id))
        .collect()
}

/// Go `partition_test.go:34::TestDropAndTruncatePartition`. Go builds the
/// 5-partition range table through internal job wrappers, drops `p0`/`p1`,
/// then truncates `p3`/`p4` with freshly generated ids
/// (`partition_test.go:98::testDropPartition`,
/// `partition_test.go:126::testTruncatePartition`). Over SQL: the drops
/// remove the partitions and their rows; the truncate keeps the partition
/// NAMES but replaces their physical ids (Go `genGlobalIDs`)
/// while the untouched partitions keep theirs, and the truncated rows are
/// gone.
#[test]
fn drop_and_truncate_partition_drops_and_replaces_partition_bodies() {
    let mut tk = TestKit::new();
    tk.must_exec(
        "create table t (c int) partition by range(c) (\
         partition p0 values less than (100), partition p1 values less than (200), \
         partition p2 values less than (300), partition p3 values less than (400), \
         partition p4 values less than (500))",
    );
    tk.must_exec("insert into t values (1), (101), (201), (301), (401)");

    // drop p0 and p1
    tk.must_exec("alter table t drop partition p0, p1");
    assert_eq!(
        partition_defs(&tk, "t")
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>(),
        vec!["p2", "p3", "p4"],
        "p0/p1 gone, p2..p4 remain"
    );
    // the dropped partitions' rows are gone
    tk.must_query("select c from t").check(["201", "301", "401"]);

    // truncate p3 and p4: names stay, physical ids are fresh
    let before = partition_defs(&tk, "t");
    tk.must_exec("alter table t truncate partition p3, p4");
    let after = partition_defs(&tk, "t");
    assert_eq!(
        after
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>(),
        vec!["p2", "p3", "p4"],
        "names survive the truncate"
    );
    for (before_def, after_def) in before.iter().zip(after.iter()) {
        assert_eq!(before_def.0, after_def.0, "same partition order");
        if before_def.0 == "p2" {
            assert_eq!(before_def.1, after_def.1, "untouched p2 keeps its id");
        } else {
            assert_ne!(
                before_def.1, after_def.1,
                "truncated {} gets a fresh id",
                before_def.0
            );
        }
    }
    // the truncated partitions' rows are gone
    tk.must_query("select c from t").check(["201"]);
}
