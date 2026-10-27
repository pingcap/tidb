// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, 2.0 (the "License");
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

//! Final-state checks ported from Go `pkg/ddl/db_change_test.go`. Online
//! schema-state and parallel-job behavior (the `afterWaitSchemaSynced`
//! failpoint callbacks) is not represented here.

use crate::support::testkit::TestKit;

/// `db_change_test.go::TestAlterIndexVisibility` -- regression test for
/// issue 70049.
///
/// `ALTER TABLE .. ALTER INDEX idx_k INVISIBLE` flips ONLY the exact name,
/// leaving the underscore-suffixed siblings `idx_k_1`/`idx_k_copy` alone --
/// Go's `setIndexVisibility` (`pkg/ddl/index.go`) matches
/// `idx.Name.L == name.L` (plus a changing/temp-index leg keyed on
/// `GetChangingOriginName`, which is what the suffix names must not be
/// mistaken for). The second fixture flips `idx_k` back VISIBLE among three
/// create-time-invisible indexes.
#[test]
fn alter_index_visibility_matches_the_exact_index_not_its_suffix_siblings() {
    let mut tk = TestKit::new();
    let query = |table: &str| {
        format!(
            "select key_name, is_visible from information_schema.tidb_indexes \
             where table_schema = 'test' and table_name = '{table}' order by key_name"
        )
    };

    tk.must_exec("create table t_invisible (k int, key idx_k(k), key idx_k_1(k), key idx_k_copy(k))");
    tk.must_exec("alter table t_invisible alter index idx_k invisible");
    tk.must_query(&query("t_invisible")).check(["idx_k NO", "idx_k_1 YES", "idx_k_copy YES"]);

    tk.must_exec(
        "create table t_visible (k int, key idx_k(k) invisible, key idx_k_1(k) invisible, \
         key idx_k_copy(k) invisible)",
    );
    tk.must_exec("alter table t_visible alter index idx_k visible");
    tk.must_query(&query("t_visible")).check(["idx_k YES", "idx_k_1 NO", "idx_k_copy NO"]);
}

/// `db_change_test.go::TestShowIndex`, final-state halves.
///
/// SHOW INDEX / `information_schema.tidb_indexes` rows across four
/// primary-key storage variants and one partitioned table:
///
/// - nonclustered int PK: PRIMARY is a real index entry, `Clustered NO`.
/// - clustered int PK (`PKIsHandle`): Go's `TableInfo.Indices` holds no
///   PRIMARY; the readers SYNTHESIZE its row first, `Clustered YES`.
/// - clustered char(100) PK: a COMMON handle, a real PRIMARY entry rendered
///   `Clustered YES`, after the table-constraint `vv`.
/// - nonclustered char(100) PK: a real PRIMARY entry again, rowid handle.
/// - the range-partitioned `tr`: `create index idx1` adds exactly one entry.
#[test]
fn show_index_entries_follow_the_primary_key_storage() {
    let mut tk = TestKit::new();
    tk.must_exec("create database test_db_state default charset utf8 default collate utf8_bin");
    tk.must_exec("use test_db_state");
    tk.must_exec("create table t(c1 int primary key nonclustered, c2 int)");
    tk.must_exec("alter table t add index c2(c2)");
    tk.must_query("show index from t").check([
        "t 0 PRIMARY 1 c1 A 0 <nil> <nil>  BTREE   YES <nil> NO NO",
        "t 1 c2 1 c2 A 0 <nil> <nil> YES BTREE   YES <nil> NO NO",
    ]);

    tk.must_exec(
        "create table tr(id int, name varchar(50), purchased date) \
         partition by range( year(purchased) ) (partition p0 values less than (1990), \
         partition p1 values less than (1995), partition p2 values less than (2000), \
         partition p3 values less than (2005), partition p4 values less than (2010), \
         partition p5 values less than (2015))",
    );
    tk.must_exec("create index idx1 on tr (purchased)");
    tk.must_query("show index from tr")
        .check(["tr 1 idx1 1 purchased A 0 <nil> <nil> YES BTREE   YES <nil> NO NO"]);

    let clustered =
        "select key_name, clustered from information_schema.tidb_indexes where table_name = 'tr' order by key_name";

    tk.must_exec("drop table if exists tr");
    tk.must_exec("create table tr(id int primary key clustered, v int, key vv(v))");
    tk.must_query("show index from tr").check([
        "tr 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES NO",
        "tr 1 vv 1 v A 0 <nil> <nil> YES BTREE   YES <nil> NO NO",
    ]);
    tk.must_query(clustered).check(["PRIMARY YES", "vv NO"]);

    tk.must_exec("drop table if exists tr");
    tk.must_exec("create table tr(id int primary key nonclustered, v int, key vv(v))");
    tk.must_query("show index from tr").check([
        "tr 1 vv 1 v A 0 <nil> <nil> YES BTREE   YES <nil> NO NO",
        "tr 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO NO",
    ]);
    tk.must_query(clustered).check(["PRIMARY NO", "vv NO"]);

    tk.must_exec("drop table if exists tr");
    tk.must_exec("create table tr(id char(100) primary key clustered, v int, key vv(v))");
    tk.must_query("show index from tr").check([
        "tr 1 vv 1 v A 0 <nil> <nil> YES BTREE   YES <nil> NO NO",
        "tr 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES NO",
    ]);
    tk.must_query(clustered).check(["PRIMARY YES", "vv NO"]);

    tk.must_exec("drop table if exists tr");
    tk.must_exec("create table tr(id char(100) primary key nonclustered, v int, key vv(v))");
    tk.must_query("show index from tr").check([
        "tr 1 vv 1 v A 0 <nil> <nil> YES BTREE   YES <nil> NO NO",
        "tr 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO NO",
    ]);
    tk.must_query(clustered).check(["PRIMARY NO", "vv NO"]);
}
