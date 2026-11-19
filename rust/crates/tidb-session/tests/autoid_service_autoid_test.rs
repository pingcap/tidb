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

//! Go `pkg/autoid_service` behind an `AUTO_ID_CACHE 1` table on the mock
//! store: Go's testkit serves the AutoID service in-process (`MockForTest`),
//! so the table's IDs, `SHOW TABLE ... NEXT_ROW_ID` and a rebase are the
//! service's. Expected rows: a Go testkit probe (`zzprobe/autoidsvc`), with
//! integer types printed as tidb-server prints them (`main.go` applies
//! `deprecate-integer-display-length`, which Go's testkit never runs).

use crate::support::testkit::*;

#[test]
fn an_auto_id_cache_1_table_allocates_from_the_autoid_service() {
    let mut tk = TestKit::new();
    tk.must_exec("use test");
    tk.must_exec("create table t (id int key auto_increment, v int) auto_id_cache 1");
    tk.must_exec("insert into t (v) values (1),(2),(3)");
    tk.must_query("select id from t order by id").check(["1", "2", "3"]);
    tk.must_query("show table t next_row_id").check(["test t id 4 AUTO_INCREMENT"]);
    tk.must_exec("alter table t auto_increment = 100");
    tk.must_exec("insert into t (v) values (4)");
    tk.must_query("select max(id) from t").check(["100"]);
    tk.must_query("show table t next_row_id").check(["test t id 101 AUTO_INCREMENT"]);
    tk.must_query("show create table t").check_with_sep(
        "|",
        ["t|CREATE TABLE `t` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  `v` int DEFAULT NULL,\n  PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=101 /*T![auto_id_cache] AUTO_ID_CACHE=1 */"],
    );
    let mut tk2 = tk.new_tk();
    tk2.must_exec("use test");
    tk2.must_exec("insert into t (v) values (5)");
    tk2.must_query("select id from t order by id").check(["1", "2", "3", "100", "101"]);
}
