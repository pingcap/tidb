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

//! The `ALTER TABLE` arms of Go `TestTableSampleBasic`
//! (`pkg/executor/sample_test.go:43-56`): `TABLESAMPLE REGIONS()` over a
//! table whose columns were added after its rows.

use crate::support::testkit::TestKit;

/// Go `createSampleTestkit` (`pkg/executor/sample_test.go:33`).
fn create_sample_testkit() -> TestKit {
    let mut tk = TestKit::new();
    tk.must_exec("drop database if exists test_table_sample");
    tk.must_exec("create database test_table_sample");
    tk.must_exec("use test_table_sample");
    tk.must_exec("set @@session.tidb_scatter_region='table'");
    tk
}

#[test]
fn table_sample_reads_columns_added_after_the_rows() {
    let mut tk = create_sample_testkit();
    tk.must_exec("create table t (a int)");
    tk.must_query("select * from t tablesample regions()").check_empty();

    tk.must_exec("insert into t values (0), (1000), (2000)");
    tk.must_query("select * from t tablesample regions()").check(["0"]);
    tk.must_exec("alter table t add column b varchar(255) not null default 'abc'");
    tk.must_query("select b from t tablesample regions()").check(["abc"]);
    tk.must_exec("alter table t add column c int as (a + 1)");
    tk.must_query("select c from t tablesample regions()").check(["1"]);
    tk.must_query("select c, _tidb_rowid from t tablesample regions()").check(["1 1"]);
    tk.must_have_plan("select * from t tablesample regions()", "TableSample");
}
