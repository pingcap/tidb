//! Go `TableReaderExecutor` over a TiKV store (`pkg/executor/table_reader.go`).

use crate::support::testkit::TestKit;

/// Go `tables.SetPBColumnsDefaultValue` -> `getColDefaultValue`: a row
/// written before a TIMESTAMP column was added reads the column's stored
/// default (UTC) in the session's zone, as the index built from it holds
/// (`tests/integrationtest/t/executor/admin.test`'s `t1`).
#[test]
fn an_added_timestamp_columns_default_reads_in_the_session_zone() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@time_zone = '+08:00'");
    tk.must_exec("create table t1 (c2 year, primary key (c2))");
    tk.must_exec("insert into t1 set c2 = '1912'");
    tk.must_exec("alter table t1 add column c3 timestamp null default '1976-08-29 16:28:11'");
    tk.must_query("select c3 from t1").check(["1976-08-29 16:28:11"]);
    tk.must_exec("alter table t1 add index idx1 (c2, c3)");
    tk.must_exec("admin check table t1");
    tk.must_exec("set @@time_zone = '+00:00'");
    tk.must_query("select c3 from t1").check(["1976-08-29 08:28:11"]);
}

/// Go `IndexReaderExecutor` over a prefix index of a clustered table
/// (`tests/integrationtest/t/session/clustered_index.test`'s
/// `TestClusteredPrefixColumn`): the prefixed column reads in full, from the
/// common handle the index entry carries.
#[test]
fn a_prefix_index_reads_a_clustered_columns_full_value() {
    let mut tk = TestKit::new();
    tk.must_exec("set @@tidb_enable_clustered_index = 'on'");
    tk.must_exec("create table t1(c1 varchar(100), c2 varchar(100), c3 varchar(100), primary key (c1,c2), key idx1 (c2(1)))");
    tk.must_exec("insert into t1 select 'a', 'cd', 'ef'");
    tk.must_query("select c2 from t1 use index(idx1)").check(["cd"]);
    tk.must_query("select count(1) from t1 use index(idx1) where c2 = 'cd'").check(["1"]);
    tk.must_exec("create table t2(c1 varchar(100), c2 varchar(100), c3 varchar(100), primary key (c1,c2(1)), key idx1 (c1,c2))");
    tk.must_exec("insert into t2 select 'a', 'cd', 'ef'");
    tk.must_query("select c2 from t2 use index(idx1)").check(["cd"]);
    tk.must_query("select count(1) from t2 use index(idx1) where c2 = 'cd'").check(["1"]);
}
