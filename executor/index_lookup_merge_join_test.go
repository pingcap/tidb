// Copyright 2021 PingCAP, Inc.
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

package executor_test

import (
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testdata"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/stretchr/testify/require"
)

func TestIndexLookupMergeJoinHang(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/IndexMergeJoinMockOOM", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/IndexMergeJoinMockOOM"))
	}()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int,b int,index idx(a))")
	tk.MustExec("create table t2 (a int,b int,index idx(a))")
	tk.MustExec("insert into t1 values (1,1),(2,2),(3,3),(2000,2000)")
	tk.MustExec("insert into t2 values (1,1),(2,2),(3,3),(2000,2000)")
	// Do not hang in index merge join when OOM occurs.
	err := tk.QueryToErr("select /*+ INL_MERGE_JOIN(t1, t2) */ * from t1, t2 where t1.a = t2.a")
	require.Error(t, err)
	require.Equal(t, "OOM test index merge join doesn't hang here.", err.Error())
}

func TestIssue28052(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (" +
		"`col_tinyint_key_signed` tinyint(4) DEFAULT NULL," +
		"`col_year_key_signed` year(4) DEFAULT NULL," +
		"KEY `col_tinyint_key_signed` (`col_tinyint_key_signed`)," +
		"KEY `col_year_key_signed` (`col_year_key_signed`)" +
		" ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

	tk.MustExec("insert into t values(-100,NULL);")
	tk.MustQuery("select /*+ inl_merge_join(t1, t2) */ count(*) from t t1 right join t t2 on t1. `col_year_key_signed` = t2. `col_tinyint_key_signed`").Check(testkit.Rows("1"))
}

func TestIssue18068(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/testIssue18068", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/testIssue18068"))
	}()
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, s")
	tk.MustExec("create table t (a int, index idx(a))")
	tk.MustExec("create table s (a int, index idx(a))")
	tk.MustExec("insert into t values(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1);")
	tk.MustExec("insert into s values(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1),(1);")
	tk.MustExec("set @@tidb_index_join_batch_size=1")
	tk.MustExec("set @@tidb_max_chunk_size=32")
	tk.MustExec("set @@tidb_init_chunk_size=1")
	tk.MustExec("set @@tidb_index_lookup_join_concurrency=2")

	tk.MustExec("select  /*+ inl_merge_join(s)*/ 1 from t join s on t.a = s.a limit 1")
	// Do not hang in index merge join when the second and third execute.
	tk.MustExec("select  /*+ inl_merge_join(s)*/ 1 from t join s on t.a = s.a limit 1")
	tk.MustExec("select  /*+ inl_merge_join(s)*/ 1 from t join s on t.a = s.a limit 1")
}

func TestIssue18631(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int, c int, d int, primary key(a,b,c))")
	tk.MustExec("create table t2(a int, b int, c int, d int, primary key(a,b,c))")
	tk.MustExec("insert into t1 values(1,1,1,1),(2,2,2,2),(3,3,3,3)")
	tk.MustExec("insert into t2 values(1,1,1,1),(2,2,2,2)")
	firstOperator := tk.MustQuery("explain format = 'brief' select /*+ inl_merge_join(t1,t2) */ * from t1 left join t2 on t1.a = t2.a and t1.c = t2.c and t1.b = t2.b order by t1.a desc").Rows()[0][0].(string)
	require.Equal(t, 0, strings.Index(firstOperator, plancodec.TypeIndexMergeJoin))
	tk.MustQuery("select /*+ inl_merge_join(t1,t2) */ * from t1 left join t2 on t1.a = t2.a and t1.c = t2.c and t1.b = t2.b order by t1.a desc").Check(testkit.Rows(
		"3 3 3 3 <nil> <nil> <nil> <nil>",
		"2 2 2 2 2 2 2 2",
		"1 1 1 1 1 1 1 1"))
}

func TestIssue19408(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1  (c_int int, primary key(c_int))")
	tk.MustExec("create table t2  (c_int int, unique key (c_int)) partition by hash (c_int) partitions 4")
	tk.MustExec("insert into t1 values (1), (2), (3), (4), (5)")
	tk.MustExec("insert into t2 select * from t1")
	tk.MustExec("begin")
	tk.MustExec("delete from t1 where c_int = 1")
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1,t2) */ * from t1, t2 where t1.c_int = t2.c_int").Sort().Check(testkit.Rows(
		"2 2",
		"3 3",
		"4 4",
		"5 5"))
	tk.MustQuery("select /*+ INL_JOIN(t1,t2) */ * from t1, t2 where t1.c_int = t2.c_int").Sort().Check(testkit.Rows(
		"2 2",
		"3 3",
		"4 4",
		"5 5"))
	tk.MustQuery("select /*+ INL_HASH_JOIN(t1,t2) */ * from t1, t2 where t1.c_int = t2.c_int").Sort().Check(testkit.Rows(
		"2 2",
		"3 3",
		"4 4",
		"5 5"))
	tk.MustExec("commit")
}

func TestIssue20137(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (id bigint(20) unsigned, primary key(id))")
	tk.MustExec("create table t2 (id bigint(20) unsigned)")
	tk.MustExec("insert into t1 values (8738875760185212610)")
	tk.MustExec("insert into t1 values (9814441339970117597)")
	tk.MustExec("insert into t2 values (8738875760185212610)")
	tk.MustExec("insert into t2 values (9814441339970117597)")
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t1, t2) */ * from t2 left join t1 on t1.id = t2.id order by t1.id").Check(
		testkit.Rows("8738875760185212610 8738875760185212610", "9814441339970117597 9814441339970117597"))
}

func TestIndexJoinOnSinglePartitionTable(t *testing.T) {
	// For issue 19145
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, val := range []string{string(variable.Static), string(variable.Dynamic)} {
		tk.MustExec("set @@tidb_partition_prune_mode= '" + val + "'")
		tk.MustExec("drop table if exists t1, t2")
		tk.MustExec("create table t1  (c_int int, c_str varchar(40), primary key (c_int) ) partition by range (c_int) ( partition p0 values less than (10), partition p1 values less than maxvalue )")
		tk.MustExec("create table t2  (c_int int, c_str varchar(40), primary key (c_int) ) partition by range (c_int) ( partition p0 values less than (10), partition p1 values less than maxvalue )")
		tk.MustExec("insert into t1 values (1, 'Alice')")
		tk.MustExec("insert into t2 values (1, 'Bob')")
		sql := "select /*+ INL_MERGE_JOIN(t1,t2) */ * from t1 join t2 partition(p0) on t1.c_int = t2.c_int and t1.c_str < t2.c_str"
		tk.MustQuery(sql).Check(testkit.Rows("1 Alice 1 Bob"))
		rows := testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + sql).Rows())
		// Partition table can't be inner side of index merge join, because it can't keep order.
		require.Equal(t, -1, strings.Index(rows[0], "IndexMergeJoin"))
		require.Equal(t, true, len(tk.MustQuery("show warnings").Rows()) > 0)

		sql = "select /*+ INL_HASH_JOIN(t1,t2) */ * from t1 join t2 partition(p0) on t1.c_int = t2.c_int and t1.c_str < t2.c_str"
		tk.MustQuery(sql).Check(testkit.Rows("1 Alice 1 Bob"))
		rows = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + sql).Rows())
		require.Equal(t, 0, strings.Index(rows[0], "IndexHashJoin"))

		sql = "select /*+ INL_JOIN(t1,t2) */ * from t1 join t2 partition(p0) on t1.c_int = t2.c_int and t1.c_str < t2.c_str"
		tk.MustQuery(sql).Check(testkit.Rows("1 Alice 1 Bob"))
		rows = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + sql).Rows())
		require.Equal(t, 0, strings.Index(rows[0], "IndexJoin"))
	}
}

func TestIssue20400(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, s")
	tk.MustExec("create table s(a int, index(a))")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustQuery("select /*+ hash_join(t,s)*/ * from t left join s on t.a=s.a and t.a>1").Check(
		testkit.Rows("1 <nil>"))
	tk.MustQuery("select /*+ inl_merge_join(t,s)*/ * from t left join s on t.a=s.a and t.a>1").Check(
		testkit.Rows("1 <nil>"))
}

func TestIssue20549(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("CREATE TABLE `t1` (`id` bigint(20) NOT NULL AUTO_INCREMENT, `t2id` bigint(20) DEFAULT NULL, PRIMARY KEY (`id`), KEY `t2id` (`t2id`));")
	tk.MustExec("INSERT INTO `t1` VALUES (1,NULL);")
	tk.MustExec("CREATE TABLE `t2` (`id` bigint(20) NOT NULL AUTO_INCREMENT, PRIMARY KEY (`id`));")
	tk.MustQuery("SELECT /*+ INL_MERGE_JOIN(t1,t2)  */ 1 from t1 left outer join t2 on t1.t2id=t2.id;").Check(
		testkit.Rows("1"))
	tk.MustQuery("SELECT /*+ HASH_JOIN(t1,t2)  */ 1 from t1 left outer join t2 on t1.t2id=t2.id;\n").Check(
		testkit.Rows("1"))
}

func TestIssue24473AndIssue25669(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists x, t2, t3")
	tk.MustExec("CREATE TABLE `x` (  `a` enum('y','b','1','x','0','null') DEFAULT NULL,  KEY `a` (`a`));")
	tk.MustExec("insert into x values(\"x\"),(\"x\"),(\"b\"),(\"y\");")
	tk.MustQuery("SELECT /*+ merge_join (t2,t3) */ t2.a,t3.a FROM x t2 inner join x t3 on t2.a = t3.a;").Sort().Check(
		testkit.Rows("b b", "x x", "x x", "x x", "x x", "y y"))
	tk.MustQuery("SELECT /*+ inl_merge_join (t2,t3) */ t2.a,t3.a FROM x t2 inner join x t3 on t2.a = t3.a;").Sort().Check(
		testkit.Rows("b b", "x x", "x x", "x x", "x x", "y y"))

	tk.MustExec("drop table if exists x, t2, t3")
	tk.MustExec("CREATE TABLE `x` (  `a` set('y','b','1','x','0','null') DEFAULT NULL,  KEY `a` (`a`));")
	tk.MustExec("insert into x values(\"x\"),(\"x\"),(\"b\"),(\"y\");")
	tk.MustQuery("SELECT /*+ merge_join (t2,t3) */ t2.a,t3.a FROM x t2 inner join x t3 on t2.a = t3.a;").Sort().Check(
		testkit.Rows("b b", "x x", "x x", "x x", "x x", "y y"))
	tk.MustQuery("SELECT /*+ inl_merge_join (t2,t3) */ t2.a,t3.a FROM x t2 inner join x t3 on t2.a = t3.a;").Sort().Check(
		testkit.Rows("b b", "x x", "x x", "x x", "x x", "y y"))
}
