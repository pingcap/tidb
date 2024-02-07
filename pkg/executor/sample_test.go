// Copyright 2020 PingCAP, Inc.
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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func createSampleTestkit(t *testing.T, store kv.Storage) *testkit.TestKit {
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test_table_sample;")
	tk.MustExec("create database test_table_sample;")
	tk.MustExec("use test_table_sample;")
	tk.MustExec("set @@global.tidb_scatter_region=1;")
	return tk
}

func TestTableSampleBasic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := createSampleTestkit(t, store)
	tk.MustExec("create table t (a int);")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows())

	tk.MustExec("insert into t values (0), (1000), (2000);")
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows("0"))
	tk.MustExec("alter table t add column b varchar(255) not null default 'abc';")
	tk.MustQuery("select b from t tablesample regions();").Check(testkit.Rows("abc"))
	tk.MustExec("alter table t add column c int as (a + 1);")
	tk.MustQuery("select c from t tablesample regions();").Check(testkit.Rows("1"))
	tk.MustQuery("select c, _tidb_rowid from t tablesample regions();").Check(testkit.Rows("1 1"))
	tk.MustHavePlan("select * from t tablesample regions();", "TableSample")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a BIGINT PRIMARY KEY AUTO_RANDOM(3), b int auto_increment, key(b)) pre_split_regions=8;")
	tk.MustQuery("select * from t tablesample regions();").Check(testkit.Rows())
	for i := 0; i < 1000; i++ {
		tk.MustExec("insert into t values();")
	}
	tk.MustQuery("select count(*) from t tablesample regions();").Check(testkit.Rows("8"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a varchar(30) collate utf8mb4_general_ci primary key);")
	tk.MustQuery("split table t between ('a') and ('z') regions 100;").Check(testkit.Rows("99 1"))
	tk.MustExec("insert into t values ('a'), ('b'), ('c'), ('d'), ('e');")
	tk.MustQuery("select a from t tablesample regions() limit 2;").Check(testkit.Rows("a", "b"))
}

func TestTableSampleMultiRegions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := createSampleTestkit(t, store)
	tk.MustExec("create table t (a int) shard_row_id_bits = 2 pre_split_regions = 2;")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?);", i)
	}
	rows := tk.MustQuery("select * from t tablesample regions();").Rows()
	require.Len(t, rows, 4)
	tk.MustQuery("select a from t tablesample regions() order by a limit 1;").Check(testkit.Rows("0"))
	tk.MustQuery("select a from t tablesample regions() where a = 0;").Check(testkit.Rows("0"))

	tk.MustExec("create table t2 (a int) shard_row_id_bits = 2 pre_split_regions = 2;")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t2 values (?);", i)
	}
	rows = tk.MustQuery("select * from t tablesample regions(), t2 tablesample regions();").Rows()
	require.Len(t, rows, 16)
	tk.MustQuery("select count(*) from t tablesample regions();").Check(testkit.Rows("4"))
	tk.MustExec("drop table t2;")
}

func TestTableSampleNoSplitTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := createSampleTestkit(t, store)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 0)
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("drop table if exists t2;")
	tk.MustExec("create table t1 (id int primary key);")
	tk.MustExec("create table t2 (id int primary key);")
	tk.MustExec("insert into t2 values(1);")
	rows := tk.MustQuery("select * from t1 tablesample regions();").Rows()
	rows2 := tk.MustQuery("select * from t2 tablesample regions();").Rows()
	require.Len(t, rows, 0)
	require.Len(t, rows2, 1)
}

func TestTableSamplePlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := createSampleTestkit(t, store)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a bigint, b int default 10);")
	tk.MustExec("split table t between (0) and (100000) regions 4;")
	tk.MustExec("insert into t(a) values (1), (2), (3);")
	rows := tk.MustQuery("explain analyze select a from t tablesample regions();").Rows()
	require.Len(t, rows, 2)
	tableSample := fmt.Sprintf("%v", rows[1])
	require.Regexp(t, ".*TableSample.*", tableSample)
}

func TestMaxChunkSize(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := createSampleTestkit(t, store)
	tk.MustExec("create table t (a int) shard_row_id_bits = 2 pre_split_regions = 2;")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?);", i)
	}
	tk.Session().GetSessionVars().MaxChunkSize = 1
	rows := tk.MustQuery("select * from t tablesample regions();").Rows()
	require.Len(t, rows, 4)
}
