// Copyright 2017 PingCAP, Inc.
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
	"testing"
	"time"

	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestShowStatsMeta(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("analyze table t, t1")
	result := tk.MustQuery("show stats_meta")
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "t", result.Rows()[0][1])
	require.Equal(t, "t1", result.Rows()[1][1])
	result = tk.MustQuery("show stats_meta where table_name = 't'")
	require.Len(t, result.Rows(), 1)
	require.Equal(t, "t", result.Rows()[0][1])
}

func TestShowStatsHistograms(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("analyze table t")
	result := tk.MustQuery("show stats_histograms")
	require.Len(t, result.Rows(), 0)
	tk.MustExec("insert into t values(1,1)")
	tk.MustExec("analyze table t")
	result = tk.MustQuery("show stats_histograms").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "a", result.Rows()[0][3])
	require.Equal(t, "b", result.Rows()[1][3])
	result = tk.MustQuery("show stats_histograms where column_name = 'a'")
	require.Len(t, result.Rows(), 1)
	require.Equal(t, "a", result.Rows()[0][3])

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b int, c int, index idx_b(b), index idx_c_a(c, a))")
	tk.MustExec("insert into t values(1,null,1),(2,null,2),(3,3,3),(4,null,4),(null,null,null)")
	res := tk.MustQuery("show stats_histograms where table_name = 't'")
	require.Len(t, res.Rows(), 0)
	tk.MustExec("analyze table t index idx_b")
	res = tk.MustQuery("show stats_histograms where table_name = 't' and column_name = 'idx_b'")
	require.Len(t, res.Rows(), 1)
}

func TestShowStatsBuckets(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	// Simple behavior testing. Version=1 is enough.
	tk.MustExec("set @@tidb_analyze_version=1")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create index idx on t(a,b)")
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("analyze table t with 0 topn")
	result := tk.MustQuery("show stats_buckets").Sort()
	result.Check(testkit.Rows("test t  a 0 0 1 1 1 1 0", "test t  b 0 0 1 1 1 1 0", "test t  idx 1 0 1 1 (1, 1) (1, 1) 0"))
	result = tk.MustQuery("show stats_buckets where column_name = 'idx'")
	result.Check(testkit.Rows("test t  idx 1 0 1 1 (1, 1) (1, 1) 0"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t (`a` datetime, `b` int, key `idx`(`a`, `b`))")
	tk.MustExec("insert into t values (\"2020-01-01\", 1)")
	tk.MustExec("analyze table t with 0 topn")
	result = tk.MustQuery("show stats_buckets").Sort()
	result.Check(testkit.Rows("test t  a 0 0 1 1 2020-01-01 00:00:00 2020-01-01 00:00:00 0", "test t  b 0 0 1 1 1 1 0", "test t  idx 1 0 1 1 (2020-01-01 00:00:00, 1) (2020-01-01 00:00:00, 1) 0"))
	result = tk.MustQuery("show stats_buckets where column_name = 'idx'")
	result.Check(testkit.Rows("test t  idx 1 0 1 1 (2020-01-01 00:00:00, 1) (2020-01-01 00:00:00, 1) 0"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t (`a` date, `b` int, key `idx`(`a`, `b`))")
	tk.MustExec("insert into t values (\"2020-01-01\", 1)")
	tk.MustExec("analyze table t with 0 topn")
	result = tk.MustQuery("show stats_buckets").Sort()
	result.Check(testkit.Rows("test t  a 0 0 1 1 2020-01-01 2020-01-01 0", "test t  b 0 0 1 1 1 1 0", "test t  idx 1 0 1 1 (2020-01-01, 1) (2020-01-01, 1) 0"))
	result = tk.MustQuery("show stats_buckets where column_name = 'idx'")
	result.Check(testkit.Rows("test t  idx 1 0 1 1 (2020-01-01, 1) (2020-01-01, 1) 0"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t (`a` timestamp, `b` int, key `idx`(`a`, `b`))")
	tk.MustExec("insert into t values (\"2020-01-01\", 1)")
	tk.MustExec("analyze table t with 0 topn")
	result = tk.MustQuery("show stats_buckets").Sort()
	result.Check(testkit.Rows("test t  a 0 0 1 1 2020-01-01 00:00:00 2020-01-01 00:00:00 0", "test t  b 0 0 1 1 1 1 0", "test t  idx 1 0 1 1 (2020-01-01 00:00:00, 1) (2020-01-01 00:00:00, 1) 0"))
	result = tk.MustQuery("show stats_buckets where column_name = 'idx'")
	result.Check(testkit.Rows("test t  idx 1 0 1 1 (2020-01-01 00:00:00, 1) (2020-01-01 00:00:00, 1) 0"))
}

func TestShowStatsHasNullValue(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, index idx(a))")
	tk.MustExec("insert into t values(NULL)")
	tk.MustExec("set @@session.tidb_analyze_version=1")
	tk.MustExec("analyze table t")
	// Null values are excluded from histogram for single-column index.
	tk.MustQuery("show stats_buckets").Check(testkit.Rows())
	tk.MustExec("insert into t values(1)")
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_buckets").Sort().Check(testkit.Rows(
		"test t  a 0 0 1 1 1 1 0",
		"test t  idx 1 0 1 1 1 1 0",
	))
	tk.MustExec("drop table t")
	tk.MustExec("create table t (a int, b int, index idx(a, b))")
	tk.MustExec("insert into t values(NULL, NULL)")
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  idx 1 0 1 1 (NULL, NULL) (NULL, NULL) 0"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a int, b int, c int, index idx_b(b), index idx_c_a(c, a))")
	tk.MustExec("insert into t values(1,null,1),(2,null,2),(3,3,3),(4,null,4),(null,null,null)")
	res := tk.MustQuery("show stats_histograms where table_name = 't'")
	require.Len(t, res.Rows(), 0)
	tk.MustExec("analyze table t index idx_b")
	res = tk.MustQuery("show stats_histograms where table_name = 't' and column_name = 'idx_b'")
	require.Len(t, res.Rows(), 1)
	require.Equal(t, "4", res.Rows()[0][7])
	res = tk.MustQuery("show stats_histograms where table_name = 't' and column_name = 'b'")
	require.Len(t, res.Rows(), 0)
	tk.MustExec("analyze table t index idx_c_a")
	res = tk.MustQuery("show stats_histograms where table_name = 't' and column_name = 'idx_c_a'")
	require.Len(t, res.Rows(), 1)
	require.Equal(t, "0", res.Rows()[0][7])
	res = tk.MustQuery("show stats_histograms where table_name = 't' and column_name = 'c'")
	require.Len(t, res.Rows(), 0)
	res = tk.MustQuery("show stats_histograms where table_name = 't' and column_name = 'a'")
	require.Len(t, res.Rows(), 0)
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(1,null,1),(2,null,2),(3,3,3),(4,null,4),(null,null,null)")
	res = tk.MustQuery("show stats_histograms where table_name = 't'")
	require.Len(t, res.Rows(), 0)
	tk.MustExec("analyze table t index")
	res = tk.MustQuery("show stats_histograms where table_name = 't'").Sort()
	require.Len(t, res.Rows(), 2)
	require.Equal(t, "4", res.Rows()[0][7])
	require.Equal(t, "0", res.Rows()[1][7])
	tk.MustExec("truncate table t")
	tk.MustExec("insert into t values(1,null,1),(2,null,2),(3,3,3),(4,null,4),(null,null,null)")
	tk.MustExec("analyze table t")
	res = tk.MustQuery("show stats_histograms where table_name = 't'").Sort()
	require.Len(t, res.Rows(), 5)
	require.Equal(t, "1", res.Rows()[0][7])
	require.Equal(t, "4", res.Rows()[1][7])
	require.Equal(t, "1", res.Rows()[2][7])
	require.Equal(t, "4", res.Rows()[3][7])
	require.Equal(t, "0", res.Rows()[4][7])
}

func TestShowPartitionStats(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	testkit.WithPruneMode(tk, variable.Static, func() {
		tk.MustExec("set @@session.tidb_enable_table_partition=1")
		// Version2 is tested in TestGlobalStatsData1/2/3 and TestAnalyzeGlobalStatsWithOpts.
		tk.MustExec("set @@session.tidb_analyze_version=1")
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		createTable := `CREATE TABLE t (a int, b int, primary key(a), index idx(b))
						PARTITION BY RANGE ( a ) (PARTITION p0 VALUES LESS THAN (6))`
		tk.MustExec(createTable)
		tk.MustExec(`insert into t values (1, 1)`)
		tk.MustExec("analyze table t")

		result := tk.MustQuery("show stats_meta")
		require.Len(t, result.Rows(), 1)
		require.Equal(t, "test", result.Rows()[0][0])
		require.Equal(t, "t", result.Rows()[0][1])
		require.Equal(t, "p0", result.Rows()[0][2])

		result = tk.MustQuery("show stats_histograms").Sort()
		require.Len(t, result.Rows(), 3)
		require.Equal(t, "p0", result.Rows()[0][2])
		require.Equal(t, "a", result.Rows()[0][3])
		require.Equal(t, "p0", result.Rows()[1][2])
		require.Equal(t, "b", result.Rows()[1][3])
		require.Equal(t, "p0", result.Rows()[2][2])
		require.Equal(t, "idx", result.Rows()[2][3])

		result = tk.MustQuery("show stats_buckets").Sort()
		result.Check(testkit.Rows("test t p0 a 0 0 1 1 1 1 0", "test t p0 b 0 0 1 1 1 1 0", "test t p0 idx 1 0 1 1 1 1 0"))

		result = tk.MustQuery("show stats_healthy")
		result.Check(testkit.Rows("test t p0 100"))
	})
}

func TestShowStatusSnapshot(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop database if exists test;")
	tk.MustExec("create database test;")
	tk.MustExec("use test;")
	tk.MustExec("create table t (a int);")

	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	snapshotTime := time.Now()

	tk.MustExec("drop table t;")
	tk.MustQuery("show table status;").Check(testkit.Rows())
	tk.MustExec("set @@tidb_snapshot = '" + snapshotTime.Format("2006-01-02 15:04:05.999999") + "'")
	result := tk.MustQuery("show table status;")
	require.Equal(t, "t", result.Rows()[0][0])
}

func TestShowStatsExtended(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	dom.StatsHandle().Clear()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values(1,1,3),(2,2,2),(3,3,1)")

	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustExec("alter table t add stats_extended s2 correlation(a,c)")
	tk.MustQuery("select name, status from mysql.stats_extended where name like 's%'").Sort().Check(testkit.Rows(
		"s1 0",
		"s2 0",
	))
	result := tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	require.Len(t, result.Rows(), 0)

	tk.MustExec("analyze table t")
	tk.MustQuery("select name, status from mysql.stats_extended where name like 's%'").Sort().Check(testkit.Rows(
		"s1 1",
		"s2 1",
	))
	result = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'").Sort()
	require.Len(t, result.Rows(), 2)
	require.Equal(t, "test", result.Rows()[0][0])
	require.Equal(t, "t", result.Rows()[0][1])
	require.Equal(t, "s1", result.Rows()[0][2])
	require.Equal(t, "[a,b]", result.Rows()[0][3])
	require.Equal(t, "correlation", result.Rows()[0][4])
	require.Equal(t, "1.000000", result.Rows()[0][5])

	require.Equal(t, "test", result.Rows()[1][0])
	require.Equal(t, "t", result.Rows()[1][1])
	require.Equal(t, "s2", result.Rows()[1][2])
	require.Equal(t, "[a,c]", result.Rows()[1][3])
	require.Equal(t, "correlation", result.Rows()[1][4])
	require.Equal(t, "-1.000000", result.Rows()[1][5])
	require.Equal(t, result.Rows()[1][6], result.Rows()[0][6])

	tk.MustExec("alter table t drop stats_extended s1")
	tk.MustExec("alter table t drop stats_extended s2")
	tk.MustQuery("select name, status from mysql.stats_extended where name like 's%'").Sort().Check(testkit.Rows(
		"s1 2",
		"s2 2",
	))
	dom.StatsHandle().Update(dom.InfoSchema())
	result = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'")
	require.Len(t, result.Rows(), 0)
}

func TestShowColumnStatsUsage(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int, index idx_a_b(a, b))")
	tk.MustExec("create table t2 (a int, b int) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than maxvalue)")

	is := dom.InfoSchema()
	t1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	t2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tk.MustExec(fmt.Sprintf("insert into mysql.column_stats_usage values (%d, %d, null, '2021-10-20 08:00:00')", t1.Meta().ID, t1.Meta().Columns[0].ID))
	tk.MustExec(fmt.Sprintf("insert into mysql.column_stats_usage values (%d, %d, '2021-10-20 09:00:00', null)", t2.Meta().ID, t2.Meta().Columns[0].ID))
	p0 := t2.Meta().GetPartitionInfo().Definitions[0]
	tk.MustExec(fmt.Sprintf("insert into mysql.column_stats_usage values (%d, %d, '2021-10-20 09:00:00', null)", p0.ID, t2.Meta().Columns[0].ID))

	result := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't1'").Sort()
	rows := result.Rows()
	require.Len(t, rows, 1)
	require.Equal(t, rows[0], []interface{}{"test", "t1", "", t1.Meta().Columns[0].Name.O, "<nil>", "2021-10-20 08:00:00"})

	result = tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't2'").Sort()
	rows = result.Rows()

	require.Len(t, rows, 2)
	require.Equal(t, rows[0], []interface{}{"test", "t2", "global", t1.Meta().Columns[0].Name.O, "2021-10-20 09:00:00", "<nil>"})
	require.Equal(t, rows[1], []interface{}{"test", "t2", p0.Name.O, t1.Meta().Columns[0].Name.O, "2021-10-20 09:00:00", "<nil>"})
}

func TestShowHistogramsInFlight(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	result := tk.MustQuery("show histograms_in_flight")
	rows := result.Rows()
	require.Len(t, rows, 1)
	require.Equal(t, rows[0][0], "0")
}

func TestShowAnalyzeStatus(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("delete from mysql.analyze_jobs")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, primary key(a), index idx(b))")
	tk.MustExec(`insert into t values (1, 1), (2, 2)`)

	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	rows := tk.MustQuery("show analyze status").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "test", rows[0][0])
	require.Equal(t, "t", rows[0][1])
	require.Equal(t, "", rows[0][2])
	require.Equal(t, "analyze table all columns with 256 buckets, 500 topn, 1 samplerate", rows[0][3])
	require.Equal(t, "2", rows[0][4])
	checkTime := func(val interface{}) {
		str, ok := val.(string)
		require.True(t, ok)
		_, err := time.Parse("2006-01-02 15:04:05", str)
		require.NoError(t, err)
	}
	checkTime(rows[0][5])
	checkTime(rows[0][6])
	require.Equal(t, "finished", rows[0][7])
	require.Equal(t, "<nil>", rows[0][8])
	serverInfo, err := infosync.GetServerInfo()
	require.NoError(t, err)
	addr := fmt.Sprintf("%s:%d", serverInfo.IP, serverInfo.Port)
	require.Equal(t, addr, rows[0][9])
	require.Equal(t, "<nil>", rows[0][10])

	tk.MustExec("delete from mysql.analyze_jobs")
	tk.MustExec("set @@tidb_analyze_version=1")
	tk.MustExec("analyze table t")
	rows = tk.MustQuery("show analyze status").Sort().Rows()
	require.Len(t, rows, 2)
	require.Equal(t, "test", rows[0][0])
	require.Equal(t, "t", rows[0][1])
	require.Equal(t, "", rows[0][2])
	require.Equal(t, "analyze columns", rows[0][3])
	require.Equal(t, "2", rows[0][4])
	checkTime(rows[0][5])
	checkTime(rows[0][6])
	require.Equal(t, "finished", rows[0][7])
	require.Equal(t, "test", rows[1][0])
	require.Equal(t, "<nil>", rows[0][8])
	require.Equal(t, addr, rows[0][9])
	require.Equal(t, "<nil>", rows[0][10])
	require.Equal(t, "t", rows[1][1])
	require.Equal(t, "", rows[1][2])
	require.Equal(t, "analyze index idx", rows[1][3])
	require.Equal(t, "2", rows[1][4])
	checkTime(rows[1][5])
	checkTime(rows[1][6])
	require.Equal(t, "finished", rows[1][7])
	require.Equal(t, "<nil>", rows[1][8])
	require.Equal(t, addr, rows[1][9])
	require.Equal(t, "<nil>", rows[1][10])
}
