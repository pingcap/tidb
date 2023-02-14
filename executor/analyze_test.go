// Copyright 2018 PingCAP, Inc.
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
	"io/ioutil"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
)

func TestAnalyzeFastSample(t *testing.T) {
	var cls testutils.Cluster
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cls = c
		}),
	)
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	var dom *domain.Domain
	session.DisableStats4Test()
	session.SetSchemaLease(0)
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()
	tk := testkit.NewTestKit(t, store)
	atomic.StoreInt64(&executor.RandSeed, 123)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, index index_b(b))")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	tid := tblInfo.ID

	// construct 5 regions split by {12, 24, 36, 48}
	splitKeys := generateTableSplitKeyForInt(tid, []int{12, 24, 36, 48})
	manipulateCluster(cls, splitKeys)

	for i := 0; i < 60; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d, %d)", i, i))
	}

	handleCols := core.BuildHandleColsForAnalyze(tk.Session(), tblInfo, true, nil)
	var colsInfo []*model.ColumnInfo //nolint: prealloc
	var indicesInfo []*model.IndexInfo
	for _, col := range tblInfo.Columns {
		if mysql.HasPriKeyFlag(col.GetFlag()) {
			continue
		}
		colsInfo = append(colsInfo, col)
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			indicesInfo = append(indicesInfo, idx)
		}
	}
	opts := make(map[ast.AnalyzeOptionType]uint64)
	opts[ast.AnalyzeOptNumSamples] = 20
	// Get a start_ts later than the above inserts.
	tk.MustExec("begin")
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	ts := txn.StartTS()
	tk.MustExec("commit")
	mockExec := &executor.AnalyzeTestFastExec{
		Ctx:         tk.Session().(sessionctx.Context),
		HandleCols:  handleCols,
		ColsInfo:    colsInfo,
		IdxsInfo:    indicesInfo,
		Concurrency: 1,
		Snapshot:    ts,
		TableID: statistics.AnalyzeTableID{
			PartitionID: -1,
			TableID:     tbl.(table.PhysicalTable).GetPhysicalID(),
		},
		TblInfo: tblInfo,
		Opts:    opts,
	}
	err = mockExec.TestFastSample()
	require.NoError(t, err)
	require.Len(t, mockExec.Collectors, 3)
	for i := 0; i < 2; i++ {
		samples := mockExec.Collectors[i].Samples
		require.Len(t, samples, 20)
		for j := 1; j < 20; j++ {
			cmp, err := samples[j].Value.Compare(tk.Session().GetSessionVars().StmtCtx, &samples[j-1].Value, collate.GetBinaryCollator())
			require.NoError(t, err)
			require.Greater(t, cmp, 0)
		}
	}
}

func TestFastAnalyze(t *testing.T) {
	t.Skip("Skip this unstable test(#25782) and bring it back before 2021-07-29.")
	var cls testutils.Cluster
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cls = c
		}),
	)
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	var dom *domain.Domain
	session.DisableStats4Test()
	session.SetSchemaLease(0)
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	dom.SetStatsUpdating(true)
	defer dom.Close()
	tk := testkit.NewTestKit(t, store)
	atomic.StoreInt64(&executor.RandSeed, 123)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c char(10), index index_b(b))")
	tk.MustExec("set @@session.tidb_enable_fast_analyze=1")
	tk.MustExec("set @@session.tidb_build_stats_concurrency=1")
	tk.MustExec("set @@tidb_analyze_version = 1")
	// Should not panic.
	tk.MustExec("analyze table t")
	tblInfo, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tid := tblInfo.Meta().ID

	// construct 6 regions split by {10, 20, 30, 40, 50}
	splitKeys := generateTableSplitKeyForInt(tid, []int{10, 20, 30, 40, 50})
	manipulateCluster(cls, splitKeys)

	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "char")`, i*3, i*3))
	}
	tk.MustExec("analyze table t with 5 buckets, 6 samples")

	is := tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	tbl := dom.StatsHandle().GetTableStats(tableInfo)
	// TODO(tangenta): add stats_meta.row_count assertion.
	for _, col := range tbl.Columns {
		ok, err := checkHistogram(tk.Session().GetSessionVars().StmtCtx, &col.Histogram)
		require.NoError(t, err)
		require.True(t, ok)
	}
	for _, idx := range tbl.Indices {
		ok, err := checkHistogram(tk.Session().GetSessionVars().StmtCtx, &idx.Histogram)
		require.NoError(t, err)
		require.True(t, ok)
	}

	// Test CM Sketch built from fast analyze.
	tk.MustExec("create table t1(a int, b int, index idx(a, b))")
	// Should not panic.
	tk.MustExec("analyze table t1")
	tk.MustExec("insert into t1 values (1,1),(1,1),(1,2),(1,2)")
	tk.MustExec("analyze table t1")
	tk.MustQuery("explain format = 'brief' select a from t1 where a = 1").Check(testkit.Rows(
		"IndexReader 4.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 4.00 cop[tikv] table:t1, index:idx(a, b) range:[1,1], keep order:false"))
	tk.MustQuery("explain format = 'brief' select a, b from t1 where a = 1 and b = 1").Check(testkit.Rows(
		"IndexReader 2.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t1, index:idx(a, b) range:[1 1,1 1], keep order:false"))
	tk.MustQuery("explain format = 'brief' select a, b from t1 where a = 1 and b = 2").Check(testkit.Rows(
		"IndexReader 2.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t1, index:idx(a, b) range:[1 2,1 2], keep order:false"))

	tk.MustExec("create table t2 (a bigint unsigned, primary key(a))")
	tk.MustExec("insert into t2 values (0), (18446744073709551615)")
	tk.MustExec("analyze table t2")
	tk.MustQuery("show stats_buckets where table_name = 't2'").Check(testkit.Rows(
		"test t2  a 0 0 1 1 0 0 0",
		"test t2  a 0 1 2 1 18446744073709551615 18446744073709551615 0"))

	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	tk.MustExec(`create table t3 (id int, v int, primary key(id), index k(v)) partition by hash (id) partitions 4`)
	tk.MustExec(`insert into t3 values(1, 1), (2, 2), (5, 1), (9, 3), (13, 3), (17, 5), (3, 0)`)
	tk.MustExec(`analyze table t3`)
	tk.MustQuery(`explain format = 'brief' select v from t3 partition(p1) where v = 3`).Check(testkit.Rows(
		"IndexReader 2.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 2.00 cop[tikv] table:t3, partition:p1, index:k(v) range:[3,3], keep order:false",
	))
	//nolint:revive,all_revive
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Dynamic) + `'`)

	// global-stats depends on stats-ver2, but stats-ver2 is not compatible with fast-analyze, so forbid using global-stats with fast-analyze now.
	// TODO: add more test cases about global-stats with fast-analyze after resolving the compatibility problem.
	/*
		// test fast analyze in dynamic mode
		tk.MustExec("drop table if exists t4;")
		tk.MustExec("create table t4(a int, b int) PARTITION BY HASH(a) PARTITIONS 2;")
		tk.MustExec("insert into t4 values(1,1),(3,3),(4,4),(2,2),(5,5);")
		// Because the statistics of partition p1 are missing, the construction of global-level stats will fail.
		tk.MustExec("analyze table t4 partition p1;")
		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 8131 Build global-level stats failed due to missing partition-level stats"))
		// Although the global-level stats build failed, we build partition-level stats for partition p1 success.
		result := tk.MustQuery("show stats_meta where table_name = 't4'").Sort()
		c.Assert(len(result.Rows()), Equals, 1)
		c.Assert(result.Rows()[0][5], Equals, "3")
		// Now, we have the partition-level stats for partition p0. We need get the stats for partition p1. And build the global-level stats.
		tk.MustExec("analyze table t4 partition p0;")
		tk.MustQuery("show warnings").Check(testkit.Rows())
		result = tk.MustQuery("show stats_meta where table_name = 't4'").Sort()
		c.Assert(len(result.Rows()), Equals, 3)
		c.Assert(result.Rows()[0][5], Equals, "5")
		c.Assert(result.Rows()[1][5], Equals, "2")
		c.Assert(result.Rows()[2][5], Equals, "3")
	*/
}

func checkHistogram(sc *stmtctx.StatementContext, hg *statistics.Histogram) (bool, error) {
	for i := 0; i < len(hg.Buckets); i++ {
		lower, upper := hg.GetLower(i), hg.GetUpper(i)
		cmp, err := upper.Compare(sc, lower, collate.GetBinaryCollator())
		if cmp < 0 || err != nil {
			return false, err
		}
		if i == 0 {
			continue
		}
		previousUpper := hg.GetUpper(i - 1)
		cmp, err = lower.Compare(sc, previousUpper, collate.GetBinaryCollator())
		if cmp <= 0 || err != nil {
			return false, err
		}
	}
	return true, nil
}

func TestAnalyzeIndexExtractTopN(t *testing.T) {
	_ = checkHistogram
	t.Skip("unstable, skip it and fix it before 20210618")
	store, err := mockstore.NewMockStore()
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	var dom *domain.Domain
	session.DisableStats4Test()
	session.SetSchemaLease(0)
	dom, err = session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database test_index_extract_topn")
	tk.MustExec("use test_index_extract_topn")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a, b))")
	tk.MustExec("insert into t values(1, 1), (1, 1), (1, 2), (1, 2)")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	tk.MustExec("analyze table t")

	is := tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	table, err := is.TableByName(model.NewCIStr("test_index_extract_topn"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	tbl := dom.StatsHandle().GetTableStats(tableInfo)

	// Construct TopN, should be (1, 1) -> 2 and (1, 2) -> 2
	topn := statistics.NewTopN(2)
	{
		key1, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx, nil, types.NewIntDatum(1), types.NewIntDatum(1))
		require.NoError(t, err)
		topn.AppendTopN(key1, 2)
		key2, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx, nil, types.NewIntDatum(1), types.NewIntDatum(2))
		require.NoError(t, err)
		topn.AppendTopN(key2, 2)
	}
	for _, idx := range tbl.Indices {
		ok, err := checkHistogram(tk.Session().GetSessionVars().StmtCtx, &idx.Histogram)
		require.NoError(t, err)
		require.True(t, ok)
		require.True(t, idx.TopN.Equal(topn))
	}
}

func TestAnalyzePartitionTableForFloat(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1 ( id bigint(20) unsigned NOT NULL AUTO_INCREMENT, num float(9,8) DEFAULT NULL, PRIMARY KEY (id)  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin PARTITION BY HASH (id) PARTITIONS 128;")
	// To reproduce the error we meet in https://github.com/pingcap/tidb/issues/35910, we should use the data provided in this issue
	b, err := ioutil.ReadFile("testdata/analyze_test_data.sql")
	require.NoError(t, err)
	sqls := strings.Split(string(b), ";")
	for _, sql := range sqls {
		if len(sql) < 1 {
			continue
		}
		tk.MustExec(sql)
	}
	tk.MustExec("analyze table t1")
}

func TestAnalyzePartitionTableByConcurrencyInDynamic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("use test")
	tk.MustExec("create table t(id int) partition by hash(id) partitions 4")
	testcases := []struct {
		concurrency string
	}{
		{
			concurrency: "1",
		},
		{
			concurrency: "2",
		},
		{
			concurrency: "3",
		},
		{
			concurrency: "4",
		},
		{
			concurrency: "5",
		},
	}
	// assert empty table
	for _, tc := range testcases {
		concurrency := tc.concurrency
		fmt.Println("testcase ", concurrency)
		tk.MustExec(fmt.Sprintf("set @@tidb_merge_partition_stats_concurrency=%v", concurrency))
		tk.MustQuery("select @@tidb_merge_partition_stats_concurrency").Check(testkit.Rows(concurrency))
		tk.MustExec(fmt.Sprintf("set @@tidb_analyze_partition_concurrency=%v", concurrency))
		tk.MustQuery("select @@tidb_analyze_partition_concurrency").Check(testkit.Rows(concurrency))

		tk.MustExec("analyze table t")
		tk.MustQuery("show stats_topn where partition_name = 'global' and table_name = 't'")
	}

	for i := 1; i <= 500; i++ {
		for j := 1; j <= 20; j++ {
			tk.MustExec(fmt.Sprintf("insert into t (id) values (%v)", j))
		}
	}
	var expected [][]interface{}
	for i := 1; i <= 20; i++ {
		expected = append(expected, []interface{}{
			strconv.FormatInt(int64(i), 10), "500",
		})
	}
	testcases = []struct {
		concurrency string
	}{
		{
			concurrency: "1",
		},
		{
			concurrency: "2",
		},
		{
			concurrency: "3",
		},
		{
			concurrency: "4",
		},
		{
			concurrency: "5",
		},
	}
	for _, tc := range testcases {
		concurrency := tc.concurrency
		fmt.Println("testcase ", concurrency)
		tk.MustExec(fmt.Sprintf("set @@tidb_merge_partition_stats_concurrency=%v", concurrency))
		tk.MustQuery("select @@tidb_merge_partition_stats_concurrency").Check(testkit.Rows(concurrency))
		tk.MustExec("analyze table t")
		tk.MustQuery("show stats_topn where partition_name = 'global' and table_name = 't'").CheckAt([]int{5, 6}, expected)
	}
}

func TestMergeGlobalStatsWithUnAnalyzedPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_partition_prune_mode=dynamic;")
	tk.MustExec("CREATE TABLE `t` (   `id` int(11) DEFAULT NULL,   `a` int(11) DEFAULT NULL, `b` int(11) DEFAULT NULL, `c` int(11) DEFAULT NULL ) PARTITION BY RANGE (`id`) (PARTITION `p0` VALUES LESS THAN (3),  PARTITION `p1` VALUES LESS THAN (7),  PARTITION `p2` VALUES LESS THAN (11));")
	tk.MustExec("insert into t values (1,1,1,1),(2,2,2,2),(4,4,4,4),(5,5,5,5),(6,6,6,6),(8,8,8,8),(9,9,9,9);")
	tk.MustExec("create index idxa on t (a);")
	tk.MustExec("create index idxb on t (b);")
	tk.MustExec("create index idxc on t (c);")
	tk.MustExec("analyze table t partition p0 index idxa;")
	tk.MustExec("analyze table t partition p1 index idxb;")
	tk.MustExec("analyze table t partition p2 index idxc;")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 The version 2 would collect all statistics not only the selected indexes",
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p2"))
	tk.MustExec("analyze table t partition p0;")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0"))
}
