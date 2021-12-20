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
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestAnalyzePartition(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	testkit.WithPruneMode(tk, variable.Static, func() {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("set @@tidb_analyze_version=2")
		createTable := `CREATE TABLE t (a int, b int, c varchar(10), primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
)`
		tk.MustExec(createTable)
		for i := 1; i < 21; i++ {
			tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
		}
		tk.MustExec("analyze table t")

		is := tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
		table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		pi := table.Meta().GetPartitionInfo()
		require.NotNil(t, pi)
		do, err := session.GetDomain(store)
		require.NoError(t, err)
		handle := do.StatsHandle()
		for _, def := range pi.Definitions {
			statsTbl := handle.GetPartitionStats(table.Meta(), def.ID)
			require.False(t, statsTbl.Pseudo)
			require.Len(t, statsTbl.Columns, 3)
			require.Len(t, statsTbl.Indices, 1)
			for _, col := range statsTbl.Columns {
				require.Greater(t, col.Len()+col.Num(), 0)
			}
			for _, idx := range statsTbl.Indices {
				require.Greater(t, idx.Len()+idx.Num(), 0)
			}
		}

		tk.MustExec("drop table t")
		tk.MustExec(createTable)
		for i := 1; i < 21; i++ {
			tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
		}
		tk.MustExec("alter table t analyze partition p0")
		is = tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
		table, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
		require.NoError(t, err)
		pi = table.Meta().GetPartitionInfo()
		require.NotNil(t, pi)

		for i, def := range pi.Definitions {
			statsTbl := handle.GetPartitionStats(table.Meta(), def.ID)
			if i == 0 {
				require.False(t, statsTbl.Pseudo)
				require.Len(t, statsTbl.Columns, 3)
				require.Len(t, statsTbl.Indices, 1)
			} else {
				require.True(t, statsTbl.Pseudo)
			}
		}
	})
}

func TestAnalyzeReplicaReadFollower(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	ctx := tk.Session().(sessionctx.Context)
	ctx.GetSessionVars().SetReplicaRead(kv.ReplicaReadFollower)
	tk.MustExec("analyze table t")
}

func TestClusterIndexAnalyze(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists test_cluster_index_analyze;")
	tk.MustExec("create database test_cluster_index_analyze;")
	tk.MustExec("use test_cluster_index_analyze;")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn

	tk.MustExec("create table t (a int, b int, c int, primary key(a, b));")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?, ?, ?)", i, i, i)
	}
	tk.MustExec("analyze table t;")
	tk.MustExec("drop table t;")

	tk.MustExec("create table t (a varchar(255), b int, c float, primary key(c, a));")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?, ?, ?)", strconv.Itoa(i), i, i)
	}
	tk.MustExec("analyze table t;")
	tk.MustExec("drop table t;")

	tk.MustExec("create table t (a char(10), b decimal(5, 3), c int, primary key(a, c, b));")
	for i := 0; i < 100; i++ {
		tk.MustExec("insert into t values (?, ?, ?)", strconv.Itoa(i), i, i)
	}
	tk.MustExec("analyze table t;")
	tk.MustExec("drop table t;")
}

func TestAnalyzeRestrict(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	ctx := tk.Session().(sessionctx.Context)
	ctx.GetSessionVars().InRestrictedSQL = true
	tk.MustExec("analyze table t")
}

func TestAnalyzeParameters(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}
	tk.MustExec("insert into t values (19), (19), (19)")

	tk.MustExec("set @@tidb_enable_fast_analyze = 1")
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("analyze table t with 30 samples")
	is := tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	tbl := dom.StatsHandle().GetTableStats(tableInfo)
	col := tbl.Columns[1]
	require.Equal(t, 20, col.Len())
	require.Len(t, col.TopN.TopN, 1)
	width, depth := col.CMSketch.GetWidthAndDepth()
	require.Equal(t, int32(5), depth)
	require.Equal(t, int32(2048), width)

	tk.MustExec("analyze table t with 4 buckets, 0 topn, 4 cmsketch width, 4 cmsketch depth")
	tbl = dom.StatsHandle().GetTableStats(tableInfo)
	col = tbl.Columns[1]
	require.Equal(t, 4, col.Len())
	require.Nil(t, col.TopN)
	width, depth = col.CMSketch.GetWidthAndDepth()
	require.Equal(t, int32(4), depth)
	require.Equal(t, int32(4), width)

	// Test very large cmsketch
	tk.MustExec(fmt.Sprintf("analyze table t with %d cmsketch width, %d cmsketch depth", core.CMSketchSizeLimit, 1))
	tbl = dom.StatsHandle().GetTableStats(tableInfo)
	col = tbl.Columns[1]
	require.Equal(t, 20, col.Len())

	require.Len(t, col.TopN.TopN, 1)
	width, depth = col.CMSketch.GetWidthAndDepth()
	require.Equal(t, int32(1), depth)
	require.Equal(t, int32(core.CMSketchSizeLimit), width)

	// Test very large cmsketch
	tk.MustExec("analyze table t with 20480 cmsketch width, 50 cmsketch depth")
	tbl = dom.StatsHandle().GetTableStats(tableInfo)
	col = tbl.Columns[1]
	require.Equal(t, 20, col.Len())
	require.Len(t, col.TopN.TopN, 1)
	width, depth = col.CMSketch.GetWidthAndDepth()
	require.Equal(t, int32(50), depth)
	require.Equal(t, int32(20480), width)
}

func TestAnalyzeTooLongColumns(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a json)")
	value := fmt.Sprintf(`{"x":"%s"}`, strings.Repeat("x", mysql.MaxFieldVarCharLength))
	tk.MustExec(fmt.Sprintf("insert into t values ('%s')", value))

	tk.MustExec("analyze table t")
	is := tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	tbl := dom.StatsHandle().GetTableStats(tableInfo)
	require.Equal(t, 0, tbl.Columns[1].Len())
	require.Equal(t, int64(65559), tbl.Columns[1].TotColSize)
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
	var colsInfo []*model.ColumnInfo
	var indicesInfo []*model.IndexInfo
	for _, col := range tblInfo.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
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

func TestIssue15993(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT PRIMARY KEY);")
	tk.MustExec("set @@tidb_enable_fast_analyze=1;")
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("ANALYZE TABLE t0 INDEX PRIMARY;")
}

func TestIssue15751(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT, c1 INT, PRIMARY KEY(c0, c1))")
	tk.MustExec("INSERT INTO t0 VALUES (0, 0)")
	tk.MustExec("set @@tidb_enable_fast_analyze=1")
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("ANALYZE TABLE t0")
}

func TestIssue15752(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("CREATE TABLE t0(c0 INT)")
	tk.MustExec("INSERT INTO t0 VALUES (0)")
	tk.MustExec("CREATE INDEX i0 ON t0(c0)")
	tk.MustExec("set @@tidb_enable_fast_analyze=1")
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("ANALYZE TABLE t0 INDEX i0")
}

type regionProperityClient struct {
	tikv.Client
	mu struct {
		sync.Mutex
		failedOnce bool
		count      int64
	}
}

func (c *regionProperityClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdDebugGetRegionProperties {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mu.count++
		// Mock failure once.
		if !c.mu.failedOnce {
			c.mu.failedOnce = true
			return &tikvrpc.Response{}, nil
		}
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

func TestFastAnalyzeRetryRowCount(t *testing.T) {
	cli := &regionProperityClient{}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}

	var cls testutils.Cluster
	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			cls = c
		}),
		mockstore.WithClientHijacker(hijackClient),
	)
	require.NoError(t, err)
	defer func() {
		err := store.Close()
		require.NoError(t, err)
	}()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	defer dom.Close()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists retry_row_count")
	tk.MustExec("create table retry_row_count(a int primary key)")
	tblInfo, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("retry_row_count"))
	require.NoError(t, err)
	tid := tblInfo.Meta().ID
	require.NoError(t, dom.StatsHandle().Update(dom.InfoSchema()))
	tk.MustExec("set @@session.tidb_enable_fast_analyze=1")
	tk.MustExec("set @@session.tidb_build_stats_concurrency=1")
	tk.MustExec("set @@tidb_analyze_version = 1")
	for i := 0; i < 30; i++ {
		tk.MustExec(fmt.Sprintf("insert into retry_row_count values (%d)", i))
	}
	tableStart := tablecodec.GenTableRecordPrefix(tid)
	cls.SplitKeys(tableStart, tableStart.PrefixNext(), 6)
	// Flush the region cache first.
	tk.MustQuery("select * from retry_row_count")
	tk.MustExec("analyze table retry_row_count")
	row := tk.MustQuery(`show stats_meta where db_name = "test" and table_name = "retry_row_count"`).Rows()[0]
	require.Equal(t, "30", row[5])
}

func TestFailedAnalyzeRequest(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, index index_b(b))")
	tk.MustExec("set @@tidb_analyze_version = 1")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/buildStatsFromResult", `return(true)`))
	_, err := tk.Exec("analyze table t")
	require.Equal(t, "mock buildStatsFromResult error", err.Error())
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/buildStatsFromResult"))
}

func TestExtractTopN(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_extract_topn")
	tk.MustExec("use test_extract_topn")
	tk.MustExec("drop table if exists test_extract_topn")
	tk.MustExec("create table test_extract_topn(a int primary key, b int, index index_b(b))")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into test_extract_topn values (%d, %d)", i, i))
	}
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into test_extract_topn values (%d, 0)", i+10))
	}
	tk.MustExec("analyze table test_extract_topn")
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test_extract_topn"), model.NewCIStr("test_extract_topn"))
	require.NoError(t, err)
	tblInfo := table.Meta()
	tblStats := dom.StatsHandle().GetTableStats(tblInfo)
	colStats := tblStats.Columns[tblInfo.Columns[1].ID]
	require.Len(t, colStats.TopN.TopN, 10)
	item := colStats.TopN.TopN[0]
	require.Equal(t, uint64(11), item.Count)
	idxStats := tblStats.Indices[tblInfo.Indices[0].ID]
	require.Len(t, idxStats.TopN.TopN, 10)
	idxItem := idxStats.TopN.TopN[0]
	require.Equal(t, uint64(11), idxItem.Count)
	// The columns are: DBName, table name, column name, is index, value, count.
	tk.MustQuery("show stats_topn where column_name in ('b', 'index_b')").Sort().Check(testkit.Rows("test_extract_topn test_extract_topn  b 0 0 11",
		"test_extract_topn test_extract_topn  b 0 1 1",
		"test_extract_topn test_extract_topn  b 0 2 1",
		"test_extract_topn test_extract_topn  b 0 3 1",
		"test_extract_topn test_extract_topn  b 0 4 1",
		"test_extract_topn test_extract_topn  b 0 5 1",
		"test_extract_topn test_extract_topn  b 0 6 1",
		"test_extract_topn test_extract_topn  b 0 7 1",
		"test_extract_topn test_extract_topn  b 0 8 1",
		"test_extract_topn test_extract_topn  b 0 9 1",
		"test_extract_topn test_extract_topn  index_b 1 0 11",
		"test_extract_topn test_extract_topn  index_b 1 1 1",
		"test_extract_topn test_extract_topn  index_b 1 2 1",
		"test_extract_topn test_extract_topn  index_b 1 3 1",
		"test_extract_topn test_extract_topn  index_b 1 4 1",
		"test_extract_topn test_extract_topn  index_b 1 5 1",
		"test_extract_topn test_extract_topn  index_b 1 6 1",
		"test_extract_topn test_extract_topn  index_b 1 7 1",
		"test_extract_topn test_extract_topn  index_b 1 8 1",
		"test_extract_topn test_extract_topn  index_b 1 9 1",
	))
}

func TestHashInTopN(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b float, c decimal(30, 10), d varchar(20))")
	tk.MustExec(`insert into t values
				(1, 1.1, 11.1, "0110"),
				(2, 2.2, 22.2, "0110"),
				(3, 3.3, 33.3, "0110"),
				(4, 4.4, 44.4, "0440")`)
	for i := 0; i < 3; i++ {
		tk.MustExec("insert into t select * from t")
	}
	tk.MustExec("set @@tidb_analyze_version = 1")
	// get stats of normal analyze
	tk.MustExec("analyze table t")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	tblStats1 := dom.StatsHandle().GetTableStats(tblInfo).Copy()
	// get stats of fast analyze
	tk.MustExec("set @@tidb_enable_fast_analyze = 1")
	tk.MustExec("analyze table t")
	tblStats2 := dom.StatsHandle().GetTableStats(tblInfo).Copy()
	// check the hash for topn
	for _, col := range tblInfo.Columns {
		topn1 := tblStats1.Columns[col.ID].TopN.TopN
		cm2 := tblStats2.Columns[col.ID].TopN
		for _, topnMeta := range topn1 {
			count2, exists := cm2.QueryTopN(topnMeta.Encoded)
			require.True(t, exists)
			require.Equal(t, topnMeta.Count, count2)
		}
	}
}

func TestNormalAnalyzeOnCommonHandle(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3, t4")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("CREATE TABLE t1 (a int primary key, b int)")
	tk.MustExec("insert into t1 values(1,1), (2,2), (3,3)")
	tk.MustExec("CREATE TABLE t2 (a varchar(255) primary key, b int)")
	tk.MustExec("insert into t2 values(\"111\",1), (\"222\",2), (\"333\",3)")
	tk.MustExec("CREATE TABLE t3 (a int, b int, c int, primary key (a, b), key(c))")
	tk.MustExec("insert into t3 values(1,1,1), (2,2,2), (3,3,3)")

	// Version2 is tested in TestStatsVer2.
	tk.MustExec("set@@tidb_analyze_version=1")
	tk.MustExec("analyze table t1, t2, t3")

	tk.MustQuery(`show stats_buckets where table_name in ("t1", "t2", "t3")`).Sort().Check(testkit.Rows(
		"test t1  a 0 0 1 1 1 1 0",
		"test t1  a 0 1 2 1 2 2 0",
		"test t1  a 0 2 3 1 3 3 0",
		"test t1  b 0 0 1 1 1 1 0",
		"test t1  b 0 1 2 1 2 2 0",
		"test t1  b 0 2 3 1 3 3 0",
		"test t2  PRIMARY 1 0 1 1 111 111 0",
		"test t2  PRIMARY 1 1 2 1 222 222 0",
		"test t2  PRIMARY 1 2 3 1 333 333 0",
		"test t2  a 0 0 1 1 111 111 0",
		"test t2  a 0 1 2 1 222 222 0",
		"test t2  a 0 2 3 1 333 333 0",
		"test t2  b 0 0 1 1 1 1 0",
		"test t2  b 0 1 2 1 2 2 0",
		"test t2  b 0 2 3 1 3 3 0",
		"test t3  PRIMARY 1 0 1 1 (1, 1) (1, 1) 0",
		"test t3  PRIMARY 1 1 2 1 (2, 2) (2, 2) 0",
		"test t3  PRIMARY 1 2 3 1 (3, 3) (3, 3) 0",
		"test t3  a 0 0 1 1 1 1 0",
		"test t3  a 0 1 2 1 2 2 0",
		"test t3  a 0 2 3 1 3 3 0",
		"test t3  b 0 0 1 1 1 1 0",
		"test t3  b 0 1 2 1 2 2 0",
		"test t3  b 0 2 3 1 3 3 0",
		"test t3  c 0 0 1 1 1 1 0",
		"test t3  c 0 1 2 1 2 2 0",
		"test t3  c 0 2 3 1 3 3 0",
		"test t3  c 1 0 1 1 1 1 0",
		"test t3  c 1 1 2 1 2 2 0",
		"test t3  c 1 2 3 1 3 3 0"))
}

func TestDefaultValForAnalyze(t *testing.T) {
	t.Skip("skip race test")
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("drop database if exists test_default_val_for_analyze;")
	tk.MustExec("create database test_default_val_for_analyze;")
	tk.MustExec("use test_default_val_for_analyze")

	tk.MustExec("create table t (a int, key(a));")
	for i := 0; i < 2048; i++ {
		tk.MustExec("insert into t values (0)")
	}
	for i := 1; i < 4; i++ {
		tk.MustExec("insert into t values (?)", i)
	}
	tk.MustQuery("select @@tidb_enable_fast_analyze").Check(testkit.Rows("0"))
	tk.MustQuery("select @@session.tidb_enable_fast_analyze").Check(testkit.Rows("0"))
	tk.MustExec("analyze table t with 0 topn;")
	tk.MustQuery("explain format = 'brief' select * from t where a = 1").Check(testkit.Rows("IndexReader_6 512.00 root  index:IndexRangeScan_5",
		"└─IndexRangeScan_5 512.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false"))
	tk.MustQuery("explain format = 'brief' select * from t where a = 999").Check(testkit.Rows("IndexReader_6 0.00 root  index:IndexRangeScan_5",
		"└─IndexRangeScan_5 0.00 cop[tikv] table:t, index:a(a) range:[999,999], keep order:false"))

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, key(a));")
	for i := 0; i < 2048; i++ {
		tk.MustExec("insert into t values (0)")
	}
	for i := 1; i < 2049; i++ {
		tk.MustExec("insert into t values (?)", i)
	}
	tk.MustExec("analyze table t with 0 topn;")
	tk.MustQuery("explain format = 'brief' select * from t where a = 1").Check(testkit.Rows("IndexReader_6 1.00 root  index:IndexRangeScan_5",
		"└─IndexRangeScan_5 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false"))
}

func TestAnalyzeFullSamplingOnIndexWithVirtualColumnOrPrefixColumn(t *testing.T) {
	t.Skip("unstable, skip it and fix it before 20210624")
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists sampling_index_virtual_col")
	tk.MustExec("create table sampling_index_virtual_col(a int, b int as (a+1), index idx(b))")
	tk.MustExec("insert into sampling_index_virtual_col (a) values (1), (2), (null), (3), (4), (null), (5), (5), (5), (5)")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("analyze table sampling_index_virtual_col with 1 topn")
	tk.MustQuery("show stats_buckets where table_name = 'sampling_index_virtual_col' and column_name = 'idx'").Check(testkit.Rows(
		"test sampling_index_virtual_col  idx 1 0 1 1 2 2 0",
		"test sampling_index_virtual_col  idx 1 1 2 1 3 3 0",
		"test sampling_index_virtual_col  idx 1 2 3 1 4 4 0",
		"test sampling_index_virtual_col  idx 1 3 4 1 5 5 0"))
	tk.MustQuery("show stats_topn where table_name = 'sampling_index_virtual_col' and column_name = 'idx'").Check(testkit.Rows("test sampling_index_virtual_col  idx 1 6 4"))
	row := tk.MustQuery(`show stats_histograms where db_name = "test" and table_name = "sampling_index_virtual_col"`).Rows()[0]
	// The NDV.
	require.Equal(t, "5", row[6])
	// The NULLs.
	require.Equal(t, "2", row[7])
	tk.MustExec("drop table if exists sampling_index_prefix_col")
	tk.MustExec("create table sampling_index_prefix_col(a varchar(3), index idx(a(1)))")
	tk.MustExec("insert into sampling_index_prefix_col (a) values ('aa'), ('ab'), ('ac'), ('bb')")
	tk.MustExec("analyze table sampling_index_prefix_col with 1 topn")
	tk.MustQuery("show stats_buckets where table_name = 'sampling_index_prefix_col' and column_name = 'idx'").Check(testkit.Rows(
		"test sampling_index_prefix_col  idx 1 0 1 1 b b 0",
	))
	tk.MustQuery("show stats_topn where table_name = 'sampling_index_prefix_col' and column_name = 'idx'").Check(testkit.Rows("test sampling_index_prefix_col  idx 1 a 3"))
}

func TestSnapshotAnalyze(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index index_a(a))")
	is := tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	tid := tblInfo.ID
	tk.MustExec("insert into t values(1),(1),(1)")
	tk.MustExec("begin")
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	startTS1 := txn.StartTS()
	tk.MustExec("commit")
	tk.MustExec("insert into t values(2),(2),(2)")
	tk.MustExec("begin")
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	startTS2 := txn.StartTS()
	tk.MustExec("commit")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/injectAnalyzeSnapshot", fmt.Sprintf("return(%d)", startTS1)))
	tk.MustExec("analyze table t")
	rows := tk.MustQuery(fmt.Sprintf("select count, snapshot from mysql.stats_meta where table_id = %d", tid)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "3", rows[0][0])
	s1Str := rows[0][1].(string)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/injectAnalyzeSnapshot", fmt.Sprintf("return(%d)", startTS2)))
	tk.MustExec("analyze table t")
	rows = tk.MustQuery(fmt.Sprintf("select count, snapshot from mysql.stats_meta where table_id = %d", tid)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "6", rows[0][0])
	s2Str := rows[0][1].(string)
	require.True(t, s1Str != s2Str)
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/injectAnalyzeSnapshot", fmt.Sprintf("return(%d)", startTS1)))
	tk.MustExec("analyze table t")
	rows = tk.MustQuery(fmt.Sprintf("select count, snapshot from mysql.stats_meta where table_id = %d", tid)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "6", rows[0][0])
	s3Str := rows[0][1].(string)
	require.Equal(t, s2Str, s3Str)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/injectAnalyzeSnapshot"))
}

func TestAdjustSampleRateNote(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	statsHandle := domain.GetDomain(tk.Session().(sessionctx.Context)).StatsHandle()
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index index_a(a))")
	require.NoError(t, statsHandle.HandleDDLEvent(<-statsHandle.DDLEventCh()))
	is := tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	tid := tblInfo.ID
	tk.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 220000 where table_id=%d", tid))
	require.NoError(t, statsHandle.Update(is))
	result := tk.MustQuery("show stats_meta where table_name = 't'")
	require.Equal(t, "220000", result.Rows()[0][5])
	tk.MustExec("analyze table t")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Analyze use auto adjusted sample rate 0.500000 for table test.t."))
	tk.MustExec("insert into t values(1),(1),(1)")
	require.NoError(t, statsHandle.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, statsHandle.Update(is))
	result = tk.MustQuery("show stats_meta where table_name = 't'")
	require.Equal(t, "3", result.Rows()[0][5])
	tk.MustExec("analyze table t")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t."))
}

func TestFastAnalyze4GlobalStats(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database if not exists test_fast_gstats`)
	tk.MustExec("use test_fast_gstats")
	tk.MustExec("set @@session.tidb_enable_fast_analyze=1")
	tk.MustExec("set @@session.tidb_build_stats_concurrency=1")
	// test fast analyze in dynamic mode
	tk.MustExec("set @@session.tidb_analyze_version = 2;")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic';")
	tk.MustExec("drop table if exists test_fast_gstats;")
	tk.MustExec("create table test_fast_gstats(a int, b int) PARTITION BY HASH(a) PARTITIONS 2;")
	tk.MustExec("insert into test_fast_gstats values(1,1),(3,3),(4,4),(2,2),(5,5);")
	err := tk.ExecToErr("analyze table test_fast_gstats;")
	require.EqualError(t, err, "Fast analyze hasn't reached General Availability and only support analyze version 1 currently.")
}

func TestAnalyzeIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (id int, v int, primary key(id), index k(v))")
	tk.MustExec("insert into t1(id, v) values(1, 2), (2, 2), (3, 2), (4, 2), (5, 1), (6, 3), (7, 4)")
	tk.MustExec("set @@tidb_analyze_version=1")
	tk.MustExec("analyze table t1 index k")
	require.Greater(t, len(tk.MustQuery("show stats_buckets where table_name = 't1' and column_name = 'k' and is_index = 1").Rows()), 0)
	tk.MustExec("set @@tidb_analyze_version=default")
	tk.MustExec("analyze table t1")
	require.Greater(t, len(tk.MustQuery("show stats_topn where table_name = 't1' and column_name = 'k' and is_index = 1").Rows()), 0)

	func() {
		defer tk.MustExec("set @@session.tidb_enable_fast_analyze=0")
		tk.MustExec("drop stats t1")
		tk.MustExec("set @@session.tidb_enable_fast_analyze=1")
		tk.MustExec("set @@tidb_analyze_version=1")
		tk.MustExec("analyze table t1 index k")
		require.Greater(t, len(tk.MustQuery("show stats_buckets where table_name = 't1' and column_name = 'k' and is_index = 1").Rows()), 1)
	}()
}

func TestAnalyzeIncremental(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.Session().GetSessionVars().EnableStreaming = false
	testAnalyzeIncremental(tk, t, dom)
}

func TestAnalyzeIncrementalStreaming(t *testing.T) {
	t.Skip("unistore hasn't support streaming yet.")
	store, clean := testkit.CreateMockStore(t)
	dom, err := session.BootstrapSession(store)
	require.NoError(t, err)

	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableStreaming = true
	testAnalyzeIncremental(tk, t, dom)
}

func testAnalyzeIncremental(tk *testkit.TestKit, t *testing.T, dom *domain.Domain) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key(a), index idx(b))")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows())
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1 0", "test t  idx 1 0 1 1 1 1 0"))
	tk.MustExec("insert into t values (2,2)")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1 0", "test t  a 0 1 2 1 2 2 0", "test t  idx 1 0 1 1 1 1 0", "test t  idx 1 1 2 1 2 2 0"))
	tk.MustExec("analyze incremental table t index")
	// Result should not change.
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1 0", "test t  a 0 1 2 1 2 2 0", "test t  idx 1 0 1 1 1 1 0", "test t  idx 1 1 2 1 2 2 0"))

	// Test analyze incremental with feedback.
	tk.MustExec("insert into t values (3,3)")
	oriProbability := statistics.FeedbackProbability.Load()
	oriMinLogCount := handle.MinLogScanCount.Load()
	defer func() {
		statistics.FeedbackProbability.Store(oriProbability)
		handle.MinLogScanCount.Store(oriMinLogCount)
	}()
	statistics.FeedbackProbability.Store(1)
	handle.MinLogScanCount.Store(0)
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := table.Meta()
	tk.MustQuery("select * from t use index(idx) where b = 3")
	tk.MustQuery("select * from t where a > 1")
	h := dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.DumpStatsFeedbackToKV())
	require.NoError(t, h.HandleUpdateStats(is))
	require.NoError(t, h.Update(is))
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1 0", "test t  a 0 1 3 0 2 2147483647 0", "test t  idx 1 0 1 1 1 1 0", "test t  idx 1 1 2 1 2 2 0"))
	tblStats := h.GetTableStats(tblInfo)
	val, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx, nil, types.NewIntDatum(3))
	require.NoError(t, err)
	require.Equal(t, uint64(1), tblStats.Indices[tblInfo.Indices[0].ID].QueryBytes(val))
	require.False(t, statistics.IsAnalyzed(tblStats.Indices[tblInfo.Indices[0].ID].Flag))
	require.False(t, statistics.IsAnalyzed(tblStats.Columns[tblInfo.Columns[0].ID].Flag))

	tk.MustExec("analyze incremental table t index")
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t  a 0 0 1 1 1 1 0", "test t  a 0 1 2 1 2 2 0", "test t  a 0 2 3 1 3 3 0",
		"test t  idx 1 0 1 1 1 1 0", "test t  idx 1 1 2 1 2 2 0", "test t  idx 1 2 3 1 3 3 0"))
	tblStats = h.GetTableStats(tblInfo)
	require.Equal(t, uint64(1), tblStats.Indices[tblInfo.Indices[0].ID].QueryBytes(val))

	// test analyzeIndexIncremental for global-level stats;
	tk.MustExec("set @@session.tidb_analyze_version = 1;")
	tk.MustQuery("select @@tidb_analyze_version").Check(testkit.Rows("1"))
	tk.MustExec("set @@tidb_partition_prune_mode = 'static';")
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t (a int, b int, primary key(a), index idx(b)) partition by range (a) (
		partition p0 values less than (10),
		partition p1 values less than (20),
		partition p2 values less than (30)
	);`)
	tk.MustExec("analyze incremental table t index")
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustQuery("show stats_buckets").Check(testkit.Rows())
	tk.MustExec("insert into t values (1,1)")
	tk.MustExec("analyze incremental table t index")
	tk.MustQuery("show warnings").Check(testkit.Rows()) // no warning
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t p0 a 0 0 1 1 1 1 0", "test t p0 idx 1 0 1 1 1 1 0"))
	tk.MustExec("insert into t values (2,2)")
	tk.MustExec("analyze incremental table t index")
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustQuery("show stats_buckets").Check(testkit.Rows("test t p0 a 0 0 1 1 1 1 0", "test t p0 a 0 1 2 1 2 2 0", "test t p0 idx 1 0 1 1 1 1 0", "test t p0 idx 1 1 2 1 2 2 0"))
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic';")
	tk.MustExec("insert into t values (11,11)")
	err = tk.ExecToErr("analyze incremental table t index")
	require.Equal(t, "[stats]: global statistics for partitioned tables unavailable in ANALYZE INCREMENTAL", err.Error())
}

func TestIssue27429(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table test.t(id int, value varchar(20) charset utf8mb4 collate utf8mb4_general_ci, value1 varchar(20) charset utf8mb4 collate utf8mb4_bin)")
	tk.MustExec("insert into test.t values (1, 'abc', 'abc '),(4, 'Abc', 'abc'),(3,'def', 'def ');")

	tk.MustQuery("select upper(group_concat(distinct value order by 1)) from test.t;").Check(testkit.Rows("ABC,DEF"))
	tk.MustQuery("select upper(group_concat(distinct value)) from test.t;").Check(testkit.Rows("ABC,DEF"))
}

func TestIssue20874(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("delete from mysql.stats_histograms")
	tk.MustExec("create table t (a char(10) collate utf8mb4_unicode_ci not null, b char(20) collate utf8mb4_general_ci not null, key idxa(a), key idxb(b))")
	tk.MustExec("insert into t values ('#', 'C'), ('$', 'c'), ('a', 'a')")
	tk.MustExec("set @@tidb_analyze_version=1")
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 0 1 1 \x02\xd2 \x02\xd2 0",
		"test t  a 0 1 2 1 \x0e\x0f \x0e\x0f 0",
		"test t  a 0 2 3 1 \x0e3 \x0e3 0",
		"test t  b 0 0 1 1 \x00A \x00A 0",
		"test t  b 0 1 3 2 \x00C \x00C 0",
		"test t  idxa 1 0 1 1 \x02\xd2 \x02\xd2 0",
		"test t  idxa 1 1 2 1 \x0e\x0f \x0e\x0f 0",
		"test t  idxa 1 2 3 1 \x0e3 \x0e3 0",
		"test t  idxb 1 0 1 1 \x00A \x00A 0",
		"test t  idxb 1 1 3 2 \x00C \x00C 0",
	))
	tk.MustQuery("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, correlation from mysql.stats_histograms").Sort().Check(testkit.Rows(
		"0 1 3 0 9 1 1",
		"0 2 2 0 9 1 -0.5",
		"1 1 3 0 0 1 0",
		"1 2 2 0 0 1 0",
	))
	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("analyze table t")
	tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 \x02\xd2 1",
		"test t  a 0 \x0e\x0f 1",
		"test t  a 0 \x0e3 1",
		"test t  b 0 \x00A 1",
		"test t  b 0 \x00C 2",
		"test t  idxa 1 \x02\xd2 1",
		"test t  idxa 1 \x0e\x0f 1",
		"test t  idxa 1 \x0e3 1",
		"test t  idxb 1 \x00A 1",
		"test t  idxb 1 \x00C 2",
	))
	tk.MustQuery("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, correlation from mysql.stats_histograms").Sort().Check(testkit.Rows(
		"0 1 3 0 6 2 1",
		"0 2 2 0 6 2 -0.5",
		"1 1 3 0 6 2 0",
		"1 2 2 0 6 2 0",
	))
}

func TestAnalyzeClusteredIndexPrimary(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t0(a varchar(20), primary key(a) clustered)")
	tk.MustExec("create table t1(a varchar(20), primary key(a))")
	tk.MustExec("insert into t0 values('1111')")
	tk.MustExec("insert into t1 values('1111')")
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("analyze table t0 index primary")
	tk.MustExec("analyze table t1 index primary")
	tk.MustQuery("show stats_buckets").Check(testkit.Rows(
		"test t0  PRIMARY 1 0 1 1 1111 1111 0",
		"test t1  PRIMARY 1 0 1 1 1111 1111 0"))
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("analyze table t0")
	tk.MustExec("analyze table t1")
	tk.MustQuery("show stats_topn").Sort().Check(testkit.Rows(""+
		"test t0  PRIMARY 1 1111 1",
		"test t0  a 0 1111 1",
		"test t1  PRIMARY 1 1111 1",
		"test t1  a 0 1111 1"))
}

func TestAnalyzeSamplingWorkPanic(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12)")
	tk.MustExec("split table t between (-9223372036854775808) and (9223372036854775807) regions 12")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockAnalyzeSamplingBuildWorkerPanic", "return(1)"))
	err := tk.ExecToErr("analyze table t")
	require.NotNil(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockAnalyzeSamplingBuildWorkerPanic"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockAnalyzeSamplingMergeWorkerPanic", "return(1)"))
	err = tk.ExecToErr("analyze table t")
	require.NotNil(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockAnalyzeSamplingMergeWorkerPanic"))
}
