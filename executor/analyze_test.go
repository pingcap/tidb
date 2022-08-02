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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

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
	require.Equal(t, 0, tbl.Columns[1].TopN.Num())
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
	var colsInfo []*model.ColumnInfo // nolint: prealloc
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
	require.NotNil(t, err)
	require.Equal(t, "mock buildStatsFromResult error", err.Error())
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/buildStatsFromResult"))
}

func TestExtractTopN(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version=1")
	defer tk.MustExec("set @@tidb_analyze_version=2")
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
	tk.MustQuery("explain format = 'brief' select * from t where a = 1").Check(testkit.Rows("IndexReader 512.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 512.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false"))
	tk.MustQuery("explain format = 'brief' select * from t where a = 999").Check(testkit.Rows("IndexReader 0.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 0.00 cop[tikv] table:t, index:a(a) range:[999,999], keep order:false"))

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, key(a));")
	for i := 0; i < 2048; i++ {
		tk.MustExec("insert into t values (0)")
	}
	for i := 1; i < 2049; i++ {
		tk.MustExec("insert into t values (?)", i)
	}
	tk.MustExec("analyze table t with 0 topn;")
	tk.MustQuery("explain format = 'brief' select * from t where a = 1").Check(testkit.Rows("IndexReader 1.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false"))
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
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Analyze use auto adjusted sample rate 0.500000 for table test.t"))
	tk.MustExec("insert into t values(1),(1),(1)")
	require.NoError(t, statsHandle.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, statsHandle.Update(is))
	result = tk.MustQuery("show stats_meta where table_name = 't'")
	require.Equal(t, "3", result.Rows()[0][5])
	tk.MustExec("analyze table t")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t"))
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
	require.EqualError(t, err, "Fast analyze hasn't reached General Availability and only support analyze version 1 currently")
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
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_analyze_version = 1")
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

func TestIssue20874(t *testing.T) {
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

func TestSmallTableAnalyzeV2(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/calcSampleRateByStorageCount", "return(1)"))
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("create table small_table_inject_pd(a int)")
	tk.MustExec("insert into small_table_inject_pd values(1), (2), (3), (4), (5)")
	tk.MustExec("analyze table small_table_inject_pd")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.small_table_inject_pd"))
	tk.MustExec(`
create table small_table_inject_pd_with_partition(
	a int
) partition by range(a) (
	partition p0 values less than (5),
	partition p1 values less than (10),
	partition p2 values less than (15)
)`)
	tk.MustExec("insert into small_table_inject_pd_with_partition values(1), (6), (11)")
	tk.MustExec("analyze table small_table_inject_pd_with_partition")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.small_table_inject_pd_with_partition's partition p0",
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.small_table_inject_pd_with_partition's partition p1",
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.small_table_inject_pd_with_partition's partition p2",
	))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/calcSampleRateByStorageCount"))
}

func TestSavedAnalyzeOptions(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal1 := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal1))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")
	originalVal2 := tk.MustQuery("select @@tidb_auto_analyze_ratio").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_ratio = %v", originalVal2))
	}()
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.01")
	originalVal3 := handle.AutoAnalyzeMinCnt
	defer func() {
		handle.AutoAnalyzeMinCnt = originalVal3
	}()
	handle.AutoAnalyzeMinCnt = 0

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("create table t(a int, b int, c int, primary key(a), key idx(b))")
	tk.MustExec("insert into t values (1,1,1),(2,1,2),(3,1,3),(4,1,4),(5,1,5),(6,1,6),(7,7,7),(8,8,8),(9,9,9)")

	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	tk.MustExec("analyze table t with 1 topn, 2 buckets")
	is := dom.InfoSchema()
	tk.MustQuery("select * from t where b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms())
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	tbl := h.GetTableStats(tableInfo)
	lastVersion := tbl.Version
	col0 := tbl.Columns[tableInfo.Columns[0].ID]
	require.Equal(t, 2, len(col0.Buckets))
	col1 := tbl.Columns[tableInfo.Columns[1].ID]
	require.Equal(t, 1, len(col1.TopN.TopN))
	require.Equal(t, 2, len(col1.Buckets))
	col2 := tbl.Columns[tableInfo.Columns[2].ID]
	require.Equal(t, 2, len(col2.Buckets))
	rs := tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tbl.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	require.Equal(t, "1", rs.Rows()[0][1])

	// auto-analyze uses the table-level options
	tk.MustExec("insert into t values (10,10,10)")
	require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.Nil(t, h.Update(is))
	h.HandleAutoAnalyze(is)
	tbl = h.GetTableStats(tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	lastVersion = tbl.Version
	col0 = tbl.Columns[tableInfo.Columns[0].ID]
	require.Equal(t, 2, len(col0.Buckets))

	// manual analyze uses the table-level persisted options by merging the new options
	tk.MustExec("analyze table t columns a,b with 1 samplerate, 3 buckets")
	tbl = h.GetTableStats(tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	lastVersion = tbl.Version
	col0 = tbl.Columns[tableInfo.Columns[0].ID]
	require.Equal(t, 3, len(col0.Buckets))
	tk.MustQuery("select * from t where b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms())
	col1 = tbl.Columns[tableInfo.Columns[1].ID]
	require.Equal(t, 1, len(col1.TopN.TopN))
	col2 = tbl.Columns[tableInfo.Columns[2].ID]
	require.Less(t, col2.LastUpdateVersion, col0.LastUpdateVersion) // not updated since removed from list
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(tbl.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "1", rs.Rows()[0][0])
	require.Equal(t, "3", rs.Rows()[0][1])
	require.Equal(t, "1", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	colIDStrs := strings.Join([]string{strconv.FormatInt(tableInfo.Columns[0].ID, 10), strconv.FormatInt(tableInfo.Columns[1].ID, 10)}, ",")
	require.Equal(t, colIDStrs, rs.Rows()[0][4])

	// disable option persistence
	tk.MustExec("set global tidb_persist_analyze_options = false")
	// manual analyze will neither use the pre-persisted options nor persist new options
	tk.MustExec("analyze table t with 2 topn")
	tbl = h.GetTableStats(tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	col0 = tbl.Columns[tableInfo.Columns[0].ID]
	require.NotEqual(t, 3, len(col0.Buckets))
	rs = tk.MustQuery("select topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tbl.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.NotEqual(t, "2", rs.Rows()[0][0])
}

func TestSavedPartitionAnalyzeOptions(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	createTable := `CREATE TABLE t (a int, b int, c varchar(10), primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20)
)`
	tk.MustExec(createTable)
	tk.MustExec("insert into t values (1,1,1),(2,1,2),(3,1,3),(4,1,4),(5,1,5),(6,1,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(11,11,11),(12,12,12),(13,13,13),(14,14,14)")

	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	// analyze partition only sets options of partition
	tk.MustExec("analyze table t partition p0 with 1 topn, 3 buckets")
	is := dom.InfoSchema()
	tk.MustQuery("select * from t where b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms())
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)
	p0 := h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
	lastVersion := p0.Version
	require.Equal(t, 3, len(p0.Columns[tableInfo.Columns[0].ID].Buckets))
	rs := tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "3", rs.Rows()[0][0])
	require.Equal(t, "1", rs.Rows()[0][1])
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "-1", rs.Rows()[0][1])

	// merge partition & table level options
	tk.MustExec("analyze table t columns a,b with 0 topn, 2 buckets")
	tk.MustQuery("select * from t where b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms())
	p0 = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
	p1 := h.GetPartitionStats(tableInfo, pi.Definitions[1].ID)
	require.Greater(t, p0.Version, lastVersion)
	lastVersion = p0.Version
	require.Equal(t, 2, len(p0.Columns[tableInfo.Columns[0].ID].Buckets))
	require.Equal(t, 2, len(p1.Columns[tableInfo.Columns[0].ID].Buckets))
	// check column c is not analyzed
	require.Less(t, p0.Columns[tableInfo.Columns[2].ID].LastUpdateVersion, p0.Columns[tableInfo.Columns[0].ID].LastUpdateVersion)
	require.Less(t, p1.Columns[tableInfo.Columns[2].ID].LastUpdateVersion, p1.Columns[tableInfo.Columns[0].ID].LastUpdateVersion)
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
	require.Equal(t, "0", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	colIDStrsAB := strings.Join([]string{strconv.FormatInt(tableInfo.Columns[0].ID, 10), strconv.FormatInt(tableInfo.Columns[1].ID, 10)}, ",")
	require.Equal(t, colIDStrsAB, rs.Rows()[0][4])
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
	require.Equal(t, "0", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][4])
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p1.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
	require.Equal(t, "0", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][4])

	// analyze partition only updates this partition, and set different collist
	tk.MustExec("analyze table t partition p1 columns a,c with 1 buckets")
	tk.MustQuery("select * from t where b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms())
	p0 = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
	p1 = h.GetPartitionStats(tableInfo, pi.Definitions[1].ID)
	require.Equal(t, p0.Version, lastVersion)
	require.Greater(t, p1.Version, lastVersion)
	lastVersion = p1.Version
	require.Equal(t, 1, len(p1.Columns[tableInfo.Columns[0].ID].Buckets))
	require.Equal(t, 2, len(p0.Columns[tableInfo.Columns[0].ID].Buckets))
	// only column c of p1 is re-analyzed
	require.Equal(t, 1, len(p1.Columns[tableInfo.Columns[2].ID].Buckets))
	require.NotEqual(t, 1, len(p0.Columns[tableInfo.Columns[2].ID].Buckets))
	colIDStrsABC := strings.Join([]string{strconv.FormatInt(tableInfo.Columns[0].ID, 10), strconv.FormatInt(tableInfo.Columns[1].ID, 10), strconv.FormatInt(tableInfo.Columns[2].ID, 10)}, ",")
	rs = tk.MustQuery("select buckets,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][1])
	rs = tk.MustQuery("select buckets,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p1.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "1", rs.Rows()[0][0])
	require.Equal(t, colIDStrsABC, rs.Rows()[0][1])
	rs = tk.MustQuery("select buckets,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][1])

	// analyze partition without options uses saved partition options
	tk.MustExec("analyze table t partition p0")
	p0 = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
	require.Greater(t, p0.Version, lastVersion)
	lastVersion = p0.Version
	require.Equal(t, 2, len(p0.Columns[tableInfo.Columns[0].ID].Buckets))
	rs = tk.MustQuery("select buckets from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	rs = tk.MustQuery("select buckets from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])

	// merge options of statement's, partition's and table's
	tk.MustExec("analyze table t partition p0 with 3 buckets")
	p0 = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
	require.Greater(t, p0.Version, lastVersion)
	require.Equal(t, 3, len(p0.Columns[tableInfo.Columns[0].ID].Buckets))
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "3", rs.Rows()[0][1])
	require.Equal(t, "0", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][4])

	// add new partitions, use table options as default
	tk.MustExec("ALTER TABLE t ADD PARTITION (PARTITION p2 VALUES LESS THAN (30))")
	tk.MustExec("insert into t values (21,21,21),(22,22,22),(23,23,23),(24,24,24)")
	tk.MustExec("analyze table t partition p2")
	is = dom.InfoSchema()
	table, err = is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo = table.Meta()
	pi = tableInfo.GetPartitionInfo()
	p2 := h.GetPartitionStats(tableInfo, pi.Definitions[2].ID)
	require.Equal(t, 2, len(p2.Columns[tableInfo.Columns[0].ID].Buckets))
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p2.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
	require.Equal(t, "0", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][4])
	rs = tk.MustQuery("select sample_rate,buckets,topn,column_choice,column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
	require.Equal(t, "0", rs.Rows()[0][2])
	require.Equal(t, "LIST", rs.Rows()[0][3])
	require.Equal(t, colIDStrsAB, rs.Rows()[0][4])

	// set analyze version back to 1, will not use persisted
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("analyze table t partition p2")
	pi = tableInfo.GetPartitionInfo()
	p2 = h.GetPartitionStats(tableInfo, pi.Definitions[2].ID)
	require.NotEqual(t, 2, len(p2.Columns[tableInfo.Columns[0].ID].Buckets))

	// drop column
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("alter table t drop column b")
	tk.MustExec("analyze table t")
	colIDStrsA := strings.Join([]string{strconv.FormatInt(tableInfo.Columns[0].ID, 10)}, ",")
	colIDStrsAC := strings.Join([]string{strconv.FormatInt(tableInfo.Columns[0].ID, 10), strconv.FormatInt(tableInfo.Columns[2].ID, 10)}, ",")
	rs = tk.MustQuery("select column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, colIDStrsA, rs.Rows()[0][0])
	rs = tk.MustQuery("select column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, colIDStrsA, rs.Rows()[0][0])
	rs = tk.MustQuery("select column_ids from mysql.analyze_options where table_id=" + strconv.FormatInt(p1.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, colIDStrsAC, rs.Rows()[0][0])

	// drop partition
	tk.MustExec("alter table t drop partition p1")
	is = dom.InfoSchema() // refresh infoschema
	require.Nil(t, h.GCStats(is, time.Duration(0)))
	rs = tk.MustQuery("select * from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	rs = tk.MustQuery("select * from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	rs = tk.MustQuery("select * from mysql.analyze_options where table_id=" + strconv.FormatInt(p1.PhysicalID, 10))
	require.Equal(t, 0, len(rs.Rows()))

	// drop table
	tk.MustExec("drop table t")
	is = dom.InfoSchema() // refresh infoschema
	require.Nil(t, h.GCStats(is, time.Duration(0)))
	rs = tk.MustQuery("select * from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	//require.Equal(t, len(rs.Rows()), 0) TODO
	rs = tk.MustQuery("select * from mysql.analyze_options where table_id=" + strconv.FormatInt(p0.PhysicalID, 10))
	require.Equal(t, 0, len(rs.Rows()))
}

func TestSavedAnalyzeOptionsForMultipleTables(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	tk.MustExec("create table t1(a int, b int, c int, primary key(a), key idx(b))")
	tk.MustExec("insert into t1 values (1,1,1),(2,1,2),(3,1,3),(4,1,4),(5,1,5),(6,1,6),(7,7,7),(8,8,8),(9,9,9)")
	tk.MustExec("create table t2(a int, b int, c int, primary key(a), key idx(b))")
	tk.MustExec("insert into t2 values (1,1,1),(2,1,2),(3,1,3),(4,1,4),(5,1,5),(6,1,6),(7,7,7),(8,8,8),(9,9,9)")

	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()

	tk.MustExec("analyze table t1 with 1 topn, 3 buckets")
	tk.MustExec("analyze table t2 with 0 topn, 2 buckets")
	tk.MustExec("analyze table t1,t2 with 2 topn")
	is := dom.InfoSchema()
	table1, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.NoError(t, err)
	table2, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.NoError(t, err)
	tableInfo1 := table1.Meta()
	tableInfo2 := table2.Meta()
	tblStats1 := h.GetTableStats(tableInfo1)
	tblStats2 := h.GetTableStats(tableInfo2)
	tbl1Col0 := tblStats1.Columns[tableInfo1.Columns[0].ID]
	tbl2Col0 := tblStats2.Columns[tableInfo2.Columns[0].ID]
	require.Equal(t, 3, len(tbl1Col0.Buckets))
	require.Equal(t, 2, len(tbl2Col0.Buckets))
	rs := tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo1.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "3", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo2.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
}

func TestSavedAnalyzeColumnOptions(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal1 := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal1))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")
	originalVal2 := tk.MustQuery("select @@tidb_auto_analyze_ratio").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_ratio = %v", originalVal2))
	}()
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.01")
	originalVal3 := handle.AutoAnalyzeMinCnt
	defer func() {
		handle.AutoAnalyzeMinCnt = originalVal3
	}()
	handle.AutoAnalyzeMinCnt = 0
	originalVal4 := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal4))
	}()
	tk.MustExec("set global tidb_enable_column_tracking = 1")

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("create table t(a int, b int, c int)")
	tk.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4)")

	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	tk.MustExec("select * from t where b > 1")
	require.NoError(t, h.DumpColStatsUsageToKV())
	tk.MustExec("analyze table t predicate columns")
	require.NoError(t, h.LoadNeededHistograms())
	tblStats := h.GetTableStats(tblInfo)
	lastVersion := tblStats.Version
	// column b is analyzed
	require.Greater(t, lastVersion, tblStats.Columns[tblInfo.Columns[0].ID].LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.Columns[tblInfo.Columns[1].ID].LastUpdateVersion)
	require.Greater(t, lastVersion, tblStats.Columns[tblInfo.Columns[2].ID].LastUpdateVersion)
	tk.MustQuery(fmt.Sprintf("select column_choice, column_ids from mysql.analyze_options where table_id = %v", tblInfo.ID)).Check(testkit.Rows("PREDICATE "))

	tk.MustExec("select * from t where c > 1")
	require.NoError(t, h.DumpColStatsUsageToKV())
	// manually analyze uses the saved option(predicate columns).
	tk.MustExec("analyze table t")
	require.NoError(t, h.LoadNeededHistograms())
	tblStats = h.GetTableStats(tblInfo)
	require.Less(t, lastVersion, tblStats.Version)
	lastVersion = tblStats.Version
	// column b, c are analyzed
	require.Greater(t, lastVersion, tblStats.Columns[tblInfo.Columns[0].ID].LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.Columns[tblInfo.Columns[1].ID].LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.Columns[tblInfo.Columns[2].ID].LastUpdateVersion)

	tk.MustExec("insert into t values (5,5,5),(6,6,6)")
	require.Nil(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.Nil(t, h.Update(is))
	// auto analyze uses the saved option(predicate columns).
	h.HandleAutoAnalyze(is)
	tblStats = h.GetTableStats(tblInfo)
	require.Less(t, lastVersion, tblStats.Version)
	lastVersion = tblStats.Version
	// column b, c are analyzed
	require.Greater(t, lastVersion, tblStats.Columns[tblInfo.Columns[0].ID].LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.Columns[tblInfo.Columns[1].ID].LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.Columns[tblInfo.Columns[2].ID].LastUpdateVersion)

	tk.MustExec("analyze table t columns a")
	tblStats = h.GetTableStats(tblInfo)
	require.Less(t, lastVersion, tblStats.Version)
	lastVersion = tblStats.Version
	// column a is analyzed
	require.Equal(t, lastVersion, tblStats.Columns[tblInfo.Columns[0].ID].LastUpdateVersion)
	require.Greater(t, lastVersion, tblStats.Columns[tblInfo.Columns[1].ID].LastUpdateVersion)
	require.Greater(t, lastVersion, tblStats.Columns[tblInfo.Columns[2].ID].LastUpdateVersion)
	tk.MustQuery(fmt.Sprintf("select column_choice, column_ids from mysql.analyze_options where table_id = %v", tblInfo.ID)).Check(testkit.Rows(fmt.Sprintf("LIST %v", tblInfo.Columns[0].ID)))

	tk.MustExec("analyze table t all columns")
	tblStats = h.GetTableStats(tblInfo)
	require.Less(t, lastVersion, tblStats.Version)
	lastVersion = tblStats.Version
	// column a, b, c are analyzed
	require.Equal(t, lastVersion, tblStats.Columns[tblInfo.Columns[0].ID].LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.Columns[tblInfo.Columns[1].ID].LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.Columns[tblInfo.Columns[2].ID].LastUpdateVersion)
	tk.MustQuery(fmt.Sprintf("select column_choice, column_ids from mysql.analyze_options where table_id = %v", tblInfo.ID)).Check(testkit.Rows("ALL "))
}

func TestAnalyzeColumnsWithPrimaryKey(t *testing.T) {
	for _, val := range []model.ColumnChoice{model.ColumnList, model.PredicateColumns} {
		func(choice model.ColumnChoice) {
			store, dom, clean := testkit.CreateMockStoreAndDomain(t)
			defer clean()

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int, c int primary key)")
			tk.MustExec("insert into t values (1,1,1), (1,1,2), (2,2,3), (2,2,4), (3,3,5), (4,3,6), (5,4,7), (6,4,8), (null,null,9)")
			require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns a with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t",
					"Warning 1105 Columns c are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case model.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("set global tidb_enable_column_tracking = 1")
				tk.MustExec("select * from t where a > 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, "a", rows[0][3])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 2, len(rows))
			require.Equal(t, "a", rows[0][3])
			require.Equal(t, "c", rows[1][3])

			tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 9"))
			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t  a 0 1 2",
					"test t  a 0 2 2",
					"test t  c 0 1 1",
					"test t  c 0 2 1"))
			tk.MustQuery(fmt.Sprintf("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 1 6 1 8 2 1",
					"0 2 0 0 8 0 0", // column b is not analyzed
					"0 3 9 0 9 2 1",
				))
			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t  a 0 0 3 1 3 5 0",
					"test t  a 0 1 4 1 6 6 0",
					"test t  c 0 0 4 1 3 6 0",
					"test t  c 0 1 7 1 7 9 0"))
		}(val)
	}
}

func TestAnalyzeColumnsWithIndex(t *testing.T) {
	for _, val := range []model.ColumnChoice{model.ColumnList, model.PredicateColumns} {
		func(choice model.ColumnChoice) {
			store, dom, clean := testkit.CreateMockStoreAndDomain(t)
			defer clean()

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int, c int, d int, index idx_b_d(b, d))")
			tk.MustExec("insert into t values (1,1,null,1), (2,1,9,1), (1,1,8,1), (2,2,7,2), (1,3,7,3), (2,4,6,4), (1,4,6,5), (2,4,6,5), (1,5,6,5)")
			require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns c with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t",
					"Warning 1105 Columns b,d are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case model.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("set global tidb_enable_column_tracking = 1")
				tk.MustExec("select * from t where c > 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, "c", rows[0][3])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 3, len(rows))
			require.Equal(t, "b", rows[0][3])
			require.Equal(t, "c", rows[1][3])
			require.Equal(t, "d", rows[2][3])

			tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 9"))
			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t  b 0 1 3",
					"test t  b 0 4 3",
					"test t  c 0 6 4",
					"test t  c 0 7 2",
					"test t  d 0 1 3",
					"test t  d 0 5 3",
					"test t  idx_b_d 1 (1, 1) 3",
					"test t  idx_b_d 1 (4, 5) 2"))
			tk.MustQuery(fmt.Sprintf("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 1 0 0 9 0 0", // column a is not analyzed
					"0 2 5 0 9 2 1",
					"0 3 4 1 8 2 -0.07",
					"0 4 5 0 9 2 1",
					"1 1 6 0 18 2 0"))
			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t  b 0 0 2 1 2 3 0",
					"test t  b 0 1 3 1 5 5 0",
					"test t  c 0 0 2 1 8 9 0",
					"test t  d 0 0 2 1 2 3 0",
					"test t  d 0 1 3 1 4 4 0",
					"test t  idx_b_d 1 0 3 1 (2, 2) (4, 4) 0",
					"test t  idx_b_d 1 1 4 1 (5, 5) (5, 5) 0"))
		}(val)
	}
}

func TestAnalyzeColumnsWithClusteredIndex(t *testing.T) {
	for _, val := range []model.ColumnChoice{model.ColumnList, model.PredicateColumns} {
		func(choice model.ColumnChoice) {
			store, dom, clean := testkit.CreateMockStoreAndDomain(t)
			defer clean()

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int, c int, d int, primary key(b, d) clustered)")
			tk.MustExec("insert into t values (1,1,null,1), (2,2,9,2), (1,3,8,3), (2,4,7,4), (1,5,7,5), (2,6,6,6), (1,7,6,7), (2,8,6,8), (1,9,6,9)")
			require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns c with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t",
					"Warning 1105 Columns b,d are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case model.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("set global tidb_enable_column_tracking = 1")
				tk.MustExec("select * from t where c > 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, "c", rows[0][3])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 3, len(rows))
			require.Equal(t, "b", rows[0][3])
			require.Equal(t, "c", rows[1][3])
			require.Equal(t, "d", rows[2][3])

			tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 9"))
			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t  PRIMARY 1 (1, 1) 1",
					"test t  PRIMARY 1 (2, 2) 1",
					"test t  b 0 1 1",
					"test t  b 0 2 1",
					"test t  c 0 6 4",
					"test t  c 0 7 2",
					"test t  d 0 1 1",
					"test t  d 0 2 1"))
			tk.MustQuery(fmt.Sprintf("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 1 0 0 9 0 0", // column a is not analyzed
					"0 2 9 0 9 2 1",
					"0 3 4 1 8 2 -0.07",
					"0 4 9 0 9 2 1",
					"1 1 9 0 18 2 0"))
			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t  PRIMARY 1 0 4 1 (3, 3) (6, 6) 0",
					"test t  PRIMARY 1 1 7 1 (7, 7) (9, 9) 0",
					"test t  b 0 0 4 1 3 6 0",
					"test t  b 0 1 7 1 7 9 0",
					"test t  c 0 0 2 1 8 9 0",
					"test t  d 0 0 4 1 3 6 0",
					"test t  d 0 1 7 1 7 9 0"))
		}(val)
	}
}

func TestAnalyzeColumnsWithDynamicPartitionTable(t *testing.T) {
	for _, val := range []model.ColumnChoice{model.ColumnList, model.PredicateColumns} {
		func(choice model.ColumnChoice) {
			store, dom, clean := testkit.CreateMockStoreAndDomain(t)
			defer clean()

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			tk.MustExec("create table t (a int, b int, c int, index idx(c)) partition by range (a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
			tk.MustExec("insert into t values (1,2,1), (2,4,1), (3,6,1), (4,8,2), (4,8,2), (5,10,3), (5,10,4), (5,10,5), (null,null,6), (11,22,7), (12,24,8), (13,26,9), (14,28,10), (15,30,11), (16,32,12), (16,32,13), (16,32,13), (16,32,14), (17,34,14), (17,34,14)")
			require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID
			defs := tbl.Meta().Partition.Definitions
			p0ID := defs[0].ID
			p1ID := defs[1].ID

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns a with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0",
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p1",
					"Warning 1105 Columns c are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case model.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("set global tidb_enable_column_tracking = 1")
				tk.MustExec("select * from t where a < 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, []interface{}{"test", "t", "global", "a"}, rows[0][:4])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 6, len(rows))
			require.Equal(t, []interface{}{"test", "t", "global", "a"}, rows[0][:4])
			require.Equal(t, []interface{}{"test", "t", "global", "c"}, rows[1][:4])
			require.Equal(t, []interface{}{"test", "t", "p0", "a"}, rows[2][:4])
			require.Equal(t, []interface{}{"test", "t", "p0", "c"}, rows[3][:4])
			require.Equal(t, []interface{}{"test", "t", "p1", "a"}, rows[4][:4])
			require.Equal(t, []interface{}{"test", "t", "p1", "c"}, rows[5][:4])

			rows = tk.MustQuery("show stats_meta where db_name = 'test' and table_name = 't'").Sort().Rows()
			require.Equal(t, 3, len(rows))
			require.Equal(t, []interface{}{"test", "t", "global", "0", "20"}, append(rows[0][:3], rows[0][4:]...))
			require.Equal(t, []interface{}{"test", "t", "p0", "0", "9"}, append(rows[1][:3], rows[1][4:]...))
			require.Equal(t, []interface{}{"test", "t", "p1", "0", "11"}, append(rows[2][:3], rows[2][4:]...))

			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't' and is_index = 0").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t global a 0 16 4",
					"test t global a 0 5 3",
					"test t global c 0 1 3",
					"test t global c 0 14 3",
					"test t p0 a 0 4 2",
					"test t p0 a 0 5 3",
					"test t p0 c 0 1 3",
					"test t p0 c 0 2 2",
					"test t p1 a 0 16 4",
					"test t p1 a 0 17 2",
					"test t p1 c 0 13 2",
					"test t p1 c 0 14 3"))

			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't' and is_index = 1").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t global idx 1 1 3",
					"test t global idx 1 14 3",
					"test t p0 idx 1 1 3",
					"test t p0 idx 1 2 2",
					"test t p1 idx 1 13 2",
					"test t p1 idx 1 14 3"))

			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't' and is_index = 0").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t global a 0 0 5 2 1 4 0",
					"test t global a 0 1 12 2 17 17 0",
					"test t global c 0 0 6 1 2 6 0",
					"test t global c 0 1 14 2 13 13 0",
					"test t p0 a 0 0 2 1 1 2 0",
					"test t p0 a 0 1 3 1 3 3 0",
					"test t p0 c 0 0 3 1 3 5 0",
					"test t p0 c 0 1 4 1 6 6 0",
					"test t p1 a 0 0 3 1 11 13 0",
					"test t p1 a 0 1 5 1 14 15 0",
					"test t p1 c 0 0 4 1 7 10 0",
					"test t p1 c 0 1 6 1 11 12 0"))

			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't' and is_index = 1").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t global idx 1 0 6 1 2 6 0",
					"test t global idx 1 1 14 2 13 13 0",
					"test t p0 idx 1 0 3 1 3 5 0",
					"test t p0 idx 1 1 4 1 6 6 0",
					"test t p1 idx 1 0 4 1 7 10 0",
					"test t p1 idx 1 1 6 1 11 12 0"))

			tk.MustQuery("select table_id, is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms order by table_id, is_index, hist_id asc").Check(
				testkit.Rows(fmt.Sprintf("%d 0 1 12 1 19 2 0", tblID), // global, a
					fmt.Sprintf("%d 0 3 14 0 20 2 0", tblID), // global, c
					fmt.Sprintf("%d 1 1 14 0 0 2 0", tblID),  // global, idx
					fmt.Sprintf("%d 0 1 5 1 8 2 1", p0ID),    // p0, a
					fmt.Sprintf("%d 0 2 0 0 8 0 0", p0ID),    // p0, b, not analyzed
					fmt.Sprintf("%d 0 3 6 0 9 2 1", p0ID),    // p0, c
					fmt.Sprintf("%d 1 1 6 0 9 2 0", p0ID),    // p0, idx
					fmt.Sprintf("%d 0 1 7 0 11 2 1", p1ID),   // p1, a
					fmt.Sprintf("%d 0 2 0 0 11 0 0", p1ID),   // p1, b, not analyzed
					fmt.Sprintf("%d 0 3 8 0 11 2 1", p1ID),   // p1, c
					fmt.Sprintf("%d 1 1 8 0 11 2 0", p1ID),   // p1, idx
				))
		}(val)
	}
}

func TestIssue34228(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`USE test`)
	tk.MustExec(`DROP TABLE IF EXISTS Issue34228`)
	tk.MustExec(`CREATE TABLE Issue34228 (id bigint NOT NULL, dt datetime NOT NULL) PARTITION BY RANGE COLUMNS(dt) (PARTITION p202201 VALUES LESS THAN ("2022-02-01"), PARTITION p202202 VALUES LESS THAN ("2022-03-01"))`)
	tk.MustExec(`INSERT INTO Issue34228 VALUES (1, '2022-02-01 00:00:02'), (2, '2022-02-01 00:00:02')`)
	tk.MustExec(`SET @@global.tidb_analyze_version = 1`)
	tk.MustExec(`SET @@session.tidb_partition_prune_mode = 'static'`)
	tk.MustExec(`ANALYZE TABLE Issue34228`)
	tk.MustExec(`SET @@session.tidb_partition_prune_mode = 'dynamic'`)
	tk.MustExec(`ANALYZE TABLE Issue34228`)
	tk.MustQuery(`SELECT * FROM Issue34228`).Sort().Check(testkit.Rows("1 2022-02-01 00:00:02", "2 2022-02-01 00:00:02"))
	// Needs a second run to hit the issue
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`USE test`)
	tk2.MustExec(`DROP TABLE IF EXISTS Issue34228`)
	tk2.MustExec(`CREATE TABLE Issue34228 (id bigint NOT NULL, dt datetime NOT NULL) PARTITION BY RANGE COLUMNS(dt) (PARTITION p202201 VALUES LESS THAN ("2022-02-01"), PARTITION p202202 VALUES LESS THAN ("2022-03-01"))`)
	tk2.MustExec(`INSERT INTO Issue34228 VALUES (1, '2022-02-01 00:00:02'), (2, '2022-02-01 00:00:02')`)
	tk2.MustExec(`SET @@global.tidb_analyze_version = 1`)
	tk2.MustExec(`SET @@session.tidb_partition_prune_mode = 'static'`)
	tk2.MustExec(`ANALYZE TABLE Issue34228`)
	tk2.MustExec(`SET @@session.tidb_partition_prune_mode = 'dynamic'`)
	tk2.MustExec(`ANALYZE TABLE Issue34228`)
	tk2.MustQuery(`SELECT * FROM Issue34228`).Sort().Check(testkit.Rows("1 2022-02-01 00:00:02", "2 2022-02-01 00:00:02"))
}

func TestAnalyzeColumnsWithStaticPartitionTable(t *testing.T) {
	for _, val := range []model.ColumnChoice{model.ColumnList, model.PredicateColumns} {
		func(choice model.ColumnChoice) {
			store, dom, clean := testkit.CreateMockStoreAndDomain(t)
			defer clean()

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			tk.MustExec("create table t (a int, b int, c int, index idx(c)) partition by range (a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
			tk.MustExec("insert into t values (1,2,1), (2,4,1), (3,6,1), (4,8,2), (4,8,2), (5,10,3), (5,10,4), (5,10,5), (null,null,6), (11,22,7), (12,24,8), (13,26,9), (14,28,10), (15,30,11), (16,32,12), (16,32,13), (16,32,13), (16,32,14), (17,34,14), (17,34,14)")
			require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			defs := tbl.Meta().Partition.Definitions
			p0ID := defs[0].ID
			p1ID := defs[1].ID

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns a with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0",
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p1",
					"Warning 1105 Columns c are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case model.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("set global tidb_enable_column_tracking = 1")
				tk.MustExec("select * from t where a < 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, []interface{}{"test", "t", "global", "a"}, rows[0][:4])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 4, len(rows))
			require.Equal(t, []interface{}{"test", "t", "p0", "a"}, rows[0][:4])
			require.Equal(t, []interface{}{"test", "t", "p0", "c"}, rows[1][:4])
			require.Equal(t, []interface{}{"test", "t", "p1", "a"}, rows[2][:4])
			require.Equal(t, []interface{}{"test", "t", "p1", "c"}, rows[3][:4])

			rows = tk.MustQuery("show stats_meta where db_name = 'test' and table_name = 't'").Sort().Rows()
			require.Equal(t, 2, len(rows))
			require.Equal(t, []interface{}{"test", "t", "p0", "0", "9"}, append(rows[0][:3], rows[0][4:]...))
			require.Equal(t, []interface{}{"test", "t", "p1", "0", "11"}, append(rows[1][:3], rows[1][4:]...))

			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't' and is_index = 0").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t p0 a 0 4 2",
					"test t p0 a 0 5 3",
					"test t p0 c 0 1 3",
					"test t p0 c 0 2 2",
					"test t p1 a 0 16 4",
					"test t p1 a 0 17 2",
					"test t p1 c 0 13 2",
					"test t p1 c 0 14 3"))

			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't' and is_index = 1").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t p0 idx 1 1 3",
					"test t p0 idx 1 2 2",
					"test t p1 idx 1 13 2",
					"test t p1 idx 1 14 3"))

			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't' and is_index = 0").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t p0 a 0 0 2 1 1 2 0",
					"test t p0 a 0 1 3 1 3 3 0",
					"test t p0 c 0 0 3 1 3 5 0",
					"test t p0 c 0 1 4 1 6 6 0",
					"test t p1 a 0 0 3 1 11 13 0",
					"test t p1 a 0 1 5 1 14 15 0",
					"test t p1 c 0 0 4 1 7 10 0",
					"test t p1 c 0 1 6 1 11 12 0"))

			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't' and is_index = 1").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t p0 idx 1 0 3 1 3 5 0",
					"test t p0 idx 1 1 4 1 6 6 0",
					"test t p1 idx 1 0 4 1 7 10 0",
					"test t p1 idx 1 1 6 1 11 12 0"))

			tk.MustQuery("select table_id, is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms order by table_id, is_index, hist_id asc").Check(
				testkit.Rows(fmt.Sprintf("%d 0 1 5 1 8 2 1", p0ID), // p0, a
					fmt.Sprintf("%d 0 2 0 0 8 0 0", p0ID),  // p0, b, not analyzed
					fmt.Sprintf("%d 0 3 6 0 9 2 1", p0ID),  // p0, c
					fmt.Sprintf("%d 1 1 6 0 9 2 0", p0ID),  // p0, idx
					fmt.Sprintf("%d 0 1 7 0 11 2 1", p1ID), // p1, a
					fmt.Sprintf("%d 0 2 0 0 11 0 0", p1ID), // p1, b, not analyzed
					fmt.Sprintf("%d 0 3 8 0 11 2 1", p1ID), // p1, c
					fmt.Sprintf("%d 1 1 8 0 11 2 0", p1ID), // p1, idx
				))
		}(val)
	}
}

func TestAnalyzeColumnsWithExtendedStats(t *testing.T) {
	for _, val := range []model.ColumnChoice{model.ColumnList, model.PredicateColumns} {
		func(choice model.ColumnChoice) {
			store, dom, clean := testkit.CreateMockStoreAndDomain(t)
			defer clean()

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("set @@tidb_enable_extended_stats = on")
			tk.MustExec("create table t (a int, b int, c int)")
			tk.MustExec("alter table t add stats_extended s1 correlation(b,c)")
			tk.MustExec("insert into t values (5,1,1), (4,2,2), (3,3,3), (2,4,4), (1,5,5)")
			require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns b with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t",
					"Warning 1105 Columns c are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case model.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("set global tidb_enable_column_tracking = 1")
				tk.MustExec("select * from t where b > 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, "b", rows[0][3])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}
			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 2, len(rows))
			require.Equal(t, "b", rows[0][3])
			require.Equal(t, "c", rows[1][3])

			tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 5"))
			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t  b 0 1 1",
					"test t  b 0 2 1",
					"test t  c 0 1 1",
					"test t  c 0 2 1"))
			tk.MustQuery(fmt.Sprintf("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 1 0 0 5 0 0", // column a is not analyzed
					"0 2 5 0 5 2 1",
					"0 3 5 0 5 2 1",
				))
			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t  b 0 0 2 1 3 4 0",
					"test t  b 0 1 3 1 5 5 0",
					"test t  c 0 0 2 1 3 4 0",
					"test t  c 0 1 3 1 5 5 0"))
			rows = tk.MustQuery("show stats_extended where db_name = 'test' and table_name = 't'").Rows()
			require.Equal(t, 1, len(rows))
			require.Equal(t, []interface{}{"test", "t", "s1", "[b,c]", "correlation", "1.000000"}, rows[0][:len(rows[0])-1])
		}(val)
	}
}

func TestAnalyzeColumnsWithVirtualColumnIndex(t *testing.T) {
	for _, val := range []model.ColumnChoice{model.ColumnList, model.PredicateColumns} {
		func(choice model.ColumnChoice) {
			store, dom, clean := testkit.CreateMockStoreAndDomain(t)
			defer clean()

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int, c int as (b+1), index idx(c))")
			tk.MustExec("insert into t (a,b) values (1,1), (2,2), (3,3), (4,4), (5,4), (6,5), (7,5), (8,5), (null,null)")
			require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns b with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t",
					"Warning 1105 Columns c are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
				))
			case model.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("set global tidb_enable_column_tracking = 1")
				tk.MustExec("select * from t where b > 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, "b", rows[0][3])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}
			// virtual column c is skipped when dumping stats into disk, so only the stats of column b are updated
			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Rows()
			require.Equal(t, 1, len(rows))
			require.Equal(t, "b", rows[0][3])

			tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 9"))
			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t  b 0 4 2",
					"test t  b 0 5 3",
					"test t  idx 1 5 2",
					"test t  idx 1 6 3"))
			tk.MustQuery(fmt.Sprintf("select is_index, hist_id, distinct_count, null_count, stats_ver, truncate(correlation,2) from mysql.stats_histograms where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 1 0 0 0 0", // column a is not analyzed
					"0 2 5 1 2 1",
					"0 3 0 0 0 0", // column c is not analyzed
					"1 1 5 1 2 0"))
			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t  b 0 0 2 1 1 2 0",
					"test t  b 0 1 3 1 3 3 0",
					"test t  idx 1 0 2 1 2 3 0",
					"test t  idx 1 1 3 1 4 4 0"))
		}(val)
	}
}

func TestAnalyzeColumnsAfterAnalyzeAll(t *testing.T) {
	for _, val := range []model.ColumnChoice{model.ColumnList, model.PredicateColumns} {
		func(choice model.ColumnChoice) {
			store, dom, clean := testkit.CreateMockStoreAndDomain(t)
			defer clean()

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int)")
			tk.MustExec("insert into t (a,b) values (1,1), (1,1), (2,2), (2,2), (3,3), (4,4)")
			require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			tk.MustExec("analyze table t with 2 topn, 2 buckets")
			tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 6"))
			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t  a 0 1 2",
					"test t  a 0 2 2",
					"test t  b 0 1 2",
					"test t  b 0 2 2"))
			tk.MustQuery(fmt.Sprintf("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 1 4 0 6 2 1",
					"0 2 4 0 6 2 1"))
			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t  a 0 0 2 1 3 4 0",
					"test t  b 0 0 2 1 3 4 0"))

			tk.MustExec("insert into t (a,b) values (1,1), (6,6)")
			require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns b with 2 topn, 2 buckets")
			case model.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("set global tidb_enable_column_tracking = 1")
				tk.MustExec("select * from t where b > 1")
				require.NoError(t, h.DumpColStatsUsageToKV())
				rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_used_at is not null").Rows()
				require.Equal(t, 1, len(rows))
				require.Equal(t, "b", rows[0][3])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			// Column a is not analyzed in second ANALYZE. We keep the outdated stats of column a rather than delete them.
			tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 8"))
			tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_idx, value, count
				testkit.Rows("test t  a 0 1 2",
					"test t  a 0 2 2",
					"test t  b 0 1 3",
					"test t  b 0 2 2"))
			tk.MustQuery(fmt.Sprintf("select is_index, hist_id, distinct_count, null_count, tot_col_size, stats_ver, truncate(correlation,2) from mysql.stats_histograms where table_id = %d", tblID)).Sort().Check(
				testkit.Rows("0 1 4 0 8 2 1", // tot_col_size of column a is updated to 8 by DumpStatsDeltaToKV
					"0 2 5 0 8 2 0.76"))
			tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(
				// db, tbl, part, col, is_index, bucket_id, count, repeats, lower, upper, ndv
				testkit.Rows("test t  a 0 0 2 1 3 4 0",
					"test t  b 0 0 2 1 3 4 0",
					"test t  b 0 1 3 1 6 6 0"))
			tk.MustQuery(fmt.Sprintf("select hist_id from mysql.stats_histograms where version = (select version from mysql.stats_meta where table_id = %d)", tblID)).Check(testkit.Rows("2"))
		}(val)
	}
}

func TestAnalyzeColumnsErrorAndWarning(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")

	// analyze version 1 doesn't support `ANALYZE COLUMNS c1, ..., cn`/`ANALYZE PREDICATE COLUMNS` currently
	tk.MustExec("set @@tidb_analyze_version = 1")
	err := tk.ExecToErr("analyze table t columns a")
	require.Equal(t, "Only the analyze version 2 supports analyzing the specified columns", err.Error())
	err = tk.ExecToErr("analyze table t predicate columns")
	require.Equal(t, "Only the analyze version 2 supports analyzing predicate columns", err.Error())

	tk.MustExec("set @@tidb_analyze_version = 2")
	// invalid column
	err = tk.ExecToErr("analyze table t columns c")
	terr := errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.ErrAnalyzeMissColumn), terr.Code())

	// If no predicate column is collected, analyze predicate columns gives a warning and falls back to analyze all columns.
	tk.MustExec("analyze table t predicate columns")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t",
		"Warning 1105 No predicate column has been collected yet for table test.t so all columns are analyzed",
	))
	rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Rows()
	require.Equal(t, 2, len(rows))

	for _, val := range []model.ColumnChoice{model.ColumnList, model.PredicateColumns} {
		func(choice model.ColumnChoice) {
			tk.MustExec("set @@tidb_analyze_version = 1")
			tk.MustExec("analyze table t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns b")
			case model.PredicateColumns:
				originalVal := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
				defer func() {
					tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal))
				}()
				tk.MustExec("set global tidb_enable_column_tracking = 1")
				tk.MustExec("select * from t where b > 1")
				require.NoError(t, dom.StatsHandle().DumpColStatsUsageToKV())
				tk.MustExec("analyze table t predicate columns")
			}
			tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
				"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t",
				"Warning 1105 Table test.t has version 1 statistics so all the columns must be analyzed to overwrite the current statistics",
			))
		}(val)
	}
}

func TestRecordHistoryStatsAfterAnalyze(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set global tidb_enable_historical_stats = 0")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10))")

	h := dom.StatsHandle()
	is := dom.InfoSchema()
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	// 1. switch off the tidb_enable_historical_stats, and there is no records in table `mysql.stats_history`
	rows := tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where table_id = '%d'", tableInfo.Meta().ID)).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 0)

	tk.MustExec("analyze table t with 2 topn")
	rows = tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where table_id = '%d'", tableInfo.Meta().ID)).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 0)

	// 2. switch on the tidb_enable_historical_stats and do analyze
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	defer tk.MustExec("set global tidb_enable_historical_stats = 0")
	tk.MustExec("analyze table t with 2 topn")
	rows = tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_history where table_id = '%d'", tableInfo.Meta().ID)).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.GreaterOrEqual(t, num, 1)

	// 3. dump current stats json
	dumpJSONTable, err := h.DumpStatsToJSON("test", tableInfo.Meta(), nil)
	require.NoError(t, err)
	jsOrigin, _ := json.Marshal(dumpJSONTable)

	// 4. get the historical stats json
	rows = tk.MustQuery(fmt.Sprintf("select * from mysql.stats_history where table_id = '%d' and create_time = ("+
		"select create_time from mysql.stats_history where table_id = '%d' order by create_time desc limit 1) "+
		"order by seq_no", tableInfo.Meta().ID, tableInfo.Meta().ID)).Rows()
	num = len(rows)
	require.GreaterOrEqual(t, num, 1)
	data := make([][]byte, num)
	for i, row := range rows {
		data[i] = []byte(row[1].(string))
	}
	jsonTbl, err := handle.BlocksToJSONTable(data)
	require.NoError(t, err)
	jsCur, err := json.Marshal(jsonTbl)
	require.NoError(t, err)
	// 5. historical stats must be equal to the current stats
	require.JSONEq(t, string(jsOrigin), string(jsCur))
}

func TestRecordHistoryStatsMetaAfterAnalyze(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set global tidb_enable_historical_stats = 0")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("analyze table test.t")

	h := dom.StatsHandle()
	is := dom.InfoSchema()
	tableInfo, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)

	// 1. switch off the tidb_enable_historical_stats, and there is no record in table `mysql.stats_meta_history`
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_meta_history where table_id = '%d'", tableInfo.Meta().ID)).Check(testkit.Rows("0"))
	// insert demo tuples, and there is no record either.
	insertNums := 5
	for i := 0; i < insertNums; i++ {
		tk.MustExec("insert into test.t (a,b) values (1,1), (2,2), (3,3)")
		err := h.DumpStatsDeltaToKV(handle.DumpDelta)
		require.NoError(t, err)
	}
	tk.MustQuery(fmt.Sprintf("select count(*) from mysql.stats_meta_history where table_id = '%d'", tableInfo.Meta().ID)).Check(testkit.Rows("0"))

	// 2. switch on the tidb_enable_historical_stats and insert tuples to produce count/modifyCount delta change.
	tk.MustExec("set global tidb_enable_historical_stats = 1")
	defer tk.MustExec("set global tidb_enable_historical_stats = 0")

	for i := 0; i < insertNums; i++ {
		tk.MustExec("insert into test.t (a,b) values (1,1), (2,2), (3,3)")
		err := h.DumpStatsDeltaToKV(handle.DumpDelta)
		require.NoError(t, err)
	}
	tk.MustQuery(fmt.Sprintf("select modify_count, count from mysql.stats_meta_history where table_id = '%d' order by create_time", tableInfo.Meta().ID)).Sort().Check(
		testkit.Rows("18 18", "21 21", "24 24", "27 27", "30 30"))
}

func checkAnalyzeStatus(t *testing.T, tk *testkit.TestKit, jobInfo, status, failReason, comment string, timeLimit int64) {
	rows := tk.MustQuery("show analyze status where table_schema = 'test' and table_name = 't' and partition_name = ''").Rows()
	require.Equal(t, 1, len(rows), comment)
	require.Equal(t, jobInfo, rows[0][3], comment)
	require.Equal(t, status, rows[0][7], comment)
	require.Equal(t, failReason, rows[0][8], comment)
	if timeLimit <= 0 {
		return
	}
	const layout = "2006-01-02 15:04:05"
	startTime, err := time.Parse(layout, rows[0][5].(string))
	require.NoError(t, err, comment)
	endTime, err := time.Parse(layout, rows[0][6].(string))
	require.NoError(t, err, comment)
	require.Less(t, endTime.Sub(startTime), time.Duration(timeLimit)*time.Second, comment)
}

func testKillAutoAnalyze(t *testing.T, ver int) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()
	tk.MustExec(fmt.Sprintf("set @@tidb_analyze_version = %v", ver))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1,2), (3,4)")
	is := dom.InfoSchema()
	h := dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t")
	tk.MustExec("insert into t values (5,6), (7,8), (9, 10)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	require.NoError(t, h.Update(is))
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	lastVersion := h.GetTableStats(tableInfo).Version
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")
	jobInfo := "auto analyze "
	if ver == 1 {
		jobInfo += "columns"
	} else {
		jobInfo += "table all columns with 256 buckets, 500 topn, 1 samplerate"
	}
	// kill auto analyze when it is pending/running/finished
	for _, status := range []string{"pending", "running", "finished"} {
		func() {
			comment := fmt.Sprintf("kill %v analyze job", status)
			tk.MustExec("delete from mysql.analyze_jobs")
			mockAnalyzeStatus := "github.com/pingcap/tidb/executor/mockKill" + strings.Title(status)
			if status == "running" {
				mockAnalyzeStatus += "V" + strconv.Itoa(ver)
			}
			mockAnalyzeStatus += "AnalyzeJob"
			require.NoError(t, failpoint.Enable(mockAnalyzeStatus, "return"))
			defer func() {
				require.NoError(t, failpoint.Disable(mockAnalyzeStatus))
			}()
			if status == "pending" || status == "running" {
				mockSlowAnalyze := "github.com/pingcap/tidb/executor/mockSlowAnalyzeV" + strconv.Itoa(ver)
				require.NoError(t, failpoint.Enable(mockSlowAnalyze, "return"))
				defer func() {
					require.NoError(t, failpoint.Disable(mockSlowAnalyze))
				}()
			}
			require.True(t, h.HandleAutoAnalyze(is), comment)
			currentVersion := h.GetTableStats(tableInfo).Version
			if status == "finished" {
				// If we kill a finished job, after kill command the status is still finished and the table stats are updated.
				checkAnalyzeStatus(t, tk, jobInfo, "finished", "<nil>", comment, -1)
				require.Greater(t, currentVersion, lastVersion, comment)
			} else {
				// If we kill a pending/running job, after kill command the status is failed and the table stats are not updated.
				// We expect the killed analyze stops quickly. Specifically, end_time - start_time < 10s.
				checkAnalyzeStatus(t, tk, jobInfo, "failed", executor.ErrQueryInterrupted.Error(), comment, 10)
				require.Equal(t, currentVersion, lastVersion, comment)
			}
		}()
	}
}

func TestKillAutoAnalyzeV1(t *testing.T) {
	testKillAutoAnalyze(t, 1)
}

func TestKillAutoAnalyzeV2(t *testing.T) {
	testKillAutoAnalyze(t, 2)
}

func TestKillAutoAnalyzeIndex(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	handle.AutoAnalyzeMinCnt = 0
	defer func() {
		handle.AutoAnalyzeMinCnt = 1000
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1,2), (3,4)")
	is := dom.InfoSchema()
	h := dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	tk.MustExec("analyze table t")
	tk.MustExec("alter table t add index idx(b)")
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	lastVersion := h.GetTableStats(tblInfo).Version
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")
	const jobInfo = "auto analyze index idx"
	// kill auto analyze when it is pending/running/finished
	for _, status := range []string{"pending", "running", "finished"} {
		func() {
			comment := fmt.Sprintf("kill %v analyze job", status)
			tk.MustExec("delete from mysql.analyze_jobs")
			mockAnalyzeStatus := "github.com/pingcap/tidb/executor/mockKill" + strings.Title(status)
			if status == "running" {
				mockAnalyzeStatus += "AnalyzeIndexJob"
			} else {
				mockAnalyzeStatus += "AnalyzeJob"
			}
			require.NoError(t, failpoint.Enable(mockAnalyzeStatus, "return"))
			defer func() {
				require.NoError(t, failpoint.Disable(mockAnalyzeStatus))
			}()
			if status == "pending" || status == "running" {
				require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockSlowAnalyzeIndex", "return"))
				defer func() {
					require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockSlowAnalyzeIndex"))
				}()
			}
			require.True(t, h.HandleAutoAnalyze(dom.InfoSchema()), comment)
			currentVersion := h.GetTableStats(tblInfo).Version
			if status == "finished" {
				// If we kill a finished job, after kill command the status is still finished and the index stats are updated.
				checkAnalyzeStatus(t, tk, jobInfo, "finished", "<nil>", comment, -1)
				require.Greater(t, currentVersion, lastVersion, comment)
			} else {
				// If we kill a pending/running job, after kill command the status is failed and the index stats are not updated.
				// We expect the killed analyze stops quickly. Specifically, end_time - start_time < 10s.
				checkAnalyzeStatus(t, tk, jobInfo, "failed", executor.ErrQueryInterrupted.Error(), comment, 10)
				require.Equal(t, currentVersion, lastVersion, comment)
			}
		}()
	}
}

func TestAnalyzeJob(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	for _, result := range []string{statistics.AnalyzeFinished, statistics.AnalyzeFailed} {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("delete from mysql.analyze_jobs")
		se := tk.Session()
		job := &statistics.AnalyzeJob{
			DBName:        "test",
			TableName:     "t",
			PartitionName: "",
			JobInfo:       "table all columns with 256 buckets, 500 topn, 1 samplerate",
		}
		executor.AddNewAnalyzeJob(se, job)
		require.NotNil(t, job.ID)
		rows := tk.MustQuery("show analyze status").Rows()
		require.Len(t, rows, 1)
		require.Equal(t, job.DBName, rows[0][0])
		require.Equal(t, job.TableName, rows[0][1])
		require.Equal(t, job.PartitionName, rows[0][2])
		require.Equal(t, job.JobInfo, rows[0][3])
		require.Equal(t, "0", rows[0][4])
		require.Equal(t, "<nil>", rows[0][5])
		require.Equal(t, "<nil>", rows[0][6])
		require.Equal(t, statistics.AnalyzePending, rows[0][7])
		require.Equal(t, "<nil>", rows[0][8])
		serverInfo, err := infosync.GetServerInfo()
		require.NoError(t, err)
		addr := fmt.Sprintf("%s:%d", serverInfo.IP, serverInfo.Port)
		require.Equal(t, addr, rows[0][9])
		connID := strconv.FormatUint(tk.Session().GetSessionVars().ConnectionID, 10)
		require.Equal(t, connID, rows[0][10])

		executor.StartAnalyzeJob(se, job)
		rows = tk.MustQuery("show analyze status").Rows()
		checkTime := func(val interface{}) {
			str, ok := val.(string)
			require.True(t, ok)
			_, err := time.Parse("2006-01-02 15:04:05", str)
			require.NoError(t, err)
		}
		checkTime(rows[0][5])
		require.Equal(t, statistics.AnalyzeRunning, rows[0][7])

		// UpdateAnalyzeJob requires the interval between two updates to mysql.analyze_jobs is more than 5 second.
		// Hence we fake last dump time as 10 second ago in order to make update to mysql.analyze_jobs happen.
		lastDumpTime := time.Now().Add(-10 * time.Second)
		job.Progress.SetLastDumpTime(lastDumpTime)
		const smallCount int64 = 100
		executor.UpdateAnalyzeJob(se, job, smallCount)
		// Delta count doesn't reach threshold so we don't dump it to mysql.analyze_jobs
		require.Equal(t, smallCount, job.Progress.GetDeltaCount())
		require.Equal(t, lastDumpTime, job.Progress.GetLastDumpTime())
		rows = tk.MustQuery("show analyze status").Rows()
		require.Equal(t, "0", rows[0][4])

		const largeCount int64 = 15000000
		executor.UpdateAnalyzeJob(se, job, largeCount)
		// Delta count reaches threshold so we dump it to mysql.analyze_jobs and update last dump time.
		require.Equal(t, int64(0), job.Progress.GetDeltaCount())
		require.True(t, job.Progress.GetLastDumpTime().After(lastDumpTime))
		lastDumpTime = job.Progress.GetLastDumpTime()
		rows = tk.MustQuery("show analyze status").Rows()
		require.Equal(t, strconv.FormatInt(smallCount+largeCount, 10), rows[0][4])

		executor.UpdateAnalyzeJob(se, job, largeCount)
		// We have just updated mysql.analyze_jobs in the previous step so we don't update it until 5 second passes or the analyze job is over.
		require.Equal(t, largeCount, job.Progress.GetDeltaCount())
		require.Equal(t, lastDumpTime, job.Progress.GetLastDumpTime())
		rows = tk.MustQuery("show analyze status").Rows()
		require.Equal(t, strconv.FormatInt(smallCount+largeCount, 10), rows[0][4])

		var analyzeErr error
		if result == statistics.AnalyzeFailed {
			analyzeErr = errors.Errorf("analyze meets error")
		}
		executor.FinishAnalyzeJob(se, job, analyzeErr)
		rows = tk.MustQuery("show analyze status").Rows()
		require.Equal(t, strconv.FormatInt(smallCount+2*largeCount, 10), rows[0][4])
		checkTime(rows[0][6])
		require.Equal(t, result, rows[0][7])
		if result == statistics.AnalyzeFailed {
			require.Equal(t, analyzeErr.Error(), rows[0][8])
		} else {
			require.Equal(t, "<nil>", rows[0][8])
		}
		// process_id is set to NULL after the analyze job is finished/failed.
		require.Equal(t, "<nil>", rows[0][10])
	}
}

func TestInsertAnalyzeJobWithLongInstance(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("delete from mysql.analyze_jobs")
	job := &statistics.AnalyzeJob{
		DBName:        "test",
		TableName:     "t",
		PartitionName: "",
		JobInfo:       "table all columns with 256 buckets, 500 topn, 1 samplerate",
	}
	h := dom.StatsHandle()
	instance := "xxxtidb-tidb-0.xxxtidb-tidb-peer.xxxx-xx-1234-xxx-123456-1-321.xyz:4000"
	require.NoError(t, h.InsertAnalyzeJob(job, instance, 1))
	rows := tk.MustQuery("show analyze status").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, instance, rows[0][9])
}

func TestShowAanalyzeStatusJobInfo(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal1 := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	originalVal2 := tk.MustQuery("select @@tidb_enable_column_tracking").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal1))
		tk.MustExec(fmt.Sprintf("set global tidb_enable_column_tracking = %v", originalVal2))
	}()
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("set global tidb_persist_analyze_options = 0")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, d int, index idx_b_d(b, d))")
	tk.MustExec("insert into t values (1,1,null,1), (2,1,9,1), (1,1,8,1), (2,2,7,2), (1,3,7,3), (2,4,6,4), (1,4,6,5), (2,4,6,5), (1,5,6,5)")
	tk.MustExec("analyze table t columns c with 2 topn, 2 buckets")
	checkJobInfo := func(expected string) {
		rows := tk.MustQuery("show analyze status where table_schema = 'test' and table_name = 't'").Rows()
		require.Equal(t, 1, len(rows))
		require.Equal(t, expected, rows[0][3])
		tk.MustExec("delete from mysql.analyze_jobs")
	}
	checkJobInfo("analyze table columns b, c, d with 2 buckets, 2 topn, 1 samplerate")
	tk.MustExec("set global tidb_enable_column_tracking = 1")
	tk.MustExec("select * from t where c > 1")
	h := dom.StatsHandle()
	require.NoError(t, h.DumpColStatsUsageToKV())
	tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
	checkJobInfo("analyze table columns b, c, d with 2 buckets, 2 topn, 1 samplerate")
	tk.MustExec("analyze table t")
	checkJobInfo("analyze table all columns with 256 buckets, 500 topn, 1 samplerate")
	tk.MustExec("set global tidb_persist_analyze_options = 1")
	tk.MustExec("analyze table t columns a with 1 topn, 3 buckets")
	checkJobInfo("analyze table columns a, b, d with 3 buckets, 1 topn, 1 samplerate")
	tk.MustExec("analyze table t")
	checkJobInfo("analyze table columns a, b, d with 3 buckets, 1 topn, 1 samplerate")
}

func TestAnalyzePartitionTableWithDynamicMode(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
	createTable := `CREATE TABLE t (a int, b int, c varchar(10), d int, primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20)
)`
	tk.MustExec(createTable)
	tk.MustExec("insert into t values (1,1,1,1),(2,1,2,2),(3,1,3,3),(4,1,4,4),(5,1,5,5),(6,1,6,6),(7,7,7,7),(8,8,8,8),(9,9,9,9)")
	tk.MustExec("insert into t values (10,10,10,10),(11,11,11,11),(12,12,12,12),(13,13,13,13),(14,14,14,14)")
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)

	// analyze table only sets table options and gen globalStats
	tk.MustExec("analyze table t columns a,c with 1 topn, 3 buckets")
	tk.MustQuery("select * from t where b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl := h.GetTableStats(tableInfo)
	lastVersion := tbl.Version
	// both globalStats and partition stats generated and options saved for column a,c
	require.Equal(t, 3, len(tbl.Columns[tableInfo.Columns[0].ID].Buckets))
	require.Equal(t, 1, len(tbl.Columns[tableInfo.Columns[0].ID].TopN.TopN))
	require.Equal(t, 3, len(tbl.Columns[tableInfo.Columns[2].ID].Buckets))
	require.Equal(t, 1, len(tbl.Columns[tableInfo.Columns[2].ID].TopN.TopN))
	rs := tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[0].ID, 10))
	require.Equal(t, 0, len(rs.Rows()))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[1].ID, 10))
	require.Equal(t, 0, len(rs.Rows()))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "3", rs.Rows()[0][0])
	require.Equal(t, "1", rs.Rows()[0][1])

	// analyze table with persisted table-level options
	tk.MustExec("analyze table t")
	tk.MustQuery("select * from t where b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl = h.GetTableStats(tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	lastVersion = tbl.Version
	require.Equal(t, 3, len(tbl.Columns[tableInfo.Columns[0].ID].Buckets))
	require.Equal(t, 1, len(tbl.Columns[tableInfo.Columns[0].ID].TopN.TopN))
	require.Equal(t, 3, len(tbl.Columns[tableInfo.Columns[2].ID].Buckets))
	require.Equal(t, 1, len(tbl.Columns[tableInfo.Columns[2].ID].TopN.TopN))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[0].ID, 10))
	require.Equal(t, 0, len(rs.Rows()))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[1].ID, 10))
	require.Equal(t, 0, len(rs.Rows()))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "3", rs.Rows()[0][0])
	require.Equal(t, "1", rs.Rows()[0][1])

	// analyze table with merged table-level options
	tk.MustExec("analyze table t with 2 topn, 2 buckets")
	tk.MustQuery("select * from t where b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl = h.GetTableStats(tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	require.Equal(t, 2, len(tbl.Columns[tableInfo.Columns[0].ID].Buckets))
	require.Equal(t, 2, len(tbl.Columns[tableInfo.Columns[0].ID].TopN.TopN))
	require.Equal(t, 2, len(tbl.Columns[tableInfo.Columns[2].ID].Buckets))
	require.Equal(t, 2, len(tbl.Columns[tableInfo.Columns[2].ID].TopN.TopN))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[0].ID, 10))
	require.Equal(t, 0, len(rs.Rows()))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[1].ID, 10))
	require.Equal(t, 0, len(rs.Rows()))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])
}

func TestAnalyzePartitionTableStaticToDynamic(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	createTable := `CREATE TABLE t (a int, b int, c varchar(10), d int, primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20)
)`
	tk.MustExec(createTable)
	tk.MustExec("insert into t values (1,1,1,1),(2,1,2,2),(3,1,3,3),(4,1,4,4),(5,1,5,5),(6,1,6,6),(7,7,7,7),(8,8,8,8),(9,9,9,9)")
	tk.MustExec("insert into t values (10,10,10,10),(11,11,11,11),(12,12,12,12),(13,13,13,13),(14,14,14,14)")
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)

	// analyze partition under static mode with options
	tk.MustExec("analyze table t partition p0 columns a,c with 1 topn, 3 buckets")
	tk.MustQuery("select * from t where b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl := h.GetTableStats(tableInfo)
	p0 := h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
	p1 := h.GetPartitionStats(tableInfo, pi.Definitions[1].ID)
	lastVersion := tbl.Version
	require.Equal(t, 3, len(p0.Columns[tableInfo.Columns[0].ID].Buckets))
	require.Equal(t, 3, len(p0.Columns[tableInfo.Columns[2].ID].Buckets))
	require.Equal(t, 0, len(p1.Columns[tableInfo.Columns[0].ID].Buckets))
	require.Equal(t, 0, len(tbl.Columns[tableInfo.Columns[0].ID].Buckets))
	rs := tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[0].ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "3", rs.Rows()[0][0])
	require.Equal(t, "1", rs.Rows()[0][1])
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[1].ID, 10))
	require.Equal(t, 0, len(rs.Rows()))
	// The columns are: table_id, sample_num, sample_rate, buckets, topn, column_choice, column_ids.
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])

	tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")

	// analyze table in dynamic mode will ignore partition-level options and use default
	tk.MustExec("analyze table t")
	tk.MustQuery("select * from t where b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl = h.GetTableStats(tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	lastVersion = tbl.Version
	p0 = h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
	p1 = h.GetPartitionStats(tableInfo, pi.Definitions[1].ID)
	require.NotEqual(t, 3, len(p0.Columns[tableInfo.Columns[0].ID].Buckets))
	require.Equal(t, len(tbl.Columns[tableInfo.Columns[0].ID].Buckets), len(p0.Columns[tableInfo.Columns[0].ID].Buckets))
	require.Equal(t, len(tbl.Columns[tableInfo.Columns[0].ID].Buckets), len(p1.Columns[tableInfo.Columns[0].ID].Buckets))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[0].ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "3", rs.Rows()[0][0])
	require.Equal(t, "1", rs.Rows()[0][1])
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[1].ID, 10))
	require.Equal(t, 0, len(rs.Rows()))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "0", rs.Rows()[0][0])

	// analyze table under dynamic mode with specified options with old partition-level options
	tk.MustExec("analyze table t columns b,d with 2 topn, 2 buckets")
	tk.MustQuery("select * from t where b > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl = h.GetTableStats(tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	lastVersion = tbl.Version
	require.Equal(t, 2, len(tbl.Columns[tableInfo.Columns[1].ID].Buckets))
	require.Equal(t, 2, len(tbl.Columns[tableInfo.Columns[3].ID].Buckets))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[0].ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "3", rs.Rows()[0][0])
	require.Equal(t, "1", rs.Rows()[0][1])
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[1].ID, 10))
	require.Equal(t, 0, len(rs.Rows()))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	require.Equal(t, "2", rs.Rows()[0][1])

	// analyze table under dynamic mode without options with old table-level & partition-level options
	tk.MustExec("analyze table t")
	tk.MustQuery("select * from t where b > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl = h.GetTableStats(tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	lastVersion = tbl.Version
	require.Equal(t, 2, len(tbl.Columns[tableInfo.Columns[3].ID].Buckets))
	require.Equal(t, 2, len(tbl.Columns[tableInfo.Columns[3].ID].TopN.TopN))

	// analyze table under dynamic mode with specified options with old table-level & partition-level options
	tk.MustExec("analyze table t with 1 topn")
	tk.MustQuery("select * from t where b > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl = h.GetTableStats(tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	require.Equal(t, 2, len(tbl.Columns[tableInfo.Columns[1].ID].Buckets))
	require.Equal(t, 2, len(tbl.Columns[tableInfo.Columns[3].ID].Buckets))
	require.Equal(t, 1, len(tbl.Columns[tableInfo.Columns[1].ID].TopN.TopN))
	require.Equal(t, 1, len(tbl.Columns[tableInfo.Columns[3].ID].TopN.TopN))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[0].ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "3", rs.Rows()[0][0])
	require.Equal(t, "1", rs.Rows()[0][1])
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(pi.Definitions[1].ID, 10))
	require.Equal(t, 0, len(rs.Rows()))
	rs = tk.MustQuery("select buckets,topn from mysql.analyze_options where table_id=" + strconv.FormatInt(tableInfo.ID, 10))
	require.Equal(t, 1, len(rs.Rows()))
	require.Equal(t, "2", rs.Rows()[0][0])
	require.Equal(t, "1", rs.Rows()[0][1])
}

func TestAnalyzePartitionUnderDynamic(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
	createTable := `CREATE TABLE t (a int, b int, c varchar(10), d int, primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20)
)`
	tk.MustExec(createTable)
	tk.MustExec("insert into t values (1,1,1,1),(2,1,2,2),(3,1,3,3),(4,1,4,4),(5,1,5,5),(6,1,6,6),(7,7,7,7),(8,8,8,8),(9,9,9,9)")
	tk.MustExec("insert into t values (10,10,10,10),(11,11,11,11),(12,12,12,12),(13,13,13,13),(14,14,14,14)")
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)

	// analyze partition with options under dynamic mode
	tk.MustExec("analyze table t partition p0 columns a,b,c with 1 topn, 3 buckets")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0",
		"Warning 1105 Ignore columns and options when analyze partition in dynamic mode",
		"Warning 8131 Build table: `t` global-level stats failed due to missing partition-level stats",
		"Warning 8131 Build table: `t` index: `idx` global-level stats failed due to missing partition-level stats",
	))
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl := h.GetTableStats(tableInfo)
	lastVersion := tbl.Version
	require.NotEqual(t, 3, len(tbl.Columns[tableInfo.Columns[2].ID].Buckets))
	require.NotEqual(t, 3, len(tbl.Columns[tableInfo.Columns[3].ID].Buckets))

	tk.MustExec("analyze table t partition p0")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0",
		"Warning 8131 Build table: `t` global-level stats failed due to missing partition-level stats",
		"Warning 8131 Build table: `t` index: `idx` global-level stats failed due to missing partition-level stats",
	))
	tbl = h.GetTableStats(tableInfo)
	require.Equal(t, tbl.Version, lastVersion) // global stats not updated
}

func TestAnalyzePartitionStaticToDynamic(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	createTable := `CREATE TABLE t (a int, b int, c varchar(10), d int, primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20)
)`
	tk.MustExec(createTable)
	tk.MustExec("insert into t values (1,1,1,1),(2,1,2,2),(3,1,3,3),(4,1,4,4),(5,1,5,5),(6,1,6,6),(7,7,7,7),(8,8,8,8),(9,9,9,9)")
	tk.MustExec("insert into t values (10,10,10,10),(11,11,11,11),(12,12,12,12),(13,13,13,13),(14,14,14,14)")
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)

	// generate old partition stats
	tk.MustExec("set global tidb_persist_analyze_options = false")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	tk.MustExec("analyze table t partition p0 columns a,c with 1 topn, 3 buckets")
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms())
	p0 := h.GetPartitionStats(tableInfo, pi.Definitions[0].ID)
	require.Equal(t, 3, len(p0.Columns[tableInfo.Columns[2].ID].Buckets))

	// analyze partition with existing stats of other partitions under dynamic
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("analyze table t partition p1 columns a,b,d with 1 topn, 3 buckets")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p1",
		"Warning 8244 Build table: `t` column: `d` global-level stats failed due to missing partition-level column stats, please run analyze table to refresh columns of all partitions",
	))

	// analyze partition with existing table-level options and existing partition stats under dynamic
	tk.MustExec("insert into mysql.analyze_options values (?,?,?,?,?,?,?)", tableInfo.ID, 0, 0, 2, 2, "DEFAULT", "")
	tk.MustExec("set global tidb_persist_analyze_options = true")
	tk.MustExec("analyze table t partition p1 columns a,b,d with 1 topn, 3 buckets")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p1",
		"Warning 1105 Ignore columns and options when analyze partition in dynamic mode",
		"Warning 8244 Build table: `t` column: `d` global-level stats failed due to missing partition-level column stats, please run analyze table to refresh columns of all partitions",
	))

	// analyze partition with existing table-level & partition-level options and existing partition stats under dynamic
	tk.MustExec("insert into mysql.analyze_options values (?,?,?,?,?,?,?)", pi.Definitions[1].ID, 0, 0, 1, 1, "DEFAULT", "")
	tk.MustExec("analyze table t partition p1 columns a,b,d with 1 topn, 3 buckets")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p1",
		"Warning 1105 Ignore columns and options when analyze partition in dynamic mode",
		"Warning 8244 Build table: `t` column: `d` global-level stats failed due to missing partition-level column stats, please run analyze table to refresh columns of all partitions",
	))
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl := h.GetTableStats(tableInfo)
	require.Equal(t, 0, len(tbl.Columns))

	// ignore both p0's 3 buckets, persisted-partition-options' 1 bucket, just use table-level 2 buckets
	tk.MustExec("analyze table t partition p0")
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl = h.GetTableStats(tableInfo)
	require.Equal(t, 2, len(tbl.Columns[tableInfo.Columns[2].ID].Buckets))
}

func TestAnalyzePartitionUnderV1Dynamic(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
	createTable := `CREATE TABLE t (a int, b int, c varchar(10), d int, primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20)
)`
	tk.MustExec(createTable)
	tk.MustExec("insert into t values (1,1,1,1),(2,1,2,2),(3,1,3,3),(4,1,4,4),(5,1,5,5),(6,1,6,6),(7,7,7,7),(8,8,8,8),(9,9,9,9)")
	tk.MustExec("insert into t values (10,10,10,10),(11,11,11,11),(12,12,12,12),(13,13,13,13),(14,14,14,14)")
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)

	// analyze partition with index and with options are allowed under dynamic V1
	tk.MustExec("analyze table t partition p0 with 1 topn, 3 buckets")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Warning 8131 Build table: `t` global-level stats failed due to missing partition-level stats",
		"Warning 8131 Build table: `t` index: `idx` global-level stats failed due to missing partition-level stats",
	))
	tk.MustExec("analyze table t partition p1 with 1 topn, 3 buckets")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows())
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl := h.GetTableStats(tableInfo)
	lastVersion := tbl.Version
	require.Equal(t, 3, len(tbl.Columns[tableInfo.Columns[2].ID].Buckets))
	require.Equal(t, 3, len(tbl.Columns[tableInfo.Columns[3].ID].Buckets))

	tk.MustExec("analyze table t partition p1 index idx with 1 topn, 2 buckets")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows())
	tbl = h.GetTableStats(tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	require.Equal(t, 2, len(tbl.Indices[tableInfo.Indices[0].ID].Buckets))
}

func TestIssue35056(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	createTable := `CREATE TABLE t (id int, a int, b varchar(10))
PARTITION BY RANGE ( id ) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20)
)`
	tk.MustExec(createTable)
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	tk.MustExec("insert into t values (1,1,1),(2,2,2),(3,3,3),(4,4,4),(7,7,7),(9,9,9)")
	tk.MustExec("insert into t values (11,11,11),(12,12,12),(14,14,14)")
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	is := dom.InfoSchema()
	h.HandleAutoAnalyze(is)
	tk.MustExec("create index idxa on t (a)")
	tk.MustExec("create index idxb on t (b)")
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)
	tk.MustExec("analyze table t partition p0 index idxa")
	tk.MustExec("analyze table t partition p1 index idxb")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("analyze table t partition p0") // no panic
}

func TestIssue35056Related(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	createTable := `CREATE TABLE t (id int)
PARTITION BY RANGE ( id ) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20)
)`
	tk.MustExec(createTable)
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	tk.MustExec("insert into t values (1),(2),(3),(4),(7),(9)")
	tk.MustExec("insert into t values (11),(12),(14)")
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	is := dom.InfoSchema()
	h.HandleAutoAnalyze(is)
	tk.MustExec("alter table t add column a int")
	tk.MustExec("alter table t add column b int")
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)
	tk.MustExec("analyze table t partition p0 columns id,a")
	tk.MustExec("analyze table t partition p1 columns id,b")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("analyze table t partition p0") // no panic
}

func TestIssue35044(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	createTable := `CREATE TABLE t (a int)
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20)
)`
	tk.MustExec(createTable)
	tk.MustExec("insert into t values (1),(2),(3)")
	tk.MustExec("insert into t values (11),(12),(14)")
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	is := dom.InfoSchema()
	table, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)
	tk.MustExec("analyze table t partition p0 columns a")
	tk.MustExec("analyze table t partition p1 columns a")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'dynamic'")
	tk.MustExec("analyze table t partition p0")
	tbl := h.GetTableStats(tableInfo)
	require.Equal(t, int64(6), tbl.Columns[tableInfo.Columns[0].ID].Histogram.NDV)
}
