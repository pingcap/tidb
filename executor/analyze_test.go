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
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

func TestIssue15993(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
