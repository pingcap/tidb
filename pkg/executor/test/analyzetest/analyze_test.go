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

package analyzetest

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

func TestAnalyzePartition(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
				require.Greater(t, col.Len()+col.TopN.Num(), 0)
			}
			for _, idx := range statsTbl.Indices {
				require.Greater(t, idx.Len()+idx.TopN.Num(), 0)
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
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	ctx := tk.Session().(sessionctx.Context)
	ctx.GetSessionVars().SetReplicaRead(kv.ReplicaReadFollower)
	tk.MustExec("analyze table t")
}
func TestAnalyzeRestrict(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	rs, err := tk.Session().ExecuteInternal(ctx, "analyze table t")
	require.Nil(t, err)
	require.Nil(t, rs)
}

func TestAnalyzeParameters(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%d)", i))
	}
	tk.MustExec("insert into t values (19), (19), (19)")

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
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a json)")
	value := fmt.Sprintf(`{"x":"%s"}`, strings.Repeat("x", mysql.MaxFieldVarCharLength))
	tk.MustExec(fmt.Sprintf("insert into t values ('%s')", value))

	tk.MustExec("set @@session.tidb_analyze_skip_column_types = ''")
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

func TestFailedAnalyzeRequest(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, index index_b(b))")
	tk.MustExec("set @@tidb_analyze_version = 1")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/buildStatsFromResult", `return(true)`))
	_, err := tk.Exec("analyze table t")
	require.NotNil(t, err)
	require.Equal(t, "mock buildStatsFromResult error", err.Error())
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/buildStatsFromResult"))
}

func TestExtractTopN(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

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

func TestNormalAnalyzeOnCommonHandle(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version=1")
	defer tk.MustExec("set @@tidb_analyze_version=2")
	originalSampleSize := executor.MaxRegionSampleSize
	// Increase MaxRegionSampleSize to ensure all samples are collected for building histogram, otherwise the test will be unstable.
	executor.MaxRegionSampleSize = 10000
	defer func() {
		executor.MaxRegionSampleSize = originalSampleSize
	}()
	tk.MustExec("drop database if exists test_default_val_for_analyze;")
	tk.MustExec("create database test_default_val_for_analyze;")
	tk.MustExec("use test_default_val_for_analyze")

	tk.MustExec("create table t (a int, key(a));")
	for i := 0; i < 256; i++ {
		tk.MustExec("insert into t values (0),(0),(0),(0),(0),(0),(0),(0)")
	}
	for i := 1; i < 4; i++ {
		tk.MustExec("insert into t values (?)", i)
	}

	// Default RPC encoding may cause statistics explain result differ and then the test unstable.
	tk.MustExec("set @@tidb_enable_chunk_rpc = on")

	tk.MustQuery("select @@tidb_enable_fast_analyze").Check(testkit.Rows("0"))
	tk.MustQuery("select @@session.tidb_enable_fast_analyze").Check(testkit.Rows("0"))
	tk.MustExec("analyze table t with 0 topn, 2 buckets, 10000 samples")
	tk.MustQuery("explain format = 'brief' select * from t where a = 1").Check(testkit.Rows("IndexReader 512.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 512.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false"))
	tk.MustQuery("explain format = 'brief' select * from t where a = 999").Check(testkit.Rows("IndexReader 0.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 0.00 cop[tikv] table:t, index:a(a) range:[999,999], keep order:false"))

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, key(a));")
	for i := 0; i < 256; i++ {
		tk.MustExec("insert into t values (0),(0),(0),(0),(0),(0),(0),(0)")
	}
	for i := 1; i < 2049; i += 8 {
		vals := make([]string, 0, 8)
		for j := i; j < i+8; j += 1 {
			vals = append(vals, fmt.Sprintf("(%v)", j))
		}
		tk.MustExec("insert into t values " + strings.Join(vals, ","))
	}
	tk.MustExec("analyze table t with 0 topn;")
	tk.MustQuery("explain format = 'brief' select * from t where a = 1").Check(testkit.Rows("IndexReader 1.00 root  index:IndexRangeScan",
		"└─IndexRangeScan 1.00 cop[tikv] table:t, index:a(a) range:[1,1], keep order:false"))
}

func TestAnalyzeFullSamplingOnIndexWithVirtualColumnOrPrefixColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
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

func testSnapshotAnalyzeAndMaxTSAnalyzeHelper(analyzeSnapshot bool) func(t *testing.T) {
	return func(t *testing.T) {
		store := testkit.CreateMockStore(t)
		tk := testkit.NewTestKit(t, store)

		tk.MustExec("use test")
		if analyzeSnapshot {
			tk.MustExec("set @@session.tidb_enable_analyze_snapshot = on")
		} else {
			tk.MustExec("set @@session.tidb_enable_analyze_snapshot = off")
		}
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
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/injectAnalyzeSnapshot", fmt.Sprintf("return(%d)", startTS1)))
		tk.MustExec("analyze table t")
		rows := tk.MustQuery(fmt.Sprintf("select count, snapshot from mysql.stats_meta where table_id = %d", tid)).Rows()
		require.Len(t, rows, 1)
		if analyzeSnapshot {
			// Analyze cannot see the second insert if it reads the snapshot.
			require.Equal(t, "3", rows[0][0])
		} else {
			// Analyze can see the second insert if it reads the latest data.
			require.Equal(t, "6", rows[0][0])
		}
		s1Str := rows[0][1].(string)
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/injectAnalyzeSnapshot", fmt.Sprintf("return(%d)", startTS2)))
		tk.MustExec("analyze table t")
		rows = tk.MustQuery(fmt.Sprintf("select count, snapshot from mysql.stats_meta where table_id = %d", tid)).Rows()
		require.Len(t, rows, 1)
		require.Equal(t, "6", rows[0][0])
		s2Str := rows[0][1].(string)
		require.True(t, s1Str != s2Str)
		tk.MustExec("set @@session.tidb_analyze_version = 2")
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/injectAnalyzeSnapshot", fmt.Sprintf("return(%d)", startTS1)))
		tk.MustExec("analyze table t")
		rows = tk.MustQuery(fmt.Sprintf("select count, snapshot from mysql.stats_meta where table_id = %d", tid)).Rows()
		require.Len(t, rows, 1)
		require.Equal(t, "6", rows[0][0])
		s3Str := rows[0][1].(string)
		// The third analyze doesn't write results into mysql.stats_xxx because its snapshot is smaller than the second analyze.
		require.Equal(t, s2Str, s3Str)
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/injectAnalyzeSnapshot"))
	}
}

func TestSnapshotAnalyzeAndMaxTSAnalyze(t *testing.T) {
	for _, analyzeSnapshot := range []bool{true, false} {
		t.Run(fmt.Sprintf("%s-%t", t.Name(), analyzeSnapshot), testSnapshotAnalyzeAndMaxTSAnalyzeHelper(analyzeSnapshot))
	}
}

func TestAdjustSampleRateNote(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Analyze use auto adjusted sample rate 0.500000 for table test.t, reason to use this rate is \"use min(1, 110000/220000) as the sample-rate=0.5\""))
	tk.MustExec("insert into t values(1),(1),(1)")
	require.NoError(t, statsHandle.DumpStatsDeltaToKV(true))
	require.NoError(t, statsHandle.Update(is))
	result = tk.MustQuery("show stats_meta where table_name = 't'")
	require.Equal(t, "3", result.Rows()[0][5])
	tk.MustExec("analyze table t")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/3) as the sample-rate=1\""))
}

func TestAnalyzeIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)

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

	tk.MustExec("drop stats t1")
	tk.MustExec("set @@tidb_analyze_version=1")
	tk.MustExec("analyze table t1 index k")
	require.Greater(t, len(tk.MustQuery("show stats_buckets where table_name = 't1' and column_name = 'k' and is_index = 1").Rows()), 1)
}

func TestIssue20874(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	store := testkit.CreateMockStore(t)

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
	tk.MustQuery("show stats_buckets").Sort().Check(testkit.Rows(
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
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12)")
	tk.MustExec("split table t between (-9223372036854775808) and (9223372036854775807) regions 12")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockAnalyzeSamplingBuildWorkerPanic", "return(1)"))
	err := tk.ExecToErr("analyze table t")
	require.NotNil(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockAnalyzeSamplingBuildWorkerPanic"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockAnalyzeSamplingMergeWorkerPanic", "return(1)"))
	err = tk.ExecToErr("analyze table t")
	require.NotNil(t, err)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockAnalyzeSamplingMergeWorkerPanic"))
}

func TestSmallTableAnalyzeV2(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/calcSampleRateByStorageCount", "return(1)"))
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("create table small_table_inject_pd(a int)")
	tk.MustExec("insert into small_table_inject_pd values(1), (2), (3), (4), (5)")
	tk.MustExec("analyze table small_table_inject_pd")
	tk.MustQuery("show warnings").Check(testkit.Rows("Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.small_table_inject_pd, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\""))
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
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.small_table_inject_pd_with_partition's partition p0, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.small_table_inject_pd_with_partition's partition p1, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.small_table_inject_pd_with_partition's partition p2, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
	))
	rows := [][]any{
		{"global", "a"},
		{"p0", "a"},
		{"p1", "a"},
		{"p2", "a"},
	}
	tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 'small_table_inject_pd_with_partition' and last_analyzed_at is not null").Sort().CheckAt([]int{2, 3}, rows)
	rows = [][]any{
		{"global", "0", "3"},
		{"p0", "0", "1"},
		{"p1", "0", "1"},
		{"p2", "0", "1"},
	}
	tk.MustQuery("show stats_meta where db_name = 'test' and table_name = 'small_table_inject_pd_with_partition'").Sort().CheckAt([]int{2, 4, 5}, rows)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/calcSampleRateByStorageCount"))
}

func TestSavedAnalyzeOptions(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	originalVal3 := exec.AutoAnalyzeMinCnt
	defer func() {
		exec.AutoAnalyzeMinCnt = originalVal3
	}()
	exec.AutoAnalyzeMinCnt = 0

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_stats_load_sync_wait = 20000") // to stabilise test
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
	require.Nil(t, h.DumpStatsDeltaToKV(true))
	require.Nil(t, h.Update(is))
	h.HandleAutoAnalyze()
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_stats_load_sync_wait = 20000") // to stabilise test
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	createTable := `CREATE TABLE t (a int, b int, c varchar(10), primary key(a), index idx(b))
PARTITION BY RANGE ( a ) (
		PARTITION p0 VALUES LESS THAN (10),
		PARTITION p1 VALUES LESS THAN (20)
)`
	tk.MustExec(createTable)
	tk.MustExec("insert into t values (1,1,1),(2,1,2),(3,1,3),(4,1,4),(5,1,5),(6,1,6),(7,7,7),(8,8,8),(9,9,9),(10,10,10),(11,11,11),(12,12,12),(13,13,13),(14,14,14)")

	h := dom.StatsHandle()

	// analyze partition only sets options of partition
	tk.MustExec("analyze table t partition p0 with 1 topn, 3 buckets")
	is := dom.InfoSchema()
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	originalVal3 := exec.AutoAnalyzeMinCnt
	defer func() {
		exec.AutoAnalyzeMinCnt = originalVal3
	}()
	exec.AutoAnalyzeMinCnt = 0
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
	require.Nil(t, h.DumpStatsDeltaToKV(true))
	require.Nil(t, h.Update(is))
	// auto analyze uses the saved option(predicate columns).
	h.HandleAutoAnalyze()
	tblStats = h.GetTableStats(tblInfo)
	require.Less(t, lastVersion, tblStats.Version)
	lastVersion = tblStats.Version
	// column b, c are analyzed
	require.Greater(t, lastVersion, tblStats.Columns[tblInfo.Columns[0].ID].LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.Columns[tblInfo.Columns[1].ID].LastUpdateVersion)
	require.Equal(t, lastVersion, tblStats.Columns[tblInfo.Columns[2].ID].LastUpdateVersion)

	tk.MustExec("analyze table t columns a")
	// TODO: the a's meta should be keep. Or the previous a's meta should be clear.
	tblStats, err = h.TableStatsFromStorage(tblInfo, tblInfo.ID, true, 0)
	require.NoError(t, err)
	require.Less(t, lastVersion, tblStats.Version)
	lastVersion = tblStats.Version
	// column a is analyzed
	require.Equal(t, lastVersion, tblStats.Columns[tblInfo.Columns[0].ID].LastUpdateVersion)
	require.Greater(t, lastVersion, tblStats.Columns[tblInfo.Columns[1].ID].LastUpdateVersion)
	require.Greater(t, lastVersion, tblStats.Columns[tblInfo.Columns[2].ID].LastUpdateVersion)
	tk.MustQuery(fmt.Sprintf("select column_choice, column_ids from mysql.analyze_options where table_id = %v", tblInfo.ID)).Check(testkit.Rows(fmt.Sprintf("LIST %v", tblInfo.Columns[0].ID)))

	tk.MustExec("analyze table t all columns")
	// TODO: the a's meta should be keep. Or the previous a's meta should be clear.
	tblStats, err = h.TableStatsFromStorage(tblInfo, tblInfo.ID, true, 0)
	require.NoError(t, err)
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
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int, c int primary key)")
			tk.MustExec("insert into t values (1,1,1), (1,1,2), (2,2,3), (2,2,4), (3,3,5), (4,3,6), (5,4,7), (6,4,8), (null,null,9)")
			require.NoError(t, h.DumpStatsDeltaToKV(true))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns a with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
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
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int, c int, d int, index idx_b_d(b, d))")
			tk.MustExec("insert into t values (1,1,null,1), (2,1,9,1), (1,1,8,1), (2,2,7,2), (1,3,7,3), (2,4,6,4), (1,4,6,5), (2,4,6,5), (1,5,6,5)")
			require.NoError(t, h.DumpStatsDeltaToKV(true))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns c with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
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
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int, c int, d int, primary key(b, d) clustered)")
			tk.MustExec("insert into t values (1,1,null,1), (2,2,9,2), (1,3,8,3), (2,4,7,4), (1,5,7,5), (2,6,6,6), (1,7,6,7), (2,8,6,8), (1,9,6,9)")
			require.NoError(t, h.DumpStatsDeltaToKV(true))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns c with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
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
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")
			tk.MustExec("create table t (a int, b int, c int, index idx(c)) partition by range (a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
			tk.MustExec("insert into t values (1,2,1), (2,4,1), (3,6,1), (4,8,2), (4,8,2), (5,10,3), (5,10,4), (5,10,5), (null,null,6), (11,22,7), (12,24,8), (13,26,9), (14,28,10), (15,30,11), (16,32,12), (16,32,13), (16,32,13), (16,32,14), (17,34,14), (17,34,14)")
			require.NoError(t, h.DumpStatsDeltaToKV(true))

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
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p1, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
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
				require.Equal(t, []any{"test", "t", "global", "a"}, rows[0][:4])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 6, len(rows))
			require.Equal(t, []any{"test", "t", "global", "a"}, rows[0][:4])
			require.Equal(t, []any{"test", "t", "global", "c"}, rows[1][:4])
			require.Equal(t, []any{"test", "t", "p0", "a"}, rows[2][:4])
			require.Equal(t, []any{"test", "t", "p0", "c"}, rows[3][:4])
			require.Equal(t, []any{"test", "t", "p1", "a"}, rows[4][:4])
			require.Equal(t, []any{"test", "t", "p1", "c"}, rows[5][:4])

			rows = tk.MustQuery("show stats_meta where db_name = 'test' and table_name = 't'").Sort().Rows()
			require.Equal(t, 3, len(rows))
			require.Equal(t, []any{"test", "t", "global", "0", "20"}, append(rows[0][:3], rows[0][4:6]...))
			require.Equal(t, []any{"test", "t", "p0", "0", "9"}, append(rows[1][:3], rows[1][4:6]...))
			require.Equal(t, []any{"test", "t", "p1", "0", "11"}, append(rows[2][:3], rows[2][4:6]...))

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

func TestAnalyzeColumnsWithStaticPartitionTable(t *testing.T) {
	for _, val := range []model.ColumnChoice{model.ColumnList, model.PredicateColumns} {
		func(choice model.ColumnChoice) {
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("set @@tidb_partition_prune_mode = 'static'")
			tk.MustExec("create table t (a int, b int, c int, index idx(c)) partition by range (a) (partition p0 values less than (10), partition p1 values less than maxvalue)")
			tk.MustExec("insert into t values (1,2,1), (2,4,1), (3,6,1), (4,8,2), (4,8,2), (5,10,3), (5,10,4), (5,10,5), (null,null,6), (11,22,7), (12,24,8), (13,26,9), (14,28,10), (15,30,11), (16,32,12), (16,32,13), (16,32,13), (16,32,14), (17,34,14), (17,34,14)")
			require.NoError(t, h.DumpStatsDeltaToKV(true))

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
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p1, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
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
				require.Equal(t, []any{"test", "t", "global", "a"}, rows[0][:4])
				tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
			}

			rows := tk.MustQuery("show column_stats_usage where db_name = 'test' and table_name = 't' and last_analyzed_at is not null").Sort().Rows()
			require.Equal(t, 4, len(rows))
			require.Equal(t, []any{"test", "t", "p0", "a"}, rows[0][:4])
			require.Equal(t, []any{"test", "t", "p0", "c"}, rows[1][:4])
			require.Equal(t, []any{"test", "t", "p1", "a"}, rows[2][:4])
			require.Equal(t, []any{"test", "t", "p1", "c"}, rows[3][:4])

			rows = tk.MustQuery("show stats_meta where db_name = 'test' and table_name = 't'").Sort().Rows()
			require.Equal(t, 2, len(rows))
			require.Equal(t, []any{"test", "t", "p0", "0", "9"}, append(rows[0][:3], rows[0][4:6]...))
			require.Equal(t, []any{"test", "t", "p1", "0", "11"}, append(rows[1][:3], rows[1][4:6]...))

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
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("set @@tidb_enable_extended_stats = on")
			tk.MustExec("create table t (a int, b int, c int)")
			tk.MustExec("alter table t add stats_extended s1 correlation(b,c)")
			tk.MustExec("insert into t values (5,1,1), (4,2,2), (3,3,3), (2,4,4), (1,5,5)")
			require.NoError(t, h.DumpStatsDeltaToKV(true))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns b with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
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
			require.Equal(t, []any{"test", "t", "s1", "[b,c]", "correlation", "1.000000"}, rows[0][:len(rows[0])-1])
		}(val)
	}
}

func TestAnalyzeColumnsWithVirtualColumnIndex(t *testing.T) {
	for _, val := range []model.ColumnChoice{model.ColumnList, model.PredicateColumns} {
		func(choice model.ColumnChoice) {
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int, c int as (b+1), index idx(c))")
			tk.MustExec("insert into t (a,b) values (1,1), (2,2), (3,3), (4,4), (5,4), (6,5), (7,5), (8,5), (null,null)")
			require.NoError(t, h.DumpStatsDeltaToKV(true))

			is := dom.InfoSchema()
			tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			switch choice {
			case model.ColumnList:
				tk.MustExec("analyze table t columns b with 2 topn, 2 buckets")
				tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
					"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
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
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int)")
			tk.MustExec("insert into t (a,b) values (1,1), (1,1), (2,2), (2,2), (3,3), (4,4)")
			require.NoError(t, h.DumpStatsDeltaToKV(true))

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
			require.NoError(t, h.DumpStatsDeltaToKV(true))

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

func TestAnalyzeSampleRateReason(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))

	tk.MustExec(`analyze table t`)
	tk.MustQuery(`show warnings`).Sort().Check(testkit.Rows(
		`Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is "use min(1, 110000/10000) as the sample-rate=1"`))

	tk.MustExec(`insert into t values (1, 1), (2, 2), (3, 3)`)
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`show warnings`).Sort().Check(testkit.Rows(
		`Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is "TiDB assumes that the table is empty, use sample-rate=1"`))
}

func TestAnalyzeColumnsErrorAndWarning(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")

	// analyze version 1 doesn't support `ANALYZE COLUMNS c1, ..., cn`/`ANALYZE PREDICATE COLUMNS` currently
	tk.MustExec("set @@tidb_analyze_version = 1")
	err := tk.ExecToErr("analyze table t columns a")
	require.Equal(t, "Only the version 2 of analyze supports analyzing the specified columns", err.Error())
	err = tk.ExecToErr("analyze table t predicate columns")
	require.Equal(t, "Only the version 2 of analyze supports analyzing predicate columns", err.Error())

	tk.MustExec("set @@tidb_analyze_version = 2")
	// invalid column
	err = tk.ExecToErr("analyze table t columns c")
	terr := errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(errno.ErrAnalyzeMissColumn), terr.Code())

	// If no predicate column is collected, analyze predicate columns gives a warning and falls back to analyze all columns.
	tk.MustExec("analyze table t predicate columns")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		`Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is "use min(1, 110000/10000) as the sample-rate=1"`,
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
				`Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is "TiDB assumes that the table is empty, use sample-rate=1"`,
				"Warning 1105 Table test.t has version 1 statistics so all the columns must be analyzed to overwrite the current statistics",
			))
		}(val)
	}
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
	const layout = time.DateTime
	startTime, err := time.Parse(layout, rows[0][5].(string))
	require.NoError(t, err, comment)
	endTime, err := time.Parse(layout, rows[0][6].(string))
	require.NoError(t, err, comment)
	require.Less(t, endTime.Sub(startTime), time.Duration(timeLimit)*time.Second, comment)
}

func testKillAutoAnalyze(t *testing.T, ver int) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	exec.AutoAnalyzeMinCnt = 0
	defer func() {
		exec.AutoAnalyzeMinCnt = 1000
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
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	tk.MustExec("analyze table t")
	tk.MustExec("insert into t values (5,6), (7,8), (9, 10)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
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
	for _, status := range []string{
		"pending",
		"running",
		"finished",
	} {
		func() {
			comment := fmt.Sprintf("kill %v analyze job", status)
			tk.MustExec("delete from mysql.analyze_jobs")
			mockAnalyzeStatus := "github.com/pingcap/tidb/pkg/executor/mockKill" + strings.Title(status)
			if status == "running" {
				mockAnalyzeStatus += "V" + strconv.Itoa(ver)
			}
			mockAnalyzeStatus += "AnalyzeJob"
			require.NoError(t, failpoint.Enable(mockAnalyzeStatus, "return"))
			defer func() {
				require.NoError(t, failpoint.Disable(mockAnalyzeStatus))
			}()
			if status == "pending" || status == "running" {
				mockSlowAnalyze := "github.com/pingcap/tidb/pkg/executor/mockSlowAnalyzeV" + strconv.Itoa(ver)
				require.NoError(t, failpoint.Enable(mockSlowAnalyze, "return"))
				defer func() {
					require.NoError(t, failpoint.Disable(mockSlowAnalyze))
				}()
			}
			require.True(t, h.HandleAutoAnalyze(), comment)
			currentVersion := h.GetTableStats(tableInfo).Version
			if status == "finished" {
				// If we kill a finished job, after kill command the status is still finished and the table stats are updated.
				checkAnalyzeStatus(t, tk, jobInfo, "finished", "<nil>", comment, -1)
				require.Greater(t, currentVersion, lastVersion, comment)
			} else {
				// If we kill a pending/running job, after kill command the status is failed and the table stats are not updated.
				// We expect the killed analyze stops quickly. Specifically, end_time - start_time < 10s.
				checkAnalyzeStatus(t, tk, jobInfo, "failed", exeerrors.ErrQueryInterrupted.Error(), comment, 10)
				require.Equal(t, currentVersion, lastVersion, comment)
			}
		}()
	}
}

func TestKillAutoAnalyze(t *testing.T) {
	// version 1
	testKillAutoAnalyze(t, 1)
	// version 2
	testKillAutoAnalyze(t, 2)
}

func TestKillAutoAnalyzeIndex(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	oriStart := tk.MustQuery("select @@tidb_auto_analyze_start_time").Rows()[0][0].(string)
	oriEnd := tk.MustQuery("select @@tidb_auto_analyze_end_time").Rows()[0][0].(string)
	exec.AutoAnalyzeMinCnt = 0
	defer func() {
		exec.AutoAnalyzeMinCnt = 1000
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
	require.NoError(t, h.DumpStatsDeltaToKV(true))
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
			mockAnalyzeStatus := "github.com/pingcap/tidb/pkg/executor/mockKill" + strings.Title(status)
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
				require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockSlowAnalyzeIndex", "return"))
				defer func() {
					require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockSlowAnalyzeIndex"))
				}()
			}
			require.True(t, h.HandleAutoAnalyze(), comment)
			currentVersion := h.GetTableStats(tblInfo).Version
			if status == "finished" {
				// If we kill a finished job, after kill command the status is still finished and the index stats are updated.
				checkAnalyzeStatus(t, tk, jobInfo, "finished", "<nil>", comment, -1)
				require.Greater(t, currentVersion, lastVersion, comment)
			} else {
				// If we kill a pending/running job, after kill command the status is failed and the index stats are not updated.
				// We expect the killed analyze stops quickly. Specifically, end_time - start_time < 10s.
				checkAnalyzeStatus(t, tk, jobInfo, "failed", exeerrors.ErrQueryInterrupted.Error(), comment, 10)
				require.Equal(t, currentVersion, lastVersion, comment)
			}
		}()
	}
}

func TestAnalyzeJob(t *testing.T) {
	store := testkit.CreateMockStore(t)
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
		ctx := context.WithValue(context.Background(), executor.AnalyzeProgressTest, 100)
		rows = tk.MustQueryWithContext(ctx, "show analyze status").Rows()
		checkTime := func(val any) {
			str, ok := val.(string)
			require.True(t, ok)
			_, err := time.Parse(time.DateTime, str)
			require.NoError(t, err)
		}
		checkTime(rows[0][5])
		require.Equal(t, statistics.AnalyzeRunning, rows[0][7])
		require.Equal(t, "9m0s", rows[0][11]) // REMAINING_SECONDS
		require.Equal(t, "0.1", rows[0][12])  // PROGRESS
		require.Equal(t, "0", rows[0][13])    // ESTIMATED_TOTAL_ROWS

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
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	tk.MustExec("set global tidb_persist_analyze_options = 1")
	tk.MustExec("set global tidb_enable_column_tracking = 1")
	tk.MustExec("select * from t where c > 1")
	h := dom.StatsHandle()
	require.NoError(t, h.DumpColStatsUsageToKV())
	tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
	checkJobInfo("analyze table columns b, c, d with 2 buckets, 2 topn, 1 samplerate")
	tk.MustExec("analyze table t")
	checkJobInfo("analyze table columns b, c, d with 2 buckets, 2 topn, 1 samplerate")
	tk.MustExec("analyze table t columns a with 1 topn, 3 buckets")
	checkJobInfo("analyze table columns a, b, d with 3 buckets, 1 topn, 1 samplerate")
	tk.MustExec("analyze table t")
	checkJobInfo("analyze table columns a, b, d with 3 buckets, 1 topn, 1 samplerate")
}

func TestAnalyzePartitionTableWithDynamicMode(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_stats_load_sync_wait = 20000") // to stabilise test
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()
	tk.MustExec("set global tidb_persist_analyze_options = true")

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_stats_load_sync_wait = 20000") // to stabilise test
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
	p0, err = h.TableStatsFromStorage(tableInfo, pi.Definitions[0].ID, true, 0)
	require.NoError(t, err)
	p1, err = h.TableStatsFromStorage(tableInfo, pi.Definitions[1].ID, true, 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(p0.Columns[tableInfo.Columns[0].ID].Buckets))
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
		"Warning 1105 Ignore columns and options when analyze partition in dynamic mode",
	))
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl := h.GetTableStats(tableInfo)
	lastVersion := tbl.Version
	require.NotEqual(t, 3, len(tbl.Columns[tableInfo.Columns[2].ID].Buckets))
	require.NotEqual(t, 3, len(tbl.Columns[tableInfo.Columns[3].ID].Buckets))

	tk.MustExec("analyze table t partition p0")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0, reason to use this rate is \"use min(1, 110000/9) as the sample-rate=1\"",
	))
	tbl = h.GetTableStats(tableInfo)
	require.Greater(t, tbl.Version, lastVersion) // global stats updated
}

func TestAnalyzePartitionStaticToDynamic(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("set @@session.tidb_stats_load_sync_wait = 20000") // to stabilise test
	tk.MustExec("set @@session.tidb_skip_missing_partition_stats = 0")
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
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p1, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
		"Warning 8244 Build global-level stats failed due to missing partition-level column stats: table `t` partition `p0` column `d`, please run analyze table to refresh columns of all partitions",
	))

	// analyze partition with existing table-level options and existing partition stats under dynamic
	tk.MustExec("insert into mysql.analyze_options values (?,?,?,?,?,?,?)", tableInfo.ID, 0, 0, 2, 2, "DEFAULT", "")
	tk.MustExec("set global tidb_persist_analyze_options = true")
	tk.MustExec("analyze table t partition p1 columns a,b,d with 1 topn, 3 buckets")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p1, reason to use this rate is \"use min(1, 110000/5) as the sample-rate=1\"",
		"Warning 1105 Ignore columns and options when analyze partition in dynamic mode",
		"Warning 8244 Build global-level stats failed due to missing partition-level column stats: table `t` partition `p0` column `d`, please run analyze table to refresh columns of all partitions",
	))

	// analyze partition with existing table-level & partition-level options and existing partition stats under dynamic
	tk.MustExec("insert into mysql.analyze_options values (?,?,?,?,?,?,?)", pi.Definitions[1].ID, 0, 0, 1, 1, "DEFAULT", "")
	tk.MustExec("analyze table t partition p1 columns a,b,d with 1 topn, 3 buckets")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p1, reason to use this rate is \"use min(1, 110000/5) as the sample-rate=1\"",
		"Warning 1105 Ignore columns and options when analyze partition in dynamic mode",
		"Warning 8244 Build global-level stats failed due to missing partition-level column stats: table `t` partition `p0` column `d`, please run analyze table to refresh columns of all partitions",
	))
	// flaky test, fix it later
	//tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1 and d > 1")
	//require.NoError(t, h.LoadNeededHistograms())
	//tbl := h.GetTableStats(tableInfo)
	//require.Equal(t, 0, len(tbl.Columns))

	// ignore both p0's 3 buckets, persisted-partition-options' 1 bucket, just use table-level 2 buckets
	tk.MustExec("analyze table t partition p0")
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms())
	tbl := h.GetTableStats(tableInfo)
	require.Equal(t, 2, len(tbl.Columns[tableInfo.Columns[2].ID].Buckets))
}

func TestAnalyzePartitionUnderV1Dynamic(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	originalVal := tk.MustQuery("select @@tidb_persist_analyze_options").Rows()[0][0].(string)
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_persist_analyze_options = %v", originalVal))
	}()

	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 1")
	tk.MustExec("set @@session.tidb_stats_load_sync_wait = 20000") // to stabilise test
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
	rows := tk.MustQuery("show warnings").Rows()
	require.Len(t, rows, 0)
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	h.HandleAutoAnalyze()
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	h.HandleAutoAnalyze()
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
	store, dom := testkit.CreateMockStoreAndDomain(t)
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
	tbl, err := h.TableStatsFromStorage(table.Meta(), table.Meta().ID, true, 0)
	require.NoError(t, err)
	require.Equal(t, int64(6), tbl.Columns[tableInfo.Columns[0].ID].Histogram.NDV)
}

func TestAutoAnalyzeAwareGlobalVariableChange(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select @@global.tidb_enable_analyze_snapshot").Check(testkit.Rows("0"))
	// We want to test that HandleAutoAnalyze is aware of setting @@global.tidb_enable_analyze_snapshot to 1 and reads data from snapshot.
	tk.MustExec("set @@global.tidb_enable_analyze_snapshot = 1")
	tk.MustExec("set @@global.tidb_analyze_version = 2")
	tk.MustExec("create table t(a int)")
	h := dom.StatsHandle()
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tid := tbl.Meta().ID
	tk.MustExec("insert into t values(1),(2),(3)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	err = h.Update(dom.InfoSchema())
	require.NoError(t, err)
	tk.MustExec("analyze table t")
	tk.MustQuery(fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tid)).Check(testkit.Rows(
		"3 0",
	))

	originalVal1 := exec.AutoAnalyzeMinCnt
	originalVal2 := tk.MustQuery("select @@global.tidb_auto_analyze_ratio").Rows()[0][0].(string)
	exec.AutoAnalyzeMinCnt = 0
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.001")
	defer func() {
		exec.AutoAnalyzeMinCnt = originalVal1
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_ratio = %v", originalVal2))
	}()

	tk.MustExec("begin")
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	startTS := txn.StartTS()
	tk.MustExec("commit")

	tk.MustExec("insert into t values(4),(5),(6)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	err = h.Update(dom.InfoSchema())
	require.NoError(t, err)

	// Simulate that the analyze would start before and finish after the second insert.
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/injectAnalyzeSnapshot", fmt.Sprintf("return(%d)", startTS)))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/injectBaseCount", "return(3)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/injectBaseModifyCount", "return(0)"))
	require.True(t, h.HandleAutoAnalyze())
	// Check the count / modify_count changes during the analyze are not lost.
	tk.MustQuery(fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tid)).Check(testkit.Rows(
		"6 3",
	))
	// Check the histogram is correct for the snapshot analyze.
	tk.MustQuery(fmt.Sprintf("select distinct_count from mysql.stats_histograms where table_id = %d", tid)).Check(testkit.Rows(
		"3",
	))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/injectAnalyzeSnapshot"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/injectBaseCount"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/injectBaseModifyCount"))
}

func TestAnalyzeColumnsSkipMVIndexJsonCol(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("set @@tidb_analyze_version = 2")
	tk.MustExec("create table t (a int, b int, c json, index idx_b(b), index idx_c((cast(json_extract(c, _utf8mb4'$') as char(32) array))))")
	tk.MustExec(`insert into t values (1, 1, '["a1", "a2"]'), (2, 2, '["b1", "b2"]'), (3, 3, '["c1", "c2"]'), (2, 2, '["c1", "c2"]')`)
	require.NoError(t, h.DumpStatsDeltaToKV(true))

	tk.MustExec("analyze table t columns a")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(""+
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
		"Warning 1105 Columns b are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats"))
	tk.MustQuery("select job_info from mysql.analyze_jobs where table_schema = 'test' and table_name = 't'").Sort().Check(
		testkit.Rows(
			"analyze index idx_c",
			"analyze table columns a, b with 256 buckets, 500 topn, 1 samplerate",
		))

	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	stats := h.GetTableStats(tblInfo)
	require.True(t, stats.Columns[tblInfo.Columns[0].ID].IsStatsInitialized())
	require.True(t, stats.Columns[tblInfo.Columns[1].ID].IsStatsInitialized())
	require.False(t, stats.Columns[tblInfo.Columns[2].ID].IsStatsInitialized())
	require.True(t, stats.Indices[tblInfo.Indices[0].ID].IsStatsInitialized())
	require.True(t, stats.Indices[tblInfo.Indices[1].ID].IsStatsInitialized())
}

// TestAnalyzeMVIndex tests analyzing the mv index use some real data in the table.
// It checks the analyze jobs, async loading and the stats content in the memory.
func TestAnalyzeMVIndex(t *testing.T) {
	t.Skip()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/DebugAnalyzeJobOperations", "return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/DebugAnalyzeJobOperations", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/DebugAnalyzeJobOperations"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/DebugAnalyzeJobOperations"))
	}()
	// 1. prepare the table and insert data
	store, dom := testkit.CreateMockStoreAndDomain(t)
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, j json, index ia(a)," +
		"index ij_signed((cast(j->'$.signed' as signed array)))," +
		"index ij_unsigned((cast(j->'$.unsigned' as unsigned array)))," +
		// date currently incompatible with mysql
		//"index ij_date((cast(j->'$.dt' as date array)))," +
		// datetime currently incompatible with mysql
		//"index ij_datetime((cast(j->'$.dttm' as datetime(6) array)))," +
		// time currently incompatible with mysql
		//"index ij_time((cast(j->'$.tm' as time(6) array)))," +
		"index ij_double((cast(j->'$.dbl' as double array)))," +
		// decimal not supported yet
		//"index ij_decimal((cast(j->'$.dcm' as decimal(15,5) array)))," +
		"index ij_binary((cast(j->'$.bin' as binary(50) array)))," +
		"index ij_char((cast(j->'$.char' as char(50) array)))" +
		")")
	require.NoError(t, h.HandleDDLEvent(<-h.DDLEventCh()))
	jsonData := []map[string]any{
		{
			"signed":   []int64{1, 2, 300, 300, 0, 4, 5, -40000},
			"unsigned": []uint64{0, 3, 4, 600, 12},
			"dt":       []string{"2020-01-23", "2021-03-21", "2011-11-11", "2015-06-18", "1990-03-21", "2050-12-12"},
			"dttm":     []string{"2021-01-11 12:00:00.123456", "2025-05-15 15:50:00.5", "2020-01-01 18:17:16.555", "2100-01-01 15:16:17", "1950-01-01 00:00:00.00008"},
			"tm":       []string{"100:00:30.5", "-321:00:01.16"},
			"dbl":      []float64{-21.5, 2.15, 10.555555, 0.000005, 0.00},
			"dcm":      []float64{1.1, 2.2, 10.1234, -12.34, -1000.56789},
			"bin":      []string{"aaaaaa", "bbbb", "ppp", "ccc", "asdf", "qwer", "yuiop", "1234", "5678", "0000", "zzzz"},
			"char":     []string{"aaa", "cccccc", "eee", "asdf", "qwer", "yuiop", "!@#$"},
		},
		{
			"signed":   []int64{1, 2, 300, 300, 0, 4, 5, -40000},
			"unsigned": []uint64{0, 3, 4, 600, 12},
			"dt":       []string{"2020-01-23", "2021-03-21", "2011-11-11", "2015-06-18", "1990-03-21", "2050-12-12"},
			"dttm":     []string{"2021-01-11 12:00:00.123456", "2025-05-15 15:50:00.5", "2020-01-01 18:17:16.555", "2100-01-01 15:16:17", "1950-01-01 00:00:00.00008"},
			"tm":       []string{"100:00:30.5", "-321:00:01.16", "09:11:47", "8:50.10"},
			"dbl":      []float64{-21.5, 2.15, 10.555555, 0.000005, 0.00, 10.9876},
			"dcm":      []float64{1.1, 2.2, 10.1234, -12.34, 987.654},
			"bin":      []string{"aaaaaa", "bbbb", "ppp", "ccc", "asdf", "qwer", "ghjk", "0000", "zzzz"},
			"char":     []string{"aaa", "cccccc", "eee", "asdf", "qwer", "yuiop", "!@#$"},
		},
		{
			"signed":   []int64{1, 2, 300, 300, 0, 4, -5, 13245},
			"unsigned": []uint64{0, 3, 4, 600, 3112},
			"dt":       []string{"2020-01-23", "2021-03-21", "2011-11-11", "2015-06-18", "1990-03-21", "2050-12-12"},
			"dttm":     []string{"2021-01-11 12:00:00.123456", "2025-05-15 15:50:00.5", "2020-01-01 18:17:16.555", "2340-01-01 15:16:17", "1950-01-01 00:00:00.00008"},
			"tm":       []string{"100:00:30.5", "-321:00:01.16", "09:11:47", "8:50.10", "1:10:43"},
			"dbl":      []float64{-21.5, 2.15, 10.555555, -12.000005, 0.00, 10.9876},
			"dcm":      []float64{1.1, 2.2, 10.1234, -12.34, 987.654},
			"bin":      []string{"aaaaaa", "bbbb", "ppp", "ccc", "asdf", "qwer", "1234", "0000", "zzzz"},
			"char":     []string{"aaa", "cccccc", "eee", "asdf", "qwer", "yuiop", "!@#$"},
		},
		{
			"signed":   []int64{1, 2, 300, 300, 0, 4, -5, 13245},
			"unsigned": []uint64{0, 3, 4, 600, 3112},
			"dt":       []string{"2020-01-23", "2021-03-21", "2011-11-11", "2015-06-18", "1990-03-21", "2050-12-12"},
			"dttm":     []string{"2021-01-11 12:00:00.123456", "2025-05-15 15:50:00.5", "2110-01-01 18:17:16", "2340-01-01 15:16:17", "1950-01-01 00:00:00.00008"},
			"tm":       []string{"100:00:30.5", "-321:00:01.16", "09:11:47", "8:50.10", "1:10:43"},
			"dbl":      []float64{-21.5, 2.15, 10.555555, 0.000005, 0.00, 10.9876},
			"dcm":      []float64{1.1, 2.2, 10.1234, -12.34, -123.654},
			"bin":      []string{"aaaaaa", "bbbb", "ppp", "ccc", "egfb", "nfre", "1234", "0000", "zzzz"},
			"char":     []string{"aaa", "cccccc", "eee", "asdf", "k!@cvd", "yuiop", "%*$%#@qwe"},
		},
		{
			"signed":   []int64{1, 2, 300, -300, 0, 100, -5, 13245},
			"unsigned": []uint64{0, 3, 4, 600, 3112},
			"dt":       []string{"2020-01-23", "2021-03-21", "2011-11-11", "2015-06-18", "1990-03-21", "2050-12-12"},
			"dttm":     []string{"2021-01-11 12:00:00.123456", "2025-05-15 15:50:00.5", "2110-01-01 22:17:16", "2340-01-22 15:16:17", "1950-01-01 00:12:00.00008"},
			"tm":       []string{"100:00:30.5", "-321:00:01.16", "09:11:47", "8:5.10", "12:4:43"},
			"dbl":      []float64{-21.5, 2.15, 10.555555, 0.000005, 0.00, 10.9876},
			"dcm":      []float64{1.1, 2.2, 10.1234, -12.34, 987.654},
			"bin":      []string{"aaaaaa", "bbbb", "ppp", "ccc", "egfb", "nfre", "1234", "3796", "zzzz"},
			"char":     []string{"aaa", "cccccc", "eee", "asdf", "kicvd", "yuiop", "%*asdf@"},
		},
		{
			"signed":   []int64{1, 2, 300, 300, 0, 4, -5, 13245},
			"unsigned": []uint64{0, 3, 4, 600, 3112},
			"dt":       []string{"2020-01-23", "2021-03-21", "2011-11-11", "2015-06-18", "1990-03-21", "2050-12-12"},
			"dttm":     []string{"2021-01-11 12:00:00.123456", "2025-05-15 15:50:00.5", "2020-01-01 18:17:16.555", "2100-01-01 15:16:17", "1950-01-01 00:00:00.00008"},
			"tm":       []string{"100:00:30.5", "-321:00:01.16", "09:11:47", "8:50.10", "1:10:43"},
			"dbl":      []float64{-21.5, 2.15, 10.555555, 0.000005, 0.00, 10.9876},
			"dcm":      []float64{1.1, 2.2, 10.1234, -12.34, 987.654},
			"bin":      []string{"aaaaaa", "bbbb", "ppp", "ccc", "egfb", "nfre", "1234", "0000", "zzzz"},
			"char":     []string{"aaa", "cccccc", "eee", "asdf", "k!@cvd", "yuiop", "%*$%#@qwe"},
		},
	}
	for i := 0; i < 3; i++ {
		jsonValue := jsonData[i]
		jsonValueStr, err := json.Marshal(jsonValue)
		require.NoError(t, err)
		tk.MustExec(fmt.Sprintf("insert into t values (%d, '%s')", 1, jsonValueStr))
	}
	tk.MustExec("insert into t select * from t")
	tk.MustExec("insert into t select * from t")
	tk.MustExec("insert into t select * from t")
	for i := 3; i < 6; i++ {
		jsonValue := jsonData[i]
		jsonValueStr, err := json.Marshal(jsonValue)
		require.NoError(t, err)
		tk.MustExec(fmt.Sprintf("insert into t values (%d, '%s')", 1, jsonValueStr))
	}
	require.NoError(t, h.DumpStatsDeltaToKV(true))

	// 2. analyze and check analyze jobs
	tk.MustExec("analyze table t with 1 samplerate, 3 topn")
	tk.MustQuery("select id, table_schema, table_name, partition_name, job_info, processed_rows, state from mysql.analyze_jobs order by id").
		Check(testkit.Rows("1 test t  analyze table columns a with 256 buckets, 3 topn, 1 samplerate 27 finished",
			"2 test t  analyze index ij_signed 190 finished",
			"3 test t  analyze index ij_unsigned 135 finished",
			"4 test t  analyze index ij_double 154 finished",
			"5 test t  analyze index ij_binary 259 finished",
			"6 test t  analyze index ij_char 189 finished",
		))

	// 3. test stats loading
	// 3.1. turn off sync loading, stats on all indexes should be allEvicted, but these queries should trigger async loading
	tk.MustExec("set session tidb_stats_load_sync_wait = 0")
	tk.MustQuery("explain format = brief select * from t where 1 member of (j->'$.signed')").Check(testkit.Rows(
		"IndexMerge 0.03 root  type: union",
		"├─IndexRangeScan(Build) 0.03 cop[tikv] table:t, index:ij_signed(cast(json_extract(`j`, _utf8mb4'$.signed') as signed array)) range:[1,1], keep order:false, stats:partial[ia:allEvicted, ij_signed:allEvicted, j:unInitialized]",
		"└─TableRowIDScan(Probe) 0.03 cop[tikv] table:t keep order:false, stats:partial[ia:allEvicted, ij_signed:allEvicted, j:unInitialized]",
	))
	tk.MustQuery("explain format = brief select * from t where 1 member of (j->'$.unsigned')").Check(testkit.Rows(
		"IndexMerge 0.03 root  type: union",
		"├─IndexRangeScan(Build) 0.03 cop[tikv] table:t, index:ij_unsigned(cast(json_extract(`j`, _utf8mb4'$.unsigned') as unsigned array)) range:[1,1], keep order:false, stats:partial[ia:allEvicted, ij_unsigned:allEvicted, j:unInitialized]",
		"└─TableRowIDScan(Probe) 0.03 cop[tikv] table:t keep order:false, stats:partial[ia:allEvicted, ij_unsigned:allEvicted, j:unInitialized]",
	))
	tk.MustQuery("explain format = brief select * from t where 10.01 member of (j->'$.dbl')").Check(testkit.Rows(
		"TableReader 21.60 root  data:Selection",
		"└─Selection 21.60 cop[tikv]  json_memberof(cast(10.01, json BINARY), json_extract(test.t.j, \"$.dbl\"))",
		"  └─TableFullScan 27.00 cop[tikv] table:t keep order:false, stats:partial[ia:allEvicted, j:unInitialized]",
	))
	tk.MustQuery("explain format = brief select * from t where '1' member of (j->'$.bin')").Check(testkit.Rows(
		"IndexMerge 0.03 root  type: union",
		"├─IndexRangeScan(Build) 0.03 cop[tikv] table:t, index:ij_binary(cast(json_extract(`j`, _utf8mb4'$.bin') as binary(50) array)) range:[\"1\",\"1\"], keep order:false, stats:partial[ia:allEvicted, ij_binary:allEvicted, j:unInitialized]",
		"└─TableRowIDScan(Probe) 0.03 cop[tikv] table:t keep order:false, stats:partial[ia:allEvicted, ij_binary:allEvicted, j:unInitialized]",
	))
	tk.MustQuery("explain format = brief select * from t where '1' member of (j->'$.char')").Check(testkit.Rows(
		"IndexMerge 0.03 root  type: union",
		"├─IndexRangeScan(Build) 0.03 cop[tikv] table:t, index:ij_char(cast(json_extract(`j`, _utf8mb4'$.char') as char(50) array)) range:[\"1\",\"1\"], keep order:false, stats:partial[ia:allEvicted, ij_char:allEvicted, j:unInitialized]",
		"└─TableRowIDScan(Probe) 0.03 cop[tikv] table:t keep order:false, stats:partial[ia:allEvicted, ij_char:allEvicted, j:unInitialized]",
	))
	// 3.2. emulate the background async loading
	require.NoError(t, h.LoadNeededHistograms())
	// 3.3. now, stats on all indexes should be loaded
	tk.MustQuery("explain format = brief select /*+ use_index_merge(t, ij_signed) */ * from t where 1 member of (j->'$.signed')").Check(testkit.Rows(
		"IndexMerge 27.00 root  type: union",
		"├─IndexRangeScan(Build) 27.00 cop[tikv] table:t, index:ij_signed(cast(json_extract(`j`, _utf8mb4'$.signed') as signed array)) range:[1,1], keep order:false, stats:partial[j:unInitialized]",
		"└─TableRowIDScan(Probe) 27.00 cop[tikv] table:t keep order:false, stats:partial[j:unInitialized]",
	))
	tk.MustQuery("explain format = brief select /*+ use_index_merge(t, ij_unsigned) */* from t where 1 member of (j->'$.unsigned')").Check(testkit.Rows(
		"IndexMerge 18.00 root  type: union",
		"├─IndexRangeScan(Build) 18.00 cop[tikv] table:t, index:ij_unsigned(cast(json_extract(`j`, _utf8mb4'$.unsigned') as unsigned array)) range:[1,1], keep order:false, stats:partial[j:unInitialized]",
		"└─TableRowIDScan(Probe) 18.00 cop[tikv] table:t keep order:false, stats:partial[j:unInitialized]",
	))
	tk.MustQuery("explain format = brief select /*+ use_index_merge(t, ij_double) */ * from t where 10.01 member of (j->'$.dbl')").Check(testkit.Rows(
		"TableReader 21.60 root  data:Selection",
		"└─Selection 21.60 cop[tikv]  json_memberof(cast(10.01, json BINARY), json_extract(test.t.j, \"$.dbl\"))",
		"  └─TableFullScan 27.00 cop[tikv] table:t keep order:false, stats:partial[j:unInitialized]",
	))
	tk.MustQuery("explain format = brief select /*+ use_index_merge(t, ij_binary) */ * from t where '1' member of (j->'$.bin')").Check(testkit.Rows(
		"IndexMerge 14.83 root  type: union",
		"├─IndexRangeScan(Build) 14.83 cop[tikv] table:t, index:ij_binary(cast(json_extract(`j`, _utf8mb4'$.bin') as binary(50) array)) range:[\"1\",\"1\"], keep order:false, stats:partial[j:unInitialized]",
		"└─TableRowIDScan(Probe) 14.83 cop[tikv] table:t keep order:false, stats:partial[j:unInitialized]",
	))
	tk.MustQuery("explain format = brief select /*+ use_index_merge(t, ij_char) */ * from t where '1' member of (j->'$.char')").Check(testkit.Rows(
		"IndexMerge 13.50 root  type: union",
		"├─IndexRangeScan(Build) 13.50 cop[tikv] table:t, index:ij_char(cast(json_extract(`j`, _utf8mb4'$.char') as char(50) array)) range:[\"1\",\"1\"], keep order:false, stats:partial[j:unInitialized]",
		"└─TableRowIDScan(Probe) 13.50 cop[tikv] table:t keep order:false, stats:partial[j:unInitialized]",
	))

	// 3.4. clean up the stats and re-analyze the table
	tk.MustExec("drop stats t")
	tk.MustExec("analyze table t with 1 samplerate, 3 topn")
	// 3.5. turn on the sync loading, stats on mv indexes should be loaded
	tk.MustExec("set session tidb_stats_load_sync_wait = 1000")
	tk.MustQuery("explain format = brief select /*+ use_index_merge(t, ij_signed) */ * from t where 1 member of (j->'$.signed')").Check(testkit.Rows(
		"IndexMerge 27.00 root  type: union",
		"├─IndexRangeScan(Build) 27.00 cop[tikv] table:t, index:ij_signed(cast(json_extract(`j`, _utf8mb4'$.signed') as signed array)) range:[1,1], keep order:false, stats:partial[j:unInitialized]",
		"└─TableRowIDScan(Probe) 27.00 cop[tikv] table:t keep order:false, stats:partial[j:unInitialized]",
	))
	tk.MustQuery("explain format = brief select /*+ use_index_merge(t, ij_unsigned) */ * from t where 1 member of (j->'$.unsigned')").Check(testkit.Rows(
		"IndexMerge 18.00 root  type: union",
		"├─IndexRangeScan(Build) 18.00 cop[tikv] table:t, index:ij_unsigned(cast(json_extract(`j`, _utf8mb4'$.unsigned') as unsigned array)) range:[1,1], keep order:false, stats:partial[j:unInitialized]",
		"└─TableRowIDScan(Probe) 18.00 cop[tikv] table:t keep order:false, stats:partial[j:unInitialized]",
	))
	tk.MustQuery("explain format = brief select /*+ use_index_merge(t, ij_binary) */ * from t where '1' member of (j->'$.bin')").Check(testkit.Rows(
		"IndexMerge 14.83 root  type: union",
		"├─IndexRangeScan(Build) 14.83 cop[tikv] table:t, index:ij_binary(cast(json_extract(`j`, _utf8mb4'$.bin') as binary(50) array)) range:[\"1\",\"1\"], keep order:false, stats:partial[j:unInitialized]",
		"└─TableRowIDScan(Probe) 14.83 cop[tikv] table:t keep order:false, stats:partial[j:unInitialized]",
	))
	tk.MustQuery("explain format = brief select /*+ use_index_merge(t, ij_char) */ * from t where '1' member of (j->'$.char')").Check(testkit.Rows(
		"IndexMerge 13.50 root  type: union",
		"├─IndexRangeScan(Build) 13.50 cop[tikv] table:t, index:ij_char(cast(json_extract(`j`, _utf8mb4'$.char') as char(50) array)) range:[\"1\",\"1\"], keep order:false, stats:partial[j:unInitialized]",
		"└─TableRowIDScan(Probe) 13.50 cop[tikv] table:t keep order:false, stats:partial[j:unInitialized]",
	))

	// 4. check stats content in the memory
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustQuery("show stats_meta").CheckAt([]int{0, 1, 4, 5}, testkit.Rows("test t 0 27"))
	tk.MustQuery("show stats_histograms").CheckAt([]int{0, 1, 3, 4, 6, 7, 8, 9, 10}, testkit.Rows(
		// db_name, table_name, column_name, is_index, distinct_count, null_count, avg_col_size, correlation, load_status
		"test t a 0 1 0 1 1 allEvicted",
		"test t ia 1 1 0 0 0 allLoaded",
		"test t ij_signed 1 11 0 0 0 allLoaded",
		"test t ij_unsigned 1 6 0 0 0 allLoaded",
		"test t ij_double 1 7 0 0 0 allLoaded",
		"test t ij_binary 1 15 0 0 0 allLoaded",
		"test t ij_char 1 11 0 0 0 allLoaded",
	))
	tk.MustQuery("show stats_topn").Check(testkit.Rows(
		// db_name, table_name, partition_name, column_name, is_index, value, count
		"test t  ia 1 1 27",
		"test t  ij_signed 1 0 27",
		"test t  ij_signed 1 1 27",
		"test t  ij_signed 1 2 27",
		"test t  ij_unsigned 1 0 27",
		"test t  ij_unsigned 1 3 27",
		"test t  ij_unsigned 1 4 27",
		"test t  ij_double 1 -21.5 27",
		"test t  ij_double 1 0 27",
		"test t  ij_double 1 2.15 27",
		"test t  ij_binary 1 aaaaaa 27",
		"test t  ij_binary 1 bbbb 27",
		"test t  ij_binary 1 ccc 27",
		"test t  ij_char 1 aaa 27",
		"test t  ij_char 1 asdf 27",
		"test t  ij_char 1 cccccc 27",
	))
	tk.MustQuery("show stats_buckets").Check(testkit.Rows(
		// db_name, table_name, partition_name, column_name, is_index, bucket_id, count, repeats, lower_bound, upper_bound, ndv
		"test t  ij_signed 1 0 16 16 -40000 -40000 0",
		"test t  ij_signed 1 1 17 1 -300 -300 0",
		"test t  ij_signed 1 2 28 11 -5 -5 0",
		"test t  ij_signed 1 3 54 26 4 4 0",
		"test t  ij_signed 1 4 70 16 5 5 0",
		"test t  ij_signed 1 5 71 1 100 100 0",
		"test t  ij_signed 1 6 98 27 300 300 0",
		"test t  ij_signed 1 7 109 11 13245 13245 0",
		"test t  ij_unsigned 1 0 16 16 12 12 0",
		"test t  ij_unsigned 1 1 43 27 600 600 0",
		"test t  ij_unsigned 1 2 54 11 3112 3112 0",
		"test t  ij_double 1 0 8 8 -12.000005 -12.000005 0",
		"test t  ij_double 1 1 27 19 0.000005 0.000005 0",
		"test t  ij_double 1 2 54 27 10.555555 10.555555 0",
		"test t  ij_double 1 3 73 19 10.9876 10.9876 0",
		"test t  ij_binary 1 0 26 26 0000 0000 0",
		"test t  ij_binary 1 1 45 19 1234 1234 0",
		"test t  ij_binary 1 2 46 1 3796 3796 0",
		"test t  ij_binary 1 3 54 8 5678 5678 0",
		"test t  ij_binary 1 4 78 24 asdf asdf 0",
		"test t  ij_binary 1 5 81 3 egfb egfb 0",
		"test t  ij_binary 1 6 89 8 ghjk ghjk 0",
		"test t  ij_binary 1 7 92 3 nfre nfre 0",
		"test t  ij_binary 1 8 119 27 ppp ppp 0",
		"test t  ij_binary 1 9 143 24 qwer qwer 0",
		"test t  ij_binary 1 10 151 8 yuiop yuiop 0",
		"test t  ij_binary 1 11 178 27 zzzz zzzz 0",
		"test t  ij_char 1 0 24 24 !@#$ !@#$ 0",
		"test t  ij_char 1 1 26 2 %*$%#@qwe %*$%#@qwe 0",
		"test t  ij_char 1 2 27 1 %*asdf@ %*asdf@ 0",
		"test t  ij_char 1 3 54 27 eee eee 0",
		"test t  ij_char 1 4 56 2 k!@cvd k!@cvd 0",
		"test t  ij_char 1 5 57 1 kicvd kicvd 0",
		"test t  ij_char 1 6 81 24 qwer qwer 0",
		"test t  ij_char 1 7 108 27 yuiop yuiop 0",
	))
}

func TestAnalyzePartitionVerify(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	sql := "create table t(a int,b varchar(100),c int,INDEX idx_c(c)) PARTITION BY RANGE ( a ) ("
	for n := 100; n < 1000; n = n + 100 {
		sql += "PARTITION p" + fmt.Sprint(n) + " VALUES LESS THAN (" + fmt.Sprint(n) + "),"
	}
	sql += "PARTITION p" + fmt.Sprint(1000) + " VALUES LESS THAN MAXVALUE)"
	tk.MustExec(sql)
	// insert random data into table t
	insertStr := "insert into t (a,b,c) values(0, 'abc', 0)"
	for i := 1; i < 1000; i++ {
		insertStr += fmt.Sprintf(" ,(%d, '%s', %d)", i, "abc", i)
	}
	insertStr += ";"
	tk.MustExec(insertStr)
	tk.MustExec("analyze table t")

	result := tk.MustQuery("show stats_histograms where Db_name='test'").Sort()
	require.NotNil(t, result)
	require.Len(t, result.Rows(), 4+4*10) // 4 columns * 10 partiion+ 4 global columns
	for _, row := range result.Rows() {
		if row[2] == "global" {
			if row[3] == "b" {
				// global column b has 1 distinct value
				require.Equal(t, "1", row[6])
			} else {
				require.Equal(t, "1000", row[6])
			}
		} else {
			if row[3] == "b" {
				require.Equal(t, "1", row[6])
			} else {
				require.Equal(t, "100", row[6])
			}
		}
	}
}
