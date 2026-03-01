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
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics"
	statstestutil "github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/analyzehelper"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
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
		analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "c")
		tk.MustExec("analyze table t")

		is := tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
		table, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		pi := table.Meta().GetPartitionInfo()
		require.NotNil(t, pi)
		do, err := session.GetDomain(store)
		require.NoError(t, err)
		handle := do.StatsHandle()
		for _, def := range pi.Definitions {
			statsTbl := handle.GetPhysicalTableStats(def.ID, table.Meta())
			require.False(t, statsTbl.Pseudo)
			require.Equal(t, statsTbl.ColNum(), 3)
			require.Equal(t, statsTbl.IdxNum(), 1)
			statsTbl.ForEachColumnImmutable(func(_ int64, col *statistics.Column) bool {
				require.Greater(t, col.Len()+col.TopN.Num(), 0)
				return false
			})
			statsTbl.ForEachIndexImmutable(func(_ int64, idx *statistics.Index) bool {
				require.Greater(t, idx.Len()+idx.TopN.Num(), 0)
				return false
			})
		}

		tk.MustExec("drop table t")
		tk.MustExec(createTable)
		for i := 1; i < 21; i++ {
			tk.MustExec(fmt.Sprintf(`insert into t values (%d, %d, "hello")`, i, i))
		}
		tk.MustExec("alter table t analyze partition p0")
		is = tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
		table, err = is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
		require.NoError(t, err)
		pi = table.Meta().GetPartitionInfo()
		require.NotNil(t, pi)

		for i, def := range pi.Definitions {
			statsTbl := handle.GetPhysicalTableStats(def.ID, table.Meta())
			if i == 0 {
				require.False(t, statsTbl.Pseudo)
				require.Equal(t, statsTbl.ColNum(), 3)
				require.Equal(t, statsTbl.IdxNum(), 1)
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

func TestAnalyzeTooLongColumns(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a json)")
	value := fmt.Sprintf(`{"x":"%s"}`, strings.Repeat("x", mysql.MaxFieldVarCharLength))
	tk.MustExec(fmt.Sprintf("insert into t values ('%s')", value))

	tk.MustExec("set @@session.tidb_analyze_skip_column_types = ''")
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "a")
	tk.MustExec("analyze table t")
	is := tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	table, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	tbl := dom.StatsHandle().GetPhysicalTableStats(tableInfo.ID, tableInfo)
	col1 := tbl.GetCol(1)
	require.Equal(t, 0, col1.Len())
	require.Equal(t, 0, col1.TopN.Num())
	require.Equal(t, int64(65559), col1.TotColSize)
}

func TestExtractTopN(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists test_extract_topn")
	tk.MustExec("use test_extract_topn")
	tk.MustExec("drop table if exists test_extract_topn")
	tk.MustExec("create table test_extract_topn(a int primary key, b int, index index_b(b))")
	tk.MustExec("set @@session.tidb_analyze_version=2")
	for i := range 10 {
		tk.MustExec(fmt.Sprintf("insert into test_extract_topn values (%d, %d)", i, i))
	}
	for i := range 10 {
		tk.MustExec(fmt.Sprintf("insert into test_extract_topn values (%d, 0)", i+10))
	}
	tk.MustExec("analyze table test_extract_topn")
	is := dom.InfoSchema()
	table, err := is.TableByName(context.Background(), ast.NewCIStr("test_extract_topn"), ast.NewCIStr("test_extract_topn"))
	require.NoError(t, err)
	tblInfo := table.Meta()
	tblStats := dom.StatsHandle().GetPhysicalTableStats(tblInfo.ID, tblInfo)
	colStats := tblStats.GetCol(tblInfo.Columns[1].ID)
	require.Len(t, colStats.TopN.TopN, 10)
	item := colStats.TopN.TopN[0]
	require.Equal(t, uint64(11), item.Count)
	idxStats := tblStats.GetIdx(tblInfo.Indices[0].ID)
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
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
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
	err := statstestutil.HandleNextDDLEventWithTxn(statsHandle)
	require.NoError(t, err)
	is := tk.Session().(sessionctx.Context).GetInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	tid := tblInfo.ID
	tk.MustExec(fmt.Sprintf("update mysql.stats_meta set count = 220000 where table_id=%d", tid))
	require.NoError(t, statsHandle.Update(context.Background(), is))
	result := tk.MustQuery("show stats_meta where table_name = 't'")
	require.Equal(t, "220000", result.Rows()[0][5])
	tk.MustExec("analyze table t")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 0.500000 for table test.t, reason to use this rate is \"use min(1, 110000/220000) as the sample-rate=0.5\"",
	))
	tk.MustExec("insert into t values(1),(1),(1)")
	tk.MustExec("flush stats_delta")
	require.NoError(t, statsHandle.Update(context.Background(), is))
	result = tk.MustQuery("show stats_meta where table_name = 't'")
	require.Equal(t, "3", result.Rows()[0][5])
	tk.MustExec("analyze table t")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/3) as the sample-rate=1\"",
	))
}

func TestIssue20874(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("delete from mysql.stats_histograms")
	tk.MustExec("create table t (a char(10) collate utf8mb4_unicode_ci not null, b char(20) collate utf8mb4_general_ci not null, key idxa(a), key idxb(b))")
	tk.MustExec("insert into t values ('#', 'C'), ('$', 'c'), ('a', 'a')")
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

func TestAnalyzeSamplingWorkPanic(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("create table t(a int, index idx(a))")
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
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/calcSampleRateByStorageCount", "return(1)"))
	tk.MustExec("use test")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("create table small_table_inject_pd(a int)")
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "small_table_inject_pd", "a")
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
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "small_table_inject_pd_with_partition", "a")
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

func TestAnalyzeColumnsAfterAnalyzeAll(t *testing.T) {
	for _, val := range []ast.ColumnChoice{ast.ColumnList, ast.PredicateColumns} {
		func(choice ast.ColumnChoice) {
			store, dom := testkit.CreateMockStoreAndDomain(t)

			tk := testkit.NewTestKit(t, store)
			h := dom.StatsHandle()
			tk.MustExec("use test")
			tk.MustExec("drop table if exists t")
			tk.MustExec("set @@tidb_analyze_version = 2")
			tk.MustExec("create table t (a int, b int)")
			tk.MustExec("insert into t (a,b) values (1,1), (1,1), (2,2), (2,2), (3,3), (4,4)")
			tk.MustExec("flush stats_delta")

			is := dom.InfoSchema()
			tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
			require.NoError(t, err)
			tblID := tbl.Meta().ID

			tk.MustExec("analyze table t all columns with 2 topn, 2 buckets")
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
			tk.MustExec("flush stats_delta")

			switch choice {
			case ast.ColumnList:
				tk.MustExec("analyze table t columns b with 2 topn, 2 buckets")
			case ast.PredicateColumns:
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
				testkit.Rows("0 1 4 0 6 2 1",
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
	store, _ := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("flush stats_delta")
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "a", "b")

	tk.MustExec(`analyze table t`)
	tk.MustQuery(`show warnings`).Sort().Check(testkit.Rows(
		`Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is "use min(1, 110000/10000) as the sample-rate=1"`))
	tk.MustExec(`insert into t values (1, 1), (2, 2), (3, 3)`)
	tk.MustExec("flush stats_delta")
	tk.MustExec(`analyze table t`)
	tk.MustQuery(`show warnings`).Sort().Check(testkit.Rows(
		`Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is "TiDB assumes that the table is empty, use sample-rate=1"`))
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
	statistics.AutoAnalyzeMinCnt = 0
	defer func() {
		statistics.AutoAnalyzeMinCnt = 1000
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_start_time='%v'", oriStart))
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_end_time='%v'", oriEnd))
	}()
	tk.MustExec(fmt.Sprintf("set @@tidb_analyze_version = %v", ver))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1,2), (3,4)")
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "a", "b")
	is := dom.InfoSchema()
	h := dom.StatsHandle()
	tk.MustExec("flush stats_delta")
	tk.MustExec("analyze table t")
	tk.MustExec("insert into t values (5,6), (7,8), (9, 10)")
	tk.MustExec("flush stats_delta")
	require.NoError(t, h.Update(context.Background(), is))
	table, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	lastVersion := h.GetPhysicalTableStats(tableInfo.ID, tableInfo).Version
	tk.MustExec("set global tidb_auto_analyze_start_time='00:00 +0000'")
	tk.MustExec("set global tidb_auto_analyze_end_time='23:59 +0000'")
	jobInfo := "auto analyze "
	if ver == 1 {
		jobInfo += "columns"
	} else {
		jobInfo += "table all columns with 256 buckets, 100 topn, 1 samplerate"
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
			currentVersion := h.GetPhysicalTableStats(tableInfo.ID, tableInfo).Version
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
	testKillAutoAnalyze(t, 2)
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
			JobInfo:       "table all columns with 256 buckets, 100 topn, 1 samplerate",
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
		statsHandle := domain.GetDomain(tk.Session()).StatsHandle()
		statsHandle.StartAnalyzeJob(job)
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

		// UpdateAnalyzeJobProgress requires the interval between two updates to mysql.analyze_jobs is more than 5 second.
		// Hence we fake last dump time as 10 second ago in order to make update to mysql.analyze_jobs happen.
		lastDumpTime := time.Now().Add(-10 * time.Second)
		job.Progress.SetLastDumpTime(lastDumpTime)
		const smallCount int64 = 100
		statsHandle.UpdateAnalyzeJobProgress(job, smallCount)
		// Delta count doesn't reach threshold so we don't dump it to mysql.analyze_jobs
		require.Equal(t, smallCount, job.Progress.GetDeltaCount())
		require.Equal(t, lastDumpTime, job.Progress.GetLastDumpTime())
		rows = tk.MustQuery("show analyze status").Rows()
		require.Equal(t, "0", rows[0][4])

		const largeCount int64 = 15000000
		statsHandle.UpdateAnalyzeJobProgress(job, largeCount)
		// Delta count reaches threshold so we dump it to mysql.analyze_jobs and update last dump time.
		require.Equal(t, int64(0), job.Progress.GetDeltaCount())
		require.True(t, job.Progress.GetLastDumpTime().After(lastDumpTime))
		lastDumpTime = job.Progress.GetLastDumpTime()
		rows = tk.MustQuery("show analyze status").Rows()
		require.Equal(t, strconv.FormatInt(smallCount+largeCount, 10), rows[0][4])

		statsHandle.UpdateAnalyzeJobProgress(job, largeCount)
		// We have just updated mysql.analyze_jobs in the previous step so we don't update it until 5 second passes or the analyze job is over.
		require.Equal(t, largeCount, job.Progress.GetDeltaCount())
		require.Equal(t, lastDumpTime, job.Progress.GetLastDumpTime())
		rows = tk.MustQuery("show analyze status").Rows()
		require.Equal(t, strconv.FormatInt(smallCount+largeCount, 10), rows[0][4])

		var analyzeErr error
		if result == statistics.AnalyzeFailed {
			analyzeErr = errors.Errorf("analyze meets error")
		}
		statsHandle.FinishAnalyzeJob(job, analyzeErr, statistics.TableAnalysisJob)
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
		JobInfo:       "table all columns with 256 buckets, 100 topn, 1 samplerate",
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
	checkJobInfo("analyze table all indexes, columns b, c, d with 2 buckets, 2 topn, 1 samplerate")
	tk.MustExec("set global tidb_persist_analyze_options = 1")
	tk.MustExec("select * from t where c > 1")
	h := dom.StatsHandle()
	require.NoError(t, h.DumpColStatsUsageToKV())
	tk.MustExec("analyze table t predicate columns with 2 topn, 2 buckets")
	checkJobInfo("analyze table all indexes, columns b, c, d with 2 buckets, 2 topn, 1 samplerate")
	tk.MustExec("analyze table t")
	checkJobInfo("analyze table all indexes, columns b, c, d with 2 buckets, 2 topn, 1 samplerate")
	tk.MustExec("analyze table t columns a with 1 topn, 3 buckets")
	checkJobInfo("analyze table all indexes, columns a, b, d with 3 buckets, 1 topn, 1 samplerate")
	tk.MustExec("analyze table t")
	checkJobInfo("analyze table all indexes, columns a, b, d with 3 buckets, 1 topn, 1 samplerate")
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
	table, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)

	// analyze table only sets table options and gen globalStats
	tk.MustExec("analyze table t columns a,c with 1 topn, 3 buckets")
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tbl := h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	lastVersion := tbl.Version
	// both globalStats and partition stats generated and options saved for column a,c
	require.Equal(t, 3, len(tbl.GetCol(tableInfo.Columns[0].ID).Buckets))
	require.Equal(t, 1, len(tbl.GetCol(tableInfo.Columns[0].ID).TopN.TopN))
	require.Equal(t, 3, len(tbl.GetCol(tableInfo.Columns[2].ID).Buckets))
	require.Equal(t, 1, len(tbl.GetCol(tableInfo.Columns[2].ID).TopN.TopN))
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
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tbl = h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	lastVersion = tbl.Version
	require.Equal(t, 3, len(tbl.GetCol(tableInfo.Columns[0].ID).Buckets))
	require.Equal(t, 1, len(tbl.GetCol(tableInfo.Columns[0].ID).TopN.TopN))
	require.Equal(t, 3, len(tbl.GetCol(tableInfo.Columns[2].ID).Buckets))
	require.Equal(t, 1, len(tbl.GetCol(tableInfo.Columns[2].ID).TopN.TopN))
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
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tbl = h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	require.Equal(t, 2, len(tbl.GetCol(tableInfo.Columns[0].ID).Buckets))
	require.Equal(t, 2, len(tbl.GetCol(tableInfo.Columns[0].ID).TopN.TopN))
	require.Equal(t, 2, len(tbl.GetCol(tableInfo.Columns[2].ID).Buckets))
	require.Equal(t, 2, len(tbl.GetCol(tableInfo.Columns[2].ID).TopN.TopN))
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
	table, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)

	// analyze partition under static mode with options
	tk.MustExec("analyze table t partition p0 columns a,c with 1 topn, 3 buckets")
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tbl := h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	p0 := h.GetPhysicalTableStats(pi.Definitions[0].ID, tableInfo)
	p1 := h.GetPhysicalTableStats(pi.Definitions[1].ID, tableInfo)
	lastVersion := tbl.Version
	require.Equal(t, 3, len(p0.GetCol(tableInfo.Columns[0].ID).Buckets))
	require.Equal(t, 3, len(p0.GetCol(tableInfo.Columns[2].ID).Buckets))
	require.Equal(t, 0, len(p1.GetCol(tableInfo.Columns[0].ID).Buckets))
	require.Equal(t, 0, len(tbl.GetCol(tableInfo.Columns[0].ID).Buckets))
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
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tbl = h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	lastVersion = tbl.Version
	p0, err = h.TableStatsFromStorage(tableInfo, pi.Definitions[0].ID, true, 0)
	require.NoError(t, err)
	p1, err = h.TableStatsFromStorage(tableInfo, pi.Definitions[1].ID, true, 0)
	require.NoError(t, err)
	require.Equal(t, 0, len(p0.GetCol(tableInfo.Columns[0].ID).Buckets))
	require.Equal(t, len(tbl.GetCol(tableInfo.Columns[0].ID).Buckets), len(p0.GetCol(tableInfo.Columns[0].ID).Buckets))
	require.Equal(t, len(tbl.GetCol(tableInfo.Columns[0].ID).Buckets), len(p1.GetCol(tableInfo.Columns[0].ID).Buckets))
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
	tk.MustQuery("select * from t where a > 1 and b > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tbl = h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	lastVersion = tbl.Version
	require.Equal(t, 2, len(tbl.GetCol(tableInfo.Columns[1].ID).Buckets))
	require.Equal(t, 2, len(tbl.GetCol(tableInfo.Columns[3].ID).Buckets))
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
	tk.MustQuery("select * from t where a > 1 and b > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tbl = h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	lastVersion = tbl.Version
	require.Equal(t, 2, len(tbl.GetCol(tableInfo.Columns[3].ID).Buckets))
	require.Equal(t, 2, len(tbl.GetCol(tableInfo.Columns[3].ID).TopN.TopN))

	// analyze table under dynamic mode with specified options with old table-level & partition-level options
	tk.MustExec("analyze table t with 1 topn")
	tk.MustQuery("select * from t where a > 1 and b > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tbl = h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.Greater(t, tbl.Version, lastVersion)
	require.Equal(t, 2, len(tbl.GetCol(tableInfo.Columns[1].ID).Buckets))
	require.Equal(t, 2, len(tbl.GetCol(tableInfo.Columns[3].ID).Buckets))
	require.Equal(t, 1, len(tbl.GetCol(tableInfo.Columns[1].ID).TopN.TopN))
	require.Equal(t, 1, len(tbl.GetCol(tableInfo.Columns[3].ID).TopN.TopN))
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
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "a", "b", "c", "d")
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	is := dom.InfoSchema()
	table, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
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
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tbl := h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	lastVersion := tbl.Version
	require.NotEqual(t, 3, len(tbl.GetCol(tableInfo.Columns[2].ID).Buckets))
	require.NotEqual(t, 3, len(tbl.GetCol(tableInfo.Columns[3].ID).Buckets))

	tk.MustExec("analyze table t partition p0")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0, reason to use this rate is \"use min(1, 110000/9) as the sample-rate=1\"",
	))
	tbl = h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.Greater(t, tbl.Version, lastVersion) // global stats updated
}

func TestAnalyzePartitionStaticToDynamic(t *testing.T) {
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
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
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "a", "b", "c", "d")
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	is := dom.InfoSchema()
	table, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	pi := tableInfo.GetPartitionInfo()
	require.NotNil(t, pi)

	// generate old partition stats
	tk.MustExec("set global tidb_persist_analyze_options = false")
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	tk.MustExec("analyze table t partition p0 columns a,c with 1 topn, 3 buckets")
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	p0 := h.GetPhysicalTableStats(pi.Definitions[0].ID, tableInfo)
	require.Equal(t, 3, len(p0.GetCol(tableInfo.Columns[2].ID).Buckets))

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
	//require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	//tbl := h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	//require.Equal(t, 0, len(tbl.Columns))

	// ignore both p0's 3 buckets, persisted-partition-options' 1 bucket, just use table-level 2 buckets
	tk.MustExec("analyze table t partition p0")
	tk.MustQuery("select * from t where a > 1 and b > 1 and c > 1 and d > 1")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tbl := h.GetPhysicalTableStats(tableInfo.ID, tableInfo)
	require.Equal(t, 2, len(tbl.GetCol(tableInfo.Columns[2].ID).Buckets))
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
	table, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
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
	table, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
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
	require.Equal(t, int64(6), tbl.GetCol(tableInfo.Columns[0].ID).Histogram.NDV)
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
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "a")
	h := dom.StatsHandle()
	err := statstestutil.HandleNextDDLEventWithTxn(h)
	require.NoError(t, err)
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tid := tbl.Meta().ID
	tk.MustExec("insert into t values(1),(2),(3)")
	tk.MustExec("flush stats_delta")
	err = h.Update(context.Background(), dom.InfoSchema())
	require.NoError(t, err)
	tk.MustExec("analyze table t")
	tk.MustQuery(fmt.Sprintf("select count, modify_count from mysql.stats_meta where table_id = %d", tid)).Check(testkit.Rows(
		"3 0",
	))

	originalVal1 := statistics.AutoAnalyzeMinCnt
	originalVal2 := tk.MustQuery("select @@global.tidb_auto_analyze_ratio").Rows()[0][0].(string)
	statistics.AutoAnalyzeMinCnt = 0
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.001")
	defer func() {
		statistics.AutoAnalyzeMinCnt = originalVal1
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_ratio = %v", originalVal2))
	}()

	tk.MustExec("begin")
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	startTS := txn.StartTS()
	tk.MustExec("commit")
	tk.MustExec("insert into t values(4),(5),(6)")
	tk.MustExec("flush stats_delta")
	err = h.Update(context.Background(), dom.InfoSchema())
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
	tk.MustExec("flush stats_delta")

	tk.MustExec("analyze table t columns a")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(""+
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"",
		"Warning 1105 Columns b are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats"))
	tk.MustQuery("select job_info from mysql.analyze_jobs where table_schema = 'test' and table_name = 't'").Sort().Check(
		testkit.Rows(
			"analyze index idx_c",
			"analyze table index idx_b, columns a, b with 256 buckets, 100 topn, 1 samplerate",
		))

	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tblInfo := tbl.Meta()
	stats := h.GetPhysicalTableStats(tblInfo.ID, tblInfo)
	require.True(t, stats.GetCol(tblInfo.Columns[0].ID).IsStatsInitialized())
	require.True(t, stats.GetCol(tblInfo.Columns[1].ID).IsStatsInitialized())
	require.False(t, stats.GetCol(tblInfo.Columns[2].ID).IsStatsInitialized())
	require.True(t, stats.GetIdx(tblInfo.Indices[0].ID).IsStatsInitialized())
	require.True(t, stats.GetIdx(tblInfo.Indices[1].ID).IsStatsInitialized())
}

// TestAnalyzeMVIndex tests analyzing the mv index use some real data in the table.
// It checks the analyze jobs, async loading and the stats content in the memory.
func TestAnalyzeMVIndex(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/DebugAnalyzeJobOperations", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/DebugAnalyzeJobOperations"))
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
	err := statstestutil.HandleNextDDLEventWithTxn(h)
	require.NoError(t, err)
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
	for i := range 3 {
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
	tk.MustExec("flush stats_delta")

	// 2. analyze and check analyze jobs
	tk.MustExec("analyze table t with 1 samplerate, 3 topn")
	tk.MustQuery("select id, table_schema, table_name, partition_name, job_info, processed_rows, state from mysql.analyze_jobs order by id").
		Check(testkit.Rows("1 test t  analyze table index ia, column a with 256 buckets, 3 topn, 1 samplerate 27 finished",
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
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
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
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tk.MustQuery("show stats_meta").CheckAt([]int{0, 1, 4, 5}, testkit.Rows("test t 0 27"))
	tk.MustQuery("show stats_histograms").Sort().CheckAt([]int{0, 1, 3, 4, 6, 7, 8, 9, 10}, testkit.Rows(
		// db_name, table_name, column_name, is_index, distinct_count, null_count, avg_col_size, correlation, load_status
		"test t a 0 1 0 1 1 allLoaded",
		"test t ia 1 1 0 0 0 allLoaded",
		"test t ij_binary 1 15 0 0 0 allLoaded",
		"test t ij_char 1 11 0 0 0 allLoaded",
		"test t ij_double 1 7 0 0 0 allLoaded",
		"test t ij_signed 1 11 0 0 0 allLoaded",
		"test t ij_unsigned 1 6 0 0 0 allLoaded",
	))
	tk.MustQuery("show stats_topn").Check(testkit.Rows(
		// db_name, table_name, partition_name, column_name, is_index, value, count
		"test t  a 0 1 27",
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
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "a", "b", "c")
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

func TestIssue55438(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t0(c0 NUMERIC , c1 BIGINT UNSIGNED  AS ((CASE 0 WHEN false THEN 1358571571 ELSE TRIM(c0) END )));")
	tk.MustExec("CREATE INDEX i0 ON t0(c1);")
	tk.MustExec("analyze table t0")
}

func TestIssue61609(t *testing.T) {
	// Analyze table with only 1 sample (of 10 rows) - TopN result should multiply the 1 value by 10.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values (0),(0),(0),(0),(0),(0),(0),(0),(0),(0);")

	tk.MustExec("explain select * from t where a = 0")
	analyzehelper.TriggerPredicateColumnsCollection(t, tk, store, "t", "a")
	tk.MustExec("analyze table t with 1 topn, 1 samples")
	tk.MustExec("explain select * from t where a = 0")

	h := dom.StatsHandle()
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))
	tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 0 10",
	))
}

// TestGeneratedColumns verifies that statistics collection works correctly for generated columns and their indexes.
//
// | Type                                             | Stats Collected | Reason                                              |
// |--------------------------------------------------|-----------------|-----------------------------------------------------|
// | Base JSON column (data)                          | ✅ Yes          | Regular column (used by the generated columns)     |
// | Virtual generated column                         | ❌ No           | Cannot evaluate expression on TiKV side            |
// | Stored generated column                          | ✅ Yes          | Value is stored in TiKV                            |
// | JSON column (not used by the generated columns)  | ❌ No           | Excluded by tidb_analyze_skip_column_types setting |
// | Index on virtual column                          | ✅ Yes          | Index entries are stored in TiKV                   |
// | Index on stored column                           | ✅ Yes          | Index entries are stored in TiKV                   |
func TestGeneratedColumns(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Create table with JSON column and generated columns
	tk.MustExec(`CREATE TABLE test_gen_cols (
		id INT PRIMARY KEY,
		data JSON,
		virtual_col VARCHAR(50) AS (JSON_UNQUOTE(JSON_EXTRACT(data, '$.name'))) VIRTUAL,
		stored_col VARCHAR(50) AS (JSON_UNQUOTE(JSON_EXTRACT(data, '$.status'))) STORED,
		json_but_not_used_by_generated_column JSON,
		INDEX idx_virtual (virtual_col),
		INDEX idx_stored (stored_col)
	)`)

	// Insert test data with simple JSON
	tk.MustExec(`INSERT INTO test_gen_cols (id, data, json_but_not_used_by_generated_column) VALUES
		(1, '{"name": "user1", "status": "active"}', '{"category": "admin", "level": 1}'),
		(2, '{"name": "user2", "status": "inactive"}', '{"category": "user", "level": 2}'),
		(3, '{"name": "user3", "status": "active"}', '{"category": "user", "level": 1}')`)

	// Analyze the table
	tk.MustExec("ANALYZE TABLE test_gen_cols")

	h := dom.StatsHandle()
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("test_gen_cols"))
	require.NoError(t, err)

	// Get the table statistics
	tblStats := h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.NotNil(t, tblStats)
	require.True(t, tblStats.IsAnalyzed())

	// For the base column used by generated columns, we should collect statistics even it is not used by any indexes.
	require.True(t, tblStats.GetCol(tbl.Meta().Columns[1].ID).IsAnalyzed())

	// For virtual generated columns, we don't collect statistics because we cannot evaluate the expression on the TiKV side
	require.False(t, tblStats.GetCol(tbl.Meta().Columns[2].ID).IsAnalyzed())

	// For stored generated columns, we collect statistics because the values are stored in TiKV
	require.True(t, tblStats.GetCol(tbl.Meta().Columns[3].ID).IsAnalyzed())

	// For JSON columns that are not used by generated columns, we don't collect statistics because we exclude it by tidb_analyze_skip_column_types.
	require.False(t, tblStats.GetCol(tbl.Meta().Columns[4].ID).IsAnalyzed())

	// For indexes on generated columns, we collect statistics because index entries are stored in TiKV regardless of whether the column is virtual or stored
	require.True(t, tblStats.GetIdx(tbl.Meta().Indices[0].ID).IsAnalyzed())
	require.True(t, tblStats.GetIdx(tbl.Meta().Indices[1].ID).IsAnalyzed())
}

// TestSkipStatsForGeneratedColumnsOnSkippedColumns verifies that when we skip JSON columns, the generated columns that depend on them are also skipped.
// See: https://github.com/pingcap/tidb/issues/62465
func TestSkipStatsForGeneratedColumnsOnSkippedColumns(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Create table with JSON column and generated columns
	tk.MustExec(`CREATE TABLE test_gen_cols (
		id INT PRIMARY KEY,
		data JSON,
		virtual_col VARCHAR(50) AS (JSON_UNQUOTE(JSON_EXTRACT(data, '$.name'))) VIRTUAL,
		stored_col VARCHAR(50) AS (JSON_UNQUOTE(JSON_EXTRACT(data, '$.status'))) STORED
	)`)
	// Insert test data with simple JSON
	tk.MustExec(`INSERT INTO test_gen_cols (id, data) VALUES
		(1, '{"name": "user1", "status": "active"}'),
		(2, '{"name": "user2", "status": "inactive"}'),
		(3, '{"name": "user3", "status": "active"}')`)

	// Explicitly set tidb_analyze_skip_column_types to skip JSON columns
	tk.MustExec("set @@tidb_analyze_skip_column_types = 'json, text, blob'")
	// Check tidb_analyze_skip_column_types setting
	tk.MustQuery("select @@tidb_analyze_skip_column_types").Check(testkit.Rows("json,text,blob"))
	// Analyze the table with all columns
	tk.MustExec("ANALYZE TABLE test_gen_cols all columns")
	h := dom.StatsHandle()
	tbl, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("test_gen_cols"))
	require.NoError(t, err)
	// Get the table statistics
	tblStats := h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.NotNil(t, tblStats)
	require.True(t, tblStats.IsAnalyzed())
	// For JSON column, it should not collect statistics because we skip it
	require.False(t, tblStats.GetCol(tbl.Meta().Columns[1].ID).IsAnalyzed())
	// For virtual generated columns, because it depends on the skipped JSON column, we also skip it
	require.False(t, tblStats.GetCol(tbl.Meta().Columns[2].ID).IsAnalyzed())
	// For stored columns, because it depends on the skipped JSON column, we also skip it
	require.False(t, tblStats.GetCol(tbl.Meta().Columns[3].ID).IsAnalyzed())

	// Test the predicate columns.
	tk.MustExec("select * from test_gen_cols where virtual_col = 'a' and stored_col = 'b'")
	require.NoError(t, h.DumpColStatsUsageToKV())
	// Check the predicate columns collection.
	rows := tk.MustQuery("show column_stats_usage where table_name = 'test_gen_cols'").Rows()
	require.Len(t, rows, 3)
	require.Equal(t, "id", rows[0][3])
	require.Equal(t, "virtual_col", rows[1][3])
	require.Equal(t, "stored_col", rows[2][3])

	tk.MustExec("ANALYZE TABLE test_gen_cols")
	tblStats = h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.NotNil(t, tblStats)
	require.True(t, tblStats.IsAnalyzed())
	// For JSON column, it should not collect statistics because we skip it
	require.False(t, tblStats.GetCol(tbl.Meta().Columns[1].ID).IsAnalyzed())
	// For virtual generated columns, because it depends on the skipped JSON column, we also skip it
	require.False(t, tblStats.GetCol(tbl.Meta().Columns[2].ID).IsAnalyzed())
	// For stored columns, because it depends on the skipped JSON column, we also skip it
	require.False(t, tblStats.GetCol(tbl.Meta().Columns[3].ID).IsAnalyzed())

	// Remove the skip setting and re-analyze
	tk.MustExec("set @@tidb_analyze_skip_column_types = 'text, blob'")
	tk.MustQuery("select @@tidb_analyze_skip_column_types").Check(testkit.Rows("text,blob"))
	tk.MustExec("ANALYZE TABLE test_gen_cols")
	tblStats = h.GetPhysicalTableStats(tbl.Meta().ID, tbl.Meta())
	require.NotNil(t, tblStats)
	require.True(t, tblStats.IsAnalyzed())
	// For JSON column, it should be analyzed now
	require.True(t, tblStats.GetCol(tbl.Meta().Columns[1].ID).IsAnalyzed())
	// For virtual generated columns, we still could not collect statistics because we couldn't evaluate the expression on the TiKV side
	require.False(t, tblStats.GetCol(tbl.Meta().Columns[2].ID).IsAnalyzed())
	// For stored columns, we can collect statistics because the values are stored in TiKV
	require.True(t, tblStats.GetCol(tbl.Meta().Columns[3].ID).IsAnalyzed())
}
