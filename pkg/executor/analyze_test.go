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
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/store/mockstore"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/stretchr/testify/require"
)

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
	b, err := os.ReadFile("testdata/analyze_test_data.sql")
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
		tk.MustExec(fmt.Sprintf("set @@global.tidb_merge_partition_stats_concurrency=%v", concurrency))
		tk.MustQuery("select @@global.tidb_merge_partition_stats_concurrency").Check(testkit.Rows(concurrency))
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
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p2, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\""))
	tk.MustExec("analyze table t partition p0;")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Note 1105 Analyze use auto adjusted sample rate 1.000000 for table test.t's partition p0, reason to use this rate is \"use min(1, 110000/2) as the sample-rate=1\""))
}

func TestSetFastAnalyzeSystemVariable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@session.tidb_enable_fast_analyze=1")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 the fast analyze feature has already been removed in TiDB v7.5.0, so this will have no effect"))
}

func TestIncrementalAnalyze(t *testing.T) {
	msg := "the incremental analyze feature has already been removed in TiDB v7.5.0, so this will have no effect"
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key(a), index idx(b))")
	tk.MustMatchErrMsg("analyze incremental table t index", msg)
	// Create a partition table.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, primary key(a), index idx(b)) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustMatchErrMsg("analyze incremental table t partition p0 index idx", msg)
}
