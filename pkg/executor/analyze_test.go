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
	"strconv"
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
		cmp, err := upper.Compare(sc.TypeCtx(), lower, collate.GetBinaryCollator())
		if cmp < 0 || err != nil {
			return false, err
		}
		if i == 0 {
			continue
		}
		previousUpper := hg.GetUpper(i - 1)
		cmp, err = lower.Compare(sc.TypeCtx(), previousUpper, collate.GetBinaryCollator())
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
		key1, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx.TimeZone(), nil, types.NewIntDatum(1), types.NewIntDatum(1))
		require.NoError(t, err)
		topn.AppendTopN(key1, 2)
		key2, err := codec.EncodeKey(tk.Session().GetSessionVars().StmtCtx.TimeZone(), nil, types.NewIntDatum(1), types.NewIntDatum(2))
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
	var expected [][]any
	for i := 1; i <= 20; i++ {
		expected = append(expected, []any{
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
