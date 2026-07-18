// Copyright 2022 PingCAP, Inc.
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

package sessiontest

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestPagingActRowsAndProcessKeys(t *testing.T) {
	// Close copr-cache
	defer config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.CoprCache.CapacityMB = 0
	})

	store := realtikvtest.CreateMockStoreAndSetup(t)
	session := testkit.NewTestKit(t, store)
	session.MustExec("use test;")
	session.MustExec("drop table if exists t;")
	session.MustExec(`set @@tidb_wait_split_region_finish=1`)
	session.MustExec("create table t(a int,b int,c int,index idx(a,b), primary key(a));")
	// prepare data, insert 10w record
	// [0, 999999]
	for i := range 100 {
		sql := "insert into t value"
		for j := range 1000 {
			if j != 0 {
				sql += ","
			}
			sql += "(" + strconv.Itoa(i*1000+j) + "," + strconv.Itoa(i*1000+j) + "," + strconv.Itoa(i*1000+j) + ")"
		}
		session.MustExec(sql)
	}

	testcase := []struct {
		regionNumLowerBound int32
		regionNumUpperBound int32
	}{
		{10, 100},    // [10, 99]
		{100, 500},   // [100,499]
		{500, 1000},  // [500,999]
		{1000, 1001}, // 1000
	}

	openOrClosePaging := []string{
		"set tidb_enable_paging = on;",
		"set tidb_enable_paging = off;",
	}

	sqls := []string{
		"desc analyze select a,b from t;",                         // TableScan
		"desc analyze select /*+ use_index(t,idx) */ a,b from t;", // IndexScan
		"desc analyze select /*+ use_index(t,idx) */ c from t;",   // IndexLookUp
	}

	checkScanOperator := func(strs []any) {
		require.Equal(t, strs[2].(string), "100000")
		if *realtikvtest.WithRealTiKV { // Unistore don't collect process_keys now
			require.True(t, strings.Contains(strs[5].(string), "total_process_keys: 100000"), strs[5])
		}
	}

	checkResult := func(result [][]any) {
		for _, strs := range result {
			if strings.Contains(strs[0].(string), "Scan") {
				checkScanOperator(strs)
			}
		}
	}

	for _, tc := range testcase {
		regionNum := rand.Int31n(tc.regionNumUpperBound-tc.regionNumLowerBound) + tc.regionNumLowerBound
		_ = session.MustQuery(fmt.Sprintf("split table t between (0) and (1000000) regions %v;", regionNum))
		_ = session.MustQuery(fmt.Sprintf("split table t index idx between (0) and (1000000) regions %v;", regionNum))
		for _, sql := range sqls {
			for _, pagingSQL := range openOrClosePaging {
				session.MustExec(pagingSQL)
				rows := session.MustQuery(sql)
				checkResult(rows.Rows())
			}
		}
	}
}

func TestIndexReaderWithPaging(t *testing.T) {
	defer config.RestoreFunc()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TiKVClient.CoprCache.CapacityMB = 0
	})
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (id int key, b int, c int, index idx (b), index idx2(c))")
	tk.MustExec("begin")
	for i := 0; i < 1024; i += 4 {
		tk.MustExec(fmt.Sprintf("insert into t (id) values (%d), (%d), (%d), (%d)", i, i+1, i+2, i+3))
	}
	tk.MustExec(`update t set b = id, c = id`)
	tk.MustExec("commit")
	// Index lookup query with paging enabled.
	tk.MustExec(`set @@tidb_enable_paging=1`)
	tk.MustExec(`set @@tidb_min_paging_size=128`)
	tk.MustExec("set @@tidb_max_chunk_size=1024;")
	tk.MustQuery("select count(c) from t use index(idx);").Check(testkit.Rows("1024")) // full scan to resolve uncommitted lock.
	tk.MustQuery("select count(b) from t use index(idx2);").Check(testkit.Rows("1024"))
	tk.MustQuery("select count(id) from t ignore index(idx, idx2)").Check(testkit.Rows("1024"))
	// Test Index Lookup Reader query.
	rows := tk.MustQuery("explain analyze select * from t use index(idx) where b>0 and b < 1024;").Rows()
	require.Len(t, rows, 3)
	explain := fmt.Sprintf("%v", rows[1])
	require.Regexp(t, ".*IndexRangeScan.*rpc_info.*Cop:{num_rpc:1, total_time:.*", explain)
	// Test Index Merge Reader query.
	rows = tk.MustQuery("explain analyze select /*+ USE_INDEX_MERGE(t, idx, idx2) */ * from t where b > 0 or c > 0;").Rows()
	require.Len(t, rows, 4)
	require.Regexp(t, "IndexMerge.*", fmt.Sprintf("%v", rows[0]))
	require.Regexp(t, ".*IndexRangeScan.*rpc_info.*Cop:{num_rpc:1, total_time:.*", fmt.Sprintf("%v", rows[1]))
	require.Regexp(t, ".*IndexRangeScan.*rpc_info.*Cop:{num_rpc:1, total_time:.*", fmt.Sprintf("%v", rows[2]))
}
