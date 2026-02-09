// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddltest_test

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func init() {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Path = "127.0.0.1:2379"
	})
}

// TestTiDBScatterRegion tests all combinations of tidb_scatter_region=table/global
// and create table\truncate table\truncate partition\add partition to verify
// the distribution of the table on each store after adding pd scatter region.
// The distribution of leaders is related to PD scheduling. In some cases,
// it is unbalanced. Testcase compares the number of regions by range.
// Since realtikv test deployment has 3 TiKV nodes, we will validate scatter in
// table/global level in integration test.
func TestTiDBScatterRegion(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	testcases := []struct {
		tableName        string
		sqls             []string
		totalRegionCount int
	}{
		{"t", []string{"CREATE TABLE t (a INT) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3;"}, 8},
		{"t", []string{"CREATE TABLE t (a INT) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3;", "truncate table t"}, 8},
		{"t", []string{`create table t(bal_dt date) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3 partition by range columns(bal_dt)
		(partition p202201 values less than('2022-02-01'), partition p202202 values less than('2022-02-02'));`,
			"ALTER TABLE t TRUNCATE PARTITION p202201;"}, 16},
		{"t", []string{`create table t(bal_dt date) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3 partition by range columns(bal_dt)
		(partition p202201 values less than('2022-02-01'));`,
			"ALTER TABLE t ADD PARTITION (PARTITION p202202 values less than('2022-02-02'));"}, 16},
	}
	for _, scatterScope := range []string{"table", "global"} {
		for _, tt := range testcases {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec(fmt.Sprintf("set @@session.tidb_scatter_region='%s';", scatterScope))
			tk.MustQuery("select @@session.tidb_scatter_region;").Check(testkit.Rows(scatterScope))
			tk.MustExec(fmt.Sprintf("drop table if exists %s;", tt.tableName))
			checkScatterScope := false
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/preSplitAndScatter", func(v string) {
				require.Equal(t, scatterScope, v)
				checkScatterScope = true
			})
			for _, sql := range tt.sqls {
				tk.MustExec(sql)
			}
			require.Equal(t, true, checkScatterScope)
			counts := getTableLeaderDistribute(t, tk, tt.tableName)
			// Validate scatter region by:
			// 1. Get the number of leaders for this table on each store.
			// 2. Check the number of leaders on each store must be less than the
			// total number of regions for this table. This indicates that the
			// table's regions are distributed across multiple stores. If the table
			// is not scattered, all leaders would be concentrated on a single store.
			for _, count := range counts {
				require.Less(t, count, tt.totalRegionCount)
			}

			tk2 := testkit.NewTestKit(t, store)
			tk2.MustExec(fmt.Sprintf("set @@global.tidb_scatter_region='%s';", scatterScope))
			tk = testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustQuery("select @@session.tidb_scatter_region;").Check(testkit.Rows(scatterScope))
			tk.MustExec(fmt.Sprintf("drop table if exists %s;", tt.tableName))
			checkScatterScope = false
			testfailpoint.EnableCall(t, "github.com/pingcap/tidb/pkg/ddl/preSplitAndScatter", func(v string) {
				require.Equal(t, scatterScope, v)
				checkScatterScope = true
			})
			for _, sql := range tt.sqls {
				tk.MustExec(sql)
			}
			require.Equal(t, true, checkScatterScope)
			counts = getTableLeaderDistribute(t, tk, tt.tableName)
			for _, count := range counts {
				require.Less(t, count, tt.totalRegionCount)
			}
		}
	}
}

// getTableLeaderDistribute query `show table TABLE regions;` to get all leader's
// distribution by LEADER_STORE_ID. Return the number of leaders on each store.
func getTableLeaderDistribute(t *testing.T, tk *testkit.TestKit, table string) []int {
	regionCount := make(map[int]int)
	re := tk.MustQuery(fmt.Sprintf("show table %s regions", table))
	for _, row := range re.Rows() {
		leaderStoreID, err := strconv.Atoi(row[4].(string))
		require.NoError(t, err)
		regionCount[leaderStoreID]++
	}
	counts := []int{}
	for _, count := range regionCount {
		counts = append(counts, count)
	}
	return counts
}

func TestUpdateSelfVersionFail(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set global tidb_enable_metadata_lock=0")
	defer func() {
		tk.MustExec("set global tidb_enable_metadata_lock=1")
	}()

	tk.MustExec("use test")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/ddl/util/PutKVToEtcdError", `3*return(true)`)

	tk.MustExec("create table t (a int)")
	tk.MustExec("drop table t")
}
