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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/testkit"
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
		scatterScope     string
		totalRegionCount int
	}{
		{"t", []string{"CREATE TABLE t (a INT) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3;"}, "table", 8},
		{"t", []string{"CREATE TABLE t (a INT) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3;"}, "global", 8},
		{"t", []string{"CREATE TABLE t (a INT) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3;", "truncate table t"}, "table", 8},
		{"t", []string{"CREATE TABLE t (a INT) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3;", "truncate table t"}, "global", 8},
		{"t", []string{`create table t(bal_dt date) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3 partition by range columns(bal_dt)
		(partition p202201 values less than('2022-02-01'), partition p202202 values less than('2022-02-02'));`,
			"ALTER TABLE t TRUNCATE PARTITION p202201;"}, "table", 16},
		{"t", []string{`create table t(bal_dt date) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3 partition by range columns(bal_dt)
		(partition p202201 values less than('2022-02-01'), partition p202202 values less than('2022-02-02'));`,
			"ALTER TABLE t TRUNCATE PARTITION p202202;"}, "global", 16},
		{"t", []string{`create table t(bal_dt date) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3 partition by range columns(bal_dt)
		(partition p202201 values less than('2022-02-01'));`,
			"ALTER TABLE t ADD PARTITION (PARTITION p202202 values less than('2022-02-02'));"}, "table", 16},
		{"t", []string{`create table t(bal_dt date) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3 partition by range columns(bal_dt)
		(partition p202201 values less than('2022-02-01'));`,
			"ALTER TABLE t ADD PARTITION (PARTITION p202203 values less than('2022-02-03'));"}, "global", 16},
	}
	for _, tt := range testcases {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec(fmt.Sprintf("set @@session.tidb_scatter_region='%s';", tt.scatterScope))
		tk.MustQuery("select @@session.tidb_scatter_region;").Check(testkit.Rows(tt.scatterScope))
		tk.MustExec(fmt.Sprintf("drop table if exists %s;", tt.tableName))
		for _, sql := range tt.sqls {
			tk.MustExec(sql)
		}
		failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/preSplitAndScatter", func(v string) {
			require.Equal(t, tt.scatterScope, v)
		})
		counts := getTableLeaderDistribute(t, tk, tt.tableName)
		// Validate scatter region by:
		// 1. Get the number of leaders for this table on each store.
		// 2. Check the number of leaders on each store must be less than the
		// total number of regions for this table. This indicates that the
		// table's regions are distributed across multiple stores. If the table
		// is not scattered, all leaders would be concentrated on a single store.
		for _, count := range counts {
			require.True(t, count < tt.totalRegionCount)
		}

		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec(fmt.Sprintf("set @@global.tidb_scatter_region='%s';", tt.scatterScope))
		tk = testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustQuery("select @@session.tidb_scatter_region;").Check(testkit.Rows(tt.scatterScope))
		tk.MustExec(fmt.Sprintf("drop table if exists %s;", tt.tableName))
		for _, sql := range tt.sqls {
			tk.MustExec(sql)
		}
		failpoint.EnableCall("github.com/pingcap/tidb/pkg/ddl/preSplitAndScatter", func(v string) {
			require.Equal(t, tt.scatterScope, v)
		})
		counts = getTableLeaderDistribute(t, tk, tt.tableName)
		for _, count := range counts {
			require.True(t, count < tt.totalRegionCount)
		}
	}
}

// getTableLeaderDistribute query `show table TABLE regions;` to get all leader's
// distribution by LEADER_STORE_ID. Return the number of leaders on each store.
// +-----------+-----------------------------+-----------------------------+-----------+-----------------+---------------------+------------+---------------+------------+----------------------+------------------+------------------------+------------------+
// | REGION_ID | START_KEY                   | END_KEY                     | LEADER_ID | LEADER_STORE_ID | PEERS               | SCATTERING | WRITTEN_BYTES | READ_BYTES | APPROXIMATE_SIZE(MB) | APPROXIMATE_KEYS | SCHEDULING_CONSTRAINTS | SCHEDULING_STATE |
// +-----------+-----------------------------+-----------------------------+-----------+-----------------+---------------------+------------+---------------+------------+----------------------+------------------+------------------------+------------------+
// |     44861 | t_615_                      | t_615_r_2305843009213693952 |     44864 |               2 | 44862, 44863, 44864 |          0 |            39 |          0 |                    1 |                0 |                        |                  |
// |     44865 | t_615_r_2305843009213693952 | t_615_r_4611686018427387904 |     44867 |               7 | 44866, 44867, 44868 |          0 |            39 |          0 |                    1 |                0 |                        |                  |
// |     44869 | t_615_r_4611686018427387904 | t_615_r_6917529027641081856 |     44870 |               1 | 44870, 44871, 44872 |          0 |            39 |          0 |                    1 |                0 |                        |                  |
// |     44897 | t_615_r_6917529027641081856 | t_616_                      |     44900 |               2 | 44898, 44899, 44900 |          0 |             0 |          0 |                    1 |                0 |                        |                  |
// |     44901 | t_616_                      | t_616_r_2305843009213693952 |     44902 |               1 | 44902, 44903, 44904 |          0 |            39 |          0 |                    1 |                0 |                        |                  |
// |     44905 | t_616_r_2305843009213693952 | t_616_r_4611686018427387904 |     44907 |               7 | 44906, 44907, 44908 |          0 |            27 |          0 |                    1 |                0 |                        |                  |
// |     44909 | t_616_r_4611686018427387904 | t_616_r_6917529027641081856 |     44912 |               2 | 44910, 44911, 44912 |          0 |             0 |          0 |                    1 |                0 |                        |                  |
// |     44913 | t_616_r_6917529027641081856 | t_617_                      |     44914 |               1 | 44914, 44915, 44916 |          0 |            39 |          0 |                    1 |                0 |                        |                  |
// |     44917 | t_617_                      | t_617_r_2305843009213693952 |     44919 |               7 | 44918, 44919, 44920 |          0 |            39 |          0 |                    1 |                0 |                        |                  |
// |     44921 | t_617_r_2305843009213693952 | t_617_r_4611686018427387904 |     44924 |               2 | 44922, 44923, 44924 |          0 |             0 |          0 |                    1 |                0 |                        |                  |
// |     44925 | t_617_r_4611686018427387904 | t_617_r_6917529027641081856 |     44926 |               1 | 44926, 44927, 44928 |          0 |             0 |          0 |                    1 |                0 |                        |                  |
// |     41001 | t_617_r_6917529027641081856 | t_281474976710654_          |     41003 |               7 | 41002, 41003, 41004 |          0 |           657 |       4177 |                    1 |                0 |                        |                  |
// +-----------+-----------------------------+-----------------------------+-----------+-----------------+---------------------+------------+---------------+------------+----------------------+------------------+------------------------+------------------+
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
