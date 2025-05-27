package ddltest_test

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

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
func TestTiDBScatterRegion(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
	testcases := []struct {
		tableName      string
		sqls           []string
		scatterScope   string
		minRegionCount int
		maxRegionCount int
	}{
		{"t", []string{"CREATE TABLE t (a INT) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3;"}, "table", 1, 6},
		{"t", []string{"CREATE TABLE t (a INT) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3;"}, "global", 1, 6},
		{"t", []string{"CREATE TABLE t (a INT) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3;", "truncate table t"}, "table", 1, 6},
		{"t", []string{"CREATE TABLE t (a INT) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3;", "truncate table t"}, "global", 1, 6},
		{"t", []string{`create table t(bal_dt date) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3 partition by range columns(bal_dt)
		(partition p202201 values less than('2022-02-01'), partition p202202 values less than('2022-02-02'));`,
			"ALTER TABLE t TRUNCATE PARTITION p202201;"}, "table", 1, 12},
		{"t", []string{`create table t(bal_dt date) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3 partition by range columns(bal_dt)
		(partition p202201 values less than('2022-02-01'), partition p202202 values less than('2022-02-02'));`,
			"ALTER TABLE t TRUNCATE PARTITION p202202;"}, "global", 1, 12},
		{"t", []string{`create table t(bal_dt date) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3 partition by range columns(bal_dt)
		(partition p202201 values less than('2022-02-01'));`,
			"ALTER TABLE t ADD PARTITION (PARTITION p202202 values less than('2022-02-02'));"}, "table", 1, 12},
		{"t", []string{`create table t(bal_dt date) SHARD_ROW_ID_BITS = 10 PRE_SPLIT_REGIONS=3 partition by range columns(bal_dt)
		(partition p202201 values less than('2022-02-01'));`,
			"ALTER TABLE t ADD PARTITION (PARTITION p202203 values less than('2022-02-03'));"}, "table", 1, 12},
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
		counts := getTableLeaderDistribute(t, tk, tt.tableName)
		for _, count := range counts {
			require.True(t, count >= tt.minRegionCount && count <= tt.maxRegionCount)
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
		counts = getTableLeaderDistribute(t, tk, tt.tableName)
		for _, count := range counts {
			require.True(t, count >= tt.minRegionCount && count <= tt.maxRegionCount)
		}
	}
}

// getTableLeaderDistribute get leader distribution of the table on all stores.
func getTableLeaderDistribute(t *testing.T, tk *testkit.TestKit, table string) []int {
	regionCount := make(map[int]int)
	re := tk.MustQuery(fmt.Sprintf("show table %s regions", table))
	for _, row := range re.Rows() {
		leaderStoreID, err := strconv.Atoi(row[4].(string))
		require.NoError(t, err)
		regionCount[leaderStoreID]++
	}
	var counts []int
	for _, count := range regionCount {
		counts = append(counts, count)
	}
	return counts
}
