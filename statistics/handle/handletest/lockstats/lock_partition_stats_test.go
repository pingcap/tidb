// Copyright 2023 PingCAP, Inc.
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

package lockstats

import (
	"strconv"
	"testing"

	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

const selectTableLockSQL = "select count(*) from mysql.stats_table_locked"

func TestLockAndUnlockPartitionStats(t *testing.T) {
	_, tk, tbl := setupTestEnvironmentWithPartitionedTableT(t)
	handle := domain.GetDomain(tk.Session()).StatsHandle()
	// Get partition stats.
	p0Id := tbl.GetPartitionInfo().Definitions[0].ID
	partitionStats := handle.GetPartitionStats(tbl, p0Id)
	for _, col := range partitionStats.Columns {
		require.True(t, col.IsStatsInitialized())
	}

	tk.MustExec("lock stats t partition p0")
	rows := tk.MustQuery(selectTableLockSQL).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, 1, num)

	rows = tk.MustQuery("show stats_locked").Rows()
	require.Len(t, rows, 1)

	tk.MustExec("insert into t(a, b) values(1,'a')")
	tk.MustExec("insert into t(a, b) values(2,'b')")

	tk.MustExec("analyze table test.t")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 skip analyze locked table: test.t partition (p0)",
	))
	partitionStats1 := handle.GetPartitionStats(tbl, p0Id)
	require.Equal(t, partitionStats, partitionStats1)
	require.Equal(t, int64(0), partitionStats1.RealtimeCount)

	tk.MustExec("unlock stats t partition p0")
	rows = tk.MustQuery(selectTableLockSQL).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, 0, num)

	tk.MustExec("analyze table test.t partition p0")
	tblStats2 := handle.GetTableStats(tbl)
	require.Equal(t, int64(2), tblStats2.RealtimeCount)

	rows = tk.MustQuery("show stats_locked").Rows()
	require.Len(t, rows, 0)
}

func TestLockAndUnlockPartitionsStats(t *testing.T) {
	_, tk, tbl := setupTestEnvironmentWithPartitionedTableT(t)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	// Get partition stats.
	p0Id := tbl.GetPartitionInfo().Definitions[0].ID
	partition0Stats := handle.GetPartitionStats(tbl, p0Id)
	for _, col := range partition0Stats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
	p1Id := tbl.GetPartitionInfo().Definitions[1].ID
	partition1Stats := handle.GetPartitionStats(tbl, p1Id)
	for _, col := range partition1Stats.Columns {
		require.True(t, col.IsStatsInitialized())
	}

	tk.MustExec("lock stats t partition p0, p1")
	rows := tk.MustQuery(selectTableLockSQL).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, 2, num)

	tk.MustExec("insert into t(a, b) values(1,'a')")
	tk.MustExec("insert into t(a, b) values(2,'b')")
	tk.MustExec("insert into t(a, b) values(11,'a')")
	tk.MustExec("insert into t(a, b) values(12,'b')")

	tk.MustExec("analyze table test.t partition p0, p1")
	partition0Stats1 := handle.GetPartitionStats(tbl, p0Id)
	require.Equal(t, partition0Stats, partition0Stats1)
	require.Equal(t, int64(0), partition0Stats1.RealtimeCount)
	partition1Stats1 := handle.GetPartitionStats(tbl, p1Id)
	require.Equal(t, partition1Stats, partition1Stats1)
	require.Equal(t, int64(0), partition1Stats1.RealtimeCount)

	rows = tk.MustQuery("show stats_locked").Rows()
	require.Len(t, rows, 2)

	tk.MustExec("unlock stats t partition p0, p1")
	rows = tk.MustQuery(selectTableLockSQL).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, 0, num)

	tk.MustExec("analyze table test.t partition p0, p1")
	partition0Stats2 := handle.GetPartitionStats(tbl, p0Id)
	require.Equal(t, int64(2), partition0Stats2.RealtimeCount)
	partition1Stats2 := handle.GetPartitionStats(tbl, p1Id)
	require.Equal(t, int64(2), partition1Stats2.RealtimeCount)

	tblStats := handle.GetTableStats(tbl)
	require.Equal(t, int64(4), tblStats.RealtimeCount)

	rows = tk.MustQuery("show stats_locked").Rows()
	require.Len(t, rows, 0)
}

func TestLockAndUnlockPartitionStatsRepeatedly(t *testing.T) {
	_, tk, tbl := setupTestEnvironmentWithPartitionedTableT(t)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	// Get partition stats.
	p0Id := tbl.GetPartitionInfo().Definitions[0].ID
	partition0Stats := handle.GetPartitionStats(tbl, p0Id)
	for _, col := range partition0Stats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
	p1Id := tbl.GetPartitionInfo().Definitions[1].ID
	partition1Stats := handle.GetPartitionStats(tbl, p1Id)
	for _, col := range partition1Stats.Columns {
		require.True(t, col.IsStatsInitialized())
	}

	tk.MustExec("lock stats t partition p0")
	rows := tk.MustQuery(selectTableLockSQL).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, 1, num)

	// Lock the partition again and check the warning.
	tk.MustExec("lock stats t partition p0")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 skip locking locked partition of table test.t: p0",
	))

	// Unlock the partition.
	tk.MustExec("unlock stats t partition p0")
	rows = tk.MustQuery(selectTableLockSQL).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, 0, num)

	// Unlock the partition again and check the warning.
	tk.MustExec("unlock stats t partition p0")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 skip unlocking unlocked partition of table test.t: p0",
	))
}

// TestSkipLockPartition tests that skip locking partition stats
// when the whole table is already locked.
func TestSkipLockPartition(t *testing.T) {
	_, tk, tbl := setupTestEnvironmentWithPartitionedTableT(t)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	// Get partition stats.
	p0Id := tbl.GetPartitionInfo().Definitions[0].ID
	partition0Stats := handle.GetPartitionStats(tbl, p0Id)
	for _, col := range partition0Stats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
	p1Id := tbl.GetPartitionInfo().Definitions[1].ID
	partition1Stats := handle.GetPartitionStats(tbl, p1Id)
	for _, col := range partition1Stats.Columns {
		require.True(t, col.IsStatsInitialized())
	}

	tk.MustExec("lock stats t")
	rows := tk.MustQuery(selectTableLockSQL).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, 3, num)

	// Lock the partition and check the warning.
	tk.MustExec("lock stats t partition p0")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 skip locking partitions of locked table: test.t",
	))
}

func TestUnlockOnePartitionOfLockedTableWouldFail(t *testing.T) {
	_, tk, tbl := setupTestEnvironmentWithPartitionedTableT(t)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	// Get partition stats.
	p0Id := tbl.GetPartitionInfo().Definitions[0].ID
	partition0Stats := handle.GetPartitionStats(tbl, p0Id)
	for _, col := range partition0Stats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
	p1Id := tbl.GetPartitionInfo().Definitions[1].ID
	partition1Stats := handle.GetPartitionStats(tbl, p1Id)
	for _, col := range partition1Stats.Columns {
		require.True(t, col.IsStatsInitialized())
	}

	tk.MustExec("lock stats t")
	rows := tk.MustQuery(selectTableLockSQL).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, 3, num)

	// Unlock the partition and check the warning.
	tk.MustExec("unlock stats t partition p0")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 skip unlocking partitions of locked table: test.t",
	))

	// No partition is unlocked.
	rows = tk.MustQuery(selectTableLockSQL).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, 3, num)
}

func TestUnlockTheWholeTableWouldUnlockLockedPartitionsAndGenerateWarning(t *testing.T) {
	_, tk, tbl := setupTestEnvironmentWithPartitionedTableT(t)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	// Get partition stats.
	p0Id := tbl.GetPartitionInfo().Definitions[0].ID
	partition0Stats := handle.GetPartitionStats(tbl, p0Id)
	for _, col := range partition0Stats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
	p1Id := tbl.GetPartitionInfo().Definitions[1].ID
	partition1Stats := handle.GetPartitionStats(tbl, p1Id)
	for _, col := range partition1Stats.Columns {
		require.True(t, col.IsStatsInitialized())
	}

	tk.MustExec("lock stats t partition p0")
	rows := tk.MustQuery(selectTableLockSQL).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, 1, num)

	// Unlock the whole table and check the warning.
	tk.MustExec("unlock stats t")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 skip unlocking unlocked table: test.t",
	))

	// Should unlock the locked partition.
	rows = tk.MustQuery(selectTableLockSQL).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, 0, num)
}

func TestSkipLockALotOfPartitions(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), index idx_b (b)) partition by range(a) " +
		"(partition p0 values less than (10), partition p1 values less than (20), " +
		"partition a values less than (30), " +
		"partition b values less than (40), " +
		"partition g values less than (90), " +
		"partition h values less than (100))")

	tk.MustExec("lock stats t partition p0, p1, a, b, g, h")

	// Skip locking a lot of partitions.
	tk.MustExec("lock stats t partition p0, p1, a, b, g, h")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 skip locking locked partitions of table test.t: a, b, g, h, p0, p1",
	))
}

func setupTestEnvironmentWithPartitionedTableT(t *testing.T) (kv.Storage, *testkit.TestKit, *model.TableInfo) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), index idx_b (b)) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	tk.MustExec("analyze table test.t")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.Nil(t, err)

	return store, tk, tbl.Meta()
}
