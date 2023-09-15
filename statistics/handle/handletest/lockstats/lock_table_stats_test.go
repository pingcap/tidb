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
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestLockAndUnlockTableStats(t *testing.T) {
	_, tk, tbl := setupTestEnvironmentWithTableT(t)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	tblStats := handle.GetTableStats(tbl)
	for _, col := range tblStats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
	tk.MustExec("lock stats t")

	rows := tk.MustQuery(selectTableLockSQL).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 1)

	tk.MustExec("insert into t(a, b) values(1,'a')")
	tk.MustExec("insert into t(a, b) values(2,'b')")

	tk.MustExec("analyze table test.t")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 skip analyze locked table: test.t",
	))
	tblStats1 := handle.GetTableStats(tbl)
	require.Equal(t, tblStats, tblStats1)

	lockedTables, err := handle.GetTableLockedAndClearForTest()
	require.Nil(t, err)
	require.Equal(t, 1, len(lockedTables))

	tk.MustExec("unlock stats t")
	rows = tk.MustQuery(selectTableLockSQL).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 0)

	tk.MustExec("analyze table test.t")
	tblStats2 := handle.GetTableStats(tbl)
	require.Equal(t, int64(2), tblStats2.RealtimeCount)
}

func TestLockAndUnlockPartitionedTableStats(t *testing.T) {
	_, tk, tbl := setupTestEnvironmentWithPartitionedTableT(t)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	tblStats := handle.GetTableStats(tbl)
	for _, col := range tblStats.Columns {
		require.True(t, col.IsStatsInitialized())
	}

	tk.MustExec("lock stats t")
	rows := tk.MustQuery(selectTableLockSQL).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 3)

	rows = tk.MustQuery("show stats_locked").Rows()
	require.Len(t, rows, 3)

	tk.MustExec("analyze table test.t")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 skip analyze locked tables: test.t partition (p0), test.t partition (p1)",
	))

	tk.MustExec("unlock stats t")
	rows = tk.MustQuery(selectTableLockSQL).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 0)

	rows = tk.MustQuery("show stats_locked").Rows()
	require.Len(t, rows, 0)
}

func TestLockTableAndUnlockTableStatsRepeatedly(t *testing.T) {
	_, tk, tbl := setupTestEnvironmentWithTableT(t)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	tblStats := handle.GetTableStats(tbl)
	for _, col := range tblStats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
	tk.MustExec("lock stats t")

	rows := tk.MustQuery(selectTableLockSQL).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 1)

	tk.MustExec("insert into t(a, b) values(1,'a')")
	tk.MustExec("insert into t(a, b) values(2,'b')")

	tk.MustExec("analyze table test.t")
	tblStats1 := handle.GetTableStats(tbl)
	require.Equal(t, tblStats, tblStats1)

	// Lock the table again and check the warning.
	lockedTables1, err := handle.GetTableLockedAndClearForTest()
	require.Nil(t, err)
	tk.MustExec("lock stats t")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 skip locking locked table: test.t",
	))

	lockedTables2, err := handle.GetTableLockedAndClearForTest()
	require.Nil(t, err)
	require.Equal(t, lockedTables1, lockedTables2)

	// Unlock the table.
	tk.MustExec("unlock stats t")
	rows = tk.MustQuery(selectTableLockSQL).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 0)

	tk.MustExec("analyze table test.t")
	tblStats2 := handle.GetTableStats(tbl)
	require.Equal(t, int64(2), tblStats2.RealtimeCount)

	// Unlock the table again and check the warning.
	tk.MustExec("unlock stats t")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 skip unlocking unlocked table: test.t",
	))
}

func TestLockAndUnlockTablesStats(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a int, b varchar(10), index idx_b (b))")
	tk.MustExec("create table t2(a int, b varchar(10), index idx_b (b))")
	tk.MustExec("analyze table test.t1, test.t2")
	tbl1, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	require.Nil(t, err)
	tbl2, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t2"))
	require.Nil(t, err)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	tbl1Stats := handle.GetTableStats(tbl1.Meta())
	for _, col := range tbl1Stats.Columns {
		require.Eventually(t, func() bool {
			return col.IsStatsInitialized()
		}, 1*time.Second, 100*time.Millisecond)
	}
	tbl2Stats := handle.GetTableStats(tbl2.Meta())
	for _, col := range tbl2Stats.Columns {
		require.Eventually(t, func() bool {
			return col.IsStatsInitialized()
		}, 1*time.Second, 100*time.Millisecond)
	}

	tk.MustExec("lock stats t1, t2")
	rows := tk.MustQuery(selectTableLockSQL).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 2)

	tk.MustExec("insert into t1(a, b) values(1,'a')")
	tk.MustExec("insert into t1(a, b) values(2,'b')")

	tk.MustExec("insert into t2(a, b) values(1,'a')")
	tk.MustExec("insert into t2(a, b) values(2,'b')")

	tk.MustExec("analyze table test.t1, test.t2")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 skip analyze locked tables: test.t1, test.t2",
	))
	tbl1Stats1 := handle.GetTableStats(tbl1.Meta())
	require.Equal(t, tbl1Stats, tbl1Stats1)
	tbl2Stats1 := handle.GetTableStats(tbl2.Meta())
	require.Equal(t, tbl2Stats, tbl2Stats1)

	lockedTables, err := handle.GetTableLockedAndClearForTest()
	require.Nil(t, err)
	require.Equal(t, 2, len(lockedTables))

	tk.MustExec("unlock stats test.t1, test.t2")
	rows = tk.MustQuery(selectTableLockSQL).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 0)

	tk.MustExec("analyze table test.t1, test.t2")
	tbl1Stats2 := handle.GetTableStats(tbl1.Meta())
	require.Equal(t, int64(2), tbl1Stats2.RealtimeCount)
	tbl2Stats2 := handle.GetTableStats(tbl2.Meta())
	require.Equal(t, int64(2), tbl2Stats2.RealtimeCount)
}

func TestLockAndUnlockTablePrivilege(t *testing.T) {
	store, tk, tbl := setupTestEnvironmentWithTableT(t)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	tblStats := handle.GetTableStats(tbl)
	for _, col := range tblStats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
	// With privilege.
	tk.MustExec("lock stats t")
	rows := tk.MustQuery(selectTableLockSQL).Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 1)
	tk.MustExec("unlock stats t")
	rows = tk.MustQuery(selectTableLockSQL).Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 0)

	// Add a user.
	tk.MustExec("drop user if exists myuser@localhost")
	tk.MustExec("create user myuser@localhost")
	// Only grant delete privilege.
	tk.MustExec("grant delete on test.* to myuser@localhost")

	// Without privilege.
	tk1 := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil, nil))
	tk1.SetSession(se)

	tk1.MustExec("use test")
	_, err = tk1.Exec("lock stats t")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]INSERT command denied to user 'myuser'@'localhost' for table 't'", err.Error())
	_, err = tk1.Exec("unlock stats t")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]INSERT command denied to user 'myuser'@'localhost' for table 't'", err.Error())

	// Grant INSERT privilege.
	tk.MustExec("grant insert on test.* to myuser@localhost")
	tk.MustExec("flush privileges")

	// Try again.
	_, err = tk1.Exec("lock stats t")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]SELECT command denied to user 'myuser'@'localhost' for table 't'", err.Error())
	_, err = tk1.Exec("unlock stats t")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]SELECT command denied to user 'myuser'@'localhost' for table 't'", err.Error())

	// Grant SELECT privilege.
	tk.MustExec("grant select on test.* to myuser@localhost")
	tk.MustExec("flush privileges")

	// Try again
	tk1.MustExec("lock stats t")
	tk1.MustExec("unlock stats t")
}

func TestShowStatsLockedTablePrivilege(t *testing.T) {
	store, tk, tbl := setupTestEnvironmentWithTableT(t)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	tblStats := handle.GetTableStats(tbl)
	for _, col := range tblStats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
	// With privilege.
	tk.MustExec("lock stats t")
	rows := tk.MustQuery("show stats_locked").Rows()
	require.Len(t, rows, 1)

	// Add a user.
	tk.MustExec("drop user if exists myuser@localhost")
	tk.MustExec("create user myuser@localhost")
	// Only grant INSERT privilege.
	tk.MustExec("grant insert on mysql.* to myuser@localhost")

	// Without privilege.
	tk1 := testkit.NewTestKit(t, store)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "myuser", Hostname: "localhost"}, nil, nil, nil))
	tk1.SetSession(se)
	_, err = tk1.Exec("show stats_locked")
	require.Error(t, err)
	require.Equal(t, "[planner:1142]SHOW command denied to user 'myuser'@'localhost' for table 'stats_table_locked'", err.Error())

	// Grant SELECT privilege.
	tk.MustExec("grant select on mysql.* to myuser@localhost")
	tk.MustExec("flush privileges")

	// Try again
	rows = tk.MustQuery("show stats_locked").Rows()
	require.Len(t, rows, 1)
}

func setupTestEnvironmentWithTableT(t *testing.T) (kv.Storage, *testkit.TestKit, *model.TableInfo) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), index idx_b (b))")
	tk.MustExec("analyze table test.t")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.Nil(t, err)

	return store, tk, tbl.Meta()
}
