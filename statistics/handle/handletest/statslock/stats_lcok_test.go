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

package statslock

import (
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestStatsLockAndUnlockTable(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), index idx_b (b))")
	tk.MustExec("analyze table test.t")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.Nil(t, err)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	tblStats := handle.GetTableStats(tbl.Meta())
	for _, col := range tblStats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
	tk.MustExec("lock stats t")

	rows := tk.MustQuery("select count(*) from mysql.stats_table_locked").Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 1)

	tk.MustExec("insert into t(a, b) values(1,'a')")
	tk.MustExec("insert into t(a, b) values(2,'b')")

	tk.MustExec("analyze table test.t")
	tblStats1 := handle.GetTableStats(tbl.Meta())
	require.Equal(t, tblStats, tblStats1)

	tableLocked1 := handle.GetTableLockedAndClearForTest()
	err = handle.LoadLockedTables()
	require.Nil(t, err)
	tableLocked2 := handle.GetTableLockedAndClearForTest()
	require.Equal(t, tableLocked1, tableLocked2)

	tk.MustExec("unlock stats t")
	rows = tk.MustQuery("select count(*) from mysql.stats_table_locked").Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 0)

	tk.MustExec("analyze table test.t")
	tblStats2 := handle.GetTableStats(tbl.Meta())
	require.Equal(t, int64(2), tblStats2.RealtimeCount)
}

func TestStatsLockTableRepeatedly(t *testing.T) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version = 1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10), index idx_b (b))")
	tk.MustExec("analyze table test.t")
	tbl, err := dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.Nil(t, err)

	handle := domain.GetDomain(tk.Session()).StatsHandle()
	tblStats := handle.GetTableStats(tbl.Meta())
	for _, col := range tblStats.Columns {
		require.True(t, col.IsStatsInitialized())
	}
	tk.MustExec("lock stats t")

	rows := tk.MustQuery("select count(*) from mysql.stats_table_locked").Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 1)

	tk.MustExec("insert into t(a, b) values(1,'a')")
	tk.MustExec("insert into t(a, b) values(2,'b')")

	tk.MustExec("analyze table test.t")
	tblStats1 := handle.GetTableStats(tbl.Meta())
	require.Equal(t, tblStats, tblStats1)

	// Lock the table again and check the warning.
	tableLocked1 := handle.GetTableLockedAndClearForTest()
	tk.MustExec("lock stats t")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows(
		"Warning 1105 skip locking locked table: test.t",
	))

	err = handle.LoadLockedTables()
	require.Nil(t, err)
	tableLocked2 := handle.GetTableLockedAndClearForTest()
	require.Equal(t, tableLocked1, tableLocked2)
}

func TestStatsLockAndUnlockTables(t *testing.T) {
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

	rows := tk.MustQuery("select count(*) from mysql.stats_table_locked").Rows()
	num, _ := strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 2)

	tk.MustExec("insert into t1(a, b) values(1,'a')")
	tk.MustExec("insert into t1(a, b) values(2,'b')")

	tk.MustExec("insert into t2(a, b) values(1,'a')")
	tk.MustExec("insert into t2(a, b) values(2,'b')")

	tk.MustExec("analyze table test.t1, test.t2")
	tbl1Stats1 := handle.GetTableStats(tbl1.Meta())
	require.Equal(t, tbl1Stats, tbl1Stats1)
	tbl2Stats1 := handle.GetTableStats(tbl2.Meta())
	require.Equal(t, tbl2Stats, tbl2Stats1)

	tableLocked1 := handle.GetTableLockedAndClearForTest()
	err = handle.LoadLockedTables()
	require.Nil(t, err)
	tableLocked2 := handle.GetTableLockedAndClearForTest()
	require.Equal(t, tableLocked1, tableLocked2)

	tk.MustExec("unlock stats test.t1, test.t2")
	rows = tk.MustQuery("select count(*) from mysql.stats_table_locked").Rows()
	num, _ = strconv.Atoi(rows[0][0].(string))
	require.Equal(t, num, 0)

	tk.MustExec("analyze table test.t1, test.t2")
	tbl1Stats2 := handle.GetTableStats(tbl1.Meta())
	require.Equal(t, int64(2), tbl1Stats2.RealtimeCount)
	tbl2Stats2 := handle.GetTableStats(tbl2.Meta())
	require.Equal(t, int64(2), tbl2Stats2.RealtimeCount)
}
