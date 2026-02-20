// Copyright 2020 PingCAP, Inc.
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

package sortexec_test

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TODO remove spill as it should be tested in sort_spill_test.go
// TODO add some new fail points, as some fail point may be aborted after the refine
func TestSortInDisk(t *testing.T) {
	for i, v := range []bool{false, true} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			testSortInDisk(t, v)
		})
	}
}

// TODO remove failpoint
func testSortInDisk(t *testing.T, removeDir bool) {
	restore := config.RestoreFunc()
	defer restore()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/testSortedRowContainerSpill", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/testSortedRowContainerSpill"))
	}()
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
	tk.MustExec("SET GLOBAL tidb_mem_oom_action='LOG'")
	tk.MustExec("use test")

	sm := &testkit.MockSessionManager{
		PS: make([]*sessmgr.ProcessInfo, 0),
	}
	tk.Session().SetSessionManager(sm)
	dom.ExpensiveQueryHandle().SetSessionManager(sm)

	if removeDir {
		require.Nil(t, os.RemoveAll(config.GetGlobalConfig().TempStoragePath))
		defer func() {
			_, err := os.Stat(config.GetGlobalConfig().TempStoragePath)
			if err != nil {
				require.True(t, os.IsExist(err))
			}
		}()
	}

	tk.MustExec("set @@tidb_mem_quota_query=1;")
	tk.MustExec("set @@tidb_max_chunk_size=32;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 int, c3 int)")
	var buf bytes.Buffer
	buf.WriteString("insert into t values ")
	for i := range 5 {
		for j := i; j < 1024; j += 5 {
			if j > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(fmt.Sprintf("(%v, %v, %v)", j, j, j))
		}
	}
	tk.MustExec(buf.String())
	result := tk.MustQuery("select * from t order by c1")
	for i := range 1024 {
		require.Equal(t, fmt.Sprint(i), result.Rows()[i][0].(string))
		require.Equal(t, fmt.Sprint(i), result.Rows()[i][1].(string))
		require.Equal(t, fmt.Sprint(i), result.Rows()[i][2].(string))
	}
	require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed())
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), int64(0))
	require.Equal(t, int64(0), tk.Session().GetSessionVars().StmtCtx.DiskTracker.BytesConsumed())
	require.Greater(t, tk.Session().GetSessionVars().StmtCtx.DiskTracker.MaxConsumed(), int64(0))
}

func TestIssue16696(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TempStoragePath = t.TempDir()
		conf.Performance.EnableStatsCacheMemQuota = true
	})
	alarmRatio := vardef.MemoryUsageAlarmRatio.Load()
	vardef.MemoryUsageAlarmRatio.Store(0.0)
	defer vardef.MemoryUsageAlarmRatio.Store(alarmRatio)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/testSortedRowContainerSpill", "return(true)"))
	defer require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/testSortedRowContainerSpill"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/testRowContainerSpill", "return(true)"))
	defer require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/testRowContainerSpill"))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
	tk.MustExec("SET GLOBAL tidb_mem_oom_action='LOG'")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (`a` int(11) DEFAULT NULL,`b` int(11) DEFAULT NULL)")
	tk.MustExec("insert into t values (1, 1)")
	for range 6 {
		tk.MustExec("insert into t select * from t")
	}
	tk.MustExec("set tidb_mem_quota_query = 1;")
	rows := tk.MustQuery("explain analyze  select t1.a, t1.a +1 from t t1 join t t2 join t t3 order by t1.a").Rows()
	for _, row := range rows {
		length := len(row)
		line := fmt.Sprintf("%v", row)
		disk := fmt.Sprintf("%v", row[length-1])
		if strings.Contains(line, "Sort") || strings.Contains(line, "HashJoin") {
			require.NotContains(t, disk, "0 Bytes")
			require.True(t, strings.Contains(disk, "MB") ||
				strings.Contains(disk, "KB") ||
				strings.Contains(disk, "Bytes"))
		}
	}
}

func TestAQSortExecMatchesStdSortExec(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '+00:00'")

	tk.MustExec("drop table if exists t_aqsort")
	tk.MustExec(`
create table t_aqsort (
  id bigint primary key,
  s varchar(300) collate utf8mb4_general_ci,
  u bigint unsigned,
  d decimal(20,6),
  ts timestamp(6) null,
  tm time(6) null,
  n int null
)`)

	words := []string{"a", "A", "abc", "AbC", "hello", "HeLLo", "", "z"}
	var buf bytes.Buffer
	buf.WriteString("insert into t_aqsort values ")
	for i := 0; i < 200; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		s := words[i%len(words)]
		if i%17 == 0 {
			pattern := s + "_"
			repeat := (200 + len(pattern) - 1) / len(pattern)
			s = strings.Repeat(pattern, repeat)[:200]
		}
		u := uint64(i%7) * 100
		d := fmt.Sprintf("%d.%06d", i%13-6, (i*97)%1_000_000)

		var ts any = "NULL"
		if i%9 != 0 {
			t := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * time.Minute).Format("2006-01-02 15:04:05.000000")
			ts = "'" + t + "'"
		}

		var tm any = "NULL"
		if i%11 != 0 {
			tm = fmt.Sprintf("'0%d:%02d:%02d.%06d'", i%10, i%60, (i*7)%60, (i*19)%1_000_000)
		}

		var n any = "NULL"
		if i%5 != 0 {
			n = fmt.Sprintf("%d", i%23-11)
		}

		buf.WriteString(fmt.Sprintf("(%d,'%s',%d,%s,%s,%s,%s)", i, s, u, d, ts, tm, n))
	}
	tk.MustExec(buf.String())

	query := `
select id, s, u, d, ts, tm, n
from t_aqsort
order by s, n desc, u, d desc, ts, tm, id`
	windowQuery := `
select sum(rn) from (
  select row_number() over (order by s, n desc, u, d desc, ts, tm, id) as rn
  from t_aqsort
) t`

	tk.MustExec("set tidb_enable_aqsort=0")
	std := tk.MustQuery(query).Rows()
	stdWindow := tk.MustQuery(windowQuery).Rows()

	tk.MustExec("set tidb_enable_aqsort=1")
	aqs := tk.MustQuery(query).Rows()
	require.Equal(t, std, aqs)
	aqsWindow := tk.MustQuery(windowQuery).Rows()
	require.Equal(t, stdWindow, aqsWindow)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/sortexec/AQSortForceEncodeKeyError", "return(true)"))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/sortexec/AQSortForceEncodeKeyError"))
	}()
	aqsFallback := tk.MustQuery(query).Rows()
	require.Equal(t, std, aqsFallback)
	aqsWindowFallback := tk.MustQuery(windowQuery).Rows()
	require.Equal(t, stdWindow, aqsWindowFallback)
}
