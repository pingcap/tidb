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

package memtest

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

func TestInsertUpdateTrackerOnCleanUp(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")

	originConsume := tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed()
	// assert insert
	tk.MustExec("insert t (id) values (1)")
	tk.MustExec("insert t (id) values (2)")
	tk.MustExec("insert t (id) values (3)")
	afterConsume := tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed()
	require.Equal(t, afterConsume, originConsume)

	originConsume = tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed()
	// assert update
	tk.MustExec("update t set id = 4 where id = 1")
	tk.MustExec("update t set id = 5 where id = 2")
	tk.MustExec("update t set id = 6 where id = 3")
	afterConsume = tk.Session().GetSessionVars().StmtCtx.MemTracker.BytesConsumed()
	require.Equal(t, afterConsume, originConsume)
}

func TestGlobalMemArbitrator(t *testing.T) {
	memory.SetupGlobalMemArbitratorForTest(t.TempDir())
	defer memory.CleanupGlobalMemArbitratorForTest()

	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExecToErr("set @@tidb_mem_arbitrator_mode = standard") // only global
	require.Equal(t, tk.ExecToErr("set global tidb_mem_arbitrator_mode = 1").Error(), "tidb_mem_arbitrator_mode: disable; standard; priority;")
	require.Equal(t, memory.ArbitratorModeDisable, memory.GlobalMemArbitrator().WorkMode())
	require.True(t, memory.GlobalMemArbitrator() == nil)

	tk.MustExec("set global tidb_mem_arbitrator_mode = standard")
	tk.MustQuery("select @@tidb_mem_arbitrator_mode").Check(testkit.Rows("standard"))
	require.Equal(t, memory.ArbitratorModeStandard, memory.GlobalMemArbitrator().WorkMode())

	tk.MustExec("set global tidb_mem_arbitrator_mode = priority")
	tk.MustQuery("select @@tidb_mem_arbitrator_mode").Check(testkit.Rows("priority"))
	require.Equal(t, memory.ArbitratorModePriority, memory.GlobalMemArbitrator().WorkMode())

	tk.MustExec("set global tidb_mem_arbitrator_mode = default")
	tk.MustQuery("select @@tidb_mem_arbitrator_mode").Check(testkit.Rows("disable"))
	require.Equal(t, memory.ArbitratorModeDisable, memory.GlobalMemArbitrator().WorkMode())

	const maxServerLimit uint64 = 1e15
	maxServerLimitStr := fmt.Sprintf("%d", maxServerLimit)
	tk.MustExec(fmt.Sprintf("set global tidb_server_memory_limit=%d", maxServerLimit))
	tk.MustQuery("select @@tidb_server_memory_limit").Check(testkit.Rows(maxServerLimitStr))
	require.Equal(t, maxServerLimit, memory.ServerMemoryLimit.Load())
	require.Equal(t, uint64(0), memory.GlobalMemArbitrator().Limit()) // 0 under disable mode

	require.Equal(t, tk.ExecToErr("set global tidb_mem_arbitrator_soft_limit=-1").Error(), "tidb_mem_arbitrator_soft_limit: 0 (default); (0, 1.0] float-rate * server-limit; (1, server-limit] integer bytes; auto;")
	tk.MustExec("set global tidb_mem_arbitrator_soft_limit = 12345678")
	tk.MustQuery("select @@tidb_mem_arbitrator_soft_limit").Check(testkit.Rows("12345678"))
	require.Equal(t, uint64(0), memory.GlobalMemArbitrator().SoftLimit()) // 0 under disable mode

	// normal under standard mode
	tk.MustExec("set global tidb_mem_arbitrator_mode = standard")
	require.Equal(t, maxServerLimit, memory.GlobalMemArbitrator().Limit())
	require.Equal(t, uint64(12345678), memory.GlobalMemArbitrator().SoftLimit())

	tk.MustExecToErr("set @@tidb_mem_arbitrator_soft_limit = 0") // only global

	// default: 0.95 * server-limit
	tk.MustExec("set global tidb_mem_arbitrator_soft_limit = default")
	tk.MustQuery("select @@tidb_mem_arbitrator_soft_limit").Check(testkit.Rows("0"))
	require.Equal(t, uint64(float64(memory.ServerMemoryLimit.Load())*0.95), memory.GlobalMemArbitrator().SoftLimit())

	// 1.0 * server-limit
	tk.MustExec("set global tidb_mem_arbitrator_soft_limit = 1")
	tk.MustQuery("select @@tidb_mem_arbitrator_soft_limit").Check(testkit.Rows("1"))
	require.Equal(t, uint64(float64(memory.ServerMemoryLimit.Load())), memory.GlobalMemArbitrator().SoftLimit())
	// 0.5 * server-limit
	tk.MustExec("set global tidb_mem_arbitrator_soft_limit = 0.5")
	tk.MustQuery("select @@tidb_mem_arbitrator_soft_limit").Check(testkit.Rows("0.5"))
	require.Equal(t, uint64(float64(memory.ServerMemoryLimit.Load())*0.5), memory.GlobalMemArbitrator().SoftLimit())

	// fixed value: le than server-limit
	tk.MustExec(fmt.Sprintf("set global tidb_mem_arbitrator_soft_limit=%d", maxServerLimit*10))
	tk.MustQuery("select @@tidb_mem_arbitrator_soft_limit").Check(testkit.Rows(fmt.Sprintf("%d", maxServerLimit*10)))
	require.Equal(t, memory.ServerMemoryLimit.Load(), memory.GlobalMemArbitrator().SoftLimit())
	tk.MustExec("set global tidb_mem_arbitrator_soft_limit = 100")
	tk.MustQuery("select @@tidb_mem_arbitrator_soft_limit").Check(testkit.Rows("100"))
	require.Equal(t, uint64(100), memory.GlobalMemArbitrator().SoftLimit())

	// auto mode
	tk.MustExec("set global tidb_mem_arbitrator_soft_limit = auto")
	tk.MustQuery("select @@tidb_mem_arbitrator_soft_limit").Check(testkit.Rows("auto"))
	require.Equal(t, uint64(float64(memory.ServerMemoryLimit.Load())*0.95), memory.GlobalMemArbitrator().SoftLimit())

	require.Equal(t, tk.ExecToErr("set tidb_mem_arbitrator_wait_averse=anonymous").Error(), "tidb_mem_arbitrator_wait_averse: 0 (disable); 1 (enable); nolimit;")
	tk.MustExec("set tidb_mem_arbitrator_wait_averse = 1")
	tk.MustQuery("select @@tidb_mem_arbitrator_wait_averse").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_mem_arbitrator_wait_averse = default")
	tk.MustQuery("select @@tidb_mem_arbitrator_wait_averse").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_mem_arbitrator_wait_averse = nolimit")
	tk.MustQuery("select @@tidb_mem_arbitrator_wait_averse").Check(testkit.Rows("nolimit"))
	tk.MustExecToErr("set global tidb_mem_arbitrator_wait_averse=0")
	tk.MustExec("set tidb_mem_arbitrator_wait_averse=0")
	tk.MustQuery("select @@tidb_mem_arbitrator_wait_averse").Check(testkit.Rows("0"))

	tk.MustExecToErr("set global tidb_mem_arbitrator_query_reserved = 0")
	tk.MustQuery("select @@tidb_mem_arbitrator_query_reserved").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_mem_arbitrator_query_reserved = default")
	tk.MustQuery("select @@tidb_mem_arbitrator_query_reserved").Check(testkit.Rows("0"))
	require.Equal(t, tk.ExecToErr("set tidb_mem_arbitrator_query_reserved = 9223372036854775808").Error(), "tidb_mem_arbitrator_query_reserved: 0 (default); (1, server-limit] integer bytes;")

	tk.MustExecToErr("set tidb_mem_arbitrator_query_reserved = 1")
	tk.MustExec("set tidb_mem_arbitrator_query_reserved = 2")
	tk.MustQuery("select @@tidb_mem_arbitrator_query_reserved").Check(testkit.Rows("2"))
	tk.MustExec("set tidb_mem_arbitrator_query_reserved = 100000")
	tk.MustQuery("select @@tidb_mem_arbitrator_query_reserved").Check(testkit.Rows("100000"))
	tk.MustQuery("select /*+ set_var(tidb_mem_arbitrator_query_reserved=1234) */ @@tidb_mem_arbitrator_query_reserved").Check(testkit.Rows("1234"))
	tk.MustExec("set tidb_mem_arbitrator_query_reserved = default")

	tk.MustExec("set global tidb_enable_resource_control=on")
	tk.MustExec("create resource group rg1 RU_PER_SEC=111 priority=LOW")
	tk.MustExec("create resource group rg2 RU_PER_SEC=222 priority=HIGH")
	tk.MustExec("create resource group rg3 RU_PER_SEC=333")
	tk.MustQuery("select NAME,RU_PER_SEC,PRIORITY from information_schema.resource_groups where name='rg2'").Check(testkit.Rows("rg2 222 HIGH"))
	tk.MustQuery("select NAME,RU_PER_SEC,PRIORITY from information_schema.resource_groups where name='rg3'").Check(testkit.Rows("rg3 333 MEDIUM"))
	tk.MustQuery("select NAME,RU_PER_SEC,PRIORITY from information_schema.resource_groups where name='rg1'").Check(testkit.Rows("rg1 111 LOW"))

	tk.MustExec("use test; create table t (a int)")
	tk.MustExec("insert into t values (1)")

	tk.MustQuery("select /*+ resource_group(rg1) set_var(tidb_mem_arbitrator_query_reserved=100000) */ * from t").Check(testkit.Rows("1"))
	tk.MustQuery("select /*+ resource_group(rg2) set_var(tidb_mem_arbitrator_query_reserved=100000) */ * from t").Check(testkit.Rows("1"))
	tk.MustQuery("select /*+ resource_group(rg3) set_var(tidb_mem_arbitrator_query_reserved=100000) */ * from t").Check(testkit.Rows("1"))

	{
		execMetrics := memory.GlobalMemArbitrator().ExecMetrics()
		require.Equal(t, int64(3), execMetrics.Task.Succ)
		require.Equal(t, int64(0), execMetrics.Task.Fail)
		require.Equal(t, memory.NumByPriority{0, 0, 0}, execMetrics.Task.SuccByPriority)
	}

	tk.MustExec("set global tidb_mem_arbitrator_mode = priority")
	tk.MustQuery("select /*+ resource_group(rg1) set_var(tidb_mem_arbitrator_query_reserved=100000) */ * from t").Check(testkit.Rows("1"))
	{
		execMetrics := memory.GlobalMemArbitrator().ExecMetrics()
		require.Equal(t, int64(4), execMetrics.Task.Succ)
		require.Equal(t, int64(0), execMetrics.Task.Fail)
		require.Equal(t, memory.NumByPriority{1, 0, 0}, execMetrics.Task.SuccByPriority)
	}
	tk.MustQuery("select /*+ resource_group(rg2) set_var(tidb_mem_arbitrator_query_reserved=100000) */ * from t").Check(testkit.Rows("1"))
	{
		execMetrics := memory.GlobalMemArbitrator().ExecMetrics()
		require.Equal(t, int64(5), execMetrics.Task.Succ)
		require.Equal(t, int64(0), execMetrics.Task.Fail)
		require.Equal(t, memory.NumByPriority{1, 0, 1}, execMetrics.Task.SuccByPriority)
	}
	tk.MustQuery("select /*+ resource_group(rg3) set_var(tidb_mem_arbitrator_query_reserved=100000) */ * from t").Check(testkit.Rows("1"))

	{
		execMetrics := memory.GlobalMemArbitrator().ExecMetrics()
		require.Equal(t, int64(6), execMetrics.Task.Succ)
		require.Equal(t, int64(0), execMetrics.Task.Fail)
		require.Equal(t, memory.NumByPriority{1, 1, 1}, execMetrics.Task.SuccByPriority)
	}

	expectTaskFail := int64(0)
	expectCancelWaitAverse := int64(0)
	expectCancelStandardMode := int64(0)
	tk.MustExec("set tidb_mem_arbitrator_wait_averse=1")
	for i := range 3 {
		sql := fmt.Sprintf("select /*+ resource_group(rg%d) set_var(tidb_mem_arbitrator_query_reserved=%s) */ * from t", i+1, maxServerLimitStr)
		require.ErrorContains(t, tk.QueryToErr(sql), "[executor:8180]Query execution was stopped by the global memory arbitrator [reason=CANCEL(out-of-quota & wait-averse)] [conn=")
		expectTaskFail++
		expectCancelWaitAverse++
		{
			execMetrics := memory.GlobalMemArbitrator().ExecMetrics()
			require.Equal(t, int64(6), execMetrics.Task.Succ)
			require.Equal(t, expectTaskFail, execMetrics.Task.Fail)
			require.Equal(t, memory.NumByPriority{1, 1, 1}, execMetrics.Task.SuccByPriority)
			require.Equal(t, expectCancelWaitAverse, execMetrics.Cancel.WaitAverse)
		}
	}

	{ // out-of-quota under standard mode
		tk.MustExec("set global tidb_mem_arbitrator_mode = standard")
		for i := range 3 {
			sql := fmt.Sprintf("select /*+ resource_group(rg%d) set_var(tidb_mem_arbitrator_query_reserved=%s) */ * from t", i+1, maxServerLimitStr)
			require.ErrorContains(t, tk.QueryToErr(sql), "[executor:8180]Query execution was stopped by the global memory arbitrator [reason=CANCEL(out-of-quota & standard-mode)] [conn=")
			expectCancelStandardMode++
			expectTaskFail++
			{
				execMetrics := memory.GlobalMemArbitrator().ExecMetrics()
				require.Equal(t, int64(6), execMetrics.Task.Succ)
				require.Equal(t, expectTaskFail, execMetrics.Task.Fail)
				require.Equal(t, memory.NumByPriority{1, 1, 1}, execMetrics.Task.SuccByPriority)
				require.Equal(t, expectCancelWaitAverse, execMetrics.Cancel.WaitAverse)
				require.Equal(t, expectCancelStandardMode, execMetrics.Cancel.StandardMode)
			}
		}
	}
	{
		require.True(t, tk.Session().GetSessionVars().ConnectionID != 0)
		b := memory.GlobalMemArbitrator().GetAwaitFreeBudgets(tk.Session().GetSessionVars().ConnectionID)
		b.ConsumeQuota(0, memory.DefMaxLimit)
		require.True(t, b.Used.Load() == memory.DefMaxLimit)
		tk.MustExec("set global tidb_mem_arbitrator_mode = standard")
		tk.MustExec("set tidb_mem_arbitrator_wait_averse = default")
		m0 := memory.GlobalMemArbitrator().ExecMetrics()
		require.ErrorContains(t, tk.ExecToErr("select /*+ set_var(tidb_mem_arbitrator_query_reserved=1) */ * from t"), "[executor:8180]Query execution was stopped by the global memory arbitrator [reason=CANCEL(out-of-quota & standard-mode), path=ParseSQL] [conn=")
		m1 := memory.GlobalMemArbitrator().ExecMetrics()
		require.Equal(t, m0.Cancel, m1.Cancel)
		tk.MustExec("set global tidb_mem_arbitrator_mode = priority")
		tk.MustExec("set tidb_mem_arbitrator_wait_averse = 1")
		require.ErrorContains(t, tk.ExecToErr("select * from t"), "[executor:8180]Query execution was stopped by the global memory arbitrator [reason=CANCEL(out-of-quota & wait-averse), path=ParseSQL] [conn=")
		m2 := memory.GlobalMemArbitrator().ExecMetrics()
		require.Equal(t, m2.Cancel, m1.Cancel)
		tk.MustExec("set tidb_mem_arbitrator_wait_averse = default")
		tk.MustExec("select * from information_schema.resource_groups where name<>'?'")
		m3 := memory.GlobalMemArbitrator().ExecMetrics()
		require.Equal(t, m2.Cancel, m3.Cancel)
		require.Equal(t, m2.Task.Succ+1, m3.Task.Succ)
		require.Equal(t, m2.Task.SuccByPriority[1]+1, m3.Task.SuccByPriority[1])
		b.ConsumeQuota(0, -memory.DefMaxLimit)
	}
	// out-of-quota under priority mode & kill under oom risk will be covered in tracker tests
}
