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

package memorycontrol

import (
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/statistics/handle"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/memory"
	"github.com/stretchr/testify/require"
)

func TestGlobalMemoryControlForAnalyze(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk0 := testkit.NewTestKit(t, store)
	tk0.MustExec("set global tidb_mem_oom_action = 'cancel'")
	tk0.MustExec("set global tidb_server_memory_limit = 512MB")
	tk0.MustExec("set global tidb_server_memory_limit_sess_min_size = 128")

	sm := &testkit.MockSessionManager{
		PS: []*util.ProcessInfo{tk0.Session().ShowProcess()},
	}
	dom.ServerMemoryLimitHandle().SetSessionManager(sm)
	go dom.ServerMemoryLimitHandle().Run()

	tk0.MustExec("use test")
	tk0.MustExec("create table t(a int)")
	tk0.MustExec("insert into t select 1")
	for i := 1; i <= 8; i++ {
		tk0.MustExec("insert into t select * from t") // 256 Lines
	}
	sql := "analyze table t with 1.0 samplerate;" // Need about 100MB
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/util/memory/ReadMemStats", `return(536870912)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockAnalyzeMergeWorkerSlowConsume", `return(100)`))
	_, err := tk0.Exec(sql)
	require.True(t, strings.Contains(err.Error(), memory.PanicMemoryExceedWarnMsg+memory.WarnMsgSuffixForInstance))
	runtime.GC()
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/util/memory/ReadMemStats"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockAnalyzeMergeWorkerSlowConsume"))
	tk0.MustExec(sql)
}

func TestGlobalMemoryControlForPrepareAnalyze(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk0 := testkit.NewTestKit(t, store)
	tk0.MustExec("set global tidb_mem_oom_action = 'cancel'")
	tk0.MustExec("set global tidb_mem_quota_query = 209715200 ") // 200MB
	tk0.MustExec("set global tidb_server_memory_limit = 5GB")
	tk0.MustExec("set global tidb_server_memory_limit_sess_min_size = 128")

	sm := &testkit.MockSessionManager{
		PS: []*util.ProcessInfo{tk0.Session().ShowProcess()},
	}
	dom.ServerMemoryLimitHandle().SetSessionManager(sm)
	go dom.ServerMemoryLimitHandle().Run()

	tk0.MustExec("use test")
	tk0.MustExec("create table t(a int)")
	tk0.MustExec("insert into t select 1")
	for i := 1; i <= 8; i++ {
		tk0.MustExec("insert into t select * from t") // 256 Lines
	}
	sqlPrepare := "prepare stmt from 'analyze table t with 1.0 samplerate';"
	sqlExecute := "execute stmt;"                                                                                 // Need about 100MB
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/util/memory/ReadMemStats", `return(536870912)`)) // 512MB
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockAnalyzeMergeWorkerSlowConsume", `return(100)`))
	runtime.GC()
	// won't be killed by tidb_mem_quota_query
	tk0.MustExec(sqlPrepare)
	tk0.MustExec(sqlExecute)
	runtime.GC()
	// killed by tidb_server_memory_limit
	tk0.MustExec("set global tidb_server_memory_limit = 512MB")
	_, err0 := tk0.Exec(sqlPrepare)
	require.NoError(t, err0)
	_, err1 := tk0.Exec(sqlExecute)
	// Killed and the WarnMsg is WarnMsgSuffixForInstance instead of WarnMsgSuffixForSingleQuery
	require.True(t, strings.Contains(err1.Error(), memory.PanicMemoryExceedWarnMsg+memory.WarnMsgSuffixForInstance))
	runtime.GC()
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/util/memory/ReadMemStats"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockAnalyzeMergeWorkerSlowConsume"))
	tk0.MustExec(sqlPrepare)
	tk0.MustExec(sqlExecute)
}

func TestGlobalMemoryControlForAutoAnalyze(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	originalVal1 := tk.MustQuery("select @@global.tidb_mem_oom_action").Rows()[0][0].(string)
	tk.MustExec("set global tidb_mem_oom_action = 'cancel'")
	//originalVal2 := tk.MustQuery("select @@global.tidb_server_memory_limit").Rows()[0][0].(string)
	tk.MustExec("set global tidb_server_memory_limit = 512MB")
	originalVal3 := tk.MustQuery("select @@global.tidb_server_memory_limit_sess_min_size").Rows()[0][0].(string)
	tk.MustExec("set global tidb_server_memory_limit_sess_min_size = 128")
	defer func() {
		tk.MustExec(fmt.Sprintf("set global tidb_mem_oom_action = %v", originalVal1))
		//tk.MustExec(fmt.Sprintf("set global tidb_server_memory_limit = %v", originalVal2))
		tk.MustExec(fmt.Sprintf("set global tidb_server_memory_limit_sess_min_size = %v", originalVal3))
	}()

	// clean child trackers
	oldChildTrackers := executor.GlobalAnalyzeMemoryTracker.GetChildrenForTest()
	for _, tracker := range oldChildTrackers {
		tracker.Detach()
	}
	defer func() {
		for _, tracker := range oldChildTrackers {
			tracker.AttachTo(executor.GlobalAnalyzeMemoryTracker)
		}
	}()
	childTrackers := executor.GlobalAnalyzeMemoryTracker.GetChildrenForTest()
	require.Len(t, childTrackers, 0)

	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t select 1")
	for i := 1; i <= 8; i++ {
		tk.MustExec("insert into t select * from t") // 256 Lines
	}
	_, err0 := tk.Exec("analyze table t with 1.0 samplerate;")
	require.NoError(t, err0)
	rs0 := tk.MustQuery("select fail_reason from mysql.analyze_jobs where table_name=? and state=? limit 1", "t", "failed")
	require.Len(t, rs0.Rows(), 0)

	h := dom.StatsHandle()
	originalVal4 := handle.AutoAnalyzeMinCnt
	originalVal5 := tk.MustQuery("select @@global.tidb_auto_analyze_ratio").Rows()[0][0].(string)
	handle.AutoAnalyzeMinCnt = 0
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.001")
	defer func() {
		handle.AutoAnalyzeMinCnt = originalVal4
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_ratio = %v", originalVal5))
	}()

	sm := &testkit.MockSessionManager{
		Dom: dom,
		PS:  []*util.ProcessInfo{tk.Session().ShowProcess()},
	}
	dom.ServerMemoryLimitHandle().SetSessionManager(sm)
	go dom.ServerMemoryLimitHandle().Run()

	tk.MustExec("insert into t values(4),(5),(6)")
	require.NoError(t, h.DumpStatsDeltaToKV(handle.DumpAll))
	err := h.Update(dom.InfoSchema())
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/util/memory/ReadMemStats", `return(536870912)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockAnalyzeMergeWorkerSlowConsume", `return(100)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/util/memory/ReadMemStats"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockAnalyzeMergeWorkerSlowConsume"))
	}()
	tk.MustQuery("select 1")
	childTrackers = executor.GlobalAnalyzeMemoryTracker.GetChildrenForTest()
	require.Len(t, childTrackers, 0)

	h.HandleAutoAnalyze(dom.InfoSchema())
	rs := tk.MustQuery("select fail_reason from mysql.analyze_jobs where table_name=? and state=? limit 1", "t", "failed")
	failReason := rs.Rows()[0][0].(string)
	require.True(t, strings.Contains(failReason, memory.PanicMemoryExceedWarnMsg+memory.WarnMsgSuffixForInstance))

	childTrackers = executor.GlobalAnalyzeMemoryTracker.GetChildrenForTest()
	require.Len(t, childTrackers, 0)
}
