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
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/statistics"
	"github.com/pingcap/tidb/pkg/statistics/handle/ddl/testutil"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

func newLiveSessionManager(dom *domain.Domain, sessions ...sessionapi.Session) *testkit.MockSessionManager {
	conn := make(map[uint64]sessionapi.Session, len(sessions))
	for _, se := range sessions {
		conn[se.GetSessionVars().ConnectionID] = se
	}
	return &testkit.MockSessionManager{
		Dom:  dom,
		Conn: conn,
	}
}

func TestLiveSessionManagerTracksLatestProcessInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	connID := tk.Session().GetSessionVars().ConnectionID

	staleSM := &testkit.MockSessionManager{
		PS: []*sessmgr.ProcessInfo{tk.Session().ShowProcess()},
	}
	liveSM := newLiveSessionManager(nil, tk.Session())

	nextTime := time.Now().Add(time.Second)
	tk.Session().SetProcessInfo("analyze table t with 1.0 samplerate", nextTime, mysql.ComQuery, 0)

	staleInfo, ok := staleSM.GetProcessInfo(connID)
	require.True(t, ok)
	liveInfo, ok := liveSM.GetProcessInfo(connID)
	require.True(t, ok)

	require.NotSame(t, staleInfo, liveInfo)
	require.NotEqual(t, staleInfo.Time, liveInfo.Time)
	require.NotEqual(t, staleInfo.Info, liveInfo.Info)
	require.Equal(t, nextTime, liveInfo.Time)
	require.Equal(t, "analyze table t with 1.0 samplerate", liveInfo.Info)
}

func TestGlobalMemoryControlForAnalyze(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk0 := testkit.NewTestKit(t, store)
	tk0.MustExec("set global tidb_mem_oom_action = 'cancel'")
	tk0.MustExec("set global tidb_server_memory_limit = 512MB")
	tk0.MustExec("set global tidb_server_memory_limit_sess_min_size = 128")

	sm := newLiveSessionManager(dom, tk0.Session())
	dom.ServerMemoryLimitHandle().SetSessionManager(sm)
	go dom.ServerMemoryLimitHandle().Run()

	tk0.MustExec("use test")
	tk0.MustExec("create table t(a int)")
	tk0.MustExec("insert into t select 1")
	for i := 1; i <= 8; i++ {
		tk0.MustExec("insert into t select * from t") // 256 Lines
	}
	sql := "analyze table t with 1.0 samplerate;" // Need about 100MB
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/memory/ReadMemStats", `return(536870912)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockAnalyzeMergeWorkerSlowConsume", `return(100)`))
	_, err := tk0.Exec(sql)
	require.True(t, strings.Contains(err.Error(), "Your query has been cancelled due to exceeding the allowed memory limit for the tidb-server instance and this query is currently using the most memory. Please try narrowing your query scope or increase the tidb_server_memory_limit and try again."))
	runtime.GC()
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/memory/ReadMemStats"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockAnalyzeMergeWorkerSlowConsume"))
	tk0.MustExec(sql)
}

func TestGlobalMemoryControlForPrepareAnalyze(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk0 := testkit.NewTestKit(t, store)
	tk0.MustExec("set global tidb_mem_oom_action = 'cancel'")
	tk0.MustExec("set global tidb_mem_quota_query = 209715200 ") // 200MB
	tk0.MustExec("set global tidb_server_memory_limit = 5GB")
	tk0.MustExec("set global tidb_server_memory_limit_sess_min_size = 128")

	sm := newLiveSessionManager(dom, tk0.Session())
	dom.ServerMemoryLimitHandle().SetSessionManager(sm)
	go dom.ServerMemoryLimitHandle().Run()

	tk0.MustExec("use test")
	tk0.MustExec("create table t(a int)")
	tk0.MustExec("insert into t select 1")
	for i := 1; i <= 8; i++ {
		tk0.MustExec("insert into t select * from t") // 256 Lines
	}
	sqlPrepare := "prepare stmt from 'analyze table t with 1.0 samplerate';"
	sqlExecute := "execute stmt;"                                                                                     // Need about 100MB
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/memory/ReadMemStats", `return(536870912)`)) // 512MB
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockAnalyzeMergeWorkerSlowConsume", `return(100)`))
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
	require.True(t, strings.Contains(err1.Error(), "Your query has been cancelled due to exceeding the allowed memory limit for the tidb-server instance and this query is currently using the most memory. Please try narrowing your query scope or increase the tidb_server_memory_limit and try again."), err1.Error())
	runtime.GC()
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/memory/ReadMemStats"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockAnalyzeMergeWorkerSlowConsume"))
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
	h := dom.StatsHandle()
	testutil.HandleNextDDLEventWithTxn(h)
	tk.MustExec("insert into t select 1")
	for i := 1; i <= 8; i++ {
		tk.MustExec("insert into t select * from t") // 256 Lines
	}
	_, err0 := tk.Exec("analyze table t with 1.0 samplerate;")
	require.NoError(t, err0)
	rs0 := tk.MustQuery("select fail_reason from mysql.analyze_jobs where table_name=? and state=? limit 1", "t", "failed")
	require.Len(t, rs0.Rows(), 0)

	originalVal4 := statistics.AutoAnalyzeMinCnt
	originalVal5 := tk.MustQuery("select @@global.tidb_auto_analyze_ratio").Rows()[0][0].(string)
	statistics.AutoAnalyzeMinCnt = 0
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.001")
	defer func() {
		statistics.AutoAnalyzeMinCnt = originalVal4
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_ratio = %v", originalVal5))
	}()

	sm := newLiveSessionManager(dom, tk.Session())
	dom.ServerMemoryLimitHandle().SetSessionManager(sm)
	go dom.ServerMemoryLimitHandle().Run()

	tk.MustExec("insert into t values(4),(5),(6)")
	tk.MustExec("flush stats_delta *.*")
	err := h.Update(context.Background(), dom.InfoSchema())
	require.NoError(t, err)

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/memory/ReadMemStats", `return(536870912)`))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockAnalyzeMergeWorkerSlowConsume", `return(100)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/memory/ReadMemStats"))
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockAnalyzeMergeWorkerSlowConsume"))
	}()
	tk.MustQuery("select 1")
	childTrackers = executor.GlobalAnalyzeMemoryTracker.GetChildrenForTest()
	require.Len(t, childTrackers, 0)

	h.HandleAutoAnalyze()
	rs := tk.MustQuery("select fail_reason from mysql.analyze_jobs where table_name=? and state=? limit 1", "t", "failed")
	failReason := rs.Rows()[0][0].(string)
	require.True(t, strings.Contains(failReason, "Your query has been cancelled due to exceeding the allowed memory limit for the tidb-server instance and this query is currently using the most memory. Please try narrowing your query scope or increase the tidb_server_memory_limit and try again."))

	childTrackers = executor.GlobalAnalyzeMemoryTracker.GetChildrenForTest()
	require.Len(t, childTrackers, 0)
}

func TestMemQuotaAnalyze(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table tbl_2 (a int, b varchar(64), c int, primary key(a), key idx_b(b), key idx_c(c)) partition by range (a) (partition p0 values less than (0), partition p1 values less than (10), partition p2 values less than (maxvalue));")
	tk.MustExec("insert into tbl_2 values (-1, 'a', 1), (1, 'b', 2), (11, 'c', 3);")
	tk.MustExec("set global tidb_mem_quota_analyze=128;")
	tk.MustExecToErr("analyze table tbl_2;")
}

func TestMemQuotaAnalyze2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table tbl_2 (a int primary key, b varchar(64), c int, key idx_b(b), key idx_c(c));")
	tk.MustExec("insert into tbl_2 values (1, 'a', 1), (2, 'b', 2), (3, 'c', 3);")
	tk.MustExec("set global tidb_mem_quota_analyze=128;")
	tk.MustExecToErr("analyze table tbl_2;")
}

func TestAnalyzeV2MemoryUsageMetricNeverNegative(t *testing.T) {
	// This test should be fast because the whole package is marked as `timeout = "short"` in Bazel.
	const valueLen = 8 * 1024
	intest.Assert(statistics.MaxSampleValueLength > valueLen)

	store, _ := testkit.CreateMockStoreAndDomain(t)
	oldChildTrackers := executor.GlobalAnalyzeMemoryTracker.GetChildrenForTest()
	for _, tracker := range oldChildTrackers {
		tracker.Detach()
	}
	defer func() {
		for _, tracker := range oldChildTrackers {
			tracker.AttachTo(executor.GlobalAnalyzeMemoryTracker)
		}
	}()
	executor.GlobalAnalyzeMemoryTracker.ReplaceBytesUsed(0)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_analyze_version=2")
	tk.MustExec("set @@tidb_build_sampling_stats_concurrency=1")
	tk.MustExec("set @@tidb_analyze_skip_column_types = ''")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_mem_usage")
	tk.MustExec("create table t_mem_usage(a text collate utf8mb4_general_ci)")
	tk.MustExec(fmt.Sprintf("insert into t_mem_usage values (repeat('a', %d))", valueLen))
	for range 6 {
		tk.MustExec("insert into t_mem_usage select a from t_mem_usage")
	}

	tk.MustExec("analyze table t_mem_usage with 1.0 samplerate;")
}

func TestAnalyzeSessionMemTrackerDetachOnClose(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	err := tk.ExecToErr("analyze table test.not_exists with 1 topn")
	require.Error(t, err)

	vars := tk.Session().GetSessionVars()
	// Clear any residue from the analyze statement to make the delta deterministic.
	vars.MemTracker.ReplaceBytesUsed(0)
	base := executor.GlobalAnalyzeMemoryTracker.BytesConsumed()
	vars.MemTracker.Consume(1024)
	require.Equal(t, base+1024, executor.GlobalAnalyzeMemoryTracker.BytesConsumed())

	tk.Session().Close()
	require.Equal(t, base, executor.GlobalAnalyzeMemoryTracker.BytesConsumed())
}
