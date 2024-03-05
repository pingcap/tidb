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
	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze/exec"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
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
	require.True(t, strings.Contains(err1.Error(), "Your query has been cancelled due to exceeding the allowed memory limit for the tidb-server instance and this query is currently using the most memory. Please try narrowing your query scope or increase the tidb_server_memory_limit and try again."))
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
	tk.MustExec("insert into t select 1")
	for i := 1; i <= 8; i++ {
		tk.MustExec("insert into t select * from t") // 256 Lines
	}
	_, err0 := tk.Exec("analyze table t with 1.0 samplerate;")
	require.NoError(t, err0)
	rs0 := tk.MustQuery("select fail_reason from mysql.analyze_jobs where table_name=? and state=? limit 1", "t", "failed")
	require.Len(t, rs0.Rows(), 0)

	h := dom.StatsHandle()
	originalVal4 := exec.AutoAnalyzeMinCnt
	originalVal5 := tk.MustQuery("select @@global.tidb_auto_analyze_ratio").Rows()[0][0].(string)
	exec.AutoAnalyzeMinCnt = 0
	tk.MustExec("set global tidb_auto_analyze_ratio = 0.001")
	defer func() {
		exec.AutoAnalyzeMinCnt = originalVal4
		tk.MustExec(fmt.Sprintf("set global tidb_auto_analyze_ratio = %v", originalVal5))
	}()

	sm := &testkit.MockSessionManager{
		Dom: dom,
		PS:  []*util.ProcessInfo{tk.Session().ShowProcess()},
	}
	dom.ServerMemoryLimitHandle().SetSessionManager(sm)
	go dom.ServerMemoryLimitHandle().Run()

	tk.MustExec("insert into t values(4),(5),(6)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	err := h.Update(dom.InfoSchema())
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
	tk.MustExec("create table tbl_2 ( col_20 decimal default 84232 , col_21 tinyint not null , col_22 int default 80814394 , col_23 mediumint default -8036687 not null , col_24 smallint default 9185 not null , col_25 tinyint unsigned default 65 , col_26 char(115) default 'ZyfroRODMbNDRZnPNRW' not null , col_27 bigint not null , col_28 tinyint not null , col_29 char(130) default 'UMApsVgzHblmY' , primary key idx_14 ( col_28,col_22 ) , unique key idx_15 ( col_24,col_22 ) , key idx_16 ( col_21,col_20,col_24,col_25,col_27,col_28,col_26,col_29 ) , key idx_17 ( col_24,col_25 ) , unique key idx_18 ( col_25,col_23,col_29,col_27,col_26,col_22 ) , key idx_19 ( col_25,col_22,col_26,col_23 ) , unique key idx_20 ( col_22,col_24,col_28,col_29,col_26,col_20 ) , key idx_21 ( col_25,col_24,col_26,col_29,col_27,col_22,col_28 ) ) partition by range ( col_22 ) ( partition p0 values less than (-1938341588), partition p1 values less than (-1727506184), partition p2 values less than (-1700184882), partition p3 values less than (-1596142809), partition p4 values less than (445165686) );")
	tk.MustExec("insert ignore into tbl_2 values ( 942,33,-1915007317,3408149,-3699,193,'Trywdis',1876334369465184864,115,null );")
	tk.MustExec("insert ignore into tbl_2 values ( 7,-39,-1382727205,-2544981,-28075,88,'FDhOsTRKRLCwEk',-1239168882463214388,17,'WskQzCK' );")
	tk.MustExec("insert ignore into tbl_2 values ( null,55,-388460319,-2292918,10130,162,'UqjDlYvdcNY',4872802276956896607,-51,'ORBQjnumcXP' );")
	tk.MustExec("insert ignore into tbl_2 values ( 42,-19,-9677826,-1168338,16904,79,'TzOqH',8173610791128879419,65,'lNLcvOZDcRzWvDO' );")
	tk.MustExec("insert ignore into tbl_2 values ( 2,26,369867543,-6773303,-24953,41,'BvbdrKTNtvBgsjjnxt',5996954963897924308,-95,'wRJYPBahkIGDfz' );")
	tk.MustExec("insert ignore into tbl_2 values ( 6896,3,444460824,-2070971,-13095,167,'MvWNKbaOcnVuIrtbT',6968339995987739471,-5,'zWipNBxGeVmso' );")
	tk.MustExec("insert ignore into tbl_2 values ( 58761,112,-1535034546,-5837390,-14204,157,'',-8319786912755096816,15,'WBjsozfBfrPPHmKv' );")
	tk.MustExec("insert ignore into tbl_2 values ( 84923,113,-973946646,406140,25040,51,'THQdwkQvppWZnULm',5469507709881346105,94,'oGNmoxLLgHkdyDCT' );")
	tk.MustExec("insert ignore into tbl_2 values ( 0,-104,-488745187,-1941015,-2646,39,'jyKxfs',-5307175470406648836,46,'KZpfjFounVgFeRPa' );")
	tk.MustExec("insert ignore into tbl_2 values ( 4,97,2105289255,1034363,28385,192,'',4429378142102752351,8,'jOk' );")
	tk.MustExec("set global tidb_mem_quota_analyze=128;")
	tk.MustExecToErr("analyze table tbl_2;")
}

func TestMemQuotaAnalyze2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table tbl_2 ( col_20 decimal default 84232 , col_21 tinyint not null , col_22 int default 80814394 , col_23 mediumint default -8036687 not null , col_24 smallint default 9185 not null , col_25 tinyint unsigned default 65 , col_26 char(115) default 'ZyfroRODMbNDRZnPNRW' not null , col_27 bigint not null , col_28 tinyint not null , col_29 char(130) default 'UMApsVgzHblmY' , primary key idx_14 ( col_28,col_22 ) , unique key idx_15 ( col_24,col_22 ) , key idx_16 ( col_21,col_20,col_24,col_25,col_27,col_28,col_26,col_29 ) , key idx_17 ( col_24,col_25 ) , unique key idx_18 ( col_25,col_23,col_29,col_27,col_26,col_22 ) , key idx_19 ( col_25,col_22,col_26,col_23 ) , unique key idx_20 ( col_22,col_24,col_28,col_29,col_26,col_20 ) , key idx_21 ( col_25,col_24,col_26,col_29,col_27,col_22,col_28 ) );")
	tk.MustExec("insert ignore into tbl_2 values ( 942,33,-1915007317,3408149,-3699,193,'Trywdis',1876334369465184864,115,null );")
	tk.MustExec("set global tidb_mem_quota_analyze=128;")
	tk.MustExecToErr("analyze table tbl_2;")
}
