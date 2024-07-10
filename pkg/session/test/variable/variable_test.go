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

package variable

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

func TestForbidSettingBothTSVariable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	// For mock tikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	// Set tidb_snapshot and assert tidb_read_staleness
	tk.MustExec("set @@tidb_snapshot = '2007-01-01 15:04:05.999999'")
	tk.MustGetErrMsg("set @@tidb_read_staleness='-5'", "tidb_snapshot should be clear before setting tidb_read_staleness")
	tk.MustExec("set @@tidb_snapshot = ''")
	tk.MustExec("set @@tidb_read_staleness='-5'")

	// Set tidb_read_staleness and assert tidb_snapshot
	tk.MustExec("set @@tidb_read_staleness='-5'")
	tk.MustGetErrMsg("set @@tidb_snapshot = '2007-01-01 15:04:05.999999'", "tidb_read_staleness should be clear before setting tidb_snapshot")
	tk.MustExec("set @@tidb_read_staleness = ''")
	tk.MustExec("set @@tidb_snapshot = '2007-01-01 15:04:05.999999'")
}

func TestCoprocessorOOMAction(t *testing.T) {
	// Assert Coprocessor OOMAction
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_enable_rate_limit_action=true")
	tk.MustExec("create database testoom")
	tk.MustExec("use testoom")
	tk.MustExec(`set @@tidb_wait_split_region_finish=1`)
	// create table for non keep-order case
	tk.MustExec("drop table if exists t5")
	tk.MustExec("create table t5(id int)")
	tk.MustQuery(`split table t5 between (0) and (10000) regions 10`).Check(testkit.Rows("9 1"))
	// create table for keep-order case
	tk.MustExec("drop table if exists t6")
	tk.MustExec("create table t6(id int, index(id))")
	tk.MustQuery(`split table t6 between (0) and (10000) regions 10`).Check(testkit.Rows("10 1"))
	tk.MustQuery("split table t6 INDEX id between (0) and (10000) regions 10;").Check(testkit.Rows("10 1"))
	count := 10
	for i := 0; i < count; i++ {
		tk.MustExec(fmt.Sprintf("insert into t5 (id) values (%v)", i))
		tk.MustExec(fmt.Sprintf("insert into t6 (id) values (%v)", i))
	}

	testcases := []struct {
		name string
		sql  string
	}{
		{
			name: "keep Order",
			sql:  "select id from t6 order by id",
		},
		{
			name: "non keep Order",
			sql:  "select id from t5",
		},
	}
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/testRateLimitActionMockConsumeAndAssert", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/testRateLimitActionMockConsumeAndAssert"))
	}()

	enableOOM := func(tk *testkit.TestKit, name, sql string) {
		t.Logf("enable OOM, testcase: %v", name)
		// larger than 4 copResponse, smaller than 5 copResponse
		quota := 5*copr.MockResponseSizeForTest - 100
		defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
		tk.MustExec("SET GLOBAL tidb_mem_oom_action='CANCEL'")
		tk.MustExec("use testoom")
		tk.MustExec("set @@tidb_enable_rate_limit_action=1")
		tk.MustExec("set @@tidb_distsql_scan_concurrency = 10")
		tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%v;", quota))
		var expect []string
		for i := 0; i < count; i++ {
			expect = append(expect, fmt.Sprintf("%v", i))
		}
		tk.MustQuery(sql).Sort().Check(testkit.Rows(expect...))
		// assert oom action worked by max consumed > memory quota
		require.Greater(t, tk.Session().GetSessionVars().StmtCtx.MemTracker.MaxConsumed(), int64(quota))
	}

	disableOOM := func(tk *testkit.TestKit, name, sql string) {
		t.Logf("disable OOM, testcase: %v", name)
		quota := 5*copr.MockResponseSizeForTest - 100
		tk.MustExec("SET GLOBAL tidb_mem_oom_action='CANCEL'")
		defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
		tk.MustExec("use testoom")
		tk.MustExec("set @@tidb_enable_rate_limit_action=0")
		tk.MustExec("set @@tidb_distsql_scan_concurrency = 10")
		tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%v;", quota))
		err := tk.QueryToErr(sql)
		require.Error(t, err)
		require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(err))
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/testRateLimitActionMockWaitMax", `return(true)`))
	// assert oom action and switch
	for _, testcase := range testcases {
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		tk.SetSession(se)
		enableOOM(tk, testcase.name, testcase.sql)
		tk.MustExec("set @@tidb_enable_rate_limit_action = 0")
		disableOOM(tk, testcase.name, testcase.sql)
		tk.MustExec("set @@tidb_enable_rate_limit_action = 1")
		enableOOM(tk, testcase.name, testcase.sql)
		se.Close()
	}
	globaltk := testkit.NewTestKit(t, store)
	globaltk.MustExec("use testoom")
	globaltk.MustExec("set global tidb_enable_rate_limit_action= 0")
	for _, testcase := range testcases {
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		tk.SetSession(se)
		disableOOM(tk, testcase.name, testcase.sql)
		se.Close()
	}
	globaltk.MustExec("set global tidb_enable_rate_limit_action= 1")
	for _, testcase := range testcases {
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		tk.SetSession(se)
		enableOOM(tk, testcase.name, testcase.sql)
		se.Close()
	}
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/testRateLimitActionMockWaitMax"))

	// assert oom fallback
	for _, testcase := range testcases {
		t.Log(testcase.name)
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		tk.SetSession(se)
		tk.MustExec("use testoom")
		tk.MustExec("set tidb_distsql_scan_concurrency = 1")
		tk.MustExec("set @@tidb_mem_quota_query=1;")
		tk.MustExec("SET GLOBAL tidb_mem_oom_action='CANCEL'")
		err = tk.QueryToErr(testcase.sql)
		require.Error(t, err)
		require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(err))
		tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
		se.Close()
	}
}

func TestCorrectScopeError(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeNone, Name: "sv_none", Value: "acdc"})
	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeGlobal, Name: "sv_global", Value: "acdc"})
	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeSession, Name: "sv_session", Value: "acdc"})
	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeGlobal | variable.ScopeSession, Name: "sv_both", Value: "acdc"})

	// check set behavior

	// none
	_, err := tk.Exec("SET sv_none='acdc'")
	require.Equal(t, "[variable:1238]Variable 'sv_none' is a read only variable", err.Error())
	_, err = tk.Exec("SET GLOBAL sv_none='acdc'")
	require.Equal(t, "[variable:1238]Variable 'sv_none' is a read only variable", err.Error())

	// global
	tk.MustExec("SET GLOBAL sv_global='acdc'")
	_, err = tk.Exec("SET sv_global='acdc'")
	require.Equal(t, "[variable:1229]Variable 'sv_global' is a GLOBAL variable and should be set with SET GLOBAL", err.Error())

	// session
	_, err = tk.Exec("SET GLOBAL sv_session='acdc'")
	require.Equal(t, "[variable:1228]Variable 'sv_session' is a SESSION variable and can't be used with SET GLOBAL", err.Error())
	tk.MustExec("SET sv_session='acdc'")

	// both
	tk.MustExec("SET GLOBAL sv_both='acdc'")
	tk.MustExec("SET sv_both='acdc'")

	// unregister
	variable.UnregisterSysVar("sv_none")
	variable.UnregisterSysVar("sv_global")
	variable.UnregisterSysVar("sv_session")
	variable.UnregisterSysVar("sv_both")
}

func TestReadDMLBatchSize(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set global tidb_dml_batch_size=1000")
	se, err := session.CreateSession(store)
	require.NoError(t, err)

	// `select 1` to load the global variables.
	_, _ = se.Execute(context.TODO(), "select 1")
	require.Equal(t, 1000, se.GetSessionVars().DMLBatchSize)
}

func TestSetEnableRateLimitAction(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("set @@tidb_enable_rate_limit_action=true")
	// assert default value
	result := tk.MustQuery("select @@tidb_enable_rate_limit_action;")
	result.Check(testkit.Rows("1"))
	tk.MustExec("use test")
	tk.MustExec("create table tmp123(id int)")
	rs, err := tk.Exec("select * from tmp123;")
	require.NoError(t, err)
	haveRateLimitAction := false
	action := tk.Session().GetSessionVars().MemTracker.GetFallbackForTest(false)
	for ; action != nil; action = action.GetFallback() {
		if action.GetPriority() == memory.DefRateLimitPriority {
			haveRateLimitAction = true
			break
		}
	}
	require.True(t, haveRateLimitAction)
	err = rs.Close()
	require.NoError(t, err)

	// assert set sys variable
	tk.MustExec("set global tidb_enable_rate_limit_action= '0';")
	tk.Session().Close()

	tk.RefreshSession()
	result = tk.MustQuery("select @@tidb_enable_rate_limit_action;")
	result.Check(testkit.Rows("0"))

	haveRateLimitAction = false
	action = tk.Session().GetSessionVars().MemTracker.GetFallbackForTest(false)
	for ; action != nil; action = action.GetFallback() {
		if action.GetPriority() == memory.DefRateLimitPriority {
			haveRateLimitAction = true
			break
		}
	}
	require.False(t, haveRateLimitAction)
}

func TestMaxExecutionTime(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("use test")
	tk.MustExec("create table MaxExecTime( id int,name varchar(128),age int);")
	tk.MustExec("begin")
	tk.MustExec("insert into MaxExecTime (id,name,age) values (1,'john',18),(2,'lary',19),(3,'lily',18);")

	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(1000) MAX_EXECUTION_TIME(500) */ * FROM MaxExecTime;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "MAX_EXECUTION_TIME() is defined more than once, only the last definition takes effect: MAX_EXECUTION_TIME(500)")
	require.True(t, tk.Session().GetSessionVars().StmtCtx.HasMaxExecutionTime)
	require.Equal(t, uint64(500), tk.Session().GetSessionVars().StmtCtx.MaxExecutionTime)
	require.Equal(t, uint64(500), tk.Session().GetSessionVars().GetMaxExecutionTime())

	tk.MustQuery("select @@MAX_EXECUTION_TIME;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.MAX_EXECUTION_TIME;").Check(testkit.Rows("0"))
	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(1000) */ * FROM MaxExecTime;")

	tk.MustExec("set @@global.MAX_EXECUTION_TIME = 300;")
	tk.MustQuery("select * FROM MaxExecTime;")

	tk.MustExec("set @@MAX_EXECUTION_TIME = 150;")
	tk.MustQuery("select * FROM MaxExecTime;")
	require.Equal(t, uint64(150), tk.Session().GetSessionVars().GetMaxExecutionTime())
	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(1000) */ * FROM MaxExecTime;")
	require.Equal(t, uint64(1000), tk.Session().GetSessionVars().GetMaxExecutionTime())

	tk.MustQuery("select @@global.MAX_EXECUTION_TIME;").Check(testkit.Rows("300"))
	tk.MustQuery("select @@MAX_EXECUTION_TIME;").Check(testkit.Rows("150"))

	tk.MustExec("set @@global.MAX_EXECUTION_TIME = 0;")
	tk.MustExec("set @@MAX_EXECUTION_TIME = 0;")
	tk.MustExec("commit")
	tk.MustExec("drop table if exists MaxExecTime;")
}

func TestReplicaRead(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	require.Equal(t, kv.ReplicaReadLeader, tk.Session().GetSessionVars().GetReplicaRead())
	tk.MustExec("set @@tidb_replica_read = 'follower';")
	require.Equal(t, kv.ReplicaReadFollower, tk.Session().GetSessionVars().GetReplicaRead())
	tk.MustExec("set @@tidb_replica_read = 'leader';")
	require.Equal(t, kv.ReplicaReadLeader, tk.Session().GetSessionVars().GetReplicaRead())
}

func TestIsolationRead(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	require.Len(t, tk.Session().GetSessionVars().GetIsolationReadEngines(), 3)
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash';")
	engines := tk.Session().GetSessionVars().GetIsolationReadEngines()
	require.Len(t, engines, 1)
	_, hasTiFlash := engines[kv.TiFlash]
	_, hasTiKV := engines[kv.TiKV]
	require.True(t, hasTiFlash)
	require.False(t, hasTiKV)
}

func TestLastQueryInfo(t *testing.T) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockRUConsumption", `return()`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockRUConsumption"))
	}()
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec(`prepare stmt1 from 'select * from t'`)
	tk.MustExec("execute stmt1")
	checkMatch := func(actual []string, expected []any) bool {
		return strings.Contains(actual[0], expected[0].(string))
	}
	tk.MustQuery("select @@tidb_last_query_info;").CheckWithFunc(testkit.Rows(`"ru_consumption":15`), checkMatch)
	tk.MustExec("select a from t where a = 1")
	tk.MustQuery("select @@tidb_last_query_info;").CheckWithFunc(testkit.Rows(`"ru_consumption":27`), checkMatch)
	tk.MustQuery("select @@tidb_last_query_info;").CheckWithFunc(testkit.Rows(`"ru_consumption":30`), checkMatch)
}
