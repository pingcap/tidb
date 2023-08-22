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
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/copr"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/memory"
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

func TestTiDBReadStaleness(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_read_staleness='-5'")
	tk.MustExec("set @@tidb_read_staleness='-100'")
	err := tk.ExecToErr("set @@tidb_read_staleness='-5s'")
	require.Error(t, err)
	err = tk.ExecToErr("set @@tidb_read_staleness='foo'")
	require.Error(t, err)
	tk.MustExec("set @@tidb_read_staleness=''")
	tk.MustExec("set @@tidb_read_staleness='0'")
}

// TestSetGroupConcatMaxLen is for issue #7034
func TestSetGroupConcatMaxLen(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Normal case
	tk.MustExec("set global group_concat_max_len = 100")
	tk.MustExec("set @@session.group_concat_max_len = 50")
	result := tk.MustQuery("show global variables  where variable_name='group_concat_max_len';")
	result.Check(testkit.Rows("group_concat_max_len 100"))

	result = tk.MustQuery("show session variables  where variable_name='group_concat_max_len';")
	result.Check(testkit.Rows("group_concat_max_len 50"))

	result = tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("50"))

	result = tk.MustQuery("select @@global.group_concat_max_len;")
	result.Check(testkit.Rows("100"))

	result = tk.MustQuery("select @@session.group_concat_max_len;")
	result.Check(testkit.Rows("50"))

	tk.MustExec("set @@group_concat_max_len = 1024")

	result = tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("1024"))

	result = tk.MustQuery("select @@global.group_concat_max_len;")
	result.Check(testkit.Rows("100"))

	result = tk.MustQuery("select @@session.group_concat_max_len;")
	result.Check(testkit.Rows("1024"))

	// Test value out of range
	tk.MustExec("set @@group_concat_max_len=1")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect group_concat_max_len value: '1'"))
	result = tk.MustQuery("select @@group_concat_max_len;")
	result.Check(testkit.Rows("4"))

	_, err := tk.Exec("set @@group_concat_max_len = 18446744073709551616")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar), fmt.Sprintf("err %v", err))

	// Test illegal type
	_, err = tk.Exec("set @@group_concat_max_len='hello'")
	require.True(t, terror.ErrorEqual(err, variable.ErrWrongTypeForVar), fmt.Sprintf("err %v", err))
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/copr/testRateLimitActionMockConsumeAndAssert", `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/copr/testRateLimitActionMockConsumeAndAssert"))
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
		tk.MustExec("use testoom")
		tk.MustExec("set @@tidb_enable_rate_limit_action=0")
		tk.MustExec("set @@tidb_distsql_scan_concurrency = 10")
		tk.MustExec(fmt.Sprintf("set @@tidb_mem_quota_query=%v;", quota))
		err := tk.QueryToErr(sql)
		require.Error(t, err)
		require.Regexp(t, memory.PanicMemoryExceedWarnMsg+memory.WarnMsgSuffixForSingleQuery, err)
	}

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/copr/testRateLimitActionMockWaitMax", `return(true)`))
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
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/copr/testRateLimitActionMockWaitMax"))

	// assert oom fallback
	for _, testcase := range testcases {
		t.Log(testcase.name)
		se, err := session.CreateSession4Test(store)
		require.NoError(t, err)
		tk.SetSession(se)
		tk.MustExec("use testoom")
		tk.MustExec("set tidb_distsql_scan_concurrency = 1")
		tk.MustExec("set @@tidb_mem_quota_query=1;")
		err = tk.QueryToErr(testcase.sql)
		require.Error(t, err)
		require.Regexp(t, memory.PanicMemoryExceedWarnMsg+memory.WarnMsgSuffixForSingleQuery, err)
		se.Close()
	}
}

func TestStatementCountLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	setTxnTk := testkit.NewTestKit(t, store)
	setTxnTk.MustExec("set global tidb_txn_mode=''")
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table stmt_count_limit (id int)")
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.StmtCountLimit = 3
	})
	tk.MustExec("set tidb_disable_txn_auto_retry = 0")
	tk.MustExec("begin")
	tk.MustExec("insert into stmt_count_limit values (1)")
	tk.MustExec("insert into stmt_count_limit values (2)")
	_, err := tk.Exec("insert into stmt_count_limit values (3)")
	require.Error(t, err)

	// begin is counted into history but this one is not.
	tk.MustExec("SET SESSION autocommit = false")
	tk.MustExec("insert into stmt_count_limit values (1)")
	tk.MustExec("insert into stmt_count_limit values (2)")
	tk.MustExec("insert into stmt_count_limit values (3)")
	_, err = tk.Exec("insert into stmt_count_limit values (4)")
	require.Error(t, err)
}

// TestDefaultWeekFormat checks for issue #21510.
func TestDefaultWeekFormat(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("set @@global.default_week_format = 4;")
	defer tk1.MustExec("set @@global.default_week_format = default;")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustQuery("select week('2020-02-02'), @@default_week_format, week('2020-02-02');").Check(testkit.Rows("6 4 6"))
}

func TestIssue21944(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	_, err := tk1.Exec("set @@tidb_current_ts=1;")
	require.Equal(t, "[variable:1238]Variable 'tidb_current_ts' is a read only variable", err.Error())
}

func TestIssue21943(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	_, err := tk.Exec("set @@last_plan_from_binding='123';")
	require.Equal(t, "[variable:1238]Variable 'last_plan_from_binding' is a read only variable", err.Error())

	_, err = tk.Exec("set @@last_plan_from_cache='123';")
	require.Equal(t, "[variable:1238]Variable 'last_plan_from_cache' is a read only variable", err.Error())
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

func TestGlobalVarCollationServer(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@global.collation_server=utf8mb4_general_ci")
	tk.MustQuery("show global variables like 'collation_server'").Check(testkit.Rows("collation_server utf8mb4_general_ci"))
	tk = testkit.NewTestKit(t, store)
	tk.MustQuery("show global variables like 'collation_server'").Check(testkit.Rows("collation_server utf8mb4_general_ci"))
	tk.MustQuery("show variables like 'collation_server'").Check(testkit.Rows("collation_server utf8mb4_general_ci"))
}

func TestMemoryUsageAlarmVariable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=1")
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("1"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=0")
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("0"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=0.7")
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("0.7"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=1.1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_memory_usage_alarm_ratio value: '1.1'"))
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("1"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_ratio=-1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_memory_usage_alarm_ratio value: '-1'"))
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_ratio").Check(testkit.Rows("0"))
	require.Error(t, tk.ExecToErr("set @@session.tidb_memory_usage_alarm_ratio=0.8"))

	tk.MustExec("set @@global.tidb_memory_usage_alarm_keep_record_num=1")
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_keep_record_num").Check(testkit.Rows("1"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_keep_record_num=100")
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_keep_record_num").Check(testkit.Rows("100"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_keep_record_num=0")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_memory_usage_alarm_keep_record_num value: '0'"))
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_keep_record_num").Check(testkit.Rows("1"))
	tk.MustExec("set @@global.tidb_memory_usage_alarm_keep_record_num=10001")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_memory_usage_alarm_keep_record_num value: '10001'"))
	tk.MustQuery("select @@global.tidb_memory_usage_alarm_keep_record_num").Check(testkit.Rows("10000"))
}

func TestSelectLockInShare(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t_sel_in_share")
	tk.MustExec("CREATE TABLE t_sel_in_share (id int DEFAULT NULL)")
	tk.MustExec("insert into t_sel_in_share values (11)")
	require.Error(t, tk.ExecToErr("select * from t_sel_in_share lock in share mode"))
	tk.MustExec("set @@tidb_enable_noop_functions = 1")
	tk.MustQuery("select * from t_sel_in_share lock in share mode").Check(testkit.Rows("11"))
	tk.MustExec("DROP TABLE t_sel_in_share")
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

	tk.MustQuery("select @@MAX_EXECUTION_TIME;").Check(testkit.Rows("0"))
	tk.MustQuery("select @@global.MAX_EXECUTION_TIME;").Check(testkit.Rows("0"))
	tk.MustQuery("select /*+ MAX_EXECUTION_TIME(1000) */ * FROM MaxExecTime;")

	tk.MustExec("set @@global.MAX_EXECUTION_TIME = 300;")
	tk.MustQuery("select * FROM MaxExecTime;")

	tk.MustExec("set @@MAX_EXECUTION_TIME = 150;")
	tk.MustQuery("select * FROM MaxExecTime;")

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

func TestEnablePartition(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_table_partition=off")
	tk.MustQuery("show variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition OFF"))

	tk.MustExec("set global tidb_enable_table_partition = on")

	tk.MustQuery("show variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition OFF"))
	tk.MustQuery("show global variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition ON"))

	tk.MustExec("set tidb_enable_list_partition=off")
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition OFF"))
	tk.MustExec("set global tidb_enable_list_partition=on")
	tk.MustQuery("show global variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition OFF"))

	tk.MustExec("set tidb_enable_list_partition=1")
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))

	tk.MustExec("set tidb_enable_list_partition=on")
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))

	tk.MustQuery("show global variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
	tk.MustExec("set global tidb_enable_list_partition=off")
	tk.MustQuery("show global variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition OFF"))
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
	tk.MustExec("set tidb_enable_list_partition=off")
	tk.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition OFF"))

	tk.MustExec("set global tidb_enable_list_partition=on")
	tk.MustQuery("show global variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustQuery("show variables like 'tidb_enable_table_partition'").Check(testkit.Rows("tidb_enable_table_partition ON"))
	tk1.MustQuery("show variables like 'tidb_enable_list_partition'").Check(testkit.Rows("tidb_enable_list_partition ON"))
}

func TestIgnoreForeignKey(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@foreign_key_checks=0")
	sqlText := `CREATE TABLE address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`
	tk.MustExec(sqlText)
}

func TestIndexMergeRuntimeStats(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_index_merge = 1")
	tk.MustExec("create table t1(id int primary key, a int, b int, c int, d int)")
	tk.MustExec("create index t1a on t1(a)")
	tk.MustExec("create index t1b on t1(b)")
	tk.MustExec("insert into t1 values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5)")
	rows := tk.MustQuery("explain analyze select /*+ use_index_merge(t1, primary, t1a) */ * from t1 where id < 2 or a > 4;").Rows()
	require.Len(t, rows, 4)
	explain := fmt.Sprintf("%v", rows[0])
	pattern := ".*time:.*loops:.*index_task:{fetch_handle:.*, merge:.*}.*table_task:{num.*concurrency.*fetch_row.*wait_time.*}.*"
	require.Regexp(t, pattern, explain)
	tableRangeExplain := fmt.Sprintf("%v", rows[1])
	indexExplain := fmt.Sprintf("%v", rows[2])
	tableExplain := fmt.Sprintf("%v", rows[3])
	require.Regexp(t, ".*time:.*loops:.*cop_task:.*", tableRangeExplain)
	require.Regexp(t, ".*time:.*loops:.*cop_task:.*", indexExplain)
	require.Regexp(t, ".*time:.*loops:.*cop_task:.*", tableExplain)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustQuery("select /*+ use_index_merge(t1, primary, t1a) */ * from t1 where id < 2 or a > 4 order by a").Check(testkit.Rows("1 1 1 1 1", "5 5 5 5 5"))
}

func TestSysdateIsNow(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("show variables like '%tidb_sysdate_is_now%'").Check(testkit.Rows("tidb_sysdate_is_now OFF"))
	require.False(t, tk.Session().GetSessionVars().SysdateIsNow)
	tk.MustExec("set @@tidb_sysdate_is_now=true")
	tk.MustQuery("show variables like '%tidb_sysdate_is_now%'").Check(testkit.Rows("tidb_sysdate_is_now ON"))
	require.True(t, tk.Session().GetSessionVars().SysdateIsNow)
}

func TestSessionAlias(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select @@tidb_session_alias").Check(testkit.Rows(""))
	// normal set
	tk.MustExec("set @@tidb_session_alias='alias123'")
	tk.MustQuery("select @@tidb_session_alias").Check(testkit.Rows("alias123"))
	// set a long value
	val := "0123456789012345678901234567890123456789012345678901234567890123456789"
	tk.MustExec("set @@tidb_session_alias=?", val)
	tk.MustQuery("select @@tidb_session_alias").Check(testkit.Rows(val[:64]))
	// an invalid value
	err := tk.ExecToErr("set @@tidb_session_alias='abc '")
	require.EqualError(t, err, "[variable:1231]Incorrect value for variable @@tidb_session_alias 'abc '")
	tk.MustQuery("select @@tidb_session_alias").Check(testkit.Rows(val[:64]))
	// reset to empty
	tk.MustExec("set @@tidb_session_alias=''")
	tk.MustQuery("select @@tidb_session_alias").Check(testkit.Rows(""))
}
