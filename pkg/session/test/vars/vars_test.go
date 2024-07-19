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

package vars

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/domain"
	tikv "github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/txnkv/transaction"
)

func TestKVVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_backoff_lock_fast = 1")
	tk.MustExec("set @@tidb_backoff_weight = 100")
	tk.MustExec("create table if not exists kvvars (a int key)")
	tk.MustExec("insert into kvvars values (1)")
	tk.MustExec("begin")
	txn, err := tk.Session().Txn(false)
	require.NoError(t, err)
	vars := txn.GetVars().(*tikv.Variables)
	require.Equal(t, 1, vars.BackoffLockFast)
	require.Equal(t, 100, vars.BackOffWeight)
	tk.MustExec("rollback")
	tk.MustExec("set @@tidb_backoff_weight = 50")
	tk.MustExec("set @@autocommit = 0")
	tk.MustExec("select * from kvvars")
	require.True(t, tk.Session().GetSessionVars().InTxn())
	txn, err = tk.Session().Txn(false)
	require.NoError(t, err)
	vars = txn.GetVars().(*tikv.Variables)
	require.Equal(t, 50, vars.BackOffWeight)

	tk.MustExec("set @@autocommit = 1")
	require.Nil(t, failpoint.Enable("tikvclient/probeSetVars", `return(true)`))
	tk.MustExec("select * from kvvars where a = 1")
	require.Nil(t, failpoint.Disable("tikvclient/probeSetVars"))
	require.True(t, transaction.SetSuccess.Load())
	transaction.SetSuccess.Store(false)
}

func TestRemovedSysVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	variable.RegisterSysVar(&variable.SysVar{Scope: variable.ScopeGlobal | variable.ScopeSession, Name: "bogus_var", Value: "acdc"})
	result := tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'bogus_var'")
	result.Check(testkit.Rows("bogus_var acdc"))
	result = tk.MustQuery("SELECT @@GLOBAL.bogus_var")
	result.Check(testkit.Rows("acdc"))
	tk.MustExec("SET GLOBAL bogus_var = 'newvalue'")

	// unregister
	variable.UnregisterSysVar("bogus_var")

	result = tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'bogus_var'")
	result.Check(testkit.Rows()) // empty
	tk.MustContainErrMsg("SET GLOBAL bogus_var = 'newvalue'", "[variable:1193]Unknown system variable 'bogus_var'")
	tk.MustContainErrMsg("SELECT @@GLOBAL.bogus_var", "[variable:1193]Unknown system variable 'bogus_var'")
}

func TestTiKVSystemVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	result := tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'tidb_gc_enable'") // default is on from the sysvar
	result.Check(testkit.Rows("tidb_gc_enable ON"))
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_enable'")
	result.Check(testkit.Rows()) // but no value in the table (yet) because the value has not been set and the GC has never been run

	// update will set a value in the table
	tk.MustExec("SET GLOBAL tidb_gc_enable = 1")
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_enable'")
	result.Check(testkit.Rows("true"))

	tk.MustExec("UPDATE mysql.tidb SET variable_value = 'false' WHERE variable_name='tikv_gc_enable'")
	result = tk.MustQuery("SELECT @@tidb_gc_enable;")
	result.Check(testkit.Rows("0")) // reads from mysql.tidb value and changes to false

	tk.MustExec("SET GLOBAL tidb_gc_concurrency = -1") // sets auto concurrency and concurrency
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_auto_concurrency'")
	result.Check(testkit.Rows("true"))
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_concurrency'")
	result.Check(testkit.Rows("-1"))

	tk.MustExec("SET GLOBAL tidb_gc_concurrency = 5") // sets auto concurrency and concurrency
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_auto_concurrency'")
	result.Check(testkit.Rows("false"))
	result = tk.MustQuery("SELECT variable_value FROM mysql.tidb WHERE variable_name = 'tikv_gc_concurrency'")
	result.Check(testkit.Rows("5"))

	tk.MustExec("UPDATE mysql.tidb SET variable_value = 'true' WHERE variable_name='tikv_gc_auto_concurrency'")
	result = tk.MustQuery("SELECT @@tidb_gc_concurrency;")
	result.Check(testkit.Rows("-1")) // because auto_concurrency is turned on it takes precedence

	tk.MustExec("REPLACE INTO mysql.tidb (variable_value, variable_name) VALUES ('15m', 'tikv_gc_run_interval')")
	result = tk.MustQuery("SELECT @@GLOBAL.tidb_gc_run_interval;")
	result.Check(testkit.Rows("15m0s"))
	result = tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'tidb_gc_run_interval'")
	result.Check(testkit.Rows("tidb_gc_run_interval 15m0s"))

	tk.MustExec("SET GLOBAL tidb_gc_run_interval = '9m'") // too small
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Truncated incorrect tidb_gc_run_interval value: '9m'"))
	result = tk.MustQuery("SHOW GLOBAL VARIABLES LIKE 'tidb_gc_run_interval'")
	result.Check(testkit.Rows("tidb_gc_run_interval 10m0s"))

	tk.MustExec("SET GLOBAL tidb_gc_run_interval = '700000000000ns'") // specified in ns, also valid

	_, err := tk.Exec("SET GLOBAL tidb_gc_run_interval = '11mins'")
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'tidb_gc_run_interval'", err.Error())
}

func TestUpgradeSysvars(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session().(variable.GlobalVarAccessor)

	// Set the global var to a non-canonical form of the value
	// i.e. implying that it was set from an earlier version of TiDB.

	tk.MustExec(`REPLACE INTO mysql.global_variables (variable_name, variable_value) VALUES ('tidb_enable_noop_functions', '0')`)
	domain.GetDomain(tk.Session()).NotifyUpdateSysVarCache(true) // update cache
	v, err := se.GetGlobalSysVar("tidb_enable_noop_functions")
	require.NoError(t, err)
	require.Equal(t, "OFF", v)

	// Set the global var to ""  which is the invalid version of this from TiDB 4.0.16
	// the err is quashed by the GetGlobalSysVar, and the default value is restored.
	// This helps callers of GetGlobalSysVar(), which can't individually be expected
	// to handle upgrade/downgrade issues correctly.

	tk.MustExec(`REPLACE INTO mysql.global_variables (variable_name, variable_value) VALUES ('rpl_semi_sync_slave_enabled', '')`)
	domain.GetDomain(tk.Session()).NotifyUpdateSysVarCache(true) // update cache
	v, err = se.GetGlobalSysVar("rpl_semi_sync_slave_enabled")
	require.NoError(t, err)
	require.Equal(t, "OFF", v) // the default value is restored.
	result := tk.MustQuery("SHOW VARIABLES LIKE 'rpl_semi_sync_slave_enabled'")
	result.Check(testkit.Rows("rpl_semi_sync_slave_enabled OFF"))

	// Ensure variable out of range is converted to in range after upgrade.
	// This further helps for https://github.com/pingcap/tidb/pull/28842

	tk.MustExec(`REPLACE INTO mysql.global_variables (variable_name, variable_value) VALUES ('tidb_executor_concurrency', '999')`)
	domain.GetDomain(tk.Session()).NotifyUpdateSysVarCache(true) // update cache
	v, err = se.GetGlobalSysVar("tidb_executor_concurrency")
	require.NoError(t, err)
	require.Equal(t, "256", v) // the max value is restored.

	// Handle the case of a completely bogus value from an earlier version of TiDB.
	// This could be the case if an ENUM sysvar removes a value.

	tk.MustExec(`REPLACE INTO mysql.global_variables (variable_name, variable_value) VALUES ('tidb_enable_noop_functions', 'SOMEVAL')`)
	domain.GetDomain(tk.Session()).NotifyUpdateSysVarCache(true) // update cache
	v, err = se.GetGlobalSysVar("tidb_enable_noop_functions")
	require.NoError(t, err)
	require.Equal(t, "OFF", v) // the default value is restored.
}

func TestSetInstanceSysvarBySetGlobalSysVar(t *testing.T) {
	varName := "tidb_general_log"
	defaultValue := "OFF" // This is the default value for tidb_general_log

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	se := tk.Session().(variable.GlobalVarAccessor)

	// Get globalSysVar twice and get the same default value
	v, err := se.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, defaultValue, v)
	v, err = se.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, defaultValue, v)

	// session.GetGlobalSysVar would not get the value which session.SetGlobalSysVar writes,
	// because SetGlobalSysVar calls SetGlobalFromHook, which uses TiDBGeneralLog's SetGlobal,
	// but GetGlobalSysVar could not access TiDBGeneralLog's GetGlobal.

	// set to "1"
	err = se.SetGlobalSysVar(context.Background(), varName, "ON")
	require.NoError(t, err)
	v, err = se.GetGlobalSysVar(varName)
	tk.MustQuery("select @@global.tidb_general_log").Check(testkit.Rows("1"))
	require.NoError(t, err)
	require.Equal(t, defaultValue, v)

	// set back to "0"
	err = se.SetGlobalSysVar(context.Background(), varName, defaultValue)
	require.NoError(t, err)
	v, err = se.GetGlobalSysVar(varName)
	tk.MustQuery("select @@global.tidb_general_log").Check(testkit.Rows("0"))
	require.NoError(t, err)
	require.Equal(t, defaultValue, v)
}

func TestTimeZone(t *testing.T) {
	store := testkit.CreateMockStore(t)

	// TestCastTimeToDate
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '-8:00'")
	date := time.Now().In(time.FixedZone("", -8*int(time.Hour/time.Second)))
	tk.MustQuery("select cast(time('12:23:34') as date)").Check(testkit.Rows(date.Format(time.DateOnly)))
	tk.MustExec("set time_zone = '+08:00'")
	date = time.Now().In(time.FixedZone("", 8*int(time.Hour/time.Second)))
	tk.MustQuery("select cast(time('12:23:34') as date)").Check(testkit.Rows(date.Format(time.DateOnly)))

	// TestSetGlobalTZ
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +08:00"))
	tk.MustExec("set global time_zone = '+00:00'")
	tk.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +08:00"))
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +00:00"))
}

func TestGlobalVarAccessor(t *testing.T) {
	varName := "max_allowed_packet"
	varValue := strconv.FormatUint(variable.DefMaxAllowedPacket, 10) // This is the default value for max_allowed_packet

	// The value of max_allowed_packet should be a multiple of 1024,
	// so the setting of varValue1 and varValue2 would be truncated to varValue0
	varValue0 := "4194304"
	varValue1 := "4194305"
	varValue2 := "4194306"

	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	se := tk.Session().(variable.GlobalVarAccessor)
	// Get globalSysVar twice and get the same value
	v, err := se.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, varValue, v)
	v, err = se.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, varValue, v)
	// Set global var to another value
	err = se.SetGlobalSysVar(context.Background(), varName, varValue1)
	require.NoError(t, err)
	v, err = se.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, varValue0, v)
	require.NoError(t, tk.Session().CommitTxn(context.TODO()))

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	se1 := tk1.Session().(variable.GlobalVarAccessor)
	v, err = se1.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, varValue0, v)
	err = se1.SetGlobalSysVar(context.Background(), varName, varValue2)
	require.NoError(t, err)
	v, err = se1.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, varValue0, v)
	require.NoError(t, tk1.Session().CommitTxn(context.TODO()))

	// Make sure the change is visible to any client that accesses that global variable.
	v, err = se.GetGlobalSysVar(varName)
	require.NoError(t, err)
	require.Equal(t, varValue0, v)

	// For issue 10955, make sure the new session load `max_execution_time` into sessionVars.
	tk1.MustExec("set @@global.max_execution_time = 100")
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
	require.Equal(t, uint64(100), tk2.Session().GetSessionVars().MaxExecutionTime)
	require.Equal(t, uint64(100), tk2.Session().GetSessionVars().GetMaxExecutionTime())
	tk1.MustExec("set @@global.max_execution_time = 0")

	result := tk.MustQuery("show global variables  where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))
	result = tk.MustQuery("show session variables  where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))
	tk.MustExec("set session sql_select_limit=100000000000;")
	result = tk.MustQuery("show global variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))
	result = tk.MustQuery("show session variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 100000000000"))
	tk.MustExec("set @@global.sql_select_limit = 1")
	result = tk.MustQuery("show global variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 1"))
	tk.MustExec("set @@global.sql_select_limit = default")
	result = tk.MustQuery("show global variables where variable_name='sql_select_limit';")
	result.Check(testkit.Rows("sql_select_limit 18446744073709551615"))

	result = tk.MustQuery("select @@global.autocommit;")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select @@autocommit;")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@global.autocommit = 0;")
	result = tk.MustQuery("select @@global.autocommit;")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select @@autocommit;")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@global.autocommit=1")

	err = tk.ExecToErr("set global time_zone = 'timezone'")
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, variable.ErrUnknownTimeZone))
}

func TestPrepareExecuteWithSQLHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	se := tk.Session()
	se.SetConnectionID(1)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int primary key)")

	type hintCheck struct {
		hint  string
		check func(*hint.StmtHints)
	}

	hintChecks := []hintCheck{
		{
			hint: "MEMORY_QUOTA(1024 MB)",
			check: func(stmtHint *hint.StmtHints) {
				require.True(t, stmtHint.HasMemQuotaHint)
				require.Equal(t, int64(1024*1024*1024), stmtHint.MemQuotaQuery)
			},
		},
		{
			hint: "READ_CONSISTENT_REPLICA()",
			check: func(stmtHint *hint.StmtHints) {
				require.True(t, stmtHint.HasReplicaReadHint)
				require.Equal(t, byte(tikv.ReplicaReadFollower), stmtHint.ReplicaRead)
			},
		},
		{
			hint: "MAX_EXECUTION_TIME(1000)",
			check: func(stmtHint *hint.StmtHints) {
				require.True(t, stmtHint.HasMaxExecutionTime)
				require.Equal(t, uint64(1000), stmtHint.MaxExecutionTime)
			},
		},
		{
			hint: "USE_TOJA(TRUE)",
			check: func(stmtHint *hint.StmtHints) {
				require.True(t, stmtHint.HasAllowInSubqToJoinAndAggHint)
				require.True(t, stmtHint.AllowInSubqToJoinAndAgg)
			},
		},
		{
			hint: "RESOURCE_GROUP(rg1)",
			check: func(stmtHint *hint.StmtHints) {
				require.True(t, stmtHint.HasResourceGroup)
				require.Equal(t, "rg1", stmtHint.ResourceGroup)
			},
		},
	}

	for i, check := range hintChecks {
		// common path
		tk.MustExec(fmt.Sprintf("prepare stmt%d from 'select /*+ %s */ * from t'", i, check.hint))
		for j := 0; j < 10; j++ {
			tk.MustQuery(fmt.Sprintf("execute stmt%d", i))
			check.check(&tk.Session().GetSessionVars().StmtCtx.StmtHints)
		}
		// fast path
		tk.MustExec(fmt.Sprintf("prepare fast%d from 'select /*+ %s */ * from t where a = 1'", i, check.hint))
		for j := 0; j < 10; j++ {
			tk.MustQuery(fmt.Sprintf("execute fast%d", i))
			check.check(&tk.Session().GetSessionVars().StmtCtx.StmtHints)
		}
	}
}
