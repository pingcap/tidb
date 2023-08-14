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
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	tikv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
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

func TestEnableLegacyInstanceScope(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	// enable 'switching' to SESSION variables
	tk.MustExec("set tidb_enable_legacy_instance_scope = 1")
	tk.MustExec("set tidb_general_log = 1")
	tk.MustQuery(`show warnings`).Check(testkit.Rows(fmt.Sprintf("Warning %d modifying tidb_general_log will require SET GLOBAL in a future version of TiDB", errno.ErrInstanceScope)))
	require.True(t, tk.Session().GetSessionVars().EnableLegacyInstanceScope)

	// disable 'switching' to SESSION variables
	tk.MustExec("set tidb_enable_legacy_instance_scope = 0")
	tk.MustGetErrCode("set tidb_general_log = 1", errno.ErrGlobalVariable)
	require.False(t, tk.Session().GetSessionVars().EnableLegacyInstanceScope)
}

func TestSetPDClientDynamicOption(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 0.5;")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("0.5"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 1;")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 1.5;")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("1.5"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 10;")
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("10"))
	require.Error(t, tk.ExecToErr("set tidb_tso_client_batch_max_wait_time = 0;"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = -1;")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tso_client_batch_max_wait_time value: '-1'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = -0.1;")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tso_client_batch_max_wait_time value: '-0.1'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 10.1;")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tso_client_batch_max_wait_time value: '10.1'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("10"))
	tk.MustExec("set global tidb_tso_client_batch_max_wait_time = 11;")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect tidb_tso_client_batch_max_wait_time value: '11'"))
	tk.MustQuery("select @@tidb_tso_client_batch_max_wait_time;").Check(testkit.Rows("10"))

	tk.MustQuery("select @@tidb_enable_tso_follower_proxy;").Check(testkit.Rows("0"))
	tk.MustExec("set global tidb_enable_tso_follower_proxy = on;")
	tk.MustQuery("select @@tidb_enable_tso_follower_proxy;").Check(testkit.Rows("1"))
	tk.MustExec("set global tidb_enable_tso_follower_proxy = off;")
	tk.MustQuery("select @@tidb_enable_tso_follower_proxy;").Check(testkit.Rows("0"))
	require.Error(t, tk.ExecToErr("set tidb_tso_client_batch_max_wait_time = 0;"))
}

func TestCastTimeToDate(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set time_zone = '-8:00'")
	date := time.Now().In(time.FixedZone("", -8*int(time.Hour/time.Second)))
	tk.MustQuery("select cast(time('12:23:34') as date)").Check(testkit.Rows(date.Format(time.DateOnly)))

	tk.MustExec("set time_zone = '+08:00'")
	date = time.Now().In(time.FixedZone("", 8*int(time.Hour/time.Second)))
	tk.MustQuery("select cast(time('12:23:34') as date)").Check(testkit.Rows(date.Format(time.DateOnly)))
}

func TestSetGlobalTZ(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +08:00"))

	tk.MustExec("set global time_zone = '+00:00'")

	tk.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +08:00"))

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustQuery("show variables like 'time_zone'").Check(testkit.Rows("time_zone +00:00"))
}
func TestSetVarHint(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("sql_mode", mysql.DefaultSQLMode))
	tk.MustQuery("SELECT /*+ SET_VAR(sql_mode=ALLOW_INVALID_DATES) */ @@sql_mode;").Check(testkit.Rows("ALLOW_INVALID_DATES"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@sql_mode;").Check(testkit.Rows(mysql.DefaultSQLMode))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tmp_table_size", "16777216"))
	tk.MustQuery("SELECT /*+ SET_VAR(tmp_table_size=1024) */ @@tmp_table_size;").Check(testkit.Rows("1024"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@tmp_table_size;").Check(testkit.Rows("16777216"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("range_alloc_block_size", "4096"))
	tk.MustQuery("SELECT /*+ SET_VAR(range_alloc_block_size=4294967295) */ @@range_alloc_block_size;").Check(testkit.Rows("4294967295"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@range_alloc_block_size;").Check(testkit.Rows("4096"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_execution_time", "0"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_execution_time=1) */ @@max_execution_time;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_execution_time;").Check(testkit.Rows("0"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_kv_read_timeout", "0"))
	tk.MustQuery("SELECT /*+ SET_VAR(tidb_kv_read_timeout=10) */ @@tidb_kv_read_timeout;").Check(testkit.Rows("10"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@tidb_kv_read_timeout;").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_kv_read_timeout = 5")
	tk.MustQuery("SELECT /*+ tidb_kv_read_timeout(1) */ @@tidb_kv_read_timeout;").Check(testkit.Rows("5"))
	require.Equal(t, tk.Session().GetSessionVars().GetTidbKvReadTimeout(), uint64(1))
	tk.MustQuery("SELECT @@tidb_kv_read_timeout;").Check(testkit.Rows("5"))
	require.Equal(t, tk.Session().GetSessionVars().GetTidbKvReadTimeout(), uint64(5))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("time_zone", "SYSTEM"))
	tk.MustQuery("SELECT /*+ SET_VAR(time_zone='+12:00') */ @@time_zone;").Check(testkit.Rows("+12:00"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@time_zone;").Check(testkit.Rows("SYSTEM"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("join_buffer_size", "262144"))
	tk.MustQuery("SELECT /*+ SET_VAR(join_buffer_size=128) */ @@join_buffer_size;").Check(testkit.Rows("128"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@join_buffer_size;").Check(testkit.Rows("262144"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_length_for_sort_data", "1024"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_length_for_sort_data=4) */ @@max_length_for_sort_data;").Check(testkit.Rows("4"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_length_for_sort_data;").Check(testkit.Rows("1024"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_error_count", "64"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_error_count=0) */ @@max_error_count;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_error_count;").Check(testkit.Rows("64"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("sql_buffer_result", "OFF"))
	tk.MustQuery("SELECT /*+ SET_VAR(sql_buffer_result=ON) */ @@sql_buffer_result;").Check(testkit.Rows("ON"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@sql_buffer_result;").Check(testkit.Rows("OFF"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_heap_table_size", "16777216"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_heap_table_size=16384) */ @@max_heap_table_size;").Check(testkit.Rows("16384"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_heap_table_size;").Check(testkit.Rows("16777216"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tmp_table_size", "16777216"))
	tk.MustQuery("SELECT /*+ SET_VAR(tmp_table_size=16384) */ @@tmp_table_size;").Check(testkit.Rows("16384"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@tmp_table_size;").Check(testkit.Rows("16777216"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("div_precision_increment", "4"))
	tk.MustQuery("SELECT /*+ SET_VAR(div_precision_increment=0) */ @@div_precision_increment;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@div_precision_increment;").Check(testkit.Rows("4"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("sql_auto_is_null", "OFF"))
	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("tidb_enable_noop_functions", "ON"))
	tk.MustQuery("SELECT /*+ SET_VAR(sql_auto_is_null=1) */ @@sql_auto_is_null;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	require.NoError(t, tk.Session().GetSessionVars().SetSystemVarWithoutValidation("tidb_enable_noop_functions", "OFF"))
	tk.MustQuery("SELECT @@sql_auto_is_null;").Check(testkit.Rows("0"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("sort_buffer_size", "262144"))
	tk.MustQuery("SELECT /*+ SET_VAR(sort_buffer_size=32768) */ @@sort_buffer_size;").Check(testkit.Rows("32768"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@sort_buffer_size;").Check(testkit.Rows("262144"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_join_size", "18446744073709551615"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_join_size=1) */ @@max_join_size;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_join_size;").Check(testkit.Rows("18446744073709551615"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_seeks_for_key", "18446744073709551615"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_seeks_for_key=1) */ @@max_seeks_for_key;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_seeks_for_key;").Check(testkit.Rows("18446744073709551615"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_sort_length", "1024"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_sort_length=4) */ @@max_sort_length;").Check(testkit.Rows("4"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_sort_length;").Check(testkit.Rows("1024"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("bulk_insert_buffer_size", "8388608"))
	tk.MustQuery("SELECT /*+ SET_VAR(bulk_insert_buffer_size=0) */ @@bulk_insert_buffer_size;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@bulk_insert_buffer_size;").Check(testkit.Rows("8388608"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("sql_big_selects", "1"))
	tk.MustQuery("SELECT /*+ SET_VAR(sql_big_selects=0) */ @@sql_big_selects;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@sql_big_selects;").Check(testkit.Rows("1"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("read_rnd_buffer_size", "262144"))
	tk.MustQuery("SELECT /*+ SET_VAR(read_rnd_buffer_size=1) */ @@read_rnd_buffer_size;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@read_rnd_buffer_size;").Check(testkit.Rows("262144"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("unique_checks", "1"))
	tk.MustQuery("SELECT /*+ SET_VAR(unique_checks=0) */ @@unique_checks;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@unique_checks;").Check(testkit.Rows("1"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("read_buffer_size", "131072"))
	tk.MustQuery("SELECT /*+ SET_VAR(read_buffer_size=8192) */ @@read_buffer_size;").Check(testkit.Rows("8192"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@read_buffer_size;").Check(testkit.Rows("131072"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("default_tmp_storage_engine", "InnoDB"))
	tk.MustQuery("SELECT /*+ SET_VAR(default_tmp_storage_engine='CSV') */ @@default_tmp_storage_engine;").Check(testkit.Rows("CSV"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@default_tmp_storage_engine;").Check(testkit.Rows("InnoDB"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("optimizer_search_depth", "62"))
	tk.MustQuery("SELECT /*+ SET_VAR(optimizer_search_depth=1) */ @@optimizer_search_depth;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@optimizer_search_depth;").Check(testkit.Rows("62"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("max_points_in_geometry", "65536"))
	tk.MustQuery("SELECT /*+ SET_VAR(max_points_in_geometry=3) */ @@max_points_in_geometry;").Check(testkit.Rows("3"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@max_points_in_geometry;").Check(testkit.Rows("65536"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("updatable_views_with_limit", "YES"))
	tk.MustQuery("SELECT /*+ SET_VAR(updatable_views_with_limit=0) */ @@updatable_views_with_limit;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@updatable_views_with_limit;").Check(testkit.Rows("YES"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("optimizer_prune_level", "1"))
	tk.MustQuery("SELECT /*+ SET_VAR(optimizer_prune_level=0) */ @@optimizer_prune_level;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@optimizer_prune_level;").Check(testkit.Rows("1"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("group_concat_max_len", "1024"))
	tk.MustQuery("SELECT /*+ SET_VAR(group_concat_max_len=4) */ @@group_concat_max_len;").Check(testkit.Rows("4"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@group_concat_max_len;").Check(testkit.Rows("1024"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("eq_range_index_dive_limit", "200"))
	tk.MustQuery("SELECT /*+ SET_VAR(eq_range_index_dive_limit=0) */ @@eq_range_index_dive_limit;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@eq_range_index_dive_limit;").Check(testkit.Rows("200"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("sql_safe_updates", "0"))
	tk.MustQuery("SELECT /*+ SET_VAR(sql_safe_updates=1) */ @@sql_safe_updates;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@sql_safe_updates;").Check(testkit.Rows("0"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("end_markers_in_json", "0"))
	tk.MustQuery("SELECT /*+ SET_VAR(end_markers_in_json=1) */ @@end_markers_in_json;").Check(testkit.Rows("1"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@end_markers_in_json;").Check(testkit.Rows("0"))

	require.NoError(t, tk.Session().GetSessionVars().SetSystemVar("windowing_use_high_precision", "ON"))
	tk.MustQuery("SELECT /*+ SET_VAR(windowing_use_high_precision=OFF) */ @@windowing_use_high_precision;").Check(testkit.Rows("0"))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	tk.MustQuery("SELECT @@windowing_use_high_precision;").Check(testkit.Rows("1"))

	tk.MustExec("SELECT /*+ SET_VAR(sql_safe_updates = 1) SET_VAR(max_heap_table_size = 1G) */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)

	tk.MustExec("SELECT /*+ SET_VAR(collation_server = 'utf8') */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "[planner:3637]Variable 'collation_server' cannot be set using SET_VAR hint.")

	tk.MustExec("SELECT /*+ SET_VAR(max_size = 1G) */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "[planner:3128]Unresolved name 'max_size' for SET_VAR hint")

	tk.MustExec("SELECT /*+ SET_VAR(group_concat_max_len = 1024) SET_VAR(group_concat_max_len = 2048) */ 1;")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, "[planner:3126]Hint SET_VAR(group_concat_max_len=2048) is ignored as conflicting/duplicated.")
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

func TestGetSysVariables(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test ScopeSession
	tk.MustExec("select @@warning_count")
	tk.MustExec("select @@session.warning_count")
	tk.MustExec("select @@local.warning_count")
	err := tk.ExecToErr("select @@global.warning_count")
	require.True(t, terror.ErrorEqual(err, variable.ErrIncorrectScope), fmt.Sprintf("err %v", err))

	// Test ScopeGlobal
	tk.MustExec("select @@max_connections")
	tk.MustExec("select @@global.max_connections")
	tk.MustGetErrMsg("select @@session.max_connections", "[variable:1238]Variable 'max_connections' is a GLOBAL variable")
	tk.MustGetErrMsg("select @@local.max_connections", "[variable:1238]Variable 'max_connections' is a GLOBAL variable")

	// Test ScopeNone
	tk.MustExec("select @@performance_schema_max_mutex_classes")
	tk.MustExec("select @@global.performance_schema_max_mutex_classes")
	// For issue 19524, test
	tk.MustExec("select @@session.performance_schema_max_mutex_classes")
	tk.MustExec("select @@local.performance_schema_max_mutex_classes")
	tk.MustGetErrMsg("select @@global.last_insert_id", "[variable:1238]Variable 'last_insert_id' is a SESSION variable")
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
		check func(*stmtctx.StmtHints)
	}

	hintChecks := []hintCheck{
		{
			hint: "MEMORY_QUOTA(1024 MB)",
			check: func(stmtHint *stmtctx.StmtHints) {
				require.True(t, stmtHint.HasMemQuotaHint)
				require.Equal(t, int64(1024*1024*1024), stmtHint.MemQuotaQuery)
			},
		},
		{
			hint: "READ_CONSISTENT_REPLICA()",
			check: func(stmtHint *stmtctx.StmtHints) {
				require.True(t, stmtHint.HasReplicaReadHint)
				require.Equal(t, byte(tikv.ReplicaReadFollower), stmtHint.ReplicaRead)
			},
		},
		{
			hint: "MAX_EXECUTION_TIME(1000)",
			check: func(stmtHint *stmtctx.StmtHints) {
				require.True(t, stmtHint.HasMaxExecutionTime)
				require.Equal(t, uint64(1000), stmtHint.MaxExecutionTime)
			},
		},
		{
			hint: "USE_TOJA(TRUE)",
			check: func(stmtHint *stmtctx.StmtHints) {
				require.True(t, stmtHint.HasAllowInSubqToJoinAndAggHint)
				require.True(t, stmtHint.AllowInSubqToJoinAndAgg)
			},
		},
		{
			hint: "RESOURCE_GROUP(rg1)",
			check: func(stmtHint *stmtctx.StmtHints) {
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
