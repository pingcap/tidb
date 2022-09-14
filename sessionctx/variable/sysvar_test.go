// Copyright 2015 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/stretchr/testify/require"
)

func TestSQLSelectLimit(t *testing.T) {
	sv := GetSysVar(SQLSelectLimit)
	vars := NewSessionVars()
	val, err := sv.Validate(vars, "-10", ScopeSession)
	require.NoError(t, err) // it has autoconvert out of range.
	require.Equal(t, "0", val)

	val, err = sv.Validate(vars, "9999", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "9999", val)

	require.Nil(t, sv.SetSessionFromHook(vars, "9999")) // sets
	require.Equal(t, uint64(9999), vars.SelectLimit)
}

func TestSQLModeVar(t *testing.T) {
	sv := GetSysVar(SQLModeVar)
	vars := NewSessionVars()
	val, err := sv.Validate(vars, "strict_trans_tabLES  ", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "STRICT_TRANS_TABLES", val)

	_, err = sv.Validate(vars, "strict_trans_tabLES,nonsense_option", ScopeSession)
	require.Equal(t, "ERROR 1231 (42000): Variable 'sql_mode' can't be set to the value of 'NONSENSE_OPTION'", err.Error())

	val, err = sv.Validate(vars, "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION", val)

	require.Nil(t, sv.SetSessionFromHook(vars, val)) // sets to strict from above
	require.True(t, vars.StrictSQLMode)

	sqlMode, err := mysql.GetSQLMode(val)
	require.NoError(t, err)
	require.Equal(t, sqlMode, vars.SQLMode)

	// Set it to non strict.
	val, err = sv.Validate(vars, "ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION", val)

	require.Nil(t, sv.SetSessionFromHook(vars, val)) // sets to non-strict from above
	require.False(t, vars.StrictSQLMode)
	sqlMode, err = mysql.GetSQLMode(val)
	require.NoError(t, err)
	require.Equal(t, sqlMode, vars.SQLMode)
}

func TestMaxExecutionTime(t *testing.T) {
	sv := GetSysVar(MaxExecutionTime)
	vars := NewSessionVars()

	val, err := sv.Validate(vars, "-10", ScopeSession)
	require.NoError(t, err) // it has autoconvert out of range.
	require.Equal(t, "0", val)

	val, err = sv.Validate(vars, "99999", ScopeSession)
	require.NoError(t, err) // it has autoconvert out of range.
	require.Equal(t, "99999", val)

	require.Nil(t, sv.SetSessionFromHook(vars, "99999")) // sets
	require.Equal(t, uint64(99999), vars.MaxExecutionTime)
}

func TestCollationServer(t *testing.T) {
	sv := GetSysVar(CollationServer)
	vars := NewSessionVars()

	val, err := sv.Validate(vars, "LATIN1_bin", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "latin1_bin", val) // test normalization

	_, err = sv.Validate(vars, "BOGUSCOLLation", ScopeSession)
	require.Equal(t, "[ddl:1273]Unknown collation: 'BOGUSCOLLation'", err.Error())

	require.Nil(t, sv.SetSessionFromHook(vars, "latin1_bin"))
	require.Equal(t, "latin1", vars.systems[CharacterSetServer]) // check it also changes charset.

	require.Nil(t, sv.SetSessionFromHook(vars, "utf8mb4_bin"))
	require.Equal(t, "utf8mb4", vars.systems[CharacterSetServer]) // check it also changes charset.
}

func TestTimeZone(t *testing.T) {
	sv := GetSysVar(TimeZone)
	vars := NewSessionVars()

	// TiDB uses the Golang TZ library, so TZs are case-sensitive.
	// Unfortunately this is not strictly MySQL compatible. i.e.
	// This should not fail:
	// val, err := sv.Validate(vars, "America/EDMONTON", ScopeSession)
	// See: https://github.com/pingcap/tidb/issues/8087

	val, err := sv.Validate(vars, "America/Edmonton", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "America/Edmonton", val)

	val, err = sv.Validate(vars, "+10:00", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "+10:00", val)

	val, err = sv.Validate(vars, "UTC", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "UTC", val)

	val, err = sv.Validate(vars, "+00:00", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "+00:00", val)

	require.Nil(t, sv.SetSessionFromHook(vars, "UTC")) // sets
	tz, err := parseTimeZone("UTC")
	require.NoError(t, err)
	require.Equal(t, tz, vars.TimeZone)
}

func TestTxnIsolation(t *testing.T) {
	sv := GetSysVar(TxnIsolation)
	vars := NewSessionVars()

	_, err := sv.Validate(vars, "on", ScopeSession)
	require.Equal(t, "[variable:1231]Variable 'tx_isolation' can't be set to the value of 'on'", err.Error())

	val, err := sv.Validate(vars, "read-COMMitted", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "READ-COMMITTED", val)

	_, err = sv.Validate(vars, "Serializable", ScopeSession)
	require.Equal(t, "[variable:8048]The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error", err.Error())

	_, err = sv.Validate(vars, "read-uncommitted", ScopeSession)
	require.Equal(t, "[variable:8048]The isolation level 'READ-UNCOMMITTED' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error", err.Error())

	// Enable global skip isolation check doesn't affect current session
	require.Nil(t, GetSysVar(TiDBSkipIsolationLevelCheck).SetGlobalFromHook(vars, "ON", true))
	_, err = sv.Validate(vars, "Serializable", ScopeSession)
	require.Equal(t, "[variable:8048]The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error", err.Error())

	// Enable session skip isolation check
	require.Nil(t, GetSysVar(TiDBSkipIsolationLevelCheck).SetSessionFromHook(vars, "ON"))

	val, err = sv.Validate(vars, "Serializable", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "SERIALIZABLE", val)

	// Init TiDBSkipIsolationLevelCheck like what loadCommonGlobalVariables does
	vars = NewSessionVars()
	require.NoError(t, vars.SetSystemVarWithRelaxedValidation(TiDBSkipIsolationLevelCheck, "1"))
	val, err = sv.Validate(vars, "Serializable", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "SERIALIZABLE", val)
}

func TestTiDBMultiStatementMode(t *testing.T) {
	sv := GetSysVar(TiDBMultiStatementMode)
	vars := NewSessionVars()

	val, err := sv.Validate(vars, "on", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "ON", val)
	require.Nil(t, sv.SetSessionFromHook(vars, val))
	require.Equal(t, 1, vars.MultiStatementMode)

	val, err = sv.Validate(vars, "0", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "OFF", val)
	require.Nil(t, sv.SetSessionFromHook(vars, val))
	require.Equal(t, 0, vars.MultiStatementMode)

	val, err = sv.Validate(vars, "Warn", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "WARN", val)
	require.Nil(t, sv.SetSessionFromHook(vars, val))
	require.Equal(t, 2, vars.MultiStatementMode)
}

func TestReadOnlyNoop(t *testing.T) {
	vars := NewSessionVars()
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	noopFuncs := GetSysVar(TiDBEnableNoopFuncs)

	// For session scope
	for _, name := range []string{TxReadOnly, TransactionReadOnly} {
		sv := GetSysVar(name)
		val, err := sv.Validate(vars, "on", ScopeSession)
		require.Equal(t, "[variable:1235]function READ ONLY has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions", err.Error())
		require.Equal(t, "OFF", val)

		require.NoError(t, noopFuncs.SetSessionFromHook(vars, "ON"))
		_, err = sv.Validate(vars, "on", ScopeSession)
		require.NoError(t, err)
		require.NoError(t, noopFuncs.SetSessionFromHook(vars, "OFF")) // restore default.
	}

	// For global scope
	for _, name := range []string{TxReadOnly, TransactionReadOnly, OfflineMode, SuperReadOnly, ReadOnly} {
		sv := GetSysVar(name)
		val, err := sv.Validate(vars, "on", ScopeGlobal)
		if name == OfflineMode {
			require.Equal(t, "[variable:1235]function OFFLINE MODE has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions", err.Error())
		} else {
			require.Equal(t, "[variable:1235]function READ ONLY has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions", err.Error())
		}
		require.Equal(t, "OFF", val)
		require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(TiDBEnableNoopFuncs, "ON"))
		_, err = sv.Validate(vars, "on", ScopeGlobal)
		require.NoError(t, err)
		require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(TiDBEnableNoopFuncs, "OFF"))
	}
}

func TestSkipInit(t *testing.T) {
	sv := SysVar{Scope: ScopeGlobal, Name: "skipinit1", Value: On, Type: TypeBool}
	require.True(t, sv.SkipInit())

	sv = SysVar{Scope: ScopeGlobal | ScopeSession, Name: "skipinit1", Value: On, Type: TypeBool}
	require.False(t, sv.SkipInit())

	sv = SysVar{Scope: ScopeSession, Name: "skipinit1", Value: On, Type: TypeBool}
	require.False(t, sv.SkipInit())

	sv = SysVar{Scope: ScopeSession, Name: "skipinit1", Value: On, Type: TypeBool, skipInit: true}
	require.True(t, sv.SkipInit())
}

func TestSessionGetterFuncs(t *testing.T) {
	vars := NewSessionVars()
	val, err := vars.GetSessionOrGlobalSystemVar(TiDBCurrentTS)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d", vars.TxnCtx.StartTS), val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBLastTxnInfo)
	require.NoError(t, err)
	require.Equal(t, vars.LastTxnInfo, val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBLastQueryInfo)
	require.NoError(t, err)
	info, err := json.Marshal(vars.LastQueryInfo)
	require.NoError(t, err)
	require.Equal(t, string(info), val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBFoundInPlanCache)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(vars.PrevFoundInPlanCache), val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBFoundInBinding)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(vars.PrevFoundInBinding), val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBTxnScope)
	require.NoError(t, err)
	require.Equal(t, vars.TxnScope.GetVarValue(), val)
}

func TestInstanceScopedVars(t *testing.T) {
	vars := NewSessionVars()
	val, err := vars.GetSessionOrGlobalSystemVar(TiDBGeneralLog)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(ProcessGeneralLog.Load()), val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBPProfSQLCPU)
	require.NoError(t, err)
	expected := "0"
	if EnablePProfSQLCPU.Load() {
		expected = "1"
	}
	require.Equal(t, expected, val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBExpensiveQueryTimeThreshold)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d", atomic.LoadUint64(&ExpensiveQueryTimeThreshold)), val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBMemoryUsageAlarmRatio)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%g", MemoryUsageAlarmRatio.Load()), val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBForcePriority)
	require.NoError(t, err)
	require.Equal(t, mysql.Priority2Str[mysql.PriorityEnum(atomic.LoadInt32(&ForcePriority))], val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBDDLSlowOprThreshold)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(uint64(atomic.LoadUint32(&DDLSlowOprThreshold)), 10), val)

	val, err = vars.GetSessionOrGlobalSystemVar(PluginDir)
	require.NoError(t, err)
	require.Equal(t, config.GetGlobalConfig().Instance.PluginDir, val)

	val, err = vars.GetSessionOrGlobalSystemVar(PluginLoad)
	require.NoError(t, err)
	require.Equal(t, config.GetGlobalConfig().Instance.PluginLoad, val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBSlowLogThreshold)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Instance.SlowThreshold), 10), val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBRecordPlanInSlowLog)
	require.NoError(t, err)
	enabled := atomic.LoadUint32(&config.GetGlobalConfig().Instance.RecordPlanInSlowLog) == 1
	require.Equal(t, BoolToOnOff(enabled), val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBEnableSlowLog)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(config.GetGlobalConfig().Instance.EnableSlowLog.Load()), val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBCheckMb4ValueInUTF8)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load()), val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBEnableCollectExecutionInfo)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(config.GetGlobalConfig().Instance.EnableCollectExecutionInfo), val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBConfig)
	require.NoError(t, err)
	expected, err = config.GetJSONConfig()
	require.NoError(t, err)
	require.Equal(t, expected, val)

	val, err = vars.GetSessionOrGlobalSystemVar(TiDBLogFileMaxDays)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprint(GlobalLogMaxDays.Load()), val)
}

func TestSecureAuth(t *testing.T) {
	sv := GetSysVar(SecureAuth)
	vars := NewSessionVars()
	_, err := sv.Validate(vars, "OFF", ScopeGlobal)
	require.Equal(t, "[variable:1231]Variable 'secure_auth' can't be set to the value of 'OFF'", err.Error())
	val, err := sv.Validate(vars, "ON", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, "ON", val)
}

func TestTiDBReplicaRead(t *testing.T) {
	sv := GetSysVar(TiDBReplicaRead)
	vars := NewSessionVars()
	val, err := sv.Validate(vars, "follower", ScopeGlobal)
	require.Equal(t, val, "follower")
	require.NoError(t, err)
}

func TestSQLAutoIsNull(t *testing.T) {
	svSQL, svNoop := GetSysVar(SQLAutoIsNull), GetSysVar(TiDBEnableNoopFuncs)
	vars := NewSessionVars()
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	_, err := svSQL.Validate(vars, "ON", ScopeSession)
	require.True(t, terror.ErrorEqual(err, ErrFunctionsNoopImpl))
	// change tidb_enable_noop_functions to 1, it will success
	require.NoError(t, svNoop.SetSessionFromHook(vars, "ON"))
	_, err = svSQL.Validate(vars, "ON", ScopeSession)
	require.NoError(t, err)
	require.NoError(t, svSQL.SetSessionFromHook(vars, "ON"))
	res, ok := vars.GetSystemVar(SQLAutoIsNull)
	require.True(t, ok)
	require.Equal(t, "ON", res)
	// restore tidb_enable_noop_functions to 0 failed, as sql_auto_is_null is 1
	_, err = svNoop.Validate(vars, "OFF", ScopeSession)
	require.True(t, terror.ErrorEqual(err, errValueNotSupportedWhen))
	// after set sql_auto_is_null to 0, restore success
	require.NoError(t, svSQL.SetSessionFromHook(vars, "OFF"))
	require.NoError(t, svNoop.SetSessionFromHook(vars, "OFF"))

	// Only test validate as MockGlobalAccessor do not support SetGlobalSysVar
	_, err = svSQL.Validate(vars, "ON", ScopeGlobal)
	require.True(t, terror.ErrorEqual(err, ErrFunctionsNoopImpl))
}

func TestLastInsertID(t *testing.T) {
	vars := NewSessionVars()
	val, err := vars.GetSessionOrGlobalSystemVar(LastInsertID)
	require.NoError(t, err)
	require.Equal(t, val, "0")

	vars.StmtCtx.PrevLastInsertID = 21
	val, err = vars.GetSessionOrGlobalSystemVar(LastInsertID)
	require.NoError(t, err)
	require.Equal(t, val, "21")
}

func TestTimestamp(t *testing.T) {
	vars := NewSessionVars()
	val, err := vars.GetSessionOrGlobalSystemVar(Timestamp)
	require.NoError(t, err)
	require.NotEqual(t, "", val)

	vars.systems[Timestamp] = "10"
	val, err = vars.GetSessionOrGlobalSystemVar(Timestamp)
	require.NoError(t, err)
	require.Equal(t, "10", val)

	vars.systems[Timestamp] = "0" // set to default
	val, err = vars.GetSessionOrGlobalSystemVar(Timestamp)
	require.NoError(t, err)
	require.NotEqual(t, "", val)
	require.NotEqual(t, "10", val)

	// Test validating a value that less than the minimum one.
	sv := GetSysVar(Timestamp)
	_, err = sv.Validate(vars, "-5", ScopeSession)
	require.NoError(t, err)
	warn := vars.StmtCtx.GetWarnings()[0].Err
	require.Equal(t, "[variable:1292]Truncated incorrect timestamp value: '-5'", warn.Error())

	// Test validating values that larger than the maximum one.
	_, err = sv.Validate(vars, "3147483698", ScopeSession)
	require.Equal(t, "[variable:1231]Variable 'timestamp' can't be set to the value of '3147483698'", err.Error())

	_, err = sv.Validate(vars, "2147483648", ScopeSession)
	require.Equal(t, "[variable:1231]Variable 'timestamp' can't be set to the value of '2147483648'", err.Error())

	// Test validating the maximum value.
	_, err = sv.Validate(vars, "2147483647", ScopeSession)
	require.NoError(t, err)
}

func TestIdentity(t *testing.T) {
	vars := NewSessionVars()
	val, err := vars.GetSessionOrGlobalSystemVar(Identity)
	require.NoError(t, err)
	require.Equal(t, val, "0")

	vars.StmtCtx.PrevLastInsertID = 21
	val, err = vars.GetSessionOrGlobalSystemVar(Identity)
	require.NoError(t, err)
	require.Equal(t, val, "21")
}

func TestLcTimeNamesReadOnly(t *testing.T) {
	sv := GetSysVar("lc_time_names")
	vars := NewSessionVars()
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	_, err := sv.Validate(vars, "newvalue", ScopeGlobal)
	require.Error(t, err)
}

func TestLcMessagesReadOnly(t *testing.T) {
	sv := GetSysVar("lc_messages")
	vars := NewSessionVars()
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	_, err := sv.Validate(vars, "newvalue", ScopeGlobal)
	require.Error(t, err)
}

func TestDDLWorkers(t *testing.T) {
	svWorkerCount, svBatchSize := GetSysVar(TiDBDDLReorgWorkerCount), GetSysVar(TiDBDDLReorgBatchSize)
	vars := NewSessionVars()
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()

	val, err := svWorkerCount.Validate(vars, "-100", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, val, "1") // converts it to min value
	val, err = svWorkerCount.Validate(vars, "1234", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, val, "256") // converts it to max value
	val, err = svWorkerCount.Validate(vars, "100", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, val, "100") // unchanged

	val, err = svBatchSize.Validate(vars, "10", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, val, fmt.Sprint(MinDDLReorgBatchSize)) // converts it to min value
	val, err = svBatchSize.Validate(vars, "999999", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, val, fmt.Sprint(MaxDDLReorgBatchSize)) // converts it to max value
	val, err = svBatchSize.Validate(vars, "100", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, val, "100") // unchanged
}

func TestDefaultCharsetAndCollation(t *testing.T) {
	vars := NewSessionVars()
	val, err := vars.GetSessionOrGlobalSystemVar(CharacterSetConnection)
	require.NoError(t, err)
	require.Equal(t, val, mysql.DefaultCharset)
	val, err = vars.GetSessionOrGlobalSystemVar(CollationConnection)
	require.NoError(t, err)
	require.Equal(t, val, mysql.DefaultCollationName)
}

func TestIndexMergeSwitcher(t *testing.T) {
	vars := NewSessionVars()
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	val, err := vars.GetSessionOrGlobalSystemVar(TiDBEnableIndexMerge)
	require.NoError(t, err)
	require.Equal(t, DefTiDBEnableIndexMerge, true)
	require.Equal(t, BoolToOnOff(DefTiDBEnableIndexMerge), val)
}

func TestNetBufferLength(t *testing.T) {
	netBufferLength := GetSysVar(NetBufferLength)
	vars := NewSessionVars()
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()

	val, err := netBufferLength.Validate(vars, "1", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, "1024", val) // converts it to min value
	val, err = netBufferLength.Validate(vars, "10485760", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, "1048576", val) // converts it to max value
	val, err = netBufferLength.Validate(vars, "524288", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, "524288", val) // unchanged
}

func TestTiDBBatchPendingTiFlashCount(t *testing.T) {
	sv := GetSysVar(TiDBBatchPendingTiFlashCount)
	vars := NewSessionVars()
	val, err := sv.Validate(vars, "-10", ScopeSession)
	require.NoError(t, err) // it has autoconvert out of range.
	require.Equal(t, "0", val)

	val, err = sv.Validate(vars, "9999", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "9999", val)

	_, err = sv.Validate(vars, "1.5", ScopeSession)
	require.Error(t, err)
	require.EqualError(t, err, "[variable:1232]Incorrect argument type to variable 'tidb_batch_pending_tiflash_count'")
}

func TestTiDBMemQuotaQuery(t *testing.T) {
	sv := GetSysVar(TiDBMemQuotaQuery)
	vars := NewSessionVars()

	for _, scope := range []ScopeFlag{ScopeGlobal, ScopeSession} {
		newVal := 32 * 1024 * 1024
		val, err := sv.Validate(vars, fmt.Sprintf("%d", newVal), scope)
		require.Equal(t, val, "33554432")
		require.NoError(t, err)

		// min value out of range
		newVal = -2
		expected := -1
		val, err = sv.Validate(vars, fmt.Sprintf("%d", newVal), scope)
		// expected to truncate
		require.Equal(t, val, fmt.Sprintf("%d", expected))
		require.NoError(t, err)
	}
}

func TestTiDBQueryLogMaxLen(t *testing.T) {
	sv := GetSysVar(TiDBQueryLogMaxLen)
	vars := NewSessionVars()

	newVal := 32 * 1024 * 1024
	val, err := sv.Validate(vars, fmt.Sprintf("%d", newVal), ScopeGlobal)
	require.Equal(t, val, "33554432")
	require.NoError(t, err)

	// out of range
	newVal = 1073741825
	expected := 1073741824
	val, err = sv.Validate(vars, fmt.Sprintf("%d", newVal), ScopeGlobal)
	// expected to truncate
	require.Equal(t, val, fmt.Sprintf("%d", expected))
	require.NoError(t, err)

	// min value out of range
	newVal = -2
	expected = 0
	val, err = sv.Validate(vars, fmt.Sprintf("%d", newVal), ScopeGlobal)
	// expected to set to min value
	require.Equal(t, val, fmt.Sprintf("%d", expected))
	require.NoError(t, err)
}

func TestTiDBCommitterConcurrency(t *testing.T) {
	sv := GetSysVar(TiDBCommitterConcurrency)
	vars := NewSessionVars()

	newVal := 1024
	val, err := sv.Validate(vars, fmt.Sprintf("%d", newVal), ScopeGlobal)
	require.Equal(t, val, "1024")
	require.NoError(t, err)

	// out of range
	newVal = 10001
	expected := 10000
	val, err = sv.Validate(vars, fmt.Sprintf("%d", newVal), ScopeGlobal)
	// expected to truncate
	require.Equal(t, val, fmt.Sprintf("%d", expected))
	require.NoError(t, err)

	// min value out of range
	newVal = 0
	expected = 1
	val, err = sv.Validate(vars, fmt.Sprintf("%d", newVal), ScopeGlobal)
	// expected to set to min value
	require.Equal(t, val, fmt.Sprintf("%d", expected))
	require.NoError(t, err)
}

func TestTiDBDDLFlashbackConcurrency(t *testing.T) {
	sv := GetSysVar(TiDBDDLFlashbackConcurrency)
	vars := NewSessionVars()

	newVal := 128
	val, err := sv.Validate(vars, fmt.Sprintf("%d", newVal), ScopeGlobal)
	require.Equal(t, val, "128")
	require.NoError(t, err)

	// out of range
	newVal = MaxConfigurableConcurrency + 1
	expected := MaxConfigurableConcurrency
	val, err = sv.Validate(vars, fmt.Sprintf("%d", newVal), ScopeGlobal)
	// expected to truncate
	require.Equal(t, val, fmt.Sprintf("%d", expected))
	require.NoError(t, err)

	// min value out of range
	newVal = 0
	expected = 1
	val, err = sv.Validate(vars, fmt.Sprintf("%d", newVal), ScopeGlobal)
	// expected to set to min value
	require.Equal(t, val, fmt.Sprintf("%d", expected))
	require.NoError(t, err)
}

func TestDefaultMemoryDebugModeValue(t *testing.T) {
	vars := NewSessionVars()
	val, err := vars.GetSessionOrGlobalSystemVar(TiDBMemoryDebugModeMinHeapInUse)
	require.NoError(t, err)
	require.Equal(t, val, "0")
	val, err = vars.GetSessionOrGlobalSystemVar(TiDBMemoryDebugModeAlarmRatio)
	require.NoError(t, err)
	require.Equal(t, val, "0")
}

func TestSetTIDBFastDDL(t *testing.T) {
	vars := NewSessionVars()
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	fastDDL := GetSysVar(TiDBDDLEnableFastReorg)

	// Default off
	require.Equal(t, fastDDL.Value, Off)

	// Set to On
	err := mock.SetGlobalSysVar(TiDBDDLEnableFastReorg, On)
	require.NoError(t, err)
	val, err1 := mock.GetGlobalSysVar(TiDBDDLEnableFastReorg)
	require.NoError(t, err1)
	require.Equal(t, On, val)

	// Set to off
	err = mock.SetGlobalSysVar(TiDBDDLEnableFastReorg, Off)
	require.NoError(t, err)
	val, err1 = mock.GetGlobalSysVar(TiDBDDLEnableFastReorg)
	require.NoError(t, err1)
	require.Equal(t, Off, val)
}

func TestSetTIDBDiskQuota(t *testing.T) {
	vars := NewSessionVars()
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	diskQuota := GetSysVar(TiDBDDLDiskQuota)
	var (
		gb  int64 = 1024 * 1024 * 1024
		pb  int64 = 1024 * 1024 * 1024 * 1024 * 1024
		err error
		val string
	)
	// Default 100 GB
	require.Equal(t, diskQuota.Value, strconv.FormatInt(100*gb, 10))

	// MinValue is 100 GB, set to 50 Gb is not allowed
	err = mock.SetGlobalSysVar(TiDBDDLDiskQuota, strconv.FormatInt(50*gb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBDDLDiskQuota)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(100*gb, 10), val)

	// Set to 100 GB
	err = mock.SetGlobalSysVar(TiDBDDLDiskQuota, strconv.FormatInt(100*gb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBDDLDiskQuota)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(100*gb, 10), val)

	// Set to 200 GB
	err = mock.SetGlobalSysVar(TiDBDDLDiskQuota, strconv.FormatInt(200*gb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBDDLDiskQuota)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(200*gb, 10), val)

	// Set to 1 Pb
	err = mock.SetGlobalSysVar(TiDBDDLDiskQuota, strconv.FormatInt(pb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBDDLDiskQuota)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(pb, 10), val)

	// MaxValue is 1 PB, set to 2 Pb is not allowed, it will set back to 1 PB max allowed value.
	err = mock.SetGlobalSysVar(TiDBDDLDiskQuota, strconv.FormatInt(2*pb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBDDLDiskQuota)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(pb, 10), val)
}
