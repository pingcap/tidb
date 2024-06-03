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
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestSQLSelectLimit(t *testing.T) {
	sv := GetSysVar(SQLSelectLimit)
	vars := NewSessionVars(nil)
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
	vars := NewSessionVars(nil)
	val, err := sv.Validate(vars, "strict_trans_tabLES  ", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "STRICT_TRANS_TABLES", val)

	_, err = sv.Validate(vars, "strict_trans_tabLES,nonsense_option", ScopeSession)
	require.Equal(t, "ERROR 1231 (42000): Variable 'sql_mode' can't be set to the value of 'NONSENSE_OPTION'", err.Error())

	val, err = sv.Validate(vars, "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION", val)

	require.Nil(t, sv.SetSessionFromHook(vars, val)) // sets to strict from above
	require.True(t, vars.SQLMode.HasStrictMode())

	sqlMode, err := mysql.GetSQLMode(val)
	require.NoError(t, err)
	require.Equal(t, sqlMode, vars.SQLMode)

	// Set it to non strict.
	val, err = sv.Validate(vars, "ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION", val)

	require.Nil(t, sv.SetSessionFromHook(vars, val)) // sets to non-strict from above
	require.False(t, vars.SQLMode.HasStrictMode())
	sqlMode, err = mysql.GetSQLMode(val)
	require.NoError(t, err)
	require.Equal(t, sqlMode, vars.SQLMode)
}

func TestMaxExecutionTime(t *testing.T) {
	sv := GetSysVar(MaxExecutionTime)
	vars := NewSessionVars(nil)

	val, err := sv.Validate(vars, "-10", ScopeSession)
	require.NoError(t, err) // it has autoconvert out of range.
	require.Equal(t, "0", val)

	val, err = sv.Validate(vars, "99999", ScopeSession)
	require.NoError(t, err) // it has autoconvert out of range.
	require.Equal(t, "99999", val)

	require.Nil(t, sv.SetSessionFromHook(vars, "99999")) // sets
	require.Equal(t, uint64(99999), vars.MaxExecutionTime)
}

func TestTiFlashMaxBytes(t *testing.T) {
	varNames := []string{TiDBMaxBytesBeforeTiFlashExternalJoin, TiDBMaxBytesBeforeTiFlashExternalGroupBy, TiDBMaxBytesBeforeTiFlashExternalSort}
	for index, varName := range varNames {
		sv := GetSysVar(varName)
		vars := NewSessionVars(nil)
		val, err := sv.Validate(vars, "-10", ScopeSession)
		require.NoError(t, err) // it has autoconvert out of range.
		require.Equal(t, "-1", val)
		val, err = sv.Validate(vars, "-10", ScopeGlobal)
		require.NoError(t, err) // it has autoconvert out of range.
		require.Equal(t, "-1", val)
		val, err = sv.Validate(vars, "100", ScopeSession)
		require.NoError(t, err)
		require.Equal(t, "100", val)

		_, err = sv.Validate(vars, strconv.FormatUint(uint64(math.MaxInt64)+1, 10), ScopeSession)
		// can not autoconvert because the input is out of the range of Int64
		require.Error(t, err)

		require.Nil(t, sv.SetSessionFromHook(vars, "10000")) // sets
		switch index {
		case 0:
			require.Equal(t, int64(10000), vars.TiFlashMaxBytesBeforeExternalJoin)
		case 1:
			require.Equal(t, int64(10000), vars.TiFlashMaxBytesBeforeExternalGroupBy)
		case 2:
			require.Equal(t, int64(10000), vars.TiFlashMaxBytesBeforeExternalSort)
		}
	}
}

func TestTiFlashMemQuotaQueryPerNode(t *testing.T) {
	// test TiFlash query memory threshold
	sv := GetSysVar(TiFlashMemQuotaQueryPerNode)
	vars := NewSessionVars(nil)
	val, err := sv.Validate(vars, "-10", ScopeSession)
	require.NoError(t, err) // it has been auto converted if out of range
	require.Equal(t, "-1", val)
	val, err = sv.Validate(vars, "-10", ScopeGlobal)
	require.NoError(t, err) // it has been auto converted if out of range
	require.Equal(t, "-1", val)
	val, err = sv.Validate(vars, "100", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "100", val)
	_, err = sv.Validate(vars, strconv.FormatUint(uint64(math.MaxInt64)+1, 10), ScopeSession)
	// can not autoconvert because the input is out of the range of Int64
	require.Error(t, err)
	require.Nil(t, sv.SetSessionFromHook(vars, "10000")) // sets
	require.Equal(t, int64(10000), vars.TiFlashMaxQueryMemoryPerNode)
}

func TestTiFlashQuerySpillRatio(t *testing.T) {
	// test TiFlash auto spill ratio
	sv := GetSysVar(TiFlashQuerySpillRatio)
	vars := NewSessionVars(nil)
	val, err := sv.Validate(vars, "-10", ScopeSession)
	require.NoError(t, err) // it has been auto converted if out of range
	require.Equal(t, "0", val)
	val, err = sv.Validate(vars, "-10", ScopeGlobal)
	require.NoError(t, err) // it has been auto converted if out of range
	require.Equal(t, "0", val)
	_, err = sv.Validate(vars, "100", ScopeSession)
	require.Error(t, err)
	_, err = sv.Validate(vars, "0.9", ScopeSession)
	require.Error(t, err)
	val, err = sv.Validate(vars, "0.85", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "0.85", val)
	require.Nil(t, sv.SetSessionFromHook(vars, "0.75")) // sets
	require.Equal(t, 0.75, vars.TiFlashQuerySpillRatio)
}

func TestCollationServer(t *testing.T) {
	sv := GetSysVar(CollationServer)
	vars := NewSessionVars(nil)

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

func TestDefaultCollationForUTF8MB4(t *testing.T) {
	sv := GetSysVar(DefaultCollationForUTF8MB4)
	vars := NewSessionVars(nil)

	// test normalization
	val, err := sv.Validate(vars, "utf8mb4_BIN", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "utf8mb4_bin", val)
	warn := vars.StmtCtx.GetWarnings()[0].Err
	require.Equal(t, "[variable:1681]Updating 'default_collation_for_utf8mb4' is deprecated. It will be made read-only in a future release.", warn.Error())
	val, err = sv.Validate(vars, "utf8mb4_GENeral_CI", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, "utf8mb4_general_ci", val)
	warn = vars.StmtCtx.GetWarnings()[0].Err
	require.Equal(t, "[variable:1681]Updating 'default_collation_for_utf8mb4' is deprecated. It will be made read-only in a future release.", warn.Error())
	val, err = sv.Validate(vars, "utf8mb4_0900_AI_CI", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "utf8mb4_0900_ai_ci", val)
	warn = vars.StmtCtx.GetWarnings()[0].Err
	require.Equal(t, "[variable:1681]Updating 'default_collation_for_utf8mb4' is deprecated. It will be made read-only in a future release.", warn.Error())
	// test set variable failed
	_, err = sv.Validate(vars, "LATIN1_bin", ScopeSession)
	require.EqualError(t, err, ErrInvalidDefaultUTF8MB4Collation.GenWithStackByArgs("latin1_bin").Error())
}

func TestTimeZone(t *testing.T) {
	sv := GetSysVar(TimeZone)
	vars := NewSessionVars(nil)

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
	tz, err := timeutil.ParseTimeZone("UTC")
	require.NoError(t, err)
	require.Equal(t, tz, vars.TimeZone)
}

func TestTxnIsolation(t *testing.T) {
	sv := GetSysVar(TxnIsolation)
	vars := NewSessionVars(nil)

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
	require.Nil(t, GetSysVar(TiDBSkipIsolationLevelCheck).SetGlobalFromHook(context.Background(), vars, "ON", true))
	_, err = sv.Validate(vars, "Serializable", ScopeSession)
	require.Equal(t, "[variable:8048]The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error", err.Error())

	// Enable session skip isolation check
	require.Nil(t, GetSysVar(TiDBSkipIsolationLevelCheck).SetSessionFromHook(vars, "ON"))

	val, err = sv.Validate(vars, "Serializable", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "SERIALIZABLE", val)

	// Init TiDBSkipIsolationLevelCheck like what loadCommonGlobalVariables does
	vars = NewSessionVars(nil)
	require.NoError(t, vars.SetSystemVarWithRelaxedValidation(TiDBSkipIsolationLevelCheck, "1"))
	val, err = sv.Validate(vars, "Serializable", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "SERIALIZABLE", val)
}

func TestTiDBMultiStatementMode(t *testing.T) {
	sv := GetSysVar(TiDBMultiStatementMode)
	vars := NewSessionVars(nil)

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
	vars := NewSessionVars(nil)
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
		require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), TiDBEnableNoopFuncs, "ON"))
		_, err = sv.Validate(vars, "on", ScopeGlobal)
		require.NoError(t, err)
		require.NoError(t, vars.GlobalVarsAccessor.SetGlobalSysVar(context.Background(), TiDBEnableNoopFuncs, "OFF"))
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
	vars := NewSessionVars(nil)
	val, err := vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBCurrentTS)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d", vars.TxnCtx.StartTS), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBLastTxnInfo)
	require.NoError(t, err)
	require.Equal(t, vars.LastTxnInfo, val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBLastQueryInfo)
	require.NoError(t, err)
	info, err := json.Marshal(vars.LastQueryInfo)
	require.NoError(t, err)
	require.Equal(t, string(info), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBFoundInPlanCache)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(vars.PrevFoundInPlanCache), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBFoundInBinding)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(vars.PrevFoundInBinding), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBTxnScope)
	require.NoError(t, err)
	require.Equal(t, vars.TxnScope.GetVarValue(), val)
}

func TestInstanceScopedVars(t *testing.T) {
	vars := NewSessionVars(nil)
	val, err := vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBGeneralLog)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(ProcessGeneralLog.Load()), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBPProfSQLCPU)
	require.NoError(t, err)
	expected := "0"
	if EnablePProfSQLCPU.Load() {
		expected = "1"
	}
	require.Equal(t, expected, val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBExpensiveQueryTimeThreshold)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d", atomic.LoadUint64(&ExpensiveQueryTimeThreshold)), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBExpensiveTxnTimeThreshold)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d", atomic.LoadUint64(&ExpensiveTxnTimeThreshold)), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBMemoryUsageAlarmRatio)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%g", MemoryUsageAlarmRatio.Load()), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBMemoryUsageAlarmKeepRecordNum)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d", MemoryUsageAlarmKeepRecordNum.Load()), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBForcePriority)
	require.NoError(t, err)
	require.Equal(t, mysql.Priority2Str[mysql.PriorityEnum(atomic.LoadInt32(&ForcePriority))], val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBDDLSlowOprThreshold)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(uint64(atomic.LoadUint32(&DDLSlowOprThreshold)), 10), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), PluginDir)
	require.NoError(t, err)
	require.Equal(t, config.GetGlobalConfig().Instance.PluginDir, val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), PluginLoad)
	require.NoError(t, err)
	require.Equal(t, config.GetGlobalConfig().Instance.PluginLoad, val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBSlowLogThreshold)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Instance.SlowThreshold), 10), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBRecordPlanInSlowLog)
	require.NoError(t, err)
	enabled := atomic.LoadUint32(&config.GetGlobalConfig().Instance.RecordPlanInSlowLog) == 1
	require.Equal(t, BoolToOnOff(enabled), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBEnableSlowLog)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(config.GetGlobalConfig().Instance.EnableSlowLog.Load()), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBCheckMb4ValueInUTF8)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(config.GetGlobalConfig().Instance.CheckMb4ValueInUTF8.Load()), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBEnableCollectExecutionInfo)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(config.GetGlobalConfig().Instance.EnableCollectExecutionInfo.Load()), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBConfig)
	require.NoError(t, err)
	expected, err = config.GetJSONConfig()
	require.NoError(t, err)
	require.Equal(t, expected, val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBLogFileMaxDays)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprint(GlobalLogMaxDays.Load()), val)

	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBRCReadCheckTS)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(EnableRCReadCheckTS.Load()), val)
}

func TestSecureAuth(t *testing.T) {
	sv := GetSysVar(SecureAuth)
	vars := NewSessionVars(nil)
	_, err := sv.Validate(vars, "OFF", ScopeGlobal)
	require.Equal(t, "[variable:1231]Variable 'secure_auth' can't be set to the value of 'OFF'", err.Error())
	val, err := sv.Validate(vars, "ON", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, "ON", val)
}

func TestTiDBReplicaRead(t *testing.T) {
	sv := GetSysVar(TiDBReplicaRead)
	vars := NewSessionVars(nil)
	val, err := sv.Validate(vars, "follower", ScopeGlobal)
	require.Equal(t, val, "follower")
	require.NoError(t, err)
}

func TestSQLAutoIsNull(t *testing.T) {
	svSQL, svNoop := GetSysVar(SQLAutoIsNull), GetSysVar(TiDBEnableNoopFuncs)
	vars := NewSessionVars(nil)
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
	vars := NewSessionVars(nil)
	val, err := vars.GetSessionOrGlobalSystemVar(context.Background(), LastInsertID)
	require.NoError(t, err)
	require.Equal(t, val, "0")

	vars.StmtCtx.PrevLastInsertID = 21
	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), LastInsertID)
	require.NoError(t, err)
	require.Equal(t, val, "21")

	vars.StmtCtx.PrevLastInsertID = 9223372036854775809
	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), LastInsertID)
	require.NoError(t, err)
	require.Equal(t, val, "9223372036854775809")

	f := GetSysVar("last_insert_id")
	d, valType, flag := f.GetNativeValType(val)
	require.Equal(t, valType, mysql.TypeLonglong)
	require.Equal(t, flag, mysql.BinaryFlag|mysql.UnsignedFlag)
	require.Equal(t, d.GetUint64(), uint64(9223372036854775809))
}

func TestTimestamp(t *testing.T) {
	vars := NewSessionVars(nil)
	val, err := vars.GetSessionOrGlobalSystemVar(context.Background(), Timestamp)
	require.NoError(t, err)
	require.NotEqual(t, "", val)

	vars.systems[Timestamp] = "10"
	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), Timestamp)
	require.NoError(t, err)
	require.Equal(t, "10", val)

	vars.systems[Timestamp] = "0" // set to default
	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), Timestamp)
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
	vars := NewSessionVars(nil)
	val, err := vars.GetSessionOrGlobalSystemVar(context.Background(), Identity)
	require.NoError(t, err)
	require.Equal(t, val, "0")

	vars.StmtCtx.PrevLastInsertID = 21
	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), Identity)
	require.NoError(t, err)
	require.Equal(t, val, "21")
}

func TestLcTimeNamesReadOnly(t *testing.T) {
	sv := GetSysVar("lc_time_names")
	vars := NewSessionVars(nil)
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	_, err := sv.Validate(vars, "newvalue", ScopeGlobal)
	require.Error(t, err)
}

func TestLcMessages(t *testing.T) {
	sv := GetSysVar("lc_messages")
	vars := NewSessionVars(nil)
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	_, err := sv.Validate(vars, "zh_CN", ScopeGlobal)
	require.NoError(t, err)
	err = sv.SetSessionFromHook(vars, "zh_CN")
	require.NoError(t, err)
	val, err := vars.GetSessionOrGlobalSystemVar(context.Background(), "lc_messages")
	require.NoError(t, err)
	require.Equal(t, val, "zh_CN")
}

func TestDDLWorkers(t *testing.T) {
	svWorkerCount, svBatchSize := GetSysVar(TiDBDDLReorgWorkerCount), GetSysVar(TiDBDDLReorgBatchSize)
	vars := NewSessionVars(nil)
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
	vars := NewSessionVars(nil)
	val, err := vars.GetSessionOrGlobalSystemVar(context.Background(), CharacterSetConnection)
	require.NoError(t, err)
	require.Equal(t, val, mysql.DefaultCharset)
	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), CollationConnection)
	require.NoError(t, err)
	require.Equal(t, val, mysql.DefaultCollationName)
}

func TestIndexMergeSwitcher(t *testing.T) {
	vars := NewSessionVars(nil)
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	val, err := vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBEnableIndexMerge)
	require.NoError(t, err)
	require.Equal(t, DefTiDBEnableIndexMerge, true)
	require.Equal(t, BoolToOnOff(DefTiDBEnableIndexMerge), val)
}

func TestNetBufferLength(t *testing.T) {
	netBufferLength := GetSysVar(NetBufferLength)
	vars := NewSessionVars(nil)
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
	vars := NewSessionVars(nil)
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
	vars := NewSessionVars(nil)

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
	vars := NewSessionVars(nil)

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
	vars := NewSessionVars(nil)

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
	vars := NewSessionVars(nil)

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
	vars := NewSessionVars(nil)
	val, err := vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBMemoryDebugModeMinHeapInUse)
	require.NoError(t, err)
	require.Equal(t, val, "0")
	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBMemoryDebugModeAlarmRatio)
	require.NoError(t, err)
	require.Equal(t, val, "0")
}

func TestSetTIDBDistributeReorg(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	// Set to on
	err := mock.SetGlobalSysVar(context.Background(), TiDBEnableDistTask, On)
	require.NoError(t, err)
	val, err := mock.GetGlobalSysVar(TiDBEnableDistTask)
	require.NoError(t, err)
	require.Equal(t, On, val)

	// Set to off
	err = mock.SetGlobalSysVar(context.Background(), TiDBEnableDistTask, Off)
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBEnableDistTask)
	require.NoError(t, err)
	require.Equal(t, Off, val)
}

func TestDefaultPartitionPruneMode(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	val, err := vars.GetSessionOrGlobalSystemVar(context.Background(), TiDBPartitionPruneMode)
	require.NoError(t, err)
	require.Equal(t, "dynamic", val)
	require.Equal(t, "dynamic", DefTiDBPartitionPruneMode)
}

func TestSetTIDBFastDDL(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	fastDDL := GetSysVar(TiDBDDLEnableFastReorg)

	// Default true
	require.Equal(t, fastDDL.Value, On)

	// Set to On
	err := mock.SetGlobalSysVar(context.Background(), TiDBDDLEnableFastReorg, On)
	require.NoError(t, err)
	val, err1 := mock.GetGlobalSysVar(TiDBDDLEnableFastReorg)
	require.NoError(t, err1)
	require.Equal(t, On, val)

	// Set to off
	err = mock.SetGlobalSysVar(context.Background(), TiDBDDLEnableFastReorg, Off)
	require.NoError(t, err)
	val, err1 = mock.GetGlobalSysVar(TiDBDDLEnableFastReorg)
	require.NoError(t, err1)
	require.Equal(t, Off, val)
}

func TestSetTIDBDiskQuota(t *testing.T) {
	vars := NewSessionVars(nil)
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
	err = mock.SetGlobalSysVar(context.Background(), TiDBDDLDiskQuota, strconv.FormatInt(50*gb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBDDLDiskQuota)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(100*gb, 10), val)

	// Set to 100 GB
	err = mock.SetGlobalSysVar(context.Background(), TiDBDDLDiskQuota, strconv.FormatInt(100*gb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBDDLDiskQuota)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(100*gb, 10), val)

	// Set to 200 GB
	err = mock.SetGlobalSysVar(context.Background(), TiDBDDLDiskQuota, strconv.FormatInt(200*gb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBDDLDiskQuota)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(200*gb, 10), val)

	// Set to 1 Pb
	err = mock.SetGlobalSysVar(context.Background(), TiDBDDLDiskQuota, strconv.FormatInt(pb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBDDLDiskQuota)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(pb, 10), val)

	// MaxValue is 1 PB, set to 2 Pb is not allowed, it will set back to 1 PB max allowed value.
	err = mock.SetGlobalSysVar(context.Background(), TiDBDDLDiskQuota, strconv.FormatInt(2*pb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBDDLDiskQuota)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(pb, 10), val)
}

func TestTiDBServerMemoryLimit(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	var (
		mb  uint64 = 1 << 20
		err error
		val string
	)
	// Test tidb_server_memory_limit
	serverMemoryLimit := GetSysVar(TiDBServerMemoryLimit)
	// Check default value
	require.Equal(t, serverMemoryLimit.Value, DefTiDBServerMemoryLimit)

	// MinValue is 512 MB
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, strconv.FormatUint(100*mb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
	require.NoError(t, err)
	require.Equal(t, "512MB", val)

	// Test Close
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, strconv.FormatUint(0, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
	require.NoError(t, err)
	require.Equal(t, "0", val)

	// Test MaxValue
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, strconv.FormatUint(math.MaxUint64, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(math.MaxUint64, 10), val)

	// Test Normal Value
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, strconv.FormatUint(1024*mb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(1024*mb, 10), val)

	// Test tidb_server_memory_limit_sess_min_size
	serverMemoryLimitSessMinSize := GetSysVar(TiDBServerMemoryLimitSessMinSize)
	// Check default value
	require.Equal(t, serverMemoryLimitSessMinSize.Value, strconv.FormatUint(DefTiDBServerMemoryLimitSessMinSize, 10))

	// MinValue is 128 Bytes
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitSessMinSize, strconv.FormatUint(100, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimitSessMinSize)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(128, 10), val)

	// Test Close
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitSessMinSize, strconv.FormatUint(0, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimitSessMinSize)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(0, 10), val)

	// Test MaxValue
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitSessMinSize, strconv.FormatUint(math.MaxUint64, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimitSessMinSize)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(math.MaxUint64, 10), val)

	// Test Normal Value
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitSessMinSize, strconv.FormatUint(200*mb, 10))
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimitSessMinSize)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(200*mb, 10), val)
}

func TestTiDBServerMemoryLimit2(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	var (
		err error
		val string
	)
	// Test tidb_server_memory_limit
	serverMemoryLimit := GetSysVar(TiDBServerMemoryLimit)
	// Check default value
	require.Equal(t, serverMemoryLimit.Value, DefTiDBServerMemoryLimit)

	total := memory.GetMemTotalIgnoreErr()
	if total > 0 {
		// Can use percentage format when TiDB can obtain physical memory
		// Test Percentage Format
		err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "1%")
		require.NoError(t, err)
		val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
		require.NoError(t, err)
		if total/100 > uint64(512<<20) {
			require.Equal(t, memory.ServerMemoryLimit.Load(), total/100)
			require.Equal(t, "1%", val)
		} else {
			require.Equal(t, memory.ServerMemoryLimit.Load(), uint64(512<<20))
			require.Equal(t, "512MB", val)
		}

		err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "0%")
		require.Error(t, err)
		err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "100%")
		require.Error(t, err)

		err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "75%")
		require.NoError(t, err)
		val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
		require.NoError(t, err)
		require.Equal(t, "75%", val)
		require.Equal(t, memory.ServerMemoryLimit.Load(), total/100*75)
	}
	// Test can't obtain physical memory
	require.Nil(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/memory/GetMemTotalError", `return(true)`))
	require.Error(t, mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "75%"))
	require.Nil(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/memory/GetMemTotalError"))

	// Test byteSize format
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "1234")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
	require.NoError(t, err)
	require.Equal(t, memory.ServerMemoryLimit.Load(), uint64(512<<20))
	require.Equal(t, "512MB", val)

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "1234567890123")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
	require.NoError(t, err)
	require.Equal(t, memory.ServerMemoryLimit.Load(), uint64(1234567890123))
	require.Equal(t, "1234567890123", val)

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "10KB")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
	require.NoError(t, err)
	require.Equal(t, memory.ServerMemoryLimit.Load(), uint64(512<<20))
	require.Equal(t, "512MB", val)

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "12345678KB")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
	require.NoError(t, err)
	require.Equal(t, memory.ServerMemoryLimit.Load(), uint64(12345678<<10))
	require.Equal(t, "12345678KB", val)

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "10MB")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
	require.NoError(t, err)
	require.Equal(t, memory.ServerMemoryLimit.Load(), uint64(512<<20))
	require.Equal(t, "512MB", val)

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "700MB")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
	require.NoError(t, err)
	require.Equal(t, memory.ServerMemoryLimit.Load(), uint64(700<<20))
	require.Equal(t, "700MB", val)

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "20GB")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
	require.NoError(t, err)
	require.Equal(t, memory.ServerMemoryLimit.Load(), uint64(20<<30))
	require.Equal(t, "20GB", val)

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "2TB")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimit)
	require.NoError(t, err)
	require.Equal(t, memory.ServerMemoryLimit.Load(), uint64(2<<40))
	require.Equal(t, "2TB", val)

	// Test error
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "123aaa123")
	require.Error(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "700MBaa")
	require.Error(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimit, "a700MB")
	require.Error(t, err)
}

func TestTiDBServerMemoryLimitSessMinSize(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	var (
		err error
		val string
	)

	serverMemroyLimitSessMinSize := GetSysVar(TiDBServerMemoryLimitSessMinSize)
	// Check default value
	require.Equal(t, serverMemroyLimitSessMinSize.Value, strconv.FormatInt(DefTiDBServerMemoryLimitSessMinSize, 10))

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitSessMinSize, "123456")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimitSessMinSize)
	require.NoError(t, err)
	require.Equal(t, memory.ServerMemoryLimitSessMinSize.Load(), uint64(123456))
	require.Equal(t, "123456", val)

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitSessMinSize, "100")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimitSessMinSize)
	require.NoError(t, err)
	require.Equal(t, memory.ServerMemoryLimitSessMinSize.Load(), uint64(128))
	require.Equal(t, "128", val)

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitSessMinSize, "123MB")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimitSessMinSize)
	require.NoError(t, err)
	require.Equal(t, memory.ServerMemoryLimitSessMinSize.Load(), uint64(123<<20))
	require.Equal(t, "128974848", val)
}

func TestTiDBServerMemoryLimitGCTrigger(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	var (
		err error
		val string
	)

	serverMemroyLimitGCTrigger := GetSysVar(TiDBServerMemoryLimitGCTrigger)
	// Check default value
	require.Equal(t, serverMemroyLimitGCTrigger.Value, strconv.FormatFloat(DefTiDBServerMemoryLimitGCTrigger, 'f', -1, 64))
	defer func() {
		err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitGCTrigger, strconv.FormatFloat(DefTiDBServerMemoryLimitGCTrigger, 'f', -1, 64))
		require.NoError(t, err)
	}()

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitGCTrigger, "0.8")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimitGCTrigger)
	require.NoError(t, err)
	require.Equal(t, gctuner.GlobalMemoryLimitTuner.GetPercentage(), 0.8)
	require.Equal(t, "0.8", val)

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitGCTrigger, "90%")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBServerMemoryLimitGCTrigger)
	require.NoError(t, err)
	require.Equal(t, gctuner.GlobalMemoryLimitTuner.GetPercentage(), 0.9)
	require.Equal(t, "0.9", val)

	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitGCTrigger, "100%")
	require.Error(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitGCTrigger, "101%")
	require.Error(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitGCTrigger, "99%")
	require.NoError(t, err)

	err = mock.SetGlobalSysVar(context.Background(), TiDBGOGCTunerThreshold, "0.4")
	require.NoError(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitGCTrigger, "49%")
	require.Error(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiDBServerMemoryLimitGCTrigger, "51%")
	require.NoError(t, err)

	err = mock.SetGlobalSysVar(context.Background(), TiDBGOGCTunerMaxValue, "50")
	require.Error(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiDBGOGCTunerMinValue, "200")
	require.NoError(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiDBGOGCTunerMinValue, "1000")
	require.Error(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiDBGOGCTunerMinValue, "100")
	require.NoError(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiDBGOGCTunerMaxValue, "200")
	require.NoError(t, err)
}

func TestSetAggPushDownGlobally(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	val, err := mock.GetGlobalSysVar(TiDBOptAggPushDown)
	require.NoError(t, err)
	require.Equal(t, "OFF", val)
	err = mock.SetGlobalSysVar(context.Background(), TiDBOptAggPushDown, "ON")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBOptAggPushDown)
	require.NoError(t, err)
	require.Equal(t, "ON", val)
}

func TestSetDeriveTopNGlobally(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	val, err := mock.GetGlobalSysVar(TiDBOptDeriveTopN)
	require.NoError(t, err)
	require.Equal(t, "OFF", val)
	err = mock.SetGlobalSysVar(context.Background(), TiDBOptDeriveTopN, "ON")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBOptDeriveTopN)
	require.NoError(t, err)
	require.Equal(t, "ON", val)
}

func TestSetJobScheduleWindow(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	// default value
	val, err := mock.GetGlobalSysVar(TiDBTTLJobScheduleWindowStartTime)
	require.NoError(t, err)
	require.Equal(t, "00:00 +0000", val)

	// set and get variable in UTC
	vars.TimeZone = time.UTC
	err = mock.SetGlobalSysVar(context.Background(), TiDBTTLJobScheduleWindowStartTime, "16:11")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBTTLJobScheduleWindowStartTime)
	require.NoError(t, err)
	require.Equal(t, "16:11 +0000", val)

	// set variable in UTC, get it in Asia/Shanghai
	vars.TimeZone = time.UTC
	err = mock.SetGlobalSysVar(context.Background(), TiDBTTLJobScheduleWindowStartTime, "16:11")
	require.NoError(t, err)
	vars.TimeZone, err = time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBTTLJobScheduleWindowStartTime)
	require.NoError(t, err)
	require.Equal(t, "16:11 +0000", val)

	// set variable in Asia/Shanghai, get it it UTC
	vars.TimeZone, err = time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiDBTTLJobScheduleWindowStartTime, "16:11")
	require.NoError(t, err)
	vars.TimeZone = time.UTC
	val, err = mock.GetGlobalSysVar(TiDBTTLJobScheduleWindowStartTime)
	require.NoError(t, err)
	require.Equal(t, "16:11 +0800", val)
}

func TestTiDBIgnoreInlistPlanDigest(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	initValue, err := mock.GetGlobalSysVar(TiDBIgnoreInlistPlanDigest)
	require.NoError(t, err)
	require.Equal(t, initValue, Off)
	// Set to On(init at start)
	err1 := mock.SetGlobalSysVar(context.Background(), TiDBIgnoreInlistPlanDigest, On)
	require.NoError(t, err1)
	NewVal, err2 := mock.GetGlobalSysVar(TiDBIgnoreInlistPlanDigest)
	require.NoError(t, err2)
	require.Equal(t, NewVal, On)
}

func TestTiDBEnableResourceControl(t *testing.T) {
	// setup the hooks for test
	// NOTE: the default system variable is true but the switch is false
	// It is initialized at the first call of `rebuildSysVarCache`
	enable := false
	EnableGlobalResourceControlFunc = func() { enable = true }
	DisableGlobalResourceControlFunc = func() { enable = false }
	setGlobalResourceControlFunc := func(enable bool) {
		if enable {
			EnableGlobalResourceControlFunc()
		} else {
			DisableGlobalResourceControlFunc()
		}
	}
	SetGlobalResourceControl.Store(&setGlobalResourceControlFunc)
	// Reset the switch. It may be set by other tests.
	EnableResourceControl.Store(false)

	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	resourceControlEnabled := GetSysVar(TiDBEnableResourceControl)

	// Default true
	require.Equal(t, resourceControlEnabled.Value, On)
	require.Equal(t, enable, false)

	// Set to On(init at start)
	err := mock.SetGlobalSysVar(context.Background(), TiDBEnableResourceControl, On)
	require.NoError(t, err)
	val, err1 := mock.GetGlobalSysVar(TiDBEnableResourceControl)
	require.NoError(t, err1)
	require.Equal(t, On, val)
	require.Equal(t, enable, true)

	// Set to Off
	err = mock.SetGlobalSysVar(context.Background(), TiDBEnableResourceControl, Off)
	require.NoError(t, err)
	val, err1 = mock.GetGlobalSysVar(TiDBEnableResourceControl)
	require.NoError(t, err1)
	require.Equal(t, Off, val)
	require.Equal(t, enable, false)

	// Set to On again
	err = mock.SetGlobalSysVar(context.Background(), TiDBEnableResourceControl, On)
	require.NoError(t, err)
	val, err1 = mock.GetGlobalSysVar(TiDBEnableResourceControl)
	require.NoError(t, err1)
	require.Equal(t, On, val)
	require.Equal(t, enable, true)
}

func TestTiDBEnableRowLevelChecksum(t *testing.T) {
	ctx := context.Background()
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	// default to false
	val, err := mock.GetGlobalSysVar(TiDBEnableRowLevelChecksum)
	require.NoError(t, err)
	require.Equal(t, Off, val)

	// enable
	err = mock.SetGlobalSysVar(ctx, TiDBEnableRowLevelChecksum, On)
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBEnableRowLevelChecksum)
	require.NoError(t, err)
	require.Equal(t, On, val)

	// disable
	err = mock.SetGlobalSysVar(ctx, TiDBEnableRowLevelChecksum, Off)
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBEnableRowLevelChecksum)
	require.NoError(t, err)
	require.Equal(t, Off, val)
}

func TestTiDBAutoAnalyzeRatio(t *testing.T) {
	ctx := context.Background()
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	// default to 0.5
	val, err := mock.GetGlobalSysVar(TiDBAutoAnalyzeRatio)
	require.NoError(t, err)
	require.Equal(t, "0.5", val)

	// set to 0.1
	err = mock.SetGlobalSysVar(ctx, TiDBAutoAnalyzeRatio, "0.1")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBAutoAnalyzeRatio)
	require.NoError(t, err)
	require.Equal(t, "0.1", val)

	// set to 1.1
	err = mock.SetGlobalSysVar(ctx, TiDBAutoAnalyzeRatio, "1.1")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBAutoAnalyzeRatio)
	require.NoError(t, err)
	require.Equal(t, "1.1", val)

	// set to 0
	err = mock.SetGlobalSysVar(ctx, TiDBAutoAnalyzeRatio, "0")
	require.Error(t, err)
	val, err = mock.GetGlobalSysVar(TiDBAutoAnalyzeRatio)
	require.NoError(t, err)
	require.Equal(t, "1.1", val)

	// set to 0.0000000001
	err = mock.SetGlobalSysVar(ctx, TiDBAutoAnalyzeRatio, "0.0000000001")
	require.Error(t, err)
	val, err = mock.GetGlobalSysVar(TiDBAutoAnalyzeRatio)
	require.NoError(t, err)
	require.Equal(t, "1.1", val)

	// set to 0.00001
	err = mock.SetGlobalSysVar(ctx, TiDBAutoAnalyzeRatio, "0.00001")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBAutoAnalyzeRatio)
	require.NoError(t, err)
	require.Equal(t, "0.00001", val)

	// set to 0.000009999
	err = mock.SetGlobalSysVar(ctx, TiDBAutoAnalyzeRatio, "0.000009999")
	require.Error(t, err)
	val, err = mock.GetGlobalSysVar(TiDBAutoAnalyzeRatio)
	require.NoError(t, err)
	require.Equal(t, "0.00001", val)
}

func TestTiDBTiFlashReplicaRead(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	tidbTiFlashReplicaRead := GetSysVar(TiFlashReplicaRead)
	// Check default value
	require.Equal(t, DefTiFlashReplicaRead, tidbTiFlashReplicaRead.Value)

	err := mock.SetGlobalSysVar(context.Background(), TiFlashReplicaRead, "all_replicas")
	require.NoError(t, err)
	val, err := mock.GetGlobalSysVar(TiFlashReplicaRead)
	require.NoError(t, err)
	require.Equal(t, "all_replicas", val)

	err = mock.SetGlobalSysVar(context.Background(), TiFlashReplicaRead, "closest_adaptive")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiFlashReplicaRead)
	require.NoError(t, err)
	require.Equal(t, "closest_adaptive", val)

	err = mock.SetGlobalSysVar(context.Background(), TiFlashReplicaRead, "closest_replicas")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiFlashReplicaRead)
	require.NoError(t, err)
	require.Equal(t, "closest_replicas", val)

	err = mock.SetGlobalSysVar(context.Background(), TiFlashReplicaRead, DefTiFlashReplicaRead)
	require.NoError(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiFlashReplicaRead, "random")
	require.Error(t, err)
	val, err = mock.GetGlobalSysVar(TiFlashReplicaRead)
	require.NoError(t, err)
	require.Equal(t, DefTiFlashReplicaRead, val)
}

func TestSetTiDBCloudStorageURI(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	cloudStorageURI := GetSysVar(TiDBCloudStorageURI)
	require.Len(t, CloudStorageURI.Load(), 0)
	defer func() {
		CloudStorageURI.Store("")
	}()

	// Default empty
	require.Len(t, cloudStorageURI.Value, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Set to noop
	noopURI := "noop://blackhole?access-key=hello&secret-access-key=world"
	err := mock.SetGlobalSysVar(ctx, TiDBCloudStorageURI, noopURI)
	require.NoError(t, err)
	val, err1 := mock.SessionVars.GetSessionOrGlobalSystemVar(ctx, TiDBCloudStorageURI)
	require.NoError(t, err1)
	require.Equal(t, noopURI, val)
	require.Equal(t, noopURI, CloudStorageURI.Load())

	// Set to s3, should fail
	err = mock.SetGlobalSysVar(ctx, TiDBCloudStorageURI, "s3://blackhole")
	require.Error(t, err, "unreachable storage URI")

	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	defer s.Close()

	// Set to s3, should return uri without variable
	s3URI := "s3://tiflow-test/?access-key=testid&secret-access-key=testkey8&session-token=testtoken&endpoint=" + s.URL
	err = mock.SetGlobalSysVar(ctx, TiDBCloudStorageURI, s3URI)
	require.NoError(t, err)
	val, err1 = mock.SessionVars.GetSessionOrGlobalSystemVar(ctx, TiDBCloudStorageURI)
	require.NoError(t, err1)
	require.True(t, strings.HasPrefix(val, "s3://tiflow-test/"))
	require.Contains(t, val, "access-key=xxxxxx")
	require.Contains(t, val, "secret-access-key=xxxxxx")
	require.Contains(t, val, "session-token=xxxxxx")
	require.Equal(t, s3URI, CloudStorageURI.Load())

	// ks3 is like s3
	ks3URI := "ks3://tiflow-test/?region=test&access-key=testid&secret-access-key=testkey8&session-token=testtoken&endpoint=" + s.URL
	err = mock.SetGlobalSysVar(ctx, TiDBCloudStorageURI, ks3URI)
	require.NoError(t, err)
	val, err1 = mock.SessionVars.GetSessionOrGlobalSystemVar(ctx, TiDBCloudStorageURI)
	require.NoError(t, err1)
	require.True(t, strings.HasPrefix(val, "ks3://tiflow-test/"))
	require.Contains(t, val, "access-key=xxxxxx")
	require.Contains(t, val, "secret-access-key=xxxxxx")
	require.Contains(t, val, "session-token=xxxxxx")
	require.Equal(t, ks3URI, CloudStorageURI.Load())

	// Set to empty, should return no error
	err = mock.SetGlobalSysVar(ctx, TiDBCloudStorageURI, "")
	require.NoError(t, err)
	val, err1 = mock.SessionVars.GetSessionOrGlobalSystemVar(ctx, TiDBCloudStorageURI)
	require.NoError(t, err1)
	require.Len(t, val, 0)
	cancel()
}

func TestGlobalSystemVariableInitialValue(t *testing.T) {
	vars := []struct {
		name    string
		val     string
		initVal string
	}{
		{
			TiDBTxnMode,
			DefTiDBTxnMode,
			"pessimistic",
		},
		{
			TiDBEnableAsyncCommit,
			BoolToOnOff(DefTiDBEnableAsyncCommit),
			BoolToOnOff(DefTiDBEnableAsyncCommit),
		},
		{
			TiDBEnable1PC,
			BoolToOnOff(DefTiDBEnable1PC),
			BoolToOnOff(DefTiDBEnable1PC),
		},
		{
			TiDBMemOOMAction,
			DefTiDBMemOOMAction,
			OOMActionLog,
		},
		{
			TiDBEnableAutoAnalyze,
			BoolToOnOff(DefTiDBEnableAutoAnalyze),
			Off,
		},
		{
			TiDBRowFormatVersion,
			strconv.Itoa(DefTiDBRowFormatV1),
			strconv.Itoa(DefTiDBRowFormatV2),
		},
		{
			TiDBTxnAssertionLevel,
			DefTiDBTxnAssertionLevel,
			AssertionFastStr,
		},
		{
			TiDBEnableMutationChecker,
			BoolToOnOff(DefTiDBEnableMutationChecker),
			On,
		},
		{
			TiDBPessimisticTransactionFairLocking,
			BoolToOnOff(DefTiDBPessimisticTransactionFairLocking),
			On,
		},
	}
	for _, v := range vars {
		initVal := GlobalSystemVariableInitialValue(v.name, v.val)
		require.Equal(t, v.initVal, initVal)
	}
}

func TestTiDBOptTxnAutoRetry(t *testing.T) {
	sv := GetSysVar(TiDBDisableTxnAutoRetry)
	vars := NewSessionVars(nil)

	for _, scope := range []ScopeFlag{ScopeSession, ScopeGlobal} {
		val, err := sv.Validate(vars, "OFF", scope)
		require.NoError(t, err)
		require.Equal(t, "ON", val)
		warn := vars.StmtCtx.GetWarnings()[0].Err
		require.Equal(t, "[variable:1287]'OFF' is deprecated and will be removed in a future release. Please use ON instead", warn.Error())
	}
}

func TestTiDBLowResTSOUpdateInterval(t *testing.T) {
	sv := GetSysVar(TiDBLowResolutionTSOUpdateInterval)
	vars := NewSessionVars(nil)

	// Too low, will get raised to the min value
	val, err := sv.Validate(vars, "0", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(GetSysVar(TiDBLowResolutionTSOUpdateInterval).MinValue, 10), val)
	warn := vars.StmtCtx.GetWarnings()[0].Err
	require.Equal(t, "[variable:1292]Truncated incorrect tidb_low_resolution_tso_update_interval value: '0'", warn.Error())

	// Too high, will get lowered to the max value
	val, err = sv.Validate(vars, "100000", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(GetSysVar(TiDBLowResolutionTSOUpdateInterval).MaxValue, 10), val)
	warn = vars.StmtCtx.GetWarnings()[1].Err
	require.Equal(t, "[variable:1292]Truncated incorrect tidb_low_resolution_tso_update_interval value: '100000'", warn.Error())

	// valid
	val, err = sv.Validate(vars, "1000", ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, "1000", val)
}

func TestSetEnableColumnTrackingAndPersistAnalyzeOptions(t *testing.T) {
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	// Test EnableColumnTracking
	val, err := mock.GetGlobalSysVar(TiDBEnableColumnTracking)
	require.NoError(t, err)
	require.Equal(t, Off, val)
	err = mock.SetGlobalSysVar(context.Background(), TiDBEnableColumnTracking, On)
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBEnableColumnTracking)
	require.NoError(t, err)
	require.Equal(t, On, val)
	// Reset back.
	err = mock.SetGlobalSysVar(context.Background(), TiDBEnableColumnTracking, Off)
	require.NoError(t, err)

	// Test PersistAnalyzeOptions
	val, err = mock.GetGlobalSysVar(TiDBPersistAnalyzeOptions)
	require.NoError(t, err)
	require.Equal(t, On, val)
	err = mock.SetGlobalSysVar(context.Background(), TiDBPersistAnalyzeOptions, Off)
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBPersistAnalyzeOptions)
	require.NoError(t, err)
	require.Equal(t, Off, val)
	// Reset back
	err = mock.SetGlobalSysVar(context.Background(), TiDBPersistAnalyzeOptions, On)
	require.NoError(t, err)

	// Set EnableColumnTracking to true when PersistAnalyzeOptions is false
	// Set to false first.
	err = mock.SetGlobalSysVar(context.Background(), TiDBEnableColumnTracking, Off)
	require.NoError(t, err)
	err = mock.SetGlobalSysVar(context.Background(), TiDBPersistAnalyzeOptions, Off)
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBPersistAnalyzeOptions)
	require.NoError(t, err)
	require.Equal(t, Off, val)
	err = mock.SetGlobalSysVar(context.Background(), TiDBEnableColumnTracking, On)
	require.Error(t, err, "enable column tracking requires to persist analyze options")
	val, err = mock.GetGlobalSysVar(TiDBEnableColumnTracking)
	require.NoError(t, err)
	require.Equal(t, Off, val)

	// Set PersistAnalyzeOptions to false when EnableColumnTracking is true
	// Set to true first.
	err = mock.SetGlobalSysVar(context.Background(), TiDBPersistAnalyzeOptions, On)
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBPersistAnalyzeOptions)
	require.NoError(t, err)
	require.Equal(t, On, val)
	err = mock.SetGlobalSysVar(context.Background(), TiDBEnableColumnTracking, On)
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(TiDBEnableColumnTracking)
	require.NoError(t, err)
	require.Equal(t, On, val)
	err = mock.SetGlobalSysVar(context.Background(), TiDBPersistAnalyzeOptions, Off)
	require.Error(t, err, "persist analyze options requires to enable column tracking")
	val, err = mock.GetGlobalSysVar(TiDBPersistAnalyzeOptions)
	require.NoError(t, err)
	require.Equal(t, On, val)
}
