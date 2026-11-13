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
	require.ErrorContains(t, err, "bucket blackhole")

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

func TestEnableWindowFunction(t *testing.T) {
	vars := NewSessionVars(nil)
	require.Equal(t, vars.EnableWindowFunction, DefEnableWindowFunction)
	require.NoError(t, vars.SetSystemVar(TiDBEnableWindowFunction, "on"))
	require.Equal(t, vars.EnableWindowFunction, true)
	require.NoError(t, vars.SetSystemVar(TiDBEnableWindowFunction, "0"))
	require.Equal(t, vars.EnableWindowFunction, false)
	require.NoError(t, vars.SetSystemVar(TiDBEnableWindowFunction, "1"))
	require.Equal(t, vars.EnableWindowFunction, true)
}
<<<<<<< HEAD
=======

func TestTiDBHashJoinVersion(t *testing.T) {
	vars := NewSessionVars(nil)
	// test the default value
	require.Equal(t, joinversion.IsOptimizedVersion(vardef.DefTiDBHashJoinVersion), vars.UseHashJoinV2)
	sv := GetSysVar(vardef.TiDBHashJoinVersion)
	// set error value
	_, err := sv.Validation(vars, "invalid", "invalid", vardef.ScopeSession)
	require.NotNil(t, err)
	// set valid value
	_, err = sv.Validation(vars, "legacy", "legacy", vardef.ScopeSession)
	require.NoError(t, err)
	_, err = sv.Validation(vars, "optimized", "optimized", vardef.ScopeSession)
	require.NoError(t, err)
	_, err = sv.Validation(vars, "Legacy", "Legacy", vardef.ScopeSession)
	require.NoError(t, err)
	_, err = sv.Validation(vars, "Optimized", "Optimized", vardef.ScopeSession)
	require.NoError(t, err)
	_, err = sv.Validation(vars, "LegaCy", "LegaCy", vardef.ScopeSession)
	require.NoError(t, err)
	_, err = sv.Validation(vars, "OptimiZed", "OptimiZed", vardef.ScopeSession)
	require.NoError(t, err)
}

func TestTiDBEnableFullOuterJoin(t *testing.T) {
	vars := NewSessionVars(nil)
	require.Equal(t, vardef.DefTiDBEnableFullOuterJoin, vars.EnableFullOuterJoin)
	require.NoError(t, vars.SetSystemVar(vardef.TiDBEnableFullOuterJoin, "on"))
	require.True(t, vars.EnableFullOuterJoin)
	require.NoError(t, vars.SetSystemVar(vardef.TiDBEnableFullOuterJoin, "0"))
	require.False(t, vars.EnableFullOuterJoin)
	require.NoError(t, vars.SetSystemVar(vardef.TiDBEnableFullOuterJoin, "1"))
	require.True(t, vars.EnableFullOuterJoin)
}

func TestTiDBMViewEnable(t *testing.T) {
	vars := NewSessionVars(nil)
	require.Equal(t, vardef.DefTiDBMViewEnable, vars.EnableMView)
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMViewEnable, "on"))
	require.True(t, vars.EnableMView)
	require.NoError(t, vars.SetSystemVar(vardef.TiDBMViewEnable, "off"))
	require.False(t, vars.EnableMView)
}

func TestTiDBAutoAnalyzeConcurrencyValidation(t *testing.T) {
	vars := NewSessionVars(nil)
	sysVar := GetSysVar(vardef.TiDBAutoAnalyzeConcurrency)
	require.NotNil(t, sysVar)
	require.Equal(t, "3", sysVar.Value)

	tests := []struct {
		name                string
		autoAnalyze         bool
		autoAnalyzePriority bool
		input               string
		expectError         bool
	}{
		{
			name:                "Auto analyze disabled",
			autoAnalyze:         false,
			autoAnalyzePriority: true,
			input:               "10",
			expectError:         true,
		},
		{
			name:                "Auto analyze priority queue disabled",
			autoAnalyze:         true,
			autoAnalyzePriority: false,
			input:               "10",
			expectError:         true,
		},
		{
			name:                "Both disabled",
			autoAnalyze:         false,
			autoAnalyzePriority: false,
			input:               "10",
			expectError:         true,
		},
		// Last so it ends as its defaults
		{
			name:                "Both enabled, valid input",
			autoAnalyze:         true,
			autoAnalyzePriority: true,
			input:               "10",
			expectError:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vardef.RunAutoAnalyze.Store(tt.autoAnalyze)
			vardef.EnableAutoAnalyzePriorityQueue.Store(tt.autoAnalyzePriority)

			_, err := sysVar.Validate(vars, tt.input, vardef.ScopeGlobal)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTiDBAnalyzeDefaultBucketAndTopNOptions(t *testing.T) {
	ctx := context.Background()
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	origBuckets := vardef.AnalyzeDefaultNumBuckets.Load()
	origTopN := vardef.AnalyzeDefaultNumTopN.Load()
	defer func() {
		vardef.AnalyzeDefaultNumBuckets.Store(origBuckets)
		vardef.AnalyzeDefaultNumTopN.Store(origTopN)
	}()

	bucketsVar := GetSysVar(vardef.TiDBAnalyzeDefaultNumBuckets)
	require.NotNil(t, bucketsVar)
	require.NoError(t, bucketsVar.SetGlobalFromHook(ctx, vars, "100", false))
	require.Equal(t, uint64(100), vardef.AnalyzeDefaultNumBuckets.Load())

	val, err := bucketsVar.GetGlobal(ctx, vars)
	require.NoError(t, err)
	require.Equal(t, "100", val)

	// Unsigned sysvar validation normalizes out-of-range values to the configured
	// boundary instead of returning an error.
	val, err = bucketsVar.Validate(vars, "0", vardef.ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(vardef.MinTiDBAnalyzeDefaultNumBuckets, 10), val)

	val, err = bucketsVar.Validate(vars, "100001", vardef.ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(vardef.MaxTiDBAnalyzeDefaultNumBuckets, 10), val)

	topNVar := GetSysVar(vardef.TiDBAnalyzeDefaultNumTopN)
	require.NotNil(t, topNVar)
	require.NoError(t, topNVar.SetGlobalFromHook(ctx, vars, "50", false))
	require.Equal(t, uint64(50), vardef.AnalyzeDefaultNumTopN.Load())

	val, err = topNVar.GetGlobal(ctx, vars)
	require.NoError(t, err)
	require.Equal(t, "50", val)

	val, err = topNVar.Validate(vars, "0", vardef.ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatInt(vardef.MinTiDBAnalyzeDefaultNumTopN, 10), val)

	val, err = topNVar.Validate(vars, "100001", vardef.ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(vardef.MaxTiDBAnalyzeDefaultNumTopN, 10), val)
}

func TestTiDBAnalyzeStoreBatchSize(t *testing.T) {
	vars := NewSessionVars(nil)
	require.Equal(t, vardef.DefTiDBAnalyzeStoreBatchSize, vars.AnalyzeStoreBatchSize)

	sysVar := GetSysVar(vardef.TiDBAnalyzeStoreBatchSize)
	require.NotNil(t, sysVar)
	require.True(t, sysVar.HasGlobalScope())
	require.True(t, sysVar.HasSessionScope())
	require.Equal(t, strconv.Itoa(vardef.DefTiDBAnalyzeStoreBatchSize), sysVar.Value)

	require.NoError(t, vars.SetSystemVar(vardef.TiDBAnalyzeStoreBatchSize, "0"))
	require.Zero(t, vars.AnalyzeStoreBatchSize)

	require.NoError(t, vars.SetSystemVar(vardef.TiDBAnalyzeStoreBatchSize, strconv.FormatUint(vardef.MaxTiDBAnalyzeStoreBatchSize+1, 10)))
	require.Equal(t, int(vardef.MaxTiDBAnalyzeStoreBatchSize), vars.AnalyzeStoreBatchSize)
}

func TestTiDBOptSelectivityFactor(t *testing.T) {
	ctx := context.Background()
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock
	val, err := vars.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptSelectivityFactor)
	require.NoError(t, err)
	require.NotEqual(t, 0.8, val)

	// set valid value
	require.NoError(t, vars.SetSystemVar(vardef.TiDBOptSelectivityFactor, "0.7"))
	val, err = vars.GetSessionOrGlobalSystemVar(context.Background(), vardef.TiDBOptSelectivityFactor)
	require.NoError(t, err)
	require.NotEqual(t, 0.7, val)

	// set invalid value
	err = mock.SetGlobalSysVar(ctx, vardef.TiDBOptSelectivityFactor, "1.1")
	require.NoError(t, err)
	_, err = mock.GetGlobalSysVar(vardef.TiDBOptSelectivityFactor)
	require.NoError(t, err)
	warn := vars.StmtCtx.GetWarnings()[0].Err
	require.Equal(t, "[variable:1292]Truncated incorrect tidb_opt_selectivity_factor value: '1.1'", warn.Error())
}

func TestSynonyms(t *testing.T) {
	sysVar := GetSysVar(vardef.TxnIsolation)
	require.NotNil(t, sysVar)

	vars := NewSessionVars(nil)

	// It does not permit SERIALIZABLE by default.
	_, err := sysVar.Validate(vars, "SERIALIZABLE", vardef.ScopeSession)
	require.Error(t, err)
	require.Equal(t, "[variable:8048]The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error", err.Error())

	// Enable Skip isolation check
	require.Nil(t, GetSysVar(vardef.TiDBSkipIsolationLevelCheck).SetSessionFromHook(vars, "ON"))

	// Serializable is now permitted.
	_, err = sysVar.Validate(vars, "SERIALIZABLE", vardef.ScopeSession)
	require.NoError(t, err)

	// Currently TiDB returns a warning because of SERIALIZABLE, but in future
	// it may also return a warning because TxnIsolation is deprecated.

	warn := vars.StmtCtx.GetWarnings()[0].Err
	require.Equal(t, "[variable:8048]The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error", warn.Error())

	require.Nil(t, sysVar.SetSessionFromHook(vars, "SERIALIZABLE"))

	// When we set TxnIsolation, it also updates TransactionIsolation.
	require.Equal(t, "SERIALIZABLE", vars.systems[vardef.TxnIsolation])
	require.Equal(t, vars.systems[vardef.TxnIsolation], vars.systems[vardef.TransactionIsolation])
}

func TestScope(t *testing.T) {
	sv := SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "mynewsysvar", Value: vardef.On, Type: vardef.TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	require.True(t, sv.HasSessionScope())
	require.True(t, sv.HasGlobalScope())
	require.False(t, sv.HasInstanceScope())
	require.False(t, sv.HasNoneScope())

	sv = SysVar{Scope: vardef.ScopeGlobal, Name: "mynewsysvar", Value: vardef.On, Type: vardef.TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	require.False(t, sv.HasSessionScope())
	require.True(t, sv.HasGlobalScope())
	require.False(t, sv.HasInstanceScope())
	require.False(t, sv.HasNoneScope())

	sv = SysVar{Scope: vardef.ScopeSession, Name: "mynewsysvar", Value: vardef.On, Type: vardef.TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	require.True(t, sv.HasSessionScope())
	require.False(t, sv.HasGlobalScope())
	require.False(t, sv.HasInstanceScope())
	require.False(t, sv.HasNoneScope())

	sv = SysVar{Scope: vardef.ScopeNone, Name: "mynewsysvar", Value: vardef.On, Type: vardef.TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	require.False(t, sv.HasSessionScope())
	require.False(t, sv.HasGlobalScope())
	require.False(t, sv.HasInstanceScope())
	require.True(t, sv.HasNoneScope())

	sv = SysVar{Scope: vardef.ScopeInstance, Name: "mynewsysvar", Value: vardef.On, Type: vardef.TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	require.False(t, sv.HasSessionScope())
	require.False(t, sv.HasGlobalScope())
	require.True(t, sv.HasInstanceScope())
	require.False(t, sv.HasNoneScope())

	sv = SysVar{Scope: vardef.ScopeSession, Name: "mynewsysvar", Value: vardef.On, Type: vardef.TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	require.Error(t, sv.validateScope(vardef.ScopeGlobal))
}

// TestSkipInitIsUsed ensures that no new variables are added with skipInit: true.
// This feature is deprecated, and if you need to run code to differentiate between init and "SET" (rare),
// you can instead check if s.StmtCtx.StmtType == "Set".
// The reason it is deprecated is that the behavior is typically wrong:
// it means session settings won't inherit from global and don't apply until you first set
// them in each session. This is a very weird behavior.
// See: https://github.com/pingcap/tidb/issues/35051
func TestSkipInitIsUsed(t *testing.T) {
	for _, sv := range GetSysVars() {
		if sv.skipInit {
			// skipInit only ever applied to session scope, so if anyone is setting it on
			// a variable without session, that doesn't make sense.
			require.True(t, sv.HasSessionScope(), fmt.Sprintf("skipInit has no effect on a variable without session scope: %s", sv.Name))
			// Since SetSession is the "init function" there is no init function to skip.
			require.NotNil(t, sv.SetSession, fmt.Sprintf("skipInit has no effect on variables without an init (setsession) func: %s", sv.Name))
			// Skipinit has no use on noop funcs, since noop funcs always skipinit.
			require.False(t, sv.IsNoop, fmt.Sprintf("skipInit has no effect on noop variables: %s", sv.Name))

			// Test for variables that have a default of "0" or "OFF"
			// If it is session-only scoped there is likely no bug now.
			// If it is also global-scoped, then there is a bug as soon as the global changes.
			if !(sv.Name == vardef.RandSeed1 || sv.Name == vardef.RandSeed2) {
				// The bug is because the tests might not realize the SetSession func was not called on init,
				// because it would initialize some session field to the empty value anyway.
				require.NotEqual(t, "0", sv.Value, fmt.Sprintf("default value is zero: %s", sv.Name))
				require.NotEqual(t, "OFF", sv.Value, fmt.Sprintf("default value is OFF: %s", sv.Name))
			}

			// Many of these variables might allow skipInit to be removed,
			// they need to be checked first. The purpose of this test is to make
			// sure we don't introduce any new variables with skipInit, which seems
			// to be a problem.
			switch sv.Name {
			case vardef.TiDBSnapshot,
				vardef.TiDBEnableChunkRPC,
				vardef.TxnIsolationOneShot,
				vardef.TiDBDDLReorgPriority,
				vardef.TiDBSlowQueryFile,
				vardef.TiDBWaitSplitRegionFinish,
				vardef.TiDBWaitSplitRegionTimeout,
				vardef.TiDBMetricSchemaStep,
				vardef.TiDBMetricSchemaRangeDuration,
				vardef.RandSeed1,
				vardef.RandSeed2,
				vardef.CollationDatabase,
				vardef.CollationConnection,
				vardef.CharsetDatabase,
				vardef.CharacterSetConnection,
				vardef.CharacterSetServer,
				vardef.TiDBOptTiFlashConcurrencyFactor,
				vardef.TiDBOptSeekFactor:
				continue
			}
			require.Equal(t, false, sv.skipInit, fmt.Sprintf("skipInit should not be set on new system variables. variable %s is in violation", sv.Name))
		}
	}
}

func TestSysVarCacheValues(t *testing.T) {
	RegisterSysVar(&SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "test_cache_session_var", Value: "a"})
	RegisterSysVar(&SysVar{Scope: vardef.ScopeGlobal, Name: "test_cache_global_var", Value: "b"})
	RegisterSysVar(&SysVar{Scope: vardef.ScopeGlobal, Name: "test_cache_config_var", Value: "c", IsInitedFromConfig: true})
	defer func() {
		UnregisterSysVar("test_cache_session_var")
		UnregisterSysVar("test_cache_global_var")
		UnregisterSysVar("test_cache_config_var")
	}()

	session, global := SysVarCacheValues(map[string]string{
		"test_cache_session_var": "x",
		"test_cache_global_var":  "y",
		"test_cache_config_var":  "z",
	})
	require.Equal(t, "x", session["test_cache_session_var"])
	require.Equal(t, "x", global["test_cache_session_var"])
	require.NotContains(t, session, "test_cache_global_var")
	require.Equal(t, "y", global["test_cache_global_var"])
	require.Equal(t, "c", global["test_cache_config_var"])
	require.Equal(t, vardef.On, session[vardef.TiDBRedactLog])
	require.Equal(t, vardef.On, global[vardef.TiDBRedactLog])
	for _, value := range []string{vardef.Off, vardef.Marker} {
		session, global = SysVarCacheValues(map[string]string{vardef.TiDBRedactLog: value})
		require.Equal(t, value, session[vardef.TiDBRedactLog])
		require.Equal(t, value, global[vardef.TiDBRedactLog])
	}
}

func TestMVServiceGlobalSysVars(t *testing.T) {
	mock := NewMockGlobalAccessor4Tests()
	for _, tt := range []struct{ name, value string }{
		{vardef.TiDBMViewTaskMax, "0"},
		{vardef.TiDBMViewTaskRefreshRatio, "0.6"},
		{vardef.TiDBMViewTaskThresholdCPU, "0.8"},
		{vardef.TiDBMViewTaskThresholdMemory, "0.8"},
		{vardef.TiDBMViewRefreshHistTime, "168"},
		{vardef.TiDBMLogPurgeHistTime, "168"},
		{vardef.TiDBMLogAutoAnalyzeRatio, "10"},
	} {
		sysVar := GetSysVar(tt.name)
		require.Equal(t, vardef.ScopeGlobal, sysVar.Scope, tt.name)
		require.False(t, sysVar.IsSensitive, tt.name)
		value, err := mock.GetGlobalSysVar(tt.name)
		require.NoError(t, err)
		require.Equal(t, tt.value, value, tt.name)
	}

	oldTaskMax := SetMVServiceTaskMaxConcurrency.Load()
	oldRefreshRatio := SetMVServiceRefreshTaskConcurrencyRatio.Load()
	oldCPU := SetMVServiceTaskThresholdCPU.Load()
	oldMemory := SetMVServiceTaskThresholdMemory.Load()
	oldRefreshHist := SetMVServiceMViewRefreshHistRetention.Load()
	oldPurgeHist := SetMVServiceMLogPurgeHistRetention.Load()
	defer func() {
		SetMVServiceTaskMaxConcurrency.Store(oldTaskMax)
		SetMVServiceRefreshTaskConcurrencyRatio.Store(oldRefreshRatio)
		SetMVServiceTaskThresholdCPU.Store(oldCPU)
		SetMVServiceTaskThresholdMemory.Store(oldMemory)
		SetMVServiceMViewRefreshHistRetention.Store(oldRefreshHist)
		SetMVServiceMLogPurgeHistRetention.Store(oldPurgeHist)
	}()

	gotTaskMax := -1
	gotRefreshRatio := -1.0
	gotCPU := -1.0
	gotMemory := -1.0
	var gotRefreshHist time.Duration
	var gotPurgeHist time.Duration

	taskMaxFn := func(v int) { gotTaskMax = v }
	refreshRatioFn := func(v float64) { gotRefreshRatio = v }
	cpuFn := func(v float64) { gotCPU = v }
	memoryFn := func(v float64) { gotMemory = v }
	refreshHistFn := func(v time.Duration) { gotRefreshHist = v }
	purgeHistFn := func(v time.Duration) { gotPurgeHist = v }
	SetMVServiceTaskMaxConcurrency.Store(&taskMaxFn)
	SetMVServiceRefreshTaskConcurrencyRatio.Store(&refreshRatioFn)
	SetMVServiceTaskThresholdCPU.Store(&cpuFn)
	SetMVServiceTaskThresholdMemory.Store(&memoryFn)
	SetMVServiceMViewRefreshHistRetention.Store(&refreshHistFn)
	SetMVServiceMLogPurgeHistRetention.Store(&purgeHistFn)

	require.NoError(t, mock.SetGlobalSysVar(context.Background(), vardef.TiDBMViewTaskMax, "0"))
	require.Equal(t, 0, gotTaskMax)
	require.NoError(t, mock.SetGlobalSysVar(context.Background(), vardef.TiDBMViewTaskMax, "257"))
	require.Equal(t, 256, gotTaskMax)

	require.NoError(t, mock.SetGlobalSysVar(context.Background(), vardef.TiDBMViewTaskRefreshRatio, "0.6"))
	require.Equal(t, 0.6, gotRefreshRatio)
	require.Error(t, mock.SetGlobalSysVar(context.Background(), vardef.TiDBMViewTaskRefreshRatio, "1"))
	require.Error(t, mock.SetGlobalSysVar(context.Background(), vardef.TiDBMViewTaskRefreshRatio, "0"))
	require.Equal(t, 0.6, gotRefreshRatio)

	require.NoError(t, mock.SetGlobalSysVar(context.Background(), vardef.TiDBMViewTaskThresholdCPU, "0.7"))
	require.Equal(t, 0.7, gotCPU)

	require.NoError(t, mock.SetGlobalSysVar(context.Background(), vardef.TiDBMViewTaskThresholdMemory, "0.6"))
	require.Equal(t, 0.6, gotMemory)

	require.NoError(t, mock.SetGlobalSysVar(context.Background(), vardef.TiDBMViewRefreshHistTime, "24"))
	require.Equal(t, 24*time.Hour, gotRefreshHist)

	require.NoError(t, mock.SetGlobalSysVar(context.Background(), vardef.TiDBMLogPurgeHistTime, "48"))
	require.Equal(t, 48*time.Hour, gotPurgeHist)
	require.NoError(t, mock.SetGlobalSysVar(context.Background(), vardef.TiDBMViewRefreshHistTime, "0"))
	require.Equal(t, time.Hour, gotRefreshHist)
	require.NoError(t, mock.SetGlobalSysVar(context.Background(), vardef.TiDBMLogPurgeHistTime, "8761"))
	require.Equal(t, 8760*time.Hour, gotPurgeHist)
}

func TestMVServiceFloatSysVarsRejectNaN(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name   string
		setter interface {
			Load() *func(float64)
			Store(*func(float64))
		}
	}{
		{vardef.TiDBMViewTaskRefreshRatio, &SetMVServiceRefreshTaskConcurrencyRatio},
		{vardef.TiDBMViewTaskThresholdCPU, &SetMVServiceTaskThresholdCPU},
		{vardef.TiDBMViewTaskThresholdMemory, &SetMVServiceTaskThresholdMemory},
		{vardef.TiDBMLogAutoAnalyzeRatio, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mock := NewMockGlobalAccessor4Tests()
			calls := 0
			if tt.setter != nil {
				old := tt.setter.Load()
				t.Cleanup(func() { tt.setter.Store(old) })
				setter := func(float64) { calls++ }
				tt.setter.Store(&setter)
			}

			require.NoError(t, mock.SetGlobalSysVar(ctx, tt.name, "0.5"))
			previousCalls := calls
			for _, value := range []string{"NaN", "nan", "NAN"} {
				require.ErrorContains(t, mock.SetGlobalSysVar(ctx, tt.name, value), tt.name)
				stored, err := mock.GetGlobalSysVar(tt.name)
				require.NoError(t, err)
				require.Equal(t, "0.5", stored)
				require.Equal(t, previousCalls, calls, "invalid value must not invoke the callback")
			}
		})
	}
}

func TestTiDBMLogAutoAnalyzeRatio(t *testing.T) {
	ctx := context.Background()
	vars := NewSessionVars(nil)
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	val, err := mock.GetGlobalSysVar(vardef.TiDBMLogAutoAnalyzeRatio)
	require.NoError(t, err)
	require.Equal(t, "10", val)

	err = mock.SetGlobalSysVar(ctx, vardef.TiDBMLogAutoAnalyzeRatio, "20")
	require.NoError(t, err)
	val, err = mock.GetGlobalSysVar(vardef.TiDBMLogAutoAnalyzeRatio)
	require.NoError(t, err)
	require.Equal(t, "20", val)

	err = mock.SetGlobalSysVar(ctx, vardef.TiDBMLogAutoAnalyzeRatio, "0")
	require.Error(t, err)
	val, err = mock.GetGlobalSysVar(vardef.TiDBMLogAutoAnalyzeRatio)
	require.NoError(t, err)
	require.Equal(t, "20", val)

	err = mock.SetGlobalSysVar(ctx, vardef.TiDBMLogAutoAnalyzeRatio, "0.000009999")
	require.Error(t, err)
	val, err = mock.GetGlobalSysVar(vardef.TiDBMLogAutoAnalyzeRatio)
	require.NoError(t, err)
	require.Equal(t, "20", val)
}

func TestRemovedHistoricalStatsVarsWarn(t *testing.T) {
	// The historical stats feature was removed; its variables are kept as no-op
	// variables for compatibility and warn on a value that had an effect before.
	for _, name := range []string{vardef.TiDBEnableHistoricalStats, vardef.TiDBEnableHistoricalStatsForCapture} {
		sv := GetSysVar(name)
		vars := NewSessionVars(nil)
		val, err := sv.Validate(vars, "ON", vardef.ScopeGlobal)
		require.NoError(t, err)
		require.Equal(t, "ON", val)
		warns := vars.StmtCtx.GetWarnings()
		require.Len(t, warns, 1)
		require.Contains(t, warns[0].Err.Error(), "the historical statistics feature has been removed")

		// Turning the switch off must not warn.
		vars = NewSessionVars(nil)
		val, err = sv.Validate(vars, "OFF", vardef.ScopeGlobal)
		require.NoError(t, err)
		require.Equal(t, "OFF", val)
		require.Len(t, vars.StmtCtx.GetWarnings(), 0)
	}

	// No retention value has an effect, because TiDB no longer deletes the legacy rows.
	sv := GetSysVar(vardef.TiDBHistoricalStatsDuration)
	vars := NewSessionVars(nil)
	val, err := sv.Validate(vars, "24h", vardef.ScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, "24h0m0s", val)
	warns := vars.StmtCtx.GetWarnings()
	require.Len(t, warns, 1)
	require.Contains(t, warns[0].Err.Error(), "the historical statistics feature has been removed")
}
>>>>>>> fb0565aa38b (statistics: remove historical stats feature (#69889))
