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
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/stretchr/testify/require"
)

func TestSysVar(t *testing.T) {
	f := GetSysVar("autocommit")
	require.NotNil(t, f)

	f = GetSysVar("wrong-var-name")
	require.Nil(t, f)

	f = GetSysVar("explicit_defaults_for_timestamp")
	require.NotNil(t, f)
	require.Equal(t, "ON", f.Value)

	f = GetSysVar("port")
	require.NotNil(t, f)
	require.Equal(t, "4000", f.Value)

	f = GetSysVar("tidb_low_resolution_tso")
	require.Equal(t, "OFF", f.Value)

	f = GetSysVar("tidb_replica_read")
	require.Equal(t, "leader", f.Value)

	f = GetSysVar("tidb_enable_table_partition")
	require.Equal(t, "ON", f.Value)

	f = GetSysVar("version_compile_os")
	require.Equal(t, runtime.GOOS, f.Value)

	f = GetSysVar("version_compile_machine")
	require.Equal(t, runtime.GOARCH, f.Value)
}

func TestError(t *testing.T) {
	kvErrs := []*terror.Error{
		ErrUnsupportedValueForVar,
		ErrUnknownSystemVar,
		ErrIncorrectScope,
		ErrUnknownTimeZone,
		ErrReadOnly,
		ErrWrongValueForVar,
		ErrWrongTypeForVar,
		ErrTruncatedWrongValue,
		ErrMaxPreparedStmtCountReached,
		ErrUnsupportedIsolationLevel,
	}
	for _, err := range kvErrs {
		require.True(t, terror.ToSQLError(err).Code != mysql.ErrUnknown)
	}
}

func TestRegistrationOfNewSysVar(t *testing.T) {
	count := len(GetSysVars())
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: On, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		return fmt.Errorf("set should fail")
	}}

	RegisterSysVar(&sv)
	require.Len(t, GetSysVars(), count+1)

	sysVar := GetSysVar("mynewsysvar")
	require.NotNil(t, sysVar)

	vars := NewSessionVars()

	// It is a boolean, try to set it to a bogus value
	_, err := sysVar.Validate(vars, "ABCD", ScopeSession)
	require.Error(t, err)

	// Boolean oN or 1 converts to canonical ON or OFF
	normalizedVal, err := sysVar.Validate(vars, "oN", ScopeSession)
	require.Equal(t, "ON", normalizedVal)
	require.NoError(t, err)
	normalizedVal, err = sysVar.Validate(vars, "0", ScopeSession)
	require.Equal(t, "OFF", normalizedVal)
	require.NoError(t, err)

	err = sysVar.SetSessionFromHook(vars, "OFF") // default is on
	require.Equal(t, "set should fail", err.Error())

	// Test unregistration restores previous count
	UnregisterSysVar("mynewsysvar")
	require.Equal(t, len(GetSysVars()), count)
}

func TestIntValidation(t *testing.T) {
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: "123", Type: TypeInt, MinValue: 10, MaxValue: 300, AllowAutoValue: true}
	vars := NewSessionVars()

	_, err := sv.Validate(vars, "oN", ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	val, err := sv.Validate(vars, "301", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "300", val)

	val, err = sv.Validate(vars, "5", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "10", val)

	val, err = sv.Validate(vars, "300", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "300", val)

	// out of range but permitted due to auto value
	val, err = sv.Validate(vars, "-1", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "-1", val)
}

func TestUintValidation(t *testing.T) {
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: "123", Type: TypeUnsigned, MinValue: 10, MaxValue: 300, AllowAutoValue: true}
	vars := NewSessionVars()

	_, err := sv.Validate(vars, "oN", ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	_, err = sv.Validate(vars, "", ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	val, err := sv.Validate(vars, "301", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "300", val)

	val, err = sv.Validate(vars, "-301", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "10", val)

	_, err = sv.Validate(vars, "-ERR", ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	val, err = sv.Validate(vars, "5", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "10", val)

	val, err = sv.Validate(vars, "300", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "300", val)

	// out of range but permitted due to auto value
	val, err = sv.Validate(vars, "-1", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "-1", val)
}

func TestEnumValidation(t *testing.T) {
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: On, Type: TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	vars := NewSessionVars()

	_, err := sv.Validate(vars, "randomstring", ScopeSession)
	require.Equal(t, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of 'randomstring'", err.Error())

	val, err := sv.Validate(vars, "oFf", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "OFF", val)

	val, err = sv.Validate(vars, "On", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "ON", val)

	val, err = sv.Validate(vars, "auto", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "AUTO", val)

	// Also settable by numeric offset.
	val, err = sv.Validate(vars, "2", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "AUTO", val)
}

func TestSynonyms(t *testing.T) {
	sysVar := GetSysVar(TxnIsolation)
	require.NotNil(t, sysVar)

	vars := NewSessionVars()

	// It does not permit SERIALIZABLE by default.
	_, err := sysVar.Validate(vars, "SERIALIZABLE", ScopeSession)
	require.Error(t, err)
	require.Equal(t, "[variable:8048]The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error", err.Error())

	// Enable Skip isolation check
	require.Nil(t, GetSysVar(TiDBSkipIsolationLevelCheck).SetSessionFromHook(vars, "ON"))

	// Serializable is now permitted.
	_, err = sysVar.Validate(vars, "SERIALIZABLE", ScopeSession)
	require.NoError(t, err)

	// Currently TiDB returns a warning because of SERIALIZABLE, but in future
	// it may also return a warning because TxnIsolation is deprecated.

	warn := vars.StmtCtx.GetWarnings()[0].Err
	require.Equal(t, "[variable:8048]The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error", warn.Error())

	require.Nil(t, sysVar.SetSessionFromHook(vars, "SERIALIZABLE"))

	// When we set TxnIsolation, it also updates TransactionIsolation.
	require.Equal(t, "SERIALIZABLE", vars.systems[TxnIsolation])
	require.Equal(t, vars.systems[TxnIsolation], vars.systems[TransactionIsolation])
}

func TestDeprecation(t *testing.T) {
	sysVar := GetSysVar(TiDBIndexLookupConcurrency)
	require.NotNil(t, sysVar)

	vars := NewSessionVars()

	_, err := sysVar.Validate(vars, "123", ScopeSession)
	require.NoError(t, err)

	// There was no error but there is a deprecation warning.
	warn := vars.StmtCtx.GetWarnings()[0].Err
	require.Equal(t, "[variable:1287]'tidb_index_lookup_concurrency' is deprecated and will be removed in a future release. Please use tidb_executor_concurrency instead", warn.Error())
}

func TestScope(t *testing.T) {
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: On, Type: TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	require.True(t, sv.HasSessionScope())
	require.True(t, sv.HasGlobalScope())
	require.False(t, sv.HasInstanceScope())
	require.False(t, sv.HasNoneScope())

	sv = SysVar{Scope: ScopeGlobal, Name: "mynewsysvar", Value: On, Type: TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	require.False(t, sv.HasSessionScope())
	require.True(t, sv.HasGlobalScope())
	require.False(t, sv.HasInstanceScope())
	require.False(t, sv.HasNoneScope())

	sv = SysVar{Scope: ScopeSession, Name: "mynewsysvar", Value: On, Type: TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	require.True(t, sv.HasSessionScope())
	require.False(t, sv.HasGlobalScope())
	require.False(t, sv.HasInstanceScope())
	require.False(t, sv.HasNoneScope())

	sv = SysVar{Scope: ScopeNone, Name: "mynewsysvar", Value: On, Type: TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	require.False(t, sv.HasSessionScope())
	require.False(t, sv.HasGlobalScope())
	require.False(t, sv.HasInstanceScope())
	require.True(t, sv.HasNoneScope())

	sv = SysVar{Scope: ScopeInstance, Name: "mynewsysvar", Value: On, Type: TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	require.False(t, sv.HasSessionScope())
	require.False(t, sv.HasGlobalScope())
	require.True(t, sv.HasInstanceScope())
	require.False(t, sv.HasNoneScope())
}

func TestBuiltInCase(t *testing.T) {
	// All Sysvars should have lower case names.
	// This tests builtins.
	for name := range GetSysVars() {
		require.Equal(t, strings.ToLower(name), name)
	}
}

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

func TestForeignKeyChecks(t *testing.T) {
	sv := GetSysVar(ForeignKeyChecks)
	vars := NewSessionVars()

	val, err := sv.Validate(vars, "on", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "OFF", val) // warns and refuses to set ON.

	warn := vars.StmtCtx.GetWarnings()[0].Err
	require.Equal(t, "[variable:8047]variable 'foreign_key_checks' does not yet support value: on", warn.Error())

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

// TestIsNoop is used by the documentation to auto-generate docs for real sysvars.
func TestIsNoop(t *testing.T) {
	sv := GetSysVar(TiDBMultiStatementMode)
	require.False(t, sv.IsNoop)

	sv = GetSysVar(InnodbLockWaitTimeout)
	require.False(t, sv.IsNoop)

	sv = GetSysVar(InnodbFastShutdown)
	require.True(t, sv.IsNoop)

	sv = GetSysVar(ReadOnly)
	require.True(t, sv.IsNoop)
}

func TestTiDBReadOnly(t *testing.T) {
	rro := GetSysVar(TiDBRestrictedReadOnly)
	sro := GetSysVar(TiDBSuperReadOnly)

	vars := NewSessionVars()
	mock := NewMockGlobalAccessor4Tests()
	mock.SessionVars = vars
	vars.GlobalVarsAccessor = mock

	// turn on tidb_restricted_read_only should turn on tidb_super_read_only
	require.NoError(t, mock.SetGlobalSysVar(rro.Name, "ON"))
	result, err := mock.GetGlobalSysVar(sro.Name)
	require.NoError(t, err)
	require.Equal(t, "ON", result)

	// can't turn off tidb_super_read_only if tidb_restricted_read_only is on
	err = mock.SetGlobalSysVar(sro.Name, "OFF")
	require.Error(t, err)
	require.Equal(t, "can't turn off tidb_super_read_only when tidb_restricted_read_only is on", err.Error())

	// turn off tidb_restricted_read_only won't affect tidb_super_read_only
	require.NoError(t, mock.SetGlobalSysVar(rro.Name, "OFF"))
	result, err = mock.GetGlobalSysVar(rro.Name)
	require.NoError(t, err)
	require.Equal(t, "OFF", result)

	result, err = mock.GetGlobalSysVar(sro.Name)
	require.NoError(t, err)
	require.Equal(t, "ON", result)

	// it is ok to turn off tidb_super_read_only now
	require.NoError(t, mock.SetGlobalSysVar(sro.Name, "OFF"))
	result, err = mock.GetGlobalSysVar(sro.Name)
	require.NoError(t, err)
	require.Equal(t, "OFF", result)
}

func TestInstanceScopedVars(t *testing.T) {
	// This tests instance scoped variables through GetSessionOrGlobalSystemVar().
	// Eventually these should be changed to use getters so that the switch
	// statement in GetSessionOnlySysVars can be removed.

	vars := NewSessionVars()

	val, err := GetSessionOrGlobalSystemVar(vars, TiDBCurrentTS)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d", vars.TxnCtx.StartTS), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBLastTxnInfo)
	require.NoError(t, err)
	require.Equal(t, vars.LastTxnInfo, val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBLastQueryInfo)
	require.NoError(t, err)
	info, err := json.Marshal(vars.LastQueryInfo)
	require.NoError(t, err)
	require.Equal(t, string(info), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBGeneralLog)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(ProcessGeneralLog.Load()), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBPProfSQLCPU)
	require.NoError(t, err)
	expected := "0"
	if EnablePProfSQLCPU.Load() {
		expected = "1"
	}
	require.Equal(t, expected, val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBExpensiveQueryTimeThreshold)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%d", atomic.LoadUint64(&ExpensiveQueryTimeThreshold)), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBMemoryUsageAlarmRatio)
	require.NoError(t, err)
	require.Equal(t, fmt.Sprintf("%g", MemoryUsageAlarmRatio.Load()), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBForcePriority)
	require.NoError(t, err)
	require.Equal(t, mysql.Priority2Str[mysql.PriorityEnum(atomic.LoadInt32(&ForcePriority))], val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBDDLSlowOprThreshold)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(uint64(atomic.LoadUint32(&DDLSlowOprThreshold)), 10), val)

	val, err = GetSessionOrGlobalSystemVar(vars, PluginDir)
	require.NoError(t, err)
	require.Equal(t, config.GetGlobalConfig().Plugin.Dir, val)

	val, err = GetSessionOrGlobalSystemVar(vars, PluginLoad)
	require.NoError(t, err)
	require.Equal(t, config.GetGlobalConfig().Plugin.Load, val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBSlowLogThreshold)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Log.SlowThreshold), 10), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBRecordPlanInSlowLog)
	require.NoError(t, err)
	enabled := atomic.LoadUint32(&config.GetGlobalConfig().Log.RecordPlanInSlowLog) == 1
	require.Equal(t, BoolToOnOff(enabled), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBEnableSlowLog)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(config.GetGlobalConfig().Log.EnableSlowLog.Load()), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBQueryLogMaxLen)
	require.NoError(t, err)
	require.Equal(t, strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Log.QueryLogMaxLen), 10), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBCheckMb4ValueInUTF8)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(config.GetGlobalConfig().CheckMb4ValueInUTF8.Load()), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBFoundInPlanCache)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(vars.PrevFoundInPlanCache), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBFoundInBinding)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(vars.PrevFoundInBinding), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBEnableCollectExecutionInfo)
	require.NoError(t, err)
	require.Equal(t, BoolToOnOff(config.GetGlobalConfig().EnableCollectExecutionInfo), val)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBTxnScope)
	require.NoError(t, err)
	require.Equal(t, vars.TxnScope.GetVarValue(), val)
}

// TestDefaultValuesAreSettable that sysvars defaults are logically valid. i.e.
// the default itself must validate without error provided the scope and read-only is correct.
// The default values should also be normalized for consistency.
func TestDefaultValuesAreSettable(t *testing.T) {
	vars := NewSessionVars()
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	for _, sv := range GetSysVars() {
		if sv.HasSessionScope() && !sv.ReadOnly {
			val, err := sv.Validate(vars, sv.Value, ScopeSession)
			require.Equal(t, val, sv.Value)
			require.NoError(t, err)
		}

		if sv.HasGlobalScope() && !sv.ReadOnly {
			val, err := sv.Validate(vars, sv.Value, ScopeGlobal)
			require.Equal(t, val, sv.Value)
			require.NoError(t, err)
		}
	}
}

// TestSettersandGetters tests that sysvars are logically correct with getter and setter functions.
// i.e. it doesn't make sense to have a SetSession function on a variable that is only globally scoped.
func TestSettersandGetters(t *testing.T) {
	for _, sv := range GetSysVars() {
		if !sv.HasSessionScope() {
			// There are some historial exceptions where global variables are loaded into the session.
			// Please don't add to this list, the behavior is not MySQL compatible.
			switch sv.Name {
			case TiDBEnableChangeMultiSchema, TiDBDDLReorgBatchSize,
				TiDBMaxDeltaSchemaCount, InitConnect, MaxPreparedStmtCount,
				TiDBDDLReorgWorkerCount, TiDBDDLErrorCountLimit, TiDBRowFormatVersion,
				TiDBEnableTelemetry, TiDBEnablePointGetCache:
				continue
			}
			require.Nil(t, sv.SetSession)
			require.Nil(t, sv.GetSession)
		}
		if !sv.HasGlobalScope() && !sv.HasInstanceScope() {
			require.Nil(t, sv.SetGlobal)
			if sv.Name == Timestamp {
				// The Timestamp sysvar will have GetGlobal func even though it does not have global scope.
				// It's GetGlobal func will only be called when "set timestamp = default".
				continue
			}
			require.Nil(t, sv.GetGlobal)
		}
	}
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

func TestValidateWithRelaxedValidation(t *testing.T) {
	sv := GetSysVar(SecureAuth)
	vars := NewSessionVars()
	val := sv.ValidateWithRelaxedValidation(vars, "1", ScopeGlobal)
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
	val, err := GetSessionOrGlobalSystemVar(vars, LastInsertID)
	require.NoError(t, err)
	require.Equal(t, val, "0")

	vars.StmtCtx.PrevLastInsertID = 21
	val, err = GetSessionOrGlobalSystemVar(vars, LastInsertID)
	require.NoError(t, err)
	require.Equal(t, val, "21")
}

func TestTimestamp(t *testing.T) {
	vars := NewSessionVars()
	val, err := GetSessionOrGlobalSystemVar(vars, Timestamp)
	require.NoError(t, err)
	require.NotEqual(t, "", val)

	vars.systems[Timestamp] = "10"
	val, err = GetSessionOrGlobalSystemVar(vars, Timestamp)
	require.NoError(t, err)
	require.Equal(t, "10", val)

	vars.systems[Timestamp] = "0" // set to default
	val, err = GetSessionOrGlobalSystemVar(vars, Timestamp)
	require.NoError(t, err)
	require.NotEqual(t, "", val)
	require.NotEqual(t, "10", val)
}

func TestIdentity(t *testing.T) {
	vars := NewSessionVars()
	val, err := GetSessionOrGlobalSystemVar(vars, Identity)
	require.NoError(t, err)
	require.Equal(t, val, "0")

	vars.StmtCtx.PrevLastInsertID = 21
	val, err = GetSessionOrGlobalSystemVar(vars, Identity)
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
	val, err := GetSessionOrGlobalSystemVar(vars, CharacterSetConnection)
	require.NoError(t, err)
	require.Equal(t, val, mysql.DefaultCharset)
	val, err = GetSessionOrGlobalSystemVar(vars, CollationConnection)
	require.NoError(t, err)
	require.Equal(t, val, mysql.DefaultCollationName)
}

func TestInstanceScope(t *testing.T) {
	// Instance scope used to be settable via "SET SESSION", which is weird to any MySQL user.
	// It is now settable via SET GLOBAL, but to work correctly a sysvar can only ever
	// be INSTANCE scoped or GLOBAL scoped, never *both* at the same time (at least for now).
	// Otherwise the semantics are confusing to users for how precedence applies.

	for _, sv := range GetSysVars() {
		require.False(t, sv.HasGlobalScope() && sv.HasInstanceScope(), "sysvar %s has both instance and global scope", sv.Name)
		if sv.HasInstanceScope() {
			require.NotNil(t, sv.GetGlobal)
			require.NotNil(t, sv.SetGlobal)
		}
	}

	count := len(GetSysVars())
	sv := SysVar{Scope: ScopeInstance, Name: "newinstancesysvar", Value: On, Type: TypeBool,
		SetGlobal: func(s *SessionVars, val string) error {
			return fmt.Errorf("set should fail")
		},
		GetGlobal: func(s *SessionVars) (string, error) {
			return "", fmt.Errorf("get should fail")
		},
	}

	RegisterSysVar(&sv)
	require.Len(t, GetSysVars(), count+1)

	sysVar := GetSysVar("newinstancesysvar")
	require.NotNil(t, sysVar)

	vars := NewSessionVars()

	// It is a boolean, try to set it to a bogus value
	_, err := sysVar.Validate(vars, "ABCD", ScopeInstance)
	require.Error(t, err)

	// Boolean oN or 1 converts to canonical ON or OFF
	normalizedVal, err := sysVar.Validate(vars, "oN", ScopeInstance)
	require.Equal(t, "ON", normalizedVal)
	require.NoError(t, err)
	normalizedVal, err = sysVar.Validate(vars, "0", ScopeInstance)
	require.Equal(t, "OFF", normalizedVal)
	require.NoError(t, err)

	err = sysVar.SetGlobalFromHook(vars, "OFF", true) // default is on
	require.Equal(t, "set should fail", err.Error())

	// Test unregistration restores previous count
	UnregisterSysVar("newinstancesysvar")
	require.Equal(t, len(GetSysVars()), count)
}

func TestIndexMergeSwitcher(t *testing.T) {
	vars := NewSessionVars()
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	val, err := GetSessionOrGlobalSystemVar(vars, TiDBEnableIndexMerge)
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
