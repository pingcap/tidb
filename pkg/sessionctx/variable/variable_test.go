// Copyright 2022 PingCAP, Inc.
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
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/types"
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

	// default enable vectorized_expression
	f = GetSysVar("tidb_enable_vectorized_expression")
	require.Equal(t, "ON", f.Value)
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

	vars := NewSessionVars(nil)

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
	vars := NewSessionVars(nil)

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
	vars := NewSessionVars(nil)

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
	vars := NewSessionVars(nil)

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

func TestDurationValidation(t *testing.T) {
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: "10m0s", Type: TypeDuration, MinValue: int64(time.Second), MaxValue: uint64(time.Hour)}
	vars := NewSessionVars(nil)

	_, err := sv.Validate(vars, "1hr", ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	val, err := sv.Validate(vars, "1ms", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "1s", val) // truncates to min

	val, err = sv.Validate(vars, "2h10m", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "1h0m0s", val) // truncates to max
}

func TestFloatValidation(t *testing.T) {
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: "10m0s", Type: TypeFloat, MinValue: 2, MaxValue: 7}
	vars := NewSessionVars(nil)

	_, err := sv.Validate(vars, "stringval", ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	_, err = sv.Validate(vars, "", ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	val, err := sv.Validate(vars, "1.1", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "2", val) // truncates to min

	val, err = sv.Validate(vars, "22", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "7", val) // truncates to max
}

func TestBoolValidation(t *testing.T) {
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: Off, Type: TypeBool}
	vars := NewSessionVars(nil)

	_, err := sv.Validate(vars, "0.000", ScopeSession)
	require.Equal(t, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of '0.000'", err.Error())
	_, err = sv.Validate(vars, "1.000", ScopeSession)
	require.Equal(t, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of '1.000'", err.Error())
	val, err := sv.Validate(vars, "0", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, Off, val)
	val, err = sv.Validate(vars, "1", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, On, val)
	val, err = sv.Validate(vars, "OFF", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, Off, val)
	val, err = sv.Validate(vars, "ON", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, On, val)
	val, err = sv.Validate(vars, "off", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, Off, val)
	val, err = sv.Validate(vars, "on", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, On, val)

	// test AutoConvertNegativeBool
	sv = SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: Off, Type: TypeBool, AutoConvertNegativeBool: true}
	val, err = sv.Validate(vars, "-1", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, On, val)
	val, err = sv.Validate(vars, "1", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, On, val)
	val, err = sv.Validate(vars, "0", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, Off, val)
}

func TestTimeValidation(t *testing.T) {
	sv := SysVar{Scope: ScopeSession, Name: "mynewsysvar", Value: "23:59 +0000", Type: TypeTime}
	vars := NewSessionVars(nil)

	val, err := sv.Validate(vars, "23:59 +0000", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "23:59 +0000", val)

	val, err = sv.Validate(vars, "3:00 +0000", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "03:00 +0000", val)

	_, err = sv.Validate(vars, "0.000", ScopeSession)
	require.Error(t, err)
}

func TestGetNativeValType(t *testing.T) {
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: Off, Type: TypeBool}

	nativeVal, nativeType, flag := sv.GetNativeValType("ON")
	require.Equal(t, mysql.TypeLonglong, nativeType)
	require.Equal(t, mysql.BinaryFlag, flag)
	require.Equal(t, types.NewIntDatum(1), nativeVal)

	nativeVal, nativeType, flag = sv.GetNativeValType("OFF")
	require.Equal(t, mysql.TypeLonglong, nativeType)
	require.Equal(t, mysql.BinaryFlag, flag)
	require.Equal(t, types.NewIntDatum(0), nativeVal)

	nativeVal, nativeType, flag = sv.GetNativeValType("bogus")
	require.Equal(t, mysql.TypeLonglong, nativeType)
	require.Equal(t, mysql.BinaryFlag, flag)
	require.Equal(t, types.NewIntDatum(0), nativeVal)

	sv = SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: Off, Type: TypeUnsigned}
	nativeVal, nativeType, flag = sv.GetNativeValType("1234")
	require.Equal(t, mysql.TypeLonglong, nativeType)
	require.Equal(t, mysql.UnsignedFlag|mysql.BinaryFlag, flag)
	require.Equal(t, types.NewUintDatum(1234), nativeVal)
	nativeVal, nativeType, flag = sv.GetNativeValType("bogus")
	require.Equal(t, mysql.TypeLonglong, nativeType)
	require.Equal(t, mysql.UnsignedFlag|mysql.BinaryFlag, flag)
	require.Equal(t, types.NewUintDatum(0), nativeVal) // converts to zero

	sv = SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: "abc"}
	nativeVal, nativeType, flag = sv.GetNativeValType("1234")
	require.Equal(t, mysql.TypeVarString, nativeType)
	require.Equal(t, uint(0), flag)
	require.Equal(t, types.NewStringDatum("1234"), nativeVal)
}

func TestSynonyms(t *testing.T) {
	sysVar := GetSysVar(TxnIsolation)
	require.NotNil(t, sysVar)

	vars := NewSessionVars(nil)

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

	vars := NewSessionVars(nil)

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

	sv = SysVar{Scope: ScopeSession, Name: "mynewsysvar", Value: On, Type: TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	require.Error(t, sv.validateScope(ScopeGlobal))
}

func TestBuiltInCase(t *testing.T) {
	// All Sysvars should have lower case names.
	// This tests builtins.
	for name := range GetSysVars() {
		require.Equal(t, strings.ToLower(name), name)
	}
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

	sv = GetSysVar(DefaultPasswordLifetime)
	require.False(t, sv.IsNoop)
}

// TestDefaultValuesAreSettable that sysvars defaults are logically valid. i.e.
// the default itself must validate without error provided the scope and read-only is correct.
// The default values should also be normalized for consistency.
func TestDefaultValuesAreSettable(t *testing.T) {
	vars := NewSessionVars(nil)
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	for _, sv := range GetSysVars() {
		if sv.HasSessionScope() && !sv.ReadOnly {
			val, err := sv.Validate(vars, sv.Value, ScopeSession)
			require.NoError(t, err)
			require.Equal(t, val, sv.Value)
		}

		if sv.HasGlobalScope() && !sv.ReadOnly {
			val, err := sv.Validate(vars, sv.Value, ScopeGlobal)
			require.NoError(t, err)
			require.Equal(t, val, sv.Value)
		}
	}
}

func TestLimitBetweenVariable(t *testing.T) {
	require.Less(t, DefTiDBGOGCTunerThreshold+0.05, DefTiDBServerMemoryLimitGCTrigger)
}

// TestSysVarNameIsLowerCase tests that no new sysvars are added with uppercase characters.
// In MySQL variables are always lowercase, and can be set in a case-insensitive way.
func TestSysVarNameIsLowerCase(t *testing.T) {
	for _, sv := range GetSysVars() {
		require.Equal(t, strings.ToLower(sv.Name), sv.Name, "sysvar name contains uppercase characters")
	}
}

// TestSettersandGetters tests that sysvars are logically correct with getter and setter functions.
// i.e. it doesn't make sense to have a SetSession function on a variable that is only globally scoped.
func TestSettersandGetters(t *testing.T) {
	for _, sv := range GetSysVars() {
		if !sv.HasSessionScope() {
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
			if !(sv.Name == RandSeed1 || sv.Name == RandSeed2) {
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
			case TiDBTxnScope,
				TiDBSnapshot,
				TiDBEnableChunkRPC,
				TxnIsolationOneShot,
				TiDBDDLReorgPriority,
				TiDBSlowQueryFile,
				TiDBWaitSplitRegionFinish,
				TiDBWaitSplitRegionTimeout,
				TiDBMetricSchemaStep,
				TiDBMetricSchemaRangeDuration,
				RandSeed1,
				RandSeed2,
				CollationDatabase,
				CollationConnection,
				CharsetDatabase,
				CharacterSetConnection,
				CharacterSetServer,
				TiDBOptTiFlashConcurrencyFactor,
				TiDBOptSeekFactor:
				continue
			}
			require.Equal(t, false, sv.skipInit, fmt.Sprintf("skipInit should not be set on new system variables. variable %s is in violation", sv.Name))
		}
	}
}

func TestScopeToString(t *testing.T) {
	require.Equal(t, "GLOBAL", ScopeGlobal.String())
	require.Equal(t, "SESSION", ScopeSession.String())
	require.Equal(t, "INSTANCE", ScopeInstance.String())
	require.Equal(t, "NONE", ScopeNone.String())
	tmp := ScopeSession + ScopeGlobal
	require.Equal(t, "SESSION,GLOBAL", tmp.String())
	// this is not currently possible, but might be in future.
	// *but* global + instance is not possible. these are mutually exclusive by design.
	tmp = ScopeSession + ScopeInstance
	require.Equal(t, "SESSION,INSTANCE", tmp.String())
}

func TestValidateWithRelaxedValidation(t *testing.T) {
	sv := GetSysVar(SecureAuth)
	vars := NewSessionVars(nil)
	val := sv.ValidateWithRelaxedValidation(vars, "1", ScopeGlobal)
	require.Equal(t, "ON", val)

	// Relaxed validation catches the error and squashes it.
	// The incorrect value is returned as-is.
	// I am not sure this is the correct behavior, we might need to
	// change it to return the default instead in future.
	sv = GetSysVar(DefaultAuthPlugin)
	val = sv.ValidateWithRelaxedValidation(vars, "RandomText", ScopeGlobal)
	require.Equal(t, "RandomText", val)

	// Validation func fails, the error is also caught and squashed.
	// The incorrect value is returned as-is.
	sv = GetSysVar(InitConnect)
	val = sv.ValidateWithRelaxedValidation(vars, "RandomText - should be valid SQL", ScopeGlobal)
	require.Equal(t, "RandomText - should be valid SQL", val)
}

func TestInstanceConfigHasMatchingSysvar(t *testing.T) {
	// This tests that each item in [instance] has a sysvar of the same name.
	// The whole point of moving items to [instance] is to unify the name between
	// config and sysvars. See: docs/design/2021-12-08-instance-scope.md#introduction
	cfg, err := config.GetJSONConfig()
	require.NoError(t, err)
	var v any
	json.Unmarshal([]byte(cfg), &v)
	data := v.(map[string]any)
	for k, v := range data {
		if k != "instance" {
			continue
		}
		instanceSection := v.(map[string]any)
		for instanceName := range instanceSection {
			// Need to check there is a sysvar named instanceName.
			sv := GetSysVar(instanceName)
			require.NotNil(t, sv, fmt.Sprintf("config option: instance.%v requires a matching sysvar of the same name", instanceName))
		}
	}
}

func TestInstanceScope(t *testing.T) {
	// Instance scope used to be settable via "SET SESSION", which is weird to any MySQL user.
	// It is now settable via SET GLOBAL, but to work correctly a sysvar can only ever
	// be INSTANCE scoped or GLOBAL scoped, never *both* at the same time (at least for now).
	// Otherwise the semantics are confusing to users for how precedence applies.

	for _, sv := range GetSysVars() {
		require.False(t, sv.HasGlobalScope() && sv.HasInstanceScope(), "sysvar %s has both instance and global scope", sv.Name)
		if sv.HasInstanceScope() {
			require.Nil(t, sv.GetSession)
			require.Nil(t, sv.SetSession)
		}
	}

	count := len(GetSysVars())
	sv := SysVar{Scope: ScopeInstance, Name: "newinstancesysvar", Value: On, Type: TypeBool,
		SetGlobal: func(_ context.Context, s *SessionVars, val string) error {
			return fmt.Errorf("set should fail")
		},
		GetGlobal: func(_ context.Context, s *SessionVars) (string, error) {
			return "", fmt.Errorf("get should fail")
		},
	}

	RegisterSysVar(&sv)
	require.Len(t, GetSysVars(), count+1)

	sysVar := GetSysVar("newinstancesysvar")
	require.NotNil(t, sysVar)

	vars := NewSessionVars(nil)

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

	err = sysVar.SetGlobalFromHook(context.Background(), vars, "OFF", true) // default is on
	require.Equal(t, "set should fail", err.Error())

	// Test unregistration restores previous count
	UnregisterSysVar("newinstancesysvar")
	require.Equal(t, len(GetSysVars()), count)
}

func TestSetSysVar(t *testing.T) {
	vars := NewSessionVars(nil)
	vars.GlobalVarsAccessor = NewMockGlobalAccessor4Tests()
	originalVal := GetSysVar(SystemTimeZone).Value
	SetSysVar(SystemTimeZone, "America/New_York")
	require.Equal(t, "America/New_York", GetSysVar(SystemTimeZone).Value)
	// Test alternative Get
	val, err := GetSysVar(SystemTimeZone).GetGlobalFromHook(context.Background(), vars)
	require.Nil(t, err)
	require.Equal(t, "America/New_York", val)
	SetSysVar(SystemTimeZone, originalVal) // restore
	require.Equal(t, originalVal, GetSysVar(SystemTimeZone).Value)
}

func TestSkipSysvarCache(t *testing.T) {
	require.True(t, GetSysVar(TiDBGCEnable).SkipSysvarCache())
	require.True(t, GetSysVar(TiDBGCRunInterval).SkipSysvarCache())
	require.True(t, GetSysVar(TiDBGCLifetime).SkipSysvarCache())
	require.True(t, GetSysVar(TiDBGCConcurrency).SkipSysvarCache())
	require.True(t, GetSysVar(TiDBGCScanLockMode).SkipSysvarCache())
	require.False(t, GetSysVar(TiDBEnableAsyncCommit).SkipSysvarCache())
}

func TestTimeValidationWithTimezone(t *testing.T) {
	sv := SysVar{Scope: ScopeSession, Name: "mynewsysvar", Value: "23:59 +0000", Type: TypeTime}
	vars := NewSessionVars(nil)

	// In timezone UTC
	vars.TimeZone = time.UTC
	val, err := sv.Validate(vars, "23:59", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "23:59 +0000", val)

	// In timezone Asia/Shanghai
	vars.TimeZone, err = time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	val, err = sv.Validate(vars, "23:59", ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "23:59 +0800", val)
}

func TestOrderByDependency(t *testing.T) {
	// Some other exceptions:
	// - tidb_snapshot and tidb_read_staleness can not be set at the same time. It doesn't affect dependency.
	vars := map[string]string{
		"unknown":                               "1",
		TxReadOnly:                              "1",
		SQLAutoIsNull:                           "1",
		TiDBEnableNoopFuncs:                     "1",
		TiDBEnforceMPPExecution:                 "1",
		TiDBAllowMPPExecution:                   "1",
		TiDBTxnScope:                            kv.LocalTxnScope,
		TiDBEnableLocalTxn:                      "1",
		TiDBEnablePlanReplayerContinuousCapture: "1",
		TiDBEnableHistoricalStats:               "1",
	}
	names := OrderByDependency(vars)
	require.Greater(t, slices.Index(names, TxReadOnly), slices.Index(names, TiDBEnableNoopFuncs))
	require.Greater(t, slices.Index(names, SQLAutoIsNull), slices.Index(names, TiDBEnableNoopFuncs))
	require.Greater(t, slices.Index(names, TiDBEnforceMPPExecution), slices.Index(names, TiDBAllowMPPExecution))
	// Depended variables below are global variables, so actually it doesn't matter.
	require.Greater(t, slices.Index(names, TiDBTxnScope), slices.Index(names, TiDBEnableLocalTxn))
	require.Greater(t, slices.Index(names, TiDBEnablePlanReplayerContinuousCapture), slices.Index(names, TiDBEnableHistoricalStats))
	require.Contains(t, names, "unknown")
}
