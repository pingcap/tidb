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

package tests

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
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestSysVar(t *testing.T) {
	f := variable.GetSysVar("autocommit")
	require.NotNil(t, f)

	f = variable.GetSysVar("wrong-var-name")
	require.Nil(t, f)

	f = variable.GetSysVar("explicit_defaults_for_timestamp")
	require.NotNil(t, f)
	require.Equal(t, "ON", f.Value)

	f = variable.GetSysVar("port")
	require.NotNil(t, f)
	require.Equal(t, "4000", f.Value)

	f = variable.GetSysVar("tidb_low_resolution_tso")
	require.Equal(t, "OFF", f.Value)

	f = variable.GetSysVar("tidb_replica_read")
	require.Equal(t, "leader", f.Value)

	f = variable.GetSysVar("tidb_enable_table_partition")
	require.Equal(t, "ON", f.Value)

	f = variable.GetSysVar("version_compile_os")
	require.Equal(t, runtime.GOOS, f.Value)

	f = variable.GetSysVar("version_compile_machine")
	require.Equal(t, runtime.GOARCH, f.Value)

	// default enable vectorized_expression
	f = variable.GetSysVar("tidb_enable_vectorized_expression")
	require.Equal(t, "ON", f.Value)
}

func TestError(t *testing.T) {
	kvErrs := []*terror.Error{
		variable.ErrUnsupportedValueForVar,
		variable.ErrUnknownSystemVar,
		variable.ErrIncorrectScope,
		variable.ErrUnknownTimeZone,
		variable.ErrReadOnly,
		variable.ErrWrongValueForVar,
		variable.ErrWrongTypeForVar,
		variable.ErrTruncatedWrongValue,
		variable.ErrMaxPreparedStmtCountReached,
		variable.ErrUnsupportedIsolationLevel,
	}
	for _, err := range kvErrs {
		require.True(t, terror.ToSQLError(err).Code != mysql.ErrUnknown)
	}
}

func TestRegistrationOfNewSysVar(t *testing.T) {
	count := len(variable.GetSysVars())
	sv := variable.SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "mynewsysvar", Value: vardef.On, Type: vardef.TypeBool, SetSession: func(s *variable.SessionVars, val string) error {
		return fmt.Errorf("set should fail")
	}}

	variable.RegisterSysVar(&sv)
	require.Len(t, variable.GetSysVars(), count+1)

	sysVar := variable.GetSysVar("mynewsysvar")
	require.NotNil(t, sysVar)

	vars := variable.NewSessionVars(nil)

	// It is a boolean, try to set it to a bogus value
	_, err := sysVar.Validate(vars, "ABCD", vardef.ScopeSession)
	require.Error(t, err)

	// Boolean oN or 1 converts to canonical ON or OFF
	normalizedVal, err := sysVar.Validate(vars, "oN", vardef.ScopeSession)
	require.Equal(t, "ON", normalizedVal)
	require.NoError(t, err)
	normalizedVal, err = sysVar.Validate(vars, "0", vardef.ScopeSession)
	require.Equal(t, "OFF", normalizedVal)
	require.NoError(t, err)

	err = sysVar.SetSessionFromHook(vars, "OFF") // default is on
	require.Equal(t, "set should fail", err.Error())

	// Test unregistration restores previous count
	variable.UnregisterSysVar("mynewsysvar")
	require.Equal(t, len(variable.GetSysVars()), count)
}

func TestIntValidation(t *testing.T) {
	sv := variable.SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "mynewsysvar", Value: "123", Type: vardef.TypeInt, MinValue: 10, MaxValue: 300, AllowAutoValue: true}
	vars := variable.NewSessionVars(nil)

	_, err := sv.Validate(vars, "oN", vardef.ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	val, err := sv.Validate(vars, "301", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "300", val)

	val, err = sv.Validate(vars, "5", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "10", val)

	val, err = sv.Validate(vars, "300", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "300", val)

	// out of range but permitted due to auto value
	val, err = sv.Validate(vars, "-1", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "-1", val)
}

func TestUintValidation(t *testing.T) {
	sv := variable.SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "mynewsysvar", Value: "123", Type: vardef.TypeUnsigned, MinValue: 10, MaxValue: 300, AllowAutoValue: true}
	vars := variable.NewSessionVars(nil)

	_, err := sv.Validate(vars, "oN", vardef.ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	_, err = sv.Validate(vars, "", vardef.ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	val, err := sv.Validate(vars, "301", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "300", val)

	val, err = sv.Validate(vars, "-301", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "10", val)

	_, err = sv.Validate(vars, "-ERR", vardef.ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	val, err = sv.Validate(vars, "5", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "10", val)

	val, err = sv.Validate(vars, "300", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "300", val)

	// out of range but permitted due to auto value
	val, err = sv.Validate(vars, "-1", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "-1", val)
}

func TestEnumValidation(t *testing.T) {
	sv := variable.SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "mynewsysvar", Value: vardef.On, Type: vardef.TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	vars := variable.NewSessionVars(nil)

	_, err := sv.Validate(vars, "randomstring", vardef.ScopeSession)
	require.Equal(t, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of 'randomstring'", err.Error())

	val, err := sv.Validate(vars, "oFf", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "OFF", val)

	val, err = sv.Validate(vars, "On", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "ON", val)

	val, err = sv.Validate(vars, "auto", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "AUTO", val)

	// Also settable by numeric offset.
	val, err = sv.Validate(vars, "2", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "AUTO", val)
}

func TestDurationValidation(t *testing.T) {
	sv := variable.SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "mynewsysvar", Value: "10m0s", Type: vardef.TypeDuration, MinValue: int64(time.Second), MaxValue: uint64(time.Hour)}
	vars := variable.NewSessionVars(nil)

	_, err := sv.Validate(vars, "1hr", vardef.ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	val, err := sv.Validate(vars, "1ms", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "1s", val) // truncates to min

	val, err = sv.Validate(vars, "2h10m", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "1h0m0s", val) // truncates to max
}

func TestFloatValidation(t *testing.T) {
	sv := variable.SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "mynewsysvar", Value: "10m0s", Type: vardef.TypeFloat, MinValue: 2, MaxValue: 7}
	vars := variable.NewSessionVars(nil)

	_, err := sv.Validate(vars, "stringval", vardef.ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	_, err = sv.Validate(vars, "", vardef.ScopeSession)
	require.Equal(t, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'", err.Error())

	val, err := sv.Validate(vars, "1.1", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "2", val) // truncates to min

	val, err = sv.Validate(vars, "22", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "7", val) // truncates to max
}

func TestBoolValidation(t *testing.T) {
	sv := variable.SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "mynewsysvar", Value: vardef.Off, Type: vardef.TypeBool}
	vars := variable.NewSessionVars(nil)

	_, err := sv.Validate(vars, "0.000", vardef.ScopeSession)
	require.Equal(t, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of '0.000'", err.Error())
	_, err = sv.Validate(vars, "1.000", vardef.ScopeSession)
	require.Equal(t, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of '1.000'", err.Error())
	val, err := sv.Validate(vars, "0", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, vardef.Off, val)
	val, err = sv.Validate(vars, "1", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, vardef.On, val)
	val, err = sv.Validate(vars, "OFF", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, vardef.Off, val)
	val, err = sv.Validate(vars, "ON", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, vardef.On, val)
	val, err = sv.Validate(vars, "off", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, vardef.Off, val)
	val, err = sv.Validate(vars, "on", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, vardef.On, val)

	// test AutoConvertNegativeBool
	sv = variable.SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "mynewsysvar", Value: vardef.Off, Type: vardef.TypeBool, AutoConvertNegativeBool: true}
	val, err = sv.Validate(vars, "-1", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, vardef.On, val)
	val, err = sv.Validate(vars, "1", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, vardef.On, val)
	val, err = sv.Validate(vars, "0", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, vardef.Off, val)
}

func TestTimeValidation(t *testing.T) {
	sv := variable.SysVar{Scope: vardef.ScopeSession, Name: "mynewsysvar", Value: "23:59 +0000", Type: vardef.TypeTime}
	vars := variable.NewSessionVars(nil)

	val, err := sv.Validate(vars, "23:59 +0000", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "23:59 +0000", val)

	val, err = sv.Validate(vars, "3:00 +0000", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "03:00 +0000", val)

	_, err = sv.Validate(vars, "0.000", vardef.ScopeSession)
	require.Error(t, err)
}

func TestGetNativeValType(t *testing.T) {
	sv := variable.SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "mynewsysvar", Value: vardef.Off, Type: vardef.TypeBool}

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

	sv = variable.SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "mynewsysvar", Value: vardef.Off, Type: vardef.TypeUnsigned}
	nativeVal, nativeType, flag = sv.GetNativeValType("1234")
	require.Equal(t, mysql.TypeLonglong, nativeType)
	require.Equal(t, mysql.UnsignedFlag|mysql.BinaryFlag, flag)
	require.Equal(t, types.NewUintDatum(1234), nativeVal)
	nativeVal, nativeType, flag = sv.GetNativeValType("bogus")
	require.Equal(t, mysql.TypeLonglong, nativeType)
	require.Equal(t, mysql.UnsignedFlag|mysql.BinaryFlag, flag)
	require.Equal(t, types.NewUintDatum(0), nativeVal) // converts to zero

	sv = variable.SysVar{Scope: vardef.ScopeGlobal | vardef.ScopeSession, Name: "mynewsysvar", Value: "abc"}
	nativeVal, nativeType, flag = sv.GetNativeValType("1234")
	require.Equal(t, mysql.TypeVarString, nativeType)
	require.Equal(t, uint(0), flag)
	require.Equal(t, types.NewStringDatum("1234"), nativeVal)
}

func TestDeprecation(t *testing.T) {
	sysVar := variable.GetSysVar(vardef.TiDBIndexLookupConcurrency)
	require.NotNil(t, sysVar)

	vars := variable.NewSessionVars(nil)

	_, err := sysVar.Validate(vars, "123", vardef.ScopeSession)
	require.NoError(t, err)

	// There was no error but there is a deprecation warning.
	warn := vars.StmtCtx.GetWarnings()[0].Err
	require.Equal(t, "[variable:1287]'tidb_index_lookup_concurrency' is deprecated and will be removed in a future release. Please use tidb_executor_concurrency instead", warn.Error())
}

func TestBuiltInCase(t *testing.T) {
	// All Sysvars should have lower case names.
	// This tests builtins.
	for name := range variable.GetSysVars() {
		require.Equal(t, strings.ToLower(name), name)
	}
}

// TestIsNoop is used by the documentation to auto-generate docs for real sysvars.
func TestIsNoop(t *testing.T) {
	sv := variable.GetSysVar(vardef.TiDBMultiStatementMode)
	require.False(t, sv.IsNoop)

	sv = variable.GetSysVar(vardef.InnodbLockWaitTimeout)
	require.False(t, sv.IsNoop)

	sv = variable.GetSysVar(vardef.InnodbFastShutdown)
	require.True(t, sv.IsNoop)

	sv = variable.GetSysVar(vardef.ReadOnly)
	require.True(t, sv.IsNoop)

	sv = variable.GetSysVar(vardef.DefaultPasswordLifetime)
	require.False(t, sv.IsNoop)
}

// TestDefaultValuesAreSettable that sysvars defaults are logically valid. i.e.
// the default itself must validate without error provided the scope and read-only is correct.
// The default values should also be normalized for consistency.
func TestDefaultValuesAreSettable(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.GlobalVarsAccessor = variable.NewMockGlobalAccessor4Tests()
	for _, sv := range variable.GetSysVars() {
		if sv.HasSessionScope() && !sv.ReadOnly && !sv.InternalSessionVariable {
			val, err := sv.Validate(vars, sv.Value, vardef.ScopeSession)
			require.NoError(t, err)
			require.Equal(t, val, sv.Value)
		}

		if sv.HasGlobalScope() && !sv.ReadOnly {
			val, err := sv.Validate(vars, sv.Value, vardef.ScopeGlobal)
			require.NoError(t, err)
			require.Equal(t, val, sv.Value)
		}
	}
}

func TestLimitBetweenVariable(t *testing.T) {
	require.Less(t, vardef.DefTiDBGOGCTunerThreshold+0.05, vardef.DefTiDBServerMemoryLimitGCTrigger)
}

// TestSysVarNameIsLowerCase tests that no new sysvars are added with uppercase characters.
// In MySQL variables are always lowercase, and can be set in a case-insensitive way.
func TestSysVarNameIsLowerCase(t *testing.T) {
	for _, sv := range variable.GetSysVars() {
		require.Equal(t, strings.ToLower(sv.Name), sv.Name, "sysvar name contains uppercase characters")
	}
}

// TestSettersandGetters tests that sysvars are logically correct with getter and setter functions.
// i.e. it doesn't make sense to have a SetSession function on a variable that is only globally scoped.
func TestSettersandGetters(t *testing.T) {
	for _, sv := range variable.GetSysVars() {
		if !sv.HasSessionScope() {
			require.Nil(t, sv.SetSession)
			require.Nil(t, sv.GetSession)
		}
		if !sv.HasGlobalScope() && !sv.HasInstanceScope() {
			require.Nil(t, sv.SetGlobal)
			if sv.Name == vardef.Timestamp {
				// The Timestamp sysvar will have GetGlobal func even though it does not have global scope.
				// It's GetGlobal func will only be called when "set timestamp = default".
				continue
			}
			require.Nil(t, sv.GetGlobal)
		}
	}
}

func TestScopeToString(t *testing.T) {
	require.Equal(t, "GLOBAL", vardef.ScopeGlobal.String())
	require.Equal(t, "SESSION", vardef.ScopeSession.String())
	require.Equal(t, "INSTANCE", vardef.ScopeInstance.String())
	require.Equal(t, "NONE", vardef.ScopeNone.String())
	tmp := vardef.ScopeSession + vardef.ScopeGlobal
	require.Equal(t, "SESSION,GLOBAL", tmp.String())
	// this is not currently possible, but might be in future.
	// *but* global + instance is not possible. these are mutually exclusive by design.
	tmp = vardef.ScopeSession + vardef.ScopeInstance
	require.Equal(t, "SESSION,INSTANCE", tmp.String())
}

func TestValidateWithRelaxedValidation(t *testing.T) {
	sv := variable.GetSysVar(vardef.SecureAuth)
	vars := variable.NewSessionVars(nil)
	val := sv.ValidateWithRelaxedValidation(vars, "1", vardef.ScopeGlobal)
	require.Equal(t, "ON", val)

	// Relaxed validation catches the error and squashes it.
	// The incorrect value is returned as-is.
	// I am not sure this is the correct behavior, we might need to
	// change it to return the default instead in future.
	sv = variable.GetSysVar(vardef.DefaultAuthPlugin)
	val = sv.ValidateWithRelaxedValidation(vars, "RandomText", vardef.ScopeGlobal)
	require.Equal(t, "RandomText", val)

	// Validation func fails, the error is also caught and squashed.
	// The incorrect value is returned as-is.
	sv = variable.GetSysVar(vardef.InitConnect)
	val = sv.ValidateWithRelaxedValidation(vars, "RandomText - should be valid SQL", vardef.ScopeGlobal)
	require.Equal(t, "RandomText - should be valid SQL", val)
}

func TestValidateInternalSessionVariable(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	for _, n := range []string{vardef.TiDBRedactLog, vardef.TiDBInstancePlanCacheMaxMemSize} {
		sv := variable.GetSysVar(n)
		_, err := sv.Validate(vars, "1", vardef.ScopeSession)
		require.NotNil(t, err)
	}
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
			sv := variable.GetSysVar(instanceName)
			require.NotNil(t, sv, fmt.Sprintf("config option: instance.%v requires a matching sysvar of the same name", instanceName))
		}
	}
}

func TestInstanceScope(t *testing.T) {
	// Instance scope used to be settable via "SET SESSION", which is weird to any MySQL user.
	// It is now settable via SET GLOBAL, but to work correctly a sysvar can only ever
	// be INSTANCE scoped or GLOBAL scoped, never *both* at the same time (at least for now).
	// Otherwise the semantics are confusing to users for how precedence applies.

	// Now Instance scope is a valid scope, and it can be used with GLOBAL scope at the same time.
	for _, sv := range variable.GetSysVars() {
		// But instance scope should not have Set/GetSession
		if sv.HasInstanceScope() {
			require.Nil(t, sv.GetSession)
			require.Nil(t, sv.SetSession)
		}
	}

	count := len(variable.GetSysVars())
	sv := variable.SysVar{Scope: vardef.ScopeInstance, Name: "newinstancesysvar", Value: vardef.On, Type: vardef.TypeBool,
		SetGlobal: func(_ context.Context, s *variable.SessionVars, val string) error {
			return fmt.Errorf("set should fail")
		},
		GetGlobal: func(_ context.Context, s *variable.SessionVars) (string, error) {
			return "", fmt.Errorf("get should fail")
		},
	}

	variable.RegisterSysVar(&sv)
	require.Len(t, variable.GetSysVars(), count+1)

	sysVar := variable.GetSysVar("newinstancesysvar")
	require.NotNil(t, sysVar)

	vars := variable.NewSessionVars(nil)

	// It is a boolean, try to set it to a bogus value
	_, err := sysVar.Validate(vars, "ABCD", vardef.ScopeInstance)
	require.Error(t, err)

	// Boolean oN or 1 converts to canonical ON or OFF
	normalizedVal, err := sysVar.Validate(vars, "oN", vardef.ScopeInstance)
	require.Equal(t, "ON", normalizedVal)
	require.NoError(t, err)
	normalizedVal, err = sysVar.Validate(vars, "0", vardef.ScopeInstance)
	require.Equal(t, "OFF", normalizedVal)
	require.NoError(t, err)

	err = sysVar.SetGlobalFromHook(context.Background(), vars, "OFF", true) // default is on
	require.Equal(t, "set should fail", err.Error())

	// Test unregistration restores previous count
	variable.UnregisterSysVar("newinstancesysvar")
	require.Equal(t, len(variable.GetSysVars()), count)
}

func TestSetSysVar(t *testing.T) {
	vars := variable.NewSessionVars(nil)
	vars.GlobalVarsAccessor = variable.NewMockGlobalAccessor4Tests()
	originalVal := variable.GetSysVar(vardef.SystemTimeZone).Value
	variable.SetSysVar(vardef.SystemTimeZone, "America/New_York")
	require.Equal(t, "America/New_York", variable.GetSysVar(vardef.SystemTimeZone).Value)
	// Test alternative Get
	val, err := variable.GetSysVar(vardef.SystemTimeZone).GetGlobalFromHook(context.Background(), vars)
	require.Nil(t, err)
	require.Equal(t, "America/New_York", val)
	variable.SetSysVar(vardef.SystemTimeZone, originalVal) // restore
	require.Equal(t, originalVal, variable.GetSysVar(vardef.SystemTimeZone).Value)
}

func TestSkipSysvarCache(t *testing.T) {
	require.True(t, variable.GetSysVar(vardef.TiDBGCEnable).SkipSysvarCache())
	require.True(t, variable.GetSysVar(vardef.TiDBGCRunInterval).SkipSysvarCache())
	require.True(t, variable.GetSysVar(vardef.TiDBGCLifetime).SkipSysvarCache())
	require.True(t, variable.GetSysVar(vardef.TiDBGCConcurrency).SkipSysvarCache())
	require.True(t, variable.GetSysVar(vardef.TiDBGCScanLockMode).SkipSysvarCache())
	require.False(t, variable.GetSysVar(vardef.TiDBEnableAsyncCommit).SkipSysvarCache())
}

func TestTimeValidationWithTimezone(t *testing.T) {
	sv := variable.SysVar{Scope: vardef.ScopeSession, Name: "mynewsysvar", Value: "23:59 +0000", Type: vardef.TypeTime}
	vars := variable.NewSessionVars(nil)

	// In timezone UTC
	vars.TimeZone = time.UTC
	val, err := sv.Validate(vars, "23:59", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "23:59 +0000", val)

	// In timezone Asia/Shanghai
	vars.TimeZone, err = time.LoadLocation("Asia/Shanghai")
	require.NoError(t, err)
	val, err = sv.Validate(vars, "23:59", vardef.ScopeSession)
	require.NoError(t, err)
	require.Equal(t, "23:59 +0800", val)
}

func TestOrderByDependency(t *testing.T) {
	// Some other exceptions:
	// - tidb_snapshot and tidb_read_staleness can not be set at the same time. It doesn't affect dependency.
	vars := map[string]string{
		"unknown":                                      "1",
		vardef.TxReadOnly:                              "1",
		vardef.SQLAutoIsNull:                           "1",
		vardef.TiDBEnableNoopFuncs:                     "1",
		vardef.TiDBEnforceMPPExecution:                 "1",
		vardef.TiDBAllowMPPExecution:                   "1",
		vardef.TiDBEnableLocalTxn:                      "1",
		vardef.TiDBEnablePlanReplayerContinuousCapture: "1",
		vardef.TiDBEnableHistoricalStats:               "1",
	}
	names := variable.OrderByDependency(vars)
	require.Greater(t, slices.Index(names, vardef.TxReadOnly), slices.Index(names, vardef.TiDBEnableNoopFuncs))
	require.Greater(t, slices.Index(names, vardef.SQLAutoIsNull), slices.Index(names, vardef.TiDBEnableNoopFuncs))
	require.Greater(t, slices.Index(names, vardef.TiDBEnforceMPPExecution), slices.Index(names, vardef.TiDBAllowMPPExecution))
	// Depended variables below are global variables, so actually it doesn't matter.
	require.Greater(t, slices.Index(names, vardef.TiDBEnablePlanReplayerContinuousCapture), slices.Index(names, vardef.TiDBEnableHistoricalStats))
	require.Contains(t, names, "unknown")
}
