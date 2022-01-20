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
// See the License for the specific language governing permissions and
// limitations under the License.

package variable

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSysVarSuite{})

type testSysVarSuite struct {
}

func (*testSysVarSuite) TestSysVar(c *C) {
	f := GetSysVar("autocommit")
	c.Assert(f, NotNil)

	f = GetSysVar("wrong-var-name")
	c.Assert(f, IsNil)

	f = GetSysVar("explicit_defaults_for_timestamp")
	c.Assert(f, NotNil)
	c.Assert(f.Value, Equals, "ON")

	f = GetSysVar("port")
	c.Assert(f, NotNil)
	c.Assert(f.Value, Equals, "4000")

	f = GetSysVar("tidb_low_resolution_tso")
	c.Assert(f.Value, Equals, "OFF")

	f = GetSysVar("tidb_replica_read")
	c.Assert(f.Value, Equals, "leader")

	f = GetSysVar("tidb_enable_table_partition")
	c.Assert(f.Value, Equals, "ON")
}

func (*testSysVarSuite) TestTxnMode(c *C) {
	seVar := NewSessionVars()
	c.Assert(seVar, NotNil)
	c.Assert(seVar.TxnMode, Equals, "")
	err := seVar.setTxnMode("pessimistic")
	c.Assert(err, IsNil)
	err = seVar.setTxnMode("optimistic")
	c.Assert(err, IsNil)
	err = seVar.setTxnMode("")
	c.Assert(err, IsNil)
	err = seVar.setTxnMode("something else")
	c.Assert(err, NotNil)
}

func (*testSysVarSuite) TestError(c *C) {
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
		c.Assert(terror.ToSQLError(err).Code != mysql.ErrUnknown, IsTrue)
	}
}

func (*testSysVarSuite) TestRegistrationOfNewSysVar(c *C) {
	count := len(GetSysVars())
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: On, Type: TypeBool, SetSession: func(s *SessionVars, val string) error {
		return fmt.Errorf("set should fail")
	}}

	RegisterSysVar(&sv)
	c.Assert(count+1, Equals, len(GetSysVars()))

	sysVar := GetSysVar("mynewsysvar")
	c.Assert(sysVar, NotNil)

	vars := NewSessionVars()

	// It is a boolean, try to set it to a bogus value
	_, err := sysVar.Validate(vars, "ABCD", ScopeSession)
	c.Assert(err, NotNil)

	// Boolean oN or 1 converts to canonical ON or OFF
	normalizedVal, err := sysVar.Validate(vars, "oN", ScopeSession)
	c.Assert(normalizedVal, Equals, "ON")
	c.Assert(err, IsNil)
	normalizedVal, err = sysVar.Validate(vars, "0", ScopeSession)
	c.Assert(normalizedVal, Equals, "OFF")
	c.Assert(err, IsNil)

	err = sysVar.SetSessionFromHook(vars, "OFF") // default is on
	c.Assert(err.Error(), Matches, "set should fail")

	// Test unregistration restores previous count
	UnregisterSysVar("mynewsysvar")
	c.Assert(count, Equals, len(GetSysVars()))
}

func (*testSysVarSuite) TestIntValidation(c *C) {
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: "123", Type: TypeInt, MinValue: 10, MaxValue: 300, AllowAutoValue: true}
	vars := NewSessionVars()

	_, err := sv.Validate(vars, "oN", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'")

	_, err = sv.Validate(vars, "301", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of '301'")

	_, err = sv.Validate(vars, "5", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of '5'")

	val, err := sv.Validate(vars, "300", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "300")

	// out of range but permitted due to auto value
	val, err = sv.Validate(vars, "-1", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "-1")
}

func (*testSysVarSuite) TestUintValidation(c *C) {
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: "123", Type: TypeUnsigned, MinValue: 10, MaxValue: 300, AllowAutoValue: true}
	vars := NewSessionVars()

	_, err := sv.Validate(vars, "oN", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'")

	_, err = sv.Validate(vars, "", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:1232]Incorrect argument type to variable 'mynewsysvar'")

	_, err = sv.Validate(vars, "301", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of '301'")

	_, err = sv.Validate(vars, "-301", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of '-301'")

	_, err = sv.Validate(vars, "-ERR", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of '-ERR'")

	_, err = sv.Validate(vars, "5", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of '5'")

	val, err := sv.Validate(vars, "300", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "300")

	// out of range but permitted due to auto value
	val, err = sv.Validate(vars, "-1", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "-1")
}

func (*testSysVarSuite) TestEnumValidation(c *C) {
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: On, Type: TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	vars := NewSessionVars()

	_, err := sv.Validate(vars, "randomstring", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:1231]Variable 'mynewsysvar' can't be set to the value of 'randomstring'")

	val, err := sv.Validate(vars, "oFf", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "OFF")

	val, err = sv.Validate(vars, "On", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "ON")

	val, err = sv.Validate(vars, "auto", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "AUTO")

	// Also settable by numeric offset.
	val, err = sv.Validate(vars, "2", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "AUTO")
}

func (*testSysVarSuite) TestSynonyms(c *C) {
	sysVar := GetSysVar(TxnIsolation)
	c.Assert(sysVar, NotNil)

	vars := NewSessionVars()

	// It does not permit SERIALIZABLE by default.
	_, err := sysVar.Validate(vars, "SERIALIZABLE", ScopeSession)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[variable:8048]The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error")

	// Enable Skip isolation check
	c.Assert(GetSysVar(TiDBSkipIsolationLevelCheck).SetSessionFromHook(vars, "ON"), IsNil)

	// Serializable is now permitted.
	_, err = sysVar.Validate(vars, "SERIALIZABLE", ScopeSession)
	c.Assert(err, IsNil)

	// Currently TiDB returns a warning because of SERIALIZABLE, but in future
	// it may also return a warning because TxnIsolation is deprecated.

	warn := vars.StmtCtx.GetWarnings()[0].Err
	c.Assert(warn.Error(), Equals, "[variable:8048]The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error")

	c.Assert(sysVar.SetSessionFromHook(vars, "SERIALIZABLE"), IsNil)

	// When we set TxnIsolation, it also updates TransactionIsolation.
	c.Assert(vars.systems[TxnIsolation], Equals, "SERIALIZABLE")
	c.Assert(vars.systems[TransactionIsolation], Equals, vars.systems[TxnIsolation])
}

func (*testSysVarSuite) TestDeprecation(c *C) {
	sysVar := GetSysVar(TiDBIndexLookupConcurrency)
	c.Assert(sysVar, NotNil)

	vars := NewSessionVars()

	_, err := sysVar.Validate(vars, "1234", ScopeSession)
	c.Assert(err, IsNil)

	// There was no error but there is a deprecation warning.
	warn := vars.StmtCtx.GetWarnings()[0].Err
	c.Assert(warn.Error(), Equals, "[variable:1287]'tidb_index_lookup_concurrency' is deprecated and will be removed in a future release. Please use tidb_executor_concurrency instead")
}

func (*testSysVarSuite) TestScope(c *C) {
	sv := SysVar{Scope: ScopeGlobal | ScopeSession, Name: "mynewsysvar", Value: On, Type: TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	c.Assert(sv.HasSessionScope(), IsTrue)
	c.Assert(sv.HasGlobalScope(), IsTrue)
	c.Assert(sv.HasNoneScope(), IsFalse)

	sv = SysVar{Scope: ScopeGlobal, Name: "mynewsysvar", Value: On, Type: TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	c.Assert(sv.HasSessionScope(), IsFalse)
	c.Assert(sv.HasGlobalScope(), IsTrue)
	c.Assert(sv.HasNoneScope(), IsFalse)

	sv = SysVar{Scope: ScopeSession, Name: "mynewsysvar", Value: On, Type: TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	c.Assert(sv.HasSessionScope(), IsTrue)
	c.Assert(sv.HasGlobalScope(), IsFalse)
	c.Assert(sv.HasNoneScope(), IsFalse)

	sv = SysVar{Scope: ScopeNone, Name: "mynewsysvar", Value: On, Type: TypeEnum, PossibleValues: []string{"OFF", "ON", "AUTO"}}
	c.Assert(sv.HasSessionScope(), IsFalse)
	c.Assert(sv.HasGlobalScope(), IsFalse)
	c.Assert(sv.HasNoneScope(), IsTrue)
}

func (*testSysVarSuite) TestBuiltInCase(c *C) {
	// All Sysvars should have lower case names.
	// This tests builtins.
	for name := range GetSysVars() {
		c.Assert(name, Equals, strings.ToLower(name))
	}
}

func (*testSysVarSuite) TestSQLSelectLimit(c *C) {
	sv := GetSysVar(SQLSelectLimit)
	vars := NewSessionVars()
	val, err := sv.Validate(vars, "-10", ScopeSession)
	c.Assert(err, IsNil) // it has autoconvert out of range.
	c.Assert(val, Equals, "0")

	val, err = sv.Validate(vars, "9999", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "9999")

	c.Assert(sv.SetSessionFromHook(vars, "9999"), IsNil) // sets
	c.Assert(vars.SelectLimit, Equals, uint64(9999))
}

func (*testSysVarSuite) TestSQLModeVar(c *C) {
	sv := GetSysVar(SQLModeVar)
	vars := NewSessionVars()
	val, err := sv.Validate(vars, "strict_trans_tabLES  ", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "STRICT_TRANS_TABLES")

	_, err = sv.Validate(vars, "strict_trans_tabLES,nonsense_option", ScopeSession)
	c.Assert(err.Error(), Equals, "ERROR 1231 (42000): Variable 'sql_mode' can't be set to the value of 'NONSENSE_OPTION'")

	val, err = sv.Validate(vars, "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION")

	c.Assert(sv.SetSessionFromHook(vars, val), IsNil) // sets to strict from above
	c.Assert(vars.StrictSQLMode, IsTrue)

	sqlMode, err := mysql.GetSQLMode(val)
	c.Assert(err, IsNil)
	c.Assert(vars.SQLMode, Equals, sqlMode)

	// Set it to non strict.
	val, err = sv.Validate(vars, "ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION")

	c.Assert(sv.SetSessionFromHook(vars, val), IsNil) // sets to non-strict from above
	c.Assert(vars.StrictSQLMode, IsFalse)
	sqlMode, err = mysql.GetSQLMode(val)
	c.Assert(err, IsNil)
	c.Assert(vars.SQLMode, Equals, sqlMode)
}

func (*testSysVarSuite) TestMaxExecutionTime(c *C) {
	sv := GetSysVar(MaxExecutionTime)
	vars := NewSessionVars()

	val, err := sv.Validate(vars, "-10", ScopeSession)
	c.Assert(err, IsNil) // it has autoconvert out of range.
	c.Assert(val, Equals, "0")

	val, err = sv.Validate(vars, "99999", ScopeSession)
	c.Assert(err, IsNil) // it has autoconvert out of range.
	c.Assert(val, Equals, "99999")

	c.Assert(sv.SetSessionFromHook(vars, "99999"), IsNil) // sets
	c.Assert(vars.MaxExecutionTime, Equals, uint64(99999))
}

func (*testSysVarSuite) TestCollationServer(c *C) {
	sv := GetSysVar(CollationServer)
	vars := NewSessionVars()

	val, err := sv.Validate(vars, "LATIN1_bin", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "latin1_bin") // test normalization

	_, err = sv.Validate(vars, "BOGUSCOLLation", ScopeSession)
	c.Assert(err.Error(), Equals, "[ddl:1273]Unknown collation: 'BOGUSCOLLation'")

	c.Assert(sv.SetSessionFromHook(vars, "latin1_bin"), IsNil)
	c.Assert(vars.systems[CharacterSetServer], Equals, "latin1") // check it also changes charset.

	c.Assert(sv.SetSessionFromHook(vars, "utf8mb4_bin"), IsNil)
	c.Assert(vars.systems[CharacterSetServer], Equals, "utf8mb4") // check it also changes charset.
}

func (*testSysVarSuite) TestTimeZone(c *C) {
	sv := GetSysVar(TimeZone)
	vars := NewSessionVars()

	// TiDB uses the Golang TZ library, so TZs are case-sensitive.
	// Unfortunately this is not strictly MySQL compatible. i.e.
	// This should not fail:
	// val, err := sv.Validate(vars, "America/EDMONTON", ScopeSession)
	// See: https://github.com/pingcap/tidb/issues/8087

	val, err := sv.Validate(vars, "America/Edmonton", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "America/Edmonton")

	val, err = sv.Validate(vars, "+10:00", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "+10:00")

	val, err = sv.Validate(vars, "UTC", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "UTC")

	val, err = sv.Validate(vars, "+00:00", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "+00:00")

	c.Assert(sv.SetSessionFromHook(vars, "UTC"), IsNil) // sets
	tz, err := parseTimeZone("UTC")
	c.Assert(err, IsNil)
	c.Assert(vars.TimeZone, Equals, tz)

}

func (*testSysVarSuite) TestForeignKeyChecks(c *C) {
	sv := GetSysVar(ForeignKeyChecks)
	vars := NewSessionVars()

	val, err := sv.Validate(vars, "on", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "OFF") // warns and refuses to set ON.

	warn := vars.StmtCtx.GetWarnings()[0].Err
	c.Assert(warn.Error(), Equals, "[variable:8047]variable 'foreign_key_checks' does not yet support value: on")

}

func (*testSysVarSuite) TestTxnIsolation(c *C) {
	sv := GetSysVar(TxnIsolation)
	vars := NewSessionVars()

	_, err := sv.Validate(vars, "on", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:1231]Variable 'tx_isolation' can't be set to the value of 'on'")

	val, err := sv.Validate(vars, "read-COMMitted", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "READ-COMMITTED")

	_, err = sv.Validate(vars, "Serializable", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:8048]The isolation level 'SERIALIZABLE' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error")

	_, err = sv.Validate(vars, "read-uncommitted", ScopeSession)
	c.Assert(err.Error(), Equals, "[variable:8048]The isolation level 'READ-UNCOMMITTED' is not supported. Set tidb_skip_isolation_level_check=1 to skip this error")

	vars.systems[TiDBSkipIsolationLevelCheck] = "ON"

	val, err = sv.Validate(vars, "Serializable", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "SERIALIZABLE")
}

func (*testSysVarSuite) TestTiDBMultiStatementMode(c *C) {
	sv := GetSysVar(TiDBMultiStatementMode)
	vars := NewSessionVars()

	val, err := sv.Validate(vars, "on", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "ON")
	c.Assert(sv.SetSessionFromHook(vars, val), IsNil)
	c.Assert(vars.MultiStatementMode, Equals, 1)

	val, err = sv.Validate(vars, "0", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "OFF")
	c.Assert(sv.SetSessionFromHook(vars, val), IsNil)
	c.Assert(vars.MultiStatementMode, Equals, 0)

	val, err = sv.Validate(vars, "Warn", ScopeSession)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "WARN")
	c.Assert(sv.SetSessionFromHook(vars, val), IsNil)
	c.Assert(vars.MultiStatementMode, Equals, 2)
}

func (*testSysVarSuite) TestReadOnlyNoop(c *C) {
	vars := NewSessionVars()
	for _, name := range []string{TxReadOnly, TransactionReadOnly} {
		sv := GetSysVar(name)
		val, err := sv.Validate(vars, "on", ScopeSession)
		c.Assert(err.Error(), Equals, "[variable:1235]function READ ONLY has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions")
		c.Assert(val, Equals, "OFF")
	}
}

func (*testSysVarSuite) TestSkipInit(c *C) {
	sv := SysVar{Scope: ScopeGlobal, Name: "skipinit1", Value: On, Type: TypeBool}
	c.Assert(sv.SkipInit(), IsTrue)

	sv = SysVar{Scope: ScopeGlobal | ScopeSession, Name: "skipinit1", Value: On, Type: TypeBool}
	c.Assert(sv.SkipInit(), IsFalse)

	sv = SysVar{Scope: ScopeSession, Name: "skipinit1", Value: On, Type: TypeBool}
	c.Assert(sv.SkipInit(), IsFalse)

	sv = SysVar{Scope: ScopeSession, Name: "skipinit1", Value: On, Type: TypeBool, skipInit: true}
	c.Assert(sv.SkipInit(), IsTrue)
}

// IsNoop is used by the documentation to auto-generate docs for real sysvars.
func (*testSysVarSuite) TestIsNoop(c *C) {
	sv := GetSysVar(TiDBMultiStatementMode)
	c.Assert(sv.IsNoop, IsFalse)

	sv = GetSysVar(InnodbLockWaitTimeout)
	c.Assert(sv.IsNoop, IsFalse)

	sv = GetSysVar(InnodbFastShutdown)
	c.Assert(sv.IsNoop, IsTrue)

	sv = GetSysVar(ReadOnly)
	c.Assert(sv.IsNoop, IsTrue)
}

<<<<<<< HEAD
func (*testSysVarSuite) TestInstanceScopedVars(c *C) {
=======
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
>>>>>>> 1a146fabd... variables: add constraints on tidb_super_read_only when tidb_restricted_read_only is turned on (#31746)
	// This tests instance scoped variables through GetSessionOrGlobalSystemVar().
	// Eventually these should be changed to use getters so that the switch
	// statement in GetSessionOnlySysVars can be removed.

	vars := NewSessionVars()

	val, err := GetSessionOrGlobalSystemVar(vars, TiDBCurrentTS)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, fmt.Sprintf("%d", vars.TxnCtx.StartTS))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBLastTxnInfo)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, vars.LastTxnInfo)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBLastQueryInfo)
	c.Assert(err, IsNil)
	info, err := json.Marshal(vars.LastQueryInfo)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, string(info))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBGeneralLog)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, BoolToOnOff(ProcessGeneralLog.Load()))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBPProfSQLCPU)
	c.Assert(err, IsNil)
	expected := "0"
	if EnablePProfSQLCPU.Load() {
		expected = "1"
	}
	c.Assert(val, Equals, expected)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBExpensiveQueryTimeThreshold)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, fmt.Sprintf("%d", atomic.LoadUint64(&ExpensiveQueryTimeThreshold)))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBMemoryUsageAlarmRatio)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, fmt.Sprintf("%g", MemoryUsageAlarmRatio.Load()))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBForcePriority)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, mysql.Priority2Str[mysql.PriorityEnum(atomic.LoadInt32(&ForcePriority))])

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBDDLSlowOprThreshold)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, strconv.FormatUint(uint64(atomic.LoadUint32(&DDLSlowOprThreshold)), 10))

	val, err = GetSessionOrGlobalSystemVar(vars, PluginDir)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, config.GetGlobalConfig().Plugin.Dir)

	val, err = GetSessionOrGlobalSystemVar(vars, PluginLoad)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, config.GetGlobalConfig().Plugin.Load)

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBSlowLogThreshold)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Log.SlowThreshold), 10))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBRecordPlanInSlowLog)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, strconv.FormatUint(uint64(atomic.LoadUint32(&config.GetGlobalConfig().Log.RecordPlanInSlowLog)), 10))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBEnableSlowLog)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, BoolToOnOff(config.GetGlobalConfig().Log.EnableSlowLog))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBQueryLogMaxLen)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, strconv.FormatUint(atomic.LoadUint64(&config.GetGlobalConfig().Log.QueryLogMaxLen), 10))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBCheckMb4ValueInUTF8)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, BoolToOnOff(config.GetGlobalConfig().CheckMb4ValueInUTF8))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBCapturePlanBaseline)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, CapturePlanBaseline.GetVal())

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBFoundInPlanCache)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, BoolToOnOff(vars.PrevFoundInPlanCache))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBFoundInBinding)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, BoolToOnOff(vars.PrevFoundInBinding))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBEnableCollectExecutionInfo)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, BoolToOnOff(config.GetGlobalConfig().EnableCollectExecutionInfo))

	val, err = GetSessionOrGlobalSystemVar(vars, TiDBTxnScope)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, vars.TxnScope.GetVarValue())
}

// Test that sysvars defaults are logically valid. i.e.
// the default itself must validate without error provided the scope and read-only is correct.
// The default values should also be normalized for consistency.
func (*testSysVarSuite) TestDefaultValuesAreSettable(c *C) {
	vars := NewSessionVars()
	for _, sv := range GetSysVars() {
		if sv.HasSessionScope() && !sv.ReadOnly {
			val, err := sv.Validate(vars, sv.Value, ScopeSession)
			c.Assert(sv.Value, Equals, val)
			c.Assert(err, IsNil)
		}

		if sv.HasGlobalScope() && !sv.ReadOnly {
			if sv.Name == TiDBEnableNoopFuncs {
				// TODO: this requires access to the global var accessor,
				// which is not available in this test.
				continue
			}
			val, err := sv.Validate(vars, sv.Value, ScopeGlobal)
			c.Assert(sv.Value, Equals, val)
			c.Assert(err, IsNil)
		}
	}
}

// This tests that sysvars are logically correct with getter and setter functions.
// i.e. it doesn't make sense to have a SetSession function on a variable that is only globally scoped.
func (*testSysVarSuite) TestSettersandGetters(c *C) {
	for _, sv := range GetSysVars() {
		if !sv.HasSessionScope() {
			// There are some historial exceptions where global variables are loaded into the session.
			// Please don't add to this list, the behavior is not MySQL compatible.
			switch sv.Name {
			case TiDBEnableChangeMultiSchema, TiDBDDLReorgBatchSize, TiDBEnableAlterPlacement,
				TiDBMaxDeltaSchemaCount, InitConnect, MaxPreparedStmtCount,
				TiDBDDLReorgWorkerCount, TiDBDDLErrorCountLimit, TiDBRowFormatVersion,
				TiDBEnableTelemetry, TiDBEnablePointGetCache:
				continue
			}
			c.Assert(sv.SetSession, IsNil)
			c.Assert(sv.GetSession, IsNil)
		}
		if !sv.HasGlobalScope() {
			c.Assert(sv.SetGlobal, IsNil)
			c.Assert(sv.GetGlobal, IsNil)
		}
	}
}

func (*testSysVarSuite) TestSecureAuth(c *C) {
	sv := GetSysVar(SecureAuth)
	vars := NewSessionVars()
	_, err := sv.Validate(vars, "OFF", ScopeGlobal)
	c.Assert(err.Error(), Equals, "[variable:1231]Variable 'secure_auth' can't be set to the value of 'OFF'")
	val, err := sv.Validate(vars, "ON", ScopeGlobal)
	c.Assert(err, IsNil)
	c.Assert(val, Equals, "ON")
}

func (*testSysVarSuite) TestValidateWithRelaxedValidation(c *C) {
	sv := GetSysVar(SecureAuth)
	vars := NewSessionVars()
	val := sv.ValidateWithRelaxedValidation(vars, "1", ScopeGlobal)
	c.Assert(val, Equals, "ON")
}
