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
	"fmt"
	"strings"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
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
	normalizedVal, err = sysVar.Validate(vars, "0", ScopeSession)
	c.Assert(normalizedVal, Equals, "OFF")

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
