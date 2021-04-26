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
