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
