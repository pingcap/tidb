// Copyright 2020 PingCAP, Inc.
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

package stmtsummary

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
)

var _ = Suite(&testVariablesSuite{})

type testVariablesSuite struct {
}

func (s *testVariablesSuite) TestSetInVariable(c *C) {
	sv := newSysVars()
	st := sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(config.GetGlobalConfig().StmtSummary.MaxStmtCount))

	err := sv.setVariable(typeMaxStmtCount, "10", false)
	c.Assert(err, IsNil)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(10))
	err = sv.setVariable(typeMaxStmtCount, "100", false)
	c.Assert(err, IsNil)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(100))
	err = sv.setVariable(typeMaxStmtCount, "10", true)
	c.Assert(err, IsNil)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(10))
	err = sv.setVariable(typeMaxStmtCount, "100", true)
	c.Assert(err, IsNil)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(100))
	err = sv.setVariable(typeMaxStmtCount, "10", false)
	c.Assert(err, IsNil)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(100))
	err = sv.setVariable(typeMaxStmtCount, "", true)
	c.Assert(err, IsNil)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(10))
	err = sv.setVariable(typeMaxStmtCount, "", false)
	c.Assert(err, IsNil)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(config.GetGlobalConfig().StmtSummary.MaxStmtCount))
}

func (s *testVariablesSuite) TestSetBoolVariable(c *C) {
	sv := newSysVars()
	en := sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, config.GetGlobalConfig().StmtSummary.Enable)

	err := sv.setVariable(typeEnable, "OFF", false)
	c.Assert(err, IsNil)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, false)
	err = sv.setVariable(typeEnable, "ON", false)
	c.Assert(err, IsNil)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, true)
	err = sv.setVariable(typeEnable, "OFF", true)
	c.Assert(err, IsNil)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, false)
	err = sv.setVariable(typeEnable, "ON", true)
	c.Assert(err, IsNil)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, true)
	err = sv.setVariable(typeEnable, "OFF", false)
	c.Assert(err, IsNil)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, true)
	err = sv.setVariable(typeEnable, "", true)
	c.Assert(err, IsNil)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, false)
	err = sv.setVariable(typeEnable, "ON", false)
	c.Assert(err, IsNil)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, true)
	err = sv.setVariable(typeEnable, "", false)
	c.Assert(err, IsNil)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, config.GetGlobalConfig().StmtSummary.Enable)
}

func (s *testVariablesSuite) TestMinValue(c *C) {
	sv := newSysVars()
	err := sv.setVariable(typeMaxStmtCount, "0", false)
	c.Assert(err, IsNil)
	v := sv.getVariable(typeMaxStmtCount)
	c.Assert(v, Greater, int64(0))

	err = sv.setVariable(typeMaxSQLLength, "0", false)
	c.Assert(err, IsNil)
	v = sv.getVariable(typeMaxSQLLength)
	c.Assert(v, Equals, int64(0))

	err = sv.setVariable(typeHistorySize, "0", false)
	c.Assert(err, IsNil)
	v = sv.getVariable(typeHistorySize)
	c.Assert(v, Equals, int64(0))

	err = sv.setVariable(typeRefreshInterval, "0", false)
	c.Assert(err, IsNil)
	v = sv.getVariable(typeRefreshInterval)
	c.Assert(v, Greater, int64(0))
}
