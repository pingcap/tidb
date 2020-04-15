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
	"github.com/pingcap/tidb/v4/config"
)

var _ = Suite(&testVariablesSuite{})

type testVariablesSuite struct {
}

func (s *testVariablesSuite) TestSetInVariable(c *C) {
	sv := newSysVars()
	st := sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(config.GetGlobalConfig().StmtSummary.MaxStmtCount))

	sv.setVariable(typeMaxStmtCount, "10", false)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(10))
	sv.setVariable(typeMaxStmtCount, "100", false)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(100))
	sv.setVariable(typeMaxStmtCount, "10", true)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(10))
	sv.setVariable(typeMaxStmtCount, "100", true)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(100))
	sv.setVariable(typeMaxStmtCount, "10", false)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(100))
	sv.setVariable(typeMaxStmtCount, "", true)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(10))
	sv.setVariable(typeMaxStmtCount, "", false)
	st = sv.getVariable(typeMaxStmtCount)
	c.Assert(st, Equals, int64(config.GetGlobalConfig().StmtSummary.MaxStmtCount))
}

func (s *testVariablesSuite) TestSetBoolVariable(c *C) {
	sv := newSysVars()
	en := sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, config.GetGlobalConfig().StmtSummary.Enable)

	sv.setVariable(typeEnable, "OFF", false)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, false)
	sv.setVariable(typeEnable, "ON", false)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, true)
	sv.setVariable(typeEnable, "OFF", true)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, false)
	sv.setVariable(typeEnable, "ON", true)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, true)
	sv.setVariable(typeEnable, "OFF", false)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, true)
	sv.setVariable(typeEnable, "", true)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, false)
	sv.setVariable(typeEnable, "ON", false)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, true)
	sv.setVariable(typeEnable, "", false)
	en = sv.getVariable(typeEnable)
	c.Assert(en > 0, Equals, config.GetGlobalConfig().StmtSummary.Enable)
}

func (s *testVariablesSuite) TestMinValue(c *C) {
	sv := newSysVars()
	sv.setVariable(typeMaxStmtCount, "0", false)
	v := sv.getVariable(typeMaxStmtCount)
	c.Assert(v, Greater, int64(0))

	sv.setVariable(typeMaxSQLLength, "0", false)
	v = sv.getVariable(typeMaxSQLLength)
	c.Assert(v, Equals, int64(0))

	sv.setVariable(typeHistorySize, "0", false)
	v = sv.getVariable(typeHistorySize)
	c.Assert(v, Equals, int64(0))

	sv.setVariable(typeRefreshInterval, "0", false)
	v = sv.getVariable(typeRefreshInterval)
	c.Assert(v, Greater, int64(0))
}
