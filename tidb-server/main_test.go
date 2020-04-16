// Copyright 2019 PingCAP, Inc.
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

package main

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
)

var isCoverageServer = "0"

// TestRunMain is a dummy test case, which contains only the main function of tidb-server,
// and it is used to generate coverage_server.
func TestRunMain(t *testing.T) {
	if isCoverageServer == "1" {
		main()
	}
}

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testMainSuite{})

type testMainSuite struct{}

func (t *testMainSuite) TestSetGlobalVars(c *C) {
	c.Assert(variable.SysVars[variable.TiDBIsolationReadEngines], Equals, "tikv, tiflash, tidb")
	c.Assert(variable.SysVars[variable.TIDBMemQuotaQuery], Equals, 1 << 30)

	config.GetGlobalConfig().IsolationRead.Engines = []string{"tikv", "tidb"}
	config.GetGlobalConfig().MemQuotaQuery = 9999999
	setGlobalVars()

	c.Assert(variable.SysVars[variable.TiDBIsolationReadEngines], Equals, "tikv, tidb")
	c.Assert(variable.SysVars[variable.TIDBMemQuotaQuery], Equals, 9999999)
}
