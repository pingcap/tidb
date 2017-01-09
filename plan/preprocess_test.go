// Copyright 2017 PingCAP, Inc.
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

package plan_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/util/testkit"
)

type testCheckPrivilegeSuite struct {
	*parser.Parser
}

var _ = Suite(&testCheckPrivilegeSuite{})

func (s *testCheckPrivilegeSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
}

func (s *testCheckPrivilegeSuite) TestCheckPrivilege(c *C) {
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	defer store.Close()
	testKit := testkit.NewTestKit(c, store)
	testKit.MustExec("use test")

	ctx := testKit.Se.(context.Context)
	ctx.GetSessionVars().CurrentDB = "test"

	testcases := []struct {
		src   string
		valid bool
	}{
		{"select * from t1", true},
		{"create table t (v1 int, v2 int)", true},
	}

	for _, tc := range testcases {
		node, err := s.ParseOneStmt(tc.src, "", "")
		c.Assert(err, IsNil)

		plan.CheckPrivilege(node, ctx, privilege.GetPrivilegeChecker(ctx))
	}
}
