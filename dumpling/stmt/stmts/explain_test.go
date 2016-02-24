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

package stmts_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/stmt/stmts"
	"github.com/pingcap/tidb/util/mock"
)

func (s *testStmtSuite) TestExplain(c *C) {
	testSQL := "explain select 1"

	rawStmtList, err := tidb.Parse(s.ctx, testSQL)
	c.Assert(err, IsNil)
	c.Assert(rawStmtList, HasLen, 1)
	stmtList, err := tidb.Compile(s.ctx, rawStmtList)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)

	testStmt, ok := stmtList[0].(*stmts.ExplainStmt)
	c.Assert(ok, IsTrue)

	c.Assert(testStmt.IsDDL(), IsFalse)
	c.Assert(len(testStmt.OriginText()), Greater, 0)

	newTestSql := "explain " + testSQL
	newTestStmt := &stmts.ExplainStmt{S: testStmt, Text: newTestSql}

	mf := newMockFormatter()
	ctx := mock.NewContext()
	variable.BindSessionVars(ctx)
	newTestStmt.Explain(ctx, mf)
	c.Assert(mf.Len(), Greater, 0)

	_, err = testStmt.Exec(ctx)
	c.Assert(err, IsNil)

	showColumnSQL := "desc t;"

	rawStmtList, err = tidb.Parse(s.ctx, showColumnSQL)
	c.Assert(err, IsNil)
	c.Assert(rawStmtList, HasLen, 1)
	stmtList, err = tidb.Compile(s.ctx, rawStmtList)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)

	testStmt, ok = stmtList[0].(*stmts.ExplainStmt)
	c.Assert(ok, IsTrue)

	showStmt, ok := testStmt.S.(*stmts.ShowStmt)
	c.Assert(ok, IsTrue)

	// Mock DBName for ShowStmt
	showStmt.DBName = "test"

	mf = newMockFormatter()
	testStmt.Explain(ctx, mf)
	c.Assert(mf.Len(), Greater, 0)

	_, err = testStmt.Exec(ctx)
	c.Assert(err, IsNil)
}
