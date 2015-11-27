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
	"github.com/pingcap/tidb/stmt/stmts"
)

func (s *testStmtSuite) TestDropDatabase(c *C) {
	testSQL := "drop database if exists drop_test;"

	stmtList, err := tidb.Compile(s.ctx, testSQL)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)

	testStmt, ok := stmtList[0].(*stmts.DropDatabaseStmt)
	c.Assert(ok, IsTrue)

	c.Assert(testStmt.IsDDL(), IsTrue)
	c.Assert(len(testStmt.OriginText()), Greater, 0)

	mf := newMockFormatter()
	testStmt.Explain(nil, mf)
	c.Assert(mf.Len(), Greater, 0)

	mustExec(c, s.testDB, testSQL)
}

func (s *testStmtSuite) TestDropTable(c *C) {
	testSQL := "drop table if exists drop_table;"

	stmtList, err := tidb.Compile(s.ctx, testSQL)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)

	testStmt, ok := stmtList[0].(*stmts.DropTableStmt)
	c.Assert(ok, IsTrue)

	c.Assert(testStmt.IsDDL(), IsTrue)
	c.Assert(len(testStmt.OriginText()), Greater, 0)

	mf := newMockFormatter()
	testStmt.Explain(nil, mf)
	c.Assert(mf.Len(), Greater, 0)

	mustExec(c, s.testDB, testSQL)
	mustExec(c, s.testDB, "create table if not exists t (c int)")
	mustExec(c, s.testDB, "drop table t")
}

func (s *testStmtSuite) TestDropIndex(c *C) {
	testSQL := "drop index if exists drop_index on t;"

	stmtList, err := tidb.Compile(s.ctx, testSQL)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)

	testStmt, ok := stmtList[0].(*stmts.DropIndexStmt)
	c.Assert(ok, IsTrue)

	c.Assert(testStmt.IsDDL(), IsTrue)
	c.Assert(len(testStmt.OriginText()), Greater, 0)

	mf := newMockFormatter()
	testStmt.Explain(nil, mf)
	c.Assert(mf.Len(), Greater, 0)

	mustExec(c, s.testDB, testSQL)
}
