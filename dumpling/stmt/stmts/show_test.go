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

func (s *testStmtSuite) TestShow(c *C) {
	testSQL := `drop table if exists show_test; create table show_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);`
	mustExec(c, s.testDB, testSQL)

	testSQL = "show columns from show_test;"
	stmtList, err := tidb.Compile(s.ctx, testSQL)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)

	testStmt, ok := stmtList[0].(*stmts.ShowStmt)
	c.Assert(ok, IsTrue)

	c.Assert(testStmt.IsDDL(), IsFalse)
	c.Assert(len(testStmt.OriginText()), Greater, 0)

	testSQL = "show create table show_test;"
	stmtList, err = tidb.Compile(s.ctx, testSQL)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)
	testStmt, ok = stmtList[0].(*stmts.ShowStmt)
	c.Assert(ok, IsTrue)

	mf := newMockFormatter()
	testStmt.Explain(nil, mf)
	c.Assert(mf.Len(), Greater, 0)

	testSQL = "SHOW VARIABLES LIKE 'character_set_results';"
	stmtList, err = tidb.Compile(s.ctx, testSQL)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)
	testStmt, ok = stmtList[0].(*stmts.ShowStmt)
	c.Assert(ok, IsTrue)
	c.Assert(testStmt.Pattern, NotNil, Commentf("S: %s", testStmt.Text))
}
