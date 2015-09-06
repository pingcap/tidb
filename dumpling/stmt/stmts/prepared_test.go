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
	"github.com/pingcap/tidb/stmt/stmts"
)

func (s *testStmtSuite) TestPreparedStmt(c *C) {
	testSQL := `drop table if exists prepare_test;
    create table prepare_test (id int PRIMARY KEY AUTO_INCREMENT, c1 int, c2 int, c3 int default 1);
    insert prepare_test (c1) values (1),(2),(NULL);`
	mustExec(c, s.testDB, testSQL)

	testSQL = `prepare stmt_test_1 from 'select id from prepare_test where id > ?'; set @a = 1; execute stmt_test_1 using @a;`
	mustExec(c, s.testDB, testSQL)

	testSQL = `prepare stmt_test_2 from 'select 1'`
	mustExec(c, s.testDB, testSQL)

	errTestSQL := `prepare stmt_test_3 from 'select id from prepare_test where id > ?;select id from prepare_test where id > ?;'`
	tx := mustBegin(c, s.testDB)
	_, err := tx.Exec(errTestSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	errTestSQL = `prepare stmt_test_4 from 'select id from prepare_test where id > ? and id < ?'; set @a = 1; execute stmt_test_4 using @a;`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errTestSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	testSQL = `prepare stmt_test_5 from 'select id from prepare_test where id > ?'; deallocate prepare stmt_test_5;`
	mustExec(c, s.testDB, testSQL)

	errTestSQL = `set @a = 1; execute stmt_test_5 using @a;`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errTestSQL)
	c.Assert(err, NotNil)
	tx.Rollback()

	// Mock PreparedStmt.
	testPrepareStmt := &stmts.PreparedStmt{}

	mf := newMockFormatter()
	testPrepareStmt.Explain(nil, mf)
	c.Assert(mf.Len(), Greater, 0)

	c.Assert(testPrepareStmt.IsDDL(), IsFalse)
	c.Assert(len(testPrepareStmt.OriginText()), Equals, 0)

	_, err = testPrepareStmt.Exec(nil)
	c.Assert(err, IsNil)

	// Mock DeallocateStmt.
	testDeallocateStmt := &stmts.DeallocateStmt{}

	mf = newMockFormatter()
	testDeallocateStmt.Explain(nil, mf)
	c.Assert(mf.Len(), Greater, 0)

	c.Assert(testDeallocateStmt.IsDDL(), IsFalse)
	c.Assert(len(testDeallocateStmt.OriginText()), Equals, 0)

	// Mock ExecuteStmt.
	testExecuteStmt := &stmts.ExecuteStmt{}

	mf = newMockFormatter()
	testExecuteStmt.Explain(nil, mf)
	c.Assert(mf.Len(), Greater, 0)

	c.Assert(testExecuteStmt.IsDDL(), IsFalse)
	c.Assert(len(testExecuteStmt.OriginText()), Equals, 0)
}
