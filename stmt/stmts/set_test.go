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

func (s *testStmtSuite) TestSet(c *C) {
	testSQL := "SET @a = 1;"
	mustExec(c, s.testDB, testSQL)

	stmtList, err := tidb.Compile(testSQL)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)

	testStmt, ok := stmtList[0].(*stmts.SetStmt)
	c.Assert(ok, IsTrue)

	c.Assert(testStmt.IsDDL(), IsFalse)
	c.Assert(len(testStmt.Variables[0].String()), Greater, 0)
	c.Assert(len(testStmt.OriginText()), Greater, 0)

	mf := newMockFormatter()
	testStmt.Explain(nil, mf)
	c.Assert(mf.Len(), Greater, 0)

	testSQL = `SET @a = "1";`
	mustExec(c, s.testDB, testSQL)

	testSQL = "SET @a = null;"
	mustExec(c, s.testDB, testSQL)

	testSQL = "SET @@global.autocommit = 1;"
	mustExec(c, s.testDB, testSQL)

	stmtList, err = tidb.Compile(testSQL)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)

	testStmt, ok = stmtList[0].(*stmts.SetStmt)
	c.Assert(ok, IsTrue)

	c.Assert(testStmt.IsDDL(), IsFalse)
	c.Assert(len(testStmt.Variables[0].String()), Greater, 0)
	c.Assert(len(testStmt.OriginText()), Greater, 0)

	mf = newMockFormatter()
	testStmt.Explain(nil, mf)
	c.Assert(mf.Len(), Greater, 0)

	testSQL = "SET @@global.autocommit = null;"
	mustExec(c, s.testDB, testSQL)

	testSQL = "SET @@autocommit = 1;"
	mustExec(c, s.testDB, testSQL)

	stmtList, err = tidb.Compile(testSQL)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)

	testStmt, ok = stmtList[0].(*stmts.SetStmt)
	c.Assert(ok, IsTrue)

	c.Assert(testStmt.IsDDL(), IsFalse)
	c.Assert(len(testStmt.Variables[0].String()), Greater, 0)
	c.Assert(len(testStmt.OriginText()), Greater, 0)

	mf = newMockFormatter()
	testStmt.Explain(nil, mf)
	c.Assert(mf.Len(), Greater, 0)

	testSQL = "SET @@autocommit = null;"
	mustExec(c, s.testDB, testSQL)

	errTestSql := "SET @@date_format = 1;"
	tx := mustBegin(c, s.testDB)
	_, err = tx.Exec(errTestSql)
	c.Assert(err, NotNil)
	tx.Rollback()

	errTestSql = "SET @@rewriter_enabled = 1;"
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errTestSql)
	c.Assert(err, NotNil)
	tx.Rollback()

	errTestSql = "SET xxx = abcd;"
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errTestSql)
	c.Assert(err, NotNil)
	tx.Rollback()

	errTestSql = "SET @@global.a = 1;"
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errTestSql)
	c.Assert(err, NotNil)
	tx.Rollback()

	errTestSql = "SET @@global.timestamp = 1;"
	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec(errTestSql)
	c.Assert(err, NotNil)
	tx.Rollback()
}

func (s *testStmtSuite) TestSetCharsetStmt(c *C) {
	testSQL := `SET NAMES utf8;`

	stmtList, err := tidb.Compile(testSQL)
	c.Assert(err, IsNil)
	c.Assert(stmtList, HasLen, 1)

	testStmt, ok := stmtList[0].(*stmts.SetCharsetStmt)
	c.Assert(ok, IsTrue)

	c.Assert(testStmt.IsDDL(), IsFalse)
	c.Assert(len(testStmt.OriginText()), Greater, 0)

	_, err = testStmt.Exec(nil)
	c.Assert(err, IsNil)

	mf := newMockFormatter()
	testStmt.Explain(nil, mf)
	c.Assert(mf.Len(), Greater, 0)
}

func (s *testStmtSuite) TestSetPwdStmt(c *C) {
	// Mock SetPwdStmt.
	testStmt := &stmts.SetPwdStmt{
		User:     "root@localhost",
		Password: "password",
	}

	testStmt.SetText(`SET PASSWORD FOR 'root'@'localhost' = 'password';`)

	c.Assert(testStmt.IsDDL(), IsFalse)
	c.Assert(len(testStmt.OriginText()), Greater, 0)

	_, err := testStmt.Exec(nil)
	c.Assert(err, IsNil)
}
