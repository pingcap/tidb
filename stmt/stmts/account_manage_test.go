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
	"github.com/pingcap/tidb/util"
)

func (s *testStmtSuite) TestCreateUserStmt(c *C) {
	// Make sure user test not in mysql.User.
	tx := mustBegin(c, s.testDB)
	rows, err := tx.Query(`SELECT Password FROM mysql.User WHERE User="test" and Host="localhost"`)
	c.Assert(err, IsNil)
	c.Assert(rows.Next(), IsFalse)
	rows.Close()
	mustCommit(c, tx)
	// Create user test.
	createUserSQL := `CREATE USER 'test'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	// Make sure user test in mysql.User.
	tx = mustBegin(c, s.testDB)
	rows, err = tx.Query(`SELECT Password FROM mysql.User WHERE User="test" and Host="localhost"`)
	c.Assert(err, IsNil)
	rows.Next()
	var pwd string
	rows.Scan(&pwd)
	c.Assert(pwd, Equals, util.EncodePassword("123"))
	c.Assert(rows.Next(), IsFalse)
	rows.Close()
	mustCommit(c, tx)
	// Create duplicate user with IfNotExists will be success.
	createUserSQL = `CREATE USER IF NOT EXISTS 'test'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)

	// Create duplicate user without IfNotExists will cause error.
	createUserSQL = `CREATE USER 'test'@'localhost' IDENTIFIED BY '123';`
	tx = mustBegin(c, s.testDB)
	_, err = tx.Query(createUserSQL)
	c.Assert(err, NotNil)
}

func (s *testStmtSuite) TestSetPwdStmt(c *C) {
	tx := mustBegin(c, s.testDB)
	rows, err := tx.Query(`SELECT Password FROM mysql.User WHERE User="root" and Host="localhost"`)
	c.Assert(err, IsNil)
	rows.Next()
	var pwd string
	rows.Scan(&pwd)
	c.Assert(pwd, Equals, "")
	c.Assert(rows.Next(), IsFalse)
	rows.Close()
	mustCommit(c, tx)

	tx = mustBegin(c, s.testDB)
	tx.Query(`SET PASSWORD FOR 'root'@'localhost' = 'password';`)
	mustCommit(c, tx)

	tx = mustBegin(c, s.testDB)
	rows, err = tx.Query(`SELECT Password FROM mysql.User WHERE User="root" and Host="localhost"`)
	c.Assert(err, IsNil)
	rows.Next()
	rows.Scan(&pwd)
	c.Assert(pwd, Equals, util.EncodePassword("password"))
	c.Assert(rows.Next(), IsFalse)
	rows.Close()
	mustCommit(c, tx)
}
