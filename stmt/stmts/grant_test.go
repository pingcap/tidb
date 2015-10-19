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
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/mysql"
)

func (s *testStmtSuite) TestGrantGlobal(c *C) {
	tx := mustBegin(c, s.testDB)
	// Create a new user.
	createUserSQL := `CREATE USER 'testGlobal'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	mustCommit(c, tx)
	// Make sure all the global privs for new user is "N".
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testDB\" and host=\"localhost\";", mysql.Priv2UserCol[v])
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(sql)
		c.Assert(err, IsNil)
		matchRows(c, rows, [][]interface{}{{"N"}})
		rows.Close()
		mustCommit(c, tx)
	}

	// Grant each priv to the user.
	for _, v := range mysql.AllGlobalPrivs {
		sql := fmt.Sprintf("GRANT %s ON *.* TO 'testGlobal'@'localhost';", mysql.Priv2Str[v])
		mustExec(c, s.testDB, sql)
		sql = fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testGlobal\" and host=\"localhost\"", mysql.Priv2UserCol[v])
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(sql)
		c.Assert(err, IsNil)
		matchRows(c, rows, [][]interface{}{{"Y"}})
		rows.Close()
		mustCommit(c, tx)
	}

	tx = mustBegin(c, s.testDB)
	// Create a new user.
	createUserSQL = `CREATE USER 'testGlobal1'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	mustExec(c, s.testDB, "GRANT ALL ON *.* TO 'testGlobal1'@'localhost';")
	mustCommit(c, tx)
	// Make sure all the global privs for granted user is "Y".
	for _, v := range mysql.AllGlobalPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testGlobal1\" and host=\"localhost\"", mysql.Priv2UserCol[v])
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(sql)
		c.Assert(err, IsNil)
		matchRows(c, rows, [][]interface{}{{"Y"}})
		rows.Close()
		mustCommit(c, tx)
	}
}

func (s *testStmtSuite) TestGrantDBScope(c *C) {
	tx := mustBegin(c, s.testDB)
	// Create a new user.
	createUserSQL := `CREATE USER 'testDB'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	mustCommit(c, tx)
	// Make sure all the db privs for new user is empty.
	sql := fmt.Sprintf("SELECT * FROM mysql.db WHERE User=\"testDB\" and host=\"localhost\"")
	tx = mustBegin(c, s.testDB)
	rows, err := tx.Query(sql)
	c.Assert(err, IsNil)
	c.Assert(rows.Next(), IsFalse)
	mustCommit(c, tx)

	// Grant each priv to the user.
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf("GRANT %s ON test.* TO 'testDB'@'localhost';", mysql.Priv2Str[v])
		mustExec(c, s.testDB, sql)
		sql = fmt.Sprintf("SELECT %s FROM mysql.DB WHERE User=\"testDB\" and host=\"localhost\" and db=\"test\"", mysql.Priv2UserCol[v])
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(sql)
		c.Assert(err, IsNil)
		matchRows(c, rows, [][]interface{}{{"Y"}})
		rows.Close()
		mustCommit(c, tx)
	}

	tx = mustBegin(c, s.testDB)
	// Create a new user.
	createUserSQL = `CREATE USER 'testDB1'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	mustExec(c, s.testDB, "USE test;")
	mustExec(c, s.testDB, "GRANT ALL ON * TO 'testDB1'@'localhost';")
	mustCommit(c, tx)
	// Make sure all the db privs for granted user is "Y".
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.DB WHERE User=\"testDB1\" and host=\"localhost\" and db=\"test\";", mysql.Priv2UserCol[v])
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(sql)
		c.Assert(err, IsNil)
		matchRows(c, rows, [][]interface{}{{"Y"}})
		rows.Close()
		mustCommit(c, tx)
	}
}

func (s *testStmtSuite) TestTableScope(c *C) {
	tx := mustBegin(c, s.testDB)
	// Create a new user.
	createUserSQL := `CREATE USER 'testTbl'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	mustExec(c, s.testDB, `CREATE TABLE test.test1(c1 int);`)
	mustCommit(c, tx)
	// Make sure all the table privs for new user is empty.
	tx = mustBegin(c, s.testDB)
	rows, err := tx.Query(`SELECT * FROM mysql.Tables_priv WHERE User="testTbl" and host="localhost" and db="test" and Table_name="test1"`)
	c.Assert(err, IsNil)
	c.Assert(rows.Next(), IsFalse)
	mustCommit(c, tx)

	// Grant each priv to the user.
	for _, v := range mysql.AllTablePrivs {
		sql := fmt.Sprintf("GRANT %s ON test.test1 TO 'testTbl'@'localhost';", mysql.Priv2Str[v])
		mustExec(c, s.testDB, sql)
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(`SELECT Table_priv FROM mysql.Tables_priv WHERE User="testTbl" and host="localhost" and db="test" and Table_name="test1";`)
		c.Assert(err, IsNil)
		rows.Next()
		var p string
		rows.Scan(&p)
		c.Assert(strings.Index(p, mysql.Priv2SetStr[v]), Greater, -1)
		c.Assert(rows.Next(), IsFalse)
		rows.Close()
		mustCommit(c, tx)
	}

	tx = mustBegin(c, s.testDB)
	// Create a new user.
	createUserSQL = `CREATE USER 'testTbl1'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	mustExec(c, s.testDB, "USE test;")
	mustExec(c, s.testDB, `CREATE TABLE test2(c1 int);`)
	// Grant all table scope privs.
	mustExec(c, s.testDB, "GRANT ALL ON test2 TO 'testTbl1'@'localhost';")
	mustCommit(c, tx)
	// Make sure all the table privs for granted user are in the Table_priv set.
	for _, v := range mysql.AllTablePrivs {
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(`SELECT Table_priv FROM mysql.Tables_priv WHERE User="testTbl1" and host="localhost" and db="test" and Table_name="test2";`)
		c.Assert(err, IsNil)
		rows.Next()
		var p string
		rows.Scan(&p)
		c.Assert(strings.Index(p, mysql.Priv2SetStr[v]), Greater, -1)
		c.Assert(rows.Next(), IsFalse)
		rows.Close()
		mustCommit(c, tx)
	}
}

func (s *testStmtSuite) TestColumnScope(c *C) {
	tx := mustBegin(c, s.testDB)
	// Create a new user.
	createUserSQL := `CREATE USER 'testCol'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	mustExec(c, s.testDB, `CREATE TABLE test.test3(c1 int, c2 int);`)
	mustCommit(c, tx)

	// Make sure all the column privs for new user is empty.
	tx = mustBegin(c, s.testDB)
	rows, err := tx.Query(`SELECT * FROM mysql.Columns_priv WHERE User="testCol" and host="localhost" and db="test" and Table_name="test3" and Column_name="c1"`)
	c.Assert(err, IsNil)
	c.Assert(rows.Next(), IsFalse)
	mustCommit(c, tx)
	tx = mustBegin(c, s.testDB)
	rows, err = tx.Query(`SELECT * FROM mysql.Columns_priv WHERE User="testCol" and host="localhost" and db="test" and Table_name="test3" and Column_name="c2"`)
	c.Assert(err, IsNil)
	c.Assert(rows.Next(), IsFalse)
	mustCommit(c, tx)

	// Grant each priv to the user.
	for _, v := range mysql.AllColumnPrivs {
		sql := fmt.Sprintf("GRANT %s(c1) ON test.test3 TO 'testCol'@'localhost';", mysql.Priv2Str[v])
		mustExec(c, s.testDB, sql)
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(`SELECT Column_priv FROM mysql.Columns_priv WHERE User="testCol" and host="localhost" and db="test" and Table_name="test3" and Column_name="c1";`)
		c.Assert(err, IsNil)
		rows.Next()
		var p string
		rows.Scan(&p)
		c.Assert(strings.Index(p, mysql.Priv2SetStr[v]), Greater, -1)
		c.Assert(rows.Next(), IsFalse)
		rows.Close()
		mustCommit(c, tx)
	}

	tx = mustBegin(c, s.testDB)
	// Create a new user.
	createUserSQL = `CREATE USER 'testCol1'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	mustExec(c, s.testDB, "USE test;")
	// Grant all column scope privs.
	mustExec(c, s.testDB, "GRANT ALL(c2) ON test3 TO 'testCol1'@'localhost';")
	mustCommit(c, tx)
	// Make sure all the column privs for granted user are in the Column_priv set.
	for _, v := range mysql.AllColumnPrivs {
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(`SELECT Column_priv FROM mysql.Columns_priv WHERE User="testCol1" and host="localhost" and db="test" and Table_name="test3" and Column_name="c2";`)
		c.Assert(err, IsNil)
		rows.Next()
		var p string
		rows.Scan(&p)
		c.Assert(strings.Index(p, mysql.Priv2SetStr[v]), Greater, -1)
		c.Assert(rows.Next(), IsFalse)
		rows.Close()
		mustCommit(c, tx)
	}
}
