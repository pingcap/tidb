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

	. "github.com/pingcap/check"
	mysql "github.com/pingcap/tidb/mysqldef"
)

func (s *testStmtSuite) TestGrantGlobal(c *C) {
	tx := mustBegin(c, s.testDB)
	// Create a new user
	createUserSQL := `CREATE USER 'testGlobal'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	mustCommit(c, tx)
	// Make sure all the global privs for new user is "N"
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testDB\" and host=\"localhost\";", mysql.Priv2UserCol[v])
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(sql)
		c.Assert(err, IsNil)
		rows.Next()
		var p string
		rows.Scan(&p)
		c.Assert(p, Equals, "N")
		c.Assert(rows.Next(), IsFalse)
		rows.Close()
		mustCommit(c, tx)
	}

	// Grant each priv to the user
	for _, v := range mysql.AllGlobalPrivs {
		sql := fmt.Sprintf("GRANT %s ON *.* TO 'testGlobal'@'localhost';", mysql.Priv2Str[v])
		mustExec(c, s.testDB, sql)
		sql = fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testGlobal\" and host=\"localhost\"", mysql.Priv2UserCol[v])
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(sql)
		c.Assert(err, IsNil)
		rows.Next()
		var p string
		rows.Scan(&p)
		c.Assert(p, Equals, "Y")
		c.Assert(rows.Next(), IsFalse)
		rows.Close()
		mustCommit(c, tx)
	}

	tx = mustBegin(c, s.testDB)
	// Create a new user
	createUserSQL = `CREATE USER 'testGlobal1'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	mustExec(c, s.testDB, "GRANT ALL ON *.* TO 'testGlobal1'@'localhost';")
	mustCommit(c, tx)
	// Make sure all the global privs for granted user is "Y"
	for _, v := range mysql.AllGlobalPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.User WHERE User=\"testGlobal1\" and host=\"localhost\"", mysql.Priv2UserCol[v])
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(sql)
		c.Assert(err, IsNil)
		rows.Next()
		var p string
		rows.Scan(&p)
		c.Assert(p, Equals, "Y")
		c.Assert(rows.Next(), IsFalse)
		rows.Close()
		mustCommit(c, tx)
	}
}

func (s *testStmtSuite) TestGrantDBScope(c *C) {
	tx := mustBegin(c, s.testDB)
	// Create a new user
	createUserSQL := `CREATE USER 'testDB'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	mustCommit(c, tx)
	// Make sure all the db privs for new user is empty
	sql := fmt.Sprintf("SELECT * FROM mysql.User WHERE User=\"testGlobal\" and host=\"localhost\"")
	tx = mustBegin(c, s.testDB)
	rows, err := tx.Query(sql)
	c.Assert(err, IsNil)
	c.Assert(rows.Next(), IsFalse)
	mustCommit(c, tx)

	// Grant each priv to the user
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf("GRANT %s ON test.* TO 'testDB'@'localhost';", mysql.Priv2Str[v])
		mustExec(c, s.testDB, sql)
		sql = fmt.Sprintf("SELECT %s FROM mysql.DB WHERE User=\"testDB\" and host=\"localhost\" and db=\"test\"", mysql.Priv2UserCol[v])
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(sql)
		c.Assert(err, IsNil)
		rows.Next()
		var p string
		rows.Scan(&p)
		c.Assert(p, Equals, "Y")
		c.Assert(rows.Next(), IsFalse)
		rows.Close()
		mustCommit(c, tx)
	}

	tx = mustBegin(c, s.testDB)
	// Create a new user
	createUserSQL = `CREATE USER 'testDB1'@'localhost' IDENTIFIED BY '123';`
	mustExec(c, s.testDB, createUserSQL)
	mustExec(c, s.testDB, "USE test;")
	mustExec(c, s.testDB, "GRANT ALL ON * TO 'testDB1'@'localhost';")
	mustCommit(c, tx)
	// Make sure all the global privs for granted user is "Y"
	for _, v := range mysql.AllDBPrivs {
		sql := fmt.Sprintf("SELECT %s FROM mysql.DB WHERE User=\"testDB1\" and host=\"localhost\" and db=\"test\";", mysql.Priv2UserCol[v])
		tx = mustBegin(c, s.testDB)
		rows, err := tx.Query(sql)
		c.Assert(err, IsNil)
		rows.Next()
		var p string
		rows.Scan(&p)
		c.Assert(p, Equals, "Y")
		c.Assert(rows.Next(), IsFalse)
		rows.Close()
		mustCommit(c, tx)
	}
}
