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
	"database/sql"
	"fmt"
	"testing"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/mock"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStmtSuite{})

type testStmtSuite struct {
	dbName string

	testDB                   *sql.DB
	createDBSql              string
	dropDBSql                string
	useDBSql                 string
	createTableSql           string
	insertSql                string
	selectSql                string
	createSystemDBSQL        string
	createUserTableSQL       string
	createDBPrivTableSQL     string
	createTablePrivTableSQL  string
	createColumnPrivTableSQL string

	ctx context.Context
}

func (s *testStmtSuite) SetUpTest(c *C) {
	log.SetLevelByString("error")
	s.dbName = "teststmts"
	var err error
	s.testDB, err = sql.Open(tidb.DriverName, tidb.EngineGoLevelDBMemory+"/"+s.dbName+"/"+s.dbName)
	c.Assert(err, IsNil)
	// create db
	s.createDBSql = fmt.Sprintf("create database if not exists %s;", s.dbName)
	s.dropDBSql = fmt.Sprintf("drop database if exists %s;", s.dbName)
	s.useDBSql = fmt.Sprintf("use %s;", s.dbName)
	s.createTableSql = `
    CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));
    CREATE TABLE test1(id INT NOT NULL DEFAULT 2, name varchar(255), PRIMARY KEY(id), INDEX name(name));
    CREATE TABLE test2(id INT NOT NULL DEFAULT 3, name varchar(255), PRIMARY KEY(id));`

	s.selectSql = `SELECT * from test limit 2;`
	mustExec(c, s.testDB, s.createDBSql)
	mustExec(c, s.testDB, s.useDBSql)

	s.createSystemDBSQL = fmt.Sprintf("create database if not exists %s;", mysql.SystemDB)
	s.createUserTableSQL = tidb.CreateUserTable
	s.createDBPrivTableSQL = tidb.CreateDBPrivTable
	s.createTablePrivTableSQL = tidb.CreateTablePrivTable
	s.createColumnPrivTableSQL = tidb.CreateColumnPrivTable

	mustExec(c, s.testDB, s.createSystemDBSQL)
	mustExec(c, s.testDB, s.createUserTableSQL)
	mustExec(c, s.testDB, s.createDBPrivTableSQL)
	mustExec(c, s.testDB, s.createTablePrivTableSQL)
	mustExec(c, s.testDB, s.createColumnPrivTableSQL)

	s.ctx = mock.NewContext()
	variable.BindSessionVars(s.ctx)
}

func (s *testStmtSuite) TearDownTest(c *C) {
	// drop db
	mustExec(c, s.testDB, s.dropDBSql)
}

func (s *testStmtSuite) TestCreateTable(c *C) {
	stmtList, err := tidb.Compile(s.ctx, s.createDBSql+" CREATE TABLE if not exists test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")
	c.Assert(err, IsNil)

	for _, stmt := range stmtList {
		c.Assert(len(stmt.OriginText()), Greater, 0)

		mf := newMockFormatter()
		stmt.Explain(nil, mf)
		c.Assert(mf.Len(), Greater, 0)
	}

	// Test create an exist database
	tx := mustBegin(c, s.testDB)
	_, err = tx.Exec(fmt.Sprintf("CREATE database %s;", s.dbName))
	c.Assert(err, NotNil)
	tx.Rollback()

	// Test create an exist table
	mustExec(c, s.testDB, "CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	tx = mustBegin(c, s.testDB)
	_, err = tx.Exec("CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")
	c.Assert(err, NotNil)
	tx.Rollback()

	// Test "if not exist"
	mustExec(c, s.testDB, "CREATE TABLE if not exists test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")

	// Testcase for https://github.com/pingcap/tidb/issues/312
	mustExec(c, s.testDB, `create table issue312_1 (c float(24));`)
	mustExec(c, s.testDB, `create table issue312_2 (c float(25));`)
	tx = mustBegin(c, s.testDB)
	rows, err := tx.Query(`desc issue312_1`)
	c.Assert(err, IsNil)
	for rows.Next() {
		var (
			c1 string
			c2 string
			c3 string
			c4 string
			c5 string
			c6 string
		)
		rows.Scan(&c1, &c2, &c3, &c4, &c5, &c6)
		c.Assert(c2, Equals, "float")
	}
	rows.Close()
	mustCommit(c, tx)
	tx = mustBegin(c, s.testDB)
	rows, err = tx.Query(`desc issue312_2`)
	c.Assert(err, IsNil)
	for rows.Next() {
		var (
			c1 string
			c2 string
			c3 string
			c4 string
			c5 string
			c6 string
		)
		rows.Scan(&c1, &c2, &c3, &c4, &c5, &c6)
		c.Assert(c2, Equals, "double")
	}
	rows.Close()
	mustCommit(c, tx)
}

func (s *testStmtSuite) TestCreateIndex(c *C) {
	mustExec(c, s.testDB, s.createTableSql)
	stmtList, err := tidb.Compile(s.ctx, "CREATE index name_idx on test (name)")
	c.Assert(err, IsNil)

	str := stmtList[0].OriginText()
	c.Assert(0, Less, len(str))

	mf := newMockFormatter()
	stmtList[0].Explain(nil, mf)
	c.Assert(mf.Len(), Greater, 0)

	tx := mustBegin(c, s.testDB)
	_, err = tx.Exec("CREATE TABLE test(id INT NOT NULL DEFAULT 1, name varchar(255), PRIMARY KEY(id));")
	c.Assert(err, NotNil)
	tx.Rollback()

	// Test not exist
	mustExec(c, s.testDB, "CREATE index name_idx on test (name)")
}

func mustBegin(c *C, currDB *sql.DB) *sql.Tx {
	tx, err := currDB.Begin()
	c.Assert(err, IsNil)
	return tx
}

func mustCommit(c *C, tx *sql.Tx) {
	err := tx.Commit()
	c.Assert(err, IsNil)
}

func mustExecuteSql(c *C, tx *sql.Tx, sql string) sql.Result {
	r, err := tx.Exec(sql)
	c.Assert(err, IsNil, Commentf(sql))
	return r
}

func mustExec(c *C, currDB *sql.DB, sql string) sql.Result {
	tx := mustBegin(c, currDB)
	r := mustExecuteSql(c, tx, sql)
	mustCommit(c, tx)
	return r
}

func checkResult(c *C, r sql.Result, affectedRows int64, insertID int64) {
	gotRows, err := r.RowsAffected()
	c.Assert(err, IsNil)
	c.Assert(gotRows, Equals, affectedRows)

	gotID, err := r.LastInsertId()
	c.Assert(err, IsNil)
	c.Assert(gotID, Equals, insertID)
}
