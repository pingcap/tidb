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
