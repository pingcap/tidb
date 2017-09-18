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

package tidb

import (
	"fmt"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/auth"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

var (
	_ = Suite(&testSessionSuite{})
	_ = Suite(&test1435Suite{})
)

type testSessionSuite struct {
	dbName         string
	createDBSQL    string
	dropDBSQL      string
	useDBSQL       string
	createTableSQL string
	dropTableSQL   string
	selectSQL      string

	store kv.Storage
	dom   *domain.Domain
}

func (s *testSessionSuite) SetUpSuite(c *C) {
	s.dbName = "test_session_db"
	s.dropTableSQL = `Drop TABLE if exists t;`
	s.createTableSQL = `CREATE TABLE t(id TEXT);`
	s.selectSQL = `SELECT * from t;`

	s.store = newStore(c, s.dbName)
	dom, err := BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.dom = dom
}

func (s *testSessionSuite) TearDownSuite(c *C) {
	removeStore(c, s.dbName)
	s.dom.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestSchemaCheckerSimple(c *C) {
	defer testleak.AfterTest(c)()
	lease := 5 * time.Millisecond
	validator := domain.NewSchemaValidator(lease)
	checker := &schemaLeaseChecker{SchemaValidator: validator}

	// Add some schema versions and delta table IDs.
	ts := uint64(time.Now().UnixNano())
	validator.Update(ts, 0, 2, []int64{1})
	validator.Update(ts, 2, 4, []int64{2})

	// checker's schema version is the same as the current schema version.
	checker.schemaVer = 4
	err := checker.Check(ts)
	c.Assert(err, IsNil)

	// checker's schema version is less than the current schema version, and it doesn't exist in validator's items.
	// checker's related table ID isn't in validator's changed table IDs.
	checker.schemaVer = 2
	checker.relatedTableIDs = []int64{3}
	err = checker.Check(ts)
	c.Assert(err, IsNil)
	// The checker's schema version isn't in validator's items.
	checker.schemaVer = 1
	checker.relatedTableIDs = []int64{3}
	err = checker.Check(ts)
	c.Assert(terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), IsTrue)
	// checker's related table ID is in validator's changed table IDs.
	checker.relatedTableIDs = []int64{2}
	err = checker.Check(ts)
	c.Assert(terror.ErrorEqual(err, domain.ErrInfoSchemaChanged), IsTrue)

	// validator's latest schema version is expired.
	time.Sleep(lease + time.Microsecond)
	checker.schemaVer = 4
	checker.relatedTableIDs = []int64{3}
	err = checker.Check(ts)
	c.Assert(err, IsNil)
	nowTS := uint64(time.Now().UnixNano())
	// Use checker.SchemaValidator.Check instead of checker.Check here because backoff make CI slow.
	result := checker.SchemaValidator.Check(nowTS, checker.schemaVer, checker.relatedTableIDs)
	c.Assert(result, Equals, domain.ResultUnknown)
}

func (s *testSessionSuite) TestPrepare(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_prepare"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	// create table
	mustExecSQL(c, se, s.dropTableSQL)
	mustExecSQL(c, se, s.createTableSQL)
	// insert data
	mustExecSQL(c, se, `INSERT INTO t VALUES ("id");`)
	id, ps, fields, err := se.PrepareStmt("select id+? from t")
	c.Assert(err, IsNil)
	c.Assert(fields, HasLen, 1)
	c.Assert(id, Equals, uint32(1))
	c.Assert(ps, Equals, 1)
	mustExecSQL(c, se, `set @a=1`)
	_, err = se.ExecutePreparedStmt(id, "1")
	c.Assert(err, IsNil)
	err = se.DropPreparedStmt(id)
	c.Assert(err, IsNil)

	mustExecSQL(c, se, "prepare stmt from 'select 1+?'")
	mustExecSQL(c, se, "set @v1=100")
	rs := mustExecSQL(c, se, "execute stmt using @v1")
	r, err := rs.Next()
	c.Assert(err, IsNil)
	c.Assert(r.Data[0].GetFloat64(), Equals, float64(101))

	mustExecSQL(c, se, "set @v2=200")
	rs = mustExecSQL(c, se, "execute stmt using @v2")
	r, err = rs.Next()
	c.Assert(err, IsNil)
	c.Assert(r.Data[0].GetFloat64(), Equals, float64(201))

	mustExecSQL(c, se, "set @v3=300")
	rs = mustExecSQL(c, se, "execute stmt using @v3")
	r, err = rs.Next()
	c.Assert(err, IsNil)
	c.Assert(r.Data[0].GetFloat64(), Equals, float64(301))
	mustExecSQL(c, se, "deallocate prepare stmt")

	// Execute prepared statements for more than one time.
	mustExecSQL(c, se, "create table multiexec (a int, b int)")
	mustExecSQL(c, se, "insert multiexec values (1, 1), (2, 2)")
	id, _, _, err = se.PrepareStmt("select a from multiexec where b = ? order by b")
	c.Assert(err, IsNil)
	rs, err = se.ExecutePreparedStmt(id, 1)
	c.Assert(err, IsNil)
	rs.Close()
	rs, err = se.ExecutePreparedStmt(id, 2)
	rs.Close()
	c.Assert(err, IsNil)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestResultField(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_result_field"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	// create table
	mustExecSQL(c, se, s.dropTableSQL)
	mustExecSQL(c, se, "create table t (id int);")

	mustExecSQL(c, se, `INSERT INTO t VALUES (1);`)
	mustExecSQL(c, se, `INSERT INTO t VALUES (2);`)
	r := mustExecSQL(c, se, `SELECT count(*) from t;`)
	c.Assert(r, NotNil)
	_, err := GetRows(r)
	c.Assert(err, IsNil)
	fields, err := r.Fields()
	c.Assert(err, IsNil)
	c.Assert(len(fields), Equals, 1)
	field := fields[0].Column
	c.Assert(field.Tp, Equals, mysql.TypeLonglong)
	c.Assert(field.Flen, Equals, 21)
	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestPrimaryKeyAutoincrement(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_primary_key_auto_increment"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL, name varchar(255) UNIQUE NOT NULL, status int)")
	mustExecSQL(c, se, "insert t (name) values (?)", "abc")
	id := se.LastInsertID()
	c.Check(id != 0, IsTrue)

	se2 := newSession(c, s.store, dbName)
	rs := mustExecSQL(c, se2, "select * from t")
	c.Assert(rs, NotNil)
	row, err := rs.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, id, []byte("abc"), nil)

	mustExecSQL(c, se, "update t set name = 'abc', status = 1 where id = ?", id)
	rs = mustExecSQL(c, se2, "select * from t")
	c.Assert(rs, NotNil)
	row, err = rs.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, id, []byte("abc"), 1)
	// Check for pass bool param to tidb prepared statement
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (id tiny)")
	mustExecSQL(c, se, "insert t values (?)", true)
	rs = mustExecSQL(c, se, "select * from t")
	c.Assert(rs, NotNil)
	row, err = rs.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, int8(1))

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestAutoIncrementID(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_auto_increment_id"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	mustExecSQL(c, se, "insert t values ()")
	mustExecSQL(c, se, "insert t values ()")
	mustExecSQL(c, se, "insert t values ()")
	se.Execute("drop table if exists t;")
	mustExecSQL(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)")
	mustExecSQL(c, se, "insert t values ()")
	lastID := se.LastInsertID()
	c.Assert(lastID, Less, uint64(4))
	mustExecSQL(c, se, "insert t () values ()")
	c.Assert(se.LastInsertID(), Greater, lastID)
	lastID = se.LastInsertID()
	mustExecSQL(c, se, "insert t values (100)")
	c.Assert(se.LastInsertID(), Equals, uint64(100))

	// If the auto_increment column value is given, it uses the value of the latest row.
	mustExecSQL(c, se, "insert t values (120), (112)")
	c.Assert(se.LastInsertID(), Equals, uint64(112))

	// The last_insert_id function only use last auto-generated id.
	mustExecMatch(c, se, "select last_insert_id()", [][]interface{}{{lastID}})

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (i tinyint unsigned not null auto_increment, primary key (i));")
	mustExecSQL(c, se, "insert into t set i = 254;")
	mustExecSQL(c, se, "insert t values ()")

	// The last insert ID doesn't care about primary key, it is set even if its a normal index column.
	mustExecSQL(c, se, "create table autoid (id int auto_increment, index (id))")
	mustExecSQL(c, se, "insert autoid values ()")
	c.Assert(se.LastInsertID(), Greater, uint64(0))
	mustExecSQL(c, se, "insert autoid values (100)")
	c.Assert(se.LastInsertID(), Equals, uint64(100))

	mustExecMatch(c, se, "select last_insert_id(20)", [][]interface{}{{20}})
	mustExecMatch(c, se, "select last_insert_id()", [][]interface{}{{20}})

	mustExecSQL(c, se, dropDBSQL)
}

func checkTxn(c *C, se Session, stmt string, expectStatus uint16) {
	mustExecSQL(c, se, stmt)
	if expectStatus != 0 {
		c.Assert(se.(*session).txn.Valid(), IsTrue)
	}
}

func checkInTrans(c *C, se Session, stmt string, expectStatus uint16) {
	checkTxn(c, se, stmt, expectStatus)
	ret := se.(*session).sessionVars.Status & mysql.ServerStatusInTrans
	c.Assert(ret, Equals, expectStatus)
}

// TestInTrans ...
// See https://dev.mysql.com/doc/internals/en/status-flags.html
func (s *testSessionSuite) TestInTrans(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_intrans"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	checkInTrans(c, se, "drop table if exists t;", 0)
	checkInTrans(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)", 0)
	checkInTrans(c, se, "insert t values ()", 0)
	checkInTrans(c, se, "begin", 1)
	checkInTrans(c, se, "insert t values ()", 1)
	checkInTrans(c, se, "drop table if exists t;", 0)
	checkInTrans(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)", 0)
	checkInTrans(c, se, "insert t values ()", 0)
	checkInTrans(c, se, "commit", 0)
	checkInTrans(c, se, "insert t values ()", 0)

	checkInTrans(c, se, "set autocommit=0;", 0)
	checkInTrans(c, se, "begin", 1)
	checkInTrans(c, se, "insert t values ()", 1)
	checkInTrans(c, se, "commit", 0)
	checkInTrans(c, se, "insert t values ()", 1)
	checkInTrans(c, se, "commit", 0)

	checkInTrans(c, se, "set autocommit=1;", 0)
	checkInTrans(c, se, "drop table if exists t;", 0)
	checkInTrans(c, se, "create table t (id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL)", 0)
	checkInTrans(c, se, "begin", 1)
	checkInTrans(c, se, "insert t values ()", 1)
	checkInTrans(c, se, "rollback", 0)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue1118(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue1118"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	c.Assert(se.(*session).txn, IsNil)

	// insert
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 int not null auto_increment, c2 int, PRIMARY KEY (c1))")
	mustExecSQL(c, se, "insert into t set c2 = 11")
	r := mustExecSQL(c, se, "select last_insert_id()")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)
	mustExecSQL(c, se, "insert into t (c2) values (22), (33), (44)")
	r = mustExecSQL(c, se, "select last_insert_id()")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 2)
	mustExecSQL(c, se, "insert into t (c1, c2) values (10, 55)")
	r = mustExecSQL(c, se, "select last_insert_id()")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 2)

	// replace
	mustExecSQL(c, se, "replace t (c2) values(66)")
	r = mustExecSQL(c, se, "select * from t")
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	matches(c, rows, [][]interface{}{{1, 11}, {2, 22}, {3, 33}, {4, 44}, {10, 55}, {11, 66}})
	r = mustExecSQL(c, se, "select last_insert_id()")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 11)

	// update
	mustExecSQL(c, se, "update t set c1=last_insert_id(c1 + 100)")
	r = mustExecSQL(c, se, "select * from t")
	rows, err = GetRows(r)
	c.Assert(err, IsNil)
	matches(c, rows, [][]interface{}{{101, 11}, {102, 22}, {103, 33}, {104, 44}, {110, 55}, {111, 66}})
	r = mustExecSQL(c, se, "select last_insert_id()")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 111)
	mustExecSQL(c, se, "insert into t (c2) values (77)")
	r = mustExecSQL(c, se, "select last_insert_id()")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 112)

	// drop
	mustExecSQL(c, se, "drop table t")
	r = mustExecSQL(c, se, "select last_insert_id()")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 112)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue827(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue827"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	se1 := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "drop table if exists t1")
	c.Assert(se.(*session).txn, IsNil)
	mustExecSQL(c, se, "create table t1 (c2 int, c3 int, c1 int not null auto_increment, PRIMARY KEY (c1))")
	mustExecSQL(c, se, "insert into t1 set c2 = 30")

	mustExecSQL(c, se, "drop table if exists t")
	c.Assert(se.(*session).txn, IsNil)
	mustExecSQL(c, se, "create table t (c2 int, c1 int not null auto_increment, PRIMARY KEY (c1))")
	mustExecSQL(c, se, "insert into t (c2) values (1), (2), (3), (4), (5)")

	// insert values
	lastInsertID := se.LastInsertID()
	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "insert into t (c2) values (11), (12), (13)")
	rs, err := exec(se, "select c1 from t where c2 = 11")
	c.Assert(err, IsNil)
	expect, err := GetRows(rs)
	c.Assert(err, IsNil)
	_, err = exec(se, "update t set c2 = 33 where c2 = 1")
	c.Assert(err, IsNil)

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2 = 22 where c2 = 1")
	mustExecSQL(c, se1, "commit")

	_, err = exec(se, "commit")
	c.Assert(err, IsNil)

	rs, err = exec(se, "select c1 from t where c2 = 11")
	c.Assert(err, IsNil)
	r, err := GetRows(rs)
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, expect)
	currLastInsertID := se.GetSessionVars().PrevLastInsertID
	c.Assert(lastInsertID+5, Equals, currLastInsertID)

	// insert set
	lastInsertID = currLastInsertID
	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "insert into t set c2 = 31")
	rs, err = exec(se, "select c1 from t where c2 = 31")
	c.Assert(err, IsNil)
	expect, err = GetRows(rs)
	c.Assert(err, IsNil)
	_, err = exec(se, "update t set c2 = 44 where c2 = 2")
	c.Assert(err, IsNil)

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2 = 55 where c2 = 2")
	mustExecSQL(c, se1, "commit")

	_, err = exec(se, "commit")
	c.Assert(err, IsNil)

	rs, err = exec(se, "select c1 from t where c2 = 31")
	c.Assert(err, IsNil)
	r, err = GetRows(rs)
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, expect)
	currLastInsertID = se.GetSessionVars().PrevLastInsertID
	c.Assert(lastInsertID+3, Equals, currLastInsertID)

	// replace
	lastInsertID = currLastInsertID
	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "insert into t (c2) values (21), (22), (23)")
	rs, err = exec(se, "select c1 from t where c2 = 21")
	c.Assert(err, IsNil)
	expect, err = GetRows(rs)
	c.Assert(err, IsNil)
	_, err = exec(se, "update t set c2 = 66 where c2 = 3")
	c.Assert(err, IsNil)

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2 = 77 where c2 = 3")
	mustExecSQL(c, se1, "commit")

	_, err = exec(se, "commit")
	c.Assert(err, IsNil)

	rs, err = exec(se, "select c1 from t where c2 = 21")
	c.Assert(err, IsNil)
	r, err = GetRows(rs)
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, expect)
	currLastInsertID = se.GetSessionVars().PrevLastInsertID
	c.Assert(lastInsertID+1, Equals, currLastInsertID)

	// update
	lastInsertID = currLastInsertID
	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "insert into t set c2 = 41")
	mustExecSQL(c, se, "update t set c1 = 0 where c2 = 41")
	rs, err = exec(se, "select c1 from t where c2 = 41")
	c.Assert(err, IsNil)
	expect, err = GetRows(rs)
	c.Assert(err, IsNil)
	_, err = exec(se, "update t set c2 = 88 where c2 = 4")
	c.Assert(err, IsNil)

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2 = 99 where c2 = 4")
	mustExecSQL(c, se1, "commit")

	_, err = exec(se, "commit")
	c.Assert(err, IsNil)

	rs, err = exec(se, "select c1 from t where c2 = 41")
	c.Assert(err, IsNil)
	r, err = GetRows(rs)
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, expect)
	currLastInsertID = se.GetSessionVars().PrevLastInsertID
	c.Assert(lastInsertID+3, Equals, currLastInsertID)

	// prepare
	lastInsertID = currLastInsertID
	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "prepare stmt from 'insert into t (c2) values (?)'")
	mustExecSQL(c, se, "set @v1=100")
	mustExecSQL(c, se, "set @v2=200")
	mustExecSQL(c, se, "set @v3=300")
	mustExecSQL(c, se, "execute stmt using @v1")
	mustExecSQL(c, se, "execute stmt using @v2")
	mustExecSQL(c, se, "execute stmt using @v3")
	mustExecSQL(c, se, "deallocate prepare stmt")
	rs, err = exec(se, "select c1 from t where c2 = 12")
	c.Assert(err, IsNil)
	expect, err = GetRows(rs)
	c.Assert(err, IsNil)
	_, err = exec(se, "update t set c2 = 111 where c2 = 5")
	c.Assert(err, IsNil)

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2 = 222 where c2 = 5")
	mustExecSQL(c, se1, "commit")

	_, err = exec(se, "commit")
	c.Assert(err, IsNil)

	rs, err = exec(se, "select c1 from t where c2 = 12")
	c.Assert(err, IsNil)
	r, err = GetRows(rs)
	c.Assert(err, IsNil)
	c.Assert(r, DeepEquals, expect)
	currLastInsertID = se.GetSessionVars().PrevLastInsertID
	c.Assert(lastInsertID+3, Equals, currLastInsertID)

	mustExecSQL(c, se, dropDBSQL)
	se.Close()
	se1.Close()
}

func (s *testSessionSuite) TestIssue996(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue827"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "drop table if exists t")
	c.Assert(se.(*session).txn, IsNil)
	mustExecSQL(c, se, "create table t (c2 int, c3 int, c1 int not null auto_increment, PRIMARY KEY (c1))")
	mustExecSQL(c, se, "insert into t set c2 = 30")

	// insert values
	lastInsertID := se.LastInsertID()
	mustExecSQL(c, se, "prepare stmt1 from 'insert into t (c2) values (?)'")
	mustExecSQL(c, se, "set @v1=10")
	mustExecSQL(c, se, "set @v2=20")
	mustExecSQL(c, se, "execute stmt1 using @v1")
	mustExecSQL(c, se, "execute stmt1 using @v2")
	mustExecSQL(c, se, "deallocate prepare stmt1")
	rs, err := exec(se, "select c1 from t where c2 = 20")
	c.Assert(err, IsNil)
	r, err := GetRows(rs)
	c.Assert(err, IsNil)
	c.Assert(r, NotNil)
	currLastInsertID := se.GetSessionVars().PrevLastInsertID
	c.Assert(r[0][0].GetValue(), DeepEquals, int64(currLastInsertID))
	c.Assert(lastInsertID+2, Equals, currLastInsertID)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue986(c *C) {
	defer testleak.AfterTest(c)()
	sqlText := `CREATE TABLE address (
 		id bigint(20) NOT NULL AUTO_INCREMENT,
 		PRIMARY KEY (id));`
	dbName := "test_issue827"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, sqlText)
	sqlText = `insert into address values ('10')`
	mustExecSQL(c, se, sqlText)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue1089(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue1089"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	r := mustExecSQL(c, se, "select cast(0.5 as unsigned)")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)
	r = mustExecSQL(c, se, "select cast(-0.5 as signed)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, -1)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue1135(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue1135"
	dropDBSQL1 := fmt.Sprintf("drop database %s;", dbName)
	dropDBSQL2 := fmt.Sprintf("drop database %s;", dbName+"1")
	se := newSession(c, s.store, dbName)
	se1 := newSession(c, s.store, dbName+"1")

	mustExecSQL(c, se1, "drop table if exists t")
	mustExecSQL(c, se1, "create table t (F1 VARCHAR(30));")
	mustExecSQL(c, se1, "insert into t (F1) values ('1'), ('4');")

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (F1 VARCHAR(30));")
	mustExecSQL(c, se, "insert into t (F1) values ('1'), ('2');")
	mustExecSQL(c, se, "delete m1 from t m2,t m1 where m1.F1>1;")
	r := mustExecSQL(c, se, "select * from t;")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, []interface{}{'1'})

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (F1 VARCHAR(30));")
	mustExecSQL(c, se, "insert into t (F1) values ('1'), ('2');")
	mustExecSQL(c, se, "delete m1 from t m1,t m2 where true and m1.F1<2;")
	r = mustExecSQL(c, se, "select * from t;")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, []interface{}{'2'})

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (F1 VARCHAR(30));")
	mustExecSQL(c, se, "insert into t (F1) values ('1'), ('2');")
	mustExecSQL(c, se, "delete m1 from t m1,t m2 where false;")
	r = mustExecSQL(c, se, "select * from t;")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, []interface{}{'1'})
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, []interface{}{'2'})

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (F1 VARCHAR(30));")
	mustExecSQL(c, se, "insert into t (F1) values ('1'), ('2');")
	mustExecSQL(c, se, "delete m1, m2 from t m1,t m2 where m1.F1>m2.F1;")
	r = mustExecSQL(c, se, "select * from t;")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (F1 VARCHAR(30));")
	mustExecSQL(c, se, "insert into t (F1) values ('1'), ('2');")
	sql := fmt.Sprintf("delete %s.t from %s.t inner join %s.t where %s.t.F1 > %s.t.F1",
		dbName+"1", dbName+"1", dbName, dbName+"1", dbName)
	mustExecSQL(c, se1, sql)
	r = mustExecSQL(c, se1, "select * from t;")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, []interface{}{'1'})

	mustExecSQL(c, se, dropDBSQL1)
	mustExecSQL(c, se, dropDBSQL2)
}

func (s *testSessionSuite) TestIssue1114(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue1114"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "set @tmp = 0")
	mustExecSQL(c, se, "set @tmp := @tmp + 1")
	r := mustExecSQL(c, se, "select @tmp")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	r = mustExecSQL(c, se, "select @tmp1 = 1, @tmp2 := 2")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, nil, 2)

	r = mustExecSQL(c, se, "select @tmp1 := 11, @tmp2")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 11, 2)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c int);")
	mustExecSQL(c, se, "insert into t values (1),(2);")
	mustExecSQL(c, se, "update t set c = 3 WHERE c = @var:= 1")
	r = mustExecSQL(c, se, "select * from t")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 3)
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 2)

	r = mustExecSQL(c, se, "select @tmp := count(*) from t")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 2)

	r = mustExecSQL(c, se, "select @tmp := c-2 from t where c=3")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestRow(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_row"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	r := mustExecSQL(c, se, "select row(1, 1) in (row(1, 1))")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	r = mustExecSQL(c, se, "select row(1, 1) in (row(1, 0))")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 0)

	r = mustExecSQL(c, se, "select row(1, 1) in (select 1, 1)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	r = mustExecSQL(c, se, "select row(1, 1) > row(1, 0)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	r = mustExecSQL(c, se, "select row(1, 1) > (select 1, 0)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	r = mustExecSQL(c, se, "select 1 > (select 1)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 0)

	r = mustExecSQL(c, se, "select (select 1)")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 1)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestMySQLTypes(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_mysql_types"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	r := mustExecSQL(c, se, `select 0x01 + 1, x'4D7953514C' = "MySQL"`)
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 2, 1)
	r.Close()

	r = mustExecSQL(c, se, `select 0b01 + 1, 0b01000001 = "A"`)
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 2, 1)
	r.Close()

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestShow(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_show"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "set global autocommit=1")
	r := mustExecSQL(c, se, "show global variables where variable_name = 'autocommit'")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "autocommit", "ON")

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, `create table if not exists t (c int) comment '注释'`)
	r = mustExecSQL(c, se, `show columns from t`)
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 1)
	match(c, rows[0], "c", "int(11)", "YES", "", nil, "")

	r = mustExecSQL(c, se, "show collation where Charset = 'utf8' and Collation = 'utf8_bin'")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "utf8_bin", "utf8", 83, "", "Yes", 1)

	r = mustExecSQL(c, se, "show tables")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 1)

	r = mustExecSQL(c, se, "show full tables")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 2)

	r = mustExecSQL(c, se, "show create table t")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 2)
	c.Assert(row.Data[0].GetString(), Equals, "t")

	r = mustExecSQL(c, se, fmt.Sprintf("show table status from %s like 't';", dbName))
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 18)
	c.Assert(row.Data[0].GetString(), Equals, "t")
	c.Assert(row.Data[17].GetString(), Equals, "注释")

	r = mustExecSQL(c, se, "show databases like 'test'")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 1)
	c.Assert(row.Data[0].GetString(), Equals, "test")

	r = mustExecSQL(c, se, "grant all on *.* to 'root'@'%'")
	r = mustExecSQL(c, se, "show grants")
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 1)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestBit(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_bit"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 bit(2))")
	mustExecSQL(c, se, "insert into t values (0), (1), (2), (3)")
	_, err := exec(se, "insert into t values (4)")
	c.Assert(err, NotNil)
	_, err = exec(se, "insert into t values ('a')")
	c.Assert(err, NotNil)
	r := mustExecSQL(c, se, "select * from t where c1 = 2")
	row, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data[0].GetBinaryLiteral(), DeepEquals, types.NewBinaryLiteralFromUint(2, -1))

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 bit(31))")
	mustExecSQL(c, se, "insert into t values (0x7fffffff)")
	_, err = exec(se, "insert into t values (0x80000000)")
	c.Assert(err, NotNil)
	_, err = exec(se, "insert into t values (0xffffffff)")
	c.Assert(err, NotNil)
	mustExecSQL(c, se, "insert into t values ('123')")
	mustExecSQL(c, se, "insert into t values ('1234')")
	_, err = exec(se, "insert into t values ('12345)")
	c.Assert(err, NotNil)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 bit(62))")
	mustExecSQL(c, se, "insert into t values ('12345678')")
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 bit(61))")
	_, err = exec(se, "insert into t values ('12345678')")
	c.Assert(err, NotNil)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 bit(32))")
	mustExecSQL(c, se, "insert into t values (0x7fffffff)")
	mustExecSQL(c, se, "insert into t values (0xffffffff)")
	_, err = exec(se, "insert into t values (0x1ffffffff)")
	c.Assert(err, NotNil)
	mustExecSQL(c, se, "insert into t values ('1234')")
	_, err = exec(se, "insert into t values ('12345')")
	c.Assert(err, NotNil)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 bit(64))")
	mustExecSQL(c, se, "insert into t values (0xffffffffffffffff)")
	mustExecSQL(c, se, "insert into t values ('12345678')")
	_, err = exec(se, "insert into t values ('123456789')")
	c.Assert(err, NotNil)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestEnum(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_enum"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c enum('a', 'b', 'c'))")
	mustExecSQL(c, se, "insert into t values ('a'), (2), ('c')")
	r := mustExecSQL(c, se, "select * from t where c = 'a'")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "a")

	r = mustExecSQL(c, se, "select c + 1 from t where c = 2")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "3")

	mustExecSQL(c, se, "delete from t")
	mustExecSQL(c, se, "insert into t values ()")
	mustExecSQL(c, se, "insert into t values (null), ('1')")
	r = mustExecSQL(c, se, "select c + 1 from t where c = 1")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "2")

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestSet(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_set"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c set('a', 'b', 'c'))")
	mustExecSQL(c, se, "insert into t values ('a'), (2), ('c'), ('a,b'), ('b,a')")
	r := mustExecSQL(c, se, "select * from t where c = 'a'")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "a")

	r = mustExecSQL(c, se, "select * from t where c = 'a,b'")
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 2)

	r = mustExecSQL(c, se, "select c + 1 from t where c = 2")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "3")

	mustExecSQL(c, se, "delete from t")
	mustExecSQL(c, se, "insert into t values ()")
	mustExecSQL(c, se, "insert into t values (null), ('1')")
	r = mustExecSQL(c, se, "select c + 1 from t where c = 1")
	row, err = r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, "2")

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestDatabase(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_database"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	// Test database.
	mustExecSQL(c, se, "create database xxx")
	mustExecSQL(c, se, "drop database xxx")

	mustExecSQL(c, se, "drop database if exists xxx")
	mustExecSQL(c, se, "create database xxx")
	mustExecSQL(c, se, "create database if not exists xxx")
	mustExecSQL(c, se, "drop database if exists xxx")

	// Test schema.
	mustExecSQL(c, se, "create schema xxx")
	mustExecSQL(c, se, "drop schema xxx")

	mustExecSQL(c, se, "drop schema if exists xxx")
	mustExecSQL(c, se, "create schema xxx")
	mustExecSQL(c, se, "create schema if not exists xxx")
	mustExecSQL(c, se, "drop schema if exists xxx")

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestWhereLike(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_where_like"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t(c int, index(c))")
	mustExecSQL(c, se, "insert into t values (1),(2),(3),(-11),(11),(123),(211),(210)")
	mustExecSQL(c, se, "insert into t values ()")

	r := mustExecSQL(c, se, "select c from t where c like '%1%'")
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 6)

	mustExecSQL(c, se, "select c from t where c like binary('abc')")

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestDefaultFlenBug(c *C) {
	defer testleak.AfterTest(c)()
	// If set unspecified column flen to 0, it will cause bug in union.
	// This test is used to prevent the bug reappear.
	dbName := "test_default_flen_bug"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "create table t1 (c double);")
	mustExecSQL(c, se, "create table t2 (c double);")
	mustExecSQL(c, se, "insert into t1 value (73);")
	mustExecSQL(c, se, "insert into t2 value (930);")
	// The data in the second src will be casted as the type of the first src.
	// If use flen=0, it will be truncated.
	r := mustExecSQL(c, se, "select c from t1 union (select c from t2) order by c;")
	rows, err := GetRows(r)
	c.Assert(err, IsNil)
	c.Assert(rows, HasLen, 2)
	c.Assert(rows[1][0].GetFloat64(), Equals, float64(930))

	mustExecSQL(c, se, dropDBSQL)
	c.Assert(err, IsNil)
}

func (s *testSessionSuite) TestExecRestrictedSQL(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_exec_restricted_sql"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName).(*session)
	r, _, err := se.ExecRestrictedSQL(se, "select 1;")
	c.Assert(err, IsNil)
	c.Assert(len(r), Equals, 1)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestOrderBy(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_order_by"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 varchar(20))")
	mustExecSQL(c, se, "insert into t values (1, 2, 'abc'), (2, 1, 'bcd')")

	// Fix issue https://github.com/pingcap/tidb/issues/337
	mustExecMatch(c, se, "select c1 as a, c1 as b from t order by c1", [][]interface{}{{1, 1}, {2, 2}})

	mustExecMatch(c, se, "select c1 as a, t.c1 as a from t order by a desc", [][]interface{}{{2, 2}, {1, 1}})
	mustExecMatch(c, se, "select c1 as c2 from t order by c2", [][]interface{}{{1}, {2}})
	mustExecMatch(c, se, "select sum(c1) from t order by sum(c1)", [][]interface{}{{3}})
	mustExecMatch(c, se, "select c1 as c2 from t order by c2 + 1", [][]interface{}{{2}, {1}})

	// Order by position
	mustExecMatch(c, se, "select * from t order by 1", [][]interface{}{{1, 2, []byte("abc")}, {2, 1, []byte("bcd")}})
	mustExecMatch(c, se, "select * from t order by 2", [][]interface{}{{2, 1, []byte("bcd")}, {1, 2, []byte("abc")}})
	mustExecFailed(c, se, "select * from t order by 0")
	mustExecFailed(c, se, "select * from t order by 4")

	mustExecFailed(c, se, "select c1 as a, c2 as a from t order by a")

	mustExecFailed(c, se, "(select c1 as c2, c2 from t) union (select c1, c2 from t) order by c2")
	mustExecFailed(c, se, "(select c1 as c2, c2 from t) union (select c1, c2 from t) order by c1")

	// Ordery by binary
	mustExecMatch(c, se, "select c1, c3 from t order by binary c1 desc", [][]interface{}{{2, []byte("bcd")}, {1, []byte("abc")}})
	mustExecMatch(c, se, "select c1, c2 from t order by binary c3", [][]interface{}{{1, 2}, {2, 1}})

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestHaving(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_having"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 int)")
	mustExecSQL(c, se, "insert into t values (1,2,3), (2, 3, 1), (3, 1, 2)")

	mustExecMatch(c, se, "select c1 as c2, c3 from t having c2 = 2", [][]interface{}{{2, 1}})
	mustExecMatch(c, se, "select c1 as c2, c3 from t group by c2 having c2 = 2;", [][]interface{}{{1, 3}})
	mustExecMatch(c, se, "select c1 as c2, c3 from t group by c2 having sum(c2) = 2;", [][]interface{}{{1, 3}})
	mustExecMatch(c, se, "select c1 as c2, c3 from t group by c3 having sum(c2) = 2;", [][]interface{}{{1, 3}})
	mustExecMatch(c, se, "select c1 as c2, c3 from t group by c3 having sum(0) + c2 = 2;", [][]interface{}{{2, 1}})
	mustExecMatch(c, se, "select c1 as a from t having c1 = 1;", [][]interface{}{{1}})
	mustExecMatch(c, se, "select t.c1 from t having c1 = 1;", [][]interface{}{{1}})
	mustExecMatch(c, se, "select a.c1 from t as a having c1 = 1;", [][]interface{}{{1}})
	mustExecMatch(c, se, "select c1 as a from t group by c3 having sum(a) = 1;", [][]interface{}{{1}})
	mustExecMatch(c, se, "select c1 as a from t group by c3 having sum(a) + a = 2;", [][]interface{}{{1}})
	mustExecMatch(c, se, "select a.c1 as c, a.c1 as d from t as a, t as b having c1 = 1 limit 1;", [][]interface{}{{1, 1}})

	mustExecMatch(c, se, "select sum(c1) from t group by c1 having sum(c1)", [][]interface{}{{1}, {2}, {3}})
	mustExecMatch(c, se, "select sum(c1) - 1 from t group by c1 having sum(c1) - 1", [][]interface{}{{1}, {2}})
	mustExecMatch(c, se, "select 1 from t group by c1 having sum(abs(c2 + c3)) = c1", [][]interface{}{{1}})

	mustExecFailed(c, se, "select c1 from t having c2")
	mustExecFailed(c, se, "select c1 from t having c2 + 1")
	mustExecFailed(c, se, "select c1 from t group by c2 + 1 having c2")
	mustExecFailed(c, se, "select c1 from t group by c2 + 1 having c2 + 1")
	mustExecFailed(c, se, "select c1 as c2, c2 from t having c2")
	mustExecFailed(c, se, "select c1 as c2, c2 from t having c2 + 1")
	mustExecFailed(c, se, "select c1 as a, c2 as a from t having a")
	mustExecFailed(c, se, "select c1 as a, c2 as a from t having a + 1")
	mustExecFailed(c, se, "select c1 + 1 from t having c1")
	mustExecFailed(c, se, "select c1 + 1 from t having c1 + 1")
	mustExecFailed(c, se, "select a.c1 as c, b.c1 as d from t as a, t as b having c1")
	mustExecFailed(c, se, "select 1 from t having sum(avg(c1))")

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestResultType(c *C) {
	defer testleak.AfterTest(c)()
	// Testcase for https://github.com/pingcap/tidb/issues/325
	dbName := "test_result_type"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	rs := mustExecSQL(c, se, `select cast(null as char(30))`)
	c.Assert(rs, NotNil)
	row, err := rs.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data[0].GetValue(), IsNil)
	fs, err := rs.Fields()
	c.Assert(err, IsNil)
	c.Assert(fs[0].Column.FieldType.Tp, Equals, mysql.TypeVarString)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue461(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue461"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se1 := newSession(c, s.store, dbName)
	mustExecSQL(c, se1,
		`CREATE TABLE test ( id int(11) UNSIGNED NOT NULL AUTO_INCREMENT, val int UNIQUE, PRIMARY KEY (id)); `)
	mustExecSQL(c, se1, "begin;")
	mustExecSQL(c, se1, "insert into test(id, val) values(1, 1);")
	se2 := newSession(c, s.store, dbName)
	mustExecSQL(c, se2, "begin;")
	mustExecSQL(c, se2, "insert into test(id, val) values(2, 2);")
	se3 := newSession(c, s.store, dbName)
	mustExecSQL(c, se3, "begin;")
	mustExecSQL(c, se3, "insert into test(id, val) values(1, 2);")
	mustExecSQL(c, se3, "commit;")
	_, err := se1.Execute("commit")
	c.Assert(err, NotNil)
	// Check error type and error message
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '1' for key 'PRIMARY'")

	_, err = se2.Execute("commit")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, kv.ErrKeyExists), IsTrue)
	c.Assert(err.Error(), Equals, "[kv:1062]Duplicate entry '2' for key 'val'")

	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "drop table test;")
	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue463(c *C) {
	defer testleak.AfterTest(c)()
	// Testcase for https://github.com/pingcap/tidb/issues/463
	dbName := "test_issue463"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "DROP TABLE IF EXISTS test")
	mustExecSQL(c, se,
		`CREATE TABLE test (
			id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
			val int UNIQUE,
			PRIMARY KEY (id)
		);`)
	mustExecSQL(c, se, "insert into test(id, val) values(1, 1);")
	mustExecFailed(c, se, "insert into test(id, val) values(2, 1);")
	mustExecSQL(c, se, "insert into test(id, val) values(2, 2);")

	mustExecSQL(c, se, "begin;")
	mustExecSQL(c, se, "insert into test(id, val) values(3, 3);")
	mustExecFailed(c, se, "insert into test(id, val) values(4, 3);")
	mustExecSQL(c, se, "insert into test(id, val) values(4, 4);")
	mustExecSQL(c, se, "commit;")
	se1 := newSession(c, s.store, dbName)
	mustExecSQL(c, se1, "begin;")
	mustExecSQL(c, se1, "insert into test(id, val) values(5, 6);")
	mustExecSQL(c, se, "begin;")
	mustExecSQL(c, se, "insert into test(id, val) values(20, 6);")
	mustExecSQL(c, se, "commit;")
	mustExecFailed(c, se1, "commit;")
	mustExecSQL(c, se1, "insert into test(id, val) values(5, 5);")

	mustExecSQL(c, se, "drop table test;")

	mustExecSQL(c, se,
		`CREATE TABLE test (
			id int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
			val1 int UNIQUE,
			val2 int UNIQUE,
			PRIMARY KEY (id)
		);`)
	mustExecSQL(c, se, "insert into test(id, val1, val2) values(1, 1, 1);")
	mustExecSQL(c, se, "insert into test(id, val1, val2) values(2, 2, 2);")
	mustExecFailed(c, se, "update test set val1 = 3, val2 = 2 where id = 1;")
	mustExecSQL(c, se, "insert into test(id, val1, val2) values(3, 3, 3);")
	mustExecSQL(c, se, "drop table test;")

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue177(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue177"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, `drop table if exists t1;`)
	mustExecSQL(c, se, `drop table if exists t2;`)
	mustExecSQL(c, se, "CREATE TABLE `t1` ( `a` char(3) NOT NULL default '', `b` char(3) NOT NULL default '', `c` char(3) NOT NULL default '', PRIMARY KEY  (`a`,`b`,`c`)) ENGINE=InnoDB;")
	mustExecSQL(c, se, "CREATE TABLE `t2` ( `a` char(3) NOT NULL default '', `b` char(3) NOT NULL default '', `c` char(3) NOT NULL default '', PRIMARY KEY  (`a`,`b`,`c`)) ENGINE=InnoDB;")
	mustExecSQL(c, se, `INSERT INTO t1 VALUES (1,1,1);`)
	mustExecSQL(c, se, `INSERT INTO t2 VALUES (1,1,1);`)
	mustExecSQL(c, se, `PREPARE my_stmt FROM "SELECT t1.b, count(*) FROM t1 group by t1.b having count(*) > ALL (SELECT COUNT(*) FROM t2 WHERE t2.a=1 GROUP By t2.b)";`)
	mustExecSQL(c, se, `EXECUTE my_stmt;`)
	mustExecSQL(c, se, `EXECUTE my_stmt;`)
	mustExecSQL(c, se, `deallocate prepare my_stmt;`)
	mustExecSQL(c, se, `drop table t1,t2;`)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestFieldText(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_field_text"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (a int)")
	tests := []struct {
		sql   string
		field string
	}{
		{"select distinct(a) from t", "a"},
		{"select (1)", "1"},
		{"select (1+1)", "(1+1)"},
		{"select a from t", "a"},
		{"select        ((a+1))     from t", "((a+1))"},
		{"select 1 /*!32301 +1 */;", "1  +1 "},
		{"select /*!32301 1  +1 */;", "1  +1 "},
		{"/*!32301 select 1  +1 */;", "1  +1 "},
		{"select 1 + /*!32301 1 +1 */;", "1 +  1 +1 "},
		{"select 1 /*!32301 + 1, 1 */;", "1  + 1"},
		{"select /*!32301 1, 1 +1 */;", "1"},
		{"select /*!32301 1 + 1, */ +1;", "1 + 1"},
	}
	for _, tt := range tests {
		results, err := se.Execute(tt.sql)
		c.Assert(err, IsNil)
		result := results[0]
		fields, err := result.Fields()
		c.Assert(err, IsNil)
		c.Assert(fields[0].ColumnAsName.O, Equals, tt.field)
	}

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIndexPointLookup(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_index_point_lookup"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (a int)")
	mustExecSQL(c, se, "insert t values (1), (2), (3)")
	mustExecMatch(c, se, "select * from t where a = '1.1';", [][]interface{}{})
	mustExecMatch(c, se, "select * from t where a > '1.1';", [][]interface{}{{2}, {3}})
	mustExecMatch(c, se, "select * from t where a = '2';", [][]interface{}{{2}})
	mustExecMatch(c, se, "select * from t where a = 3;", [][]interface{}{{3}})
	mustExecMatch(c, se, "select * from t where a = 4;", [][]interface{}{})
	mustExecSQL(c, se, "drop table t")

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue454(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue454"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "drop table if exists t1")
	mustExecSQL(c, se, "create table t1 (c1 int, c2 int, c3 int);")
	mustExecSQL(c, se, "insert into t1 set c1=1, c2=2, c3=1;")
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 int, primary key (c1));")
	mustExecSQL(c, se, "insert into t set c1=1, c2=4;")
	mustExecSQL(c, se, "insert into t select * from t1 limit 1 on duplicate key update c3=3333;")

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue456(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue456"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "drop table if exists t1")
	mustExecSQL(c, se, "create table t1 (c1 int, c2 int, c3 int);")
	mustExecSQL(c, se, "replace into t1 set c1=1, c2=2, c3=1;")
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 int, primary key (c1));")
	mustExecSQL(c, se, "replace into t set c1=1, c2=4;")
	mustExecSQL(c, se, "replace into t select * from t1 limit 1;")

	mustExecSQL(c, se, dropDBSQL)
}

// TestIssue571 ...
// For https://github.com/pingcap/tidb/issues/571
func (s *testSessionSuite) TestIssue571(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue571"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "begin")
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c int)")
	mustExecSQL(c, se, "insert t values (1), (2), (3)")
	mustExecSQL(c, se, "commit")

	se1 := newSession(c, s.store, dbName)
	se1.(*session).unlimitedRetryCount = true
	mustExecSQL(c, se1, "SET SESSION autocommit=1;")
	se2 := newSession(c, s.store, dbName)
	se2.(*session).unlimitedRetryCount = true
	mustExecSQL(c, se2, "SET SESSION autocommit=1;")
	se3 := newSession(c, s.store, dbName)
	se3.(*session).unlimitedRetryCount = true
	mustExecSQL(c, se3, "SET SESSION autocommit=0;")

	var wg sync.WaitGroup
	wg.Add(3)
	f1 := func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			mustExecSQL(c, se1, "update t set c = 1;")
		}
	}
	f2 := func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			mustExecSQL(c, se2, "update t set c = ?;", 1)
		}
	}
	f3 := func() {
		defer wg.Done()
		for i := 0; i < 30; i++ {
			mustExecSQL(c, se3, "begin")
			mustExecSQL(c, se3, "update t set c = 1;")
			mustExecSQL(c, se3, "commit")
		}
	}
	go f1()
	go f2()
	go f3()
	wg.Wait()

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue620(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue620"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "drop table if exists t1")
	mustExecSQL(c, se, "drop table if exists t2")
	mustExecSQL(c, se, "drop table if exists t3")
	mustExecSQL(c, se, "create table t1(id int primary key auto_increment, c int);")
	mustExecSQL(c, se, "create table t2(c int);")
	mustExecSQL(c, se, "insert into t2 values (1);")
	mustExecSQL(c, se, "create table t3(id int, c int);")
	mustExecSQL(c, se, "insert into t3 values (2,2);")
	mustExecSQL(c, se, "insert into t1(c) select * from t2; insert into t1 select * from t3;")
	mustExecMatch(c, se, "select * from t1;", [][]interface{}{{1, 1}, {2, 2}})

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestRetryPreparedStmt(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_retry_prepare_stmt"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	se1 := newSession(c, s.store, dbName)
	se2 := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "drop table if exists t")
	c.Assert(se.(*session).txn, IsNil)
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 int)")
	mustExecSQL(c, se, "insert t values (11, 2, 3)")

	mustExecSQL(c, se1, "begin")
	mustExecSQL(c, se1, "update t set c2=? where c1=11;", 21)

	mustExecSQL(c, se2, "begin")
	mustExecSQL(c, se2, "update t set c2=? where c1=11", 22)
	mustExecSQL(c, se2, "commit")

	mustExecSQL(c, se1, "commit")

	se3 := newSession(c, s.store, dbName)
	r := mustExecSQL(c, se3, "select c2 from t where c1=11")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 21)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestSleep(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_sleep"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "select sleep(0.01);")
	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (a int);")
	mustExecSQL(c, se, "insert t values (sleep(0.02));")
	r := mustExecSQL(c, se, "select * from t;")
	row, err := r.Next()
	c.Assert(err, IsNil)
	match(c, row.Data, 0)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue893(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue893"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "drop table if exists t1; create table t1(id int ); insert into t1 values (1);")
	mustExecMatch(c, se, "select * from t1;", [][]interface{}{{1}})

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIssue1265(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_issue1265"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)

	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (a decimal unique);")
	mustExecSQL(c, se, "insert t values ('100');")
	mustExecFailed(c, se, "insert t values ('1e2');")

	mustExecSQL(c, se, dropDBSQL)
}

type test1435Suite struct{}

func (s *test1435Suite) SetUpSuite(c *C) {
}

func (s *test1435Suite) TestIssue1435(c *C) {
	defer testleak.AfterTest(c)()
	localstore.MockRemoteStore = true
	dbName := "test_issue1435"
	store := newStoreWithBootstrap(c, dbName)
	se := newSession(c, store, dbName)
	se1 := newSession(c, store, dbName)
	se2 := newSession(c, store, dbName)
	// Make sure statements can't retry.
	se.(*session).sessionVars.RetryInfo.Retrying = true
	se1.(*session).sessionVars.RetryInfo.Retrying = true
	se2.(*session).sessionVars.RetryInfo.Retrying = true

	ctx := se.(context.Context)
	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (a int);")
	mustExecSQL(c, se, "drop table if exists t1;")
	mustExecSQL(c, se, "create table t1 (a int);")
	mustExecSQL(c, se, "drop table if exists t2;")
	mustExecSQL(c, se, "create table t2 (a int);")
	startCh1 := make(chan struct{}, 0)
	startCh2 := make(chan struct{}, 0)
	endCh1 := make(chan error, 0)
	endCh2 := make(chan error, 0)
	execFailedFunc := func(s Session, tbl string, start chan struct{}, end chan error) {
		// execute successfully
		_, err := exec(s, "begin;")
		c.Check(err, IsNil)
		<-start
		<-start

		_, err = exec(s, fmt.Sprintf("insert into %s values(1)", tbl))
		c.Check(err, IsNil)

		// table t1 executes failed
		// table t2 executes successfully
		_, err = exec(s, "commit")
		end <- err
	}

	go execFailedFunc(se1, "t1", startCh1, endCh1)
	go execFailedFunc(se2, "t2", startCh2, endCh2)
	// Make sure two insert transactions are begin.
	startCh1 <- struct{}{}
	startCh2 <- struct{}{}

	select {
	case <-endCh1:
		// Make sure the first insert statement isn't finish.
		c.Error("The statement shouldn't be executed")
		c.FailNow()
	default:
	}
	// Make sure loading information schema is failed and server is invalid.
	sessionctx.GetDomain(ctx).MockReloadFailed.SetValue(true)
	err := sessionctx.GetDomain(ctx).Reload()
	c.Assert(err, NotNil)
	lease := sessionctx.GetDomain(ctx).DDL().GetLease()
	time.Sleep(lease)
	// Make sure insert to table t1 transaction executes.
	startCh1 <- struct{}{}
	// Make sure executing insert statement is failed when server is invalid.
	mustExecFailed(c, se, "insert t values (100);")
	err = <-endCh1
	c.Assert(err, NotNil)

	// recover
	select {
	case <-endCh2:
		// Make sure the second insert statement isn't finish.
		c.Error("The statement shouldn't be executed")
		c.FailNow()
	default:
	}

	ver, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	c.Assert(ver, NotNil)
	sessionctx.GetDomain(ctx).MockReloadFailed.SetValue(false)
	time.Sleep(lease)
	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (a int);")
	mustExecSQL(c, se, "insert t values (100);")
	// Make sure insert to table t2 transaction executes.
	startCh2 <- struct{}{}
	err = <-endCh2
	c.Assert(err, IsNil, Commentf("err:%v", err))

	se.Close()
	se1.Close()
	se2.Close()
	sessionctx.GetDomain(ctx).Close()
	err = store.Close()
	c.Assert(err, IsNil)
	localstore.MockRemoteStore = false
}

/* Test cases for session. */

func (s *testSessionSuite) TestSession(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_session"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "ROLLBACK;")

	mustExecSQL(c, se, dropDBSQL)
	se.Close()
}

func (s *testSessionSuite) TestSessionAuth(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_session_auth"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	defer se.Close()
	c.Assert(se.Auth(&auth.UserIdentity{Username: "Any not exist username with zero password!", Hostname: "anyhost"}, []byte(""), []byte("")), IsFalse)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestSkipWithGrant(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_skip_with_grant"
	se := newSession(c, s.store, dbName)
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)

	save1 := privileges.Enable
	save2 := privileges.SkipWithGrant

	privileges.Enable = true
	privileges.SkipWithGrant = false
	c.Assert(se.Auth(&auth.UserIdentity{Username: "user_not_exist"}, []byte("yyy"), []byte("zzz")), IsFalse)

	privileges.SkipWithGrant = true
	c.Assert(se.Auth(&auth.UserIdentity{Username: "xxx", Hostname: "%"}, []byte("yyy"), []byte("zzz")), IsTrue)
	c.Assert(se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, []byte(""), []byte("")), IsTrue)
	mustExecSQL(c, se, "create table t (id int)")

	privileges.Enable = save1
	privileges.SkipWithGrant = save2
	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestSubstringIndexExpr(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_substring_index_expr"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (c varchar(128));")
	mustExecSQL(c, se, `insert into t values ("www.pingcap.com");`)
	mustExecMatch(c, se, "SELECT DISTINCT SUBSTRING_INDEX(c, '.', 2) from t;", [][]interface{}{{"www.pingcap"}})

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIndexMaxLength(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_index_max_length"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "drop table if exists t;")

	// create simple index at table creation
	_, err := exec(se, "create table t (c1 varchar(3073), index(c1));")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	// create simple index after table creation
	mustExecSQL(c, se, "create table t (c1 varchar(3073));")
	_, err = exec(se, "create index idx_c1 on t(c1) ")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	// create compound index at table creation
	mustExecSQL(c, se, "drop table if exists t;")
	_, err = exec(se, "create table t (c1 varchar(3072), c2 varchar(1), index(c1, c2));")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	_, err = exec(se, "create table t (c1 varchar(3072), c2 char(1), index(c1, c2));")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	_, err = exec(se, "create table t (c1 varchar(3072), c2 char, index(c1, c2));")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	_, err = exec(se, "create table t (c1 varchar(3072), c2 date, index(c1, c2));")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	_, err = exec(se, "create table t (c1 varchar(3068), c2 timestamp(1), index(c1, c2));")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	mustExecSQL(c, se, "create table t (c1 varchar(3068), c2 bit(26), index(c1, c2));") // 26 bit = 4 bytes
	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (c1 varchar(3068), c2 bit(32), index(c1, c2));") // 32 bit = 4 bytes
	mustExecSQL(c, se, "drop table if exists t;")
	_, err = exec(se, "create table t (c1 varchar(3068), c2 bit(33), index(c1, c2));")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	// create compound index after table creation
	mustExecSQL(c, se, "create table t (c1 varchar(3072), c2 varchar(1));")
	_, err = exec(se, "create index idx_c1_c2 on t(c1, c2);")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (c1 varchar(3072), c2 char(1));")
	_, err = exec(se, "create index idx_c1_c2 on t(c1, c2);")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (c1 varchar(3072), c2 char);")
	_, err = exec(se, "create index idx_c1_c2 on t(c1, c2);")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (c1 varchar(3072), c2 date);")
	_, err = exec(se, "create index idx_c1_c2 on t(c1, c2);")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (c1 varchar(3068), c2 timestamp(1));")
	_, err = exec(se, "create index idx_c1_c2 on t(c1, c2);")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestSpecifyIndexPrefixLength(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_specify_index_prefix_length"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "drop table if exists t;")

	_, err := exec(se, "create table t (c1 char, index(c1(3)));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	_, err = exec(se, "create table t (c1 int, index(c1(3)));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	_, err = exec(se, "create table t (c1 bit(10), index(c1(3)));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	mustExecSQL(c, se, "create table t (c1 char, c2 int, c3 bit(10));")

	_, err = exec(se, "create index idx_c1 on t (c1(3));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	_, err = exec(se, "create index idx_c1 on t (c2(3));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	_, err = exec(se, "create index idx_c1 on t (c3(3));")
	// ERROR 1089 (HY000): Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	mustExecSQL(c, se, "drop table if exists t;")

	_, err = exec(se, "create table t (c1 int, c2 blob, c3 varchar(64), index(c2));")
	// ERROR 1170 (42000): BLOB/TEXT column 'c2' used in key specification without a key length
	c.Assert(err, NotNil)

	mustExecSQL(c, se, "create table t (c1 int, c2 blob, c3 varchar(64));")
	_, err = exec(se, "create index idx_c1 on t (c2);")
	// ERROR 1170 (42000): BLOB/TEXT column 'c2' used in key specification without a key length
	c.Assert(err, NotNil)

	_, err = exec(se, "create index idx_c1 on t (c2(555555));")
	// ERROR 1071 (42000): Specified key was too long; max key length is 3072 bytes
	c.Assert(err, NotNil)

	_, err = exec(se, "create index idx_c1 on t (c1(5))")
	// ERROR 1089 (HY000): Incorrect prefix key;
	// the used key part isn't a string, the used length is longer than the key part,
	// or the storage engine doesn't support unique prefix keys
	c.Assert(err, NotNil)

	mustExecSQL(c, se, "create index idx_c1 on t (c1);")
	mustExecSQL(c, se, "create index idx_c2 on t (c2(3));")
	mustExecSQL(c, se, "create unique index idx_c3 on t (c3(5));")

	mustExecSQL(c, se, "insert into t values (3, 'abc', 'def');")
	sql := "select c2 from t where c2 = 'abc';"
	mustExecMatch(c, se, sql, [][]interface{}{{[]byte("abc")}})

	mustExecSQL(c, se, "insert into t values (4, 'abcd', 'xxx');")
	mustExecSQL(c, se, "insert into t values (4, 'abcf', 'yyy');")
	sql = "select c2 from t where c2 = 'abcf';"
	mustExecMatch(c, se, sql, [][]interface{}{{[]byte("abcf")}})
	sql = "select c2 from t where c2 = 'abcd';"
	mustExecMatch(c, se, sql, [][]interface{}{{[]byte("abcd")}})

	mustExecSQL(c, se, "insert into t values (4, 'ignore', 'abcdeXXX');")
	_, err = exec(se, "insert into t values (5, 'ignore', 'abcdeYYY');")
	// ERROR 1062 (23000): Duplicate entry 'abcde' for key 'idx_c3'
	c.Assert(err, NotNil)
	sql = "select c3 from t where c3 = 'abcde';"
	mustExecMatch(c, se, sql, [][]interface{}{})

	mustExecSQL(c, se, "delete from t where c3 = 'abcdeXXX';")
	mustExecSQL(c, se, "delete from t where c2 = 'abc';")

	mustExecMatch(c, se, "select c2 from t where c2 > 'abcd';", [][]interface{}{{[]byte("abcf")}})
	mustExecMatch(c, se, "select c2 from t where c2 < 'abcf';", [][]interface{}{{[]byte("abcd")}})
	mustExecMatch(c, se, "select c2 from t where c2 >= 'abcd';", [][]interface{}{{[]byte("abcd")}, {[]byte("abcf")}})
	mustExecMatch(c, se, "select c2 from t where c2 <= 'abcf';", [][]interface{}{{[]byte("abcd")}, {[]byte("abcf")}})
	mustExecMatch(c, se, "select c2 from t where c2 != 'abc';", [][]interface{}{{[]byte("abcd")}, {[]byte("abcf")}})
	mustExecMatch(c, se, "select c2 from t where c2 != 'abcd';", [][]interface{}{{[]byte("abcf")}})

	mustExecSQL(c, se, "drop table if exists t1;")
	mustExecSQL(c, se, "create table t1 (a int, b char(255), key(a, b(20)));")
	mustExecSQL(c, se, "insert into t1 values (0, '1');")
	mustExecSQL(c, se, "update t1 set b = b + 1 where a = 0;")
	mustExecMatch(c, se, "select b from t1 where a = 0;", [][]interface{}{{[]byte("2")}})

	// test union index.
	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (a text, b text, c int, index (a(3), b(3), c));")
	mustExecSQL(c, se, "insert into t values ('abc', 'abcd', 1);")
	mustExecSQL(c, se, "insert into t values ('abcx', 'abcf', 2);")
	mustExecSQL(c, se, "insert into t values ('abcy', 'abcf', 3);")
	mustExecSQL(c, se, "insert into t values ('bbc', 'abcd', 4);")
	mustExecSQL(c, se, "insert into t values ('bbcz', 'abcd', 5);")
	mustExecSQL(c, se, "insert into t values ('cbck', 'abd', 6);")
	mustExecMatch(c, se, "select c from t where a = 'abc' and b <= 'abc';", [][]interface{}{})
	mustExecMatch(c, se, "select c from t where a = 'abc' and b <= 'abd';", [][]interface{}{{1}})
	mustExecMatch(c, se, "select c from t where a < 'cbc' and b > 'abcd';", [][]interface{}{{2}, {3}})
	mustExecMatch(c, se, "select c from t where a <= 'abd' and b > 'abc';", [][]interface{}{{1}, {2}, {3}})
	mustExecMatch(c, se, "select c from t where a < 'bbcc' and b = 'abcd';", [][]interface{}{{1}, {4}})
	mustExecMatch(c, se, "select c from t where a > 'bbcf';", [][]interface{}{{5}, {6}})

	mustExecSQL(c, se, dropDBSQL)
	se.Close()
}

func (s *testSessionSuite) TestIndexColumnLength(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_index_column_length"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "drop table if exists t;")
	mustExecSQL(c, se, "create table t (c1 int, c2 blob);")
	mustExecSQL(c, se, "create index idx_c1 on t(c1);")
	mustExecSQL(c, se, "create index idx_c2 on t(c2(6));")

	is := s.dom.InfoSchema()
	tab, err2 := is.TableByName(model.NewCIStr(dbName), model.NewCIStr("t"))
	c.Assert(err2, Equals, nil)

	idxC1Cols := tables.FindIndexByColName(tab, "c1").Meta().Columns
	c.Assert(idxC1Cols[0].Length, Equals, types.UnspecifiedLength)

	idxC2Cols := tables.FindIndexByColName(tab, "c2").Meta().Columns
	c.Assert(idxC2Cols[0].Length, Equals, 6)

	mustExecSQL(c, se, dropDBSQL)
}

func (s *testSessionSuite) TestIgnoreForeignKey(c *C) {
	c.Skip("skip panic")
	defer testleak.AfterTest(c)()
	sqlText := `CREATE TABLE address (
		id bigint(20) NOT NULL AUTO_INCREMENT,
		user_id bigint(20) NOT NULL,
		PRIMARY KEY (id),
		CONSTRAINT FK_7rod8a71yep5vxasb0ms3osbg FOREIGN KEY (user_id) REFERENCES waimaiqa.user (id),
		INDEX FK_7rod8a71yep5vxasb0ms3osbg (user_id) comment ''
		) ENGINE=InnoDB AUTO_INCREMENT=30 DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ROW_FORMAT=COMPACT COMMENT='' CHECKSUM=0 DELAY_KEY_WRITE=0;`
	dbName := "test_ignore_foreignkey"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, sqlText)
	mustExecSQL(c, se, dropDBSQL)
}

// TestISColumns tests information_schema.columns.
func (s *testSessionSuite) TestISColumns(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_is_columns"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	sql := "select ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS;"
	mustExecSQL(c, se, sql)

	mustExecSQL(c, se, dropDBSQL)
}
