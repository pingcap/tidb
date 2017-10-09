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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testleak"
)

var (
	_ = Suite(&testSessionSuite{})
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

func (s *testSessionSuite) TestHaving(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_having"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
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
