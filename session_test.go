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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/terror"
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
	store, dom := newStoreWithBootstrap(c, dbName)
	defer dom.Close()
	defer store.Close()
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
	localstore.MockRemoteStore = false
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
