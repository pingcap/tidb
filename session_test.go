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

func (s *testSessionSuite) TestHaving(c *C) {
	defer testleak.AfterTest(c)()
	dbName := "test_having"
	dropDBSQL := fmt.Sprintf("drop database %s;", dbName)
	se := newSession(c, s.store, dbName)
	mustExecSQL(c, se, "drop table if exists t")
	mustExecSQL(c, se, "create table t (c1 int, c2 int, c3 int)")
	mustExecSQL(c, se, "insert into t values (1,2,3), (2, 3, 1), (3, 1, 2)")
	mustExecSQL(c, se, "set sql_mode = 'STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION'")

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
