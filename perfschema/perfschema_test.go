// Copyright 2016 PingCAP, Inc.
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

package perfschema_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/perfschema"
	"github.com/pingcap/tidb/util/testleak"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

type testPerfSchemaSuit struct {
	vars map[string]interface{}
}

var _ = Suite(&testPerfSchemaSuit{
	vars: make(map[string]interface{}),
})

func (s *testPerfSchemaSuit) SetUpSuite(c *C) {
	perfschema.EnablePerfSchema()
}

func checkResult(c *C, se tidb.Session, affectedRows uint64, insertID uint64) {
	gotRows := se.AffectedRows()
	c.Assert(gotRows, Equals, affectedRows)

	gotID := se.LastInsertID()
	c.Assert(gotID, Equals, insertID)
}

var testConnID uint64

func newSession(c *C, store kv.Storage, dbName string) tidb.Session {
	se, err := tidb.CreateSession(store)
	c.Assert(err, IsNil)
	id := atomic.AddUint64(&testConnID, 1)
	se.SetConnectionID(id)
	c.Assert(err, IsNil)
	se.Auth(`root@%`, nil, []byte("012345678901234567890"))
	if len(dbName) != 0 {
		mustExecSQL(c, se, "create database if not exists "+dbName)
		mustExecSQL(c, se, "use "+dbName)
	}
	return se
}

func (p *testPerfSchemaSuit) TestInsert(c *C) {
	defer testleak.AfterTest(c)()
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	defer store.Close()
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	se := newSession(c, store, "")
	defer se.Close()
	mustExec(c, se, `insert into performance_schema.setup_actors values("localhost", "nieyy", "contributor", "NO", "NO");`)
	checkResult(c, se, 0, 0)

	cnt := mustQuery(c, se, "select * from performance_schema.setup_actors")
	c.Assert(cnt, Equals, 2)

	mustExec(c, se, `insert into performance_schema.setup_actors (host, user, role) values ("localhost", "lijian", "contributor");`)
	checkResult(c, se, 0, 0)
	cnt = mustQuery(c, se, "select * from performance_schema.setup_actors")
	c.Assert(cnt, Equals, 3)

	mustExec(c, se, `insert into performance_schema.setup_objects values("EVENT", "test", "%", "NO", "NO")`)
	checkResult(c, se, 0, 0)
	cnt = mustQuery(c, se, "select * from performance_schema.setup_objects")
	c.Assert(cnt, Equals, 13)

	mustExec(c, se, `insert into performance_schema.setup_objects (object_schema, object_name) values ("test1", "%")`)
	checkResult(c, se, 0, 0)
	cnt = mustQuery(c, se, "select * from performance_schema.setup_objects")
	c.Assert(cnt, Equals, 14)

	mustFailExec(c, se, `insert into performance_schema.setup_actors (host) values (null);`)
	mustFailExec(c, se, `insert into performance_schema.setup_objects (object_type) values (null);`)
	mustFailExec(c, se, `insert into performance_schema.setup_instruments values("select", "N/A", "N/A");`)
	mustFailExec(c, se, `insert into performance_schema.setup_consumers values("events_stages_current", "N/A");`)
	mustFailExec(c, se, `insert into performance_schema.setup_timers values("timer1", "XXXSECOND");`)
}

func (p *testPerfSchemaSuit) TestInstrument(c *C) {
	defer testleak.AfterTest(c)()
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory + "/test_instrument_db")
	c.Assert(err, IsNil)
	defer store.Close()
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	se := newSession(c, store, "test_instrument_db")
	defer se.Close()

	cnt := mustQuery(c, se, "select * from performance_schema.setup_instruments")
	c.Assert(cnt, Greater, 0)

	mustExec(c, se, "drop database test_instrument_db")
}

func (p *testPerfSchemaSuit) TestConcurrentStatement(c *C) {
	defer testleak.AfterTest(c)()
	store, err := tidb.NewStore(tidb.EngineGoLevelDBMemory + "/test_con_stmt")
	c.Assert(err, IsNil)
	defer store.Close()
	_, err = tidb.BootstrapSession(store)
	c.Assert(err, IsNil)
	se := newSession(c, store, "test_con_stmt")

	mustExec(c, se, "drop table if exists test")
	mustExec(c, se, "create table test (a int, b int)")

	var wg sync.WaitGroup
	iFunc := func(a, b int) {
		defer wg.Done()
		sess := newSession(c, store, "test_con_stmt")
		mustExec(c, sess, fmt.Sprintf(`INSERT INTO test VALUES (%d, %d);`, a, b))
	}
	sFunc := func() {
		defer wg.Done()
		sess := newSession(c, store, "test_con_stmt")
		mustQuery(c, sess, "select * from performance_schema.events_statements_current")
		mustQuery(c, sess, "select * from performance_schema.events_statements_history")
	}

	cnt := 10
	for i := 0; i < cnt; i++ {
		wg.Add(2)
		go iFunc(i, i)
		go sFunc()
	}
	wg.Wait()
}

func exec(se tidb.Session, sql string, args ...interface{}) (ast.RecordSet, error) {
	if len(args) == 0 {
		rs, err := se.Execute(sql)
		if err == nil && len(rs) > 0 {
			return rs[0], nil
		}
		return nil, err
	}
	stmtID, _, _, err := se.PrepareStmt(sql)
	if err != nil {
		return nil, err
	}
	rs, err := se.ExecutePreparedStmt(stmtID, args...)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func mustExecSQL(c *C, se tidb.Session, sql string, args ...interface{}) ast.RecordSet {
	rs, err := exec(se, sql, args...)
	c.Assert(err, IsNil)
	return rs
}
func mustBegin(c *C, se tidb.Session) {
	mustExecSQL(c, se, "begin")
}

func mustCommit(c *C, se tidb.Session) {
	mustExecSQL(c, se, "commit")
}

func mustQuery(c *C, se tidb.Session, s string) int {
	mustBegin(c, se)
	r := mustExecSQL(c, se, s)
	_, err := r.Fields()
	c.Assert(err, IsNil)
	cnt := 0
	for row, err := r.Next(); row != nil && err == nil; row, err = r.Next() {
		cnt++
	}
	c.Assert(err, IsNil)
	mustCommit(c, se)
	return cnt
}

func mustExec(c *C, se tidb.Session, sql string) ast.RecordSet {
	mustBegin(c, se)
	r := mustExecSQL(c, se, sql)
	mustCommit(c, se)
	return r
}

func mustFailExec(c *C, se tidb.Session, sql string) {
	mustBegin(c, se)
	_, err := exec(se, sql)
	c.Assert(err, NotNil)
	mustCommit(c, se)
}
