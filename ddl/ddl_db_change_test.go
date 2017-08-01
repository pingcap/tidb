// Copyright 2017 PingCAP, Inc.
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

package ddl_test

import (
	"strings"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/store/localstore"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testStateChangeSuite{})

type testStateChangeSuite struct {
	lease time.Duration
	store kv.Storage
	dom   *domain.Domain
	se    tidb.Session
	p     *parser.Parser
}

func (s *testStateChangeSuite) SetUpSuite(c *C) {
	s.lease = 200 * time.Millisecond
	var err error
	s.store, err = tidb.NewStore(tidb.EngineGoLevelDBMemory)
	c.Assert(err, IsNil)
	localstore.MockRemoteStore = true
	tidb.SetSchemaLease(s.lease)
	s.dom, err = tidb.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.se, err = tidb.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = s.se.Execute("create database test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute("use test_db_state")
	c.Assert(err, IsNil)
	s.p = parser.New()
}

func (s *testStateChangeSuite) TearDownSuite(c *C) {
	s.se.Execute("drop database if exists test_db_state")
	s.se.Close()
	s.dom.Close()
	s.store.Close()
}

func (s *testStateChangeSuite) TestTwoStates(c *C) {
	cnt := 5
	// New the testExecInfo.
	testInfo := &testExecInfo{
		execCases: cnt,
		sqlInfos:  make([]*sqlInfo, 4),
	}
	for i := 0; i < len(testInfo.sqlInfos); i++ {
		sqlInfo := &sqlInfo{cases: make([]*stateCase, cnt)}
		for j := 0; j < cnt; j++ {
			sqlInfo.cases[j] = new(stateCase)
		}
		testInfo.sqlInfos[i] = sqlInfo
	}
	err := testInfo.createSessions(s.store, "test_db_state")
	c.Assert(err, IsNil)
	// Fill the SQLs and expected error messages.
	testInfo.sqlInfos[0].sql = "insert into t (c1, c2, c3, c4) value(2, 'b', 'N', '2017-07-02')"
	testInfo.sqlInfos[1].sql = "insert into t (c1, c2, c3, d3, c4) value(3, 'b', 'N', 'a', '2017-07-03')"
	unknownColErr := errors.New("unknown column d3")
	testInfo.sqlInfos[1].cases[0].expectedErr = unknownColErr
	testInfo.sqlInfos[1].cases[1].expectedErr = unknownColErr
	testInfo.sqlInfos[1].cases[2].expectedErr = unknownColErr
	testInfo.sqlInfos[1].cases[3].expectedErr = unknownColErr
	testInfo.sqlInfos[2].sql = "update t set c2 = 'c2_update'"
	testInfo.sqlInfos[3].sql = "replace into t values(5, 'e', 'N', '2017-07-05')'"
	testInfo.sqlInfos[3].cases[4].expectedErr = errors.New("Column count doesn't match value count at row 1")
	alterTableSQL := "alter table t add column d3 enum('a', 'b') not null default 'a' after c3"
	s.test(c, "", alterTableSQL, testInfo)
	// TODO: Add more DDL statements.
}

func (s *testStateChangeSuite) test(c *C, tableName, alterTableSQL string, testInfo *testExecInfo) {
	defer testleak.AfterTest(c)()
	_, err := s.se.Execute(`create table t (
		c1 int,
		c2 varchar(64),
		c3 enum('N','Y') not null default 'N', 
		c4 timestamp on update current_timestamp,
		key(c1, c2))`)
	c.Assert(err, IsNil)
	defer s.se.Execute("drop table t")
	_, err = s.se.Execute("insert into t values(1, 'a', 'N', '2017-07-01')")
	c.Assert(err, IsNil)

	callback := &ddl.TestDDLCallback{}
	prevState := model.StateNone
	var checkErr error
	err = testInfo.parseSQLs(s.p)
	c.Assert(err, IsNil, Commentf("error stack %v", errors.ErrorStack(err)))
	times := 0
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == prevState || checkErr != nil || times >= 3 {
			return
		}
		times++
		switch job.SchemaState {
		case model.StateDeleteOnly:
			// This state we execute every sqlInfo one time using the first session and other information.
			err = testInfo.compileSQL(0)
			if err != nil {
				checkErr = err
				break
			}
			err = testInfo.execSQL(0)
			if err != nil {
				checkErr = err
			}
		case model.StateWriteOnly:
			// This state we put the schema information to the second case.
			err = testInfo.compileSQL(1)
			if err != nil {
				checkErr = err
			}
		case model.StateWriteReorganization:
			// This state we execute every sqlInfo one time using the third session and other information.
			err = testInfo.compileSQL(2)
			if err != nil {
				checkErr = err
				break
			}
			err = testInfo.execSQL(2)
			if err != nil {
				checkErr = err
				break
			}
			// Mock the server is in `write only` state.
			err = testInfo.execSQL(1)
			if err != nil {
				checkErr = err
				break
			}
			// This state we put the schema information to the fourth case.
			err = testInfo.compileSQL(3)
			if err != nil {
				checkErr = err
			}
		}
	}
	d := s.dom.DDL()
	d.SetHook(callback)
	_, err = s.se.Execute(alterTableSQL)
	c.Assert(err, IsNil)
	err = testInfo.compileSQL(4)
	c.Assert(err, IsNil)
	err = testInfo.execSQL(4)
	c.Assert(err, IsNil)
	// Mock the server is in `write reorg` state.
	err = testInfo.execSQL(3)
	c.Assert(err, IsNil)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	callback = &ddl.TestDDLCallback{}
	d.SetHook(callback)
}

type stateCase struct {
	session     tidb.Session
	rawStmt     ast.StmtNode
	stmt        ast.Statement
	expectedErr error
}

type sqlInfo struct {
	sql string
	// cases is multiple stateCases.
	// Every case need to be executed with the different schema state.
	cases []*stateCase
}

// testExecInfo contains some SQL information and the number of times each SQL is executed
// in a DDL statement.
type testExecInfo struct {
	// execCases represents every SQL need to be executed execCases times.
	// And the schema state is different at each execution.
	execCases int
	// sqlInfos represents this test information has multiple SQLs to test.
	sqlInfos []*sqlInfo
}

func (t *testExecInfo) createSessions(store kv.Storage, useDB string) error {
	var err error
	for i, info := range t.sqlInfos {
		for j, c := range info.cases {
			c.session, err = tidb.CreateSession(store)
			if err != nil {
				return errors.Trace(err)
			}
			_, err = c.session.Execute("use " + useDB)
			if err != nil {
				return errors.Trace(err)
			}
			// It's used to debug.
			c.session.SetConnectionID(uint64(i*10 + j))
		}
	}
	return nil
}

func (t *testExecInfo) parseSQLs(p *parser.Parser) error {
	if t.execCases <= 0 {
		return nil
	}
	var err error
	for _, sqlInfo := range t.sqlInfos {
		seVars := sqlInfo.cases[0].session.GetSessionVars()
		charset, collation := seVars.GetCharsetInfo()
		for j := 0; j < t.execCases; j++ {
			sqlInfo.cases[j].rawStmt, err = p.ParseOneStmt(sqlInfo.sql, charset, collation)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (t *testExecInfo) compileSQL(idx int) error {
	var err error
	compiler := executor.Compiler{}
	for _, info := range t.sqlInfos {
		c := info.cases[idx]
		se := c.session
		se.PrepareTxnCtx()
		ctx := se.(context.Context)
		executor.ResetStmtCtx(ctx, c.rawStmt)
		c.stmt, err = compiler.Compile(ctx, c.rawStmt)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *testExecInfo) execSQL(idx int) error {
	for _, sqlInfo := range t.sqlInfos {
		c := sqlInfo.cases[idx]
		ctx := c.session.(context.Context)
		_, err := c.stmt.Exec(ctx)
		if c.expectedErr != nil {
			if err == nil {
				err = errors.Errorf("expected error %s but got nil", c.expectedErr)
			} else if strings.Contains(err.Error(), c.expectedErr.Error()) {
				err = nil
			}
		}
		if err != nil {
			return errors.Trace(err)
		}
		err = c.session.CommitTxn()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
