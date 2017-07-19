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
	s.se.Execute("use test_db_state")
	s.p = parser.New()
}

func (s *testStateChangeSuite) TearDownSuite(c *C) {
	s.se.Execute("drop database if exists test_db_state")
	s.dom.Close()
	s.store.Close()
	s.se.Close()
}

func (s *testStateChangeSuite) TestTowStates(c *C) {
	testInfo := &testExecInfo{
		execKinds: 5,
		sqlInfos:  make([]*sqlInfo, 4),
	}
	for i := 0; i < len(testInfo.sqlInfos); i++ {
		testInfo.sqlInfos[i] = &sqlInfo{
			rawStmts: make([]ast.StmtNode, testInfo.execKinds),
			stmts:    make([]ast.Statement, testInfo.execKinds),
			errs:     make([]error, testInfo.execKinds),
		}
	}
	err := testInfo.createSessions(s.store, "test_db_state")
	c.Assert(err, IsNil)
	testInfo.sqlInfos[0].sql = "insert into t (c1, c2, c3, c4) value(2, 'b', 'N', '2017-07-02')"
	testInfo.sqlInfos[1].sql = "insert into t (c1, c2, c3, d3, c4) value(3, 'b', 'N', 'a', '2017-07-03')"
	unknownColErr := errors.New("unknown column d3")
	testInfo.sqlInfos[1].errs[0] = unknownColErr
	testInfo.sqlInfos[1].errs[1] = unknownColErr
	testInfo.sqlInfos[1].errs[2] = unknownColErr
	testInfo.sqlInfos[1].errs[3] = unknownColErr
	testInfo.sqlInfos[2].sql = "update t set c2 = 'c2_update'"
	// TODO: This code need to be remove after fix this bug.
	testInfo.sqlInfos[2].errs[3] = errors.New("overflow enum boundary")
	testInfo.sqlInfos[3].sql = "replace into t values(5, 'e', 'N', '2017-07-05')'"
	testInfo.sqlInfos[3].errs[4] = errors.New("Column count doesn't match value count at row 1")
	alterTableSQL := "alter table t add column d3 enum('a', 'b') not null default 'a' after c3"
	s.test(c, "", alterTableSQL, testInfo)
	// TODO: Add more DDL statements.
}

func (s *testStateChangeSuite) test(c *C, tableName, alterTableSQL string, testInfo *testExecInfo) {
	defer testleak.AfterTest(c)()
	s.se.Execute(`create table t (
		c1 int,
		c2 varchar(64),
		c3 enum('N','Y') not null default 'N', 
		c4 timestamp on update current_timestamp,
		key(c1, c2))`)
	defer s.se.Execute("drop table t")
	s.se.Execute("insert into t values(1, 1, 'a', 'N', '2017-07-01')")

	callback := &ddl.TestDDLCallback{}
	prevState := model.StateNone
	var checkErr error
	err := testInfo.parseSQLs(s.p)
	c.Assert(err, IsNil, Commentf("error stack %v", errors.ErrorStack(err)))
	times := 0
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == prevState || checkErr != nil || times >= 3 {
			return
		}
		times++
		switch job.SchemaState {
		case model.StateDeleteOnly:
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
			err = testInfo.compileSQL(1)
			if err != nil {
				checkErr = err
			}
		case model.StateWriteReorganization:
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
			err = testInfo.compileSQL(3)
			if err != nil {
				checkErr = err
			}
		}
	}
	d := s.dom.DDL()
	d.SetHook(callback)
	s.se.Execute(alterTableSQL)
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

type sqlInfo struct {
	sql      string
	sessions []tidb.Session
	rawStmts []ast.StmtNode
	stmts    []ast.Statement
	errs     []error
}

type testExecInfo struct {
	execKinds int
	sqlInfos  []*sqlInfo
}

func (t *testExecInfo) createSessions(store kv.Storage, useDB string) error {
	for i, info := range t.sqlInfos {
		info.sessions = make([]tidb.Session, 0, t.execKinds)
		for j := 0; j < t.execKinds; j++ {
			se, err := tidb.CreateSession(store)
			if err != nil {
				return errors.Trace(err)
			}
			se.Execute("use " + useDB)
			se.SetConnectionID(uint64(i*10 + j))
			info.sessions = append(info.sessions, se)
		}
	}
	return nil
}

func (t *testExecInfo) parseSQLs(p *parser.Parser) error {
	var err error
	for i, sqlInfo := range t.sqlInfos {
		if len(sqlInfo.sessions) == 0 {
			return errors.Errorf("no,%d session not init", i)
		}
		seVars := sqlInfo.sessions[0].GetSessionVars()
		charset, collation := seVars.GetCharsetInfo()
		for j := 0; j < t.execKinds; j++ {
			sqlInfo.rawStmts[j], err = p.ParseOneStmt(sqlInfo.sql, charset, collation)
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
		se := info.sessions[idx]
		se.PrepareTxnCtx()
		ctx := se.(context.Context)
		executor.ResetStmtCtx(ctx, info.rawStmts[idx])
		info.stmts[idx], err = compiler.Compile(ctx, info.rawStmts[idx])
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *testExecInfo) execSQL(idx int) error {
	for _, sqlInfo := range t.sqlInfos {
		ctx := sqlInfo.sessions[idx].(context.Context)
		_, err := sqlInfo.stmts[idx].Exec(ctx)
		if err != nil {
			expectedErr := sqlInfo.errs[idx]
			if expectedErr != nil && strings.Contains(err.Error(), expectedErr.Error()) {
				return nil
			}
			return errors.Trace(err)
		}
		err = sqlInfo.sessions[idx].CommitTxn()
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
