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
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tidb/util/testkit"
	"go.uber.org/zap"
)

var _ = Suite(&testStateChangeSuite{})
var _ = SerialSuites(&serialTestStateChangeSuite{})

type serialTestStateChangeSuite struct {
	testStateChangeSuiteBase
}

type testStateChangeSuite struct {
	testStateChangeSuiteBase
}

type testStateChangeSuiteBase struct {
	lease  time.Duration
	store  kv.Storage
	dom    *domain.Domain
	se     session.Session
	p      *parser.Parser
	preSQL string
}

func (s *testStateChangeSuiteBase) SetUpSuite(c *C) {
	s.lease = 200 * time.Millisecond
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	session.SetSchemaLease(s.lease)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create database test_db_state default charset utf8 default collate utf8_bin")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	s.p = parser.New()
}

func (s *testStateChangeSuiteBase) TearDownSuite(c *C) {
	s.se.Execute(context.Background(), "drop database if exists test_db_state")
	s.se.Close()
	s.dom.Close()
	s.store.Close()
}

// TestShowCreateTable tests the result of "show create table" when we are running "add index" or "add column".
func (s *serialTestStateChangeSuite) TestShowCreateTable(c *C) {
	config.GetGlobalConfig().Experimental.AllowsExpressionIndex = true
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int)")
	tk.MustExec("create table t2 (a int, b varchar(10)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci")
	// tkInternal is used to execute additional sql (here show create table) in ddl change callback.
	// Using same `tk` in different goroutines may lead to data race.
	tkInternal := testkit.NewTestKit(c, s.store)
	tkInternal.MustExec("use test")

	var checkErr error
	testCases := []struct {
		sql         string
		expectedRet string
	}{
		{"alter table t add index idx(id)",
			"CREATE TABLE `t` (\n  `id` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"},
		{"alter table t add index idx1(id)",
			"CREATE TABLE `t` (\n  `id` int(11) DEFAULT NULL,\n  KEY `idx` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"},
		{"alter table t add column c int",
			"CREATE TABLE `t` (\n  `id` int(11) DEFAULT NULL,\n  KEY `idx` (`id`),\n  KEY `idx1` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"},
		{"alter table t2 add column c varchar(1)",
			"CREATE TABLE `t2` (\n  `a` int(11) DEFAULT NULL,\n  `b` varchar(10) COLLATE utf8mb4_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"},
		{"alter table t2 add column d varchar(1)",
			"CREATE TABLE `t2` (\n  `a` int(11) DEFAULT NULL,\n  `b` varchar(10) COLLATE utf8mb4_general_ci DEFAULT NULL,\n  `c` varchar(1) COLLATE utf8mb4_general_ci DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci"},
	}
	prevState := model.StateNone
	callback := &ddl.TestDDLCallback{}
	currTestCaseOffset := 0
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == prevState || checkErr != nil {
			return
		}
		if job.State == model.JobStateDone {
			currTestCaseOffset++
		}
		if job.SchemaState != model.StatePublic {
			var result sqlexec.RecordSet
			tbl2 := testGetTableByName(c, tkInternal.Se, "test", "t2")
			if job.TableID == tbl2.Meta().ID {
				// Try to do not use mustQuery in hook func, cause assert fail in mustQuery will cause ddl job hung.
				result, checkErr = tkInternal.Exec("show create table t2")
				if checkErr != nil {
					return
				}
			} else {
				result, checkErr = tkInternal.Exec("show create table t")
				if checkErr != nil {
					return
				}
			}
			req := result.NewChunk()
			checkErr = result.Next(context.Background(), req)
			if checkErr != nil {
				return
			}
			got := req.GetRow(0).GetString(1)
			expected := testCases[currTestCaseOffset].expectedRet
			if got != expected {
				checkErr = errors.Errorf("got %s, expected %s", got, expected)
			}
		}
	}
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	defer d.(ddl.DDLForTest).SetHook(originalCallback)
	d.(ddl.DDLForTest).SetHook(callback)
	for _, tc := range testCases {
		tk.MustExec(tc.sql)
		c.Assert(checkErr, IsNil)
	}
}

// TestDropNotNullColumn is used to test issue #8654.
func (s *testStateChangeSuite) TestDropNotNullColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, a int not null default 11)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("create table t1 (id int, b varchar(255) not null)")
	tk.MustExec("insert into t1 values(2, '')")
	tk.MustExec("create table t2 (id int, c time not null)")
	tk.MustExec("insert into t2 values(3, '11:22:33')")
	tk.MustExec("create table t3 (id int, d json not null)")
	tk.MustExec("insert into t3 values(4, d)")
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test")

	var checkErr error
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	callback := &ddl.TestDDLCallback{}
	sqlNum := 0
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		originalCallback.OnChanged(nil)
		if job.SchemaState == model.StateWriteOnly {
			switch sqlNum {
			case 0:
				_, checkErr = tk1.Exec("insert into t set id = 1")
			case 1:
				_, checkErr = tk1.Exec("insert into t1 set id = 2")
			case 2:
				_, checkErr = tk1.Exec("insert into t2 set id = 3")
			case 3:
				_, checkErr = tk1.Exec("insert into t3 set id = 4")
			}
		}
	}

	d.(ddl.DDLForTest).SetHook(callback)
	tk.MustExec("alter table t drop column a")
	c.Assert(checkErr, IsNil)
	sqlNum++
	tk.MustExec("alter table t1 drop column b")
	c.Assert(checkErr, IsNil)
	sqlNum++
	tk.MustExec("alter table t2 drop column c")
	c.Assert(checkErr, IsNil)
	sqlNum++
	tk.MustExec("alter table t3 drop column d")
	c.Assert(checkErr, IsNil)
	d.(ddl.DDLForTest).SetHook(originalCallback)
	tk.MustExec("drop table t, t1, t2, t3")
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
	unknownColErr := "[planner:1054]Unknown column 'd3' in 'field list'"
	testInfo.sqlInfos[1].cases[0].expectedCompileErr = unknownColErr
	testInfo.sqlInfos[1].cases[1].expectedCompileErr = unknownColErr
	testInfo.sqlInfos[1].cases[2].expectedCompileErr = unknownColErr
	testInfo.sqlInfos[1].cases[3].expectedCompileErr = unknownColErr
	testInfo.sqlInfos[2].sql = "update t set c2 = 'c2_update'"
	testInfo.sqlInfos[3].sql = "replace into t values(5, 'e', 'N', '2017-07-05')"
	testInfo.sqlInfos[3].cases[4].expectedCompileErr = "[planner:1136]Column count doesn't match value count at row 1"
	alterTableSQL := "alter table t add column d3 enum('a', 'b') not null default 'a' after c3"
	s.test(c, "", alterTableSQL, testInfo)
	// TODO: Add more DDL statements.
}

func (s *testStateChangeSuite) test(c *C, tableName, alterTableSQL string, testInfo *testExecInfo) {
	_, err := s.se.Execute(context.Background(), `create table t (
		c1 int,
		c2 varchar(64),
		c3 enum('N','Y') not null default 'N',
		c4 timestamp on update current_timestamp,
		key(c1, c2))`)
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table t")
	_, err = s.se.Execute(context.Background(), "insert into t values(1, 'a', 'N', '2017-07-01')")
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
	originalCallback := d.GetHook()
	defer d.(ddl.DDLForTest).SetHook(originalCallback)
	d.(ddl.DDLForTest).SetHook(callback)
	_, err = s.se.Execute(context.Background(), alterTableSQL)
	c.Assert(err, IsNil)
	err = testInfo.compileSQL(4)
	c.Assert(err, IsNil)
	err = testInfo.execSQL(4)
	c.Assert(err, IsNil)
	// Mock the server is in `write reorg` state.
	err = testInfo.execSQL(3)
	c.Assert(err, IsNil)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
}

type stateCase struct {
	session            session.Session
	rawStmt            ast.StmtNode
	stmt               sqlexec.Statement
	expectedExecErr    string
	expectedCompileErr string
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
			c.session, err = session.CreateSession4Test(store)
			if err != nil {
				return errors.Trace(err)
			}
			_, err = c.session.Execute(context.Background(), "use "+useDB)
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

func (t *testExecInfo) compileSQL(idx int) (err error) {
	for _, info := range t.sqlInfos {
		c := info.cases[idx]
		compiler := executor.Compiler{Ctx: c.session}
		se := c.session
		ctx := context.TODO()
		se.PrepareTxnCtx(ctx)
		sctx := se.(sessionctx.Context)
		if err = executor.ResetContextOfStmt(sctx, c.rawStmt); err != nil {
			return errors.Trace(err)
		}
		c.stmt, err = compiler.Compile(ctx, c.rawStmt)
		if c.expectedCompileErr != "" {
			if err == nil {
				err = errors.Errorf("expected error %s but got nil", c.expectedCompileErr)
			} else if err.Error() == c.expectedCompileErr {
				err = nil
			}
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (t *testExecInfo) execSQL(idx int) error {
	for _, sqlInfo := range t.sqlInfos {
		c := sqlInfo.cases[idx]
		if c.expectedCompileErr != "" {
			continue
		}
		_, err := c.stmt.Exec(context.TODO())
		if c.expectedExecErr != "" {
			if err == nil {
				err = errors.Errorf("expected error %s but got nil", c.expectedExecErr)
			} else if err.Error() == c.expectedExecErr {
				err = nil
			}
		}
		if err != nil {
			return errors.Trace(err)
		}
		err = c.session.CommitTxn(context.TODO())
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

type sqlWithErr struct {
	sql       string
	expectErr error
}

type expectQuery struct {
	sql  string
	rows []string
}

func (s *testStateChangeSuite) TestAppendEnum(c *C) {
	_, err := s.se.Execute(context.Background(), `create table t (
			c1 varchar(64),
			c2 enum('N','Y') not null default 'N',
			c3 timestamp on update current_timestamp,
			c4 int primary key,
			unique key idx2 (c2, c3))`)
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table t")
	_, err = s.se.Execute(context.Background(), "insert into t values('a', 'N', '2017-07-01', 8)")
	c.Assert(err, IsNil)
	// Make sure these sqls use the the plan of index scan.
	_, err = s.se.Execute(context.Background(), "drop stats t")
	c.Assert(err, IsNil)
	se, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)

	_, err = s.se.Execute(context.Background(), "insert into t values('a', 'A', '2018-09-19', 9)")
	c.Assert(err.Error(), Equals, "[table:1366]Incorrect enum value: 'A' for column 'c2' at row 1")
	failAlterTableSQL1 := "alter table t change c2 c2 enum('N') DEFAULT 'N'"
	_, err = s.se.Execute(context.Background(), failAlterTableSQL1)
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported modify column: the number of enum column's elements is less than the original: 2")
	failAlterTableSQL2 := "alter table t change c2 c2 int default 0"
	_, err = s.se.Execute(context.Background(), failAlterTableSQL2)
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported modify column: cannot modify enum type column's to type int(11)")
	alterTableSQL := "alter table t change c2 c2 enum('N','Y','A') DEFAULT 'A'"
	_, err = s.se.Execute(context.Background(), alterTableSQL)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "insert into t values('a', 'A', '2018-09-20', 10)")
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "insert into t (c1, c3, c4) values('a', '2018-09-21', 11)")
	c.Assert(err, IsNil)

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db_state")
	result, err := s.execQuery(tk, "select c4, c2 from t order by c4 asc")
	c.Assert(err, IsNil)
	expected := []string{"8 N", "10 A", "11 A"}
	checkResult(result, testkit.Rows(expected...))

	_, err = s.se.Execute(context.Background(), "update t set c2='N' where c4 = 10")
	c.Assert(err, IsNil)
	result, err = s.execQuery(tk, "select c2 from t where c4 = 10")
	c.Assert(err, IsNil)
	expected = []string{"8 N", "10 N", "11 A"}
	checkResult(result, testkit.Rows(expected...))
}

// https://github.com/pingcap/tidb/pull/6249 fixes the following two test cases.
func (s *testStateChangeSuite) TestWriteOnlyWriteNULL(c *C) {
	sqls := make([]sqlWithErr, 1)
	sqls[0] = sqlWithErr{"insert t set c1 = 'c1_new', c3 = '2019-02-12', c4 = 8 on duplicate key update c1 = values(c1)", nil}
	addColumnSQL := "alter table t add column c5 int not null default 1 after c4"
	expectQuery := &expectQuery{"select c4, c5 from t", []string{"8 1"}}
	// TODO: This case should always fail in write-only state, but it doesn't. We use write-reorganization state here to keep it running stable. It need a double check.
	s.runTestInSchemaState(c, model.StateWriteReorganization, true, addColumnSQL, sqls, expectQuery)
}

func (s *testStateChangeSuite) TestWriteOnlyOnDupUpdate(c *C) {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t", nil}
	sqls[1] = sqlWithErr{"insert t set c1 = 'c1_dup', c3 = '2018-02-12', c4 = 2 on duplicate key update c1 = values(c1)", nil}
	sqls[2] = sqlWithErr{"insert t set c1 = 'c1_new', c3 = '2019-02-12', c4 = 2 on duplicate key update c1 = values(c1)", nil}
	addColumnSQL := "alter table t add column c5 int not null default 1 after c4"
	expectQuery := &expectQuery{"select c4, c5 from t", []string{"2 1"}}
	// TODO: This case should always fail in write-only state, but it doesn't. We use write-reorganization state here to keep it running stable. It need a double check.
	s.runTestInSchemaState(c, model.StateWriteReorganization, true, addColumnSQL, sqls, expectQuery)
}

func (s *testStateChangeSuite) TestWriteOnlyOnDupUpdateForAddColumns(c *C) {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t", nil}
	sqls[1] = sqlWithErr{"insert t set c1 = 'c1_dup', c3 = '2018-02-12', c4 = 2 on duplicate key update c1 = values(c1)", nil}
	sqls[2] = sqlWithErr{"insert t set c1 = 'c1_new', c3 = '2019-02-12', c4 = 2 on duplicate key update c1 = values(c1)", nil}
	addColumnsSQL := "alter table t add column c5 int not null default 1 after c4, add column c44 int not null default 1"
	expectQuery := &expectQuery{"select c4, c5, c44 from t", []string{"2 1 1"}}
	// TODO: This case should always fail in write-only state, but it doesn't. We use write-reorganization state here to keep it running stable. It need a double check.
	s.runTestInSchemaState(c, model.StateWriteReorganization, true, addColumnsSQL, sqls, expectQuery)
}

// TestWriteOnly tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *testStateChangeSuite) TestWriteOnly(c *C) {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t where c1 = 'a'", nil}
	sqls[1] = sqlWithErr{"update t use index(idx2) set c1 = 'c1_update' where c1 = 'a'", nil}
	sqls[2] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2018-02-12', c4 = 1", nil}
	addColumnSQL := "alter table t add column c5 int not null default 1 first"
	s.runTestInSchemaState(c, model.StateWriteOnly, true, addColumnSQL, sqls, nil)
}

// TestWriteOnlyForAddColumns tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *testStateChangeSuite) TestWriteOnlyForAddColumns(c *C) {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t where c1 = 'a'", nil}
	sqls[1] = sqlWithErr{"update t use index(idx2) set c1 = 'c1_update' where c1 = 'a'", nil}
	sqls[2] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2018-02-12', c4 = 1", nil}
	addColumnsSQL := "alter table t add column c5 int not null default 1 first, add column c6 int not null default 1"
	s.runTestInSchemaState(c, model.StateWriteOnly, true, addColumnsSQL, sqls, nil)
}

// TestDeletaOnly tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *testStateChangeSuite) TestDeleteOnly(c *C) {
	sqls := make([]sqlWithErr, 1)
	sqls[0] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2018-02-12', c4 = 1",
		errors.Errorf("Can't find column c1")}
	dropColumnSQL := "alter table t drop column c1"
	s.runTestInSchemaState(c, model.StateDeleteOnly, true, dropColumnSQL, sqls, nil)
}

// TestDeleteOnlyForDropColumns tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *testStateChangeSuite) TestDeleteOnlyForDropColumns(c *C) {
	sqls := make([]sqlWithErr, 1)
	sqls[0] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2018-02-12', c4 = 1",
		errors.Errorf("Can't find column c1")}
	dropColumnsSQL := "alter table t drop column c1, drop column c3"
	s.runTestInSchemaState(c, model.StateDeleteOnly, true, dropColumnsSQL, sqls, nil)
}

func (s *testStateChangeSuite) TestWriteOnlyForDropColumn(c *C) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), `create table tt (c1 int, c4 int)`)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tt (c1, c4) values(8, 8)")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table tt")

	sqls := make([]sqlWithErr, 2)
	sqls[0] = sqlWithErr{"update t set c1='5', c3='2020-03-01';", errors.New("[planner:1054]Unknown column 'c3' in 'field list'")}
	sqls[1] = sqlWithErr{"update t t1, tt t2 set t1.c1='5', t1.c3='2020-03-01', t2.c1='10' where t1.c4=t2.c4",
		errors.New("[planner:1054]Unknown column 'c3' in 'field list'")}
	// TODO: Fix the case of sqls[2].
	// sqls[2] = sqlWithErr{"update t set c1='5' where c3='2017-07-01';", errors.New("[planner:1054]Unknown column 'c3' in 'field list'")}
	dropColumnSQL := "alter table t drop column c3"
	query := &expectQuery{sql: "select * from t;", rows: []string{"a N 8"}}
	s.runTestInSchemaState(c, model.StateWriteOnly, false, dropColumnSQL, sqls, query)
}

func (s *testStateChangeSuite) TestWriteOnlyForDropColumns(c *C) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), `create table t_drop_columns (c1 int, c4 int)`)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into t_drop_columns (c1, c4) values(8, 8)")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table t_drop_columns")

	sqls := make([]sqlWithErr, 2)
	sqls[0] = sqlWithErr{"update t set c1='5', c3='2020-03-01';", errors.New("[planner:1054]Unknown column 'c3' in 'field list'")}
	sqls[1] = sqlWithErr{"update t t1, t_drop_columns t2 set t1.c1='5', t1.c3='2020-03-01', t2.c1='10' where t1.c4=t2.c4",
		errors.New("[planner:1054]Unknown column 'c3' in 'field list'")}
	// TODO: Fix the case of sqls[2].
	// sqls[2] = sqlWithErr{"update t set c1='5' where c3='2017-07-01';", errors.New("[planner:1054]Unknown column 'c3' in 'field list'")}
	dropColumnsSQL := "alter table t drop column c3, drop column c1"
	query := &expectQuery{sql: "select * from t;", rows: []string{"N 8"}}
	s.runTestInSchemaState(c, model.StateWriteOnly, false, dropColumnsSQL, sqls, query)
}

func (s *testStateChangeSuiteBase) runTestInSchemaState(c *C, state model.SchemaState, isOnJobUpdated bool, alterTableSQL string,
	sqlWithErrs []sqlWithErr, expectQuery *expectQuery) {
	_, err := s.se.Execute(context.Background(), `create table t (
	 	c1 varchar(64),
	 	c2 enum('N','Y') not null default 'N',
	 	c3 timestamp on update current_timestamp,
	 	c4 int primary key,
	 	unique key idx2 (c2))`)
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table t")
	_, err = s.se.Execute(context.Background(), "insert into t values('a', 'N', '2017-07-01', 8)")
	c.Assert(err, IsNil)
	// Make sure these sqls use the the plan of index scan.
	_, err = s.se.Execute(context.Background(), "drop stats t")
	c.Assert(err, IsNil)

	callback := &ddl.TestDDLCallback{}
	prevState := model.StateNone
	var checkErr error
	times := 0
	se, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	cbFunc := func(job *model.Job) {
		if job.SchemaState == prevState || checkErr != nil || times >= 3 {
			return
		}
		times++
		if job.SchemaState != state {
			return
		}
		for _, sqlWithErr := range sqlWithErrs {
			_, err = se.Execute(context.Background(), sqlWithErr.sql)
			if !terror.ErrorEqual(err, sqlWithErr.expectErr) {
				checkErr = err
				if checkErr == nil {
					checkErr = errors.New("err can't be nil")
				}
				break
			}
		}
	}
	if isOnJobUpdated {
		callback.OnJobUpdatedExported = cbFunc
	} else {
		callback.OnJobRunBeforeExported = cbFunc
	}
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	d.(ddl.DDLForTest).SetHook(callback)
	_, err = s.se.Execute(context.Background(), alterTableSQL)
	c.Assert(err, IsNil)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	d.(ddl.DDLForTest).SetHook(originalCallback)

	if expectQuery != nil {
		tk := testkit.NewTestKit(c, s.store)
		tk.MustExec("use test_db_state")
		result, err := s.execQuery(tk, expectQuery.sql)
		c.Assert(err, IsNil)
		err = checkResult(result, testkit.Rows(expectQuery.rows...))
		c.Assert(err, IsNil)
	}
}

func (s *testStateChangeSuiteBase) execQuery(tk *testkit.TestKit, sql string, args ...interface{}) (*testkit.Result, error) {
	comment := Commentf("sql:%s, args:%v", sql, args)
	rs, err := tk.Exec(sql, args...)
	if err != nil {
		return nil, err
	}
	result := tk.ResultSetToResult(rs, comment)
	return result, nil
}

func checkResult(result *testkit.Result, expected [][]interface{}) error {
	got := fmt.Sprintf("%s", result.Rows())
	need := fmt.Sprintf("%s", expected)
	if got != need {
		return fmt.Errorf("need %v, but got %v", need, got)
	}
	return nil
}

func (s *testStateChangeSuiteBase) CheckResult(tk *testkit.TestKit, sql string, args ...interface{}) (*testkit.Result, error) {
	comment := Commentf("sql:%s, args:%v", sql, args)
	rs, err := tk.Exec(sql, args...)
	if err != nil {
		return nil, err
	}
	result := tk.ResultSetToResult(rs, comment)
	return result, nil
}

func (s *testStateChangeSuite) TestShowIndex(c *C) {
	_, err := s.se.Execute(context.Background(), `create table t(c1 int primary key, c2 int)`)
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table t")

	callback := &ddl.TestDDLCallback{}
	prevState := model.StateNone
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db_state")
	showIndexSQL := `show index from t`
	var checkErr error
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == prevState || checkErr != nil {
			return
		}
		switch job.SchemaState {
		case model.StateDeleteOnly, model.StateWriteOnly, model.StateWriteReorganization:
			result, err1 := s.execQuery(tk, showIndexSQL)
			if err1 != nil {
				checkErr = err1
				break
			}
			checkErr = checkResult(result, testkit.Rows("t 0 PRIMARY 1 c1 A 0 <nil> <nil>  BTREE   YES NULL"))
		}
	}

	d := s.dom.DDL()
	originalCallback := d.GetHook()
	d.(ddl.DDLForTest).SetHook(callback)
	alterTableSQL := `alter table t add index c2(c2)`
	_, err = s.se.Execute(context.Background(), alterTableSQL)
	c.Assert(err, IsNil)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")

	result, err := s.execQuery(tk, showIndexSQL)
	c.Assert(err, IsNil)
	err = checkResult(result, testkit.Rows(
		"t 0 PRIMARY 1 c1 A 0 <nil> <nil>  BTREE   YES NULL",
		"t 1 c2 1 c2 A 0 <nil> <nil> YES BTREE   YES NULL",
	))
	c.Assert(err, IsNil)
	d.(ddl.DDLForTest).SetHook(originalCallback)

	c.Assert(err, IsNil)

	_, err = s.se.Execute(context.Background(), `create table tr(
		id int, name varchar(50),
		purchased date
	)
	partition by range( year(purchased) ) (
    	partition p0 values less than (1990),
    	partition p1 values less than (1995),
    	partition p2 values less than (2000),
    	partition p3 values less than (2005),
    	partition p4 values less than (2010),
    	partition p5 values less than (2015)
   	);`)
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table tr")
	_, err = s.se.Execute(context.Background(), "create index idx1 on tr (purchased);")
	c.Assert(err, IsNil)
	result, err = s.execQuery(tk, "show index from tr;")
	c.Assert(err, IsNil)
	err = checkResult(result, testkit.Rows("tr 1 idx1 1 purchased A 0 <nil> <nil> YES BTREE   YES NULL"))
	c.Assert(err, IsNil)
}

func (s *testStateChangeSuite) TestParallelAlterModifyColumn(c *C) {
	sql := "ALTER TABLE t MODIFY COLUMN b int FIRST;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
		_, err := s.se.Execute(context.Background(), "select * from t")
		c.Assert(err, IsNil)
	}
	s.testControlParallelExecSQL(c, sql, sql, f)
}

// TODO: This test is not a test that performs two DDLs in parallel.
// So we should not use the function of testControlParallelExecSQL. We will handle this test in the next PR.
// func (s *testStateChangeSuite) TestParallelColumnModifyingDefinition(c *C) {
// 	sql1 := "insert into t(b) values (null);"
// 	sql2 := "alter table t change b b2 bigint not null;"
// 	f := func(c *C, err1, err2 error) {
// 		c.Assert(err1, IsNil)
// 		if err2 != nil {
// 			c.Assert(err2.Error(), Equals, "[ddl:1265]Data truncated for column 'b2' at row 1")
// 		}
// 	}
// 	s.testControlParallelExecSQL(c, sql1, sql2, f)
// }

func (s *testStateChangeSuite) TestParallelAddColumAndSetDefaultValue(c *C) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), `create table tx (
		c1 varchar(64),
		c2 enum('N','Y') not null default 'N',
		primary key idx2 (c2, c1))`)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "insert into tx values('a', 'N')")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table tx")

	sql1 := "alter table tx add column cx int after c1"
	sql2 := "alter table tx alter c2 set default 'N'"

	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
		_, err := s.se.Execute(context.Background(), "delete from tx where c1='a'")
		c.Assert(err, IsNil)
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelChangeColumnName(c *C) {
	sql1 := "ALTER TABLE t CHANGE a aa int;"
	sql2 := "ALTER TABLE t CHANGE b aa int;"
	f := func(c *C, err1, err2 error) {
		// Make sure only a DDL encounters the error of 'duplicate column name'.
		var oneErr error
		if (err1 != nil && err2 == nil) || (err1 == nil && err2 != nil) {
			if err1 != nil {
				oneErr = err1
			} else {
				oneErr = err2
			}
		}
		c.Assert(oneErr.Error(), Equals, "[schema:1060]Duplicate column name 'aa'")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelAlterAddIndex(c *C) {
	sql1 := "ALTER TABLE t add index index_b(b);"
	sql2 := "CREATE INDEX index_b ON t (c);"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[ddl:1061]index already exist index_b")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

func (s *serialTestStateChangeSuite) TestParallelAlterAddExpressionIndex(c *C) {
	config.GetGlobalConfig().Experimental.AllowsExpressionIndex = true
	sql1 := "ALTER TABLE t add index expr_index_b((b+1));"
	sql2 := "CREATE INDEX expr_index_b ON t ((c+1));"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[ddl:1061]index already exist expr_index_b")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelAddPrimaryKey(c *C) {
	sql1 := "ALTER TABLE t add primary key index_b(b);"
	sql2 := "ALTER TABLE t add primary key index_b(c);"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[schema:1068]Multiple primary key defined")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelAlterAddPartition(c *C) {
	sql1 := `alter table t_part add partition (
    partition p2 values less than (30)
   );`
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[ddl:1493]VALUES LESS THAN value must be strictly increasing for each partition")
	}
	s.testControlParallelExecSQL(c, sql1, sql1, f)
}

func (s *testStateChangeSuite) TestParallelDropColumn(c *C) {
	sql := "ALTER TABLE t drop COLUMN c ;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[ddl:1091]column c doesn't exist")
	}
	s.testControlParallelExecSQL(c, sql, sql, f)
}

func (s *testStateChangeSuite) TestParallelDropColumns(c *C) {
	sql := "ALTER TABLE t drop COLUMN b, drop COLUMN c;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[ddl:1091]current error msg: Cancelled DDL job, original error msg: column b doesn't exist")
	}
	s.testControlParallelExecSQL(c, sql, sql, f)
}

func (s *testStateChangeSuite) TestParallelDropIfExistsColumns(c *C) {
	sql := "ALTER TABLE t drop COLUMN if exists b, drop COLUMN if exists c;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
	}
	s.testControlParallelExecSQL(c, sql, sql, f)
}

func (s *testStateChangeSuite) TestParallelDropIndex(c *C) {
	sql1 := "alter table t drop index idx1 ;"
	sql2 := "alter table t drop index idx2 ;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[autoid:1075]Incorrect table definition; there can be only one auto column and it must be defined as a key")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelDropPrimaryKey(c *C) {
	s.preSQL = "ALTER TABLE t add primary key index_b(c);"
	defer func() {
		s.preSQL = ""
	}()
	sql1 := "alter table t drop primary key;"
	sql2 := "alter table t drop primary key;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[ddl:1091]index PRIMARY doesn't exist")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

func (s *testStateChangeSuite) TestParallelCreateAndRename(c *C) {
	sql1 := "create table t_exists(c int);"
	sql2 := "alter table t rename to t_exists;"
	defer s.se.Execute(context.Background(), "drop table t_exists")
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[schema:1050]Table 't_exists' already exists")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

type checkRet func(c *C, err1, err2 error)

func (s *testStateChangeSuiteBase) prepareTestControlParallelExecSQL(c *C) (session.Session, session.Session, chan struct{}, ddl.Callback) {
	callback := &ddl.TestDDLCallback{}
	times := 0
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if times != 0 {
			return
		}
		var qLen int
		for {
			kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
				jobs, err1 := admin.GetDDLJobs(txn)
				if err1 != nil {
					return err1
				}
				qLen = len(jobs)
				return nil
			})
			if qLen == 2 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		times++
	}
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	d.(ddl.DDLForTest).SetHook(callback)

	se, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	se1, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se1.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	ch := make(chan struct{})
	// Make sure the sql1 is put into the DDLJobQueue.
	go func() {
		var qLen int
		for {
			kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
				jobs, err3 := admin.GetDDLJobs(txn)
				if err3 != nil {
					return err3
				}
				qLen = len(jobs)
				return nil
			})
			if qLen == 1 {
				// Make sure sql2 is executed after the sql1.
				close(ch)
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return se, se1, ch, originalCallback
}

func (s *testStateChangeSuiteBase) testControlParallelExecSQL(c *C, sql1, sql2 string, f checkRet) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create table t(a int, b int, c int, d int auto_increment,e int, index idx1(d), index idx2(d,e))")
	c.Assert(err, IsNil)
	if len(s.preSQL) != 0 {
		_, err := s.se.Execute(context.Background(), s.preSQL)
		c.Assert(err, IsNil)
	}
	defer s.se.Execute(context.Background(), "drop table t")

	_, err = s.se.Execute(context.Background(), "drop database if exists t_part")
	c.Assert(err, IsNil)
	s.se.Execute(context.Background(), `create table t_part (a int key)
	 partition by range(a) (
	 partition p0 values less than (10),
	 partition p1 values less than (20)
	 );`)

	se, se1, ch, originalCallback := s.prepareTestControlParallelExecSQL(c)
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originalCallback)

	var err1 error
	var err2 error
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err1 = se.Execute(context.Background(), sql1)
	}()
	go func() {
		defer wg.Done()
		<-ch
		_, err2 = se1.Execute(context.Background(), sql2)
	}()

	wg.Wait()
	f(c, err1, err2)
}

func (s *testStateChangeSuite) TestParallelUpdateTableReplica(c *C) {
	ctx := context.Background()
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(ctx, "drop table if exists t1;")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(ctx, "create table t1 (a int);")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(ctx, "alter table t1 set tiflash replica 3 location labels 'a','b';")
	c.Assert(err, IsNil)

	se, se1, ch, originalCallback := s.prepareTestControlParallelExecSQL(c)
	defer s.dom.DDL().(ddl.DDLForTest).SetHook(originalCallback)

	t1 := testGetTableByName(c, se, "test_db_state", "t1")

	var err1 error
	var err2 error
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		// Mock for table tiflash replica was available.
		err1 = domain.GetDomain(se).DDL().UpdateTableReplicaInfo(se, t1.Meta().ID, true)
	}()
	go func() {
		defer wg.Done()
		<-ch
		// Mock for table tiflash replica was available.
		err2 = domain.GetDomain(se1).DDL().UpdateTableReplicaInfo(se1, t1.Meta().ID, true)
	}()
	wg.Wait()
	c.Assert(err1, IsNil)
	c.Assert(err2.Error(), Equals, "[ddl:-1]the replica available status of table t1 is already updated")
}

func (s *testStateChangeSuite) testParallelExecSQL(c *C, sql string) {
	se, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)

	se1, err1 := session.CreateSession(s.store)
	c.Assert(err1, IsNil)
	_, err = se1.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)

	var err2, err3 error
	wg := sync.WaitGroup{}

	callback := &ddl.TestDDLCallback{}
	once := sync.Once{}
	callback.OnJobUpdatedExported = func(job *model.Job) {
		// sleep a while, let other job enqueue.
		once.Do(func() {
			time.Sleep(time.Millisecond * 10)
		})
	}

	d := s.dom.DDL()
	originalCallback := d.GetHook()
	defer d.(ddl.DDLForTest).SetHook(originalCallback)
	d.(ddl.DDLForTest).SetHook(callback)

	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err2 = se.Execute(context.Background(), sql)
	}()

	go func() {
		defer wg.Done()
		_, err3 = se1.Execute(context.Background(), sql)
	}()
	wg.Wait()
	c.Assert(err2, IsNil)
	c.Assert(err3, IsNil)
}

// TestCreateTableIfNotExists parallel exec create table if not exists xxx. No error returns is expected.
func (s *testStateChangeSuite) TestCreateTableIfNotExists(c *C) {
	defer s.se.Execute(context.Background(), "drop table test_not_exists")
	s.testParallelExecSQL(c, "create table if not exists test_not_exists(a int);")
}

// TestCreateDBIfNotExists parallel exec create database if not exists xxx. No error returns is expected.
func (s *testStateChangeSuite) TestCreateDBIfNotExists(c *C) {
	defer s.se.Execute(context.Background(), "drop database test_not_exists")
	s.testParallelExecSQL(c, "create database if not exists test_not_exists;")
}

// TestDDLIfNotExists parallel exec some DDLs with `if not exists` clause. No error returns is expected.
func (s *testStateChangeSuite) TestDDLIfNotExists(c *C) {
	defer s.se.Execute(context.Background(), "drop table test_not_exists")
	_, err := s.se.Execute(context.Background(), "create table if not exists test_not_exists(a int)")
	c.Assert(err, IsNil)

	// ADD COLUMN
	s.testParallelExecSQL(c, "alter table test_not_exists add column if not exists b int")

	// ADD COLUMNS
	s.testParallelExecSQL(c, "alter table test_not_exists add column if not exists (c11 int, d11 int)")

	// ADD INDEX
	s.testParallelExecSQL(c, "alter table test_not_exists add index if not exists idx_b (b)")

	// CREATE INDEX
	s.testParallelExecSQL(c, "create index if not exists idx_b on test_not_exists (b)")
}

// TestDDLIfExists parallel exec some DDLs with `if exists` clause. No error returns is expected.
func (s *testStateChangeSuite) TestDDLIfExists(c *C) {
	defer func() {
		s.se.Execute(context.Background(), "drop table test_exists")
		s.se.Execute(context.Background(), "drop table test_exists_2")
	}()
	_, err := s.se.Execute(context.Background(), "create table if not exists test_exists (a int key, b int)")
	c.Assert(err, IsNil)

	// DROP COLUMNS
	s.testParallelExecSQL(c, "alter table test_exists drop column if exists c, drop column if exists d")

	// DROP COLUMN
	s.testParallelExecSQL(c, "alter table test_exists drop column if exists b") // only `a` exists now

	// CHANGE COLUMN
	s.testParallelExecSQL(c, "alter table test_exists change column if exists a c int") // only, `c` exists now

	// MODIFY COLUMN
	s.testParallelExecSQL(c, "alter table test_exists modify column if exists a bigint")

	// DROP INDEX
	_, err = s.se.Execute(context.Background(), "alter table test_exists add index idx_c (c)")
	c.Assert(err, IsNil)
	s.testParallelExecSQL(c, "alter table test_exists drop index if exists idx_c")

	// DROP PARTITION (ADD PARTITION tested in TestParallelAlterAddPartition)
	_, err = s.se.Execute(context.Background(), "create table test_exists_2 (a int key) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20))")
	c.Assert(err, IsNil)
	s.testParallelExecSQL(c, "alter table test_exists_2 drop partition if exists p1")
}

// TestParallelDDLBeforeRunDDLJob tests a session to execute DDL with an outdated information schema.
// This test is used to simulate the following conditions:
// In a cluster, TiDB "a" executes the DDL.
// TiDB "b" fails to load schema, then TiDB "b" executes the DDL statement associated with the DDL statement executed by "a".
func (s *testStateChangeSuite) TestParallelDDLBeforeRunDDLJob(c *C) {
	defer s.se.Execute(context.Background(), "drop table test_table")
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create table test_table (c1 int, c2 int default 1, index (c1))")
	c.Assert(err, IsNil)

	// Create two sessions.
	se, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	se1, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se1.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)

	intercept := &ddl.TestInterceptor{}
	firstConnID := uint64(1)
	finishedCnt := int32(0)
	interval := 5 * time.Millisecond
	var sessionCnt int32 // sessionCnt is the number of sessions that goes into the function of OnGetInfoSchema.
	intercept.OnGetInfoSchemaExported = func(ctx sessionctx.Context, is infoschema.InfoSchema) infoschema.InfoSchema {
		// The following code is for testing.
		// Make sure the two sessions get the same information schema before executing DDL.
		// After the first session executes its DDL, then the second session executes its DDL.
		var info infoschema.InfoSchema
		atomic.AddInt32(&sessionCnt, 1)
		for {
			// Make sure there are two sessions running here.
			if atomic.LoadInt32(&sessionCnt) == 2 {
				info = is
				break
			}
			// Print log to notify if TestParallelDDLBeforeRunDDLJob hang up
			log.Info("sleep in TestParallelDDLBeforeRunDDLJob", zap.String("interval", interval.String()))
			time.Sleep(interval)
		}

		currID := ctx.GetSessionVars().ConnectionID
		for {
			seCnt := atomic.LoadInt32(&sessionCnt)
			// Make sure the two session have got the same information schema. And the first session can continue to go on,
			// or the frist session finished this SQL(seCnt = finishedCnt), then other sessions can continue to go on.
			if currID == firstConnID || seCnt == finishedCnt {
				break
			}
			// Print log to notify if TestParallelDDLBeforeRunDDLJob hang up
			log.Info("sleep in TestParallelDDLBeforeRunDDLJob", zap.String("interval", interval.String()))
			time.Sleep(interval)
		}

		return info
	}
	d := s.dom.DDL()
	d.(ddl.DDLForTest).SetInterceptoror(intercept)

	// Make sure the connection 1 executes a SQL before the connection 2.
	// And the connection 2 executes a SQL with an outdated information schema.
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()

		se.SetConnectionID(firstConnID)
		_, err1 := se.Execute(context.Background(), "alter table test_table drop column c2")
		c.Assert(err1, IsNil)
		atomic.StoreInt32(&sessionCnt, finishedCnt)
	}()
	go func() {
		defer wg.Done()

		se1.SetConnectionID(2)
		_, err2 := se1.Execute(context.Background(), "alter table test_table add column c2 int")
		c.Assert(err2, NotNil)
		c.Assert(strings.Contains(err2.Error(), "Information schema is changed"), IsTrue)
	}()

	wg.Wait()

	intercept = &ddl.TestInterceptor{}
	d.(ddl.DDLForTest).SetInterceptoror(intercept)
}

func (s *testStateChangeSuite) TestParallelAlterSchemaCharsetAndCollate(c *C) {
	sql := "ALTER SCHEMA test_db_state CHARSET utf8mb4 COLLATE utf8mb4_general_ci"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, IsNil)
	}
	s.testControlParallelExecSQL(c, sql, sql, f)
	sql = `SELECT default_character_set_name, default_collation_name
			FROM information_schema.schemata
			WHERE schema_name='test_db_state'`
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery(sql).Check(testkit.Rows("utf8mb4 utf8mb4_general_ci"))
}

// TestParallelTruncateTableAndAddColumn tests add column when truncate table.
func (s *testStateChangeSuite) TestParallelTruncateTableAndAddColumn(c *C) {
	sql1 := "truncate table t"
	sql2 := "alter table t add column c3 int"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, NotNil)
		c.Assert(err2.Error(), Equals, "[domain:8028]Information schema is changed during the execution of the statement(for example, table definition may be updated by other DDL ran in parallel). If you see this error often, try increasing `tidb_max_delta_schema_count`. [try again later]")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

// TestParallelTruncateTableAndAddColumns tests add columns when truncate table.
func (s *testStateChangeSuite) TestParallelTruncateTableAndAddColumns(c *C) {
	sql1 := "truncate table t"
	sql2 := "alter table t add column c3 int, add column c4 int"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, NotNil)
		c.Assert(err2.Error(), Equals, "[domain:8028]Information schema is changed during the execution of the statement(for example, table definition may be updated by other DDL ran in parallel). If you see this error often, try increasing `tidb_max_delta_schema_count`. [try again later]")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

// TestParallelFlashbackTable tests parallel flashback table.
func (s *serialTestStateChangeSuite) TestParallelFlashbackTable(c *C) {
	c.Assert(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`), IsNil)
	defer func(originGC bool) {
		c.Assert(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"), IsNil)
		if originGC {
			ddl.EmulatorGCEnable()
		} else {
			ddl.EmulatorGCDisable()
		}
	}(ddl.IsEmulatorGCEnable())

	// disable emulator GC.
	// Disable emulator GC, otherwise, emulator GC will delete table record as soon as possible after executing drop table DDL.
	ddl.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - time.Duration(48*60*60*time.Second)).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk := testkit.NewTestKit(c, s.store)
	// clear GC variables first.
	tk.MustExec("delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// set GC enable.
	err := gcutil.EnableGC(tk.Se)
	c.Assert(err, IsNil)

	// prepare dropped table.
	tk.MustExec("use test_db_state")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int);")
	tk.MustExec("drop table if exists t")
	// Test parallel flashback table.
	sql1 := "flashback table t to t_flashback"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2, NotNil)
		c.Assert(err2.Error(), Equals, "[schema:1050]Table 't_flashback' already exists")

	}
	s.testControlParallelExecSQL(c, sql1, sql1, f)
}
