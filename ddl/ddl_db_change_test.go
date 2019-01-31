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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"golang.org/x/net/context"
)

var _ = Suite(&testStateChangeSuite{})

type testStateChangeSuite struct {
	lease time.Duration
	store kv.Storage
	dom   *domain.Domain
	se    session.Session
	p     *parser.Parser
}

func (s *testStateChangeSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()
	s.lease = 200 * time.Millisecond
	var err error
	s.store, err = mockstore.NewMockTikvStore()
	c.Assert(err, IsNil)
	session.SetSchemaLease(s.lease)
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	s.se, err = session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create database test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	s.p = parser.New()
}

func (s *testStateChangeSuite) TearDownSuite(c *C) {
	s.se.Execute(context.Background(), "drop database if exists test_db_state")
	s.se.Close()
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

// TestShowCreateTable tests the result of "show create table" when we are running "add index" or "add column".
func (s *testStateChangeSuite) TestShowCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, index idx (id))")

	var checkErr error
	prevState := model.StateNone
	callback := &ddl.TestDDLCallback{}
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == prevState || checkErr != nil {
			return
		}
		if job.SchemaState != model.StatePublic {
			result := tk.MustQuery("show create table t")
			got := result.Rows()[0][1]
			var expected string
			if job.Type == model.ActionAddIndex {
				expected = "CREATE TABLE `t` (\n  `id` int(11) DEFAULT NULL,\n  KEY `idx` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
			} else if job.Type == model.ActionAddColumn {
				expected = "CREATE TABLE `t` (\n  `id` int(11) DEFAULT NULL,\n  KEY `idx` (`id`),\n  KEY `idx1` (`id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
			}
			if got != expected {
				checkErr = errors.Errorf("got %s, expected %s", got, expected)
			}
		}
	}
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	defer d.SetHook(originalCallback)
	d.SetHook(callback)
	tk.MustExec("alter table t add index idx1(id)")
	c.Assert(checkErr, IsNil)
	tk.MustExec("alter table t add column c int")
	c.Assert(checkErr, IsNil)
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

	d.SetHook(callback)
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
	d.SetHook(originalCallback)
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
	defer d.SetHook(originalCallback)
	d.SetHook(callback)
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
	session     session.Session
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
		executor.ResetStmtCtx(sctx, c.rawStmt)

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
		_, err := c.stmt.Exec(context.TODO())
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
		c.session.StmtCommit()
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

// TestWriteOnly tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *testStateChangeSuite) TestWriteOnly(c *C) {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t where c1 = 'a'", nil}
	sqls[1] = sqlWithErr{"update t use index(idx2) set c1 = 'c1_update' where c1 = 'a'", nil}
	sqls[0] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2018-02-12', c4 = 1", nil}
	addColumnSQL := "alter table t add column a int not null default 1 first"
	s.runTestInSchemaState(c, model.StateWriteOnly, "", addColumnSQL, sqls)
}

// TestDeletaOnly tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *testStateChangeSuite) TestDeleteOnly(c *C) {
	sqls := make([]sqlWithErr, 1)
	sqls[0] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2018-02-12', c4 = 1",
		errors.Errorf("Can't find column c1")}
	dropColumnSQL := "alter table t drop column c1"
	s.runTestInSchemaState(c, model.StateDeleteOnly, "", dropColumnSQL, sqls)
}

func (s *testStateChangeSuite) runTestInSchemaState(c *C, state model.SchemaState, tableName, alterTableSQL string,
	sqlWithErrs []sqlWithErr) {
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

	callback := &ddl.TestDDLCallback{}
	prevState := model.StateNone
	var checkErr error
	times := 0
	se, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	callback.OnJobUpdatedExported = func(job *model.Job) {
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
				break
			}
		}
	}
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	d.SetHook(callback)
	_, err = s.se.Execute(context.Background(), alterTableSQL)
	c.Assert(err, IsNil)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")
	d.SetHook(originalCallback)
}

func (s *testStateChangeSuite) execQuery(tk *testkit.TestKit, sql string, args ...interface{}) (*testkit.Result, error) {
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

func (s *testStateChangeSuite) CheckResult(tk *testkit.TestKit, sql string, args ...interface{}) (*testkit.Result, error) {
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
			checkErr = checkResult(result, testkit.Rows("t 0 PRIMARY 1 c1 A 0 <nil> <nil>  BTREE  "))
		}
	}

	d := s.dom.DDL()
	originalCallback := d.GetHook()
	d.SetHook(callback)
	alterTableSQL := `alter table t add index c2(c2)`
	_, err = s.se.Execute(context.Background(), alterTableSQL)
	c.Assert(err, IsNil)
	c.Assert(errors.ErrorStack(checkErr), Equals, "")

	result, err := s.execQuery(tk, showIndexSQL)
	c.Assert(err, IsNil)
	err = checkResult(result, testkit.Rows("t 0 PRIMARY 1 c1 A 0 <nil> <nil>  BTREE  ", "t 1 c2 1 c2 A 0 <nil> <nil> YES BTREE  "))
	c.Assert(err, IsNil)
	d.SetHook(originalCallback)
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
	err = checkResult(result, testkit.Rows("tr 1 idx1 1 purchased A 0 <nil> <nil> YES BTREE  "))
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

func (s *testStateChangeSuite) TestParallelCreateAndRename(c *C) {
	sql1 := "create table t_exists(c int);"
	sql2 := "alter table t rename to t_exists;"
	f := func(c *C, err1, err2 error) {
		c.Assert(err1, IsNil)
		c.Assert(err2.Error(), Equals, "[schema:1050]Table 't_exists' already exists")
	}
	s.testControlParallelExecSQL(c, sql1, sql2, f)
}

type checkRet func(c *C, err1, err2 error)

func (s *testStateChangeSuite) testControlParallelExecSQL(c *C, sql1, sql2 string, f checkRet) {
	_, err := s.se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	_, err = s.se.Execute(context.Background(), "create table t(a int, b int, c int)")
	c.Assert(err, IsNil)
	defer s.se.Execute(context.Background(), "drop table t")

	callback := &ddl.TestDDLCallback{}
	times := 0
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if times != 0 {
			return
		}
		var qLen int64
		var err1 error
		for {
			kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
				m := meta.NewMeta(txn)
				qLen, err1 = m.DDLJobQueueLen()
				if err1 != nil {
					return err1
				}
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
	defer d.SetHook(originalCallback)
	d.SetHook(callback)

	wg := sync.WaitGroup{}
	var err1 error
	var err2 error
	se, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	se1, err := session.CreateSession(s.store)
	c.Assert(err, IsNil)
	_, err = se1.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)
	wg.Add(2)
	ch := make(chan struct{})
	go func() {
		defer wg.Done()
		close(ch)
		_, err1 = se.Execute(context.Background(), sql1)
	}()
	go func() {
		defer wg.Done()
		<-ch
		// Make sure sql2 is executed after the sql1.
		time.Sleep(time.Millisecond * 10)
		_, err2 = se1.Execute(context.Background(), sql2)
	}()

	wg.Wait()
	f(c, err1, err2)
}

func (s *testStateChangeSuite) testParallelExecSQL(c *C, sql string) {
	se, err := session.CreateSession(s.store)
	_, err = se.Execute(context.Background(), "use test_db_state")
	c.Assert(err, IsNil)

	se1, err1 := session.CreateSession(s.store)
	_, err = se1.Execute(context.Background(), "use test_db_state")
	c.Assert(err1, IsNil)

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
	defer d.SetHook(originalCallback)
	d.SetHook(callback)

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
