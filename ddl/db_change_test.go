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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/ddl"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/gcutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

type stateChangeSuite struct {
	suite.Suite
	store kv.Storage
	dom   *domain.Domain
	tk    *testkit.TestKit
}

func TestStateChange(t *testing.T) {
	suite.Run(t, new(stateChangeSuite))
}

func (s *stateChangeSuite) SetupSuite() {
	ddl.SetWaitTimeWhenErrorOccurred(1 * time.Microsecond)
	session.SetSchemaLease(200 * time.Millisecond)

	var err error
	s.store, err = mockstore.NewMockStore()
	s.Require().NoError(err)
	s.dom, err = session.BootstrapSession(s.store)
	s.Require().NoError(err)
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("create database test_db_state default charset utf8 default collate utf8_bin")
	tk.MustExec("use test_db_state")
}

func (s *stateChangeSuite) SetupTest() {
	s.tk = testkit.NewTestKit(s.T(), s.store)
	s.tk.MustExec("use test_db_state")
}

func (s *stateChangeSuite) TearDownSuite() {
	s.dom.Close()
	s.Require().NoError(s.store.Close())
}

// TestShowCreateTable tests the result of "show create table" when we are running "add index" or "add column".
func (s *stateChangeSuite) TestShowCreateTable() {
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int)")
	tk.MustExec("create table t2 (a int, b varchar(10)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci")
	// tkInternal is used to execute additional sql (here show create table) in ddl change callback.
	// Using same `tk` in different goroutines may lead to data race.
	tkInternal := testkit.NewTestKit(s.T(), s.store)
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
			tbl2 := external.GetTableByName(s.T(), tkInternal, "test", "t2")
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
			req := result.NewChunk(nil)
			checkErr = result.Next(context.Background(), req)
			if checkErr != nil {
				return
			}
			got := req.GetRow(0).GetString(1)
			expected := testCases[currTestCaseOffset].expectedRet
			if got != expected {
				checkErr = errors.Errorf("got %s, expected %s", got, expected)
			}
			terror.Log(result.Close())
		}
	}
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	defer d.SetHook(originalCallback)
	d.SetHook(callback)
	for _, tc := range testCases {
		tk.MustExec(tc.sql)
		s.Require().NoError(checkErr)
	}
}

// TestDropNotNullColumn is used to test issue #8654.
func (s *stateChangeSuite) TestDropNotNullColumn() {
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int, a int not null default 11)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("create table t1 (id int, b varchar(255) not null)")
	tk.MustExec("insert into t1 values(2, '')")
	tk.MustExec("create table t2 (id int, c time not null)")
	tk.MustExec("insert into t2 values(3, '11:22:33')")
	tk.MustExec("create table t3 (id int, d json not null)")
	tk.MustExec("insert into t3 values(4, d)")
	tk1 := testkit.NewTestKit(s.T(), s.store)
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
		err := originalCallback.OnChanged(nil)
		s.Require().NoError(err)
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
	s.Require().NoError(checkErr)
	sqlNum++
	tk.MustExec("alter table t1 drop column b")
	s.Require().NoError(checkErr)
	sqlNum++
	tk.MustExec("alter table t2 drop column c")
	s.Require().NoError(checkErr)
	sqlNum++
	tk.MustExec("alter table t3 drop column d")
	s.Require().NoError(checkErr)
	d.SetHook(originalCallback)
	tk.MustExec("drop table t, t1, t2, t3")
}

func (s *stateChangeSuite) TestTwoStates() {
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
	s.Require().NoError(err)
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
	s.test(alterTableSQL, testInfo)
	// TODO: Add more DDL statements.
}

func (s *stateChangeSuite) test(alterTableSQL string, testInfo *testExecInfo) {
	s.tk.MustExec(`create table t (
		c1 int,
		c2 varchar(64),
		c3 enum('N','Y') not null default 'N',
		c4 timestamp on update current_timestamp,
		key(c1, c2))`)
	defer s.tk.MustExec("drop table t")

	s.tk.MustExec("insert into t values(1, 'a', 'N', '2017-07-01')")

	callback := &ddl.TestDDLCallback{}
	prevState := model.StateNone
	s.Require().NoError(testInfo.parseSQLs(parser.New()))

	times := 0
	var checkErr error
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == prevState || checkErr != nil || times >= 3 {
			return
		}
		times++
		switch job.SchemaState {
		case model.StateDeleteOnly:
			// This state we execute every sqlInfo one time using the first session and other information.
			err := testInfo.compileSQL(0)
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
			err := testInfo.compileSQL(1)
			if err != nil {
				checkErr = err
			}
		case model.StateWriteReorganization:
			// This state we execute every sqlInfo one time using the third session and other information.
			err := testInfo.compileSQL(2)
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
	s.tk.MustExec(alterTableSQL)
	s.Require().NoError(testInfo.compileSQL(4))
	s.Require().NoError(testInfo.execSQL(4))
	// Mock the server is in `write reorg` state.
	s.Require().NoError(testInfo.execSQL(3))
	s.Require().NoError(checkErr)
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
		if err = se.PrepareTxnCtx(ctx); err != nil {
			return err
		}
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

func (s *stateChangeSuite) TestAppendEnum() {
	s.tk.MustExec(`create table t (
			c1 varchar(64),
			c2 enum('N','Y') not null default 'N',
			c3 timestamp on update current_timestamp,
			c4 int primary key,
			unique key idx2 (c2, c3))`)
	defer s.tk.MustExec("drop table t")
	s.tk.MustExec("insert into t values('a', 'N', '2017-07-01', 8)")
	// Make sure these SQLs use the plan of index scan.
	s.tk.MustExec("drop stats t")
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test_db_state")

	err := s.tk.ExecToErr("insert into t values('a', 'A', '2018-09-19', 9)")
	s.Require().EqualError(err, "[types:1265]Data truncated for column 'c2' at row 1")

	s.tk.MustExec("alter table t change c2 c2 enum('N') DEFAULT 'N'")
	s.tk.MustExec("alter table t change c2 c2 int default 0")
	s.tk.MustExec("alter table t change c2 c2 enum('N','Y','A') DEFAULT 'A'")
	s.tk.MustExec("insert into t values('a', 'A', '2018-09-20', 10)")
	s.tk.MustExec("insert into t (c1, c3, c4) values('a', '2018-09-21', 11)")

	tk = testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test_db_state")
	tk.MustQuery("select c4, c2 from t order by c4 asc").Check(testkit.Rows("8 N", "10 A", "11 A"))

	// fixed
	s.tk.MustExec("update t set c2='N' where c4 = 10")
	tk.MustQuery("select c2 from t where c4 = 10").Check(testkit.Rows("N"))

}

// https://github.com/pingcap/tidb/pull/6249 fixes the following two test cases.
func (s *stateChangeSuite) TestWriteOnlyWriteNULL() {
	sqls := make([]sqlWithErr, 1)
	sqls[0] = sqlWithErr{"insert t set c1 = 'c1_new', c3 = '2019-02-12', c4 = 8 on duplicate key update c1 = values(c1)", nil}
	addColumnSQL := "alter table t add column c5 int not null default 1 after c4"
	expectQuery := &expectQuery{"select c4, c5 from t", []string{"8 1"}}
	s.runTestInSchemaState(model.StateWriteOnly, true, addColumnSQL, sqls, expectQuery)
}

func (s *stateChangeSuite) TestWriteOnlyOnDupUpdate() {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t", nil}
	sqls[1] = sqlWithErr{"insert t set c1 = 'c1_dup', c3 = '2018-02-12', c4 = 2 on duplicate key update c1 = values(c1)", nil}
	sqls[2] = sqlWithErr{"insert t set c1 = 'c1_new', c3 = '2019-02-12', c4 = 2 on duplicate key update c1 = values(c1)", nil}
	addColumnSQL := "alter table t add column c5 int not null default 1 after c4"
	expectQuery := &expectQuery{"select c4, c5 from t", []string{"2 1"}}
	s.runTestInSchemaState(model.StateWriteOnly, true, addColumnSQL, sqls, expectQuery)
}

func (s *stateChangeSuite) TestWriteOnlyOnDupUpdateForAddColumns() {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t", nil}
	sqls[1] = sqlWithErr{"insert t set c1 = 'c1_dup', c3 = '2018-02-12', c4 = 2 on duplicate key update c1 = values(c1)", nil}
	sqls[2] = sqlWithErr{"insert t set c1 = 'c1_new', c3 = '2019-02-12', c4 = 2 on duplicate key update c1 = values(c1)", nil}
	addColumnsSQL := "alter table t add column c5 int not null default 1 after c4, add column c44 int not null default 1"
	expectQuery := &expectQuery{"select c4, c5, c44 from t", []string{"2 1 1"}}
	s.runTestInSchemaState(model.StateWriteOnly, true, addColumnsSQL, sqls, expectQuery)
}

func (s *stateChangeSuite) TestWriteReorgForModifyColumnTimestampToInt() {
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test_db_state")
	tk.MustExec("drop table if exists tt")
	tk.MustExec("create table tt(id int primary key auto_increment, c1 timestamp default '2020-07-10 01:05:08');")
	tk.MustExec("insert into tt values();")
	defer tk.MustExec("drop table if exists tt")

	sqls := make([]sqlWithErr, 1)
	sqls[0] = sqlWithErr{"insert into tt values();", nil}
	modifyColumnSQL := "alter table tt modify column c1 bigint;"
	expectQuery := &expectQuery{"select c1 from tt", []string{"20200710010508", "20200710010508"}}
	s.runTestInSchemaState(model.StateWriteReorganization, true, modifyColumnSQL, sqls, expectQuery)
}

type idxType byte

const (
	noneIdx    idxType = 0
	uniqIdx    idxType = 1
	primaryIdx idxType = 2
)

// TestWriteReorgForModifyColumn tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *stateChangeSuite) TestWriteReorgForModifyColumn() {
	modifyColumnSQL := "alter table tt change column c cc tinyint not null default 1 first"
	s.testModifyColumn(model.StateWriteReorganization, modifyColumnSQL, noneIdx)
}

// TestWriteReorgForModifyColumnWithUniqIdx tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *stateChangeSuite) TestWriteReorgForModifyColumnWithUniqIdx() {
	modifyColumnSQL := "alter table tt change column c cc tinyint unsigned not null default 1 first"
	s.testModifyColumn(model.StateWriteReorganization, modifyColumnSQL, uniqIdx)
}

// TestWriteReorgForModifyColumnWithPKIsHandle tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *stateChangeSuite) TestWriteReorgForModifyColumnWithPKIsHandle() {
	modifyColumnSQL := "alter table tt change column c cc tinyint not null default 1 first"

	s.tk.MustExec("use test_db_state")
	s.tk.MustExec("drop table if exists tt")
	s.tk.MustExec(`create table tt (a int not null, b int default 1, c int not null default 0, unique index idx(c), primary key idx1(a) clustered, index idx2(a, c))`)
	s.tk.MustExec("insert into tt (a, c) values(-1, -11)")
	s.tk.MustExec("insert into tt (a, c) values(1, 11)")
	defer s.tk.MustExec("drop table if exists tt")

	sqls := make([]sqlWithErr, 12)
	sqls[0] = sqlWithErr{"delete from tt where c = -11", nil}
	sqls[1] = sqlWithErr{"update tt use index(idx2) set a = 12, c = 555 where c = 11", errors.Errorf("[types:1690]constant 555 overflows tinyint")}
	sqls[2] = sqlWithErr{"update tt use index(idx2) set a = 12, c = 10 where c = 11", nil}
	sqls[3] = sqlWithErr{"insert into tt (a, c) values(2, 22)", nil}
	sqls[4] = sqlWithErr{"update tt use index(idx2) set a = 21, c = 2 where c = 22", nil}
	sqls[5] = sqlWithErr{"update tt use index(idx2) set a = 23 where c = 2", nil}
	sqls[6] = sqlWithErr{"insert tt set a = 31, c = 333", errors.Errorf("[types:1690]constant 333 overflows tinyint")}
	sqls[7] = sqlWithErr{"insert tt set a = 32, c = 123", nil}
	sqls[8] = sqlWithErr{"insert tt set a = 33", nil}
	sqls[9] = sqlWithErr{"insert into tt select * from tt order by c limit 1 on duplicate key update c = 44;", nil}
	sqls[10] = sqlWithErr{"replace into tt values(5, 55, 56)", nil}
	sqls[11] = sqlWithErr{"replace into tt values(6, 66, 56)", nil}

	query := &expectQuery{sql: "admin check table tt;", rows: nil}
	s.runTestInSchemaState(model.StateWriteReorganization, false, modifyColumnSQL, sqls, query)
}

// TestWriteReorgForModifyColumnWithPrimaryIdx tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *stateChangeSuite) TestWriteReorgForModifyColumnWithPrimaryIdx() {
	modifyColumnSQL := "alter table tt change column c cc tinyint not null default 1 first"
	s.testModifyColumn(model.StateWriteReorganization, modifyColumnSQL, uniqIdx)
}

// TestWriteReorgForModifyColumnWithoutFirst tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *stateChangeSuite) TestWriteReorgForModifyColumnWithoutFirst() {
	modifyColumnSQL := "alter table tt change column c cc tinyint not null default 1"
	s.testModifyColumn(model.StateWriteReorganization, modifyColumnSQL, noneIdx)
}

// TestWriteReorgForModifyColumnWithoutDefaultVal tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *stateChangeSuite) TestWriteReorgForModifyColumnWithoutDefaultVal() {
	modifyColumnSQL := "alter table tt change column c cc tinyint first"
	s.testModifyColumn(model.StateWriteReorganization, modifyColumnSQL, noneIdx)
}

// TestDeleteOnlyForModifyColumnWithoutDefaultVal tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *stateChangeSuite) TestDeleteOnlyForModifyColumnWithoutDefaultVal() {
	modifyColumnSQL := "alter table tt change column c cc tinyint first"
	s.testModifyColumn(model.StateDeleteOnly, modifyColumnSQL, noneIdx)
}

func (s *stateChangeSuite) testModifyColumn(state model.SchemaState, modifyColumnSQL string, idx idxType) {
	s.tk.MustExec("use test_db_state")
	s.tk.MustExec("drop table if exists tt")
	switch idx {
	case uniqIdx:
		s.tk.MustExec(`create table tt  (a varchar(64), b int default 1, c int not null default 0, unique index idx(c), unique index idx1(a), index idx2(a, c))`)
	case primaryIdx:
		// TODO: Support modify/change column with the primary key.
		s.tk.MustExec(`create table tt  (a varchar(64), b int default 1, c int not null default 0, index idx(c), primary index idx1(a), index idx2(a, c))`)
	default:
		s.tk.MustExec(`create table tt  (a varchar(64), b int default 1, c int not null default 0, index idx(c), index idx1(a), index idx2(a, c))`)
	}
	s.tk.MustExec("insert into tt (a, c) values('a', 11)")
	s.tk.MustExec("insert into tt (a, c) values('b', 22)")
	defer s.tk.MustExec("drop table if exists tt")

	sqls := make([]sqlWithErr, 13)
	sqls[0] = sqlWithErr{"delete from tt where c = 11", nil}
	if state == model.StateWriteReorganization {
		sqls[1] = sqlWithErr{"update tt use index(idx2) set a = 'a_update', c = 555 where c = 22", errors.Errorf("[types:1690]constant 555 overflows tinyint")}
		sqls[4] = sqlWithErr{"insert tt set a = 'a_insert', c = 333", errors.Errorf("[types:1690]constant 333 overflows tinyint")}
	} else {
		sqls[1] = sqlWithErr{"update tt use index(idx2) set a = 'a_update', c = 2 where c = 22", nil}
		sqls[4] = sqlWithErr{"insert tt set a = 'a_insert', b = 123, c = 111", nil}
	}
	sqls[2] = sqlWithErr{"update tt use index(idx2) set a = 'a_update', c = 2 where c = 22", nil}
	sqls[3] = sqlWithErr{"update tt use index(idx2) set a = 'a_update_1' where c = 2", nil}
	if idx == noneIdx {
		sqls[5] = sqlWithErr{"insert tt set a = 'a_insert', c = 111", nil}
	} else {
		sqls[5] = sqlWithErr{"insert tt set a = 'a_insert_1', c = 123", nil}
	}
	sqls[6] = sqlWithErr{"insert tt set a = 'a_insert_2'", nil}
	sqls[7] = sqlWithErr{"insert into tt select * from tt order by c limit 1 on duplicate key update c = 44;", nil}
	sqls[8] = sqlWithErr{"insert ignore into tt values('a_insert_2', 2, 0), ('a_insert_ignore_1', 1, 123), ('a_insert_ignore_1', 1, 33)", nil}
	sqls[9] = sqlWithErr{"insert ignore into tt values('a_insert_ignore_2', 1, 123) on duplicate key update c = 33 ", nil}
	sqls[10] = sqlWithErr{"insert ignore into tt values('a_insert_ignore_3', 1, 123) on duplicate key update c = 66 ", nil}
	sqls[11] = sqlWithErr{"replace into tt values('a_replace_1', 55, 56)", nil}
	sqls[12] = sqlWithErr{"replace into tt values('a_replace_2', 77, 56)", nil}

	query := &expectQuery{sql: "admin check table tt;", rows: nil}
	s.runTestInSchemaState(state, false, modifyColumnSQL, sqls, query)
}

// TestWriteOnly tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *stateChangeSuite) TestWriteOnly() {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t where c1 = 'a'", nil}
	sqls[1] = sqlWithErr{"update t use index(idx2) set c1 = 'c1_update' where c1 = 'a'", nil}
	sqls[2] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2018-02-12', c4 = 1", nil}
	addColumnSQL := "alter table t add column c5 int not null default 1 first"
	s.runTestInSchemaState(model.StateWriteOnly, true, addColumnSQL, sqls, nil)
}

// TestWriteOnlyForAddColumns tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *stateChangeSuite) TestWriteOnlyForAddColumns() {
	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"delete from t where c1 = 'a'", nil}
	sqls[1] = sqlWithErr{"update t use index(idx2) set c1 = 'c1_update' where c1 = 'a'", nil}
	sqls[2] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2018-02-12', c4 = 1", nil}
	addColumnsSQL := "alter table t add column c5 int not null default 1 first, add column c6 int not null default 1"
	s.runTestInSchemaState(model.StateWriteOnly, true, addColumnsSQL, sqls, nil)
}

// TestDeleteOnly tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *stateChangeSuite) TestDeleteOnly() {
	s.tk.MustExec("use test_db_state")
	s.tk.MustExec("drop table if exists tt")
	s.tk.MustExec(`create table tt (c varchar(64), c4 int)`)
	s.tk.MustExec("insert into tt (c, c4) values('a', 8)")
	defer s.tk.MustExec("drop table if exists tt")

	sqls := make([]sqlWithErr, 5)
	sqls[0] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2018-02-12', c4 = 1",
		errors.Errorf("Can't find column c1")}
	sqls[1] = sqlWithErr{"update t set c1 = 'c1_insert', c3 = '2018-02-12', c4 = 1",
		errors.Errorf("[planner:1054]Unknown column 'c1' in 'field list'")}
	sqls[2] = sqlWithErr{"delete from t where c1='a'",
		errors.Errorf("[planner:1054]Unknown column 'c1' in 'where clause'")}
	sqls[3] = sqlWithErr{"delete t, tt from tt inner join t on t.c4=tt.c4 where tt.c='a' and t.c1='a'",
		errors.Errorf("[planner:1054]Unknown column 't.c1' in 'where clause'")}
	sqls[4] = sqlWithErr{"delete t, tt from tt inner join t on t.c1=tt.c where tt.c='a'",
		errors.Errorf("[planner:1054]Unknown column 't.c1' in 'on clause'")}
	query := &expectQuery{sql: "select * from t;", rows: []string{"N 2017-07-01 00:00:00 8"}}
	dropColumnSQL := "alter table t drop column c1"
	s.runTestInSchemaState(model.StateDeleteOnly, true, dropColumnSQL, sqls, query)
}

// TestSchemaChangeForDropColumnWithIndexes test for modify data when a middle-state column with indexes in it.
func (s *stateChangeSuite) TestSchemaChangeForDropColumnWithIndexes() {
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test_db_state")
	sqls := make([]sqlWithErr, 5)
	sqls[0] = sqlWithErr{"delete from t1", nil}
	sqls[1] = sqlWithErr{"delete from t1 where b=1", errors.Errorf("[planner:1054]Unknown column 'b' in 'where clause'")}
	sqls[2] = sqlWithErr{"insert into t1(a) values(1);", nil}
	sqls[3] = sqlWithErr{"update t1 set a = 2 where a=1;", nil}
	sqls[4] = sqlWithErr{"delete from t1", nil}
	prepare := func() {
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t1(a bigint unsigned not null primary key, b int, c int, index idx(b));")
		tk.MustExec("insert into t1 values(1,1,1);")
	}
	prepare()
	dropColumnSQL := "alter table t1 drop column b"
	query := &expectQuery{sql: "select * from t1;", rows: []string{}}
	s.runTestInSchemaState(model.StateWriteOnly, true, dropColumnSQL, sqls, query)
	prepare()
	s.runTestInSchemaState(model.StateDeleteOnly, true, dropColumnSQL, sqls, query)
	prepare()
	s.runTestInSchemaState(model.StateDeleteReorganization, true, dropColumnSQL, sqls, query)
}

// TestSchemaChangeForDropColumnWithIndexes test for modify data when some middle-state columns with indexes in it.
func (s *stateChangeSuite) TestSchemaChangeForDropColumnsWithIndexes() {
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test_db_state")
	sqls := make([]sqlWithErr, 5)
	sqls[0] = sqlWithErr{"delete from t1", nil}
	sqls[1] = sqlWithErr{"delete from t1 where b=1", errors.Errorf("[planner:1054]Unknown column 'b' in 'where clause'")}
	sqls[2] = sqlWithErr{"insert into t1(a) values(1);", nil}
	sqls[3] = sqlWithErr{"update t1 set a = 2 where a=1;", nil}
	sqls[4] = sqlWithErr{"delete from t1", nil}
	prepare := func() {
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t1(a bigint unsigned not null primary key, b int, c int, d int, index idx(b), index idx2(d));")
		tk.MustExec("insert into t1 values(1,1,1,1);")
	}
	prepare()
	dropColumnSQL := "alter table t1 drop column b, drop column d"
	query := &expectQuery{sql: "select * from t1;", rows: []string{}}
	s.runTestInSchemaState(model.StateWriteOnly, true, dropColumnSQL, sqls, query)
	prepare()
	s.runTestInSchemaState(model.StateDeleteOnly, true, dropColumnSQL, sqls, query)
	prepare()
	s.runTestInSchemaState(model.StateDeleteReorganization, true, dropColumnSQL, sqls, query)
}

// TestDeleteOnlyForDropExpressionIndex tests for deleting data when the hidden column is delete-only state.
func (s *stateChangeSuite) TestDeleteOnlyForDropExpressionIndex() {
	s.tk.MustExec("use test_db_state")
	s.tk.MustExec("drop table if exists tt")
	s.tk.MustExec(`create table tt (a int, b int)`)
	s.tk.MustExec(`alter table tt add index expr_idx((a+1))`)
	s.tk.MustExec("insert into tt (a, b) values(8, 8)")
	defer s.tk.MustExec("drop table if exists tt")

	sqls := make([]sqlWithErr, 1)
	sqls[0] = sqlWithErr{"delete from tt where b=8", nil}
	dropIdxSQL := "alter table tt drop index expr_idx"
	s.runTestInSchemaState(model.StateDeleteOnly, true, dropIdxSQL, sqls, nil)

	s.tk.MustExec("admin check table tt")
}

// TestDeleteOnlyForDropColumns tests whether the correct columns is used in PhysicalIndexScan's ToPB function.
func (s *stateChangeSuite) TestDeleteOnlyForDropColumns() {
	sqls := make([]sqlWithErr, 1)
	sqls[0] = sqlWithErr{"insert t set c1 = 'c1_insert', c3 = '2018-02-12', c4 = 1",
		errors.Errorf("Can't find column c1")}
	dropColumnsSQL := "alter table t drop column c1, drop column c3"
	s.runTestInSchemaState(model.StateDeleteOnly, true, dropColumnsSQL, sqls, nil)
}

func (s *stateChangeSuite) TestWriteOnlyForDropColumn() {
	s.tk.MustExec("use test_db_state")
	s.tk.MustExec("drop table if exists tt")
	s.tk.MustExec(`create table tt (c1 int, c4 int)`)
	s.tk.MustExec("insert into tt (c1, c4) values(8, 8)")
	defer s.tk.MustExec("drop table if exists tt")

	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"update t set c1='5', c3='2020-03-01';", errors.New("[planner:1054]Unknown column 'c3' in 'field list'")}
	sqls[1] = sqlWithErr{"update t set c1='5', c3='2020-03-01' where c4 = 8;", errors.New("[planner:1054]Unknown column 'c3' in 'field list'")}
	sqls[2] = sqlWithErr{"update t t1, tt t2 set t1.c1='5', t1.c3='2020-03-01', t2.c1='10' where t1.c4=t2.c4",
		errors.New("[planner:1054]Unknown column 'c3' in 'field list'")}
	sqls[2] = sqlWithErr{"update t set c1='5' where c3='2017-07-01';", errors.New("[planner:1054]Unknown column 'c3' in 'where clause'")}
	dropColumnSQL := "alter table t drop column c3"
	query := &expectQuery{sql: "select * from t;", rows: []string{"a N 8"}}
	s.runTestInSchemaState(model.StateWriteOnly, false, dropColumnSQL, sqls, query)
}

func (s *stateChangeSuite) TestWriteOnlyForDropColumns() {
	s.tk.MustExec("use test_db_state")
	s.tk.MustExec(`create table t_drop_columns (c1 int, c4 int)`)
	s.tk.MustExec("insert into t_drop_columns (c1, c4) values(8, 8)")
	defer s.tk.MustExec("drop table t_drop_columns")

	sqls := make([]sqlWithErr, 3)
	sqls[0] = sqlWithErr{"update t set c1='5', c3='2020-03-01';", errors.New("[planner:1054]Unknown column 'c1' in 'field list'")}
	sqls[1] = sqlWithErr{"update t t1, t_drop_columns t2 set t1.c1='5', t1.c3='2020-03-01', t2.c1='10' where t1.c4=t2.c4",
		errors.New("[planner:1054]Unknown column 'c1' in 'field list'")}
	sqls[2] = sqlWithErr{"update t set c1='5' where c3='2017-07-01';", errors.New("[planner:1054]Unknown column 'c3' in 'where clause'")}
	dropColumnsSQL := "alter table t drop column c3, drop column c1"
	query := &expectQuery{sql: "select * from t;", rows: []string{"N 8"}}
	s.runTestInSchemaState(model.StateWriteOnly, false, dropColumnsSQL, sqls, query)
}

func (s *stateChangeSuite) runTestInSchemaState(
	state model.SchemaState,
	isOnJobUpdated bool,
	alterTableSQL string,
	sqlWithErrs []sqlWithErr,
	expectQuery *expectQuery,
) {
	s.tk.MustExec(`create table t (
	 	c1 varchar(64),
	 	c2 enum('N','Y') not null default 'N',
	 	c3 timestamp on update current_timestamp,
	 	c4 int primary key,
	 	unique key idx2 (c2))`)
	defer s.tk.MustExec("drop table t")
	s.tk.MustExec("insert into t values('a', 'N', '2017-07-01', 8)")
	// Make sure these SQLs use the plan of index scan.
	s.tk.MustExec("drop stats t")

	callback := &ddl.TestDDLCallback{Do: s.dom}
	prevState := model.StateNone
	var checkErr error
	se, err := session.CreateSession(s.store)
	s.Require().NoError(err)
	_, err = se.Execute(context.Background(), "use test_db_state")
	s.Require().NoError(err)
	cbFunc := func(job *model.Job) {
		if job.SchemaState == prevState || checkErr != nil {
			return
		}
		prevState = job.SchemaState
		if job.SchemaState != state {
			return
		}
		for _, sqlWithErr := range sqlWithErrs {
			_, err1 := se.Execute(context.Background(), sqlWithErr.sql)
			if !terror.ErrorEqual(err1, sqlWithErr.expectErr) {
				checkErr = errors.Errorf("sql: %s, expect err: %v, got err: %v", sqlWithErr.sql, sqlWithErr.expectErr, err1)
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
	d.SetHook(callback)
	s.tk.MustExec(alterTableSQL)
	s.Require().NoError(checkErr)
	d.SetHook(originalCallback)

	if expectQuery != nil {
		tk := testkit.NewTestKit(s.T(), s.store)
		tk.MustExec("use test_db_state")
		rs, _ := tk.Exec(expectQuery.sql)
		if expectQuery.rows == nil {
			s.Require().Nil(rs)
		} else {
			rows := tk.ResultSetToResult(rs, fmt.Sprintf("sql:%s", expectQuery.sql))
			rows.Check(testkit.Rows(expectQuery.rows...))
		}
	}
}

func (s *stateChangeSuite) TestShowIndex() {
	s.tk.MustExec(`create table t(c1 int primary key nonclustered, c2 int)`)
	defer s.tk.MustExec("drop table t")

	callback := &ddl.TestDDLCallback{}
	prevState := model.StateNone
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test_db_state")
	showIndexSQL := `show index from t`
	var checkErr error
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if job.SchemaState == prevState || checkErr != nil {
			return
		}
		switch job.SchemaState {
		case model.StateDeleteOnly, model.StateWriteOnly, model.StateWriteReorganization:
			result, err1 := tk.Exec(showIndexSQL)
			if err1 != nil {
				checkErr = err1
				break
			}
			rows := tk.ResultSetToResult(result, fmt.Sprintf("sql:%s", showIndexSQL))
			got := fmt.Sprintf("%s", rows.Rows())
			need := fmt.Sprintf("%s", testkit.Rows("t 0 PRIMARY 1 c1 A 0 <nil> <nil>  BTREE   YES <nil> NO"))
			if got != need {
				checkErr = fmt.Errorf("need %v, but got %v", need, got)
			}
		}
	}

	d := s.dom.DDL()
	originalCallback := d.GetHook()
	d.SetHook(callback)
	alterTableSQL := `alter table t add index c2(c2)`
	s.tk.MustExec(alterTableSQL)
	s.Require().NoError(checkErr)

	tk.MustQuery(showIndexSQL).Check(testkit.Rows(
		"t 0 PRIMARY 1 c1 A 0 <nil> <nil>  BTREE   YES <nil> NO",
		"t 1 c2 1 c2 A 0 <nil> <nil> YES BTREE   YES <nil> NO",
	))
	d.SetHook(originalCallback)

	s.tk.MustExec(`create table tr(
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
	defer s.tk.MustExec("drop table tr")
	s.tk.MustExec("create index idx1 on tr (purchased);")
	tk.MustQuery("show index from tr;").Check(testkit.Rows("tr 1 idx1 1 purchased A 0 <nil> <nil> YES BTREE   YES <nil> NO"))

	s.tk.MustExec("drop table if exists tr")
	s.tk.MustExec("create table tr(id int primary key clustered, v int, key vv(v))")
	tk.MustQuery("show index from tr").Check(testkit.Rows("tr 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES", "tr 1 vv 1 v A 0 <nil> <nil> YES BTREE   YES <nil> NO"))
	tk.MustQuery("select key_name, clustered from information_schema.tidb_indexes where table_name = 'tr' order by key_name").Check(testkit.Rows("PRIMARY YES", "vv NO"))

	s.tk.MustExec("drop table if exists tr")
	s.tk.MustExec("create table tr(id int primary key nonclustered, v int, key vv(v))")
	tk.MustQuery("show index from tr").Check(testkit.Rows("tr 1 vv 1 v A 0 <nil> <nil> YES BTREE   YES <nil> NO", "tr 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("select key_name, clustered from information_schema.tidb_indexes where table_name = 'tr' order by key_name").Check(testkit.Rows("PRIMARY NO", "vv NO"))

	s.tk.MustExec("drop table if exists tr")
	s.tk.MustExec("create table tr(id char(100) primary key clustered, v int, key vv(v))")
	tk.MustQuery("show index from tr").Check(testkit.Rows("tr 1 vv 1 v A 0 <nil> <nil> YES BTREE   YES <nil> NO", "tr 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> YES"))
	tk.MustQuery("select key_name, clustered from information_schema.tidb_indexes where table_name = 'tr' order by key_name").Check(testkit.Rows("PRIMARY YES", "vv NO"))

	s.tk.MustExec("drop table if exists tr")
	s.tk.MustExec("create table tr(id char(100) primary key nonclustered, v int, key vv(v))")
	tk.MustQuery("show index from tr").Check(testkit.Rows("tr 1 vv 1 v A 0 <nil> <nil> YES BTREE   YES <nil> NO", "tr 0 PRIMARY 1 id A 0 <nil> <nil>  BTREE   YES <nil> NO"))
	tk.MustQuery("select key_name, clustered from information_schema.tidb_indexes where table_name = 'tr' order by key_name").Check(testkit.Rows("PRIMARY NO", "vv NO"))
}

func (s *stateChangeSuite) TestParallelAlterModifyColumn() {
	sql := "ALTER TABLE t MODIFY COLUMN b int FIRST;"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().NoError(err2)
		s.tk.MustExec("select * from t")
	}
	s.testControlParallelExecSQL("", sql, sql, f)
}

func (s *stateChangeSuite) TestParallelAlterModifyColumnWithData() {
	// modify column: double -> int
	// modify column: double -> int
	sql := "ALTER TABLE t MODIFY COLUMN c int;"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[ddl:1072]column c id 3 does not exist, this column may have been updated by other DDL ran in parallel")
		rs, err := s.tk.Exec("select * from t")
		s.Require().NoError(err)
		sRows, err := session.ResultSetToStringSlice(context.Background(), s.tk.Session(), rs)
		s.Require().NoError(err)
		s.Require().Equal("3", sRows[0][2])
		s.Require().NoError(rs.Close())
		s.tk.MustExec("insert into t values(11, 22, 33.3, 44, 55)")
		rs, err = s.tk.Exec("select * from t")
		s.Require().NoError(err)
		sRows, err = session.ResultSetToStringSlice(context.Background(), s.tk.Session(), rs)
		s.Require().NoError(err)
		s.Require().Equal("33", sRows[1][2])
		s.Require().NoError(rs.Close())
	}
	s.testControlParallelExecSQL("", sql, sql, f)

	// modify column: int -> double
	// rename column: double -> int
	sql1 := "ALTER TABLE t MODIFY b double;"
	sql2 := "ALTER TABLE t RENAME COLUMN b to bb;"
	f = func(err1, err2 error) {
		s.Require().Nil(err1)
		s.Require().Nil(err2)
		rs, err := s.tk.Exec("select * from t")
		s.Require().NoError(err)
		sRows, err := session.ResultSetToStringSlice(context.Background(), s.tk.Session(), rs)
		s.Require().NoError(err)
		s.Require().Equal("2", sRows[0][1])
		s.Require().NoError(rs.Close())
		s.tk.MustExec("insert into t values(11, 22.2, 33, 44, 55)")
		rs, err = s.tk.Exec("select * from t")
		s.Require().NoError(err)
		sRows, err = session.ResultSetToStringSlice(context.Background(), s.tk.Session(), rs)
		s.Require().NoError(err)
		s.Require().Equal("22", sRows[1][1])
		s.Require().NoError(rs.Close())
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)

	// modify column: int -> double
	// modify column: double -> int
	sql2 = "ALTER TABLE t CHANGE b bb int;"
	f = func(err1, err2 error) {
		s.Require().Nil(err1)
		s.Require().Nil(err2)
		rs, err := s.tk.Exec("select * from t")
		s.Require().NoError(err)
		sRows, err := session.ResultSetToStringSlice(context.Background(), s.tk.Session(), rs)
		s.Require().NoError(err)
		s.Require().Equal("2", sRows[0][1])
		s.Require().NoError(rs.Close())
		s.tk.MustExec("insert into t values(11, 22.2, 33, 44, 55)")
		rs, err = s.tk.Exec("select * from t")
		s.Require().NoError(err)
		sRows, err = session.ResultSetToStringSlice(context.Background(), s.tk.Session(), rs)
		s.Require().NoError(err)
		s.Require().Equal("22", sRows[1][1])
		s.Require().NoError(rs.Close())
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

func (s *stateChangeSuite) TestParallelAlterModifyColumnToNotNullWithData() {
	// double null -> int not null
	// double null -> int not null
	sql := "ALTER TABLE t MODIFY COLUMN c int not null;"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[ddl:1072]column c id 3 does not exist, this column may have been updated by other DDL ran in parallel")
		rs, err := s.tk.Exec("select * from t")
		s.Require().NoError(err)
		sRows, err := session.ResultSetToStringSlice(context.Background(), s.tk.Session(), rs)
		s.Require().NoError(err)
		s.Require().Equal("3", sRows[0][2])
		s.Require().NoError(rs.Close())
		err = s.tk.ExecToErr("insert into t values(11, 22, null, 44, 55)")
		s.Require().Error(err)
		s.tk.MustExec("insert into t values(11, 22, 33.3, 44, 55)")
		rs, err = s.tk.Exec("select * from t")
		s.Require().NoError(err)
		sRows, err = session.ResultSetToStringSlice(context.Background(), s.tk.Session(), rs)
		s.Require().NoError(err)
		s.Require().Equal("33", sRows[1][2])
		s.Require().NoError(rs.Close())
	}
	s.testControlParallelExecSQL("", sql, sql, f)

	// int null -> double not null
	// double not null -> int null
	sql1 := "ALTER TABLE t CHANGE b b double not null;"
	sql2 := "ALTER TABLE t CHANGE b bb int null;"
	f = func(err1, err2 error) {
		s.Require().Nil(err1)
		s.Require().Nil(err2)
		rs, err := s.tk.Exec("select * from t")
		s.Require().NoError(err)
		sRows, err := session.ResultSetToStringSlice(context.Background(), s.tk.Session(), rs)
		s.Require().NoError(err)
		s.Require().Equal("2", sRows[0][1])
		s.Require().NoError(rs.Close())
		err = s.tk.ExecToErr("insert into t values(11, null, 33, 44, 55)")
		s.Require().NoError(err)
		s.tk.MustExec("insert into t values(11, 22.2, 33, 44, 55)")
		rs, err = s.tk.Exec("select * from t")
		s.Require().NoError(err)
		sRows, err = session.ResultSetToStringSlice(context.Background(), s.tk.Session(), rs)
		s.Require().NoError(err)
		s.Require().Equal("<nil>", sRows[1][1])
		s.Require().Equal("22", sRows[2][1])
		s.Require().NoError(rs.Close())
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

func (s *stateChangeSuite) TestParallelAddGeneratedColumnAndAlterModifyColumn() {
	sql1 := "ALTER TABLE t ADD COLUMN f INT GENERATED ALWAYS AS(a+1);"
	sql2 := "ALTER TABLE t MODIFY COLUMN a tinyint;"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[ddl:8200]Unsupported modify column: oldCol is a dependent column 'a' for generated column")
		s.tk.MustExec("select * from t")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

func (s *stateChangeSuite) TestParallelAlterModifyColumnAndAddPK() {
	sql1 := "ALTER TABLE t ADD PRIMARY KEY (b) NONCLUSTERED;"
	sql2 := "ALTER TABLE t MODIFY COLUMN b tinyint;"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[ddl:8200]Unsupported modify column: this column has primary key flag")
		s.tk.MustExec("select * from t")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

// TODO: This test is not a test that performs two DDLs in parallel.
// So we should not use the function of testControlParallelExecSQL. We will handle this test in the next PR.
// func (s *stateChangeSuite) TestParallelColumnModifyingDefinition() {
// 	sql1 := "insert into t(b) values (null);"
// 	sql2 := "alter table t change b b2 bigint not null;"
// 	f := func(err1, err2 error) {
// 		s.Require().NoError(err1)
// 		if err2 != nil {
//			s.Require().ErrorEqual(err2, "[ddl:1265]Data truncated for column 'b2' at row 1")
// 		}
// 	}
// 	s.testControlParallelExecSQL("", sql1, sql2, f)
// }

func (s *stateChangeSuite) TestParallelAddColumAndSetDefaultValue() {
	s.tk.MustExec("use test_db_state")
	s.tk.MustExec(`create table tx (
		c1 varchar(64),
		c2 enum('N','Y') not null default 'N',
		primary key idx2 (c2, c1))`)
	s.tk.MustExec("insert into tx values('a', 'N')")
	defer s.tk.MustExec("drop table tx")

	sql1 := "alter table tx add column cx int after c1"
	sql2 := "alter table tx alter c2 set default 'N'"

	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().NoError(err2)
		s.tk.MustExec("delete from tx where c1='a'")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

func (s *stateChangeSuite) TestParallelChangeColumnName() {
	sql1 := "ALTER TABLE t CHANGE a aa int;"
	sql2 := "ALTER TABLE t CHANGE b aa int;"
	f := func(err1, err2 error) {
		// Make sure only a DDL encounters the error of 'duplicate column name'.
		var oneErr error
		if (err1 != nil && err2 == nil) || (err1 == nil && err2 != nil) {
			if err1 != nil {
				oneErr = err1
			} else {
				oneErr = err2
			}
		}
		s.Require().EqualError(oneErr, "[schema:1060]Duplicate column name 'aa'")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

func (s *stateChangeSuite) TestParallelAlterAddIndex() {
	sql1 := "ALTER TABLE t add index index_b(b);"
	sql2 := "CREATE INDEX index_b ON t (c);"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[ddl:1061]index already exist index_b")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

func (s *stateChangeSuite) TestParallelAlterAddExpressionIndex() {
	sql1 := "ALTER TABLE t add index expr_index_b((b+1));"
	sql2 := "CREATE INDEX expr_index_b ON t ((c+1));"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[ddl:1061]index already exist expr_index_b")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

func (s *stateChangeSuite) TestParallelAddPrimaryKey() {
	sql1 := "ALTER TABLE t add primary key index_b(b);"
	sql2 := "ALTER TABLE t add primary key index_b(c);"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[schema:1068]Multiple primary key defined")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

func (s *stateChangeSuite) TestParallelAlterAddPartition() {
	sql1 := `alter table t_part add partition (
    partition p2 values less than (30)
   );`
	sql2 := `alter table t_part add partition (
    partition p3 values less than (30)
   );`
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[ddl:1493]VALUES LESS THAN value must be strictly increasing for each partition")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

func (s *stateChangeSuite) TestParallelDropColumn() {
	sql := "ALTER TABLE t drop COLUMN c ;"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[ddl:1091]column c doesn't exist")
	}
	s.testControlParallelExecSQL("", sql, sql, f)
}

func (s *stateChangeSuite) TestParallelDropColumns() {
	sql := "ALTER TABLE t drop COLUMN b, drop COLUMN c;"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[ddl:1091]column b doesn't exist")
	}
	s.testControlParallelExecSQL("", sql, sql, f)
}

func (s *stateChangeSuite) TestParallelDropIfExistsColumns() {
	sql := "ALTER TABLE t drop COLUMN if exists b, drop COLUMN if exists c;"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().NoError(err2)
	}
	s.testControlParallelExecSQL("", sql, sql, f)
}

func (s *stateChangeSuite) TestParallelDropIndex() {
	sql1 := "alter table t drop index idx1 ;"
	sql2 := "alter table t drop index idx2 ;"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[autoid:1075]Incorrect table definition; there can be only one auto column and it must be defined as a key")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

func (s *stateChangeSuite) TestParallelDropPrimaryKey() {
	sql1 := "alter table t drop primary key;"
	sql2 := "alter table t drop primary key;"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[ddl:1091]index PRIMARY doesn't exist")
	}
	s.testControlParallelExecSQL("ALTER TABLE t add primary key index_b(c);", sql1, sql2, f)
}

func (s *stateChangeSuite) TestParallelCreateAndRename() {
	sql1 := "create table t_exists(c int);"
	sql2 := "alter table t rename to t_exists;"
	defer s.tk.MustExec("drop table if exists t_exists ")
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[schema:1050]Table 't_exists' already exists")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

func (s *stateChangeSuite) TestParallelAlterAndDropSchema() {
	s.tk.MustExec("create database db_drop_db")
	sql1 := "DROP SCHEMA db_drop_db"
	sql2 := "ALTER SCHEMA db_drop_db CHARSET utf8mb4 COLLATE utf8mb4_general_ci"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[schema:1008]Can't drop database ''; database doesn't exist")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

func (s *stateChangeSuite) prepareTestControlParallelExecSQL() (*testkit.TestKit, *testkit.TestKit, chan struct{}, ddl.Callback) {
	callback := &ddl.TestDDLCallback{}
	times := 0
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if times != 0 {
			return
		}
		var qLen int
		for {
			err := kv.RunInNewTxn(context.Background(), s.store, false, func(ctx context.Context, txn kv.Transaction) error {
				jobs, err1 := ddl.GetDDLJobs(txn)
				if err1 != nil {
					return err1
				}
				qLen = len(jobs)
				return nil
			})
			s.Require().NoError(err)
			if qLen == 2 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		times++
	}
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	d.SetHook(callback)

	tk1 := testkit.NewTestKit(s.T(), s.store)
	tk1.MustExec("use test_db_state")

	tk2 := testkit.NewTestKit(s.T(), s.store)
	tk2.MustExec("use test_db_state")
	ch := make(chan struct{})
	// Make sure the sql1 is put into the DDLJobQueue.
	go func() {
		var qLen int
		for {
			err := kv.RunInNewTxn(context.Background(), s.store, false, func(ctx context.Context, txn kv.Transaction) error {
				jobs, err3 := ddl.GetDDLJobs(txn)
				if err3 != nil {
					return err3
				}
				qLen = len(jobs)
				return nil
			})
			s.Require().NoError(err)
			if qLen == 1 {
				// Make sure sql2 is executed after the sql1.
				close(ch)
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()
	return tk1, tk2, ch, originalCallback
}

func (s *stateChangeSuite) testControlParallelExecSQL(preSQL, sql1, sql2 string, f func(e1, e2 error)) {
	s.tk.MustExec("use test_db_state")
	s.tk.MustExec("create table t(a int, b int, c double default null, d int auto_increment,e int, index idx1(d), index idx2(d,e))")
	if len(preSQL) != 0 {
		s.tk.MustExec(preSQL)
	}
	s.tk.MustExec("insert into t values(1, 2, 3.1234, 4, 5)")

	defer s.tk.MustExec("drop table t")

	// fixed
	s.tk.MustExec("drop table if exists t_part")
	s.tk.MustExec(`create table t_part (a int key)
	 	partition by range(a) (
	 	partition p0 values less than (10),
	 	partition p1 values less than (20)
	 	);`)

	tk1, tk2, ch, originalCallback := s.prepareTestControlParallelExecSQL()
	defer s.dom.DDL().SetHook(originalCallback)

	var err1 error
	var err2 error
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		var rs sqlexec.RecordSet
		rs, err1 = tk1.Exec(sql1)
		if err1 == nil && rs != nil {
			s.Require().NoError(rs.Close())
		}
	})
	wg.Run(func() {
		<-ch
		var rs sqlexec.RecordSet
		rs, err2 = tk2.Exec(sql2)
		if err2 == nil && rs != nil {
			s.Require().NoError(rs.Close())
		}
	})

	wg.Wait()
	f(err1, err2)
}

func (s *stateChangeSuite) TestParallelUpdateTableReplica() {
	s.Require().NoError(failpoint.Enable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount", `return(true)`))
	defer func() {
		s.Require().NoError(failpoint.Disable("github.com/pingcap/tidb/infoschema/mockTiFlashStoreCount"))
	}()

	s.tk.MustExec("use test_db_state")
	s.tk.MustExec("drop table if exists t1;")
	s.tk.MustExec("create table t1 (a int);")
	s.tk.MustExec("alter table t1 set tiflash replica 3 location labels 'a','b';")

	tk1, tk2, ch, originalCallback := s.prepareTestControlParallelExecSQL()
	defer s.dom.DDL().SetHook(originalCallback)

	t1 := external.GetTableByName(s.T(), s.tk, "test_db_state", "t1")

	var err1 error
	var err2 error
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		// Mock for table tiflash replica was available.
		err1 = domain.GetDomain(tk1.Session()).DDL().UpdateTableReplicaInfo(tk1.Session(), t1.Meta().ID, true)
	})
	wg.Run(func() {
		<-ch
		// Mock for table tiflash replica was available.
		err2 = domain.GetDomain(tk2.Session()).DDL().UpdateTableReplicaInfo(tk2.Session(), t1.Meta().ID, true)
	})
	wg.Wait()
	s.Require().NoError(err1)
	s.Require().EqualError(err2, "[ddl:-1]the replica available status of table t1 is already updated")
}

func (s *stateChangeSuite) testParallelExecSQL(sql string) {
	tk1 := testkit.NewTestKit(s.T(), s.store)
	tk1.MustExec("use test_db_state")

	tk2 := testkit.NewTestKit(s.T(), s.store)
	tk2.MustExec("use test_db_state")

	var err2, err3 error
	var wg util.WaitGroupWrapper

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
	wg.Run(func() {
		err2 = tk1.ExecToErr(sql)
	})
	wg.Run(func() {
		err3 = tk2.ExecToErr(sql)
	})
	wg.Wait()
	s.Require().NoError(err2)
	s.Require().NoError(err3)
}

// TestCreateTableIfNotExists parallel exec create table if not exists xxx. No error returns is expected.
func (s *stateChangeSuite) TestCreateTableIfNotExists() {
	defer s.tk.MustExec("drop table test_not_exists")
	s.testParallelExecSQL("create table if not exists test_not_exists(a int);")
}

// TestCreateDBIfNotExists parallel exec create database if not exists xxx. No error returns is expected.
func (s *stateChangeSuite) TestCreateDBIfNotExists() {
	defer s.tk.MustExec("drop database test_not_exists")
	s.testParallelExecSQL("create database if not exists test_not_exists;")
}

// TestDDLIfNotExists parallel exec some DDLs with `if not exists` clause. No error returns is expected.
func (s *stateChangeSuite) TestDDLIfNotExists() {
	defer s.tk.MustExec("drop table test_not_exists")
	s.tk.MustExec("create table if not exists test_not_exists(a int)")

	// ADD COLUMN
	s.testParallelExecSQL("alter table test_not_exists add column if not exists b int")

	// ADD COLUMNS
	s.testParallelExecSQL("alter table test_not_exists add column if not exists (c11 int, d11 int)")

	// ADD INDEX
	s.testParallelExecSQL("alter table test_not_exists add index if not exists idx_b (b)")

	// CREATE INDEX
	s.testParallelExecSQL("create index if not exists idx_b on test_not_exists (b)")
}

// TestDDLIfExists parallel exec some DDLs with `if exists` clause. No error returns is expected.
func (s *stateChangeSuite) TestDDLIfExists() {
	defer func() {
		s.tk.MustExec("drop table test_exists")
		s.tk.MustExec("drop table test_exists_2")
	}()
	s.tk.MustExec("create table if not exists test_exists (a int key, b int)")

	// DROP COLUMNS
	s.testParallelExecSQL("alter table test_exists drop column if exists c, drop column if exists d")

	// DROP COLUMN
	s.testParallelExecSQL("alter table test_exists drop column if exists b") // only `a` exists now

	// CHANGE COLUMN
	s.testParallelExecSQL("alter table test_exists change column if exists a c int") // only, `c` exists now

	// MODIFY COLUMN
	s.testParallelExecSQL("alter table test_exists modify column if exists a bigint")

	// DROP INDEX
	s.tk.MustExec("alter table test_exists add index idx_c (c)")
	s.testParallelExecSQL("alter table test_exists drop index if exists idx_c")

	// DROP PARTITION (ADD PARTITION tested in TestParallelAlterAddPartition)
	s.tk.MustExec("create table test_exists_2 (a int key) partition by range(a) (partition p0 values less than (10), partition p1 values less than (20), partition p2 values less than (30))")
	s.testParallelExecSQL("alter table test_exists_2 drop partition if exists p1")
}

// TestParallelDDLBeforeRunDDLJob tests a session to execute DDL with an outdated information schema.
// This test is used to simulate the following conditions:
// In a cluster, TiDB "a" executes the DDL.
// TiDB "b" fails to load schema, then TiDB "b" executes the DDL statement associated with the DDL statement executed by "a".
func (s *stateChangeSuite) TestParallelDDLBeforeRunDDLJob() {
	defer s.tk.MustExec("drop table test_table")
	s.tk.MustExec("use test_db_state")
	s.tk.MustExec("create table test_table (c1 int, c2 int default 1, index (c1))")

	// Create two sessions.
	tk1 := testkit.NewTestKit(s.T(), s.store)
	tk1.MustExec("use test_db_state")

	tk2 := testkit.NewTestKit(s.T(), s.store)
	tk2.MustExec("use test_db_state")

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
			// or the first session finished this SQL(seCnt = finishedCnt), then other sessions can continue to go on.
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
	d.(ddl.DDLForTest).SetInterceptor(intercept)

	// Make sure the connection 1 executes a SQL before the connection 2.
	// And the connection 2 executes a SQL with an outdated information schema.
	var wg util.WaitGroupWrapper
	wg.Run(func() {
		tk1.Session().SetConnectionID(firstConnID)
		tk1.MustExec("alter table test_table drop column c2")
		// Sleep a while to make sure the connection 2 break out the first for loop in OnGetInfoSchemaExported, otherwise atomic.LoadInt32(&sessionCnt) == 2 will be false forever.
		time.Sleep(100 * time.Millisecond)
		atomic.StoreInt32(&sessionCnt, finishedCnt)
	})
	wg.Run(func() {
		tk2.Session().SetConnectionID(2)
		err := tk2.ExecToErr("alter table test_table add column c2 int")
		s.Require().Error(err)
		s.Require().Contains(err.Error(), "Information schema is changed")
	})

	wg.Wait()

	intercept = &ddl.TestInterceptor{}
	d.(ddl.DDLForTest).SetInterceptor(intercept)
}

func (s *stateChangeSuite) TestParallelAlterSchemaCharsetAndCollate() {
	sql := "ALTER SCHEMA test_db_state CHARSET utf8mb4 COLLATE utf8mb4_general_ci"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().NoError(err2)
	}
	s.testControlParallelExecSQL("", sql, sql, f)
	sql = `SELECT default_character_set_name, default_collation_name
			FROM information_schema.schemata
			WHERE schema_name='test_db_state'`
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustQuery(sql).Check(testkit.Rows("utf8mb4 utf8mb4_general_ci"))
}

// TestParallelTruncateTableAndAddColumn tests add column when truncate table.
func (s *stateChangeSuite) TestParallelTruncateTableAndAddColumn() {
	sql1 := "truncate table t"
	sql2 := "alter table t add column c3 int"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[domain:8028]Information schema is changed during the execution of the statement(for example, table definition may be updated by other DDL ran in parallel). If you see this error often, try increasing `tidb_max_delta_schema_count`. [try again later]")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

// TestParallelTruncateTableAndAddColumns tests add columns when truncate table.
func (s *stateChangeSuite) TestParallelTruncateTableAndAddColumns() {
	sql1 := "truncate table t"
	sql2 := "alter table t add column c3 int, add column c4 int"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[domain:8028]Information schema is changed during the execution of the statement(for example, table definition may be updated by other DDL ran in parallel). If you see this error often, try increasing `tidb_max_delta_schema_count`. [try again later]")
	}
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

// TestParallelFlashbackTable tests parallel flashback table.
func (s *stateChangeSuite) TestParallelFlashbackTable() {
	s.Require().NoError(failpoint.Enable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange", `return(true)`))
	defer func(originGC bool) {
		s.Require().NoError(failpoint.Disable("github.com/pingcap/tidb/meta/autoid/mockAutoIDChange"))
		if originGC {
			ddlutil.EmulatorGCEnable()
		} else {
			ddlutil.EmulatorGCDisable()
		}
	}(ddlutil.IsEmulatorGCEnable())

	// disable emulator GC.
	// Disable emulator GC, otherwise, emulator GC will delete table record as soon as possible after executing drop table DDL.
	ddlutil.EmulatorGCDisable()
	gcTimeFormat := "20060102-15:04:05 -0700 MST"
	timeBeforeDrop := time.Now().Add(0 - 48*60*60*time.Second).Format(gcTimeFormat)
	safePointSQL := `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('tikv_gc_safe_point', '%[1]s', '')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[1]s'`
	tk := testkit.NewTestKit(s.T(), s.store)
	// clear GC variables first.
	tk.MustExec("delete from mysql.tidb where variable_name in ( 'tikv_gc_safe_point','tikv_gc_enable' )")
	// set GC safe point
	tk.MustExec(fmt.Sprintf(safePointSQL, timeBeforeDrop))
	// set GC enable.
	err := gcutil.EnableGC(tk.Session())
	s.Require().NoError(err)

	// prepare dropped table.
	tk.MustExec("use test_db_state")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int);")
	tk.MustExec("drop table if exists t")
	// Test parallel flashback table.
	sql1 := "flashback table t to t_flashback"
	f := func(err1, err2 error) {
		s.Require().NoError(err1)
		s.Require().EqualError(err2, "[schema:1050]Table 't_flashback' already exists")
	}
	s.testControlParallelExecSQL("", sql1, sql1, f)

	// Test parallel flashback table with different name
	tk.MustExec("drop table t_flashback")
	sql1 = "flashback table t_flashback"
	sql2 := "flashback table t_flashback to t_flashback2"
	s.testControlParallelExecSQL("", sql1, sql2, f)
}

// TestModifyColumnTypeArgs test job raw args won't be updated when error occurs in `updateVersionAndTableInfo`.
func (s *stateChangeSuite) TestModifyColumnTypeArgs() {
	s.Require().NoError(failpoint.Enable("github.com/pingcap/tidb/ddl/mockUpdateVersionAndTableInfoErr", `return(2)`))
	defer func() {
		s.Require().NoError(failpoint.Disable("github.com/pingcap/tidb/ddl/mockUpdateVersionAndTableInfoErr"))
	}()

	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_modify_column_args")
	tk.MustExec("create table t_modify_column_args(a int, unique(a))")

	err := tk.ExecToErr("alter table t_modify_column_args modify column a tinyint")
	s.Require().Error(err)
	// error goes like `mock update version and tableInfo error,jobID=xx`
	strs := strings.Split(err.Error(), ",")
	s.Require().Equal("[ddl:-1]mock update version and tableInfo error", strs[0])

	jobID := strings.Split(strs[1], "=")[1]
	tbl := external.GetTableByName(s.T(), tk, "test", "t_modify_column_args")
	s.Require().Len(tbl.Meta().Columns, 1)
	s.Require().Len(tbl.Meta().Indices, 1)

	id, err := strconv.Atoi(jobID)
	s.Require().NoError(err)
	var historyJob *model.Job
	err = kv.RunInNewTxn(context.Background(), s.store, false, func(ctx context.Context, txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		historyJob, err = t.GetHistoryDDLJob(int64(id))
		if err != nil {
			return err
		}
		return nil
	})
	s.Require().NoError(err)
	s.Require().NotNil(historyJob)

	var (
		newCol                *model.ColumnInfo
		oldColName            *model.CIStr
		modifyColumnTp        byte
		updatedAutoRandomBits uint64
		changingCol           *model.ColumnInfo
		changingIdxs          []*model.IndexInfo
	)
	pos := &ast.ColumnPosition{}
	err = historyJob.DecodeArgs(&newCol, &oldColName, pos, &modifyColumnTp, &updatedAutoRandomBits, &changingCol, &changingIdxs)
	s.Require().NoError(err)
	s.Require().Nil(changingCol)
	s.Require().Nil(changingIdxs)
}

func (s *stateChangeSuite) TestWriteReorgForColumnTypeChange() {
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test_db_state")
	tk.MustExec(`CREATE TABLE t_ctc (
  a DOUBLE NULL DEFAULT '1.732088511183121',
  c char(30) NOT NULL,
  KEY idx (a,c)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_bin COMMENT='…comment';
`)
	defer tk.MustExec("drop table t_ctc")

	sqls := make([]sqlWithErr, 2)
	sqls[0] = sqlWithErr{"INSERT INTO t_ctc SET c = 'zr36f7ywjquj1curxh9gyrwnx', a = '1.9897043136824033';", nil}
	sqls[1] = sqlWithErr{"DELETE FROM t_ctc;", nil}
	dropColumnsSQL := "alter table t_ctc change column a ddd TIME NULL DEFAULT '18:21:32' AFTER c;"
	query := &expectQuery{sql: "admin check table t_ctc;", rows: nil}
	s.runTestInSchemaState(model.StateWriteReorganization, false, dropColumnsSQL, sqls, query)
}

func (s *stateChangeSuite) TestCreateExpressionIndex() {
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test_db_state")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int default 0, b int default 0)")
	defer tk.MustExec("drop table t")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3), (4, 4)")

	tk1 := testkit.NewTestKit(s.T(), s.store)
	tk1.MustExec("use test_db_state")

	stateDeleteOnlySQLs := []string{"insert into t values (5, 5)", "begin pessimistic;", "insert into t select * from t", "rollback", "insert into t set b = 6", "update t set b = 7 where a = 1", "delete from t where b = 4"}
	stateWriteOnlySQLs := []string{"insert into t values (8, 8)", "begin pessimistic;", "insert into t select * from t", "rollback", "insert into t set b = 9", "update t set b = 7 where a = 2", "delete from t where b = 3"}
	stateWriteReorganizationSQLs := []string{"insert into t values (10, 10)", "begin pessimistic;", "insert into t select * from t", "rollback", "insert into t set b = 11", "update t set b = 7 where a = 5", "delete from t where b = 6"}

	// If waitReorg timeout, the worker may enter writeReorg more than 2 times.
	reorgTime := 0
	var checkErr error
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	defer d.SetHook(originalCallback)
	callback := &ddl.TestDDLCallback{}
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		err := originalCallback.OnChanged(nil)
		s.Require().NoError(err)
		switch job.SchemaState {
		case model.StateDeleteOnly:
			for _, sql := range stateDeleteOnlySQLs {
				_, checkErr = tk1.Exec(sql)
				if checkErr != nil {
					return
				}
			}
			// (1, 7), (2, 2), (3, 3), (5, 5), (0, 6)
		case model.StateWriteOnly:
			for _, sql := range stateWriteOnlySQLs {
				_, checkErr = tk1.Exec(sql)
				if checkErr != nil {
					return
				}
			}
			// (1, 7), (2, 7), (5, 5), (0, 6), (8, 8), (0, 9)
		case model.StateWriteReorganization:
			if reorgTime < 2 {
				reorgTime++
			} else {
				return
			}
			for _, sql := range stateWriteReorganizationSQLs {
				_, checkErr = tk1.Exec(sql)
				if checkErr != nil {
					return
				}
			}
			// (1, 7), (2, 7), (5, 7), (8, 8), (0, 9), (10, 10), (10, 10), (0, 11), (0, 11)
		}
	}

	d.SetHook(callback)
	tk.MustExec("alter table t add index idx((b+1))")
	s.Require().NoError(checkErr)
	tk.MustExec("admin check table t")
	tk.MustQuery("select * from t order by a, b").Check(testkit.Rows("0 9", "0 11", "0 11", "1 7", "2 7", "5 7", "8 8", "10 10", "10 10"))
}

func (s *stateChangeSuite) TestCreateUniqueExpressionIndex() {
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test_db_state")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int default 0, b int default 0)")
	defer tk.MustExec("drop table t")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3), (4, 4)")

	tk1 := testkit.NewTestKit(s.T(), s.store)
	tk1.MustExec("use test_db_state")

	stateDeleteOnlySQLs := []string{"insert into t values (5, 5)", "begin pessimistic;", "insert into t select * from t", "rollback", "insert into t set b = 6", "update t set b = 7 where a = 1", "delete from t where b = 4"}

	// If waitReorg timeout, the worker may enter writeReorg more than 2 times.
	reorgTime := 0
	var checkErr error
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	defer d.SetHook(originalCallback)
	callback := &ddl.TestDDLCallback{}
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		err := originalCallback.OnChanged(nil)
		s.Require().NoError(err)
		switch job.SchemaState {
		case model.StateDeleteOnly:
			for _, sql := range stateDeleteOnlySQLs {
				_, checkErr = tk1.Exec(sql)
				if checkErr != nil {
					return
				}
			}
			// (1, 7), (2, 2), (3, 3), (5, 5), (0, 6)
		case model.StateWriteOnly:
			_, checkErr = tk1.Exec("insert into t values (8, 8)")
			if checkErr != nil {
				return
			}
			_, checkErr = tk1.Exec("begin pessimistic;")
			if checkErr != nil {
				return
			}
			_, tmpErr := tk1.Exec("insert into t select * from t")
			if tmpErr == nil {
				checkErr = errors.New("should not be nil")
				return
			}
			_, checkErr = tk1.Exec("rollback")
			if checkErr != nil {
				return
			}
			_, checkErr = tk1.Exec("insert into t set b = 9")
			if checkErr != nil {
				return
			}
			_, checkErr = tk1.Exec("update t set b = 7 where a = 2")
			if checkErr != nil {
				return
			}
			_, checkErr = tk1.Exec("delete from t where b = 3")
			if checkErr != nil {
				return
			}
			// (1, 7), (2, 7), (5, 5), (0, 6), (8, 8), (0, 9)
		case model.StateWriteReorganization:
			if reorgTime < 2 {
				reorgTime++
			} else {
				return
			}
			_, checkErr = tk1.Exec("insert into t values (10, 10) on duplicate key update a = 11")
			if checkErr != nil {
				return
			}
			_, checkErr = tk1.Exec("begin pessimistic;")
			if checkErr != nil {
				return
			}
			_, tmpErr := tk1.Exec("insert into t select * from t")
			if tmpErr == nil {
				checkErr = errors.New("should not be nil")
				return
			}
			_, checkErr = tk1.Exec("rollback")
			if checkErr != nil {
				return
			}
			_, checkErr = tk1.Exec("insert into t set b = 11 on duplicate key update a = 13")
			if checkErr != nil {
				return
			}
			_, checkErr = tk1.Exec("update t set b = 7 where a = 5")
			if checkErr != nil {
				return
			}
			_, checkErr = tk1.Exec("delete from t where b = 6")
			if checkErr != nil {
				return
			}
			// (1, 7), (2, 7), (5, 7), (8, 8), (13, 9), (11, 10), (0, 11)
		}
	}

	d.SetHook(callback)
	tk.MustExec("alter table t add unique index idx((a*b+1))")
	s.Require().NoError(checkErr)
	tk.MustExec("admin check table t")
	tk.MustQuery("select * from t order by a, b").Check(testkit.Rows("0 11", "1 7", "2 7", "5 7", "8 8", "11 10", "13 9"))
}

func (s *stateChangeSuite) TestDropExpressionIndex() {
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test_db_state")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int default 0, b int default 0, key idx((b+1)))")
	defer tk.MustExec("drop table t")
	tk.MustExec("insert into t values (1, 1), (2, 2), (3, 3), (4, 4)")

	tk1 := testkit.NewTestKit(s.T(), s.store)
	tk1.MustExec("use test_db_state")
	stateDeleteOnlySQLs := []string{"insert into t values (5, 5)", "begin pessimistic;", "insert into t select * from t", "rollback", "insert into t set b = 6", "update t set b = 7 where a = 1", "delete from t where b = 4"}
	stateWriteOnlySQLs := []string{"insert into t values (8, 8)", "begin pessimistic;", "insert into t select * from t", "rollback", "insert into t set b = 9", "update t set b = 7 where a = 2", "delete from t where b = 3"}
	stateWriteReorganizationSQLs := []string{"insert into t values (10, 10)", "begin pessimistic;", "insert into t select * from t", "rollback", "insert into t set b = 11", "update t set b = 7 where a = 5", "delete from t where b = 6"}

	var checkErr error
	d := s.dom.DDL()
	originalCallback := d.GetHook()
	defer d.SetHook(originalCallback)
	callback := &ddl.TestDDLCallback{}
	callback.OnJobUpdatedExported = func(job *model.Job) {
		if checkErr != nil {
			return
		}
		err := originalCallback.OnChanged(nil)
		s.Require().NoError(err)
		switch job.SchemaState {
		case model.StateDeleteOnly:
			for _, sql := range stateDeleteOnlySQLs {
				_, checkErr = tk1.Exec(sql)
				if checkErr != nil {
					return
				}
			}
			// (1, 7), (2, 7), (5, 5), (8, 8), (0, 9), (0, 6)
		case model.StateWriteOnly:
			for _, sql := range stateWriteOnlySQLs {
				_, checkErr = tk1.Exec(sql)
				if checkErr != nil {
					return
				}
			}
			// (1, 1), (2, 7), (4, 4), (8, 8), (0, 9)
		case model.StateDeleteReorganization:
			for _, sql := range stateWriteReorganizationSQLs {
				_, checkErr = tk1.Exec(sql)
				if checkErr != nil {
					return
				}
			}
			// (1, 7), (2, 7), (5, 7), (8, 8), (0, 9), (10, 10), (0, 11)
		}
	}

	d.SetHook(callback)
	tk.MustExec("alter table t drop index idx")
	s.Require().NoError(checkErr)
	tk.MustExec("admin check table t")
	tk.MustQuery("select * from t order by a, b").Check(testkit.Rows("0 9", "0 11", "1 7", "2 7", "5 7", "8 8", "10 10"))
}

func (s *stateChangeSuite) TestExpressionIndexDDLError() {
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test_db_state")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx((a+b)))")
	tk.MustGetErrCode("alter table t rename column b to b2", errno.ErrDependentByFunctionalIndex)
	tk.MustGetErrCode("alter table t drop column b", errno.ErrDependentByFunctionalIndex)
	tk.MustExec("drop table t")
}

func (s *stateChangeSuite) TestRestrainDropColumnWithIndex() {
	tk := testkit.NewTestKit(s.T(), s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, index(a));")
	tk.MustExec("set @@GLOBAL.tidb_enable_change_multi_schema=0")
	tk.MustQuery("select @@tidb_enable_change_multi_schema").Check(testkit.Rows("0"))
	tk.MustGetErrCode("alter table t drop column a;", errno.ErrUnsupportedDDLOperation)
	tk.MustExec("set @@GLOBAL.tidb_enable_change_multi_schema=1")
	tk.MustExec("alter table t drop column a;")
	tk.MustExec("drop table if exists t;")
}

func TestParallelRenameTable(t *testing.T) {
	store, d, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create database test2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int default 0, b int default 0, key idx((b+1)))")
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk3 := testkit.NewTestKit(t, store)
	tk3.MustExec("use test")

	var concurrentDDLQueryPre string
	var concurrentDDLQuery string
	firstDDL := true

	var wg sync.WaitGroup
	var checkErr error
	d2 := d.DDL()
	originalCallback := d2.GetHook()
	defer d2.SetHook(originalCallback)
	callback := &ddl.TestDDLCallback{Do: d}
	callback.OnJobRunBeforeExported = func(job *model.Job) {
		switch job.SchemaState {
		case model.StateNone:
			if firstDDL {
				firstDDL = false
			} else {
				return
			}
			wg.Add(1)
			go func() {
				if concurrentDDLQueryPre != "" {
					wg.Add(1)
					go func() {
						// We assume that no error, we don't want to test it.
						tk3.MustExec(concurrentDDLQueryPre)
						wg.Done()
					}()
					time.Sleep(10 * time.Millisecond)
				}
				_, err := tk1.Exec(concurrentDDLQuery)
				if err != nil {
					checkErr = err
				}
				wg.Done()
			}()
			time.Sleep(10 * time.Millisecond)
		}
	}

	d2.SetHook(callback)

	// rename then add column
	concurrentDDLQuery = "alter table t add column g int"
	tk.MustExec("rename table t to t1")
	wg.Wait()
	require.Error(t, checkErr)
	require.True(t, strings.Contains(checkErr.Error(), "Table 'test.t' doesn't exist"), checkErr.Error())
	tk.MustExec("rename table t1 to t")
	checkErr = nil

	// rename then add column, but rename to other database
	concurrentDDLQuery = "alter table t add column g int"
	firstDDL = true
	tk.MustExec("rename table t to test2.t1")
	wg.Wait()
	require.Error(t, checkErr)
	// [schema:1146]Table '(Schema ID 1).(Table ID 65)' doesn't exist
	require.True(t, strings.Contains(checkErr.Error(), "doesn't exist"), checkErr.Error())
	tk.MustExec("rename table test2.t1 to test.t")
	checkErr = nil

	// rename then add column, but rename to other database and create same name table
	concurrentDDLQuery = "alter table t add column g int"
	firstDDL = true
	tk.MustExec("rename table t to test2.t1")
	concurrentDDLQueryPre = "create table t(a int)"
	wg.Wait()
	require.Error(t, checkErr)
	// [schema:1146]Table '(Schema ID 1).(Table ID 65)' doesn't exist
	require.True(t, strings.Contains(checkErr.Error(), "doesn't exist"), checkErr.Error())
	tk.MustExec("rename table test2.t1 to test.t")
	concurrentDDLQueryPre = ""
	checkErr = nil

	// rename then rename
	concurrentDDLQuery = "rename table t to t2"
	firstDDL = true
	tk.MustExec("rename table t to t1")
	wg.Wait()
	require.Error(t, checkErr)
	require.True(t, strings.Contains(checkErr.Error(), "Table 'test.t' doesn't exist"), checkErr.Error())
	tk.MustExec("rename table t1 to t")
	checkErr = nil

	// rename then rename, but rename to other database
	concurrentDDLQuery = "rename table t to t2"
	firstDDL = true
	tk.MustExec("rename table t to test2.t1")
	wg.Wait()
	require.Error(t, checkErr)
	require.True(t, strings.Contains(checkErr.Error(), "doesn't exist"), checkErr.Error())
	tk.MustExec("rename table test2.t1 to test.t")
	checkErr = nil

	// renames then add index on one table
	tk.MustExec("create table t2(a int)")
	tk.MustExec("create table t3(a int)")
	concurrentDDLQuery = "alter table t add index(a)"
	firstDDL = true
	tk.MustExec("rename table t to tt, t2 to tt2, t3 to tt3")
	wg.Wait()
	require.Error(t, checkErr)
	require.True(t, strings.Contains(checkErr.Error(), "Table 'test.t' doesn't exist"), checkErr.Error())
	tk.MustExec("rename table tt to t")
}
