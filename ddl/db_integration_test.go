// Copyright 2018 PingCAP, Inc.
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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	tmysql "github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/types"
	// "github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testIntegrationSuite{})

type testIntegrationSuite struct {
	lease time.Duration
	store kv.Storage
	dom   *domain.Domain
	ctx   sessionctx.Context
}

func (s *testIntegrationSuite) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	var err error
	testleak.BeforeTest()
	s.lease = 200 * time.Millisecond
	s.store, s.dom, err = newStoreWithBootstrap(s.lease)
	c.Assert(err, IsNil)

	//	tk := testkit.NewTestKit(c, s.store)
	//	tk.MustExec("USE test;")
	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	s.ctx = se.(sessionctx.Context)
	_, err = se.Execute(context.Background(), "create database test_db")
	c.Assert(err, IsNil)
}

func newStoreWithBootstrap(lease time.Duration) (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	session.SetSchemaLease(lease)
	session.SetStatsLease(0)
	dom, err := session.BootstrapSession(store)
	return store, dom, errors.Trace(err)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testIntegrationSuite) TestInvalidDefault(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test;")

	_, err := tk.Exec("create table t(c1 decimal default 1.7976931348623157E308)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidDefault), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("create table t( c1 varchar(2) default 'TiDB');")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidDefault), IsTrue, Commentf("err %v", err))
}

// for issue #3848
func (s *testIntegrationSuite) TestInvalidNameWhenCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test;")

	_, err := tk.Exec("create table t(xxx.t.a bigint)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ddl.ErrWrongDBName), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("create table t(test.tttt.a bigint)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ddl.ErrWrongTableName), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("create table t(t.tttt.a bigint)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, ddl.ErrWrongDBName), IsTrue, Commentf("err %v", err))
}

// for issue #6879
func (s *testIntegrationSuite) TestCreateTableIfNotExists(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test;")

	tk.MustExec("create table t1(a bigint)")
	tk.MustExec("create table t(a bigint)")

	// Test duplicate create-table with `LIKE` clause
	tk.MustExec("create table if not exists t like t1;")
	warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), GreaterEqual, 1)
	lastWarn := warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(infoschema.ErrTableExists, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))
	c.Assert(lastWarn.Level, Equals, stmtctx.WarnLevelNote)

	// Test duplicate create-table without `LIKE` clause
	tk.MustExec("create table if not exists t(b bigint, c varchar(60));")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), GreaterEqual, 1)
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(infoschema.ErrTableExists, lastWarn.Err), IsTrue)
}

func (s *testIntegrationSuite) TestUniquekeyNullValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test")

	tk.MustExec("create table t(a int primary key, b varchar(255))")

	tk.MustExec("insert into t values(1, NULL)")
	tk.MustExec("insert into t values(2, NULL)")
	tk.MustExec("alter table t add unique index b(b);")
	res := tk.MustQuery("select count(*) from t use index(b);")
	res.Check(testkit.Rows("2"))
	tk.MustExec("admin check table t")
	tk.MustExec("admin check index t b")
}

func (s *testIntegrationSuite) TestEndIncluded(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test")
	tk.MustExec("create table t(a int, b int)")
	for i := 0; i < ddl.DefaultTaskHandleCnt+1; i++ {
		tk.MustExec("insert into t values(1, 1)")
	}
	tk.MustExec("alter table t add index b(b);")
	tk.MustExec("admin check index t b")
	tk.MustExec("admin check table t")
}

func (s *testIntegrationSuite) testErrorCode(c *C, tk *testkit.TestKit, sql string, errCode int) {
	_, err := tk.Exec(sql)
	c.Assert(err, NotNil)
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	c.Assert(ok, IsTrue, Commentf("err: %T", originErr))
	c.Assert(tErr.ToSQLError().Code, DeepEquals, uint16(errCode), Commentf("MySQL code:%v", tErr.ToSQLError()))
}

// TestModifyColumnAfterAddIndex Issue 5134
func (s *testIntegrationSuite) TestModifyColumnAfterAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table city (city VARCHAR(2) KEY);")
	tk.MustExec("alter table city change column city city varchar(50);")
	tk.MustExec(`insert into city values ("abc"), ("abd");`)
}

func (s *testIntegrationSuite) TestIssue2293(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t_issue_2293 (a int)")
	sql := "alter table t_issue_2293 add b int not null default 'a'"
	s.testErrorCode(c, tk, sql, tmysql.ErrInvalidDefault)
	tk.MustExec("insert into t_issue_2293 value(1)")
	tk.MustQuery("select * from t_issue_2293").Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite) TestIssue6101(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (quantity decimal(2) unsigned);")
	_, err := tk.Exec("insert into t1 values (500), (-500), (~0), (-1);")
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(tmysql.ErrWarnDataOutOfRange))
	tk.MustExec("drop table t1")

	tk.MustExec("set sql_mode=''")
	tk.MustExec("create table t1 (quantity decimal(2) unsigned);")
	tk.MustExec("insert into t1 values (500), (-500), (~0), (-1);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("99", "0", "99", "0"))
	tk.MustExec("drop table t1")
}

func (s *testIntegrationSuite) TestIssue3833(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table issue3833 (b char(0))")
	s.testErrorCode(c, tk, "create index idx on issue3833 (b)", tmysql.ErrWrongKeyColumn)
	s.testErrorCode(c, tk, "alter table issue3833 add index idx (b)", tmysql.ErrWrongKeyColumn)
	s.testErrorCode(c, tk, "create table issue3833_2 (b char(0), index (b))", tmysql.ErrWrongKeyColumn)
}

func (s *testIntegrationSuite) TestIssue2858And2717(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("create table t_issue_2858_bit (a bit(64) default b'0')")
	tk.MustExec("insert into t_issue_2858_bit value ()")
	tk.MustExec(`insert into t_issue_2858_bit values (100), ('10'), ('\0')`)
	tk.MustQuery("select a+0 from t_issue_2858_bit").Check(testkit.Rows("0", "100", "12592", "0"))
	tk.MustExec(`alter table t_issue_2858_bit alter column a set default '\0'`)

	tk.MustExec("create table t_issue_2858_hex (a int default 0x123)")
	tk.MustExec("insert into t_issue_2858_hex value ()")
	tk.MustExec("insert into t_issue_2858_hex values (123), (0x321)")
	tk.MustQuery("select a from t_issue_2858_hex").Check(testkit.Rows("291", "123", "801"))
	tk.MustExec(`alter table t_issue_2858_hex alter column a set default 0x321`)
}

func (s *testIntegrationSuite) TestIssue4432(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("create table tx (col bit(10) default 'a')")
	tk.MustExec("insert into tx value ()")
	tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	tk.MustExec("drop table tx")

	tk.MustExec("create table tx (col bit(10) default 0x61)")
	tk.MustExec("insert into tx value ()")
	tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	tk.MustExec("drop table tx")

	tk.MustExec("create table tx (col bit(10) default 97)")
	tk.MustExec("insert into tx value ()")
	tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	tk.MustExec("drop table tx")

	tk.MustExec("create table tx (col bit(10) default 0b1100001)")
	tk.MustExec("insert into tx value ()")
	tk.MustQuery("select * from tx").Check(testkit.Rows("\x00a"))
	tk.MustExec("drop table tx")
}
