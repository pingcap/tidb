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
	"fmt"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
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
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testIntegrationSuite{})

type testIntegrationSuite struct {
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
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)
	s.ctx = mock.NewContext()
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testIntegrationSuite) TestNoZeroDateMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	defer tk.MustExec("set session sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';")

	tk.MustExec("use test;")
	tk.MustExec("set session sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ENGINE_SUBSTITUTION';")

	_, err := tk.Exec("create table test_zero_date(agent_start_time date NOT NULL DEFAULT '0000-00-00')")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidDefault), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("create table test_zero_date(agent_start_time datetime NOT NULL DEFAULT '0000-00-00 00:00:00')")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidDefault), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("create table test_zero_date(agent_start_time timestamp NOT NULL DEFAULT '0000-00-00 00:00:00')")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidDefault), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("create table test_zero_date(a timestamp default '0000-00-00 00')")
	c.Assert(err, NotNil)

	_, err = tk.Exec("create table test_zero_date(a timestamp default 0)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidDefault), IsTrue, Commentf("err %v", err))
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

func (s *testIntegrationSuite) TestNullGeneratedColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (" +
		"`a` int(11) DEFAULT NULL," +
		"`b` int(11) DEFAULT NULL," +
		"`c` int(11) GENERATED ALWAYS AS (`a` + `b`) VIRTUAL DEFAULT NULL," +
		"`h` varchar(10) DEFAULT NULL," +
		"`m` int(11) DEFAULT NULL" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

	tk.MustExec("insert into t values()")
	tk.MustExec("alter table t add index idx_c(c)")
	tk.MustExec("drop table t")
}

func (s *testIntegrationSuite) TestChangingCharsetToUtf8(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a varchar(20) charset utf8)")
	tk.MustExec("insert into t1 values (?)", "t1_value")

	tk.MustExec("alter table t1 modify column a varchar(20) charset utf8mb4")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("t1_value"))

	tk.MustExec("create table t(a varchar(20) charset latin1)")
	tk.MustExec("insert into t values (?)", "t_value")

	tk.MustExec("alter table t modify column a varchar(20) charset latin1")
	tk.MustQuery("select * from t;").Check(testkit.Rows("t_value"))

	rs, err := tk.Exec("alter table t modify column a varchar(20) charset utf8")
	if rs != nil {
		rs.Close()
	}
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:210]unsupported modify charset from latin1 to utf8")
	rs, err = tk.Exec("alter table t modify column a varchar(20) charset utf8mb4")
	if rs != nil {
		rs.Close()
	}
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:210]unsupported modify charset from latin1 to utf8mb4")

	rs, err = tk.Exec("alter table t modify column a varchar(20) charset utf8mb4 collate utf8bin")
	if rs != nil {
		rs.Close()
	}
	c.Assert(err, NotNil)
	rs, err = tk.Exec("alter table t modify column a varchar(20) charset utf8 collate utf8_bin")
	if rs != nil {
		rs.Close()
	}
	c.Assert(err, NotNil)
	rs, err = tk.Exec("alter table t modify column a varchar(20) charset utf8mb4 collate utf8mb4_general_ci")
	if rs != nil {
		rs.Close()
	}
	c.Assert(err, NotNil)
}

func (s *testIntegrationSuite) TestChangingTableCharset(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test")
	tk.MustExec("create table t(a char(10)) charset latin1 collate latin1_bin")
	rs, err := tk.Exec("alter table t charset gbk")
	if rs != nil {
		rs.Close()
	}
	c.Assert(err.Error(), Equals, "Unknown charset gbk")
	rs, err = tk.Exec("alter table t charset utf8 collate latin1_bin")
	if rs != nil {
		rs.Close()
	}
	c.Assert(err, NotNil)
}

func (s *testIntegrationSuite) TestCaseInsensitiveCharsetAndCollate(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("create database if not exists test_charset_collate")
	defer tk.MustExec("drop database test_charset_collate")
	tk.MustExec("use test_charset_collate")
	tk.MustExec("create table t(id int) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COLLATE=UTF8_BIN;")
	tk.MustExec("create table t1(id int) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COLLATE=uTF8_BIN;")
	tk.MustExec("create table t2(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8 COLLATE=utf8_BIN;")
	tk.MustExec("create table t3(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8mb4 COLLATE=utf8MB4_BIN;")
	tk.MustExec("create table t4(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8mb4 COLLATE=utf8MB4_general_ci;")
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	session.SetSchemaLease(0)
	session.SetStatsLease(0)
	dom, err := session.BootstrapSession(store)
	return store, dom, errors.Trace(err)
}

func (s *testIntegrationSuite) TestResolveCharset(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists resolve_charset")
	tk.MustExec(`CREATE TABLE resolve_charset (a varchar(255) DEFAULT NULL) DEFAULT CHARSET=latin1`)
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("resolve_charset"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Cols()[0].Charset, Equals, "latin1")
	tk.MustExec("INSERT INTO resolve_charset VALUES('鰈')")

	tk.MustExec("create database resolve_charset charset binary")
	tk.MustExec("use resolve_charset")
	tk.MustExec(`CREATE TABLE resolve_charset (a varchar(255) DEFAULT NULL) DEFAULT CHARSET=latin1`)

	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("resolve_charset"), model.NewCIStr("resolve_charset"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Cols()[0].Charset, Equals, "latin1")
	c.Assert(tbl.Meta().Charset, Equals, "latin1")

	tk.MustExec(`CREATE TABLE resolve_charset1 (a varchar(255) DEFAULT NULL)`)
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("resolve_charset"), model.NewCIStr("resolve_charset1"))
	c.Assert(err, IsNil)
	c.Assert(tbl.Cols()[0].Charset, Equals, "binary")
	c.Assert(tbl.Meta().Charset, Equals, "binary")
}
