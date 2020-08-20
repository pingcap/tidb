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
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/cluster"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/israce"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testIntegrationSuite1{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite2{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite3{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite4{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite5{&testIntegrationSuite{}})
var _ = Suite(&testIntegrationSuite6{&testIntegrationSuite{}})
var _ = SerialSuites(&testIntegrationSuite7{&testIntegrationSuite{}})

type testIntegrationSuite struct {
	lease   time.Duration
	cluster cluster.Cluster
	store   kv.Storage
	dom     *domain.Domain
	ctx     sessionctx.Context
}

func setupIntegrationSuite(s *testIntegrationSuite, c *C) {
	var err error
	s.lease = 50 * time.Millisecond
	ddl.SetWaitTimeWhenErrorOccurred(0)

	s.store, err = mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c cluster.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
			s.cluster = c
		}),
	)
	c.Assert(err, IsNil)
	session.SetSchemaLease(s.lease)
	session.DisableStats4Test()
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)

	se, err := session.CreateSession4Test(s.store)
	c.Assert(err, IsNil)
	s.ctx = se.(sessionctx.Context)
	_, err = se.Execute(context.Background(), "create database test_db")
	c.Assert(err, IsNil)
}

func tearDownIntegrationSuiteTest(s *testIntegrationSuite, c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func tearDownIntegrationSuite(s *testIntegrationSuite, c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	setupIntegrationSuite(s, c)
}

func (s *testIntegrationSuite) TearDownSuite(c *C) {
	tearDownIntegrationSuite(s, c)
}

type testIntegrationSuite1 struct{ *testIntegrationSuite }
type testIntegrationSuite2 struct{ *testIntegrationSuite }

func (s *testIntegrationSuite2) TearDownTest(c *C) {
	tearDownIntegrationSuiteTest(s.testIntegrationSuite, c)
}

type testIntegrationSuite3 struct{ *testIntegrationSuite }
type testIntegrationSuite4 struct{ *testIntegrationSuite }
type testIntegrationSuite5 struct{ *testIntegrationSuite }
type testIntegrationSuite6 struct{ *testIntegrationSuite }
type testIntegrationSuite7 struct{ *testIntegrationSuite }

func (s *testIntegrationSuite5) TestNoZeroDateMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	defer tk.MustExec("set session sql_mode='ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION';")

	tk.MustExec("use test;")
	tk.MustExec("set session sql_mode='STRICT_TRANS_TABLES,NO_ZERO_DATE,NO_ENGINE_SUBSTITUTION';")
	tk.MustGetErrCode("create table test_zero_date(agent_start_time date NOT NULL DEFAULT '0000-00-00')", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create table test_zero_date(agent_start_time datetime NOT NULL DEFAULT '0000-00-00 00:00:00')", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create table test_zero_date(agent_start_time timestamp NOT NULL DEFAULT '0000-00-00 00:00:00')", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create table test_zero_date(a timestamp default '0000-00-00 00');", errno.ErrInvalidDefault)
	tk.MustGetErrCode("create table test_zero_date(a timestamp default 0);", errno.ErrInvalidDefault)
}

func (s *testIntegrationSuite2) TestInvalidDefault(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test;")

	_, err := tk.Exec("create table t(c1 decimal default 1.7976931348623157E308)")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidDefault), IsTrue, Commentf("err %v", err))

	_, err = tk.Exec("create table t( c1 varchar(2) default 'TiDB');")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidDefault), IsTrue, Commentf("err %v", err))
}

// TestKeyWithoutLength for issue #13452
func (s testIntegrationSuite3) TestKeyWithoutLengthCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test")

	_, err := tk.Exec("create table t_without_length (a text primary key)")
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*BLOB/TEXT column 'a' used in key specification without a key length")
}

// TestInvalidNameWhenCreateTable for issue #3848
func (s *testIntegrationSuite3) TestInvalidNameWhenCreateTable(c *C) {
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

// TestCreateTableIfNotExists for issue #6879
func (s *testIntegrationSuite3) TestCreateTableIfNotExists(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test;")

	tk.MustExec("create table ct1(a bigint)")
	tk.MustExec("create table ct(a bigint)")

	// Test duplicate create-table with `LIKE` clause
	tk.MustExec("create table if not exists ct like ct1;")
	warnings := tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), GreaterEqual, 1)
	lastWarn := warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(infoschema.ErrTableExists, lastWarn.Err), IsTrue, Commentf("err %v", lastWarn.Err))
	c.Assert(lastWarn.Level, Equals, stmtctx.WarnLevelNote)

	// Test duplicate create-table without `LIKE` clause
	tk.MustExec("create table if not exists ct(b bigint, c varchar(60));")
	warnings = tk.Se.GetSessionVars().StmtCtx.GetWarnings()
	c.Assert(len(warnings), GreaterEqual, 1)
	lastWarn = warnings[len(warnings)-1]
	c.Assert(terror.ErrorEqual(infoschema.ErrTableExists, lastWarn.Err), IsTrue)
}

// for issue #9910
func (s *testIntegrationSuite2) TestCreateTableWithKeyWord(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test;")

	_, err := tk.Exec("create table t1(pump varchar(20), drainer varchar(20), node_id varchar(20), node_state varchar(20));")
	c.Assert(err, IsNil)
}

func (s *testIntegrationSuite1) TestUniqueKeyNullValue(c *C) {
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

func (s *testIntegrationSuite2) TestUniqueKeyNullValueClusterIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("drop database if exists unique_null_val;")
	tk.MustExec("create database unique_null_val;")
	tk.MustExec("use unique_null_val;")
	tk.MustExec("create table t (a varchar(10), b float, c varchar(255), primary key (a, b));")
	tk.MustExec("insert into t values ('1', 1, NULL);")
	tk.MustExec("insert into t values ('2', 2, NULL);")
	tk.MustExec("alter table t add unique index c(c);")
	tk.MustQuery("select count(*) from t use index(c);").Check(testkit.Rows("2"))
	tk.MustExec("admin check table t;")
	tk.MustExec("admin check index t c;")
}

// TestModifyColumnAfterAddIndex Issue 5134
func (s *testIntegrationSuite3) TestModifyColumnAfterAddIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table city (city VARCHAR(2) KEY);")
	tk.MustExec("alter table city change column city city varchar(50);")
	tk.MustExec(`insert into city values ("abc"), ("abd");`)
}

func (s *testIntegrationSuite3) TestIssue2293(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t_issue_2293 (a int)")
	tk.MustGetErrCode("alter table t_issue_2293 add b int not null default 'a'", errno.ErrInvalidDefault)
	tk.MustExec("insert into t_issue_2293 value(1)")
	tk.MustQuery("select * from t_issue_2293").Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite2) TestIssue6101(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (quantity decimal(2) unsigned);")
	_, err := tk.Exec("insert into t1 values (500), (-500), (~0), (-1);")
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(errno.ErrWarnDataOutOfRange))
	tk.MustExec("drop table t1")

	tk.MustExec("set sql_mode=''")
	tk.MustExec("create table t1 (quantity decimal(2) unsigned);")
	tk.MustExec("insert into t1 values (500), (-500), (~0), (-1);")
	tk.MustQuery("select * from t1").Check(testkit.Rows("99", "0", "99", "0"))
	tk.MustExec("drop table t1")
}

func (s *testIntegrationSuite1) TestIndexLength(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table idx_len(a int(0), b timestamp(0), c datetime(0), d time(0), f float(0), g decimal(0))")
	tk.MustExec("create index idx on idx_len(a)")
	tk.MustExec("alter table idx_len add index idxa(a)")
	tk.MustExec("create index idx1 on idx_len(b)")
	tk.MustExec("alter table idx_len add index idxb(b)")
	tk.MustExec("create index idx2 on idx_len(c)")
	tk.MustExec("alter table idx_len add index idxc(c)")
	tk.MustExec("create index idx3 on idx_len(d)")
	tk.MustExec("alter table idx_len add index idxd(d)")
	tk.MustExec("create index idx4 on idx_len(f)")
	tk.MustExec("alter table idx_len add index idxf(f)")
	tk.MustExec("create index idx5 on idx_len(g)")
	tk.MustExec("alter table idx_len add index idxg(g)")
	tk.MustExec("create table idx_len1(a int(0), b timestamp(0), c datetime(0), d time(0), f float(0), g decimal(0), index(a), index(b), index(c), index(d), index(f), index(g))")
}

func (s *testIntegrationSuite3) TestIssue3833(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table issue3833 (b char(0), c binary(0), d  varchar(0))")
	tk.MustGetErrCode("create index idx on issue3833 (b)", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("alter table issue3833 add index idx (b)", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("create table issue3833_2 (b char(0), c binary(0), d varchar(0), index(b))", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("create index idx on issue3833 (c)", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("alter table issue3833 add index idx (c)", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("create table issue3833_2 (b char(0), c binary(0), d varchar(0), index(c))", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("create index idx on issue3833 (d)", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("alter table issue3833 add index idx (d)", errno.ErrWrongKeyColumn)
	tk.MustGetErrCode("create table issue3833_2 (b char(0), c binary(0), d varchar(0), index(d))", errno.ErrWrongKeyColumn)
}

func (s *testIntegrationSuite1) TestIssue2858And2717(c *C) {
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

func (s *testIntegrationSuite1) TestIssue4432(c *C) {
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

func (s *testIntegrationSuite1) TestIssue5092(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("create table t_issue_5092 (a int)")
	tk.MustExec("alter table t_issue_5092 add column (b int, c int)")
	tk.MustExec("alter table t_issue_5092 add column if not exists (b int, c int)")
	tk.MustExec("alter table t_issue_5092 add column b1 int after b, add column c1 int after c")
	tk.MustExec("alter table t_issue_5092 add column d int after b, add column e int first, add column f int after c1, add column g int, add column h int first")
	tk.MustQuery("show create table t_issue_5092").Check(testkit.Rows("t_issue_5092 CREATE TABLE `t_issue_5092` (\n" +
		"  `h` int(11) DEFAULT NULL,\n" +
		"  `e` int(11) DEFAULT NULL,\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `d` int(11) DEFAULT NULL,\n" +
		"  `b1` int(11) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  `c1` int(11) DEFAULT NULL,\n" +
		"  `f` int(11) DEFAULT NULL,\n" +
		"  `g` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// The following two statements are consistent with MariaDB.
	tk.MustGetErrCode("alter table t_issue_5092 add column if not exists d int, add column d int", errno.ErrDupFieldName)
	tk.MustExec("alter table t_issue_5092 add column dd int, add column if not exists dd int")
	tk.MustExec("alter table t_issue_5092 add column if not exists (d int, e int), add column ff text")
	tk.MustExec("alter table t_issue_5092 add column b2 int after b1, add column c2 int first")
	tk.MustQuery("show create table t_issue_5092").Check(testkit.Rows("t_issue_5092 CREATE TABLE `t_issue_5092` (\n" +
		"  `c2` int(11) DEFAULT NULL,\n" +
		"  `h` int(11) DEFAULT NULL,\n" +
		"  `e` int(11) DEFAULT NULL,\n" +
		"  `a` int(11) DEFAULT NULL,\n" +
		"  `b` int(11) DEFAULT NULL,\n" +
		"  `d` int(11) DEFAULT NULL,\n" +
		"  `b1` int(11) DEFAULT NULL,\n" +
		"  `b2` int(11) DEFAULT NULL,\n" +
		"  `c` int(11) DEFAULT NULL,\n" +
		"  `c1` int(11) DEFAULT NULL,\n" +
		"  `f` int(11) DEFAULT NULL,\n" +
		"  `g` int(11) DEFAULT NULL,\n" +
		"  `dd` int(11) DEFAULT NULL,\n" +
		"  `ff` text DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t_issue_5092")

	tk.MustExec("create table t_issue_5092 (a int default 1)")
	tk.MustExec("alter table t_issue_5092 add column (b int default 2, c int default 3)")
	tk.MustExec("alter table t_issue_5092 add column b1 int default 22 after b, add column c1 int default 33 after c")
	tk.MustExec("insert into t_issue_5092 value ()")
	tk.MustQuery("select * from t_issue_5092").Check(testkit.Rows("1 2 22 3 33"))
	tk.MustExec("alter table t_issue_5092 add column d int default 4 after c1, add column aa int default 0 first")
	tk.MustQuery("select * from t_issue_5092").Check(testkit.Rows("0 1 2 22 3 33 4"))
	tk.MustQuery("show create table t_issue_5092").Check(testkit.Rows("t_issue_5092 CREATE TABLE `t_issue_5092` (\n" +
		"  `aa` int(11) DEFAULT 0,\n" +
		"  `a` int(11) DEFAULT 1,\n" +
		"  `b` int(11) DEFAULT 2,\n" +
		"  `b1` int(11) DEFAULT 22,\n" +
		"  `c` int(11) DEFAULT 3,\n" +
		"  `c1` int(11) DEFAULT 33,\n" +
		"  `d` int(11) DEFAULT 4\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	tk.MustExec("drop table t_issue_5092")

	tk.MustExec("create table t_issue_5092 (a int)")
	tk.MustExec("alter table t_issue_5092 add column (b int, c int)")
	tk.MustExec("alter table t_issue_5092 drop column b,drop column c")
	tk.MustGetErrCode("alter table t_issue_5092 drop column c, drop column c", errno.ErrCantDropFieldOrKey)
	tk.MustExec("alter table t_issue_5092 drop column if exists b,drop column if exists c")
	tk.MustGetErrCode("alter table t_issue_5092 drop column g, drop column d", errno.ErrCantDropFieldOrKey)
	tk.MustExec("drop table t_issue_5092")

	tk.MustExec("create table t_issue_5092 (a int)")
	tk.MustExec("alter table t_issue_5092 add column (b int, c int)")
	tk.MustGetErrCode("alter table t_issue_5092 drop column if exists a, drop column b, drop column c", errno.ErrCantRemoveAllFields)
	tk.MustGetErrCode("alter table t_issue_5092 drop column if exists c, drop column c", errno.ErrCantDropFieldOrKey)
	tk.MustExec("alter table t_issue_5092 drop column c, drop column if exists c")
	tk.MustExec("drop table t_issue_5092")
}

func (s *testIntegrationSuite5) TestErrnoErrorCode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")

	// create database
	sql := "create database aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	tk.MustGetErrCode(sql, errno.ErrTooLongIdent)
	sql = "create database test"
	tk.MustGetErrCode(sql, errno.ErrDBCreateExists)
	sql = "create database test1 character set uft8;"
	tk.MustGetErrCode(sql, errno.ErrUnknownCharacterSet)
	sql = "create database test2 character set gkb;"
	tk.MustGetErrCode(sql, errno.ErrUnknownCharacterSet)
	sql = "create database test3 character set laitn1;"
	tk.MustGetErrCode(sql, errno.ErrUnknownCharacterSet)
	// drop database
	sql = "drop database db_not_exist"
	tk.MustGetErrCode(sql, errno.ErrDBDropExists)
	// create table
	tk.MustExec("create table test_error_code_succ (c1 int, c2 int, c3 int, primary key(c3))")
	sql = "create table test_error_code_succ (c1 int, c2 int, c3 int)"
	tk.MustGetErrCode(sql, errno.ErrTableExists)
	sql = "create table test_error_code1 (c1 int, c2 int, c2 int)"
	tk.MustGetErrCode(sql, errno.ErrDupFieldName)
	sql = "create table test_error_code1 (c1 int, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int)"
	tk.MustGetErrCode(sql, errno.ErrTooLongIdent)
	sql = "create table aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa(a int)"
	tk.MustGetErrCode(sql, errno.ErrTooLongIdent)
	sql = "create table test_error_code1 (c1 int, c2 int, key aa (c1, c2), key aa (c1))"
	tk.MustGetErrCode(sql, errno.ErrDupKeyName)
	sql = "create table test_error_code1 (c1 int, c2 int, c3 int, key(c_not_exist))"
	tk.MustGetErrCode(sql, errno.ErrKeyColumnDoesNotExits)
	sql = "create table test_error_code1 (c1 int, c2 int, c3 int, primary key(c_not_exist))"
	tk.MustGetErrCode(sql, errno.ErrKeyColumnDoesNotExits)
	sql = "create table test_error_code1 (c1 int not null default '')"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)
	sql = "CREATE TABLE `t` (`a` double DEFAULT 1.0 DEFAULT 2.0 DEFAULT now());"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)
	sql = "CREATE TABLE `t` (`a` double DEFAULT now());"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)
	sql = "create table t1(a int) character set uft8;"
	tk.MustGetErrCode(sql, errno.ErrUnknownCharacterSet)
	sql = "create table t1(a int) character set gkb;"
	tk.MustGetErrCode(sql, errno.ErrUnknownCharacterSet)
	sql = "create table t1(a int) character set laitn1;"
	tk.MustGetErrCode(sql, errno.ErrUnknownCharacterSet)
	sql = "create table test_error_code (a int not null ,b int not null,c int not null, d int not null, foreign key (b, c) references product(id));"
	tk.MustGetErrCode(sql, errno.ErrWrongFkDef)
	sql = "create table test_error_code_2;"
	tk.MustGetErrCode(sql, errno.ErrTableMustHaveColumns)
	sql = "create table test_error_code_2 (unique(c1));"
	tk.MustGetErrCode(sql, errno.ErrTableMustHaveColumns)
	sql = "create table test_error_code_2(c1 int, c2 int, c3 int, primary key(c1), primary key(c2));"
	tk.MustGetErrCode(sql, errno.ErrMultiplePriKey)
	sql = "create table test_error_code_3(pt blob ,primary key (pt));"
	tk.MustGetErrCode(sql, errno.ErrBlobKeyWithoutLength)
	sql = "create table test_error_code_3(a text, unique (a(3073)));"
	tk.MustGetErrCode(sql, errno.ErrTooLongKey)
	sql = "create table test_error_code_3(`id` int, key `primary`(`id`));"
	tk.MustGetErrCode(sql, errno.ErrWrongNameForIndex)
	sql = "create table t2(c1.c2 blob default null);"
	tk.MustGetErrCode(sql, errno.ErrWrongTableName)
	sql = "create table t2 (id int default null primary key , age int);"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)
	sql = "create table t2 (id int null primary key , age int);"
	tk.MustGetErrCode(sql, errno.ErrPrimaryCantHaveNull)
	sql = "create table t2 (id int default null, age int, primary key(id));"
	tk.MustGetErrCode(sql, errno.ErrPrimaryCantHaveNull)
	sql = "create table t2 (id int null, age int, primary key(id));"
	tk.MustGetErrCode(sql, errno.ErrPrimaryCantHaveNull)
	sql = "create table t2 (id int auto_increment);"
	tk.MustGetErrCode(sql, errno.ErrWrongAutoKey)
	sql = "create table t2 (id int auto_increment, a int key);"
	tk.MustGetErrCode(sql, errno.ErrWrongAutoKey)
	sql = "create table t2 (a datetime(2) default current_timestamp(3));"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)
	sql = "create table t2 (a datetime(2) default current_timestamp(2) on update current_timestamp);"
	tk.MustGetErrCode(sql, errno.ErrInvalidOnUpdate)
	sql = "create table t2 (a datetime default current_timestamp on update current_timestamp(2));"
	tk.MustGetErrCode(sql, errno.ErrInvalidOnUpdate)
	sql = "create table t2 (a datetime(2) default current_timestamp(2) on update current_timestamp(3));"
	tk.MustGetErrCode(sql, errno.ErrInvalidOnUpdate)
	sql = "create table t(a blob(10), index(a(0)));"
	tk.MustGetErrCode(sql, errno.ErrKeyPart0)
	sql = "create table t(a char(10), index(a(0)));"
	tk.MustGetErrCode(sql, errno.ErrKeyPart0)

	sql = "create table t2 (id int primary key , age int);"
	tk.MustExec(sql)

	// add column
	sql = "alter table test_error_code_succ add column c1 int"
	tk.MustGetErrCode(sql, errno.ErrDupFieldName)
	sql = "alter table test_error_code_succ add column aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int"
	tk.MustGetErrCode(sql, errno.ErrTooLongIdent)
	sql = "alter table test_comment comment 'test comment'"
	tk.MustGetErrCode(sql, errno.ErrNoSuchTable)
	sql = "alter table test_error_code_succ add column `a ` int ;"
	tk.MustGetErrCode(sql, errno.ErrWrongColumnName)
	tk.MustExec("create table test_on_update (c1 int, c2 int);")
	sql = "alter table test_on_update add column c3 int on update current_timestamp;"
	tk.MustGetErrCode(sql, errno.ErrInvalidOnUpdate)
	sql = "create table test_on_update_2(c int on update current_timestamp);"
	tk.MustGetErrCode(sql, errno.ErrInvalidOnUpdate)

	// add columns
	sql = "alter table test_error_code_succ add column c1 int, add column c1 int"
	tk.MustGetErrCode(sql, errno.ErrDupFieldName)
	sql = "alter table test_error_code_succ add column (aa int, aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa int)"
	tk.MustGetErrCode(sql, errno.ErrTooLongIdent)
	sql = "alter table test_error_code_succ add column `a ` int, add column `b ` int;"
	tk.MustGetErrCode(sql, errno.ErrWrongColumnName)
	tk.MustExec("create table test_add_columns_on_update (c1 int, c2 int);")
	sql = "alter table test_add_columns_on_update add column cc int, add column c3 int on update current_timestamp;"
	tk.MustGetErrCode(sql, errno.ErrInvalidOnUpdate)

	// drop column
	sql = "alter table test_error_code_succ drop c_not_exist"
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	tk.MustExec("create table test_drop_column (c1 int );")
	sql = "alter table test_drop_column drop column c1;"
	tk.MustGetErrCode(sql, errno.ErrCantRemoveAllFields)
	// drop columns
	sql = "alter table test_error_code_succ drop c_not_exist, drop cc_not_exist"
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	tk.MustExec("create table test_drop_columns (c1 int);")
	tk.MustExec("alter table test_drop_columns add column c2 int first, add column c3 int after c1")
	sql = "alter table test_drop_columns drop column c1, drop column c2, drop column c3;"
	tk.MustGetErrCode(sql, errno.ErrCantRemoveAllFields)
	sql = "alter table test_drop_columns drop column c1, add column c2 int;"
	tk.MustGetErrCode(sql, errno.ErrUnsupportedDDLOperation)
	sql = "alter table test_drop_columns drop column c1, drop column c1;"
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	// add index
	sql = "alter table test_error_code_succ add index idx (c_not_exist)"
	tk.MustGetErrCode(sql, errno.ErrKeyColumnDoesNotExits)
	tk.MustExec("alter table test_error_code_succ add index idx (c1)")
	sql = "alter table test_error_code_succ add index idx (c1)"
	tk.MustGetErrCode(sql, errno.ErrDupKeyName)
	// drop index
	sql = "alter table test_error_code_succ drop index idx_not_exist"
	tk.MustGetErrCode(sql, errno.ErrCantDropFieldOrKey)
	sql = "alter table test_error_code_succ drop column c3"
	tk.MustGetErrCode(sql, errno.ErrUnsupportedDDLOperation)
	// modify column
	sql = "alter table test_error_code_succ modify testx.test_error_code_succ.c1 bigint"
	tk.MustGetErrCode(sql, errno.ErrWrongDBName)
	sql = "alter table test_error_code_succ modify t.c1 bigint"
	tk.MustGetErrCode(sql, errno.ErrWrongTableName)
	// insert value
	tk.MustExec("create table test_error_code_null(c1 char(100) not null);")
	sql = "insert into test_error_code_null (c1) values(null);"
	tk.MustGetErrCode(sql, errno.ErrBadNull)
}

func (s *testIntegrationSuite3) TestTableDDLWithFloatType(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustGetErrCode("create table t (a decimal(1, 2))", errno.ErrMBiggerThanD)
	tk.MustGetErrCode("create table t (a float(1, 2))", errno.ErrMBiggerThanD)
	tk.MustGetErrCode("create table t (a double(1, 2))", errno.ErrMBiggerThanD)
	tk.MustExec("create table t (a double(1, 1))")
	tk.MustGetErrCode("alter table t add column b decimal(1, 2)", errno.ErrMBiggerThanD)
	// add multi columns now not support, so no case.
	tk.MustGetErrCode("alter table t modify column a float(1, 4)", errno.ErrMBiggerThanD)
	tk.MustGetErrCode("alter table t change column a aa float(1, 4)", errno.ErrMBiggerThanD)
	tk.MustExec("drop table t")
}

func (s *testIntegrationSuite1) TestTableDDLWithTimeType(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustGetErrCode("create table t (a time(7))", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("create table t (a datetime(7))", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("create table t (a timestamp(7))", errno.ErrTooBigPrecision)
	_, err := tk.Exec("create table t (a time(-1))")
	c.Assert(err, NotNil)
	tk.MustExec("create table t (a datetime)")
	tk.MustGetErrCode("alter table t add column b time(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t add column b datetime(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t add column b timestamp(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t modify column a time(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t modify column a datetime(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t modify column a timestamp(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t change column a aa time(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t change column a aa datetime(7)", errno.ErrTooBigPrecision)
	tk.MustGetErrCode("alter table t change column a aa timestamp(7)", errno.ErrTooBigPrecision)
	tk.MustExec("alter table t change column a aa datetime(0)")
	tk.MustExec("drop table t")
}

func (s *testIntegrationSuite2) TestUpdateMultipleTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database umt_db")
	tk.MustExec("use umt_db")
	tk.MustExec("create table t1 (c1 int, c2 int)")
	tk.MustExec("insert t1 values (1, 1), (2, 2)")
	tk.MustExec("create table t2 (c1 int, c2 int)")
	tk.MustExec("insert t2 values (1, 3), (2, 5)")
	ctx := tk.Se.(sessionctx.Context)
	dom := domain.GetDomain(ctx)
	is := dom.InfoSchema()
	db, ok := is.SchemaByName(model.NewCIStr("umt_db"))
	c.Assert(ok, IsTrue)
	t1Tbl, err := is.TableByName(model.NewCIStr("umt_db"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	t1Info := t1Tbl.Meta()

	// Add a new column in write only state.
	newColumn := &model.ColumnInfo{
		ID:                 100,
		Name:               model.NewCIStr("c3"),
		Offset:             2,
		DefaultValue:       9,
		OriginDefaultValue: 9,
		FieldType:          *types.NewFieldType(mysql.TypeLonglong),
		State:              model.StateWriteOnly,
	}
	t1Info.Columns = append(t1Info.Columns, newColumn)

	kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UpdateTable(db.ID, t1Info), IsNil)
		return nil
	})
	err = dom.Reload()
	c.Assert(err, IsNil)

	tk.MustExec("update t1, t2 set t1.c1 = 8, t2.c2 = 10 where t1.c2 = t2.c1")
	tk.MustQuery("select * from t1").Check(testkit.Rows("8 1", "8 2"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 10", "2 10"))

	newColumn.State = model.StatePublic

	kv.RunInNewTxn(s.store, false, func(txn kv.Transaction) error {
		m := meta.NewMeta(txn)
		_, err = m.GenSchemaVersion()
		c.Assert(err, IsNil)
		c.Assert(m.UpdateTable(db.ID, t1Info), IsNil)
		return nil
	})
	err = dom.Reload()
	c.Assert(err, IsNil)

	tk.MustQuery("select * from t1").Check(testkit.Rows("8 1 9", "8 2 9"))
	tk.MustExec("drop database umt_db")
}

func (s *testIntegrationSuite2) TestNullGeneratedColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (" +
		"`a` int(11) DEFAULT NULL," +
		"`b` int(11) DEFAULT NULL," +
		"`c` int(11) GENERATED ALWAYS AS (`a` + `b`) VIRTUAL," +
		"`h` varchar(10) DEFAULT NULL," +
		"`m` int(11) DEFAULT NULL" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")

	tk.MustExec("insert into t values()")
	tk.MustExec("alter table t add index idx_c(c)")
	tk.MustExec("drop table t")
}

func (s *testIntegrationSuite2) TestDependedGeneratedColumnPrior2GeneratedColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (" +
		"`a` int(11) DEFAULT NULL," +
		"`b` int(11) GENERATED ALWAYS AS (`a` + 1) VIRTUAL," +
		"`c` int(11) GENERATED ALWAYS AS (`b` + 1) VIRTUAL" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	// should check unknown column first, then the prior ones.
	sql := "alter table t add column d int as (c + f + 1) first"
	tk.MustGetErrCode(sql, errno.ErrBadField)

	// depended generated column should be prior to generated column self
	sql = "alter table t add column d int as (c+1) first"
	tk.MustGetErrCode(sql, errno.ErrGeneratedColumnNonPrior)

	// correct case
	tk.MustExec("alter table t add column d int as (c+1) after c")

	// check position nil case
	tk.MustExec("alter table t add column(e int as (c+1))")
}

func (s *testIntegrationSuite3) TestChangingCharsetToUtf8(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("create table t1(a varchar(20) charset utf8)")
	tk.MustExec("insert into t1 values (?)", "t1_value")
	tk.MustExec("alter table t1 collate uTf8mB4_uNiCoDe_Ci charset Utf8mB4 charset uTF8Mb4 collate UTF8MB4_BiN")
	tk.MustExec("alter table t1 modify column a varchar(20) charset utf8mb4")
	tk.MustQuery("select * from t1;").Check(testkit.Rows("t1_value"))

	tk.MustExec("create table t(a varchar(20) charset latin1)")
	tk.MustExec("insert into t values (?)", "t_value")

	tk.MustExec("alter table t modify column a varchar(20) charset latin1")
	tk.MustQuery("select * from t;").Check(testkit.Rows("t_value"))

	tk.MustGetErrCode("alter table t modify column a varchar(20) charset utf8", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify column a varchar(20) charset utf8mb4", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify column a varchar(20) charset utf8 collate utf8_bin", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t modify column a varchar(20) charset utf8mb4 collate utf8mb4_general_ci", errno.ErrUnsupportedDDLOperation)

	tk.MustGetErrCode("alter table t modify column a varchar(20) charset utf8mb4 collate utf8bin", errno.ErrUnknownCollation)
	tk.MustGetErrCode("alter table t collate LATIN1_GENERAL_CI charset utf8 collate utf8_bin", errno.ErrConflictingDeclarations)
	tk.MustGetErrCode("alter table t collate LATIN1_GENERAL_CI collate UTF8MB4_UNICODE_ci collate utf8_bin", errno.ErrCollationCharsetMismatch)
}

func (s *testIntegrationSuite4) TestChangingTableCharset(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test")
	tk.MustExec("create table t(a char(10)) charset latin1 collate latin1_bin")

	tk.MustGetErrCode("alter table t charset gbk", errno.ErrUnknownCharacterSet)
	tk.MustGetErrCode("alter table t charset ''", errno.ErrUnknownCharacterSet)

	tk.MustGetErrCode("alter table t charset utf8mb4 collate '' collate utf8mb4_bin;", errno.ErrUnknownCollation)

	tk.MustGetErrCode("alter table t charset utf8 collate latin1_bin", errno.ErrCollationCharsetMismatch)
	tk.MustGetErrCode("alter table t charset utf8 collate utf8mb4_bin;", errno.ErrCollationCharsetMismatch)
	tk.MustGetErrCode("alter table t charset utf8 collate utf8_bin collate utf8mb4_bin collate utf8_bin;", errno.ErrCollationCharsetMismatch)

	tk.MustGetErrCode("alter table t charset utf8", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t charset utf8mb4", errno.ErrUnsupportedDDLOperation)
	tk.MustGetErrCode("alter table t charset utf8mb4 collate utf8mb4_bin", errno.ErrUnsupportedDDLOperation)

	tk.MustGetErrCode("alter table t charset latin1 charset utf8 charset utf8mb4 collate utf8_bin;", errno.ErrConflictingDeclarations)

	// Test change column charset when changing table charset.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a varchar(10)) charset utf8")
	tk.MustExec("alter table t convert to charset utf8mb4;")
	checkCharset := func(chs, coll string) {
		tbl := testGetTableByName(c, s.ctx, "test", "t")
		c.Assert(tbl, NotNil)
		c.Assert(tbl.Meta().Charset, Equals, chs)
		c.Assert(tbl.Meta().Collate, Equals, coll)
		for _, col := range tbl.Meta().Columns {
			c.Assert(col.Charset, Equals, chs)
			c.Assert(col.Collate, Equals, coll)
		}
	}
	checkCharset(charset.CharsetUTF8MB4, charset.CollationUTF8MB4)

	// Test when column charset can not convert to the target charset.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a varchar(10) character set ascii) charset utf8mb4")
	tk.MustGetErrCode("alter table t convert to charset utf8mb4;", errno.ErrUnsupportedDDLOperation)

	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a varchar(10) character set utf8) charset utf8")
	tk.MustExec("alter table t convert to charset utf8 collate utf8_general_ci;")
	checkCharset(charset.CharsetUTF8, "utf8_general_ci")

	// Test when table charset is equal to target charset but column charset is not equal.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a varchar(10) character set utf8) charset utf8mb4")
	tk.MustExec("alter table t convert to charset utf8mb4 collate utf8mb4_general_ci;")
	checkCharset(charset.CharsetUTF8MB4, "utf8mb4_general_ci")

	// Mock table info with charset is "". Old TiDB maybe create table with charset is "".
	db, ok := domain.GetDomain(s.ctx).InfoSchema().SchemaByName(model.NewCIStr("test"))
	c.Assert(ok, IsTrue)
	tbl := testGetTableByName(c, s.ctx, "test", "t")
	tblInfo := tbl.Meta().Clone()
	tblInfo.Charset = ""
	tblInfo.Collate = ""
	updateTableInfo := func(tblInfo *model.TableInfo) {
		mockCtx := mock.NewContext()
		mockCtx.Store = s.store
		err := mockCtx.NewTxn(context.Background())
		c.Assert(err, IsNil)
		txn, err := mockCtx.Txn(true)
		c.Assert(err, IsNil)
		mt := meta.NewMeta(txn)

		err = mt.UpdateTable(db.ID, tblInfo)
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
	}
	updateTableInfo(tblInfo)

	// check table charset is ""
	tk.MustExec("alter table t add column b varchar(10);") //  load latest schema.
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().Charset, Equals, "")
	c.Assert(tbl.Meta().Collate, Equals, "")
	// Test when table charset is "", this for compatibility.
	tk.MustExec("alter table t convert to charset utf8mb4;")
	checkCharset(charset.CharsetUTF8MB4, charset.CollationUTF8MB4)

	// Test when column charset is "".
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	tblInfo = tbl.Meta().Clone()
	tblInfo.Columns[0].Charset = ""
	tblInfo.Columns[0].Collate = ""
	updateTableInfo(tblInfo)
	// check table charset is ""
	tk.MustExec("alter table t drop column b;") //  load latest schema.
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().Columns[0].Charset, Equals, "")
	c.Assert(tbl.Meta().Columns[0].Collate, Equals, "")
	tk.MustExec("alter table t convert to charset utf8mb4;")
	checkCharset(charset.CharsetUTF8MB4, charset.CollationUTF8MB4)

	tk.MustExec("drop table t")
	tk.MustExec("create table t (a blob) character set utf8;")
	tk.MustExec("alter table t charset=utf8mb4 collate=utf8mb4_bin;")
	tk.MustQuery("show create table t").Check(testutil.RowsWithSep("|",
		"t CREATE TABLE `t` (\n"+
			"  `a` blob DEFAULT NULL\n"+
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a varchar(5) charset utf8) charset utf8")
	tk.MustExec("alter table t charset utf8mb4")
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().Charset, Equals, "utf8mb4")
	c.Assert(tbl.Meta().Collate, Equals, "utf8mb4_bin")
	for _, col := range tbl.Meta().Columns {
		// Column charset and collate should remain unchanged.
		c.Assert(col.Charset, Equals, "utf8")
		c.Assert(col.Collate, Equals, "utf8_bin")
	}

	tk.MustExec("drop table t")
	tk.MustExec("create table t(a varchar(5) charset utf8 collate utf8_unicode_ci) charset utf8 collate utf8_unicode_ci")
	tk.MustExec("alter table t collate utf8_danish_ci")
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().Charset, Equals, "utf8")
	c.Assert(tbl.Meta().Collate, Equals, "utf8_danish_ci")
	for _, col := range tbl.Meta().Columns {
		c.Assert(col.Charset, Equals, "utf8")
		// Column collate should remain unchanged.
		c.Assert(col.Collate, Equals, "utf8_unicode_ci")
	}
}

func (s *testIntegrationSuite5) TestModifyColumnOption(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	errMsg := "[ddl:8200]" // unsupported modify column with references
	assertErrCode := func(sql string, errCodeStr string) {
		_, err := tk.Exec(sql)
		c.Assert(err, NotNil)
		c.Assert(err.Error()[:len(errCodeStr)], Equals, errCodeStr)
	}

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (b char(1) default null) engine=InnoDB default charset=utf8mb4 collate=utf8mb4_general_ci")
	tk.MustExec("alter table t1 modify column b char(1) character set utf8mb4 collate utf8mb4_general_ci")

	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (b char(1) collate utf8mb4_general_ci)")
	tk.MustExec("alter table t1 modify b char(1) character set utf8mb4 collate utf8mb4_general_ci")

	tk.MustExec("drop table t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (a int(11) default null)")
	tk.MustExec("create table t2 (b char, c int)")
	assertErrCode("alter table t2 modify column c int references t1(a)", errMsg)
	_, err := tk.Exec("alter table t1 change a a varchar(16)")
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported modify column: type varchar(16) not match origin int(11)")
	_, err = tk.Exec("alter table t1 change a a varchar(10)")
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported modify column: type varchar(10) not match origin int(11)")
	_, err = tk.Exec("alter table t1 change a a datetime")
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported modify column: type datetime not match origin int(11)")
	_, err = tk.Exec("alter table t1 change a a int(11) unsigned")
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported modify column: can't change unsigned integer to signed or vice versa, and tidb_enable_change_column_type is false")
	_, err = tk.Exec("alter table t2 change b b int(11) unsigned")
	c.Assert(err.Error(), Equals, "[ddl:8200]Unsupported modify column: type int(11) not match origin char(1)")
}

func (s *testIntegrationSuite4) TestIndexOnMultipleGeneratedColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int as (a + 1), c int as (b + 1))")
	tk.MustExec("insert into t (a) values (1)")
	tk.MustExec("create index idx on t (c)")
	tk.MustQuery("select * from t where c > 1").Check(testkit.Rows("1 2 3"))
	res := tk.MustQuery("select * from t use index(idx) where c > 1")
	tk.MustQuery("select * from t ignore index(idx) where c > 1").Check(res.Rows())
	tk.MustExec("admin check table t")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int as (a + 1), c int as (b + 1), d int as (c + 1))")
	tk.MustExec("insert into t (a) values (1)")
	tk.MustExec("create index idx on t (d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("1 2 3 4"))
	res = tk.MustQuery("select * from t use index(idx) where d > 2")
	tk.MustQuery("select * from t ignore index(idx) where d > 2").Check(res.Rows())
	tk.MustExec("admin check table t")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a bigint, b decimal as (a+1), c varchar(20) as (b*2), d float as (a*23+b-1+length(c)))")
	tk.MustExec("insert into t (a) values (1)")
	tk.MustExec("create index idx on t (d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("1 2 4 25"))
	res = tk.MustQuery("select * from t use index(idx) where d > 2")
	tk.MustQuery("select * from t ignore index(idx) where d > 2").Check(res.Rows())
	tk.MustExec("admin check table t")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10), b float as (length(a)+123), c varchar(20) as (right(a, 2)), d float as (b+b-7+1-3+3*ASCII(c)))")
	tk.MustExec("insert into t (a) values ('adorable')")
	tk.MustExec("create index idx on t (d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("adorable 131 le 577")) // 131+131-7+1-3+3*108
	res = tk.MustQuery("select * from t use index(idx) where d > 2")
	tk.MustQuery("select * from t ignore index(idx) where d > 2").Check(res.Rows())
	tk.MustExec("admin check table t")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a bigint, b decimal as (a), c int(10) as (a+b), d float as (a+b+c), e decimal as (a+b+c+d))")
	tk.MustExec("insert into t (a) values (1)")
	tk.MustExec("create index idx on t (d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("1 1 2 4 8"))
	res = tk.MustQuery("select * from t use index(idx) where d > 2")
	tk.MustQuery("select * from t ignore index(idx) where d > 2").Check(res.Rows())
	tk.MustExec("admin check table t")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint, b bigint as (a+1) virtual, c bigint as (b+1) virtual)")
	tk.MustExec("alter table t add index idx_b(b)")
	tk.MustExec("alter table t add index idx_c(c)")
	tk.MustExec("insert into t(a) values(1)")
	tk.MustExec("alter table t add column(d bigint as (c+1) virtual)")
	tk.MustExec("alter table t add index idx_d(d)")
	tk.MustQuery("select * from t where d > 2").Check(testkit.Rows("1 2 3 4"))
	res = tk.MustQuery("select * from t use index(idx_d) where d > 2")
	tk.MustQuery("select * from t ignore index(idx_d) where d > 2").Check(res.Rows())
	tk.MustExec("admin check table t")
}

func (s *testIntegrationSuite2) TestCaseInsensitiveCharsetAndCollate(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("create database if not exists test_charset_collate")
	defer tk.MustExec("drop database test_charset_collate")
	tk.MustExec("use test_charset_collate")
	tk.MustExec("create table t(id int) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COLLATE=UTF8_BIN;")
	tk.MustExec("create table t1(id int) ENGINE=InnoDB DEFAULT CHARSET=UTF8 COLLATE=uTF8_BIN;")
	tk.MustExec("create table t2(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8 COLLATE=utf8_BIN;")
	tk.MustExec("create table t3(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8mb4 COLLATE=utf8MB4_BIN;")
	tk.MustExec("create table t4(id int) ENGINE=InnoDB DEFAULT CHARSET=Utf8mb4 COLLATE=utf8MB4_general_ci;")

	tk.MustExec("create table t5(a varchar(20)) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=UTF8MB4_GENERAL_CI;")
	tk.MustExec("insert into t5 values ('特克斯和凯科斯群岛')")

	db, ok := domain.GetDomain(s.ctx).InfoSchema().SchemaByName(model.NewCIStr("test_charset_collate"))
	c.Assert(ok, IsTrue)
	tbl := testGetTableByName(c, s.ctx, "test_charset_collate", "t5")
	tblInfo := tbl.Meta().Clone()
	c.Assert(tblInfo.Charset, Equals, "utf8mb4")
	c.Assert(tblInfo.Columns[0].Charset, Equals, "utf8mb4")

	tblInfo.Version = model.TableInfoVersion2
	tblInfo.Charset = "UTF8MB4"

	updateTableInfo := func(tblInfo *model.TableInfo) {
		mockCtx := mock.NewContext()
		mockCtx.Store = s.store
		err := mockCtx.NewTxn(context.Background())
		c.Assert(err, IsNil)
		txn, err := mockCtx.Txn(true)
		c.Assert(err, IsNil)
		mt := meta.NewMeta(txn)
		c.Assert(ok, IsTrue)
		err = mt.UpdateTable(db.ID, tblInfo)
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
	}
	updateTableInfo(tblInfo)
	tk.MustExec("alter table t5 add column b varchar(10);") //  load latest schema.

	tblInfo = testGetTableByName(c, s.ctx, "test_charset_collate", "t5").Meta()
	c.Assert(tblInfo.Charset, Equals, "utf8mb4")
	c.Assert(tblInfo.Columns[0].Charset, Equals, "utf8mb4")

	// For model.TableInfoVersion3, it is believed that all charsets / collations are lower-cased, do not do case-convert
	tblInfo = tblInfo.Clone()
	tblInfo.Version = model.TableInfoVersion3
	tblInfo.Charset = "UTF8MB4"
	updateTableInfo(tblInfo)
	tk.MustExec("alter table t5 add column c varchar(10);") //  load latest schema.

	tblInfo = testGetTableByName(c, s.ctx, "test_charset_collate", "t5").Meta()
	c.Assert(tblInfo.Charset, Equals, "UTF8MB4")
	c.Assert(tblInfo.Columns[0].Charset, Equals, "utf8mb4")
}

func (s *testIntegrationSuite3) TestZeroFillCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists abc;")
	tk.MustExec("create table abc(y year, z tinyint(10) zerofill, primary key(y));")
	is := s.dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("abc"))
	c.Assert(err, IsNil)
	var yearCol, zCol *model.ColumnInfo
	for _, col := range tbl.Meta().Columns {
		if col.Name.String() == "y" {
			yearCol = col
		}
		if col.Name.String() == "z" {
			zCol = col
		}
	}
	c.Assert(yearCol, NotNil)
	c.Assert(yearCol.Tp, Equals, mysql.TypeYear)
	c.Assert(mysql.HasUnsignedFlag(yearCol.Flag), IsTrue)

	c.Assert(zCol, NotNil)
	c.Assert(mysql.HasUnsignedFlag(zCol.Flag), IsTrue)
}

func (s *testIntegrationSuite5) TestBitDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t_bit (c1 bit(10) default 250, c2 int);")
	tk.MustExec("insert into t_bit set c2=1;")
	tk.MustQuery("select bin(c1),c2 from t_bit").Check(testkit.Rows("11111010 1"))
	tk.MustExec("drop table t_bit")

	tk.MustExec("create table t_bit (a int)")
	tk.MustExec("insert into t_bit value (1)")
	tk.MustExec("alter table t_bit add column c bit(16) null default b'1100110111001'")
	tk.MustQuery("select c from t_bit").Check(testkit.Rows("\x19\xb9"))
	tk.MustExec("update t_bit set c = b'11100000000111'")
	tk.MustQuery("select c from t_bit").Check(testkit.Rows("\x38\x07"))

	tk.MustExec(`create table testalltypes1 (
    field_1 bit default 1,
    field_2 tinyint null default null
	);`)
	tk.MustExec(`create table testalltypes2 (
    field_1 bit null default null,
    field_2 tinyint null default null,
    field_3 tinyint unsigned null default null,
    field_4 bigint null default null,
    field_5 bigint unsigned null default null,
    field_6 mediumblob null default null,
    field_7 longblob null default null,
    field_8 blob null default null,
    field_9 tinyblob null default null,
    field_10 varbinary(255) null default null,
    field_11 binary(255) null default null,
    field_12 mediumtext null default null,
    field_13 longtext null default null,
    field_14 text null default null,
    field_15 tinytext null default null,
    field_16 char(255) null default null,
    field_17 numeric null default null,
    field_18 decimal null default null,
    field_19 integer null default null,
    field_20 integer unsigned null default null,
    field_21 int null default null,
    field_22 int unsigned null default null,
    field_23 mediumint null default null,
    field_24 mediumint unsigned null default null,
    field_25 smallint null default null,
    field_26 smallint unsigned null default null,
    field_27 float null default null,
    field_28 double null default null,
    field_29 double precision null default null,
    field_30 real null default null,
    field_31 varchar(255) null default null,
    field_32 date null default null,
    field_33 time null default null,
    field_34 datetime null default null,
    field_35 timestamp null default null
	);`)
}

func (s *testIntegrationSuite5) TestBackwardCompatibility(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test_backward_compatibility")
	defer tk.MustExec("drop database test_backward_compatibility")
	tk.MustExec("use test_backward_compatibility")
	tk.MustExec("create table t(a int primary key, b int)")
	for i := 0; i < 200; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%v, %v)", i, i))
	}

	// alter table t add index idx_b(b);
	is := s.dom.InfoSchema()
	schemaName := model.NewCIStr("test_backward_compatibility")
	tableName := model.NewCIStr("t")
	schema, ok := is.SchemaByName(schemaName)
	c.Assert(ok, IsTrue)
	tbl, err := is.TableByName(schemaName, tableName)
	c.Assert(err, IsNil)

	// Split the table.
	s.cluster.SplitTable(tbl.Meta().ID, 100)

	unique := false
	indexName := model.NewCIStr("idx_b")
	indexPartSpecification := &ast.IndexPartSpecification{
		Column: &ast.ColumnName{
			Schema: schemaName,
			Table:  tableName,
			Name:   model.NewCIStr("b"),
		},
		Length: types.UnspecifiedLength,
	}
	indexPartSpecifications := []*ast.IndexPartSpecification{indexPartSpecification}
	var indexOption *ast.IndexOption
	job := &model.Job{
		SchemaID:   schema.ID,
		TableID:    tbl.Meta().ID,
		Type:       model.ActionAddIndex,
		BinlogInfo: &model.HistoryInfo{},
		Args:       []interface{}{unique, indexName, indexPartSpecifications, indexOption},
	}
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	t := meta.NewMeta(txn)
	job.ID, err = t.GenGlobalID()
	c.Assert(err, IsNil)
	job.Version = 1
	job.StartTS = txn.StartTS()

	// Simulate old TiDB init the add index job, old TiDB will not init the model.Job.ReorgMeta field,
	// if we set job.SnapshotVer here, can simulate the behavior.
	job.SnapshotVer = txn.StartTS()
	err = t.EnQueueDDLJob(job)
	c.Assert(err, IsNil)
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)
	ticker := time.NewTicker(s.lease)
	defer ticker.Stop()
	for range ticker.C {
		historyJob, err := getHistoryDDLJob(s.store, job.ID)
		c.Assert(err, IsNil)
		if historyJob == nil {

			continue
		}
		c.Assert(historyJob.Error, IsNil)

		if historyJob.IsSynced() {
			break
		}
	}

	// finished add index
	tk.MustExec("admin check index t idx_b")
}

func (s *testIntegrationSuite3) TestMultiRegionGetTableEndHandle(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("drop database if exists test_get_endhandle")
	tk.MustExec("create database test_get_endhandle")
	tk.MustExec("use test_get_endhandle")

	tk.MustExec("create table t(a bigint PRIMARY KEY, b int)")
	for i := 0; i < 1000; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%v, %v)", i, i))
	}

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_get_endhandle"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	d := s.dom.DDL()
	testCtx := newTestMaxTableRowIDContext(c, d, tbl)

	// Split the table.
	s.cluster.SplitTable(tblID, 100)

	maxHandle, emptyTable := getMaxTableHandle(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxHandle, Equals, kv.IntHandle(1000))

	tk.MustExec("insert into t values(10000, 1000)")
	maxHandle, emptyTable = getMaxTableHandle(testCtx, s.store)
	c.Assert(emptyTable, IsFalse)
	c.Assert(maxHandle, Equals, kv.IntHandle(1001))
}

type testMaxTableRowIDContext struct {
	c   *C
	d   ddl.DDL
	tbl table.Table
}

func newTestMaxTableRowIDContext(c *C, d ddl.DDL, tbl table.Table) *testMaxTableRowIDContext {
	return &testMaxTableRowIDContext{
		c:   c,
		d:   d,
		tbl: tbl,
	}
}

func getMaxTableHandle(ctx *testMaxTableRowIDContext, store kv.Storage) (kv.Handle, bool) {
	c := ctx.c
	d := ctx.d
	tbl := ctx.tbl
	curVer, err := store.CurrentVersion()
	c.Assert(err, IsNil)
	maxHandle, emptyTable, err := d.GetTableMaxHandle(curVer.Ver, tbl.(table.PhysicalTable))
	c.Assert(err, IsNil)
	return maxHandle, emptyTable
}

func checkGetMaxTableRowID(ctx *testMaxTableRowIDContext, store kv.Storage, expectEmpty bool, expectMaxHandle kv.Handle) {
	c := ctx.c
	maxHandle, emptyTable := getMaxTableHandle(ctx, store)
	c.Assert(emptyTable, Equals, expectEmpty)
	c.Assert(maxHandle, testutil.HandleEquals, expectMaxHandle)
}

func getHistoryDDLJob(store kv.Storage, id int64) (*model.Job, error) {
	var job *model.Job

	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		var err1 error
		job, err1 = t.GetHistoryDDLJob(id)
		return errors.Trace(err1)
	})

	return job, errors.Trace(err)
}

func (s *testIntegrationSuite6) TestCreateTableTooLarge(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	sql := "create table t_too_large ("
	cnt := 3000
	for i := 1; i <= cnt; i++ {
		sql += fmt.Sprintf("a%d double, b%d double, c%d double, d%d double", i, i, i, i)
		if i != cnt {
			sql += ","
		}
	}
	sql += ");"
	tk.MustGetErrCode(sql, errno.ErrTooManyFields)

	originLimit := atomic.LoadUint32(&ddl.TableColumnCountLimit)
	atomic.StoreUint32(&ddl.TableColumnCountLimit, uint32(cnt*4))
	_, err := tk.Exec(sql)
	c.Assert(kv.ErrEntryTooLarge.Equal(err), IsTrue, Commentf("err:%v", err))
	atomic.StoreUint32(&ddl.TableColumnCountLimit, originLimit)
}

func (s *testIntegrationSuite3) TestChangeColumnPosition(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("create table position (a int default 1, b int default 2)")
	tk.MustExec("insert into position value ()")
	tk.MustExec("insert into position values (3,4)")
	tk.MustQuery("select * from position").Check(testkit.Rows("1 2", "3 4"))
	tk.MustExec("alter table position modify column b int first")
	tk.MustQuery("select * from position").Check(testkit.Rows("2 1", "4 3"))
	tk.MustExec("insert into position value ()")
	tk.MustQuery("select * from position").Check(testkit.Rows("2 1", "4 3", "<nil> 1"))

	tk.MustExec("create table position1 (a int, b int, c double, d varchar(5))")
	tk.MustExec(`insert into position1 value (1, 2, 3.14, 'TiDB')`)
	tk.MustExec("alter table position1 modify column d varchar(5) after a")
	tk.MustQuery("select * from position1").Check(testkit.Rows("1 TiDB 2 3.14"))
	tk.MustExec("alter table position1 modify column a int after c")
	tk.MustQuery("select * from position1").Check(testkit.Rows("TiDB 2 3.14 1"))
	tk.MustExec("alter table position1 modify column c double first")
	tk.MustQuery("select * from position1").Check(testkit.Rows("3.14 TiDB 2 1"))
	tk.MustGetErrCode("alter table position1 modify column b int after b", errno.ErrBadField)

	tk.MustExec("create table position2 (a int, b int)")
	tk.MustExec("alter table position2 add index t(a, b)")
	tk.MustExec("alter table position2 modify column b int first")
	tk.MustExec("insert into position2 value (3, 5)")
	tk.MustQuery("select a from position2 where a = 3").Check(testkit.Rows())
	tk.MustExec("alter table position2 change column b c int first")
	tk.MustQuery("select * from position2 where c = 3").Check(testkit.Rows("3 5"))
	tk.MustGetErrCode("alter table position2 change column c b int after c", errno.ErrBadField)

	tk.MustExec("create table position3 (a int default 2)")
	tk.MustExec("alter table position3 modify column a int default 5 first")
	tk.MustExec("insert into position3 value ()")
	tk.MustQuery("select * from position3").Check(testkit.Rows("5"))

	tk.MustExec("create table position4 (a int, b int)")
	tk.MustExec("alter table position4 add index t(b)")
	tk.MustExec("alter table position4 change column b c int first")
	createSQL := tk.MustQuery("show create table position4").Rows()[0][1]
	exceptedSQL := []string{
		"CREATE TABLE `position4` (",
		"  `c` int(11) DEFAULT NULL,",
		"  `a` int(11) DEFAULT NULL,",
		"  KEY `t` (`c`)",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin",
	}
	c.Assert(createSQL, Equals, strings.Join(exceptedSQL, "\n"))
}

func (s *testIntegrationSuite2) TestAddIndexAfterAddColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("create table test_add_index_after_add_col(a int, b int not null default '0')")
	tk.MustExec("insert into test_add_index_after_add_col values(1, 2),(2,2)")
	tk.MustExec("alter table test_add_index_after_add_col add column c int not null default '0'")
	sql := "alter table test_add_index_after_add_col add unique index cc(c) "
	tk.MustGetErrCode(sql, errno.ErrDupEntry)
	sql = "alter table test_add_index_after_add_col add index idx_test(f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15,f16,f17);"
	tk.MustGetErrCode(sql, errno.ErrTooManyKeyParts)
}

func (s *testIntegrationSuite3) TestResolveCharset(c *C) {
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

func (s *testIntegrationSuite6) TestAddColumnTooMany(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	count := int(atomic.LoadUint32(&ddl.TableColumnCountLimit) - 1)
	var cols []string
	for i := 0; i < count; i++ {
		cols = append(cols, fmt.Sprintf("a%d int", i))
	}
	createSQL := fmt.Sprintf("create table t_column_too_many (%s)", strings.Join(cols, ","))
	tk.MustExec(createSQL)
	tk.MustExec("alter table t_column_too_many add column a_512 int")
	alterSQL := "alter table t_column_too_many add column a_513 int"
	tk.MustGetErrCode(alterSQL, errno.ErrTooManyFields)
}

func (s *testIntegrationSuite3) TestAlterColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")

	tk.MustExec("create table test_alter_column (a int default 111, b varchar(8), c varchar(8) not null, d timestamp on update current_timestamp)")
	tk.MustExec("insert into test_alter_column set b = 'a', c = 'aa'")
	tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111"))
	ctx := tk.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo := tbl.Meta()
	colA := tblInfo.Columns[0]
	hasNoDefault := mysql.HasNoDefaultValueFlag(colA.Flag)
	c.Assert(hasNoDefault, IsFalse)
	tk.MustExec("alter table test_alter_column alter column a set default 222")
	tk.MustExec("insert into test_alter_column set b = 'b', c = 'bb'")
	tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111", "222"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colA = tblInfo.Columns[0]
	hasNoDefault = mysql.HasNoDefaultValueFlag(colA.Flag)
	c.Assert(hasNoDefault, IsFalse)
	tk.MustExec("alter table test_alter_column alter column b set default null")
	tk.MustExec("insert into test_alter_column set c = 'cc'")
	tk.MustQuery("select b from test_alter_column").Check(testkit.Rows("a", "b", "<nil>"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colC := tblInfo.Columns[2]
	hasNoDefault = mysql.HasNoDefaultValueFlag(colC.Flag)
	c.Assert(hasNoDefault, IsTrue)
	tk.MustExec("alter table test_alter_column alter column c set default 'xx'")
	tk.MustExec("insert into test_alter_column set a = 123")
	tk.MustQuery("select c from test_alter_column").Check(testkit.Rows("aa", "bb", "cc", "xx"))
	is = domain.GetDomain(ctx).InfoSchema()
	tbl, err = is.TableByName(model.NewCIStr("test_db"), model.NewCIStr("test_alter_column"))
	c.Assert(err, IsNil)
	tblInfo = tbl.Meta()
	colC = tblInfo.Columns[2]
	hasNoDefault = mysql.HasNoDefaultValueFlag(colC.Flag)
	c.Assert(hasNoDefault, IsFalse)
	// TODO: After fix issue 2606.
	// tk.MustExec( "alter table test_alter_column alter column d set default null")
	tk.MustExec("alter table test_alter_column alter column a drop default")
	tk.MustExec("insert into test_alter_column set b = 'd', c = 'dd'")
	tk.MustQuery("select a from test_alter_column").Check(testkit.Rows("111", "222", "222", "123", "<nil>"))

	// for failing tests
	sql := "alter table db_not_exist.test_alter_column alter column b set default 'c'"
	tk.MustGetErrCode(sql, errno.ErrNoSuchTable)
	sql = "alter table test_not_exist alter column b set default 'c'"
	tk.MustGetErrCode(sql, errno.ErrNoSuchTable)
	sql = "alter table test_alter_column alter column col_not_exist set default 'c'"
	tk.MustGetErrCode(sql, errno.ErrBadField)
	sql = "alter table test_alter_column alter column c set default null"
	tk.MustGetErrCode(sql, errno.ErrInvalidDefault)

	// The followings tests whether adding constraints via change / modify column
	// is forbidden as expected.
	tk.MustExec("drop table if exists mc")
	tk.MustExec("create table mc(a int key, b int, c int)")
	_, err = tk.Exec("alter table mc modify column a int key") // Adds a new primary key
	c.Assert(err, NotNil)
	_, err = tk.Exec("alter table mc modify column c int unique") // Adds a new unique key
	c.Assert(err, NotNil)
	result := tk.MustQuery("show create table mc")
	createSQL := result.Rows()[0][1]
	expected := "CREATE TABLE `mc` (\n  `a` int(11) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createSQL, Equals, expected)

	// Change / modify column should preserve index options.
	tk.MustExec("drop table if exists mc")
	tk.MustExec("create table mc(a int key, b int, c int unique)")
	tk.MustExec("alter table mc modify column a bigint") // NOT NULL & PRIMARY KEY should be preserved
	tk.MustExec("alter table mc modify column b bigint")
	tk.MustExec("alter table mc modify column c bigint") // Unique should be preserved
	result = tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL,\n  `b` bigint(20) DEFAULT NULL,\n  `c` bigint(20) DEFAULT NULL,\n  PRIMARY KEY (`a`),\n  UNIQUE KEY `c` (`c`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createSQL, Equals, expected)

	// Dropping or keeping auto_increment is allowed, however adding is not allowed.
	tk.MustExec("drop table if exists mc")
	tk.MustExec("create table mc(a int key auto_increment, b int)")
	tk.MustExec("alter table mc modify column a bigint auto_increment") // Keeps auto_increment
	result = tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL AUTO_INCREMENT,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createSQL, Equals, expected)
	_, err = tk.Exec("alter table mc modify column a bigint") // Droppping auto_increment is not allow when @@tidb_allow_remove_auto_inc == 'off'
	c.Assert(err, NotNil)
	tk.MustExec("set @@tidb_allow_remove_auto_inc = on")
	tk.MustExec("alter table mc modify column a bigint") // Dropping auto_increment is ok when @@tidb_allow_remove_auto_inc == 'on'
	result = tk.MustQuery("show create table mc")
	createSQL = result.Rows()[0][1]
	expected = "CREATE TABLE `mc` (\n  `a` bigint(20) NOT NULL,\n  `b` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	c.Assert(createSQL, Equals, expected)

	_, err = tk.Exec("alter table mc modify column a bigint auto_increment") // Adds auto_increment should throw error
	c.Assert(err, NotNil)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1 (a varchar(10),b varchar(100),c tinyint,d varchar(3071),index(a),index(a,b),index (c,d)) charset = ascii;")
	tk.MustGetErrCode("alter table t1 modify column a varchar(3000);", errno.ErrTooLongKey)
	// check modify column with rename column.
	tk.MustGetErrCode("alter table t1 change column a x varchar(3000);", errno.ErrTooLongKey)
	tk.MustGetErrCode("alter table t1 modify column c bigint;", errno.ErrTooLongKey)

	tk.MustExec("drop table if exists multi_unique")
	tk.MustExec("create table multi_unique (a int unique unique)")
	tk.MustExec("drop table multi_unique")
	tk.MustExec("create table multi_unique (a int key primary key unique unique)")
	tk.MustExec("drop table multi_unique")
	tk.MustExec("create table multi_unique (a int key unique unique key unique)")
	tk.MustExec("drop table multi_unique")
	tk.MustExec("create table multi_unique (a serial serial default value)")
	tk.MustExec("drop table multi_unique")
	tk.MustExec("create table multi_unique (a serial serial default value serial default value)")
	tk.MustExec("drop table multi_unique")
}

func (s *testIntegrationSuite) assertWarningExec(tk *testkit.TestKit, c *C, sql string, expectedWarn *terror.Error) {
	_, err := tk.Exec(sql)
	c.Assert(err, IsNil)
	st := tk.Se.GetSessionVars().StmtCtx
	c.Assert(st.WarningCount(), Equals, uint16(1))
	c.Assert(expectedWarn.Equal(st.GetWarnings()[0].Err), IsTrue, Commentf("error:%v", err))
}

func (s *testIntegrationSuite) assertAlterWarnExec(tk *testkit.TestKit, c *C, sql string) {
	s.assertWarningExec(tk, c, sql, ddl.ErrAlterOperationNotSupported)
}

func (s *testIntegrationSuite) assertAlterErrorExec(tk *testkit.TestKit, c *C, sql string) {
	tk.MustGetErrCode(sql, errno.ErrAlterOperationNotSupportedReason)
}

func (s *testIntegrationSuite3) TestAlterAlgorithm(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	defer tk.MustExec("drop table if exists t")

	tk.MustExec(`create table t(
	a int,
	b varchar(100),
	c int,
	INDEX idx_c(c)) PARTITION BY RANGE ( a ) (
	PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
	)`)
	s.assertAlterErrorExec(tk, c, "alter table t modify column a bigint, ALGORITHM=INPLACE;")
	tk.MustExec("alter table t modify column a bigint, ALGORITHM=INPLACE, ALGORITHM=INSTANT;")
	tk.MustExec("alter table t modify column a bigint, ALGORITHM=DEFAULT;")

	// Test add/drop index
	s.assertAlterErrorExec(tk, c, "alter table t add index idx_b(b), ALGORITHM=INSTANT")
	s.assertAlterWarnExec(tk, c, "alter table t add index idx_b1(b), ALGORITHM=COPY")
	tk.MustExec("alter table t add index idx_b2(b), ALGORITHM=INPLACE")
	s.assertAlterErrorExec(tk, c, "alter table t drop index idx_b, ALGORITHM=INPLACE")
	s.assertAlterWarnExec(tk, c, "alter table t drop index idx_b1, ALGORITHM=COPY")
	tk.MustExec("alter table t drop index idx_b2, ALGORITHM=INSTANT")

	// Test rename
	s.assertAlterWarnExec(tk, c, "alter table t rename to t1, ALGORITHM=COPY")
	s.assertAlterErrorExec(tk, c, "alter table t1 rename to t, ALGORITHM=INPLACE")
	tk.MustExec("alter table t1 rename to t, ALGORITHM=INSTANT")
	tk.MustExec("alter table t rename to t1, ALGORITHM=DEFAULT")
	tk.MustExec("alter table t1 rename to t")

	// Test rename index
	s.assertAlterWarnExec(tk, c, "alter table t rename index idx_c to idx_c1, ALGORITHM=COPY")
	s.assertAlterErrorExec(tk, c, "alter table t rename index idx_c1 to idx_c, ALGORITHM=INPLACE")
	tk.MustExec("alter table t rename index idx_c1 to idx_c, ALGORITHM=INSTANT")
	tk.MustExec("alter table t rename index idx_c to idx_c1, ALGORITHM=DEFAULT")

	// partition.
	s.assertAlterWarnExec(tk, c, "alter table t ALGORITHM=COPY, truncate partition p1")
	s.assertAlterErrorExec(tk, c, "alter table t ALGORITHM=INPLACE, truncate partition p2")
	tk.MustExec("alter table t ALGORITHM=INSTANT, truncate partition p3")

	s.assertAlterWarnExec(tk, c, "alter table t add partition (partition p4 values less than (2002)), ALGORITHM=COPY")
	s.assertAlterErrorExec(tk, c, "alter table t add partition (partition p5 values less than (3002)), ALGORITHM=INPLACE")
	tk.MustExec("alter table t add partition (partition p6 values less than (4002)), ALGORITHM=INSTANT")

	s.assertAlterWarnExec(tk, c, "alter table t ALGORITHM=COPY, drop partition p4")
	s.assertAlterErrorExec(tk, c, "alter table t ALGORITHM=INPLACE, drop partition p5")
	tk.MustExec("alter table t ALGORITHM=INSTANT, drop partition p6")

	// Table options
	s.assertAlterWarnExec(tk, c, "alter table t comment = 'test', ALGORITHM=COPY")
	s.assertAlterErrorExec(tk, c, "alter table t comment = 'test', ALGORITHM=INPLACE")
	tk.MustExec("alter table t comment = 'test', ALGORITHM=INSTANT")

	s.assertAlterWarnExec(tk, c, "alter table t default charset = utf8mb4, ALGORITHM=COPY")
	s.assertAlterErrorExec(tk, c, "alter table t default charset = utf8mb4, ALGORITHM=INPLACE")
	tk.MustExec("alter table t default charset = utf8mb4, ALGORITHM=INSTANT")
}

func (s *testIntegrationSuite3) TestAlterTableAddUniqueOnPartionRangeColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")

	tk.MustExec(`create table t(
	a int,
	b varchar(100),
	c int,
	INDEX idx_c(c))
	PARTITION BY RANGE COLUMNS( a ) (
		PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
	)`)
	tk.MustExec("insert into t values (4, 'xxx', 4)")
	tk.MustExec("insert into t values (4, 'xxx', 9)") // Note the repeated 4
	tk.MustExec("insert into t values (17, 'xxx', 12)")
	tk.MustGetErrCode("alter table t add unique index idx_a(a)", errno.ErrDupEntry)

	tk.MustExec("delete from t where a = 4")
	tk.MustExec("alter table t add unique index idx_a(a)")
	tk.MustExec("alter table t add unique index idx_ac(a, c)")
	tk.MustGetErrCode("alter table t add unique index idx_b(b)", errno.ErrUniqueKeyNeedAllFieldsInPf)
}

func (s *testIntegrationSuite5) TestFulltextIndexIgnore(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_ft")
	defer tk.MustExec("drop table if exists t_ft")
	// Make sure that creating and altering to add a fulltext key gives the correct warning
	s.assertWarningExec(tk, c, "create table t_ft (a text, fulltext key (a))", ddl.ErrTableCantHandleFt)
	s.assertWarningExec(tk, c, "alter table t_ft add fulltext key (a)", ddl.ErrTableCantHandleFt)

	// Make sure table t_ft still has no indexes even after it was created and altered
	r := tk.MustQuery("show index from t_ft")
	c.Assert(r.Rows(), HasLen, 0)
	r = tk.MustQuery("select * from information_schema.statistics where table_schema='test' and table_name='t_ft'")
	c.Assert(r.Rows(), HasLen, 0)
}

func (s *testIntegrationSuite1) TestTreatOldVersionUTF8AsUTF8MB4(c *C) {
	if israce.RaceEnabled {
		c.Skip("skip race test")
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (a varchar(10) character set utf8, b varchar(10) character set ascii) charset=utf8mb4;")
	tk.MustGetErrCode("insert into t set a= x'f09f8c80';", errno.ErrTruncatedWrongValueForField)
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Mock old version table info with column charset is utf8.
	db, ok := domain.GetDomain(s.ctx).InfoSchema().SchemaByName(model.NewCIStr("test"))
	tbl := testGetTableByName(c, s.ctx, "test", "t")
	tblInfo := tbl.Meta().Clone()
	tblInfo.Version = model.TableInfoVersion0
	tblInfo.Columns[0].Version = model.ColumnInfoVersion0
	updateTableInfo := func(tblInfo *model.TableInfo) {
		mockCtx := mock.NewContext()
		mockCtx.Store = s.store
		err := mockCtx.NewTxn(context.Background())
		c.Assert(err, IsNil)
		txn, err := mockCtx.Txn(true)
		c.Assert(err, IsNil)
		mt := meta.NewMeta(txn)
		c.Assert(ok, IsTrue)
		err = mt.UpdateTable(db.ID, tblInfo)
		c.Assert(err, IsNil)
		err = txn.Commit(context.Background())
		c.Assert(err, IsNil)
	}
	updateTableInfo(tblInfo)
	tk.MustExec("alter table t add column c varchar(10) character set utf8;") // load latest schema.
	c.Assert(config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4, IsTrue)
	tk.MustExec("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,\n" +
		"  `c` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = false
	})
	tk.MustExec("alter table t drop column c;") //  reload schema.
	tk.MustGetErrCode("insert into t set a= x'f09f8c80'", errno.ErrTruncatedWrongValueForField)
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Mock old version table info with table and column charset is utf8.
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	tblInfo = tbl.Meta().Clone()
	tblInfo.Charset = charset.CharsetUTF8
	tblInfo.Collate = charset.CollationUTF8
	tblInfo.Version = model.TableInfoVersion0
	tblInfo.Columns[0].Version = model.ColumnInfoVersion0
	updateTableInfo(tblInfo)

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = true
	})
	tk.MustExec("alter table t add column c varchar(10);") //  load latest schema.
	tk.MustExec("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,\n" +
		"  `c` varchar(10) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = false
	})
	tk.MustExec("alter table t drop column c;") //  reload schema.
	tk.MustGetErrCode("insert into t set a= x'f09f8c80'", errno.ErrTruncatedWrongValueForField)
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"))

	// Test modify column charset.
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = true
	})
	tk.MustExec("alter table t modify column a varchar(10) character set utf8mb4") //  change column charset.
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl.Meta().Columns[0].Charset, Equals, charset.CharsetUTF8MB4)
	c.Assert(tbl.Meta().Columns[0].Collate, Equals, charset.CollationUTF8MB4)
	c.Assert(tbl.Meta().Columns[0].Version, Equals, model.ColumnInfoVersion0)
	tk.MustExec("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
	// Test for change column should not modify the column version.
	tk.MustExec("alter table t change column a a varchar(20)") //  change column.
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl.Meta().Columns[0].Charset, Equals, charset.CharsetUTF8MB4)
	c.Assert(tbl.Meta().Columns[0].Collate, Equals, charset.CollationUTF8MB4)
	c.Assert(tbl.Meta().Columns[0].Version, Equals, model.ColumnInfoVersion0)

	// Test for v2.1.5 and v2.1.6 that table version is 1 but column version is 0.
	tbl = testGetTableByName(c, s.ctx, "test", "t")
	tblInfo = tbl.Meta().Clone()
	tblInfo.Charset = charset.CharsetUTF8
	tblInfo.Collate = charset.CollationUTF8
	tblInfo.Version = model.TableInfoVersion1
	tblInfo.Columns[0].Version = model.ColumnInfoVersion0
	tblInfo.Columns[0].Charset = charset.CharsetUTF8
	tblInfo.Columns[0].Collate = charset.CollationUTF8
	updateTableInfo(tblInfo)
	c.Assert(config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4, IsTrue)
	tk.MustExec("alter table t change column b b varchar(20) character set ascii") // reload schema.
	tk.MustExec("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(20) DEFAULT NULL,\n" +
		"  `b` varchar(20) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = false
	})
	tk.MustExec("alter table t change column b b varchar(30) character set ascii") // reload schema.
	tk.MustGetErrCode("insert into t set a= x'f09f8c80'", errno.ErrTruncatedWrongValueForField)
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(20) DEFAULT NULL,\n" +
		"  `b` varchar(30) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"))

	// Test for alter table convert charset
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = true
	})
	tk.MustExec("alter table t drop column b") // reload schema.
	tk.MustExec("alter table t convert to charset utf8mb4;")

	config.UpdateGlobal(func(conf *config.Config) {
		conf.TreatOldVersionUTF8AsUTF8MB4 = false
	})
	tk.MustExec("alter table t add column b varchar(50);") // reload schema.
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(20) DEFAULT NULL,\n" +
		"  `b` varchar(50) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func (s *testIntegrationSuite3) TestDefaultValueIsString(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default b'1');")
	tbl := testGetTableByName(c, s.ctx, "test", "t")
	c.Assert(tbl.Meta().Columns[0].DefaultValue, Equals, "1")
}

func (s *testIntegrationSuite5) TestChangingDBCharset(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("DROP DATABASE IF EXISTS alterdb1")
	tk.MustExec("CREATE DATABASE alterdb1 CHARSET=utf8 COLLATE=utf8_unicode_ci")

	// No default DB errors.
	noDBFailedCases := []struct {
		stmt   string
		errMsg string
	}{
		{
			"ALTER DATABASE CHARACTER SET = 'utf8'",
			"[planner:1046]No database selected",
		},
		{
			"ALTER SCHEMA `` CHARACTER SET = 'utf8'",
			"[ddl:1102]Incorrect database name ''",
		},
	}
	for _, fc := range noDBFailedCases {
		c.Assert(tk.ExecToErr(fc.stmt).Error(), Equals, fc.errMsg, Commentf("%v", fc.stmt))
	}

	verifyDBCharsetAndCollate := func(dbName, chs string, coll string) {
		// check `SHOW CREATE SCHEMA`.
		r := tk.MustQuery("SHOW CREATE SCHEMA " + dbName).Rows()[0][1].(string)
		c.Assert(strings.Contains(r, "CHARACTER SET "+chs), IsTrue)

		template := `SELECT
					DEFAULT_CHARACTER_SET_NAME,
					DEFAULT_COLLATION_NAME
				FROM INFORMATION_SCHEMA.SCHEMATA
				WHERE SCHEMA_NAME = '%s'`
		sql := fmt.Sprintf(template, dbName)
		tk.MustQuery(sql).Check(testkit.Rows(fmt.Sprintf("%s %s", chs, coll)))

		dom := domain.GetDomain(s.ctx)
		// Make sure the table schema is the new schema.
		err := dom.Reload()
		c.Assert(err, IsNil)
		dbInfo, ok := dom.InfoSchema().SchemaByName(model.NewCIStr(dbName))
		c.Assert(ok, Equals, true)
		c.Assert(dbInfo.Charset, Equals, chs)
		c.Assert(dbInfo.Collate, Equals, coll)
	}

	tk.MustExec("ALTER SCHEMA alterdb1 COLLATE = utf8mb4_general_ci")
	verifyDBCharsetAndCollate("alterdb1", "utf8mb4", "utf8mb4_general_ci")

	tk.MustExec("DROP DATABASE IF EXISTS alterdb2")
	tk.MustExec("CREATE DATABASE alterdb2 CHARSET=utf8 COLLATE=utf8_unicode_ci")
	tk.MustExec("USE alterdb2")

	failedCases := []struct {
		stmt   string
		errMsg string
	}{
		{
			"ALTER SCHEMA `` CHARACTER SET = 'utf8'",
			"[ddl:1102]Incorrect database name ''",
		},
		{
			"ALTER DATABASE CHARACTER SET = ''",
			"[parser:1115]Unknown character set: ''",
		},
		{
			"ALTER DATABASE CHARACTER SET = 'INVALID_CHARSET'",
			"[parser:1115]Unknown character set: 'INVALID_CHARSET'",
		},
		{
			"ALTER SCHEMA COLLATE = ''",
			"[ddl:1273]Unknown collation: ''",
		},
		{
			"ALTER DATABASE COLLATE = 'INVALID_COLLATION'",
			"[ddl:1273]Unknown collation: 'INVALID_COLLATION'",
		},
		{
			"ALTER DATABASE CHARACTER SET = 'utf8' DEFAULT CHARSET = 'utf8mb4'",
			"[ddl:1302]Conflicting declarations: 'CHARACTER SET utf8' and 'CHARACTER SET utf8mb4'",
		},
		{
			"ALTER SCHEMA CHARACTER SET = 'utf8' COLLATE = 'utf8mb4_bin'",
			"[ddl:1302]Conflicting declarations: 'CHARACTER SET utf8' and 'CHARACTER SET utf8mb4'",
		},
		{
			"ALTER DATABASE COLLATE = 'utf8mb4_bin' COLLATE = 'utf8_bin'",
			"[ddl:1302]Conflicting declarations: 'CHARACTER SET utf8mb4' and 'CHARACTER SET utf8'",
		},
	}

	for _, fc := range failedCases {
		c.Assert(tk.ExecToErr(fc.stmt).Error(), Equals, fc.errMsg, Commentf("%v", fc.stmt))
	}
	tk.MustExec("ALTER SCHEMA CHARACTER SET = 'utf8' COLLATE = 'utf8_unicode_ci'")
	verifyDBCharsetAndCollate("alterdb2", "utf8", "utf8_unicode_ci")

	tk.MustExec("ALTER SCHEMA CHARACTER SET = 'utf8mb4'")
	verifyDBCharsetAndCollate("alterdb2", "utf8mb4", "utf8mb4_bin")

	tk.MustExec("ALTER SCHEMA CHARACTER SET = 'utf8mb4' COLLATE = 'utf8mb4_general_ci'")
	verifyDBCharsetAndCollate("alterdb2", "utf8mb4", "utf8mb4_general_ci")

	// Test changing charset of schema with uppercase name. See https://github.com/pingcap/tidb/issues/19273.
	tk.MustExec("drop database if exists TEST_UPPERCASE_DB_CHARSET;")
	tk.MustExec("create database TEST_UPPERCASE_DB_CHARSET;")
	tk.MustExec("use TEST_UPPERCASE_DB_CHARSET;")
	tk.MustExec("alter database TEST_UPPERCASE_DB_CHARSET default character set utf8;")
	tk.MustExec("alter database TEST_UPPERCASE_DB_CHARSET default character set utf8mb4;")
}

func (s *testIntegrationSuite4) TestDropAutoIncrementIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int auto_increment, unique key (a))")
	dropIndexSQL := "alter table t1 drop index a"
	tk.MustGetErrCode(dropIndexSQL, errno.ErrWrongAutoKey)

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int(11) not null auto_increment, b int(11), c bigint, unique key (a, b, c))")
	dropIndexSQL = "alter table t1 drop index a"
	tk.MustGetErrCode(dropIndexSQL, errno.ErrWrongAutoKey)
}

func (s *testIntegrationSuite4) TestInsertIntoGeneratedColumnWithDefaultExpr(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	// insert into virtual / stored columns
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int, b int as (-a) virtual, c int as (-a) stored)")
	tk.MustExec("insert into t1 values (1, default, default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 -1 -1"))
	tk.MustExec("delete from t1")

	// insert multiple rows
	tk.MustExec("insert into t1(a,b) values (1, default), (2, default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 -1 -1", "2 -2 -2"))
	tk.MustExec("delete from t1")

	// insert into generated columns only
	tk.MustExec("insert into t1(b) values (default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1(c) values (default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustExec("delete from t1")

	// generated columns with index
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 like t1")
	tk.MustExec("alter table t2 add index idx1(a)")
	tk.MustExec("alter table t2 add index idx2(b)")
	tk.MustExec("insert into t2 values (1, default, default)")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 -1 -1"))
	tk.MustExec("delete from t2")
	tk.MustExec("alter table t2 drop index idx1")
	tk.MustExec("alter table t2 drop index idx2")
	tk.MustExec("insert into t2 values (1, default, default)")
	tk.MustQuery("select * from t2").Check(testkit.Rows("1 -1 -1"))

	// generated columns in different position
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3 (gc1 int as (r+1), gc2 int as (r+1) stored, gc3 int as (gc2+1), gc4 int as (gc1+1) stored, r int)")
	tk.MustExec("insert into t3 values (default, default, default, default, 1)")
	tk.MustQuery("select * from t3").Check(testkit.Rows("2 2 3 3 1"))

	// generated columns in replace statement
	tk.MustExec("create table t4 (a int key, b int, c int as (a+1), d int as (b+1) stored)")
	tk.MustExec("insert into t4 values (1, 10, default, default)")
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 10 2 11"))
	tk.MustExec("replace into t4 values (1, 20, default, default)")
	tk.MustQuery("select * from t4").Check(testkit.Rows("1 20 2 21"))

	// generated columns with default function is not allowed
	tk.MustExec("create table t5 (a int default 10, b int as (a+1))")
	tk.MustGetErrCode("insert into t5 values (20, default(a))", errno.ErrBadGeneratedColumn)

	tk.MustExec("drop table t1, t2, t3, t4, t5")
}

func (s *testIntegrationSuite3) TestSqlFunctionsInGeneratedColumns(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")

	// In generated columns expression, these items are not allowed:
	// 1. Blocked function (for full function list, please visit https://github.com/mysql/mysql-server/blob/5.7/errno-test/suite/gcol/inc/gcol_blocked_sql_funcs_main.inc)
	// Note: This list is not complete, if you need a complete list, please refer to MySQL 5.7 source code.
	tk.MustGetErrCode("create table t (a int, b int as (sysdate()))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	// 2. Non-builtin function
	tk.MustGetErrCode("create table t (a int, b int as (non_exist_funcA()))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	// 3. values(x) function
	tk.MustGetErrCode("create table t (a int, b int as (values(a)))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	// 4. Subquery
	tk.MustGetErrCode("create table t (a int, b int as ((SELECT 1 FROM t1 UNION SELECT 1 FROM t1)))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	// 5. Variable & functions related to variable
	tk.MustExec("set @x = 1")
	tk.MustGetErrCode("create table t (a int, b int as (@x))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode("create table t (a int, b int as (@@max_connections))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode("create table t (a int, b int as (@y:=1))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode(`create table t (a int, b int as (getvalue("x")))`, errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode(`create table t (a int, b int as (setvalue("y", 1)))`, errno.ErrGeneratedColumnFunctionIsNotAllowed)
	// 6. Aggregate function
	tk.MustGetErrCode("create table t1 (a int, b int as (avg(a)));", errno.ErrInvalidGroupFuncUse)

	// Determinate functions are allowed:
	tk.MustExec("create table t1 (a int, b int generated always as (abs(a)) virtual)")
	tk.MustExec("insert into t1 values (-1, default)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("-1 1"))

	// Functions added in MySQL 8.0, but now not supported in TiDB
	// They will be deal with non-exists function, and throw error.git
	tk.MustGetErrCode("create table t (a int, b int as (updatexml(1, 1, 1)))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode("create table t (a int, b int as (statement_digest(1)))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode("create table t (a int, b int as (statement_digest_text(1)))", errno.ErrGeneratedColumnFunctionIsNotAllowed)
}

func (s *testIntegrationSuite3) TestParserIssue284(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table test.t_parser_issue_284(c1 int not null primary key)")
	_, err := tk.Exec("create table test.t_parser_issue_284_2(id int not null primary key, c1 int not null, constraint foreign key (c1) references t_parser_issue_284(c1))")
	c.Assert(err, IsNil)

	tk.MustExec("drop table test.t_parser_issue_284")
	tk.MustExec("drop table test.t_parser_issue_284_2")
}

func (s *testIntegrationSuite7) TestAddExpressionIndex(c *C) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowsExpressionIndex = true
	})
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")

	tk.MustGetErrCode("create table t(a int, b int, index((a+b)));", errno.ErrNotSupportedYet)

	tk.MustExec("create table t (a int, b real);")
	tk.MustExec("insert into t values (1, 2.1);")
	tk.MustExec("alter table t add index idx((a+b));")

	tblInfo, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	columns := tblInfo.Meta().Columns
	c.Assert(len(columns), Equals, 3)
	c.Assert(columns[2].Hidden, IsTrue)

	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2.1"))

	tk.MustExec("alter table t add index idx_multi((a+b),(a+1), b);")
	tblInfo, err = s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	columns = tblInfo.Meta().Columns
	c.Assert(len(columns), Equals, 5)
	c.Assert(columns[3].Hidden, IsTrue)
	c.Assert(columns[4].Hidden, IsTrue)

	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2.1"))

	tk.MustExec("alter table t drop index idx;")
	tblInfo, err = s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	columns = tblInfo.Meta().Columns
	c.Assert(len(columns), Equals, 4)

	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2.1"))

	tk.MustExec("alter table t drop index idx_multi;")
	tblInfo, err = s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	columns = tblInfo.Meta().Columns
	c.Assert(len(columns), Equals, 2)

	tk.MustQuery("select * from t;").Check(testkit.Rows("1 2.1"))

	// Issue #17111
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a varchar(10), b varchar(10));")
	tk.MustExec("alter table t1 add unique index ei_ab ((concat(a, b)));")
	tk.MustExec("alter table t1 alter index ei_ab invisible;")

	// Test experiment switch.
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowsExpressionIndex = false
	})
	tk.MustGetErrMsg("create index d on t((a+1))", "[ddl:8200]Unsupported creating expression index without allow-expression-index in config")
}

func (s *testIntegrationSuite7) TestCreateExpressionIndexError(c *C) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowsExpressionIndex = true
	})

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b real);")
	tk.MustGetErrCode("alter table t add primary key ((a+b));", errno.ErrFunctionalIndexPrimaryKey)

	// Test for error
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b real);")
	tk.MustGetErrCode("alter table t add primary key ((a+b));", errno.ErrFunctionalIndexPrimaryKey)
	tk.MustGetErrCode("alter table t add index ((rand()));", errno.ErrGeneratedColumnFunctionIsNotAllowed)
	tk.MustGetErrCode("alter table t add index ((now()+1));", errno.ErrGeneratedColumnFunctionIsNotAllowed)

	tk.MustExec("alter table t add column (_V$_idx_0 int);")
	tk.MustGetErrCode("alter table t add index idx((a+1));", errno.ErrDupFieldName)
	tk.MustExec("alter table t drop column _V$_idx_0;")
	tk.MustExec("alter table t add index idx((a+1));")
	tk.MustGetErrCode("alter table t add column (_V$_idx_0 int);", errno.ErrDupFieldName)
	tk.MustExec("alter table t drop index idx;")
	tk.MustExec("alter table t add column (_V$_idx_0 int);")

	tk.MustExec("alter table t add column (_V$_expression_index_0 int);")
	tk.MustGetErrCode("alter table t add index ((a+1));", errno.ErrDupFieldName)
	tk.MustExec("alter table t drop column _V$_expression_index_0;")
	tk.MustExec("alter table t add index ((a+1));")
	tk.MustGetErrCode("alter table t drop column _V$_expression_index_0;", errno.ErrCantDropFieldOrKey)
	tk.MustGetErrCode("alter table t add column e int as (_V$_expression_index_0 + 1);", errno.ErrBadField)
}

func (s *testIntegrationSuite7) TestAddExpressionIndexOnPartition(c *C) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowsExpressionIndex = true
	})
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t(
	a int,
	b varchar(100),
	c int)
	PARTITION BY RANGE ( a ) (
	PARTITION p0 VALUES LESS THAN (6),
		PARTITION p1 VALUES LESS THAN (11),
		PARTITION p2 VALUES LESS THAN (16),
		PARTITION p3 VALUES LESS THAN (21)
	);`)
	tk.MustExec("insert into t values (1, 'test', 2), (12, 'test', 3), (15, 'test', 10), (20, 'test', 20);")
	tk.MustExec("alter table t add index idx((a+c));")

	tblInfo, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	columns := tblInfo.Meta().Columns
	c.Assert(len(columns), Equals, 4)
	c.Assert(columns[3].Hidden, IsTrue)

	tk.MustQuery("select * from t order by a;").Check(testkit.Rows("1 test 2", "12 test 3", "15 test 10", "20 test 20"))
}

// TestCreateTableWithAutoIdCache test the auto_id_cache table option.
// `auto_id_cache` take effects on handle too when `PKIshandle` is false,
// or even there is no auto_increment column at all.
func (s *testIntegrationSuite3) TestCreateTableWithAutoIdCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")

	// Test primary key is handle.
	tk.MustExec("create table t(a int auto_increment key) auto_id_cache 100")
	tblInfo, err := s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().AutoIdCache, Equals, int64(100))
	tk.MustExec("insert into t values()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustExec("delete from t")

	// Invalid the allocator cache, insert will trigger a new cache
	tk.MustExec("rename table t to t1;")
	tk.MustExec("insert into t1 values()")
	tk.MustQuery("select * from t1").Check(testkit.Rows("101"))

	// Test primary key is not handle.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t(a int) auto_id_cache 100")
	_, err = s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	tk.MustExec("insert into t values()")
	tk.MustQuery("select _tidb_rowid from t").Check(testkit.Rows("1"))
	tk.MustExec("delete from t")

	// Invalid the allocator cache, insert will trigger a new cache
	tk.MustExec("rename table t to t1;")
	tk.MustExec("insert into t1 values()")
	tk.MustQuery("select _tidb_rowid from t1").Check(testkit.Rows("101"))

	// Test both auto_increment and rowid exist.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t(a int null, b int auto_increment unique) auto_id_cache 100")
	_, err = s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)

	tk.MustExec("insert into t(b) values(NULL)")
	tk.MustQuery("select b, _tidb_rowid from t").Check(testkit.Rows("1 2"))
	tk.MustExec("delete from t")

	// Invalid the allocator cache, insert will trigger a new cache.
	tk.MustExec("rename table t to t1;")
	tk.MustExec("insert into t1(b) values(NULL)")
	tk.MustQuery("select b, _tidb_rowid from t1").Check(testkit.Rows("101 102"))
	tk.MustExec("delete from t1")

	// Test alter auto_id_cache.
	tk.MustExec("alter table t1 auto_id_cache 200")
	tblInfo, err = s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t1"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().AutoIdCache, Equals, int64(200))

	tk.MustExec("insert into t1(b) values(NULL)")
	tk.MustQuery("select b, _tidb_rowid from t1").Check(testkit.Rows("201 202"))
	tk.MustExec("delete from t1")

	// Invalid the allocator cache, insert will trigger a new cache.
	tk.MustExec("rename table t1 to t;")
	tk.MustExec("insert into t(b) values(NULL)")
	tk.MustQuery("select b, _tidb_rowid from t").Check(testkit.Rows("401 402"))
	tk.MustExec("delete from t")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t(a int auto_increment key) auto_id_cache 3")
	tblInfo, err = s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	c.Assert(err, IsNil)
	c.Assert(tblInfo.Meta().AutoIdCache, Equals, int64(3))

	// Test insert batch size(4 here) greater than the customized autoid step(3 here).
	tk.MustExec("insert into t(a) values(NULL),(NULL),(NULL),(NULL)")
	tk.MustQuery("select a from t").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustExec("delete from t")

	// Invalid the allocator cache, insert will trigger a new cache.
	tk.MustExec("rename table t to t1;")
	tk.MustExec("insert into t1(a) values(NULL)")
	next := tk.MustQuery("select a from t1").Rows()[0][0].(string)
	nextInt, err := strconv.Atoi(next)
	c.Assert(err, IsNil)
	c.Assert(nextInt, Greater, 5)

	// Test auto_id_cache overflows int64.
	tk.MustExec("drop table if exists t;")
	_, err = tk.Exec("create table t(a int) auto_id_cache = 9223372036854775808")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "table option auto_id_cache overflows int64")

	tk.MustExec("create table t(a int) auto_id_cache = 9223372036854775807")
	_, err = tk.Exec("alter table t auto_id_cache = 9223372036854775808")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "table option auto_id_cache overflows int64")
}

func (s *testIntegrationSuite4) TestAlterIndexVisibility(c *C) {
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Experimental.AllowsExpressionIndex = true
	})
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists alter_index_test")
	tk.MustExec("USE alter_index_test;")
	tk.MustExec("drop table if exists t, t1, t2, t3;")

	tk.MustExec("create table t(a int NOT NULL, b int, key(a), unique(b) invisible)")
	query := queryIndexOnTable("alter_index_test", "t")
	tk.MustQuery(query).Check(testkit.Rows("a YES", "b NO"))

	tk.MustExec("alter table t alter index a invisible")
	tk.MustQuery(query).Check(testkit.Rows("a NO", "b NO"))

	tk.MustExec("alter table t alter index b visible")
	tk.MustQuery(query).Check(testkit.Rows("a NO", "b YES"))

	tk.MustExec("alter table t alter index b invisible")
	tk.MustQuery(query).Check(testkit.Rows("a NO", "b NO"))

	tk.MustGetErrMsg("alter table t alter index non_exists_idx visible", "[schema:1176]Key 'non_exists_idx' doesn't exist in table 't'")

	// Alter implicit primary key to invisible index should throw error
	tk.MustExec("create table t1(a int NOT NULL, unique(a))")
	tk.MustGetErrMsg("alter table t1 alter index a invisible", "[ddl:3522]A primary key index cannot be invisible")

	// Alter explicit primary key to invisible index should throw error
	tk.MustExec("create table t2(a int, primary key(a))")
	tk.MustGetErrMsg("alter table t2 alter index PRIMARY invisible", `[parser:1064]You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 34 near "PRIMARY invisible" `)

	// Alter expression index
	tk.MustExec("create table t3(a int NOT NULL, b int)")
	tk.MustExec("alter table t3 add index idx((a+b));")
	query = queryIndexOnTable("alter_index_test", "t3")
	tk.MustQuery(query).Check(testkit.Rows("idx YES"))

	tk.MustExec("alter table t3 alter index idx invisible")
	tk.MustQuery(query).Check(testkit.Rows("idx NO"))
}

func queryIndexOnTable(dbName, tableName string) string {
	return fmt.Sprintf("select distinct index_name, is_visible from information_schema.statistics where table_schema = '%s' and table_name = '%s' order by index_name", dbName, tableName)
}

func (s *testIntegrationSuite5) TestDropColumnWithCompositeIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	query := queryIndexOnTable("drop_composite_index_test", "t_drop_column_with_comp_idx")
	tk.MustExec("create database if not exists drop_composite_index_test")
	tk.MustExec("use drop_composite_index_test")
	tk.MustExec("create table t_drop_column_with_comp_idx(a int, b int, c int)")
	defer tk.MustExec("drop table if exists t_drop_column_with_comp_idx")
	tk.MustExec("create index idx_bc on t_drop_column_with_comp_idx(b, c)")
	tk.MustExec("create index idx_b on t_drop_column_with_comp_idx(b)")
	tk.MustGetErrMsg("alter table t_drop_column_with_comp_idx drop column b", "[ddl:8200]can't drop column b with composite index covered or Primary Key covered now")
	tk.MustQuery(query).Check(testkit.Rows("idx_b YES", "idx_bc YES"))
	tk.MustExec("alter table t_drop_column_with_comp_idx alter index idx_bc invisible")
	tk.MustExec("alter table t_drop_column_with_comp_idx alter index idx_b invisible")
	tk.MustGetErrMsg("alter table t_drop_column_with_comp_idx drop column b", "[ddl:8200]can't drop column b with composite index covered or Primary Key covered now")
	tk.MustQuery(query).Check(testkit.Rows("idx_b NO", "idx_bc NO"))
}

func (s *testIntegrationSuite5) TestDropColumnWithIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("create table t_drop_column_with_idx(a int, b int, c int)")
	defer tk.MustExec("drop table if exists t_drop_column_with_idx")
	tk.MustExec("create index idx on t_drop_column_with_idx(b)")
	tk.MustExec("alter table t_drop_column_with_idx drop column b")
	query := queryIndexOnTable("test_db", "t_drop_column_with_idx")
	tk.MustQuery(query).Check(testkit.Rows())
}

func (s *testIntegrationSuite5) TestDropColumnWithMultiIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("create table t_drop_column_with_idx(a int, b int, c int)")
	defer tk.MustExec("drop table if exists t_drop_column_with_idx")
	tk.MustExec("create index idx_1 on t_drop_column_with_idx(b)")
	tk.MustExec("create index idx_2 on t_drop_column_with_idx(b)")
	tk.MustExec("alter table t_drop_column_with_idx drop column b")
	query := queryIndexOnTable("test_db", "t_drop_column_with_idx")
	tk.MustQuery(query).Check(testkit.Rows())
}

func (s *testIntegrationSuite5) TestDropColumnsWithMultiIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test_db")
	tk.MustExec("create table t_drop_columns_with_idx(a int, b int, c int)")
	defer tk.MustExec("drop table if exists t_drop_columns_with_idx")
	tk.MustExec("create index idx_1 on t_drop_columns_with_idx(b)")
	tk.MustExec("create index idx_2 on t_drop_columns_with_idx(b)")
	tk.MustExec("create index idx_3 on t_drop_columns_with_idx(c)")
	tk.MustExec("alter table t_drop_columns_with_idx drop column b, drop column c")
	query := queryIndexOnTable("test_db", "t_drop_columns_with_idx")
	tk.MustQuery(query).Check(testkit.Rows())
}

func (s *testIntegrationSuite7) TestAutoIncrementAllocator(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.AlterPrimaryKey = false
	})
	tk.MustExec("drop database if exists test_create_table_option_auto_inc;")
	tk.MustExec("create database test_create_table_option_auto_inc;")
	tk.MustExec("use test_create_table_option_auto_inc;")

	tk.MustExec("create table t (a bigint primary key) auto_increment = 10;")
	tk.MustExec("alter table t auto_increment = 10;")
}
