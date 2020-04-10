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

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
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

// TestKeyWithoutLength for issue #13452
func (s testIntegrationSuite) TestKeyWithoutLengthCreateTable(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test")

	_, err := tk.Exec("create table t_without_length (a text primary key)")
	c.Assert(err, NotNil)
	c.Assert(err, ErrorMatches, ".*BLOB/TEXT column 'a' used in key specification without a key length")
}

// TestInvalidNameWhenCreateTable for issue #3848
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

// TestCreateTableIfNotExists for issue #6879
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

func (s *testIntegrationSuite) TestCreateTableWithKeyWord(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("USE test;")

	_, err := tk.Exec("create table t1(pump varchar(20), drainer varchar(20), node_id varchar(20), node_state varchar(20));")
	c.Assert(err, IsNil)
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

func (s *testIntegrationSuite) TestIndexLength(c *C) {
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

func (s *testIntegrationSuite) TestDependedGeneratedColumnPrior2GeneratedColumn(c *C) {
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
	assertErrorCode(c, tk, sql, mysql.ErrBadField)

	// depended generated column should be prior to generated column self
	sql = "alter table t add column d int as (c+1) first"
	assertErrorCode(c, tk, sql, mysql.ErrGeneratedColumnNonPrior)

	// correct case
	tk.MustExec("alter table t add column d int as (c+1) after c")

	// check position nil case
	tk.MustExec("alter table t add column(e int as (c+1))")
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
	c.Assert(err.Error(), Equals, "[parser:1115]Unknown character set: 'gbk'")
	rs, err = tk.Exec("alter table t charset utf8")
	if rs != nil {
		rs.Close()
	}
	c.Assert(err.Error(), Equals, "[ddl:210]unsupported modify charset from latin1 to utf8")

	rs, err = tk.Exec("alter table t charset utf8 collate latin1_bin")
	if rs != nil {
		rs.Close()
	}
	c.Assert(err, NotNil)

	rs, err = tk.Exec("alter table t charset utf8mb4")
	if rs != nil {
		rs.Close()
	}
	c.Assert(err.Error(), Equals, "[ddl:210]unsupported modify charset from latin1 to utf8mb4")

	rs, err = tk.Exec("alter table t charset utf8mb4 collate utf8mb4_bin")
	c.Assert(err, NotNil)

	rs, err = tk.Exec("alter table t charset ''")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[parser:1115]Unknown character set: ''")

	rs, err = tk.Exec("alter table t collate ''")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1273]Unknown collation: ''")

	rs, err = tk.Exec("alter table t charset utf8mb4 collate '' collate utf8mb4_bin;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1273]Unknown collation: ''")

	rs, err = tk.Exec("alter table t charset latin1 charset utf8 charset utf8mb4 collate utf8_bin;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1302]Conflicting declarations: 'CHARACTER SET latin1' and 'CHARACTER SET utf8'")

	rs, err = tk.Exec("alter table t charset utf8 collate utf8mb4_bin;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1253]COLLATION 'utf8mb4_bin' is not valid for CHARACTER SET 'utf8'")

	rs, err = tk.Exec("alter table t charset utf8 collate utf8_bin collate utf8mb4_bin collate utf8_bin;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:1253]COLLATION 'utf8mb4_bin' is not valid for CHARACTER SET 'utf8'")

	// Test change column charset when changing table charset.
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a varchar(10)) charset utf8")
	tk.MustExec("alter table t convert to charset utf8mb4;")
	checkCharset := func(chs, coll string) {
		tbl := testGetTableByName(c, tk.Se, "test", "t")
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
	_, err = tk.Exec("alter table t convert to charset utf8mb4;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[ddl:210]unsupported modify charset from ascii to utf8mb4")

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
	db, ok := domain.GetDomain(tk.Se).InfoSchema().SchemaByName(model.NewCIStr("test"))
	c.Assert(ok, IsTrue)
	tbl := testGetTableByName(c, tk.Se, "test", "t")
	tblInfo := tbl.Meta().Clone()
	tblInfo.Charset = ""
	tblInfo.Collate = ""
	updateTableInfo := func(tblInfo *model.TableInfo) {
		mockCtx := mock.NewContext()
		mockCtx.Store = s.store
		err = mockCtx.NewTxn()
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
	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl, NotNil)
	c.Assert(tbl.Meta().Charset, Equals, "")
	c.Assert(tbl.Meta().Collate, Equals, "")
	// Test when table charset is "", this for compatibility.
	tk.MustExec("alter table t convert to charset utf8mb4;")
	checkCharset(charset.CharsetUTF8MB4, charset.CollationUTF8MB4)

	// Test when column charset is "".
	tbl = testGetTableByName(c, tk.Se, "test", "t")
	tblInfo = tbl.Meta().Clone()
	tblInfo.Columns[0].Charset = ""
	tblInfo.Columns[0].Collate = ""
	updateTableInfo(tblInfo)
	// check table charset is ""
	tk.MustExec("alter table t drop column b;") //  load latest schema.
	tbl = testGetTableByName(c, tk.Se, "test", "t")
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

}

func (s *testIntegrationSuite) TestModifyingColumnOption(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	errMsg := "[ddl:203]" // unsupported modify column with references
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
	c.Assert(err.Error(), Equals, "[ddl:203]unsupported modify column type varchar(16) not match origin int(11)")
	_, err = tk.Exec("alter table t1 change a a varchar(10)")
	c.Assert(err.Error(), Equals, "[ddl:203]unsupported modify column type varchar(10) not match origin int(11)")
	_, err = tk.Exec("alter table t1 change a a datetime")
	c.Assert(err.Error(), Equals, "[ddl:203]unsupported modify column type datetime not match origin int(11)")
	_, err = tk.Exec("alter table t1 change a a int(11) unsigned")
	c.Assert(err.Error(), Equals, "[ddl:203]unsupported modify column can't change unsigned integer to signed or vice versa")
	_, err = tk.Exec("alter table t2 change b b int(11) unsigned")
	c.Assert(err.Error(), Equals, "[ddl:203]unsupported modify column type int(11) not match origin char(1)")
}

func (s *testIntegrationSuite) TestIndexOnMultipleGeneratedColumn(c *C) {
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

	tk.MustExec("create table t5(a varchar(20)) ENGINE=InnoDB DEFAULT CHARSET=UTF8MB4 COLLATE=UTF8MB4_GENERAL_CI;")
	tk.MustExec("insert into t5 values ('特克斯和凯科斯群岛')")

	db, ok := domain.GetDomain(tk.Se).InfoSchema().SchemaByName(model.NewCIStr("test_charset_collate"))
	c.Assert(ok, IsTrue)
	tbl := testGetTableByName(c, tk.Se, "test_charset_collate", "t5")
	tblInfo := tbl.Meta().Clone()
	c.Assert(tblInfo.Charset, Equals, "utf8mb4")
	c.Assert(tblInfo.Columns[0].Charset, Equals, "utf8mb4")

	tblInfo.Version = model.TableInfoVersion2
	tblInfo.Charset = "UTF8MB4"

	updateTableInfo := func(tblInfo *model.TableInfo) {
		mockCtx := mock.NewContext()
		mockCtx.Store = s.store
		err := mockCtx.NewTxn()
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

	tblInfo = testGetTableByName(c, tk.Se, "test_charset_collate", "t5").Meta()
	c.Assert(tblInfo.Charset, Equals, "utf8mb4")
	c.Assert(tblInfo.Columns[0].Charset, Equals, "utf8mb4")

	// For model.TableInfoVersion3, it is believed that all charsets / collations are lower-cased, do not do case-convert
	tblInfo = tblInfo.Clone()
	tblInfo.Version = model.TableInfoVersion3
	tblInfo.Charset = "UTF8MB4"
	updateTableInfo(tblInfo)
	tk.MustExec("alter table t5 add column c varchar(10);") //  load latest schema.

	tblInfo = testGetTableByName(c, tk.Se, "test_charset_collate", "t5").Meta()
	c.Assert(tblInfo.Charset, Equals, "UTF8MB4")
	c.Assert(tblInfo.Columns[0].Charset, Equals, "utf8mb4")
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

func (s *testIntegrationSuite) TestIgnoreColumnUTF8Charset(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (a varchar(10) character set utf8, b varchar(10) character set ascii) charset=utf8mb4;")
	_, err := tk.Exec("insert into t set a= x'f09f8c80'")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Mock old version table info with column charset is utf8.
	db, ok := domain.GetDomain(tk.Se).InfoSchema().SchemaByName(model.NewCIStr("test"))
	tbl := testGetTableByName(c, tk.Se, "test", "t")
	tblInfo := tbl.Meta().Clone()
	tblInfo.Version = model.TableInfoVersion0
	tblInfo.Columns[0].Version = model.ColumnInfoVersion0
	updateTableInfo := func(tblInfo *model.TableInfo) {
		mockCtx := mock.NewContext()
		mockCtx.Store = s.store
		err := mockCtx.NewTxn()
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

	config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 = false
	tk.MustExec("alter table t drop column c;") //  reload schema.
	_, err = tk.Exec("insert into t set a= x'f09f8c80'")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	// Mock old version table info with table and column charset is utf8.
	tbl = testGetTableByName(c, tk.Se, "test", "t")
	tblInfo = tbl.Meta().Clone()
	tblInfo.Charset = charset.CharsetUTF8
	tblInfo.Collate = charset.CollationUTF8
	tblInfo.Version = model.TableInfoVersion0
	tblInfo.Columns[0].Version = model.ColumnInfoVersion0
	updateTableInfo(tblInfo)

	config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 = true
	tk.MustExec("alter table t add column c varchar(10);") //  load latest schema.
	tk.MustExec("insert into t set a= x'f09f8c80'")
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL,\n" +
		"  `c` varchar(10) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))

	config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 = false
	tk.MustExec("alter table t drop column c;") //  reload schema.
	_, err = tk.Exec("insert into t set a= x'f09f8c80'")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(10) DEFAULT NULL,\n" +
		"  `b` varchar(10) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"))

	// Test modify column charset.
	config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 = true
	tk.MustExec("alter table t modify column a varchar(10) character set utf8mb4") //  change column charset.
	tbl = testGetTableByName(c, tk.Se, "test", "t")
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
	tbl = testGetTableByName(c, tk.Se, "test", "t")
	c.Assert(tbl.Meta().Columns[0].Charset, Equals, charset.CharsetUTF8MB4)
	c.Assert(tbl.Meta().Columns[0].Collate, Equals, charset.CollationUTF8MB4)
	c.Assert(tbl.Meta().Columns[0].Version, Equals, model.ColumnInfoVersion0)

	// Test for v2.1.5 and v2.1.6 that table version is 1 but column version is 0.
	tbl = testGetTableByName(c, tk.Se, "test", "t")
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

	config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 = false
	tk.MustExec("alter table t change column b b varchar(30) character set ascii") // reload schema.
	_, err = tk.Exec("insert into t set a= x'f09f8c80'")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, table.ErrTruncatedWrongValueForField), IsTrue, Commentf("err %v", err))
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(20) DEFAULT NULL,\n" +
		"  `b` varchar(30) CHARACTER SET ascii COLLATE ascii_bin DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"))

	// Test for alter table convert charset
	config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 = true
	tk.MustExec("alter table t drop column b") // reload schema.
	tk.MustExec("alter table t convert to charset utf8mb4;")

	config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 = false
	tk.MustExec("alter table t add column b varchar(50);") // reload schema.
	// TODO: fix  this after PR 9790.
	tk.MustQuery("show create table t").Check(testkit.Rows("t CREATE TABLE `t` (\n" +
		"  `a` varchar(20) DEFAULT NULL,\n" +
		"  `b` varchar(50) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"))
}

func (s *testIntegrationSuite) TestChangingDBCharset(c *C) {
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
		_, err := tk.Exec(fc.stmt)
		c.Assert(err.Error(), Equals, fc.errMsg, Commentf("%v", fc.stmt))
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

		dom := domain.GetDomain(tk.Se.(sessionctx.Context))
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
		_, err := tk.Exec(fc.stmt)
		c.Assert(err.Error(), Equals, fc.errMsg, Commentf("%v", fc.stmt))
	}
	tk.MustExec("ALTER SCHEMA CHARACTER SET = 'utf8' COLLATE = 'utf8_unicode_ci'")
	verifyDBCharsetAndCollate("alterdb2", "utf8", "utf8_unicode_ci")

	tk.MustExec("ALTER SCHEMA CHARACTER SET = 'utf8mb4'")
	verifyDBCharsetAndCollate("alterdb2", "utf8mb4", "utf8mb4_bin")

	tk.MustExec("ALTER SCHEMA CHARACTER SET = 'utf8mb4' COLLATE = 'utf8mb4_general_ci'")
	verifyDBCharsetAndCollate("alterdb2", "utf8mb4", "utf8mb4_general_ci")
}

func (s *testIntegrationSuite) TestDropAutoIncrement(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	assertErrMsg := func(sql string) {
		_, err := tk.Exec(sql)
		c.Assert(err, NotNil)
		errMsg := "[autoid:1075]Incorrect table definition; there can be only one auto column and it must be defined as a key"
		c.Assert(err.Error(), Equals, errMsg)
	}

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int auto_increment, unique key (a))")
	dropIndexSQL := "alter table t1 drop index a"
	assertErrMsg(dropIndexSQL)

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int(11) not null auto_increment, b int(11), c bigint, unique key (a, b, c))")
	dropIndexSQL = "alter table t1 drop index a"
	assertErrMsg(dropIndexSQL)

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int auto_increment key)")
	_, err := tk.Exec("alter table t modify column a int")
	c.Assert(err, NotNil)
	tk.MustExec("set @@tidb_allow_remove_auto_inc = on")
	tk.MustExec("alter table t1 modify column a int")
	tk.MustExec("set @@tidb_allow_remove_auto_inc = off")
}

func (s *testIntegrationSuite) TestInvalidDefaultOrOnUpdateClause(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	sql := "create table t2 (a datetime(2) default current_timestamp(3))"
	assertErrorCode(c, tk, sql, mysql.ErrInvalidDefault)
	sql = "create table t2 (a datetime(2) default current_timestamp(2) on update current_timestamp)"
	assertErrorCode(c, tk, sql, mysql.ErrInvalidOnUpdate)
	sql = "create table t2 (a datetime default current_timestamp on update current_timestamp(2))"
	assertErrorCode(c, tk, sql, mysql.ErrInvalidOnUpdate)
	sql = "create table t2 (a datetime(2) default current_timestamp(2) on update current_timestamp(3))"
	assertErrorCode(c, tk, sql, mysql.ErrInvalidOnUpdate)
}

func (s *testIntegrationSuite) TestBitDefaultValue(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	tk.MustExec("create table t_bit (a int)")
	tk.MustExec("insert into t_bit value (1)")
	tk.MustExec("alter table t_bit add column c bit(16) null default b'1100110111001'")
	tk.MustQuery("select c from t_bit").Check(testkit.Rows("\x19\xb9"))
	tk.MustExec("update t_bit set c = b'11100000000111'")
	tk.MustQuery("select c from t_bit").Check(testkit.Rows("\x38\x07"))
}

func (s *testIntegrationSuite) TestMultipleUnique(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database if not exists test")
	tk.MustExec("use test")

	tk.MustExec("drop table if exists multi_unique")
	tk.MustExec("create table multi_unique (a int unique unique)")
	tk.MustExec("drop table multi_unique")
	tk.MustExec("create table multi_unique (a int key primary key unique unique)")
	tk.MustExec("drop table multi_unique")
	tk.MustExec("create table multi_unique (a int key unique unique key unique)")
	tk.MustExec("drop table multi_unique")
}

func (s *testIntegrationSuite) TestParserIssue284(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table test.t_parser_issue_284(c1 int not null primary key)")
	_, err := tk.Exec("create table test.t_parser_issue_284_2(id int not null primary key, c1 int not null, constraint foreign key (c1) references t_parser_issue_284(c1))")
	c.Assert(err, IsNil)

	tk.MustExec("drop table test.t_parser_issue_284")
	tk.MustExec("drop table test.t_parser_issue_284_2")
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
	tblInfo, err = s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
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
	tblInfo, err = s.dom.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
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
