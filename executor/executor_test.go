// Copyright 2015 PingCAP, Inc.
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

package executor_test

import (
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	pb "github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta/autoid"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/mocktikv"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/admin"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	goctx "golang.org/x/net/context"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	logLevel := os.Getenv("log_level")
	logutil.InitLogger(&logutil.LogConfig{
		Level: logLevel,
	})
	TestingT(t)
}

var _ = Suite(&testSuite{})
var _ = Suite(&testContextOptionSuite{})

type testSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore *mocktikv.MvccStore
	store     kv.Storage
	*parser.Parser

	autoIDStep int64
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in executor test")

func (s *testSuite) SetUpSuite(c *C) {
	s.autoIDStep = autoid.GetStep()
	autoid.SetStep(5000)
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		s.cluster = mocktikv.NewCluster()
		mocktikv.BootstrapWithSingleStore(s.cluster)
		s.mvccStore = mocktikv.NewMvccStore()
		store, err := tikv.NewMockTikvStore(
			tikv.WithCluster(s.cluster),
			tikv.WithMVCCStore(s.mvccStore),
		)
		c.Assert(err, IsNil)
		s.store = store
		tidb.SetSchemaLease(0)
		tidb.SetStatsLease(0)
	} else {
		store, err := tidb.NewStore("memory://test/test")
		c.Assert(err, IsNil)
		s.store = store
	}
	_, err := tidb.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testSuite) TearDownSuite(c *C) {
	s.store.Close()
	autoid.SetStep(s.autoIDStep)
}

func (s *testSuite) SetUpTest(c *C) {
	testleak.BeforeTest()
}

func (s *testSuite) TearDownTest(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
	testleak.AfterTest(c)()
}

func (s *testSuite) TestAdmin(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("insert admin_test (c1) values (1),(2),(NULL)")

	goCtx := goctx.Background()
	// cancel DDL jobs test
	r, err := tk.Exec("admin cancel ddl jobs 1")
	c.Assert(err, IsNil, Commentf("err %v", err))
	row, err := r.Next(goCtx)
	c.Assert(err, IsNil)
	c.Assert(row.Len(), Equals, 2)
	c.Assert(row.GetInt64(0), Equals, int64(1))
	c.Assert(row.GetString(1), Equals, "error: Can't find this job")

	r, err = tk.Exec("admin show ddl")
	c.Assert(err, IsNil)
	row, err = r.Next(goCtx)
	c.Assert(err, IsNil)
	c.Assert(row.Len(), Equals, 4)
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	ddlInfo, err := admin.GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(row.GetInt64(0), Equals, ddlInfo.SchemaVer)
	// TODO: Pass this test.
	// rowOwnerInfos := strings.Split(row.Data[1].GetString(), ",")
	// ownerInfos := strings.Split(ddlInfo.Owner.String(), ",")
	// c.Assert(rowOwnerInfos[0], Equals, ownerInfos[0])
	c.Assert(row.GetString(2), Equals, "")
	row, err = r.Next(goCtx)
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)
	err = txn.Rollback()
	c.Assert(err, IsNil)

	// show DDL jobs test
	r, err = tk.Exec("admin show ddl jobs")
	c.Assert(err, IsNil)
	row, err = r.Next(goCtx)
	c.Assert(err, IsNil)
	c.Assert(row.Len(), Equals, 2)
	txn, err = s.store.Begin()
	c.Assert(err, IsNil)
	historyJobs, err := admin.GetHistoryDDLJobs(txn)
	c.Assert(len(historyJobs), Greater, 1)
	c.Assert(len(row.GetString(0)), Greater, 0)
	c.Assert(err, IsNil)
	c.Assert(row.GetString(1), Equals, historyJobs[0].State.String())
	c.Assert(err, IsNil)

	// check table test
	tk.MustExec("create table admin_test1 (c1 int, c2 int default 1, index (c1))")
	tk.MustExec("insert admin_test1 (c1) values (21),(22)")
	r, err = tk.Exec("admin check table admin_test, admin_test1")
	c.Assert(err, IsNil)
	c.Assert(r, IsNil)
	// error table name
	r, err = tk.Exec("admin check table admin_test_error")
	c.Assert(err, NotNil)
	// different index values
	ctx := tk.Se.(context.Context)
	dom := domain.GetDomain(ctx)
	is := dom.InfoSchema()
	c.Assert(is, NotNil)
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("admin_test"))
	c.Assert(err, IsNil)
	c.Assert(tb.Indices(), HasLen, 1)
	_, err = tb.Indices()[0].Create(mock.NewContext(), txn, types.MakeDatums(int64(10)), 1)
	c.Assert(err, IsNil)
	err = txn.Commit(goctx.Background())
	c.Assert(err, IsNil)
	r, err = tk.Exec("admin check table admin_test")
	c.Assert(err, NotNil)
}

func (s *testSuite) fillData(tk *testkit.TestKit, table string) {
	tk.MustExec("use test")
	tk.MustExec(fmt.Sprintf("create table %s(id int not null default 1, name varchar(255), PRIMARY KEY(id));", table))

	// insert data
	tk.MustExec(fmt.Sprintf("insert INTO %s VALUES (1, \"hello\");", table))
	tk.CheckExecResult(1, 0)
	tk.MustExec(fmt.Sprintf("insert into %s values (2, \"hello\");", table))
	tk.CheckExecResult(1, 0)
}

type testCase struct {
	data1    []byte
	data2    []byte
	expected []string
	restData []byte
}

func checkCases(tests []testCase, ld *executor.LoadDataInfo,
	c *C, tk *testkit.TestKit, ctx context.Context, selectSQL, deleteSQL string) {
	for _, tt := range tests {
		c.Assert(ctx.NewTxn(), IsNil)
		data, reachLimit, err1 := ld.InsertData(tt.data1, tt.data2)
		c.Assert(err1, IsNil)
		c.Assert(reachLimit, IsFalse)
		if tt.restData == nil {
			c.Assert(data, HasLen, 0,
				Commentf("data1:%v, data2:%v, data:%v", string(tt.data1), string(tt.data2), string(data)))
		} else {
			c.Assert(data, DeepEquals, tt.restData,
				Commentf("data1:%v, data2:%v, data:%v", string(tt.data1), string(tt.data2), string(data)))
		}
		terror.Log(ctx.StmtCommit())
		err1 = ctx.Txn().Commit(goctx.Background())
		c.Assert(err1, IsNil)
		r := tk.MustQuery(selectSQL)
		r.Check(testutil.RowsWithSep("|", tt.expected...))
		tk.MustExec(deleteSQL)
	}
}

func (s *testSuite) TestSelectWithoutFrom(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	r := tk.MustQuery("select 1 + 2*3")
	r.Check(testkit.Rows("7"))

	r = tk.MustQuery(`select _utf8"string";`)
	r.Check(testkit.Rows("string"))
}

// TestSelectBackslashN Issue 3685.
func (s *testSuite) TestSelectBackslashN(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	sql := `select \N;`
	r := tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err := tk.Exec(sql)
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "NULL")

	sql = `select "\N";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("N"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `N`)

	tk.MustExec("use test;")
	tk.MustExec("create table test (`\\N` int);")
	tk.MustExec("insert into test values (1);")
	tk.CheckExecResult(1, 0)
	sql = "select * from test;"
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("1"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `\N`)

	sql = `select \N from test;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(err, IsNil)
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `NULL`)

	sql = `select (\N) from test;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `NULL`)

	sql = "select `\\N` from test;"
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("1"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `\N`)

	sql = "select (`\\N`) from test;"
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("1"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `\N`)

	sql = `select '\N' from test;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("N"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `N`)

	sql = `select ('\N') from test;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("N"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `N`)
}

// TestSelectNull Issue #4053.
func (s *testSuite) TestSelectNull(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	sql := `select nUll;`
	r := tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err := tk.Exec(sql)
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `NULL`)

	sql = `select (null);`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `NULL`)

	sql = `select null+NULL;`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(err, IsNil)
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `null+NULL`)
}

// TestSelectStringLiteral Issue #3686.
func (s *testSuite) TestSelectStringLiteral(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	sql := `select 'abc';`
	r := tk.MustQuery(sql)
	r.Check(testkit.Rows("abc"))
	rs, err := tk.Exec(sql)
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `abc`)

	sql = `select (('abc'));`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("abc"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `abc`)

	sql = `select 'abc'+'def';`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("0"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, `'abc'+'def'`)

	// Below checks whether leading invalid chars are trimmed.
	sql = "select '\n';"
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("\n"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "")

	sql = "select '\t   col';" // Lowercased letter is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "col")

	sql = "select '\t   Col';" // Uppercased letter is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "Col")

	sql = "select '\n\t   中文 col';" // Chinese char is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "中文 col")

	sql = "select ' \r\n  .col';" // Punctuation is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, ".col")

	sql = "select '   😆col';" // Emoji is a valid char.
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "😆col")

	// Below checks whether trailing invalid chars are preserved.
	sql = `select 'abc   ';`
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "abc   ")

	sql = `select '  abc   123   ';`
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "abc   123   ")

	// Issue #4239.
	sql = `select 'a' ' ' 'string';`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("a string"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "a")

	sql = `select 'a' " " "string";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("a string"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "a")

	sql = `select 'string' 'string';`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("stringstring"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "string")

	sql = `select "ss" "a";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("ssa"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "ss")

	sql = `select "ss" "a" "b";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("ssab"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "ss")

	sql = `select "ss" "a" ' ' "b";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("ssa b"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "ss")

	sql = `select "ss" "a" ' ' "b" ' ' "d";`
	r = tk.MustQuery(sql)
	r.Check(testkit.Rows("ssa b d"))
	rs, err = tk.Exec(sql)
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.O, Equals, "ss")
}

func (s *testSuite) TestSelectLimit(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "select_limit")

	tk.MustExec("insert INTO select_limit VALUES (3, \"hello\");")
	tk.CheckExecResult(1, 0)
	tk.MustExec("insert INTO select_limit VALUES (4, \"hello\");")
	tk.CheckExecResult(1, 0)

	r := tk.MustQuery("select * from select_limit limit 1;")
	r.Check(testkit.Rows("1 hello"))

	r = tk.MustQuery("select id from (select * from select_limit limit 1) k where id != 1;")
	r.Check(testkit.Rows())

	r = tk.MustQuery("select * from select_limit limit 18446744073709551615 offset 0;")
	r.Check(testkit.Rows("1 hello", "2 hello", "3 hello", "4 hello"))

	r = tk.MustQuery("select * from select_limit limit 18446744073709551615 offset 1;")
	r.Check(testkit.Rows("2 hello", "3 hello", "4 hello"))

	r = tk.MustQuery("select * from select_limit limit 18446744073709551615 offset 3;")
	r.Check(testkit.Rows("4 hello"))

	_, err := tk.Exec("select * from select_limit limit 18446744073709551616 offset 3;")
	c.Assert(err, NotNil)
}

func (s *testSuite) TestSelectOrderBy(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	s.fillData(tk, "select_order_test")

	// Test star field
	r := tk.MustQuery("select * from select_order_test where id = 1 order by id limit 1 offset 0;")
	r.Check(testkit.Rows("1 hello"))

	r = tk.MustQuery("select id from select_order_test order by id desc limit 1 ")
	r.Check(testkit.Rows("2"))

	r = tk.MustQuery("select id from select_order_test order by id + 1 desc limit 1 ")
	r.Check(testkit.Rows("2"))

	// Test limit
	r = tk.MustQuery("select * from select_order_test order by name, id limit 1 offset 0;")
	r.Check(testkit.Rows("1 hello"))

	// Test limit
	r = tk.MustQuery("select id as c1, name from select_order_test order by 2, id limit 1 offset 0;")
	r.Check(testkit.Rows("1 hello"))

	// Test limit overflow
	r = tk.MustQuery("select * from select_order_test order by name, id limit 100 offset 0;")
	r.Check(testkit.Rows("1 hello", "2 hello"))

	// Test offset overflow
	r = tk.MustQuery("select * from select_order_test order by name, id limit 1 offset 100;")
	r.Check(testkit.Rows())

	// Test multiple field
	r = tk.MustQuery("select id, name from select_order_test where id = 1 group by id, name limit 1 offset 0;")
	r.Check(testkit.Rows("1 hello"))

	// Test limit + order by
	for i := 3; i <= 10; i += 1 {
		tk.MustExec(fmt.Sprintf("insert INTO select_order_test VALUES (%d, \"zz\");", i))
	}
	tk.MustExec("insert INTO select_order_test VALUES (10086, \"hi\");")
	for i := 11; i <= 20; i += 1 {
		tk.MustExec(fmt.Sprintf("insert INTO select_order_test VALUES (%d, \"hh\");", i))
	}
	for i := 21; i <= 30; i += 1 {
		tk.MustExec(fmt.Sprintf("insert INTO select_order_test VALUES (%d, \"zz\");", i))
	}
	tk.MustExec("insert INTO select_order_test VALUES (1501, \"aa\");")
	r = tk.MustQuery("select * from select_order_test order by name, id limit 1 offset 3;")
	r.Check(testkit.Rows("11 hh"))
	tk.MustExec("drop table select_order_test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (1, 2)")
	tk.MustExec("insert t values (1, 3)")
	r = tk.MustQuery("select 1-d as d from t order by d;")
	r.Check(testkit.Rows("-2", "-1", "0"))
	r = tk.MustQuery("select 1-d as d from t order by d + 1;")
	r.Check(testkit.Rows("0", "-1", "-2"))
	r = tk.MustQuery("select t.d from t order by d;")
	r.Check(testkit.Rows("1", "2", "3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert t values (1, 2, 3)")
	r = tk.MustQuery("select b from (select a,b from t order by a,c) t")
	r.Check(testkit.Rows("2"))
	r = tk.MustQuery("select b from (select a,b from t order by a,c limit 1) t")
	r.Check(testkit.Rows("2"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("insert into t values(1, 1), (2, 2)")
	tk.MustQuery("select * from t where 1 order by b").Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("select * from t where a between 1 and 2 order by a desc").Check(testkit.Rows("2 2", "1 1"))

	// Test double read and topN is pushed down to first read plan.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, index idx(b))")
	tk.MustExec("insert into t values(1, 3, 1)")
	tk.MustExec("insert into t values(2, 2, 2)")
	tk.MustExec("insert into t values(3, 1, 3)")
	tk.MustQuery("select * from t use index(idx) order by a desc limit 1").Check(testkit.Rows("3 1 3"))

	// Test double read which needs to keep order.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, key b (b))")
	tk.Se.GetSessionVars().IndexLookupSize = 3
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values(%d, %d)", i, 10-i))
	}
	tk.MustQuery("select a from t use index(b) order by b").Check(testkit.Rows("9", "8", "7", "6", "5", "4", "3", "2", "1", "0"))
}

func (s *testSuite) TestOrderBy(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int, c2 int, c3 varchar(20))")
	tk.MustExec("insert into t values (1, 2, 'abc'), (2, 1, 'bcd')")

	// Fix issue https://github.com/pingcap/tidb/issues/337
	tk.MustQuery("select c1 as a, c1 as b from t order by c1").Check(testkit.Rows("1 1", "2 2"))

	tk.MustQuery("select c1 as a, t.c1 as a from t order by a desc").Check(testkit.Rows("2 2", "1 1"))
	tk.MustQuery("select c1 as c2 from t order by c2").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select sum(c1) from t order by sum(c1)").Check(testkit.Rows("3"))
	tk.MustQuery("select c1 as c2 from t order by c2 + 1").Check(testkit.Rows("2", "1"))

	// Order by position.
	tk.MustQuery("select * from t order by 1").Check(testkit.Rows("1 2 abc", "2 1 bcd"))
	tk.MustQuery("select * from t order by 2").Check(testkit.Rows("2 1 bcd", "1 2 abc"))

	// Order by binary.
	tk.MustQuery("select c1, c3 from t order by binary c1 desc").Check(testkit.Rows("2 bcd", "1 abc"))
	tk.MustQuery("select c1, c2 from t order by binary c3").Check(testkit.Rows("1 2", "2 1"))
}

func (s *testSuite) TestSelectErrorRow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	_, err := tk.Exec("select row(1, 1) from test")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select * from test group by row(1, 1);")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select * from test order by row(1, 1);")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select * from test having row(1, 1);")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select (select 1, 1) from test;")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select * from test group by (select 1, 1);")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select * from test order by (select 1, 1);")
	c.Assert(err, NotNil)

	_, err = tk.Exec("select * from test having (select 1, 1);")
	c.Assert(err, NotNil)
}

// TestIssue2612 is related with https://github.com/pingcap/tidb/issues/2612
func (s *testSuite) TestIssue2612(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (
		create_at datetime NOT NULL DEFAULT '1000-01-01 00:00:00',
		finish_at datetime NOT NULL DEFAULT '1000-01-01 00:00:00');`)
	tk.MustExec(`insert into t values ('2016-02-13 15:32:24',  '2016-02-11 17:23:22');`)
	rs, err := tk.Exec(`select timediff(finish_at, create_at) from t;`)
	c.Assert(err, IsNil)
	row, err := rs.Next(goctx.Background())
	c.Assert(err, IsNil)
	c.Assert(row.GetDuration(0).String(), Equals, "-46:09:02")
}

// TestIssue345 is related with https://github.com/pingcap/tidb/issues/345
func (s *testSuite) TestIssue345(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1, t2`)
	tk.MustExec(`create table t1 (c1 int);`)
	tk.MustExec(`create table t2 (c2 int);`)
	tk.MustExec(`insert into t1 values (1);`)
	tk.MustExec(`insert into t2 values (2);`)
	tk.MustExec(`update t1, t2 set t1.c1 = 2, t2.c2 = 1;`)
	tk.MustExec(`update t1, t2 set c1 = 2, c2 = 1;`)
	tk.MustExec(`update t1 as a, t2 as b set a.c1 = 2, b.c2 = 1;`)

	// Check t1 content
	r := tk.MustQuery("SELECT * FROM t1;")
	r.Check(testkit.Rows("2"))
	// Check t2 content
	r = tk.MustQuery("SELECT * FROM t2;")
	r.Check(testkit.Rows("1"))

	tk.MustExec(`update t1 as a, t2 as t1 set a.c1 = 1, t1.c2 = 2;`)
	// Check t1 content
	r = tk.MustQuery("SELECT * FROM t1;")
	r.Check(testkit.Rows("1"))
	// Check t2 content
	r = tk.MustQuery("SELECT * FROM t2;")
	r.Check(testkit.Rows("2"))

	_, err := tk.Exec(`update t1 as a, t2 set t1.c1 = 10;`)
	c.Assert(err, NotNil)
}

func (s *testSuite) TestIssue5055(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1, t2`)
	tk.MustExec(`create table t1 (a int);`)
	tk.MustExec(`create table t2 (a int);`)
	tk.MustExec(`insert into t1 values(1);`)
	tk.MustExec(`insert into t2 values(1);`)
	result := tk.MustQuery("select tbl1.* from (select t1.a, 1 from t1) tbl1 left join t2 tbl2 on tbl1.a = tbl2.a order by tbl1.a desc limit 1;")
	result.Check(testkit.Rows("1 1"))
}

func (s *testSuite) TestUnion(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	testSQL := `drop table if exists union_test; create table union_test(id int);`
	tk.MustExec(testSQL)

	testSQL = `drop table if exists union_test;`
	tk.MustExec(testSQL)
	testSQL = `create table union_test(id int);`
	tk.MustExec(testSQL)
	testSQL = `insert union_test values (1),(2)`
	tk.MustExec(testSQL)

	testSQL = `select id from union_test union select id from union_test;`
	r := tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1", "2"))

	testSQL = `select * from (select id from union_test union select id from union_test) t order by id;`
	r = tk.MustQuery(testSQL)
	r.Check(testkit.Rows("1", "2"))

	r = tk.MustQuery("select 1 union all select 1")
	r.Check(testkit.Rows("1", "1"))

	r = tk.MustQuery("select 1 union all select 1 union select 1")
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("select 1 as a union (select 2) order by a limit 1")
	r.Check(testkit.Rows("1"))

	r = tk.MustQuery("select 1 as a union (select 2) order by a limit 1, 1")
	r.Check(testkit.Rows("2"))

	r = tk.MustQuery("select id from union_test union all (select 1) order by id desc")
	r.Check(testkit.Rows("2", "1", "1"))

	r = tk.MustQuery("select id as a from union_test union (select 1) order by a desc")
	r.Check(testkit.Rows("2", "1"))

	r = tk.MustQuery(`select null as a union (select "abc") order by a`)
	r.Check(testkit.Rows("<nil>", "abc"))

	r = tk.MustQuery(`select "abc" as a union (select 1) order by a`)
	r.Check(testkit.Rows("1", "abc"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (c int, d int)")
	tk.MustExec("insert t1 values (NULL, 1)")
	tk.MustExec("insert t1 values (1, 1)")
	tk.MustExec("insert t1 values (1, 2)")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2 (c int, d int)")
	tk.MustExec("insert t2 values (1, 3)")
	tk.MustExec("insert t2 values (1, 1)")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3 (c int, d int)")
	tk.MustExec("insert t3 values (3, 2)")
	tk.MustExec("insert t3 values (4, 3)")
	r = tk.MustQuery(`select sum(c1), c2 from (select c c1, d c2 from t1 union all select d c1, c c2 from t2 union all select c c1, d c2 from t3) x group by c2 order by c2`)
	r.Check(testkit.Rows("5 1", "4 2", "4 3"))

	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1 (a int primary key)")
	tk.MustExec("create table t2 (a int primary key)")
	tk.MustExec("create table t3 (a int primary key)")
	tk.MustExec("insert t1 values (7), (8)")
	tk.MustExec("insert t2 values (1), (9)")
	tk.MustExec("insert t3 values (2), (3)")
	r = tk.MustQuery("select * from t1 union all select * from t2 union all (select * from t3) order by a limit 2")
	r.Check(testkit.Rows("1", "2"))

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create table t2 (a int)")
	tk.MustExec("insert t1 values (2), (1)")
	tk.MustExec("insert t2 values (3), (4)")
	r = tk.MustQuery("select * from t1 union all (select * from t2) order by a limit 1")
	r.Check(testkit.Rows("1"))
	r = tk.MustQuery("select (select * from t1 where a != t.a union all (select * from t2 where a != t.a) order by a limit 1) from t1 t")
	r.Check(testkit.Rows("1", "2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int unsigned primary key auto_increment, c1 int, c2 int, index c1_c2 (c1, c2))")
	tk.MustExec("insert into t (c1, c2) values (1, 1)")
	tk.MustExec("insert into t (c1, c2) values (1, 2)")
	tk.MustExec("insert into t (c1, c2) values (2, 3)")
	r = tk.MustQuery("select * from t where t.c1 = 1 union select * from t where t.id = 1")
	r.Check(testkit.Rows("1 1 1", "2 1 2"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (f1 DATE)")
	tk.MustExec("INSERT INTO t VALUES ('1978-11-26')")
	r = tk.MustQuery("SELECT f1+0 FROM t UNION SELECT f1+0 FROM t")
	r.Check(testkit.Rows("19781126"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int, b int)")
	tk.MustExec("INSERT INTO t VALUES ('1', '1')")
	r = tk.MustQuery("select b from (SELECT * FROM t UNION ALL SELECT a, b FROM t order by a) t")

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a DECIMAL(4,2))")
	tk.MustExec("INSERT INTO t VALUE(12.34)")
	r = tk.MustQuery("SELECT 1 AS c UNION select a FROM t")
	r.Sort().Check(testkit.Rows("1.00", "12.34"))

	// #issue3771
	r = tk.MustQuery("SELECT 'a' UNION SELECT CONCAT('a', -4)")
	r.Sort().Check(testkit.Rows("a", "a-4"))

	// test race
	tk.MustQuery("SELECT @x:=0 UNION ALL SELECT @x:=0 UNION ALL SELECT @x")

	// test field tp
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("CREATE TABLE t1 (a date)")
	tk.MustExec("CREATE TABLE t2 (a date)")
	tk.MustExec("SELECT a from t1 UNION select a FROM t2")
	tk.MustQuery("show create table t1").Check(testkit.Rows("t1 CREATE TABLE `t1` (\n" + "  `a` date DEFAULT NULL\n" + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"))

	// Move from session test.
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (c double);")
	tk.MustExec("create table t2 (c double);")
	tk.MustExec("insert into t1 value (73);")
	tk.MustExec("insert into t2 value (930);")
	// If set unspecified column flen to 0, it will cause bug in union.
	// This test is used to prevent the bug reappear.
	tk.MustQuery("select c from t1 union (select c from t2) order by c").Check(testkit.Rows("73", "930"))

	// issue 5703
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a date)")
	tk.MustExec("insert into t value ('2017-01-01'), ('2017-01-02')")
	r = tk.MustQuery("(select a from t where a < 0) union (select a from t where a > 0) order by a")
	r.Check(testkit.Rows("2017-01-01", "2017-01-02"))
}

func (s *testSuite) TestIn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (c1 int primary key, c2 int, key c (c2));`)
	for i := 0; i <= 200; i++ {
		tk.MustExec(fmt.Sprintf("insert t values(%d, %d)", i, i))
	}
	queryStr := `select c2 from t where c1 in ('7', '10', '112', '111', '98', '106', '100', '9', '18', '17') order by c2`
	r := tk.MustQuery(queryStr)
	r.Check(testkit.Rows("7", "9", "10", "17", "18", "98", "100", "106", "111", "112"))

	queryStr = `select c2 from t where c1 in ('7a')`
	tk.MustQuery(queryStr).Check(testkit.Rows("7"))
}

func (s *testSuite) TestTablePKisHandleScan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int PRIMARY KEY AUTO_INCREMENT)")
	tk.MustExec("insert t values (),()")
	tk.MustExec("insert t values (-100),(0)")

	tests := []struct {
		sql    string
		result [][]interface{}
	}{
		{
			"select * from t",
			testkit.Rows("-100", "1", "2", "3"),
		},
		{
			"select * from t where a = 1",
			testkit.Rows("1"),
		},
		{
			"select * from t where a != 1",
			testkit.Rows("-100", "2", "3"),
		},
		{
			"select * from t where a >= '1.1'",
			testkit.Rows("2", "3"),
		},
		{
			"select * from t where a < '1.1'",
			testkit.Rows("-100", "1"),
		},
		{
			"select * from t where a > '-100.1' and a < 2",
			testkit.Rows("-100", "1"),
		},
		{
			"select * from t where a is null",
			testkit.Rows(),
		}, {
			"select * from t where a is true",
			testkit.Rows("-100", "1", "2", "3"),
		}, {
			"select * from t where a is false",
			testkit.Rows(),
		},
		{
			"select * from t where a in (1, 2)",
			testkit.Rows("1", "2"),
		},
		{
			"select * from t where a between 1 and 2",
			testkit.Rows("1", "2"),
		},
	}

	for _, tt := range tests {
		result := tk.MustQuery(tt.sql)
		result.Check(tt.result)
	}
}

func (s *testSuite) TestIndexScan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unique)")
	tk.MustExec("insert t values (-1), (2), (3), (5), (6), (7), (8), (9)")
	result := tk.MustQuery("select a from t where a < 0 or (a >= 2.1 and a < 5.1) or ( a > 5.9 and a <= 7.9) or a > '8.1'")
	result.Check(testkit.Rows("-1", "3", "5", "6", "7", "9"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unique)")
	tk.MustExec("insert t values (0)")
	result = tk.MustQuery("select NULL from t ")
	result.Check(testkit.Rows("<nil>"))
	// test for double read
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unique, b int)")
	tk.MustExec("insert t values (5, 0)")
	tk.MustExec("insert t values (4, 0)")
	tk.MustExec("insert t values (3, 0)")
	tk.MustExec("insert t values (2, 0)")
	tk.MustExec("insert t values (1, 0)")
	tk.MustExec("insert t values (0, 0)")
	result = tk.MustQuery("select * from t order by a limit 3")
	result.Check(testkit.Rows("0 0", "1 0", "2 0"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unique, b int)")
	tk.MustExec("insert t values (0, 1)")
	tk.MustExec("insert t values (1, 2)")
	tk.MustExec("insert t values (2, 1)")
	tk.MustExec("insert t values (3, 2)")
	tk.MustExec("insert t values (4, 1)")
	tk.MustExec("insert t values (5, 2)")
	result = tk.MustQuery("select * from t where a < 5 and b = 1 limit 2")
	result.Check(testkit.Rows("0 1", "2 1"))
	tk.MustExec("drop table if exists tab1")
	tk.MustExec("CREATE TABLE tab1(pk INTEGER PRIMARY KEY, col0 INTEGER, col1 FLOAT, col3 INTEGER, col4 FLOAT)")
	tk.MustExec("CREATE INDEX idx_tab1_0 on tab1 (col0)")
	tk.MustExec("CREATE INDEX idx_tab1_1 on tab1 (col1)")
	tk.MustExec("CREATE INDEX idx_tab1_3 on tab1 (col3)")
	tk.MustExec("CREATE INDEX idx_tab1_4 on tab1 (col4)")
	tk.MustExec("INSERT INTO tab1 VALUES(1,37,20.85,30,10.69)")
	result = tk.MustQuery("SELECT pk FROM tab1 WHERE ((col3 <= 6 OR col3 < 29 AND (col0 < 41)) OR col3 > 42) AND col1 >= 96.1 AND col3 = 30 AND col3 > 17 AND (col0 BETWEEN 36 AND 42)")
	result.Check(testkit.Rows())
	tk.MustExec("drop table if exists tab1")
	tk.MustExec("CREATE TABLE tab1(pk INTEGER PRIMARY KEY, a INTEGER, b INTEGER)")
	tk.MustExec("CREATE INDEX idx_tab1_0 on tab1 (a)")
	tk.MustExec("INSERT INTO tab1 VALUES(1,1,1)")
	tk.MustExec("INSERT INTO tab1 VALUES(2,2,1)")
	tk.MustExec("INSERT INTO tab1 VALUES(3,1,2)")
	tk.MustExec("INSERT INTO tab1 VALUES(4,2,2)")
	result = tk.MustQuery("SELECT * FROM tab1 WHERE pk <= 3 AND a = 1")
	result.Check(testkit.Rows("1 1 1", "3 1 2"))
	result = tk.MustQuery("SELECT * FROM tab1 WHERE pk <= 4 AND a = 1 AND b = 2")
	result.Check(testkit.Rows("3 1 2"))
	tk.MustExec("CREATE INDEX idx_tab1_1 on tab1 (b, a)")
	result = tk.MustQuery("SELECT pk FROM tab1 WHERE b > 1")
	result.Check(testkit.Rows("3", "4"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a varchar(3), index(a))")
	tk.MustExec("insert t values('aaa'), ('aab')")
	result = tk.MustQuery("select * from t where a >= 'aaaa' and a < 'aabb'")
	result.Check(testkit.Rows("aab"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a int primary key, b int, c int, index(c))")
	tk.MustExec("insert t values(1, 1, 1), (2, 2, 2), (4, 4, 4), (3, 3, 3), (5, 5, 5)")
	// Test for double read and top n.
	result = tk.MustQuery("select a from t where c >= 2 order by b desc limit 1")
	result.Check(testkit.Rows("5"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(50) primary key, b int, c int, index idx(b))")
	tk.MustExec("insert into t values('aa', 1, 1)")
	tk.MustQuery("select * from t use index(idx) where a > 'a'").Check(testkit.Rows("aa 1 1"))
}

func (s *testSuite) TestIndexReverseOrder(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key auto_increment, b int, index idx (b))")
	tk.MustExec("insert t (b) values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)")
	result := tk.MustQuery("select b from t order by b desc")
	result.Check(testkit.Rows("9", "8", "7", "6", "5", "4", "3", "2", "1", "0"))
	result = tk.MustQuery("select b from t where b <3 or (b >=6 and b < 8) order by b desc")
	result.Check(testkit.Rows("7", "6", "2", "1", "0"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index idx (b, a))")
	tk.MustExec("insert t values (0, 2), (1, 2), (2, 2), (0, 1), (1, 1), (2, 1), (0, 0), (1, 0), (2, 0)")
	result = tk.MustQuery("select b, a from t order by b, a desc")
	result.Check(testkit.Rows("0 2", "0 1", "0 0", "1 2", "1 1", "1 0", "2 2", "2 1", "2 0"))
}

func (s *testSuite) TestTableReverseOrder(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key auto_increment, b int)")
	tk.MustExec("insert t (b) values (1), (2), (3), (4), (5), (6), (7), (8), (9)")
	result := tk.MustQuery("select b from t order by a desc")
	result.Check(testkit.Rows("9", "8", "7", "6", "5", "4", "3", "2", "1"))
	result = tk.MustQuery("select a from t where a <3 or (a >=6 and a < 8) order by a desc")
	result.Check(testkit.Rows("7", "6", "2", "1"))
}

func (s *testSuite) TestDefaultNull(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key auto_increment, b int default 1, c int)")
	tk.MustExec("insert t values ()")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 <nil>"))
	tk.MustExec("update t set b = NULL where a = 1")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 <nil> <nil>"))
	tk.MustExec("update t set c = 1")
	tk.MustQuery("select * from t ").Check(testkit.Rows("1 <nil> 1"))
	tk.MustExec("delete from t where a = 1")
	tk.MustExec("insert t (a) values (1)")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 1 <nil>"))
}

func (s *testSuite) TestUnsignedPKColumn(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int unsigned primary key, b int, c int, key idx_ba (b, c, a));")
	tk.MustExec("insert t values (1, 1, 1)")
	result := tk.MustQuery("select * from t;")
	result.Check(testkit.Rows("1 1 1"))
	tk.MustExec("update t set c=2 where a=1;")
	result = tk.MustQuery("select * from t where b=1;")
	result.Check(testkit.Rows("1 1 2"))
}

func (s *testSuite) TestJSON(c *C) {
	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists test_json")
	tk.MustExec("create table test_json (id int, a json)")
	tk.MustExec(`insert into test_json (id, a) values (1, '{"a":[1,"2",{"aa":"bb"},4],"b":true}')`)
	tk.MustExec(`insert into test_json (id, a) values (2, "null")`)
	tk.MustExec(`insert into test_json (id, a) values (3, null)`)
	tk.MustExec(`insert into test_json (id, a) values (4, 'true')`)
	tk.MustExec(`insert into test_json (id, a) values (5, '3')`)
	tk.MustExec(`insert into test_json (id, a) values (5, '4.0')`)
	tk.MustExec(`insert into test_json (id, a) values (6, '"string"')`)

	var result *testkit.Result
	result = tk.MustQuery(`select tj.a from test_json tj order by tj.id`)
	result.Check(testkit.Rows(`{"a":[1,"2",{"aa":"bb"},4],"b":true}`, "null", "<nil>", "true", "3", "4", `"string"`))

	// Check json_type function
	result = tk.MustQuery(`select json_type(a) from test_json tj order by tj.id`)
	result.Check(testkit.Rows("OBJECT", "NULL", "<nil>", "BOOLEAN", "INTEGER", "DOUBLE", "STRING"))

	// Check json compare with primitives.
	result = tk.MustQuery(`select a from test_json tj where a = 3`)
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery(`select a from test_json tj where a = 4.0`)
	result.Check(testkit.Rows("4"))
	result = tk.MustQuery(`select a from test_json tj where a = true`)
	result.Check(testkit.Rows("true"))
	result = tk.MustQuery(`select a from test_json tj where a = "string"`)
	result.Check(testkit.Rows(`"string"`))

	// Check cast(true/false as JSON).
	result = tk.MustQuery(`select cast(true as JSON)`)
	result.Check(testkit.Rows(`true`))
	result = tk.MustQuery(`select cast(false as JSON)`)
	result.Check(testkit.Rows(`false`))

	// Check two json grammar sugar.
	result = tk.MustQuery(`select a->>'$.a[2].aa' as x, a->'$.b' as y from test_json having x is not null order by id`)
	result.Check(testkit.Rows(`bb true`))
	result = tk.MustQuery(`select a->'$.a[2].aa' as x, a->>'$.b' as y from test_json having x is not null order by id`)
	result.Check(testkit.Rows(`"bb" true`))

	// Check some DDL limits for TEXT/BLOB/JSON column.
	var err error
	var terr *terror.Error

	_, err = tk.Exec(`create table test_bad_json(a json default '{}')`)
	c.Assert(err, NotNil)
	terr = errors.Trace(err).(*errors.Err).Cause().(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrBlobCantHaveDefault))

	_, err = tk.Exec(`create table test_bad_json(a blob default 'hello')`)
	c.Assert(err, NotNil)
	terr = errors.Trace(err).(*errors.Err).Cause().(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrBlobCantHaveDefault))

	_, err = tk.Exec(`create table test_bad_json(a text default 'world')`)
	c.Assert(err, NotNil)
	terr = errors.Trace(err).(*errors.Err).Cause().(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrBlobCantHaveDefault))

	// check json fields cannot be used as key.
	_, err = tk.Exec(`create table test_bad_json(id int, a json, key (a))`)
	c.Assert(err, NotNil)
	terr = errors.Trace(err).(*errors.Err).Cause().(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrJSONUsedAsKey))

	// check CAST AS JSON.
	result = tk.MustQuery(`select CAST('3' AS JSON), CAST('{}' AS JSON), CAST(null AS JSON)`)
	result.Check(testkit.Rows(`3 {} <nil>`))
}

func (s *testSuite) TestMultiUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_mu (a int primary key, b int, c int)`)
	tk.MustExec(`INSERT INTO test_mu VALUES (1, 2, 3), (4, 5, 6), (7, 8, 9)`)

	// Test INSERT ... ON DUPLICATE UPDATE set_lists.
	tk.MustExec(`INSERT INTO test_mu VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE b = 3, c = b`)
	result := tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 3 3`, `4 5 6`, `7 8 9`))

	tk.MustExec(`INSERT INTO test_mu VALUES (1, 2, 3) ON DUPLICATE KEY UPDATE c = 2, b = c+5`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 5 6`, `7 8 9`))

	// Test UPDATE ... set_lists.
	tk.MustExec(`UPDATE test_mu SET b = 0, c = b WHERE a = 4`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 0 0`, `7 8 9`))

	tk.MustExec(`UPDATE test_mu SET c = 8, b = c WHERE a = 4`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 8 8`, `7 8 9`))

	tk.MustExec(`UPDATE test_mu SET c = b, b = c WHERE a = 7`)
	result = tk.MustQuery(`SELECT * FROM test_mu ORDER BY a`)
	result.Check(testkit.Rows(`1 7 2`, `4 8 8`, `7 8 8`))
}

func (s *testSuite) TestGeneratedColumnWrite(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_gc_write (a int primary key, b int, c int as (a+8) virtual)`)
	tk.MustExec(`CREATE TABLE test_gc_write_1 (a int primary key, b int, c int)`)

	tests := []struct {
		stmt string
		err  int
	}{
		// Can't modify generated column by values.
		{`insert into test_gc_write (a, b, c) values (1, 1, 1)`, mysql.ErrBadGeneratedColumn},
		{`insert into test_gc_write values (1, 1, 1)`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by select clause.
		{`insert into test_gc_write select 1, 1, 1`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by on duplicate clause.
		{`insert into test_gc_write (a, b) values (1, 1) on duplicate key update c = 1`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by set.
		{`insert into test_gc_write set a = 1, b = 1, c = 1`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by update clause.
		{`update test_gc_write set c = 1`, mysql.ErrBadGeneratedColumn},
		// Can't modify generated column by multi-table update clause.
		{`update test_gc_write, test_gc_write_1 set test_gc_write.c = 1`, mysql.ErrBadGeneratedColumn},

		// Can insert without generated columns.
		{`insert into test_gc_write (a, b) values (1, 1)`, 0},
		{`insert into test_gc_write set a = 2, b = 2`, 0},
		// Can update without generated columns.
		{`update test_gc_write set b = 2 where a = 2`, 0},
		{`update test_gc_write t1, test_gc_write_1 t2 set t1.b = 3, t2.b = 4`, 0},

		// But now we can't do this, just as same with MySQL 5.7:
		{`insert into test_gc_write values (1, 1)`, mysql.ErrWrongValueCountOnRow},
		{`insert into test_gc_write select 1, 1`, mysql.ErrWrongValueCountOnRow},
	}
	for _, tt := range tests {
		_, err := tk.Exec(tt.stmt)
		if tt.err != 0 {
			c.Assert(err, NotNil)
			terr := errors.Trace(err).(*errors.Err).Cause().(*terror.Error)
			c.Assert(terr.Code(), Equals, terror.ErrCode(tt.err))
		} else {
			c.Assert(err, IsNil)
		}
	}
}

// TestGeneratedColumnRead tests select generated columns from table.
// They should be calculated from their generation expressions.
func (s *testSuite) TestGeneratedColumnRead(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE test_gc_read(a int primary key, b int, c int as (a+b), d int as (a*b) stored)`)

	// Insert only column a and b, leave c and d be calculated from them.
	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (0,null),(1,2),(3,4)`)
	result := tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil>`, `1 2 3 2`, `3 4 7 12`))

	tk.MustExec(`INSERT INTO test_gc_read SET a = 5, b = 10`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil>`, `1 2 3 2`, `3 4 7 12`, `5 10 15 50`))

	tk.MustExec(`REPLACE INTO test_gc_read (a, b) VALUES (5, 6)`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil>`, `1 2 3 2`, `3 4 7 12`, `5 6 11 30`))

	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (5, 8) ON DUPLICATE KEY UPDATE b = 9`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil>`, `1 2 3 2`, `3 4 7 12`, `5 9 14 45`))

	// Test select only-generated-column-without-dependences.
	result = tk.MustQuery(`SELECT c, d FROM test_gc_read`)
	result.Check(testkit.Rows(`<nil> <nil>`, `3 2`, `7 12`, `14 45`))

	// Test order of on duplicate key update list.
	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (5, 8) ON DUPLICATE KEY UPDATE a = 6, b = a`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil>`, `1 2 3 2`, `3 4 7 12`, `6 6 12 36`))

	tk.MustExec(`INSERT INTO test_gc_read (a, b) VALUES (6, 8) ON DUPLICATE KEY UPDATE b = 8, a = b`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil> <nil>`, `1 2 3 2`, `3 4 7 12`, `8 8 16 64`))

	// Test where-conditions on virtual/stored generated columns.
	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE c = 7`)
	result.Check(testkit.Rows(`3 4 7 12`))

	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE d = 64`)
	result.Check(testkit.Rows(`8 8 16 64`))

	// Test update where-conditions on virtual/generated columns.
	tk.MustExec(`UPDATE test_gc_read SET a = a + 100 WHERE c = 7`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE c = 107`)
	result.Check(testkit.Rows(`103 4 107 412`))

	// Test update where-conditions on virtual/generated columns.
	tk.MustExec(`UPDATE test_gc_read m SET m.a = m.a + 100 WHERE c = 107`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE c = 207`)
	result.Check(testkit.Rows(`203 4 207 812`))

	tk.MustExec(`UPDATE test_gc_read SET a = a - 200 WHERE d = 812`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read WHERE d = 12`)
	result.Check(testkit.Rows(`3 4 7 12`))

	// Test on-conditions on virtual/stored generated columns.
	tk.MustExec(`CREATE TABLE test_gc_help(a int primary key, b int, c int, d int)`)
	tk.MustExec(`INSERT INTO test_gc_help(a, b, c, d) SELECT * FROM test_gc_read`)

	result = tk.MustQuery(`SELECT t1.* FROM test_gc_read t1 JOIN test_gc_help t2 ON t1.c = t2.c ORDER BY t1.a`)
	result.Check(testkit.Rows(`1 2 3 2`, `3 4 7 12`, `8 8 16 64`))

	result = tk.MustQuery(`SELECT t1.* FROM test_gc_read t1 JOIN test_gc_help t2 ON t1.d = t2.d ORDER BY t1.a`)
	result.Check(testkit.Rows(`1 2 3 2`, `3 4 7 12`, `8 8 16 64`))

	// Test generated column in subqueries.
	result = tk.MustQuery(`SELECT * FROM test_gc_read t WHERE t.a not in (SELECT t.a FROM test_gc_read t where t.c > 5)`)
	result.Sort().Check(testkit.Rows(`0 <nil> <nil> <nil>`, `1 2 3 2`))

	result = tk.MustQuery(`SELECT * FROM test_gc_read t WHERE t.c in (SELECT t.c FROM test_gc_read t where t.c > 5)`)
	result.Sort().Check(testkit.Rows(`3 4 7 12`, `8 8 16 64`))

	result = tk.MustQuery(`SELECT tt.b FROM test_gc_read tt WHERE tt.a = (SELECT max(t.a) FROM test_gc_read t WHERE t.c = tt.c) ORDER BY b`)
	result.Check(testkit.Rows(`2`, `4`, `8`))

	// Test aggregation on virtual/stored generated columns.
	result = tk.MustQuery(`SELECT c, sum(a) aa, max(d) dd FROM test_gc_read GROUP BY c ORDER BY aa`)
	result.Check(testkit.Rows(`<nil> 0 <nil>`, `3 1 2`, `7 3 12`, `16 8 64`))

	result = tk.MustQuery(`SELECT a, sum(c), sum(d) FROM test_gc_read GROUP BY a ORDER BY a`)
	result.Check(testkit.Rows(`0 <nil> <nil>`, `1 3 2`, `3 7 12`, `8 16 64`))

	// Test multi-update on generated columns.
	tk.MustExec(`UPDATE test_gc_read m, test_gc_read n SET m.a = m.a + 10, n.a = n.a + 10`)
	result = tk.MustQuery(`SELECT * FROM test_gc_read ORDER BY a`)
	result.Check(testkit.Rows(`10 <nil> <nil> <nil>`, `11 2 13 22`, `13 4 17 52`, `18 8 26 144`))

	// Test different types between generation expression and generated column.
	tk.MustExec(`CREATE TABLE test_gc_read_cast(a VARCHAR(255), b VARCHAR(255), c INT AS (JSON_EXTRACT(a, b)), d INT AS (JSON_EXTRACT(a, b)) STORED)`)
	tk.MustExec(`INSERT INTO test_gc_read_cast (a, b) VALUES ('{"a": "3"}', '$.a')`)
	result = tk.MustQuery(`SELECT c, d FROM test_gc_read_cast`)
	result.Check(testkit.Rows(`3 3`))

	tk.MustExec(`CREATE TABLE test_gc_read_cast_1(a VARCHAR(255), b VARCHAR(255), c ENUM("red", "yellow") AS (JSON_UNQUOTE(JSON_EXTRACT(a, b))))`)
	tk.MustExec(`INSERT INTO test_gc_read_cast_1 (a, b) VALUES ('{"a": "yellow"}', '$.a')`)
	result = tk.MustQuery(`SELECT c FROM test_gc_read_cast_1`)
	result.Check(testkit.Rows(`yellow`))

	tk.MustExec(`CREATE TABLE test_gc_read_cast_2( a JSON, b JSON AS (a->>'$.a'))`)
	tk.MustExec(`INSERT INTO test_gc_read_cast_2(a) VALUES ('{"a": "{    \\\"key\\\": \\\"\\u6d4b\\\"    }"}')`)
	result = tk.MustQuery(`SELECT b FROM test_gc_read_cast_2`)
	result.Check(testkit.Rows(`{"key":"测"}`))

	_, err := tk.Exec(`INSERT INTO test_gc_read_cast_1 (a, b) VALUES ('{"a": "invalid"}', '$.a')`)
	c.Assert(err, NotNil)

	// Test not null generated columns.
	tk.MustExec(`CREATE TABLE test_gc_read_1(a int primary key, b int, c int as (a+b) not null, d int as (a*b) stored)`)
	tk.MustExec(`CREATE TABLE test_gc_read_2(a int primary key, b int, c int as (a+b), d int as (a*b) stored not null)`)
	tests := []struct {
		stmt string
		err  int
	}{
		// Can't insert these records, because generated columns are not null.
		{`insert into test_gc_read_1(a, b) values (1, null)`, mysql.ErrBadNull},
		{`insert into test_gc_read_2(a, b) values (1, null)`, mysql.ErrBadNull},
	}
	for _, tt := range tests {
		_, err := tk.Exec(tt.stmt)
		if tt.err != 0 {
			c.Assert(err, NotNil)
			terr := errors.Trace(err).(*errors.Err).Cause().(*terror.Error)
			c.Assert(terr.Code(), Equals, terror.ErrCode(tt.err))
		} else {
			c.Assert(err, IsNil)
		}
	}
}

func (s *testSuite) TestToPBExpr(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a decimal(10,6), b decimal, index idx_b (b))")
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert t values (1.1, 1.1)")
	tk.MustExec("insert t values (2.4, 2.4)")
	tk.MustExec("insert t values (3.3, 2.7)")
	result := tk.MustQuery("select * from t where a < 2.399999")
	result.Check(testkit.Rows("1.100000 1"))
	result = tk.MustQuery("select * from t where a > 1.5")
	result.Check(testkit.Rows("2.400000 2", "3.300000 3"))
	result = tk.MustQuery("select * from t where a <= 1.1")
	result.Check(testkit.Rows("1.100000 1"))
	result = tk.MustQuery("select * from t where b >= 3")
	result.Check(testkit.Rows("3.300000 3"))
	result = tk.MustQuery("select * from t where not (b = 1)")
	result.Check(testkit.Rows("2.400000 2", "3.300000 3"))
	result = tk.MustQuery("select * from t where b&1 = a|1")
	result.Check(testkit.Rows("1.100000 1"))
	result = tk.MustQuery("select * from t where b != 2 and b <=> 3")
	result.Check(testkit.Rows("3.300000 3"))
	result = tk.MustQuery("select * from t where b in (3)")
	result.Check(testkit.Rows("3.300000 3"))
	result = tk.MustQuery("select * from t where b not in (1, 2)")
	result.Check(testkit.Rows("3.300000 3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(255), b int)")
	tk.MustExec("insert t values ('abc123', 1)")
	tk.MustExec("insert t values ('ab123', 2)")
	result = tk.MustQuery("select * from t where a like 'ab%'")
	result.Check(testkit.Rows("abc123 1", "ab123 2"))
	result = tk.MustQuery("select * from t where a like 'ab_12'")
	result.Check(nil)
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key)")
	tk.MustExec("insert t values (1)")
	tk.MustExec("insert t values (2)")
	result = tk.MustQuery("select * from t where not (a = 1)")
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery("select * from t where not(not (a = 1))")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from t where not(a != 1 and a != 2)")
	result.Check(testkit.Rows("1", "2"))
}

func (s *testSuite) TestDatumXAPI(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a decimal(10,6), b decimal, index idx_b (b))")
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert t values (1.1, 1.1)")
	tk.MustExec("insert t values (2.2, 2.2)")
	tk.MustExec("insert t values (3.3, 2.7)")
	result := tk.MustQuery("select * from t where a > 1.5")
	result.Check(testkit.Rows("2.200000 2", "3.300000 3"))
	result = tk.MustQuery("select * from t where b > 1.5")
	result.Check(testkit.Rows("2.200000 2", "3.300000 3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a time(3), b time, index idx_a (a))")
	tk.MustExec("insert t values ('11:11:11', '11:11:11')")
	tk.MustExec("insert t values ('11:11:12', '11:11:12')")
	tk.MustExec("insert t values ('11:11:13', '11:11:13')")
	result = tk.MustQuery("select * from t where a > '11:11:11.5'")
	result.Check(testkit.Rows("11:11:12.000 11:11:12", "11:11:13.000 11:11:13"))
	result = tk.MustQuery("select * from t where b > '11:11:11.5'")
	result.Check(testkit.Rows("11:11:12.000 11:11:12", "11:11:13.000 11:11:13"))
}

func (s *testSuite) TestSQLMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a tinyint not null)")
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")
	_, err := tk.Exec("insert t values ()")
	c.Check(err, NotNil)

	_, err = tk.Exec("insert t values ('1000')")
	c.Check(err, NotNil)

	tk.MustExec("create table if not exists tdouble (a double(3,2))")
	_, err = tk.Exec("insert tdouble values (10.23)")
	c.Check(err, NotNil)

	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert t values ()")
	tk.MustExec("insert t values (1000)")
	tk.MustQuery("select * from t").Check(testkit.Rows("0", "127"))

	tk.MustExec("insert tdouble values (10.23)")
	tk.MustQuery("select * from tdouble").Check(testkit.Rows("9.99"))

	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")
	tk.MustExec("set @@global.sql_mode = ''")

	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")
	tk2.MustExec("create table t2 (a varchar(3))")
	tk2.MustExec("insert t2 values ('abcd')")
	tk2.MustQuery("select * from t2").Check(testkit.Rows("abc"))

	// session1 is still in strict mode.
	_, err = tk.Exec("insert t2 values ('abcd')")
	c.Check(err, NotNil)
	// Restore original global strict mode.
	tk.MustExec("set @@global.sql_mode = 'STRICT_TRANS_TABLES'")
}

func (s *testSuite) TestTableDual(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	result := tk.MustQuery("Select 1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("Select 1 from dual")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("Select count(*) from dual")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("Select 1 from dual where 1")
	result.Check(testkit.Rows("1"))
}

func (s *testSuite) TestTableScan(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use information_schema")
	result := tk.MustQuery("select * from schemata")
	// There must be these tables: information_schema, mysql, performance_schema and test.
	c.Assert(len(result.Rows()), GreaterEqual, 4)
	tk.MustExec("use test")
	tk.MustExec("create database mytest")
	rowStr1 := fmt.Sprintf("%s %s %s %s %v", "def", "mysql", "utf8", "utf8_bin", nil)
	rowStr2 := fmt.Sprintf("%s %s %s %s %v", "def", "mytest", "utf8", "utf8_bin", nil)
	tk.MustExec("use information_schema")
	result = tk.MustQuery("select * from schemata where schema_name = 'mysql'")
	result.Check(testkit.Rows(rowStr1))
	result = tk.MustQuery("select * from schemata where schema_name like 'my%'")
	result.Check(testkit.Rows(rowStr1, rowStr2))
	result = tk.MustQuery("select 1 from tables limit 1")
	result.Check(testkit.Rows("1"))
}

func (s *testSuite) TestAdapterStatement(c *C) {
	se, err := tidb.CreateSession4Test(s.store)
	c.Check(err, IsNil)
	se.GetSessionVars().TxnCtx.InfoSchema = domain.GetDomain(se).InfoSchema()
	compiler := &executor.Compiler{Ctx: se}
	stmtNode, err := s.ParseOneStmt("select 1", "", "")
	c.Check(err, IsNil)
	stmt, err := compiler.Compile(goctx.TODO(), stmtNode)
	c.Check(err, IsNil)
	c.Check(stmt.OriginText(), Equals, "select 1")

	stmtNode, err = s.ParseOneStmt("create table test.t (a int)", "", "")
	c.Check(err, IsNil)
	stmt, err = compiler.Compile(goctx.TODO(), stmtNode)
	c.Check(err, IsNil)
	c.Check(stmt.OriginText(), Equals, "create table test.t (a int)")
}

func (s *testSuite) TestPointGet(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use mysql")
	ctx := tk.Se.(context.Context)
	tests := map[string]bool{
		"select * from help_topic where name='aaa'":         true,
		"select * from help_topic where help_topic_id=1":    true,
		"select * from help_topic where help_category_id=1": false,
	}
	infoSchema := executor.GetInfoSchema(ctx)

	for sqlStr, result := range tests {
		stmtNode, err := s.ParseOneStmt(sqlStr, "", "")
		c.Check(err, IsNil)
		err = plan.Preprocess(ctx, stmtNode, infoSchema, false)
		c.Check(err, IsNil)
		p, err := plan.Optimize(ctx, stmtNode, infoSchema)
		c.Check(err, IsNil)
		ret := executor.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, p)
		c.Assert(ret, Equals, result)
	}
}

func (s *testSuite) TestRow(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (1, 3)")
	tk.MustExec("insert t values (2, 1)")
	tk.MustExec("insert t values (2, 3)")
	result := tk.MustQuery("select * from t where (c, d) < (2,2)")
	result.Check(testkit.Rows("1 1", "1 3", "2 1"))
	result = tk.MustQuery("select * from t where (1,2,3) > (3,2,1)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t where row(1,2,3) > (3,2,1)")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select * from t where (c, d) = (select * from t where (c,d) = (1,1))")
	result.Check(testkit.Rows("1 1"))
	result = tk.MustQuery("select * from t where (c, d) = (select * from t k where (t.c,t.d) = (c,d))")
	result.Check(testkit.Rows("1 1", "1 3", "2 1", "2 3"))
	result = tk.MustQuery("select (1, 2, 3) < (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) <= (2, 3, 3)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select (2, 3, 4) <= (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) <= (2, 1, 4)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select (2, 3, 4) >= (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) = (2, 3, 4)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (2, 3, 4) != (2, 3, 4)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select row(1, 1) in (row(1, 1))")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select row(1, 0) in (row(1, 1))")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select row(1, 1) in (select 1, 1)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select row(1, 1) > row(1, 0)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select row(1, 1) > (select 1, 0)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select 1 > (select 1)")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select (select 1)")
	result.Check(testkit.Rows("1"))
}

func (s *testSuite) TestColumnName(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	rs, err := tk.Exec("select 1 + c, count(*) from t")
	c.Check(err, IsNil)
	fields := rs.Fields()
	c.Check(len(fields), Equals, 2)
	c.Check(fields[0].Column.Name.L, Equals, "1 + c")
	c.Check(fields[1].Column.Name.L, Equals, "count(*)")
	rs, err = tk.Exec("select (c) > all (select c from t) from t")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.L, Equals, "(c) > all (select c from t)")
	tk.MustExec("begin")
	tk.MustExec("insert t values(1,1)")
	rs, err = tk.Exec("select c d, d c from t")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(len(fields), Equals, 2)
	c.Check(fields[0].Column.Name.L, Equals, "d")
	c.Check(fields[1].Column.Name.L, Equals, "c")
	// Test case for query a column of a table.
	// In this case, all attributes have values.
	rs, err = tk.Exec("select c as a from t as t2")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(fields[0].Column.Name.L, Equals, "a")
	c.Check(fields[0].ColumnAsName.L, Equals, "a")
	c.Check(fields[0].Table.Name.L, Equals, "t")
	c.Check(fields[0].TableAsName.L, Equals, "t2")
	c.Check(fields[0].DBName.L, Equals, "test")
	// Test case for query a expression which only using constant inputs.
	// In this case, the table, org_table and database attributes will all be empty.
	rs, err = tk.Exec("select hour(1) as a from t as t2")
	c.Check(err, IsNil)
	fields = rs.Fields()
	c.Check(fields[0].Column.Name.L, Equals, "a")
	c.Check(fields[0].ColumnAsName.L, Equals, "a")
	c.Check(fields[0].Table.Name.L, Equals, "")
	c.Check(fields[0].TableAsName.L, Equals, "")
	c.Check(fields[0].DBName.L, Equals, "")
}

func (s *testSuite) TestSelectVar(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (d int)")
	tk.MustExec("insert into t values(1), (2), (1)")
	// This behavior is different from MySQL.
	result := tk.MustQuery("select @a, @a := d+1 from t")
	result.Check(testkit.Rows("<nil> 2", "2 3", "3 2"))
}

func (s *testSuite) TestHistoryRead(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists history_read")
	tk.MustExec("create table history_read (a int)")
	tk.MustExec("insert history_read values (1)")

	// For mocktikv, safe point is not initialized, we manually insert it for snapshot to use.
	safePointName := "tikv_gc_safe_point"
	safePointValue := "20060102-15:04:05 -0700 MST"
	safePointComment := "All versions after safe point can be accessed. (DO NOT EDIT)"
	updateSafePoint := fmt.Sprintf(`INSERT INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
	ON DUPLICATE KEY
	UPDATE variable_value = '%[2]s', comment = '%[3]s'`, safePointName, safePointValue, safePointComment)
	tk.MustExec(updateSafePoint)

	// Set snapshot to a time before save point will fail.
	_, err := tk.Exec("set @@tidb_snapshot = '2006-01-01 15:04:05.999999'")
	c.Assert(terror.ErrorEqual(err, variable.ErrSnapshotTooOld), IsTrue)
	// SnapshotTS Is not updated if check failed.
	c.Assert(tk.Se.GetSessionVars().SnapshotTS, Equals, uint64(0))

	curVer1, _ := s.store.CurrentVersion()
	time.Sleep(time.Millisecond)
	snapshotTime := time.Now()
	time.Sleep(time.Millisecond)
	curVer2, _ := s.store.CurrentVersion()
	tk.MustExec("insert history_read values (2)")
	tk.MustQuery("select * from history_read").Check(testkit.Rows("1", "2"))
	tk.MustExec("set @@tidb_snapshot = '" + snapshotTime.Format("2006-01-02 15:04:05.999999") + "'")
	ctx := tk.Se.(context.Context)
	snapshotTS := ctx.GetSessionVars().SnapshotTS
	c.Assert(snapshotTS, Greater, curVer1.Ver)
	c.Assert(snapshotTS, Less, curVer2.Ver)
	tk.MustQuery("select * from history_read").Check(testkit.Rows("1"))
	_, err = tk.Exec("insert history_read values (2)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("update history_read set a = 3 where a = 1")
	c.Assert(err, NotNil)
	_, err = tk.Exec("delete from history_read where a = 1")
	c.Assert(err, NotNil)
	tk.MustExec("set @@tidb_snapshot = ''")
	tk.MustQuery("select * from history_read").Check(testkit.Rows("1", "2"))
	tk.MustExec("insert history_read values (3)")
	tk.MustExec("update history_read set a = 4 where a = 3")
	tk.MustExec("delete from history_read where a = 1")

	time.Sleep(time.Millisecond)
	snapshotTime = time.Now()
	time.Sleep(time.Millisecond)
	tk.MustExec("alter table history_read add column b int")
	tk.MustExec("insert history_read values (8, 8), (9, 9)")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2 <nil>", "4 <nil>", "8 8", "9 9"))
	tk.MustExec("set @@tidb_snapshot = '" + snapshotTime.Format("2006-01-02 15:04:05.999999") + "'")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2", "4"))
	tk.MustExec("set @@tidb_snapshot = ''")
	tk.MustQuery("select * from history_read order by a").Check(testkit.Rows("2 <nil>", "4 <nil>", "8 8", "9 9"))
}

func (s *testSuite) TestScanControlSelection(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, index idx_b(b))")
	tk.MustExec("insert into t values (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 3)")
	tk.MustQuery("select (select count(1) k from t s where s.b = t1.c) from t t1").Check(testkit.Rows("3", "3", "1", "0"))
}

func (s *testSuite) TestSimpleDAG(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 3)")
	tk.MustQuery("select a from t").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustQuery("select * from t where a = 4").Check(testkit.Rows("4 2 3"))
	tk.MustQuery("select a from t limit 1").Check(testkit.Rows("1"))
	tk.MustQuery("select a from t order by a desc").Check(testkit.Rows("4", "3", "2", "1"))
	tk.MustQuery("select a from t order by a desc limit 1").Check(testkit.Rows("4"))
	tk.MustQuery("select a from t order by b desc limit 1").Check(testkit.Rows("4"))
	tk.MustQuery("select a from t where a < 3").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t where b > 1").Check(testkit.Rows("4"))
	tk.MustQuery("select a from t where b > 1 and a < 3").Check(testkit.Rows())
	tk.MustQuery("select count(*) from t where b > 1 and a < 3").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t").Check(testkit.Rows("4"))
	tk.MustQuery("select count(*), c from t group by c").Check(testkit.Rows("2 1", "1 2", "1 3"))
	tk.MustQuery("select sum(c) from t group by b").Check(testkit.Rows("4", "3"))
	tk.MustQuery("select avg(a) from t group by b").Check(testkit.Rows("2.0000", "4.0000"))
	tk.MustQuery("select sum(distinct c) from t group by b").Check(testkit.Rows("3", "3"))

	tk.MustExec("create index i on t(c,b)")
	tk.MustQuery("select a from t where c = 1").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t where c = 1 and a < 2").Check(testkit.Rows("1"))
	tk.MustQuery("select a from t where c = 1 order by a limit 1").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t where c = 1 ").Check(testkit.Rows("2"))
	tk.MustExec("create index i1 on t(b)")
	tk.MustQuery("select c from t where b = 2").Check(testkit.Rows("3"))
	tk.MustQuery("select * from t where b = 2").Check(testkit.Rows("4 2 3"))
	tk.MustQuery("select count(*) from t where b = 1").Check(testkit.Rows("3"))
	tk.MustQuery("select * from t where b = 1 and a > 1 limit 1").Check(testkit.Rows("2 1 1"))

	// Test time push down.
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, c1 datetime);")
	tk.MustExec("insert into t values (1, '2015-06-07 12:12:12')")
	tk.MustQuery("select id from t where c1 = '2015-06-07 12:12:12'").Check(testkit.Rows("1"))
}

func (s *testSuite) TestTimestampTimeZone(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (ts timestamp)")
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("insert into t values ('2017-04-27 22:40:42')")
	// The timestamp will get different value if time_zone session variable changes.
	tests := []struct {
		timezone string
		expect   string
	}{
		{"+10:00", "2017-04-28 08:40:42"},
		{"-6:00", "2017-04-27 16:40:42"},
	}
	for _, tt := range tests {
		tk.MustExec(fmt.Sprintf("set time_zone = '%s'", tt.timezone))
		tk.MustQuery("select * from t").Check(testkit.Rows(tt.expect))
	}

	// For issue https://github.com/pingcap/tidb/issues/3467
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`CREATE TABLE t1 (
 	      id bigint(20) NOT NULL AUTO_INCREMENT,
 	      uid int(11) DEFAULT NULL,
 	      datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
 	      ip varchar(128) DEFAULT NULL,
 	    PRIMARY KEY (id),
 	      KEY i_datetime (datetime),
 	      KEY i_userid (uid)
 	    );`)
	tk.MustExec(`INSERT INTO t1 VALUES (123381351,1734,"2014-03-31 08:57:10","127.0.0.1");`)
	r := tk.MustQuery("select datetime from t1;") // Cover TableReaderExec
	r.Check(testkit.Rows("2014-03-31 08:57:10"))
	r = tk.MustQuery("select datetime from t1 where datetime='2014-03-31 08:57:10';")
	r.Check(testkit.Rows("2014-03-31 08:57:10")) // Cover IndexReaderExec
	r = tk.MustQuery("select * from t1 where datetime='2014-03-31 08:57:10';")
	r.Check(testkit.Rows("123381351 1734 2014-03-31 08:57:10 127.0.0.1")) // Cover IndexLookupExec

	// For issue https://github.com/pingcap/tidb/issues/3485
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`CREATE TABLE t1 (
	    id bigint(20) NOT NULL AUTO_INCREMENT,
	    datetime timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	    PRIMARY KEY (id)
	  );`)
	tk.MustExec(`INSERT INTO t1 VALUES (123381351,"2014-03-31 08:57:10");`)
	r = tk.MustQuery(`select * from t1 where datetime="2014-03-31 08:57:10";`)
	r.Check(testkit.Rows("123381351 2014-03-31 08:57:10"))
	tk.MustExec(`alter table t1 add key i_datetime (datetime);`)
	r = tk.MustQuery(`select * from t1 where datetime="2014-03-31 08:57:10";`)
	r.Check(testkit.Rows("123381351 2014-03-31 08:57:10"))
	r = tk.MustQuery(`select * from t1;`)
	r.Check(testkit.Rows("123381351 2014-03-31 08:57:10"))
	r = tk.MustQuery("select datetime from t1 where datetime='2014-03-31 08:57:10';")
	r.Check(testkit.Rows("2014-03-31 08:57:10"))
}

func (s *testSuite) TestTiDBCurrentTS(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))
	tk.MustExec("begin")
	rows := tk.MustQuery("select @@tidb_current_ts").Rows()
	tsStr := rows[0][0].(string)
	c.Assert(tsStr, Equals, fmt.Sprintf("%d", tk.Se.Txn().StartTS()))
	tk.MustExec("begin")
	rows = tk.MustQuery("select @@tidb_current_ts").Rows()
	newTsStr := rows[0][0].(string)
	c.Assert(newTsStr, Equals, fmt.Sprintf("%d", tk.Se.Txn().StartTS()))
	c.Assert(newTsStr, Not(Equals), tsStr)
	tk.MustExec("commit")
	tk.MustQuery("select @@tidb_current_ts").Check(testkit.Rows("0"))

	_, err := tk.Exec("set @@tidb_current_ts = '1'")
	c.Assert(terror.ErrorEqual(err, variable.ErrReadOnly), IsTrue)
}

func (s *testSuite) TestSelectForUpdate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk1 := testkit.NewTestKit(c, s.store)
	tk1.MustExec("use test")
	tk2 := testkit.NewTestKit(c, s.store)
	tk2.MustExec("use test")

	tk.MustExec("drop table if exists t, t1")

	c.Assert(tk.Se.Txn(), IsNil)
	tk.MustExec("create table t (c1 int, c2 int, c3 int)")
	tk.MustExec("insert t values (11, 2, 3)")
	tk.MustExec("insert t values (12, 2, 3)")
	tk.MustExec("insert t values (13, 2, 3)")

	tk.MustExec("create table t1 (c1 int)")
	tk.MustExec("insert t1 values (11)")

	// conflict
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t where c1=11 for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=211 where c1=11")
	tk2.MustExec("commit")

	_, err := tk1.Exec("commit")
	c.Assert(err, NotNil)

	// no conflict for subquery.
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t where exists(select null from t1 where t1.c1=t.c1) for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=211 where c1=12")
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	// not conflict
	tk1.MustExec("begin")
	tk1.MustQuery("select * from t where c1=11 for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=22 where c1=12")
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	// not conflict, auto commit
	tk1.MustExec("set @@autocommit=1;")
	tk1.MustQuery("select * from t where c1=11 for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t set c2=211 where c1=11")
	tk2.MustExec("commit")

	tk1.MustExec("commit")

	// conflict
	tk1.MustExec("begin")
	tk1.MustQuery("select * from (select * from t for update) t join t1 for update")

	tk2.MustExec("begin")
	tk2.MustExec("update t1 set c1 = 13")
	tk2.MustExec("commit")

	_, err = tk1.Exec("commit")
	c.Assert(err, NotNil)

}

func (s *testSuite) TestEmptyEnum(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (e enum('Y', 'N'))")
	tk.MustExec("set sql_mode='STRICT_TRANS_TABLES'")
	_, err := tk.Exec("insert into t values (0)")
	c.Assert(terror.ErrorEqual(err, types.ErrTruncated), IsTrue)
	_, err = tk.Exec("insert into t values ('abc')")
	c.Assert(terror.ErrorEqual(err, types.ErrTruncated), IsTrue)

	tk.MustExec("set sql_mode=''")
	tk.MustExec("insert into t values (0)")
	tk.MustQuery("select * from t").Check(testkit.Rows(""))
	tk.MustExec("insert into t values ('abc')")
	tk.MustQuery("select * from t").Check(testkit.Rows("", ""))
	tk.MustExec("insert into t values (null)")
	tk.MustQuery("select * from t").Check(testkit.Rows("", "", "<nil>"))
}

// TestIssue4024 This tests https://github.com/pingcap/tidb/issues/4024
func (s *testSuite) TestIssue4024(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("create database test2")
	tk.MustExec("use test2")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("update t, test2.t set test2.t.a=2")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustQuery("select * from test2.t").Check(testkit.Rows("2"))
	tk.MustExec("update test.t, test2.t set test.t.a=3")
	tk.MustQuery("select * from t").Check(testkit.Rows("3"))
	tk.MustQuery("select * from test2.t").Check(testkit.Rows("2"))
}

const (
	checkRequestOff          = 0
	checkRequestPriority     = 1
	checkRequestNotFillCache = 2
	checkRequestSyncLog      = 3
)

type checkRequestClient struct {
	tikv.Client
	priority     pb.CommandPri
	notFillCache bool
	mu           struct {
		sync.RWMutex
		checkFlags uint32
		syncLog    bool
	}
}

func (c *checkRequestClient) SendReq(ctx goctx.Context, addr string, req *tikvrpc.Request) (*tikvrpc.Response, error) {
	resp, err := c.Client.SendReq(ctx, addr, req)
	c.mu.RLock()
	checkFlags := c.mu.checkFlags
	c.mu.RUnlock()
	if checkFlags == checkRequestPriority {
		switch req.Type {
		case tikvrpc.CmdCop:
			if c.priority != req.Priority {
				return nil, errors.New("fail to set priority")
			}
		}
	} else if checkFlags == checkRequestNotFillCache {
		if c.notFillCache != req.NotFillCache {
			return nil, errors.New("fail to set not fail cache")
		}
	} else if checkFlags == checkRequestSyncLog {
		switch req.Type {
		case tikvrpc.CmdPrewrite, tikvrpc.CmdCommit:
			c.mu.RLock()
			syncLog := c.mu.syncLog
			c.mu.RUnlock()
			if syncLog != req.SyncLog {
				return nil, errors.New("fail to set sync log")
			}
		}
	}
	return resp, err
}

type testContextOptionSuite struct {
	store kv.Storage
	dom   *domain.Domain
	cli   *checkRequestClient
}

func (s *testContextOptionSuite) SetUpSuite(c *C) {
	cli := &checkRequestClient{}
	hijackClient := func(c tikv.Client) tikv.Client {
		cli.Client = c
		return cli
	}
	s.cli = cli

	var err error
	s.store, err = tikv.NewMockTikvStore(
		tikv.WithHijackClient(hijackClient),
	)
	c.Assert(err, IsNil)
	s.dom, err = tidb.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testContextOptionSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
}

func (s *testContextOptionSuite) TestCoprocessorPriority(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key)")
	tk.MustExec("create table t1 (id int, v int, unique index i_id (id))")
	defer tk.MustExec("drop table t")
	defer tk.MustExec("drop table t1")
	tk.MustExec("insert into t values (1)")

	// Insert some data to make sure plan build IndexLookup for t1.
	for i := 0; i < 10; i++ {
		tk.MustExec(fmt.Sprintf("insert into t1 values (%d, %d)", i, i))
	}

	cli := s.cli
	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestPriority
	cli.mu.Unlock()
	cli.priority = pb.CommandPri_High
	tk.MustQuery("select id from t where id = 1")
	tk.MustQuery("select * from t1 where id = 1")

	cli.priority = pb.CommandPri_Low
	tk.MustQuery("select count(*) from t")

	cli.priority = pb.CommandPri_Low
	tk.MustExec("update t set id = 3")

	cli.priority = pb.CommandPri_Low
	tk.MustExec("delete from t")

	cli.priority = pb.CommandPri_Normal
	tk.MustExec("insert into t values (2)")

	// TODO: Those are not point get, but they should be high priority.
	// cli.priority = pb.CommandPri_High
	// tk.MustExec("delete from t where id = 2")
	// tk.MustExec("update t set id = 2 where id = 1")

	// Test priority specified by SQL statement.
	cli.priority = pb.CommandPri_High
	tk.MustQuery("select HIGH_PRIORITY * from t")

	cli.priority = pb.CommandPri_Low
	tk.MustQuery("select LOW_PRIORITY id from t where id = 1")

	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestOff
	cli.mu.Unlock()
}

func (s *testContextOptionSuite) TestNotFillCache(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key)")
	defer tk.MustExec("drop table t")
	tk.MustExec("insert into t values (1)")

	cli := s.cli
	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestNotFillCache
	cli.mu.Unlock()
	cli.notFillCache = true
	tk.MustQuery("select SQL_NO_CACHE * from t")

	cli.notFillCache = false
	tk.MustQuery("select SQL_CACHE * from t")
	tk.MustQuery("select * from t")

	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestOff
	cli.mu.Unlock()
}

func (s *testContextOptionSuite) TestSyncLog(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	cli := s.cli
	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestSyncLog
	cli.mu.syncLog = true
	cli.mu.Unlock()
	tk.MustExec("create table t (id int primary key)")
	cli.mu.Lock()
	cli.mu.syncLog = false
	cli.mu.Unlock()
	tk.MustExec("insert into t values (1)")

	cli.mu.Lock()
	cli.mu.checkFlags = checkRequestOff
	cli.mu.Unlock()
}

func (s *testSuite) TestHandleTransfer(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, index idx(a))")
	tk.MustExec("insert into t values(1), (2), (4)")
	tk.MustExec("begin")
	tk.MustExec("update t set a = 3 where a = 4")
	// test table scan read whose result need handle.
	tk.MustQuery("select * from t ignore index(idx)").Check(testkit.Rows("1", "2", "3"))
	tk.MustExec("insert into t values(4)")
	// test single read whose result need handle
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("1", "2", "3", "4"))
	tk.MustExec("update t set a = 5 where a = 3")
	tk.MustQuery("select * from t use index(idx)").Check(testkit.Rows("1", "2", "4", "5"))
	tk.MustExec("commit")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("insert into t values(3, 3), (1, 1), (2, 2)")
	// Second test double read.
	tk.MustQuery("select * from t use index(idx) order by a").Check(testkit.Rows("1 1", "2 2", "3 3"))
}

func (s *testSuite) TestBit(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(2))")
	tk.MustExec("insert into t values (0), (1), (2), (3)")
	_, err := tk.Exec("insert into t values (4)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("insert into t values ('a')")
	c.Assert(err, NotNil)
	r, err := tk.Exec("select * from t where c1 = 2")
	c.Assert(err, IsNil)
	row, err := r.Next(goctx.Background())
	c.Assert(err, IsNil)
	c.Assert(types.BinaryLiteral(row.GetBytes(0)), DeepEquals, types.NewBinaryLiteralFromUint(2, -1))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(31))")
	tk.MustExec("insert into t values (0x7fffffff)")
	_, err = tk.Exec("insert into t values (0x80000000)")
	c.Assert(err, NotNil)
	_, err = tk.Exec("insert into t values (0xffffffff)")
	c.Assert(err, NotNil)
	tk.MustExec("insert into t values ('123')")
	tk.MustExec("insert into t values ('1234')")
	_, err = tk.Exec("insert into t values ('12345)")
	c.Assert(err, NotNil)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(62))")
	tk.MustExec("insert into t values ('12345678')")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(61))")
	_, err = tk.Exec("insert into t values ('12345678')")
	c.Assert(err, NotNil)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(32))")
	tk.MustExec("insert into t values (0x7fffffff)")
	tk.MustExec("insert into t values (0xffffffff)")
	_, err = tk.Exec("insert into t values (0x1ffffffff)")
	c.Assert(err, NotNil)
	tk.MustExec("insert into t values ('1234')")
	_, err = tk.Exec("insert into t values ('12345')")
	c.Assert(err, NotNil)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 bit(64))")
	tk.MustExec("insert into t values (0xffffffffffffffff)")
	tk.MustExec("insert into t values ('12345678')")
	_, err = tk.Exec("insert into t values ('123456789')")
	c.Assert(err, NotNil)
}

func (s *testSuite) TestEnum(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c enum('a', 'b', 'c'))")
	tk.MustExec("insert into t values ('a'), (2), ('c')")
	tk.MustQuery("select * from t where c = 'a'").Check(testkit.Rows("a"))

	tk.MustQuery("select c + 1 from t where c = 2").Check(testkit.Rows("3"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values ()")
	tk.MustExec("insert into t values (null), ('1')")
	tk.MustQuery("select c + 1 from t where c = 1").Check(testkit.Rows("2"))
}

func (s *testSuite) TestSet(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c set('a', 'b', 'c'))")
	tk.MustExec("insert into t values ('a'), (2), ('c'), ('a,b'), ('b,a')")
	tk.MustQuery("select * from t where c = 'a'").Check(testkit.Rows("a"))

	tk.MustQuery("select * from t where c = 'a,b'").Check(testkit.Rows("a,b", "a,b"))

	tk.MustQuery("select c + 1 from t where c = 2").Check(testkit.Rows("3"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values ()")
	tk.MustExec("insert into t values (null), ('1')")
	tk.MustQuery("select c + 1 from t where c = 1").Check(testkit.Rows("2"))
}

func (s *testSuite) TestSubqueryInValues(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, name varchar(20))")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (gid int)")

	tk.MustExec("insert into t1 (gid) value (1)")
	tk.MustExec("insert into t (id, name) value ((select gid from t1) ,'asd')")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 asd"))
}

func (s *testSuite) TestEnhancedRangeAccess(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values(1, 2), (2, 1)")
	tk.MustQuery("select * from t where (a = 1 and b = 2) or (a = 2 and b = 1)").Check(testkit.Rows("1 2", "2 1"))
	tk.MustQuery("select * from t where (a = 1 and b = 1) or (a = 2 and b = 2)").Check(nil)
}

// TestMaxInt64Handle Issue #4810
func (s *testSuite) TestMaxInt64Handle(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id bigint, PRIMARY KEY (id))")
	tk.MustExec("insert into t values(9223372036854775807)")
	tk.MustExec("select * from t where id = 9223372036854775807")
	tk.MustQuery("select * from t where id = 9223372036854775807;").Check(testkit.Rows("9223372036854775807"))
	tk.MustQuery("select * from t").Check(testkit.Rows("9223372036854775807"))
	_, err := tk.Exec("insert into t values(9223372036854775807)")
	c.Assert(err, NotNil)
	tk.MustExec("delete from t where id = 9223372036854775807")
	tk.MustQuery("select * from t").Check(nil)
}

func (s *testSuite) TestTableScanWithPointRanges(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, PRIMARY KEY (id))")
	tk.MustExec("insert into t values(1), (5), (10)")
	tk.MustQuery("select * from t where id in(1, 2, 10)").Check(testkit.Rows("1", "10"))
}

func (s *testSuite) TestUnsignedPk(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id bigint unsigned primary key)")
	var num1, num2 uint64 = math.MaxInt64 + 1, math.MaxInt64 + 2
	tk.MustExec(fmt.Sprintf("insert into t values(%v), (%v), (1), (2)", num1, num2))
	num1Str := strconv.FormatUint(num1, 10)
	num2Str := strconv.FormatUint(num2, 10)
	tk.MustQuery("select * from t order by id").Check(testkit.Rows("1", "2", num1Str, num2Str))
	tk.MustQuery("select * from t where id not in (2)").Check(testkit.Rows(num1Str, num2Str, "1"))
}

func (s *testSuite) TestEarlyClose(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table earlyclose (id int primary key)")

	// Insert 1000 rows.
	var values []string
	for i := 0; i < 1000; i++ {
		values = append(values, fmt.Sprintf("(%d)", i))
	}
	tk.MustExec("insert earlyclose values " + strings.Join(values, ","))

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("earlyclose"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	// Split the table.
	s.cluster.SplitTable(s.mvccStore, tblID, 500)

	goCtx := goctx.Background()
	for i := 0; i < 500; i++ {
		rss, err := tk.Se.Execute(goCtx, "select * from earlyclose order by id")
		c.Assert(err, IsNil)
		rs := rss[0]
		_, err = rs.Next(goCtx)
		c.Assert(err, IsNil)
		rs.Close()
	}
}
