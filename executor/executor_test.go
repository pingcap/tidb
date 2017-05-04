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
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/inspectkv"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	store kv.Storage
	*parser.Parser
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in executor test")

func (s *testSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		store, err := tikv.NewMockTikvStore("")
		c.Assert(err, IsNil)
		s.store = store
		tidb.SetSchemaLease(0)
	} else {
		store, err := tidb.NewStore("memory://test/test")
		c.Assert(err, IsNil)
		s.store = store
	}
	_, err := tidb.BootstrapSession(s.store)
	c.Assert(err, IsNil)
	logLevel := os.Getenv("log_level")
	log.SetLevelByString(logLevel)
}

func (s *testSuite) TearDownSuite(c *C) {
	s.store.Close()
}

func (s *testSuite) cleanEnv(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testSuite) TestAdmin(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists admin_test")
	tk.MustExec("create table admin_test (c1 int, c2 int, c3 int default 1, index (c1))")
	tk.MustExec("insert admin_test (c1) values (1),(2),(NULL)")
	r, err := tk.Exec("admin show ddl")
	c.Assert(err, IsNil)
	row, err := r.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data, HasLen, 6)
	txn, err := s.store.Begin()
	c.Assert(err, IsNil)
	ddlInfo, err := inspectkv.GetDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(row.Data[0].GetInt64(), Equals, ddlInfo.SchemaVer)
	rowOwnerInfos := strings.Split(row.Data[1].GetString(), ",")
	ownerInfos := strings.Split(ddlInfo.Owner.String(), ",")
	c.Assert(rowOwnerInfos[0], Equals, ownerInfos[0])
	c.Assert(row.Data[2].GetString(), Equals, "")
	bgInfo, err := inspectkv.GetBgDDLInfo(txn)
	c.Assert(err, IsNil)
	c.Assert(row.Data[3].GetInt64(), Equals, bgInfo.SchemaVer)
	rowOwnerInfos = strings.Split(row.Data[4].GetString(), ",")
	ownerInfos = strings.Split(bgInfo.Owner.String(), ",")
	c.Assert(rowOwnerInfos[0], Equals, ownerInfos[0])
	row, err = r.Next()
	c.Assert(err, IsNil)
	c.Assert(row, IsNil)

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
	domain := sessionctx.GetDomain(ctx)
	is := domain.InfoSchema()
	c.Assert(is, NotNil)
	tb, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("admin_test"))
	c.Assert(err, IsNil)
	c.Assert(tb.Indices(), HasLen, 1)
	_, err = tb.Indices()[0].Create(txn, types.MakeDatums(int64(10)), 1)
	c.Assert(err, IsNil)
	err = txn.Commit()
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
		err1 = ctx.Txn().Commit()
		c.Assert(err1, IsNil)
		r := tk.MustQuery(selectSQL)
		r.Check(testutil.RowsWithSep("|", tt.expected...))
		tk.MustExec(deleteSQL)
	}
}

func (s *testSuite) TestSelectWithoutFrom(c *C) {
	defer testleak.AfterTest(c)()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	r := tk.MustQuery("select 1 + 2*3")
	r.Check(testkit.Rows("7"))

	r = tk.MustQuery(`select _utf8"string";`)
	r.Check(testkit.Rows("string"))
}

func (s *testSuite) TestSelectLimit(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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

func (s *testSuite) TestDAG(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk.MustExec("use test")
	tk.MustExec("create table select_dag(id int not null default 1, name varchar(255));")

	tk.MustExec("insert INTO select_dag VALUES (1, \"hello\");")
	tk.MustExec("insert INTO select_dag VALUES (2, \"hello\");")
	tk.MustExec("insert INTO select_dag VALUES (3, \"hello\");")
	tk.CheckExecResult(1, 0)

	r := tk.MustQuery("select * from select_dag;")
	r.Check(testkit.Rows("1 hello", "2 hello", "3 hello"))

	r = tk.MustQuery("select * from select_dag where id > 1;")
	r.Check(testkit.Rows("2 hello", "3 hello"))

	// for limit
	r = tk.MustQuery("select * from select_dag limit 1;")
	r.Check(testkit.Rows("1 hello"))
	r = tk.MustQuery("select * from select_dag limit 0;")
	r.Check(testkit.Rows())
	r = tk.MustQuery("select * from select_dag limit 5;")
	r.Check(testkit.Rows("1 hello", "2 hello", "3 hello"))
}

func (s *testSuite) TestSelectOrderBy(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
	tk.MustExec("begin")
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
}

func (s *testSuite) TestSelectErrorRow(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (
		create_at datetime NOT NULL DEFAULT '1000-01-01 00:00:00',
		finish_at datetime NOT NULL DEFAULT '1000-01-01 00:00:00');`)
	tk.MustExec(`insert into t values ('2016-02-13 15:32:24',  '2016-02-11 17:23:22');`)
	rs, err := tk.Exec(`select timediff(finish_at, create_at) from t;`)
	c.Assert(err, IsNil)
	row, err := rs.Next()
	c.Assert(err, IsNil)
	row.Data[0].GetMysqlDuration().String()
}

// TestIssue345 is related with https://github.com/pingcap/tidb/issues/345
func (s *testSuite) TestIssue345(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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

func (s *testSuite) TestUnion(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	testSQL := `select 1 union select 0;`
	tk.MustExec(testSQL)

	testSQL = `drop table if exists union_test; create table union_test(id int);`
	tk.MustExec(testSQL)

	testSQL = `drop table if exists union_test;`
	tk.MustExec(testSQL)
	testSQL = `create table union_test(id int);`
	tk.MustExec(testSQL)
	testSQL = `insert union_test values (1),(2); select id from union_test union select 1;`
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
}

func (s *testSuite) TestIn(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
}

func (s *testSuite) TestIndexReverseOrder(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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

func (s *testSuite) TestBuiltin(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// for is true
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, index idx_b (b))")
	tk.MustExec("insert t values (1, 1)")
	tk.MustExec("insert t values (2, 2)")
	tk.MustExec("insert t values (3, 2)")
	result := tk.MustQuery("select * from t where b is true")
	result.Check(testkit.Rows("1 1", "2 2", "3 2"))
	result = tk.MustQuery("select all + a from t where a = 1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from t where a is false")
	result.Check(nil)
	result = tk.MustQuery("select * from t where a is not true")
	result.Check(nil)
	// for in
	result = tk.MustQuery("select * from t where b in (a)")
	result.Check(testkit.Rows("1 1", "2 2"))
	result = tk.MustQuery("select * from t where b not in (a)")
	result.Check(testkit.Rows("3 2"))

	// test cast
	result = tk.MustQuery("select cast(1 as decimal(3,2))")
	result.Check(testkit.Rows("1.00"))
	result = tk.MustQuery("select cast('1991-09-05 11:11:11' as datetime)")
	result.Check(testkit.Rows("1991-09-05 11:11:11"))
	result = tk.MustQuery("select cast(cast('1991-09-05 11:11:11' as datetime) as char)")
	result.Check(testkit.Rows("1991-09-05 11:11:11"))
	result = tk.MustQuery("select cast('11:11:11' as time)")
	result.Check(testkit.Rows("11:11:11"))
	result = tk.MustQuery("select * from t where a > cast(2 as decimal)")
	result.Check(testkit.Rows("3 2"))

	// test unhex and hex
	result = tk.MustQuery("select unhex('4D7953514C')")
	result.Check(testkit.Rows("MySQL"))
	result = tk.MustQuery("select unhex(hex('string'))")
	result.Check(testkit.Rows("string"))
	result = tk.MustQuery("select unhex('ggg')")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select unhex(-1)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select hex(unhex('1267'))")
	result.Check(testkit.Rows("1267"))
	result = tk.MustQuery("select hex(unhex(1267))")
	result.Check(testkit.Rows("1267"))

	// select from_unixtime
	result = tk.MustQuery("select from_unixtime(1451606400)")
	unixTime := time.Unix(1451606400, 0).String()[:19]
	result.Check(testkit.Rows(unixTime))
	result = tk.MustQuery("select from_unixtime(1451606400.123456)")
	unixTime = time.Unix(1451606400, 123456000).String()[:26]
	result.Check(testkit.Rows(unixTime))
	result = tk.MustQuery("select from_unixtime(1451606400.1234567)")
	unixTime = time.Unix(1451606400, 123456700).Round(time.Microsecond).Format("2006-01-02 15:04:05.000000")[:26]
	result.Check(testkit.Rows(unixTime))
	result = tk.MustQuery("select from_unixtime(1451606400.999999)")
	unixTime = time.Unix(1451606400, 999999000).String()[:26]
	result.Check(testkit.Rows(unixTime))

	// test strcmp
	result = tk.MustQuery("select strcmp('abc', 'def')")
	result.Check(testkit.Rows("-1"))
	result = tk.MustQuery("select strcmp('abc', 'aba')")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select strcmp('abc', 'abc')")
	result.Check(testkit.Rows("0"))

	// for case
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(255), b int)")
	tk.MustExec("insert t values ('str1', 1)")
	result = tk.MustQuery("select * from t where a = case b when 1 then 'str1' when 2 then 'str2' end")
	result.Check(testkit.Rows("str1 1"))
	result = tk.MustQuery("select * from t where a = case b when 1 then 'str2' when 2 then 'str3' end")
	result.Check(nil)
	tk.MustExec("insert t values ('str2', 2)")
	result = tk.MustQuery("select * from t where a = case b when 2 then 'str2' when 3 then 'str3' end")
	result.Check(testkit.Rows("str2 2"))
	tk.MustExec("insert t values ('str3', 3)")
	result = tk.MustQuery("select * from t where a = case b when 4 then 'str4' when 5 then 'str5' else 'str3' end")
	result.Check(testkit.Rows("str3 3"))
	result = tk.MustQuery("select * from t where a = case b when 4 then 'str4' when 5 then 'str5' else 'str6' end")
	result.Check(nil)
	result = tk.MustQuery("select * from t where a = case  when b then 'str3' when 1 then 'str1' else 'str2' end")
	result.Check(testkit.Rows("str3 3"))
	tk.MustExec("delete from t")
	tk.MustExec("insert t values ('str2', 0)")
	result = tk.MustQuery("select * from t where a = case  when b then 'str3' when 0 then 'str1' else 'str2' end")
	result.Check(testkit.Rows("str2 0"))
	tk.MustExec("insert t values ('str1', null)")
	result = tk.MustQuery("select * from t where a = case b when null then 'str3' when 10 then 'str1' else 'str2' end")
	result.Check(testkit.Rows("str2 0"))
	result = tk.MustQuery("select * from t where a = case null when b then 'str3' when 10 then 'str1' else 'str2' end")
	result.Check(testkit.Rows("str2 0"))
	result = tk.MustQuery("select cast(1234 as char(3))")
	result.Check(testkit.Rows("123"))

	// testCase is for like and regexp
	type testCase struct {
		pattern string
		val     string
		result  int
	}
	patternMatching := func(c *C, tk *testkit.TestKit, queryOp string, data []testCase) {
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (a varchar(255), b int)")
		for i, d := range data {
			tk.MustExec(fmt.Sprintf("insert into t values('%s', %d)", d.val, i))
			result := tk.MustQuery(fmt.Sprintf("select * from t where a %s '%s'", queryOp, d.pattern))
			if d.result == 1 {
				rowStr := fmt.Sprintf("%s %d", d.val, i)
				result.Check(testkit.Rows(rowStr))
			} else {
				result.Check(nil)
			}
			tk.MustExec(fmt.Sprintf("delete from t where b = %d", i))
		}
	}
	// for like
	likeTests := []testCase{
		{"a", "a", 1},
		{"a", "b", 0},
		{"aA", "Aa", 1},
		{"aA%", "aAab", 1},
		{"aA_", "Aaab", 0},
		{"aA_", "Aab", 1},
		{"", "", 1},
		{"", "a", 0},
	}
	patternMatching(c, tk, "like", likeTests)
	// for regexp
	likeTests = []testCase{
		{"^$", "a", 0},
		{"a", "a", 1},
		{"a", "b", 0},
		{"aA", "aA", 1},
		{".", "a", 1},
		{"^.$", "ab", 0},
		{"..", "b", 0},
		{".ab", "aab", 1},
		{"ab.", "abcd", 1},
		{".*", "abcd", 1},
	}
	patternMatching(c, tk, "regexp", likeTests)

	// for found_rows
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustQuery("select * from t") // Test XSelectTableExec
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("1")) // Last query is found_rows(), it returns 1 row with value 0
	tk.MustExec("insert t values (1),(2),(2)")
	tk.MustQuery("select * from t")
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("3"))
	tk.MustQuery("select * from t where a = 0")
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("0"))
	tk.MustQuery("select * from t where a = 1")
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("1"))
	tk.MustQuery("select * from t where a like '2'") // Test SelectionExec
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("2"))
	tk.MustQuery("show tables like 't'")
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t") // Test ProjectionExec
	result = tk.MustQuery("select found_rows()")
	result.Check(testkit.Rows("1"))
}

func (s *testSuite) TestToPBExpr(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
	result.Check(testkit.Rows("11:11:12 11:11:12", "11:11:13 11:11:13"))
	result = tk.MustQuery("select * from t where b > '11:11:11.5'")
	result.Check(testkit.Rows("11:11:12 11:11:12", "11:11:13 11:11:13"))
}

func (s *testSuite) TestSQLMode(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
	defer testleak.AfterTest(c)()
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
	defer testleak.AfterTest(c)()
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
}

func (s *testSuite) TestAdapterStatement(c *C) {
	defer testleak.AfterTest(c)()
	se, err := tidb.CreateSession(s.store)
	c.Check(err, IsNil)
	se.GetSessionVars().TxnCtx.InfoSchema = sessionctx.GetDomain(se).InfoSchema()
	compiler := &executor.Compiler{}
	ctx := se.(context.Context)
	stmtNode, err := s.ParseOneStmt("select 1", "", "")
	c.Check(err, IsNil)
	stmt, err := compiler.Compile(ctx, stmtNode)
	c.Check(err, IsNil)
	c.Check(stmt.OriginText(), Equals, "select 1")

	stmtNode, err = s.ParseOneStmt("create table t (a int)", "", "")
	c.Check(err, IsNil)
	stmt, err = compiler.Compile(ctx, stmtNode)
	c.Check(err, IsNil)
	c.Check(stmt.OriginText(), Equals, "create table t (a int)")
}

func (s *testSuite) TestPointGet(c *C) {
	defer testleak.AfterTest(c)()
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
		err = plan.Preprocess(stmtNode, infoSchema, ctx)
		c.Check(err, IsNil)
		// Validate should be after NameResolve.
		err = plan.Validate(stmtNode, false)
		c.Check(err, IsNil)
		plan, err := plan.Optimize(ctx, stmtNode, infoSchema)
		c.Check(err, IsNil)
		ret := executor.IsPointGetWithPKOrUniqueKeyByAutoCommit(ctx, plan)
		c.Assert(ret, Equals, result)
	}

}

func (s *testSuite) TestRow(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
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
}

func (s *testSuite) TestColumnName(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c int, d int)")
	rs, err := tk.Exec("select 1 + c, count(*) from t")
	c.Check(err, IsNil)
	fields, err := rs.Fields()
	c.Check(err, IsNil)
	c.Check(len(fields), Equals, 2)
	c.Check(fields[0].Column.Name.L, Equals, "1 + c")
	c.Check(fields[1].Column.Name.L, Equals, "count(*)")
	rs, err = tk.Exec("select (c) > all (select c from t) from t")
	c.Check(err, IsNil)
	fields, err = rs.Fields()
	c.Check(err, IsNil)
	c.Check(len(fields), Equals, 1)
	c.Check(fields[0].Column.Name.L, Equals, "(c) > all (select c from t)")
	tk.MustExec("begin")
	tk.MustExec("insert t values(1,1)")
	rs, err = tk.Exec("select c d, d c from t")
	c.Check(err, IsNil)
	fields, err = rs.Fields()
	c.Check(err, IsNil)
	c.Check(len(fields), Equals, 2)
	c.Check(fields[0].Column.Name.L, Equals, "d")
	c.Check(fields[1].Column.Name.L, Equals, "c")
}

func (s *testSuite) TestSelectVar(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (d int)")
	tk.MustExec("insert into t values(1), (2), (1)")
	result := tk.MustQuery("select @a, @a := d+1 from t")
	result.Check(testkit.Rows("<nil> 2", "<nil> 3", "<nil> 2"))
	result = tk.MustQuery("select @a, @a := d+1 from t")
	result.Check(testkit.Rows("2 2", "2 3", "3 2"))
}

func (s *testSuite) TestHistoryRead(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists history_read")
	tk.MustExec("create table history_read (a int)")
	tk.MustExec("insert history_read values (1)")
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
	_, err := tk.Exec("insert history_read values (2)")
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
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int, index idx_b(b))")
	tk.MustExec("insert into t values (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 3)")
	tk.MustQuery("select (select count(1) k from t s where s.b = t1.c) from t t1").Check(testkit.Rows("3", "3", "1", "0"))
}

func (s *testSuite) TestSimpleDAG(c *C) {
	defer func() {
		plan.UseDAGPlanBuilder = false
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, c int)")
	tk.MustExec("insert into t values (1, 1, 1), (2, 1, 1), (3, 1, 2), (4, 2, 3)")
	plan.UseDAGPlanBuilder = true
	tk.MustQuery("select a from t").Check(testkit.Rows("1", "2", "3", "4"))
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

	tk.MustExec("create index i on t(c,b)")
	tk.MustQuery("select a from t where c = 1").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t where c = 1 and a < 2").Check(testkit.Rows("1"))
	tk.MustQuery("select a from t where c = 1 order by a limit 1").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t where c = 1 ").Check(testkit.Rows("2"))
}

func (s *testSuite) TestConvertToBit(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (a bit(64))")
	tk.MustExec("create table t1 (a varchar(2))")
	tk.MustExec(`insert t1 value ('10')`)
	tk.MustExec(`insert t select a from t1`)
	tk.MustQuery("select a+0 from t").Check(testkit.Rows("12592"))

	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (a bit(64))")
	tk.MustExec("create table t1 (a binary(2))")
	tk.MustExec(`insert t1 value ('10')`)
	tk.MustExec(`insert t select a from t1`)
	tk.MustQuery("select a+0 from t").Check(testkit.Rows("12592"))

	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (a bit(64))")
	tk.MustExec("create table t1 (a datetime)")
	tk.MustExec(`insert t1 value ('09-01-01')`)
	tk.MustExec(`insert t select a from t1`)
	tk.MustQuery("select a+0 from t").Check(testkit.Rows("20090101000000"))
}
