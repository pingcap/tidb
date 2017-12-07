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

package mysql_test

import (
	"flag"
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/mocktikv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	goctx "golang.org/x/net/context"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testMySQLConstSuite{})

type testMySQLConstSuite struct {
	cluster   *mocktikv.Cluster
	mvccStore *mocktikv.MvccStore
	store     kv.Storage
	*parser.Parser
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in executor test")

func (s *testMySQLConstSuite) SetUpSuite(c *C) {
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

func (s *testMySQLConstSuite) TestGetSQLMode(c *C) {
	defer testleak.AfterTest(c)()

	positiveCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE"},
		{",,NO_ZERO_DATE"},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE"},
		{""},
		{", "},
		{","},
	}

	for _, t := range positiveCases {
		_, err := mysql.GetSQLMode(mysql.FormatSQLModeStr(t.arg))
		c.Assert(err, IsNil)
	}

	negativeCases := []struct {
		arg string
	}{
		{"NO_ZERO_DATE, NO_ZERO_IN_DATE"},
		{"NO_ZERO_DATE,adfadsdfasdfads"},
		{", ,NO_ZERO_DATE"},
		{" ,"},
	}

	for _, t := range negativeCases {
		_, err := mysql.GetSQLMode(mysql.FormatSQLModeStr(t.arg))
		c.Assert(err, NotNil)
	}
}

func (s *testMySQLConstSuite) TestSQLMode(c *C) {
	defer testleak.AfterTest(c)()

	tests := []struct {
		arg                           string
		hasNoZeroDateMode             bool
		hasNoZeroInDateMode           bool
		hasErrorForDivisionByZeroMode bool
	}{
		{"NO_ZERO_DATE", true, false, false},
		{"NO_ZERO_IN_DATE", false, true, false},
		{"ERROR_FOR_DIVISION_BY_ZERO", false, false, true},
		{"NO_ZERO_IN_DATE,NO_ZERO_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE", true, true, false},
		{"NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", true, true, true},
		{"NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO", false, true, true},
		{"", false, false, false},
	}

	for _, t := range tests {
		sqlMode, _ := mysql.GetSQLMode(t.arg)
		c.Assert(sqlMode.HasNoZeroDateMode(), Equals, t.hasNoZeroDateMode)
		c.Assert(sqlMode.HasNoZeroInDateMode(), Equals, t.hasNoZeroInDateMode)
		c.Assert(sqlMode.HasErrorForDivisionByZeroMode(), Equals, t.hasErrorForDivisionByZeroMode)
	}
}

func (s *testMySQLConstSuite) TestRealAsFloatMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a real);")
	result := tk.MustQuery("desc t")
	c.Check(result.Rows(), HasLen, 1)
	row := result.Rows()[0]
	c.Assert(row[1], Equals, "double")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("set sql_mode='REAL_AS_FLOAT'")
	tk.MustExec("create table t (a real)")
	result = tk.MustQuery("desc t")
	c.Check(result.Rows(), HasLen, 1)
	row = result.Rows()[0]
	c.Assert(row[1], Equals, "float")
}

func (s *testMySQLConstSuite) TestPipesAsConcatMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("SET sql_mode='PIPES_AS_CONCAT';")
	r := tk.MustQuery(`SELECT 'hello' || 'world';`)
	r.Check(testkit.Rows("helloworld"))
}

func (s *testMySQLConstSuite) TestNoUnsignedSubtractionMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	goCtx := goctx.Background()
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION'")
	r := tk.MustQuery("SELECT CAST(0 as UNSIGNED) - 1;")
	r.Check(testkit.Rows("-1"))
	rs, _ := tk.Exec("SELECT CAST(18446744073709551615 as UNSIGNED) - 1;")
	_, err := tidb.GetRows4Test(goCtx, rs)
	c.Assert(err, NotNil)
	c.Assert(rs.Close(), IsNil)
	rs, _ = tk.Exec("SELECT 1 - CAST(18446744073709551615 as UNSIGNED);")
	_, err = tidb.GetRows4Test(goCtx, rs)
	c.Assert(err, NotNil)
	c.Assert(rs.Close(), IsNil)
	rs, _ = tk.Exec("SELECT CAST(-1 as UNSIGNED) - 1")
	_, err = tidb.GetRows4Test(goCtx, rs)
	c.Assert(err, NotNil)
	c.Assert(rs.Close(), IsNil)
	rs, _ = tk.Exec("SELECT CAST(9223372036854775808 as UNSIGNED) - 1")
	_, err = tidb.GetRows4Test(goCtx, rs)
	c.Assert(err, NotNil)
	c.Assert(rs.Close(), IsNil)
}

func (s *testMySQLConstSuite) TestHighNotPrecedenceMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int);")
	tk.MustExec("insert into t1 values (0),(1),(NULL);")
	r := tk.MustQuery(`SELECT * FROM t1 WHERE NOT a BETWEEN 2 AND 3;`)
	r.Check(testkit.Rows("0", "1"))
	r = tk.MustQuery(`SELECT NOT 1 BETWEEN -5 AND 5;`)
	r.Check(testkit.Rows("0"))
	tk.MustExec("set sql_mode='high_not_precedence';")
	r = tk.MustQuery(`SELECT * FROM t1 WHERE NOT a BETWEEN 2 AND 3;`)
	r.Check(testkit.Rows())
	r = tk.MustQuery(`SELECT NOT 1 BETWEEN -5 AND 5;`)
	r.Check(testkit.Rows("1"))
}

func (s *testMySQLConstSuite) TestIgnoreSpaceMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode=''")
	tk.MustExec("CREATE TABLE COUNT (a bigint);")
	tk.MustExec("DROP TABLE COUNT;")
	tk.MustExec("CREATE TABLE `COUNT` (a bigint);")
	tk.MustExec("DROP TABLE COUNT;")
	_, err := tk.Exec("CREATE TABLE COUNT(a bigint);")
	c.Assert(err, NotNil)
	tk.MustExec("CREATE TABLE test.COUNT(a bigint);")
	tk.MustExec("DROP TABLE COUNT;")

	tk.MustExec("CREATE TABLE BIT_AND (a bigint);")
	tk.MustExec("DROP TABLE BIT_AND;")
	tk.MustExec("CREATE TABLE `BIT_AND` (a bigint);")
	tk.MustExec("DROP TABLE BIT_AND;")
	_, err = tk.Exec("CREATE TABLE BIT_AND(a bigint);")
	c.Assert(err, NotNil)
	tk.MustExec("CREATE TABLE test.BIT_AND(a bigint);")
	tk.MustExec("DROP TABLE BIT_AND;")

	tk.MustExec("CREATE TABLE NOW (a bigint);")
	tk.MustExec("DROP TABLE NOW;")
	tk.MustExec("CREATE TABLE `NOW` (a bigint);")
	tk.MustExec("DROP TABLE NOW;")
	_, err = tk.Exec("CREATE TABLE NOW(a bigint);")
	c.Assert(err, NotNil)
	tk.MustExec("CREATE TABLE test.NOW(a bigint);")
	tk.MustExec("DROP TABLE NOW;")

	tk.MustExec("set sql_mode='IGNORE_SPACE'")
	_, err = tk.Exec("CREATE TABLE COUNT (a bigint);")
	c.Assert(err, NotNil)
	tk.MustExec("CREATE TABLE `COUNT` (a bigint);")
	tk.MustExec("DROP TABLE COUNT;")
	_, err = tk.Exec("CREATE TABLE COUNT(a bigint);")
	c.Assert(err, NotNil)
	tk.MustExec("CREATE TABLE test.COUNT(a bigint);")
	tk.MustExec("DROP TABLE COUNT;")

	_, err = tk.Exec("CREATE TABLE BIT_AND (a bigint);")
	c.Assert(err, NotNil)
	tk.MustExec("CREATE TABLE `BIT_AND` (a bigint);")
	tk.MustExec("DROP TABLE BIT_AND;")
	_, err = tk.Exec("CREATE TABLE BIT_AND(a bigint);")
	c.Assert(err, NotNil)
	tk.MustExec("CREATE TABLE test.BIT_AND(a bigint);")
	tk.MustExec("DROP TABLE BIT_AND;")

	_, err = tk.Exec("CREATE TABLE NOW (a bigint);")
	c.Assert(err, NotNil)
	tk.MustExec("CREATE TABLE `NOW` (a bigint);")
	tk.MustExec("DROP TABLE NOW;")
	_, err = tk.Exec("CREATE TABLE NOW(a bigint);")
	c.Assert(err, NotNil)
	tk.MustExec("CREATE TABLE test.NOW(a bigint);")
	tk.MustExec("DROP TABLE NOW;")

}

func (s *testMySQLConstSuite) TestPadCharToFullLengthMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// test type `CHAR(n)`
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a char(10));")
	tk.MustExec("insert into t1 values ('xy');")
	tk.MustExec("set sql_mode='';")
	r := tk.MustQuery(`SELECT a='xy        ', char_length(a) FROM t1;`)
	r.Check(testkit.Rows("0 2"))
	r = tk.MustQuery(`SELECT count(*) FROM t1 WHERE a='xy        ';`)
	r.Check(testkit.Rows("0"))
	tk.MustExec("set sql_mode='PAD_CHAR_TO_FULL_LENGTH';")
	r = tk.MustQuery(`SELECT a='xy        ', char_length(a) FROM t1;`)
	r.Check(testkit.Rows("1 10"))
	r = tk.MustQuery(`SELECT count(*) FROM t1 WHERE a='xy        ';`)
	r.Check(testkit.Rows("1"))

	// test type `VARCHAR(n)`
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a varchar(10));")
	tk.MustExec("insert into t1 values ('xy');")
	tk.MustExec("set sql_mode='';")
	r = tk.MustQuery(`SELECT a='xy        ', char_length(a) FROM t1;`)
	r.Check(testkit.Rows("0 2"))
	r = tk.MustQuery(`SELECT count(*) FROM t1 WHERE a='xy        ';`)
	r.Check(testkit.Rows("0"))
	tk.MustExec("set sql_mode='PAD_CHAR_TO_FULL_LENGTH';")
	r = tk.MustQuery(`SELECT a='xy        ', char_length(a) FROM t1;`)
	r.Check(testkit.Rows("0 2"))
}

func (s *testMySQLConstSuite) TestNoBackslashEscapesMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set sql_mode=''")
	r := tk.MustQuery("SELECT '\\\\'")
	r.Check(testkit.Rows("\\"))
	tk.MustExec("set sql_mode='NO_BACKSLASH_ESCAPES'")
	r = tk.MustQuery("SELECT '\\\\'")
	r.Check(testkit.Rows("\\\\"))
}
