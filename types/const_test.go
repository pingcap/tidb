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

package types_test

import (
	"context"
	"flag"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/tikv/client-go/v2/testutils"
)

var _ = Suite(&testMySQLConstSuite{})

type testMySQLConstSuite struct {
	cluster testutils.Cluster
	store   kv.Storage
	dom     *domain.Domain
	*parser.Parser
}

var mockTikv = flag.Bool("mockTikv", true, "use mock tikv store in executor test")

func (s *testMySQLConstSuite) SetUpSuite(c *C) {
	s.Parser = parser.New()
	flag.Lookup("mockTikv")
	useMockTikv := *mockTikv
	if useMockTikv {
		store, err := mockstore.NewMockStore(
			mockstore.WithClusterInspector(func(c testutils.Cluster) {
				mockstore.BootstrapWithSingleStore(c)
				s.cluster = c
			}),
		)
		c.Assert(err, IsNil)
		s.store = store
		session.SetSchemaLease(0)
		session.DisableStats4Test()
	}
	var err error
	s.dom, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testMySQLConstSuite) TearDownSuite(c *C) {
	s.dom.Close()
	s.store.Close()
	testleak.AfterTest(c)()
}

func (s *testMySQLConstSuite) TestGetSQLMode(c *C) {
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

func (s *testMySQLConstSuite) TestIssue22387(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set sql_mode=''")
	err := tk.QueryToErr("select 12 - cast(15 as unsigned);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(12 - 15)'")

	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION';")
	tk.MustQuery("select 12 - cast(15 as unsigned);").Check(testkit.Rows("-3"))
}

func (s *testMySQLConstSuite) TestIssue22389(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION';")
	tk.MustExec("DROP TABLE IF EXISTS tb5")
	tk.MustExec("create table tb5(a bigint, b bigint);")
	tk.MustExec("insert into tb5 values (10, -9223372036854775808);")
	err := tk.QueryToErr("select a - b from tb5;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(test.tb5.a - test.tb5.b)'")
	tk.MustExec("set sql_mode=''")
	err = tk.QueryToErr("select a - b from tb5;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(test.tb5.a - test.tb5.b)'")
}

func (s *testMySQLConstSuite) TestIssue22390(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode='';")
	tk.MustExec("DROP TABLE IF EXISTS tb5")
	tk.MustExec("create table tb5(a bigint, b bigint);")
	tk.MustExec("insert into tb5 values (10, -9223372036854775808);")
	err := tk.QueryToErr("select a - b from tb5;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(test.tb5.a - test.tb5.b)'")

	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION';")
	err = tk.QueryToErr("select a - b from tb5;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(test.tb5.a - test.tb5.b)'")
}

func (s *testMySQLConstSuite) TestIssue22442(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set sql_mode='';")
	tk.MustQuery("select cast(-1 as unsigned) - cast(-1 as unsigned);").Check(testkit.Rows("0"))

	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION';")
	tk.MustQuery("select cast(-1 as unsigned) - cast(-1 as unsigned);").Check(testkit.Rows("0"))
}

func (s *testMySQLConstSuite) TestIssue22444(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION'; ")
	tk.MustQuery("select cast(-1 as unsigned) - cast(-10000 as unsigned); ").Check(testkit.Rows("9999"))

	tk.MustExec("set sql_mode='';")
	tk.MustQuery("select cast(-1 as unsigned) - cast(-10000 as unsigned); ").Check(testkit.Rows("9999"))
}

func (s *testMySQLConstSuite) TestIssue22445(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION'; ")
	tk.MustQuery("select cast(-12 as unsigned) - cast(-1 as unsigned);").Check(testkit.Rows("-11"))

	tk.MustExec("set sql_mode='';")
	err := tk.QueryToErr("select cast(-12 as unsigned) - cast(-1 as unsigned);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(18446744073709551604 - 18446744073709551615)'")
}

func (s *testMySQLConstSuite) TestIssue22446(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION'; ")
	tk.MustQuery("select cast(-1 as unsigned) - 9223372036854775808").Check(testkit.Rows("9223372036854775807"))

	tk.MustExec("set sql_mode=''; ")
	tk.MustQuery("select cast(-1 as unsigned) - 9223372036854775808").Check(testkit.Rows("9223372036854775807"))
}

func (s *testMySQLConstSuite) TestIssue22447(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION'; ")
	tk.MustQuery("select 9223372036854775808 - cast(-1 as unsigned)").Check(testkit.Rows("-9223372036854775807"))

	tk.MustExec("set sql_mode='';")
	err := tk.QueryToErr("select 9223372036854775808 - cast(-1 as unsigned)")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(9223372036854775808 - 18446744073709551615)'")
}

func (s *testMySQLConstSuite) TestNoUnsignedSubtractionMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	ctx := context.Background()
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION'")
	r := tk.MustQuery("SELECT CAST(0 as UNSIGNED) - 1;")
	r.Check(testkit.Rows("-1"))

	// 1. minusFUU
	err := tk.QueryToErr("SELECT CAST(-1 as UNSIGNED) - cast(9223372036854775807 as unsigned);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(18446744073709551615 - 9223372036854775807)'")

	err = tk.QueryToErr("SELECT CAST(0 as UNSIGNED) - cast(9223372036854775809 as unsigned);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(0 - 9223372036854775809)'")

	tk.MustQuery("SELECT CAST(0 as UNSIGNED) - cast(9223372036854775808 as unsigned);").Check(testkit.Rows("-9223372036854775808"))
	tk.MustQuery("SELECT CAST(-1 as UNSIGNED) - cast(-9223372036854775808 as unsigned);").Check(testkit.Rows("9223372036854775807"))
	tk.MustQuery("SELECT cast(0 as unsigned) - cast(9223372036854775808 as unsigned);").Check(testkit.Rows("-9223372036854775808"))

	// 2. minusSS
	err = tk.QueryToErr("SELECT -9223372036854775808 - (1);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(-9223372036854775808 - 1)'")

	err = tk.QueryToErr("SELECT 1 - (-9223372036854775808);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(1 - -9223372036854775808)'")

	err = tk.QueryToErr("SELECT 1 - (-9223372036854775807);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(1 - -9223372036854775807)'")

	// 3. minusFUS
	err = tk.QueryToErr("SELECT CAST(-12 as UNSIGNED) - (-1);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(18446744073709551604 - -1)'")

	err = tk.QueryToErr("SELECT CAST(9223372036854775808 as UNSIGNED) - (0);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(9223372036854775808 - 0)'")

	err = tk.QueryToErr("SELECT CAST(-1 as UNSIGNED) - (9223372036854775807);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(18446744073709551615 - 9223372036854775807)'")

	err = tk.QueryToErr("SELECT CAST(9223372036854775808 as UNSIGNED) - 0;")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(9223372036854775808 - 0)'")

	tk.MustQuery("SELECT CAST(-1 as UNSIGNED) - (9223372036854775808);").Check(testkit.Rows("9223372036854775807"))

	err = tk.QueryToErr("SELECT CAST(1 as UNSIGNED) - (-9223372036854775808);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(1 - -9223372036854775808)'")

	err = tk.QueryToErr("SELECT CAST(1 as UNSIGNED) - (-9223372036854775807);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(1 - -9223372036854775807)'")

	tk.MustQuery("SELECT CAST(1 as UNSIGNED) - (-9223372036854775806)").Check(testkit.Rows("9223372036854775807"))
	tk.MustQuery("select cast(0 as unsigned) - 9223372036854775807").Check(testkit.Rows("-9223372036854775807"))

	// 4. minusFSU
	err = tk.QueryToErr("SELECT CAST(1 as SIGNED) - cast(9223372036854775810 as unsigned);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(1 - 9223372036854775810)'")

	err = tk.QueryToErr("SELECT CAST(-1 as SIGNED) - cast(9223372036854775808 as unsigned);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(-1 - 9223372036854775808)'")

	err = tk.QueryToErr("SELECT CAST(-9223372036854775807 as SIGNED) - cast(-1 as unsigned);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(-9223372036854775807 - 18446744073709551615)'")

	err = tk.QueryToErr("SELECT CAST(-1 as SIGNED) - cast(9223372036854775808 as unsigned);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT value is out of range in '(-1 - 9223372036854775808)'")

	tk.MustQuery("select 0 - cast(9223372036854775807 as unsigned)").Check(testkit.Rows("-9223372036854775807"))
	tk.MustQuery("SELECT CAST(1 as SIGNED) - cast(9223372036854775809 as unsigned)").Check(testkit.Rows("-9223372036854775808"))
	tk.MustQuery("SELECT CAST(-1 as SIGNED) - cast(9223372036854775807 as unsigned)").Check(testkit.Rows("-9223372036854775808"))

	rs, _ := tk.Exec("SELECT 1 - CAST(18446744073709551615 as UNSIGNED);")
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(rs.Close(), IsNil)
	rs, _ = tk.Exec("SELECT CAST(-1 as UNSIGNED) - 1")
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(rs.Close(), IsNil)
	rs, _ = tk.Exec("SELECT CAST(9223372036854775808 as UNSIGNED) - 1")
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, IsNil)
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

func (s *testMySQLConstSuite) TestNoBackslashEscapesMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set sql_mode=''")
	r := tk.MustQuery("SELECT '\\\\'")
	r.Check(testkit.Rows("\\"))
	tk.MustExec("set sql_mode='NO_BACKSLASH_ESCAPES'")
	r = tk.MustQuery("SELECT '\\\\'")
	r.Check(testkit.Rows("\\\\"))
}

func (s *testMySQLConstSuite) TestServerStatus(c *C) {
	tests := []struct {
		arg            uint16
		IsCursorExists bool
	}{
		{0, false},
		{mysql.ServerStatusInTrans | mysql.ServerStatusNoBackslashEscaped, false},
		{mysql.ServerStatusCursorExists, true},
		{mysql.ServerStatusCursorExists | mysql.ServerStatusLastRowSend, true},
	}

	for _, t := range tests {
		ret := mysql.HasCursorExistsFlag(t.arg)
		c.Assert(ret, Equals, t.IsCursorExists)
	}
}
