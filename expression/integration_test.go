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

package expression_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testIntegrationSuite{})

type testIntegrationSuite struct {
	store kv.Storage
}

func (s *testIntegrationSuite) cleanEnv(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	r := tk.MustQuery("show tables")
	for _, tb := range r.Rows() {
		tableName := tb[0]
		tk.MustExec(fmt.Sprintf("drop table %v", tableName))
	}
}

func (s *testIntegrationSuite) SetUpSuite(c *C) {
	s.store, _ = newStoreWithBootstrap()
}

func (s *testIntegrationSuite) TestFuncREPEAT(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS table_string;")
	tk.MustExec("CREATE TABLE table_string(a CHAR(20), b VARCHAR(20), c TINYTEXT, d TEXT(20), e MEDIUMTEXT, f LONGTEXT, g BIGINT);")
	tk.MustExec("INSERT INTO table_string (a, b, c, d, e, f, g) VALUES ('a', 'b', 'c', 'd', 'e', 'f', 2);")
	tk.CheckExecResult(1, 0)

	r := tk.MustQuery("SELECT REPEAT(a, g), REPEAT(b, g), REPEAT(c, g), REPEAT(d, g), REPEAT(e, g), REPEAT(f, g) FROM table_string;")
	r.Check(testkit.Rows("aa bb cc dd ee ff"))

	r = tk.MustQuery("SELECT REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g), REPEAT(NULL, g) FROM table_string;")
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery("SELECT REPEAT(a, NULL), REPEAT(b, NULL), REPEAT(c, NULL), REPEAT(d, NULL), REPEAT(e, NULL), REPEAT(f, NULL) FROM table_string;")
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery("SELECT REPEAT(a, 2), REPEAT(b, 2), REPEAT(c, 2), REPEAT(d, 2), REPEAT(e, 2), REPEAT(f, 2) FROM table_string;")
	r.Check(testkit.Rows("aa bb cc dd ee ff"))

	r = tk.MustQuery("SELECT REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2), REPEAT(NULL, 2) FROM table_string;")
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery("SELECT REPEAT(a, -1), REPEAT(b, -2), REPEAT(c, -2), REPEAT(d, -2), REPEAT(e, -2), REPEAT(f, -2) FROM table_string;")
	r.Check(testkit.Rows("     "))

	r = tk.MustQuery("SELECT REPEAT(a, 0), REPEAT(b, 0), REPEAT(c, 0), REPEAT(d, 0), REPEAT(e, 0), REPEAT(f, 0) FROM table_string;")
	r.Check(testkit.Rows("     "))

	r = tk.MustQuery("SELECT REPEAT(a, 16777217), REPEAT(b, 16777217), REPEAT(c, 16777217), REPEAT(d, 16777217), REPEAT(e, 16777217), REPEAT(f, 16777217) FROM table_string;")
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))
}

func (s *testIntegrationSuite) TestMiscellaneousBuiltin(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()

	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	// for uuid
	r := tk.MustQuery("select uuid(), uuid(), uuid(), uuid(), uuid(), uuid();")
	for _, it := range r.Rows() {
		for _, item := range it {
			uuid, ok := item.(string)
			c.Assert(ok, Equals, true)
			list := strings.Split(uuid, "-")
			c.Assert(len(list), Equals, 5)
			c.Assert(len(list[0]), Equals, 8)
			c.Assert(len(list[1]), Equals, 4)
			c.Assert(len(list[2]), Equals, 4)
			c.Assert(len(list[3]), Equals, 4)
			c.Assert(len(list[4]), Equals, 12)
		}
	}
}

func (s *testIntegrationSuite) TestConvertToBit(c *C) {
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

func (s *testIntegrationSuite) TestMathBuiltin(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// for degrees
	result := tk.MustQuery("select degrees(0), degrees(1)")
	result.Check(testkit.Rows("0 57.29577951308232"))
	result = tk.MustQuery("select degrees(2), degrees(5)")
	result.Check(testkit.Rows("114.59155902616465 286.4788975654116"))

	// for sin
	result = tk.MustQuery("select sin(0), sin(1.5707963267949)")
	result.Check(testkit.Rows("0 1"))
	result = tk.MustQuery("select sin(1), sin(100)")
	result.Check(testkit.Rows("0.8414709848078965 -0.5063656411097588"))
	result = tk.MustQuery("select sin('abcd')")
	result.Check(testkit.Rows("0"))

	// for cos
	result = tk.MustQuery("select cos(0), cos(3.1415926535898)")
	result.Check(testkit.Rows("1 -1"))
	result = tk.MustQuery("select cos('abcd')")
	result.Check(testkit.Rows("1"))

	// for tan
	result = tk.MustQuery("select tan(0.00), tan(PI()/4)")
	result.Check(testkit.Rows("0 1"))
	result = tk.MustQuery("select tan('abcd')")
	result.Check(testkit.Rows("0"))

	// for log2
	result = tk.MustQuery("select log2(0.0)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select log2(4)")
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery("select log2('8.0abcd')")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("select log2(-1)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select log2(NULL)")
	result.Check(testkit.Rows("<nil>"))

	// for log10
	result = tk.MustQuery("select log10(0.0)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select log10(100)")
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery("select log10('1000.0abcd')")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("select log10(-1)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select log10(NULL)")
	result.Check(testkit.Rows("<nil>"))

	//for log
	result = tk.MustQuery("select log(0.0)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select log(100)")
	result.Check(testkit.Rows("4.605170185988092"))
	result = tk.MustQuery("select log('100.0abcd')")
	result.Check(testkit.Rows("4.605170185988092"))
	result = tk.MustQuery("select log(-1)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select log(NULL)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select log(NULL, NULL)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select log(1, 100)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select log(0.5, 0.25)")
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery("select log(-1, 0.25)")
	result.Check(testkit.Rows("<nil>"))

	// for atan
	result = tk.MustQuery("select atan(0), atan(-1), atan(1), atan(1,2)")
	result.Check(testkit.Rows("0 -0.7853981633974483 0.7853981633974483 0.4636476090008061"))
	result = tk.MustQuery("select atan('tidb')")
	result.Check(testkit.Rows("0"))

	// for asin
	result = tk.MustQuery("select asin(0), asin(-2), asin(2), asin(1)")
	result.Check(testkit.Rows("0 <nil> <nil> 1.5707963267948966"))
	result = tk.MustQuery("select asin('tidb')")
	result.Check(testkit.Rows("0"))

	// for acos
	result = tk.MustQuery("select acos(0), acos(-2), acos(2), acos(1)")
	result.Check(testkit.Rows("1.5707963267948966 <nil> <nil> 0"))
	result = tk.MustQuery("select acos('tidb')")
	result.Check(testkit.Rows("1.5707963267948966"))

	// for pi
	result = tk.MustQuery("select pi()")
	result.Check(testkit.Rows("3.141592653589793"))

	// for floor
	result = tk.MustQuery("select floor(0), floor(null), floor(1.23), floor(-1.23), floor(1)")
	result.Check(testkit.Rows("0 <nil> 1 -2 1"))
	result = tk.MustQuery("select floor('tidb'), floor('1tidb'), floor('tidb1')")
	result.Check(testkit.Rows("0 1 0"))
	result = tk.MustQuery("SELECT floor(t.c_datetime) FROM (select CAST('2017-07-19 00:00:00' AS DATETIME) AS c_datetime) AS t")
	result.Check(testkit.Rows("20170719000000"))
	result = tk.MustQuery("SELECT floor(t.c_time) FROM (select CAST('12:34:56' AS TIME) AS c_time) AS t")
	result.Check(testkit.Rows("123456"))
	result = tk.MustQuery("SELECT floor(t.c_time) FROM (select CAST('00:34:00' AS TIME) AS c_time) AS t")
	result.Check(testkit.Rows("3400"))
	result = tk.MustQuery("SELECT floor(t.c_time) FROM (select CAST('00:00:00' AS TIME) AS c_time) AS t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT floor(t.c_decimal) FROM (SELECT CAST('-10.01' AS DECIMAL(10,2)) AS c_decimal) AS t")
	result.Check(testkit.Rows("-11"))
	result = tk.MustQuery("SELECT floor(t.c_decimal) FROM (SELECT CAST('-10.01' AS DECIMAL(10,1)) AS c_decimal) AS t")
	result.Check(testkit.Rows("-10"))

	// for ceil/ceiling
	result = tk.MustQuery("select ceil(0), ceil(null), ceil(1.23), ceil(-1.23), ceil(1)")
	result.Check(testkit.Rows("0 <nil> 2 -1 1"))
	result = tk.MustQuery("select ceiling(0), ceiling(null), ceiling(1.23), ceiling(-1.23), ceiling(1)")
	result.Check(testkit.Rows("0 <nil> 2 -1 1"))
	result = tk.MustQuery("select ceil('tidb'), ceil('1tidb'), ceil('tidb1'), ceiling('tidb'), ceiling('1tidb'), ceiling('tidb1')")
	result.Check(testkit.Rows("0 1 0 0 1 0"))
	result = tk.MustQuery("select ceil(t.c_datetime), ceiling(t.c_datetime) from (select cast('2017-07-20 00:00:00' as datetime) as c_datetime) as t")
	result.Check(testkit.Rows("20170720000000 20170720000000"))
	result = tk.MustQuery("select ceil(t.c_time), ceiling(t.c_time) from (select cast('12:34:56' as time) as c_time) as t")
	result.Check(testkit.Rows("123456 123456"))
	result = tk.MustQuery("select ceil(t.c_time), ceiling(t.c_time) from (select cast('00:34:00' as time) as c_time) as t")
	result.Check(testkit.Rows("3400 3400"))
	result = tk.MustQuery("select ceil(t.c_time), ceiling(t.c_time) from (select cast('00:00:00' as time) as c_time) as t")
	result.Check(testkit.Rows("0 0"))
	result = tk.MustQuery("select ceil(t.c_decimal), ceiling(t.c_decimal) from (select cast('-10.01' as decimal(10,2)) as c_decimal) as t")
	result.Check(testkit.Rows("-10 -10"))
	result = tk.MustQuery("select ceil(t.c_decimal), ceiling(t.c_decimal) from (select cast('-10.01' as decimal(10,1)) as c_decimal) as t")
	result.Check(testkit.Rows("-10 -10"))
	result = tk.MustQuery("select floor(18446744073709551615), ceil(18446744073709551615)")
	result.Check(testkit.Rows("18446744073709551615 18446744073709551615"))
	result = tk.MustQuery("select floor(18446744073709551615.1233), ceil(18446744073709551615.1233)")
	result.Check(testkit.Rows("18446744073709551615 18446744073709551616"))
	result = tk.MustQuery("select floor(-18446744073709551617), ceil(-18446744073709551617), floor(-18446744073709551617.11), ceil(-18446744073709551617.11)")
	result.Check(testkit.Rows("-18446744073709551617 -18446744073709551617 -18446744073709551618 -18446744073709551617"))

	// for cot
	result = tk.MustQuery("select cot(1), cot(-1), cot(NULL)")
	result.Check(testkit.Rows("0.6420926159343308 -0.6420926159343308 <nil>"))
	result = tk.MustQuery("select cot('1tidb')")
	result.Check(testkit.Rows("0.6420926159343308"))
	rs, err := tk.Exec("select cot(0)")
	c.Assert(err, IsNil)
	_, err = tidb.GetRows(rs)
	c.Assert(err, NotNil)
	terr := errors.Trace(err).(*errors.Err).Cause().(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrDataOutOfRange))

	//for exp
	result = tk.MustQuery("select exp(0), exp(1), exp(-1), exp(1.2), exp(NULL)")
	result.Check(testkit.Rows("1 2.718281828459045 0.36787944117144233 3.3201169227365472 <nil>"))
	result = tk.MustQuery("select exp('tidb'), exp('1tidb')")
	result.Check(testkit.Rows("1 2.718281828459045"))
	rs, err = tk.Exec("select exp(1000000)")
	c.Assert(err, IsNil)
	_, err = tidb.GetRows(rs)
	c.Assert(err, NotNil)
	terr = errors.Trace(err).(*errors.Err).Cause().(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrDataOutOfRange))
}

func (s *testIntegrationSuite) TestStringBuiltin(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// for length
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101)`)
	result := tk.MustQuery("select length(a), length(b), length(c), length(d), length(e), length(f), length(null) from t")
	result.Check(testkit.Rows("1 3 19 8 6 2 <nil>"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20))")
	tk.MustExec(`insert into t values("tidb  "), (concat("a  ", "b  "))`)
	result = tk.MustQuery("select a, length(a) from t")
	result.Check(testkit.Rows("tidb 4", "a  b 4"))

	// for concat
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef")`)
	result = tk.MustQuery("select concat(a, b, c, d, e) from t")
	result.Check(testkit.Rows("11.12017-01-01 12:01:0112:01:01abcdef"))
	result = tk.MustQuery("select concat(null)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select concat(null, a, b) from t")
	result.Check(testkit.Rows("<nil>"))

	// for concat_ws
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef")`)
	result = tk.MustQuery("select concat_ws('|', a, b, c, d, e) from t")
	result.Check(testkit.Rows("1|1.1|2017-01-01 12:01:01|12:01:01|abcdef"))
	result = tk.MustQuery("select concat_ws(null, null)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select concat_ws(null, a, b) from t")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select concat_ws(',', 'a', 'b')")
	result.Check(testkit.Rows("a,b"))
	result = tk.MustQuery("select concat_ws(',','First name',NULL,'Last Name')")
	result.Check(testkit.Rows("First name,Last Name"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a binary(3))")
	tk.MustExec("insert into t values('a')")
	result = tk.MustQuery(`select concat_ws(',', a, 'test') = 'a\0\0,test' from t`)
	result.Check(testkit.Rows("1"))

	// for ascii
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010)`)
	result = tk.MustQuery("select ascii(a), ascii(b), ascii(c), ascii(d), ascii(e), ascii(f) from t")
	result.Check(testkit.Rows("50 50 50 50 49 10"))
	result = tk.MustQuery("select ascii('123'), ascii(123), ascii(''), ascii('你好'), ascii(NULL)")
	result.Check(testkit.Rows("49 49 0 228 <nil>"))

	// for lower
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f binary(3), g binary(3))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 'aa', 'BB')`)
	result = tk.MustQuery("select lower(a), lower(b), lower(c), lower(d), lower(e), lower(f), lower(g), lower(null) from t")
	result.Check(testkit.Rows("1 1.1 2017-01-01 12:01:01 12:01:01 abcdef aa\x00 BB\x00 <nil>"))

	// for upper
	result = tk.MustQuery("select upper(a), upper(b), upper(c), upper(d), upper(e), upper(f), upper(g), upper(null) from t")
	result.Check(testkit.Rows("1 1.1 2017-01-01 12:01:01 12:01:01 ABCDEF aa\x00 BB\x00 <nil>"))

	// for strcmp
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values("123", 123, 12.34, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select strcmp(a, "123"), strcmp(b, "123"), strcmp(c, "12.34"), strcmp(d, "2017-01-01 12:01:01"), strcmp(e, "12:01:01") from t`)
	result.Check(testkit.Rows("0 0 0 0 0"))
	result = tk.MustQuery(`select strcmp("1", "123"), strcmp("123", "1"), strcmp("123", "45"), strcmp("123", null), strcmp(null, "123")`)
	result.Check(testkit.Rows("-1 1 -1 <nil> <nil>"))
	result = tk.MustQuery(`select strcmp("", "123"), strcmp("123", ""), strcmp("", ""), strcmp("", null), strcmp(null, "")`)
	result.Check(testkit.Rows("-1 1 0 <nil> <nil>"))

	// for left
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('abcde', 1234, 12.34, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery("select left(a, 2), left(b, 2), left(c, 2), left(d, 2), left(e, 2) from t")
	result.Check(testkit.Rows("ab 12 12 20 12"))
	result = tk.MustQuery(`select left("abc", 0), left("abc", -1), left(NULL, 1), left("abc", NULL)`)
	result.Check(testkit.Rows("  <nil> <nil>"))
	result = tk.MustQuery(`select left("abc", "a"), left("abc", 1.9), left("abc", 1.2)`)
	result.Check(testkit.Rows(" ab a"))
	result = tk.MustQuery(`select left("中文abc", 2), left("中文abc", 3), left("中文abc", 4)`)
	result.Check(testkit.Rows("中文 中文a 中文ab"))
	// for right, reuse the table created for left
	result = tk.MustQuery("select right(a, 3), right(b, 3), right(c, 3), right(d, 3), right(e, 3) from t")
	result.Check(testkit.Rows("cde 234 .34 :01 :01"))
	result = tk.MustQuery(`select right("abcde", 0), right("abcde", -1), right("abcde", 100), right(NULL, 1), right("abcde", NULL)`)
	result.Check(testkit.Rows("  abcde <nil> <nil>"))
	result = tk.MustQuery(`select right("abcde", "a"), right("abcde", 1.9), right("abcde", 1.2)`)
	result.Check(testkit.Rows(" de e"))
	result = tk.MustQuery(`select right("中文abc", 2), right("中文abc", 4), right("中文abc", 5)`)
	result.Check(testkit.Rows("bc 文abc 中文abc"))

	// for ord
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select ord(a), ord(b), ord(c), ord(d), ord(e), ord(f), ord(g), ord(h), ord(i) from t")
	result.Check(testkit.Rows("50 50 50 50 49 10 53 52 116"))
	result = tk.MustQuery("select ord('123'), ord(123), ord(''), ord('你好'), ord(NULL), ord('👍')")
	result.Check(testkit.Rows("49 49 0 14990752 <nil> 4036989325"))

	// for space
	result = tk.MustQuery(`select space(0), space(2), space(-1), space(1.1), space(1.9)`)
	result.Check(testutil.RowsWithSep(",", ",  ,, ,  "))
	result = tk.MustQuery(`select space("abc"), space("2"), space("1.1"), space(''), space(null)`)
	result.Check(testutil.RowsWithSep(",", ",  , ,,<nil>"))

	// for replace
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('www.mysql.com', 1234, 12.34, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select replace(a, 'mysql', 'pingcap'), replace(b, 2, 55), replace(c, 34, 0), replace(d, '-', '/'), replace(e, '01', '22') from t`)
	result.Check(testutil.RowsWithSep(",", "www.pingcap.com,15534,12.0,2017/01/01 12:01:01,12:22:22"))
	result = tk.MustQuery(`select replace('aaa', 'a', ''), replace(null, 'a', 'b'), replace('a', null, 'b'), replace('a', 'b', null)`)
	result.Check(testkit.Rows(" <nil> <nil> <nil>"))

	// for tobase64
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10), g binary(20), h blob(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101, "512", "abc")`)
	result = tk.MustQuery("select to_base64(a), to_base64(b), to_base64(c), to_base64(d), to_base64(e), to_base64(f), to_base64(g), to_base64(h), to_base64(null) from t")
	result.Check(testkit.Rows("MQ== MS4x MjAxNy0wMS0wMSAxMjowMTowMQ== MTI6MDE6MDE= YWJjZGVm ABU= NTEyAAAAAAAAAAAAAAAAAAAAAAA= YWJj <nil>"))

	// for substr
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('Sakila', 12345, 123.45, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select substr(a, 3), substr(b, 2, 3), substr(c, -3), substr(d, -8), substr(e, -3, 100) from t`)
	result.Check(testkit.Rows("kila 234 .45 12:01:01 :01"))
	result = tk.MustQuery(`select substr('Sakila', 100), substr('Sakila', -100), substr('Sakila', -5, 3), substr('Sakila', 2, -1)`)
	result.Check(testutil.RowsWithSep(",", ",,aki,"))
	result = tk.MustQuery(`select substr('foobarbar' from 4), substr('Sakila' from -4 for 2)`)
	result.Check(testkit.Rows("barbar ki"))
	result = tk.MustQuery(`select substr(null, 2, 3), substr('foo', null, 3), substr('foo', 2, null)`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select substr('中文abc', 2), substr('中文abc', 3), substr("中文abc", 1, 2)`)
	result.Check(testkit.Rows("文abc abc 中文"))

	// for bit_length
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10), g binary(20), h varbinary(20))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101, "g", "h")`)
	result = tk.MustQuery("select bit_length(a), bit_length(b), bit_length(c), bit_length(d), bit_length(e), bit_length(f), bit_length(g), bit_length(h), bit_length(null) from t")
	result.Check(testkit.Rows("8 24 152 64 48 16 160 8 <nil>"))

	// for substring_index
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20), b int, c double, d datetime, e time)")
	tk.MustExec(`insert into t values('www.pingcap.com', 12345, 123.45, "2017-01-01 12:01:01", "12:01:01")`)
	result = tk.MustQuery(`select substring_index(a, '.', 2), substring_index(b, '.', 2), substring_index(c, '.', -1), substring_index(d, '-', 1), substring_index(e, ':', -2) from t`)
	result.Check(testkit.Rows("www.pingcap 12345 45 2017 01:01"))
	result = tk.MustQuery(`select substring_index('www.pingcap.com', '.', 0), substring_index('www.pingcap.com', '.', 100), substring_index('www.pingcap.com', '.', -100)`)
	result.Check(testkit.Rows(" www.pingcap.com www.pingcap.com"))
	result = tk.MustQuery(`select substring_index('www.pingcap.com', 'd', 1), substring_index('www.pingcap.com', '', 1), substring_index('', '.', 1)`)
	result.Check(testutil.RowsWithSep(",", "www.pingcap.com,,"))
	result = tk.MustQuery(`select substring_index(null, '.', 1), substring_index('www.pingcap.com', null, 1), substring_index('www.pingcap.com', '.', null)`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))

	// for hex
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20), b int, c double, d datetime, e time, f decimal(5, 2), g bit(4))")
	tk.MustExec(`insert into t values('www.pingcap.com', 12345, 123.45, "2017-01-01 12:01:01", "12:01:01", 123.45, 0b1100)`)
	result = tk.MustQuery(`select hex(a), hex(b), hex(c), hex(d), hex(e), hex(f), hex(g) from t`)
	result.Check(testkit.Rows("7777772E70696E676361702E636F6D 3039 7B 323031372D30312D30312031323A30313A3031 31323A30313A3031 7B C"))
	result = tk.MustQuery(`select hex('abc'), hex('你好'), hex(12), hex(12.3), hex(12.8)`)
	result.Check(testkit.Rows("616263 E4BDA0E5A5BD C C D"))
	result = tk.MustQuery(`select hex(-1), hex(-12.3), hex(-12.8), hex(0x12), hex(null)`)
	result.Check(testkit.Rows("FFFFFFFFFFFFFFFF FFFFFFFFFFFFFFF4 FFFFFFFFFFFFFFF3 12 <nil>"))

	// for unhex
	result = tk.MustQuery(`select unhex('4D7953514C'), unhex('313233'), unhex(313233), unhex('')`)
	result.Check(testkit.Rows("MySQL 123 123 "))
	result = tk.MustQuery(`select unhex('string'), unhex('你好'), unhex(123.4), unhex(null)`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))

	// for ltrim and rtrim
	result = tk.MustQuery(`select ltrim('   bar   '), ltrim('bar'), ltrim(''), ltrim(null)`)
	result.Check(testutil.RowsWithSep(",", "bar   ,bar,,<nil>"))
	result = tk.MustQuery(`select rtrim('   bar   '), rtrim('bar'), rtrim(''), rtrim(null)`)
	result.Check(testutil.RowsWithSep(",", "   bar,bar,,<nil>"))

	// for trim
	result = tk.MustQuery(`select trim('   bar   '), trim(leading 'x' from 'xxxbarxxx'), trim(trailing 'xyz' from 'barxxyz'), trim(both 'x' from 'xxxbarxxx')`)
	result.Check(testkit.Rows("bar barxxx barx bar"))
	result = tk.MustQuery(`select trim(leading from '   bar'), trim('x' from 'xxxbarxxx'), trim('x' from 'bar'), trim('' from '   bar   ')`)
	result.Check(testutil.RowsWithSep(",", "bar,bar,bar,   bar   "))
	result = tk.MustQuery(`select trim(''), trim('x' from '')`)
	result.Check(testutil.RowsWithSep(",", ","))
	result = tk.MustQuery(`select trim(null from 'bar'), trim('x' from null), trim(null), trim(leading null from 'bar')`)
	// FIXME: the result for trim(leading null from 'bar') should be <nil>, current is 'bar'
	result.Check(testkit.Rows("<nil> <nil> <nil> bar"))
}

func (s *testIntegrationSuite) TestEncryptionBuiltin(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// for password
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(41), b char(41), c char(41))")
	tk.MustExec(`insert into t values(NULL, '', 'abc')`)
	result := tk.MustQuery("select password(a) from t")
	result.Check(testkit.Rows(""))
	result = tk.MustQuery("select password(b) from t")
	result.Check(testkit.Rows(""))
	result = tk.MustQuery("select password(c) from t")
	result.Check(testkit.Rows("*0D3CED9BEC10A777AEC23CCC353A8C08A633045E"))

	// for md5
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select md5(a), md5(b), md5(c), md5(d), md5(e), md5(f), md5(g), md5(h), md5(i) from t")
	result.Check(testkit.Rows("c81e728d9d4c2f636f067f89cc14862c c81e728d9d4c2f636f067f89cc14862c 1a18da63cbbfb49cb9616e6bfd35f662 bad2fa88e1f35919ec7584cc2623a310 991f84d41d7acff6471e536caa8d97db 68b329da9893e34099c7d8ad5cb9c940 5c9f0e9b3b36276731bfba852a73ccc6 642e92efb79421734881b53e1e1b18b6 c337e11bfca9f12ae9b1342901e04379"))
	result = tk.MustQuery("select md5('123'), md5(123), md5(''), md5('你好'), md5(NULL), md5('👍')")
	result.Check(testkit.Rows(`202cb962ac59075b964b07152d234b70 202cb962ac59075b964b07152d234b70 d41d8cd98f00b204e9800998ecf8427e 7eca689f0d3389d9dea66ae112e5cfd7 <nil> 0215ac4dab1ecaf71d83f98af5726984`))
}

func (s *testIntegrationSuite) TestTimeBuiltin(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// for makeDate
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101)`)
	result := tk.MustQuery("select makedate(a,a), makedate(b,b), makedate(c,c), makedate(d,d), makedate(e,e), makedate(f,f), makedate(null,null), makedate(a,b) from t")
	result.Check(testkit.Rows("2001-01-01 2001-01-01 <nil> <nil> <nil> 2021-01-21 <nil> 2001-01-01"))

	// fixed issue #3986
	tk.MustExec("SET SQL_MODE='NO_ENGINE_SUBSTITUTION';")
	tk.MustExec("SET TIME_ZONE='+03:00';")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t (ix TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);")
	tk.MustExec("INSERT INTO t VALUES (0), (20030101010160), (20030101016001), (20030101240101), (20030132010101), (20031301010101), (20031200000000), (20030000000000);")
	result = tk.MustQuery("SELECT CAST(ix AS SIGNED) FROM t;")
	result.Check(testkit.Rows("0", "0", "0", "0", "0", "0", "0", "0"))

}

func (s *testIntegrationSuite) TestOpBuiltin(c *C) {
	defer func() {
		s.cleanEnv(c)
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// for logicAnd
	result := tk.MustQuery("select 1 && 1, 1 && 0, 0 && 1, 0 && 0, 2 && -1, null && 1, '1a' && 'a'")
	result.Check(testkit.Rows("1 0 0 0 1 <nil> 0"))

	// for bitNeg
	result = tk.MustQuery("select ~123, ~-123, ~null")
	result.Check(testkit.Rows("18446744073709551492 122 <nil>"))
	// for logicNot
	result = tk.MustQuery("select !1, !123, !0, !null")
	result.Check(testkit.Rows("0 0 1 <nil>"))
	// for logicalXor
	result = tk.MustQuery("select 1 xor 1, 1 xor 0, 0 xor 1, 0 xor 0, 2 xor -1, null xor 1, '1a' xor 'a'")
	result.Check(testkit.Rows("0 1 1 0 0 <nil> 1"))
	// for bitAnd
	result = tk.MustQuery("select 123 & 321, -123 & 321, null & 1")
	result.Check(testkit.Rows("65 257 <nil>"))
	// for bitOr
	result = tk.MustQuery("select 123 | 321, -123 | 321, null | 1")
	result.Check(testkit.Rows("379 18446744073709551557 <nil>"))
	// for bitXor
	result = tk.MustQuery("select 123 ^ 321, -123 ^ 321, null ^ 1")
	result.Check(testkit.Rows("314 18446744073709551300 <nil>"))
	// for leftShift
	result = tk.MustQuery("select 123 << 2, -123 << 2, null << 1")
	result.Check(testkit.Rows("492 18446744073709551124 <nil>"))
	// for rightShift
	result = tk.MustQuery("select 123 >> 2, -123 >> 2, null >> 1")
	result.Check(testkit.Rows("30 4611686018427387873 <nil>"))
	// for logicOr
	result = tk.MustQuery("select 1 || 1, 1 || 0, 0 || 1, 0 || 0, 2 || -1, null || 1, '1a' || 'a'")
	result.Check(testkit.Rows("1 1 1 0 1 1 1"))
}

func (s *testIntegrationSuite) TestBuiltin(c *C) {
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
	result = tk.MustQuery("select cast(-1 as unsigned)")
	result.Check(testkit.Rows("18446744073709551615"))

	// fix issue #3942
	result = tk.MustQuery("select cast('-24 100:00:00' as time);")
	result.Check(testkit.Rows("-676:00:00"))
	result = tk.MustQuery("select cast('12:00:00.000000' as datetime);")
	result.Check(testkit.Rows("2012-00-00 00:00:00"))
	result = tk.MustQuery("select cast('-34 100:00:00' as time);")
	result.Check(testkit.Rows("-838:59:59"))

	// Fix issue #3691, cast compability.
	result = tk.MustQuery("select cast('18446744073709551616' as unsigned);")
	result.Check(testkit.Rows("18446744073709551615"))
	result = tk.MustQuery("select cast('18446744073709551616' as signed);")
	result.Check(testkit.Rows("-1"))
	result = tk.MustQuery("select cast('9223372036854775808' as signed);")
	result.Check(testkit.Rows("-9223372036854775808"))
	result = tk.MustQuery("select cast('9223372036854775809' as signed);")
	result.Check(testkit.Rows("-9223372036854775807"))
	result = tk.MustQuery("select cast('9223372036854775807' as signed);")
	result.Check(testkit.Rows("9223372036854775807"))
	result = tk.MustQuery("select cast('18446744073709551615' as signed);")
	result.Check(testkit.Rows("-1"))
	result = tk.MustQuery("select cast('18446744073709551614' as signed);")
	result.Check(testkit.Rows("-2"))
	result = tk.MustQuery("select cast(18446744073709551615 as unsigned);")
	result.Check(testkit.Rows("18446744073709551615"))
	result = tk.MustQuery("select cast(18446744073709551616 as unsigned);")
	result.Check(testkit.Rows("18446744073709551615"))
	result = tk.MustQuery("select cast(18446744073709551616 as signed);")
	result.Check(testkit.Rows("9223372036854775807"))
	result = tk.MustQuery("select cast(18446744073709551617 as signed);")
	result.Check(testkit.Rows("9223372036854775807"))
	result = tk.MustQuery("select cast(18446744073709551615 as signed);")
	result.Check(testkit.Rows("-1"))
	result = tk.MustQuery("select cast(18446744073709551614 as signed);")
	result.Check(testkit.Rows("-2"))
	result = tk.MustQuery("select cast(-18446744073709551616 as signed);")
	result.Check(testkit.Rows("-9223372036854775808"))
	result = tk.MustQuery("select cast(18446744073709551614.9 as unsigned);") // Round up
	result.Check(testkit.Rows("18446744073709551615"))
	result = tk.MustQuery("select cast(18446744073709551614.4 as unsigned);") // Round down
	result.Check(testkit.Rows("18446744073709551614"))
	result = tk.MustQuery("select cast(-9223372036854775809 as signed);")
	result.Check(testkit.Rows("-9223372036854775808"))
	result = tk.MustQuery("select cast(-9223372036854775809 as unsigned);")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select cast(-9223372036854775808 as unsigned);")
	result.Check(testkit.Rows("9223372036854775808"))
	result = tk.MustQuery("select cast('-9223372036854775809' as unsigned);")
	result.Check(testkit.Rows("9223372036854775808"))
	result = tk.MustQuery("select cast('-9223372036854775807' as unsigned);")
	result.Check(testkit.Rows("9223372036854775809"))
	result = tk.MustQuery("select cast('-2' as unsigned);")
	result.Check(testkit.Rows("18446744073709551614"))
	result = tk.MustQuery("select cast(cast(1-2 as unsigned) as signed integer);")
	result.Check(testkit.Rows("-1"))
	result = tk.MustQuery("select cast(1 as signed int)")
	result.Check(testkit.Rows("1"))

	// test cast time as decimal overflow
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(s1 time);")
	tk.MustExec("insert into t1 values('11:11:11');")
	result = tk.MustQuery("select cast(s1 as decimal(7, 2)) from t1;")
	result.Check(testkit.Rows("99999.99"))
	result = tk.MustQuery("select cast(s1 as decimal(8, 2)) from t1;")
	result.Check(testkit.Rows("111111.00"))
	_, err := tk.Exec("insert into t1 values(cast('111111.00' as decimal(7, 2)));")
	c.Assert(err, NotNil)

	result = tk.MustQuery(`select CAST(0x8fffffffffffffff as signed) a,
	CAST(0xfffffffffffffffe as signed) b,
	CAST(0xffffffffffffffff as unsigned) c;`)
	result.Check(testkit.Rows("-8070450532247928833 -2 18446744073709551615"))

	result = tk.MustQuery(`select cast("1:2:3" as TIME) = "1:02:03"`)
	result.Check(testkit.Rows("0"))

	// fixed issue #3471
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a time(6));")
	tk.MustExec("insert into t value('12:59:59.999999')")
	result = tk.MustQuery("select cast(a as signed) from t")
	result.Check(testkit.Rows("130000"))

	// fixed issue #3762
	result = tk.MustQuery("select -9223372036854775809;")
	result.Check(testkit.Rows("-9223372036854775809"))
	result = tk.MustQuery("select --9223372036854775809;")
	result.Check(testkit.Rows("9223372036854775809"))
	result = tk.MustQuery("select -9223372036854775808;")
	result.Check(testkit.Rows("-9223372036854775808"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint(30));")
	_, err = tk.Exec("insert into t values(-9223372036854775809)")
	c.Assert(err, NotNil)

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
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a binary(8))")
	tk.MustExec(`insert into t values('test')`)
	result = tk.MustQuery("select hex(a) from t")
	result.Check(testkit.Rows("7465737400000000"))
	result = tk.MustQuery("select unhex(a) from t")
	result.Check(testkit.Rows("<nil>"))

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
	result = tk.MustQuery("select substr(null, 1, 2)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select substr('123', null, 2)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select substr('123', 1, null)")
	result.Check(testkit.Rows("<nil>"))

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
	result = tk.MustQuery("select cast(1234 as char(0))")
	result.Check(testkit.Rows(""))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1406 Data Too Long, field len 0, data len 4"))
	result = tk.MustQuery("select CAST( - 8 AS DECIMAL ) * + 52 + 87 < - 86")
	result.Check(testkit.Rows("1"))

	// issue 3884
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (c1 date, c2 datetime, c3 timestamp, c4 time, c5 year);")
	tk.MustExec("INSERT INTO t values ('2000-01-01', '2000-01-01 12:12:12', '2000-01-01 12:12:12', '12:12:12', '2000');")
	tk.MustExec("INSERT INTO t values ('2000-02-01', '2000-02-01 12:12:12', '2000-02-01 12:12:12', '13:12:12', 2000);")
	tk.MustExec("INSERT INTO t values ('2000-03-01', '2000-03-01', '2000-03-01 12:12:12', '1 12:12:12', 2000);")
	tk.MustExec("INSERT INTO t SET c1 = '2000-04-01', c2 = '2000-04-01', c3 = '2000-04-01 12:12:12', c4 = '-1 13:12:12', c5 = 2000;")
	result = tk.MustQuery("SELECT c4 FROM t where c4 < '-13:12:12';")
	result.Check(testkit.Rows("-37:12:12"))

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
			result = tk.MustQuery(fmt.Sprintf("select * from t where a %s '%s'", queryOp, d.pattern))
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
