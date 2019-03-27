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
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/kv"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/sqlexec"
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

func (s *testIntegrationSuite) TestFuncREPEAT(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
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

func (s *testIntegrationSuite) TestFuncLpadAndRpad(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec(`USE test;`)
	tk.MustExec(`DROP TABLE IF EXISTS t;`)
	tk.MustExec(`CREATE TABLE t(a BINARY(10), b CHAR(10));`)
	tk.MustExec(`INSERT INTO t SELECT "中文", "abc";`)
	result := tk.MustQuery(`SELECT LPAD(a, 11, "a"), LPAD(b, 2, "xx") FROM t;`)
	result.Check(testkit.Rows("a中文\x00\x00\x00\x00 ab"))
	result = tk.MustQuery(`SELECT RPAD(a, 11, "a"), RPAD(b, 2, "xx") FROM t;`)
	result.Check(testkit.Rows("中文\x00\x00\x00\x00a ab"))
	result = tk.MustQuery(`SELECT LPAD("中文", 5, "字符"), LPAD("中文", 1, "a");`)
	result.Check(testkit.Rows("字符字中文 中"))
	result = tk.MustQuery(`SELECT RPAD("中文", 5, "字符"), RPAD("中文", 1, "a");`)
	result.Check(testkit.Rows("中文字符字 中"))
	result = tk.MustQuery(`SELECT RPAD("中文", -5, "字符"), RPAD("中文", 10, "");`)
	result.Check(testkit.Rows("<nil> <nil>"))
	result = tk.MustQuery(`SELECT LPAD("中文", -5, "字符"), LPAD("中文", 10, "");`)
	result.Check(testkit.Rows("<nil> <nil>"))
}

func (s *testIntegrationSuite) TestMiscellaneousBuiltin(c *C) {
	ctx := context.Background()
	defer s.cleanEnv(c)

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
	tk.MustQuery("select sleep(1);").Check(testkit.Rows("0"))
	tk.MustQuery("select sleep(0);").Check(testkit.Rows("0"))
	tk.MustQuery("select sleep('a');").Check(testkit.Rows("0"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1265 Data Truncated"))
	rs, err := tk.Exec("select sleep(-1);")
	c.Assert(err, IsNil)
	c.Assert(rs, NotNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(rs.Close(), IsNil)

	tk.MustQuery("SELECT INET_ATON('10.0.5.9');").Check(testkit.Rows("167773449"))
	tk.MustQuery("SELECT INET_NTOA(167773449);").Check(testkit.Rows("10.0.5.9"))
	tk.MustQuery("SELECT HEX(INET6_ATON('fdfe::5a55:caff:fefa:9089'));").Check(testkit.Rows("FDFE0000000000005A55CAFFFEFA9089"))
	tk.MustQuery("SELECT HEX(INET6_ATON('10.0.5.9'));").Check(testkit.Rows("0A000509"))
	tk.MustQuery("SELECT INET6_NTOA(INET6_ATON('fdfe::5a55:caff:fefa:9089'));").Check(testkit.Rows("fdfe::5a55:caff:fefa:9089"))
	tk.MustQuery("SELECT INET6_NTOA(INET6_ATON('10.0.5.9'));").Check(testkit.Rows("10.0.5.9"))
	tk.MustQuery("SELECT INET6_NTOA(UNHEX('FDFE0000000000005A55CAFFFEFA9089'));").Check(testkit.Rows("fdfe::5a55:caff:fefa:9089"))
	tk.MustQuery("SELECT INET6_NTOA(UNHEX('0A000509'));").Check(testkit.Rows("10.0.5.9"))

	tk.MustQuery(`SELECT IS_IPV4('10.0.5.9'), IS_IPV4('10.0.5.256');`).Check(testkit.Rows("1 0"))
	tk.MustQuery(`SELECT IS_IPV4_COMPAT(INET6_ATON('::10.0.5.9'));`).Check(testkit.Rows("1"))
	tk.MustQuery(`SELECT IS_IPV4_COMPAT(INET6_ATON('::ffff:10.0.5.9'));`).Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT
	  IS_IPV4_COMPAT(INET6_ATON('::192.168.0.1')),
	  IS_IPV4_COMPAT(INET6_ATON('::c0a8:0001')),
	  IS_IPV4_COMPAT(INET6_ATON('::c0a8:1'));`).Check(testkit.Rows("1 1 1"))
	tk.MustQuery(`SELECT IS_IPV4_MAPPED(INET6_ATON('::10.0.5.9'));`).Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT IS_IPV4_MAPPED(INET6_ATON('::ffff:10.0.5.9'));`).Check(testkit.Rows("1"))
	tk.MustQuery(`SELECT
	  IS_IPV4_MAPPED(INET6_ATON('::ffff:192.168.0.1')),
	  IS_IPV4_MAPPED(INET6_ATON('::ffff:c0a8:0001')),
	  IS_IPV4_MAPPED(INET6_ATON('::ffff:c0a8:1'));`).Check(testkit.Rows("1 1 1"))
	tk.MustQuery(`SELECT IS_IPV6('10.0.5.9'), IS_IPV6('::1');`).Check(testkit.Rows("0 1"))

	tk.MustExec("drop table if exists t1;")
	tk.MustExec(`create table t1(
        a int,
        b int not null,
        c int not null default 0,
        d int default 0,
        unique key(b,c),
        unique key(b,d)
);`)
	tk.MustExec("insert into t1 (a,b) values(1,10),(1,20),(2,30),(2,40);")
	tk.MustQuery("select any_value(a), sum(b) from t1;").Check(testkit.Rows("1 100"))
	tk.MustQuery("select a,any_value(b),sum(c) from t1 group by a order by a;").Check(testkit.Rows("1 10 0", "2 30 0"))

	// for locks
	result := tk.MustQuery(`SELECT GET_LOCK('test_lock1', 10);`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`SELECT GET_LOCK('test_lock2', 10);`)
	result.Check(testkit.Rows("1"))

	result = tk.MustQuery(`SELECT RELEASE_LOCK('test_lock2');`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`SELECT RELEASE_LOCK('test_lock1');`)
	result.Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite) TestConvertToBit(c *C) {
	defer s.cleanEnv(c)
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
	ctx := context.Background()
	defer s.cleanEnv(c)
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
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a decimal(40,20) UNSIGNED);")
	tk.MustExec("insert into t values(2.99999999900000000000), (12), (0);")
	tk.MustQuery("select a, ceil(a) from t where ceil(a) > 1;").Check(testkit.Rows("2.99999999900000000000 3", "12.00000000000000000000 12"))
	tk.MustQuery("select a, ceil(a) from t;").Check(testkit.Rows("2.99999999900000000000 3", "12.00000000000000000000 12", "0.00000000000000000000 0"))
	tk.MustQuery("select ceil(-29464);").Check(testkit.Rows("-29464"))
	tk.MustQuery("select a, floor(a) from t where floor(a) > 1;").Check(testkit.Rows("2.99999999900000000000 2", "12.00000000000000000000 12"))
	tk.MustQuery("select a, floor(a) from t;").Check(testkit.Rows("2.99999999900000000000 2", "12.00000000000000000000 12", "0.00000000000000000000 0"))
	tk.MustQuery("select floor(-29464);").Check(testkit.Rows("-29464"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a decimal(40,20), b bigint);`)
	tk.MustExec(`insert into t values(-2.99999990000000000000, -1);`)
	tk.MustQuery(`select floor(a), floor(a), floor(a) from t;`).Check(testkit.Rows(`-3 -3 -3`))
	tk.MustQuery(`select b, floor(b) from t;`).Check(testkit.Rows(`-1 -1`))

	// for cot
	result = tk.MustQuery("select cot(1), cot(-1), cot(NULL)")
	result.Check(testkit.Rows("0.6420926159343308 -0.6420926159343308 <nil>"))
	result = tk.MustQuery("select cot('1tidb')")
	result.Check(testkit.Rows("0.6420926159343308"))
	rs, err := tk.Exec("select cot(0)")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	terr := errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrDataOutOfRange))
	c.Assert(rs.Close(), IsNil)

	//for exp
	result = tk.MustQuery("select exp(0), exp(1), exp(-1), exp(1.2), exp(NULL)")
	result.Check(testkit.Rows("1 2.718281828459045 0.36787944117144233 3.3201169227365472 <nil>"))
	result = tk.MustQuery("select exp('tidb'), exp('1tidb')")
	result.Check(testkit.Rows("1 2.718281828459045"))
	rs, err = tk.Exec("select exp(1000000)")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrDataOutOfRange))
	c.Assert(rs.Close(), IsNil)

	// for conv
	result = tk.MustQuery("SELECT CONV('a', 16, 2);")
	result.Check(testkit.Rows("1010"))
	result = tk.MustQuery("SELECT CONV('6E', 18, 8);")
	result.Check(testkit.Rows("172"))
	result = tk.MustQuery("SELECT CONV(-17, 10, -18);")
	result.Check(testkit.Rows("-H"))
	result = tk.MustQuery("SELECT CONV(10+'10'+'10'+X'0a', 10, 10);")
	result.Check(testkit.Rows("40"))
	result = tk.MustQuery("SELECT CONV('a', 1, 10);")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT CONV('a', 37, 10);")
	result.Check(testkit.Rows("<nil>"))

	// for abs
	result = tk.MustQuery("SELECT ABS(-1);")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("SELECT ABS('abc');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT ABS(18446744073709551615);")
	result.Check(testkit.Rows("18446744073709551615"))
	result = tk.MustQuery("SELECT ABS(123.4);")
	result.Check(testkit.Rows("123.4"))
	result = tk.MustQuery("SELECT ABS(-123.4);")
	result.Check(testkit.Rows("123.4"))
	result = tk.MustQuery("SELECT ABS(1234E-1);")
	result.Check(testkit.Rows("123.4"))
	result = tk.MustQuery("SELECT ABS(-9223372036854775807);")
	result.Check(testkit.Rows("9223372036854775807"))
	result = tk.MustQuery("SELECT ABS(NULL);")
	result.Check(testkit.Rows("<nil>"))
	rs, err = tk.Exec("SELECT ABS(-9223372036854775808);")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrDataOutOfRange))
	c.Assert(rs.Close(), IsNil)

	// for round
	result = tk.MustQuery("SELECT ROUND(2.5), ROUND(-2.5), ROUND(25E-1);")
	result.Check(testkit.Rows("3 -3 3")) // TODO: Should be 3 -3 2
	result = tk.MustQuery("SELECT ROUND(2.5, NULL), ROUND(NULL, 4), ROUND(NULL, NULL), ROUND(NULL);")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("SELECT ROUND('123.4'), ROUND('123e-2');")
	result.Check(testkit.Rows("123 1"))
	result = tk.MustQuery("SELECT ROUND(-9223372036854775808);")
	result.Check(testkit.Rows("-9223372036854775808"))
	result = tk.MustQuery("SELECT ROUND(123.456, 0), ROUND(123.456, 1), ROUND(123.456, 2), ROUND(123.456, 3), ROUND(123.456, 4), ROUND(123.456, -1), ROUND(123.456, -2), ROUND(123.456, -3), ROUND(123.456, -4);")
	result.Check(testkit.Rows("123 123.5 123.46 123.456 123.4560 120 100 0 0"))
	result = tk.MustQuery("SELECT ROUND(123456E-3, 0), ROUND(123456E-3, 1), ROUND(123456E-3, 2), ROUND(123456E-3, 3), ROUND(123456E-3, 4), ROUND(123456E-3, -1), ROUND(123456E-3, -2), ROUND(123456E-3, -3), ROUND(123456E-3, -4);")
	result.Check(testkit.Rows("123 123.5 123.46 123.456 123.456 120 100 0 0")) // TODO: Column 5 should be 123.4560

	// for truncate
	result = tk.MustQuery("SELECT truncate(123, -2), truncate(123, 2), truncate(123, 1), truncate(123, -1);")
	result.Check(testkit.Rows("100 123 123 120"))
	result = tk.MustQuery("SELECT truncate(123.456, -2), truncate(123.456, 2), truncate(123.456, 1), truncate(123.456, 3), truncate(1.23, 100), truncate(123456E-3, 2);")
	result.Check(testkit.Rows("100 123.45 123.4 123.456 1.230000000000000000000000000000 123.45"))
	result = tk.MustQuery("SELECT truncate(9223372036854775807, -7), truncate(9223372036854775808, -10), truncate(cast(-1 as unsigned), -10);")
	result.Check(testkit.Rows("9223372036850000000 9223372030000000000 18446744070000000000"))

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a date, b datetime, c timestamp, d varchar(20));`)
	tk.MustExec(`insert into t select "1234-12-29", "1234-12-29 16:24:13.9912", "2014-12-29 16:19:28", "12.34567";`)

	// NOTE: the actually result is: 12341220 12341229.0 12341200 12341229.00,
	// but Datum.ToString() don't format decimal length for float numbers.
	result = tk.MustQuery(`select truncate(a, -1), truncate(a, 1), truncate(a, -2), truncate(a, 2) from t;`)
	result.Check(testkit.Rows("12341220 12341229 12341200 12341229"))

	// NOTE: the actually result is: 12341229162410 12341229162414.0 12341229162400 12341229162414.00,
	// but Datum.ToString() don't format decimal length for float numbers.
	result = tk.MustQuery(`select truncate(b, -1), truncate(b, 1), truncate(b, -2), truncate(b, 2) from t;`)
	result.Check(testkit.Rows("12341229162410 12341229162414 12341229162400 12341229162414"))

	// NOTE: the actually result is: 20141229161920 20141229161928.0 20141229161900 20141229161928.00,
	// but Datum.ToString() don't format decimal length for float numbers.
	result = tk.MustQuery(`select truncate(c, -1), truncate(c, 1), truncate(c, -2), truncate(c, 2) from t;`)
	result.Check(testkit.Rows("20141229161920 20141229161928 20141229161900 20141229161928"))

	result = tk.MustQuery(`select truncate(d, -1), truncate(d, 1), truncate(d, -2), truncate(d, 2) from t;`)
	result.Check(testkit.Rows("10 12.3 0 12.34"))

	// for pow
	result = tk.MustQuery("SELECT POW('12', 2), POW(1.2e1, '2.0'), POW(12, 2.0);")
	result.Check(testkit.Rows("144 144 144"))
	result = tk.MustQuery("SELECT POW(null, 2), POW(2, null), POW(null, null);")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery("SELECT POW(0, 0);")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("SELECT POW(0, 0.1), POW(0, 0.5), POW(0, 1);")
	result.Check(testkit.Rows("0 0 0"))
	rs, err = tk.Exec("SELECT POW(0, -1);")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	terr = errors.Cause(err).(*terror.Error)
	c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrDataOutOfRange))
	c.Assert(rs.Close(), IsNil)

	// for sign
	result = tk.MustQuery("SELECT SIGN('12'), SIGN(1.2e1), SIGN(12), SIGN(0.0000012);")
	result.Check(testkit.Rows("1 1 1 1"))
	result = tk.MustQuery("SELECT SIGN('-12'), SIGN(-1.2e1), SIGN(-12), SIGN(-0.0000012);")
	result.Check(testkit.Rows("-1 -1 -1 -1"))
	result = tk.MustQuery("SELECT SIGN('0'), SIGN('-0'), SIGN(0);")
	result.Check(testkit.Rows("0 0 0"))
	result = tk.MustQuery("SELECT SIGN(NULL);")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT SIGN(-9223372036854775808), SIGN(9223372036854775808);")
	result.Check(testkit.Rows("-1 1"))

	// for sqrt
	result = tk.MustQuery("SELECT SQRT(-10), SQRT(144), SQRT(4.84), SQRT(0.04), SQRT(0);")
	result.Check(testkit.Rows("<nil> 12 2.2 0.2 0"))

	// for crc32
	result = tk.MustQuery("SELECT crc32(0), crc32(-0), crc32('0'), crc32('abc'), crc32('ABC'), crc32(NULL), crc32(''), crc32('hello world!')")
	result.Check(testkit.Rows("4108050209 4108050209 4108050209 891568578 2743272264 <nil> 0 62177901"))

	// for radians
	result = tk.MustQuery("SELECT radians(1.0), radians(pi()), radians(pi()/2), radians(180), radians(1.009);")
	result.Check(testkit.Rows("0.017453292519943295 0.05483113556160754 0.02741556778080377 3.141592653589793 0.01761037215262278"))

	// for rand
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1),(2),(3)")
	tk.MustQuery("select rand(a) from t").Check(testkit.Rows("0.6046602879796196", "0.16729663442585624", "0.7199826688373036"))
	tk.MustQuery("select rand(1), rand(2), rand(3)").Check(testkit.Rows("0.6046602879796196 0.16729663442585624 0.7199826688373036"))
}

func (s *testIntegrationSuite) TestStringBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	ctx := context.Background()

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
	tk.MustExec("drop table if exists t")
	// Fix issue 9123
	tk.MustExec("create table t(a char(32) not null, b float default '0') engine=innodb default charset=utf8mb4")
	tk.MustExec("insert into t value('0a6f9d012f98467f8e671e9870044528', 208.867)")
	result = tk.MustQuery("select concat_ws( ',', b) from t where a = '0a6f9d012f98467f8e671e9870044528';")
	result.Check(testkit.Rows("208.867"))

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

	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a tinyint(2), b varchar(10));`)
	tk.MustExec(`insert into t values (1, 'a'), (12, 'a'), (126, 'a'), (127, 'a')`)
	tk.MustQuery(`select concat_ws('#', a, b) from t;`).Check(testkit.Rows(
		`1#a`,
		`12#a`,
		`126#a`,
		`127#a`,
	))

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
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a binary(10))")
	tk.MustExec(`insert into t select "中文abc"`)
	result = tk.MustQuery(`select left(a, 3), left(a, 6), left(a, 7) from t`)
	result.Check(testkit.Rows("中 中文 中文a"))
	result = tk.MustQuery(`select right(a, 2), right(a, 7) from t`)
	result.Check(testkit.Rows("c\x00 文abc\x00"))

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

	// for from_base64
	result = tk.MustQuery(`select from_base64("abcd"), from_base64("asc")`)
	result.Check(testkit.Rows("i\xb7\x1d <nil>"))
	result = tk.MustQuery(`select from_base64("MQ=="), from_base64(1234)`)
	result.Check(testkit.Rows("1 \xd7m\xf8"))

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
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a binary(10))")
	tk.MustExec(`insert into t select "中文abc"`)
	result = tk.MustQuery(`select substr(a, 4), substr(a, 1, 3), substr(a, 1, 6) from t`)
	result.Check(testkit.Rows("文abc\x00 中 中文"))
	result = tk.MustQuery(`select substr("string", -1), substr("string", -2), substr("中文", -1), substr("中文", -2) from t`)
	result.Check(testkit.Rows("g ng 文 中文"))

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
	tk.MustQuery(`select substring_index('xyz', 'abc', 9223372036854775808)`).Check(testkit.Rows(``))
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
	result = tk.MustQuery(`select ltrim("\t   bar   "), ltrim("   \tbar"), ltrim("\n  bar"), ltrim("\r  bar")`)
	result.Check(testutil.RowsWithSep(",", "\t   bar   ,\tbar,\n  bar,\r  bar"))
	result = tk.MustQuery(`select rtrim("   bar   \t"), rtrim("bar\t   "), rtrim("bar   \n"), rtrim("bar   \r")`)
	result.Check(testutil.RowsWithSep(",", "   bar   \t,bar\t,bar   \n,bar   \r"))

	// for reverse
	tk.MustExec(`DROP TABLE IF EXISTS t;`)
	tk.MustExec(`CREATE TABLE t(a BINARY(6));`)
	tk.MustExec(`INSERT INTO t VALUES("中文");`)
	result = tk.MustQuery(`SELECT a, REVERSE(a), REVERSE("中文"), REVERSE("123 ") FROM t;`)
	result.Check(testkit.Rows("中文 \x87\x96歸\xe4 文中  321"))
	result = tk.MustQuery(`SELECT REVERSE(123), REVERSE(12.09) FROM t;`)
	result.Check(testkit.Rows("321 90.21"))

	// for trim
	result = tk.MustQuery(`select trim('   bar   '), trim(leading 'x' from 'xxxbarxxx'), trim(trailing 'xyz' from 'barxxyz'), trim(both 'x' from 'xxxbarxxx')`)
	result.Check(testkit.Rows("bar barxxx barx bar"))
	result = tk.MustQuery(`select trim('\t   bar\n   '), trim('   \rbar   \t')`)
	result.Check(testutil.RowsWithSep(",", "\t   bar\n,\rbar   \t"))
	result = tk.MustQuery(`select trim(leading from '   bar'), trim('x' from 'xxxbarxxx'), trim('x' from 'bar'), trim('' from '   bar   ')`)
	result.Check(testutil.RowsWithSep(",", "bar,bar,bar,   bar   "))
	result = tk.MustQuery(`select trim(''), trim('x' from '')`)
	result.Check(testutil.RowsWithSep(",", ","))
	result = tk.MustQuery(`select trim(null from 'bar'), trim('x' from null), trim(null), trim(leading null from 'bar')`)
	// FIXME: the result for trim(leading null from 'bar') should be <nil>, current is 'bar'
	result.Check(testkit.Rows("<nil> <nil> <nil> bar"))

	// for locate
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(20), b int, c double, d datetime, e time, f binary(5))")
	tk.MustExec(`insert into t values('www.pingcap.com', 12345, 123.45, "2017-01-01 12:01:01", "12:01:01", "HelLo")`)
	result = tk.MustQuery(`select locate(".ping", a), locate(".ping", a, 5) from t`)
	result.Check(testkit.Rows("4 0"))
	result = tk.MustQuery(`select locate("234", b), locate("235", b, 10) from t`)
	result.Check(testkit.Rows("2 0"))
	result = tk.MustQuery(`select locate(".45", c), locate(".35", b) from t`)
	result.Check(testkit.Rows("4 0"))
	result = tk.MustQuery(`select locate("El", f), locate("ll", f), locate("lL", f), locate("Lo", f), locate("lo", f) from t`)
	result.Check(testkit.Rows("0 0 3 4 0"))
	result = tk.MustQuery(`select locate("01 12", d) from t`)
	result.Check(testkit.Rows("9"))
	result = tk.MustQuery(`select locate("文", "中文字符串", 2)`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`select locate("文", "中文字符串", 3)`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select locate("文", "中文字符串")`)
	result.Check(testkit.Rows("2"))

	// for bin
	result = tk.MustQuery(`select bin(-1);`)
	result.Check(testkit.Rows("1111111111111111111111111111111111111111111111111111111111111111"))
	result = tk.MustQuery(`select bin(5);`)
	result.Check(testkit.Rows("101"))
	result = tk.MustQuery(`select bin("中文");`)
	result.Check(testkit.Rows("0"))

	// for character_length
	result = tk.MustQuery(`select character_length(null), character_length("Hello"), character_length("a中b文c"),
	character_length(123), character_length(12.3456);`)
	result.Check(testkit.Rows("<nil> 5 5 3 7"))

	// for char_length
	result = tk.MustQuery(`select char_length(null), char_length("Hello"), char_length("a中b文c"), char_length(123),char_length(12.3456);`)
	result.Check(testkit.Rows("<nil> 5 5 3 7"))
	result = tk.MustQuery(`select char_length(null), char_length("Hello"), char_length("a 中 b 文 c"), char_length("НОЧЬ НА ОКРАИНЕ МОСКВЫ");`)
	result.Check(testkit.Rows("<nil> 5 9 22"))
	// for char_length, binary string type
	result = tk.MustQuery(`select char_length(null), char_length(binary("Hello")), char_length(binary("a 中 b 文 c")), char_length(binary("НОЧЬ НА ОКРАИНЕ МОСКВЫ"));`)
	result.Check(testkit.Rows("<nil> 5 13 41"))

	// for elt
	result = tk.MustQuery(`select elt(0, "abc", "def"), elt(2, "hello", "中文", "tidb"), elt(4, "hello", "中文",
	"tidb");`)
	result.Check(testkit.Rows("<nil> 中文 <nil>"))

	// for instr
	result = tk.MustQuery(`select instr("中国", "国"), instr("中国", ""), instr("abc", ""), instr("", ""), instr("", "abc");`)
	result.Check(testkit.Rows("2 1 1 1 0"))
	result = tk.MustQuery(`select instr("中国", null), instr(null, ""), instr(null, null);`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a binary(20), b char(20));`)
	tk.MustExec(`insert into t values("中国", cast("国" as binary)), ("中国", ""), ("abc", ""), ("", ""), ("", "abc");`)
	result = tk.MustQuery(`select instr(a, b) from t;`)
	result.Check(testkit.Rows("4", "1", "1", "1", "0"))

	// for oct
	result = tk.MustQuery(`select oct("aaaa"), oct("-1.9"),  oct("-9999999999999999999999999"), oct("9999999999999999999999999");`)
	result.Check(testkit.Rows("0 1777777777777777777777 1777777777777777777777 1777777777777777777777"))
	result = tk.MustQuery(`select oct(-1.9), oct(1.9), oct(-1), oct(1), oct(-9999999999999999999999999), oct(9999999999999999999999999);`)
	result.Check(testkit.Rows("1777777777777777777777 1 1777777777777777777777 1 1777777777777777777777 1777777777777777777777"))

	// #issue 4356
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (b BIT(8));")
	tk.MustExec(`INSERT INTO t SET b = b'11111111';`)
	tk.MustExec(`INSERT INTO t SET b = b'1010';`)
	tk.MustExec(`INSERT INTO t SET b = b'0101';`)
	result = tk.MustQuery(`SELECT b+0, BIN(b), OCT(b), HEX(b) FROM t;`)
	result.Check(testkit.Rows("255 11111111 377 FF", "10 1010 12 A", "5 101 5 5"))

	// for find_in_set
	result = tk.MustQuery(`select find_in_set("", ""), find_in_set("", ","), find_in_set("中文", "字符串,中文"), find_in_set("b,", "a,b,c,d");`)
	result.Check(testkit.Rows("0 1 2 0"))
	result = tk.MustQuery(`select find_in_set(NULL, ""), find_in_set("", NULL), find_in_set(1, "2,3,1");`)
	result.Check(testkit.Rows("<nil> <nil> 3"))

	// for make_set
	result = tk.MustQuery(`select make_set(0, "12"), make_set(3, "aa", "11"), make_set(3, NULL, "中文"), make_set(NULL, "aa");`)
	result.Check(testkit.Rows(" aa,11 中文 <nil>"))

	// for quote
	result = tk.MustQuery(`select quote("aaaa"), quote(""), quote("\"\""), quote("\n\n");`)
	result.Check(testkit.Rows("'aaaa' '' '\"\"' '\n\n'"))
	result = tk.MustQuery(`select quote(0121), quote(0000), quote("中文"), quote(NULL);`)
	result.Check(testkit.Rows("'121' '0' '中文' <nil>"))

	// for convert
	result = tk.MustQuery(`select convert("123" using "866"), convert("123" using "binary"), convert("中文" using "binary"), convert("中文" using "utf8"), convert("中文" using "utf8mb4"), convert(cast("中文" as binary) using "utf8");`)
	result.Check(testkit.Rows("123 123 中文 中文 中文 中文"))

	// for insert
	result = tk.MustQuery(`select insert("中文", 1, 1, cast("aaa" as binary)), insert("ba", -1, 1, "aaa"), insert("ba", 1, 100, "aaa"), insert("ba", 100, 1, "aaa");`)
	result.Check(testkit.Rows("aaa文 ba aaa ba"))
	result = tk.MustQuery(`select insert("bb", NULL, 1, "aa"), insert("bb", 1, NULL, "aa"), insert(NULL, 1, 1, "aaa"), insert("bb", 1, 1, NULL);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))

	// for export_set
	result = tk.MustQuery(`select export_set(7, "1", "0", ",", 65);`)
	result.Check(testkit.Rows("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(7, "1", "0", ",", -1);`)
	result.Check(testkit.Rows("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(7, "1", "0", ",");`)
	result.Check(testkit.Rows("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(7, "1", "0");`)
	result.Check(testkit.Rows("1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0"))
	result = tk.MustQuery(`select export_set(NULL, "1", "0", ",", 65);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select export_set(7, "1", "0", ",", 1);`)
	result.Check(testkit.Rows("1"))

	// for format
	result = tk.MustQuery(`select format(12332.1, 4), format(12332.2, 0), format(12332.2, 2,'en_US');`)
	result.Check(testkit.Rows("12,332.1000 12,332 12,332.20"))
	result = tk.MustQuery(`select format(NULL, 4), format(12332.2, NULL);`)
	result.Check(testkit.Rows("<nil> <nil>"))
	rs, err := tk.Exec(`select format(12332.2, 2,'es_EC');`)
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Matches, "not support for the specific locale")
	c.Assert(rs.Close(), IsNil)

	// for field
	result = tk.MustQuery(`select field(1, 2, 1), field(1, 0, NULL), field(1, NULL, 2, 1), field(NULL, 1, 2, NULL);`)
	result.Check(testkit.Rows("2 0 3 0"))
	result = tk.MustQuery(`select field("1", 2, 1), field(1, "0", NULL), field("1", NULL, 2, 1), field(NULL, 1, "2", NULL);`)
	result.Check(testkit.Rows("2 0 3 0"))
	result = tk.MustQuery(`select field("1", 2, 1), field(1, "abc", NULL), field("1", NULL, 2, 1), field(NULL, 1, "2", NULL);`)
	result.Check(testkit.Rows("2 0 3 0"))
	result = tk.MustQuery(`select field("abc", "a", 1), field(1.3, "1.3", 1.5);`)
	result.Check(testkit.Rows("1 1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a decimal(11, 8), b decimal(11,8))")
	tk.MustExec("insert into t values('114.57011441','38.04620115'), ('-38.04620119', '38.04620115');")
	result = tk.MustQuery("select a,b,concat_ws(',',a,b) from t")
	result.Check(testkit.Rows("114.57011441 38.04620115 114.57011441,38.04620115",
		"-38.04620119 38.04620115 -38.04620119,38.04620115"))
}

func (s *testIntegrationSuite) TestEncryptionBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	ctx := context.Background()

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

	// for sha/sha1
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select sha1(a), sha1(b), sha1(c), sha1(d), sha1(e), sha1(f), sha1(g), sha1(h), sha1(i) from t")
	result.Check(testkit.Rows("da4b9237bacccdf19c0760cab7aec4a8359010b0 da4b9237bacccdf19c0760cab7aec4a8359010b0 ce0d88c5002b6cf7664052f1fc7d652cbdadccec 6c6956de323692298e4e5ad3028ff491f7ad363c 1906f8aeb5a717ca0f84154724045839330b0ea9 adc83b19e793491b1c6ea0fd8b46cd9f32e592fc 9aadd14ceb737b28697b8026f205f4b3e31de147 64e095fe763fc62418378753f9402623bea9e227 4df56fc09a3e66b48fb896e90b0a6fc02c978e9e"))
	result = tk.MustQuery("select sha1('123'), sha1(123), sha1(''), sha1('你好'), sha1(NULL)")
	result.Check(testkit.Rows(`40bd001563085fc35165329ea1ff5c5ecbdbbeef 40bd001563085fc35165329ea1ff5c5ecbdbbeef da39a3ee5e6b4b0d3255bfef95601890afd80709 440ee0853ad1e99f962b63e459ef992d7c211722 <nil>`))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select sha(a), sha(b), sha(c), sha(d), sha(e), sha(f), sha(g), sha(h), sha(i) from t")
	result.Check(testkit.Rows("da4b9237bacccdf19c0760cab7aec4a8359010b0 da4b9237bacccdf19c0760cab7aec4a8359010b0 ce0d88c5002b6cf7664052f1fc7d652cbdadccec 6c6956de323692298e4e5ad3028ff491f7ad363c 1906f8aeb5a717ca0f84154724045839330b0ea9 adc83b19e793491b1c6ea0fd8b46cd9f32e592fc 9aadd14ceb737b28697b8026f205f4b3e31de147 64e095fe763fc62418378753f9402623bea9e227 4df56fc09a3e66b48fb896e90b0a6fc02c978e9e"))
	result = tk.MustQuery("select sha('123'), sha(123), sha(''), sha('你好'), sha(NULL)")
	result.Check(testkit.Rows(`40bd001563085fc35165329ea1ff5c5ecbdbbeef 40bd001563085fc35165329ea1ff5c5ecbdbbeef da39a3ee5e6b4b0d3255bfef95601890afd80709 440ee0853ad1e99f962b63e459ef992d7c211722 <nil>`))

	// for sha2
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	result = tk.MustQuery("select sha2(a, 224), sha2(b, 0), sha2(c, 512), sha2(d, 256), sha2(e, 384), sha2(f, 0), sha2(g, 512), sha2(h, 256), sha2(i, 224) from t")
	result.Check(testkit.Rows("58b2aaa0bfae7acc021b3260e941117b529b2e69de878fd7d45c61a9 d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35 42415572557b0ca47e14fa928e83f5746d33f90c74270172cc75c61a78db37fe1485159a4fd75f33ab571b154572a5a300938f7d25969bdd05d8ac9dd6c66123 8c2fa3f276952c92b0b40ed7d27454e44b8399a19769e6bceb40da236e45a20a b11d35f1a37e54d5800d210d8e6b80b42c9f6d20ea7ae548c762383ebaa12c5954c559223c6c7a428e37af96bb4f1e0d 01ba4719c80b6fe911b091a7c05124b64eeece964e09c058ef8f9805daca546b 9550da35ea1683abaf5bfa8de68fe02b9c6d756c64589d1ef8367544c254f5f09218a6466cadcee8d74214f0c0b7fb342d1a9f3bd4d406aacf7be59c327c9306 98010bd9270f9b100b6214a21754fd33bdc8d41b2bc9f9dd16ff54d3c34ffd71 a7cddb7346fbc66ab7f803e865b74cbd99aace8e7dabbd8884c148cb"))
	result = tk.MustQuery("select sha2('123', 512), sha2(123, 512), sha2('', 512), sha2('你好', 224), sha2(NULL, 256), sha2('foo', 123)")
	result.Check(testkit.Rows(`3c9909afec25354d551dae21590bb26e38d53f2173b8d3dc3eee4c047e7ab1c1eb8b85103e3be7ba613b31bb5c9c36214dc9f14a42fd7a2fdb84856bca5c44c2 3c9909afec25354d551dae21590bb26e38d53f2173b8d3dc3eee4c047e7ab1c1eb8b85103e3be7ba613b31bb5c9c36214dc9f14a42fd7a2fdb84856bca5c44c2 cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e e91f006ed4e0882de2f6a3c96ec228a6a5c715f356d00091bce842b5 <nil> <nil>`))

	// for AES_ENCRYPT
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b int, c double, d datetime, e time, f bit(4), g binary(20), h blob(10), i text(30))")
	tk.MustExec(`insert into t values('2', 2, 2.3, "2017-01-01 12:01:01", "12:01:01", 0b1010, "512", "48", "tidb")`)
	tk.MustExec("SET block_encryption_mode='aes-128-ecb';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key')), HEX(AES_ENCRYPT(b, 'key')), HEX(AES_ENCRYPT(c, 'key')), HEX(AES_ENCRYPT(d, 'key')), HEX(AES_ENCRYPT(e, 'key')), HEX(AES_ENCRYPT(f, 'key')), HEX(AES_ENCRYPT(g, 'key')), HEX(AES_ENCRYPT(h, 'key')), HEX(AES_ENCRYPT(i, 'key')) from t")
	result.Check(testkit.Rows("B3800B3A3CB4ECE2051A3E80FE373EAC B3800B3A3CB4ECE2051A3E80FE373EAC 9E018F7F2838DBA23C57F0E4CCF93287 E764D3E9D4AF8F926CD0979DDB1D0AF40C208B20A6C39D5D028644885280973A C452FFEEB76D3F5E9B26B8D48F7A228C 181BD5C81CBD36779A3C9DD5FF486B35 CE15F14AC7FF4E56ECCF148DE60E4BEDBDB6900AD51383970A5F32C59B3AC6E3 E1B29995CCF423C75519790F54A08CD2 84525677E95AC97698D22E1125B67E92"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar')), HEX(AES_ENCRYPT(123, 'foobar')), HEX(AES_ENCRYPT('', 'foobar')), HEX(AES_ENCRYPT('你好', 'foobar')), AES_ENCRYPT(NULL, 'foobar')")
	result.Check(testkit.Rows(`45ABDD5C4802EFA6771A94C43F805208 45ABDD5C4802EFA6771A94C43F805208 791F1AEB6A6B796E6352BF381895CA0E D0147E2EB856186F146D9F6DE33F9546 <nil>`))
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', 'iv')), HEX(AES_ENCRYPT(b, 'key', 'iv')) from t")
	result.Check(testkit.Rows("B3800B3A3CB4ECE2051A3E80FE373EAC B3800B3A3CB4ECE2051A3E80FE373EAC"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1618|<IV> option ignored", "Warning|1618|<IV> option ignored"))
	tk.MustExec("SET block_encryption_mode='aes-128-cbc';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', '1234567890123456')), HEX(AES_ENCRYPT(b, 'key', '1234567890123456')), HEX(AES_ENCRYPT(c, 'key', '1234567890123456')), HEX(AES_ENCRYPT(d, 'key', '1234567890123456')), HEX(AES_ENCRYPT(e, 'key', '1234567890123456')), HEX(AES_ENCRYPT(f, 'key', '1234567890123456')), HEX(AES_ENCRYPT(g, 'key', '1234567890123456')), HEX(AES_ENCRYPT(h, 'key', '1234567890123456')), HEX(AES_ENCRYPT(i, 'key', '1234567890123456')) from t")
	result.Check(testkit.Rows("341672829F84CB6B0BE690FEC4C4DAE9 341672829F84CB6B0BE690FEC4C4DAE9 D43734E147A12BB96C6897C4BBABA283 16F2C972411948DCEF3659B726D2CCB04AD1379A1A367FA64242058A50211B67 41E71D0C58967C1F50EEC074523946D1 1117D292E2D39C3EAA3B435371BE56FC 8ACB7ECC0883B672D7BD1CFAA9FA5FAF5B731ADE978244CD581F114D591C2E7E D2B13C30937E3251AEDA73859BA32E4B 2CF4A6051FF248A67598A17AA2C17267"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT(123, 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('你好', 'foobar', '1234567890123456')), AES_ENCRYPT(NULL, 'foobar', '1234567890123456')")
	result.Check(testkit.Rows(`80D5646F07B4654B05A02D9085759770 80D5646F07B4654B05A02D9085759770 B3C14BA15030D2D7E99376DBE011E752 0CD2936EE4FEC7A8CDF6208438B2BC05 <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-128-ofb';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', '1234567890123456')), HEX(AES_ENCRYPT(b, 'key', '1234567890123456')), HEX(AES_ENCRYPT(c, 'key', '1234567890123456')), HEX(AES_ENCRYPT(d, 'key', '1234567890123456')), HEX(AES_ENCRYPT(e, 'key', '1234567890123456')), HEX(AES_ENCRYPT(f, 'key', '1234567890123456')), HEX(AES_ENCRYPT(g, 'key', '1234567890123456')), HEX(AES_ENCRYPT(h, 'key', '1234567890123456')), HEX(AES_ENCRYPT(i, 'key', '1234567890123456')) from t")
	result.Check(testkit.Rows("40 40 40C35C 40DD5EBDFCAA397102386E27DDF97A39ECCEC5 43DF55BAE0A0386D 78 47DC5D8AD19A085C32094E16EFC34A08D6FEF459 46D5 06840BE8"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT(123, 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('你好', 'foobar', '1234567890123456')), AES_ENCRYPT(NULL, 'foobar', '1234567890123456')")
	result.Check(testkit.Rows(`48E38A 48E38A  9D6C199101C3 <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-192-ofb';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', '1234567890123456')), HEX(AES_ENCRYPT(b, 'key', '1234567890123456')), HEX(AES_ENCRYPT(c, 'key', '1234567890123456')), HEX(AES_ENCRYPT(d, 'key', '1234567890123456')), HEX(AES_ENCRYPT(e, 'key', '1234567890123456')), HEX(AES_ENCRYPT(f, 'key', '1234567890123456')), HEX(AES_ENCRYPT(g, 'key', '1234567890123456')), HEX(AES_ENCRYPT(h, 'key', '1234567890123456')), HEX(AES_ENCRYPT(i, 'key', '1234567890123456')) from t")
	result.Check(testkit.Rows("4B 4B 4B573F 4B493D42572E6477233A429BF3E0AD39DB816D 484B36454B24656B 73 4C483E757A1E555A130B62AAC1DA9D08E1B15C47 4D41 0D106817"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT(123, 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('你好', 'foobar', '1234567890123456')), AES_ENCRYPT(NULL, 'foobar', '1234567890123456')")
	result.Check(testkit.Rows(`3A76B0 3A76B0  EFF92304268E <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-256-ofb';")
	result = tk.MustQuery("select HEX(AES_ENCRYPT(a, 'key', '1234567890123456')), HEX(AES_ENCRYPT(b, 'key', '1234567890123456')), HEX(AES_ENCRYPT(c, 'key', '1234567890123456')), HEX(AES_ENCRYPT(d, 'key', '1234567890123456')), HEX(AES_ENCRYPT(e, 'key', '1234567890123456')), HEX(AES_ENCRYPT(f, 'key', '1234567890123456')), HEX(AES_ENCRYPT(g, 'key', '1234567890123456')), HEX(AES_ENCRYPT(h, 'key', '1234567890123456')), HEX(AES_ENCRYPT(i, 'key', '1234567890123456')) from t")
	result.Check(testkit.Rows("16 16 16D103 16CF01CBC95D33E2ED721CBD930262415A69AD 15CD0ACCD55732FE 2E 11CE02FCE46D02CFDD433C8CA138527060599C35 10C7 5096549E"))
	result = tk.MustQuery("select HEX(AES_ENCRYPT('123', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT(123, 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('', 'foobar', '1234567890123456')), HEX(AES_ENCRYPT('你好', 'foobar', '1234567890123456')), AES_ENCRYPT(NULL, 'foobar', '1234567890123456')")
	result.Check(testkit.Rows(`E842C5 E842C5  3DCD5646767D <nil>`))

	// for AES_DECRYPT
	tk.MustExec("SET block_encryption_mode='aes-128-ecb';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar'), 'bar')")
	result.Check(testkit.Rows("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('45ABDD5C4802EFA6771A94C43F805208'), 'foobar'), AES_DECRYPT(UNHEX('791F1AEB6A6B796E6352BF381895CA0E'), 'foobar'), AES_DECRYPT(UNHEX('D0147E2EB856186F146D9F6DE33F9546'), 'foobar'), AES_DECRYPT(NULL, 'foobar'), AES_DECRYPT('SOME_THING_STRANGE', 'foobar')")
	result.Check(testkit.Rows(`123  你好 <nil> <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-128-cbc';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar', '1234567890123456'), 'bar', '1234567890123456')")
	result.Check(testkit.Rows("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('80D5646F07B4654B05A02D9085759770'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('B3C14BA15030D2D7E99376DBE011E752'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('0CD2936EE4FEC7A8CDF6208438B2BC05'), 'foobar', '1234567890123456'), AES_DECRYPT(NULL, 'foobar', '1234567890123456'), AES_DECRYPT('SOME_THING_STRANGE', 'foobar', '1234567890123456')")
	result.Check(testkit.Rows(`123  你好 <nil> <nil>`))
	tk.MustExec("SET block_encryption_mode='aes-128-ofb';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar', '1234567890123456'), 'bar', '1234567890123456')")
	result.Check(testkit.Rows("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('48E38A'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX(''), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('9D6C199101C3'), 'foobar', '1234567890123456'), AES_DECRYPT(NULL, 'foobar', '1234567890123456'), HEX(AES_DECRYPT('SOME_THING_STRANGE', 'foobar', '1234567890123456'))")
	result.Check(testkit.Rows(`123  你好 <nil> 2A9EF431FB2ACB022D7F2E7C71EEC48C7D2B`))
	tk.MustExec("SET block_encryption_mode='aes-192-ofb';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar', '1234567890123456'), 'bar', '1234567890123456')")
	result.Check(testkit.Rows("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('3A76B0'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX(''), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('EFF92304268E'), 'foobar', '1234567890123456'), AES_DECRYPT(NULL, 'foobar', '1234567890123456'), HEX(AES_DECRYPT('SOME_THING_STRANGE', 'foobar', '1234567890123456'))")
	result.Check(testkit.Rows(`123  你好 <nil> 580BCEA4DC67CF33FF2C7C570D36ECC89437`))
	tk.MustExec("SET block_encryption_mode='aes-256-ofb';")
	result = tk.MustQuery("select AES_DECRYPT(AES_ENCRYPT('foo', 'bar', '1234567890123456'), 'bar', '1234567890123456')")
	result.Check(testkit.Rows("foo"))
	result = tk.MustQuery("select AES_DECRYPT(UNHEX('E842C5'), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX(''), 'foobar', '1234567890123456'), AES_DECRYPT(UNHEX('3DCD5646767D'), 'foobar', '1234567890123456'), AES_DECRYPT(NULL, 'foobar', '1234567890123456'), HEX(AES_DECRYPT('SOME_THING_STRANGE', 'foobar', '1234567890123456'))")
	result.Check(testkit.Rows(`123  你好 <nil> 8A3FBBE68C9465834584430E3AEEBB04B1F5`))

	// for COMPRESS
	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec("CREATE TABLE t1(a VARCHAR(1000));")
	tk.MustExec("INSERT INTO t1 VALUES('12345'), ('23456');")
	result = tk.MustQuery("SELECT HEX(COMPRESS(a)) FROM t1;")
	result.Check(testkit.Rows("05000000789C323432363105040000FFFF02F80100", "05000000789C323236313503040000FFFF03070105"))
	tk.MustExec("DROP TABLE IF EXISTS t2;")
	tk.MustExec("CREATE TABLE t2(a VARCHAR(1000), b VARBINARY(1000));")
	tk.MustExec("INSERT INTO t2 (a, b) SELECT a, COMPRESS(a) from t1;")
	result = tk.MustQuery("SELECT a, HEX(b) FROM t2;")
	result.Check(testkit.Rows("12345 05000000789C323432363105040000FFFF02F80100", "23456 05000000789C323236313503040000FFFF03070105"))

	// for UNCOMPRESS
	result = tk.MustQuery("SELECT UNCOMPRESS(COMPRESS('123'))")
	result.Check(testkit.Rows("123"))
	result = tk.MustQuery("SELECT UNCOMPRESS(UNHEX('03000000789C3334320600012D0097'))")
	result.Check(testkit.Rows("123"))
	result = tk.MustQuery("SELECT UNCOMPRESS(UNHEX('03000000789C32343206040000FFFF012D0097'))")
	result.Check(testkit.Rows("123"))
	tk.MustExec("INSERT INTO t2 VALUES ('12345', UNHEX('05000000789C3334323631050002F80100'))")
	result = tk.MustQuery("SELECT UNCOMPRESS(a), UNCOMPRESS(b) FROM t2;")
	result.Check(testkit.Rows("<nil> 12345", "<nil> 23456", "<nil> 12345"))

	// for UNCOMPRESSED_LENGTH
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(COMPRESS('123'))")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(UNHEX('03000000789C3334320600012D0097'))")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(UNHEX('03000000789C32343206040000FFFF012D0097'))")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH('')")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(UNHEX('0100'))")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNCOMPRESSED_LENGTH(a), UNCOMPRESSED_LENGTH(b) FROM t2;")
	result.Check(testkit.Rows("875770417 5", "892613426 5", "875770417 5"))

	// for RANDOM_BYTES
	lengths := []int{0, -5, 1025, 4000}
	for _, len := range lengths {
		rs, err := tk.Exec(fmt.Sprintf("SELECT RANDOM_BYTES(%d);", len))
		c.Assert(err, IsNil, Commentf("%v", len))
		_, err = session.GetRows4Test(ctx, tk.Se, rs)
		c.Assert(err, NotNil, Commentf("%v", len))
		terr := errors.Cause(err).(*terror.Error)
		c.Assert(terr.Code(), Equals, terror.ErrCode(mysql.ErrDataOutOfRange), Commentf("%v", len))
		c.Assert(rs.Close(), IsNil)
	}
	tk.MustQuery("SELECT RANDOM_BYTES('1');")
	tk.MustQuery("SELECT RANDOM_BYTES(1024);")
	result = tk.MustQuery("SELECT RANDOM_BYTES(NULL);")
	result.Check(testkit.Rows("<nil>"))
}

func (s *testIntegrationSuite) TestTimeBuiltin(c *C) {
	originSQLMode := s.ctx.GetSessionVars().StrictSQLMode
	s.ctx.GetSessionVars().StrictSQLMode = true
	defer func() {
		s.ctx.GetSessionVars().StrictSQLMode = originSQLMode
		s.cleanEnv(c)
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// for makeDate
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c datetime, d time, e char(20), f bit(10))")
	tk.MustExec(`insert into t values(1, 1.1, "2017-01-01 12:01:01", "12:01:01", "abcdef", 0b10101)`)
	result := tk.MustQuery("select makedate(a,a), makedate(b,b), makedate(c,c), makedate(d,d), makedate(e,e), makedate(f,f), makedate(null,null), makedate(a,b) from t")
	result.Check(testkit.Rows("2001-01-01 2001-01-01 <nil> <nil> <nil> 2021-01-21 <nil> 2001-01-01"))

	// for date
	result = tk.MustQuery(`select date("2019-09-12"), date("2019-09-12 12:12:09"), date("2019-09-12 12:12:09.121212");`)
	result.Check(testkit.Rows("2019-09-12 2019-09-12 2019-09-12"))
	result = tk.MustQuery(`select date("0000-00-00"), date("0000-00-00 12:12:09"), date("0000-00-00 00:00:00.121212"), date("0000-00-00 00:00:00.000000");`)
	result.Check(testkit.Rows("<nil> 0000-00-00 0000-00-00 <nil>"))
	result = tk.MustQuery(`select date("aa"), date(12.1), date("");`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))

	// for year
	result = tk.MustQuery(`select year("2013-01-09"), year("2013-00-09"), year("000-01-09"), year("1-01-09"), year("20131-01-09"), year(null);`)
	result.Check(testkit.Rows("2013 2013 0 1 <nil> <nil>"))
	result = tk.MustQuery(`select year("2013-00-00"), year("2013-00-00 00:00:00"), year("0000-00-00 12:12:12"), year("2017-00-00 12:12:12");`)
	result.Check(testkit.Rows("2013 2013 0 2017"))
	result = tk.MustQuery(`select year("aa"), year(2013), year(2012.09), year("1-01"), year("-09");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	_, err := tk.Exec(`insert into t select year("aa")`)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue, Commentf("err %v", err))
	tk.MustExec(`set sql_mode='STRICT_TRANS_TABLES'`) // without zero date
	tk.MustExec(`insert into t select year("0000-00-00 00:00:00")`)
	tk.MustExec(`set sql_mode="NO_ZERO_DATE";`) // with zero date
	tk.MustExec(`insert into t select year("0000-00-00 00:00:00")`)
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`set sql_mode="NO_ZERO_DATE,STRICT_TRANS_TABLES";`)
	_, err = tk.Exec(`insert into t select year("0000-00-00 00:00:00");`)
	c.Assert(err, NotNil)
	c.Assert(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue, Commentf("err %v", err))
	tk.MustExec(`insert into t select 1`)
	tk.MustExec(`set sql_mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";`)
	_, err = tk.Exec(`update t set a = year("aa")`)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue, Commentf("err %v", err))
	_, err = tk.Exec(`delete from t where a = year("aa")`)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue, Commentf("err %v", err))

	// for month
	result = tk.MustQuery(`select month("2013-01-09"), month("2013-00-09"), month("000-01-09"), month("1-01-09"), month("20131-01-09"), month(null);`)
	result.Check(testkit.Rows("1 0 1 1 <nil> <nil>"))
	result = tk.MustQuery(`select month("2013-00-00"), month("2013-00-00 00:00:00"), month("0000-00-00 12:12:12"), month("2017-00-00 12:12:12");`)
	result.Check(testkit.Rows("0 0 0 0"))
	result = tk.MustQuery(`select month("aa"), month(2013), month(2012.09), month("1-01"), month("-09");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select month("2013-012-09"), month("2013-0000000012-09"), month("2013-30-09"), month("000-41-09");`)
	result.Check(testkit.Rows("12 12 <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	_, err = tk.Exec(`insert into t select month("aa")`)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue)
	tk.MustExec(`insert into t select month("0000-00-00 00:00:00")`)
	tk.MustExec(`set sql_mode="NO_ZERO_DATE";`)
	tk.MustExec(`insert into t select month("0000-00-00 00:00:00")`)
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`set sql_mode="NO_ZERO_DATE,STRICT_TRANS_TABLES";`)
	_, err = tk.Exec(`insert into t select month("0000-00-00 00:00:00");`)
	c.Assert(err, NotNil)
	c.Assert(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue, Commentf("err %v", err))
	tk.MustExec(`insert into t select 1`)
	tk.MustExec(`set sql_mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";`)
	tk.MustExec(`insert into t select 1`)
	_, err = tk.Exec(`update t set a = month("aa")`)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue)
	_, err = tk.Exec(`delete from t where a = month("aa")`)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue)

	// for week
	result = tk.MustQuery(`select week("2012-12-22"), week("2012-12-22", -2), week("2012-12-22", 0), week("2012-12-22", 1), week("2012-12-22", 2), week("2012-12-22", 200);`)
	result.Check(testkit.Rows("51 51 51 51 51 51"))
	result = tk.MustQuery(`select week("2008-02-20"), week("2008-02-20", 0), week("2008-02-20", 1), week("2009-02-20", 2), week("2008-02-20", 3), week("2008-02-20", 4);`)
	result.Check(testkit.Rows("7 7 8 7 8 8"))
	result = tk.MustQuery(`select week("2008-02-20", 5), week("2008-02-20", 6), week("2009-02-20", 7), week("2008-02-20", 8), week("2008-02-20", 9);`)
	result.Check(testkit.Rows("7 8 7 7 8"))
	result = tk.MustQuery(`select week("aa", 1), week(null, 2), week(11, 2), week(12.99, 2);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select week("aa"), week(null), week(11), week(12.99);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a datetime)`)
	_, err = tk.Exec(`insert into t select week("aa", 1)`)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue)
	tk.MustExec(`insert into t select now()`)
	_, err = tk.Exec(`update t set a = week("aa", 1)`)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue)
	_, err = tk.Exec(`delete from t where a = week("aa", 1)`)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue)

	// for weekofyear
	result = tk.MustQuery(`select weekofyear("2012-12-22"), weekofyear("2008-02-20"), weekofyear("aa"), weekofyear(null), weekofyear(11), weekofyear(12.99);`)
	result.Check(testkit.Rows("51 8 <nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	_, err = tk.Exec(`insert into t select weekofyear("aa")`)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue)
	tk.MustExec(`insert into t select 1`)
	_, err = tk.Exec(`update t set a = weekofyear("aa")`)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue)
	_, err = tk.Exec(`delete from t where a = weekofyear("aa")`)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue)

	// for weekday
	result = tk.MustQuery(`select weekday("2012-12-20"), weekday("2012-12-21"), weekday("2012-12-22"), weekday("2012-12-23"), weekday("2012-12-24"), weekday("2012-12-25"), weekday("2012-12-26"), weekday("2012-12-27");`)
	result.Check(testkit.Rows("3 4 5 6 0 1 2 3"))
	result = tk.MustQuery(`select weekday("2012-12-90"), weekday("0000-00-00"), weekday("aa"), weekday(null), weekday(11), weekday(12.99);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	// for quarter
	result = tk.MustQuery(`select quarter("2012-00-20"), quarter("2012-01-21"), quarter("2012-03-22"), quarter("2012-05-23"), quarter("2012-08-24"), quarter("2012-09-25"), quarter("2012-11-26"), quarter("2012-12-27");`)
	result.Check(testkit.Rows("0 1 1 2 3 3 4 4"))
	result = tk.MustQuery(`select quarter("2012-14-20"), quarter("0000-00-00"), quarter("aa"), quarter(null), quarter(11), quarter(12.99);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	// for from_days
	result = tk.MustQuery(`select from_days(0), from_days(-199), from_days(1111), from_days(120), from_days(1), from_days(1111111), from_days(9999999), from_days(22222);`)
	result.Check(testkit.Rows("0000-00-00 0000-00-00 0003-01-16 0000-00-00 0000-00-00 3042-02-13 0000-00-00 0060-11-03"))
	result = tk.MustQuery(`select from_days("2012-14-20"), from_days("111a"), from_days("aa"), from_days(null), from_days("123asf"), from_days(12.99);`)
	result.Check(testkit.Rows("0005-07-05 0000-00-00 0000-00-00 <nil> 0000-00-00 0000-00-00"))

	// Fix issue #3923
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:00' as time), '12:00:00');")
	result.Check(testkit.Rows("00:00:00"))
	result = tk.MustQuery("select timediff('12:00:00', cast('2004-12-30 12:00:00' as time));")
	result.Check(testkit.Rows("00:00:00"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:00' as time), '2004-12-30 12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('2004-12-30 12:00:00', cast('2004-12-30 12:00:00' as time));")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), '2004-12-30 12:00:00');")
	result.Check(testkit.Rows("00:00:01"))
	result = tk.MustQuery("select timediff('2004-12-30 12:00:00', cast('2004-12-30 12:00:01' as datetime));")
	result.Check(testkit.Rows("-00:00:01"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as time), '-34 00:00:00');")
	result.Check(testkit.Rows("828:00:01"))
	result = tk.MustQuery("select timediff('-34 00:00:00', cast('2004-12-30 12:00:01' as time));")
	result.Check(testkit.Rows("-828:00:01"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), cast('2004-12-30 11:00:01' as datetime));")
	result.Check(testkit.Rows("01:00:00"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), '2004-12-30 12:00:00.1');")
	result.Check(testkit.Rows("00:00:00.9"))
	result = tk.MustQuery("select timediff('2004-12-30 12:00:00.1', cast('2004-12-30 12:00:01' as datetime));")
	result.Check(testkit.Rows("-00:00:00.9"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as datetime), '-34 124:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('-34 124:00:00', cast('2004-12-30 12:00:01' as datetime));")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff(cast('2004-12-30 12:00:01' as time), '-34 124:00:00');")
	result.Check(testkit.Rows("838:59:59"))
	result = tk.MustQuery("select timediff('-34 124:00:00', cast('2004-12-30 12:00:01' as time));")
	result.Check(testkit.Rows("-838:59:59"))
	result = tk.MustQuery("select timediff(cast('2004-12-30' as datetime), '12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('12:00:00', cast('2004-12-30' as datetime));")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('12:00:00', '-34 12:00:00');")
	result.Check(testkit.Rows("838:59:59"))
	result = tk.MustQuery("select timediff('12:00:00', '34 12:00:00');")
	result.Check(testkit.Rows("-816:00:00"))
	result = tk.MustQuery("select timediff('2014-1-2 12:00:00', '-34 12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('-34 12:00:00', '2014-1-2 12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('2014-1-2 12:00:00', '12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('12:00:00', '2014-1-2 12:00:00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timediff('2014-1-2 12:00:00', '2014-1-1 12:00:00');")
	result.Check(testkit.Rows("24:00:00"))

	result = tk.MustQuery("select timestampadd(MINUTE, 1, '2003-01-02'), timestampadd(WEEK, 1, '2003-01-02 23:59:59')" +
		", timestampadd(MICROSECOND, 1, 950501);")
	result.Check(testkit.Rows("2003-01-02 00:01:00 2003-01-09 23:59:59 1995-05-01 00:00:00.000001"))
	result = tk.MustQuery("select timestampadd(day, 2, 950501), timestampadd(MINUTE, 37.5,'2003-01-02'), timestampadd(MINUTE, 37.49,'2003-01-02')," +
		" timestampadd(YeAr, 1, '2003-01-02');")
	result.Check(testkit.Rows("1995-05-03 00:00:00 2003-01-02 00:38:00 2003-01-02 00:37:00 2004-01-02 00:00:00"))
	result = tk.MustQuery("select to_seconds(950501), to_seconds('2009-11-29'), to_seconds('2009-11-29 13:43:32'), to_seconds('09-11-29 13:43:32');")
	result.Check(testkit.Rows("62966505600 63426672000 63426721412 63426721412"))
	result = tk.MustQuery("select to_days(950501), to_days('2007-10-07'), to_days('2007-10-07 00:00:59'), to_days('0000-01-01')")
	result.Check(testkit.Rows("728779 733321 733321 1"))

	result = tk.MustQuery("select last_day('2003-02-05'), last_day('2004-02-05'), last_day('2004-01-01 01:01:01'), last_day(950501);")
	result.Check(testkit.Rows("2003-02-28 2004-02-29 2004-01-31 1995-05-31"))

	tk.MustExec("SET SQL_MODE='';")
	result = tk.MustQuery("select last_day('0000-00-00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select to_days('0000-00-00');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select to_seconds('0000-00-00');")
	result.Check(testkit.Rows("<nil>"))

	result = tk.MustQuery("select timestamp('2003-12-31'), timestamp('2003-12-31 12:00:00','12:00:00');")
	result.Check(testkit.Rows("2003-12-31 00:00:00 2004-01-01 00:00:00"))
	result = tk.MustQuery("select timestamp(20170118123950.123), timestamp(20170118123950.999);")
	result.Check(testkit.Rows("2017-01-18 12:39:50.123 2017-01-18 12:39:50.999"))
	result = tk.MustQuery("select timestamp('2003-12-31', '01:01:01.01'), timestamp('2003-12-31 12:34', '01:01:01.01')," +
		" timestamp('2008-12-31','00:00:00.0'), timestamp('2008-12-31 00:00:00.000');")
	result.Check(testkit.Rows("2003-12-31 01:01:01.01 2003-12-31 13:35:01.01 2008-12-31 00:00:00.0 2008-12-31 00:00:00.000"))
	result = tk.MustQuery("select timestamp('2003-12-31', 1), timestamp('2003-12-31', -1);")
	result.Check(testkit.Rows("2003-12-31 00:00:01 2003-12-30 23:59:59"))
	result = tk.MustQuery("select timestamp('2003-12-31', '2000-12-12 01:01:01.01'), timestamp('2003-14-31','01:01:01.01');")
	result.Check(testkit.Rows("<nil> <nil>"))

	result = tk.MustQuery("select TIMESTAMPDIFF(MONTH,'2003-02-01','2003-05-01'), TIMESTAMPDIFF(yEaR,'2002-05-01', " +
		"'2001-01-01'), TIMESTAMPDIFF(minute,binary('2003-02-01'),'2003-05-01 12:05:55'), TIMESTAMPDIFF(day," +
		"'1995-05-02', 950501);")
	result.Check(testkit.Rows("3 -1 128885 -1"))

	result = tk.MustQuery("select datediff('2007-12-31 23:59:59','2007-12-30'), datediff('2010-11-30 23:59:59', " +
		"'2010-12-31'), datediff(950501,'2016-01-13'), datediff(950501.9,'2016-01-13'), datediff(binary(950501), '2016-01-13');")
	result.Check(testkit.Rows("1 -31 -7562 -7562 -7562"))
	result = tk.MustQuery("select datediff('0000-01-01','0001-01-01'), datediff('0001-00-01', '0001-00-01'), datediff('0001-01-00','0001-01-00'), datediff('2017-01-01','2017-01-01');")
	result.Check(testkit.Rows("-365 <nil> <nil> 0"))

	// for ADDTIME
	result = tk.MustQuery("select addtime('01:01:11', '00:00:01.013'), addtime('01:01:11.00', '00:00:01'), addtime" +
		"('2017-01-01 01:01:11.12', '00:00:01'), addtime('2017-01-01 01:01:11.12', '00:00:01.88');")
	result.Check(testkit.Rows("01:01:12.013000 01:01:12 2017-01-01 01:01:12.120000 2017-01-01 01:01:13"))
	result = tk.MustQuery("select addtime(cast('01:01:11' as time(4)), '00:00:01.013'), addtime(cast('01:01:11.00' " +
		"as datetime(3)), '00:00:01')," + " addtime(cast('2017-01-01 01:01:11.12' as date), '00:00:01'), addtime(cast" +
		"(cast('2017-01-01 01:01:11.12' as date) as datetime(2)), '00:00:01.88');")
	result.Check(testkit.Rows("01:01:12.0130 2001-01-11 00:00:01.000 00:00:01 2017-01-01 00:00:01.88"))
	result = tk.MustQuery("select addtime('2017-01-01 01:01:01', 5), addtime('2017-01-01 01:01:01', -5), addtime('2017-01-01 01:01:01', 0.0), addtime('2017-01-01 01:01:01', 1.34);")
	result.Check(testkit.Rows("2017-01-01 01:01:06 2017-01-01 01:00:56 2017-01-01 01:01:01 2017-01-01 01:01:02.340000"))
	result = tk.MustQuery("select addtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time)), addtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time(5)))")
	result.Check(testkit.Rows("2001-01-11 00:00:01.000 2001-01-11 00:00:01.00000"))
	result = tk.MustQuery("select addtime(cast('01:01:11.00' as date), cast('00:00:01' as time));")
	result.Check(testkit.Rows("00:00:01"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime, b timestamp, c time)")
	tk.MustExec(`insert into t values("2017 01-01 12:30:31", "2017 01-01 12:30:31", "01:01:01")`)
	result = tk.MustQuery("select addtime(a, b), addtime(cast(a as date), b), addtime(b,a), addtime(a,c), addtime(b," +
		"c), addtime(c,a), addtime(c,b)" +
		" from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil> 2017-01-01 13:31:32 2017-01-01 13:31:32 <nil> <nil>"))

	// for SUBTIME
	result = tk.MustQuery("select subtime('01:01:11', '00:00:01.013'), subtime('01:01:11.00', '00:00:01'), subtime" +
		"('2017-01-01 01:01:11.12', '00:00:01'), subtime('2017-01-01 01:01:11.12', '00:00:01.88');")
	result.Check(testkit.Rows("01:01:09.987000 01:01:10 2017-01-01 01:01:10.120000 2017-01-01 01:01:09.240000"))
	result = tk.MustQuery("select subtime(cast('01:01:11' as time(4)), '00:00:01.013'), subtime(cast('01:01:11.00' " +
		"as datetime(3)), '00:00:01')," + " subtime(cast('2017-01-01 01:01:11.12' as date), '00:00:01'), subtime(cast" +
		"(cast('2017-01-01 01:01:11.12' as date) as datetime(2)), '00:00:01.88');")
	result.Check(testkit.Rows("01:01:09.9870 2001-01-10 23:59:59.000 -00:00:01 2016-12-31 23:59:58.12"))
	result = tk.MustQuery("select subtime('2017-01-01 01:01:01', 5), subtime('2017-01-01 01:01:01', -5), subtime('2017-01-01 01:01:01', 0.0), subtime('2017-01-01 01:01:01', 1.34);")
	result.Check(testkit.Rows("2017-01-01 01:00:56 2017-01-01 01:01:06 2017-01-01 01:01:01 2017-01-01 01:00:59.660000"))
	result = tk.MustQuery("select subtime('01:01:11', '0:0:1.013'), subtime('01:01:11.00', '0:0:1'), subtime('2017-01-01 01:01:11.12', '0:0:1'), subtime('2017-01-01 01:01:11.12', '0:0:1.120000');")
	result.Check(testkit.Rows("01:01:09.987000 01:01:10 2017-01-01 01:01:10.120000 2017-01-01 01:01:10"))
	result = tk.MustQuery("select subtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time)), subtime(cast('01:01:11.00' as datetime(3)), cast('00:00:01' as time(5)))")
	result.Check(testkit.Rows("2001-01-10 23:59:59.000 2001-01-10 23:59:59.00000"))
	result = tk.MustQuery("select subtime(cast('01:01:11.00' as date), cast('00:00:01' as time));")
	result.Check(testkit.Rows("-00:00:01"))
	result = tk.MustQuery("select subtime(a, b), subtime(cast(a as date), b), subtime(b,a), subtime(a,c), subtime(b," +
		"c), subtime(c,a), subtime(c,b) from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil> 2017-01-01 11:29:30 2017-01-01 11:29:30 <nil> <nil>"))

	// ADDTIME & SUBTIME issue #5966
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime, b timestamp, c time, d date, e bit(1))")
	tk.MustExec(`insert into t values("2017-01-01 12:30:31", "2017-01-01 12:30:31", "01:01:01", "2017-01-01", 0b1)`)

	result = tk.MustQuery("select addtime(a, e), addtime(b, e), addtime(c, e), addtime(d, e) from t")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("select addtime('2017-01-01 01:01:01', 0b1), addtime('2017-01-01', b'1'), addtime('01:01:01', 0b1011)")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery("select addtime('2017-01-01', 1), addtime('2017-01-01 01:01:01', 1), addtime(cast('2017-01-01' as date), 1)")
	result.Check(testkit.Rows("2017-01-01 00:00:01 2017-01-01 01:01:02 00:00:01"))

	result = tk.MustQuery("select subtime(a, e), subtime(b, e), subtime(c, e), subtime(d, e) from t")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("select subtime('2017-01-01 01:01:01', 0b1), subtime('2017-01-01', b'1'), subtime('01:01:01', 0b1011)")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery("select subtime('2017-01-01', 1), subtime('2017-01-01 01:01:01', 1), subtime(cast('2017-01-01' as date), 1)")
	result.Check(testkit.Rows("2016-12-31 23:59:59 2017-01-01 01:01:00 -00:00:01"))

	// fixed issue #3986
	tk.MustExec("SET SQL_MODE='NO_ENGINE_SUBSTITUTION';")
	tk.MustExec("SET TIME_ZONE='+03:00';")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t (ix TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP);")
	tk.MustExec("INSERT INTO t VALUES (0), (20030101010160), (20030101016001), (20030101240101), (20030132010101), (20031301010101), (20031200000000), (20030000000000);")
	result = tk.MustQuery("SELECT CAST(ix AS SIGNED) FROM t;")
	result.Check(testkit.Rows("0", "0", "0", "0", "0", "0", "0", "0"))

	// test time
	result = tk.MustQuery("select time('2003-12-31 01:02:03')")
	result.Check(testkit.Rows("01:02:03"))
	result = tk.MustQuery("select time('2003-12-31 01:02:03.000123')")
	result.Check(testkit.Rows("01:02:03.000123"))
	result = tk.MustQuery("select time('01:02:03.000123')")
	result.Check(testkit.Rows("01:02:03.000123"))
	result = tk.MustQuery("select time('01:02:03')")
	result.Check(testkit.Rows("01:02:03"))
	result = tk.MustQuery("select time('-838:59:59.000000')")
	result.Check(testkit.Rows("-838:59:59.000000"))
	result = tk.MustQuery("select time('-838:59:59.000001')")
	result.Check(testkit.Rows("-838:59:59.000000"))
	result = tk.MustQuery("select time('-839:59:59.000000')")
	result.Check(testkit.Rows("-838:59:59.000000"))
	result = tk.MustQuery("select time('840:59:59.000000')")
	result.Check(testkit.Rows("838:59:59.000000"))
	// FIXME: #issue 4193
	// result = tk.MustQuery("select time('840:59:60.000000')")
	// result.Check(testkit.Rows("<nil>"))
	// result = tk.MustQuery("select time('800:59:59.9999999')")
	// result.Check(testkit.Rows("801:00:00.000000"))
	// result = tk.MustQuery("select time('12003-12-10 01:02:03.000123')")
	// result.Check(testkit.Rows("<nil>")
	// result = tk.MustQuery("select time('')")
	// result.Check(testkit.Rows("<nil>")
	// result = tk.MustQuery("select time('2003-12-10-10 01:02:03.000123')")
	// result.Check(testkit.Rows("00:20:03")

	//for hour
	result = tk.MustQuery(`SELECT hour("12:13:14.123456"), hour("12:13:14.000010"), hour("272:59:55"), hour(020005), hour(null), hour("27aaaa2:59:55");`)
	result.Check(testkit.Rows("12 12 272 2 <nil> <nil>"))

	// for hour, issue #4340
	result = tk.MustQuery(`SELECT HOUR(20171222020005);`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT HOUR(20171222020005.1);`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT HOUR(20171222020005.1e0);`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT HOUR("20171222020005");`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`SELECT HOUR("20171222020005.1");`)
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery(`select hour(20171222);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select hour(8381222);`)
	result.Check(testkit.Rows("838"))
	result = tk.MustQuery(`select hour(10000000000);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select hour(10100000000);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select hour(10001000000);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select hour(10101000000);`)
	result.Check(testkit.Rows("0"))

	// for minute
	result = tk.MustQuery(`SELECT minute("12:13:14.123456"), minute("12:13:14.000010"), minute("272:59:55"), minute(null), minute("27aaaa2:59:55");`)
	result.Check(testkit.Rows("13 13 59 <nil> <nil>"))

	// for second
	result = tk.MustQuery(`SELECT second("12:13:14.123456"), second("12:13:14.000010"), second("272:59:55"), second(null), second("27aaaa2:59:55");`)
	result.Check(testkit.Rows("14 14 55 <nil> <nil>"))

	// for microsecond
	result = tk.MustQuery(`SELECT microsecond("12:00:00.123456"), microsecond("12:00:00.000010"), microsecond(null), microsecond("27aaaa2:59:55");`)
	result.Check(testkit.Rows("123456 10 <nil> <nil>"))

	// for period_add
	result = tk.MustQuery(`SELECT period_add(191, 2), period_add(191, -2), period_add(0, 20), period_add(0, 0);`)
	result.Check(testkit.Rows("200809 200805 0 0"))
	result = tk.MustQuery(`SELECT period_add(NULL, 2), period_add(-191, NULL), period_add(NULL, NULL), period_add(12.09, -2), period_add("21aa", "11aa"), period_add("", "");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> 200010 200208 0"))

	// for period_diff
	result = tk.MustQuery(`SELECT period_diff(191, 2), period_diff(191, -2), period_diff(0, 0), period_diff(191, 191);`)
	result.Check(testkit.Rows("101 -2213609288845122103 0 0"))
	result = tk.MustQuery(`SELECT period_diff(NULL, 2), period_diff(-191, NULL), period_diff(NULL, NULL), period_diff(12.09, 2), period_diff("21aa", "11aa"), period_diff("", "");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> 10 10 0"))

	// TODO: fix `CAST(xx as duration)` and release the test below:
	// result = tk.MustQuery(`SELECT hour("aaa"), hour(123456), hour(1234567);`)
	// result = tk.MustQuery(`SELECT minute("aaa"), minute(123456), minute(1234567);`)
	// result = tk.MustQuery(`SELECT second("aaa"), second(123456), second(1234567);`)
	// result = tk.MustQuery(`SELECT microsecond("aaa"), microsecond(123456), microsecond(1234567);`)

	// for time_format
	result = tk.MustQuery("SELECT TIME_FORMAT('150:02:28', '%H:%i:%s %p');")
	result.Check(testkit.Rows("150:02:28 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('bad string', '%H:%i:%s %p');")
	result.Check(testkit.Rows("00:00:00 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT(null, '%H:%i:%s %p');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(123, '%H:%i:%s %p');")
	result.Check(testkit.Rows("00:01:23 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('24:00:00', '%r');")
	result.Check(testkit.Rows("12:00:00 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('25:00:00', '%r');")
	result.Check(testkit.Rows("01:00:00 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('24:00:00', '%l %p');")
	result.Check(testkit.Rows("12 AM"))

	// for date_format
	result = tk.MustQuery(`SELECT DATE_FORMAT('2017-06-15', '%W %M %e %Y %r %y');`)
	result.Check(testkit.Rows("Thursday June 15 2017 12:00:00 AM 17"))
	result = tk.MustQuery(`SELECT DATE_FORMAT(151113102019.12, '%W %M %e %Y %r %y');`)
	result.Check(testkit.Rows("Friday November 13 2015 10:20:19 AM 15"))
	result = tk.MustQuery(`SELECT DATE_FORMAT('0000-00-00', '%W %M %e %Y %r %y');`)
	result.Check(testkit.Rows("<nil>"))

	// for yearweek
	result = tk.MustQuery(`select yearweek("2014-12-27"), yearweek("2014-29-27"), yearweek("2014-00-27"), yearweek("2014-12-27 12:38:32"), yearweek("2014-12-27 12:38:32.1111111"), yearweek("2014-12-27 12:90:32"), yearweek("2014-12-27 89:38:32.1111111");`)
	result.Check(testkit.Rows("201451 <nil> <nil> 201451 201451 <nil> <nil>"))
	result = tk.MustQuery(`select yearweek(12121), yearweek(1.00009), yearweek("aaaaa"), yearweek(""), yearweek(NULL);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select yearweek("0000-00-00"), yearweek("2019-01-29", "aa"), yearweek("2011-01-01", null);`)
	result.Check(testkit.Rows("<nil> 201904 201052"))

	// for dayOfWeek, dayOfMonth, dayOfYear
	result = tk.MustQuery(`select dayOfWeek(null), dayOfWeek("2017-08-12"), dayOfWeek("0000-00-00"), dayOfWeek("2017-00-00"), dayOfWeek("0000-00-00 12:12:12"), dayOfWeek("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 7 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfYear(null), dayOfYear("2017-08-12"), dayOfYear("0000-00-00"), dayOfYear("2017-00-00"), dayOfYear("0000-00-00 12:12:12"), dayOfYear("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 224 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfMonth(null), dayOfMonth("2017-08-12"), dayOfMonth("0000-00-00"), dayOfMonth("2017-00-00"), dayOfMonth("0000-00-00 12:12:12"), dayOfMonth("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 12 0 0 0 0"))

	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	result = tk.MustQuery(`select dayOfWeek(null), dayOfWeek("2017-08-12"), dayOfWeek("0000-00-00"), dayOfWeek("2017-00-00"), dayOfWeek("0000-00-00 12:12:12"), dayOfWeek("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 7 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfYear(null), dayOfYear("2017-08-12"), dayOfYear("0000-00-00"), dayOfYear("2017-00-00"), dayOfYear("0000-00-00 12:12:12"), dayOfYear("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 224 <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select dayOfMonth(null), dayOfMonth("2017-08-12"), dayOfMonth("0000-00-00"), dayOfMonth("2017-00-00"), dayOfMonth("0000-00-00 12:12:12"), dayOfMonth("2017-00-00 12:12:12")`)
	result.Check(testkit.Rows("<nil> 12 <nil> 0 0 0"))

	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	tk.MustExec(`insert into t value(1)`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	_, err = tk.Exec("insert into t value(dayOfWeek('0000-00-00'))")
	c.Assert(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue)
	_, err = tk.Exec(`update t set a = dayOfWeek("0000-00-00")`)
	c.Assert(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue)
	_, err = tk.Exec(`delete from t where a = dayOfWeek(123)`)
	c.Assert(err, IsNil)

	_, err = tk.Exec("insert into t value(dayOfMonth('2017-00-00'))")
	c.Assert(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue)
	tk.MustExec("insert into t value(dayOfMonth('0000-00-00'))")
	tk.MustExec(`update t set a = dayOfMonth("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE';")
	tk.MustExec("insert into t value(dayOfMonth('0000-00-00'))")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`update t set a = dayOfMonth("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE,STRICT_TRANS_TABLES';")
	_, err = tk.Exec("insert into t value(dayOfMonth('0000-00-00'))")
	c.Assert(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue)
	tk.MustExec("insert into t value(0)")
	_, err = tk.Exec(`update t set a = dayOfMonth("0000-00-00")`)
	c.Assert(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue)
	_, err = tk.Exec(`delete from t where a = dayOfMonth(123)`)
	c.Assert(err, IsNil)

	_, err = tk.Exec("insert into t value(dayOfYear('0000-00-00'))")
	c.Assert(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue)
	_, err = tk.Exec(`update t set a = dayOfYear("0000-00-00")`)
	c.Assert(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue)
	_, err = tk.Exec(`delete from t where a = dayOfYear(123)`)
	c.Assert(err, IsNil)

	tk.MustExec("set sql_mode = ''")

	// for unix_timestamp
	tk.MustExec("SET time_zone = '+00:00';")
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113);")
	result.Check(testkit.Rows("1447372800"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(20151113);")
	result.Check(testkit.Rows("1447372800"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019);")
	result.Check(testkit.Rows("1447410019"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019e0);")
	result.Check(testkit.Rows("1447410019.000000"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(15111310201912e-2);")
	result.Check(testkit.Rows("1447410019.120000"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019.12);")
	result.Check(testkit.Rows("1447410019.12"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(151113102019.1234567);")
	result.Check(testkit.Rows("1447410019.123457"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(20151113102019);")
	result.Check(testkit.Rows("1447410019"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2015-11-13 10:20:19');")
	result.Check(testkit.Rows("1447410019"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2015-11-13 10:20:19.012');")
	result.Check(testkit.Rows("1447410019.012"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1969-12-31 23:59:59');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-13-01 00:00:00');")
	// FIXME: MySQL returns 0 here.
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 03:14:07.999999');")
	result.Check(testkit.Rows("2147483647.999999"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 03:14:08');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(0);")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(-1);")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP(12345);")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2017-01-01')")
	result.Check(testkit.Rows("1483228800"))
	// Test different time zone.
	tk.MustExec("SET time_zone = '+08:00';")
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 00:00:00');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('1970-01-01 08:00:00');")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2015-11-13 18:20:19.012'), UNIX_TIMESTAMP('2015-11-13 18:20:19.0123');")
	result.Check(testkit.Rows("1447410019.012 1447410019.0123"))
	result = tk.MustQuery("SELECT UNIX_TIMESTAMP('2038-01-19 11:14:07.999999');")
	result.Check(testkit.Rows("2147483647.999999"))

	result = tk.MustQuery("SELECT TIME_FORMAT('bad string', '%H:%i:%s %p');")
	result.Check(testkit.Rows("00:00:00 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT(null, '%H:%i:%s %p');")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("SELECT TIME_FORMAT(123, '%H:%i:%s %p');")
	result.Check(testkit.Rows("00:01:23 AM"))

	// for monthname
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a varchar(10))`)
	tk.MustExec(`insert into t value("abc")`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustExec(`update t set a = monthname("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`update t set a = monthname("0000-00-00")`)
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE'")
	_, err = tk.Exec(`update t set a = monthname("0000-00-00")`)
	c.Assert(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue)
	_, err = tk.Exec(`delete from t where a = monthname(123)`)
	c.Assert(err, IsNil)
	result = tk.MustQuery(`select monthname("2017-12-01"), monthname("0000-00-00"), monthname("0000-01-00"), monthname("0000-01-00 00:00:00")`)
	result.Check(testkit.Rows("December <nil> January January"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))

	// for dayname
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a varchar(10))`)
	tk.MustExec(`insert into t value("abc")`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	_, err = tk.Exec("insert into t value(dayname('0000-00-00'))")
	c.Assert(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue)
	_, err = tk.Exec(`update t set a = dayname("0000-00-00")`)
	c.Assert(types.ErrIncorrectDatetimeValue.Equal(err), IsTrue)
	_, err = tk.Exec(`delete from t where a = dayname(123)`)
	c.Assert(err, IsNil)
	result = tk.MustQuery(`select dayname("2017-12-01"), dayname("0000-00-00"), dayname("0000-01-00"), dayname("0000-01-00 00:00:00")`)
	result.Check(testkit.Rows("Friday <nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'",
		"Warning|1292|Incorrect datetime value: '0000-01-00 00:00:00.000000'",
		"Warning|1292|Incorrect datetime value: '0000-01-00 00:00:00.000000'"))

	// for sec_to_time
	result = tk.MustQuery("select sec_to_time(NULL)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select sec_to_time(2378), sec_to_time(3864000), sec_to_time(-3864000)")
	result.Check(testkit.Rows("00:39:38 838:59:59 -838:59:59"))
	result = tk.MustQuery("select sec_to_time(86401.4), sec_to_time(-86401.4), sec_to_time(864014e-1), sec_to_time(-864014e-1), sec_to_time('86401.4'), sec_to_time('-86401.4')")
	result.Check(testkit.Rows("24:00:01.4 -24:00:01.4 24:00:01.400000 -24:00:01.400000 24:00:01.400000 -24:00:01.400000"))
	result = tk.MustQuery("select sec_to_time(86401.54321), sec_to_time(86401.543212345)")
	result.Check(testkit.Rows("24:00:01.54321 24:00:01.543212"))
	result = tk.MustQuery("select sec_to_time('123.4'), sec_to_time('123.4567891'), sec_to_time('123')")
	result.Check(testkit.Rows("00:02:03.400000 00:02:03.456789 00:02:03.000000"))

	// for time_to_sec
	result = tk.MustQuery("select time_to_sec(NULL)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select time_to_sec('22:23:00'), time_to_sec('00:39:38'), time_to_sec('23:00'), time_to_sec('00:00'), time_to_sec('00:00:00'), time_to_sec('23:59:59')")
	result.Check(testkit.Rows("80580 2378 82800 0 0 86399"))
	result = tk.MustQuery("select time_to_sec('1:0'), time_to_sec('1:00'), time_to_sec('1:0:0'), time_to_sec('-02:00'), time_to_sec('-02:00:05'), time_to_sec('020005')")
	result.Check(testkit.Rows("3600 3600 3600 -7200 -7205 7205"))
	result = tk.MustQuery("select time_to_sec('20171222020005'), time_to_sec(020005), time_to_sec(20171222020005), time_to_sec(171222020005)")
	result.Check(testkit.Rows("7205 7205 7205 7205"))

	// for str_to_date
	result = tk.MustQuery("select str_to_date('01-01-2017', '%d-%m-%Y'), str_to_date('59:20:12 01-01-2017', '%s:%i:%H %d-%m-%Y'), str_to_date('59:20:12', '%s:%i:%H')")
	result.Check(testkit.Rows("2017-01-01 2017-01-01 12:20:59 12:20:59"))
	result = tk.MustQuery("select str_to_date('aaa01-01-2017', 'aaa%d-%m-%Y'), str_to_date('59:20:12 aaa01-01-2017', '%s:%i:%H aaa%d-%m-%Y'), str_to_date('59:20:12aaa', '%s:%i:%Haaa')")
	result.Check(testkit.Rows("2017-01-01 2017-01-01 12:20:59 12:20:59"))
	result = tk.MustQuery("select str_to_date('01-01-2017', '%d'), str_to_date('59', '%d-%Y')")
	// TODO: MySQL returns "<nil> <nil>".
	result.Check(testkit.Rows("0000-00-01 <nil>"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00'"))
	result = tk.MustQuery("select str_to_date('2018-6-1', '%Y-%m-%d'), str_to_date('2018-6-1', '%Y-%c-%d'), str_to_date('59:20:1', '%s:%i:%k'), str_to_date('59:20:1', '%s:%i:%l')")
	result.Check(testkit.Rows("2018-06-01 2018-06-01 01:20:59 01:20:59"))

	// for maketime
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a double, b float, c decimal(10,4));`)
	tk.MustExec(`insert into t value(1.23, 2.34, 3.1415)`)
	result = tk.MustQuery("select maketime(1,1,a), maketime(2,2,b), maketime(3,3,c) from t;")
	result.Check(testkit.Rows("01:01:01.230000 02:02:02.340000 03:03:03.1415"))
	result = tk.MustQuery("select maketime(12, 13, 14), maketime('12', '15', 30.1), maketime(0, 1, 59.1), maketime(0, 1, '59.1'), maketime(0, 1, 59.5)")
	result.Check(testkit.Rows("12:13:14 12:15:30.1 00:01:59.1 00:01:59.100000 00:01:59.5"))
	result = tk.MustQuery("select maketime(12, 15, 60), maketime(12, 15, '60'), maketime(12, 60, 0), maketime(12, 15, null)")
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	result = tk.MustQuery("select maketime('', '', ''), maketime('h', 'm', 's');")
	result.Check(testkit.Rows("00:00:00.000000 00:00:00.000000"))

	// for get_format
	result = tk.MustQuery(`select GET_FORMAT(DATE,'USA'), GET_FORMAT(DATE,'JIS'), GET_FORMAT(DATE,'ISO'), GET_FORMAT(DATE,'EUR'),
	GET_FORMAT(DATE,'INTERNAL'), GET_FORMAT(DATETIME,'USA') , GET_FORMAT(DATETIME,'JIS'), GET_FORMAT(DATETIME,'ISO'),
	GET_FORMAT(DATETIME,'EUR') , GET_FORMAT(DATETIME,'INTERNAL'), GET_FORMAT(TIME,'USA') , GET_FORMAT(TIME,'JIS'),
	GET_FORMAT(TIME,'ISO'), GET_FORMAT(TIME,'EUR'), GET_FORMAT(TIME,'INTERNAL')`)
	result.Check(testkit.Rows("%m.%d.%Y %Y-%m-%d %Y-%m-%d %d.%m.%Y %Y%m%d %Y-%m-%d %H.%i.%s %Y-%m-%d %H:%i:%s %Y-%m-%d %H:%i:%s %Y-%m-%d %H.%i.%s %Y%m%d%H%i%s %h:%i:%s %p %H:%i:%s %H:%i:%s %H.%i.%s %H%i%s"))

	// for convert_tz
	result = tk.MustQuery(`select convert_tz("2004-01-01 12:00:00", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00.01", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00.01234567", "+00:00", "+10:32");`)
	result.Check(testkit.Rows("2004-01-01 22:32:00 2004-01-01 22:32:00.01 2004-01-01 22:32:00.012346"))
	// TODO: release the following test after fix #4462
	//result = tk.MustQuery(`select convert_tz(20040101, "+00:00", "+10:32"), convert_tz(20040101.01, "+00:00", "+10:32"), convert_tz(20040101.01234567, "+00:00", "+10:32");`)
	//result.Check(testkit.Rows("2004-01-01 10:32:00 2004-01-01 10:32:00.00 2004-01-01 10:32:00.000000"))

	// for from_unixtime
	tk.MustExec(`set @@session.time_zone = "+08:00"`)
	result = tk.MustQuery(`select from_unixtime(20170101), from_unixtime(20170101.9999999), from_unixtime(20170101.999), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x"), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x")`)
	result.Check(testkit.Rows("1970-08-22 18:48:21 1970-08-22 18:48:22.000000 1970-08-22 18:48:21.999 1970 22nd August 06:48:21 1970 1970 22nd August 06:48:21 1970"))
	tk.MustExec(`set @@session.time_zone = "+00:00"`)
	result = tk.MustQuery(`select from_unixtime(20170101), from_unixtime(20170101.9999999), from_unixtime(20170101.999), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x"), from_unixtime(20170101.999, "%Y %D %M %h:%i:%s %x")`)
	result.Check(testkit.Rows("1970-08-22 10:48:21 1970-08-22 10:48:22.000000 1970-08-22 10:48:21.999 1970 22nd August 10:48:21 1970 1970 22nd August 10:48:21 1970"))
	tk.MustExec(`set @@session.time_zone = @@global.time_zone`)

	// for extract
	result = tk.MustQuery(`select extract(day from '800:12:12'), extract(hour from '800:12:12'), extract(month from 20170101), extract(day_second from '2017-01-01 12:12:12')`)
	result.Check(testkit.Rows("12 800 1 1121212"))

	// for adddate, subdate
	dateArithmeticalTests := []struct {
		Date      string
		Interval  string
		Unit      string
		AddResult string
		SubResult string
	}{
		{"\"2011-11-11\"", "1", "DAY", "2011-11-12", "2011-11-10"},
		{"NULL", "1", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11\"", "NULL", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11 10:10:10\"", "1000", "MICROSECOND", "2011-11-11 10:10:10.001000", "2011-11-11 10:10:09.999000"},
		{"\"2011-11-11 10:10:10\"", "\"10\"", "SECOND", "2011-11-11 10:10:20", "2011-11-11 10:10:00"},
		{"\"2011-11-11 10:10:10\"", "\"10\"", "MINUTE", "2011-11-11 10:20:10", "2011-11-11 10:00:10"},
		{"\"2011-11-11 10:10:10\"", "\"10\"", "HOUR", "2011-11-11 20:10:10", "2011-11-11 00:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"11\"", "DAY", "2011-11-22 10:10:10", "2011-10-31 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"2\"", "WEEK", "2011-11-25 10:10:10", "2011-10-28 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"2\"", "MONTH", "2012-01-11 10:10:10", "2011-09-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"4\"", "QUARTER", "2012-11-11 10:10:10", "2010-11-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"2\"", "YEAR", "2013-11-11 10:10:10", "2009-11-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"10.00100000\"", "SECOND_MICROSECOND", "2011-11-11 10:10:20.100000", "2011-11-11 10:09:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"10.0010000000\"", "SECOND_MICROSECOND", "2011-11-11 10:10:30", "2011-11-11 10:09:50"},
		{"\"2011-11-11 10:10:10\"", "\"10.0010000010\"", "SECOND_MICROSECOND", "2011-11-11 10:10:30.000010", "2011-11-11 10:09:49.999990"},
		{"\"2011-11-11 10:10:10\"", "\"10:10.100\"", "MINUTE_MICROSECOND", "2011-11-11 10:20:20.100000", "2011-11-11 09:59:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"10:10\"", "MINUTE_SECOND", "2011-11-11 10:20:20", "2011-11-11 10:00:00"},
		{"\"2011-11-11 10:10:10\"", "\"10:10:10.100\"", "HOUR_MICROSECOND", "2011-11-11 20:20:20.100000", "2011-11-10 23:59:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"10:10:10\"", "HOUR_SECOND", "2011-11-11 20:20:20", "2011-11-11 00:00:00"},
		{"\"2011-11-11 10:10:10\"", "\"10:10\"", "HOUR_MINUTE", "2011-11-11 20:20:10", "2011-11-11 00:00:10"},
		{"\"2011-11-11 10:10:10\"", "\"11 10:10:10.100\"", "DAY_MICROSECOND", "2011-11-22 20:20:20.100000", "2011-10-30 23:59:59.900000"},
		{"\"2011-11-11 10:10:10\"", "\"11 10:10:10\"", "DAY_SECOND", "2011-11-22 20:20:20", "2011-10-31 00:00:00"},
		{"\"2011-11-11 10:10:10\"", "\"11 10:10\"", "DAY_MINUTE", "2011-11-22 20:20:10", "2011-10-31 00:00:10"},
		{"\"2011-11-11 10:10:10\"", "\"11 10\"", "DAY_HOUR", "2011-11-22 20:10:10", "2011-10-31 00:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"11-1\"", "YEAR_MONTH", "2022-12-11 10:10:10", "2000-10-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"11-11\"", "YEAR_MONTH", "2023-10-11 10:10:10", "1999-12-11 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"20\"", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "19.88", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"19.88\"", "DAY", "2011-11-30 10:10:10", "2011-10-23 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"prefix19suffix\"", "DAY", "2011-11-30 10:10:10", "2011-10-23 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"20-11\"", "DAY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"20,11\"", "daY", "2011-12-01 10:10:10", "2011-10-22 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"1000\"", "dAy", "2014-08-07 10:10:10", "2009-02-14 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "\"true\"", "Day", "2011-11-12 10:10:10", "2011-11-10 10:10:10"},
		{"\"2011-11-11 10:10:10\"", "true", "Day", "2011-11-12 10:10:10", "2011-11-10 10:10:10"},
		{"\"2011-11-11\"", "1", "DAY", "2011-11-12", "2011-11-10"},
		{"\"2011-11-11\"", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"\"2011-11-11\"", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"\"2011-11-11\"", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},
		{"\"2011-11-11\"", "\"10:10\"", "HOUR_MINUTE", "2011-11-11 10:10:00", "2011-11-10 13:50:00"},
		{"\"2011-11-11\"", "\"10:10:10\"", "HOUR_SECOND", "2011-11-11 10:10:10", "2011-11-10 13:49:50"},
		{"\"2011-11-11\"", "\"10:10:10.101010\"", "HOUR_MICROSECOND", "2011-11-11 10:10:10.101010", "2011-11-10 13:49:49.898990"},
		{"\"2011-11-11\"", "\"10:10\"", "MINUTE_SECOND", "2011-11-11 00:10:10", "2011-11-10 23:49:50"},
		{"\"2011-11-11\"", "\"10:10.101010\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.101010", "2011-11-10 23:49:49.898990"},
		{"\"2011-11-11\"", "\"10.101010\"", "SECOND_MICROSECOND", "2011-11-11 00:00:10.101010", "2011-11-10 23:59:49.898990"},
		{"\"2011-11-11 00:00:00\"", "1", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"\"2011-11-11 00:00:00\"", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"\"2011-11-11 00:00:00\"", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"\"2011-11-11 00:00:00\"", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"\"2011-11-11\"", "\"abc1000\"", "MICROSECOND", "<nil>", "<nil>"},
		{"\"20111111 10:10:10\"", "\"1\"", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11\"", "\"10\"", "SECOND_MICROSECOND", "<nil>", "<nil>"},
		{"\"2011-11-11\"", "\"10.0000\"", "MINUTE_MICROSECOND", "<nil>", "<nil>"},
		{"\"2011-11-11\"", "\"10:10:10\"", "MINUTE_MICROSECOND", "<nil>", "<nil>"},

		{"cast(\"2011-11-11\" as datetime)", "\"10:10:10\"", "MINUTE_MICROSECOND", "<nil>", "<nil>"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "1", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"1\"", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"cast(\"2011-11-11\" as date)", "\"10:10:10\"", "MINUTE_MICROSECOND", "<nil>", "<nil>"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "1", "DAY", "2011-11-12", "2011-11-10"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"cast(\"2011-11-11 00:00:00\" as date)", "\"1\"", "DAY", "2011-11-12", "2011-11-10"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "\"10\"", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "\"10\"", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as date)", "\"10\"", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		// interval decimal support
		{"\"2011-01-01 00:00:00\"", "10.10", "YEAR_MONTH", "2021-11-01 00:00:00", "2000-03-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_HOUR", "2011-01-11 10:00:00", "2010-12-21 14:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR_MINUTE", "2011-01-01 10:10:00", "2010-12-31 13:50:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_MINUTE", "2011-01-01 10:10:00", "2010-12-31 13:50:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_SECOND", "2011-01-01 00:10:10", "2010-12-31 23:49:50"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR_SECOND", "2011-01-01 00:10:10", "2010-12-31 23:49:50"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MINUTE_SECOND", "2011-01-01 00:10:10", "2010-12-31 23:49:50"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MINUTE_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "SECOND_MICROSECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "YEAR", "2021-01-01 00:00:00", "2001-01-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "QUARTER", "2013-07-01 00:00:00", "2008-07-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MONTH", "2011-11-01 00:00:00", "2010-03-01 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "WEEK", "2011-03-12 00:00:00", "2010-10-23 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "DAY", "2011-01-11 00:00:00", "2010-12-22 00:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "HOUR", "2011-01-01 10:00:00", "2010-12-31 14:00:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MINUTE", "2011-01-01 00:10:00", "2010-12-31 23:50:00"},
		{"\"2011-01-01 00:00:00\"", "10.10", "SECOND", "2011-01-01 00:00:10.100000", "2010-12-31 23:59:49.900000"},
		{"\"2011-01-01 00:00:00\"", "10.10", "MICROSECOND", "2011-01-01 00:00:00.000010", "2010-12-31 23:59:59.999990"},
		{"\"2011-01-01 00:00:00\"", "10.90", "MICROSECOND", "2011-01-01 00:00:00.000011", "2010-12-31 23:59:59.999989"},

		{"\"2009-01-01\"", "6/4", "HOUR_MINUTE", "2009-01-04 12:20:00", "2008-12-28 11:40:00"},
		{"\"2009-01-01\"", "6/0", "HOUR_MINUTE", "<nil>", "<nil>"},
		{"\"1970-01-01 12:00:00\"", "CAST(6/4 AS DECIMAL(3,1))", "HOUR_MINUTE", "1970-01-01 13:05:00", "1970-01-01 10:55:00"},
		//for issue #8077
		{"\"2012-01-02\"", "\"prefix8\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
		{"\"2012-01-02\"", "\"prefix8prefix\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
		{"\"2012-01-02\"", "\"8:00\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
		{"\"2012-01-02\"", "\"8:00:00\"", "HOUR", "2012-01-02 08:00:00", "2012-01-01 16:00:00"},
	}
	for _, tc := range dateArithmeticalTests {
		addDate := fmt.Sprintf("select adddate(%s, interval %s %s);", tc.Date, tc.Interval, tc.Unit)
		subDate := fmt.Sprintf("select subdate(%s, interval %s %s);", tc.Date, tc.Interval, tc.Unit)
		result = tk.MustQuery(addDate)
		result.Check(testkit.Rows(tc.AddResult))
		result = tk.MustQuery(subDate)
		result.Check(testkit.Rows(tc.SubResult))
	}

	// for localtime, localtimestamp
	result = tk.MustQuery(`select localtime() = now(), localtime = now(), localtimestamp() = now(), localtimestamp = now()`)
	result.Check(testkit.Rows("1 1 1 1"))

	// for current_timestamp, current_timestamp()
	result = tk.MustQuery(`select current_timestamp() = now(), current_timestamp = now()`)
	result.Check(testkit.Rows("1 1"))

	// for tidb_parse_tso
	tk.MustExec("SET time_zone = '+00:00';")
	result = tk.MustQuery(`select tidb_parse_tso(404411537129996288)`)
	result.Check(testkit.Rows("2018-11-20 09:53:04.877000"))
	result = tk.MustQuery(`select tidb_parse_tso("404411537129996288")`)
	result.Check(testkit.Rows("2018-11-20 09:53:04.877000"))
	result = tk.MustQuery(`select tidb_parse_tso(1)`)
	result.Check(testkit.Rows("1970-01-01 00:00:00.000000"))
	result = tk.MustQuery(`select tidb_parse_tso(0)`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select tidb_parse_tso(-1)`)
	result.Check(testkit.Rows("<nil>"))
}

func (s *testIntegrationSuite) TestOpBuiltin(c *C) {
	defer s.cleanEnv(c)
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
	// for unaryPlus
	result = tk.MustQuery(`select +1, +0, +(-9), +(-0.001), +0.999, +null, +"aaa"`)
	result.Check(testkit.Rows("1 0 -9 -0.001 0.999 <nil> aaa"))
}

func (s *testIntegrationSuite) TestBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	ctx := context.Background()

	// for is true && is false
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
	result = tk.MustQuery(`select 1 is true, 0 is true, null is true, "aaa" is true, "" is true, -12.00 is true, 0.0 is true, 0.0000001 is true;`)
	result.Check(testkit.Rows("1 0 0 0 0 1 0 1"))
	result = tk.MustQuery(`select 1 is false, 0 is false, null is false, "aaa" is false, "" is false, -12.00 is false, 0.0 is false, 0.0000001 is false;`)
	result.Check(testkit.Rows("0 1 0 1 1 0 1 0"))

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
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a decimal(3, 1), b double, c datetime, d time, e int)")
	tk.MustExec("insert into t value(12.3, 1.23, '2017-01-01 12:12:12', '12:12:12', 123)")
	result = tk.MustQuery("select cast(a as json), cast(b as json), cast(c as json), cast(d as json), cast(e as json) from t")
	result.Check(testkit.Rows(`12.3 1.23 "2017-01-01 12:12:12.000000" "12:12:12.000000" 123`))
	result = tk.MustQuery(`select cast(10101000000 as time);`)
	result.Check(testkit.Rows("00:00:00"))
	result = tk.MustQuery(`select cast(10101001000 as time);`)
	result.Check(testkit.Rows("00:10:00"))
	result = tk.MustQuery(`select cast(10000000000 as time);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select cast(20171222020005 as time);`)
	result.Check(testkit.Rows("02:00:05"))
	result = tk.MustQuery(`select cast(8380000 as time);`)
	result.Check(testkit.Rows("838:00:00"))
	result = tk.MustQuery(`select cast(8390000 as time);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select cast(8386000 as time);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select cast(8385960 as time);`)
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery(`select cast(cast('2017-01-01 01:01:11.12' as date) as datetime(2));`)
	result.Check(testkit.Rows("2017-01-01 00:00:00.00"))

	result = tk.MustQuery(`select cast(20170118.999 as datetime);`)
	result.Check(testkit.Rows("2017-01-18 00:00:00"))

	// Test corner cases of cast string as datetime
	result = tk.MustQuery(`select cast("170102034" as datetime);`)
	result.Check(testkit.Rows("2017-01-02 03:04:00"))
	result = tk.MustQuery(`select cast("1701020304" as datetime);`)
	result.Check(testkit.Rows("2017-01-02 03:04:00"))
	result = tk.MustQuery(`select cast("1701020304." as datetime);`)
	result.Check(testkit.Rows("2017-01-02 03:04:00"))
	result = tk.MustQuery(`select cast("1701020304.1" as datetime);`)
	result.Check(testkit.Rows("2017-01-02 03:04:01"))
	result = tk.MustQuery(`select cast("1701020304.111" as datetime);`)
	result.Check(testkit.Rows("2017-01-02 03:04:11"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '1701020304.111'"))
	result = tk.MustQuery(`select cast("17011" as datetime);`)
	result.Check(testkit.Rows("2017-01-01 00:00:00"))
	result = tk.MustQuery(`select cast("150101." as datetime);`)
	result.Check(testkit.Rows("2015-01-01 00:00:00"))
	result = tk.MustQuery(`select cast("150101.a" as datetime);`)
	result.Check(testkit.Rows("2015-01-01 00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '150101.a'"))
	result = tk.MustQuery(`select cast("150101.1a" as datetime);`)
	result.Check(testkit.Rows("2015-01-01 01:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '150101.1a'"))
	result = tk.MustQuery(`select cast("150101.1a1" as datetime);`)
	result.Check(testkit.Rows("2015-01-01 01:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '150101.1a1'"))
	result = tk.MustQuery(`select cast("1101010101.111" as datetime);`)
	result.Check(testkit.Rows("2011-01-01 01:01:11"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '1101010101.111'"))
	result = tk.MustQuery(`select cast("1101010101.11aaaaa" as datetime);`)
	result.Check(testkit.Rows("2011-01-01 01:01:11"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '1101010101.11aaaaa'"))
	result = tk.MustQuery(`select cast("1101010101.a1aaaaa" as datetime);`)
	result.Check(testkit.Rows("2011-01-01 01:01:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '1101010101.a1aaaaa'"))
	result = tk.MustQuery(`select cast("1101010101.11" as datetime);`)
	result.Check(testkit.Rows("2011-01-01 01:01:11"))
	tk.MustQuery("select @@warning_count;").Check(testkit.Rows("0"))
	result = tk.MustQuery(`select cast("1101010101.111" as datetime);`)
	result.Check(testkit.Rows("2011-01-01 01:01:11"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '1101010101.111'"))
	result = tk.MustQuery(`select cast("970101.111" as datetime);`)
	result.Check(testkit.Rows("1997-01-01 11:01:00"))
	tk.MustQuery("select @@warning_count;").Check(testkit.Rows("0"))
	result = tk.MustQuery(`select cast("970101.11111" as datetime);`)
	result.Check(testkit.Rows("1997-01-01 11:11:01"))
	tk.MustQuery("select @@warning_count;").Check(testkit.Rows("0"))
	result = tk.MustQuery(`select cast("970101.111a1" as datetime);`)
	result.Check(testkit.Rows("1997-01-01 11:01:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '970101.111a1'"))

	// for ISNULL
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int, d char(10), e datetime, f float, g decimal(10, 3))")
	tk.MustExec("insert t values (1, 0, null, null, null, null, null)")
	result = tk.MustQuery("select ISNULL(a), ISNULL(b), ISNULL(c), ISNULL(d), ISNULL(e), ISNULL(f), ISNULL(g) from t")
	result.Check(testkit.Rows("0 0 1 1 1 1 1"))

	// fix issue #3942
	result = tk.MustQuery("select cast('-24 100:00:00' as time);")
	result.Check(testkit.Rows("-676:00:00"))
	result = tk.MustQuery("select cast('12:00:00.000000' as datetime);")
	result.Check(testkit.Rows("2012-00-00 00:00:00"))
	result = tk.MustQuery("select cast('-34 100:00:00' as time);")
	result.Check(testkit.Rows("-838:59:59"))

	// fix issue #4324. cast decimal/int/string to time compatibility.
	invalidTimes := []string{
		"10009010",
		"239010",
		"233070",
		"23:90:10",
		"23:30:70",
		"239010.2",
		"233070.8",
	}
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t (ix TIME);")
	tk.MustExec("SET SQL_MODE='';")
	for _, invalidTime := range invalidTimes {
		msg := fmt.Sprintf("Warning 1292 Truncated incorrect time value: '%s'", invalidTime)
		result = tk.MustQuery(fmt.Sprintf("select cast('%s' as time);", invalidTime))
		result.Check(testkit.Rows("<nil>"))
		result = tk.MustQuery("show warnings")
		result.Check(testkit.Rows(msg))
		_, err := tk.Exec(fmt.Sprintf("insert into t select cast('%s' as time);", invalidTime))
		c.Assert(err, IsNil)
		result = tk.MustQuery("show warnings")
		result.Check(testkit.Rows(msg))
	}
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")
	for _, invalidTime := range invalidTimes {
		msg := fmt.Sprintf("Warning 1292 Truncated incorrect time value: '%s'", invalidTime)
		result = tk.MustQuery(fmt.Sprintf("select cast('%s' as time);", invalidTime))
		result.Check(testkit.Rows("<nil>"))
		result = tk.MustQuery("show warnings")
		result.Check(testkit.Rows(msg))
		_, err := tk.Exec(fmt.Sprintf("insert into t select cast('%s' as time);", invalidTime))
		c.Assert(err.Error(), Equals, fmt.Sprintf("[types:1292]Truncated incorrect time value: '%s'", invalidTime))
	}

	// Fix issue #3691, cast compatibility.
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

	// test case decimal precision less than the scale.
	rs, err := tk.Exec("select cast(12.1 as decimal(3, 4));")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1427]For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '').")
	c.Assert(rs.Close(), IsNil)

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
	result = tk.MustQuery("select from_unixtime(1511247196661)")
	result.Check(testkit.Rows("<nil>"))

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
	tk.MustExec("insert t values (null, 4)")
	result = tk.MustQuery("select * from t where b < case a when null then 0 when 'str2' then 0 else 9 end")
	result.Check(testkit.Rows("<nil> 4"))
	result = tk.MustQuery("select * from t where b = case when a is null then 4 when  a = 'str5' then 7 else 9 end")
	result.Check(testkit.Rows("<nil> 4"))

	// for cast
	result = tk.MustQuery("select cast(1234 as char(3))")
	result.Check(testkit.Rows("123"))
	result = tk.MustQuery("select cast(1234 as char(0))")
	result.Check(testkit.Rows(""))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows("Warning 1406 Data Too Long, field len 0, data len 4"))
	result = tk.MustQuery("select CAST( - 8 AS DECIMAL ) * + 52 + 87 < - 86")
	result.Check(testkit.Rows("1"))

	// for char
	result = tk.MustQuery("select char(97, 100, 256, 89)")
	result.Check(testkit.Rows("ad\x01\x00Y"))
	result = tk.MustQuery("select char(97, null, 100, 256, 89)")
	result.Check(testkit.Rows("ad\x01\x00Y"))
	result = tk.MustQuery("select char(97, null, 100, 256, 89 using utf8)")
	result.Check(testkit.Rows("ad\x01\x00Y"))
	result = tk.MustQuery("select char(97, null, 100, 256, 89 using ascii)")
	result.Check(testkit.Rows("ad\x01\x00Y"))
	charRecordSet, err := tk.Exec("select char(97, null, 100, 256, 89 using tidb)")
	c.Assert(err, IsNil)
	c.Assert(charRecordSet, NotNil)
	_, err = session.GetRows4Test(ctx, tk.Se, charRecordSet)
	c.Assert(err.Error(), Equals, "unknown encoding: tidb")

	// issue 3884
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (c1 date, c2 datetime, c3 timestamp, c4 time, c5 year);")
	tk.MustExec("INSERT INTO t values ('2000-01-01', '2000-01-01 12:12:12', '2000-01-01 12:12:12', '12:12:12', '2000');")
	tk.MustExec("INSERT INTO t values ('2000-02-01', '2000-02-01 12:12:12', '2000-02-01 12:12:12', '13:12:12', 2000);")
	tk.MustExec("INSERT INTO t values ('2000-03-01', '2000-03-01', '2000-03-01 12:12:12', '1 12:12:12', 2000);")
	tk.MustExec("INSERT INTO t SET c1 = '2000-04-01', c2 = '2000-04-01', c3 = '2000-04-01 12:12:12', c4 = '-1 13:12:12', c5 = 2000;")
	result = tk.MustQuery("SELECT c4 FROM t where c4 < '-13:12:12';")
	result.Check(testkit.Rows("-37:12:12"))
	result = tk.MustQuery(`SELECT 1 DIV - - 28 + ( - SUM( - + 25 ) ) * - CASE - 18 WHEN 44 THEN NULL ELSE - 41 + 32 + + - 70 - + COUNT( - 95 ) * 15 END + 92`)
	result.Check(testkit.Rows("2442"))

	// for regexp, rlike
	// https://github.com/pingcap/tidb/issues/4080
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (a char(10), b varchar(10), c binary(10), d varbinary(10));`)
	tk.MustExec(`insert into t values ('text','text','text','text');`)
	result = tk.MustQuery(`select a regexp 'Xt' from t;`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select b regexp 'Xt' from t;`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select c regexp 'Xt' from t;`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select d regexp 'Xt' from t;`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select a rlike 'Xt' from t;`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select b rlike 'Xt' from t;`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select c rlike 'Xt' from t;`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select d rlike 'Xt' from t;`)
	result.Check(testkit.Rows("0"))

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
		{"aA", "Aa", 0},
		{`aA%`, "aAab", 1},
		{"aA_", "Aaab", 0},
		{"Aa_", "Aab", 1},
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

	// for #9838
	result = tk.MustQuery("select cast(1 as signed) + cast(9223372036854775807 as unsigned);")
	result.Check(testkit.Rows("9223372036854775808"))
	result = tk.MustQuery("select cast(9223372036854775807 as unsigned) + cast(1 as signed);")
	result.Check(testkit.Rows("9223372036854775808"))
	err = tk.QueryToErr("select cast(9223372036854775807 as signed) + cast(9223372036854775809 as unsigned);")
	c.Assert(err, NotNil)
	err = tk.QueryToErr("select cast(9223372036854775809 as unsigned) + cast(9223372036854775807 as signed);")
	c.Assert(err, NotNil)
	err = tk.QueryToErr("select cast(-9223372036854775807 as signed) + cast(9223372036854775806 as unsigned);")
	c.Assert(err, NotNil)
	err = tk.QueryToErr("select cast(9223372036854775806 as unsigned) + cast(-9223372036854775807 as signed);")
	c.Assert(err, NotNil)
}

func (s *testIntegrationSuite) TestInfoBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// for last_insert_id
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int auto_increment, a int, PRIMARY KEY (id))")
	tk.MustExec("insert into t(a) values(1)")
	result := tk.MustQuery("select last_insert_id();")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t values(2, 1)")
	result = tk.MustQuery("select last_insert_id();")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t(a) values(1)")
	result = tk.MustQuery("select last_insert_id();")
	result.Check(testkit.Rows("3"))

	result = tk.MustQuery("select last_insert_id(5);")
	result.Check(testkit.Rows("5"))
	result = tk.MustQuery("select last_insert_id();")
	result.Check(testkit.Rows("5"))

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

	// for database
	result = tk.MustQuery("select database()")
	result.Check(testkit.Rows("test"))
	tk.MustExec("drop database test")
	result = tk.MustQuery("select database()")
	result.Check(testkit.Rows("<nil>"))
	tk.MustExec("create database test")
	tk.MustExec("use test")

	// for current_user
	sessionVars := tk.Se.GetSessionVars()
	originUser := sessionVars.User
	sessionVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost", AuthUsername: "root", AuthHostname: "127.0.%%"}
	result = tk.MustQuery("select current_user()")
	result.Check(testkit.Rows("root@127.0.%%"))
	sessionVars.User = originUser

	// for user
	sessionVars.User = &auth.UserIdentity{Username: "root", Hostname: "localhost", AuthUsername: "root", AuthHostname: "127.0.%%"}
	result = tk.MustQuery("select user()")
	result.Check(testkit.Rows("root@localhost"))
	sessionVars.User = originUser

	// for connection_id
	originConnectionID := sessionVars.ConnectionID
	sessionVars.ConnectionID = uint64(1)
	result = tk.MustQuery("select connection_id()")
	result.Check(testkit.Rows("1"))
	sessionVars.ConnectionID = originConnectionID

	// for version
	result = tk.MustQuery("select version()")
	result.Check(testkit.Rows(mysql.ServerVersion))

	// for row_count
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, PRIMARY KEY (a))")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("0"))
	tk.MustExec("insert into t(a, b) values(1, 11), (2, 22), (3, 33)")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("3"))
	tk.MustExec("select * from t")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("-1"))
	tk.MustExec("update t set b=22 where a=1")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("1"))
	tk.MustExec("update t set b=22 where a=1")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("0"))
	tk.MustExec("delete from t where a=2")
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select row_count();")
	result.Check(testkit.Rows("-1"))

	// for benchmark
	success := testkit.Rows("0")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	result = tk.MustQuery(`select benchmark(3, benchmark(2, length("abc")))`)
	result.Check(success)
	err := tk.ExecToErr(`select benchmark(3, length("a", "b"))`)
	c.Assert(err, NotNil)
	// Quoted from https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_benchmark
	// Although the expression can be a subquery, it must return a single column and at most a single row.
	// For example, BENCHMARK(10, (SELECT * FROM t)) will fail if the table t has more than one column or
	// more than one row.
	oneColumnQuery := "select benchmark(10, (select a from t))"
	twoColumnQuery := "select benchmark(10, (select * from t))"
	// rows * columns:
	// 0 * 1, success;
	result = tk.MustQuery(oneColumnQuery)
	result.Check(success)
	// 0 * 2, error;
	err = tk.ExecToErr(twoColumnQuery)
	c.Assert(err, NotNil)
	// 1 * 1, success;
	tk.MustExec("insert t values (1, 2)")
	result = tk.MustQuery(oneColumnQuery)
	result.Check(success)
	// 1 * 2, error;
	err = tk.ExecToErr(twoColumnQuery)
	c.Assert(err, NotNil)
	// 2 * 1, error;
	tk.MustExec("insert t values (3, 4)")
	err = tk.ExecToErr(oneColumnQuery)
	c.Assert(err, NotNil)
	// 2 * 2, error.
	err = tk.ExecToErr(twoColumnQuery)
	c.Assert(err, NotNil)
}

func (s *testIntegrationSuite) TestControlBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// for ifnull
	result := tk.MustQuery("select ifnull(1, 2)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select ifnull(null, 2)")
	result.Check(testkit.Rows("2"))
	result = tk.MustQuery("select ifnull(1, null)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select ifnull(null, null)")
	result.Check(testkit.Rows("<nil>"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1(a decimal(20,4))")
	tk.MustExec("create table t2(a decimal(20,4))")
	tk.MustExec("insert into t1 select 1.2345")
	tk.MustExec("insert into t2 select 1.2345")

	result = tk.MustQuery(`select sum(ifnull(a, 0)) from (
	select ifnull(a, 0) as a from t1
	union all
	select ifnull(a, 0) as a from t2
	) t;`)
	result.Check(testkit.Rows("2.4690"))

	// for if
	result = tk.MustQuery(`select IF(0,"ERROR","this"),IF(1,"is","ERROR"),IF(NULL,"ERROR","a"),IF(1,2,3)|0,IF(1,2.0,3.0)+0;`)
	result.Check(testkit.Rows("this is a 2 2.0"))
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (st varchar(255) NOT NULL, u int(11) NOT NULL);")
	tk.MustExec("INSERT INTO t1 VALUES ('a',1),('A',1),('aa',1),('AA',1),('a',1),('aaa',0),('BBB',0);")
	result = tk.MustQuery("select if(1,st,st) s from t1 order by s;")
	result.Check(testkit.Rows("A", "AA", "BBB", "a", "a", "aa", "aaa"))
	result = tk.MustQuery("select if(u=1,st,st) s from t1 order by s;")
	result.Check(testkit.Rows("A", "AA", "BBB", "a", "a", "aa", "aaa"))
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("CREATE TABLE t1 (a varchar(255), b time, c int)")
	tk.MustExec("INSERT INTO t1 VALUE('abc', '12:00:00', 0)")
	tk.MustExec("INSERT INTO t1 VALUE('1abc', '00:00:00', 1)")
	tk.MustExec("INSERT INTO t1 VALUE('0abc', '12:59:59', 0)")
	result = tk.MustQuery("select if(a, b, c), if(b, a, c), if(c, a, b) from t1")
	result.Check(testkit.Rows("0 abc 12:00:00", "00:00:00 1 1abc", "0 0abc 12:59:59"))
	result = tk.MustQuery("select if(1, 1.0, 1)")
	result.Check(testkit.Rows("1.0"))
	// FIXME: MySQL returns `1.0`.
	result = tk.MustQuery("select if(1, 1, 1.0)")
	result.Check(testkit.Rows("1"))

	result = tk.MustQuery("SELECT 79 + + + CASE -87 WHEN -30 THEN COALESCE(COUNT(*), +COALESCE(+15, -33, -12 ) + +72) WHEN +COALESCE(+AVG(DISTINCT(60)), 21) THEN NULL ELSE NULL END AS col0;")
	result.Check(testkit.Rows("<nil>"))

	result = tk.MustQuery("SELECT -63 + COALESCE ( - 83, - 61 + - + 72 * - CAST( NULL AS SIGNED ) + + 3 );")
	result.Check(testkit.Rows("-146"))
}

func (s *testIntegrationSuite) TestArithmeticBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	ctx := context.Background()

	// for plus
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4, 2), b DECIMAL(5, 3));")
	tk.MustExec("INSERT INTO t(a, b) VALUES(1.09, 1.999), (-1.1, -0.1);")
	result := tk.MustQuery("SELECT a+b FROM t;")
	result.Check(testkit.Rows("3.089", "-1.200"))
	result = tk.MustQuery("SELECT b+12, b+0.01, b+0.00001, b+12.00001 FROM t;")
	result.Check(testkit.Rows("13.999 2.009 1.99901 13.99901", "11.900 -0.090 -0.09999 11.90001"))
	result = tk.MustQuery("SELECT 1+12, 21+0.01, 89+\"11\", 12+\"a\", 12+NULL, NULL+1, NULL+NULL;")
	result.Check(testkit.Rows("13 21.01 100 12 <nil> <nil> <nil>"))
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT UNSIGNED, b BIGINT UNSIGNED);")
	tk.MustExec("INSERT INTO t SELECT 1<<63, 1<<63;")
	rs, err := tk.Exec("SELECT a+b FROM t;")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err := session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(test.t.a + test.t.b)'")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select cast(-3 as signed) + cast(2 as unsigned);")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(-3 + 2)'")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select cast(2 as unsigned) + cast(-3 as signed);")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(2 + -3)'")
	c.Assert(rs.Close(), IsNil)

	// for minus
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4, 2), b DECIMAL(5, 3));")
	tk.MustExec("INSERT INTO t(a, b) VALUES(1.09, 1.999), (-1.1, -0.1);")
	result = tk.MustQuery("SELECT a-b FROM t;")
	result.Check(testkit.Rows("-0.909", "-1.000"))
	result = tk.MustQuery("SELECT b-12, b-0.01, b-0.00001, b-12.00001 FROM t;")
	result.Check(testkit.Rows("-10.001 1.989 1.99899 -10.00101", "-12.100 -0.110 -0.10001 -12.10001"))
	result = tk.MustQuery("SELECT 1-12, 21-0.01, 89-\"11\", 12-\"a\", 12-NULL, NULL-1, NULL-NULL;")
	result.Check(testkit.Rows("-11 20.99 78 12 <nil> <nil> <nil>"))
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT UNSIGNED, b BIGINT UNSIGNED);")
	tk.MustExec("INSERT INTO t SELECT 1, 4;")
	rs, err = tk.Exec("SELECT a-b FROM t;")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(test.t.a - test.t.b)'")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select cast(-1 as signed) - cast(-1 as unsigned);")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(-1 - 18446744073709551615)'")
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select cast(-1 as unsigned) - cast(-1 as signed);")
	c.Assert(errors.ErrorStack(err), Equals, "")
	c.Assert(rs, NotNil)
	rows, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(rows, IsNil)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1690]BIGINT UNSIGNED value is out of range in '(18446744073709551615 - -1)'")
	c.Assert(rs.Close(), IsNil)

	tk.MustQuery("select 1234567890 * 1234567890").Check(testkit.Rows("1524157875019052100"))
	rs, err = tk.Exec("select 1234567890 * 12345671890")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)
	tk.MustQuery("select cast(1234567890 as unsigned int) * 12345671890").Check(testkit.Rows("15241570095869612100"))
	tk.MustQuery("select 123344532434234234267890.0 * 1234567118923479823749823749.230").Check(testkit.Rows("152277104042296270209916846800130443726237424001224.7000"))
	rs, err = tk.Exec("select 123344532434234234267890.0 * 12345671189234798237498232384982309489238402830480239849238048239084749.230")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)
	// FIXME: There is something wrong in showing float number.
	//tk.MustQuery("select 1.797693134862315708145274237317043567981e+308 * 1").Check(testkit.Rows("1.7976931348623157e308"))
	//tk.MustQuery("select 1.797693134862315708145274237317043567981e+308 * -1").Check(testkit.Rows("-1.7976931348623157e308"))
	rs, err = tk.Exec("select 1.797693134862315708145274237317043567981e+308 * 1.1")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)
	rs, err = tk.Exec("select 1.797693134862315708145274237317043567981e+308 * -1.1")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)
	result = tk.MustQuery(`select cast(-3 as unsigned) - cast(-1 as signed);`)
	result.Check(testkit.Rows("18446744073709551614"))

	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a DECIMAL(4, 2), b DECIMAL(5, 3));")
	tk.MustExec("INSERT INTO t(a, b) VALUES(-1.09, 1.999);")
	result = tk.MustQuery("SELECT a/b, a/12, a/-0.01, b/12, b/-0.01, b/0.000, NULL/b, b/NULL, NULL/NULL FROM t;")
	result.Check(testkit.Rows("-0.545273 -0.090833 109.000000 0.1665833 -199.9000000 <nil> <nil> <nil> <nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))
	rs, err = tk.Exec("select 1e200/1e-200")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)

	// for intDiv
	result = tk.MustQuery("SELECT 13 DIV 12, 13 DIV 0.01, -13 DIV 2, 13 DIV NULL, NULL DIV 13, NULL DIV NULL;")
	result.Check(testkit.Rows("1 1300 -6 <nil> <nil> <nil>"))
	result = tk.MustQuery("SELECT 2.4 div 1.1, 2.4 div 1.2, 2.4 div 1.3;")
	result.Check(testkit.Rows("2 2 1"))
	result = tk.MustQuery("SELECT 1.175494351E-37 div 1.7976931348623157E+308, 1.7976931348623157E+308 div -1.7976931348623157E+307, 1 div 1e-82;")
	result.Check(testkit.Rows("0 -1 <nil>"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|",
		"Warning|1292|Truncated incorrect DECIMAL value: 'cast(1.7976931348623157e+308)'",
		"Warning|1292|Truncated incorrect DECIMAL value: 'cast(1.7976931348623157e+308)'",
		"Warning|1292|Truncated incorrect DECIMAL value: 'cast(-1.7976931348623158e+307)'",
		"Warning|1365|Division by 0"))
	rs, err = tk.Exec("select 1e300 DIV 1.5")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(terror.ErrorEqual(err, types.ErrOverflow), IsTrue)
	c.Assert(rs.Close(), IsNil)

	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (c_varchar varchar(255), c_time time, nonzero int, zero int, c_int_unsigned int unsigned, c_timestamp timestamp, c_enum enum('a','b','c'));")
	tk.MustExec("INSERT INTO t VALUE('abc', '12:00:00', 12, 0, 5, '2017-08-05 18:19:03', 'b');")
	result = tk.MustQuery("select c_varchar div nonzero, c_time div nonzero, c_time div zero, c_timestamp div nonzero, c_timestamp div zero, c_varchar div zero from t;")
	result.Check(testkit.Rows("0 10000 <nil> 1680900431825 <nil> <nil>"))
	result = tk.MustQuery("select c_enum div nonzero from t;")
	result.Check(testkit.Rows("0"))
	tk.MustQuery("select c_enum div zero from t").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select nonzero div zero from t").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))
	result = tk.MustQuery("select c_time div c_enum, c_timestamp div c_time, c_timestamp div c_enum from t;")
	result.Check(testkit.Rows("60000 168090043 10085402590951"))
	result = tk.MustQuery("select c_int_unsigned div nonzero, nonzero div c_int_unsigned, c_int_unsigned div zero from t;")
	result.Check(testkit.Rows("0 2 <nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))

	// for mod
	result = tk.MustQuery("SELECT CAST(1 AS UNSIGNED) MOD -9223372036854775808, -9223372036854775808 MOD CAST(1 AS UNSIGNED);")
	result.Check(testkit.Rows("1 0"))
	result = tk.MustQuery("SELECT 13 MOD 12, 13 MOD 0.01, -13 MOD 2, 13 MOD NULL, NULL MOD 13, NULL DIV NULL;")
	result.Check(testkit.Rows("1 0.00 -1 <nil> <nil> <nil>"))
	result = tk.MustQuery("SELECT 2.4 MOD 1.1, 2.4 MOD 1.2, 2.4 mod 1.30;")
	result.Check(testkit.Rows("0.2 0.0 1.10"))
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (c_varchar varchar(255), c_time time, nonzero int, zero int, c_timestamp timestamp, c_enum enum('a','b','c'));")
	tk.MustExec("INSERT INTO t VALUE('abc', '12:00:00', 12, 0, '2017-08-05 18:19:03', 'b');")
	result = tk.MustQuery("select c_varchar MOD nonzero, c_time MOD nonzero, c_timestamp MOD nonzero, c_enum MOD nonzero from t;")
	result.Check(testkit.Rows("0 0 3 2"))
	result = tk.MustQuery("select c_time MOD c_enum, c_timestamp MOD c_time, c_timestamp MOD c_enum from t;")
	result.Check(testkit.Rows("0 21903 1"))
	tk.MustQuery("select c_enum MOD zero from t;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))
	tk.MustExec("SET SQL_MODE='ERROR_FOR_DIVISION_BY_ZERO,STRICT_ALL_TABLES';")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (v int);")
	tk.MustExec("INSERT IGNORE INTO t VALUE(12 MOD 0);")
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1365 Division by 0"))
	tk.MustQuery("select v from t;").Check(testkit.Rows("<nil>"))

	_, err = tk.Exec("INSERT INTO t VALUE(12 MOD 0);")
	c.Assert(terror.ErrorEqual(err, expression.ErrDivisionByZero), IsTrue)

	tk.MustQuery("select sum(1.2e2) * 0.1").Check(testkit.Rows("12"))
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a double)")
	tk.MustExec("insert into t value(1.2)")
	tk.MustQuery("select sum(a) * 0.1 from t").Check(testkit.Rows("0.12"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a double)")
	tk.MustExec("insert into t value(1.2)")
	result = tk.MustQuery("select * from t where a/0 > 1")
	result.Check(testkit.Rows())
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1105|Division by 0"))

	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a BIGINT, b DECIMAL(6, 2));")
	tk.MustExec("INSERT INTO t VALUES(0, 1.12), (1, 1.21);")
	tk.MustQuery("SELECT a/b FROM t;").Check(testkit.Rows("0.0000", "0.8264"))
}

func (s *testIntegrationSuite) TestCompareBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	// compare as JSON
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (pk int  NOT NULL PRIMARY KEY AUTO_INCREMENT, i INT, j JSON);")
	tk.MustExec(`INSERT INTO t(i, j) VALUES (0, NULL)`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (1, '{"a": 2}')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (2, '[1,2]')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (3, '{"a":"b", "c":"d","ab":"abc", "bc": ["x", "y"]}')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (4, '["here", ["I", "am"], "!!!"]')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (5, '"scalar string"')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (6, 'true')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (7, 'false')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (8, 'null')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (9, '-1')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (10, CAST(CAST(1 AS UNSIGNED) AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (11, '32767')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (12, '32768')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (13, '-32768')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (14, '-32769')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (15, '2147483647')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (16, '2147483648')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (17, '-2147483648')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (18, '-2147483649')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (19, '18446744073709551615')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (20, '18446744073709551616')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (21, '3.14')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (22, '{}')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (23, '[]')`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (24, CAST(CAST('2015-01-15 23:24:25' AS DATETIME) AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (25, CAST(CAST('23:24:25' AS TIME) AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (26, CAST(CAST('2015-01-15' AS DATE) AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (27, CAST(TIMESTAMP('2015-01-15 23:24:25') AS JSON))`)
	tk.MustExec(`INSERT INTO t(i, j) VALUES (28, CAST('[]' AS CHAR CHARACTER SET 'ascii'))`)

	result := tk.MustQuery(`SELECT i,
		(j = '"scalar string"') AS c1,
		(j = 'scalar string') AS c2,
		(j = CAST('"scalar string"' AS JSON)) AS c3,
		(j = CAST(CAST(j AS CHAR CHARACTER SET 'utf8mb4') AS JSON)) AS c4,
		(j = CAST(NULL AS JSON)) AS c5,
		(j = NULL) AS c6,
		(j <=> NULL) AS c7,
		(j <=> CAST(NULL AS JSON)) AS c8,
		(j IN (-1, 2, 32768, 3.14)) AS c9,
		(j IN (CAST('[1, 2]' AS JSON), CAST('{}' AS JSON), CAST(3.14 AS JSON))) AS c10,
		(j = (SELECT j FROM t WHERE j = CAST('null' AS JSON))) AS c11,
		(j = (SELECT j FROM t WHERE j IS NULL)) AS c12,
		(j = (SELECT j FROM t WHERE 1<>1)) AS c13,
		(j = DATE('2015-01-15')) AS c14,
		(j = TIME('23:24:25')) AS c15,
		(j = TIMESTAMP('2015-01-15 23:24:25')) AS c16,
		(j = CURRENT_TIMESTAMP) AS c17,
		(JSON_EXTRACT(j, '$.a') = 2) AS c18
		FROM t
		ORDER BY i;`)
	result.Check(testkit.Rows("0 <nil> <nil> <nil> <nil> <nil> <nil> 1 1 <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil> <nil>",
		"1 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 1",
		"2 0 0 0 1 <nil> <nil> 0 0 0 1 0 <nil> <nil> 0 0 0 0 <nil>",
		"3 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 0",
		"4 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"5 0 1 1 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"6 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"7 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"8 0 0 0 1 <nil> <nil> 0 0 0 0 1 <nil> <nil> 0 0 0 0 <nil>",
		"9 0 0 0 1 <nil> <nil> 0 0 1 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"10 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"11 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"12 0 0 0 1 <nil> <nil> 0 0 1 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"13 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"14 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"15 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"16 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"17 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"18 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"19 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"20 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"21 0 0 0 1 <nil> <nil> 0 0 1 1 0 <nil> <nil> 0 0 0 0 <nil>",
		"22 0 0 0 1 <nil> <nil> 0 0 0 1 0 <nil> <nil> 0 0 0 0 <nil>",
		"23 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>",
		"24 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 1 0 <nil>",
		"25 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 1 0 0 <nil>",
		"26 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 1 0 0 0 <nil>",
		"27 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 1 0 <nil>",
		"28 0 0 0 1 <nil> <nil> 0 0 0 0 0 <nil> <nil> 0 0 0 0 <nil>"))

	// for coalesce
	result = tk.MustQuery("select coalesce(NULL), coalesce(NULL, NULL), coalesce(NULL, NULL, NULL);")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustQuery(`select coalesce(cast(1 as json), cast(2 as json));`).Check(testkit.Rows(`1`))
	tk.MustQuery(`select coalesce(NULL, cast(2 as json));`).Check(testkit.Rows(`2`))
	tk.MustQuery(`select coalesce(cast(1 as json), NULL);`).Check(testkit.Rows(`1`))
	tk.MustQuery(`select coalesce(NULL, NULL);`).Check(testkit.Rows(`<nil>`))

	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a int, b double, c datetime, d time, e char(20), f bit(10))")
	tk.MustExec(`insert into t2 values(1, 1.1, "2017-08-01 12:01:01", "12:01:01", "abcdef", 0b10101)`)

	result = tk.MustQuery("select coalesce(NULL, a), coalesce(NULL, b, a), coalesce(c, NULL, a, b), coalesce(d, NULL), coalesce(d, c), coalesce(NULL, NULL, e, 1), coalesce(f), coalesce(1, a, b, c, d, e, f) from t2")
	result.Check(testkit.Rows(fmt.Sprintf("1 1.1 2017-08-01 12:01:01 12:01:01 %s 12:01:01 abcdef 21 1", time.Now().In(tk.Se.GetSessionVars().Location()).Format("2006-01-02"))))

	// nullif
	result = tk.MustQuery(`SELECT NULLIF(NULL, 1), NULLIF(1, NULL), NULLIF(1, 1), NULLIF(NULL, NULL);`)
	result.Check(testkit.Rows("<nil> 1 <nil> <nil>"))

	result = tk.MustQuery(`SELECT NULLIF(1, 1.0), NULLIF(1, "1.0");`)
	result.Check(testkit.Rows("<nil> <nil>"))

	result = tk.MustQuery(`SELECT NULLIF("abc", 1);`)
	result.Check(testkit.Rows("abc"))

	result = tk.MustQuery(`SELECT NULLIF(1+2, 1);`)
	result.Check(testkit.Rows("3"))

	result = tk.MustQuery(`SELECT NULLIF(1, 1+2);`)
	result.Check(testkit.Rows("1"))

	result = tk.MustQuery(`SELECT NULLIF(2+3, 1+2);`)
	result.Check(testkit.Rows("5"))

	result = tk.MustQuery(`SELECT HEX(NULLIF("abc", 1));`)
	result.Check(testkit.Rows("616263"))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a date)")
	result = tk.MustQuery("desc select a = a from t")
	result.Check(testkit.Rows(
		"Projection_3 10000.00 root eq(test.t.a, test.t.a)",
		"└─TableReader_5 10000.00 root data:TableScan_4",
		"  └─TableScan_4 10000.00 cop table:t, range:[-inf,+inf], keep order:false, stats:pseudo",
	))

	// for interval
	result = tk.MustQuery(`select interval(null, 1, 2), interval(1, 2, 3), interval(2, 1, 3)`)
	result.Check(testkit.Rows("-1 0 1"))
	result = tk.MustQuery(`select interval(3, 1, 2), interval(0, "b", "1", "2"), interval("a", "b", "1", "2")`)
	result.Check(testkit.Rows("2 1 1"))
	result = tk.MustQuery(`select interval(23, 1, 23, 23, 23, 30, 44, 200), interval(23, 1.7, 15.3, 23.1, 30, 44, 200), interval(9007199254740992, 9007199254740993)`)
	result.Check(testkit.Rows("4 2 0"))
	result = tk.MustQuery(`select interval(cast(9223372036854775808 as unsigned), cast(9223372036854775809 as unsigned)), interval(9223372036854775807, cast(9223372036854775808 as unsigned)), interval(-9223372036854775807, cast(9223372036854775808 as unsigned))`)
	result.Check(testkit.Rows("0 0 0"))
	result = tk.MustQuery(`select interval(cast(9223372036854775806 as unsigned), 9223372036854775807), interval(cast(9223372036854775806 as unsigned), -9223372036854775807), interval("9007199254740991", "9007199254740992")`)
	result.Check(testkit.Rows("0 1 0"))
	result = tk.MustQuery(`select interval(9007199254740992, "9007199254740993"), interval("9007199254740992", 9007199254740993), interval("9007199254740992", "9007199254740993")`)
	result.Check(testkit.Rows("1 1 1"))
	result = tk.MustQuery(`select INTERVAL(100, NULL, NULL, NULL, NULL, NULL, 100);`)
	result.Check(testkit.Rows("6"))

	// for greatest
	result = tk.MustQuery(`select greatest(1, 2, 3), greatest("a", "b", "c"), greatest(1.1, 1.2, 1.3), greatest("123a", 1, 2)`)
	result.Check(testkit.Rows("3 c 1.3 123"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1265|Data Truncated"))
	result = tk.MustQuery(`select greatest(cast("2017-01-01" as datetime), "123", "234", cast("2018-01-01" as date)), greatest(cast("2017-01-01" as date), "123", null)`)
	// todo: MySQL returns "2018-01-01 <nil>"
	result.Check(testkit.Rows("2018-01-01 00:00:00 <nil>"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|invalid time format: '123'", "Warning|1292|invalid time format: '234'", "Warning|1292|invalid time format: '123'"))
	// for least
	result = tk.MustQuery(`select least(1, 2, 3), least("a", "b", "c"), least(1.1, 1.2, 1.3), least("123a", 1, 2)`)
	result.Check(testkit.Rows("1 a 1.1 1"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1265|Data Truncated"))
	result = tk.MustQuery(`select least(cast("2017-01-01" as datetime), "123", "234", cast("2018-01-01" as date)), least(cast("2017-01-01" as date), "123", null)`)
	result.Check(testkit.Rows("123 <nil>"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning|1292|invalid time format: '123'", "Warning|1292|invalid time format: '234'", "Warning|1292|invalid time format: '123'"))
	tk.MustQuery(`select 1 < 17666000000000000000, 1 > 17666000000000000000, 1 = 17666000000000000000`).Check(testkit.Rows("1 0 0"))

	tk.MustExec("drop table if exists t")
	// insert value at utc timezone
	tk.MustExec("set time_zone = '+00:00'")
	tk.MustExec("create table t(a timestamp)")
	tk.MustExec("insert into t value('1991-05-06 04:59:28')")
	// check daylight saving time in Asia/Shanghai
	tk.MustExec("set time_zone='Asia/Shanghai'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1991-05-06 13:59:28"))
	// insert an nonexistent time
	tk.MustExec("set time_zone = 'America/Los_Angeles'")
	_, err := tk.Exec("insert into t value('2011-03-13 02:00:00')")
	c.Assert(err, NotNil)
	// reset timezone to a +8 offset
	tk.MustExec("set time_zone = '+08:00'")
	tk.MustQuery("select * from t").Check(testkit.Rows("1991-05-06 12:59:28"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint unsigned)")
	tk.MustExec("insert into t value(17666000000000000000)")
	tk.MustQuery("select * from t where a = 17666000000000000000").Check(testkit.Rows("17666000000000000000"))

	// test for compare row
	result = tk.MustQuery(`select row(1,2,3)=row(1,2,3)`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select row(1,2,3)=row(1+3,2,3)`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select row(1,2,3)<>row(1,2,3)`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select row(1,2,3)<>row(1+3,2,3)`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select row(1+3,2,3)<>row(1+3,2,3)`)
	result.Check(testkit.Rows("0"))
}

func (s *testIntegrationSuite) TestAggregationBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a decimal(7, 6))")
	tk.MustExec("insert into t values(1.123456), (1.123456)")
	result := tk.MustQuery("select avg(a) from t")
	result.Check(testkit.Rows("1.1234560000"))

	tk.MustExec("use test")
	tk.MustExec("drop table t")
	tk.MustExec("CREATE TABLE `t` (	`a` int, KEY `idx_a` (`a`))")
	result = tk.MustQuery("select avg(a) from t")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select max(a), min(a) from t")
	result.Check(testkit.Rows("<nil> <nil>"))
	result = tk.MustQuery("select distinct a from t")
	result.Check(testkit.Rows())
	result = tk.MustQuery("select sum(a) from t")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select count(a) from t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("18446744073709551615"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinBitOr(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint)")
	tk.MustExec("insert into t values(null);")
	result := tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("0"))
	tk.MustExec("insert into t values(1);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t values(2);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("3"))
	tk.MustExec("insert into t values(4);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("7"))
	result = tk.MustQuery("select a, bit_or(a) from t group by a order by a")
	result.Check(testkit.Rows("<nil> 0", "1 1", "2 2", "4 4"))
	tk.MustExec("insert into t values(-1);")
	result = tk.MustQuery("select bit_or(a) from t")
	result.Check(testkit.Rows("18446744073709551615"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinBitXor(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint)")
	tk.MustExec("insert into t values(null);")
	result := tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("0"))
	tk.MustExec("insert into t values(1);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t values(2);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("3"))
	tk.MustExec("insert into t values(3);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("0"))
	tk.MustExec("insert into t values(3);")
	result = tk.MustQuery("select bit_xor(a) from t")
	result.Check(testkit.Rows("3"))
	result = tk.MustQuery("select a, bit_xor(a) from t group by a order by a")
	result.Check(testkit.Rows("<nil> 0", "1 1", "2 2", "3 0"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinBitAnd(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a bigint)")
	tk.MustExec("insert into t values(null);")
	result := tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("18446744073709551615"))
	tk.MustExec("insert into t values(7);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("7"))
	tk.MustExec("insert into t values(5);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("5"))
	tk.MustExec("insert into t values(3);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("1"))
	tk.MustExec("insert into t values(2);")
	result = tk.MustQuery("select bit_and(a) from t")
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery("select a, bit_and(a) from t group by a order by a desc")
	result.Check(testkit.Rows("7 7", "5 5", "3 3", "2 2", "<nil> 18446744073709551615"))
}

func (s *testIntegrationSuite) TestAggregationBuiltinGroupConcat(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a varchar(100))")
	tk.MustExec("create table d(a varchar(100))")
	tk.MustExec("insert into t values('hello'), ('hello')")
	result := tk.MustQuery("select group_concat(a) from t")
	result.Check(testkit.Rows("hello,hello"))

	tk.MustExec("set @@group_concat_max_len=7")
	result = tk.MustQuery("select group_concat(a) from t")
	result.Check(testkit.Rows("hello,h"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning 1260 Some rows were cut by GROUPCONCAT(test.t.a)"))

	_, err := tk.Exec("insert into d select group_concat(a) from t")
	c.Assert(errors.Cause(err).(*terror.Error).Code(), Equals, terror.ErrCode(mysql.ErrCutValueGroupConcat))

	tk.Exec("set sql_mode=''")
	tk.MustExec("insert into d select group_concat(a) from t")
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|", "Warning 1260 Some rows were cut by GROUPCONCAT(test.t.a)"))
	tk.MustQuery("select * from d").Check(testkit.Rows("hello,h"))
}

func (s *testIntegrationSuite) TestOtherBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b double, c varchar(20), d datetime, e time)")
	tk.MustExec("insert into t value(1, 2, 'string', '2017-01-01 12:12:12', '12:12:12')")

	// for in
	result := tk.MustQuery("select 1 in (a, b, c), 'string' in (a, b, c), '2017-01-01 12:12:12' in (c, d, e), '12:12:12' in (c, d, e) from t")
	result.Check(testkit.Rows("1 1 1 1"))
	result = tk.MustQuery("select 1 in (null, c), 2 in (null, c) from t")
	result.Check(testkit.Rows("<nil> <nil>"))
	result = tk.MustQuery("select 0 in (a, b, c), 0 in (a, b, c), 3 in (a, b, c), 4 in (a, b, c) from t")
	result.Check(testkit.Rows("1 1 0 0"))
	result = tk.MustQuery("select (0,1) in ((0,1), (0,2)), (0,1) in ((0,0), (0,2))")
	result.Check(testkit.Rows("1 0"))

	result = tk.MustQuery(`select bit_count(121), bit_count(-1), bit_count(null), bit_count("1231aaa");`)
	result.Check(testkit.Rows("5 64 <nil> 7"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b time, c double, d varchar(10))")
	tk.MustExec(`insert into t values(1, '01:01:01', 1.1, "1"), (2, '02:02:02', 2.2, "2")`)
	tk.MustExec(`insert into t(a, b) values(1, '12:12:12') on duplicate key update a = values(b)`)
	result = tk.MustQuery(`select a from t order by a`)
	result.Check(testkit.Rows("2", "121212"))
	tk.MustExec(`insert into t values(2, '12:12:12', 1.1, "3.3") on duplicate key update a = values(c) + values(d)`)
	result = tk.MustQuery(`select a from t order by a`)
	result.Check(testkit.Rows("4", "121212"))

	// for setvar, getvar
	tk.MustExec(`set @varname = "Abc"`)
	result = tk.MustQuery(`select @varname, @VARNAME`)
	result.Check(testkit.Rows("Abc Abc"))

	// for values
	tk.MustExec("drop table t")
	tk.MustExec("CREATE TABLE `t` (`id` varchar(32) NOT NULL, `count` decimal(18,2), PRIMARY KEY (`id`));")
	tk.MustExec("INSERT INTO t (id,count)VALUES('abc',2) ON DUPLICATE KEY UPDATE count=if(VALUES(count) > count,VALUES(count),count)")
	result = tk.MustQuery("select count from t where id = 'abc'")
	result.Check(testkit.Rows("2.00"))
	tk.MustExec("INSERT INTO t (id,count)VALUES('abc',265.0) ON DUPLICATE KEY UPDATE count=if(VALUES(count) > count,VALUES(count),count)")
	result = tk.MustQuery("select count from t where id = 'abc'")
	result.Check(testkit.Rows("265.00"))

	// for values(issue #4884)
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table test(id int not null, val text, primary key(id));")
	tk.MustExec("insert into test values(1,'hello');")
	result = tk.MustQuery("select * from test;")
	result.Check(testkit.Rows("1 hello"))
	tk.MustExec("insert into test values(1, NULL) on duplicate key update val = VALUES(val);")
	result = tk.MustQuery("select * from test;")
	result.Check(testkit.Rows("1 <nil>"))

	tk.MustExec("drop table if exists test;")
	tk.MustExec(`create table test(
		id int not null,
		a text,
		b blob,
		c varchar(20),
		d int,
		e float,
		f DECIMAL(6,4),
		g JSON,
		primary key(id));`)

	tk.MustExec(`insert into test values(1,'txt hello', 'blb hello', 'vc hello', 1, 1.1, 1.0, '{"key1": "value1", "key2": "value2"}');`)
	tk.MustExec(`insert into test values(1, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
	on duplicate key update
	a = values(a),
	b = values(b),
	c = values(c),
	d = values(d),
	e = values(e),
	f = values(f),
	g = values(g);`)

	result = tk.MustQuery("select * from test;")
	result.Check(testkit.Rows("1 <nil> <nil> <nil> <nil> <nil> <nil> <nil>"))
}

func (s *testIntegrationSuite) TestDateBuiltin(c *C) {
	ctx := context.Background()
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("create table t (d date);")
	tk.MustExec("insert into t values ('1997-01-02')")
	tk.MustExec("insert into t values ('1998-01-02')")
	r := tk.MustQuery("select * from t where d < date '1998-01-01';")
	r.Check(testkit.Rows("1997-01-02"))

	r = tk.MustQuery("select date'20171212'")
	r.Check(testkit.Rows("2017-12-12"))

	r = tk.MustQuery("select date'2017/12/12'")
	r.Check(testkit.Rows("2017-12-12"))

	r = tk.MustQuery("select date'2017/12-12'")
	r.Check(testkit.Rows("2017-12-12"))

	tk.MustExec("set sql_mode = ''")
	r = tk.MustQuery("select date '0000-00-00';")
	r.Check(testkit.Rows("0000-00-00"))

	tk.MustExec("set sql_mode = 'NO_ZERO_IN_DATE'")
	r = tk.MustQuery("select date '0000-00-00';")
	r.Check(testkit.Rows("0000-00-00"))

	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	rs, err := tk.Exec("select date '0000-00-00';")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("0000-00-00")), IsTrue)
	c.Assert(rs.Close(), IsNil)

	tk.MustExec("set sql_mode = ''")
	r = tk.MustQuery("select date '2007-10-00';")
	r.Check(testkit.Rows("2007-10-00"))

	tk.MustExec("set sql_mode = 'NO_ZERO_IN_DATE'")
	rs, _ = tk.Exec("select date '2007-10-00';")
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("2017-10-00")), IsTrue)
	c.Assert(rs.Close(), IsNil)

	tk.MustExec("set sql_mode = 'NO_ZERO_DATE'")
	r = tk.MustQuery("select date '2007-10-00';")
	r.Check(testkit.Rows("2007-10-00"))

	tk.MustExec("set sql_mode = 'NO_ZERO_IN_DATE,NO_ZERO_DATE'")

	rs, _ = tk.Exec("select date '2007-10-00';")
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("2017-10-00")), IsTrue)
	c.Assert(rs.Close(), IsNil)

	rs, err = tk.Exec("select date '0000-00-00';")
	c.Assert(err, IsNil)
	_, err = session.GetRows4Test(ctx, tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("0000-00-00")), IsTrue)
	c.Assert(rs.Close(), IsNil)

	r = tk.MustQuery("select date'1998~01~02'")
	r.Check(testkit.Rows("1998-01-02"))

	r = tk.MustQuery("select date'731124', date '011124'")
	r.Check(testkit.Rows("1973-11-24 2001-11-24"))

	_, err = tk.Exec("select date '0000-00-00 00:00:00';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("0000-00-00 00:00:00")), IsTrue)

	_, err = tk.Exec("select date '2017-99-99';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue)

	_, err = tk.Exec("select date '2017-2-31';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrInvalidTimeFormat), IsTrue)

	_, err = tk.Exec("select date '201712-31';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("201712-31")), IsTrue)

	_, err = tk.Exec("select date 'abcdefg';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("abcdefg")), IsTrue)
}

func (s *testIntegrationSuite) TestJSONBuiltin(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE `my_collection` (	`doc` json DEFAULT NULL, `_id` varchar(32) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(doc,'$._id'))) STORED NOT NULL, PRIMARY KEY (`_id`))")
	_, err := tk.Exec("UPDATE `test`.`my_collection` SET doc=JSON_SET(doc) WHERE (JSON_EXTRACT(doc,'$.name') = 'clare');")
	c.Assert(err, NotNil)
}

func (s *testIntegrationSuite) TestTimeLiteral(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)

	r := tk.MustQuery("select time '117:01:12';")
	r.Check(testkit.Rows("117:01:12"))

	r = tk.MustQuery("select time '01:00:00.999999';")
	r.Check(testkit.Rows("01:00:00.999999"))

	r = tk.MustQuery("select time '1 01:00:00';")
	r.Check(testkit.Rows("25:00:00"))

	r = tk.MustQuery("select time '110:00:00';")
	r.Check(testkit.Rows("110:00:00"))

	r = tk.MustQuery("select time'-1:1:1.123454656';")
	r.Check(testkit.Rows("-01:01:01.123455"))

	r = tk.MustQuery("select time '33:33';")
	r.Check(testkit.Rows("33:33:00"))

	r = tk.MustQuery("select time '1.1';")
	r.Check(testkit.Rows("00:00:01.1"))

	r = tk.MustQuery("select time '21';")
	r.Check(testkit.Rows("00:00:21"))

	r = tk.MustQuery("select time '20 20:20';")
	r.Check(testkit.Rows("500:20:00"))

	_, err := tk.Exec("select time '2017-01-01 00:00:00';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("2017-01-01 00:00:00")), IsTrue)

	_, err = tk.Exec("select time '071231235959.999999';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("071231235959.999999")), IsTrue)

	_, err = tk.Exec("select time '20171231235959.999999';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("20171231235959.999999")), IsTrue)
}

func (s *testIntegrationSuite) TestTimestampLiteral(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)

	r := tk.MustQuery("select timestamp '2017-01-01 00:00:00';")
	r.Check(testkit.Rows("2017-01-01 00:00:00"))

	r = tk.MustQuery("select timestamp '2017@01@01 00:00:00';")
	r.Check(testkit.Rows("2017-01-01 00:00:00"))

	r = tk.MustQuery("select timestamp '2017@01@01 00~00~00';")
	r.Check(testkit.Rows("2017-01-01 00:00:00"))

	r = tk.MustQuery("select timestamp '2017@01@0001 00~00~00.333';")
	r.Check(testkit.Rows("2017-01-01 00:00:00.333"))

	_, err := tk.Exec("select timestamp '00:00:00';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("00:00:00")), IsTrue)

	_, err = tk.Exec("select timestamp '1992-01-03';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("1992-01-03")), IsTrue)

	_, err = tk.Exec("select timestamp '20171231235959.999999';")
	c.Assert(err, NotNil)
	c.Assert(terror.ErrorEqual(err, types.ErrIncorrectDatetimeValue.GenWithStackByArgs("20171231235959.999999")), IsTrue)
}

func (s *testIntegrationSuite) TestLiterals(c *C) {
	defer s.cleanEnv(c)
	tk := testkit.NewTestKit(c, s.store)
	r := tk.MustQuery("SELECT LENGTH(b''), LENGTH(B''), b''+1, b''-1, B''+1;")
	r.Check(testkit.Rows("0 0 1 -1 1"))
}

func (s *testIntegrationSuite) TestFuncJSON(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS table_json;")
	tk.MustExec("CREATE TABLE table_json(a json, b VARCHAR(255));")

	j1 := `{"\\"hello\\"": "world", "a": [1, "2", {"aa": "bb"}, 4.0, {"aa": "cc"}], "b": true, "c": ["d"]}`
	j2 := `[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]`
	for _, j := range []string{j1, j2} {
		tk.MustExec(fmt.Sprintf(`INSERT INTO table_json values('%s', '%s')`, j, j))
	}

	r := tk.MustQuery(`select json_type(a), json_type(b) from table_json`)
	r.Check(testkit.Rows("OBJECT OBJECT", "ARRAY ARRAY"))

	r = tk.MustQuery(`select json_unquote('hello'), json_unquote('world')`)
	r.Check(testkit.Rows("hello world"))

	r = tk.MustQuery(`select
		json_quote(''),
		json_quote('""'),
		json_quote('a'),
		json_quote('3'),
		json_quote('{"a": "b"}'),
		json_quote('{"a":     "b"}'),
		json_quote('hello,"quoted string",world'),
		json_quote('hello,"宽字符",world'),
		json_quote('Invalid Json string	is OK'),
		json_quote('1\u2232\u22322')
	`)
	r.Check(testkit.Rows(
		`"" "\"\"" "a" "3" "{\"a\": \"b\"}" "{\"a\":     \"b\"}" "hello,\"quoted string\",world" "hello,\"宽字符\",world" "Invalid Json string\tis OK" "1u2232u22322"`,
	))

	r = tk.MustQuery(`select json_extract(a, '$.a[1]'), json_extract(b, '$.b') from table_json`)
	r.Check(testkit.Rows("\"2\" true", "<nil> <nil>"))

	r = tk.MustQuery(`select json_extract(json_set(a, '$.a[1]', 3), '$.a[1]'), json_extract(json_set(b, '$.b', false), '$.b') from table_json`)
	r.Check(testkit.Rows("3 false", "<nil> <nil>"))

	r = tk.MustQuery(`select json_extract(json_insert(a, '$.a[1]', 3), '$.a[1]'), json_extract(json_insert(b, '$.b', false), '$.b') from table_json`)
	r.Check(testkit.Rows("\"2\" true", "<nil> <nil>"))

	r = tk.MustQuery(`select json_extract(json_replace(a, '$.a[1]', 3), '$.a[1]'), json_extract(json_replace(b, '$.b', false), '$.b') from table_json`)
	r.Check(testkit.Rows("3 false", "<nil> <nil>"))

	r = tk.MustQuery(`select json_extract(json_merge(a, cast(b as JSON)), '$[0].a[0]') from table_json`)
	r.Check(testkit.Rows("1", "1"))

	r = tk.MustQuery(`select json_extract(json_array(1,2,3), '$[1]')`)
	r.Check(testkit.Rows("2"))

	r = tk.MustQuery(`select json_extract(json_object(1,2,3,4), '$."1"')`)
	r.Check(testkit.Rows("2"))

	tk.MustExec(`update table_json set a=json_set(a,'$.a',json_object('a',1,'b',2)) where json_extract(a,'$.a[1]') = '2'`)
	r = tk.MustQuery(`select json_extract(a, '$.a.a'), json_extract(a, '$.a.b') from table_json`)
	r.Check(testkit.Rows("1 2", "<nil> <nil>"))

	r = tk.MustQuery(`select json_contains(NULL, '1'), json_contains('1', NULL), json_contains('1', '1', NULL)`)
	r.Check(testkit.Rows("<nil> <nil> <nil>"))
	r = tk.MustQuery(`select json_contains('{}','{}'), json_contains('[1]','1'), json_contains('[1]','"1"'), json_contains('[1,2,[1,[5,[3]]]]', '[1,3]', '$[2]'), json_contains('[1,2,[1,[5,{"a":[2,3]}]]]', '[1,{"a":[3]}]', "$[2]"), json_contains('{"a":1}', '{"a":1,"b":2}', "$")`)
	r.Check(testkit.Rows("1 1 0 1 1 0"))
	r = tk.MustQuery(`select json_contains('{"a": 1}', '1', "$.c"), json_contains('{"a": [1, 2]}', '1', "$.a[2]"), json_contains('{"a": [1, {"a": 1}]}', '1', "$.a[1].b")`)
	r.Check(testkit.Rows("<nil> <nil> <nil>"))
	rs, err := tk.Exec("select json_contains('1','1','$.*')")
	c.Assert(err, IsNil)
	c.Assert(rs, NotNil)
	_, err = session.GetRows4Test(context.Background(), tk.Se, rs)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[json:3149]In this situation, path expressions may not contain the * and ** tokens.")

	r = tk.MustQuery(`select
		json_contains_path(NULL, 'one', "$.c"),
		json_contains_path(NULL, 'all', "$.c"),
		json_contains_path('{"a": 1}', NULL, "$.c"),
		json_contains_path('{"a": 1}', 'one', NULL),
		json_contains_path('{"a": 1}', 'all', NULL)
	`)
	r.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))

	r = tk.MustQuery(`select
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.c.d'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.a.d'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.c.d'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.a.d')
	`)
	r.Check(testkit.Rows("1 0 1 0"))

	r = tk.MustQuery(`select
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.a', '$.e'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.a', '$.b'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.a', '$.e'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.a', '$.b')
	`)
	r.Check(testkit.Rows("1 1 0 1"))

	r = tk.MustQuery(`select
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$.*'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'one', '$[*]'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$.*'),
		json_contains_path('{"a": 1, "b": 2, "c": {"d": 4}}', 'all', '$[*]')
	`)
	r.Check(testkit.Rows("1 0 1 0"))

	r = tk.MustQuery(`select

		json_keys('{}'),
		json_keys('{"a": 1, "b": 2}'),
		json_keys('{"a": {"c": 3}, "b": 2}'),
		json_keys('{"a": {"c": 3}, "b": 2}', "$.a")
	`)
	r.Check(testkit.Rows(`[] ["a", "b"] ["a", "b"] ["c"]`))

	r = tk.MustQuery(`select
		json_length('1'),
		json_length('{}'),
		json_length('[]'),
		json_length('{"a": 1}'),
		json_length('{"a": 1, "b": 2}'),
		json_length('[1, 2, 3]')
	`)
	r.Check(testkit.Rows("1 0 0 1 2 3"))
}

func (s *testIntegrationSuite) TestColumnInfoModified(c *C) {
	testKit := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists tab0")
	testKit.MustExec("CREATE TABLE tab0(col0 INTEGER, col1 INTEGER, col2 INTEGER)")
	testKit.MustExec("SELECT + - (- CASE + col0 WHEN + CAST( col0 AS SIGNED ) THEN col1 WHEN 79 THEN NULL WHEN + - col1 THEN col0 / + col0 END ) * - 16 FROM tab0")
	ctx := testKit.Se.(sessionctx.Context)
	is := domain.GetDomain(ctx).InfoSchema()
	tbl, _ := is.TableByName(model.NewCIStr("test"), model.NewCIStr("tab0"))
	col := table.FindCol(tbl.Cols(), "col1")
	c.Assert(col.Tp, Equals, mysql.TypeLong)
}

func (s *testIntegrationSuite) TestSetVariables(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	_, err := tk.Exec("set sql_mode='adfasdfadsfdasd';")
	c.Assert(err, NotNil)
	_, err = tk.Exec("set @@sql_mode='adfasdfadsfdasd';")
	c.Assert(err, NotNil)
	_, err = tk.Exec("set @@global.sql_mode='adfasdfadsfdasd';")
	c.Assert(err, NotNil)
	_, err = tk.Exec("set @@session.sql_mode='adfasdfadsfdasd';")
	c.Assert(err, NotNil)

	var r *testkit.Result
	_, err = tk.Exec("set @@session.sql_mode=',NO_ZERO_DATE,ANSI,ANSI_QUOTES';")
	c.Assert(err, IsNil)
	r = tk.MustQuery(`select @@session.sql_mode`)
	r.Check(testkit.Rows("NO_ZERO_DATE,REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"))
	r = tk.MustQuery(`show variables like 'sql_mode'`)
	r.Check(testkit.Rows("sql_mode NO_ZERO_DATE,REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"))

	// for invalid SQL mode.
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tab0")
	tk.MustExec("CREATE TABLE tab0(col1 time)")
	_, err = tk.Exec("set sql_mode='STRICT_TRANS_TABLES';")
	c.Assert(err, IsNil)
	_, err = tk.Exec("INSERT INTO tab0 select cast('999:44:33' as time);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1292]Truncated incorrect time value: '999h44m33s'")
	_, err = tk.Exec("set sql_mode=' ,';")
	c.Assert(err, NotNil)
	_, err = tk.Exec("INSERT INTO tab0 select cast('999:44:33' as time);")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "[types:1292]Truncated incorrect time value: '999h44m33s'")

	// issue #5478
	_, err = tk.Exec("set session transaction read write;")
	c.Assert(err, IsNil)
	_, err = tk.Exec("set global transaction read write;")
	c.Assert(err, IsNil)
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("0 0 0 0"))

	_, err = tk.Exec("set session transaction read only;")
	c.Assert(err, IsNil)
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("1 0 1 0"))
	_, err = tk.Exec("set global transaction read only;")
	c.Assert(err, IsNil)
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("1 1 1 1"))

	_, err = tk.Exec("set session transaction read write;")
	c.Assert(err, IsNil)
	_, err = tk.Exec("set global transaction read write;")
	c.Assert(err, IsNil)
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("0 0 0 0"))

	_, err = tk.Exec("set @@global.max_user_connections='';")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, variable.ErrWrongTypeForVar.GenWithStackByArgs("max_user_connections").Error())
	_, err = tk.Exec("set @@global.max_prepared_stmt_count='';")
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, variable.ErrWrongTypeForVar.GenWithStackByArgs("max_prepared_stmt_count").Error())
}

func (s *testIntegrationSuite) TestIssues(c *C) {
	// for issue #4954
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a CHAR(5) CHARACTER SET latin1);")
	tk.MustExec("INSERT INTO t VALUES ('oe');")
	tk.MustExec("INSERT INTO t VALUES (0xf6);")
	r := tk.MustQuery(`SELECT * FROM t WHERE a= 'oe';`)
	r.Check(testkit.Rows("oe"))
	r = tk.MustQuery(`SELECT HEX(a) FROM t WHERE a= 0xf6;`)
	r.Check(testkit.Rows("F6"))

	// for issue #4006
	tk.MustExec(`drop table if exists tb`)
	tk.MustExec("create table tb(id int auto_increment primary key, v varchar(32));")
	tk.MustExec("insert into tb(v) (select v from tb);")
	r = tk.MustQuery(`SELECT * FROM tb;`)
	r.Check(testkit.Rows())
	tk.MustExec(`insert into tb(v) values('hello');`)
	tk.MustExec("insert into tb(v) (select v from tb);")
	r = tk.MustQuery(`SELECT * FROM tb;`)
	r.Check(testkit.Rows("1 hello", "2 hello"))

	// for issue #5111
	tk.MustExec(`drop table if exists t`)
	tk.MustExec("create table t(c varchar(32));")
	tk.MustExec("insert into t values('1e649'),('-1e649');")
	r = tk.MustQuery(`SELECT * FROM t where c < 1;`)
	r.Check(testkit.Rows("-1e649"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|",
		"Warning|1292|Truncated incorrect DOUBLE value: '1e649'",
		"Warning|1292|Truncated incorrect DOUBLE value: '-1e649'"))
	r = tk.MustQuery(`SELECT * FROM t where c > 1;`)
	r.Check(testkit.Rows("1e649"))
	tk.MustQuery("show warnings").Check(testutil.RowsWithSep("|",
		"Warning|1292|Truncated incorrect DOUBLE value: '1e649'",
		"Warning|1292|Truncated incorrect DOUBLE value: '-1e649'"))

	// for issue #5293
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert t values (1)")
	tk.MustQuery("select * from t where cast(a as binary)").Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite) TestInPredicate4UnsignedInt(c *C) {
	// for issue #6661
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (a bigint unsigned,key (a));")
	tk.MustExec("INSERT INTO t VALUES (0), (4), (5), (6), (7), (8), (9223372036854775810), (18446744073709551614), (18446744073709551615);")
	r := tk.MustQuery(`SELECT a FROM t WHERE a NOT IN (-1, -2, 18446744073709551615);`)
	r.Check(testkit.Rows("0", "4", "5", "6", "7", "8", "9223372036854775810", "18446744073709551614"))
	r = tk.MustQuery(`SELECT a FROM t WHERE a NOT IN (-1, -2, 4, 9223372036854775810);`)
	r.Check(testkit.Rows("0", "5", "6", "7", "8", "18446744073709551614", "18446744073709551615"))
	r = tk.MustQuery(`SELECT a FROM t WHERE a NOT IN (-1, -2, 0, 4, 18446744073709551614);`)
	r.Check(testkit.Rows("5", "6", "7", "8", "9223372036854775810", "18446744073709551615"))

	// for issue #4473
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1 (some_id smallint(5) unsigned,key (some_id) )")
	tk.MustExec("insert into t1 values (1),(2)")
	r = tk.MustQuery(`select some_id from t1 where some_id not in(2,-1);`)
	r.Check(testkit.Rows("1"))
}

func (s *testIntegrationSuite) TestFilterExtractFromDNF(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int)")

	tests := []struct {
		exprStr string
		result  string
	}{
		{
			exprStr: "a = 1 or a = 1 or a = 1",
			result:  "[eq(test.t.a, 1)]",
		},
		{
			exprStr: "a = 1 or a = 1 or (a = 1 and b = 1)",
			result:  "[eq(test.t.a, 1)]",
		},
		{
			exprStr: "(a = 1 and a = 1) or a = 1 or b = 1",
			result:  "[or(or(and(eq(test.t.a, 1), eq(test.t.a, 1)), eq(test.t.a, 1)), eq(test.t.b, 1))]",
		},
		{
			exprStr: "(a = 1 and b = 2) or (a = 1 and b = 3) or (a = 1 and b = 4)",
			result:  "[eq(test.t.a, 1) or(eq(test.t.b, 2), or(eq(test.t.b, 3), eq(test.t.b, 4)))]",
		},
		{
			exprStr: "(a = 1 and b = 1 and c = 1) or (a = 1 and b = 1) or (a = 1 and b = 1 and c > 2 and c < 3)",
			result:  "[eq(test.t.a, 1) eq(test.t.b, 1)]",
		},
	}

	for _, tt := range tests {
		sql := "select * from t where " + tt.exprStr
		ctx := tk.Se.(sessionctx.Context)
		sc := ctx.GetSessionVars().StmtCtx
		stmts, err := session.Parse(ctx, sql)
		c.Assert(err, IsNil, Commentf("error %v, for expr %s", err, tt.exprStr))
		c.Assert(stmts, HasLen, 1)
		is := domain.GetDomain(ctx).InfoSchema()
		err = plannercore.Preprocess(ctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for resolve name, expr %s", err, tt.exprStr))
		p, err := plannercore.BuildLogicalPlan(ctx, stmts[0], is)
		c.Assert(err, IsNil, Commentf("error %v, for build plan, expr %s", err, tt.exprStr))
		selection := p.(plannercore.LogicalPlan).Children()[0].(*plannercore.LogicalSelection)
		conds := make([]expression.Expression, 0, len(selection.Conditions))
		for _, cond := range selection.Conditions {
			conds = append(conds, expression.PushDownNot(ctx, cond, false))
		}
		afterFunc := expression.ExtractFiltersFromDNFs(ctx, conds)
		sort.Slice(afterFunc, func(i, j int) bool {
			return bytes.Compare(afterFunc[i].HashCode(sc), afterFunc[j].HashCode(sc)) < 0
		})
		c.Assert(fmt.Sprintf("%s", afterFunc), Equals, tt.result, Commentf("wrong result for expr: %s", tt.exprStr))
	}
}

func (s *testIntegrationSuite) testTiDBIsOwnerFunc(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	result := tk.MustQuery("select tidb_is_ddl_owner()")
	ddlOwnerChecker := tk.Se.DDLOwnerChecker()
	c.Assert(ddlOwnerChecker, NotNil)
	var ret int64
	if ddlOwnerChecker.IsOwner() {
		ret = 1
	}
	result.Check(testkit.Rows(fmt.Sprintf("%v", ret)))
}

func newStoreWithBootstrap() (kv.Storage, *domain.Domain, error) {
	store, err := mockstore.NewMockTikvStore()
	if err != nil {
		return nil, nil, err
	}
	session.SetSchemaLease(0)
	dom, err := session.BootstrapSession(store)
	return store, dom, err
}

func (s *testIntegrationSuite) TestTwoDecimalTruncate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode=''")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t1(a decimal(10,5), b decimal(10,1))")
	tk.MustExec("insert into t1 values(123.12345, 123.12345)")
	tk.MustExec("update t1 set b = a")
	res := tk.MustQuery("select a, b from t1")
	res.Check(testkit.Rows("123.12345 123.1"))
	res = tk.MustQuery("select 2.00000000000000000000000000000001 * 1.000000000000000000000000000000000000000000002")
	res.Check(testkit.Rows("2.000000000000000000000000000000"))
}

func (s *testIntegrationSuite) TestPrefixIndex(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1 (
  			name varchar(12) DEFAULT NULL,
  			KEY pname (name(12))
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci`)

	tk.MustExec("insert into t1 values('借款策略集_网页');")
	res := tk.MustQuery("select * from t1 where name = '借款策略集_网页';")
	res.Check(testkit.Rows("借款策略集_网页"))

	tk.MustExec(`CREATE TABLE prefix (
		a int(11) NOT NULL,
		b varchar(55) DEFAULT NULL,
		c int(11) DEFAULT NULL,
		PRIMARY KEY (a),
		KEY prefix_index (b(2)),
		KEY prefix_complex (a,b(2))
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)

	tk.MustExec("INSERT INTO prefix VALUES(0, 'b', 2), (1, 'bbb', 3), (2, 'bbc', 4), (3, 'bbb', 5), (4, 'abc', 6), (5, 'abc', 7), (6, 'abc', 7), (7, 'ÿÿ', 8), (8, 'ÿÿ0', 9), (9, 'ÿÿÿ', 10);")
	res = tk.MustQuery("select c, b from prefix where b > 'ÿ' and b < 'ÿÿc'")
	res.Check(testkit.Rows("8 ÿÿ", "9 ÿÿ0"))

	res = tk.MustQuery("select a, b from prefix where b LIKE 'ÿÿ%'")
	res.Check(testkit.Rows("7 ÿÿ", "8 ÿÿ0", "9 ÿÿÿ"))
}

func (s *testIntegrationSuite) TestDecimalMul(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test")
	tk.MustExec("create table t(a decimal(38, 17));")
	tk.MustExec("insert into t select 0.5999991229316*0.918755041726043;")
	res := tk.MustQuery("select * from t;")
	res.Check(testkit.Rows("0.55125221922461136"))
}

func (s *testIntegrationSuite) TestUnknowHintIgnore(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("USE test")
	tk.MustExec("create table t(a int)")
	tk.MustQuery("select /*+ unknown_hint(c1)*/ 1").Check(testkit.Rows("1"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1064 You have an error in your SQL syntax; check the manual that corresponds to your TiDB version for the right syntax to use line 1 column 29 near \"unknown_hint(c1)*/ 1\" "))
	_, err := tk.Exec("select 1 from /*+ test1() */ t")
	c.Assert(err, NotNil)
}

func (s *testIntegrationSuite) TestValuesInNonInsertStmt(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a bigint, b double, c decimal, d varchar(20), e datetime, f time, g json);`)
	tk.MustExec(`insert into t values(1, 1.1, 2.2, "abc", "2018-10-24", NOW(), "12");`)
	res := tk.MustQuery(`select values(a), values(b), values(c), values(d), values(e), values(f), values(g) from t;`)
	res.Check(testkit.Rows(`<nil> <nil> <nil> <nil> <nil> <nil> <nil>`))
}

func (s *testIntegrationSuite) TestForeignKeyVar(c *C) {

	tk := testkit.NewTestKit(c, s.store)

	tk.MustExec("SET FOREIGN_KEY_CHECKS=1")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1105 variable 'foreign_key_checks' does not yet support value: 1"))
}

func (s *testIntegrationSuite) TestUserVarMockWindFunc(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (a int, b varchar (20), c varchar (20));`)
	tk.MustExec(`insert into t values
					(1,'key1-value1','insert_order1'),
    				(1,'key1-value2','insert_order2'),
    				(1,'key1-value3','insert_order3'),
    				(1,'key1-value4','insert_order4'),
    				(1,'key1-value5','insert_order5'),
    				(1,'key1-value6','insert_order6'),
    				(2,'key2-value1','insert_order1'),
    				(2,'key2-value2','insert_order2'),
    				(2,'key2-value3','insert_order3'),
    				(2,'key2-value4','insert_order4'),
    				(2,'key2-value5','insert_order5'),
    				(2,'key2-value6','insert_order6'),
    				(3,'key3-value1','insert_order1'),
    				(3,'key3-value2','insert_order2'),
    				(3,'key3-value3','insert_order3'),
    				(3,'key3-value4','insert_order4'),
    				(3,'key3-value5','insert_order5'),
    				(3,'key3-value6','insert_order6');
					`)
	tk.MustExec(`SET @LAST_VAL := NULL;`)
	tk.MustExec(`SET @ROW_NUM := 0;`)

	tk.MustQuery(`select * from (
					SELECT a,
    				       @ROW_NUM := IF(a = @LAST_VAL, @ROW_NUM + 1, 1) AS ROW_NUM,
    				       @LAST_VAL := a AS LAST_VAL,
    				       b,
    				       c
    				FROM (select * from t where a in (1, 2, 3) ORDER BY a, c) t1
				) t2
				where t2.ROW_NUM < 2;
				`).Check(testkit.Rows(
		`1 1 1 key1-value1 insert_order1`,
		`2 1 2 key2-value1 insert_order1`,
		`3 1 3 key3-value1 insert_order1`,
	))

	tk.MustQuery(`select * from (
					SELECT a,
    				       @ROW_NUM := IF(a = @LAST_VAL, @ROW_NUM + 1, 1) AS ROW_NUM,
    				       @LAST_VAL := a AS LAST_VAL,
    				       b,
    				       c
    				FROM (select * from t where a in (1, 2, 3) ORDER BY a, c) t1
				) t2;
				`).Check(testkit.Rows(
		`1 1 1 key1-value1 insert_order1`,
		`1 2 1 key1-value2 insert_order2`,
		`1 3 1 key1-value3 insert_order3`,
		`1 4 1 key1-value4 insert_order4`,
		`1 5 1 key1-value5 insert_order5`,
		`1 6 1 key1-value6 insert_order6`,
		`2 1 2 key2-value1 insert_order1`,
		`2 2 2 key2-value2 insert_order2`,
		`2 3 2 key2-value3 insert_order3`,
		`2 4 2 key2-value4 insert_order4`,
		`2 5 2 key2-value5 insert_order5`,
		`2 6 2 key2-value6 insert_order6`,
		`3 1 3 key3-value1 insert_order1`,
		`3 2 3 key3-value2 insert_order2`,
		`3 3 3 key3-value3 insert_order3`,
		`3 4 3 key3-value4 insert_order4`,
		`3 5 3 key3-value5 insert_order5`,
		`3 6 3 key3-value6 insert_order6`,
	))
}

func (s *testIntegrationSuite) TestCastAsTime(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (col1 bigint, col2 double, col3 decimal, col4 varchar(20), col5 json);`)
	tk.MustExec(`insert into t values (1, 1, 1, "1", "1");`)
	tk.MustExec(`insert into t values (null, null, null, null, null);`)
	tk.MustQuery(`select cast(col1 as time), cast(col2 as time), cast(col3 as time), cast(col4 as time), cast(col5 as time) from t where col1 = 1;`).Check(testkit.Rows(
		`00:00:01 00:00:01 00:00:01 00:00:01 00:00:01`,
	))
	tk.MustQuery(`select cast(col1 as time), cast(col2 as time), cast(col3 as time), cast(col4 as time), cast(col5 as time) from t where col1 is null;`).Check(testkit.Rows(
		`<nil> <nil> <nil> <nil> <nil>`,
	))

	err := tk.ExecToErr(`select cast(col1 as time(31)) from t where col1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(col2 as time(31)) from t where col1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(col3 as time(31)) from t where col1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(col4 as time(31)) from t where col1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")

	err = tk.ExecToErr(`select cast(col5 as time(31)) from t where col1 is null;`)
	c.Assert(err.Error(), Equals, "[expression:1426]Too big precision 31 specified for column 'CAST'. Maximum is 6.")
}

func (s *testIntegrationSuite) TestValuesFloat32(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (i int key, j float);`)
	tk.MustExec(`insert into t values (1, 0.01);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 0.01`))
	tk.MustExec(`insert into t values (1, 0.02) on duplicate key update j = values (j);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 0.02`))
}

func (s *testIntegrationSuite) TestFuncNameConst(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	defer s.cleanEnv(c)
	tk.MustExec("USE test;")
	tk.MustExec("DROP TABLE IF EXISTS t;")
	tk.MustExec("CREATE TABLE t(a CHAR(20), b VARCHAR(20), c BIGINT);")
	tk.MustExec("INSERT INTO t (b, c) values('hello', 1);")

	r := tk.MustQuery("SELECT name_const('test_int', 1), name_const('test_float', 3.1415);")
	r.Check(testkit.Rows("1 3.1415"))
	r = tk.MustQuery("SELECT name_const('test_string', 'hello'), name_const('test_nil', null);")
	r.Check(testkit.Rows("hello <nil>"))
	r = tk.MustQuery("SELECT name_const('test_string', 1) + c FROM t;")
	r.Check(testkit.Rows("2"))
	r = tk.MustQuery("SELECT concat('hello', name_const('test_string', 'world')) FROM t;")
	r.Check(testkit.Rows("helloworld"))
	err := tk.ExecToErr(`select name_const(a,b) from t;`)
	c.Assert(err.Error(), Equals, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const(a,"hello") from t;`)
	c.Assert(err.Error(), Equals, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const("hello", b) from t;`)
	c.Assert(err.Error(), Equals, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const("hello", 1+1) from t;`)
	c.Assert(err.Error(), Equals, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const(concat('a', 'b'), 555) from t;`)
	c.Assert(err.Error(), Equals, "[planner:1210]Incorrect arguments to NAME_CONST")
	err = tk.ExecToErr(`select name_const(555) from t;`)
	c.Assert(err.Error(), Equals, "[expression:1582]Incorrect parameter count in the call to native function 'name_const'")

	var rs sqlexec.RecordSet
	rs, err = tk.Exec(`select name_const("hello", 1);`)
	c.Assert(err, IsNil)
	c.Assert(len(rs.Fields()), Equals, 1)
	c.Assert(rs.Fields()[0].Column.Name.L, Equals, "hello")
}

func (s *testIntegrationSuite) TestValuesEnum(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t (a bigint primary key, b enum('a','b','c'));`)
	tk.MustExec(`insert into t values (1, "a");`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 a`))
	tk.MustExec(`insert into t values (1, "b") on duplicate key update b = values(b);`)
	tk.MustQuery(`select * from t;`).Check(testkit.Rows(`1 b`))
}

func (s *testIntegrationSuite) TestIssue9325(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a timestamp) partition by range(unix_timestamp(a)) (partition p0 values less than(unix_timestamp('2019-02-16 14:20:00')), partition p1 values less than (maxvalue))")
	tk.MustExec("insert into t values('2019-02-16 14:19:59'), ('2019-02-16 14:20:01')")
	result := tk.MustQuery("select * from t where a between timestamp'2019-02-16 14:19:00' and timestamp'2019-02-16 14:21:00'")
	c.Assert(result.Rows(), HasLen, 2)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a timestamp)")
	tk.MustExec("insert into t values('2019-02-16 14:19:59'), ('2019-02-16 14:20:01')")
	result = tk.MustQuery("select * from t where a < timestamp'2019-02-16 14:21:00'")
	result.Check(testkit.Rows("2019-02-16 14:19:59", "2019-02-16 14:20:01"))
}
