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

package core_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testExpressionRewriterSuite{})

type testExpressionRewriterSuite struct {
}

func (s *testExpressionRewriterSuite) TestIfNullEliminateColName(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int not null)")
	rs, err := tk.Exec("select ifnull(a,b) from t")
	c.Assert(err, IsNil)
	fields := rs.Fields()
	c.Assert(fields[0].Column.Name.L, Equals, "ifnull(a,b)")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(e int not null, b int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("create table t1(e int not null, b int)")
	tk.MustExec("insert into t1 values(1, 1)")
	rows := tk.MustQuery("select b from t where ifnull(e, b)")
	rows.Check(testkit.Rows("1"))
	rows = tk.MustQuery("select b from t1 where ifnull(e, b)")
	rows.Check(testkit.Rows("1"))
}

func (s *testExpressionRewriterSuite) TestBinaryOpFunction(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t(a int, b int, c int);")
	tk.MustExec("INSERT INTO t VALUES (1, 2, 3), (NULL, 2, 3  ), (1, NULL, 3),(1, 2,   NULL),(NULL, 2, 3+1), (1, NULL, 3+1), (1, 2+1, NULL),(NULL, 2, 3-1), (1, NULL, 3-1), (1, 2-1, NULL)")
	tk.MustQuery("SELECT * FROM t WHERE (a,b,c) <= (1,2,3) order by b").Check(testkit.Rows("1 1 <nil>", "1 2 3"))
	tk.MustQuery("SELECT * FROM t WHERE (a,b,c) > (1,2,3) order by b").Check(testkit.Rows("1 3 <nil>"))
}

func (s *testExpressionRewriterSuite) TestDefaultFunction(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec(`create table t1(
		a varchar(10) default 'def',
		b varchar(10),
		c int default '10',
		d double default '3.14',
		e datetime default '20180101',
		f datetime default current_timestamp);`)
	tk.MustExec("insert into t1(a, b, c, d) values ('1', '1', 1, 1)")
	tk.MustQuery(`select
		default(a) as defa,
		default(b) as defb,
		default(c) as defc,
		default(d) as defd,
		default(e) as defe,
		default(f) as deff
		from t1`).Check(testutil.RowsWithSep("|", "def|<nil>|10|3.14|2018-01-01 00:00:00|<nil>"))
	err = tk.ExecToErr("select default(x) from t1")
	c.Assert(err.Error(), Equals, "[planner:1054]Unknown column 'x' in 'field list'")

	tk.MustQuery("select default(a0) from (select a as a0 from t1) as t0").Check(testkit.Rows("def"))
	err = tk.ExecToErr("select default(a0) from (select a+1 as a0 from t1) as t0")
	c.Assert(err.Error(), Equals, "[table:1364]Field 'a0' doesn't have a default value")

	tk.MustExec("create table t2(a varchar(10), b varchar(10))")
	tk.MustExec("insert into t2 values ('1', '1')")
	err = tk.ExecToErr("select default(a) from t1, t2")
	c.Assert(err.Error(), Equals, "[expression:1052]Column 'a' in field list is ambiguous")
	tk.MustQuery("select default(t1.a) from t1, t2").Check(testkit.Rows("def"))

	tk.MustExec(`create table t3(
		a datetime default current_timestamp,
		b timestamp default current_timestamp,
		c timestamp(6) default current_timestamp(6),
		d varchar(20) default 'current_timestamp')`)
	tk.MustExec("insert into t3 values ()")
	tk.MustQuery(`select
		default(a) as defa,
		default(b) as defb,
		default(c) as defc,
		default(d) as defd
		from t3`).Check(testutil.RowsWithSep("|", "<nil>|0000-00-00 00:00:00|0000-00-00 00:00:00.000000|current_timestamp"))

	tk.MustExec(`create table t4(a int default 1, b varchar(5))`)
	tk.MustExec(`insert into t4 values (0, 'B'), (1, 'B'), (2, 'B')`)
	tk.MustExec(`create table t5(d int default 0, e varchar(5))`)
	tk.MustExec(`insert into t5 values (5, 'B')`)

	tk.MustQuery(`select a from t4 where a > (select default(d) from t5 where t4.b = t5.e)`).Check(testkit.Rows("1", "2"))
	tk.MustQuery(`select a from t4 where a > (select default(a) from t5 where t4.b = t5.e)`).Check(testkit.Rows("2"))

	tk.MustExec("prepare stmt from 'select default(a) from t1';")
	tk.MustQuery("execute stmt").Check(testkit.Rows("def"))
	tk.MustExec("alter table t1 modify a varchar(10) default 'DEF'")
	tk.MustQuery("execute stmt").Check(testkit.Rows("DEF"))

	tk.MustExec("update t1 set c = c + default(c)")
	tk.MustQuery("select c from t1").Check(testkit.Rows("11"))

	tk.MustExec("create table t6(a int default -1, b int)")
	tk.MustExec(`insert into t6 values (0, 0), (1, 1), (2, 2)`)
	tk.MustExec("create table t7(a int default 1, b int)")
	tk.MustExec(`insert into t7 values (0, 0), (1, 1), (2, 2)`)

	tk.MustQuery(`select a from t6 where a > (select default(a) from t7 where t6.a = t7.a)`).Check(testkit.Rows("2"))
	tk.MustQuery(`select a, default(a) from t6 where a > (select default(a) from t7 where t6.a = t7.a)`).Check(testkit.Rows("2 -1"))

	tk.MustExec("create table t8(a int default 1, b int default -1)")
	tk.MustExec(`insert into t8 values (0, 0), (1, 1)`)

	tk.MustQuery(`select a, a from t8 order by default(a)`).Check(testkit.Rows("0 0", "1 1"))
	tk.MustQuery(`select a from t8 order by default(b)`).Check(testkit.Rows("0", "1"))
	tk.MustQuery(`select a from t8 order by default(b) * a`).Check(testkit.Rows("1", "0"))
}

func (s *testExpressionRewriterSuite) TestCompareSubquery(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists s")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("create table s(a int, b int)")
	tk.MustExec("insert into t values(1, null), (2, null)")

	// Test empty checker.
	tk.MustQuery("select a != any (select a from s) from t").Check(testkit.Rows(
		"0",
		"0",
	))
	tk.MustQuery("select b != any (select a from s) from t").Check(testkit.Rows(
		"0",
		"0",
	))
	tk.MustQuery("select a = all (select a from s) from t").Check(testkit.Rows(
		"1",
		"1",
	))
	tk.MustQuery("select b = all (select a from s) from t").Check(testkit.Rows(
		"1",
		"1",
	))
	tk.MustQuery("select * from t where a != any (select a from s)").Check(testkit.Rows())
	tk.MustQuery("select * from t where b != any (select a from s)").Check(testkit.Rows())
	tk.MustQuery("select * from t where a = all (select a from s)").Check(testkit.Rows(
		"1 <nil>",
		"2 <nil>",
	))
	tk.MustQuery("select * from t where b = all (select a from s)").Check(testkit.Rows(
		"1 <nil>",
		"2 <nil>",
	))
	// Test outer null checker.
	tk.MustQuery("select b != any (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
	tk.MustQuery("select b = all (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
	tk.MustQuery("select * from t t1 where b != any (select a from t t2)").Check(testkit.Rows())
	tk.MustQuery("select * from t t1 where b = all (select a from t t2)").Check(testkit.Rows())

	tk.MustExec("delete from t where a = 2")
	tk.MustQuery("select b != any (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
	))
	tk.MustQuery("select b = all (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
	))
	tk.MustQuery("select * from t t1 where b != any (select a from t t2)").Check(testkit.Rows())
	tk.MustQuery("select * from t t1 where b = all (select a from t t2)").Check(testkit.Rows())

	// Test inner null checker.
	tk.MustExec("insert into t values(null, 1)")
	tk.MustQuery("select b != any (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
	tk.MustQuery("select b = all (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"<nil>",
	))
	tk.MustQuery("select * from t t1 where b != any (select a from t t2)").Check(testkit.Rows())
	tk.MustQuery("select * from t t1 where b = all (select a from t t2)").Check(testkit.Rows())

	tk.MustExec("delete from t where b = 1")
	tk.MustExec("insert into t values(null, 2)")
	tk.MustQuery("select b != any (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"1",
	))
	tk.MustQuery("select b = all (select a from t t2) from t t1").Check(testkit.Rows(
		"<nil>",
		"0",
	))
	tk.MustQuery("select * from t t1 where b != any (select a from t t2)").Check(testkit.Rows(
		"<nil> 2",
	))
	tk.MustQuery("select * from t t1 where b = all (select a from t t2)").Check(testkit.Rows())

	// for issue 20059
	tk.MustExec("DROP TABLE IF EXISTS `t`")
	tk.MustExec("CREATE TABLE `t` (  `a` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("INSERT INTO `t` VALUES (1);")
	tk.MustExec("DROP TABLE IF EXISTS `table_40_utf8_4`;")
	tk.MustExec("CREATE TABLE `table_40_utf8_4` (`col_tinyint_key_unsigned` tinyint(4) DEFAULT NULL,  `col_bit64_key_signed` bit(64) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;")
	tk.MustExec("INSERT INTO `table_40_utf8_4` VALUES (31, -18);")
	tk.MustQuery("select count(1) from table_40_utf8_4 where ( select count(1) from t where table_40_utf8_4.col_bit64_key_signed!=table_40_utf8_4.col_tinyint_key_unsigned)").Check(testkit.Rows("1"))
}

func (s *testExpressionRewriterSuite) TestCheckFullGroupBy(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustQuery("select t1.a, (select max(t2.b) from t t2) from t t1").Check(testkit.Rows())
	err = tk.ExecToErr("select t1.a, (select t2.a, max(t2.b) from t t2) from t t1")
	c.Assert(terror.ErrorEqual(err, core.ErrMixOfGroupFuncAndFields), IsTrue, Commentf("err %v", err))
}

func (s *testExpressionRewriterSuite) TestPatternLikeToExpression(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustQuery("select 0 like 'a string';").Check(testkit.Rows("0"))
	tk.MustQuery("select 0.0 like 'a string';").Check(testkit.Rows("0"))
	tk.MustQuery("select 0 like '0.00';").Check(testkit.Rows("0"))
	tk.MustQuery("select cast(\"2011-5-3\" as datetime) like \"2011-05-03\";").Check(testkit.Rows("0"))
	tk.MustQuery("select 1 like '1';").Check(testkit.Rows("1"))
	tk.MustQuery("select 0 like '0';").Check(testkit.Rows("1"))
	tk.MustQuery("select 0.00 like '0.00';").Check(testkit.Rows("1"))
}

func (s *testExpressionRewriterSuite) TestIssue20007(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (c_int int, c_str varchar(40), c_datetime datetime, primary key(c_int));")
	tk.MustExec("create table t2 (c_int int, c_str varchar(40), c_datetime datetime, primary key (c_datetime)) partition by range (to_days(c_datetime)) ( partition p0 values less than (to_days('2020-02-01')), partition p1 values less than (to_days('2020-04-01')), partition p2 values less than (to_days('2020-06-01')), partition p3 values less than maxvalue);")
	tk.MustExec("insert into t1 (c_int, c_str, c_datetime) values (1, 'xenodochial bassi', '2020-04-29 03:22:51'), (2, 'epic wiles', '2020-01-02 23:29:51'), (3, 'silly burnell', '2020-02-25 07:43:07');")
	tk.MustExec("insert into t2 (c_int, c_str, c_datetime) values (1, 'trusting matsumoto', '2020-01-07 00:57:18'), (2, 'pedantic boyd', '2020-06-08 23:12:16'), (null, 'strange hypatia', '2020-05-23 17:45:27');")
	// Test 10 times.
	for i := 0; i < 10; i++ {
		tk.MustQuery("select * from t1 where c_int != any (select c_int from t2 where t1.c_str <= t2.c_str); ").Check(
			testkit.Rows("2 epic wiles 2020-01-02 23:29:51", "3 silly burnell 2020-02-25 07:43:07"))
	}
}

func (s *testExpressionRewriterSuite) TestIssue9869(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a int, b bigint unsigned);")
	tk.MustExec("insert into t1 (a, b) values (1,4572794622775114594), (2,18196094287899841997),(3,11120436154190595086);")
	tk.MustQuery("select (case t1.a when 0 then 0 else t1.b end), cast(t1.b as signed)  from t1;").Check(
		testkit.Rows("4572794622775114594 4572794622775114594", "18196094287899841997 -250649785809709619", "11120436154190595086 -7326307919518956530"))
}

func (s *testExpressionRewriterSuite) TestIssue17652(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(x bigint unsigned);")
	tk.MustExec("insert into t values( 9999999703771440633);")
	tk.MustQuery("select ifnull(max(x), 0) from t").Check(
		testkit.Rows("9999999703771440633"))
}

func (s *testExpressionRewriterSuite) TestCompareMultiFieldsInSubquery(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1, t2, t3, t4;")
	tk.MustExec("CREATE TABLE t1(c1 int, c2 int);")
	tk.MustExec("CREATE TABLE t2(c1 int, c2 int);")
	tk.MustExec("CREATE TABLE t3(c1 int, c2 int);")
	tk.MustExec("CREATE TABLE t4(c1 int, c2 int);")
	tk.MustExec("INSERT INTO t1 VALUES (0, 0), (NULL, NULL);")
	tk.MustExec("INSERT INTO t2 VALUES (0, 0), (NULL, NULL);")
	tk.MustExec("INSERT INTO t3 VALUES (1, 2);")
	// issue #13551 and #21674
	tk.MustQuery("SELECT * FROM t2 WHERE (SELECT c1, c2 FROM t2 LIMIT 1) = ANY (SELECT c1, c2 FROM t1);").Check(testkit.Rows("0 0", "<nil> <nil>"))
	tk.MustQuery("SELECT * FROM t2 WHERE (SELECT c1 FROM t2 LIMIT 1) = ANY (SELECT c1 FROM t1);").Check(testkit.Rows("0 0", "<nil> <nil>"))
	tk.MustQuery("SELECT * FROM t2 WHERE (SELECT c1, c2 FROM t2 order by c1 LIMIT 1) = ANY (SELECT c1, c2 FROM t1);").Check(testkit.Rows())

	tk.MustQuery("SELECT * FROM t3 WHERE (SELECT c1 FROM t3 LIMIT 1) != ALL(SELECT c1 FROM t4);").Check(testkit.Rows("1 2"))
	tk.MustQuery("SELECT * FROM t3 WHERE (SELECT c1, c2 FROM t3 LIMIT 1) != ALL(SELECT c1, c2 FROM t4);").Check(testkit.Rows("1 2"))
	tk.MustExec("INSERT INTO t4 VALUES (1, 3);")
	tk.MustQuery("SELECT * FROM t3 WHERE (SELECT c1 FROM t3 LIMIT 1) != ALL(SELECT c1 FROM t4);").Check(testkit.Rows())
	tk.MustQuery("SELECT * FROM t3 WHERE (SELECT c1, c2 FROM t3 LIMIT 1) != ALL(SELECT c1, c2 FROM t4);").Check(testkit.Rows("1 2"))
	tk.MustExec("INSERT INTO t4 VALUES (1, 2);")
	tk.MustQuery("SELECT * FROM t3 WHERE (SELECT c1 FROM t3 LIMIT 1) != ALL(SELECT c1 FROM t4);").Check(testkit.Rows())
	tk.MustQuery("SELECT * FROM t3 WHERE (SELECT c1, c2 FROM t3 LIMIT 1) != ALL(SELECT c1, c2 FROM t4);").Check(testkit.Rows())

}

func (s *testExpressionRewriterSuite) TestIssue22818(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a time);")
	tk.MustExec("insert into t values(\"23:22:22\");")
	tk.MustQuery("select * from t where a between \"23:22:22\" and \"23:22:22\"").Check(
		testkit.Rows("23:22:22"))
}

func (s *testExpressionRewriterSuite) TestIssue24705(c *C) {
	defer testleak.AfterTest(c)()
	store, dom, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)
	defer func() {
		dom.Close()
		store.Close()
	}()

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1,t2;")
	tk.MustExec("create table t1 (c_int int, c_str varchar(40) character set utf8 collate utf8_general_ci);")
	tk.MustExec("create table t2 (c_int int, c_str varchar(40) character set utf8 collate utf8_unicode_ci);")
	err = tk.ExecToErr("select * from t1 where c_str < any (select c_str from t2 where c_int between 6 and 9);")
	c.Assert(err.Error(), Equals, "[expression:1267]Illegal mix of collations (utf8_general_ci,IMPLICIT) and (utf8_unicode_ci,IMPLICIT) for operation '<'")
}
