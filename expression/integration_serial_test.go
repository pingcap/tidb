// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package expression_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestIssue17727(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	tk.SetSession(se)
	require.NoError(t, err)

	tk.MustExec("use test;")
	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec("CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY auto_increment, a timestamp NOT NULL);")
	tk.MustExec("INSERT INTO t1 VALUES (null, '2020-05-30 20:30:00');")
	tk.MustExec("PREPARE mystmt FROM 'SELECT * FROM t1 WHERE UNIX_TIMESTAMP(a) >= ?';")
	tk.MustExec("SET @a=1590868800;")
	tk.MustQuery("EXECUTE mystmt USING @a;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec("SET @a=1590868801;")
	tk.MustQuery("EXECUTE mystmt USING @a;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt from 'select unix_timestamp(?)';")
	tk.MustExec("set @a = '2020-05-30 20:30:00';")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1590841800"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))

	tk.MustExec("set @a = '2020-06-12 13:47:58';")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1591940878"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
}

func TestIssue17891(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, value set ('a','b','c') charset utf8mb4 collate utf8mb4_bin default 'a,b ');")
	tk.MustExec("drop table t")
	tk.MustExec("create table test(id int, value set ('a','b','c') charset utf8mb4 collate utf8mb4_general_ci default 'a,B ,C');")
}

func TestIssue31174(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(4) collate utf8_general_ci primary key /*T![clustered_index] clustered */);")
	tk.MustExec("insert into t values('`?');")
	// The 'like' condition can not be used to construct the range.
	tk.HasPlan("select * from t where a like '`%';", "TableFullScan")
	tk.MustQuery("select * from t where a like '`%';").Check(testkit.Rows("`?"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(4) collate binary primary key /*T![clustered_index] clustered */);")
	tk.MustExec("insert into t values('`?');")
	tk.HasPlan("select * from t where a like '`%';", "TableRangeScan")
	tk.MustQuery("select * from t where a like '`%';").Check(testkit.Rows("`?\x00\x00"))
}

func TestIssue20268(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (   `a` enum('a','b') DEFAULT NULL ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;")
	tk.MustExec("insert into t values('a');")
	tk.MustExec("select * from t where a = 'A';")
}

func TestCollationBasic(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_ci(a varchar(10) collate utf8mb4_general_ci, unique key(a))")
	tk.MustExec("insert into t_ci values ('a')")
	tk.MustQuery("select * from t_ci").Check(testkit.Rows("a"))
	tk.MustQuery("select * from t_ci").Check(testkit.Rows("a"))
	tk.MustQuery("select * from t_ci where a='a'").Check(testkit.Rows("a"))
	tk.MustQuery("select * from t_ci where a='A'").Check(testkit.Rows("a"))
	tk.MustQuery("select * from t_ci where a='a   '").Check(testkit.Rows("a"))
	tk.MustQuery("select * from t_ci where a='a                    '").Check(testkit.Rows("a"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) primary key,b int)")
	tk.MustExec("insert into t values ('a', 1), ('b', 3), ('a', 2) on duplicate key update b = b + 1;")
	tk.MustExec("set autocommit=0")
	tk.MustExec("insert into t values ('a', 1), ('b', 3), ('a', 2) on duplicate key update b = b + 1;")
	tk.MustQuery("select * from t").Check(testkit.Rows("a 4", "b 4"))
	tk.MustExec("set autocommit=1")
	tk.MustQuery("select * from t").Check(testkit.Rows("a 4", "b 4"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10),b int, key tk (a))")
	tk.MustExec("insert into t values ('', 1), ('', 3)")
	tk.MustExec("set autocommit=0")
	tk.MustExec("update t set b = b + 1")
	tk.MustQuery("select * from t").Check(testkit.Rows(" 2", " 4"))
	tk.MustExec("set autocommit=1")
	tk.MustQuery("select * from t").Check(testkit.Rows(" 2", " 4"))

	tk.MustExec("drop table t_ci")
	tk.MustExec("create table t_ci(id bigint primary key, a varchar(10) collate utf8mb4_general_ci, unique key(a, id))")
	tk.MustExec("insert into t_ci values (1, 'a')")
	tk.MustQuery("select a from t_ci").Check(testkit.Rows("a"))
	tk.MustQuery("select a from t_ci").Check(testkit.Rows("a"))
	tk.MustQuery("select a from t_ci where a='a'").Check(testkit.Rows("a"))
	tk.MustQuery("select a from t_ci where a='A'").Check(testkit.Rows("a"))
	tk.MustQuery("select a from t_ci where a='a   '").Check(testkit.Rows("a"))
	tk.MustQuery("select a from t_ci where a='a                    '").Check(testkit.Rows("a"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c set('A', 'B') collate utf8mb4_general_ci);")
	tk.MustExec("insert into t values('a');")
	tk.MustExec("insert into t values('B');")
	tk.MustQuery("select c from t where c = 'a';").Check(testkit.Rows("A"))
	tk.MustQuery("select c from t where c = 'A';").Check(testkit.Rows("A"))
	tk.MustQuery("select c from t where c = 'b';").Check(testkit.Rows("B"))
	tk.MustQuery("select c from t where c = 'B';").Check(testkit.Rows("B"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE `t1` (" +
		"  `COL1` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL," +
		"  PRIMARY KEY (`COL1`(5)) clustered" +
		")")
	tk.MustExec("INSERT INTO `t1` VALUES ('Ȇ');")
	tk.MustQuery("select * from t1 where col1 not in (0xc484, 0xe5a4bc, 0xc3b3);").Check(testkit.Rows("Ȇ"))
	tk.MustQuery("select * from t1 where col1 >= 0xc484 and col1 <= 0xc3b3;").Check(testkit.Rows("Ȇ"))

	tk.MustQuery("select collation(IF('a' < 'B' collate utf8mb4_general_ci, 'smaller', 'greater' collate utf8mb4_unicode_ci));").Check(testkit.Rows("utf8mb4_unicode_ci"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10))")
	tk.MustExec("insert into t values ('a')")
	tk.MustQuery("select * from t where a in ('b' collate utf8mb4_general_ci, 'A', 3)").Check(testkit.Rows("a"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(`COL2` tinyint(16) DEFAULT NULL);")
	tk.MustExec("insert into t values(0);")
	tk.MustQuery("select * from t WHERE COL2 IN (0xfc);").Check(testkit.Rows())
	tk.MustQuery("select * from t WHERE COL2 = 0xfc;").Check(testkit.Rows())
}

func TestWeightString(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	type testCase struct {
		input                    []string
		result                   []string
		resultAsChar1            []string
		resultAsChar3            []string
		resultAsBinary1          []string
		resultAsBinary5          []string
		resultExplicitCollateBin []string
	}
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, a varchar(20) collate utf8mb4_general_ci)")
	cases := testCase{
		input:                    []string{"aAÁàãăâ", "a", "a  ", "中", "中 "},
		result:                   []string{"\x00A\x00A\x00A\x00A\x00A\x00A\x00A", "\x00A", "\x00A", "\x4E\x2D", "\x4E\x2D"},
		resultAsChar1:            []string{"\x00A", "\x00A", "\x00A", "\x4E\x2D", "\x4E\x2D"},
		resultAsChar3:            []string{"\x00A\x00A\x00A", "\x00A", "\x00A", "\x4E\x2D", "\x4E\x2D"},
		resultAsBinary1:          []string{"a", "a", "a", "\xE4", "\xE4"},
		resultAsBinary5:          []string{"aA\xc3\x81\xc3", "a\x00\x00\x00\x00", "a  \x00\x00", "中\x00\x00", "中 \x00"},
		resultExplicitCollateBin: []string{"aAÁàãăâ", "a", "a", "中", "中"},
	}
	values := make([]string, len(cases.input))
	for i, input := range cases.input {
		values[i] = fmt.Sprintf("(%d, '%s')", i, input)
	}
	tk.MustExec("insert into t values " + strings.Join(values, ","))
	rows := tk.MustQuery("select weight_string(a) from t order by id").Rows()
	for i, out := range cases.result {
		require.Equal(t, out, rows[i][0].(string))
	}
	rows = tk.MustQuery("select weight_string(a as char(1)) from t order by id").Rows()
	for i, out := range cases.resultAsChar1 {
		require.Equal(t, out, rows[i][0].(string))
	}
	rows = tk.MustQuery("select weight_string(a as char(3)) from t order by id").Rows()
	for i, out := range cases.resultAsChar3 {
		require.Equal(t, out, rows[i][0].(string))
	}
	rows = tk.MustQuery("select weight_string(a as binary(1)) from t order by id").Rows()
	for i, out := range cases.resultAsBinary1 {
		require.Equal(t, out, rows[i][0].(string))
	}
	rows = tk.MustQuery("select weight_string(a as binary(5)) from t order by id").Rows()
	for i, out := range cases.resultAsBinary5 {
		require.Equal(t, out, rows[i][0].(string))
	}
	require.Equal(t, "<nil>", tk.MustQuery("select weight_string(NULL);").Rows()[0][0])
	require.Equal(t, "<nil>", tk.MustQuery("select weight_string(7);").Rows()[0][0])
	require.Equal(t, "<nil>", tk.MustQuery("select weight_string(cast(7 as decimal(5)));").Rows()[0][0])
	require.Equal(t, "2019-08-21", tk.MustQuery("select weight_string(cast(20190821 as date));").Rows()[0][0])
	require.Equal(t, "2019-", tk.MustQuery("select weight_string(cast(20190821 as date) as binary(5));").Rows()[0][0])
	require.Equal(t, "<nil>", tk.MustQuery("select weight_string(7.0);").Rows()[0][0])
	require.Equal(t, "7\x00", tk.MustQuery("select weight_string(7 AS BINARY(2));").Rows()[0][0])
	// test explicit collation
	require.Equal(t, "\x4E\x2D", tk.MustQuery("select weight_string('中 ' collate utf8mb4_general_ci);").Rows()[0][0])
	require.Equal(t, "中", tk.MustQuery("select weight_string('中 ' collate utf8mb4_bin);").Rows()[0][0])
	require.Equal(t, "\xFB\x40\xCE\x2D", tk.MustQuery("select weight_string('中 ' collate utf8mb4_unicode_ci);").Rows()[0][0])
	require.Equal(t, "utf8mb4_general_ci", tk.MustQuery("select collation(a collate utf8mb4_general_ci) from t order by id").Rows()[0][0])
	require.Equal(t, "utf8mb4_general_ci", tk.MustQuery("select collation('中 ' collate utf8mb4_general_ci);").Rows()[0][0])
	rows = tk.MustQuery("select weight_string(a collate utf8mb4_bin) from t order by id").Rows()
	for i, out := range cases.resultExplicitCollateBin {
		require.Equal(t, out, rows[i][0].(string))
	}
	tk.MustGetErrMsg("select weight_string(a collate utf8_general_ci) from t order by id", "[ddl:1253]COLLATION 'utf8_general_ci' is not valid for CHARACTER SET 'utf8mb4'")
	tk.MustGetErrMsg("select weight_string('中' collate utf8_bin)", "[ddl:1253]COLLATION 'utf8_bin' is not valid for CHARACTER SET 'utf8mb4'")
}

func TestCollationCreateIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) collate utf8mb4_general_ci);")
	tk.MustExec("insert into t values ('a');")
	tk.MustExec("insert into t values ('A');")
	tk.MustExec("insert into t values ('b');")
	tk.MustExec("insert into t values ('B');")
	tk.MustExec("insert into t values ('a');")
	tk.MustExec("insert into t values ('A');")
	tk.MustExec("insert into t values ('ß');")
	tk.MustExec("insert into t values ('sa');")
	tk.MustExec("create index idx on t(a);")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("a", "A", "a", "A", "b", "B", "ß", "sa"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) collate utf8mb4_unicode_ci);")
	tk.MustExec("insert into t values ('a');")
	tk.MustExec("insert into t values ('A');")
	tk.MustExec("insert into t values ('b');")
	tk.MustExec("insert into t values ('B');")
	tk.MustExec("insert into t values ('a');")
	tk.MustExec("insert into t values ('A');")
	tk.MustExec("insert into t values ('ß');")
	tk.MustExec("insert into t values ('sa');")
	tk.MustExec("create index idx on t(a);")
	tk.MustQuery("select * from t order by a").Check(testkit.Rows("a", "A", "a", "A", "b", "B", "sa", "ß"))
}

func TestCollateConstantPropagation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a char(10) collate utf8mb4_bin, b char(10) collate utf8mb4_general_ci);")
	tk.MustExec("insert into t values ('a', 'A');")
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b='a' collate utf8mb4_general_ci;").Check(nil)
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b>='a' collate utf8mb4_general_ci;").Check(nil)
	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a char(10) collate utf8mb4_general_ci, b char(10) collate utf8mb4_general_ci);")
	tk.MustExec("insert into t values ('A', 'a');")
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b='a' collate utf8mb4_bin;").Check(testkit.Rows("A a A a"))
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b>='a' collate utf8mb4_bin;").Check(testkit.Rows("A a A a"))
	tk.MustExec("drop table t;")
	tk.MustExec("set names utf8mb4")
	tk.MustExec("create table t (a char(10) collate utf8mb4_general_ci, b char(10) collate utf8_general_ci);")
	tk.MustExec("insert into t values ('a', 'A');")
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b='A'").Check(testkit.Rows("a A a A"))
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a char collate utf8_general_ci, b char collate utf8mb4_general_ci, c char collate utf8_bin);")
	tk.MustExec("insert into t values ('b', 'B', 'B');")
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b=t2.c;").Check(testkit.Rows("b B B b B B"))
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a char collate utf8_bin, b char collate utf8_general_ci);")
	tk.MustExec("insert into t values ('a', 'A');")
	tk.MustQuery("select * from t t1, t t2 where t1.b=t2.b and t2.b=t1.a collate utf8_general_ci;").Check(testkit.Rows("a A a A"))
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("set names utf8mb4 collate utf8mb4_general_ci;")
	tk.MustExec("create table t1(a char, b varchar(10)) charset utf8mb4 collate utf8mb4_general_ci;")
	tk.MustExec("create table t2(a char, b varchar(10)) charset utf8mb4 collate utf8mb4_bin;")
	tk.MustExec("insert into t1 values ('A', 'a');")
	tk.MustExec("insert into t2 values ('a', 'a')")
	tk.MustQuery("select * from t1 left join t2 on t1.a = t2.a where t1.a = 'a';").Check(testkit.Rows("A a <nil> <nil>"))
	tk.MustExec("drop table t;")
	tk.MustExec("set names utf8mb4 collate utf8mb4_general_ci;")
	tk.MustExec("create table t(a char collate utf8mb4_bin, b char collate utf8mb4_general_ci);")
	tk.MustExec("insert into t values ('a', 'a');")
	tk.MustQuery("select * from t t1, t t2 where  t2.b = 'A' and lower(concat(t1.a , '' ))  = t2.b;").Check(testkit.Rows("a a a a"))
	tk.MustExec("drop table t;")
	tk.MustExec("create table t(a char collate utf8_unicode_ci, b char collate utf8mb4_unicode_ci, c char collate utf8_bin);")
	tk.MustExec("insert into t values ('b', 'B', 'B');")
	tk.MustQuery("select * from t t1, t t2 where t1.a=t2.b and t2.b=t2.c;").Check(testkit.Rows("b B B b B B"))
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("set names utf8mb4 collate utf8mb4_unicode_ci;")
	tk.MustExec("create table t1(a char, b varchar(10)) charset utf8mb4 collate utf8mb4_unicode_ci;")
	tk.MustExec("create table t2(a char, b varchar(10)) charset utf8mb4 collate utf8mb4_bin;")
	tk.MustExec("insert into t1 values ('A', 'a');")
	tk.MustExec("insert into t2 values ('a', 'a')")
	tk.MustQuery("select * from t1 left join t2 on t1.a = t2.a where t1.a = 'a';").Check(testkit.Rows("A a <nil> <nil>"))
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("set names utf8mb4 collate utf8mb4_general_ci;")
	tk.MustExec("create table t1(a char, b varchar(10)) charset utf8mb4 collate utf8mb4_general_ci;")
	tk.MustExec("create table t2(a char, b varchar(10)) charset utf8mb4 collate utf8mb4_unicode_ci;")
	tk.MustExec("insert into t1 values ('ß', 's');")
	tk.MustExec("insert into t2 values ('s', 's')")
	tk.MustQuery("select * from t1 left join t2 on t1.a = t2.a collate utf8mb4_unicode_ci where t1.a = 's';").Check(testkit.Rows("ß s <nil> <nil>"))
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(a char(10) collate utf8mb4_general_ci, index (a));")
	tk.MustExec("create table t2(a char(10) collate utf8_bin, index (a));")
	tk.MustExec("insert into t1 values ('a');")
	tk.MustExec("insert into t2 values ('A');")
	tk.MustExec("set names utf8 collate utf8_general_ci;")
	tk.MustQuery("select * from t1, t2 where t1.a=t2.a and t1.a= 'a';").Check(testkit.Rows("a A"))
	tk.MustQuery("select * from t1 where a='a' and a = 'A'").Check(testkit.Rows("a"))
}

func TestMixCollation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustGetErrMsg(`select 'a' collate utf8mb4_bin = 'a' collate utf8mb4_general_ci;`, "[expression:1267]Illegal mix of collations (utf8mb4_bin,EXPLICIT) and (utf8mb4_general_ci,EXPLICIT) for operation '='")

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`create table t (
			mb4general varchar(10) charset utf8mb4 collate utf8mb4_general_ci,
			mb4unicode varchar(10) charset utf8mb4 collate utf8mb4_unicode_ci,
			mb4bin     varchar(10) charset utf8mb4 collate utf8mb4_bin,
			general    varchar(10) charset utf8 collate utf8_general_ci,
			unicode    varchar(10) charset utf8 collate utf8_unicode_ci,
			utfbin     varchar(10) charset utf8 collate utf8_bin,
			bin        varchar(10) charset binary collate binary,
			latin1_bin varchar(10) charset latin1 collate latin1_bin,
			ascii_bin  varchar(10) charset ascii collate ascii_bin,
    		i          int
	);`)
	tk.MustExec("insert into t values ('s', 's', 's', 's', 's', 's', 's', 's', 's', 1);")
	tk.MustExec("set names utf8mb4 collate utf8mb4_general_ci;")

	tk.MustQuery("select * from t where mb4unicode = 's' collate utf8mb4_unicode_ci;").Check(testkit.Rows("s s s s s s s s s 1"))
	tk.MustQuery(`select * from t t1, t t2 where t1.mb4unicode = t2.mb4general collate utf8mb4_general_ci;`).Check(testkit.Rows("s s s s s s s s s 1 s s s s s s s s s 1"))
	tk.MustQuery(`select * from t t1, t t2 where t1.mb4general = t2.mb4unicode collate utf8mb4_general_ci;`).Check(testkit.Rows("s s s s s s s s s 1 s s s s s s s s s 1"))
	tk.MustQuery(`select * from t t1, t t2 where t1.mb4general = t2.mb4unicode collate utf8mb4_unicode_ci;`).Check(testkit.Rows("s s s s s s s s s 1 s s s s s s s s s 1"))
	tk.MustQuery(`select * from t t1, t t2 where t1.mb4unicode = t2.mb4general collate utf8mb4_unicode_ci;`).Check(testkit.Rows("s s s s s s s s s 1 s s s s s s s s s 1"))
	tk.MustQuery(`select * from t where mb4general = mb4bin collate utf8mb4_general_ci;`).Check(testkit.Rows("s s s s s s s s s 1"))
	tk.MustQuery(`select * from t where mb4unicode = mb4general collate utf8mb4_unicode_ci;`).Check(testkit.Rows("s s s s s s s s s 1"))
	tk.MustQuery(`select * from t where mb4general = mb4unicode collate utf8mb4_unicode_ci;`).Check(testkit.Rows("s s s s s s s s s 1"))
	tk.MustQuery(`select * from t where mb4unicode = 's' collate utf8mb4_unicode_ci;`).Check(testkit.Rows("s s s s s s s s s 1"))
	tk.MustQuery("select * from t where mb4unicode = mb4bin;").Check(testkit.Rows("s s s s s s s s s 1"))
	tk.MustQuery("select * from t where general = mb4unicode;").Check(testkit.Rows("s s s s s s s s s 1"))
	tk.MustQuery("select * from t where unicode = mb4unicode;").Check(testkit.Rows("s s s s s s s s s 1"))
	tk.MustQuery("select * from t where mb4unicode = mb4unicode;").Check(testkit.Rows("s s s s s s s s s 1"))

	tk.MustQuery("select collation(concat(mb4unicode, mb4general collate utf8mb4_unicode_ci)) from t;").Check(testkit.Rows("utf8mb4_unicode_ci"))
	tk.MustQuery("select collation(concat(mb4general, mb4unicode, mb4bin)) from t;").Check(testkit.Rows("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(mb4general, mb4unicode, mb4bin)) from t;").Check(testkit.Rows("1"))
	tk.MustQuery("select collation(concat(mb4unicode, mb4bin, concat(mb4general))) from t;").Check(testkit.Rows("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(mb4unicode, mb4bin)) from t;").Check(testkit.Rows("2"))
	tk.MustQuery("select collation(concat(mb4unicode, mb4bin)) from t;").Check(testkit.Rows("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(mb4bin, concat(mb4general))) from t;").Check(testkit.Rows("2"))
	tk.MustQuery("select collation(concaT(mb4bin, cOncAt(mb4general))) from t;").Check(testkit.Rows("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(mb4unicode, mb4bin, concat(mb4general))) from t;").Check(testkit.Rows("2"))
	tk.MustQuery("select collation(concat(mb4unicode, mb4bin, concat(mb4general))) from t;").Check(testkit.Rows("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(mb4unicode, mb4general)) from t;").Check(testkit.Rows("1"))
	tk.MustQuery("select collation(coalesce(mb4unicode, mb4general)) from t;").Check(testkit.Rows("utf8mb4_bin"))
	tk.MustQuery("select coercibility(coalesce(mb4unicode, mb4general)) from t;").Check(testkit.Rows("1"))
	tk.MustQuery("select collation(CONCAT(concat(mb4unicode), concat(mb4general))) from t;").Check(testkit.Rows("utf8mb4_bin"))
	tk.MustQuery("select coercibility(cONcat(unicode, general)) from t;").Check(testkit.Rows("1"))
	tk.MustQuery("select collation(concAt(unicode, general)) from t;").Check(testkit.Rows("utf8_bin"))
	tk.MustQuery("select collation(concat(bin, mb4general)) from t;").Check(testkit.Rows("binary"))
	tk.MustQuery("select coercibility(concat(bin, mb4general)) from t;").Check(testkit.Rows("2"))
	tk.MustQuery("select collation(concat(mb4unicode, ascii_bin)) from t;").Check(testkit.Rows("utf8mb4_unicode_ci"))
	tk.MustQuery("select coercibility(concat(mb4unicode, ascii_bin)) from t;").Check(testkit.Rows("2"))
	tk.MustQuery("select collation(concat(mb4unicode, mb4unicode)) from t;").Check(testkit.Rows("utf8mb4_unicode_ci"))
	tk.MustQuery("select coercibility(concat(mb4unicode, mb4unicode)) from t;").Check(testkit.Rows("2"))
	tk.MustQuery("select collation(concat(bin, bin)) from t;").Check(testkit.Rows("binary"))
	tk.MustQuery("select coercibility(concat(bin, bin)) from t;").Check(testkit.Rows("2"))
	tk.MustQuery("select collation(concat(latin1_bin, ascii_bin)) from t;").Check(testkit.Rows("latin1_bin"))
	tk.MustQuery("select coercibility(concat(latin1_bin, ascii_bin)) from t;").Check(testkit.Rows("2"))
	tk.MustQuery("select collation(concat(mb4unicode, bin)) from t;").Check(testkit.Rows("binary"))
	tk.MustQuery("select coercibility(concat(mb4unicode, bin)) from t;").Check(testkit.Rows("2"))
	tk.MustQuery("select collation(mb4general collate utf8mb4_unicode_ci) from t;").Check(testkit.Rows("utf8mb4_unicode_ci"))
	tk.MustQuery("select coercibility(mb4general collate utf8mb4_unicode_ci) from t;").Check(testkit.Rows("0"))
	tk.MustQuery("select collation(concat(concat(mb4unicode, mb4general), concat(unicode, general))) from t;").Check(testkit.Rows("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(concat(mb4unicode, mb4general), concat(unicode, general))) from t;").Check(testkit.Rows("1"))
	tk.MustQuery("select collation(concat(i, 1)) from t;").Check(testkit.Rows("utf8mb4_general_ci"))
	tk.MustQuery("select coercibility(concat(i, 1)) from t;").Check(testkit.Rows("4"))
	tk.MustQuery("select collation(concat(i, user())) from t;").Check(testkit.Rows("utf8mb4_bin"))
	tk.MustQuery("select coercibility(concat(i, user())) from t;").Check(testkit.Rows("3"))
	tk.MustGetErrMsg("select * from t where mb4unicode = mb4general;", "[expression:1267]Illegal mix of collations (utf8mb4_unicode_ci,IMPLICIT) and (utf8mb4_general_ci,IMPLICIT) for operation '='")
	tk.MustGetErrMsg("select * from t where unicode = general;", "[expression:1267]Illegal mix of collations (utf8_unicode_ci,IMPLICIT) and (utf8_general_ci,IMPLICIT) for operation '='")
	tk.MustGetErrMsg("select concat(mb4general) = concat(mb4unicode) from t;", "[expression:1267]Illegal mix of collations (utf8mb4_general_ci,IMPLICIT) and (utf8mb4_unicode_ci,IMPLICIT) for operation '='")
	tk.MustGetErrMsg("select * from t t1, t t2 where t1.mb4unicode = t2.mb4general;", "[expression:1267]Illegal mix of collations (utf8mb4_unicode_ci,IMPLICIT) and (utf8mb4_general_ci,IMPLICIT) for operation '='")
	tk.MustGetErrMsg("select field('s', mb4general, mb4unicode, mb4bin) from t;", "[expression:1271]Illegal mix of collations for operation 'field'")
	tk.MustGetErrMsg("select concat(mb4unicode, mb4general) = mb4unicode from t;", "[expression:1267]Illegal mix of collations (utf8mb4_bin,NONE) and (utf8mb4_unicode_ci,IMPLICIT) for operation '='")

	tk.MustExec("drop table t;")
}

func prepare4Join(tk *testkit.TestKit) {
	tk.MustExec("USE test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t_bin")
	tk.MustExec("CREATE TABLE `t` ( `a` int(11) NOT NULL,`b` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL)")
	tk.MustExec("CREATE TABLE `t_bin` ( `a` int(11) NOT NULL,`b` varchar(5) CHARACTER SET binary)")
	tk.MustExec("insert into t values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	tk.MustExec("insert into t_bin values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
}

func TestCollateHashJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	prepare4Join(tk)
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b order by t1.a").Check(
		testkit.Rows("1 a", "1 a", "1 a", "1 a", "2 À", "2 À", "2 À", "2 À", "3 á", "3 á", "3 á", "3 á", "4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b order by t1.a").Check(
		testkit.Rows("1 a", "2 À", "3 á", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>t2.a order by t1.a").Check(
		testkit.Rows("2 À", "3 á", "3 á", "4 à", "4 à", "4 à"))
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>t2.a order by t1.a").Check(
		testkit.Rows())
}

func TestCollateHashJoin2(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	prepare4Join2(tk)
	tk.MustQuery("select /*+ TIDB_HJ(t1, t2) */ * from t1, t2 where t1.v=t2.v order by t1.id").Check(
		testkit.Rows("1 a a", "2 À À", "3 á á", "4 à à", "5 b b", "6 c c", "7    "))
}

func prepare4Join2(tk *testkit.TestKit) {
	tk.MustExec("USE test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (id int, v varchar(5) character set binary, key(v))")
	tk.MustExec("create table t2 (v varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, key(v))")
	tk.MustExec("insert into t1 values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	tk.MustExec("insert into t2 values ('a'), ('À'), ('á'), ('à'), ('b'), ('c'), (' ')")
}

func TestCollateMergeJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	prepare4Join(tk)
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b order by t1.a").Check(
		testkit.Rows("1 a", "1 a", "1 a", "1 a", "2 À", "2 À", "2 À", "2 À", "3 á", "3 á", "3 á", "3 á", "4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b order by t1.a").Check(
		testkit.Rows("1 a", "2 À", "3 á", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "4 à", "4 à", "4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>3 order by t1.a").Check(
		testkit.Rows("4 à", "5 b", "6 c", "7  "))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t t1, t t2 where t1.b=t2.b and t1.a>t2.a order by t1.a").Check(
		testkit.Rows("2 À", "3 á", "3 á", "4 à", "4 à", "4 à"))
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ t1.a, t1.b from t_bin t1, t_bin t2 where t1.b=t2.b and t1.a>t2.a order by t1.a").Check(
		testkit.Rows())
}

func TestCollateMergeJoin2(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	prepare4Join2(tk)
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.v=t2.v order by t1.id").Check(
		testkit.Rows("1 a a", "2 À À", "3 á á", "4 à à", "5 b b", "6 c c", "7    "))
}

func TestCollateIndexMergeJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, b varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci, key(a), key(b))")
	tk.MustExec("insert into t values ('a', 'x'), ('x', 'À'), ('á', 'x'), ('à', 'à'), ('à', 'x')")

	tk.MustExec("set tidb_enable_index_merge=1")
	tk.MustQuery("select /*+ USE_INDEX_MERGE(t, a, b) */ * from t where a = 'a' or b = 'a'").Sort().Check(
		testkit.Rows("a x", "x À", "à x", "à à", "á x"))
}

func TestNewCollationCheckClusterIndexTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t(name char(255) primary key, b int, c int, index idx(name), unique index uidx(name))")
	tk.MustExec("insert into t values(\"aaaa\", 1, 1), (\"bbb\", 2, 2), (\"ccc\", 3, 3)")
	tk.MustExec("admin check table t")
}

func prepare4Collation(tk *testkit.TestKit, hasIndex bool) {
	tk.MustExec("USE test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t_bin")
	idxSQL := ", key(v)"
	if !hasIndex {
		idxSQL = ""
	}
	tk.MustExec(fmt.Sprintf("create table t (id int, v varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL %v)", idxSQL))
	tk.MustExec(fmt.Sprintf("create table t_bin (id int, v varchar(5) CHARACTER SET binary %v)", idxSQL))
	tk.MustExec("insert into t values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	tk.MustExec("insert into t_bin values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
}

func TestCollateSelection(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	prepare4Collation(tk, false)
	tk.MustQuery("select v from t where v='a' order by id").Check(testkit.Rows("a", "À", "á", "à"))
	tk.MustQuery("select v from t_bin where v='a' order by id").Check(testkit.Rows("a"))
	tk.MustQuery("select v from t where v<'b' and id<=3").Check(testkit.Rows("a", "À", "á"))
	tk.MustQuery("select v from t_bin where v<'b' and id<=3").Check(testkit.Rows("a"))
}

func TestCollateSort(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	prepare4Collation(tk, false)
	tk.MustQuery("select id from t order by v, id").Check(testkit.Rows("7", "1", "2", "3", "4", "5", "6"))
	tk.MustQuery("select id from t_bin order by v, id").Check(testkit.Rows("7", "1", "5", "6", "2", "4", "3"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10) collate utf8mb4_general_ci, key(a))")
	tk.MustExec("insert into t values ('a'), ('A'), ('b')")
	tk.MustExec("insert into t values ('a'), ('A'), ('b')")
	tk.MustExec("insert into t values ('a'), ('A'), ('b')")
	tk.MustQuery("select * from t order by a collate utf8mb4_bin").Check(testkit.Rows("A", "A", "A", "a", "a", "a", "b", "b", "b"))
	tk.MustQuery("select * from t order by a collate utf8mb4_general_ci").Check(testkit.Rows("a", "A", "a", "A", "a", "A", "b", "b", "b"))
	tk.MustQuery("select * from t order by a collate utf8mb4_unicode_ci").Check(testkit.Rows("a", "A", "a", "A", "a", "A", "b", "b", "b"))
}

func TestCollateHashAgg(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	prepare4Collation(tk, false)
	tk.HasPlan("select distinct(v) from t_bin", "HashAgg")
	tk.MustQuery("select distinct(v) from t_bin").Sort().Check(testkit.Rows(" ", "a", "b", "c", "À", "à", "á"))
	tk.HasPlan("select distinct(v) from t", "HashAgg")
	tk.MustQuery("select distinct(v) from t").Sort().Check(testkit.Rows(" ", "a", "b", "c"))
	tk.HasPlan("select v, count(*) from t_bin group by v", "HashAgg")
	tk.MustQuery("select v, count(*) from t_bin group by v").Sort().Check(testkit.Rows("  1", "a 1", "b 1", "c 1", "À 1", "à 1", "á 1"))
	tk.HasPlan("select v, count(*) from t group by v", "HashAgg")
	tk.MustQuery("select v, count(*) from t group by v").Sort().Check(testkit.Rows("  1", "a 4", "b 1", "c 1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10) collate utf8mb4_general_ci, key(a))")
	tk.MustExec("insert into t values ('a'), ('A'), ('b')")
	tk.MustExec("insert into t values ('a'), ('A'), ('b')")
	tk.MustExec("insert into t values ('a'), ('A'), ('b')")
	tk.MustExec("insert into t values ('s'), ('ss'), ('ß')")
	tk.MustQuery("select count(1) from t group by a collate utf8mb4_bin order by a collate utf8mb4_bin").Check(testkit.Rows("3", "3", "3", "1", "1", "1"))
	tk.MustQuery("select count(1) from t group by a collate utf8mb4_unicode_ci order by a collate utf8mb4_unicode_ci").Check(testkit.Rows("6", "3", "1", "2"))
	tk.MustQuery("select count(1) from t group by a collate utf8mb4_general_ci order by a collate utf8mb4_general_ci").Check(testkit.Rows("6", "3", "2", "1"))
}

func TestCollateStreamAgg(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	prepare4Collation(tk, true)
	tk.HasPlan("select distinct(v) from t_bin", "StreamAgg")
	tk.MustQuery("select distinct(v) from t_bin").Sort().Check(testkit.Rows(" ", "a", "b", "c", "À", "à", "á"))
	tk.HasPlan("select distinct(v) from t", "StreamAgg")
	tk.MustQuery("select distinct(v) from t").Sort().Check(testkit.Rows(" ", "a", "b", "c"))
	tk.HasPlan("select v, count(*) from t_bin group by v", "StreamAgg")
	tk.MustQuery("select v, count(*) from t_bin group by v").Sort().Check(testkit.Rows("  1", "a 1", "b 1", "c 1", "À 1", "à 1", "á 1"))
	tk.HasPlan("select v, count(*) from t group by v", "StreamAgg")
	tk.MustQuery("select v, count(*) from t group by v").Sort().Check(testkit.Rows("  1", "a 4", "b 1", "c 1"))
}

func TestCollateIndexReader(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	prepare4Collation(tk, true)
	tk.HasPlan("select v from t where v < 'b'  order by v", "IndexReader")
	tk.MustQuery("select v from t where v < 'b' order by v").Check(testkit.Rows(" ", "a", "À", "á", "à"))
	tk.HasPlan("select v from t where v < 'b' and v > ' ' order by v", "IndexReader")
	tk.MustQuery("select v from t where v < 'b' and v > ' ' order by v").Check(testkit.Rows("a", "À", "á", "à"))
	tk.HasPlan("select v from t_bin where v < 'b' order by v", "IndexReader")
	tk.MustQuery("select v from t_bin where v < 'b' order by v").Sort().Check(testkit.Rows(" ", "a"))
	tk.HasPlan("select v from t_bin where v < 'b' and v > ' ' order by v", "IndexReader")
	tk.MustQuery("select v from t_bin where v < 'b' and v > ' ' order by v").Sort().Check(testkit.Rows("a"))
}

func TestCollateIndexLookup(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	prepare4Collation(tk, true)

	tk.HasPlan("select id from t where v < 'b'", "IndexLookUp")
	tk.MustQuery("select id from t where v < 'b'").Sort().Check(testkit.Rows("1", "2", "3", "4", "7"))
	tk.HasPlan("select id from t where v < 'b' and v > ' '", "IndexLookUp")
	tk.MustQuery("select id from t where v < 'b' and v > ' '").Sort().Check(testkit.Rows("1", "2", "3", "4"))
	tk.HasPlan("select id from t_bin where v < 'b'", "IndexLookUp")
	tk.MustQuery("select id from t_bin where v < 'b'").Sort().Check(testkit.Rows("1", "7"))
	tk.HasPlan("select id from t_bin where v < 'b' and v > ' '", "IndexLookUp")
	tk.MustQuery("select id from t_bin where v < 'b' and v > ' '").Sort().Check(testkit.Rows("1"))
}

func TestIssue16668(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tx")
	tk.MustExec("CREATE TABLE `tx` ( `a` int(11) NOT NULL,`b` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL)")
	tk.MustExec("insert into tx values (1, 'a'), (2, 'À'), (3, 'á'), (4, 'à'), (5, 'b'), (6, 'c'), (7, ' ')")
	tk.MustQuery("select count(distinct(b)) from tx").Check(testkit.Rows("4"))
}

func TestIssue27091(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tx")
	tk.MustExec("CREATE TABLE `tx` ( `a` int(11) NOT NULL,`b` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL, `c` varchar(5) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL)")
	tk.MustExec("insert into tx values (1, 'a', 'a'), (2, 'A ', 'a '), (3, 'A', 'A'), (4, 'a ', 'A ')")
	tk.MustQuery("select count(distinct b) from tx").Check(testkit.Rows("1"))
	tk.MustQuery("select count(distinct c) from tx").Check(testkit.Rows("2"))
	tk.MustQuery("select count(distinct b, c) from tx where a < 3").Check(testkit.Rows("1"))
	tk.MustQuery("select approx_count_distinct(b) from tx").Check(testkit.Rows("1"))
	tk.MustQuery("select approx_count_distinct(c) from tx").Check(testkit.Rows("2"))
	tk.MustQuery("select approx_count_distinct(b, c) from tx where a < 3").Check(testkit.Rows("1"))
}

func TestCollateStringFunction(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustQuery("select field('a', 'b', 'a');").Check(testkit.Rows("2"))
	tk.MustQuery("select field('a', 'b', 'A');").Check(testkit.Rows("0"))
	tk.MustQuery("select field('a', 'b', 'A' collate utf8mb4_bin);").Check(testkit.Rows("0"))
	tk.MustQuery("select field('a', 'b', 'a ' collate utf8mb4_bin);").Check(testkit.Rows("2"))
	tk.MustQuery("select field('a', 'b', 'A' collate utf8mb4_unicode_ci);").Check(testkit.Rows("2"))
	tk.MustQuery("select field('a', 'b', 'a ' collate utf8mb4_unicode_ci);").Check(testkit.Rows("2"))
	tk.MustQuery("select field('a', 'b', 'A' collate utf8mb4_general_ci);").Check(testkit.Rows("2"))
	tk.MustQuery("select field('a', 'b', 'a ' collate utf8mb4_general_ci);").Check(testkit.Rows("2"))

	tk.MustExec("USE test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b char (10)) collate utf8mb4_general_ci")
	tk.MustExec("insert into t values ('a', 'A')")
	tk.MustQuery("select field(a, b) from t").Check(testkit.Rows("1"))

	tk.MustQuery("select FIND_IN_SET('a','b,a,c,d');").Check(testkit.Rows("2"))
	tk.MustQuery("select FIND_IN_SET('a','b,A,c,d');").Check(testkit.Rows("0"))
	tk.MustQuery("select FIND_IN_SET('a','b,A,c,d' collate utf8mb4_bin);").Check(testkit.Rows("0"))
	tk.MustQuery("select FIND_IN_SET('a','b,a ,c,d' collate utf8mb4_bin);").Check(testkit.Rows("2"))
	tk.MustQuery("select FIND_IN_SET('a','b,A,c,d' collate utf8mb4_general_ci);").Check(testkit.Rows("2"))
	tk.MustQuery("select FIND_IN_SET('a','b,a ,c,d' collate utf8mb4_general_ci);").Check(testkit.Rows("2"))

	tk.MustExec("set names utf8mb4 collate utf8mb4_general_ci;")
	tk.MustQuery("select collation(cast('a' as char));").Check(testkit.Rows("utf8mb4_general_ci"))
	tk.MustQuery("select collation(cast('a' as binary));").Check(testkit.Rows("binary"))
	tk.MustQuery("select collation(cast('a' collate utf8mb4_bin as char));").Check(testkit.Rows("utf8mb4_general_ci"))
	tk.MustQuery("select collation(cast('a' collate utf8mb4_bin as binary));").Check(testkit.Rows("binary"))

	tk.MustQuery("select FIND_IN_SET('a','b,A,c,d' collate utf8mb4_unicode_ci);").Check(testkit.Rows("2"))
	tk.MustQuery("select FIND_IN_SET('a','b,a ,c,d' collate utf8mb4_unicode_ci);").Check(testkit.Rows("2"))

	tk.MustExec("select concat('a' collate utf8mb4_bin, 'b' collate utf8mb4_bin);")
	tk.MustGetErrMsg("select concat('a' collate utf8mb4_bin, 'b' collate utf8mb4_general_ci);", "[expression:1267]Illegal mix of collations (utf8mb4_bin,EXPLICIT) and (utf8mb4_general_ci,EXPLICIT) for operation 'concat'")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char)")
	tk.MustGetErrMsg("select * from t t1 join t t2 on t1.a collate utf8mb4_bin = t2.a collate utf8mb4_general_ci;", "[expression:1267]Illegal mix of collations (utf8mb4_bin,EXPLICIT) and (utf8mb4_general_ci,EXPLICIT) for operation '='")

	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec("CREATE TABLE t1 ( a int, p1 VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_bin,p2 VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci , p3 VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,p4 VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci ,n1 VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_bin,n2 VARCHAR(255) CHARACTER SET utf8 COLLATE utf8_general_ci , n3 VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,n4 VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci );")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values(1,'  0aA1!测试テストמבחן  ','  0aA1!测试テストמבחן 	','  0aA1!测试テストמבחן 	','  0aA1!测试テストמבחן 	','  0Aa1!测试テストמבחן  ','  0Aa1!测试テストמבחן 	','  0Aa1!测试テストמבחן 	','  0Aa1!测试テストמבחן 	');")

	tk.MustQuery("select INSTR(p1,n1) from t1;").Check(testkit.Rows("0"))
	tk.MustQuery("select INSTR(p1,n2) from t1;").Check(testkit.Rows("0"))
	tk.MustQuery("select INSTR(p1,n3) from t1;").Check(testkit.Rows("0"))
	tk.MustQuery("select INSTR(p1,n4) from t1;").Check(testkit.Rows("0"))
	tk.MustQuery("select INSTR(p2,n1) from t1;").Check(testkit.Rows("0"))
	tk.MustQuery("select INSTR(p2,n2) from t1;").Check(testkit.Rows("1"))
	tk.MustQuery("select INSTR(p2,n3) from t1;").Check(testkit.Rows("0"))
	tk.MustQuery("select INSTR(p2,n4) from t1;").Check(testkit.Rows("1"))
	tk.MustQuery("select INSTR(p3,n1) from t1;").Check(testkit.Rows("0"))
	tk.MustQuery("select INSTR(p3,n2) from t1;").Check(testkit.Rows("0"))
	tk.MustQuery("select INSTR(p3,n3) from t1;").Check(testkit.Rows("0"))
	tk.MustQuery("select INSTR(p3,n4) from t1;").Check(testkit.Rows("0"))
	tk.MustQuery("select INSTR(p4,n1) from t1;").Check(testkit.Rows("0"))
	tk.MustQuery("select INSTR(p4,n2) from t1;").Check(testkit.Rows("1"))
	tk.MustQuery("select INSTR(p4,n3) from t1;").Check(testkit.Rows("0"))
	tk.MustQuery("select INSTR(p4,n4) from t1;").Check(testkit.Rows("1"))

	tk.MustExec("truncate table t1;")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (1,'0aA1!测试テストמבחן  ','0aA1!测试テストמבחן 	','0aA1!测试テストמבחן 	','0aA1!测试テストמבחן 	','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (2,'0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (3,'0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0Aa1!测试テストמבחן  ','0Aa1!测试テストמבחן  ','0Aa1!测试テストמבחן  ','0Aa1!测试テストמבחן  ');")

	tk.MustQuery("select LOCATE(p1,n1) from t1;").Check(testkit.Rows("0", "0", "0"))
	tk.MustQuery("select LOCATE(p1,n2) from t1;").Check(testkit.Rows("0", "0", "0"))
	tk.MustQuery("select LOCATE(p1,n3) from t1;").Check(testkit.Rows("0", "0", "0"))
	tk.MustQuery("select LOCATE(p1,n4) from t1;").Check(testkit.Rows("0", "1", "1"))
	tk.MustQuery("select LOCATE(p2,n1) from t1;").Check(testkit.Rows("0", "0", "0"))
	tk.MustQuery("select LOCATE(p2,n2) from t1;").Check(testkit.Rows("0", "1", "1"))
	tk.MustQuery("select LOCATE(p2,n3) from t1;").Check(testkit.Rows("0", "0", "0"))
	tk.MustQuery("select LOCATE(p2,n4) from t1;").Check(testkit.Rows("0", "1", "1"))
	tk.MustQuery("select LOCATE(p3,n1) from t1;").Check(testkit.Rows("0", "0", "0"))
	tk.MustQuery("select LOCATE(p3,n2) from t1;").Check(testkit.Rows("0", "0", "0"))
	tk.MustQuery("select LOCATE(p3,n3) from t1;").Check(testkit.Rows("0", "0", "0"))
	tk.MustQuery("select LOCATE(p3,n4) from t1;").Check(testkit.Rows("0", "0", "0"))
	tk.MustQuery("select LOCATE(p4,n1) from t1;").Check(testkit.Rows("0", "1", "1"))
	tk.MustQuery("select LOCATE(p4,n2) from t1;").Check(testkit.Rows("0", "1", "1"))
	tk.MustQuery("select LOCATE(p4,n3) from t1;").Check(testkit.Rows("0", "0", "0"))
	tk.MustQuery("select LOCATE(p4,n4) from t1;").Check(testkit.Rows("0", "1", "1"))

	tk.MustQuery("select locate('S', 's' collate utf8mb4_general_ci);").Check(testkit.Rows("1"))
	tk.MustQuery("select locate('S', 'a' collate utf8mb4_general_ci);").Check(testkit.Rows("0"))
	// MySQL return 0 here, I believe it is a bug in MySQL since 'ß' == 's' under utf8mb4_general_ci collation.
	tk.MustQuery("select locate('ß', 's' collate utf8mb4_general_ci);").Check(testkit.Rows("1"))
	tk.MustQuery("select locate('world', 'hello world' collate utf8mb4_general_ci);").Check(testkit.Rows("7"))
	tk.MustQuery("select locate(' ', 'hello world' collate utf8mb4_general_ci);").Check(testkit.Rows("6"))
	tk.MustQuery("select locate('  ', 'hello world' collate utf8mb4_general_ci);").Check(testkit.Rows("0"))

	tk.MustQuery("select locate('S', 's' collate utf8mb4_unicode_ci);").Check(testkit.Rows("1"))
	tk.MustQuery("select locate('S', 'a' collate utf8mb4_unicode_ci);").Check(testkit.Rows("0"))
	tk.MustQuery("select locate('ß', 'ss' collate utf8mb4_unicode_ci);").Check(testkit.Rows("1"))
	tk.MustQuery("select locate('world', 'hello world' collate utf8mb4_unicode_ci);").Check(testkit.Rows("7"))
	tk.MustQuery("select locate(' ', 'hello world' collate utf8mb4_unicode_ci);").Check(testkit.Rows("6"))
	tk.MustQuery("select locate('  ', 'hello world' collate utf8mb4_unicode_ci);").Check(testkit.Rows("0"))

	tk.MustExec("truncate table t1;")
	tk.MustExec("insert into t1 (a) values (1);")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (2,'0aA1!测试テストמבחן  ','0aA1!测试テストמבחן       ','0aA1!测试テストמבחן  ','0aA1!测试テストמבחן  ','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (3,'0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן','0Aa1!测试テストמבחן');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (4,'0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0Aa1!测试テストמבחן  ','0Aa1!测试テストמבחן  ','0Aa1!测试テストמבחן  ','0Aa1!测试テストמבחן  ');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (5,'0aA1!测试テストמבחן0aA1!测试','0aA1!测试テストמבחן0aA1!测试','0aA1!测试テストמבחן0aA1!测试','0aA1!测试テストמבחן0aA1!测试','0Aa1!测试','0Aa1!测试','0Aa1!测试','0Aa1!测试');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (6,'0aA1!测试テストמבחן0aA1!测试','0aA1!测试テストמבחן0aA1!测试','0aA1!测试テストמבחן0aA1!测试','0aA1!测试テストמבחן0aA1!测试','0aA1!测试','0aA1!测试','0aA1!测试','0aA1!测试');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (7,'0aA1!测试テストמבחן  ','0aA1!测试テストמבחן       ','0aA1!测试テストמבחן  ','0aA1!测试テストמבחן  ','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן');")
	tk.MustExec("insert into t1 (a,p1,p2,p3,p4,n1,n2,n3,n4) values (8,'0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן','0aA1!测试テストמבחן  ','0aA1!测试テストמבחן  ','0aA1!测试テストמבחן  ','0aA1!测试テストמבחן  ');")

	tk.MustQuery("select p1 REGEXP n1 from t1;").Check(testkit.Rows("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p1 REGEXP n2 from t1;").Check(testkit.Rows("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p1 REGEXP n3 from t1;").Check(testkit.Rows("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p1 REGEXP n4 from t1;").Check(testkit.Rows("<nil>", "1", "1", "0", "1", "1", "1", "0"))
	tk.MustQuery("select p2 REGEXP n1 from t1;").Check(testkit.Rows("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p2 REGEXP n2 from t1;").Check(testkit.Rows("<nil>", "1", "1", "0", "1", "1", "1", "0"))
	tk.MustQuery("select p2 REGEXP n3 from t1;").Check(testkit.Rows("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p2 REGEXP n4 from t1;").Check(testkit.Rows("<nil>", "1", "1", "0", "1", "1", "1", "0"))
	tk.MustQuery("select p3 REGEXP n1 from t1;").Check(testkit.Rows("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p3 REGEXP n2 from t1;").Check(testkit.Rows("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p3 REGEXP n3 from t1;").Check(testkit.Rows("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p3 REGEXP n4 from t1;").Check(testkit.Rows("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p4 REGEXP n1 from t1;").Check(testkit.Rows("<nil>", "1", "1", "0", "1", "1", "1", "0"))
	tk.MustQuery("select p4 REGEXP n2 from t1;").Check(testkit.Rows("<nil>", "1", "1", "0", "1", "1", "1", "0"))
	tk.MustQuery("select p4 REGEXP n3 from t1;").Check(testkit.Rows("<nil>", "0", "0", "0", "0", "1", "1", "0"))
	tk.MustQuery("select p4 REGEXP n4 from t1;").Check(testkit.Rows("<nil>", "1", "1", "0", "1", "1", "1", "0"))

	tk.MustExec("drop table t1;")
}

func TestCollateLike(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set names utf8mb4 collate utf8mb4_general_ci")
	tk.MustQuery("select 'a' like 'A'").Check(testkit.Rows("1"))
	tk.MustQuery("select 'a' like 'A' collate utf8mb4_general_ci").Check(testkit.Rows("1"))
	tk.MustQuery("select 'a' like 'À'").Check(testkit.Rows("1"))
	tk.MustQuery("select 'a' like '%À'").Check(testkit.Rows("1"))
	tk.MustQuery("select 'a' like '%À '").Check(testkit.Rows("0"))
	tk.MustQuery("select 'a' like 'À%'").Check(testkit.Rows("1"))
	tk.MustQuery("select 'a' like 'À_'").Check(testkit.Rows("0"))
	tk.MustQuery("select 'a' like '%À%'").Check(testkit.Rows("1"))
	tk.MustQuery("select 'aaa' like '%ÀAa%'").Check(testkit.Rows("1"))
	tk.MustExec("set names utf8mb4 collate utf8mb4_bin")

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t_like;")
	tk.MustExec("create table t_like(id int, b varchar(20) collate utf8mb4_general_ci);")
	tk.MustExec("insert into t_like values (1, 'aaa'), (2, 'abc'), (3, 'aac');")
	tk.MustQuery("select b like 'AaÀ' from t_like order by id;").Check(testkit.Rows("1", "0", "0"))
	tk.MustQuery("select b like 'Aa_' from t_like order by id;").Check(testkit.Rows("1", "0", "1"))
	tk.MustQuery("select b like '_A_' from t_like order by id;").Check(testkit.Rows("1", "0", "1"))
	tk.MustQuery("select b from t_like where b like 'Aa_' order by id;").Check(testkit.Rows("aaa", "aac"))
	tk.MustQuery("select b from t_like where b like 'A%' order by id;").Check(testkit.Rows("aaa", "abc", "aac"))
	tk.MustQuery("select b from t_like where b like '%A%' order by id;").Check(testkit.Rows("aaa", "abc", "aac"))
	tk.MustExec("alter table t_like add index idx_b(b);")
	tk.MustQuery("select b from t_like use index(idx_b) where b like 'Aa_' order by id;").Check(testkit.Rows("aaa", "aac"))
	tk.MustQuery("select b from t_like use index(idx_b) where b like 'A%' order by id;").Check(testkit.Rows("aaa", "abc", "aac"))
	tk.MustQuery("select b from t_like use index(idx_b) where b like '%A%' order by id;").Check(testkit.Rows("aaa", "abc", "aac"))
}

func TestCollateSubQuery(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	prepare4Collation(tk, false)
	tk.MustQuery("select id from t where v in (select v from t_bin) order by id").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7"))
	tk.MustQuery("select id from t_bin where v in (select v from t) order by id").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7"))
	tk.MustQuery("select id from t where v not in (select v from t_bin) order by id").Check(testkit.Rows())
	tk.MustQuery("select id from t_bin where v not in (select v from t) order by id").Check(testkit.Rows())
	tk.MustQuery("select id from t where exists (select 1 from t_bin where t_bin.v=t.v) order by id").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7"))
	tk.MustQuery("select id from t_bin where exists (select 1 from t where t_bin.v=t.v) order by id").Check(testkit.Rows("1", "2", "3", "4", "5", "6", "7"))
	tk.MustQuery("select id from t where not exists (select 1 from t_bin where t_bin.v=t.v) order by id").Check(testkit.Rows())
	tk.MustQuery("select id from t_bin where not exists (select 1 from t where t_bin.v=t.v) order by id").Check(testkit.Rows())
}

func TestCollateDDL(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database t;")
	tk.MustExec("use t;")
	tk.MustExec("drop database t;")
}

func TestNewCollationWithClusterIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t(d double primary key, a int, name varchar(255), index idx(name(2)), index midx(a, name))")
	tk.MustExec("insert into t values(2.11, 1, \"aa\"), (-1, 0, \"abcd\"), (9.99, 0, \"aaaa\")")
	tk.MustQuery("select d from t use index(idx) where name=\"aa\"").Check(testkit.Rows("2.11"))
}

func TestNewCollationBinaryFlag(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(2) binary, index (a));")
	tk.MustExec("insert into t values ('a ');")
	tk.MustQuery("select hex(a) from t;").Check(testkit.Rows("6120"))
	tk.MustQuery("select hex(a) from t use index (a);").Check(testkit.Rows("6120"))

	showCreateTable := func(createSQL string) string {
		tk.MustExec("drop table if exists t;")
		tk.MustExec(createSQL)
		s := tk.MustQuery("show create table t;").Rows()[0][1].(string)
		return s
	}
	var sct string
	// define case = tuple(table_charset, table_collation, column_charset, column_collation)
	// case: (nil, nil, nil, nil)
	sct = showCreateTable("create table t(a varchar(10) binary);")
	require.Contains(t, sct, "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	// case: (nil, utf8_general_ci, nil, nil)
	sct = showCreateTable("create table t(a varchar(10) binary) collate utf8_general_ci;")
	require.Contains(t, sct, "varchar(10) COLLATE utf8_bin")
	require.Contains(t, sct, "ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci")
	// case: (nil, nil, nil, utf8_general_ci)
	sct = showCreateTable("create table t(a varchar(10) binary collate utf8_general_ci);")
	require.Contains(t, sct, "varchar(10) CHARACTER SET utf8 COLLATE utf8_bin")
	// case: (nil, nil, utf8, utf8_general_ci)
	sct = showCreateTable("create table t(a varchar(10) binary charset utf8 collate utf8_general_ci);")
	require.Contains(t, sct, "varchar(10) CHARACTER SET utf8 COLLATE utf8_general_ci")
	// case: (utf8, utf8_general_ci, utf8mb4, utf8mb4_unicode_ci)
	sct = showCreateTable("create table t(a varchar(10) binary charset utf8mb4 collate utf8mb4_unicode_ci) charset utf8 collate utf8_general_ci;")
	require.Contains(t, sct, "varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
	require.Contains(t, sct, "ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci")
	// case: (nil, nil, binary, nil)
	sct = showCreateTable("create table t(a varchar(10) binary charset binary);")
	require.Contains(t, sct, "varbinary(10) DEFAULT NULL")
	require.Contains(t, sct, "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
}

func TestIssue17176(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustGetErrMsg("create table t(a enum('a', 'a ')) charset utf8 collate utf8_bin;", "[types:1291]Column 'a' has duplicated value 'a' in ENUM")
	tk.MustGetErrMsg("create table t(a enum('a', 'Á')) charset utf8 collate utf8_general_ci;", "[types:1291]Column 'a' has duplicated value 'Á' in ENUM")
	tk.MustGetErrMsg("create table t(a enum('a', 'a ')) charset utf8mb4 collate utf8mb4_bin;", "[types:1291]Column 'a' has duplicated value 'a' in ENUM")
	tk.MustExec("create table t(a enum('a', 'A')) charset utf8 collate utf8_bin;")
	tk.MustExec("drop table t;")
	tk.MustExec("create table t3(a enum('a', 'A')) charset utf8mb4 collate utf8mb4_bin;")
}

func TestIssue18638(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(10) collate utf8mb4_bin, b varchar(10) collate utf8mb4_general_ci);")
	tk.MustExec("insert into t (a, b) values ('a', 'A');")
	tk.MustQuery("select * from t t1, t t2 where t1.a = t2.b collate utf8mb4_general_ci;").Check(testkit.Rows("a A a A"))
	tk.MustQuery("select * from t t1 left join t t2 on t1.a = t2.b collate utf8mb4_general_ci;").Check(testkit.Rows("a A a A"))
}

func TestCollationText(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a TINYTEXT collate UTF8MB4_GENERAL_CI, UNIQUE KEY `a`(`a`(10)));")
	tk.MustExec("insert into t (a) values ('A');")
	tk.MustQuery("select * from t t1 inner join t t2 on t1.a = t2.a where t1.a = 'A';").Check(testkit.Rows("A A"))
	tk.MustExec("update t set a = 'B';")
	tk.MustExec("admin check table t;")
}

func TestIssue18662(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(10) collate utf8mb4_bin, b varchar(10) collate utf8mb4_general_ci);")
	tk.MustExec("insert into t (a, b) values ('a', 'A');")
	tk.MustQuery("select * from t where field('A', a collate utf8mb4_general_ci, b) > 1;").Check(testkit.Rows())
	tk.MustQuery("select * from t where field('A', a, b collate utf8mb4_general_ci) > 1;").Check(testkit.Rows())
	tk.MustQuery("select * from t where field('A' collate utf8mb4_general_ci, a, b) > 1;").Check(testkit.Rows())
	tk.MustQuery("select * from t where field('A', a, b) > 1;").Check(testkit.Rows("a A"))
}

func TestIssue19116(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set names utf8mb4 collate utf8mb4_general_ci;")
	tk.MustQuery("select collation(concat(1 collate `binary`));").Check(testkit.Rows("binary"))
	tk.MustQuery("select coercibility(concat(1 collate `binary`));").Check(testkit.Rows("0"))
	tk.MustQuery("select collation(concat(NULL,NULL));").Check(testkit.Rows("binary"))
	tk.MustQuery("select coercibility(concat(NULL,NULL));").Check(testkit.Rows("6"))
	tk.MustQuery("select collation(concat(1,1));").Check(testkit.Rows("utf8mb4_general_ci"))
	tk.MustQuery("select coercibility(concat(1,1));").Check(testkit.Rows("4"))
	tk.MustQuery("select collation(1);").Check(testkit.Rows("binary"))
	tk.MustQuery("select coercibility(1);").Check(testkit.Rows("5"))
	tk.MustQuery("select coercibility(1=1);").Check(testkit.Rows("5"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime)")
	tk.MustExec("insert into t values ('2020-02-02')")
	tk.MustQuery("select collation(concat(unix_timestamp(a))) from t;").Check(testkit.Rows("utf8mb4_general_ci"))
	tk.MustQuery("select coercibility(concat(unix_timestamp(a))) from t;").Check(testkit.Rows("4"))
}

func TestIssue17063(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec("create table t(a char, b char) collate utf8mb4_general_ci;")
	tk.MustExec(`insert into t values('a', 'b');`)
	tk.MustExec(`insert into t values('a', 'B');`)
	tk.MustQuery(`select * from t where if(a='x', a, b) = 'b';`).Check(testkit.Rows("a b", "a B"))
	tk.MustQuery(`select collation(if(a='x', a, b)) from t;`).Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery(`select coercibility(if(a='x', a, b)) from t;`).Check(testkit.Rows("2", "2"))
	tk.MustQuery(`select collation(lag(b, 1, 'B') over w) from t window w as (order by b);`).Check(testkit.Rows("utf8mb4_general_ci", "utf8mb4_general_ci"))
	tk.MustQuery(`select coercibility(lag(b, 1, 'B') over w) from t window w as (order by b);`).Check(testkit.Rows("2", "2"))
}

func TestIssue11177(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("SELECT 'lvuleck' BETWEEN '2008-09-16 22:23:50' AND 0;").Check(testkit.Rows("0"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect DOUBLE value: 'lvuleck'", "Warning 1292 Truncated incorrect DOUBLE value: '2008-09-16 22:23:50'"))
	tk.MustQuery("SELECT 'aa' BETWEEN 'bb' AND 0;").Check(testkit.Rows("1"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect DOUBLE value: 'aa'", "Warning 1292 Truncated incorrect DOUBLE value: 'bb'"))
	tk.MustQuery("select 1 between 0 and b'110';").Check(testkit.Rows("1"))
	tk.MustQuery("show warnings;").Check(testkit.Rows())
	tk.MustQuery("select 'b' between 'a' and b'110';").Check(testkit.Rows("0"))
	tk.MustQuery("show warnings;").Check(testkit.Rows())
}

func TestIssue19804(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a set('a', 'b', 'c'));`)
	tk.MustGetErrMsg("alter table t change a a set('a', 'b', 'c', 'c');", "[types:1291]Column 'a' has duplicated value 'c' in SET")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a enum('a', 'b', 'c'));`)
	tk.MustGetErrMsg("alter table t change a a enum('a', 'b', 'c', 'c');", "[types:1291]Column 'a' has duplicated value 'c' in ENUM")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a set('a', 'b', 'c'));`)
	tk.MustExec(`alter table t change a a set('a', 'b', 'c', 'd');`)
	tk.MustExec(`insert into t values('d');`)
	tk.MustGetErrMsg(`alter table t change a a set('a', 'b', 'c', 'e', 'f');`, "[types:1265]Data truncated for column 'a', value is 'd'")
}

func TestIssue20209(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`set @@character_set_client=utf8mb4;`)
	tk.MustExec(`set @@collation_connection=utf8_bin;`)
	tk.MustExec("CREATE VIEW tview_1 AS SELECT 'a' AS `id`;")
}

func TestIssue18949(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a enum('a ', 'b\t', ' c '), b set('a ', 'b\t', ' c '));`)
	result := tk.MustQuery("show create table t").Rows()[0][1]
	require.Regexp(t, `(?s).*enum\('a','b	',' c'\).*set\('a','b	',' c'\).*`, result)
	tk.MustExec(`alter table t change a aa enum('a   ', 'b\t', ' c ');`)
	result = tk.MustQuery("show create table t").Rows()[0][1]
	require.Regexp(t, `(?s).*enum\('a','b	',' c'\).*set\('a','b	',' c'\).*`, result)
}

func TestClusteredIndexAndNewCollationIndexEncodeDecodeV5(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t(a int, b char(10) collate utf8mb4_bin, c char(10) collate utf8mb4_general_ci," +
		"d varchar(10) collate utf8mb4_bin, e varchar(10) collate utf8mb4_general_ci, f char(10) collate utf8mb4_unicode_ci, g varchar(10) collate utf8mb4_unicode_ci, " +
		"primary key(a, b, c, d, e, f, g), key a(a), unique key ua(a), key b(b), unique key ub(b), key c(c), unique key uc(c)," +
		"key d(d), unique key ud(d),key e(e), unique key ue(e), key f(f), key g(g), unique key uf(f), unique key ug(g))")

	tk.MustExec("insert into t values (1, '啊  ', '啊  ', '啊  ', '啊  ', '啊  ', '啊  ')")
	// Single Read.
	tk.MustQuery("select * from t ").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))

	tk.MustQuery("select * from t use index(a)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(ua)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(b)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(ub)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(c)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(uc)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(d)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(ud)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(e)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(ue)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(f)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(uf)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(g)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(ug)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))

	tk.MustExec("alter table t add column h varchar(10) collate utf8mb4_general_ci default '🐸'")
	tk.MustExec("alter table t add column i varchar(10) collate utf8mb4_general_ci default '🐸'")
	tk.MustExec("alter table t add index h(h)")
	tk.MustExec("alter table t add unique index uh(h)")

	tk.MustQuery("select * from t use index(h)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(uh)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))

	// Double read.
	tk.MustQuery("select * from t use index(a)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(ua)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(b)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(ub)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(c)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(uc)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(d)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(ud)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(e)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(ue)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustExec("admin check table t")
	tk.MustExec("admin recover index t a")
	tk.MustExec("alter table t add column n char(10) COLLATE utf8mb4_unicode_ci")
	tk.MustExec("alter table t add index n(n)")
	tk.MustExec("update t set n = '吧';")
	tk.MustQuery("select * from t").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸,吧"))
	tk.MustQuery("select * from t use index(n)").Check(testkit.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸,吧"))
	tk.MustExec("admin check table t")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a varchar(255) COLLATE utf8_general_ci primary key clustered, b int) partition by range columns(a) " +
		"(partition p0 values less than ('0'), partition p1 values less than MAXVALUE);")
	tk.MustExec("alter table t add index b(b);")
	tk.MustExec("insert into t values ('0', 1);")
	tk.MustQuery("select * from t use index(b);").Check(testkit.Rows("0 1"))
	tk.MustQuery("select * from t use index();").Check(testkit.Rows("0 1"))
	tk.MustExec("admin check table t")
}

func TestClusteredIndexAndNewCollation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("CREATE TABLE `t` (" +
		"`a` char(10) COLLATE utf8mb4_unicode_ci NOT NULL," +
		"`b` char(20) COLLATE utf8mb4_general_ci NOT NULL," +
		"`c` int(11) NOT NULL," +
		"PRIMARY KEY (`a`,`b`,`c`)," +
		"KEY `idx` (`a`))")

	tk.MustExec("begin")
	tk.MustExec("insert into t values ('a6', 'b6', 3)")
	tk.MustQuery("select * from t").Check(testkit.Rows("a6 b6 3"))
	tk.MustQuery("select * from t where a='a6'").Check(testkit.Rows("a6 b6 3"))
	tk.MustExec("delete from t")
	tk.MustQuery("select * from t").Check(testkit.Rows())
	tk.MustExec("commit")
	tk.MustQuery("select * from t").Check(testkit.Rows())

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(`a` char(10) COLLATE utf8mb4_unicode_ci NOT NULL key)")
	tk.MustExec("insert into t values ('&');")
	tk.MustExec("replace into t values ('&');")
	tk.MustQuery("select * from t").Check(testkit.Rows("&"))
}

func TestIssue20608(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select '䇇Հ' collate utf8mb4_bin like '___Հ';").Check(testkit.Rows("0"))
}

func TestIssue20161(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(raw JSON);`)
	tk.MustExec(`insert into t(raw) values('["a","ab"]'), ('["a"]'), (null);`)
	tk.MustQuery(`SELECT JSON_SEARCH(raw,'one','c') FROM t;`).
		Check(testkit.Rows("<nil>", "<nil>", "<nil>"))
}

func TestCollationIndexJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b char(10), key(b)) collate utf8mb4_general_ci")
	tk.MustExec("create table t2(a int, b char(10), key(b)) collate ascii_bin")
	tk.MustExec("insert into t1 values (1, 'a')")
	tk.MustExec("insert into t2 values (1, 'A')")

	tk.MustQuery("select /*+ inl_join(t1) */ t1.b, t2.b from t1 join t2 where t1.b=t2.b").Check(testkit.Rows("a A"))
	tk.MustQuery("select /*+ hash_join(t1) */ t1.b, t2.b from t1 join t2 where t1.b=t2.b").Check(testkit.Rows("a A"))
	tk.MustQuery("select /*+ merge_join(t1) */ t1.b, t2.b from t1 join t2 where t1.b=t2.b").Check(testkit.Rows("a A"))
	tk.MustQuery("select /*+ inl_hash_join(t1) */ t1.b, t2.b from t1 join t2 where t1.b=t2.b").Check(testkit.Rows("a A"))
	tk.MustQuery("select /*+ inl_hash_join(t2) */ t1.b, t2.b from t1 join t2 where t1.b=t2.b").Check(testkit.Rows("a A"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 Optimizer Hint /*+ INL_HASH_JOIN(t2) */ is inapplicable"))
	tk.MustQuery("select /*+ inl_merge_join(t1) */ t1.b, t2.b from t1 join t2 where t1.b=t2.b").Check(testkit.Rows("a A"))
	tk.MustQuery("select /*+ inl_merge_join(t2) */ t1.b, t2.b from t1 join t2 where t1.b=t2.b").Check(testkit.Rows("a A"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 Optimizer Hint /*+ INL_MERGE_JOIN(t2) */ is inapplicable"))

	tk.MustExec("drop table if exists a, b")
	tk.MustExec("create table a(i int, k varbinary(40), v int, primary key(i, k) clustered)")
	tk.MustExec("create table b(i int, k varchar(40), v int, primary key(i, k) clustered)")
	tk.MustExec("insert into a select 3, 'nice mccarthy', 10")
	tk.MustQuery("select * from a, b where a.i = b.i and a.k = b.k").Check(testkit.Rows())

	tk.MustExec("drop table if exists a, b")
	tk.MustExec("create table a(i int  NOT NULL, k varbinary(40)  NOT NULL, v int, key idx1(i, k))")
	tk.MustExec("create table b(i int  NOT NULL, k varchar(40)  NOT NULL, v int, key idx1(i, k))")
	tk.MustExec("insert into a select 3, 'nice mccarthy', 10")
	tk.MustQuery(" select /*+ inl_join(b) */ b.i from a, b where a.i = b.i and a.k = b.k").Check(testkit.Rows())
}

func TestCollationMergeJoin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (" +
		"  `col_10` blob DEFAULT NULL," +
		"  `col_11` decimal(17,5) NOT NULL," +
		"  `col_13` varchar(381) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'Yr'," +
		"  PRIMARY KEY (`col_13`,`col_11`) CLUSTERED," +
		"  KEY `idx_4` (`col_10`(3))" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("insert into t values ('a', 12523, 'A');")
	tk.MustExec("insert into t values ('A', 2, 'a');")
	tk.MustExec("insert into t values ('a', 23, 'A');")
	tk.MustExec("insert into t values ('a', 23, 'h2');")
	tk.MustExec("insert into t values ('a', 23, 'h3');")
	tk.MustExec("insert into t values ('a', 23, 'h4');")
	tk.MustExec("insert into t values ('a', 23, 'h5');")
	tk.MustExec("insert into t values ('a', 23, 'h6');")
	tk.MustExec("insert into t values ('a', 23, 'h7');")
	tk.MustQuery("select /*+ MERGE_JOIN(t) */ t.* from t where col_13 in ( select col_10 from t where t.col_13 in ( 'a', 'b' ) ) order by col_10 ;").Check(
		testkit.Rows("\x41 2.00000 a", "\x61 23.00000 A", "\x61 12523.00000 A"))
}

func TestIssue20876(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (" +
		"  `a` char(10) COLLATE utf8mb4_unicode_ci NOT NULL," +
		"  `b` char(20) COLLATE utf8mb4_general_ci NOT NULL," +
		"  `c` int(11) NOT NULL," +
		"  PRIMARY KEY (`a`,`b`,`c`)," +
		"  KEY `idx` (`a`)" +
		")")
	tk.MustExec("insert into t values ('#', 'C', 10), ('$', 'c', 20), ('$', 'c', 30), ('a', 'a', 10), ('A', 'A', 30)")
	tk.MustExec("analyze table t")
	tk.MustQuery("select * from t where a='#';").Check(testkit.Rows("# C 10"))
}

func TestLikeWithCollation(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery(`select 'a' like 'A' collate utf8mb4_unicode_ci;`).Check(testkit.Rows("1"))
	tk.MustGetErrMsg(`select 'a' collate utf8mb4_bin like 'A' collate utf8mb4_unicode_ci;`, "[expression:1267]Illegal mix of collations (utf8mb4_bin,EXPLICIT) and (utf8mb4_unicode_ci,EXPLICIT) for operation 'like'")
	tk.MustQuery(`select '😛' collate utf8mb4_general_ci like '😋';`).Check(testkit.Rows("1"))
	tk.MustQuery(`select '😛' collate utf8mb4_general_ci = '😋';`).Check(testkit.Rows("1"))
	tk.MustQuery(`select '😛' collate utf8mb4_unicode_ci like '😋';`).Check(testkit.Rows("0"))
	tk.MustQuery(`select '😛' collate utf8mb4_unicode_ci = '😋';`).Check(testkit.Rows("1"))
}

func TestCollationPrefixClusteredIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (k char(20), v int, primary key (k(4)) clustered, key (k)) collate utf8mb4_general_ci;")
	tk.MustExec("insert into t values('01233', 1);")
	tk.MustExec("create index idx on t(k(2))")
	tk.MustQuery("select * from t use index(k_2);").Check(testkit.Rows("01233 1"))
	tk.MustQuery("select * from t use index(idx);").Check(testkit.Rows("01233 1"))
	tk.MustExec("admin check table t;")
}

func TestIssue23805(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `tbl_5` (" +
		"  `col_25` time NOT NULL DEFAULT '05:35:58'," +
		"  `col_26` blob NOT NULL," +
		"  `col_27` double NOT NULL," +
		"  `col_28` char(83) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL," +
		"  `col_29` timestamp NOT NULL," +
		"  `col_30` varchar(36) COLLATE utf8mb4_general_ci NOT NULL DEFAULT 'ywzIn'," +
		"  `col_31` binary(85) DEFAULT 'OIstcXsGmAyc'," +
		"  `col_32` datetime NOT NULL DEFAULT '2024-08-02 00:00:00'," +
		"  PRIMARY KEY (`col_26`(3),`col_27`) /*T![clustered_index] CLUSTERED */," +
		"  UNIQUE KEY `idx_10` (`col_26`(5)));")
	tk.MustExec("insert ignore into tbl_5 set col_28 = 'ZmZIdSnq' , col_25 = '18:50:52.00' on duplicate key update col_26 = 'y';\n")
}

func TestIssue26662(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a varchar(36) NOT NULL) ENGINE = InnoDB DEFAULT CHARSET = utf8 COLLATE = utf8_general_ci;")
	tk.MustExec("set names utf8;")
	tk.MustQuery("select t2.b from (select t1.a as b from t1 union all select t1.a as b from t1) t2 where case when (t2.b is not null) then t2.b else '' end > '1234567';").
		Check(testkit.Rows())
}

func TestIssue30245(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustGetErrCode("select case 1 when 1 then 'a' collate utf8mb4_unicode_ci else 'b' collate utf8mb4_general_ci end", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select case when 1 then 'a' collate utf8mb4_unicode_ci when 2 then 'b' collate utf8mb4_general_ci end", mysql.ErrCantAggregate2collations)
	tk.MustGetErrCode("select case 1 when 1 then 'a' collate utf8mb4_unicode_ci when 2 then 'b' collate utf8mb4_general_ci else 'b' collate utf8mb4_bin end", mysql.ErrCantAggregate3collations)
}

func TestCollationForBinaryLiteral(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (`COL1` tinyblob NOT NULL,  `COL2` binary(1) NOT NULL,  `COL3` bigint(11) NOT NULL,  PRIMARY KEY (`COL1`(5),`COL2`,`COL3`) /*T![clustered_index] CLUSTERED */)")
	tk.MustExec("insert into t values(0x1E,0xEC,6966939640596047133);")
	tk.MustQuery("select * from t where col1 not in (0x1B,0x20) order by col1").Check(testkit.Rows("\x1e \xec 6966939640596047133"))
	tk.MustExec("drop table t")
}

func TestMathBuiltin(t *testing.T) {
	t.Skip("it has been broken. Please fix it as soon as possible.")
	ctx := context.Background()
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
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

	// for log
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
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	terr := errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(mysql.ErrDataOutOfRange), terr.Code())
	require.NoError(t, rs.Close())

	// for exp
	result = tk.MustQuery("select exp(0), exp(1), exp(-1), exp(1.2), exp(NULL)")
	result.Check(testkit.Rows("1 2.718281828459045 0.36787944117144233 3.3201169227365472 <nil>"))
	result = tk.MustQuery("select exp('tidb'), exp('1tidb')")
	result.Check(testkit.Rows("1 2.718281828459045"))
	rs, err = tk.Exec("select exp(1000000)")
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(mysql.ErrDataOutOfRange), terr.Code())
	require.NoError(t, rs.Close())
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a float)")
	tk.MustExec("insert into t values(1000000)")
	rs, err = tk.Exec("select exp(a) from t")
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(mysql.ErrDataOutOfRange), terr.Code())
	require.EqualError(t, err, "[types:1690]DOUBLE value is out of range in 'exp(test.t.a)'")
	require.NoError(t, rs.Close())

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
	result = tk.MustQuery("SELECT CONV(0x0020, 2, 2);")
	result.Check(testkit.Rows("100000"))
	result = tk.MustQuery("SELECT CONV(0b10, 16, 2)")
	result.Check(testkit.Rows("10"))
	result = tk.MustQuery("SELECT CONV(0b10, 16, 8)")
	result.Check(testkit.Rows("2"))
	tk.MustExec("drop table if exists bit")
	tk.MustExec("create table bit(b bit(10))")
	tk.MustExec(`INSERT INTO bit (b) VALUES
			(0b0000010101),
			(0b0000010101),
			(NULL),
			(0b0000000001),
			(0b0000000000),
			(0b1111111111),
			(0b1111111111),
			(0b1111111111),
			(0b0000000000),
			(0b0000000000),
			(0b0000000000),
			(0b0000000000),
			(0b0000100000);`)
	tk.MustQuery("select conv(b, 2, 2) from `bit`").Check(testkit.Rows(
		"10101",
		"10101",
		"<nil>",
		"1",
		"0",
		"1111111111",
		"1111111111",
		"1111111111",
		"0",
		"0",
		"0",
		"0",
		"100000"))

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
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(mysql.ErrDataOutOfRange), terr.Code())
	require.NoError(t, rs.Close())

	// for round
	result = tk.MustQuery("SELECT ROUND(2.5), ROUND(-2.5), ROUND(25E-1);")
	result.Check(testkit.Rows("3 -3 2"))
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
	result = tk.MustQuery("SELECT ROUND(1e14, 1), ROUND(1e15, 1), ROUND(1e308, 1)")
	result.Check(testkit.Rows("100000000000000 1000000000000000 100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"))
	result = tk.MustQuery("SELECT ROUND(1e-14, 1), ROUND(1e-15, 1), ROUND(1e-308, 1)")
	result.Check(testkit.Rows("0 0 0"))

	// for truncate
	result = tk.MustQuery("SELECT truncate(123, -2), truncate(123, 2), truncate(123, 1), truncate(123, -1);")
	result.Check(testkit.Rows("100 123 123 120"))
	result = tk.MustQuery("SELECT truncate(123.456, -2), truncate(123.456, 2), truncate(123.456, 1), truncate(123.456, 3), truncate(1.23, 100), truncate(123456E-3, 2);")
	result.Check(testkit.Rows("100 123.45 123.4 123.456 1.230000000000000000000000000000 123.45"))
	result = tk.MustQuery("SELECT truncate(9223372036854775807, -7), truncate(9223372036854775808, -10), truncate(cast(-1 as unsigned), -10);")
	result.Check(testkit.Rows("9223372036850000000 9223372030000000000 18446744070000000000"))
	// issue 17181,19390
	tk.MustQuery("select truncate(42, -9223372036854775808);").Check(testkit.Rows("0"))
	tk.MustQuery("select truncate(42, 9223372036854775808);").Check(testkit.Rows("42"))
	tk.MustQuery("select truncate(42, -2147483648);").Check(testkit.Rows("0"))
	tk.MustQuery("select truncate(42, 2147483648);").Check(testkit.Rows("42"))
	tk.MustQuery("select truncate(42, 18446744073709551615);").Check(testkit.Rows("42"))
	tk.MustQuery("select truncate(42, 4294967295);").Check(testkit.Rows("42"))
	tk.MustQuery("select truncate(42, -0);").Check(testkit.Rows("42"))
	tk.MustQuery("select truncate(42, -307);").Check(testkit.Rows("0"))
	tk.MustQuery("select truncate(42, -308);").Check(testkit.Rows("0"))
	tk.MustQuery("select truncate(42, -309);").Check(testkit.Rows("0"))
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec("create table t (a bigint unsigned);")
	tk.MustExec("insert into t values (18446744073709551615), (4294967295), (9223372036854775808), (2147483648);")
	tk.MustQuery("select truncate(42, a) from t;").Check(testkit.Rows("42", "42", "42", "42"))

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

	result = tk.MustQuery(`select truncate(json_array(), 1), truncate("cascasc", 1);`)
	result.Check(testkit.Rows("0 0"))

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
	require.NoError(t, err)
	_, err = session.GetRows4Test(ctx, tk.Session(), rs)
	require.Error(t, err)
	terr = errors.Cause(err).(*terror.Error)
	require.Equal(t, errors.ErrCode(mysql.ErrDataOutOfRange), terr.Code())
	require.NoError(t, rs.Close())

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
	tk.Session().GetSessionVars().MaxChunkSize = 1
	tk.MustQuery("select rand(1) from t").Sort().Check(testkit.Rows("0.1418603212962489", "0.40540353712197724", "0.8716141803857071"))
	tk.MustQuery("select rand(a) from t").Check(testkit.Rows("0.40540353712197724", "0.6555866465490187", "0.9057697559760601"))
	tk.MustQuery("select rand(1), rand(2), rand(3)").Check(testkit.Rows("0.40540353712197724 0.6555866465490187 0.9057697559760601"))
	tk.MustQuery("set @@rand_seed1=10000000,@@rand_seed2=1000000")
	tk.MustQuery("select rand()").Check(testkit.Rows("0.028870999839968048"))
	tk.MustQuery("select rand(1)").Check(testkit.Rows("0.40540353712197724"))
	tk.MustQuery("select rand()").Check(testkit.Rows("0.11641535266900002"))
}

func TestTimeBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	originSQLMode := tk.Session().GetSessionVars().StrictSQLMode
	tk.Session().GetSessionVars().StrictSQLMode = true
	defer func() {
		tk.Session().GetSessionVars().StrictSQLMode = originSQLMode
	}()
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
	result.Check(testkit.Rows("2013 2013 0 2001 <nil> <nil>"))
	result = tk.MustQuery(`select year("2013-00-00"), year("2013-00-00 00:00:00"), year("0000-00-00 12:12:12"), year("2017-00-00 12:12:12");`)
	result.Check(testkit.Rows("2013 2013 0 2017"))
	result = tk.MustQuery(`select year("aa"), year(2013), year(2012.09), year("1-01"), year("-09");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	_, err := tk.Exec(`insert into t select year("aa")`)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue), "err %v", err)
	tk.MustExec(`set sql_mode='STRICT_TRANS_TABLES'`) // without zero date
	tk.MustExec(`insert into t select year("0000-00-00 00:00:00")`)
	tk.MustExec(`set sql_mode="NO_ZERO_DATE";`) // with zero date
	tk.MustExec(`insert into t select year("0000-00-00 00:00:00")`)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`set sql_mode="NO_ZERO_DATE,STRICT_TRANS_TABLES";`)
	_, err = tk.Exec(`insert into t select year("0000-00-00 00:00:00");`)
	require.Error(t, err)
	require.True(t, types.ErrWrongValue.Equal(err), "err %v", err)

	tk.MustExec(`insert into t select 1`)
	tk.MustExec(`set sql_mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";`)
	_, err = tk.Exec(`update t set a = year("aa")`)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue), "err %v", err)
	_, err = tk.Exec(`delete from t where a = year("aa")`)
	// Only `code` can be used to compare because the error `class` information
	// will be lost after expression push-down
	require.Equal(t, types.ErrWrongValue.Code(), errors.Cause(err).(*terror.Error).Code(), "err %v", err)

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
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue), "err: %v", err)
	tk.MustExec(`insert into t select month("0000-00-00 00:00:00")`)
	tk.MustExec(`set sql_mode="NO_ZERO_DATE";`)
	tk.MustExec(`insert into t select month("0000-00-00 00:00:00")`)
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`set sql_mode="NO_ZERO_DATE,STRICT_TRANS_TABLES";`)
	_, err = tk.Exec(`insert into t select month("0000-00-00 00:00:00");`)
	require.Error(t, err)
	require.True(t, types.ErrWrongValue.Equal(err), "err: %v", err)
	tk.MustExec(`insert into t select 1`)
	tk.MustExec(`set sql_mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";`)
	tk.MustExec(`insert into t select 1`)
	_, err = tk.Exec(`update t set a = month("aa")`)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))
	_, err = tk.Exec(`delete from t where a = month("aa")`)
	require.Equal(t, types.ErrWrongValue.Code(), errors.Cause(err).(*terror.Error).Code(), "err %v", err)

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
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))
	tk.MustExec(`insert into t select now()`)
	_, err = tk.Exec(`update t set a = week("aa", 1)`)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))
	_, err = tk.Exec(`delete from t where a = week("aa", 1)`)
	require.Equal(t, types.ErrWrongValue.Code(), errors.Cause(err).(*terror.Error).Code(), "err %v", err)

	// for weekofyear
	result = tk.MustQuery(`select weekofyear("2012-12-22"), weekofyear("2008-02-20"), weekofyear("aa"), weekofyear(null), weekofyear(11), weekofyear(12.99);`)
	result.Check(testkit.Rows("51 8 <nil> <nil> <nil> <nil>"))
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a bigint)`)
	_, err = tk.Exec(`insert into t select weekofyear("aa")`)
	require.Error(t, err)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))

	tk.MustExec(`insert into t select 1`)
	_, err = tk.Exec(`update t set a = weekofyear("aa")`)
	require.True(t, terror.ErrorEqual(err, types.ErrWrongValue))
	_, err = tk.Exec(`delete from t where a = weekofyear("aa")`)
	require.Equal(t, types.ErrWrongValue.Code(), errors.Cause(err).(*terror.Error).Code(), "err %v", err)

	// for weekday
	result = tk.MustQuery(`select weekday("2012-12-20"), weekday("2012-12-21"), weekday("2012-12-22"), weekday("2012-12-23"), weekday("2012-12-24"), weekday("2012-12-25"), weekday("2012-12-26"), weekday("2012-12-27");`)
	result.Check(testkit.Rows("3 4 5 6 0 1 2 3"))
	result = tk.MustQuery(`select weekday("2012-12-90"), weekday("0000-00-00"), weekday("aa"), weekday(null), weekday(11), weekday(12.99);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil> <nil>"))

	// for quarter
	result = tk.MustQuery(`select quarter("2012-00-20"), quarter("2012-01-21"), quarter("2012-03-22"), quarter("2012-05-23"), quarter("2012-08-24"), quarter("2012-09-25"), quarter("2012-11-26"), quarter("2012-12-27");`)
	result.Check(testkit.Rows("0 1 1 2 3 3 4 4"))
	result = tk.MustQuery(`select quarter("2012-14-20"), quarter("aa"), quarter(null), quarter(11), quarter(12.99);`)
	result.Check(testkit.Rows("<nil> <nil> <nil> <nil> <nil>"))
	result = tk.MustQuery(`select quarter("0000-00-00"), quarter("0000-00-00 00:00:00");`)
	result.Check(testkit.Rows("0 0"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	result = tk.MustQuery(`select quarter(0), quarter(0.0), quarter(0e1), quarter(0.00);`)
	result.Check(testkit.Rows("0 0 0 0"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

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
	tk.MustQuery("select timediff(cast('10:10:10' as time), cast('10:10:11' as time))").Check(testkit.Rows("-00:00:01"))

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
	// Issue https://github.com/pingcap/tidb/issues/20003
	result = tk.MustQuery("select timestamp(0.0001, 0.00001);")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select timestamp('2003-12-31', '01:01:01.01'), timestamp('2003-12-31 12:34', '01:01:01.01')," +
		" timestamp('2008-12-31','00:00:00.0'), timestamp('2008-12-31 00:00:00.000');")

	tk.MustQuery(`select timestampadd(second, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2001-01-01 00:00:01"))
	tk.MustQuery(`select timestampadd(hour, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2001-01-01 01:00:00"))
	tk.MustQuery(`select timestampadd(day, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2001-01-02"))
	tk.MustQuery(`select timestampadd(month, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2001-02-01"))
	tk.MustQuery(`select timestampadd(year, 1, cast("2001-01-01" as date))`).Check(testkit.Rows("2002-01-01"))
	tk.MustQuery(`select timestampadd(second, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2001-01-01 00:00:01"))
	tk.MustQuery(`select timestampadd(hour, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2001-01-01 01:00:00"))
	tk.MustQuery(`select timestampadd(day, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2001-01-02 00:00:00"))
	tk.MustQuery(`select timestampadd(month, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2001-02-01 00:00:00"))
	tk.MustQuery(`select timestampadd(year, 1, cast("2001-01-01" as datetime))`).Check(testkit.Rows("2002-01-01 00:00:00"))

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
	tk.MustExec(`insert into t values("2017-01-01 12:30:31", "2017-01-01 12:30:31", "01:01:01")`)
	result = tk.MustQuery("select addtime(a, b), addtime(cast(a as date), b), addtime(b,a), addtime(a,c), addtime(b," +
		"c), addtime(c,a), addtime(c,b)" +
		" from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil> 2017-01-01 13:31:32 2017-01-01 13:31:32 <nil> <nil>"))
	result = tk.MustQuery("select addtime('01:01:11', cast('1' as time))")
	result.Check(testkit.Rows("01:01:12"))
	tk.MustQuery("select addtime(cast(null as char(20)), cast('1' as time))").Check(testkit.Rows("<nil>"))
	require.NoError(t, tk.QueryToErr(`select addtime("01:01:11", cast('sdf' as time))`))
	tk.MustQuery(`select addtime("01:01:11", cast(null as char(20)))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select addtime(cast(1 as time), cast(1 as time))`).Check(testkit.Rows("00:00:02"))
	tk.MustQuery(`select addtime(cast(null as time), cast(1 as time))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select addtime(cast(1 as time), cast(null as time))`).Check(testkit.Rows("<nil>"))

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
	tk.MustQuery("select subtime(cast('10:10:10' as time), cast('9:10:10' as time))").Check(testkit.Rows("01:00:00"))
	tk.MustQuery("select subtime('10:10:10', cast('9:10:10' as time))").Check(testkit.Rows("01:00:00"))

	// SUBTIME issue #31868
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a DATETIME(6))")
	tk.MustExec(`insert into t values ("1000-01-01 01:00:00.000000"), ("1000-01-01 01:00:00.000001")`)
	tk.MustQuery(`SELECT SUBTIME(a, '00:00:00.000001') FROM t ORDER BY a;`).Check(testkit.Rows("1000-01-01 00:59:59.999999", "1000-01-01 01:00:00.000000"))
	tk.MustQuery(`SELECT SUBTIME(a, '10:00:00.000001') FROM t ORDER BY a;`).Check(testkit.Rows("0999-12-31 14:59:59.999999", "0999-12-31 15:00:00.000000"))

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

	result = tk.MustQuery("select addtime(-32073, 0), addtime(0, -32073);")
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select addtime(-32073, c), addtime(c, -32073) from t;")
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select addtime(a, -32073), addtime(b, -32073), addtime(d, -32073) from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))

	result = tk.MustQuery("select subtime(-32073, 0), subtime(0, -32073);")
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select subtime(-32073, c), subtime(c, -32073) from t;")
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))
	result = tk.MustQuery("select subtime(a, -32073), subtime(b, -32073), subtime(d, -32073) from t;")
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'",
		"Warning|1292|Truncated incorrect time value: '-32073'"))

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

	// Issue 20995
	result = tk.MustQuery("select time('0.1234567')")
	result.Check(testkit.Rows("00:00:00.123457"))

	// for hour
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
	result = tk.MustQuery(`SELECT period_add(200807, 2), period_add(200807, -2);`)
	result.Check(testkit.Rows("200809 200805"))
	result = tk.MustQuery(`SELECT period_add(NULL, 2), period_add(-191, NULL), period_add(NULL, NULL), period_add(12.09, -2), period_add("200207aa", "1aa");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> 200010 200208"))
	for _, errPeriod := range []string{
		"period_add(0, 20)", "period_add(0, 0)", "period_add(-1, 1)", "period_add(200013, 1)", "period_add(-200012, 1)", "period_add('', '')",
	} {
		err := tk.QueryToErr(fmt.Sprintf("SELECT %v;", errPeriod))
		require.Error(t, err, "[expression:1210]Incorrect arguments to period_add")
	}

	// for period_diff
	result = tk.MustQuery(`SELECT period_diff(200807, 200705), period_diff(200807, 200908);`)
	result.Check(testkit.Rows("14 -13"))
	result = tk.MustQuery(`SELECT period_diff(NULL, 2), period_diff(-191, NULL), period_diff(NULL, NULL), period_diff(12.09, 2), period_diff("12aa", "11aa");`)
	result.Check(testkit.Rows("<nil> <nil> <nil> 10 1"))
	for _, errPeriod := range []string{
		"period_diff(-00013,1)", "period_diff(00013,1)", "period_diff(0, 0)", "period_diff(200013, 1)", "period_diff(5612, 4513)", "period_diff('', '')",
	} {
		err := tk.QueryToErr(fmt.Sprintf("SELECT %v;", errPeriod))
		require.Error(t, err, "[expression:1210]Incorrect arguments to period_diff")
	}

	// TODO: fix `CAST(xx as duration)` and release the test below:
	// result = tk.MustQuery(`SELECT hour("aaa"), hour(123456), hour(1234567);`)
	// result = tk.MustQuery(`SELECT minute("aaa"), minute(123456), minute(1234567);`)
	// result = tk.MustQuery(`SELECT second("aaa"), second(123456), second(1234567);`)
	// result = tk.MustQuery(`SELECT microsecond("aaa"), microsecond(123456), microsecond(1234567);`)

	// for time_format
	result = tk.MustQuery("SELECT TIME_FORMAT('150:02:28', '%H:%i:%s %p');")
	result.Check(testkit.Rows("150:02:28 AM"))
	result = tk.MustQuery("SELECT TIME_FORMAT('bad string', '%H:%i:%s %p');")
	result.Check(testkit.Rows("<nil>"))
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
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	result = tk.MustQuery(`SELECT DATE_FORMAT('0', '%W %M %e %Y %r %y'), DATE_FORMAT('0.0', '%W %M %e %Y %r %y'), DATE_FORMAT(0, 0);`)
	result.Check(testkit.Rows("<nil> <nil> 0"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect time value: '0'",
		"Warning|1292|Incorrect datetime value: '0.0'"))
	result = tk.MustQuery(`SELECT DATE_FORMAT(0, '%W %M %e %Y %r %y'), DATE_FORMAT(0.0, '%W %M %e %Y %r %y');`)
	result.Check(testkit.Rows("<nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows())

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
	require.True(t, types.ErrWrongValue.Equal(err), "%v", err)
	_, err = tk.Exec(`update t set a = dayOfWeek("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = dayOfWeek(123)`)
	require.NoError(t, err)

	tk.MustExec("insert into t value(dayOfMonth('2017-00-00'))")
	tk.MustExec("insert into t value(dayOfMonth('0000-00-00'))")
	tk.MustExec(`update t set a = dayOfMonth("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE';")
	tk.MustExec("insert into t value(dayOfMonth('0000-00-00'))")
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`update t set a = dayOfMonth("0000-00-00")`)
	tk.MustExec("set sql_mode = 'NO_ZERO_DATE,STRICT_TRANS_TABLES';")
	_, err = tk.Exec("insert into t value(dayOfMonth('0000-00-00'))")
	require.True(t, types.ErrWrongValue.Equal(err))
	tk.MustExec("insert into t value(0)")
	_, err = tk.Exec(`update t set a = dayOfMonth("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = dayOfMonth(123)`)
	require.NoError(t, err)

	_, err = tk.Exec("insert into t value(dayOfYear('0000-00-00'))")
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`update t set a = dayOfYear("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = dayOfYear(123)`)
	require.NoError(t, err)

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
	// result = tk.MustQuery("SELECT UNIX_TIMESTAMP(-1);")
	// result.Check(testkit.Rows("0"))
	// result = tk.MustQuery("SELECT UNIX_TIMESTAMP(12345);")
	// result.Check(testkit.Rows("0"))
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
	result.Check(testkit.Rows("<nil>"))
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
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))
	tk.MustExec(`update t set a = monthname("0000-00-00")`)
	tk.MustExec("set sql_mode = ''")
	tk.MustExec("insert into t value(monthname('0000-00-00'))")
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES,NO_ZERO_DATE'")
	_, err = tk.Exec(`update t set a = monthname("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = monthname(123)`)
	require.NoError(t, err)
	result = tk.MustQuery(`select monthname("2017-12-01"), monthname("0000-00-00"), monthname("0000-01-00"), monthname("0000-01-00 00:00:00")`)
	result.Check(testkit.Rows("December <nil> January January"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|", "Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'"))

	// for dayname
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t(a varchar(10))`)
	tk.MustExec(`insert into t value("abc")`)
	tk.MustExec("set sql_mode = 'STRICT_TRANS_TABLES'")

	_, err = tk.Exec("insert into t value(dayname('0000-00-00'))")
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`update t set a = dayname("0000-00-00")`)
	require.True(t, types.ErrWrongValue.Equal(err))
	_, err = tk.Exec(`delete from t where a = dayname(123)`)
	require.NoError(t, err)
	result = tk.MustQuery(`select dayname("2017-12-01"), dayname("0000-00-00"), dayname("0000-01-00"), dayname("0000-01-00 00:00:00")`)
	result.Check(testkit.Rows("Friday <nil> <nil> <nil>"))
	tk.MustQuery("show warnings").Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00.000000'",
		"Warning|1292|Incorrect datetime value: '0000-01-00 00:00:00.000000'",
		"Warning|1292|Incorrect datetime value: '0000-01-00 00:00:00.000000'"))
	// for dayname implicit cast to boolean and real
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-07')`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-07') is true`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-07') is false`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-08')`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-08') is true`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select 1 from dual where dayname('2016-03-08') is false`)
	result.Check(testkit.Rows())
	result = tk.MustQuery(`select cast(dayname("2016-03-07") as double), cast(dayname("2016-03-08") as double)`)
	result.Check(testkit.Rows("0 1"))

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
	result = tk.MustQuery("show warnings")
	result.Sort().Check(testkit.RowsWithSep("|",
		"Warning|1292|Incorrect datetime value: '0000-00-00 00:00:00'",
		"Warning|1292|Truncated incorrect datetime value: '01-01-2017'"))

	result = tk.MustQuery("select str_to_date('2018-6-1', '%Y-%m-%d'), str_to_date('2018-6-1', '%Y-%c-%d'), str_to_date('59:20:1', '%s:%i:%k'), str_to_date('59:20:1', '%s:%i:%l')")
	result.Check(testkit.Rows("2018-06-01 2018-06-01 01:20:59 01:20:59"))

	result = tk.MustQuery("select str_to_date('2020-07-04 11:22:33 PM c', '%Y-%m-%d %r')")
	result.Check(testkit.Rows("2020-07-04 23:22:33"))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.RowsWithSep("|", "Warning|1292|Truncated incorrect datetime value: '2020-07-04 11:22:33 PM c'"))

	result = tk.MustQuery("select str_to_date('11:22:33 PM', ' %r')")
	result.Check(testkit.Rows("23:22:33"))
	result = tk.MustQuery("show warnings")
	result.Check(testkit.Rows())

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
	result = tk.MustQuery(`select convert_tz(20040101, "+00:00", "+10:32"), convert_tz(20040101.01, "+00:00", "+10:32"), convert_tz(20040101.01234567, "+00:00", "+10:32");`)
	result.Check(testkit.Rows("2004-01-01 10:32:00 2004-01-01 10:32:00.00 2004-01-01 10:32:00.000000"))
	result = tk.MustQuery(`select convert_tz(NULL, "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", NULL, "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", NULL);`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select convert_tz("a", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", "a", "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", "a");`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select convert_tz("", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", "", "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", "");`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))
	result = tk.MustQuery(`select convert_tz("0", "+00:00", "+10:32"), convert_tz("2004-01-01 12:00:00", "0", "+10:32"), convert_tz("2004-01-01 12:00:00", "+00:00", "0");`)
	result.Check(testkit.Rows("<nil> <nil> <nil>"))

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
	result = tk.MustQuery("select extract(day_microsecond from '2017-01-01 12:12:12'), extract(day_microsecond from '01 12:12:12'), extract(day_microsecond from '12:12:12'), extract(day_microsecond from '01 00:00:00.89')")
	result.Check(testkit.Rows("1121212000000 361212000000 121212000000 240000890000"))
	result = tk.MustQuery("select extract(day_second from '2017-01-01 12:12:12'), extract(day_second from '01 12:12:12'), extract(day_second from '12:12:12'), extract(day_second from '01 00:00:00.89')")
	result.Check(testkit.Rows("1121212 361212 121212 240000"))
	result = tk.MustQuery("select extract(day_minute from '2017-01-01 12:12:12'), extract(day_minute from '01 12:12:12'), extract(day_minute from '12:12:12'), extract(day_minute from '01 00:00:00.89')")
	result.Check(testkit.Rows("11212 3612 1212 2400"))
	result = tk.MustQuery("select extract(day_hour from '2017-01-01 12:12:12'), extract(day_hour from '01 12:12:12'), extract(day_hour from '12:12:12'), extract(day_hour from '01 00:00:00.89')")
	result.Check(testkit.Rows("112 36 12 24"))
	result = tk.MustQuery("select extract(day_microsecond from cast('2017-01-01 12:12:12' as datetime)), extract(day_second from cast('2017-01-01 12:12:12' as datetime)), extract(day_minute from cast('2017-01-01 12:12:12' as datetime)), extract(day_hour from cast('2017-01-01 12:12:12' as datetime))")
	result.Check(testkit.Rows("1121212000000 1121212 11212 112"))

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

		{"\"2011-11-11\"", "\"abc1000\"", "MICROSECOND", "2011-11-11 00:00:00", "2011-11-11 00:00:00"},
		{"\"20111111 10:10:10\"", "\"1\"", "DAY", "<nil>", "<nil>"},
		{"\"2011-11-11\"", "\"10\"", "SECOND_MICROSECOND", "2011-11-11 00:00:00.100000", "2011-11-10 23:59:59.900000"},
		{"\"2011-11-11\"", "\"10.0000\"", "MINUTE_MICROSECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},
		{"\"2011-11-11\"", "\"10:10:10\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.100000", "2011-11-10 23:49:49.900000"},

		{"cast(\"2011-11-11\" as datetime)", "\"10:10:10\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.100000", "2011-11-10 23:49:49.900000"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "1", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "10", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"1\"", "DAY", "2011-11-12 00:00:00", "2011-11-10 00:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "HOUR", "2011-11-11 10:00:00", "2011-11-10 14:00:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "MINUTE", "2011-11-11 00:10:00", "2011-11-10 23:50:00"},
		{"cast(\"2011-11-11 00:00:00\" as datetime)", "\"10\"", "SECOND", "2011-11-11 00:00:10", "2011-11-10 23:59:50"},

		{"cast(\"2011-11-11\" as date)", "\"10:10:10\"", "MINUTE_MICROSECOND", "2011-11-11 00:10:10.100000", "2011-11-10 23:49:49.900000"},
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
		// for issue #8077
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
	tk.MustQuery(`select subdate(cast("2000-02-01" as datetime), cast(1 as decimal))`).Check(testkit.Rows("2000-01-31 00:00:00"))
	tk.MustQuery(`select subdate(cast("2000-02-01" as datetime), cast(null as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast(null as datetime), cast(1 as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast("2000-02-01" as datetime), cast("xxx" as decimal))`).Check(testkit.Rows("2000-02-01 00:00:00"))
	tk.MustQuery(`select subdate(cast("xxx" as datetime), cast(1 as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast(20000101 as SIGNED), cast("1" as decimal))`).Check(testkit.Rows("1999-12-31"))
	tk.MustQuery(`select subdate(cast(20000101 as SIGNED), cast("xxx" as decimal))`).Check(testkit.Rows("2000-01-01"))
	tk.MustQuery(`select subdate(cast("abc" as SIGNED), cast("1" as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast(null as SIGNED), cast("1" as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select subdate(cast(20000101 as SIGNED), cast(null as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(1 as decimal))`).Check(testkit.Rows("2000-02-02 00:00:00"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(null as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast(null as datetime), cast(1 as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast("xxx" as decimal))`).Check(testkit.Rows("2000-02-01 00:00:00"))
	tk.MustQuery(`select adddate(cast("xxx" as datetime), cast(1 as decimal))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(1 as SIGNED))`).Check(testkit.Rows("2000-02-02 00:00:00"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast(null as SIGNED))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast(null as datetime), cast(1 as SIGNED))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(cast("2000-02-01" as datetime), cast("xxx" as SIGNED))`).Check(testkit.Rows("2000-02-01 00:00:00"))
	tk.MustQuery(`select adddate(cast("xxx" as datetime), cast(1 as SIGNED))`).Check(testkit.Rows("<nil>"))
	tk.MustQuery(`select adddate(20100101, cast(1 as decimal))`).Check(testkit.Rows("2010-01-02"))
	tk.MustQuery(`select adddate(cast('10:10:10' as time), 1)`).Check(testkit.Rows("34:10:10"))
	tk.MustQuery(`select adddate(cast('10:10:10' as time), cast(1 as decimal))`).Check(testkit.Rows("34:10:10"))

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

	// for tidb_bounded_staleness
	tk.MustExec("SET time_zone = '+00:00';")
	tt := time.Now().UTC()
	ts := oracle.GoTimeToTS(tt)
	tidbBoundedStalenessTests := []struct {
		sql          string
		injectSafeTS uint64
		expect       string
	}{
		{
			sql:          `select tidb_bounded_staleness(DATE_SUB(NOW(), INTERVAL 600 SECOND), DATE_ADD(NOW(), INTERVAL 600 SECOND))`,
			injectSafeTS: ts,
			expect:       tt.Format(types.TimeFSPFormat[:len(types.TimeFSPFormat)-3]),
		},
		{
			sql: `select tidb_bounded_staleness("2021-04-27 12:00:00.000", "2021-04-27 13:00:00.000")`,
			injectSafeTS: func() uint64 {
				tt, err := time.Parse("2006-01-02 15:04:05.000", "2021-04-27 13:30:04.877")
				require.NoError(t, err)
				return oracle.GoTimeToTS(tt)
			}(),
			expect: "2021-04-27 13:00:00.000",
		},
		{
			sql: `select tidb_bounded_staleness("2021-04-27 12:00:00.000", "2021-04-27 13:00:00.000")`,
			injectSafeTS: func() uint64 {
				tt, err := time.Parse("2006-01-02 15:04:05.000", "2021-04-27 11:30:04.877")
				require.NoError(t, err)
				return oracle.GoTimeToTS(tt)
			}(),
			expect: "2021-04-27 12:00:00.000",
		},
		{
			sql:          `select tidb_bounded_staleness("2021-04-27 12:00:00.000", "2021-04-27 11:00:00.000")`,
			injectSafeTS: 0,
			expect:       "<nil>",
		},
		// Time is too small.
		{
			sql:          `select tidb_bounded_staleness("0020-04-27 12:00:00.000", "2021-04-27 11:00:00.000")`,
			injectSafeTS: 0,
			expect:       "1970-01-01 00:00:00.000",
		},
		// Wrong value.
		{
			sql:          `select tidb_bounded_staleness(1, 2)`,
			injectSafeTS: 0,
			expect:       "<nil>",
		},
		{
			sql:          `select tidb_bounded_staleness("invalid_time_1", "invalid_time_2")`,
			injectSafeTS: 0,
			expect:       "<nil>",
		},
	}
	for _, test := range tidbBoundedStalenessTests {
		require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/injectSafeTS",
			fmt.Sprintf("return(%v)", test.injectSafeTS)))
		tk.MustQuery(test.sql).Check(testkit.Rows(test.expect))
	}
	failpoint.Disable("github.com/pingcap/tidb/expression/injectSafeTS")
	// test whether tidb_bounded_staleness is deterministic
	result = tk.MustQuery(`select tidb_bounded_staleness(NOW(), DATE_ADD(NOW(), INTERVAL 600 SECOND)), tidb_bounded_staleness(NOW(), DATE_ADD(NOW(), INTERVAL 600 SECOND))`)
	require.Len(t, result.Rows()[0], 2)
	require.Equal(t, result.Rows()[0][0], result.Rows()[0][1])
	preResult := result.Rows()[0][0]
	time.Sleep(time.Second)
	result = tk.MustQuery(`select tidb_bounded_staleness(NOW(), DATE_ADD(NOW(), INTERVAL 600 SECOND)), tidb_bounded_staleness(NOW(), DATE_ADD(NOW(), INTERVAL 600 SECOND))`)
	require.Len(t, result.Rows()[0], 2)
	require.Equal(t, result.Rows()[0][0], result.Rows()[0][1])
	require.NotEqual(t, preResult, result.Rows()[0][0])

	// fix issue 10308
	result = tk.MustQuery("select time(\"- -\");")
	result.Check(testkit.Rows("00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect time value: '- -'"))
	result = tk.MustQuery("select time(\"---1\");")
	result.Check(testkit.Rows("00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect time value: '---1'"))
	result = tk.MustQuery("select time(\"-- --1\");")
	result.Check(testkit.Rows("00:00:00"))
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1292 Truncated incorrect time value: '-- --1'"))

	// fix issue #15185
	result = tk.MustQuery(`select timestamp(11111.1111)`)
	result.Check(testkit.Rows("2001-11-11 00:00:00.0000"))
	result = tk.MustQuery(`select timestamp(cast(11111.1111 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2001-11-11 00:00:00.00000"))
	result = tk.MustQuery(`select timestamp(1021121141105.4324)`)
	result.Check(testkit.Rows("0102-11-21 14:11:05.4324"))
	result = tk.MustQuery(`select timestamp(cast(1021121141105.4324 as decimal(60, 5)))`)
	result.Check(testkit.Rows("0102-11-21 14:11:05.43240"))
	result = tk.MustQuery(`select timestamp(21121141105.101)`)
	result.Check(testkit.Rows("2002-11-21 14:11:05.101"))
	result = tk.MustQuery(`select timestamp(cast(21121141105.101 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2002-11-21 14:11:05.10100"))
	result = tk.MustQuery(`select timestamp(1121141105.799055)`)
	result.Check(testkit.Rows("2000-11-21 14:11:05.799055"))
	result = tk.MustQuery(`select timestamp(cast(1121141105.799055 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2000-11-21 14:11:05.79906"))
	result = tk.MustQuery(`select timestamp(121141105.123)`)
	result.Check(testkit.Rows("2000-01-21 14:11:05.123"))
	result = tk.MustQuery(`select timestamp(cast(121141105.123 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2000-01-21 14:11:05.12300"))
	result = tk.MustQuery(`select timestamp(1141105)`)
	result.Check(testkit.Rows("0114-11-05 00:00:00"))
	result = tk.MustQuery(`select timestamp(cast(1141105 as decimal(60, 5)))`)
	result.Check(testkit.Rows("0114-11-05 00:00:00.00000"))
	result = tk.MustQuery(`select timestamp(41105.11)`)
	result.Check(testkit.Rows("2004-11-05 00:00:00.00"))
	result = tk.MustQuery(`select timestamp(cast(41105.11 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2004-11-05 00:00:00.00000"))
	result = tk.MustQuery(`select timestamp(1105.3)`)
	result.Check(testkit.Rows("2000-11-05 00:00:00.0"))
	result = tk.MustQuery(`select timestamp(cast(1105.3 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2000-11-05 00:00:00.00000"))
	result = tk.MustQuery(`select timestamp(105)`)
	result.Check(testkit.Rows("2000-01-05 00:00:00"))
	result = tk.MustQuery(`select timestamp(cast(105 as decimal(60, 5)))`)
	result.Check(testkit.Rows("2000-01-05 00:00:00.00000"))
}

func TestBuiltin(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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
	// Issue https://github.com/pingcap/tidb/issues/19986
	result = tk.MustQuery("select 1 from dual where sec_to_time(2/10) is true")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select 1 from dual where sec_to_time(2/10) is false")
	result.Check(nil)
	// Issue https://github.com/pingcap/tidb/issues/19999
	result = tk.MustQuery("select 1 from dual where timediff((7/'2014-07-07 02:30:02'),'2012-01-16') is true")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select 1 from dual where timediff((7/'2014-07-07 02:30:02'),'2012-01-16') is false")
	result.Check(nil)
	// Issue https://github.com/pingcap/tidb/issues/20001
	result = tk.MustQuery("select 1 from dual where time(0.0001) is true")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select 1 from dual where time(0.0001) is false")
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
	tk.MustQuery(`select convert(a2.a, unsigned int) from (select cast('"9223372036854775808"' as json) as a) as a2;`)

	tk.MustExec(`create table tb5(a bigint(64) unsigned, b double);`)
	tk.MustExec(`insert into tb5 (a, b) values (9223372036854776000, 9223372036854776000);`)
	tk.MustExec(`insert into tb5 (a, b) select * from (select cast(a as json) as a1, b from tb5) as t where t.a1 = t.b;`)
	tk.MustExec(`drop table tb5;`)

	tk.MustExec(`create table tb5(a float(53));`)
	tk.MustExec(`insert into tb5(a) values (13835058055282163712);`)
	tk.MustQuery(`select convert(t.a1, signed int) from (select convert(a, json) as a1 from tb5) as t`)
	tk.MustExec(`drop table tb5;`)

	// test builtinCastIntAsIntSig
	// Cast MaxUint64 to unsigned should be -1
	tk.MustQuery("select cast(0xffffffffffffffff as signed);").Check(testkit.Rows("-1"))
	tk.MustQuery("select cast(0x9999999999999999999999999999999999999999999 as signed);").Check(testkit.Rows("-1"))
	tk.MustExec("create table tb5(a bigint);")
	tk.MustExec("set sql_mode=''")
	tk.MustExec("insert into tb5(a) values (0xfffffffffffffffffffffffff);")
	tk.MustQuery("select * from tb5;").Check(testkit.Rows("9223372036854775807"))
	tk.MustExec("drop table tb5;")

	tk.MustExec(`create table tb5(a double);`)
	tk.MustExec(`insert into test.tb5 (a) values (18446744073709551616);`)
	tk.MustExec(`insert into test.tb5 (a) values (184467440737095516160);`)
	result = tk.MustQuery(`select cast(a as unsigned) from test.tb5;`)
	// Note: MySQL will return 9223372036854775807, and it should be a bug.
	result.Check(testkit.Rows("18446744073709551615", "18446744073709551615"))
	tk.MustExec(`drop table tb5;`)

	// test builtinCastIntAsDecimalSig
	tk.MustExec(`create table tb5(a bigint(64) unsigned, b decimal(64, 10));`)
	tk.MustExec(`insert into tb5 (a, b) values (9223372036854775808, 9223372036854775808);`)
	tk.MustExec(`insert into tb5 (select * from tb5 where a = b);`)
	result = tk.MustQuery(`select * from tb5;`)
	result.Check(testkit.Rows("9223372036854775808 9223372036854775808.0000000000", "9223372036854775808 9223372036854775808.0000000000"))
	tk.MustExec(`drop table tb5;`)

	// test builtinCastIntAsRealSig
	tk.MustExec(`create table tb5(a bigint(64) unsigned, b double(64, 10));`)
	tk.MustExec(`insert into tb5 (a, b) values (13835058000000000000, 13835058000000000000);`)
	tk.MustExec(`insert into tb5 (select * from tb5 where a = b);`)
	result = tk.MustQuery(`select * from tb5;`)
	result.Check(testkit.Rows("13835058000000000000 13835058000000000000", "13835058000000000000 13835058000000000000"))
	tk.MustExec(`drop table tb5;`)

	// test builtinCastRealAsIntSig
	tk.MustExec(`create table tb5(a double, b float);`)
	tk.MustExec(`insert into tb5 (a, b) values (184467440737095516160, 184467440737095516160);`)
	tk.MustQuery(`select * from tb5 where cast(a as unsigned int)=0;`).Check(testkit.Rows())
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1690 constant 1.844674407370955e+20 overflows bigint"))
	_ = tk.MustQuery(`select * from tb5 where cast(b as unsigned int)=0;`)
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1690 constant 1.844674407370955e+20 overflows bigint"))
	tk.MustExec(`drop table tb5;`)
	tk.MustExec(`create table tb5(a double, b bigint unsigned);`)
	tk.MustExec(`insert into tb5 (a, b) values (18446744073709551616, 18446744073709551615);`)
	_ = tk.MustQuery(`select * from tb5 where cast(a as unsigned int)=b;`)
	// TODO `obtained string = "[18446744073709552000 18446744073709551615]`
	// result.Check(testkit.Rows("18446744073709551616 18446744073709551615"))
	tk.MustQuery("show warnings;").Check(testkit.Rows())
	tk.MustExec(`drop table tb5;`)

	// test builtinCastJSONAsIntSig
	tk.MustExec(`create table tb5(a json, b bigint unsigned);`)
	tk.MustExec(`insert into tb5 (a, b) values ('184467440737095516160', 18446744073709551615);`)
	_ = tk.MustQuery(`select * from tb5 where cast(a as unsigned int)=b;`)
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1690 constant 1.844674407370955e+20 overflows bigint"))
	_ = tk.MustQuery(`select * from tb5 where cast(b as unsigned int)=0;`)
	tk.MustQuery("show warnings;").Check(testkit.Rows())
	tk.MustExec(`drop table tb5;`)
	tk.MustExec(`create table tb5(a json, b bigint unsigned);`)
	tk.MustExec(`insert into tb5 (a, b) values ('92233720368547758080', 18446744073709551615);`)
	_ = tk.MustQuery(`select * from tb5 where cast(a as signed int)=b;`)
	tk.MustQuery("show warnings;").Check(testkit.Rows("Warning 1690 constant 9.223372036854776e+19 overflows bigint"))
	tk.MustExec(`drop table tb5;`)

	// test builtinCastIntAsStringSig
	tk.MustExec(`create table tb5(a bigint(64) unsigned,b varchar(50));`)
	tk.MustExec(`insert into tb5(a, b) values (9223372036854775808, '9223372036854775808');`)
	tk.MustExec(`insert into tb5(select * from tb5 where a = b);`)
	result = tk.MustQuery(`select * from tb5;`)
	result.Check(testkit.Rows("9223372036854775808 9223372036854775808", "9223372036854775808 9223372036854775808"))
	tk.MustExec(`drop table tb5;`)

	// test builtinCastIntAsDecimalSig
	tk.MustExec(`drop table if exists tb5`)
	tk.MustExec(`create table tb5 (a decimal(65), b bigint(64) unsigned);`)
	tk.MustExec(`insert into tb5 (a, b) values (9223372036854775808, 9223372036854775808);`)
	result = tk.MustQuery(`select cast(b as decimal(64)) from tb5 union all select b from tb5;`)
	result.Check(testkit.Rows("9223372036854775808", "9223372036854775808"))
	tk.MustExec(`drop table tb5`)

	// test builtinCastIntAsRealSig
	tk.MustExec(`drop table if exists tb5`)
	tk.MustExec(`create table tb5 (a bigint(64) unsigned, b double(64, 10));`)
	tk.MustExec(`insert into tb5 (a, b) values (9223372036854775808, 9223372036854775808);`)
	result = tk.MustQuery(`select a from tb5 where a = b union all select b from tb5;`)
	result.Check(testkit.Rows("9223372036854776000", "9223372036854776000"))
	tk.MustExec(`drop table tb5`)

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
		require.NoError(t, err)
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
		require.Error(t, err, fmt.Sprintf("[types:1292]Truncated incorrect time value: '%s'", invalidTime))
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

	// test cast as double
	result = tk.MustQuery("select cast(1 as double)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(cast(12345 as unsigned) as double)")
	result.Check(testkit.Rows("12345"))
	result = tk.MustQuery("select cast(1.1 as double)")
	result.Check(testkit.Rows("1.1"))
	result = tk.MustQuery("select cast(-1.1 as double)")
	result.Check(testkit.Rows("-1.1"))
	result = tk.MustQuery("select cast('123.321' as double)")
	result.Check(testkit.Rows("123.321"))
	result = tk.MustQuery("select cast('12345678901234567890' as double) = 1.2345678901234567e19")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(-1 as double)")
	result.Check(testkit.Rows("-1"))
	result = tk.MustQuery("select cast(null as double)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select cast(12345678901234567890 as double) = 1.2345678901234567e19")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(cast(-1 as unsigned) as double) = 1.8446744073709552e19")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(1e100 as double) = 1e100")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(123456789012345678901234567890 as double) = 1.2345678901234568e29")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(0x12345678 as double)")
	result.Check(testkit.Rows("305419896"))

	// test cast as float
	result = tk.MustQuery("select cast(1 as float)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(cast(12345 as unsigned) as float)")
	result.Check(testkit.Rows("12345"))
	result = tk.MustQuery("select cast(1.1 as float) = 1.1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(-1.1 as float) = -1.1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast('123.321' as float) =123.321")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast('12345678901234567890' as float) = 1.2345678901234567e19")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(-1 as float)")
	result.Check(testkit.Rows("-1"))
	result = tk.MustQuery("select cast(null as float)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select cast(12345678901234567890 as float) = 1.2345678901234567e19")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(cast(-1 as unsigned) as float) = 1.8446744073709552e19")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(1e100 as float(40)) = 1e100")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(123456789012345678901234567890 as float(40)) = 1.2345678901234568e29")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(0x12345678 as float(40)) = 305419896")
	result.Check(testkit.Rows("1"))

	// test cast as real
	result = tk.MustQuery("select cast(1 as real)")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(cast(12345 as unsigned) as real)")
	result.Check(testkit.Rows("12345"))
	result = tk.MustQuery("select cast(1.1 as real) = 1.1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(-1.1 as real) = -1.1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast('123.321' as real) =123.321")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast('12345678901234567890' as real) = 1.2345678901234567e19")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(-1 as real)")
	result.Check(testkit.Rows("-1"))
	result = tk.MustQuery("select cast(null as real)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select cast(12345678901234567890 as real) = 1.2345678901234567e19")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(cast(-1 as unsigned) as real) = 1.8446744073709552e19")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(1e100 as real) = 1e100")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(123456789012345678901234567890 as real) = 1.2345678901234568e29")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select cast(0x12345678 as real) = 305419896")
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
	require.Error(t, err)

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
	require.Error(t, err)

	// test case decimal precision less than the scale.
	_, err = tk.Exec("select cast(12.1 as decimal(3, 4));")
	require.Error(t, err)
	require.Error(t, err, "[types:1427]For float(M,D), double(M,D) or decimal(M,D), M must be >= D (column '12.1').")

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
	// NOTE (#17013): make from_unixtime stable in different timezone: the result of from_unixtime
	// depends on the local time zone of the test environment, thus the result checking must
	// consider the time zone convert.
	tz := tk.Session().GetSessionVars().StmtCtx.TimeZone
	result = tk.MustQuery("select from_unixtime(1451606400)")
	unixTime := time.Unix(1451606400, 0).In(tz).String()[:19]
	result.Check(testkit.Rows(unixTime))
	result = tk.MustQuery("select from_unixtime(14516064000/10)")
	result.Check(testkit.Rows(fmt.Sprintf("%s.0000", unixTime)))
	result = tk.MustQuery("select from_unixtime('14516064000'/10)")
	result.Check(testkit.Rows(fmt.Sprintf("%s.000000", unixTime)))
	result = tk.MustQuery("select from_unixtime(cast(1451606400 as double))")
	result.Check(testkit.Rows(fmt.Sprintf("%s.000000", unixTime)))
	result = tk.MustQuery("select from_unixtime(cast(cast(1451606400 as double) as DECIMAL))")
	result.Check(testkit.Rows(unixTime))
	result = tk.MustQuery("select from_unixtime(cast(cast(1451606400 as double) as DECIMAL(65,1)))")
	result.Check(testkit.Rows(fmt.Sprintf("%s.0", unixTime)))
	result = tk.MustQuery("select from_unixtime(1451606400.123456)")
	unixTime = time.Unix(1451606400, 123456000).In(tz).String()[:26]
	result.Check(testkit.Rows(unixTime))
	result = tk.MustQuery("select from_unixtime(1451606400.1234567)")
	unixTime = time.Unix(1451606400, 123456700).In(tz).Round(time.Microsecond).Format("2006-01-02 15:04:05.000000")[:26]
	result.Check(testkit.Rows(unixTime))
	result = tk.MustQuery("select from_unixtime(1451606400.999999)")
	unixTime = time.Unix(1451606400, 999999000).In(tz).String()[:26]
	result.Check(testkit.Rows(unixTime))
	result = tk.MustQuery("select from_unixtime(1511247196661)")
	result.Check(testkit.Rows("<nil>"))
	result = tk.MustQuery("select from_unixtime('1451606400.123');")
	unixTime = time.Unix(1451606400, 0).In(tz).String()[:19]
	result.Check(testkit.Rows(fmt.Sprintf("%s.123000", unixTime)))

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t value(1451606400);")
	result = tk.MustQuery("select from_unixtime(a) from t;")
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
	tk.MustExec("insert t values (null, 4)")
	result = tk.MustQuery("select * from t where b < case a when null then 0 when 'str2' then 0 else 9 end")
	result.Check(testkit.Rows("<nil> 4"))
	result = tk.MustQuery("select * from t where b = case when a is null then 4 when  a = 'str5' then 7 else 9 end")
	result.Check(testkit.Rows("<nil> 4"))
	result = tk.MustQuery(`SELECT -Max(+23) * -+Cast(--10 AS SIGNED) * -CASE
                                               WHEN 0 > 85 THEN NULL
                                               WHEN NOT
              CASE +55
                WHEN +( +82 ) + -89 * -69 THEN +Count(-88)
                WHEN +CASE 57
                        WHEN +89 THEN -89 * Count(*)
                        WHEN 17 THEN NULL
                      END THEN ( -10 )
              END IS NULL THEN NULL
                                               ELSE 83 + 48
                                             END AS col0; `)
	result.Check(testkit.Rows("-30130"))

	// return type of case when expr should not include NotNullFlag. issue-23036
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int not null)")
	tk.MustExec("insert into t1 values(1)")
	result = tk.MustQuery("select (case when null then c1 end) is null from t1")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (case when null then c1 end) is not null from t1")
	result.Check(testkit.Rows("0"))

	// test warnings
	tk.MustQuery("select case when b=0 then 1 else 1/b end from t")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select if(b=0, 1, 1/b) from t")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select ifnull(b, b/0) from t")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("select case when 1 then 1 else 1/0 end")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery(" select if(1,1,1/0)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select ifnull(1, 1/0)")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustExec("delete from t")
	tk.MustExec("insert t values ('str2', 0)")
	tk.MustQuery("select case when b < 1 then 1 else 1/0 end from t")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select case when b < 1 then 1 when 1/0 then b else 1/0 end from t")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select if(b < 1 , 1, 1/0) from t")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select ifnull(b, 1/0) from t")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select COALESCE(1, b, b/0) from t")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select 0 and b/0 from t")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select 1 or b/0 from t")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("select 1 or 1/0")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select 0 and 1/0")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select COALESCE(1, 1/0)")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select interval(1,0,1,2,1/0)")
	tk.MustQuery("show warnings").Check(testkit.Rows())

	tk.MustQuery("select case 2.0 when 2.0 then 3.0 when 3.0 then 2.0 end").Check(testkit.Rows("3.0"))
	tk.MustQuery("select case 2.0 when 3.0 then 2.0 when 4.0 then 3.0 else 5.0 end").Check(testkit.Rows("5.0"))
	tk.MustQuery("select case cast('2011-01-01' as date) when cast('2011-01-01' as date) then cast('2011-02-02' as date) end").Check(testkit.Rows("2011-02-02"))
	tk.MustQuery("select case cast('2012-01-01' as date) when cast('2011-01-01' as date) then cast('2011-02-02' as date) else cast('2011-03-03' as date) end").Check(testkit.Rows("2011-03-03"))
	tk.MustQuery("select case cast('10:10:10' as time) when cast('10:10:10' as time) then cast('11:11:11' as time) end").Check(testkit.Rows("11:11:11"))
	tk.MustQuery("select case cast('10:10:13' as time) when cast('10:10:10' as time) then cast('11:11:11' as time) else cast('22:22:22' as time) end").Check(testkit.Rows("22:22:22"))

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
	err = tk.ExecToErr("select char(97, null, 100, 256, 89 using tidb)")
	require.Error(t, err, "[parser:1115]Unknown character set: 'tidb'")

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
	result = tk.MustQuery(`select a regexp 'xt' from t;`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select b regexp 'xt' from t;`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select b regexp binary 'Xt' from t;`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select c regexp 'Xt' from t;`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select d regexp 'Xt' from t;`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select a rlike 'xt' from t;`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select a rlike binary 'Xt' from t;`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select b rlike 'xt' from t;`)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery(`select c rlike 'Xt' from t;`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select d rlike 'Xt' from t;`)
	result.Check(testkit.Rows("0"))
	result = tk.MustQuery(`select 'a' regexp 'A', 'a' regexp binary 'A'`)
	result.Check(testkit.Rows("0 0"))

	// testCase is for like and regexp
	type testCase struct {
		pattern string
		val     string
		result  int
	}
	patternMatching := func(tk *testkit.TestKit, queryOp string, data []testCase) {
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
	patternMatching(tk, "like", likeTests)
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
	patternMatching(tk, "regexp", likeTests)

	// for #9838
	result = tk.MustQuery("select cast(1 as signed) + cast(9223372036854775807 as unsigned);")
	result.Check(testkit.Rows("9223372036854775808"))
	result = tk.MustQuery("select cast(9223372036854775807 as unsigned) + cast(1 as signed);")
	result.Check(testkit.Rows("9223372036854775808"))
	err = tk.QueryToErr("select cast(9223372036854775807 as signed) + cast(9223372036854775809 as unsigned);")
	require.Error(t, err)
	err = tk.QueryToErr("select cast(9223372036854775809 as unsigned) + cast(9223372036854775807 as signed);")
	require.Error(t, err)
	err = tk.QueryToErr("select cast(-9223372036854775807 as signed) + cast(9223372036854775806 as unsigned);")
	require.Error(t, err)
	err = tk.QueryToErr("select cast(9223372036854775806 as unsigned) + cast(-9223372036854775807 as signed);")
	require.Error(t, err)

	result = tk.MustQuery(`select 1 / '2007' div 1;`)
	result.Check(testkit.Rows("0"))
}

func TestSetVariables(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	_, err := tk.Exec("set sql_mode='adfasdfadsfdasd';")
	require.Error(t, err)
	_, err = tk.Exec("set @@sql_mode='adfasdfadsfdasd';")
	require.Error(t, err)
	_, err = tk.Exec("set @@global.sql_mode='adfasdfadsfdasd';")
	require.Error(t, err)
	_, err = tk.Exec("set @@session.sql_mode='adfasdfadsfdasd';")
	require.Error(t, err)

	var r *testkit.Result
	_, err = tk.Exec("set @@session.sql_mode=',NO_ZERO_DATE,ANSI,ANSI_QUOTES';")
	require.NoError(t, err)
	r = tk.MustQuery(`select @@session.sql_mode`)
	r.Check(testkit.Rows("NO_ZERO_DATE,REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"))
	r = tk.MustQuery(`show variables like 'sql_mode'`)
	r.Check(testkit.Rows("sql_mode NO_ZERO_DATE,REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ONLY_FULL_GROUP_BY,ANSI"))

	// for invalid SQL mode.
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tab0")
	tk.MustExec("CREATE TABLE tab0(col1 time)")
	_, err = tk.Exec("set sql_mode='STRICT_TRANS_TABLES';")
	require.NoError(t, err)
	_, err = tk.Exec("INSERT INTO tab0 select cast('999:44:33' as time);")
	require.Error(t, err)
	require.Error(t, err, "[types:1292]Truncated incorrect time value: '999:44:33'")
	_, err = tk.Exec("set sql_mode=' ,';")
	require.Error(t, err)
	_, err = tk.Exec("INSERT INTO tab0 select cast('999:44:33' as time);")
	require.Error(t, err)
	require.Error(t, err, "[types:1292]Truncated incorrect time value: '999:44:33'")

	// issue #5478
	_, err = tk.Exec("set session transaction read write;")
	require.NoError(t, err)
	_, err = tk.Exec("set global transaction read write;")
	require.NoError(t, err)
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("0 0 0 0"))

	_, err = tk.Exec("set session transaction read only;")
	require.Error(t, err)

	_, err = tk.Exec("start transaction read only;")
	require.Error(t, err)

	_, err = tk.Exec("set tidb_enable_noop_functions=1")
	require.NoError(t, err)

	tk.MustExec("set session transaction read only;")
	tk.MustExec("start transaction read only;")

	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("1 0 1 0"))
	_, err = tk.Exec("set global transaction read only;")
	require.Error(t, err)
	tk.MustExec("set global tidb_enable_noop_functions=1;")
	tk.MustExec("set global transaction read only;")
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("1 1 1 1"))

	_, err = tk.Exec("set session transaction read write;")
	require.NoError(t, err)
	_, err = tk.Exec("set global transaction read write;")
	require.NoError(t, err)
	r = tk.MustQuery(`select @@session.tx_read_only, @@global.tx_read_only, @@session.transaction_read_only, @@global.transaction_read_only;`)
	r.Check(testkit.Rows("0 0 0 0"))

	// reset
	tk.MustExec("set tidb_enable_noop_functions=0")
	tk.MustExec("set global tidb_enable_noop_functions=1")

	_, err = tk.Exec("set @@global.max_user_connections='';")
	require.Error(t, err)
	require.Error(t, err, variable.ErrWrongTypeForVar.GenWithStackByArgs("max_user_connections").Error())
	_, err = tk.Exec("set @@global.max_prepared_stmt_count='';")
	require.Error(t, err)
	require.Error(t, err, variable.ErrWrongTypeForVar.GenWithStackByArgs("max_prepared_stmt_count").Error())
}

func TestPreparePlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)
	// Use the example from the docs https://docs.pingcap.com/tidb/stable/sql-prepare-plan-cache
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("prepare stmt from 'select * from t where a = ?';")
	tk.MustExec("set @a = 1;")
	tk.MustExec("execute stmt using @a;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec("execute stmt using @a;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
}

func TestPreparePlanCacheOnCachedTable(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	plannercore.SetPreparedPlanCache(true)
	require.True(t, plannercore.PreparedPlanCacheEnabled())

	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	tk.MustExec("alter table t cache")

	var readFromTableCache bool
	for i := 0; i < 50; i++ {
		tk.MustQuery("select * from t where a = 1")
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			readFromTableCache = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	require.True(t, readFromTableCache)
	// already read cache after reading first time
	tk.MustExec("prepare stmt from 'select * from t where a = ?';")
	tk.MustExec("set @a = 1;")
	tk.MustExec("execute stmt using @a;")
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec("execute stmt using @a;")
	readFromTableCache = tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache
	require.True(t, readFromTableCache)
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
}

func TestIssue16205(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("prepare stmt from 'select random_bytes(3)'")
	rows1 := tk.MustQuery("execute stmt").Rows()
	require.Len(t, rows1, 1)
	rows2 := tk.MustQuery("execute stmt").Rows()
	require.Len(t, rows2, 1)
	require.NotEqual(t, rows1[0][0].(string), rows2[0][0].(string))
}

func TestRowCountPlanCache(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int auto_increment primary key)")
	tk.MustExec("prepare stmt from 'select row_count()';")
	tk.MustExec("insert into t values()")
	res := tk.MustQuery("execute stmt").Rows()
	require.Len(t, res, 1)
	require.Equal(t, "1", res[0][0])
	tk.MustExec("insert into t values(),(),()")
	res = tk.MustQuery("execute stmt").Rows()
	require.Len(t, res, 1)
	require.Equal(t, "3", res[0][0])
}

func TestCacheRegexpr(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a varchar(40))")
	tk.MustExec("insert into t1 values ('C1'),('R1')")
	tk.MustExec("prepare stmt1 from 'select a from t1 where a rlike ?'")
	tk.MustExec("set @a='^C.*'")
	tk.MustQuery("execute stmt1 using @a").Check(testkit.Rows("C1"))
	tk.MustExec("set @a='^R.*'")
	tk.MustQuery("execute stmt1 using @a").Check(testkit.Rows("R1"))
}

func TestCacheRefineArgs(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(col_int int)")
	tk.MustExec("insert into t values(null)")
	tk.MustExec("prepare stmt from 'SELECT ((col_int is true) = ?) AS res FROM t'")
	tk.MustExec("set @p0='0.8'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows("0"))
	tk.MustExec("set @p0='0'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt from 'SELECT UCASE(?) < col_int from t;';")
	tk.MustExec("set @a1 = 'xayh7vrWVNqZtzlJmdJQUwAHnkI8Ec';")
	tk.MustQuery("execute stmt using @a1;").Check(testkit.Rows("<nil>"))

	tk.MustExec("delete from t")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("prepare stmt from 'SELECT col_int < ? FROM t'")
	tk.MustExec("set @p0='-184467440737095516167.1'")
	tk.MustQuery("execute stmt using @p0").Check(testkit.Rows("0"))
}

func TestCacheConstEval(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	orgEnable := plannercore.PreparedPlanCacheEnabled()
	defer func() {
		plannercore.SetPreparedPlanCache(orgEnable)
	}()
	plannercore.SetPreparedPlanCache(true)
	var err error
	se, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	require.NoError(t, err)
	tk.SetSession(se)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(col_double double)")
	tk.MustExec("insert into t values (1)")
	tk.Session().GetSessionVars().EnableVectorizedExpression = false
	tk.MustExec("insert into mysql.expr_pushdown_blacklist values('cast', 'tikv,tiflash,tidb', 'for test')")
	tk.MustExec("admin reload expr_pushdown_blacklist")
	tk.MustExec("prepare stmt from 'SELECT * FROM (SELECT col_double AS c0 FROM t) t WHERE (ABS((REPEAT(?, ?) OR 5617780767323292672)) < LN(EXP(c0)) + (? ^ ?))'")
	tk.MustExec("set @a1 = 'JuvkBX7ykVux20zQlkwDK2DFelgn7'")
	tk.MustExec("set @a2 = 1")
	tk.MustExec("set @a3 = -112990.35179796701")
	tk.MustExec("set @a4 = 87997.92704840179")
	// Main purpose here is checking no error is reported. 1 is the result when plan cache is disabled, it is
	// incompatible with MySQL actually, update the result after fixing it.
	tk.MustQuery("execute stmt using @a1, @a2, @a3, @a4").Check(testkit.Rows("1"))
	tk.Session().GetSessionVars().EnableVectorizedExpression = true
	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name = 'cast' and store_type = 'tikv,tiflash,tidb' and reason = 'for test'")
	tk.MustExec("admin reload expr_pushdown_blacklist")
}

func TestIssue24502(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t0,t1;")
	tk.MustExec("create table t0(col1 varchar(255));")
	tk.MustExec("create table t1(col1 int(11));")
	tk.MustExec(`insert into t0 values("\\9jM\\M");`)
	tk.MustExec(`insert into t1 values(0);`)
	tk.MustExec(`insert into t1 values(null);`)
	tk.MustExec(`insert into t1 values(null);`)

	tk.MustQuery(`select t0.col1, t1.col1 from t0 left join t1 on t0.col1 not like t0.col1;`).
		Check(testkit.Rows(`\9jM\M <nil>`, `\9jM\M <nil>`, `\9jM\M 0`))

	tk.MustQuery(`select 'a' like '\\a'`).Check(testkit.Rows("1"))
	tk.MustQuery(`select 'a' like '+a' escape '+'`).Check(testkit.Rows("1"))
}

func TestIssue17233(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists table_int")
	tk.MustExec(`CREATE TABLE table_int (
	  id_0 int(16) NOT NULL AUTO_INCREMENT,
	  col_int_0 int(16) DEFAULT NULL,
	  PRIMARY KEY (id_0),
	  KEY fvclc (id_0,col_int_0));`)
	tk.MustExec("INSERT INTO table_int VALUES (1,NULL),(2,NULL),(3,65535),(4,1),(5,0),(6,NULL),(7,-1),(8,65535),(9,NULL),(10,65535),(11,-1),(12,0),(13,-1),(14,1),(15,65535),(16,0),(17,1),(18,0),(19,0)")

	tk.MustExec("drop table if exists table_varchar")
	tk.MustExec(`CREATE TABLE table_varchar (
	  id_2 int(16) NOT NULL AUTO_INCREMENT,
	  col_varchar_2 varchar(511) DEFAULT NULL,
	  PRIMARY KEY (id_2));`)
	tk.MustExec(`INSERT INTO table_varchar VALUES (1,''),(2,''),(3,''),(4,''),(5,''),(6,''),(7,''),(8,''),(9,''),(10,''),(11,''),(12,'');`)

	tk.MustExec("drop table if exists table_float_varchar")
	tk.MustExec(`CREATE TABLE table_int_float_varchar (
	  id_6 int(16) NOT NULL AUTO_INCREMENT,
	  col_int_6 int(16) NOT NULL,
	  col_float_6 float DEFAULT NULL,
	  col_varchar_6 varchar(511) DEFAULT NULL,
	  PRIMARY KEY (id_6,col_int_6)
	)
	PARTITION BY RANGE ( col_int_6 ) (
	  PARTITION p0 VALUES LESS THAN (1),
	  PARTITION p2 VALUES LESS THAN (1000),
	  PARTITION p3 VALUES LESS THAN (10000),
	  PARTITION p5 VALUES LESS THAN (1000000),
	  PARTITION p7 VALUES LESS THAN (100000000),
	  PARTITION p9 VALUES LESS THAN (10000000000),
	  PARTITION p10 VALUES LESS THAN (100000000000),
	  PARTITION pn VALUES LESS THAN (MAXVALUE));`)
	tk.MustExec(`INSERT INTO table_int_float_varchar VALUES (1,-1,0.1,'0000-00-00 00:00:00'),(2,0,0,NULL),(3,-1,1,NULL),(4,0,NULL,NULL),(7,0,0.5,NULL),(8,0,0,NULL),(10,-1,0,'-1'),(5,1,-0.1,NULL),(6,1,0.1,NULL),(9,65535,0,'1');`)

	tk.MustExec("drop table if exists table_float")
	tk.MustExec(`CREATE TABLE table_float (
	  id_1 int(16) NOT NULL AUTO_INCREMENT,
	  col_float_1 float DEFAULT NULL,
	  PRIMARY KEY (id_1),
	  KEY zbjus (id_1,col_float_1));`)
	tk.MustExec(`INSERT INTO table_float VALUES (1,NULL),(2,-0.1),(3,-1),(4,NULL),(5,-0.1),(6,0),(7,0),(8,-1),(9,NULL),(10,NULL),(11,0.1),(12,-1);`)

	tk.MustExec("drop view if exists view_4")
	tk.MustExec(`CREATE DEFINER='root'@'127.0.0.1' VIEW view_4 (col_1, col_2, col_3, col_4, col_5, col_6, col_7, col_8, col_9, col_10) AS
    SELECT /*+ USE_INDEX(table_int fvclc, fvclc)*/
        tmp1.id_6 AS col_1,
        tmp1.col_int_6 AS col_2,
        tmp1.col_float_6 AS col_3,
        tmp1.col_varchar_6 AS col_4,
        tmp2.id_2 AS col_5,
        tmp2.col_varchar_2 AS col_6,
        tmp3.id_0 AS col_7,
        tmp3.col_int_0 AS col_8,
        tmp4.id_1 AS col_9,
        tmp4.col_float_1 AS col_10
    FROM ((
            test.table_int_float_varchar AS tmp1 LEFT JOIN
            test.table_varchar AS tmp2 ON ((NULL<=tmp2.col_varchar_2)) IS NULL
        ) JOIN
        test.table_int AS tmp3 ON (1.117853833115198e-03!=tmp1.col_int_6))
    JOIN
        test.table_float AS tmp4 ON !((1900370398268920328=0e+00)) WHERE ((''<='{Gm~PcZNb') OR (tmp2.id_2 OR tmp3.col_int_0)) ORDER BY col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8,col_9,col_10 LIMIT 20580,5;`)

	tk.MustExec("drop view if exists view_10")
	tk.MustExec(`CREATE DEFINER='root'@'127.0.0.1' VIEW view_10 (col_1, col_2) AS
    SELECT  table_int.id_0 AS col_1,
            table_int.col_int_0 AS col_2
    FROM test.table_int
    WHERE
        ((-1e+00=1) OR (0e+00>=table_int.col_int_0))
    ORDER BY col_1,col_2
    LIMIT 5,9;`)

	tk.MustQuery("SELECT col_1 FROM test.view_10").Sort().Check(testkit.Rows("16", "18", "19"))
	tk.MustQuery("SELECT col_1 FROM test.view_4").Sort().Check(testkit.Rows("8", "8", "8", "8", "8"))
	tk.MustQuery("SELECT view_10.col_1 FROM view_4 JOIN view_10").Check(testkit.Rows("16", "16", "16", "16", "16", "18", "18", "18", "18", "18", "19", "19", "19", "19", "19"))
}

func TestIssue17989(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b tinyint as(a+1), c int as(b+1));")
	tk.MustExec("set sql_mode='';")
	tk.MustExec("insert into t(a) values(2000);")
	tk.MustExec("create index idx on t(c);")
	tk.MustQuery("select c from t;").Check(testkit.Rows("128"))
	tk.MustExec("admin check table t")
}

func TestNullValueRange(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index(a))")
	tk.MustExec("insert into t values (null, 0), (null, 1), (10, 11), (10, 12)")
	tk.MustQuery("select * from t use index(a) where a is null order by b").Check(testkit.Rows("<nil> 0", "<nil> 1"))
	tk.MustQuery("select * from t use index(a) where a<=>null order by b").Check(testkit.Rows("<nil> 0", "<nil> 1"))
	tk.MustQuery("select * from t use index(a) where a<=>10 order by b").Check(testkit.Rows("10 11", "10 12"))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int, c int, unique key(a, b, c))")
	tk.MustExec("insert into t1 values (1, null, 1), (1, null, 2), (1, null, 3), (1, null, 4)")
	tk.MustExec("insert into t1 values (1, 1, 1), (1, 2, 2), (1, 3, 33), (1, 4, 44)")
	tk.MustQuery("select c from t1 where a=1 and b<=>null and c>2 order by c").Check(testkit.Rows("3", "4"))
	tk.MustQuery("select c from t1 where a=1 and b is null and c>2 order by c").Check(testkit.Rows("3", "4"))
	tk.MustQuery("select c from t1 where a=1 and b is not null and c>2 order by c").Check(testkit.Rows("33", "44"))
}

func TestIssue18652(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1 ( `pk` int not null primary key auto_increment, `col_smallint_key_signed` smallint  , key (`col_smallint_key_signed`))")
	tk.MustExec("INSERT INTO `t1` VALUES (1,0),(2,NULL),(3,NULL),(4,0),(5,0),(6,NULL),(7,NULL),(8,0),(9,0),(10,0)")
	tk.MustQuery("SELECT * FROM t1 WHERE ( LOG( `col_smallint_key_signed`, -8297584758403770424 ) ) DIV 1").Check(testkit.Rows())
}

func TestIssue19045(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1, t2")
	tk.MustExec(`CREATE TABLE t (
  id int(11) NOT NULL AUTO_INCREMENT,
  a char(10) DEFAULT NULL,
  PRIMARY KEY (id)
);`)
	tk.MustExec(`CREATE TABLE t1 (
  id int(11) NOT NULL AUTO_INCREMENT,
  a char(10) DEFAULT NULL,
  b char(10) DEFAULT NULL,
  c char(10) DEFAULT NULL,
  PRIMARY KEY (id)
);`)
	tk.MustExec(`CREATE TABLE t2 (
  id int(11) NOT NULL AUTO_INCREMENT,
  a char(10) DEFAULT NULL,
  b char(10) DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY b (b)
);`)
	tk.MustExec(`insert into t1(a,b,c) values('hs4_0004', "04", "101"), ('a01', "01", "101"),('a011', "02", "101");`)
	tk.MustExec(`insert into t2(a,b) values("02","03");`)
	tk.MustExec(`insert into t(a) values('101'),('101');`)
	tk.MustQuery(`select  ( SELECT t1.a FROM  t1,  t2 WHERE t1.b = t2.a AND  t2.b = '03' AND t1.c = a.a) invode from t a ;`).Check(testkit.Rows("a011", "a011"))
}

// issues 14448, 19383, 17734
func TestNoopFunctions(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t1")
	tk.MustExec("CREATE TABLE t1 (a INT NOT NULL PRIMARY KEY)")
	tk.MustExec("INSERT INTO t1 VALUES (1),(2),(3)")

	message := `.* has only noop implementation in tidb now, use tidb_enable_noop_functions to enable these functions`
	stmts := []string{
		"SELECT SQL_CALC_FOUND_ROWS * FROM t1 LIMIT 1",
		"SELECT * FROM t1 LOCK IN SHARE MODE",
		"SELECT * FROM t1 GROUP BY a DESC",
		"SELECT * FROM t1 GROUP BY a ASC",
	}

	for _, stmt := range stmts {
		// test on
		tk.MustExec("SET tidb_enable_noop_functions='ON'")
		tk.MustExec(stmt)
		// test warning
		tk.MustExec("SET tidb_enable_noop_functions='WARN'")
		tk.MustExec(stmt)
		warn := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		require.Regexp(t, message, warn[0].Err.Error())
		// test off
		tk.MustExec("SET tidb_enable_noop_functions='OFF'")
		_, err := tk.Exec(stmt)
		require.Regexp(t, message, err.Error())
	}

	// These statements return a different error message
	// to the above. Test for error, not specifically the message.
	// After they execute, we need to reset the values because
	// otherwise tidb_enable_noop_functions can't be changed.

	stmts = []string{
		"START TRANSACTION READ ONLY",
		"SET TRANSACTION READ ONLY",
		"SET tx_read_only = 1",
		"SET transaction_read_only = 1",
	}

	for _, stmt := range stmts {
		// test off
		tk.MustExec("SET tidb_enable_noop_functions='OFF'")
		_, err := tk.Exec(stmt)
		require.Error(t, err)
		// test warning
		tk.MustExec("SET tidb_enable_noop_functions='WARN'")
		tk.MustExec(stmt)
		warn := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
		require.Len(t, warn, 1)
		// test on
		tk.MustExec("SET tidb_enable_noop_functions='ON'")
		tk.MustExec(stmt)

		// Reset (required for future loop iterations and future tests)
		tk.MustExec("SET tx_read_only = 0")
		tk.MustExec("SET transaction_read_only = 0")
	}
}

func TestIssue19315(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE `t` (`a` bit(10) DEFAULT NULL,`b` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("INSERT INTO `t` VALUES (_binary '\\0',1),(_binary '\\0',2),(_binary '\\0',5),(_binary '\\0',4),(_binary '\\0',2),(_binary '\\0	',4)")
	tk.MustExec("CREATE TABLE `t1` (`a` int(11) DEFAULT NULL, `b` int(11) DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin")
	tk.MustExec("INSERT INTO `t1` VALUES (1,1),(1,5),(2,3),(2,4),(3,3)")
	err := tk.QueryToErr("select * from t where t.b > (select min(t1.b) from t1 where t1.a > t.a)")
	require.NoError(t, err)
}

func TestIssue18674(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select -1.0 % -1.0").Check(testkit.Rows("0.0"))
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(`pk` int primary key,`col_float_key_signed` float  ,key (`col_float_key_signed`))")
	tk.MustExec("insert into t1 values (0, null), (1, 0), (2, -0), (3, 1), (-1,-1)")
	tk.MustQuery("select * from t1 where ( `col_float_key_signed` % `col_float_key_signed`) IS FALSE").Sort().Check(testkit.Rows("-1 -1", "3 1"))
	tk.MustQuery("select  `col_float_key_signed` , `col_float_key_signed` % `col_float_key_signed` from t1").Sort().Check(testkit.Rows(
		"-1 -0", "0 <nil>", "0 <nil>", "1 0", "<nil> <nil>"))
	tk.MustQuery("select  `col_float_key_signed` , (`col_float_key_signed` % `col_float_key_signed`) IS FALSE from t1").Sort().Check(testkit.Rows(
		"-1 1", "0 0", "0 0", "1 1", "<nil> 0"))
}

func TestJsonObjectCompare(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustQuery("select json_object('k', -1) > json_object('k', 2)").Check(testkit.Rows("0"))
	tk.MustQuery("select json_object('k', -1) < json_object('k', 2)").Check(testkit.Rows("1"))

	tk.MustExec("drop table if exists tx")
	tk.MustExec("create table tx(a double, b int)")
	tk.MustExec("insert into tx values (3.0, 3)")
	tk.MustQuery("select json_object('k', a) = json_object('k', b) from tx").Check(testkit.Rows("1"))
}

func TestIssue21290(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(a date);")
	tk.MustExec("insert into t1 values (20100202);")
	tk.MustQuery("select a in ('2020-02-02', 20100202) from t1;").Check(testkit.Rows("1"))
}

// for issue 20128
func TestIssue20128(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(b enum('a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z') DEFAULT NULL, c decimal(40,20));")
	tk.MustExec("insert into t values('z', 19.18040000000000000000);")
	tk.MustExec("insert into t values('z', 26.18040000000000000000);")
	tk.MustExec("insert into t values('z', 25.18040000000000000000);")
	tk.MustQuery("select * from t where t.b > t.c;").Check(testkit.Rows("z 19.18040000000000000000", "z 25.18040000000000000000"))
	tk.MustQuery("select * from t where t.b < t.c;").Check(testkit.Rows("z 26.18040000000000000000"))
}

func TestCrossDCQuery(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop placement policy if exists p1")
	tk.MustExec("drop placement policy if exists p2")
	tk.MustExec("create placement policy p1 leader_constraints='[+zone=sh]'")
	tk.MustExec("create placement policy p2 leader_constraints='[+zone=bj]'")
	tk.MustExec(`create table t1 (c int primary key, d int,e int,index idx_d(d),index idx_e(e))
PARTITION BY RANGE (c) (
	PARTITION p0 VALUES LESS THAN (6) placement policy p1,
	PARTITION p1 VALUES LESS THAN (11) placement policy p2
);`)
	defer func() {
		tk.MustExec("drop table if exists t1")
		tk.MustExec("drop placement policy if exists p1")
		tk.MustExec("drop placement policy if exists p2")
	}()

	tk.MustExec(`insert into t1 (c,d,e) values (1,1,1);`)
	tk.MustExec(`insert into t1 (c,d,e) values (2,3,5);`)
	tk.MustExec(`insert into t1 (c,d,e) values (3,5,7);`)

	testcases := []struct {
		name      string
		txnScope  string
		zone      string
		sql       string
		expectErr error
	}{
		// FIXME: block by https://github.com/pingcap/tidb/issues/21872
		//{
		//	name:      "cross dc read to sh by holding bj, IndexReader",
		//	txnScope:  "bj",
		//	sql:       "select /*+ USE_INDEX(t1, idx_d) */ d from t1 where c < 5 and d < 1;",
		//	expectErr: fmt.Errorf(".*can not be read by.*"),
		//},
		// FIXME: block by https://github.com/pingcap/tidb/issues/21847
		//{
		//	name:      "cross dc read to sh by holding bj, BatchPointGet",
		//	txnScope:  "bj",
		//	sql:       "select * from t1 where c in (1,2,3,4);",
		//	expectErr: fmt.Errorf(".*can not be read by.*"),
		//},
		{
			name:      "cross dc read to sh by holding bj, PointGet",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select * from t1 where c = 1",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "cross dc read to sh by holding bj, IndexLookUp",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select * from t1 use index (idx_d) where c < 5 and d < 5;",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "cross dc read to sh by holding bj, IndexMerge",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select /*+ USE_INDEX_MERGE(t1, idx_d, idx_e) */ * from t1 where c <5 and (d =5 or e=5);",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "cross dc read to sh by holding bj, TableReader",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select * from t1 where c < 6",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "cross dc read to global by holding bj",
			txnScope:  "local",
			zone:      "bj",
			sql:       "select * from t1",
			expectErr: fmt.Errorf(".*can not be read by.*"),
		},
		{
			name:      "read sh dc by holding sh",
			txnScope:  "local",
			zone:      "sh",
			sql:       "select * from t1 where c < 6",
			expectErr: nil,
		},
		{
			name:      "read sh dc by holding global",
			txnScope:  "global",
			zone:      "",
			sql:       "select * from t1 where c < 6",
			expectErr: nil,
		},
	}
	tk.MustExec("set global tidb_enable_local_txn = on;")
	for _, testcase := range testcases {
		t.Log(testcase.name)
		require.NoError(t, failpoint.Enable("tikvclient/injectTxnScope",
			fmt.Sprintf(`return("%v")`, testcase.zone)))
		tk.MustExec(fmt.Sprintf("set @@txn_scope='%v'", testcase.txnScope))
		tk.Exec("begin")
		res, err := tk.Exec(testcase.sql)
		_, resErr := session.GetRows4Test(context.Background(), tk.Session(), res)
		var checkErr error
		if err != nil {
			checkErr = err
		} else {
			checkErr = resErr
		}
		if testcase.expectErr != nil {
			require.Error(t, checkErr)
			require.Regexp(t, ".*can not be read by.*", checkErr.Error())
		} else {
			require.NoError(t, checkErr)
		}
		if res != nil {
			res.Close()
		}
		tk.Exec("commit")
	}
	require.NoError(t, failpoint.Disable("tikvclient/injectTxnScope"))
	tk.MustExec("set global tidb_enable_local_txn = off;")
}

func TestCollationUnion2(t *testing.T) {
	// For issue 22179
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(10))")
	tk.MustExec("insert into t values('aaaaaaaaa'),('天王盖地虎宝塔镇河妖')")
	tk.MustQuery("select * from t").Check(testkit.Rows("aaaaaaaaa", "天王盖地虎宝塔镇河妖"))

	// check the collation of sub query of union statement.
	tk.MustQuery("select collation(a) from (select null as a) aaa").Check(testkit.Rows("binary"))
	tk.MustQuery("select collation(a) from (select a from t limit 1) aaa").Check(testkit.Rows("utf8mb4_bin"))

	// Reverse sub query of union statement.
	tk.MustQuery("select * from (select null as a union all select a from t) aaa order by a").Check(testkit.Rows("<nil>", "aaaaaaaaa", "天王盖地虎宝塔镇河妖"))
	tk.MustQuery("select * from (select a from t) aaa union all select null as a order by a").Check(testkit.Rows("<nil>", "aaaaaaaaa", "天王盖地虎宝塔镇河妖"))
	tk.MustExec("drop table if exists t")
}

func TestPartitionPruningRelaxOP(t *testing.T) {
	// Discovered while looking at issue 19941 (not completely related)
	// relaxOP relax the op > to >= and < to <=
	// Sometime we need to relax the condition, for example:
	// col < const => f(col) <= const
	// datetime < 2020-02-11 16:18:42 => to_days(datetime) <= to_days(2020-02-11)
	// We can't say:
	// datetime < 2020-02-11 16:18:42 => to_days(datetime) < to_days(2020-02-11)

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec(`CREATE TABLE t1 (d date NOT NULL) PARTITION BY RANGE (YEAR(d))
	 (PARTITION p2016 VALUES LESS THAN (2017), PARTITION p2017 VALUES LESS THAN (2018), PARTITION p2018 VALUES LESS THAN (2019),
	 PARTITION p2019 VALUES LESS THAN (2020), PARTITION pmax VALUES LESS THAN MAXVALUE)`)

	tk.MustExec(`INSERT INTO t1 VALUES ('2016-01-01'), ('2016-06-01'), ('2016-09-01'), ('2017-01-01'),
	('2017-06-01'), ('2017-09-01'), ('2018-01-01'), ('2018-06-01'), ('2018-09-01'), ('2018-10-01'),
	('2018-11-01'), ('2018-12-01'), ('2018-12-31'), ('2019-01-01'), ('2019-06-01'), ('2019-09-01'),
	('2020-01-01'), ('2020-06-01'), ('2020-09-01');`)

	tk.MustQuery("SELECT COUNT(*) FROM t1 WHERE d < '2018-01-01'").Check(testkit.Rows("6"))
	tk.MustQuery("SELECT COUNT(*) FROM t1 WHERE d > '2018-01-01'").Check(testkit.Rows("12"))
}
