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
	"fmt"
	"math"
	"strings"
	"testing"

	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/testutil"
	"github.com/stretchr/testify/require"
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
	session, err := session.CreateSession4TestWithOpt(store, &session.Opt{
		PreparedPlanCache: kvcache.NewSimpleLRUCache(100, 0.1, math.MaxUint64),
	})
	tk.SetSession(session)
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
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("1"))
}

func TestIssue17891(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int, value set ('a','b','c') charset utf8mb4 collate utf8mb4_bin default 'a,b ');")
	tk.MustExec("drop table t")
	tk.MustExec("create table test(id int, value set ('a','b','c') charset utf8mb4 collate utf8mb4_general_ci default 'a,B ,C');")
}

func TestIssue20268(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
}

func TestWeightString(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	prepare4Join2(tk)
	tk.MustQuery("select /*+ TIDB_SMJ(t1, t2) */ * from t1, t2 where t1.v=t2.v order by t1.id").Check(
		testkit.Rows("1 a a", "2 À À", "3 á á", "4 à à", "5 b b", "6 c c", "7    "))
}

func TestCollateIndexMergeJoin(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	tk.MustQuery("select locate('S', 's' collate utf8mb4_unicode_ci);").Check(testkit.Rows("1"))
	tk.MustQuery("select locate('S', 'a' collate utf8mb4_unicode_ci);").Check(testkit.Rows("0"))
	tk.MustQuery("select locate('ß', 'ss' collate utf8mb4_unicode_ci);").Check(testkit.Rows("1"))

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database t;")
	tk.MustExec("use t;")
	tk.MustExec("drop database t;")
}

func TestNewCollationWithClusterIndex(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test;`)
	tk.MustExec(`set @@character_set_client=utf8mb4;`)
	tk.MustExec(`set @@collation_connection=utf8_bin;`)
	tk.MustExec("CREATE VIEW tview_1 AS SELECT 'a' AS `id`;")
}

func TestIssue18949(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	tk.MustQuery("select * from t ").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))

	tk.MustQuery("select * from t use index(a)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(ua)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(b)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(ub)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(c)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(uc)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(d)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(ud)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(e)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(ue)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(f)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(uf)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(g)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))
	tk.MustQuery("select * from t use index(ug)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  "))

	tk.MustExec("alter table t add column h varchar(10) collate utf8mb4_general_ci default '🐸'")
	tk.MustExec("alter table t add column i varchar(10) collate utf8mb4_general_ci default '🐸'")
	tk.MustExec("alter table t add index h(h)")
	tk.MustExec("alter table t add unique index uh(h)")

	tk.MustQuery("select * from t use index(h)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(uh)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))

	// Double read.
	tk.MustQuery("select * from t use index(a)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(ua)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(b)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(ub)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(c)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(uc)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(d)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(ud)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(e)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustQuery("select * from t use index(ue)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸"))
	tk.MustExec("admin check table t")
	tk.MustExec("admin recover index t a")
	tk.MustExec("alter table t add column n char(10) COLLATE utf8mb4_unicode_ci")
	tk.MustExec("alter table t add index n(n)")
	tk.MustExec("update t set n = '吧';")
	tk.MustQuery("select * from t").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸,吧"))
	tk.MustQuery("select * from t use index(n)").Check(testutil.RowsWithSep(",", "1,啊,啊,啊  ,啊  ,啊,啊  ,🐸,🐸,吧"))
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select '䇇Հ' collate utf8mb4_bin like '___Հ';").Check(testkit.Rows("0"))
}

func TestIssue20161(t *testing.T) {
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

	tk.MustQuery(`select 'a' like 'A' collate utf8mb4_unicode_ci;`).Check(testkit.Rows("1"))
	tk.MustGetErrMsg(`select 'a' collate utf8mb4_bin like 'A' collate utf8mb4_unicode_ci;`, "[expression:1267]Illegal mix of collations (utf8mb4_bin,EXPLICIT) and (utf8mb4_unicode_ci,EXPLICIT) for operation 'like'")
	tk.MustQuery(`select '😛' collate utf8mb4_general_ci like '😋';`).Check(testkit.Rows("1"))
	tk.MustQuery(`select '😛' collate utf8mb4_general_ci = '😋';`).Check(testkit.Rows("1"))
	tk.MustQuery(`select '😛' collate utf8mb4_unicode_ci like '😋';`).Check(testkit.Rows("0"))
	tk.MustQuery(`select '😛' collate utf8mb4_unicode_ci = '😋';`).Check(testkit.Rows("1"))
}

func TestCollationUnion(t *testing.T) {
	// For issue 19694.
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustQuery("select cast('2010-09-09' as date) a union select  '2010-09-09  ' order by a;").Check(testkit.Rows("2010-09-09", "2010-09-09  "))
	res := tk.MustQuery("select cast('2010-09-09' as date) a union select  '2010-09-09  ';")
	require.Len(t, res.Rows(), 2)
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	res = tk.MustQuery("select cast('2010-09-09' as date) a union select  '2010-09-09  ';")
	require.Len(t, res.Rows(), 1)
}

func TestCollationPrefixClusteredIndex(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)

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
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
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

func TestCollationForBinaryLiteral(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	collate.SetNewCollationEnabledForTest(true)
	defer collate.SetNewCollationEnabledForTest(false)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE t (`COL1` tinyblob NOT NULL,  `COL2` binary(1) NOT NULL,  `COL3` bigint(11) NOT NULL,  PRIMARY KEY (`COL1`(5),`COL2`,`COL3`) /*T![clustered_index] CLUSTERED */)")
	tk.MustExec("insert into t values(0x1E,0xEC,6966939640596047133);")
	tk.MustQuery("select * from t where col1 not in (0x1B,0x20) order by col1").Check(testkit.Rows("\x1e \xec 6966939640596047133"))
	tk.MustExec("drop table t")
}
