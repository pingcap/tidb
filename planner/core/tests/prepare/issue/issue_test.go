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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package issue

import (
	"testing"

	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestIssue31280(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists UK_MU15569;")
	tk.MustExec("CREATE TABLE `UK_MU15569` (" +
		"`COL1` varbinary(20) DEFAULT NULL," +
		"`COL2` bit(16) DEFAULT NULL," +
		"`COL3` time DEFAULT NULL," +
		"`COL4` int(11) DEFAULT NULL," +
		"UNIQUE KEY `U_M_COL4` (`COL1`(10),`COL2`)," +
		"UNIQUE KEY `U_M_COL5` (`COL3`,`COL2`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into UK_MU15569  values(0x1C4FDBA09B42D999AC3019B6A9C0C787FBA08446, 0xCA74, '-836:46:08', 735655453);")

	tk.MustExec(`prepare stmt from 'select * from UK_MU15569 where col2 >= ? and col1 is not null and col3 = ?;';`)

	tk.MustExec("set @a=-32373, @b='545:50:46.85487';")
	// The tableDual plan can not be cached.
	res := tk.MustQuery("execute stmt using @a,@b;")
	require.Len(t, res.Rows(), 0)

	tk.MustExec("set @a=-27225, @b='-836:46:08';")
	res = tk.MustQuery("execute stmt using @a,@b;")
	require.Len(t, res.Rows(), 1)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a,@b;")
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	res1 := tk.MustQuery("select * from UK_MU15569 where col2 >= -27225 and col1 is not null and col3 = '-836:46:08';")
	require.Equal(t, res1.Rows(), res.Rows())
}

func TestIssue31375(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists IDT_MULTI15844STROBJSTROBJ;")
	tk.MustExec("CREATE TABLE `IDT_MULTI15844STROBJSTROBJ` (" +
		"`COL1` enum('bb','aa') DEFAULT NULL," +
		"`COL2` smallint(41) DEFAULT NULL," +
		"`COL3` year(4) DEFAULT NULL," +
		"KEY `U_M_COL4` (`COL1`,`COL2`)," +
		"KEY `U_M_COL5` (`COL3`,`COL2`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into IDT_MULTI15844STROBJSTROBJ values('bb', -16994, 1987);")

	tk.MustExec(`prepare stmt from 'SELECT/*+ HASH_JOIN(t1, t2) */ t2.* FROM IDT_MULTI15844STROBJSTROBJ t1 LEFT JOIN IDT_MULTI15844STROBJSTROBJ t2 ON t1.col1 = t2.col1 WHERE t1.col2 BETWEEN ? AND ? AND t1.col1 >= ?;';`)

	tk.MustExec("set @a=752400293960, @b=258241896853, @c='none';")
	// The tableDual plan can not be cached.
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows())

	tk.MustExec("set @a=-170756280585, @b=3756, @c='aa';")
	tk.MustQuery("execute stmt using @a,@b,@c;").Check(testkit.Rows("bb -16994 1987"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a,@b,@c;")
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestIssue29303(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec(`set tidb_enable_clustered_index=on`)
	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists PK_MULTI_COL_360`)
	tk.MustExec(`CREATE TABLE PK_MULTI_COL_360 (
	  COL1 blob NOT NULL,
	  COL2 char(1) NOT NULL,
	  PRIMARY KEY (COL1(5),COL2) /*T![clustered_index] CLUSTERED */)`)
	tk.MustExec(`INSERT INTO PK_MULTI_COL_360 VALUES 	('�', '龂')`)
	tk.MustExec(`prepare stmt from 'SELECT/*+ INL_JOIN(t1, t2) */ * FROM PK_MULTI_COL_360 t1 JOIN PK_MULTI_COL_360 t2 ON t1.col1 = t2.col1 WHERE t2.col2 BETWEEN ? AND ? AND t1.col2 BETWEEN ? AND ?'`)
	tk.MustExec(`set @a="捲", @b="颽", @c="睭", @d="詼"`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows())
	tk.MustExec(`set @a="龂", @b="龂", @c="龂", @d="龂"`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows("� 龂 � 龂"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // unsafe range
}

func TestIssue34725(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`CREATE TABLE t (
		a int(11) DEFAULT NULL,
		b int(11) GENERATED ALWAYS AS (a) STORED NOT NULL,
		PRIMARY KEY (b))`)
	tk.MustExec(`insert into t(a) values(102)`)
	tk.MustExec(`prepare stmt from "select * from t where b in (?, ?, ?)"`)
	tk.MustExec(`set @a=102, @b=102, @c=102`)
	tk.MustQuery(`execute stmt using @a,@b,@c`).Check(testkit.Rows(`102 102`))
	tk.MustExec(`set @a=-97, @b=-97, @c=-97`)
	tk.MustQuery(`execute stmt using @a,@b,@c`).Check(testkit.Rows())
}

func TestIssue33628(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=0`)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (a int primary key, b int)`)
	tk.MustExec(`prepare stmt from "select * from t where a=10"`) // point-get plan
	tk.MustExec(`execute stmt`)
	tk.MustExec(`execute stmt`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func TestIssue28942(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists IDT_MULTI15853STROBJSTROBJ`)
	tk.MustExec(`
	CREATE TABLE IDT_MULTI15853STROBJSTROBJ (
	  COL1 enum('aa','bb','cc') DEFAULT NULL,
	  COL2 mediumint(41) DEFAULT NULL,
	  KEY U_M_COL4 (COL1,COL2)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	tk.MustExec(`insert into IDT_MULTI15853STROBJSTROBJ values("aa", 1)`)
	tk.MustExec(`prepare stmt from 'SELECT * FROM IDT_MULTI15853STROBJSTROBJ WHERE col1 = ? AND col1 != ?'`)
	tk.MustExec(`set @a="mm", @b="aa"`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows()) // empty result
	tk.MustExec(`set @a="aa", @b="aa"`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows()) // empty result
}

func TestIssue28254(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists PK_GCOL_STORED9816")
	tk.MustExec("CREATE TABLE `PK_GCOL_STORED9816` (`COL102` decimal(55,0) DEFAULT NULL)")
	tk.MustExec("insert into PK_GCOL_STORED9816 values(9710290195629059011)")
	tk.MustExec("prepare stmt from 'select count(*) from PK_GCOL_STORED9816 where col102 > ?'")
	tk.MustExec("set @a=9860178624005968368")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("0"))
	tk.MustExec("set @a=-7235178122860450591")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1"))
	tk.MustExec("set @a=9860178624005968368")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("0"))
	tk.MustExec("set @a=-7235178122860450591")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1"))
}

func TestIssue33067(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS `t`")
	tk.MustExec("CREATE TABLE `t` (`COL1` char(20) DEFAULT NULL, `COL2` bit(16),`COL3` date, KEY `U_M_COL5` (`COL3`,`COL2`))")
	tk.MustExec("insert into t values ('','>d','9901-06-17')")
	tk.MustExec("prepare stmt from 'select * from t where col1 is not null and col2 not in (?, ?, ?) and col3 in (?, ?, ?)'")
	tk.MustExec(`set @a=-21188, @b=26824, @c=31855, @d="5597-1-4", @e="5755-12-6", @f="1253-7-12"`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d,@e,@f`).Check(testkit.Rows())
	tk.MustExec(`set @a=-5360, @b=-11715, @c=9399, @d="9213-09-13", @e="4705-12-24", @f="9901-06-17"`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d,@e,@f`).Check(testkit.Rows(" >d 9901-06-17"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue42439(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`CREATE TABLE UK_MU16407 (COL3 timestamp NULL DEFAULT NULL, UNIQUE KEY U3(COL3))`)
	tk.MustExec(`insert into UK_MU16407 values("1985-08-31 18:03:27")`)
	tk.MustExec(`PREPARE st FROM 'SELECT COL3 FROM UK_MU16407 WHERE COL3>?'`)
	tk.MustExec(`set @a='2039-1-19 3:14:40'`)
	tk.MustExec(`execute st using @a`) // no error
	tk.MustExec(`set @a='1950-1-19 3:14:40'`)
	tk.MustQuery(`execute st using @a`).Check(testkit.Rows(`1985-08-31 18:03:27`))
}

func TestIssue29486(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists UK_MULTI_COL_11691`)
	tk.MustExec(`CREATE TABLE UK_MULTI_COL_11691 (
		COL1 binary(20) DEFAULT NULL,
		COL2 tinyint(16) DEFAULT NULL,
		COL3 time DEFAULT NULL,
		UNIQUE KEY U_M_COL (COL1(10),COL2,COL3))`)
	tk.MustExec(`insert into UK_MULTI_COL_11691 values(0x340C604874B52E8D30440E8DC2BB170621D8A088, 126, "-105:17:32"),
	(0x28EC2EDBAC7DF99045BDD0FCEAADAFBAC2ACF76F, 126, "102:54:04"),
	(0x11C38221B3B1E463C94EC39F0D481303A58A50DC, 118, "599:13:47"),
	(0x03E2FC9E0C846FF1A926BF829FA9D7BAED3FD7B1, 118, "-257:45:13")`)

	tk.MustExec(`prepare stmt from 'SELECT/*+ INL_JOIN(t1, t2) */ t2.COL2 FROM UK_MULTI_COL_11691 t1 JOIN UK_MULTI_COL_11691 t2 ON t1.col1 = t2.col1 WHERE t1.col2 BETWEEN ? AND ? AND t2.col2 BETWEEN ? AND ?'`)
	tk.MustExec(`set @a=-29408, @b=-9254, @c=-1849, @d=-2346`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows())
	tk.MustExec(`set @a=126, @b=126, @c=-125, @d=707`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows("126", "126"))
}

func TestIssue28867(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`CREATE TABLE t1 (c_int int, c_str varchar(40), PRIMARY KEY (c_int, c_str))`)
	tk.MustExec(`CREATE TABLE t2 (c_str varchar(40), PRIMARY KEY (c_str))`)
	tk.MustExec(`insert into t1 values (1, '1')`)
	tk.MustExec(`insert into t2 values ('1')`)

	tk.MustExec(`prepare stmt from 'select /*+ INL_JOIN(t1,t2) */ * from t1 join t2 on t1.c_str <= t2.c_str where t1.c_int in (?,?)'`)
	tk.MustExec(`set @a=10, @b=20`)
	tk.MustQuery(`execute stmt using @a, @b`).Check(testkit.Rows())
	tk.MustExec(`set @a=1, @b=2`)
	tk.MustQuery(`execute stmt using @a, @b`).Check(testkit.Rows("1 1 1"))

	// test case for IndexJoin + PlanCache
	tk.MustExec(`drop table t1, t2`)
	tk.MustExec(`create table t1 (a int, b int, c int, index idxab(a, b, c))`)
	tk.MustExec(`create table t2 (a int, b int)`)

	tk.MustExec(`prepare stmt from 'select /*+ INL_JOIN(t1,t2) */ * from t1, t2 where t1.a=t2.a and t1.b=?'`)
	tk.MustExec(`set @a=1`)
	tk.MustExec(`execute stmt using @a`)
	tk.MustExec(`execute stmt using @a`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`prepare stmt from 'select /*+ INL_JOIN(t1,t2) */ * from t1, t2 where t1.a=t2.a and t1.c=?'`)
	tk.MustExec(`set @a=1`)
	tk.MustExec(`execute stmt using @a`)
	tk.MustExec(`execute stmt using @a`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestIssue29565(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists PK_SIGNED_10094`)
	tk.MustExec(`CREATE TABLE PK_SIGNED_10094 (COL1 decimal(55,0) NOT NULL, PRIMARY KEY (COL1))`)
	tk.MustExec(`insert into PK_SIGNED_10094  values(-9999999999999999999999999999999999999999999999999999999)`)
	tk.MustExec(`prepare stmt from 'select * from PK_SIGNED_10094 where col1 != ? and col1 + 10 <=> ? + 10'`)
	tk.MustExec(`set @a=7309027171262036496, @b=-9798213896406520625`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows())
	tk.MustExec(`set @a=5408499810319315618, @b=-9999999999999999999999999999999999999999999999999999999`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows("-9999999999999999999999999999999999999999999999999999999"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`set @a=7309027171262036496, @b=-9798213896406520625`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestIssue31730(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists PK_S_MULTI_37;`)
	tk.MustExec(`CREATE TABLE PK_S_MULTI_37 (COL1 decimal(55,0) NOT NULL, COL2 decimal(55,0) NOT NULL,PRIMARY KEY (COL1, COL2) /*T![clustered_index] NONCLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;`)
	tk.MustExec(`insert into PK_S_MULTI_37 values(-9999999999999999999999999999999999999999999999, 1);`)
	tk.MustExec(`prepare stmt from 'SELECT SUM(COL1+?), col2 FROM PK_S_MULTI_37 GROUP BY col2';`)
	tk.MustExec(`set @a=1;`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("-9999999999999999999999999999999999999999999998 1"))
}

func TestIssue28828(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_collect_execution_info=0;")
	tk.MustExec("CREATE TABLE t (" +
		"id bigint(20) NOT NULL," +
		"audit_id bigint(20) NOT NULL," +
		"PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */," +
		"KEY index_audit_id (audit_id)" +
		");")
	tk.MustExec("insert into t values(1,9941971237863475), (2,9941971237863476), (3, 0);")
	tk.MustExec("prepare stmt from 'select * from t where audit_id=?';")
	tk.MustExec("set @a='9941971237863475', @b=9941971237863475, @c='xayh7vrWVNqZtzlJmdJQUwAHnkI8Ec', @d='0.0', @e='0.1', @f = '9941971237863476';")

	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1 9941971237863475"))
	tk.MustQuery("execute stmt using @b;").Check(testkit.Rows("1 9941971237863475"))
	// When the type of parameters have been changed, the plan cache can not be used.
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @c;").Check(testkit.Rows("3 0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @d;").Check(testkit.Rows("3 0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @e;").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @d;").Check(testkit.Rows("3 0"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @f;").Check(testkit.Rows("2 9941971237863476"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustExec("prepare stmt from 'select count(*) from t where audit_id in (?, ?, ?, ?, ?)';")
	tk.MustQuery("execute stmt using @a, @b, @c, @d, @e;").Check(testkit.Rows("2"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @f, @b, @c, @d, @e;").Check(testkit.Rows("3"))
	tk.MustQuery("select @@last_plan_from_cache;").Check(testkit.Rows("0"))
}

func TestIssue28920(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists UK_GCOL_VIRTUAL_18928`)
	tk.MustExec(`
	CREATE TABLE UK_GCOL_VIRTUAL_18928 (
	  COL102 bigint(20) DEFAULT NULL,
	  COL103 bigint(20) DEFAULT NULL,
	  COL1 bigint(20) GENERATED ALWAYS AS (COL102 & 10) VIRTUAL,
	  COL2 varchar(20) DEFAULT NULL,
	  COL4 datetime DEFAULT NULL,
	  COL3 bigint(20) DEFAULT NULL,
	  COL5 float DEFAULT NULL,
	  UNIQUE KEY UK_COL1 (COL1))`)
	tk.MustExec(`insert into UK_GCOL_VIRTUAL_18928(col102,col2) values("-5175976006730879891", "屘厒镇览錻碛斵大擔觏譨頙硺箄魨搝珄鋧扭趖")`)
	tk.MustExec(`prepare stmt from 'SELECT * FROM UK_GCOL_VIRTUAL_18928 WHERE col1 < ? AND col2 != ?'`)
	tk.MustExec(`set @a=10, @b="aa"`)
	tk.MustQuery(`execute stmt using @a, @b`).Check(testkit.Rows("-5175976006730879891 <nil> 8 屘厒镇览錻碛斵大擔觏譨頙硺箄魨搝珄鋧扭趖 <nil> <nil> <nil>"))
}

func TestIssue18066(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)
	tk.RefreshConnectionID()
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("prepare stmt from 'select * from t'")
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select EXEC_COUNT,plan_cache_hits, plan_in_cache from information_schema.statements_summary where digest_text='select * from `t`'").Check(
		testkit.Rows("1 0 0"))
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select EXEC_COUNT,plan_cache_hits, plan_in_cache from information_schema.statements_summary where digest_text='select * from `t`'").Check(
		testkit.Rows("2 1 1"))
	tk.MustExec("prepare stmt from 'select * from t'")
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("select EXEC_COUNT, plan_cache_hits, plan_in_cache from information_schema.statements_summary where digest_text='select * from `t`'").Check(
		testkit.Rows("3 2 1"))
}

func TestIssue26873(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t(a int primary key, b int, c int)")
	tk.MustExec("prepare stmt from 'select * from t where a = 2 or a = ?'")
	tk.MustExec("set @p = 3")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue29511(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("CREATE TABLE `t` (`COL1` bigint(20) DEFAULT NULL COMMENT 'WITH DEFAULT', UNIQUE KEY `UK_COL1` (`COL1`))")
	tk.MustExec("insert into t values(-3865356285544170443),(9223372036854775807);")
	tk.MustExec("prepare stmt from 'select/*+ hash_agg() */ max(col1) from t where col1 = ? and col1 > ?;';")
	tk.MustExec("set @a=-3865356285544170443, @b=-4055949188488870713;")
	tk.MustQuery("execute stmt using @a,@b;").Check(testkit.Rows("-3865356285544170443"))
}

func TestIssue23671(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (a int, b int, index ab(a, b))")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("prepare s1 from 'select * from t use index(ab) where a>=? and b>=? and b<=?'")
	tk.MustExec("set @a=1, @b=1, @c=1")
	tk.MustQuery("execute s1 using @a, @b, @c").Check(testkit.Rows("1 1"))
	tk.MustExec("set @a=1, @b=1, @c=10")
	tk.MustQuery("execute s1 using @a, @b, @c").Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // b>=1 and b<=1 --> b=1
}

func TestIssue29296(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists UK_MU14722`)
	tk.MustExec(`CREATE TABLE UK_MU14722 (
	  COL1 tinytext DEFAULT NULL,
	  COL2 tinyint(16) DEFAULT NULL,
	  COL3 datetime DEFAULT NULL,
	  COL4 int(11) DEFAULT NULL,
	  UNIQUE KEY U_M_COL (COL1(10)),
	  UNIQUE KEY U_M_COL2 (COL2),
	  UNIQUE KEY U_M_COL3 (COL3))`)
	tk.MustExec(`insert into UK_MU14722 values("輮睅麤敜溺她晁瀪襄頮鹛涓誗钷廔筪惌嶙鎢塴", -121, "3383-02-19 07:58:28" , -639457963),
		("偧孇鱓鼂瘠钻篝醗時鷷聽箌磇砀玸眞扦鸇祈灇", 127, "7902-03-05 08:54:04", -1094128660),
		("浀玡慃淛漉围甧鴎史嬙砊齄w章炢忲噑硓哈樘", -127, "5813-04-16 03:07:20", -333397107),
		("鑝粼啎鸼贖桖弦簼赭蠅鏪鐥蕿捐榥疗耹岜鬓槊", -117, "7753-11-24 10:14:24", 654872077)`)
	tk.MustExec(`prepare stmt from 'SELECT * FROM UK_MU14722 WHERE col2 > ? OR col2 BETWEEN ? AND ? ORDER BY COL2 + ? LIMIT 3'`)
	tk.MustExec(`set @a=30410, @b=3937, @c=22045, @d=-4374`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows())
	tk.MustExec(`set @a=127, @b=127, @c=127, @d=127`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows(`偧孇鱓鼂瘠钻篝醗時鷷聽箌磇砀玸眞扦鸇祈灇 127 7902-03-05 08:54:04 -1094128660`))
}

func TestIssue28246(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists PK_AUTO_RANDOM9111;")
	tk.MustExec("CREATE TABLE `PK_AUTO_RANDOM9111` (   `COL1` bigint(45) NOT NULL  ,   `COL2` varchar(20) DEFAULT NULL,   `COL4` datetime DEFAULT NULL,   `COL3` bigint(20) DEFAULT NULL,   `COL5` float DEFAULT NULL,   PRIMARY KEY (`COL1`)  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into PK_AUTO_RANDOM9111(col1) values (-9223372036854775808), (9223372036854775807);")
	tk.MustExec("set @a=9223372036854775807, @b=1")
	tk.MustExec(`prepare stmt from 'select min(col1) from PK_AUTO_RANDOM9111 where col1 > ?;';`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("<nil>"))
	// The plan contains the tableDual, so it will not be cached.
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("9223372036854775807"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestIssue29805(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_clustered_index=on;")
	tk.MustExec("drop table if exists PK_TCOLLATION10197;")
	tk.MustExec("CREATE TABLE `PK_TCOLLATION10197` (`COL1` char(1) NOT NULL, PRIMARY KEY (`COL1`(1)) /*T![clustered_index] CLUSTERED */) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into PK_TCOLLATION10197 values('龺');")
	tk.MustExec("set @a='畻', @b='龺';")
	tk.MustExec(`prepare stmt from 'select/*+ hash_agg() */ count(distinct col1) from PK_TCOLLATION10197 where col1 > ?;';`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("0"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`prepare stmt from 'select/*+ hash_agg() */ count(distinct col1) from PK_TCOLLATION10197 where col1 > ?;';`)
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("0"))

	tk.MustQuery("select/*+ hash_agg() */ count(distinct col1) from PK_TCOLLATION10197 where col1 > '龺';").Check(testkit.Rows("0"))
}

func TestIssue29993(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")

	// test PointGet + cluster index
	tk.MustExec("set tidb_enable_clustered_index=on;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (`COL1` enum('a', 'b') NOT NULL PRIMARY KEY, col2 int) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into t values('a', 1), ('b', 2);")
	tk.MustExec("set @a='a', @b='b', @z='z';")
	tk.MustExec(`prepare stmt from 'select col1 from t where col1 = ? and col2 in (1, 2);';`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("a"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("b"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())
	// The length of range have been changed, so the plan can not be cached.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())

	// test batchPointGet + cluster index
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (`COL1` enum('a', 'b') NOT NULL, col2 int, PRIMARY KEY(col1, col2)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into t values('a', 1), ('b', 2);")
	tk.MustExec("set @a='a', @b='b', @z='z';")
	tk.MustExec(`prepare stmt from 'select col1 from t where (col1, col2) in ((?, 1));';`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("a"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // invalid since 'z' is not in enum('a', 'b')
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())

	// test PointGet + non cluster index
	tk.MustExec("set tidb_enable_clustered_index=off;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (`COL1` enum('a', 'b') NOT NULL PRIMARY KEY, col2 int) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into t values('a', 1), ('b', 2);")
	tk.MustExec("set @a='a', @b='b', @z='z';")
	tk.MustExec(`prepare stmt from 'select col1 from t where col1 = ? and col2 in (1, 2);';`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("a"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows("b"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())
	// The length of range have been changed, so the plan can not be cached.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())

	// test batchPointGet + non cluster index
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (`COL1` enum('a', 'b') NOT NULL, col2 int, PRIMARY KEY(col1, col2)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into t values('a', 1), ('b', 2);")
	tk.MustExec("set @a='a', @b='b', @z='z';")
	tk.MustExec(`prepare stmt from 'select col1 from t where (col1, col2) in ((?, 1));';`)
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows("a"))
	tk.MustQuery("execute stmt using @b").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // invalid since 'z' is not in enum('a', 'b')
	tk.MustQuery("execute stmt using @z").Check(testkit.Rows())
}

func TestIssue30100(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set tidb_enable_prepared_plan_cache=1`)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(col1 enum('aa', 'bb'), col2 int, index(col1, col2));")
	tk.MustExec("insert into t values('aa', 333);")
	tk.MustExec(`prepare stmt from 'SELECT * FROM t t1 JOIN t t2 ON t1.col1 = t2.col1 WHERE t1.col1 <=> NULL';`)
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("execute stmt").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`prepare stmt from 'SELECT * FROM t t1 JOIN t t2 ON t1.col1 = t2.col1 WHERE t1.col1 <=> NULL and t2.col2 > ?';`)
	tk.MustExec("set @a=0;")
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows())
	tk.MustQuery("execute stmt using @a").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestIssue37901(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists t4`)
	tk.MustExec(`create table t4 (a date)`)
	tk.MustExec(`prepare st1 from "insert into t4(a) select dt from (select ? as dt from dual union all select sysdate() ) a";`)
	tk.MustExec(`set @t='2022-01-01 00:00:00.000000'`)
	tk.MustExec(`execute st1 using @t`)
	tk.MustQuery(`select count(*) from t4`).Check(testkit.Rows("2"))
}
