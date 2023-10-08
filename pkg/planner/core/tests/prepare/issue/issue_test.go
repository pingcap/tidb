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

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestIssueEnablePreparedPlanCache2(t *testing.T) {
	// Issue18066
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
	// TestIssue26873
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t(a int primary key, b int, c int)")
	tk.MustExec("prepare stmt from 'select * from t where a = 2 or a = ?'")
	tk.MustExec("set @p = 3")
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustQuery("execute stmt using @p").Check(testkit.Rows())
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	// TestIssue29511
	tk.MustExec("drop table if exists t")

	tk.MustExec("CREATE TABLE `t` (`COL1` bigint(20) DEFAULT NULL COMMENT 'WITH DEFAULT', UNIQUE KEY `UK_COL1` (`COL1`))")
	tk.MustExec("insert into t values(-3865356285544170443),(9223372036854775807);")
	tk.MustExec("prepare stmt from 'select/*+ hash_agg() */ max(col1) from t where col1 = ? and col1 > ?;';")
	tk.MustExec("set @a=-3865356285544170443, @b=-4055949188488870713;")
	tk.MustQuery("execute stmt using @a,@b;").Check(testkit.Rows("-3865356285544170443"))
	// TestIssue23671
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (a int, b int, index ab(a, b))")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("prepare s1 from 'select * from t use index(ab) where a>=? and b>=? and b<=?'")
	tk.MustExec("set @a=1, @b=1, @c=1")
	tk.MustQuery("execute s1 using @a, @b, @c").Check(testkit.Rows("1 1"))
	tk.MustExec("set @a=1, @b=1, @c=10")
	tk.MustQuery("execute s1 using @a, @b, @c").Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // b>=1 and b<=1 --> b=1
	// TestIssue28920
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

	// Issue29296
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
	// TestIssue28246
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
