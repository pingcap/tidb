// Copyright 2022 PingCAP, Inc.
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

package core_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/planner"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/size"
	"github.com/stretchr/testify/require"
)

func TestInitLRUWithSystemVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@session.tidb_prepared_plan_cache_size = 0") // MinValue: 1
	tk.MustQuery("select @@session.tidb_prepared_plan_cache_size").Check(testkit.Rows("1"))
	sessionVar := tk.Session().GetSessionVars()

	lru := plannercore.NewLRUPlanCache(uint(sessionVar.PreparedPlanCacheSize), 0, 0, tk.Session(), false)
	require.NotNil(t, lru)
}

func TestIssue45086(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	tk.MustExec(`CREATE TABLE t (a int(11) DEFAULT NULL, b date DEFAULT NULL)`)
	tk.MustExec(`INSERT INTO t VALUES (1, current_date())`)

	tk.MustExec(`PREPARE stmt FROM 'SELECT * FROM t WHERE b=current_date()'`)
	require.Equal(t, len(tk.MustQuery(`EXECUTE stmt`).Rows()), 1)
}

func TestIssue43311(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table test.t (id int, value decimal(7,4), c1 int, c2 int)`)
	tk.MustExec(`insert into test.t values (1,1.9285,54,28), (1,1.9286,54,28)`)

	tk.MustExec(`set session tidb_enable_non_prepared_plan_cache=0`)
	tk.MustQuery(`select * from t where value = 54 / 28`).Check(testkit.Rows()) // empty

	tk.MustExec(`set session tidb_enable_non_prepared_plan_cache=1`)
	tk.MustQuery(`select * from t where value = 54 / 28`).Check(testkit.Rows()) // empty
	tk.MustQuery(`select * from t where value = 54 / 28`).Check(testkit.Rows()) // empty

	tk.MustExec(`prepare st from 'select * from t where value = ? / ?'`)
	tk.MustExec(`set @a=54, @b=28`)
	tk.MustQuery(`execute st using @a, @b`).Check(testkit.Rows()) // empty
	tk.MustQuery(`execute st using @a, @b`).Check(testkit.Rows()) // empty
}

func TestIssue44830(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set @@tidb_opt_fix_control = "44830:ON"`)
	tk.MustExec(`create table t (a int, primary key(a))`)
	tk.MustExec(`create table t1 (a int, b int, primary key(a, b))`) // multiple-column primary key
	tk.MustExec(`insert into t values (1), (2), (3)`)
	tk.MustExec(`insert into t1 values (1, 1), (2, 2), (3, 3)`)
	tk.MustExec(`set @a=1, @b=2, @c=3`)

	// single-column primary key cases
	tk.MustExec(`prepare st from 'select * from t where 1=1 and a in (?, ?, ?)'`)
	tk.MustQuery(`execute st using @a, @b, @c`).Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery(`execute st using @a, @b, @c`).Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery(`execute st using @a, @b, @b`).Sort().Check(testkit.Rows("1", "2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // range length changed
	tk.MustQuery(`execute st using @b, @b, @b`).Sort().Check(testkit.Rows("2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // range length changed
	tk.MustQuery(`execute st using @a, @b, @c`).Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery(`execute st using @a, @b, @b`).Sort().Check(testkit.Rows("1", "2"))
	tk.MustQuery(`execute st using @a, @b, @b`).Sort().Check(testkit.Rows("1", "2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // contain duplicated values in the in-list

	// multi-column primary key cases
	tk.MustExec(`prepare st from 'select * from t1 where 1=1 and (a, b) in ((?, ?), (?, ?), (?, ?))'`)
	tk.MustQuery(`execute st using @a, @a, @b, @b, @c, @c`).Sort().Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk.MustQuery(`execute st using @a, @a, @b, @b, @c, @c`).Sort().Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery(`execute st using @a, @a, @b, @b, @b, @b`).Sort().Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // range length changed
	tk.MustQuery(`execute st using @b, @b, @b, @b, @b, @b`).Sort().Check(testkit.Rows("2 2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // range length changed
	tk.MustQuery(`execute st using @b, @b, @b, @b, @c, @c`).Sort().Check(testkit.Rows("2 2", "3 3"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // range length changed
	tk.MustQuery(`execute st using @a, @a, @a, @a, @a, @a`).Sort().Check(testkit.Rows("1 1"))
	tk.MustQuery(`execute st using @a, @a, @a, @a, @a, @a`).Sort().Check(testkit.Rows("1 1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // contain duplicated values in the in-list
	tk.MustQuery(`execute st using @a, @a, @b, @b, @b, @b`).Sort().Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery(`execute st using @a, @a, @b, @b, @b, @b`).Sort().Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // contain duplicated values in the in-list
}

func TestIssue44830NonPrep(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`set @@tidb_opt_fix_control = "44830:ON"`)
	tk.MustExec(`create table t1 (a int, b int, primary key(a, b))`) // multiple-column primary key
	tk.MustExec(`insert into t1 values (1, 1), (2, 2), (3, 3)`)
	tk.MustExec(`set @a=1, @b=2, @c=3`)

	tk.MustQuery(`select * from t1 where 1=1 and (a, b) in ((1, 1), (2, 2), (3, 3))`).Sort().Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk.MustQuery(`select * from t1 where 1=1 and (a, b) in ((1, 1), (2, 2), (3, 3))`).Sort().Check(testkit.Rows("1 1", "2 2", "3 3"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery(`select * from t1 where 1=1 and (a, b) in ((1, 1), (2, 2), (2, 2))`).Sort().Check(testkit.Rows("1 1", "2 2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`select * from t1 where 1=1 and (a, b) in ((2, 2), (2, 2), (2, 2))`).Sort().Check(testkit.Rows("2 2"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`select * from t1 where 1=1 and (a, b) in ((1, 1), (1, 1), (1, 1))`).Sort().Check(testkit.Rows("1 1"))
	tk.MustQuery(`select * from t1 where 1=1 and (a, b) in ((1, 1), (1, 1), (1, 1))`).Sort().Check(testkit.Rows("1 1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func TestPlanCacheSizeSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	// default value = 100
	tk.MustQuery(`select @@tidb_prepared_plan_cache_size`).Check(testkit.Rows("100"))
	tk.MustQuery(`select @@tidb_session_plan_cache_size`).Check(testkit.Rows("100"))

	// keep the same value when updating any one of them
	tk.MustExec(`set @@tidb_prepared_plan_cache_size = 200`)
	tk.MustQuery(`select @@tidb_prepared_plan_cache_size`).Check(testkit.Rows("200"))
	tk.MustQuery(`select @@tidb_session_plan_cache_size`).Check(testkit.Rows("200"))
	tk.MustExec(`set @@tidb_session_plan_cache_size = 300`)
	tk.MustQuery(`select @@tidb_prepared_plan_cache_size`).Check(testkit.Rows("300"))
	tk.MustQuery(`select @@tidb_session_plan_cache_size`).Check(testkit.Rows("300"))

	tk.MustExec(`set global tidb_prepared_plan_cache_size = 400`)
	tk1 := testkit.NewTestKit(t, store)
	tk1.MustQuery(`select @@tidb_prepared_plan_cache_size`).Check(testkit.Rows("400"))
	tk1.MustQuery(`select @@tidb_session_plan_cache_size`).Check(testkit.Rows("400"))

	tk.MustExec(`set global tidb_session_plan_cache_size = 500`)
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustQuery(`select @@tidb_prepared_plan_cache_size`).Check(testkit.Rows("500"))
	tk2.MustQuery(`select @@tidb_session_plan_cache_size`).Check(testkit.Rows("500"))
}

func TestPlanCacheUnsafeRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int unsigned, key(a))`)
	tk.MustExec(`prepare st from 'select a from t use index(a) where a<?'`)
	tk.MustExec(`set @a=10`)
	tk.MustExec(`execute st using @a`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`set @a=-10`) // invalid range for an unsigned column
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`set @a=10`) // plan cache can work again
	tk.MustExec(`execute st using @a`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`create table t1 (a enum('1', '2'), key(a))`)
	tk.MustExec(`prepare st from 'select a from t1 use index(a) where a=?'`)
	tk.MustExec(`set @a='1'`)
	tk.MustExec(`execute st using @a`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`set @a='x'`) // invalid value for this column
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`set @a='1'`) // plan cache can work again
	tk.MustExec(`execute st using @a`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestIssue43405(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`insert into t values (1), (2), (3), (4)`)
	tk.MustExec(`prepare st from 'select * from t where a!=? and a in (?, ?, ?)'`)
	tk.MustExec(`set @a=1, @b=2, @c=3, @d=4`)
	tk.MustQuery(`execute st using @a, @a, @a, @a`).Sort().Check(testkit.Rows())
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: NE/INList simplification is triggered"))
	tk.MustQuery(`execute st using @a, @a, @b, @c`).Sort().Check(testkit.Rows("2", "3"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: NE/INList simplification is triggered"))
	tk.MustQuery(`execute st using @a, @b, @c, @d`).Sort().Check(testkit.Rows("2", "3", "4"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: NE/INList simplification is triggered"))

	tk.MustExec(`CREATE TABLE UK_SIGNED_19384 (
    COL1 decimal(37,4) unsigned DEFAULT NULL COMMENT 'WITH DEFAULT',
    COL2 varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,
    COL4 datetime DEFAULT NULL,
    COL3 bigint DEFAULT NULL,
    COL5 float DEFAULT NULL,
    UNIQUE KEY UK_COL1 (COL1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
	tk.MustExec(`INSERT INTO UK_SIGNED_19384 VALUES
  (729024465529090.5423,'劗驻胭毤橰亀讁陶ĉ突錌ͳ河碡祁聓兕锻觰俆','4075-07-11 12:02:57',6021562653572886552,1.93349e38),
  (492790234219503.0846,'硴皡箒嫹璞玚囑蚂身囈軔獰髴囥慍廂頚禌浖蕐','1193-09-27 12:13:40',1836453747944153034,-2.67982e38),
  (471841432147994.4981,'豻貐裝濂婝蒙蘦镢県蟎髓蓼窘搴熾臐哥递泒執','1618-01-24 05:06:44',6669616052974883820,9.38232e37)`)
	tk.MustExec(`prepare stmt from 'select/*+ tidb_inlj(t1) */ t1.col1 from UK_SIGNED_19384 t1 join UK_SIGNED_19384 t2 on t1.col1 = t2.col1 where t1. col1 != ? and t2. col1 in (?, ?, ?)'`)
	tk.MustExec(`set @a=999999999999999999999999999999999.9999, @b=999999999999999999999999999999999.9999, @c=999999999999999999999999999999999.9999, @d=999999999999999999999999999999999.9999`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Check(testkit.Rows()) // empty result
	tk.MustExec(`set @a=895769331208356.9029, @b=471841432147994.4981, @c=729024465529090.5423, @d=492790234219503.0846`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d`).Sort().Check(testkit.Rows(
		"471841432147994.4981",
		"492790234219503.0846",
		"729024465529090.5423"))
}

func TestIssue40296(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`create database test_40296`)
	tk.MustExec(`use test_40296`)
	tk.MustExec(`CREATE TABLE IDT_MULTI15880STROBJSTROBJ (
  COL1 enum('aa','bb','cc','dd','ff','gg','kk','ll','mm','ee') DEFAULT NULL,
  COL2 decimal(20,0) DEFAULT NULL,
  COL3 date DEFAULT NULL,
  KEY U_M_COL4 (COL1,COL2),
  KEY U_M_COL5 (COL3,COL2))`)
	tk.MustExec(`insert into IDT_MULTI15880STROBJSTROBJ values("ee", -9605492323393070105, "0850-03-15")`)
	tk.MustExec(`set session tidb_enable_non_prepared_plan_cache=on`)
	tk.MustQuery(`select * from IDT_MULTI15880STROBJSTROBJ where col1 in ("dd", "dd") or col2 = 9923875910817805958 or col3 = "9994-11-11"`).Check(
		testkit.Rows())
	tk.MustQuery(`select * from IDT_MULTI15880STROBJSTROBJ where col1 in ("aa", "aa") or col2 = -9605492323393070105 or col3 = "0005-06-22"`).Check(
		testkit.Rows("ee -9605492323393070105 0850-03-15"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // unary operator '-' is not supported now.
}

func TestIssue43522(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`CREATE TABLE UK_SIGNED_19385 (
  COL1 decimal(37,4) unsigned DEFAULT '101.0000' COMMENT 'WITH DEFAULT',
  COL2 varchar(20) DEFAULT NULL,
  COL4 datetime DEFAULT NULL,
  COL3 bigint(20) DEFAULT NULL,
  COL5 float DEFAULT NULL,
  UNIQUE KEY UK_COL1 (COL1) /*!80000 INVISIBLE */)`)
	tk.MustExec(`INSERT INTO UK_SIGNED_19385 VALUES (999999999999999999999999999999999.9999,'苊檷鞤寰抿逿詸叟艟俆錟什姂庋鴪鎅枀礰扚匝','8618-02-11 03:30:03',7016504421081900731,2.77465e38)`)
	tk.MustQuery(`select * from UK_SIGNED_19385 where col1 = 999999999999999999999999999999999.9999 and
    col1 * 999999999999999999999999999999999.9999 between 999999999999999999999999999999999.9999 and
    999999999999999999999999999999999.9999`).Check(testkit.Rows()) // empty and no error
}

func TestIssue43520(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`CREATE TABLE IDT_20290 (
  COL1 mediumtext DEFAULT NULL,
  COL2 decimal(52,7) DEFAULT NULL,
  COL3 datetime DEFAULT NULL,
  KEY U_M_COL (COL1(10),COL2,COL3) /*!80000 INVISIBLE */)`)
	tk.MustExec(`INSERT INTO IDT_20290 VALUES
  ('',210255309400.4264137,'4273-04-17 17:26:51'),
  (NULL,952470120213.2538798,'7087-08-19 21:38:49'),
  ('俦',486763966102.1656494,'8846-06-12 12:02:13'),
  ('憁',610644171405.5953911,'2529-07-19 17:24:49'),
  ('顜',-359717183823.5275069,'2599-04-01 00:12:08'),
  ('塼',466512908211.1135111,'1477-10-20 07:14:51'),
  ('宻',-564216096745.0427987,'7071-11-20 13:38:24'),
  ('網',-483373421083.4724254,'2910-02-19 18:29:17'),
  ('顥',164020607693.9988781,'2820-10-12 17:38:44'),
  ('谪',25949740494.3937876,'6527-05-30 22:58:37')`)
	err := tk.QueryToErr(`select * from IDT_20290 where col2 * 049015787697063065230692384394107598316198958.1850509 >= 659971401668884663953087553591534913868320924.5040396 and col2 = 869042976700631943559871054704914143535627349.9659934`)
	require.ErrorContains(t, err, "value is out of range in")
}

func TestIssue14875(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t(a varchar(8) not null, b varchar(8) not null)`)
	tk.MustExec(`insert into t values('1','1')`)
	tk.MustExec(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and t2.b = '1' and t2.b = ?"`)
	tk.MustExec(`set @a = '1'`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("1"))
	tk.MustExec(`set @a = '2'`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("0"))

	tk.MustExec(`prepare stmt from "select count(1) from t t1, t t2 where t1.a = t2.a and t1.a > ?"`)
	tk.MustExec(`set @a = '1'`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("0"))
	tk.MustExec(`set @a = '0'`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("1"))
}

func TestIssue14871(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t(a varchar(8), b varchar(8))`)
	tk.MustExec(`insert into t values('1','1')`)
	tk.MustExec(`prepare stmt from "select count(1) from t t1 left join t t2 on t1.a = t2.a where t2.b = ? and t2.b = ?"`)
	tk.MustExec(`set @p0 = '1', @p1 = '2'`)
	tk.MustQuery(`execute stmt using @p0, @p1`).Check(testkit.Rows("0"))
	tk.MustExec(`set @p0 = '1', @p1 = '1'`)
	tk.MustQuery(`execute stmt using @p0, @p1`).Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheDMLHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache_for_dml=1`)

	tk.MustExec(`insert into t values (1)`)
	tk.MustExec(`insert into t values (1)`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`update t set a=1`)
	tk.MustExec(`update t set a=1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`delete from t where a=1`)
	tk.MustExec(`delete from t where a=1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`insert /*+ ignore_plan_cache() */ into t values (1)`)
	tk.MustExec(`insert /*+ ignore_plan_cache() */ into t values (1)`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`update /*+ ignore_plan_cache() */ t set a=1`)
	tk.MustExec(`update /*+ ignore_plan_cache() */ t set a=1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`delete /*+ ignore_plan_cache() */ from t where a=1`)
	tk.MustExec(`delete /*+ ignore_plan_cache() */ from t where a=1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	tk.MustExec(`insert into t values (1)`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`update t set a=1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`delete from t where a=1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCachePlanString(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a))`)
	tk.MustExec(`set @@tidb_enable_non_prepared_plan_cache=1`)

	ctx := tk.Session()
	planString := func(sql string) string {
		stmts, err := session.Parse(ctx, sql)
		require.NoError(t, err)
		stmt := stmts[0]
		ret := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(context.Background(), ctx, stmt, plannercore.WithPreprocessorReturn(ret))
		require.NoError(t, err)
		p, _, err := planner.Optimize(context.TODO(), ctx, stmt, ret.InfoSchema)
		require.NoError(t, err)
		return plannercore.ToString(p)
	}

	require.Equal(t, planString("select a from t where a < 1"), "IndexReader(Index(t.a)[[-inf,1)])")
	require.Equal(t, planString("select a from t where a < 10"), "IndexReader(Index(t.a)[[-inf,10)])") // range 1 -> 10
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	require.Equal(t, planString("select * from t where b < 1"), "TableReader(Table(t)->Sel([lt(test.t.b, 1)]))")
	require.Equal(t, planString("select * from t where b < 10"), "TableReader(Table(t)->Sel([lt(test.t.b, 10)]))") // filter 1 -> 10
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheJSONFilter(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, b json)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select * from t where a<1`)
	tk.MustExec(`select * from t where a<2`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// queries with filters with JSON columns are not supported
	tk.MustExec(`select * from t where b<1`)
	tk.MustExec(`select * from t where b<2`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec(`select b from t where a<1`)
	tk.MustExec(`select b from t where a<2`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheEnumFilter(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, b enum('1', '2', '3'))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select * from t where a<1`)
	tk.MustExec(`select * from t where a<2`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// queries with filters with enum columns are not supported
	tk.MustExec(`select * from t where b='1'`)
	tk.MustExec(`select * from t where b='2'`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec(`select b from t where a<1`)
	tk.MustExec(`select b from t where a<2`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheDateFormat(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`create table t1 (s1 char(20) character set latin1)`)
	tk.MustExec(`insert into t1 values (date_format('2004-02-02','%M'))`) // no error
	tk.MustQuery(`select * from t1`).Check(testkit.Rows(`February`))
}

func TestNonPreparedPlanCacheNullValue(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select * from t where a=1`)
	tk.MustExec(`select * from t where a=2`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`select * from t where a=null`) // query with null value cannot hit
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec(`select * from t where a=2`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheInListChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select * from t where a in (1, 2, 3)`)
	tk.MustExec(`select * from t where a in (2, 3, 4)`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`select * from t where a in (2, 3, 4, 5)`) // cannot hit the previous plan
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`select * from t where a in (1, 2, 3, 4)`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheMemoryTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select data_type from INFORMATION_SCHEMA.columns where table_name = 'v'`)
	tk.MustExec(`select data_type from INFORMATION_SCHEMA.columns where table_name = 'v'`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func TestNonPreparedPlanCacheTooManyConsts(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	var x []string
	for i := 0; i < 201; i++ {
		x = append(x, fmt.Sprintf("%v", i))
	}
	list1 := strings.Join(x[:199], ", ")
	list2 := strings.Join(x[:200], ", ")
	list3 := strings.Join(x[:201], ", ")

	tk.MustExec(fmt.Sprintf(`select * from t where a in (%v)`, list1))
	tk.MustExec(fmt.Sprintf(`select * from t where a in (%v)`, list1))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec(fmt.Sprintf(`select * from t where a in (%v)`, list2))
	tk.MustExec(fmt.Sprintf(`select * from t where a in (%v)`, list2))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// query has more than 50 consts cannot hit
	tk.MustExec(fmt.Sprintf(`select * from t where a in (%v)`, list3))
	tk.MustExec(fmt.Sprintf(`select * from t where a in (%v)`, list3))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestNonPreparedPlanCacheSchemaChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("alter table t add index idx_a(a)")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // cannot hit since the schema changed
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedCacheWithPreparedCache(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`prepare st from 'select * from t where a=1'`)
	tk.MustExec(`execute st`)
	tk.MustExec(`execute st`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`select * from t where a=1`) // cannot hit since these 2 plan cache are separated
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select * from t where a=1`)
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=0")
	tk.MustExec(`select * from t where a=1`) // the session-level switch can take effect in real time
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestNonPreparedPlanCacheSwitch2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)

	for nonPrep := 0; nonPrep <= 1; nonPrep++ {
		for prep := 0; prep <= 1; prep++ {
			tk.MustExec("create table t(a int)")
			tk.MustExec(fmt.Sprintf(`set tidb_enable_non_prepared_plan_cache=%v`, nonPrep))
			tk.MustExec(fmt.Sprintf(`set tidb_enable_prepared_plan_cache=%v`, prep))

			tk.MustExec(`select * from t where a<1`)
			tk.MustExec(`select * from t where a<2`)
			tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows(fmt.Sprintf("%v", nonPrep)))

			tk.MustExec(`prepare st from 'select * from t where a<?'`)
			tk.MustExec(`set @a=1`)
			tk.MustExec(`execute st using @a`)
			tk.MustExec(`set @a=2`)
			tk.MustExec(`execute st using @a`)
			tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows(fmt.Sprintf("%v", prep)))

			tk.MustExec("drop table t")
		}
	}
}

func TestNonPreparedPlanCacheUnknownSchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table tt(a char(2) primary key, b char(2))`)
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")
	err := tk.ExecToErr(`select tt.* from tt tmp where a='aa'`)
	require.Equal(t, err.Error(), "[planner:1051]Unknown table 'tt'")
}

func TestNonPreparedPlanCacheReason(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`explain format = 'plan_cache' select * from t where a=1`)
	tk.MustExec(`explain format = 'plan_cache' select * from t where a=1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`explain format = 'plan_cache' select * from (select * from t) tx`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip non-prepared plan-cache: queries that have sub-queries are not supported`))

	// no warning if disable this feature
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=0")
	tk.MustExec(`explain format = 'plan_cache' select * from t where a+1=1`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`explain format = 'plan_cache' select * from t t1, t t2`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
	tk.MustExec(`explain format = 'plan_cache' select * from t where a in (select a from t)`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
}

func TestNonPreparedPlanCacheSysSchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")
	tk.MustExec(`explain format='plan_cache' select address from PERFORMANCE_SCHEMA.tikv_profile_cpu`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip non-prepared plan-cache: access tables in system schema`))

	tk.MustExec(`use PERFORMANCE_SCHEMA`)
	tk.MustExec(`explain format='plan_cache' select address from tikv_profile_cpu`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip non-prepared plan-cache: access tables in system schema`))
}

func TestNonPreparedPlanCacheSQLMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select * from t where a=1`)
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("set @@sql_mode=''") // cannot hit since sql-mode changed
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestPreparedPlanCacheLargePlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, b int, c varchar(2048))")

	baseSQL := "select * from t, t t1 where t1.c=space(2048) and t.c=space(2048) and t.a=t1.b"
	var baseSQLs []string
	for i := 0; i < 30; i++ {
		baseSQLs = append(baseSQLs, baseSQL)
	}

	tk.MustExec("prepare st from '" + strings.Join(baseSQLs[:15], " union all ") + "'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1")) // less than 2MB threshold

	tk.MustExec("prepare st from '" + strings.Join(baseSQLs[:30], " union all ") + "'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // large than 2MB threshold

	tk.MustExec(fmt.Sprintf("set tidb_plan_cache_max_plan_size=%v", 1*size.GB))
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1")) // less than 1GB threshold
}

func TestPreparedPlanCacheLongInList(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, b int)")

	genInList := func(l int) string {
		var elements []string
		for i := 0; i < l; i++ {
			elements = append(elements, fmt.Sprintf("%v", i))
		}
		return "(" + strings.Join(elements, ",") + ")"
	}

	// the limitation is 200
	tk.MustExec(fmt.Sprintf(`prepare st_199 from 'select * from t where a in %v'`, genInList(199)))
	tk.MustExec(`execute st_199`)
	tk.MustExec(`execute st_199`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(fmt.Sprintf(`prepare st_201 from 'select * from t where a in %v'`, genInList(201)))
	tk.MustExec(`execute st_201`)
	tk.MustExec(`execute st_201`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	tk.MustExec(fmt.Sprintf(`prepare st_99_100 from 'select * from t where a in %v and b in %v'`, genInList(99), genInList(100)))
	tk.MustExec(`execute st_99_100`)
	tk.MustExec(`execute st_99_100`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(fmt.Sprintf(`prepare st_100_101 from 'select * from t where a in %v and b in %v'`, genInList(100), genInList(101)))
	tk.MustExec(`execute st_100_101`)
	tk.MustExec(`execute st_100_101`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func TestPreparedPlanCacheStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values (2)")

	tk.MustExec(`prepare st from 'select * from t where a=?'`)
	tk.MustExec(`set @a=1`)
	tk.MustExec(`execute st using @a`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec("analyze table t")
	tk.MustExec("set tidb_plan_cache_invalidation_on_fresh_stats = 0")
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec("set tidb_plan_cache_invalidation_on_fresh_stats = 1")
	tk.MustExec(`execute st using @a`) // stats changes can affect prep cache hit if we turn on the variable
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheStats(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values (2)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select * from t where a=1`)
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("analyze table t")
	tk.MustExec(`select * from t where a=1`) // stats changes can affect non-prep cache hit
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec(`select * from t where a=1`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheHints(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, index(a))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec("select /*+ use_index(t, a) */ * from t where a=1")
	tk.MustExec("select /*+ use_index(t, a) */ * from t where a=1") // cannot hit since it has a hint
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheParamInit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table tx(a double, b int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")
	tk.MustExec(`insert into tx values (3.0, 3)`)
	tk.MustQuery("select json_object('k', a) = json_object('k', b) from tx").Check(testkit.Rows("1")) // no error
	tk.MustQuery("select json_object('k', a) = json_object('k', b) from tx").Check(testkit.Rows("1"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestNonPreparedPlanCacheBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, index(a))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec("create binding for select * from t where a=1 using select /*+ use_index(t, a) */ * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("drop binding for select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheWithExplain(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")
	tk.MustExec("select * from t where a=1") // cache this plan

	tk.MustQuery("explain select * from t where a=2").Check(testkit.Rows(
		`TableReader_7 10.00 root  data:Selection_6`,
		`└─Selection_6 10.00 cop[tikv]  eq(test.t.a, 2)`,
		`  └─TableFullScan_5 10000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustQuery("explain format=verbose select * from t where a=2").Check(testkit.Rows(
		`TableReader_7 10.00 168975.57 root  data:Selection_6`,
		`└─Selection_6 10.00 2534000.00 cop[tikv]  eq(test.t.a, 2)`,
		`  └─TableFullScan_5 10000.00 2035000.00 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.MustQuery("explain analyze select * from t where a=2").CheckAt([]int{0, 1, 2, 3}, [][]interface{}{
		{"TableReader_7", "10.00", "0", "root"},
		{"└─Selection_6", "10.00", "0", "cop[tikv]"},
		{"  └─TableFullScan_5", "10000.00", "0", "cop[tikv]"},
	})
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestNonPreparedPlanCacheFastPointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int primary key, b int, unique key(b))`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	// fast plans have a higher priority than non-prep cache plan
	tk.MustQuery(`explain format='brief' select a from t where a in (1, 2)`).Check(testkit.Rows(
		`Batch_Point_Get 2.00 root table:t handle:[1 2], keep order:false, desc:false`))
	tk.MustQuery(`select a from t where a in (1, 2)`).Check(testkit.Rows())
	tk.MustQuery(`select a from t where a in (1, 2)`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	tk.MustQuery(`explain format='brief' select b from t where b = 1`).Check(testkit.Rows(
		`Point_Get 1.00 root table:t, index:b(b) `))
	tk.MustQuery(`select b from t where b = 1`).Check(testkit.Rows())
	tk.MustQuery(`select b from t where b = 1`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func TestNonPreparedPlanCacheSetOperations(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	// queries with set operations cannot hit the cache
	for _, q := range []string{
		`select * from t union select * from t`,
		`select * from t union distinct select * from t`,
		`select * from t union all select * from t`,
		`select * from t except select * from t`,
		`select * from t intersect select * from t`,
	} {
		tk.MustQuery(q).Check(testkit.Rows())
		tk.MustQuery(q).Check(testkit.Rows())
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	}
}

func TestNonPreparedPlanCacheInformationSchema(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_non_prepared_plan_cache=1")
	p := parser.New()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable(), plannercore.MockUnsignedTable()})

	stmt, err := p.ParseOneStmt("select avg(a),avg(b),avg(c) from t", "", "")
	require.NoError(t, err)
	err = plannercore.Preprocess(context.Background(), tk.Session(), stmt, plannercore.WithPreprocessorReturn(&plannercore.PreprocessorReturn{InfoSchema: is}))
	require.NoError(t, err) // no error
	_, _, err = planner.Optimize(context.TODO(), tk.Session(), stmt, is)
	require.NoError(t, err) // no error
	_, _, err = planner.Optimize(context.TODO(), tk.Session(), stmt, is)
	require.NoError(t, err) // no error
	require.True(t, tk.Session().GetSessionVars().FoundInPlanCache)
}

func TestNonPreparedPlanCacheSpecialTables(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`create definer='root'@'localhost' view t_v as select * from t`)
	tk.MustExec(`create table t_p (a int) partition by hash(a) partitions 4`)
	tk.MustExec(`create temporary table t_t (a int)`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	// queries that access partitioning tables, view, temporary tables or contain CTE cannot hit the cache.
	for _, q := range []string{
		`select * from t_v`,
		`select * from t_p`,
		`select * from t_t`,
		`with t_cte as (select * from t) select * from t_cte`,
	} {
		tk.MustQuery(q).Check(testkit.Rows())
		tk.MustQuery(q).Check(testkit.Rows())
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	}
}

func TestNonPreparedPlanParameterType(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, key(a))`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	tk.MustQuery(`select * from t where a=1`).Check(testkit.Rows())
	tk.MustQuery(`select * from t where a=1`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustQuery(`select * from t where a=1.1`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`explain format = 'plan_cache' select * from t where a=1.1`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip non-prepared plan-cache: '1.1' may be converted to INT`))

	tk.MustQuery(`select * from t where a='1'`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`explain format = 'plan_cache' select * from t where a='1'`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip non-prepared plan-cache: '1' may be converted to INT`))
}

func TestIssue43852(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t6 (a date, b date, key(a))`)
	tk.MustExec(`insert into t6 values ('2023-01-21', '2023-01-05')`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustQuery(`select * from t6 where a in (2015, '8')`).Check(testkit.Rows())
	tk.MustQuery(`select * from t6 where a in (2009, '2023-01-21')`).Check(testkit.Rows(`2023-01-21 2023-01-05`))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestNonPreparedPlanTypeRandomly(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, key(a))`)
	tk.MustExec(`create table t2 (a varchar(8), b varchar(8), key(a))`)
	tk.MustExec(`create table t3 (a double, b double, key(a))`)
	tk.MustExec(`create table t4 (a decimal(4, 2), b decimal(4, 2), key(a))`)
	tk.MustExec(`create table t5 (a year, b year, key(a))`)
	tk.MustExec(`create table t6 (a date, b date, key(a))`)
	tk.MustExec(`create table t7 (a datetime, b datetime, key(a))`)

	n := 30
	for i := 0; i < n; i++ {
		tk.MustExec(fmt.Sprintf(`insert into t1 values (%v, %v)`, randNonPrepTypeVal(t, n, "int"), randNonPrepTypeVal(t, n, "int")))
		tk.MustExec(fmt.Sprintf(`insert into t2 values (%v, %v)`, randNonPrepTypeVal(t, n, "varchar"), randNonPrepTypeVal(t, n, "varchar")))
		tk.MustExec(fmt.Sprintf(`insert into t3 values (%v, %v)`, randNonPrepTypeVal(t, n, "double"), randNonPrepTypeVal(t, n, "double")))
		tk.MustExec(fmt.Sprintf(`insert into t4 values (%v, %v)`, randNonPrepTypeVal(t, n, "decimal"), randNonPrepTypeVal(t, n, "decimal")))
		// TODO: fix it later
		//tk.MustExec(fmt.Sprintf(`insert into t5 values (%v, %v)`, randNonPrepTypeVal(t, n, "year"), randNonPrepTypeVal(t, n, "year")))
		tk.MustExec(fmt.Sprintf(`insert into t6 values (%v, %v)`, randNonPrepTypeVal(t, n, "date"), randNonPrepTypeVal(t, n, "date")))
		tk.MustExec(fmt.Sprintf(`insert into t7 values (%v, %v)`, randNonPrepTypeVal(t, n, "datetime"), randNonPrepTypeVal(t, n, "datetime")))
	}

	for i := 0; i < 200; i++ {
		q := fmt.Sprintf(`select * from t%v where %v`, rand.Intn(7)+1, randNonPrepFilter(t, n))
		tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
		r0 := tk.MustQuery(q).Sort()            // the first execution
		tk.MustQuery(q).Sort().Check(r0.Rows()) // may hit the cache
		tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`)
		tk.MustQuery(q).Sort().Check(r0.Rows()) // disable the non-prep cache
	}
}

func randNonPrepFilter(t *testing.T, scale int) string {
	switch rand.Intn(4) {
	case 0: // >=
		return fmt.Sprintf(`a >= %v`, randNonPrepVal(t, scale))
	case 1: // <
		return fmt.Sprintf(`a < %v`, randNonPrepVal(t, scale))
	case 2: // =
		return fmt.Sprintf(`a = %v`, randNonPrepVal(t, scale))
	case 3: // in
		return fmt.Sprintf(`a in (%v, %v)`, randNonPrepVal(t, scale), randNonPrepVal(t, scale))
	}
	require.Error(t, errors.New(""))
	return ""
}

func randNonPrepVal(t *testing.T, scale int) string {
	return randNonPrepTypeVal(t, scale, [7]string{"int", "varchar", "double",
		"decimal", "year", "datetime", "date"}[rand.Intn(7)])
}

func randNonPrepTypeVal(t *testing.T, scale int, typ string) string {
	switch typ {
	case "int":
		return fmt.Sprintf("%v", rand.Intn(scale)-(scale/2))
	case "varchar":
		return fmt.Sprintf("'%v'", rand.Intn(scale)-(scale/2))
	case "double", "decimal":
		return fmt.Sprintf("%v", float64(rand.Intn(scale)-(scale/2))/float64(10))
	case "year":
		return fmt.Sprintf("%v", 2000+rand.Intn(scale))
	case "date":
		return fmt.Sprintf("'2023-01-%02d'", rand.Intn(scale)+1)
	case "timestamp", "datetime":
		return fmt.Sprintf("'2023-01-01 00:00:%02d'", rand.Intn(scale))
	default:
		require.Error(t, errors.New(typ))
		return ""
	}
}

func TestNonPreparedPlanCacheBasically(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, c int, d int, key(b), key(c, d))`)
	for i := 0; i < 20; i++ {
		tk.MustExec(fmt.Sprintf("insert into t values (%v, %v, %v, %v)", i, rand.Intn(20), rand.Intn(20), rand.Intn(20)))
	}

	queries := []string{
		"select * from t where a<10",
		"select * from t where a<13 and b<15",
		"select * from t where b=13",
		"select * from t where c<8",
		"select * from t where d>8",
		"select * from t where c=8 and d>10",
		"select * from t where a<12 and b<13 and c<12 and d>2",
		"select * from t where a in (1, 2, 3)",
		"select * from t where a<13 or b<15",
		"select * from t where a<13 or b<15 and c=13",
		"select * from t where a in (1, 2)",
		"select * from t where a in (1, 2) and b in (1, 2, 3)",
		"select * from t where a in (1, 2) and b < 15",
		"select * from t where a between 1 and 10",
		"select * from t where a between 1 and 10 and b < 15",
	}

	for _, query := range queries {
		tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`)
		resultNormal := tk.MustQuery(query).Sort()
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

		tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
		tk.MustQuery(query)                                                    // first process
		tk.MustQuery(query).Sort().Check(resultNormal.Rows())                  // equal to the result without plan-cache
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // this plan is from plan-cache
	}
}

func TestNonPreparedPlanCacheInternalSQL(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, index(a))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	tk.Session().GetSessionVars().InRestrictedSQL = true
	tk.MustExecWithContext(ctx, "select * from t where a=1")
	tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("0"))

	tk.Session().GetSessionVars().InRestrictedSQL = false
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheSelectLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, index(a))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec("select * from t where a=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("set @@session.sql_select_limit=1")
	tk.MustExec("select * from t where a=1")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue41626(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a year)`)
	tk.MustExec(`insert into t values (2000)`)
	tk.MustExec(`prepare st from 'select * from t where a<?'`)
	tk.MustExec(`set @a=12`)
	tk.MustQuery(`execute st using @a`).Check(testkit.Rows("2000"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: '12' may be converted to INT"))
}

func TestIssue38269(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0")
	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2(a int, b int, c int, index idx(a, b))")
	tk.MustExec("prepare stmt1 from 'select /*+ inl_join(t2) */ * from t1 join t2 on t1.a = t2.a where t2.b in (?, ?, ?)'")
	tk.MustExec("set @a = 10, @b = 20, @c = 30, @d = 40, @e = 50, @f = 60")
	tk.MustExec("execute stmt1 using @a, @b, @c")
	tk.MustExec("execute stmt1 using @d, @e, @f")
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Contains(t, rows[6][4], "range: decided by [eq(test.t2.a, test.t1.a) in(test.t2.b, 40, 50, 60)]")
}

func TestIssue38533(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key (a))")
	tk.MustExec(`prepare st from "select /*+ use_index(t, a) */ a from t where a=? and a=?"`)
	tk.MustExec(`set @a=1`)
	tk.MustExec(`execute st using @a, @a`)
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	plan := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.True(t, strings.Contains(plan[1][0].(string), "RangeScan")) // range-scan instead of full-scan

	tk.MustExec(`execute st using @a, @a`)
	tk.MustExec(`execute st using @a, @a`)
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestInvalidRange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key(a))")
	tk.MustExec("prepare st from 'select * from t where a>? and a<?'")
	tk.MustExec("set @l=100, @r=10")
	tk.MustExec("execute st using @l, @r")

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{{"TableDual_5"}}) // use TableDual directly instead of TableFullScan

	tk.MustExec("execute st using @l, @r")
	tk.MustExec("execute st using @l, @r")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue40093(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (a int, b int, key(b, a))")
	tk.MustExec("prepare st from 'select * from t1 left join t2 on t1.a=t2.a where t2.b in (?)'")
	tk.MustExec("set @b=1")
	tk.MustExec("execute st using @b")

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{
			{"Projection_9"},
			{"└─HashJoin_21"},
			{"  ├─IndexReader_26(Build)"},
			{"  │ └─IndexRangeScan_25"}, // RangeScan instead of FullScan
			{"  └─TableReader_24(Probe)"},
			{"    └─Selection_23"},
			{"      └─TableFullScan_22"},
		})

	tk.MustExec("execute st using @b")
	tk.MustExec("execute st using @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestIssue38205(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
	tk.MustExec("CREATE TABLE `item` (`id` int, `vid` varbinary(16), `sid` int)")
	tk.MustExec("CREATE TABLE `lv` (`item_id` int, `sid` int, KEY (`sid`,`item_id`))")

	tk.MustExec("prepare stmt from 'SELECT /*+ TIDB_INLJ(lv, item) */ * FROM lv LEFT JOIN item ON lv.sid = item.sid AND lv.item_id = item.id WHERE item.sid = ? AND item.vid IN (?, ?)'")
	tk.MustExec("set @a=1, @b='1', @c='3'")
	tk.MustExec("execute stmt using @a, @b, @c")

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})

	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{
			{"IndexJoin_10"},
			{"├─TableReader_19(Build)"},
			{"│ └─Selection_18"},
			{"│   └─TableFullScan_17"}, // RangeScan instead of FullScan
			{"└─IndexReader_9(Probe)"},
			{"  └─Selection_8"},
			{"    └─IndexRangeScan_7"},
		})

	tk.MustExec("execute stmt using @a, @b, @c")
	tk.MustExec("execute stmt using @a, @b, @c")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestInsertStmtHint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")

	// without hint
	tk.MustExec("prepare st from 'insert into t values (1)'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// ignore-hint in insert-stmt can work
	tk.MustExec("prepare st from 'insert into t select * from t'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("prepare st from 'insert /*+ ignore_plan_cache() */ into t select * from t'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestLongInsertStmt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")

	tk.MustExec(`prepare inert200 from 'insert into t values (1)` + strings.Repeat(", (1)", 199) + "'")
	tk.MustExec(`execute inert200`)
	tk.MustExec(`execute inert200`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`prepare inert201 from 'insert into t values (1)` + strings.Repeat(", (1)", 200) + "'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: too many values in the insert statement"))
	tk.MustExec(`execute inert201`)
	tk.MustExec(`execute inert201`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func TestIssue38710(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists UK_NO_PRECISION_19392;")
	tk.MustExec("CREATE TABLE `UK_NO_PRECISION_19392` (\n  `COL1` bit(1) DEFAULT NULL,\n  `COL2` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,\n  `COL4` datetime DEFAULT NULL,\n  `COL3` bigint DEFAULT NULL,\n  `COL5` float DEFAULT NULL,\n  UNIQUE KEY `UK_COL1` (`COL1`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("INSERT INTO `UK_NO_PRECISION_19392` VALUES (0x00,'缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈','9294-12-26 06:50:40',-3088380202191555887,-3.33294e38),(NULL,'仲膩蕦圓猴洠飌镂喵疎偌嫺荂踖Ƕ藨蜿諪軁笞','1746-08-30 18:04:04',-4016793239832666288,-2.52633e38),(0x01,'冑溜畁脊乤纊繳蟥哅稐奺躁悼貘飗昹槐速玃沮','1272-01-19 23:03:27',-8014797887128775012,1.48868e38);\n")
	tk.MustExec(`prepare stmt from 'select * from UK_NO_PRECISION_19392 where col1 between ? and ? or col3 = ? or col2 in (?, ?, ?);';`)
	tk.MustExec("set @a=0x01, @b=0x01, @c=-3088380202191555887, @d=\"缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈\", @e=\"缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈\", @f=\"缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈\";")
	rows := tk.MustQuery(`execute stmt using @a,@b,@c,@d,@e,@f;`) // can not be cached because @a = @b
	require.Equal(t, 2, len(rows.Rows()))

	tk.MustExec(`set @a=NULL, @b=NULL, @c=-4016793239832666288, @d="缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈", @e="仲膩蕦圓猴洠飌镂喵疎偌嫺荂踖Ƕ藨蜿諪軁笞", @f="缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈";`)
	rows = tk.MustQuery(`execute stmt using @a,@b,@c,@d,@e,@f;`)
	require.Equal(t, 2, len(rows.Rows()))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))

	rows = tk.MustQuery(`execute stmt using @a,@b,@c,@d,@e,@f;`)
	require.Equal(t, 2, len(rows.Rows()))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec(`set @a=0x01, @b=0x01, @c=-3088380202191555887, @d="缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈", @e="缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈", @f="缎馗惫砲兣肬憵急鳸嫅稩邏蠧鄂艘腯灩專妴粈";`)
	rows = tk.MustQuery(`execute stmt using @a,@b,@c,@d,@e,@f;`)
	require.Equal(t, 2, len(rows.Rows()))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // can not use the cache because the types for @a and @b are not equal to the cached plan
}

func TestPlanCacheExprBlacklistCompatibility(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")

	tk.MustExec("prepare st from 'select * from t where mod(a, 2)=1'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("insert into mysql.expr_pushdown_blacklist(name) values('mod')")
	tk.MustExec(`admin reload expr_pushdown_blacklist`)
	tk.MustExec("execute st")                                              // no `mod can not be pushed-down` error
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // expr blacklist is updated
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestPlanCacheDiagInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, key(a), key(b))")

	tk.MustExec("prepare stmt from 'select /*+ ignore_plan_cache() */ * from t'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: ignore plan cache by hint"))

	tk.MustExec("prepare stmt from 'select * from t order by ?'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query has 'order by ?' is un-cacheable"))

	tk.MustExec("prepare stmt from 'select * from t where a=?'")
	tk.MustExec("set @a='123'")
	tk.MustExec("execute stmt using @a") // '123' -> 123
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: '123' may be converted to INT"))

	tk.MustExec("prepare stmt from 'select * from t where a=? and a=?'")
	tk.MustExec("set @a=1, @b=1")
	tk.MustExec("execute stmt using @a, @b") // a=1 and a=1 -> a=1
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: some parameters may be overwritten"))
}

func TestIssue40224(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key(a))")
	tk.MustExec("prepare st from 'select a from t where a in (?, ?)'")
	tk.MustExec("set @a=1.0, @b=2.0")
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: '1.0' may be converted to INT"))
	tk.MustExec("execute st using @a, @b")
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{
			{"IndexReader_6"},
			{"└─IndexRangeScan_5"}, // range scan not full scan
		})

	tk.MustExec("set @a=1, @b=2")
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("show warnings").Check(testkit.Rows()) // no warning for INT values
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1")) // cacheable for INT
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).CheckAt([]int{0},
		[][]interface{}{
			{"IndexReader_6"},
			{"└─IndexRangeScan_5"}, // range scan not full scan
		})
}

func TestIssue40225(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key(a))")
	tk.MustExec("prepare st from 'select * from t where a<?'")
	tk.MustExec("set @a='1'")
	tk.MustExec("execute st using @a")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows("Warning 1105 skip prepared plan-cache: '1' may be converted to INT")) // plan-cache limitation
	tk.MustExec("create binding for select * from t where a<1 using select /*+ ignore_plan_cache() */ * from t where a<1")
	tk.MustExec("execute st using @a")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows("Warning 1105 skip prepared plan-cache: ignore plan cache by binding"))
	// no warning about plan-cache limitations('1' -> INT) since plan-cache is totally disabled.

	tk.MustExec("prepare st from 'select * from t where a>?'")
	tk.MustExec("set @a=1")
	tk.MustExec("execute st using @a")
	tk.MustExec("execute st using @a")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("create binding for select * from t where a>1 using select /*+ ignore_plan_cache() */ * from t where a>1")
	tk.MustExec("execute st using @a")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	tk.MustExec("execute st using @a")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
}

func TestIssue40679(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, key(a));")
	tk.MustExec("prepare st from 'select * from t use index(a) where a < ?'")
	tk.MustExec("set @a1=1.1")
	tk.MustExec("execute st using @a1")

	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.True(t, strings.Contains(rows[1][0].(string), "RangeScan")) // RangeScan not FullScan

	tk.MustExec("execute st using @a1")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: '1.1' may be converted to INT"))
}

func TestIssue38335(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE PK_LP9463 (
  COL1 mediumint NOT NULL DEFAULT '77' COMMENT 'NUMERIC PK',
  COL2 varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,
  COL4 datetime DEFAULT NULL,
  COL3 bigint DEFAULT NULL,
  COL5 float DEFAULT NULL,
  PRIMARY KEY (COL1))`)
	tk.MustExec(`
INSERT INTO PK_LP9463 VALUES (-7415279,'笚綷想摻癫梒偆荈湩窐曋繾鏫蘌憬稁渣½隨苆','1001-11-02 05:11:33',-3745331437675076296,-3.21618e38),
(-7153863,'鯷氤衡椻闍饑堀鱟垩啵緬氂哨笂序鉲秼摀巽茊','6800-06-20 23:39:12',-7871155140266310321,-3.04829e38),
(77,'娥藨潰眤徕菗柢礥蕶浠嶲憅榩椻鍙鑜堋ᛀ暵氎','4473-09-13 01:18:59',4076508026242316746,-1.9525e38),
(16614,'阖旕雐盬皪豧篣哙舄糗悄蟊鯴瞶珧赺潴嶽簤彉','2745-12-29 00:29:06',-4242415439257105874,2.71063e37)`)
	tk.MustExec(`prepare stmt from 'SELECT *, rank() OVER (PARTITION BY col2 ORDER BY COL1) FROM PK_LP9463 WHERE col1 != ? AND col1 < ?'`)
	tk.MustExec(`set @a=-8414766051197, @b=-8388608`)
	tk.MustExec(`execute stmt using @a,@b`)
	tk.MustExec(`set @a=16614, @b=16614`)
	rows := tk.MustQuery(`execute stmt using @a,@b`).Sort()
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`SELECT *, rank() OVER (PARTITION BY col2 ORDER BY COL1) FROM PK_LP9463 WHERE col1 != 16614 and col1 < 16614`).Sort().Check(rows.Rows())
}

func TestIssue41032(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE PK_SIGNED_10087 (
	 COL1 mediumint(8) unsigned NOT NULL,
	 COL2 varchar(20) DEFAULT NULL,
	 COL4 datetime DEFAULT NULL,
	 COL3 bigint(20) DEFAULT NULL,
	 COL5 float DEFAULT NULL,
	 PRIMARY KEY (COL1) )`)
	tk.MustExec(`insert into PK_SIGNED_10087 values(0, "痥腜蟿鮤枓欜喧檕澙姭袐裄钭僇剕焍哓閲疁櫘", "0017-11-14 05:40:55", -4504684261333179273, 7.97449e37)`)
	tk.MustExec(`prepare stmt from 'SELECT/*+ HASH_JOIN(t1, t2) */ t2.* FROM PK_SIGNED_10087 t1 JOIN PK_SIGNED_10087 t2 ON t1.col1 = t2.col1 WHERE t2.col1 >= ? AND t1.col1 >= ?;'`)
	tk.MustExec(`set @a=0, @b=0`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows("0 痥腜蟿鮤枓欜喧檕澙姭袐裄钭僇剕焍哓閲疁櫘 0017-11-14 05:40:55 -4504684261333179273 79744900000000000000000000000000000000"))
	tk.MustExec(`set @a=8950167, @b=16305982`)
	tk.MustQuery(`execute stmt using @a,@b`).Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestSetPlanCacheLimitSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustQuery("select @@session.tidb_enable_plan_cache_for_param_limit").Check(testkit.Rows("1"))
	tk.MustQuery("select @@global.tidb_enable_plan_cache_for_param_limit").Check(testkit.Rows("1"))

	tk.MustExec("set @@session.tidb_enable_plan_cache_for_param_limit = OFF;")
	tk.MustQuery("select @@session.tidb_enable_plan_cache_for_param_limit").Check(testkit.Rows("0"))

	tk.MustExec("set @@session.tidb_enable_plan_cache_for_param_limit = 1;")
	tk.MustQuery("select @@session.tidb_enable_plan_cache_for_param_limit").Check(testkit.Rows("1"))

	tk.MustExec("set @@global.tidb_enable_plan_cache_for_param_limit = off;")
	tk.MustQuery("select @@global.tidb_enable_plan_cache_for_param_limit").Check(testkit.Rows("0"))

	tk.MustExec("set @@global.tidb_enable_plan_cache_for_param_limit = ON;")
	tk.MustQuery("select @@global.tidb_enable_plan_cache_for_param_limit").Check(testkit.Rows("1"))

	tk.MustGetErrMsg("set @@global.tidb_enable_plan_cache_for_param_limit = '';", "[variable:1231]Variable 'tidb_enable_plan_cache_for_param_limit' can't be set to the value of ''")
	tk.MustGetErrMsg("set @@global.tidb_enable_plan_cache_for_param_limit = 11;", "[variable:1231]Variable 'tidb_enable_plan_cache_for_param_limit' can't be set to the value of '11'")
	tk.MustGetErrMsg("set @@global.tidb_enable_plan_cache_for_param_limit = enabled;", "[variable:1231]Variable 'tidb_enable_plan_cache_for_param_limit' can't be set to the value of 'enabled'")
	tk.MustGetErrMsg("set @@global.tidb_enable_plan_cache_for_param_limit = disabled;", "[variable:1231]Variable 'tidb_enable_plan_cache_for_param_limit' can't be set to the value of 'disabled'")
	tk.MustGetErrMsg("set @@global.tidb_enable_plan_cache_for_param_limit = open;", "[variable:1231]Variable 'tidb_enable_plan_cache_for_param_limit' can't be set to the value of 'open'")
}

func TestPlanCacheLimitSwitchEffective(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, key(a))")

	checkIfCached := func(res string) {
		tk.MustExec("set @a = 1")
		tk.MustExec("execute stmt using @a")
		tk.MustExec("execute stmt using @a")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(res))
	}

	// before prepare
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_param_limit = OFF")
	tk.MustExec("prepare stmt from 'select * from t limit ?'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query has 'limit ?' is un-cacheable"))
	checkIfCached("0")
	tk.MustExec("deallocate prepare stmt")

	// after prepare
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_param_limit = ON")
	tk.MustExec("prepare stmt from 'select * from t limit ?'")
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_param_limit = OFF")
	checkIfCached("0")
	tk.MustExec("execute stmt using @a")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: the switch 'tidb_enable_plan_cache_for_param_limit' is off"))
	tk.MustExec("deallocate prepare stmt")

	// after execute
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_param_limit = ON")
	tk.MustExec("prepare stmt from 'select * from t limit ?'")
	checkIfCached("1")
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_param_limit = OFF")
	checkIfCached("0")
	tk.MustExec("deallocate prepare stmt")
}

func TestPlanCacheWithLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int)")

	testCases := []struct {
		sql    string
		params []int
	}{
		{"prepare stmt from 'select * from t limit ?'", []int{1}},
		{"prepare stmt from 'select * from t limit 1, ?'", []int{1}},
		{"prepare stmt from 'select * from t limit ?, 1'", []int{1}},
		{"prepare stmt from 'select * from t limit ?, ?'", []int{1, 2}},
		{"prepare stmt from 'delete from t order by a limit ?'", []int{1}},
		{"prepare stmt from 'insert into t select * from t order by a desc limit ?'", []int{1}},
		{"prepare stmt from 'insert into t select * from t order by a desc limit ?, ?'", []int{1, 2}},
		{"prepare stmt from 'update t set a = 1 limit ?'", []int{1}},
		{"prepare stmt from '(select * from t order by a limit ?) union (select * from t order by a desc limit ?)'", []int{1, 2}},
	}

	for idx, testCase := range testCases {
		tk.MustExec(testCase.sql)
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}

		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

		if idx < 9 {
			// none point get plan
			tk.MustExec("set @a0 = 6")
			tk.MustExec("execute stmt using " + strings.Join(using, ", "))
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		}
	}

	tk.MustExec("prepare stmt from 'select * from t limit ?'")
	tk.MustExec("set @a = 10001")
	tk.MustExec("execute stmt using @a")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: limit count is too large"))
}

func TestPlanCacheMemoryTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t1 (a int)`)
	tk.MustExec(`create table t2 (a int, b int)`)

	tk.MustExec(`prepare st from 'select count(*) from information_schema.COLUMNS where table_name=?'`)
	tk.MustExec(`set @a='t1'`)
	tk.MustQuery(`execute st using @a`).Check(testkit.Rows("1")) // 1 column
	tk.MustExec(`set @a='t2'`)
	tk.MustQuery(`execute st using @a`).Check(testkit.Rows("2"))           // 2 columns
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // plan accessing memory tables cannot hit the cache
}

func TestSetPlanCacheSubquerySwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustQuery("select @@session.tidb_enable_plan_cache_for_subquery").Check(testkit.Rows("1"))
	tk.MustQuery("select @@global.tidb_enable_plan_cache_for_subquery").Check(testkit.Rows("1"))

	tk.MustExec("set @@session.tidb_enable_plan_cache_for_subquery = OFF;")
	tk.MustQuery("select @@session.tidb_enable_plan_cache_for_subquery").Check(testkit.Rows("0"))

	tk.MustExec("set @@session.tidb_enable_plan_cache_for_subquery = 1;")
	tk.MustQuery("select @@session.tidb_enable_plan_cache_for_subquery").Check(testkit.Rows("1"))

	tk.MustExec("set @@global.tidb_enable_plan_cache_for_subquery = off;")
	tk.MustQuery("select @@global.tidb_enable_plan_cache_for_subquery").Check(testkit.Rows("0"))

	tk.MustExec("set @@global.tidb_enable_plan_cache_for_subquery = ON;")
	tk.MustQuery("select @@global.tidb_enable_plan_cache_for_subquery").Check(testkit.Rows("1"))

	tk.MustGetErrMsg("set @@global.tidb_enable_plan_cache_for_subquery = '';", "[variable:1231]Variable 'tidb_enable_plan_cache_for_subquery' can't be set to the value of ''")
	tk.MustGetErrMsg("set @@global.tidb_enable_plan_cache_for_subquery = 11;", "[variable:1231]Variable 'tidb_enable_plan_cache_for_subquery' can't be set to the value of '11'")
	tk.MustGetErrMsg("set @@global.tidb_enable_plan_cache_for_subquery = enabled;", "[variable:1231]Variable 'tidb_enable_plan_cache_for_subquery' can't be set to the value of 'enabled'")
	tk.MustGetErrMsg("set @@global.tidb_enable_plan_cache_for_subquery = disabled;", "[variable:1231]Variable 'tidb_enable_plan_cache_for_subquery' can't be set to the value of 'disabled'")
	tk.MustGetErrMsg("set @@global.tidb_enable_plan_cache_for_subquery = open;", "[variable:1231]Variable 'tidb_enable_plan_cache_for_subquery' can't be set to the value of 'open'")
}

func TestPlanCacheSubQuerySwitchEffective(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, key(a))")
	tk.MustExec("create table s(a int, key(a))")

	checkIfCached := func(res string) {
		tk.MustExec("execute stmt")
		tk.MustExec("execute stmt")
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(res))
	}

	// before prepare
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_subquery = OFF")
	tk.MustExec("prepare stmt from 'select * from t where a in (select a from s)'")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query has sub-queries is un-cacheable"))
	checkIfCached("0")
	tk.MustExec("deallocate prepare stmt")

	// after prepare
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_subquery = ON")
	tk.MustExec("prepare stmt from 'select * from t where a in (select a from s)'")
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_subquery = OFF")
	checkIfCached("0")
	tk.MustExec("execute stmt")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: the switch 'tidb_enable_plan_cache_for_subquery' is off"))
	tk.MustExec("deallocate prepare stmt")

	// after execute
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_subquery = ON")
	tk.MustExec("prepare stmt from 'select * from t where a in (select a from s)'")
	checkIfCached("1")
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_subquery = OFF")
	checkIfCached("0")
	tk.MustExec("deallocate prepare stmt")
}

func TestPlanCacheWithSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")

	testCases := []struct {
		sql            string
		params         []int
		cacheAble      string
		isDecorrelated bool
	}{
		{"select * from t t1 where exists (select 1 from t t2 where t2.b < t1.b and t2.b < ?)", []int{1}, "1", false},      // exist
		{"select * from t t1 where t1.a in (select a from t t2 where t2.b < ?)", []int{1}, "1", false},                     // in
		{"select * from t t1 where t1.a > (select max(a) from t t2 where t2.b < t1.b and t2.b < ?)", []int{1}, "0", false}, // scala
		{"select * from t t1 where t1.a > (select 1 from t t2 where t2.b<?)", []int{1}, "0", true},                         // uncorrelated
		{"select * from t t1 where exists (select b from t t2 where t1.a = t2.a and t2.b<? limit 1)", []int{1}, "1", false},
		{"select * from t t1 where exists (select b from t t2 where t1.a = t2.a and t2.b<? limit ?)", []int{1, 1}, "1", false},
	}

	// switch on
	for _, testCase := range testCases {
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", testCase.sql))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}

		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows(testCase.cacheAble))
		if testCase.cacheAble == "0" {
			tk.MustExec("execute stmt using " + strings.Join(using, ", "))
			if testCase.isDecorrelated {
				tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query has uncorrelated sub-queries is un-cacheable"))
			} else {
				tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: PhysicalApply plan is un-cacheable"))
			}
		}
	}
	// switch off
	tk.MustExec("set @@session.tidb_enable_plan_cache_for_subquery = 0")
	for _, testCase := range testCases {
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", testCase.sql))
		tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: query has sub-queries is un-cacheable"))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}

		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}
}

func TestIssue41828(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE IDT_MULTI15840STROBJSTROBJ (
  COL1 enum('aa', 'zzz') DEFAULT NULL,
  COL2 smallint(6) DEFAULT NULL,
  COL3 date DEFAULT NULL,
  KEY U_M_COL4 (COL1,COL2),
  KEY U_M_COL5 (COL3,COL2))`)

	tk.MustExec(`INSERT INTO IDT_MULTI15840STROBJSTROBJ VALUES ('zzz',1047,'6115-06-05'),('zzz',-23221,'4250-09-03'),('zzz',27138,'1568-07-30'),('zzz',-30903,'6753-08-21'),('zzz',-26875,'6117-10-10')`)
	tk.MustExec(`prepare stmt from 'select * from IDT_MULTI15840STROBJSTROBJ where col3 <=> ? or col1 in (?, ?, ?) and col2 not between ? and ?'`)
	tk.MustExec(`set @a="0051-12-23", @b="none", @c="none", @d="none", @e=-32757, @f=-32757`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d,@e,@f`).Check(testkit.Rows())
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1105 skip prepared plan-cache: IndexMerge plan with full-scan is un-cacheable`))
	tk.MustExec(`set @a="9795-01-10", @b="aa", @c="aa", @d="aa", @e=31928, @f=31928`)
	tk.MustQuery(`execute stmt using @a,@b,@c,@d,@e,@f`).Check(testkit.Rows())
}

func TestPlanCacheSubquerySPMEffective(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")

	testCases := []struct {
		sql    string
		params []int
	}{
		{"select * from t t1 where exists (select /*/ 1 from t t2 where t2.b < t1.b and t2.b < ?)", []int{1}}, // exist
		{"select * from t t1 where exists (select /*/ b from t t2 where t1.a = t2.a and t2.b < ? limit ?)", []int{1, 1}},
		{"select * from t t1 where t1.a in (select /*/ a from t t2 where t2.a > ? and t1.a = t2.a)", []int{1}},
		{"select * from t t1 where t1.a < (select /*/ sum(t2.a) from t t2 where t2.b = t1.b and t2.a > ?)", []int{1}},
	}

	// hint
	for _, testCase := range testCases {
		sql := strings.Replace(testCase.sql, "/*/", "/*+ NO_DECORRELATE() */", 1)
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", sql))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
	tk.MustExec("deallocate prepare stmt")

	// binding before prepare
	for _, testCase := range testCases {
		sql := strings.Replace(testCase.sql, "/*/", "", 1)
		bindSQL := strings.Replace(testCase.sql, "/*/", "/*+ NO_DECORRELATE() */", 1)
		tk.MustExec("create binding for " + sql + " using " + bindSQL)
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", sql))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}

	// binding after prepare
	for _, testCase := range testCases {
		sql := strings.Replace(testCase.sql, "/*/", "", 1)
		bindSQL := strings.Replace(testCase.sql, "/*/", "/*+ NO_DECORRELATE() */", 1)
		tk.MustExec(fmt.Sprintf("prepare stmt from '%s'", sql))
		var using []string
		for i, p := range testCase.params {
			tk.MustExec(fmt.Sprintf("set @a%d = %d", i, p))
			using = append(using, fmt.Sprintf("@a%d", i))
		}
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustExec("create binding for " + sql + " using " + bindSQL)
		tk.MustExec("execute stmt using " + strings.Join(using, ", "))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
}

func TestNonPreparedPlanCacheDMLSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t (a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=0")
	tk.MustExec(`insert into t values (1)`)
	tk.MustExec(`insert into t values (1)`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`select a from t where a < 2 for update`)
	tk.MustExec(`select a from t where a < 2 for update`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`set @x:=1`)
	tk.MustExec(`set @x:=1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	tk.MustExec("set tidb_enable_non_prepared_plan_cache_for_dml=1")
	tk.MustExec(`insert into t values (1)`)
	tk.MustExec(`insert into t values (1)`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`select a from t where a < 2 for update`)
	tk.MustExec(`select a from t where a < 2 for update`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheUnderscoreCharset(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t (a varchar(10), b int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select * from t where a = _utf8'a'`)
	tk.MustExec(`select * from t where a = _utf8'a'`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`select * from t where a = _latin1'a'`)
	tk.MustExec(`select * from t where a = _latin1'a'`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`select * from t where a = N'a'`) // equals to _utf8'a'
	tk.MustExec(`select * from t where a = N'a'`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`select * from t where a = 'a'`)
	tk.MustExec(`select * from t where a = 'a'`) // can hit if no underscore charset
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheGroupBy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, b int, index(a))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	tk.MustExec(`select sum(b) from t group by a+1`)
	tk.MustExec(`select sum(b) from t group by a+1`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`select sum(b) from t group by a+2`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`select sum(b) from t group by a+2`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func TestNonPreparedPlanCacheFieldNames(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, index(a))")
	tk.MustExec("create table tt(a varchar(10))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	checkFieldName := func(sql, hit string, fields ...string) {
		rs, err := tk.Exec(sql)
		require.NoError(t, err)
		for i, f := range rs.Fields() {
			require.Equal(t, f.Column.Name.L, fields[i])
		}
		require.NoError(t, rs.Close())
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows(hit))
	}

	checkFieldName(`select a+1 from t where a<10`, `0`, `a+1`)
	checkFieldName(`select a+1 from t where a<20`, `1`, `a+1`)
	checkFieldName(`select a+2 from t where a<30`, `0`, `a+2`) // can not hit since field names changed
	checkFieldName(`select a+2 from t where a<40`, `1`, `a+2`)
	checkFieldName(`select a,a+1 from t where a<30`, `0`, `a`, `a+1`) // can not hit since field names changed
	checkFieldName(`select a,a+1 from t where a<40`, `1`, `a`, `a+1`)
	checkFieldName(`select a+'123' from tt where a='1'`, `0`, `a+'123'`)
	checkFieldName(`select a+'123' from tt where a='2'`, `1`, `a+'123'`)

	checkFieldName(`select 1 from t where a<10`, `0`, `1`)
	checkFieldName(`select 1 from t where a<20`, `1`, `1`)
	checkFieldName(`select 2 from t where a<10`, `0`, `2`)
	checkFieldName(`select 2 from t where a<20`, `1`, `2`)
	checkFieldName(`select 1,2 from t where a<10`, `0`, `1`, `2`)
	checkFieldName(`select 1,2 from t where a<20`, `1`, `1`, `2`)
}

func TestNonPreparedPlanCacheExplain(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("create table t(a int, index(a))")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	formats := []string{
		"row",
		"brief",
		"dot",
		"tidb_json",
		"verbose",
		"cost_trace",
	}

	for _, format := range formats {
		tk.MustExec(fmt.Sprintf("explain format = '%v' select * from t", format))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}

	for _, format := range formats {
		tk.MustExec(fmt.Sprintf("explain format = '%v' select * from t limit 1", format))
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}
}

func TestIssue42125(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int, b int, c int, unique key(a, b))")

	// should use BatchPointGet
	tk.MustExec("prepare st from 'select * from t where a=1 and b in (?, ?)'")
	tk.MustExec("set @a=1, @b=2")
	tk.MustExec("execute st using @a, @b")
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows := tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, rows[0][0], "Batch_Point_Get_5") // use BatchPointGet
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 skip prepared plan-cache: Batch/PointGet plans may be over-optimized"))

	// should use PointGet: unsafe PointGet
	tk.MustExec("prepare st from 'select * from t where a=1 and b>=? and b<=?'")
	tk.MustExec("set @a=1, @b=1")
	tk.MustExec("execute st using @a, @b")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Equal(t, rows[0][0], "Point_Get_5") // use Point_Get_5
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0")) // cannot hit

	// safe PointGet
	tk.MustExec("prepare st from 'select * from t where a=1 and b=? and c<?'")
	tk.MustExec("set @a=1, @b=1")
	tk.MustExec("execute st using @a, @b")
	tkProcess = tk.Session().ShowProcess()
	ps = []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	require.Contains(t, rows[0][0], "Selection") // PointGet -> Selection
	require.Contains(t, rows[1][0], "Point_Get")
	tk.MustExec("execute st using @a, @b")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1")) // can hit
}

func TestNonPreparedPlanExplainWarning(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, c int, d int, e enum('1', '2', '3'), s set('1', '2', '3'), j json, bt bit(8), key(b), key(c, d))`)
	tk.MustExec("create table t1(a int, b int, index idx_b(b)) partition by range(a) ( partition p0 values less than (6), partition p1 values less than (11) )")
	tk.MustExec("create table t2(a int, b int) partition by hash(a) partitions 11")
	tk.MustExec("create table t3(a int, first_name varchar(50), last_name varchar(50), full_name varchar(101) generated always as (concat(first_name,' ',last_name)))")
	tk.MustExec("create or replace SQL SECURITY INVOKER view v as select a from t")
	tk.MustExec("analyze table t, t1, t2") // eliminate stats warnings
	tk.MustExec("set @@session.tidb_enable_non_prepared_plan_cache = 1")

	supported := []string{
		"select * from t where a<10",
		"select * from t where a<13 and b<15",
		"select * from t where b=13",
		"select * from t where c<8",
		"select * from t where d>8",
		"select * from t where c=8 and d>10",
		"select * from t where a<12 and b<13 and c<12 and d>2",
		"select * from t where a in (1, 2, 3)",
		"select * from t where a<13 or b<15",
		"select * from t where a<13 or b<15 and c=13",
		"select * from t where a in (1, 2)",
		"select * from t where a in (1, 2) and b in (1, 2, 3)",
		"select * from t where a in (1, 2) and b < 15",
		"select * from t where a between 1 and 10",
		"select * from t where a between 1 and 10 and b < 15",
		"select * from t where a+b=13",
		"select * from t where mod(a, 3)=1",
		"select * from t where d>now()",
		"select distinct a from t1 where a > 1 and b < 2",          // distinct
		"select count(*) from t1 where a > 1 and b < 2 group by a", // group by
		"select * from t1 order by a",                              // order by
	}

	unsupported := []string{
		"select /*+ use_index(t1, idx_b) */ * from t1 where a > 1 and b < 2",               // hint
		"select a, sum(b) as c from t1 where a > 1 and b < 2 group by a having sum(b) > 1", // having
		"select * from (select * from t1) t",                                               // sub-query
		"select * from t1 where a in (select a from t)",                                    // uncorrelated sub-query
		"select * from t1 where a in (select a from t where a > t1.a)",                     // correlated sub-query
		"select * from t where j < 1",                                                      // json
		"select * from t where a > 1 and j < 1",
		"select * from t where e < '1'", // enum
		"select * from t where a > 1 and e < '1'",
		"select * from t where s < '1'", // set
		"select * from t where a > 1 and s < '1'",
		"select * from t where bt > 0", // bit
		"select * from t where a > 1 and bt > 0",
		"select data_type from INFORMATION_SCHEMA.columns where table_name = 'v'", // memTable
		"select * from t3 where full_name = 'a b'",                                // generated column
		"select * from t3 where a > 1 and full_name = 'a b'",
		"select * from v",                // view
		"select * from t where a = null", // null
		"select * from t where false",    // table dual
	}

	reasons := []string{
		"skip non-prepared plan-cache: queries that have hints, having-clause, window-function are not supported",
		"skip non-prepared plan-cache: queries that have hints, having-clause, window-function are not supported",
		"skip non-prepared plan-cache: queries that have sub-queries are not supported",
		"skip non-prepared plan-cache: queries that access partitioning table are not supported",
		"skip non-prepared plan-cache: queries that access partitioning table are not supported",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: query has some filters with JSON, Enum, Set or Bit columns",
		"skip non-prepared plan-cache: access tables in system schema",
		"skip non-prepared plan-cache: queries that have generated columns are not supported",
		"skip non-prepared plan-cache: queries that have generated columns are not supported",
		"skip non-prepared plan-cache: queries that access views are not supported",
		"skip non-prepared plan-cache: query has null constants",
		"skip non-prepared plan-cache: some parameters may be overwritten when constant propagation",
	}

	all := append(supported, unsupported...)

	explainFormats := []string{
		types.ExplainFormatBrief,
		types.ExplainFormatDOT,
		types.ExplainFormatHint,
		types.ExplainFormatROW,
		types.ExplainFormatVerbose,
		types.ExplainFormatTraditional,
		types.ExplainFormatBinary,
		types.ExplainFormatTiDBJSON,
		types.ExplainFormatCostTrace,
	}
	// all cases no warnings use other format
	for _, q := range all {
		tk.MustExec("explain " + q)
		tk.MustQuery("show warnings").Check(testkit.Rows())
		tk.MustExec("explain " + q)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
	for _, format := range explainFormats {
		for _, q := range all {
			tk.MustExec(fmt.Sprintf("explain format = '%v' %v", format, q))
			//tk.MustQuery("show warnings").Check(testkit.Rows())
			tk.MustQuery("show warnings").CheckNotContain("plan cache")
			tk.MustExec(fmt.Sprintf("explain format = '%v' %v", format, q))
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
		}
	}

	// unsupported case with warning use 'plan_cache' format
	for idx, q := range unsupported {
		tk.MustExec("explain format = 'plan_cache'" + q)
		warn := tk.MustQuery("show warnings").Rows()[0]
		require.Equal(t, reasons[idx], warn[2])
	}
}

func TestIssue42150(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("CREATE TABLE `t1` (`c_int` int(11) NOT NULL,  `c_str` varchar(40) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,  `c_datetime` datetime DEFAULT NULL,  `c_timestamp` timestamp NULL DEFAULT NULL,  `c_double` double DEFAULT NULL,  `c_decimal` decimal(12,6) DEFAULT NULL,  `c_enum` enum('blue','green','red','yellow','white','orange','purple') NOT NULL,  PRIMARY KEY (`c_int`,`c_enum`) /*T![clustered_index] CLUSTERED */,  KEY `c_decimal` (`c_decimal`),  UNIQUE KEY `c_datetime` (`c_datetime`),  UNIQUE KEY `c_timestamp` (`c_timestamp`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("CREATE TABLE `t2` (`c_int` int(11) NOT NULL,  `c_str` varchar(40) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,  `c_datetime` datetime DEFAULT NULL,  `c_timestamp` timestamp NULL DEFAULT NULL,  `c_double` double DEFAULT NULL,  `c_decimal` decimal(12,6) DEFAULT NULL,  `c_enum` enum('blue','green','red','yellow','white','orange','purple') NOT NULL,  PRIMARY KEY (`c_int`,`c_enum`) /*T![clustered_index] CLUSTERED */,  KEY `c_decimal` (`c_decimal`),  UNIQUE KEY `c_datetime` (`c_datetime`),  UNIQUE KEY `c_timestamp` (`c_timestamp`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("create table t (a int, b int, primary key(a), key(b))")
	tk.MustExec("set @v0 = 'nice hellman', @v1 = 'flamboyant booth', @v2 = 'quirky brahmagupta'")
	tk.MustExec("prepare stmt16 from 'select * from t1 where c_enum in (select c_enum from t2 where t1.c_str in (?, ?, ?))'")
	tk.MustExec("execute stmt16 using @v0, @v1, @v2;")
	tk.MustExec("execute stmt16 using @v0, @v1, @v2;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("prepare stmt from 'select c_enum from t1'")
	tk.MustExec("execute stmt;")
	tk.MustExec("execute stmt;")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("prepare st from 'select a from t use index(b)'")
	tk.MustExec("execute st")
	tk.MustExec("execute st")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheDML(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache_for_dml=1`)
	tk.MustExec("create table t (a int default 0, b int default 0)")

	for _, sql := range []string{
		`select a from t for update`,
		`select a from t where a<10 for update`,
		`insert into t values (1, 1)`,
		`insert into t (a, b) values (1, 1)`,
		`insert into t (a) values (1)`,
		`insert into t (b) values (1)`,
		`insert into t select * from t`,
		`insert into t select * from t where a>10`,
		`update t set a=1`,
		`update t set a=1 where a>10`,
		`update t set a=1, b=1`,
		`update t set a=a+1 where a>10`,
		`delete from t`,
		`delete from t where a>10`,
	} {
		tk.MustExec(sql)
		tk.MustExec(sql)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	}
}

func TestNonPreparedPlanCachePanic(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)

	tk.MustExec("create table t (a varchar(255), b int, c char(10), primary key (c, a));")
	ctx := tk.Session().(sessionctx.Context)

	s := parser.New()
	for _, sql := range []string{
		"select 1 from t where a='x'",
		"select * from t where c='x'",
		"select * from t where a='x' and c='x'",
		"select * from t where a='x' and c='x' and b=1",
	} {
		stmtNode, err := s.ParseOneStmt(sql, "", "")
		require.NoError(t, err)
		preprocessorReturn := &plannercore.PreprocessorReturn{}
		err = plannercore.Preprocess(context.Background(), ctx, stmtNode, plannercore.WithPreprocessorReturn(preprocessorReturn))
		require.NoError(t, err)
		_, _, err = planner.Optimize(context.TODO(), ctx, stmtNode, preprocessorReturn.InfoSchema)
		require.NoError(t, err) // not panic
	}
}

func TestNonPreparedPlanCacheMultiStmt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache_for_dml=1`)
	tk.MustExec("create table t (a int)")

	tk.MustExec("update t set a=1 where a<10")
	tk.MustExec("update t set a=2 where a<12")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	// multi-stmt SQL cannot hit the cache
	tk.MustExec("update t set a=1 where a<10; update t set a=2 where a<12")
	tk.MustExec("update t set a=1 where a<10; update t set a=2 where a<12")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))

	tk.MustExec("update t set a=2 where a<12")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestNonPreparedPlanCacheJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec("create table t1 (a int, b int, c int)")
	tk.MustExec("create table t2 (a int, b int, c int)")
	tk.MustExec("create table t3 (a int, b int, c int)")
	tk.MustExec("create table t4 (a int, x int)")

	supported := []string{
		"select * from t1, t2 where t1.a=t2.a and t1.b<10",
		"select * from t1, t2",
		"select * from t1, t2 where t1.a<t2.a and t2.c=10",
		"select * from t1 tx, t1 ty",
		"select * from t1 tx, t1 ty where tx.a=ty.a",
		"select * from t1 inner join t2",
		"select * from t1 inner join t2 on t1.a=t2.a",
		"select * from t1 inner join t2 on t1.a=t2.a and t2.c<10",
		"select * from t1 left join t2 on t1.a=t2.a",
		"select * from t1 left join t2 on t1.a=t2.a and t2.c<10",
		"select * from t1, t4 where t1.a=t4.a and t4.x=10",
		"select * from t1 inner join t4 on t1.a=t4.a and t4.x=10",
	}
	unsupported := []string{
		"select * from t1, t2, t3",                // 3-way join
		"select * from t1, t2, t1 tx",             // 3-way join
		"select * from t1, (select * from t2) t2", // subquery
	}

	for _, sql := range supported {
		tk.MustExec(sql)
		tk.MustExec(sql)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	}
	for _, sql := range unsupported {
		tk.MustExec(sql)
		tk.MustExec(sql)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
}

func TestNonPreparedPlanCacheAggSort(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`set sql_mode=''`)
	tk.MustExec("create table t (a int, b int)")

	supported := []string{
		"select count(*) from t",
		"select max(b) from t group by a",
		"select count(*), a from t group by a, b",
		"select a from t order by a",
		"select a, b, a+b from t order by a, b",
	}
	unsupported := []string{
		"select sum(b) from t group by a+1",
		"select count(*) from t group by a+b",
		"select a, b, count(*) from t group by 1",              // group by position
		"select a, b, count(*) from t group by 1, a",           // group by position
		"select a, b, count(*) from t group by a having b < 1", // not support having-clause
		"select a from t order by a+1",
		"select a, b, a+b from t order by a + b",
	}

	for _, sql := range supported {
		tk.MustExec(sql)
		tk.MustExec(sql)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	}
	for _, sql := range unsupported {
		tk.MustExec(sql)
		tk.MustExec(sql)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
}

func TestNonPreparedPlanCacheOrder(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec("create table t (a int, b int)")

	supported := []string{
		"select a from t order by a",
		"select a from t order by a asc",
		"select a from t order by a desc",
		"select a from t order by a,b desc",
	}
	unsupported := []string{
		"select a from t order by 1", // order by position
		"select a from t order by a, 1",
		"select a from t order by a+1,b-2",
		"select a from t order by a desc, b+2",
	}

	for _, sql := range supported {
		tk.MustExec(sql)
		tk.MustExec(sql)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	}
	for _, sql := range unsupported {
		tk.MustExec(sql)
		tk.MustExec(sql)
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
}

func TestNonPreparedPlanCacheUnicode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`create table t1 (a varchar(10) character set latin1, b int)`)
	tk.MustExec(`insert into t1 values ('a',1)`)
	tk.MustQuery(`select concat(a, if(b>10, N'x', N'y')) from t1`).Check(testkit.Rows("ay")) // no error
	tk.MustQuery(`select concat(a, if(b>10, N'x', N'y')) from t1`).Check(testkit.Rows("ay"))
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestNonPreparedPlanCacheAutoStmtRetry(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("create table t(id int primary key, k int, UNIQUE KEY(k))")
	tk1.MustExec("insert into t values(1, 1)")

	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk2.MustExec("use test")
	tk1.MustExec("begin")
	tk1.MustExec("update t set k=3 where id=1")

	var wg sync.WaitGroup
	var tk2Err error
	wg.Add(1)
	go func() {
		// trigger statement auto-retry on tk2
		_, tk2Err = tk2.Exec("insert into t values(3, 3)")
		wg.Done()
	}()
	time.Sleep(100 * time.Millisecond)
	_, err := tk1.Exec("commit")
	require.NoError(t, err)
	wg.Wait()
	require.ErrorContains(t, tk2Err, "Duplicate entry")
}

func TestIssue43667Concurrency(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table cycle (pk int key, val int)")
	var wg sync.WaitGroup
	concurrency := 30
	for i := 0; i < concurrency; i++ {
		tk.MustExec(fmt.Sprintf("insert into cycle values (%v,%v)", i, i))
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("set @@tidb_enable_non_prepared_plan_cache=1")
			query := fmt.Sprintf("select (val) from cycle where pk = %v", id)
			for j := 0; j < 5000; j++ {
				tk.MustQuery(query).Check(testkit.Rows(fmt.Sprintf("%v", id)))
			}
		}(i)
	}
	wg.Wait()
}

func TestIssue43667(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`create table cycle (pk int not null primary key, sk int not null, val int)`)
	tk.MustExec(`insert into cycle values (4, 4, 4)`)
	tk.MustExec(`insert into cycle values (7, 7, 7)`)

	tk.MustQuery(`select (val) from cycle where pk = 4`).Check(testkit.Rows("4"))
	tk.MustQuery(`select (val) from cycle where pk = 7`).Check(testkit.Rows("7"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	updateAST := func(stmt ast.StmtNode) {
		v := stmt.(*ast.SelectStmt).Where.(*ast.BinaryOperationExpr).R.(*driver.ValueExpr)
		v.Datum.SetInt64(7)
	}

	tctx := context.WithValue(context.Background(), plannercore.PlanCacheKeyTestIssue43667, updateAST)
	tk.MustQueryWithContext(tctx, `select (val) from cycle where pk = 4`).Check(testkit.Rows("4"))
}

func TestIssue45253(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`CREATE TABLE t1 (c1 INT)`)
	tk.MustExec(`INSERT INTO t1 VALUES (1)`)

	tk.MustQuery(`SELECT c1 FROM t1 WHERE TO_BASE64('牵')`).Check(testkit.Rows("1"))
	tk.MustQuery(`SELECT c1 FROM t1 WHERE TO_BASE64('牵')`).Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery(`SELECT c1 FROM t1 WHERE TO_BASE64('哈')`).Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustQuery(`SELECT c1 FROM t1 WHERE TO_BASE64('')`).Check(testkit.Rows())
}

func TestIssue45378(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`CREATE TABLE t1(c1 INT)`)
	tk.MustExec(`INSERT INTO t1 VALUES (1)`)

	tk.MustQuery(`SELECT c1 FROM t1 WHERE UNHEX(2038330881)`).Check(testkit.Rows("1"))
	tk.MustQuery(`SELECT c1 FROM t1 WHERE UNHEX(2038330881)`).Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestIssue46159(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a varchar(10), key(a(5)))`)
	tk.MustExec(`prepare st from 'select a from t use index(a) where a=?'`)
	tk.MustExec(`set @a='a'`)
	tk.MustQuery(`execute st using @a`).Check(testkit.Rows())
	tk.MustQuery(`execute st using @a`).Check(testkit.Rows())
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip plan-cache: plan rebuild failed, rebuild to get an unsafe range"))
}

func TestBuiltinFuncFlen(t *testing.T) {
	// same as TestIssue45378 and TestIssue45253
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1(c1 INT)`)
	tk.MustExec(`INSERT INTO t1 VALUES (1)`)

	funcs := []string{ast.Abs, ast.Acos, ast.Asin, ast.Atan, ast.Ceil, ast.Ceiling, ast.Cos,
		ast.CRC32, ast.Degrees, ast.Floor, ast.Ln, ast.Log, ast.Log2, ast.Log10, ast.Unhex,
		ast.Radians, ast.Rand, ast.Round, ast.Sign, ast.Sin, ast.Sqrt, ast.Tan, ast.SM3,
		ast.Quote, ast.RTrim, ast.ToBase64, ast.Trim, ast.Upper, ast.Ucase, ast.Hex,
		ast.BitLength, ast.CharLength, ast.Compress, ast.MD5, ast.SHA1, ast.SHA}
	args := []string{"2038330881", "'2038330881'", "'牵'", "-1", "''", "0"}

	for _, f := range funcs {
		for _, a := range args {
			q := fmt.Sprintf("SELECT c1 from t1 where %s(%s)", f, a)
			tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
			r1 := tk.MustQuery(q)
			tk.MustExec(`set tidb_enable_non_prepared_plan_cache=0`)
			r2 := tk.MustQuery(q)
			r1.Sort().Check(r2.Sort().Rows())
		}
	}
}

func TestNonPreparedPlanCacheBuiltinFuncs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set tidb_enable_non_prepared_plan_cache=1`)
	tk.MustExec(`create table t (a int, b varchar(32), c datetime, key(a))`)

	// normal builtin functions can be supported
	supportedCases := []string{
		`select * from t where mod(a, 5) < 2`,
		`select * from t where c < now()`,
		`select date_format(c, '%Y-%m-%d') from t where a < 10`,
		`select str_to_date(b, '%Y-%m-%d') from t where a < 10`,
		`select * from t where a-2 < 20`,
		`select * from t where a+b > 100`,
	}
	for _, sql := range supportedCases {
		tk.MustExec(sql)
		tk.MustExec(sql)
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	}

	// unsupported cases
	unsupportedCases := []string{
		`select * from t where -a > 10`,                  // '-' cannot support
		`select * from t where a < 1 and b like '%abc%'`, // LIKE
		`select database() from t`,
	}
	for _, sql := range unsupportedCases {
		tk.MustExec(sql)
		tk.MustExec(sql)
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	}
}

func BenchmarkPlanCacheInsert(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")

	tk.MustExec("prepare st from 'insert into t values (1)'")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec("execute st")
	}
}

func BenchmarkNonPreparedPlanCacheDML(b *testing.B) {
	store := testkit.CreateMockStore(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int)")
	tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tk.MustExec("insert into t values (1)")
		tk.MustExec("update t set a = 2 where a = 1")
		tk.MustExec("delete from t where a = 2")
	}
}
