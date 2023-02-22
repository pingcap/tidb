// Copyright 2019 PingCAP, Inc.
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
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestShowSubquery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(10), b int, c int)")
	tk.MustQuery("show columns from t where true").Check(testkit.Rows(
		"a varchar(10) YES  <nil> ",
		"b int(11) YES  <nil> ",
		"c int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field = 'b'").Check(testkit.Rows(
		"b int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field in (select 'b')").Check(testkit.Rows(
		"b int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field in (select 'b') and true").Check(testkit.Rows(
		"b int(11) YES  <nil> ",
	))
	tk.MustQuery("show columns from t where field in (select 'b') and false").Check(testkit.Rows())
	tk.MustExec("insert into t values('c', 0, 0)")
	tk.MustQuery("show columns from t where field < all (select a from t)").Sort().Check(testkit.Rows(
		"a varchar(10) YES  <nil> ",
		"b int(11) YES  <nil> ",
	))
	tk.MustExec("insert into t values('b', 0, 0)")
	tk.MustQuery("show columns from t where field < all (select a from t)").Check(testkit.Rows(
		"a varchar(10) YES  <nil> ",
	))
}

func TestJoinOperatorRightAssociative(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1,10),(2,20)")
	// make sure this join won't rewrite as left-associative join like (t0 join t1) join t2 when explicit parent existed.
	// mysql will detect the t0.a is out of it's join parent scope and errors like ERROR 1054 (42S22): Unknown column 't0.a' in 'on clause'
	err := tk.ExecToErr("select t1.* from t t0 cross join (t t1 join t t2 on 100=t0.a);")
	require.Error(t, err)
	require.EqualError(t, err, "[planner:1054]Unknown column 't0.a' in 'on clause'")
}

func TestPpdWithSetVar(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(c1 int, c2 varchar(255))")
	tk.MustExec("insert into t values(1,'a'),(2,'d'),(3,'c')")

	tk.MustQuery("select t01.c1,t01.c2,t01.c3 from (select t1.*,@c3:=@c3+1 as c3 from (select t.*,@c3:=0 from t order by t.c1)t1)t01 where t01.c3=1 and t01.c2='d'").Check(testkit.Rows())
	tk.MustQuery("select t01.c1,t01.c2,t01.c3 from (select t1.*,@c3:=@c3+1 as c3 from (select t.*,@c3:=0 from t order by t.c1)t1)t01 where t01.c3=2 and t01.c2='d'").Check(testkit.Rows("2 d 2"))
}

func TestBitColErrorMessage(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists bit_col_t")
	tk.MustExec("create table bit_col_t (a bit(64))")
	tk.MustExec("drop table bit_col_t")
	tk.MustExec("create table bit_col_t (a bit(1))")
	tk.MustExec("drop table bit_col_t")
	tk.MustGetErrCode("create table bit_col_t (a bit(0))", mysql.ErrInvalidFieldSize)
	tk.MustGetErrCode("create table bit_col_t (a bit(65))", mysql.ErrTooBigDisplaywidth)
}

func TestAggPushDownLeftJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists customer")
	tk.MustExec("create table customer (C_CUSTKEY bigint(20) NOT NULL, C_NAME varchar(25) NOT NULL, " +
		"C_ADDRESS varchar(25) NOT NULL, PRIMARY KEY (`C_CUSTKEY`) /*T![clustered_index] CLUSTERED */)")
	tk.MustExec("drop table if exists orders")
	tk.MustExec("create table orders (O_ORDERKEY bigint(20) NOT NULL, O_CUSTKEY bigint(20) NOT NULL, " +
		"O_TOTALPRICE decimal(15,2) NOT NULL, PRIMARY KEY (`O_ORDERKEY`) /*T![clustered_index] CLUSTERED */)")
	tk.MustExec("insert into customer values (6, \"xiao zhang\", \"address1\");")
	tk.MustExec("set @@tidb_opt_agg_push_down=1;")

	tk.MustQuery("select c_custkey, count(o_orderkey) as c_count from customer left outer join orders " +
		"on c_custkey = o_custkey group by c_custkey").Check(testkit.Rows("6 0"))
	tk.MustQuery("explain format='brief' select c_custkey, count(o_orderkey) as c_count from customer left outer join orders " +
		"on c_custkey = o_custkey group by c_custkey").Check(testkit.Rows(
		"Projection 10000.00 root  test.customer.c_custkey, Column#7",
		"└─Projection 10000.00 root  if(isnull(Column#8), 0, 1)->Column#7, test.customer.c_custkey",
		"  └─HashJoin 10000.00 root  left outer join, equal:[eq(test.customer.c_custkey, test.orders.o_custkey)]",
		"    ├─HashAgg(Build) 8000.00 root  group by:test.orders.o_custkey, funcs:count(Column#9)->Column#8, funcs:firstrow(test.orders.o_custkey)->test.orders.o_custkey",
		"    │ └─TableReader 8000.00 root  data:HashAgg",
		"    │   └─HashAgg 8000.00 cop[tikv]  group by:test.orders.o_custkey, funcs:count(test.orders.o_orderkey)->Column#9",
		"    │     └─TableFullScan 10000.00 cop[tikv] table:orders keep order:false, stats:pseudo",
		"    └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"      └─TableFullScan 10000.00 cop[tikv] table:customer keep order:false, stats:pseudo"))

	tk.MustQuery("select c_custkey, count(o_orderkey) as c_count from orders right outer join customer " +
		"on c_custkey = o_custkey group by c_custkey").Check(testkit.Rows("6 0"))
	tk.MustQuery("explain format='brief' select c_custkey, count(o_orderkey) as c_count from orders right outer join customer " +
		"on c_custkey = o_custkey group by c_custkey").Check(testkit.Rows(
		"Projection 10000.00 root  test.customer.c_custkey, Column#7",
		"└─Projection 10000.00 root  if(isnull(Column#8), 0, 1)->Column#7, test.customer.c_custkey",
		"  └─HashJoin 10000.00 root  right outer join, equal:[eq(test.orders.o_custkey, test.customer.c_custkey)]",
		"    ├─HashAgg(Build) 8000.00 root  group by:test.orders.o_custkey, funcs:count(Column#9)->Column#8, funcs:firstrow(test.orders.o_custkey)->test.orders.o_custkey",
		"    │ └─TableReader 8000.00 root  data:HashAgg",
		"    │   └─HashAgg 8000.00 cop[tikv]  group by:test.orders.o_custkey, funcs:count(test.orders.o_orderkey)->Column#9",
		"    │     └─TableFullScan 10000.00 cop[tikv] table:orders keep order:false, stats:pseudo",
		"    └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"      └─TableFullScan 10000.00 cop[tikv] table:customer keep order:false, stats:pseudo"))
}

func TestIssue22298(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t;`)
	tk.MustExec(`create table t(a int, b int);`)
	tk.MustGetErrMsg(`select * from t where 0 and c = 10;`, "[planner:1054]Unknown column 'c' in 'where clause'")
}

func TestIssue24571(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create view v as select 1 as b;`)
	tk.MustExec(`create table t (a int);`)
	tk.MustExec(`update v, t set a=2;`)
	tk.MustGetErrCode(`update v, t set b=2;`, mysql.ErrNonUpdatableTable)
	tk.MustExec("create database db1")
	tk.MustExec("use db1")
	tk.MustExec("update test.t, (select 1 as a) as t set test.t.a=1;")
	// bug in MySQL: ERROR 1288 (HY000): The target table t of the UPDATE is not updatable
	tk.MustExec("update (select 1 as a) as t, test.t set test.t.a=1;")
}

func TestBuildUpdateListResolver(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// For issue https://github.com/pingcap/tidb/issues/24567
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t(a int)")
	tk.MustExec("create table t1(b int)")
	tk.MustGetErrCode("update (select 1 as a) as t set a=1", mysql.ErrNonUpdatableTable)
	tk.MustGetErrCode("update (select 1 as a) as t, t1 set a=1", mysql.ErrNonUpdatableTable)
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t1")

	// For issue https://github.com/pingcap/tidb/issues/30031
	tk.MustExec("create table t(a int default -1, c int as (a+10) stored)")
	tk.MustExec("insert into t(a) values(1)")
	tk.MustExec("update test.t, (select 1 as b) as t set test.t.a=default")
	tk.MustQuery("select * from t").Check(testkit.Rows("-1 9"))
	tk.MustExec("drop table if exists t")
}

func TestIssue22828(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`create table t (c int);`)
	tk.MustGetErrMsg(`select group_concat((select concat(c,group_concat(c)) FROM t where xxx=xxx)) FROM t;`, "[planner:1054]Unknown column 'xxx' in 'where clause'")
}

func TestIssue35623(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1;`)
	tk.MustExec(`drop view if exists v1;`)
	tk.MustExec(`CREATE TABLE t1(c0 INT UNIQUE);`)
	tk.MustExec("CREATE definer='root'@'localhost' VIEW v1(c0) AS SELECT 1 FROM t1;")
	err := tk.ExecToErr("SELECT v1.c0 FROM v1 WHERE (true)LIKE(v1.c0);")
	require.NoError(t, err)

	err = tk.ExecToErr("SELECT v2.c0 FROM (select 1 as c0 from t1) v2 WHERE (v2.c0)like(True);")
	require.NoError(t, err)
}

func TestIssue37971(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t3;`)
	tk.MustExec(`CREATE TABLE t3(c0 INT, primary key(c0));`)
	err := tk.ExecToErr("SELECT v2.c0 FROM (select 1 as c0 from t3) v2 WHERE (v2.c0)like(True);")
	require.NoError(t, err)
}

func TestJoinNotNullFlag(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(x int not null)")
	tk.MustExec("create table t2(x int)")
	tk.MustExec("insert into t2 values (1)")

	tk.MustQuery("select IFNULL((select t1.x from t1 where t1.x = t2.x), 'xxx') as col1 from t2").Check(testkit.Rows("xxx"))
	tk.MustQuery("select ifnull(t1.x, 'xxx') from t2 left join t1 using(x)").Check(testkit.Rows("xxx"))
	tk.MustQuery("select ifnull(t1.x, 'xxx') from t2 natural left join t1").Check(testkit.Rows("xxx"))
}

func TestAntiJoinConstProp(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int not null, b int not null)")
	tk.MustExec("insert into t1 values (1,1)")
	tk.MustExec("create table t2(a int not null, b int not null)")
	tk.MustExec("insert into t2 values (2,2)")

	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t2.a = t1.a and t2.a > 1)").Check(testkit.Rows(
		"1 1",
	))
	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t2.b = t1.b and t2.a > 1)").Check(testkit.Rows(
		"1 1",
	))
	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t2.b = t1.b and t2.b > 1)").Check(testkit.Rows(
		"1 1",
	))
	tk.MustQuery("select q.a in (select count(*) from t1 s where not exists (select 1 from t1 p where q.a > 1 and p.a = s.a)) from t1 q").Check(testkit.Rows(
		"1",
	))
	tk.MustQuery("select q.a in (select not exists (select 1 from t1 p where q.a > 1 and p.a = s.a) from t1 s) from t1 q").Check(testkit.Rows(
		"1",
	))

	tk.MustExec("drop table t1, t2")
	tk.MustExec("create table t1(a int not null, b int)")
	tk.MustExec("insert into t1 values (1,null)")
	tk.MustExec("create table t2(a int not null, b int)")
	tk.MustExec("insert into t2 values (2,2)")

	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t2.b > t1.b)").Check(testkit.Rows(
		"1 <nil>",
	))
	tk.MustQuery("select * from t1 where t1.a not in (select a from t2 where t1.a = 2)").Check(testkit.Rows(
		"1 <nil>",
	))
}

func TestNoneAccessPathsFoundByIsolationRead(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")

	tk.MustExec("select * from t")

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")

	// Don't filter mysql.SystemDB by isolation read.
	tk.MustQuery("explain format = 'brief' select * from mysql.stats_meta").Check(testkit.Rows(
		"TableReader 10000.00 root  data:TableFullScan",
		"└─TableFullScan 10000.00 cop[tikv] table:stats_meta keep order:false, stats:pseudo"))

	_, err := tk.Exec("select * from t")
	require.EqualError(t, err, "[planner:1815]Internal : No access path for table 't' is found with 'tidb_isolation_read_engines' = 'tiflash', valid values can be 'tikv'. Please check tiflash replica or ensure the query is readonly.")

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash, tikv'")
	tk.MustExec("select * from t")
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.IsolationRead.Engines = []string{"tiflash"}
	})
	// Change instance config doesn't affect isolation read.
	tk.MustExec("select * from t")
}

func TestPartitionTableDynamicModeUnderNewCollation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database test_new_collation")
	tk.MustExec("use test_new_collation")
	tk.MustExec("set @@tidb_partition_prune_mode = 'dynamic'")

	// hash + range partition
	tk.MustExec(`CREATE TABLE thash (a int, c varchar(20) charset utf8mb4 collate utf8mb4_general_ci, key(a)) partition by hash(a) partitions 4`)
	tk.MustExec(`CREATE TABLE trange (a int, c varchar(20) charset utf8mb4 collate utf8mb4_general_ci, key(a)) partition by range(a) (
						partition p0 values less than (10),
						partition p1 values less than (20),
						partition p2 values less than (30),
						partition p3 values less than (40))`)
	tk.MustExec(`insert into thash values (1, 'a'), (1, 'A'), (11, 'a'), (11, 'A'), (21, 'a'), (21, 'A'), (31, 'a'), (31, 'A')`)
	tk.MustExec(`insert into trange values (1, 'a'), (1, 'A'), (11, 'a'), (11, 'A'), (21, 'a'), (21, 'A'), (31, 'a'), (31, 'A')`)
	tk.MustQuery(`select * from thash use index(a) where a in (1, 11, 31) and c='a'`).Sort().Check(testkit.Rows("1 A", "1 a", "11 A", "11 a", "31 A", "31 a"))
	tk.MustQuery(`select * from thash ignore index(a) where a in (1, 11, 31) and c='a'`).Sort().Check(testkit.Rows("1 A", "1 a", "11 A", "11 a", "31 A", "31 a"))
	tk.MustQuery(`select * from trange use index(a) where a in (1, 11, 31) and c='a'`).Sort().Check(testkit.Rows("1 A", "1 a", "11 A", "11 a", "31 A", "31 a"))
	tk.MustQuery(`select * from trange ignore index(a) where a in (1, 11, 31) and c='a'`).Sort().Check(testkit.Rows("1 A", "1 a", "11 A", "11 a", "31 A", "31 a"))

	// range partition and partitioned by utf8mb4_general_ci
	tk.MustExec(`create table strrange(a varchar(10) charset utf8mb4 collate utf8mb4_general_ci, b int) partition by range columns(a) (
						partition p0 values less than ('a'),
						partition p1 values less than ('k'),
						partition p2 values less than ('z'))`)
	tk.MustExec("insert into strrange values ('a', 1), ('A', 1), ('y', 1), ('Y', 1), ('q', 1)")
	tk.MustQuery("select * from strrange where a in ('a', 'y')").Sort().Check(testkit.Rows("A 1", "Y 1", "a 1", "y 1"))

	// list partition and partitioned by utf8mb4_general_ci
	tk.MustExec(`create table strlist(a varchar(10) charset utf8mb4 collate utf8mb4_general_ci, b int) partition by list columns (a) (
						partition p0 values in ('a', 'b'),
						partition p1 values in ('c', 'd'),
						partition p2 values in ('e', 'f'))`)
	tk.MustExec("insert into strlist values ('a', 1), ('A', 1), ('d', 1), ('D', 1), ('e', 1)")
	tk.MustQuery(`select * from strlist where a='a'`).Sort().Check(testkit.Rows("A 1", "a 1"))
	tk.MustQuery(`select * from strlist where a in ('D', 'e')`).Sort().Check(testkit.Rows("D 1", "d 1", "e 1"))
}

func TestAggPushDownEngine(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b varchar(20))")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")

	tk.MustQuery("explain format = 'brief' select approx_count_distinct(a) from t").Check(testkit.Rows(
		"StreamAgg 1.00 root  funcs:approx_count_distinct(Column#5)->Column#3",
		"└─TableReader 1.00 root  data:StreamAgg",
		"  └─StreamAgg 1.00 batchCop[tiflash]  funcs:approx_count_distinct(test.t.a)->Column#5",
		"    └─TableFullScan 10000.00 batchCop[tiflash] table:t keep order:false, stats:pseudo"))

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tikv'")

	tk.MustQuery("explain format = 'brief' select approx_count_distinct(a) from t").Check(testkit.Rows(
		"HashAgg 1.00 root  funcs:approx_count_distinct(test.t.a)->Column#3",
		"└─TableReader 10000.00 root  data:TableFullScan",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo"))
}

func TestIssue15110(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists crm_rd_150m")
	tk.MustExec(`CREATE TABLE crm_rd_150m (
	product varchar(256) DEFAULT NULL,
		uks varchar(16) DEFAULT NULL,
		brand varchar(256) DEFAULT NULL,
		cin varchar(16) DEFAULT NULL,
		created_date timestamp NULL DEFAULT NULL,
		quantity int(11) DEFAULT NULL,
		amount decimal(11,0) DEFAULT NULL,
		pl_date timestamp NULL DEFAULT NULL,
		customer_first_date timestamp NULL DEFAULT NULL,
		recent_date timestamp NULL DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "crm_rd_150m" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("explain format = 'brief' SELECT count(*) FROM crm_rd_150m dataset_48 WHERE (CASE WHEN (month(dataset_48.customer_first_date)) <= 30 THEN '新客' ELSE NULL END) IS NOT NULL;")
}

func TestIssue40910(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t(a int, b int, index idx_a(a), index idx_b(b));`)

	tk.MustExec("select * from t where a > 1 and a < 10 order by b;")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	tk.MustExec("create session binding for select * from t where a > 1 and a < 10 order by b using select /*+ use_index(t, idx_a) */ * from t where a > 1 and a < 10 order by b;")
	tk.MustExec("select * from t where a > 1 and a < 10 order by b;")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))

	tk.MustExec("select /*+ use_index(t, idx_b) */ * from t where a > 1 and a < 10 order by b;")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))

	tk.MustExec("select /*+ use_index(t, idx_b) */ * from t where a > 1 and a < 10 order by b;")
	tk.MustQuery("show warnings").Check(testkit.Rows(
		"Warning 1105 The system ignores the hints in the current query and uses the hints specified in the bindSQL: SELECT /*+ use_index(`t` `idx_a`)*/ * FROM `test`.`t` WHERE `a` > 1 AND `a` < 10 ORDER BY `b`"))
}

func TestSplitJoinHint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t(a int, b int, index idx_a(a), index idx_b(b));`)

	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
	tk.MustExec("select /*+ hash_join(t1) merge_join(t2) */ * from t t1 join t t2 join t t3 where t1.a = t2.a and t2.a=t3.a")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 Join hints are conflict, you can only specify one type of join"))

	tk.MustExec(`set @@tidb_opt_advanced_join_hint=1`)
	tk.MustExec("select /*+ hash_join(t1) merge_join(t2) */ * from t t1 join t t2 join t t3 where t1.a = t2.a and t2.a=t3.a")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 Join hints conflict after join reorder phase, you can only specify one type of join"))

	tk.MustExec(`set @@tidb_opt_advanced_join_hint=0`)
}

func TestKeepOrderHintWithBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int, b int, index idx_a(a));")

	// create binding for order_index hint
	tk.MustExec("select * from t1 where a<10 order by a limit 1;")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	tk.MustExec("create global binding for select * from t1 where a<10 order by a limit 1 using select /*+ order_index(t1, idx_a) */ * from t1 where a<10 order by a limit 1;")
	tk.MustExec("select * from t1 where a<10 order by a limit 1;")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res := tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from `test` . `t1` where `a` < ? order by `a` limit ?")
	require.Equal(t, res[0][1], "SELECT /*+ order_index(`t1` `idx_a`)*/ * FROM `test`.`t1` WHERE `a` < 10 ORDER BY `a` LIMIT 1")

	tk.MustExec("drop global binding for select * from t1 where a<10 order by a limit 1;")
	tk.MustExec("select * from t1 where a<10 order by a limit 1;")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, len(res), 0)

	// create binding for no_order_index hint
	tk.MustExec("create global binding for select * from t1 where a<10 order by a limit 1 using select /*+ no_order_index(t1, idx_a) */ * from t1 where a<10 order by a limit 1;")
	tk.MustExec("select * from t1 where a<10 order by a limit 1;")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from `test` . `t1` where `a` < ? order by `a` limit ?")
	require.Equal(t, res[0][1], "SELECT /*+ no_order_index(`t1` `idx_a`)*/ * FROM `test`.`t1` WHERE `a` < 10 ORDER BY `a` LIMIT 1")

	tk.MustExec("drop global binding for select * from t1 where a<10 order by a limit 1;")
	tk.MustExec("select * from t1 where a<10 order by a limit 1;")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, len(res), 0)
}

func TestViewHintWithBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop view if exists v, v1")
	tk.MustExec("drop table if exists t, t1, t2, t3")
	tk.MustExec("create table t(a int, b int);")
	tk.MustExec("create table t1(a int, b int);")
	tk.MustExec("create table t2(a int, b int);")
	tk.MustExec("create table t3(a int, b int)")
	tk.MustExec("create definer='root'@'localhost' view v as select t.a, t.b from t join (select count(*) as a from t1 join t2 join t3 where t1.b=t2.b and t2.a = t3.a group by t2.a) tt on t.a = tt.a;")
	tk.MustExec("create definer='root'@'localhost' view v1 as select t.a, t.b from t join (select count(*) as a from t1 join v on t1.b=v.b group by v.a) tt on t.a = tt.a;")
	tk.MustExec("create definer='root'@'localhost' view v2 as select t.a, t.b from t join (select count(*) as a from t1 join v1 on t1.b=v1.b group by v1.a) tt on t.a = tt.a;")

	tk.MustExec("select * from v2")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	tk.MustExec("create global binding for select * from v2 using select /*+ qb_name(qb_v_2, v2.v1@sel_2 .v@sel_2 .@sel_2), merge_join(t1@qb_v_2), stream_agg(@qb_v_2), qb_name(qb_v_1, v2. v1@sel_2 .v@sel_2 .@sel_1), merge_join(t@qb_v_1) */ * from v2;")
	tk.MustExec("select * from v2")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
	res := tk.MustQuery("show global bindings").Rows()
	require.Equal(t, res[0][0], "select * from `test` . `v2`")
	require.Equal(t, res[0][1], "SELECT /*+ qb_name(`qb_v_2` , `v2`. `v1`@`sel_2`. `v`@`sel_2`. ``@`sel_2`) merge_join(`t1`@`qb_v_2`) stream_agg(@`qb_v_2`) qb_name(`qb_v_1` , `v2`. `v1`@`sel_2`. `v`@`sel_2`. ``@`sel_1`) merge_join(`t`@`qb_v_1`)*/ * FROM `test`.`v2`")

	tk.MustExec("drop global binding for select * from v2")
	tk.MustExec("select * from v2")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))
	res = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, len(res), 0)
}

func TestPartitionPruningWithDateType(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime) partition by range columns (a) (partition p1 values less than ('20000101'), partition p2 values less than ('2000-10-01'));")
	tk.MustExec("insert into t values ('20000201'), ('19000101');")

	// cannot get the statistical information immediately
	// tk.MustQuery(`SELECT PARTITION_NAME,TABLE_ROWS FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = 't';`).Check(testkit.Rows("p1 1", "p2 1"))
	str := tk.MustQuery(`desc select * from t where a < '2000-01-01';`).Rows()[0][3].(string)
	require.True(t, strings.Contains(str, "partition:p1"))
}

func TestPartitionPruningForEQ(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a datetime, b int) partition by range(weekday(a)) (partition p0 values less than(10), partition p1 values less than (100))")

	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	pt := tbl.(table.PartitionedTable)
	query, err := expression.ParseSimpleExprWithTableInfo(tk.Session(), "a = '2020-01-01 00:00:00'", tbl.Meta())
	require.NoError(t, err)
	dbName := model.NewCIStr(tk.Session().GetSessionVars().CurrentDB)
	columns, names, err := expression.ColumnInfos2ColumnsAndNames(tk.Session(), dbName, tbl.Meta().Name, tbl.Meta().Cols(), tbl.Meta())
	require.NoError(t, err)
	// Even the partition is not monotonous, EQ condition should be prune!
	// select * from t where a = '2020-01-01 00:00:00'
	res, err := core.PartitionPruning(tk.Session(), pt, []expression.Expression{query}, nil, columns, names)
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, 0, res[0])
}

func TestErrNoDB(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create user test")
	_, err := tk.Exec("grant select on test1111 to test@'%'")
	require.Equal(t, core.ErrNoDB, errors.Cause(err))
	_, err = tk.Exec("grant select on * to test@'%'")
	require.Equal(t, core.ErrNoDB, errors.Cause(err))
	_, err = tk.Exec("revoke select on * from test@'%'")
	require.Equal(t, core.ErrNoDB, errors.Cause(err))
	tk.MustExec("use test")
	tk.MustExec("create table test1111 (id int)")
	tk.MustExec("grant select on test1111 to test@'%'")
}

func TestINLJHintSmallTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int not null, b int, key(a))")
	tk.MustExec("insert into t1 values(1,1),(2,2)")
	tk.MustExec("create table t2(a int not null, b int, key(a))")
	tk.MustExec("insert into t2 values(1,1),(2,2),(3,3),(4,4),(5,5)")
	tk.MustExec("analyze table t1, t2")
	tk.MustExec("explain format = 'brief' select /*+ TIDB_INLJ(t1) */ * from t1 join t2 on t1.a = t2.a")
}

func TestInvisibleIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	// Optimizer cannot see invisible indexes.
	tk.MustExec("create table t(a int, b int, unique index i_a (a) invisible, unique index i_b(b))")
	tk.MustExec("insert into t values (1,2)")

	// For issue 26217, can't use invisible index after admin check table.
	tk.MustExec("admin check table t")

	// Optimizer cannot use invisible indexes.
	tk.MustQuery("select a from t order by a").Check(testkit.Rows("1"))
	require.False(t, tk.MustUseIndex("select a from t order by a", "i_a"))
	tk.MustQuery("select a from t where a > 0").Check(testkit.Rows("1"))
	require.False(t, tk.MustUseIndex("select a from t where a > 1", "i_a"))

	// If use invisible indexes in index hint and sql hint, throw an error.
	errStr := "[planner:1176]Key 'i_a' doesn't exist in table 't'"
	tk.MustGetErrMsg("select * from t use index(i_a)", errStr)
	tk.MustGetErrMsg("select * from t force index(i_a)", errStr)
	tk.MustGetErrMsg("select * from t ignore index(i_a)", errStr)
	tk.MustQuery("select /*+ USE_INDEX(t, i_a) */ * from t")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, errStr)
	tk.MustQuery("select /*+ IGNORE_INDEX(t, i_a), USE_INDEX(t, i_b) */ a from t order by a")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, errStr)
	tk.MustQuery("select /*+ FORCE_INDEX(t, i_a), USE_INDEX(t, i_b) */ a from t order by a")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, errStr)
	// For issue 15519
	inapplicableErrStr := "[planner:1815]force_index(test.aaa) is inapplicable, check whether the table(test.aaa) exists"
	tk.MustQuery("select /*+ FORCE_INDEX(aaa) */ * from t")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 1)
	require.EqualError(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings()[0].Err, inapplicableErrStr)

	tk.MustExec("admin check table t")
	tk.MustExec("admin check index t i_a")
}

func TestTopNByConstFunc(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select max(t.col) from (select 'a' as col union all select '' as col) as t").Check(testkit.Rows(
		"a",
	))
}

func TestIssue32672(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	for _, agg := range []string{"stream", "hash"} {
		rs := tk.MustQuery(fmt.Sprintf("explain format='verbose' select /*+ %v_agg() */ count(*) from t", agg)).Rows()
		// cols: id, estRows, estCost, ...
		operator := rs[0][0].(string)
		cost, err := strconv.ParseFloat(rs[0][2].(string), 64)
		require.NoError(t, err)
		require.True(t, strings.Contains(strings.ToLower(operator), agg))
		require.True(t, cost > 0)
	}
}

func TestIssue15546(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, pt, vt")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1, 1)")
	tk.MustExec("create table pt(a int primary key, b int) partition by range(a) (" +
		"PARTITION `p0` VALUES LESS THAN (10), PARTITION `p1` VALUES LESS THAN (20), PARTITION `p2` VALUES LESS THAN (30))")
	tk.MustExec("insert into pt values(1, 1), (11, 11), (21, 21)")
	tk.MustExec("create definer='root'@'localhost' view vt(a, b) as select a, b from t")
	tk.MustQuery("select * from pt, vt where pt.a = vt.a").Check(testkit.Rows("1 1 1 1"))
}

func TestApproxCountDistinctInPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int(11), b int) partition by range (a) (partition p0 values less than (3), partition p1 values less than maxvalue);")
	tk.MustExec("insert into t values(1, 1), (2, 1), (3, 1), (4, 2), (4, 2)")
	tk.MustExec("set session tidb_opt_agg_push_down=1")
	tk.MustExec(`set @@tidb_partition_prune_mode='` + string(variable.Static) + `'`)
	tk.MustQuery("explain format = 'brief' select approx_count_distinct(a), b from t group by b order by b desc").Check(testkit.Rows("Sort 16000.00 root  test.t.b:desc",
		"└─HashAgg 16000.00 root  group by:test.t.b, funcs:approx_count_distinct(Column#5)->Column#4, funcs:firstrow(Column#6)->test.t.b",
		"  └─PartitionUnion 16000.00 root  ",
		"    ├─HashAgg 8000.00 root  group by:test.t.b, funcs:approx_count_distinct(test.t.a)->Column#5, funcs:firstrow(test.t.b)->Column#6, funcs:firstrow(test.t.b)->test.t.b",
		"    │ └─TableReader 10000.00 root  data:TableFullScan",
		"    │   └─TableFullScan 10000.00 cop[tikv] table:t, partition:p0 keep order:false, stats:pseudo",
		"    └─HashAgg 8000.00 root  group by:test.t.b, funcs:approx_count_distinct(test.t.a)->Column#5, funcs:firstrow(test.t.b)->Column#6, funcs:firstrow(test.t.b)->test.t.b",
		"      └─TableReader 10000.00 root  data:TableFullScan",
		"        └─TableFullScan 10000.00 cop[tikv] table:t, partition:p1 keep order:false, stats:pseudo"))
	tk.MustQuery("select approx_count_distinct(a), b from t group by b order by b desc").Check(testkit.Rows("1 2", "3 1"))
}

func TestIssue17813(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists hash_partition_overflow")
	tk.MustExec("create table hash_partition_overflow (c0 bigint unsigned) partition by hash(c0) partitions 3")
	tk.MustExec("insert into hash_partition_overflow values (9223372036854775808)")
	tk.MustQuery("select * from hash_partition_overflow where c0 = 9223372036854775808").Check(testkit.Rows("9223372036854775808"))
	tk.MustQuery("select * from hash_partition_overflow where c0 in (1, 9223372036854775808)").Check(testkit.Rows("9223372036854775808"))
}

func TestIssue15813(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("create table t0(c0 int primary key)")
	tk.MustExec("create table t1(c0 int primary key)")
	tk.MustExec("CREATE INDEX i0 ON t0(c0)")
	tk.MustExec("CREATE INDEX i0 ON t1(c0)")
	tk.MustQuery("select /*+ MERGE_JOIN(t0, t1) */ * from t0, t1 where t0.c0 = t1.c0").Check(testkit.Rows())
}

func TestIssue31261(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists PK_MULTI_COL_5177`)
	tk.MustExec(`	CREATE TABLE PK_MULTI_COL_5177 (
		COL1 binary(10) NOT NULL,
		COL2 varbinary(10) NOT NULL,
		COL3 smallint(45) NOT NULL,
		PRIMARY KEY (COL1(5),COL2,COL3),
		UNIQUE KEY UIDXM (COL1(5),COL2),
		UNIQUE KEY UIDX (COL2),
		KEY IDX3 (COL3),
		KEY IDXM (COL3,COL2))`)
	tk.MustExec(`insert into PK_MULTI_COL_5177(col1, col2, col3) values(0x00000000000000000000, 0x002B200DF5BA03E59F82, 1)`)
	require.Len(t, tk.MustQuery(`select col1, col2 from PK_MULTI_COL_5177 where col1 = 0x00000000000000000000 and col2 in (0x002B200DF5BA03E59F82, 0x002B200DF5BA03E59F82, 0x002B200DF5BA03E59F82)`).Rows(), 1)
	require.Len(t, tk.MustQuery(`select col1, col2 from PK_MULTI_COL_5177 where col1 = 0x00000000000000000000 and col2 = 0x002B200DF5BA03E59F82`).Rows(), 1)
}

func TestFullGroupByOrderBy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustQuery("select count(a) as b from t group by a order by b").Check(testkit.Rows())
	err := tk.ExecToErr("select count(a) as cnt from t group by a order by b")
	require.True(t, terror.ErrorEqual(err, core.ErrFieldNotInGroupBy))
}

func TestIssue15858(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	tk.MustExec("select * from t t1, (select a from t order by a+1) t2 where t1.a = t2.a")
}

func TestIssue15846(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("CREATE TABLE t0(t0 INT UNIQUE);")
	tk.MustExec("CREATE TABLE t1(c0 FLOAT);")
	tk.MustExec("INSERT INTO t1(c0) VALUES (0);")
	tk.MustExec("INSERT INTO t0(t0) VALUES (NULL), (NULL);")
	tk.MustQuery("SELECT t1.c0 FROM t1 LEFT JOIN t0 ON 1;").Check(testkit.Rows("0", "0"))

	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("CREATE TABLE t0(t0 INT);")
	tk.MustExec("CREATE TABLE t1(c0 FLOAT);")
	tk.MustExec("INSERT INTO t1(c0) VALUES (0);")
	tk.MustExec("INSERT INTO t0(t0) VALUES (NULL), (NULL);")
	tk.MustQuery("SELECT t1.c0 FROM t1 LEFT JOIN t0 ON 1;").Check(testkit.Rows("0", "0"))

	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("CREATE TABLE t0(t0 INT);")
	tk.MustExec("CREATE TABLE t1(c0 FLOAT);")
	tk.MustExec("create unique index idx on t0(t0);")
	tk.MustExec("INSERT INTO t1(c0) VALUES (0);")
	tk.MustExec("INSERT INTO t0(t0) VALUES (NULL), (NULL);")
	tk.MustQuery("SELECT t1.c0 FROM t1 LEFT JOIN t0 ON 1;").Check(testkit.Rows("0", "0"))
}

func TestFloorUnixTimestampPruning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists floor_unix_timestamp")
	tk.MustExec(`create table floor_unix_timestamp (ts timestamp(3))
partition by range (floor(unix_timestamp(ts))) (
partition p0 values less than (unix_timestamp('2020-04-05 00:00:00')),
partition p1 values less than (unix_timestamp('2020-04-12 00:00:00')),
partition p2 values less than (unix_timestamp('2020-04-15 00:00:00')))`)
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-04 00:00:00')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-04 23:59:59.999')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-05 00:00:00')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-05 00:00:00.001')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-12 01:02:03.456')")
	tk.MustExec("insert into floor_unix_timestamp values ('2020-04-14 00:00:42')")
	tk.MustQuery("select count(*) from floor_unix_timestamp where '2020-04-05 00:00:00.001' = ts").Check(testkit.Rows("1"))
	tk.MustQuery("select * from floor_unix_timestamp where ts > '2020-04-05 00:00:00' order by ts").Check(testkit.Rows("2020-04-05 00:00:00.001", "2020-04-12 01:02:03.456", "2020-04-14 00:00:42.000"))
	tk.MustQuery("select count(*) from floor_unix_timestamp where ts <= '2020-04-05 23:00:00'").Check(testkit.Rows("4"))
	tk.MustQuery("select * from floor_unix_timestamp partition(p1, p2) where ts > '2020-04-14 00:00:00'").Check(testkit.Rows("2020-04-14 00:00:42.000"))
}

func TestIssue16290And16292(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, primary key(a));")
	tk.MustExec("insert into t values(1, 1);")

	for i := 0; i <= 1; i++ {
		tk.MustExec(fmt.Sprintf("set session tidb_opt_agg_push_down = %v", i))

		tk.MustQuery("select avg(a) from (select * from t ta union all select * from t tb) t;").Check(testkit.Rows("1.0000"))
		tk.MustQuery("select avg(b) from (select * from t ta union all select * from t tb) t;").Check(testkit.Rows("1.0000"))
		tk.MustQuery("select count(distinct a) from (select * from t ta union all select * from t tb) t;").Check(testkit.Rows("1"))
		tk.MustQuery("select count(distinct b) from (select * from t ta union all select * from t tb) t;").Check(testkit.Rows("1"))
	}
}

func TestTableDualWithRequiredProperty(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (a int, b int) partition by range(a) " +
		"(partition p0 values less than(10), partition p1 values less than MAXVALUE)")
	tk.MustExec("create table t2 (a int, b int)")
	tk.MustExec("select /*+ MERGE_JOIN(t1, t2) */ * from t1 partition (p0), t2  where t1.a > 100 and t1.a = t2.a")
}

func TestIssue16837(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int,b int,c int,d int,e int,unique key idx_ab(a,b),unique key(c),unique key(d))")
	tk.MustQuery("explain format = 'brief' select /*+ use_index_merge(t,c,idx_ab) */ * from t where a = 1 or (e = 1 and c = 1)").Check(testkit.Rows(
		"IndexMerge 0.01 root  type: union",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:idx_ab(a, b) range:[1,1], keep order:false, stats:pseudo",
		"├─IndexRangeScan(Build) 1.00 cop[tikv] table:t, index:c(c) range:[1,1], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.01 cop[tikv]  or(eq(test.t.a, 1), and(eq(test.t.e, 1), eq(test.t.c, 1)))",
		"  └─TableRowIDScan 11.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("insert into t values (2, 1, 1, 1, 2)")
	tk.MustQuery("select /*+ use_index_merge(t,c,idx_ab) */ * from t where a = 1 or (e = 1 and c = 1)").Check(testkit.Rows())
}

func TestIndexMergePartialScansClusteredIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, primary key (a, b) clustered, key idx_c(c));")
	tk.MustExec("insert into t values (1, 1, 1), (10, 10, 10), (100, 100, 100);")
	const queryTemplate = "select /*+ use_index_merge(t) */ %s from t where %s order by a, b;"
	projections := [][]string{{"a"}, {"b"}, {"c"}, {"a", "b"}, {"b", "c"}, {"c", "a"}, {"b", "a", "c"}}
	cases := []struct {
		condition string
		expected  []string
	}{
		{
			// 3 table scans
			"a < 2 or a < 10 or a > 11", []string{"1", "100"},
		},
		{
			// 3 index scans
			"c < 10 or c < 11 or c > 50", []string{"1", "10", "100"},
		},
		{
			// 1 table scan + 1 index scan
			"a < 2 or c > 10000", []string{"1"},
		},
		{
			// 2 table scans + 1 index scan
			"a < 2 or a > 88 or c > 10000", []string{"1", "100"},
		},
		{
			// 2 table scans + 2 index scans
			"a < 2 or (a >= 10 and b >= 10) or c > 100 or c < 1", []string{"1", "10", "100"},
		},
		{
			// 3 table scans + 2 index scans
			"a < 2 or (a >= 10 and b >= 10) or (a >= 20 and b < 10) or c > 100 or c < 1", []string{"1", "10", "100"},
		},
	}
	for _, p := range projections {
		for _, ca := range cases {
			query := fmt.Sprintf(queryTemplate, strings.Join(p, ","), ca.condition)
			tk.HasPlan(query, "IndexMerge")
			expected := make([]string, 0, len(ca.expected))
			for _, datum := range ca.expected {
				row := strings.Repeat(datum+" ", len(p))
				expected = append(expected, row[:len(row)-1])
			}
			tk.MustQuery(query).Check(testkit.Rows(expected...))
		}
	}
}

func TestIndexMergePartialScansTiDBRowID(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, unique key (a, b), key idx_c(c));")
	tk.MustExec("insert into t values (1, 1, 1), (10, 10, 10), (100, 100, 100);")
	const queryTemplate = "select /*+ use_index_merge(t) */ %s from t where %s order by a;"
	projections := [][]string{{"a"}, {"b"}, {"c"}, {"a", "b"}, {"b", "c"}, {"c", "a"}, {"b", "a", "c"}}
	cases := []struct {
		condition string
		expected  []string
	}{
		{
			// 3 index scans
			"c < 10 or c < 11 or c > 50", []string{"1", "10", "100"},
		},
		{
			// 2 index scans
			"c < 10 or a < 2", []string{"1"},
		},
		{
			// 1 table scan + 1 index scan
			"_tidb_rowid < 2 or c > 10000", []string{"1"},
		},
		{
			// 2 table scans + 1 index scan
			"_tidb_rowid < 2 or _tidb_rowid < 10 or c > 11", []string{"1", "10", "100"},
		},
		{
			// 1 table scans + 3 index scans
			"_tidb_rowid < 2 or (a >= 10 and b >= 10) or c > 100 or c < 1", []string{"1", "10", "100"},
		},
		{
			// 1 table scans + 4 index scans
			"_tidb_rowid < 2 or (a >= 10 and b >= 10) or (a >= 20 and b < 10) or c > 100 or c < 1", []string{"1", "10", "100"},
		},
	}
	for _, p := range projections {
		for _, ca := range cases {
			query := fmt.Sprintf(queryTemplate, strings.Join(p, ","), ca.condition)
			tk.HasPlan(query, "IndexMerge")
			expected := make([]string, 0, len(ca.expected))
			for _, datum := range ca.expected {
				row := strings.Repeat(datum+" ", len(p))
				expected = append(expected, row[:len(row)-1])
			}
			tk.MustQuery(query).Check(testkit.Rows(expected...))
		}
	}
}

func TestIndexMergePartialScansPKIsHandle(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, c int, primary key (a), unique key (b), key idx_c(c));")
	tk.MustExec("insert into t values (1, 1, 1), (10, 10, 10), (100, 100, 100);")
	const queryTemplate = "select /*+ use_index_merge(t) */ %s from t where %s order by b;"
	projections := [][]string{{"a"}, {"b"}, {"c"}, {"a", "b"}, {"b", "c"}, {"c", "a"}, {"b", "a", "c"}}
	cases := []struct {
		condition string
		expected  []string
	}{
		{
			// 3 index scans
			"b < 10 or c < 11 or c > 50", []string{"1", "10", "100"},
		},
		{
			// 1 table scan + 1 index scan
			"a < 2 or c > 10000", []string{"1"},
		},
		{
			// 2 table scans + 1 index scan
			"a < 2 or a < 10 or b > 11", []string{"1", "100"},
		},
		{
			// 1 table scans + 3 index scans
			"a < 2 or b >= 10 or c > 100 or c < 1", []string{"1", "10", "100"},
		},
		{
			// 3 table scans + 2 index scans
			"a < 2 or a >= 10 or a >= 20 or c > 100 or b < 1", []string{"1", "10", "100"},
		},
	}
	for _, p := range projections {
		for _, ca := range cases {
			query := fmt.Sprintf(queryTemplate, strings.Join(p, ","), ca.condition)
			tk.HasPlan(query, "IndexMerge")
			expected := make([]string, 0, len(ca.expected))
			for _, datum := range ca.expected {
				row := strings.Repeat(datum+" ", len(p))
				expected = append(expected, row[:len(row)-1])
			}
			tk.MustQuery(query).Check(testkit.Rows(expected...))
		}
	}
}

func TestIssue23919(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")

	// Test for the minimal reproducible case.
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int, index(a), index(b)) partition by hash (a) partitions 2;")
	tk.MustExec("insert into t values (1, 5);")
	tk.MustQuery("select /*+ use_index_merge( t ) */ * from t where a in (3) or b in (5) order by a;").
		Check(testkit.Rows("1 5"))

	// Test for the original case.
	tk.MustExec("drop table if exists t;")
	tk.MustExec(`CREATE TABLE t (
  col_5 text NOT NULL,
  col_6 tinyint(3) unsigned DEFAULT NULL,
  col_7 float DEFAULT '4779.165058537128',
  col_8 smallint(6) NOT NULL DEFAULT '-24790',
  col_9 date DEFAULT '2031-01-15',
  col_37 int(11) DEFAULT '1350204687',
  PRIMARY KEY (col_5(6),col_8) /*T![clustered_index] NONCLUSTERED */,
  UNIQUE KEY idx_6 (col_9,col_7,col_8),
  KEY idx_8 (col_8,col_6,col_5(6),col_9,col_7),
  KEY idx_9 (col_9,col_7,col_8)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY RANGE ( col_8 ) (
  PARTITION p0 VALUES LESS THAN (-17650),
  PARTITION p1 VALUES LESS THAN (-13033),
  PARTITION p2 VALUES LESS THAN (2521),
  PARTITION p3 VALUES LESS THAN (7510)
);`)
	tk.MustExec("insert into t values ('', NULL, 6304.0146, -24790, '2031-01-15', 1350204687);")
	tk.MustQuery("select  var_samp(col_7) aggCol from (select  /*+ use_index_merge( t ) */ * from t where " +
		"t.col_9 in ( '2002-06-22' ) or t.col_5 in ( 'PkfzI'  ) or t.col_8 in ( -24874 ) and t.col_6 > null and " +
		"t.col_5 > 'r' and t.col_9 in ( '1979-09-04' ) and t.col_7 < 8143.667552769195 or " +
		"t.col_5 in ( 'iZhfEjRWci' , 'T' , ''  ) or t.col_9 <> '1976-09-11' and t.col_7 = 8796.436181615773 and " +
		"t.col_8 = 7372 order by col_5,col_8  ) ordered_tbl group by col_6;").Check(testkit.Rows("<nil>"))
}

func TestIssue16407(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int,b char(100),key(a),key(b(10)))")
	tk.MustQuery("explain format = 'brief' select /*+ use_index_merge(t) */ * from t where a=10 or b='x'").Check(testkit.Rows(
		"IndexMerge 0.04 root  type: union",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:a(a) range:[10,10], keep order:false, stats:pseudo",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:b(b) range:[\"x\",\"x\"], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.04 cop[tikv]  or(eq(test.t.a, 10), eq(test.t.b, \"x\"))",
		"  └─TableRowIDScan 19.99 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("insert into t values (1, 'xx')")
	tk.MustQuery("select /*+ use_index_merge(t) */ * from t where a=10 or b='x'").Check(testkit.Rows())
}

func TestNotReadOnlySQLOnTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b varchar(20))")
	tk.MustExec(`set @@tidb_isolation_read_engines = "tiflash"`)
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	err := tk.ExecToErr("select * from t for update")
	require.EqualError(t, err, `[planner:1815]Internal : No access path for table 't' is found with 'tidb_isolation_read_engines' = 'tiflash', valid values can be 'tiflash, tikv'. Please check tiflash replica or ensure the query is readonly.`)

	err = tk.ExecToErr("insert into t select * from t")
	require.EqualError(t, err, `[planner:1815]Internal : No access path for table 't' is found with 'tidb_isolation_read_engines' = 'tiflash', valid values can be 'tiflash, tikv'. Please check tiflash replica or ensure the query is readonly.`)

	tk.MustExec("prepare stmt_insert from 'insert into t select * from t where t.a = ?'")
	tk.MustExec("set @a=1")
	err = tk.ExecToErr("execute stmt_insert using @a")
	require.EqualError(t, err, `[planner:1815]Internal : No access path for table 't' is found with 'tidb_isolation_read_engines' = 'tiflash', valid values can be 'tiflash, tikv'. Please check tiflash replica or ensure the query is readonly.`)
}

func TestSelectLimit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1),(1),(2)")

	// normal test
	tk.MustExec("set @@session.sql_select_limit=1")
	result := tk.MustQuery("select * from t order by a")
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from t order by a limit 2")
	result.Check(testkit.Rows("1", "1"))
	tk.MustExec("set @@session.sql_select_limit=default")
	result = tk.MustQuery("select * from t order by a")
	result.Check(testkit.Rows("1", "1", "2"))

	// test for subquery
	tk.MustExec("set @@session.sql_select_limit=1")
	result = tk.MustQuery("select * from (select * from t) s order by a")
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from (select * from t limit 2) s order by a") // limit write in subquery, has no effect.
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select (select * from t limit 1) s") // limit write in subquery, has no effect.
	result.Check(testkit.Rows("1"))
	result = tk.MustQuery("select * from t where t.a in (select * from t) limit 3") // select_limit will not effect subquery
	result.Sort().Check(testkit.Rows("1", "1", "2"))
	result = tk.MustQuery("select * from (select * from t) s limit 3") // select_limit will not effect subquery
	result.Sort().Check(testkit.Rows("1", "1", "2"))

	// test for union
	result = tk.MustQuery("select * from t union all select * from t limit 2") // limit outside subquery
	result.Check(testkit.Rows("1", "1"))
	result = tk.MustQuery("select * from t union all (select * from t limit 2)") // limit inside subquery
	result.Check(testkit.Rows("1"))

	// test for prepare & execute
	tk.MustExec("prepare s1 from 'select * from t where a = ?'")
	tk.MustExec("set @a = 1")
	result = tk.MustQuery("execute s1 using @a")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@session.sql_select_limit=default")
	result = tk.MustQuery("execute s1 using @a")
	result.Check(testkit.Rows("1", "1"))
	tk.MustExec("set @@session.sql_select_limit=1")
	tk.MustExec("prepare s2 from 'select * from t where a = ? limit 3'")
	result = tk.MustQuery("execute s2 using @a") // if prepare stmt has limit, select_limit takes no effect.
	result.Check(testkit.Rows("1", "1"))

	// test for create view
	tk.MustExec("set @@session.sql_select_limit=1")
	tk.MustExec("create definer='root'@'localhost' view s as select * from t") // select limit should not effect create view
	result = tk.MustQuery("select * from s")
	result.Check(testkit.Rows("1"))
	tk.MustExec("set @@session.sql_select_limit=default")
	result = tk.MustQuery("select * from s")
	result.Sort().Check(testkit.Rows("1", "1", "2"))

	// test for DML
	tk.MustExec("set @@session.sql_select_limit=1")
	tk.MustExec("create table b (a int)")
	tk.MustExec("insert into b select * from t") // all values are inserted
	result = tk.MustQuery("select * from b limit 3")
	result.Sort().Check(testkit.Rows("1", "1", "2"))
	tk.MustExec("update b set a = 2 where a = 1") // all values are updated
	result = tk.MustQuery("select * from b limit 3")
	result.Check(testkit.Rows("2", "2", "2"))
	result = tk.MustQuery("select * from b")
	result.Check(testkit.Rows("2"))
	tk.MustExec("delete from b where a = 2") // all values are deleted
	result = tk.MustQuery("select * from b")
	result.Check(testkit.Rows())
}

func TestHintParserWarnings(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, key(a), key(b));")
	tk.MustExec("select /*+ use_index_merge() */ * from t where a = 1 or b = 1;")
	rows := tk.MustQuery("show warnings;").Rows()
	require.Len(t, rows, 1)
}

func TestIssue16935(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 INT);")
	tk.MustExec("INSERT INTO t0(c0) VALUES (1), (1), (1), (1), (1), (1);")
	tk.MustExec("CREATE definer='root'@'localhost' VIEW v0(c0) AS SELECT NULL FROM t0;")
	tk.MustQuery("SELECT * FROM t0 LEFT JOIN v0 ON TRUE WHERE v0.c0 IS NULL;")
}

func TestClusterIndexUniqueDoubleRead(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database cluster_idx_unique_double_read;")
	tk.MustExec("use cluster_idx_unique_double_read;")
	defer tk.MustExec("drop database cluster_idx_unique_double_read;")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("drop table if exists t")

	tk.MustExec("create table t (a varchar(64), b varchar(64), uk int, v int, primary key(a, b), unique key uuk(uk));")
	tk.MustExec("insert t values ('a', 'a1', 1, 11), ('b', 'b1', 2, 22), ('c', 'c1', 3, 33);")
	tk.MustQuery("select * from t use index (uuk);").Check(testkit.Rows("a a1 1 11", "b b1 2 22", "c c1 3 33"))
}

func TestIssue18984(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t2")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("create table t(a int, b int, c int, primary key(a, b))")
	tk.MustExec("create table t2(a int, b int, c int, d int, primary key(a,b), index idx(c))")
	tk.MustExec("insert into t values(1,1,1), (2,2,2), (3,3,3)")
	tk.MustExec("insert into t2 values(1,2,3,4), (2,4,3,5), (1,3,1,1)")
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t) */ * from t right outer join t2 on t.a=t2.c").Check(testkit.Rows(
		"1 1 1 1 3 1 1",
		"3 3 3 1 2 3 4",
		"3 3 3 2 4 3 5"))
	tk.MustQuery("select /*+ INL_MERGE_JOIN(t2) */ * from t left outer join t2 on t.a=t2.c").Check(testkit.Rows(
		"1 1 1 1 3 1 1",
		"2 2 2 <nil> <nil> <nil> <nil>",
		"3 3 3 1 2 3 4",
		"3 3 3 2 4 3 5"))
}

func TestTimeToSecPushDownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a time(4))")
	tk.MustExec("insert into t values('700:10:10.123456')")
	tk.MustExec("insert into t values('20:20:20')")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "10000.00", "root", " MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "10000.00", "mpp[tiflash]", " ExchangeType: PassThrough"},
		{"  └─Projection_4", "10000.00", "mpp[tiflash]", " time_to_sec(test.t.a)->Column#3"},
		{"    └─TableFullScan_7", "10000.00", "mpp[tiflash]", "table:t", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select time_to_sec(a) from t;").Check(rows)
}

func TestRightShiftPushDownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(2147483647, 32)")
	tk.MustExec("insert into t values(12, 2)")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "rightshift(test.t.a, test.t.b)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select a >> b from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestBitColumnPushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=1")
	tk.MustExec("create table t1(a bit(8), b int)")
	tk.MustExec("create table t2(a bit(8), b int)")
	tk.MustExec("insert into t1 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)")
	tk.MustExec("insert into t2 values ('1', 1), ('2', 2), ('3', 3), ('4', 4), ('1', 1), ('2', 2), ('3', 3), ('4', 4)")
	sql := "select b from t1 where t1.b > (select min(t2.b) from t2 where t2.a < t1.a)"
	tk.MustQuery(sql).Sort().Check(testkit.Rows("2", "2", "3", "3", "4", "4"))
	rows := [][]interface{}{
		{"Projection_15", "root", "test.t1.b"},
		{"└─Apply_17", "root", "CARTESIAN inner join, other cond:gt(test.t1.b, Column#7)"},
		{"  ├─TableReader_20(Build)", "root", "data:Selection_19"},
		{"  │ └─Selection_19", "cop[tikv]", "not(isnull(test.t1.b))"},
		{"  │   └─TableFullScan_18", "cop[tikv]", "keep order:false, stats:pseudo"},
		{"  └─Selection_21(Probe)", "root", "not(isnull(Column#7))"},
		{"    └─StreamAgg_23", "root", "funcs:min(test.t2.b)->Column#7"},
		{"      └─TopN_24", "root", "test.t2.b, offset:0, count:1"},
		{"        └─TableReader_32", "root", "data:TopN_31"},
		{"          └─TopN_31", "cop[tikv]", "test.t2.b, offset:0, count:1"},
		{"            └─Selection_30", "cop[tikv]", "lt(test.t2.a, test.t1.a), not(isnull(test.t2.b))"},
		{"              └─TableFullScan_29", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery(fmt.Sprintf("explain analyze %s", sql)).CheckAt([]int{0, 3, 6}, rows)
	tk.MustExec("insert t1 values ('A', 1);")
	sql = "select a from t1 where ascii(a)=65"
	tk.MustQuery(sql).Check(testkit.Rows("A"))
	rows = [][]interface{}{
		{"TableReader_7", "root", "data:Selection_6"},
		{"└─Selection_6", "cop[tikv]", "eq(ascii(cast(test.t1.a, var_string(1))), 65)"},
		{"  └─TableFullScan_5", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery(fmt.Sprintf("explain analyze %s", sql)).CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = `eq(concat(cast(test.t1.a, var_string(1)), "A"), "AA")`
	sql = "select a from t1 where concat(a, 'A')='AA'"
	tk.MustQuery(sql).Check(testkit.Rows("A"))
	tk.MustQuery(fmt.Sprintf("explain analyze %s", sql)).CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = `eq(cast(test.t1.a, binary(1)), "A")`
	sql = "select a from t1 where binary a='A'"
	tk.MustQuery(sql).Check(testkit.Rows("A"))
	tk.MustQuery(fmt.Sprintf("explain analyze %s", sql)).CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = `eq(cast(test.t1.a, var_string(1)), "A")`
	sql = "select a from t1 where cast(a as char)='A'"
	tk.MustQuery(sql).Check(testkit.Rows("A"))
	tk.MustQuery(fmt.Sprintf("explain analyze %s", sql)).CheckAt([]int{0, 3, 6}, rows)

	tk.MustExec("insert into mysql.expr_pushdown_blacklist values('bit', 'tikv','');")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	rows = [][]interface{}{
		{"Selection_5", "root", `eq(cast(test.t1.a, var_string(1)), "A")`},
		{"└─TableReader_7", "root", "data:TableFullScan_6"},
		{"  └─TableFullScan_6", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	sql = "select a from t1 where cast(a as char)='A'"
	tk.MustQuery(sql).Check(testkit.Rows("A"))
	tk.MustQuery(fmt.Sprintf("explain analyze %s", sql)).CheckAt([]int{0, 3, 6}, rows)

	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name='bit'")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	sql = "select a from t1 where ascii(a)=65"
	tk.MustQuery(sql).Check(testkit.Rows("A"))
	rows = [][]interface{}{
		{"TableReader_7", "root", "data:Selection_6"},
		{"└─Selection_6", "cop[tikv]", "eq(ascii(cast(test.t1.a, var_string(1))), 65)"},
		{"  └─TableFullScan_5", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery(fmt.Sprintf("explain analyze %s", sql)).CheckAt([]int{0, 3, 6}, rows)

	// test collation
	tk.MustExec("update mysql.tidb set VARIABLE_VALUE='True' where VARIABLE_NAME='new_collation_enabled'")
	tk.MustQuery("SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='new_collation_enabled';").Check(
		testkit.Rows("True"))
	tk.MustExec("create table t3 (a bit(8));")
	tk.MustExec("insert into t3 values (65)")
	tk.MustExec("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
	tk.MustQuery("select a from t3 where cast(a as char) = 'a'").Check(testkit.Rows())
	tk.MustExec("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci")
	tk.MustQuery("select a from t3 where cast(a as char) = 'a'").Check(testkit.Rows("A"))
}

func TestSysdatePushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int signed, id2 int unsigned, c varchar(11), d datetime, b double, e bit(10))")
	tk.MustExec("insert into t(id, id2, c, d) values (-1, 1, 'abc', '2021-12-12')")
	rows := [][]interface{}{
		{"TableReader_7", "root", "data:Selection_6"},
		{"└─Selection_6", "cop[tikv]", "gt(test.t.d, sysdate())"},
		{"  └─TableFullScan_5", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where d > sysdate()").
		CheckAt([]int{0, 3, 6}, rows)
	// assert sysdate isn't now after set global tidb_sysdate_is_now in the same session
	tk.MustExec("set global tidb_sysdate_is_now='1'")
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where d > sysdate()").
		CheckAt([]int{0, 3, 6}, rows)

	// assert sysdate is now after set global tidb_sysdate_is_now in the new session
	tk = testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	now := time.Now()
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/expression/injectNow", fmt.Sprintf(`return(%d)`, now.Unix())))
	rows[1][2] = fmt.Sprintf("gt(test.t.d, %v)", now.Format("2006-01-02 15:04:05"))
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where d > sysdate()").
		CheckAt([]int{0, 3, 6}, rows)
	failpoint.Disable("github.com/pingcap/tidb/expression/injectNow")

	// assert sysdate isn't now after set session tidb_sysdate_is_now false in the same session
	tk.MustExec("set tidb_sysdate_is_now='0'")
	rows[1][2] = "gt(test.t.d, sysdate())"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where d > sysdate()").
		CheckAt([]int{0, 3, 6}, rows)
}

func TestTimeScalarFunctionPushDownResult(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(col1 datetime, col2 datetime, y int(8), m int(8), d int(8)) CHARSET=utf8 COLLATE=utf8_general_ci;")
	tk.MustExec("insert into t values ('2022-03-24 01:02:03.040506', '9999-12-31 23:59:59', 9999, 12, 31);")
	testcases := []struct {
		sql      string
		function string
	}{
		{
			sql:      "select col1, hour(col1) from t where hour(col1)=hour('2022-03-24 01:02:03.040506');",
			function: "hour",
		},
		{
			sql:      "select col1, month(col1) from t where month(col1)=month('2022-03-24 01:02:03.040506');",
			function: "month",
		},
		{
			sql:      "select col1, minute(col1) from t where minute(col1)=minute('2022-03-24 01:02:03.040506');",
			function: "minute",
		},
		{
			function: "second",
			sql:      "select col1, second(col1) from t where second(col1)=second('2022-03-24 01:02:03.040506');",
		},
		{
			function: "microsecond",
			sql:      "select col1, microsecond(col1) from t where microsecond(col1)=microsecond('2022-03-24 01:02:03.040506');",
		},
		{
			function: "dayName",
			sql:      "select col1, dayName(col1) from t where dayName(col1)=dayName('2022-03-24 01:02:03.040506');",
		},
		{
			function: "dayOfMonth",
			sql:      "select col1, dayOfMonth(col1) from t where dayOfMonth(col1)=dayOfMonth('2022-03-24 01:02:03.040506');",
		},
		{
			function: "dayOfWeek",
			sql:      "select col1, dayOfWeek(col1) from t where dayOfWeek(col1)=dayOfWeek('2022-03-24 01:02:03.040506');",
		},
		{
			function: "dayOfYear",
			sql:      "select col1, dayOfYear(col1) from t where dayOfYear(col1)=dayOfYear('2022-03-24 01:02:03.040506');",
		},
		{
			function: "Date",
			sql:      "select col1, Date(col1) from t where Date(col1)=Date('2022-03-24 01:02:03.040506');",
		},
		{
			function: "Week",
			sql:      "select col1, Week(col1) from t where Week(col1)=Week('2022-03-24 01:02:03.040506');",
		},
		{
			function: "time_to_sec",
			sql:      "select col1, time_to_sec (col1) from t where time_to_sec(col1)=time_to_sec('2022-03-24 01:02:03.040506');",
		},
		{
			function: "DateDiff",
			sql:      "select col1, DateDiff(col1, col2) from t where DateDiff(col1, col2)=DateDiff('2022-03-24 01:02:03.040506', '9999-12-31 23:59:59');",
		},
		{
			function: "MonthName",
			sql:      "select col1, MonthName(col1) from t where MonthName(col1)=MonthName('2022-03-24 01:02:03.040506');",
		},
		{
			function: "MakeDate",
			sql:      "select col1, MakeDate(9999, 31) from t where MakeDate(y, d)=MakeDate(9999, 31);",
		},
		{
			function: "MakeTime",
			sql:      "select col1, MakeTime(12, 12, 31) from t where MakeTime(m, m, d)=MakeTime(12, 12, 31);",
		},
	}
	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name != 'date_add'")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
	for _, testcase := range testcases {
		r1 := tk.MustQuery(testcase.sql).Rows()
		tk.MustExec(fmt.Sprintf("insert into mysql.expr_pushdown_blacklist(name) values('%s');", testcase.function))
		tk.MustExec("admin reload expr_pushdown_blacklist;")
		r2 := tk.MustQuery(testcase.sql).Rows()
		require.EqualValues(t, r2, r1, testcase.sql)
	}
	tk.MustExec("delete from mysql.expr_pushdown_blacklist where name != 'date_add'")
	tk.MustExec("admin reload expr_pushdown_blacklist;")
}

func TestNumberFunctionPushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int signed, b int unsigned,c double)")
	tk.MustExec("insert into t values (-1,61,4.4)")
	testcases := []struct {
		sql      string
		function string
	}{
		{
			sql:      "select a, mod(a,2) from t where mod(-1,2)=mod(a,2);",
			function: "mod",
		},
		{
			sql:      "select b, mod(b,2) from t where mod(61,2)=mod(b,2);",
			function: "mod",
		},
		{
			sql:      "select b,unhex(b) from t where unhex(61) = unhex(b)",
			function: "unhex",
		},
		{
			sql:      "select b, oct(b) from t where oct(61) = oct(b)",
			function: "oct",
		},
		{
			sql:      "select c, sin(c) from t where sin(4.4) = sin(c)",
			function: "sin",
		},
		{
			sql:      "select c, asin(c) from t where asin(4.4) = asin(c)",
			function: "asin",
		},
		{
			sql:      "select c, cos(c) from t where cos(4.4) = cos(c)",
			function: "cos",
		},
		{
			sql:      "select c, acos(c) from t where acos(4.4) = acos(c)",
			function: "acos",
		},
		{
			sql:      "select b,atan(b) from t where atan(61)=atan(b)",
			function: "atan",
		},
		{
			sql:      "select b, atan2(b, c) from t where atan2(61,4.4)=atan2(b,c)",
			function: "atan2",
		},
		{
			sql:      "select b,cot(b) from t where cot(61)=cot(b)",
			function: "cot",
		},
		{
			sql:      "select c from t where pi() < c",
			function: "pi",
		},
	}
	for _, testcase := range testcases {
		tk.MustExec("delete from mysql.expr_pushdown_blacklist where name != 'date_add'")
		tk.MustExec("admin reload expr_pushdown_blacklist;")
		r1 := tk.MustQuery(testcase.sql).Rows()
		tk.MustExec(fmt.Sprintf("insert into mysql.expr_pushdown_blacklist(name) values('%s');", testcase.function))
		tk.MustExec("admin reload expr_pushdown_blacklist;")
		r2 := tk.MustQuery(testcase.sql).Rows()
		require.EqualValues(t, r2, r1, testcase.sql)
	}
}

func TestScalarFunctionPushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int signed, id2 int unsigned, c varchar(11), d datetime, b double, e bit(10))")
	tk.MustExec("insert into t(id, id2, c, d) values (-1, 1, '{\"a\":1}', '2021-12-12')")
	rows := [][]interface{}{
		{"TableReader_7", "root", "data:Selection_6"},
		{"└─Selection_6", "cop[tikv]", "right(test.t.c, 1)"},
		{"  └─TableFullScan_5", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where right(c,1);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "mod(test.t.id, test.t.id)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where mod(id, id);").
		CheckAt([]int{0, 3, 6}, rows)
	rows[1][2] = "mod(test.t.id, test.t.id2)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where mod(id, id2);").
		CheckAt([]int{0, 3, 6}, rows)
	rows[1][2] = "mod(test.t.id2, test.t.id)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where mod(id2, id);").
		CheckAt([]int{0, 3, 6}, rows)
	rows[1][2] = "mod(test.t.id2, test.t.id2)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where mod(id2, id2);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "sin(cast(test.t.id, double BINARY))"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where sin(id);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "asin(cast(test.t.id, double BINARY))"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where asin(id);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "cos(cast(test.t.id, double BINARY))"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where cos(id);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "acos(cast(test.t.id, double BINARY))"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where acos(id);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "atan(cast(test.t.id, double BINARY))"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where atan(id);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "atan2(cast(test.t.id, double BINARY), cast(test.t.id, double BINARY))"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where atan2(id,id);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "hour(cast(test.t.d, time))"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where hour(d);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "hour(cast(test.t.d, time))"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where hour(d);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "minute(cast(test.t.d, time))"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where minute(d);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "second(cast(test.t.d, time))"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where second(d);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "month(test.t.d)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where month(d);").
		CheckAt([]int{0, 3, 6}, rows)

	//rows[1][2] = "dayname(test.t.d)"
	//tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where dayname(d);").
	//	CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "dayofmonth(test.t.d)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where dayofmonth(d);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "from_days(test.t.id)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where from_days(id);").
		CheckAt([]int{0, 3, 6}, rows)

	//rows[1][2] = "last_day(test.t.d)"
	//tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where last_day(d);").
	//	CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "gt(4, test.t.id)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where pi() > id;").
		CheckAt([]int{0, 3, 6}, rows)

	//rows[1][2] = "truncate(test.t.id, 0)"
	//tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where truncate(id,0)").
	//	CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "round(test.t.b)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where round(b)").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "date(test.t.d)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where date(d)").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "week(test.t.d)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where week(d)").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "datediff(test.t.d, test.t.d)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where datediff(d,d)").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "gt(test.t.d, sysdate())"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where d > sysdate()").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "ascii(cast(test.t.e, var_string(2)))"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where ascii(e);").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "eq(json_valid(test.t.c), 1)"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where json_valid(c)=1;").
		CheckAt([]int{0, 3, 6}, rows)

	rows[1][2] = "json_contains(cast(test.t.c, json BINARY), cast(\"1\", json BINARY))"
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where json_contains(c, '1');").
		CheckAt([]int{0, 3, 6}, rows)
}

func TestDistinctScalarFunctionPushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int not null, b int not null, c int not null, primary key (a,c)) partition by range (c) (partition p0 values less than (5), partition p1 values less than (10))")
	tk.MustExec("insert into t values(1,1,1),(2,2,2),(3,1,3),(7,1,7),(8,2,8),(9,2,9)")
	tk.MustQuery("select count(distinct b+1) as col from t").Check(testkit.Rows(
		"2",
	))
}

func TestReverseUTF8PushDownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(256))")
	tk.MustExec("insert into t values('pingcap')")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "reverse(test.t.a)->Column#3"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}

	tk.MustQuery("explain select reverse(a) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestReversePushDownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a binary(32))")
	tk.MustExec("insert into t values('pingcap')")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "reverse(test.t.a)->Column#3"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}

	tk.MustQuery("explain select reverse(a) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestSpacePushDownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values(5)")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "space(test.t.a)->Column#3"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}

	tk.MustQuery("explain select space(a) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestExplainAnalyzePointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b varchar(20))")
	tk.MustExec("insert into t values (1,1)")

	res := tk.MustQuery("explain analyze select * from t where a=1;")
	checkExplain := func(rpc string) {
		resBuff := bytes.NewBufferString("")
		for _, row := range res.Rows() {
			_, _ = fmt.Fprintf(resBuff, "%s\n", row)
		}
		explain := resBuff.String()
		require.Containsf(t, explain, rpc+":{num_rpc:", "%s", explain)
		require.Containsf(t, explain, "total_time:", "%s", explain)
	}
	checkExplain("Get")
	res = tk.MustQuery("explain analyze select * from t where a in (1,2,3);")
	checkExplain("BatchGet")
}

func TestExplainAnalyzeDML(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(" create table t (a int, b int, unique index (a));")
	tk.MustExec("insert into t values (1,1)")

	res := tk.MustQuery("explain analyze select * from t where a=1;")
	checkExplain := func(rpc string) {
		resBuff := bytes.NewBufferString("")
		for _, row := range res.Rows() {
			_, _ = fmt.Fprintf(resBuff, "%s\n", row)
		}
		explain := resBuff.String()
		require.Containsf(t, explain, rpc+":{num_rpc:", "%s", explain)
		require.Containsf(t, explain, "total_time:", "%s", explain)
	}
	checkExplain("Get")
	res = tk.MustQuery("explain analyze insert ignore into t values (1,1),(2,2),(3,3),(4,4);")
	checkExplain("BatchGet")
}

func TestExplainAnalyzeDML2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	cases := []struct {
		prepare    string
		sql        string
		planRegexp string
	}{
		// Test for alloc auto ID.
		{
			sql:        "insert into t () values ()",
			planRegexp: ".*prepare.*total.*, auto_id_allocator.*alloc_cnt: 1, Get.*num_rpc.*total_time.*commit_txn.*prewrite.*get_commit_ts.*commit.*write_keys.*, insert.*",
		},
		// Test for rebase ID.
		{
			sql:        "insert into t (a) values (99000000000)",
			planRegexp: ".*prepare.*total.*, auto_id_allocator.*rebase_cnt: 1, Get.*num_rpc.*total_time.*commit_txn.*prewrite.*get_commit_ts.*commit.*write_keys.*, insert.*",
		},
		// Test for alloc auto ID and rebase ID.
		{
			sql:        "insert into t (a) values (null), (99000000000)",
			planRegexp: ".*prepare.*total.*, auto_id_allocator.*alloc_cnt: 1, rebase_cnt: 1, Get.*num_rpc.*total_time.*commit_txn.*prewrite.*get_commit_ts.*commit.*write_keys.*, insert.*",
		},
		// Test for insert ignore.
		{
			sql:        "insert ignore into t values (null,1), (2, 2), (99000000000, 3), (100000000000, 4)",
			planRegexp: ".*prepare.*total.*, auto_id_allocator.*alloc_cnt: 1, rebase_cnt: 2, Get.*num_rpc.*total_time.*commit_txn.*count: 3, prewrite.*get_commit_ts.*commit.*write_keys.*, check_insert.*",
		},
		// Test for insert on duplicate.
		{
			sql:        "insert into t values (null,null), (1,1),(2,2) on duplicate key update a = a + 100000000000",
			planRegexp: ".*prepare.*total.*, auto_id_allocator.*alloc_cnt: 1, rebase_cnt: 1, Get.*num_rpc.*total_time.*commit_txn.*count: 2, prewrite.*get_commit_ts.*commit.*write_keys.*, check_insert.*",
		},
		// Test for replace with alloc ID.
		{
			sql:        "replace into t () values ()",
			planRegexp: ".*auto_id_allocator.*alloc_cnt: 1, Get.*num_rpc.*total_time.*commit_txn.*prewrite.*get_commit_ts.*commit.*write_keys.*",
		},
		// Test for replace with alloc ID and rebase ID.
		{
			sql:        "replace into t (a) values (null), (99000000000)",
			planRegexp: ".*auto_id_allocator.*alloc_cnt: 1, rebase_cnt: 1, Get.*num_rpc.*total_time.*commit_txn.*prewrite.*get_commit_ts.*commit.*write_keys.*",
		},
		// Test for update with rebase ID.
		{
			prepare:    "insert into t values (1,1),(2,2)",
			sql:        "update t set a=a*100000000000",
			planRegexp: ".*auto_id_allocator.*rebase_cnt: 2, Get.*num_rpc.*total_time.*commit_txn.*prewrite.*get_commit_ts.*commit.*write_keys.*",
		},
	}

	for _, ca := range cases {
		for i := 0; i < 3; i++ {
			tk.MustExec("drop table if exists t")
			switch i {
			case 0:
				tk.MustExec("create table t (a bigint auto_increment, b int, primary key (a));")
			case 1:
				tk.MustExec("create table t (a bigint unsigned auto_increment, b int, primary key (a));")
			case 2:
				if strings.Contains(ca.sql, "on duplicate key") {
					continue
				}
				tk.MustExec("create table t (a bigint primary key auto_random(5), b int);")
				tk.MustExec("set @@allow_auto_random_explicit_insert=1;")
			default:
				panic("should never happen")
			}
			if ca.prepare != "" {
				tk.MustExec(ca.prepare)
			}
			res := tk.MustQuery("explain analyze " + ca.sql)
			resBuff := bytes.NewBufferString("")
			for _, row := range res.Rows() {
				_, _ = fmt.Fprintf(resBuff, "%s\t", row)
			}
			explain := resBuff.String()
			require.Regexpf(t, ca.planRegexp, explain, "idx: %v,sql: %v", i, ca.sql)
		}
	}

	// Test for table without auto id.
	for _, ca := range cases {
		tk.MustExec("drop table if exists t")
		tk.MustExec("create table t (a bigint, b int);")
		tk.MustExec("insert into t () values ()")
		if ca.prepare != "" {
			tk.MustExec(ca.prepare)
		}
		res := tk.MustQuery("explain analyze " + ca.sql)
		resBuff := bytes.NewBufferString("")
		for _, row := range res.Rows() {
			_, _ = fmt.Fprintf(resBuff, "%s\t", row)
		}
		explain := resBuff.String()
		require.NotContainsf(t, explain, "auto_id_allocator", "sql: %v, explain: %v", ca.sql, explain)
	}
}

func TestPartialBatchPointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c_int int, c_str varchar(40), primary key(c_int, c_str))")
	tk.MustExec("insert into t values (3, 'bose')")
	tk.MustQuery("select * from t where c_int in (3)").Check(testkit.Rows(
		"3 bose",
	))
	tk.MustQuery("select * from t where c_int in (3) or c_str in ('yalow') and c_int in (1, 2)").Check(testkit.Rows(
		"3 bose",
	))
}

func TestIssue19926(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ta;")
	tk.MustExec("drop table if exists tb;")
	tk.MustExec("drop table if exists tc;")
	tk.MustExec("drop view if exists v;")
	tk.MustExec("CREATE TABLE `ta`  (\n  `id` varchar(36) NOT NULL ,\n  `status` varchar(1) NOT NULL \n);")
	tk.MustExec("CREATE TABLE `tb`  (\n  `id` varchar(36) NOT NULL ,\n  `status` varchar(1) NOT NULL \n);")
	tk.MustExec("CREATE TABLE `tc`  (\n  `id` varchar(36) NOT NULL ,\n  `status` varchar(1) NOT NULL \n);")
	tk.MustExec("insert into ta values('1','1');")
	tk.MustExec("insert into tb values('1','1');")
	tk.MustExec("insert into tc values('1','1');")
	tk.MustExec("create definer='root'@'localhost' view v as\nselect \nconcat(`ta`.`status`,`tb`.`status`) AS `status`, \n`ta`.`id` AS `id`  from (`ta` join `tb`) \nwhere (`ta`.`id` = `tb`.`id`);")
	tk.MustQuery("SELECT tc.status,v.id FROM tc, v WHERE tc.id = v.id AND v.status = '11';").Check(testkit.Rows("1 1"))
}

func TestDeleteUsingJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int primary key, b int)")
	tk.MustExec("create table t2(a int primary key, b int)")
	tk.MustExec("insert into t1 values(1,1),(2,2)")
	tk.MustExec("insert into t2 values(2,2)")
	tk.MustExec("delete t1.* from t1 join t2 using (a)")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select * from t2").Check(testkit.Rows("2 2"))
}

func Test19942(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("CREATE TABLE test.`t` (" +
		"  `a` int(11) NOT NULL," +
		"  `b` varchar(10) COLLATE utf8_general_ci NOT NULL," +
		"  `c` varchar(50) COLLATE utf8_general_ci NOT NULL," +
		"  `d` char(10) NOT NULL," +
		"  PRIMARY KEY (`c`)," +
		"  UNIQUE KEY `a_uniq` (`a`)," +
		"  UNIQUE KEY `b_uniq` (`b`)," +
		"  UNIQUE KEY `d_uniq` (`d`)," +
		"  KEY `a_idx` (`a`)," +
		"  KEY `b_idx` (`b`)," +
		"  KEY `d_idx` (`d`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (1, '1', '0', '1');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (2, ' 2', ' 0', ' 2');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (3, '  3 ', '  3 ', '  3 ');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (4, 'a', 'a   ', 'a');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (5, ' A  ', ' A   ', ' A  ');")
	tk.MustExec("INSERT INTO test.t (a, b, c, d) VALUES (6, ' E', 'é        ', ' E');")

	mkr := func() [][]interface{} {
		return testkit.RowsWithSep("|",
			"3|  3 |  3 |  3",
			"2| 2  0| 2",
			"5| A  | A   | A",
			"1|1|0|1",
			"4|a|a   |a",
			"6| E|é        | E")
	}
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`a_uniq`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`b_uniq`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`d_uniq`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`a_idx`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`b_idx`);").Check(mkr())
	tk.MustQuery("SELECT * FROM `test`.`t` FORCE INDEX(`d_idx`);").Check(mkr())
	tk.MustExec("admin check table t")
}

func TestPartitionUnionWithPPruningColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (\n  `fid` bigint(36) NOT NULL,\n  `oty` varchar(30) DEFAULT NULL,\n  `oid` int(11) DEFAULT NULL,\n  `pid` bigint(20) DEFAULT NULL,\n  `bid` int(11) DEFAULT NULL,\n  `r5` varchar(240) DEFAULT '',\n  PRIMARY KEY (`fid`)\n)PARTITION BY HASH( `fid` ) PARTITIONS 4;")

	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (59, 'm',  441, 1,  2143,  'LE1264_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (135, 'm',  1121, 1,  2423,  'LE2008_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (139, 'm',  1125, 1,  2432, 'LE2005_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (143, 'm',  1129, 1,  2438,  'LE2006_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (147, 'm',  1133, 1,  2446,  'LE2014_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (167, 'm',  1178, 1,  2512,  'LE2055_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (171, 'm',  1321, 1,  2542,  'LE1006_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (179, 'm',  1466, 1,  2648,  'LE2171_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (187, 'm',  1567, 1,  2690,  'LE1293_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (57, 'm',  341, 1,  2102,  'LE1001_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (137, 'm',  1123, 1,  2427,  'LE2003_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (145, 'm',  1131, 1,  2442,  'LE2048_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (138, 'm',  1124, 1,  2429,  'LE2004_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (142, 'm',  1128, 1,  2436,  'LE2049_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (174, 'm',  1381, 1,  2602,  'LE2170_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (28, 'm',  81, 1,  2023,  'LE1009_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (60, 'm',  442, 1,  2145,  'LE1263_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (136, 'm',  1122, 1,  2425,  'LE2002_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (140, 'm',  1126, 1,  2434,  'LE2001_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (168, 'm',  1179, 1,  2514,  'LE2052_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (196, 'm',  3380, 1,  2890,  'LE1300_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (208, 'm',  3861, 1,  3150,  'LE1323_r5');")
	tk.MustExec("INSERT INTO t (fid, oty, oid, pid, bid, r5) VALUES (432, 'm',  4060, 1,  3290,  'LE1327_r5');")

	tk.MustQuery("SELECT DISTINCT t.bid, t.r5 FROM t left join t parent on parent.oid = t.pid WHERE t.oty = 'm';").Sort().Check(
		testkit.Rows("2023 LE1009_r5",
			"2102 LE1001_r5",
			"2143 LE1264_r5",
			"2145 LE1263_r5",
			"2423 LE2008_r5",
			"2425 LE2002_r5",
			"2427 LE2003_r5",
			"2429 LE2004_r5",
			"2432 LE2005_r5",
			"2434 LE2001_r5",
			"2436 LE2049_r5",
			"2438 LE2006_r5",
			"2442 LE2048_r5",
			"2446 LE2014_r5",
			"2512 LE2055_r5",
			"2514 LE2052_r5",
			"2542 LE1006_r5",
			"2602 LE2170_r5",
			"2648 LE2171_r5",
			"2690 LE1293_r5",
			"2890 LE1300_r5",
			"3150 LE1323_r5",
			"3290 LE1327_r5"))
}

func TestIssue20139(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int, c int) partition by range (id) (partition p0 values less than (4), partition p1 values less than (7))")
	tk.MustExec("insert into t values(3, 3), (5, 5)")
	plan := tk.MustQuery("explain format = 'brief' select * from t where c = 1 and id = c")
	plan.Check(testkit.Rows(
		"TableReader 0.01 root partition:p0 data:Selection",
		"└─Selection 0.01 cop[tikv]  eq(test.t.c, 1), eq(test.t.id, 1)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustExec("drop table t")
}

func TestIssue14481(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int default null, b int default null, c int default null)")
	plan := tk.MustQuery("explain format = 'brief' select * from t where a = 1 and a = 2")
	plan.Check(testkit.Rows("TableDual 0.00 root  rows:0"))
	tk.MustExec("drop table t")
}

func TestQueryBlockTableAliasInHint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	require.True(t, tk.HasPlan("select /*+ HASH_JOIN(@sel_1 t2) */ * FROM (select 1) t1 NATURAL LEFT JOIN (select 2) t2", "HashJoin"))
	tk.MustQuery("select /*+ HASH_JOIN(@sel_1 t2) */ * FROM (select 1) t1 NATURAL LEFT JOIN (select 2) t2").Check(testkit.Rows(
		"1 2",
	))
	require.Len(t, tk.Session().GetSessionVars().StmtCtx.GetWarnings(), 0)
}

func TestIssue10448(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")

	tk.MustExec("create table t(pk int(11) primary key)")
	tk.MustExec("insert into t values(1),(2),(3)")
	tk.MustQuery("select a from (select pk as a from t) t1 where a = 18446744073709551615").Check(testkit.Rows())
}

func TestMultiUpdateOnPrimaryKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int not null primary key)")
	tk.MustExec("insert into t values (1)")
	tk.MustGetErrMsg(`UPDATE t m, t n SET m.a = m.a + 10, n.a = n.a + 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10) not null primary key)")
	tk.MustExec("insert into t values ('abc')")
	tk.MustGetErrMsg(`UPDATE t m, t n SET m.a = 'def', n.a = 'xyz'`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, primary key (a, b))")
	tk.MustExec("insert into t values (1, 2)")
	tk.MustGetErrMsg(`UPDATE t m, t n SET m.a = m.a + 10, n.b = n.b + 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("insert into t values (1, 2)")
	tk.MustGetErrMsg(`UPDATE t m, t n SET m.a = m.a + 10, n.a = n.a + 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)

	tk.MustExec(`UPDATE t m, t n SET m.b = m.b + 10, n.b = n.b + 10`)
	tk.MustQuery("SELECT * FROM t").Check(testkit.Rows("1 12"))

	tk.MustGetErrMsg(`UPDATE t m, t n SET m.a = m.a + 1, n.b = n.b + 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)
	tk.MustGetErrMsg(`UPDATE t m, t n, t q SET m.a = m.a + 1, n.b = n.b + 10, q.b = q.b - 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)
	tk.MustGetErrMsg(`UPDATE t m, t n, t q SET m.b = m.b + 1, n.a = n.a + 10, q.b = q.b - 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'n'.`)
	tk.MustGetErrMsg(`UPDATE t m, t n, t q SET m.b = m.b + 1, n.b = n.b + 10, q.a = q.a - 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'm' and 'q'.`)
	tk.MustGetErrMsg(`UPDATE t q, t n, t m SET m.b = m.b + 1, n.b = n.b + 10, q.a = q.a - 10`,
		`[planner:1706]Primary key/partition key update is not allowed since the table is updated both as 'q' and 'n'.`)

	tk.MustExec("update t m, t n set m.a = n.a+10 where m.a=n.a")
	tk.MustQuery("select * from t").Check(testkit.Rows("11 12"))
}

func TestOrderByHavingNotInSelect(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ttest")
	tk.MustExec("create table ttest (v1 int, v2 int)")
	tk.MustExec("insert into ttest values(1, 2), (4,6), (1, 7)")
	tk.MustGetErrMsg("select v1 from ttest order by count(v2)",
		"[planner:3029]Expression #1 of ORDER BY contains aggregate function and applies to the result of a non-aggregated query")
	tk.MustGetErrMsg("select v1 from ttest having count(v2)",
		"[planner:8123]In aggregated query without GROUP BY, expression #1 of SELECT list contains nonaggregated column 'v1'; this is incompatible with sql_mode=only_full_group_by")
	tk.MustGetErrMsg("select v2, v1 from (select * from ttest) t1 join (select 1, 2) t2 group by v1",
		"[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t1.v2' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
	tk.MustGetErrMsg("select v2, v1 from (select t1.v1, t2.v2 from ttest t1 join ttest t2) t3 join (select 1, 2) t2 group by v1",
		"[planner:1055]Expression #1 of SELECT list is not in GROUP BY clause and contains nonaggregated column 'test.t3.v2' which is not functionally dependent on columns in GROUP BY clause; this is incompatible with sql_mode=only_full_group_by")
}

func TestUpdateSetDefault(t *testing.T) {
	store := testkit.CreateMockStore(t)
	// #20598
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table tt (x int, z int as (x+10) stored)")
	tk.MustExec("insert into tt(x) values (1)")
	tk.MustExec("update tt set x=2, z = default")
	tk.MustExec("update tt set x=2, z = default(z)")
	tk.MustQuery("select * from tt").Check(testkit.Rows("2 12"))

	tk.MustGetErrMsg("update tt set x=2, z = default(x)",
		"[planner:3105]The value specified for generated column 'z' in table 'tt' is not allowed.")
	tk.MustGetErrMsg("update tt set z = 123",
		"[planner:3105]The value specified for generated column 'z' in table 'tt' is not allowed.")
	tk.MustGetErrMsg("update tt as ss set z = 123",
		"[planner:3105]The value specified for generated column 'z' in table 'tt' is not allowed.")
	tk.MustGetErrMsg("update tt as ss set x = 3, z = 13",
		"[planner:3105]The value specified for generated column 'z' in table 'tt' is not allowed.")
	tk.MustGetErrMsg("update tt as s1, tt as s2 set s1.z = default, s2.z = 456",
		"[planner:3105]The value specified for generated column 'z' in table 'tt' is not allowed.")
}

func TestExtendedStatsSwitch(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int not null, key(a), key(b))")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3),(4,4),(5,5),(6,6)")

	tk.MustExec("set session tidb_enable_extended_stats = off")
	tk.MustGetErrMsg("alter table t add stats_extended s1 correlation(a,b)",
		"Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	tk.MustGetErrMsg("alter table t drop stats_extended s1",
		"Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")
	tk.MustGetErrMsg("admin reload stats_extended",
		"Extended statistics feature is not generally available now, and tidb_enable_extended_stats is OFF")

	tk.MustExec("set session tidb_enable_extended_stats = on")
	tk.MustExec("alter table t add stats_extended s1 correlation(a,b)")
	tk.MustQuery("select stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"<nil> 0",
	))
	tk.MustExec("set session tidb_enable_extended_stats = off")
	// Analyze should not collect extended stats.
	tk.MustExec("analyze table t")
	tk.MustQuery("select stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"<nil> 0",
	))
	tk.MustExec("set session tidb_enable_extended_stats = on")
	// Analyze would collect extended stats.
	tk.MustExec("analyze table t")
	tk.MustQuery("select stats, status from mysql.stats_extended where name = 's1'").Check(testkit.Rows(
		"1.000000 1",
	))

	// Estimated index scan count is 4 using extended stats.
	tk.MustQuery("explain format = 'brief' select * from t use index(b) where a > 3 order by b limit 1").Check(testkit.Rows(
		"Limit 1.00 root  offset:0, count:1",
		"└─Projection 1.00 root  test.t.a, test.t.b",
		"  └─IndexLookUp 1.00 root  ",
		"    ├─IndexFullScan(Build) 4.00 cop[tikv] table:t, index:b(b) keep order:true",
		"    └─Selection(Probe) 1.00 cop[tikv]  gt(test.t.a, 3)",
		"      └─TableRowIDScan 4.00 cop[tikv] table:t keep order:false",
	))
	tk.MustExec("set session tidb_enable_extended_stats = off")
	// Estimated index scan count is 2 using independent assumption.
	tk.MustQuery("explain format = 'brief' select * from t use index(b) where a > 3 order by b limit 1").Check(testkit.Rows(
		"Limit 1.00 root  offset:0, count:1",
		"└─Projection 1.00 root  test.t.a, test.t.b",
		"  └─IndexLookUp 1.00 root  ",
		"    ├─IndexFullScan(Build) 2.00 cop[tikv] table:t, index:b(b) keep order:true",
		"    └─Selection(Probe) 1.00 cop[tikv]  gt(test.t.a, 3)",
		"      └─TableRowIDScan 2.00 cop[tikv] table:t keep order:false",
	))
}

func TestOrderByNotInSelectDistinct(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// #12442
	tk.MustExec("drop table if exists ttest")
	tk.MustExec("create table ttest (v1 int, v2 int)")
	tk.MustExec("insert into ttest values(1, 2), (4,6), (1, 7)")

	tk.MustGetErrMsg("select distinct v1 from ttest order by v2",
		"[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.ttest.v2' which is not in SELECT list; this is incompatible with DISTINCT")
	tk.MustGetErrMsg("select distinct v1+1 from ttest order by v1",
		"[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.ttest.v1' which is not in SELECT list; this is incompatible with DISTINCT")
	tk.MustGetErrMsg("select distinct v1+1 from ttest order by 1+v1",
		"[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.ttest.v1' which is not in SELECT list; this is incompatible with DISTINCT")
	tk.MustGetErrMsg("select distinct v1+1 from ttest order by v1+2",
		"[planner:3065]Expression #1 of ORDER BY clause is not in SELECT list, references column 'test.ttest.v1' which is not in SELECT list; this is incompatible with DISTINCT")
	tk.MustGetErrMsg("select distinct count(v1) from ttest group by v2 order by sum(v1)",
		"[planner:3066]Expression #1 of ORDER BY clause is not in SELECT list, contains aggregate function; this is incompatible with DISTINCT")
	tk.MustGetErrMsg("select distinct sum(v1)+1 from ttest group by v2 order by sum(v1)",
		"[planner:3066]Expression #1 of ORDER BY clause is not in SELECT list, contains aggregate function; this is incompatible with DISTINCT")

	// Expressions in ORDER BY whole match some fields in DISTINCT.
	tk.MustQuery("select distinct v1+1 from ttest order by v1+1").Check(testkit.Rows("2", "5"))
	tk.MustQuery("select distinct count(v1) from ttest order by count(v1)").Check(testkit.Rows("3"))
	tk.MustQuery("select distinct count(v1) from ttest group by v2 order by count(v1)").Check(testkit.Rows("1"))
	tk.MustQuery("select distinct sum(v1) from ttest group by v2 order by sum(v1)").Check(testkit.Rows("1", "4"))
	tk.MustQuery("select distinct v1, v2 from ttest order by 1, 2").Check(testkit.Rows("1 2", "1 7", "4 6"))
	tk.MustQuery("select distinct v1, v2 from ttest order by 2, 1").Check(testkit.Rows("1 2", "4 6", "1 7"))

	// Referenced columns of expressions in ORDER BY whole match some fields in DISTINCT,
	// both original expression and alias can be referenced.
	tk.MustQuery("select distinct v1 from ttest order by v1+1").Check(testkit.Rows("1", "4"))
	tk.MustQuery("select distinct v1, v2 from ttest order by v1+1, v2").Check(testkit.Rows("1 2", "1 7", "4 6"))
	tk.MustQuery("select distinct v1+1 as z, v2 from ttest order by v1+1, z+v2").Check(testkit.Rows("2 2", "2 7", "5 6"))
	tk.MustQuery("select distinct sum(v1) as z from ttest group by v2 order by z+1").Check(testkit.Rows("1", "4"))
	tk.MustQuery("select distinct sum(v1)+1 from ttest group by v2 order by sum(v1)+1").Check(testkit.Rows("2", "5"))
	tk.MustQuery("select distinct v1 as z from ttest order by v1+z").Check(testkit.Rows("1", "4"))
}

func TestInvalidNamedWindowSpec(t *testing.T) {
	store := testkit.CreateMockStore(t)
	// #12356
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS temptest")
	tk.MustExec("create table temptest (val int, val1 int)")
	tk.MustQuery("SELECT val FROM temptest WINDOW w AS (ORDER BY val RANGE 1 PRECEDING)").Check(testkit.Rows())
	tk.MustGetErrMsg("SELECT val FROM temptest WINDOW w AS (ORDER BY val, val1 RANGE 1 PRECEDING)",
		"[planner:3587]Window 'w' with RANGE N PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression, of numeric or temporal type")
	tk.MustGetErrMsg("select val1, avg(val1) as a from temptest group by val1 window w as (order by a)",
		"[planner:1054]Unknown column 'a' in 'window order by'")
	tk.MustGetErrMsg("select val1, avg(val1) as a from temptest group by val1 window w as (partition by a)",
		"[planner:1054]Unknown column 'a' in 'window partition by'")
}

func TestCorrelatedAggregate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// #18350
	tk.MustExec("DROP TABLE IF EXISTS tab, tab2")
	tk.MustExec("CREATE TABLE tab(i INT)")
	tk.MustExec("CREATE TABLE tab2(j INT)")
	tk.MustExec("insert into tab values(1),(2),(3)")
	tk.MustExec("insert into tab2 values(1),(2),(3),(15)")
	tk.MustQuery(`SELECT m.i,
       (SELECT COUNT(n.j)
           FROM tab2 WHERE j=15) AS o
    FROM tab m, tab2 n GROUP BY 1 order by m.i`).Check(testkit.Rows("1 4", "2 4", "3 4"))
	tk.MustQuery(`SELECT
         (SELECT COUNT(n.j)
             FROM tab2 WHERE j=15) AS o
    FROM tab m, tab2 n order by m.i`).Check(testkit.Rows("12"))

	// #17748
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (m int, n int)")
	tk.MustExec("insert into t1 values (2,2), (2,2), (3,3), (3,3), (3,3), (4,4)")
	tk.MustExec("insert into t2 values (1,11), (2,22), (3,32), (4,44), (4,44)")
	tk.MustExec("set @@sql_mode='TRADITIONAL'")

	tk.MustQuery(`select count(*) c, a,
		( select group_concat(count(a)) from t2 where m = a )
		from t1 group by a order by a`).
		Check(testkit.Rows("2 2 2", "3 3 3", "1 4 1,1"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (1,1),(2,1),(2,2),(3,1),(3,2),(3,3)")

	// Sub-queries in SELECT fields
	// from SELECT fields
	tk.MustQuery("select (select count(a)) from t").Check(testkit.Rows("6"))
	tk.MustQuery("select (select (select (select count(a)))) from t").Check(testkit.Rows("6"))
	tk.MustQuery("select (select (select count(n.a)) from t m order by count(m.b)) from t n").Check(testkit.Rows("6"))
	// from WHERE
	tk.MustQuery("select (select count(n.a) from t where count(n.a)=3) from t n").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select (select count(a) from t where count(distinct n.a)=3) from t n").Check(testkit.Rows("6"))
	// from HAVING
	tk.MustQuery("select (select count(n.a) from t having count(n.a)=6 limit 1) from t n").Check(testkit.Rows("6"))
	tk.MustQuery("select (select count(n.a) from t having count(distinct n.b)=3 limit 1) from t n").Check(testkit.Rows("6"))
	tk.MustQuery("select (select sum(distinct n.a) from t having count(distinct n.b)=3 limit 1) from t n").Check(testkit.Rows("6"))
	tk.MustQuery("select (select sum(distinct n.a) from t having count(distinct n.b)=6 limit 1) from t n").Check(testkit.Rows("<nil>"))
	// from ORDER BY
	tk.MustQuery("select (select count(n.a) from t order by count(n.b) limit 1) from t n").Check(testkit.Rows("6"))
	tk.MustQuery("select (select count(distinct n.b) from t order by count(n.b) limit 1) from t n").Check(testkit.Rows("3"))
	// from TableRefsClause
	tk.MustQuery("select (select cnt from (select count(a) cnt) s) from t").Check(testkit.Rows("6"))
	tk.MustQuery("select (select count(cnt) from (select count(a) cnt) s) from t").Check(testkit.Rows("1"))
	// from sub-query inside aggregate
	tk.MustQuery("select (select sum((select count(a)))) from t").Check(testkit.Rows("6"))
	tk.MustQuery("select (select sum((select count(a))+sum(a))) from t").Check(testkit.Rows("20"))
	// from GROUP BY
	tk.MustQuery("select (select count(a) from t group by count(n.a)) from t n").Check(testkit.Rows("6"))
	tk.MustQuery("select (select count(distinct a) from t group by count(n.a)) from t n").Check(testkit.Rows("3"))

	// Sub-queries in HAVING
	tk.MustQuery("select sum(a) from t having (select count(a)) = 0").Check(testkit.Rows())
	tk.MustQuery("select sum(a) from t having (select count(a)) > 0").Check(testkit.Rows("14"))

	// Sub-queries in ORDER BY
	tk.MustQuery("select count(a) from t group by b order by (select count(a))").Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery("select count(a) from t group by b order by (select -count(a))").Check(testkit.Rows("3", "2", "1"))

	// Nested aggregate (correlated aggregate inside aggregate)
	tk.MustQuery("select (select sum(count(a))) from t").Check(testkit.Rows("6"))
	tk.MustQuery("select (select sum(sum(a))) from t").Check(testkit.Rows("14"))

	// Combining aggregates
	tk.MustQuery("select count(a), (select count(a)) from t").Check(testkit.Rows("6 6"))
	tk.MustQuery("select sum(distinct b), count(a), (select count(a)), (select cnt from (select sum(distinct b) as cnt) n) from t").
		Check(testkit.Rows("6 6 6 6"))
}

func TestCorrelatedColumnAggFuncPushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("insert into t values (1,1);")
	tk.MustQuery("select (select count(n.a + a) from t) from t n;").Check(testkit.Rows(
		"1",
	))
}

// Test for issue https://github.com/pingcap/tidb/issues/21607.
func TestConditionColPruneInPhysicalUnionScan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a int, b int);")
	tk.MustExec("begin;")
	tk.MustExec("insert into t values (1, 2);")
	tk.MustQuery("select count(*) from t where b = 1 and b in (3);").
		Check(testkit.Rows("0"))

	tk.MustExec("drop table t;")
	tk.MustExec("create table t (a int, b int as (a + 1), c int as (b + 1));")
	tk.MustExec("begin;")
	tk.MustExec("insert into t (a) values (1);")
	tk.MustQuery("select count(*) from t where b = 1 and b in (3);").
		Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t where c = 1 and c in (3);").
		Check(testkit.Rows("0"))
}

// Test for issue https://github.com/pingcap/tidb/issues/18320
func TestNonaggregateColumnWithSingleValueInOnlyFullGroupByMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, c int)")
	tk.MustExec("insert into t values (1, 2, 3), (4, 5, 6), (7, 8, 9)")
	tk.MustQuery("select a, count(b) from t where a = 1").Check(testkit.Rows("1 1"))
	tk.MustQuery("select a, count(b) from t where a = 10").Check(testkit.Rows("<nil> 0"))
	tk.MustQuery("select a, c, sum(b) from t where a = 1 group by c").Check(testkit.Rows("1 3 2"))
	tk.MustGetErrMsg("select a from t where a = 1 order by count(b)", "[planner:3029]Expression #1 of ORDER BY contains aggregate function and applies to the result of a non-aggregated query")
	tk.MustQuery("select a from t where a = 1 having count(b) > 0").Check(testkit.Rows("1"))
}

func TestIssue22040(t *testing.T) {
	store := testkit.CreateMockStore(t)
	// #22040
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int, primary key(a,b))")
	// valid case
	tk.MustExec("select * from t where (a,b) in ((1,2),(1,2))")
	// invalid case, column count doesn't match
	{
		err := tk.ExecToErr("select * from t where (a,b) in (1,2)")
		require.IsType(t, expression.ErrOperandColumns, errors.Cause(err))
	}
	{
		err := tk.ExecToErr("select * from t where (a,b) in ((1,2),1)")
		require.IsType(t, expression.ErrOperandColumns, errors.Cause(err))
	}
}

func TestIssue22071(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (a int);")
	tk.MustExec("insert into t values(1),(2),(5)")
	tk.MustQuery("select n in (1,2) from (select a in (1,2) as n from t) g;").Sort().Check(testkit.Rows("0", "1", "1"))
	tk.MustQuery("select n in (1,n) from (select a in (1,2) as n from t) g;").Check(testkit.Rows("1", "1", "1"))
}

func TestCreateViewIsolationRead(t *testing.T) {
	store := testkit.CreateMockStore(t)
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	require.NoError(t, se.Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk := testkit.NewTestKit(t, store)
	tk.SetSession(se)

	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int);")
	tk.MustExec("set session tidb_isolation_read_engines='tiflash,tidb';")
	// No error for CreateView.
	tk.MustExec("create view v0 (a, avg_b) as select a, avg(b) from t group by a;")
	tk.MustGetErrMsg("select * from v0;", "[planner:1815]Internal : No access path for table 't' is found with 'tidb_isolation_read_engines' = 'tiflash,tidb', valid values can be 'tikv'.")
	tk.MustExec("set session tidb_isolation_read_engines='tikv,tiflash,tidb';")
	tk.MustQuery("select * from v0;").Check(testkit.Rows())
}

func TestIssue22199(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(i int primary key, j int, index idx_j(j))")
	tk.MustExec("create table t2(i int primary key, j int, index idx_j(j))")
	tk.MustGetErrMsg("select t1.*, (select t2.* from t1) from t1", "[planner:1051]Unknown table 't2'")
}

func TestIssue22892(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='static'")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(a int) partition by hash (a) partitions 5;")
	tk.MustExec("insert into t1 values (0);")
	tk.MustQuery("select * from t1 where a not between 1 and 2;").Check(testkit.Rows("0"))

	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a int) partition by hash (a) partitions 5;")
	tk.MustExec("insert into t2 values (0);")
	tk.MustQuery("select * from t2 where a not between 1 and 2;").Check(testkit.Rows("0"))
}

func TestIssue26719(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table tx (a int) partition by range (a) (partition p0 values less than (10), partition p1 values less than (20))`)
	tk.MustExec(`insert into tx values (1)`)
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")

	tk.MustExec(`begin`)
	tk.MustExec(`delete from tx where a in (1)`)
	tk.MustQuery(`select * from tx PARTITION(p0)`).Check(testkit.Rows())
	tk.MustQuery(`select * from tx`).Check(testkit.Rows())
	tk.MustExec(`rollback`)
}

func TestIssue32428(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table `t1` (`a` enum('aa') DEFAULT NULL, KEY `k` (`a`))")
	tk.MustExec("insert into t1 values('aa')")
	tk.MustExec("insert into t1 values(null)")
	tk.MustQuery("select a from t1 where a<=>'aa'").Check(testkit.Rows("aa"))
	tk.MustQuery("select a from t1 where a<=>null").Check(testkit.Rows("<nil>"))

	tk.MustExec(`CREATE TABLE IDT_MULTI15860STROBJSTROBJ (
	  COL1 enum('aa') DEFAULT NULL,
	  COL2 int(41) DEFAULT NULL,
	  COL3 year(4) DEFAULT NULL,
	  KEY U_M_COL4 (COL1,COL2),
	  KEY U_M_COL5 (COL3,COL2))`)
	tk.MustExec(`insert into IDT_MULTI15860STROBJSTROBJ  values("aa", 1013610488, 1982)`)
	tk.MustQuery(`SELECT * FROM IDT_MULTI15860STROBJSTROBJ t1 RIGHT JOIN IDT_MULTI15860STROBJSTROBJ t2 ON t1.col1 <=> t2.col1 where t1.col1 is null and t2.col1 = "aa"`).Check(testkit.Rows()) // empty result
	tk.MustExec(`prepare stmt from "SELECT * FROM IDT_MULTI15860STROBJSTROBJ t1 RIGHT JOIN IDT_MULTI15860STROBJSTROBJ t2 ON t1.col1 <=> t2.col1 where t1.col1 is null and t2.col1 = ?"`)
	tk.MustExec(`set @a="aa"`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows()) // empty result
}

func TestDeleteStmt(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("delete t from t;")
	tk.MustExec("delete t from test.t as t;")
	tk.MustGetErrCode("delete test.t from test.t as t;", mysql.ErrUnknownTable)
	tk.MustExec("delete test.t from t;")
	tk.MustExec("create database db1")
	tk.MustExec("use db1")
	tk.MustExec("create table t(a int)")
	tk.MustGetErrCode("delete test.t from t;", mysql.ErrUnknownTable)
}

func TestIndexMergeConstantTrue(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int primary key, b int not null, key(b))")
	tk.MustExec("delete /*+ use_index_merge(t) */ FROM t WHERE a=1 OR (b < SOME (SELECT /*+ use_index_merge(t)*/ b FROM t WHERE a<2 OR b<2))")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int not null, b int not null, key(a), key(b))")
	tk.MustExec("delete /*+ use_index_merge(t) */ FROM t WHERE a=1 OR (b < SOME (SELECT /*+ use_index_merge(t)*/ b FROM t WHERE a<2 OR b<2))")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int not null, c int, key(a), key(b,c))")
	tk.MustExec("delete /*+ use_index_merge(t) */ FROM t WHERE a=1 OR (a<2 and b<2)")
}

func TestIndexMergeTableFilter(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, c int, d int, key(a), key(b));")
	tk.MustExec("insert into t values(10,1,1,10)")

	tk.MustQuery("explain format = 'brief' select /*+ use_index_merge(t) */ * from t where a=10 or (b=10 and c=10)").Check(testkit.Rows(
		"IndexMerge 0.02 root  type: union",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:a(a) range:[10,10], keep order:false, stats:pseudo",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:b(b) range:[10,10], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.02 cop[tikv]  or(eq(test.t.a, 10), and(eq(test.t.b, 10), eq(test.t.c, 10)))",
		"  └─TableRowIDScan 19.99 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ use_index_merge(t) */ * from t where a=10 or (b=10 and c=10)").Check(testkit.Rows(
		"10 1 1 10",
	))
	tk.MustQuery("explain format = 'brief' select /*+ use_index_merge(t) */ * from t where (a=10 and d=10) or (b=10 and c=10)").Check(testkit.Rows(
		"IndexMerge 0.00 root  type: union",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:a(a) range:[10,10], keep order:false, stats:pseudo",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:b(b) range:[10,10], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.00 cop[tikv]  or(and(eq(test.t.a, 10), eq(test.t.d, 10)), and(eq(test.t.b, 10), eq(test.t.c, 10)))",
		"  └─TableRowIDScan 19.99 cop[tikv] table:t keep order:false, stats:pseudo",
	))
	tk.MustQuery("select /*+ use_index_merge(t) */ * from t where (a=10 and d=10) or (b=10 and c=10)").Check(testkit.Rows(
		"10 1 1 10",
	))
}

func TestIssue22850(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE t1 (a int(11))")
	tk.MustQuery("SELECT @v:=(SELECT 1 FROM t1 t2 LEFT JOIN t1 ON t1.a GROUP BY t1.a) FROM t1").Check(testkit.Rows()) // work fine
}

func TestJoinSchemaChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int(11))")
	tk.MustExec("create table t2(a decimal(40,20) unsigned, b decimal(40,20))")
	tk.MustQuery("select count(*) as x from t1 group by a having x not in (select a from t2 where x = t2.b)").Check(testkit.Rows())
}

// #22949: test HexLiteral Used in GetVar expr
func TestGetVarExprWithHexLiteral(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1_no_idx;")
	tk.MustExec("create table t1_no_idx(id int, col_bit bit(16));")
	tk.MustExec("insert into t1_no_idx values(1, 0x3135);")
	tk.MustExec("insert into t1_no_idx values(2, 0x0f);")

	tk.MustExec("prepare stmt from 'select id from t1_no_idx where col_bit = ?';")
	tk.MustExec("set @a = 0x3135;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustExec("set @a = 0x0F;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("2"))

	// same test, but use IN expr
	tk.MustExec("prepare stmt from 'select id from t1_no_idx where col_bit in (?)';")
	tk.MustExec("set @a = 0x3135;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustExec("set @a = 0x0F;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("2"))

	// same test, but use table with index on col_bit
	tk.MustExec("drop table if exists t2_idx;")
	tk.MustExec("create table t2_idx(id int, col_bit bit(16), key(col_bit));")
	tk.MustExec("insert into t2_idx values(1, 0x3135);")
	tk.MustExec("insert into t2_idx values(2, 0x0f);")

	tk.MustExec("prepare stmt from 'select id from t2_idx where col_bit = ?';")
	tk.MustExec("set @a = 0x3135;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustExec("set @a = 0x0F;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("2"))

	// same test, but use IN expr
	tk.MustExec("prepare stmt from 'select id from t2_idx where col_bit in (?)';")
	tk.MustExec("set @a = 0x3135;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
	tk.MustExec("set @a = 0x0F;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("2"))

	// test col varchar with GetVar
	tk.MustExec("drop table if exists t_varchar;")
	tk.MustExec("create table t_varchar(id int, col_varchar varchar(100), key(col_varchar));")
	tk.MustExec("insert into t_varchar values(1, '15');")
	tk.MustExec("prepare stmt from 'select id from t_varchar where col_varchar = ?';")
	tk.MustExec("set @a = 0x3135;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
}

// test BitLiteral used with GetVar
func TestGetVarExprWithBitLiteral(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t1_no_idx;")
	tk.MustExec("create table t1_no_idx(id int, col_bit bit(16));")
	tk.MustExec("insert into t1_no_idx values(1, 0x3135);")
	tk.MustExec("insert into t1_no_idx values(2, 0x0f);")

	tk.MustExec("prepare stmt from 'select id from t1_no_idx where col_bit = ?';")
	// 0b11000100110101 is 0x3135
	tk.MustExec("set @a = 0b11000100110101;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))

	// same test, but use IN expr
	tk.MustExec("prepare stmt from 'select id from t1_no_idx where col_bit in (?)';")
	tk.MustExec("set @a = 0b11000100110101;")
	tk.MustQuery("execute stmt using @a;").Check(testkit.Rows("1"))
}

func TestIndexMergeClusterIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 float, c2 int, c3 int, primary key (c1) /*T![clustered_index] CLUSTERED */, key idx_1 (c2), key idx_2 (c3))")
	tk.MustExec("insert into t values(1.0,1,2),(2.0,2,1),(3.0,1,1),(4.0,2,2)")
	tk.MustQuery("select /*+ use_index_merge(t) */ c3 from t where c3 = 1 or c2 = 1").Sort().Check(testkit.Rows(
		"1",
		"1",
		"2",
	))
	tk.MustExec("drop table t")
	tk.MustExec("create table t (a int, b int, c int, primary key (a,b) /*T![clustered_index] CLUSTERED */, key idx_c(c))")
	tk.MustExec("insert into t values (0,1,2)")
	tk.MustQuery("select /*+ use_index_merge(t) */ c from t where c > 10 or a < 1").Check(testkit.Rows(
		"2",
	))
}

func TestIssue23736(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("create table t0(a int, b int, c int as (a + b) virtual, unique index (c) invisible);")
	tk.MustExec("create table t1(a int, b int, c int as (a + b) virtual);")
	tk.MustExec("insert into t0(a, b) values (12, -1), (8, 7);")
	tk.MustExec("insert into t1(a, b) values (12, -1), (8, 7);")
	tk.MustQuery("select /*+ stream_agg() */ count(1) from t0 where c > 10 and b < 2;").Check(testkit.Rows("1"))
	tk.MustQuery("select /*+ stream_agg() */ count(1) from t1 where c > 10 and b < 2;").Check(testkit.Rows("1"))
	tk.MustExec("delete from t0")
	tk.MustExec("insert into t0(a, b) values (5, 1);")
	tk.MustQuery("select /*+ nth_plan(3) */ count(1) from t0 where c > 10 and b < 2;").Check(testkit.Rows("0"))

	// Should not use invisible index
	require.False(t, tk.MustUseIndex("select /*+ stream_agg() */ count(1) from t0 where c > 10 and b < 2", "c"))
}

// https://github.com/pingcap/tidb/issues/23802
func TestPanicWhileQueryTableWithIsNull(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists NT_HP27193")
	tk.MustExec("CREATE TABLE `NT_HP27193` (  `COL1` int(20) DEFAULT NULL,  `COL2` varchar(20) DEFAULT NULL,  `COL4` datetime DEFAULT NULL,  `COL3` bigint(20) DEFAULT NULL,  `COL5` float DEFAULT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin PARTITION BY HASH ( `COL1`%`COL3` ) PARTITIONS 10;")
	rs, err := tk.Exec("select col1 from NT_HP27193 where col1 is null;")
	require.NoError(t, err)
	rs.Close()
	tk.MustExec("INSERT INTO NT_HP27193 (COL2, COL4, COL3, COL5) VALUES ('m',  '2020-05-04 13:15:27', 8,  2602)")
	rs, err = tk.Exec("select col1 from NT_HP27193 where col1 is null;")
	require.NoError(t, err)
	rs.Close()
	tk.MustExec("drop table if exists NT_HP27193")
}

func TestIssue23846(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varbinary(10),UNIQUE KEY(a))")
	tk.MustExec("insert into t values(0x00A4EEF4FA55D6706ED5)")
	tk.MustQuery("select count(*) from t where a=0x00A4EEF4FA55D6706ED5").Check(testkit.Rows("1"))
	tk.MustQuery("select * from t where a=0x00A4EEF4FA55D6706ED5").Check(testkit.Rows("\x00\xa4\xee\xf4\xfaU\xd6pn\xd5")) // not empty
}

func TestIssue23839(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists BB")
	tk.MustExec("CREATE TABLE `BB` (\n" +
		"	`col_int` int(11) DEFAULT NULL,\n" +
		"	`col_varchar_10` varchar(10) DEFAULT NULL,\n" +
		"	`pk` int(11) NOT NULL AUTO_INCREMENT,\n" +
		"	`col_int_not_null` int(11) NOT NULL,\n" +
		"	`col_decimal` decimal(10,0) DEFAULT NULL,\n" +
		"	`col_datetime` datetime DEFAULT NULL,\n" +
		"	`col_decimal_not_null` decimal(10,0) NOT NULL,\n" +
		"	`col_datetime_not_null` datetime NOT NULL,\n" +
		"	`col_varchar_10_not_null` varchar(10) NOT NULL,\n" +
		"	PRIMARY KEY (`pk`) /*T![clustered_index] CLUSTERED */\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=2000001")
	tk.Exec("explain SELECT OUTR . col2 AS X FROM (SELECT INNR . col1 as col1, SUM( INNR . col2 ) as col2 FROM (SELECT INNR . `col_int_not_null` + 1 as col1, INNR . `pk` as col2 FROM BB AS INNR) AS INNR GROUP BY col1) AS OUTR2 INNER JOIN (SELECT INNR . col1 as col1, MAX( INNR . col2 ) as col2 FROM (SELECT INNR . `col_int_not_null` + 1 as col1, INNR . `pk` as col2 FROM BB AS INNR) AS INNR GROUP BY col1) AS OUTR ON OUTR2.col1 = OUTR.col1 GROUP BY OUTR . col1, OUTR2 . col1 HAVING X <> 'b'")
}

func TestIssue24281(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists member, agent, deposit, view_member_agents")
	tk.MustExec("create table member(login varchar(50) NOT NULL, agent_login varchar(100) DEFAULT NULL, PRIMARY KEY(login))")
	tk.MustExec("create table agent(login varchar(50) NOT NULL, data varchar(100) DEFAULT NULL, share_login varchar(50) NOT NULL, PRIMARY KEY(login))")
	tk.MustExec("create table deposit(id varchar(50) NOT NULL, member_login varchar(50) NOT NULL, transfer_amount int NOT NULL, PRIMARY KEY(id), KEY midx(member_login, transfer_amount))")
	tk.MustExec("create definer='root'@'localhost' view view_member_agents (member, share_login) as select m.login as member, a.share_login AS share_login from member as m join agent as a on m.agent_login = a.login")

	tk.MustExec(" select s.member_login as v1, SUM(s.transfer_amount) AS v2 " +
		"FROM deposit AS s " +
		"JOIN view_member_agents AS v ON s.member_login = v.member " +
		"WHERE 1 = 1 AND v.share_login = 'somevalue' " +
		"GROUP BY s.member_login " +
		"UNION select 1 as v1, 2 as v2")
}

func TestIssue25799(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec(`create table t1 (a float default null, b smallint(6) DEFAULT NULL)`)
	tk.MustExec(`insert into t1 values (1, 1)`)
	tk.MustExec(`create table t2 (a float default null, b tinyint(4) DEFAULT NULL, key b (b))`)
	tk.MustExec(`insert into t2 values (null, 1)`)
	tk.HasPlan(`select /*+ TIDB_INLJ(t2@sel_2) */ t1.a, t1.b from t1 where t1.a not in (select t2.a from t2 where t1.b=t2.b)`, `IndexJoin`)
	tk.MustQuery(`select /*+ TIDB_INLJ(t2@sel_2) */ t1.a, t1.b from t1 where t1.a not in (select t2.a from t2 where t1.b=t2.b)`).Check(testkit.Rows())
}

func TestLimitWindowColPrune(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustQuery("select count(a) f1, row_number() over (order by count(a)) as f2 from t limit 1").Check(testkit.Rows("1 1"))
}

func TestIncrementalAnalyzeStatsVer2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b int, index idx_b(b))")
	tk.MustExec("insert into t values(1,1),(2,2),(3,3)")
	tk.MustExec("set @@session.tidb_analyze_version = 2")
	tk.MustExec("analyze table t")
	is := tk.Session().GetInfoSchema().(infoschema.InfoSchema)
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tblID := tbl.Meta().ID
	rows := tk.MustQuery(fmt.Sprintf("select distinct_count from mysql.stats_histograms where table_id = %d and is_index = 1", tblID)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "3", rows[0][0])
	tk.MustExec("insert into t values(4,4),(5,5),(6,6)")
	tk.MustExec("analyze incremental table t index idx_b")
	warns := tk.Session().GetSessionVars().StmtCtx.GetWarnings()
	require.Len(t, warns, 3)
	require.EqualError(t, warns[0].Err, "The version 2 would collect all statistics not only the selected indexes")
	require.EqualError(t, warns[1].Err, "The version 2 stats would ignore the INCREMENTAL keyword and do full sampling")
	require.EqualError(t, warns[2].Err, "Analyze use auto adjusted sample rate 1.000000 for table test.t")
	rows = tk.MustQuery(fmt.Sprintf("select distinct_count from mysql.stats_histograms where table_id = %d and is_index = 1", tblID)).Rows()
	require.Len(t, rows, 1)
	require.Equal(t, "6", rows[0][0])
}

func TestConflictReadFromStorage(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (
					a int, b int, c varchar(20),
					primary key(a), key(b), key(c)
				) partition by range columns(a) (
					partition p0 values less than(6),
					partition p1 values less than(11),
					partition p2 values less than(16));`)
	tk.MustExec(`insert into t values (1,1,"1"), (2,2,"2"), (8,8,"8"), (11,11,"11"), (15,15,"15")`)
	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustQuery(`explain select /*+ read_from_storage(tikv[t partition(p0)], tiflash[t partition(p1, p2)]) */ * from t`)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 Storage hints are conflict, you can only specify one storage type of table test.t"))
	tk.MustQuery(`explain select /*+ read_from_storage(tikv[t], tiflash[t]) */ * from t`)
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1815 Storage hints are conflict, you can only specify one storage type of table test.t"))
}

func TestIssue27167(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set names utf8mb4")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists all_types")

	tk.MustExec("CREATE TABLE `all_types` (" +
		"`id` int(11) NOT NULL," +
		"`d_tinyint` tinyint(4) DEFAULT NULL," +
		"`d_smallint` smallint(6) DEFAULT NULL," +
		"`d_int` int(11) DEFAULT NULL," +
		"`d_bigint` bigint(20) DEFAULT NULL," +
		"`d_float` float DEFAULT NULL," +
		"`d_double` double DEFAULT NULL," +
		"`d_decimal` decimal(10,2) DEFAULT NULL," +
		"`d_bit` bit(10) DEFAULT NULL," +
		"`d_binary` binary(10) DEFAULT NULL," +
		"`d_date` date DEFAULT NULL," +
		"`d_datetime` datetime DEFAULT NULL," +
		"`d_timestamp` timestamp NULL DEFAULT NULL," +
		"`d_varchar` varchar(20) NULL default NULL," +
		"PRIMARY KEY (`id`));",
	)

	tk.MustQuery("select @@collation_connection;").Check(testkit.Rows("utf8mb4_bin"))

	tk.MustExec(`insert into all_types values(0, 0, 1, 2, 3, 1.5, 2.2, 10.23, 12, 'xy', '2021-12-12', '2021-12-12 12:00:00', '2021-12-12 12:00:00', '123');`)

	tk.MustQuery("select collation(c) from (select d_date c from all_types union select d_int c from all_types) t").Check(testkit.Rows("utf8mb4_bin", "utf8mb4_bin"))
	tk.MustQuery("select collation(c) from (select d_date c from all_types union select d_int collate binary c from all_types) t").Check(testkit.Rows("binary", "binary"))
	tk.MustQuery("select collation(c) from (select d_date c from all_types union select d_float c from all_types) t").Check(testkit.Rows("utf8mb4_bin", "utf8mb4_bin"))
	// timestamp also OK
	tk.MustQuery("select collation(c) from (select d_timestamp c from all_types union select d_float c from all_types) t").Check(testkit.Rows("utf8mb4_bin", "utf8mb4_bin"))
}

func TestIssue25300(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`create table t (a char(65) collate utf8_unicode_ci, b text collate utf8_general_ci not null);`)
	tk.MustExec(`insert into t values ('a', 'A');`)
	tk.MustExec(`insert into t values ('b', 'B');`)
	tk.MustGetErrCode(`(select a from t) union ( select b from t);`, mysql.ErrCantAggregateNcollations)
	tk.MustGetErrCode(`(select 'a' collate utf8mb4_unicode_ci) union (select 'b' collate utf8mb4_general_ci);`, mysql.ErrCantAggregateNcollations)
	tk.MustGetErrCode(`(select a from t) union ( select b from t) union all select 'a';`, mysql.ErrCantAggregateNcollations)
	tk.MustGetErrCode(`(select a from t) union ( select b from t) union select 'a';`, mysql.ErrCantAggregateNcollations)
	tk.MustGetErrCode(`(select a from t) union ( select b from t) union select 'a' except select 'd';`, mysql.ErrCantAggregateNcollations)
}

func TestSelectIgnoreTemporaryTableInView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "localhost", CurrentUser: true, AuthUsername: "root", AuthHostname: "%"}, nil, []byte("012345678901234567890"))
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (c int, d int)")
	tk.MustExec("create view v1 as select * from t1 order by a")
	tk.MustExec("create view v2 as select * from ((select * from t1) union (select * from t2)) as tt order by a, b")
	tk.MustExec("create view v3 as select * from v1 order by a")
	tk.MustExec("create view v4 as select * from t1, t2 where t1.a = t2.c order by a, b")
	tk.MustExec("create view v5 as select * from (select * from t1) as t1 order by a")

	tk.MustExec("insert into t1 values (1, 2), (3, 4)")
	tk.MustExec("insert into t2 values (3, 5), (6, 7)")

	tk.MustExec("create temporary table t1 (a int, b int)")
	tk.MustExec("create temporary table t2 (c int, d int)")
	tk.MustQuery("select * from t1").Check(testkit.Rows())
	tk.MustQuery("select * from t2").Check(testkit.Rows())

	tk.MustQuery("select * from v1").Check(testkit.Rows("1 2", "3 4"))
	tk.MustQuery("select * from v2").Check(testkit.Rows("1 2", "3 4", "3 5", "6 7"))
	tk.MustQuery("select * from v3").Check(testkit.Rows("1 2", "3 4"))
	tk.MustQuery("select * from v4").Check(testkit.Rows("3 4 3 5"))
	tk.MustQuery("select * from v5").Check(testkit.Rows("1 2", "3 4"))
}

func TestIssue26250(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table tp (id int primary key) partition by range (id) (partition p0 values less than (100));")
	tk.MustExec("create table tn (id int primary key);")
	tk.MustExec("insert into tp values(1),(2);")
	tk.MustExec("insert into tn values(1),(2);")
	tk.MustQuery("select * from tp,tn where tp.id=tn.id and tn.id=1 for update;").Check(testkit.Rows("1 1"))
}

func TestCorrelationAdjustment4Limit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (pk int primary key auto_increment, year int, c varchar(256), index idx_year(year))")

	insertWithYear := func(n, year int) {
		for i := 0; i < n; i++ {
			tk.MustExec(fmt.Sprintf("insert into t (year, c) values (%v, space(256))", year))
		}
	}
	insertWithYear(10, 2000)
	insertWithYear(10, 2001)
	insertWithYear(10, 2002)
	tk.MustExec("analyze table t")

	// case 1
	tk.MustExec("set @@tidb_opt_enable_correlation_adjustment = false")
	// the estRow for TableFullScan is under-estimated since we have to scan through 2000 and 2001 to access 2002,
	// but the formula(LimitNum / Selectivity) based on uniform-assumption cannot consider this factor.
	tk.MustQuery("explain format=brief select * from t use index(primary) where year=2002 limit 1").Check(testkit.Rows(
		"Limit 1.00 root  offset:0, count:1",
		"└─TableReader 1.00 root  data:Limit",
		"  └─Limit 1.00 cop[tikv]  offset:0, count:1",
		"    └─Selection 1.00 cop[tikv]  eq(test.t.year, 2002)",
		"      └─TableFullScan 3.00 cop[tikv] table:t keep order:false"))

	// case 2: after enabling correlation adjustment, this factor can be considered.
	tk.MustExec("set @@tidb_opt_enable_correlation_adjustment = true")
	tk.MustQuery("explain format=brief select * from t use index(primary) where year=2002 limit 1").Check(testkit.Rows(
		"Limit 1.00 root  offset:0, count:1",
		"└─TableReader 1.00 root  data:Limit",
		"  └─Limit 1.00 cop[tikv]  offset:0, count:1",
		"    └─Selection 1.00 cop[tikv]  eq(test.t.year, 2002)",
		"      └─TableFullScan 21.00 cop[tikv] table:t keep order:false"))

	tk.MustExec("truncate table t")
	for y := 2000; y <= 2050; y++ {
		insertWithYear(2, y)
	}
	tk.MustExec("analyze table t")

	// case 3: correlation adjustment is only allowed to update the upper-bound, so estRow = max(1/selectivity, adjustedCount);
	// 1/sel = 1/(1/NDV) is around 50, adjustedCount is 1 since the first row can meet the requirement `year=2000`;
	// in this case the estRow is over-estimated, but it's safer that can avoid to convert IndexScan to TableScan incorrectly in some cases.
	tk.MustQuery("explain format=brief select * from t use index(primary) where year=2000 limit 1").Check(testkit.Rows(
		"Limit 1.00 root  offset:0, count:1",
		"└─TableReader 1.00 root  data:Limit",
		"  └─Limit 1.00 cop[tikv]  offset:0, count:1",
		"    └─Selection 1.00 cop[tikv]  eq(test.t.year, 2000)",
		"      └─TableFullScan 51.00 cop[tikv] table:t keep order:false"))
}

func TestCTESelfJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(t1a int, t1b int, t1c int)")
	tk.MustExec("create table t2(t2a int, t2b int, t2c int)")
	tk.MustExec("create table t3(t3a int, t3b int, t3c int)")
	tk.MustExec(`
		with inv as
		(select t1a , t3a, sum(t2c)
			from t1, t2, t3
			where t2a = t1a
				and t2b = t3b
				and t3c = 1998
			group by t1a, t3a)
		select inv1.t1a, inv2.t3a
		from inv inv1, inv inv2
		where inv1.t1a = inv2.t1a
			and inv1.t3a = 4
			and inv2.t3a = 4+1`)
}

// https://github.com/pingcap/tidb/issues/26214
func TestIssue26214(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table `t` (`a` int(11) default null, `b` int(11) default null, `c` int(11) default null, key `expression_index` ((case when `a` < 0 then 1 else 2 end)))")
	_, err := tk.Exec("select * from t  where case when a < 0 then 1 else 2 end <= 1 order by 4;")
	require.True(t, core.ErrUnknownColumn.Equal(err))
}

func TestCreateViewWithWindowFunc(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t6;")
	tk.MustExec("CREATE TABLE t6(t TIME, ts TIMESTAMP);")
	tk.MustExec("INSERT INTO t6 VALUES ('12:30', '2016-07-05 08:30:42');")
	tk.MustExec("drop view if exists v;")
	tk.MustExec("CREATE definer='root'@'localhost' VIEW v AS SELECT COUNT(*) OVER w0, COUNT(*) OVER w from t6 WINDOW w0 AS (), w  AS (w0 ORDER BY t);")
	rows := tk.MustQuery("select * from v;")
	rows.Check(testkit.Rows("1 1"))
}

func TestIssue29834(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists IDT_MC21814;")
	tk.MustExec("CREATE TABLE `IDT_MC21814` (`COL1` year(4) DEFAULT NULL,`COL2` year(4) DEFAULT NULL,KEY `U_M_COL` (`COL1`,`COL2`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("insert into IDT_MC21814 values(1901, 2119), (2155, 2000);")
	tk.MustQuery("SELECT/*+ INL_JOIN(t1, t2), nth_plan(1) */ t2.* FROM IDT_MC21814 t1 LEFT JOIN IDT_MC21814 t2 ON t1.col1 = t2.col1 WHERE t2.col2 BETWEEN 2593 AND 1971 AND t1.col1 IN (2155, 1901, 1967);").Check(testkit.Rows())
	tk.MustQuery("SELECT/*+ INL_JOIN(t1, t2), nth_plan(2) */ t2.* FROM IDT_MC21814 t1 LEFT JOIN IDT_MC21814 t2 ON t1.col1 = t2.col1 WHERE t2.col2 BETWEEN 2593 AND 1971 AND t1.col1 IN (2155, 1901, 1967);").Check(testkit.Rows())
	// Only can generate one index join plan. Because the index join inner child can not be tableDual.
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 The parameter of nth_plan() is out of range"))
}

func TestIssue29221(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_index_merge=on;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int, b int, index idx_a(a), index idx_b(b));")
	tk.MustExec("set @@session.sql_select_limit=3;")
	tk.MustQuery("explain format = 'brief' select * from t where a = 1 or b = 1;").Check(testkit.Rows(
		"Limit 3.00 root  offset:0, count:3",
		"└─IndexMerge 3.00 root  type: union",
		"  ├─Limit(Build) 1.50 cop[tikv]  offset:0, count:3",
		"  │ └─IndexRangeScan 1.50 cop[tikv] table:t, index:idx_a(a) range:[1,1], keep order:false, stats:pseudo",
		"  ├─Limit(Build) 1.50 cop[tikv]  offset:0, count:3",
		"  │ └─IndexRangeScan 1.50 cop[tikv] table:t, index:idx_b(b) range:[1,1], keep order:false, stats:pseudo",
		"  └─TableRowIDScan(Probe) 3.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("explain format = 'brief' select /*+ use_index_merge(t) */ * from t where a = 1 or b = 1;").Check(testkit.Rows(
		"Limit 3.00 root  offset:0, count:3",
		"└─IndexMerge 3.00 root  type: union",
		"  ├─Limit(Build) 1.50 cop[tikv]  offset:0, count:3",
		"  │ └─IndexRangeScan 1.50 cop[tikv] table:t, index:idx_a(a) range:[1,1], keep order:false, stats:pseudo",
		"  ├─Limit(Build) 1.50 cop[tikv]  offset:0, count:3",
		"  │ └─IndexRangeScan 1.50 cop[tikv] table:t, index:idx_b(b) range:[1,1], keep order:false, stats:pseudo",
		"  └─TableRowIDScan(Probe) 3.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustExec("set @@session.sql_select_limit=18446744073709551615;")
	tk.MustQuery("explain format = 'brief' select * from t where a = 1 or b = 1;").Check(testkit.Rows(
		"IndexMerge 19.99 root  type: union",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:idx_a(a) range:[1,1], keep order:false, stats:pseudo",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:idx_b(b) range:[1,1], keep order:false, stats:pseudo",
		"└─TableRowIDScan(Probe) 19.99 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("explain format = 'brief' select * from t where a = 1 or b = 1 limit 3;").Check(testkit.Rows(
		"Limit 3.00 root  offset:0, count:3",
		"└─IndexMerge 3.00 root  type: union",
		"  ├─Limit(Build) 1.50 cop[tikv]  offset:0, count:3",
		"  │ └─IndexRangeScan 1.50 cop[tikv] table:t, index:idx_a(a) range:[1,1], keep order:false, stats:pseudo",
		"  ├─Limit(Build) 1.50 cop[tikv]  offset:0, count:3",
		"  │ └─IndexRangeScan 1.50 cop[tikv] table:t, index:idx_b(b) range:[1,1], keep order:false, stats:pseudo",
		"  └─TableRowIDScan(Probe) 3.00 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("explain format = 'brief' select /*+ use_index_merge(t) */ * from t where a = 1 or b = 1;").Check(testkit.Rows(
		"IndexMerge 19.99 root  type: union",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:idx_a(a) range:[1,1], keep order:false, stats:pseudo",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t, index:idx_b(b) range:[1,1], keep order:false, stats:pseudo",
		"└─TableRowIDScan(Probe) 19.99 cop[tikv] table:t keep order:false, stats:pseudo"))
	tk.MustQuery("explain format = 'brief' select /*+ use_index_merge(t) */ * from t where a = 1 or b = 1 limit 3;").Check(testkit.Rows(
		"Limit 3.00 root  offset:0, count:3",
		"└─IndexMerge 3.00 root  type: union",
		"  ├─Limit(Build) 1.50 cop[tikv]  offset:0, count:3",
		"  │ └─IndexRangeScan 1.50 cop[tikv] table:t, index:idx_a(a) range:[1,1], keep order:false, stats:pseudo",
		"  ├─Limit(Build) 1.50 cop[tikv]  offset:0, count:3",
		"  │ └─IndexRangeScan 1.50 cop[tikv] table:t, index:idx_b(b) range:[1,1], keep order:false, stats:pseudo",
		"  └─TableRowIDScan(Probe) 3.00 cop[tikv] table:t keep order:false, stats:pseudo"))
}

func TestLimitPushDown(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")

	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`insert into t values (1)`)
	tk.MustExec(`analyze table t`)

	tk.MustExec(`set tidb_opt_limit_push_down_threshold=0`)
	tk.MustQuery(`explain format=brief select a from t order by a desc limit 10`).Check(testkit.Rows(
		`TopN 1.00 root  test.t.a:desc, offset:0, count:10`,
		`└─TableReader 1.00 root  data:TableFullScan`,
		`  └─TableFullScan 1.00 cop[tikv] table:t keep order:false`))

	tk.MustExec(`set tidb_opt_limit_push_down_threshold=10`)
	tk.MustQuery(`explain format=brief select a from t order by a desc limit 10`).Check(testkit.Rows(
		`TopN 1.00 root  test.t.a:desc, offset:0, count:10`,
		`└─TableReader 1.00 root  data:TopN`,
		`  └─TopN 1.00 cop[tikv]  test.t.a:desc, offset:0, count:10`,
		`    └─TableFullScan 1.00 cop[tikv] table:t keep order:false`))

	tk.MustQuery(`explain format=brief select a from t order by a desc limit 11`).Check(testkit.Rows(
		`TopN 1.00 root  test.t.a:desc, offset:0, count:11`,
		`└─TableReader 1.00 root  data:TableFullScan`,
		`  └─TableFullScan 1.00 cop[tikv] table:t keep order:false`))

	tk.MustQuery(`explain format=brief select /*+ limit_to_cop() */ a from t order by a desc limit 11`).Check(testkit.Rows(
		`TopN 1.00 root  test.t.a:desc, offset:0, count:11`,
		`└─TableReader 1.00 root  data:TopN`,
		`  └─TopN 1.00 cop[tikv]  test.t.a:desc, offset:0, count:11`,
		`    └─TableFullScan 1.00 cop[tikv] table:t keep order:false`))
}

func TestIssue26559(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a timestamp, b datetime);")
	tk.MustExec("insert into t values('2020-07-29 09:07:01', '2020-07-27 16:57:36');")
	tk.MustQuery("select greatest(a, b) from t union select null;").Sort().Check(testkit.Rows("2020-07-29 09:07:01", "<nil>"))
}

func TestIssue29503(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Status.RecordQPSbyDB = true
	})

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int);")
	require.NoError(t, tk.ExecToErr("create binding for select 1 using select 1;"))
	require.NoError(t, tk.ExecToErr("create binding for select a from t using select a from t;"))
	res := tk.MustQuery("show session bindings;")
	require.Len(t, res.Rows(), 2)
}

func TestIssues27130(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("create table t1( a enum('y','b','Abc','null'),b enum('y','b','Abc','null'),key(a));")
	tk.MustQuery(`explain format=brief select * from t1 where a like "A%"`).Check(testkit.Rows(
		"TableReader 8000.00 root  data:Selection",
		"└─Selection 8000.00 cop[tikv]  like(test.t1.a, \"A%\", 92)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
	))
	tk.MustQuery(`explain format=brief select * from t1 where b like "A%"`).Check(testkit.Rows(
		"TableReader 8000.00 root  data:Selection",
		"└─Selection 8000.00 cop[tikv]  like(test.t1.b, \"A%\", 92)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
	))

	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2( a enum('y','b','Abc','null'),b enum('y','b','Abc','null'),key(a, b));")
	tk.MustQuery(`explain format=brief select * from t2 where a like "A%"`).Check(testkit.Rows(
		"IndexReader 8000.00 root  index:Selection",
		"└─Selection 8000.00 cop[tikv]  like(test.t2.a, \"A%\", 92)",
		"  └─IndexFullScan 10000.00 cop[tikv] table:t2, index:a(a, b) keep order:false, stats:pseudo",
	))
	tk.MustQuery(`explain format=brief select * from t2 where a like "A%" and b like "A%"`).Check(testkit.Rows(
		"IndexReader 8000.00 root  index:Selection",
		"└─Selection 8000.00 cop[tikv]  like(test.t2.a, \"A%\", 92), like(test.t2.b, \"A%\", 92)",
		"  └─IndexFullScan 10000.00 cop[tikv] table:t2, index:a(a, b) keep order:false, stats:pseudo",
	))

	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3( a int,b enum('y','b','Abc','null'), c enum('y','b','Abc','null'),key(a, b, c));")
	tk.MustQuery(`explain format=brief select * from t3 where a = 1 and b like "A%"`).Check(testkit.Rows(
		"IndexReader 8.00 root  index:Selection",
		"└─Selection 8.00 cop[tikv]  like(test.t3.b, \"A%\", 92)",
		"  └─IndexRangeScan 10.00 cop[tikv] table:t3, index:a(a, b, c) range:[1,1], keep order:false, stats:pseudo",
	))
}

func TestIssue27242(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists UK_MU16407")
	tk.MustExec("CREATE TABLE UK_MU16407 (COL3 timestamp NULL DEFAULT NULL, UNIQUE KEY U3(COL3));")
	defer tk.MustExec("DROP TABLE UK_MU16407")
	tk.MustExec(`insert into UK_MU16407 values("1985-08-31 18:03:27");`)
	tk.MustExec(`SELECT COL3 FROM UK_MU16407 WHERE COL3>_utf8mb4'2039-1-19 3:14:40';`)
}

func verifyTimestampOutOfRange(tk *testkit.TestKit) {
	tk.MustQuery(`select * from t28424 where t != "2038-1-19 3:14:08"`).Sort().Check(testkit.Rows("1970-01-01 00:00:01]\n[2038-01-19 03:14:07"))
	tk.MustQuery(`select * from t28424 where t < "2038-1-19 3:14:08"`).Sort().Check(testkit.Rows("1970-01-01 00:00:01]\n[2038-01-19 03:14:07"))
	tk.MustQuery(`select * from t28424 where t <= "2038-1-19 3:14:08"`).Sort().Check(testkit.Rows("1970-01-01 00:00:01]\n[2038-01-19 03:14:07"))
	tk.MustQuery(`select * from t28424 where t >= "2038-1-19 3:14:08"`).Check(testkit.Rows())
	tk.MustQuery(`select * from t28424 where t > "2038-1-19 3:14:08"`).Check(testkit.Rows())
	tk.MustQuery(`select * from t28424 where t != "1970-1-1 0:0:0"`).Sort().Check(testkit.Rows("1970-01-01 00:00:01]\n[2038-01-19 03:14:07"))
	tk.MustQuery(`select * from t28424 where t < "1970-1-1 0:0:0"`).Check(testkit.Rows())
	tk.MustQuery(`select * from t28424 where t <= "1970-1-1 0:0:0"`).Check(testkit.Rows())
	tk.MustQuery(`select * from t28424 where t >= "1970-1-1 0:0:0"`).Sort().Check(testkit.Rows("1970-01-01 00:00:01]\n[2038-01-19 03:14:07"))
	tk.MustQuery(`select * from t28424 where t > "1970-1-1 0:0:0"`).Sort().Check(testkit.Rows("1970-01-01 00:00:01]\n[2038-01-19 03:14:07"))
}

func TestIssue28424(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t28424, dt28242")

	tk.MustExec(`set time_zone='+00:00'`)
	tk.MustExec(`drop table if exists t28424,dt28424`)
	tk.MustExec(`create table t28424 (t timestamp)`)
	defer tk.MustExec("DROP TABLE t28424")
	tk.MustExec(`insert into t28424 values ("2038-01-19 03:14:07"), ("1970-01-01 00:00:01")`)

	verifyTimestampOutOfRange(tk)
	tk.MustExec(`alter table t28424 add unique index (t)`)
	verifyTimestampOutOfRange(tk)
	tk.MustExec(`create table dt28424 (dt datetime)`)
	defer tk.MustExec("DROP TABLE dt28424")
	tk.MustExec(`insert into dt28424 values ("2038-01-19 03:14:07"), ("1970-01-01 00:00:01")`)
	tk.MustExec(`insert into dt28424 values ("1969-12-31 23:59:59"), ("1970-01-01 00:00:00"), ("2038-03-19 03:14:08")`)
	tk.MustQuery(`select * from t28424 right join dt28424 on t28424.t = dt28424.dt`).Sort().Check(testkit.Rows(
		"1970-01-01 00:00:01 1970-01-01 00:00:01]\n" +
			"[2038-01-19 03:14:07 2038-01-19 03:14:07]\n" +
			"[<nil> 1969-12-31 23:59:59]\n" +
			"[<nil> 1970-01-01 00:00:00]\n" +
			"[<nil> 2038-03-19 03:14:08"))
}

func TestTemporaryTableForCte(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create temporary table tmp1(a int, b int, c int);")
	tk.MustExec("insert into tmp1 values (1,1,1),(2,2,2),(3,3,3),(4,4,4);")
	rows := tk.MustQuery("with cte1 as (with cte2 as (select * from tmp1) select * from cte2) select * from cte1 left join tmp1 on cte1.c=tmp1.c;")
	rows.Check(testkit.Rows("1 1 1 1 1 1", "2 2 2 2 2 2", "3 3 3 3 3 3", "4 4 4 4 4 4"))
	rows = tk.MustQuery("with cte1 as (with cte2 as (select * from tmp1) select * from cte2) select * from cte1 t1 left join cte1 t2 on t1.c=t2.c;")
	rows.Check(testkit.Rows("1 1 1 1 1 1", "2 2 2 2 2 2", "3 3 3 3 3 3", "4 4 4 4 4 4"))
	rows = tk.MustQuery("WITH RECURSIVE cte(a) AS (SELECT 1 UNION SELECT a+1 FROM tmp1 WHERE a < 5) SELECT * FROM cte order by a;")
	rows.Check(testkit.Rows("1", "2", "3", "4", "5"))
}

func TestIssue27797(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	origin := tk.MustQuery("SELECT @@session.tidb_partition_prune_mode")
	originStr := origin.Rows()[0][0].(string)
	defer func() {
		tk.MustExec("set @@session.tidb_partition_prune_mode = '" + originStr + "'")
	}()
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t27797")
	tk.MustExec("create table t27797(a int, b int, c int, d int) " +
		"partition by range columns(d) (" +
		"partition p0 values less than (20)," +
		"partition p1 values less than(40)," +
		"partition p2 values less than(60));")
	tk.MustExec("insert into t27797 values(1,1,1,1), (2,2,2,2), (22,22,22,22), (44,44,44,44);")
	tk.MustExec("set sql_mode='';")
	result := tk.MustQuery("select count(*) from (select a, b from t27797 where d > 1 and d < 60 and b > 0 group by b, c) tt;")
	result.Check(testkit.Rows("3"))

	tk.MustExec("drop table if exists IDT_HP24172")
	tk.MustExec("CREATE TABLE `IDT_HP24172` ( " +
		"`COL1` mediumint(16) DEFAULT NULL, " +
		"`COL2` varchar(20) DEFAULT NULL, " +
		"`COL4` datetime DEFAULT NULL, " +
		"`COL3` bigint(20) DEFAULT NULL, " +
		"`COL5` float DEFAULT NULL, " +
		"KEY `UM_COL` (`COL1`,`COL3`) " +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin " +
		"PARTITION BY HASH( `COL1`+`COL3` ) " +
		"PARTITIONS 8;")
	tk.MustExec("insert into IDT_HP24172(col1) values(8388607);")
	result = tk.MustQuery("select col2 from IDT_HP24172 where col1 = 8388607 and col1 in (select col1 from IDT_HP24172);")
	result.Check(testkit.Rows("<nil>"))
}

func TestIssue27949(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t27949")
	tk.MustExec("create table t27949 (a int, b int, key(b))")
	tk.MustQuery("explain format = 'brief' select * from t27949 where b=1").Check(testkit.Rows("IndexLookUp 10.00 root  ",
		"├─IndexRangeScan(Build) 10.00 cop[tikv] table:t27949, index:b(b) range:[1,1], keep order:false, stats:pseudo",
		"└─TableRowIDScan(Probe) 10.00 cop[tikv] table:t27949 keep order:false, stats:pseudo"))
	tk.MustExec("create global binding for select * from t27949 where b=1 using select * from t27949 ignore index(b) where b=1")
	tk.MustQuery("explain format = 'brief' select * from t27949 where b=1").Check(testkit.Rows("TableReader 10.00 root  data:Selection",
		"└─Selection 10.00 cop[tikv]  eq(test.t27949.b, 1)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t27949 keep order:false, stats:pseudo"))
	tk.MustExec("set @@sql_select_limit=100")
	tk.MustQuery("explain format = 'brief' select * from t27949 where b=1").Check(testkit.Rows("Limit 10.00 root  offset:0, count:100",
		"└─TableReader 10.00 root  data:Limit",
		"  └─Limit 10.00 cop[tikv]  offset:0, count:100",
		"    └─Selection 10.00 cop[tikv]  eq(test.t27949.b, 1)",
		"      └─TableFullScan 10000.00 cop[tikv] table:t27949 keep order:false, stats:pseudo"))

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx_a(a));")
	tk.MustExec("create binding for select * from t  using select * from t use index(idx_a);")
	tk.MustExec("select * from t;")
	tk.MustQuery("select @@last_plan_from_binding;").Check(testkit.Rows("1"))
	tk.MustExec("prepare stmt from 'select * from t';")
	tk.MustExec("execute stmt;")
	tk.MustQuery("select @@last_plan_from_binding;").Check(testkit.Rows("1"))
}

func TestIssue28154(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	defer func() {
		tk.MustExec("drop table if exists t")
	}()
	tk.MustExec("create table t(a TEXT)")
	tk.MustExec("insert into t values('abc')")
	result := tk.MustQuery("select * from t where from_base64('')")
	result.Check(testkit.Rows())
	_, err := tk.Exec("update t set a = 'def' where from_base64('')")
	require.EqualError(t, err, "[types:1292]Truncated incorrect DOUBLE value: ''")
	result = tk.MustQuery("select * from t where from_base64('invalidbase64')")
	result.Check(testkit.Rows())
	tk.MustExec("update t set a = 'hig' where from_base64('invalidbase64')")
	result = tk.MustQuery("select * from t where from_base64('test')")
	result.Check(testkit.Rows())
	_, err = tk.Exec("update t set a = 'xyz' where from_base64('test')")
	require.Error(t, err)
	require.Regexp(t, "\\[types:1292\\]Truncated incorrect DOUBLE value.*", err.Error())
	result = tk.MustQuery("select * from t")
	result.Check(testkit.Rows("abc"))
}

func TestIssues29711(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists tbl_29711")
	tk.MustExec("CREATE TABLE `tbl_29711` (" +
		"`col_250` text COLLATE utf8_unicode_ci NOT NULL," +
		"`col_251` enum('Alice','Bob','Charlie','David') COLLATE utf8_unicode_ci NOT NULL DEFAULT 'Charlie'," +
		"PRIMARY KEY (`col_251`,`col_250`(1)) NONCLUSTERED);")
	tk.MustQuery("explain format=brief " +
		"select col_250,col_251 from tbl_29711 where col_251 between 'Bob' and 'David' order by col_250,col_251 limit 6;").
		Check(testkit.Rows(
			"TopN 6.00 root  test.tbl_29711.col_250, test.tbl_29711.col_251, offset:0, count:6",
			"└─IndexLookUp 6.00 root  ",
			"  ├─IndexRangeScan(Build) 30.00 cop[tikv] table:tbl_29711, index:PRIMARY(col_251, col_250) range:[\"Bob\",\"Bob\"], [\"Charlie\",\"Charlie\"], [\"David\",\"David\"], keep order:false, stats:pseudo",
			"  └─TopN(Probe) 6.00 cop[tikv]  test.tbl_29711.col_250, test.tbl_29711.col_251, offset:0, count:6",
			"    └─TableRowIDScan 30.00 cop[tikv] table:tbl_29711 keep order:false, stats:pseudo",
		))

	tk.MustExec("drop table if exists t29711")
	tk.MustExec("CREATE TABLE `t29711` (" +
		"`a` varchar(10) DEFAULT NULL," +
		"`b` int(11) DEFAULT NULL," +
		"`c` int(11) DEFAULT NULL," +
		"KEY `ia` (`a`(2)))")
	tk.MustQuery("explain format=brief select * from t29711 use index (ia) order by a limit 10;").
		Check(testkit.Rows(
			"TopN 10.00 root  test.t29711.a, offset:0, count:10",
			"└─IndexLookUp 10.00 root  ",
			"  ├─IndexFullScan(Build) 10000.00 cop[tikv] table:t29711, index:ia(a) keep order:false, stats:pseudo",
			"  └─TopN(Probe) 10.00 cop[tikv]  test.t29711.a, offset:0, count:10",
			"    └─TableRowIDScan 10000.00 cop[tikv] table:t29711 keep order:false, stats:pseudo",
		))
}

func TestIssue27313(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(100), b int, c int, index idx1(a(2), b), index idx2(a))")
	tk.MustExec("explain format = 'verbose' select * from t where a = 'abcdefghijk' and b > 4")
	// no warning indicates that idx2 is not pruned by idx1.
	tk.MustQuery("show warnings").Check(testkit.Rows())
}

func TestIssue30094(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t30094;`)
	tk.MustExec(`create table t30094(a varchar(10));`)
	tk.MustQuery(`explain format = 'brief' select * from t30094 where cast(a as float) and cast(a as char);`).Check(testkit.Rows(
		"TableReader 8000.00 root  data:Selection",
		"└─Selection 8000.00 cop[tikv]  cast(test.t30094.a, float BINARY), cast(test.t30094.a, var_string(5))",
		"  └─TableFullScan 10000.00 cop[tikv] table:t30094 keep order:false, stats:pseudo",
	))
	tk.MustQuery(`explain format = 'brief' select * from t30094 where  concat(a,'1') = _binary 0xe59388e59388e59388 collate binary and concat(a,'1') = _binary 0xe598bfe598bfe598bf collate binary;`).Check(testkit.Rows(
		"TableReader 8000.00 root  data:Selection",
		"└─Selection 8000.00 cop[tikv]  eq(concat(test.t30094.a, \"1\"), \"0xe59388e59388e59388\"), eq(concat(test.t30094.a, \"1\"), \"0xe598bfe598bfe598bf\")",
		"  └─TableFullScan 10000.00 cop[tikv] table:t30094 keep order:false, stats:pseudo",
	))
}

func TestIssue29705(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	origin := tk.MustQuery("SELECT @@session.tidb_partition_prune_mode")
	originStr := origin.Rows()[0][0].(string)
	defer func() {
		tk.MustExec("set @@session.tidb_partition_prune_mode = '" + originStr + "'")
	}()
	tk.MustExec("set @@session.tidb_partition_prune_mode = 'static'")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(id int) partition by hash(id) partitions 4;")
	tk.MustExec("insert into t values(1);")
	result := tk.MustQuery("SELECT COUNT(1) FROM ( SELECT COUNT(1) FROM t b GROUP BY id) a;")
	result.Check(testkit.Rows("1"))
}

func TestIssue30271(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a char(10), b char(10), c char(10), index (a, b, c)) collate utf8mb4_bin;")
	tk.MustExec("insert into t values ('b', 'a', '1'), ('b', 'A', '2'), ('c', 'a', '3');")
	tk.MustExec("set names utf8mb4 collate utf8mb4_general_ci;")
	tk.MustQuery("select * from t where (a>'a' and b='a') or (b = 'A' and a < 'd') order by a,c;").Check(testkit.Rows("b a 1", "b A 2", "c a 3"))
}

func TestIssue30804(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("create table t2(a int, b int)")
	// minimal reproduction of https://github.com/pingcap/tidb/issues/30804
	tk.MustExec("select avg(0) over w from t1 window w as (order by (select 1))")
	// named window cannot be used in subquery
	err := tk.ExecToErr("select avg(0) over w from t1 where b > (select sum(t2.a) over w from t2) window w as (partition by t1.b)")
	require.True(t, core.ErrWindowNoSuchWindow.Equal(err))
	tk.MustExec("select avg(0) over w1 from t1 where b > (select sum(t2.a) over w2 from t2 window w2 as (partition by t2.b)) window w1 as (partition by t1.b)")
}

func TestIndexMergeWarning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tk.MustExec("select /*+ use_index_merge(t1) */ * from t1 where c1 < 1 or c2 < 1")
	warningMsg := "Warning 1105 IndexMerge is inapplicable or disabled. No available filter or available index."
	tk.MustQuery("show warnings").Check(testkit.Rows(warningMsg))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1(c1 int, c2 int, key(c1), key(c2))")
	tk.MustExec("select /*+ use_index_merge(t1), no_index_merge() */ * from t1 where c1 < 1 or c2 < 1")
	warningMsg = "Warning 1105 IndexMerge is inapplicable or disabled. Got no_index_merge hint or tidb_enable_index_merge is off."
	tk.MustQuery("show warnings").Check(testkit.Rows(warningMsg))

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create temporary table t1(c1 int, c2 int, key(c1), key(c2))")
	tk.MustExec("select /*+ use_index_merge(t1) */ * from t1 where c1 < 1 or c2 < 1")
	warningMsg = "Warning 1105 IndexMerge is inapplicable or disabled. Cannot use IndexMerge on temporary table."
	tk.MustQuery("show warnings").Check(testkit.Rows(warningMsg))
}

func TestIssue20510(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("CREATE TABLE t1 (a int PRIMARY KEY, b int)")
	tk.MustExec("CREATE TABLE t2 (a int PRIMARY KEY, b int)")
	tk.MustExec("INSERT INTO t1 VALUES (1,1), (2,1), (3,1), (4,2)")
	tk.MustExec("INSERT INTO t2 VALUES (1,2), (2,2)")

	tk.MustQuery("explain format=brief SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.a WHERE not(0+(t1.a=30 and t2.b=1));").Check(testkit.Rows(
		"Selection 8000.00 root  not(plus(0, and(eq(test.t1.a, 30), eq(test.t2.b, 1))))",
		"└─MergeJoin 10000.00 root  left outer join, left key:test.t1.a, right key:test.t2.a",
		"  ├─TableReader(Build) 8000.00 root  data:Selection",
		"  │ └─Selection 8000.00 cop[tikv]  not(istrue_with_null(plus(0, and(eq(test.t2.a, 30), eq(test.t2.b, 1)))))",
		"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:true, stats:pseudo",
		"  └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"    └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:true, stats:pseudo"))
	tk.MustQuery("SELECT * FROM t1 LEFT JOIN t2 ON t1.a=t2.a WHERE not(0+(t1.a=30 and t2.b=1));").Check(testkit.Rows(
		"1 1 1 2",
		"2 1 2 2",
		"3 1 <nil> <nil>",
		"4 2 <nil> <nil>",
	))
}

func TestIssue31035(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 longtext, c2 decimal(37, 4), unique key(c1(10)), unique key(c2));")
	tk.MustExec("insert into t1 values('眐', -962541614831459.7458);")
	tk.MustQuery("select * from t1 order by c2 + 10;").Check(testkit.Rows("眐 -962541614831459.7458"))
}

// TestDNFCondSelectivityWithConst test selectivity calculation with DNF conditions with one is const.
// Close https://github.com/pingcap/tidb/issues/31096
func TestDNFCondSelectivityWithConst(t *testing.T) {
	store := testkit.CreateMockStore(t)
	testKit := testkit.NewTestKit(t, store)
	testKit.MustExec("use test")
	testKit.MustExec("drop table if exists t1")
	testKit.MustExec("create table t1(a int, b int, c int);")
	testKit.MustExec("insert into t1 value(10,10,10)")
	for i := 0; i < 7; i++ {
		testKit.MustExec("insert into t1 select * from t1")
	}
	testKit.MustExec("insert into t1 value(1,1,1)")
	testKit.MustExec("analyze table t1")

	testKit.MustQuery("explain format = 'brief' select * from t1 where a=1 or b=1;").Check(testkit.Rows(
		"TableReader 1.99 root  data:Selection",
		"└─Selection 1.99 cop[tikv]  or(eq(test.t1.a, 1), eq(test.t1.b, 1))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustQuery("explain format = 'brief' select * from t1 where 0=1 or a=1 or b=1;").Check(testkit.Rows(
		"TableReader 1.99 root  data:Selection",
		"└─Selection 1.99 cop[tikv]  or(0, or(eq(test.t1.a, 1), eq(test.t1.b, 1)))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustQuery("explain format = 'brief' select * from t1 where null or a=1 or b=1;").Check(testkit.Rows(
		"TableReader 1.99 root  data:Selection",
		"└─Selection 1.99 cop[tikv]  or(0, or(eq(test.t1.a, 1), eq(test.t1.b, 1)))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustQuery("explain format = 'brief' select * from t1 where a=1 or false or b=1;").Check(testkit.Rows(
		"TableReader 1.99 root  data:Selection",
		"└─Selection 1.99 cop[tikv]  or(eq(test.t1.a, 1), or(0, eq(test.t1.b, 1)))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustQuery("explain format = 'brief' select * from t1 where a=1 or b=1 or \"false\";").Check(testkit.Rows(
		"TableReader 1.99 root  data:Selection",
		"└─Selection 1.99 cop[tikv]  or(eq(test.t1.a, 1), or(eq(test.t1.b, 1), 0))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustQuery("explain format = 'brief' select * from t1 where 1=1 or a=1 or b=1;").Check(testkit.Rows(
		"TableReader 129.00 root  data:Selection",
		"└─Selection 129.00 cop[tikv]  or(1, or(eq(test.t1.a, 1), eq(test.t1.b, 1)))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustQuery("explain format = 'brief' select * from t1 where a=1 or b=1 or 1=1;").Check(testkit.Rows(
		"TableReader 129.00 root  data:Selection",
		"└─Selection 129.00 cop[tikv]  or(eq(test.t1.a, 1), or(eq(test.t1.b, 1), 1))",
		"  └─TableFullScan 129.00 cop[tikv] table:t1 keep order:false"))
	testKit.MustExec("drop table if exists t1")
}

func TestIssue31202(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t31202(a int primary key, b int);")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t31202", L: "t31202"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	tk.MustQuery("explain format = 'brief' select * from t31202;").Check(testkit.Rows(
		"TableReader 10000.00 root  MppVersion: 1, data:ExchangeSender",
		"└─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─TableFullScan 10000.00 mpp[tiflash] table:t31202 keep order:false, stats:pseudo"))

	tk.MustQuery("explain format = 'brief' select * from t31202 use index (primary);").Check(testkit.Rows(
		"TableReader 10000.00 root  data:TableFullScan",
		"└─TableFullScan 10000.00 cop[tikv] table:t31202 keep order:false, stats:pseudo"))
	tk.MustExec("drop table if exists t31202")
}

func TestNaturalJoinUpdateSameTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("create database natural_join_update")
	defer tk.MustExec("drop database natural_join_update")
	tk.MustExec("use natural_join_update")
	tk.MustExec("create table t1(a int, b int)")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustExec("update t1 as a natural join t1 b SET a.a = 2, b.b = 3")
	tk.MustQuery("select * from t1").Sort().Check(testkit.Rows("2 3", "2 3"))
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a int primary key, b int)")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustGetErrCode(`update t1 as a natural join t1 b SET a.a = 2, b.b = 3`, mysql.ErrMultiUpdateKeyConflict)
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (a int, b int) partition by hash (a) partitions 3")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustGetErrCode(`update t1 as a natural join t1 b SET a.a = 2, b.b = 3`, mysql.ErrMultiUpdateKeyConflict)
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (A int, b int) partition by hash (b) partitions 3")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustGetErrCode(`update t1 as a natural join t1 B SET a.A = 2, b.b = 3`, mysql.ErrMultiUpdateKeyConflict)
	_, err := tk.Exec(`update t1 as a natural join t1 B SET a.A = 2, b.b = 3`)
	require.Error(t, err)
	require.Regexp(t, ".planner:1706.Primary key/partition key update is not allowed since the table is updated both as 'a' and 'B'.", err.Error())
	tk.MustExec("drop table t1")
	tk.MustExec("create table t1 (A int, b int) partition by RANGE COLUMNS (b) (partition `pNeg` values less than (0),partition `pPos` values less than MAXVALUE)")
	tk.MustExec("insert into t1 values (1,1),(2,2)")
	tk.MustGetErrCode(`update t1 as a natural join t1 B SET a.A = 2, b.b = 3`, mysql.ErrMultiUpdateKeyConflict)
	tk.MustExec("drop table t1")
}

func TestAggPushToCopForCachedTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec(`create table t32157(
  process_code varchar(8) NOT NULL,
  ctrl_class varchar(2) NOT NULL,
  ctrl_type varchar(1) NOT NULL,
  oper_no varchar(12) DEFAULT NULL,
  modify_date datetime DEFAULT NULL,
  d_c_flag varchar(2) NOT NULL,
  PRIMARY KEY (process_code,ctrl_class,d_c_flag) NONCLUSTERED);`)
	tk.MustExec("insert into t32157 values ('GDEP0071', '05', '1', '10000', '2016-06-29 00:00:00', 'C')")
	tk.MustExec("insert into t32157 values ('GDEP0071', '05', '0', '0000', '2016-06-01 00:00:00', 'D')")
	tk.MustExec("alter table t32157 cache")

	tk.MustQuery("explain format = 'brief' select /*+AGG_TO_COP()*/ count(*) from t32157 ignore index(primary) where process_code = 'GDEP0071'").Check(testkit.Rows(
		"StreamAgg 1.00 root  funcs:count(1)->Column#8]\n" +
			"[└─UnionScan 10.00 root  eq(test.t32157.process_code, \"GDEP0071\")]\n" +
			"[  └─TableReader 10.00 root  data:Selection]\n" +
			"[    └─Selection 10.00 cop[tikv]  eq(test.t32157.process_code, \"GDEP0071\")]\n" +
			"[      └─TableFullScan 10000.00 cop[tikv] table:t32157 keep order:false, stats:pseudo"))

	var readFromCacheNoPanic bool
	for i := 0; i < 10; i++ {
		tk.MustQuery("select /*+AGG_TO_COP()*/ count(*) from t32157 ignore index(primary) where process_code = 'GDEP0071'").Check(testkit.Rows("2"))
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			readFromCacheNoPanic = true
			break
		}
	}
	require.True(t, readFromCacheNoPanic)

	tk.MustExec("drop table if exists t31202")
}

func TestTiFlashFineGrainedShuffleWithMaxTiFlashThreads(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@tidb_enforce_mpp = on")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec("create table t1(c1 int, c2 int)")
	tbl1, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t1", L: "t1"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	sql := "explain select row_number() over w1 from t1 window w1 as (partition by c1);"

	getStreamCountFromExplain := func(rows [][]interface{}) (res []uint64) {
		re := regexp.MustCompile("stream_count: ([0-9]+)")
		for _, row := range rows {
			buf := bytes.NewBufferString("")
			_, _ = fmt.Fprintf(buf, "%s\n", row)
			if matched := re.FindStringSubmatch(buf.String()); matched != nil {
				require.Equal(t, len(matched), 2)
				c, err := strconv.ParseUint(matched[1], 10, 64)
				require.NoError(t, err)
				res = append(res, c)
			}
		}
		return res
	}

	// tiflash_fine_grained_shuffle_stream_count should be same with tidb_max_tiflash_threads.
	tk.MustExec("set @@tiflash_fine_grained_shuffle_stream_count = 0")
	tk.MustExec("set @@tidb_max_tiflash_threads = 10")
	rows := tk.MustQuery(sql).Rows()
	streamCount := getStreamCountFromExplain(rows)
	// require.Equal(t, len(streamCount), 1)
	require.Equal(t, uint64(10), streamCount[0])

	// tiflash_fine_grained_shuffle_stream_count should be default value when tidb_max_tiflash_threads is -1.
	tk.MustExec("set @@tiflash_fine_grained_shuffle_stream_count = 0")
	tk.MustExec("set @@tidb_max_tiflash_threads = -1")
	rows = tk.MustQuery(sql).Rows()
	streamCount = getStreamCountFromExplain(rows)
	// require.Equal(t, len(streamCount), 1)
	require.Equal(t, uint64(variable.DefStreamCountWhenMaxThreadsNotSet), streamCount[0])

	// tiflash_fine_grained_shuffle_stream_count should be default value when tidb_max_tiflash_threads is 0.
	tk.MustExec("set @@tiflash_fine_grained_shuffle_stream_count = 0")
	tk.MustExec("set @@tidb_max_tiflash_threads = 0")
	rows = tk.MustQuery(sql).Rows()
	streamCount = getStreamCountFromExplain(rows)
	// require.Equal(t, len(streamCount), 1)
	require.Equal(t, uint64(variable.DefStreamCountWhenMaxThreadsNotSet), streamCount[0])

	// Disabled when tiflash_fine_grained_shuffle_stream_count is -1.
	tk.MustExec("set @@tiflash_fine_grained_shuffle_stream_count = -1")
	tk.MustExec("set @@tidb_max_tiflash_threads = 10")
	rows = tk.MustQuery(sql).Rows()
	streamCount = getStreamCountFromExplain(rows)
	require.Equal(t, len(streamCount), 0)

	// Test when tiflash_fine_grained_shuffle_stream_count is greater than 0.
	tk.MustExec("set @@tiflash_fine_grained_shuffle_stream_count = 16")
	tk.MustExec("set @@tidb_max_tiflash_threads = 10")
	rows = tk.MustQuery(sql).Rows()
	streamCount = getStreamCountFromExplain(rows)
	// require.Equal(t, len(streamCount), 1)
	require.Equal(t, uint64(16), streamCount[0])
}

func TestIssue33175(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("create table t (id bigint(45) unsigned not null, c varchar(20), primary key(id));")
	tk.MustExec("insert into t values (9734095886065816707, 'a'), (10353107668348738101, 'b'), (0, 'c');")
	tk.MustExec("begin")
	tk.MustExec("insert into t values (33, 'd');")
	tk.MustQuery("select max(id) from t;").Check(testkit.Rows("10353107668348738101"))
	tk.MustExec("rollback")

	tk.MustExec("alter table t cache")
	for {
		tk.MustQuery("select max(id) from t;").Check(testkit.Rows("10353107668348738101"))
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			break
		}
	}

	// // With subquery, like the original issue case.
	for {
		tk.MustQuery("select * from t where id > (select  max(id) from t where t.id > 0);").Check(testkit.Rows())
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			break
		}
	}

	// Test order by desc / asc.
	tk.MustQuery("select id from t order by id desc;").Check(testkit.Rows(
		"10353107668348738101",
		"9734095886065816707",
		"0"))

	tk.MustQuery("select id from t order by id asc;").Check(testkit.Rows(
		"0",
		"9734095886065816707",
		"10353107668348738101"))

	tk.MustExec("alter table t nocache")
	tk.MustExec("drop table t")

	// Cover more code that use union scan
	// TableReader/IndexReader/IndexLookup
	for idx, q := range []string{
		"create temporary table t (id bigint unsigned, c int default null, index(id))",
		"create temporary table t (id bigint unsigned primary key)",
	} {
		tk.MustExec(q)
		tk.MustExec("insert into t(id) values (1), (3), (9734095886065816707), (9734095886065816708)")
		tk.MustQuery("select min(id) from t").Check(testkit.Rows("1"))
		tk.MustQuery("select max(id) from t").Check(testkit.Rows("9734095886065816708"))
		tk.MustQuery("select id from t order by id asc").Check(testkit.Rows(
			"1", "3", "9734095886065816707", "9734095886065816708"))
		tk.MustQuery("select id from t order by id desc").Check(testkit.Rows(
			"9734095886065816708", "9734095886065816707", "3", "1"))
		if idx == 0 {
			tk.MustQuery("select * from t order by id asc").Check(testkit.Rows(
				"1 <nil>",
				"3 <nil>",
				"9734095886065816707 <nil>",
				"9734095886065816708 <nil>"))
			tk.MustQuery("select * from t order by id desc").Check(testkit.Rows(
				"9734095886065816708 <nil>",
				"9734095886065816707 <nil>",
				"3 <nil>",
				"1 <nil>"))
		}
		tk.MustExec("drop table t")
	}

	// More and more test
	tk.MustExec("create global temporary table `tmp1` (id bigint unsigned primary key) on commit delete rows;")
	tk.MustExec("begin")
	tk.MustExec("insert into tmp1 values (0),(1),(2),(65536),(9734095886065816707),(9734095886065816708);")
	tk.MustQuery("select * from tmp1 where id <= 65534 or (id > 65535 and id < 9734095886065816700) or id >= 9734095886065816707 order by id desc;").Check(testkit.Rows(
		"9734095886065816708", "9734095886065816707", "65536", "2", "1", "0"))

	tk.MustQuery("select * from tmp1 where id <= 65534 or (id > 65535 and id < 9734095886065816700) or id >= 9734095886065816707 order by id asc;").Check(testkit.Rows(
		"0", "1", "2", "65536", "9734095886065816707", "9734095886065816708"))

	tk.MustExec("create global temporary table `tmp2` (id bigint primary key) on commit delete rows;")
	tk.MustExec("begin")
	tk.MustExec("insert into tmp2 values(-2),(-1),(0),(1),(2);")
	tk.MustQuery("select * from tmp2 where id <= -1 or id > 0 order by id desc;").Check(testkit.Rows("2", "1", "-1", "-2"))
	tk.MustQuery("select * from tmp2 where id <= -1 or id > 0 order by id asc;").Check(testkit.Rows("-2", "-1", "1", "2"))
}

func TestIssue33042(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t1(id int primary key, col1 int)")
	tk.MustExec("create table t2(id int primary key, col1 int)")
	tk.MustQuery("explain format='brief' SELECT /*+ merge_join(t1, t2)*/ * FROM (t1 LEFT JOIN t2 ON t1.col1=t2.id) order by t2.id;").Check(
		testkit.Rows(
			"Sort 12500.00 root  test.t2.id",
			"└─MergeJoin 12500.00 root  left outer join, left key:test.t1.col1, right key:test.t2.id",
			"  ├─TableReader(Build) 10000.00 root  data:TableFullScan",
			"  │ └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:true, stats:pseudo",
			"  └─Sort(Probe) 10000.00 root  test.t1.col1",
			"    └─TableReader 10000.00 root  data:TableFullScan",
			"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
		),
	)
}

func TestIssue29663(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t1 (a int, b int)")
	tk.MustExec("create table t2 (c int, d int)")
	tk.MustExec("insert into t1 values(1, 1), (1,2),(2,1),(2,2)")
	tk.MustExec("insert into t2 values(1, 3), (1,4),(2,5),(2,6)")

	tk.MustQuery("explain select one.a from t1 one order by (select two.d from t2 two where two.c = one.b)").Check(testkit.Rows(
		"Projection_16 10000.00 root  test.t1.a",
		"└─Sort_17 10000.00 root  test.t2.d",
		"  └─Apply_20 10000.00 root  CARTESIAN left outer join",
		"    ├─TableReader_22(Build) 10000.00 root  data:TableFullScan_21",
		"    │ └─TableFullScan_21 10000.00 cop[tikv] table:one keep order:false, stats:pseudo",
		"    └─MaxOneRow_23(Probe) 10000.00 root  ",
		"      └─TableReader_26 20000.00 root  data:Selection_25",
		"        └─Selection_25 20000.00 cop[tikv]  eq(test.t2.c, test.t1.b)",
		"          └─TableFullScan_24 20000000.00 cop[tikv] table:two keep order:false, stats:pseudo"))
}

func TestIssue31609(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustQuery("explain select rank() over (partition by table_name) from information_schema.tables").Check(testkit.Rows(
		"Projection_7 10000.00 root  Column#27",
		"└─Shuffle_11 10000.00 root  execution info: concurrency:5, data sources:[MemTableScan_9]",
		"  └─Window_8 10000.00 root  rank()->Column#27 over(partition by Column#3)",
		"    └─Sort_10 10000.00 root  Column#3",
		"      └─MemTableScan_9 10000.00 root table:TABLES ",
	))
}

func TestDecimalOverflow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table deci (a decimal(65,30),b decimal(65,0))")
	tk.MustExec("insert into deci values (1234567890.123456789012345678901234567890,987654321098765432109876543210987654321098765432109876543210)")
	tk.MustQuery("select a from deci union ALL select b from deci;").Sort().Check(testkit.Rows("1234567890.123456789012345678901234567890", "99999999999999999999999999999999999.999999999999999999999999999999"))
}

func TestIssue35083(t *testing.T) {
	defer func() {
		variable.SetSysVar(variable.TiDBOptProjectionPushDown, variable.BoolToOnOff(config.GetGlobalConfig().Performance.ProjectionPushDown))
	}()
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Performance.ProjectionPushDown = true
	})
	variable.SetSysVar(variable.TiDBOptProjectionPushDown, variable.BoolToOnOff(config.GetGlobalConfig().Performance.ProjectionPushDown))
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t1 (a varchar(100), b int)")
	tk.MustQuery("select @@tidb_opt_projection_push_down").Check(testkit.Rows("1"))
	tk.MustQuery("explain format = 'brief' select cast(a as datetime) from t1").Check(testkit.Rows(
		"TableReader 10000.00 root  data:Projection",
		"└─Projection 10000.00 cop[tikv]  cast(test.t1.a, datetime BINARY)->Column#4",
		"  └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
}

func TestIssue25813(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a json);")
	tk.MustExec("insert into t values('{\"id\": \"ish\"}');")
	tk.MustQuery("select t2.a from t t1 left join t t2 on t1.a=t2.a where t2.a->'$.id'='ish';").Check(testkit.Rows("{\"id\": \"ish\"}"))

	tk.MustQuery("explain format = 'brief' select * from t t1 left join t t2 on t1.a=t2.a where t2.a->'$.id'='ish';").Check(testkit.Rows(
		"Selection 8000.00 root  eq(json_extract(test.t.a, \"$.id\"), cast(\"ish\", json BINARY))",
		"└─HashJoin 10000.00 root  left outer join, equal:[eq(test.t.a, test.t.a)]",
		"  ├─TableReader(Build) 8000.00 root  data:Selection",
		"  │ └─Selection 8000.00 cop[tikv]  not(isnull(cast(test.t.a, var_string(4294967295))))",
		"  │   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"  └─TableReader(Probe) 10000.00 root  data:TableFullScan",
		"    └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
}

func TestRepeatPushDownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(2147483647, 2)")
	tk.MustExec("insert into t values(12, 2)")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "repeat(cast(test.t.a, var_string(20)), test.t.b)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select repeat(a,b) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestIssue36194(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	// create virtual tiflash replica.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustQuery("explain format = 'brief' select /*+ read_from_storage(tiflash[t]) */ * from t where a + 1 > 20 limit 100;;").Check(testkit.Rows(
		"Limit 100.00 root  offset:0, count:100",
		"└─TableReader 100.00 root  MppVersion: 1, data:ExchangeSender",
		"  └─ExchangeSender 100.00 mpp[tiflash]  ExchangeType: PassThrough",
		"    └─Limit 100.00 mpp[tiflash]  offset:0, count:100",
		"      └─Selection 100.00 mpp[tiflash]  gt(plus(test.t.a, 1), 20)",
		"        └─TableFullScan 125.00 mpp[tiflash] table:t keep order:false, stats:pseudo"))
}

func TestGetFormatPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(location varchar(10));")
	tk.MustExec("insert into t values('USA'), ('JIS'), ('ISO'), ('EUR'), ('INTERNAL')")
	tk.MustExec("set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash';")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	tk.MustQuery("explain format = 'brief' select GET_FORMAT(DATE, location) from t;").Check(testkit.Rows(
		"TableReader 10000.00 root  MppVersion: 1, data:ExchangeSender",
		"└─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─Projection 10000.00 mpp[tiflash]  get_format(DATE, test.t.location)->Column#3",
		"    └─TableFullScan 10000.00 mpp[tiflash] table:t keep order:false, stats:pseudo"))
}

func TestAggWithJsonPushDownToTiFlash(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a json);")
	tk.MustExec("insert into t values(null);")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"HashAgg_6", "root", "funcs:avg(Column#4)->Column#3"},
		{"└─Projection_19", "root", "cast(test.t.a, double BINARY)->Column#4"},
		{"  └─TableReader_12", "root", "data:TableFullScan_11"},
		{"    └─TableFullScan_11", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select avg(a) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]interface{}{
		{"HashAgg_6", "root", "funcs:sum(Column#4)->Column#3"},
		{"└─Projection_19", "root", "cast(test.t.a, double BINARY)->Column#4"},
		{"  └─TableReader_12", "root", "data:TableFullScan_11"},
		{"    └─TableFullScan_11", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select sum(a) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]interface{}{
		{"HashAgg_6", "root", "funcs:group_concat(Column#4 separator \",\")->Column#3"},
		{"└─Projection_13", "root", "cast(test.t.a, var_string(4294967295))->Column#4"},
		{"  └─TableReader_10", "root", "data:TableFullScan_9"},
		{"    └─TableFullScan_9", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select /*+ hash_agg() */  group_concat(a) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestLeftShiftPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(2147483647, 32)")
	tk.MustExec("insert into t values(12, 2)")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "leftshift(test.t.a, test.t.b)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select a << b from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestIssue36609(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("use test")
	tk.MustExec("create table t1(a int, b int, c int, d int, index ia(a), index ib(b), index ic(c), index id(d))")
	tk.MustExec("create table t2(a int, b int, c int, d int, index ia(a), index ib(b), index ic(c), index id(d))")
	tk.MustExec("create table t3(a int, b int, c int, d int, index ia(a), index ib(b), index ic(c), index id(d))")
	tk.MustExec("create table t4(a int, b int, c int, d int, index ia(a), index ib(b), index ic(c), index id(d))")
	tk.MustExec("create table t5(a int, b int, c int, d int, index ia(a), index ib(b), index ic(c), index id(d))")
	tk.MustQuery("select * from t3 straight_join t4 on t3.a = t4.b straight_join t2 on t3.d = t2.c straight_join t1 on t1.a = t2.b straight_join t5 on t4.c = t5.d where t2.b < 100 and t4.a = 10;")
	tk.MustQuery("select * from information_schema.statements_summary;")
}

func TestHexIntOrStrPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(10));")
	tk.MustExec("insert into t values(1, 'tiflash');")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "hex(test.t.a)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select hex(a) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "hex(test.t.b)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select hex(b) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestBinPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int);")
	tk.MustExec("insert into t values(1);")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "bin(test.t.a)->Column#3"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select bin(a) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestEltPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(20))")
	tk.MustExec("insert into t values(2147483647, '32')")
	tk.MustExec("insert into t values(12, 'abc')")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "elt(test.t.a, test.t.b)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select elt(a, b) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestRegexpInstrPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists test.t;")
	tk.MustExec("create table test.t (expr varchar(30), pattern varchar(30), pos int, occur int, ret_op int, match_type varchar(30));")
	tk.MustExec("insert into test.t values ('123', '12.', 1, 1, 0, ''), ('aBb', 'bb', 1, 1, 0, 'i'), ('ab\nabc', '^abc$', 1, 1, 0, 'm');")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "regexp_instr(test.t.expr, test.t.pattern, 1, 1, 0, test.t.match_type)->Column#8"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select regexp_instr(expr, pattern, 1, 1, 0, match_type) as res from test.t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestRegexpSubstrPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists test.t;")
	tk.MustExec("create table test.t (expr varchar(30), pattern varchar(30), pos int, occur int, match_type varchar(30));")
	tk.MustExec("insert into test.t values ('123', '12.', 1, 1, ''), ('aBb', 'bb', 1, 1, 'i'), ('ab\nabc', '^abc$', 1, 1, 'm');")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "regexp_substr(test.t.expr, test.t.pattern, 1, 1, test.t.match_type)->Column#7"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select regexp_substr(expr, pattern, 1, 1, match_type) as res from test.t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestRegexpReplacePushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists test.t;")
	tk.MustExec("create table test.t (expr varchar(30), pattern varchar(30), repl varchar(30), pos int, occur int, match_type varchar(30));")
	tk.MustExec("insert into test.t values ('123', '12.', '233', 1, 1, ''), ('aBb', 'bb', 'bc', 1, 1, 'i'), ('ab\nabc', '^abc$', 'd', 1, 1, 'm');")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "regexp_replace(test.t.expr, test.t.pattern, test.t.repl, 1, 1, test.t.match_type)->Column#8"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select regexp_replace(expr, pattern, repl, 1, 1, match_type) as res from test.t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestCastTimeAsDurationToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a date, b datetime(4))")
	tk.MustExec("insert into t values('2021-10-26', '2021-10-26')")
	tk.MustExec("insert into t values('2021-10-26', '2021-10-26 11:11:11')")
	tk.MustExec("insert into t values('2021-10-26', '2021-10-26 11:11:11.111111')")
	tk.MustExec("insert into t values('2021-10-26', '2021-10-26 11:11:11.123456')")
	tk.MustExec("insert into t values('2021-10-26', '2021-10-26 11:11:11.999999')")

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "cast(test.t.a, time BINARY)->Column#4, cast(test.t.b, time BINARY)->Column#5"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select cast(a as time), cast(b as time) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestUnhexPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b varchar(20));")
	tk.MustExec("insert into t values(6162, '7469666C617368');")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1;")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "unhex(cast(test.t.a, var_string(20)))->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select unhex(a) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "unhex(test.t.b)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select unhex(b) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestLeastGretestStringPushDownToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(20), b varchar(20))")
	tk.MustExec("insert into t values('123', '234')")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "least(test.t.a, test.t.b)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select least(a, b) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "greatest(test.t.a, test.t.b)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select greatest(a, b) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestPartitionTableFallBackStatic(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_partition_prune_mode='static'")
	tk.MustExec("CREATE TABLE t (a int) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11));")
	tk.MustExec("insert into t values (1),(2),(3),(4),(7),(8),(9),(10)")
	tk.MustExec("analyze table t")

	// use static plan in static mode
	rows := [][]interface{}{
		{"PartitionUnion", "", ""},
		{"├─TableReader", "", "data:TableFullScan"},
		{"│ └─TableFullScan", "table:t, partition:p0", "keep order:false"},
		{"└─TableReader", "", "data:TableFullScan"},
		{"  └─TableFullScan", "table:t, partition:p1", "keep order:false"},
	}
	tk.MustQuery("explain format='brief' select * from t").CheckAt([]int{0, 3, 4}, rows)

	tk.MustExec("CREATE TABLE t2 (a int) PARTITION BY RANGE (a) (PARTITION p0 VALUES LESS THAN (6),PARTITION p1 VALUES LESS THAN (11));")
	tk.MustExec("insert into t2 values (1),(2),(3),(4),(7),(8),(9),(10)")
	tk.MustExec("analyze table t2")
	tk.MustExec("set @@tidb_partition_prune_mode='dynamic'")

	// use static plan in dynamic mode due to having not global stats
	tk.MustQuery("explain format='brief' select * from t").CheckAt([]int{0, 3, 4}, rows)
	tk.MustExec("analyze table t")

	// use dynamic plan in dynamic mode with global stats
	rows = [][]interface{}{
		{"TableReader", "partition:all", "data:TableFullScan"},
		{"└─TableFullScan", "table:t", "keep order:false"},
	}
	tk.MustQuery("explain format='brief' select * from t").CheckAt([]int{0, 3, 4}, rows)

	rows = [][]interface{}{
		{"Union", "", ""},
		{"├─PartitionUnion", "", ""},
		{"│ ├─TableReader", "", "data:TableFullScan"},
		{"│ │ └─TableFullScan", "table:t, partition:p0", "keep order:false"},
		{"│ └─TableReader", "", "data:TableFullScan"},
		{"│   └─TableFullScan", "table:t, partition:p1", "keep order:false"},
		{"└─PartitionUnion", "", ""},
		{"  ├─TableReader", "", "data:TableFullScan"},
		{"  │ └─TableFullScan", "table:t2, partition:p0", "keep order:false"},
		{"  └─TableReader", "", "data:TableFullScan"},
		{"    └─TableFullScan", "table:t2, partition:p1", "keep order:false"},
	}
	// use static plan in dynamic mode due to t2 has no global stats
	tk.MustQuery("explain format='brief' select  * from t union all select * from t2;").CheckAt([]int{0, 3, 4}, rows)
}

func TestEnableTiFlashReadForWriteStmt(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1, 2)")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("set @@tidb_enable_tiflash_read_for_write_stmt = ON")

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	tbl2, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t2", L: "t2"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl2.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	checkMpp := func(r [][]interface{}) {
		check := false
		for i := range r {
			if r[i][2] == "mpp[tiflash]" {
				check = true
				break
			}
		}
		require.Equal(t, check, true)
	}

	// Insert into ... select
	rs := tk.MustQuery("explain insert into t2 select a+b from t").Rows()
	checkMpp(rs)

	rs = tk.MustQuery("explain insert into t2 select t.a from t2 join t on t2.a = t.a").Rows()
	checkMpp(rs)

	// Replace into ... select
	rs = tk.MustQuery("explain replace into t2 select a+b from t").Rows()
	checkMpp(rs)

	// CTE
	rs = tk.MustQuery("explain update t set a=a+1 where b in (select a from t2 where t.a > t2.a)").Rows()
	checkMpp(rs)
}

func TestPointGetWithSelectLock(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(a int, b int, primary key(a, b));")
	tk.MustExec("create table t1(c int unique, d int);")
	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}
	tbl1, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t1", L: "t1"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl1.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	sqls := []string{
		"explain select a, b from t where (a = 1 and b = 2) or (a =2 and b = 1) for update;",
		"explain select a, b from t where a = 1 and b = 2 for update;",
		"explain select c, d from t1 where c = 1 for update;",
		"explain select c, d from t1 where c = 1 and d = 1 for update;",
		"explain select c, d from t1 where (c = 1 or c = 2 )and d = 1 for update;",
		"explain select c, d from t1 where c in (1,2,3,4) for update;",
	}
	tk.MustExec("set @@tidb_enable_tiflash_read_for_write_stmt = on;")
	tk.MustExec("set @@tidb_isolation_read_engines='tidb,tiflash';")
	tk.MustExec("begin;")
	// assert point get / batch point get can't work with tiflash in interaction txn
	for _, sql := range sqls {
		err = tk.ExecToErr(sql)
		require.Error(t, err)
	}
	// assert point get / batch point get can work with tikv in interaction txn
	tk.MustExec("set @@tidb_isolation_read_engines='tidb,tikv,tiflash';")
	for _, sql := range sqls {
		tk.MustQuery(sql)
	}
	tk.MustExec("commit")
	// assert point get / batch point get can work with tiflash in auto commit
	tk.MustExec("set @@tidb_isolation_read_engines='tidb,tiflash';")
	for _, sql := range sqls {
		tk.MustQuery(sql)
	}
}

func TestTableRangeFallback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int primary key, b int)")
	tk.MustExec("create table t2 (c int)")
	tk.MustQuery("explain format='brief' select * from t1 where a in (10, 20, 30, 40, 50) and b > 1").Check(testkit.Rows(
		"Selection 1.67 root  gt(test.t1.b, 1)",
		"└─Batch_Point_Get 5.00 root table:t1 handle:[10 20 30 40 50], keep order:false, desc:false"))
	tk.MustQuery("explain format='brief' select * from t1 join t2 on t1.b = t2.c where t1.a in (10, 20, 30, 40, 50)").Check(testkit.Rows(
		"HashJoin 6.24 root  inner join, equal:[eq(test.t1.b, test.t2.c)]",
		"├─Selection(Build) 5.00 root  not(isnull(test.t1.b))",
		"│ └─Batch_Point_Get 5.00 root table:t1 handle:[10 20 30 40 50], keep order:false, desc:false",
		"└─TableReader(Probe) 9990.00 root  data:Selection",
		"  └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.c))",
		"    └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo"))
	tk.MustExec("set @@tidb_opt_range_max_size=10")
	tk.MustQuery("explain format='brief' select * from t1 where a in (10, 20, 30, 40, 50) and b > 1").Check(testkit.Rows(
		"TableReader 8000.00 root  data:Selection",
		"└─Selection 8000.00 cop[tikv]  gt(test.t1.b, 1), in(test.t1.a, 10, 20, 30, 40, 50)",
		"  └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 10 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))
	tk.MustQuery("explain format='brief' select * from t1 join t2 on t1.b = t2.c where t1.a in (10, 20, 30, 40, 50)").Check(testkit.Rows(
		"HashJoin 10000.00 root  inner join, equal:[eq(test.t1.b, test.t2.c)]",
		"├─TableReader(Build) 8000.00 root  data:Selection",
		"│ └─Selection 8000.00 cop[tikv]  not(isnull(test.t2.c))",
		"│   └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"└─TableReader(Probe) 8000.00 root  data:Selection",
		"  └─Selection 8000.00 cop[tikv]  in(test.t1.a, 10, 20, 30, 40, 50), not(isnull(test.t1.b))",
		"    └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 10 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))
}

func TestIndexRangeFallback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a varchar(10), b varchar(10), c varchar(10), index idx_a_b(a(2), b(2)))")
	tk.MustExec("create table t2 (d varchar(10))")
	tk.MustExec("create table t3 (pk int primary key, a int, key(a))")
	tk.MustExec("create table t4 (a int, b int, c int, index idx_a_b(a, b))")

	// Simple index range fallback case.
	tk.MustExec("set @@tidb_opt_range_max_size=0")
	tk.MustQuery("explain format='brief' select * from t1 where a in ('aaa', 'bbb', 'ccc') and b in ('ddd', 'eee', 'fff')").Check(testkit.Rows(
		"IndexLookUp 0.90 root  ",
		"├─IndexRangeScan(Build) 0.90 cop[tikv] table:t1, index:idx_a_b(a, b) range:[\"aa\" \"dd\",\"aa\" \"dd\"], [\"aa\" \"ee\",\"aa\" \"ee\"], [\"aa\" \"ff\",\"aa\" \"ff\"], [\"bb\" \"dd\",\"bb\" \"dd\"], [\"bb\" \"ee\",\"bb\" \"ee\"], [\"bb\" \"ff\",\"bb\" \"ff\"], [\"cc\" \"dd\",\"cc\" \"dd\"], [\"cc\" \"ee\",\"cc\" \"ee\"], [\"cc\" \"ff\",\"cc\" \"ff\"], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.90 cop[tikv]  in(test.t1.a, \"aaa\", \"bbb\", \"ccc\"), in(test.t1.b, \"ddd\", \"eee\", \"fff\")",
		"  └─TableRowIDScan 0.90 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustExec("set @@tidb_opt_range_max_size=1000")
	tk.MustQuery("explain format='brief' select * from t1 where a in ('aaa', 'bbb', 'ccc') and b in ('ddd', 'eee', 'fff')").Check(testkit.Rows(
		"IndexLookUp 0.09 root  ",
		"├─IndexRangeScan(Build) 30.00 cop[tikv] table:t1, index:idx_a_b(a, b) range:[\"aa\",\"aa\"], [\"bb\",\"bb\"], [\"cc\",\"cc\"], keep order:false, stats:pseudo",
		"└─Selection(Probe) 0.09 cop[tikv]  in(test.t1.a, \"aaa\", \"bbb\", \"ccc\"), in(test.t1.b, \"ddd\", \"eee\", \"fff\")",
		"  └─TableRowIDScan 30.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 1000 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))

	// Index range fallback under join.
	tk.MustExec("set @@tidb_opt_range_max_size=0")
	tk.MustQuery("explain format='brief' select * from t1 join t2 on t1.c = t2.d where a in ('aaa', 'bbb', 'ccc') and b in ('ddd', 'eee', 'fff')").Check(testkit.Rows(
		"HashJoin 1.12 root  inner join, equal:[eq(test.t1.c, test.t2.d)]",
		"├─IndexLookUp(Build) 0.90 root  ",
		"│ ├─IndexRangeScan(Build) 0.90 cop[tikv] table:t1, index:idx_a_b(a, b) range:[\"aa\" \"dd\",\"aa\" \"dd\"], [\"aa\" \"ee\",\"aa\" \"ee\"], [\"aa\" \"ff\",\"aa\" \"ff\"], [\"bb\" \"dd\",\"bb\" \"dd\"], [\"bb\" \"ee\",\"bb\" \"ee\"], [\"bb\" \"ff\",\"bb\" \"ff\"], [\"cc\" \"dd\",\"cc\" \"dd\"], [\"cc\" \"ee\",\"cc\" \"ee\"], [\"cc\" \"ff\",\"cc\" \"ff\"], keep order:false, stats:pseudo",
		"│ └─Selection(Probe) 0.90 cop[tikv]  in(test.t1.a, \"aaa\", \"bbb\", \"ccc\"), in(test.t1.b, \"ddd\", \"eee\", \"fff\"), not(isnull(test.t1.c))",
		"│   └─TableRowIDScan 0.90 cop[tikv] table:t1 keep order:false, stats:pseudo",
		"└─TableReader(Probe) 9990.00 root  data:Selection",
		"  └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.d))",
		"    └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo"))
	tk.MustExec("set @@tidb_opt_range_max_size=1000")
	tk.MustQuery("explain format='brief' select * from t1 join t2 on t1.c = t2.d where a in ('aaa', 'bbb', 'ccc') and b in ('ddd', 'eee', 'fff')").Check(testkit.Rows(
		"HashJoin 0.11 root  inner join, equal:[eq(test.t1.c, test.t2.d)]",
		"├─IndexLookUp(Build) 0.09 root  ",
		"│ ├─IndexRangeScan(Build) 30.00 cop[tikv] table:t1, index:idx_a_b(a, b) range:[\"aa\",\"aa\"], [\"bb\",\"bb\"], [\"cc\",\"cc\"], keep order:false, stats:pseudo",
		"│ └─Selection(Probe) 0.09 cop[tikv]  in(test.t1.a, \"aaa\", \"bbb\", \"ccc\"), in(test.t1.b, \"ddd\", \"eee\", \"fff\"), not(isnull(test.t1.c))",
		"│   └─TableRowIDScan 30.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
		"└─TableReader(Probe) 9990.00 root  data:Selection",
		"  └─Selection 9990.00 cop[tikv]  not(isnull(test.t2.d))",
		"    └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 1000 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))

	// Index range fallback in index column + pk column schema.
	tk.MustExec("set @@tidb_opt_range_max_size=0")
	tk.MustQuery("explain format='brief' select /*+ use_index(t3, a) */ * from t3 where a in (1, 3, 5) and pk in (2, 4, 6)").Check(testkit.Rows(
		"IndexReader 0.90 root  index:IndexRangeScan",
		"└─IndexRangeScan 0.90 cop[tikv] table:t3, index:a(a) range:[1 2,1 2], [1 4,1 4], [1 6,1 6], [3 2,3 2], [3 4,3 4], [3 6,3 6], [5 2,5 2], [5 4,5 4], [5 6,5 6], keep order:false, stats:pseudo"))
	tk.MustExec("set @@tidb_opt_range_max_size=1000")
	tk.MustQuery("explain format='brief' select /*+ use_index(t3, a) */ * from t3 where a in (1, 3, 5) and pk in (2, 4, 6)").Check(testkit.Rows(
		"IndexReader 0.01 root  index:Selection",
		"└─Selection 0.01 cop[tikv]  in(test.t3.pk, 2, 4, 6)",
		"  └─IndexRangeScan 30.00 cop[tikv] table:t3, index:a(a) range:[1,1], [3,3], [5,5], keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 1000 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))

	// Index range fallback for idx_col1 in (?, ?, ?) and idx_col2 = ? conditions. Prevent SplitCorColAccessCondFromFilters
	// adding idx_col2 = ? back to access conditions.
	tk.MustExec("set @@tidb_opt_range_max_size=0")
	tk.MustQuery("explain format='brief' select /*+ use_index(t4, idx_a_b) */ * from t4 where a in (1, 3, 5) and b = 2").Check(testkit.Rows(
		"IndexLookUp 0.30 root  ",
		"├─IndexRangeScan(Build) 0.30 cop[tikv] table:t4, index:idx_a_b(a, b) range:[1 2,1 2], [3 2,3 2], [5 2,5 2], keep order:false, stats:pseudo",
		"└─TableRowIDScan(Probe) 0.30 cop[tikv] table:t4 keep order:false, stats:pseudo"))
	tk.MustExec("set @@tidb_opt_range_max_size=1000")
	tk.MustQuery("explain format='brief' select /*+ use_index(t4, idx_a_b) */ * from t4 where a in (1, 3, 5) and b = 2").Check(testkit.Rows(
		"IndexLookUp 0.03 root  ",
		"├─Selection(Build) 0.03 cop[tikv]  eq(test.t4.b, 2)",
		"│ └─IndexRangeScan 30.00 cop[tikv] table:t4, index:idx_a_b(a, b) range:[1,1], [3,3], [5,5], keep order:false, stats:pseudo",
		"└─TableRowIDScan(Probe) 0.03 cop[tikv] table:t4 keep order:false, stats:pseudo"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 1000 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))
}

func TestPlanCacheForTableRangeFallback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("set @@tidb_enable_prepared_plan_cache=1")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int primary key, b int)")
	tk.MustExec("set @@tidb_opt_range_max_size=10")
	tk.MustExec("prepare stmt from 'select * from t where a in (?, ?, ?, ?, ?) and b > 1'")
	tk.MustExec("set @a=10, @b=20, @c=30, @d=40, @e=50")
	tk.MustExec("execute stmt using @a, @b, @c, @d, @e")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows("Warning 1105 Memory capacity of 10 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen",
		"Warning 1105 skip plan-cache: in-list is too long"))
	tk.MustExec("execute stmt using @a, @b, @c, @d, @e")
	// The plan with range fallback is not cached.
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestPlanCacheForIndexRangeFallback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0") // In this way `explain for connection id` doesn't display execution info.
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(10), b varchar(10), c varchar(10), index idx_a_b(a, b))")
	tk.MustExec("set @@tidb_opt_range_max_size=1330") // 1330 is the memory usage of ["aa","aa"], ["bb","bb"], ["cc","cc"], ["dd","dd"], ["ee","ee"].
	rows := tk.MustQuery("explain format='brief' select * from t where a in ('aa', 'bb', 'cc', 'dd', 'ee')").Rows()
	require.True(t, strings.Contains(rows[1][0].(string), "IndexRangeScan"))
	require.True(t, strings.Contains(rows[1][4].(string), "range:[\"aa\",\"aa\"], [\"bb\",\"bb\"], [\"cc\",\"cc\"], [\"dd\",\"dd\"], [\"ee\",\"ee\"]"))
	rows = tk.MustQuery("explain format='brief' select * from t where a in ('aaaaaaaaaa', 'bbbbbbbbbb', 'cccccccccc', 'dddddddddd', 'eeeeeeeeee')").Rows()
	// 1330 is not enough for ["aaaaaaaaaa","aaaaaaaaaa"], ["bbbbbbbbbb","bbbbbbbbbb"], ["cccccccccc","cccccccccc"], ["dddddddddd","dddddddddd"], ["eeeeeeeeee","eeeeeeeeee"].
	// So it falls back to table full scan.
	require.True(t, strings.Contains(rows[2][0].(string), "TableFullScan"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 1330 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))

	// Test rebuilding ranges for the cached plan doesn't have memory limit.
	tk.MustExec("prepare stmt1 from 'select * from t where a in (?, ?, ?, ?, ?)'")
	tk.MustExec("set @a='aa', @b='bb', @c='cc', @d='dd', @e='ee'")
	tk.MustExec("execute stmt1 using @a, @b, @c, @d, @e")
	tk.MustQuery("show warnings").Check(testkit.Rows()) // Range fallback doesn't happen and the plan can be put into cache.
	tk.MustExec("set @a='aaaaaaaaaa', @b='bbbbbbbbbb', @c='cccccccccc', @d='dddddddddd', @e='eeeeeeeeee'")
	tk.MustExec("execute stmt1 using @a, @b, @c, @d, @e")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("execute stmt1 using @a, @b, @c, @d, @e")
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	// We don't limit range mem usage when rebuilding ranges for the cached plan.
	// So ["aaaaaaaaaa","aaaaaaaaaa"], ["bbbbbbbbbb","bbbbbbbbbb"], ["cccccccccc","cccccccccc"], ["dddddddddd","dddddddddd"], ["eeeeeeeeee","eeeeeeeeee"] can still be built even if its mem usage exceeds 1330.
	require.True(t, strings.Contains(rows[2][0].(string), "IndexRangeScan"))
	require.True(t, strings.Contains(rows[2][4].(string), "range:[\"aaaaaaaaaa\",\"aaaaaaaaaa\"], [\"bbbbbbbbbb\",\"bbbbbbbbbb\"], [\"cccccccccc\",\"cccccccccc\"], [\"dddddddddd\",\"dddddddddd\"], [\"eeeeeeeeee\",\"eeeeeeeeee\"]"))

	// Test the plan with range fallback would not be put into cache.
	tk.MustExec("prepare stmt2 from 'select * from t where a in (?, ?, ?, ?, ?) and b in (?, ?, ?, ?, ?)'")
	tk.MustExec("set @a='aa', @b='bb', @c='cc', @d='dd', @e='ee', @f='ff', @g='gg', @h='hh', @i='ii', @j='jj'")
	tk.MustExec("execute stmt2 using @a, @b, @c, @d, @e, @f, @g, @h, @i, @j")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows("Warning 1105 Memory capacity of 1330 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen",
		"Warning 1105 skip plan-cache: in-list is too long"))
	tk.MustExec("execute stmt2 using @a, @b, @c, @d, @e, @f, @g, @h, @i, @j")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

func TestCorColRangeWithRangeMaxSize(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2, t3")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("create table t2 (a int, b int, c int, index idx_a_b(a, b))")
	tk.MustExec("create table t3(a int primary key)")
	tk.MustExec("insert into t1 values (2), (4), (6)")
	tk.MustExec("insert into t2 (a, b) values (1, 2), (3, 2), (5, 2)")
	tk.MustExec("insert into t3 values (2), (4)")
	tk.MustExec("insert into mysql.opt_rule_blacklist value(\"decorrelate\")")
	tk.MustExec("admin reload opt_rule_blacklist")
	defer func() {
		tk.MustExec("delete from mysql.opt_rule_blacklist where name = \"decorrelate\"")
		tk.MustExec("admin reload opt_rule_blacklist")
	}()

	// Correlated column in index range.
	tk.MustExec("set @@tidb_opt_range_max_size=1000")
	rows := tk.MustQuery("explain format='brief' select * from t1 where exists (select * from t2 where t2.a in (1, 3, 5) and b >= 2 and t2.b = t1.a)").Rows()
	// 1000 is not enough for [1 2,1 +inf], [3 2,3 +inf], [5 2,5 +inf]. So b >= 2 is not used to build ranges.
	require.True(t, strings.Contains(rows[4][0].(string), "Selection"))
	require.True(t, strings.Contains(rows[4][4].(string), "ge(test.t2.b, 2)"))
	// 1000 is not enough for [1 ?,1 ?], [3 ?,3 ?], [5 ?,5 ?] but we don't restrict range mem usage when appending col = cor_col
	// conditions to access conditions in SplitCorColAccessCondFromFilters.
	require.True(t, strings.Contains(rows[5][0].(string), "IndexRangeScan"))
	require.True(t, strings.Contains(rows[5][4].(string), "range: decided by [in(test.t2.a, 1, 3, 5) eq(test.t2.b, test.t1.a)]"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 1000 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))
	// We need to rebuild index ranges each time the value of correlated column test.t1.a changes. We don't restrict range
	// mem usage when rebuilding index ranges, otherwise range fallback would happen when rebuilding index ranges, causing
	// to wrong query results.
	tk.MustQuery("select * from t1 where exists (select * from t2 where t2.a in (1, 3, 5) and b >= 2 and t2.b = t1.a)").Check(testkit.Rows("2"))

	// Correlated column in table range.
	tk.MustExec("set @@tidb_opt_range_max_size=1")
	rows = tk.MustQuery("explain format='brief' select * from t1 where exists (select * from t3 where t3.a = t1.a)").Rows()
	// 1 is not enough for [?,?] but we don't restrict range mem usage when adding col = cor_col to access conditions.
	require.True(t, strings.Contains(rows[4][0].(string), "TableRangeScan"))
	require.True(t, strings.Contains(rows[4][4].(string), "range: decided by [eq(test.t3.a, test.t1.a)]"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	// We need to rebuild table ranges each time the value of correlated column test.t1.a changes. We don't restrict range
	// mem usage when rebuilding table ranges, otherwise range fallback would happen when rebuilding table ranges, causing
	// to wrong query results.
	tk.MustQuery("select * from t1 where exists (select * from t3 where t3.a = t1.a)").Check(testkit.Rows("2", "4"))
}

func TestIssue37760(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key)")
	tk.MustExec("insert into t values (2), (4), (6)")
	tk.MustExec("set @@tidb_opt_range_max_size=1")
	tk.MustQuery("select * from t where a").Check(testkit.Rows("2", "4", "6"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 1 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))
}

// TestExplainAnalyzeDMLCommit covers the issue #37373.
func TestExplainAnalyzeDMLCommit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int key, c2 int);")
	tk.MustExec("insert into t values (1, 1)")

	err := failpoint.Enable("github.com/pingcap/tidb/session/mockSleepBeforeTxnCommit", "return(500)")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/session/mockSleepBeforeTxnCommit")
	}()
	// The commit is paused by the failpoint, after the fix the explain statement
	// execution should proceed after the commit finishes.
	_, err = tk.Exec("explain analyze delete from t;")
	require.NoError(t, err)
	tk.MustQuery("select * from t").Check(testkit.Rows())
}

func TestPlanCacheForIndexJoinRangeFallback(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`set @@tidb_enable_prepared_plan_cache=1`)
	tk.MustExec("set @@tidb_enable_collect_execution_info=0")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b varchar(10), c varchar(10), index idx_a_b(a, b))")
	tk.MustExec("create table t2(d int)")
	tk.MustExec("set @@tidb_opt_range_max_size=1275")
	// 1275 is enough for [? a,? a], [? b,? b], [? c,? c] but is not enough for [? aaaaaa,? aaaaaa], [? bbbbbb,? bbbbbb], [? cccccc,? cccccc].
	rows := tk.MustQuery("explain format='brief' select /*+ inl_join(t1) */ * from  t1 join t2 on t1.a = t2.d where t1.b in ('a', 'b', 'c')").Rows()
	require.True(t, strings.Contains(rows[6][4].(string), "range: decided by [eq(test.t1.a, test.t2.d) in(test.t1.b, a, b, c)]"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	rows = tk.MustQuery("explain format='brief' select /*+ inl_join(t1) */ * from  t1 join t2 on t1.a = t2.d where t1.b in ('aaaaaa', 'bbbbbb', 'cccccc');").Rows()
	require.True(t, strings.Contains(rows[6][4].(string), "range: decided by [eq(test.t1.a, test.t2.d)]"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 1275 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))

	tk.MustExec("prepare stmt1 from 'select /*+ inl_join(t1) */ * from  t1 join t2 on t1.a = t2.d where t1.b in (?, ?, ?)'")
	tk.MustExec("set @a='a', @b='b', @c='c'")
	tk.MustExec("execute stmt1 using @a, @b, @c")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustExec("set @a='aaaaaa', @b='bbbbbb', @c='cccccc'")
	tk.MustExec("execute stmt1 using @a, @b, @c")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
	tk.MustExec("execute stmt1 using @a, @b, @c")
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	rows = tk.MustQuery(fmt.Sprintf("explain for connection %d", tkProcess.ID)).Rows()
	// We don't limit range mem usage when rebuilding index join ranges for the cached plan. So [? aaaaaa,? aaaaaa], [? bbbbbb,? bbbbbb], [? cccccc,? cccccc] can be built.
	require.True(t, strings.Contains(rows[6][4].(string), "range: decided by [eq(test.t1.a, test.t2.d) in(test.t1.b, aaaaaa, bbbbbb, cccccc)]"))

	// Test the plan with range fallback would not be put into cache.
	tk.MustExec("prepare stmt2 from 'select /*+ inl_join(t1) */ * from  t1 join t2 on t1.a = t2.d where t1.b in (?, ?, ?, ?, ?)'")
	tk.MustExec("set @a='a', @b='b', @c='c', @d='d', @e='e'")
	tk.MustExec("execute stmt2 using @a, @b, @c, @d, @e")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows("Warning 1105 Memory capacity of 1275 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen",
		"Warning 1105 skip plan-cache: in-list is too long"))
	tk.MustExec("execute stmt2 using @a, @b, @c, @d, @e")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
}

// https://github.com/pingcap/tidb/issues/38295.
func TestIssue38295(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t0(c0 BLOB(298) , c1 BLOB(182) , c2 NUMERIC);")
	tk.MustExec("CREATE VIEW v0(c0) AS SELECT t0.c1 FROM t0;")
	tk.MustExec("INSERT INTO t0 VALUES (-1, 'a', '2046549365');")
	tk.MustExec("CREATE INDEX i0 ON t0(c2);")
	tk.MustGetErrCode("SELECT t0.c1, t0.c2 FROM t0 GROUP BY MOD(t0.c0, DEFAULT(t0.c2));", errno.ErrFieldNotInGroupBy)
	tk.MustExec("UPDATE t0 SET c2=1413;")
}

func TestOuterJoinEliminationForIssue18216(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (a int, c int);")
	tk.MustExec("insert into t1 values (1, 1), (1, 2), (2, 3), (2, 4);")
	tk.MustExec("create table t2 (a int, c int);")
	tk.MustExec("insert into t2 values (1, 1), (1, 2), (2, 3), (2, 4);")
	// The output might be unstable.
	tk.MustExec("select group_concat(c order by (select group_concat(c order by a) from t2 where a=t1.a)) from t1; ")
	tk.MustQuery("select group_concat(c order by (select group_concat(c order by c) from t2 where a=t1.a), c desc) from t1;").Check(testkit.Rows("2,1,4,3"))
}

func TestAutoIncrementCheckWithCheckConstraint(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t (
		id INTEGER NOT NULL AUTO_INCREMENT,
		CHECK (id IN (0, 1)),
		KEY idx_autoinc_id (id)
	)`)
}

// https://github.com/pingcap/tidb/issues/36888.
func TestIssue36888(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t0(c0 INT);")
	tk.MustExec("CREATE TABLE t1(c0 INT);")

	tk.MustExec("INSERT INTO t0 VALUES (NULL);")
	tk.MustQuery("SELECT t0.c0 FROM t0 LEFT JOIN t1 ON t0.c0>=t1.c0 WHERE (CONCAT_WS(t0.c0, t1.c0) IS NULL);").Check(testkit.Rows("<nil>"))
}

// https://github.com/pingcap/tidb/issues/40285.
func TestIssue40285(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t(col1 enum('p5', '9a33x') NOT NULL DEFAULT 'p5',col2 tinyblob DEFAULT NULL) ENGINE = InnoDB DEFAULT CHARSET = latin1 COLLATE = latin1_bin;")
	tk.MustQuery("(select last_value(col1) over () as r0 from t) union all (select col2 as r0 from t);")
}

// https://github.com/pingcap/tidb/issues/41273
func TestIssue41273(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t (
    	a set('nwbk','r5','1ad3u','van','ir1z','y','9m','f1','z','e6yd','wfev') NOT NULL DEFAULT 'ir1z,f1,e6yd',
    	b enum('soo2','4s4j','qi9om','8ue','i71o','qon','3','3feh','6o1i','5yebx','d') NOT NULL DEFAULT '8ue',
    	c varchar(66) DEFAULT '13mdezixgcn',
    	PRIMARY KEY (a,b) /*T![clustered_index] CLUSTERED */,
    	UNIQUE KEY ib(b),
    	KEY ia(a)
    )ENGINE=InnoDB DEFAULT CHARSET=ascii COLLATE=ascii_bin;`)
	tk.MustExec("INSERT INTO t VALUES('ir1z,f1,e6yd','i71o','13mdezixgcn'),('ir1z,f1,e6yd','d','13mdezixgcn'),('nwbk','8ue','13mdezixgcn');")
	expectedRes := []string{"ir1z,f1,e6yd d 13mdezixgcn", "ir1z,f1,e6yd i71o 13mdezixgcn", "nwbk 8ue 13mdezixgcn"}
	tk.MustQuery("select * from t where a between 'e6yd' and 'z' or b <> '8ue';").Sort().Check(testkit.Rows(expectedRes...))
	tk.MustQuery("select /*+ use_index_merge(t) */ * from t where a between 'e6yd' and 'z' or b <> '8ue';").Sort().Check(testkit.Rows(expectedRes...))
	// For now tidb doesn't support push set type to TiKV, and column a is a set type, so we shouldn't generate a IndexMerge path.
	require.False(t, tk.HasPlanForLastExecution("IndexMerge"))
}

func TestIsIPv4ToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(v4 varchar(100), v6 varchar(100))")
	tk.MustExec("insert into t values('123.123.123.123', 'F746:C349:48E3:22F2:81E0:0EA8:E7B6:8286')")
	tk.MustExec("insert into t values('0.0.0.0', '0000:0000:0000:0000:0000:0000:0000:0000')")
	tk.MustExec("insert into t values('127.0.0.1', '2001:0:2851:b9f0:6d:2326:9036:f37a')")

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "is_ipv4(test.t.v4)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select is_ipv4(v4) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestIsIPv6ToTiFlash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(v4 varchar(100), v6 varchar(100))")
	tk.MustExec("insert into t values('123.123.123.123', 'F746:C349:48E3:22F2:81E0:0EA8:E7B6:8286')")
	tk.MustExec("insert into t values('0.0.0.0', '0000:0000:0000:0000:0000:0000:0000:0000')")
	tk.MustExec("insert into t values('127.0.0.1', '2001:0:2851:b9f0:6d:2326:9036:f37a')")

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]interface{}{
		{"TableReader_9", "root", "MppVersion: 1, data:ExchangeSender_8"},
		{"└─ExchangeSender_8", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "is_ipv6(test.t.v6)->Column#4"},
		{"    └─TableFullScan_7", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select is_ipv6(v6) from t;").CheckAt([]int{0, 2, 4}, rows)
}

// https://github.com/pingcap/tidb/issues/41355
// The "virtual generated column" push down is not supported now.
// This test covers: TopN, Projection, Selection.
func TestVirtualExprPushDown(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE t (c1 int DEFAULT 0, c2 int GENERATED ALWAYS AS (abs(c1)) VIRTUAL);")
	tk.MustExec("insert into t(c1) values(1), (-1), (2), (-2), (99), (-99);")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tikv'")

	// TopN to tikv.
	rows := [][]interface{}{
		{"TopN_7", "root", "test.t.c2, offset:0, count:2"},
		{"└─TableReader_13", "root", "data:TableFullScan_12"},
		{"  └─TableFullScan_12", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select * from t order by c2 limit 2;").CheckAt([]int{0, 2, 4}, rows)

	// Projection to tikv.
	rows = [][]interface{}{
		{"Projection_3", "root", "plus(test.t.c1, test.t.c2)->Column#4"},
		{"└─TableReader_5", "root", "data:TableFullScan_4"},
		{"  └─TableFullScan_4", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	tk.MustExec("set session tidb_opt_projection_push_down='ON';")
	tk.MustQuery("explain select c1 + c2 from t;").CheckAt([]int{0, 2, 4}, rows)
	tk.MustExec("set session tidb_opt_projection_push_down='OFF';")

	// Selection to tikv.
	rows = [][]interface{}{
		{"Selection_7", "root", "gt(test.t.c2, 1)"},
		{"└─TableReader_6", "root", "data:TableFullScan_5"},
		{"  └─TableFullScan_5", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select * from t where c2 > 1;").CheckAt([]int{0, 2, 4}, rows)

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tblInfo := range db.Tables {
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	// TopN to tiflash.
	rows = [][]interface{}{
		{"TopN_7", "root", "test.t.c2, offset:0, count:2"},
		{"└─TableReader_15", "root", "data:TableFullScan_14"},
		{"  └─TableFullScan_14", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select * from t order by c2 limit 2;").CheckAt([]int{0, 2, 4}, rows)

	// Projection to tiflash.
	rows = [][]interface{}{
		{"Projection_3", "root", "plus(test.t.c1, test.t.c2)->Column#4"},
		{"└─TableReader_6", "root", "data:TableFullScan_5"},
		{"  └─TableFullScan_5", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustExec("set session tidb_opt_projection_push_down='ON';")
	tk.MustQuery("explain select c1 + c2 from t;").CheckAt([]int{0, 2, 4}, rows)
	tk.MustExec("set session tidb_opt_projection_push_down='OFF';")

	// Selection to tiflash.
	rows = [][]interface{}{
		{"Selection_8", "root", "gt(test.t.c2, 1)"},
		{"└─TableReader_7", "root", "data:TableFullScan_6"},
		{"  └─TableFullScan_6", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select * from t where c2 > 1;").CheckAt([]int{0, 2, 4}, rows)
}

// https://github.com/pingcap/tidb/issues/41458
func TestIssue41458(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("use test")
	tk.MustExec(`create table t (a int, b int, c int, index ia(a));`)
	tk.MustExec("select  * from t t1 join t t2 on t1.b = t2.b join t t3 on t2.b=t3.b join t t4 on t3.b=t4.b where t3.a=1 and t2.a=2;")
	rawRows := tk.MustQuery("select plan from information_schema.statements_summary where SCHEMA_NAME = 'test' and STMT_TYPE = 'Select';").Sort().Rows()
	plan := rawRows[0][0].(string)
	rows := strings.Split(plan, "\n")
	rows = rows[1:]
	expectedRes := []string{
		"Projection",
		"└─HashJoin",
		"  ├─HashJoin",
		"  │ ├─HashJoin",
		"  │ │ ├─IndexLookUp",
		"  │ │ │ ├─IndexRangeScan",
		"  │ │ │ └─Selection",
		"  │ │ │   └─TableRowIDScan",
		"  │ │ └─IndexLookUp",
		"  │ │   ├─IndexRangeScan",
		"  │ │   └─Selection",
		"  │ │     └─TableRowIDScan",
		"  │ └─TableReader",
		"  │   └─Selection",
		"  │     └─TableFullScan",
		"  └─TableReader",
		"    └─Selection",
		"      └─TableFullScan",
	}
	for i, row := range rows {
		fields := strings.Split(row, "\t")
		fields = strings.Split(fields[1], "_")
		op := fields[0]
		require.Equalf(t, expectedRes[i], op, fmt.Sprintf("Mismatch at index %d.", i))
	}
}
