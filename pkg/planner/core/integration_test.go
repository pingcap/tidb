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

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

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
	require.EqualError(t, err, "[planner:1815]Internal : No access path for table 't' is found with 'tidb_isolation_read_engines' = 'tiflash', valid values can be 'tikv'. Please check tiflash replica.")

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash, tikv'")
	tk.MustExec("select * from t")
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.IsolationRead.Engines = []string{"tiflash"}
	})
	// Change instance config doesn't affect isolation read.
	tk.MustExec("select * from t")
}

func TestAggPushDownEngine(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int primary key, b varchar(20))")
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
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

func TestIssue15110And49616(t *testing.T) {
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "crm_rd_150m" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
	tk.MustExec("explain format = 'brief' SELECT count(*) FROM crm_rd_150m dataset_48 WHERE (CASE WHEN (month(dataset_48.customer_first_date)) <= 30 THEN '新客' ELSE NULL END) IS NOT NULL;")

	// for #49616
	tk.MustExec(`use test`)
	tk.MustExec("set @@session.tidb_isolation_read_engines = 'tikv'")
	tk.MustExec(`create table t1 (k int, a int)`)
	tk.MustExec(`create table t2 (k int, b int, key(k))`)
	tk.MustHavePlan(`select /*+ tidb_inlj(t2, t1) */ *
  from t2 left join t1 on t1.k=t2.k
  where a>0 or (a=0 and b>0)`, `IndexJoin`)
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
	query, err := expression.ParseSimpleExpr(tk.Session().GetExprCtx(), "a = '2020-01-01 00:00:00'", expression.WithTableInfo("", tbl.Meta()))
	require.NoError(t, err)
	dbName := model.NewCIStr(tk.Session().GetSessionVars().CurrentDB)
	columns, names, err := expression.ColumnInfos2ColumnsAndNames(tk.Session().GetExprCtx(), dbName, tbl.Meta().Name, tbl.Meta().Cols(), tbl.Meta())
	require.NoError(t, err)
	// Even the partition is not monotonous, EQ condition should be prune!
	// select * from t where a = '2020-01-01 00:00:00'
	res, err := core.PartitionPruning(tk.Session().GetPlanCtx(), pt, []expression.Expression{query}, nil, columns, names)
	require.NoError(t, err)
	require.Len(t, res, 1)
	require.Equal(t, 0, res[0])
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	err := tk.ExecToErr("select * from t for update")
	require.EqualError(t, err, `[planner:1815]Internal : No access path for table 't' is found with 'tidb_isolation_read_engines' = 'tiflash', valid values can be 'tiflash, tikv'. Please check tiflash replica or check if the query is not readonly and sql mode is strict.`)

	err = tk.ExecToErr("insert into t select * from t")
	require.EqualError(t, err, `[planner:1815]Internal : No access path for table 't' is found with 'tidb_isolation_read_engines' = 'tiflash', valid values can be 'tiflash, tikv'. Please check tiflash replica or check if the query is not readonly and sql mode is strict.`)

	tk.MustExec("prepare stmt_insert from 'insert into t select * from t where t.a = ?'")
	tk.MustExec("set @a=1")
	err = tk.ExecToErr("execute stmt_insert using @a")
	require.EqualError(t, err, `[planner:1815]Internal : No access path for table 't' is found with 'tidb_isolation_read_engines' = 'tiflash', valid values can be 'tiflash, tikv'. Please check tiflash replica or check if the query is not readonly and sql mode is strict.`)
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "10000.00", "root", " MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "10000.00", "mpp[tiflash]", " ExchangeType: PassThrough"},
		{"  └─Projection_4", "10000.00", "mpp[tiflash]", " time_to_sec(test.t.a)->Column#3"},
		{"    └─TableFullScan_8", "10000.00", "mpp[tiflash]", "table:t", "keep order:false, stats:pseudo"},
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "rightshift(test.t.a, test.t.b)->Column#4"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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
	rows := [][]any{
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
	rows = [][]any{
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
	rows = [][]any{
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
	rows = [][]any{
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
	rows := [][]any{
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/expression/injectNow", fmt.Sprintf(`return(%d)`, now.Unix())))
	rows[1][2] = fmt.Sprintf("gt(test.t.d, %v)", now.Format(time.DateTime))
	tk.MustQuery("explain analyze select /*+read_from_storage(tikv[t])*/ * from t where d > sysdate()").
		CheckAt([]int{0, 3, 6}, rows)
	failpoint.Disable("github.com/pingcap/tidb/pkg/expression/injectNow")

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
	rows := [][]any{
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "reverse(test.t.a)->Column#3"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "reverse(test.t.a)->Column#3"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "space(test.t.a)->Column#3"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}

	tk.MustQuery("explain select space(a) from t;").CheckAt([]int{0, 2, 4}, rows)
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

func TestConflictReadFromStorage(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/pkg/planner/core/forceDynamicPrune")
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
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
		"TableReader 10000.00 root  MppVersion: 2, data:ExchangeSender",
		"└─ExchangeSender 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─TableFullScan 10000.00 mpp[tiflash] table:t31202 keep order:false, stats:pseudo"))

	tk.MustQuery("explain format = 'brief' select * from t31202 use index (primary);").Check(testkit.Rows(
		"TableReader 10000.00 root  data:TableFullScan",
		"└─TableFullScan 10000.00 cop[tikv] table:t31202 keep order:false, stats:pseudo"))
	tk.MustExec("drop table if exists t31202")
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

	require.Eventually(t, func() bool {
		tk.MustQuery("select /*+AGG_TO_COP()*/ count(*) from t32157 ignore index(primary) where process_code = 'GDEP0071'").Check(testkit.Rows("2"))
		return tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache
	}, 10*time.Second, 500*time.Millisecond)

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

	getStreamCountFromExplain := func(rows [][]any) (res []uint64) {
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

func TestIssue37986(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`drop table if exists t3`)
	tk.MustExec(`CREATE TABLE t3(c0 INT, primary key(c0))`)
	tk.MustExec(`insert into t3 values(1), (2), (3), (4), (5), (6), (7), (8), (9), (10)`)
	rs := tk.MustQuery(`SELECT v2.c0 FROM (select rand() as c0 from t3) v2 order by v2.c0 limit 10`).Rows()
	lastVal := -1.0
	for _, r := range rs {
		v := r[0].(string)
		val, err := strconv.ParseFloat(v, 64)
		require.NoError(t, err)
		require.True(t, val >= lastVal)
		lastVal = val
	}

	tk.MustQuery(`explain format='brief' SELECT v2.c0 FROM (select rand() as c0 from t3) v2 order by v2.c0 limit 10`).
		Check(testkit.Rows(`TopN 10.00 root  Column#2, offset:0, count:10`,
			`└─Projection 10000.00 root  rand()->Column#2`,
			`  └─TableReader 10000.00 root  data:TableFullScan`,
			`    └─TableFullScan 10000.00 cop[tikv] table:t3 keep order:false, stats:pseudo`))
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "repeat(cast(test.t.a, var_string(20)), test.t.b)->Column#4"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select repeat(a,b) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestIssue50235(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table tt (c year(4) NOT NULL DEFAULT '2016', primary key(c));`)
	tk.MustExec(`insert into tt values (2016);`)
	tk.MustQuery(`select * from tt where c < 16212511333665770580`).Check(testkit.Rows("2016"))
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}
	tk.MustQuery("explain format = 'brief' select /*+ read_from_storage(tiflash[t]) */ * from t where a + 1 > 20 limit 100;;").Check(testkit.Rows(
		"Limit 100.00 root  offset:0, count:100",
		"└─TableReader 100.00 root  MppVersion: 2, data:ExchangeSender",
		"  └─ExchangeSender 100.00 mpp[tiflash]  ExchangeType: PassThrough",
		"    └─Limit 100.00 mpp[tiflash]  offset:0, count:100",
		"      └─Selection 100.00 mpp[tiflash]  gt(plus(test.t.a, 1), 20)",
		"        └─TableFullScan 125.00 mpp[tiflash] table:t pushed down filter:empty, keep order:false, stats:pseudo"))
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
		"TableReader 10000.00 root  MppVersion: 2, data:ExchangeSender",
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
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	// Create virtual tiflash replica info.
	dom := domain.GetDomain(tk.Session())
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"HashAgg_6", "root", "funcs:avg(Column#4)->Column#3"},
		{"└─Projection_19", "root", "cast(test.t.a, double BINARY)->Column#4"},
		{"  └─TableReader_12", "root", "data:TableFullScan_11"},
		{"    └─TableFullScan_11", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select avg(a) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]any{
		{"HashAgg_6", "root", "funcs:sum(Column#4)->Column#3"},
		{"└─Projection_19", "root", "cast(test.t.a, double BINARY)->Column#4"},
		{"  └─TableReader_12", "root", "data:TableFullScan_11"},
		{"    └─TableFullScan_11", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select sum(a) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]any{
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "leftshift(test.t.a, test.t.b)->Column#4"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select a << b from t;").CheckAt([]int{0, 2, 4}, rows)
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

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "hex(test.t.a)->Column#4"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select hex(a) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "hex(test.t.b)->Column#4"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "bin(test.t.a)->Column#3"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "elt(test.t.a, test.t.b)->Column#4"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "regexp_instr(test.t.expr, test.t.pattern, 1, 1, 0, test.t.match_type)->Column#8"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "regexp_substr(test.t.expr, test.t.pattern, 1, 1, test.t.match_type)->Column#7"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "regexp_replace(test.t.expr, test.t.pattern, test.t.repl, 1, 1, test.t.match_type)->Column#8"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "cast(test.t.a, time BINARY)->Column#4, cast(test.t.b, time BINARY)->Column#5"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "unhex(cast(test.t.a, var_string(20)))->Column#4"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select unhex(a) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "unhex(test.t.b)->Column#4"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "least(test.t.a, test.t.b)->Column#4"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select least(a, b) from t;").CheckAt([]int{0, 2, 4}, rows)

	rows = [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "greatest(test.t.a, test.t.b)->Column#4"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select greatest(a, b) from t;").CheckAt([]int{0, 2, 4}, rows)
}

func TestTiFlashReadForWriteStmt(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("insert into t values(1, 2)")
	tk.MustExec("drop table if exists t2")
	tk.MustExec("create table t2(a int)")
	tk.MustExec("set @@tidb_allow_mpp=1")

	// Default should be 1
	tk.MustQuery("select @@tidb_enable_tiflash_read_for_write_stmt").Check(testkit.Rows("1"))
	// Set ON
	tk.MustExec("set @@tidb_enable_tiflash_read_for_write_stmt = ON")
	tk.MustQuery("show warnings").Check(testkit.Rows())
	tk.MustQuery("select @@tidb_enable_tiflash_read_for_write_stmt").Check(testkit.Rows("1"))
	// Set OFF
	tk.MustExec("set @@tidb_enable_tiflash_read_for_write_stmt = OFF")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 tidb_enable_tiflash_read_for_write_stmt is always turned on. This variable has been deprecated and will be removed in the future releases"))
	tk.MustQuery("select @@tidb_enable_tiflash_read_for_write_stmt").Check(testkit.Rows("1"))

	tbl, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t", L: "t"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	tbl2, err := dom.InfoSchema().TableByName(model.CIStr{O: "test", L: "test"}, model.CIStr{O: "t2", L: "t2"})
	require.NoError(t, err)
	// Set the hacked TiFlash replica for explain tests.
	tbl2.Meta().TiFlashReplica = &model.TiFlashReplicaInfo{Count: 1, Available: true}

	checkRes := func(r [][]any, pos int, expected string) {
		check := false
		for i := range r {
			if r[i][pos] == expected {
				check = true
				break
			}
		}
		require.Equal(t, check, true)
	}

	check := func(query string) {
		// If sql mode is strict, read does not push down to tiflash
		tk.MustExec("set @@sql_mode = 'strict_trans_tables'")
		tk.MustExec("set @@tidb_enforce_mpp=0")
		rs := tk.MustQuery(query).Rows()
		checkRes(rs, 2, "cop[tikv]")
		tk.MustQuery("show warnings").Check(testkit.Rows())

		// If sql mode is strict and tidb_enforce_mpp is on, read does not push down to tiflash
		// and should return a warning.
		tk.MustExec("set @@tidb_enforce_mpp=1")
		rs = tk.MustQuery(query).Rows()
		checkRes(rs, 2, "cop[tikv]")
		rs = tk.MustQuery("show warnings").Rows()
		checkRes(rs, 2, "MPP mode may be blocked because the query is not readonly and sql mode is strict.")

		// If sql mode is not strict, read should push down to tiflash
		tk.MustExec("set @@sql_mode = ''")
		rs = tk.MustQuery(query).Rows()
		checkRes(rs, 2, "mpp[tiflash]")
		tk.MustQuery("show warnings").Check(testkit.Rows())
	}

	// Insert into ... select
	check("explain insert into t2 select a+b from t")
	check("explain insert into t2 select t.a from t2 join t on t2.a = t.a")

	// Replace into ... select
	check("explain replace into t2 select a+b from t")

	// CTE
	check("explain update t set a=a+1 where b in (select a from t2 where t.a > t2.a)")
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
	tk.MustExec("set @@sql_mode='';")
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
	require.True(t, strings.Contains(rows[1][0].(string), "IndexRangeScan"))
	require.True(t, strings.Contains(rows[1][4].(string), "range:[\"aaaaaaaaaa\",\"aaaaaaaaaa\"], [\"bbbbbbbbbb\",\"bbbbbbbbbb\"], [\"cccccccccc\",\"cccccccccc\"], [\"dddddddddd\",\"dddddddddd\"], [\"eeeeeeeeee\",\"eeeeeeeeee\"]"))

	// Test the plan with range fallback would not be put into cache.
	tk.MustExec("prepare stmt2 from 'select * from t where a in (?, ?, ?, ?, ?) and b in (?, ?, ?, ?, ?)'")
	tk.MustExec("set @a='aa', @b='bb', @c='cc', @d='dd', @e='ee', @f='ff', @g='gg', @h='hh', @i='ii', @j='jj'")
	tk.MustExec("execute stmt2 using @a, @b, @c, @d, @e, @f, @g, @h, @i, @j")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows("Warning 1105 Memory capacity of 1330 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen",
		"Warning 1105 skip prepared plan-cache: in-list is too long"))
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

// TestExplainAnalyzeDMLCommit covers the issue #37373.
func TestExplainAnalyzeDMLCommit(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (c1 int key, c2 int);")
	tk.MustExec("insert into t values (1, 1)")

	err := failpoint.Enable("github.com/pingcap/tidb/pkg/session/mockSleepBeforeTxnCommit", "return(500)")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tidb/pkg/session/mockSleepBeforeTxnCommit")
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
	tk.MustExec("set @@tidb_opt_range_max_size=1260")
	// 1260 is enough for [? a,? a], [? b,? b], [? c,? c] but is not enough for [? aaaaaa,? aaaaaa], [? bbbbbb,? bbbbbb], [? cccccc,? cccccc].
	rows := tk.MustQuery("explain format='brief' select /*+ inl_join(t1) */ * from  t1 join t2 on t1.a = t2.d where t1.b in ('a', 'b', 'c')").Rows()
	require.True(t, strings.Contains(rows[6][4].(string), "range: decided by [eq(test.t1.a, test.t2.d) in(test.t1.b, a, b, c)]"))
	tk.MustQuery("show warnings").Check(testkit.Rows())
	rows = tk.MustQuery("explain format='brief' select /*+ inl_join(t1) */ * from  t1 join t2 on t1.a = t2.d where t1.b in ('aaaaaa', 'bbbbbb', 'cccccc');").Rows()
	require.Contains(t, rows[6][4].(string), "range: decided by [eq(test.t1.a, test.t2.d)]")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 Memory capacity of 1260 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen"))

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
	require.Contains(t, rows[6][4].(string), "range: decided by [eq(test.t1.a, test.t2.d) in(test.t1.b, aaaaaa, bbbbbb, cccccc)]")

	// Test the plan with range fallback would not be put into cache.
	tk.MustExec("prepare stmt2 from 'select /*+ inl_join(t1) */ * from  t1 join t2 on t1.a = t2.d where t1.b in (?, ?, ?, ?, ?)'")
	tk.MustExec("set @a='a', @b='b', @c='c', @d='d', @e='e'")
	tk.MustExec("execute stmt2 using @a, @b, @c, @d, @e")
	tk.MustQuery("show warnings").Sort().Check(testkit.Rows("Warning 1105 Memory capacity of 1260 bytes for 'tidb_opt_range_max_size' exceeded when building ranges. Less accurate ranges such as full range are chosen",
		"Warning 1105 skip prepared plan-cache: in-list is too long"))
	tk.MustExec("execute stmt2 using @a, @b, @c, @d, @e")
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "is_ipv4(test.t.v4)->Column#4"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	rows := [][]any{
		{"TableReader_10", "root", "MppVersion: 2, data:ExchangeSender_9"},
		{"└─ExchangeSender_9", "mpp[tiflash]", "ExchangeType: PassThrough"},
		{"  └─Projection_4", "mpp[tiflash]", "is_ipv6(test.t.v6)->Column#4"},
		{"    └─TableFullScan_8", "mpp[tiflash]", "keep order:false, stats:pseudo"},
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
	tk.MustExec("set @@session.tidb_allow_tiflash_cop=ON")

	// TopN to tikv.
	rows := [][]any{
		{"TopN_7", "root", "test.t.c2, offset:0, count:2"},
		{"└─TableReader_13", "root", "data:TableFullScan_12"},
		{"  └─TableFullScan_12", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select * from t order by c2 limit 2;").CheckAt([]int{0, 2, 4}, rows)

	// Projection to tikv.
	rows = [][]any{
		{"Projection_3", "root", "plus(test.t.c1, test.t.c2)->Column#4"},
		{"└─TableReader_5", "root", "data:TableFullScan_4"},
		{"  └─TableFullScan_4", "cop[tikv]", "keep order:false, stats:pseudo"},
	}
	tk.MustExec("set session tidb_opt_projection_push_down='ON';")
	tk.MustQuery("explain select c1 + c2 from t;").CheckAt([]int{0, 2, 4}, rows)
	tk.MustExec("set session tidb_opt_projection_push_down='OFF';")

	// Selection to tikv.
	rows = [][]any{
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
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "t" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	// TopN to tiflash.
	rows = [][]any{
		{"TopN_7", "root", "test.t.c2, offset:0, count:2"},
		{"└─TableReader_15", "root", "data:TableFullScan_14"},
		{"  └─TableFullScan_14", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select * from t order by c2 limit 2;").CheckAt([]int{0, 2, 4}, rows)

	// Projection to tiflash.
	rows = [][]any{
		{"Projection_3", "root", "plus(test.t.c1, test.t.c2)->Column#4"},
		{"└─TableReader_6", "root", "data:TableFullScan_5"},
		{"  └─TableFullScan_5", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustExec("set session tidb_opt_projection_push_down='ON';")
	tk.MustQuery("explain select c1 + c2 from t;").CheckAt([]int{0, 2, 4}, rows)
	tk.MustExec("set session tidb_opt_projection_push_down='OFF';")

	// Selection to tiflash.
	rows = [][]any{
		{"Selection_8", "root", "gt(test.t.c2, 1)"},
		{"└─TableReader_7", "root", "data:TableFullScan_6"},
		{"  └─TableFullScan_6", "cop[tiflash]", "keep order:false, stats:pseudo"},
	}
	tk.MustQuery("explain select * from t where c2 > 1;").CheckAt([]int{0, 2, 4}, rows)
}

func TestWindowRangeFramePushDownTiflash(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists test.first_range;")
	tk.MustExec("create table test.first_range(p int not null, o int not null, v int not null, o_datetime datetime not null, o_time time not null);")
	tk.MustExec("insert into test.first_range (p, o, v, o_datetime, o_time) values (0, 0, 0, '2023-9-20 11:17:10', '11:17:10');")

	tk.MustExec("drop table if exists test.first_range_d64;")
	tk.MustExec("create table test.first_range_d64(p int not null, o decimal(17,1) not null, v int not null);")
	tk.MustExec("insert into test.first_range_d64 (p, o, v) values (0, 0.1, 0), (1, 1.0, 1), (1, 2.1, 2), (1, 4.1, 4), (1, 8.1, 8), (2, 0.0, 0), (2, 3.1, 3), (2, 10.0, 10), (2, 13.1, 13), (2, 15.1, 15), (3, 1.1, 1), (3, 2.9, 3), (3, 5.1, 5), (3, 9.1, 9), (3, 15.0, 15), (3, 20.1, 20), (3, 31.1, 31);")

	tk.MustExec("set @@tidb_allow_mpp=1; set @@tidb_enforce_mpp=1")
	tk.MustExec("set @@tidb_isolation_read_engines = 'tiflash'")

	// Create virtual tiflash replica info.
	is := dom.InfoSchema()
	db, exists := is.SchemaByName(model.NewCIStr("test"))
	require.True(t, exists)
	for _, tbl := range is.SchemaTables(db.Name) {
		tblInfo := tbl.Meta()
		if tblInfo.Name.L == "first_range" || tblInfo.Name.L == "first_range_d64" {
			tblInfo.TiFlashReplica = &model.TiFlashReplicaInfo{
				Count:     1,
				Available: true,
			}
		}
	}

	tk.MustExec(`set @@tidb_max_tiflash_threads=20`)

	tk.MustQuery("explain select *, first_value(v) over (partition by p order by o range between 3 preceding and 0 following) as a from test.first_range;").Check(testkit.Rows(
		"TableReader_23 10000.00 root  MppVersion: 2, data:ExchangeSender_22",
		"└─ExchangeSender_22 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─Window_21 10000.00 mpp[tiflash]  first_value(test.first_range.v)->Column#8 over(partition by test.first_range.p order by test.first_range.o range between 3 preceding and 0 following), stream_count: 20",
		"    └─Sort_13 10000.00 mpp[tiflash]  test.first_range.p, test.first_range.o, stream_count: 20",
		"      └─ExchangeReceiver_12 10000.00 mpp[tiflash]  stream_count: 20",
		"        └─ExchangeSender_11 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.first_range.p, collate: binary], stream_count: 20",
		"          └─TableFullScan_10 10000.00 mpp[tiflash] table:first_range keep order:false, stats:pseudo"))

	tk.MustQuery("explain select *, first_value(v) over (partition by p order by o range between 3 preceding and 2.9E0 following) as a from test.first_range;").Check(testkit.Rows(
		"TableReader_23 10000.00 root  MppVersion: 2, data:ExchangeSender_22",
		"└─ExchangeSender_22 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─Window_21 10000.00 mpp[tiflash]  first_value(test.first_range.v)->Column#8 over(partition by test.first_range.p order by test.first_range.o range between 3 preceding and 2.9 following), stream_count: 20",
		"    └─Sort_13 10000.00 mpp[tiflash]  test.first_range.p, test.first_range.o, stream_count: 20",
		"      └─ExchangeReceiver_12 10000.00 mpp[tiflash]  stream_count: 20",
		"        └─ExchangeSender_11 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.first_range.p, collate: binary], stream_count: 20",
		"          └─TableFullScan_10 10000.00 mpp[tiflash] table:first_range keep order:false, stats:pseudo"))

	tk.MustQuery("explain select *, first_value(v) over (partition by p order by o range between 2.3 preceding and 0 following) as a from test.first_range_d64;").Check(testkit.Rows(
		"TableReader_23 10000.00 root  MppVersion: 2, data:ExchangeSender_22",
		"└─ExchangeSender_22 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─Window_21 10000.00 mpp[tiflash]  first_value(test.first_range_d64.v)->Column#6 over(partition by test.first_range_d64.p order by test.first_range_d64.o range between 2.3 preceding and 0 following), stream_count: 20",
		"    └─Sort_13 10000.00 mpp[tiflash]  test.first_range_d64.p, test.first_range_d64.o, stream_count: 20",
		"      └─ExchangeReceiver_12 10000.00 mpp[tiflash]  stream_count: 20",
		"        └─ExchangeSender_11 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.first_range_d64.p, collate: binary], stream_count: 20",
		"          └─TableFullScan_10 10000.00 mpp[tiflash] table:first_range_d64 keep order:false, stats:pseudo"))

	tk.MustQuery("explain select *, first_value(v) over (partition by p order by o_datetime range between interval 1 day preceding and interval 1 day following) as a from test.first_range;").Check(testkit.Rows(
		"TableReader_23 10000.00 root  MppVersion: 2, data:ExchangeSender_22",
		"└─ExchangeSender_22 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"  └─Window_21 10000.00 mpp[tiflash]  first_value(test.first_range.v)->Column#8 over(partition by test.first_range.p order by test.first_range.o_datetime range between interval 1 \"DAY\" preceding and interval 1 \"DAY\" following), stream_count: 20",
		"    └─Sort_13 10000.00 mpp[tiflash]  test.first_range.p, test.first_range.o_datetime, stream_count: 20",
		"      └─ExchangeReceiver_12 10000.00 mpp[tiflash]  stream_count: 20",
		"        └─ExchangeSender_11 10000.00 mpp[tiflash]  ExchangeType: HashPartition, Compression: FAST, Hash Cols: [name: test.first_range.p, collate: binary], stream_count: 20",
		"          └─TableFullScan_10 10000.00 mpp[tiflash] table:first_range keep order:false, stats:pseudo"))

	tk.MustQuery("explain select *, first_value(v) over (partition by p order by o_time range between interval 1 day preceding and interval 1 day following) as a from test.first_range;").Check(testkit.Rows(
		"Shuffle_13 10000.00 root  execution info: concurrency:5, data sources:[TableReader_11]",
		"└─Window_8 10000.00 root  first_value(test.first_range.v)->Column#8 over(partition by test.first_range.p order by test.first_range.o_time range between interval 1 \"DAY\" preceding and interval 1 \"DAY\" following)",
		"  └─Sort_12 10000.00 root  test.first_range.p, test.first_range.o_time",
		"    └─ShuffleReceiver_14 10000.00 root  ",
		"      └─TableReader_11 10000.00 root  MppVersion: 2, data:ExchangeSender_10",
		"        └─ExchangeSender_10 10000.00 mpp[tiflash]  ExchangeType: PassThrough",
		"          └─TableFullScan_9 10000.00 mpp[tiflash] table:first_range keep order:false, stats:pseudo"))
}

func TestIssue46556(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`CREATE TABLE t0(c0 BLOB);`)
	tk.MustExec(`CREATE definer='root'@'localhost' VIEW v0(c0) AS SELECT NULL FROM t0 GROUP BY NULL;`)
	tk.MustExec(`SELECT t0.c0 FROM t0 NATURAL JOIN v0 WHERE v0.c0 LIKE v0.c0;`) // no error
	tk.MustQuery(`explain format='brief' SELECT t0.c0 FROM t0 NATURAL JOIN v0 WHERE v0.c0 LIKE v0.c0`).Check(
		testkit.Rows(`HashJoin 0.00 root  inner join, equal:[eq(Column#5, test.t0.c0)]`,
			`├─Projection(Build) 0.00 root  <nil>->Column#5`,
			`│ └─TableDual 0.00 root  rows:0`,
			`└─TableReader(Probe) 7992.00 root  data:Selection`,
			`  └─Selection 7992.00 cop[tikv]  like(test.t0.c0, test.t0.c0, 92), not(isnull(test.t0.c0))`,
			`    └─TableFullScan 10000.00 cop[tikv] table:t0 keep order:false, stats:pseudo`))
}

// https://github.com/pingcap/tidb/issues/41458
func TestIssue41458(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
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

func TestIssue48257(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	h := dom.StatsHandle()
	oriLease := h.Lease()
	h.SetLease(1)
	defer func() {
		h.SetLease(oriLease)
	}()
	tk.MustExec("use test")

	// 1. test sync load
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t value(1)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(dom.InfoSchema()))
	tk.MustExec("analyze table t")
	tk.MustQuery("explain format = brief select * from t").Check(testkit.Rows(
		"TableReader 1.00 root  data:TableFullScan",
		"└─TableFullScan 1.00 cop[tikv] table:t keep order:false",
	))
	tk.MustExec("insert into t value(1)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(dom.InfoSchema()))
	tk.MustQuery("explain format = brief select * from t").Check(testkit.Rows(
		"TableReader 2.00 root  data:TableFullScan",
		"└─TableFullScan 2.00 cop[tikv] table:t keep order:false",
	))
	tk.MustExec("set tidb_opt_objective='determinate'")
	tk.MustQuery("explain format = brief select * from t").Check(testkit.Rows(
		"TableReader 1.00 root  data:TableFullScan",
		"└─TableFullScan 1.00 cop[tikv] table:t keep order:false",
	))
	tk.MustExec("set tidb_opt_objective='moderate'")

	// 2. test async load
	tk.MustExec("set tidb_stats_load_sync_wait = 0")
	tk.MustExec("create table t1(a int)")
	tk.MustExec("insert into t1 value(1)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(dom.InfoSchema()))
	tk.MustExec("analyze table t1")
	tk.MustQuery("explain format = brief select * from t1").Check(testkit.Rows(
		"TableReader 1.00 root  data:TableFullScan",
		"└─TableFullScan 1.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
	))
	tk.MustExec("insert into t1 value(1)")
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(dom.InfoSchema()))
	tk.MustQuery("explain format = brief select * from t1").Check(testkit.Rows(
		"TableReader 2.00 root  data:TableFullScan",
		"└─TableFullScan 2.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
	))
	tk.MustExec("set tidb_opt_objective='determinate'")
	tk.MustQuery("explain format = brief select * from t1").Check(testkit.Rows(
		"TableReader 10000.00 root  data:TableFullScan",
		"└─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo",
	))
	require.NoError(t, h.LoadNeededHistograms())
	tk.MustQuery("explain format = brief select * from t1").Check(testkit.Rows(
		"TableReader 1.00 root  data:TableFullScan",
		"└─TableFullScan 1.00 cop[tikv] table:t1 keep order:false",
	))
}

func TestIssue52472(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE t1 ( c1 int);")
	tk.MustExec("CREATE TABLE t2 ( c1 int unsigned);")
	tk.MustExec("CREATE TABLE t3 ( c1 bigint unsigned);")
	tk.MustExec("INSERT INTO t1 (c1) VALUES (8);")
	tk.MustExec("INSERT INTO t2 (c1) VALUES (2454396638);")

	// union int and unsigned int will be promoted to long long
	rs, err := tk.Exec("SELECT c1 FROM t1 UNION ALL SELECT c1 FROM t2")
	require.NoError(t, err)
	require.Len(t, rs.Fields(), 1)
	require.Equal(t, mysql.TypeLonglong, rs.Fields()[0].Column.FieldType.GetType())
	require.NoError(t, rs.Close())

	// union int (even literal) and unsigned bigint will be promoted to decimal
	rs, err = tk.Exec("SELECT 0 UNION ALL SELECT c1 FROM t3")
	require.NoError(t, err)
	require.Len(t, rs.Fields(), 1)
	require.Equal(t, mysql.TypeNewDecimal, rs.Fields()[0].Column.FieldType.GetType())
	require.NoError(t, rs.Close())
}
