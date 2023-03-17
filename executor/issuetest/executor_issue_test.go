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

package issuetest_test

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/pingcap/failpoint"
	_ "github.com/pingcap/tidb/autoid_service"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestIssue23993(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Real cast to time should return NULL
	tk.MustExec("drop table if exists t_issue_23993")
	tk.MustExec("create table t_issue_23993(a double)")
	tk.MustExec("insert into t_issue_23993 values(-790822912)")
	tk.MustQuery("select cast(a as time) from t_issue_23993").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select a from t_issue_23993 where cast(a as time)").Check(testkit.Rows())
	// Int cast to time should return NULL
	tk.MustExec("drop table if exists t_issue_23993")
	tk.MustExec("create table t_issue_23993(a int)")
	tk.MustExec("insert into t_issue_23993 values(-790822912)")
	tk.MustQuery("select cast(a as time) from t_issue_23993").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select a from t_issue_23993 where cast(a as time)").Check(testkit.Rows())
	// Decimal cast to time should return NULL
	tk.MustExec("drop table if exists t_issue_23993")
	tk.MustExec("create table t_issue_23993(a decimal)")
	tk.MustExec("insert into t_issue_23993 values(-790822912)")
	tk.MustQuery("select cast(a as time) from t_issue_23993").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select a from t_issue_23993 where cast(a as time)").Check(testkit.Rows())
	// String cast to time should not return NULL
	tk.MustExec("drop table if exists t_issue_23993")
	tk.MustExec("create table t_issue_23993(a varchar(255))")
	tk.MustExec("insert into t_issue_23993 values('-790822912')")
	tk.MustQuery("select cast(a as time) from t_issue_23993").Check(testkit.Rows("-838:59:59"))
	tk.MustQuery("select a from t_issue_23993 where cast(a as time)").Check(testkit.Rows("-790822912"))
}

func TestIssue22201(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("SELECT HEX(WEIGHT_STRING('ab' AS BINARY(1000000000000000000)));").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1301 Result of cast_as_binary() was larger than max_allowed_packet (67108864) - truncated"))
	tk.MustQuery("SELECT HEX(WEIGHT_STRING('ab' AS char(1000000000000000000)));").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1301 Result of weight_string() was larger than max_allowed_packet (67108864) - truncated"))
}

func TestIssue22941(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists m, mp")
	tk.MustExec(`CREATE TABLE m (
		mid varchar(50) NOT NULL,
		ParentId varchar(50) DEFAULT NULL,
		PRIMARY KEY (mid),
		KEY ind_bm_parent (ParentId,mid)
	)`)
	// mp should have more columns than m
	tk.MustExec(`CREATE TABLE mp (
		mpid bigint(20) unsigned NOT NULL DEFAULT '0',
		mid varchar(50) DEFAULT NULL COMMENT '模块主键',
		sid int,
	PRIMARY KEY (mpid)
	);`)

	tk.MustExec(`insert into mp values("1","1","0");`)
	tk.MustExec(`insert into m values("0", "0");`)
	rs := tk.MustQuery(`SELECT ( SELECT COUNT(1) FROM m WHERE ParentId = c.mid ) expand,  bmp.mpid,  bmp.mpid IS NULL,bmp.mpid IS NOT NULL, sid FROM m c LEFT JOIN mp bmp ON c.mid = bmp.mid  WHERE c.ParentId = '0'`)
	rs.Check(testkit.Rows("1 <nil> 1 0 <nil>"))

	rs = tk.MustQuery(`SELECT  bmp.mpid,  bmp.mpid IS NULL,bmp.mpid IS NOT NULL FROM m c LEFT JOIN mp bmp ON c.mid = bmp.mid  WHERE c.ParentId = '0'`)
	rs.Check(testkit.Rows("<nil> 1 0"))
}

func TestIssue23609(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("CREATE TABLE `t1` (\n  `a` timestamp NULL DEFAULT NULL,\n  `b` year(4) DEFAULT NULL,\n  KEY `a` (`a`),\n  KEY `b` (`b`)\n)")
	tk.MustExec("insert into t1 values(\"2002-10-03 04:28:53\",2000), (\"2002-10-03 04:28:53\",2002), (NULL, 2002)")
	tk.MustQuery("select /*+ inl_join (x,y) */ * from t1 x cross join t1 y on x.a=y.b").Check(testkit.Rows())
	tk.MustQuery("select * from t1 x cross join t1 y on x.a>y.b order by x.a, x.b, y.a, y.b").Check(testkit.Rows("2002-10-03 04:28:53 2000 <nil> 2002", "2002-10-03 04:28:53 2000 2002-10-03 04:28:53 2000", "2002-10-03 04:28:53 2000 2002-10-03 04:28:53 2002", "2002-10-03 04:28:53 2002 <nil> 2002", "2002-10-03 04:28:53 2002 2002-10-03 04:28:53 2000", "2002-10-03 04:28:53 2002 2002-10-03 04:28:53 2002"))
	tk.MustQuery("select * from t1 where a = b").Check(testkit.Rows())
	tk.MustQuery("select * from t1 where a < b").Check(testkit.Rows())
	require.Equal(t, uint16(0), tk.Session().GetSessionVars().StmtCtx.WarningCount())
}

func TestIssue24091(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	defer tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t(a int) partition by hash (a div 0) partitions 10;")
	tk.MustExec("insert into t values (NULL);")

	tk.MustQuery("select null div 0;").Check(testkit.Rows("<nil>"))
	tk.MustQuery("select * from t;").Check(testkit.Rows("<nil>"))
}

func TestIssue24210(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=1")

	// for ProjectionExec
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockProjectionExecBaseExecutorOpenReturnedError", `return(true)`))
	err := tk.ExecToErr("select a from (select 1 as a, 2 as b) t")
	require.EqualError(t, err, "mock ProjectionExec.baseExecutor.Open returned error")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockProjectionExecBaseExecutorOpenReturnedError"))

	// for HashAggExec
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockHashAggExecBaseExecutorOpenReturnedError", `return(true)`))
	err = tk.ExecToErr("select sum(a) from (select 1 as a, 2 as b) t group by b")
	require.EqualError(t, err, "mock HashAggExec.baseExecutor.Open returned error")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockHashAggExecBaseExecutorOpenReturnedError"))

	// for StreamAggExec
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockStreamAggExecBaseExecutorOpenReturnedError", `return(true)`))
	err = tk.ExecToErr("select sum(a) from (select 1 as a, 2 as b) t")
	require.EqualError(t, err, "mock StreamAggExec.baseExecutor.Open returned error")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockStreamAggExecBaseExecutorOpenReturnedError"))

	// for SelectionExec
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/executor/mockSelectionExecBaseExecutorOpenReturnedError", `return(true)`))
	err = tk.ExecToErr("select * from (select rand() as a) t where a > 0")
	require.EqualError(t, err, "mock SelectionExec.baseExecutor.Open returned error")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/executor/mockSelectionExecBaseExecutorOpenReturnedError"))
}

func TestIssue25506(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tbl_3, tbl_23")
	tk.MustExec("create table tbl_3 (col_15 bit(20))")
	tk.MustExec("insert into tbl_3 values (0xFFFF)")
	tk.MustExec("insert into tbl_3 values (0xFF)")
	tk.MustExec("create table tbl_23 (col_15 bit(15))")
	tk.MustExec("insert into tbl_23 values (0xF)")
	tk.MustQuery("(select col_15 from tbl_23) union all (select col_15 from tbl_3 for update) order by col_15").Check(testkit.Rows("\x00\x00\x0F", "\x00\x00\xFF", "\x00\xFF\xFF"))
}

func TestIssue26348(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec(`CREATE TABLE t (
a varchar(8) DEFAULT NULL,
b varchar(8) DEFAULT NULL,
c decimal(20,2) DEFAULT NULL,
d decimal(15,8) DEFAULT NULL
);`)
	tk.MustExec(`insert into t values(20210606, 20210606, 50000.00, 5.04600000);`)
	tk.MustQuery(`select a * c *(d/36000) from t;`).Check(testkit.Rows("141642663.71666598"))
	tk.MustQuery(`select cast(a as double) * cast(c as double) *cast(d/36000 as double) from t;`).Check(testkit.Rows("141642663.71666598"))
	tk.MustQuery("select 20210606*50000.00*(5.04600000/36000)").Check(testkit.Rows("141642663.71666599297980"))

	// differs from MySQL cause constant-fold .
	tk.MustQuery("select \"20210606\"*50000.00*(5.04600000/36000)").Check(testkit.Rows("141642663.71666598"))
	tk.MustQuery("select cast(\"20210606\" as double)*50000.00*(5.04600000/36000)").Check(testkit.Rows("141642663.71666598"))
}

func TestIssue26532(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select greatest(cast(\"2020-01-01 01:01:01\" as datetime), cast(\"2019-01-01 01:01:01\" as datetime) )union select null;").Sort().Check(testkit.Rows("2020-01-01 01:01:01", "<nil>"))
	tk.MustQuery("select least(cast(\"2020-01-01 01:01:01\" as datetime), cast(\"2019-01-01 01:01:01\" as datetime) )union select null;").Sort().Check(testkit.Rows("2019-01-01 01:01:01", "<nil>"))
	tk.MustQuery("select greatest(\"2020-01-01 01:01:01\" ,\"2019-01-01 01:01:01\" )union select null;").Sort().Check(testkit.Rows("2020-01-01 01:01:01", "<nil>"))
	tk.MustQuery("select least(\"2020-01-01 01:01:01\" , \"2019-01-01 01:01:01\" )union select null;").Sort().Check(testkit.Rows("2019-01-01 01:01:01", "<nil>"))
}

func TestIssue25447(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1(a int, b varchar(8))")
	tk.MustExec("insert into t1 values(1,'1')")
	tk.MustExec("create table t2(a int , b varchar(8) GENERATED ALWAYS AS (c) VIRTUAL, c varchar(8), PRIMARY KEY (a))")
	tk.MustExec("insert into t2(a) values(1)")
	tk.MustQuery("select /*+ tidb_inlj(t2) */ t2.b, t1.b from t1 join t2 ON t2.a=t1.a").Check(testkit.Rows("<nil> 1"))
}

func TestIssue23602(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeOn
	tk.MustExec("CREATE TABLE t (a bigint unsigned PRIMARY KEY)")
	defer tk.MustExec("DROP TABLE t")
	tk.MustExec("INSERT INTO t VALUES (0),(1),(2),(3),(18446744073709551600),(18446744073709551605),(18446744073709551610),(18446744073709551615)")
	tk.MustExec("ANALYZE TABLE t")
	tk.MustQuery(`EXPLAIN FORMAT = 'brief' SELECT a FROM t WHERE a >= 0x1 AND a <= 0x2`).Check(testkit.Rows(
		"TableReader 2.00 root  data:TableRangeScan]\n" +
			"[└─TableRangeScan 2.00 cop[tikv] table:t range:[1,2], keep order:false"))
	tk.MustQuery(`EXPLAIN FORMAT = 'brief' SELECT a FROM t WHERE a BETWEEN 0x1 AND 0x2`).Check(testkit.Rows(
		"TableReader 2.00 root  data:TableRangeScan]\n" +
			"[└─TableRangeScan 2.00 cop[tikv] table:t range:[1,2], keep order:false"))
	tk.MustQuery("SELECT a FROM t WHERE a BETWEEN 0xFFFFFFFFFFFFFFF5 AND X'FFFFFFFFFFFFFFFA'").Check(testkit.Rows("18446744073709551605", "18446744073709551610"))
}

func TestIssue28935(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@tidb_enable_vectorized_expression=true")
	tk.MustQuery(`select trim(leading from " a "), trim(both from " a "), trim(trailing from " a ")`).Check(testkit.Rows("a  a  a"))
	tk.MustQuery(`select trim(leading null from " a "), trim(both null from " a "), trim(trailing null from " a ")`).Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustQuery(`select trim(null from " a ")`).Check(testkit.Rows("<nil>"))

	tk.MustExec("set @@tidb_enable_vectorized_expression=false")
	tk.MustQuery(`select trim(leading from " a "), trim(both from " a "), trim(trailing from " a ")`).Check(testkit.Rows("a  a  a"))
	tk.MustQuery(`select trim(leading null from " a "), trim(both null from " a "), trim(trailing null from " a ")`).Check(testkit.Rows("<nil> <nil> <nil>"))
	tk.MustQuery(`select trim(null from " a ")`).Check(testkit.Rows("<nil>"))
}

func TestIssue29412(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t29142_1")
	tk.MustExec("drop table if exists t29142_2")
	tk.MustExec("create table t29142_1(a int);")
	tk.MustExec("create table t29142_2(a double);")
	tk.MustExec("insert into t29142_1 value(20);")
	tk.MustQuery("select sum(distinct a) as x from t29142_1 having x > some ( select a from t29142_2 where x in (a));").Check(nil)
}

func TestIssue28650(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(a int, index(a));")
	tk.MustExec("create table t2(a int, c int, b char(50), index(a,c,b));")
	tk.MustExec("set tidb_enable_rate_limit_action=off;")

	var wg util.WaitGroupWrapper
	sql := `explain analyze
	select /*+ stream_agg(@sel_1) stream_agg(@sel_3) %s(@sel_2 t2)*/ count(1) from
		(
			SELECT t2.a AS t2_external_user_ext_id, t2.b AS t2_t1_ext_id  FROM t2 INNER JOIN (SELECT t1.a AS d_t1_ext_id  FROM t1 GROUP BY t1.a) AS anon_1 ON anon_1.d_t1_ext_id = t2.a  WHERE t2.c = 123 AND t2.b
		IN ("%s") ) tmp`

	sqls := make([]string, 2)
	wg.Run(func() {
		inElems := make([]string, 1000)
		for i := 0; i < len(inElems); i++ {
			inElems[i] = fmt.Sprintf("wm_%dbDgAAwCD-v1QB%dxky-g_dxxQCw", rand.Intn(100), rand.Intn(100))
		}
		sqls[0] = fmt.Sprintf(sql, "inl_join", strings.Join(inElems, "\",\""))
		sqls[1] = fmt.Sprintf(sql, "inl_hash_join", strings.Join(inElems, "\",\""))
	})

	tk.MustExec("insert into t1 select rand()*400;")
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into t1 select rand()*400 from t1;")
	}
	tk.MustExec("SET GLOBAL tidb_mem_oom_action = 'CANCEL'")
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action='LOG'")
	wg.Wait()
	for _, sql := range sqls {
		tk.MustExec("set @@tidb_mem_quota_query = 1073741824") // 1GB
		require.Nil(t, tk.QueryToErr(sql))
		tk.MustExec("set @@tidb_mem_quota_query = 33554432") // 32MB, out of memory during executing
		require.True(t, strings.Contains(tk.QueryToErr(sql).Error(), "Out Of Memory Quota!"))
		tk.MustExec("set @@tidb_mem_quota_query = 65536") // 64KB, out of memory during building the plan
		require.True(t, strings.Contains(tk.ExecToErr(sql).Error(), "Out Of Memory Quota!"))
	}
}

func TestIssue30289(t *testing.T) {
	fpName := "github.com/pingcap/tidb/executor/issue30289"
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	require.NoError(t, failpoint.Enable(fpName, `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable(fpName))
	}()
	err := tk.QueryToErr("select /*+ hash_join(t1) */ * from t t1 join t t2 on t1.a=t2.a")
	require.EqualError(t, err, "issue30289 build return error")
}

func TestIssue29498(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("DROP TABLE IF EXISTS t1;")
	tk.MustExec("CREATE TABLE t1 (t3 TIME(3), d DATE, t TIME);")
	tk.MustExec("INSERT INTO t1 VALUES ('00:00:00.567', '2002-01-01', '00:00:02');")

	res := tk.MustQuery("SELECT CONCAT(IFNULL(t3, d)) AS col1 FROM t1;")
	row := res.Rows()[0][0].(string)
	require.Equal(t, mysql.MaxDatetimeWidthNoFsp+3+1, len(row))
	require.Equal(t, "00:00:00.567", row[len(row)-12:])

	res = tk.MustQuery("SELECT IFNULL(t3, d) AS col1 FROM t1;")
	row = res.Rows()[0][0].(string)
	require.Equal(t, mysql.MaxDatetimeWidthNoFsp+3+1, len(row))
	require.Equal(t, "00:00:00.567", row[len(row)-12:])

	res = tk.MustQuery("SELECT CONCAT(IFNULL(t, d)) AS col1 FROM t1;")
	row = res.Rows()[0][0].(string)
	require.Equal(t, mysql.MaxDatetimeWidthNoFsp, len(row))
	require.Equal(t, "00:00:02", row[len(row)-8:])

	res = tk.MustQuery("SELECT IFNULL(t, d) AS col1 FROM t1;")
	row = res.Rows()[0][0].(string)
	require.Equal(t, mysql.MaxDatetimeWidthNoFsp, len(row))
	require.Equal(t, "00:00:02", row[len(row)-8:])

	res = tk.MustQuery("SELECT CONCAT(xx) FROM (SELECT t3 AS xx FROM t1 UNION SELECT d FROM t1) x ORDER BY -xx LIMIT 1;")
	row = res.Rows()[0][0].(string)
	require.Equal(t, mysql.MaxDatetimeWidthNoFsp+3+1, len(row))
	require.Equal(t, "00:00:00.567", row[len(row)-12:])

	res = tk.MustQuery("SELECT CONCAT(CASE WHEN d IS NOT NULL THEN t3 ELSE d END) AS col1 FROM t1;")
	row = res.Rows()[0][0].(string)
	require.Equal(t, mysql.MaxDatetimeWidthNoFsp+3+1, len(row))
	require.Equal(t, "00:00:00.567", row[len(row)-12:])
}

func TestIssue30971(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (id int);")
	tk.MustExec("create table t2 (id int, c int);")

	testCases := []struct {
		sql    string
		fields int
	}{
		// Fix a bug that the column length field returned to client is incorrect using MySQL prepare protocol.
		{"select * from t1 union select 1 from t1", 1},
		{"select c from t2 union select * from t1", 1},
		{"select * from t1", 1},
		{"select * from t2 where c in (select * from t1)", 2},
		{"insert into t1 values (?)", 0},
		{"update t1 set id = ?", 0},
	}
	for _, test := range testCases {
		_, _, fields, err := tk.Session().PrepareStmt(test.sql)
		require.NoError(t, err)
		require.Len(t, fields, test.fields)
	}
}

func TestIssue31678(t *testing.T) {
	// The issue31678 is mainly about type conversion in UNION
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("USE test")
	tk.MustExec("DROP TABLE IF EXISTS t1, t2;")

	// https://github.com/pingcap/tidb/issues/31678
	tk.MustExec("CREATE TABLE t1 (c VARCHAR(11)) CHARACTER SET utf8mb4")
	tk.MustExec("CREATE TABLE t2 (b CHAR(1) CHARACTER SET binary, i INT)")
	tk.MustExec("INSERT INTO t1 (c) VALUES ('н1234567890')")
	tk.MustExec("INSERT INTO t2 (b, i) VALUES ('1', 1)")
	var tests = []struct {
		query           string
		expectedFlen    int
		expectedCharset string
		result          []string
	}{
		{"SELECT c FROM t1 UNION SELECT b FROM t2", 44, "binary", []string{"1", "н1234567890"}},
		{"SELECT c FROM t1 UNION SELECT i FROM t2", 20, "utf8mb4", []string{"1", "н1234567890"}},
		{"SELECT i FROM t2 UNION SELECT c FROM t1", 20, "utf8mb4", []string{"1", "н1234567890"}},
		{"SELECT b FROM t2 UNION SELECT c FROM t1", 44, "binary", []string{"1", "н1234567890"}},
	}
	for _, test := range tests {
		tk.MustQuery(test.query).Sort().Check(testkit.Rows(test.result...))
		rs, err := tk.Exec(test.query)
		require.NoError(t, err)
		resultFields := rs.Fields()
		require.Equal(t, 1, len(resultFields), test.query)
		require.Equal(t, test.expectedFlen, resultFields[0].Column.FieldType.GetFlen(), test.query)
		require.Equal(t, test.expectedCharset, resultFields[0].Column.FieldType.GetCharset(), test.query)
	}
	tk.MustExec("DROP TABLE t1, t2;")

	// test some other charset
	tk.MustExec("CREATE TABLE t1 (c1 VARCHAR(5) CHARACTER SET utf8mb4, c2 VARCHAR(1) CHARACTER SET binary)")
	tk.MustExec("CREATE TABLE t2 (c1 CHAR(10) CHARACTER SET GBK, c2 VARCHAR(50) CHARACTER SET binary)")
	tk.MustExec("INSERT INTO t1 VALUES ('一二三四五', '1')")
	tk.MustExec("INSERT INTO t2 VALUES ('一二三四五六七八九十', '1234567890')")
	gbkResult, err := charset.NewCustomGBKEncoder().String("一二三四五六七八九十")
	require.NoError(t, err)
	tests = []struct {
		query           string
		expectedFlen    int
		expectedCharset string
		result          []string
	}{
		{"SELECT c1 FROM t1 UNION SELECT c1 FROM t2", 10, "utf8mb4", []string{"一二三四五", "一二三四五六七八九十"}},
		{"SELECT c1 FROM t1 UNION SELECT c2 FROM t2", 50, "binary", []string{"1234567890", "一二三四五"}},
		{"SELECT c2 FROM t1 UNION SELECT c1 FROM t2", 20, "binary", []string{"1", gbkResult}},
		{"SELECT c2 FROM t1 UNION SELECT c2 FROM t2", 50, "binary", []string{"1", "1234567890"}},
	}
	for _, test := range tests {
		tk.MustQuery(test.query).Sort().Check(testkit.Rows(test.result...))
		rs, err := tk.Exec(test.query)
		require.NoError(t, err)
		resultFields := rs.Fields()
		require.Equal(t, 1, len(resultFields), test.query)
		require.Equal(t, test.expectedFlen, resultFields[0].Column.FieldType.GetFlen(), test.query)
		require.Equal(t, test.expectedCharset, resultFields[0].Column.FieldType.GetCharset(), test.query)
	}
	tk.MustExec("DROP TABLE t1, t2;")
}

func TestIndexJoin31494(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1(a int(11) default null, b int(11) default null, key(b));")
	insertStr := "insert into t1 values(1, 1)"
	for i := 1; i < 32768; i++ {
		insertStr += fmt.Sprintf(", (%d, %d)", i, i)
	}
	tk.MustExec(insertStr)
	tk.MustExec("create table t2(a int(11) default null, b int(11) default null, c int(11) default null)")
	insertStr = "insert into t2 values(1, 1, 1)"
	for i := 1; i < 32768; i++ {
		insertStr += fmt.Sprintf(", (%d, %d, %d)", i, i, i)
	}
	tk.MustExec(insertStr)
	sm := &testkit.MockSessionManager{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Session().SetSessionManager(sm)
	dom.ExpensiveQueryHandle().SetSessionManager(sm)
	defer tk.MustExec("SET GLOBAL tidb_mem_oom_action = DEFAULT")
	tk.MustExec("SET GLOBAL tidb_mem_oom_action='CANCEL'")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
	tk.MustExec("set @@tidb_mem_quota_query=2097152;")
	// This bug will be reproduced in 10 times.
	for i := 0; i < 10; i++ {
		err := tk.QueryToErr("select /*+ inl_join(t1) */ * from t1 right join t2 on t1.b=t2.b;")
		require.Error(t, err)
		require.Regexp(t, "Out Of Memory Quota!.*", err.Error())
		err = tk.QueryToErr("select /*+ inl_hash_join(t1) */ * from t1 right join t2 on t1.b=t2.b;")
		require.Error(t, err)
		require.Regexp(t, "Out Of Memory Quota!.*", err.Error())
	}
}

// Details at https://github.com/pingcap/tidb/issues/31038
func TestFix31038(t *testing.T) {
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.Instance.EnableCollectExecutionInfo.Store(false)
	})
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t123")
	tk.MustExec("create table t123 (id int);")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/copr/disable-collect-execution", `return(true)`))
	tk.MustQuery("select * from t123;")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/copr/disable-collect-execution"))
}

func TestFix31530(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk2.MustExec("use test")
	defer func() {
		tk.MustExec("drop table if exists t1")
	}()
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (id int primary key, v int)")
	tk.MustExec("insert into t1 values(1, 10)")
	tk.MustExec("begin pessimistic")
	tk.MustQuery("select * from t1").Check(testkit.Rows("1 10"))

	// update t1 before session1 transaction not finished
	tk2.MustExec("update t1 set v=11 where id=1")

	tk.MustQuery("(select 'a' as c, id, v from t1 for update) union all (select 'b', id, v from t1) order by c").Check(testkit.Rows("a 1 11", "b 1 10"))
	tk.MustQuery("(select 'a' as c, id, v from t1) union all (select 'b', id, v from t1 for update) order by c").Check(testkit.Rows("a 1 10", "b 1 11"))
	tk.MustQuery("(select 'a' as c, id, v from t1 where id=1 for update) union all (select 'b', id, v from t1 where id=1) order by c").Check(testkit.Rows("a 1 11", "b 1 10"))
	tk.MustQuery("(select 'a' as c, id, v from t1 where id=1) union all (select 'b', id, v from t1 where id=1 for update) order by c").Check(testkit.Rows("a 1 10", "b 1 11"))
	tk.MustExec("rollback")
}

func TestFix31537(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@foreign_key_checks=0")
	tk.MustExec(`CREATE TABLE trade (
  t_id bigint(16) NOT NULL AUTO_INCREMENT,
  t_dts datetime NOT NULL,
  t_st_id char(4) NOT NULL,
  t_tt_id char(3) NOT NULL,
  t_is_cash tinyint(1) NOT NULL,
  t_s_symb char(15) NOT NULL,
  t_qty mediumint(7) NOT NULL,
  t_bid_price decimal(8,2) NOT NULL,
  t_ca_id bigint(12) NOT NULL,
  t_exec_name varchar(49) NOT NULL,
  t_trade_price decimal(8,2) DEFAULT NULL,
  t_chrg decimal(10,2) NOT NULL,
  t_comm decimal(10,2) NOT NULL,
  t_tax decimal(10,2) NOT NULL,
  t_lifo tinyint(1) NOT NULL,
  PRIMARY KEY (t_id) /*T![clustered_index] CLUSTERED */,
  KEY i_t_ca_id_dts (t_ca_id,t_dts),
  KEY i_t_s_symb_dts (t_s_symb,t_dts),
  CONSTRAINT fk_trade_st FOREIGN KEY (t_st_id) REFERENCES status_type (st_id),
  CONSTRAINT fk_trade_tt FOREIGN KEY (t_tt_id) REFERENCES trade_type (tt_id),
  CONSTRAINT fk_trade_s FOREIGN KEY (t_s_symb) REFERENCES security (s_symb),
  CONSTRAINT fk_trade_ca FOREIGN KEY (t_ca_id) REFERENCES customer_account (ca_id)
) ;`)
	tk.MustExec(`CREATE TABLE trade_history (
  th_t_id bigint(16) NOT NULL,
  th_dts datetime NOT NULL,
  th_st_id char(4) NOT NULL,
  PRIMARY KEY (th_t_id,th_st_id) /*T![clustered_index] NONCLUSTERED */,
  KEY i_th_t_id_dts (th_t_id,th_dts),
  CONSTRAINT fk_trade_history_t FOREIGN KEY (th_t_id) REFERENCES trade (t_id),
  CONSTRAINT fk_trade_history_st FOREIGN KEY (th_st_id) REFERENCES status_type (st_id)
);
`)
	tk.MustExec(`CREATE TABLE status_type (
  st_id char(4) NOT NULL,
  st_name char(10) NOT NULL,
  PRIMARY KEY (st_id) /*T![clustered_index] NONCLUSTERED */
);`)
	tk.MustQuery(`trace plan SELECT T_ID, T_S_SYMB, T_QTY, ST_NAME, TH_DTS FROM ( SELECT T_ID AS ID FROM TRADE WHERE T_CA_ID = 43000014236 ORDER BY T_DTS DESC LIMIT 10 ) T, TRADE, TRADE_HISTORY, STATUS_TYPE WHERE TRADE.T_ID = ID AND TRADE_HISTORY.TH_T_ID = TRADE.T_ID AND STATUS_TYPE.ST_ID = TRADE_HISTORY.TH_ST_ID ORDER BY TH_DTS DESC LIMIT 30;`)
}

func TestIssue30382(t *testing.T) {
	failpoint.Enable("github.com/pingcap/tidb/planner/core/forceDynamicPrune", `return(true)`)
	defer failpoint.Disable("github.com/pingcap/tidb/planner/core/forceDynamicPrune")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=2")
	tk.MustExec("set @@session.tidb_enable_list_partition = ON;")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1  (c_int int, c_str varchar(40), c_decimal decimal(12, 6), primary key (c_int) , key(c_str(2)) , key(c_decimal) ) partition by list (c_int) ( partition p0 values IN (1, 5, 9, 13, 17, 21, 25, 29, 33, 37), partition p1 values IN (2, 6, 10, 14, 18, 22, 26, 30, 34, 38), partition p2 values IN (3, 7, 11, 15, 19, 23, 27, 31, 35, 39), partition p3 values IN (4, 8, 12, 16, 20, 24, 28, 32, 36, 40)) ;")
	tk.MustExec("create table t2  (c_int int, c_str varchar(40), c_decimal decimal(12, 6), primary key (c_int) , key(c_str) , key(c_decimal) ) partition by hash (c_int) partitions 4;")
	tk.MustExec("insert into t1 values (6, 'musing mayer', 1.280), (7, 'wizardly heisenberg', 6.589), (8, 'optimistic swirles', 9.633), (9, 'hungry haslett', 2.659), (10, 'stupefied wiles', 2.336);")
	tk.MustExec("insert into t2 select * from t1 ;")
	tk.MustExec("begin;")
	tk.MustQuery("select * from t1 where c_str <> any (select c_str from t2 where c_decimal < 5) for update;").Sort().Check(testkit.Rows(
		"10 stupefied wiles 2.336000",
		"6 musing mayer 1.280000",
		"7 wizardly heisenberg 6.589000",
		"8 optimistic swirles 9.633000",
		"9 hungry haslett 2.659000"))
	tk.MustQuery("explain format = 'brief' select * from t1 where c_str <> any (select c_str from t2 where c_decimal < 5) for update;").Check(testkit.Rows(
		"SelectLock 6400.00 root  for update 0",
		"└─HashJoin 6400.00 root  CARTESIAN inner join, other cond:or(gt(Column#8, 1), or(ne(test.t1.c_str, Column#7), if(ne(Column#9, 0), NULL, 0)))",
		"  ├─Selection(Build) 0.80 root  ne(Column#10, 0)",
		"  │ └─HashAgg 1.00 root  funcs:max(Column#17)->Column#7, funcs:count(distinct Column#18)->Column#8, funcs:sum(Column#19)->Column#9, funcs:count(1)->Column#10",
		"  │   └─Projection 3323.33 root  test.t2.c_str, test.t2.c_str, cast(isnull(test.t2.c_str), decimal(20,0) BINARY)->Column#19",
		"  │     └─TableReader 3323.33 root partition:all data:Selection",
		"  │       └─Selection 3323.33 cop[tikv]  lt(test.t2.c_decimal, 5)",
		"  │         └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"  └─TableReader(Probe) 8000.00 root partition:all data:Selection",
		"    └─Selection 8000.00 cop[tikv]  if(isnull(test.t1.c_str), NULL, 1)",
		"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustExec("commit")
}

func Test12201(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists e")
	tk.MustExec("create table e (e enum('a', 'b'))")
	tk.MustExec("insert into e values ('a'), ('b')")
	tk.MustQuery("select * from e where case 1 when 0 then e end").Check(testkit.Rows())
	tk.MustQuery("select * from e where case 1 when 1 then e end").Check(testkit.Rows("a", "b"))
	tk.MustQuery("select * from e where case e when 1 then e end").Check(testkit.Rows("a"))
	tk.MustQuery("select * from e where case 1 when e then e end").Check(testkit.Rows("a"))
}

func TestIssue21451(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (en enum('c', 'b', 'a'));")
	tk.MustExec("insert into t values ('a'), ('b'), ('c');")
	tk.MustQuery("select max(en) from t;").Check(testkit.Rows("c"))
	tk.MustQuery("select min(en) from t;").Check(testkit.Rows("a"))
	tk.MustQuery("select * from t order by en;").Check(testkit.Rows("c", "b", "a"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(s set('c', 'b', 'a'));")
	tk.MustExec("insert into t values ('a'), ('b'), ('c');")
	tk.MustQuery("select max(s) from t;").Check(testkit.Rows("c"))
	tk.MustQuery("select min(s) from t;").Check(testkit.Rows("a"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(id int, en enum('c', 'b', 'a'))")
	tk.MustExec("insert into t values (1, 'a'),(2, 'b'), (3, 'c'), (1, 'c');")
	tk.MustQuery("select id, max(en) from t where id=1 group by id;").Check(testkit.Rows("1 c"))
	tk.MustQuery("select id, min(en) from t where id=1 group by id;").Check(testkit.Rows("1 a"))
	tk.MustExec("drop table t")

	tk.MustExec("create table t(id int, s set('c', 'b', 'a'));")
	tk.MustExec("insert into t values (1, 'a'),(2, 'b'), (3, 'c'), (1, 'c');")
	tk.MustQuery("select id, max(s) from t where id=1 group by id;").Check(testkit.Rows("1 c"))
	tk.MustQuery("select id, min(s) from t where id=1 group by id;").Check(testkit.Rows("1 a"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(e enum('e','d','c','b','a'))")
	tk.MustExec("insert into t values ('e'),('d'),('c'),('b'),('a');")
	tk.MustQuery("select * from t order by e limit 1;").Check(testkit.Rows("e"))

	tk.MustExec("drop table t")
	tk.MustExec("create table t(s set('e', 'd', 'c', 'b', 'a'))")
	tk.MustExec("insert into t values ('e'),('d'),('c'),('b'),('a');")
	tk.MustQuery("select * from t order by s limit 1;").Check(testkit.Rows("e"))
	tk.MustExec("drop table t")
}

func TestIssue15563(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustQuery("select distinct 0.7544678906163867 /  0.68234634;").Check(testkit.Rows("1.10569639842486251190"))
}

func TestIssue22231(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_issue_22231")
	tk.MustExec("create table t_issue_22231(a datetime)")
	tk.MustExec("insert into t_issue_22231 values('2020--05-20 01:22:12')")
	tk.MustQuery("select * from t_issue_22231 where a >= '2020-05-13 00:00:00 00:00:00' and a <= '2020-05-28 23:59:59 00:00:00'").Check(testkit.Rows("2020-05-20 01:22:12"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '2020-05-13 00:00:00 00:00:00'", "Warning 1292 Truncated incorrect datetime value: '2020-05-28 23:59:59 00:00:00'"))

	tk.MustQuery("select cast('2020-10-22 10:31-10:12' as datetime)").Check(testkit.Rows("2020-10-22 10:31:10"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '2020-10-22 10:31-10:12'"))
	tk.MustQuery("select cast('2020-05-28 23:59:59 00:00:00' as datetime)").Check(testkit.Rows("2020-05-28 23:59:59"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1292 Truncated incorrect datetime value: '2020-05-28 23:59:59 00:00:00'"))
	tk.MustExec("drop table if exists t_issue_22231")

	tk.MustQuery("SELECT CAST(\"1111111111-\" AS DATE);")
	tk.MustQuery("SHOW WARNINGS").Check(testkit.Rows("Warning 1292 Incorrect datetime value: '1111111111-'"))
}

// TestIssue2612 is related with https://github.com/pingcap/tidb/issues/2612
func TestIssue2612(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t`)
	tk.MustExec(`create table t (
		create_at datetime NOT NULL DEFAULT '1000-01-01 00:00:00',
		finish_at datetime NOT NULL DEFAULT '1000-01-01 00:00:00');`)
	tk.MustExec(`insert into t values ('2016-02-13 15:32:24',  '2016-02-11 17:23:22');`)
	rs, err := tk.Exec(`select timediff(finish_at, create_at) from t;`)
	require.NoError(t, err)
	req := rs.NewChunk(nil)
	err = rs.Next(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, "-46:09:02", req.GetRow(0).GetDuration(0, 0).String())
	require.Nil(t, rs.Close())
}

// TestIssue345 is related with https://github.com/pingcap/tidb/issues/345
func TestIssue345(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1, t2`)
	tk.MustExec(`create table t1 (c1 int);`)
	tk.MustExec(`create table t2 (c2 int);`)
	tk.MustExec(`insert into t1 values (1);`)
	tk.MustExec(`insert into t2 values (2);`)
	tk.MustExec(`update t1, t2 set t1.c1 = 2, t2.c2 = 1;`)
	tk.MustExec(`update t1, t2 set c1 = 2, c2 = 1;`)
	tk.MustExec(`update t1 as a, t2 as b set a.c1 = 2, b.c2 = 1;`)

	// Check t1 content
	r := tk.MustQuery("SELECT * FROM t1;")
	r.Check(testkit.Rows("2"))
	// Check t2 content
	r = tk.MustQuery("SELECT * FROM t2;")
	r.Check(testkit.Rows("1"))

	tk.MustExec(`update t1 as a, t2 as t1 set a.c1 = 1, t1.c2 = 2;`)
	// Check t1 content
	r = tk.MustQuery("SELECT * FROM t1;")
	r.Check(testkit.Rows("1"))
	// Check t2 content
	r = tk.MustQuery("SELECT * FROM t2;")
	r.Check(testkit.Rows("2"))

	_, err := tk.Exec(`update t1 as a, t2 set t1.c1 = 10;`)
	require.Error(t, err)
}

func TestIssue5055(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`drop table if exists t1, t2`)
	tk.MustExec(`create table t1 (a int);`)
	tk.MustExec(`create table t2 (a int);`)
	tk.MustExec(`insert into t1 values(1);`)
	tk.MustExec(`insert into t2 values(1);`)
	result := tk.MustQuery("select tbl1.* from (select t1.a, 1 from t1) tbl1 left join t2 tbl2 on tbl1.a = tbl2.a order by tbl1.a desc limit 1;")
	result.Check(testkit.Rows("1 1"))
}

// TestIssue4024 This tests https://github.com/pingcap/tidb/issues/4024
func TestIssue4024(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database test2")
	tk.MustExec("use test2")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("use test")
	tk.MustExec("create table t(a int)")
	tk.MustExec("insert into t values(1)")
	tk.MustExec("update t, test2.t set test2.t.a=2")
	tk.MustQuery("select * from t").Check(testkit.Rows("1"))
	tk.MustQuery("select * from test2.t").Check(testkit.Rows("2"))
	tk.MustExec("update test.t, test2.t set test.t.a=3")
	tk.MustQuery("select * from t").Check(testkit.Rows("3"))
	tk.MustQuery("select * from test2.t").Check(testkit.Rows("2"))
}

func TestIssue5666(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("set @@profiling=1")
	tk.MustQuery("SELECT QUERY_ID, SUM(DURATION) AS SUM_DURATION FROM INFORMATION_SCHEMA.PROFILING GROUP BY QUERY_ID;").Check(testkit.Rows("0 0"))
}

func TestIssue5341(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("drop table if exists test.t")
	tk.MustExec("create table test.t(a char)")
	tk.MustExec("insert into test.t value('a')")
	tk.MustQuery("select * from test.t where a < 1 order by a limit 0;").Check(testkit.Rows())
}

func TestIssue16921(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (a float);")
	tk.MustExec("create index a on t(a);")
	tk.MustExec("insert into t values (1.0), (NULL), (0), (2.0);")
	tk.MustQuery("select `a` from `t` use index (a) where !`a`;").Check(testkit.Rows("0"))
	tk.MustQuery("select `a` from `t` ignore index (a) where !`a`;").Check(testkit.Rows("0"))
	tk.MustQuery("select `a` from `t` use index (a) where `a`;").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select `a` from `t` ignore index (a) where `a`;").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t use index (a) where not a is true;").Check(testkit.Rows("<nil>", "0"))
	tk.MustQuery("select a from t use index (a) where not not a is true;").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t use index (a) where not not a;").Check(testkit.Rows("1", "2"))
	tk.MustQuery("select a from t use index (a) where not not not a is true;").Check(testkit.Rows("<nil>", "0"))
	tk.MustQuery("select a from t use index (a) where not not not a;").Check(testkit.Rows("0"))
}

func TestIssue19100(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2;")
	tk.MustExec("create table t1 (c decimal);")
	tk.MustExec("create table t2 (c decimal, key(c));")
	tk.MustExec("insert into t1 values (null);")
	tk.MustExec("insert into t2 values (null);")
	tk.MustQuery("select count(*) from t1 where not c;").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t2 where not c;").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t1 where c;").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t2 where c;").Check(testkit.Rows("0"))
}

func TestIssue27232(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a timestamp)")
	tk.MustExec("insert into t values (\"1970-07-23 10:04:59\"), (\"2038-01-19 03:14:07\")")
	tk.MustQuery("select * from t where date_sub(a, interval 10 month) = date_sub(\"1970-07-23 10:04:59\", interval 10 month)").Check(testkit.Rows("1970-07-23 10:04:59"))
	tk.MustQuery("select * from t where timestampadd(hour, 1, a ) = timestampadd(hour, 1, \"2038-01-19 03:14:07\")").Check(testkit.Rows("2038-01-19 03:14:07"))
}

func TestIssue15718(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt(a decimal(10, 0), b varchar(1), c time);")
	tk.MustExec("insert into tt values(0, '2', null), (7, null, '1122'), (NULL, 'w', null), (NULL, '2', '3344'), (NULL, NULL, '0'), (7, 'f', '33');")
	tk.MustQuery("select a and b as d, a or c as e from tt;").Check(testkit.Rows("0 <nil>", "<nil> 1", "0 <nil>", "<nil> 1", "<nil> <nil>", "0 1"))

	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table tt(a decimal(10, 0), b varchar(1), c time);")
	tk.MustExec("insert into tt values(0, '2', '123'), (7, null, '1122'), (null, 'w', null);")
	tk.MustQuery("select a and b as d, a, b from tt order by d limit 1;").Check(testkit.Rows("<nil> 7 <nil>"))
	tk.MustQuery("select b or c as d, b, c from tt order by d limit 1;").Check(testkit.Rows("<nil> w <nil>"))

	tk.MustExec("drop table if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 FLOAT);")
	tk.MustExec("INSERT INTO t0(c0) VALUES (NULL);")
	tk.MustQuery("SELECT * FROM t0 WHERE NOT(0 OR t0.c0);").Check(testkit.Rows())
}

func TestIssue15767(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists tt;")
	tk.MustExec("create table t(a int, b char);")
	tk.MustExec("insert into t values (1,'s'),(2,'b'),(1,'c'),(2,'e'),(1,'a');")
	tk.MustExec("insert into t select * from t;")
	tk.MustExec("insert into t select * from t;")
	tk.MustExec("insert into t select * from t;")
	tk.MustQuery("select b, count(*) from ( select b from t order by a limit 20 offset 2) as s group by b order by b;").Check(testkit.Rows("a 6", "c 7", "s 7"))
}

func TestIssue16025(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t0;")
	tk.MustExec("CREATE TABLE t0(c0 NUMERIC PRIMARY KEY);")
	tk.MustExec("INSERT IGNORE INTO t0(c0) VALUES (NULL);")
	tk.MustQuery("SELECT * FROM t0 WHERE c0;").Check(testkit.Rows())
}

func TestIssue16854(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("CREATE TABLE `t` (	`a` enum('WAITING','PRINTED','STOCKUP','CHECKED','OUTSTOCK','PICKEDUP','WILLBACK','BACKED') DEFAULT NULL)")
	tk.MustExec("insert into t values(1),(2),(3),(4),(5),(6),(7);")
	for i := 0; i < 7; i++ {
		tk.MustExec("insert into t select * from t;")
	}
	tk.MustExec("set @@tidb_max_chunk_size=100;")
	tk.MustQuery("select distinct a from t order by a").Check(testkit.Rows("WAITING", "PRINTED", "STOCKUP", "CHECKED", "OUTSTOCK", "PICKEDUP", "WILLBACK"))
	tk.MustExec("drop table t")

	tk.MustExec("CREATE TABLE `t` (	`a` set('WAITING','PRINTED','STOCKUP','CHECKED','OUTSTOCK','PICKEDUP','WILLBACK','BACKED') DEFAULT NULL)")
	tk.MustExec("insert into t values(1),(2),(3),(4),(5),(6),(7);")
	for i := 0; i < 7; i++ {
		tk.MustExec("insert into t select * from t;")
	}
	tk.MustExec("set @@tidb_max_chunk_size=100;")
	tk.MustQuery("select distinct a from t order by a").Check(testkit.Rows("WAITING", "PRINTED", "WAITING,PRINTED", "STOCKUP", "WAITING,STOCKUP", "PRINTED,STOCKUP", "WAITING,PRINTED,STOCKUP"))
	tk.MustExec("drop table t")
}

func issue20975Prepare(t *testing.T, store kv.Storage) (*testkit.TestKit, *testkit.TestKit) {
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t1, t2")
	tk2.MustExec("use test")
	tk1.MustExec("create table t1(id int primary key, c int)")
	tk1.MustExec("insert into t1 values(1, 10), (2, 20)")
	return tk1, tk2
}

func TestIssue20975UpdateNoChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1, tk2 := issue20975Prepare(t, store)
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("update t1 set c=c")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")
}

func TestIssue20975SelectForUpdate(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1, tk2 := issue20975Prepare(t, store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")
}

func TestIssue20975SelectForUpdatePointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1, tk2 := issue20975Prepare(t, store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id=1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id=1 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")
}

func TestIssue20975SelectForUpdateBatchPointGet(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1, tk2 := issue20975Prepare(t, store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id in (1, 2) for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id in (1, 2) for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")
}

func issue20975PreparePartitionTable(t *testing.T, store kv.Storage) (*testkit.TestKit, *testkit.TestKit) {
	tk1 := testkit.NewTestKit(t, store)
	tk2 := testkit.NewTestKit(t, store)
	tk1.MustExec("use test")
	tk1.MustExec("drop table if exists t1, t2")
	tk2.MustExec("use test")
	tk1.MustExec(`create table t1(id int primary key, c int) partition by range (id) (
		partition p1 values less than (10),
		partition p2 values less than (20)
	)`)
	tk1.MustExec("insert into t1 values(1, 10), (2, 20), (11, 30), (12, 40)")
	return tk1, tk2
}

func TestIssue20975UpdateNoChangeWithPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1, tk2 := issue20975PreparePartitionTable(t, store)

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("update t1 set c=c")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")
}

func TestIssue20975SelectForUpdateWithPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk1, tk2 := issue20975PreparePartitionTable(t, store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")
}

func TestIssue20975SelectForUpdatePointGetWithPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1, tk2 := issue20975PreparePartitionTable(t, store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id=1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id=12 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id=1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id=12 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")
}

func TestIssue20975SelectForUpdateBatchPointGetWithPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk1, tk2 := issue20975PreparePartitionTable(t, store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id in (1, 2) for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id in (11, 12) for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")

	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id in (1, 11) for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id in (1, 2) for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id in (11, 12) for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id in (1, 11) for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")
}

func TestIssue20305(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t2 (a year(4))")
	tk.MustExec("insert into t2 values(69)")
	tk.MustQuery("select * from t2 where a <= 69").Check(testkit.Rows("2069"))
	// the following test is a regression test that matches MySQL's behavior.
	tk.MustExec("drop table if exists t3")
	tk.MustExec("CREATE TABLE `t3` (`y` year DEFAULT NULL, `a` int DEFAULT NULL)")
	tk.MustExec("INSERT INTO `t3` VALUES (2069, 70), (2010, 11), (2155, 2156), (2069, 69)")
	tk.MustQuery("SELECT * FROM `t3` where y <= a").Check(testkit.Rows("2155 2156"))
}

func TestIssue22817(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t3")
	tk.MustExec("create table t3 (a year)")
	tk.MustExec("insert into t3 values (1991), (\"1992\"), (\"93\"), (94)")
	tk.MustQuery("select * from t3 where a >= NULL").Check(testkit.Rows())
}

func TestIssue13953(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("CREATE TABLE `t` (`id` int(11) DEFAULT NULL, `tp_bigint` bigint(20) DEFAULT NULL )")
	tk.MustExec("insert into t values(0,1),(1,9215570218099803537)")
	tk.MustQuery("select A.tp_bigint,B.id from t A join t B on A.id < B.id * 16 where A.tp_bigint = B.id;").Check(
		testkit.Rows("1 1"))
}

func Test17780(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0")
	tk.MustExec("create table t0 (c0 double)")
	tk.MustExec("insert into t0 values (1e30)")
	tk.MustExec("update t0 set c0=0 where t0.c0 like 0")
	// the update should not affect c0
	tk.MustQuery("select count(*) from t0 where c0 = 0").Check(testkit.Rows("0"))
}

func TestIssue9918(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a year)")
	tk.MustExec("insert into t values(0)")
	tk.MustQuery("select cast(a as char) from t").Check(testkit.Rows("0000"))
}

func Test13004(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// see https://dev.mysql.com/doc/refman/5.6/en/date-and-time-literals.html, timestamp here actually produces a datetime
	tk.MustQuery("SELECT TIMESTAMP '9999-01-01 00:00:00'").Check(testkit.Rows("9999-01-01 00:00:00"))
}

func Test12178(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists ta")
	tk.MustExec("create table ta(id decimal(60,2))")
	tk.MustExec("insert into ta values (JSON_EXTRACT('{\"c\": \"1234567890123456789012345678901234567890123456789012345\"}', '$.c'))")
	tk.MustQuery("select * from ta").Check(testkit.Rows("1234567890123456789012345678901234567890123456789012345.00"))
}

func Test11883(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (f1 json)")
	tk.MustExec("insert into t1(f1) values ('\"asd\"'),('\"asdf\"'),('\"asasas\"')")
	tk.MustQuery("select f1 from t1 where json_extract(f1,\"$\") in (\"asd\",\"asasas\",\"asdf\")").Check(testkit.Rows("\"asd\"", "\"asdf\"", "\"asasas\""))
	tk.MustQuery("select f1 from t1 where json_extract(f1, '$') = 'asd'").Check(testkit.Rows("\"asd\""))
	// MySQL produces empty row for the following SQL, I doubt it should be MySQL's bug.
	tk.MustQuery("select f1 from t1 where case json_extract(f1,\"$\") when \"asd\" then 1 else 0 end").Check(testkit.Rows("\"asd\""))
	tk.MustExec("delete from t1")
	tk.MustExec("insert into t1 values ('{\"a\": 1}')")
	// the first value in the tuple should be interpreted as string instead of JSON, so no row will be returned
	tk.MustQuery("select f1 from t1 where f1 in ('{\"a\": 1}', 'asdf', 'asdf')").Check(testkit.Rows())
	// and if we explicitly cast it into a JSON value, the check will pass
	tk.MustQuery("select f1 from t1 where f1 in (cast('{\"a\": 1}' as JSON), 'asdf', 'asdf')").Check(testkit.Rows("{\"a\": 1}"))
	tk.MustQuery("select json_extract('\"asd\"', '$') = 'asd'").Check(testkit.Rows("1"))
	tk.MustQuery("select json_extract('\"asd\"', '$') <=> 'asd'").Check(testkit.Rows("1"))
	tk.MustQuery("select json_extract('\"asd\"', '$') <> 'asd'").Check(testkit.Rows("0"))
	tk.MustQuery("select json_extract('{\"f\": 1.0}', '$.f') = 1.0").Check(testkit.Rows("1"))
	tk.MustQuery("select json_extract('{\"f\": 1.0}', '$.f') = '1.0'").Check(testkit.Rows("0"))
	tk.MustQuery("select json_extract('{\"n\": 1}', '$') = '{\"n\": 1}'").Check(testkit.Rows("0"))
	tk.MustQuery("select json_extract('{\"n\": 1}', '$') <> '{\"n\": 1}'").Check(testkit.Rows("1"))
}

func Test15492(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("insert into t values (2, 20), (1, 10), (3, 30)")
	tk.MustQuery("select a + 1 as field1, a as field2 from t order by field1, field2 limit 2").Check(testkit.Rows("2 1", "3 2"))
}

func TestIssue23567(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	oriProbability := statistics.FeedbackProbability.Load()
	statistics.FeedbackProbability.Store(1.0)
	defer func() { statistics.FeedbackProbability.Store(oriProbability) }()
	failpoint.Enable("github.com/pingcap/tidb/statistics/feedbackNoNDVCollect", `return("")`)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a bigint unsigned, b int, primary key(a))")
	tk.MustExec("insert into t values (1, 1), (2, 2)")
	tk.MustExec("analyze table t")
	// The SQL should not panic.
	tk.MustQuery("select count(distinct b) from t")
	failpoint.Disable("github.com/pingcap/tidb/statistics/feedbackNoNDVCollect")
}

func TestIssue33038(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t1")
	tk.MustExec("create table t (id int, c int as (id))")
	tk.MustExec("begin")
	tk.MustExec("insert into t(id) values (1),(2),(3),(4)")
	tk.MustExec("insert into t(id) select id from t")
	tk.MustExec("insert into t(id) select id from t")
	tk.MustExec("insert into t(id) select id from t")
	tk.MustExec("insert into t(id) select id from t")
	tk.MustExec("insert into t(id) values (5)")
	tk.MustQuery("select * from t where c = 5").Check(testkit.Rows("5 5"))

	tk.MustExec("use test")
	tk.MustExec("set @@tidb_max_chunk_size=16")
	tk.MustExec("create table t1 (id int, c int as (id))")
	tk.MustExec("insert into t1(id) values (1),(2),(3),(4)")
	tk.MustExec("insert into t1(id) select id from t1")
	tk.MustExec("insert into t1(id) select id from t1")
	tk.MustExec("insert into t1(id) select id from t1")
	tk.MustExec("insert into t1(id) values (5)")
	tk.MustExec("alter table t1 cache")

	for {
		tk.MustQuery("select * from t1 where c = 5").Check(testkit.Rows("5 5"))
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			break
		}
	}
}

func TestIssue33214(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (col enum('a', 'b', 'c') default null)")
	tk.MustExec("insert into t values ('a'), ('b'), ('c'), (null), ('c')")
	tk.MustExec("alter table t cache")
	for {
		tk.MustQuery("select col from t t1 where (select count(*) from t t2 where t2.col = t1.col or t2.col =  'sdf') > 1;").Check(testkit.Rows("c", "c"))
		if tk.Session().GetSessionVars().StmtCtx.ReadFromTableCache {
			break
		}
	}
}

func TestIssue982(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table t (c int auto_increment, key(c)) auto_id_cache 1;")
	tk.MustExec("insert into t values();")
	tk.MustExec("insert into t values();")
	tk.MustQuery("select * from t;").Check(testkit.Rows("1", "2"))
}

func TestIssue24627(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	for _, sql := range []string{
		"create table test(id float primary key clustered AUTO_INCREMENT, col1 int);",
		"create table test(id float primary key nonclustered AUTO_INCREMENT, col1 int) AUTO_ID_CACHE 1;",
	} {
		tk.MustExec("drop table if exists test;")
		tk.MustExec(sql)
		tk.MustExec("replace into test(col1) values(1);")
		tk.MustExec("replace into test(col1) values(2);")
		tk.MustQuery("select * from test;").Check(testkit.Rows("1 1", "2 2"))
		tk.MustExec("drop table test")
	}

	for _, sql := range []string{
		"create table test2(id double primary key clustered AUTO_INCREMENT, col1 int);",
		"create table test2(id double primary key nonclustered AUTO_INCREMENT, col1 int) AUTO_ID_CACHE 1;",
	} {
		tk.MustExec(sql)
		tk.MustExec("replace into test2(col1) values(1);")
		tk.MustExec("insert into test2(col1) values(1);")
		tk.MustExec("replace into test2(col1) values(1);")
		tk.MustExec("insert into test2(col1) values(1);")
		tk.MustExec("replace into test2(col1) values(1);")
		tk.MustExec("replace into test2(col1) values(1);")
		tk.MustQuery("select * from test2").Check(testkit.Rows("1 1", "2 1", "3 1", "4 1", "5 1", "6 1"))
		tk.MustExec("drop table test2")
	}
}

func TestIssue39618(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1;")
	tk.MustExec(`CREATE TABLE t1 (
  c_int int(11) NOT NULL,
  c_str varbinary(40) NOT NULL,
  c_datetime datetime DEFAULT NULL,
  c_timestamp timestamp NULL DEFAULT NULL,
  c_double double DEFAULT NULL,
  c_decimal decimal(12,6) DEFAULT NULL,
  c_enum enum('blue','green','red','yellow','white','orange','purple') DEFAULT NULL,
  PRIMARY KEY (c_int,c_str) /*T![clustered_index] CLUSTERED */,
  KEY c_int_2 (c_int),
  KEY c_decimal (c_decimal),
  KEY c_datetime (c_datetime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY LIST COLUMNS(c_int)
(PARTITION p0 VALUES IN (1,5,9,13,17,21,25,29,33,37),
 PARTITION p1 VALUES IN (2,6,10,14,18,22,26,30,34,38),
 PARTITION p2 VALUES IN (3,7,11,15,19,23,27,31,35,39),
 PARTITION p3 VALUES IN (4,8,12,16,20,24,28,32,36,40));`)
	tk.MustExec("INSERT INTO t1 VALUES (3,'bold goldberg','2020-01-07 12:08:19','2020-06-19 08:13:35',0.941002,5.303000,'yellow'),(1,'crazy wescoff','2020-03-24 21:51:02','2020-06-19 08:13:35',47.565275,6.313000,'orange'),(5,'relaxed gagarin','2020-05-20 11:36:26','2020-06-19 08:13:35',38.948617,3.143000,'green'),(9,'gifted vaughan','2020-04-09 16:19:45','2020-06-19 08:13:35',95.922976,8.708000,'yellow'),(2,'focused taussig','2020-05-17 17:58:34','2020-06-19 08:13:35',4.137803,4.902000,'white'),(6,'fervent yonath','2020-05-26 03:55:25','2020-06-19 08:13:35',72.394272,6.491000,'white'),(18,'mystifying bhaskara','2020-02-19 10:41:48','2020-06-19 08:13:35',10.832397,9.707000,'red'),(4,'goofy saha','2020-03-11 13:24:31','2020-06-19 08:13:35',39.007216,2.446000,'blue'),(20,'mystifying bhaskara','2020-04-03 11:33:27','2020-06-19 08:13:35',85.190386,6.787000,'blue');")

	tk.MustExec("DROP TABLE IF EXISTS t2;")
	tk.MustExec(`CREATE TABLE t2 (
  c_int int(11) NOT NULL,
  c_str varbinary(40) NOT NULL,
  c_datetime datetime DEFAULT NULL,
  c_timestamp timestamp NULL DEFAULT NULL,
  c_double double DEFAULT NULL,
  c_decimal decimal(12,6) DEFAULT NULL,
  c_enum enum('blue','green','red','yellow','white','orange','purple') DEFAULT NULL,
  PRIMARY KEY (c_int,c_str) /*T![clustered_index] CLUSTERED */,
  KEY c_int_2 (c_int),
  KEY c_decimal (c_decimal),
  KEY c_datetime (c_datetime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
PARTITION BY LIST COLUMNS(c_int)
(PARTITION p0 VALUES IN (1,5,9,13,17,21,25,29,33,37),
 PARTITION p1 VALUES IN (2,6,10,14,18,22,26,30,34,38),
 PARTITION p2 VALUES IN (3,7,11,15,19,23,27,31,35,39),
 PARTITION p3 VALUES IN (4,8,12,16,20,24,28,32,36,40));
`)

	tk.MustExec("INSERT INTO t2 VALUES (1,'crazy wescoff','2020-03-24 21:51:02','2020-04-01 12:11:56',47.565275,6.313000,'orange'),(1,'unruffled johnson','2020-06-30 03:42:58','2020-06-14 00:16:50',35.444084,1.090000,'red'),(5,'relaxed gagarin','2020-05-20 11:36:26','2020-02-19 12:25:48',38.948617,3.143000,'green'),(9,'eloquent archimedes','2020-02-16 04:20:21','2020-05-23 15:42:33',32.310878,5.855000,'orange'),(9,'gifted vaughan','2020-04-09 16:19:45','2020-05-15 01:42:16',95.922976,8.708000,'yellow'),(13,'dreamy benz','2020-04-27 17:43:44','2020-03-27 06:33:03',39.539233,4.823000,'red'),(3,'bold goldberg','2020-01-07 12:08:19','2020-03-10 18:37:09',0.941002,5.303000,'yellow'),(3,'youthful yonath','2020-01-12 17:10:39','2020-06-10 15:13:44',66.288511,6.046000,'white'),(7,'upbeat bhabha','2020-04-29 01:17:05','2020-03-11 22:58:43',23.316987,9.026000,'yellow'),(11,'quizzical ritchie','2020-05-16 08:21:36','2020-03-05 19:23:25',75.019379,0.260000,'purple'),(2,'dazzling kepler','2020-04-11 04:38:59','2020-05-06 04:42:32',78.798503,2.274000,'purple'),(2,'focused taussig','2020-05-17 17:58:34','2020-02-25 09:11:03',4.137803,4.902000,'white'),(2,'sharp ptolemy',NULL,'2020-05-17 18:04:19',NULL,5.573000,'purple'),(6,'fervent yonath','2020-05-26 03:55:25','2020-05-06 14:23:44',72.394272,6.491000,'white'),(10,'musing wu','2020-04-03 11:33:27','2020-05-24 06:11:56',85.190386,6.787000,'blue'),(8,'hopeful keller','2020-02-19 10:41:48','2020-04-19 17:10:36',10.832397,9.707000,'red'),(12,'exciting boyd',NULL,'2020-03-28 18:27:23',NULL,9.249000,'blue');")

	tk.MustExec("set tidb_txn_assertion_level=strict;")
	tk.MustExec("begin")
	tk.MustExec("delete t1, t2 from t1, t2 where t1.c_enum in ('blue');")
	tk.MustExec("commit")
}

func TestIssue40158(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (_id int PRIMARY KEY, c1 char, index (c1));")
	tk.MustExec("insert into t1 values (1, null);")
	tk.MustQuery("select * from t1 where c1 is null and _id < 1;").Check(testkit.Rows())
}

func TestIssue40596(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec(`CREATE TABLE t1 (
  c1 double DEFAULT '1.335088259490289',
  c2 set('mj','4s7ht','z','3i','b26','9','cg11','uvzcp','c','ns','fl9') NOT NULL DEFAULT 'mj,z,3i,9,cg11,c',
  PRIMARY KEY (c2) /*T![clustered_index] CLUSTERED */,
  KEY i1 (c1),
  KEY i2 (c1),
  KEY i3 (c1)
) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci;`)
	tk.MustExec("INSERT INTO t1 VALUES (634.2783557491367,''),(2000.5041449792013,'4s7ht'),(634.2783557491367,'3i'),(634.2783557491367,'9'),(7803.173688589342,'uvzcp'),(634.2783557491367,'ns'),(634.2783557491367,'fl9');")
	tk.MustExec(`CREATE TABLE t2 (
  c3 decimal(56,16) DEFAULT '931359772706767457132645278260455518957.9866038319986886',
  c4 set('3bqx','g','6op3','2g','jf','arkd3','y0b','jdy','1g','ff5z','224b') DEFAULT '3bqx,2g,ff5z,224b',
  c5 smallint(6) NOT NULL DEFAULT '-25973',
  c6 year(4) DEFAULT '2122',
  c7 text DEFAULT NULL,
  PRIMARY KEY (c5) /*T![clustered_index] CLUSTERED */,
  KEY i4 (c6),
  KEY i5 (c5)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT=''
PARTITION BY HASH (c5) PARTITIONS 4;`)
	tk.MustExec("INSERT INTO t2 VALUES (465.0000000000000000,'jdy',-8542,2008,'FgZXe');")
	tk.MustExec("set @@sql_mode='';")
	tk.MustExec("set tidb_partition_prune_mode=dynamic;")
	tk.MustExec("analyze table t1;")
	tk.MustExec("analyze table t2;")

	// No nil pointer panic
	tk.MustQuery("select    /*+ inl_join( t1 , t2 ) */ avg(   t2.c5 ) as r0 , repeat( t2.c7 , t2.c5 ) as r1 , locate( t2.c7 , t2.c7 ) as r2 , unhex( t1.c1 ) as r3 from t1 right join t2 on t1.c2 = t2.c5 where not( t2.c5 in ( -7860 ,-13384 ,-12940 ) ) and not( t1.c2 between '4s7ht' and 'mj' );").Check(testkit.Rows("<nil> <nil> <nil> <nil>"))
	// Again, a simpler reproduce.
	tk.MustQuery("select /*+ inl_join (t1, t2) */ t2.c5 from t1 right join t2 on t1.c2 = t2.c5 where not( t1.c2 between '4s7ht' and 'mj' );").Check(testkit.Rows())
}

func TestIssueRaceWhenBuildingExecutorConcurrently(t *testing.T) {
	// issue: https://github.com/pingcap/tidb/issues/41412
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, c int, index idx_a(a), index idx_b(b))")
	for i := 0; i < 2000; i++ {
		v := i * 100
		tk.MustExec("insert into t values(?, ?, ?)", v, v, v)
	}
	tk.MustQuery("select /*+ inl_merge_join(t1, t2) */ * from t t1 right join t t2 on t1.a = t2.b and t1.c = t2.c")
}
