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

package executor_test

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/util"
	"github.com/stretchr/testify/require"
)

func TestIssue23993(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("SELECT HEX(WEIGHT_STRING('ab' AS BINARY(1000000000000000000)));").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1301 Result of cast_as_binary() was larger than max_allowed_packet (67108864) - truncated"))
	tk.MustQuery("SELECT HEX(WEIGHT_STRING('ab' AS char(1000000000000000000)));").Check(testkit.Rows("<nil>"))
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1301 Result of weight_string() was larger than max_allowed_packet (67108864) - truncated"))
}

func TestIssue22941(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustQuery("select greatest(cast(\"2020-01-01 01:01:01\" as datetime), cast(\"2019-01-01 01:01:01\" as datetime) )union select null;").Sort().Check(testkit.Rows("2020-01-01 01:01:01", "<nil>"))
	tk.MustQuery("select least(cast(\"2020-01-01 01:01:01\" as datetime), cast(\"2019-01-01 01:01:01\" as datetime) )union select null;").Sort().Check(testkit.Rows("2019-01-01 01:01:01", "<nil>"))
	tk.MustQuery("select greatest(\"2020-01-01 01:01:01\" ,\"2019-01-01 01:01:01\" )union select null;").Sort().Check(testkit.Rows("2020-01-01 01:01:01", "<nil>"))
	tk.MustQuery("select least(\"2020-01-01 01:01:01\" , \"2019-01-01 01:01:01\" )union select null;").Sort().Check(testkit.Rows("2019-01-01 01:01:01", "<nil>"))
}

func TestIssue25447(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMAction = config.OOMActionCancel
	})
	defer func() {
		config.UpdateGlobal(func(conf *config.Config) {
			conf.OOMAction = config.OOMActionLog
		})
	}()
	wg.Wait()
	for _, sql := range sqls {
		tk.MustExec("set @@tidb_mem_quota_query = 1073741824") // 1GB
		require.Nil(t, tk.QueryToErr(sql))
		tk.MustExec("set @@tidb_mem_quota_query = 33554432") // 32MB, out of memory during executing
		require.True(t, strings.Contains(tk.QueryToErr(sql).Error(), "Out Of Memory Quota!"))
		tk.MustExec("set @@tidb_mem_quota_query = 65536") // 64KB, out of memory during building the plan
		func() {
			defer func() {
				r := recover()
				require.NotNil(t, r)
				err := errors.Errorf("%v", r)
				require.True(t, strings.Contains(err.Error(), "Out Of Memory Quota!"))
			}()
			tk.MustExec(sql)
		}()
	}
}

func TestIssue30289(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	fpName := "github.com/pingcap/tidb/executor/issue30289"
	require.NoError(t, failpoint.Enable(fpName, `return(true)`))
	defer func() {
		require.NoError(t, failpoint.Disable(fpName))
	}()
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	err := tk.QueryToErr("select /*+ hash_join(t1) */ * from t t1 join t t2 on t1.a=t2.a")
	require.Regexp(t, "issue30289 build return error", err.Error())
}

func TestIssue29498(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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

func TestIndexJoin31494(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()
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
	sm := &mockSessionManager1{
		PS: make([]*util.ProcessInfo, 0),
	}
	tk.Session().SetSessionManager(sm)
	dom.ExpensiveQueryHandle().SetSessionManager(sm)
	defer config.RestoreFunc()()
	config.UpdateGlobal(func(conf *config.Config) {
		conf.OOMAction = config.OOMActionCancel
	})
	require.True(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil))
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
		conf.EnableCollectExecutionInfo = false
	})
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t123")
	tk.MustExec("create table t123 (id int);")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/copr/disable-collect-execution", `return(true)`))
	tk.MustQuery("select * from t123;")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/copr/disable-collect-execution"))
}

func TestFix31530(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
	store, clean := testkit.CreateMockStore(t)
	defer clean()
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
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
		"  │ └─StreamAgg 1.00 root  funcs:max(Column#17)->Column#7, funcs:count(distinct Column#18)->Column#8, funcs:sum(Column#19)->Column#9, funcs:count(1)->Column#10",
		"  │   └─Projection 3323.33 root  test.t2.c_str, test.t2.c_str, cast(isnull(test.t2.c_str), decimal(20,0) BINARY)->Column#19",
		"  │     └─TableReader 3323.33 root partition:all data:Selection",
		"  │       └─Selection 3323.33 cop[tikv]  lt(test.t2.c_decimal, 5)",
		"  │         └─TableFullScan 10000.00 cop[tikv] table:t2 keep order:false, stats:pseudo",
		"  └─TableReader(Probe) 8000.00 root partition:all data:Selection",
		"    └─Selection 8000.00 cop[tikv]  if(isnull(test.t1.c_str), NULL, 1)",
		"      └─TableFullScan 10000.00 cop[tikv] table:t1 keep order:false, stats:pseudo"))
	tk.MustExec("commit")
}
