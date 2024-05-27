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
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	_ "github.com/pingcap/tidb/pkg/autoid_service"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/stretchr/testify/require"
)

func TestIssue24210(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_cost_model_version=1")

	// for ProjectionExec
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockProjectionExecBaseExecutorOpenReturnedError", `return(true)`))
	err := tk.ExecToErr("select a from (select 1 as a, 2 as b) t")
	require.EqualError(t, err, "mock ProjectionExec.baseExecutor.Open returned error")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockProjectionExecBaseExecutorOpenReturnedError"))

	// for HashAggExec
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/mockHashAggExecBaseExecutorOpenReturnedError", `return(true)`))
	err = tk.ExecToErr("select sum(a) from (select 1 as a, 2 as b) t group by b")
	require.EqualError(t, err, "mock HashAggExec.baseExecutor.Open returned error")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/aggregate/mockHashAggExecBaseExecutorOpenReturnedError"))

	// for StreamAggExec
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/aggregate/mockStreamAggExecBaseExecutorOpenReturnedError", `return(true)`))
	err = tk.ExecToErr("select sum(a) from (select 1 as a, 2 as b) t")
	require.EqualError(t, err, "mock StreamAggExec.baseExecutor.Open returned error")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/aggregate/mockStreamAggExecBaseExecutorOpenReturnedError"))

	// for SelectionExec
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/mockSelectionExecBaseExecutorOpenReturnedError", `return(true)`))
	err = tk.ExecToErr("select * from (select rand() as a) t where a > 0")
	require.EqualError(t, err, "mock SelectionExec.baseExecutor.Open returned error")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/mockSelectionExecBaseExecutorOpenReturnedError"))
}

func TestUnionIssue(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// Issue25506
	tk.MustExec("use test")
	tk.MustExec("drop table if exists tbl_3, tbl_23")
	tk.MustExec("create table tbl_3 (col_15 bit(20))")
	tk.MustExec("insert into tbl_3 values (0xFFFF)")
	tk.MustExec("insert into tbl_3 values (0xFF)")
	tk.MustExec("create table tbl_23 (col_15 bit(15))")
	tk.MustExec("insert into tbl_23 values (0xF)")
	tk.MustQuery("(select col_15 from tbl_23) union all (select col_15 from tbl_3 for update) order by col_15").Check(testkit.Rows("\x00\x00\x0F", "\x00\x00\xFF", "\x00\xFF\xFF"))
	// Issue26532
	tk.MustQuery("select greatest(cast(\"2020-01-01 01:01:01\" as datetime), cast(\"2019-01-01 01:01:01\" as datetime) )union select null;").Sort().Check(testkit.Rows("2020-01-01 01:01:01", "<nil>"))
	tk.MustQuery("select least(cast(\"2020-01-01 01:01:01\" as datetime), cast(\"2019-01-01 01:01:01\" as datetime) )union select null;").Sort().Check(testkit.Rows("2019-01-01 01:01:01", "<nil>"))
	tk.MustQuery("select greatest(\"2020-01-01 01:01:01\" ,\"2019-01-01 01:01:01\" )union select null;").Sort().Check(testkit.Rows("2020-01-01 01:01:01", "<nil>"))
	tk.MustQuery("select least(\"2020-01-01 01:01:01\" , \"2019-01-01 01:01:01\" )union select null;").Sort().Check(testkit.Rows("2019-01-01 01:01:01", "<nil>"))
	// Issue30971
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
	// 31530
	tk2 := testkit.NewTestKit(t, store)
	tk2.MustExec("use test")
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
		require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(tk.QueryToErr(sql)))
		tk.MustExec("set @@tidb_mem_quota_query = 65536") // 64KB, out of memory during building the plan
		require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(tk.ExecToErr(sql)))
	}
}

func TestIssue30289(t *testing.T) {
	fpName := "github.com/pingcap/tidb/pkg/executor/join/issue30289"
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

func TestIssue51998(t *testing.T) {
	fpName := "github.com/pingcap/tidb/pkg/executor/join/issue51998"
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
	require.EqualError(t, err, "issue51998 build return error")
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
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("set @@tidb_mem_quota_query=2097152;")
	// This bug will be reproduced in 10 times.
	for i := 0; i < 10; i++ {
		err := tk.QueryToErr("select /*+ inl_join(t1) */ * from t1 right join t2 on t1.b=t2.b;")
		require.Error(t, err)
		require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(err))
		err = tk.QueryToErr("select /*+ inl_hash_join(t1) */ * from t1 right join t2 on t1.b=t2.b;")
		require.Error(t, err)
		require.True(t, exeerrors.ErrMemoryExceedForQuery.Equal(err))
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
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/store/copr/disable-collect-execution", `return(true)`))
	tk.MustQuery("select * from t123;")
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/store/copr/disable-collect-execution"))
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

func TestIssue20975(t *testing.T) {
	store := testkit.CreateMockStore(t)
	// UpdateNoChange
	tk1, tk2 := issue20975Prepare(t, store)
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("update t1 set c=c")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	// SelectForUpdate
	tk1, tk2 = issue20975Prepare(t, store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")

	// SelectForUpdatePointGet
	tk1, tk2 = issue20975Prepare(t, store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 where id=1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 where id=1 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")

	// SelectForUpdateBatchPointGet
	tk1, tk2 = issue20975Prepare(t, store)
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

func TestIssue20975WithPartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)

	// UpdateNoChange
	tk1, tk2 := issue20975PreparePartitionTable(t, store)
	tk1.MustExec("begin pessimistic")
	tk1.MustExec("update t1 set c=c")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	// SelectForUpdate
	tk1, tk2 = issue20975PreparePartitionTable(t, store)
	tk1.MustExec("begin")
	tk1.MustExec("select * from t1 for update")
	tk2.MustExec("create table t2(a int)")
	tk1.MustExec("commit")

	tk1.MustExec("begin pessimistic")
	tk1.MustExec("select * from t1 for update")
	tk2.MustExec("drop table t2")
	tk1.MustExec("commit")

	// SelectForUpdatePointGet
	tk1, tk2 = issue20975PreparePartitionTable(t, store)
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

	// SelectForUpdateBatchPointGet
	tk1, tk2 = issue20975PreparePartitionTable(t, store)
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

func TestIssue42298(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("alter table t add column b int")
	res := tk.MustQuery("admin show ddl job queries limit 268430000")
	require.Greater(t, len(res.Rows()), 0, len(res.Rows()))
	res = tk.MustQuery("admin show ddl job queries limit 999 offset 268430000")
	require.Zero(t, len(res.Rows()), len(res.Rows()))
}

func TestIssue42662(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().ConnectionID = 12345
	tk.Session().GetSessionVars().MemTracker = memory.NewTracker(memory.LabelForSession, -1)
	tk.Session().GetSessionVars().MemTracker.SessionID.Store(12345)
	tk.Session().GetSessionVars().MemTracker.IsRootTrackerOfSess = true

	sm := &testkit.MockSessionManager{
		PS: []*util.ProcessInfo{tk.Session().ShowProcess()},
	}
	sm.Conn = make(map[uint64]sessiontypes.Session)
	sm.Conn[tk.Session().GetSessionVars().ConnectionID] = tk.Session()
	dom.ServerMemoryLimitHandle().SetSessionManager(sm)
	go dom.ServerMemoryLimitHandle().Run()

	tk.MustExec("use test")
	tk.MustQuery("select connection_id()").Check(testkit.Rows("12345"))
	tk.MustExec("drop table if exists t1, t2")
	tk.MustExec("create table t1 (a int, b int, c int)")
	tk.MustExec("create table t2 (a int, b int, c int)")
	tk.MustExec("insert into t1 values (1, 1, 1), (1, 2, 2), (2, 1, 3), (2, 2, 4)")
	tk.MustExec("insert into t2 values (1, 1, 1), (1, 2, 2), (2, 1, 3), (2, 2, 4)")
	// set tidb_server_memory_limit to 1.6GB, tidb_server_memory_limit_sess_min_size to 128MB
	tk.MustExec("set global tidb_server_memory_limit='1600MB'")
	tk.MustExec("set global tidb_server_memory_limit_sess_min_size=128*1024*1024")
	tk.MustExec("set global tidb_mem_oom_action = 'cancel'")
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/executor/join/issue42662_1", `return(true)`))
	// tk.Session() should be marked as MemoryTop1Tracker but not killed.
	tk.MustQuery("select /*+ hash_join(t1)*/ * from t1 join t2 on t1.a = t2.a and t1.b = t2.b")

	// try to trigger the kill top1 logic
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/pkg/util/servermemorylimit/issue42662_2", `return(true)`))
	time.Sleep(1 * time.Second)

	// no error should be returned
	tk.MustQuery("select count(*) from t1")
	tk.MustQuery("select count(*) from t1")

	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/executor/join/issue42662_1"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/pkg/util/servermemorylimit/issue42662_2"))
}

func TestIssue50393(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1, t2")

	tk.MustExec("create table t1 (a blob)")
	tk.MustExec("create table t2 (a blob)")
	tk.MustExec("insert into t1 values (0xC2A0)")
	tk.MustExec("insert into t2 values (0xC2)")
	tk.MustQuery("select count(*) from t1,t2 where t1.a like concat(\"%\",t2.a,\"%\")").Check(testkit.Rows("1"))
}

func TestIssue51874(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().AllowProjectionPushDown = true

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t, t2")
	tk.MustExec("create table t (a int, b int)")
	tk.MustExec("create table t2 (i int)")
	tk.MustExec("insert into t values (5, 6), (1, 7)")
	tk.MustExec("insert into t2 values (10), (100)")
	tk.MustQuery("select (select sum(a) over () from t2 limit 1) from t;").Check(testkit.Rows("10", "2"))
}

func TestIssue51777(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.Session().GetSessionVars().AllowProjectionPushDown = true

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0, t1")
	tk.MustExec("create table t0 (c_k int)")
	tk.MustExec("create table t1 (c_pv int)")
	tk.MustExec("insert into t0 values(-2127559046),(-190905159),(-171305020),(-59638845),(98004414),(2111663670),(2137868682),(2137868682),(2142611610)")
	tk.MustExec("insert into t1 values(-2123227448), (2131706870), (-2071508387), (2135465388), (2052805244), (-2066000113)")
	tk.MustQuery("SELECT ( select  (ref_4.c_pv <= ref_3.c_k) as c0 from t1 as ref_4 order by c0 asc limit 1) as p2 FROM t0 as ref_3 order by p2;").Check(testkit.Rows("0", "0", "0", "0", "0", "0", "1", "1", "1"))
}

func TestIssue52978(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (-1790816583),(2049821819), (-1366665321), (536581933), (-1613686445)")
	tk.MustQuery("select min(truncate(cast(-26340 as double), ref_11.a)) as c3 from t as ref_11;").Check(testkit.Rows("-26340"))
	tk.MustExec("drop table if exists t")
}

func TestIssue53221(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a varchar(20))")
	tk.MustExec("insert into t values ('')")
	tk.MustExec("insert into t values ('')")
	err := tk.QueryToErr("select regexp_like('hello', t.a) from test.t")
	require.ErrorContains(t, err, "Empty pattern is invalid")

	err = tk.QueryToErr("select regexp_instr('hello', t.a) from test.t")
	require.ErrorContains(t, err, "Empty pattern is invalid")

	err = tk.QueryToErr("select regexp_substr('hello', t.a) from test.t")
	require.ErrorContains(t, err, "Empty pattern is invalid")

	err = tk.QueryToErr("select regexp_replace('hello', t.a, 'd') from test.t")
	require.ErrorContains(t, err, "Empty pattern is invalid")

	tk.MustExec("drop table if exists t")
}
