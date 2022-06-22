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

package bindinfo_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/testkit"
	utilparser "github.com/pingcap/tidb/util/parser"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func utilCleanBindingEnv(tk *testkit.TestKit, dom *domain.Domain) {
	tk.MustExec("delete from mysql.bind_info where source != 'builtin'")
	dom.BindHandle().Clear()
}

func utilNormalizeWithDefaultDB(t *testing.T, sql, db string) (string, string) {
	testParser := parser.New()
	stmt, err := testParser.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	normalized, digest := parser.NormalizeDigest(utilparser.RestoreWithDefaultDB(stmt, "test", ""))
	return normalized, digest.String()
}

func TestBindingCache(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx);")
	tk.MustExec("create database tmp")
	tk.MustExec("use tmp")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx);")

	require.Nil(t, dom.BindHandle().Update(false))
	require.Nil(t, dom.BindHandle().Update(false))
	res := tk.MustQuery("show global bindings")
	require.Equal(t, 2, len(res.Rows()))

	tk.MustExec("drop global binding for select * from t;")
	require.Nil(t, dom.BindHandle().Update(false))
	require.Equal(t, 1, len(dom.BindHandle().GetAllBindRecord()))
}

func TestBindingLastUpdateTime(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0;")
	tk.MustExec("create table t0(a int, key(a));")
	tk.MustExec("create global binding for select * from t0 using select * from t0 use index(a);")
	tk.MustExec("admin reload bindings;")

	bindHandle := bindinfo.NewBindHandle(tk.Session())
	err := bindHandle.Update(true)
	require.NoError(t, err)
	sql, hash := parser.NormalizeDigest("select * from test . t0")
	bindData := bindHandle.GetBindRecord(hash.String(), sql, "test")
	require.Equal(t, 1, len(bindData.Bindings))
	bind := bindData.Bindings[0]
	updateTime := bind.UpdateTime.String()

	rows1 := tk.MustQuery("show status like 'last_plan_binding_update_time';").Rows()
	updateTime1 := rows1[0][1]
	require.Equal(t, updateTime, updateTime1)

	rows2 := tk.MustQuery("show session status like 'last_plan_binding_update_time';").Rows()
	updateTime2 := rows2[0][1]
	require.Equal(t, updateTime, updateTime2)
	tk.MustQuery(`show global status like 'last_plan_binding_update_time';`).Check(testkit.Rows())
}

func TestBindingLastUpdateTimeWithInvalidBind(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	rows0 := tk.MustQuery("show status like 'last_plan_binding_update_time';").Rows()
	updateTime0 := rows0[0][1]
	require.Equal(t, updateTime0, "0000-00-00 00:00:00")

	tk.MustExec("insert into mysql.bind_info values('select * from `test` . `t`', 'select * from `test` . `t` use index(`idx`)', 'test', 'enabled', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int)")
	tk.MustExec("admin reload bindings;")

	rows1 := tk.MustQuery("show status like 'last_plan_binding_update_time';").Rows()
	updateTime1 := rows1[0][1]
	require.Equal(t, updateTime1, "2000-01-01 09:00:00.000")

	rows2 := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows2, 0)
}

func TestBindParse(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("create table t(i int)")
	tk.MustExec("create index index_t on t(i)")

	originSQL := "select * from `test` . `t`"
	bindSQL := "select * from `test` . `t` use index(index_t)"
	defaultDb := "test"
	status := bindinfo.Enabled
	charset := "utf8mb4"
	collation := "utf8mb4_bin"
	source := bindinfo.Manual
	sql := fmt.Sprintf(`INSERT INTO mysql.bind_info(original_sql,bind_sql,default_db,status,create_time,update_time,charset,collation,source) VALUES ('%s', '%s', '%s', '%s', NOW(), NOW(),'%s', '%s', '%s')`,
		originSQL, bindSQL, defaultDb, status, charset, collation, source)
	tk.MustExec(sql)
	bindHandle := bindinfo.NewBindHandle(tk.Session())
	err := bindHandle.Update(true)
	require.NoError(t, err)
	require.Equal(t, 1, bindHandle.Size())

	sql, hash := parser.NormalizeDigest("select * from test . t")
	bindData := bindHandle.GetBindRecord(hash.String(), sql, "test")
	require.NotNil(t, bindData)
	require.Equal(t, "select * from `test` . `t`", bindData.OriginalSQL)
	bind := bindData.Bindings[0]
	require.Equal(t, "select * from `test` . `t` use index(index_t)", bind.BindSQL)
	require.Equal(t, "test", bindData.Db)
	require.Equal(t, bindinfo.Enabled, bind.Status)
	require.Equal(t, "utf8mb4", bind.Charset)
	require.Equal(t, "utf8mb4_bin", bind.Collation)
	require.NotNil(t, bind.CreateTime)
	require.NotNil(t, bind.UpdateTime)
	dur, err := bind.SinceUpdateTime()
	require.NoError(t, err)
	require.GreaterOrEqual(t, int64(dur), int64(0))

	// Test fields with quotes or slashes.
	sql = `CREATE GLOBAL BINDING FOR  select * from t where i BETWEEN "a" and "b" USING select * from t use index(index_t) where i BETWEEN "a\nb\rc\td\0e" and 'x'`
	tk.MustExec(sql)
	tk.MustExec(`DROP global binding for select * from t use index(idx) where i BETWEEN "a\nb\rc\td\0e" and "x"`)

	// Test SetOprStmt.
	tk.MustExec(`create binding for select * from t union all select * from t using select * from t use index(index_t) union all select * from t use index()`)
	tk.MustExec(`drop binding for select * from t union all select * from t using select * from t use index(index_t) union all select * from t use index()`)
	tk.MustExec(`create binding for select * from t INTERSECT select * from t using select * from t use index(index_t) INTERSECT select * from t use index()`)
	tk.MustExec(`drop binding for select * from t INTERSECT select * from t using select * from t use index(index_t) INTERSECT select * from t use index()`)
	tk.MustExec(`create binding for select * from t EXCEPT select * from t using select * from t use index(index_t) EXCEPT select * from t use index()`)
	tk.MustExec(`drop binding for select * from t EXCEPT select * from t using select * from t use index(index_t) EXCEPT select * from t use index()`)
	tk.MustExec(`create binding for (select * from t) union all (select * from t) using (select * from t use index(index_t)) union all (select * from t use index())`)
	tk.MustExec(`drop binding for (select * from t) union all (select * from t) using (select * from t use index(index_t)) union all (select * from t use index())`)

	// Test Update / Delete.
	tk.MustExec("create table t1(a int, b int, c int, key(b), key(c))")
	tk.MustExec("create table t2(a int, b int, c int, key(b), key(c))")
	tk.MustExec("create binding for delete from t1 where b = 1 and c > 1 using delete /*+ use_index(t1, c) */ from t1 where b = 1 and c > 1")
	tk.MustExec("drop binding for delete from t1 where b = 1 and c > 1 using delete /*+ use_index(t1, c) */ from t1 where b = 1 and c > 1")
	tk.MustExec("create binding for delete t1, t2 from t1 inner join t2 on t1.b = t2.b where t1.c = 1 using delete /*+ hash_join(t1, t2), use_index(t1, c) */ t1, t2 from t1 inner join t2 on t1.b = t2.b where t1.c = 1")
	tk.MustExec("drop binding for delete t1, t2 from t1 inner join t2 on t1.b = t2.b where t1.c = 1 using delete /*+ hash_join(t1, t2), use_index(t1, c) */ t1, t2 from t1 inner join t2 on t1.b = t2.b where t1.c = 1")
	tk.MustExec("create binding for update t1 set a = 1 where b = 1 and c > 1 using update /*+ use_index(t1, c) */ t1 set a = 1 where b = 1 and c > 1")
	tk.MustExec("drop binding for update t1 set a = 1 where b = 1 and c > 1 using update /*+ use_index(t1, c) */ t1 set a = 1 where b = 1 and c > 1")
	tk.MustExec("create binding for update t1, t2 set t1.a = 1 where t1.b = t2.b using update /*+ inl_join(t1) */ t1, t2 set t1.a = 1 where t1.b = t2.b")
	tk.MustExec("drop binding for update t1, t2 set t1.a = 1 where t1.b = t2.b using update /*+ inl_join(t1) */ t1, t2 set t1.a = 1 where t1.b = t2.b")
	// Test Insert / Replace.
	tk.MustExec("create binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1 using insert into t1 select /*+ use_index(t2,c) */ * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("drop binding for insert into t1 select * from t2 where t2.b = 1 and t2.c > 1 using insert into t1 select /*+ use_index(t2,c) */ * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("create binding for replace into t1 select * from t2 where t2.b = 1 and t2.c > 1 using replace into t1 select /*+ use_index(t2,c) */ * from t2 where t2.b = 1 and t2.c > 1")
	tk.MustExec("drop binding for replace into t1 select * from t2 where t2.b = 1 and t2.c > 1 using replace into t1 select /*+ use_index(t2,c) */ * from t2 where t2.b = 1 and t2.c > 1")
	err = tk.ExecToErr("create binding for insert into t1 values(1,1,1) using insert into t1 values(1,1,1)")
	require.Equal(t, "create binding only supports INSERT / REPLACE INTO SELECT", err.Error())
	err = tk.ExecToErr("create binding for replace into t1 values(1,1,1) using replace into t1 values(1,1,1)")
	require.Equal(t, "create binding only supports INSERT / REPLACE INTO SELECT", err.Error())

	// Test errors.
	tk.MustExec(`drop table if exists t1`)
	tk.MustExec("create table t1(i int, s varchar(20))")
	_, err = tk.Exec("create global binding for select * from t using select * from t1 use index for join(index_t)")
	require.NotNil(t, err, "err %v", err)
}

func TestEvolveInvalidBindings(t *testing.T) {
	originalVal := config.CheckTableBeforeDrop
	config.CheckTableBeforeDrop = true
	defer func() {
		config.CheckTableBeforeDrop = originalVal
	}()

	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx_a(a))")
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ USE_INDEX(t) */ * from t where a > 10")
	// Manufacture a rejected binding by hacking mysql.bind_info.
	tk.MustExec("insert into mysql.bind_info values('select * from test . t where a > ?', 'SELECT /*+ USE_INDEX(t,idx_a) */ * FROM test.t WHERE a > 10', 'test', 'rejected', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustQuery("select bind_sql, status from mysql.bind_info where source != 'builtin'").Sort().Check(testkit.Rows(
		"SELECT /*+ USE_INDEX(`t` )*/ * FROM `test`.`t` WHERE `a` > 10 enabled",
		"SELECT /*+ USE_INDEX(t,idx_a) */ * FROM test.t WHERE a > 10 rejected",
	))
	// Reload cache from mysql.bind_info.
	dom.BindHandle().Clear()
	require.Nil(t, dom.BindHandle().Update(true))

	tk.MustExec("alter table t drop index idx_a")
	tk.MustExec("admin evolve bindings")
	require.Nil(t, dom.BindHandle().Update(false))
	rows := tk.MustQuery("show global bindings").Sort().Rows()
	require.Equal(t, 2, len(rows))
	// Make sure this "enabled" binding is not overrided.
	require.Equal(t, "SELECT /*+ USE_INDEX(`t` )*/ * FROM `test`.`t` WHERE `a` > 10", rows[0][1])
	status := rows[0][3].(string)
	require.True(t, status == bindinfo.Enabled)
	require.Equal(t, "SELECT /*+ USE_INDEX(t,idx_a) */ * FROM test.t WHERE a > 10", rows[1][1])
	status = rows[1][3].(string)
	require.True(t, status == bindinfo.Enabled || status == bindinfo.Rejected)
}

func TestSetBindingStatus(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx_a(a))")
	tk.MustQuery("show global bindings").Check(testkit.Rows())
	tk.MustExec("create global binding for select * from t where a > 10 using select /*+ USE_INDEX(t, idx_a) */ * from t where a > 10")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Enabled, rows[0][3])
	tk.MustExec("select * from t where a > 10")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))

	tk.MustExec("set binding disabled for select * from t where a > 10 using select * from t where a > 10")
	tk.MustQuery("show warnings").Check(testkit.Rows("Warning 1105 There are no bindings can be set the status. Please check the SQL text"))
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Enabled, rows[0][3])
	tk.MustExec("select * from t where a > 10")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))

	tk.MustExec("set binding disabled for select * from t where a > 10")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Disabled, rows[0][3])
	tk.MustExec("select * from t where a > 10")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))

	tk.MustExec("set binding enabled for select * from t where a > 10")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Enabled, rows[0][3])

	tk.MustExec("set binding disabled for select * from t where a > 10")
	tk.MustExec("create global binding for select * from t where a > 10 using select * from t where a > 10")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Enabled, rows[0][3])
	tk.MustExec("select * from t where a > 10")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))

	tk.MustExec("set binding disabled for select * from t where a > 10 using select * from t where a > 10")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Disabled, rows[0][3])
	tk.MustExec("select * from t where a > 10")
	tk.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("0"))

	tk.MustExec("set binding enabled for select * from t where a > 10 using select * from t where a > 10")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Enabled, rows[0][3])

	tk.MustExec("set binding disabled for select * from t where a > 10 using select * from t where a > 10")
	tk.MustExec("drop global binding for select * from t where a > 10 using select * from t where a > 10")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 0)
}

func TestSetBindingStatusWithoutBindingInCache(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx_a(a))")
	utilCleanBindingEnv(tk, dom)
	tk.MustQuery("show global bindings").Check(testkit.Rows())

	// Simulate creating bindings on other machines
	tk.MustExec("insert into mysql.bind_info values('select * from `test` . `t` where `a` > ?', 'SELECT /*+ USE_INDEX(`t` `idx_a`)*/ * FROM `test`.`t` WHERE `a` > 10', 'test', 'deleted', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustExec("insert into mysql.bind_info values('select * from `test` . `t` where `a` > ?', 'SELECT /*+ USE_INDEX(`t` `idx_a`)*/ * FROM `test`.`t` WHERE `a` > 10', 'test', 'enabled', '2000-01-02 09:00:00', '2000-01-02 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	dom.BindHandle().Clear()
	tk.MustExec("set binding disabled for select * from t where a > 10")
	tk.MustExec("admin reload bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Disabled, rows[0][3])

	// clear the mysql.bind_info
	utilCleanBindingEnv(tk, dom)

	// Simulate creating bindings on other machines
	tk.MustExec("insert into mysql.bind_info values('select * from `test` . `t` where `a` > ?', 'SELECT * FROM `test`.`t` WHERE `a` > 10', 'test', 'deleted', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	tk.MustExec("insert into mysql.bind_info values('select * from `test` . `t` where `a` > ?', 'SELECT * FROM `test`.`t` WHERE `a` > 10', 'test', 'disabled', '2000-01-02 09:00:00', '2000-01-02 09:00:00', '', '','" +
		bindinfo.Manual + "')")
	dom.BindHandle().Clear()
	tk.MustExec("set binding enabled for select * from t where a > 10")
	tk.MustExec("admin reload bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Enabled, rows[0][3])

	utilCleanBindingEnv(tk, dom)
}

var testSQLs = []struct {
	createSQL   string
	overlaySQL  string
	querySQL    string
	originSQL   string
	bindSQL     string
	dropSQL     string
	memoryUsage float64
}{
	{
		createSQL:   "binding for select * from t where i>100 using select * from t use index(index_t) where i>100",
		overlaySQL:  "binding for select * from t where i>99 using select * from t use index(index_t) where i>99",
		querySQL:    "select * from t where i          >      30.0",
		originSQL:   "select * from `test` . `t` where `i` > ?",
		bindSQL:     "SELECT * FROM `test`.`t` USE INDEX (`index_t`) WHERE `i` > 99",
		dropSQL:     "binding for select * from t where i>100",
		memoryUsage: float64(167),
	},
	{
		createSQL:   "binding for select * from t union all select * from t using select * from t use index(index_t) union all select * from t use index()",
		overlaySQL:  "",
		querySQL:    "select * from t union all         select * from t",
		originSQL:   "select * from `test` . `t` union all select * from `test` . `t`",
		bindSQL:     "SELECT * FROM `test`.`t` USE INDEX (`index_t`) UNION ALL SELECT * FROM `test`.`t` USE INDEX ()",
		dropSQL:     "binding for select * from t union all select * from t",
		memoryUsage: float64(237),
	},
	{
		createSQL:   "binding for (select * from t) union all (select * from t) using (select * from t use index(index_t)) union all (select * from t use index())",
		overlaySQL:  "",
		querySQL:    "(select * from t) union all         (select * from t)",
		originSQL:   "( select * from `test` . `t` ) union all ( select * from `test` . `t` )",
		bindSQL:     "(SELECT * FROM `test`.`t` USE INDEX (`index_t`)) UNION ALL (SELECT * FROM `test`.`t` USE INDEX ())",
		dropSQL:     "binding for (select * from t) union all (select * from t)",
		memoryUsage: float64(249),
	},
	{
		createSQL:   "binding for select * from t intersect select * from t using select * from t use index(index_t) intersect select * from t use index()",
		overlaySQL:  "",
		querySQL:    "select * from t intersect         select * from t",
		originSQL:   "select * from `test` . `t` intersect select * from `test` . `t`",
		bindSQL:     "SELECT * FROM `test`.`t` USE INDEX (`index_t`) INTERSECT SELECT * FROM `test`.`t` USE INDEX ()",
		dropSQL:     "binding for select * from t intersect select * from t",
		memoryUsage: float64(237),
	},
	{
		createSQL:   "binding for select * from t except select * from t using select * from t use index(index_t) except select * from t use index()",
		overlaySQL:  "",
		querySQL:    "select * from t except         select * from t",
		originSQL:   "select * from `test` . `t` except select * from `test` . `t`",
		bindSQL:     "SELECT * FROM `test`.`t` USE INDEX (`index_t`) EXCEPT SELECT * FROM `test`.`t` USE INDEX ()",
		dropSQL:     "binding for select * from t except select * from t",
		memoryUsage: float64(231),
	},
	{
		createSQL:   "binding for select * from t using select /*+ use_index(t,index_t)*/ * from t",
		overlaySQL:  "",
		querySQL:    "select * from t ",
		originSQL:   "select * from `test` . `t`",
		bindSQL:     "SELECT /*+ use_index(`t` `index_t`)*/ * FROM `test`.`t`",
		dropSQL:     "binding for select * from t",
		memoryUsage: float64(166),
	},
	{
		createSQL:   "binding for delete from t where i = 1 using delete /*+ use_index(t,index_t) */ from t where i = 1",
		overlaySQL:  "",
		querySQL:    "delete    from t where   i = 2",
		originSQL:   "delete from `test` . `t` where `i` = ?",
		bindSQL:     "DELETE /*+ use_index(`t` `index_t`)*/ FROM `test`.`t` WHERE `i` = 1",
		dropSQL:     "binding for delete from t where i = 1",
		memoryUsage: float64(190),
	},
	{
		createSQL:   "binding for delete t, t1 from t inner join t1 on t.s = t1.s where t.i = 1 using delete /*+ use_index(t,index_t), hash_join(t,t1) */ t, t1 from t inner join t1 on t.s = t1.s where t.i = 1",
		overlaySQL:  "",
		querySQL:    "delete t,   t1 from t inner join t1 on t.s = t1.s  where   t.i = 2",
		originSQL:   "delete `test` . `t` , `test` . `t1` from `test` . `t` join `test` . `t1` on `t` . `s` = `t1` . `s` where `t` . `i` = ?",
		bindSQL:     "DELETE /*+ use_index(`t` `index_t`) hash_join(`t`, `t1`)*/ `test`.`t`,`test`.`t1` FROM `test`.`t` JOIN `test`.`t1` ON `t`.`s` = `t1`.`s` WHERE `t`.`i` = 1",
		dropSQL:     "binding for delete t, t1 from t inner join t1 on t.s = t1.s where t.i = 1",
		memoryUsage: float64(402),
	},
	{
		createSQL:   "binding for update t set s = 'a' where i = 1 using update /*+ use_index(t,index_t) */ t set s = 'a' where i = 1",
		overlaySQL:  "",
		querySQL:    "update   t  set s='b' where i=2",
		originSQL:   "update `test` . `t` set `s` = ? where `i` = ?",
		bindSQL:     "UPDATE /*+ use_index(`t` `index_t`)*/ `test`.`t` SET `s`='a' WHERE `i` = 1",
		dropSQL:     "binding for update t set s = 'a' where i = 1",
		memoryUsage: float64(204),
	},
	{
		createSQL:   "binding for update t, t1 set t.s = 'a' where t.i = t1.i using update /*+ inl_join(t1) */ t, t1 set t.s = 'a' where t.i = t1.i",
		overlaySQL:  "",
		querySQL:    "update   t  , t1 set t.s='b' where t.i=t1.i",
		originSQL:   "update ( `test` . `t` ) join `test` . `t1` set `t` . `s` = ? where `t` . `i` = `t1` . `i`",
		bindSQL:     "UPDATE /*+ inl_join(`t1`)*/ (`test`.`t`) JOIN `test`.`t1` SET `t`.`s`='a' WHERE `t`.`i` = `t1`.`i`",
		dropSQL:     "binding for update t, t1 set t.s = 'a' where t.i = t1.i",
		memoryUsage: float64(262),
	},
	{
		createSQL:   "binding for insert into t1 select * from t where t.i = 1 using insert into t1 select /*+ use_index(t,index_t) */ * from t where t.i = 1",
		overlaySQL:  "",
		querySQL:    "insert  into   t1 select * from t where t.i  = 2",
		originSQL:   "insert into `test` . `t1` select * from `test` . `t` where `t` . `i` = ?",
		bindSQL:     "INSERT INTO `test`.`t1` SELECT /*+ use_index(`t` `index_t`)*/ * FROM `test`.`t` WHERE `t`.`i` = 1",
		dropSQL:     "binding for insert into t1 select * from t where t.i = 1",
		memoryUsage: float64(254),
	},
	{
		createSQL:   "binding for replace into t1 select * from t where t.i = 1 using replace into t1 select /*+ use_index(t,index_t) */ * from t where t.i = 1",
		overlaySQL:  "",
		querySQL:    "replace  into   t1 select * from t where t.i  = 2",
		originSQL:   "replace into `test` . `t1` select * from `test` . `t` where `t` . `i` = ?",
		bindSQL:     "REPLACE INTO `test`.`t1` SELECT /*+ use_index(`t` `index_t`)*/ * FROM `test`.`t` WHERE `t`.`i` = 1",
		dropSQL:     "binding for replace into t1 select * from t where t.i = 1",
		memoryUsage: float64(256),
	},
}

func TestGlobalBinding(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	for _, testSQL := range testSQLs {
		utilCleanBindingEnv(tk, dom)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t(i int, s varchar(20))")
		tk.MustExec("create table t1(i int, s varchar(20))")
		tk.MustExec("create index index_t on t(i,s)")

		metrics.BindTotalGauge.Reset()
		metrics.BindMemoryUsage.Reset()

		_, err := tk.Exec("create global " + testSQL.createSQL)
		require.NoError(t, err, "err %v", err)

		if testSQL.overlaySQL != "" {
			_, err = tk.Exec("create global " + testSQL.overlaySQL)
			require.NoError(t, err)
		}

		pb := &dto.Metric{}
		err = metrics.BindTotalGauge.WithLabelValues(metrics.ScopeGlobal, bindinfo.Enabled).Write(pb)
		require.NoError(t, err)
		require.Equal(t, float64(1), pb.GetGauge().GetValue())
		err = metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeGlobal, bindinfo.Enabled).Write(pb)
		require.NoError(t, err)
		require.Equal(t, testSQL.memoryUsage, pb.GetGauge().GetValue())

		sql, hash := utilNormalizeWithDefaultDB(t, testSQL.querySQL, "test")

		bindData := dom.BindHandle().GetBindRecord(hash, sql, "test")
		require.NotNil(t, bindData)
		require.Equal(t, testSQL.originSQL, bindData.OriginalSQL)
		bind := bindData.Bindings[0]
		require.Equal(t, testSQL.bindSQL, bind.BindSQL)
		require.Equal(t, "test", bindData.Db)
		require.Equal(t, bindinfo.Enabled, bind.Status)
		require.NotNil(t, bind.Charset)
		require.NotNil(t, bind.Collation)
		require.NotNil(t, bind.CreateTime)
		require.NotNil(t, bind.UpdateTime)

		rs, err := tk.Exec("show global bindings")
		require.NoError(t, err)
		chk := rs.NewChunk(nil)
		err = rs.Next(context.TODO(), chk)
		require.NoError(t, err)
		require.Equal(t, 1, chk.NumRows())
		row := chk.GetRow(0)
		require.Equal(t, testSQL.originSQL, row.GetString(0))
		require.Equal(t, testSQL.bindSQL, row.GetString(1))
		require.Equal(t, "test", row.GetString(2))
		require.Equal(t, bindinfo.Enabled, row.GetString(3))
		require.NotNil(t, row.GetTime(4))
		require.NotNil(t, row.GetTime(5))
		require.NotNil(t, row.GetString(6))
		require.NotNil(t, row.GetString(7))

		bindHandle := bindinfo.NewBindHandle(tk.Session())
		err = bindHandle.Update(true)
		require.NoError(t, err)
		require.Equal(t, 1, bindHandle.Size())

		bindData = bindHandle.GetBindRecord(hash, sql, "test")
		require.NotNil(t, bindData)
		require.Equal(t, testSQL.originSQL, bindData.OriginalSQL)
		bind = bindData.Bindings[0]
		require.Equal(t, testSQL.bindSQL, bind.BindSQL)
		require.Equal(t, "test", bindData.Db)
		require.Equal(t, bindinfo.Enabled, bind.Status)
		require.NotNil(t, bind.Charset)
		require.NotNil(t, bind.Collation)
		require.NotNil(t, bind.CreateTime)
		require.NotNil(t, bind.UpdateTime)

		_, err = tk.Exec("drop global " + testSQL.dropSQL)
		require.NoError(t, err)
		bindData = dom.BindHandle().GetBindRecord(hash, sql, "test")
		require.Nil(t, bindData)

		err = metrics.BindTotalGauge.WithLabelValues(metrics.ScopeGlobal, bindinfo.Enabled).Write(pb)
		require.NoError(t, err)
		require.Equal(t, float64(0), pb.GetGauge().GetValue())
		err = metrics.BindMemoryUsage.WithLabelValues(metrics.ScopeGlobal, bindinfo.Enabled).Write(pb)
		require.NoError(t, err)
		// From newly created global bind handle.
		require.Equal(t, testSQL.memoryUsage, pb.GetGauge().GetValue())

		bindHandle = bindinfo.NewBindHandle(tk.Session())
		err = bindHandle.Update(true)
		require.NoError(t, err)
		require.Equal(t, 0, bindHandle.Size())

		bindData = bindHandle.GetBindRecord(hash, sql, "test")
		require.Nil(t, bindData)

		rs, err = tk.Exec("show global bindings")
		require.NoError(t, err)
		chk = rs.NewChunk(nil)
		err = rs.Next(context.TODO(), chk)
		require.NoError(t, err)
		require.Equal(t, 0, chk.NumRows())

		_, err = tk.Exec("delete from mysql.bind_info where source != 'builtin'")
		require.NoError(t, err)
	}
}

func TestOutdatedInfoSchema(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx)")
	require.Nil(t, dom.BindHandle().Update(false))
	utilCleanBindingEnv(tk, dom)
	tk.MustExec("create global binding for select * from t using select * from t use index(idx)")
}

func TestReloadBindings(t *testing.T) {
	store, dom, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx)")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Equal(t, 1, len(rows))
	rows = tk.MustQuery("select * from mysql.bind_info where source != 'builtin'").Rows()
	require.Equal(t, 1, len(rows))
	tk.MustExec("delete from mysql.bind_info where source != 'builtin'")
	require.Nil(t, dom.BindHandle().Update(false))
	rows = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, 1, len(rows))
	require.Nil(t, dom.BindHandle().Update(true))
	rows = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, 1, len(rows))
	tk.MustExec("admin reload bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, 0, len(rows))
}
