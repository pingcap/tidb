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

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/bindinfo/internal"
	"github.com/pingcap/tidb/pkg/bindinfo/norm"
	"github.com/pingcap/tidb/pkg/parser"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestBindingCache(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx);")
	tk.MustExec("create database tmp")
	tk.MustExec("use tmp")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx);")

	require.Nil(t, dom.BindHandle().LoadFromStorageToCache(false))
	require.Nil(t, dom.BindHandle().LoadFromStorageToCache(false))
	res := tk.MustQuery("show global bindings")
	require.Equal(t, 2, len(res.Rows()))

	tk.MustExec("drop global binding for select * from t;")
	require.Nil(t, dom.BindHandle().LoadFromStorageToCache(false))
	require.Equal(t, 1, len(dom.BindHandle().GetAllGlobalBindings()))
}

func TestBindingLastUpdateTime(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t0;")
	tk.MustExec("create table t0(a int, key(a));")
	tk.MustExec("create global binding for select * from t0 using select * from t0 use index(a);")
	tk.MustExec("admin reload bindings;")

	bindHandle := bindinfo.NewGlobalBindingHandle(&mockSessionPool{tk.Session()})
	err := bindHandle.LoadFromStorageToCache(true)
	require.NoError(t, err)
	stmt, err := parser.New().ParseOneStmt("select * from test . t0", "", "")
	require.NoError(t, err)

	_, fuzzyDigest := norm.NormalizeStmtForBinding(stmt, norm.WithFuzz(true))
	binding, matched := bindHandle.MatchGlobalBinding(tk.Session(), fuzzyDigest, bindinfo.CollectTableNames(stmt))
	require.True(t, matched)
	updateTime := binding.UpdateTime.String()

	rows1 := tk.MustQuery("show status like 'last_plan_binding_update_time';").Rows()
	updateTime1 := rows1[0][1]
	require.Equal(t, updateTime, updateTime1)

	rows2 := tk.MustQuery("show session status like 'last_plan_binding_update_time';").Rows()
	updateTime2 := rows2[0][1]
	require.Equal(t, updateTime, updateTime2)
	tk.MustQuery(`show global status like 'last_plan_binding_update_time';`).Check(testkit.Rows())
}

func TestBindingLastUpdateTimeWithInvalidBind(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)

	rows0 := tk.MustQuery("show status like 'last_plan_binding_update_time';").Rows()
	updateTime0 := rows0[0][1]
	require.Equal(t, updateTime0, "0000-00-00 00:00:00")

	tk.MustExec("insert into mysql.bind_info values('select * from `test` . `t`', 'select * from `test` . `t` use index(`idx`)', 'test', 'enabled', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "', '', '')")
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
	store := testkit.CreateMockStore(t)

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
	_, digest := parser.NormalizeDigestForBinding(originSQL)
	mockDigest := digest.String()
	sql := fmt.Sprintf(`INSERT INTO mysql.bind_info(original_sql,bind_sql,default_db,status,create_time,update_time,charset,collation,source, sql_digest, plan_digest) VALUES ('%s', '%s', '%s', '%s', NOW(), NOW(),'%s', '%s', '%s', '%s', '%s')`,
		originSQL, bindSQL, defaultDb, status, charset, collation, source, mockDigest, mockDigest)
	tk.MustExec(sql)
	bindHandle := bindinfo.NewGlobalBindingHandle(&mockSessionPool{tk.Session()})
	err := bindHandle.LoadFromStorageToCache(true)
	require.NoError(t, err)
	require.Equal(t, 1, bindHandle.Size())

	stmt, err := parser.New().ParseOneStmt("select * from test . t", "", "")
	require.NoError(t, err)
	_, fuzzyDigest := norm.NormalizeStmtForBinding(stmt, norm.WithFuzz(true))
	binding, matched := bindHandle.MatchGlobalBinding(tk.Session(), fuzzyDigest, bindinfo.CollectTableNames(stmt))
	require.True(t, matched)
	require.Equal(t, "select * from `test` . `t`", binding.OriginalSQL)
	require.Equal(t, "select * from `test` . `t` use index(index_t)", binding.BindSQL)
	require.Equal(t, "test", binding.Db)
	require.Equal(t, bindinfo.Enabled, binding.Status)
	require.Equal(t, "utf8mb4", binding.Charset)
	require.Equal(t, "utf8mb4_bin", binding.Collation)
	require.NotNil(t, binding.CreateTime)
	require.NotNil(t, binding.UpdateTime)
	dur, err := binding.SinceUpdateTime()
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

func TestSetBindingStatus(t *testing.T) {
	store := testkit.CreateMockStore(t)

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
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, index idx_a(a))")
	internal.UtilCleanBindingEnv(tk, dom)
	tk.MustQuery("show global bindings").Check(testkit.Rows())

	// Simulate creating bindings on other machines
	_, sqlDigest := parser.NormalizeDigestForBinding("select * from `test` . `t` where `a` > ?")
	tk.MustExec("insert into mysql.bind_info values('select * from `test` . `t` where `a` > ?', 'SELECT /*+ USE_INDEX(`t` `idx_a`)*/ * FROM `test`.`t` WHERE `a` > 10', 'test', 'deleted', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "', '" + sqlDigest.String() + "', '')")
	tk.MustExec("insert into mysql.bind_info values('select * from `test` . `t` where `a` > ?', 'SELECT /*+ USE_INDEX(`t` `idx_a`)*/ * FROM `test`.`t` WHERE `a` > 10', 'test', 'enabled', '2000-01-02 09:00:00', '2000-01-02 09:00:00', '', '','" +
		bindinfo.Manual + "', '" + sqlDigest.String() + "', '')")
	dom.BindHandle().Clear()
	tk.MustExec("set binding disabled for select * from t where a > 10")
	tk.MustExec("admin reload bindings")
	rows := tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Disabled, rows[0][3])

	// clear the mysql.bind_info
	internal.UtilCleanBindingEnv(tk, dom)

	// Simulate creating bindings on other machines
	tk.MustExec("insert into mysql.bind_info values('select * from `test` . `t` where `a` > ?', 'SELECT * FROM `test`.`t` WHERE `a` > 10', 'test', 'deleted', '2000-01-01 09:00:00', '2000-01-01 09:00:00', '', '','" +
		bindinfo.Manual + "', '" + sqlDigest.String() + "', '')")
	tk.MustExec("insert into mysql.bind_info values('select * from `test` . `t` where `a` > ?', 'SELECT * FROM `test`.`t` WHERE `a` > 10', 'test', 'disabled', '2000-01-02 09:00:00', '2000-01-02 09:00:00', '', '','" +
		bindinfo.Manual + "', '" + sqlDigest.String() + "', '')")
	dom.BindHandle().Clear()
	tk.MustExec("set binding enabled for select * from t where a > 10")
	tk.MustExec("admin reload bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Len(t, rows, 1)
	require.Equal(t, bindinfo.Enabled, rows[0][3])

	internal.UtilCleanBindingEnv(tk, dom)
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
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)

	for _, testSQL := range testSQLs {
		internal.UtilCleanBindingEnv(tk, dom)
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t")
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t(i int, s varchar(20))")
		tk.MustExec("create table t1(i int, s varchar(20))")
		tk.MustExec("create index index_t on t(i,s)")

		_, err := tk.Exec("create global " + testSQL.createSQL)
		require.NoError(t, err, "err %v", err)

		if testSQL.overlaySQL != "" {
			_, err = tk.Exec("create global " + testSQL.overlaySQL)
			require.NoError(t, err)
		}

		stmt, _, _ := internal.UtilNormalizeWithDefaultDB(t, testSQL.querySQL)

		_, fuzzyDigest := norm.NormalizeStmtForBinding(stmt, norm.WithFuzz(true))
		binding, matched := dom.BindHandle().MatchGlobalBinding(tk.Session(), fuzzyDigest, bindinfo.CollectTableNames(stmt))
		require.True(t, matched)
		require.Equal(t, testSQL.originSQL, binding.OriginalSQL)
		require.Equal(t, testSQL.bindSQL, binding.BindSQL)
		require.Equal(t, "test", binding.Db)
		require.Equal(t, bindinfo.Enabled, binding.Status)
		require.NotNil(t, binding.Charset)
		require.NotNil(t, binding.Collation)
		require.NotNil(t, binding.CreateTime)
		require.NotNil(t, binding.UpdateTime)

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

		bindHandle := bindinfo.NewGlobalBindingHandle(&mockSessionPool{tk.Session()})
		err = bindHandle.LoadFromStorageToCache(true)
		require.NoError(t, err)
		require.Equal(t, 1, bindHandle.Size())

		_, fuzzyDigest = norm.NormalizeStmtForBinding(stmt, norm.WithFuzz(true))
		binding, matched = dom.BindHandle().MatchGlobalBinding(tk.Session(), fuzzyDigest, bindinfo.CollectTableNames(stmt))
		require.True(t, matched)
		require.Equal(t, testSQL.originSQL, binding.OriginalSQL)
		require.Equal(t, testSQL.bindSQL, binding.BindSQL)
		require.Equal(t, "test", binding.Db)
		require.Equal(t, bindinfo.Enabled, binding.Status)
		require.NotNil(t, binding.Charset)
		require.NotNil(t, binding.Collation)
		require.NotNil(t, binding.CreateTime)
		require.NotNil(t, binding.UpdateTime)

		_, err = tk.Exec("drop global " + testSQL.dropSQL)
		require.Equal(t, uint64(1), tk.Session().AffectedRows())
		require.NoError(t, err)
		_, fuzzyDigest = norm.NormalizeStmtForBinding(stmt, norm.WithFuzz(true))
		_, matched = dom.BindHandle().MatchGlobalBinding(tk.Session(), fuzzyDigest, bindinfo.CollectTableNames(stmt))
		require.False(t, matched) // dropped
		bindHandle = bindinfo.NewGlobalBindingHandle(&mockSessionPool{tk.Session()})
		err = bindHandle.LoadFromStorageToCache(true)
		require.NoError(t, err)
		require.Equal(t, 0, bindHandle.Size())

		_, fuzzyDigest = norm.NormalizeStmtForBinding(stmt, norm.WithFuzz(true))
		_, matched = dom.BindHandle().MatchGlobalBinding(tk.Session(), fuzzyDigest, bindinfo.CollectTableNames(stmt))
		require.False(t, matched) // dropped

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
	store, dom := testkit.CreateMockStoreAndDomain(t)

	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int, index idx(a))")
	tk.MustExec("create global binding for select * from t using select * from t use index(idx)")
	require.Nil(t, dom.BindHandle().LoadFromStorageToCache(false))
	internal.UtilCleanBindingEnv(tk, dom)
	tk.MustExec("create global binding for select * from t using select * from t use index(idx)")
}

func TestReloadBindings(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)

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
	require.Nil(t, dom.BindHandle().LoadFromStorageToCache(false))
	rows = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, 1, len(rows))
	tk.MustExec("admin reload bindings")
	rows = tk.MustQuery("show global bindings").Rows()
	require.Equal(t, 0, len(rows))
}

func TestSetVarFixControlWithBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec(`create table t(id int, a varchar(100), b int, c int, index idx_ab(a, b))`)
	tk.MustQuery(`explain select * from t where c = 10 and (a = 'xx' or (a = 'kk' and b = 1))`).Check(
		testkit.Rows(
			`IndexLookUp_12 0.01 root  `,
			`├─Selection_10(Build) 0.02 cop[tikv]  or(eq(test.t.a, "xx"), and(eq(test.t.a, "kk"), eq(test.t.b, 1)))`,
			`│ └─IndexRangeScan_8 20.00 cop[tikv] table:t, index:idx_ab(a, b) range:["kk","kk"], ["xx","xx"], keep order:false, stats:pseudo`,
			`└─Selection_11(Probe) 0.01 cop[tikv]  eq(test.t.c, 10)`,
			`  └─TableRowIDScan_9 0.02 cop[tikv] table:t keep order:false, stats:pseudo`))

	tk.MustExec(`create global binding using select /*+ set_var(tidb_opt_fix_control='44389:ON') */ * from t where c = 10 and (a = 'xx' or (a = 'kk' and b = 1))`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning

	// the fix control can take effect
	tk.MustQuery(`explain select * from t where c = 10 and (a = 'xx' or (a = 'kk' and b = 1))`).Check(
		testkit.Rows(`IndexLookUp_11 0.01 root  `,
			`├─IndexRangeScan_8(Build) 10.10 cop[tikv] table:t, index:idx_ab(a, b) range:["kk" 1,"kk" 1], ["xx","xx"], keep order:false, stats:pseudo`,
			`└─Selection_10(Probe) 0.01 cop[tikv]  eq(test.t.c, 10)`,
			`  └─TableRowIDScan_9 10.10 cop[tikv] table:t keep order:false, stats:pseudo`))
	tk.MustQuery(`select @@last_plan_from_binding`).Check(testkit.Rows("1"))
}

func TestRemoveDuplicatedPseudoBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	checkPseudoBinding := func(num int) {
		tk.MustQuery(fmt.Sprintf("select count(1) from mysql.bind_info where original_sql='%s'",
			bindinfo.BuiltinPseudoSQL4BindLock)).Check(testkit.Rows(fmt.Sprintf("%d", num)))
	}
	insertPseudoBinding := func() {
		tk.MustExec(fmt.Sprintf(`INSERT INTO mysql.bind_info(original_sql, bind_sql, default_db, status, create_time, update_time, charset, collation, source)
            VALUES ('%v', '%v', "mysql", '%v', "2000-01-01 00:00:00", "2000-01-01 00:00:00", "", "", '%v')`,
			bindinfo.BuiltinPseudoSQL4BindLock, bindinfo.BuiltinPseudoSQL4BindLock, bindinfo.Builtin, bindinfo.Builtin))
	}
	removeDuplicated := func() {
		tk.MustExec(bindinfo.StmtRemoveDuplicatedPseudoBinding)
	}

	checkPseudoBinding(1)
	insertPseudoBinding()
	checkPseudoBinding(2)
	removeDuplicated()
	checkPseudoBinding(1)

	insertPseudoBinding()
	insertPseudoBinding()
	insertPseudoBinding()
	checkPseudoBinding(4)
	removeDuplicated()
	checkPseudoBinding(1)
	removeDuplicated()
	checkPseudoBinding(1)
}

type mockSessionPool struct {
	se sessiontypes.Session
}

func (p *mockSessionPool) Get() (pools.Resource, error) {
	return p.se, nil
}

func (p *mockSessionPool) Put(pools.Resource) {
}
