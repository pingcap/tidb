// Copyright 2024 PingCAP, Inc.
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

package instanceplancache

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/parser/auth"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestInstancePlanCacheMinSize(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExecToErr("set global tidb_instance_plan_cache_max_size=0")
	tk.MustExecToErr("set global tidb_instance_plan_cache_max_size=1")
	tk.MustExecToErr("set global tidb_instance_plan_cache_max_size=101KiB")
	tk.MustExecToErr("set global tidb_instance_plan_cache_max_size=10001KiB")
	tk.MustExecToErr("set global tidb_instance_plan_cache_max_size=99MiB")
	tk.MustExec("set global tidb_instance_plan_cache_max_size=100MiB")
	tk.MustExec("set global tidb_instance_plan_cache_max_size=101MiB")
	tk.MustExec("set global tidb_instance_plan_cache_max_size=2000000KiB")
}

func TestInstancePlanCacheVars(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int)`)

	// check default values
	tk.MustQuery(`select @@tidb_enable_instance_plan_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`select @@tidb_instance_plan_cache_max_size`).Check(testkit.Rows("104857600"))
	tk.MustQuery(`select @@tidb_instance_plan_cache_reserved_percentage`).Check(testkit.Rows("0.1"))
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustQuery(`select @@tidb_enable_instance_plan_cache`).Check(testkit.Rows("1"))

	// check invalid values
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustQuery(`select @@tidb_enable_instance_plan_cache`).Check(testkit.Rows("1"))
	tk.MustExecToErr(`set global tidb_instance_plan_cache_max_size=-1`)
	tk.MustExecToErr(`set global tidb_instance_plan_cache_max_size=-1111111111111`)
	tk.MustExecToErr(`set global tidb_instance_plan_cache_max_size=dslfj`)
	tk.MustExec(`set global tidb_instance_plan_cache_max_size=1234560000`)
	tk.MustQuery(`select @@tidb_instance_plan_cache_max_size`).Check(testkit.Rows("1234560000"))
	tk.MustExec(`set global tidb_instance_plan_cache_reserved_percentage=-1`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1292 Truncated incorrect tidb_instance_plan_cache_reserved_percentage value: '-1'`))
	tk.MustExec(`set global tidb_instance_plan_cache_reserved_percentage=1.1100`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(`Warning 1292 Truncated incorrect tidb_instance_plan_cache_reserved_percentage value: '1.1100'`))
	tk.MustExec(`set global tidb_instance_plan_cache_reserved_percentage=0.1`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows())
}

func TestInstancePlanCacheBinding(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, key(b))`)
	tk.MustExec(`create table t2 (a int, b int, key(b))`)
	tk.MustExec(`create table t3 (a int, b int, key(b))`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustQuery(`select @@tidb_enable_instance_plan_cache`).Check(testkit.Rows("1"))

	// normal binding
	tk.MustExec(`prepare st from 'select * from t1 where a=?'`)
	tk.MustExec(`set @a=1`)
	tk.MustExec(`execute st using @a`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`create binding using select /*+ use_index(t1, b) */ * from t1 where a=2`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // can't hit due to the binding
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // hit the bound plan

	// cross-db binding
	tk.MustExec(`set @@tidb_opt_enable_fuzzy_binding=1`)
	tk.MustExec(`prepare st from 'select * from t2 where a=?'`)
	tk.MustExec(`set @a=1`)
	tk.MustExec(`execute st using @a`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`create binding using select /*+ use_index(t2, b) */ * from *.t2 where a=2`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // can't hit due to the binding
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // hit the bound plan

	// ignore-plan-cache hint
	tk.MustExec(`prepare st from 'select /*+ ignore_plan_cache() */ * from t3 where b=?'`)
	tk.MustExec(`set @a=1`)
	tk.MustExec(`execute st using @a`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))

	// ignore-plan-cache hint with binding
	tk.MustExec(`prepare st from 'select * from t3 where a=?'`)
	tk.MustExec(`set @a=1`)
	tk.MustExec(`execute st using @a`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`create binding using select /*+ ignore_plan_cache() */ * from t3 where a=2`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
}

func TestInstancePlanCacheReason(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t1 (a int, b int, key(b))`)
	tk.MustExec(`create table t2 (a int, b int, key(b))`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustQuery(`select @@tidb_enable_instance_plan_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`prepare st from 'select * from t1 where t1.a > (select 1 from t2 where t2.b<1)'`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(
		"Warning 1105 skip prepared plan-cache: query has uncorrelated sub-queries is un-cacheable"))

	tk.MustExec(`prepare st from 'select * from t1 limit ?'`)
	tk.MustExec(`set @a=1000000`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(
		"Warning 1105 skip prepared plan-cache: limit count is too large"))

	tk.MustExec(`prepare st from 'select b from t1 where b < ?'`)
	tk.MustExec(`set @a='123'`)
	tk.MustExec(`execute st using @a`)
	tk.MustQuery(`show warnings`).Check(testkit.Rows(
		"Warning 1105 skip prepared plan-cache: '123' may be converted to INT"))
}

func TestInstancePlanCacheStaleRead(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec(`create table t (a int)`)
	time.Sleep(time.Second)
	ts0 := tk.MustQuery(`select now()`).Rows()[0][0].(string)
	time.Sleep(time.Second)
	tk.MustExec(`insert into t values (1)`)
	time.Sleep(time.Second)
	ts1 := tk.MustQuery(`select now()`).Rows()[0][0].(string)
	time.Sleep(time.Second)
	tk.MustExec(`insert into t values (2), (3)`)
	time.Sleep(time.Second)
	ts2 := tk.MustQuery(`select now()`).Rows()[0][0].(string)

	// as-of-timestamp statements
	tk.MustExecToErr(`prepare st from 'select * from t as of timestamp ?'`) // not supported yet

	tk.MustExec(fmt.Sprintf(`prepare st from 'select * from t as of timestamp "%v"'`, ts0))
	tk.MustQuery(`execute st`).Sort().Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`execute st`).Sort().Check(testkit.Rows())
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(fmt.Sprintf(`prepare st from 'select * from t as of timestamp "%v"'`, ts1))
	tk.MustQuery(`execute st`).Sort().Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`execute st`).Sort().Check(testkit.Rows("1"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(fmt.Sprintf(`prepare st from 'select * from t as of timestamp "%v"'`, ts2))
	tk.MustQuery(`execute st`).Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`execute st`).Sort().Check(testkit.Rows("1", "2", "3"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestInstancePlanCacheInTxn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`prepare st from 'select * from t'`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`begin`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // can't hit
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`insert into t values (1)`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // dirty-data
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`rollback`)

	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`begin`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	tk.MustExec(`rollback`)
}

func TestInstancePlanCacheSchemaChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec(`create table t (a int)`)
	tk.MustExec(`prepare st from 'select * from t'`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`alter table t add column b int`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // due to DDL
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`alter table t drop column b`)
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // due to DDL
	tk.MustExec(`execute st`)
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestInstancePlanCachePrivilegeChanges(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)

	tk.MustExec(`create table t (a int, primary key(a))`)
	tk.MustExec(`CREATE USER 'u1'`)
	tk.MustExec(`grant select on test.t to 'u1'`)

	u1 := testkit.NewTestKit(t, store)
	require.NoError(t, u1.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))

	u1.MustExec(`prepare st from 'select a from test.t where a<1'`)
	u1.MustExec(`execute st`)
	u1.MustExec(`execute st`)
	u1.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`revoke select on test.t from 'u1'`)
	u1.MustExecToErr(`execute st`) // no privilege

	tk.MustExec(`grant select on test.t to 'u1'`)
	u1.MustExec(`execute st`)
	u1.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // hit the cache again
}

func TestInstancePlanCacheDifferentCollation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec(`create table t (a int, b int)`)

	u1 := testkit.NewTestKit(t, store)
	u1.MustExec(`use test`)
	u1.MustExec(`prepare st from 'select a from t where a=1'`)
	u1.MustExec(`execute st`)
	u1.MustExec(`execute st`)
	u1.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	u2 := testkit.NewTestKit(t, store)
	u2.MustExec(`use test`)
	u2.MustExec(`set @@collation_connection=utf8mb4_0900_ai_ci`)
	u2.MustExec(`prepare st from 'select a from t where a=1'`)
	u2.MustExec(`execute st`)
	u2.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	u3 := testkit.NewTestKit(t, store)
	u3.MustExec(`use test`)
	u3.MustExec(`prepare st from 'select a from t where a=1'`)
	u3.MustExec(`execute st`)
	u3.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestInstancePlanCacheDifferentCharset(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec(`create table t (a int, b int)`)

	u1 := testkit.NewTestKit(t, store)
	u1.MustExec(`use test`)
	u1.MustExec(`prepare st from 'select a from t where a=1'`)
	u1.MustExec(`execute st`)
	u1.MustExec(`execute st`)
	u1.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	u2 := testkit.NewTestKit(t, store)
	u2.MustExec(`use test`)
	u2.MustExec(`set @@character_set_connection=latin1`)
	u2.MustExec(`prepare st from 'select a from t where a=1'`)
	u2.MustExec(`execute st`)
	u2.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	u3 := testkit.NewTestKit(t, store)
	u3.MustExec(`use test`)
	u3.MustExec(`prepare st from 'select a from t where a=1'`)
	u3.MustExec(`execute st`)
	u3.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestInstancePlanCacheDifferentUsers(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)

	tk.MustExec(`create table t (a int, primary key(a))`)
	tk.MustExec(`CREATE USER 'u1'`)
	tk.MustExec(`grant select on test.t to 'u1'`)
	tk.MustExec(`CREATE USER 'u1'@'localhost'`)
	tk.MustExec(`grant select on test.t to 'u1'@'localhost'`)
	tk.MustExec(`CREATE USER 'u2'`)
	tk.MustExec(`grant select on test.t to 'u2'`)

	u1 := testkit.NewTestKit(t, store)
	require.NoError(t, u1.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))
	u1Local := testkit.NewTestKit(t, store)
	require.NoError(t, u1Local.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "localhost"}, nil, nil, nil))
	u2 := testkit.NewTestKit(t, store)
	require.NoError(t, u2.Session().Auth(&auth.UserIdentity{Username: "u2", Hostname: "%"}, nil, nil, nil))
	u1Dup := testkit.NewTestKit(t, store)
	require.NoError(t, u1Dup.Session().Auth(&auth.UserIdentity{Username: "u1", Hostname: "%"}, nil, nil, nil))

	u1.MustExec(`prepare st from 'select a from test.t where a=1'`)
	u1.MustExec(`execute st`)
	u1.MustExec(`execute st`)
	u1.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	u1Local.MustExec(`prepare st from 'select a from test.t where a=1'`)
	u1Local.MustExec(`execute st`)
	u1Local.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // can't hit, different localhost
	u1Local.MustExec(`execute st`)
	u1Local.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	u2.MustExec(`prepare st from 'select a from test.t where a=1'`)
	u2.MustExec(`execute st`)
	u2.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0")) // can't hit, different user
	u2.MustExec(`execute st`)
	u2.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	u1Dup.MustExec(`prepare st from 'select a from test.t where a=1'`)
	u1Dup.MustExec(`execute st`)
	u1Dup.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1")) // can hit, same user and localhost
	u1Dup.MustExec(`execute st`)
	u1Dup.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
}

func TestInstancePlanCachePartitioning(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec(`set @@tidb_partition_prune_mode='dynamic'`)

	tk.MustExec(`create table t (a int, b varchar(255)) partition by hash(a) partitions 3`)
	tk.MustExec(`insert into t values (1,"a"),(2,"b"),(3,"c"),(4,"d"),(5,"e"),(6,"f")`)
	tk.MustExec(`prepare stmt from 'select a,b from t where a = ?;'`)
	tk.MustExec(`set @a=1`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("1 a"))
	// Same partition works, due to pruning is not affected
	tk.MustExec(`set @a=4`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("4 d"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))

	tk.MustExec(`set @@tidb_partition_prune_mode='static'`)
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("4 d"))
	tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("0"))
	tk.MustQuery(`execute stmt using @a`).Check(testkit.Rows("4 d"))
	tk.MustQuery(`show warnings`).Check(testkit.Rows("Warning 1105 skip prepared plan-cache: Static partition pruning mode"))
}

func TestInstancePlanCachePlan(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec(`create table t1 (a int, b int, c int, d int, primary key(a), key(b), unique key(c))`)
	tk.MustExec(`create table t2 (a int, b int, c int, d int, primary key(a), key(b), unique key(c))`)
	tkProcess := tk.Session().ShowProcess()
	ps := []*util.ProcessInfo{tkProcess}
	tk.Session().SetSessionManager(&testkit.MockSessionManager{PS: ps})
	sessionID := tkProcess.ID
	tk2 := testkit.NewTestKit(t, store)

	type Case struct {
		SQL  string
		Args []string
		Plan string
	}

	cases := []Case{
		{
			SQL:  "select * from t1 where a = ?",
			Args: []string{"1"},
			Plan: "Point_Get",
		},
		{
			SQL:  "select * from t1 where c = ?",
			Args: []string{"1"},
			Plan: "Point_Get",
		},
		{
			SQL:  "select * from t1 where a in (?,?)",
			Args: []string{"1", "2"},
			Plan: "Batch_Point_Get",
		},
		{
			SQL:  "select * from t1 where c in (?,?)",
			Args: []string{"1", "2"},
			Plan: "Batch_Point_Get",
		},
		{
			SQL:  "select * from t1 union all select * from t2 where a<?",
			Args: []string{"1"},
			Plan: "Union",
		},
		{
			SQL:  "select a, b from t1 where b<? union all select a, b from t2 where a<?",
			Args: []string{"1", "2"},
			Plan: "Union",
		},
		{
			SQL:  "select /*+ tidb_inlj(t2) */ * from t1, t2 where t1.a=? and t1.b=t2.b",
			Args: []string{"1"},
			Plan: "IndexJoin",
		},
		{
			SQL:  "select /*+ use_index_merge(t1, b, c) */ * from t1 where b=? or c=?",
			Args: []string{"1", "2"},
			Plan: "IndexMerge",
		},
		{
			SQL:  "update t1 set b=1 where b=?",
			Args: []string{"1"},
			Plan: "Update",
		},
		{
			SQL:  "delete from t1 where b=?",
			Args: []string{"1"},
			Plan: "Delete",
		},
		{
			SQL:  "insert ignore into t1 values (?,?,?,?)",
			Args: []string{"1", "2", "3", "4"},
			Plan: "Insert",
		},
	}

	for _, c := range cases {
		tk.MustExec(fmt.Sprintf("prepare stmt from '%v'", c.SQL))
		using := ""
		for i, arg := range c.Args {
			tk.MustExec(fmt.Sprintf("set @p%v = %v", i, arg))
			if i == 0 {
				using = fmt.Sprintf("@p%v", i)
			} else {
				using = fmt.Sprintf("%v, @p%v", using, i)
			}
		}
		tk.MustExec(fmt.Sprintf("execute stmt using %v", using))
		plan := tk2.MustQuery(fmt.Sprintf(`explain for connection %v`, sessionID)).Rows()
		expectedPlan := false
		for _, r := range plan {
			if strings.Contains(r[0].(string), c.Plan) {
				expectedPlan = true
			}
		}
		require.True(t, expectedPlan, c.SQL)
		tk.MustQuery(`show warnings`).Check(testkit.Rows()) // no warning
		tk.MustExec(fmt.Sprintf("execute stmt using %v", using))
		tk.MustQuery(`select @@last_plan_from_cache`).Check(testkit.Rows("1"))
	}
}

func TestInstancePlanCacheMetaInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a))`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec(`set @a=1, @b=2`)

	tk.MustExec(`prepare st from "select a from t where a<?"`)
	tk.MustExec(`execute st using @a`)
	tk.MustExec(`create global binding using select /*+ use_index(t, a) */ a from t where a=1 and b>1`)
	tk.MustExec(`prepare st from "select a from t where a=? and b>?"`)
	tk.MustExec(`execute st using @a, @b`)
	tk.MustExec(`prepare st from "insert into t values (?, 1)"`)
	tk.MustExec(`execute st using @a`)
	tk.MustExec(`prepare st from "delete from t where a=?"`)
	tk.MustExec(`execute st using @a`)

	sctx := tk.Session()
	values := domain.GetDomain(sctx).GetInstancePlanCache().All()
	require.Len(t, values, 4)
	sort.Slice(values, func(i, j int) bool {
		return values[i].(*plannercore.PlanCacheValue).SQLDigest <
			values[j].(*plannercore.PlanCacheValue).SQLDigest
	})
	v0 := values[0].(*plannercore.PlanCacheValue)
	v1 := values[1].(*plannercore.PlanCacheValue)
	v2 := values[2].(*plannercore.PlanCacheValue)
	v3 := values[3].(*plannercore.PlanCacheValue)

	require.True(t, v0.SQLDigest != "") // not empty
	require.Equal(t, "insert into t values (?, 1)", v0.SQLText)
	require.Equal(t, "Insert", v0.StmtType)
	require.Equal(t, "root", v0.ParseUser)
	require.Equal(t, "", v0.Binding)
	require.True(t, v0.OptimizerEnvHash != "") // not empty
	require.Equal(t, "1", v0.ParseValues)

	require.True(t, v1.SQLDigest != "") // not empty
	require.Equal(t, "select a from t where a<?", v1.SQLText)
	require.Equal(t, "Select", v1.StmtType)
	require.Equal(t, "root", v1.ParseUser)
	require.Equal(t, "", v1.Binding)
	require.True(t, v1.OptimizerEnvHash != "") // not empty
	require.Equal(t, "1", v1.ParseValues)

	require.True(t, v2.SQLDigest != "") // not empty
	require.Equal(t, "delete from t where a=?", v2.SQLText)
	require.Equal(t, "Delete", v2.StmtType)
	require.Equal(t, "root", v2.ParseUser)
	require.Equal(t, "", v2.Binding)
	require.True(t, v2.OptimizerEnvHash != "") // not empty
	require.Equal(t, "1", v2.ParseValues)

	require.True(t, v3.SQLDigest != "") // not empty
	require.Equal(t, "select a from t where a=? and b>?", v3.SQLText)
	require.Equal(t, "Select", v3.StmtType)
	require.Equal(t, "root", v3.ParseUser)
	require.Equal(t, "SELECT /*+ use_index(`t` `a`)*/ `a` FROM `test`.`t` WHERE `a` = 1 AND `b` > 1", v3.Binding)
	require.True(t, v3.OptimizerEnvHash != "") // not empty
	require.Equal(t, "(1, 2)", v3.ParseValues)
}

func TestInstancePlanCacheRuntimeInfo(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a))`)
	tk.MustExec(`insert into t values (1, 1)`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec(`set @a=1, @b=2`)

	tk.MustExec(`prepare st1 from "select a from t where a<=?"`)
	tk.MustExec(`execute st1 using @a`)
	tk.MustExec(`execute st1 using @a`)
	tk.MustExec(`execute st1 using @a`)
	tk.MustExec(`execute st1 using @a`)
	tk.MustExec(`prepare st2 from "select a from t where a=? and b=?"`)
	tk.MustExec(`execute st2 using @a, @b`)
	tk.MustExec(`execute st2 using @a, @b`)

	sctx := tk.Session()
	values := domain.GetDomain(sctx).GetInstancePlanCache().All()
	require.Len(t, values, 2)
	sort.Slice(values, func(i, j int) bool {
		return values[i].(*plannercore.PlanCacheValue).SQLDigest <
			values[j].(*plannercore.PlanCacheValue).SQLDigest
	})
	v0 := values[0].(*plannercore.PlanCacheValue)
	v1 := values[1].(*plannercore.PlanCacheValue)

	require.Equal(t, v0.SQLText, "select a from t where a<=?")
	exec, _, _, sumLat, _ := v0.RuntimeInfo()
	require.Equal(t, int(exec), 4)
	require.True(t, sumLat != 0)

	require.Equal(t, v1.SQLText, "select a from t where a=? and b=?")
	exec, _, _, sumLat, _ = v1.RuntimeInfo()
	require.Equal(t, int(exec), 2)
	require.True(t, sumLat != 0)

	tk.MustExec(`execute st1 using @a`)
	exec, _, _, _, _ = v0.RuntimeInfo()
	require.Equal(t, int(exec), 5)
}

func TestInstancePlanCacheView(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	tk.MustExec(`use test`)
	tk.MustExec(`create table t (a int, b int, key(a))`)
	tk.MustExec(`set global tidb_enable_instance_plan_cache=1`)
	tk.MustExec(`set @a=1, @b=2`)

	tk.MustExec(`prepare st1 from "select a from t where a<=?"`)
	tk.MustExec(`prepare st2 from "select a from t where a=? and b=?"`)
	tk.MustExec(`execute st1 using @a`)
	tk.MustExec(`execute st1 using @a`)
	tk.MustExec(`execute st2 using @a, @b`)
	tk.MustExec(`execute st2 using @a, @b`)

	tk.MustQuery(`select sql_text, stmt_type, parse_user, parse_values, executions from information_schema.tidb_plan_cache order by sql_text`).Check(
		testkit.Rows("select a from t where a<=? Select root 1 2",
			"select a from t where a=? and b=? Select root (1, 2) 2"))

	tk.MustExec(`execute st1 using @a`)
	tk.MustExec(`execute st2 using @a, @b`)
	tk.MustQuery(`select sql_text, stmt_type, parse_user, parse_values, executions from information_schema.tidb_plan_cache order by sql_text`).Check(
		testkit.Rows("select a from t where a<=? Select root 1 3",
			"select a from t where a=? and b=? Select root (1, 2) 3"))
}
