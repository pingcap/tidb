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
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

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
	tk.MustExec(`set global tidb_instance_plan_cache_max_size=123456`)
	tk.MustQuery(`select @@tidb_instance_plan_cache_max_size`).Check(testkit.Rows("123456"))
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
