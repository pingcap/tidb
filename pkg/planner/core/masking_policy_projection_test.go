// Copyright 2026 PingCAP, Inc.
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
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestMaskingPolicyProjection(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(id int primary key, c varchar(10))")
	tk.MustExec("insert into t values (1, 'a'), (2, 'b')")
	tk.MustExec("create masking policy p on t(c) as concat(c, 'x') enable")

	tk.MustQuery("select c from t order by id").Check(testkit.Rows("ax", "bx"))
	tk.MustQuery("select concat(c, '-') from t where c = 'a'").Check(testkit.Rows("ax-"))

	// Predicate should still use original value.
	rows := tk.MustQuery("select c from t where c = 'ax'").Rows()
	require.Len(t, rows, 0)

	// Exercise the masking builtin, DDL cache invalidation, and both fast paths together.
	tk.MustExec("alter table t modify masking policy p set expression = mask_full(c)")
	tk.MustQuery("select c from t order by id").Check(testkit.Rows("X", "X"))
	tk.MustQuery("select c from t where id = 1").Check(testkit.Rows("X"))
	tk.MustQuery("select c from t where id in (1, 2)").Check(testkit.Rows("X", "X"))
	tk.MustExec("alter table t disable masking policy p")
	tk.MustQuery("select c from t order by id").Check(testkit.Rows("a", "b"))
	tk.MustExec("alter table t enable masking policy p")
	tk.MustQuery("select c from t order by id").Check(testkit.Rows("X", "X"))
}

func TestMaskingPolicyPointGet(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_pointget")
	tk.MustExec("create table t_pointget(id int primary key, c varchar(10))")
	tk.MustExec("insert into t_pointget values (1, 'secret'), (2, 'hidden')")
	tk.MustExec("create masking policy p_pointget on t_pointget(c) as concat('***', c) enable")

	// Test Point_Get with primary key lookup
	tk.MustQuery("select c from t_pointget where id = 1").Check(testkit.Rows("***secret"))
	tk.MustQuery("select c from t_pointget where id = 2").Check(testkit.Rows("***hidden"))

	// Test Point_Get with multiple columns
	tk.MustQuery("select id, c from t_pointget where id = 1").Check(testkit.Rows("1 ***secret"))

	// Test Point_Get with expression using masked column
	tk.MustQuery("select concat(c, '-') from t_pointget where id = 1").Check(testkit.Rows("***secret-"))

	// Test Point_Get with unique index lookup
	tk.MustExec("create unique index idx_c on t_pointget(c)")
	tk.MustQuery("select c from t_pointget where c = 'secret'").Check(testkit.Rows("***secret"))

	// Predicate should still use original value
	rows := tk.MustQuery("select c from t_pointget where c = '***secret'").Rows()
	require.Len(t, rows, 0)
}

func TestMaskingPolicyBatchPointGet(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t_batch_pointget")
	tk.MustExec("create table t_batch_pointget(id int primary key, c varchar(10))")
	tk.MustExec("insert into t_batch_pointget values (1, 'secret'), (2, 'hidden'), (3, 'masked')")
	tk.MustExec("create masking policy p_batch on t_batch_pointget(c) as concat('***', c) enable")

	// Test BatchPointGet with WHERE pk IN (...)
	tk.MustQuery("select c from t_batch_pointget where id in (1, 2)").Check(testkit.Rows("***secret", "***hidden"))

	// Test BatchPointGet with multiple values
	tk.MustQuery("select c from t_batch_pointget where id in (1, 2, 3)").Check(testkit.Rows("***secret", "***hidden", "***masked"))

	// Test BatchPointGet with multiple columns
	tk.MustQuery("select id, c from t_batch_pointget where id in (1, 2)").Check(testkit.Rows("1 ***secret", "2 ***hidden"))

	// Test BatchPointGet with expression using masked column
	tk.MustQuery("select concat(c, '-') from t_batch_pointget where id in (1)").Check(testkit.Rows("***secret-"))

	// Test BatchPointGet with unique index IN clause
	tk.MustExec("create unique index idx_c on t_batch_pointget(c)")
	tk.MustQuery("select c from t_batch_pointget where c in ('secret', 'hidden')").Check(testkit.Rows("***secret", "***hidden"))

	// Predicate should still use original value
	rows := tk.MustQuery("select c from t_batch_pointget where c in ('***secret')").Rows()
	require.Len(t, rows, 0)

	// Test single value in IN (might use PointGet or BatchPointGet)
	tk.MustQuery("select c from t_batch_pointget where id in (1)").Check(testkit.Rows("***secret"))
}

func TestMaskingPolicyResultShapes(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_shapes(id int primary key, c varchar(10))")
	tk.MustExec("insert into t_shapes values (1, 'aa'), (2, 'bb'), (3, 'aa')")
	tk.MustExec("create masking policy p_shapes on t_shapes(c) as concat('[', c, ']') enable")

	for _, sql := range []string{
		"select concat(c, '-') from t_shapes limit 1",
		"select concat(c, '-') from t_shapes order by c limit 1",
		"select concat(c, '-') from t_shapes where c = 'aa' limit 1",
		"select concat(d, '-') from (select concat(c, '') as d from t_shapes) s where d = 'aa' limit 1",
	} {
		tk.MustQuery(sql).Check(testkit.Rows("[aa]-"))
	}
	tk.MustQuery("select concat(c, '-') from t_shapes order by c desc limit 1").Check(testkit.Rows("[bb]-"))
	tk.MustQuery("select *, concat(c, '-') from t_shapes where id = 1 limit 1").Check(testkit.Rows("1 [aa] [aa]-"))
	tk.MustQuery("select concat(c, '-') as d from t_shapes having d = 'bb-' order by d limit 1").Check(testkit.Rows("[bb]-"))
	tk.MustQuery("with s as (select c from t_shapes) select /*+ no_merge(s) */ concat(c, '-') from s where c = 'bb' limit 1").Check(testkit.Rows("[bb]-"))
	tk.MustQuery("with s as (select c from t_shapes) select concat(s1.c, '-') from s s1 join s s2 on s1.c = s2.c where s1.c = 'bb' limit 1").Check(testkit.Rows("[bb]-"))
	tk.MustQuery("select concat(s.c, '-') from (select c from t_shapes where id = 2) s left join t_shapes t on s.c = t.c limit 1").Check(testkit.Rows("[bb]-"))
	tk.MustQuery("select max(c), min(c) from t_shapes").Check(testkit.Rows("[bb] [aa]"))

	// DISTINCT compares raw result values, even when they have the same masked display.
	tk.MustExec("alter table t_shapes modify masking policy p_shapes set expression = mask_full(c)")
	tk.MustQuery("select distinct concat(c, '-') from t_shapes order by 1 limit 2").Check(testkit.Rows("XX-", "XX-"))
	tk.MustQuery("select concat(c, '-') from t_shapes group by c having c = 'bb' limit 1").Check(testkit.Rows("XX-"))
}

func TestMaskingPolicyUnionOrigins(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_union_origins(c varchar(10))")
	tk.MustExec("insert into t_union_origins values ('aa'), ('bb')")
	tk.MustExec("create masking policy p_union_origins on t_union_origins(c) as mask_full(c) enable")
	tk.MustExec("create table t_other_origins(c varchar(10))")
	tk.MustExec("insert into t_other_origins values ('cc')")
	tk.MustExec("create masking policy p_other_origins on t_other_origins(c) as concat('[', c, ']') enable")

	for _, sql := range []string{
		"select 'public' as c union all select c from t_union_origins",
		"select c from t_union_origins union all select 'public'",
		"select 'public' as c union select c from t_union_origins",
		"select c from t_union_origins union select c from t_other_origins",
		"select c from t_union_origins union select concat(c, '-') from t_union_origins",
		"select c from (select 'public' as c union all select c from t_union_origins) s",
		"with s as (select 'public' as c union all select c from t_union_origins) select s1.c from s s1 join s s2 on s1.c = s2.c",
	} {
		require.ErrorContains(t, tk.ExecToErr(sql), "masking policy cannot be preserved through UNION", sql)
	}

	tk.MustQuery("select c from t_union_origins union select c from t_union_origins").Check(testkit.Rows("XX", "XX"))
	tk.MustQuery("select c from t_union_origins intersect select 'bb'").Check(testkit.Rows("XX"))
	tk.MustQuery("select c from t_union_origins except select 'bb'").Check(testkit.Rows("XX"))
	tk.MustQuery("select 'bb' intersect select c from t_union_origins").Check(testkit.Rows("bb"))
	tk.MustQuery("select count(*) from (select 'public' as c union all select c from t_union_origins) s").Check(testkit.Rows("3"))
	tk.MustExec("alter table t_union_origins disable masking policy p_union_origins")
	tk.MustQuery("select 'public' as c union all select c from t_union_origins").Sort().Check(testkit.Rows("aa", "bb", "public"))
}

func TestMaskingPolicyFastPathTypes(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mask_types(id int primary key, c int, f float)")
	tk.MustExec("insert into t_mask_types values (1, 123456789, 1.5), (2, 987654321, 2.5)")
	tk.MustExec("create masking policy p_mask_types on t_mask_types(c) as concat('x', c) enable")
	tk.MustExec("create masking policy p_mask_float on t_mask_types(f) as f + 1 enable")
	for _, suffix := range []string{"", " where id = 1", " where id in (1, 2)"} {
		rows := testkit.Rows("x123456789 2.5", "x987654321 3.5")
		if suffix == " where id = 1" {
			rows = rows[:1]
		}
		tk.MustQuery("select c, f from t_mask_types" + suffix).Sort().Check(rows)
	}
	tk.MustQuery("select f from t_mask_types where id = 1").Check(testkit.Rows("2.5"))
	tk.MustQuery("select f from t_mask_types where id in (1, 2)").Sort().Check(testkit.Rows("2.5", "3.5"))
	tk.MustExec("alter table t_mask_types modify masking policy p_mask_types set expression = c + 1")
	tk.MustQuery("select c from t_mask_types where id = 1").Check(testkit.Rows("123456790"))
	tk.MustQuery("select c from t_mask_types where id in (1, 2)").Sort().Check(testkit.Rows("123456790", "987654322"))
}

func TestMaskingPolicyUnsupportedResults(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_unsupported(id int primary key, c varchar(10))")
	tk.MustExec("insert into t_unsupported values (1, 'secret')")
	tk.MustExec("create masking policy p_unsupported on t_unsupported(c) as mask_full(c) enable")
	require.ErrorContains(t, tk.ExecToErr("select group_concat(c) from t_unsupported"), "masking policy cannot be preserved through aggregate")
	require.ErrorContains(t, tk.ExecToErr("select (select c from t_unsupported where id = 1)"), "masking policy cannot be preserved through scalar subquery")
	require.ErrorContains(t, tk.ExecToErr("select (select t.c) from t_unsupported t"), "masking policy cannot be preserved through scalar subquery")
	require.ErrorContains(t, tk.ExecToErr("select concat(c, rand()) from t_unsupported order by 1"), "masking policy cannot be preserved through mutable expression")
	// Predicate subqueries still compare original values.
	tk.MustQuery("select id from t_unsupported where c = (select c from t_unsupported where id = 1)").Check(testkit.Rows("1"))
}
