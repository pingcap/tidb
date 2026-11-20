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
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/auth"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	core "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/base"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
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

func TestMaskingPolicyViewIgnoresTemporaryShadow(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table view_source(c varchar(20))")
	tk.MustExec("insert into view_source values ('secret')")
	tk.MustExec("create view masked_view as select c from view_source")
	tk.MustExec("create masking policy p on view_source(c) as mask_full(c)")
	tk.MustQuery("select c from masked_view").Check(testkit.Rows("XXXXXX"))
	tk.MustExec("create temporary table view_source(c varchar(20))")
	tk.MustExec("insert into view_source values ('public')")
	tk.MustQuery("select c from masked_view").Check(testkit.Rows("XXXXXX"))
	tk.MustQuery("select c from view_source").Check(testkit.Rows("public"))
}

func TestMaskingPolicySubqueryPreservesOuterClause(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table clause_source(id int primary key, c varchar(20) not null)")
	tk.MustExec("insert into clause_source values (1,'secret')")
	tk.MustExec("create masking policy p on clause_source(c) as mask_null(c)")
	for _, predicate := range []string{
		"1 in (select id from clause_source order by id limit 1)",
		"1 = any (select id from clause_source order by id limit 1)",
		"1 = all (select id from clause_source order by id limit 1)",
	} {
		tk.MustQuery("select " + predicate + ", c is null from clause_source").Check(testkit.Rows("1 1"))
	}
}

func TestMaskingPolicyMaterializedCTEPredicateUnion(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_opt_force_inline_cte=0")
	tk.MustExec("create table cte_predicate(c varchar(20))")
	tk.MustExec("insert into cte_predicate values ('secret')")
	tk.MustExec("create masking policy p on cte_predicate(c) as mask_full(c)")
	query := "with x as (select c from cte_predicate) select 'XXXXXX' in (select c from x union all select 'public') from x"
	require.Contains(t, fmt.Sprint(tk.MustQuery("explain format='brief' "+query).Rows()), "CTE")
	tk.MustQuery(query).Check(testkit.Rows("1"))
	tk.MustQuery("with x as (select c from cte_predicate) select 'secret' in (select c from x union all select 'public') from x").Check(testkit.Rows("0"))
	tk.MustExec("set tidb_opt_force_inline_cte=1")
	tk.MustQuery(query).Check(testkit.Rows("1"))
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
	tk.MustQuery("select c from t_batch_pointget where c in ('secret', 'hidden')").Sort().Check(testkit.Rows("***hidden", "***secret"))

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

	// ROLLUP grouping-set rows keep NULL instead of masking the raw value.
	tk.MustQuery("select c, count(*) from t_shapes group by c with rollup order by c").Check(testkit.Rows("<nil> 3", "[aa] 2", "[bb] 1"))
	tk.MustQuery("select c, id from t_shapes where id < 3 group by c, id with rollup order by c, id").Check(testkit.Rows(
		"<nil> <nil>", "[aa] <nil>", "[aa] 1", "[bb] <nil>", "[bb] 2"))
	// A NULL that is present in the data still goes through the policy.
	tk.MustExec("create table t_rollup_null(id int primary key, c varchar(10))")
	tk.MustExec("insert into t_rollup_null values (1, 'aa'), (2, null)")
	tk.MustExec("create masking policy p_rollup_null on t_rollup_null(c) as ifnull(c, 'N/A') enable")
	tk.MustQuery("select c, count(*) from t_rollup_null group by c with rollup order by grouping(c), c").Check(testkit.Rows("N/A 1", "aa 1", "<nil> 2"))

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
	// Predicate subqueries compare with their masked results.
	tk.MustQuery("select id from t_unsupported where c = (select c from t_unsupported where id = 1)").Check(testkit.Rows())
	tk.MustQuery("select id from t_unsupported where 'XXXXXX' = (select c from t_unsupported where id = 1)").Check(testkit.Rows("1"))
}

func TestMaskingPolicyWindowResults(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_window(id int primary key, c varchar(10))")
	tk.MustExec("insert into t_window values (1, 'secret'), (2, 'hidden')")
	tk.MustExec("create masking policy p_window on t_window(c) as mask_full(c) enable")
	for _, expr := range []string{
		"max(c) over ()",
		"min(c) over ()",
		"lag(c) over (order by id)",
		"lead(c) over (order by id)",
		"first_value(c) over (order by id)",
		"last_value(c) over (order by id)",
		"nth_value(c, 1) over (order by id)",
		"max(concat(c, '-')) over ()",
	} {
		sql := "select " + expr + " from t_window"
		require.ErrorContains(t, tk.ExecToErr(sql), "masking policy cannot be preserved through window function", sql)
	}
	for _, sql := range []string{
		"select concat(c, '-'), max(c) over () from t_window",
		"select concat(c, '-'), row_number() over (order by id), max(c) over () from t_window",
	} {
		require.ErrorContains(t, tk.ExecToErr(sql), "masking policy cannot be preserved through window function", sql)
	}
	// Unprotected window arguments and inherited protected columns remain usable.
	tk.MustQuery("select c, max(id) over () from t_window order by id").Check(testkit.Rows("XXXXXX 2", "XXXXXX 2"))
	tk.MustQuery("select concat(c, '-'), row_number() over (order by id) from t_window order by id").Check(testkit.Rows("XXXXXX- 1", "XXXXXX- 2"))
	tk.MustExec("alter table t_window disable masking policy p_window")
	tk.MustQuery("select max(c) over () from t_window").Check(testkit.Rows("secret", "secret"))
}

func TestMaskingPolicyScalarSubqueryClauses(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_scalar(id int primary key, c varchar(10))")
	tk.MustExec("insert into t_scalar values (1, 'secret'), (2, 'hidden')")
	tk.MustExec("create masking policy p_scalar on t_scalar(c) as mask_full(c) enable")
	for _, sql := range []string{
		"select (select id from t_scalar order by id limit 1), (select c from t_scalar where id = 1)",
		"select concat((select id from t_scalar order by id limit 1), (select c from t_scalar where id = 1))",
		"select (select max(id) from t_scalar having max(id) > 0), (select c from t_scalar where id = 1)",
	} {
		require.ErrorContains(t, tk.ExecToErr(sql), "masking policy cannot be preserved through scalar subquery", sql)
	}
	// Nested clauses must not turn a predicate subquery into a result-field check.
	tk.MustQuery("select id from t_scalar where id = (select id from t_scalar order by id limit 1) and c <> (select c from t_scalar where id = 1)").Check(testkit.Rows("1"))
	tk.MustQuery("select (select id from t_scalar order by id limit 1), (select max(id) from t_scalar)").Check(testkit.Rows("1 2"))

	// Values stored or returned outside a SELECT result are masked by the subquery itself.
	tk.MustExec("create table t_scalar_dst(id int primary key, c varchar(10))")
	tk.MustExec("insert into t_scalar_dst values (1, 'x'), (2, 'y')")
	tk.MustExec("set @v = (select c from t_scalar where id = 1)")
	tk.MustQuery("select @v").Check(testkit.Rows("XXXXXX"))
	tk.MustExec("do (@z := (select concat(c, '!') from t_scalar where id = 1))")
	tk.MustQuery("select @z").Check(testkit.Rows("XXXXXX!"))
	tk.MustExec("update t_scalar_dst set c = (select c from t_scalar where id = 1) where id = 1")
	tk.MustExec("insert into t_scalar_dst values (3, (select c from t_scalar where id = 2))")
	tk.MustExec("insert into t_scalar_dst values (2, 'z') on duplicate key update c = (select c from t_scalar where id = 1)")
	tk.MustQuery("select id, c from t_scalar_dst order by id").Check(testkit.Rows("1 XXXXXX", "2 XXXXXX", "3 XXXXXX"))
	// An error raised while storing a masked value must not reveal the original value.
	err := tk.ExecToErr("update t_scalar_dst set id = (select c from t_scalar where id = 1) where id = 1")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "secret")
	// A predicate subquery nested in a value subquery compares its masked result.
	tk.MustExec("set @w = (select c from t_scalar where id = 1 and c <> (select c from t_scalar where id = 1))")
	tk.MustQuery("select @w").Check(testkit.Rows("XXXXXX"))
	// EXISTS, IN and quantified subqueries do not pass the value context to their inner query blocks.
	tk.MustExec("set @e = exists(select 1 from t_scalar where c = (select c from t_scalar where id = 1))")
	tk.MustExec("set @i = 1 in (select id from t_scalar where c = (select c from t_scalar where id = 1))")
	tk.MustExec("set @a = 1 = any(select id from t_scalar where c = (select c from t_scalar where id = 1))")
	tk.MustQuery("select @e, @i, @a").Check(testkit.Rows("0 0 0"))
	tk.MustExec("update t_scalar_dst set id = 10 + exists(select 1 from t_scalar where c = (select c from t_scalar where id = 1)) where id = 3")
	tk.MustQuery("select id from t_scalar_dst where id > 3").Check(testkit.Rows("10"))
}

func TestMaskingPolicyFastPathRepresentations(t *testing.T) {
	store, _ := testkit.CreateMockStoreAndDomain(t)
	for _, tc := range []struct {
		name   string
		tp     string
		expr   string
		values []string
	}{
		{name: "bit", tp: "bit(8)", expr: "c + 1", values: []string{"2", "3"}},
		{name: "tinyint", tp: "tinyint", expr: "c + 1000", values: []string{"1001", "1002"}},
		{name: "smallint", tp: "smallint", expr: "c + 100000", values: []string{"100001", "100002"}},
		{name: "int", tp: "int", expr: "c + 3000000000", values: []string{"3000000001", "3000000002"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			table := "t_repr_" + tc.name
			tk.MustExec("create table " + table + "(id int primary key, c " + tc.tp + ")")
			tk.MustExec("insert into " + table + " values (1, 1), (2, 2)")
			tk.MustExec("create masking policy p_" + tc.name + " on " + table + "(c) as " + tc.expr + " enable")
			for _, suffix := range []string{" order by id", " where id = 1", " where id in (1, 2)"} {
				sql := "select c from " + table + suffix
				rs, err := tk.Exec(sql)
				require.NoError(t, err, sql)
				require.Equal(t, mysql.TypeLonglong, rs.Fields()[0].Column.GetType(), sql)
				require.NoError(t, rs.Close())
				rows := tc.values
				if suffix == " where id = 1" {
					rows = rows[:1]
				}
				tk.MustQuery(sql).Check(testkit.Rows(rows...))
			}
		})
	}
	// Type-preserving masks must retain both fast paths.
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_compatible(id int primary key, c bigint, s varchar(10))")
	tk.MustExec("insert into t_compatible values (1, 1, 'aa'), (2, 2, 'bb')")
	tk.MustExec("create masking policy p_compatible on t_compatible(c) as c + 1 enable")
	tk.MustExec("create masking policy p_string on t_compatible(s) as mask_full(s) enable")
	for _, suffix := range []string{" where id = 1", " where id in (1, 2)"} {
		sql := "select c, s from t_compatible" + suffix
		require.Contains(t, tk.MustQuery("explain format = 'brief' " + sql).Rows()[0][0], "Point_Get")
		rows := testkit.Rows("2 XX", "3 XX")
		if suffix == " where id = 1" {
			rows = rows[:1]
		}
		tk.MustQuery(sql).Check(rows)
	}
}

func TestMaskingPolicyMultiSchemaRename(t *testing.T) {
	for _, size := range []uint64{0, variable.DefTiDBSchemaCacheSize} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			store, dom := testkit.CreateMockStoreAndDomain(t)
			tk := testkit.NewTestKit(t, store)
			originalSize := variable.SchemaCacheSize.Load()
			t.Cleanup(func() { variable.SchemaCacheSize.Store(originalSize) })
			variable.SchemaCacheSize.Store(size)
			require.NoError(t, dom.Reload())
			tk.MustExec("use test")
			tk.MustExec("create table t_multi(c varchar(64))")
			tk.MustExec("insert into t_multi values ('secret')")
			tk.MustExec("create masking policy p on t_multi(c) as mask_full(c) enable")
			tk.MustQuery("select c from t_multi").Check(testkit.Rows("XXXXXX"))
			tk.MustExec("alter table t_multi change column c d varchar(64), add column x int")
			tk.MustQuery("select d from t_multi").Check(testkit.Rows("XXXXXX"))
			tk.MustExec("alter table t_multi modify masking policy p set expression = mask_partial(d, 1, 1, '*')")
			tk.MustQuery("select d from t_multi").Check(testkit.Rows("s****t"))
		})
	}
}

func TestMaskingPolicyIdentityPlanCache(t *testing.T) {
	for _, instance := range []bool{false, true} {
		t.Run(fmt.Sprint("instance=", instance), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			root := testkit.NewTestKit(t, store)
			require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
			root.MustExec("use test")
			root.MustExec("create table t_identity(id int primary key,c varchar(64))")
			root.MustExec("insert into t_identity values (1,'secret'),(2,'secret')")
			root.MustExec("create user 'u_identity'@'%'")
			root.MustExec("grant select on test.* to 'u_identity'@'%'")
			ctx := context.Background()
			if instance {
				ctx = context.WithValue(ctx, core.PlanCacheKeyEnableInstancePlanCache{}, true)
			}
			for _, fn := range []string{"mask_full", "user", "session_user", "system_user"} {
				policyExpr := "mask_full(c)"
				if fn != "mask_full" {
					policyExpr = fmt.Sprintf("case when current_user()='u_identity@%%' and %s()='u_identity@192.0.2.1' then c else mask_full(c) end", fn)
				}
				root.MustExec("create or replace masking policy p on t_identity(c) as " + policyExpr)
				for _, query := range []struct {
					sql, params string
					rows        int
				}{
					{"select c from t_identity where id = ?", "@a", 1},
					{"select c from t_identity where id in (?, ?)", "@a,@b", 2},
					{"select c from t_identity where id >= ? order by id", "@a", 2},
				} {
					for sessionIdx, host := range []string{"192.0.2.1", "192.0.2.2"} {
						tk := testkit.NewTestKit(t, store)
						require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u_identity", Hostname: host}, nil, nil, nil))
						tk.MustExec("use test")
						tk.MustExecWithContext(ctx, "prepare st from '"+query.sql+"'")
						tk.MustExecWithContext(ctx, "set @a=1,@b=2")
						value := "XXXXXX"
						if fn != "mask_full" && sessionIdx == 0 {
							value = "secret"
						}
						expected := make([]string, query.rows)
						for i := range expected {
							expected[i] = value
						}
						for attempt := range 2 {
							tk.MustQueryWithContext(ctx, "execute st using "+query.params).Check(testkit.Rows(expected...))
							hit := "0"
							if attempt > 0 || (instance && sessionIdx > 0) {
								hit = "1"
							}
							tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows(hit))
						}
					}
				}
			}
		})
	}
}

func TestMaskingPolicyRoleResultPlanCache(t *testing.T) {
	for _, instance := range []bool{false, true} {
		t.Run(fmt.Sprint(instance), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			root := testkit.NewTestKit(t, store)
			root.MustExec("create user role_cache_user")
			root.MustExec("create role role_cache_unmask")
			root.MustExec("grant role_cache_unmask to role_cache_user")
			root.MustExec("grant select on test.* to role_cache_user")
			root.MustExec("create table test.role_cache_result(id int primary key,c varchar(64))")
			root.MustExec("insert into test.role_cache_result values (1,'secret')")
			root.MustExec("create masking policy p on test.role_cache_result(c) as case when current_role() != 'NONE' then c else mask_full(c) end")
			tk := testkit.NewTestKit(t, store)
			require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "role_cache_user", Hostname: "%"}, nil, nil, nil))
			ctx := context.Background()
			if instance {
				ctx = context.WithValue(ctx, core.PlanCacheKeyEnableInstancePlanCache{}, true)
			}
			tk.MustExecWithContext(ctx, "prepare st from 'select c from test.role_cache_result where id=?'")
			tk.MustExec("set @a=1")
			for i, role := range []string{"all", "none", "all"} {
				tk.MustExec("set role " + role)
				value := "secret"
				if role == "none" {
					value = "XXXXXX"
				}
				tk.MustQueryWithContext(ctx, "execute st using @a").Check(testkit.Rows(value))
				hit := "1"
				if i == 0 {
					hit = "0"
				}
				tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows(hit))
			}
		})
	}
}

func TestMaskingPolicyNonPreparedPlanCache(t *testing.T) {
	for _, instance := range []bool{false, true} {
		t.Run(fmt.Sprint(instance), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			root := testkit.NewTestKit(t, store)
			root.MustExec("create user np_mask_user")
			root.MustExec("create role np_mask_role")
			root.MustExec("grant np_mask_role to np_mask_user")
			root.MustExec("grant select on test.* to np_mask_user")
			root.MustExec("create table test.np_mask(id int primary key,c varchar(64))")
			root.MustExec("insert into test.np_mask values (1,'secret')")
			root.MustExec("create masking policy p on test.np_mask(c) as case when current_user()='np_mask_user@%' and user()='np_mask_user@192.0.2.1' and current_role()!='NONE' then c else mask_full(c) end")
			ctx := context.Background()
			if instance {
				ctx = context.WithValue(ctx, core.PlanCacheKeyEnableInstancePlanCache{}, true)
			}
			for sessionIdx, host := range []string{"192.0.2.1", "192.0.2.2"} {
				tk := testkit.NewTestKit(t, store)
				require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "np_mask_user", Hostname: host}, nil, nil, nil))
				tk.MustExec("use test")
				tk.MustExec("set tidb_enable_non_prepared_plan_cache=1")
				for roleIdx, role := range []string{"all", "none", "all"} {
					tk.MustExec("set role " + role)
					value := "XXXXXX"
					if sessionIdx == 0 && role != "none" {
						value = "secret"
					}
					for attempt := range 2 {
						tk.MustQueryWithContext(ctx, "select c from test.np_mask where id>=1").Check(testkit.Rows(value))
						hit := "1"
						if attempt == 0 && roleIdx == 0 && (!instance || sessionIdx == 0) {
							hit = "0"
						}
						tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows(hit))
					}
				}
			}
		})
	}
}

func TestMaskingPolicyUnsafeFunctionSkipsPlanCache(t *testing.T) {
	for _, instance := range []bool{false, true} {
		t.Run(fmt.Sprint(instance), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustExec("create table unsafe_mask(id int primary key,c varchar(64))")
			tk.MustExec("insert into unsafe_mask values (1,'secret')")
			ctx := context.Background()
			if instance {
				ctx = context.WithValue(ctx, core.PlanCacheKeyEnableInstancePlanCache{}, true)
			}
			for _, policy := range []struct{ expr, setup1, setup2 string }{
				{"case when connection_id()=10 then c else mask_full(c) end", "", ""},
				// AES captures block_encryption_mode while its signature is built.
				{"case when length(aes_encrypt('x','key','0123456789abcdef'))=16 then c else mask_full(c) end", "set block_encryption_mode='aes-128-ecb'", "set block_encryption_mode='aes-128-ofb'"},
			} {
				tk.MustExec("create or replace masking policy p on unsafe_mask(c) as " + policy.expr)
				tk.MustExecWithContext(ctx, "prepare st from 'select c from unsafe_mask where id=?'")
				tk.MustExec("set @a=1")
				for i, setup := range []string{policy.setup1, policy.setup2} {
					tk.Session().GetSessionVars().ConnectionID = uint64(10 + i*10)
					if setup != "" {
						tk.MustExec(setup)
					}
					value := "secret"
					if i == 1 {
						value = "XXXXXX"
					}
					tk.MustQueryWithContext(ctx, "execute st using @a").Check(testkit.Rows(value))
					tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("0"))
				}
				tk.MustExec("deallocate prepare st")
			}
		})
	}
}

func TestMaskingPolicyUserVariableNotCached(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table var_mask(id int primary key,c varchar(64))")
	tk.MustExec("insert into var_mask values (1,'secret')")
	tk.MustExec("set @unmask=1")
	tk.MustExec("create masking policy p on var_mask(c) as case when @unmask=1 then c else mask_full(c) end")
	// The outer query marks @unmask as readonly for this statement. The policy
	// must keep its dependency rather than cache that statement's value.
	tk.MustQuery("select c from var_mask where id=@unmask").Check(testkit.Rows("secret"))
	tk.MustExec("set @unmask=0")
	tk.MustQuery("select c from var_mask where id=1").Check(testkit.Rows("XXXXXX"))
	tk.MustExec("prepare st from 'select c from var_mask where id=?'")
	tk.MustExec("set @a=1")
	for range 2 {
		tk.MustQuery("execute st using @a").Check(testkit.Rows("XXXXXX"))
		tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("0"))
	}
}

func TestMaskingPolicyExprCacheSQLMode(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set sql_mode=''")
	tk.MustExec("create table mode_mask(id int primary key,c bigint unsigned)")
	tk.MustExec("insert into mode_mask values (1,1)")
	tk.MustExec("create masking policy p on mode_mask(c) as c-2")
	tk.MustExec("prepare st from 'select c from mode_mask where id=?'")
	tk.MustExec("set @a=1")
	err := tk.QueryToErr("execute st using @a")
	require.ErrorContains(t, err, "out of range")
	tk.MustExec("set sql_mode='NO_UNSIGNED_SUBTRACTION'")
	for range 2 {
		tk.MustQuery("execute st using @a").Check(testkit.Rows("-1"))
	}
	tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
}

func TestMaskingPolicyLoadFailureBlocksQueries(t *testing.T) {
	// Exercise the lazy fallback with a cold cache instead of the cache warmed by Reload.
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/domain/skipLoadMaskingPolicies", "return(true)")
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_load(id int primary key, c varchar(64))")
	tk.MustExec("insert into t_load values (1, 'secret')")
	tk.MustExec("create masking policy p on t_load(c) as mask_full(c) enable")
	testfailpoint.Enable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadMaskingPoliciesError", "return(true)")
	for _, sql := range []string{"select c from t_load where id=1", "select c from t_load order by id", "show masking policies for t_load", "show create table t_load", "alter table t_load modify column c varchar(100)", "select id from t_load where c = (select c from t_load where id = 1)", "select id from t_load where 'secret' in (select c from t_load)"} {
		rs, err := tk.Exec(sql)
		if err == nil && rs != nil {
			_, err = session.GetRows4Test(context.Background(), tk.Session(), rs)
			require.NoError(t, rs.Close())
		}
		require.ErrorContains(t, err, "mock masking policy load error", sql)
	}
	testfailpoint.Disable(t, "github.com/pingcap/tidb/pkg/infoschema/mockLoadMaskingPoliciesError")
	tk.MustQuery("select c from t_load where id=1").Check(testkit.Rows("XXXXXX"))
}

func TestMaskingPolicyCorrelatedResults(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_corr(id int primary key, c varchar(20), d varchar(20), n int)")
	tk.MustExec("insert into t_corr values (1, 'secret', 'untouched', 0)")
	tk.MustExec("create masking policy p on t_corr(c) as mask_full(c) restrict on none")
	for _, sql := range []string{
		"select d.c from t_corr t, lateral (select t.c as c) d",
		"select e.c from t_corr t, lateral (select t.c as c) d, lateral (select d.c as c) e",
		"select (select (select t.c)) from t_corr t",
		"update t_corr set d = (select t_corr.c) where id = 1",
		"update t_corr set d = (select concat(t_corr.c, '!')) where id = 1",
		"update t_corr set d = (select (select t_corr.c)) where id = 1",
		"update t_corr set n = (select t_corr.c) where id = 1",
	} {
		err := tk.ExecToErr(sql)
		require.ErrorContains(t, err, "masking policy cannot be preserved through scalar subquery", sql)
		require.NotContains(t, err.Error(), "secret", sql)
	}
	tk.MustQuery("select d, n from t_corr").Check(testkit.Rows("untouched 0"))
	tk.MustQuery("select d.id from t_corr t, lateral (select t.id) d").Check(testkit.Rows("1"))
	tk.MustExec("update t_corr set d = (select cast(t_corr.id as char)) where id = 1")
	tk.MustQuery("select d from t_corr").Check(testkit.Rows("1"))
	// A correlated predicate must continue to compare original values.
	tk.MustExec("update t_corr set d = (select 'safe' from t_corr s where s.c = t_corr.c limit 1)")
	tk.MustQuery("select d from t_corr").Check(testkit.Rows("safe"))
	tk.MustExec("alter table t_corr disable masking policy p")
	tk.MustExec("update t_corr set d = (select t_corr.c) where id = 1")
	tk.MustQuery("select d from t_corr").Check(testkit.Rows("secret"))
}

func TestMaskingPolicyCollationSubstitution(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_collation(c varchar(20) collate utf8mb4_bin)")
	tk.MustExec("insert into t_collation values ('secret')")
	tk.MustExec("create masking policy p on t_collation(c) as _utf8mb4'X' collate utf8mb4_general_ci")
	tk.MustQuery("select c from t_collation").Check(testkit.Rows("X"))
	// An expression over the column is built for the value its policy returns:
	// the result has the policy's collation, and a comparison in the select
	// list is case-insensitive as that collation is.
	tk.MustQuery("select collation(c), concat(c, '!'), collation(concat(c, '!')) from t_collation").
		Check(testkit.Rows("utf8mb4_general_ci X! utf8mb4_general_ci"))
	tk.MustQuery("select c = 'x', c = 'X', concat(c, '!') = 'x!', c in ('x', 'y'), c between 'w' and 'x' from t_collation").
		Check(testkit.Rows("1 1 1 1 1"))
	tk.MustQuery("select x from (select c = 'x' as x from t_collation) s").Check(testkit.Rows("1"))
	// A predicate compares the stored value with the column's binary collation.
	tk.MustQuery("select count(*) from t_collation where c = 'secret'").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t_collation where c = 'SECRET'").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t_collation where c = 'x'").Check(testkit.Rows("0"))
	// Without the policy the same select-list comparison is binary.
	tk.MustExec("alter table t_collation disable masking policy p")
	tk.MustQuery("select collation(c), c = 'SECRET', c = 'secret' from t_collation").Check(testkit.Rows("utf8mb4_bin 0 1"))
}

func TestMaskingBuiltinNullableParameters(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_nullable(c varchar(20) not null, d date not null)")
	tk.MustExec("insert into t_nullable values ('secret', '2001-02-03')")
	for _, expr := range []string{"mask_partial(c, 0, 0, NULL)", "mask_partial(c, NULL, 0, '*')", "mask_partial(c, 0, NULL, '*')", "mask_date(d, NULL)"} {
		tk.MustQuery("select " + expr + " is null from t_nullable").Check(testkit.Rows("1"))
		tk.MustQuery("select c from t_nullable where " + expr + " is null").Check(testkit.Rows("secret"))
	}
}

func TestMaskingPolicyCaseFoldingWithoutPolicies(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_case(id int primary key)")
	tk.MustExec("insert into t_case values (1), (2)")
	sql := "select id from t_case where id = case when 1 then 1 else sleep(0) end"
	tk.MustQuery(sql).Check(testkit.Rows("1"))
	require.Contains(t, tk.MustQuery("explain format='brief' " + sql).Rows()[0][0], "Point_Get")
}

func TestMaskingPolicyPredicateSubqueryResults(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_pred_src(id int primary key, c varchar(20))")
	tk.MustExec("insert into t_pred_src values (1, 'secret')")
	tk.MustExec("create masking policy p on t_pred_src(c) as mask_full(c)")
	tk.MustExec("create table t_pred_dst(id int, c varchar(20))")
	reset := func() {
		tk.MustExec("delete from t_pred_dst")
		tk.MustExec("insert into t_pred_dst values (1, 'secret'), (2, 'XXXXXX')")
	}
	reset()
	// Subquery results in predicates are masked; base columns keep their values.
	for _, tc := range []struct {
		pred string
		ids  []string
	}{
		{"c = (select c from t_pred_src)", []string{"2"}},
		// MIN/MAX results keep their original values in predicates.
		{"c = (select max(c) from t_pred_src)", []string{"1"}},
		{"c = (select s.c from t_pred_src s where s.id = t_pred_dst.id)", nil},
		{"c in (select c from t_pred_src)", []string{"2"}},
		{"c not in (select c from t_pred_src)", []string{"1"}},
		{"c = any (select c from t_pred_src)", []string{"2"}},
		{"c <> all (select c from t_pred_src)", []string{"1"}},
		{"(select c from t_pred_src) = 'secret'", nil},
		{"c = (select c from (select c from t_pred_src) d)", []string{"2"}},
		{"c = (with x as (select c from t_pred_src) select c from x)", []string{"2"}},
		// Comparisons inside the subquery use the original values.
		{"exists (select 1 from t_pred_src s where s.c = t_pred_dst.c)", []string{"1"}},
		{"id = (select max(s.id) from t_pred_src s where s.c = t_pred_dst.c)", []string{"1"}},
	} {
		rows := make([][]any, 0, len(tc.ids))
		for _, id := range tc.ids {
			rows = append(rows, []any{id})
		}
		tk.MustQuery("select id from t_pred_dst where " + tc.pred + " order by id").Check(rows)
		tk.MustExec("update t_pred_dst set c = 'hit' where " + tc.pred)
		tk.MustQuery("select id from t_pred_dst where c = 'hit' order by id").Check(rows)
		reset()
		tk.MustExec("delete from t_pred_dst where " + tc.pred)
		tk.MustQuery("select count(*) from t_pred_dst").Check(testkit.Rows(strconv.Itoa(2 - len(tc.ids))))
		reset()
	}
	// Results whose policy cannot be preserved keep comparing original values.
	for _, pred := range []string{
		"(select s.c) = 'secret'",
		"(select s.c) in ('secret')",
		"'secret' in (select s.c)",
		"'secret' = any (select s.c)",
		"(select group_concat(c) from t_pred_src) = 'secret'",
		"(select count(distinct c) from t_pred_src) = 1",
	} {
		tk.MustQuery("select s.id from t_pred_src s where " + pred).Check(testkit.Rows("1"))
	}
	// Each column of a row result is handled separately.
	for _, pred := range []string{
		"(c, 1) in (select c, count(c) from t_pred_src group by c)",
		"(c, 1) = any (select c, count(c) from t_pred_src group by c)",
		"(c, 1) in (select concat(c, ''), count(c) from t_pred_src group by c)",
		"(c, 1) = any (select concat(c, ''), count(c) from t_pred_src group by c)",
		"(c, 'secret') in (select c, group_concat(c) from t_pred_src group by c)",
	} {
		tk.MustQuery("select id from t_pred_dst where " + pred).Check(testkit.Rows("2"))
	}
	tk.MustQuery("select s.id from t_pred_src s where (select s.c) = 'XXXXXX'").Check(testkit.Rows())
	tk.MustQuery("select id from t_pred_dst where id = (select count(c) from t_pred_src) order by id").Check(testkit.Rows("1"))
	// A UNION column takes the policy of its protected branch for all rows.
	tk.MustExec("insert into t_pred_dst values (3, 'public')")
	for _, pred := range []string{
		"c in (select c from t_pred_src union all select 'public')",
		"c in (select 'public' union all select c from t_pred_src)",
		"c = any (select c from t_pred_src union select 'public')",
		"(c, 1) in (select c, 1 from t_pred_src union all select 'public', 1)",
		"c in (select count(c) from t_pred_src union all select c from t_pred_src)",
		"c in (select c from t_pred_src union all select group_concat(c) from t_pred_src)",
		"c in (select concat(c, '') from t_pred_src union all select 'public')",
		"c in (select concat(id, c) from t_pred_src union all select 'public')",
		"c in (select 'public' union all (select 'other' union all select c from t_pred_src))",
		"c in (select c from t_pred_src group by c union all select 'public')",
		"c in (select c from (select c, row_number() over () rn from t_pred_src) x union all select 'public')",
	} {
		tk.MustQuery("select id from t_pred_dst where " + pred + " order by id").Check(testkit.Rows("2"))
	}
	// A materialized CTE passes the policy of its seed column to a UNION.
	for _, sql := range []string{
		"with x as (select c from t_pred_src) select id from t_pred_dst where c in (select c from x union all select c from x) order by id",
		"with x as (select c from t_pred_src) select id from t_pred_dst where c in (select c from x union all select 'public') and c in (select c from x) order by id",
		"with x as (select c from t_pred_src) select id from t_pred_dst where c in (select 'public' union all select c from x) and exists (select 1 from x) order by id",
	} {
		tk.MustQuery(sql).Check(testkit.Rows("2"))
	}
	tk.MustQuery("with recursive x as (select c from t_pred_src union all select c from x where false) " +
		"select id from t_pred_dst where c in (select c from x union all select 'public') order by id").Check(testkit.Rows("1", "3"))
	// Derived values, such as aggregates, do not carry a policy into a UNION.
	tk.MustQuery("select id from t_pred_dst where c in (select max(c) from t_pred_src union all select 'public') order by id").Check(testkit.Rows("1", "3"))
	tk.MustExec("delete from t_pred_dst where id = 3")
	tk.MustQuery("select id from t_pred_dst group by id having max(c) = (select c from t_pred_src) order by id").Check(testkit.Rows("2"))
	tk.MustQuery("select id from t_pred_dst order by (select c from t_pred_src), id").Check(testkit.Rows("1", "2"))
	tk.MustExec("alter table t_pred_src disable masking policy p")
	tk.MustQuery("select id from t_pred_dst where c = (select c from t_pred_src)").Check(testkit.Rows("1"))
}

func TestMaskingPolicyUsingNaturalJoin(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_common(c varchar(20))")
	tk.MustExec("insert into t_common values ('secret')")
	tk.MustExec("create masking policy p on t_common(c) as mask_full(c)")
	for _, sql := range []string{
		"select l.c,r.c from t_common l join t_common r using(c)",
		"select l.c,r.c from t_common l natural join t_common r",
		"select l.c,r.c from t_common l left join t_common r using(c)",
		"select l.c,r.c from t_common l right join t_common r using(c)",
		"select l.c,r.c from t_common l join t_common r using(c) where r.c='secret'",
	} {
		tk.MustQuery(sql).Check(testkit.Rows("XXXXXX XXXXXX"))
	}
	tk.MustQuery("select r.* from t_common l join t_common r using(c)").Check(testkit.Rows("XXXXXX"))
	tk.MustQuery("select concat(r.c,'!') from t_common l join t_common r using(c)").Check(testkit.Rows("XXXXXX!"))
}

func TestMaskingPolicyNotNullResultExpressions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_mask_null(id int primary key, c varchar(20) not null)")
	tk.MustExec("insert into t_mask_null values (1,'secret'),(2,'hidden')")
	tk.MustExec("create masking policy p on t_mask_null(c) as mask_null(c)")
	tk.MustQuery("select c,c is null,c is not null from t_mask_null order by id").Check(testkit.Rows("<nil> 1 0", "<nil> 1 0"))
	tk.MustQuery("select c is null from (select c from t_mask_null) s").Check(testkit.Rows("1", "1"))
	// A subquery in an earlier field keeps the following fields in the field list.
	tk.MustExec("create table t_mask_null_sub(id int)")
	tk.MustExec("insert into t_mask_null_sub values (1),(2)")
	for _, sub := range []string{
		"id in (select id from t_mask_null_sub order by id limit 1)",
		"id = any (select id from t_mask_null_sub order by id limit 1)",
		"exists (select id from t_mask_null_sub order by id limit 1)",
		"(select id from t_mask_null_sub order by id limit 1)",
	} {
		tk.MustQuery("select " + sub + " is not null,c is null from t_mask_null order by id").Check(testkit.Rows("1 1", "1 1"))
	}
	// Unreachable mutable branches still fold before masking is applied.
	tk.MustQuery("select if(1,c,concat(c,rand())) from t_mask_null").Check(testkit.Rows("<nil>", "<nil>"))
	tk.MustQuery("select c from t_mask_null where c is null").Check(testkit.Rows())
	tk.MustQuery("select c from t_mask_null where c is not null").Check(testkit.Rows("<nil>", "<nil>"))
	for _, sql := range []string{
		"select c from t_mask_null where id=1",
		"select c from t_mask_null where id in (1,2)",
		"select c from t_mask_null",
	} {
		rs, err := tk.Exec(sql)
		require.NoError(t, err)
		require.False(t, mysql.HasNotNullFlag(rs.Fields()[0].Column.GetFlag()), sql)
		require.NoError(t, rs.Close())
	}
}

func TestMaskingPolicyViewWithShadowingTemporaryTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_shadow(id int primary key, c varchar(20))")
	tk.MustExec("insert into t_shadow values (1,'secret')")
	tk.MustExec("create masking policy p on t_shadow(c) as mask_full(c)")
	tk.MustExec("create view v_shadow as select id, c from t_shadow")
	tk.MustExec("create table t_shadow_dst(c varchar(20))")
	tk.MustExec("insert into t_shadow_dst values ('secret'),('XXXXXX')")
	check := func() {
		tk.MustQuery("select c from v_shadow").Check(testkit.Rows("XXXXXX"))
		tk.MustQuery("select c from v_shadow where id = 1").Check(testkit.Rows("XXXXXX"))
		tk.MustQuery("select concat(c,'!') from v_shadow").Check(testkit.Rows("XXXXXX!"))
		tk.MustQuery("select c from t_shadow_dst where c in (select c from v_shadow)").Check(testkit.Rows("XXXXXX"))
		tk.MustQuery("select c from t_shadow_dst where c in (select c from v_shadow union all select 'public')").Check(testkit.Rows("XXXXXX"))
	}
	check()
	// The view still reads the permanent table, so its policy still applies
	// when a local temporary table takes the name in this session.
	tk.MustExec("create temporary table t_shadow(id int primary key, c varchar(20))")
	check()
	tk.MustQuery("select c from t_shadow").Check(testkit.Rows())
	tk.MustExec("drop temporary table t_shadow")
	tk.MustExec("create temporary table t_shadow(c varchar(20), id int primary key)")
	check()
}

func TestMaskingPolicyAfterCTEWithoutFrom(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_cte_flag(id int primary key, c varchar(20))")
	tk.MustExec("insert into t_cte_flag values (1,'secret')")
	tk.MustExec("create masking policy p on t_cte_flag(c) as mask_full(c)")
	// A WITH clause whose body has no FROM clause must not leave the builder in
	// "building a CTE" state: the enclosing query still masks its result.
	for _, sql := range []string{
		"select c, (with x as (select 1) select 1) from t_cte_flag",
		"select c, (with x as (select 1) select * from x) from t_cte_flag",
		"select c, (with x as (select 1), y as (select 2) select 1) from t_cte_flag",
		"select c, (with recursive x as (select 1 union all select 1 from x where false) select 1) from t_cte_flag",
	} {
		tk.MustQuery(sql).Check(testkit.Rows("XXXXXX 1"))
	}
	tk.MustQuery("select c from t_cte_flag where id in (with x as (select 1) select 1)").Check(testkit.Rows("XXXXXX"))
	tk.MustQuery("select c from t_cte_flag where exists (with x as (select 1) select 1)").Check(testkit.Rows("XXXXXX"))
	tk.MustQuery("with x as (select 1) select c from t_cte_flag").Check(testkit.Rows("XXXXXX"))
}

func TestMaskingPolicyPredicateUsingJoinRedundantColumn(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_using_a(id int primary key, c varchar(20))")
	tk.MustExec("create table t_using_b(id int primary key, c varchar(20))")
	tk.MustExec("insert into t_using_a values (1,'secret')")
	tk.MustExec("insert into t_using_b values (1,'secret')")
	tk.MustExec("create table t_using_probe(id int primary key, c varchar(20))")
	tk.MustExec("insert into t_using_probe values (1,'secret'),(2,'XXXXXX')")
	tk.MustExec("create masking policy p on t_using_b(c) as mask_full(c)")
	// The UNION branch projects the side of the common column that the join
	// keeps only in its full schema. The predicate still compares the masked
	// value, as it does without the UNION.
	for _, sql := range []string{
		"select id from t_using_probe where c in (select b.c from t_using_a a join t_using_b b using (c)) order by id",
		"select id from t_using_probe where c in (select b.c from t_using_a a join t_using_b b using (c) union all select 'public') order by id",
		"select id from t_using_probe where c in (select b.c from t_using_a a natural join t_using_b b union all select 'public') order by id",
		"select id from t_using_probe where c in (select 'public' union all select b.c from t_using_a a join t_using_b b using (id, c) where a.id > 0) order by id",
		"select id from t_using_probe where c in (select b.c from t_using_a a join t_using_b b using (id) union all select 'public') order by id",
	} {
		tk.MustQuery(sql).Check(testkit.Rows("2"))
	}
}

func TestMaskingPolicyNullableCastSubstitution(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table nullable_cast(c varchar(20) not null)")
	tk.MustExec("insert into nullable_cast values ('42')")
	tk.MustExec("create masking policy p on nullable_cast(c) as mask_null(c)")
	for _, expr := range []string{"c + 1", "cast(c as signed)", "cast(c as decimal(10,2)) + 1", "cast(c as double) + 1"} {
		tk.MustQuery("select " + expr + ", (" + expr + ") is null, (" + expr + ") is not null, if((" + expr + ") is null, 'yes', 'no'), case when (" + expr + ") is null then 'yes' else 'no' end from nullable_cast").Check(testkit.Rows("<nil> 1 0 yes yes"))
	}
	tk.MustQuery("select (c + 1) is null from nullable_cast where c + 1 = 43").Check(testkit.Rows("1"))
	tk.MustQuery("select (v + 1) is null from (select c as v from nullable_cast) d").Check(testkit.Rows("1"))
	tk.MustExec("prepare st from 'select c + 1, (c + 1) is null from nullable_cast'")
	for range 2 {
		tk.MustQuery("execute st").Check(testkit.Rows("<nil> 1"))
	}
}

func TestMaskingPolicyIfnullFallback(t *testing.T) {
	tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
	tk.MustExec("use test")
	for _, declaration := range []string{"not null", "null"} {
		tk.MustExec("create table ifnull_source(c varchar(20) " + declaration + ")")
		tk.MustExec("insert into ifnull_source values('secret')")
		tk.MustQuery("select ifnull(c,'fallback') from ifnull_source").Check(testkit.Rows("secret"))
		tk.MustExec("create masking policy p on ifnull_source(c) as mask_null(c)")
		tk.MustQuery("select ifnull(c,'fallback') from ifnull_source").Check(testkit.Rows("fallback"))
		tk.MustExec("drop table ifnull_source")
	}
}

func TestMaskingPolicyComparisonOnNotNullColumn(t *testing.T) {
	tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
	tk.MustExec("use test")
	// Each comparison is constant for the original NOT NULL column, so its raw
	// result is known without reading the row.
	cases := []struct{ expr, raw string }{
		{"u < 0", "0"}, {"u >= 0", "1"}, {"u = -1", "0"}, {"u <> -1", "1"}, {"-1 < u", "1"},
		{"u > -1", "1"}, {"u <= -1", "0"}, {"0 > u", "0"}, {"not (u < 0)", "1"},
		{"u between -5 and -1", "0"}, {"u not between -5 and -1", "1"}, {"u in (-1)", "0"},
		{"(u, ti) = (-1, 1)", "0"},
		{"ti < 1000.5", "1"}, {"1000.5 > ti", "1"}, {"ti > -1000.5", "1"}, {"ti = 1.5", "0"},
		{"ti in (1.5)", "0"}, {"ti in (1.5, 2.5)", "0"},
		{"b = 18446744073709551615", "0"},
	}
	derived := []struct{ expr, raw, masked string }{
		{"u < 0 or u is null", "0", "1"},
		{"(u < 0) is null", "0", "1"},
		{"coalesce(u < 0, 9)", "0", "9"},
		{"if(u >= 0, 'yes', 'no')", "yes", "no"},
		{"u <=> -1", "0", "0"},
	}
	for _, declaration := range []string{"not null", "null"} {
		tk.MustExec("create table cmp_source(id int primary key, u int unsigned " + declaration +
			", ti tinyint " + declaration + ", b bigint " + declaration + ", key(u))")
		tk.MustExec("insert into cmp_source values (1, 1, 1, 1)")
		for _, tc := range cases {
			tk.MustQuery("select " + tc.expr + " from cmp_source").Check(testkit.Rows(tc.raw))
		}
		for _, tc := range derived {
			tk.MustQuery("select " + tc.expr + " from cmp_source").Check(testkit.Rows(tc.raw))
		}
		for _, col := range []string{"u", "ti", "b"} {
			tk.MustExec("create masking policy p_" + col + " on cmp_source(" + col + ") as mask_null(" + col + ")")
		}
		for _, tc := range cases {
			tk.MustQuery("select " + tc.expr + " from cmp_source").Check(testkit.Rows("<nil>"))
			tk.MustQuery("select x from (select " + tc.expr + " as x from cmp_source) s").Check(testkit.Rows("<nil>"))
			tk.MustQuery("select x is null from (select " + tc.expr + " as x from cmp_source) s").Check(testkit.Rows("1"))
			tk.MustQuery("select " + tc.expr + ", count(*) from cmp_source group by u, ti, b").Check(testkit.Rows("<nil> 1"))
			// Predicates keep comparing the original values.
			tk.MustQuery("select count(*) from cmp_source where " + tc.expr).Check(testkit.Rows(tc.raw))
		}
		tk.MustQuery("select count(*) from cmp_source s join cmp_source r on s.u >= 0 and r.u < 0").Check(testkit.Rows("0"))
		tk.MustQuery("select count(*) from cmp_source s join cmp_source r on s.u >= 0 and r.u > -1").Check(testkit.Rows("1"))
		for _, tc := range derived {
			tk.MustQuery("select " + tc.expr + " from cmp_source").Check(testkit.Rows(tc.masked))
			tk.MustQuery("select x from (select " + tc.expr + " as x from cmp_source) s").Check(testkit.Rows(tc.masked))
		}
		tk.MustQuery("select u < 0, u from cmp_source where u >= 0 and not (u < 0)").Check(testkit.Rows("<nil> <nil>"))
		rs, err := tk.Exec("select u < 0, ti < 1000.5 from cmp_source")
		require.NoError(t, err)
		for _, field := range rs.Fields() {
			require.False(t, mysql.HasNotNullFlag(field.Column.GetFlag()))
		}
		require.NoError(t, rs.Close())

		// A cached plan must not keep a result folded for one parameter value.
		tk.MustExec("prepare cmp_int from 'select u < ?, u >= ? from cmp_source'")
		tk.MustExec("prepare cmp_where from 'select count(*) from cmp_source where u >= ?'")
		for i, param := range []string{"0", "-1", "0", "2"} {
			tk.MustExec("set @p = " + param)
			tk.MustQuery("execute cmp_int using @p, @p").Check(testkit.Rows("<nil> <nil>"))
			if i > 0 {
				tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
			}
			want := "1"
			if param == "2" {
				want = "0"
			}
			tk.MustQuery("execute cmp_where using @p").Check(testkit.Rows(want))
		}
		tk.MustExec("prepare cmp_decimal from 'select ti < ?, ti = ? from cmp_source'")
		for _, param := range []string{"1000.5", "1.5", "-1000.5"} {
			tk.MustExec("set @p = " + param)
			tk.MustQuery("execute cmp_decimal using @p, @p").Check(testkit.Rows("<nil> <nil>"))
		}
		tk.MustExec("drop table cmp_source")
	}
}

func TestMaskingPolicyTypeChangingResultExpressions(t *testing.T) {
	tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
	tk.MustExec("use test")
	tk.MustExec("create table type_source(id int primary key, ti tinyint not null, c int, d datetime, y year, " +
		"dec2 decimal(5,2), s varchar(10), u int)")
	tk.MustExec("insert into type_source values (1, 1, 1, '2024-01-02 03:04:05', 2012, 1.5, 'x2', 1)")
	// raw is the result for the original column, masked the result of the same
	// expression over the value its policy returns: 1.5 for ti, 'x1' for c,
	// '2024' for d and '2012' for y.
	cases := []struct{ expr, raw, masked string }{
		{"ti < 1.3", "1", "0"}, {"ti = 1.5", "0", "1"}, {"ti <=> 1.5", "0", "1"}, {"ti = 1", "1", "0"},
		{"ti between 1.4 and 1.6", "0", "1"}, {"ti not between 1.4 and 1.6", "1", "0"},
		{"ti in (1.5)", "0", "1"}, {"ti in (1.5, 7)", "0", "1"}, {"ti not in (1.5, 7)", "1", "0"},
		{"not (ti < 1.3)", "0", "1"}, {"if(ti = 1.5, 'y', 'n')", "n", "y"},
		{"case when ti = 1.5 then 'y' else 'n' end", "n", "y"}, {"case ti when 1.5 then 'y' else 'n' end", "n", "y"},
		{"(ti, 1) = (1.5, 1)", "0", "1"}, {"nullif(ti, 1.5)", "1", "<nil>"}, {"ti + 1", "2", "2.5"},
		// An explicit cast is kept as written.
		{"cast(ti as signed)", "1", "2"}, {"cast(ti as signed) = 2", "0", "1"},
		// Only one side of the comparison is masked.
		{"ti = dec2", "0", "1"}, {"dec2 = ti", "0", "1"}, {"c < s", "0", "1"}, {"s > c", "0", "1"}, {"c = u", "1", "0"},
		{"c = 'x1'", "0", "1"}, {"c <=> 'x1'", "0", "1"}, {"c = 1", "1", "0"}, {"c = 0", "0", "1"}, {"c + 1", "2", "1"},
		{"c in ('x1')", "0", "1"}, {"c in ('x1', 'y')", "0", "1"}, {"c between 'x0' and 'x2'", "0", "1"},
		{"case c when 'x1' then 'y' else 'n' end", "n", "y"}, {"if(c = 'x1', c, 'no')", "no", "x1"},
		{"concat(c, '!')", "1!", "x1!"}, {"coalesce(c, 'z')", "1", "x1"},
		{"d = '2024'", "<nil>", "1"}, {"d = 2024", "0", "1"}, {"d > '2024-01-01'", "1", "0"},
		{"d between '2023' and '2025'", "<nil>", "1"}, {"d in ('2024', '2')", "<nil>", "1"},
		{"y = 12", "1", "0"}, {"y = '2012'", "1", "1"}, {"y < 13", "1", "0"}, {"y between 11 and 13", "1", "0"},
	}
	check := func(masked bool) {
		for _, tc := range cases {
			want := tc.raw
			if masked {
				want = tc.masked
			}
			tk.MustQuery("select " + tc.expr + " from type_source").Check(testkit.Rows(want))
			tk.MustQuery("select x from (select " + tc.expr + " as x from type_source) z").Check(testkit.Rows(want))
			tk.MustQuery("select count(*) from type_source where (" + tc.expr + ") <=> " + "'" + tc.raw + "'").
				Check(testkit.Rows(map[bool]string{true: "1", false: "0"}[tc.raw != "<nil>"]))
		}
	}
	plans := func() (rows [][]any) {
		for _, expr := range []string{"u < 1.3", "u = '1'", "y < 13", "d = 20240102030405", "u in ('1', 2.0)", "d between '2023' and '2025'"} {
			rows = append(rows, tk.MustQuery("explain format='brief' select "+expr+" from type_source").Rows()...)
		}
		return rows
	}
	check(false)
	ordinary := plans()
	// A policy anywhere makes result expressions keep what masking may need.
	// A column without a policy still gets the ordinary expression and result.
	tk.MustExec("create table type_other(c int)")
	tk.MustExec("create masking policy p on type_other(c) as mask_null(c)")
	check(false)
	require.Equal(t, ordinary, plans())

	tk.MustExec("create masking policy p_ti on type_source(ti) as ti + 0.5")
	tk.MustExec("create masking policy p_c on type_source(c) as concat('x', c)")
	tk.MustExec("create masking policy p_d on type_source(d) as date_format(d, '%Y')")
	tk.MustExec("create masking policy p_y on type_source(y) as concat(y, '')")
	check(true)
	// WITH ROLLUP replaces the grouping columns of a built expression in place.
	tk.MustQuery("select ti < 1.3, ti = 1.5, c = 'x1', count(*) from type_source group by ti, c with rollup").Sort().
		Check(testkit.Rows("0 1 1 1", "0 1 <nil> 1", "<nil> <nil> <nil> 1"))
	tk.MustQuery("select max(ti) < 1.3, min(ti) between 1.4 and 1.6, distinct_x from " +
		"(select distinct ti = 1.5 as distinct_x, ti from type_source) z group by distinct_x").Check(testkit.Rows("0 1 1"))
	tk.MustQuery("select a.ti < 1.3, b.c = 'x1', a.ti = b.dec2 from type_source a join type_source b on a.ti = b.c").
		Check(testkit.Rows("0 1 1"))

	// A view and a CTE are query blocks of their own.
	tk.MustExec("create view type_view as select ti < 1.3 as lt, c = 'x1' as eq, d between '2023' and '2025' as bt from type_source")
	tk.MustQuery("select lt, eq, bt, not lt from type_view").Check(testkit.Rows("0 1 1 1"))
	tk.MustQuery("with w as (select ti in (1.5, 7) as x, c in ('x1') as y from type_source) select x, y, x + y from w").
		Check(testkit.Rows("1 1 2"))

	// The parameters of a cached plan are compared with the masked values too.
	tk.MustExec("prepare cmp_cached from 'select ti < ?, ti + ? from type_source'")
	tk.MustExec("prepare cmp_derived from 'select x from (select ti <= ? as x from type_source) z'")
	for i, tc := range []struct{ n, want string }{
		{"2", "1 3.5"}, {"1", "0 2.5"}, {"2", "1 3.5"}, {"1", "0 2.5"},
	} {
		tk.MustExec("set @n = " + tc.n)
		tk.MustQuery("execute cmp_cached using @n, @n").Check(testkit.Rows(tc.want))
		if i > 0 {
			tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
		}
		tk.MustQuery("execute cmp_derived using @n").Check(testkit.Rows(tc.want[:1]))
	}
	// A policy calling CONCAT is not cached; each execution builds its plan.
	tk.MustExec("prepare cmp_string from 'select c = ?, c in (?, ''y'') from type_source'")
	for _, tc := range []struct{ str, want string }{{"'x1'", "1 1"}, {"'x2'", "0 0"}, {"'x1'", "1 1"}, {"0", "1 1"}} {
		tk.MustExec("set @s = " + tc.str)
		tk.MustQuery("execute cmp_string using @s, @s").Check(testkit.Rows(tc.want))
	}
	tk.MustExec("prepare cmp_decimal from 'select ti < ?, ti = ?, ti between ? and ?, ti in (?, 7) from type_source'")
	for _, tc := range []struct{ lt, eq, want string }{
		{"1.3", "1.5", "0 1 0 1"}, {"1.6", "1.4", "1 0 1 0"}, {"1.6", "1.5", "1 1 1 1"}, {"1", "2", "0 0 0 0"},
	} {
		tk.MustExec("set @lt = " + tc.lt + ", @eq = " + tc.eq)
		tk.MustQuery("execute cmp_decimal using @lt, @eq, @eq, @lt, @eq").Check(testkit.Rows(tc.want))
	}
}

func TestMaskingPolicyTypeChangingPlanCache(t *testing.T) {
	for _, instance := range []bool{false, true} {
		t.Run(fmt.Sprint("instance=", instance), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			root := testkit.NewTestKit(t, store)
			root.MustExec("use test")
			root.MustExec("create table type_cache(id int primary key, ti tinyint not null)")
			root.MustExec("insert into type_cache values (1, 1)")
			root.MustExec("create masking policy p on type_cache(ti) as ti + 0.5")
			ctx := context.Background()
			if instance {
				ctx = context.WithValue(ctx, core.PlanCacheKeyEnableInstancePlanCache{}, true)
			}
			// The masked value is 1.5 and the stored one 1.
			run := func(tk *testkit.TestKit, firstHit bool) {
				tk.MustExecWithContext(ctx, "use test")
				tk.MustExecWithContext(ctx, "prepare st from 'select ti in (?, ?), ti between ? and ?, x from "+
					"(select ti not in (?, ?) as x from type_cache) s, type_cache'")
				for i, tc := range []struct{ a, b, want string }{
					{"1", "2", "0 1 1"}, {"0", "1", "0 0 1"}, {"2", "3", "0 0 1"}, {"1", "2", "0 1 1"},
				} {
					tk.MustExecWithContext(ctx, "set @a="+tc.a+", @b="+tc.b)
					tk.MustQueryWithContext(ctx, "execute st using @a, @b, @a, @b, @a, @b").Check(testkit.Rows(tc.want))
					hit := "0"
					if i > 0 || firstHit {
						hit = "1"
					}
					tk.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows(hit))
				}
				tk.MustQueryWithContext(ctx, "select count(*) from type_cache where ti in (1, 2) and ti between 1 and 2").
					Check(testkit.Rows("1"))
			}
			first := testkit.NewTestKit(t, store)
			run(first, false)
			second := testkit.NewTestKit(t, store)
			run(second, instance)
			// A plan cached by a closed session is still used by the others.
			first.Session().Close()
			run(second, true)
			run(testkit.NewTestKit(t, store), instance)
		})
	}
}

func TestMaskingPolicyRollupNestedExpressionWithoutPolicy(t *testing.T) {
	tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
	tk.MustExec("use test")
	tk.MustExec("create table rollup_plain(a int, b int, s varchar(10))")
	tk.MustExec("insert into rollup_plain values (1, 2, 'x'), (1, 3, 'y'), (2, 2, 'z')")
	// Nested over the grouping columns: with rollup replaces them in place at
	// every level of the built expression.
	nested := "a + b"
	for range 12 {
		nested = "(" + nested + ") + if(a < b, a, b) * 1.5"
	}
	queries := []string{
		"select " + nested + ", count(*) from rollup_plain group by a, b with rollup",
		"select concat(a, '-', ifnull(b, 'all')), a between 1 and b, a in (b, 1.0), grouping(a), count(*) " +
			"from rollup_plain group by a, b with rollup",
		"select x, n from (select " + nested + " as x, count(*) as n from rollup_plain group by a, b with rollup) z",
	}
	ordinary := make([][][]any, 0, len(queries))
	for _, query := range queries {
		ordinary = append(ordinary, tk.MustQuery(query).Sort().Rows())
	}
	require.Len(t, ordinary[0], 6)
	tk.MustExec("create table rollup_masked(c int)")
	tk.MustExec("create masking policy p on rollup_masked(c) as mask_null(c)")
	for i, query := range queries {
		require.Equal(t, ordinary[i], tk.MustQuery(query).Sort().Rows(), query)
	}
	// The same expression over a masked grouping column is built for its policy.
	tk.MustExec("create masking policy p on rollup_plain(a) as a + 0.5")
	tk.MustQuery("select a, a + b, a < 1.3, count(*) from rollup_plain where b = 2 group by a, b with rollup").Sort().
		Check(testkit.Rows("1.5 3.5 0 1", "1.5 <nil> 0 1", "2.5 4.5 0 1", "2.5 <nil> 0 1", "<nil> <nil> <nil> 2"))
}

func TestMaskingPolicyExplicitCollationAndLength(t *testing.T) {
	tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
	tk.MustExec("use test")
	// The policies return a value longer than their columns, with another
	// collation for c. The original values are NULL or one character.
	tk.MustExec("create table collate_masked(id int primary key, c varchar(1) collate utf8mb4_bin, k char(1))")
	tk.MustExec("insert into collate_masked values (1, null, null), (2, null, null), (3, 'z', 'z')")
	tk.MustExec("create table collate_plain(id int primary key)")
	tk.MustExec("insert into collate_plain values (1), (2), (3)")
	policies := map[string]string{"c": "_utf8mb4'Alphabet' collate utf8mb4_general_ci", "k": "'Alphabet'"}
	for col, policy := range policies {
		tk.MustExec("create masking policy p_" + col + " on collate_masked(" + col + ") as " + policy)
	}
	// X is the masked column, or the value of its policy over the plain table:
	// every expression must give the same result, type and error for both.
	exprs := []string{
		"X", "collation(X)", "coercibility(X)", "length(X)", "charset(X)",
		"X collate utf8mb4_bin", "collation(X collate utf8mb4_bin)", "coercibility(X collate utf8mb4_bin)",
		"length(X collate utf8mb4_bin)", "charset(X collate utf8mb4_bin)",
		"X collate utf8mb4_bin = 'Alphabet'", "X collate utf8mb4_bin = 'ALPHABET'", "X collate utf8mb4_general_ci = 'ALPHABET'",
		"X = 'ALPHABET'", "X = 'Alphabet  '", "X like 'alpha%'",
		"concat(X, '') collate utf8mb4_bin = 'ALPHABET'", "concat(X, '') collate utf8mb4_general_ci = 'ALPHABET'",
		"upper(concat(X, '') collate utf8mb4_bin) = 'ALPHABET'", "lower(concat(X, '') collate utf8mb4_bin) = 'ALPHABET'",
		"collation(concat(X, '') collate utf8mb4_bin)", "coercibility(concat(X, '') collate utf8mb4_bin)",
		"length(concat(X, '') collate utf8mb4_bin)", "collation(upper(X collate utf8mb4_bin))",
		"ifnull(X, 'n') collate utf8mb4_bin = 'ALPHABET'", "lower(X collate utf8mb4_bin) = 'ALPHABET'",
		"concat(X collate utf8mb4_bin, '!') = 'ALPHABET!'", "strcmp(X collate utf8mb4_bin, 'ALPHABET')",
		"X collate utf8mb4_bin in ('ALPHABET', 'x')", "X collate utf8mb4_general_ci in ('ALPHABET', 'x')",
		"X collate utf8mb4_bin between 'ALPHABET' and 'ALPHABET'", "X collate utf8mb4_bin like 'ALPHA%'",
		"X collate utf8mb4_bin <=> 'ALPHABET'", "(X collate utf8mb4_bin, 1) = ('ALPHABET', 1)",
		"if(X collate utf8mb4_bin = 'ALPHABET', 'y', 'n')", "case X collate utf8mb4_bin when 'ALPHABET' then 'y' else 'n' end",
		// An explicit collation on the other operand, and conflicting ones.
		"X = 'ALPHABET' collate utf8mb4_bin", "X = _utf8mb4'ALPHABET' collate utf8mb4_general_ci",
		"X in ('x', 'ALPHABET' collate utf8mb4_bin)", "X collate utf8mb4_bin = 'a' collate utf8mb4_general_ci",
		"X collate utf8mb4_unicode_ci = concat(X, '') collate utf8mb4_bin",
		// Explicit conversions are kept as written.
		"binary X = 'ALPHABET'", "cast(X as binary) = 'ALPHABET'", "cast(X as char)", "length(cast(X as char))",
		"cast(X as char(3))", "cast(X as char) collate utf8mb4_general_ci = 'ALPHABET'",
		"convert(X using utf8mb4) = 'ALPHABET'", "collation(convert(X using utf8mb4))",
		// The length of the column does not limit what its policy returns.
		"concat(X, '')", "substr(X, 2, 3)", "left(X, 3)", "right(X, 3)", "lpad(X, 10, '*')", "rpad(X, 10, '*')",
		"length(concat(X, '!'))", "repeat(X, 2)", "reverse(X)", "insert(X, 2, 1, '--')",
	}
	type outcome struct {
		rows   [][]any
		fields []string
		err    string
	}
	run := func(sql string) (o outcome) {
		rs, err := tk.Exec(sql)
		if err != nil {
			o.err = err.Error()
			return o
		}
		if rs == nil {
			return o
		}
		for _, field := range rs.Fields() {
			tp := &field.Column.FieldType
			o.fields = append(o.fields, fmt.Sprint(tp.GetType(), tp.GetFlen(), tp.GetCharset(), tp.GetCollate()))
		}
		o.rows = tk.ResultSetToResult(rs, sql).Rows()
		return o
	}
	shapes := []string{
		"select E from T order by id",
		"select x from (select E as x, id from T) s order by id",
		"with w as (select E as x, id from T) select x from w order by id",
		"select x from V order by id",
	}
	for col, policy := range policies {
		for _, expr := range exprs {
			masked := strings.ReplaceAll(expr, "X", col)
			plain := strings.ReplaceAll(expr, "X", "("+policy+")")
			// An expression that is invalid for the policy's value is rejected
			// with the error of the plain one, also when creating a view.
			maskedView := run("create or replace view collate_masked_v as select " + masked + " as x, id from collate_masked")
			plainView := run("create or replace view collate_plain_v as select " + plain + " as x, id from collate_plain")
			require.Equal(t, plainView, maskedView, expr)
			for _, shape := range shapes {
				if maskedView.err != "" && strings.Contains(shape, " V ") {
					continue
				}
				got := run(strings.NewReplacer("E", masked, "T", "collate_masked", "V", "collate_masked_v").Replace(shape))
				want := run(strings.NewReplacer("E", plain, "T", "collate_plain", "V", "collate_plain_v").Replace(shape))
				require.Equal(t, want, got, "%s over %s in %s", expr, col, shape)
			}
		}
	}
	// A predicate compares the original value with the column's collation.
	tk.MustQuery("select id from collate_masked where c collate utf8mb4_general_ci = 'Z' and k = 'z'").Check(testkit.Rows("3"))
	tk.MustQuery("select count(*) from collate_masked where c collate utf8mb4_bin = 'Alphabet' or c = 'Z'").Check(testkit.Rows("0"))

	// A prepared statement is planned once and then served from the plan cache.
	for _, stmt := range []string{
		"select X collate utf8mb4_bin = ?, concat(X, '') collate utf8mb4_bin = ?, length(X collate utf8mb4_bin), cast(X as char) from T order by id",
		"select x from (select X collate utf8mb4_bin in (?, ?) as x, id from T) s order by id",
	} {
		tk.MustExec("prepare masked_stmt from \"" + strings.NewReplacer("X", "c", "T", "collate_masked").Replace(stmt) + "\"")
		tk.MustExec("prepare plain_stmt from \"" + strings.NewReplacer("X", "("+policies["c"]+")", "T", "collate_plain").Replace(stmt) + "\"")
		for i, param := range []string{"Alphabet", "ALPHABET", "Alphabet", "A"} {
			tk.MustExec("set @p = '" + param + "'")
			want := tk.MustQuery("execute plain_stmt using @p, @p").Rows()
			tk.MustQuery("execute masked_stmt using @p, @p").Check(want)
			if i > 0 {
				tk.MustQuery("select @@last_plan_from_cache").Check(testkit.Rows("1"))
			}
		}
	}
}

// maskingOutcome is what a statement returns: its rows and result types, or its error.
type maskingOutcome struct {
	rows   [][]any
	fields []string
	err    string
}

func runMaskingOutcome(ctx context.Context, tk *testkit.TestKit, sql string) (o maskingOutcome) {
	rs, err := tk.ExecWithContext(ctx, sql)
	if err != nil {
		o.err = err.Error()
		return o
	}
	if rs == nil {
		return o
	}
	for _, field := range rs.Fields() {
		tp := &field.Column.FieldType
		o.fields = append(o.fields, fmt.Sprint(tp.GetType(), tp.GetFlen(), tp.GetCharset(), tp.GetCollate()))
	}
	o.rows = tk.ResultSetToResultWithCtx(ctx, rs, sql).Rows()
	return o
}

// TestMaskingPolicyViewCastBeforePolicy tests that a view created before the
// policy keeps a cast as written: the length of the column when the view was
// created is not stored in its definition.
func TestMaskingPolicyViewCastBeforePolicy(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_vc(id int primary key, c varchar(1))")
	tk.MustExec("insert into t_vc values (1, null)")
	tk.MustExec("create view v_before as select cast(c as char) a, cast(c as char(3)) b, convert(c, char) d from t_vc")
	def := tk.MustQuery("show create view v_before").Rows()[0][1].(string)
	require.Contains(t, def, "CAST(`c` AS CHAR) AS `a`")
	require.Contains(t, def, "CAST(`c` AS CHAR(3)) AS `b`")
	require.NotContains(t, def, "CHAR(1)")
	tk.MustExec("create masking policy p on t_vc(c) as 'Alphabet' enable")
	tk.MustExec("create view v_after as select cast(c as char) a, cast(c as char(3)) b, convert(c, char) d from t_vc")
	for _, v := range []string{"v_before", "v_after"} {
		tk.MustQuery("select a, b, d from " + v).Check(testkit.Rows("Alphabet Alp Alphabet"))
	}
	// Without a policy, the view follows a column that becomes longer.
	tk.MustExec("alter table t_vc drop masking policy p")
	tk.MustExec("alter table t_vc modify c varchar(12)")
	tk.MustExec("update t_vc set c = 'Alphabet'")
	tk.MustQuery("select a, b, d from v_before").Check(testkit.Rows("Alphabet Alp Alphabet"))
}

func TestMaskingPolicyPreparedCastBeforePolicy(t *testing.T) {
	for _, mode := range []string{"session", "instance", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
			ctx := context.Background()
			if mode == "instance" {
				ctx = context.WithValue(ctx, core.PlanCacheKeyEnableInstancePlanCache{}, true)
			}
			tk.MustExec("use test")
			if mode == "disabled" {
				tk.MustExec("set tidb_enable_prepared_plan_cache = 0")
			}
			tk.MustExec("create table cast_masked(id int primary key, c varchar(1))")
			tk.MustExec("insert into cast_masked values (1, null), (2, null)")
			tk.MustExec("create table cast_plain(id int primary key)")
			tk.MustExec("insert into cast_plain values (1), (2)")
			// X is the masked column, or the value its policy will return.
			stmts := []string{
				"select cast(X as char), length(cast(X as char)) from T order by id",
				"select cast(X as char(3)), cast(X as binary), binary X from T order by id",
				"select convert(X, char), convert(X using utf8mb4) from T order by id",
				"select X collate utf8mb4_general_ci, length(X collate utf8mb4_general_ci), X collate utf8mb4_general_ci = 'ALPHABET' from T order by id",
				"select concat(cast(X as char), '!'), cast(concat(X, '!') as char) from T order by id",
				"select x from (select cast(X as char) as x, id from T) s order by id",
				"select cast(X as char) = ?, convert(X, char) = ? from T order by id",
			}
			using := func(stmt string) string {
				if strings.Contains(stmt, "?") {
					return " using @p, @p"
				}
				return ""
			}
			tk.MustExec("set @p = 'Alphabet'")
			for i, stmt := range stmts {
				name := fmt.Sprint("early_", i)
				tk.MustExecWithContext(ctx, "prepare "+name+" from \""+strings.NewReplacer("X", "c", "T", "cast_masked").Replace(stmt)+"\"")
				// The first plan is built while no policy exists.
				for range 2 {
					require.Empty(t, runMaskingOutcome(ctx, tk, "execute "+name+using(stmt)).err)
				}
			}
			tk.MustExec("create masking policy p on cast_masked(c) as 'Alphabet'")
			for i, stmt := range stmts {
				tk.MustExecWithContext(ctx, "prepare plain from \""+strings.NewReplacer("X", "('Alphabet')", "T", "cast_plain").Replace(stmt)+"\"")
				want := runMaskingOutcome(ctx, tk, "execute plain"+using(stmt))
				require.Empty(t, want.err)
				// The first execution after the policy plans the statement again.
				name := fmt.Sprint("early_", i)
				for range 2 {
					require.Equal(t, want, runMaskingOutcome(ctx, tk, "execute "+name+using(stmt)), stmt)
				}
				tk.MustExecWithContext(ctx, "prepare fresh from \""+strings.NewReplacer("X", "c", "T", "cast_masked").Replace(stmt)+"\"")
				require.Equal(t, want, runMaskingOutcome(ctx, tk, "execute fresh"+using(stmt)), stmt)
			}
			// Without a policy, the length follows the column at each planning.
			tk.MustExec("create table cast_alter(c varchar(1))")
			tk.MustExec("insert into cast_alter values ('z')")
			tk.MustExecWithContext(ctx, "prepare altered from 'select cast(c as char), cast(c as char(3)) from cast_alter'")
			tk.MustQueryWithContext(ctx, "execute altered").Check(testkit.Rows("z z"))
			tk.MustExec("alter table cast_alter modify c varchar(10)")
			tk.MustExec("update cast_alter set c = 'zzzzzz'")
			tk.MustQueryWithContext(ctx, "execute altered").Check(testkit.Rows("zzzzzz zzz"))
		})
	}
}

func TestMaskingPolicyCollationForPolicyCharset(t *testing.T) {
	tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
	ctx := context.Background()
	tk.MustExec("use test")
	// d has no policy. The plain tables hold the same d and no masked column.
	for _, tbl := range []struct{ name, charset, policy string }{
		{"latin", "latin1", "_utf8mb4'Alphabet' collate utf8mb4_general_ci"},
		{"utf8", "utf8mb4", "_latin1'Alphabet' collate latin1_bin"},
	} {
		masked, plain := "charset_"+tbl.name, "charset_"+tbl.name+"_plain"
		tk.MustExec("create table " + masked + "(id int primary key, c varchar(10) charset " + tbl.charset + ", d varchar(10) charset " + tbl.charset + ")")
		tk.MustExec("create table " + plain + "(id int primary key, d varchar(10) charset " + tbl.charset + ")")
		tk.MustExec("insert into " + masked + " values (1, 'abc', 'xyz'), (2, null, null)")
		tk.MustExec("insert into " + plain + " values (1, 'xyz'), (2, null)")
		tk.MustExec("create masking policy p on " + masked + "(c) as " + tbl.policy)
		exprs := []string{
			"X", "charset(X)", "collation(X)",
			"X collate utf8mb4_bin", "X collate utf8mb4_bin = 'ALPHABET'", "X collate utf8mb4_general_ci = 'ALPHABET'",
			"collation(X collate utf8mb4_bin)", "length(X collate utf8mb4_bin)", "upper(X collate utf8mb4_bin)",
			"concat(X, '') collate utf8mb4_bin", "collation(concat(X, '') collate utf8mb4_bin)",
			"concat(X, '') collate utf8mb4_general_ci = 'ALPHABET'", "ifnull(X, 'n') collate utf8mb4_unicode_ci",
			"X collate latin1_bin", "X collate latin1_bin = 'ALPHABET'", "X collate latin1_swedish_ci = 'ALPHABET'",
			"concat(X, '') collate latin1_bin", "collation(concat(X, '') collate latin1_swedish_ci)",
			"cast(X as char) collate latin1_bin", "cast(X as char) collate utf8mb4_bin",
			"X collate gbk_bin", "concat(X, '') collate ascii_bin",
			// Columns with and without a policy in one expression.
			"concat(d, X) collate utf8mb4_bin", "concat(d, X) collate latin1_bin", "concat(X, d) collate utf8mb4_general_ci = 'alphabetxyz'",
			"d collate utf8mb4_bin", "d collate latin1_bin", "concat(d, '') collate utf8mb4_bin", "concat(d, '') collate latin1_bin",
			"X collate utf8mb4_bin, d collate utf8mb4_bin", "X collate latin1_bin, d collate latin1_bin",
		}
		shapes := []string{
			"select E from T order by id",
			"select * from (select id, E as x from T) s order by id",
			"with w as (select id as wid, E as x from T) select * from w order by wid",
		}
		valid, invalid := 0, 0
		for _, expr := range exprs {
			for _, shape := range shapes {
				got := runMaskingOutcome(ctx, tk, strings.NewReplacer("E", strings.ReplaceAll(expr, "X", "c"), "T", masked).Replace(shape))
				want := runMaskingOutcome(ctx, tk, strings.NewReplacer("E", strings.ReplaceAll(expr, "X", "("+tbl.policy+")"), "T", plain).Replace(shape))
				if strings.HasPrefix(shape, "select E") {
					// The plain table has no column c to name the result after.
					require.Equal(t, want.rows, got.rows, "%s in %s", expr, shape)
					require.Equal(t, want.fields, got.fields, "%s in %s", expr, shape)
					require.Equal(t, want.err, got.err, "%s in %s", expr, shape)
				} else {
					require.Equal(t, want, got, "%s in %s", expr, shape)
				}
				if want.err == "" {
					valid++
				} else if strings.Contains(want.err, "1253") {
					invalid++
				}
			}
		}
		// Both outcomes are covered for each direction.
		require.Greater(t, valid, len(shapes)*5)
		require.Greater(t, invalid, len(shapes)*5)

		// A predicate compares the original value: the collation must be one
		// of the column's charset.
		own, foreign := "latin1_bin", "utf8mb4_bin"
		if tbl.charset == "utf8mb4" {
			own, foreign = foreign, own
		}
		tk.MustQuery("select id from " + masked + " where c collate " + own + " = 'abc'").Check(testkit.Rows("1"))
		for _, predicate := range []string{
			"select id from TBL where COL collate " + foreign + " = 'abc'",
			"select id from TBL where concat(COL, '') collate " + foreign + " = 'abc'",
			"select s.id from TBL s join TBL r on s.COL collate " + foreign + " = r.COL",
			"select count(*) from TBL group by COL collate " + foreign,
			"select id from TBL order by COL collate " + foreign,
		} {
			want := runMaskingOutcome(ctx, tk, strings.NewReplacer("TBL", plain, "COL", "d").Replace(predicate)).err
			require.Contains(t, want, "1253", predicate)
			require.Equal(t, want, runMaskingOutcome(ctx, tk, strings.NewReplacer("TBL", masked, "COL", "c").Replace(predicate)).err, predicate)
			require.Equal(t, want, runMaskingOutcome(ctx, tk, strings.NewReplacer("TBL", masked, "COL", "d").Replace(predicate)).err, predicate)
		}

		// Without an enabled policy the column is validated as any other.
		tk.MustExec("alter table " + masked + " disable masking policy p")
		for _, expr := range []string{"X collate utf8mb4_bin", "X collate latin1_bin", "concat(X, '') collate utf8mb4_bin", "concat(X, '') collate latin1_bin = 'ABC'"} {
			for _, shape := range shapes {
				got := runMaskingOutcome(ctx, tk, strings.NewReplacer("E", strings.ReplaceAll(expr, "X", "c"), "T", masked).Replace(shape))
				want := runMaskingOutcome(ctx, tk, strings.NewReplacer("E", strings.ReplaceAll(expr, "X", "d"), "T", plain).Replace(shape))
				require.Equal(t, want.err, got.err, "%s in %s", expr, shape)
				require.Equal(t, want.err == "", strings.Contains(expr, own), expr)
			}
		}
		tk.MustQuery("select c collate " + own + " from " + masked + " where id = 1").Check(testkit.Rows("abc"))
	}
}

func TestMaskingPolicyDerivedRollup(t *testing.T) {
	tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
	tk.MustExec("use test")
	tk.MustExec("create table rollup_source(c varchar(20))")
	tk.MustExec("insert into rollup_source values('secret'),(null)")
	tk.MustExec("create masking policy p on rollup_source(c) as mask_full(c)")
	for _, grouping := range []string{"d", "d,d"} {
		query := "select d,count(*) from (select concat(c,'!') as d from rollup_source) s group by " +
			grouping + " with rollup"
		expected := testkit.Rows("<nil> 1", "<nil> 2", "XXXXXX! 1")
		if grouping == "d,d" {
			expected = testkit.Rows("<nil> 1", "<nil> 1", "<nil> 2", "XXXXXX! 1", "XXXXXX! 1")
		}
		tk.MustQuery(query).Sort().Check(expected)
	}
}

func TestMaskingPolicyOuterJoinNullExtension(t *testing.T) {
	tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
	tk.MustExec("use test")
	tk.MustExec("create table join_l(id int primary key)")
	tk.MustExec("create table join_r(id int primary key,c varchar(20))")
	tk.MustExec("insert into join_l values(1),(2),(3)")
	tk.MustExec("insert into join_r values(1,'secret'),(3,null)")
	tk.MustExec("create masking policy p on join_r(c) as mask_full(c)")
	for _, from := range []string{
		"join_l l left join (select id,concat_ws('',c,'!') as d from join_r) s on l.id=s.id",
		"(select id,concat_ws('',c,'!') as d from join_r) s right join join_l l on l.id=s.id",
	} {
		tk.MustQuery("select l.id,s.d from " + from + " order by l.id").Check(testkit.Rows("1 XXXXXX!", "2 <nil>", "3 !"))
		tk.MustQuery("select l.id,ifnull(s.d,'missing') from " + from + " order by l.id").
			Check(testkit.Rows("1 XXXXXX!", "2 missing", "3 !"))
	}
	// Expressions written above the join still evaluate for unmatched rows.
	tk.MustQuery("select l.id,concat_ws('',r.c,'!') from join_l l left join join_r r on l.id=r.id order by l.id").
		Check(testkit.Rows("1 XXXXXX!", "2 !", "3 !"))
}

func TestMaskingPolicyRestoreModeFastPlans(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table restore_gap(id int primary key,c varchar(20))")
	tk.MustExec("insert into restore_gap values(1,'secret')")
	tk.MustExec("create masking policy p on restore_gap(c) as mask_full(c)")
	tk.MustExec("create user restore_reader")
	tk.MustExec("grant select on test.restore_gap to restore_reader")
	reader := testkit.NewTestKit(t, store)
	require.NoError(t, reader.Session().Auth(&auth.UserIdentity{Username: "restore_reader", Hostname: "%"}, nil, nil, nil))
	reader.MustExec("use test")
	reader.MustExec("prepare cached_read from 'select c from restore_gap where id=?'")
	reader.MustExec("set @id=1")
	reader.MustQuery("execute cached_read using @id").Check(testkit.Rows("XXXXXX"))
	tbl, err := dom.InfoSchema().TableByName(context.Background(), pmodel.NewCIStr("test"), pmodel.NewCIStr("restore_gap"))
	require.NoError(t, err)
	require.NoError(t, dom.DDLExecutor().AlterTableMode(tk.Session(), &model.AlterTableModeArgs{
		SchemaID: tbl.Meta().DBID, TableID: tbl.Meta().ID, TableMode: model.TableModeRestore,
	}))
	// A table in Restore mode is not readable, whichever plan the query would get.
	for _, query := range []string{
		"select c from restore_gap where id=1", "select c from restore_gap where id in(1,2)",
		"select c from restore_gap", "execute cached_read using @id",
	} {
		reader.MustGetErrCode(query, errno.ErrProtectedTableMode)
	}
}

func TestMaskingPolicyScalarSemiJoinMultiOutput(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table semi_multi_src(id int primary key,k int,c varchar(20),e varchar(20))")
	tk.MustExec("insert into semi_multi_src values(1,1,'secret','second'),(2,null,null,'hidden')")
	tk.MustExec("create table semi_multi_rhs(id int primary key)")
	tk.MustExec("insert into semi_multi_rhs values(1)")
	tk.MustExec("create masking policy p on semi_multi_src(c) as mask_full(c)")
	tk.MustExec("create masking policy q on semi_multi_src(e) as mask_full(e)")
	derived := " from (select id,k,concat(c,'!') as d from semi_multi_src) s"
	for _, tc := range []struct {
		expr string
		want []string
	}{
		{"s.id in(select id from semi_multi_rhs)", []string{"1", "0"}},
		{"s.id not in(select id from semi_multi_rhs)", []string{"0", "1"}},
		{"s.id=any(select id from semi_multi_rhs)", []string{"1", "0"}},
		{"s.k in(select id from semi_multi_rhs)", []string{"1", "<nil>"}},
		{"s.k not in(select id from semi_multi_rhs)", []string{"0", "<nil>"}},
		{"s.k=any(select id from semi_multi_rhs)", []string{"1", "<nil>"}},
		{"s.id in(select id from semi_multi_rhs union all select null)", []string{"1", "<nil>"}},
		{"s.id not in(select id from semi_multi_rhs union all select null)", []string{"0", "<nil>"}},
		{"s.id=any(select id from semi_multi_rhs union all select null)", []string{"1", "<nil>"}},
		{"s.k in(select id from semi_multi_rhs where id<0)", []string{"0", "0"}},
		{"s.k not in(select id from semi_multi_rhs where id<0)", []string{"1", "1"}},
		{"s.k=any(select id from semi_multi_rhs where id<0)", []string{"0", "0"}},
		{"s.id in(select /*+ no_decorrelate() */ id from semi_multi_rhs r where r.id=s.id)", []string{"1", "0"}},
		{"s.id not in(select /*+ no_decorrelate() */ id from semi_multi_rhs r where r.id=s.id)", []string{"0", "1"}},
		{"s.id=any(select /*+ no_decorrelate() */ id from semi_multi_rhs r where r.id=s.id)", []string{"1", "0"}},
	} {
		t.Run(tc.expr, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustQuery("select s.d," + tc.expr + derived + " order by s.id").
				Check(testkit.Rows("XXXXXX! "+tc.want[0], "<nil> "+tc.want[1]))
			tk.MustQuery("select " + tc.expr + ",s.d" + derived + " order by s.id").
				Check(testkit.Rows(tc.want[0]+" XXXXXX!", tc.want[1]+" <nil>"))
		})
	}
	applyQuery := "select s.d,s.id in(select /*+ no_decorrelate() */ id from semi_multi_rhs r where r.id=s.id)" + derived
	require.Contains(t, fmt.Sprint(tk.MustQuery("explain format='brief' "+applyQuery).Rows()), "Apply")
	// Several dependencies, repeated inputs and nested scalar joins must all
	// survive schema rebuilding without replacing a generated boolean column.
	tk.MustQuery("select s.d,s.id in(select id from semi_multi_rhs)," +
		"s.id not in(select id from semi_multi_rhs),s.id=any(select id from semi_multi_rhs),s.d,s.dd" +
		" from (select id,concat(c,'/',e) as d,concat(c,'#') as dd from semi_multi_src) s order by s.id").
		Check(testkit.Rows("XXXXXX/XXXXXX 1 0 1 XXXXXX/XXXXXX XXXXXX#", "<nil> 0 1 0 <nil> <nil>"))
	// A predicate subquery may retain an unsupported result while masking its
	// other outputs. Its failed attempt must still leave a valid child schema.
	tk.MustQuery("select ('XXXXXX!',1) in(select s.d,s.d in(select 'secret!')" + derived + ")").
		Check(testkit.Rows("1"))
	tk.MustQuery("select (1,'XXXXXX!') in(select s.d in(select 'secret!'),s.d" + derived + ")").
		Check(testkit.Rows("1"))
	tk.MustExec("create table semi_multi_dst(d varchar(40),matched int)")
	tk.MustExec("insert into semi_multi_dst select s.d,s.id in(select id from semi_multi_rhs)" + derived)
	tk.MustQuery("select d,matched from semi_multi_dst order by matched desc").Check(testkit.Rows("XXXXXX! 1", "<nil> 0"))
	tk.MustExec("truncate table semi_multi_dst")
	for _, comparison := range []string{"in", "not in", "=any"} {
		expr := "s.d " + comparison + "(select 'secret!')"
		for _, fields := range []string{"s.d," + expr, expr + ",s.d"} {
			require.ErrorContains(t, tk.ExecToErr("select "+fields+derived), "masking policy cannot be preserved")
		}
	}
	tk.MustExec("alter table semi_multi_src modify masking policy p set restrict on(insert_into_select)")
	for _, comparison := range []string{"in", "not in", "=any"} {
		expr := "s.d " + comparison + "(select 'secret!')"
		tk.MustGetErrCode("insert into semi_multi_dst select s.d,"+expr+derived, errno.ErrAccessDeniedToMaskedColumn)
		tk.MustGetErrCode("insert into semi_multi_dst(matched,d) select "+expr+",s.d"+derived, errno.ErrAccessDeniedToMaskedColumn)
	}
	tk.MustQuery("select * from semi_multi_dst").Check(testkit.Rows())
	tk.MustExec("alter table semi_multi_src disable masking policy p")
	tk.MustQuery("select s.d,s.d in(select 'secret!')" + derived + " order by s.id").
		Check(testkit.Rows("secret! 1", "<nil> <nil>"))
	tk.MustExec("alter table semi_multi_src drop masking policy p")
	tk.MustExec("alter table semi_multi_src drop masking policy q")
	tk.MustQuery("select s.d,s.id in(select id from semi_multi_rhs)" + derived + " order by s.id").
		Check(testkit.Rows("secret! 1", "<nil> 0"))
}

func TestMaskingPolicyDDLPlanCache(t *testing.T) {
	for _, instance := range []bool{false, true} {
		t.Run(fmt.Sprintf("instance=%t", instance), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			root := testkit.NewTestKit(t, store)
			root.MustExec("use test")
			root.MustExec("create table masking_ddl_cache(id int primary key,c varchar(20))")
			root.MustExec("insert into masking_ddl_cache values(1,'secret')")
			root.MustExec("create user masking_ddl_reader")
			root.MustExec("grant select on test.* to masking_ddl_reader")
			reader := testkit.NewTestKit(t, store)
			require.NoError(t, reader.Session().Auth(&auth.UserIdentity{Username: "masking_ddl_reader", Hostname: "%"}, nil, nil, nil))
			reader.MustExec("use test")
			ctx := context.Background()
			if instance {
				ctx = context.WithValue(ctx, core.PlanCacheKeyEnableInstancePlanCache{}, true)
			}
			reader.MustExecWithContext(ctx, "prepare st from 'select c from masking_ddl_cache where id>=? order by id'")
			reader.MustExec("set @id=1")
			for _, tc := range []struct{ ddl, want string }{
				{"", "secret"},
				{"create masking policy p on masking_ddl_cache(c) as mask_full(c)", "XXXXXX"},
				{"alter table masking_ddl_cache modify masking policy p set expression=c", "secret"},
				{"alter table masking_ddl_cache modify masking policy p set expression=mask_full(c)", "XXXXXX"},
				{"alter table masking_ddl_cache disable masking policy p", "secret"},
				{"alter table masking_ddl_cache enable masking policy p", "XXXXXX"},
				{"alter table masking_ddl_cache drop masking policy p", "secret"},
			} {
				if tc.ddl != "" {
					root.MustExec(tc.ddl)
				}
				for range 2 {
					reader.MustQueryWithContext(ctx, "execute st using @id").Check(testkit.Rows(tc.want))
				}
				reader.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("1"))
			}
		})
	}
}

func TestMaskingPolicyScalarSubqueryAsRelationalKey(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_key(id int primary key, c varchar(10))")
	tk.MustExec("insert into t_key values (1,'b'),(2,'a'),(3,'a')")
	tk.MustExec("create masking policy p on t_key(c) as mask_full(c)")
	sub := "(select i.c from t_key i where i.id = o.id)"
	// Grouping, ordering and window keys work on the original values: 'a' and
	// 'b' mask to the same value, and must still be two groups.
	tk.MustQuery("select count(*) from t_key o group by " + sub).Sort().Check(testkit.Rows("1", "2"))
	tk.MustQuery("select o.id from t_key o order by " + sub + ", o.id").Check(testkit.Rows("2", "3", "1"))
	tk.MustQuery("select o.id, row_number() over (partition by " + sub + " order by o.id) from t_key o order by o.id").
		Check(testkit.Rows("1 1", "2 1", "3 2"))
	tk.MustQuery("select o.id, rank() over (order by " + sub + ") from t_key o order by o.id").
		Check(testkit.Rows("1 3", "2 1", "3 1"))
	// A subquery inside a window key is a query block of its own: the scalar
	// subquery in its predicate is compared as masked, whether the block is
	// entered through EXISTS, IN or a quantified comparison.
	inner := "(select i.c from t_key i where i.id = j.id) = 'a'"
	for _, key := range []string{
		"exists (select 1 from t_key j where j.id = o.id and " + inner + ")",
		"o.id in (select j.id from t_key j where " + inner + ")",
		"o.id = any (select j.id from t_key j where " + inner + ")",
	} {
		tk.MustQuery("select o.id, row_number() over (partition by " + key + " order by o.id) from t_key o order by o.id").
			Check(testkit.Rows("1 1", "2 2", "3 3"))
	}
	// A predicate still compares with the masked value.
	tk.MustQuery("select count(*) from t_key o where " + sub + " = 'a'").Check(testkit.Rows("0"))
	tk.MustQuery("select count(*) from t_key o having max(" + sub + ") = 'X'").Check(testkit.Rows("3"))
}

func TestMaskingPolicyScalarSubqueryInFunctionArgs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t_arg(id int primary key, c varchar(20))")
	tk.MustExec("insert into t_arg values (1,'secret'),(2,'hidden')")
	tk.MustExec("create table t_arg_plain(id int primary key, c varchar(20))")
	tk.MustExec("insert into t_arg_plain values (1,'secret'),(2,'hidden')")
	tk.MustExec("create table t_arg_dst(id int primary key, c varchar(20))")
	tk.MustExec("insert into t_arg_dst values (1,'x')")
	// Without a policy the arguments are ordinary values.
	tk.MustQuery("select max((select max(i.c) from t_arg i)) from t_arg o where 1=1").Check(testkit.Rows("secret"))
	tk.MustExec("prepare s from 'select max((select max(i.c) from t_arg i)) from t_arg o where o.id > ?'")
	tk.MustExec("set @a = 0")
	tk.MustQuery("execute s using @a").Check(testkit.Rows("secret"))
	tk.MustExec("create masking policy p on t_arg(c) as mask_full(c) enable")
	tk.MustQuery("select max((select max(i.c) from t_arg_plain i)) from t_arg_plain o where 1=1").Check(testkit.Rows("secret"))

	// The arguments of an aggregate or window function in the select list are
	// results, whichever clause was built before them.
	const notPreserved = "masking policy cannot be preserved through scalar subquery"
	for _, sub := range []string{
		"(select max(i.c) from t_arg i)",
		"(select i.c from t_arg i where i.id = o.id)",
		"(select max(i.c) from t_arg i where i.id = o.id)",
	} {
		for _, sql := range []string{
			"select max(" + sub + ") from t_arg o where 1=1",
			"select max(" + sub + ") from t_arg o",
			"select max(" + sub + ") from t_arg o group by o.id",
			"select min(" + sub + ") from t_arg o where 1=1",
			"select group_concat(" + sub + ") from t_arg o where 1=1",
			"select count(distinct " + sub + ") from t_arg o where 1=1",
			"select sum(cast(" + sub + " as signed)) from t_arg o where 1=1",
			"select any_value(" + sub + ") from t_arg o where 1=1",
			"select concat(max(" + sub + "), '!') from t_arg o where 1=1",
			"select max(" + sub + ") from t_arg o where 1=1 having max(" + sub + ") = 'secret'",
			"select max(" + sub + ") from t_arg o where 1=1 order by max(" + sub + ")",
			"select * from (select max(" + sub + ") m from t_arg o where 1=1) d",
			"with d as (select max(" + sub + ") m from t_arg o where 1=1) select * from d",
			"select max(" + sub + ") from t_arg o where 1=1 union all select 'z'",
			"select sum(length(max(" + sub + "))) over () from t_arg o where 1=1",
			"insert into t_arg_dst select 5, max(" + sub + ") from t_arg o where 1=1",
			"update t_arg_dst set c = (select max(" + sub + ") from t_arg o where 1=1)",
			"set @v = (select max(" + sub + ") from t_arg o where 1=1)",
		} {
			require.ErrorContains(t, tk.ExecToErr(sql), notPreserved, sql)
		}
	}
	for _, sql := range []string{
		"select max((select max(i.c) from t_arg i)) over () from t_arg o",
		"select max((select max(i.c) from t_arg i)) over () from t_arg o having 1=1",
		"select * from (select max((select max(i.c) from t_arg i)) over () m from t_arg o having 1=1) d",
		"select o.id from t_arg o having 1=1 order by max((select max(i.c) from t_arg i)) over (), o.id",
	} {
		require.ErrorContains(t, tk.ExecToErr(sql), notPreserved, sql)
	}
	tk.MustQuery("select c from t_arg_dst").Check(testkit.Rows("x"))
	// A statement prepared before the policy existed is rejected as well.
	require.ErrorContains(t, tk.ExecToErr("execute s using @a"), notPreserved)
	require.ErrorContains(t, tk.ExecToErr("execute s using @a"), notPreserved)
	require.ErrorContains(t, tk.ExecToErr("prepare s from 'select max((select max(i.c) from t_arg i)) from t_arg o where o.id > ?'"), notPreserved)

	// An aggregate used only by HAVING is a predicate operand, as in WHERE: the
	// value is compared as masked, and MIN/MAX of a subquery as original.
	tk.MustQuery("select count(*) from t_arg o where o.c = (select max(i.c) from t_arg i)").Check(testkit.Rows("1"))
	tk.MustQuery("select count(*) from t_arg o where (select max(i.c) from t_arg i) = 'secret'").Check(testkit.Rows("2"))
	tk.MustQuery("select count(*) from t_arg o where (select i.c from t_arg i where i.id = o.id) = 'secret'").Check(testkit.Rows("0"))
	for _, where := range []string{"", " where 1=1"} {
		tk.MustQuery("select count(*) from t_arg o" + where + " having max((select max(i.c) from t_arg i)) = 'secret'").Check(testkit.Rows("2"))
		tk.MustQuery("select count(*) from t_arg o" + where + " having max((select max(i.c) from t_arg i)) = 'XXXXXX'").Check(testkit.Rows())
		tk.MustQuery("select count(*) from t_arg o" + where + " having max((select i.c from t_arg i where i.id = o.id)) = 'XXXXXX'").Check(testkit.Rows("2"))
		tk.MustQuery("select count(*) from t_arg o" + where + " having max((select i.c from t_arg i where i.id = o.id)) = 'secret'").Check(testkit.Rows())
		// An aggregate used only by ORDER BY is a sort key, as are the ORDER BY
		// items of GROUP_CONCAT: they work on the original values.
		tk.MustQuery("select o.id from t_arg o" + where + " group by o.id order by max((select i.c from t_arg i where i.id = o.id))").Check(testkit.Rows("2", "1"))
		tk.MustQuery("select o.id from t_arg o" + where + " group by o.id order by max((select max(i.c) from t_arg i)), o.id").Check(testkit.Rows("1", "2"))
		tk.MustQuery("select group_concat(o.id order by (select i.c from t_arg i where i.id = o.id)) from t_arg o" + where).Check(testkit.Rows("2,1"))
	}
	// So is an ORDER BY item of a query block nested in a select list.
	tk.MustQuery("select (select o.id from t_arg o order by (select i.c from t_arg i where i.id = o.id) limit 1)").Check(testkit.Rows("2"))
	tk.MustExec("prepare s from 'select count(*) from t_arg o where o.id > ? having max((select max(i.c) from t_arg i)) = ?'")
	tk.MustExec("set @b = 'secret'")
	tk.MustQuery("execute s using @a, @b").Check(testkit.Rows("2"))
	tk.MustQuery("execute s using @a, @b").Check(testkit.Rows("2"))
}

func TestMaskingPolicyNestedScalarSubqueryInWrites(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create table t_w(id int primary key, c varchar(20))")
	root.MustExec("create table t_wr(id int primary key, c varchar(20))")
	root.MustExec("create table t_w_plain(id int primary key, c varchar(20))")
	root.MustExec("create table t_w_dst(id int primary key, c varchar(20))")
	root.MustExec("create table t_w_other(id int primary key)")
	for _, tbl := range []string{"t_w", "t_wr", "t_w_plain"} {
		root.MustExec("insert into " + tbl + " values (1,'secret'),(2,'hidden')")
	}
	root.MustExec("insert into t_w_dst values (1,'x')")
	root.MustExec("insert into t_w_other values (1)")
	root.MustExec("create masking policy p on t_w(c) as case when current_user()='root@%' then c else mask_full(c) end enable")
	root.MustExec(`create masking policy pr on t_wr(c) as case when current_user()='root@%' then c else mask_full(c) end
		restrict on (insert_into_select, update_select, delete_select) enable`)
	root.MustExec("create user u_w")
	root.MustExec("grant select, insert, update, delete on test.* to u_w")
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u_w", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	unchanged := func() {
		root.MustQuery("select id, c from t_w_dst").Check(testkit.Rows("1 x"))
	}

	// The fields of a query block nested in a write are results, as in SELECT:
	// a protected scalar subquery among them is rejected in every statement.
	const notPreserved = "masking policy cannot be preserved through scalar subquery"
	for _, tbl := range []string{"t_w", "t_wr"} {
		for _, sub := range []string{
			"(select max(i.c) from " + tbl + " i)",
			"(select i.c from " + tbl + " i order by i.id limit 1)",
			"(select max(i.c) from " + tbl + " i where i.id = d.id)",
		} {
			correlated := strings.Contains(sub, "d.id")
			for _, sql := range []string{
				"update t_w_dst d set c = (select " + sub + ")",
				"update t_w_dst d set c = (select (select " + sub + "))",
				"update t_w_dst d set c = (select " + sub + " from t_w_other)",
				"update t_w_dst d set c = concat((select " + sub + "), '!')",
				"update t_w_dst d, t_w_other o set d.c = (select " + sub + ") where d.id = o.id",
				"update t_w_dst d, (select " + sub + " m) s set d.c = s.m",
				"update t_w_dst d, (select (select " + sub + ") m) s set d.c = s.m",
				"with s as (select " + sub + " m) update t_w_dst d, s set d.c = s.m",
				"update t_w_dst d set c = (select 1 from (select " + sub + " m) s where s.m = 'secret')",
				"insert into t_w_dst values (5, (select " + sub + "))",
				"insert into t_w_dst select 5, " + sub,
				"insert into t_w_dst select 5, (select " + sub + ") from t_w_other",
				"insert into t_w_dst select 5, m from (select " + sub + " m) s",
				"insert into t_w_dst set id = 5, c = (select " + sub + ")",
				"replace into t_w_dst values (1, (select " + sub + "))",
				"replace into t_w_dst select 1, " + sub,
				"insert into t_w_dst values (1, 'z') on duplicate key update c = (select " + sub + ")",
				"set @v = (select " + sub + ")",
				"do (@v := (select " + sub + "))",
			} {
				if correlated && (strings.Contains(sql, " s set ") || !strings.Contains(sql, "t_w_dst d")) {
					continue
				}
				err := tk.ExecToErr(sql)
				// RESTRICT ON is checked first for the subqueries of UPDATE.
				if tbl == "t_wr" && (strings.HasPrefix(sql, "update") || strings.HasPrefix(sql, "with")) {
					require.True(t, terror.ErrorEqual(err, plannererrors.ErrAccessDeniedToMaskedColumn), "%s: %v", sql, err)
				} else {
					require.ErrorContains(t, err, notPreserved, sql)
				}
				unchanged()
			}
		}
	}
	tk.MustQuery("select @v").Check(testkit.Rows("<nil>"))
	require.ErrorContains(t, tk.ExecToErr("prepare s from 'update t_w_dst set c = (select (select max(i.c) from t_w i)) where id > ?'"), notPreserved)
	// Without RESTRICT ON the same shapes are still rejected.
	root.MustExec("alter table t_wr modify masking policy pr set restrict on none")
	require.ErrorContains(t, tk.ExecToErr("update t_w_dst set c = (select (select max(i.c) from t_wr i))"), notPreserved)
	require.ErrorContains(t, tk.ExecToErr("update t_w_dst, (select (select max(i.c) from t_wr i) m) s set c = s.m"), notPreserved)
	unchanged()

	// A scalar subquery in the assignment itself stores its masked value.
	for _, sub := range []string{
		"(select max(i.c) from t_w i)",
		"(select i.c from t_w i order by i.id limit 1)",
		"(select i.c from t_w i where i.id = t_w_dst.id)",
	} {
		tk.MustExec("update t_w_dst set c = " + sub)
		root.MustQuery("select id, c from t_w_dst").Check(testkit.Rows("1 XXXXXX"))
		root.MustExec("update t_w_dst set c = 'x'")
	}
	// Predicates keep comparing masked values, and MIN/MAX of a subquery as
	// original, in the statement and in the query blocks nested in it. A
	// subquery that is only compared does not make its fields results.
	for _, tc := range []struct{ cond, rows string }{
		{"(select max(i.c) from t_w i) = 'secret'", "1 hit"},
		{"(select max(i.c) from t_w i) = 'XXXXXX'", "1 x"},
		{"(select i.c from t_w i where i.id = d.id) = 'secret'", "1 x"},
		{"(select i.c from t_w i where i.id = d.id) = 'XXXXXX'", "1 hit"},
	} {
		for _, where := range []string{
			tc.cond,
			"(select " + tc.cond + ")",
			"(select (select " + tc.cond + "))",
			"(select m from (select " + tc.cond + " m) s)",
			"d.id in (select d.id from t_w_other where " + tc.cond + ")",
			"exists (select 1 from t_w_other having " + tc.cond + ")",
		} {
			tk.MustExec("update t_w_dst d set c = 'hit' where " + where)
			root.MustQuery("select id, c from t_w_dst").Check(testkit.Rows(tc.rows))
			root.MustExec("update t_w_dst set c = 'x'")
			tk.MustExec("delete d from t_w_dst d where " + where)
			if tc.rows == "1 hit" {
				root.MustQuery("select id, c from t_w_dst").Check(testkit.Rows())
				root.MustExec("insert into t_w_dst values (1,'x')")
			}
			unchanged()
		}
	}

	// Tables without a policy are not affected.
	tk.MustExec("update t_w_dst set c = (select (select max(i.c) from t_w_plain i))")
	root.MustQuery("select id, c from t_w_dst").Check(testkit.Rows("1 secret"))
	root.MustExec("update t_w_dst set c = 'x'")
	tk.MustExec("prepare s from 'update t_w_dst set c = (select (select max(i.c) from t_w_plain i)) where id > ?'")
	tk.MustExec("set @a = 0")
	tk.MustExec("execute s using @a")
	root.MustQuery("select id, c from t_w_dst").Check(testkit.Rows("1 secret"))
	root.MustExec("update t_w_dst set c = 'x'")
	// The shape is unsupported whoever runs it.
	require.ErrorContains(t, root.ExecToErr("update t_w_dst set c = (select (select max(i.c) from t_w i))"), notPreserved)
	root.MustExec("update t_w_dst set c = (select max(i.c) from t_w i)")
	root.MustQuery("select id, c from t_w_dst").Check(testkit.Rows("1 secret"))
}

func TestMaskingPolicyRestrictDirectUpdateAssignment(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create table t_da(id int primary key, c varchar(20), c2 varchar(20))")
	root.MustExec("create table t_da_free(id int primary key, c varchar(20), c2 varchar(20))")
	root.MustExec("create table t_da_dst(id int primary key, c varchar(20))")
	root.MustExec("create masking policy p on t_da(c) as case when current_user()='root@%' then c else mask_full(c) end restrict on (update_select) enable")
	root.MustExec("create masking policy p_free on t_da_free(c) as case when current_user()='root@%' then c else mask_full(c) end restrict on (insert_into_select, delete_select) enable")
	root.MustExec("create user u_da")
	root.MustExec("grant select, insert, update, delete on test.* to u_da")
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u_da", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	reset := func() {
		root.MustExec("delete from t_da_dst")
		root.MustExec("insert into t_da_dst values (1,'x')")
		for _, tbl := range []string{"t_da", "t_da_free"} {
			root.MustExec("delete from " + tbl)
			root.MustExec("insert into " + tbl + " values (1,'secret','a'),(2,'hidden','b')")
		}
	}
	src := func(tbl string) *testkit.Result {
		return root.MustQuery("select id, c, c2 from " + tbl + " order by id")
	}
	original := testkit.Rows("1 secret a", "2 hidden b")
	reset()

	// UPDATE_SELECT covers an assignment that reads the protected column
	// directly and writes it to another column or another row.
	denied := []string{
		"update t_da_dst d join T s on s.id = d.id set d.c = s.c",
		"update t_da_dst d, T set d.c = T.c where d.id = T.id",
		"update t_da_dst d join T s on s.id = d.id set d.c = concat(s.c, '!')",
		"update t_da_dst d join T s on s.id = d.id set d.c = if(s.c = 'secret', 'y', 'n')",
		"update t_da_dst d join T s on s.id = d.id set d.c = length(s.c)",
		"update T set c2 = c",
		"update T set c2 = c order by id limit 1",
		"update T set c2 = 'k', c = upper(c)",
		"update T a join T b on a.id = b.id + 1 set a.c = b.c",
		"update T a join T b on a.id = b.id set a.c = b.c",
		"update T set c = concat(c, 'x')",
		"update t_da_dst d set c = (select s.c from T s where s.id = d.id)",
	}
	for _, sql := range denied {
		sql = strings.ReplaceAll(sql, "T", "t_da")
		tk.MustGetErrCode(sql, errno.ErrAccessDeniedToMaskedColumn)
		root.MustQuery("select id, c from t_da_dst").Check(testkit.Rows("1 x"))
		src("t_da").Check(original)
	}
	require.ErrorContains(t, tk.ExecToErr("prepare s from 'update t_da set c2 = c where id > ?'"), "Access to masked column")

	// Not restricted: the column assigned to itself, other columns of the
	// table, and comparisons with the protected column.
	for _, tc := range []struct {
		sql      string
		dst, src [][]any
	}{
		{"update t_da set c = c", testkit.Rows("1 x"), original},
		{"update t_da a join t_da b on a.id = b.id set a.c = a.c", testkit.Rows("1 x"), original},
		{"update t_da_dst d join t_da s on s.id = d.id set d.c = s.c2", testkit.Rows("1 a"), original},
		{"update t_da_dst d join t_da s on s.c = 'secret' set d.c = 'hit'", testkit.Rows("1 hit"), original},
		{"update t_da set c2 = 'w' where c = 'secret'", testkit.Rows("1 x"), testkit.Rows("1 secret w", "2 hidden b")},
		{"update t_da set c = 'new' where c = 'secret'", testkit.Rows("1 x"), testkit.Rows("1 new a", "2 hidden b")},
		{"update t_da set c = c2", testkit.Rows("1 x"), testkit.Rows("1 a a", "2 b b")},
		{"delete d from t_da_dst d join t_da s on s.id = d.id where s.c = 'secret'", testkit.Rows(), original},
	} {
		tk.MustExec(tc.sql)
		root.MustQuery("select id, c from t_da_dst").Check(tc.dst)
		src("t_da").Check(tc.src)
		reset()
	}

	// A policy without UPDATE_SELECT keeps writing original values, and so
	// does an identity that the policy does not mask.
	for _, who := range []*testkit.TestKit{tk, root} {
		tbl := "t_da_free"
		if who == root {
			tbl = "t_da"
		}
		who.MustExec("update t_da_dst d join " + tbl + " s on s.id = d.id set d.c = s.c")
		root.MustQuery("select id, c from t_da_dst").Check(testkit.Rows("1 secret"))
		who.MustExec("update " + tbl + " set c2 = concat(c, '!')")
		src(tbl).Check(testkit.Rows("1 secret secret!", "2 hidden hidden!"))
		reset()
	}
	root.MustExec("alter table t_da modify masking policy p set restrict on none")
	tk.MustExec("update t_da set c2 = c")
	src("t_da").Check(testkit.Rows("1 secret secret", "2 hidden hidden"))
}

// TestMaskingPolicyRestrictOnDuplicateAssignment tests that an assignment of
// INSERT ... ON DUPLICATE KEY UPDATE reading a protected column of the
// existing row is restricted like an UPDATE assignment.
// TestMaskingPolicyRestrictPointUpdateAssignment tests that an UPDATE by
// primary key, which may get a fast plan, is restricted like any other UPDATE.
func TestMaskingPolicyRestrictPointUpdateAssignment(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create table t_pu(id int primary key, c varchar(20), c2 varchar(20))")
	root.MustExec("create table t_pu_free(id int primary key, c varchar(20), c2 varchar(20))")
	root.MustExec("create masking policy p on t_pu(c) as case when current_user()='root@%' then c else mask_full(c) end restrict on (update_select) enable")
	root.MustExec("create masking policy p_free on t_pu_free(c) as case when current_user()='root@%' then c else mask_full(c) end enable")
	root.MustExec("create user u_pu")
	root.MustExec("grant select, insert, update, delete on test.* to u_pu")
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u_pu", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	original := testkit.Rows("1 secret a", "2 hidden b")
	reset := func() {
		for _, tbl := range []string{"t_pu", "t_pu_free"} {
			root.MustExec("delete from " + tbl)
			root.MustExec("insert into " + tbl + " values (1,'secret','a'),(2,'hidden','b')")
		}
	}
	rows := func(tbl string) *testkit.Result {
		return root.MustQuery("select id, c, c2 from " + tbl + " order by id")
	}
	reset()
	// Load the policies, as the fast plans are only taken afterwards.
	tk.MustQuery("select c from t_pu where id = 1").Check(testkit.Rows("XXXXXX"))

	for _, sql := range []string{
		"update t_pu set c2 = c where id = 1",
		"update t_pu set c2 = c where id in (1, 2)",
		"update t_pu set c2 = concat(c, '!') where id = 1",
		"update t_pu set c = upper(c) where id = 1",
		"update t_pu set c2 = 'k', c = concat(c, 'x') where id in (1, 2)",
	} {
		tk.MustGetErrCode(sql, errno.ErrAccessDeniedToMaskedColumn)
		rows("t_pu").Check(original)
	}
	require.ErrorContains(t, tk.ExecToErr("prepare st from 'update t_pu set c2 = c where id = ?'"), "Access to masked column")
	// An identity that reads the original value prepares and executes it.
	root.MustExec("prepare st from 'update t_pu set c2 = c where id = ?'")
	root.MustExec("set @a = 1")
	root.MustExec("execute st using @a")
	rows("t_pu").Check(testkit.Rows("1 secret secret", "2 hidden b"))
	reset()

	// Not restricted.
	for _, tc := range []struct {
		sql  string
		rows [][]any
	}{
		{"update t_pu set c = c where id = 1", original},
		{"update t_pu set c2 = 'w' where id = 1", testkit.Rows("1 secret w", "2 hidden b")},
		{"update t_pu set c = c2 where id in (1, 2)", testkit.Rows("1 a a", "2 b b")},
		{"update t_pu set c = 'new' where id = 2", testkit.Rows("1 secret a", "2 new b")},
	} {
		tk.MustExec(tc.sql)
		rows("t_pu").Check(tc.rows)
		reset()
	}
	tk.MustExec("update t_pu_free set c2 = c where id = 1")
	rows("t_pu_free").Check(testkit.Rows("1 secret secret", "2 hidden b"))
	root.MustExec("update t_pu set c2 = c where id in (1, 2)")
	rows("t_pu").Check(testkit.Rows("1 secret secret", "2 hidden hidden"))
}

// TestMaskingPolicyExplainRedactsValues tests that EXPLAIN does not show the
// original values a statement computes from a masked column.
func TestMaskingPolicyExplainRedactsValues(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create table t_ex(id int primary key, c varchar(20), b varchar(20), key(c))")
	root.MustExec("insert into t_ex values (1, 'secret', 'plainvalue')")
	root.MustExec("create masking policy p on t_ex(c) as case when current_user()='root@%' then c else mask_full(c) end enable")
	root.MustExec("create user u_ex")
	root.MustExec("grant select on test.* to u_ex")
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u_ex", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	plan := func(who *testkit.TestKit, sql string) string {
		var sb strings.Builder
		for _, row := range who.MustQuery(sql).Rows() {
			sb.WriteString(fmt.Sprintln(row...))
		}
		return sb.String()
	}
	for _, who := range []*testkit.TestKit{tk, root} {
		who.MustExec("set session tidb_redact_log = 'OFF'")
		who.MustExec("set session tidb_opt_enable_non_eval_scalar_subquery = OFF")
	}
	for _, format := range []string{"explain ", "explain format='brief' ", "explain analyze ", "explain format='verbose' "} {
		for _, sql := range []string{
			"select count(*) from t_ex where c = (select min(c) from t_ex)",
			"select count(*) from t_ex where c >= (select max(c) from t_ex)",
			"select count(*) from t_ex where b = (select max(c) from t_ex)",
		} {
			require.NotContains(t, plan(tk, format+sql), "secret", format+sql)
			// An identity that reads the original value keeps its plans.
			require.Contains(t, plan(root, format+sql), "secret", format+sql)
		}
		// A statement that reads no masked column keeps its values.
		require.Contains(t, plan(tk, format+"select count(*) from t_ex where b = (select max(b) from t_ex)"), "plainvalue")
	}
	require.Equal(t, "OFF", tk.MustQuery("select @@session.tidb_redact_log").Rows()[0][0])

	// The plan recorded for the statement itself does not keep them either.
	// Each identity runs a statement of its own, as a summary is shared.
	summary := "select plan from information_schema.statements_summary where digest_text like '%%%s%%' and digest_text not like '%%statements_summary%%'"
	tk.MustQuery("select count(*) as masked_reader from t_ex where c = (select min(c) from t_ex)").Check(testkit.Rows("1"))
	root.MustQuery("select count(*) as exempt_reader from t_ex where c = (select min(c) from t_ex)").Check(testkit.Rows("1"))
	maskedPlan := plan(tk, fmt.Sprintf(summary, "masked_reader"))
	require.Contains(t, maskedPlan, "IndexRangeScan")
	require.NotContains(t, maskedPlan, "secret")
	require.Contains(t, plan(root, fmt.Sprintf(summary, "exempt_reader")), "secret")
}

func TestMaskingPolicyRestrictOnDuplicateAssignment(t *testing.T) {
	store := testkit.CreateMockStore(t)
	root := testkit.NewTestKit(t, store)
	require.NoError(t, root.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "%"}, nil, nil, nil))
	root.MustExec("use test")
	root.MustExec("create table t_od(id int primary key, c varchar(20), c2 varchar(20))")
	root.MustExec("create table t_od_free(id int primary key, c varchar(20), c2 varchar(20))")
	root.MustExec("create table t_od_src(id int primary key, v varchar(20))")
	root.MustExec("insert into t_od_src values (1, 'from_src')")
	root.MustExec("create masking policy p on t_od(c) as case when current_role() != 'NONE' or current_user()='root@%' then c else mask_full(c) end restrict on (update_select) enable")
	root.MustExec("create masking policy p_free on t_od_free(c) as case when current_user()='root@%' then c else mask_full(c) end restrict on (insert_into_select, delete_select) enable")
	root.MustExec("create user u_od")
	root.MustExec("create role r_od")
	root.MustExec("grant r_od to u_od")
	root.MustExec("grant select, insert, update, delete on test.* to u_od")
	tk := testkit.NewTestKit(t, store)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "u_od", Hostname: "%"}, nil, nil, nil))
	tk.MustExec("use test")
	original := testkit.Rows("1 secret a")
	reset := func() {
		for _, tbl := range []string{"t_od", "t_od_free"} {
			root.MustExec("delete from " + tbl)
			root.MustExec("insert into " + tbl + " values (1,'secret','a')")
		}
	}
	rows := func(tbl string) *testkit.Result {
		return root.MustQuery("select id, c, c2 from " + tbl + " order by id")
	}
	reset()

	for _, sql := range []string{
		"insert into t_od values (1, 'n', 'n') on duplicate key update c2 = c",
		"insert into t_od values (1, 'n', 'n') on duplicate key update c2 = t_od.c",
		"insert into t_od values (1, 'n', 'n') on duplicate key update c2 = concat(c, '!')",
		"insert into t_od values (1, 'n', 'n') on duplicate key update c2 = if(c = 'secret', 'y', 'n')",
		"insert into t_od values (1, 'n', 'n') on duplicate key update c = upper(c)",
		"insert into t_od values (1, 'n', 'n') on duplicate key update c2 = 'k', c = concat(c, values(c))",
		"insert into t_od set id = 1, c = 'n' on duplicate key update c2 = c",
		"insert into t_od(id, c2) select id, v from t_od_src on duplicate key update c2 = t_od.c",
		"insert into t_od(id, c2) select id, v from t_od_src on duplicate key update c2 = concat(t_od.c, t_od_src.v)",
	} {
		tk.MustGetErrCode(sql, errno.ErrAccessDeniedToMaskedColumn)
		rows("t_od").Check(original)
	}
	require.ErrorContains(t, tk.ExecToErr("prepare s from 'insert into t_od values (?, ?, ?) on duplicate key update c2 = c'"), "Access to masked column")

	// Not restricted: the values of the new row, constants, other columns
	// and the protected column assigned to itself.
	for _, tc := range []struct {
		sql  string
		rows [][]any
	}{
		{"insert into t_od values (1, 'n', 'm') on duplicate key update c = values(c), c2 = values(c2)", testkit.Rows("1 n m")},
		{"insert into t_od values (1, 'n', 'm') on duplicate key update c2 = 'k'", testkit.Rows("1 secret k")},
		{"insert into t_od values (1, 'n', 'm') on duplicate key update c = c2", testkit.Rows("1 a a")},
		{"insert into t_od values (1, 'n', 'm') on duplicate key update c = c", original},
		{"insert into t_od values (1, 'n', 'm') on duplicate key update c2 = concat(c2, values(c))", testkit.Rows("1 secret an")},
		{"insert into t_od(id, c2) select id, v from t_od_src on duplicate key update c2 = t_od_src.v", testkit.Rows("1 secret from_src")},
		{"insert into t_od values (2, 'n', 'm') on duplicate key update c2 = 'k'", testkit.Rows("1 secret a", "2 n m")},
	} {
		tk.MustExec(tc.sql)
		rows("t_od").Check(tc.rows)
		reset()
	}

	// A policy without UPDATE_SELECT and an identity that the policy does not
	// mask keep writing original values.
	tk.MustExec("insert into t_od_free values (1, 'n', 'n') on duplicate key update c2 = concat(c, '!')")
	rows("t_od_free").Check(testkit.Rows("1 secret secret!"))
	root.MustExec("insert into t_od values (1, 'n', 'n') on duplicate key update c2 = concat(c, '!')")
	rows("t_od").Check(testkit.Rows("1 secret secret!"))
	reset()

	// A cached plan is checked again for the identity that runs it.
	tk.MustExec("set role all")
	tk.MustExec("prepare st from 'insert into t_od values (?, ?, ?) on duplicate key update c2 = c'")
	tk.MustExec("set @a = 1, @b = 'n'")
	tk.MustExec("execute st using @a, @b, @b")
	rows("t_od").Check(testkit.Rows("1 secret secret"))
	tk.MustExec("execute st using @a, @b, @b")
	reset()
	tk.MustExec("set role none")
	require.ErrorContains(t, tk.ExecToErr("execute st using @a, @b, @b"), "Access to masked column")
	rows("t_od").Check(original)
	tk.MustExec("set role all")
	tk.MustExec("execute st using @a, @b, @b")
	rows("t_od").Check(testkit.Rows("1 secret secret"))
	reset()

	root.MustExec("alter table t_od modify masking policy p set restrict on none")
	tk.MustExec("set role none")
	tk.MustExec("insert into t_od values (1, 'n', 'n') on duplicate key update c2 = c")
	rows("t_od").Check(testkit.Rows("1 secret secret"))
}

func TestMaskingPolicyRestrictDirectUpdateAssignmentPlanCache(t *testing.T) {
	for _, instance := range []bool{false, true} {
		t.Run(fmt.Sprint(instance), func(t *testing.T) {
			store := testkit.CreateMockStore(t)
			root := testkit.NewTestKit(t, store)
			root.MustExec("create user da_role_user")
			root.MustExec("create role da_unmask_role")
			root.MustExec("grant da_unmask_role to da_role_user")
			root.MustExec("grant select,update on test.* to da_role_user")
			root.MustExec("create table test.da_src(id int primary key, c varchar(20), c2 varchar(20))")
			root.MustExec("create table test.da_dst(id int primary key, c varchar(20))")
			root.MustExec("insert into test.da_src values (1, 'secret', 'a')")
			root.MustExec("insert into test.da_dst values (1, 'x')")
			root.MustExec("create masking policy p on test.da_src(c) as case when current_role() != 'NONE' then c else mask_full(c) end restrict on (update_select)")
			ctx := context.Background()
			if instance {
				ctx = context.WithValue(ctx, core.PlanCacheKeyEnableInstancePlanCache{}, true)
			}
			user := testkit.NewTestKit(t, store)
			require.NoError(t, user.Session().Auth(&auth.UserIdentity{Username: "da_role_user", Hostname: "%"}, nil, nil, nil))
			other := testkit.NewTestKit(t, store)
			require.NoError(t, other.Session().Auth(&auth.UserIdentity{Username: "da_role_user", Hostname: "%"}, nil, nil, nil))
			for _, sql := range []string{
				"update test.da_src set c2 = c where id > ?",
				"update test.da_dst d join test.da_src s on s.id = d.id set d.c = s.c where d.id > ?",
			} {
				root.MustExec("update test.da_src set c2 = 'a'")
				root.MustExec("update test.da_dst set c = 'x'")
				unchanged := func() {
					root.MustQuery("select c2 from test.da_src").Check(testkit.Rows("a"))
					root.MustQuery("select c from test.da_dst").Check(testkit.Rows("x"))
				}
				user.MustExec("set role all")
				user.MustExec("set @a = 5")
				user.MustExecWithContext(ctx, "prepare st from '"+sql+"'")
				for range 2 {
					user.MustExecWithContext(ctx, "execute st using @a")
				}
				user.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("1"))
				// The cached plan is checked again for the identity that runs it.
				user.MustExec("set role none")
				_, err := user.ExecWithContext(ctx, "execute st using @a")
				require.ErrorContains(t, err, "Access to masked column")
				if instance {
					// So is the plan another session finds in the instance cache.
					other.MustExec("set role all")
					other.MustExec("set @a = 0")
					other.MustExecWithContext(ctx, "prepare st from '"+sql+"'")
					other.MustExec("set role none")
					cloned := false
					execCtx := context.WithValue(ctx, core.PlanCacheKeyTestClone{}, func(base.Plan, base.Plan) { cloned = true })
					_, err = other.ExecWithContext(execCtx, "execute st using @a")
					require.ErrorContains(t, err, "Access to masked column")
					require.True(t, cloned)
					other.MustExec("deallocate prepare st")
				}
				user.MustExec("set @a = 0")
				_, err = user.ExecWithContext(ctx, "execute st using @a")
				require.ErrorContains(t, err, "Access to masked column")
				unchanged()
				user.MustExec("set role all")
				user.MustExecWithContext(ctx, "execute st using @a")
				user.MustQueryWithContext(ctx, "select @@last_plan_from_cache").Check(testkit.Rows("1"))
				if strings.Contains(sql, "da_dst") {
					root.MustQuery("select c from test.da_dst").Check(testkit.Rows("secret"))
				} else {
					root.MustQuery("select c2 from test.da_src").Check(testkit.Rows("secret"))
				}
				user.MustExec("deallocate prepare st")
			}
		})
	}
}
