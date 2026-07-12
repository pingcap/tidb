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

package integration_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestAutoEmbeddingVectorSearchCompatibility preserves successful
// GeneratedExprString rewrite shapes and exercises resolver-only lineage paths.
func TestAutoEmbeddingVectorSearchCompatibility(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	if !enableStarterDeployModeForTest(t) {
		t.Skip("EMBED_TEXT is only supported in starter deployment mode")
	}
	ensureMockEmbeddingProvider(t, tk)

	// Keep these option keys non-colliding. Colliding @search trim chains have
	// pre-existing map-iteration nondeterminism unrelated to provenance resolution.
	const generatedOptions = `{"plus":0.1,"plus@search":0.2}`
	const explicitEmbedding = `embed_text('mock/json', '[1,2,3]', '{"plus":0.2}')`

	createAutoEmbedTable := func(name string) {
		t.Helper()
		tk.MustExec(`create table ` + name + `(
			id int primary key,
			text text,
			vec vector(3) generated always as
				(embed_text('mock/json', text, '` + generatedOptions + `')) stored,
			dist double
		)`)
		tk.MustExec(`insert into ` + name + `(id, text, dist) values
			(1, '[1,2,3]', 0), (2, '[4,5,6]', 0)`)
	}
	createAutoEmbedTable("ae_t")
	createAutoEmbedTable("ae_same")
	createAutoEmbedTable("ae_third")
	tk.MustExec(`create view ae_view as select vec from ae_t`)

	checkRewrite := func(rewrittenSQL, explicitSQL string) {
		t.Helper()
		tk.MustQuery(rewrittenSQL).Check(tk.MustQuery(explicitSQL).Rows())
	}
	checkRewritePlan := func(sql string) {
		t.Helper()
		tk.MustQuery(`explain format = 'plan_tree' ` + sql).MultiCheckContain([]string{"vec_l2_distance"})
	}

	for _, tc := range []struct {
		embedFn    string
		distanceFn string
	}{
		{embedFn: "vec_embed_l1_distance", distanceFn: "vec_l1_distance"},
		{embedFn: "vec_embed_l2_distance", distanceFn: "vec_l2_distance"},
		{embedFn: "vec_embed_cosine_distance", distanceFn: "vec_cosine_distance"},
		{embedFn: "vec_embed_negative_inner_product", distanceFn: "vec_negative_inner_product"},
	} {
		checkRewrite(
			`select `+tc.embedFn+`(vec, '[1,2,3]') from ae_t order by id`,
			`select `+tc.distanceFn+`(vec, `+explicitEmbedding+`) from ae_t order by id`,
		)
	}

	for _, tc := range []struct {
		rewrittenSQL string
		explicitSQL  string
	}{
		{
			rewrittenSQL: `select id from ae_t where vec_embed_l2_distance(vec, '[1,2,3]') < 1 order by id`,
			explicitSQL:  `select id from ae_t where vec_l2_distance(vec, ` + explicitEmbedding + `) < 1 order by id`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance(vec, '[1,2,3]') from ae_t where false`,
			explicitSQL:  `select vec_l2_distance(vec, ` + explicitEmbedding + `) from ae_t where false`,
		},
		{
			rewrittenSQL: `select id from ae_t order by vec_embed_l2_distance(vec, '[1,2,3]'), id`,
			explicitSQL:  `select id from ae_t order by vec_l2_distance(vec, ` + explicitEmbedding + `), id`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance(v, '[1,2,3]') from (select vec as v from ae_t) x order by 1`,
			explicitSQL:  `select vec_l2_distance(v, ` + explicitEmbedding + `) from (select vec as v from ae_t) x order by 1`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance(vec, '[1,2,3]') from (select vec from ae_t order by id limit 2) x order by 1`,
			explicitSQL:  `select vec_l2_distance(vec, ` + explicitEmbedding + `) from (select vec from ae_t order by id limit 2) x order by 1`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance(vec, '[1,2,3]') from (select vec, row_number() over(order by id) rn from ae_t) x order by 1`,
			explicitSQL:  `select vec_l2_distance(vec, ` + explicitEmbedding + `) from (select vec, row_number() over(order by id) rn from ae_t) x order by 1`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance(ae_t.vec, '[1,2,3]') from ae_t join ae_same on ae_t.id = ae_same.id order by ae_t.id`,
			explicitSQL:  `select vec_l2_distance(ae_t.vec, ` + explicitEmbedding + `) from ae_t join ae_same on ae_t.id = ae_same.id order by ae_t.id`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance(vec, '[1,2,3]') from (select vec from ae_t union all select vec from ae_same) x order by 1`,
			explicitSQL:  `select vec_l2_distance(vec, ` + explicitEmbedding + `) from (select vec from ae_t union all select vec from ae_same) x order by 1`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance(vec, '[1,2,3]') from (select distinct vec from ae_t) x order by 1`,
			explicitSQL:  `select vec_l2_distance(vec, ` + explicitEmbedding + `) from (select distinct vec from ae_t) x order by 1`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance(vec, '[1,2,3]') from (select vec from ae_t group by vec) x order by 1`,
			explicitSQL:  `select vec_l2_distance(vec, ` + explicitEmbedding + `) from (select vec from ae_t group by vec) x order by 1`,
		},
		{
			rewrittenSQL: `with x as (select vec from ae_t) select vec_embed_l2_distance(vec, '[1,2,3]') from x order by 1`,
			explicitSQL:  `with x as (select vec from ae_t) select vec_l2_distance(vec, ` + explicitEmbedding + `) from x order by 1`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance(vec, '[1,2,3]') from ae_view order by 1`,
			explicitSQL:  `select vec_l2_distance(vec, ` + explicitEmbedding + `) from ae_view order by 1`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance(vec, (select '[1,2,3]' where ae_t.id = 1)) from ae_t where id = 1`,
			explicitSQL:  `select vec_l2_distance(vec, embed_text('mock/json', (select '[1,2,3]' where ae_t.id = 1), '{"plus":0.2}')) from ae_t where id = 1`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance((select x.vec from ae_t x where x.id = ae_t.id), '[1,2,3]') from ae_t order by id`,
			explicitSQL:  `select vec_l2_distance((select x.vec from ae_t x where x.id = ae_t.id), ` + explicitEmbedding + `) from ae_t order by id`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance(d.vec, '[1,2,3]') from (select distinct id, vec from ae_t) d where exists (select 1 from ae_same where ae_same.id = d.id) order by d.id`,
			explicitSQL:  `select vec_l2_distance(d.vec, ` + explicitEmbedding + `) from (select distinct id, vec from ae_t) d where exists (select 1 from ae_same where ae_same.id = d.id) order by d.id`,
		},
		{
			rewrittenSQL: `select vec_embed_l2_distance(d.vec, '[1,2,3]') from (select distinct id, vec from ae_t) d where not exists (select 1 from ae_same where ae_same.id = d.id + 10) order by d.id`,
			explicitSQL:  `select vec_l2_distance(d.vec, ` + explicitEmbedding + `) from (select distinct id, vec from ae_t) d where not exists (select 1 from ae_same where ae_same.id = d.id + 10) order by d.id`,
		},
	} {
		checkRewrite(tc.rewrittenSQL, tc.explicitSQL)
	}

	// NextGen does not execute vector-valued USING join keys yet, so these
	// compatibility rows validate lowering at the planner boundary.
	checkRewritePlan(`select vec_embed_l2_distance(vec, '[1,2,3]') from ae_t join ae_same using(vec) order by 1`)
	checkRewritePlan(`select vec_embed_l2_distance(ae_same.vec, '[1,2,3]') from ae_t join ae_same using(vec) where ae_t.id > 0 order by ae_t.id limit 2`)
	checkRewritePlan(`select vec_embed_l2_distance(ae_same.vec, (select '[1,2,3]' where ae_t.id = 1)) from ae_t join ae_same using(vec) where ae_t.id = 1`)
	checkRewritePlan(`select vec_embed_l2_distance(vec, '[1,2,3]') from ae_t join ae_same using(vec) join ae_third using(vec) order by 1`)

	tk.MustExec(`set tidb_enable_prepared_plan_cache = on`)
	tk.MustExec(`prepare ae_stmt from 'select id from ae_t where vec_embed_l2_distance(vec, ?) < 1 order by id'`)
	tk.MustExec(`set @ae_query = '[1,2,3]'`)
	tk.MustQuery(`execute ae_stmt using @ae_query`).Check(testkit.Rows("1"))
	tk.MustExec(`deallocate prepare ae_stmt`)

	createAutoEmbedTable("ae_dml")
	tk.MustExec(`update ae_dml set dist = vec_embed_l2_distance(vec, '[1,2,3]') where id = 1`)
	tk.MustQuery(`select dist = vec_l2_distance(vec, ` + explicitEmbedding + `) from ae_dml where id = 1`).Check(testkit.Rows("1"))
	tk.MustExec(`delete from ae_dml where vec_embed_l2_distance(vec, '[1,2,3]') < 1`)
	tk.MustQuery(`select id from ae_dml order by id`).Check(testkit.Rows("2"))

	createAutoEmbedTable("ae_dup")
	tk.MustExec(`insert into ae_dup(id, text, dist) values (1, '[1,2,3]', 0)
		on duplicate key update dist = vec_embed_l2_distance(ae_dup.vec, '[1,2,3]')`)
	tk.MustQuery(`select dist = vec_l2_distance(vec, ` + explicitEmbedding + `) from ae_dup where id = 1`).Check(testkit.Rows("1"))

	for _, cascades := range []string{"off", "on"} {
		tk.MustExec(`set tidb_enable_cascades_planner = ` + cascades)
		target := "ae_insert_source_" + cascades
		tk.MustExec(`create table ` + target + `(id int primary key, vec vector(3), dist double)`)
		tk.MustExec(`insert into ` + target + ` values (1, '[0,0,0]', 0)`)
		tk.MustExec(`insert into ` + target + `(id, vec, dist)
			select id, src.vec, 0 from ae_t src where id = 1
			on duplicate key update dist = vec_embed_l2_distance(src.vec, '[1,2,3]')`)
		tk.MustQuery(`select dist = vec_l2_distance((select vec from ae_t where id = 1), ` + explicitEmbedding + `) from ` + target).
			Check(testkit.Rows("1"))
		tk.MustExec(`insert into ` + target + `(id, vec, dist)
			select id, src.vec, 0 from ae_t src where false
			on duplicate key update dist = vec_embed_l2_distance(src.vec, '[1,2,3]')`)
	}
}

func TestAutoEmbeddingVectorSearchRejects(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	if !enableStarterDeployModeForTest(t) {
		t.Skip("EMBED_TEXT is only supported in starter deployment mode")
	}
	ensureMockEmbeddingProvider(t, tk)

	tk.MustExec(`create table ae_r(
		id int primary key,
		text text,
		vec vector(3) generated always as (embed_text('mock/json', text)) stored,
		dist double
	)`)
	tk.MustExec(`create table ae_r_diff(
		id int primary key,
		text text,
		vec vector(3) generated always as (embed_text('mock/other', text)) stored,
		dist double
	)`)
	tk.MustExec(`create table ae_r_dim4(
		dim4_id int primary key,
		text4 text,
		vec vector(4) generated always as (embed_text('mock/json', text4)) stored,
		dist4 double
	)`)
	tk.MustExec(`create table ae_r_normal(id int primary key, vec vector(3), dist double)`)
	tk.MustExec(`create table ae_r_normal_nat(nid int primary key, vec vector(3))`)
	tk.MustExec(`create table ae_r_target(id int primary key, vec vector(3), dist double)`)

	const firstArgError = "first argument must be a vector embedding column generated by EMBED_TEXT()"
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{name: "ordinary vector", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from ae_r_normal`},
		{name: "ordinary vector with false predicate", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from ae_r_normal where false`},
		{name: "cast output", sql: `select vec_embed_l2_distance(cast(vec as vector(3)), '[1,2,3]') from ae_r`},
		{name: "window output", sql: `select vec_embed_l2_distance(wv, '[1,2,3]') from (select first_value(vec) over(order by id) wv from ae_r) x`},
		{name: "aggregate output", sql: `select vec_embed_l2_distance(v, '[1,2,3]') from (select any_value(vec) v from ae_r) x`},
		{name: "mixed union", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from (select vec from ae_r union all select vec from ae_r_normal) x`},
		{name: "conflicting union", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from (select vec from ae_r union all select vec from ae_r_diff) x`},
		{name: "union distinct", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from (select vec from ae_r union select vec from ae_r) x`},
		{name: "intersect", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from (select vec from ae_r intersect select vec from ae_r) x`},
		{name: "except", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from (select vec from ae_r except select vec from ae_r) x`},
		{name: "using visible", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from ae_r join ae_r_normal using(vec)`},
		{name: "using auto qualified", sql: `select vec_embed_l2_distance(ae_r.vec, '[1,2,3]') from ae_r join ae_r_normal using(vec)`},
		{name: "using normal qualified", sql: `select vec_embed_l2_distance(ae_r_normal.vec, '[1,2,3]') from ae_r join ae_r_normal using(vec)`},
		{name: "using below wrappers", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from (select vec from ae_r join ae_r_normal using(vec) where ae_r.id > 0 order by ae_r.id limit 1) x`},
		{name: "using below apply", sql: `select vec_embed_l2_distance(ae_r.vec, (select '[1,2,3]' where ae_r.id = 1)) from ae_r join ae_r_normal using(vec)`},
		{name: "using different dimensions visible", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from ae_r right join ae_r_dim4 using(vec)`},
		{name: "using different dimensions qualified", sql: `select vec_embed_l2_distance(ae_r_dim4.vec, '[1,2,3]') from ae_r right join ae_r_dim4 using(vec)`},
		{name: "natural visible", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from ae_r natural join ae_r_normal_nat`},
		{name: "natural auto qualified", sql: `select vec_embed_l2_distance(ae_r.vec, '[1,2,3]') from ae_r natural join ae_r_normal_nat`},
		{name: "natural different dimensions visible", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from ae_r natural right join ae_r_dim4`},
		{name: "natural different dimensions qualified", sql: `select vec_embed_l2_distance(ae_r.vec, '[1,2,3]') from ae_r natural right join ae_r_dim4`},
		{name: "recursive cte", sql: `with recursive x(n, vec) as (select 1, vec from ae_r union all select n + 1, vec from x where n < 2) select vec_embed_l2_distance(vec, '[1,2,3]') from x`},
		{name: "rollup output", sql: `select vec_embed_l2_distance(vec, '[1,2,3]') from (select vec from ae_r group by vec with rollup) x`},
	} {
		err := tk.ExecToErr(tc.sql)
		require.ErrorContainsf(t, err, firstArgError, "case %q", tc.name)
	}

	for _, tc := range []struct {
		name string
		sql  string
	}{
		{name: "update using", sql: `update ae_r join ae_r_normal using(vec) set ae_r.dist = vec_embed_l2_distance(ae_r.vec, '[1,2,3]')`},
		{name: "delete using", sql: `delete ae_r from ae_r join ae_r_normal using(vec) where vec_embed_l2_distance(ae_r.vec, '[1,2,3]') < 1`},
		{name: "update natural", sql: `update ae_r natural join ae_r_normal_nat set ae_r.dist = vec_embed_l2_distance(ae_r.vec, '[1,2,3]')`},
		{name: "delete natural", sql: `delete ae_r from ae_r natural join ae_r_normal_nat where vec_embed_l2_distance(ae_r.vec, '[1,2,3]') < 1`},
	} {
		err := tk.ExecToErr(tc.sql)
		require.ErrorContainsf(t, err, firstArgError, "case %q", tc.name)
	}

	for _, cascades := range []string{"off", "on"} {
		tk.MustExec(`set tidb_enable_cascades_planner = ` + cascades)
		for _, tc := range []struct {
			name  string
			setOp string
		}{
			{name: "intersect", setOp: "intersect"},
			{name: "except", setOp: "except"},
		} {
			err := tk.ExecToErr(`insert into ae_r_target(id, vec, dist)
				select id, vec, 0 from (
					select id, vec from ae_r ` + tc.setOp + `
					select id, vec from ae_r_normal group by id, vec
				) s on duplicate key update
				dist = vec_embed_l2_distance(s.vec, '[1,2,3]')`)
			require.ErrorContainsf(t, err, firstArgError, "%s with cascades %s", tc.name, cascades)
		}
	}
}
