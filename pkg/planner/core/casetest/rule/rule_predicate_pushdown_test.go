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

package rule

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func runPredicatePushdownTestData(t *testing.T, tk *testkit.TestKit, name string) {
	var input []string
	var output []struct {
		SQL     string
		Plan    []string
		Warning []string
	}
	predicatePushdownSuiteData := GetPredicatePushdownSuiteData()
	predicatePushdownSuiteData.LoadTestCasesByName(name, t, &input, &output)
	require.Equal(t, len(input), len(output))
	for i := range input {
		if strings.Contains(input[i], "set") {
			tk.MustExec(input[i])
			continue
		}
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' " + input[i]).Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'brief' " + input[i]).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warning...))
	}
}

func TestConstantPropagateWithCollation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	// create table
	tk.MustExec("use test")
	tk.MustExec("create table t (id int primary key, name varchar(20));")

	runPredicatePushdownTestData(t, tk, "TestConstantPropagateWithCollation")
}

func TestNestedInNonNullKeysPropagation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id bigint primary key)")
	tk.MustExec("create table a (id bigint primary key auto_increment, t_id bigint, key(t_id))")
	tk.MustExec("create table b (id bigint primary key auto_increment, t_id bigint, key(t_id))")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20)")
	tk.MustExec("insert into a(t_id) values (1),(3),(3),(5),(7),(null)")
	tk.MustExec("insert into b(t_id) values (2),(3),(8),(11),(15),(null)")
	for _, rewrite := range []string{"ON", "OFF"} {
		for _, tc := range []struct {
			name   string
			filter string
			rows   []string
		}{
			{name: "single_value", filter: "o.id in (3)", rows: []string{"3"}},
			{name: "multiple_values", filter: "o.id in (3,7,11,17)", rows: []string{"3", "7", "11"}},
			{name: "range", filter: "o.id between 3 and 8", rows: []string{"3", "5", "7", "8"}},
		} {
			t.Run(rewrite+"_"+tc.name, func(t *testing.T) {
				tk := testkit.NewTestKit(t, store)
				tk.MustExec("use test")
				tk.MustExec("set tidb_opt_insubq_to_join_and_agg = " + rewrite)
				sql := "select o.id from t o where " + tc.filter +
					" and (o.id in (select i.id from t i where i.id in (select t_id from a))" +
					" or o.id in (select i.id from t i where i.id in (select t_id from b))) order by o.id"
				tk.MustQuery(sql).Check(testkit.Rows(tc.rows...))
				plan := strings.Join(testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' "+sql).Rows()), "\n")
				require.NotContains(t, plan, "FullScan", "the outer key filter must also restrict the nested IN inputs:\n%s", plan)
			})
		}
	}
	sql := "select count(*) from t o where o.id between 3 and 8" +
		" and (o.id in (select i.id from t i where i.id in (select t_id from a))" +
		" or o.id in (select i.id from t i where i.id in (select t_id from b)))"
	tk.MustQuery(sql).Check(testkit.Rows("4"))
	plan := strings.Join(testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'brief' "+sql).Rows()), "\n")
	require.NotContains(t, plan, "FullScan", "the COUNT query must also restrict the nested IN inputs:\n%s", plan)
}

func TestScalarInNullSemanticsWithPropagation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t (id bigint primary key)")
	tk.MustExec("create table n (id bigint, key(id))")
	tk.MustExec("create table p (id bigint)")
	tk.MustExec("create table pairs_outer (id bigint not null, k bigint)")
	tk.MustExec("create table pairs_inner (id bigint not null, k bigint, key(id))")
	tk.MustExec("insert into t values (1),(2),(3),(7),(9)")
	tk.MustExec("insert into n values (null),(3)")
	tk.MustExec("insert into p values (null),(3),(7),(8)")
	tk.MustExec("insert into pairs_outer values (3,1),(7,1)")
	tk.MustExec("insert into pairs_inner values (3,null),(99,null)")
	for _, tc := range []struct {
		name string
		sql  string
		rows []string
	}{
		{
			name: "nullable_inner_key",
			sql:  "select o.id, o.id in (select id from n), o.id not in (select id from n) from t o where o.id in (3,7) order by o.id",
			rows: []string{"3 1 0", "7 <nil> <nil>"},
		},
		{
			name: "nullable_outer_key",
			sql:  "select o.id, o.id in (select id from t), o.id not in (select id from t) from p o where o.id is null or o.id in (3,8) order by o.id",
			rows: []string{"<nil> <nil> <nil>", "3 1 0", "8 0 1"},
		},
		{
			name: "null_extended_primary_key",
			sql:  "select o.id, i.id in (select id from t), i.id not in (select id from t) from t o left join t i on i.id = o.id + 100 where o.id in (3,7) order by o.id",
			rows: []string{"3 <nil> <nil>", "7 <nil> <nil>"},
		},
		{
			name: "empty_inner",
			sql:  "select o.id, o.id in (select id from n where id < 0), o.id not in (select id from n where id < 0) from p o where o.id is null or o.id in (3,7) order by o.id",
			rows: []string{"<nil> 0 1", "3 0 1", "7 0 1"},
		},
		{
			name: "mixed_nullable_row_keys",
			sql:  "select o.id, (o.id,o.k) in (select id,k from pairs_inner), (o.id,o.k) not in (select id,k from pairs_inner) from pairs_outer o where o.id in (3,7) and o.k in (1,2) order by o.id",
			rows: []string{"3 <nil> <nil>", "7 0 1"},
		},
		{
			name: "non_null_scalar_in",
			sql:  "select o.id, o.id in (select id from t where id < 5), o.id not in (select id from t where id < 5) from t o where o.id in (3,7) order by o.id",
			rows: []string{"3 1 0", "7 0 1"},
		},
		{
			name: "keep_limit_boundary",
			sql:  "select o.id, o.id in (select id from t order by id limit 2), o.id not in (select id from t order by id limit 2) from t o where o.id in (3,7) order by o.id",
			rows: []string{"3 0 1", "7 0 1"},
		},
		{
			name: "null_safe_equality",
			sql:  "select o.id, exists (select 1 from n where n.id <=> o.id) from p o where o.id is null or o.id in (3,7) order by o.id",
			rows: []string{"<nil> 1", "3 1", "7 0"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tk := testkit.NewTestKit(t, store)
			tk.MustExec("use test")
			tk.MustQuery(tc.sql).Check(testkit.Rows(tc.rows...))
		})
	}
}
