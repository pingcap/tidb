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

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func runPredicatePushdownTestData(t *testing.T, tk *testkit.TestKit, cascades, name string) {
	var input []string
	var output []struct {
		SQL     string
		Plan    []string
		Warning []string
	}
	predicatePushdownSuiteData := GetPredicatePushdownSuiteData()
	predicatePushdownSuiteData.LoadTestCasesByName(name, t, &input, &output, cascades)
	require.Equal(t, len(input), len(output))
	for i := range input {
		if strings.Contains(input[i], "set") {
			tk.MustExec(input[i])
			continue
		}
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'plan_tree' " + input[i]).Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
		})
		tk.MustQuery("explain format = 'plan_tree' " + input[i]).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warning...))
	}
}

func runPredicatePushdownTestDataWithResult(t *testing.T, tk *testkit.TestKit, cascades, name string) {
	var input []string
	var output []struct {
		SQL     string
		Plan    []string
		Result  []string
		Warning []string
	}
	predicatePushdownSuiteData := GetPredicatePushdownSuiteData()
	predicatePushdownSuiteData.LoadTestCasesByName(name, t, &input, &output, cascades)
	require.Equal(t, len(input), len(output))
	for i := range input {
		if strings.Contains(input[i], "set") {
			tk.MustExec(input[i])
			continue
		}
		testdata.OnRecord(func() {
			output[i].SQL = input[i]
			output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'plan_tree' " + input[i]).Rows())
			output[i].Warning = testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows())
			output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(input[i]).Rows())
		})
		tk.MustQuery("explain format = 'plan_tree' " + input[i]).Check(testkit.Rows(output[i].Plan...))
		tk.MustQuery("show warnings").Check(testkit.Rows(output[i].Warning...))
		tk.MustQuery(input[i]).Check(testkit.Rows(output[i].Result...))
	}
}

func TestPredicatePushdownSuite(t *testing.T) {
	t.Run("nested_in_non_null_keys", func(t *testing.T) {
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
					plan := strings.Join(testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'plan_tree' "+sql).Rows()), "\n")
					require.NotContains(t, plan, "FullScan", "the outer key filter must also restrict the nested IN inputs:\n%s", plan)
				})
			}
		}
		sql := "select count(*) from t o where o.id between 3 and 8" +
			" and (o.id in (select i.id from t i where i.id in (select t_id from a))" +
			" or o.id in (select i.id from t i where i.id in (select t_id from b)))"
		tk.MustQuery(sql).Check(testkit.Rows("4"))
		plan := strings.Join(testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'plan_tree' "+sql).Rows()), "\n")
		require.NotContains(t, plan, "FullScan", "the COUNT query must also restrict the nested IN inputs:\n%s", plan)
	})
	t.Run("scalar_in_null_semantics", func(t *testing.T) {
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
	})
	t.Run("preserve join comparison collation", func(t *testing.T) {
		tk := testkit.NewTestKit(t, testkit.CreateMockStore(t))
		tk.MustExec("use test")
		tk.MustExec("create table tb0(id int unique auto_increment not null, c0 year default 2000, c1 datetime, c2 longtext, c3 varbinary(182)) charset=utf8mb4")
		tk.MustExec("create table tb1(id int unique auto_increment not null, c0 char(162), primary key(id)) charset=latin1")
		tk.MustExec("create table tb2(id int unique auto_increment not null, c0 binary(8), c1 time default '12:00:00', c2 decimal(33,25) default 0, c3 varchar(135) default 'test')")
		tk.MustExec("create view v1 as select id,c0,c1,c2,c3,case when id is null then 'NO_DATA' else 'HAS_DATA' end as data_status from tb2 order by id desc limit 100")
		query := "select count(t2.c0),count(*) from v1 t0 inner join tb1 t1 on t0.data_status=t1.c0 inner join tb0 t2 on t1.id=t2.c2 right join (select c0 as col_0 from tb1) t3 on t2.c1=t3.col_0 where t2.c2 group by t0.id,t0.c0,t0.c2,t0.data_status having count(*)<87"
		tk.MustQuery(query).Check(testkit.Rows())
		tk.MustExec("insert into tb2(id) values(1)")
		tk.MustExec("insert into tb1(id,c0) values(1,'HAS_DATA'),(2,'2024-01-01 00:00:00')")
		tk.MustExec("insert into tb0(c1,c2) values('2024-01-01 00:00:00','1')")
		tk.MustQuery(query).Check(testkit.Rows("1 1"))
		tk.MustQuery(strings.ReplaceAll(query, "t0.data_status=t1.c0", "t1.c0=t0.data_status")).Check(testkit.Rows("1 1"))
		tk.MustQuery(strings.ReplaceAll(query, "t0.data_status=t1.c0", "t1.c0<=>t0.data_status")).Check(testkit.Rows("1 1"))
	})
	t.Run("TestConstantPropagateWithCollation", testConstantPropagateWithCollation)
	t.Run("TestPredicatePushDown", testPredicatePushDown)
}

func testConstantPropagateWithCollation(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("create table t (id int primary key, name varchar(20));")
		tk.MustExec(`create table foo(a int, b int, c int, primary key(a));`)
		tk.MustExec(`create table bar(a int, b int, c int, primary key(a));`)
		tk.MustExec("create table t0 (k0 int, p1 bigint)")
		tk.MustExec("create table t1 (k0 int, d0 varchar(64))")
		tk.MustExec("create table t2 (k0 int, k1 int)")
		tk.MustExec("create table t3 (k0 int, d0 decimal(12,2))")
		tk.MustExec("create table t4 (k0 int)")
		tk.MustExec("drop table if exists t1_65994")
		tk.MustExec("drop table if exists t2_65994")
		tk.MustExec("create table t1_65994 (a int, b int)")
		tk.MustExec("create table t2_65994 (a int, b int)")
		tk.MustExec("insert into t0 values (1, 10), (2, 20), (3, 30)")
		tk.MustExec("insert into t2 values (1, 100), (3, 300), (4, 400)")
		runPredicatePushdownTestDataWithResult(t, tk, cascades, "TestConstantPropagateWithCollation")
	})
}

func testPredicatePushDown(t *testing.T) {
	testkit.RunTestUnderCascadesWithDomain(t, func(t *testing.T, tk *testkit.TestKit, dom *domain.Domain, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`CREATE TABLE crm_rd_150m (
	product varchar(256) DEFAULT NULL,
		uks varchar(16) DEFAULT NULL,
		brand varchar(256) DEFAULT NULL,
		cin varchar(16) DEFAULT NULL,
		created_date timestamp NULL DEFAULT NULL,
		quantity int(11) DEFAULT NULL,
		amount decimal(11,0) DEFAULT NULL,
		pl_date timestamp NULL DEFAULT NULL,
		customer_first_date timestamp NULL DEFAULT NULL,
		recent_date timestamp NULL DEFAULT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;`)

		// Create virtual tiflash replica info.
		testkit.SetTiFlashReplica(t, dom, "test", "crm_rd_150m")

		tk.MustExec("set @@session.tidb_isolation_read_engines = 'tiflash'")
		tk.MustExec("explain format = 'plan_tree' SELECT /* issue:15110 */ count(*) FROM crm_rd_150m dataset_48 WHERE (CASE WHEN (month(dataset_48.customer_first_date)) <= 30 THEN '新客' ELSE NULL END) IS NOT NULL;")

		tk.MustExec("drop table if exists t31202")
		tk.MustExec("create table t31202(a int primary key, b int);")

		// Set the hacked TiFlash replica for explain tests.
		testkit.SetTiFlashReplica(t, dom, "test", "t31202")

		tk.MustQuery("explain format = 'plan_tree' select /* issue:31202 */ * from t31202;").Check(testkit.Rows(
			"TableReader root  MppVersion: 3, data:ExchangeSender",
			"└─ExchangeSender mpp[tiflash]  ExchangeType: PassThrough",
			"  └─TableFullScan mpp[tiflash] table:t31202 keep order:false, stats:pseudo"))

		tk.MustExec("set @@session.tidb_isolation_read_engines = 'tikv'")
		tk.MustQuery("explain format = 'plan_tree' select /* issue:31202 */ * from t31202 use index (primary);").Check(testkit.Rows(
			"TableReader root  data:TableFullScan",
			"└─TableFullScan cop[tikv] table:t31202 keep order:false, stats:pseudo"))
	})
}
