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

package schema

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

func TestSchemaCannotFindColumnRegression(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec(`drop view if exists v_issue65892_topn, v_issue65892_lookup`)
		tk.MustExec(`drop table if exists t1, t3, t4, t_issue65892_topn, t_issue65892_lookup`)
		tk.MustExec(`create table t1 (
  id bigint primary key,
  left_v bigint not null
)`)
		tk.MustExec(`create table t3 (
  id bigint primary key,
  right_v bigint not null
)`)
		tk.MustExec(`create table t4 (
  id bigint primary key,
  right_v bigint not null,
  flag tinyint not null
)`)
		tk.MustExec("insert into t1 values (10, 93)")
		tk.MustExec("insert into t3 values (10, 749), (20, 749), (30, 1000)")
		tk.MustExec("insert into t4 values (10, 749, 1), (20, 749, 0), (30, 1000, 1)")
		tk.MustExec(`create table t_issue65892_topn (
  id int primary key,
  payload int not null,
  sort_key int not null
)`)
		tk.MustExec(`create table t_issue65892_lookup (
  id int primary key,
  payload int not null
)`)
		tk.MustExec("insert into t_issue65892_topn values (1, 60, 10), (2, 70, 20), (3, 90, 30)")
		tk.MustExec("insert into t_issue65892_lookup values (1, 55), (2, 65), (4, 88)")
		tk.MustExec(`create view v_issue65892_topn (c0, c1, c2) as
select id as c0, payload as c1, 28 - sort_key as c2
from t_issue65892_topn
order by 28 - sort_key
limit 2`)
		tk.MustExec(`create view v_issue65892_lookup (c0, c1) as
select id as c0, payload as c1
from t_issue65892_lookup`)

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suite := GetSchemaSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			testdata.OnRecord(func() {
				planRows := testdata.ConvertRowsToStrings(tk.MustQuery("explain format = 'plan_tree' " + sql).Rows())
				if len(planRows) == 0 {
					t.Fatalf("empty plan for sql: %s", sql)
				}
				output[i].SQL = sql
				output[i].Plan = planRows
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("explain format = 'plan_tree' " + sql).Check(testkit.Rows(output[i].Plan...))
			tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
		}
		tk.MustQuery("SELECT /* issue:66272-nested */ t1.id FROM t1 JOIN t3 USING(id) JOIN t4 ON t4.id = t1.id WHERE t3.id >= 10 AND t3.id <= 20 AND t1.left_v = 93 AND t4.flag = 1").Check(testkit.Rows(
			"10",
		))
		tk.MustQuery("SELECT /* issue:66272-having */ id FROM t1 JOIN t3 USING(id) GROUP BY id HAVING t3.id = 10").Check(testkit.Rows(
			"10",
		))
		tk.MustQuery("SELECT /* issue:66272-orderby */ t1.id FROM t1 JOIN t3 USING(id) WHERE t3.id = 10 ORDER BY t3.id").Check(testkit.Rows(
			"10",
		))
		tk.MustQuery("SELECT /* issue:66272-all */ id AS t0_id FROM t1 JOIN t3 USING(id) WHERE (((t3.right_v = 749) AND (t3.id = 10)) AND (t1.left_v = 93)) AND (t3.right_v = ALL (SELECT t3.right_v FROM t3 WHERE t3.right_v = 749))").Check(testkit.Rows(
			"10",
		))
		stmtID, _, fields, err := tk.Session().PrepareStmt("SELECT /* issue:66272-metadata */ t3.id FROM t1 JOIN t3 USING(id)")
		tk.RequireNoError(err)
		defer func() {
			tk.RequireNoError(tk.Session().DropPreparedStmt(stmtID))
		}()
		tk.RequireEqual(1, len(fields))
		tk.RequireEqual("t3", fields[0].Table.Name.O)
		tk.RequireEqual("t3", fields[0].TableAsName.O)
		tk.RequireEqual("id", fields[0].Column.Name.O)
		tk.RequireEqual("id", fields[0].ColumnAsName.O)
		tk.MustQuery("SELECT /* issue:66272-metadata */ t3.id FROM t1 JOIN t3 USING(id)").Check(testkit.Rows(
			"10",
		))

		tk.MustExec("drop table if exists t_mixed_l, t_mixed_r")
		tk.MustExec("create table t_mixed_l (id varchar(10) primary key, left_v int not null)")
		tk.MustExec("create table t_mixed_r (id int primary key, right_v int not null)")
		tk.MustExec("insert into t_mixed_l values ('01', 10), ('02', 20)")
		tk.MustExec("insert into t_mixed_r values (1, 100), (2, 200)")
		tk.MustQuery("SELECT /* issue:66272-type-safe */ t_mixed_r.id FROM t_mixed_l JOIN t_mixed_r USING(id) WHERE t_mixed_r.id = '01a'").Check(testkit.Rows(
			"1",
		))

		tk.MustExec("drop table if exists t_up_l, t_up_r")
		tk.MustExec("create table t_up_l (id int primary key, a int not null)")
		tk.MustExec("create table t_up_r (id int primary key)")
		tk.MustExec("insert into t_up_l values (1, 2), (2, 100), (3, 300)")
		tk.MustExec("insert into t_up_r values (2), (3)")
		tk.MustExec("update t_up_l join t_up_r using(id) set t_up_l.a = t_up_l.a + 1000 where t_up_r.id = 2")
		tk.MustQuery("select id, a from t_up_l order by id").Check(testkit.Rows(
			"1 2",
			"2 1100",
			"3 300",
		))

		tk.MustExec("drop table if exists t_del_l, t_del_r")
		tk.MustExec("create table t_del_l (id int primary key, a int not null)")
		tk.MustExec("create table t_del_r (id int primary key)")
		tk.MustExec("insert into t_del_l values (1, 2), (2, 9), (3, 2)")
		tk.MustExec("insert into t_del_r values (2), (3)")
		tk.MustExec("delete t_del_l from t_del_l join t_del_r using(id) where t_del_r.id = 2")
		tk.MustQuery("select id, a from t_del_l order by id").Check(testkit.Rows(
			"1 2",
			"3 2",
		))

		tk.MustExec("drop table if exists t_ru_l, t_ru_r")
		tk.MustExec("create table t_ru_l (id int primary key, a int not null)")
		tk.MustExec("create table t_ru_r (id int primary key)")
		tk.MustExec("insert into t_ru_l values (1, 2), (2, 100), (3, 300)")
		tk.MustExec("insert into t_ru_r values (2), (4)")
		tk.MustExec("update t_ru_l right join t_ru_r using(id) set t_ru_l.a = t_ru_l.a + 1000 where t_ru_r.id = 2")
		tk.MustQuery("select id, a from t_ru_l order by id").Check(testkit.Rows(
			"1 2",
			"2 1100",
			"3 300",
		))

		tk.MustExec("drop table if exists t_rd_l, t_rd_r")
		tk.MustExec("create table t_rd_l (id int primary key, a int not null)")
		tk.MustExec("create table t_rd_r (id int primary key)")
		tk.MustExec("insert into t_rd_l values (1, 2), (2, 9), (3, 2)")
		tk.MustExec("insert into t_rd_r values (2), (4)")
		tk.MustExec("delete t_rd_l from t_rd_l right join t_rd_r using(id) where t_rd_r.id = 2")
		tk.MustQuery("select id, a from t_rd_l order by id").Check(testkit.Rows(
			"1 2",
			"3 2",
		))

		tk.MustExec("drop table if exists t_outer_l, t_outer_r")
		tk.MustExec("create table t_outer_l (id int primary key, a int not null)")
		tk.MustExec("create table t_outer_r (id int primary key)")
		tk.MustExec("insert into t_outer_l values (1, 10), (2, 20)")
		tk.MustExec("insert into t_outer_r values (2), (3)")
		tk.MustQuery("select count(*) from t_outer_l left join t_outer_r using(id) where t_outer_r.id is null").Check(testkit.Rows(
			"1",
		))
		tk.MustQuery("select count(*) from t_outer_l right join t_outer_r using(id) where t_outer_l.id is null").Check(testkit.Rows(
			"1",
		))

		issue65886SourceSQL := prepareIssue65886RegressionSchema(tk)
		for _, tc := range []struct {
			sql      string
			expected []string
		}{
			{
				sql:      "select /* issue:65886-view */ count(*) as cnt, sum(sum1) as sum1 from issue65886_v85 where sum1 > 20 and sum1 < 74",
				expected: []string{"0 <nil>"},
			},
			{
				sql: "select /* issue:65886-derived */ count(*) as cnt, sum(sum1) as sum1 from (" +
					issue65886SourceSQL + ") issue65886_dt where sum1 > 20 and sum1 < 74",
				expected: []string{"0 <nil>"},
			},
			{
				sql: "select /* issue:65886-subquery */ (select count(*) from (" +
					issue65886SourceSQL + ") issue65886_sq where sum1 > 20 and sum1 < 74)",
				expected: []string{"0"},
			},
			{
				sql: "with issue65886_cte as (" + issue65886SourceSQL +
					") select /* issue:65886-cte */ count(*) as cnt, sum(sum1) as sum1 from issue65886_cte where sum1 > 20 and sum1 < 74",
				expected: []string{"0 <nil>"},
			},
		} {
			tk.MustQuery(tc.sql).Check(testkit.Rows(tc.expected...))
		}
	})
}

func prepareIssue65886RegressionSchema(tk *testkit.TestKit) string {
	tk.MustExec("drop view if exists issue65886_v85, issue65886_v45, issue65886_v26, issue65886_v0")
	tk.MustExec("drop table if exists issue65886_t0, issue65886_t2, issue65886_t3, issue65886_t4")
	tk.MustExec(`create table issue65886_t0 (
  id bigint not null,
  k0 int not null,
  k1 bigint not null,
  k2 int not null,
  k3 varchar(64) not null,
  p0 varchar(64) not null,
  p1 int not null,
  primary key (id)
)`)
	tk.MustExec(`create table issue65886_t2 (
  id bigint not null,
  k1 bigint not null,
  k0 int not null,
  d0 int not null,
  d1 double not null,
  primary key (id)
)`)
	tk.MustExec(`create table issue65886_t3 (
  id bigint not null,
  k2 int not null,
  k0 int not null,
  d0 tinyint(1) not null,
  d1 double not null,
  primary key (id)
)`)
	tk.MustExec(`create table issue65886_t4 (
  id bigint not null,
  k3 varchar(64) not null,
  k0 int not null,
  d0 float not null,
  d1 bigint not null,
  primary key (id)
)`)
	tk.MustExec(`create algorithm=undefined sql security definer view issue65886_v0 (c0, c1) as
select 28.17 as c0, _utf8mb4's41' as c1
from ((issue65886_t0 left join issue65886_t4 using (k3, k0)) right join issue65886_t3 on (issue65886_t0.k0 = issue65886_t3.k0))
right join issue65886_t2 on (issue65886_t0.k1 = issue65886_t2.k1)
where (issue65886_t0.k1 < issue65886_t3.k2)
order by issue65886_t2.id, issue65886_t0.k0`)
	tk.MustExec(`create algorithm=undefined sql security definer view issue65886_v26 (c0, c1, c2) as
select
  (issue65886_t4.d1 + issue65886_t3.k2) as c0,
  _utf8mb4'2024-01-25 12:10:00' as c1,
  (select count(1) as cnt from issue65886_v0 where (issue65886_v0.c0 = issue65886_t0.k2) order by count(1) limit 7) as c2
from (issue65886_t0 left join issue65886_t3 on (issue65886_t0.k0 = issue65886_t3.k0))
join issue65886_t4 on (issue65886_t0.k3 = issue65886_t4.k3)
where ((issue65886_t0.k3 in (select issue65886_t4.k3 as c0 from issue65886_t4 where (issue65886_t4.k0 = issue65886_t0.k0))) and (issue65886_t0.k1 in (18, 76)))`)
	tk.MustExec(`create algorithm=undefined sql security definer view issue65886_v45 (c0) as
select 5.92 as c0
from ((issue65886_t0 right join issue65886_t3 on (issue65886_t0.k2 = issue65886_t3.k2)) left join issue65886_t4 on (issue65886_t0.k3 = issue65886_t4.k3))
right join issue65886_t2 on (issue65886_t0.k1 = issue65886_t2.k1)
where (issue65886_t0.k0 <= issue65886_t3.k0)`)
	sourceSQL := `select
  issue65886_t0.k2 as g0,
  count(1) as cnt,
  sum(issue65886_t2.d0) as sum1
from (issue65886_t0 left join issue65886_t4 on (issue65886_t0.k0 = issue65886_t4.k0))
left join issue65886_t2 using (k1)
where
  ((issue65886_t4.k0 < issue65886_t2.k1)
  and (not (issue65886_t0.k0 in (select issue65886_v45.c0 as c0 from issue65886_v45 where (issue65886_v45.c0 = issue65886_t0.k2)))
  and not (issue65886_t0.k2 in (select issue65886_v26.c2 as c0 from issue65886_v26 where (issue65886_v26.c0 = issue65886_t0.k3) limit 5))))
group by issue65886_t0.k2`
	tk.MustExec(`create algorithm=undefined sql security definer view issue65886_v85 (g0, cnt, sum1) as ` + sourceSQL)
	return sourceSQL
}
