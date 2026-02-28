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
		tk.MustExec(`drop table if exists t1, t3, t4`)
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
				planRows := testdata.ConvertRowsToStrings(tk.MustQuery("explain format='brief' " + sql).Rows())
				if len(planRows) == 0 {
					t.Fatalf("empty plan for sql: %s", sql)
				}
				output[i].SQL = sql
				output[i].Plan = planRows
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("explain format='brief' " + sql).Check(testkit.Rows(output[i].Plan...))
			tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
		}
		tk.MustQuery("SELECT /* issue:66272-nested */ t1.id FROM t1 JOIN t3 USING(id) JOIN t4 ON t4.id = t1.id WHERE t3.id >= 10 AND t3.id <= 20 AND t1.left_v = 93 AND t4.flag = 1").Check(testkit.Rows(
			"10",
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
	})
}
