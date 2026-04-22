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

package windows

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

func TestWindowSubqueryRewrite(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists temperature_data, humidity_data, weather_report")
		defer tk.MustExec("drop table if exists temperature_data, humidity_data, weather_report")
		tk.MustExec("create table temperature_data (temperature double)")
		tk.MustExec("create table humidity_data (humidity double)")
		tk.MustExec("create table weather_report (report_id double, report_date varchar(100))")

		tk.MustExec("insert into temperature_data values (1.0)")
		tk.MustExec("insert into humidity_data values (0.5)")
		tk.MustExec("insert into weather_report values (2.0, 'test')")

		result := tk.MustQuery(`
   SELECT
     EXISTS (
       SELECT
         FIRST_VALUE(temp_data.temperature) OVER weather_window AS first_temperature,
         MIN(report_data.report_id) OVER weather_window AS min_report_id
       FROM
         temperature_data AS temp_data
       WINDOW weather_window AS (
         PARTITION BY EXISTS (
           SELECT
             report_data.report_date AS report_date
           FROM
             humidity_data AS humidity_data
           WHERE temp_data.temperature >= humidity_data.humidity
         )
       )
     ) AS is_exist
   FROM
     weather_report AS report_data;
 `)

		result.Check(testkit.Rows("1"))

		result = tk.MustQuery(`
   SELECT
     EXISTS (
       SELECT
         FIRST_VALUE(temp_data.temperature) OVER weather_window AS first_temperature,
         MIN(report_data.report_id) OVER weather_window AS min_report_id
       FROM
         temperature_data AS temp_data
       WINDOW weather_window AS (
         PARTITION BY temp_data.temperature 
       )
     ) AS is_exist
   FROM
     weather_report AS report_data;
 `)

		result.Check(testkit.Rows("1"))

		tk.MustExec("drop table if exists t1, t2")
		defer tk.MustExec("drop table if exists t1, t2")
		tk.MustExec("create table t1 (c1 int)")
		tk.MustExec("create table t2 (c1 int)")
		tk.MustExec("insert into t1 values (1), (2)")
		tk.MustExec("insert into t2 values (1), (1), (2)")

		tk.MustQuery("select count(1 in (select t2.c1 from t2)) over () from t1").Check(testkit.Rows("2", "2"))
		tk.MustQuery("select count(1 = any (select t2.c1 from t2)) over () from t1").Check(testkit.Rows("2", "2"))
	})
}

func TestWindowSubqueryOuterRef(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2, t3")
		defer tk.MustExec("drop table if exists t1, t2, t3")
		tk.MustExec("create table t1 (c1 int primary key)")
		tk.MustExec("create table t2 (c1 int, c2 text)")
		tk.MustExec("create table t3 (c1 int, c3 int)")
		tk.MustExec("insert into t1 values (10)")
		tk.MustExec("insert into t2 values (1, 'alpha'), (1, 'beta'), (2, 'gamma')")
		tk.MustExec("insert into t3 values (1, 100), (2, 200)")

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suiteData := getWindowPushDownSuiteData()
		suiteData.LoadTestCases(t, &input, &output, cascades, caller)
		for i, sql := range input {
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("EXPLAIN FORMAT='plan_tree' " + sql).Rows())
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("EXPLAIN FORMAT='plan_tree' " + sql).Check(testkit.Rows(output[i].Plan...))
			tk.MustQuery(sql).Check(testkit.Rows(output[i].Result...))
		}
	})
}
