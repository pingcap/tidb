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

func TestWindowWithCorrelatedSubQuery(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("use test")
		testKit.MustExec("CREATE TABLE temperature_data (temperature double);")
		testKit.MustExec("CREATE TABLE humidity_data (humidity double);")
		testKit.MustExec("CREATE TABLE weather_report (report_id double, report_date varchar(100));")

		testKit.MustExec("INSERT INTO temperature_data VALUES (1.0);")
		testKit.MustExec("INSERT INTO humidity_data VALUES (0.5);")
		testKit.MustExec("INSERT INTO weather_report VALUES (2.0, 'test');")

		result := testKit.MustQuery(`
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

		result = testKit.MustQuery(`
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

	})
}

func TestWindowSubqueryOuterRef(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2")
		tk.MustExec("create table t1 (c1 int primary key)")
		tk.MustExec("create table t2 (c1 int, c2 text)")
		tk.MustExec("insert into t1 values (10)")
		tk.MustExec("insert into t2 values (1, 'alpha'), (1, 'beta'), (2, 'gamma')")

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
