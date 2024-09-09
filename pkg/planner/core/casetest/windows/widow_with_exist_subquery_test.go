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
)

func TestWindowWithCorrelatedSubQuery(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE temperature_data (temperature double);")
	tk.MustExec("CREATE TABLE humidity_data (humidity double);")
	tk.MustExec("CREATE TABLE weather_report (report_id double, report_date varchar(100));")

	tk.MustExec("INSERT INTO temperature_data VALUES (1.0);")
	tk.MustExec("INSERT INTO humidity_data VALUES (0.5);")
	tk.MustExec("INSERT INTO weather_report VALUES (2.0, 'test');")

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
}
