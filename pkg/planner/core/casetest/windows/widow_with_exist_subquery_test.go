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

func TestWindowWithOuterJoinAndCTE(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t0, t1")
		tk.MustExec(`CREATE TABLE t0 (
  id bigint NOT NULL,
  k0 int NOT NULL,
  k1 varchar(64) NOT NULL,
  k2 bigint NOT NULL,
  k3 int NOT NULL,
  p0 varchar(64) NOT NULL,
  p1 int NOT NULL,
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)
		tk.MustExec(`CREATE TABLE t1 (
  id bigint NOT NULL,
  k0 int NOT NULL,
  d0 decimal(12,2) NOT NULL,
  d1 float NOT NULL,
  PRIMARY KEY (id) /*T![clustered_index] CLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin`)

		tk.MustQuery(`WITH cte_0 AS (
    SELECT t0.k0 AS c0, t0.p0 AS c1, t0.p1 AS c2
    FROM t0
    WHERE (((t0.p0 < t0.k1) AND (t0.k0 = t0.k3)) AND NOT (t0.k3 IN (83)))
    ORDER BY t0.id
    LIMIT 3
)
SELECT /* issue:66904 */ t0.id AS c0,
       ROW_NUMBER() OVER w2 AS c1,
       AVG(t1.d0) OVER (ORDER BY t0.k0 DESC, cte_0.c2 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS c2
FROM (SELECT cte_0.c0 AS c0, cte_0.c1 AS c1, cte_0.c2 AS c2 FROM cte_0) AS cte_0
LEFT JOIN (SELECT t1.id AS id, t1.k0 AS k0, t1.d0 AS d0, t1.d1 AS d1 FROM t1) AS t1 ON (1 = 0)
RIGHT JOIN (SELECT t0.id AS id, t0.k0 AS k0, t0.k1 AS k1, t0.k2 AS k2, t0.k3 AS k3, t0.p0 AS p0, t0.p1 AS p1 FROM t0) AS t0
  ON ((t1.k0 = t0.k0) AND (t0.k3 <= t1.d1))
WHERE NOT (t1.k0 IN (73, 35, 61))
WINDOW w2 AS (
    PARTITION BY cte_0.c1, t1.d1
    ORDER BY t0.k0, t0.k1 DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
ORDER BY (cte_0.c2 + t0.id) DESC, (t0.k3 - t1.d1) DESC`)

		tk.MustQuery(`WITH cte_0 AS (
    SELECT t0.k0 AS c0, t0.p0 AS c1, t0.p1 AS c2
    FROM t0
    WHERE (((t0.p0 < t0.k1) AND (t0.k0 = t0.k3)) AND NOT (t0.k3 IN (83)))
    ORDER BY t0.id
    LIMIT 3
)
SELECT /* issue:66904 start-bound */ t0.id AS c0,
       ROW_NUMBER() OVER w2 AS c1,
       AVG(t1.d0) OVER (ORDER BY t0.k0 DESC, cte_0.c2 RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS c2
FROM (SELECT cte_0.c0 AS c0, cte_0.c1 AS c1, cte_0.c2 AS c2 FROM cte_0) AS cte_0
LEFT JOIN (SELECT t1.id AS id, t1.k0 AS k0, t1.d0 AS d0, t1.d1 AS d1 FROM t1) AS t1 ON (1 = 0)
RIGHT JOIN (SELECT t0.id AS id, t0.k0 AS k0, t0.k1 AS k1, t0.k2 AS k2, t0.k3 AS k3, t0.p0 AS p0, t0.p1 AS p1 FROM t0) AS t0
  ON ((t1.k0 = t0.k0) AND (t0.k3 <= t1.d1))
WHERE NOT (t1.k0 IN (73, 35, 61))
WINDOW w2 AS (
    PARTITION BY cte_0.c1, t1.d1
    ORDER BY t0.k0, t0.k1 DESC
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
)
ORDER BY (cte_0.c2 + t0.id) DESC, (t0.k3 - t1.d1) DESC`)
	})
}
