// Copyright 2025 PingCAP, Inc.
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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestSetVarTimestampHintsWorks(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)

		// test that the timestamp continues to update (default behavior)
		testKit.MustExec(`set timestamp=default;`)
		require.Equal(t, "42", testKit.MustQuery(`select /*+ set_var(timestamp=1) */ @@timestamp + 41;`).Rows()[0][0].(string))
		firstts := testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string)
		require.Eventually(t, func() bool {
			return firstts < testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string)
		}, time.Second, time.Microsecond*10)

		// test that the set value is preserved
		testKit.MustExec(`set timestamp=1745862208.446495;`)
		require.Equal(t, "1745862208.446495", testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
		require.Equal(t, "1", testKit.MustQuery(`select /*+ set_var(timestamp=1) */ @@timestamp;`).Rows()[0][0].(string))
		require.Equal(t, "1745862208.446495", testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
	})
}

func TestSetVarTimestampHintsWorksWithBindings(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
		testKit.MustExec(`create session binding for select @@timestamp + 41 using select /*+ set_var(timestamp=1) */ @@timestamp + 41;`)

		// test that bindings with hints correctly restore the default timestamp
		testKit.MustExec(`set timestamp=default;`)
		require.Equal(t, "42", testKit.MustQuery(`select @@timestamp + 41;`).Rows()[0][0].(string))
		testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
		firstts := testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string)
		require.Eventually(t, func() bool {
			return firstts < testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string)
		}, time.Second, time.Microsecond*10)

		// test that bindings with hints correctly restore the previous non-default timestamp value
		testKit.MustExec(`set timestamp=1745862208.446495;`)
		require.Equal(t, "1745862208.446495", testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
		require.Equal(t, "42", testKit.MustQuery(`select @@timestamp + 41;`).Rows()[0][0].(string))
		testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
		require.Equal(t, "1745862208.446495", testKit.MustQuery(`select @@timestamp;`).Rows()[0][0].(string))
	})
}

func TestSetVarInQueriesAndBindingsWorkTogether(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
		testKit.MustExec(`set @@max_execution_time=2000;`)
		testKit.MustExec(`create table foo (a int);`)
		testKit.MustExec(`create session binding for select * from foo where a = 1 using select /*+ set_var(max_execution_time=1234) */ * from foo where a = 1;`)
		testKit.MustExec(`select /*+ set_var(max_execution_time=2222) */ * from foo where a = 1;`)
		testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
		testKit.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))
	})
}

func TestSetVarHintsWithExplain(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)

		testKit.MustExec(`set @@max_execution_time=2000;`)
		testKit.MustExec(`explain select /*+ set_var(max_execution_time=100) */ @@max_execution_time;`)
		testKit.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))

		testKit.MustExec(`create table t(a int);`)
		testKit.MustExec(`create global binding for select * from t where a = 1 and sleep(0.1) using select /*+ SET_VAR(max_execution_time=500) */ * from t where a = 1 and sleep(0.1);`)
		testKit.MustExec(`select * from t where a = 1 and sleep(0.1);`)
		testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
		testKit.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))

		testKit.MustExec(`explain select * from t where a = 1 and sleep(0.1);`)
		testKit.MustQuery("select @@last_plan_from_binding").Check(testkit.Rows("1"))
		testKit.MustQuery("select @@max_execution_time;").Check(testkit.Rows("2000"))
	})
}

func TestWriteSlowLogHint(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec(`use test`)
		testKit.MustExec(`create table t(a int);`)
		testKit.MustExec(`select * from t where a = 1;`)

		core, recorded := observer.New(zap.WarnLevel)
		logger := zap.New(core)
		prev := logutil.SlowQueryLogger
		logutil.SlowQueryLogger = logger
		defer func() { logutil.SlowQueryLogger = prev }()

		sql := "select /*+ write_slow_log */ * from t where a = 1;"
		checkWriteSlowLog := func(expectWrite bool) {
			if !expectWrite {
				require.Equal(t, 0, recorded.Len())
			} else {
				require.NotEqual(t, 0, recorded.Len())
			}

			writeMsg := slices.ContainsFunc(recorded.All(), func(entry observer.LoggedEntry) bool {
				if entry.Level == zap.WarnLevel && strings.Contains(entry.Message, sql) {
					return true
				}
				return false
			})
			require.Equal(t, expectWrite, writeMsg)
		}

		testKit.MustExec(`select * from t where a = 1;`)
		checkWriteSlowLog(false)

		testKit.MustExec(sql)
		checkWriteSlowLog(true)
	})
}

func TestIndexHint(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("USE test")
		testKit.MustExec("DROP TABLE IF EXISTS t_alias")
		testKit.MustExec(`
       CREATE TABLE t_alias (
          a INT PRIMARY KEY,
          b INT,
          c INT,
          UNIQUE KEY idx_b (b),
          KEY idx_c (c)
       );`)
		testKit.MustExec("INSERT INTO t_alias VALUES (1, 1, 1), (2, 2, 20), (3, 3, 30), (4, 4, 400), (5, 5, 50);")
		query := "SELECT /*+ INDEX(t_alias, idx_b) */ * FROM t_alias WHERE c > 10;"
		rows := testKit.MustQuery(query).Rows()
		// Verify that the query's execution plan correctly uses the specified index.
		testKit.MustUseIndex(query, "idx_b") // Pass the complete query statement.
		// Verify the correctness of the results.
		require.Len(t, rows, 4, "The query should return 4 rows")
	})
}

// TestNoIndexHint tests the NO_INDEX hint, which is an alias for IGNORE_INDEX.
func TestNoIndexHint(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("USE test")
		testKit.MustExec("DROP TABLE IF EXISTS t_no_index")
		testKit.MustExec(`
		CREATE TABLE t_no_index (
			a INT,
			b INT,
			KEY idx_a (a),
			KEY idx_b (b)
		);`)
		testKit.MustExec("INSERT INTO t_no_index VALUES (1,1), (5,5), (10,10), (20,20);")

		// The optimizer would normally choose idx_a. The NO_INDEX hint should prevent this.
		query := "SELECT /*+ NO_INDEX(t_no_index, idx_a) */ * FROM t_no_index WHERE a > 5;"

		// Check that the plan does not use the ignored index 'idx_a' and falls back to a table scan.
		rows := testKit.MustQuery("EXPLAIN " + query).Rows()
		var planUsesIgnoredIndex bool
		for _, row := range rows {
			if strings.Contains(row[3].(string), "index:idx_a") {
				planUsesIgnoredIndex = true
				break
			}
		}
		require.False(t, planUsesIgnoredIndex, "The plan should not use the ignored index 'idx_a'")
		testKit.MustHavePlan(query, "TableFullScan")

		// Check that the query result is still correct.
		result := testKit.MustQuery(query)
		result.Check(testkit.Rows("10 10", "20 20"))
	})
}

func TestIndexCombineHint(t *testing.T) {
	testkit.RunTestUnderCascades(t, func(t *testing.T, testKit *testkit.TestKit, cascades, caller string) {
		testKit.MustExec("USE test")
		testKit.MustExec("DROP TABLE IF EXISTS t_index_combine")
		testKit.MustExec(`
		CREATE TABLE t_index_combine (
			a INT,
			b INT,
			KEY idx_a (a),
			KEY idx_b (b)
		);`)

		// Insert more data to make index merge a viable option for the optimizer.
		testKit.MustExec("INSERT INTO t_index_combine VALUES (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (10, 10), (15, 15), (20, 20), (25, 25), (30, 30), (35, 35), (40, 40), (45, 45), (50, 50);")
		testKit.MustExec("ANALYZE TABLE t_index_combine;")
		query := "SELECT /*+ INDEX_COMBINE(t_index_combine, idx_a, idx_b) */ * FROM t_index_combine WHERE a < 5 OR b > 40;"
		testKit.MustHavePlan(query, "IndexMerge")

		// Verify the correctness of the query results.
		result := testKit.MustQuery(query)
		result.Sort().Check(testkit.Rows(
			"1 1",
			"2 2",
			"3 3",
			"4 4",
			"45 45",
			"50 50",
		))
	})
}
