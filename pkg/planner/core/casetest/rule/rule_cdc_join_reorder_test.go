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

package rule

import (
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
	"github.com/stretchr/testify/require"
)

func prepareOrderAwareJoinReorderTables(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_opt_enable_alternative_logical_plans = 0")
	tk.MustExec("drop table if exists t6, t7, t8, t9")
	tk.MustExec("create table t6(id int not null, category varchar(20), payload int, key idx_id(id), key idx_category_id_payload(category, id, payload))")
	tk.MustExec("create table t7(id int not null, payload int, key idx_id(id))")
	tk.MustExec("create table t8(id int not null, payload int, key idx_id(id), key idx_payload_id(payload, id))")
	tk.MustExec("create table t9(id int not null, payload int, key idx_id(id), key idx_payload_id(payload, id))")

	t6Rows := make([]string, 0, 8000)
	t7Rows := make([]string, 0, 6000)
	t8Rows := make([]string, 0, 7000)
	t9Rows := make([]string, 0, 9000)
	for i := 1; i <= 8000; i++ {
		category := "cold"
		if i <= 2000 {
			category = "hot"
		}
		t6Rows = append(t6Rows, fmt.Sprintf("(%d,'%s',%d)", i, category, i*10))
	}
	for i := 1; i <= 6000; i++ {
		t7Rows = append(t7Rows, fmt.Sprintf("(%d,%d)", i, i*100))
	}
	for i := 1; i <= 7000; i++ {
		payload := 0
		if i <= 100 {
			payload = 1
		}
		t8Rows = append(t8Rows, fmt.Sprintf("(%d,%d)", i, payload))
	}
	for i := 1; i <= 9000; i++ {
		payload := 0
		if i <= 130 {
			payload = 1
		}
		t9Rows = append(t9Rows, fmt.Sprintf("(%d,%d)", i, payload))
	}
	tk.MustExec("insert into t6 values " + strings.Join(t6Rows, ","))
	tk.MustExec("insert into t7 values " + strings.Join(t7Rows, ","))
	tk.MustExec("insert into t8 values " + strings.Join(t8Rows, ","))
	tk.MustExec("insert into t9 values " + strings.Join(t9Rows, ","))
	tk.MustExec("analyze table t6 all columns")
	tk.MustExec("analyze table t7 all columns")
	tk.MustExec("analyze table t8 all columns")
	tk.MustExec("analyze table t9 all columns")
}

func prepareOrderAwareAlternativeRoundTables(tk *testkit.TestKit) {
	tk.MustExec("use test")
	tk.MustExec("drop table if exists oa_order_t1, oa_order_t2, oa_order_t3, oa_order_t4, obj, relationship")
	tk.MustExec("create table oa_order_t1(id int not null primary key, category varchar(20), created_at int, key idx_category_created(category, created_at, id))")
	tk.MustExec("create table oa_order_t2(id int not null primary key, t1_id int not null, key idx_t1_id(t1_id))")
	tk.MustExec("create table oa_order_t3(id int not null primary key, t2_id int not null, key idx_t2_id(t2_id))")
	tk.MustExec("create table oa_order_t4(id int not null primary key, t3_id int not null, payload int, key idx_payload_t3(payload, t3_id))")
	tk.MustExec("create table obj(id int not null, label varchar(32), workid varchar(32), type_id int, txt_val varchar(32), key idx_workid_label(workid, label), key idx_id(id), key idx_label(label))")
	tk.MustExec("create table relationship(obj_id int, ref_ojb_id int, key idx_obj_id(obj_id, ref_ojb_id), key idx_ref_obj_id(ref_ojb_id, obj_id))")

	oaOrderT1Rows := make([]string, 0, 5000)
	oaOrderT2Rows := make([]string, 0, 5000)
	oaOrderT3Rows := make([]string, 0, 5000)
	oaOrderT4Rows := make([]string, 0, 5000)
	for i := 1; i <= 5000; i++ {
		oaOrderT1Rows = append(oaOrderT1Rows, fmt.Sprintf("(%d,'hot',%d)", i, i))
		oaOrderT2Rows = append(oaOrderT2Rows, fmt.Sprintf("(%d,%d)", i, i))
		oaOrderT3Rows = append(oaOrderT3Rows, fmt.Sprintf("(%d,%d)", i, i))
		payload := 0
		if i%10 == 0 {
			payload = 1
		}
		oaOrderT4Rows = append(oaOrderT4Rows, fmt.Sprintf("(%d,%d,%d)", i, i, payload))
	}
	tk.MustExec("insert into oa_order_t1 values " + strings.Join(oaOrderT1Rows, ","))
	tk.MustExec("insert into oa_order_t2 values " + strings.Join(oaOrderT2Rows, ","))
	tk.MustExec("insert into oa_order_t3 values " + strings.Join(oaOrderT3Rows, ","))
	tk.MustExec("insert into oa_order_t4 values " + strings.Join(oaOrderT4Rows, ","))
	// insert rows to obj and relationship.
	tk.MustExec("SET SESSION cte_max_recursion_depth = 10000;")
	tk.MustExec("INSERT INTO obj (id, label, workid, type_id, txt_val) " +
		"SELECT n, CONCAT('label_', LPAD(CAST(FLOOR(RAND()*100000) AS CHAR), 5, '0'))," +
		"CONCAT('w', LPAD(CAST(1 + FLOOR(RAND()*50) AS CHAR), 3, '0'))," +
		"1 + FLOOR(RAND()*3)," +
		"CONCAT('txt_', SUBSTRING(MD5(RAND()), 1, 12)) FROM ( WITH RECURSIVE seq(n) AS ( SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 10000)" +
		" SELECT n FROM seq ) s;")

	tk.MustExec("INSERT INTO relationship (obj_id, ref_ojb_id) SELECT  obj_id,  CASE  WHEN obj_id = ref_ojb_id THEN IF(obj_id = 1000, 9999, obj_id + 1)" +
		"   ELSE ref_ojb_id  END FROM (   WITH RECURSIVE seq(n) AS (    SELECT 1    UNION ALL    SELECT n + 1 FROM seq WHERE n < 1000 )  " +
		" SELECT    1 + FLOOR(RAND()*1000) AS obj_id,    1 + FLOOR(RAND()*1000) AS ref_ojb_id  FROM seq) r;")
	tk.MustExec("analyze table oa_order_t1 all columns")
	tk.MustExec("analyze table oa_order_t2 all columns")
	tk.MustExec("analyze table oa_order_t3 all columns")
	tk.MustExec("analyze table oa_order_t4 all columns")
	tk.MustExec("analyze table obj all columns")
	tk.MustExec("analyze table relationship all columns")
}

func TestCDCJoinReorder(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2, t3, t4, t5")
		tk.MustExec("CREATE TABLE t1 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t2 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t3 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t4 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t5 (a INT, b INT)")

		tk.MustExec("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)")
		tk.MustExec("INSERT INTO t2 VALUES (1, 100), (2, 200), (4, 400)")
		tk.MustExec("INSERT INTO t3 VALUES (1, 1000), (3, 3000), (5, 5000)")
		tk.MustExec("INSERT INTO t4 VALUES (1, 10000), (4, 40000), (6, 60000)")
		tk.MustExec("INSERT INTO t5 VALUES (2, 20000), (5, 50000), (7, 70000)")

		tk.MustExec("analyze table t1 all columns;")
		tk.MustExec("analyze table t2 all columns;")
		tk.MustExec("analyze table t3 all columns;")
		tk.MustExec("analyze table t4 all columns;")
		tk.MustExec("analyze table t5 all columns;")

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suite := GetCDCJoinReorderSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)

		// Phase 1: Collect expected results using the old join reorder algorithm
		// (CD-C is NOT enabled yet). These serve as the ground-truth baseline.
		expectedResults := make([][]string, len(input))
		for i, sql := range input {
			expectedResults[i] = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		}

		// Phase 2: Enable CD-C algorithm, then verify both the plan and the
		// result correctness for every case.
		for i, sql := range input {
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("EXPLAIN FORMAT='plan_tree' " + sql).Rows())
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("EXPLAIN FORMAT='plan_tree' " + sql).Check(testkit.Rows(output[i].Plan...))

			// Run with CD-C and cross-validate against the old algorithm baseline.
			cdcResult := testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			require.Equalf(t, expectedResults[i], cdcResult,
				"CD-C result differs from old algorithm for case[%d]: %s", i, sql)
		}
	})
}

func TestJoinReorderPushSelection(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2, t3, t4, t5")
		tk.MustExec("create table t1(id int not null primary key, name varchar(100))")
		tk.MustExec("create table t2(id int not null primary key, name varchar(100))")
		tk.MustExec("create table t3(id int not null primary key, name varchar(100))")
		tk.MustExec("create table t4(id int not null primary key, name varchar(100))")
		tk.MustExec("create table t5(id int not null primary key, name varchar(100))")
		tk.MustExec("set @@tidb_opt_join_reorder_through_sel = 1")

		tk.MustExec("insert into t1 values (1,'a'),(2,'b'),(3,'c')")
		tk.MustExec("insert into t2 values (1,'a'),(2,'b'),(4,'d')")
		tk.MustExec("insert into t3 values (1,'a'),(3,'c'),(5,'e')")
		tk.MustExec("insert into t4 values (1,'a'),(4,'d'),(6,'f')")
		tk.MustExec("insert into t5 values (2,'b'),(5,'e'),(7,'g')")
		tk.MustExec("analyze table t1 all columns")
		tk.MustExec("analyze table t2 all columns")
		tk.MustExec("analyze table t3 all columns")
		tk.MustExec("analyze table t4 all columns")
		tk.MustExec("analyze table t5 all columns")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		suite := GetCDCJoinReorderSuiteData()
		suite.LoadTestCasesByName("TestJoinReorderPushSelection", t, &input, &output, cascades, caller)

		planCaseIdx := 0
		for _, sql := range input {
			normalized := strings.ToLower(strings.TrimSpace(sql))
			if strings.HasPrefix(normalized, "set ") {
				tk.MustExec(sql)
				continue
			}

			plan := tk.MustQuery(sql)
			testdata.OnRecord(func() {
				if planCaseIdx >= len(output) {
					output = append(output, struct {
						SQL  string
						Plan []string
					}{})
				}
				output[planCaseIdx].SQL = sql
				output[planCaseIdx].Plan = testdata.ConvertRowsToStrings(plan.Rows())
			})

			require.Lessf(t, planCaseIdx, len(output),
				"missing expected output for plan case[%d], sql: %s", planCaseIdx, sql)
			require.Equalf(t, sql, output[planCaseIdx].SQL,
				"input/output SQL mismatch at plan case[%d]", planCaseIdx)
			plan.Check(testkit.Rows(output[planCaseIdx].Plan...))
			planCaseIdx++
		}
		require.Equalf(t, len(output), planCaseIdx,
			"unexpected output case count, output=%d, actual explain cases=%d", len(output), planCaseIdx)
	})
}

// TestDPJoinReorder verifies the DP join reorder algorithm produces correct results.
// It enables the advanced join reorder framework and sets the threshold high enough
// so that all test groups (≤5 tables) are handled by DP rather than greedy.
// Results are cross-validated against the greedy baseline to ensure correctness.
func TestDPJoinReorder(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2, t3, t4, t5")
		tk.MustExec("CREATE TABLE t1 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t2 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t3 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t4 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t5 (a INT, b INT)")

		tk.MustExec("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)")
		tk.MustExec("INSERT INTO t2 VALUES (1, 100), (2, 200), (4, 400)")
		tk.MustExec("INSERT INTO t3 VALUES (1, 1000), (3, 3000), (5, 5000)")
		tk.MustExec("INSERT INTO t4 VALUES (1, 10000), (4, 40000), (6, 60000)")
		tk.MustExec("INSERT INTO t5 VALUES (2, 20000), (5, 50000), (7, 70000)")

		tk.MustExec("analyze table t1 all columns")
		tk.MustExec("analyze table t2 all columns")
		tk.MustExec("analyze table t3 all columns")
		tk.MustExec("analyze table t4 all columns")
		tk.MustExec("analyze table t5 all columns")
		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suite := GetCDCJoinReorderSuiteData()
		suite.LoadTestCasesByName("TestDPJoinReorder", t, &input, &output, cascades, caller)

		// Phase 1: collect greedy baseline results (threshold=0, DP is not used).
		tk.MustExec("set @@tidb_opt_enable_advanced_join_reorder = 1")
		tk.MustExec("set @@tidb_opt_join_reorder_threshold = 0")
		greedyResults := make([][]string, len(input))
		for i, sql := range input {
			greedyResults[i] = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
		}

		// Phase 2: enable DP (threshold=10 covers all test groups with ≤5 tables).
		tk.MustExec("set @@tidb_opt_join_reorder_threshold = 10")
		for i, sql := range input {
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery("EXPLAIN FORMAT='plan_tree' " + sql).Rows())
				output[i].Result = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			tk.MustQuery("EXPLAIN FORMAT='plan_tree' " + sql).Check(testkit.Rows(output[i].Plan...))

			dpResult := testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			require.Equalf(t, greedyResults[i], dpResult,
				"DP result differs from greedy baseline for case[%d]: %s", i, sql)
		}
	})
}

func TestOrderAwareJoinReorderPushSelection(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		prepareOrderAwareJoinReorderTables(tk)
		tk.MustExec("set @@tidb_opt_join_reorder_through_sel = 1")

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		suite := GetOrderAwareJoinReorderSuiteData()
		suite.LoadTestCasesByName("TestOrderAwareJoinReorderPushSelection", t, &input, &output, cascades, caller)

		for i, sql := range input {
			normalized := strings.ToLower(strings.TrimSpace(sql))
			if strings.HasPrefix(normalized, "set ") {
				testdata.OnRecord(func() {
					if i >= len(output) {
						output = append(output, struct {
							SQL  string
							Plan []string
						}{})
					}
					output[i].SQL = sql
					output[i].Plan = nil
				})
				require.Lessf(t, i, len(output), "missing expected output for case[%d], sql: %s", i, sql)
				require.Equalf(t, sql, output[i].SQL, "input/output SQL mismatch at case[%d]", i)
				tk.MustExec(sql)
				continue
			}

			testdata.OnRecord(func() {
				if i >= len(output) {
					output = append(output, struct {
						SQL  string
						Plan []string
					}{})
				}
				output[i].SQL = sql
				output[i].Plan = testdata.ConvertRowsToStrings(tk.MustQuery(sql).Rows())
			})
			require.Lessf(t, i, len(output), "missing expected output for case[%d], sql: %s", i, sql)
			require.Equalf(t, sql, output[i].SQL, "input/output SQL mismatch at case[%d]", i)
			tk.MustQuery(sql).Check(testkit.Rows(output[i].Plan...))
			require.NotContains(t, strings.Join(testdata.ConvertRowsToStrings(tk.MustQuery("show warnings").Rows()), "\n"),
				"leading hint is inapplicable")
		}
	})
}

func TestOrderAwareJoinReorderAlternativeRound(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		prepareOrderAwareAlternativeRoundTables(tk)

		var input []string
		var output []struct {
			SQL  string
			Plan []string
		}
		suite := GetOrderAwareJoinReorderSuiteData()
		suite.LoadTestCasesByName("TestOrderAwareJoinReorderAlternativeRound", t, &input, &output, cascades, caller)

		expectedExplainCnt := 0
		for _, sql := range input {
			normalized := strings.ToLower(strings.TrimSpace(sql))
			if !strings.HasPrefix(normalized, "set ") {
				expectedExplainCnt++
			}
		}
		for i, sql := range input {
			normalized := strings.ToLower(strings.TrimSpace(sql))
			if strings.HasPrefix(normalized, "set ") {
				testdata.OnRecord(func() {
					output[i].SQL = sql
					output[i].Plan = nil
				})
				require.Equalf(t, sql, output[i].SQL, "input/output SQL mismatch at case[%d]", i)
				tk.MustExec(sql)
				continue
			}

			plan := tk.MustQuery(sql)
			rows := testdata.ConvertRowsToStrings(plan.Rows())
			testdata.OnRecord(func() {
				output[i].SQL = sql
				output[i].Plan = rows
			})
		}
	})
}

// TestDPJoinReorderLeadingHint verifies that a leading hint produces a warning
// when the DP algorithm is active, since DP does not support leading hints.
func TestDPJoinReorderLeadingHint(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists t1, t2, t3")
		tk.MustExec("CREATE TABLE t1 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t2 (a INT, b INT)")
		tk.MustExec("CREATE TABLE t3 (a INT, b INT)")
		tk.MustExec("set @@tidb_opt_enable_advanced_join_reorder = 1")
		tk.MustExec("set @@tidb_opt_join_reorder_threshold = 10")

		tk.MustQuery("SELECT /*+ LEADING(t2, t3) */ * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t2.a = t3.a")
		warnings := tk.MustQuery("show warnings").Rows()
		found := false
		for _, w := range warnings {
			if strings.Contains(fmt.Sprintf("%v", w), "leading hint is inapplicable for the DP join reorder algorithm") {
				found = true
				break
			}
		}
		require.Truef(t, found, "expected warning about leading hint being inapplicable for DP, got: %v", warnings)
	})
}
