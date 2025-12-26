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

package rule

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/testdata"
)

// TestOuterToSemiJoin tests the optimizer rule that converts Outer Join to Semi Join.
// This rule is a crucial optimization that simplifies the query plan and improves performance.
//
// The core idea is to identify when an Outer Join (LEFT or RIGHT) behaves like an Inner Join
// or an Anti Join due to "null-rejecting" conditions in the WHERE clause.
//
// 1.  LEFT JOIN to SEMI JOIN:
//     When a WHERE clause condition rejects NULL values from the inner table (the right table
//     in a LEFT JOIN), it effectively filters out all rows where the join condition failed.
//     This makes the LEFT JOIN behave like an INNER JOIN. If the query only selects columns
//     from the outer table, this can be further simplified to a more efficient SEMI JOIN,
//     which only checks for the existence of matching rows without producing duplicates.
//     Example: `SELECT a.* FROM a LEFT JOIN b ON a.id=b.id WHERE b.val > 0`
//     can be converted to `SELECT a.* FROM a SEMI JOIN b ON a.id=b.id WHERE b.val > 0`.
//
// 2.  LEFT JOIN to ANTI SEMI JOIN:
//     When a WHERE clause condition specifically checks for `IS NULL` on a NOT NULL column
//     of the inner table, it means the query is looking for rows in the outer table that
//     have NO match in the inner table. This is the exact definition of an ANTI SEMI JOIN.
//     Example: `SELECT a.* FROM a LEFT JOIN b ON a.id=b.id WHERE b.id IS NULL`
//     can be converted to `SELECT a.* FROM a ANTI SEMI JOIN b ON a.id=b.id`.
//
// This test suite covers a wide range of scenarios, including equi-joins, non-equi-joins,
// null-safe joins (`<=>`), and various null-rejecting conditions to ensure the rule's
// correctness and robustness.
func TestOuterToSemiJoin(tt *testing.T) {
	testkit.RunTestUnderCascades(tt, func(t *testing.T, tk *testkit.TestKit, cascades, caller string) {
		tk.MustExec("use test")
		tk.MustExec("drop table if exists A, B")
		// A.id is not a primary key to allow NULL values for the test.
		tk.MustExec("CREATE TABLE A (id INT, val INT, nullable_val INT)")
		tk.MustExec("CREATE TABLE B (id INT PRIMARY KEY, a_id INT, val INT, non_null_col INT NOT NULL, nullable_col INT)")

		// Insert data into A
		// A.val=10, 20, NULL, 40
		tk.MustExec("INSERT INTO A VALUES (1, 10, 100), (2, 20, NULL), (3, NULL, 300), (4, 40, 400), (NULL, 50, 500)")

		// Insert data into B
		// B.val=10 matches A.val=10.
		// B.val=NULL matches A.val=NULL via <=>.
		// B.val=500 and 600 have no match in A.val.
		tk.MustExec("INSERT INTO B VALUES (101, 1, 10, 1, 1), (102, 2, NULL, 2, NULL), (103, 5, 500, 5, 5), (104, NULL, 600, 6, 6)")

		var input []string
		var output []struct {
			SQL    string
			Plan   []string
			Result []string
		}
		suite := GetOuterToSemiJoinSuiteData()
		suite.LoadTestCases(t, &input, &output, cascades, caller)
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
