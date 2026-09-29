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
)

// TestInPredicateWithCollation tests that IN predicate optimization correctly handles collation.
// This is a regression test for issue #71700.
func TestInPredicateWithCollation(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")

	// Test case 1: Basic IN + NE with PAD SPACE collation
	tk.MustExec("CREATE TABLE t1(x TEXT)")
	tk.MustExec("INSERT INTO t1 VALUES ('')")

	// x = '' (empty string)
	// x IN (' ', 'a') should match because ' ' = '' with PAD SPACE
	// x <> '' should not match
	// Result: should return 0 rows
	result := tk.MustQuery("SELECT COUNT(*) FROM t1 WHERE x IN (' ', 'a') AND x <> ''")
	result.Check(testkit.Rows("0"))

	// Verify individual conditions
	result = tk.MustQuery("SELECT x IN (' ', 'a') AS in_result, x <> '' AS ne_result FROM t1")
	result.Check(testkit.Rows("1 0"))

	tk.MustExec("DROP TABLE t1")

	// Test case 2: VARCHAR column
	tk.MustExec("CREATE TABLE t2(x VARCHAR(10))")
	tk.MustExec("INSERT INTO t2 VALUES ('')")

	result = tk.MustQuery("SELECT COUNT(*) FROM t2 WHERE x IN (' ', 'a') AND x <> ''")
	result.Check(testkit.Rows("0"))

	tk.MustExec("DROP TABLE t2")

	// Test case 3: Multiple values
	tk.MustExec("CREATE TABLE t3(x TEXT)")
	tk.MustExec("INSERT INTO t3 VALUES (''), ('a'), ('b')")

	// Should return 2 rows (a and b, not empty string)
	result = tk.MustQuery("SELECT COUNT(*) FROM t3 WHERE x IN (' ', 'a', 'b') AND x <> ''")
	result.Check(testkit.Rows("2"))

	tk.MustExec("DROP TABLE t3")

	// Test case 4: CHAR column (padded with spaces)
	tk.MustExec("CREATE TABLE t4(x CHAR(10))")
	tk.MustExec("INSERT INTO t4 VALUES ('')")

	result = tk.MustQuery("SELECT COUNT(*) FROM t4 WHERE x IN (' ', 'a') AND x <> ''")
	result.Check(testkit.Rows("0"))

	tk.MustExec("DROP TABLE t4")

	// Test case 5: Constant propagation with IN and string collation
	tk.MustExec("CREATE TABLE t5(a TEXT, b TEXT)")
	tk.MustExec("INSERT INTO t5 VALUES ('x', 'x')")
	tk.MustExec("INSERT INTO t5 VALUES ('y', 'z')")

	// a = b is true for first row
	// a IN (b, 'other') should also be true
	result = tk.MustQuery("SELECT COUNT(*) FROM t5 WHERE a = b AND a IN (b, 'other')")
	result.Check(testkit.Rows("1"))

	tk.MustExec("DROP TABLE t5")
}

// TestInPredicateWithNO PAD tests IN predicate with NO PAD collation (utf8mb4_0900_ai_ci).
func TestInPredicateWithNOPAD(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)

	tk.MustExec("use test")

	// Test with NO PAD collation (utf8mb4_0900_ai_ci)
	tk.MustExec("CREATE TABLE t1(x TEXT COLLATE utf8mb4_0900_ai_ci)")
	tk.MustExec("INSERT INTO t1 VALUES ('')")

	// With NO PAD collation, ' ' != '' (trailing spaces are significant)
	// So x IN (' ', 'a') should not match empty string
	// Result: should return 0 rows
	result := tk.MustQuery("SELECT COUNT(*) FROM t1 WHERE x IN (' ', 'a') AND x <> ''")
	result.Check(testkit.Rows("0"))

	// Verify that ' ' and '' are different with NO PAD
	result = tk.MustQuery("SELECT ' ' = ''")
	result.Check(testkit.Rows("0"))

	tk.MustExec("DROP TABLE t1")
}
