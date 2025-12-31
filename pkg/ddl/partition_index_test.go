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

package ddl_test

import (
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestIssue65289ExchangePartitionGlobalIndex tests that after EXCHANGE PARTITION,
// creating a global index properly indexes all rows even when duplicate _tidb_rowid
// values exist across partitions.
// See: https://github.com/pingcap/tidb/issues/65289
func TestIssue65289ExchangePartitionGlobalIndexClustered(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Create non-partitioned table with nonclustered primary key
	tk.MustExec("drop table if exists t, tp")
	tk.MustExec("CREATE TABLE t (a INT, b INT, dt DATE, PRIMARY KEY (a))")
	//tk.MustExec("CREATE TABLE t (a INT, b INT, dt DATE, PRIMARY KEY (a) NONCLUSTERED)")

	// Create partitioned table with nonclustered primary key
	//tk.MustExec(`CREATE TABLE tp (a INT, b INT, dt DATE, PRIMARY KEY (a) NONCLUSTERED)
	tk.MustExec(`CREATE TABLE tp (a INT, b INT, dt DATE, PRIMARY KEY (a))
		PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (5),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (20)
		)`)

	// Insert data into partitioned table
	// p0 will have a=2,4
	// p1 will have a=6
	// p2 will be empty
	tk.MustExec("insert into tp (a,b) values (2,2),(4,4),(6,6)")

	// Insert data into non-partitioned table
	tk.MustExec("insert into t (a,b) values (12,2),(14,4),(16,6)")

	tk.MustQuery("select a, b from tp order by a").Check(testkit.Rows(""+
		"2 2",
		"4 4",
		"6 6"))

	tk.MustQuery("select a, b from t order by a").Check(testkit.Rows(""+
		"12 2",
		"14 4",
		"16 6"))

	// Exchange partition p2 with table t
	// After this, p2 will have a=12,14,16 with _tidb_rowid 1,2,3
	// which duplicates the _tidb_rowid values in p0 (1,2) and p1 (3)
	tk.MustExec("ALTER TABLE tp EXCHANGE PARTITION p2 WITH TABLE t")

	tk.MustQuery("select a, b from tp order by a").Check(testkit.Rows(""+
		"2 2",
		"4 4",
		"6 6",
		"12 2",
		"14 4",
		"16 6"))

	// Create global index on column b
	// This is where the bug occurs: without the fix, the index would only
	// contain 3 entries instead of 6 because rows with duplicate
	// (b, _tidb_rowid) pairs from different partitions would overwrite each other
	tk.MustExec("create index idx_b on tp(b) global")

	// Verify both queries return the same count
	// Bug symptom: use index returns 3 rows, ignore index returns 6 rows
	countWithIndex := tk.MustQuery("select count(*) from tp use index(idx_b)").Rows()[0][0]
	countWithoutIndex := tk.MustQuery("select count(*) from tp ignore index(idx_b)").Rows()[0][0]
	require.Equal(t, "6", countWithIndex, "Index should contain all 6 rows")
	require.Equal(t, "6", countWithoutIndex, "Table scan should find all 6 rows")
	require.Equal(t, countWithIndex, countWithoutIndex, "Index and table scan counts must match")

	// Verify all rows are accessible via the global index
	rowsWithIndex := tk.MustQuery("select a, b from tp use index(idx_b) order by a").Rows()
	rowsWithoutIndex := tk.MustQuery("select a, b from tp ignore index(idx_b) order by a").Rows()
	require.Equal(t, 6, len(rowsWithIndex), "Index scan should return all 6 rows")
	require.Equal(t, 6, len(rowsWithoutIndex), "Table scan should return all 6 rows")

	// Verify the data matches
	for i := 0; i < 6; i++ {
		require.Equal(t, rowsWithoutIndex[i][0], rowsWithIndex[i][0],
			"Row %d column a should match between index and table scan", i)
		require.Equal(t, rowsWithoutIndex[i][1], rowsWithIndex[i][1],
			"Row %d column b should match between index and table scan", i)
	}

	// Most importantly: admin check should pass
	// Bug symptom: "data inconsistency in table: tp, index: idx_b, handle: 3"
	tk.MustExec("admin check table tp")

	// Clean up
	tk.MustExec("drop table t, tp")
}

// TestIssue65289ExchangePartitionGlobalIndex tests that after EXCHANGE PARTITION,
// creating a global index properly indexes all rows even when duplicate _tidb_rowid
// values exist across partitions.
// See: https://github.com/pingcap/tidb/issues/65289
func TestIssue65289ExchangePartitionGlobalIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Create non-partitioned table with nonclustered primary key
	tk.MustExec("drop table if exists t, tp")
	tk.MustExec("CREATE TABLE t (a INT, b INT, dt DATE, PRIMARY KEY (a) NONCLUSTERED)")

	// Create partitioned table with nonclustered primary key
	tk.MustExec(`CREATE TABLE tp (a INT, b INT, dt DATE, PRIMARY KEY (a) NONCLUSTERED)
		PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (5),
			PARTITION p1 VALUES LESS THAN (11),
			PARTITION p2 VALUES LESS THAN (20)
		)`)

	// Insert data into partitioned table
	// p0 will have a=2,4 (with _tidb_rowid 1,2)
	// p1 will have a=6 (with _tidb_rowid 3)
	// p2 will be empty
	tk.MustExec("insert into tp (a,b) values (2,2),(4,4),(6,6)")

	// Insert data into non-partitioned table
	// t will have a=12,14,16 (with _tidb_rowid 1,2,3)
	tk.MustExec("insert into t (a,b) values (12,2),(14,4),(16,6)")

	// Verify initial data - check _tidb_rowid values
	rows := tk.MustQuery("select a, b, _tidb_rowid from tp order by a").Rows()
	require.Equal(t, 3, len(rows))
	require.Equal(t, "2", rows[0][0])
	require.Equal(t, "2", rows[0][1])
	require.Equal(t, "1", rows[0][2]) // _tidb_rowid = 1

	rows = tk.MustQuery("select a, b, _tidb_rowid from t order by a").Rows()
	require.Equal(t, 3, len(rows))
	require.Equal(t, "12", rows[0][0])
	require.Equal(t, "2", rows[0][1])
	require.Equal(t, "1", rows[0][2]) // _tidb_rowid = 1 (duplicate with tp!)

	// Exchange partition p2 with table t
	// After this, p2 will have a=12,14,16 with _tidb_rowid 1,2,3
	// which duplicates the _tidb_rowid values in p0 (1,2) and p1 (3)
	tk.MustExec("ALTER TABLE tp EXCHANGE PARTITION p2 WITH TABLE t")

	// Verify data after exchange - now we have duplicate _tidb_rowid values
	rows = tk.MustQuery("select a, b, _tidb_rowid from tp order by a").Rows()
	require.Equal(t, 6, len(rows))
	// p0: a=2 (rowid=1), a=4 (rowid=2)
	require.Equal(t, "2", rows[0][0])
	require.Equal(t, "1", rows[0][2])
	require.Equal(t, "4", rows[1][0])
	require.Equal(t, "2", rows[1][2])
	// p1: a=6 (rowid=3)
	require.Equal(t, "6", rows[2][0])
	require.Equal(t, "3", rows[2][2])
	// p2: a=12 (rowid=1), a=14 (rowid=2), a=16 (rowid=3) - all duplicates!
	require.Equal(t, "12", rows[3][0])
	require.Equal(t, "1", rows[3][2]) // Same as p0's first row!
	require.Equal(t, "14", rows[4][0])
	require.Equal(t, "2", rows[4][2]) // Same as p0's second row!
	require.Equal(t, "16", rows[5][0])
	require.Equal(t, "3", rows[5][2]) // Same as p1's row!

	// Create global index on column b
	// This is where the bug occurs: without the fix, the index would only
	// contain 3 entries instead of 6 because rows with duplicate
	// (b, _tidb_rowid) pairs from different partitions would overwrite each other
	tk.MustExec("create index idx_b on tp(b) global")

	tk.MustQuery("select * from tp where b = 2").Sort().Check(testkit.Rows("12 2 <nil>", "2 2 <nil>"))
	// Verify both queries return the same count
	// Bug symptom: use index returns 3 rows, ignore index returns 6 rows
	countWithIndex := tk.MustQuery("select count(*) from tp use index(idx_b)").Rows()[0][0]
	countWithoutIndex := tk.MustQuery("select count(*) from tp ignore index(idx_b)").Rows()[0][0]
	require.Equal(t, "6", countWithIndex, "Index should contain all 6 rows")
	require.Equal(t, "6", countWithoutIndex, "Table scan should find all 6 rows")
	require.Equal(t, countWithIndex, countWithoutIndex, "Index and table scan counts must match")

	// Verify all rows are accessible via the global index
	rowsWithIndex := tk.MustQuery("select a, b from tp use index(idx_b) order by a").Rows()
	rowsWithoutIndex := tk.MustQuery("select a, b from tp ignore index(idx_b) order by a").Rows()
	require.Equal(t, 6, len(rowsWithIndex), "Index scan should return all 6 rows")
	require.Equal(t, 6, len(rowsWithoutIndex), "Table scan should return all 6 rows")

	// Verify the data matches
	for i := 0; i < 6; i++ {
		require.Equal(t, rowsWithoutIndex[i][0], rowsWithIndex[i][0],
			"Row %d column a should match between index and table scan", i)
		require.Equal(t, rowsWithoutIndex[i][1], rowsWithIndex[i][1],
			"Row %d column b should match between index and table scan", i)
	}

	// Most importantly: admin check should pass
	// Bug symptom: "data inconsistency in table: tp, index: idx_b, handle: 3"
	tk.MustExec("admin check table tp")

	// Clean up
	tk.MustExec("drop table t, tp")
}

// TestGlobalIndexWithDuplicateHandles tests that global indexes correctly handle
// duplicate handles across partitions (the core encoding/decoding logic).
func TestGlobalIndexWithDuplicateHandles(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Setup: Create a partitioned table and force duplicate handles via exchange partition
	tk.MustExec("drop table if exists t, tp")
	tk.MustExec("CREATE TABLE tp (id INT, val INT, PRIMARY KEY (id) NONCLUSTERED) PARTITION BY RANGE (id) (PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN (200))")
	tk.MustExec("CREATE TABLE t (id INT, val INT, PRIMARY KEY (id) NONCLUSTERED)")

	// Insert data
	tk.MustExec("insert into tp values (10, 100), (20, 200)")
	tk.MustExec("insert into t values (150, 100), (160, 200)")

	// Exchange - now p1 will have _tidb_rowid values that might duplicate p0
	tk.MustExec("ALTER TABLE tp EXCHANGE PARTITION p1 WITH TABLE t")

	// Create global index - should handle duplicate handles correctly
	tk.MustExec("create index idx_val on tp(val) global")

	// Verify index works correctly
	result := tk.MustQuery("select count(*) from tp use index(idx_val) where val = 100").Rows()[0][0]
	require.Equal(t, "2", result, "Should find both rows with val=100")

	result = tk.MustQuery("select count(*) from tp use index(idx_val) where val = 200").Rows()[0][0]
	require.Equal(t, "2", result, "Should find both rows with val=200")

	// Admin check should pass
	tk.MustExec("admin check table tp")

	tk.MustExec("drop table t, tp")
}
