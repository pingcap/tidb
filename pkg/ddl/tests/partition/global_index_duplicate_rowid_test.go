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

package partition

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// TestGlobalIndexDuplicateRowIDDetection tests that creating a non-unique global index
// on a non-clustered partitioned table with duplicate _tidb_rowid values (from EXCHANGE PARTITION)
// correctly detects the collision and returns an error.
// This is a regression test for issue #65289.
func TestGlobalIndexDuplicateRowIDDetection(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_exchange_partition=1")

	// Test with tidb_ddl_enable_fast_reorg OFF (temp index path)
	t.Run("FastReorgOFF", func(t *testing.T) {
		tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg=0")
		testGlobalIndexDuplicateRowID(t, tk)
	})

	// Test with tidb_ddl_enable_fast_reorg ON (direct backfill path)
	t.Run("FastReorgON", func(t *testing.T) {
		tk.MustExec("set @@global.tidb_ddl_enable_fast_reorg=1")
		testGlobalIndexDuplicateRowID(t, tk)
	})
}

func testGlobalIndexDuplicateRowID(t *testing.T, tk *testkit.TestKit) {
	tk.MustExec("drop table if exists t_part, t_normal")

	// Create a non-clustered partitioned table (no explicit PK, so uses _tidb_rowid)
	tk.MustExec(`CREATE TABLE t_part (
		a INT,
		b VARCHAR(50)
	) PARTITION BY RANGE (a) (
		PARTITION p0 VALUES LESS THAN (100),
		PARTITION p1 VALUES LESS THAN (200)
	)`)

	// Insert data into partitions
	tk.MustExec("INSERT INTO t_part VALUES (10, 'p0_row1'), (20, 'p0_row2'), (30, 'p0_row3')")
	tk.MustExec("INSERT INTO t_part VALUES (110, 'p1_row1'), (120, 'p1_row2')")

	// Create a normal non-partitioned table to exchange with
	tk.MustExec(`CREATE TABLE t_normal (
		a INT,
		b VARCHAR(50)
	)`)

	// Insert data with values that will go into p0 after exchange
	// These rows will get _tidb_rowid values that may conflict with existing p0 rows
	tk.MustExec("INSERT INTO t_normal VALUES (40, 'new_row1'), (50, 'new_row2')")

	// Perform EXCHANGE PARTITION
	// This moves t_normal data into p0 without regenerating _tidb_rowid
	// Now p0 and p1 may have overlapping _tidb_rowid values
	tk.MustExec("ALTER TABLE t_part EXCHANGE PARTITION p0 WITH TABLE t_normal")

	// Verify the exchange worked
	tk.MustQuery("SELECT COUNT(*) FROM t_part").Check(testkit.Rows("5"))

	// Attempt to create a non-unique global index
	// This should detect the duplicate _tidb_rowid collision and fail
	err := tk.ExecToErr("CREATE INDEX idx_global ON t_part(b) GLOBAL")
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate _tidb_rowid values detected")
	require.Contains(t, err.Error(), "EXCHANGE PARTITION")

	// Verify the error code is ErrUnsupportedDDLOperation (which we use for this error)
	require.Equal(t, errno.ErrUnsupportedDDLOperation, int(err.(*terror.Error).Code()))

	// Cleanup
	tk.MustExec("drop table if exists t_part, t_normal")
}

// TestGlobalIndexNoDuplicateRowID verifies that creating global indexes works
// correctly when there are no duplicate _tidb_rowid values (no false positives).
func TestGlobalIndexNoDuplicateRowID(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	// Test with both fast reorg settings
	for _, fastReorg := range []int{0, 1} {
		t.Run(fmt.Sprintf("FastReorg%d", fastReorg), func(t *testing.T) {
			tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_enable_fast_reorg=%d", fastReorg))
			tk.MustExec("drop table if exists t_part")

			// Create a non-clustered partitioned table
			tk.MustExec(`CREATE TABLE t_part (
				a INT,
				b VARCHAR(50)
			) PARTITION BY RANGE (a) (
				PARTITION p0 VALUES LESS THAN (100),
				PARTITION p1 VALUES LESS THAN (200)
			)`)

			// Insert data normally (no EXCHANGE PARTITION, so no duplicate _tidb_rowid)
			tk.MustExec("INSERT INTO t_part VALUES (10, 'p0_row1'), (20, 'p0_row2')")
			tk.MustExec("INSERT INTO t_part VALUES (110, 'p1_row1'), (120, 'p1_row2')")

			// Creating a non-unique global index should succeed
			tk.MustExec("CREATE INDEX idx_global ON t_part(b) GLOBAL")

			// Verify the index was created
			tk.MustQuery("SELECT COUNT(*) FROM t_part USE INDEX(idx_global)").Check(testkit.Rows("4"))

			// Cleanup
			tk.MustExec("drop table if exists t_part")
		})
	}
}

// TestGlobalIndexClusteredTable verifies that clustered tables are not affected
// (they don't use _tidb_rowid, so no collision can occur).
func TestGlobalIndexClusteredTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_exchange_partition=1")

	for _, fastReorg := range []int{0, 1} {
		t.Run(fmt.Sprintf("FastReorg%d", fastReorg), func(t *testing.T) {
			tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_enable_fast_reorg=%d", fastReorg))
			tk.MustExec("drop table if exists t_part, t_normal")

			// Create a CLUSTERED partitioned table (uses PK as handle, not _tidb_rowid)
			tk.MustExec(`CREATE TABLE t_part (
				id INT PRIMARY KEY,
				b VARCHAR(50)
			) PARTITION BY RANGE (id) (
				PARTITION p0 VALUES LESS THAN (100),
				PARTITION p1 VALUES LESS THAN (200)
			)`)

			tk.MustExec("INSERT INTO t_part VALUES (10, 'p0_row1'), (20, 'p0_row2')")
			tk.MustExec("INSERT INTO t_part VALUES (110, 'p1_row1'), (120, 'p1_row2')")

			// Create normal table for exchange
			tk.MustExec(`CREATE TABLE t_normal (
				id INT PRIMARY KEY,
				b VARCHAR(50)
			)`)
			tk.MustExec("INSERT INTO t_normal VALUES (40, 'new_row1'), (50, 'new_row2')")

			// Exchange partition
			tk.MustExec("ALTER TABLE t_part EXCHANGE PARTITION p0 WITH TABLE t_normal")

			// Creating global index should succeed even after exchange
			// because clustered table uses PK handle, not _tidb_rowid
			tk.MustExec("CREATE INDEX idx_global ON t_part(b) GLOBAL")

			// Verify the index works
			tk.MustQuery("SELECT COUNT(*) FROM t_part USE INDEX(idx_global)").Check(testkit.Rows("4"))

			tk.MustExec("drop table if exists t_part, t_normal")
		})
	}
}

// TestGlobalIndexUniqueIndex verifies that unique global indexes still work
// (they already have duplicate checking logic).
func TestGlobalIndexUniqueIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_exchange_partition=1")

	for _, fastReorg := range []int{0, 1} {
		t.Run(fmt.Sprintf("FastReorg%d", fastReorg), func(t *testing.T) {
			tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_enable_fast_reorg=%d", fastReorg))
			tk.MustExec("drop table if exists t_part, t_normal")

			// Create a non-clustered partitioned table
			tk.MustExec(`CREATE TABLE t_part (
				a INT,
				b VARCHAR(50)
			) PARTITION BY RANGE (a) (
				PARTITION p0 VALUES LESS THAN (100),
				PARTITION p1 VALUES LESS THAN (200)
			)`)

			tk.MustExec("INSERT INTO t_part VALUES (10, 'unique1'), (20, 'unique2')")
			tk.MustExec("INSERT INTO t_part VALUES (110, 'unique3'), (120, 'unique4')")

			// Create normal table
			tk.MustExec(`CREATE TABLE t_normal (
				a INT,
				b VARCHAR(50)
			)`)
			tk.MustExec("INSERT INTO t_normal VALUES (40, 'unique5'), (50, 'unique6')")

			// Exchange partition
			tk.MustExec("ALTER TABLE t_part EXCHANGE PARTITION p0 WITH TABLE t_normal")

			// Creating a UNIQUE global index should succeed
			// (unique indexes have their own collision detection)
			tk.MustExec("CREATE UNIQUE INDEX idx_global_unique ON t_part(b) GLOBAL")

			// Verify the index works
			tk.MustQuery("SELECT COUNT(*) FROM t_part USE INDEX(idx_global_unique)").Check(testkit.Rows("4"))

			tk.MustExec("drop table if exists t_part, t_normal")
		})
	}
}

// TestGlobalIndexLocalIndex verifies that local (non-global) indexes are not affected.
func TestGlobalIndexLocalIndex(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_exchange_partition=1")

	for _, fastReorg := range []int{0, 1} {
		t.Run(fmt.Sprintf("FastReorg%d", fastReorg), func(t *testing.T) {
			tk.MustExec(fmt.Sprintf("set @@global.tidb_ddl_enable_fast_reorg=%d", fastReorg))
			tk.MustExec("drop table if exists t_part, t_normal")

			// Create a non-clustered partitioned table
			tk.MustExec(`CREATE TABLE t_part (
				a INT,
				b VARCHAR(50)
			) PARTITION BY RANGE (a) (
				PARTITION p0 VALUES LESS THAN (100),
				PARTITION p1 VALUES LESS THAN (200)
			)`)

			tk.MustExec("INSERT INTO t_part VALUES (10, 'p0_row1'), (20, 'p0_row2')")
			tk.MustExec("INSERT INTO t_part VALUES (110, 'p1_row1'), (120, 'p1_row2')")

			// Create normal table
			tk.MustExec(`CREATE TABLE t_normal (
				a INT,
				b VARCHAR(50)
			)`)
			tk.MustExec("INSERT INTO t_normal VALUES (40, 'new_row1'), (50, 'new_row2')")

			// Exchange partition
			tk.MustExec("ALTER TABLE t_part EXCHANGE PARTITION p0 WITH TABLE t_normal")

			// Creating a LOCAL index should succeed
			// (local indexes are partition-scoped, so no cross-partition collision)
			tk.MustExec("CREATE INDEX idx_local ON t_part(b) LOCAL")

			// Verify the index works
			tk.MustQuery("SELECT COUNT(*) FROM t_part USE INDEX(idx_local)").Check(testkit.Rows("4"))

			tk.MustExec("drop table if exists t_part, t_normal")
		})
	}
}
