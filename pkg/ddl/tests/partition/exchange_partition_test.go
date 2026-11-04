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
	"slices"
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/testkit/external"
	"github.com/stretchr/testify/require"
)

func TestExchangeRangeColumnsPartition(t *testing.T) {
	store := testkit.CreateMockStore(t)

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_exchange_partition=1")

	// Create a table partitioned by range columns with multiple column types
	tk.MustExec(`CREATE TABLE t1 (
		id INT NOT NULL,
		age INT,
		name VARCHAR(50)
	) PARTITION BY RANGE COLUMNS(age, name) (
		PARTITION p0 VALUES LESS THAN (20, 'm'),
		PARTITION p1 VALUES LESS THAN (30, 'm'),
		PARTITION p2 VALUES LESS THAN (30, MAXVALUE),
		PARTITION p3 VALUES LESS THAN (40, 'm'),
		PARTITION p4 VALUES LESS THAN (MAXVALUE, MAXVALUE)
	)`)

	// Define test values for each column type
	ageValues := []any{
		nil,         // NULL
		-2147483648, // min int
		2147483647,  // max int
		0,
		19, // boundary-1
		20, // boundary 1
		29, // boundary-1
		30, // boundary 2
		39, // boundary-1
		40, // boundary 3
	}

	nameValues := []any{
		nil, // NULL
		"",  // empty string
		"l", // boundary-1
		"m", // boundary
		"n", // boundary+1
	}

	// Generate all combinations
	id := 0
	addComma := false
	query := "INSERT INTO t1 VALUES "
	for _, age := range ageValues {
		for _, name := range nameValues {
			id++
			// if id != 26 {
			// 	continue
			// }
			if addComma {
				query += ","
			}
			if age == nil && name == nil {
				query += fmt.Sprintf("(%d, NULL, NULL)", id)
			} else if age == nil {
				query += fmt.Sprintf("(%d, NULL, %q)", id, name)
			} else if name == nil {
				query += fmt.Sprintf("(%d, %d, NULL)", id, age)
			} else {
				query += fmt.Sprintf("(%d, %d, %q)", id, age, name)
			}
			addComma = true
		}
	}
	tk.MustExec(query)

	// Save initial counts per partition
	initialResults := make(map[string]*testkit.Result)
	for _, p := range []string{"p0", "p1", "p2", "p3", "p4"} {
		result := tk.MustQuery(fmt.Sprintf("SELECT * FROM t1 PARTITION(%s)", p)).Sort()
		initialResults[p] = result
	}

	// Create empty exchange table
	tk.MustExec(`CREATE TABLE t2 (
			id INT NOT NULL,
			age INT,
			name VARCHAR(50)
		)`)
	// Test each partition
	partitionNames := []string{"p0", "p1", "p2", "p3", "p4"}
	for i, p := range partitionNames {
		// Exchange partition out
		tk.MustExec(fmt.Sprintf("ALTER TABLE t1 EXCHANGE PARTITION %s WITH TABLE t2", p))

		// Verify partition is now empty
		tk.MustQuery(fmt.Sprintf("SELECT COUNT(*) FROM t1 PARTITION(%s)", p)).Check(testkit.Rows("0"))

		// Verify all rows moved to t2
		tk.MustQuery("SELECT * FROM t2").Sort().Check(initialResults[p].Rows())

		// Exchange partition back
		tk.MustExec(fmt.Sprintf("ALTER TABLE t1 EXCHANGE PARTITION %s WITH TABLE t2", p))

		// Verify results are back to initial state
		tk.MustQuery(fmt.Sprintf("SELECT * FROM t1 PARTITION(%s)", p)).Sort().Check(initialResults[p].Rows())

		// Check that no non-matching rows will be allowed to be exchanged
		otherPartitions := strings.Join(slices.Concat(partitionNames[:i], partitionNames[i+1:]), ",")
		for j := 1; j <= id; j++ {
			res := tk.MustQuery(fmt.Sprintf("select * from t1 partition (%s) where id = %d", p, j))
			if len(res.Rows()) > 0 {
				// Skip rows from current partition, since already tested above.
				continue
			}
			tk.MustExec(fmt.Sprintf("insert into t2 select * from t1 partition (%s) where id = %d", otherPartitions, j))
			tk.MustContainErrMsg(fmt.Sprintf("ALTER TABLE t1 EXCHANGE PARTITION %s WITH TABLE t2 /* j = %d */", p, j), "[ddl:1737]Found a row that does not match the partition")
			tk.MustExec(`truncate table t2`)
		}
	}
	// Cleanup exchange table
	tk.MustExec("DROP TABLE t2")

	// Clean up
	tk.MustExec("DROP TABLE t1")
}

// TestIssue72478ExchangePartitionMaxIDs covers column, index, foreign key, and
// check constraint ID allocators during EXCHANGE PARTITION. Column and index IDs
// remain in exchanged data and the partitioned table's other partitions, so each
// table must allocate past its physical data's historical IDs. Foreign key and
// check constraint counters stay with each table's retained metadata.
// See https://github.com/pingcap/tidb/issues/72478.
func TestIssue72478ExchangePartitionMaxIDs(t *testing.T) {
	store := testkit.CreateMockStore(t)
	newKit := func(t *testing.T) *testkit.TestKit {
		t.Helper()
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_enable_exchange_partition=1")
		return tk
	}
	enableGlobalVar := func(t *testing.T, tk *testkit.TestKit, name string) {
		t.Helper()
		original := tk.MustQuery("SELECT @@GLOBAL." + name).Rows()[0][0]
		t.Cleanup(func() {
			tk.MustExec(fmt.Sprintf("SET @@GLOBAL.%s = %v", name, original))
		})
		tk.MustExec("SET @@GLOBAL." + name + " = 1")
	}

	// The non-partitioned table has the higher counters. Its dropped column
	// value moves into p0, while p1 keeps the partitioned table's own rows.
	t.Run("non-partitioned table has higher column and index id", func(t *testing.T) {
		tk := newKit(t)
		tk.MustExec(`CREATE TABLE pt (
			a INT,
			INDEX idx_a(a)
		) PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (100),
			PARTITION p1 VALUES LESS THAN (MAXVALUE)
		)`)
		tk.MustExec(`CREATE TABLE nt (
			a INT,
			b INT,
			INDEX idx_a(a),
			INDEX idx_b(b)
		)`)
		tk.MustExec(`INSERT INTO nt VALUES (1, 777)`)
		tk.MustExec(`INSERT INTO pt VALUES (5), (150)`)
		tk.MustExec(`ALTER TABLE nt DROP INDEX idx_b`)
		tk.MustExec(`ALTER TABLE nt DROP COLUMN b`)
		tk.MustExec(`ALTER TABLE pt EXCHANGE PARTITION p0 WITH TABLE nt`)
		ntInfo := external.GetTableByName(t, tk, "test", "nt").Meta()
		require.Equal(t, int64(1), ntInfo.MaxColumnID)
		require.Equal(t, int64(1), ntInfo.MaxIndexID)

		tk.MustExec(`ALTER TABLE pt ADD COLUMN c INT DEFAULT 0`)
		tk.MustExec(`INSERT INTO pt (a) VALUES (2)`)
		tk.MustQuery(`SELECT a, c FROM pt ORDER BY a`).Check(testkit.Rows("1 0", "2 0", "150 0"))
		tk.MustExec(`ALTER TABLE nt ADD COLUMN c INT DEFAULT 0`)
		tk.MustExec(`INSERT INTO nt (a) VALUES (6)`)
		tk.MustQuery(`SELECT a, c FROM nt ORDER BY a`).Check(testkit.Rows("5 0", "6 0"))

		// pt keeps the dropped idx_b ID from nt, so its next index is 3.
		// nt receives pt's original partition, which has only used index ID 1.
		tk.MustExec(`ALTER TABLE pt ADD INDEX idx_new(a)`)
		tk.MustExec(`ALTER TABLE nt ADD INDEX idx_new(a)`)
		tk.MustQuery(`SELECT table_name, index_id FROM information_schema.tidb_indexes
			WHERE table_schema = 'test' AND key_name = 'idx_new' AND table_name IN ('pt', 'nt')
			ORDER BY table_name`).Check(testkit.Rows("nt 2", "pt 3"))
		tk.MustExec(`admin check table pt`)
		tk.MustExec(`admin check table nt`)
	})

	// The partitioned table has the higher counters. p1 still contains the
	// dropped column, and p0's rows move to the non-partitioned table.
	t.Run("partitioned table has higher column and index id", func(t *testing.T) {
		tk := newKit(t)
		tk.MustExec(`CREATE TABLE rpt (
			a INT,
			b INT,
			INDEX idx_a(a),
			INDEX idx_b(b)
		) PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (100),
			PARTITION p1 VALUES LESS THAN (MAXVALUE)
		)`)
		tk.MustExec(`CREATE TABLE rnt (
			a INT,
			INDEX idx_a(a)
		)`)
		tk.MustExec(`INSERT INTO rpt VALUES (1, 888), (101, 999)`)
		tk.MustExec(`ALTER TABLE rpt DROP INDEX idx_b`)
		tk.MustExec(`ALTER TABLE rpt DROP COLUMN b`)
		tk.MustExec(`ALTER TABLE rpt EXCHANGE PARTITION p0 WITH TABLE rnt`)

		tk.MustExec(`ALTER TABLE rnt ADD COLUMN c INT DEFAULT 0`)
		tk.MustQuery(`SELECT a, c FROM rnt`).Check(testkit.Rows("1 0"))
		tk.MustExec(`ALTER TABLE rpt ADD COLUMN c INT DEFAULT 0`)
		tk.MustQuery(`SELECT a, c FROM rpt`).Check(testkit.Rows("101 0"))

		tk.MustExec(`ALTER TABLE rpt ADD INDEX idx_new(a)`)
		tk.MustExec(`ALTER TABLE rnt ADD INDEX idx_new(a)`)
		tk.MustQuery(`SELECT table_name, index_id FROM information_schema.tidb_indexes
			WHERE table_schema = 'test' AND key_name = 'idx_new' AND table_name IN ('rpt', 'rnt')
			ORDER BY table_name`).Check(testkit.Rows("rnt 3", "rpt 3"))
		tk.MustExec(`admin check table rpt`)
		tk.MustExec(`admin check table rnt`)
	})

	// Foreign key and check constraint IDs are not encoded in KV, so each
	// table keeps its own counters. A partitioned table cannot own a foreign key.
	t.Run("non-partitioned table has higher foreign key and constraint id", func(t *testing.T) {
		tk := newKit(t)
		enableGlobalVar(t, tk, "tidb_enable_foreign_key")
		enableGlobalVar(t, tk, "tidb_enable_check_constraint")
		tk.MustExec(`CREATE TABLE parent (a INT PRIMARY KEY)`)
		tk.MustExec(`CREATE TABLE fk_nt (
			a INT,
			INDEX idx_a(a),
			CONSTRAINT fk_old FOREIGN KEY (a) REFERENCES parent(a),
			CONSTRAINT c_old CHECK (a < 1000),
			CONSTRAINT c_live CHECK (a < 2000)
		)`)
		tk.MustExec(`ALTER TABLE fk_nt DROP FOREIGN KEY fk_old`)
		tk.MustExec(`ALTER TABLE fk_nt DROP CONSTRAINT c_old`)
		tk.MustExec(`CREATE TABLE fk_pt (
			a INT,
			INDEX idx_a(a)
		) PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (100),
			PARTITION p1 VALUES LESS THAN (MAXVALUE)
		)`)
		tk.MustExec(`ALTER TABLE fk_pt EXCHANGE PARTITION p0 WITH TABLE fk_nt`)
		assertMaxForeignKeyAndConstraintID(t, tk, "fk_pt", 0, 0)
		assertMaxForeignKeyAndConstraintID(t, tk, "fk_nt", 1, 2)
		// NT retains its live constraint ID even though PT's original maximum
		// was zero. A new constraint must not reuse that live ID.
		tk.MustExec(`ALTER TABLE fk_nt ADD CONSTRAINT c_new CHECK (a >= 0)`)
		ntInfo := external.GetTableByName(t, tk, "test", "fk_nt").Meta()
		require.Equal(t, int64(3), ntInfo.MaxConstraintID)
		require.Equal(t, int64(2), ntInfo.FindConstraintInfoByName("c_live").ID)
		require.Equal(t, int64(3), ntInfo.FindConstraintInfoByName("c_new").ID)
		tk.MustExec(`ALTER TABLE fk_pt ADD CONSTRAINT c_pt_new CHECK (a >= 0)`)
		assertMaxForeignKeyAndConstraintID(t, tk, "fk_pt", 0, 1)
	})

	// The partitioned table keeps its higher check constraint ID counter.
	t.Run("partitioned table has higher constraint id", func(t *testing.T) {
		tk := newKit(t)
		enableGlobalVar(t, tk, "tidb_enable_check_constraint")
		tk.MustExec(`CREATE TABLE cst_pt (
			a INT,
			CONSTRAINT c_old CHECK (a < 1000)
		) PARTITION BY RANGE (a) (
			PARTITION p0 VALUES LESS THAN (100),
			PARTITION p1 VALUES LESS THAN (MAXVALUE)
		)`)
		tk.MustExec(`ALTER TABLE cst_pt DROP CONSTRAINT c_old`)
		tk.MustExec(`CREATE TABLE cst_nt (a INT)`)
		tk.MustExec(`ALTER TABLE cst_pt EXCHANGE PARTITION p0 WITH TABLE cst_nt`)
		assertMaxForeignKeyAndConstraintID(t, tk, "cst_pt", 0, 1)
		assertMaxForeignKeyAndConstraintID(t, tk, "cst_nt", 0, 0)
	})
}

func assertMaxForeignKeyAndConstraintID(t *testing.T, tk *testkit.TestKit, name string, fkID, constraintID int64) {
	t.Helper()
	meta := external.GetTableByName(t, tk, "test", name).Meta()
	require.Equal(t, fkID, meta.MaxForeignKeyID, name)
	require.Equal(t, constraintID, meta.MaxConstraintID, name)
}
