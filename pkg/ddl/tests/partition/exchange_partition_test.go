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
		otherPartitions := strings.Join(append(append([]string{}, partitionNames[:i]...), partitionNames[i+1:]...), ",")
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

// TestIssue72478ExchangePartitionMaxColumnAndIndexID covers EXCHANGE PARTITION
// when the tables' current columns and indexes match but one side has used a
// higher column or index ID. Those IDs stay in exchanged rows and in the
// partitioned table's other partitions, so both tables must allocate past the
// higher counter. See https://github.com/pingcap/tidb/issues/72478.
func TestIssue72478ExchangePartitionMaxColumnAndIndexID(t *testing.T) {
	store := testkit.CreateMockStore(t)
	newKit := func(t *testing.T) *testkit.TestKit {
		t.Helper()
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		tk.MustExec("set @@tidb_enable_exchange_partition=1")
		return tk
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
		tk.MustExec(`INSERT INTO pt VALUES (150)`)
		tk.MustExec(`ALTER TABLE nt DROP INDEX idx_b`)
		tk.MustExec(`ALTER TABLE nt DROP COLUMN b`)
		tk.MustExec(`ALTER TABLE pt EXCHANGE PARTITION p0 WITH TABLE nt`)

		tk.MustExec(`ALTER TABLE pt ADD COLUMN c INT DEFAULT 0`)
		tk.MustExec(`INSERT INTO pt (a) VALUES (2)`)
		tk.MustQuery(`SELECT a, c FROM pt ORDER BY a`).Check(testkit.Rows("1 0", "2 0", "150 0"))
		tk.MustExec(`ALTER TABLE nt ADD COLUMN c INT DEFAULT 0`)
		tk.MustQuery(`SELECT a, c FROM nt`).Check(testkit.Rows())

		// idx_a is 1 and the dropped idx_b was 2, so the next index on either
		// table must be 3.
		tk.MustExec(`ALTER TABLE pt ADD INDEX idx_new(a)`)
		tk.MustExec(`ALTER TABLE nt ADD INDEX idx_new(a)`)
		tk.MustQuery(`SELECT table_name, index_id FROM information_schema.tidb_indexes
			WHERE table_schema = 'test' AND key_name = 'idx_new' AND table_name IN ('pt', 'nt')
			ORDER BY table_name`).Check(testkit.Rows("nt 3", "pt 3"))
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

	// Foreign key and check constraint IDs are not encoded in KV. Exchange
	// still raises both counters. The non-partitioned table contributes the
	// foreign key ID, because a partitioned table cannot own a foreign key.
	t.Run("non-partitioned table has higher foreign key and constraint id", func(t *testing.T) {
		tk := newKit(t)
		fkEnabled := tk.MustQuery("select @@global.tidb_enable_foreign_key").Rows()[0][0]
		checkEnabled := tk.MustQuery("select @@global.tidb_enable_check_constraint").Rows()[0][0]
		tk.MustExec("set @@global.tidb_enable_foreign_key = 1")
		tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
		t.Cleanup(func() {
			tk.MustExec(fmt.Sprintf("set @@global.tidb_enable_foreign_key = %v", fkEnabled))
			tk.MustExec(fmt.Sprintf("set @@global.tidb_enable_check_constraint = %v", checkEnabled))
		})
		tk.MustExec(`CREATE TABLE parent (a INT PRIMARY KEY)`)
		tk.MustExec(`CREATE TABLE fk_nt (
			a INT,
			INDEX idx_a(a),
			CONSTRAINT fk_old FOREIGN KEY (a) REFERENCES parent(a),
			CONSTRAINT c_old CHECK (a < 1000)
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
		assertMaxForeignKeyAndConstraintID(t, tk, "fk_pt", "fk_nt", 1, 1)
	})

	// The partitioned table contributes the higher check constraint ID.
	t.Run("partitioned table has higher constraint id", func(t *testing.T) {
		tk := newKit(t)
		checkEnabled := tk.MustQuery("select @@global.tidb_enable_check_constraint").Rows()[0][0]
		tk.MustExec("set @@global.tidb_enable_check_constraint = 1")
		t.Cleanup(func() {
			tk.MustExec(fmt.Sprintf("set @@global.tidb_enable_check_constraint = %v", checkEnabled))
		})
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
		assertMaxForeignKeyAndConstraintID(t, tk, "cst_pt", "cst_nt", 0, 1)
	})
}

func assertMaxForeignKeyAndConstraintID(t *testing.T, tk *testkit.TestKit, pt, nt string, fkID, constraintID int64) {
	t.Helper()
	for _, name := range []string{pt, nt} {
		meta := external.GetTableByName(t, tk, "test", name).Meta()
		require.Equal(t, fkID, meta.MaxForeignKeyID, name)
		require.Equal(t, constraintID, meta.MaxConstraintID, name)
	}
}
