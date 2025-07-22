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
