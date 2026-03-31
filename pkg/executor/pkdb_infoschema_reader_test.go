// Copyright 2020 PingCAP, Inc.
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

package executor_test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestTableRegions(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	t.Run("Basic table", func(t *testing.T) {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
		tk.MustExec("set global tidb_scatter_region = 'table'")

		// A simple non-partitioned table with a primary key.
		tk.MustExec("drop table if exists t1")
		tk.MustExec("create table t1 (id int primary key, name varchar(32))")
		tk.MustExec("insert into t1 values (1, 'sh'), (2, 'sh2')")

		require.Eventually(t, func() bool {
			// 1. Fetch the region keys for the table from information_schema.table_regions.
			// For a newly created small table, there is typically only one region.
			rows := tk.MustQuery("select start_key, end_key from information_schema.table_regions where table_name = 't1'").Rows()
			if len(rows) != 1 {
				return false
			}

			startKey := rows[0][0]
			endKey := rows[0][1]

			// 2. Construct the query with the TABLESPLIT clause.
			query := fmt.Sprintf("select id, name from t1 TABLESPLIT('%s', '%s') order by id", startKey, endKey)

			// 3. Execute the query and verify the result.
			// The expected result should be all rows from the table, as the split keys cover the entire table.
			rows = tk.MustQuery(query).Rows()
			return len(rows) == 2 &&
				rows[0][0].(string) == "1" && rows[0][1].(string) == "sh" &&
				rows[1][0].(string) == "2" && rows[1][1].(string) == "sh2"
		}, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("Partitioned Table", func(t *testing.T) {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
		tk.MustExec("set global tidb_scatter_region = 'table'")

		// A table partitioned by range. Each partition should have its own region(s).
		tk.MustExec("drop table if exists pt1")
		tk.MustExec(`
            create table pt1 (id int, name varchar(32))
            partition by range (id) (
                partition p0 values less than (100),
                partition p1 values less than (200),
                partition p2 values less than (MAXVALUE)
            )
        `)
		tk.MustExec("insert into pt1 values (1, 'a'), (101, 'b'), (201, 'c')")

		require.Eventually(t, func() bool {
			rows := tk.MustQuery("select count(*) from information_schema.table_regions where table_name='pt1'").Rows()
			if len(rows) != 1 {
				return false
			}
			if rows[0][0].(string) != "3" {
				return false
			}

			// 1. Fetch the region keys corresponding to partition p0.
			rowsP0 := tk.MustQuery("select start_key, end_key from information_schema.table_regions where table_name = 'pt1' and partition_name = 'p0'").Rows()
			if len(rowsP0) != 1 {
				return false
			}
			startKeyP0 := rowsP0[0][0]
			endKeyP0 := rowsP0[0][1]

			// 2. Construct the query for partition p0 and verify the result.
			queryP0 := fmt.Sprintf("select id, name from pt1 TABLESPLIT('%s', '%s')", startKeyP0, endKeyP0)
			// It is expected to return only the data from partition p0.
			rows = tk.MustQuery(queryP0).Rows()
			if len(rows) != 1 || rows[0][0].(string) != "1" || rows[0][1].(string) != "a" {
				return false
			}

			// 3. Fetch the region keys corresponding to partition p1.
			rowsP1 := tk.MustQuery("select start_key, end_key from information_schema.table_regions where table_name = 'pt1' and partition_name = 'p1'").Rows()
			if len(rowsP1) != 1 {
				return false
			}
			startKeyP1 := rowsP1[0][0]
			endKeyP1 := rowsP1[0][1]

			// 4. Construct the query for partition p1 and verify the result.
			queryP1 := fmt.Sprintf("select id, name from pt1 TABLESPLIT('%s', '%s')", startKeyP1, endKeyP1)
			// It is expected to return only the data from partition p1.
			rows = tk.MustQuery(queryP1).Rows()
			if len(rows) != 1 || rows[0][0].(string) != "101" || rows[0][1].(string) != "b" {
				return false
			}

			return true
		}, 5*time.Second, 50*time.Millisecond)
	})

	t.Run("Empty Table", func(t *testing.T) {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
		tk.MustExec("set global tidb_scatter_region = 'table'")

		// A table with no data should still be associated with a region.
		tk.MustExec("drop table if exists t_empty")
		tk.MustExec("create table t_empty (id int)")
		tk.MustQuery("select count(*) from information_schema.table_regions where table_name='t_empty'").Check(testkit.Rows(
			"1",
		))
	})

	t.Run("Basic table with index", func(t *testing.T) {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
		tk.MustExec("set global tidb_scatter_region = 'table'")

		// A table with a secondary index. The query should only show the table's row region, not the index region.
		tk.MustExec("drop table if exists t_with_index")
		tk.MustExec("create table t_with_index (id int primary key, name varchar(32), key idx_name (name))")
		tk.MustExec("insert into t_with_index values (1, 'a'), (2, 'b')")
		tk.MustQuery("select count(*) from information_schema.table_regions where table_name='t_with_index'").Check(testkit.Rows(
			"1",
		))
	})

	t.Run("Partitioned table with index", func(t *testing.T) {
		atomic.StoreUint32(&ddl.EnableSplitTableRegion, 1)
		tk.MustExec("set global tidb_scatter_region = 'table'")

		// A partitioned table with a secondary index. The query should only show the table's row region, not the index region.
		tk.MustExec("drop table if exists pt_with_index")
		tk.MustExec(`
            create table pt_with_index (id int, name varchar(32), key idx_name (name))
            partition by range (id) (
                partition p0 values less than (10),
                partition p1 values less than (20)
            )
        `)
		tk.MustExec("insert into pt_with_index values (1, 'a'), (11, 'b')")
		tk.MustQuery("select count(*) from information_schema.table_regions where table_name='pt_with_index'").Check(testkit.Rows(
			"2",
		))
	})
}
