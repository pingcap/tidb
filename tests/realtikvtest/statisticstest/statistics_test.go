// Copyright 2022 PingCAP, Inc.
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

package statisticstest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/statistics/asyncload"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/tests/realtikvtest"
	"github.com/stretchr/testify/require"
)

func TestNewCollationStatsWithPrefixIndex(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	defer func() {
		tk := testkit.NewTestKit(t, store)
		tk.MustExec("use test")
		r := tk.MustQuery("show tables")
		for _, tb := range r.Rows() {
			tableName := tb[0]
			tk.MustExec(fmt.Sprintf("drop table %v", tableName))
		}
		tk.MustExec("delete from mysql.stats_meta")
		tk.MustExec("delete from mysql.stats_histograms")
		tk.MustExec("delete from mysql.stats_buckets")
		dom.StatsHandle().Clear()
	}()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a varchar(40) collate utf8mb4_general_ci, index ia3(a(3)), index ia10(a(10)), index ia(a))")
	tk.MustExec("insert into t values('aaAAaaaAAAabbc'), ('AaAaAaAaAaAbBC'), ('AAAaabbBBbbb'), ('AAAaabbBBbbbccc'), ('aaa'), ('Aa'), ('A'), ('ab')")
	tk.MustExec("insert into t values('b'), ('bBb'), ('Bb'), ('bA'), ('BBBB'), ('BBBBBDDDDDdd'), ('bbbbBBBBbbBBR'), ('BBbbBBbbBBbbBBRRR')")
	h := dom.StatsHandle()
	tk.MustExec("set @@session.tidb_analyze_version=1")
	require.NoError(t, h.DumpStatsDeltaToKV(true))

	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 'aaa'")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))

	tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 0 1 1 \x00A \x00A 0",
		"test t  a 0 1 2 1 \x00A\x00A \x00A\x00A 0",
		"test t  a 0 10 12 1 \x00B\x00B\x00B \x00B\x00B\x00B 0",
		"test t  a 0 11 13 1 \x00B\x00B\x00B\x00B \x00B\x00B\x00B\x00B 0",
		"test t  a 0 12 14 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 0",
		"test t  a 0 13 15 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R 0",
		"test t  a 0 14 16 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 0",
		"test t  a 0 2 3 1 \x00A\x00A\x00A \x00A\x00A\x00A 0",
		"test t  a 0 3 5 2 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 0",
		"test t  a 0 4 6 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B 0",
		"test t  a 0 5 7 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C 0",
		"test t  a 0 6 8 1 \x00A\x00B \x00A\x00B 0",
		"test t  a 0 7 9 1 \x00B \x00B 0",
		"test t  a 0 8 10 1 \x00B\x00A \x00B\x00A 0",
		"test t  a 0 9 11 1 \x00B\x00B \x00B\x00B 0",
		"test t  ia 1 0 1 1 \x00A \x00A 0",
		"test t  ia 1 1 2 1 \x00A\x00A \x00A\x00A 0",
		"test t  ia 1 10 12 1 \x00B\x00B\x00B \x00B\x00B\x00B 0",
		"test t  ia 1 11 13 1 \x00B\x00B\x00B\x00B \x00B\x00B\x00B\x00B 0",
		"test t  ia 1 12 14 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 0",
		"test t  ia 1 13 15 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R 0",
		"test t  ia 1 14 16 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 0",
		"test t  ia 1 2 3 1 \x00A\x00A\x00A \x00A\x00A\x00A 0",
		"test t  ia 1 3 5 2 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 0",
		"test t  ia 1 4 6 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B 0",
		"test t  ia 1 5 7 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C 0",
		"test t  ia 1 6 8 1 \x00A\x00B \x00A\x00B 0",
		"test t  ia 1 7 9 1 \x00B \x00B 0",
		"test t  ia 1 8 10 1 \x00B\x00A \x00B\x00A 0",
		"test t  ia 1 9 11 1 \x00B\x00B \x00B\x00B 0",
		"test t  ia10 1 0 1 1 \x00A \x00A 0",
		"test t  ia10 1 1 2 1 \x00A\x00A \x00A\x00A 0",
		"test t  ia10 1 10 13 1 \x00B\x00B\x00B\x00B \x00B\x00B\x00B\x00B 0",
		"test t  ia10 1 11 15 2 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B 0",
		"test t  ia10 1 12 16 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D 0",
		"test t  ia10 1 2 3 1 \x00A\x00A\x00A \x00A\x00A\x00A 0",
		"test t  ia10 1 3 5 2 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A 0",
		"test t  ia10 1 4 7 2 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B 0",
		"test t  ia10 1 5 8 1 \x00A\x00B \x00A\x00B 0",
		"test t  ia10 1 6 9 1 \x00B \x00B 0",
		"test t  ia10 1 7 10 1 \x00B\x00A \x00B\x00A 0",
		"test t  ia10 1 8 11 1 \x00B\x00B \x00B\x00B 0",
		"test t  ia10 1 9 12 1 \x00B\x00B\x00B \x00B\x00B\x00B 0",
		"test t  ia3 1 0 1 1 \x00A \x00A 0",
		"test t  ia3 1 1 2 1 \x00A\x00A \x00A\x00A 0",
		"test t  ia3 1 2 7 5 \x00A\x00A\x00A \x00A\x00A\x00A 0",
		"test t  ia3 1 3 8 1 \x00A\x00B \x00A\x00B 0",
		"test t  ia3 1 4 9 1 \x00B \x00B 0",
		"test t  ia3 1 5 10 1 \x00B\x00A \x00B\x00A 0",
		"test t  ia3 1 6 11 1 \x00B\x00B \x00B\x00B 0",
		"test t  ia3 1 7 16 5 \x00B\x00B\x00B \x00B\x00B\x00B 0",
	))
	tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 2",
	))
	tk.MustQuery("select is_index, hist_id, distinct_count, null_count, stats_ver, correlation from mysql.stats_histograms").Sort().Check(testkit.Rows(
		"0 1 15 0 1 0.8411764705882353",
		"1 1 8 0 1 0",
		"1 2 13 0 1 0",
		"1 3 15 0 1 0",
	))

	tk.MustExec("set @@session.tidb_analyze_version=2")
	h = dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(true))

	tk.MustExec("analyze table t")
	tk.MustExec("explain select * from t where a = 'aaa'")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))

	tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows())
	tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 \x00A 1",
		"test t  a 0 \x00A\x00A 1",
		"test t  a 0 \x00A\x00A\x00A 1",
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 2",
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B 1",
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C 1",
		"test t  a 0 \x00A\x00B 1",
		"test t  a 0 \x00B 1",
		"test t  a 0 \x00B\x00A 1",
		"test t  a 0 \x00B\x00B 1",
		"test t  a 0 \x00B\x00B\x00B 1",
		"test t  a 0 \x00B\x00B\x00B\x00B 1",
		"test t  a 0 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 1",
		"test t  a 0 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R 1",
		"test t  a 0 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 1",
		"test t  ia 1 \x00A 1",
		"test t  ia 1 \x00A\x00A 1",
		"test t  ia 1 \x00A\x00A\x00A 1",
		"test t  ia 1 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 2",
		"test t  ia 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B 1",
		"test t  ia 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00C\x00C\x00C 1",
		"test t  ia 1 \x00A\x00B 1",
		"test t  ia 1 \x00B 1",
		"test t  ia 1 \x00B\x00A 1",
		"test t  ia 1 \x00B\x00B 1",
		"test t  ia 1 \x00B\x00B\x00B 1",
		"test t  ia 1 \x00B\x00B\x00B\x00B 1",
		"test t  ia 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 1",
		"test t  ia 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R 1",
		"test t  ia 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 1",
		"test t  ia10 1 \x00A 1",
		"test t  ia10 1 \x00A\x00A 1",
		"test t  ia10 1 \x00A\x00A\x00A 1",
		"test t  ia10 1 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A 2",
		"test t  ia10 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B 2",
		"test t  ia10 1 \x00A\x00B 1",
		"test t  ia10 1 \x00B 1",
		"test t  ia10 1 \x00B\x00A 1",
		"test t  ia10 1 \x00B\x00B 1",
		"test t  ia10 1 \x00B\x00B\x00B 1",
		"test t  ia10 1 \x00B\x00B\x00B\x00B 1",
		"test t  ia10 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B 2",
		"test t  ia10 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D 1",
		"test t  ia3 1 \x00A 1",
		"test t  ia3 1 \x00A\x00A 1",
		"test t  ia3 1 \x00A\x00A\x00A 5",
		"test t  ia3 1 \x00A\x00B 1",
		"test t  ia3 1 \x00B 1",
		"test t  ia3 1 \x00B\x00A 1",
		"test t  ia3 1 \x00B\x00B 1",
		"test t  ia3 1 \x00B\x00B\x00B 5",
	))
	tk.MustQuery("select is_index, hist_id, distinct_count, null_count, stats_ver, correlation from mysql.stats_histograms").Sort().Check(testkit.Rows(
		"0 1 15 0 2 0.8411764705882353",
		"1 1 8 0 2 0",
		"1 2 13 0 2 0",
		"1 3 15 0 2 0",
	))
}

func TestBlockMergeFMSketch(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_async_merge_global_stats=OFF;")
	defer func() {
		tk.MustExec("set @@tidb_enable_async_merge_global_stats=ON;")
	}()
	checkFMSketch(tk)
}

func TestAsyncMergeFMSketch(t *testing.T) {
	store := realtikvtest.CreateMockStoreAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("set @@tidb_enable_async_merge_global_stats=ON;")
	checkFMSketch(tk)
}

func checkFMSketch(tk *testkit.TestKit) {
	tk.MustExec(`CREATE TABLE employees  (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,fname VARCHAR(25) NOT NULL,lname VARCHAR(25) NOT NULL,store_id INT NOT NULL,department_id INT NOT NULL
) PARTITION BY RANGE(id)  (
    PARTITION p0 VALUES LESS THAN (5),
    PARTITION p1 VALUES LESS THAN (10),
    PARTITION p2 VALUES LESS THAN (15),
    PARTITION p3 VALUES LESS THAN MAXVALUE
);`)
	tk.MustExec(`INSERT INTO employees(FNAME,LNAME,STORE_ID,DEPARTMENT_ID) VALUES
    ('Bob', 'Taylor', 3, 2), ('Frank', 'Williams', 1, 2),
    ('Ellen', 'Johnson', 3, 4), ('Jim', 'Smith', 2, 4),
    ('Mary', 'Jones', 1, 1), ('Linda', 'Black', 2, 3),
    ('Ed', 'Jones', 2, 1), ('June', 'Wilson', 3, 1),
    ('Andy', 'Smith', 1, 3), ('Lou', 'Waters', 2, 4),
    ('Jill', 'Stone', 1, 4), ('Roger', 'White', 3, 2),
    ('Howard', 'Andrews', 1, 2), ('Fred', 'Goldberg', 3, 3),
    ('Barbara', 'Brown', 2, 3), ('Alice', 'Rogers', 2, 2),
    ('Mark', 'Morgan', 3, 3), ('Karen', 'Cole', 3, 2);`)
	tk.MustExec("ANALYZE TABLE employees;")
	tk.MustExec("select * from employees;")
	tk.MustExec("alter table employees truncate partition p0;")
	tk.MustExec("select * from employees;")
	tk.MustExec("analyze table employees partition p3;")
	tk.MustExec("select * from employees;")
	tk.MustQuery(`SHOW STATS_HISTOGRAMS WHERE TABLE_NAME='employees' and partition_name="global"  and column_name="id"`).CheckAt([]int{6}, [][]any{
		{"14"}})
}

func TestNoNeedIndexStatsLoading(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test;")
	tk.MustExec("drop table if exists t;")
	// 1. Create a table and the statsHandle.Update(do.InfoSchema()) will load this table into the stats cache.
	tk.MustExec("create table if not exists t(a int, b int, index ia(a));")
	// 2. Drop the stats of the stats, it will clean up all system table records for this table.
	tk.MustExec("drop stats t;")
	// 3. Insert some data and wait for the modify_count and the count is not null in the mysql.stats_meta.
	tk.MustExec("insert into t value(1,1), (2,2);")
	h := dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
	// 4. Try to select some data from this table by ID, it would trigger an async load.
	tk.MustExec("set tidb_opt_objective='determinate';")
	tk.MustQuery("select * from t where a = 1 and b = 1;").Check(testkit.Rows("1 1"))
	table, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	checkTableIDInItems(t, table.Meta().ID)
}

func checkTableIDInItems(t *testing.T, tableID int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	done := make(chan bool)

	// First, confirm that the table ID is in the items.
	items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
	for _, item := range items {
		if item.TableID == tableID {
			// Then, continuously check until it no longer exists or timeout.
			go func() {
				for {
					select {
					case <-ticker.C:
						items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
						found := false
						for _, item := range items {
							if item.TableID == tableID {
								found = true
							}
						}
						if !found {
							done <- true
						}
					case <-ctx.Done():
						return
					}
				}
			}()
			break
		}
	}

	select {
	case <-done:
		t.Log("Table ID has been removed from items")
	case <-ctx.Done():
		t.Fatal("Timeout: Table ID was not removed from items within the time limit")
	}
}
