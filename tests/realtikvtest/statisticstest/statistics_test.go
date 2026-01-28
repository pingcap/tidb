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
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/statistics/asyncload"
	"github.com/pingcap/tidb/pkg/statistics/handle/storage"
	"github.com/pingcap/tidb/pkg/statistics/handle/util"
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
	tk.MustExec("set @@session.tidb_analyze_version=2")
	h := dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(true))

	tk.MustExec("analyze table t")
	// Wait for stats to be fully persisted and loaded
	require.NoError(t, h.Update(context.Background(), dom.InfoSchema()))
	// Priming select followed by explain to load needed histograms.
	tk.MustExec("select count(*) from t where a = 'aaa'")
	tk.MustExec("explain select * from t where a = 'aaa'")
	require.NoError(t, h.LoadNeededHistograms(dom.InfoSchema()))

	tk.MustQuery("show stats_buckets where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 0 3 1 \x00A \x00A\x00A\x00A 0",
		"test t  a 0 1 6 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B \x00A\x00B 0",
		"test t  a 0 2 9 1 \x00B \x00B\x00B 0",
		"test t  a 0 3 12 1 \x00B\x00B\x00B \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 0",
		"test t  a 0 4 14 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 0",
		"test t  ia 1 0 3 1 \x00A \x00A\x00A\x00A 0",
		"test t  ia 1 1 6 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B\x00B\x00B \x00A\x00B 0",
		"test t  ia 1 2 9 1 \x00B \x00B\x00B 0",
		"test t  ia 1 3 12 1 \x00B\x00B\x00B \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R\x00R\x00R 0",
		"test t  ia 1 4 14 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00R \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D\x00D\x00D 0",
		"test t  ia10 1 0 3 1 \x00A \x00A\x00A\x00A 0",
		"test t  ia10 1 1 6 1 \x00A\x00B \x00B\x00A 0",
		"test t  ia10 1 2 9 1 \x00B\x00B \x00B\x00B\x00B\x00B 0",
		"test t  ia10 1 3 10 1 \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D \x00B\x00B\x00B\x00B\x00B\x00D\x00D\x00D\x00D\x00D 0",
	))
	tk.MustQuery("show stats_topn where db_name = 'test' and table_name = 't'").Sort().Check(testkit.Rows(
		"test t  a 0 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 2",
		"test t  ia 1 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00C 2",
		"test t  ia10 1 \x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A\x00A 2",
		"test t  ia10 1 \x00A\x00A\x00A\x00A\x00A\x00B\x00B\x00B\x00B\x00B 2",
		"test t  ia10 1 \x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B\x00B 2",
		"test t  ia3 1 \x00A 1",
		"test t  ia3 1 \x00A\x00A 1",
		"test t  ia3 1 \x00A\x00A\x00A 5",
		"test t  ia3 1 \x00A\x00B 1",
		"test t  ia3 1 \x00B 1",
		"test t  ia3 1 \x00B\x00A 1",
		"test t  ia3 1 \x00B\x00B 1",
		"test t  ia3 1 \x00B\x00B\x00B 5",
	))
	tblInfo, err := dom.InfoSchema().TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableID := tblInfo.Meta().ID
	// Check histogram stats, using tolerance for correlation which can vary slightly
	rows := tk.MustQuery("select is_index, hist_id, distinct_count, null_count, stats_ver, correlation from mysql.stats_histograms where table_id = ?", tableID).Sort().Rows()
	require.Len(t, rows, 4)

	// Check column histogram (is_index=0)
	require.Equal(t, "0", rows[0][0])
	require.Equal(t, "1", rows[0][1])
	require.Equal(t, "15", rows[0][2])
	require.Equal(t, "0", rows[0][3])
	require.Equal(t, "2", rows[0][4])
	correlation := rows[0][5].(string)
	correlationFloat, err := strconv.ParseFloat(correlation, 64)
	require.NoError(t, err)
	require.InDelta(t, 0.8411764705882353, correlationFloat, 0.01, "correlation should be approximately 0.841")

	// Check index histograms (is_index=1)
	tk.MustQuery("select is_index, hist_id, distinct_count, null_count, stats_ver, correlation from mysql.stats_histograms where is_index=1 and table_id = ?", tableID).Sort().Check(testkit.Rows(
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

func TestLoadNonExistentIndexStats(t *testing.T) {
	store, dom := realtikvtest.CreateMockStoreAndDomainAndSetup(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t;")
	tk.MustExec("create table if not exists t(a int, b int);")
	// Add an index after table creation. The index histogram will exist in the stats cache
	// but won't have actual histogram data loaded yet since the table hasn't been analyzed.
	tk.MustExec("alter table t add index ia(a);")
	tk.MustExec("insert into t value(1,1), (2,2);")
	h := dom.StatsHandle()
	require.NoError(t, h.DumpStatsDeltaToKV(true))
	ctx := context.Background()
	require.NoError(t, h.Update(ctx, dom.InfoSchema()))
	// Trigger async load of index histogram by using the index in a query.
	// Setting this variable to determinate marks the pseudo table stats as able to trigger loading (CanNotTriggerLoad=false), which enables statistics loading.
	// See more at IndexStatsIsInvalid and GetStatsTable functions.
	tk.MustExec("set tidb_opt_objective='determinate';")
	tk.MustQuery("select * from t where a = 1 and b = 1;").Check(testkit.Rows("1 1"))
	table, err := dom.InfoSchema().TableByName(ctx, ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableInfo := table.Meta()
	addedIndexID := tableInfo.Indices[0].ID
	// Wait for the async load to add the index to AsyncLoadHistogramNeededItems.
	// We should have 3 items: columns a, b, and index ia.
	require.Eventually(t, func() bool {
		items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
		for _, item := range items {
			if item.IsIndex && item.TableID == tableInfo.ID && item.ID == addedIndexID {
				// NOTE: Because the real TiKV test enables sync load by default,
				// the column stats may or may not be in the AsyncLoadHistogramNeededItems. Therefore, we only check the index here.
				return true
			}
		}
		return false
	}, time.Second*5, time.Millisecond*100, "Index ia should be in AsyncLoadHistogramNeededItems")

	// Verify that LoadNeededHistograms doesn't panic when the pseudo index stats exists in the cache
	// but doesn't have histogram data in mysql.stats_histograms yet.
	err = util.CallWithSCtx(h.SPool(), func(sctx sessionctx.Context) error {
		require.NotPanics(t, func() {
			err := storage.LoadNeededHistograms(sctx, dom.InfoSchema(), h)
			require.NoError(t, err)
		})
		return nil
	}, util.FlagWrapTxn)
	require.NoError(t, err)

	// Verify all items were removed from AsyncLoadHistogramNeededItems after loading.
	items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
	require.Equal(t, len(items), 0, "AsyncLoadHistogramNeededItems should be empty after loading")
}
