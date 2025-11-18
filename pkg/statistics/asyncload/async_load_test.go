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

package asyncload_test

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/statistics/asyncload"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

func TestLoadColumnStatisticsAfterTableDrop(t *testing.T) {
	// Use real tikv to enable the sync and async load.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	// Turn off the sync load.
	testKit.MustExec("SET @@tidb_stats_load_sync_wait = 0;")
	testKit.MustExec("use test")
	testKit.MustExec("DROP TABLE IF EXISTS t1")
	testKit.MustExec("CREATE TABLE t1 (a INT, b INT, c INT);")
	testKit.MustExec("INSERT INTO t1 VALUES (1,3,0), (2,2,0), (3,2,0);")
	handle := dom.StatsHandle()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	testKit.MustExec("ANALYZE TABLE t1 ALL COLUMNS;")
	// This will add the table to the AsyncLoadHistogramNeededItems.
	testKit.MustExec("SELECT * FROM t1 WHERE b = 2;")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	tableID := tableInfo.ID
	require.Eventually(t, func() bool {
		// Check the table is in the items.
		items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
		found := false
		for _, item := range items {
			if item.TableItemID.TableID == tableID {
				found = true
				break
			}
		}
		return found
	}, 5*time.Second, 2*time.Second,
		"table %d should be in the items", tableID,
	)

	// Drop the table.
	testKit.MustExec("DROP TABLE t1;")
	err = handle.LoadNeededHistograms(dom.InfoSchema())
	require.NoError(t, err)

	// Check the table is removed from the items.
	items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
	for _, item := range items {
		require.NotEqual(t, tableID, item.TableItemID.TableID)
	}
}

func TestLoadStatisticsAfterColumnDrop(t *testing.T) {
	// Use real tikv to enable the sync and async load.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	// Turn off the sync load.
	testKit.MustExec("SET @@tidb_stats_load_sync_wait = 0;")
	testKit.MustExec("use test")
	testKit.MustExec("DROP TABLE IF EXISTS t1")
	testKit.MustExec("CREATE TABLE t1 (a INT, b INT, c INT);")
	testKit.MustExec("INSERT INTO t1 VALUES (1,3,0), (2,2,0), (3,2,0);")
	handle := dom.StatsHandle()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	testKit.MustExec("ANALYZE TABLE t1 ALL COLUMNS;")
	// This will add the table to the AsyncLoadHistogramNeededItems.
	testKit.MustExec("SELECT * FROM t1 WHERE b = 2;")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	tableID := tableInfo.ID
	require.Eventually(t, func() bool {
		// Check the table is in the items.
		items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
		found := false
		for _, item := range items {
			if item.TableItemID.TableID == tableID {
				found = true
				break
			}
		}
		return found
	}, 5*time.Second, 2*time.Second,
		"table %d should be in the items", tableID,
	)

	// Drop the column.
	testKit.MustExec("ALTER TABLE t1 DROP COLUMN b;")
	err = handle.LoadNeededHistograms(dom.InfoSchema())
	require.NoError(t, err)

	// Check the table is removed from the items.
	items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
	for _, item := range items {
		require.NotEqual(t, tableID, item.TableItemID.TableID)
	}
}

func TestLoadIndexStatisticsAfterTableDrop(t *testing.T) {
	// Use real tikv to enable the sync and async load.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	// Turn off the sync load.
	testKit.MustExec("SET @@tidb_stats_load_sync_wait = 0;")
	testKit.MustExec("use test")
	testKit.MustExec("DROP TABLE IF EXISTS t1")
	testKit.MustExec("CREATE TABLE t1 (a INT, b INT, c INT, INDEX idx_b (b));")
	testKit.MustExec("INSERT INTO t1 VALUES (1,3,0), (2,2,0), (3,2,0);")
	handle := dom.StatsHandle()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	testKit.MustExec("ANALYZE TABLE t1 ALL COLUMNS;")
	// This will add the table to the AsyncLoadHistogramNeededItems.
	testKit.MustExec("SELECT * FROM t1 WHERE b = 2;")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	tableID := tableInfo.ID
	require.Eventually(t, func() bool {
		// Check the table is in the items.
		items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
		found := false
		for _, item := range items {
			if item.TableItemID.TableID == tableID {
				found = true
				break
			}
		}
		return found
	}, 5*time.Second, 2*time.Second,
		"table %d should be in the items", tableID,
	)

	// Drop the table.
	testKit.MustExec("DROP TABLE t1;")
	err = handle.LoadNeededHistograms(dom.InfoSchema())
	require.NoError(t, err)

	// Check the table is removed from the items.
	items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
	for _, item := range items {
		require.NotEqual(t, tableID, item.TableItemID.TableID)
	}
}

func TestLoadStatisticsAfterIndexDrop(t *testing.T) {
	// Use real tikv to enable the sync and async load.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	// Turn off the sync load.
	testKit.MustExec("SET @@tidb_stats_load_sync_wait = 0;")
	testKit.MustExec("use test")
	testKit.MustExec("DROP TABLE IF EXISTS t1")
	testKit.MustExec("CREATE TABLE t1 (a INT, b INT, c INT, INDEX idx_b (b));")
	testKit.MustExec("INSERT INTO t1 VALUES (1,3,0), (2,2,0), (3,2,0);")
	handle := dom.StatsHandle()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	testKit.MustExec("ANALYZE TABLE t1 ALL COLUMNS;")
	// This will add the table to the AsyncLoadHistogramNeededItems.
	testKit.MustExec("SELECT * FROM t1 WHERE b = 2;")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	tableID := tableInfo.ID
	require.Eventually(t, func() bool {
		// Check the table is in the items.
		items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
		found := false
		for _, item := range items {
			if item.TableItemID.TableID == tableID {
				found = true
				break
			}
		}
		return found
	}, 5*time.Second, 2*time.Second,
		"table %d should be in the items", tableID,
	)

	// Drop the index.
	testKit.MustExec("ALTER TABLE t1 DROP INDEX idx_b;")
	err = handle.LoadNeededHistograms(dom.InfoSchema())
	require.NoError(t, err)

	// Check the table is removed from the items.
	items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
	for _, item := range items {
		require.NotEqual(t, tableID, item.TableItemID.TableID)
	}
}

func TestLoadCorruptedStatistics(t *testing.T) {
	// Use real tikv to enable the sync and async load.
	store, dom := testkit.CreateMockStoreAndDomain(t)
	testKit := testkit.NewTestKit(t, store)
	// Turn off the sync load.
	testKit.MustExec("SET @@tidb_stats_load_sync_wait = 0;")
	testKit.MustExec("use test")
	testKit.MustExec("DROP TABLE IF EXISTS t1")
	testKit.MustExec("CREATE TABLE t1 (a INT, b INT, c INT, INDEX idx_b (b));")
	testKit.MustExec("INSERT INTO t1 VALUES (1,3,0), (2,2,0), (3,2,0);")
	handle := dom.StatsHandle()
	require.NoError(t, handle.DumpStatsDeltaToKV(true))
	require.NoError(t, handle.Update(context.Background(), dom.InfoSchema()))
	// Only collect the histogram buckets.
	testKit.MustExec("ANALYZE TABLE t1 ALL COLUMNS WITH 0 TOPN;")
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t1"))
	require.NoError(t, err)
	tableInfo := tbl.Meta()
	tableID := tableInfo.ID
	// Corrupt the statistics.
	testKit.MustExec("UPDATE mysql.stats_buckets SET upper_bound = 'who knows what it is' WHERE table_id = ?;", tableID)
	// This will add the table to the AsyncLoadHistogramNeededItems.
	testKit.MustExec("SELECT * FROM t1 WHERE b = 2;")

	require.Eventually(t, func() bool {
		// Check the table is in the items.
		items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
		found := false
		for _, item := range items {
			if item.TableItemID.TableID == tableID {
				found = true
				break
			}
		}
		return found
	}, 5*time.Second, 2*time.Second,
		"table %d should be in the items", tableID,
	)

	err = handle.LoadNeededHistograms(dom.InfoSchema())
	require.NoError(t, err)
	// Check the table is removed from the items.
	items := asyncload.AsyncLoadHistogramNeededItems.AllItems()
	for _, item := range items {
		require.NotEqual(t, tableID, item.TableItemID.TableID)
	}
}
