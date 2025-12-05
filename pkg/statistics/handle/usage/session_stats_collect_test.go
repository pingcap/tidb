// Copyright 2023 PingCAP, Inc.
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

package usage_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
)

// Test that first-time predicate usage creates a row with last_used_at set (NULL -> value).
func TestPredicateUsage_FirstTouchCreatesRow(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")

	// trigger predicate usage on column a
	tk.MustQuery("select * from t where a > 0").Check(testkit.Rows())
	require.NoError(t, dom.StatsHandle().DumpColStatsUsageToKV())

	// resolve table and column IDs
	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableID := tbl.Meta().ID
	colAID := tbl.Meta().Columns[0].ID

	// verify last_used_at is NOT NULL for (tableID, colAID)
	rows := tk.MustQuery(
		fmt.Sprintf("select last_used_at from mysql.column_stats_usage where table_id=%d and column_id=%d", tableID, colAID),
	).Rows()
	require.Len(t, rows, 1)
	require.NotEqual(t, "<nil>", rows[0][0])
}

// Test that repeated usage within throttle interval does not bump last_used_at.
func TestPredicateUsage_NoBumpWithinThrottle(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")

	// first touch
	tk.MustQuery("select * from t where a > 0").Check(testkit.Rows())
	require.NoError(t, dom.StatsHandle().DumpColStatsUsageToKV())

	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableID := tbl.Meta().ID
	colAID := tbl.Meta().Columns[0].ID

	ts1 := tk.MustQuery(
		fmt.Sprintf("select last_used_at from mysql.column_stats_usage where table_id=%d and column_id=%d", tableID, colAID),
	).Rows()[0][0].(string)

	// touch again shortly and dump; throttled path should skip bump
	tk.MustQuery("select * from t where a > 0").Check(testkit.Rows())
	require.NoError(t, dom.StatsHandle().DumpColStatsUsageToKV())

	ts2 := tk.MustQuery(
		fmt.Sprintf("select last_used_at from mysql.column_stats_usage where table_id=%d and column_id=%d", tableID, colAID),
	).Rows()[0][0].(string)
	require.Equal(t, ts1, ts2)
}

// Test that when stored last_used_at is very old, a new usage bumps it (exceeds 12h threshold).
func TestPredicateUsage_BumpAfterOldStoredValue(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int, b int)")

	// first touch to create the row
	tk.MustQuery("select * from t where a > 0").Check(testkit.Rows())
	require.NoError(t, dom.StatsHandle().DumpColStatsUsageToKV())

	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr("t"))
	require.NoError(t, err)
	tableID := tbl.Meta().ID
	colAID := tbl.Meta().Columns[0].ID

	// set an ancient last_used_at so the next usage exceeds throttle interval
	tk.MustExec(fmt.Sprintf(
		"update mysql.column_stats_usage set last_used_at = timestamp '2000-01-01 00:00:00' where table_id=%d and column_id=%d",
		tableID, colAID,
	))

	// touch again and dump; should bump to a recent timestamp
	tk.MustQuery("select * from t where a > 0").Check(testkit.Rows())
	require.NoError(t, dom.StatsHandle().DumpColStatsUsageToKV())

	// verify last_used_at changed from the ancient value
	got := tk.MustQuery(
		fmt.Sprintf("select last_used_at from mysql.column_stats_usage where table_id=%d and column_id=%d", tableID, colAID),
	).Rows()[0][0].(string)
	require.NotEqual(t, "2000-01-01 00:00:00", got)
}

type testTimeCollector struct {
	mu   sync.Mutex
	data []time.Duration
}

func (c *testTimeCollector) Record(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data = append(c.data, d)
}

func calculateStats(durations []time.Duration) (avg, minVal, maxVal time.Duration) {
	if len(durations) == 0 {
		return 0, 0, 0
	}
	var sum time.Duration
	minVal = durations[0]
	maxVal = durations[0]
	for _, d := range durations {
		sum += d
		if d < minVal {
			minVal = d
		}
		if d > maxVal {
			maxVal = d
		}
	}
	avg = time.Duration(int64(sum) / int64(len(durations)))
	return avg, minVal, maxVal
}

func TestDumpColStatsUsageWriter_ConcurrentMultiTables(t *testing.T) {
	t.Skip("Skipping concurrent stats test - run manually if needed")
	// create 100 domains to mimic 100 TiDB nodes; share the same underlying store
	dec := testkit.NewDistExecutionContextWithLease(t, 10, 200*time.Millisecond)
	defer dec.Close()
	tk := testkit.NewTestKit(t, dec.GetDomain(0).Store())
	tk.MustExec("use test")

	// attach collector to capture per-query timings inside usage code
	tc := &testTimeCollector{}

	const tableCount = 100
	const numCols = 1000
	for i := 1; i <= tableCount; i++ {
		tk.MustExec(fmt.Sprintf("drop table if exists t%d", i))
		cols := make([]string, 0, numCols)
		for c := 0; c < numCols; c++ {
			cols = append(cols, fmt.Sprintf("c%d int", c))
		}
		tk.MustExec(fmt.Sprintf("create table t%d (%s)", i, strings.Join(cols, ", ")))
	}

	// collect ids
	is := dec.GetDomain(0).InfoSchema()
	tableIDs := make([]int64, 0, tableCount)
	allCols := make([][]int64, 0, tableCount)
	for i := 1; i <= tableCount; i++ {
		tbl, err := is.TableByName(context.Background(), model.NewCIStr("test"), model.NewCIStr(fmt.Sprintf("t%d", i)))
		require.NoError(t, err)
		tid := tbl.Meta().ID
		tableIDs = append(tableIDs, tid)
		cids := make([]int64, 0, numCols)
		for c := 0; c < numCols; c++ {
			cids = append(cids, tbl.Meta().Columns[c].ID)
		}
		allCols = append(allCols, cids)
	}

	seedTSUTC := time.Now().UTC().Format("2006-01-02 15:04:05")
	seed := make([]usage.ColStatsUsageEntry, 0, tableCount*numCols)
	for i, tid := range tableIDs {
		for _, cid := range allCols[i] {
			seed = append(seed, usage.ColStatsUsageEntry{TableID: tid, ColumnID: cid, LastUsedAt: seedTSUTC})
		}
	}
	require.NoError(t, usage.DumpColStatsUsageEntries(dec.GetDomain(0).StatsHandle().SPool(), seed, nil))

	noBumpTSUTC := time.Now().UTC().Add(10 * time.Minute).Format("2006-01-02 15:04:05")
	bumpTSUTC := time.Now().UTC().Add(13 * time.Hour).Format("2006-01-02 15:04:05")
	entries := make([]usage.ColStatsUsageEntry, 0, tableCount*numCols)
	updatedExpect := 0
	idx := 0
	for i, tid := range tableIDs {
		for _, cid := range allCols[i] {
			if idx%1000 == 0 {
				entries = append(entries, usage.ColStatsUsageEntry{TableID: tid, ColumnID: cid, LastUsedAt: bumpTSUTC})
				updatedExpect++
			} else {
				entries = append(entries, usage.ColStatsUsageEntry{TableID: tid, ColumnID: cid, LastUsedAt: noBumpTSUTC})
			}
			idx++
		}
	}

	const goroutines = 10
	elapsedCh := make(chan time.Duration, goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			gStart := time.Now()
			dom := dec.GetDomain(id)
			err := usage.DumpColStatsUsageEntries(dom.StatsHandle().SPool(), entries, tc)
			gElapsed := time.Since(gStart)
			if err != nil {
				t.Logf("writer goroutine %d error: %v (elapsed=%s)", id, err, gElapsed)
			}
			require.NoError(t, err)
			t.Logf("writer goroutine %d finished in %s", id, gElapsed)
			elapsedCh <- gElapsed
		}(g)
	}
	goroutineTimes := make([]time.Duration, goroutines)
	for g := 0; g < goroutines; g++ {
		goroutineTimes[g] = <-elapsedCh
	}
	avg, minVal, maxVal := calculateStats(goroutineTimes)

	// verify only the intended subset updated: match exact UTC bumpTS via CONVERT_TZ
	totalUpdated := 0
	for _, tid := range tableIDs {
		row := tk.MustQuery(
			fmt.Sprintf(
				"select count(*) from mysql.column_stats_usage where table_id=%d and CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00') = timestamp '%s'",
				tid, bumpTSUTC,
			),
		).Rows()[0][0].(string)
		var c int
		_, _ = fmt.Sscanf(row, "%d", &c)
		totalUpdated += c
	}
	require.Equal(t, updatedExpect, totalUpdated)
	// one consolidated summary line combining per-goroutine and collector stats
	tc.mu.Lock()
	batches := len(tc.data)
	colAvg, colMinVal, colMaxVal := calculateStats(tc.data)
	tc.mu.Unlock()
	t.Logf("DumpColStatsUsage concurrent: goroutines=%d tables=%d cols/table=%d per-g: avg=%s min=%s max=%s; updated=%d/%d; batches=%d per-batch: avg=%s min=%s max=%s", goroutines, tableCount, numCols, avg, minVal, maxVal, totalUpdated, tableCount*numCols, batches, colAvg, colMinVal, colMaxVal)
}
