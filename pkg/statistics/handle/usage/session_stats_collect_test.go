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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/parser/ast"
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
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableID := tbl.Meta().ID
	colAID := tbl.Meta().Columns[0].ID

	// verify last_used_at is NOT NULL for (tableID, colAID)
	rows := tk.MustQuery(
		"select last_used_at from mysql.column_stats_usage where table_id=? and column_id=?",
		tableID,
		colAID,
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
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableID := tbl.Meta().ID
	colAID := tbl.Meta().Columns[0].ID

	ts1 := tk.MustQuery(
		"select last_used_at from mysql.column_stats_usage where table_id=? and column_id=?",
		tableID,
		colAID,
	).Rows()[0][0].(string)

	// touch again shortly and dump; throttled path should skip bump
	tk.MustQuery("select * from t where a > 0").Check(testkit.Rows())
	require.NoError(t, dom.StatsHandle().DumpColStatsUsageToKV())

	ts2 := tk.MustQuery(
		"select last_used_at from mysql.column_stats_usage where table_id=? and column_id=?",
		tableID,
		colAID,
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
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableID := tbl.Meta().ID
	colAID := tbl.Meta().Columns[0].ID

	// set an ancient last_used_at so the next usage exceeds throttle interval
	tk.MustExec(
		"update mysql.column_stats_usage set last_used_at = cast(? as datetime) where table_id=? and column_id=?",
		"2000-01-01 00:00:00",
		tableID,
		colAID,
	)

	// touch again and dump; should bump to a recent timestamp
	tk.MustQuery("select * from t where a > 0").Check(testkit.Rows())
	require.NoError(t, dom.StatsHandle().DumpColStatsUsageToKV())

	// verify last_used_at changed from the ancient value
	got := tk.MustQuery(
		"select last_used_at from mysql.column_stats_usage where table_id=? and column_id=?",
		tableID,
		colAID,
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
		tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr(fmt.Sprintf("t%d", i)))
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
			"select count(*) from mysql.column_stats_usage where table_id=? and CONVERT_TZ(last_used_at, @@TIME_ZONE, '+00:00') = cast(? as datetime)",
			tid,
			bumpTSUTC,
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

func TestDumpStatsDeltaPersistsInitTime(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int)")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
	tk.MustExec("analyze table t")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableID := tbl.Meta().ID

	origMaxDuration := usage.GetDumpStatsMaxDurationForTest()
	origRatio := usage.DumpStatsDeltaRatio
	usage.SetDumpStatsMaxDurationForTest(50 * time.Millisecond)
	usage.DumpStatsDeltaRatio = 0.5
	t.Cleanup(func() {
		usage.SetDumpStatsMaxDurationForTest(origMaxDuration)
		usage.DumpStatsDeltaRatio = origRatio
	})

	tk.MustExec("insert into t values (11)")
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(false))
	tk.MustQuery("select modify_count from mysql.stats_meta where table_id = ?", tableID).
		Check(testkit.Rows("0"))

	time.Sleep(usage.GetDumpStatsMaxDurationForTest() + 100*time.Millisecond)

	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(false))
	tk.MustQuery("select modify_count from mysql.stats_meta where table_id = ?", tableID).
		Check(testkit.Rows("1"))
}

func TestDumpStatsDeltaMergeKeepsEarliestInitTime(t *testing.T) {
	store, dom := testkit.CreateMockStoreAndDomain(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	// Test setup:
	// 1) Create/analyze t and t_lock so stats_meta has baseline rows.
	// 2) Lock t_lock's stats_meta row with FOR UPDATE to block the first DumpStatsDeltaToKV.
	// 3) While blocked, write to t again and run a second dump to produce a later InitTime.
	// 4) Release the lock, wait past dumpStatsMaxDuration, then dump and verify the earlier InitTime triggers flushing.
	tk.MustExec("drop table if exists t")
	tk.MustExec("drop table if exists t_lock")
	tk.MustExec("create table t (a int)")
	tk.MustExec("create table t_lock (a int)")
	tk.MustExec("insert into t values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)")
	tk.MustExec("insert into t_lock values (1)")
	tk.MustExec("analyze table t")
	tk.MustExec("analyze table t_lock")

	is := dom.InfoSchema()
	tbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t"))
	require.NoError(t, err)
	tableID := tbl.Meta().ID
	lockTbl, err := is.TableByName(context.Background(), ast.NewCIStr("test"), ast.NewCIStr("t_lock"))
	require.NoError(t, err)
	lockTableID := lockTbl.Meta().ID

	require.NoError(t, dom.StatsHandle().Update(context.Background(), is))
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(true))
	tk.MustQuery("select count(*) from mysql.stats_meta where table_id = ?", lockTableID).
		Check(testkit.Rows("1"))

	origMaxDuration := usage.GetDumpStatsMaxDurationForTest()
	origRatio := usage.DumpStatsDeltaRatio
	usage.SetDumpStatsMaxDurationForTest(100 * time.Millisecond)
	usage.DumpStatsDeltaRatio = 0.5
	t.Cleanup(func() {
		usage.SetDumpStatsMaxDurationForTest(origMaxDuration)
		usage.DumpStatsDeltaRatio = origRatio
	})

	baseRows := tk.MustQuery("select modify_count from mysql.stats_meta where table_id = ?", tableID).Rows()
	require.Len(t, baseRows, 1)
	baseCount, err := strconv.Atoi(baseRows[0][0].(string))
	require.NoError(t, err)

	tk.MustExec("insert into t values (11)")
	tk.MustExec("insert into t_lock values (2),(3)")

	tkLock := testkit.NewTestKit(t, store)
	tkLock.MustExec("set @@tidb_txn_mode = 'pessimistic'")
	tkLock.MustExec("begin")
	tkLock.MustQuery("select * from mysql.stats_meta where table_id = ? for update", lockTableID)

	dump1Err := make(chan error, 1)
	dump1Start := time.Now()
	go func() {
		dump1Err <- dom.StatsHandle().DumpStatsDeltaToKV(false)
	}()

	time.Sleep(20 * time.Millisecond)
	select {
	case err := <-dump1Err:
		t.Fatalf("first dump finished early: %v", err)
	default:
	}

	time.Sleep(50 * time.Millisecond)
	tk.MustExec("insert into t values (12)")

	dump2Start := time.Now()
	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(false))

	afterSecond := tk.MustQuery("select modify_count from mysql.stats_meta where table_id = ?", tableID).Rows()
	require.Len(t, afterSecond, 1)
	afterSecondCount, err := strconv.Atoi(afterSecond[0][0].(string))
	require.NoError(t, err)
	require.Equal(t, baseCount, afterSecondCount)

	tkLock.MustExec("rollback")
	require.NoError(t, <-dump1Err)

	target := dump1Start.Add(usage.GetDumpStatsMaxDurationForTest() + 10*time.Millisecond)
	if wait := time.Until(target); wait > 0 {
		time.Sleep(wait)
	}
	require.True(t, time.Since(dump2Start) < usage.GetDumpStatsMaxDurationForTest())

	require.NoError(t, dom.StatsHandle().DumpStatsDeltaToKV(false))
	finalRows := tk.MustQuery("select modify_count from mysql.stats_meta where table_id = ?", tableID).Rows()
	require.Len(t, finalRows, 1)
	finalCount, err := strconv.Atoi(finalRows[0][0].(string))
	require.NoError(t, err)
	require.Equal(t, baseCount+2, finalCount)
}
