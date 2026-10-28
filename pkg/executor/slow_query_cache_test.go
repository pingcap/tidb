// Copyright 2026 PingCAP, Inc.
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

package executor

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/require"
)

func TestSlowQueryCacheReadWrite(t *testing.T) {
	cache := newSlowQueryCache()
	key := slowQueryKeyOf(123, 1.25, "0123456789abcdef")
	cache.handleWrite(slowQueryCacheWriteTask{key: key, sql: "select 123;"})

	result := &slowQueryCacheReadTask{
		keys:   []slowQueryKey{key, slowQueryKeyOf(456, 2.5, "another")},
		values: make([]string, 2),
		done:   make(chan struct{}),
	}
	cache.handleRead(result)

	require.Equal(t, []string{"select 123;", ""}, result.values)
	require.Equal(t, "0123456789", key.digest)
}

func TestSlowQueryCacheDuplicateKeyUsesLatestValue(t *testing.T) {
	cache := newSlowQueryCache()
	key := slowQueryKeyOf(123, 1.25, "0123456789abcdef")
	cache.handleWrite(slowQueryCacheWriteTask{key: key, sql: "select 1;"})
	cache.handleWrite(slowQueryCacheWriteTask{key: key, sql: "select 2;"})

	result := &slowQueryCacheReadTask{
		keys:   []slowQueryKey{key},
		values: make([]string, 1),
		done:   make(chan struct{}),
	}
	cache.handleRead(result)
	require.Equal(t, []string{"select 2;"}, result.values)
	// Both values stay. The index map is not charged.
	require.Equal(t, getSlowQueryValueMemUsage(key, "select 1;")+getSlowQueryValueMemUsage(key, "select 2;"), cache.memoryUsage)
}

func TestSlowQueryCacheUsesPreallocatedRing(t *testing.T) {
	cache := newSlowQueryCache()
	require.Zero(t, len(cache.slowQueries))
	require.Equal(t, slowQueryCacheCapacity, cap(cache.slowQueries))
	require.Zero(t, cache.writePos)
	for i := 0; i < slowQueryCacheCapacity; i++ {
		key := slowQueryKeyOf(uint64(i), 1, strconv.Itoa(i))
		cache.handleWrite(slowQueryCacheWriteTask{key: key, sql: "x"})
	}

	require.Len(t, cache.slowQueries, slowQueryCacheCapacity)
	require.Equal(t, 0, cache.writePos)
	require.Equal(t, "x", lookupSlowQueryCache(cache, slowQueryKeyOf(0, 1, "0"))[0])

	newKey := slowQueryKeyOf(slowQueryCacheCapacity, 1, strconv.Itoa(slowQueryCacheCapacity))
	cache.handleWrite(slowQueryCacheWriteTask{key: newKey, sql: "new"})
	require.Equal(t, "", lookupSlowQueryCache(cache, slowQueryKeyOf(0, 1, "0"))[0])
	require.Equal(t, "new", lookupSlowQueryCache(cache, newKey)[0])
	require.Equal(t, 1, cache.writePos)
}

func TestSlowQueryCacheEvictsOldestAndFreezesRing(t *testing.T) {
	first := slowQueryKeyOf(1, 1, "first-key")
	second := slowQueryKeyOf(2, 1, "second-key")
	third := slowQueryKeyOf(3, 1, "third-key")
	droppedKey := slowQueryKeyOf(4, 1, "dropped")
	firstSQL, secondSQL, thirdSQL := "12345", "12345", "123456"
	droppedSQL := "123456789012"
	twoEntries := getSlowQueryValueMemUsage(first, firstSQL) + getSlowQueryValueMemUsage(second, secondSQL)
	afterThird := getSlowQueryValueMemUsage(second, secondSQL) + getSlowQueryValueMemUsage(third, thirdSQL)
	afterDropped := getSlowQueryValueMemUsage(third, thirdSQL) + getSlowQueryValueMemUsage(droppedKey, droppedSQL)
	require.Greater(t, afterDropped, afterThird)

	cache := newSlowQueryCacheWithLimits(4, afterThird, 8)
	cache.handleWrite(slowQueryCacheWriteTask{key: first, sql: firstSQL})
	cache.handleWrite(slowQueryCacheWriteTask{key: second, sql: secondSQL})
	require.Equal(t, twoEntries, cache.memoryUsage)

	cache.handleWrite(slowQueryCacheWriteTask{key: third, sql: thirdSQL})
	require.Equal(t, afterThird, cache.memoryUsage)
	// The two unused slots are abandoned once the memory budget is reached.
	require.Len(t, cache.slowQueries, 2)
	require.Equal(t, 4, cap(cache.slowQueries))
	require.Equal(t, []string{"", secondSQL, thirdSQL}, lookupSlowQueryCache(cache, first, second, third))

	// Replacing the next slot would pass the frozen ceiling, so the SQL is discarded.
	before := cache.memoryUsage
	pos := cache.writePos
	cache.handleWrite(slowQueryCacheWriteTask{key: droppedKey, sql: droppedSQL})
	require.Equal(t, before, cache.memoryUsage)
	require.Equal(t, pos, cache.writePos)
	require.Equal(t, []string{secondSQL}, lookupSlowQueryCache(cache, second))
}

func TestSlowQueryCacheDuplicateKeySurvivesEvictionOfOlderCopy(t *testing.T) {
	key := slowQueryKeyOf(1, 1, "0123456789zzz")
	other := slowQueryKeyOf(2, 1, "other-digest")
	olderSQL, latestSQL, otherSQL := "0123456789", "abcdefghij", "12345"
	twoCopies := getSlowQueryValueMemUsage(key, olderSQL) + getSlowQueryValueMemUsage(key, latestSQL)
	afterOther := getSlowQueryValueMemUsage(key, latestSQL) + getSlowQueryValueMemUsage(other, otherSQL)
	cache := newSlowQueryCacheWithLimits(4, twoCopies, 8)
	cache.handleWrite(slowQueryCacheWriteTask{key: key, sql: olderSQL})
	cache.handleWrite(slowQueryCacheWriteTask{key: key, sql: latestSQL})
	cache.handleWrite(slowQueryCacheWriteTask{key: other, sql: otherSQL})

	require.Equal(t, []string{latestSQL, otherSQL}, lookupSlowQueryCache(cache, key, other))
	require.Equal(t, afterOther, cache.memoryUsage)

	replacement := slowQueryKeyOf(3, 1, "replacement")
	cache.handleWrite(slowQueryCacheWriteTask{key: replacement, sql: otherSQL})
	require.Equal(t, []string{"", otherSQL, otherSQL}, lookupSlowQueryCache(cache, key, other, replacement))
	require.Equal(t, getSlowQueryValueMemUsage(other, otherSQL)+getSlowQueryValueMemUsage(replacement, otherSQL), cache.memoryUsage)
}

func TestSlowQueryCacheDropsOversizedAndUnqueuedSQL(t *testing.T) {
	fitsKey := slowQueryKeyOf(2, 1, "fits")
	fitsSQL := "12345"
	fitsMem := getSlowQueryValueMemUsage(fitsKey, fitsSQL)
	hugeKey := slowQueryKeyOf(1, 1, "huge")
	hugeSQL := strings.Repeat("a", 11)
	require.Greater(t, getSlowQueryValueMemUsage(hugeKey, hugeSQL), fitsMem)

	cache := newSlowQueryCacheWithLimits(4, fitsMem, 1)
	cache.handleWrite(slowQueryCacheWriteTask{key: hugeKey, sql: hugeSQL})
	require.Zero(t, cache.memoryUsage)
	require.Zero(t, cache.writePos)

	cache.handleWrite(slowQueryCacheWriteTask{key: fitsKey, sql: fitsSQL})
	require.Equal(t, fitsMem, cache.memoryUsage)

	// The worker, not the producer, drops SQL larger than maxMem. Until then
	// the text only occupies the channel and memoryUsage stays unchanged.
	require.True(t, cache.tryToCache(slowQueryCacheWriteTask{key: slowQueryKeyOf(3, 1, "huge"), sql: strings.Repeat("a", 17)}))
	require.Equal(t, fitsMem, cache.memoryUsage)
	require.False(t, cache.tryToCache(slowQueryCacheWriteTask{key: slowQueryKeyOf(4, 1, "queued"), sql: "one"}))
	require.Equal(t, 1, len(cache.writeCh))

	cache.start()
	require.Eventually(t, func() bool {
		got, err := cache.getUnredactedQueries(context.Background(), []slowQueryKey{
			slowQueryKeyOf(2, 1, "fits"),
			slowQueryKeyOf(3, 1, "huge"),
		})
		return err == nil && len(cache.writeCh) == 0 && len(got) == 2 && got[0] == "12345" && got[1] == ""
	}, time.Second, 5*time.Millisecond)
}

func TestSlowQueryCacheAccountsMemoryOnWorker(t *testing.T) {
	first := slowQueryKeyOf(1, 1, "queued")
	second := slowQueryKeyOf(2, 1, "also-queued")
	firstSQL := strings.Repeat("a", 10)
	secondSQL := strings.Repeat("b", 10)
	firstMem := getSlowQueryValueMemUsage(first, firstSQL)
	secondMem := getSlowQueryValueMemUsage(second, secondSQL)
	cache := newSlowQueryCacheWithLimits(4, secondMem, 8)
	require.True(t, cache.tryToCache(slowQueryCacheWriteTask{key: first, sql: firstSQL}))
	require.True(t, cache.tryToCache(slowQueryCacheWriteTask{key: second, sql: secondSQL}))
	require.Equal(t, 2, len(cache.writeCh))
	require.Zero(t, cache.memoryUsage)

	cache.start()
	require.Eventually(t, func() bool {
		got, err := cache.getUnredactedQueries(context.Background(), []slowQueryKey{first, second})
		// The second insert freezes the ring and reuses the only slot.
		return err == nil && len(got) == 2 && got[0] == "" && got[1] == secondSQL
	}, time.Second, 5*time.Millisecond)

	// The SQL can sit in the channel. The worker drops it because the ring is
	// empty and the SlowQuery plus key exceeds maxMem.
	droppedKey := slowQueryKeyOf(3, 1, "dropped-later")
	droppedSQL := strings.Repeat("c", 12)
	require.Greater(t, getSlowQueryValueMemUsage(droppedKey, droppedSQL), firstMem)
	dropped := newSlowQueryCacheWithLimits(4, firstMem, 4)
	require.True(t, dropped.tryToCache(slowQueryCacheWriteTask{
		key: droppedKey,
		sql: droppedSQL,
	}))
	require.Zero(t, dropped.memoryUsage)
	dropped.start()
	require.Eventually(t, func() bool {
		got, err := dropped.getUnredactedQueries(context.Background(), []slowQueryKey{droppedKey})
		return err == nil && len(dropped.writeCh) == 0 && len(got) == 1 && got[0] == ""
	}, time.Second, 5*time.Millisecond)
}

func TestSlowQueryCacheDiagnosticModeDoesNotStartOrEnqueue(t *testing.T) {
	if !intest.InTest {
		t.Skip("diagnosticmode.SetForTest requires the intest build tag")
	}
	normal := newSlowQueryCache()
	normal.start()
	require.True(t, normal.running)

	restore := diagnosticmode.SetForTest(true)
	t.Cleanup(restore)

	cache := newSlowQueryCache()
	cache.start()
	require.False(t, cache.running)

	key := slowQueryKeyOf(1, 1, "diag")
	cache.putIntoCache(key, "select 1;")
	require.False(t, cache.running)
	require.Zero(t, len(cache.writeCh))

	got, err := cache.getUnredactedQueries(context.Background(), []slowQueryKey{key})
	require.NoError(t, err)
	require.Nil(t, got)
	require.False(t, cache.running)
	require.Zero(t, len(cache.readCh))

	// dataForSlowLog must skip the lookup. The column keeps the parsed value.
	row := []types.Datum{types.NewStringDatum("from-log")}
	retriever := &slowQueryRetriever{
		inMemoryOriginalQueryIndex: 0,
		taskList:                   make(chan slowLogTask, 1),
	}
	resultCh := make(chan parsedSlowLog, 1)
	resultCh <- parsedSlowLog{rows: [][]types.Datum{row}, keys: []slowQueryKey{key}}
	retriever.taskList <- slowLogTask{resultCh: resultCh}
	rows, err := retriever.dataForSlowLog(context.Background())
	require.NoError(t, err)
	require.Equal(t, "from-log", rows[0][0].GetString())
}

func TestSlowQueryCacheLookupRespectsContext(t *testing.T) {
	cache := newSlowQueryCache()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	values, err := cache.getUnredactedQueries(ctx, []slowQueryKey{slowQueryKeyOf(1, 1, "cancelled")})
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, values)
	require.False(t, cache.running)
}

func TestSlowQueryCacheAsyncReadWrite(t *testing.T) {
	cache := newSlowQueryCache()
	cache.start()
	// An empty cache returns without queueing a read.
	missing := slowQueryKeyOf(1, 1, "missing")
	got, err := cache.getUnredactedQueries(context.Background(), []slowQueryKey{missing})
	require.NoError(t, err)
	require.Equal(t, []string{""}, got)
	require.Zero(t, len(cache.readCh))
	require.False(t, cache.hasSQL.Load())

	key := slowQueryKeyOf(7, 1.5, "0123456789abcdef")
	cache.putIntoCache(key, "select 1;")
	require.Eventually(t, func() bool {
		got, err := cache.getUnredactedQueries(context.Background(), []slowQueryKey{key})
		return err == nil && len(got) == 1 && got[0] == "select 1;"
	}, time.Second, 5*time.Millisecond)
}

func TestSlowQueryCacheEntrypointsWithoutWorker(t *testing.T) {
	cache := newSlowQueryCache()
	key := slowQueryKeyOf(1, 1, "missing")
	cache.putIntoCache(key, "select 1;")
	require.False(t, cache.running)
	require.Zero(t, len(cache.writeCh))

	values, err := cache.getUnredactedQueries(context.Background(), []slowQueryKey{key})
	require.NoError(t, err)
	require.Equal(t, []string{""}, values)
	require.False(t, cache.running)

	// running is set without a worker. A missing empty-cache check would enqueue
	// the read and then wait until the context ends.
	cache.running = true
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	values, err = cache.getUnredactedQueries(ctx, []slowQueryKey{key})
	require.NoError(t, err)
	require.Equal(t, []string{""}, values)
	require.Zero(t, len(cache.readCh))
	require.False(t, cache.hasSQL.Load())
}

func TestSlowQueryTimeKeyMatchesSlowLogFormat(t *testing.T) {
	queryTime := 123456789 * time.Nanosecond
	formatted := strconv.FormatFloat(queryTime.Seconds(), 'f', -1, 64)
	parsed, err := strconv.ParseFloat(formatted, 64)
	require.NoError(t, err)
	require.Equal(t, parsed, slowQueryTimeKey(queryTime))
}

func lookupSlowQueryCache(cache *slowQueryCache, keys ...slowQueryKey) []string {
	task := &slowQueryCacheReadTask{
		keys:   keys,
		values: make([]string, len(keys)),
		done:   make(chan struct{}),
	}
	cache.handleRead(task)
	return task.values
}
