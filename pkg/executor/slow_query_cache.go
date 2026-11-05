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
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/config/diagnosticmode"
	"github.com/pingcap/tidb/pkg/util"
)

const (
	// slowQueryCacheCapacity is the number of entries retained by the cache.
	// The backing array is allocated when the cache is initialized. The slice
	// grows until this capacity is reached, then writes reuse its oldest slot.
	slowQueryCacheCapacity = 50000
	// slowQueryCacheMaxMem is the budget for entries the worker has retained.
	// Plaintext waiting on writeCh is not included. Once appending an entry
	// would pass the budget, the unused slots are abandoned and the ring stays
	// frozen. A later statement can reuse an existing slot; the replaced
	// entry's bytes are released when that write lands. The index map and the
	// preallocated backing array are not counted.
	slowQueryCacheMaxMem        = 50 * 1024 * 1024
	slowQueryCacheWriteChanSize = 10
	slowQueryCacheReadChanSize  = 100
)

// slowQueryKey identifies one slow query in the plaintext cache. Digest is
// shortened to 10 characters. The three fields are the same ones written to
// the slow log, so a later read can find the row again.
type slowQueryKey struct {
	txnStartTS uint64
	queryTime  float64
	digest     string
}

// slowQuery is one cached slow query. Only the unredacted SQL is retained;
// the key is kept so the slot can be removed from the index when it is reused.
// Every element of slowQueries is occupied. A slot that no longer holds its
// original key is identified by comparing Key with the lookup key.
type slowQuery struct {
	Key           slowQueryKey
	UnredactedSQL string
}

type slowQueryCacheWriteTask struct {
	key slowQueryKey
	sql string
}

// slowQueryCacheReadTask is one batch lookup. values is filled by the cache
// worker and done wakes the reader after that fill finishes.
type slowQueryCacheReadTask struct {
	keys   []slowQueryKey
	values []string
	done   chan struct{}
}

// slowQueryCache is owned by one background goroutine. Callers only send tasks.
// memoryUsage is updated only by that goroutine. A producer drops a statement
// when writeCh is full, without consulting the memory budget. Readers block
// until woken or their context ends. Non-diagnostic TiDB starts the worker in
// Server.Run. Read and write entrypoints check that lifecycle state before
// sending tasks; diagnostic mode neither starts the worker nor enqueues reads
// or writes.
type slowQueryCache struct {
	writeCh chan slowQueryCacheWriteTask
	readCh  chan *slowQueryCacheReadTask
	running bool
	// stopCh is closed by stop after the test has finished sending. exited is
	// closed by the worker after that loop returns. Production never closes
	// stopCh. Both stay nil until start.
	stopCh chan struct{}
	exited chan struct{}

	slowQueries []slowQuery
	writePos    int
	// frozen stays set after the first insert that cannot be appended. A later
	// shorter replacement may free memory, but growing again would leave
	// writePos behind the chronological order of the slots.
	frozen      bool
	locations   map[slowQueryKey]int
	memoryUsage int64
	maxMemUsage int64
}

var globalSlowQueryCache = newSlowQueryCache()

func newSlowQueryCache() *slowQueryCache {
	return newSlowQueryCacheWithLimits(slowQueryCacheCapacity, slowQueryCacheMaxMem, slowQueryCacheWriteChanSize)
}

func newSlowQueryCacheWithLimits(capacity int, maxMem int64, writeChanSize int) *slowQueryCache {
	if capacity < 0 {
		capacity = 0
	}
	return &slowQueryCache{
		writeCh:     make(chan slowQueryCacheWriteTask, writeChanSize),
		readCh:      make(chan *slowQueryCacheReadTask, slowQueryCacheReadChanSize),
		slowQueries: make([]slowQuery, 0, capacity),
		locations:   make(map[slowQueryKey]int),
		maxMemUsage: maxMem,
	}
}

// StartSlowQueryCache starts the process-wide worker that retains plaintext
// slow-query SQL. Server.Run calls it before accepting connections, whether or
// not tidb_redact_log is enabled. That setting only decides whether a slow
// query is inserted. Lookups always read the cache. Extra calls do not start
// another worker. Diagnostic mode leaves the worker unstarted.
func StartSlowQueryCache() {
	globalSlowQueryCache.start()
}

func (c *slowQueryCache) start() {
	if diagnosticmode.Enabled() {
		return
	}
	if c.checkRunning() {
		return
	}
	c.stopCh = make(chan struct{})
	c.exited = make(chan struct{})
	c.running = true
	go c.run()
}

// stop closes the worker and waits until it returns. Call it only after this
// cache's sends have finished, from the same goroutine that started it. Tests
// do that in Cleanup, so the worker observes a closed channel instead of a
// shared flag.
func (c *slowQueryCache) stop() {
	if !c.running {
		return
	}
	close(c.stopCh)
	<-c.exited
	c.running = false
}

func (c *slowQueryCache) checkRunning() bool {
	return c.running
}

func (c *slowQueryCache) run() {
	// One goroutine owns the ring and the map. Restart it in this same
	// goroutine after a recovered panic so a reader waiting on a task cannot
	// block forever. util.Recover re-panics on "assert failed" in tests; catch
	// that here so this goroutine still comes back. A closed stopCh ends the
	// worker instead of restarting it.
	defer close(c.exited)
	for {
		c.recoverLoop()
		select {
		case <-c.stopCh:
			return
		default:
		}
	}
}

func (c *slowQueryCache) recoverLoop() {
	defer func() { _ = recover() }()
	c.loop()
}

func (c *slowQueryCache) loop() {
	defer util.Recover(metricsLabelSlowQueryCache, "slowQueryCache", nil, false)
	for {
		// Reads already waiting go first. Writes stay queued until the read
		// queue is empty.
		if task, ok := c.pollRead(); ok {
			c.handleRead(task)
			continue
		}
		select {
		case <-c.stopCh:
			return
		case task := <-c.readCh:
			c.handleRead(task)
		case task := <-c.writeCh:
			// Priority of read task is higher
			if readTask, ok := c.pollRead(); ok {
				c.handleRead(readTask)
			}
			c.handleWrite(task)
		}
	}
}

const metricsLabelSlowQueryCache = "executor"

func (c *slowQueryCache) pollRead() (*slowQueryCacheReadTask, bool) {
	select {
	case task := <-c.readCh:
		return task, true
	default:
		return nil, false
	}
}

func (c *slowQueryCache) putIntoCache(key slowQueryKey, sql string) {
	if sql == "" || diagnosticmode.Enabled() || !c.checkRunning() {
		return
	}
	c.tryToCache(slowQueryCacheWriteTask{key: key, sql: sql})
}

// tryToCache reports whether the write was queued. A full writeCh drops the
// statement. The memory budget is applied later, by the worker.
func (c *slowQueryCache) tryToCache(task slowQueryCacheWriteTask) bool {
	if task.sql == "" {
		return false
	}
	select {
	case c.writeCh <- task:
		return true
	default:
		return false
	}
}

func (c *slowQueryCache) getUnredactedQueries(ctx context.Context, keys []slowQueryKey) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if len(keys) == 0 || diagnosticmode.Enabled() || !c.checkRunning() {
		return nil, nil
	}

	// Share the caller's keys. handleRead only reads them, and the caller does
	// not modify the slice while the task is queued. On cancel the task keeps
	// the slice until the worker finishes.
	task := &slowQueryCacheReadTask{
		keys:   keys,
		values: make([]string, len(keys)),
		done:   make(chan struct{}),
	}

	select {
	case c.readCh <- task:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	select {
	case <-task.done:
		return task.values, nil
	case <-ctx.Done():
		// Prefer a result that is already waiting. Otherwise leave the task
		// for the worker and let this query return.
		select {
		case <-task.done:
			return task.values, nil
		default:
			return nil, ctx.Err()
		}
	}
}

func (c *slowQueryCache) handleRead(task *slowQueryCacheReadTask) {
	defer close(task.done)
	if len(c.slowQueries) == 0 {
		return
	}
	for i, key := range task.keys {
		index, ok := c.locations[key]
		if !ok || index < 0 || index >= len(c.slowQueries) {
			continue
		}

		query := &c.slowQueries[index]
		if query.Key == key {
			task.values[i] = query.UnredactedSQL
		}
	}
}

func (c *slowQueryCache) handleWrite(task slowQueryCacheWriteTask) {
	entryMemUsage := getSlowQueryValueMemUsage(task.key, task.sql)
	if task.sql == "" || entryMemUsage > c.maxMemUsage {
		return
	}

	if !c.frozen && len(c.slowQueries) < cap(c.slowQueries) && c.memoryUsage+entryMemUsage <= c.maxMemUsage {
		c.memoryUsage += entryMemUsage
		c.appendSlowQuery(task.key, task.sql)
		return
	}

	// Nothing has been stored, or this cache has no slots. An empty ring is not
	// frozen, so a later smaller SQL can still become the first entry.
	if len(c.slowQueries) == 0 {
		return
	}
	// Stay frozen from here, including when this replacement is discarded. A
	// later SQL that fits in the remaining budget reuses writePos instead of
	// appending.
	c.frozen = true

	delta := c.getReplacementDelta(c.writePos, task.key, task.sql)
	if c.memoryUsage+delta > c.maxMemUsage {
		return
	}
	c.memoryUsage += delta
	c.overwrite(task.key, task.sql)
}

func (c *slowQueryCache) appendSlowQuery(key slowQueryKey, sql string) {
	// A repeated key leaves the older slot in place until the ring reuses it.
	// The map always refers to the latest insert and is not part of memoryUsage.
	// handleWrite has already charged this statement.
	c.slowQueries = append(c.slowQueries, slowQuery{Key: key, UnredactedSQL: sql})
	c.locations[key] = len(c.slowQueries) - 1
}

func (c *slowQueryCache) overwrite(key slowQueryKey, sql string) {
	index := c.writePos
	old := &c.slowQueries[index]
	// A duplicate key keeps the older copy until this slot is reused. Drop the
	// index entry only when it still names this slot.
	if oldIndex, ok := c.locations[old.Key]; ok && oldIndex == index {
		delete(c.locations, old.Key)
	}

	*old = slowQuery{Key: key, UnredactedSQL: sql}
	c.locations[key] = index
	c.advanceWritePos()
}

// getReplacementDelta is the retained SlowQuery memory change when a slot is
// overwritten. The index map is not included.
func (c *slowQueryCache) getReplacementDelta(index int, key slowQueryKey, sql string) int64 {
	old := &c.slowQueries[index]
	return getSlowQueryValueMemUsage(key, sql) - getSlowQueryValueMemUsage(old.Key, old.UnredactedSQL)
}

// getSlowQueryValueMemUsage is one retained SlowQuery: the struct, the SQL bytes, and
// the key's digest bytes. unsafe.Sizeof includes the key fields and string
// headers stored inline in the struct.
func getSlowQueryValueMemUsage(key slowQueryKey, sql string) int64 {
	return slowQueryStructSize + int64(len(sql)) + int64(len(key.digest))
}

func (c *slowQueryCache) advanceWritePos() {
	c.writePos++
	if c.writePos >= len(c.slowQueries) {
		c.writePos = 0
	}
}

var slowQueryStructSize = int64(unsafe.Sizeof(slowQuery{}))

func slowQueryKeyOf(txnStartTS uint64, queryTime float64, digest string) slowQueryKey {
	if len(digest) > 10 {
		digest = digest[:10]
	}
	return slowQueryKey{txnStartTS: txnStartTS, queryTime: queryTime, digest: digest}
}

// slowQueryTimeKey converts a duration to the Query_time value written in the
// slow log, so a cache lookup parsed back from that field hits the same key.
// The log formats seconds with strconv.FormatFloat precision -1, which parses
// back to the same float64, so the write path does not format the value again.
func slowQueryTimeKey(queryTime time.Duration) float64 {
	return queryTime.Seconds()
}
