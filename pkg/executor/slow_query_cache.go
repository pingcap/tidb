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
	// slowQueryCacheMaxMem is the budget for retained entries. Once appending an
	// entry would pass it, the unused slots are abandoned and later writes reuse
	// existing slots. The index map and the preallocated backing array are not
	// counted.
	slowQueryCacheMaxMem   = 50 * 1024 * 1024
	slowQueryCacheChanSize = 1000
)

// slowQueryKey identifies one slow query in the plaintext cache. Digest is
// shortened to 10 characters. The three fields are the same ones written to
// the slow log, so a later read can find the row again.
type slowQueryKey struct {
	txnStartTS uint64
	queryTime  float64
	digest     string
}

// SlowQuery is one cached slow query. Only the unredacted SQL is retained;
// the key is kept so the slot can be removed from the index when it is reused.
// Every element of slowQueries is occupied. A slot that no longer holds its
// original key is identified by comparing Key with the lookup key.
type SlowQuery struct {
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
// A full write queue drops the entry. Plaintext waiting in writeCh is not
// counted; the worker applies maxMem when it handles the write. Readers block
// until woken or their context ends. Non-diagnostic TiDB starts the worker in
// Server.Run. Read and write entrypoints check that lifecycle state before
// sending tasks; diagnostic mode neither starts the worker nor enqueues reads
// or writes.
type slowQueryCache struct {
	writeCh chan slowQueryCacheWriteTask
	readCh  chan *slowQueryCacheReadTask
	running bool

	slowQueries []SlowQuery
	writePos    int
	locations   map[slowQueryKey]int
	memoryUsage int64
	maxMemUsage int64
}

var globalSlowQueryCache = newSlowQueryCache()

func newSlowQueryCache() *slowQueryCache {
	return newSlowQueryCacheWithLimits(slowQueryCacheCapacity, slowQueryCacheMaxMem, slowQueryCacheChanSize)
}

func newSlowQueryCacheWithLimits(capacity int, maxMem int64, chanSize int) *slowQueryCache {
	if capacity < 0 {
		capacity = 0
	}
	return &slowQueryCache{
		writeCh:     make(chan slowQueryCacheWriteTask, chanSize),
		readCh:      make(chan *slowQueryCacheReadTask, chanSize),
		slowQueries: make([]SlowQuery, 0, capacity),
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
	c.running = true
	go c.run()
}

func (c *slowQueryCache) checkRunning() bool {
	return c.running
}

func (c *slowQueryCache) run() {
	// One goroutine owns the ring and the map. Restart it in this same
	// goroutine after a recovered panic so a reader waiting on a task cannot
	// block forever. util.Recover re-panics on "assert failed" in tests; catch
	// that here so this goroutine still comes back.
	for {
		c.recoverLoop()
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

// tryToCache reports whether the write was queued. A full queue drops the SQL.
// Size limits are applied later by the worker.
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
	if len(keys) == 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if diagnosticmode.Enabled() {
		return nil, nil
	}
	if !c.checkRunning() {
		return make([]string, len(keys)), nil
	}

	task := &slowQueryCacheReadTask{
		keys:   append([]slowQueryKey(nil), keys...),
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

	if len(c.slowQueries) < cap(c.slowQueries) && c.memoryUsage+entryMemUsage <= c.maxMemUsage {
		c.appendSlowQuery(task.key, task.sql)
		return
	}

	// Nothing has been stored, or this cache has no slots.
	if len(c.slowQueries) == 0 {
		return
	}

	delta := c.getReplacementDelta(c.writePos, task.key, task.sql)
	if c.memoryUsage+delta > c.maxMemUsage {
		return
	}

	// The ring is frozen from the first time an insert does not fit in the
	// memory budget or after all slots are used. Later inserts reuse the oldest
	// retained entry.
	c.overwrite(task.key, task.sql)
}

func (c *slowQueryCache) appendSlowQuery(key slowQueryKey, sql string) {
	// A repeated key leaves the older slot in place until the ring reuses it.
	// The map always refers to the latest insert and is not part of memoryUsage.
	c.memoryUsage += getSlowQueryValueMemUsage(key, sql)
	c.slowQueries = append(c.slowQueries, SlowQuery{Key: key, UnredactedSQL: sql})
	c.locations[key] = len(c.slowQueries) - 1
}

func (c *slowQueryCache) overwrite(key slowQueryKey, sql string) {
	index := c.writePos
	delta := c.getReplacementDelta(index, key, sql)
	old := &c.slowQueries[index]
	// A duplicate key keeps the older copy until this slot is reused. Drop the
	// index entry only when it still names this slot.
	if oldIndex, ok := c.locations[old.Key]; ok && oldIndex == index {
		delete(c.locations, old.Key)
	}

	*old = SlowQuery{Key: key, UnredactedSQL: sql}
	c.locations[key] = index
	c.memoryUsage += delta
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

var slowQueryStructSize = int64(unsafe.Sizeof(SlowQuery{}))

func slowQueryKeyOf(txnStartTS uint64, queryTime float64, digest string) slowQueryKey {
	if len(digest) > 10 {
		digest = digest[:10]
	}
	return slowQueryKey{txnStartTS: txnStartTS, queryTime: queryTime, digest: digest}
}

// slowQueryTimeKey converts a duration to the Query_time value written in the
// slow log, so a cache lookup parsed back from that field hits the same key.
func slowQueryTimeKey(queryTime time.Duration) float64 {
	seconds := queryTime.Seconds()
	parsed, err := strconv.ParseFloat(strconv.FormatFloat(seconds, 'f', -1, 64), 64)
	if err != nil {
		return seconds
	}
	return parsed
}
