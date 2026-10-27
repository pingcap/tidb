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
	// slowQueryGroupCapacity is the fixed cap of every data node. The sentinel
	// head node keeps a zero-cap slice so it can be recognized as the head.
	slowQueryGroupCapacity = 1000
	// slowQueryCacheMaxMem is the budget for retained entries. Once an insert
	// would pass it, the ring stops growing and later inserts reuse slots.
	// Each entry is one SlowQuery value. The index map is not counted.
	slowQueryCacheMaxMem = 50 * 1024 * 1024
	// slowQueryCacheFixedMem is the admission limit after the ring is frozen.
	// Replacing a short entry with a longer one can raise usage above 50MiB, but
	// a single insert must not push retained memory past 60MiB.
	slowQueryCacheFixedMem = 60 * 1024 * 1024
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
type SlowQuery struct {
	Key           slowQueryKey
	UnredactedSQL string
	valid         bool
}

// SlowQueryGroup is one node in the circular slow-query list. Data nodes own a
// slice whose cap stays at slowQueryGroupCapacity. The head node's slice cap is
// 0 and exists only to mark the head of the ring.
type SlowQueryGroup struct {
	SlowQueries []SlowQuery
	Next        *SlowQueryGroup
}

// slowQueryCacheLocation is the write cursor and the map value. group and index
// together name one slot inside a SlowQueryGroup.
type slowQueryCacheLocation struct {
	group *SlowQueryGroup
	index int
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
// counted; the worker applies maxMem and fixedMem when it handles the write.
// Readers block until woken or their context ends. Non-diagnostic TiDB starts
// the worker in Server.Run. Read and write entrypoints check that lifecycle
// state before sending tasks; diagnostic mode neither starts the worker nor
// enqueues reads or writes.
type slowQueryCache struct {
	writeCh chan slowQueryCacheWriteTask
	readCh  chan *slowQueryCacheReadTask
	running bool

	head          *SlowQueryGroup
	tail          *SlowQueryGroup
	writePos      *slowQueryCacheLocation
	locations     map[slowQueryKey]slowQueryCacheLocation
	memoryUsage   int64
	capacityFixed bool

	groupCapacity int
	maxMem        int64
	fixedMem      int64
}

var globalSlowQueryCache = newSlowQueryCache()

func newSlowQueryCache() *slowQueryCache {
	return newSlowQueryCacheWithLimits(slowQueryGroupCapacity, slowQueryCacheMaxMem, slowQueryCacheFixedMem, slowQueryCacheChanSize)
}

func newSlowQueryCacheWithLimits(groupCapacity int, maxMem, fixedMem int64, chanSize int) *slowQueryCache {
	c := &slowQueryCache{
		writeCh:       make(chan slowQueryCacheWriteTask, chanSize),
		readCh:        make(chan *slowQueryCacheReadTask, chanSize),
		locations:     make(map[slowQueryKey]slowQueryCacheLocation),
		groupCapacity: groupCapacity,
		maxMem:        maxMem,
		fixedMem:      fixedMem,
	}
	c.head = &SlowQueryGroup{SlowQueries: make([]SlowQuery, 0)}
	// The first data node exists before any SQL arrives, so appends never see a
	// nil tail. writePos stays nil until the first SQL is stored.
	c.tail = &SlowQueryGroup{
		SlowQueries: make([]SlowQuery, 0, groupCapacity),
		Next:        c.head,
	}
	c.head.Next = c.tail
	return c
}

// StartSlowQueryCache starts the process-wide worker that retains plaintext
// slow-query SQL. Server.Run calls it before accepting connections, whether or
// not tidb_redact_log is enabled. That setting only decides whether SQL text is
// recorded and looked up. Extra calls do not start another worker.
// Diagnostic mode leaves the worker unstarted.
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
	for i, key := range task.keys {
		loc, ok := c.locations[key]
		if !ok || loc.group == nil || loc.index < 0 || loc.index >= len(loc.group.SlowQueries) {
			continue
		}

		query := &loc.group.SlowQueries[loc.index]
		if query.valid && query.Key == key {
			task.values[i] = query.UnredactedSQL
		}
	}
}

func (c *slowQueryCache) handleWrite(task slowQueryCacheWriteTask) {
	sqlSize := int64(len(task.sql))

	// Avoid too large sql
	if sqlSize == 0 || sqlSize > c.fixedMem {
		return
	}

	if !c.capacityFixed && c.memoryUsage+c.appendDelta(task.key, task.sql) <= c.maxMem {
		c.appendSlowQuery(task.key, task.sql)
		return
	}

	// Nothing has been stored, and this entry alone does not fit in 50MiB.
	if c.writePos == nil {
		return
	}

	delta := c.replacementDelta(c.writePos, task.key, task.sql)
	if c.memoryUsage+delta > c.fixedMem {
		return
	}

	// The ring is frozen from the first time an insert does not fit. Later
	// inserts keep checking memory, now against the 60MiB ceiling, and reuse
	// the oldest slot instead of allocating another group.
	c.capacityFixed = true
	c.overwrite(task.key, task.sql)
}

func (c *slowQueryCache) appendSlowQuery(key slowQueryKey, sql string) {
	if len(c.tail.SlowQueries) == cap(c.tail.SlowQueries) {
		group := &SlowQueryGroup{
			SlowQueries: make([]SlowQuery, 0, c.groupCapacity),
			Next:        c.head,
		}
		c.tail.Next = group
		c.tail = group
	}
	if c.writePos == nil {
		c.writePos = &slowQueryCacheLocation{group: c.tail, index: 0}
	}

	index := len(c.tail.SlowQueries)
	// A repeated key leaves the older slot in place until the ring reuses it.
	// The map always refers to the latest insert and is not part of memoryUsage.
	c.memoryUsage += c.appendDelta(key, sql)
	c.tail.SlowQueries = append(c.tail.SlowQueries, SlowQuery{Key: key, UnredactedSQL: sql, valid: true})
	c.locations[key] = slowQueryCacheLocation{group: c.tail, index: index}
}

func (c *slowQueryCache) overwrite(key slowQueryKey, sql string) {
	loc := c.writePos
	delta := c.replacementDelta(loc, key, sql)
	old := &loc.group.SlowQueries[loc.index]
	if old.valid {
		if oldLoc, ok := c.locations[old.Key]; ok && oldLoc == *loc {
			delete(c.locations, old.Key)
		}
	}

	*old = SlowQuery{Key: key, UnredactedSQL: sql, valid: true}
	c.locations[key] = *loc
	c.memoryUsage += delta
	c.advanceWritePos()
}

// appendDelta is the memory added by retaining one more SlowQuery.
func (*slowQueryCache) appendDelta(key slowQueryKey, sql string) int64 {
	return slowQueryValueMem(key, sql)
}

// replacementDelta is the retained SlowQuery memory change when a slot is
// overwritten. The index map is not included.
func (*slowQueryCache) replacementDelta(loc *slowQueryCacheLocation, key slowQueryKey, sql string) int64 {
	added := slowQueryValueMem(key, sql)
	old := &loc.group.SlowQueries[loc.index]
	if !old.valid {
		return added
	}
	return added - slowQueryValueMem(old.Key, old.UnredactedSQL)
}

// slowQueryValueMem is one retained SlowQuery: the struct, the SQL bytes, and
// the key's digest bytes. unsafe.Sizeof includes the key fields and string
// headers stored inline in the struct.
func slowQueryValueMem(key slowQueryKey, sql string) int64 {
	return slowQueryStructSize + int64(len(sql)) + int64(len(key.digest))
}

func (c *slowQueryCache) advanceWritePos() {
	loc := c.writePos
	next := loc.group
	idx := loc.index + 1
	if idx >= len(next.SlowQueries) {
		idx = 0
		next = next.Next
		if next == c.head {
			next = next.Next
		}
	}
	c.writePos = &slowQueryCacheLocation{group: next, index: idx}
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
