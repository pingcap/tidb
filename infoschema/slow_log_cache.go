// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package infoschema

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	defSlowQueryBufferSize = 500000
	slowQueryBufferSize    = defSlowQueryBufferSize
)

var globalSlowQueryReader slowLogReader

// slowLogReader uses to read slow log data from slow log file.
// It will cache the data in a ring buffer to avoid repeated read file and parse data.
type slowLogReader struct {
	sync.RWMutex
	// TODO: support adjust cache size.
	cache *slowLogCache
}

// getSlowLogData uses to get slow log data.
// It will try to get from cache data first.
// If cache not match, it will get data by parse slow log file, and then update the cache by async.
func (s *slowLogReader) getSlowLogData(filePath string, tz *time.Location) ([][]types.Datum, error) {
	if s.cacheValid(filePath) {
		// try read with buffer.
		return s.getSlowLogDataWithCache(tz)

	}
	return s.parseSlowLogFileAndUpdateCache(tz, filePath, 0)
}

// getSlowLogDataWithCache gets data from cache and get the remain data by parse slow log file.
// It will update cache if has new data.
func (s *slowLogReader) getSlowLogDataWithCache(tz *time.Location) ([][]types.Datum, error) {
	s.RLock()
	// Get before cached tuples.
	beforeCacheTuples, hasTruncate, err := s.parseSlowLogFileBeforeCachedAndCheckTruncate(tz)
	if err != nil {
		s.RUnlock()
		return nil, err
	}
	// Try to avoid reallocate memory.
	rows := make([][]types.Datum, len(beforeCacheTuples), s.cache.buf.len()+len(beforeCacheTuples)+100)
	// get all cache tuples.
	rows = s.getSlowLogRowsFromCache(tz, rows)
	cacheEndTime := s.cache.getEndTime()
	readOffsetAfterCache := s.cache.getEndOffset()
	s.RUnlock()

	if hasTruncate {
		readOffsetAfterCache = 0
	}
	// Get after cached tuples. Put this out lock is for reduce lock time.
	afterCacheTuples, err := s.parseSlowLogFileAfterCached(tz, cacheEndTime, readOffsetAfterCache)
	if err != nil {
		return nil, err
	}
	logutil.Logger(context.Background()).Info("slow query read data with cache", zap.Int("before cached", len(beforeCacheTuples)), zap.Int("cached", len(rows)), zap.Int("after cached", len(afterCacheTuples)))

	// Fill before cached tuples.
	for i := range beforeCacheTuples {
		rows[i] = beforeCacheTuples[i].convertToDatumRow()
	}
	// Append after cache tuples.
	for i := range afterCacheTuples {
		rows = append(rows, afterCacheTuples[i].convertToDatumRow())
	}
	// Update cache.
	go s.updateCache(s.cache.filePath, afterCacheTuples)
	return rows, nil
}

// parseSlowLogFileAndUpdateCache gets data by parse the slow log file, and then update the cache.
func (s *slowLogReader) parseSlowLogFileAndUpdateCache(tz *time.Location, filePath string, offset int64) ([][]types.Datum, error) {
	tuples, err := parseSlowLogDataFromFile(tz, filePath, offset, nil, nil)
	if err != nil {
		return nil, err
	}
	go s.updateCache(filePath, tuples)
	return convertSlowLogTuplesToDatums(tuples), nil
}

// parseSlowLogFileBeforeCachedAndCheckTruncate parse slow log file data which before cached start time
// and check the log file has been truncated.
func (s *slowLogReader) parseSlowLogFileBeforeCachedAndCheckTruncate(tz *time.Location) ([]*slowQueryTuple, bool, error) {
	cacheStartTime := s.cache.getStartTime()
	first := true
	hasTruncate := false
	beforeCacheTuples, err := parseSlowLogDataFromFile(tz, s.cache.filePath, 0, func(t time.Time) bool {
		if first {
			first = false
			hasTruncate = t.After(cacheStartTime)
		}
		return t.Before(cacheStartTime)
	}, nil)
	return beforeCacheTuples, hasTruncate, err
}

// getSlowLogRowsFromCache gets slow log rows from cache.
func (s *slowLogReader) getSlowLogRowsFromCache(tz *time.Location, rows [][]types.Datum) [][]types.Datum {
	s.cache.buf.iterate(func(d interface{}) bool {
		tuple := d.(*slowQueryTuple)
		rows = append(rows, tuple.convertToDatumRow())
		return false
	})
	return rows
}

// parseSlowLogFileAfterCached parse slow log file data which after cached end time.
func (s *slowLogReader) parseSlowLogFileAfterCached(tz *time.Location, cacheEndTime time.Time, offset int64) ([]*slowQueryTuple, error) {
	afterCacheTuples, err := parseSlowLogDataFromFile(tz, s.cache.filePath, offset, nil, func(t time.Time) bool {
		return t.Before(cacheEndTime) || t.Equal(cacheEndTime)
	})
	if err != nil {
		return nil, err
	}
	return afterCacheTuples, err
}

func (s *slowLogReader) updateCache(filePath string, tuples []*slowQueryTuple) {
	s.Lock()
	defer s.Unlock()
	if s.cache == nil {
		s.cache = newSlowLogBuffer(config.GetGlobalConfig().Log.SlowQueryFile, slowQueryBufferSize)
	}
	if filePath != s.cache.filePath {
		logutil.Logger(context.Background()).Info("slow query cache file not match", zap.String("cache file", s.cache.filePath), zap.String("data file", filePath))
		return
	}
	if s.cache.buf.isEmpty() {
		for i := range tuples {
			s.cache.buf.write(tuples[i])
		}
		return
	}
	cacheEndTuple := s.cache.getEndTuple()
	for i := range tuples {
		if tuples[i].time.Before(cacheEndTuple.time) {
			continue
		}
		if tuples[i].equal(cacheEndTuple) {
			continue
		}

		s.cache.buf.write(tuples[i])
	}
}

// slowLogCache is a cache for slow log data.
type slowLogCache struct {
	filePath string
	buf      *ringBuffer
}

func newSlowLogBuffer(filePath string, size int) *slowLogCache {
	return &slowLogCache{
		filePath: filePath,
		buf:      newRingBuffer(size),
	}
}

// cacheValid validates whether the cache is validate for the target file path.
func (s *slowLogReader) cacheValid(filePath string) bool {
	return s.cache != nil && s.cache.filePath == filePath && !s.cache.buf.isEmpty()
}

func (c *slowLogCache) getEndOffset() int64 {
	tuple := c.buf.getEnd()
	if tuple == nil {
		return 0
	}
	return tuple.(*slowQueryTuple).endOffset
}

func (c *slowLogCache) getStartTime() time.Time {
	var t time.Time
	tuple := c.buf.getStart()
	if tuple == nil {
		return t
	}
	return tuple.(*slowQueryTuple).time
}

func (c *slowLogCache) getEndTime() time.Time {
	var t time.Time
	tuple := c.buf.getEnd()
	if tuple == nil {
		return t
	}
	return tuple.(*slowQueryTuple).time
}

func (c *slowLogCache) getEndTuple() *slowQueryTuple {
	tuple := c.buf.getEnd()
	if tuple == nil {
		return nil
	}
	return tuple.(*slowQueryTuple)
}

// ringBuffer is not safe for concurrent read/write, but it is safe to concurrent read.
type ringBuffer struct {
	data        []interface{}
	start, next int
	full        bool
}

func newRingBuffer(size int) *ringBuffer {
	return &ringBuffer{
		data:  make([]interface{}, size),
		start: 0,
		next:  0,
		full:  false,
	}
}

func (r *ringBuffer) write(d interface{}) {
	if r.start == r.next && r.full {
		r.start++
		r.start = r.start % len(r.data)
	}
	r.data[r.next] = d
	r.next++
	if r.next >= len(r.data) {
		r.next = 0
		r.full = true
	}
}

func (r *ringBuffer) getStart() interface{} {
	if r.isEmpty() {
		return nil
	}
	return r.data[r.start]
}

func (r *ringBuffer) getEnd() interface{} {
	if r.isEmpty() {
		return nil
	}
	end := r.next - 1
	if end < 0 {
		end = len(r.data) - 1
	}
	return r.data[end]
}

func (r *ringBuffer) isEmpty() bool {
	return r.next == 0 && !r.full
}

// iterate iterates all buffered data.
func (r *ringBuffer) iterate(fn func(d interface{}) bool) {
	if r.isEmpty() {
		return
	}
	end := r.next
	if end <= r.start {
		end = len(r.data)
	}
	for i := r.start; i < end; i++ {
		if fn(r.data[i]) {
			return
		}
	}
	if r.next > r.start {
		return
	}
	end = r.next
	for i := 0; i < end; i++ {
		if fn(r.data[i]) {
			return
		}
	}
}

func (r *ringBuffer) len() int {
	if r.isEmpty() {
		return 0
	}
	if r.next <= r.start {
		return len(r.data)
	}
	return r.next - r.start
}

func (r *ringBuffer) readAll() []interface{} {
	if r.isEmpty() {
		return nil
	}
	data := make([]interface{}, 0, r.len())
	r.iterate(func(d interface{}) bool {
		data = append(data, d)
		return false
	})
	return data
}

func (r *ringBuffer) resize(size int) {
	rb := newRingBuffer(size)
	r.iterate(func(d interface{}) bool {
		rb.write(d)
		return false
	})
	r.data = rb.data
	r.start = rb.start
	r.next = rb.next
	r.full = rb.full
}

func (r *ringBuffer) clear() {
	r.start = 0
	r.next = 0
	r.full = false
}
