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
	"bufio"
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

var (
	defSlowQueryBufferSize = 500000
	slowQueryBufferSize    = defSlowQueryBufferSize
)

var globalSlowQueryReader slowQueryReader

type slowQueryReader struct {
	sync.RWMutex
	cache *slowQueryBuffer
}

func (s *slowQueryReader) readSlowLogData(filePath string, tz *time.Location) ([][]types.Datum, error) {
	if s.cache != nil && s.cache.filePath == filePath && !s.cache.buf.isEmpty() {
		// try read with buffer.
		return s.readSlowLogDataWithCache(tz)

	}
	return s.ReadSlowLogDataFromFileWithUpdateCache(tz, filePath, 0, nil, nil)
}

func (s *slowQueryReader) readSlowLogDataWithCache(tz *time.Location) ([][]types.Datum, error) {
	s.RLock()
	// Get before cached tuples.
	beforeCacheTuples, hasTruncate, err := s.parseSlowLogDataFromFileBeforeCachedAndCheckTruncate(tz)
	if err != nil {
		s.RUnlock()
		return nil, err
	}
	// Try to avoid reallocate memory.
	rows := make([][]types.Datum, len(beforeCacheTuples), s.cache.buf.len()+len(beforeCacheTuples)+100)
	// get all cache tuples.
	rows = s.getSlowLogDataFromCache(tz, rows)
	cacheEndTime := s.readCacheAtEnd().time
	startReadPosAfterCache := s.cache.getEndPos()
	s.RUnlock()

	if hasTruncate {
		startReadPosAfterCache = 0
	}
	// Get after cached tuples. Put this out lock is for reduce lock time.
	afterCacheTuples, err := s.parseSlowLogDataFromFileAfterCached(tz, cacheEndTime, startReadPosAfterCache)
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

func (s *slowQueryReader) parseSlowLogDataFromFileBeforeCachedAndCheckTruncate(tz *time.Location) ([]*slowQueryTuple, bool, error) {
	cacheStartTime := s.readCacheAtStart().time
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

func (s *slowQueryReader) getSlowLogDataFromCache(tz *time.Location, rows [][]types.Datum) [][]types.Datum {
	s.cache.buf.iterate(func(d interface{}) bool {
		tuple := d.(*slowQueryTuple)
		rows = append(rows, tuple.convertToDatumRow())
		return false
	})
	return rows
}

func (s *slowQueryReader) parseSlowLogDataFromFileAfterCached(tz *time.Location, cacheEndTime time.Time, offset int64) ([]*slowQueryTuple, error) {
	afterCacheTuples, err := parseSlowLogDataFromFile(tz, s.cache.filePath, offset, nil, func(t time.Time) bool {
		return t.Before(cacheEndTime) || t.Equal(cacheEndTime)
	})
	if err != nil {
		return nil, err
	}
	return afterCacheTuples, err
}

func (s *slowQueryReader) ReadSlowLogDataFromFileWithUpdateCache(tz *time.Location, filePath string, offset int64, filterFn func(t time.Time) bool, bypassFn func(t time.Time) bool) ([][]types.Datum, error) {
	tuples, err := parseSlowLogDataFromFile(tz, filePath, offset, filterFn, bypassFn)
	if err != nil {
		return nil, err
	}
	go s.updateCache(filePath, tuples)
	return convertSlowLogTuplesToDatums(tuples), nil
}

func (s *slowQueryReader) ReadSlowLogDataFromFileWithoutUpdateCache(tz *time.Location, filePath string, offset int64, filterFn func(t time.Time) bool, bypassFn func(t time.Time) bool) ([][]types.Datum, []*slowQueryTuple, error) {
	tuples, err := parseSlowLogDataFromFile(tz, filePath, offset, filterFn, bypassFn)
	if err != nil {
		return nil, nil, err
	}
	return convertSlowLogTuplesToDatums(tuples), tuples, nil
}

func convertSlowLogTuplesToDatums(tuples []*slowQueryTuple) [][]types.Datum {
	rows := make([][]types.Datum, len(tuples))
	for i := range tuples {
		rows[i] = tuples[i].convertToDatumRow()
	}
	return rows
}

// ReadSlowLogDataFromFile reads slow query data from slow log file.
// If filterFn(t) return false, will stop read and return directly.
// If bypassFn(t) return true, will bypass the current tuple.
// ReadSlowLogDataFromFile exports for testing.
func parseSlowLogDataFromFile(tz *time.Location, filePath string, offset int64, filterFn func(t time.Time) bool, bypassFn func(t time.Time) bool) (tuples []*slowQueryTuple, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer func() {
		if err = file.Close(); err != nil {
			logutil.Logger(context.Background()).Error("close slow log file failed.", zap.String("file", filePath), zap.Error(err))
		}
	}()

	if offset > 0 {
		if _, err := file.Seek(offset, 0); err != nil {
			return nil, err
		}
	}

	reader := bufio.NewReader(file)
	startFlag := false
	currentPos := offset
	var st *slowQueryTuple
	for {
		lineByte, err := getOneLine(reader)
		currentPos += int64(len(lineByte) + 1)
		if err != nil {
			if err == io.EOF {
				return tuples, nil
			}
			return tuples, err
		}
		line := string(hack.String(lineByte))
		// Check slow log entry start flag.
		if !startFlag && strings.HasPrefix(line, variable.SlowLogStartPrefixStr) {
			st = &slowQueryTuple{}
			err = st.setFieldValue(tz, variable.SlowLogTimeStr, line[len(variable.SlowLogStartPrefixStr):])
			if err != nil {
				return tuples, err
			}
			startFlag = true
			if filterFn != nil && !filterFn(st.time) {
				return tuples, err
			}
			if bypassFn != nil && bypassFn(st.time) {
				startFlag = false
			}
			continue
		}

		if startFlag {
			// Parse slow log field.
			if strings.HasPrefix(line, variable.SlowLogRowPrefixStr) {
				line = line[len(variable.SlowLogRowPrefixStr):]
				fieldValues := strings.Split(line, " ")
				for i := 0; i < len(fieldValues)-1; i += 2 {
					field := fieldValues[i]
					if strings.HasSuffix(field, ":") {
						field = field[:len(field)-1]
					}
					err = st.setFieldValue(tz, field, fieldValues[i+1])
					if err != nil {
						return tuples, err
					}
				}
			} else if strings.HasSuffix(line, variable.SlowLogSQLSuffixStr) {
				// Get the sql string, and mark the start flag to false.
				err = st.setFieldValue(tz, variable.SlowLogQuerySQLStr, string(hack.Slice(line)))
				if err != nil {
					return tuples, err
				}
				st.endPos = currentPos
				tuples = append(tuples, st)
				startFlag = false
			} else {
				startFlag = false
			}
		}
	}
}

func (s *slowQueryReader) updateCache(filePath string, tuples []*slowQueryTuple) {
	s.Lock()
	defer s.Unlock()
	if s.cache == nil {
		s.cache = newSlowQueryBuffer(config.GetGlobalConfig().Log.SlowQueryFile, slowQueryBufferSize)
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
	cacheEndTuple := s.readCacheAtEnd()
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

func (s *slowQueryReader) readCacheAtStart() *slowQueryTuple {
	tuple := s.cache.buf.getStart()
	if tuple == nil {
		return nil
	}
	return tuple.(*slowQueryTuple)
}

func (s *slowQueryReader) readCacheAtEnd() *slowQueryTuple {
	tuple := s.cache.buf.getEnd()
	if tuple == nil {
		return nil
	}
	return tuple.(*slowQueryTuple)
}

type slowQueryBuffer struct {
	filePath string
	buf      *ringBuffer
}

func newSlowQueryBuffer(filePath string, size int) *slowQueryBuffer {
	return &slowQueryBuffer{
		filePath: filePath,
		buf:      newRingBuffer(size),
	}
}

func (b *slowQueryBuffer) getEndPos() int64 {
	if b.buf.isEmpty() {
		return 0
	}
	return b.buf.getEnd().(*slowQueryTuple).endPos
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

func (r *ringBuffer) cap() int {
	return len(r.data)
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
