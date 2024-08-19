// Copyright 2024 PingCAP, Inc.
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

package indexusage

import (
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/statistics/handle/usage/collector"
)

// GlobalIndexID is the key type for indexUsageMap.
type GlobalIndexID struct {
	TableID int64
	IndexID int64
}

// Sample is the data structure to store index usage information.
type Sample struct {
	// LastUsedAt records the last time the index is used.
	LastUsedAt time.Time
	// QueryTotal records the total counts of queries which used this index.
	QueryTotal uint64
	// KvReqTotal records the count of KV requests which are sent to read this index.
	KvReqTotal uint64
	// RowAccessTotal sums the number of the rows scanned using this index.
	RowAccessTotal uint64
	// PercentageAccess is a histogram where each bucket represents the number of accesses to the index where the
	// percentage of scanned rows to the total number of rows in the table falls within the ranges of
	// 0, 0-1, 1-10, 10-20, 20-50, 50-100, and 100.
	PercentageAccess [7]uint64
}

var bucketBound = [6]float64{0, 0.01, 0.1, 0.2, 0.5, 1.0}

func getIndexUsageAccessBucket(percentage float64) int {
	if percentage == 0 {
		return 0
	}

	bucket := 0
	for i := 1; i < len(bucketBound); i++ {
		if percentage >= bucketBound[i-1] && percentage < bucketBound[i] {
			bucket = i
			break
		}
	}
	if percentage == 1.0 {
		bucket = len(bucketBound)
	}

	return bucket
}

// NewSample creates a new data point for index usage.
func NewSample(queryTotal uint64, kvReqTotal uint64, rowAccess uint64, tableTotalRows uint64) Sample {
	percentageAccess := [len(bucketBound) + 1]uint64{}

	// if the `tableTotalRows` == 0, record the percentage as 1 and use the last bucket.
	bucket := len(bucketBound)
	if tableTotalRows > 0 {
		rowAccessPercentage := float64(rowAccess) / float64(tableTotalRows)
		bucket = getIndexUsageAccessBucket(rowAccessPercentage)
	}
	percentageAccess[bucket] = 1

	return Sample{
		QueryTotal:       queryTotal,
		KvReqTotal:       kvReqTotal,
		RowAccessTotal:   rowAccess,
		PercentageAccess: percentageAccess,
		LastUsedAt:       time.Now(),
	}
}

type indexUsage map[GlobalIndexID]Sample

var indexUsagePool = sync.Pool{
	New: func() any {
		return make(indexUsage)
	},
}

func (m indexUsage) updateByKey(id GlobalIndexID, sample Sample) {
	item := m[id]
	item.QueryTotal += sample.QueryTotal
	item.RowAccessTotal += sample.RowAccessTotal
	item.KvReqTotal += sample.KvReqTotal
	for i, val := range sample.PercentageAccess {
		item.PercentageAccess[i] += val
	}

	if item.LastUsedAt.Before(sample.LastUsedAt) {
		item.LastUsedAt = sample.LastUsedAt
	}
	m[id] = item
}

func (m indexUsage) update(tableID int64, indexID int64, sample Sample) {
	id := GlobalIndexID{TableID: tableID, IndexID: indexID}
	m.updateByKey(id, sample)
}

func (m indexUsage) merge(destMap indexUsage) {
	for id := range destMap {
		item := destMap[id]
		m.updateByKey(id, item)
	}
}

// Collector represents a data structure to record the index usage for the whole node
type Collector struct {
	collector  collector.GlobalCollector[indexUsage]
	indexUsage indexUsage
	sync.RWMutex

	closed bool
}

// NewCollector create an index usage collector
func NewCollector() *Collector {
	iuc := &Collector{
		indexUsage: indexUsagePool.Get().(indexUsage),
	}
	iuc.collector = collector.NewGlobalCollector[indexUsage](iuc.merge)

	return iuc
}

// GetIndexUsage returns the index usage information
func (c *Collector) GetIndexUsage(tableID int64, indexID int64) Sample {
	c.RLock()
	defer c.RUnlock()

	info, ok := c.indexUsage[GlobalIndexID{tableID, indexID}]
	if !ok {
		// It seems fine to return an empty sample if the caller doesn't care whether this index actually exists in the
		// collector. If the caller needs to know whether it exists (though I cannot image the scenario now), we can
		// change the return value from `Sample` to `Sample, bool`.
		return Sample{}
	}
	return info
}

func (c *Collector) merge(delta indexUsage) {
	c.Lock()
	defer c.Unlock()

	c.indexUsage.merge(delta)

	// return the `delta` to the pool
	clear(delta)
	indexUsagePool.Put(delta)
}

// SpawnSessionCollector creates a new session collector attached to this global collector
func (c *Collector) SpawnSessionCollector() *SessionIndexUsageCollector {
	return &SessionIndexUsageCollector{
		indexUsage: indexUsagePool.Get().(indexUsage),
		collector:  c.collector.SpawnSession(),
	}
}

// StartWorker starts the background worker inside
func (c *Collector) StartWorker() {
	c.collector.StartWorker()
}

// Close closes the background worker inside
func (c *Collector) Close() {
	c.collector.Close()
}

// GCIndexUsage will delete the usage information of non-existent indexes.
// `tableMetaLookup` argument is represented as a function (but not `sessionctx.Context` or `infoschema.InfoSchema`) to
// avoid depending on `sessionctx.Context`.
func (c *Collector) GCIndexUsage(tableMetaLookup func(id int64) (*model.TableInfo, bool)) {
	// it's possible to split `s.Mutex` into multiple mutex to avoid blocking the creation of session and sweepiing index
	// However, as all these operations are infrequent, keeping a simpler mutex is enough.
	c.Lock()
	defer c.Unlock()
	for k := range c.indexUsage {
		tbl, ok := tableMetaLookup(k.TableID)
		if !ok {
			delete(c.indexUsage, k)
			continue
		}
		foundIdx := false
		for _, idx := range tbl.Indices {
			if idx.ID == k.IndexID {
				foundIdx = true
				break
			}
		}
		if !foundIdx {
			delete(c.indexUsage, k)
		}
	}
}

// SessionIndexUsageCollector collects index usage per-session
type SessionIndexUsageCollector struct {
	indexUsage indexUsage

	collector collector.SessionCollector[indexUsage]
}

// Update updates the indexUsage in SessionIndexUsageCollector
func (s *SessionIndexUsageCollector) Update(tableID int64, indexID int64, sample Sample) {
	s.indexUsage.update(tableID, indexID, sample)
}

// Report reports the indexUsage in `SessionIndexUsageCollector` to the global collector
func (s *SessionIndexUsageCollector) Report() {
	if len(s.indexUsage) == 0 {
		return
	}
	if s.collector.SendDelta(s.indexUsage) {
		s.indexUsage = indexUsagePool.Get().(indexUsage)
	}
}

// Flush reports the indexUsage in `SessionIndexUsageCollector` to the global collector. It'll block until the data is
// received
func (s *SessionIndexUsageCollector) Flush() {
	if len(s.indexUsage) == 0 {
		return
	}
	s.collector.SendDeltaSync(s.indexUsage)
	s.indexUsage = indexUsagePool.Get().(indexUsage)
}

// StmtIndexUsageCollector removes the duplicates index for recording `QueryTotal` in session collector
type StmtIndexUsageCollector struct {
	recordedIndex    map[GlobalIndexID]struct{}
	sessionCollector *SessionIndexUsageCollector
	sync.Mutex
}

// NewStmtIndexUsageCollector creates a new StmtIndexUsageCollector.
func NewStmtIndexUsageCollector(sessionCollector *SessionIndexUsageCollector) *StmtIndexUsageCollector {
	return &StmtIndexUsageCollector{
		recordedIndex:    make(map[GlobalIndexID]struct{}),
		sessionCollector: sessionCollector,
	}
}

// Update updates the index usage in the internal session collector. The `sample.QueryTotal` will be modified according
// to whether this index has been recorded in this statement usage collector.
func (s *StmtIndexUsageCollector) Update(tableID int64, indexID int64, sample Sample) {
	// The session index usage collector and the map inside cannot be updated concurrently. However, for executors with
	// multiple workers, it's possible for them to be closed (and update stats) at the same time, so a lock is needed
	// here.
	s.Lock()
	defer s.Unlock()

	// If the usage of the table/index has been recorded in the statement, it'll not update the `QueryTotal`. Before the
	// execution of each statement, the `StmtIndexUsageCollector` and internal map will be re-created.
	idxID := GlobalIndexID{IndexID: indexID, TableID: tableID}
	if _, ok := s.recordedIndex[idxID]; !ok {
		sample.QueryTotal = 1
		s.recordedIndex[idxID] = struct{}{}
	} else {
		sample.QueryTotal = 0
	}

	s.sessionCollector.Update(tableID, indexID, sample)
}

// Reset resets the recorded index in the collector to avoid re-allocating for each statement.
func (s *StmtIndexUsageCollector) Reset() {
	s.Lock()
	defer s.Unlock()

	clear(s.recordedIndex)
}
