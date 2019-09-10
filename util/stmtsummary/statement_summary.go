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

package stmtsummary

import (
	"strings"
	"sync"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
)

type stmtSummaryCacheKey struct {
	// Same statements may appear in different schema, but they refer to different tables.
	schemaName string
	digest     string
	// TODO: add plan digest
	// `hash` is the hash value of this object
	hash []byte
}

// Hash implements SimpleLRUCache.Key
func (key *stmtSummaryCacheKey) Hash() []byte {
	if len(key.hash) == 0 {
		key.hash = make([]byte, 0, len(key.schemaName)+len(key.digest))
		key.hash = append(key.hash, hack.Slice(key.digest)...)
		key.hash = append(key.hash, hack.Slice(strings.ToLower(key.schemaName))...)
	}
	return key.hash
}

// A LRU cache that stores statement summary.
type stmtSummaryByDigest struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	sync.RWMutex
	summaryMap *kvcache.SimpleLRUCache
}

// StmtSummary is the global object for statement summary.
var StmtSummary = NewStmtSummaryByDigest()

// Summary of each type of statements.
type stmtSummary struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	sync.Mutex
	schemaName      string
	digest          string
	normalizedSQL   string
	sampleSQL       string
	execCount       uint64
	sumLatency      uint64
	maxLatency      uint64
	minLatency      uint64
	sumAffectedRows uint64
	// Number of rows sent to client.
	sumSentRows uint64
	// The first time this type of SQL executes.
	firstSeen time.Time
	// The last time this type of SQL executes.
	lastSeen time.Time
}

// StmtExecInfo records execution status of each statement.
type StmtExecInfo struct {
	SchemaName    string
	OriginalSQL   string
	NormalizedSQL string
	Digest        string
	TotalLatency  uint64
	AffectedRows  uint64
	// Number of rows sent to client.
	SentRows  uint64
	StartTime time.Time
}

// NewStmtSummaryByDigest creates a stmtSummaryByDigest.
func NewStmtSummaryByDigest() *stmtSummaryByDigest {
	maxStmtCount := config.GetGlobalConfig().StmtSummary.MaxStmtCount
	return &stmtSummaryByDigest{
		summaryMap: kvcache.NewSimpleLRUCache(maxStmtCount, 0, 0),
	}
}

// Convert StmtExecInfo to stmtSummary
func convertStmtExecInfoToSummary(sei *StmtExecInfo) *stmtSummary {
	// Trim SQL to size MaxSQLLength
	maxSQLLength := config.GetGlobalConfig().StmtSummary.MaxSQLLength
	normalizedSQL := sei.NormalizedSQL
	if len(normalizedSQL) > int(maxSQLLength) {
		normalizedSQL = normalizedSQL[:maxSQLLength]
	}
	sampleSQL := sei.OriginalSQL
	if len(sampleSQL) > int(maxSQLLength) {
		sampleSQL = sampleSQL[:maxSQLLength]
	}

	return &stmtSummary{
		schemaName:      sei.SchemaName,
		digest:          sei.Digest,
		normalizedSQL:   normalizedSQL,
		sampleSQL:       sampleSQL,
		execCount:       1,
		sumLatency:      sei.TotalLatency,
		maxLatency:      sei.TotalLatency,
		minLatency:      sei.TotalLatency,
		sumAffectedRows: sei.AffectedRows,
		sumSentRows:     sei.SentRows,
		firstSeen:       sei.StartTime,
		lastSeen:        sei.StartTime,
	}
}

// Add a new StmtExecInfo to stmtSummary
func addStmtExecInfoToSummary(sei *StmtExecInfo, ss *stmtSummary) {
	ss.Lock()

	ss.sumLatency += sei.TotalLatency
	ss.execCount++
	if sei.TotalLatency > ss.maxLatency {
		ss.maxLatency = sei.TotalLatency
	}
	if sei.TotalLatency < ss.minLatency {
		ss.minLatency = sei.TotalLatency
	}
	// TODO: uint64 overflow
	ss.sumAffectedRows += sei.AffectedRows
	ss.sumSentRows += sei.SentRows
	if sei.StartTime.Before(ss.firstSeen) {
		ss.firstSeen = sei.StartTime
	}
	if ss.lastSeen.Before(sei.StartTime) {
		ss.lastSeen = sei.StartTime
	}

	ss.Unlock()
}

// Add a statement to statement summary.
func (ss *stmtSummaryByDigest) AddStatement(sei *StmtExecInfo) {
	key := &stmtSummaryCacheKey{
		schemaName: sei.SchemaName,
		digest:     sei.Digest,
	}

	var summary *stmtSummary
	ss.Lock()
	value, ok := ss.summaryMap.Get(key)
	if ok {
		summary = value.(*stmtSummary)
	} else {
		newSummary := convertStmtExecInfoToSummary(sei)
		ss.summaryMap.Put(key, newSummary)
	}
	ss.Unlock()

	// Lock a single entry, not the whole cache.
	if summary != nil {
		addStmtExecInfoToSummary(sei, summary)
	}
}

// Remove all statement summaries.
func (ss *stmtSummaryByDigest) Clear() {
	ss.Lock()
	ss.summaryMap.DeleteAll()
	ss.Unlock()
}

// Convert statement summary to Datum
func (ss *stmtSummaryByDigest) ToDatum() [][]types.Datum {
	ss.Lock()
	values := ss.summaryMap.Values()
	ss.Unlock()

	rows := make([][]types.Datum, 0, len(values))
	for _, value := range values {
		summary := value.(*stmtSummary)
		summary.Lock()
		record := types.MakeDatums(
			summary.schemaName,
			summary.digest,
			summary.normalizedSQL,
			summary.execCount,
			summary.sumLatency,
			summary.maxLatency,
			summary.minLatency,
			summary.sumLatency/summary.execCount, // AVG_LATENCY
			summary.sumAffectedRows,
			types.Time{Time: types.FromGoTime(summary.firstSeen), Type: mysql.TypeTimestamp},
			types.Time{Time: types.FromGoTime(summary.lastSeen), Type: mysql.TypeTimestamp},
			summary.sampleSQL,
		)
		summary.Unlock()
		rows = append(rows, record)
	}

	return rows
}
