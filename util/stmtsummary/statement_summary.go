// Copyright 2016 PingCAP, Inc.
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
	hash       []byte
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
	sync.RWMutex
	summaryMap *kvcache.SimpleLRUCache
}

var StmtSummary = NewStmtSummaryByDigest()

// Summary of each type of statements.
type stmtSummary struct {
	schemaName      string
	digest          string
	normalizedSQL   string
	sampleSQL       string
	execCount       uint64
	sumTotalLatency uint64
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

// Used for recording execution status of each statement.
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

func NewStmtSummaryByDigest() *stmtSummaryByDigest {
	maxStmtCount := config.GetGlobalConfig().StmtSummary.MaxStmtCount
	return &stmtSummaryByDigest{
		summaryMap: kvcache.NewSimpleLRUCache(maxStmtCount, 0, 0),
	}
}

// Convert StmtExecInfo to stmtSummary
func convertStmtExecInfoToSummary(sei *StmtExecInfo) *stmtSummary {
	return &stmtSummary{
		schemaName:      sei.SchemaName,
		digest:          sei.Digest,
		normalizedSQL:   sei.NormalizedSQL,
		sampleSQL:       sei.OriginalSQL,
		execCount:       1,
		sumTotalLatency: sei.TotalLatency,
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
	ss.sumTotalLatency += sei.TotalLatency
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
	if ss.lastSeen.Before(sei.StartTime) {
		ss.lastSeen = sei.StartTime
	}
}

// Add a statement to statement summary.
func (ss *stmtSummaryByDigest) AddStatement(sei *StmtExecInfo) {
	key := &stmtSummaryCacheKey{
		schemaName: sei.SchemaName,
		digest:     sei.Digest,
	}

	ss.Lock()
	summary, ok := ss.summaryMap.Get(key)
	if ok {
		addStmtExecInfoToSummary(sei, summary.(*stmtSummary))
	} else {
		summary = convertStmtExecInfoToSummary(sei)
		ss.summaryMap.Put(key, summary)
	}
	ss.Unlock()
}

// Convert statement summary to Datum
func (ss *stmtSummaryByDigest) ToDatum() [][]types.Datum {
	ss.RLock()
	rows := make([][]types.Datum, 0, ss.summaryMap.Size())
	// Summary of each statement may be modified at the same time,
	// so surround the whole block with read lock.
	for _, value := range ss.summaryMap.Values() {
		summary := value.(*stmtSummary)
		record := types.MakeDatums(
			summary.schemaName,
			summary.digest,
			summary.normalizedSQL,
			summary.execCount,
			summary.sumTotalLatency,
			summary.maxLatency,
			summary.minLatency,
			summary.sumTotalLatency/summary.execCount, // AVG_LATENCY
			summary.sumAffectedRows,
			types.Time{Time: types.FromGoTime(summary.firstSeen), Type: mysql.TypeTimestamp},
			types.Time{Time: types.FromGoTime(summary.lastSeen), Type: mysql.TypeTimestamp},
			summary.sampleSQL,
		)
		rows = append(rows, record)
	}
	ss.RUnlock()

	return rows
}
