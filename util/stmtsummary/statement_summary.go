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

// There're many types of statement summary tables in MySQL, but we have
// only implemented events_statement_summary_by_digest for now.

// stmtSummaryByDigestKey defines key for stmtSummaryByDigestMap.summaryMap
type stmtSummaryByDigestKey struct {
	// Same statements may appear in different schema, but they refer to different tables.
	schemaName string
	digest     string
	// TODO: add plan digest
	// `hash` is the hash value of this object
	hash []byte
}

// Hash implements SimpleLRUCache.Key
func (key *stmtSummaryByDigestKey) Hash() []byte {
	if len(key.hash) == 0 {
		key.hash = make([]byte, 0, len(key.schemaName)+len(key.digest))
		key.hash = append(key.hash, hack.Slice(key.digest)...)
		key.hash = append(key.hash, hack.Slice(strings.ToLower(key.schemaName))...)
	}
	return key.hash
}

// stmtSummaryByDigestMap is a LRU cache that stores statement summaries.
type stmtSummaryByDigestMap struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	sync.Mutex
	summaryMap *kvcache.SimpleLRUCache

	// enabledWrapper encapsulates variables needed to judge whether statement summary is enabled.
	enabledWrapper struct {
		sync.RWMutex
		// enabled indicates whether statement summary is enabled in current server.
		sessionEnabled string
		// setInSession indicates whether statement summary has been set in any session.
		globalEnabled string
	}
}

// StmtSummaryByDigestMap is a global map containing all statement summaries.
var StmtSummaryByDigestMap = newStmtSummaryByDigestMap()

// stmtSummaryByDigest is the summary for each type of statements.
type stmtSummaryByDigest struct {
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

// StmtExecInfo records execution information of each statement.
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

// newStmtSummaryByDigestMap creates an empty stmtSummaryByDigestMap.
func newStmtSummaryByDigestMap() *stmtSummaryByDigestMap {
	maxStmtCount := config.GetGlobalConfig().StmtSummary.MaxStmtCount
	ssMap := &stmtSummaryByDigestMap{
		summaryMap: kvcache.NewSimpleLRUCache(maxStmtCount, 0, 0),
	}
	// enabledWrapper.defaultEnabled will be initialized in package variable.
	ssMap.enabledWrapper.sessionEnabled = ""
	ssMap.enabledWrapper.globalEnabled = ""
	return ssMap
}

// newStmtSummaryByDigest creates a stmtSummaryByDigest from StmtExecInfo
func newStmtSummaryByDigest(sei *StmtExecInfo) *stmtSummaryByDigest {
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

	return &stmtSummaryByDigest{
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

// Add a StmtExecInfo to stmtSummary
func (ssbd *stmtSummaryByDigest) add(sei *StmtExecInfo) {
	ssbd.Lock()

	ssbd.sumLatency += sei.TotalLatency
	ssbd.execCount++
	if sei.TotalLatency > ssbd.maxLatency {
		ssbd.maxLatency = sei.TotalLatency
	}
	if sei.TotalLatency < ssbd.minLatency {
		ssbd.minLatency = sei.TotalLatency
	}
	ssbd.sumAffectedRows += sei.AffectedRows
	ssbd.sumSentRows += sei.SentRows
	if sei.StartTime.Before(ssbd.firstSeen) {
		ssbd.firstSeen = sei.StartTime
	}
	if ssbd.lastSeen.Before(sei.StartTime) {
		ssbd.lastSeen = sei.StartTime
	}

	ssbd.Unlock()
}

// AddStatement adds a statement to StmtSummaryByDigestMap.
func (ssMap *stmtSummaryByDigestMap) AddStatement(sei *StmtExecInfo) {
	key := &stmtSummaryByDigestKey{
		schemaName: sei.SchemaName,
		digest:     sei.Digest,
	}

	ssMap.Lock()
	// Check again. Statements could be added before disabling the flag and after Clear()
	if !ssMap.Enabled() {
		ssMap.Unlock()
		return
	}
	value, ok := ssMap.summaryMap.Get(key)
	if !ok {
		newSummary := newStmtSummaryByDigest(sei)
		ssMap.summaryMap.Put(key, newSummary)
	}
	ssMap.Unlock()

	// Lock a single entry, not the whole cache.
	if ok {
		value.(*stmtSummaryByDigest).add(sei)
	}
}

// Clear removes all statement summaries.
func (ssMap *stmtSummaryByDigestMap) Clear() {
	ssMap.Lock()
	ssMap.summaryMap.DeleteAll()
	ssMap.Unlock()
}

// ToDatum converts statement summary to Datum
func (ssMap *stmtSummaryByDigestMap) ToDatum() [][]types.Datum {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	ssMap.Unlock()

	rows := make([][]types.Datum, 0, len(values))
	for _, value := range values {
		summary := value.(*stmtSummaryByDigest)
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

// GetMoreThanOnceSelect gets select SQLs that occurred more than once.
func (ssMap *stmtSummaryByDigestMap) GetMoreThanOnceSelect() ([]string, []string) {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	ssMap.Unlock()

	schemas := make([]string, 0, len(values))
	sqls := make([]string, 0, len(values))
	for _, value := range values {
		summary := value.(*stmtSummaryByDigest)
		summary.Lock()
		if strings.HasPrefix(summary.normalizedSQL, "select") && summary.execCount > 1 {
			schemas = append(schemas, summary.schemaName)
			sqls = append(sqls, summary.sampleSQL)
		}
		summary.Unlock()
	}
	return schemas, sqls
}

// SetEnabled enables or disables statement summary in global(cluster) or session(server) scope.
func (ssMap *stmtSummaryByDigestMap) SetEnabled(value string, inSession bool) {
	value = ssMap.normalizeEnableValue(value)

	ssMap.enabledWrapper.Lock()
	if inSession {
		ssMap.enabledWrapper.sessionEnabled = value
	} else {
		ssMap.enabledWrapper.globalEnabled = value
	}
	sessionEnabled := ssMap.enabledWrapper.sessionEnabled
	globalEnabled := ssMap.enabledWrapper.globalEnabled
	ssMap.enabledWrapper.Unlock()

	// Clear all summaries once statement summary is disabled.
	var needClear bool
	if ssMap.isSet(sessionEnabled) {
		needClear = !ssMap.isEnabled(sessionEnabled)
	} else {
		needClear = !ssMap.isEnabled(globalEnabled)
	}
	if needClear {
		ssMap.Clear()
	}
}

// Enabled returns whether statement summary is enabled.
func (ssMap *stmtSummaryByDigestMap) Enabled() bool {
	ssMap.enabledWrapper.RLock()
	var enabled bool
	if ssMap.isSet(ssMap.enabledWrapper.sessionEnabled) {
		enabled = ssMap.isEnabled(ssMap.enabledWrapper.sessionEnabled)
	} else {
		enabled = ssMap.isEnabled(ssMap.enabledWrapper.globalEnabled)
	}
	ssMap.enabledWrapper.RUnlock()
	return enabled
}

// normalizeEnableValue converts 'ON' to '1' and 'OFF' to '0'
func (ssMap *stmtSummaryByDigestMap) normalizeEnableValue(value string) string {
	switch {
	case strings.EqualFold(value, "ON"):
		return "1"
	case strings.EqualFold(value, "OFF"):
		return "0"
	default:
		return value
	}
}

// isEnabled converts a string value to bool.
// 1 indicates true, 0 or '' indicates false.
func (ssMap *stmtSummaryByDigestMap) isEnabled(value string) bool {
	return value == "1"
}

// isSet judges whether the variable is set.
func (ssMap *stmtSummaryByDigestMap) isSet(value string) bool {
	return value != ""
}
