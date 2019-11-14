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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/execdetails"
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
	// `hash` is the hash value of this object
	hash []byte
}

// Hash implements SimpleLRUCache.Key
func (key *stmtSummaryByDigestKey) Hash() []byte {
	if len(key.hash) == 0 {
		key.hash = make([]byte, 0, len(key.schemaName)+len(key.digest)+8)
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

	// sysVars encapsulates variables needed to judge whether statement summary is enabled.
	sysVars struct {
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
	// basic
	schemaName    string
	digest        string
	normalizedSQL string
	sampleSQL     string
	tableIDs      string
	indexNames    string
	user          string
	execCount     int64
	// latency
	sumLatency        time.Duration
	maxLatency        time.Duration
	minLatency        time.Duration
	sumParseLatency   time.Duration
	maxParseLatency   time.Duration
	sumCompileLatency time.Duration
	maxCompileLatency time.Duration
	// coprocessor
	numCopTasks          int64
	sumCopProcessTime    int64
	maxCopProcessTime    time.Duration
	maxCopProcessAddress string
	sumCopWaitTime       int64
	maxCopWaitTime       time.Duration
	maxCopWaitAddress    string
	// TiKV
	sumProcessTime   time.Duration
	maxProcessTime   time.Duration
	sumWaitTime      time.Duration
	maxWaitTime      time.Duration
	sumBackoffTime   time.Duration
	maxBackoffTime   time.Duration
	sumTotalKeys     int64
	maxTotalKeys     int64
	sumProcessedKeys int64
	maxProcessedKeys int64
	// txn
	sumGetCommitTsTime   time.Duration
	maxGetCommitTsTime   time.Duration
	sumPrewriteTime      time.Duration
	maxPrewriteTime      time.Duration
	sumCommitTime        time.Duration
	maxCommitTime        time.Duration
	sumLocalLatchTime    time.Duration
	maxLocalLatchTime    time.Duration
	sumCommitBackoffTime int64
	maxCommitBackoffTime int64
	sumResolveLockTime   int64
	maxResolveLockTime   int64
	sumWriteKeys         int64
	maxWriteKeys         int
	sumWriteSize         int64
	maxWriteSize         int
	sumPrewriteRegionNum int64
	maxPrewriteRegionNum int32
	sumTxnRetry          int64
	maxTxnRetry          int
	backoffTypes         map[fmt.Stringer]int
	// other
	sumMem int64
	maxMem int64
	// The first time this type of SQL executes.
	firstSeen time.Time
	// The last time this type of SQL executes.
	lastSeen time.Time
}

// StmtExecInfo records execution information of each statement.
type StmtExecInfo struct {
	SchemaName     string
	OriginalSQL    string
	NormalizedSQL  string
	Digest         string
	User           string
	TotalLatency   time.Duration
	ParseLatency   time.Duration
	CompileLatency time.Duration
	TableIDs       string
	IndexNames     string
	CopTasks       *stmtctx.CopTasksDetails
	ExecDetail     *execdetails.ExecDetails
	MemMax         int64
	StartTime      time.Time
}

// newStmtSummaryByDigestMap creates an empty stmtSummaryByDigestMap.
func newStmtSummaryByDigestMap() *stmtSummaryByDigestMap {
	maxStmtCount := config.GetGlobalConfig().StmtSummary.MaxStmtCount
	ssMap := &stmtSummaryByDigestMap{
		summaryMap: kvcache.NewSimpleLRUCache(maxStmtCount, 0, 0),
	}
	// sysVars.defaultEnabled will be initialized in package variable.
	ssMap.sysVars.sessionEnabled = ""
	ssMap.sysVars.globalEnabled = ""
	return ssMap
}

// AddStatement adds a statement to StmtSummaryByDigestMap.
func (ssMap *stmtSummaryByDigestMap) AddStatement(sei *StmtExecInfo) {
	key := &stmtSummaryByDigestKey{
		schemaName: sei.SchemaName,
		digest:     sei.Digest,
	}

	// Enclose the block in a function to ensure the lock will always be released.
	value, ok := func() (kvcache.Value, bool) {
		ssMap.Lock()
		defer ssMap.Unlock()

		// Check again. Statements could be added before disabling the flag and after Clear().
		if !ssMap.Enabled() {
			return nil, false
		}

		value, ok := ssMap.summaryMap.Get(key)
		if !ok {
			newSummary := newStmtSummaryByDigest(sei)
			ssMap.summaryMap.Put(key, newSummary)
		}
		return value, ok
	}()

	// Lock a single entry, not the whole cache.
	if ok {
		value.(*stmtSummaryByDigest).add(sei)
	}
}

// Clear removes all statement summaries.
func (ssMap *stmtSummaryByDigestMap) Clear() {
	ssMap.Lock()
	defer ssMap.Unlock()

	ssMap.summaryMap.DeleteAll()
}

// Convert statement summary to Datum
func (ssMap *stmtSummaryByDigestMap) ToDatum() [][]types.Datum {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	ssMap.Unlock()

	rows := make([][]types.Datum, 0, len(values))
	for _, value := range values {
		ssbd := value.(*stmtSummaryByDigest)
		record := ssbd.toDatum()
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
		ssbd := value.(*stmtSummaryByDigest)
		// `normalizedSQL` & `schemaName` & `sampleSQL` won't change once created,
		// so locking is not needed.
		if strings.HasPrefix(ssbd.normalizedSQL, "select") {
			schemas = append(schemas, ssbd.schemaName)
			sqls = append(sqls, ssbd.sampleSQL)
		}
	}
	return schemas, sqls
}

// SetEnabled enables or disables statement summary in global(cluster) or session(server) scope.
func (ssMap *stmtSummaryByDigestMap) SetEnabled(value string, inSession bool) {
	value = ssMap.normalizeEnableValue(value)

	ssMap.sysVars.Lock()
	if inSession {
		ssMap.sysVars.sessionEnabled = value
	} else {
		ssMap.sysVars.globalEnabled = value
	}
	sessionEnabled := ssMap.sysVars.sessionEnabled
	globalEnabled := ssMap.sysVars.globalEnabled
	ssMap.sysVars.Unlock()

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
	ssMap.sysVars.RLock()
	defer ssMap.sysVars.RUnlock()

	var enabled bool
	if ssMap.isSet(ssMap.sysVars.sessionEnabled) {
		enabled = ssMap.isEnabled(ssMap.sysVars.sessionEnabled)
	} else {
		enabled = ssMap.isEnabled(ssMap.sysVars.globalEnabled)
	}
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

// newStmtSummaryByDigest creates a stmtSummaryByDigest from StmtExecInfo
func newStmtSummaryByDigest(sei *StmtExecInfo) *stmtSummaryByDigest {
	// Trim SQL to size MaxSQLLength.
	maxSQLLength := config.GetGlobalConfig().StmtSummary.MaxSQLLength
	normalizedSQL := sei.NormalizedSQL
	if len(normalizedSQL) > int(maxSQLLength) {
		normalizedSQL = normalizedSQL[:maxSQLLength]
	}
	sampleSQL := sei.OriginalSQL
	if len(sampleSQL) > int(maxSQLLength) {
		sampleSQL = sampleSQL[:maxSQLLength]
	}

	ssbd := &stmtSummaryByDigest{
		schemaName:    sei.SchemaName,
		digest:        sei.Digest,
		normalizedSQL: normalizedSQL,
		sampleSQL:     sampleSQL,
		tableIDs:      sei.TableIDs,
		indexNames:    sei.IndexNames,
		minLatency:    sei.TotalLatency,
		backoffTypes:  make(map[fmt.Stringer]int),
		firstSeen:     sei.StartTime,
		lastSeen:      sei.StartTime,
	}
	ssbd.add(sei)
	return ssbd
}

func (ssbd *stmtSummaryByDigest) add(sei *StmtExecInfo) {
	ssbd.Lock()
	defer ssbd.Unlock()

	if ssbd.user == "" {
		ssbd.user = sei.User
	}
	ssbd.execCount++

	// latency
	ssbd.sumLatency += sei.TotalLatency
	if sei.TotalLatency > ssbd.maxLatency {
		ssbd.maxLatency = sei.TotalLatency
	}
	if sei.TotalLatency < ssbd.minLatency {
		ssbd.minLatency = sei.TotalLatency
	}
	ssbd.sumParseLatency += sei.ParseLatency
	if sei.ParseLatency > ssbd.maxParseLatency {
		ssbd.maxParseLatency = sei.ParseLatency
	}
	ssbd.sumCompileLatency += sei.CompileLatency
	if sei.CompileLatency > ssbd.maxCompileLatency {
		ssbd.maxCompileLatency = sei.CompileLatency
	}

	// coprocessor
	numCopTasks := int64(sei.CopTasks.NumCopTasks)
	ssbd.numCopTasks += numCopTasks
	ssbd.sumCopProcessTime += sei.CopTasks.AvgProcessTime.Nanoseconds() * numCopTasks
	if sei.CopTasks.MaxProcessTime > ssbd.maxCopProcessTime {
		ssbd.maxCopProcessTime = sei.CopTasks.MaxProcessTime
		ssbd.maxCopProcessAddress = sei.CopTasks.MaxProcessAddress
	}
	ssbd.sumCopWaitTime += sei.CopTasks.AvgWaitTime.Nanoseconds() * numCopTasks
	if sei.CopTasks.MaxWaitTime > ssbd.maxCopWaitTime {
		ssbd.maxCopWaitTime = sei.CopTasks.MaxWaitTime
		ssbd.maxCopWaitAddress = sei.CopTasks.MaxWaitAddress
	}

	// TiKV
	ssbd.sumProcessTime += sei.ExecDetail.ProcessTime
	if sei.ExecDetail.ProcessTime > ssbd.maxProcessTime {
		ssbd.maxProcessTime = sei.ExecDetail.ProcessTime
	}
	ssbd.sumWaitTime += sei.ExecDetail.WaitTime
	if sei.ExecDetail.WaitTime > ssbd.maxWaitTime {
		ssbd.maxWaitTime = sei.ExecDetail.WaitTime
	}
	ssbd.sumBackoffTime += sei.ExecDetail.BackoffTime
	if sei.ExecDetail.BackoffTime > ssbd.maxBackoffTime {
		ssbd.maxBackoffTime = sei.ExecDetail.BackoffTime
	}
	ssbd.sumTotalKeys += sei.ExecDetail.TotalKeys
	if sei.ExecDetail.TotalKeys > ssbd.maxTotalKeys {
		ssbd.maxTotalKeys = sei.ExecDetail.TotalKeys
	}
	ssbd.sumProcessedKeys += sei.ExecDetail.ProcessedKeys
	if sei.ExecDetail.ProcessedKeys > ssbd.maxProcessedKeys {
		ssbd.maxProcessedKeys = sei.ExecDetail.ProcessedKeys
	}

	// txn
	commitDetails := sei.ExecDetail.CommitDetail
	if commitDetails != nil {
		ssbd.sumPrewriteTime += commitDetails.PrewriteTime
		if commitDetails.PrewriteTime > ssbd.maxPrewriteTime {
			ssbd.maxPrewriteTime = commitDetails.PrewriteTime
		}
		ssbd.sumCommitTime += commitDetails.CommitTime
		if commitDetails.CommitTime > ssbd.maxCommitTime {
			ssbd.maxCommitTime = commitDetails.CommitTime
		}
		ssbd.sumGetCommitTsTime += commitDetails.GetCommitTsTime
		if commitDetails.GetCommitTsTime > ssbd.maxGetCommitTsTime {
			ssbd.maxGetCommitTsTime = commitDetails.GetCommitTsTime
		}
		ssbd.sumCommitBackoffTime += commitDetails.CommitBackoffTime
		if commitDetails.CommitBackoffTime > ssbd.maxCommitBackoffTime {
			ssbd.maxCommitBackoffTime = commitDetails.CommitBackoffTime
		}
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLockTime)
		ssbd.sumResolveLockTime += resolveLockTime
		if resolveLockTime > ssbd.maxResolveLockTime {
			ssbd.maxResolveLockTime = resolveLockTime
		}
		ssbd.sumLocalLatchTime += commitDetails.LocalLatchTime
		if commitDetails.LocalLatchTime > ssbd.maxLocalLatchTime {
			ssbd.maxLocalLatchTime = commitDetails.LocalLatchTime
		}
		ssbd.sumWriteKeys += int64(commitDetails.WriteKeys)
		if commitDetails.WriteKeys > ssbd.maxWriteKeys {
			ssbd.maxWriteKeys = commitDetails.WriteKeys
		}
		ssbd.sumWriteSize += int64(commitDetails.WriteSize)
		if commitDetails.WriteSize > ssbd.maxWriteSize {
			ssbd.maxWriteSize = commitDetails.WriteSize
		}
		prewriteRegionNum := atomic.LoadInt32(&commitDetails.PrewriteRegionNum)
		ssbd.sumPrewriteRegionNum += int64(prewriteRegionNum)
		if prewriteRegionNum > ssbd.maxPrewriteRegionNum {
			ssbd.maxPrewriteRegionNum = prewriteRegionNum
		}
		ssbd.sumTxnRetry += int64(commitDetails.TxnRetry)
		if commitDetails.TxnRetry > ssbd.maxTxnRetry {
			ssbd.maxTxnRetry = commitDetails.TxnRetry
		}
		commitDetails.Mu.Lock()
		for _, backoffType := range commitDetails.Mu.BackoffTypes {
			if _, ok := ssbd.backoffTypes[backoffType]; ok {
				ssbd.backoffTypes[backoffType] += 1
			} else {
				ssbd.backoffTypes[backoffType] = 1
			}
		}
		commitDetails.Mu.Unlock()
	}

	// other
	ssbd.sumMem += sei.MemMax
	if sei.MemMax > ssbd.maxMem {
		ssbd.maxMem = sei.MemMax
	}
	if sei.StartTime.Before(ssbd.firstSeen) {
		ssbd.firstSeen = sei.StartTime
	}
	if ssbd.lastSeen.Before(sei.StartTime) {
		ssbd.lastSeen = sei.StartTime
	}
}

func (ssbd *stmtSummaryByDigest) toDatum() []types.Datum {
	ssbd.Lock()
	defer ssbd.Unlock()

	return types.MakeDatums(
		ssbd.schemaName,
		ssbd.digest,
		ssbd.normalizedSQL,
		convertEmptyToNil(ssbd.tableIDs),
		convertEmptyToNil(ssbd.indexNames),
		convertEmptyToNil(ssbd.user),
		ssbd.execCount,
		int64(ssbd.sumLatency),
		int64(ssbd.maxLatency),
		int64(ssbd.minLatency),
		avgInt(int64(ssbd.sumLatency), ssbd.execCount),
		avgInt(int64(ssbd.sumParseLatency), ssbd.execCount),
		int64(ssbd.maxParseLatency),
		avgInt(int64(ssbd.sumCompileLatency), ssbd.execCount),
		int64(ssbd.maxCompileLatency),
		ssbd.numCopTasks,
		avgInt(ssbd.sumCopProcessTime, ssbd.numCopTasks),
		int64(ssbd.maxCopProcessTime),
		convertEmptyToNil(ssbd.maxCopProcessAddress),
		avgInt(ssbd.sumCopWaitTime, ssbd.numCopTasks),
		int64(ssbd.maxCopWaitTime),
		convertEmptyToNil(ssbd.maxCopWaitAddress),
		avgInt(int64(ssbd.sumProcessTime), ssbd.execCount),
		int64(ssbd.maxProcessTime),
		avgInt(int64(ssbd.sumWaitTime), ssbd.execCount),
		int64(ssbd.maxWaitTime),
		avgInt(int64(ssbd.sumBackoffTime), ssbd.execCount),
		int64(ssbd.maxBackoffTime),
		avgInt(ssbd.sumTotalKeys, ssbd.execCount),
		ssbd.maxTotalKeys,
		avgInt(ssbd.sumProcessedKeys, ssbd.execCount),
		ssbd.maxProcessedKeys,
		avgInt(int64(ssbd.sumPrewriteTime), ssbd.execCount),
		int64(ssbd.maxPrewriteTime),
		avgInt(int64(ssbd.sumCommitTime), ssbd.execCount),
		int64(ssbd.maxCommitTime),
		avgInt(int64(ssbd.sumGetCommitTsTime), ssbd.execCount),
		int64(ssbd.maxGetCommitTsTime),
		avgInt(ssbd.sumCommitBackoffTime, ssbd.execCount),
		ssbd.maxCommitBackoffTime,
		avgInt(ssbd.sumResolveLockTime, ssbd.execCount),
		ssbd.maxResolveLockTime,
		avgInt(int64(ssbd.sumLocalLatchTime), ssbd.execCount),
		int64(ssbd.maxLocalLatchTime),
		avgFloat(ssbd.sumWriteKeys, ssbd.execCount),
		ssbd.maxWriteKeys,
		avgFloat(ssbd.sumWriteSize, ssbd.execCount),
		ssbd.maxWriteSize,
		avgFloat(ssbd.sumPrewriteRegionNum, ssbd.execCount),
		ssbd.maxPrewriteRegionNum,
		avgFloat(ssbd.sumTxnRetry, ssbd.execCount),
		ssbd.maxTxnRetry,
		fmt.Sprintf("%v", ssbd.backoffTypes),
		avgInt(ssbd.sumMem, ssbd.execCount),
		ssbd.maxMem,
		types.Time{Time: types.FromGoTime(ssbd.firstSeen), Type: mysql.TypeTimestamp},
		types.Time{Time: types.FromGoTime(ssbd.lastSeen), Type: mysql.TypeTimestamp},
		ssbd.sampleSQL,
	)
}

func avgInt(sum int64, count int64) int64 {
	if count > 0 {
		return sum / count
	}
	return 0
}

func avgFloat(sum int64, count int64) float64 {
	if count > 0 {
		return float64(sum) / float64(count)
	}
	return 0
}

func convertEmptyToNil(str string) interface{} {
	if str == "" {
		return nil
	}
	return str
}
