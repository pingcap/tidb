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
	"bytes"
	"fmt"
	"sort"
	"strconv"
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
// only implemented events_statements_summary_by_digest for now.

// stmtSummaryByDigestKey defines key for stmtSummaryByDigestMap.summaryMap.
type stmtSummaryByDigestKey struct {
	// Same statements may appear in different schema, but they refer to different tables.
	schemaName string
	digest     string
	// `hash` is the hash value of this object.
	hash []byte
}

// Hash implements SimpleLRUCache.Key.
func (key *stmtSummaryByDigestKey) Hash() []byte {
	if len(key.hash) == 0 {
		key.hash = make([]byte, 0, len(key.schemaName)+len(key.digest))
		key.hash = append(key.hash, hack.Slice(key.digest)...)
		key.hash = append(key.hash, hack.Slice(key.schemaName)...)
	}
	return key.hash
}

// stmtSummaryByDigestMap is a LRU cache that stores statement summaries.
type stmtSummaryByDigestMap struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	sync.Mutex
	summaryMap *kvcache.SimpleLRUCache
	// These fields are used for rolling summary.
	beginTimeForCurInterval int64
	lastCheckExpireTime     int64

	// sysVars encapsulates system variables needed to control statement summary.
	sysVars struct {
		sync.RWMutex
		// enabled indicates whether statement summary is enabled in current server.
		sessionEnabled string
		// setInSession indicates whether statement summary has been set in any session.
		globalEnabled string
		// These variables indicate the refresh interval of summaries.
		// They must be > 0.
		sessionRefreshInterval string
		globalRefreshInterval  string
		// A cached result. It must be read atomically.
		refreshInterval int64
	}
}

// StmtSummaryByDigestMap is a global map containing all statement summaries.
var StmtSummaryByDigestMap = newStmtSummaryByDigestMap()

// stmtSummaryByDigest is the summary for each type of statements.
type stmtSummaryByDigest struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	sync.Mutex
	// Each summary is summarized between [beginTime, beginTime + interval].
	beginTime int64
	// basic
	schemaName    string
	digest        string
	stmtType      string
	normalizedSQL string
	sampleSQL     string
	tableNames    string
	indexNames    string
	sampleUser    string
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
	commitCount          int64
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
	sumMem          int64
	maxMem          int64
	sumAffectedRows uint64
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
	StmtCtx        *stmtctx.StatementContext
	CopTasks       *stmtctx.CopTasksDetails
	ExecDetail     *execdetails.ExecDetails
	MemMax         int64
	StartTime      time.Time
}

// newStmtSummaryByDigestMap creates an empty stmtSummaryByDigestMap.
func newStmtSummaryByDigestMap() *stmtSummaryByDigestMap {
	maxStmtCount := config.GetGlobalConfig().StmtSummary.MaxStmtCount
	ssMap := &stmtSummaryByDigestMap{
		summaryMap:              kvcache.NewSimpleLRUCache(maxStmtCount, 0, 0),
		beginTimeForCurInterval: 0,
		lastCheckExpireTime:     0,
	}
	// sysVars.defaultEnabled will be initialized in package variable.
	ssMap.sysVars.sessionEnabled = ""
	ssMap.sysVars.globalEnabled = ""
	return ssMap
}

// AddStatement adds a statement to StmtSummaryByDigestMap.
func (ssMap *stmtSummaryByDigestMap) AddStatement(sei *StmtExecInfo) {
	// All times are counted in seconds.
	now := time.Now().Unix()

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

		// Check refreshing every second.
		if now > ssMap.lastCheckExpireTime {
			intervalSeconds := ssMap.RefreshInterval()
			if intervalSeconds <= 0 {
				return nil, false
			}

			if ssMap.beginTimeForCurInterval+intervalSeconds <= now {
				// `beginTimeForCurInterval` is a multiple of intervalSeconds, so that when the interval is a multiple
				// of 60 (or 600, 1800, 3600, etc), begin time shows 'XX:XX:00', not 'XX:XX:01'~'XX:XX:59'.
				ssMap.beginTimeForCurInterval = now / intervalSeconds * intervalSeconds
			}
			ssMap.lastCheckExpireTime = now
		}

		value, ok := ssMap.summaryMap.Get(key)
		// Replacing an element in LRU cache can only be `Delete` + `Put`.
		if ok && (value.(*stmtSummaryByDigest).beginTime < ssMap.beginTimeForCurInterval) {
			ssMap.summaryMap.Delete(key)
			ok = false
		}
		if !ok {
			newSummary := newStmtSummaryByDigest(sei, ssMap.beginTimeForCurInterval)
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
	ssMap.beginTimeForCurInterval = 0
	ssMap.lastCheckExpireTime = 0
}

// ToDatum converts statement summary to datum.
func (ssMap *stmtSummaryByDigestMap) ToDatum() [][]types.Datum {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	ssMap.Unlock()

	rows := make([][]types.Datum, 0, len(values))
	for _, value := range values {
		ssbd := value.(*stmtSummaryByDigest)
		record := ssbd.toDatum(ssMap.beginTimeForCurInterval)
		if record != nil {
			rows = append(rows, record)
		}
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
		// `stmtType` won't change once created, so locking is not needed.
		if ssbd.stmtType == "select" {
			ssbd.Lock()
			if ssbd.execCount > 1 {
				schemas = append(schemas, ssbd.schemaName)
				sqls = append(sqls, ssbd.sampleSQL)
			}
			ssbd.Unlock()
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

// normalizeEnableValue converts 'ON' to '1' and 'OFF' to '0'.
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

// SetRefreshInterval sets refreshing interval in ssMap.sysVars.
func (ssMap *stmtSummaryByDigestMap) SetRefreshInterval(value string, inSession bool) {
	ssMap.sysVars.Lock()
	if inSession {
		ssMap.sysVars.sessionRefreshInterval = value
	} else {
		ssMap.sysVars.globalRefreshInterval = value
	}
	sessionRefreshInterval := ssMap.sysVars.sessionRefreshInterval
	globalRefreshInterval := ssMap.sysVars.globalRefreshInterval
	ssMap.sysVars.Unlock()

	// Calculate the cached `refreshInterval`.
	var interval int
	var err error
	if ssMap.isSet(sessionRefreshInterval) {
		interval, err = strconv.Atoi(sessionRefreshInterval)
		if err != nil {
			interval = 0
		}
	}
	if interval <= 0 {
		interval, err = strconv.Atoi(globalRefreshInterval)
		if err != nil {
			interval = 0
		}
	}
	if interval > 0 {
		atomic.StoreInt64(&ssMap.sysVars.refreshInterval, int64(interval))
	}
}

func (ssMap *stmtSummaryByDigestMap) RefreshInterval() int64 {
	return atomic.LoadInt64(&ssMap.sysVars.refreshInterval)
}

// newStmtSummaryByDigest creates a stmtSummaryByDigest from StmtExecInfo.
func newStmtSummaryByDigest(sei *StmtExecInfo, beginTime int64) *stmtSummaryByDigest {
	// Trim SQL to size MaxSQLLength.
	maxSQLLength := config.GetGlobalConfig().StmtSummary.MaxSQLLength
	normalizedSQL := sei.NormalizedSQL
	if len(normalizedSQL) > int(maxSQLLength) {
		normalizedSQL = normalizedSQL[:maxSQLLength]
	}

	// Use "," to separate table names and index names to support FIND_IN_SET.
	var buffer bytes.Buffer
	for i, value := range sei.StmtCtx.Tables {
		buffer.WriteString(strings.ToLower(value.DB))
		buffer.WriteString(".")
		buffer.WriteString(strings.ToLower(value.Table))
		if i < len(sei.StmtCtx.Tables)-1 {
			buffer.WriteString(",")
		}
	}
	tableNames := buffer.String()

	ssbd := &stmtSummaryByDigest{
		beginTime:     beginTime,
		schemaName:    sei.SchemaName,
		digest:        sei.Digest,
		stmtType:      strings.ToLower(sei.StmtCtx.StmtType),
		normalizedSQL: normalizedSQL,
		tableNames:    tableNames,
		indexNames:    strings.Join(sei.StmtCtx.IndexNames, ","),
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

	if sei.User != "" {
		ssbd.sampleUser = sei.User
	}

	maxSQLLength := config.GetGlobalConfig().StmtSummary.MaxSQLLength
	sampleSQL := sei.OriginalSQL
	if len(sampleSQL) > int(maxSQLLength) {
		sampleSQL = sampleSQL[:maxSQLLength]
	}
	ssbd.sampleSQL = sampleSQL
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
		ssbd.commitCount++
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
			ssbd.backoffTypes[backoffType] += 1
		}
		commitDetails.Mu.Unlock()
	}

	// other
	ssbd.sumAffectedRows += sei.StmtCtx.AffectedRows()
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

func (ssbd *stmtSummaryByDigest) toDatum(oldestBeginTime int64) []types.Datum {
	ssbd.Lock()
	defer ssbd.Unlock()

	if ssbd.beginTime < oldestBeginTime {
		return nil
	}

	return types.MakeDatums(
		types.Time{Time: types.FromGoTime(time.Unix(ssbd.beginTime, 0)), Type: mysql.TypeTimestamp},
		ssbd.stmtType,
		ssbd.schemaName,
		ssbd.digest,
		ssbd.normalizedSQL,
		convertEmptyToNil(ssbd.tableNames),
		convertEmptyToNil(ssbd.indexNames),
		convertEmptyToNil(ssbd.sampleUser),
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
		avgInt(int64(ssbd.sumPrewriteTime), ssbd.commitCount),
		int64(ssbd.maxPrewriteTime),
		avgInt(int64(ssbd.sumCommitTime), ssbd.commitCount),
		int64(ssbd.maxCommitTime),
		avgInt(int64(ssbd.sumGetCommitTsTime), ssbd.commitCount),
		int64(ssbd.maxGetCommitTsTime),
		avgInt(ssbd.sumCommitBackoffTime, ssbd.commitCount),
		ssbd.maxCommitBackoffTime,
		avgInt(ssbd.sumResolveLockTime, ssbd.commitCount),
		ssbd.maxResolveLockTime,
		avgInt(int64(ssbd.sumLocalLatchTime), ssbd.commitCount),
		int64(ssbd.maxLocalLatchTime),
		avgFloat(ssbd.sumWriteKeys, ssbd.commitCount),
		ssbd.maxWriteKeys,
		avgFloat(ssbd.sumWriteSize, ssbd.commitCount),
		ssbd.maxWriteSize,
		avgFloat(ssbd.sumPrewriteRegionNum, ssbd.commitCount),
		int(ssbd.maxPrewriteRegionNum),
		avgFloat(ssbd.sumTxnRetry, ssbd.commitCount),
		ssbd.maxTxnRetry,
		formatBackoffTypes(ssbd.backoffTypes),
		avgInt(ssbd.sumMem, ssbd.execCount),
		ssbd.maxMem,
		avgFloat(int64(ssbd.sumAffectedRows), ssbd.execCount),
		types.Time{Time: types.FromGoTime(ssbd.firstSeen), Type: mysql.TypeTimestamp},
		types.Time{Time: types.FromGoTime(ssbd.lastSeen), Type: mysql.TypeTimestamp},
		ssbd.sampleSQL,
	)
}

// Format the backoffType map to a string or nil.
func formatBackoffTypes(backoffMap map[fmt.Stringer]int) interface{} {
	type backoffStat struct {
		backoffType fmt.Stringer
		count       int
	}

	size := len(backoffMap)
	if size == 0 {
		return nil
	}

	backoffArray := make([]backoffStat, 0, len(backoffMap))
	for backoffType, count := range backoffMap {
		backoffArray = append(backoffArray, backoffStat{backoffType, count})
	}
	sort.Slice(backoffArray, func(i, j int) bool {
		return backoffArray[i].count > backoffArray[j].count
	})

	var buffer bytes.Buffer
	for index, stat := range backoffArray {
		if _, err := fmt.Fprintf(&buffer, "%v:%d", stat.backoffType, stat.count); err != nil {
			return "FORMAT ERROR"
		}
		if index < len(backoffArray)-1 {
			buffer.WriteString(",")
		}
	}
	return buffer.String()
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
