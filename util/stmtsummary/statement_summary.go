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
	"container/list"
	"context"
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
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/plancodec"
	"go.uber.org/zap"
)

// There're many types of statement summary tables in MySQL, but we have
// only implemented events_statements_summary_by_digest for now.

// stmtSummaryByDigestKey defines key for stmtSummaryByDigestMap.summaryMap.
type stmtSummaryByDigestKey struct {
	// Same statements may appear in different schema, but they refer to different tables.
	schemaName string
	digest     string
	// The digest of the previous statement.
	prevDigest string
	// The digest of the plan of this SQL.
	planDigest string
	// `hash` is the hash value of this object.
	hash []byte
}

// Hash implements SimpleLRUCache.Key.
// Only when current SQL is `commit` do we record `prevSQL`. Otherwise, `prevSQL` is empty.
// `prevSQL` is included in the key To distinguish different transactions.
func (key *stmtSummaryByDigestKey) Hash() []byte {
	if len(key.hash) == 0 {
		key.hash = make([]byte, 0, len(key.schemaName)+len(key.digest)+len(key.prevDigest)+len(key.planDigest))
		key.hash = append(key.hash, hack.Slice(key.digest)...)
		key.hash = append(key.hash, hack.Slice(key.schemaName)...)
		key.hash = append(key.hash, hack.Slice(key.prevDigest)...)
		key.hash = append(key.hash, hack.Slice(key.planDigest)...)
	}
	return key.hash
}

// stmtSummaryByDigestMap is a LRU cache that stores statement summaries.
type stmtSummaryByDigestMap struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	sync.Mutex
	summaryMap *kvcache.SimpleLRUCache
	// beginTimeForCurInterval is the begin time for current summary.
	beginTimeForCurInterval int64

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

		// These variables indicate the max history size of each summary.
		// They must be > 0.
		sessionHistorySize string
		globalHistorySize  string
		// A cached result. It must be read atomically.
		historySize int32
	}
}

// StmtSummaryByDigestMap is a global map containing all statement summaries.
var StmtSummaryByDigestMap = newStmtSummaryByDigestMap()

// stmtSummaryByDigest is the summary for each type of statements.
type stmtSummaryByDigest struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	// Mutex is only used to lock `history`.
	sync.Mutex
	// Each element in history is a summary in one interval.
	history *list.List
	// Following fields are common for each summary element.
	// They won't change once this object is created, so locking is not needed.
	schemaName    string
	digest        string
	planDigest    string
	stmtType      string
	normalizedSQL string
	tableNames    string
}

// stmtSummaryByDigestElement is the summary for each type of statements in current interval.
type stmtSummaryByDigestElement struct {
	sync.Mutex
	// Each summary is summarized between [beginTime, endTime).
	beginTime int64
	endTime   int64
	// basic
	sampleSQL  string
	prevSQL    string
	samplePlan string
	sampleUser string
	indexNames []string
	execCount  int64
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
	sumBackoffTimes      int64
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
	PrevSQL        string
	PrevSQLDigest  string
	PlanGenerator  func() string
	PlanDigest     string
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
		summaryMap: kvcache.NewSimpleLRUCache(maxStmtCount, 0, 0),
	}
	// Initialize these configurations by values in the config file.
	// They may be overwritten by system variables later.
	if config.GetGlobalConfig().StmtSummary.Enable {
		ssMap.sysVars.globalEnabled = "1"
	}
	ssMap.sysVars.refreshInterval = int64(config.GetGlobalConfig().StmtSummary.RefreshInterval)
	ssMap.sysVars.historySize = int32(config.GetGlobalConfig().StmtSummary.HistorySize)
	return ssMap
}

// AddStatement adds a statement to StmtSummaryByDigestMap.
func (ssMap *stmtSummaryByDigestMap) AddStatement(sei *StmtExecInfo) {
	// All times are counted in seconds.
	now := time.Now().Unix()

	intervalSeconds := ssMap.refreshInterval()
	historySize := ssMap.historySize()

	key := &stmtSummaryByDigestKey{
		schemaName: sei.SchemaName,
		digest:     sei.Digest,
		prevDigest: sei.PrevSQLDigest,
		planDigest: sei.PlanDigest,
	}

	// Enclose the block in a function to ensure the lock will always be released.
	value, beginTime, ok := func() (kvcache.Value, int64, bool) {
		ssMap.Lock()
		defer ssMap.Unlock()

		// Check again. Statements could be added before disabling the flag and after Clear().
		if !ssMap.Enabled() {
			return nil, 0, false
		}

		if ssMap.beginTimeForCurInterval+intervalSeconds <= now {
			// `beginTimeForCurInterval` is a multiple of intervalSeconds, so that when the interval is a multiple
			// of 60 (or 600, 1800, 3600, etc), begin time shows 'XX:XX:00', not 'XX:XX:01'~'XX:XX:59'.
			ssMap.beginTimeForCurInterval = now / intervalSeconds * intervalSeconds
		}

		beginTime := ssMap.beginTimeForCurInterval
		value, ok := ssMap.summaryMap.Get(key)
		if !ok {
			newSummary := newStmtSummaryByDigest(sei, beginTime, intervalSeconds, historySize)
			ssMap.summaryMap.Put(key, newSummary)
		}
		return value, beginTime, ok
	}()

	// Lock a single entry, not the whole cache.
	if ok {
		value.(*stmtSummaryByDigest).add(sei, beginTime, intervalSeconds, historySize)
	}
}

// Clear removes all statement summaries.
func (ssMap *stmtSummaryByDigestMap) Clear() {
	ssMap.Lock()
	defer ssMap.Unlock()

	ssMap.summaryMap.DeleteAll()
	ssMap.beginTimeForCurInterval = 0
}

// ToCurrentDatum converts current statement summaries to datum.
func (ssMap *stmtSummaryByDigestMap) ToCurrentDatum() [][]types.Datum {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	beginTime := ssMap.beginTimeForCurInterval
	ssMap.Unlock()

	rows := make([][]types.Datum, 0, len(values))
	for _, value := range values {
		record := value.(*stmtSummaryByDigest).toCurrentDatum(beginTime)
		if record != nil {
			rows = append(rows, record)
		}
	}
	return rows
}

// ToHistoryDatum converts history statements summaries to datum.
func (ssMap *stmtSummaryByDigestMap) ToHistoryDatum() [][]types.Datum {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	ssMap.Unlock()

	historySize := ssMap.historySize()
	rows := make([][]types.Datum, 0, len(values)*historySize)
	for _, value := range values {
		records := value.(*stmtSummaryByDigest).toHistoryDatum(historySize)
		rows = append(rows, records...)
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
			if ssbd.history.Len() > 0 {
				ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
				ssElement.Lock()
				if ssbd.history.Len() > 1 || ssElement.execCount > 1 {
					schemas = append(schemas, ssbd.schemaName)
					sqls = append(sqls, ssElement.sampleSQL)
				}
				ssElement.Unlock()
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
	// If session and global variables are both '', use the value in config.
	if interval <= 0 {
		interval = config.GetGlobalConfig().StmtSummary.RefreshInterval
	}
	atomic.StoreInt64(&ssMap.sysVars.refreshInterval, int64(interval))
}

// refreshInterval gets the refresh interval for summaries.
func (ssMap *stmtSummaryByDigestMap) refreshInterval() int64 {
	return atomic.LoadInt64(&ssMap.sysVars.refreshInterval)
}

// SetHistorySize sets the history size for all summaries.
func (ssMap *stmtSummaryByDigestMap) SetHistorySize(value string, inSession bool) {
	ssMap.sysVars.Lock()
	if inSession {
		ssMap.sysVars.sessionHistorySize = value
	} else {
		ssMap.sysVars.globalHistorySize = value
	}
	sessionHistorySize := ssMap.sysVars.sessionHistorySize
	globalHistorySize := ssMap.sysVars.globalHistorySize
	ssMap.sysVars.Unlock()

	// Calculate the cached `historySize`.
	size := -1
	var err error
	if ssMap.isSet(sessionHistorySize) {
		size, err = strconv.Atoi(sessionHistorySize)
		if err != nil {
			size = -1
		}
	}
	if size < 0 {
		size, err = strconv.Atoi(globalHistorySize)
		if err != nil {
			size = -1
		}
	}
	// If session and global variables are both '', use the value in config.
	if size < 0 {
		size = config.GetGlobalConfig().StmtSummary.HistorySize
	}
	atomic.StoreInt32(&ssMap.sysVars.historySize, int32(size))
}

// historySize gets the history size for summaries.
func (ssMap *stmtSummaryByDigestMap) historySize() int {
	return int(atomic.LoadInt32(&ssMap.sysVars.historySize))
}

// newStmtSummaryByDigest creates a stmtSummaryByDigest from StmtExecInfo.
func newStmtSummaryByDigest(sei *StmtExecInfo, beginTime int64, intervalSeconds int64, historySize int) *stmtSummaryByDigest {
	// Use "," to separate table names to support FIND_IN_SET.
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
		schemaName:    sei.SchemaName,
		digest:        sei.Digest,
		planDigest:    sei.PlanDigest,
		stmtType:      strings.ToLower(sei.StmtCtx.StmtType),
		normalizedSQL: formatSQL(sei.NormalizedSQL),
		tableNames:    tableNames,
		history:       list.New(),
	}
	ssbd.add(sei, beginTime, intervalSeconds, historySize)
	return ssbd
}

func (ssbd *stmtSummaryByDigest) add(sei *StmtExecInfo, beginTime int64, intervalSeconds int64, historySize int) {
	// Enclose this block in a function to ensure the lock will always be released.
	ssElement, isElementNew := func() (*stmtSummaryByDigestElement, bool) {
		ssbd.Lock()
		defer ssbd.Unlock()

		var ssElement *stmtSummaryByDigestElement
		isElementNew := true
		if ssbd.history.Len() > 0 {
			lastElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
			if lastElement.beginTime >= beginTime {
				ssElement = lastElement
				isElementNew = false
			} else {
				// The last elements expires to the history.
				lastElement.onExpire(intervalSeconds)
			}
		}
		if isElementNew {
			// If the element is new created, `ssElement.add(sei)` should be done inside the lock of `ssbd`.
			ssElement = newStmtSummaryByDigestElement(sei, beginTime, intervalSeconds)
			ssbd.history.PushBack(ssElement)
		}

		// `historySize` might be modified anytime, so check expiration every time.
		// Even if history is set to 0, current summary is still needed.
		for ssbd.history.Len() > historySize && ssbd.history.Len() > 1 {
			ssbd.history.Remove(ssbd.history.Front())
		}

		return ssElement, isElementNew
	}()

	// Lock a single entry, not the whole `ssbd`.
	if !isElementNew {
		ssElement.add(sei, intervalSeconds)
	}
}

func (ssbd *stmtSummaryByDigest) toCurrentDatum(beginTimeForCurInterval int64) []types.Datum {
	var ssElement *stmtSummaryByDigestElement

	ssbd.Lock()
	if ssbd.history.Len() > 0 {
		ssElement = ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	}
	ssbd.Unlock()

	// `ssElement` is lazy expired, so expired elements could also be read.
	// `beginTime` won't change since `ssElement` is created, so locking is not needed here.
	if ssElement == nil || ssElement.beginTime < beginTimeForCurInterval {
		return nil
	}
	return ssElement.toDatum(ssbd)
}

func (ssbd *stmtSummaryByDigest) toHistoryDatum(historySize int) [][]types.Datum {
	// Collect all history summaries to an array.
	ssElements := ssbd.collectHistorySummaries(historySize)

	rows := make([][]types.Datum, 0, len(ssElements))
	for _, ssElement := range ssElements {
		rows = append(rows, ssElement.toDatum(ssbd))
	}
	return rows
}

// collectHistorySummaries puts at most `historySize` summaries to an array.
func (ssbd *stmtSummaryByDigest) collectHistorySummaries(historySize int) []*stmtSummaryByDigestElement {
	ssbd.Lock()
	defer ssbd.Unlock()

	ssElements := make([]*stmtSummaryByDigestElement, 0, ssbd.history.Len())
	for listElement := ssbd.history.Front(); listElement != nil && len(ssElements) < historySize; listElement = listElement.Next() {
		ssElement := listElement.Value.(*stmtSummaryByDigestElement)
		ssElements = append(ssElements, ssElement)
	}
	return ssElements
}

func newStmtSummaryByDigestElement(sei *StmtExecInfo, beginTime int64, intervalSeconds int64) *stmtSummaryByDigestElement {
	// sampleSQL / sampleUser / samplePlan / prevSQL / indexNames store the values shown at the first time,
	// because it compacts performance to update every time.
	ssElement := &stmtSummaryByDigestElement{
		beginTime: beginTime,
		sampleSQL: formatSQL(sei.OriginalSQL),
		// PrevSQL is already truncated to cfg.Log.QueryLogMaxLen.
		prevSQL: sei.PrevSQL,
		// samplePlan needs to be decoded so it can't be truncated.
		samplePlan:   sei.PlanGenerator(),
		sampleUser:   sei.User,
		indexNames:   sei.StmtCtx.IndexNames,
		minLatency:   sei.TotalLatency,
		firstSeen:    sei.StartTime,
		lastSeen:     sei.StartTime,
		backoffTypes: make(map[fmt.Stringer]int, 0),
	}
	ssElement.add(sei, intervalSeconds)
	return ssElement
}

// onExpire is called when this element expires to history.
func (ssElement *stmtSummaryByDigestElement) onExpire(intervalSeconds int64) {
	ssElement.Lock()
	defer ssElement.Unlock()

	// refreshInterval may change anytime, so we need to update endTime.
	if ssElement.beginTime+intervalSeconds > ssElement.endTime {
		// // If interval changes to a bigger value, update endTime to beginTime + interval.
		ssElement.endTime = ssElement.beginTime + intervalSeconds
	} else if ssElement.beginTime+intervalSeconds < ssElement.endTime {
		now := time.Now().Unix()
		// If interval changes to a smaller value and now > beginTime + interval, update endTime to current time.
		if now > ssElement.beginTime+intervalSeconds {
			ssElement.endTime = now
		}
	}
}

func (ssElement *stmtSummaryByDigestElement) add(sei *StmtExecInfo, intervalSeconds int64) {
	ssElement.Lock()
	defer ssElement.Unlock()

	// refreshInterval may change anytime, update endTime ASAP.
	ssElement.endTime = ssElement.beginTime + intervalSeconds
	ssElement.execCount++

	// latency
	ssElement.sumLatency += sei.TotalLatency
	if sei.TotalLatency > ssElement.maxLatency {
		ssElement.maxLatency = sei.TotalLatency
	}
	if sei.TotalLatency < ssElement.minLatency {
		ssElement.minLatency = sei.TotalLatency
	}
	ssElement.sumParseLatency += sei.ParseLatency
	if sei.ParseLatency > ssElement.maxParseLatency {
		ssElement.maxParseLatency = sei.ParseLatency
	}
	ssElement.sumCompileLatency += sei.CompileLatency
	if sei.CompileLatency > ssElement.maxCompileLatency {
		ssElement.maxCompileLatency = sei.CompileLatency
	}

	// coprocessor
	numCopTasks := int64(sei.CopTasks.NumCopTasks)
	ssElement.numCopTasks += numCopTasks
	ssElement.sumCopProcessTime += sei.CopTasks.AvgProcessTime.Nanoseconds() * numCopTasks
	if sei.CopTasks.MaxProcessTime > ssElement.maxCopProcessTime {
		ssElement.maxCopProcessTime = sei.CopTasks.MaxProcessTime
		ssElement.maxCopProcessAddress = sei.CopTasks.MaxProcessAddress
	}
	ssElement.sumCopWaitTime += sei.CopTasks.AvgWaitTime.Nanoseconds() * numCopTasks
	if sei.CopTasks.MaxWaitTime > ssElement.maxCopWaitTime {
		ssElement.maxCopWaitTime = sei.CopTasks.MaxWaitTime
		ssElement.maxCopWaitAddress = sei.CopTasks.MaxWaitAddress
	}

	// TiKV
	ssElement.sumProcessTime += sei.ExecDetail.ProcessTime
	if sei.ExecDetail.ProcessTime > ssElement.maxProcessTime {
		ssElement.maxProcessTime = sei.ExecDetail.ProcessTime
	}
	ssElement.sumWaitTime += sei.ExecDetail.WaitTime
	if sei.ExecDetail.WaitTime > ssElement.maxWaitTime {
		ssElement.maxWaitTime = sei.ExecDetail.WaitTime
	}
	ssElement.sumBackoffTime += sei.ExecDetail.BackoffTime
	if sei.ExecDetail.BackoffTime > ssElement.maxBackoffTime {
		ssElement.maxBackoffTime = sei.ExecDetail.BackoffTime
	}
	ssElement.sumTotalKeys += sei.ExecDetail.TotalKeys
	if sei.ExecDetail.TotalKeys > ssElement.maxTotalKeys {
		ssElement.maxTotalKeys = sei.ExecDetail.TotalKeys
	}
	ssElement.sumProcessedKeys += sei.ExecDetail.ProcessedKeys
	if sei.ExecDetail.ProcessedKeys > ssElement.maxProcessedKeys {
		ssElement.maxProcessedKeys = sei.ExecDetail.ProcessedKeys
	}

	// txn
	commitDetails := sei.ExecDetail.CommitDetail
	if commitDetails != nil {
		ssElement.commitCount++
		ssElement.sumPrewriteTime += commitDetails.PrewriteTime
		if commitDetails.PrewriteTime > ssElement.maxPrewriteTime {
			ssElement.maxPrewriteTime = commitDetails.PrewriteTime
		}
		ssElement.sumCommitTime += commitDetails.CommitTime
		if commitDetails.CommitTime > ssElement.maxCommitTime {
			ssElement.maxCommitTime = commitDetails.CommitTime
		}
		ssElement.sumGetCommitTsTime += commitDetails.GetCommitTsTime
		if commitDetails.GetCommitTsTime > ssElement.maxGetCommitTsTime {
			ssElement.maxGetCommitTsTime = commitDetails.GetCommitTsTime
		}
		ssElement.sumCommitBackoffTime += commitDetails.CommitBackoffTime
		if commitDetails.CommitBackoffTime > ssElement.maxCommitBackoffTime {
			ssElement.maxCommitBackoffTime = commitDetails.CommitBackoffTime
		}
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLockTime)
		ssElement.sumResolveLockTime += resolveLockTime
		if resolveLockTime > ssElement.maxResolveLockTime {
			ssElement.maxResolveLockTime = resolveLockTime
		}
		ssElement.sumLocalLatchTime += commitDetails.LocalLatchTime
		if commitDetails.LocalLatchTime > ssElement.maxLocalLatchTime {
			ssElement.maxLocalLatchTime = commitDetails.LocalLatchTime
		}
		ssElement.sumWriteKeys += int64(commitDetails.WriteKeys)
		if commitDetails.WriteKeys > ssElement.maxWriteKeys {
			ssElement.maxWriteKeys = commitDetails.WriteKeys
		}
		ssElement.sumWriteSize += int64(commitDetails.WriteSize)
		if commitDetails.WriteSize > ssElement.maxWriteSize {
			ssElement.maxWriteSize = commitDetails.WriteSize
		}
		prewriteRegionNum := atomic.LoadInt32(&commitDetails.PrewriteRegionNum)
		ssElement.sumPrewriteRegionNum += int64(prewriteRegionNum)
		if prewriteRegionNum > ssElement.maxPrewriteRegionNum {
			ssElement.maxPrewriteRegionNum = prewriteRegionNum
		}
		ssElement.sumTxnRetry += int64(commitDetails.TxnRetry)
		if commitDetails.TxnRetry > ssElement.maxTxnRetry {
			ssElement.maxTxnRetry = commitDetails.TxnRetry
		}
		commitDetails.Mu.Lock()
		ssElement.sumBackoffTimes += int64(len(commitDetails.Mu.BackoffTypes))
		for _, backoffType := range commitDetails.Mu.BackoffTypes {
			ssElement.backoffTypes[backoffType] += 1
		}
		commitDetails.Mu.Unlock()
	}

	// other
	ssElement.sumAffectedRows += sei.StmtCtx.AffectedRows()
	ssElement.sumMem += sei.MemMax
	if sei.MemMax > ssElement.maxMem {
		ssElement.maxMem = sei.MemMax
	}
	if sei.StartTime.Before(ssElement.firstSeen) {
		ssElement.firstSeen = sei.StartTime
	}
	if ssElement.lastSeen.Before(sei.StartTime) {
		ssElement.lastSeen = sei.StartTime
	}
}

func (ssElement *stmtSummaryByDigestElement) toDatum(ssbd *stmtSummaryByDigest) []types.Datum {
	ssElement.Lock()
	defer ssElement.Unlock()

	plan, err := plancodec.DecodePlan(ssElement.samplePlan)
	if err != nil {
		logutil.Logger(context.Background()).Error("decode plan in statement summary failed", zap.String("plan", ssElement.samplePlan), zap.Error(err))
		plan = ""
	}

	// Actually, there's a small chance that endTime is out of date, but it's hard to keep it up to date all the time.
	return types.MakeDatums(
		types.Time{Time: types.FromGoTime(time.Unix(ssElement.beginTime, 0)), Type: mysql.TypeTimestamp},
		types.Time{Time: types.FromGoTime(time.Unix(ssElement.endTime, 0)), Type: mysql.TypeTimestamp},
		ssbd.stmtType,
		ssbd.schemaName,
		ssbd.digest,
		ssbd.normalizedSQL,
		convertEmptyToNil(ssbd.tableNames),
		convertEmptyToNil(strings.Join(ssElement.indexNames, ",")),
		convertEmptyToNil(ssElement.sampleUser),
		ssElement.execCount,
		int64(ssElement.sumLatency),
		int64(ssElement.maxLatency),
		int64(ssElement.minLatency),
		avgInt(int64(ssElement.sumLatency), ssElement.execCount),
		avgInt(int64(ssElement.sumParseLatency), ssElement.execCount),
		int64(ssElement.maxParseLatency),
		avgInt(int64(ssElement.sumCompileLatency), ssElement.execCount),
		int64(ssElement.maxCompileLatency),
		ssElement.numCopTasks,
		avgInt(ssElement.sumCopProcessTime, ssElement.numCopTasks),
		int64(ssElement.maxCopProcessTime),
		convertEmptyToNil(ssElement.maxCopProcessAddress),
		avgInt(ssElement.sumCopWaitTime, ssElement.numCopTasks),
		int64(ssElement.maxCopWaitTime),
		convertEmptyToNil(ssElement.maxCopWaitAddress),
		avgInt(int64(ssElement.sumProcessTime), ssElement.execCount),
		int64(ssElement.maxProcessTime),
		avgInt(int64(ssElement.sumWaitTime), ssElement.execCount),
		int64(ssElement.maxWaitTime),
		avgInt(int64(ssElement.sumBackoffTime), ssElement.execCount),
		int64(ssElement.maxBackoffTime),
		avgInt(ssElement.sumTotalKeys, ssElement.execCount),
		ssElement.maxTotalKeys,
		avgInt(ssElement.sumProcessedKeys, ssElement.execCount),
		ssElement.maxProcessedKeys,
		avgInt(int64(ssElement.sumPrewriteTime), ssElement.commitCount),
		int64(ssElement.maxPrewriteTime),
		avgInt(int64(ssElement.sumCommitTime), ssElement.commitCount),
		int64(ssElement.maxCommitTime),
		avgInt(int64(ssElement.sumGetCommitTsTime), ssElement.commitCount),
		int64(ssElement.maxGetCommitTsTime),
		avgInt(ssElement.sumCommitBackoffTime, ssElement.commitCount),
		ssElement.maxCommitBackoffTime,
		avgInt(ssElement.sumResolveLockTime, ssElement.commitCount),
		ssElement.maxResolveLockTime,
		avgInt(int64(ssElement.sumLocalLatchTime), ssElement.commitCount),
		int64(ssElement.maxLocalLatchTime),
		avgFloat(ssElement.sumWriteKeys, ssElement.commitCount),
		ssElement.maxWriteKeys,
		avgFloat(ssElement.sumWriteSize, ssElement.commitCount),
		ssElement.maxWriteSize,
		avgFloat(ssElement.sumPrewriteRegionNum, ssElement.commitCount),
		int(ssElement.maxPrewriteRegionNum),
		avgFloat(ssElement.sumTxnRetry, ssElement.commitCount),
		ssElement.maxTxnRetry,
		ssElement.sumBackoffTimes,
		formatBackoffTypes(ssElement.backoffTypes),
		avgInt(ssElement.sumMem, ssElement.execCount),
		ssElement.maxMem,
		avgFloat(int64(ssElement.sumAffectedRows), ssElement.execCount),
		types.Time{Time: types.FromGoTime(ssElement.firstSeen), Type: mysql.TypeTimestamp},
		types.Time{Time: types.FromGoTime(ssElement.lastSeen), Type: mysql.TypeTimestamp},
		ssElement.sampleSQL,
		ssElement.prevSQL,
		ssbd.planDigest,
		plan,
	)
}

// Truncate SQL to maxSQLLength.
func formatSQL(sql string) string {
	maxSQLLength := config.GetGlobalConfig().StmtSummary.MaxSQLLength
	length := len(sql)
	if length > int(maxSQLLength) {
		sql = fmt.Sprintf("%.*s(len:%d)", maxSQLLength, sql, length)
	}
	return sql
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
