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
	"container/list"
	"hash/fnv"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
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
	// Hash value of the plan before truncating.
	planHash uint64
	// `hash` is the hash value of this object
	hash []byte
}

// Hash implements SimpleLRUCache.Key
func (key *stmtSummaryByDigestKey) Hash() []byte {
	if len(key.hash) == 0 {
		key.hash = make([]byte, 0, len(key.schemaName)+len(key.digest)+8)
		key.hash = append(key.hash, hack.Slice(key.digest)...)
		key.hash = append(key.hash, hack.Slice(string(key.planHash))...)
		key.hash = append(key.hash, hack.Slice(strings.ToLower(key.schemaName))...)
	}
	return key.hash
}

// stmtSummaryByDigestMap is a LRU cache that stores statement summaries.
type stmtSummaryByDigestMap struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	sync.Mutex
	summaryMap *kvcache.SimpleLRUCache
	// These fields are used in updating summary history.
	beginTimeForCurInterval int64
	lastCheckExpireTime     int64
	oldestBeginTime         int64

	// sysVars encapsulates variables needed to judge whether statement summary is enabled.
	sysVars struct {
		sync.RWMutex
		// enabled indicates whether statement summary is enabled in current server.
		sessionEnabled string
		// setInSession indicates whether statement summary has been set in any session.
		globalEnabled string
		// historyHours indicates the max hours the history of summaries can be kept.
		// It must be > 0.
		sessionHistoryHours string
		globalHistoryHours  string
		historyHours        int32
		// intervalMinutes indicates the refresh interval of summaries.
		// It must be > 0.
		sessionIntervalMinutes string
		globalIntervalMinutes  string
		intervalMinutes        int32
	}
}

// StmtSummaryByDigestMap is a global map containing all statement summaries.
var StmtSummaryByDigestMap = newStmtSummaryByDigestMap()

type stmtSummaryByDigest struct {
	// Mutex is only used to lock `history`.
	sync.Mutex
	// Each element in history is a summary in one interval.
	history *list.List
	// Following fields are common for each interval and won't change once assigned.
	schemaName    string
	digest        string
	plan          string
	normalizedSQL string
	sampleSQL     string
	tableIDs      string
	indexNames    string
}

// stmtSummaryByDigest is the summary for each type of statements.
type stmtSummaryByDigestElement struct {
	sync.Mutex
	// Each summary is summarized between [beginTime, beginTime + interval]
	beginTime int64
	user      string
	execCount int64
	// Latency
	sumLatency        time.Duration
	maxLatency        time.Duration
	minLatency        time.Duration
	sumParseLatency   time.Duration
	maxParseLatency   time.Duration
	sumCompileLatency time.Duration
	maxCompileLatency time.Duration
	// CopTasks
	numCopTasks          int64
	sumCopProcessTime    int64
	maxCopProcessTime    time.Duration
	maxCopProcessAddress string
	sumCopWaitTime       int64
	maxCopWaitTime       time.Duration
	maxCopWaitAddress    string
	// ExecDetails
	sumProcessTime   time.Duration
	maxProcessTime   time.Duration
	sumWaitTime      time.Duration
	maxWaitTime      time.Duration
	sumBackoffTime   time.Duration
	maxBackoffTime   time.Duration
	sumRequestCount  int64
	maxRequestCount  int64
	sumTotalKeys     int64
	maxTotalKeys     int64
	sumProcessedKeys int64
	maxProcessedKeys int64
	// Other
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
	TableIDs       string
	IndexNames     string
	CopTasks       *stmtctx.CopTasksDetails
	ExecDetail     *execdetails.ExecDetails
	MemMax         int64
	Plan           string
	AffectedRows   uint64
	StartTime      time.Time
}

// newStmtSummaryByDigestMap creates an empty stmtSummaryByDigestMap.
func newStmtSummaryByDigestMap() *stmtSummaryByDigestMap {
	maxStmtCount := config.GetGlobalConfig().StmtSummary.MaxStmtCount
	ssMap := &stmtSummaryByDigestMap{
		summaryMap:              kvcache.NewSimpleLRUCache(maxStmtCount, 0, 0),
		beginTimeForCurInterval: 0,
		lastCheckExpireTime:     0,
		oldestBeginTime:         0,
	}
	// sysVars.defaultEnabled will be initialized in package variable.
	ssMap.sysVars.sessionEnabled = ""
	ssMap.sysVars.globalEnabled = ""
	return ssMap
}

// AddStatement adds a statement to StmtSummaryByDigestMap.
func (ssMap *stmtSummaryByDigestMap) AddStatement(sei *StmtExecInfo) {
	hash := fnv.New64()
	if _, err := hash.Write(hack.Slice(sei.Plan)); err != nil {
		terror.Log(err)
		return
	}
	key := &stmtSummaryByDigestKey{
		schemaName: sei.SchemaName,
		digest:     sei.Digest,
		planHash:   hash.Sum64(),
	}

	// All times are count in seconds.
	now := time.Now().Unix()

	// Enclose the block in a function to ensure the lock will always be released.
	value, ok := func() (kvcache.Value, bool) {
		ssMap.Lock()
		defer ssMap.Unlock()

		// Check again. Statements could be added before disabling the flag and after Clear().
		if !ssMap.Enabled() {
			return nil, false
		}

		// Check expiration of history every minute.
		if now >= ssMap.lastCheckExpireTime+60 {
			intervalSeconds := int64(atomic.LoadInt32(&ssMap.sysVars.intervalMinutes) * 60)
			if ssMap.beginTimeForCurInterval+intervalSeconds <= now {
				// `beginTimeForCurInterval` is a multiple of intervalSeconds.
				ssMap.beginTimeForCurInterval = now / intervalSeconds * intervalSeconds
			}
			historySeconds := int64(atomic.LoadInt32(&ssMap.sysVars.historyHours)) * 60 * 60
			ssMap.oldestBeginTime = ssMap.beginTimeForCurInterval - historySeconds
			// `lastCheckExpireTime` is a multiple of 60.
			ssMap.lastCheckExpireTime = now / 60 * 60
		}

		value, ok := ssMap.summaryMap.Get(key)
		if !ok {
			newSummary := newStmtSummaryByDigest(sei, ssMap.beginTimeForCurInterval, ssMap.oldestBeginTime)
			ssMap.summaryMap.Put(key, newSummary)
		}
		return value, ok
	}()

	// Lock a single entry, not the whole cache.
	if ok {
		value.(*stmtSummaryByDigest).add(sei, ssMap.beginTimeForCurInterval, ssMap.oldestBeginTime)
	}
}

// Clear removes all statement summaries.
func (ssMap *stmtSummaryByDigestMap) Clear() {
	ssMap.Lock()
	defer ssMap.Unlock()

	ssMap.summaryMap.DeleteAll()
	ssMap.beginTimeForCurInterval = 0
	ssMap.lastCheckExpireTime = 0
	ssMap.oldestBeginTime = 0
}

// ToCurrentDatum converts statement summary in current interval to Datum.
func (ssMap *stmtSummaryByDigestMap) ToCurrentDatum() [][]types.Datum {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	ssMap.Unlock()

	rows := make([][]types.Datum, 0, len(values))
	for _, value := range values {
		summary := value.(*stmtSummaryByDigest)
		record := summary.toCurrentDatum(ssMap.beginTimeForCurInterval)
		rows = append(rows, record...)
	}

	return rows
}

// ToHistoryDatum converts statement summary in the history to Datum.
func (ssMap *stmtSummaryByDigestMap) ToHistoryDatum() [][]types.Datum {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	ssMap.Unlock()

	rows := make([][]types.Datum, 0, len(values))
	for _, value := range values {
		summary := value.(*stmtSummaryByDigest)
		records := summary.toHistoryDatum(ssMap.oldestBeginTime)
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
		summary := value.(*stmtSummaryByDigest)
		var ssElement *stmtSummaryByDigestElement
		if strings.HasPrefix(summary.normalizedSQL, "select") {
			summary.Lock()
			if summary.history.Len() > 0 {
				ssElement = summary.history.Back().Value.(*stmtSummaryByDigestElement)
			}
			summary.Unlock()
		}

		if ssElement != nil {
			ssElement.Lock()
			if ssElement.execCount > 1 {
				schemas = append(schemas, summary.schemaName)
				sqls = append(sqls, summary.sampleSQL)
			}
			ssElement.Unlock()
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

// SetHistoryMaxHours sets history hours in ssMap.sysVars.
func (ssMap *stmtSummaryByDigestMap) SetHistoryMaxHours(value string, inSession bool) {
	ssMap.sysVars.Lock()
	if inSession {
		ssMap.sysVars.sessionHistoryHours = value
	} else {
		ssMap.sysVars.globalHistoryHours = value
	}
	sessionHistoryHours := ssMap.sysVars.sessionHistoryHours
	globalHistoryHours := ssMap.sysVars.globalHistoryHours
	ssMap.sysVars.Unlock()

	var hours int
	var err error
	if ssMap.isSet(sessionHistoryHours) {
		hours, err = strconv.Atoi(sessionHistoryHours)
		if err != nil {
			hours = 0
		}
	}
	if hours <= 0 {
		hours, err = strconv.Atoi(globalHistoryHours)
		if err != nil {
			hours = 0
		}
	}
	if hours > 0 {
		// In case of overflow in calculation.
		if hours > math.MaxInt16 {
			hours = math.MaxInt16
		}
		atomic.StoreInt32(&ssMap.sysVars.historyHours, int32(hours))
	}
}

// SetIntervalMinutes sets interval minutes in ssMap.sysVars.
func (ssMap *stmtSummaryByDigestMap) SetIntervalMinutes(value string, inSession bool) {
	ssMap.sysVars.Lock()
	if inSession {
		ssMap.sysVars.sessionIntervalMinutes = value
	} else {
		ssMap.sysVars.globalIntervalMinutes = value
	}
	sessionIntervalMinutes := ssMap.sysVars.sessionIntervalMinutes
	globalIntervalMinutes := ssMap.sysVars.globalIntervalMinutes
	ssMap.sysVars.Unlock()

	var minutes int
	var err error
	if ssMap.isSet(sessionIntervalMinutes) {
		minutes, err = strconv.Atoi(sessionIntervalMinutes)
		if err != nil {
			minutes = 0
		}
	}
	if minutes <= 0 {
		minutes, err = strconv.Atoi(globalIntervalMinutes)
		if err != nil {
			minutes = 0
		}
	}
	if minutes > 0 {
		// In case of overflow in calculation.
		if minutes > math.MaxInt16 {
			minutes = math.MaxInt16
		}
		atomic.StoreInt32(&ssMap.sysVars.intervalMinutes, int32(minutes))
	}
}

// newStmtSummaryByDigest creates a stmtSummaryByDigest from StmtExecInfo
func newStmtSummaryByDigest(sei *StmtExecInfo, latestBeginTime int64, oldestBeginTime int64) *stmtSummaryByDigest {
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
	plan := sei.Plan
	if len(plan) > int(maxSQLLength) {
		plan = plan[:maxSQLLength]
	}

	ssbd := &stmtSummaryByDigest{
		schemaName:    sei.SchemaName,
		digest:        sei.Digest,
		plan:          plan,
		normalizedSQL: normalizedSQL,
		sampleSQL:     sampleSQL,
		tableIDs:      sei.TableIDs,
		indexNames:    sei.IndexNames,
		history:       list.New(),
	}
	ssbd.add(sei, latestBeginTime, oldestBeginTime)
	return ssbd
}

// Add a StmtExecInfo to stmtSummary
func (ssbd *stmtSummaryByDigest) add(sei *StmtExecInfo, latestBeginTime int64, oldestBeginTime int64) {
	ssbd.Lock()
	defer ssbd.Unlock()

	var ssElement *stmtSummaryByDigestElement
	if ssbd.history.Len() > 0 {
		latestElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
		if latestElement.beginTime < latestBeginTime {
			// Remove expired elements when a new element is to be added.
			for {
				oldestElement := ssbd.history.Front()
				if oldestElement != nil && oldestElement.Value.(*stmtSummaryByDigestElement).beginTime < oldestBeginTime {
					ssbd.history.Remove(oldestElement)
				} else {
					break
				}
			}
		} else {
			ssElement = latestElement
		}
	}
	// Create a summary for current interval.
	if ssElement == nil {
		ssElement = newStmtSummaryByDigestElement(sei, latestBeginTime)
		ssbd.history.PushBack(ssElement)
	}

	// `sei` must be added to ssElement before it can be read, so it's still in the lock of `ssbd`.
	ssElement.add(sei)
}

func (ssbd *stmtSummaryByDigest) toCurrentDatum(beginTimeForCurInterval int64) [][]types.Datum {
	var ssElement *stmtSummaryByDigestElement

	ssbd.Lock()
	if ssbd.history.Len() > 0 {
		ssElement = ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
	}
	ssbd.Unlock()

	rows := make([][]types.Datum, 0, 1)
	// `ssElement` is lazy removed, so expired elements could also be read.
	// `beginTime` won't change, so lock is not needed here.
	if ssElement != nil && ssElement.beginTime >= beginTimeForCurInterval {
		rows = append(rows, ssElement.toDatum(ssbd))
	}
	return rows
}

func (ssbd *stmtSummaryByDigest) toHistoryDatum(oldestBeginTime int64) [][]types.Datum {
	// Collect all history summaries to an array.
	ssElements := ssbd.collectHistorySummaries(oldestBeginTime)

	rows := make([][]types.Datum, 0, len(ssElements))
	for _, ssElement := range ssElements {
		rows = append(rows, ssElement.toDatum(ssbd))
	}
	return rows
}

func (ssbd *stmtSummaryByDigest) collectHistorySummaries(oldestBeginTime int64) []*stmtSummaryByDigestElement {
	ssbd.Lock()
	defer ssbd.Unlock()

	ssElements := make([]*stmtSummaryByDigestElement, 0, ssbd.history.Len())
	for listElement := ssbd.history.Front(); listElement != nil; listElement = listElement.Next() {
		ssElement := listElement.Value.(*stmtSummaryByDigestElement)
		// `ssElements` are lazy removed, so expired elements may be read.
		if ssElement.beginTime > oldestBeginTime {
			ssElements = append(ssElements, ssElement)
		}
	}
	return ssElements
}

func newStmtSummaryByDigestElement(sei *StmtExecInfo, beginTime int64) *stmtSummaryByDigestElement {
	return &stmtSummaryByDigestElement{
		beginTime:  beginTime,
		user:       sei.User,
		minLatency: sei.TotalLatency,
		firstSeen:  sei.StartTime,
		lastSeen:   sei.StartTime,
	}
}

func (ssElement *stmtSummaryByDigestElement) add(sei *StmtExecInfo) {
	ssElement.Lock()
	defer ssElement.Unlock()

	if ssElement.user == "" {
		ssElement.user = sei.User
	}
	ssElement.execCount++

	// Latency
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

	// Coprocessor tasks
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

	// Execute details
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
	ssElement.sumRequestCount += int64(sei.ExecDetail.RequestCount)
	if int64(sei.ExecDetail.RequestCount) > ssElement.maxRequestCount {
		ssElement.maxRequestCount = int64(sei.ExecDetail.RequestCount)
	}
	ssElement.sumTotalKeys += sei.ExecDetail.TotalKeys
	if sei.ExecDetail.TotalKeys > ssElement.maxTotalKeys {
		ssElement.maxTotalKeys = sei.ExecDetail.TotalKeys
	}
	ssElement.sumProcessedKeys += sei.ExecDetail.ProcessedKeys
	if sei.ExecDetail.ProcessedKeys > ssElement.maxProcessedKeys {
		ssElement.maxProcessedKeys = sei.ExecDetail.ProcessedKeys
	}

	// Other
	ssElement.sumMem += sei.MemMax
	if sei.MemMax > ssElement.maxMem {
		ssElement.maxMem = sei.MemMax
	}
	ssElement.sumAffectedRows += sei.AffectedRows
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

	return types.MakeDatums(
		types.Time{Time: types.FromGoTime(time.Unix(ssElement.beginTime, 0)), Type: mysql.TypeTimestamp},
		ssbd.schemaName,
		ssbd.digest,
		ssbd.normalizedSQL,
		ssbd.tableIDs,
		ssbd.indexNames,
		ssElement.user,
		ssElement.execCount,
		int64(ssElement.sumLatency),
		int64(ssElement.maxLatency),
		int64(ssElement.minLatency),
		avg(int64(ssElement.sumLatency), ssElement.execCount),
		avg(int64(ssElement.sumParseLatency), ssElement.execCount),
		int64(ssElement.maxParseLatency),
		avg(int64(ssElement.sumCompileLatency), ssElement.execCount),
		int64(ssElement.maxCompileLatency),
		ssElement.numCopTasks,
		avg(ssElement.sumCopProcessTime, ssElement.numCopTasks),
		int64(ssElement.maxCopProcessTime),
		ssElement.maxCopProcessAddress,
		avg(ssElement.sumCopWaitTime, ssElement.numCopTasks),
		int64(ssElement.maxCopWaitTime),
		ssElement.maxCopWaitAddress,
		avg(int64(ssElement.sumProcessTime), ssElement.execCount),
		int64(ssElement.maxProcessTime),
		avg(int64(ssElement.sumWaitTime), ssElement.execCount),
		int64(ssElement.maxWaitTime),
		avg(int64(ssElement.sumBackoffTime), ssElement.execCount),
		int64(ssElement.maxBackoffTime),
		avg(ssElement.sumRequestCount, ssElement.execCount),
		ssElement.maxRequestCount,
		avg(ssElement.sumTotalKeys, ssElement.execCount),
		ssElement.maxTotalKeys,
		avg(ssElement.sumProcessedKeys, ssElement.execCount),
		ssElement.maxProcessedKeys,
		avg(ssElement.sumMem, ssElement.execCount),
		ssElement.maxMem,
		ssElement.sumAffectedRows,
		types.Time{Time: types.FromGoTime(ssElement.firstSeen), Type: mysql.TypeTimestamp},
		types.Time{Time: types.FromGoTime(ssElement.lastSeen), Type: mysql.TypeTimestamp},
		ssbd.sampleSQL,
	)
}

func avg(sum int64, count int64) int64 {
	if count > 0 {
		return sum / count
	}
	return 0
}
