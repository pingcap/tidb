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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stmtsummary

import (
	"bytes"
	"container/list"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/util/execdetails"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/plancodec"
	"github.com/tikv/client-go/v2/util"
	atomic2 "go.uber.org/atomic"
)

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

	// These options are set by global system variables and are accessed concurrently.
	optEnabled             *atomic2.Bool
	optEnableInternalQuery *atomic2.Bool
	optMaxStmtCount        *atomic2.Uint32
	optRefreshInterval     *atomic2.Int64
	optHistorySize         *atomic2.Int32
	optMaxSQLLength        *atomic2.Int32

	// other stores summary of evicted data.
	other *stmtSummaryByDigestEvicted
}

// StmtSummaryByDigestMap is a global map containing all statement summaries.
var StmtSummaryByDigestMap = newStmtSummaryByDigestMap()

// stmtSummaryByDigest is the summary for each type of statements.
type stmtSummaryByDigest struct {
	// It's rare to read concurrently, so RWMutex is not needed.
	// Mutex is only used to lock `history`.
	sync.Mutex
	initialized bool
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
	isInternal    bool
}

// stmtSummaryByDigestElement is the summary for each type of statements in current interval.
type stmtSummaryByDigestElement struct {
	sync.Mutex
	// Each summary is summarized between [beginTime, endTime).
	beginTime int64
	endTime   int64
	// basic
	sampleSQL   string
	charset     string
	collation   string
	prevSQL     string
	samplePlan  string
	planHint    string
	indexNames  []string
	execCount   int64
	sumErrors   int
	sumWarnings int
	// latency
	sumLatency        time.Duration
	maxLatency        time.Duration
	minLatency        time.Duration
	sumParseLatency   time.Duration
	maxParseLatency   time.Duration
	sumCompileLatency time.Duration
	maxCompileLatency time.Duration
	// coprocessor
	sumNumCopTasks       int64
	maxCopProcessTime    time.Duration
	maxCopProcessAddress string
	maxCopWaitTime       time.Duration
	maxCopWaitAddress    string
	// TiKV
	sumProcessTime               time.Duration
	maxProcessTime               time.Duration
	sumWaitTime                  time.Duration
	maxWaitTime                  time.Duration
	sumBackoffTime               time.Duration
	maxBackoffTime               time.Duration
	sumTotalKeys                 int64
	maxTotalKeys                 int64
	sumProcessedKeys             int64
	maxProcessedKeys             int64
	sumRocksdbDeleteSkippedCount uint64
	maxRocksdbDeleteSkippedCount uint64
	sumRocksdbKeySkippedCount    uint64
	maxRocksdbKeySkippedCount    uint64
	sumRocksdbBlockCacheHitCount uint64
	maxRocksdbBlockCacheHitCount uint64
	sumRocksdbBlockReadCount     uint64
	maxRocksdbBlockReadCount     uint64
	sumRocksdbBlockReadByte      uint64
	maxRocksdbBlockReadByte      uint64
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
	backoffTypes         map[string]int
	authUsers            map[string]struct{}
	// other
	sumMem               int64
	maxMem               int64
	sumDisk              int64
	maxDisk              int64
	sumAffectedRows      uint64
	sumKVTotal           time.Duration
	sumPDTotal           time.Duration
	sumBackoffTotal      time.Duration
	sumWriteSQLRespTotal time.Duration
	sumResultRows        int64
	maxResultRows        int64
	minResultRows        int64
	prepared             bool
	// The first time this type of SQL executes.
	firstSeen time.Time
	// The last time this type of SQL executes.
	lastSeen time.Time
	// plan cache
	planInCache   bool
	planCacheHits int64
	planInBinding bool
	// pessimistic execution retry information.
	execRetryCount uint
	execRetryTime  time.Duration
}

// StmtExecInfo records execution information of each statement.
type StmtExecInfo struct {
	SchemaName     string
	OriginalSQL    string
	Charset        string
	Collation      string
	NormalizedSQL  string
	Digest         string
	PrevSQL        string
	PrevSQLDigest  string
	PlanGenerator  func() (string, string)
	PlanDigest     string
	PlanDigestGen  func() string
	User           string
	TotalLatency   time.Duration
	ParseLatency   time.Duration
	CompileLatency time.Duration
	StmtCtx        *stmtctx.StatementContext
	CopTasks       *stmtctx.CopTasksDetails
	ExecDetail     *execdetails.ExecDetails
	MemMax         int64
	DiskMax        int64
	StartTime      time.Time
	IsInternal     bool
	Succeed        bool
	PlanInCache    bool
	PlanInBinding  bool
	ExecRetryCount uint
	ExecRetryTime  time.Duration
	execdetails.StmtExecDetails
	ResultRows      int64
	TiKVExecDetails util.ExecDetails
	Prepared        bool
}

// newStmtSummaryByDigestMap creates an empty stmtSummaryByDigestMap.
func newStmtSummaryByDigestMap() *stmtSummaryByDigestMap {

	ssbde := newStmtSummaryByDigestEvicted()

	// This initializes the stmtSummaryByDigestMap with "compiled defaults"
	// (which are regrettably duplicated from sessionctx/variable/tidb_vars.go).
	// Unfortunately we need to do this to avoid circular dependencies, but the correct
	// values will be applied on startup as soon as domain.LoadSysVarCacheLoop() is called,
	// which in turn calls func domain.checkEnableServerGlobalVar(name, sVal string) for each sysvar.
	// Currently this is early enough in the startup sequence.
	maxStmtCount := uint(3000)
	newSsMap := &stmtSummaryByDigestMap{
		summaryMap:             kvcache.NewSimpleLRUCache(maxStmtCount, 0, 0),
		optMaxStmtCount:        atomic2.NewUint32(uint32(maxStmtCount)),
		optEnabled:             atomic2.NewBool(true),
		optEnableInternalQuery: atomic2.NewBool(false),
		optRefreshInterval:     atomic2.NewInt64(1800),
		optHistorySize:         atomic2.NewInt32(24),
		optMaxSQLLength:        atomic2.NewInt32(4096),
		other:                  ssbde,
	}
	newSsMap.summaryMap.SetOnEvict(func(k kvcache.Key, v kvcache.Value) {
		historySize := newSsMap.historySize()
		newSsMap.other.AddEvicted(k.(*stmtSummaryByDigestKey), v.(*stmtSummaryByDigest), historySize)
	})
	return newSsMap
}

// AddStatement adds a statement to StmtSummaryByDigestMap.
func (ssMap *stmtSummaryByDigestMap) AddStatement(sei *StmtExecInfo) {
	// All times are counted in seconds.
	now := time.Now().Unix()

	failpoint.Inject("mockTimeForStatementsSummary", func(val failpoint.Value) {
		// mockTimeForStatementsSummary takes string of Unix timestamp
		if unixTimeStr, ok := val.(string); ok {
			unixTime, err := strconv.ParseInt(unixTimeStr, 10, 64)
			if err != nil {
				panic(err.Error())
			} else {
				now = unixTime
			}
		}
	})

	intervalSeconds := ssMap.refreshInterval()
	historySize := ssMap.historySize()

	key := &stmtSummaryByDigestKey{
		schemaName: sei.SchemaName,
		digest:     sei.Digest,
		prevDigest: sei.PrevSQLDigest,
		planDigest: sei.PlanDigest,
	}
	// Calculate hash value in advance, to reduce the time holding the lock.
	key.Hash()

	// Enclose the block in a function to ensure the lock will always be released.
	summary, beginTime := func() (*stmtSummaryByDigest, int64) {
		ssMap.Lock()
		defer ssMap.Unlock()

		// Check again. Statements could be added before disabling the flag and after Clear().
		if !ssMap.Enabled() {
			return nil, 0
		}
		if sei.IsInternal && !ssMap.EnabledInternal() {
			return nil, 0
		}

		if ssMap.beginTimeForCurInterval+intervalSeconds <= now {
			// `beginTimeForCurInterval` is a multiple of intervalSeconds, so that when the interval is a multiple
			// of 60 (or 600, 1800, 3600, etc), begin time shows 'XX:XX:00', not 'XX:XX:01'~'XX:XX:59'.
			ssMap.beginTimeForCurInterval = now / intervalSeconds * intervalSeconds
		}

		beginTime := ssMap.beginTimeForCurInterval
		value, ok := ssMap.summaryMap.Get(key)
		var summary *stmtSummaryByDigest
		if !ok {
			// Lazy initialize it to release ssMap.mutex ASAP.
			summary = new(stmtSummaryByDigest)
			ssMap.summaryMap.Put(key, summary)
		} else {
			summary = value.(*stmtSummaryByDigest)
		}
		summary.isInternal = summary.isInternal && sei.IsInternal
		return summary, beginTime
	}()
	// Lock a single entry, not the whole cache.
	if summary != nil {
		summary.add(sei, beginTime, intervalSeconds, historySize)
	}
}

// Clear removes all statement summaries.
func (ssMap *stmtSummaryByDigestMap) Clear() {
	ssMap.Lock()
	defer ssMap.Unlock()

	ssMap.summaryMap.DeleteAll()
	ssMap.other.Clear()
	ssMap.beginTimeForCurInterval = 0
}

// clearInternal removes all statement summaries which are internal summaries.
func (ssMap *stmtSummaryByDigestMap) clearInternal() {
	ssMap.Lock()
	defer ssMap.Unlock()

	for _, key := range ssMap.summaryMap.Keys() {
		summary, ok := ssMap.summaryMap.Get(key)
		if !ok {
			continue
		}
		if summary.(*stmtSummaryByDigest).isInternal {
			ssMap.summaryMap.Delete(key)
		}
	}
}

// BindableStmt is a wrapper struct for a statement that is extracted from statements_summary and can be
// created binding on.
type BindableStmt struct {
	Schema    string
	Query     string
	PlanHint  string
	Charset   string
	Collation string
	Users     map[string]struct{} // which users have processed this stmt
}

// GetMoreThanCntBindableStmt gets users' select/update/delete SQLs that occurred more than the specified count.
func (ssMap *stmtSummaryByDigestMap) GetMoreThanCntBindableStmt(cnt int64) []*BindableStmt {
	ssMap.Lock()
	values := ssMap.summaryMap.Values()
	ssMap.Unlock()

	stmts := make([]*BindableStmt, 0, len(values))
	for _, value := range values {
		ssbd := value.(*stmtSummaryByDigest)
		func() {
			ssbd.Lock()
			defer ssbd.Unlock()
			if ssbd.initialized && (ssbd.stmtType == "Select" || ssbd.stmtType == "Delete" || ssbd.stmtType == "Update" || ssbd.stmtType == "Insert" || ssbd.stmtType == "Replace") {
				if ssbd.history.Len() > 0 {
					ssElement := ssbd.history.Back().Value.(*stmtSummaryByDigestElement)
					ssElement.Lock()

					// Empty auth users means that it is an internal queries.
					if len(ssElement.authUsers) > 0 && (int64(ssbd.history.Len()) > cnt || ssElement.execCount > cnt) {
						stmt := &BindableStmt{
							Schema:    ssbd.schemaName,
							Query:     ssElement.sampleSQL,
							PlanHint:  ssElement.planHint,
							Charset:   ssElement.charset,
							Collation: ssElement.collation,
							Users:     ssElement.authUsers,
						}
						// If it is SQL command prepare / execute, the ssElement.sampleSQL is `execute ...`, we should get the original select query.
						// If it is binary protocol prepare / execute, ssbd.normalizedSQL should be same as ssElement.sampleSQL.
						if ssElement.prepared {
							stmt.Query = ssbd.normalizedSQL
						}
						stmts = append(stmts, stmt)
					}
					ssElement.Unlock()
				}
			}
		}()
	}
	return stmts
}

// SetEnabled enables or disables statement summary
func (ssMap *stmtSummaryByDigestMap) SetEnabled(value bool) error {
	// `optEnabled` and `ssMap` don't need to be strictly atomically updated.
	ssMap.optEnabled.Store(value)
	if !value {
		ssMap.Clear()
	}
	return nil
}

// Enabled returns whether statement summary is enabled.
func (ssMap *stmtSummaryByDigestMap) Enabled() bool {
	return ssMap.optEnabled.Load()
}

// SetEnabledInternalQuery enables or disables internal statement summary
func (ssMap *stmtSummaryByDigestMap) SetEnabledInternalQuery(value bool) error {
	// `optEnableInternalQuery` and `ssMap` don't need to be strictly atomically updated.
	ssMap.optEnableInternalQuery.Store(value)
	if !value {
		ssMap.clearInternal()
	}
	return nil
}

// EnabledInternal returns whether internal statement summary is enabled.
func (ssMap *stmtSummaryByDigestMap) EnabledInternal() bool {
	return ssMap.optEnableInternalQuery.Load()
}

// SetRefreshInterval sets refreshing interval in ssMap.sysVars.
func (ssMap *stmtSummaryByDigestMap) SetRefreshInterval(value int64) error {
	ssMap.optRefreshInterval.Store(value)
	return nil
}

// refreshInterval gets the refresh interval for summaries.
func (ssMap *stmtSummaryByDigestMap) refreshInterval() int64 {
	return ssMap.optRefreshInterval.Load()
}

// SetHistorySize sets the history size for all summaries.
func (ssMap *stmtSummaryByDigestMap) SetHistorySize(value int) error {
	ssMap.optHistorySize.Store(int32(value))
	return nil
}

// historySize gets the history size for summaries.
func (ssMap *stmtSummaryByDigestMap) historySize() int {
	return int(ssMap.optHistorySize.Load())
}

// SetHistorySize sets the history size for all summaries.
func (ssMap *stmtSummaryByDigestMap) SetMaxStmtCount(value uint) error {
	// `optMaxStmtCount` and `ssMap` don't need to be strictly atomically updated.
	ssMap.optMaxStmtCount.Store(uint32(value))

	ssMap.Lock()
	defer ssMap.Unlock()
	return ssMap.summaryMap.SetCapacity(value)
}

// Used by tests
// nolint: unused
func (ssMap *stmtSummaryByDigestMap) maxStmtCount() int {
	return int(ssMap.optMaxStmtCount.Load())
}

// SetHistorySize sets the history size for all summaries.
func (ssMap *stmtSummaryByDigestMap) SetMaxSQLLength(value int) error {
	ssMap.optMaxSQLLength.Store(int32(value))
	return nil
}

func (ssMap *stmtSummaryByDigestMap) maxSQLLength() int {
	return int(ssMap.optMaxSQLLength.Load())
}

// newStmtSummaryByDigest creates a stmtSummaryByDigest from StmtExecInfo.
func (ssbd *stmtSummaryByDigest) init(sei *StmtExecInfo, beginTime int64, intervalSeconds int64, historySize int) {
	// Use "," to separate table names to support FIND_IN_SET.
	var buffer bytes.Buffer
	for i, value := range sei.StmtCtx.Tables {
		// In `create database` statement, DB name is not empty but table name is empty.
		if len(value.Table) == 0 {
			continue
		}
		buffer.WriteString(strings.ToLower(value.DB))
		buffer.WriteString(".")
		buffer.WriteString(strings.ToLower(value.Table))
		if i < len(sei.StmtCtx.Tables)-1 {
			buffer.WriteString(",")
		}
	}
	tableNames := buffer.String()

	planDigest := sei.PlanDigest
	if sei.PlanDigestGen != nil && len(planDigest) == 0 {
		// It comes here only when the plan is 'Point_Get'.
		planDigest = sei.PlanDigestGen()
	}
	ssbd.schemaName = sei.SchemaName
	ssbd.digest = sei.Digest
	ssbd.planDigest = planDigest
	ssbd.stmtType = sei.StmtCtx.StmtType
	ssbd.normalizedSQL = formatSQL(sei.NormalizedSQL)
	ssbd.tableNames = tableNames
	ssbd.history = list.New()
	ssbd.initialized = true
}

func (ssbd *stmtSummaryByDigest) add(sei *StmtExecInfo, beginTime int64, intervalSeconds int64, historySize int) {
	// Enclose this block in a function to ensure the lock will always be released.
	ssElement, isElementNew := func() (*stmtSummaryByDigestElement, bool) {
		ssbd.Lock()
		defer ssbd.Unlock()

		if !ssbd.initialized {
			ssbd.init(sei, beginTime, intervalSeconds, historySize)
		}

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

// collectHistorySummaries puts at most `historySize` summaries to an array.
func (ssbd *stmtSummaryByDigest) collectHistorySummaries(checker *stmtSummaryChecker, historySize int) []*stmtSummaryByDigestElement {
	ssbd.Lock()
	defer ssbd.Unlock()

	if !ssbd.initialized {
		return nil
	}
	if checker != nil && !checker.isDigestValid(ssbd.digest) {
		return nil
	}

	ssElements := make([]*stmtSummaryByDigestElement, 0, ssbd.history.Len())
	for listElement := ssbd.history.Front(); listElement != nil && len(ssElements) < historySize; listElement = listElement.Next() {
		ssElement := listElement.Value.(*stmtSummaryByDigestElement)
		ssElements = append(ssElements, ssElement)
	}
	return ssElements
}

var maxEncodedPlanSizeInBytes = 1024 * 1024

func newStmtSummaryByDigestElement(sei *StmtExecInfo, beginTime int64, intervalSeconds int64) *stmtSummaryByDigestElement {
	// sampleSQL / authUsers(sampleUser) / samplePlan / prevSQL / indexNames store the values shown at the first time,
	// because it compacts performance to update every time.
	samplePlan, planHint := sei.PlanGenerator()
	if len(samplePlan) > maxEncodedPlanSizeInBytes {
		samplePlan = plancodec.PlanDiscardedEncoded
	}
	ssElement := &stmtSummaryByDigestElement{
		beginTime: beginTime,
		sampleSQL: formatSQL(sei.OriginalSQL),
		charset:   sei.Charset,
		collation: sei.Collation,
		// PrevSQL is already truncated to cfg.Log.QueryLogMaxLen.
		prevSQL: sei.PrevSQL,
		// samplePlan needs to be decoded so it can't be truncated.
		samplePlan:    samplePlan,
		planHint:      planHint,
		indexNames:    sei.StmtCtx.IndexNames,
		minLatency:    sei.TotalLatency,
		firstSeen:     sei.StartTime,
		lastSeen:      sei.StartTime,
		backoffTypes:  make(map[string]int),
		authUsers:     make(map[string]struct{}),
		planInCache:   false,
		planCacheHits: 0,
		planInBinding: false,
		prepared:      sei.Prepared,
		minResultRows: math.MaxInt64,
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

	// add user to auth users set
	if len(sei.User) > 0 {
		ssElement.authUsers[sei.User] = struct{}{}
	}

	// refreshInterval may change anytime, update endTime ASAP.
	ssElement.endTime = ssElement.beginTime + intervalSeconds
	ssElement.execCount++
	if !sei.Succeed {
		ssElement.sumErrors += 1
	}
	ssElement.sumWarnings += int(sei.StmtCtx.WarningCount())

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
	ssElement.sumNumCopTasks += numCopTasks
	if sei.CopTasks.MaxProcessTime > ssElement.maxCopProcessTime {
		ssElement.maxCopProcessTime = sei.CopTasks.MaxProcessTime
		ssElement.maxCopProcessAddress = sei.CopTasks.MaxProcessAddress
	}
	if sei.CopTasks.MaxWaitTime > ssElement.maxCopWaitTime {
		ssElement.maxCopWaitTime = sei.CopTasks.MaxWaitTime
		ssElement.maxCopWaitAddress = sei.CopTasks.MaxWaitAddress
	}

	// TiKV
	ssElement.sumProcessTime += sei.ExecDetail.TimeDetail.ProcessTime
	if sei.ExecDetail.TimeDetail.ProcessTime > ssElement.maxProcessTime {
		ssElement.maxProcessTime = sei.ExecDetail.TimeDetail.ProcessTime
	}
	ssElement.sumWaitTime += sei.ExecDetail.TimeDetail.WaitTime
	if sei.ExecDetail.TimeDetail.WaitTime > ssElement.maxWaitTime {
		ssElement.maxWaitTime = sei.ExecDetail.TimeDetail.WaitTime
	}
	ssElement.sumBackoffTime += sei.ExecDetail.BackoffTime
	if sei.ExecDetail.BackoffTime > ssElement.maxBackoffTime {
		ssElement.maxBackoffTime = sei.ExecDetail.BackoffTime
	}

	if sei.ExecDetail.ScanDetail != nil {
		ssElement.sumTotalKeys += sei.ExecDetail.ScanDetail.TotalKeys
		if sei.ExecDetail.ScanDetail.TotalKeys > ssElement.maxTotalKeys {
			ssElement.maxTotalKeys = sei.ExecDetail.ScanDetail.TotalKeys
		}
		ssElement.sumProcessedKeys += sei.ExecDetail.ScanDetail.ProcessedKeys
		if sei.ExecDetail.ScanDetail.ProcessedKeys > ssElement.maxProcessedKeys {
			ssElement.maxProcessedKeys = sei.ExecDetail.ScanDetail.ProcessedKeys
		}
		ssElement.sumRocksdbDeleteSkippedCount += sei.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount
		if sei.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount > ssElement.maxRocksdbDeleteSkippedCount {
			ssElement.maxRocksdbDeleteSkippedCount = sei.ExecDetail.ScanDetail.RocksdbDeleteSkippedCount
		}
		ssElement.sumRocksdbKeySkippedCount += sei.ExecDetail.ScanDetail.RocksdbKeySkippedCount
		if sei.ExecDetail.ScanDetail.RocksdbKeySkippedCount > ssElement.maxRocksdbKeySkippedCount {
			ssElement.maxRocksdbKeySkippedCount = sei.ExecDetail.ScanDetail.RocksdbKeySkippedCount
		}
		ssElement.sumRocksdbBlockCacheHitCount += sei.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount
		if sei.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount > ssElement.maxRocksdbBlockCacheHitCount {
			ssElement.maxRocksdbBlockCacheHitCount = sei.ExecDetail.ScanDetail.RocksdbBlockCacheHitCount
		}
		ssElement.sumRocksdbBlockReadCount += sei.ExecDetail.ScanDetail.RocksdbBlockReadCount
		if sei.ExecDetail.ScanDetail.RocksdbBlockReadCount > ssElement.maxRocksdbBlockReadCount {
			ssElement.maxRocksdbBlockReadCount = sei.ExecDetail.ScanDetail.RocksdbBlockReadCount
		}
		ssElement.sumRocksdbBlockReadByte += sei.ExecDetail.ScanDetail.RocksdbBlockReadByte
		if sei.ExecDetail.ScanDetail.RocksdbBlockReadByte > ssElement.maxRocksdbBlockReadByte {
			ssElement.maxRocksdbBlockReadByte = sei.ExecDetail.ScanDetail.RocksdbBlockReadByte
		}
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
		resolveLockTime := atomic.LoadInt64(&commitDetails.ResolveLock.ResolveLockTime)
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
		commitBackoffTime := commitDetails.Mu.CommitBackoffTime
		ssElement.sumCommitBackoffTime += commitBackoffTime
		if commitBackoffTime > ssElement.maxCommitBackoffTime {
			ssElement.maxCommitBackoffTime = commitBackoffTime
		}
		ssElement.sumBackoffTimes += int64(len(commitDetails.Mu.BackoffTypes))
		for _, backoffType := range commitDetails.Mu.BackoffTypes {
			ssElement.backoffTypes[backoffType] += 1
		}
		commitDetails.Mu.Unlock()
	}

	// plan cache
	if sei.PlanInCache {
		ssElement.planInCache = true
		ssElement.planCacheHits += 1
	} else {
		ssElement.planInCache = false
	}

	// SPM
	if sei.PlanInBinding {
		ssElement.planInBinding = true
	} else {
		ssElement.planInBinding = false
	}

	// other
	ssElement.sumAffectedRows += sei.StmtCtx.AffectedRows()
	ssElement.sumMem += sei.MemMax
	if sei.MemMax > ssElement.maxMem {
		ssElement.maxMem = sei.MemMax
	}
	ssElement.sumDisk += sei.DiskMax
	if sei.DiskMax > ssElement.maxDisk {
		ssElement.maxDisk = sei.DiskMax
	}
	if sei.StartTime.Before(ssElement.firstSeen) {
		ssElement.firstSeen = sei.StartTime
	}
	if ssElement.lastSeen.Before(sei.StartTime) {
		ssElement.lastSeen = sei.StartTime
	}
	if sei.ExecRetryCount > 0 {
		ssElement.execRetryCount += sei.ExecRetryCount
		ssElement.execRetryTime += sei.ExecRetryTime
	}
	if sei.ResultRows > 0 {
		ssElement.sumResultRows += sei.ResultRows
		if ssElement.maxResultRows < sei.ResultRows {
			ssElement.maxResultRows = sei.ResultRows
		}
		if ssElement.minResultRows > sei.ResultRows {
			ssElement.minResultRows = sei.ResultRows
		}
	} else {
		ssElement.minResultRows = 0
	}
	ssElement.sumKVTotal += time.Duration(atomic.LoadInt64(&sei.TiKVExecDetails.WaitKVRespDuration))
	ssElement.sumPDTotal += time.Duration(atomic.LoadInt64(&sei.TiKVExecDetails.WaitPDRespDuration))
	ssElement.sumBackoffTotal += time.Duration(atomic.LoadInt64(&sei.TiKVExecDetails.BackoffDuration))
	ssElement.sumWriteSQLRespTotal += sei.StmtExecDetails.WriteSQLRespDuration
}

// Truncate SQL to maxSQLLength.
func formatSQL(sql string) string {
	maxSQLLength := StmtSummaryByDigestMap.maxSQLLength()
	length := len(sql)
	if length > maxSQLLength {
		sql = fmt.Sprintf("%.*s(len:%d)", maxSQLLength, sql, length)
	}
	return sql
}

// Format the backoffType map to a string or nil.
func formatBackoffTypes(backoffMap map[string]int) interface{} {
	type backoffStat struct {
		backoffType string
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
