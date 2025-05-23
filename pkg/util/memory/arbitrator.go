// Copyright 2018 PingCAP, Inc.
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

package memory

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// PoolEntryState represents the state of a pool entry
type PoolEntryState int32

const (
	// PoolEntryStateNormal indicates that the pool entry is in a normal state
	PoolEntryStateNormal PoolEntryState = iota
	// PoolEntryStateStop indicates that the pool entry is stopped
	PoolEntryStateStop
)

// ArbitrateResult represents the results of the arbitration process
type ArbitrateResult int32

const (
	// ArbitrateOk indicates that the arbitration is successful.
	ArbitrateOk ArbitrateResult = iota
	// ArbitrateFail indicates that the arbitration is failed
	ArbitrateFail
)

// SoftLimitMode represents the mode of soft limit for the mem-arbitrator
type SoftLimitMode int32

const (
	// SoftLimitModeDefault indicates that soft-limit is same as the threshold of oom risk
	SoftLimitModeDefault SoftLimitMode = iota
	// SoftLimitModeSpecified indicates that the soft-limit is a specified num of bytes or rate of the limit
	SoftLimitModeSpecified
	// SoftLimitModeAuto indicates that the soft-limit is auto calculated by the mem-arbitrator
	SoftLimitModeAuto
)

const (
	// ArbitratorSoftLimitModeAutoName is the name of the default soft limit mode
	ArbitratorSoftLimitModeAutoName = "AUTO"
	// ArbitratorModeStandardName is the name of the standard mode
	ArbitratorModeStandardName = "STANDARD"
	// ArbitratorModePriorityName is the name of the priority mode
	ArbitratorModePriorityName = "PRIORITY"
	// ArbitratorModeDisableName is the name of the disable mode
	ArbitratorModeDisableName = "DISABLE"
	// DefMaxLimit is the default maximum limit of mem quota
	DefMaxLimit int64 = 5e15

	defTaskTickDur                                   = time.Millisecond * 10
	defMinHeapFreeSpeedBPS                    int64  = 100 * mb
	defHeapReclaimCheckDuration                      = time.Second * 1
	defHeapReclaimCheckMaxDuration                   = time.Second * 5
	defCheckOOMRatio                                 = 0.95
	defCheckSafeRatio                                = 0.9
	defTickDurMilli                                  = kilo * 1
	defTrackMemStatsDurMilli                         = kilo * 1
	defMax                                    int64  = 9e15
	defServerlimitSmallLimitNum                      = 1000
	defServerlimitMinUnitNum                         = 500
	defServerlimitMaxUnitNum                         = 100
	defUpdateMemConsumedTimeAlignSec                 = 30
	defUpdateMemMagnifUtimeAlign                     = 30
	defUpdateMemMagnifiUtimeAlignStandardMode        = 1
	defUpdateBufferTimeAlignSec                      = 60
	defRedundancy                                    = 2
	defPoolReservedQuota                             = 4 * mb
	defAwaitFreePoolAllocAlignSize                   = defPoolReservedQuota + mb
	defAwaitFreePoolShardNum                  int64  = 256
	defAwaitFreePoolShrinkDurMilli                   = kilo * 5
	defPoolStatusShards                              = 128
	defPoolQuotaShards                               = 27 // quota >= BaseQuotaUnit * 2^(max_shards - 2) will be put into the last shard
	prime64                                   uint64 = 1099511628211
	initHashKey                               uint64 = 14695981039346656037
	defKillCancelCheckDuration                       = time.Second * 5
	defKillCancelCheckTimeout                        = time.Second * 20
	defDigestProfileSmallMemTimeoutSec               = 60 * 60 * 24     // 1 day
	defDigestProfileMemTimeoutSec                    = 60 * 60 * 24 * 7 // 1 week
	baseQuotaUnit                                    = 4 * kb           // 4KB
)

// ArbitratorWorkMode represents the work mode of the arbitrator: Standard, Priority, Disable
type ArbitratorWorkMode int32

const (
	// ArbitratorModeStandard indicates the standard mode
	ArbitratorModeStandard ArbitratorWorkMode = iota
	// ArbitratorModePriority indicates the priority mode
	ArbitratorModePriority
	// ArbitratorModeDisable indicates the mem-arbitrator is disabled
	ArbitratorModeDisable
)

// ArbitrationPriority represents the priority of the task: Low, Medium, High
type ArbitrationPriority int32

type entryExecState int32

const (
	execStateIdle entryExecState = iota
	execStateRunning
	execStatePrivileged
)

const (
	// ArbitrationPriorityLow indicates the low priority
	ArbitrationPriorityLow ArbitrationPriority = iota
	// ArbitrationPriorityMedium indicates the medium priority
	ArbitrationPriorityMedium
	// ArbitrationPriorityHigh indicates the high priority
	ArbitrationPriorityHigh

	minArbitrationPriority = ArbitrationPriorityLow
	maxArbitrationPriority = ArbitrationPriorityHigh + 1
	maxArbitrateMode       = maxArbitrationPriority + 1

	// ArbitrationWaitAverse indicates the wait-averse property
	ArbitrationWaitAverse = maxArbitrationPriority
)

var errArbitrateFailError = errors.New("failed to allocate resource from arbitrator")

var arbitrationPriorityNames = [maxArbitrationPriority]string{"LOW", "MEDIUM", "HIGH"}

// String returns the string representation of the ArbitrationPriority
func (p ArbitrationPriority) String() string {
	return arbitrationPriorityNames[p]
}

var arbitratorWorkModeNames = []string{ArbitratorModeStandardName, ArbitratorModePriorityName, ArbitratorModeDisableName}

// String returns the string representation of the ArbitratorWorkMode
func (m ArbitratorWorkMode) String() string {
	return arbitratorWorkModeNames[m]
}

func (m *MemArbitrator) taskNumByPriority(priority ArbitrationPriority) int64 {
	return m.tasks.fifoByPriority[priority].size()
}

func (m *MemArbitrator) taskNumOfWaitAverse() int64 {
	return m.tasks.fifoWaitAverse.size()
}

func (m *MemArbitrator) firstTaskEntry(priority ArbitrationPriority) *rootPoolEntry {
	return m.tasks.fifoByPriority[priority].front()
}

func (m *MemArbitrator) removeTaskImpl(entry *rootPoolEntry) bool {
	if entry.taskMu.fifo.valid() {
		m.tasks.fifoTasks.remove(entry.taskMu.fifo)
		entry.taskMu.fifo.reset()
		m.tasks.fifoByPriority[entry.taskMu.fifoByPriority.priority].remove(entry.taskMu.fifoByPriority.wrapListElement)
		entry.taskMu.fifoByPriority.reset()
		if entry.taskMu.fifoWaitAverse.valid() {
			m.tasks.fifoWaitAverse.remove(entry.taskMu.fifoWaitAverse)
			entry.taskMu.fifoWaitAverse.reset()
		}
		return true
	}
	return false
}

// there is no need to wind up if task has been removed by cancel.
func (m *MemArbitrator) removeTask(entry *rootPoolEntry) (res bool) {
	m.tasks.Lock()
	//
	res = m.removeTaskImpl(entry)
	//
	m.tasks.Unlock()
	return res
}

func (m *MemArbitrator) addTask(entry *rootPoolEntry) {
	m.tasks.Lock()
	//
	entry.taskMu.fifo = m.tasks.fifoTasks.pushBack(entry)
	priority := entry.ctx.memPriority
	entry.taskMu.fifoByPriority.priority = priority
	entry.taskMu.fifoByPriority.wrapListElement = m.tasks.fifoByPriority[priority].pushBack(entry)
	if entry.ctx.waitAverse {
		entry.taskMu.fifoWaitAverse = m.tasks.fifoWaitAverse.pushBack(entry)
	}
	//
	m.tasks.Unlock()
}

func (m *MemArbitrator) frontTaskEntry() (entry *rootPoolEntry) {
	m.tasks.Lock()
	//
	entry = m.tasks.fifoTasks.front()
	//
	m.tasks.Unlock()
	return
}

func (m *MemArbitrator) extractFirstTaskEntry() (entry *rootPoolEntry) {
	m.tasks.Lock()
	//
	if m.privilegedEntry != nil {
		if m.privilegedEntry.taskMu.fifo.valid() {
			m.tasks.fifoTasks.moveToFront(m.privilegedEntry.taskMu.fifo)
			entry = m.privilegedEntry
		}
	}

	if entry == nil {
		if m.execMu.mode == ArbitratorModePriority {
			for priority := maxArbitrationPriority - 1; priority >= minArbitrationPriority; priority-- {
				if entry = m.firstTaskEntry(priority); entry != nil {
					break
				}
			}
		} else {
			entry = m.tasks.fifoTasks.front()
		}
	}
	//
	m.tasks.Unlock()
	return
}

type rootPoolEntry struct {
	pool   *ResourcePool
	taskMu struct { // protected by the tasks mutex of arbitrator
		fifo           wrapListElement
		fifoWaitAverse wrapListElement
		fifoByPriority struct {
			wrapListElement
			priority ArbitrationPriority
		}
	}

	// context of execution
	// mutable when entry is idle and the mutex of root pool is locked
	ctx struct {
		atomic.Pointer[ArbitrationContext]
		cancelCh        <-chan struct{}
		memPriority     ArbitrationPriority
		waitAverse      bool
		preferPrivilege bool
	}
	request struct {
		resultCh chan ArbitrateResult // arbitrator will send result in `windup
		quota    int64                // mutable for ResourcePool
	}
	arbitratorMu struct { // mutable for arbitrator
		shard       *entryMapShard
		quotaShard  *entryQuotaShard
		underKill   entryKillCancelCtx
		underCancel entryKillCancelCtx
		quota       int64 // -1: uninitiated
		destroyed   bool
	}
	stateMu struct {
		sync.Mutex
		state PoolEntryState

		// execStateIdle -> execStateRunning -> execStatePrivileged -> execStateIdle
		// execStateIdle -> execStateRunning -> execStateIdle
		exec entryExecState

		pendingReset struct { // mutable once entry is idle
			sync.Mutex
			quota int64
		}
	}
}

type mapUIDEntry map[uint64]*rootPoolEntry

type entryMapShard struct {
	entries mapUIDEntry
	sync.RWMutex
}

type entryQuotaShard struct {
	entries mapUIDEntry
}

func (e *rootPoolEntry) state() (s PoolEntryState) {
	s = PoolEntryState(atomic.LoadInt32((*int32)(&e.stateMu.state)))
	return s
}

func (e *rootPoolEntry) execState() entryExecState {
	return entryExecState(atomic.LoadInt32((*int32)(&e.stateMu.exec)))
}

func (e *rootPoolEntry) setExecState(s entryExecState) {
	atomic.StoreInt32((*int32)(&e.stateMu.exec), int32(s))
}

func (e *rootPoolEntry) intoExecPrivileged() bool {
	return atomic.CompareAndSwapInt32((*int32)(&e.stateMu.exec), int32(execStateRunning), int32(execStatePrivileged))
}

func (e *rootPoolEntry) notRunning() bool {
	return e.state() == PoolEntryStateStop || e.execState() == execStateIdle || e.stateMu.pendingReset.quota > 0
}

type entryMap struct {
	quotaShards  [maxArbitrationPriority][]*entryQuotaShard
	contextCache struct { // cache for traversing all entries concurrently
		sync.Map // map[uint64]*rootPoolEntry
		num      atomic.Int64
	}
	shards                    []*entryMapShard
	shardsMask                uint64
	maxQuotaShardIndex        int // for quota >= `BaseQuotaUnit * 2^(maxQuotaShard - 1)`
	minQuotaShardIndexToCheck int // ignore the pool with smaller quota
}

// controlled by arbitrator
func (m *entryMap) delete(entry *rootPoolEntry) {
	uid := entry.pool.uid

	if entry.arbitratorMu.quotaShard != nil {
		delete(entry.arbitratorMu.quotaShard.entries, uid)
		entry.arbitratorMu.quota = 0
		entry.arbitratorMu.quotaShard = nil
	}

	entry.arbitratorMu.shard.delete(uid)
	entry.arbitratorMu.shard = nil

	if _, loaded := m.contextCache.LoadAndDelete(uid); loaded {
		m.contextCache.num.Add(-1)
	}
}

// controlled by arbitrator
func (m *entryMap) addQuota(entry *rootPoolEntry, delta int64) {
	if delta == 0 {
		return
	}

	uid := entry.pool.UID()

	entry.arbitratorMu.quota += delta

	if entry.arbitratorMu.quota == 0 { // remove
		delete(entry.arbitratorMu.quotaShard.entries, uid)
		entry.arbitratorMu.quotaShard = nil
		return
	}

	newPos := getQuotaShard(entry.arbitratorMu.quota, m.maxQuotaShardIndex)
	newShard := m.quotaShards[entry.ctx.memPriority][newPos]
	if newShard != entry.arbitratorMu.quotaShard {
		if entry.arbitratorMu.quotaShard != nil {
			delete(entry.arbitratorMu.quotaShard.entries, uid)
		}
		entry.arbitratorMu.quotaShard = newShard
		entry.arbitratorMu.quotaShard.entries[uid] = entry
	}
}

func (m *entryMap) getStatusShard(key uint64) *entryMapShard {
	return m.shards[shardIndexByUID(key, m.shardsMask)]
}

func (m *entryMap) getQuotaShard(priority ArbitrationPriority, quota int64) *entryQuotaShard {
	return m.quotaShards[priority][getQuotaShard(quota, m.maxQuotaShardIndex)]
}

func (s *entryMapShard) get(key uint64) (e *rootPoolEntry, ok bool) {
	s.RLock()
	e, ok = s.entries[key]
	s.RUnlock()
	return
}

func (s *entryMapShard) delete(key uint64) {
	s.Lock()
	//
	delete(s.entries, key)
	//
	s.Unlock()
}

func (s *entryMapShard) emplace(key uint64, tar *rootPoolEntry) (e *rootPoolEntry, ok bool) {
	s.Lock()
	//
	e, ok = s.entries[key]
	if !ok {
		s.entries[key] = tar
		e = tar
		ok = true
	} else {
		ok = false
	}
	//
	s.Unlock()
	return
}

func (m *entryMap) emplace(pool *ResourcePool) (*rootPoolEntry, bool) {
	key := pool.UID()
	s := m.getStatusShard(key)
	if v, ok := s.get(key); ok {
		return v, false
	}
	tar := &rootPoolEntry{pool: pool}
	tar.arbitratorMu.shard = s
	tar.request.resultCh = make(chan ArbitrateResult, 1)

	return s.emplace(key, tar)
}

func (m *entryMap) init(shardNum uint64, maxQuotaShard int, minQuotaForReclaim int64) {
	m.shards = make([]*entryMapShard, shardNum)
	m.shardsMask = shardNum - 1
	m.maxQuotaShardIndex = maxQuotaShard
	m.minQuotaShardIndexToCheck = getQuotaShard(minQuotaForReclaim, m.maxQuotaShardIndex)
	for p := minArbitrationPriority; p < maxArbitrationPriority; p++ {
		m.quotaShards[p] = make([]*entryQuotaShard, m.maxQuotaShardIndex)
		for i := range m.maxQuotaShardIndex {
			m.quotaShards[p][i] = &entryQuotaShard{
				entries: make(mapUIDEntry),
			}
		}
	}

	for i := range shardNum {
		m.shards[i] = &entryMapShard{
			entries: make(mapUIDEntry),
		}
	}
}

// if entry is in task queue, it must have acquire the unique request lock and wait for callback
// this func only can be invoked after `removeTask`
func (e *rootPoolEntry) windUp(delta int64, r ArbitrateResult) {
	e.pool.forceAddCap(delta)
	e.request.resultCh <- r
}

// non thread safe: the mutex of root pool must have been locked
func (m *MemArbitrator) blockingAllocate(entry *rootPoolEntry, requestedBytes int64) ArbitrateResult {
	if entry.execState() == execStateIdle {
		return ArbitrateFail
	}

	m.prepareAlloc(entry, requestedBytes)
	return m.waitAlloc(entry)
}

// non thread safe: the mutex of root pool must have been locked
func (m *MemArbitrator) prepareAlloc(entry *rootPoolEntry, requestedBytes int64) {
	{
		entry.request.quota = requestedBytes
		m.addTask(entry)
		m.notifer.WeakWake()
	}

	m.tasks.pendingAlloc.Add(entry.request.quota)
}

// non thread safe: the mutex of root pool must have been locked
func (m *MemArbitrator) waitAlloc(entry *rootPoolEntry) ArbitrateResult {
	res := ArbitrateOk
	select {
	case res = <-entry.request.resultCh:
		if res == ArbitrateFail {
			atomic.AddInt64(&m.execMetrics.task.fail, 1)
		} else {
			atomic.AddInt64(&m.execMetrics.task.success, 1)
		}
	case <-entry.ctx.cancelCh:
		// 1. cancel by session interruption
		// 2. cancel by arbitrator
		res = ArbitrateFail
		atomic.AddInt64(&m.execMetrics.task.fail, 1)

		if !m.removeTask(entry) {
			<-entry.request.resultCh
		}
	}

	m.tasks.pendingAlloc.Add(-entry.request.quota)
	entry.request.quota = 0

	return res
}

type blockedState struct {
	allocated int64
	utimeSec  int64
}

// PoolAllocProfile represents the profile of root pool allocation in the mem-arbitrator
type PoolAllocProfile struct {
	SmallPoolLimit   int64 // limit / 1000
	PoolAllocUnit    int64 // limit / 500
	MaxPoolAllocUnit int64 // limit / 100
}

type holder64Bytes [64]byte

// MemArbitrator represents the main structure aka `mem-arbitrator`
type MemArbitrator struct {
	execMu struct {
		startTime    time.Time
		blockedState blockedState
		sync.Mutex
		mode ArbitratorWorkMode // work mode of each round
	}
	actions   MemArbitratorActions
	controlMu struct {
		finishCh chan struct{}
		sync.Mutex
		running bool
	}
	debug           struct{ now func() time.Time }
	privilegedEntry *rootPoolEntry
	underKill       mapEntryWithMem
	underCancel     mapEntryWithMem
	notifer         Notifer
	cleanupMu       struct {
		fifoTasks wrapList[*rootPoolEntry]
		sync.Mutex
	}
	tasks struct {
		fifoByPriority [maxArbitrationPriority]wrapList[*rootPoolEntry]
		fifoTasks      wrapList[*rootPoolEntry]
		fifoWaitAverse wrapList[*rootPoolEntry]
		pendingAlloc   atomic.Int64
		sync.Mutex
	}
	digestProfileCache struct {
		shards     []digestProfileShard
		shardsMask uint64
		num        atomic.Int64
	}
	entryMap  entryMap
	awaitFree struct {
		pool                 *ResourcePool
		budget               FixSizeBatchBudgets
		lastQuotaUsed        int64
		lastHeapInuse        int64
		lastShrinkUtimeMilli atomic.Int64
	}
	heapController heapController
	poolAllocStats struct {
		sync.RWMutex
		PoolAllocProfile
		mediumQuota atomic.Int64
		timedMap    [2 + defRedundancy]struct {
			sync.RWMutex
			statisticsTimedMapElement
		}
		lastStoreUtimeMilli atomic.Int64
	}

	buffer buffer // only works under `ArbitratorModePriority`

	mu struct {
		sync.Mutex
		_         holder64Bytes
		allocated int64 // allocated mem quota
		_         holder64Bytes
		limit     int64 // hard limit of mem quota
		threshold struct {
			risk    int64
			oomRisk int64
		}
		softLimit struct {
			mode      SoftLimitMode
			size      int64
			specified struct {
				size int64
				rate int64
			}
		}
	}
	execMetrics execMetrics
	avoidance   struct {
		lastUpdateUtimeMilli atomic.Int64
		size                 atomic.Int64
		heapTracked          atomic.Int64
		memMagnif            struct {
			sync.Mutex
			ratio atomic.Int64
		}
	}
	tickTask struct {
		sync.Mutex
		lastTickUtimeMilli atomic.Int64
	}
	UnixTimeSec      int64
	rootPoolNum      atomic.Int64
	mode             ArbitratorWorkMode
	ableForRuntimeGC int32
}

type buffer struct {
	size       atomic.Int64
	quotaLimit atomic.Int64
	timedMap   [2 + defRedundancy]struct {
		sync.RWMutex
		wrapTimeSizeQuota
	}
}

func (m *MemArbitrator) setBufferSize(v int64) {
	m.buffer.size.Store(v)
}

func (m *MemArbitrator) setQuotaLimit(v int64) {
	m.buffer.quotaLimit.Store(v)
}

type digestProfileShard struct {
	sync.Map //map[uint64]*digestProfile
	num      atomic.Int64
}

// MemArbitratorActions represents the actions of the mem-arbitrator
type MemArbitratorActions struct {
	Info, Warn, Error func(format string, args ...zap.Field) // log actions

	UpdateRuntimeMemStats func() // update runtime memory statistics
	GC                    func() // garbage collection
}

type awaitFreePoolExecMetrics struct {
	success int64
	fail    int64
	shrink  int64
}

type pairSuccessFail struct{ success, fail int64 }

type numByPriority [maxArbitrationPriority]int64
type execMetricsAction struct {
	gc                    int64
	updateRuntimeMemStats int64
	recordMemState        pairSuccessFail
}

type execMetricsRisk struct {
	memRisk int64
	oomRisk int64
	oomKill numByPriority
}

type execMetricsCancel struct {
	standardMode int64
	priorityMode numByPriority
	waitAverse   int64
}

type execMetrics struct {
	task struct {
		pairSuccessFail                 // all work modes
		successByPriority numByPriority // work mode priority
	}

	cancel    execMetricsCancel
	awaitFree awaitFreePoolExecMetrics
	action    execMetricsAction
	risk      execMetricsRisk
}

// ExecMetrics returns the execution metrics of the mem-arbitrator
func (m *MemArbitrator) ExecMetrics() map[string]int64 {
	return map[string]int64{
		"task-success":                    m.execMetrics.task.success,
		"task-fail":                       m.execMetrics.task.fail,
		"task-success-prio-low":           m.execMetrics.task.successByPriority[ArbitrationPriorityLow],
		"task-success-prio-medium":        m.execMetrics.task.successByPriority[ArbitrationPriorityMedium],
		"task-success-prio-high":          m.execMetrics.task.successByPriority[ArbitrationPriorityHigh],
		"action-gc":                       m.execMetrics.action.gc,
		"action-update-runtime-mem-stats": m.execMetrics.action.updateRuntimeMemStats,
		"action-recordmemstate-success":   m.execMetrics.action.recordMemState.success,
		"action-recordmemstate-fail":      m.execMetrics.action.recordMemState.fail,
		"awaitfree-success":               m.execMetrics.awaitFree.success,
		"awaitfree-fail":                  m.execMetrics.awaitFree.fail,
		"awaitfree-shrink":                m.execMetrics.awaitFree.shrink,
		"memRisk":                         m.execMetrics.risk.memRisk,
		"oomRisk":                         m.execMetrics.risk.oomRisk,
		"oomKill-prio-low":                m.execMetrics.risk.oomKill[ArbitrationPriorityLow],
		"oomKill-prio-medium":             m.execMetrics.risk.oomKill[ArbitrationPriorityMedium],
		"oomKill-prio-high":               m.execMetrics.risk.oomKill[ArbitrationPriorityHigh],
	}
}

// SetWorkMode sets the work mode of the mem-arbitrator
func (m *MemArbitrator) SetWorkMode(newMode ArbitratorWorkMode) (oriMode ArbitratorWorkMode) {
	oriMode = ArbitratorWorkMode(atomic.SwapInt32((*int32)(&m.mode), int32(newMode)))
	m.wake()
	return
}

// WorkMode returns the current work mode of the mem-arbitrator
func (m *MemArbitrator) WorkMode() ArbitratorWorkMode {
	return m.workMode()
}

// PoolAllocProfile returns the profile of root pool allocation in the mem-arbitrator
func (m *MemArbitrator) PoolAllocProfile() (res PoolAllocProfile) {
	limit := m.limit()
	return PoolAllocProfile{
		SmallPoolLimit:   max(1, limit/defServerlimitSmallLimitNum),
		PoolAllocUnit:    max(1, limit/defServerlimitMinUnitNum),
		MaxPoolAllocUnit: max(1, limit/defServerlimitMaxUnitNum),
	}
}

func (m *MemArbitrator) workMode() ArbitratorWorkMode {
	return ArbitratorWorkMode(atomic.LoadInt32((*int32)(&m.mode)))
}

// GetDigestProfileCache returns the digest profile cache for a given digest-id and utime
func (m *MemArbitrator) GetDigestProfileCache(digestID uint64, utimeSec int64) (int64, bool) {
	d := &m.digestProfileCache.shards[digestID&m.digestProfileCache.shardsMask]
	e, ok := d.Load(digestID)
	if !ok {
		return 0, false
	}

	pf := e.(*digestProfile)

	if utimeSec > pf.lastFetchUtimeSec.Load() {
		pf.lastFetchUtimeSec.Store(utimeSec)
	}

	return pf.maxVal.Load(), true
}

func (m *MemArbitrator) shrinkDigestProfile(utimeSec int64, limit, shrinkToLimit int64) (shrinkedNum int64) {
	if m.digestProfileCache.num.Load() <= limit {
		return
	}

	var valMap [defPoolQuotaShards]int

	for i := range m.digestProfileCache.shards {
		d := &m.digestProfileCache.shards[i]
		if d.num.Load() == 0 {
			continue
		}
		dn := int64(0)
		d.Range(func(k, v any) bool {
			pf := v.(*digestProfile)
			maxVal := pf.maxVal.Load()
			{ // try to delete timeout cache
				needDelete := false

				if maxVal > m.poolAllocStats.SmallPoolLimit {
					if utimeSec-pf.lastFetchUtimeSec.Load() > defDigestProfileMemTimeoutSec {
						needDelete = true
					}
				} else { // small max-val
					if utimeSec-pf.lastFetchUtimeSec.Load() > defDigestProfileSmallMemTimeoutSec {
						needDelete = true
					}
				}

				if needDelete {
					if _, loaded := d.LoadAndDelete(k); loaded {
						d.num.Add(-1)
						dn++
						return true
					}
				}
			}
			index := getQuotaShard(maxVal, defPoolQuotaShards)
			valMap[index]++
			return true
		})
		m.digestProfileCache.num.Add(-dn)
		shrinkedNum += dn
	}

	toShinkNum := m.digestProfileCache.num.Load() - shrinkToLimit
	if toShinkNum <= 0 {
		return
	}

	shrinkMaxSize := DefMaxLimit
	{
		n := int64(0)
		for i := range defPoolQuotaShards {
			if n += int64(valMap[i]); n >= toShinkNum {
				shrinkMaxSize = baseQuotaUnit * (1 << i)
				break
			}
		}
	}

	for i := range m.digestProfileCache.shards {
		d := &m.digestProfileCache.shards[i]
		if d.num.Load() == 0 {
			continue
		}
		dn := int64(0)
		d.Range(func(k, v any) bool {
			if pf := v.(*digestProfile); pf.maxVal.Load() < shrinkMaxSize {
				if _, loaded := d.LoadAndDelete(k); loaded {
					d.num.Add(-1)
					toShinkNum--
					dn++
				}
			}

			return toShinkNum > 0
		})
		m.digestProfileCache.num.Add(-dn)
		shrinkedNum += dn

		if toShinkNum <= 0 {
			break
		}
	}

	return
}

// UpdateDigestProfileCache updates the digest profile cache for a given digest-id
func (m *MemArbitrator) UpdateDigestProfileCache(digestID uint64, memConsumed int64, utimeSec int64) {
	d := &m.digestProfileCache.shards[digestID&m.digestProfileCache.shardsMask]
	var pf *digestProfile
	if e, ok := d.Load(digestID); ok {
		pf = e.(*digestProfile)
	} else {
		pf = &digestProfile{}
		if actual, loaded := d.LoadOrStore(digestID, pf); loaded {
			pf = actual.(*digestProfile)
		} else {
			d.num.Add(1)
			m.digestProfileCache.num.Add(1)
		}
	}

	const maxNum = int64(len(pf.timedMap))
	const maxDur = maxNum - defRedundancy

	tsAlign := utimeSec / defUpdateBufferTimeAlignSec
	tar := &pf.timedMap[tsAlign%maxNum]

	if oriTs := tar.tsAlign.Load(); oriTs < tsAlign && oriTs != 0 {
		tar.Lock()
		//
		if oriTs = tar.tsAlign.Load(); oriTs < tsAlign && oriTs != 0 {
			tar.wrapTimeMaxval = wrapTimeMaxval{}
		}
		//
		tar.Unlock()
	}

	tar.RLock()

	updateSize := false
	cleanNext := false

	if tar.tsAlign.Load() == 0 {
		if tar.tsAlign.CompareAndSwap(0, tsAlign) {
			cleanNext = true
		}
	}

	for oldVal := tar.maxVal.Load(); oldVal < memConsumed; oldVal = tar.maxVal.Load() {
		if tar.maxVal.CompareAndSwap(oldVal, memConsumed) {
			updateSize = true
			break
		}
	}

	if updateSize {
		maxv := tar.maxVal.Load()
		// tsAlign-1, tsAlign
		for i := range maxDur {
			d := &pf.timedMap[(maxNum+tsAlign-i)%maxNum]

			if ts := d.tsAlign.Load(); ts > tsAlign-maxDur && ts <= tsAlign {
				maxv = max(maxv, d.maxVal.Load())
			}
		}
		pf.maxVal.CompareAndSwap(pf.maxVal.Load(), maxv) // force update
	}

	tar.RUnlock()

	if utimeSec > pf.lastFetchUtimeSec.Load() {
		pf.lastFetchUtimeSec.Store(utimeSec)
	}

	if cleanNext {
		d := &pf.timedMap[(tsAlign+1)%maxNum]
		d.Lock()
		//
		if ts := d.tsAlign.Load(); ts < (tsAlign+1) && ts != 0 {
			d.wrapTimeMaxval = wrapTimeMaxval{}
		}
		//
		d.Unlock()
	}
}

type digestProfile struct {
	maxVal   atomic.Int64
	timedMap [2 + defRedundancy]struct {
		sync.RWMutex
		wrapTimeMaxval
	}
	lastFetchUtimeSec atomic.Int64
}

type wrapTimeMaxval struct {
	tsAlign atomic.Int64
	maxVal  atomic.Int64
}

type wrapTimeSizeQuota struct {
	ts    atomic.Int64
	size  atomic.Int64
	quota atomic.Int64
}

type statisticsTimedMapElement struct {
	tsAlign atomic.Int64
	slot    [defServerlimitMinUnitNum]uint32
	num     atomic.Uint32
}

type entryKillCancelCtx struct {
	startTime time.Time
	reclaim   int64
	start     bool
	fail      bool
}

type mapEntryWithMem struct {
	entries mapUIDEntry
	num     int64
}

func (x *mapEntryWithMem) delete(entry *rootPoolEntry) {
	delete(x.entries, entry.pool.uid)
	x.num--
}
func (x *mapEntryWithMem) init() {
	x.entries = make(mapUIDEntry)
}

func (x *mapEntryWithMem) add(entry *rootPoolEntry) {
	x.entries[entry.pool.uid] = entry
	x.num++
}

func (m *MemArbitrator) addUnderKill(entry *rootPoolEntry, memoryUsed int64, startTime time.Time) {
	if !entry.arbitratorMu.underKill.start {
		m.underKill.add(entry)
		entry.arbitratorMu.underKill = entryKillCancelCtx{
			start:     true,
			startTime: startTime,
			reclaim:   memoryUsed,
		}
	}
}

func (m *MemArbitrator) addUnderCancel(entry *rootPoolEntry, memoryUsed int64, startTime time.Time) {
	if !entry.arbitratorMu.underCancel.start {
		m.underCancel.add(entry)
		entry.arbitratorMu.underCancel = entryKillCancelCtx{
			start:     true,
			startTime: startTime,
			reclaim:   memoryUsed,
		}
	}
}

func (m *MemArbitrator) deleteUnderKill(entry *rootPoolEntry) {
	if entry.arbitratorMu.underKill.start {
		m.underKill.delete(entry)
		entry.arbitratorMu.underKill.start = false

		m.warnKillCancel(entry, &entry.arbitratorMu.underKill, "Finish to `KILL` root pool")
	}
}

func (m *MemArbitrator) deleteUnderCancel(entry *rootPoolEntry) {
	if entry.arbitratorMu.underCancel.start {
		m.underCancel.delete(entry)
		entry.arbitratorMu.underCancel.start = false
	}
}

type memProfile struct {
	startUtimeMilli int64
	tsAlign         int64
	heap            int64 // max heap-alloc size after GC
	quota           int64 // max quota allocated when failed to arbitrate
	ratio           int64 // heap / quota
}

type heapController struct {
	memStateRecorder struct {
		RecordMemState
		lastMemState         atomic.Pointer[RuntimeMemStateV1]
		lastRecordUtimeMilli atomic.Int64
		sync.Mutex
		pendingStore atomic.Bool
	}
	lastGC   lastGCStats
	oomCheck struct {
		startTime    time.Time
		lastMemStats struct {
			startTime     time.Time
			heapTotalFree int64
		}
		minHeapFreeSpeedBPS atomic.Int64
		start               bool
		eachRound           struct{ gcExecuted bool }
	}
	timedMemProfile [2]memProfile
	heapAlloc       atomic.Int64
	heapTotalFree   atomic.Int64

	// `inuse` span + `stack` span == approx all heap
	heapInuse  atomic.Int64
	stackInuse atomic.Int64
}

func (m *MemArbitrator) lastMemState() (res *RuntimeMemStateV1) {
	res = m.heapController.memStateRecorder.lastMemState.Load()
	return
}

type lastGCStats struct {
	startTime   time.Time    // start time
	heapAlloc   atomic.Int64 // heap alloc size after GC
	endUtimeSec atomic.Int64 // end time
}

// RecordMemState is an interface for recording runtime memory state
type RecordMemState interface {
	Load() (*RuntimeMemStateV1, error)
	Store(*RuntimeMemStateV1) error
}

func (m *MemArbitrator) recordMemConsumed(memConsumed, utimeSec int64) {
	m.poolAllocStats.RLock()
	defer m.poolAllocStats.RUnlock()

	const maxNum = int64(len(m.poolAllocStats.timedMap))

	tsAlign := utimeSec / defUpdateMemConsumedTimeAlignSec
	tar := &m.poolAllocStats.timedMap[tsAlign%maxNum]

	if oriTs := tar.tsAlign.Load(); oriTs < tsAlign && oriTs != 0 {
		tar.Lock()
		//
		if oriTs = tar.tsAlign.Load(); oriTs < tsAlign && oriTs != 0 {
			tar.statisticsTimedMapElement = statisticsTimedMapElement{}
		}
		//
		tar.Unlock()
	}

	cleanNext := false
	tar.RLock()
	//

	if tar.tsAlign.Load() == 0 {
		if tar.tsAlign.CompareAndSwap(0, tsAlign) {
			cleanNext = true
		}
	}

	{
		pos := min(memConsumed/m.poolAllocStats.PoolAllocUnit, defServerlimitMinUnitNum-1)
		atomic.AddUint32(&tar.slot[pos], 1)
		tar.num.Add(1)
	}

	//
	tar.RUnlock()

	if cleanNext {
		d := &m.poolAllocStats.timedMap[(tsAlign+1)%maxNum]
		d.Lock()
		//
		if v := d.tsAlign.Load(); v < (tsAlign+1) && v != 0 {
			d.statisticsTimedMapElement = statisticsTimedMapElement{}
		}
		//
		d.Unlock()
	}
}

func (m *MemArbitrator) tryToUpdateBuffer(memConsumed, memQuotaLimit, utimeSec int64) {
	const maxNum = int64(len(m.buffer.timedMap))
	const maxDur = maxNum - defRedundancy

	tsAlign := utimeSec / defUpdateBufferTimeAlignSec
	tar := &m.buffer.timedMap[tsAlign%maxNum]

	if oriTs := tar.ts.Load(); oriTs < tsAlign && oriTs != 0 {
		tar.Lock()
		//
		if oriTs = tar.ts.Load(); oriTs < tsAlign && oriTs != 0 {
			tar.wrapTimeSizeQuota = wrapTimeSizeQuota{}
		}
		//
		tar.Unlock()
	}

	tar.RLock()

	updateSize := false
	updateQuota := false
	cleanNext := false

	if ts := tar.ts.Load(); ts == 0 {
		if tar.ts.CompareAndSwap(0, tsAlign) {
			cleanNext = true
		}
	}

	for oldVal := tar.size.Load(); oldVal < memConsumed; oldVal = tar.size.Load() {
		if tar.size.CompareAndSwap(oldVal, memConsumed) {
			updateSize = true
			break
		}
	}

	for oldVal := tar.quota.Load(); oldVal < memQuotaLimit; oldVal = tar.quota.Load() {
		if tar.quota.CompareAndSwap(oldVal, memQuotaLimit) {
			updateQuota = true
			break
		}
	}

	if updateSize || updateQuota {
		// tsAlign-1, tsAlign
		for i := range maxDur {
			d := &m.buffer.timedMap[(maxNum+tsAlign-i)%maxNum]

			if ts := d.ts.Load(); ts > tsAlign-maxDur && ts <= tsAlign {
				memConsumed = max(memConsumed, d.size.Load())
				memQuotaLimit = max(memQuotaLimit, d.quota.Load())
			}
		}
		if updateSize && m.buffer.size.Load() != memConsumed {
			m.setBufferSize(memConsumed)
		}
		if updateQuota && m.buffer.quotaLimit.Load() != memQuotaLimit {
			m.setQuotaLimit(memQuotaLimit)
		}
	}

	tar.RUnlock()

	if cleanNext {
		d := &m.buffer.timedMap[(tsAlign+1)%maxNum]
		d.Lock()
		//
		if v := d.ts.Load(); v < tsAlign+1 && v != 0 {
			d.wrapTimeSizeQuota = wrapTimeSizeQuota{}
		}
		//
		d.Unlock()
	}
}

func (m *MemArbitrator) gc() {
	if m.actions.GC != nil {
		m.actions.GC()
	}
	m.execMetrics.action.gc++
}

func (m *MemArbitrator) reclaimHeap() {
	m.heapController.lastGC.startTime = now()
	m.gc()
	m.refreshRuntimeMemStats() // refresh runtime mem stats after GC and record
	endTime := now()
	endUtimeSec := endTime.Unix()
	m.heapController.lastGC.heapAlloc.Store(m.heapController.heapAlloc.Load())
	m.UnixTimeSec = endUtimeSec
	m.heapController.lastGC.endUtimeSec.Store(endUtimeSec)
}

// SetMinHeapFreeSpeedBPS sets the minimum heap alloc/free speed (bytes per second)
func (m *MemArbitrator) SetMinHeapFreeSpeedBPS(sz int64) {
	m.heapController.oomCheck.minHeapFreeSpeedBPS.Store(sz)
}

// MinHeapFreeSpeedBPS returns the minimum heap alloc/free speed (bytes per second)
func (m *MemArbitrator) MinHeapFreeSpeedBPS() int64 {
	return m.heapController.oomCheck.minHeapFreeSpeedBPS.Load()
}

// ResetRootPoolByID resets the root pool by ID and analyze the memory consumption info
func (m *MemArbitrator) ResetRootPoolByID(uid uint64, maxMemConsumed int64, tune bool) {
	entry := m.getRootPoolEntry(uid)
	if entry == nil {
		return
	}

	if tune {
		memQuotaLimit := int64(0)
		if ctx := entry.ctx.Load(); ctx != nil {
			memQuotaLimit = ctx.memQuotaLimit
		}

		m.tryToUpdateBuffer(
			maxMemConsumed,
			memQuotaLimit,
			m.UnixTimeSec)

		if maxMemConsumed > m.poolAllocStats.SmallPoolLimit {
			m.recordMemConsumed(
				maxMemConsumed,
				m.UnixTimeSec)
		}
	}

	m.resetRootPoolEntry(entry)
	m.wake()
}

// SetAbleToGC sets the flag to allow garbage collection
func (m *MemArbitrator) SetAbleToGC() {
	m.ableForRuntimeGC = 1
}

func (m *MemArbitrator) resetRootPoolEntry(entry *rootPoolEntry) bool {
	{
		entry.stateMu.Lock()
		//
		if entry.execState() == execStateIdle {
			entry.stateMu.Unlock()
			return false
		}
		entry.setExecState(execStateIdle)
		//
		entry.stateMu.Unlock()
	}

	// aquiure the lock of root pool:
	// - wait for the alloc task to finish
	// - publish the state of entry
	if releasedSize := entry.pool.Stop(); releasedSize > 0 {
		entry.stateMu.pendingReset.Lock()
		//
		entry.stateMu.pendingReset.quota += releasedSize
		//
		entry.stateMu.pendingReset.Unlock()
	}

	{
		m.cleanupMu.Lock()
		//
		m.cleanupMu.fifoTasks.pushBack(entry)
		//
		m.cleanupMu.Unlock()
	}

	return true
}

func (m *MemArbitrator) warnKillCancel(entry *rootPoolEntry, ctx *entryKillCancelCtx, reason string) {
	m.actions.Warn(
		reason,
		zap.Uint64("uid", entry.pool.uid),
		zap.String("name", entry.pool.name),
		zap.String("mem-priority", entry.ctx.memPriority.String()),
		zap.Int64("reclaimed", ctx.reclaim),
		zap.Time("start-time", ctx.startTime),
	)
}

// RemoveRootPoolByID removes & terminates the root pool by ID
func (m *MemArbitrator) RemoveRootPoolByID(uid uint64) bool {
	entry := m.getRootPoolEntry(uid)
	if entry == nil {
		return false
	}

	if m.removeRootPoolEntry(entry) {
		m.wake()
		return true
	}
	return false
}

func (m *MemArbitrator) removeRootPoolEntry(entry *rootPoolEntry) bool {
	{
		entry.stateMu.Lock()
		//
		if entry.stateMu.state == PoolEntryStateStop {
			entry.stateMu.Unlock()
			return false
		}
		entry.stateMu.state = PoolEntryStateStop
		entry.setExecState(execStateIdle)
		//
		entry.stateMu.Unlock()
	}

	// make the alloc task failed in arbitrator;
	{
		m.cleanupMu.Lock()
		//
		m.cleanupMu.fifoTasks.pushBack(entry)
		//
		m.cleanupMu.Unlock()
	}
	// aquiure the lock of root pool and clean up
	entry.pool.Stop()
	// any new lock of root pool must have sensed the exec state is idle

	return true
}

func (m *MemArbitrator) getRootPoolEntry(uid uint64) *rootPoolEntry {
	if e, ok := m.entryMap.getStatusShard(uid).get(uid); ok {
		return e
	}
	return nil
}

// RootPoolEntry represents the wrapped entry of root pool in the mem-arbitrator
type RootPoolEntry struct {
	Pool  *ResourcePool
	entry *rootPoolEntry
}

// FindRootPool finds the root pool by ID
func (m *MemArbitrator) FindRootPool(uid uint64) RootPoolEntry {
	if e := m.getRootPoolEntry(uid); e != nil {
		return RootPoolEntry{e.pool, e}
	}
	return RootPoolEntry{nil, nil}
}

// EmplaceRootPool emplaces a new root pool with the given uid (uid < 0 means the internal pool)
func (m *MemArbitrator) EmplaceRootPool(uid uint64) (res RootPoolEntry, err error) {
	if res.entry = m.getRootPoolEntry(uid); res.entry != nil {
		res.Pool = res.entry.pool
		return
	}

	res.Pool = &ResourcePool{
		name:           fmt.Sprintf("root-pool-%d", uid),
		uid:            uid,
		limit:          DefMaxLimit,
		allocAlignSize: 1,
	}
	res.entry, err = m.addRootPool(res.Pool)
	return
}

func (m *MemArbitrator) addRootPool(pool *ResourcePool) (*rootPoolEntry, error) {
	if b := pool.capacity(); b != 0 {
		return nil, fmt.Errorf("%s: has %d bytes budget left", pool.name, b)
	}
	if pool.mu.budget.pool != nil {
		return nil, fmt.Errorf("%s: already started with pool %s", pool.name, pool.mu.budget.pool.Name())
	}
	if pool.reserved != 0 {
		return nil, fmt.Errorf("%s: has %d reserved budget left", pool.name, pool.reserved)
	}

	entry, ok := m.entryMap.emplace(pool)

	if !ok {
		return nil, fmt.Errorf("%s: already exists", pool.name)
	}

	m.rootPoolNum.Add(1)
	return entry, nil
}

func (m *MemArbitrator) adjustSoftLimitWithLock() {
	if m.mu.softLimit.mode == SoftLimitModeSpecified {
		if m.mu.softLimit.specified.size > 0 {
			m.mu.softLimit.size = min(m.mu.softLimit.specified.size, m.mu.limit)
		} else {
			m.mu.softLimit.size = min(multiRatio(m.mu.limit, m.mu.softLimit.specified.rate), m.mu.limit)
		}
	} else {
		m.mu.softLimit.size = m.mu.threshold.oomRisk
	}
}

// SetSoftLimit sets the soft limit of the mem-arbitrator
func (m *MemArbitrator) SetSoftLimit(softLimit int64, sortLimitRate float64, mode SoftLimitMode) {
	m.mu.Lock()
	//
	m.mu.softLimit.mode = mode
	if mode == SoftLimitModeSpecified {
		m.mu.softLimit.specified.size = softLimit
		m.mu.softLimit.specified.rate = intoRatio(sortLimitRate)
	}
	m.adjustSoftLimitWithLock()
	//
	m.mu.Unlock()
}

func (m *MemArbitrator) softLimit() int64 {
	return atomic.LoadInt64(&m.mu.softLimit.size)
}

// SoftLimit returns the soft limit of the mem-arbitrator
func (m *MemArbitrator) SoftLimit() int64 {
	return m.softLimit()
}

// SetLimit sets the limit of the mem-arbitrator and returns whether the limit has changed
func (m *MemArbitrator) SetLimit(x uint64) (changed bool) {
	limit := min(int64(x), DefMaxLimit)
	if limit <= 0 {
		return
	}

	needWake := false
	{
		m.mu.Lock()
		//
		if limit != m.mu.limit {
			changed = true
			needWake = limit > m.mu.limit // update to a greater limit
			m.mu.limit = limit
			m.mu.threshold.risk = int64(float64(limit) * defCheckSafeRatio)
			m.mu.threshold.oomRisk = int64(float64(limit) * defCheckOOMRatio)
			m.adjustSoftLimitWithLock()
		}
		//
		m.mu.Unlock()
	}

	if changed {
		m.resetStatistics()
	}

	if needWake {
		m.weakWake()
	}
	return
}

func (m *MemArbitrator) resetStatistics() {
	m.poolAllocStats.Lock()
	//
	m.poolAllocStats.PoolAllocProfile = m.PoolAllocProfile()
	for i := range m.poolAllocStats.timedMap {
		m.poolAllocStats.timedMap[i].statisticsTimedMapElement = statisticsTimedMapElement{}
	}
	//
	m.poolAllocStats.Unlock()
}

func (m *MemArbitrator) release(x int64) {
	if x <= 0 {
		return
	}
	{
		m.mu.Lock()
		//
		m.mu.allocated -= x
		//
		m.mu.Unlock()
	}
}

func (m *MemArbitrator) allocated() int64 {
	return atomic.LoadInt64(&m.mu.allocated)
}

func (m *MemArbitrator) lastBlockedAt() (allocated, utimeSec int64) {
	return atomic.LoadInt64(&m.execMu.blockedState.allocated), atomic.LoadInt64(&m.execMu.blockedState.utimeSec)
}

// Allocated returns the allocated mem quota of the mem-arbitrator
func (m *MemArbitrator) Allocated() int64 {
	return m.allocated()
}

// PendingAllocSize returns the pending alloc mem quota of the mem-arbitrator
func (m *MemArbitrator) PendingAllocSize() int64 {
	return m.tasks.pendingAlloc.Load()
}

// TaskNum returns the number of pending tasks in the mem-arbitrator
func (m *MemArbitrator) TaskNum() int64 {
	return m.tasks.fifoTasks.size()
}

// RootPoolNum returns the number of root pools in the mem-arbitrator
func (m *MemArbitrator) RootPoolNum() int64 {
	return m.rootPoolNum.Load()
}

func (m *MemArbitrator) limit() int64 {
	return atomic.LoadInt64(&m.mu.limit)
}

// Limit returns the mem quota limit of the mem-arbitrator
func (m *MemArbitrator) Limit() uint64 {
	return uint64(m.limit())
}

func (m *MemArbitrator) allocateFromArbitrator(remainBytes int64, leastLeft int64) (bool, int64) {
	reclaimedBytes := int64(0)
	ok := false
	{
		m.mu.Lock()
		//
		if m.mu.allocated <= m.mu.limit-leastLeft-remainBytes {
			m.mu.allocated += remainBytes
			reclaimedBytes += remainBytes
			ok = true
		} else if v := m.mu.limit - leastLeft - m.mu.allocated; v > 0 {
			m.mu.allocated += v
			reclaimedBytes += v
		}
		//
		m.mu.Unlock()
	}

	return ok, reclaimedBytes
}

func (m *MemArbitrator) doReclaimMemByPriority(target *rootPoolEntry, remainBytes int64) {
	underReclaimBytes := int64(0)

	// check under canceling pool entries
	if m.underCancel.num > 0 {
		now := m.innerTime()
		for uid, entry := range m.underCancel.entries {
			ctx := &entry.arbitratorMu.underCancel
			if ctx.fail {
				continue
			}
			if deadline := ctx.startTime.Add(defKillCancelCheckTimeout); now.Compare(deadline) >= 0 {
				m.actions.Warn("Failed to `CANCEL` root pool due to timeout",
					zap.Uint64("uid", uid),
					zap.String("name", entry.pool.name),
					zap.Int64("quota-to-reclaim", ctx.reclaim),
					zap.String("mem-priority", entry.ctx.memPriority.String()),
					zap.Time("start-time", ctx.startTime),
					zap.Time("deadline", deadline),
				)
				ctx.fail = true
				continue
			}
			underReclaimBytes += ctx.reclaim
		}
	}

	// remain-bytes <= 0
	if underReclaimBytes >= remainBytes {
		return
	}

	// task whose mode is wait_averse must have been cleaned

	for prio := minArbitrationPriority; prio < target.ctx.memPriority; prio++ {
		for pos := m.entryMap.maxQuotaShardIndex - 1; pos >= m.entryMap.minQuotaShardIndexToCheck; pos-- {
			for _, entry := range m.entryMap.quotaShards[prio][pos].entries {
				if entry.arbitratorMu.underCancel.start || entry.notRunning() {
					continue
				}
				if entry.ctx.waitAverse {
					continue
				}
				if ctx := entry.ctx.Load(); ctx.available() {
					m.execMetrics.cancel.priorityMode[prio]++
					ctx.stop(false)

					if m.removeTask(entry) {
						entry.windUp(0, ArbitrateFail)
					}
					m.addUnderCancel(entry, entry.arbitratorMu.quota, m.innerTime())
					underReclaimBytes += entry.arbitratorMu.quota
					if underReclaimBytes >= remainBytes {
						return
					}
				}
			}
		}
	}
}

func (m *MemArbitrator) allocateFromPrivilegedBudget(target *rootPoolEntry, remainBytes int64) (bool, int64) {
	ok := false
	if m.privilegedEntry == target {
		ok = true
	} else if m.privilegedEntry == nil && target.ctx.preferPrivilege {
		if target.intoExecPrivileged() {
			m.privilegedEntry = target
			ok = true
		} else {
			ok = false
		}
	}

	if !ok {
		return false, 0
	}

	{
		m.mu.Lock()
		//
		m.mu.allocated += remainBytes
		//
		m.mu.Unlock()
	}

	return ok, remainBytes
}

func (m *MemArbitrator) tryRuntimeGC() bool {
	if atomic.SwapInt32(&m.ableForRuntimeGC, 0) != 0 {
		m.updateTrackedMemStats()
		m.reclaimHeap()
		return true
	}
	return false
}

// reserved buffer for arbitrate process
func (m *MemArbitrator) reservedBuffer() int64 {
	if m.execMu.mode == ArbitratorModePriority {
		return m.buffer.size.Load()
	}
	return 0
}

func (m *MemArbitrator) arbitrate(target *rootPoolEntry) (bool, int64) {
	reclaimedBytes := int64(0)
	remainBytes := target.request.quota

	forceSpecialBudget := false
	for m.heapController.heapAlloc.Load() > m.limit()-m.reservedBuffer()-remainBytes {
		if !m.tryRuntimeGC() {
			forceSpecialBudget = true // only could alloc from privileged budget
			break
		}
	}

	{
		ok := false
		reclaimed := int64(0)
		if m.execMu.mode == ArbitratorModePriority {
			ok, reclaimed = m.allocateFromPrivilegedBudget(target, remainBytes)
			reclaimedBytes += reclaimed
			remainBytes -= reclaimed
		}
		if ok {
			return true, reclaimedBytes
		} else if forceSpecialBudget {
			return false, reclaimedBytes
		}
	}

	for {
		ok, reclaimed := m.allocateFromArbitrator(remainBytes, m.reservedBuffer()+m.avoidance.size.Load())
		reclaimedBytes += reclaimed
		remainBytes -= reclaimed
		if ok {
			return true, reclaimedBytes
		}
		if !m.tryRuntimeGC() {
			break
		}
	}

	return false, reclaimedBytes
}

// NewMemArbitrator creates a new mem-arbitrator heap instance
func NewMemArbitrator(
	limit int64,
	shardNum uint64,
	maxQuotaShardNum int,
	minQuotaForReclaim int64,
	recorder RecordMemState,
) *MemArbitrator {
	m := newMemArbitrator(limit, shardNum, maxQuotaShardNum, minQuotaForReclaim, recorder)
	return m
}

func newMemArbitrator(limit int64, shardNum uint64, maxQuotaShardNum int, minQuotaForReclaim int64, recorder RecordMemState) *MemArbitrator {
	if limit <= 0 {
		limit = DefMaxLimit
	}
	m := &MemArbitrator{
		mode: ArbitratorModeDisable,
	}
	m.tasks.fifoTasks.init()
	for i := range m.tasks.fifoByPriority {
		m.tasks.fifoByPriority[i].init()
	}
	shardNum = nextPow2(shardNum)
	m.tasks.fifoWaitAverse.init()
	m.notifer = NewNotifer()
	m.entryMap.init(shardNum, maxQuotaShardNum, minQuotaForReclaim)
	m.mu.limit = limit
	m.mu.softLimit.size = limit
	m.mu.threshold.oomRisk = limit
	m.mu.threshold.risk = limit
	m.resetStatistics()
	m.SetMinHeapFreeSpeedBPS(defMinHeapFreeSpeedBPS)
	m.cleanupMu.fifoTasks.init()
	m.underKill.init()
	m.underCancel.init()
	{
		f := func(string, ...zap.Field) {}
		m.actions.Info = f
		m.actions.Warn = f
		m.actions.Error = f
	}
	{
		m.heapController.memStateRecorder.Lock()
		//
		m.heapController.memStateRecorder.RecordMemState = recorder
		if s, err := recorder.Load(); err == nil {
			m.heapController.memStateRecorder.lastMemState.Store(s)
		}
		//
		m.heapController.memStateRecorder.Unlock()
	}
	m.digestProfileCache.shards = make([]digestProfileShard, shardNum)
	m.digestProfileCache.shardsMask = shardNum - 1
	return m
}

func (m *MemArbitrator) doCancelPendingTasks(prio ArbitrationPriority, waitAverse bool) (cnt int64) {
	var entries [64]*rootPoolEntry

	fifo := &m.tasks.fifoWaitAverse
	if !waitAverse {
		fifo = &m.tasks.fifoByPriority[prio]
	}

	for {
		size := 0
		{
			m.tasks.Lock()
			//
			for {
				entry := fifo.front()
				if entry == nil {
					break
				}
				if m.removeTaskImpl(entry) {
					entries[size] = entry
					size++
				}
				if size == len(entries) {
					break
				}
			}
			//
			m.tasks.Unlock()
		}

		for i := range size {
			entry := entries[i]
			if ctx := entry.ctx.Load(); ctx.available() {
				ctx.stop(false)
			}
			entry.windUp(0, ArbitrateFail)
		}

		cnt += int64(size)

		if size != len(entries) {
			break
		}
	}

	return cnt
}

func (m *MemArbitrator) doExecuteFirstTask() (exec bool) {
	if m.tasks.fifoTasks.empty() {
		return
	}

	entry := m.extractFirstTaskEntry()

	if entry == nil {
		return
	}

	if entry.arbitratorMu.destroyed {
		if m.removeTask(entry) {
			entry.windUp(0, ArbitrateFail)
		}
		return true
	}

	{
		ok, reclaimedBytes := m.arbitrate(entry)

		if ok {
			exec = true

			if m.removeTask(entry) {
				if m.execMu.mode == ArbitratorModePriority {
					m.execMetrics.task.successByPriority[entry.taskMu.fifoByPriority.priority]++
				}
				m.entryMap.addQuota(entry, reclaimedBytes)
				// wind up & publish the result
				entry.windUp(reclaimedBytes, ArbitrateOk)
			} else {
				// subscription task may have been canceled
				m.release(reclaimedBytes)
			}
		} else {
			m.release(reclaimedBytes)
			m.execMu.blockedState.allocated = m.mu.allocated
			m.execMu.blockedState.utimeSec = m.UnixTimeSec
			m.doReclaimByWorkMode(entry, reclaimedBytes)
		}
	}

	return
}

func (m *MemArbitrator) doReclaimNonBlockingTasks() {
	if m.execMu.mode == ArbitratorModeStandard {
		for prio := minArbitrationPriority; prio < maxArbitrationPriority; prio++ {
			if m.taskNumByPriority(prio) != 0 {
				m.execMetrics.cancel.standardMode += m.doCancelPendingTasks(prio, false)
			}
		}
	} else if m.taskNumOfWaitAverse() != 0 {
		m.execMetrics.cancel.waitAverse += m.doCancelPendingTasks(maxArbitrationPriority, true)
	}
}

func (m *MemArbitrator) doReclaimByWorkMode(entry *rootPoolEntry, reclaimedBytes int64) {
	m.doReclaimNonBlockingTasks()
	if entry.ctx.waitAverse {
		return
	}
	if m.execMu.mode == ArbitratorModePriority {
		m.doReclaimMemByPriority(entry, entry.request.quota-reclaimedBytes)
	}
}

func (m *MemArbitrator) doExecuteCleanupTasks() {
	for {
		var entry *rootPoolEntry
		{
			m.cleanupMu.Lock()
			//
			entry = m.cleanupMu.fifoTasks.popFront()
			//
			m.cleanupMu.Unlock()
		}
		if entry == nil {
			break
		}

		if m.privilegedEntry == entry {
			m.privilegedEntry = nil
		}
		m.deleteUnderCancel(entry)
		if entry.state() != PoolEntryStateStop { // reset pool entry
			var toRelease int64
			{
				entry.stateMu.pendingReset.Lock()
				//
				toRelease = entry.stateMu.pendingReset.quota
				entry.stateMu.pendingReset.quota = 0
				//
				entry.stateMu.pendingReset.Unlock()
			}
			if toRelease > 0 {
				m.release(toRelease)
				m.entryMap.addQuota(entry, -toRelease)
			}
		} else { // remove pool entry
			m.deleteUnderKill(entry)

			if !entry.arbitratorMu.destroyed {
				m.release(entry.arbitratorMu.quota)
				m.entryMap.delete(entry)
				m.rootPoolNum.Add(-1)
				entry.arbitratorMu.destroyed = true
			}

			if m.removeTask(entry) {
				entry.windUp(0, ArbitrateFail)
			}
		}
	}
}

func (m *MemArbitrator) implicitRun() { // satisfy any subscription task
	if m.tasks.fifoTasks.empty() {
		return
	}
	{
		m.execMu.Lock()
		//
		for { // make all tasks success
			entry := m.frontTaskEntry()
			if entry == nil {
				break
			}

			if entry.arbitratorMu.destroyed {
				if m.removeTask(entry) {
					entry.windUp(0, ArbitrateFail)
				}
				continue
			}

			if m.removeTask(entry) {
				{
					m.mu.Lock()
					//
					m.mu.allocated += entry.request.quota
					//
					m.mu.Unlock()
				}

				m.entryMap.addQuota(entry, entry.request.quota)
				entry.windUp(entry.request.quota, ArbitrateOk)
			}
		}
		//
		m.execMu.Unlock()
	}
}

// -1: at ArbitratorModeDisable
// -2: mem unsafe
// >= 0: execute / cancel task num
func (m *MemArbitrator) runOneRound() (taskExecNum int) {
	m.execMu.startTime = now()
	if t := m.execMu.startTime.Unix(); t != m.UnixTimeSec { // update per second duration and reduce force sharing
		m.UnixTimeSec = t
	}

	if mode := m.workMode(); m.execMu.mode != mode {
		m.execMu.mode = mode
		if mode == ArbitratorModeDisable { // switch to disable mode
			m.execMu.blockedState = blockedState{}
		}
	}

	if !m.cleanupMu.fifoTasks.empty() {
		m.execMu.Lock()
		//
		m.doExecuteCleanupTasks()
		//
		m.execMu.Unlock()
	}

	if m.execMu.mode == ArbitratorModeDisable {
		m.implicitRun()
		return -1
	}

	if !m.handleMemIssues() { // mem is still unsafe
		return -2
	}

	{
		m.execMu.Lock()
		//
		for m.doExecuteFirstTask() {
			taskExecNum++
		}
		//
		m.execMu.Unlock()
	}
	return taskExecNum
}

func (m *MemArbitrator) asyncRun(duration time.Duration) bool {
	if m.controlMu.running {
		return false
	}
	m.controlMu.running = true
	m.controlMu.finishCh = make(chan struct{})

	go func() {
		ticker := time.NewTicker(duration)
		for m.controlMu.running {
			select {
			case <-ticker.C:
				m.weakWake()
			case <-m.notifer.C:
				m.notifer.setNotAwake()
				m.runOneRound()
			}
		}

		ticker.Stop()
		close(m.controlMu.finishCh)
	}()
	return true
}

// StartWithMemArbitratorContext starts the root pool with the given context
func (r *RootPoolEntry) StartWithMemArbitratorContext(m *MemArbitrator, ctx *ArbitrationContext) bool {
	return m.restartEntryByContext(r.entry, ctx)
}

func (m *MemArbitrator) restartEntryByContext(entry *rootPoolEntry, ctx *ArbitrationContext) bool {
	if entry == nil {
		return false
	}
	entry.stateMu.Lock()
	defer entry.stateMu.Unlock()

	if entry.state() != PoolEntryStateNormal || entry.execState() != execStateIdle {
		return false
	}

	entry.pool.mu.Lock()
	defer entry.pool.mu.Unlock()

	if ctx != nil {
		if ctx.hisMaxMemUsed > m.buffer.size.Load() {
			m.setBufferSize(ctx.hisMaxMemUsed)
		} else if ctx.memQuotaLimit > m.buffer.quotaLimit.Load() {
			m.setBufferSize(ctx.memQuotaLimit)
			m.setQuotaLimit(ctx.memQuotaLimit)
		}

		if ctx.waitAverse {
			entry.ctx.preferPrivilege = false
			entry.ctx.memPriority = ArbitrationPriorityHigh
		} else {
			entry.ctx.preferPrivilege = ctx.preferPrivilege
			entry.ctx.memPriority = ctx.memPriority
		}

		entry.ctx.cancelCh = ctx.cancelCh
		entry.ctx.waitAverse = ctx.waitAverse
	} else {
		entry.ctx.cancelCh = nil
		entry.ctx.waitAverse = false
		entry.ctx.memPriority = ArbitrationPriorityMedium
		entry.ctx.preferPrivilege = false
	}

	entry.ctx.Store(ctx)

	if _, loaded := m.entryMap.contextCache.LoadOrStore(entry.pool.uid, entry); !loaded {
		m.entryMap.contextCache.num.Add(1)
	}

	if entry.pool.actions.OutOfCapacityActionCB == nil {
		entry.pool.actions.OutOfCapacityActionCB = func(s OutOfCapacityActionArgs) error {
			if m.blockingAllocate(entry, s.Request) != ArbitrateOk {
				return errArbitrateFailError
			}
			return nil
		}
	}
	entry.pool.mu.stopped = false

	entry.setExecState(execStateRunning)

	return true
}

// DebugFields is used to store debug fields for logging
type DebugFields struct {
	fields [30]zap.Field
	n      int
}

// ConcurrentBudget represents a wrapped budget of the resource pool for concurrent usage
type ConcurrentBudget struct {
	pool            *ResourcePool
	Used            atomic.Int64
	Capacity        atomic.Int64
	HeapInuse       atomic.Int64
	LastUsedTimeSec int64
	sync.Mutex
}

// FixSizeBatchBudgets represents a fixed batch of concurrent budgets
type FixSizeBatchBudgets struct {
	shards   []ConcurrentBudget
	sizeMask uint64
}

func (s *FixSizeBatchBudgets) shrink(minRemain int64, utimeMilli int64) (reclaimed int64) {
	for i := range s.shards {
		b := &s.shards[i]

		if used := b.Used.Load(); used > 0 {
			if b.Capacity.Load()-(used+minRemain) >= b.pool.allocAlignSize && b.TryLock() {
				if used = b.Used.Load(); used > 0 {
					toReclaim := b.Capacity.Load() - (used + minRemain)

					if toReclaim >= b.pool.allocAlignSize {
						b.Capacity.Add(-toReclaim)
						reclaimed += toReclaim
					}
				}
				b.Unlock()
			}
		} else {
			if b.Capacity.Load() > 0 && b.LastUsedTimeSec*kilo+defAwaitFreePoolShrinkDurMilli <= utimeMilli && b.TryLock() {
				if toReclaim := b.Capacity.Load(); b.Used.Load() <= 0 && toReclaim > 0 {
					b.Capacity.Add(-toReclaim)
					reclaimed += toReclaim
				}
				b.Unlock()
			}
		}
	}

	return
}

// InitUpperPool initializes the upper pool of the concurrent budget
func (b *ConcurrentBudget) InitUpperPool(pool *ResourcePool) {
	b.pool = pool
}

// Clear clears the concurrent budget and returns the capacity
func (b *ConcurrentBudget) Clear() int64 {
	b.Lock()
	//
	budgetCap := b.Capacity.Swap(0)
	b.Used.Store(0)
	if budgetCap > 0 {
		b.pool.release(budgetCap)
	}
	b.pool = nil
	//
	b.Unlock()
	return budgetCap
}

// Reserve reserves a given capacity for the concurrent budget
func (b *ConcurrentBudget) Reserve(newCap int64) (duration time.Duration, err error) {
	b.Lock()
	startTime := time.Now()
	//
	extra := max(newCap, b.Used.Add(0), b.Capacity.Load()) - b.Capacity.Load()
	if err = b.pool.allocate(extra); err == nil {
		b.Capacity.Add(extra)
	}
	//
	duration = time.Since(startTime)
	b.Unlock()
	return
}

// PullFromUpstream tries to pull from the upstream pool if the used size exceeds the capacity
func (b *ConcurrentBudget) PullFromUpstream() (duration time.Duration, err error) {
	b.Lock()
	startTime := time.Now()
	//
	x := b.Used.Add(0) - b.Capacity.Load()
	if x > 0 {
		extra := b.pool.roundSize(x)
		if err = b.pool.allocate(extra); err == nil {
			b.Capacity.Add(extra)
		}
	}
	//
	duration = time.Since(startTime)
	b.Unlock()
	return
}

// PullFromUpstreamV2 tries to pull from the upstream pool with a limit on the size
func (b *ConcurrentBudget) PullFromUpstreamV2(maxSize int64) (duration time.Duration, err error) {
	b.Lock()
	startTime := time.Now()
	//
	used, capacity := b.Used.Add(0), b.Capacity.Load()
	if diff := used - capacity; diff > 0 {
		extra := max(min(max(((used*2850)>>10)-capacity, b.pool.allocAlignSize), maxSize), b.pool.roundSize(diff))
		if err = b.pool.allocate(extra); err == nil {
			b.Capacity.Add(extra)
		}
	}
	//
	duration = time.Since(startTime)
	b.Unlock()
	return
}

// AutoRun starts the work groutine of the mem-arbitrator asynchronously
func (m *MemArbitrator) AutoRun(
	actions MemArbitratorActions,
) bool {
	return m.autoRun(
		actions,
		defAwaitFreePoolAllocAlignSize,
		defAwaitFreePoolShardNum,
		defTaskTickDur,
	)
}

func (m *MemArbitrator) autoRun(
	actions MemArbitratorActions,
	awaitFreePoolAllocAlignSize, awaitFreePoolShardNum int64,
	taskTickDur time.Duration,
) bool {
	m.controlMu.Lock()
	defer m.controlMu.Unlock()

	if m.controlMu.running {
		return false
	}

	{ // init
		m.actions = actions
		m.refreshRuntimeMemStats()
		m.initAwaitFreePool(awaitFreePoolAllocAlignSize, awaitFreePoolShardNum)
		m.SetAbleToGC()
	}
	return m.asyncRun(taskTickDur)
}

func (m *MemArbitrator) refreshRuntimeMemStats() {
	if m.actions.UpdateRuntimeMemStats != nil {
		m.actions.UpdateRuntimeMemStats() // invoke `SetRuntimeMemStats`
	}
	m.execMetrics.action.updateRuntimeMemStats++
}

// RuntimeMemStats represents the runtime memory statistics
type RuntimeMemStats struct {
	Alloc, HeapInuse, TotalAlloc, StackInuse int64
}

// SetRuntimeMemStats sets the runtime memory statistics. It may be invoked by: `refreshRuntimeMemStats -> action.UpdateRuntimeMemStats`; `HandleRuntime`;
func (m *MemArbitrator) SetRuntimeMemStats(s RuntimeMemStats) {
	m.heapController.heapAlloc.Store(s.Alloc)
	m.heapController.heapInuse.Store(s.HeapInuse) // heapInuse >= alloc
	m.heapController.heapTotalFree.Store(s.TotalAlloc - s.Alloc)
	m.heapController.stackInuse.Store(s.StackInuse)

	m.updateAvoidSize() // update out-of-control / avoidance size
}

func (m *MemArbitrator) updateAvoidSize() {
	capacity := m.mu.softLimit.size
	if m.mu.softLimit.mode == SoftLimitModeAuto {
		if ratio := m.memMagnif(); ratio != 0 {
			newCap := calcRatio(m.mu.limit, ratio)
			capacity = min(capacity, newCap)
		}
	}
	avoidSize := max(
		0,
		m.heapController.heapInuse.Load()+m.heapController.stackInuse.Load()-m.avoidance.heapTracked.Load(), // out of control size
		m.mu.limit-capacity,
	)
	m.avoidance.size.Store(avoidSize)
}

func (m *MemArbitrator) weakWake() {
	m.notifer.WeakWake()
}

func (m *MemArbitrator) wake() {
	m.notifer.Wake()
}

func (m *MemArbitrator) updatePoolMediumCapacity(utimeMilli int64) {
	s := &m.poolAllocStats
	const maxNum = int64(len(s.timedMap))
	const maxDur = maxNum - defRedundancy

	{
		s.RLock()
		//

		tsAlign := utimeMilli / kilo / defUpdateMemConsumedTimeAlignSec
		tar1 := &s.timedMap[(maxNum+tsAlign-1)%maxNum]
		tar2 := &s.timedMap[tsAlign%maxNum]

		if ts := tar1.tsAlign.Load(); ts <= tsAlign-maxDur || ts > tsAlign {
			tar1 = nil
		}
		if ts := tar2.tsAlign.Load(); ts <= tsAlign-maxDur || ts > tsAlign {
			tar2 = nil
		}

		total := uint32(0)
		if tar1 != nil {
			tar1.RLock()
			total += tar1.num.Add(0)
		}
		if tar2 != nil {
			tar2.RLock()
			total += tar2.num.Add(0)
		}

		if total != 0 {
			expect := max(1, (total+1)/2)
			cnt := uint32(0)
			index := 0

			for i := range defServerlimitMinUnitNum {
				if tar1 != nil {
					cnt += tar1.slot[i]
				}
				if tar2 != nil {
					cnt += tar2.slot[i]
				}
				if cnt >= expect {
					index = i
					break
				}
			}

			res := s.PoolAllocUnit * int64(index+1)

			s.mediumQuota.Store(res)
		}

		if tar1 != nil {
			tar1.RUnlock()
		}
		if tar2 != nil {
			tar2.RUnlock()
		}

		//
		s.RUnlock()
	}

	m.tryStorePoolMediumCapacity(utimeMilli, m.poolMediumQuota())
}

func (m *MemArbitrator) tryStorePoolMediumCapacity(utimeMilli int64, capacity int64) bool {
	if lastState := m.lastMemState(); lastState == nil ||
		(m.poolAllocStats.lastStoreUtimeMilli.Load()+defTickDurMilli*10 /* 10x */ <= utimeMilli &&
			lastState.PoolMediumCap != capacity) {
		var memState *RuntimeMemStateV1

		if lastState != nil {
			s := *lastState // copy
			s.PoolMediumCap = capacity
			memState = &s
		} else {
			memState = &RuntimeMemStateV1{
				Version:       1,
				PoolMediumCap: capacity,
			}
		}

		_ = m.recordMemState(memState, "new root pool medium cap")
		m.poolAllocStats.lastStoreUtimeMilli.Store(utimeMilli)
		return true
	}
	return false
}

func (m *MemArbitrator) poolMediumQuota() int64 {
	return m.poolAllocStats.mediumQuota.Load()
}

// SuggestPoolInitCap returns the suggested initial capacity for the pool
func (m *MemArbitrator) SuggestPoolInitCap() int64 {
	return m.poolMediumQuota()
}

func (m *MemArbitrator) updateMemMagnification(utimeMilli int64) (updatedPreProf *memProfile) {
	const maxNum = int64(len(m.heapController.timedMemProfile))

	curTsAlign := utimeMilli / kilo / defUpdateMemMagnifUtimeAlign
	profs := &m.heapController.timedMemProfile
	cur := &profs[curTsAlign%maxNum]
	if cur.tsAlign < curTsAlign {
		{ // update previous record
			preTs := curTsAlign - 1
			preIdx := (maxNum + preTs) % maxNum
			pre := &profs[preIdx]

			pre.ratio = 0
			if pre.tsAlign == preTs && pre.quota > 0 {
				if pre.heap > 0 {
					pre.ratio = calcRatio(pre.heap, pre.quota)
				}
				updatedPreProf = pre
			}
		}

		v := int64(0)
		// check the memory profile in the last 60s
		for _, tsAlign := range []int64{curTsAlign - 2, curTsAlign - 1} {
			tar := &profs[(maxNum+tsAlign)%maxNum]
			if tar.tsAlign != tsAlign ||
				tar.heap >= m.mu.threshold.oomRisk { // calculate the magnification only when the heap is safe
				v = 0
				break
			}

			if tar.ratio <= 0 {
				break // if any record is not valid,
			}
			v = max(v, tar.ratio)
		}

		updated := false
		var oriRatio, newRatio int64

		if v != 0 && m.avoidance.memMagnif.TryLock() {
			if oriRatio = m.memMagnif(); oriRatio != 0 && v < oriRatio-10 /* 1 percent */ {
				newRatio = (oriRatio + v) / 2
				if newRatio <= kilo {
					newRatio = 0
				}
				m.doSetMemMagnif(newRatio)
				updated = true
			}
			m.avoidance.memMagnif.Unlock()
		}

		if updated {
			m.actions.Info("Update mem quota magnification ratio",
				zap.Int64("ori-ratio(‰)", oriRatio),
				zap.Int64("new-ratio(‰)", newRatio),
			)

			if lastMemState := m.lastMemState(); lastMemState != nil && newRatio < lastMemState.Magnif {
				memState := RuntimeMemStateV1{
					Version:       1,
					Magnif:        newRatio,
					PoolMediumCap: m.poolMediumQuota(),
				}
				_ = m.recordMemState(&memState, "new magnification ratio")
			}
		}

		*cur = memProfile{
			tsAlign:         curTsAlign,
			startUtimeMilli: utimeMilli,
		}
	}

	if cur.tsAlign == curTsAlign {
		if ts := m.heapController.lastGC.endUtimeSec.Load(); curTsAlign == ts/defUpdateMemMagnifUtimeAlign {
			cur.heap = max(cur.heap, m.heapController.lastGC.heapAlloc.Load())
		}
		if blockedSize, utimeSec := m.lastBlockedAt(); blockedSize > 0 &&
			utimeSec/defUpdateMemMagnifUtimeAlign == curTsAlign {
			cur.quota = max(cur.quota, blockedSize)
		}
	}
	return
}

func (m *MemArbitrator) doSetMemMagnif(ratio int64) {
	m.avoidance.memMagnif.ratio.Store(ratio)
}

func (m *MemArbitrator) memMagnif() int64 {
	return m.avoidance.memMagnif.ratio.Load()
}

func (m *MemArbitrator) awaitFreePoolCap() int64 {
	if m.awaitFree.pool == nil {
		return 0
	}
	return m.awaitFree.pool.capacity()
}

func (m *MemArbitrator) awaitFreePoolHeapInuse() (res int64) {
	for i := range m.awaitFree.budget.shards {
		d := m.awaitFree.budget.shards[i].HeapInuse.Load()
		if d > 0 {
			res += d
		}
	}
	m.awaitFree.lastHeapInuse = res
	return
}

func (m *MemArbitrator) awaitFreePoolUsed() (res int64) {
	for i := range m.awaitFree.budget.shards {
		d := m.awaitFree.budget.shards[i].Used.Load()
		if d > 0 {
			res += d
		}
	}
	m.awaitFree.lastQuotaUsed = res
	return
}

func (m *MemArbitrator) executeTick(utimeMilli int64) bool { // exec batch tasks every 1s
	if m.heapController.oomCheck.start { // skip if oom check is running because mem state is not safe
		return false
	}

	if m.tickTask.lastTickUtimeMilli.Load()+defTickDurMilli > utimeMilli {
		return false
	}
	m.tickTask.Lock()
	defer m.tickTask.Unlock()

	m.tickTask.lastTickUtimeMilli.Store(utimeMilli)

	// mem magnification
	if updatedPreProf := m.updateMemMagnification(utimeMilli); updatedPreProf != nil {
		pre := updatedPreProf
		profile := m.recordDebugProfile()
		profile.append(
			zap.Int64("blocked-heap", pre.heap), zap.Int64("blocked-quota", pre.quota),
			zap.Int64("magnification-ratio(‰)", pre.ratio),
			zap.Time("start-time", time.UnixMilli(pre.startUtimeMilli)),
		)
		m.actions.Info("arbitrator timed mem profile",
			profile.fields[:profile.n]...,
		)
	}
	// suggest pool cap
	m.updatePoolMediumCapacity(utimeMilli)

	// shrink mem profile cache
	m.shrinkDigestProfile(utimeMilli/kilo, 2e6, 1e6)

	return true
}

func (d *DebugFields) append(f ...zap.Field) {
	n := min(len(f), len(d.fields)-d.n)
	for i := range n {
		d.fields[d.n] = f[i]
		d.n++
	}
}

func (m *MemArbitrator) recordDebugProfile() (f DebugFields) {
	taskNumByMode := m.TaskNumByMode()
	memMagnif := m.memMagnif()
	if memMagnif == 0 {
		memMagnif = -1
	}
	f.append(
		zap.Int64("mem-inuse", m.heapController.heapInuse.Load()),
		zap.Int64("mem-alloc", m.heapController.heapAlloc.Load()),
		zap.Int64("mem-alloc-risk", m.mu.threshold.risk),
		zap.Int64("mem-inuse-oomrisk", m.mu.threshold.oomRisk),
		zap.Int64("quota-allocated", m.mu.allocated),
		zap.Int64("quota-limit", m.mu.limit),
		zap.Int64("quota-softlimit", m.mu.softLimit.size),
		zap.Int64("mem-magnification-ratio(‰)", memMagnif),
		zap.Int64("root-pool-num", m.RootPoolNum()),
		zap.Int64("await-free-cap", m.awaitFreePoolCap()),
		zap.Int64("await-free-used", m.awaitFree.lastQuotaUsed),
		zap.Int64("await-free-heap", m.awaitFree.lastHeapInuse),
		zap.Int64("heap-tracked", m.avoidance.heapTracked.Load()),
		zap.Int64("out-of-control", m.avoidance.size.Load()),
		zap.Int64("reserved-buffer", m.buffer.size.Load()),
		zap.Int64("alloc-task-num", m.TaskNum()),
		zap.Int64("pending-alloc-size", m.PendingAllocSize()),
		zap.Int64("task-priority-low", taskNumByMode[ArbitrationPriorityLow]),
		zap.Int64("task-priority-medium", taskNumByMode[ArbitrationPriorityMedium]),
		zap.Int64("task-priority-high", taskNumByMode[ArbitrationPriorityHigh]),
		zap.Int64("digest-profile-num", m.digestProfileCache.num.Load()),
	)
	if m.heapController.oomCheck.start {
		f.append(zap.Time("oom-check-start", m.heapController.oomCheck.startTime))
	}
	return
}

// HandleRuntime handles the runtime memory statistics
func (m *MemArbitrator) HandleRuntime(s RuntimeMemStats) {
	// shrink fast alloc pool
	m.tryShrinkAwaitFreePool(defPoolReservedQuota, nowUnixMilli())
	// update tracked mem stats
	m.tryUpdateTrackedMemStats(nowUnixMilli())
	// set runtime mem stats & update avoidance size
	m.SetRuntimeMemStats(s)
	m.executeTick(nowUnixMilli())
	m.weakWake()
}

func (m *MemArbitrator) tryUpdateTrackedMemStats(utimeMilli int64) bool {
	if m.avoidance.lastUpdateUtimeMilli.Load()+defTrackMemStatsDurMilli <= utimeMilli {
		m.updateTrackedMemStats()
		return true
	}
	return false
}

func (m *MemArbitrator) updateTrackedMemStats() {
	totalHeapInuse := int64(0)
	if m.entryMap.contextCache.num.Load() != 0 {
		maxMemUsed := int64(0)
		m.entryMap.contextCache.Range(func(_, value any) bool {
			e := value.(*rootPoolEntry)
			if e.notRunning() {
				return true
			}
			if ctx := e.ctx.Load(); ctx.available() {
				memUsed := ctx.arbitrateHelper.HeapInuse()
				totalHeapInuse += memUsed
				maxMemUsed = max(maxMemUsed, memUsed)
			}
			return true
		})
		if m.buffer.size.Load() < maxMemUsed {
			m.tryToUpdateBuffer(maxMemUsed, 0, m.UnixTimeSec)
		}
	}

	awaitFreeUsed := m.awaitFreePoolHeapInuse()
	totalHeapInuse += awaitFreeUsed

	m.avoidance.heapTracked.Store(totalHeapInuse)
	m.avoidance.lastUpdateUtimeMilli.Store(nowUnixMilli())
}

func (m *MemArbitrator) tryShrinkAwaitFreePool(minRemain int64, utimeMilli int64) bool {
	if m.awaitFree.lastShrinkUtimeMilli.Load()+defAwaitFreePoolShrinkDurMilli <= utimeMilli {
		m.shrinkAwaitFreePool(minRemain, utimeMilli)
		return true
	}
	return false
}

func (m *MemArbitrator) shrinkAwaitFreePool(minRemain int64, utimeMilli int64) {
	poolReleased := int64(0)
	reclaimed := m.awaitFree.budget.shrink(minRemain, utimeMilli)
	{
		m.awaitFree.pool.mu.Lock()
		//
		m.awaitFree.pool.doRelease(reclaimed) // release even if `reclaimed` is 0
		poolReleased = m.awaitFree.pool.mu.budget.release()
		//
		m.awaitFree.pool.mu.Unlock()
	}
	if poolReleased > 0 {
		{
			m.mu.Lock()
			//
			m.mu.allocated -= poolReleased
			//
			m.mu.Unlock()
		}
		atomic.AddInt64(&m.execMetrics.awaitFree.shrink, 1)
		m.weakWake()
	}
	m.awaitFree.lastShrinkUtimeMilli.Store(nowUnixMilli())
}

func (m *MemArbitrator) hasNoMemRisk() bool {
	return m.heapController.heapAlloc.Load() < m.mu.threshold.risk
}

func (m *MemArbitrator) isMemSafe() bool {
	return m.heapController.heapInuse.Load() < m.mu.threshold.oomRisk
}

func (m *MemArbitrator) calcMemRisk() *RuntimeMemStateV1 {
	memState := RuntimeMemStateV1{
		Version: 1,
		LastRisk: LastRisk{
			HeapAlloc:  m.heapController.heapAlloc.Load(),
			QuotaAlloc: m.allocated(),
		},

		PoolMediumCap: m.poolMediumQuota(),
	}

	if memState.LastRisk.QuotaAlloc == 0 || memState.LastRisk.HeapAlloc <= memState.LastRisk.QuotaAlloc {
		return nil
	}
	memState.Magnif = calcRatio(memState.LastRisk.HeapAlloc, memState.LastRisk.QuotaAlloc) + 100 /* 10 percent */
	if p := m.lastMemState(); p != nil {
		memState.Magnif = max(memState.Magnif, p.Magnif)
	}

	return &memState
}

// return `true` is memory state is safe
func (m *MemArbitrator) handleMemIssues() (isSafe bool) {
	if m.heapController.oomCheck.start {
		m.heapController.oomCheck.eachRound.gcExecuted = false

		if m.tryRuntimeGC() {
			m.heapController.oomCheck.eachRound.gcExecuted = true
		} else {
			m.refreshRuntimeMemStats()
		}

		if m.isMemSafe() && m.hasNoMemRisk() {
			m.updateTrackedMemStats()
			m.updateAvoidSize() // no need to refresh runtime mem stats

			{ // warning
				profile := m.recordDebugProfile()
				m.actions.Info("Heap memory usage is safe", profile.fields[:profile.n]...)
			}
			m.heapController.oomCheck.start = false
			return true
		}

		m.handleMemUnsafe()
		return false
	} else if !m.isMemSafe() {
		m.handleMemUnsafe()
		return false
	}
	return true
}

func (m *MemArbitrator) innerTime() time.Time {
	if m.debug.now != nil {
		return m.debug.now()
	}
	return now()
}

func (m *MemArbitrator) handleMemUnsafe() {
	{
		m.execMu.Lock()
		//
		m.doReclaimNonBlockingTasks()
		//
		m.execMu.Unlock()
	}

	now := m.innerTime()

	if !m.heapController.oomCheck.start {
		m.heapController.oomCheck.lastMemStats.heapTotalFree = m.heapController.heapTotalFree.Load()
		m.heapController.oomCheck.lastMemStats.startTime = now
		m.heapController.oomCheck.start = true
		m.heapController.oomCheck.startTime = now
		m.execMetrics.risk.memRisk++

		{
			profile := m.recordDebugProfile()
			m.actions.Warn("Heap memory usage reach threshold", profile.fields[:profile.n]...)
		}

		{ // GC
			m.reclaimHeap()
		}

		if memState := m.calcMemRisk(); memState != nil {
			{
				m.avoidance.memMagnif.Lock()
				//
				m.doSetMemMagnif(memState.Magnif)
				//
				m.avoidance.memMagnif.Unlock()
			}

			if err := m.recordMemState(memState, "oom risk"); err != nil {
				m.actions.Error("Failed to save mem-risk", zap.Error(err))
			}
		}

		return
	}

	minHeapFreeSpeedBPS := m.MinHeapFreeSpeedBPS()
	if dur := now.Sub(m.heapController.oomCheck.lastMemStats.startTime); dur >= defHeapReclaimCheckDuration {
		heapFrees := m.heapController.heapTotalFree.Load() - m.heapController.oomCheck.lastMemStats.heapTotalFree
		freeSpeedBPS := int64(float64(heapFrees) / dur.Seconds())
		needReclaimHeap := false
		if hasMemOOMRisk(freeSpeedBPS, minHeapFreeSpeedBPS, now, m.heapController.oomCheck.startTime) {
			m.execMetrics.risk.oomRisk++
			// dumpHeapProfile()
			var newKillNum int
			var reclaiming int64
			memToReclaim := m.heapController.heapInuse.Load() - m.mu.threshold.risk
			{
				{ // warning
					profile := m.recordDebugProfile()
					profile.append(
						zap.Float64("mem-free-speed(MiB/s)", float64(freeSpeedBPS*100/mb)/100),
						zap.Float64("required-speed(MiB/s)", float64(minHeapFreeSpeedBPS)/float64(mb)),
						zap.Int64("mem-to-reclaim", max(0, memToReclaim)),
					)
					m.actions.Warn("Runtime memory free(use) speed is too low, start to reclaim by `KILL`", profile.fields[:profile.n]...)
				}
				newKillNum, reclaiming = m.killTopnEntry(memToReclaim)
			}

			needReclaimHeap = newKillNum == 0
			if newKillNum != 0 {
				m.heapController.oomCheck.startTime = m.innerTime() // restart oom check

				{ // warning
					profile := m.recordDebugProfile()
					profile.append(
						zap.Int64("total-under-kill-num", m.underKill.num), zap.Int("new-kill-num", newKillNum),
						zap.Int64("total-under-reclaim", reclaiming),
						zap.Int64("rest-to-reclaim", max(0, memToReclaim-reclaiming)),
					)
					m.actions.Warn("Restart runtime memory check", profile.fields[:profile.n]...)
				}
			} else if m.underKill.num == 0 {
				// error
				profile := m.recordDebugProfile()
				m.actions.Error("No more root pool in arbitrator can be killed to resolve OOM risk",
					profile.fields[:profile.n]...,
				)
			}
		} else {
			{ // warning
				profile := m.recordDebugProfile()
				profile.append(zap.Float64("mem-free-speed(MiB/s)",
					float64(freeSpeedBPS*100/mb)/100),
					zap.Float64("required-speed(MiB/s)", float64(minHeapFreeSpeedBPS)/float64(mb)))
				m.actions.Warn("Runtime memory free speed meets require, start re-check", profile.fields[:profile.n]...)
			}
			needReclaimHeap = true
		}

		now := m.innerTime()
		m.heapController.oomCheck.lastMemStats.heapTotalFree = m.heapController.heapTotalFree.Load()
		m.heapController.oomCheck.lastMemStats.startTime = now
		if needReclaimHeap && !m.heapController.oomCheck.eachRound.gcExecuted {
			m.reclaimHeap()
		}
	}
}

func hasMemOOMRisk(freeSpeedBPS, minHeapFreeSpeedBPS int64, now, startTime time.Time) bool {
	return freeSpeedBPS < minHeapFreeSpeedBPS || now.Sub(startTime) > defHeapReclaimCheckMaxDuration
}

func (m *MemArbitrator) killTopnEntry(required int64) (newKillNum int, reclaimed int64) {
	m.execMu.Lock()
	defer m.execMu.Unlock()

	if m.underKill.num > 0 {
		now := m.innerTime()
		for uid, entry := range m.underKill.entries {
			ctx := &entry.arbitratorMu.underKill
			if ctx.fail {
				continue
			}
			if deadline := ctx.startTime.Add(defKillCancelCheckTimeout); now.Compare(deadline) >= 0 {
				m.actions.Error("Failed to `KILL` root pool due to timeout",
					zap.Uint64("uid", uid),
					zap.String("name", entry.pool.name),
					zap.Int64("mem-to-reclaim", ctx.reclaim),
					zap.String("mem-priority", entry.ctx.memPriority.String()),
					zap.Time("start-time", ctx.startTime),
					zap.Time("deadline", deadline),
				)
				ctx.fail = true
				continue
			}
			reclaimed += ctx.reclaim
		}
	}

	if reclaimed >= required {
		return
	}

	for prio := minArbitrationPriority; prio < maxArbitrationPriority; prio++ {
		for pos := m.entryMap.maxQuotaShardIndex - 1; pos >= m.entryMap.minQuotaShardIndexToCheck; pos-- {
			for uid, entry := range m.entryMap.quotaShards[prio][pos].entries {
				if entry.arbitratorMu.underKill.start || entry.notRunning() {
					continue
				}

				if ctx := entry.ctx.Load(); ctx.available() {
					memoryUsed := ctx.arbitrateHelper.HeapInuse()

					if memoryUsed <= 0 {
						continue
					}

					m.addUnderKill(entry, memoryUsed, m.innerTime())
					reclaimed += memoryUsed
					ctx.stop(true)
					newKillNum++
					m.execMetrics.risk.oomKill[prio]++

					{ // warning
						m.actions.Warn("Start to `KILL` root pool",
							zap.Uint64("uid", uid),
							zap.String("name", entry.pool.name),
							zap.Int64("mem-used", memoryUsed),
							zap.String("mem-priority", ctx.memPriority.String()),
							zap.Int64("rest-to-reclaim", max(0, required-reclaimed)))
					}
					if m.removeTask(entry) {
						{ // warning
							m.actions.Warn("Make the mem quota subscription failed",
								zap.Uint64("uid", uid), zap.String("name", entry.pool.name))
						}
						entry.windUp(0, ArbitrateFail)
					}

					if reclaimed >= required {
						return
					}
				}
			}
		}
	}

	return
}

// LastRisk represents the last risk state of memory
type LastRisk struct {
	HeapAlloc  int64 `json:"heap"`
	QuotaAlloc int64 `json:"quota"`
}

// RuntimeMemStateV1 represents the runtime memory state
type RuntimeMemStateV1 struct {
	Version  int64    `json:"version"`
	LastRisk LastRisk `json:"last-risk"`
	// magnification ratio of heap-alloc/quota
	Magnif int64 `json:"magnif"`
	// medium quota usage of root pools
	PoolMediumCap int64 `json:"pool-medium-cap"`

	// TODO: top-n profiles by digest
	// topNProfiles [3][2]int64 `json:"top-n-profiles"`
}

func (m *MemArbitrator) recordMemState(s *RuntimeMemStateV1, reason string) error {
	m.heapController.memStateRecorder.Lock()
	defer m.heapController.memStateRecorder.Unlock()
	m.heapController.memStateRecorder.lastMemState.Store(s)

	if err := m.heapController.memStateRecorder.Store(s); err != nil {
		atomic.AddInt64(&m.execMetrics.action.recordMemState.fail, 1)
		return err
	}
	atomic.AddInt64(&m.execMetrics.action.recordMemState.success, 1)
	m.actions.Info("Record mem state",
		zap.String("reason", reason),
		zap.String("data", fmt.Sprintf("%+v", s)),
	)
	return nil
}

// GetAwaitFreeBudgets returns the concurrent budget shard by the given uid
func (m *MemArbitrator) GetAwaitFreeBudgets(uid uint64) *ConcurrentBudget {
	index := shardIndexByUID(uid, m.awaitFree.budget.sizeMask)
	return &m.awaitFree.budget.shards[index]
}

func (m *MemArbitrator) initAwaitFreePool(allocAlignSize, shardNum int64) {
	if allocAlignSize <= 0 {
		allocAlignSize = defAwaitFreePoolAllocAlignSize
	}

	p := &ResourcePool{
		name:           "await-free-pool",
		uid:            0,
		limit:          DefMaxLimit,
		allocAlignSize: allocAlignSize,

		maxUnusedBlocks: 0,
	}

	p.SetOutOfCapacityAction(func(s OutOfCapacityActionArgs) error {
		if m.heapController.heapAlloc.Load() > m.mu.threshold.risk-s.Request {
			m.execMetrics.awaitFree.fail++
			return errArbitrateFailError
		}
		if m.heapController.heapInuse.Load() > m.mu.threshold.oomRisk-s.Request {
			m.execMetrics.awaitFree.fail++
			return errArbitrateFailError
		}

		if m.mu.allocated > m.mu.limit-m.avoidance.size.Load()-s.Request {
			m.execMetrics.awaitFree.fail++
			return errArbitrateFailError
		}

		{
			m.mu.Lock()
			//
			m.mu.allocated += s.Request
			//
			m.mu.Unlock()
		}

		p.forceAddCap(s.Request)

		m.execMetrics.awaitFree.success++

		return nil
	})

	m.awaitFree.pool = p

	{
		cnt := nextPow2(uint64(shardNum))
		m.awaitFree.budget = FixSizeBatchBudgets{
			make([]ConcurrentBudget, cnt),
			cnt - 1,
		}
		for i := range m.awaitFree.budget.shards {
			m.awaitFree.budget.shards[i].InitUpperPool(p)
		}
	}
}

// ArbitrateHelper is an interface for the arbitrate helper
type ArbitrateHelper interface {
	Kill() bool       // kill by arbitrator only when meeting oom risk
	Cancel() bool     // cancel by arbitrator
	HeapInuse() int64 // track heap usage
}

// ArbitrationContext represents the context & properties of the root pool
type ArbitrationContext struct {
	arbitrateHelper ArbitrateHelper
	cancelCh        <-chan struct{}
	hisMaxMemUsed   int64
	memQuotaLimit   int64
	memPriority     ArbitrationPriority
	stopped         atomic.Bool
	waitAverse      bool
	preferPrivilege bool
}

func (ctx *ArbitrationContext) available() bool {
	if ctx != nil && ctx.arbitrateHelper != nil && !ctx.stopped.Load() {
		return true
	}
	return false
}

func (ctx *ArbitrationContext) stop(stopByKill bool) {
	if ctx.stopped.Swap(true) {
		return
	}
	if stopByKill {
		ctx.arbitrateHelper.Kill()
	} else {
		ctx.arbitrateHelper.Cancel()
	}
}

// NewArbitrationContext creates a new arbitration context
func NewArbitrationContext(
	cancelCh <-chan struct{},
	hisMaxMemUsed, memQuotaLimit int64,
	arbitrateHelper ArbitrateHelper,
	memPriority ArbitrationPriority,
	waitAverse bool,
	preferPrivilege bool,
) *ArbitrationContext {
	return &ArbitrationContext{
		hisMaxMemUsed:   hisMaxMemUsed,
		memQuotaLimit:   memQuotaLimit,
		cancelCh:        cancelCh,
		arbitrateHelper: arbitrateHelper,
		memPriority:     memPriority,
		waitAverse:      waitAverse,
		preferPrivilege: preferPrivilege,
	}
}

type numByAllMode [maxArbitrateMode]int64

type waitingTaskNum numByAllMode

// TaskNumByMode returns the number of tasks by mode
func (m *MemArbitrator) TaskNumByMode() (res waitingTaskNum) {
	for i := minArbitrationPriority; i < maxArbitrationPriority; i++ {
		res[i] = m.taskNumByPriority(i)
	}
	res[ArbitrationWaitAverse] = m.taskNumOfWaitAverse()
	return
}

// ConsumeQuotaFromAwaitFreePool consumes quota from the await-free pool by the given uid
func (m *MemArbitrator) ConsumeQuotaFromAwaitFreePool(uid uint64, req int64) error {
	return m.GetAwaitFreeBudgets(uid).ConsumeQuota(m.UnixTimeSec, req)
}

// ConsumeQuota consumes quota from the concurrent budget
// req >= 0: alloc quota; try to pull from upstream;
// req < 0: release quota
func (b *ConcurrentBudget) ConsumeQuota(utimeSec int64, req int64) error {
	if req >= 0 {
		if b.LastUsedTimeSec != utimeSec {
			b.LastUsedTimeSec = utimeSec
		}
		if b.Used.Add(req) > b.Capacity.Load() {
			if _, err := b.PullFromUpstream(); err != nil {
				return err
			}
		}
	} else {
		b.Used.Add(req)
	}
	return nil
}

// ReportHeapInuseToAwaitFreePool reports the heap inuse to the await-free pool by the given uid
func (m *MemArbitrator) ReportHeapInuseToAwaitFreePool(uid uint64, req int64) {
	m.GetAwaitFreeBudgets(uid).ReportHeapInuse(req)
}

// ReportHeapInuse reports the heap inuse to the concurrent budget
// req > 0: consume
// req < 0: release
func (b *ConcurrentBudget) ReportHeapInuse(req int64) {
	b.HeapInuse.Add(req)
}
