// Copyright 2017 The tidb Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package mem

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type PoolEntryState int32

const (
	PoolEntryStateNormal PoolEntryState = iota
	PoolEntryStateStop
)

type ArbitrateResult int32

const (
	ArbitrateOk ArbitrateResult = iota
	ArbitrateFail
)

const (
	KB            int64 = 1 << 10
	MB                  = KB << 10
	GB                  = MB << 10
	BaseQuotaUnit       = 4 * KB // 4KB
	Kilo                = 1000
)

type SoftLimitMode int32

const (
	// SoftLimitModeDefault: same as the threshold of oom risk
	SoftLimitModeDefault SoftLimitMode = iota
	// SoftLimitModeSpecified: specified size or rate
	SoftLimitModeSpecified
	// SoftLimitModeAuto: auto calculate
	SoftLimitModeAuto
)

const ArbitratorSoftLimitModeAutoName = "AUTO"
const ArbitratorModeStandardName = "STANDARD"
const ArbitratorModePriorityName = "PRIORITY"

// `runtime.ReadMemStats` costs 33751*2.6 cycles
const DefTaskTickDur = time.Millisecond * 10
const DefMinHeapFreeSpeedBPS int64 = 100 * MB
const DefHeapReclaimCheckDuration = time.Second * 1
const DefHeapReclaimCheckMaxDuration = time.Second * 5
const DefCheckOOMRatio float64 = 0.95
const DefCheckSafeRatio float64 = 0.9
const DefTickDurMilli = Kilo * 1
const DefTrackMemStatsDurMilli = Kilo * 1
const DefMaxLimit int64 = 5e15
const DefMax int64 = 9e15
const DefServerlimitSmallLimitNum = 1000
const DefServerlimitMinUnitNum = 500
const DefServerlimitMaxUnitNum = 100
const DefUpdateMemConsumedTimeAlignSec = 30
const DefUpdateMemMagnifUtimeAlign = 30
const DefUpdateMemMagnifiUtimeAlignStandardMode = 1
const DefUpdateBufferTimeAlignSec = 60
const DefRedundancy = 2
const DefPoolReservedQuota int64 = 4 * MB
const DefFastAllocPoolAllocAlignSize int64 = DefPoolReservedQuota + MB
const DefFastAllocPoolShardNum int64 = 256
const DefFastAllocPoolholderShardNum int64 = 64
const DefFastAllocPoolShrinkDurMilli = Kilo * 5
const DefPoolStatusShards uint64 = 128
const DefPoolQuotaShards int = 27 // quota >= BaseQuotaUnit * 2^(max_shards - 2) will be put into the last shard
const prime64 uint64 = 1099511628211
const initHashKey = uint64(14695981039346656037)
const DefKillCancelCheckDuration = time.Second * 5
const DefKillCancelCheckTimeout = time.Second * 20
const DefDigestProfileSmallMemTimeoutSec int64 = 60 * 60 * 24 // 1 day
const DefDigestProfileMemTimeoutSec int64 = 60 * 60 * 24 * 7  // 1 week

type entryExecState int32

// mode of the arbitrator
// 3 modes: Standard, Priority, Disable
type ArbitratorWorkMode int32

// 3 priorities: Low, Medium, High
type ArbitrateMemPriority int32

const (
	execStateIdle entryExecState = iota
	execStateRunning
	execStatePrivileged
)

const (
	// priority mode: cancel by priority when quota is insufficient
	ArbitrateMemPriorityLow ArbitrateMemPriority = iota
	ArbitrateMemPriorityMedium
	ArbitrateMemPriorityHigh
)

const (
	minArbitrateMemPriority = ArbitrateMemPriorityLow
	maxArbitrateMemPriority = ArbitrateMemPriorityHigh + 1
	ArbitrateWaitAverse     = maxArbitrateMemPriority
	maxArbitrateMode        = maxArbitrateMemPriority + 1
)

const (
	ArbitratorModeStandard ArbitratorWorkMode = iota
	ArbitratorModePriority
	ArbitratorModeDisable
)

var ArbitrateMemPriorityNames = [maxArbitrateMemPriority]string{"LOW", "MEDIUM", "HIGH"}

func (p ArbitrateMemPriority) String() string {
	return ArbitrateMemPriorityNames[p]
}

var ArbitratorModeStandardNames = []string{ArbitratorModeStandardName, ArbitratorModePriorityName, "DISABLE"}

func (m ArbitratorWorkMode) String() string {
	return ArbitratorModeStandardNames[m]
}

func (m *MemArbitrator) taskNumByPriority(priority ArbitrateMemPriority) int64 {
	return m.tasks.fifoByPriority[priority].size()
}

func (m *MemArbitrator) taskNumOfWaitAverse() int64 {
	return m.tasks.fifoWaitAverse.size()
}

func (m *MemArbitrator) firstTaskEntry(priority ArbitrateMemPriority) *rootPoolEntry {
	return m.tasks.fifoByPriority[priority].front()
}

func (m *MemArbitrator) removeTaskImpl(entry *rootPoolEntry) bool {
	if entry.taskMu.fifo.valid() {
		m.tasks.fifoTasks.remove(entry.taskMu.fifo)
		entry.taskMu.fifo.reset()
		m.tasks.fifoByPriority[entry.taskMu.fifoByPriority.priority].remove(entry.taskMu.fifoByPriority.WrapListElement)
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
	entry.taskMu.fifoByPriority.WrapListElement = m.tasks.fifoByPriority[priority].pushBack(entry)
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
			for priority := maxArbitrateMemPriority - 1; priority >= minArbitrateMemPriority; priority-- {
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
	pool *ResourcePool

	// context of execution
	// mutable when entry is idle and the mutex of root pool is locked
	ctx struct {
		atomic.Pointer[Context]

		memPriority     ArbitrateMemPriority
		waitAverse      bool
		preferPrivilege bool

		cancelCh <-chan struct{}
	}

	stateMu struct {
		sync.Mutex
		state PoolEntryState

		// execStateIdle -> execStateRunning -> execStateFastAlloc
		// execStateIdle -> execStateRunning -> execStateIdle
		exec entryExecState

		pendingReset struct { // mutable once entry is idle
			sync.Mutex
			quota int64
		}
	}

	request struct {
		// mutable for ResourcePool
		quota int64
		// arbitrator is able to send result for windup
		resultCh chan ArbitrateResult
	}

	arbitratorMu struct { // mutable for arbitrator
		quota       int64 // -1: uninitiated
		destroyed   bool
		underKill   entryKillCancelCtx
		underCancel entryKillCancelCtx

		shard      *entryMapShard
		quotaShard *entryQuotaShard
	}

	taskMu struct { // protected by the tasks mutex of arbitrator
		fifo           WrapListElement
		fifoByPriority struct {
			WrapListElement
			priority ArbitrateMemPriority
		}
		fifoWaitAverse WrapListElement
	}
}

type mapUidEntry map[uint64]*rootPoolEntry

type entryMapShard struct {
	sync.RWMutex
	entries mapUidEntry
}

type entryQuotaShard struct {
	entries mapUidEntry
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
	shards     []*entryMapShard
	shardsMask uint64
	//
	quotaShards               [maxArbitrateMemPriority][]*entryQuotaShard
	maxQuotaShardIndex        int // for quota >= `BaseQuotaUnit * 2^(maxQuotaShard - 1)`
	minQuotaShardIndexToCheck int // ignore the pool with smaller quota

	// cache for traversing all entries concurrently
	contextCache struct { // mapUidEntry
		sync.Map
		num atomic.Int64
	}
}

// controlled by arbitrator
func (p *entryMap) delete(entry *rootPoolEntry) {
	uid := entry.pool.uid

	if entry.arbitratorMu.quotaShard != nil {
		delete(entry.arbitratorMu.quotaShard.entries, uid)
		entry.arbitratorMu.quota = 0
		entry.arbitratorMu.quotaShard = nil
	}

	entry.arbitratorMu.shard.delete(uid)
	entry.arbitratorMu.shard = nil

	if _, loaded := p.contextCache.LoadAndDelete(uid); loaded {
		p.contextCache.num.Add(-1)
	}
}

// controlled by arbitrator
func (p *entryMap) addQuota(entry *rootPoolEntry, delta int64) {
	if delta == 0 {
		return
	}

	uid := entry.pool.Uid()

	entry.arbitratorMu.quota += delta

	if entry.arbitratorMu.quota == 0 { // remove
		delete(entry.arbitratorMu.quotaShard.entries, uid)
		entry.arbitratorMu.quotaShard = nil
		return
	}

	newPos := getQuotaShard(entry.arbitratorMu.quota, p.maxQuotaShardIndex)
	newShard := p.quotaShards[entry.ctx.memPriority][newPos]
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

func (m *entryMap) getQuotaShard(priority ArbitrateMemPriority, quota int64) *entryQuotaShard {
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
	key := pool.Uid()
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
	for p := minArbitrateMemPriority; p < maxArbitrateMemPriority; p++ {
		m.quotaShards[p] = make([]*entryQuotaShard, m.maxQuotaShardIndex)
		for i := 0; i < m.maxQuotaShardIndex; i++ {
			m.quotaShards[p][i] = &entryQuotaShard{
				entries: make(mapUidEntry),
			}
		}
	}

	for i := uint64(0); i < shardNum; i++ {
		m.shards[i] = &entryMapShard{
			entries: make(mapUidEntry),
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

type PoolAllocProfile struct {
	SmallPoolLimit   int64 // limit / 1000
	PoolAllocUnit    int64 // limit / 500
	MaxPoolAllocUnit int64 // limit / 100
}

type holder64Bytes [64]byte

type MemArbitrator struct {
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
			mode SoftLimitMode
			size int64

			specified struct {
				size int64
				rate int64
			}
		}
	}

	execMu struct {
		sync.Mutex

		blockedState blockedState

		mode      ArbitratorWorkMode // work mode for each round
		startTime time.Time
	}

	controlMu struct {
		sync.Mutex
		running  bool
		finishCh chan struct{}
	}

	entryMap entryMap

	tasks struct {
		sync.Mutex

		fifoTasks      WrapList[*rootPoolEntry]
		fifoByPriority [maxArbitrateMemPriority]WrapList[*rootPoolEntry]
		fifoWaitAverse WrapList[*rootPoolEntry]

		pendingAlloc atomic.Int64
	}

	privilegedEntry *rootPoolEntry
	underKill       mapEntryWithMem
	underCancel     mapEntryWithMem

	rootPoolNum atomic.Int64

	heapController heapController

	avoidance struct {
		lastUpdateUtimeMilli atomic.Int64
		size                 atomic.Int64
		heapTracked          atomic.Int64
		//
		memMagnif struct {
			sync.Mutex
			ratio atomic.Int64
		}
	}

	tickTask struct {
		sync.Mutex
		lastTickUtimeMilli atomic.Int64
	}

	ableForRuntimeGC int32

	cleanupMu struct {
		sync.Mutex
		fifoTasks WrapList[*rootPoolEntry]
	}

	buffer buffer // only works under `ArbitratorModePriority`

	poolAllocStats struct {
		sync.RWMutex
		PoolAllocProfile

		mediumQuota atomic.Int64
		timedMap    [2 + DefRedundancy]struct {
			sync.RWMutex
			statisticsTimedMapElement
		}
		lastStoreUtimeMilli atomic.Int64
	}

	awaitFree struct {
		pool   *ResourcePool
		budget FixSizeBatchBudgets // use quota and track
		holder FixSizeBatchBudgets // hold quota but not use

		lastMemUsed          int64
		lastShrinkUtimeMilli atomic.Int64
	}

	digestProfileCache struct {
		shards     []digestProfileShard
		shardsMask uint64

		num atomic.Int64
	}

	UnixTimeSec int64

	mode ArbitratorWorkMode

	actions MemArbitratorActions

	notifer Notifer

	debug struct {
		now func() time.Time
	}

	execMetrics execMetrics
}

type buffer struct {
	size       atomic.Int64
	quotaLimit atomic.Int64
	timedMap   [2 + DefRedundancy]struct {
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

type MemArbitratorActions struct {
	Info, Warn, Error func(format string, args ...zap.Field)

	UpdateRuntimeMemStats, GC func()
}

type fastAllocPoolExecMetrics struct {
	success int64
	fail    int64
	shrink  int64
}

type pairSuccessFail struct{ success, fail int64 }

type numByPriority [maxArbitrateMemPriority]int64
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
	fastAlloc fastAllocPoolExecMetrics
	action    execMetricsAction
	execMetricsRisk
}

func (m *MemArbitrator) ExecMetrics() map[string]int64 {
	return map[string]int64{
		"task-success":                    m.execMetrics.task.success,
		"task-fail":                       m.execMetrics.task.fail,
		"task-success-prio-low":           m.execMetrics.task.successByPriority[ArbitrateMemPriorityLow],
		"task-success-prio-medium":        m.execMetrics.task.successByPriority[ArbitrateMemPriorityMedium],
		"task-success-prio-high":          m.execMetrics.task.successByPriority[ArbitrateMemPriorityHigh],
		"action-gc":                       m.execMetrics.action.gc,
		"action-update-runtime-mem-stats": m.execMetrics.action.updateRuntimeMemStats,
		"action-recordmemstate-success":   m.execMetrics.action.recordMemState.success,
		"action-recordmemstate-fail":      m.execMetrics.action.recordMemState.fail,
		"fastAlloc-success":               m.execMetrics.fastAlloc.success,
		"fastAlloc-fail":                  m.execMetrics.fastAlloc.fail,
		"fastAlloc-shrink":                m.execMetrics.fastAlloc.shrink,
		"memRisk":                         m.execMetrics.memRisk,
		"oomRisk":                         m.execMetrics.oomRisk,
		"oomKill-prio-low":                m.execMetrics.oomKill[ArbitrateMemPriorityLow],
		"oomKill-prio-medium":             m.execMetrics.oomKill[ArbitrateMemPriorityMedium],
		"oomKill-prio-high":               m.execMetrics.oomKill[ArbitrateMemPriorityHigh],
	}
}

func (m *MemArbitrator) SetWorkMode(newMode ArbitratorWorkMode) (oriMode ArbitratorWorkMode) {
	oriMode = ArbitratorWorkMode(atomic.SwapInt32((*int32)(&m.mode), int32(newMode)))
	m.wake()
	return
}

func (m *MemArbitrator) WorkMode() ArbitratorWorkMode {
	return m.workMode()
}

func (m *MemArbitrator) PoolAllocProfile() (res PoolAllocProfile) {
	limit := m.limit()
	return PoolAllocProfile{
		SmallPoolLimit:   max(1, limit/DefServerlimitSmallLimitNum),
		PoolAllocUnit:    max(1, limit/DefServerlimitMinUnitNum),
		MaxPoolAllocUnit: max(1, limit/DefServerlimitMaxUnitNum),
	}
}

func (m *MemArbitrator) workMode() ArbitratorWorkMode {
	return ArbitratorWorkMode(atomic.LoadInt32((*int32)(&m.mode)))
}

func (m *MemArbitrator) GetDigestProfileCache(digestID uint64, utimeSec int64) (int64, bool) {
	d := &m.digestProfileCache.shards[digestID&m.digestProfileCache.shardsMask]
	var pf *digestProfile
	if e, ok := d.Load(digestID); ok {
		pf = e.(*digestProfile)
	} else {
		return 0, false
	}

	if utimeSec > pf.lastFetchUtimeSec.Load() {
		pf.lastFetchUtimeSec.Store(utimeSec)
	}

	return pf.maxVal.Load(), true
}

func (m *MemArbitrator) shrinkDigestProfile(utimeSec int64, limit, shrinkToLimit int64) (shrinkedNum int64) {
	if m.digestProfileCache.num.Load() <= limit {
		return
	}

	var valMap [DefPoolQuotaShards]int

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
					if utimeSec-pf.lastFetchUtimeSec.Load() > DefDigestProfileMemTimeoutSec {
						needDelete = true
					}
				} else { // small max-val
					if utimeSec-pf.lastFetchUtimeSec.Load() > DefDigestProfileSmallMemTimeoutSec {
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
			index := getQuotaShard(maxVal, DefPoolQuotaShards)
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
		for i := 0; i < DefPoolQuotaShards; i++ {
			if n += int64(valMap[i]); n >= toShinkNum {
				shrinkMaxSize = BaseQuotaUnit * (1 << i)
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
	const maxDur = maxNum - DefRedundancy

	tsAlign := utimeSec / DefUpdateBufferTimeAlignSec
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
		for i := int64(0); i < maxDur; i++ {
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
	timedMap [2 + DefRedundancy]struct {
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
	slot    [DefServerlimitMinUnitNum]uint32
	num     atomic.Uint32
}

type entryKillCancelCtx struct {
	bool
	startTime time.Time
	reclaim   int64
	fail      bool
}

type mapEntryWithMem struct {
	entries mapUidEntry
	num     int64
}

func (x *mapEntryWithMem) delete(entry *rootPoolEntry) {
	delete(x.entries, entry.pool.uid)
	x.num--
}
func (x *mapEntryWithMem) init() {
	x.entries = make(mapUidEntry)
}

func (x *mapEntryWithMem) add(entry *rootPoolEntry) {
	x.entries[entry.pool.uid] = entry
	x.num++
}

func (m *MemArbitrator) addUnderKill(entry *rootPoolEntry, memoryUsed int64, startTime time.Time) {
	if !entry.arbitratorMu.underKill.bool {
		m.underKill.add(entry)
		entry.arbitratorMu.underKill = entryKillCancelCtx{
			bool:      true,
			startTime: startTime,
			reclaim:   memoryUsed,
		}
	}
}

func (m *MemArbitrator) addUnderCancel(entry *rootPoolEntry, memoryUsed int64, startTime time.Time) {
	if !entry.arbitratorMu.underCancel.bool {
		m.underCancel.add(entry)
		entry.arbitratorMu.underCancel = entryKillCancelCtx{
			bool:      true,
			startTime: startTime,
			reclaim:   memoryUsed,
		}
	}
}

func (m *MemArbitrator) deleteUnderKill(entry *rootPoolEntry) {
	if entry.arbitratorMu.underKill.bool {
		m.underKill.delete(entry)
		entry.arbitratorMu.underKill.bool = false

		m.warnKillCancel(entry, &entry.arbitratorMu.underKill, "Finish to `KILL` root pool")
	}
}

func (m *MemArbitrator) deleteUnderCancel(entry *rootPoolEntry) {
	if entry.arbitratorMu.underCancel.bool {
		m.underCancel.delete(entry)
		entry.arbitratorMu.underCancel.bool = false
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
	heapAlloc     atomic.Int64
	heapTotalFree atomic.Int64

	// `in use`` span + `stack` span == approx all memory
	heapInuse  atomic.Int64
	stackInuse atomic.Int64

	timedMemProfile [2]memProfile

	oomCheck struct {
		start        bool
		startTime    time.Time
		lastMemStats struct {
			startTime     time.Time
			heapTotalFree int64
		}
		minHeapFreeSpeedBPS atomic.Int64

		eachRound struct {
			gcExecuted bool
		}
	}

	memStateRecorder struct {
		RecordMemState
		sync.Mutex

		lastMemState         atomic.Pointer[RuntimeMemStateV1]
		lastRecordUtimeMilli atomic.Int64
		pendingStore         atomic.Bool
	}

	lastGC lastGCStats
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

type RecordMemState interface {
	Load() (*RuntimeMemStateV1, error)
	Store(*RuntimeMemStateV1) error
}

func (m *MemArbitrator) recordMemConsumed(memConsumed, utimeSec int64) {
	m.poolAllocStats.RLock()
	defer m.poolAllocStats.RUnlock()

	const maxNum = int64(len(m.poolAllocStats.timedMap))

	tsAlign := utimeSec / DefUpdateMemConsumedTimeAlignSec
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
		pos := min(memConsumed/m.poolAllocStats.PoolAllocUnit, DefServerlimitMinUnitNum-1)
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
	const maxDur = maxNum - DefRedundancy

	tsAlign := utimeSec / DefUpdateBufferTimeAlignSec
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
		for i := int64(0); i < maxDur; i++ {
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

func (m *MemArbitrator) SetMinHeapFreeSpeedBPS(sz int64) {
	m.heapController.oomCheck.minHeapFreeSpeedBPS.Store(sz)
}

func (m *MemArbitrator) MinHeapFreeSpeedBPS() int64 {
	return m.heapController.oomCheck.minHeapFreeSpeedBPS.Load()
}

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
	e, ok := m.entryMap.getStatusShard(uid).get(uid)
	if ok {
		return e
	} else {
		return nil
	}
}

type RootPoolEntry struct {
	Pool  *ResourcePool
	entry *rootPoolEntry
}

func (m *MemArbitrator) FindRootPool(uid uint64) RootPoolEntry {
	if e := m.getRootPoolEntry(uid); e != nil {
		return RootPoolEntry{e.pool, e}
	}
	return RootPoolEntry{nil, nil}
}

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
	if b := pool.cap(); b != 0 {
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

func (m *MemArbitrator) SoftLimit() int64 {
	return m.softLimit()
}

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
			m.mu.limit = int64(limit)
			m.mu.threshold.risk = int64(float64(limit) * DefCheckSafeRatio)
			m.mu.threshold.oomRisk = int64(float64(limit) * DefCheckOOMRatio)
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

func (m *MemArbitrator) lastBlockedAt() (int64, int64) {
	return atomic.LoadInt64(&m.execMu.blockedState.allocated), atomic.LoadInt64(&m.execMu.blockedState.utimeSec)
}

func (m *MemArbitrator) Allocated() int64 {
	return m.allocated()
}

func (m *MemArbitrator) PendingAllocSize() int64 {
	return m.tasks.pendingAlloc.Load()
}

func (m *MemArbitrator) TaskNum() int64 {
	return m.tasks.fifoTasks.size()
}

func (m *MemArbitrator) RootPoolNum() int64 {
	return m.rootPoolNum.Load()
}

func (m *MemArbitrator) limit() int64 {
	return atomic.LoadInt64(&m.mu.limit)
}

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
			remainBytes = 0
			ok = true
		} else if v := m.mu.limit - leastLeft - m.mu.allocated; v > 0 {
			m.mu.allocated += v
			reclaimedBytes += v
			remainBytes -= v
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
			if deadline := ctx.startTime.Add(DefKillCancelCheckTimeout); now.Compare(deadline) >= 0 {
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

	for prio := minArbitrateMemPriority; prio < target.ctx.memPriority; prio++ {
		for pos := m.entryMap.maxQuotaShardIndex - 1; pos >= m.entryMap.minQuotaShardIndexToCheck; pos-- {
			for _, entry := range m.entryMap.quotaShards[prio][pos].entries {
				if entry.arbitratorMu.underCancel.bool || entry.notRunning() {
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
		} else {
			if !m.tryRuntimeGC() {
				break
			}
		}
	}

	return false, reclaimedBytes
}

func NewMemArbitrator(
	limit int64,
	shardNum uint64, maxQuotaShardNum int,
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
	m.SetMinHeapFreeSpeedBPS(DefMinHeapFreeSpeedBPS)
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

func (m *MemArbitrator) doCancelPendingTasks(prio ArbitrateMemPriority, waitAverse bool) (cnt int64) {
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
		for prio := minArbitrateMemPriority; prio < maxArbitrateMemPriority; prio++ {
			if m.taskNumByPriority(prio) != 0 {
				m.execMetrics.cancel.standardMode += m.doCancelPendingTasks(prio, false)
			}
		}
	} else if m.taskNumOfWaitAverse() != 0 {
		m.execMetrics.cancel.waitAverse += m.doCancelPendingTasks(maxArbitrateMemPriority, true)
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
		if entry != nil {
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
		} else {
			break
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
			taskExecNum += 1
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

func (r *RootPoolEntry) StartWithMemArbitratorContext(m *MemArbitrator, ctx *Context) bool {
	return m.restartEntryByContext(r.entry, ctx)
}

func (m *MemArbitrator) restartEntryByContext(entry *rootPoolEntry, ctx *Context) bool {
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
			entry.ctx.memPriority = ArbitrateMemPriorityHigh
		} else {
			entry.ctx.preferPrivilege = ctx.preferPrivilege
			entry.ctx.memPriority = ctx.memPriority
		}

		entry.ctx.cancelCh = ctx.cancelCh
		entry.ctx.waitAverse = ctx.waitAverse
	} else {
		entry.ctx.cancelCh = nil
		entry.ctx.waitAverse = false
		entry.ctx.memPriority = ArbitrateMemPriority(ArbitrateMemPriorityMedium)
		entry.ctx.preferPrivilege = false
	}

	entry.ctx.Store(ctx)

	if _, loaded := m.entryMap.contextCache.LoadOrStore(entry.pool.uid, entry); !loaded {
		m.entryMap.contextCache.num.Add(1)
	}

	if entry.pool.actions.outOfCapacityAction == nil {
		entry.pool.actions.outOfCapacityAction = func(s OutOfCapacityActionArgs) error {
			if m.blockingAllocate(entry, s.request) != ArbitrateOk {
				return errArbitrateFailError
			}
			return nil
		}
	}
	entry.pool.mu.stopped = false

	entry.setExecState(execStateRunning)

	return true
}
