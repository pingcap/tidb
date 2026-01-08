// Copyright 2025 PingCAP, Inc.
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

	"github.com/pingcap/tidb/pkg/util/intest"
	"go.uber.org/zap"
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
	// SoftLimitModeDisable indicates that soft-limit is same as the threshold of oom risk
	SoftLimitModeDisable SoftLimitMode = iota
	// SoftLimitModeSpecified indicates that the soft-limit is a specified num of bytes or rate of the limit
	SoftLimitModeSpecified
	// SoftLimitModeAuto indicates that the soft-limit is auto calculated by the mem-arbitrator
	SoftLimitModeAuto
)

const (
	// ArbitratorSoftLimitModDisableName is the name of the soft limit mode default
	ArbitratorSoftLimitModDisableName = "0"
	// ArbitratorSoftLimitModeAutoName is the name of the soft limit mode auto
	ArbitratorSoftLimitModeAutoName = "auto"
	// ArbitratorModeStandardName is the name of the standard mode
	ArbitratorModeStandardName = "standard"
	// ArbitratorModePriorityName is the name of the priority mode
	ArbitratorModePriorityName = "priority"
	// ArbitratorModeDisableName is the name of the disable mode
	ArbitratorModeDisableName = "disable"
	// DefMaxLimit is the default maximum limit of mem quota
	DefMaxLimit int64 = 5e15

	defTaskTickDur                            = time.Millisecond * 10
	defMinHeapFreeBPS                  int64  = 100 * byteSizeMB
	defHeapReclaimCheckDuration               = time.Second * 1
	defHeapReclaimCheckMaxDuration            = time.Second * 5
	defOOMRiskRatio                           = 0.95
	defMemRiskRatio                           = 0.9
	defTickDurMilli                           = kilo * 1             // 1s
	defStorePoolMediumCapDurMilli             = defTickDurMilli * 10 // 10s
	defTrackMemStatsDurMilli                  = kilo * 1
	defMax                             int64  = 9e15
	defServerlimitSmallLimitNum               = 1000
	defServerlimitMinUnitNum                  = 500
	defServerlimitMaxUnitNum                  = 100
	defUpdateMemConsumedTimeAlignSec          = 30
	defUpdateMemMagnifUtimeAlign              = 30
	defUpdateBufferTimeAlignSec               = 60
	defRedundancy                             = 2
	defPoolReservedQuota                      = byteSizeMB
	defAwaitFreePoolAllocAlignSize            = defPoolReservedQuota + byteSizeMB
	defAwaitFreePoolShardNum           int64  = 256
	defAwaitFreePoolShrinkDurMilli            = kilo * 2
	defPoolStatusShards                       = 128
	defPoolQuotaShards                        = 27 // quota >= BaseQuotaUnit * 2^(max_shards - 2) will be put into the last shard
	prime64                            uint64 = 1099511628211
	initHashKey                        uint64 = 14695981039346656037
	defKillCancelCheckTimeout                 = time.Second * 20
	defDigestProfileSmallMemTimeoutSec        = 60 * 60 * 24     // 1 day
	defDigestProfileMemTimeoutSec             = 60 * 60 * 24 * 7 // 1 week
	baseQuotaUnit                             = 4 * byteSizeKB   // 4KB
	defMaxMagnif                              = kilo * 10
	defMaxDigestProfileCacheLimit             = 4e4
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

	maxArbitratorMode
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

var mockNow func() time.Time

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
	return m.tasks.fifoByPriority[priority].approxSize()
}

func (m *MemArbitrator) taskNumOfWaitAverse() int64 {
	return m.tasks.fifoWaitAverse.approxSize()
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

	res = m.removeTaskImpl(entry)

	m.tasks.Unlock()
	return res
}

func (m *MemArbitrator) addTask(entry *rootPoolEntry) {
	m.tasks.waitingAlloc.Add(entry.request.quota) // before tasks lock
	{
		m.tasks.Lock()

		priority := entry.ctx.memPriority
		entry.taskMu.fifoByPriority.priority = priority
		entry.taskMu.fifoByPriority.wrapListElement = m.tasks.fifoByPriority[priority].pushBack(entry)
		if entry.ctx.waitAverse {
			entry.taskMu.fifoWaitAverse = m.tasks.fifoWaitAverse.pushBack(entry)
		}
		entry.taskMu.fifo = m.tasks.fifoTasks.pushBack(entry)

		m.tasks.Unlock()
	}
}

func (m *MemArbitrator) frontTaskEntry() (entry *rootPoolEntry) {
	m.tasks.Lock()

	entry = m.tasks.fifoTasks.front()

	m.tasks.Unlock()
	return
}

func (m *MemArbitrator) extractFirstTaskEntry() (entry *rootPoolEntry) {
	m.tasks.Lock()

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
		cancelCh <-chan struct{}

		// properties hint of the entry; data race is acceptable;
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
		quotaToReclaim atomic.Int64
		sync.Mutex
		stop atomic.Bool

		// execStateIdle -> execStateRunning -> execStatePrivileged -> execStateIdle
		// execStateIdle -> execStateRunning -> execStateIdle
		exec entryExecState
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
	return e.stateMu.stop.Load() || e.execState() == execStateIdle || e.stateMu.quotaToReclaim.Load() > 0
}

type entryMap struct {
	quotaShards  [maxArbitrationPriority][]*entryQuotaShard // entries order by priority, quota
	contextCache struct {                                   // cache for traversing all entries concurrently
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

	delete(s.entries, key)

	s.Unlock()
}

func (s *entryMapShard) emplace(key uint64, tar *rootPoolEntry) (e *rootPoolEntry, ok bool) {
	s.Lock()

	e, found := s.entries[key]
	ok = !found
	if !found {
		s.entries[key] = tar
		e = tar
	}

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
	entry.request.quota = requestedBytes
	m.addTask(entry)
	m.notifer.WeakWake()
}

// non thread safe: the mutex of root pool must have been locked
func (m *MemArbitrator) waitAlloc(entry *rootPoolEntry) ArbitrateResult {
	res := ArbitrateOk
	select {
	case res = <-entry.request.resultCh:
		if res == ArbitrateFail {
			atomic.AddInt64(&m.execMetrics.Task.Fail, 1)
		} else {
			atomic.AddInt64(&m.execMetrics.Task.Succ, 1)
		}
	case <-entry.ctx.cancelCh:
		// 1. cancel by session
		// 2. stop by the arbitrate-helper (cancel / kill by arbitrator)
		res = ArbitrateFail
		atomic.AddInt64(&m.execMetrics.Task.Fail, 1)

		if !m.removeTask(entry) {
			<-entry.request.resultCh
		}
	}

	m.tasks.waitingAlloc.Add(-entry.request.quota)
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

// MemArbitrator represents the main structure aka `mem-arbitrator`
type MemArbitrator struct {
	execMu struct {
		startTime    time.Time          // start time of each round
		blockedState blockedState       // blocked state during arbitration
		mode         ArbitratorWorkMode // work mode of each round
	}
	actions   MemArbitratorActions // actions interfaces
	controlMu struct {             // control the async work process
		finishCh chan struct{}
		sync.Mutex
		running atomic.Bool
	}
	privilegedEntry *rootPoolEntry  // entry with privilege will always be satisfied first
	underKill       mapEntryWithMem // entries under `KILL` operation
	underCancel     mapEntryWithMem // entries under `CANCEL` operation
	notifer         Notifer         // wake up the async work process
	cleanupMu       struct {        // cleanup the state of the entry
		fifoTasks wrapList[*rootPoolEntry]
		sync.Mutex
	}
	tasks struct {
		fifoByPriority [maxArbitrationPriority]wrapList[*rootPoolEntry] // tasks by priority
		fifoTasks      wrapList[*rootPoolEntry]                         // all tasks in FIFO order
		fifoWaitAverse wrapList[*rootPoolEntry]                         // tasks with wait-averse property
		waitingAlloc   atomic.Int64                                     // total waiting allocation size
		sync.Mutex
	}
	digestProfileCache struct {
		shards     []digestProfileShard
		shardsMask uint64
		num        atomic.Int64
		limit      int64 // max number of digest profiles; shrink to limit/2 when num > limit;
	}
	entryMap  entryMap // sharded hash map & ordered quota map
	awaitFree struct { // await-free pool
		pool   *ResourcePool
		budget struct { // fixed size budget shards
			shards   []TrackedConcurrentBudget
			sizeMask uint64
		}
		lastQuotaUsage       memPoolQuotaUsage // tracked heap memory usage & quota usage
		lastShrinkUtimeMilli atomic.Int64
	}

	heapController heapController // monitor runtime mem stats; resolve mem issues; record mem profiles;

	poolAllocStats struct { // statistics of root pool allocation
		sync.RWMutex
		PoolAllocProfile
		mediumQuota atomic.Int64 // medium (max quota usage of root pool)
		timedMap    [2 + defRedundancy]struct {
			sync.RWMutex
			statisticsTimedMapElement
		}
		lastUpdateUtimeMilli atomic.Int64
	}

	buffer buffer // reserved buffer quota which only works under priority mode

	mu struct {
		sync.Mutex
		_         cpuCacheLinePad
		allocated int64  // allocated mem quota
		released  uint64 // total released mem quota
		lastGC    uint64 // total released mem quota at last GC point
		_         cpuCacheLinePad
		limit     int64 // hard limit of mem quota which is same as the server limit
		threshold struct {
			risk    int64 // threshold of mem risk
			oomRisk int64 // threshold of oom risk
		}
		softLimit struct {
			mode      SoftLimitMode
			size      int64
			specified struct {
				size  int64
				ratio int64 // ratio of soft-limit to hard-limit
			}
		}
	}
	execMetrics execMetricsCounter // execution metrics
	avoidance   struct {
		size        atomic.Int64 // size of quota cannot be allocated
		heapTracked struct {     // tracked heap memory usage
			atomic.Int64
			lastUpdateUtimeMilli atomic.Int64
		}
		memMagnif struct { // memory pressure magnification factor: ratio of runtime memory usage to quota usage
			sync.Mutex
			ratio atomic.Int64
		}
		awaitFreeBudgetKickOutIdx uint64 // round-robin index to clean await-free pool budget when quota is insufficient
	}
	tickTask struct { // periodic task
		sync.Mutex
		lastTickUtimeMilli atomic.Int64
	}
	UnixTimeSec int64 // approximate unix time in seconds
	rootPoolNum atomic.Int64
	mode        ArbitratorWorkMode
}

type buffer struct {
	size       atomic.Int64 // approximate max quota usage of root pool
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
	pairSuccessFail
	Shrink      int64
	ForceShrink int64
}

type pairSuccessFail struct{ Succ, Fail int64 }

// NumByPriority represents the number of tasks by priority
type NumByPriority [maxArbitrationPriority]int64

type execMetricsAction struct {
	GC                    int64
	UpdateRuntimeMemStats int64
	RecordMemState        pairSuccessFail
}

type execMetricsRisk struct {
	Mem     int64
	OOM     int64
	OOMKill NumByPriority
}

type execMetricsCancel struct {
	StandardMode int64
	PriorityMode NumByPriority
	WaitAverse   int64
}

type execMetricsTask struct {
	pairSuccessFail               // all work modes
	SuccByPriority  NumByPriority // priority mode
}

type execMetricsCounter struct {
	Task         execMetricsTask
	Cancel       execMetricsCancel
	AwaitFree    awaitFreePoolExecMetrics
	Action       execMetricsAction
	Risk         execMetricsRisk
	ShrinkDigest int64
}

// ExecMetrics returns the reference of the execution metrics
//
//go:norace
func (m *MemArbitrator) ExecMetrics() execMetricsCounter {
	if m == nil {
		return execMetricsCounter{}
	}
	return m.execMetrics
}

// SetWorkMode sets the work mode of the mem-arbitrator
func (m *MemArbitrator) SetWorkMode(newMode ArbitratorWorkMode) (oriMode ArbitratorWorkMode) {
	oriMode = ArbitratorWorkMode(atomic.SwapInt32((*int32)(&m.mode), int32(newMode)))
	m.wake()
	return
}

// WorkMode returns the current work mode of the mem-arbitrator
func (m *MemArbitrator) WorkMode() ArbitratorWorkMode {
	if m == nil {
		return ArbitratorModeDisable
	}
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

func (m *MemArbitrator) shrinkDigestProfile(utimeSec int64, limit, shrinkTo int64) (shrinkedNum int64) {
	if m.digestProfileCache.num.Load() <= limit {
		return
	}

	m.execMetrics.ShrinkDigest++

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

	toShinkNum := m.digestProfileCache.num.Load() - shrinkTo
	if toShinkNum <= 0 {
		return
	}

	shrinkMaxSize := DefMaxLimit
	{ // find the max size to shrink
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

		if oriTs = tar.tsAlign.Load(); oriTs < tsAlign && oriTs != 0 {
			tar.wrapTimeMaxval = wrapTimeMaxval{}
		}

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

		if ts := d.tsAlign.Load(); ts < (tsAlign+1) && ts != 0 {
			d.wrapTimeMaxval = wrapTimeMaxval{}
		}

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
	num     atomic.Uint64
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

//go:norace
func (x *mapEntryWithMem) approxSize() int64 {
	return x.num
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
	}
	memRisk struct {
		startTime struct {
			t         time.Time
			unixMilli atomic.Int64
		}
		lastMemStats struct {
			startTime     time.Time
			heapTotalFree int64
		}
		minHeapFreeBPS int64
		oomRisk        bool
	}
	timedMemProfile [2]memProfile
	lastGC          struct {
		heapAlloc atomic.Int64 // heap alloc size after GC
		utime     atomic.Int64 // end time of last GC
	}
	heapTotalFree atomic.Int64
	heapAlloc     atomic.Int64 // heap-alloc <= heap-inuse
	heapInuse     atomic.Int64
	memOffHeap    atomic.Int64 // off-heap memory: `stack` + `gc` + `other` + `meta` ...
	memInuse      atomic.Int64 // heap-inuse + off-heap: must be less than runtime-limit to avoid Heavy GC / OOM
	sync.Mutex
}

func (m *MemArbitrator) lastMemState() (res *RuntimeMemStateV1) {
	res = m.heapController.memStateRecorder.lastMemState.Load()
	return
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

		if oriTs = tar.tsAlign.Load(); oriTs < tsAlign && oriTs != 0 {
			tar.statisticsTimedMapElement = statisticsTimedMapElement{}
		}

		tar.Unlock()
	}

	cleanNext := false
	{
		tar.RLock()

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

		tar.RUnlock()
	}

	if cleanNext {
		d := &m.poolAllocStats.timedMap[(tsAlign+1)%maxNum]
		d.Lock()

		if v := d.tsAlign.Load(); v < (tsAlign+1) && v != 0 {
			d.statisticsTimedMapElement = statisticsTimedMapElement{}
		}

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

		if oriTs = tar.ts.Load(); oriTs < tsAlign && oriTs != 0 {
			tar.wrapTimeSizeQuota = wrapTimeSizeQuota{}
		}

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

		if v := d.ts.Load(); v < tsAlign+1 && v != 0 {
			d.wrapTimeSizeQuota = wrapTimeSizeQuota{}
		}

		d.Unlock()
	}
}

func (m *MemArbitrator) gc() {
	m.mu.lastGC = m.mu.released
	if m.actions.GC != nil {
		m.actions.GC()
	}
	atomic.AddInt64(&m.execMetrics.Action.GC, 1)
}

func (m *MemArbitrator) reclaimHeap() {
	m.gc()
	m.refreshRuntimeMemStats() // refresh runtime mem stats after GC and record
}

func (m *MemArbitrator) setMinHeapFreeBPS(sz int64) {
	m.heapController.memRisk.minHeapFreeBPS = sz
}

func (m *MemArbitrator) minHeapFreeBPS() int64 {
	return m.heapController.memRisk.minHeapFreeBPS
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
			m.approxUnixTimeSec())

		if maxMemConsumed > m.poolAllocStats.SmallPoolLimit {
			m.recordMemConsumed(
				maxMemConsumed,
				m.approxUnixTimeSec())
		}
	}

	m.resetRootPoolEntry(entry)
	m.wake()
}

func (m *MemArbitrator) resetRootPoolEntry(entry *rootPoolEntry) bool {
	{
		entry.stateMu.Lock()

		if entry.execState() == execStateIdle {
			entry.stateMu.Unlock()
			return false
		}
		entry.setExecState(execStateIdle)

		entry.stateMu.Unlock()
	}

	// aquiure the lock of root pool:
	// - wait for the alloc task to finish
	// - publish the state of entry
	if releasedSize := entry.pool.Stop(); releasedSize > 0 {
		entry.stateMu.quotaToReclaim.Add(releasedSize)
	}

	{
		m.cleanupMu.Lock()

		m.cleanupMu.fifoTasks.pushBack(entry)

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
		if ctx := entry.ctx.Load(); ctx != nil && ctx.arbitrateHelper != nil {
			ctx.arbitrateHelper.Finish()
		}
		m.wake()
		return true
	}
	return false
}

func (m *MemArbitrator) removeRootPoolEntry(entry *rootPoolEntry) bool {
	{
		entry.stateMu.Lock()

		if entry.stateMu.stop.Swap(true) {
			entry.stateMu.Unlock()
			return false
		}

		if entry.execState() != execStateIdle {
			entry.setExecState(execStateIdle)
		}

		entry.stateMu.Unlock()
	}

	// make the alloc task failed in arbitrator;
	{
		m.cleanupMu.Lock()

		m.cleanupMu.fifoTasks.pushBack(entry)

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

type rootPool struct {
	entry      *rootPoolEntry
	arbitrator *MemArbitrator
}

// FindRootPool finds the root pool by ID
func (m *MemArbitrator) FindRootPool(uid uint64) rootPool {
	if e := m.getRootPoolEntry(uid); e != nil {
		return rootPool{e, m}
	}
	return rootPool{}
}

// EmplaceRootPool emplaces a new root pool with the given uid (uid < 0 means the internal pool)
func (m *MemArbitrator) EmplaceRootPool(uid uint64) (rootPool, error) {
	if e := m.getRootPoolEntry(uid); e != nil {
		return rootPool{e, m}, nil
	}

	pool := &ResourcePool{
		name:           fmt.Sprintf("root-%d", uid),
		uid:            uid,
		limit:          DefMaxLimit,
		allocAlignSize: 1,
	}
	entry, err := m.addRootPool(pool)
	return rootPool{entry, m}, err
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

func (m *MemArbitrator) doAdjustSoftLimit() {
	var softLimit int64
	limit := m.limit()
	if m.mu.softLimit.mode == SoftLimitModeSpecified {
		if m.mu.softLimit.specified.size > 0 {
			softLimit = min(m.mu.softLimit.specified.size, limit)
		} else {
			softLimit = min(multiRatio(limit, m.mu.softLimit.specified.ratio), limit)
		}
	} else {
		softLimit = m.oomRisk()
	}
	m.mu.softLimit.size = softLimit
}

// SetSoftLimit sets the soft limit of the mem-arbitrator
func (m *MemArbitrator) SetSoftLimit(softLimit int64, sortLimitRatio float64, mode SoftLimitMode) {
	m.mu.Lock()

	m.mu.softLimit.mode = mode
	if mode == SoftLimitModeSpecified {
		m.mu.softLimit.specified.size = softLimit
		m.mu.softLimit.specified.ratio = intoRatio(sortLimitRatio)
	}
	m.doAdjustSoftLimit()

	m.mu.Unlock()
}

//go:norace
func (m *MemArbitrator) softLimit() int64 {
	return m.mu.softLimit.size
}

// SoftLimit returns the soft limit of the mem-arbitrator
func (m *MemArbitrator) SoftLimit() uint64 {
	if m == nil {
		return 0
	}
	return uint64(m.softLimit())
}

func (m *MemArbitrator) doSetLimit(limit int64) {
	m.mu.limit = limit
	m.mu.threshold.oomRisk = int64(float64(limit) * defOOMRiskRatio)
	m.mu.threshold.risk = int64(float64(limit) * defMemRiskRatio)
	m.doAdjustSoftLimit()
}

// SetLimit sets the limit of the mem-arbitrator and returns whether the limit has changed
func (m *MemArbitrator) SetLimit(x uint64) (changed bool) {
	newLimit := min(int64(x), DefMaxLimit)
	if newLimit <= 0 {
		return
	}

	needWake := false
	{
		m.mu.Lock()

		if limit := m.limit(); newLimit != limit {
			changed = true
			needWake = newLimit > limit // update to a greater limit
			m.doSetLimit(newLimit)
		}

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

	m.poolAllocStats.PoolAllocProfile = m.PoolAllocProfile()
	for i := range m.poolAllocStats.timedMap {
		m.poolAllocStats.timedMap[i].statisticsTimedMapElement = statisticsTimedMapElement{}
	}

	m.poolAllocStats.Unlock()
}

func (m *MemArbitrator) alloc(x int64) {
	m.mu.Lock()

	m.doAlloc(x)

	m.mu.Unlock()
}

func (m *MemArbitrator) doAlloc(x int64) {
	m.mu.allocated += x
}

func (m *MemArbitrator) release(x int64) {
	if x <= 0 {
		return
	}
	m.alloc(-x)
}

//go:norace
func (m *MemArbitrator) allocated() int64 {
	return m.mu.allocated
}

func (m *MemArbitrator) lastBlockedAt() (allocated, utimeSec int64) {
	return m.execMu.blockedState.allocated, m.execMu.blockedState.utimeSec
}

//go:norace
func (b *blockedState) reset() {
	*b = blockedState{}
}

//go:norace
func (m *MemArbitrator) updateBlockedAt() {
	m.execMu.blockedState = blockedState{m.allocated(), m.approxUnixTimeSec()}
}

// Allocated returns the allocated mem quota of the mem-arbitrator
func (m *MemArbitrator) Allocated() int64 {
	return m.allocated()
}

// OutOfControl returns the size of the out-of-control mem
func (m *MemArbitrator) OutOfControl() int64 {
	return m.avoidance.size.Load()
}

// WaitingAllocSize returns the pending alloc mem quota of the mem-arbitrator
func (m *MemArbitrator) WaitingAllocSize() int64 {
	return m.tasks.waitingAlloc.Load()
}

// TaskNum returns the number of pending tasks in the mem-arbitrator
func (m *MemArbitrator) TaskNum() int64 {
	return m.tasks.fifoTasks.approxSize()
}

// RootPoolNum returns the number of root pools in the mem-arbitrator
func (m *MemArbitrator) RootPoolNum() int64 {
	return m.rootPoolNum.Load()
}

//go:norace
func (m *MemArbitrator) limit() int64 {
	return m.mu.limit
}

//go:norace
func (m *MemArbitrator) available() int64 {
	return min(m.heapAvailable(), m.quotaAvailable())
}

func (m *MemArbitrator) heapAvailable() int64 {
	return m.limit() - m.reservedBuffer() - m.heapController.heapAlloc.Load()
}

func (m *MemArbitrator) quotaAvailable() int64 {
	return m.limit() - m.reservedBuffer() - m.OutOfControl() - m.allocated()
}

// Limit returns the mem quota limit of the mem-arbitrator
func (m *MemArbitrator) Limit() uint64 {
	if m == nil {
		return 0
	}
	return uint64(m.limit())
}

func (m *MemArbitrator) allocateFromArbitrator(remainBytes int64) (bool, int64) {
	reclaimedBytes := int64(0)
	ok := false
	{
		m.mu.Lock()

		available := m.quotaAvailable()

		if remainBytes <= available {
			m.doAlloc(remainBytes)
			reclaimedBytes += remainBytes
			ok = true
		} else if available > 0 {
			m.doAlloc(available)
			reclaimedBytes += available
		}

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
				if ctx := entry.ctx.Load(); ctx.available() {
					m.execMetrics.Cancel.PriorityMode[prio]++
					ctx.stop(ArbitratorPriorityCancel)

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

	m.alloc(remainBytes)

	return ok, remainBytes
}

func (m *MemArbitrator) ableToGC() bool {
	return m.mu.released-m.mu.lastGC >= uint64(m.poolAllocStats.SmallPoolLimit)
}

func (m *MemArbitrator) tryRuntimeGC() bool {
	if m.ableToGC() {
		m.updateTrackedHeapStats()
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

	onlyPrivilegedBudget := false
	for remainBytes > m.heapAvailable() {
		if !m.tryRuntimeGC() {
			onlyPrivilegedBudget = true // only could alloc from the privileged budget
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
		} else if onlyPrivilegedBudget {
			return false, reclaimedBytes
		}
	}

	for {
		ok, reclaimed := m.allocateFromArbitrator(remainBytes)
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
func NewMemArbitrator(limit int64, shardNum uint64, maxQuotaShardNum int, minQuotaForReclaim int64, recorder RecordMemState) *MemArbitrator {
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
	m.doSetLimit(limit)
	m.resetStatistics()
	m.setMinHeapFreeBPS(defMinHeapFreeBPS)
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

		m.heapController.memStateRecorder.RecordMemState = recorder
		if s, err := recorder.Load(); err == nil && s != nil {
			m.heapController.memStateRecorder.lastMemState.Store(s)
			m.doSetMemMagnif(s.Magnif)
			m.poolAllocStats.mediumQuota.Store(s.PoolMediumCap)
		}

		m.heapController.memStateRecorder.Unlock()
	}
	m.resetDigestProfileCache(shardNum)
	return m
}

func (m *MemArbitrator) resetDigestProfileCache(shardNum uint64) {
	m.digestProfileCache.shards = make([]digestProfileShard, shardNum)
	m.digestProfileCache.shardsMask = shardNum - 1
	m.digestProfileCache.num.Store(0)
	m.digestProfileCache.limit = defMaxDigestProfileCacheLimit
}

// SetDigestProfileCacheLimit sets the limit of the digest profile cache
func (m *MemArbitrator) SetDigestProfileCacheLimit(limit int64) {
	m.digestProfileCache.limit = min(max(0, limit), defMax)
}

func (m *MemArbitrator) doCancelPendingTasks(prio ArbitrationPriority, waitAverse bool) (cnt int64) {
	var entries [64]*rootPoolEntry

	fifo := &m.tasks.fifoWaitAverse
	reason := ArbitratorWaitAverseCancel
	if !waitAverse {
		fifo = &m.tasks.fifoByPriority[prio]
		reason = ArbitratorStandardCancel
	}

	for {
		size := 0
		{
			m.tasks.Lock()

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

			m.tasks.Unlock()
		}

		for i := range size {
			entry := entries[i]
			if ctx := entry.ctx.Load(); ctx.available() {
				ctx.stop(reason)
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
	if m.tasks.fifoTasks.approxEmpty() {
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
					m.execMetrics.Task.SuccByPriority[entry.taskMu.fifoByPriority.priority]++
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
			m.updateBlockedAt()
			m.doReclaimByWorkMode(entry, reclaimedBytes)
		}
	}

	return
}

func (m *MemArbitrator) doReclaimNonBlockingTasks() {
	if m.execMu.mode == ArbitratorModeStandard {
		for prio := minArbitrationPriority; prio < maxArbitrationPriority; prio++ {
			if m.taskNumByPriority(prio) != 0 {
				m.execMetrics.Cancel.StandardMode += m.doCancelPendingTasks(prio, false)
			}
		}
	} else if m.taskNumOfWaitAverse() != 0 {
		m.execMetrics.Cancel.WaitAverse += m.doCancelPendingTasks(maxArbitrationPriority, true)
	}
}

func (m *MemArbitrator) doReclaimByWorkMode(entry *rootPoolEntry, reclaimedBytes int64) {
	waitAverse := entry.ctx.waitAverse
	m.doReclaimNonBlockingTasks()
	// entry's ctx may have been modified
	if waitAverse {
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

			entry = m.cleanupMu.fifoTasks.popFront()

			m.cleanupMu.Unlock()
		}
		if entry == nil {
			break
		}

		if m.privilegedEntry == entry {
			m.privilegedEntry = nil
		}
		m.deleteUnderCancel(entry)
		m.deleteUnderKill(entry)

		if !entry.stateMu.stop.Load() { // reset pool entry
			toRelease := entry.stateMu.quotaToReclaim.Swap(0)
			if toRelease > 0 {
				m.release(toRelease)
				atomic.AddUint64(&m.mu.released, uint64(toRelease))
				m.entryMap.addQuota(entry, -toRelease)
			}
		} else {
			if !entry.arbitratorMu.destroyed {
				if entry.arbitratorMu.quota > 0 {
					m.release(entry.arbitratorMu.quota)
					atomic.AddUint64(&m.mu.released, uint64(entry.arbitratorMu.quota))
				}
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
	if m.tasks.fifoTasks.approxEmpty() {
		return
	}

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
			m.alloc(entry.request.quota)
			m.entryMap.addQuota(entry, entry.request.quota)
			entry.windUp(entry.request.quota, ArbitrateOk)
		}
	}
}

// -1: at ArbitratorModeDisable
// -2: mem unsafe
// >= 0: execute / cancel task num
func (m *MemArbitrator) runOneRound() (taskExecNum int) {
	m.execMu.startTime = now()
	if t := m.execMu.startTime.Unix(); t != m.approxUnixTimeSec() { // update per second duration and reduce force sharing
		m.setUnixTimeSec(t)
	}

	if mode := m.workMode(); m.execMu.mode != mode {
		m.execMu.mode = mode
		if mode == ArbitratorModeDisable { // switch to disable mode
			m.setMemSafe()
			m.execMu.blockedState.reset()
		}
	}

	if !m.cleanupMu.fifoTasks.approxEmpty() {
		m.doExecuteCleanupTasks()
	}

	if m.execMu.mode == ArbitratorModeDisable {
		m.implicitRun()
		return -1
	}

	if !m.handleMemIssues() { // mem is still unsafe
		return -2
	}

	for m.doExecuteFirstTask() {
		taskExecNum++
	}

	return taskExecNum
}

func (m *MemArbitrator) asyncRun(duration time.Duration) bool {
	if m.controlMu.running.Load() {
		return false
	}
	m.controlMu.running.Store(true)
	m.controlMu.finishCh = make(chan struct{})

	go func() {
		ticker := time.NewTicker(duration)
		for m.controlMu.running.Load() {
			select {
			case <-ticker.C:
				m.weakWake()
			case <-m.notifer.C:
				m.notifer.clear()
				m.runOneRound()
			}
		}

		ticker.Stop()
		close(m.controlMu.finishCh)
	}()
	return true
}

// Restart starts the root pool with the given context
func (r *rootPool) Restart(ctx *ArbitrationContext) bool {
	return r.arbitrator.restartEntryByContext(r.entry, ctx)
}

// Pool returns the internal resource pool
func (r *rootPool) Pool() *ResourcePool {
	return r.entry.pool
}

func (m *MemArbitrator) restartEntryByContext(entry *rootPoolEntry, ctx *ArbitrationContext) bool {
	if entry == nil {
		return false
	}
	entry.stateMu.Lock()
	defer entry.stateMu.Unlock()

	if entry.stateMu.stop.Load() || entry.execState() != execStateIdle {
		return false
	}

	entry.pool.mu.Lock()
	defer entry.pool.mu.Unlock()

	if ctx != nil {
		if ctx.PrevMaxMem > m.buffer.size.Load() {
			m.setBufferSize(ctx.PrevMaxMem)
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

	entry.pool.actions.OutOfCapacityActionCB = func(s OutOfCapacityActionArgs) error {
		if m.blockingAllocate(entry, s.Request) != ArbitrateOk {
			return errArbitrateFailError
		}
		return nil
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
	Pool            *ResourcePool
	Capacity        int64
	LastUsedTimeSec int64
	sync.Mutex
	_    cpuCacheLinePad
	Used atomic.Int64
	_    cpuCacheLinePad
}

//go:norace
func (b *ConcurrentBudget) setLastUsedTimeSec(t int64) {
	b.LastUsedTimeSec = t
}

//go:norace
func (b *ConcurrentBudget) approxCapacity() int64 {
	return b.Capacity
}

func (b *ConcurrentBudget) getLastUsedTimeSec() int64 {
	return b.LastUsedTimeSec
}

// TrackedConcurrentBudget consists of ConcurrentBudget and heap inuse
type TrackedConcurrentBudget struct {
	ConcurrentBudget
	HeapInuse atomic.Int64
	_         cpuCacheLinePad
}

// Stop stops the concurrent budget and releases all the capacity and make the pool non-allocatable
func (b *ConcurrentBudget) Stop() int64 {
	b.Lock()
	defer b.Unlock()

	b.Pool.SetOutOfCapacityAction(func(OutOfCapacityActionArgs) error {
		return errArbitrateFailError
	})

	budgetCap := b.Capacity
	b.Capacity = 0
	b.Used.Store(0)
	if budgetCap > 0 {
		b.Pool.release(budgetCap)
	}

	return budgetCap
}

// Reserve reserves a given capacity for the concurrent budget
func (b *ConcurrentBudget) Reserve(newCap int64) (err error) {
	b.Lock()

	extra := max(newCap, b.Used.Load(), b.Capacity) - b.Capacity
	if err = b.Pool.allocate(extra); err == nil {
		b.Capacity += extra
	}

	b.Unlock()
	return
}

// PullFromUpstream tries to pull from the upstream pool when facing `out of capacity`
// It requires the action of the pool to be non-blocking
func (b *ConcurrentBudget) PullFromUpstream() (err error) {
	b.Lock()

	delta := b.Used.Load() - b.Capacity
	if delta > 0 {
		delta = b.Pool.roundSize(delta)
		if err = b.Pool.allocate(delta); err == nil {
			b.Capacity += delta
		}
	}

	b.Unlock()
	return
}

// AutoRun starts the work groutine of the mem-arbitrator asynchronously
func (m *MemArbitrator) AutoRun(
	actions MemArbitratorActions,
	awaitFreePoolAllocAlignSize, awaitFreePoolShardNum int64,
	taskTickDur time.Duration,
) bool {
	m.controlMu.Lock()
	defer m.controlMu.Unlock()

	if m.controlMu.running.Load() {
		return false
	}

	{ // init
		m.actions = actions
		m.refreshRuntimeMemStats()
		m.initAwaitFreePool(awaitFreePoolAllocAlignSize, awaitFreePoolShardNum)
	}
	return m.asyncRun(taskTickDur)
}

func (m *MemArbitrator) refreshRuntimeMemStats() {
	if m.actions.UpdateRuntimeMemStats != nil {
		m.actions.UpdateRuntimeMemStats() // should invoke `SetRuntimeMemStats`
	}
	atomic.AddInt64(&m.execMetrics.Action.UpdateRuntimeMemStats, 1)
}

// RuntimeMemStats represents the runtime memory statistics
type RuntimeMemStats struct {
	HeapAlloc, HeapInuse, TotalFree, MemOffHeap, LastGC int64
}

func (m *MemArbitrator) trySetRuntimeMemStats(s RuntimeMemStats) bool {
	if m.heapController.TryLock() {
		m.doSetRuntimeMemStats(s)
		m.heapController.Unlock()
		return true
	}
	return false
}

// SetRuntimeMemStats sets the runtime memory statistics. It may be invoked by `refreshRuntimeMemStats` -> `actions.UpdateRuntimeMemStats`
func (m *MemArbitrator) SetRuntimeMemStats(s RuntimeMemStats) {
	m.heapController.Lock()
	m.doSetRuntimeMemStats(s)
	m.heapController.Unlock()
}

func (m *MemArbitrator) doSetRuntimeMemStats(s RuntimeMemStats) {
	m.heapController.heapAlloc.Store(s.HeapAlloc)
	m.heapController.heapInuse.Store(s.HeapInuse)
	m.heapController.heapTotalFree.Store(s.TotalFree)
	m.heapController.memOffHeap.Store(s.MemOffHeap)
	m.heapController.memInuse.Store(s.MemOffHeap + s.HeapInuse)

	if s.LastGC > m.heapController.lastGC.utime.Load() {
		m.heapController.lastGC.heapAlloc.Store(s.HeapAlloc)
		m.heapController.lastGC.utime.Store(s.LastGC)
	}

	m.updateAvoidSize() // calc out-of-control & avoid size
}

func (m *MemArbitrator) updateAvoidSize() {
	capacity := m.softLimit()
	if m.mu.softLimit.mode == SoftLimitModeAuto {
		if ratio := m.memMagnif(); ratio != 0 {
			newCap := calcRatio(m.limit(), ratio)
			capacity = min(capacity, newCap)
		}
	}
	avoidSize := max(
		0,
		m.heapController.heapAlloc.Load()+m.heapController.memOffHeap.Load()-m.avoidance.heapTracked.Load(), // out-of-control size
		m.limit()-capacity,
	)
	m.avoidance.size.Store(avoidSize)

	if delta := m.allocated() - m.limit() + avoidSize; delta > 0 && m.awaitFree.pool.allocated() > 0 {
		reclaimed := int64(0)
		poolReleased := int64(0)
		for i := range len(m.awaitFree.budget.shards) {
			idx := (m.avoidance.awaitFreeBudgetKickOutIdx + uint64(i) + 1) & m.awaitFree.budget.sizeMask
			b := &m.awaitFree.budget.shards[idx]
			if b.approxCapacity() > 0 && b.TryLock() {
				x := min(delta-reclaimed, b.Capacity)
				b.Capacity -= x
				reclaimed += x
				b.Unlock()
			}

			if reclaimed >= delta {
				m.avoidance.awaitFreeBudgetKickOutIdx = idx
				break
			}
		}
		if reclaimed > 0 {
			m.awaitFree.pool.mu.Lock()

			m.awaitFree.pool.doRelease(reclaimed) // release even if `reclaimed` is 0
			poolReleased = m.awaitFree.pool.mu.budget.release()

			m.awaitFree.pool.mu.Unlock()
		}
		if poolReleased > 0 {
			m.release(poolReleased)
			atomic.AddInt64(&m.execMetrics.AwaitFree.ForceShrink, 1)
			atomic.AddUint64(&m.mu.released, uint64(poolReleased))
		}
	}
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

		tsAlign := utimeMilli / kilo / defUpdateMemConsumedTimeAlignSec
		tar1 := &s.timedMap[(maxNum+tsAlign-1)%maxNum]
		tar2 := &s.timedMap[tsAlign%maxNum]

		if ts := tar1.tsAlign.Load(); ts <= tsAlign-maxDur || ts > tsAlign {
			tar1 = nil
		}
		if ts := tar2.tsAlign.Load(); ts <= tsAlign-maxDur || ts > tsAlign {
			tar2 = nil
		}

		total := uint64(0)
		if tar1 != nil {
			tar1.RLock()
			total += tar1.num.Load()
		}
		if tar2 != nil {
			tar2.RLock()
			total += tar2.num.Load()
		}

		if total != 0 {
			expect := max(1, (total+1)/2)
			cnt := uint64(0)
			index := 0

			for i := range defServerlimitMinUnitNum {
				if tar1 != nil {
					cnt += uint64(tar1.slot[i])
				}
				if tar2 != nil {
					cnt += uint64(tar2.slot[i])
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

		s.RUnlock()
	}

	m.tryStorePoolMediumCapacity(utimeMilli, m.poolMediumQuota())
}

func (m *MemArbitrator) tryStorePoolMediumCapacity(utimeMilli int64, capacity int64) bool {
	if capacity == 0 {
		return false
	}
	if lastState := m.lastMemState(); lastState == nil ||
		(m.poolAllocStats.lastUpdateUtimeMilli.Load()+defStorePoolMediumCapDurMilli <= utimeMilli &&
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
		m.poolAllocStats.lastUpdateUtimeMilli.Store(utimeMilli)
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
				tar.heap >= m.oomRisk() { // calculate the magnification only when the heap is safe
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
		if ut := m.heapController.lastGC.utime.Load(); curTsAlign == ut/1e9/defUpdateMemMagnifUtimeAlign {
			cur.heap = max(cur.heap, m.heapController.lastGC.heapAlloc.Load())
		}
		if blockedSize, utimeSec := m.lastBlockedAt(); utimeSec/defUpdateMemMagnifUtimeAlign == curTsAlign {
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

//go:norace
func (m *MemArbitrator) awaitFreePoolCap() int64 {
	if m.awaitFree.pool == nil {
		return 0
	}
	return m.awaitFree.pool.capacity()
}

type memPoolQuotaUsage struct{ trackedHeap, quota int64 }

//go:norace
func (m *MemArbitrator) awaitFreePoolUsed() (res memPoolQuotaUsage) {
	for i := range m.awaitFree.budget.shards {
		if d := m.awaitFree.budget.shards[i].Used.Load(); d > 0 {
			res.quota += d
		}
		if d := m.awaitFree.budget.shards[i].HeapInuse.Load(); d > 0 {
			res.trackedHeap += d
		}
	}
	m.awaitFree.lastQuotaUsage = res
	return
}

//go:norace
func (m *MemArbitrator) approxAwaitFreePoolUsed() memPoolQuotaUsage {
	return m.awaitFree.lastQuotaUsage
}

func (m *MemArbitrator) executeTick(utimeMilli int64) bool { // exec batch tasks every 1s
	if m.atMemRisk() { // skip if oom check is running because mem state is not safe
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
			zap.Int64("last-blocked-heap", pre.heap), zap.Int64("last-blocked-quota", pre.quota),
			zap.Int64("last-magnification-ratio(‰)", pre.ratio),
			zap.Time("last-prof-start-time", time.UnixMilli(pre.startUtimeMilli)),
		)
		m.actions.Info("Mem profile timeline",
			profile.fields[:profile.n]...,
		)
	}
	// suggest pool cap
	m.updatePoolMediumCapacity(utimeMilli)
	// shrink mem profile cache
	m.shrinkDigestProfile(utimeMilli/kilo, m.digestProfileCache.limit, m.digestProfileCache.limit/2)
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
	taskNumByMode := m.TaskNumByPattern()
	memMagnif := m.memMagnif()
	if memMagnif == 0 {
		memMagnif = -1
	}
	f.append(
		zap.Int64("heap-inuse", m.heapController.heapInuse.Load()),
		zap.Int64("heap-alloc", m.heapController.heapAlloc.Load()),
		zap.Int64("mem-off-heap", m.heapController.memOffHeap.Load()),
		zap.Int64("mem-inuse", m.heapController.memInuse.Load()),
		zap.Int64("hard-limit", m.limit()),
		zap.Int64("quota-allocated", m.allocated()),
		zap.Int64("quota-softlimit", m.softLimit()),
		zap.Int64("mem-magnification-ratio(‰)", memMagnif),
		zap.Int64("root-pool-num", m.RootPoolNum()),
		zap.Int64("awaitfree-pool-cap", m.awaitFreePoolCap()),
		zap.Int64("awaitfree-pool-used", m.approxAwaitFreePoolUsed().quota),
		zap.Int64("awaitfree-pool-heapinuse", m.approxAwaitFreePoolUsed().trackedHeap),
		zap.Int64("tracked-heapinuse", m.avoidance.heapTracked.Load()),
		zap.Int64("out-of-control", m.OutOfControl()),
		zap.Int64("buffer", m.buffer.size.Load()),
		zap.Int64("task-num", m.TaskNum()),
		zap.Int64("task-priority-low", taskNumByMode[ArbitrationPriorityLow]),
		zap.Int64("task-priority-medium", taskNumByMode[ArbitrationPriorityMedium]),
		zap.Int64("task-priority-high", taskNumByMode[ArbitrationPriorityHigh]),
		zap.Int64("pending-alloc-size", m.WaitingAllocSize()),
		zap.Int64("digest-cache-num", m.digestProfileCache.num.Load()),
	)
	if t := m.heapController.memRisk.startTime.unixMilli.Load(); t != 0 {
		f.append(zap.Time("mem-risk-start", time.UnixMilli(t)))
	}
	return
}

// HandleRuntimeStats handles the runtime memory statistics
func (m *MemArbitrator) HandleRuntimeStats(s RuntimeMemStats) {
	// shrink fast alloc pool
	m.tryShrinkAwaitFreePool(defPoolReservedQuota, nowUnixMilli())
	// update tracked mem stats
	m.tryUpdateTrackedMemStats(nowUnixMilli())
	// set runtime mem stats & update avoidance size
	m.trySetRuntimeMemStats(s)
	m.executeTick(nowUnixMilli())
	m.weakWake()
}

func (m *MemArbitrator) tryUpdateTrackedMemStats(utimeMilli int64) bool {
	if m.avoidance.heapTracked.lastUpdateUtimeMilli.Load()+defTrackMemStatsDurMilli <= utimeMilli {
		m.updateTrackedHeapStats()
		return true
	}
	return false
}

func (m *MemArbitrator) updateTrackedHeapStats() {
	totalTrackedHeap := int64(0)
	if m.entryMap.contextCache.num.Load() != 0 {
		maxMemUsed := int64(0)
		m.entryMap.contextCache.Range(func(_, value any) bool {
			e := value.(*rootPoolEntry)
			if e.notRunning() {
				return true
			}
			if ctx := e.ctx.Load(); ctx.available() {
				if memUsed := ctx.arbitrateHelper.HeapInuse(); memUsed > 0 {
					totalTrackedHeap += memUsed
					maxMemUsed = max(maxMemUsed, memUsed)
				}
			}
			return true
		})
		if m.buffer.size.Load() < maxMemUsed {
			m.tryToUpdateBuffer(maxMemUsed, 0, m.approxUnixTimeSec())
		}
	}

	totalTrackedHeap += m.awaitFreePoolUsed().trackedHeap
	m.avoidance.heapTracked.Store(totalTrackedHeap)
	m.avoidance.heapTracked.lastUpdateUtimeMilli.Store(nowUnixMilli())
}

func (m *MemArbitrator) tryShrinkAwaitFreePool(minRemain int64, utimeMilli int64) bool {
	if m.awaitFree.lastShrinkUtimeMilli.Load()+defAwaitFreePoolShrinkDurMilli <= utimeMilli {
		m.shrinkAwaitFreePool(minRemain, utimeMilli)
		return true
	}
	return false
}

func (m *MemArbitrator) shrinkAwaitFreePool(minRemain int64, utimeMilli int64) {
	if m.awaitFree.pool.allocated() <= 0 {
		return
	}

	poolReleased := int64(0)
	reclaimed := int64(0)

	for i := range m.awaitFree.budget.shards {
		b := &m.awaitFree.budget.shards[i]

		if used := b.Used.Load(); used > 0 {
			if b.approxCapacity()-(used+minRemain) >= b.Pool.allocAlignSize && b.TryLock() {
				if used = b.Used.Load(); used > 0 {
					toReclaim := b.Capacity - (used + minRemain)

					if toReclaim >= b.Pool.allocAlignSize {
						b.Capacity -= toReclaim
						reclaimed += toReclaim
					}
				}
				b.Unlock()
			}
		} else {
			if b.approxCapacity() > 0 && b.getLastUsedTimeSec()*kilo+defAwaitFreePoolShrinkDurMilli <= utimeMilli && b.TryLock() {
				if toReclaim := b.Capacity; b.Used.Load() <= 0 && toReclaim > 0 {
					b.Capacity -= toReclaim
					reclaimed += toReclaim
				}
				b.Unlock()
			}
		}
	}

	if reclaimed > 0 {
		m.awaitFree.pool.mu.Lock()

		m.awaitFree.pool.doRelease(reclaimed)
		poolReleased = m.awaitFree.pool.mu.budget.release()

		m.awaitFree.pool.mu.Unlock()
	}
	if poolReleased > 0 {
		m.release(poolReleased)
		atomic.AddUint64(&m.mu.released, uint64(poolReleased))
		atomic.AddInt64(&m.execMetrics.AwaitFree.Shrink, 1)
		m.weakWake()
	}

	m.awaitFree.lastShrinkUtimeMilli.Store(utimeMilli)
}

func (m *MemArbitrator) isMemSafe() bool {
	return m.heapController.memInuse.Load() < m.oomRisk()
}

func (m *MemArbitrator) isMemNoRisk() bool {
	return m.heapController.memInuse.Load() < m.memRisk()
}

func (m *MemArbitrator) calcMemRisk() *RuntimeMemStateV1 {
	if m.mu.softLimit.mode != SoftLimitModeAuto {
		return nil
	}

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
	if m.atMemRisk() {
		gcExecuted := m.tryRuntimeGC()
		if !gcExecuted {
			m.refreshRuntimeMemStats()
		}

		if m.isMemNoRisk() {
			m.updateTrackedHeapStats()
			m.updateAvoidSize() // no need to refresh runtime mem stats

			{ // warning
				profile := m.recordDebugProfile()
				m.actions.Info("Memory is safe", profile.fields[:profile.n]...)
			}
			m.setMemSafe()
			return true
		}

		m.doReclaimNonBlockingTasks()
		m.handleMemRisk(gcExecuted)
		return false
	} else if !m.isMemSafe() {
		m.doReclaimNonBlockingTasks()
		m.intoMemRisk()
		return false
	}
	return true
}

func (*MemArbitrator) innerTime() time.Time {
	if intest.InTest {
		if mockNow != nil {
			return mockNow()
		}
	}
	return now()
}

func (m *MemArbitrator) handleMemRisk(gcExecuted bool) {
	now := m.innerTime()
	oomRisk := m.heapController.memInuse.Load() > m.limit()
	dur := now.Sub(m.heapController.memRisk.lastMemStats.startTime)
	if !oomRisk && dur < defHeapReclaimCheckDuration {
		return
	}
	heapUseBPS := int64(0)

	if dur > 0 {
		heapFrees := m.heapController.heapTotalFree.Load() - m.heapController.memRisk.lastMemStats.heapTotalFree
		heapUseBPS = int64(float64(heapFrees) / dur.Seconds())
	}
	if oomRisk || memHangRisk(heapUseBPS, m.minHeapFreeBPS(), now, m.heapController.memRisk.startTime.t) {
		m.intoOOMRisk()

		memToReclaim := m.heapController.memInuse.Load() - m.memRisk()

		{ // warning
			profile := m.recordDebugProfile()
			profile.append(
				zap.Float64("heap-use-speed(MiB/s)", float64(heapUseBPS*100/byteSizeMB)/100),
				zap.Float64("required-speed(MiB/s)", float64(m.minHeapFreeBPS())/float64(byteSizeMB)),
				zap.Int64("quota-to-reclaim", max(0, memToReclaim)),
			)
			m.actions.Warn("`OOM RISK`: try to `KILL` running root pool", profile.fields[:profile.n]...)
		}

		if newKillNum, reclaiming := m.killTopnEntry(memToReclaim); newKillNum != 0 {
			m.heapController.memRisk.startTime.t = m.innerTime() // restart oom check
			m.heapController.memRisk.startTime.unixMilli.Store(m.heapController.memRisk.startTime.t.UnixMilli())

			{ // warning
				profile := m.recordDebugProfile()
				profile.append(
					zap.Int64("pool-under-kill-num", m.underKill.num),
					zap.Int("new-kill-num", newKillNum),
					zap.Int64("quota-under-reclaim", reclaiming),
					zap.Int64("rest-quota-to-reclaim", max(0, memToReclaim-reclaiming)),
				)
				m.actions.Warn("Restart runtime memory check", profile.fields[:profile.n]...)
			}
		} else {
			underKillNum := 0
			for _, entry := range m.underKill.entries {
				if !entry.arbitratorMu.underKill.fail {
					underKillNum++
				}
			}
			if underKillNum == 0 {
				forceKill := 0
				for { // make all tasks success
					entry := m.frontTaskEntry()
					if entry == nil {
						break
					}
					// force kill
					if ctx := entry.ctx.Load(); ctx.available() {
						ctx.stop(ArbitratorOOMRiskKill)
						m.execMetrics.Risk.OOMKill[entry.ctx.memPriority]++
						forceKill++
						if m.removeTask(entry) {
							entry.windUp(0, ArbitrateFail)
						}
					}
				}
				if forceKill != 0 {
					profile := m.recordDebugProfile()
					profile.append(
						zap.Int("kill-awaiting-num", forceKill),
						zap.Int64("pool-under-kill-num", m.underKill.num),
						zap.Int64("quota-under-reclaim", reclaiming),
						zap.Int64("rest-quota-to-reclaim", max(0, memToReclaim-reclaiming)),
					)
					m.actions.Warn("No more running root pool can be killed to resolve `OOM RISK`; KILL all awaiting tasks;",
						profile.fields[:profile.n]...,
					)
				} else {
					profile := m.recordDebugProfile()
					profile.append(
						zap.Int64("pool-under-kill-num", m.underKill.num),
						zap.Int64("quota-under-reclaim", reclaiming),
						zap.Int64("rest-quota-to-reclaim", max(0, memToReclaim-reclaiming)),
					)
					m.actions.Warn("No more running root pool or awaiting task can be terminated to resolve `OOM RISK`",
						profile.fields[:profile.n]...,
					)
				}
			}
		}
	} else {
		{ // warning
			profile := m.recordDebugProfile()
			profile.append(zap.Float64("heap-use-speed(MiB/s)",
				float64(heapUseBPS*100/byteSizeMB)/100),
				zap.Float64("required-speed(MiB/s)", float64(m.minHeapFreeBPS())/float64(byteSizeMB)))
			m.actions.Warn("Runtime memory free speed meets require, start re-check", profile.fields[:profile.n]...)
		}
	}

	if dur >= defHeapReclaimCheckDuration {
		m.heapController.memRisk.lastMemStats.heapTotalFree = m.heapController.heapTotalFree.Load()
		m.heapController.memRisk.lastMemStats.startTime = m.innerTime()
	}

	if !gcExecuted {
		m.gc()
	}
}

func memHangRisk(freeSpeedBPS, minHeapFreeSpeedBPS int64, now, startTime time.Time) bool {
	return freeSpeedBPS < minHeapFreeSpeedBPS || now.Sub(startTime) > defHeapReclaimCheckMaxDuration
}

func (m *MemArbitrator) killTopnEntry(required int64) (newKillNum int, reclaimed int64) {
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
					ctx.stop(ArbitratorOOMRiskKill)
					newKillNum++
					m.execMetrics.Risk.OOMKill[prio]++

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
		m.execMetrics.Action.RecordMemState.Fail++
		return err
	}
	m.execMetrics.Action.RecordMemState.Succ++
	m.actions.Info("Record mem state",
		zap.String("reason", reason),
		zap.String("data", fmt.Sprintf("%+v", s)),
	)
	return nil
}

// GetAwaitFreeBudgets returns the concurrent budget shard by the given uid
func (m *MemArbitrator) GetAwaitFreeBudgets(uid uint64) *TrackedConcurrentBudget {
	index := shardIndexByUID(uid, m.awaitFree.budget.sizeMask)
	return &m.awaitFree.budget.shards[index]
}

func (m *MemArbitrator) initAwaitFreePool(allocAlignSize, shardNum int64) {
	if allocAlignSize <= 0 {
		allocAlignSize = defAwaitFreePoolAllocAlignSize
	}

	p := &ResourcePool{
		name:           "awaitfree-pool",
		uid:            0,
		limit:          DefMaxLimit,
		allocAlignSize: allocAlignSize,

		maxUnusedBlocks: 0,
	}

	p.SetOutOfCapacityAction(func(s OutOfCapacityActionArgs) error {
		if m.heapController.heapAlloc.Load() > m.oomRisk()-s.Request ||
			m.allocated() > m.limit()-m.OutOfControl()-s.Request {
			m.updateBlockedAt()
			m.execMetrics.AwaitFree.Fail++
			return errArbitrateFailError
		}

		m.alloc(s.Request)
		p.forceAddCap(s.Request)
		m.execMetrics.AwaitFree.Succ++

		return nil
	})

	m.awaitFree.pool = p

	{
		cnt := nextPow2(uint64(shardNum))
		m.awaitFree.budget.shards = make([]TrackedConcurrentBudget, cnt)
		m.awaitFree.budget.sizeMask = cnt - 1
		for i := range m.awaitFree.budget.shards {
			m.awaitFree.budget.shards[i].Pool = p
		}
	}
}

// ArbitratorStopReason represents the reason why the arbitrate helper will be stopped
type ArbitratorStopReason int

// ArbitrateHelperReason values
const (
	ArbitratorOOMRiskKill ArbitratorStopReason = iota
	ArbitratorWaitAverseCancel
	ArbitratorStandardCancel
	ArbitratorPriorityCancel
)

// String returns the string representation of the ArbitratorStopReason
func (r ArbitratorStopReason) String() (desc string) {
	switch r {
	case ArbitratorOOMRiskKill:
		desc = "KILL(out-of-memory)"
	case ArbitratorWaitAverseCancel:
		desc = "CANCEL(out-of-quota & wait-averse)"
	case ArbitratorStandardCancel:
		desc = "CANCEL(out-of-quota & standard-mode)"
	case ArbitratorPriorityCancel:
		desc = "CANCEL(out-of-quota & priority-mode)"
	}
	return
}

// ArbitrateHelper is an interface for the arbitrate helper
type ArbitrateHelper interface {
	Stop(ArbitratorStopReason) bool // kill by arbitrator only when meeting oom risk; cancel by arbitrator;
	HeapInuse() int64               // track heap usage
	Finish()
}

// ArbitrationContext represents the context & properties of the root pool which is accessible for the global mem-arbitrator
type ArbitrationContext struct {
	arbitrateHelper ArbitrateHelper
	cancelCh        <-chan struct{}
	PrevMaxMem      int64
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

func (ctx *ArbitrationContext) stop(reason ArbitratorStopReason) {
	if ctx.stopped.Swap(true) {
		return
	}
	ctx.arbitrateHelper.Stop(reason)
}

// NewArbitrationContext creates a new arbitration context
func NewArbitrationContext(
	cancelCh <-chan struct{},
	prevMaxMem, memQuotaLimit int64,
	arbitrateHelper ArbitrateHelper,
	memPriority ArbitrationPriority,
	waitAverse bool,
	preferPrivilege bool,
) *ArbitrationContext {
	return &ArbitrationContext{
		PrevMaxMem:      prevMaxMem,
		memQuotaLimit:   memQuotaLimit,
		cancelCh:        cancelCh,
		arbitrateHelper: arbitrateHelper,
		memPriority:     memPriority,
		waitAverse:      waitAverse,
		preferPrivilege: preferPrivilege,
	}
}

// NumByPattern represents the number of tasks by 4 pattern: priority(low, medium, high), wait-averse
type NumByPattern [maxArbitrateMode]int64

// TaskNumByPattern returns the number of tasks by pattern and there may be overlap
func (m *MemArbitrator) TaskNumByPattern() (res NumByPattern) {
	for i := minArbitrationPriority; i < maxArbitrationPriority; i++ {
		res[i] = m.taskNumByPriority(i)
	}
	res[ArbitrationWaitAverse] = m.taskNumOfWaitAverse()
	return
}

// ConsumeQuotaFromAwaitFreePool consumes quota from the awaitfree-pool by the given uid
func (m *MemArbitrator) ConsumeQuotaFromAwaitFreePool(uid uint64, req int64) bool {
	return m.GetAwaitFreeBudgets(uid).ConsumeQuota(m.approxUnixTimeSec(), req) == nil
}

// ConsumeQuota consumes quota from the concurrent budget
// req > 0: alloc quota; try to pull from upstream;
// req <= 0: release quota
func (b *ConcurrentBudget) ConsumeQuota(utimeSec int64, req int64) error {
	if req > 0 {
		if b.getLastUsedTimeSec() != utimeSec {
			b.setLastUsedTimeSec(utimeSec)
		}
		if b.Used.Add(req) > b.approxCapacity() {
			if err := b.PullFromUpstream(); err != nil {
				return err
			}
		}
	} else {
		b.Used.Add(req)
	}
	return nil
}

// ReportHeapInuseToAwaitFreePool reports the heap inuse to the awaitfree-pool by the given uid
func (m *MemArbitrator) ReportHeapInuseToAwaitFreePool(uid uint64, req int64) {
	m.GetAwaitFreeBudgets(uid).ReportHeapInuse(req)
}

// ReportHeapInuse reports the heap inuse to the concurrent budget
// req > 0: consume
// req < 0: release
func (b *TrackedConcurrentBudget) ReportHeapInuse(req int64) {
	b.HeapInuse.Add(req)
}

func (m *MemArbitrator) stop() bool {
	m.controlMu.Lock()
	defer m.controlMu.Unlock()

	if !m.controlMu.running.Load() {
		return false
	}

	m.controlMu.running.Store(false)
	m.wake()

	<-m.controlMu.finishCh

	m.runOneRound()

	return true
}

// AtMemRisk checks if the memory is under risk
func (m *MemArbitrator) AtMemRisk() bool {
	return m.atMemRisk()
}

// AtOOMRisk checks if the memory is under risk
//
//go:norace
func (m *MemArbitrator) AtOOMRisk() bool {
	return m.heapController.memRisk.oomRisk
}

func (m *MemArbitrator) atMemRisk() bool {
	return m.heapController.memRisk.startTime.unixMilli.Load() != 0
}

func (m *MemArbitrator) intoOOMRisk() {
	m.heapController.memRisk.oomRisk = true
	m.execMetrics.Risk.OOM++
}

//go:norace
func (m *MemArbitrator) oomRisk() int64 {
	return m.mu.threshold.oomRisk
}

//go:norace
func (m *MemArbitrator) memRisk() int64 {
	return m.mu.threshold.risk
}

func (m *MemArbitrator) intoMemRisk() {
	now := m.innerTime()
	m.heapController.memRisk.startTime.t = now
	m.heapController.memRisk.startTime.unixMilli.Store(now.UnixMilli())
	m.heapController.memRisk.lastMemStats.heapTotalFree = m.heapController.heapTotalFree.Load()
	m.heapController.memRisk.lastMemStats.startTime = now
	m.execMetrics.Risk.Mem++

	{
		profile := m.recordDebugProfile()
		profile.append(zap.Int64("threshold", m.mu.threshold.oomRisk))
		m.actions.Warn("Memory inuse reach threshold", profile.fields[:profile.n]...)
	}

	{ // GC
		m.reclaimHeap()
	}

	if memState := m.calcMemRisk(); memState != nil {
		if memState.Magnif > defMaxMagnif {
			// There may be extreme memory leak issues. It's recommended to set soft limit manually.
			m.actions.Warn("Memory pressure is abnormally high",
				zap.Int64("mem-magnification-ratio(‰)", memState.Magnif),
				zap.Int64("upper-limit-ratio(‰)", defMaxMagnif))
			memState.Magnif = defMaxMagnif
		}
		{
			m.avoidance.memMagnif.Lock()

			m.doSetMemMagnif(memState.Magnif)

			m.avoidance.memMagnif.Unlock()
		}

		if err := m.recordMemState(memState, "oom risk"); err != nil {
			m.actions.Error("Failed to save mem-risk", zap.Error(err))
		}
	}

	if m.isMemNoRisk() {
		m.wake()
	}
}

func (m *MemArbitrator) setMemSafe() {
	m.heapController.memRisk.startTime.unixMilli.Store(0)
	m.heapController.memRisk.oomRisk = false
}

//go:norace
func (m *MemArbitrator) setUnixTimeSec(s int64) {
	m.UnixTimeSec = s
}

func (m *MemArbitrator) approxUnixTimeSec() int64 {
	return m.UnixTimeSec
}
