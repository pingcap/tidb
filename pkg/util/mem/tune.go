package mem

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

var errArbitrateFailError = errors.New("failed to allocate resouce from arbitrator")

type DebugFieldsIterm [30]zap.Field
type DebugFields struct {
	fields DebugFieldsIterm
	n      int
}

type ConcurrentBudget struct {
	sync.Mutex
	Used     atomic.Int64
	Capacity atomic.Int64
	pool     *ResourcePool

	LastUsedTimeSec int64
}

type FixSizeBatchBudgets struct {
	shards   []ConcurrentBudget
	sizeMask uint64
}

func (s *FixSizeBatchBudgets) used() int64 {
	res := int64(0)
	for i := range s.shards {
		d := s.shards[i].Used.Load()
		if d > 0 {
			res += d
		}
	}
	return res
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
			if b.Capacity.Load() > 0 && b.LastUsedTimeSec*Kilo+DefAwaitFreePoolShrinkDurMilli <= utimeMilli && b.TryLock() {
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

func (b *ConcurrentBudget) InitUpperPool(pool *ResourcePool) {
	b.pool = pool
}

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

func (b *ConcurrentBudget) Reserve(newCap int64) (err error, duration time.Duration) {
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

func (b *ConcurrentBudget) PullFromUpstream() (err error, duration time.Duration) {
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

func (b *ConcurrentBudget) PullFromUpstreamV2(maxSize int64) (err error, duration time.Duration) {
	b.Lock()
	startTime := time.Now()
	//
	used, cap := b.Used.Add(0), b.Capacity.Load()
	if diff := used - cap; diff > 0 {
		extra := max(min(max(((used*2850)>>10)-cap, b.pool.allocAlignSize), maxSize), b.pool.roundSize(diff))
		if err = b.pool.allocate(extra); err == nil {
			b.Capacity.Add(extra)
		}
	}
	//
	duration = time.Since(startTime)
	b.Unlock()
	return
}

func (m *MemArbitrator) AutoRun(
	actions MemArbitratorActions,
) bool {
	return m.autoRun(
		actions,
		DefAwaitFreePoolAllocAlignSize,
		DefAwaitFreePoolShardNum,
		DefAwaitFreePoolholderShardNum,
		DefTaskTickDur,
	)
}

func (m *MemArbitrator) autoRun(
	actions MemArbitratorActions,
	awaitFreePoolAllocAlignSize, awaitFreePoolShardNum, awaitFreePoolholderShardNum int64,
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
		m.initAwaitFreePool(awaitFreePoolAllocAlignSize, awaitFreePoolShardNum, awaitFreePoolholderShardNum)
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

// invoked by `refreshRuntimeMemStats -> action.UpdateRuntimeMemStats`
// involed by `HandleRuntime`
func (m *MemArbitrator) SetRuntimeMemStats(s *runtime.MemStats) {
	m.setMemStats(int64(s.Alloc), int64(s.HeapInuse), int64(s.TotalAlloc), int64(s.StackInuse))
}

func (m *MemArbitrator) setMemStats(alloc, heapInuse, totalAlloc, stackInuse int64) {
	m.heapController.heapAlloc.Store(alloc)
	m.heapController.heapInuse.Store(heapInuse) // heapInuse >= alloc
	m.heapController.heapTotalFree.Store(totalAlloc - alloc)
	m.heapController.stackInuse.Store(stackInuse)

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
	const maxDur = maxNum - DefRedundancy

	{
		s.RLock()
		//

		tsAlign := utimeMilli / Kilo / DefUpdateMemConsumedTimeAlignSec
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

			for i := 0; i < DefServerlimitMinUnitNum; i++ {
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

func (m *MemArbitrator) tryStorePoolMediumCapacity(utimeMilli int64, cap int64) bool {
	if lastState := m.lastMemState(); lastState == nil ||
		(m.poolAllocStats.lastStoreUtimeMilli.Load()+DefTickDurMilli*10 /* 10x */ <= utimeMilli &&
			lastState.PoolMediumCap != cap) {
		var memState *RuntimeMemStateV1

		if lastState != nil {
			s := *lastState // copy
			s.PoolMediumCap = cap
			memState = &s
		} else {
			memState = &RuntimeMemStateV1{
				Version:       1,
				PoolMediumCap: cap,
			}
		}

		m.recordMemState(memState, "new root pool medium cap")
		m.poolAllocStats.lastStoreUtimeMilli.Store(utimeMilli)
		return true
	}
	return false
}

func (m *MemArbitrator) poolMediumQuota() int64 {
	return m.poolAllocStats.mediumQuota.Load()
}

func (m *MemArbitrator) PoolMediumCapacity() int64 {
	return m.poolMediumQuota()
}

func (m *MemArbitrator) updateMemMagnification(utimeMilli int64) (updatedPreProf *memProfile) {
	const maxNum = int64(len(m.heapController.timedMemProfile))

	curTsAlign := utimeMilli / Kilo / DefUpdateMemMagnifUtimeAlign
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

			if tar.ratio > 0 {
				v = max(v, tar.ratio)
			} else {
				break // if any record is not valid,
			}
		}

		updated := false
		var oriRatio, newRatio int64

		if v != 0 && m.avoidance.memMagnif.TryLock() {
			if oriRatio = m.memMagnif(); oriRatio != 0 && v < oriRatio-10 /* 1 percent */ {
				newRatio = (oriRatio + v) / 2
				if newRatio <= Kilo {
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
				m.recordMemState(&memState, "new magnification ratio")
			}
		}

		*cur = memProfile{
			tsAlign:         curTsAlign,
			startUtimeMilli: utimeMilli,
		}
	}

	if cur.tsAlign == curTsAlign {
		if ts := m.heapController.lastGC.endUtimeSec.Load(); curTsAlign == ts/DefUpdateMemMagnifUtimeAlign {
			cur.heap = max(cur.heap, m.heapController.lastGC.heapAlloc.Load())
		}
		if blockedSize, utimeSec := m.lastBlockedAt(); blockedSize > 0 &&
			utimeSec/DefUpdateMemMagnifUtimeAlign == curTsAlign {
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
	return m.awaitFree.pool.cap()
}

func (m *MemArbitrator) awaitFreePoolUsed() int64 {
	m.awaitFree.lastMemUsed = m.awaitFree.budget.used()
	return m.awaitFree.lastMemUsed
}

func (m *MemArbitrator) executeTick(utimeMilli int64) bool { // exec batch tasks every 1s
	if m.heapController.oomCheck.start { // skip if oom check is running because mem state is not safe
		return false
	}

	if m.tickTask.lastTickUtimeMilli.Load()+DefTickDurMilli > utimeMilli {
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
	m.shrinkDigestProfile(utimeMilli/Kilo, 2e6, 1e6)

	return true
}

func (d *DebugFields) append(f ...zap.Field) {
	n := min(len(f), len(d.fields)-d.n)
	for i := 0; i < n; i++ {
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
		zap.Int64("fast-alloc-cap", m.awaitFreePoolCap()),
		zap.Int64("fast-alloc-used", m.awaitFree.lastMemUsed),
		zap.Int64("heap-tracked", m.avoidance.heapTracked.Load()),
		zap.Int64("out-of-control", m.avoidance.size.Load()),
		zap.Int64("reserved-buffer", m.buffer.size.Load()),
		zap.Int64("alloc-task-num", m.TaskNum()),
		zap.Int64("pending-alloc-size", m.PendingAllocSize()),
		zap.Int64("task-priority-low", taskNumByMode[ArbitrateMemPriorityLow]),
		zap.Int64("task-priority-medium", taskNumByMode[ArbitrateMemPriorityMedium]),
		zap.Int64("task-priority-high", taskNumByMode[ArbitrateMemPriorityHigh]),
		zap.Int64("digest-profile-num", m.digestProfileCache.num.Load()),
	)
	if m.heapController.oomCheck.start {
		f.append(zap.Time("oom-check-start", m.heapController.oomCheck.startTime))
	}
	return
}

func (m *MemArbitrator) HandleRuntime(s *runtime.MemStats) {
	// shrink fast alloc pool
	m.tryShrinkAwaitFreePool(DefPoolReservedQuota, MB, nowUnixMilli())
	// update tracked mem stats
	m.tryUpdateTrackedMemStats(nowUnixMilli())
	// set runtime mem stats & update avoidance size
	m.SetRuntimeMemStats(s)
	m.executeTick(nowUnixMilli())
	m.weakWake()
}

func (m *MemArbitrator) tryUpdateTrackedMemStats(utimeMilli int64) bool {
	if m.avoidance.lastUpdateUtimeMilli.Load()+DefTrackMemStatsDurMilli <= utimeMilli {
		m.updateTrackedMemStats()
		return true
	}
	return false
}

func (m *MemArbitrator) updateTrackedMemStats() {
	totalMemUsed := int64(0)
	if m.entryMap.contextCache.num.Load() != 0 {
		maxMemUsed := int64(0)
		m.entryMap.contextCache.Range(func(_, value any) bool {
			e := value.(*rootPoolEntry)
			if e.notRunning() {
				return true
			}
			if ctx := e.ctx.Load(); ctx.available() {
				memUsed := ctx.arbitrateHelper.HeapInuse()
				totalMemUsed += memUsed
				maxMemUsed = max(maxMemUsed, memUsed)
			}
			return true
		})
		if m.buffer.size.Load() < maxMemUsed {
			m.tryToUpdateBuffer(maxMemUsed, 0, m.UnixTimeSec)
		}
	}

	awaitFreeUsed := m.awaitFreePoolUsed()
	totalMemUsed += awaitFreeUsed

	m.avoidance.heapTracked.Store(totalMemUsed)
	m.avoidance.lastUpdateUtimeMilli.Store(nowUnixMilli())
}

func (m *MemArbitrator) tryShrinkAwaitFreePool(minRemain, holderMinRemain int64, utimeMilli int64) bool {
	if m.awaitFree.lastShrinkUtimeMilli.Load()+DefAwaitFreePoolShrinkDurMilli <= utimeMilli {
		m.shrinkAwaitFreePool(minRemain, holderMinRemain, utimeMilli)
		return true
	}
	return false
}

func (m *MemArbitrator) shrinkAwaitFreePool(minRemain, holderMinRemain int64, utimeMilli int64) {
	poolReleased := int64(0)
	reclaimed := m.awaitFree.budget.shrink(minRemain, utimeMilli) +
		m.awaitFree.holder.shrink(holderMinRemain, utimeMilli)
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
	return int64(m.heapController.heapAlloc.Load()) < m.mu.threshold.risk
}

func (m *MemArbitrator) isMemSafe() bool {
	return int64(m.heapController.heapInuse.Load()) < m.mu.threshold.oomRisk
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

	if memState.LastRisk.QuotaAlloc != 0 && memState.LastRisk.HeapAlloc > memState.LastRisk.QuotaAlloc {
		memState.Magnif = calcRatio(memState.LastRisk.HeapAlloc, memState.LastRisk.QuotaAlloc) + 100 /* 10 percent */
		if p := m.lastMemState(); p != nil {
			memState.Magnif = max(memState.Magnif, p.Magnif)
		}
	} else {
		return nil
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
		m.execMetrics.memRisk++

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
	if dur := now.Sub(m.heapController.oomCheck.lastMemStats.startTime); dur >= DefHeapReclaimCheckDuration {
		heapFrees := m.heapController.heapTotalFree.Load() - m.heapController.oomCheck.lastMemStats.heapTotalFree
		freeSpeedBPS := int64(float64(heapFrees) / dur.Seconds())
		needReclaimHeap := false
		if hasMemOOMRisk(freeSpeedBPS, minHeapFreeSpeedBPS, now, m.heapController.oomCheck.startTime) {
			m.execMetrics.oomRisk++
			// dumpHeapProfile()
			var newKillNum int
			var reclaiming int64
			memToReclaim := int64(m.heapController.heapInuse.Load()) - m.mu.threshold.risk
			{
				{ // warning
					profile := m.recordDebugProfile()
					profile.append(
						zap.Float64("mem-free-speed(MiB/s)", float64(freeSpeedBPS*100/MB)/100),
						zap.Float64("required-speed(MiB/s)", float64(minHeapFreeSpeedBPS)/float64(MB)),
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
					float64(freeSpeedBPS*100/MB)/100),
					zap.Float64("required-speed(MiB/s)", float64(minHeapFreeSpeedBPS)/float64(MB)))
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
	return freeSpeedBPS < minHeapFreeSpeedBPS || now.Sub(startTime) > DefHeapReclaimCheckMaxDuration
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
			if deadline := ctx.startTime.Add(DefKillCancelCheckTimeout); now.Compare(deadline) >= 0 {
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

	for prio := minArbitrateMemPriority; prio < maxArbitrateMemPriority; prio++ {
		for pos := m.entryMap.maxQuotaShardIndex - 1; pos >= m.entryMap.minQuotaShardIndexToCheck; pos-- {
			for uid, entry := range m.entryMap.quotaShards[prio][pos].entries {
				if entry.arbitratorMu.underKill.bool || entry.notRunning() {
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
					m.execMetrics.oomKill[prio]++

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

type LastRisk struct {
	HeapAlloc  int64 `json:"heap"`
	QuotaAlloc int64 `json:"quota"`
}

type RuntimeMemStateV1 struct {
	Version  int64    `json:"version"`
	LastRisk LastRisk `json:"last-risk"`
	// magnification ratio of heap-alloc/quota
	Magnif int64 `json:"magnif"`
	// medium quota usage of root pools
	PoolMediumCap int64 `json:"pool-medium-cap"`
	// top-n profiles by digest
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

func (m *MemArbitrator) GetAwaitFreeBudgets(uid uint64) *ConcurrentBudget {
	index := shardIndexByUID(uid, m.awaitFree.budget.sizeMask)
	return &m.awaitFree.budget.shards[index]
}

func (m *MemArbitrator) awaitFreeHolder(uid uint64) *ConcurrentBudget {
	index := shardIndexByUID(uid, m.awaitFree.holder.sizeMask)
	return &m.awaitFree.holder.shards[index]
}

func (m *MemArbitrator) initAwaitFreePool(allocAlignSize, shardNum int64, holderShardNum int64) {
	if allocAlignSize <= 0 {
		allocAlignSize = DefAwaitFreePoolAllocAlignSize
	}

	p := &ResourcePool{
		name:                   "fast-alloc-pool",
		uid:                    0,
		limit:                  DefMaxLimit,
		allocAlignSize:         allocAlignSize,
		cleanAllOnReleaseBytes: true,
	}

	p.SetOutOfCapacityAction(func(s OutOfCapacityActionArgs) error {
		if int64(m.heapController.heapAlloc.Load()) > m.mu.threshold.risk-s.request {
			m.execMetrics.awaitFree.fail++
			return errArbitrateFailError
		}
		if int64(m.heapController.heapInuse.Load()) > m.mu.threshold.oomRisk-s.request {
			m.execMetrics.awaitFree.fail++
			return errArbitrateFailError
		}

		if m.mu.allocated > m.mu.limit-m.avoidance.size.Load()-s.request {
			m.execMetrics.awaitFree.fail++
			return errArbitrateFailError
		}

		{
			m.mu.Lock()
			//
			m.mu.allocated += s.request
			//
			m.mu.Unlock()
		}

		p.forceAddCap(s.request)

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
	{
		cnt := nextPow2(uint64(holderShardNum))
		m.awaitFree.holder = FixSizeBatchBudgets{
			make([]ConcurrentBudget, cnt),
			cnt - 1,
		}
		for i := range m.awaitFree.holder.shards {
			m.awaitFree.holder.shards[i].InitUpperPool(p)
		}
	}
}

type ArbitrateHelper interface {
	Kill() bool       // kill by arbitrator only when meeting oom risk
	Cancel() bool     // cancel by arbitrator
	HeapInuse() int64 // track heap usage
}

type Context struct { // immutable after created
	arbitrateHelper ArbitrateHelper

	hisMaxMemUsed   int64 // historical maximum memory usage
	memQuotaLimit   int64
	cancelCh        <-chan struct{}
	memPriority     ArbitrateMemPriority
	waitAverse      bool
	preferPrivilege bool
	stopped         atomic.Bool
}

func (ctx *Context) available() bool {
	if ctx != nil && ctx.arbitrateHelper != nil && !ctx.stopped.Load() {
		return true
	}
	return false
}

func (ctx *Context) stop(stopByKill bool) {
	if ctx.stopped.Swap(true) {
		return
	}
	if stopByKill {
		ctx.arbitrateHelper.Kill()
	} else {
		ctx.arbitrateHelper.Cancel()
	}
}

func NewContext(
	cancelCh <-chan struct{},
	hisMaxMemUsed, memQuotaLimit int64,
	arbitrateHelper ArbitrateHelper,
	memPriority ArbitrateMemPriority,
	waitAverse bool,
	preferPrivilege bool,
) *Context {
	return &Context{
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

func (m *MemArbitrator) TaskNumByMode() (res waitingTaskNum) {
	for i := minArbitrateMemPriority; i < maxArbitrateMemPriority; i++ {
		res[i] = m.taskNumByPriority(i)
	}
	res[ArbitrateWaitAverse] = m.taskNumOfWaitAverse()
	return
}

// req > 0: alloc quota
// req == 0: try to pull from upstream
// req < 0: release quota
// hold: if true, hold the quota but not use it
func (m *MemArbitrator) AllocQuotaFromGlobalMemArbitrator(uid uint64, req int64, hold bool) error {
	var b *ConcurrentBudget
	if hold {
		b = m.awaitFreeHolder(uid)
	} else {
		b = m.GetAwaitFreeBudgets(uid)
	}

	if req >= 0 {
		if b.LastUsedTimeSec != m.UnixTimeSec {
			b.LastUsedTimeSec = m.UnixTimeSec
		}
		if b.Used.Add(req) > b.Capacity.Load() {
			if err, _ := b.PullFromUpstream(); err != nil {
				return err
			}
		}
	} else {
		b.Used.Add(req)
	}
	return nil
}
