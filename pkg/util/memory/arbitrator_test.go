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
	"container/list"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	NoWaitAverse     = false
	RequirePrivilege = true
)

var testState *testing.T

func (m *MemArbitrator) removeEntryForTest(entry *rootPoolEntry) bool {
	require.True(testState, m.removeRootPoolEntry(entry))
	return true
}

func (m *MemArbitrator) cleanupNotifer() {
	m.notifer.Wake()
	m.notifer.Wait()
}
func (m *MemArbitrator) waitNotiferForTest() {
	m.notifer.Wait()
}

func (m *MemArbitrator) restartEntryForTest(entry *rootPoolEntry, ctx *ArbitrationContext) {
	require.True(testState, m.restartEntryByContext(entry, ctx))
}

func (m *MemArbitrator) checkAwaitFree() {
	s := int64(0)
	for i := range m.awaitFree.budget.shards {
		s += m.awaitFree.budget.shards[i].Capacity
	}
	require.True(testState, s == m.awaitFree.pool.allocated())
}

// use atomic add 0 to implement memory fence
func (m *MemArbitrator) notiferIsAwake() bool {
	return atomic.AddInt32(&m.notifer.awake, 0) != 0
}

func (s *entryQuotaShard) getEntry(key uint64) *rootPoolEntry {
	v, ok := s.entries[key]
	if ok {
		return v
	}
	return nil
}

func (m *MemArbitrator) addRootPoolForTest(
	p *ResourcePool,
	ctx *ArbitrationContext,
) *rootPoolEntry {
	entry, err := m.addRootPool(p)
	if err != nil {
		panic(err)
	}
	require.True(testState, entry != nil)
	require.True(testState, m.restartEntryByContext(entry, ctx))
	return entry
}

func (m *MemArbitrator) addEntryForTest(
	ctx *ArbitrationContext,
) *rootPoolEntry {
	p := newResourcePoolForTest("test", 1)
	return m.addRootPoolForTest(p, ctx)
}

func (m *MemArbitrator) getAllEntryForTest() mapUIDEntry {
	t := testState
	res := make(mapUIDEntry)
	emptyCnt := 0
	for _, shard := range m.entryMap.shards {
		shard.RLock()
		//
		for uid, v := range shard.entries {
			res[uid] = v
			if v.arbitratorMu.quota == 0 {
				emptyCnt += 1
				require.True(t, v.arbitratorMu.quotaShard == nil)
			} else {
				require.True(t, v.arbitratorMu.quotaShard != nil)
			}
		}
		//
		shard.RUnlock()
	}
	cnt := len(res)
	{
		require.True(t, m.RootPoolNum() == int64(cnt))
		require.True(t, m.entryMap.contextCache.num.Load() == int64(cnt))
	}

	for prio := minArbitrationPriority; prio < maxArbitrationPriority; prio++ {
		for _, shard := range m.entryMap.quotaShards[prio] {
			for uid, v := range shard.entries {
				e, ok := res[uid]
				require.True(t, ok)
				require.Equal(t, e, v)
				require.True(t, v.arbitratorMu.quotaShard == shard)
				cnt -= 1
			}
		}
	}

	cnt -= emptyCnt
	require.Equal(t, cnt, 0)
	return res
}

func (m *MemArbitrator) checkEntryForTest(expect ...*rootPoolEntry) {
	t := testState
	s := m.getAllEntryForTest()
	require.Equal(t, len(expect), len(s))

	var sumQuota int64
	for _, entry := range expect {
		uid := entry.pool.UID()
		e, ok := s[uid]
		require.True(t, ok)
		require.Equal(t, e, entry)
		require.Equal(t, m.getRootPoolEntry(uid), e)
		require.True(t, !e.arbitratorMu.destroyed)
		select {
		case <-entry.request.resultCh:
			require.True(t, false)
		default:
		}

		if e.arbitratorMu.quota == 0 {
			continue
		}

		sumQuota += e.arbitratorMu.quota
		require.Equal(t, e.arbitratorMu.quota, entry.pool.ApproxCap())
		quotaShared := m.getQuotaShard(e.ctx.memPriority, e.arbitratorMu.quota)
		require.Equal(t, quotaShared.getEntry(uid), e)
	}
	sumQuota += m.awaitFree.pool.capacity()
	require.Equal(t, sumQuota, m.Allocated())
}

func (m *MemArbitrator) getQuotaShard(priority ArbitrationPriority, quota int64) *entryQuotaShard {
	return m.entryMap.getQuotaShard(priority, quota)
}

func newResourcePoolForTest(
	name string,
	allocAlignSize int64,
) *ResourcePool {
	p := newResourcePoolWithLimit(name, 0, allocAlignSize)
	return p
}

func (m *MemArbitrator) setLimitForTest(v int64) {
	m.mu.Lock()
	//
	m.mu.limit = v
	//
	m.mu.Unlock()
}

func newMemArbitratorForTest(shardCount uint64, limit int64) (m *MemArbitrator) {
	loadEvent := 0
	m = NewMemArbitrator(limit, shardCount, 3, 0, &memStateRecorderForTest{
		load: func() (*RuntimeMemStateV1, error) {
			loadEvent++
			return nil, nil
		},
		store: func(*RuntimeMemStateV1) error {
			return nil
		},
	})
	require.True(testState, loadEvent == 1)
	m.initAwaitFreePool(4, 4)
	return
}

type arbitrateHelperForTest struct {
	cancelCh   chan struct{}
	killCB     func()
	heapUsedCB func() int64
	cancelCB   func()
}

func (t *arbitrateHelperForTest) cancelSelf() {
	close(t.cancelCh)
}

func (t *arbitrateHelperForTest) HeapInuse() int64 {
	if t.heapUsedCB != nil {
		return t.heapUsedCB()
	}
	return 0
}

func (t *arbitrateHelperForTest) Finish() {
}

func (t *arbitrateHelperForTest) Stop(reason ArbitratorStopReason) bool {
	close(t.cancelCh)
	if reason == ArbitratorOOMRiskKill {
		t.killCB()
	} else {
		t.cancelCB()
	}

	return true
}

func (m *MemArbitrator) deleteEntryForTest(enrtries ...*rootPoolEntry) {
	for _, e := range enrtries {
		m.RemoveRootPoolByID(e.pool.uid)
	}
	m.doExecuteCleanupTasks()
	m.cleanupNotifer()
}

func (m *MemArbitrator) resetEntryForTest(enrtries ...*rootPoolEntry) {
	for _, e := range enrtries {
		m.ResetRootPoolByID(e.pool.uid, 0, false)
	}
	m.doExecuteCleanupTasks()
	m.cleanupNotifer()
}

func (m *MemArbitrator) checkEntryQuotaByPriority(
	e *rootPoolEntry,
	expectedMemPriority ArbitrationPriority,
	quota int64,
) {
	t := testState
	require.True(t, expectedMemPriority == e.ctx.memPriority)
	require.True(t, quota == e.arbitratorMu.quota)

	if e.arbitratorMu.quota != 0 {
		require.True(t, expectedMemPriority == e.ctx.memPriority)
		shard := m.entryMap.quotaShards[expectedMemPriority][getQuotaShard(e.arbitratorMu.quota, m.entryMap.maxQuotaShardIndex)]
		require.True(t, shard.getEntry(e.pool.uid) == e)
		require.True(t, shard == e.arbitratorMu.quotaShard)
	}
}

func (m *MemArbitrator) resetAwaitFreeForTest() {
	t := testState
	for i := range m.awaitFree.budget.shards {
		b := &m.awaitFree.budget.shards[i]
		b.Used.Store(0)
		b.HeapInuse.Store(0)
		b.setLastUsedTimeSec(0)
	}
	require.True(t, m.awaitFreePoolUsed() == memPoolQuotaUsage{})
	m.shrinkAwaitFreePool(0, nowUnixMilli())
	require.Equal(t, int64(0), m.awaitFreePoolCap())
}

func (m *MemArbitrator) findTaskByMode(
	e *rootPoolEntry,
	prio ArbitrationPriority,
	waitAverse bool,
) (found bool) {
	t := testState
	for p := minArbitrationPriority; p < maxArbitrationPriority; p++ {
		tasks := &m.tasks.fifoByPriority[p]
		ele := tasks.base.Front()
		for i := int64(0); i < tasks.size(); i++ {
			if ele.Value.(*rootPoolEntry) == e {
				require.False(t, found)
				require.True(t, prio == p)

				found = true
				require.True(t, e.ctx.memPriority == p)
				require.True(t, e.ctx.waitAverse == waitAverse)
				require.True(t, e.taskMu.fifoByPriority.base == ele)

				if ctx := e.ctx.Load(); ctx == nil {
					require.True(t, p == ArbitrationPriorityMedium)
					require.False(t, waitAverse)
				} else {
					require.True(t, p == ctx.memPriority)
					require.True(t, waitAverse == ctx.waitAverse)
				}

				if waitAverse {
					require.True(t, !e.ctx.preferPrivilege)
					ele2 := m.tasks.fifoWaitAverse.base.Front()
					found2 := false
					for j := int64(0); j < m.tasks.fifoWaitAverse.size(); j++ {
						if ele2.Value.(*rootPoolEntry) == e {
							require.False(t, found2)
							require.True(t, e.taskMu.fifoWaitAverse.base == ele2)
							found2 = true
						}
						ele2 = ele2.Next()
					}
					require.True(t, found2)
				}

				{
					ele3 := m.tasks.fifoTasks.base.Front()
					found3 := false
					for j := int64(0); j < m.tasks.fifoTasks.size(); j++ {
						if ele3.Value.(*rootPoolEntry) == e {
							require.False(t, found3)
							require.True(t, e.taskMu.fifo.base == ele3)
							found3 = true
						}
						ele3 = ele3.Next()
					}
					require.True(t, found3)
				}
			}
			ele = ele.Next()
		}
	}

	return
}

func (m *MemArbitrator) tasksCountForTest() (sz int64) {
	for i := minArbitrationPriority; i < maxArbitrationPriority; i++ {
		sz += m.taskNumByPriority(i)
	}
	require.True(testState, sz == m.tasks.fifoTasks.size())
	require.False(testState, sz == 0 && m.WaitingAllocSize() != 0)
	return sz
}

func newCtxForTest(ch <-chan struct{}, h ArbitrateHelper, memPriority ArbitrationPriority, waitAverse bool, preferPrivilege bool) *ArbitrationContext {
	return NewArbitrationContext(ch, 0, 0, h, memPriority, waitAverse, preferPrivilege)
}

func newDefCtxForTest(memPriority ArbitrationPriority) *ArbitrationContext {
	return newCtxForTest(nil, nil, memPriority, NoWaitAverse, false)
}

func (m *MemArbitrator) newPoolWithHelperForTest(prefix string, memPriority ArbitrationPriority, waitAverse, preferPrivilege bool) *rootPoolEntry {
	ctx := m.newCtxWithHelperForTest(memPriority, waitAverse, preferPrivilege)
	pool := newResourcePoolForTest("", 1)
	pool.name = prefix + fmt.Sprintf("-%d", pool.uid)
	e := m.addRootPoolForTest(pool, ctx)
	return e
}

func (m *MemArbitrator) newCtxWithHelperForTest(memPriority ArbitrationPriority, waitAverse, preferPrivilege bool) *ArbitrationContext {
	h := &arbitrateHelperForTest{
		cancelCh: make(chan struct{}),
	}
	ctx := newCtxForTest(h.cancelCh, h, memPriority, waitAverse, preferPrivilege)
	return ctx
}

func (m *MemArbitrator) resetExecMetricsForTest() {
	m.execMetrics = execMetricsCounter{}
	m.execMu.blockedState = blockedState{}
	m.buffer = buffer{}
	m.resetDigestProfileCache(uint64(len(m.digestProfileCache.shards)))
	m.resetStatistics()
	m.mu.lastGC = m.mu.released
}

type notiferWithWg struct {
	m  *MemArbitrator
	ch chan struct{}
}

func newNotiferWrap(m *MemArbitrator) notiferWithWg {
	if m != nil {
		require.False(testState, m.notiferIsAwake())
	}
	return notiferWithWg{
		m:  m,
		ch: make(chan struct{}),
	}
}

func (n *notiferWithWg) close() {
	close(n.ch)
}

func (n *notiferWithWg) wait() {
	<-n.ch
}

func (m *MemArbitrator) frontTaskEntryForTest() (entry *rootPoolEntry) {
	entry = m.frontTaskEntry()
	if entry == nil {
		return
	}
	require.True(testState, entry.taskMu.fifo.valid())
	require.True(testState, entry.taskMu.fifo.base.Value.(*rootPoolEntry) == entry)
	return
}

func (m *MemArbitrator) checkTaskExec(task pairSuccessFail, cancelByStandardMode int64, cancel NumByPattern) {
	t := testState
	require.Equal(t, m.execMetrics.Task.pairSuccessFail, task)
	s := int64(0)
	for i := minArbitrationPriority; i < maxArbitrationPriority; i++ {
		s += m.execMetrics.Task.SuccByPriority[i]
	}
	if m.workMode() == ArbitratorModePriority {
		require.Equal(t, s, task.Succ)
	} else {
		require.True(t, s == 0)
	}
	require.True(t, m.execMetrics.Cancel == execMetricsCancel{
		StandardMode: cancelByStandardMode,
		PriorityMode: NumByPriority(cancel[:3]),
		WaitAverse:   cancel[3],
	})
}

func (m *MemArbitrator) setMemStatsForTest(alloc, heapInuse, totalAlloc, memOffHeap int64) {
	lastGC := now().UnixNano()
	m.SetRuntimeMemStats(RuntimeMemStats{alloc, heapInuse, totalAlloc - alloc, memOffHeap, lastGC})
}

type memStateRecorderForTest struct {
	load  func() (*RuntimeMemStateV1, error)
	store func(*RuntimeMemStateV1) error
}

func (m *memStateRecorderForTest) Load() (*RuntimeMemStateV1, error) {
	return m.load()
}

func (m *memStateRecorderForTest) Store(state *RuntimeMemStateV1) error {
	return m.store(state)
}

func (m *MemArbitrator) cleanDigestProfileForTest() {
	n := m.digestProfileCache.num.Load()
	require.True(testState, m.shrinkDigestProfile(defMax, 0, 0) == n)
	require.True(testState, m.digestProfileCache.num.Load() == 0)
}

func (m *MemArbitrator) wrapblockedState() blockedState {
	return m.execMu.blockedState
}

func (m *MemArbitrator) restartForTest() bool {
	m.weakWake()
	return m.asyncRun(defTaskTickDur)
}

func BenchmarkWrapList(b *testing.B) {
	data := wrapList[int64]{}
	data.init()
	pushCnt, popCnt := 0, 0
	for i := range b.N {
		if (i % 200) >= 100 {
			data.popFront()
			popCnt++
		} else {
			data.pushBack(1234)
			pushCnt++
		}
	}
	require.Equal(b, data.size(), int64(pushCnt-popCnt))
}

func BenchmarkList(b *testing.B) {
	data := list.New()
	pushCnt, popCnt := 0, 0
	for i := range b.N {
		if (i % 200) >= 100 {
			data.Remove(data.Front())
			popCnt++
		} else {
			data.PushBack(1234)
			pushCnt++
		}
	}
	require.Equal(b, data.Len(), pushCnt-popCnt)
}

func TestMemArbitratorSwitchMode(t *testing.T) {
	testState = t

	m := newMemArbitratorForTest(1, -1)
	require.Equal(t, m.WorkMode(), ArbitratorModeDisable)
	m.resetExecMetricsForTest()
	newLimit := int64(1000)
	m.setLimitForTest(newLimit)
	require.Equal(t, m.limit(), newLimit)
	actionCancel := make(map[uint64]int)

	genTestPoolWithHelper := func(memPriority ArbitrationPriority, waitAverse, preferPrivilege bool) *rootPoolEntry {
		e := m.newPoolWithHelperForTest("test", memPriority, waitAverse, preferPrivilege)
		e.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).cancelCB = func() {
			actionCancel[e.pool.uid]++
		}
		return e
	}

	genTestCtx := func(e *rootPoolEntry, memPriority ArbitrationPriority, waitAverse, preferPrivilege bool) *ArbitrationContext {
		c := m.newCtxWithHelperForTest(memPriority, waitAverse, preferPrivilege)
		c.arbitrateHelper.(*arbitrateHelperForTest).cancelCB = func() {
			actionCancel[e.pool.uid]++
		}
		return c
	}

	entry1 := genTestPoolWithHelper(ArbitrationPriorityMedium, NoWaitAverse, false)
	{ // Disable mode
		entry1.ctx.preferPrivilege = true
		require.True(t, entry1.execState() == execStateRunning)
		require.False(t, entry1.stateMu.stop.Load())
		requestSize := newLimit * 2

		m.prepareAlloc(entry1, requestSize)
		m.waitNotiferForTest()
		require.True(t, m.tasksCountForTest() == 1)
		require.True(t, m.WaitingAllocSize() == requestSize)
		require.True(t, m.WaitingAllocSize() > m.limit()) // always success even when over limit

		require.Equal(t, m.frontTaskEntryForTest(), entry1)
		require.True(t, m.getRootPoolEntry(entry1.pool.uid) == entry1)
		require.True(t, m.TaskNumByPattern() == NumByPattern{0, 1, 0, 0})
		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityMedium, 0)
		require.Equal(t, -1, m.runOneRound()) // always -1
		require.True(t, m.execMu.blockedState.allocated == 0)
		require.Equal(t, m.waitAlloc(entry1), ArbitrateOk)
		require.True(t, m.tasksCountForTest() == 0)

		require.False(t, m.findTaskByMode(entry1, ArbitrationPriorityMedium, NoWaitAverse))
		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityMedium, requestSize)

		m.checkTaskExec(pairSuccessFail{1, 0}, 0, NumByPattern{}) // always success

		require.True(t, m.privilegedEntry == nil) // can not get privilege
		require.True(t, entry1.execState() == execStateRunning)
		require.False(t, entry1.stateMu.stop.Load())

		require.Equal(t, m.allocated(), requestSize)
		require.True(t, m.allocated() > m.limit()) // success to allocate quota even over limit
		m.checkEntryForTest(entry1)

		{ // illegal operation
			e0 := m.addEntryForTest(nil)
			m.prepareAlloc(e0, 1)
			require.True(t, m.RootPoolNum() == 2)
			require.True(t, m.runOneRound() == -1)
			require.True(t, m.waitAlloc(e0) == ArbitrateOk)
			require.True(t, e0.arbitratorMu.quota == 1)

			m.prepareAlloc(e0, 1)
			require.Equal(t, ArbitrationPriorityMedium, e0.taskMu.fifoByPriority.priority)
			m.resetRootPoolEntry(e0) // reset entry
			m.restartEntryForTest(e0, m.newCtxWithHelperForTest(ArbitrationPriorityLow, NoWaitAverse, false))
			require.Equal(t, ArbitrationPriorityLow, e0.ctx.memPriority)
			require.Equal(t, ArbitrationPriorityMedium, e0.taskMu.fifoByPriority.priority)
			if m.execMu.mode == ArbitratorModeDisable {
				m.implicitRun()
			}
			require.True(t, m.waitAlloc(e0) == ArbitrateOk)
			require.True(t, e0.arbitratorMu.quota == 2)
			require.True(t, e0.arbitratorMu.quota == e0.pool.Capacity()+e0.stateMu.quotaToReclaim.Load())
			require.True(t, m.cleanupMu.fifoTasks.size() == 1)

			m.resetRootPoolEntry(e0) // 1. reset entry
			m.removeEntryForTest(e0) // 2. delete entry
			require.True(t, m.cleanupMu.fifoTasks.size() == 3)
			m.prepareAlloc(e0, 1)                  // alloc for non-exist entry
			require.True(t, m.runOneRound() == -1) // no task
			require.True(t, m.waitAlloc(e0) == ArbitrateFail)
			require.True(t, m.getRootPoolEntry(e0.pool.uid) == nil)
			require.True(t, m.RootPoolNum() == 1)
			m.prepareAlloc(e0, 1)                  // alloc for non-exist entry
			require.True(t, m.runOneRound() == -1) // wind up non-exist entry with fail
			require.True(t, m.waitAlloc(e0) == ArbitrateFail)

			e1 := m.addEntryForTest(nil)
			m.removeEntryForTest(e1)                           // 1. delete entry
			require.False(t, m.resetRootPoolEntry(e1))         // 2. reset entry failed
			require.True(t, m.cleanupMu.fifoTasks.size() == 1) //
			m.prepareAlloc(e1, 1)
			if m.execMu.mode == ArbitratorModeDisable {
				m.implicitRun()
			}
			require.True(t, m.waitAlloc(e1) == ArbitrateOk) // success
			m.doExecuteCleanupTasks()
		}
	}

	entry2 := genTestPoolWithHelper(ArbitrationPriorityMedium, NoWaitAverse, false)
	m.resetExecMetricsForTest()
	m.cleanupNotifer()
	{ // Disable mode -> Standard mode
		require.True(t, m.allocated() == 2*newLimit)

		requestSize := int64(1)

		m.prepareAlloc(entry2, requestSize)
		m.waitNotiferForTest()
		require.True(t, m.tasksCountForTest() == 1)
		require.True(t, m.WaitingAllocSize() == requestSize)

		require.Equal(t, m.frontTaskEntryForTest(), entry2)
		require.True(t, entry2.request.quota == requestSize)

		require.True(t, m.findTaskByMode(entry2, ArbitrationPriorityMedium, NoWaitAverse))
		require.True(t, m.TaskNumByPattern() == NumByPattern{0, 1, 0, 0})
		m.checkEntryQuotaByPriority(entry2, ArbitrationPriorityMedium, 0)

		m.SetWorkMode(ArbitratorModeStandard)
		require.Equal(t, m.WorkMode(), ArbitratorModeStandard)

		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 2*newLimit)

		m.checkTaskExec(pairSuccessFail{}, 1, NumByPattern{})
		require.True(t, actionCancel[entry2.pool.uid] == 1)
		require.True(t, m.waitAlloc(entry2) == ArbitrateFail)

		require.False(t, m.findTaskByMode(entry2, ArbitrationPriorityMedium, NoWaitAverse))
		m.checkEntryQuotaByPriority(entry2, ArbitrationPriorityMedium, 0)
		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityMedium, newLimit*2)
		m.checkTaskExec(pairSuccessFail{0, 1}, 1, NumByPattern{})

		require.True(t, m.tasksCountForTest() == 0)
		require.True(t, entry1.execState() == execStateRunning)
		require.True(t, entry2.execState() == execStateRunning)
		require.False(t, entry1.stateMu.stop.Load())
		require.False(t, entry2.stateMu.stop.Load())
		m.checkEntryForTest(entry1, entry2)

		entrys := make([]*rootPoolEntry, 0)
		for prio := minArbitrationPriority; prio < maxArbitrationPriority; prio++ {
			for _, waitAverse := range [2]bool{false, true} {
				e := genTestPoolWithHelper(prio, waitAverse, true)
				entrys = append(entrys, e)
				m.prepareAlloc(e, m.limit())
			}
		}
		newEntries := entrys[:]
		entrys = append(entrys, entry1, entry2)
		m.checkEntryForTest(entrys...)
		require.True(t, m.privilegedEntry == nil) // can not use privilege
		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 2*newLimit)
		require.True(t, m.privilegedEntry == nil) // can not use privilege

		for _, e := range newEntries {
			require.True(t, m.waitAlloc(e) == ArbitrateFail)
		}

		m.checkTaskExec(pairSuccessFail{0, 7}, 7, NumByPattern{})
		m.deleteEntryForTest(newEntries...)
		m.checkEntryForTest(entry1, entry2)
	}

	m.resetExecMetricsForTest()
	m.cleanupNotifer()
	actionCancel = make(map[uint64]int)
	{ // Standard mode -> Priority mode: wait until cancel self
		require.True(t, m.allocated() == 2*newLimit)

		requestSize := int64(1)
		m.resetEntryForTest(entry2)
		m.restartEntryForTest(entry2, genTestCtx(entry2, ArbitrationPriorityMedium, NoWaitAverse, false))
		m.prepareAlloc(entry2, requestSize)
		m.waitNotiferForTest()
		require.True(t, m.tasksCountForTest() == 1)
		require.True(t, m.WaitingAllocSize() == requestSize)

		e := m.frontTaskEntryForTest()
		require.Equal(t, e, entry2)

		require.True(t, m.findTaskByMode(e, ArbitrationPriorityMedium, NoWaitAverse))
		require.True(t, m.TaskNumByPattern() == NumByPattern{0, 1, 0, 0})
		m.checkEntryQuotaByPriority(e, ArbitrationPriorityMedium, 0)
		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityMedium, newLimit*2)

		m.SetWorkMode(ArbitratorModePriority)
		require.Equal(t, m.WorkMode(), ArbitratorModePriority)

		require.Equal(t, m.runOneRound(), 0)
		require.True(t, m.tasksCountForTest() == 1)
		require.True(t, m.execMu.blockedState.allocated == 2*newLimit)
		require.True(t, m.WaitingAllocSize() == requestSize)
		m.checkTaskExec(pairSuccessFail{}, 0, NumByPattern{})

		require.True(t, m.findTaskByMode(e, ArbitrationPriorityMedium, NoWaitAverse))
		require.True(t, m.TaskNumByPattern() == NumByPattern{0, 1, 0, 0})
		m.checkEntryQuotaByPriority(e, ArbitrationPriorityMedium, 0)
		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityMedium, newLimit*2)

		entry2.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).cancelSelf()
		require.True(t, m.waitAlloc(entry2) == ArbitrateFail)
		m.checkTaskExec(pairSuccessFail{0, 1}, 0, NumByPattern{})
		require.True(t, len(actionCancel) == 0)

		require.True(t, m.tasksCountForTest() == 0)
		require.True(t, entry1.execState() == execStateRunning)
		require.True(t, entry2.execState() == execStateRunning)
		require.False(t, entry1.stateMu.stop.Load())
		require.False(t, entry2.stateMu.stop.Load())
		m.checkEntryForTest(entry1, entry2)
	}

	m.resetExecMetricsForTest()
	m.cleanupNotifer()
	{ // Priority mode: interrupt lower priority tasks
		require.True(t, len(actionCancel) == 0)
		require.True(t, m.allocated() == 2*newLimit)

		requestSize := int64(1)

		m.resetEntryForTest(entry2)
		m.restartEntryForTest(entry2, newDefCtxForTest(ArbitrationPriorityHigh))
		m.prepareAlloc(entry2, requestSize)

		m.waitNotiferForTest()
		require.True(t, m.tasksCountForTest() == 1)
		require.True(t, m.WaitingAllocSize() == requestSize)
		e := m.frontTaskEntryForTest()
		require.Equal(t, entry2, m.getRootPoolEntry(e.pool.uid))

		require.True(t, entry2.request.quota == requestSize)

		require.True(t, m.findTaskByMode(entry2, ArbitrationPriorityHigh, NoWaitAverse))
		require.True(t, m.TaskNumByPattern() == NumByPattern{0, 0, 1, 0})
		m.checkEntryQuotaByPriority(entry2, ArbitrationPriorityHigh, 0)
		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityMedium, newLimit*2)

		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 2*newLimit)

		m.checkTaskExec(pairSuccessFail{}, 0, NumByPattern{0, 1, 0})

		require.True(t, m.findTaskByMode(entry2, ArbitrationPriorityHigh, NoWaitAverse))
		require.True(t, m.TaskNumByPattern() == NumByPattern{0, 0, 1, 0})
		m.checkEntryQuotaByPriority(entry2, ArbitrationPriorityHigh, 0)
		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityMedium, newLimit*2)

		require.True(t, actionCancel[entry1.pool.uid] == 1) // interrupt entry1
		m.resetRootPoolEntry(entry1)

		require.True(t, entry1.arbitratorMu.underCancel.start)
		require.True(t, entry1.arbitratorMu.underCancel.reclaim == entry1.arbitratorMu.quota)
		require.Equal(t, mapEntryWithMem{entries: map[uint64]*rootPoolEntry{
			entry1.pool.uid: entry1,
		}, num: 1}, m.underCancel)

		require.Equal(t, 1, m.runOneRound())
		require.Equal(t, m.waitAlloc(entry2), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{1, 0}, 0, NumByPattern{0, 1, 0})
		require.Equal(t, mapEntryWithMem{entries: map[uint64]*rootPoolEntry{}, num: 0}, m.underCancel)
		require.True(t, m.tasksCountForTest() == 0)

		require.False(t, m.findTaskByMode(entry2, ArbitrationPriorityHigh, NoWaitAverse))
		m.checkEntryQuotaByPriority(entry2, ArbitrationPriorityHigh, requestSize)
		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityMedium, 0)

		require.True(t, entry1.execState() == execStateIdle)
		require.True(t, entry2.execState() == execStateRunning)
		require.False(t, entry1.stateMu.stop.Load())
		require.False(t, entry2.stateMu.stop.Load())
		m.checkEntryForTest(entry1, entry2)
		m.resetEntryForTest(entry1, entry2)
	}

	m.resetExecMetricsForTest()
	m.cleanupNotifer()
	{ // Priority mode -> Disable mode
		require.True(t, m.allocated() == 0)
		m.ConsumeQuotaFromAwaitFreePool(0, 99)

		requestSize := newLimit + 1
		m.restartEntryForTest(entry1, newDefCtxForTest(ArbitrationPriorityLow))
		m.prepareAlloc(entry1, requestSize)
		m.restartEntryForTest(entry2, newDefCtxForTest(ArbitrationPriorityLow))
		m.prepareAlloc(entry2, requestSize)

		m.waitNotiferForTest()
		require.True(t, m.tasksCountForTest() == 2)
		require.True(t, m.WaitingAllocSize() == requestSize*2)

		require.True(t, m.findTaskByMode(entry1, ArbitrationPriorityLow, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry2, ArbitrationPriorityLow, NoWaitAverse))
		require.True(t, m.TaskNumByPattern() == NumByPattern{2, 0, 0, 0})
		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityLow, 0)
		require.False(t, entry2.ctx.waitAverse)
		require.True(t, entry2.ctx.memPriority == ArbitrationPriorityLow)

		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 100)
		m.checkTaskExec(pairSuccessFail{}, 0, NumByPattern{0, 0, 0})

		require.True(t, m.tasksCountForTest() == 2)
		require.True(t, m.WaitingAllocSize() == requestSize*2)

		require.True(t, m.findTaskByMode(entry1, ArbitrationPriorityLow, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry2, ArbitrationPriorityLow, NoWaitAverse))
		require.True(t, m.TaskNumByPattern() == NumByPattern{2, 0, 0, 0})
		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityLow, 0)
		m.checkEntryQuotaByPriority(entry2, ArbitrationPriorityLow, 0)

		// set work mode to `ArbitratorModeDisable` and make all subscriptions success
		m.SetWorkMode(ArbitratorModeDisable)

		require.Equal(t, -1, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 0) // reset because of new work mode

		require.Equal(t, m.waitAlloc(entry1), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry2), ArbitrateOk)
		require.False(t, m.findTaskByMode(entry1, ArbitrationPriorityLow, NoWaitAverse))
		require.False(t, m.findTaskByMode(entry2, ArbitrationPriorityLow, NoWaitAverse))
		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityLow, requestSize)
		m.checkEntryQuotaByPriority(entry2, ArbitrationPriorityLow, requestSize)
		require.True(t, m.allocated() == requestSize*2+m.awaitFreePoolCap())
		require.True(t, m.tasksCountForTest() == 0)
		m.ConsumeQuotaFromAwaitFreePool(0, -99)
		m.resetAwaitFreeForTest()
		m.checkEntryForTest(entry1, entry2)
		m.checkTaskExec(pairSuccessFail{2, 0}, 0, NumByPattern{})
		m.resetEntryForTest(entry1, entry2)
		require.True(t, m.allocated() == 0)
	}

	m.SetWorkMode(ArbitratorModePriority)
	m.resetExecMetricsForTest()
	actionCancel = make(map[uint64]int)
	{ // Priority mode: mixed task mode ArbitrateWaitAverse
		allocUnit := int64(4000)
		m.restartEntryForTest(entry1, genTestCtx(entry1, ArbitrationPriorityLow, NoWaitAverse, false))
		m.restartEntryForTest(entry2, genTestCtx(entry2, ArbitrationPriorityMedium, NoWaitAverse, false))
		entry3 := genTestPoolWithHelper(ArbitrationPriorityHigh, NoWaitAverse, false)
		entry4 := genTestPoolWithHelper(ArbitrationPriorityLow, NoWaitAverse, false)
		entry5 := genTestPoolWithHelper(ArbitrationPriorityHigh, true, false)
		m.checkEntryForTest(entry1, entry2, entry3, entry4, entry5)

		m.setLimitForTest(8 * allocUnit)

		m.prepareAlloc(entry1, allocUnit)
		m.prepareAlloc(entry2, allocUnit)
		m.prepareAlloc(entry3, allocUnit)
		m.prepareAlloc(entry4, allocUnit*4)
		m.prepareAlloc(entry5, allocUnit)

		require.True(t, m.findTaskByMode(entry1, ArbitrationPriorityLow, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry2, ArbitrationPriorityMedium, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry3, ArbitrationPriorityHigh, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry4, ArbitrationPriorityLow, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry5, ArbitrationPriorityHigh, true))

		require.True(t, m.tasksCountForTest() == 5)
		require.True(t, m.WaitingAllocSize() == 8*allocUnit)
		require.True(t, m.TaskNumByPattern() == NumByPattern{2, 1, 2, 1})

		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityLow, 0)
		m.checkEntryQuotaByPriority(entry2, ArbitrationPriorityMedium, 0)
		m.checkEntryQuotaByPriority(entry3, ArbitrationPriorityHigh, 0)
		m.checkEntryQuotaByPriority(entry4, ArbitrationPriorityLow, 0)
		m.checkEntryQuotaByPriority(entry5, ArbitrationPriorityHigh, 0)

		require.Equal(t, 5, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 0)
		require.True(t, m.allocated() == 8*allocUnit)

		require.Equal(t, m.waitAlloc(entry1), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry2), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry3), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry4), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry5), ArbitrateOk)

		m.checkTaskExec(pairSuccessFail{5, 0}, 0, NumByPattern{})
		m.checkEntryQuotaByPriority(entry1, ArbitrationPriorityLow, allocUnit)
		m.checkEntryQuotaByPriority(entry2, ArbitrationPriorityMedium, allocUnit)
		m.checkEntryQuotaByPriority(entry3, ArbitrationPriorityHigh, allocUnit)
		m.checkEntryQuotaByPriority(entry4, ArbitrationPriorityLow, allocUnit*4)
		m.checkEntryQuotaByPriority(entry5, ArbitrationPriorityHigh, allocUnit)

		require.NotEqual(t,
			m.entryMap.getQuotaShard(entry4.ctx.memPriority, entry4.arbitratorMu.quota),
			m.entryMap.getQuotaShard(entry1.ctx.memPriority, entry1.arbitratorMu.quota))

		m.prepareAlloc(entry1, allocUnit)
		m.prepareAlloc(entry2, allocUnit)
		m.prepareAlloc(entry3, allocUnit)
		m.prepareAlloc(entry4, allocUnit)
		m.prepareAlloc(entry5, allocUnit)

		require.True(t, m.findTaskByMode(entry1, ArbitrationPriorityLow, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry2, ArbitrationPriorityMedium, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry3, ArbitrationPriorityHigh, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry4, ArbitrationPriorityLow, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry5, ArbitrationPriorityHigh, true))
		require.True(t, m.tasksCountForTest() == 5)
		require.True(t, m.WaitingAllocSize() == 5*allocUnit)
		require.True(t, m.TaskNumByPattern() == NumByPattern{2, 1, 2, 1})

		require.True(t, m.underCancel.num == 0)
		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 8*allocUnit)

		require.Equal(t, m.waitAlloc(entry5), ArbitrateFail)
		require.Equal(t, m.waitAlloc(entry4), ArbitrateFail)
		require.True(t, actionCancel[entry5.pool.uid] == 1) // cancel self
		require.False(t, entry5.arbitratorMu.underCancel.start)
		require.True(t, actionCancel[entry4.pool.uid] == 1) // cancel by entry3
		require.True(t, entry4.arbitratorMu.underCancel.start)
		require.True(t, m.underCancel.num == 1)
		require.True(t, m.underCancel.entries[entry4.pool.uid] != nil)
		require.True(t, entry4.arbitratorMu.underCancel.start)
		m.checkTaskExec(pairSuccessFail{5, 2}, 0, NumByPattern{1, 0, 0, 1})
		require.True(t, m.TaskNumByPattern() == NumByPattern{1, 1, 1, 0})
		require.True(t, m.tasksCountForTest() == 3)
		require.True(t, m.WaitingAllocSize() == 3*allocUnit)

		oriMetrics := m.execMetrics
		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 8*allocUnit)
		require.True(t, m.execMetrics == oriMetrics)

		m.resetEntryForTest(entry5)
		require.Equal(t, 1, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 8*allocUnit)
		require.Equal(t, m.waitAlloc(entry3), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{6, 2}, 0, NumByPattern{1, 0, 0, 1})
		require.True(t, m.tasksCountForTest() == 2)
		require.True(t, m.WaitingAllocSize() == 2*allocUnit)
		m.setLimitForTest(1 * allocUnit)
		m.resetEntryForTest(entry3, entry4)
		require.True(t, m.underCancel.num == 0)

		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 2*allocUnit)

		m.checkTaskExec(pairSuccessFail{6, 2}, 0, NumByPattern{2, 0, 0, 1})
		require.True(t, m.underCancel.num == 1)
		require.True(t, m.underCancel.entries[entry1.pool.uid] != nil)
		require.True(t, entry1.arbitratorMu.underCancel.start)
		require.Equal(t, m.waitAlloc(entry1), ArbitrateFail)
		m.checkTaskExec(pairSuccessFail{6, 3}, 0, NumByPattern{2, 0, 0, 1})
		require.True(t, actionCancel[entry1.pool.uid] == 1) // cancel by entry2

		m.setLimitForTest(3 * allocUnit)
		require.Equal(t, 1, m.runOneRound())
		require.Equal(t, m.waitAlloc(entry2), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{7, 3}, 0, NumByPattern{2, 0, 0, 1})
		require.True(t, m.tasksCountForTest() == 0)

		m.resetEntryForTest(entry1, entry2)

		m.resetExecMetricsForTest()

		actionCancel = make(map[uint64]int)

		m.restartEntryForTest(entry1, genTestCtx(entry1, ArbitrationPriorityLow, NoWaitAverse, false))
		m.restartEntryForTest(entry2, genTestCtx(entry2, ArbitrationPriorityMedium, NoWaitAverse, false))
		m.restartEntryForTest(entry3, genTestCtx(entry3, ArbitrationPriorityHigh, NoWaitAverse, false))
		m.restartEntryForTest(entry4, genTestCtx(entry4, ArbitrationPriorityLow, NoWaitAverse, false))
		m.restartEntryForTest(entry5, genTestCtx(entry5, ArbitrationPriorityLow, NoWaitAverse, false))

		m.setLimitForTest(baseQuotaUnit * 8)

		m.prepareAlloc(entry1, 1)
		m.prepareAlloc(entry3, baseQuotaUnit*4)
		m.prepareAlloc(entry4, baseQuotaUnit)
		m.prepareAlloc(entry5, baseQuotaUnit*2)

		require.True(t, m.tasksCountForTest() == 4)
		require.True(t, m.TaskNumByPattern() == NumByPattern{3, 0, 1, 0})
		require.True(t, m.runOneRound() == 4)

		alloced := entry1.arbitratorMu.quota + entry3.arbitratorMu.quota + entry4.arbitratorMu.quota + entry5.arbitratorMu.quota
		require.True(t, m.allocated() == alloced)
		require.True(t, m.TaskNumByPattern() == NumByPattern{0, 0, 0, 0})

		require.Equal(t, m.waitAlloc(entry1), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry3), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry4), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry5), ArbitrateOk)

		require.True(t, m.tasksCountForTest() == 0)

		m.checkTaskExec(pairSuccessFail{4, 0}, 0, NumByPattern{})
		m.prepareAlloc(entry2, baseQuotaUnit*3)
		require.True(t, m.tasksCountForTest() == 1)

		require.Equal(t, 0, m.runOneRound())
		require.True(t, len(actionCancel) == 2)
		require.True(t, actionCancel[entry4.pool.uid] == 1)
		require.True(t, actionCancel[entry5.pool.uid] == 1)
		m.checkTaskExec(pairSuccessFail{4, 0}, 0, NumByPattern{2, 0, 0, 0})
		require.True(t, m.TaskNumByPattern() == NumByPattern{0, 1, 0, 0})
		require.True(t, m.tasksCountForTest() == 1)
		require.True(t, m.underCancel.num == 2)
		require.True(t, entry4.arbitratorMu.underCancel.start)
		require.True(t, entry5.arbitratorMu.underCancel.start)

		m.ResetRootPoolByID(entry4.pool.uid, 0, false)

		selfCancelCh := make(chan struct{})
		m.restartEntryForTest(entry4, newCtxForTest(selfCancelCh, nil, entry4.ctx.memPriority, NoWaitAverse, false))
		m.prepareAlloc(entry4, baseQuotaUnit*1000)
		require.True(t, m.tasksCountForTest() == 2)

		require.True(t, !entry4.stateMu.stop.Load() && entry4.execState() != execStateIdle && entry4.stateMu.quotaToReclaim.Load() > 0)
		require.True(t, entry4.notRunning())

		require.True(t, m.cleanupMu.fifoTasks.size() == 1)
		require.True(t, m.cleanupMu.fifoTasks.front() == entry4)
		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.cleanupMu.fifoTasks.size() == 0)
		require.True(t, m.TaskNumByPattern() == NumByPattern{1, 1, 0, 0})
		require.True(t, !entry4.arbitratorMu.underCancel.start)
		require.True(t, !entry4.notRunning())
		require.True(t, !entry4.stateMu.stop.Load() && entry4.execState() != execStateIdle && entry4.stateMu.quotaToReclaim.Load() == 0)

		m.checkTaskExec(pairSuccessFail{4, 0}, 0, NumByPattern{2, 0, 0, 0})
		require.Equal(t, mapEntryWithMem{entries: map[uint64]*rootPoolEntry{
			entry5.pool.uid: entry5,
		}, num: 1}, m.underCancel)

		close(selfCancelCh)
		require.True(t, m.removeTask(entry4))
		require.True(t, m.TaskNumByPattern() == NumByPattern{0, 1, 0, 0})
		require.True(t, selfCancelCh == entry4.ctx.cancelCh)
		wg := newNotiferWrap(nil)
		oriFailCnt := m.execMetrics.Task.Fail
		go func() {
			defer wg.close()
			for oriFailCnt == atomic.LoadInt64(&m.execMetrics.Task.Fail) {
				runtime.Gosched()
			}
			entry4.windUp(0, ArbitrateOk)
		}()
		require.Equal(t, m.waitAlloc(entry4), ArbitrateFail) // cancel self
		wg.wait()
		m.checkTaskExec(pairSuccessFail{4, 1}, 0, NumByPattern{2, 0, 0, 0})
		m.removeEntryForTest(entry5)

		require.True(t, entry5.notRunning())
		require.True(t, entry5.stateMu.stop.Load())
		require.True(t, !entry5.arbitratorMu.destroyed)
		require.True(t, entry5.arbitratorMu.underCancel.start)
		require.True(t, entry5.arbitratorMu.quota != 0)

		require.True(t, m.cleanupMu.fifoTasks.size() == 1)
		require.Equal(t, 1, m.runOneRound())
		require.True(t, m.underCancel.num == 0)
		require.True(t, m.cleanupMu.fifoTasks.size() == 0)
		require.True(t, m.execMetrics.Cancel.PriorityMode == NumByPriority{2, 0, 0})
		require.True(t, m.execMetrics.Cancel.WaitAverse == 0)

		require.True(t, entry5.arbitratorMu.destroyed)
		require.True(t, !entry5.arbitratorMu.underCancel.start)
		require.True(t, entry5.arbitratorMu.quota == 0)

		require.True(t, m.allocated() == alloced)
		require.Equal(t, m.waitAlloc(entry2), ArbitrateOk)
		require.True(t, m.execMetrics.Task.pairSuccessFail == pairSuccessFail{5, 1})

		m.cleanupNotifer()
	}
}

func TestMemArbitrator(t *testing.T) {
	defer func() {
		mockNow = nil
	}()

	testState = t

	type MockHeap [4]int64 // alloc, heapInuse, totalAlloc, others
	type MockLogs struct {
		info, warn, error int
	}

	m := newMemArbitratorForTest(3, -1)
	newLimit := int64(1000000000)
	m.SetWorkMode(ArbitratorModeStandard)
	require.True(t, m.notifer.isAwake())
	m.waitNotiferForTest()
	require.Equal(t, m.WorkMode(), ArbitratorModeStandard)
	debugTime := time.Time{}

	mockNow = func() time.Time {
		return debugTime
	}

	{ // Standard mode
		require.Equal(t, m.Allocated(), int64(0)) // alloced size

		require.Equal(t, m.limit(), DefMaxLimit) // limit size

		require.False(t, m.notifer.isAwake())

		m.setLimitForTest(newLimit) // update limit

		require.Equal(t, m.limit(), newLimit)

		require.True(t, m.tasksCountForTest() == 0) // pending tasks count

		expectShardCount := 4 // next pow2 of 3
		require.Equal(t, len(m.entryMap.shards), expectShardCount)
		require.Equal(t, m.entryMap.shardsMask, uint64(expectShardCount-1))
		require.Equal(t, len(m.entryMap.shards), expectShardCount)

		for pio := minArbitrationPriority; pio < maxArbitrationPriority; pio++ {
			require.Equal(t, len(m.entryMap.quotaShards[pio]), m.entryMap.maxQuotaShardIndex)
		}

		require.Equal(t, m.runOneRound(), 0) // no task has been executed successfully
		m.checkEntryForTest()

		pool1AlignSize := int64(4)
		pool1 := newResourcePoolForTest("root1", pool1AlignSize)
		uid1 := pool1.UID()
		pb1 := pool1.CreateBudget()
		{ // no out-of-memory-action
			require.Equal(t, pool1.limit, DefMaxLimit)
			require.Equal(t, pool1.mu.budget.Used(), int64(0))
			require.Equal(t, pool1.mu.budget.Capacity(), int64(0))
			require.True(t, pool1.mu.budget.Pool() == nil)

			require.Error(t, pb1.Grow(1)) // can not grow
			require.NoError(t, pb1.Grow(0))
			require.True(t, m.getRootPoolEntry(uid1) == nil)

			require.False(t, m.notiferIsAwake())
			m.checkTaskExec(pairSuccessFail{}, 0, NumByPattern{})
		}

		entry1 := m.addRootPoolForTest(pool1, nil)
		{ // with out-of-memory-action
			m.checkEntryForTest(entry1)
			require.True(t, pool1.actions.OutOfCapacityActionCB != nil)
			require.False(t, m.notiferIsAwake())
			require.True(t, m.tasksCountForTest() == 0)

			wg := newNotiferWrap(m)
			go func() {
				defer wg.close()
				m.waitNotiferForTest()
				require.True(t, m.tasksCountForTest() == 1) // pool1 need to be arbitrated
				e := m.frontTaskEntryForTest()
				require.True(t, e == entry1)
				require.True(t, e == m.getRootPoolEntry(uid1)) // in status map
				m.checkEntryQuotaByPriority(e, ArbitrationPriorityMedium, 0)
				require.True(t, m.findTaskByMode(e, ArbitrationPriorityMedium, NoWaitAverse))

				require.True(t, m.getQuotaShard(0, pool1.ApproxCap()).getEntry(uid1) == nil) // not in budget map
				require.Equal(t, m.runOneRound(), 1)
			}()

			growSize := int64(1)
			require.NoError(t, pb1.Grow(growSize))
			wg.wait()

			require.True(t, m.Allocated() == pool1AlignSize)
			require.Equal(t, pb1.Used(), growSize)
			require.Equal(t, pb1.Capacity(), pool1AlignSize)
			require.Equal(t, pb1.available(), pool1AlignSize-growSize)
			require.Equal(t, pool1.Capacity(), pool1AlignSize)
			require.Equal(t, entry1.arbitratorMu.quota, pool1AlignSize)
			require.False(t, m.notiferIsAwake())
			require.True(t, m.execMetrics.Task.pairSuccessFail == pairSuccessFail{1, 0})
			require.Equal(t, execMetricsCancel{}, m.execMetrics.Cancel)
			m.cleanupNotifer()
			m.checkEntryForTest(entry1)

			wg = newNotiferWrap(m)
			go func() {
				defer wg.close()
				m.waitNotiferForTest()

				require.True(t, m.tasksCountForTest() == 1) // pool1 need to be arbitrated
				e := m.frontTaskEntryForTest()
				require.True(t, e == entry1)
				require.Equal(t, 1, m.runOneRound())
			}()

			growSize = pool1AlignSize - growSize + 1
			require.NoError(t, pb1.Grow(growSize))
			wg.wait()

			require.True(t, m.Allocated() == 2*pool1AlignSize)
			require.Equal(t, pb1.Used(), pool1AlignSize+1)
			require.Equal(t, pb1.Capacity(), pool1AlignSize*2)
			require.Equal(t, pool1.Capacity(), pool1AlignSize*2)
			require.Equal(t, entry1.arbitratorMu.quota, pool1AlignSize*2)
			require.False(t, m.notiferIsAwake())
			m.checkTaskExec(pairSuccessFail{2, 0}, 0, NumByPattern{})
			m.cleanupNotifer()
			m.checkEntryForTest(entry1)

			// check diff quota shard
			require.True(t, m.getQuotaShard(entry1.ctx.memPriority, entry1.arbitratorMu.quota) ==
				m.getQuotaShard(ArbitrationPriorityMedium, 0))

			wg = newNotiferWrap(m)
			go func() {
				defer wg.close()
				m.waitNotiferForTest()

				require.True(t, m.tasksCountForTest() == 1) // pool1 need to be arbitrated
				e := m.frontTaskEntryForTest()
				require.True(t, e == entry1)
				require.Equal(t, m.runOneRound(), 1)
			}()
			require.NoError(t, pb1.Grow(baseQuotaUnit))
			wg.wait()
			require.True(t, m.getQuotaShard(entry1.ctx.memPriority, entry1.arbitratorMu.quota) ==
				m.getQuotaShard(ArbitrationPriorityMedium, baseQuotaUnit*2-1))
			m.checkTaskExec(pairSuccessFail{3, 0}, 0, NumByPattern{})
			m.resetEntryForTest(entry1)
		}

		{ // illegal operation
			e0 := m.addEntryForTest(nil)
			m.prepareAlloc(e0, 1)
			require.True(t, m.RootPoolNum() == 2)
			require.True(t, m.runOneRound() == 1)
			require.True(t, m.waitAlloc(e0) == ArbitrateOk)
			require.True(t, e0.arbitratorMu.quota == 1)

			m.prepareAlloc(e0, 1)
			require.Equal(t, ArbitrationPriorityMedium, e0.taskMu.fifoByPriority.priority)
			m.resetRootPoolEntry(e0) // reset entry
			m.restartEntryForTest(e0, m.newCtxWithHelperForTest(ArbitrationPriorityLow, NoWaitAverse, false))
			require.Equal(t, ArbitrationPriorityLow, e0.ctx.memPriority)
			require.Equal(t, ArbitrationPriorityMedium, e0.taskMu.fifoByPriority.priority)
			m.doExecuteFirstTask()
			require.True(t, m.waitAlloc(e0) == ArbitrateOk)
			require.True(t, e0.arbitratorMu.quota == 2)
			require.True(t, e0.arbitratorMu.quota == e0.pool.Capacity()+e0.stateMu.quotaToReclaim.Load())
			require.True(t, m.cleanupMu.fifoTasks.size() == 1)

			m.resetRootPoolEntry(e0) // 1. reset entry
			m.removeEntryForTest(e0) // 2. delete entry
			require.True(t, m.cleanupMu.fifoTasks.size() == 3)
			m.prepareAlloc(e0, 1)                 // alloc for non-exist entry
			require.True(t, m.runOneRound() == 0) // no task
			require.True(t, m.waitAlloc(e0) == ArbitrateFail)
			require.True(t, m.getRootPoolEntry(e0.pool.uid) == nil)
			require.True(t, m.RootPoolNum() == 1)
			m.prepareAlloc(e0, 1)                 // alloc for non-exist entry
			require.True(t, m.runOneRound() == 1) // wind up non-exist entry with fail
			require.True(t, m.waitAlloc(e0) == ArbitrateFail)

			e1 := m.addEntryForTest(nil)
			m.removeEntryForTest(e1)                           // 1. delete entry
			require.False(t, m.resetRootPoolEntry(e1))         // 2. reset entry failed
			require.True(t, m.cleanupMu.fifoTasks.size() == 1) //
			m.prepareAlloc(e1, 1)
			m.doExecuteFirstTask()
			require.True(t, m.waitAlloc(e1) == ArbitrateOk) // success
			m.doExecuteCleanupTasks()
		}

		{ // async run
			m.restartEntryForTest(entry1, entry1.ctx.Load())
			b := pool1.CreateBudget()
			require.True(t, m.asyncRun(time.Hour))
			require.False(t, m.asyncRun(time.Hour))
			require.NoError(t, b.Grow(baseQuotaUnit))
			require.True(t, m.stop())
			require.False(t, m.stop())
			require.True(t, !m.controlMu.running.Load())
			require.Equal(t, m.Allocated(), b.cap)
			require.Equal(t, m.limit(), newLimit)
			m.checkEntryForTest(entry1)
			m.resetEntryForTest(entry1)

			m.resetExecMetricsForTest()
			cancel := make(chan struct{})
			m.restartEntryForTest(entry1, newCtxForTest(cancel, nil, ArbitrationPriorityMedium, NoWaitAverse, false))
			wg := newNotiferWrap(m)
			go func() {
				defer wg.close()
				m.waitNotiferForTest()

				require.True(t, m.tasksCountForTest() == 1)
				require.True(t, m.frontTaskEntryForTest() == entry1)
				close(cancel)
			}()
			// blocking grow with cancel
			require.Equal(t, errArbitrateFailError, b.Grow(baseQuotaUnit))
			wg.wait()
			require.True(t, m.tasksCountForTest() == 0)
			m.checkTaskExec(pairSuccessFail{0, 1}, 0, NumByPattern{})
			m.resetEntryForTest(entry1)

			cancel = make(chan struct{})
			m.restartEntryForTest(entry1, newCtxForTest(cancel, nil, ArbitrationPriorityMedium, NoWaitAverse, false))
			wg = newNotiferWrap(m)
			go func() {
				defer wg.close()
				m.waitNotiferForTest()

				require.True(t, m.tasksCountForTest() == 1)
				require.True(t, m.frontTaskEntryForTest() == entry1)
				require.Equal(t, 0, m.runOneRound())
			}()
			require.Equal(t, errArbitrateFailError, b.Grow(newLimit+1))
			wg.wait()
			require.True(t, m.tasksCountForTest() == 0)
			m.checkTaskExec(pairSuccessFail{0, 2}, 1, NumByPattern{})
		}

		m.deleteEntryForTest(entry1)
	}

	// Priority mode
	m.SetWorkMode(ArbitratorModePriority)
	require.Equal(t, m.WorkMode(), ArbitratorModePriority)
	{ // test panic
		{
			pool := NewResourcePoolDefault("?", 1)
			pool.Start(nil, 1)
			require.Equal(t, pool.reserved, int64(1))
			require.PanicsWithError(t, "?: has 1 reserved budget left", func() { m.addRootPoolForTest(pool, nil) })
		}
		{
			pool := NewResourcePoolDefault("?", 1)
			pool.forceAddCap(1)
			require.True(t, pool.ApproxCap() != 0)
			require.PanicsWithError(t, "?: has 1 bytes budget left", func() { m.addRootPoolForTest(pool, nil) })
		}
		{
			p1 := NewResourcePoolDefault("p1", 1)
			p2 := NewResourcePoolDefault("p2", 1)
			p3 := NewResourcePoolDefault("p3", 1)
			p2.uid = p1.uid
			e1 := m.addRootPoolForTest(p1, nil)
			m.checkEntryForTest(e1)
			require.PanicsWithError(t, "p2: already exists", func() { m.addRootPoolForTest(p2, nil) })
			p3.StartNoReserved(p2)
			require.PanicsWithError(t, "p3: already started with pool p2", func() { m.addRootPoolForTest(p3, nil) })
			m.checkEntryForTest(e1)
			m.deleteEntryForTest(m.getRootPoolEntry(p1.uid))
		}
		m.checkEntryForTest()
	}
	{ // prefer privileged budget under Priority mode
		newLimit = 100
		m.resetExecMetricsForTest()
		m.setLimitForTest(newLimit)
		e1 := m.newPoolWithHelperForTest("e1", ArbitrationPriorityLow, NoWaitAverse, true)
		e1.ctx.Load().arbitrateHelper = nil
		require.True(t, m.privilegedEntry == nil)
		require.True(t, e1.ctx.preferPrivilege)

		reqQuota := newLimit + 1

		m.prepareAlloc(e1, reqQuota)
		require.True(t, m.findTaskByMode(e1, ArbitrationPriorityLow, NoWaitAverse))
		require.True(t, m.runOneRound() == 1)
		require.Equal(t, m.waitAlloc(e1), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{1, 0}, 0, NumByPattern{})
		m.checkEntryQuotaByPriority(e1, ArbitrationPriorityLow, reqQuota)
		require.True(t, m.privilegedEntry == e1)

		e2 := m.newPoolWithHelperForTest("e2", ArbitrationPriorityHigh, true, true)
		e2.ctx.Load().arbitrateHelper = nil

		require.True(t, e2.ctx.Load().preferPrivilege)
		require.False(t, e2.ctx.preferPrivilege)
		m.prepareAlloc(e2, reqQuota)
		require.True(t, m.findTaskByMode(e2, ArbitrationPriorityHigh, true))
		require.Equal(t, 0, m.runOneRound())
		require.Equal(t, m.waitAlloc(e2), ArbitrateFail)
		m.checkTaskExec(pairSuccessFail{1, 1}, 0, NumByPattern{0, 0, 0, 1})
		m.checkEntryForTest(e1, e2)
		require.True(t, m.privilegedEntry == e1)

		e3 := m.newPoolWithHelperForTest("e3", ArbitrationPriorityLow, NoWaitAverse, true)
		e3.ctx.Load().arbitrateHelper = nil

		require.True(t, e3.ctx.preferPrivilege)
		m.prepareAlloc(e3, reqQuota)
		require.True(t, m.runOneRound() == 0)
		require.True(t, m.tasksCountForTest() == 1)

		m.prepareAlloc(e1, reqQuota)
		require.True(t, m.tasksCountForTest() == 2)
		require.True(t, m.runOneRound() == 1)
		require.True(t, m.tasksCountForTest() == 1)
		require.Equal(t, m.waitAlloc(e1), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{2, 1}, 0, NumByPattern{0, 0, 0, 1})

		m.resetRootPoolEntry(e1) // to be handled by arbitrator later
		m.restartEntryForTest(e1, e1.ctx.Load())
		m.prepareAlloc(e1, reqQuota)

		require.True(t, m.tasksCountForTest() == 2)
		require.True(t, m.privilegedEntry == e1)
		require.True(t, m.runOneRound() == 1)
		require.Equal(t, m.waitAlloc(e3), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{3, 1}, 0, NumByPattern{0, 0, 0, 1})
		require.True(t, m.privilegedEntry == e3)
		m.setLimitForTest((m.WaitingAllocSize() + m.allocated()))
		require.True(t, m.runOneRound() == 1)
		require.Equal(t, m.waitAlloc(e1), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{4, 1}, 0, NumByPattern{0, 0, 0, 1})
		require.True(t, m.privilegedEntry == e3)
		m.checkEntryForTest(e1, e2, e3)
		m.deleteEntryForTest(e1, e2, e3)
	}

	{ // test soft-limit & limit
		var x = (1e9)
		require.False(t, m.SetLimit(math.MaxUint64))
		require.False(t, m.notiferIsAwake())
		require.True(t, m.SetLimit(uint64(x)))
		require.True(t, m.notiferIsAwake())
		require.Equal(t, m.mu.limit, int64(x))
		require.Equal(t, m.mu.threshold.oomRisk, int64(x*defOOMRiskRatio))
		require.Equal(t, m.mu.threshold.risk, int64(x*defMemRiskRatio))
		require.Equal(t, m.mu.softLimit.size, m.mu.threshold.oomRisk)
		require.Equal(t, m.mu.limit, int64(x))
		m.cleanupNotifer()
		require.True(t, m.SetLimit(uint64(DefMaxLimit)+1))
		require.True(t, m.notiferIsAwake())
		require.Equal(t, m.mu.limit, DefMaxLimit)
		require.Equal(t, m.mu.threshold.oomRisk, int64(float64(DefMaxLimit)*defOOMRiskRatio))
		require.Equal(t, m.mu.threshold.risk, int64(float64(DefMaxLimit)*defMemRiskRatio))
		require.True(t, m.mu.softLimit.specified.size == 0)
		require.Equal(t, m.mu.softLimit.size, m.mu.threshold.oomRisk)
		require.True(t, m.mu.softLimit.mode == SoftLimitModeDisable)

		m.SetSoftLimit(1, 0, SoftLimitModeSpecified) // specified bytes
		require.True(t, m.mu.softLimit.specified.size > 0)
		require.True(t, m.mu.softLimit.mode == SoftLimitModeSpecified)
		require.Equal(t, m.mu.softLimit.size, int64(1))
		require.True(t, m.SetLimit(100))
		require.Equal(t, m.mu.softLimit.size, int64(1))

		sortLimitRate := 0.8
		m.SetSoftLimit(0, sortLimitRate, SoftLimitModeSpecified) // specified rate of limit
		require.True(t, m.mu.softLimit.specified.size == 0)
		require.True(t, m.mu.softLimit.mode == SoftLimitModeSpecified)
		require.True(t, m.mu.softLimit.specified.ratio == int64(sortLimitRate*1000))
		require.Equal(t, m.mu.softLimit.size, int64(sortLimitRate*float64(m.mu.limit)))
		require.True(t, m.SetLimit(200))
		require.Equal(t, m.mu.softLimit.size, int64(sortLimitRate*200))

		m.SetSoftLimit(0, 0, SoftLimitModeDisable)
		require.Equal(t, m.mu.softLimit.size, m.mu.threshold.oomRisk)
		require.True(t, m.mu.softLimit.mode == SoftLimitModeDisable)
		require.True(t, m.SetLimit(100))
		require.Equal(t, m.mu.softLimit.size, m.mu.threshold.oomRisk)

		m.SetSoftLimit(0, 0, SoftLimitModeAuto)
		require.Equal(t, m.mu.softLimit.size, m.mu.threshold.oomRisk)
		require.True(t, m.mu.softLimit.mode == SoftLimitModeAuto)
		require.True(t, m.SetLimit(200))
		require.Equal(t, m.mu.softLimit.size, m.mu.threshold.oomRisk)
	}
	{ // test out-of-control
		m.SetSoftLimit(0, 0, SoftLimitModeDisable)

		require.True(t, m.awaitFreePoolCap() == 0)
		require.True(t, m.allocated() == 0)

		eleSize := m.awaitFree.pool.allocAlignSize + 1
		budgetsNum := int64(len(m.awaitFree.budget.shards))
		usedHeap := eleSize * budgetsNum
		for i := range m.awaitFree.budget.shards {
			b := &m.awaitFree.budget.shards[i]
			require.NoError(t, b.ConsumeQuota(m.approxUnixTimeSec(), eleSize))
		}
		require.True(t, m.awaitFreePoolUsed().trackedHeap == 0)
		for i := range m.awaitFree.budget.shards {
			b := &m.awaitFree.budget.shards[i]
			b.ReportHeapInuse(eleSize)
		}

		expect := awaitFreePoolExecMetrics{pairSuccessFail{budgetsNum, 0}, 0, 0}
		require.True(t, m.execMetrics.AwaitFree == expect)

		expect.Fail++
		{
			err := m.awaitFree.budget.shards[0].Reserve(m.limit())
			require.Error(t, err)
		}
		m.checkAwaitFree()
		require.True(t, m.execMetrics.AwaitFree == expect)

		require.True(t, m.awaitFreePoolUsed().trackedHeap == usedHeap)
		require.True(t, m.awaitFreePoolCap() == m.awaitFree.pool.roundSize(eleSize)*budgetsNum)
		{
			ori := nowUnixMilli() - defTrackMemStatsDurMilli
			m.avoidance.heapTracked.lastUpdateUtimeMilli.Store(ori)
			require.True(t, m.tryUpdateTrackedMemStats(nowUnixMilli()))
			require.True(t, m.avoidance.heapTracked.lastUpdateUtimeMilli.Load() != ori)
		}

		require.Equal(t, m.avoidance.heapTracked.Load(), usedHeap)
		require.True(t, m.buffer.size.Load() == 0)
		require.True(t, m.avoidance.size.Load() == 0)

		e1Men := int64(13)
		e1 := m.newPoolWithHelperForTest(
			"e1",
			ArbitrationPriorityMedium, NoWaitAverse, false)
		e1.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
			return e1Men
		}
		e2 := m.addEntryForTest(nil)
		e3 := m.addEntryForTest(newDefCtxForTest(ArbitrationPriorityMedium))
		m.updateTrackedHeapStats()
		usedHeap += e1Men
		require.Equal(t, m.avoidance.heapTracked.Load(), usedHeap)
		require.True(t, m.buffer.size.Load() == e1Men)
		require.True(t, m.avoidance.size.Load() == 0)

		free := int64(1024)
		e1Men = 17
		m.setMemStatsForTest(usedHeap+1, usedHeap+100, usedHeap+free, 67)
		require.True(t, m.heapController.heapInuse.Load() == usedHeap+100)
		require.True(t, m.heapController.heapAlloc.Load() == usedHeap+1)
		require.True(t, m.heapController.heapTotalFree.Load() == free-1)
		require.True(t, m.heapController.memInuse.Load() == usedHeap+100+67)
		require.Equal(t, m.avoidance.heapTracked.Load(), usedHeap)
		require.True(t, m.buffer.size.Load() == 13) // no update
		require.True(t, m.avoidance.size.Load() == m.heapController.heapAlloc.Load()+m.heapController.memOffHeap.Load()-m.avoidance.heapTracked.Load())
		require.True(t, m.avoidance.size.Load() > m.mu.limit-m.mu.softLimit.size)

		m.updateTrackedHeapStats()
		require.True(t, m.buffer.size.Load() == e1Men)
		e1Men = 3
		m.updateTrackedHeapStats()
		require.True(t, m.buffer.size.Load() == 17) // no update to smaller size

		now := time.Now()
		for i := range m.awaitFree.budget.shards {
			b := &m.awaitFree.budget.shards[i]
			if i%2 == 0 {
				b.Used.Store(0)
				if i == 0 {
					b.setLastUsedTimeSec(now.Unix() - (defAwaitFreePoolShrinkDurMilli/kilo - 1))
				} else {
					b.setLastUsedTimeSec(0)
				}
			} else {
				b.Used.Store(1)
			}
		}
		m.cleanupNotifer()
		require.False(t, m.notiferIsAwake())
		{
			ori := now.UnixMilli() - defAwaitFreePoolShrinkDurMilli
			m.awaitFree.lastShrinkUtimeMilli.Store(ori)
			require.True(t, m.tryShrinkAwaitFreePool(0, now.UnixMilli()))
			require.True(t, m.ableToGC())
			require.True(t, m.awaitFree.lastShrinkUtimeMilli.Load() != ori)
		}
		require.True(t, m.notiferIsAwake())
		expect.Shrink++
		require.True(t, m.execMetrics.AwaitFree == expect)
		m.checkAwaitFree()

		for i := range m.awaitFree.budget.shards {
			b := &m.awaitFree.budget.shards[i]
			if i%2 == 0 {
				require.True(t, b.Used.Load() == 0)
				if i == 0 {
					require.True(t, b.Capacity != 0)
				} else {
					require.True(t, b.Capacity == 0)
				}
			} else {
				require.True(t, b.Used.Load() == 1)
			}
		}
		require.True(t, m.awaitFreePoolCap() == m.allocated())
		m.setBufferSize(0)
		m.checkEntryForTest(e1, e2, e3)
		m.deleteEntryForTest(e1, e2, e3)
		m.checkEntryForTest()
		m.resetAwaitFreeForTest()
		m.setMemStatsForTest(0, 0, 0, 0)
		require.True(t, m.allocated() == 0)
	}

	{ // test calc buffer
		m.resetExecMetricsForTest()
		m.tryToUpdateBuffer(2, 3, defUpdateBufferTimeAlignSec)
		require.Equal(t, m.buffer.size.Load(), int64(2))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(3))

		m.tryToUpdateBuffer(1, 1, defUpdateBufferTimeAlignSec)
		require.Equal(t, m.buffer.size.Load(), int64(2))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(3))

		m.tryToUpdateBuffer(4, 1, defUpdateBufferTimeAlignSec)
		require.Equal(t, m.buffer.size.Load(), int64(4))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(3))

		m.tryToUpdateBuffer(1, 6, defUpdateBufferTimeAlignSec)
		require.Equal(t, m.buffer.size.Load(), int64(4))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(6))

		m.tryToUpdateBuffer(1, 1, defUpdateBufferTimeAlignSec*(defRedundancy))
		require.Equal(t, m.buffer.size.Load(), int64(4))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(6))

		m.tryToUpdateBuffer(3, 4, defUpdateBufferTimeAlignSec*(defRedundancy+1))
		require.Equal(t, m.buffer.size.Load(), int64(3))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(4))

		m.tryToUpdateBuffer(1, 1, defUpdateBufferTimeAlignSec*(defRedundancy+1))
		require.Equal(t, m.buffer.size.Load(), int64(3))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(4))

		m.setBufferSize(0)
		m.buffer.quotaLimit.Store(0)

		m.SetLimit(10000)
		require.Equal(t, PoolAllocProfile{10, 20, 100}, m.poolAllocStats.PoolAllocProfile)

		digestID1 := HashStr("test")
		digestID2 := digestID1 + 1
		{
			_, ok := m.GetDigestProfileCache(digestID1, 1)
			require.False(t, ok)
		}
		require.True(t, m.digestProfileCache.num.Load() == 0)
		m.UpdateDigestProfileCache(digestID1, 1009, 0)
		require.True(t, m.digestProfileCache.num.Load() == 1)
		shard := &m.digestProfileCache.shards[digestID1&m.digestProfileCache.shardsMask]
		{
			a, ok := m.GetDigestProfileCache(digestID1, 2)
			require.True(t, ok)
			require.True(t, a == 1009)

			require.True(t, shard.num.Load() == 1)
			v, f := shard.Load(digestID1)
			require.True(t, f)
			require.True(t, v.(*digestProfile).lastFetchUtimeSec.Load() == 2)
		}
		m.UpdateDigestProfileCache(digestID2, 7, 0)
		{
			a, ok := m.GetDigestProfileCache(digestID2, 5)
			require.True(t, ok)
			require.True(t, a == 7)

			require.True(t, m.digestProfileCache.num.Load() == 2)
			shard := &m.digestProfileCache.shards[digestID2&m.digestProfileCache.shardsMask]
			require.True(t, shard.num.Load() == 1)
			v, f := shard.Load(digestID2)
			require.True(t, f)
			require.True(t, v.(*digestProfile).lastFetchUtimeSec.Load() == 5)
		}
		m.UpdateDigestProfileCache(digestID1, 107, 0)
		{
			a, ok := m.GetDigestProfileCache(digestID1, 3)
			require.True(t, ok)
			require.True(t, a == 1009)

			v, f := shard.Load(digestID1)
			require.True(t, f)
			require.True(t, v.(*digestProfile).lastFetchUtimeSec.Load() == 3)
		}
		m.UpdateDigestProfileCache(digestID1, 107, defUpdateBufferTimeAlignSec)
		{
			a, ok := m.GetDigestProfileCache(digestID1, 4)
			require.True(t, ok)
			require.True(t, a == 1009)
		}
		m.UpdateDigestProfileCache(digestID1, 107, defUpdateBufferTimeAlignSec*2)
		{
			a, ok := m.GetDigestProfileCache(digestID1, 4)
			require.True(t, ok)
			require.True(t, a == 107)
		}
		m.cleanDigestProfileForTest()

		m.SetLimit(5000 * 1000)
		require.Equal(t, PoolAllocProfile{5000, 10000, 50000}, m.poolAllocStats.PoolAllocProfile)

		type dataType struct {
			v     int64
			utime int64
			cnt   int64
		}

		testNow := int64(defDigestProfileMemTimeoutSec + 1)
		uid := uint64(107)

		data := []dataType{
			{5003, 0, 2},
			{7, 0, 2}, // small
			{4099, testNow - defDigestProfileSmallMemTimeoutSec, 2}, // small
			{10003, testNow, 2},
			{5, testNow, 2},    // small
			{4111, testNow, 2}, // small
		}
		for _, d := range data {
			for i := int64(0); i < d.cnt; i++ {
				m.UpdateDigestProfileCache(uid, d.v, d.utime)
				uid++
			}
		}
		require.True(t, m.digestProfileCache.num.Load() == 12)
		require.True(t, m.shrinkDigestProfile(testNow, defMax, 0) == 0)
		require.True(t, m.shrinkDigestProfile(testNow, 11, 9) == 4)
		require.True(t, m.digestProfileCache.num.Load() == 8)
		require.True(t, m.shrinkDigestProfile(testNow, 0, 4) == 4)

		m.cleanDigestProfileForTest()
	}

	{ // test gc when quota not available
		limit := int64(1000)
		heap := int64(500)
		buffer := int64(100) // original buffer size
		alloc := int64(500)

		m.SetLimit(uint64(limit))
		m.heapController.heapAlloc.Store(heap)
		m.heapController.heapInuse.Store(heap)
		m.setBufferSize(buffer)
		m.heapController.lastGC.utime.Store(0)
		m.heapController.lastGC.heapAlloc.Store(0)
		require.True(t, m.execMetrics.Action.GC == 0)
		require.True(t, m.isMemSafe())

		expectBufferSize := int64(-1)
		gc := 0
		m.actions.GC = func() {
			require.True(t, m.buffer.size.Load() == expectBufferSize)
			gc++
		}
		gcUT := now().UnixNano()
		m.actions.UpdateRuntimeMemStats = func() {
			m.SetRuntimeMemStats(RuntimeMemStats{
				HeapAlloc:  heap,
				HeapInuse:  heap + 100,
				MemOffHeap: 3,
				LastGC:     gcUT,
			})
		}
		m.mu.lastGC = m.mu.released - uint64(m.poolAllocStats.SmallPoolLimit)
		e1 := m.newPoolWithHelperForTest("e1", ArbitrationPriorityMedium, NoWaitAverse, false)
		e1MemUsed := int64(0)
		e1.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
			return e1MemUsed
		}
		m.prepareAlloc(e1, alloc)
		expectBufferSize = 100 // original buffer size
		require.True(t, m.runOneRound() == 0)
		require.True(t, m.execMetrics.Action.GC == 1)
		require.True(t, gc == 1)
		require.True(t, m.heapController.heapAlloc.Load() == heap)
		require.True(t, m.heapController.heapInuse.Load() == heap+100)
		require.True(t, m.heapController.lastGC.heapAlloc.Load() == heap)
		require.True(t, m.heapController.lastGC.utime.Load() == gcUT)

		// unable to trigger Runtime GC
		require.True(t, m.runOneRound() == 0)
		require.True(t, m.execMetrics.Action.GC == 1)
		require.True(t, gc == 1)
		require.True(t, m.heapController.heapAlloc.Load() == heap)
		require.True(t, m.heapController.heapInuse.Load() == heap+100)
		require.True(t, m.heapController.lastGC.heapAlloc.Load() == heap)
		require.True(t, m.heapController.lastGC.utime.Load() == gcUT)

		heap = 450 // -50
		lastAlloc := m.heapController.lastGC.heapAlloc.Load()
		m.mu.lastGC = m.mu.released - uint64(m.poolAllocStats.SmallPoolLimit)
		require.True(t, m.runOneRound() == 0)
		require.True(t, m.execMetrics.Action.GC == 2)
		require.True(t, gc == 2)
		require.True(t, m.heapController.heapAlloc.Load() == heap) // heap -= 50
		require.True(t, m.heapController.heapInuse.Load() == heap+100)
		require.True(t, m.heapController.lastGC.heapAlloc.Load() == lastAlloc) // no gc
		require.True(t, m.heapController.lastGC.utime.Load() == gcUT)
		require.True(t, m.avoidance.size.Load() == heap+3)

		heap = 390 // -60
		gcUT = now().UnixNano()
		m.mu.lastGC = m.mu.released - uint64(m.poolAllocStats.SmallPoolLimit)
		require.True(t, m.runOneRound() == 1)
		require.True(t, m.waitAlloc(e1) == ArbitrateOk)
		require.True(t, m.execMetrics.Action.GC == 3)
		require.True(t, gc == 3)
		require.True(t, m.heapController.heapAlloc.Load() == heap)
		require.True(t, m.heapController.heapInuse.Load() == heap+100)
		require.True(t, m.heapController.lastGC.heapAlloc.Load() == heap)
		require.True(t, m.heapController.lastGC.utime.Load() == gcUT)
		require.True(t, m.avoidance.heapTracked.Load() == 0)
		require.True(t, m.avoidance.size.Load() == heap+3)

		m.prepareAlloc(e1, 50)
		require.True(t, m.runOneRound() == 0)
		require.True(t, m.execMetrics.Action.GC == 3)
		require.True(t, gc == 3)
		require.True(t, m.avoidance.size.Load() == heap+3)
		require.True(t, m.buffer.size.Load() == 100)

		gcUT = now().UnixNano()
		e1MemUsed = 150
		m.mu.lastGC = m.mu.released - uint64(m.poolAllocStats.SmallPoolLimit)
		require.True(t, e1MemUsed > m.buffer.size.Load())
		expectBufferSize = e1MemUsed
		require.True(t, m.runOneRound() == 1)
		require.True(t, m.waitAlloc(e1) == ArbitrateOk)

		require.True(t, m.execMetrics.Action.GC == 4)
		require.True(t, gc == 4)
		require.True(t, m.heapController.heapAlloc.Load() == heap)
		require.True(t, m.heapController.heapInuse.Load() == heap+100)
		require.True(t, m.heapController.lastGC.heapAlloc.Load() == heap)
		require.True(t, m.heapController.lastGC.utime.Load() == gcUT)
		require.True(t, m.avoidance.heapTracked.Load() == e1MemUsed)
		require.True(t, m.avoidance.size.Load() == heap+3-e1MemUsed)

		m.deleteEntryForTest(e1)
		m.checkEntryForTest()

		m.updateTrackedHeapStats()
		m.setMemStatsForTest(0, 0, 0, 0)
	}
	{ // test mem risk
		type MockMetrcis struct {
			execMetricsCounter
			logs MockLogs
		}
		newLimit := int64(100000)
		const heapInuseRateMilli int64 = defOOMRiskRatio * kilo
		mockHeap := MockHeap{multiRatio(newLimit, heapInuseRateMilli), multiRatio(newLimit, heapInuseRateMilli), 0, 0}
		tMetrics := MockMetrcis{}
		m.actions.UpdateRuntimeMemStats = func() {
			tMetrics.Action.UpdateRuntimeMemStats++
			m.setMemStatsForTest(mockHeap[0], mockHeap[1], mockHeap[2], mockHeap[3])
		}
		m.actions.GC = func() {
			tMetrics.Action.GC++
		}
		m.actions.Error = func(format string, args ...zap.Field) {
			tMetrics.logs.error++
		}
		m.actions.Warn = func(format string, args ...zap.Field) {
			tMetrics.logs.warn++
		}
		m.actions.Info = func(format string, args ...zap.Field) {
			tMetrics.logs.info++
		}

		m.resetExecMetricsForTest()
		require.True(t, m.heapController.heapAlloc.Load() == 0)
		require.True(t, m.heapController.heapInuse.Load() == 0)
		require.True(t, m.heapController.heapTotalFree.Load() == 0)
		require.True(t, m.avoidance.heapTracked.Load() == 0)
		require.True(t, m.avoidance.size.Load() == m.limit()-m.softLimit())
		require.True(t, m.avoidance.memMagnif.ratio.Load() == 0)
		m.SetLimit(uint64(newLimit))
		m.SetSoftLimit(0, 0, SoftLimitModeDisable)
		require.True(t, m.isMemSafe())
		e1 := m.newPoolWithHelperForTest("e1", ArbitrationPriorityLow, NoWaitAverse, true)
		e2 := m.newPoolWithHelperForTest("e2", ArbitrationPriorityLow, NoWaitAverse, true)
		e3 := m.newPoolWithHelperForTest("e3", ArbitrationPriorityHigh, NoWaitAverse, true)
		e4 := m.newPoolWithHelperForTest("e4", ArbitrationPriorityLow, NoWaitAverse, true)
		e5 := m.newPoolWithHelperForTest("e5", ArbitrationPriorityLow, NoWaitAverse, true)
		{
			m.setUnixTimeSec(0)
			require.True(t, m.ConsumeQuotaFromAwaitFreePool(0, 1000))
			{
				m.prepareAlloc(e1, 1000)
				m.prepareAlloc(e2, 5000)
				m.prepareAlloc(e3, 9000)
				require.True(t, m.runOneRound() == 3)
				require.True(t, m.waitAlloc(e1) == ArbitrateOk)
				require.True(t, m.waitAlloc(e2) == ArbitrateOk)
				require.True(t, m.waitAlloc(e3) == ArbitrateOk)
			}
			require.True(t, m.allocated() == 16000)
			require.True(t, m.RootPoolNum() == 5)
			require.True(t, e1.arbitratorMu.quotaShard != e2.arbitratorMu.quotaShard)
			require.True(t, e1.arbitratorMu.quotaShard != e3.arbitratorMu.quotaShard)
			require.True(t, e2.arbitratorMu.quotaShard != e3.arbitratorMu.quotaShard)
			require.True(t, m.execMetrics.Task.pairSuccessFail == pairSuccessFail{3, 0})

			e1.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
				return 1009
			}
		}
		oriReleased := m.mu.released
		oriAllocated := m.allocated()
		m.refreshRuntimeMemStats()
		require.Equal(t, m.mu.released, oriReleased+1000)
		require.Equal(t, m.allocated(), oriAllocated-1000)
		require.True(t, m.softLimit() == multiRatio(newLimit, heapInuseRateMilli))
		require.True(t, m.avoidance.size.Load() == multiRatio(newLimit, heapInuseRateMilli)) // no tracked size
		require.False(t, m.isMemSafe())                                                      // >= 0.95

		mockHeap = MockHeap{multiRatio(newLimit, 900), multiRatio(newLimit, heapInuseRateMilli), multiRatio(newLimit, 900) + 500, multiRatio(newLimit, 50)}
		m.refreshRuntimeMemStats()
		require.True(t, m.heapController.heapTotalFree.Load() == 500)
		require.False(t, m.isMemSafe()) // >= 0.95

		require.True(t, m.execMetrics.Risk == execMetricsRisk{})
		require.False(t, m.atMemRisk())
		require.True(t, m.heapController.memRisk.startTime.t.IsZero())
		require.True(t, m.heapController.memRisk.lastMemStats.startTime.IsZero())
		require.True(t, m.heapController.memRisk.lastMemStats.heapTotalFree == 0)
		require.True(t, m.minHeapFreeBPS() != 0)
		require.True(t, m.avoidance.memMagnif.ratio.Load() == 0)
		m.resetExecMetricsForTest()

		tMetrics = MockMetrcis{}

		m.heapController.memStateRecorder.RecordMemState.(*memStateRecorderForTest).store = func(rmsv *RuntimeMemStateV1) error {
			tMetrics.Action.RecordMemState.Fail++
			return errors.New("test")
		}
		m.heapController.memStateRecorder.lastMemState.Store(&RuntimeMemStateV1{
			Version: 1,
			Magnif:  1111,
		})
		m.SetSoftLimit(0, 0, SoftLimitModeAuto)
		m.setMinHeapFreeBPS(2) // 2 B/s
		debugTime = time.Now()
		startTime := debugTime
		require.True(t, m.minHeapFreeBPS() == 2)
		{
			require.True(t, m.runOneRound() == -2)
			require.True(t, m.atMemRisk())
			require.True(t, m.heapController.memRisk.startTime.t.Equal(startTime))
			require.True(t, m.heapController.memRisk.lastMemStats.startTime.Equal(startTime))
			require.True(t, m.heapController.memRisk.lastMemStats.heapTotalFree == 500)
			lastRiskMemState := m.lastMemState()
			require.True(t, lastRiskMemState != nil)
			require.Equal(t, RuntimeMemStateV1{
				Version: 1,
				LastRisk: LastRisk{
					HeapAlloc:  multiRatio(newLimit, 900),
					QuotaAlloc: 15000,
				},
				Magnif:        6100, // 90000 / 15000 + 0.1
				PoolMediumCap: 0,
			}, *lastRiskMemState)
			require.True(t, m.avoidance.memMagnif.ratio.Load() == lastRiskMemState.Magnif)
			require.True(t, m.avoidance.heapTracked.Load() == 0)
			require.True(t, m.avoidance.size.Load() == 95000) // heapinuse - 0
			require.Equal(t, MockMetrcis{
				execMetricsCounter{Action: execMetricsAction{GC: 1, UpdateRuntimeMemStats: 1, RecordMemState: pairSuccessFail{0, 1}}},
				MockLogs{0, 1, 1},
			}, tMetrics) // gc when start oom check
			require.Equal(t, m.execMetrics.Action, tMetrics.Action)
			require.True(t, m.execMetrics.Risk == execMetricsRisk{Mem: 1, OOM: 0})
		}

		{ // wait to check oom risk
			expectMetrics := m.execMetrics
			expectMetrics.Action.UpdateRuntimeMemStats++
			lastRiskMemState := *m.lastMemState()
			m.heapController.memStateRecorder.RecordMemState.(*memStateRecorderForTest).store = func(rmsv *RuntimeMemStateV1) error {
				tMetrics.Action.RecordMemState.Succ++
				return nil
			}
			debugTime = m.heapController.memRisk.lastMemStats.startTime.Add(defHeapReclaimCheckDuration - 1)
			require.True(t, m.runOneRound() == -2)
			require.True(t, m.atMemRisk())
			require.True(t, m.heapController.memRisk.startTime.t.Equal(startTime))
			require.True(t, m.heapController.memRisk.lastMemStats.startTime.Equal(startTime))
			require.True(t, m.heapController.memRisk.lastMemStats.heapTotalFree == 500)
			require.True(t, m.avoidance.memMagnif.ratio.Load() == lastRiskMemState.Magnif)
			require.True(t, *m.lastMemState() == lastRiskMemState)
			require.Equal(t, expectMetrics, m.execMetrics)
			require.Equal(t, MockMetrcis{
				execMetricsCounter{Action: expectMetrics.Action},
				MockLogs{0, 1, 1},
			}, tMetrics)
		}
		{
			require.True(t, m.avoidance.heapTracked.Load() == 0)
			expectMetrics := m.execMetrics
			m.mu.lastGC = m.mu.released - uint64(m.poolAllocStats.SmallPoolLimit)
			expectMetrics.Action.GC++
			expectMetrics.Action.UpdateRuntimeMemStats++

			require.True(t, m.runOneRound() == -2)
			require.True(t, expectMetrics == m.execMetrics)
			require.Equal(t, MockMetrcis{
				execMetricsCounter{Action: expectMetrics.Action},
				MockLogs{0, 1, 1},
			}, tMetrics)
			require.True(t, m.avoidance.heapTracked.Load() == 1009)
		}

		{ // next round of oom check
			lastRiskMemState := *m.lastMemState()
			debugTime = m.heapController.memRisk.lastMemStats.startTime.Add(defHeapReclaimCheckDuration)
			mockHeap = MockHeap{multiRatio(newLimit, 900), multiRatio(newLimit, heapInuseRateMilli), multiRatio(newLimit, 900) + 500 + 2, multiRatio(newLimit, 50)}
			require.True(t, m.runOneRound() == -2)
			require.True(t, m.atMemRisk())
			require.True(t, m.heapController.memRisk.startTime.t.Equal(startTime))
			require.True(t, m.heapController.memRisk.lastMemStats.startTime.Equal(debugTime)) // update last mem stats
			require.True(t, m.heapController.memRisk.lastMemStats.heapTotalFree == 500+2)
			require.True(t, m.avoidance.memMagnif.ratio.Load() == lastRiskMemState.Magnif)
			require.True(t, *m.lastMemState() == lastRiskMemState)
			require.Equal(t, MockMetrcis{
				execMetricsCounter{Action: execMetricsAction{
					GC:                    3, // gc when each round of oom check
					UpdateRuntimeMemStats: 4, // refresh each round; gc & refresh oom check round
					RecordMemState:        pairSuccessFail{0, 1}}},
				MockLogs{0, 2, 1},
			}, tMetrics)
			require.True(t, tMetrics.Action == m.execMetrics.Action)
			require.True(t, m.execMetrics.Risk == execMetricsRisk{1, 0, NumByPriority{}})
		}

		{
			debugTime = m.heapController.memRisk.lastMemStats.startTime.Add(defHeapReclaimCheckDuration)
			lastRiskMemState := *m.lastMemState()
			mockHeap = MockHeap{multiRatio(newLimit, 900), multiRatio(newLimit, heapInuseRateMilli), multiRatio(newLimit, 900) + 500 + 4, multiRatio(newLimit, 50)}
			m.mu.lastGC = m.mu.released - uint64(m.poolAllocStats.SmallPoolLimit)
			require.True(t, m.runOneRound() == -2)
			require.True(t, m.heapController.memRisk.lastMemStats.startTime.Equal(debugTime))
			require.True(t, m.heapController.memRisk.lastMemStats.heapTotalFree == 500+4)
			require.True(t, m.avoidance.memMagnif.ratio.Load() == lastRiskMemState.Magnif)
			require.True(t, *m.lastMemState() == lastRiskMemState)
			require.Equal(t, MockMetrcis{
				execMetricsCounter{Action: execMetricsAction{
					GC:                    4, // gc when each round of oom check
					UpdateRuntimeMemStats: 5, // refresh each round
					RecordMemState:        pairSuccessFail{0, 1}}},
				MockLogs{0, 3, 1},
			}, tMetrics)
			require.True(t, tMetrics.Action == m.execMetrics.Action)
			require.Equal(t, execMetricsRisk{1, 0, NumByPriority{}}, m.execMetrics.Risk)
		}
		{
			killEvent := make(map[uint64]int)
			lastRiskMemState := *m.lastMemState()
			e2.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).killCB = func() {
				killEvent[e2.pool.uid]++
			}
			e1.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).killCB = func() {
				require.True(t, killEvent[e2.pool.uid] != 0)
				killEvent[e1.pool.uid]++
			}
			e3.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).killCB = func() {
				require.True(t, killEvent[e2.pool.uid] != 0)
				require.True(t, killEvent[e1.pool.uid] != 0)
				killEvent[e3.pool.uid]++
			}
			e1.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
				return e1.arbitratorMu.quota
			}
			e2.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
				return e2.arbitratorMu.quota
			}
			e3.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
				return e3.arbitratorMu.quota
			}
			m.prepareAlloc(e2, 1000)
			debugTime = m.heapController.memRisk.lastMemStats.startTime.Add(defHeapReclaimCheckDuration * 1)
			mockHeap = MockHeap{multiRatio(newLimit, 900), multiRatio(newLimit, heapInuseRateMilli) - 233, multiRatio(newLimit, 900) + 500 + 4 + 1, 233}
			require.True(t, m.runOneRound() == -2)
			require.False(t, memHangRisk(m.minHeapFreeBPS(), m.minHeapFreeBPS(), time.Time{}.Add(defHeapReclaimCheckMaxDuration), time.Time{}))
			require.True(t, memHangRisk(0, 0, time.Time{}.Add(defHeapReclaimCheckMaxDuration).Add(time.Nanosecond), time.Time{}))
			require.True(t, m.heapController.memRisk.lastMemStats.startTime.Equal(debugTime))
			require.True(t, m.heapController.memRisk.lastMemStats.heapTotalFree == 500+4+1)
			require.True(t, m.avoidance.memMagnif.ratio.Load() == lastRiskMemState.Magnif)
			require.True(t, *m.lastMemState() == lastRiskMemState)
			require.Equal(t, MockMetrcis{
				execMetricsCounter{
					Action: execMetricsAction{
						GC:                    5, // gc
						UpdateRuntimeMemStats: 6, // refresh each round
						RecordMemState:        pairSuccessFail{0, 1}},
				},
				MockLogs{0, 7, 1}, // OOM RISK; Start to `KILL`; make task failed; restart check;
			}, tMetrics)
			require.True(t, m.waitAlloc(e2) == ArbitrateFail)
			require.True(t, m.execMetrics.Task.pairSuccessFail == pairSuccessFail{0, 1})
			require.Equal(t, tMetrics.Action, m.execMetrics.Action)
			require.Equal(t, execMetricsRisk{1, 1, NumByPriority{1}}, m.execMetrics.Risk)
			require.Equal(t, map[uint64]int{e2.pool.uid: 1}, killEvent)
			require.True(t, m.underKill.num == 1)

			debugTime = m.heapController.memRisk.lastMemStats.startTime.Add(defHeapReclaimCheckDuration * 1)
			require.True(t, m.runOneRound() == -2)
			require.Equal(t, MockMetrcis{
				execMetricsCounter{
					Action: execMetricsAction{
						GC:                    6,
						UpdateRuntimeMemStats: 7,
						RecordMemState:        pairSuccessFail{0, 1}}},
				MockLogs{0, 8, 1}, // OOM RISK
			}, tMetrics)
			require.True(t, e2.ctx.Load().stopped.Load())
			select {
			case <-e2.ctx.Load().cancelCh:
			default:
				require.Fail(t, "")
			}
			require.Equal(t, execMetricsRisk{1, 2, NumByPriority{1}}, m.execMetrics.Risk)

			m.mu.allocated += 1e5
			m.entryMap.addQuota(e5, 1e5)
			e5.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
				return 1e5
			}
			e5.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).killCB = func() {
				killEvent[e5.pool.uid]++
			}

			debugTime = m.heapController.memRisk.lastMemStats.startTime.Add(defKillCancelCheckTimeout)
			require.True(t, m.runOneRound() == -2)
			require.Equal(t, MockMetrcis{
				execMetricsCounter{
					Action: execMetricsAction{
						GC:                    7,
						UpdateRuntimeMemStats: 8,
						RecordMemState:        pairSuccessFail{0, 1}}},
				MockLogs{0, 11, 2}, // OOM RISK; Failed to `KILL` root pool; Start to `KILL` root pool;
			}, tMetrics)
			require.True(t, m.underKill.num == 2)
			require.True(t, m.underKill.entries[e2.pool.uid].arbitratorMu.underKill.fail)
			require.True(t, m.underKill.entries[e5.pool.uid].arbitratorMu.underKill.start)
			require.False(t, m.underKill.entries[e5.pool.uid].arbitratorMu.underKill.fail)
			require.Equal(t, execMetricsRisk{1, 3, NumByPriority{2}}, m.execMetrics.Risk)

			// release quota which make it able to gc
			m.removeEntryForTest(e2)
			m.removeEntryForTest(e5)
			debugTime = m.heapController.memRisk.lastMemStats.startTime.Add(defHeapReclaimCheckDuration)
			require.True(t, m.runOneRound() == -2)
			require.False(t, e5.arbitratorMu.underKill.start)
			require.False(t, e2.arbitratorMu.underKill.start)
			require.True(t, m.underKill.num == 2)
			require.Equal(t, MockMetrcis{
				execMetricsCounter{
					Action: execMetricsAction{
						GC:                    8,
						UpdateRuntimeMemStats: 9,
						RecordMemState:        pairSuccessFail{0, 1}}},
				MockLogs{0, 17, 2}, // Finish KILL;Finish KILL;OOM RISK;Start to KILL;Start to KILL;Restart check;
			}, tMetrics)
			require.True(t, m.avoidance.heapTracked.Load() == e1.arbitratorMu.quota+e3.arbitratorMu.quota)
			require.Equal(t, execMetricsRisk{1, 4, NumByPriority{3, 0, 1}}, m.execMetrics.Risk)
			require.Equal(t, map[uint64]int{e1.pool.uid: 1, e2.pool.uid: 1, e3.pool.uid: 1, e5.pool.uid: 1}, killEvent)
			require.True(t, m.buffer.size.Load() == 9000)

			m.removeEntryForTest(e1)
			m.removeEntryForTest(e3)
			mockHeap = MockHeap{multiRatio(newLimit, 900) - 1, multiRatio(newLimit, 900) - 1} // mem safe
			e4.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
				return 1013
			}
			require.True(t, m.runOneRound() == 0)
			require.False(t, e1.arbitratorMu.underKill.start)
			require.False(t, e3.arbitratorMu.underKill.start)
			require.True(t, m.underKill.num == 0)
			require.True(t, m.avoidance.heapTracked.Load() == 1013)
			require.Equal(t, MockMetrcis{
				execMetricsCounter{
					Action: execMetricsAction{
						GC:                    9,
						UpdateRuntimeMemStats: 10,
						RecordMemState:        pairSuccessFail{0, 1}}},
				MockLogs{1, 19, 2}, // Finish KILL;Finish KILL;mem is safe;
			}, tMetrics)
			require.True(t, !m.atMemRisk())
		}
		require.True(t, m.ConsumeQuotaFromAwaitFreePool(0, -1000))
		require.True(t, m.awaitFreePoolUsed() == memPoolQuotaUsage{})
		require.True(t, m.awaitFreePoolCap() == 0)
		m.shrinkAwaitFreePool(0, defAwaitFreePoolShrinkDurMilli)
		require.True(t, m.avoidance.memMagnif.ratio.Load() == 6100)
		require.True(t, m.avoidance.size.Load() == mockHeap[0]-1013)
		m.deleteEntryForTest(e4)
		m.checkEntryForTest()
	}
	{ // test tick task: reduce mem magnification
		m.resetExecMetricsForTest()
		require.True(t, m.RootPoolNum() == 0)
		require.True(t, m.allocated() == 0)
		debugTime = time.Unix(defUpdateMemMagnifUtimeAlign, 0)
		m.setUnixTimeSec(debugTime.Unix())

		e1ctx := m.newCtxWithHelperForTest(ArbitrationPriorityMedium, NoWaitAverse, RequirePrivilege)
		e1ctx.PrevMaxMem = 23
		e1ctx.memQuotaLimit = 29
		e1ctx.arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
			return 31
		}
		e1 := m.addEntryForTest(e1ctx)

		require.True(t, m.buffer.size.Load() == 23)
		require.True(t, m.buffer.quotaLimit.Load() == 0)
		m.updateTrackedHeapStats()
		require.True(t, m.buffer.size.Load() == 31)
		require.True(t, m.buffer.quotaLimit.Load() == 0)

		m.ResetRootPoolByID(e1.pool.uid, 19, true) // tune
		require.True(t, m.buffer.size.Load() == 31)
		require.True(t, m.buffer.quotaLimit.Load() == 29)

		m.ResetRootPoolByID(e1.pool.uid, 389, true) // tune
		require.True(t, m.buffer.size.Load() == 389)

		logs := MockLogs{}

		m.actions.Error = func(format string, args ...zap.Field) {
			logs.error++
		}
		m.actions.Warn = func(format string, args ...zap.Field) {
			logs.warn++
		}
		m.actions.Info = func(format string, args ...zap.Field) {
			logs.info++
		}

		{ // mock set oom check start
			m.heapController.memRisk.startTime.unixMilli.Store(time.Now().UnixMilli())
			require.False(t, m.executeTick(defMax))
			m.heapController.memRisk.startTime.unixMilli.Store(0)
		}

		type mockTimeline struct {
			pre int64
			now int64
		}
		mockTimeLine := mockTimeline{
			0, int64(defUpdateMemMagnifUtimeAlign * kilo),
		}

		nextTime := func() {
			mockTimeLine.pre = mockTimeLine.now
			mockTimeLine.now += defUpdateMemMagnifUtimeAlign * kilo
		}
		checkMockTimeProfImpl := func(ms, heap, quota, ratio int64) {
			sec := ms / kilo
			align := sec / defUpdateMemMagnifUtimeAlign
			require.Equal(t, memProfile{
				startUtimeMilli: ms,
				tsAlign:         align,
				heap:            heap,
				quota:           quota,
				ratio:           ratio,
			}, m.heapController.timedMemProfile[align%int64(len(m.heapController.timedMemProfile))])
		}
		checkMockTimeProf := func(ms, heap, quota int64) {
			checkMockTimeProfImpl(ms, heap, quota, 0)
		}

		{ // new suggest pool cap
			// last mem state is nil
			ori := m.lastMemState()
			require.Equal(t,
				RuntimeMemStateV1{Version: 1, LastRisk: LastRisk{HeapAlloc: 90000, QuotaAlloc: 15000}, Magnif: 6100, PoolMediumCap: 0},
				*m.lastMemState())
			require.True(t, m.avoidance.memMagnif.ratio.Load() == 6100)
			require.True(t, m.poolMediumQuota() == 0)
			require.True(t, m.poolAllocStats.lastUpdateUtimeMilli.Load() == 0)
			require.True(t, m.execMetrics.Action.RecordMemState.Succ == 0)

			m.heapController.memStateRecorder.lastMemState.Store(nil)
			require.True(t, m.executeTick(mockTimeLine.now))
			require.True(t, m.execMetrics.Action.RecordMemState.Succ == 1)
			require.True(t, logs.info == 1)
			require.True(t, m.poolMediumQuota() == 400)
			require.True(t, m.poolAllocStats.lastUpdateUtimeMilli.Load() == mockTimeLine.now)
			m.heapController.memStateRecorder.lastMemState.Store(ori)
			ori.PoolMediumCap = m.poolMediumQuota()

			// same value
			require.False(t, m.tryStorePoolMediumCapacity(mockTimeLine.now+defTickDurMilli*10+1, 400))
			// time not satisfy
			require.False(t, m.tryStorePoolMediumCapacity(mockTimeLine.now+defTickDurMilli*10-1, 399))
			require.True(t, m.execMetrics.Action.RecordMemState.Succ == 1)
			require.True(t, logs.info == 1)
			// new suggest pool cap: last mem state is not nil & SuggestPoolInitCap not same
			require.True(t, m.tryStorePoolMediumCapacity(mockTimeLine.now+defTickDurMilli*10, 401))
			require.True(t, m.execMetrics.Action.RecordMemState.Succ == 2)
			require.True(t, logs.info == 2)
			require.True(t, m.lastMemState().PoolMediumCap == 401)
		}

		{
			m.SetSoftLimit(0, 0, SoftLimitModeDisable)
			m.setMemStatsForTest(0, 0, 0, 0)
			require.True(t, m.avoidance.size.Load() == 5000)
			m.SetSoftLimit(0, 0, SoftLimitModeAuto)
			m.setMemStatsForTest(0, 0, 0, 0)
			require.True(t, m.avoidance.size.Load() == 83607) // update avoid size under auto mode
		}

		{ // init set last gc state
			m.heapController.lastGC.heapAlloc.Store(defMax)
			m.heapController.lastGC.utime.Store(0)
		}

		// mock set oom risk threshold
		m.SetLimit(15000)
		require.True(t, m.limit() == 15000)
		m.mu.allocated = 20000
		require.True(t, m.allocated() == 20000)
		// update mock timeline
		nextTime()
		m.prepareAlloc(e1, defMax)
		require.Equal(t, blockedState{}, m.wrapblockedState())
		m.setUnixTimeSec(mockTimeLine.now/kilo + defUpdateMemMagnifUtimeAlign - 1)
		m.heapController.lastGC.heapAlloc.Store(m.limit())
		m.heapController.lastGC.utime.Store(m.approxUnixTimeSec() * 1e9)
		require.False(t, m.doExecuteFirstTask())
		require.Equal(t, blockedState{20000, m.approxUnixTimeSec()}, m.wrapblockedState())
		// calculate ratio of the previous
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 3)
		// new suggest pool cap
		require.True(t, logs.info == 3)
		require.True(t, m.lastMemState().PoolMediumCap == 400)
		checkMockTimeProf(mockTimeLine.pre, 0, 0)
		checkMockTimeProf(mockTimeLine.now, 15000, 20000)
		//
		nextTime()
		m.setUnixTimeSec(mockTimeLine.now/kilo + defUpdateMemMagnifUtimeAlign - 1)
		m.mu.allocated = 40000
		m.heapController.lastGC.heapAlloc.Store(35000)
		m.heapController.lastGC.utime.Store(m.approxUnixTimeSec() * 1e9)
		require.False(t, m.doExecuteFirstTask())
		require.Equal(t, blockedState{40000, m.approxUnixTimeSec()}, m.wrapblockedState())
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 3)
		require.True(t, logs.info == 4) //Mem profile timeline
		checkMockTimeProfImpl(mockTimeLine.pre, 15000, 20000, 750)
		checkMockTimeProf(mockTimeLine.now, 35000, 40000)
		//
		nextTime()
		m.setUnixTimeSec(mockTimeLine.now/kilo + defUpdateMemMagnifUtimeAlign - 1)
		m.mu.allocated = 50000
		m.heapController.lastGC.heapAlloc.Store(40000)
		m.heapController.lastGC.utime.Store(m.approxUnixTimeSec() * 1e9)
		require.False(t, m.doExecuteFirstTask())
		require.Equal(t, blockedState{50000, m.approxUnixTimeSec()}, m.wrapblockedState())
		require.True(t, m.executeTick(mockTimeLine.now))
		// no update because the heap stats is NOT safe
		require.True(t, m.avoidance.memMagnif.ratio.Load() == 6100)
		// timed mem profile
		require.True(t, logs.info == 5)
		// record
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 3)
		checkMockTimeProfImpl(mockTimeLine.pre, 35000, 40000, 875)
		checkMockTimeProf(mockTimeLine.now, 40000, 50000)

		// restore limit to 100000
		m.SetLimit(1e5)
		nextTime()
		m.setUnixTimeSec(mockTimeLine.now/kilo + 1)
		m.execMu.blockedState.allocated = 50000
		m.execMu.blockedState.utimeSec = m.approxUnixTimeSec()
		m.heapController.lastGC.heapAlloc.Store(40000)
		m.heapController.lastGC.utime.Store(m.approxUnixTimeSec() * 1e9)
		// Update mem quota magnification ratio
		// new magnification ratio
		// timed mem profile
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (875+6100)/2) // choose 875 rather than 800
		require.True(t, logs.info == 8)
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 4)
		checkMockTimeProfImpl(mockTimeLine.pre, 40000, 50000, 800)
		checkMockTimeProf(mockTimeLine.now, 40000, 50000)

		mockTimeLine.pre = mockTimeLine.now
		mockTimeLine.now += 1 * kilo
		m.setUnixTimeSec(m.approxUnixTimeSec() + 1)
		m.mu.allocated = 40000
		m.heapController.lastGC.heapAlloc.Store(41000)
		m.heapController.lastGC.utime.Store(m.approxUnixTimeSec() * 1e9)
		require.False(t, m.doExecuteFirstTask())
		require.Equal(t, blockedState{40000, m.approxUnixTimeSec()}, m.wrapblockedState())
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (875+6100)/2) // no update
		require.True(t, logs.info == 8)
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 4)
		checkMockTimeProf(mockTimeLine.now-kilo, 41000, 50000)
		mockTimeLine.now = mockTimeLine.pre + defUpdateMemMagnifUtimeAlign*kilo
		require.True(t, m.executeTick(mockTimeLine.now))
		checkMockTimeProfImpl(mockTimeLine.pre, 41000, 50000, 820)
		checkMockTimeProf(mockTimeLine.now, 0, 0)
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (3487+820)/2)
		require.True(t, logs.info == 11)
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 5)

		nextTime()
		// no valid pre profile
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, logs.info == 13)
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 6)
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (2153+820)/2)
		checkMockTimeProf(mockTimeLine.now, 0, 0)
		checkMockTimeProf(mockTimeLine.pre, 0, 0)
		// update mem quota magnification ratio
		require.True(t, logs.info == 13)
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 6)

		nextTime()
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (2153+820)/2) // no update
		nextTime()

		// new start
		nextTime()
		m.setUnixTimeSec(mockTimeLine.now / kilo)
		m.execMu.blockedState.allocated = 10000
		m.execMu.blockedState.utimeSec = m.approxUnixTimeSec()
		m.heapController.lastGC.heapAlloc.Store(10000)
		m.heapController.lastGC.utime.Store(m.approxUnixTimeSec() * 1e9)
		require.True(t, m.executeTick(mockTimeLine.now))
		// no update
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (2153+820)/2) // no update
		require.True(t, logs.info == 13)
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 6)

		nextTime()
		m.setUnixTimeSec(mockTimeLine.now / kilo)
		m.execMu.blockedState.allocated = 11111
		m.execMu.blockedState.utimeSec = m.approxUnixTimeSec()
		m.heapController.lastGC.heapAlloc.Store(2222)
		m.heapController.lastGC.utime.Store(m.approxUnixTimeSec() * 1e9)
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (2153+820)/2) // no update
		// timed mem profile
		require.True(t, logs.info == 14)
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 6)
		checkMockTimeProfImpl(mockTimeLine.pre, 10000, 10000, 1000)
		checkMockTimeProf(mockTimeLine.now, 2222, 11111)

		nextTime()
		m.setUnixTimeSec(mockTimeLine.now / kilo)
		m.execMu.blockedState.allocated = 10000
		m.execMu.blockedState.utimeSec = m.approxUnixTimeSec()
		m.heapController.lastGC.heapAlloc.Store(10000)
		m.heapController.lastGC.utime.Store(m.approxUnixTimeSec() * 1e9)
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (1486+1000)/2)
		require.True(t, logs.info == 17)
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 7)
		checkMockTimeProfImpl(mockTimeLine.pre, 2222, 11111, 199)
		checkMockTimeProf(mockTimeLine.now, 10000, 10000)

		nextTime()
		m.setUnixTimeSec(mockTimeLine.now / kilo)
		m.execMu.blockedState.allocated = 10000
		m.execMu.blockedState.utimeSec = m.approxUnixTimeSec()
		m.heapController.lastGC.heapAlloc.Store(5000)
		m.heapController.lastGC.utime.Store(m.approxUnixTimeSec() * 1e9)
		require.True(t, m.executeTick(mockTimeLine.now))
		checkMockTimeProfImpl(mockTimeLine.pre, 10000, 10000, 1000)
		checkMockTimeProf(mockTimeLine.now, 5000, 10000)
		// choose 1000 rather than 199
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (1243+1000)/2)
		require.True(t, logs.info == 20) // update mem quota magnification ratio;
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 8)

		nextTime()
		m.setUnixTimeSec(mockTimeLine.now / kilo)
		m.execMu.blockedState.allocated = 10000
		m.execMu.blockedState.utimeSec = m.approxUnixTimeSec()
		m.heapController.lastGC.heapAlloc.Store(5000)
		m.heapController.lastGC.utime.Store(m.approxUnixTimeSec() * 1e9)
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (1121+1000)/2)
		require.True(t, logs.info == 23) // update mem quota magnification ratio;
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 9)

		nextTime()
		m.setUnixTimeSec(mockTimeLine.now / kilo)
		m.execMu.blockedState.allocated = 10000
		m.execMu.blockedState.utimeSec = m.approxUnixTimeSec()
		m.heapController.lastGC.heapAlloc.Store(20000)
		m.heapController.lastGC.utime.Store(m.approxUnixTimeSec() * 1e9)
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == 0) // smaller than 1000
		require.True(t, logs.info == 26)
		require.True(t, m.execMetrics.Action.RecordMemState.Succ == 10)
		e1.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).cancelSelf()
		m.waitAlloc(e1)
		m.deleteEntryForTest(e1)
		m.mu.allocated = 0
	}
	{
		// No more root pool can be killed
		m.resetExecMetricsForTest()
		require.True(t, m.isMemSafe())
		m.checkEntryForTest()
		m.SetLimit(100000)
		const heapInuseRateMilli = 950
		mockHeap := MockHeap{0}
		m.actions.UpdateRuntimeMemStats = func() {
			m.setMemStatsForTest(mockHeap[0], mockHeap[1], mockHeap[2], mockHeap[3])
		}
		m.actions.GC = func() {
		}
		m.actions.Error = func(format string, args ...zap.Field) {
		}
		m.actions.Warn = func(format string, args ...zap.Field) {
		}
		m.actions.Info = func(format string, args ...zap.Field) {
		}
		e1 := m.addEntryForTest(m.newCtxWithHelperForTest(ArbitrationPriorityMedium, NoWaitAverse, RequirePrivilege))
		e1.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
			return 10000
		}
		kill1 := 0
		e1.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).killCB = func() {
			kill1++
		}
		m.prepareAlloc(e1, 10000)
		require.True(t, m.runOneRound() == 1)
		require.True(t, m.waitAlloc(e1) == ArbitrateOk)
		mockHeap = MockHeap{0, multiRatio(100000, heapInuseRateMilli+1), 0, 0}
		e2 := m.addEntryForTest(m.newCtxWithHelperForTest(ArbitrationPriorityMedium, NoWaitAverse, RequirePrivilege))
		kill2 := 0
		e2.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).killCB = func() {
			kill2++
		}
		m.refreshRuntimeMemStats()
		debugNow := now()
		mockNow = func() time.Time {
			return debugNow
		}
		require.True(t, m.runOneRound() == -2)
		debugNow = debugNow.Add(time.Second)
		require.True(t, m.runOneRound() == -2)
		require.True(t, m.underKill.num == 1)
		require.True(t, e1.arbitratorMu.underKill.start)
		require.False(t, e1.arbitratorMu.underKill.fail)
		debugNow = debugNow.Add(defKillCancelCheckTimeout)
		m.prepareAlloc(e2, 10000)
		require.True(t, m.runOneRound() == -2)
		require.True(t, e1.arbitratorMu.underKill.fail)
		require.True(t, e1.arbitratorMu.underKill.start)
		require.True(t, !e2.ctx.Load().available())
		require.True(t, m.waitAlloc(e2) == ArbitrateFail)
		m.deleteEntryForTest(e1, e2)
		m.checkEntryForTest()
	}
}

func TestBasicUtils(t *testing.T) {
	defer func() {
		mockNow = nil
	}()

	testState = t

	{
		const cnt = 1 << 8
		bgID := uint64(4068484684)
		odd := 0
		for i := range cnt {
			n := shardIndexByUID(bgID+uint64(i)*2, cnt-1)
			if n&1 != 0 {
				odd++
			}
		}
		require.Equal(t, odd, cnt/2)
	}
	require.Equal(t, baseQuotaUnit*(1<<(defPoolQuotaShards-2)), 128*byteSizeGB)
	require.Equal(t, getQuotaShard(0, defPoolQuotaShards), 0)
	require.Equal(t, getQuotaShard(baseQuotaUnit-1, defPoolQuotaShards), 0)
	require.Equal(t, getQuotaShard(baseQuotaUnit, defPoolQuotaShards), 1)
	require.Equal(t, getQuotaShard(baseQuotaUnit*2-1, defPoolQuotaShards), 1)
	require.Equal(t, getQuotaShard(baseQuotaUnit*2, defPoolQuotaShards), 2)
	require.Equal(t, getQuotaShard(baseQuotaUnit*4-1, defPoolQuotaShards), 2)
	require.Equal(t, getQuotaShard(baseQuotaUnit*4, defPoolQuotaShards), 3)
	require.Equal(t, getQuotaShard(baseQuotaUnit*(1<<(defPoolQuotaShards-2))-1, defPoolQuotaShards), defPoolQuotaShards-2)
	require.Equal(t, getQuotaShard(baseQuotaUnit*(1<<(defPoolQuotaShards-2)), defPoolQuotaShards), defPoolQuotaShards-1)
	require.Equal(t, getQuotaShard(1<<63-1, defPoolQuotaShards), defPoolQuotaShards-1)

	{
		ch1 := make(chan struct{})
		n := NewNotifer()
		require.False(t, n.isAwake())
		go func() {
			defer close(ch1)
			n.Wake()
			require.True(t, n.isAwake())
			n.Wake()
			n.WeakWake()
			require.True(t, n.isAwake())
		}()
		<-ch1
		require.True(t, n.isAwake())
		n.Wait()
		require.False(t, n.isAwake())
	}
	{
		data := wrapList[*rootPoolEntry]{}
		require.True(t, data.size() == 0)
		require.Equal(t, data.base.Len(), 0)
		data.init()
		require.True(t, data.size() == 0)
		require.Equal(t, data.base.Len(), 1)

		require.Nil(t, data.popFront())
		require.Nil(t, data.front())
		p1 := &rootPoolEntry{}
		p2 := &rootPoolEntry{}
		p3 := &rootPoolEntry{}

		data.pushBack(p1)
		require.True(t, data.size() == 1)
		require.Equal(t, data.base.Len(), 2)
		require.Equal(t, data.front(), p1)

		ep2 := data.pushBack(p2)
		require.True(t, data.size() == 2)
		require.Equal(t, data.base.Len(), 3)
		require.Equal(t, data.front(), p1)

		data.remove(ep2)
		require.True(t, data.size() == 1)
		require.Equal(t, data.base.Len(), 3)
		require.Equal(t, data.front(), p1)

		data.pushBack(p3)
		require.True(t, data.size() == 2)
		require.Equal(t, data.base.Len(), 3)
		require.Equal(t, data.front(), p1)

		require.Equal(t, data.popFront(), p1)
		require.True(t, data.size() == 1)
		require.Equal(t, data.base.Len(), 3)
		require.Equal(t, data.front(), p3)

		ep1 := data.pushBack(p1)
		require.Equal(t, data.front(), p3)
		data.moveToFront(ep1)
		require.True(t, data.size() == 2)
		require.Equal(t, data.base.Len(), 3)
		require.Equal(t, data.front(), p1)

		data.pushBack(p2)
		require.True(t, data.size() == 3)
	}
	{
		nowMilli := nowUnixMilli()
		require.True(t, time.UnixMilli(nowMilli).UnixMilli() == nowMilli)
		nowSec := now().Unix()
		require.True(t, time.Unix(nowSec, 0).Unix() == nowSec)
		now := now()
		require.True(t, now.UnixMilli() >= nowMilli)
		require.True(t, now.Unix() >= nowSec)

		m := MemArbitrator{}
		tm := time.Now()
		require.True(t, mockNow == nil)
		require.True(t, !m.innerTime().IsZero())
		mockNow = func() time.Time {
			return tm
		}
		require.True(t, m.innerTime().Equal(tm))
	}
	{
		require.True(t, nextPow2(0) == 1)
		for n := uint64(1); n < 63; n++ {
			x := uint64(1) << n
			require.True(t, nextPow2(x) == x)
			if x > 2 {
				require.True(t, nextPow2(x-1) == x)
			}
		}
	}
}

func TestBench(t *testing.T) {
	testState = t

	m := NewMemArbitrator(
		4*byteSizeGB,
		defPoolStatusShards, defPoolQuotaShards,
		64*byteSizeKB, /* 64k ~ */
		&memStateRecorderForTest{
			load: func() (*RuntimeMemStateV1, error) {
				return nil, nil
			},
			store: func(*RuntimeMemStateV1) error {
				return nil
			}},
	)
	m.SetWorkMode(ArbitratorModeStandard)
	m.initAwaitFreePool(4, 4)
	const N = 3000
	{
		m.restartForTest()
		cancelPool := atomic.Int64{}
		killedPool := atomic.Int64{}
		ch1 := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < N; i += 1 {
			wg.Go(func() {
				<-ch1

				root, err := m.EmplaceRootPool(uint64(i))
				require.NoError(t, err)
				cancelCh := make(chan struct{})
				cancelEvent := 0
				killed := false
				ctx := NewArbitrationContext(
					cancelCh,
					0,
					0,
					&arbitrateHelperForTest{
						cancelCh: cancelCh,
						heapUsedCB: func() int64 {
							return 0
						},
						killCB: func() {
							cancelEvent++
							killed = true
						},
						cancelCB: func() {
							cancelEvent++
						},
					},
					ArbitrationPriorityHigh,
					false,
					true,
				)
				if !root.Restart(ctx) {
					panic(fmt.Errorf("failed to init root pool with session-id %d", root.entry.pool.uid))
				}

				b := ConcurrentBudget{Pool: root.entry.pool}

				for j := 0; j < 200; j += 1 {
					if b.Used.Add(m.limit()/150) > b.Capacity {
						_ = b.PullFromUpstream()
					}
					if cancelEvent != 0 {
						if killed {
							killedPool.Add(1)
						} else {
							cancelPool.Add(1)
						}
						break
					}
				}
				m.ResetRootPoolByID(uint64(i), b.Used.Load(), true)
			})
		}
		close(ch1)
		wg.Wait()

		require.True(t, m.rootPoolNum.Load() == N)
		require.True(t, killedPool.Load() == 0)
		require.True(t, cancelPool.Load() == N)
		for i := 0; i < N; i += 1 {
			m.RemoveRootPoolByID(uint64(i))
		}
		m.stop()
		m.checkEntryForTest()
		require.True(t, m.execMetrics.Task.Fail == N)
		require.True(t, m.execMetrics.Cancel.StandardMode == N)
		m.resetExecMetricsForTest()
	}

	m.SetWorkMode(ArbitratorModePriority)
	{
		m.restartForTest()
		cancelPool := atomic.Int64{}
		killedPool := atomic.Int64{}
		ch1 := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < N; i += 1 {
			wg.Go(func() {
				<-ch1

				root, err := m.EmplaceRootPool(uint64(i))
				require.NoError(t, err)
				cancelCh := make(chan struct{})
				cancelEvent := atomic.Int64{}
				killed := atomic.Bool{}

				prio := ArbitrationPriorityHigh
				p := i % 6
				if p < 2 {
					prio = ArbitrationPriorityLow
				} else if p < 4 {
					prio = ArbitrationPriorityMedium
				}
				waitAverse := true
				if p%2 == 0 {
					waitAverse = false
				}

				ctx := NewArbitrationContext(
					cancelCh,
					0,
					0,
					&arbitrateHelperForTest{
						cancelCh: cancelCh,
						heapUsedCB: func() int64 {
							return 0
						},
						killCB: func() {
							cancelEvent.Add(1)
							killed.Store(true)
						},
						cancelCB: func() {
							cancelEvent.Add(1)
						},
					},
					prio,
					waitAverse,
					true,
				)

				if !root.Restart(ctx) {
					panic(fmt.Errorf("failed to init root pool with session-id %d", root.entry.pool.uid))
				}

				b := ConcurrentBudget{Pool: root.entry.pool}

				for j := 0; j < 200; j += 1 {
					if b.Used.Add(m.limit()/150) > b.Capacity {
						_ = b.PullFromUpstream()
					}
					if cancelEvent.Load() != 0 {
						if killed.Load() {
							killedPool.Add(1)
						} else {
							cancelPool.Add(1)
						}
						break
					}
				}
				m.ResetRootPoolByID(uint64(i), b.Used.Load(), true)
			})
		}
		close(ch1)
		wg.Wait()

		require.True(t, m.rootPoolNum.Load() == N)
		require.True(t, killedPool.Load() == 0)
		require.True(t, cancelPool.Load() != 0)
		for i := 0; i < N; i += 1 {
			m.RemoveRootPoolByID(uint64(i))
		}
		m.stop()
		m.checkEntryForTest()
		require.True(t, m.execMetrics.Task.Fail >= N/2)
		require.Equal(t, cancelPool.Load(),
			m.execMetrics.Cancel.WaitAverse+m.execMetrics.Cancel.PriorityMode[ArbitrationPriorityLow]+m.execMetrics.Cancel.PriorityMode[ArbitrationPriorityMedium]+m.execMetrics.Cancel.PriorityMode[ArbitrationPriorityHigh])
		require.True(t, m.execMetrics.Cancel.WaitAverse == N/2)
		// under priority mode, arbitrator may cancel pool which is not waiting for alloc
		require.GreaterOrEqual(t, cancelPool.Load(), m.execMetrics.Task.Fail)
		m.resetExecMetricsForTest()
	}
}
