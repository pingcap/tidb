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

func (m *MemArbitrator) stopForTest() bool {
	m.controlMu.Lock()
	defer m.controlMu.Unlock()

	if !m.controlMu.running {
		return false
	}

	m.controlMu.running = false
	m.wake()

	<-m.controlMu.finishCh

	m.runOneRound()

	m.cleanupNotifer()

	return true
}

func (m *MemArbitrator) cleanupNotifer() {
	m.notifer.Wake()
	m.notifer.Wait()
}
func (m *MemArbitrator) waitNotiferForTest() {
	m.notifer.Wait()
}

func (m *MemArbitrator) restartEntryForTest(entry *rootPoolEntry, ctx *Context) {
	require.True(testState, m.restartEntryByContext(entry, ctx))
}

func (m *MemArbitrator) checkAwaitFree() {
	s := int64(0)
	for i := range m.awaitFree.budget.shards {
		s += m.awaitFree.budget.shards[i].Capacity.Load()
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
	ctx *Context,
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
	ctx *Context,
) *rootPoolEntry {
	p := newResourcePoolForTest("test", 1)
	return m.addRootPoolForTest(p, ctx)
}

func (m *MemArbitrator) getAllEntryForTest() mapUidEntry {
	t := testState
	res := make(mapUidEntry)
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

	for prio := minArbitrateMemPriority; prio < maxArbitrateMemPriority; prio++ {
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
		uid := entry.pool.Uid()
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
	sumQuota += m.awaitFree.pool.cap()
	require.Equal(t, sumQuota, m.Allocated())
}

func (m *MemArbitrator) getQuotaShard(priority ArbitrateMemPriority, quota int64) *entryQuotaShard {
	return m.entryMap.getQuotaShard(priority, quota)
}

func newResourcePoolForTest(
	name string,
	allocAlignSize int64,
) *ResourcePool {
	p := NewResourcePoolWithLimit(name, 0, allocAlignSize, PoolActions{})
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
	m = newMemArbitrator(limit, shardCount, 3, 0, &memStateRecorderForTest{
		load: func() (*RuntimeMemStateV1, error) {
			loadEvent++
			return nil, nil
		},
		store: func(*RuntimeMemStateV1) error {
			return nil
		},
	})
	require.True(testState, loadEvent == 1)
	m.initAwaitFreePool(4, 4, 1)
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

func (t *arbitrateHelperForTest) Kill() bool {
	close(t.cancelCh)
	t.killCB()
	return true
}

func (t *arbitrateHelperForTest) Cancel() bool {
	close(t.cancelCh)
	t.cancelCB()
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
	expectedMemPriority ArbitrateMemPriority,
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

func (m *MemArbitrator) clearAwaitFreeForTest() {
	t := testState
	for i := range m.awaitFree.budget.shards {
		b := &m.awaitFree.budget.shards[i]
		b.Used.Store(0)
		b.LastUsedTimeSec = 0
	}
	require.True(t, m.awaitFreePoolUsed() == 0)
	m.shrinkAwaitFreePool(0, 0, nowUnixMilli())
	require.Equal(t, int64(0), m.awaitFreePoolCap())
}

func (m *MemArbitrator) findTaskByMode(
	e *rootPoolEntry,
	prio ArbitrateMemPriority,
	waitAverse bool,
) (found bool) {
	t := testState
	for p := minArbitrateMemPriority; p < maxArbitrateMemPriority; p++ {
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
					require.True(t, p == ArbitrateMemPriorityMedium)
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
	for i := minArbitrateMemPriority; i < maxArbitrateMemPriority; i++ {
		sz += m.taskNumByPriority(i)
	}
	require.True(testState, sz == m.tasks.fifoTasks.size())
	require.False(testState, sz == 0 && m.PendingAllocSize() != 0)
	return sz
}

func newCtxForTest(ch <-chan struct{}, h ArbitrateHelper, memPriority ArbitrateMemPriority, waitAverse bool, preferPrivilege bool) *Context {
	return NewContext(ch, 0, 0, h, memPriority, waitAverse, preferPrivilege)
}

func newDefCtxForTest(memPriority ArbitrateMemPriority) *Context {
	return newCtxForTest(nil, nil, memPriority, NoWaitAverse, false)
}

func (m *MemArbitrator) newPoolWithHelperForTest(memPriority ArbitrateMemPriority, waitAverse, preferPrivilege bool) *rootPoolEntry {
	ctx := m.newCtxWithHelperForTest(memPriority, waitAverse, preferPrivilege)
	pool := newResourcePoolForTest("?", 1)
	e := m.addRootPoolForTest(pool, ctx)
	return e
}

func (m *MemArbitrator) newCtxWithHelperForTest(memPriority ArbitrateMemPriority, waitAverse, preferPrivilege bool) *Context {
	h := &arbitrateHelperForTest{
		cancelCh: make(chan struct{}),
	}
	ctx := newCtxForTest(h.cancelCh, h, memPriority, waitAverse, preferPrivilege)
	return ctx
}

func (m *MemArbitrator) resetExecMetrics() {
	m.execMetrics = execMetrics{}
	m.execMu.blockedState = blockedState{}
	m.buffer = buffer{}
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

func (m *MemArbitrator) checkTaskExec(task pairSuccessFail, cancelByStandardMode int64, cancel numByAllMode) {
	t := testState
	require.Equal(t, m.execMetrics.task.pairSuccessFail, task)
	s := int64(0)
	for i := minArbitrateMemPriority; i < maxArbitrateMemPriority; i++ {
		s += m.execMetrics.task.successByPriority[i]
	}
	if m.workMode() == ArbitratorModePriority {
		require.Equal(t, s, task.success)
	} else {
		require.True(t, 0 == s)
	}
	require.True(t, m.execMetrics.cancel == execMetricsCancel{
		standardMode: cancelByStandardMode,
		priorityMode: numByPriority(cancel[:3]),
		waitAverse:   cancel[3],
	})
}

func (m *MemArbitrator) setMemStatsForTest(alloc, heapInuse, totalAlloc int64) {
	m.setMemStats(alloc, heapInuse, totalAlloc, 0)
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
	require.True(testState, m.shrinkDigestProfile(DefMax, 0, 0) == n)
	require.True(testState, m.digestProfileCache.num.Load() == 0)
}

type lastBlockedAt struct {
	allocated int64
	utimeSec  int64
}

func (m *MemArbitrator) wrapLastBlockedAt() lastBlockedAt {
	allocated, utimeSec := m.lastBlockedAt()
	return lastBlockedAt{allocated, utimeSec}
}

func BenchmarkWrapList(b *testing.B) {
	data := WrapList[int64]{}
	data.init()
	pushCnt, popCnt := 0, 0
	for i := 0; i < b.N; i++ {
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
	for i := 0; i < b.N; i++ {
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
	m.resetExecMetrics()
	newLimit := int64(1000)
	m.setLimitForTest(newLimit)
	require.Equal(t, m.limit(), newLimit)
	actionCancel := make(map[uint64]int)

	genTestPoolWithHelper := func(memPriority ArbitrateMemPriority, waitAverse, preferPrivilege bool) *rootPoolEntry {
		e := m.newPoolWithHelperForTest(memPriority, waitAverse, preferPrivilege)
		e.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).cancelCB = func() {
			actionCancel[e.pool.uid]++
		}
		return e
	}

	genTestCtx := func(e *rootPoolEntry, memPriority ArbitrateMemPriority, waitAverse, preferPrivilege bool) *Context {
		c := m.newCtxWithHelperForTest(memPriority, waitAverse, preferPrivilege)
		c.arbitrateHelper.(*arbitrateHelperForTest).cancelCB = func() {
			actionCancel[e.pool.uid]++
		}
		return c
	}

	entry1 := genTestPoolWithHelper(ArbitrateMemPriorityMedium, NoWaitAverse, false)
	{ // Disable mode
		entry1.ctx.preferPrivilege = true
		require.True(t, entry1.execState() == execStateRunning)
		require.True(t, entry1.state() == PoolEntryStateNormal)
		requestSize := newLimit * 2

		m.prepareAlloc(entry1, requestSize)
		m.waitNotiferForTest()
		require.True(t, m.tasksCountForTest() == 1)
		require.True(t, m.PendingAllocSize() == requestSize)
		require.True(t, m.PendingAllocSize() > m.limit()) // always success even when over limit

		require.Equal(t, m.frontTaskEntryForTest(), entry1)
		require.True(t, m.getRootPoolEntry(entry1.pool.uid) == entry1)
		require.True(t, m.TaskNumByMode() == waitingTaskNum{0, 1, 0, 0})
		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityMedium, 0)
		require.Equal(t, -1, m.runOneRound()) // always -1
		require.True(t, m.execMu.blockedState.allocated == 0)
		require.Equal(t, m.waitAlloc(entry1), ArbitrateOk)
		require.True(t, m.tasksCountForTest() == 0)

		require.False(t, m.findTaskByMode(entry1, ArbitrateMemPriorityMedium, NoWaitAverse))
		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityMedium, requestSize)

		m.checkTaskExec(pairSuccessFail{1, 0}, 0, numByAllMode{}) // always success

		require.True(t, m.privilegedEntry == nil) // can not get privilege
		require.True(t, entry1.execState() == execStateRunning)
		require.True(t, entry1.state() == PoolEntryStateNormal)

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
			require.Equal(t, ArbitrateMemPriorityMedium, e0.taskMu.fifoByPriority.priority)
			m.resetRootPoolEntry(e0) // reset entry
			m.restartEntryForTest(e0, m.newCtxWithHelperForTest(ArbitrateMemPriorityLow, NoWaitAverse, false))
			require.Equal(t, ArbitrateMemPriorityLow, e0.ctx.memPriority)
			require.Equal(t, ArbitrateMemPriorityMedium, e0.taskMu.fifoByPriority.priority)
			if m.execMu.mode == ArbitratorModeDisable {
				m.implicitRun()
			}
			require.True(t, m.waitAlloc(e0) == ArbitrateOk)
			require.True(t, e0.arbitratorMu.quota == 2)
			require.True(t, e0.arbitratorMu.quota == e0.pool.Capacity()+e0.stateMu.pendingReset.quota)
			require.True(t, m.cleanupMu.fifoTasks.num == 1)

			m.resetRootPoolEntry(e0) // 1. reset entry
			m.removeEntryForTest(e0) // 2. delete entry
			require.True(t, m.cleanupMu.fifoTasks.num == 3)
			m.prepareAlloc(e0, 1)                  // alloc for non-exist entry
			require.True(t, m.runOneRound() == -1) // no task
			require.True(t, m.waitAlloc(e0) == ArbitrateFail)
			require.True(t, m.getRootPoolEntry(e0.pool.uid) == nil)
			require.True(t, m.RootPoolNum() == 1)
			m.prepareAlloc(e0, 1)                  // alloc for non-exist entry
			require.True(t, m.runOneRound() == -1) // wind up non-exist entry with fail
			require.True(t, m.waitAlloc(e0) == ArbitrateFail)

			e1 := m.addEntryForTest(nil)
			m.removeEntryForTest(e1)                        // 1. delete entry
			require.False(t, m.resetRootPoolEntry(e1))      // 2. reset entry failed
			require.True(t, m.cleanupMu.fifoTasks.num == 1) //
			m.prepareAlloc(e1, 1)
			if m.execMu.mode == ArbitratorModeDisable {
				m.implicitRun()
			}
			require.True(t, m.waitAlloc(e1) == ArbitrateOk) // success
			m.doExecuteCleanupTasks()
		}
	}

	entry2 := genTestPoolWithHelper(ArbitrateMemPriorityMedium, NoWaitAverse, false)
	m.resetExecMetrics()
	m.cleanupNotifer()
	{ // Disable mode -> Standard mode
		require.True(t, m.allocated() == 2*newLimit)

		requestSize := int64(1)

		m.prepareAlloc(entry2, requestSize)
		m.waitNotiferForTest()
		require.True(t, m.tasksCountForTest() == 1)
		require.True(t, m.PendingAllocSize() == requestSize)

		require.Equal(t, m.frontTaskEntryForTest(), entry2)
		require.True(t, entry2.request.quota == requestSize)

		require.True(t, m.findTaskByMode(entry2, ArbitrateMemPriorityMedium, NoWaitAverse))
		require.True(t, m.TaskNumByMode() == waitingTaskNum{0, 1, 0, 0})
		m.checkEntryQuotaByPriority(entry2, ArbitrateMemPriorityMedium, 0)

		m.SetWorkMode(ArbitratorModeStandard)
		require.Equal(t, m.WorkMode(), ArbitratorModeStandard)

		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 2*newLimit)

		m.checkTaskExec(pairSuccessFail{}, 1, numByAllMode{})
		require.True(t, actionCancel[entry2.pool.uid] == 1)
		require.True(t, m.waitAlloc(entry2) == ArbitrateFail)

		require.False(t, m.findTaskByMode(entry2, ArbitrateMemPriorityMedium, NoWaitAverse))
		m.checkEntryQuotaByPriority(entry2, ArbitrateMemPriorityMedium, 0)
		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityMedium, newLimit*2)
		m.checkTaskExec(pairSuccessFail{0, 1}, 1, numByAllMode{})

		require.True(t, m.tasksCountForTest() == 0)
		require.True(t, entry1.execState() == execStateRunning)
		require.True(t, entry2.execState() == execStateRunning)
		require.True(t, entry1.state() == PoolEntryStateNormal)
		require.True(t, entry2.state() == PoolEntryStateNormal)
		m.checkEntryForTest(entry1, entry2)

		entrys := make([]*rootPoolEntry, 0)
		for prio := minArbitrateMemPriority; prio < maxArbitrateMemPriority; prio++ {
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

		m.checkTaskExec(pairSuccessFail{0, 7}, 7, numByAllMode{})
		m.deleteEntryForTest(newEntries...)
		m.checkEntryForTest(entry1, entry2)
	}

	m.resetExecMetrics()
	m.cleanupNotifer()
	actionCancel = make(map[uint64]int)
	{ // Standard mode -> Priority mode: wait until cancel self
		require.True(t, m.allocated() == 2*newLimit)

		requestSize := int64(1)
		m.resetEntryForTest(entry2)
		m.restartEntryForTest(entry2, genTestCtx(entry2, ArbitrateMemPriorityMedium, NoWaitAverse, false))
		m.prepareAlloc(entry2, requestSize)
		m.waitNotiferForTest()
		require.True(t, m.tasksCountForTest() == 1)
		require.True(t, m.PendingAllocSize() == requestSize)

		e := m.frontTaskEntryForTest()
		require.Equal(t, e, entry2)

		require.True(t, m.findTaskByMode(e, ArbitrateMemPriorityMedium, NoWaitAverse))
		require.True(t, m.TaskNumByMode() == waitingTaskNum{0, 1, 0, 0})
		m.checkEntryQuotaByPriority(e, ArbitrateMemPriorityMedium, 0)
		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityMedium, newLimit*2)

		m.SetWorkMode(ArbitratorModePriority)
		require.Equal(t, m.WorkMode(), ArbitratorModePriority)

		require.Equal(t, m.runOneRound(), 0)
		require.True(t, m.tasksCountForTest() == 1)
		require.True(t, m.execMu.blockedState.allocated == 2*newLimit)
		require.True(t, m.PendingAllocSize() == requestSize)
		m.checkTaskExec(pairSuccessFail{}, 0, numByAllMode{})

		require.True(t, m.findTaskByMode(e, ArbitrateMemPriorityMedium, NoWaitAverse))
		require.True(t, m.TaskNumByMode() == waitingTaskNum{0, 1, 0, 0})
		m.checkEntryQuotaByPriority(e, ArbitrateMemPriorityMedium, 0)
		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityMedium, newLimit*2)

		entry2.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).cancelSelf()
		require.True(t, m.waitAlloc(entry2) == ArbitrateFail)
		m.checkTaskExec(pairSuccessFail{0, 1}, 0, numByAllMode{})
		require.True(t, len(actionCancel) == 0)

		require.True(t, m.tasksCountForTest() == 0)
		require.True(t, entry1.execState() == execStateRunning)
		require.True(t, entry2.execState() == execStateRunning)
		require.True(t, entry1.state() == PoolEntryStateNormal)
		require.True(t, entry2.state() == PoolEntryStateNormal)
		m.checkEntryForTest(entry1, entry2)
	}

	m.resetExecMetrics()
	m.cleanupNotifer()
	{ // Priority mode: interrupt lower priority tasks
		require.True(t, len(actionCancel) == 0)
		require.True(t, m.allocated() == 2*newLimit)

		requestSize := int64(1)

		m.resetEntryForTest(entry2)
		m.restartEntryForTest(entry2, newDefCtxForTest(ArbitrateMemPriorityHigh))
		m.prepareAlloc(entry2, requestSize)

		m.waitNotiferForTest()
		require.True(t, m.tasksCountForTest() == 1)
		require.True(t, m.PendingAllocSize() == requestSize)
		e := m.frontTaskEntryForTest()
		require.Equal(t, entry2, m.getRootPoolEntry(e.pool.uid))

		require.True(t, entry2.request.quota == requestSize)

		require.True(t, m.findTaskByMode(entry2, ArbitrateMemPriorityHigh, NoWaitAverse))
		require.True(t, m.TaskNumByMode() == waitingTaskNum{0, 0, 1, 0})
		m.checkEntryQuotaByPriority(entry2, ArbitrateMemPriorityHigh, 0)
		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityMedium, newLimit*2)

		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 2*newLimit)

		m.checkTaskExec(pairSuccessFail{}, 0, numByAllMode{0, 1, 0})

		require.True(t, m.findTaskByMode(entry2, ArbitrateMemPriorityHigh, NoWaitAverse))
		require.True(t, m.TaskNumByMode() == waitingTaskNum{0, 0, 1, 0})
		m.checkEntryQuotaByPriority(entry2, ArbitrateMemPriorityHigh, 0)
		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityMedium, newLimit*2)

		require.True(t, actionCancel[entry1.pool.uid] == 1) // interrupt entry1
		m.resetRootPoolEntry(entry1)

		require.True(t, entry1.arbitratorMu.underCancel.bool)
		require.True(t, entry1.arbitratorMu.underCancel.reclaim == entry1.arbitratorMu.quota)
		require.Equal(t, mapEntryWithMem{entries: map[uint64]*rootPoolEntry{
			entry1.pool.uid: entry1,
		}, num: 1}, m.underCancel)

		require.Equal(t, 1, m.runOneRound())
		require.Equal(t, m.waitAlloc(entry2), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{1, 0}, 0, numByAllMode{0, 1, 0})
		require.Equal(t, mapEntryWithMem{entries: map[uint64]*rootPoolEntry{}, num: 0}, m.underCancel)
		require.True(t, m.tasksCountForTest() == 0)

		require.False(t, m.findTaskByMode(entry2, ArbitrateMemPriorityHigh, NoWaitAverse))
		m.checkEntryQuotaByPriority(entry2, ArbitrateMemPriorityHigh, requestSize)
		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityMedium, 0)

		require.True(t, entry1.execState() == execStateIdle)
		require.True(t, entry2.execState() == execStateRunning)
		require.True(t, entry1.state() == PoolEntryStateNormal)
		require.True(t, entry2.state() == PoolEntryStateNormal)
		m.checkEntryForTest(entry1, entry2)
		m.resetEntryForTest(entry1, entry2)
	}

	m.resetExecMetrics()
	m.cleanupNotifer()
	{ // Priority mode -> Disable mode
		require.True(t, m.allocated() == 0)
		m.awaitFree.pool.allocate(123)

		requestSize := newLimit + 1
		m.restartEntryForTest(entry1, newDefCtxForTest(ArbitrateMemPriorityLow))
		m.prepareAlloc(entry1, requestSize)
		m.restartEntryForTest(entry2, newDefCtxForTest(ArbitrateMemPriorityLow))
		m.prepareAlloc(entry2, requestSize)

		m.waitNotiferForTest()
		require.True(t, m.tasksCountForTest() == 2)
		require.True(t, m.PendingAllocSize() == requestSize*2)

		require.True(t, m.findTaskByMode(entry1, ArbitrateMemPriorityLow, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry2, ArbitrateMemPriorityLow, NoWaitAverse))
		require.True(t, m.TaskNumByMode() == waitingTaskNum{2, 0, 0, 0})
		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityLow, 0)
		require.False(t, entry2.ctx.waitAverse)
		require.True(t, entry2.ctx.memPriority == ArbitrateMemPriorityLow)

		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 123)
		m.checkTaskExec(pairSuccessFail{}, 0, numByAllMode{0, 0, 0})

		require.True(t, m.tasksCountForTest() == 2)
		require.True(t, m.PendingAllocSize() == requestSize*2)

		require.True(t, m.findTaskByMode(entry1, ArbitrateMemPriorityLow, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry2, ArbitrateMemPriorityLow, NoWaitAverse))
		require.True(t, m.TaskNumByMode() == waitingTaskNum{2, 0, 0, 0})
		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityLow, 0)
		m.checkEntryQuotaByPriority(entry2, ArbitrateMemPriorityLow, 0)

		// set work mode to `ArbitratorModeDisable` and make all subscriptions success
		m.SetWorkMode(ArbitratorModeDisable)

		require.Equal(t, -1, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 0) // reset because of new work mode

		require.Equal(t, m.waitAlloc(entry1), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry2), ArbitrateOk)
		require.False(t, m.findTaskByMode(entry1, ArbitrateMemPriorityLow, NoWaitAverse))
		require.False(t, m.findTaskByMode(entry2, ArbitrateMemPriorityLow, NoWaitAverse))
		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityLow, requestSize)
		m.checkEntryQuotaByPriority(entry2, ArbitrateMemPriorityLow, requestSize)
		require.True(t, m.allocated() == requestSize*2+m.awaitFreePoolCap())
		require.True(t, m.tasksCountForTest() == 0)
		m.awaitFree.pool.release(m.awaitFree.pool.allocated())
		m.clearAwaitFreeForTest()
		m.checkEntryForTest(entry1, entry2)
		m.checkTaskExec(pairSuccessFail{2, 0}, 0, numByAllMode{})
		m.resetEntryForTest(entry1, entry2)
		require.True(t, m.allocated() == 0)
	}

	m.SetWorkMode(ArbitratorModePriority)
	m.resetExecMetrics()
	actionCancel = make(map[uint64]int)
	{ // Priority mode: mixed task mode ArbitrateWaitAverse
		allocUnit := int64(4000)
		m.restartEntryForTest(entry1, genTestCtx(entry1, ArbitrateMemPriorityLow, NoWaitAverse, false))
		m.restartEntryForTest(entry2, genTestCtx(entry2, ArbitrateMemPriorityMedium, NoWaitAverse, false))
		entry3 := genTestPoolWithHelper(ArbitrateMemPriorityHigh, NoWaitAverse, false)
		entry4 := genTestPoolWithHelper(ArbitrateMemPriorityLow, NoWaitAverse, false)
		entry5 := genTestPoolWithHelper(ArbitrateMemPriorityHigh, true, false)
		m.checkEntryForTest(entry1, entry2, entry3, entry4, entry5)

		m.setLimitForTest(8 * allocUnit)

		m.prepareAlloc(entry1, allocUnit)
		m.prepareAlloc(entry2, allocUnit)
		m.prepareAlloc(entry3, allocUnit)
		m.prepareAlloc(entry4, allocUnit*4)
		m.prepareAlloc(entry5, allocUnit)

		require.True(t, m.findTaskByMode(entry1, ArbitrateMemPriorityLow, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry2, ArbitrateMemPriorityMedium, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry3, ArbitrateMemPriorityHigh, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry4, ArbitrateMemPriorityLow, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry5, ArbitrateMemPriorityHigh, true))

		require.True(t, m.tasksCountForTest() == 5)
		require.True(t, m.PendingAllocSize() == 8*allocUnit)
		require.True(t, m.TaskNumByMode() == waitingTaskNum{2, 1, 2, 1})

		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityLow, 0)
		m.checkEntryQuotaByPriority(entry2, ArbitrateMemPriorityMedium, 0)
		m.checkEntryQuotaByPriority(entry3, ArbitrateMemPriorityHigh, 0)
		m.checkEntryQuotaByPriority(entry4, ArbitrateMemPriorityLow, 0)
		m.checkEntryQuotaByPriority(entry5, ArbitrateMemPriorityHigh, 0)

		require.Equal(t, 5, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 0)
		require.True(t, m.allocated() == 8*allocUnit)

		require.Equal(t, m.waitAlloc(entry1), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry2), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry3), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry4), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry5), ArbitrateOk)

		m.checkTaskExec(pairSuccessFail{5, 0}, 0, numByAllMode{})
		m.checkEntryQuotaByPriority(entry1, ArbitrateMemPriorityLow, allocUnit)
		m.checkEntryQuotaByPriority(entry2, ArbitrateMemPriorityMedium, allocUnit)
		m.checkEntryQuotaByPriority(entry3, ArbitrateMemPriorityHigh, allocUnit)
		m.checkEntryQuotaByPriority(entry4, ArbitrateMemPriorityLow, allocUnit*4)
		m.checkEntryQuotaByPriority(entry5, ArbitrateMemPriorityHigh, allocUnit)

		require.NotEqual(t,
			m.entryMap.getQuotaShard(entry4.ctx.memPriority, entry4.arbitratorMu.quota),
			m.entryMap.getQuotaShard(entry1.ctx.memPriority, entry1.arbitratorMu.quota))

		m.prepareAlloc(entry1, allocUnit)
		m.prepareAlloc(entry2, allocUnit)
		m.prepareAlloc(entry3, allocUnit)
		m.prepareAlloc(entry4, allocUnit)
		m.prepareAlloc(entry5, allocUnit)

		require.True(t, m.findTaskByMode(entry1, ArbitrateMemPriorityLow, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry2, ArbitrateMemPriorityMedium, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry3, ArbitrateMemPriorityHigh, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry4, ArbitrateMemPriorityLow, NoWaitAverse))
		require.True(t, m.findTaskByMode(entry5, ArbitrateMemPriorityHigh, true))
		require.True(t, m.tasksCountForTest() == 5)
		require.True(t, m.PendingAllocSize() == 5*allocUnit)
		require.True(t, m.TaskNumByMode() == waitingTaskNum{2, 1, 2, 1})

		require.True(t, m.underCancel.num == 0)
		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 8*allocUnit)

		require.Equal(t, m.waitAlloc(entry5), ArbitrateFail)
		require.Equal(t, m.waitAlloc(entry4), ArbitrateFail)
		require.True(t, actionCancel[entry5.pool.uid] == 1) // cancel self
		require.False(t, entry5.arbitratorMu.underCancel.bool)
		require.True(t, actionCancel[entry4.pool.uid] == 1) // cancel by entry3
		require.True(t, entry4.arbitratorMu.underCancel.bool)
		require.True(t, m.underCancel.num == 1)
		require.True(t, m.underCancel.entries[entry4.pool.uid] != nil)
		require.True(t, entry4.arbitratorMu.underCancel.bool)
		m.checkTaskExec(pairSuccessFail{5, 2}, 0, numByAllMode{1, 0, 0, 1})
		require.True(t, m.TaskNumByMode() == waitingTaskNum{1, 1, 1, 0})
		require.True(t, m.tasksCountForTest() == 3)
		require.True(t, m.PendingAllocSize() == 3*allocUnit)

		oriMetrics := m.execMetrics
		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 8*allocUnit)
		require.True(t, m.execMetrics == oriMetrics)

		m.resetEntryForTest(entry5)
		require.Equal(t, 1, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 8*allocUnit)
		require.Equal(t, m.waitAlloc(entry3), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{6, 2}, 0, numByAllMode{1, 0, 0, 1})
		require.True(t, m.tasksCountForTest() == 2)
		require.True(t, m.PendingAllocSize() == 2*allocUnit)
		m.setLimitForTest(1 * allocUnit)
		m.resetEntryForTest(entry3, entry4)
		require.True(t, m.underCancel.num == 0)

		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.execMu.blockedState.allocated == 2*allocUnit)

		m.checkTaskExec(pairSuccessFail{6, 2}, 0, numByAllMode{2, 0, 0, 1})
		require.True(t, m.underCancel.num == 1)
		require.True(t, m.underCancel.entries[entry1.pool.uid] != nil)
		require.True(t, entry1.arbitratorMu.underCancel.bool)
		require.Equal(t, m.waitAlloc(entry1), ArbitrateFail)
		m.checkTaskExec(pairSuccessFail{6, 3}, 0, numByAllMode{2, 0, 0, 1})
		require.True(t, actionCancel[entry1.pool.uid] == 1) // cancel by entry2

		m.setLimitForTest(3 * allocUnit)
		require.Equal(t, 1, m.runOneRound())
		require.Equal(t, m.waitAlloc(entry2), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{7, 3}, 0, numByAllMode{2, 0, 0, 1})
		require.True(t, m.tasksCountForTest() == 0)

		m.resetEntryForTest(entry1, entry2)

		m.resetExecMetrics()

		actionCancel = make(map[uint64]int)

		m.restartEntryForTest(entry1, genTestCtx(entry1, ArbitrateMemPriorityLow, NoWaitAverse, false))
		m.restartEntryForTest(entry2, genTestCtx(entry2, ArbitrateMemPriorityMedium, NoWaitAverse, false))
		m.restartEntryForTest(entry3, genTestCtx(entry3, ArbitrateMemPriorityHigh, NoWaitAverse, false))
		m.restartEntryForTest(entry4, genTestCtx(entry4, ArbitrateMemPriorityLow, NoWaitAverse, false))
		m.restartEntryForTest(entry5, genTestCtx(entry5, ArbitrateMemPriorityLow, NoWaitAverse, false))

		m.setLimitForTest(BaseQuotaUnit * 8)

		m.prepareAlloc(entry1, 1)
		m.prepareAlloc(entry3, BaseQuotaUnit*4)
		m.prepareAlloc(entry4, BaseQuotaUnit)
		m.prepareAlloc(entry5, BaseQuotaUnit*2)

		require.True(t, m.tasksCountForTest() == 4)
		require.True(t, m.TaskNumByMode() == waitingTaskNum{3, 0, 1, 0})
		require.True(t, m.runOneRound() == 4)

		alloced := entry1.arbitratorMu.quota + entry3.arbitratorMu.quota + entry4.arbitratorMu.quota + entry5.arbitratorMu.quota
		require.True(t, m.allocated() == alloced)
		require.True(t, m.TaskNumByMode() == waitingTaskNum{0, 0, 0, 0})

		require.Equal(t, m.waitAlloc(entry1), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry3), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry4), ArbitrateOk)
		require.Equal(t, m.waitAlloc(entry5), ArbitrateOk)

		require.True(t, m.tasksCountForTest() == 0)

		m.checkTaskExec(pairSuccessFail{4, 0}, 0, numByAllMode{})
		m.prepareAlloc(entry2, BaseQuotaUnit*3)
		require.True(t, m.tasksCountForTest() == 1)

		require.Equal(t, 0, m.runOneRound())
		require.True(t, len(actionCancel) == 2)
		require.True(t, actionCancel[entry4.pool.uid] == 1)
		require.True(t, actionCancel[entry5.pool.uid] == 1)
		m.checkTaskExec(pairSuccessFail{4, 0}, 0, numByAllMode{2, 0, 0, 0})
		require.True(t, m.TaskNumByMode() == waitingTaskNum{0, 1, 0, 0})
		require.True(t, m.tasksCountForTest() == 1)
		require.True(t, m.underCancel.num == 2)
		require.True(t, entry4.arbitratorMu.underCancel.bool)
		require.True(t, entry5.arbitratorMu.underCancel.bool)

		m.ResetRootPoolByID(entry4.pool.uid, 0, false)

		selfCancelCh := make(chan struct{})
		m.restartEntryForTest(entry4, newCtxForTest(selfCancelCh, nil, entry4.ctx.memPriority, NoWaitAverse, false))
		m.prepareAlloc(entry4, BaseQuotaUnit*1000)
		require.True(t, m.tasksCountForTest() == 2)

		require.True(t, entry4.state() != PoolEntryStateStop && entry4.execState() != execStateIdle && entry4.stateMu.pendingReset.quota > 0)
		require.True(t, entry4.notRunning())

		require.True(t, m.cleanupMu.fifoTasks.size() == 1)
		require.True(t, m.cleanupMu.fifoTasks.front() == entry4)
		require.Equal(t, 0, m.runOneRound())
		require.True(t, m.cleanupMu.fifoTasks.size() == 0)
		require.True(t, m.TaskNumByMode() == waitingTaskNum{1, 1, 0, 0})
		require.True(t, !entry4.arbitratorMu.underCancel.bool)
		require.True(t, !entry4.notRunning())
		require.True(t, entry4.state() != PoolEntryStateStop && entry4.execState() != execStateIdle && entry4.stateMu.pendingReset.quota == 0)

		m.checkTaskExec(pairSuccessFail{4, 0}, 0, numByAllMode{2, 0, 0, 0})
		require.Equal(t, mapEntryWithMem{entries: map[uint64]*rootPoolEntry{
			entry5.pool.uid: entry5,
		}, num: 1}, m.underCancel)

		close(selfCancelCh)
		require.True(t, m.removeTask(entry4))
		require.True(t, m.TaskNumByMode() == waitingTaskNum{0, 1, 0, 0})
		require.True(t, selfCancelCh == entry4.ctx.cancelCh)
		wg := newNotiferWrap(nil)
		oriFailCnt := m.execMetrics.task.fail
		go func() {
			defer wg.close()
			for oriFailCnt == atomic.LoadInt64(&m.execMetrics.task.fail) {
				runtime.Gosched()
			}
			entry4.windUp(0, ArbitrateOk)
		}()
		require.Equal(t, m.waitAlloc(entry4), ArbitrateFail) // cancel self
		wg.wait()
		m.checkTaskExec(pairSuccessFail{4, 1}, 0, numByAllMode{2, 0, 0, 0})
		m.removeEntryForTest(entry5)

		require.True(t, entry5.notRunning())
		require.True(t, entry5.state() == PoolEntryStateStop)
		require.True(t, !entry5.arbitratorMu.destroyed)
		require.True(t, entry5.arbitratorMu.underCancel.bool)
		require.True(t, entry5.arbitratorMu.quota != 0)

		require.True(t, m.cleanupMu.fifoTasks.size() == 1)
		require.Equal(t, 1, m.runOneRound())
		require.True(t, m.underCancel.num == 0)
		require.True(t, m.cleanupMu.fifoTasks.size() == 0)
		require.True(t, m.execMetrics.cancel.priorityMode == numByPriority{2, 0, 0})
		require.True(t, m.execMetrics.cancel.waitAverse == 0)

		require.True(t, entry5.arbitratorMu.destroyed)
		require.True(t, !entry5.arbitratorMu.underCancel.bool)
		require.True(t, entry5.arbitratorMu.quota == 0)

		require.True(t, m.allocated() == alloced)
		require.Equal(t, m.waitAlloc(entry2), ArbitrateOk)
		require.True(t, m.execMetrics.task.pairSuccessFail == pairSuccessFail{5, 1})

		m.cleanupNotifer()
	}
}

func TestMemArbitrator(t *testing.T) {
	testState = t

	type MockHeap [4]int64 // alloc, heapInuse, totalAlloc, stackInuse
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

	m.debug.now = func() time.Time {
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
		require.Equal(t, len(m.entryMap.shards), int(expectShardCount))
		require.Equal(t, m.entryMap.shardsMask, uint64(expectShardCount-1))
		require.Equal(t, len(m.entryMap.shards), expectShardCount)

		for pio := minArbitrateMemPriority; pio < maxArbitrateMemPriority; pio++ {
			require.Equal(t, len(m.entryMap.quotaShards[pio]), m.entryMap.maxQuotaShardIndex)
		}

		require.Equal(t, m.runOneRound(), 0) // no task has been executed successfully
		m.checkEntryForTest()

		pool1AlignSize := int64(4)
		pool1 := newResourcePoolForTest("root1", pool1AlignSize)
		uid1 := pool1.Uid()
		pb1 := pool1.CreateBudget()
		{ // no out-of-memory-action

			require.Equal(t, pool1.limit, int64(math.MaxInt64))
			require.Equal(t, pool1.mu.budget.Used(), int64(0))
			require.Equal(t, pool1.mu.budget.Capacity(), int64(0))
			require.True(t, pool1.mu.budget.Pool() == nil)

			require.Error(t, pb1.Grow(1)) // can not grow
			require.NoError(t, pb1.Grow(0))
			require.True(t, m.getRootPoolEntry(uid1) == nil)

			require.False(t, m.notiferIsAwake())
			m.checkTaskExec(pairSuccessFail{}, 0, numByAllMode{})
		}

		entry1 := m.addRootPoolForTest(pool1, nil)
		{ // with out-of-memory-action
			m.checkEntryForTest(entry1)
			require.True(t, pool1.actions.outOfCapacityAction != nil)
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
				m.checkEntryQuotaByPriority(e, ArbitrateMemPriorityMedium, 0)
				require.True(t, m.findTaskByMode(e, ArbitrateMemPriorityMedium, NoWaitAverse))

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
			require.True(t, m.execMetrics.task.pairSuccessFail == pairSuccessFail{1, 0})
			require.Equal(t, execMetricsCancel{}, m.execMetrics.cancel)
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
			m.checkTaskExec(pairSuccessFail{2, 0}, 0, numByAllMode{})
			m.cleanupNotifer()
			m.checkEntryForTest(entry1)

			// check diff quota shard
			require.True(t, m.getQuotaShard(entry1.ctx.memPriority, entry1.arbitratorMu.quota) ==
				m.getQuotaShard(ArbitrateMemPriorityMedium, 0))

			wg = newNotiferWrap(m)
			go func() {
				defer wg.close()
				m.waitNotiferForTest()

				require.True(t, m.tasksCountForTest() == 1) // pool1 need to be arbitrated
				e := m.frontTaskEntryForTest()
				require.True(t, e == entry1)
				require.Equal(t, m.runOneRound(), 1)
			}()
			require.NoError(t, pb1.Grow(BaseQuotaUnit))
			wg.wait()
			require.True(t, m.getQuotaShard(entry1.ctx.memPriority, entry1.arbitratorMu.quota) ==
				m.getQuotaShard(ArbitrateMemPriorityMedium, BaseQuotaUnit*2-1))
			m.checkTaskExec(pairSuccessFail{3, 0}, 0, numByAllMode{})
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
			require.Equal(t, ArbitrateMemPriorityMedium, e0.taskMu.fifoByPriority.priority)
			m.resetRootPoolEntry(e0) // reset entry
			m.restartEntryForTest(e0, m.newCtxWithHelperForTest(ArbitrateMemPriorityLow, NoWaitAverse, false))
			require.Equal(t, ArbitrateMemPriorityLow, e0.ctx.memPriority)
			require.Equal(t, ArbitrateMemPriorityMedium, e0.taskMu.fifoByPriority.priority)
			m.doExecuteFirstTask()
			require.True(t, m.waitAlloc(e0) == ArbitrateOk)
			require.True(t, e0.arbitratorMu.quota == 2)
			require.True(t, e0.arbitratorMu.quota == e0.pool.Capacity()+e0.stateMu.pendingReset.quota)
			require.True(t, m.cleanupMu.fifoTasks.num == 1)

			m.resetRootPoolEntry(e0) // 1. reset entry
			m.removeEntryForTest(e0) // 2. delete entry
			require.True(t, m.cleanupMu.fifoTasks.num == 3)
			m.prepareAlloc(e0, 1)                 // alloc for non-exist entry
			require.True(t, m.runOneRound() == 0) // no task
			require.True(t, m.waitAlloc(e0) == ArbitrateFail)
			require.True(t, m.getRootPoolEntry(e0.pool.uid) == nil)
			require.True(t, m.RootPoolNum() == 1)
			m.prepareAlloc(e0, 1)                 // alloc for non-exist entry
			require.True(t, m.runOneRound() == 1) // wind up non-exist entry with fail
			require.True(t, m.waitAlloc(e0) == ArbitrateFail)

			e1 := m.addEntryForTest(nil)
			m.removeEntryForTest(e1)                        // 1. delete entry
			require.False(t, m.resetRootPoolEntry(e1))      // 2. reset entry failed
			require.True(t, m.cleanupMu.fifoTasks.num == 1) //
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
			require.NoError(t, b.Grow(BaseQuotaUnit))
			require.True(t, m.stopForTest())
			require.False(t, m.stopForTest())
			require.True(t, !m.controlMu.running)
			require.Equal(t, m.Allocated(), b.cap)
			require.Equal(t, m.limit(), newLimit)
			m.checkEntryForTest(entry1)
			m.resetEntryForTest(entry1)

			m.resetExecMetrics()
			cancel := make(chan struct{})
			m.restartEntryForTest(entry1, newCtxForTest(cancel, nil, ArbitrateMemPriorityMedium, NoWaitAverse, false))
			wg := newNotiferWrap(m)
			go func() {
				defer wg.close()
				m.waitNotiferForTest()

				require.True(t, m.tasksCountForTest() == 1)
				require.True(t, m.frontTaskEntryForTest() == entry1)
				close(cancel)
			}()
			// blocking grow with cancel
			require.Equal(t, errArbitrateFailError, b.Grow(BaseQuotaUnit))
			wg.wait()
			require.True(t, m.tasksCountForTest() == 0)
			m.checkTaskExec(pairSuccessFail{0, 1}, 0, numByAllMode{})
			m.resetEntryForTest(entry1)

			cancel = make(chan struct{})
			m.restartEntryForTest(entry1, newCtxForTest(cancel, nil, ArbitrateMemPriorityMedium, NoWaitAverse, false))
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
			m.checkTaskExec(pairSuccessFail{0, 2}, 1, numByAllMode{})
		}

		m.deleteEntryForTest(entry1)
	}

	// Priority mode
	m.SetWorkMode(ArbitratorModePriority)
	require.Equal(t, m.WorkMode(), ArbitratorModePriority)
	{ // test panic
		{
			pool := NewResourcePool("?", 1)
			pool.Start(nil, 1)
			require.Equal(t, pool.reserved, int64(1))
			require.PanicsWithError(t, "?: has 1 reserved budget left", func() { m.addRootPoolForTest(pool, nil) })
		}
		{
			pool := NewResourcePool("?", 1)
			pool.forceAddCap(1)
			require.True(t, pool.ApproxCap() != 0)
			require.PanicsWithError(t, "?: has 1 bytes budget left", func() { m.addRootPoolForTest(pool, nil) })
		}
		{
			p1 := NewResourcePool("p1", 1)
			p2 := NewResourcePool("p2", 1)
			p3 := NewResourcePool("p3", 1)
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
		m.resetExecMetrics()
		m.setLimitForTest(newLimit)
		e1 := m.newPoolWithHelperForTest(ArbitrateMemPriorityLow, NoWaitAverse, true)
		e1.ctx.Load().arbitrateHelper = nil
		require.True(t, m.privilegedEntry == nil)
		require.True(t, e1.ctx.preferPrivilege)

		reqQuota := newLimit + 1

		m.prepareAlloc(e1, reqQuota)
		require.True(t, m.findTaskByMode(e1, ArbitrateMemPriorityLow, NoWaitAverse))
		require.True(t, m.runOneRound() == 1)
		require.Equal(t, m.waitAlloc(e1), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{1, 0}, 0, numByAllMode{})
		m.checkEntryQuotaByPriority(e1, ArbitrateMemPriorityLow, reqQuota)
		require.True(t, m.privilegedEntry == e1)

		e2 := m.newPoolWithHelperForTest(ArbitrateMemPriorityHigh, true, true)
		e2.ctx.Load().arbitrateHelper = nil

		require.True(t, e2.ctx.Load().preferPrivilege)
		require.False(t, e2.ctx.preferPrivilege)
		m.prepareAlloc(e2, reqQuota)
		require.True(t, m.findTaskByMode(e2, ArbitrateMemPriorityHigh, true))
		require.Equal(t, 0, m.runOneRound())
		require.Equal(t, m.waitAlloc(e2), ArbitrateFail)
		m.checkTaskExec(pairSuccessFail{1, 1}, 0, numByAllMode{0, 0, 0, 1})
		m.checkEntryForTest(e1, e2)
		require.True(t, m.privilegedEntry == e1)

		e3 := m.newPoolWithHelperForTest(ArbitrateMemPriorityLow, NoWaitAverse, true)
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
		m.checkTaskExec(pairSuccessFail{2, 1}, 0, numByAllMode{0, 0, 0, 1})

		m.resetRootPoolEntry(e1) // to be handled by arbitrator later
		m.restartEntryForTest(e1, e1.ctx.Load())
		m.prepareAlloc(e1, reqQuota)

		require.True(t, m.tasksCountForTest() == 2)
		require.True(t, m.privilegedEntry == e1)
		require.True(t, m.runOneRound() == 1)
		require.Equal(t, m.waitAlloc(e3), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{3, 1}, 0, numByAllMode{0, 0, 0, 1})
		require.True(t, m.privilegedEntry == e3)
		m.setLimitForTest((m.PendingAllocSize() + m.allocated()))
		require.True(t, m.runOneRound() == 1)
		require.Equal(t, m.waitAlloc(e1), ArbitrateOk)
		m.checkTaskExec(pairSuccessFail{4, 1}, 0, numByAllMode{0, 0, 0, 1})
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
		require.Equal(t, m.mu.threshold.oomRisk, int64(x*DefCheckOOMRatio))
		require.Equal(t, m.mu.threshold.risk, int64(x*DefCheckSafeRatio))
		require.Equal(t, m.mu.softLimit.size, m.mu.threshold.oomRisk)
		require.Equal(t, m.mu.limit, int64(x))
		m.cleanupNotifer()
		require.True(t, m.SetLimit(uint64(DefMaxLimit)+1))
		require.True(t, m.notiferIsAwake())
		require.Equal(t, m.mu.limit, DefMaxLimit)
		require.Equal(t, m.mu.threshold.oomRisk, int64(float64(DefMaxLimit)*DefCheckOOMRatio))
		require.Equal(t, m.mu.threshold.risk, int64(float64(DefMaxLimit)*DefCheckSafeRatio))
		require.True(t, m.mu.softLimit.specified.size == 0)
		require.Equal(t, m.mu.softLimit.size, m.mu.threshold.oomRisk)
		require.True(t, m.mu.softLimit.mode == SoftLimitModeDefault)

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
		require.True(t, m.mu.softLimit.specified.rate == int64(sortLimitRate*1000))
		require.Equal(t, m.mu.softLimit.size, int64(sortLimitRate*float64(m.mu.limit)))
		require.True(t, m.SetLimit(200))
		require.Equal(t, m.mu.softLimit.size, int64(sortLimitRate*200))

		m.SetSoftLimit(0, 0, SoftLimitModeDefault)
		require.Equal(t, m.mu.softLimit.size, m.mu.threshold.oomRisk)
		require.True(t, m.mu.softLimit.mode == SoftLimitModeDefault)
		require.True(t, m.SetLimit(100))
		require.Equal(t, m.mu.softLimit.size, m.mu.threshold.oomRisk)

		m.SetSoftLimit(0, 0, SoftLimitModeAuto)
		require.Equal(t, m.mu.softLimit.size, m.mu.threshold.oomRisk)
		require.True(t, m.mu.softLimit.mode == SoftLimitModeAuto)
		require.True(t, m.SetLimit(200))
		require.Equal(t, m.mu.softLimit.size, m.mu.threshold.oomRisk)
	}
	{ // test out-of-control
		m.SetSoftLimit(0, 0, SoftLimitModeDefault)

		require.True(t, m.awaitFreePoolCap() == 0)
		require.True(t, m.allocated() == 0)

		eleSize := m.awaitFree.pool.allocAlignSize + 1
		budgetsNum := int64(len(m.awaitFree.budget.shards))
		hpMemUsed := eleSize * budgetsNum
		for i := range m.awaitFree.budget.shards {
			b := &m.awaitFree.budget.shards[i]
			if b.Used.Add(eleSize) > b.Capacity.Load() {
				err, _ := b.PullFromUpstream()
				require.NoError(t, err)
			}
		}
		expect := awaitFreePoolExecMetrics{budgetsNum, 0, 0}
		require.True(t, m.execMetrics.awaitFree == expect)

		expect.fail++
		{
			err, _ := m.awaitFree.budget.shards[0].Reserve(m.limit())
			require.Error(t, err)
		}
		m.checkAwaitFree()
		require.True(t, m.execMetrics.awaitFree == expect)

		require.True(t, m.awaitFreePoolUsed() == hpMemUsed)
		require.True(t, m.awaitFreePoolCap() == m.awaitFree.pool.roundSize(eleSize)*budgetsNum)
		{
			ori := nowUnixMilli() - DefTrackMemStatsDurMilli
			m.avoidance.lastUpdateUtimeMilli.Store(ori)
			require.True(t, m.tryUpdateTrackedMemStats(nowUnixMilli()))
			require.True(t, m.avoidance.lastUpdateUtimeMilli.Load() != ori)
		}

		require.Equal(t, m.avoidance.heapTracked.Load(), hpMemUsed)
		require.True(t, m.buffer.size.Load() == 0)
		require.True(t, m.avoidance.size.Load() == 0)

		e1Men := int64(13)
		e1 := m.newPoolWithHelperForTest(
			ArbitrateMemPriorityMedium, NoWaitAverse, false)
		e1.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
			return e1Men
		}
		e2 := m.addEntryForTest(nil)
		e3 := m.addEntryForTest(newDefCtxForTest(ArbitrateMemPriorityMedium))
		m.updateTrackedMemStats()
		hpMemUsed += e1Men
		require.Equal(t, m.avoidance.heapTracked.Load(), hpMemUsed)
		require.True(t, m.buffer.size.Load() == e1Men)
		require.True(t, m.avoidance.size.Load() == 0)

		free := int64(1024)
		e1Men = 17
		m.setMemStatsForTest(hpMemUsed, hpMemUsed+100, hpMemUsed+free)
		require.True(t, m.heapController.heapInuse.Load() == (hpMemUsed+100))
		require.True(t, m.heapController.heapAlloc.Load() == (hpMemUsed))
		require.True(t, m.heapController.heapTotalFree.Load() == free)
		require.True(t, int64(m.heapController.heapInuse.Load())-m.avoidance.heapTracked.Load() > m.mu.limit-m.mu.softLimit.size)
		require.True(t, int64(m.heapController.heapInuse.Load())-m.avoidance.heapTracked.Load() == 100)
		require.Equal(t, m.avoidance.heapTracked.Load(), hpMemUsed)
		require.True(t, m.buffer.size.Load() == 13) // no update
		require.True(t, m.avoidance.size.Load() == 100)

		m.updateTrackedMemStats()
		require.True(t, m.buffer.size.Load() == e1Men)
		e1Men = 3
		m.updateTrackedMemStats()
		require.True(t, m.buffer.size.Load() == 17) // no update to smaller size

		now := time.Now()
		for i := range m.awaitFree.budget.shards {
			b := &m.awaitFree.budget.shards[i]
			if i%2 == 0 {
				b.Used.Store(0)
				if i == 0 {
					b.LastUsedTimeSec = now.Unix() - (DefAwaitFreePoolShrinkDurMilli/Kilo - 1)
				} else {
					b.LastUsedTimeSec = 0
				}
			} else {
				b.Used.Store(1)
			}
		}
		m.cleanupNotifer()
		require.False(t, m.notiferIsAwake())
		{
			ori := now.UnixMilli() - DefAwaitFreePoolShrinkDurMilli
			m.awaitFree.lastShrinkUtimeMilli.Store(ori)
			require.True(t, m.tryShrinkAwaitFreePool(0, 0, now.UnixMilli()))
			require.True(t, m.awaitFree.lastShrinkUtimeMilli.Load() != ori)
		}
		require.True(t, m.notiferIsAwake())
		expect.shrink++
		require.True(t, m.execMetrics.awaitFree == expect)
		m.checkAwaitFree()

		for i := range m.awaitFree.budget.shards {
			b := &m.awaitFree.budget.shards[i]
			if i%2 == 0 {
				require.True(t, b.Used.Load() == 0)
				if i == 0 {
					require.True(t, b.Capacity.Load() != 0)
				} else {
					require.True(t, b.Capacity.Load() == 0)
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
		m.clearAwaitFreeForTest()
		m.setMemStatsForTest(0, 0, 0)
		require.True(t, m.allocated() == 0)
	}

	{ // test calc buffer
		m.resetExecMetrics()
		m.tryToUpdateBuffer(2, 3, DefUpdateBufferTimeAlignSec)
		require.Equal(t, m.buffer.size.Load(), int64(2))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(3))

		m.tryToUpdateBuffer(1, 1, DefUpdateBufferTimeAlignSec)
		require.Equal(t, m.buffer.size.Load(), int64(2))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(3))

		m.tryToUpdateBuffer(4, 1, DefUpdateBufferTimeAlignSec)
		require.Equal(t, m.buffer.size.Load(), int64(4))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(3))

		m.tryToUpdateBuffer(1, 6, DefUpdateBufferTimeAlignSec)
		require.Equal(t, m.buffer.size.Load(), int64(4))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(6))

		m.tryToUpdateBuffer(1, 1, DefUpdateBufferTimeAlignSec*(DefRedundancy))
		require.Equal(t, m.buffer.size.Load(), int64(4))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(6))

		m.tryToUpdateBuffer(3, 4, DefUpdateBufferTimeAlignSec*(DefRedundancy+1))
		require.Equal(t, m.buffer.size.Load(), int64(3))
		require.Equal(t, m.buffer.quotaLimit.Load(), int64(4))

		m.tryToUpdateBuffer(1, 1, DefUpdateBufferTimeAlignSec*(DefRedundancy+1))
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
		m.UpdateDigestProfileCache(digestID1, 107, DefUpdateBufferTimeAlignSec)
		{
			a, ok := m.GetDigestProfileCache(digestID1, 4)
			require.True(t, ok)
			require.True(t, a == 1009)
		}
		m.UpdateDigestProfileCache(digestID1, 107, DefUpdateBufferTimeAlignSec*2)
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

		testNow := DefDigestProfileMemTimeoutSec + 1
		uid := uint64(107)

		data := []dataType{
			{5003, 0, 2},
			{7, 0, 2}, // small
			{4099, testNow - DefDigestProfileSmallMemTimeoutSec, 2}, // small
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
		require.True(t, m.shrinkDigestProfile(testNow, DefMax, 0) == 0)
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
		m.heapController.lastGC.endUtimeSec.Store(0)
		m.heapController.lastGC.heapAlloc.Store(0)
		require.True(t, m.execMetrics.action.gc == 0)
		require.True(t, m.isMemSafe())

		expectBufferSize := int64(-1)
		gc := 0
		m.actions.GC = func() {
			require.True(t, m.heapController.heapAlloc.Load() == heap)
			require.True(t, m.buffer.size.Load() == expectBufferSize)
			gc++
		}

		m.SetAbleToGC()
		e1 := m.newPoolWithHelperForTest(ArbitrateMemPriorityMedium, NoWaitAverse, false)
		e1MemUsed := int64(0)
		e1.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
			return e1MemUsed
		}
		m.prepareAlloc(e1, alloc)
		expectBufferSize = 100 // original buffer size
		require.True(t, m.runOneRound() == 0)
		require.True(t, m.execMetrics.action.gc == 1)
		require.True(t, gc == 1)
		require.True(t, m.heapController.heapAlloc.Load() == heap)
		require.True(t, m.heapController.heapInuse.Load() == heap)
		require.True(t, m.heapController.lastGC.heapAlloc.Load() == heap)
		require.True(t, m.heapController.lastGC.endUtimeSec.Load() != 0)
		m.heapController.lastGC.endUtimeSec.Store(0)

		// not able for Runtime GC
		require.True(t, m.runOneRound() == 0)
		require.True(t, m.execMetrics.action.gc == 1)
		require.True(t, gc == 1)
		require.True(t, m.heapController.heapAlloc.Load() == heap)
		require.True(t, m.heapController.heapInuse.Load() == heap)
		require.True(t, m.heapController.lastGC.heapAlloc.Load() == heap)
		require.True(t, m.heapController.lastGC.endUtimeSec.Load() == 0)

		m.actions.UpdateRuntimeMemStats = func() {
			heap = 450 // -50
			m.setMemStatsForTest(heap, heap, 0)
		}
		m.SetAbleToGC()
		require.True(t, m.runOneRound() == 0)
		require.True(t, m.execMetrics.action.gc == 2)
		require.True(t, gc == 2)
		require.True(t, m.heapController.heapAlloc.Load() == heap) // heap -= 50
		require.True(t, m.heapController.heapInuse.Load() == heap)
		require.True(t, m.heapController.lastGC.heapAlloc.Load() == heap)
		require.True(t, m.heapController.lastGC.endUtimeSec.Load() != 0)
		m.heapController.lastGC.endUtimeSec.Store(0)

		m.actions.UpdateRuntimeMemStats = func() {
			heap = 400 // -50
			m.setMemStatsForTest(heap, heap, 0)
		}
		m.SetAbleToGC()
		require.True(t, m.runOneRound() == 1) // success
		require.True(t, m.waitAlloc(e1) == ArbitrateOk)
		require.True(t, m.execMetrics.action.gc == 3)
		require.True(t, gc == 3)
		require.True(t, m.heapController.heapAlloc.Load() == heap) // 400
		require.True(t, m.heapController.heapInuse.Load() == heap)
		require.True(t, m.heapController.lastGC.heapAlloc.Load() == heap)
		require.True(t, m.heapController.lastGC.endUtimeSec.Load() != 0)
		require.True(t, m.avoidance.heapTracked.Load() == 0)
		require.True(t, m.avoidance.size.Load() == 400)
		m.heapController.lastGC.endUtimeSec.Store(0)

		m.prepareAlloc(e1, 50)
		require.True(t, m.runOneRound() == 0) // 1000 - 500 - 400 - 100 < 50
		require.True(t, m.execMetrics.action.gc == 3)
		require.True(t, gc == 3)
		require.True(t, m.avoidance.size.Load() == 400)
		require.True(t, m.buffer.size.Load() == 100)

		m.actions.UpdateRuntimeMemStats = func() {
			require.True(t, heap == 400)
			m.setMemStatsForTest(heap, heap, 0)
		}
		m.SetAbleToGC()
		e1MemUsed = 150
		require.True(t, e1MemUsed > m.buffer.size.Load())
		expectBufferSize = e1MemUsed          // equal to e1 memory used
		require.True(t, m.runOneRound() == 1) // success: // 1000 - 500 - 150 - 250 > 50
		require.True(t, m.waitAlloc(e1) == ArbitrateOk)

		require.True(t, m.execMetrics.action.gc == 4)
		require.True(t, gc == 4)
		require.True(t, m.heapController.heapAlloc.Load() == heap)
		require.True(t, m.heapController.heapInuse.Load() == heap)
		require.True(t, m.heapController.lastGC.heapAlloc.Load() == heap)
		require.True(t, m.heapController.lastGC.endUtimeSec.Load() != 0)
		require.True(t, m.avoidance.heapTracked.Load() == e1MemUsed)
		require.True(t, m.avoidance.size.Load() == 400-e1MemUsed)
		m.heapController.lastGC.endUtimeSec.Store(0)

		m.deleteEntryForTest(e1)
		m.checkEntryForTest()

		m.updateTrackedMemStats()
		m.setMemStatsForTest(0, 0, 0)
	}
	{ // test mem risk
		type MockMetrcis struct {
			execMetricsAction
			logs MockLogs
		}
		newLimit := int64(100000)
		const heapInuseRateMilli = 950
		mockHeap := MockHeap{0, multiRatio(newLimit, heapInuseRateMilli), 0, 0}
		tMetrics := MockMetrcis{}
		m.actions.UpdateRuntimeMemStats = func() {
			tMetrics.updateRuntimeMemStats++
			m.setMemStats(mockHeap[0], mockHeap[1], mockHeap[2], mockHeap[3])
		}
		m.actions.GC = func() {
			tMetrics.gc++
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

		m.resetExecMetrics()
		require.True(t, m.heapController.heapAlloc.Load() == 0)
		require.True(t, m.heapController.heapInuse.Load() == 0)
		require.True(t, m.heapController.heapTotalFree.Load() == 0)
		require.True(t, m.avoidance.heapTracked.Load() == 0)
		require.True(t, m.avoidance.size.Load() == m.limit()-m.softLimit())
		require.True(t, m.avoidance.memMagnif.ratio.Load() == 0)
		m.SetLimit(uint64(newLimit))
		m.SetSoftLimit(0, 0, SoftLimitModeDefault)
		require.True(t, m.isMemSafe() && m.hasNoMemRisk())
		e1 := m.newPoolWithHelperForTest(ArbitrateMemPriorityLow, NoWaitAverse, true)
		e2 := m.newPoolWithHelperForTest(ArbitrateMemPriorityLow, NoWaitAverse, true)
		e3 := m.newPoolWithHelperForTest(ArbitrateMemPriorityHigh, NoWaitAverse, true)
		e4 := m.newPoolWithHelperForTest(ArbitrateMemPriorityLow, NoWaitAverse, true)
		e5 := m.newPoolWithHelperForTest(ArbitrateMemPriorityLow, NoWaitAverse, true)
		{
			m.UnixTimeSec = 0
			require.NoError(t, m.AllocQuotaFromGlobalMemArbitrator(0, 1000, true))
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
			require.True(t, m.execMetrics.task.pairSuccessFail == pairSuccessFail{3, 0})

			e1.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
				return 1009
			}
		}
		m.refreshRuntimeMemStats()
		require.True(t, m.softLimit() == multiRatio(newLimit, heapInuseRateMilli))
		require.True(t, m.avoidance.size.Load() == multiRatio(newLimit, heapInuseRateMilli)) // no tracked size
		require.False(t, m.isMemSafe())                                                      // heapInuse >= 0.95
		require.True(t, m.hasNoMemRisk())                                                    // < 0.9

		mockHeap = MockHeap{multiRatio(newLimit, 900), multiRatio(newLimit, heapInuseRateMilli), multiRatio(newLimit, 900) + 500}
		m.refreshRuntimeMemStats()
		require.True(t, m.heapController.heapTotalFree.Load() == 500)
		require.True(t, m.heapController.heapAlloc.Load() == multiRatio(newLimit, 900))
		require.True(t, m.heapController.heapInuse.Load() == multiRatio(newLimit, heapInuseRateMilli))
		require.False(t, m.isMemSafe())    // >= 0.95
		require.False(t, m.hasNoMemRisk()) // >= 0.9

		require.True(t, m.execMetrics.risk == execMetricsRisk{})
		require.False(t, m.heapController.oomCheck.start)
		require.True(t, m.heapController.oomCheck.startTime.IsZero())
		require.True(t, m.heapController.oomCheck.lastMemStats.startTime.IsZero())
		require.True(t, m.heapController.oomCheck.lastMemStats.heapTotalFree == 0)
		require.True(t, m.MinHeapFreeSpeedBPS() != 0)
		require.True(t, m.avoidance.memMagnif.ratio.Load() == 0)
		m.resetExecMetrics()

		tMetrics = MockMetrcis{}

		m.heapController.memStateRecorder.RecordMemState.(*memStateRecorderForTest).store = func(rmsv *RuntimeMemStateV1) error {
			tMetrics.recordMemState.fail++
			return errors.New("test")
		}
		m.heapController.memStateRecorder.lastMemState.Store(&RuntimeMemStateV1{
			Version: 1,
			Magnif:  1111,
		})
		m.SetMinHeapFreeSpeedBPS(2) // 2 B/s
		debugTime = time.Now()
		startTime := debugTime
		require.True(t, m.MinHeapFreeSpeedBPS() == 2)
		{
			require.True(t, m.runOneRound() == -2)
			require.True(t, m.heapController.oomCheck.start)
			require.True(t, m.heapController.oomCheck.startTime == startTime)
			require.True(t, m.heapController.oomCheck.lastMemStats.startTime == startTime)
			require.True(t, m.heapController.oomCheck.lastMemStats.heapTotalFree == 500)
			lastRiskMemState := m.lastMemState()
			require.True(t, lastRiskMemState != nil)
			require.True(t, *lastRiskMemState == RuntimeMemStateV1{
				Version: 1,
				LastRisk: LastRisk{
					HeapAlloc:  multiRatio(newLimit, 900),
					QuotaAlloc: 16000,
				},
				Magnif:        5725, // 90000 / 16000 + 0.1
				PoolMediumCap: 0,
			})
			require.True(t, m.avoidance.memMagnif.ratio.Load() == lastRiskMemState.Magnif)
			require.True(t, m.avoidance.heapTracked.Load() == 0)
			require.True(t, m.avoidance.size.Load() == 95000) // heapinuse - 0
			require.Equal(t, MockMetrcis{
				execMetricsAction{gc: 1, updateRuntimeMemStats: 1, recordMemState: pairSuccessFail{0, 1}},
				MockLogs{0, 1, 1},
			}, tMetrics) // gc when start oom check
			require.True(t, m.execMetrics.action == tMetrics.execMetricsAction)
			require.True(t, m.execMetrics.risk == execMetricsRisk{memRisk: 1, oomRisk: 0})
		}

		{ // wait to check oom risk
			expectMetrics := m.execMetrics
			expectMetrics.action.updateRuntimeMemStats++
			lastRiskMemState := *m.lastMemState()
			m.heapController.memStateRecorder.RecordMemState.(*memStateRecorderForTest).store = func(rmsv *RuntimeMemStateV1) error {
				tMetrics.recordMemState.success++
				return nil
			}
			debugTime = m.heapController.oomCheck.lastMemStats.startTime.Add(DefHeapReclaimCheckDuration - 1)
			require.True(t, m.runOneRound() == -2)
			require.True(t, m.heapController.oomCheck.start)
			require.True(t, m.heapController.oomCheck.startTime == startTime)
			require.True(t, m.heapController.oomCheck.lastMemStats.startTime == startTime)
			require.True(t, m.heapController.oomCheck.lastMemStats.heapTotalFree == 500)
			require.True(t, m.avoidance.memMagnif.ratio.Load() == lastRiskMemState.Magnif)
			require.True(t, *m.lastMemState() == lastRiskMemState)
			require.True(t, expectMetrics == m.execMetrics)
			require.Equal(t, MockMetrcis{
				expectMetrics.action,
				MockLogs{0, 1, 1},
			}, tMetrics)
		}
		{
			require.True(t, m.avoidance.heapTracked.Load() == 0)
			expectMetrics := m.execMetrics
			m.SetAbleToGC() // able to gc
			expectMetrics.action.gc++
			expectMetrics.action.updateRuntimeMemStats++

			require.True(t, m.runOneRound() == -2)
			require.True(t, expectMetrics == m.execMetrics)
			require.Equal(t, MockMetrcis{
				expectMetrics.action,
				MockLogs{0, 1, 1},
			}, tMetrics)
			require.True(t, m.avoidance.heapTracked.Load() == 1009)
		}

		{ // next round of oom check
			lastRiskMemState := *m.lastMemState()
			debugTime = m.heapController.oomCheck.lastMemStats.startTime.Add(DefHeapReclaimCheckDuration)
			mockHeap = MockHeap{multiRatio(newLimit, 900), multiRatio(newLimit, heapInuseRateMilli), multiRatio(newLimit, 900) + 500 + 2}
			require.True(t, m.runOneRound() == -2)
			require.True(t, m.heapController.oomCheck.start)
			require.True(t, m.heapController.oomCheck.startTime == startTime)
			require.True(t, m.heapController.oomCheck.lastMemStats.startTime == debugTime) // update last mem stats
			require.True(t, m.heapController.oomCheck.lastMemStats.heapTotalFree == 500+2)
			require.True(t, m.avoidance.memMagnif.ratio.Load() == lastRiskMemState.Magnif)
			require.True(t, *m.lastMemState() == lastRiskMemState)
			require.Equal(t, MockMetrcis{
				execMetricsAction{
					gc:                    3, // gc when each round of oom check
					updateRuntimeMemStats: 5, // refresh each round; gc & refresh oom check round
					recordMemState:        pairSuccessFail{0, 1}},
				MockLogs{0, 2, 1},
			}, tMetrics)
			require.True(t, tMetrics.execMetricsAction == m.execMetrics.action)
			require.True(t, m.execMetrics.risk == execMetricsRisk{1, 0, numByPriority{}})
		}

		{
			debugTime = m.heapController.oomCheck.lastMemStats.startTime.Add(DefHeapReclaimCheckDuration * 1)
			lastRiskMemState := *m.lastMemState()
			mockHeap = MockHeap{multiRatio(newLimit, 900), multiRatio(newLimit, heapInuseRateMilli), multiRatio(newLimit, 900) + 500 + 4}
			m.SetAbleToGC()
			require.True(t, m.runOneRound() == -2)
			require.True(t, m.heapController.oomCheck.lastMemStats.startTime == debugTime)
			require.True(t, m.heapController.oomCheck.lastMemStats.heapTotalFree == 500+4)
			require.True(t, m.avoidance.memMagnif.ratio.Load() == lastRiskMemState.Magnif)
			require.True(t, *m.lastMemState() == lastRiskMemState)
			require.Equal(t, MockMetrcis{
				execMetricsAction{
					gc:                    4, // gc when each round of oom check
					updateRuntimeMemStats: 6, // refresh each round
					recordMemState:        pairSuccessFail{0, 1}},
				MockLogs{0, 3, 1},
			}, tMetrics)
			require.True(t, tMetrics.execMetricsAction == m.execMetrics.action)
			require.Equal(t, execMetricsRisk{1, 0, numByPriority{}}, m.execMetrics.risk)
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
			debugTime = m.heapController.oomCheck.lastMemStats.startTime.Add(DefHeapReclaimCheckDuration * 1)
			mockHeap = MockHeap{multiRatio(newLimit, 900), multiRatio(newLimit, heapInuseRateMilli), multiRatio(newLimit, 900) + 500 + 4 + 1}
			require.True(t, m.runOneRound() == -2)
			require.False(t, hasMemOOMRisk(m.MinHeapFreeSpeedBPS(), m.MinHeapFreeSpeedBPS(), time.Time{}.Add(DefHeapReclaimCheckMaxDuration), time.Time{}))
			require.True(t, hasMemOOMRisk(0, 0, time.Time{}.Add(DefHeapReclaimCheckMaxDuration).Add(time.Nanosecond), time.Time{}))
			require.True(t, m.heapController.oomCheck.lastMemStats.startTime == debugTime)
			require.True(t, m.heapController.oomCheck.lastMemStats.heapTotalFree == 500+4+1)
			require.True(t, m.avoidance.memMagnif.ratio.Load() == lastRiskMemState.Magnif)
			require.True(t, *m.lastMemState() == lastRiskMemState)
			require.Equal(t, MockMetrcis{
				execMetricsAction{
					gc:                    4, // no gc and reclaim by kill
					updateRuntimeMemStats: 7, // refresh each round
					recordMemState:        pairSuccessFail{0, 1}},
				MockLogs{0, 7, 1}, // free speed is too low; start to kill; make task failed; restart check;
			}, tMetrics)
			require.True(t, m.waitAlloc(e2) == ArbitrateFail)
			require.True(t, m.execMetrics.task.pairSuccessFail == pairSuccessFail{0, 1})
			require.True(t, tMetrics.execMetricsAction == m.execMetrics.action)
			require.Equal(t, execMetricsRisk{1, 1, numByPriority{1}}, m.execMetrics.risk)
			require.Equal(t, map[uint64]int{e2.pool.uid: 1}, killEvent)

			require.True(t, m.underKill.num == 1)

			debugTime = m.heapController.oomCheck.lastMemStats.startTime.Add(DefHeapReclaimCheckDuration * 1)
			require.True(t, m.runOneRound() == -2)
			require.Equal(t, MockMetrcis{
				execMetricsAction{
					gc:                    5, // gc when each round of oom check
					updateRuntimeMemStats: 9, // gc; refresh each round;
					recordMemState:        pairSuccessFail{0, 1}},
				MockLogs{0, 8, 1}, // free speed is too low
			}, tMetrics)
			require.True(t, e2.ctx.Load().stopped.Load())
			select {
			case <-e2.ctx.Load().cancelCh:
			default:
				require.Fail(t, "")
			}
			require.Equal(t, execMetricsRisk{1, 2, numByPriority{1}}, m.execMetrics.risk)

			debugTime = m.heapController.oomCheck.lastMemStats.startTime.Add(DefKillCancelCheckDuration)
			require.True(t, m.runOneRound() == -2)
			require.Equal(t, MockMetrcis{
				execMetricsAction{
					gc:                    6,  // gc when each round of oom check
					updateRuntimeMemStats: 11, // gc; refresh each round;
					recordMemState:        pairSuccessFail{0, 1}},
				MockLogs{0, 9, 1}, // KILL root pool takes too long;
			}, tMetrics)
			require.Equal(t, execMetricsRisk{1, 3, numByPriority{1}}, m.execMetrics.risk)

			m.mu.allocated += 1e5
			m.entryMap.addQuota(e5, 1e5)
			e5.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
				return 1e5
			}
			e5.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).killCB = func() {
				killEvent[e5.pool.uid]++
			}

			debugTime = m.heapController.oomCheck.lastMemStats.startTime.Add(DefKillCancelCheckTimeout)
			require.True(t, m.runOneRound() == -2)
			require.Equal(t, MockMetrcis{
				execMetricsAction{
					gc:                    6,  // gc when each round of oom check
					updateRuntimeMemStats: 12, // refresh each round;
					recordMemState:        pairSuccessFail{0, 1}},
				MockLogs{0, 12, 2}, // too low; Failed to `KILL` root pool; Start to `KILL` root pool;
			}, tMetrics)
			require.True(t, m.underKill.entries[e2.pool.uid].arbitratorMu.underKill.fail)
			require.True(t, e5.arbitratorMu.underKill.bool)
			require.Equal(t, execMetricsRisk{1, 4, numByPriority{2}}, m.execMetrics.risk)

			m.removeEntryForTest(e2)
			m.removeEntryForTest(e5)
			debugTime = m.heapController.oomCheck.lastMemStats.startTime.Add(DefKillCancelCheckTimeout + DefKillCancelCheckDuration)
			require.True(t, m.runOneRound() == -2)
			require.True(t, m.underKill.num == 2)
			require.Equal(t, MockMetrcis{
				execMetricsAction{
					gc:                    6,
					updateRuntimeMemStats: 13, // refresh each round;
					recordMemState:        pairSuccessFail{0, 1}},
				MockLogs{0, 18, 2}, // too low; Start to KILL root pool;Start to KILL root pool;Restart check;
			}, tMetrics)
			require.Equal(t, execMetricsRisk{1, 5, numByPriority{3, 0, 1}}, m.execMetrics.risk)
			require.Equal(t, map[uint64]int{e1.pool.uid: 1, e2.pool.uid: 1, e3.pool.uid: 1, e5.pool.uid: 1}, killEvent)
			m.removeEntryForTest(e1)
			m.removeEntryForTest(e3)
			mockHeap = MockHeap{multiRatio(newLimit, 900) - 1, multiRatio(newLimit, heapInuseRateMilli) - 1}
			require.True(t, m.avoidance.heapTracked.Load() == 1009)
			e4.ctx.Load().arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
				return 1013
			}
			require.True(t, m.buffer.size.Load() == 1009)
			require.True(t, m.runOneRound() == 0)
			require.True(t, m.avoidance.heapTracked.Load() == 1013)
			require.True(t, m.buffer.size.Load() == 1013)
			require.Equal(t, MockMetrcis{
				execMetricsAction{
					gc:                    6,
					updateRuntimeMemStats: 14, // gc; refresh each round;
					recordMemState:        pairSuccessFail{0, 1}},
				MockLogs{1, 20, 2}, // mem is safe; 2 * "Finish to `KILL` root pool";
			}, tMetrics)
			require.True(t, !m.heapController.oomCheck.start)
		}
		m.UnixTimeSec = 0
		require.NoError(t, m.AllocQuotaFromGlobalMemArbitrator(0, -1000, true))
		m.shrinkAwaitFreePool(0, 0, DefAwaitFreePoolShrinkDurMilli)
		require.True(t, m.avoidance.memMagnif.ratio.Load() == 5725)
		require.True(t, m.avoidance.size.Load() == mockHeap[1]-1013) // heapinuse. no affect becuase of mode is
		m.deleteEntryForTest(e4)
		m.checkEntryForTest()
	}
	{ // test tick task: reduce mem magnification
		m.resetExecMetrics()
		require.True(t, m.RootPoolNum() == 0)
		require.True(t, m.allocated() == 0)
		debugTime = time.Unix(DefUpdateMemMagnifUtimeAlign, 0)
		m.UnixTimeSec = debugTime.Unix()

		e1ctx := m.newCtxWithHelperForTest(ArbitrateMemPriorityMedium, NoWaitAverse, RequirePrivilege)
		e1ctx.hisMaxMemUsed = 23
		e1ctx.memQuotaLimit = 29
		e1ctx.arbitrateHelper.(*arbitrateHelperForTest).heapUsedCB = func() int64 {
			return 31
		}
		e1 := m.addEntryForTest(e1ctx)

		require.True(t, m.buffer.size.Load() == 23)
		require.True(t, m.buffer.quotaLimit.Load() == 0)
		m.updateTrackedMemStats()
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
			m.heapController.oomCheck.start = true
			require.False(t, m.executeTick(DefMax))
			m.heapController.oomCheck.start = false
		}

		type mockTimeline struct {
			pre int64
			now int64
		}
		mockTimeLine := mockTimeline{
			0, int64(DefUpdateMemMagnifUtimeAlign * Kilo),
		}

		nextTime := func() {
			mockTimeLine.pre = mockTimeLine.now
			mockTimeLine.now += DefUpdateMemMagnifUtimeAlign * Kilo
		}
		checkMockTimeProfImpl := func(ms, heap, quota, ratio int64) {
			sec := ms / Kilo
			align := sec / DefUpdateMemMagnifUtimeAlign
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

		{ // new suggest pool cap: last mem state is nil
			oriMemState := m.lastMemState()
			require.Equal(t,
				RuntimeMemStateV1{Version: 1, LastRisk: LastRisk{HeapAlloc: 90000, QuotaAlloc: 16000}, Magnif: 5725, PoolMediumCap: 0},
				*oriMemState)

			require.True(t, m.avoidance.memMagnif.ratio.Load() == 5725)
			require.True(t, m.poolMediumQuota() == 0)
			require.True(t, m.poolAllocStats.lastStoreUtimeMilli.Load() == 0)
			require.True(t, m.execMetrics.action.recordMemState.success == 0)

			m.heapController.memStateRecorder.lastMemState.Store(nil)
			require.True(t, m.executeTick(mockTimeLine.now))
			require.True(t, m.execMetrics.action.recordMemState.success == 1)
			require.True(t, logs.info == 1)
			require.True(t, m.poolMediumQuota() == 400)
			require.Equal(t, RuntimeMemStateV1{
				Version:       1,
				PoolMediumCap: 400,
			}, *m.lastMemState())
			oriMemState.PoolMediumCap = m.poolMediumQuota()
			m.heapController.memStateRecorder.lastMemState.Store(oriMemState)
			require.Equal(t, RuntimeMemStateV1{
				Version: 1, LastRisk: LastRisk{HeapAlloc: 90000, QuotaAlloc: 16000}, Magnif: 5725, PoolMediumCap: 400,
			}, *m.lastMemState())
			require.True(t, m.poolAllocStats.lastStoreUtimeMilli.Load() == mockTimeLine.now)
		}

		{
			// same value
			require.False(t, m.tryStorePoolMediumCapacity(mockTimeLine.now+DefTickDurMilli*10+1, 400))
			// time not satisfy
			require.False(t, m.tryStorePoolMediumCapacity(mockTimeLine.now+DefTickDurMilli*10-1, 399))
			require.True(t, m.execMetrics.action.recordMemState.success == 1)
			require.True(t, logs.info == 1)
			// new suggest pool cap: last mem state is not nil & SuggestPoolInitCap not same
			require.True(t, m.tryStorePoolMediumCapacity(mockTimeLine.now+DefTickDurMilli*10, 401))
			require.True(t, m.execMetrics.action.recordMemState.success == 2)
			require.True(t, logs.info == 2)
			require.True(t, m.lastMemState().PoolMediumCap == 401)
		}

		{
			require.True(t, m.mu.softLimit.mode == SoftLimitModeDefault)
			m.setMemStats(0, 0, 0, 0)
			require.True(t, m.avoidance.size.Load() == 5000)
			m.SetSoftLimit(0, 0, SoftLimitModeAuto)
			m.setMemStats(0, 0, 0, 0)
			require.True(t, m.avoidance.size.Load() == 82533) // update avoid size under auto mode
		}

		{ // init set last gc state
			m.heapController.lastGC.heapAlloc.Store(DefMax)
			m.heapController.lastGC.endUtimeSec.Store(0)
		}

		// mock set oom risk threshold
		m.SetLimit(15000)
		require.True(t, m.limit() == 15000)
		m.mu.allocated = 20000
		require.True(t, m.allocated() == 20000)
		// update mock timeline
		nextTime()
		m.prepareAlloc(e1, DefMax)
		require.Equal(t, lastBlockedAt{}, m.wrapLastBlockedAt())
		m.UnixTimeSec = mockTimeLine.now/Kilo + DefUpdateMemMagnifUtimeAlign - 1
		m.heapController.lastGC.heapAlloc.Store(m.limit())
		m.heapController.lastGC.endUtimeSec.Store(m.UnixTimeSec)
		require.False(t, m.doExecuteFirstTask())
		require.Equal(t, lastBlockedAt{20000, m.UnixTimeSec}, m.wrapLastBlockedAt())
		// calculate ratio of the previous
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.execMetrics.action.recordMemState.success == 3) // new suggest pool cap
		require.True(t, logs.info == 3)
		require.True(t, m.lastMemState().PoolMediumCap == 400)
		checkMockTimeProf(mockTimeLine.pre, 0, 0)
		checkMockTimeProf(mockTimeLine.now, 15000, 20000)
		//
		nextTime()
		m.UnixTimeSec = mockTimeLine.now/Kilo + DefUpdateMemMagnifUtimeAlign - 1
		m.mu.allocated = 40000
		m.heapController.lastGC.heapAlloc.Store(35000)
		m.heapController.lastGC.endUtimeSec.Store(m.UnixTimeSec)
		require.False(t, m.doExecuteFirstTask())
		require.Equal(t, lastBlockedAt{40000, m.UnixTimeSec}, m.wrapLastBlockedAt())
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, logs.info == 4)
		require.True(t, m.execMetrics.action.recordMemState.success == 3)
		checkMockTimeProfImpl(mockTimeLine.pre, 15000, 20000, 750)
		checkMockTimeProf(mockTimeLine.now, 35000, 40000)
		//
		nextTime()
		m.UnixTimeSec = mockTimeLine.now/Kilo + DefUpdateMemMagnifUtimeAlign - 1
		m.mu.allocated = 50000
		m.heapController.lastGC.heapAlloc.Store(40000)
		m.heapController.lastGC.endUtimeSec.Store(m.UnixTimeSec)
		require.False(t, m.doExecuteFirstTask())
		require.Equal(t, lastBlockedAt{50000, m.UnixTimeSec}, m.wrapLastBlockedAt())
		require.True(t, m.executeTick(mockTimeLine.now))
		// no update because the heap stats is NOT safe
		require.True(t, m.avoidance.memMagnif.ratio.Load() == 5725)
		// timed mem profile
		require.True(t, logs.info == 5)
		// record
		require.True(t, m.execMetrics.action.recordMemState.success == 3)
		checkMockTimeProfImpl(mockTimeLine.pre, 35000, 40000, 875)
		checkMockTimeProf(mockTimeLine.now, 40000, 50000)

		// restore limit to 100000
		m.SetLimit(1e5)
		nextTime()
		m.UnixTimeSec = mockTimeLine.now/Kilo + 1
		m.execMu.blockedState.allocated = 50000
		m.execMu.blockedState.utimeSec = m.UnixTimeSec
		m.heapController.lastGC.heapAlloc.Store(40000)
		m.heapController.lastGC.endUtimeSec.Store(m.UnixTimeSec)
		// Update mem quota magnification ratio
		// new magnification ratio
		// timed mem profile
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (875+5725)/2) // choose 875 rather than 800
		require.True(t, logs.info == 8)
		require.True(t, m.execMetrics.action.recordMemState.success == 4)
		checkMockTimeProfImpl(mockTimeLine.pre, 40000, 50000, 800)
		checkMockTimeProf(mockTimeLine.now, 40000, 50000)

		mockTimeLine.pre = mockTimeLine.now
		mockTimeLine.now += 1 * Kilo
		m.UnixTimeSec += 1
		m.mu.allocated = 40000
		m.heapController.lastGC.heapAlloc.Store(41000)
		m.heapController.lastGC.endUtimeSec.Store(m.UnixTimeSec)
		require.False(t, m.doExecuteFirstTask())
		require.Equal(t, lastBlockedAt{40000, m.UnixTimeSec}, m.wrapLastBlockedAt())
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (875+5725)/2) // no update
		require.True(t, logs.info == 8)
		require.True(t, m.execMetrics.action.recordMemState.success == 4)
		checkMockTimeProf(mockTimeLine.now-Kilo, 41000, 50000)
		mockTimeLine.now = mockTimeLine.pre + DefUpdateMemMagnifUtimeAlign*Kilo
		require.True(t, m.executeTick(mockTimeLine.now))
		checkMockTimeProfImpl(mockTimeLine.pre, 41000, 50000, 820)
		checkMockTimeProf(mockTimeLine.now, 0, 0)
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (3300+820)/2)
		require.True(t, logs.info == 11)
		require.True(t, m.execMetrics.action.recordMemState.success == 5)

		nextTime()
		// no valid pre profile
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, logs.info == 13)
		require.True(t, m.execMetrics.action.recordMemState.success == 6)
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (2060+820)/2)
		checkMockTimeProf(mockTimeLine.now, 0, 0)
		checkMockTimeProf(mockTimeLine.pre, 0, 0)
		// update mem quota magnification ratio
		require.True(t, logs.info == 13)
		require.True(t, m.execMetrics.action.recordMemState.success == 6)

		nextTime()
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (2060+820)/2) // no update
		nextTime()

		// new start
		nextTime()
		m.UnixTimeSec = mockTimeLine.now / Kilo
		m.execMu.blockedState.allocated = 10000
		m.execMu.blockedState.utimeSec = m.UnixTimeSec
		m.heapController.lastGC.heapAlloc.Store(10000)
		m.heapController.lastGC.endUtimeSec.Store(m.UnixTimeSec)
		require.True(t, m.executeTick(mockTimeLine.now))
		// no update
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (2060+820)/2) // no update
		require.True(t, logs.info == 13)
		require.True(t, m.execMetrics.action.recordMemState.success == 6)

		nextTime()
		m.UnixTimeSec = mockTimeLine.now / Kilo
		m.execMu.blockedState.allocated = 11111
		m.execMu.blockedState.utimeSec = m.UnixTimeSec
		m.heapController.lastGC.heapAlloc.Store(2222)
		m.heapController.lastGC.endUtimeSec.Store(m.UnixTimeSec)
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (2060+820)/2) // no update
		// timed mem profile
		require.True(t, logs.info == 14)
		require.True(t, m.execMetrics.action.recordMemState.success == 6)
		checkMockTimeProfImpl(mockTimeLine.pre, 10000, 10000, 1000)
		checkMockTimeProf(mockTimeLine.now, 2222, 11111)

		nextTime()
		m.UnixTimeSec = mockTimeLine.now / Kilo
		m.execMu.blockedState.allocated = 10000
		m.execMu.blockedState.utimeSec = m.UnixTimeSec
		m.heapController.lastGC.heapAlloc.Store(10000)
		m.heapController.lastGC.endUtimeSec.Store(m.UnixTimeSec)
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (1440+1000)/2)
		require.True(t, logs.info == 17)
		require.True(t, m.execMetrics.action.recordMemState.success == 7)
		checkMockTimeProfImpl(mockTimeLine.pre, 2222, 11111, 199)
		checkMockTimeProf(mockTimeLine.now, 10000, 10000)

		nextTime()
		m.UnixTimeSec = mockTimeLine.now / Kilo
		m.execMu.blockedState.allocated = 10000
		m.execMu.blockedState.utimeSec = m.UnixTimeSec
		m.heapController.lastGC.heapAlloc.Store(5000)
		m.heapController.lastGC.endUtimeSec.Store(m.UnixTimeSec)
		require.True(t, m.executeTick(mockTimeLine.now))
		checkMockTimeProfImpl(mockTimeLine.pre, 10000, 10000, 1000)
		checkMockTimeProf(mockTimeLine.now, 5000, 10000)
		// choose 1000 rather than 199
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (1220+1000)/2)
		require.True(t, logs.info == 20) // update mem quota magnification ratio;
		require.True(t, m.execMetrics.action.recordMemState.success == 8)

		nextTime()
		m.UnixTimeSec = mockTimeLine.now / Kilo
		m.execMu.blockedState.allocated = 10000
		m.execMu.blockedState.utimeSec = m.UnixTimeSec
		m.heapController.lastGC.heapAlloc.Store(5000)
		m.heapController.lastGC.endUtimeSec.Store(m.UnixTimeSec)
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == (1110+1000)/2)
		require.True(t, logs.info == 23) // update mem quota magnification ratio;
		require.True(t, m.execMetrics.action.recordMemState.success == 9)

		nextTime()
		m.UnixTimeSec = mockTimeLine.now / Kilo
		m.execMu.blockedState.allocated = 10000
		m.execMu.blockedState.utimeSec = m.UnixTimeSec
		m.heapController.lastGC.heapAlloc.Store(20000)
		m.heapController.lastGC.endUtimeSec.Store(m.UnixTimeSec)
		require.True(t, m.executeTick(mockTimeLine.now))
		require.True(t, m.avoidance.memMagnif.ratio.Load() == 0) // smaller than 1000
		require.True(t, logs.info == 26)
		require.True(t, m.execMetrics.action.recordMemState.success == 10)
	}
}

func TestBasicUtils(t *testing.T) {
	testState = t

	{
		const cnt = 1 << 8
		bgId := uint64(4068484684)
		odd := 0
		for i := range cnt {
			n := shardIndexByUID(bgId+uint64(i)*2, cnt-1)
			if n&1 != 0 {
				odd++
			}
		}
		require.Equal(t, odd, cnt/2)
	}
	require.Equal(t, BaseQuotaUnit*(1<<(DefPoolQuotaShards-2)), 128*GB)
	require.Equal(t, getQuotaShard(0, DefPoolQuotaShards), 0)
	require.Equal(t, getQuotaShard(BaseQuotaUnit-1, DefPoolQuotaShards), 0)
	require.Equal(t, getQuotaShard(BaseQuotaUnit, DefPoolQuotaShards), 1)
	require.Equal(t, getQuotaShard(BaseQuotaUnit*2-1, DefPoolQuotaShards), 1)
	require.Equal(t, getQuotaShard(BaseQuotaUnit*2, DefPoolQuotaShards), 2)
	require.Equal(t, getQuotaShard(BaseQuotaUnit*4-1, DefPoolQuotaShards), 2)
	require.Equal(t, getQuotaShard(BaseQuotaUnit*4, DefPoolQuotaShards), 3)
	require.Equal(t, getQuotaShard(BaseQuotaUnit*(1<<(DefPoolQuotaShards-2))-1, DefPoolQuotaShards), DefPoolQuotaShards-2)
	require.Equal(t, getQuotaShard(BaseQuotaUnit*(1<<(DefPoolQuotaShards-2)), DefPoolQuotaShards), DefPoolQuotaShards-1)
	require.Equal(t, getQuotaShard(math.MaxInt64, DefPoolQuotaShards), DefPoolQuotaShards-1)

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
		data := WrapList[*rootPoolEntry]{}
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
		require.True(t, m.debug.now == nil)
		require.True(t, !m.innerTime().IsZero())
		m.debug.now = func() time.Time {
			return tm
		}
		require.True(t, m.innerTime() == tm)
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

	m := newMemArbitrator(
		4*GB,
		DefPoolStatusShards, DefPoolQuotaShards,
		64*KB, /* 64k ~ */
		&memStateRecorderForTest{
			load: func() (*RuntimeMemStateV1, error) {
				return nil, nil
			},
			store: func(*RuntimeMemStateV1) error {
				return nil
			}},
	)
	m.SetWorkMode(ArbitratorModeStandard)
	m.initAwaitFreePool(4, 4, 1)
	const N = 3000
	{
		m.asyncRun(DefTaskTickDur)
		cancelPool := atomic.Int64{}
		killedPool := atomic.Int64{}
		ch1 := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < N; i += 1 {

			wg.Add(1)
			go func() {
				<-ch1

				root, err := m.EmplaceRootPool(uint64(i))
				require.NoError(t, err)
				cancelCh := make(chan struct{})
				cancelEvent := 0
				killed := false
				ctx := NewContext(
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
					ArbitrateMemPriorityHigh,
					false,
					true,
				)
				if !root.StartWithMemArbitratorContext(m, ctx) {
					panic(fmt.Errorf("failed to init root pool with session-id %d", root.Pool.uid))
				}

				b := ConcurrentBudget{pool: root.Pool}

				for j := 0; j < 200; j += 1 {
					if b.Used.Add(m.limit()/150) > b.Capacity.Load() {
						_, _ = b.PullFromUpstream()
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
				wg.Done()
			}()
		}
		close(ch1)
		wg.Wait()

		require.True(t, m.rootPoolNum.Load() == N)
		require.True(t, killedPool.Load() == 0)
		require.True(t, cancelPool.Load() == N)
		for i := 0; i < N; i += 1 {
			m.RemoveRootPoolByID(uint64(i))
		}
		m.stopForTest()
		m.checkEntryForTest()
		require.True(t, m.execMetrics.task.fail == N)
		require.True(t, m.execMetrics.cancel.standardMode == N)
		m.resetExecMetrics()
	}

	m.SetWorkMode(ArbitratorModePriority)
	{
		m.asyncRun(DefTaskTickDur)
		cancelPool := atomic.Int64{}
		killedPool := atomic.Int64{}
		ch1 := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < N; i += 1 {

			wg.Add(1)
			go func() {
				<-ch1

				root, err := m.EmplaceRootPool(uint64(i))
				require.NoError(t, err)
				cancelCh := make(chan struct{})
				cancelEvent := 0
				killed := false

				prio := ArbitrateMemPriorityHigh
				p := i % 6
				if p < 2 {
					prio = ArbitrateMemPriorityLow
				} else if p < 4 {
					prio = ArbitrateMemPriorityMedium
				}
				waitAverse := true
				if p%2 == 0 {
					waitAverse = false
				}

				ctx := NewContext(
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
					prio,
					waitAverse,
					true,
				)

				if !root.StartWithMemArbitratorContext(m, ctx) {
					panic(fmt.Errorf("failed to init root pool with session-id %d", root.Pool.uid))
				}

				b := ConcurrentBudget{pool: root.Pool}

				for j := 0; j < 200; j += 1 {
					if b.Used.Add(m.limit()/150) > b.Capacity.Load() {
						_, _ = b.PullFromUpstream()
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
				wg.Done()
			}()
		}
		close(ch1)
		wg.Wait()

		require.True(t, m.rootPoolNum.Load() == N)
		require.True(t, killedPool.Load() == 0)
		require.True(t, cancelPool.Load() != 0)
		for i := 0; i < N; i += 1 {
			m.RemoveRootPoolByID(uint64(i))
		}
		m.stopForTest()
		m.checkEntryForTest()
		require.True(t, m.execMetrics.task.fail >= N/2)
		require.Equal(t, cancelPool.Load(),
			m.execMetrics.cancel.waitAverse+m.execMetrics.cancel.priorityMode[ArbitrateMemPriorityLow]+m.execMetrics.cancel.priorityMode[ArbitrateMemPriorityMedium]+m.execMetrics.cancel.priorityMode[ArbitrateMemPriorityHigh])
		require.True(t, m.execMetrics.cancel.waitAverse == N/2)
		// under priority mode, arbitrator may cancel pool which is not waiting for alloc
		require.GreaterOrEqual(t, cancelPool.Load(), m.execMetrics.task.fail)
		m.resetExecMetrics()
	}
}
